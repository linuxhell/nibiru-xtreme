//! Task persistence for the RPC server: unfinished tasks survive an engine restart under
//! the same GID, paused, without writing credentials to disk.

use base64::Engine as _;
use nibiru_xtreme::{
    bittorrent::{
        metainfo::{Extras, Torrent, build_info},
        session::{Session, SessionConfig},
        torrent::Settings,
    },
    engine::Engine,
    logging::LogSink,
    rpc::{
        RpcServer,
        bt::{BtConfig, BtService},
    },
};
use serde_json::{Value, json};
use std::{net::SocketAddr, path::Path, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn sample(len: usize, seed: u64) -> Vec<u8> {
    let mut x = 0x9e3779b97f4a7c15u64 ^ seed;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

/// Origin with a stable validator and byte ranges, so an interrupted download can resume.
async fn origin(body: Vec<u8>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let body = body.clone();
            tokio::spawn(async move {
                let mut buffer = vec![0u8; 8192];
                let n = stream.read(&mut buffer).await.unwrap_or(0);
                let head = String::from_utf8_lossy(&buffer[..n]).to_lowercase();
                let total = body.len();
                let range = head.lines().find_map(|line| {
                    let spec = line.strip_prefix("range: bytes=")?;
                    let (start, end) = spec.trim().split_once('-')?;
                    let start = start.parse::<usize>().ok()?;
                    let end = end
                        .parse::<usize>()
                        .ok()
                        .unwrap_or(total - 1)
                        .min(total - 1);
                    (start <= end).then_some((start, end))
                });
                let common = "Accept-Ranges: bytes\r\nETag: \"v1\"\r\nConnection: close\r\n";
                // A range that starts at or past the end is not satisfiable: answer like a real
                // server (416) instead of replaying the whole file with 200.
                let unsatisfiable = head.lines().any(|line| {
                    line.strip_prefix("range: bytes=")
                        .and_then(|spec| spec.split_once('-'))
                        .and_then(|(start, _)| start.trim().parse::<usize>().ok())
                        .is_some_and(|start| start >= total)
                });
                if unsatisfiable {
                    let reply = format!(
                        "HTTP/1.1 416 Range Not Satisfiable\r\n{common}Content-Range: bytes */{total}\r\nContent-Length: 0\r\n\r\n"
                    );
                    let _ = stream.write_all(reply.as_bytes()).await;
                    return;
                }
                let (status, extra, slice) = match range {
                    Some((start, end)) => (
                        "206 Partial Content",
                        format!("Content-Range: bytes {start}-{end}/{total}\r\n"),
                        &body[start..=end],
                    ),
                    None => ("200 OK", String::new(), &body[..]),
                };
                let reply = format!(
                    "HTTP/1.1 {status}\r\n{common}{extra}Content-Length: {}\r\n\r\n",
                    slice.len()
                );
                let _ = stream.write_all(reply.as_bytes()).await;
                if !head.starts_with("head ") {
                    let _ = stream.write_all(slice).await;
                }
            });
        }
    });
    port
}

struct Node {
    port: u16,
    server: RpcServer,
    client: reqwest::Client,
}

async fn node(session: Option<&Path>, bt: Option<Arc<BtService>>) -> Node {
    let log = Arc::new(LogSink::new(None).unwrap());
    let mut server = RpcServer::new(Engine::new(log.clone()).unwrap(), log, "k".into());
    if let Some(bt) = bt {
        server = server.with_bittorrent(bt);
    }
    if let Some(path) = session {
        server = server.with_session(path.to_owned()).await.unwrap();
    }
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let running = server.clone();
    tokio::spawn(async move { running.serve(port).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    Node {
        port,
        server,
        client: reqwest::Client::new(),
    }
}

impl Node {
    async fn try_call(&self, method: &str, params: Vec<Value>) -> Result<Value, String> {
        let mut all = vec![json!("token:k")];
        all.extend(params);
        let response: Value = self
            .client
            .post(format!("http://127.0.0.1:{}/jsonrpc", self.port))
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":all}))
            .send()
            .await
            .map_err(|error| error.to_string())?
            .json()
            .await
            .map_err(|error| error.to_string())?;
        match response.get("error") {
            Some(error) => Err(error["message"].as_str().unwrap_or("error").to_owned()),
            None => Ok(response["result"].clone()),
        }
    }

    async fn call(&self, method: &str, params: Vec<Value>) -> Value {
        self.try_call(method, params).await.unwrap()
    }

    async fn status(&self, gid: &str) -> Value {
        self.call("nibiru.tellStatus", vec![json!(gid)]).await
    }

    async fn wait(&self, gid: &str, wanted: &str) -> Value {
        for _ in 0..400 {
            let status = self.status(gid).await;
            if status["status"] == wanted {
                return status;
            }
            assert_ne!(status["status"], "error", "{status}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("never reached {wanted}: {}", self.status(gid).await);
    }

    async fn wait_progress(&self, gid: &str) {
        for _ in 0..400 {
            let completed: u64 = self.status(gid).await["completedLength"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            if completed > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("the transfer never made progress");
    }

    async fn pause_and_save(&self, gid: &str) {
        self.call("nibiru.pause", vec![json!(gid)]).await;
        self.wait(gid, "paused").await;
        assert!(self.server.flush_session().await.is_ok());
    }
}

fn saved(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

#[tokio::test]
async fn a_paused_http_task_survives_a_restart_and_resumes_to_identical_bytes() {
    let body = sample(900_000, 3);
    let port = origin(body.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let session = dir.path().join("session.json");
    let first = node(Some(&session), None).await;
    let gid = first
        .call(
            "nibiru.addUri",
            vec![
                json!([format!("http://127.0.0.1:{port}/movie.bin")]),
                json!({"dir": dir.path(), "out": "movie.bin", "max-download-limit": "40K",
                       "referer": "https://site.example/page"}),
            ],
        )
        .await;
    let gid = gid.as_str().unwrap().to_owned();
    first.wait_progress(&gid).await;
    first.pause_and_save(&gid).await;
    assert!(saved(&session).contains(&gid));
    // Paused means paused: nothing may keep writing the file behind the restore.
    let length = || std::fs::metadata(dir.path().join("movie.bin")).map_or(0, |m| m.len());
    let at_pause = length();
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(length(), at_pause, "a paused download kept writing");
    assert!(
        at_pause < 900_000,
        "the transfer finished before it could be paused"
    );

    // "Restart": a brand new server reads the same session file.
    let second = node(Some(&session), None).await;
    let info = second.call("nibiru.getSessionInfo", vec![]).await;
    assert_eq!(info["persistent"], true);
    assert_eq!(info["restored"], 1);
    assert_eq!(info["skipped"], 0);
    let restored = second.status(&gid).await;
    assert_eq!(
        restored["status"], "paused",
        "restored tasks never start by themselves"
    );
    assert_eq!(
        restored["files"][0]["path"],
        dir.path().join("movie.bin").to_string_lossy().as_ref()
    );

    second
        .call(
            "nibiru.changeOption",
            vec![json!(gid), json!({"max-download-limit": "0"})],
        )
        .await;
    second.call("nibiru.resume", vec![json!(gid)]).await;
    second.wait(&gid, "complete").await;
    assert_eq!(std::fs::read(dir.path().join("movie.bin")).unwrap(), body);
    second.server.flush_session().await.unwrap();
    assert!(
        !saved(&session).contains(&gid),
        "a finished task leaves the session"
    );
}

#[tokio::test]
async fn credentials_never_reach_the_session_file_and_the_file_is_private() {
    let port = origin(sample(300_000, 4)).await;
    let dir = tempfile::tempdir().unwrap();
    let session = dir.path().join("session.json");
    let node = node(Some(&session), None).await;
    let url = format!("http://127.0.0.1:{port}/a.bin");
    let plain = node
        .call(
            "nibiru.addUri",
            vec![
                json!([url]),
                json!({"dir": dir.path(), "out": "plain.bin", "max-download-limit": "20K"}),
            ],
        )
        .await;
    let secret = node
        .call(
            "nibiru.addUri",
            vec![
                json!([url]),
                json!({"dir": dir.path(), "out": "secret.bin", "max-download-limit": "20K",
                       "header": ["Cookie: sid=TOPSECRET"]}),
            ],
        )
        .await;
    let (plain, secret) = (plain.as_str().unwrap(), secret.as_str().unwrap());
    node.call("nibiru.pause", vec![json!(plain)]).await;
    node.call("nibiru.pause", vec![json!(secret)]).await;
    node.wait(plain, "paused").await;
    node.wait(secret, "paused").await;
    node.server.flush_session().await.unwrap();
    let text = saved(&session);
    assert!(text.contains(plain));
    assert!(!text.contains(secret), "a task with a cookie must stay out");
    assert!(!text.contains("TOPSECRET") && !text.contains("sid="));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&session).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    // After a restart the credentialed task is unknown, so the manager recreates it.
    let after = node_after(&session).await;
    assert!(
        after
            .try_call("nibiru.tellStatus", vec![json!(secret)])
            .await
            .is_err()
    );
    assert_eq!(after.status(plain).await["status"], "paused");
}

async fn node_after(session: &Path) -> Node {
    node(Some(session), None).await
}

#[tokio::test]
async fn a_corrupt_session_file_is_set_aside_and_the_service_still_starts() {
    let dir = tempfile::tempdir().unwrap();
    let session = dir.path().join("session.json");
    std::fs::write(&session, b"\x00 not a session").unwrap();
    let node = node(Some(&session), None).await;
    let info = node.call("nibiru.getSessionInfo", vec![]).await;
    assert_eq!(
        (info["persistent"].clone(), info["restored"].clone()),
        (json!(true), json!(0))
    );
    let moved = std::fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .any(|entry| entry.file_name().to_string_lossy().contains(".corrupt-"));
    assert!(
        moved,
        "the unreadable file must be preserved, not overwritten"
    );
}

#[tokio::test]
async fn without_a_session_file_nothing_is_written_and_info_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let node = node(None, None).await;
    let info = node.call("nibiru.getSessionInfo", vec![]).await;
    assert_eq!(info["persistent"], false);
    assert!(!node.server.flush_session().await.unwrap());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn global_stat_counts_waiting_stopped_and_a_cumulative_total() {
    let port = origin(sample(200_000, 5)).await;
    let dir = tempfile::tempdir().unwrap();
    let node = node(None, None).await;
    let url = format!("http://127.0.0.1:{port}/s.bin");
    let done = node
        .call(
            "nibiru.addUri",
            vec![json!([url]), json!({"dir": dir.path(), "out": "done.bin"})],
        )
        .await;
    let done = done.as_str().unwrap();
    node.wait(done, "complete").await;
    let slow = node
        .call(
            "nibiru.addUri",
            vec![
                json!([url]),
                json!({"dir": dir.path(), "out": "slow.bin", "max-download-limit": "10K"}),
            ],
        )
        .await;
    let slow = slow.as_str().unwrap();
    node.call("nibiru.pause", vec![json!(slow)]).await;
    node.wait(slow, "paused").await;
    let stat = node.call("nibiru.getGlobalStat", vec![]).await;
    assert_eq!(
        (stat["numWaiting"].clone(), stat["numStopped"].clone()),
        (json!("1"), json!("1"))
    );
    assert_eq!(stat["numStoppedTotal"], "1");

    node.call("nibiru.removeResult", vec![json!(done)]).await;
    let stat = node.call("nibiru.getGlobalStat", vec![]).await;
    assert_eq!(stat["numStopped"], "0");
    assert_eq!(
        stat["numStoppedTotal"], "1",
        "purged results still count in the total"
    );
}

/// A real seeder and tracker so a restored torrent has someone to download from.
async fn tracker(peer: SocketAddr) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://127.0.0.1:{}/announce",
        listener.local_addr().unwrap().port()
    );
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut buffer = [0u8; 4096];
            let _ = stream.read(&mut buffer).await;
            let SocketAddr::V4(v4) = peer else { return };
            let mut peers = v4.ip().octets().to_vec();
            peers.extend(v4.port().to_be_bytes());
            let mut body = format!("d8:intervali60e5:peers{}:", peers.len()).into_bytes();
            body.extend(peers);
            body.push(b'e');
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(head.as_bytes()).await;
            let _ = stream.write_all(&body).await;
        }
    });
    url
}

async fn bt_service() -> Arc<BtService> {
    BtService::start(BtConfig {
        session: SessionConfig {
            bind: "127.0.0.1".parse().unwrap(),
            allow_private_trackers: true,
            ..SessionConfig::default()
        },
        seed_time: Some(Duration::ZERO),
        seed_ratio: None,
        allow_loopback_peers: true,
        dht: false,
        ..BtConfig::default()
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn a_paused_torrent_survives_a_restart_and_downloads_verified_data() {
    let data = sample(900_000, 21);
    let info = build_info("Show", 65_536, &[(&["movie.mkv"], &data)]);
    let seed = Session::start(SessionConfig {
        bind: "127.0.0.1".parse().unwrap(),
        ..SessionConfig::default()
    })
    .await
    .unwrap();
    let announce = tracker(SocketAddr::from(([127, 0, 0, 1], seed.port()))).await;
    let torrent = Torrent::from_info(
        &info,
        Extras {
            trackers: vec![vec![announce]],
            web_seeds: vec![],
        },
    )
    .unwrap();
    let seed_dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(seed_dir.path().join("Show")).unwrap();
    std::fs::write(seed_dir.path().join("Show/movie.mkv"), &data).unwrap();
    let mut settings = Settings::new(seed_dir.path());
    settings.allow_loopback_peers = true;
    settings.seed_ratio = None;
    seed.add_torrent(torrent.clone(), settings)
        .wait_complete(Duration::from_secs(20))
        .await
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let session = dir.path().join("session.json");
    let first = node(Some(&session), Some(bt_service().await)).await;
    let encoded = base64::engine::general_purpose::STANDARD.encode(torrent.to_torrent_file());
    let gid = first
        .call(
            "nibiru.addTorrent",
            vec![
                json!(encoded),
                json!([]),
                json!({"dir": dir.path(), "max-download-limit": "60K"}),
            ],
        )
        .await;
    let gid = gid.as_str().unwrap().to_owned();
    first.wait_progress(&gid).await;
    first.pause_and_save(&gid).await;
    assert!(saved(&session).contains(&gid));

    let second = node(Some(&session), Some(bt_service().await)).await;
    let restored = second.status(&gid).await;
    assert_eq!(restored["status"], "paused");
    assert_eq!(restored["infoHash"], first.status(&gid).await["infoHash"]);
    assert_eq!(
        second.call("nibiru.getSessionInfo", vec![]).await["restored"],
        1
    );
    second
        .call(
            "nibiru.changeOption",
            vec![json!(gid), json!({"max-download-limit": "0"})],
        )
        .await;
    second.call("nibiru.resume", vec![json!(gid)]).await;
    second.wait(&gid, "complete").await;
    assert_eq!(
        std::fs::read(dir.path().join("Show/movie.mkv")).unwrap(),
        data
    );
}

#[tokio::test]
async fn the_real_binary_recovers_active_tasks_after_being_killed() {
    use std::process::{Command, Stdio};
    let body = sample(600_000, 31);
    let port = origin(body.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let session = dir.path().join("session.json");
    let rpc_port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let spawn = || {
        Command::new(env!("CARGO_BIN_EXE_nibiru-xtreme"))
            .args([
                "serve",
                "--secret",
                "k",
                "--enable-dht",
                "false",
                "--enable-dht6",
                "false",
            ])
            .arg(format!("--port={rpc_port}"))
            .arg(format!("--dir={}", dir.path().display()))
            .arg(format!("--session-file={}", session.display()))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    };
    let client = Node {
        port: rpc_port,
        server: RpcServer::new(
            Engine::new(Arc::new(LogSink::new(None).unwrap())).unwrap(),
            Arc::new(LogSink::new(None).unwrap()),
            "k".into(),
        ),
        client: reqwest::Client::new(),
    };
    let wait_up = || async {
        for _ in 0..100 {
            if client
                .try_call("nibiru.getVersion", vec![])
                .await_ok()
                .await
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("the engine never started");
    };

    let mut first = spawn();
    wait_up().await;
    let gid = client
        .call(
            "nibiru.addUri",
            vec![
                json!([format!("http://127.0.0.1:{port}/crash.bin")]),
                json!({"dir": dir.path(), "out": "crash.bin", "max-download-limit": "100K"}),
            ],
        )
        .await;
    let gid = gid.as_str().unwrap().to_owned();
    client.wait_progress(&gid).await;
    // The session is written shortly after the task exists; then the process dies abruptly.
    for _ in 0..100 {
        if saved(&session).contains(&gid) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(saved(&session).contains(&gid), "the task was never written");
    first.kill().unwrap();
    first.wait().unwrap();

    let mut second = spawn();
    wait_up().await;
    let restored = client.status(&gid).await;
    assert_eq!(restored["status"], "paused");
    client
        .call(
            "nibiru.changeOption",
            vec![json!(gid), json!({"max-download-limit": "0"})],
        )
        .await;
    client.call("nibiru.resume", vec![json!(gid)]).await;
    client.wait(&gid, "complete").await;
    assert_eq!(std::fs::read(dir.path().join("crash.bin")).unwrap(), body);
    second.kill().unwrap();
    second.wait().unwrap();
}

trait AwaitOk {
    async fn await_ok(self) -> bool;
}

impl<F: std::future::Future<Output = Result<Value, String>>> AwaitOk for F {
    async fn await_ok(self) -> bool {
        self.await.is_ok()
    }
}
