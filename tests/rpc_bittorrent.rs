//! The aria2-compatible JSON-RPC surface the Apocalipse Download Manager uses for torrents,
//! exercised over real HTTP against real BitTorrent peers on loopback.

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
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

struct World {
    port: u16,
    client: reqwest::Client,
    torrent: Torrent,
    files: Vec<(Vec<&'static str>, Vec<u8>)>,
    _seed: Arc<Session>,
    _seed_dir: tempfile::TempDir,
}

/// A minimal HTTP tracker that always answers with one peer: the seeder.
async fn tracker(peer: SocketAddr) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
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

async fn world() -> World {
    let files = vec![
        (vec!["ep1.mkv"], sample(600_000, 11)),
        (vec!["ep2.mkv"], sample(450_000, 12)),
        (vec!["subs.srt"], sample(5_000, 13)),
    ];
    let spec: Vec<(&[&str], &[u8])> = files
        .iter()
        .map(|(p, d)| (p.as_slice(), d.as_slice()))
        .collect();
    let info = build_info("Show", 65_536, &spec);

    let seed = Session::start(SessionConfig {
        bind: "127.0.0.1".parse().unwrap(),
        ..SessionConfig::default()
    })
    .await
    .unwrap();
    let seed_addr = SocketAddr::from(([127, 0, 0, 1], seed.port()));
    let announce = tracker(seed_addr).await;
    let torrent = Torrent::from_info(
        &info,
        Extras {
            trackers: vec![vec![announce]],
            web_seeds: vec![],
        },
    )
    .unwrap();

    let seed_dir = tempfile::tempdir().unwrap();
    for (path, data) in &files {
        let full = path
            .iter()
            .fold(seed_dir.path().join("Show"), |p, c| p.join(c));
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(full, data).unwrap();
    }
    let mut settings = Settings::new(seed_dir.path());
    settings.allow_loopback_peers = true;
    settings.seed_ratio = None;
    let handle = seed.add_torrent(torrent.clone(), settings);
    handle.wait_complete(Duration::from_secs(20)).await.unwrap();

    let log = Arc::new(LogSink::new(None).unwrap());
    let bt = BtService::start(BtConfig {
        session: SessionConfig {
            bind: "127.0.0.1".parse().unwrap(),
            allow_private_trackers: true,
            ..SessionConfig::default()
        },
        seed_time: Some(Duration::ZERO), // the ADM runs with --seed-time=0
        seed_ratio: None,
        allow_loopback_peers: true,
        dht: false,
        ..BtConfig::default()
    })
    .await
    .unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let server =
        RpcServer::new(Engine::new(log.clone()).unwrap(), log, "s3cret".into()).with_bittorrent(bt);
    tokio::spawn(async move { server.serve(port).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    World {
        port,
        client: reqwest::Client::new(),
        torrent,
        files,
        _seed: seed,
        _seed_dir: seed_dir,
    }
}

impl World {
    async fn call(&self, method: &str, params: Vec<Value>) -> Result<Value, String> {
        self.call_with("s3cret", method, params).await
    }

    async fn call_with(
        &self,
        secret: &str,
        method: &str,
        params: Vec<Value>,
    ) -> Result<Value, String> {
        let mut all = vec![json!(format!("token:{secret}"))];
        all.extend(params);
        let response: Value = self
            .client
            .post(format!("http://127.0.0.1:{}/jsonrpc", self.port))
            .json(&json!({"jsonrpc":"2.0","id":"t","method":method,"params":all}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        match response.get("error") {
            Some(e) => Err(e["message"].as_str().unwrap_or("error").to_owned()),
            None => Ok(response["result"].clone()),
        }
    }

    async fn status(&self, gid: &str) -> Value {
        self.call("nibiru.tellStatus", vec![json!(gid)])
            .await
            .unwrap()
    }

    async fn wait_status(&self, gid: &str, wanted: &str) -> Value {
        for _ in 0..300 {
            let s = self.status(gid).await;
            if s["status"] == wanted {
                return s;
            }
            assert_ne!(s["status"], "error", "{s}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("never reached {wanted}: {}", self.status(gid).await);
    }

    fn magnet(&self) -> String {
        format!(
            "magnet:?xt=urn:btih:{}&dn=Show&tr={}",
            hex(&self.torrent.info_hash),
            self.torrent.trackers[0][0]
        )
    }
}

#[tokio::test]
async fn add_torrent_with_one_based_select_file_downloads_only_those_files() {
    let w = world().await;
    let dir = tempfile::tempdir().unwrap();
    let encoded = base64::engine::general_purpose::STANDARD.encode(w.torrent.to_torrent_file());
    let gid = w
        .call(
            "nibiru.addTorrent",
            vec![
                json!(encoded),
                json!([]),
                json!({"dir": dir.path(), "select-file": "2-3", "continue": "true", "file-allocation": "none",
                       "bt-prioritize-piece": "head,tail"}),
            ],
        )
        .await
        .unwrap();
    let gid = gid.as_str().unwrap().to_owned();
    assert_eq!(gid.len(), 16, "aria2-style gid");
    let done = w.wait_status(&gid, "complete").await;

    assert_eq!(done["totalLength"], "455000");
    assert_eq!(done["completedLength"], "455000");
    assert_eq!(done["infoHash"], hex(&w.torrent.info_hash));
    assert_eq!(done["numPieces"], w.torrent.piece_count().to_string());
    assert_eq!(done["pieceLength"], "65536");
    assert_eq!(done["errorCode"], "0");
    assert_eq!(done["bittorrent"]["info"]["name"], "Show");
    assert_eq!(done["bittorrent"]["mode"], "multi");
    let files = done["files"].as_array().unwrap();
    assert_eq!(files.len(), 3);
    assert_eq!(files[0]["index"], "1");
    assert_eq!(files[0]["selected"], "false");
    assert_eq!(files[1]["selected"], "true");
    assert_eq!(files[1]["length"], "450000");
    assert!(
        files[1]["path"]
            .as_str()
            .unwrap()
            .replace('\\', "/")
            .ends_with("Show/ep2.mkv"),
        "{}",
        files[1]["path"]
    );
    assert_eq!(
        std::fs::read(dir.path().join("Show/ep2.mkv")).unwrap(),
        w.files[1].1
    );
    assert_eq!(
        std::fs::read(dir.path().join("Show/subs.srt")).unwrap(),
        w.files[2].1
    );

    // keys filter
    let few = w
        .call(
            "nibiru.tellStatus",
            vec![json!(gid), json!(["status", "totalLength"])],
        )
        .await
        .unwrap();
    assert_eq!(few.as_object().unwrap().len(), 3, "{few}"); // + gid
    // The ADM then cleans up.
    w.call("nibiru.remove", vec![json!(gid)]).await.unwrap();
    assert_eq!(w.status(&gid).await["status"], "removed");
    w.call("nibiru.removeResult", vec![json!(gid)])
        .await
        .unwrap();
    assert!(w.call("nibiru.tellStatus", vec![json!(gid)]).await.is_err());
}

#[tokio::test]
async fn magnet_metadata_only_saves_the_torrent_then_the_real_download_follows() {
    let w = world().await;
    let torrents = tempfile::tempdir().unwrap();
    let gid = w
        .call(
            "nibiru.addUri",
            vec![
                json!([w.magnet()]),
                json!({"dir": torrents.path(), "bt-metadata-only": "true", "bt-save-metadata": "true",
                       "file-allocation": "none"}),
            ],
        )
        .await
        .unwrap();
    let gid = gid.as_str().unwrap().to_owned();
    let done = w.wait_status(&gid, "complete").await;
    let hash = hex(&w.torrent.info_hash);
    assert_eq!(done["infoHash"], hash);
    assert_eq!(
        done["completedLength"], done["totalLength"],
        "metadata progress is reported as bytes"
    );
    let saved = torrents.path().join(format!("{hash}.torrent"));
    let parsed = Torrent::from_torrent_file(&std::fs::read(&saved).unwrap()).unwrap();
    assert_eq!(parsed.info_hash, w.torrent.info_hash);
    assert_eq!(parsed.files.len(), 3);
    assert_eq!(
        parsed.trackers, w.torrent.trackers,
        "the magnet's tracker is kept in the saved file"
    );
    // Metadata-only never creates payload files.
    assert!(!torrents.path().join("Show").exists());
    w.call("nibiru.removeResult", vec![json!(gid)])
        .await
        .unwrap();

    // ADM step two: parse the saved file, ask the user, start the real download.
    let out = tempfile::tempdir().unwrap();
    let encoded = base64::engine::general_purpose::STANDARD.encode(std::fs::read(&saved).unwrap());
    let gid = w
        .call(
            "nibiru.addTorrent",
            vec![
                json!(encoded),
                json!([]),
                json!({"dir": out.path(), "select-file": "1"}),
            ],
        )
        .await
        .unwrap();
    let done = w.wait_status(gid.as_str().unwrap(), "complete").await;
    assert_eq!(done["totalLength"], "600000");
    assert_eq!(
        std::fs::read(out.path().join("Show/ep1.mkv")).unwrap(),
        w.files[0].1
    );
}

#[tokio::test]
async fn pause_unpause_limits_and_global_calls() {
    let w = world().await;
    let dir = tempfile::tempdir().unwrap();
    let encoded = base64::engine::general_purpose::STANDARD.encode(w.torrent.to_torrent_file());
    let gid = w
        .call(
            "nibiru.addTorrent",
            vec![
                json!(encoded),
                json!([]),
                json!({"dir": dir.path(), "max-download-limit": "40K"}),
            ],
        )
        .await
        .unwrap();
    let gid = gid.as_str().unwrap().to_owned();
    let stat = w.call("nibiru.getGlobalStat", vec![]).await.unwrap();
    assert_eq!(stat["numActive"], "1");
    w.call("nibiru.pause", vec![json!(gid)]).await.unwrap();
    assert_eq!(w.status(&gid).await["status"], "paused");
    w.call(
        "nibiru.changeOption",
        vec![json!(gid), json!({"max-download-limit": "0"})],
    )
    .await
    .unwrap();
    w.call(
        "nibiru.changeGlobalOption",
        vec![json!({"max-overall-download-limit": "0"})],
    )
    .await
    .unwrap();
    w.call("nibiru.resume", vec![json!(gid)]).await.unwrap();
    w.wait_status(&gid, "complete").await;
    let peers = w.call("nibiru.getPeers", vec![json!(gid)]).await.unwrap();
    assert!(peers.is_array());
    let version = w.call("nibiru.getVersion", vec![]).await.unwrap();
    let features = version["enabledFeatures"].to_string();
    assert!(
        features.contains("BitTorrent") && features.contains("Magnet"),
        "{features}"
    );
}

#[tokio::test]
async fn stream_url_lets_a_player_watch_the_biggest_file_while_it_downloads() {
    let w = world().await;
    let dir = tempfile::tempdir().unwrap();
    let encoded = base64::engine::general_purpose::STANDARD.encode(w.torrent.to_torrent_file());
    // Nothing selected: the download only starts because somebody presses play.
    let gid = w
        .call(
            "nibiru.addTorrent",
            vec![
                json!(encoded),
                json!([]),
                json!({"dir": dir.path(), "select-file": "3"}),
            ],
        )
        .await
        .unwrap();
    let gid = gid.as_str().unwrap().to_owned();
    let url = w.call("nibiru.streamUrl", vec![json!(gid)]).await.unwrap();
    let url = url.as_str().unwrap().to_owned();
    assert!(
        url.ends_with("/0"),
        "the biggest file (ep1.mkv) is the default: {url}"
    );
    let response = w
        .client
        .get(&url)
        .header("Range", "bytes=300000-399999")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 206);
    assert_eq!(response.headers()["content-type"], "video/x-matroska");
    assert_eq!(
        response.bytes().await.unwrap().as_ref(),
        &w.files[0].1[300_000..400_000]
    );
    // Explicit file index.
    let url2 = w
        .call("nibiru.streamUrl", vec![json!(gid), json!(1)])
        .await
        .unwrap();
    let body = w
        .client
        .get(url2.as_str().unwrap())
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(body.as_ref(), w.files[1].1.as_slice());
    let status = w.status(&gid).await;
    assert_eq!(
        status["files"][0]["selected"], "true",
        "watching selects the file"
    );
}

#[tokio::test]
async fn rpc_rejects_bad_tokens_bad_torrents_and_bad_selections() {
    let w = world().await;
    assert!(
        w.call_with("wrong", "nibiru.getVersion", vec![])
            .await
            .is_err()
    );
    assert!(
        w.call(
            "nibiru.addTorrent",
            vec![json!("not base64!!"), json!([]), json!({})]
        )
        .await
        .is_err()
    );
    let junk = base64::engine::general_purpose::STANDARD.encode(b"d4:spami1ee");
    assert!(
        w.call("nibiru.addTorrent", vec![json!(junk), json!([]), json!({})])
            .await
            .is_err()
    );
    let dir = tempfile::tempdir().unwrap();
    let encoded = base64::engine::general_purpose::STANDARD.encode(w.torrent.to_torrent_file());
    for bad in ["0", "4", "3-1", "x"] {
        let r = w
            .call(
                "nibiru.addTorrent",
                vec![
                    json!(encoded),
                    json!([]),
                    json!({"dir": dir.path(), "select-file": bad}),
                ],
            )
            .await;
        assert!(r.is_err(), "select-file={bad}");
    }
    assert!(
        w.call(
            "nibiru.addUri",
            vec![json!(["magnet:?xt=urn:btih:zzz"]), json!({})]
        )
        .await
        .is_err()
    );
    assert!(
        w.call("nibiru.tellStatus", vec![json!("ffffffffffffffff")])
            .await
            .is_err()
    );
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
