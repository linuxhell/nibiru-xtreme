use nibiru_xtreme::{
    engine::{DownloadRequest, Engine},
    logging::LogSink,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn origin(
    size: usize,
    changed: Arc<AtomicBool>,
    drift: bool,
) -> (String, tokio::task::JoinHandle<()>) {
    origin_with_destination(size, changed, drift, None).await
}

async fn origin_with_destination(
    size: usize,
    changed: Arc<AtomicBool>,
    drift: bool,
    foreign: Option<std::path::PathBuf>,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/file.bin", listener.local_addr().unwrap());
    let ranges = Arc::new(AtomicUsize::new(0));
    let work = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let changed = changed.clone();
            let ranges = ranges.clone();
            let foreign = foreign.clone();
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                let mut chunk = [0; 2048];
                while !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = stream.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    bytes.extend(&chunk[..n]);
                    if bytes.len() > 16384 {
                        return;
                    }
                }
                let headers = String::from_utf8(bytes).unwrap();
                let head = headers.starts_with("HEAD ");
                let range = headers.lines().find_map(|line| {
                    line.split_once(':')
                        .filter(|(k, _)| k.eq_ignore_ascii_case("range"))
                        .map(|(_, v)| v.trim().to_owned())
                });
                if drift && range.is_some() && ranges.fetch_add(1, Ordering::Relaxed) >= 2 {
                    changed.store(true, Ordering::Relaxed);
                }
                let second = changed.load(Ordering::Relaxed);
                let etag = if second { "\"b\"" } else { "\"a\"" };
                let valid = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(k, _)| k.eq_ignore_ascii_case("if-range"))
                            .map(|(_, v)| v.trim() == etag)
                    })
                    .unwrap_or(true);
                let (mut start, mut end) = (0, size - 1);
                let mut status = "200 OK";
                let mut content_range = String::new();
                if let Some(range) = range.filter(|_| valid) {
                    let (a, b) = range
                        .strip_prefix("bytes=")
                        .unwrap()
                        .split_once('-')
                        .unwrap();
                    start = a.parse().unwrap();
                    end = if b.is_empty() {
                        size - 1
                    } else {
                        b.parse().unwrap()
                    };
                    if start >= size {
                        let _=stream.write_all(format!("HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\nContent-Range: bytes */{size}\r\nConnection: close\r\n\r\n").as_bytes()).await;
                        return;
                    }
                    status = "206 Partial Content";
                    content_range = format!("Content-Range: bytes {start}-{end}/{size}\r\n");
                }
                let length = if head { size } else { end - start + 1 };
                if !head
                    && length > 1
                    && let Some(path) = foreign
                {
                    tokio::fs::write(path, b"user-created data").await.unwrap();
                }
                if stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: {length}\r\nAccept-Ranges: bytes\r\nETag: {etag}\r\n{content_range}Connection: close\r\n\r\n").as_bytes()).await.is_err(){return;}
                if !head {
                    let body = vec![if second { b'B' } else { b'A' }; length];
                    for chunk in body.chunks(32768) {
                        if stream.write_all(chunk).await.is_err() {
                            break;
                        }
                        if size == 1024 * 1024 {
                            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        }
                    }
                }
            });
        }
    });
    (url, work)
}
fn engine() -> Engine {
    Engine::new(Arc::new(LogSink::new(None).unwrap())).unwrap()
}

#[tokio::test]
async fn rpc_direct_download_uses_the_configured_default_directory() {
    let (url, origin_server) = origin(1000, Arc::new(AtomicBool::new(false)), false).await;
    let dir = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let log = Arc::new(LogSink::new(None).unwrap());
    let rpc = nibiru_xtreme::rpc::RpcServer::new(engine(), log, String::new())
        .with_default_dir(dir.path().to_owned());
    let server = tokio::spawn(async move { rpc.serve(port).await.unwrap() });
    let client = reqwest::Client::new();
    let endpoint = format!("http://127.0.0.1:{port}/jsonrpc");
    let name = format!("audit-default-dir-{}.bin", uuid::Uuid::new_v4());
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if client.post(&endpoint).json(&serde_json::json!({"jsonrpc":"2.0","id":1,"method":"nibiru.getVersion","params":[]}))
                .send().await.is_ok() { break; }
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    let response: serde_json::Value = client.post(&endpoint)
        .json(&serde_json::json!({"jsonrpc":"2.0","id":2,"method":"nibiru.addUri","params":[[url],{"out":name,"split":"1"}]}))
        .send().await.unwrap().json().await.unwrap();
    assert!(response["result"].is_string(), "{response}");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while std::fs::read(dir.path().join(&name)).ok() != Some(vec![b'A'; 1000]) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    server.abort();
    origin_server.abort();
}

#[tokio::test]
async fn direct_transports_cannot_modify_a_destination_leased_by_another_download() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("file.bin");
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(dir.path().join("file.bin.nibiru-http.lock"))
        .unwrap();
    lock.try_lock().unwrap();
    let mut urls = vec!["http://127.0.0.1:9/file"];
    if cfg!(feature = "ftp") {
        urls.push("ftp://127.0.0.1:9/file");
    }
    if cfg!(feature = "sftp") {
        urls.push("sftp://user:password@127.0.0.1:9/file");
    }
    for url in urls {
        let error = engine()
            .download(
                DownloadRequest {
                    url: url.into(),
                    destination: destination.clone(),
                },
                Arc::new(AtomicBool::new(false)),
                |_| {},
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("already in use"),
            "{url}: {error}"
        );
    }
    assert!(!destination.exists());
}

#[tokio::test]
async fn segmented_publication_preserves_a_racing_destination_and_an_old_partial() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("user.bin");
    let old_partial = dir.path().join(".user.bin.nibiru.part");
    tokio::fs::write(&old_partial, b"previous partial")
        .await
        .unwrap();
    let (url, server) = origin_with_destination(
        8 * 1024 * 1024,
        Arc::new(AtomicBool::new(false)),
        false,
        Some(destination.clone()),
    )
    .await;
    let result = engine()
        .download(
            DownloadRequest {
                url,
                destination: destination.clone(),
            },
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .await;
    server.abort();
    assert!(result.is_err());
    assert_eq!(
        tokio::fs::read(destination).await.unwrap(),
        b"user-created data"
    );
    assert_eq!(
        tokio::fs::read(old_partial).await.unwrap(),
        b"previous partial"
    );
    // The finished payload stays in the download's partial file, next to the user's own file.
    let completed = dir.path().join("user.bin.nibiru-http.part");
    assert_eq!(
        std::fs::read(completed)
            .expect("completed download must be kept when publication is refused"),
        vec![b'A'; 8 * 1024 * 1024]
    );
}

#[tokio::test]
async fn failed_cli_requests_do_not_print_url_secrets() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            drop(stream);
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_nibiru-xtreme"))
        .arg("download")
        .arg(format!(
            "http://audit-user:audit-password@{address}/file?token=audit-token#audit-fragment"
        ))
        .arg("--out")
        .arg(dir.path().join("file.bin"))
        .arg("--log")
        .arg(dir.path().join("log.jsonl"))
        .output()
        .await
        .unwrap();
    server.abort();
    assert!(!output.status.success());
    let diagnostic = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stderr),
        std::fs::read_to_string(dir.path().join("log.jsonl")).unwrap()
    );
    for secret in [
        "audit-user",
        "audit-password",
        "audit-token",
        "audit-fragment",
    ] {
        assert!(!diagnostic.contains(secret), "URL secret leaked: {secret}");
    }
}

#[tokio::test]
async fn single_stream_does_not_overwrite_a_file_created_while_waiting_for_headers() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("user.bin");
    let foreign = destination.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/file", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 1024];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let count = stream.read(&mut buffer).await.unwrap();
                if count == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..count]);
            }
            if request.starts_with(b"GET ") {
                tokio::fs::write(&foreign, b"user-created data")
                    .await
                    .unwrap();
                let _ = stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndata",
                    )
                    .await;
                break;
            }
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n")
                .await;
        }
    });
    let result = engine()
        .download(
            DownloadRequest {
                url,
                destination: destination.clone(),
            },
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .await;
    server.await.unwrap();
    assert!(result.is_err(), "the download replaced a foreign file");
    assert_eq!(
        tokio::fs::read(&destination).await.unwrap(),
        b"user-created data"
    );
}

#[tokio::test]
async fn changed_range_identity_cannot_publish_a_mix_of_versions() {
    let changed = Arc::new(AtomicBool::new(false));
    let (url, server) = origin(8 * 1024 * 1024, changed, true).await;
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("file.bin");
    let result = engine()
        .download(
            DownloadRequest {
                url,
                destination: destination.clone(),
            },
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .await;
    if result.is_ok() {
        assert_eq!(
            tokio::fs::read(destination).await.unwrap(),
            vec![b'B'; 8 * 1024 * 1024]
        );
    }
    server.abort();
}
#[tokio::test]
async fn resumed_single_stream_requires_the_saved_validator_and_preserves_existing_bytes() {
    let changed = Arc::new(AtomicBool::new(false));
    let (url, server) = origin(1024 * 1024, changed.clone(), false).await;
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("file.bin");
    let cancelled = Arc::new(AtomicBool::new(false));
    let flag = cancelled.clone();
    let _ = engine()
        .download(
            DownloadRequest {
                url: url.clone(),
                destination: destination.clone(),
            },
            cancelled,
            move |p| {
                if p.downloaded >= 65536 {
                    flag.store(true, Ordering::Release);
                }
            },
        )
        .await;
    let before = tokio::fs::read(&destination).await.unwrap();
    assert!(!before.is_empty());
    changed.store(true, Ordering::Release);
    assert!(
        engine()
            .download(
                DownloadRequest {
                    url: url.clone(),
                    destination: destination.clone()
                },
                Arc::new(AtomicBool::new(false)),
                |_| {}
            )
            .await
            .is_err()
    );
    assert_eq!(tokio::fs::read(&destination).await.unwrap(), before);
    changed.store(false, Ordering::Release);
    engine()
        .download(
            DownloadRequest {
                url,
                destination: destination.clone(),
            },
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .await
        .unwrap();
    assert_eq!(
        tokio::fs::read(destination).await.unwrap(),
        vec![b'A'; 1024 * 1024]
    );
    server.abort();
}
#[tokio::test]
async fn an_existing_user_file_without_a_resume_identity_is_never_modified() {
    let (url, server) = origin(1000, Arc::new(AtomicBool::new(false)), false).await;
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("user.bin");
    tokio::fs::write(&destination, b"preserve this")
        .await
        .unwrap();
    assert!(
        engine()
            .download(
                DownloadRequest {
                    url,
                    destination: destination.clone()
                },
                Arc::new(AtomicBool::new(false)),
                |_| {}
            )
            .await
            .is_err()
    );
    assert_eq!(
        tokio::fs::read(destination).await.unwrap(),
        b"preserve this"
    );
    server.abort();
}
#[tokio::test]
async fn complete_partial_file_accepts_416_only_after_head_identity_and_size_match() {
    let (url, server) = origin(1000, Arc::new(AtomicBool::new(false)), false).await;
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("file.bin");
    tokio::fs::write(&destination, vec![b'A'; 1000])
        .await
        .unwrap();
    tokio::fs::write(dir.path().join("file.bin.nibiru-http.json"),serde_json::to_vec(&serde_json::json!({"origin":url,"final_url":url,"etag":"\"a\"","modified":null,"total":1000})).unwrap()).await.unwrap();
    engine()
        .download(
            DownloadRequest {
                url,
                destination: destination.clone(),
            },
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .await
        .unwrap();
    assert_eq!(
        tokio::fs::read(destination).await.unwrap(),
        vec![b'A'; 1000]
    );
    server.abort();
}

#[tokio::test]
async fn native_rpc_rapid_pause_resume_keeps_one_writer_and_exact_final_counters() {
    use nibiru_xtreme::rpc::RpcServer;
    use serde_json::{Value, json};
    let (url, origin_server) = origin(1024 * 1024, Arc::new(AtomicBool::new(false)), false).await;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let log = Arc::new(LogSink::new(None).unwrap());
    let rpc = RpcServer::new(Engine::new(log.clone()).unwrap(), log, "integration".into());
    let server = tokio::spawn(async move { rpc.serve(port).await.unwrap() });
    let client = reqwest::Client::new();
    let endpoint = format!("http://127.0.0.1:{port}/jsonrpc");
    async fn call(
        client: &reqwest::Client,
        endpoint: &str,
        method: &str,
        params: Vec<Value>,
    ) -> Value {
        let mut all = vec![json!("token:integration")];
        all.extend(params);
        let response: Value = client
            .post(endpoint)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":all}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(response.get("error").is_none(), "{response}");
        response["result"].clone()
    }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("file.bin");
    let gid = call(
        &client,
        &endpoint,
        "nibiru.addUri",
        vec![
            json!([url]),
            json!({"dir":dir.path(),"out":"file.bin","split":"1"}),
        ],
    )
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if std::fs::metadata(&destination).is_ok_and(|m| m.len() > 0) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    for _ in 0..3 {
        call(&client, &endpoint, "nibiru.pause", vec![gid.clone()]).await;
        call(&client, &endpoint, "nibiru.resume", vec![gid.clone()]).await;
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let status = call(&client, &endpoint, "nibiru.tellStatus", vec![gid.clone()]).await;
            assert_ne!(status["status"], "error", "{status}");
            if status["status"] == "complete" {
                assert_eq!(status["completedLength"], (1024 * 1024).to_string());
                assert_eq!(status["totalLength"], (1024 * 1024).to_string());
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(std::fs::read(destination).unwrap(), vec![b'A'; 1024 * 1024]);
    let list = call(
        &client,
        &endpoint,
        "nibiru.tellStopped",
        vec![json!(0), json!(10), json!(["status"])],
    )
    .await;
    assert_eq!(list[0]["status"], "complete");
    assert!(list[0].get("totalLength").is_none());
    call(&client, &endpoint, "nibiru.removeResult", vec![gid]).await;
    assert_eq!(
        call(
            &client,
            &endpoint,
            "nibiru.tellStopped",
            vec![json!(0), json!(10)]
        )
        .await,
        json!([])
    );
    server.abort();
    origin_server.abort();
}
