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
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/file.bin", listener.local_addr().unwrap());
    let ranges = Arc::new(AtomicUsize::new(0));
    let work = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let changed = changed.clone();
            let ranges = ranges.clone();
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
