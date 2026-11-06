//! Avoid extra HTTP round trips when a download will not be split into ranges.

use nibiru_xtreme::{
    engine::{DownloadRequest, Engine, RequestOptions},
    logging::LogSink,
};
use std::sync::{Arc, Mutex, atomic::AtomicBool};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::test]
async fn small_downloads_skip_range_tests_but_explicit_probe_checks_support() {
    let body = Arc::new((0..131_072).map(|i| (i % 251) as u8).collect::<Vec<_>>());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let requests = seen.clone();
    let data = body.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let requests = requests.clone();
            let data = data.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                loop {
                    let mut buffer = [0; 4096];
                    let n = stream.read(&mut buffer).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    request.extend_from_slice(&buffer[..n]);
                    if request.windows(4).any(|s| s == b"\r\n\r\n") {
                        break;
                    }
                }
                let request = String::from_utf8(request).unwrap().to_ascii_lowercase();
                let head = request.starts_with("head ");
                let range = request.lines().any(|s| s == "range: bytes=0-0");
                requests.lock().unwrap().push(if head {
                    "HEAD"
                } else if range {
                    "GET range"
                } else {
                    "GET"
                });
                let status = if range {
                    "206 Partial Content"
                } else {
                    "200 OK"
                };
                let length = if range { 1 } else { data.len() };
                let range_header = if range {
                    format!("Content-Range: bytes 0-0/{}\r\n", data.len())
                } else {
                    String::new()
                };
                stream.write_all(format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {length}\r\nAccept-Ranges: bytes\r\n{range_header}Connection: close\r\n\r\n"
                ).as_bytes()).await.unwrap();
                if !head {
                    let _ = stream.write_all(&data[..length]).await;
                }
            });
        }
    });
    let engine = Engine::new(Arc::new(LogSink::new(None).unwrap())).unwrap();
    let url = format!("http://{address}/small.bin");
    assert_eq!(
        engine.probe(&url).await.unwrap(),
        (Some(body.len() as u64), true)
    );
    assert_eq!(*seen.lock().unwrap(), ["HEAD", "GET range"]);

    let folder = tempfile::tempdir().unwrap();
    for (name, options, expected_requests) in [
        ("auto.bin", RequestOptions::default(), vec!["HEAD", "GET"]),
        (
            "single.bin",
            RequestOptions {
                max_connections: Some(1),
                ..Default::default()
            },
            vec!["GET"],
        ),
    ] {
        seen.lock().unwrap().clear();
        let destination = folder.path().join(name);
        engine
            .clone()
            .with_request_options(options)
            .unwrap()
            .download(
                DownloadRequest {
                    url: url.clone(),
                    destination: destination.clone(),
                },
                Arc::new(AtomicBool::new(false)),
                |_| {},
            )
            .await
            .unwrap();
        assert_eq!(*seen.lock().unwrap(), expected_requests);
        assert_eq!(std::fs::read(destination).unwrap(), *body);
    }
    server.abort();
}
