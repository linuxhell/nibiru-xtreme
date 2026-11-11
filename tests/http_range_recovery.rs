use nibiru_xtreme::{
    engine::{DownloadRequest, Engine, RequestOptions},
    logging::LogSink,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
async fn recovery(stall: bool, always: bool) {
    const B: usize = 4 << 20;
    const SIZE: usize = 64 << 20;
    let count = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/file", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let count = count.clone();
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                while !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                    let mut buf = [0; 4096];
                    let n = stream.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&buf[..n]);
                }
                let request = String::from_utf8(bytes).unwrap().to_ascii_lowercase();
                assert!(request.starts_with("get "));
                let (a, b) = request
                    .lines()
                    .find_map(|l| l.strip_prefix("range: bytes="))
                    .expect("no whole-file fallback")
                    .split_once('-')
                    .unwrap();
                let (start, end) = (a.parse::<usize>().unwrap(), b.parse::<usize>().unwrap());
                let eligible = if stall { start == 0 } else { start == B };
                let fault = eligible && (always || count.fetch_add(1, Ordering::SeqCst) == 0);
                let _=stream.write_all(format!("HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{SIZE}\r\nETag: \"stable\"\r\nConnection: close\r\n\r\n",end-start+1).as_bytes()).await;
                let mut remaining = if fault {
                    if stall { 17 } else { B + 17 }
                } else {
                    end - start + 1
                };
                let payload = [0x6b; 65536];
                while remaining > 0 {
                    let n = remaining.min(payload.len());
                    if stream.write_all(&payload[..n]).await.is_err() {
                        return;
                    }
                    remaining -= n;
                }
                if fault && stall {
                    let mut sink = [0; 1];
                    let _ = stream.read(&mut sink).await;
                }
            });
        }
    });
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("file");
    let log = root.path().join("events");
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        Engine::new(Arc::new(LogSink::new(Some(&log)).unwrap()))
            .unwrap()
            .with_read_idle_timeout(Duration::from_secs(1))
            .unwrap()
            .with_request_options(RequestOptions {
                max_connections: Some(2),
                ..Default::default()
            })
            .unwrap()
            .download(
                DownloadRequest {
                    url,
                    destination: destination.clone(),
                },
                Arc::new(AtomicBool::new(false)),
                |_| {},
            ),
    )
    .await
    .unwrap();
    server.abort();
    let log = std::fs::read_to_string(log).unwrap();
    if always {
        assert!(result.is_err());
        assert!(!destination.exists());
        assert!(!log.contains("http.segmented_fallback"), "{log}");
    } else {
        result.unwrap();
        let data = std::fs::read(destination).unwrap();
        assert_eq!(data.len(), SIZE);
        assert!(data.iter().all(|b| *b == 0x6b));
        assert!(log.contains("http.range_body_retry"), "{log}");
        if !stall {
            assert!(log.contains("committed_offset=8388608"), "{log}");
        }
        assert!(!log.contains("http.segmented_fallback"), "{log}");
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_initial_body_recovers_without_losing_other_ranges() {
    recovery(true, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncated_body_retries_only_unwritten_blocks() {
    recovery(false, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn permanently_stalled_body_preserves_verified_partial() {
    recovery(true, true).await;
}
