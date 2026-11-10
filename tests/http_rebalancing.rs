//! An idle HTTP worker helps a stalled range without overlapping disk writes.
use nibiru_xtreme::{
    engine::{DownloadRequest, Engine},
    logging::LogSink,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_worker_splits_a_busy_tail_and_publishes_exact_bytes() {
    const BLOCK: usize = 4 * 1024 * 1024;
    let data = Arc::new(
        (0..64 * 1024 * 1024 + 17)
            .map(|i| ((i * 31 + 7) % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let first_end = Arc::new(AtomicU64::new(0));
    let helped = Arc::new(AtomicBool::new(false));
    let probes = Arc::new(AtomicU64::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/payload", listener.local_addr().unwrap());
    let server = {
        let (data, first_end, helped, probes) = (
            data.clone(),
            first_end.clone(),
            helped.clone(),
            probes.clone(),
        );
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (data, first_end, helped, probes) = (
                    data.clone(),
                    first_end.clone(),
                    helped.clone(),
                    probes.clone(),
                );
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                        let mut buffer = [0; 4096];
                        let n = stream.read(&mut buffer).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        request.extend_from_slice(&buffer[..n]);
                    }
                    let request = String::from_utf8(request).unwrap().to_ascii_lowercase();
                    if request.starts_with("head ") {
                        let _ = stream.write_all(format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                            data.len()).as_bytes()).await;
                        return;
                    }
                    let (start, end) = request
                        .lines()
                        .find_map(|line| line.strip_prefix("range: bytes="))
                        .unwrap()
                        .split_once('-')
                        .unwrap();
                    let (start, end) = (
                        start.parse::<usize>().unwrap(),
                        end.parse::<usize>().unwrap(),
                    );
                    assert!(end < data.len() && end >= start);
                    if start == 0 && end == 0 {
                        probes.fetch_add(1, Ordering::Relaxed);
                    } else if start == 0 {
                        first_end.store(end as u64, Ordering::Release);
                    } else if (start as u64) < first_end.load(Ordering::Acquire) {
                        // This is inside the first request: it can only be a
                        // dynamic handoff, never an ordinary queued range.
                        assert!(request.contains("if-range: \"stable\""));
                        helped.store(true, Ordering::Release);
                    }
                    if stream.write_all(format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nETag: \"stable\"\r\nConnection: close\r\n\r\n",
                        end-start+1, data.len()).as_bytes()).await.is_err() { return; }
                    for (i, block) in data[start..=end].chunks(BLOCK).enumerate() {
                        // The first worker gets one block, then cannot continue
                        // until an idle worker has actually asked for its tail.
                        while start == 0 && end > BLOCK && i > 0 && !helped.load(Ordering::Acquire)
                        {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        if stream.write_all(block).await.is_err() {
                            return;
                        }
                    }
                });
            }
        })
    };
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("download.bin");
    let log_path = root.path().join("log.jsonl");
    let engine = Engine::new(Arc::new(LogSink::new(Some(&log_path)).unwrap())).unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        engine.download(
            DownloadRequest {
                url,
                destination: destination.clone(),
            },
            Arc::new(AtomicBool::new(false)),
            |_| {},
        ),
    )
    .await;
    server.abort();
    result
        .expect("a busy tail must be handed to an idle worker")
        .unwrap();
    assert!(helped.load(Ordering::Acquire));
    assert_eq!(probes.load(Ordering::Relaxed), 0);
    assert!(
        std::fs::read(&destination).unwrap() == *data,
        "payload differs"
    );
    let log = std::fs::read_to_string(log_path).unwrap();
    assert!(log.contains("http.range_handoffs"));
    assert!(!log.contains("http.segmented_fallback"), "{log}");
    assert!(!root.path().join("download.bin.nibiru-http.part").exists());
}
