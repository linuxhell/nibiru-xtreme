//! A refused range must not poison identity or discard other healthy workers.
use nibiru_xtreme::{
    engine::{DownloadRequest, Engine},
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
    sync::{Notify, Semaphore},
};

#[derive(Clone, Copy)]
enum Mode {
    TwoConnections,
    AlwaysRefused(&'static str, u16),
}

struct Fixture {
    url: String,
    refused: Arc<AtomicUsize>,
    plain_gets: Arc<AtomicUsize>,
    first_refusal: Arc<Notify>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    async fn start(mode: Mode) -> Self {
        const SIZE: usize = 64 * 1024 * 1024;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/file", listener.local_addr().unwrap());
        let refused = Arc::new(AtomicUsize::new(0));
        let plain_gets = Arc::new(AtomicUsize::new(0));
        let first_refusal = Arc::new(Notify::new());
        let server = {
            let (refused, plain_gets, first_refusal) =
                (refused.clone(), plain_gets.clone(), first_refusal.clone());
            let slots = Arc::new(Semaphore::new(2));
            tokio::spawn(async move {
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let (refused, plain_gets, first_refusal, slots) = (
                        refused.clone(),
                        plain_gets.clone(),
                        first_refusal.clone(),
                        slots.clone(),
                    );
                    tokio::spawn(async move {
                        let mut request = Vec::new();
                        while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                            let mut bytes = [0; 4096];
                            let n = stream.read(&mut bytes).await.unwrap_or(0);
                            if n == 0 {
                                return;
                            }
                            request.extend_from_slice(&bytes[..n]);
                        }
                        let request = String::from_utf8(request).unwrap().to_ascii_lowercase();
                        if request.starts_with("head ") {
                            let _ = stream.write_all(format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {SIZE}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n"
                            ).as_bytes()).await;
                            return;
                        }
                        let range = request
                            .lines()
                            .find_map(|l| l.strip_prefix("range: bytes="));
                        if range.is_none() {
                            plain_gets.fetch_add(1, Ordering::SeqCst);
                        }
                        let permit = slots.try_acquire_owned();
                        let retry_after = match mode {
                            Mode::AlwaysRefused(value, _) => Some(value),
                            Mode::TwoConnections if permit.is_err() => Some("1"),
                            _ => None,
                        };
                        if let Some(retry_after) = retry_after {
                            refused.fetch_add(1, Ordering::SeqCst);
                            let status = match mode {
                                Mode::AlwaysRefused(_, code) => code,
                                _ => 503,
                            };
                            let _ = stream.write_all(format!(
                                "HTTP/1.1 {status} Refused\r\nContent-Length: 0\r\nRetry-After: {retry_after}\r\nConnection: close\r\n\r\n"
                            ).as_bytes()).await;
                            first_refusal.notify_one();
                            return;
                        }
                        let _permit = permit.unwrap();
                        let (start, end) = range.unwrap().split_once('-').unwrap();
                        let (start, end) = (
                            start.parse::<usize>().unwrap(),
                            end.parse::<usize>().unwrap(),
                        );
                        let _ = stream.write_all(format!(
                            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{SIZE}\r\nETag: \"stable\"\r\nConnection: close\r\n\r\n", end-start+1
                        ).as_bytes()).await;
                        let bytes = [0x5A; 64 * 1024];
                        let mut remaining = end - start + 1;
                        while remaining > 0 {
                            let n = remaining.min(bytes.len());
                            if stream.write_all(&bytes[..n]).await.is_err() {
                                return;
                            }
                            remaining -= n;
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                    });
                }
            })
        };
        Self {
            url,
            refused,
            plain_gets,
            first_refusal,
            server,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_ranges_keep_healthy_workers_and_publish_exact_bytes() {
    let fixture = Fixture::start(Mode::TwoConnections).await;
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("quota.bin");
    let log_path = root.path().join("events.jsonl");
    let engine = Engine::new(Arc::new(LogSink::new(Some(&log_path)).unwrap())).unwrap();
    tokio::time::timeout(
        Duration::from_secs(30),
        engine.download(
            DownloadRequest {
                url: fixture.url.clone(),
                destination: destination.clone(),
            },
            Arc::new(AtomicBool::new(false)),
            |_| {},
        ),
    )
    .await
    .expect("bounded quota recovery")
    .expect("503 must not discard valid workers");
    assert!(fixture.refused.load(Ordering::SeqCst) > 0);
    assert_eq!(fixture.plain_gets.load(Ordering::SeqCst), 0);
    let data = std::fs::read(destination).unwrap();
    assert_eq!(data.len(), 64 * 1024 * 1024);
    assert!(data.iter().all(|b| *b == 0x5A));
    let log = std::fs::read_to_string(log_path).unwrap();
    assert!(log.contains("http.range_retry"));
    assert!(!log.contains("http.segmented_fallback"));
}

#[tokio::test]
async fn exhausted_throttle_retries_never_bypass_the_limit_with_a_plain_get() {
    for code in [429, 503] {
        let fixture = Fixture::start(Mode::AlwaysRefused("0", code)).await;
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("blocked.bin");
        let engine = Engine::new(Arc::new(LogSink::new(None).unwrap())).unwrap();
        let error = tokio::time::timeout(
            Duration::from_secs(10),
            engine.download(
                DownloadRequest {
                    url: fixture.url.clone(),
                    destination: destination.clone(),
                },
                Arc::new(AtomicBool::new(false)),
                |_| {},
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(error.to_string().contains(&code.to_string()));
        assert!(fixture.refused.load(Ordering::SeqCst) <= 4 * 6);
        assert_eq!(fixture.plain_gets.load(Ordering::SeqCst), 0);
        assert!(!destination.exists());
    }
}

#[tokio::test]
async fn excessive_retry_after_fails_without_an_immediate_fallback_request() {
    let fixture = Fixture::start(Mode::AlwaysRefused("120", 503)).await;
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::new(Arc::new(LogSink::new(None).unwrap())).unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        engine.download(
            DownloadRequest {
                url: fixture.url.clone(),
                destination: root.path().join("wait.bin"),
            },
            Arc::new(AtomicBool::new(false)),
            |_| {},
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("503"));
    assert_eq!(fixture.plain_gets.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancellation_interrupts_a_retry_after_wait() {
    let fixture = Fixture::start(Mode::AlwaysRefused("20", 503)).await;
    let root = tempfile::tempdir().unwrap();
    let cancel = Arc::new(AtomicBool::new(false));
    let engine = Engine::new(Arc::new(LogSink::new(None).unwrap())).unwrap();
    let flag = cancel.clone();
    let refused = fixture.first_refusal.clone();
    let stopper = tokio::spawn(async move {
        refused.notified().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        flag.store(true, Ordering::Release);
    });
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        engine.download(
            DownloadRequest {
                url: fixture.url.clone(),
                destination: root.path().join("cancel.bin"),
            },
            cancel,
            |_| {},
        ),
    )
    .await
    .expect("cancel must not wait for Retry-After")
    .unwrap_err();
    stopper.abort();
    assert!(error.to_string().contains("cancelled"));
    assert_eq!(fixture.plain_gets.load(Ordering::SeqCst), 0);
}
