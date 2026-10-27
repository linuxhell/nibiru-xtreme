//! A segmented download that is paused or killed continues from the blocks already on disk.

use nibiru_xtreme::{
    engine::{DownloadRequest, Engine},
    logging::LogSink,
};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const SIZE: usize = 24 * 1024 * 1024 + 77;

fn payload(seed: u8) -> Arc<Vec<u8>> {
    Arc::new(
        (0..SIZE)
            .map(|i| ((i * 31 + usize::from(seed) * 7 + 17) % 251) as u8)
            .collect(),
    )
}

struct Origin {
    address: std::net::SocketAddr,
    /// Bytes of body served since the last reset (HEAD and 1-byte probes are negligible).
    served: Arc<AtomicU64>,
    hold_tail: Arc<AtomicBool>,
    data: Arc<Mutex<(Arc<Vec<u8>>, String)>>,
    task: tokio::task::JoinHandle<()>,
}

impl Origin {
    async fn start(data: Arc<Vec<u8>>, etag: &str, delay_ms: u64) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let served = Arc::new(AtomicU64::new(0));
        let hold_tail = Arc::new(AtomicBool::new(false));
        let gate_in = hold_tail.clone();
        let current = Arc::new(Mutex::new((data, etag.to_owned())));
        let (served_in, current_in) = (served.clone(), current.clone());
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (served, current) = (served_in.clone(), current_in.clone());
                let hold_tail = gate_in.clone();
                tokio::spawn(async move {
                    let mut request = vec![0u8; 16 * 1024];
                    let used = stream.read(&mut request).await.unwrap_or(0);
                    let text = String::from_utf8_lossy(&request[..used]).to_string();
                    let (data, etag) = current.lock().unwrap().clone();
                    let first = text.lines().next().unwrap_or_default();
                    if first.starts_with("HEAD ") {
                        let _ = stream
                            .write_all(
                                format!(
                                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"{etag}\"\r\nConnection: close\r\n\r\n",
                                    data.len()
                                )
                                .as_bytes(),
                            )
                            .await;
                        return;
                    }
                    let range = text
                        .lines()
                        .find_map(|line| {
                            line.strip_prefix("range: bytes=")
                                .or_else(|| line.strip_prefix("Range: bytes="))
                        })
                        .and_then(|value| value.split_once('-'));
                    let Some((start, end)) = range else {
                        return;
                    };
                    let start = start.parse::<usize>().unwrap();
                    let end = end.parse::<usize>().unwrap().min(data.len() - 1);
                    let body = &data[start..=end];
                    let head = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nAccept-Ranges: bytes\r\nETag: \"{etag}\"\r\nConnection: close\r\n\r\n",
                        body.len(),
                        data.len()
                    );
                    if stream.write_all(head.as_bytes()).await.is_err() {
                        return;
                    }
                    for (chunk_index, piece) in body.chunks(256 * 1024).enumerate() {
                        // A pause test gates the fixture before EOF, so even a
                        // fast/loaded runner must observe progress before completion.
                        while hold_tail.load(Ordering::Acquire)
                            && end + 1 == data.len()
                            && body.len() > 1
                            && chunk_index + 1 == body.len().div_ceil(256 * 1024)
                        {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        if stream.write_all(piece).await.is_err() {
                            return;
                        }
                        served.fetch_add(piece.len() as u64, Ordering::Relaxed);
                        if delay_ms > 0 && body.len() > 1 {
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        }
                    }
                });
            }
        });
        Self {
            address,
            served,
            hold_tail,
            data: current,
            task,
        }
    }

    fn url(&self, query: &str) -> String {
        format!("http://{}/payload.bin{query}", self.address)
    }

    fn replace(&self, data: Arc<Vec<u8>>, etag: &str) {
        *self.data.lock().unwrap() = (data, etag.to_owned());
    }
}

fn root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("nibiru-resume-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    root
}

fn engine() -> Engine {
    Engine::new(Arc::new(LogSink::new(None).unwrap())).unwrap()
}

fn partial(destination: &Path) -> (PathBuf, PathBuf) {
    let base = format!("{}.nibiru-http.part", destination.display());
    (PathBuf::from(&base), PathBuf::from(format!("{base}.json")))
}

/// Runs a download and stops it once `pause_after` bytes are in; returns the result.
async fn run(
    engine: &Engine,
    url: String,
    destination: PathBuf,
    pause_after: Option<u64>,
) -> anyhow::Result<()> {
    let cancelled = Arc::new(AtomicBool::new(false));
    let signal = cancelled.clone();
    engine
        .download(
            DownloadRequest { url, destination },
            cancelled,
            move |progress| {
                if pause_after.is_some_and(|limit| progress.downloaded >= limit) {
                    signal.store(true, Ordering::Release);
                }
            },
        )
        .await
}

async fn pause_at(
    engine: &Engine,
    origin: &Origin,
    url: String,
    destination: PathBuf,
    limit: u64,
) -> anyhow::Result<()> {
    struct ReleaseGate(Arc<AtomicBool>);
    impl Drop for ReleaseGate {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Release);
        }
    }
    origin.hold_tail.store(true, Ordering::Release);
    let _release = ReleaseGate(origin.hold_tail.clone());
    run(engine, url, destination, Some(limit)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_paused_download_continues_without_fetching_finished_blocks_again() {
    let data = payload(1);
    let origin = Origin::start(data.clone(), "v1", 15).await;
    let root = root();
    let destination = root.join("payload.bin");
    let (part, state) = partial(&destination);

    let paused = pause_at(
        &engine(),
        &origin,
        origin.url(""),
        destination.clone(),
        10 << 20,
    )
    .await;
    assert!(paused.unwrap_err().to_string().contains("cancelled"));
    assert!(
        !destination.exists(),
        "a paused download must not publish a file"
    );
    assert!(
        part.exists() && state.exists(),
        "pausing keeps the partial and its record"
    );
    let record: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state).unwrap()).unwrap();
    assert!(record["done"].as_str().unwrap().chars().any(|c| c != '0'));
    // The record never carries the URL or its query.
    assert!(
        !std::fs::read_to_string(&state)
            .unwrap()
            .contains("payload.bin")
    );

    origin.served.store(0, Ordering::Relaxed);
    run(&engine(), origin.url(""), destination.clone(), None)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&destination).unwrap(), *data);
    assert!(
        !part.exists() && !state.exists(),
        "completion removes the partial and its record"
    );
    let fetched = origin.served.load(Ordering::Relaxed);
    assert!(
        fetched < (SIZE as u64) - (8 << 20),
        "the second run fetched {fetched} of {SIZE} bytes; finished blocks were downloaded again"
    );
    origin.task.abort();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refreshed_signed_link_still_resumes_the_same_file() {
    let data = payload(2);
    let origin = Origin::start(data.clone(), "v1", 15).await;
    let root = root();
    let destination = root.join("payload.bin");

    let paused = pause_at(
        &engine(),
        &origin,
        origin.url("?t=old"),
        destination.clone(),
        10 << 20,
    )
    .await;
    assert!(paused.is_err());
    origin.served.store(0, Ordering::Relaxed);
    run(
        &engine(),
        origin.url("?t=new&sig=other"),
        destination.clone(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(std::fs::read(&destination).unwrap(), *data);
    assert!(origin.served.load(Ordering::Relaxed) < (SIZE as u64) - (8 << 20));
    origin.task.abort();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_changed_file_on_the_server_restarts_instead_of_mixing_versions() {
    let old = payload(3);
    let new = payload(4);
    let origin = Origin::start(old, "v1", 15).await;
    let root = root();
    let destination = root.join("payload.bin");

    let paused = pause_at(
        &engine(),
        &origin,
        origin.url(""),
        destination.clone(),
        10 << 20,
    )
    .await;
    assert!(paused.is_err());
    origin.replace(new.clone(), "v2");
    run(&engine(), origin.url(""), destination.clone(), None)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(&destination).unwrap(),
        *new,
        "the finished file must be entirely the new version"
    );
    origin.task.abort();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_corrupt_record_is_discarded_and_the_download_still_completes() {
    let data = payload(5);
    let origin = Origin::start(data.clone(), "v1", 15).await;
    let root = root();
    let destination = root.join("payload.bin");
    let (_, state) = partial(&destination);

    let paused = pause_at(
        &engine(),
        &origin,
        origin.url(""),
        destination.clone(),
        10 << 20,
    )
    .await;
    assert!(paused.is_err());
    std::fs::write(&state, b"{ not json").unwrap();
    run(&engine(), origin.url(""), destination.clone(), None)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&destination).unwrap(), *data);
    origin.task.abort();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_killed_download_resumes_from_its_last_saved_record() {
    let data = payload(6);
    let origin = Origin::start(data.clone(), "v1", 150).await;
    let root = root();
    let destination = root.join("payload.bin");
    let (part, state) = partial(&destination);

    // Stop the task without letting it clean up, as a killed process would, after the record
    // has been written at least once.
    let engine_one = Arc::new(engine());
    let task = {
        let (engine, url, destination) = (engine_one.clone(), origin.url(""), destination.clone());
        tokio::spawn(async move { run(&engine, url, destination, None).await })
    };
    let mut saved = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        saved = std::fs::read(&state)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .is_some_and(|record| {
                record["done"]
                    .as_str()
                    .is_some_and(|d| d.chars().any(|c| c != '0'))
            });
        if saved {
            break;
        }
    }
    assert!(
        saved,
        "the record lists finished blocks while the download runs"
    );
    task.abort();
    let _ = task.await;
    assert!(part.exists());

    origin.served.store(0, Ordering::Relaxed);
    run(&engine(), origin.url(""), destination.clone(), None)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&destination).unwrap(), *data);
    assert!(
        origin.served.load(Ordering::Relaxed) < SIZE as u64 - (3 << 20),
        "the second run fetched the whole file again"
    );
    origin.task.abort();
    let _ = std::fs::remove_dir_all(root);
}
