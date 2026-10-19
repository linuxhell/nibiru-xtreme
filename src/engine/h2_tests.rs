use super::*;
use bytes::Bytes;
use futures_util::future::poll_fn;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, tcp::OwnedReadHalf, tcp::OwnedWriteHalf},
    sync::{Mutex, mpsc},
};

// Delay frames in one direction without serializing bandwidth by packet.
// In particular, WINDOW_UPDATE and BDP PING frames experience the chosen RTT.
async fn delayed_relay(mut input: OwnedReadHalf, mut output: OwnedWriteHalf, delay: Duration) {
    let (tx, mut rx) = mpsc::channel(256);
    let reader = tokio::spawn(async move {
        loop {
            let mut buffer = vec![0; 16 * 1024];
            let Ok(used) = input.read(&mut buffer).await else {
                break;
            };
            if used == 0 {
                break;
            }
            buffer.truncate(used);
            if tx
                .send((time::Instant::now() + delay, buffer))
                .await
                .is_err()
            {
                break;
            }
        }
    });
    while let Some((ready, buffer)) = rx.recv().await {
        time::sleep_until(ready).await;
        if output.write_all(&buffer).await.is_err() {
            break;
        }
    }
    reader.abort();
    let _ = output.shutdown().await;
}

#[derive(Clone, Copy, Debug)]
enum Flow {
    Default,
    Adaptive,
    Wide,
}

async fn run_fixture(flow: Flow, size: usize, rtt: Duration) -> f64 {
    let data = Bytes::from(
        (0..size)
            .map(|i| ((i * 31 + 17) % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let server_address = listener.local_addr().unwrap();
    let server_data = data.clone();
    let request_count = Arc::new(AtomicU64::new(0));
    let max_range = Arc::new(AtomicU64::new(0));
    let server_count = request_count.clone();
    let server_max = max_range.clone();
    let next_send = Arc::new(Mutex::new(time::Instant::now()));
    let server = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let data = server_data.clone();
            let next_send = next_send.clone();
            let request_count = server_count.clone();
            let max_range = server_max.clone();
            connections.spawn(async move {
                let mut connection = h2::server::handshake(socket).await.unwrap();
                let mut responses = JoinSet::new();
                while let Some(Ok((request, mut respond))) = connection.accept().await {
                    let data = data.clone();
                    let next_send = next_send.clone();
                    let request_count = request_count.clone();
                    let max_range = max_range.clone();
                    responses.spawn(async move {
                        let mut builder =
                            http::Response::builder().header("accept-ranges", "bytes");
                        if request.method() == http::Method::HEAD {
                            respond
                                .send_response(
                                    builder
                                        .header("content-length", data.len())
                                        .body(())
                                        .unwrap(),
                                    true,
                                )
                                .unwrap();
                            return;
                        }
                        let range = request.headers()["range"].to_str().unwrap();
                        let (start, end) = range
                            .strip_prefix("bytes=")
                            .unwrap()
                            .split_once('-')
                            .unwrap();
                        let mut offset: usize = start.parse().unwrap();
                        let end: usize = end.parse::<usize>().unwrap().min(data.len() - 1);
                        if end - offset + 1 > 1 {
                            request_count.fetch_add(1, Ordering::Relaxed);
                            max_range.fetch_max((end - offset + 1) as u64, Ordering::Relaxed);
                        }
                        builder = builder
                            .status(206)
                            .header("content-length", end - offset + 1)
                            .header(
                                "content-range",
                                format!("bytes {offset}-{end}/{}", data.len()),
                            );
                        let mut stream = respond
                            .send_response(builder.body(()).unwrap(), false)
                            .unwrap();
                        while offset <= end {
                            let desired = (end - offset + 1).min(64 * 1024);
                            stream.reserve_capacity(desired);
                            let Some(Ok(capacity)) = poll_fn(|cx| stream.poll_capacity(cx)).await
                            else {
                                break;
                            };
                            if capacity == 0 {
                                continue;
                            }
                            let count = desired.min(capacity);
                            let deadline = {
                                let mut next = next_send.lock().await;
                                *next = (*next).max(time::Instant::now())
                                    + Duration::from_secs_f64(count as f64 / (110.0 * 1e6));
                                *next
                            };
                            time::sleep_until(deadline).await;
                            if stream
                                .send_data(data.slice(offset..offset + count), offset + count > end)
                                .is_err()
                            {
                                break;
                            }
                            offset += count;
                        }
                    });
                }
            });
        }
    });
    let proxy_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let proxy_address = proxy_listener.local_addr().unwrap();
    let proxy = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            let (incoming, _) = proxy_listener.accept().await.unwrap();
            connections.spawn(async move {
                let outgoing = TcpStream::connect(server_address).await.unwrap();
                incoming.set_nodelay(true).unwrap();
                outgoing.set_nodelay(true).unwrap();
                let (client_read, client_write) = incoming.into_split();
                let (server_read, server_write) = outgoing.into_split();
                tokio::join!(
                    delayed_relay(client_read, server_write, rtt),
                    delayed_relay(server_read, client_write, Duration::ZERO),
                );
            });
        }
    });
    let root = std::env::temp_dir().join(format!("nibiru-h2-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root).await.unwrap();
    let destination = root.join("payload.bin");
    let engine = Engine {
        sync_interval_bytes: 0,
        adaptive: false,
        http3_only: false,
        transport_options: Default::default(),
        client: match flow {
            Flow::Default => Client::builder(),
            Flow::Adaptive => Client::builder().http2_adaptive_window(true),
            Flow::Wide => Engine::client_builder(false),
        }
        .http2_prior_knowledge()
        .build()
        .unwrap(),
        log: Arc::new(LogSink::new(Some(&root.join("download.jsonl"))).unwrap()),
    };
    let started = Instant::now();
    time::timeout(
        Duration::from_secs(60),
        engine.download(
            DownloadRequest {
                url: format!("http://{proxy_address}/payload.bin"),
                destination: destination.clone(),
            },
            Arc::new(AtomicBool::new(false)),
            |_| {},
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let seconds = started.elapsed().as_secs_f64();
    assert_eq!(
        fs::read(&destination).await.unwrap().as_slice(),
        data.as_ref()
    );
    let log = fs::read_to_string(root.join("download.jsonl"))
        .await
        .unwrap();
    assert!(log.contains("http.segmented_started"));
    assert!(!log.contains("http.segmented_fallback"));
    assert!(log.contains("version=HTTP/2.0"));
    assert!(log.contains(&format!(
        "http2_ranges={}",
        request_count.load(Ordering::Relaxed)
    )));
    if size as u64 > MAX_HTTP_RANGE_SIZE * suggested_worker_count(size as u64) as u64 {
        assert!(max_range.load(Ordering::Relaxed) > RANGE_CHUNK_SIZE);
    }
    drop(engine);
    proxy.abort();
    server.abort();
    let _ = fs::remove_dir_all(root).await;
    seconds
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2_segmented_download_preserves_bytes_with_production_windows() {
    run_fixture(Flow::Wide, 9 * 1024 * 1024 + 321, Duration::from_millis(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2_large_ranges_stream_correctly_through_small_write_buffers() {
    run_fixture(
        Flow::Wide,
        257 * 1024 * 1024 + 321,
        Duration::from_millis(5),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "controlled throughput experiment; run in release without competing load"]
async fn compare_h2_flow_control() {
    let rtt_ms = std::env::var("NIBIRU_BENCH_RTT_MS")
        .map(|value| value.parse::<u64>().expect("invalid benchmark RTT"))
        .unwrap_or(50);
    let size_mib = std::env::var("NIBIRU_BENCH_SIZE_MIB")
        .map(|value| value.parse::<usize>().expect("invalid benchmark size"))
        .unwrap_or(256);
    let mut results = Vec::new();
    for round in 0..3 {
        let flows = [Flow::Default, Flow::Adaptive, Flow::Wide];
        for index in 0..flows.len() {
            let flow = flows[(index + round) % flows.len()];
            let seconds =
                run_fixture(flow, size_mib * 1024 * 1024, Duration::from_millis(rtt_ms)).await;
            println!(
                "h2_fixture size_mib={size_mib} rtt_ms={rtt_ms} round={round} flow={flow:?} seconds={seconds:.6} MBps={:.3}",
                size_mib as f64 * 1024.0 * 1024.0 / seconds / 1e6
            );
            results.push((flow, seconds));
        }
    }
    // Measurements are printed; throughput is intentionally not a flaky CI assertion.
    assert_eq!(results.len(), 9);
}
