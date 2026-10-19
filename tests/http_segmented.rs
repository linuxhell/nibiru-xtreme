use nibiru_xtreme::{
    engine::{DownloadRequest, Engine},
    logging::LogSink,
};
use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segmented_download_reassembles_ranges_exactly() {
    download_case(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_download_logs_stalls_and_reassembles_exactly() {
    download_case(false).await;
}

async fn download_case(segmented: bool) {
    let data = Arc::new(
        (0..(9 * 1024 * 1024 + 321))
            .map(|i| ((i * 31 + 17) % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_data = data.clone();

    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let data = server_data.clone();
            tokio::spawn(async move {
                let mut request = vec![0u8; 16 * 1024];
                let used = stream.read(&mut request).await.unwrap();
                let text = String::from_utf8_lossy(&request[..used]);
                let first = text.lines().next().unwrap_or_default();

                if first.starts_with("HEAD ") {
                    let ranges = if segmented { "bytes" } else { "none" };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: {ranges}\r\nConnection: close\r\n\r\n",
                        data.len()
                    );
                    stream.write_all(response.as_bytes()).await.unwrap();
                    return;
                }

                let range = text
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("range: bytes=")
                            .or_else(|| line.strip_prefix("Range: bytes="))
                    })
                    .and_then(|value| value.split_once('-'));

                if let Some((start, end)) = range {
                    let start = start.parse::<usize>().unwrap();
                    let end = end.parse::<usize>().unwrap().min(data.len() - 1);
                    let body = &data[start..=end];
                    let response = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        body.len(),
                        start,
                        end,
                        data.len()
                    );
                    stream.write_all(response.as_bytes()).await.unwrap();
                    if body.len() > 1 {
                        tokio::time::sleep(Duration::from_millis(1400)).await;
                    }
                    stream.write_all(body).await.unwrap();
                } else {
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        data.len()
                    );
                    stream.write_all(response.as_bytes()).await.unwrap();
                    tokio::time::sleep(Duration::from_millis(1400)).await;
                    stream.write_all(&data).await.unwrap();
                }
            });
        }
    });

    let root = std::env::temp_dir().join(format!("nibiru-segmented-{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&root).await.unwrap();
    let destination = root.join("payload.bin");

    let log_path = root.join("download.jsonl");
    let log = Arc::new(LogSink::new(Some(&log_path)).unwrap());
    let engine = Engine::new(log).unwrap();
    engine
        .download(
            DownloadRequest {
                url: format!("http://{address}/payload.bin"),
                destination: destination.clone(),
            },
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .await
        .unwrap();

    let downloaded = tokio::fs::read(&destination).await.unwrap();
    assert_eq!(downloaded.as_slice(), data.as_slice());

    let log = tokio::fs::read_to_string(&log_path).await.unwrap();
    let records: Vec<serde_json::Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let mode = if segmented { "segmented" } else { "single" };
    // A stalled body must still produce a zero-speed heartbeat before completion.
    assert!(records.iter().any(|record| {
        record["event"] == "http.progress"
            && record["detail"].as_str().is_some_and(|detail| {
                detail.contains(&format!("mode={mode} "))
                    && detail.contains("downloaded=0 ")
                    && detail.contains("speed_bytes_per_second=0 ")
            })
    }));
    let finalizing = records
        .iter()
        .position(|record| record["event"] == "http.finalization_started")
        .unwrap();
    let completed = records.last().unwrap();
    assert!(finalizing < records.len() - 1);
    assert_eq!(completed["event"], "http.download_completed");
    let detail = completed["detail"].as_str().unwrap();
    assert!(detail.contains("transfer_ms="));
    assert!(detail.contains("finalize_ms="));
    if segmented {
        let metrics = records
            .iter()
            .find(|record| record["event"] == "http.segmented_metrics")
            .unwrap();
        let detail = metrics["detail"].as_str().unwrap();
        assert!(detail.contains("http1_ranges=3 "));
        assert!(detail.contains("http2_ranges=0 "));
    }
    assert!(
        !records
            .iter()
            .any(|record| record["event"] == "http.segmented_fallback")
    );

    drop(engine);
    #[cfg(windows)]
    if segmented {
        // Exercise the packaged script under Windows PowerShell 5.1, including
        // its default output path, log file, binary lookup and saved SHA-256.
        let script = root.join("windows11.ps1");
        tokio::fs::copy(
            concat!(env!("CARGO_MANIFEST_DIR"), "/benchmarks/windows11.ps1"),
            &script,
        )
        .await
        .unwrap();
        tokio::fs::copy(
            env!("CARGO_BIN_EXE_nibiru-xtreme"),
            root.join("nibiru-xtreme.exe"),
        )
        .await
        .unwrap();
        let result = tokio::task::spawn_blocking(move || {
            std::process::Command::new("powershell.exe")
                // Do not inherit PowerShell 7 module paths from the CI shell.
                // Windows PowerShell must initialize its own native modules.
                .env_remove("PSModulePath")
                .args([
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                ])
                .arg(script)
                .arg("-Url")
                .arg(format!("http://{address}/payload.bin"))
                .arg("-Http1Only")
                .output()
                .unwrap()
        })
        .await
        .unwrap();
        assert!(
            result.status.success(),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr),
        );
        let iso = tokio::fs::read(root.join("windows11-test.iso"))
            .await
            .unwrap();
        assert_eq!(iso.as_slice(), data.as_slice());
        let summary: serde_json::Value = serde_json::from_slice(
            &tokio::fs::read(root.join("nibiru-benchmark.json"))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(summary["size_bytes"], data.len() as u64);
        assert_eq!(summary["http_mode"], "http1-only");
        assert_eq!(summary["sha256"].as_str().unwrap().len(), 64);
        assert_eq!(summary["binary_sha256"].as_str().unwrap().len(), 64);
        let script_log = tokio::fs::read_to_string(root.join("nibiru-download.jsonl"))
            .await
            .unwrap();
        assert!(script_log.contains("http.download_completed"));
    }
    server.abort();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let _ = tokio::fs::remove_dir_all(root).await;
}
