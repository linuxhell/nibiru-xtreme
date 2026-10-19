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
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
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
                    stream.write_all(body).await.unwrap();
                } else {
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        data.len()
                    );
                    stream.write_all(response.as_bytes()).await.unwrap();
                    stream.write_all(&data).await.unwrap();
                }
            });
        }
    });

    let root = std::env::temp_dir().join(format!("nibiru-segmented-{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&root).await.unwrap();
    let destination = root.join("payload.bin");

    let log = Arc::new(LogSink::new(None).unwrap());
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

    server.abort();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let _ = tokio::fs::remove_dir_all(root).await;
}
