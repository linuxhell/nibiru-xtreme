//! The first response is useful payload, never a discarded probe.
use nibiru_xtreme::{
    engine::{DownloadRequest, Engine, RequestOptions},
    logging::LogSink,
};
use std::sync::{Arc, Mutex, atomic::AtomicBool};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
async fn case(size: usize, ignore: bool, chunked: bool, single: bool) {
    let data = Arc::new((0..size).map(|i| (i % 251) as u8).collect::<Vec<_>>());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/file", listener.local_addr().unwrap());
    let (requests, payload) = (seen.clone(), data.clone());
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (requests, data) = (requests.clone(), payload.clone());
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
                assert!(request.starts_with("get "), "no HEAD at startup");
                let range = request
                    .lines()
                    .find_map(|l| l.strip_prefix("range: bytes="))
                    .map(str::to_owned);
                requests.lock().unwrap().push(range.clone());
                if size == 0 && range.is_some() {
                    let _=stream.write_all(b"HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\nContent-Range: bytes */0\r\nConnection: close\r\n\r\n").await;
                    return;
                }
                let (start, end, partial) = if let Some(range) = range.filter(|_| !ignore) {
                    let (a, b) = range.split_once('-').unwrap();
                    (
                        a.parse::<usize>().unwrap(),
                        b.parse::<usize>().unwrap().min(size - 1),
                        true,
                    )
                } else {
                    (0, size.saturating_sub(1), false)
                };
                if start > 0 {
                    assert!(request.contains("if-range: \"stable\""));
                }
                let body = if size == 0 {
                    &[][..]
                } else {
                    &data[start..=end]
                };
                let status = if partial {
                    "206 Partial Content"
                } else {
                    "200 OK"
                };
                let content_range = if partial {
                    format!("Content-Range: bytes {start}-{end}/{size}\r\n")
                } else {
                    String::new()
                };
                let framing = if chunked {
                    "Transfer-Encoding: chunked\r\n".to_owned()
                } else {
                    format!("Content-Length: {}\r\n", body.len())
                };
                if stream.write_all(format!("HTTP/1.1 {status}\r\n{framing}{content_range}ETag: \"stable\"\r\nConnection: close\r\n\r\n").as_bytes()).await.is_err(){return}
                if chunked {
                    for chunk in body.chunks(32768) {
                        if stream
                            .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                            .await
                            .is_err()
                        {
                            return;
                        }
                        if stream.write_all(chunk).await.is_err() {
                            return;
                        }
                        if stream.write_all(b"\r\n").await.is_err() {
                            return;
                        }
                    }
                    let _ = stream.write_all(b"0\r\n\r\n").await;
                } else {
                    let _ = stream.write_all(body).await;
                }
            });
        }
    });
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("file");
    Engine::new(Arc::new(LogSink::new(None).unwrap()))
        .unwrap()
        .with_request_options(RequestOptions {
            max_connections: single.then_some(1),
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
        )
        .await
        .unwrap();
    server.abort();
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        assert_eq!(
            std::fs::metadata(&destination).unwrap().file_attributes() & 0x200,
            0
        );
    }
    assert_eq!(std::fs::read(destination).unwrap(), *data);
    let requests = seen.lock().unwrap();
    if ignore || single || size <= 4 << 20 {
        assert_eq!(requests.len(), if size == 0 && !single { 2 } else { 1 });
    }
    if !ignore && !single && size > 4 << 20 {
        let mut ranges = requests
            .iter()
            .map(|r| {
                let (a, b) = r.as_ref().unwrap().split_once('-').unwrap();
                (
                    a.parse::<usize>().unwrap(),
                    b.parse::<usize>().unwrap().min(size - 1),
                )
            })
            .collect::<Vec<_>>();
        ranges.sort_unstable();
        assert_eq!(ranges[0], (0, (4 << 20) - 1));
        assert_eq!(ranges.iter().filter(|(start, _)| *start == 0).count(), 1);
        let mut next = 0;
        for (start, end) in ranges {
            assert!(start <= next);
            next = next.max(end + 1);
        }
        assert_eq!(next, size);
    }
}
#[tokio::test]
async fn one_small_partial_response_is_the_entire_download() {
    case(131071, false, false, false).await;
}
#[tokio::test]
async fn first_range_with_unknown_body_length_is_reused() {
    case((5 << 20) + 17, false, true, false).await;
}
#[tokio::test]
async fn large_download_does_not_request_the_prefix_again() {
    case((64 << 20) + 17, false, false, false).await;
}
#[tokio::test]
async fn range_ignoring_server_body_is_reused() {
    case((9 << 20) + 17, true, false, false).await;
}
#[tokio::test]
async fn explicit_single_connection_uses_one_plain_get() {
    case(150000, false, false, true).await;
}
#[tokio::test]
async fn empty_resource_uses_plain_get_after_416() {
    case(0, false, false, false).await;
}
