//! The HTTP options a download manager passes through aria2-style RPC: headers, cookies,
//! referer, proxy and speed limits.

use nibiru_xtreme::{engine::Engine, logging::LogSink, rpc::RpcServer};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

type Seen = Arc<Mutex<Vec<String>>>;

/// Serves `body` only to requests carrying `Cookie: sid=42` and `Referer: https://site.example/page`.
async fn picky_server(body: Vec<u8>, seen: Seen) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let body = body.clone();
            let seen = seen.clone();
            tokio::spawn(async move {
                let mut buffer = vec![0u8; 8192];
                let n = stream.read(&mut buffer).await.unwrap_or(0);
                let head = String::from_utf8_lossy(&buffer[..n]).to_lowercase();
                seen.lock().unwrap().push(head.clone());
                let allowed = head.contains("cookie: sid=42")
                    && head.contains("referer: https://site.example/page");
                let reply = if !allowed {
                    b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_vec()
                } else if head.starts_with("head ") {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .into_bytes()
                } else {
                    let mut out = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .into_bytes();
                    out.extend_from_slice(&body);
                    out
                };
                let _ = stream.write_all(&reply).await;
            });
        }
    });
    port
}

async fn rpc(limit: bool) -> (u16, reqwest::Client) {
    let log = Arc::new(LogSink::new(None).unwrap());
    let server = RpcServer::new(Engine::new(log.clone()).unwrap(), log, "k".into());
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let _ = limit;
    tokio::spawn(async move { server.serve(port).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    (port, reqwest::Client::new())
}

async fn call(port: u16, client: &reqwest::Client, method: &str, params: Vec<Value>) -> Value {
    let mut all = vec![json!("token:k")];
    all.extend(params);
    let response: Value = client
        .post(format!("http://127.0.0.1:{port}/jsonrpc"))
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

async fn finish(port: u16, client: &reqwest::Client, gid: &str) -> Value {
    for _ in 0..200 {
        let s = call(port, client, "nibiru.tellStatus", vec![json!(gid)]).await;
        if s["status"] == "complete" || s["status"] == "error" {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("download never finished");
}

#[tokio::test]
async fn cookies_referer_and_user_agent_reach_the_server() {
    let body: Vec<u8> = (0..70_000u32).map(|i| (i % 251) as u8).collect();
    let seen = Seen::default();
    let origin = picky_server(body.clone(), seen.clone()).await;
    let (port, client) = rpc(false).await;
    let dir = tempfile::tempdir().unwrap();
    let url = format!("http://127.0.0.1:{origin}/video.bin");

    // Without the browser's headers the server refuses.
    let gid = call(
        port,
        &client,
        "nibiru.addUri",
        vec![json!([url]), json!({"dir": dir.path(), "out": "no.bin"})],
    )
    .await;
    assert_eq!(
        finish(port, &client, gid.as_str().unwrap()).await["status"],
        "error"
    );

    // With them (exactly how the ADM passes them) it works.
    let gid = call(
        port,
        &client,
        "nibiru.addUri",
        vec![
            json!([url]),
            json!({"dir": dir.path(), "out": "yes.bin", "continue": "true",
                   "header": ["Cookie: sid=42", "X-Extra: 1"],
                   "referer": "https://site.example/page", "user-agent": "ADM-Test/1.0",
                   "split": "8", "max-connection-per-server": "8", "min-split-size": "1M"}),
        ],
    )
    .await;
    let done = finish(port, &client, gid.as_str().unwrap()).await;
    assert_eq!(done["status"], "complete", "{done}");
    assert_eq!(std::fs::read(dir.path().join("yes.bin")).unwrap(), body);
    let requests = seen.lock().unwrap().join("\n");
    assert!(requests.contains("user-agent: adm-test/1.0"), "{requests}");
    assert!(requests.contains("x-extra: 1"));
}

#[tokio::test]
async fn invalid_header_lines_are_rejected_not_sent() {
    let (port, client) = rpc(false).await;
    let response: Value = client
        .post(format!("http://127.0.0.1:{port}/jsonrpc"))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"nibiru.addUri",
                      "params":["token:k", ["http://127.0.0.1:9/x"], {"header": ["no colon here"]}]}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(response.get("error").is_some(), "{response}");
}

#[tokio::test]
async fn an_http_proxy_carries_the_request() {
    // The "proxy" answers every request itself, which proves the request went through it.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    let seen: Seen = Seen::default();
    let record = seen.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let record = record.clone();
            tokio::spawn(async move {
                let mut buffer = vec![0u8; 8192];
                let n = stream.read(&mut buffer).await.unwrap_or(0);
                let head = String::from_utf8_lossy(&buffer[..n]).to_string();
                record.lock().unwrap().push(head.clone());
                let reply = b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: close\r\n\r\nvia the proxy";
                let reply = if head.starts_with("HEAD") {
                    &reply[..reply.len() - 13]
                } else {
                    &reply[..]
                };
                let _ = stream.write_all(reply).await;
            });
        }
    });
    let (port, client) = rpc(false).await;
    let dir = tempfile::tempdir().unwrap();
    let gid = call(
        port,
        &client,
        "nibiru.addUri",
        vec![
            json!(["http://files.example.test/a.txt"]),
            json!({"dir": dir.path(), "out": "a.txt", "all-proxy": format!("http://127.0.0.1:{proxy_port}")}),
        ],
    )
    .await;
    let done = finish(port, &client, gid.as_str().unwrap()).await;
    assert_eq!(done["status"], "complete", "{done}");
    assert_eq!(
        std::fs::read(dir.path().join("a.txt")).unwrap(),
        b"via the proxy"
    );
    assert!(
        seen.lock()
            .unwrap()
            .iter()
            .any(|r| r.starts_with("GET http://files.example.test/a.txt")),
        "absolute-form request to the proxy"
    );
}

#[tokio::test]
async fn per_task_and_global_limits_slow_the_transfer_and_can_be_lifted() {
    let body = vec![7u8; 400_000];
    let seen = Seen::default();
    // Reuse the picky server's headers requirement so the same options are exercised.
    let origin = picky_server(body.clone(), seen).await;
    let (port, client) = rpc(true).await;
    let dir = tempfile::tempdir().unwrap();
    let url = format!("http://127.0.0.1:{origin}/limited.bin");
    let options = |out: &str, limit: &str| {
        json!({"dir": dir.path(), "out": out, "max-download-limit": limit,
               "header": ["Cookie: sid=42"], "referer": "https://site.example/page"})
    };
    let started = Instant::now();
    let gid = call(
        port,
        &client,
        "nibiru.addUri",
        vec![json!([url]), options("slow.bin", "100K")],
    )
    .await;
    let done = finish(port, &client, gid.as_str().unwrap()).await;
    assert_eq!(done["status"], "complete");
    assert!(
        started.elapsed() > Duration::from_millis(1800),
        "100 KB/s cap took only {:?}",
        started.elapsed()
    );

    call(
        port,
        &client,
        "nibiru.changeGlobalOption",
        vec![json!({"max-overall-download-limit": "100K"})],
    )
    .await;
    let started = Instant::now();
    let gid = call(
        port,
        &client,
        "nibiru.addUri",
        vec![json!([url]), options("global.bin", "0")],
    )
    .await;
    finish(port, &client, gid.as_str().unwrap()).await;
    assert!(
        started.elapsed() > Duration::from_millis(1800),
        "global cap took only {:?}",
        started.elapsed()
    );

    call(
        port,
        &client,
        "nibiru.changeGlobalOption",
        vec![json!({"max-overall-download-limit": "0"})],
    )
    .await;
    let started = Instant::now();
    let gid = call(
        port,
        &client,
        "nibiru.addUri",
        vec![json!([url]), options("fast.bin", "0")],
    )
    .await;
    finish(port, &client, gid.as_str().unwrap()).await;
    assert!(
        started.elapsed() < Duration::from_millis(1500),
        "uncapped took {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn split_and_server_caps_bound_actual_concurrent_http_requests() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let body = Arc::new(vec![31u8; 12 * 1024 * 1024]);
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = listener.local_addr().unwrap().port();
    let source = body.clone();
    let active_count = active.clone();
    let peak_count = peak.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let body = source.clone();
            let active = active_count.clone();
            let peak = peak_count.clone();
            tokio::spawn(async move {
                let mut buffer = vec![0; 8192];
                let n = stream.read(&mut buffer).await.unwrap();
                let request = String::from_utf8_lossy(&buffer[..n]);
                if request.starts_with("HEAD ") {
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    stream.write_all(head.as_bytes()).await.unwrap();
                    return;
                }
                let range = request.lines().find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("range: bytes=")
                        .map(str::to_owned)
                });
                let (from, to) = range
                    .as_deref()
                    .and_then(|r| r.split_once('-'))
                    .map(|(a, b)| {
                        (
                            a.parse::<usize>().unwrap(),
                            b.parse::<usize>().unwrap_or(body.len() - 1),
                        )
                    })
                    .unwrap_or((0, body.len() - 1));
                let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(count, Ordering::SeqCst);
                let status = if range.is_some() {
                    "206 Partial Content"
                } else {
                    "200 OK"
                };
                let head = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Range: bytes {from}-{to}/{}\r\nConnection: close\r\n\r\n",
                    to - from + 1,
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes()).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = stream.write_all(&body[from..=to]).await;
                active.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    let (port, client) = rpc(false).await;
    let dir = tempfile::tempdir().unwrap();
    for (split, cap, min_size, expected, name) in [
        ("1", "4", "1M", 1, "one.bin"),
        ("4", "2", "1M", 2, "two.bin"),
        ("4", "4", "20M", 1, "large-min.bin"),
    ] {
        peak.store(0, Ordering::SeqCst);
        let gid=call(port,&client,"nibiru.addUri",vec![json!([format!("http://127.0.0.1:{origin}/file")]),json!({"dir":dir.path(),"out":name,"split":split,"max-connection-per-server":cap,"min-split-size":min_size})]).await;
        let done = finish(port, &client, gid.as_str().unwrap()).await;
        assert_eq!(done["status"], "complete", "{done}");
        assert_eq!(std::fs::read(dir.path().join(name)).unwrap(), *body);
        assert_eq!(
            peak.load(Ordering::SeqCst),
            expected,
            "split={split} cap={cap} minimum={min_size}"
        );
    }
    server.abort();
}
