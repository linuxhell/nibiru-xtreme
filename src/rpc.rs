use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

pub mod bt;

use crate::{
    VERSION,
    bittorrent::limiter::RateLimiter,
    engine::{DownloadRequest, Engine, Progress, ProxyOptions, RequestOptions, safe_output_name},
    logging::LogSink,
};

#[derive(Clone)]
struct Task {
    status: String,
    total: u64,
    completed: u64,
    speed: u64,
    error: Option<String>,
    cancel: Arc<AtomicBool>,
    request: DownloadRequest,
    transport: crate::protocols::TransportOptions,
    http: RequestOptions,
    /// This task's own speed cap (`max-download-limit`).
    limit: Arc<RateLimiter>,
}

#[derive(Clone)]
pub struct RpcServer {
    engine: Engine,
    log: Arc<LogSink>,
    secret: String,
    tasks: Arc<RwLock<HashMap<String, Task>>>,
    bt: Option<Arc<bt::BtService>>,
    /// `max-overall-download-limit` for HTTP/FTP tasks (BitTorrent has its own session limit).
    global_limit: Arc<RateLimiter>,
}

impl RpcServer {
    pub fn new(engine: Engine, log: Arc<LogSink>, secret: String) -> Self {
        Self {
            engine,
            log,
            secret,
            tasks: Arc::new(RwLock::new(HashMap::new())),
            bt: None,
            global_limit: Arc::new(RateLimiter::new(0)),
        }
    }

    /// Enables BitTorrent and Magnet downloads through this server.
    pub fn with_bittorrent(mut self, service: Arc<bt::BtService>) -> Self {
        self.bt = Some(service);
        self
    }

    fn bt(&self) -> Result<&Arc<bt::BtService>> {
        self.bt
            .as_ref()
            .ok_or_else(|| anyhow!("BitTorrent is not enabled in this server"))
    }

    pub async fn serve(self, port: u16) -> Result<()> {
        let listener = TcpListener::bind(("127.0.0.1", port)).await?;
        let addr = listener.local_addr()?;
        self.log
            .info("rpc.started", &format!("listen=127.0.0.1:{}", addr.port()));
        loop {
            let (stream, peer) = listener.accept().await?;
            if !peer.ip().is_loopback() {
                continue;
            }
            let server = self.clone();
            tokio::spawn(async move {
                if let Err(error) = server.handle(stream).await {
                    server.log.warn("rpc.request_failed", &error.to_string());
                }
            });
        }
    }

    async fn handle(&self, mut stream: TcpStream) -> Result<()> {
        let mut buffer = vec![0u8; 1024 * 1024];
        let mut used = 0usize;
        let header_end = loop {
            if used == buffer.len() {
                return Err(anyhow!("request too large"));
            }
            let n = stream.read(&mut buffer[used..]).await?;
            if n == 0 {
                return Err(anyhow!("connection closed"));
            }
            used += n;
            if let Some(pos) = find_bytes(&buffer[..used], b"\r\n\r\n") {
                break pos + 4;
            }
        };

        let headers = std::str::from_utf8(&buffer[..header_end])?;
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.strip_prefix("Content-Length:")
                    .or_else(|| line.strip_prefix("content-length:"))
            })
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(0);
        if content_length > 1024 * 1024 {
            return Err(anyhow!("RPC body too large"));
        }
        while used < header_end + content_length {
            let n = stream.read(&mut buffer[used..]).await?;
            if n == 0 {
                return Err(anyhow!("connection closed"));
            }
            used += n;
        }

        let body = &buffer[header_end..header_end + content_length];
        let request: Value = serde_json::from_slice(body).context("invalid JSON-RPC body")?;
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing method"))?;
        let mut params = request
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        if !self.secret.is_empty() {
            let token = params
                .first()
                .and_then(Value::as_str)
                .and_then(|v| v.strip_prefix("token:"));
            if token != Some(self.secret.as_str()) {
                return self
                    .write_json(&mut stream, json!({"jsonrpc":"2.0","id":id,"error":{"code":-32001,"message":"unauthorized"}}))
                    .await;
            }
            params.remove(0);
        }

        let result = self.dispatch(method, params).await;
        let payload = match result {
            Ok(value) => json!({"jsonrpc":"2.0","id":id,"result":value}),
            Err(error) => {
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":error.to_string()}})
            }
        };
        self.write_json(&mut stream, payload).await
    }

    async fn write_json(&self, stream: &mut TcpStream, payload: Value) -> Result<()> {
        let body = payload.to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).await?;
        Ok(())
    }

    async fn dispatch(&self, method: &str, params: Vec<Value>) -> Result<Value> {
        let gid = params.first().and_then(Value::as_str).unwrap_or_default();
        let bt_gid = self.bt.as_ref().filter(|bt| bt.owns(gid));
        let options_at = |index: usize| params.get(index).and_then(Value::as_object);
        match method {
            "aria2.getVersion" | "nibiru.getVersion" => {
                let mut features = vec!["HTTP", "HTTPS", "HTTP/2", "WebDAV-GET", "RPC", "Resume"];
                if self.bt.is_some() {
                    features.extend(["BitTorrent", "Magnet"]);
                }
                if cfg!(feature = "ftp") {
                    features.extend(["FTP", "FTPS"]);
                }
                if cfg!(feature = "sftp") {
                    features.push("SFTP");
                }
                if cfg!(feature = "http3") {
                    features.push("HTTP/3-explicit");
                }
                Ok(json!({"version":VERSION,"enabledFeatures":features}))
            }
            "aria2.addUri" | "nibiru.addUri" => {
                let uri = params
                    .first()
                    .and_then(Value::as_array)
                    .and_then(|v| v.first())
                    .and_then(Value::as_str);
                if uri.is_some_and(|u| {
                    u.get(..7)
                        .is_some_and(|p| p.eq_ignore_ascii_case("magnet:"))
                }) {
                    return Ok(Value::String(
                        self.bt()?
                            .add_magnet(uri.unwrap_or_default(), options_at(1))
                            .await?,
                    ));
                }
                self.add_uri(params).await
            }
            "aria2.addTorrent" => {
                let torrent = params
                    .first()
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("missing torrent"))?;
                Ok(Value::String(
                    self.bt()?.add_torrent(torrent, options_at(2)).await?,
                ))
            }
            "aria2.tellStatus" | "nibiru.tellStatus" => {
                if let Some(bt) = bt_gid {
                    let keys: Option<Vec<String>> =
                        params.get(1).and_then(Value::as_array).map(|keys| {
                            keys.iter()
                                .filter_map(Value::as_str)
                                .map(str::to_owned)
                                .collect()
                        });
                    return bt.tell_status(gid, keys.as_deref());
                }
                self.tell_status(params).await
            }
            "aria2.pause" | "aria2.forcePause" | "nibiru.pause" => {
                if let Some(bt) = bt_gid {
                    bt.pause(gid)?;
                    return Ok(Value::String(gid.to_owned()));
                }
                self.pause(params).await
            }
            "aria2.unpause" | "nibiru.resume" => {
                if let Some(bt) = bt_gid {
                    bt.resume(gid)?;
                    return Ok(Value::String(gid.to_owned()));
                }
                self.resume(params).await
            }
            "aria2.remove" | "aria2.forceRemove" | "nibiru.remove" => {
                if let Some(bt) = bt_gid {
                    bt.remove(gid)?;
                    return Ok(Value::String(gid.to_owned()));
                }
                self.remove(params).await
            }
            "aria2.removeDownloadResult" => {
                if let Some(bt) = bt_gid {
                    bt.remove_result(gid)?;
                }
                Ok(Value::String("OK".into()))
            }
            "aria2.changeOption" => {
                if let (Some(bt), Some(options)) = (bt_gid, options_at(1)) {
                    bt.change_option(gid, options)?;
                } else if let Some(options) = options_at(1)
                    && let Some(limit) = options
                        .get("max-download-limit")
                        .and_then(Value::as_str)
                        .and_then(bt::parse_size)
                    && let Some(task) = self.tasks.read().await.get(gid)
                {
                    task.limit.set_rate(limit);
                }
                Ok(Value::String("OK".into()))
            }
            "aria2.changeGlobalOption" => {
                if let Some(options) = options_at(0) {
                    if let Some(limit) = options
                        .get("max-overall-download-limit")
                        .and_then(Value::as_str)
                        .and_then(bt::parse_size)
                    {
                        self.global_limit.set_rate(limit);
                    }
                    if let Some(bt) = self.bt.as_ref() {
                        bt.change_global_option(options);
                    }
                }
                Ok(Value::String("OK".into()))
            }
            "aria2.getFiles" => match bt_gid {
                Some(bt) => bt.files(gid),
                None => Ok(json!([])),
            },
            "aria2.getPeers" => match bt_gid {
                Some(bt) => bt.peers(gid),
                None => Ok(json!([])),
            },
            "aria2.getGlobalStat" => {
                let (active, down, up) = self.bt.as_ref().map(|bt| bt.totals()).unwrap_or_default();
                let http_active = self
                    .tasks
                    .read()
                    .await
                    .values()
                    .filter(|t| t.status == "active")
                    .count();
                let http_speed: u64 = self.tasks.read().await.values().map(|t| t.speed).sum();
                Ok(json!({
                    "downloadSpeed": (down + http_speed).to_string(),
                    "uploadSpeed": up.to_string(),
                    "numActive": (active + http_active).to_string(),
                    "numWaiting": "0",
                    "numStopped": "0",
                    "numStoppedTotal": "0",
                }))
            }
            "aria2.tellActive" => {
                let mut out = Vec::new();
                if let Some(bt) = &self.bt {
                    for id in bt.list_ids() {
                        if let Ok(status) = bt.tell_status(&id, None)
                            && status["status"] == "active"
                        {
                            out.push(status);
                        }
                    }
                }
                Ok(Value::Array(out))
            }
            "aria2.tellWaiting" | "aria2.tellStopped" => Ok(json!([])),
            // Not part of aria2: a loopback URL a media player can open to watch while downloading.
            "nibiru.streamUrl" => {
                let file = params.get(1).and_then(Value::as_u64).map(|i| i as usize);
                Ok(Value::String(self.bt()?.stream_url(gid, file)?))
            }
            _ => Err(anyhow!("method not implemented: {method}")),
        }
    }

    async fn add_uri(&self, params: Vec<Value>) -> Result<Value> {
        let uri = params
            .first()
            .and_then(Value::as_array)
            .and_then(|v| v.first())
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing URI"))?
            .to_owned();
        let options = params.get(1).and_then(Value::as_object);
        let dir = options
            .and_then(|m| m.get("dir"))
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        let out = options
            .and_then(|m| m.get("out"))
            .and_then(Value::as_str)
            .unwrap_or("download");
        if !safe_output_name(out) {
            return Err(anyhow!("invalid output filename"));
        }
        let request = DownloadRequest {
            url: uri,
            destination: dir.join(out),
        };
        let value = |key: &str| options.and_then(|m| m.get(key)).and_then(Value::as_str);
        let transport = crate::protocols::TransportOptions {
            ca_certificate: value("ca-certificate").map(PathBuf::from),
            ssh_host_key: value("ssh-host-key").map(str::to_owned),
            ssh_identity: value("ssh-identity").map(PathBuf::from),
        };
        let http = request_options(options)?;
        let limit = Arc::new(RateLimiter::new(
            value("max-download-limit")
                .and_then(bt::parse_size)
                .unwrap_or(0),
        ));
        let gid = uuid::Uuid::new_v4().simple().to_string();
        let cancel = Arc::new(AtomicBool::new(false));
        self.tasks.write().await.insert(
            gid.clone(),
            Task {
                status: "active".into(),
                total: 0,
                completed: 0,
                speed: 0,
                error: None,
                cancel: cancel.clone(),
                request: request.clone(),
                transport: transport.clone(),
                http: http.clone(),
                limit: limit.clone(),
            },
        );
        self.spawn_download(gid.clone(), request, cancel, transport, http, limit);
        Ok(Value::String(gid))
    }

    fn spawn_download(
        &self,
        gid: String,
        request: DownloadRequest,
        cancel: Arc<AtomicBool>,
        transport: crate::protocols::TransportOptions,
        http: RequestOptions,
        limit: Arc<RateLimiter>,
    ) {
        let engine = match self
            .engine
            .clone()
            .with_transport_options(transport)
            .with_request_options(http)
        {
            Ok(engine) => engine.with_limiters(vec![limit, self.global_limit.clone()]),
            Err(error) => {
                let tasks = self.tasks.clone();
                tokio::spawn(async move {
                    if let Some(task) = tasks.write().await.get_mut(&gid) {
                        task.status = "error".into();
                        task.error = Some(error.to_string());
                    }
                });
                return;
            }
        };
        let tasks = self.tasks.clone();
        let log = self.log.clone();
        tokio::spawn(async move {
            let gid_progress = gid.clone();
            let tasks_progress = tasks.clone();
            let result = engine
                .download(
                    request.clone(),
                    cancel.clone(),
                    move |Progress {
                              downloaded,
                              total,
                              speed_bytes_per_second,
                          }| {
                        let tasks = tasks_progress.clone();
                        let gid = gid_progress.clone();
                        tokio::spawn(async move {
                            if let Some(task) = tasks.write().await.get_mut(&gid) {
                                task.completed = downloaded;
                                task.total = total.unwrap_or_default();
                                task.speed = speed_bytes_per_second;
                            }
                        });
                    },
                )
                .await;
            let mut guard = tasks.write().await;
            if let Some(task) = guard.get_mut(&gid) {
                match result {
                    Ok(()) => {
                        task.status = "complete".into();
                        task.speed = 0;
                        if task.total == 0 {
                            task.total = task.completed;
                        }
                    }
                    Err(_error) if cancel.load(Ordering::Acquire) => {
                        task.status = "paused".into();
                        task.speed = 0;
                    }
                    Err(error) => {
                        task.status = "error".into();
                        task.speed = 0;
                        task.error = Some(error.to_string());
                        log.error("rpc.download_failed", &format!("gid={gid} error={error}"));
                    }
                }
            }
        });
    }

    async fn tell_status(&self, params: Vec<Value>) -> Result<Value> {
        let gid = params
            .first()
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing gid"))?;
        let guard = self.tasks.read().await;
        let task = guard.get(gid).ok_or_else(|| anyhow!("unknown gid"))?;
        Ok(json!({
            "gid": gid,
            "status": task.status,
            "totalLength": task.total.to_string(),
            "completedLength": task.completed.to_string(),
            "downloadSpeed": task.speed.to_string(),
            "uploadSpeed": "0",
            "connections": if task.status == "active" { "1" } else { "0" },
            "errorMessage": task.error.clone().unwrap_or_default(),
            "files": [{
                "index":"1",
                "path":task.request.destination.to_string_lossy(),
                "length":task.total.to_string(),
                "completedLength":task.completed.to_string(),
                "selected":"true",
                "uris":[{"uri":task.request.url,"status":if task.status=="active"{"used"}else{"waiting"}}]
            }]
        }))
    }

    async fn pause(&self, params: Vec<Value>) -> Result<Value> {
        let gid = params
            .first()
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing gid"))?;
        let guard = self.tasks.read().await;
        let task = guard.get(gid).ok_or_else(|| anyhow!("unknown gid"))?;
        task.cancel.store(true, Ordering::Release);
        Ok(Value::String(gid.to_owned()))
    }

    async fn resume(&self, params: Vec<Value>) -> Result<Value> {
        let gid = params
            .first()
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing gid"))?
            .to_owned();
        let (request, cancel, transport, http, limit) = {
            let mut guard = self.tasks.write().await;
            let task = guard.get_mut(&gid).ok_or_else(|| anyhow!("unknown gid"))?;
            if task.status != "paused" && task.status != "error" {
                return Ok(Value::String(gid));
            }
            let cancel = Arc::new(AtomicBool::new(false));
            task.cancel = cancel.clone();
            task.status = "active".into();
            task.error = None;
            (
                task.request.clone(),
                cancel,
                task.transport.clone(),
                task.http.clone(),
                task.limit.clone(),
            )
        };
        self.spawn_download(gid.clone(), request, cancel, transport, http, limit);
        Ok(Value::String(gid))
    }

    async fn remove(&self, params: Vec<Value>) -> Result<Value> {
        let gid = params
            .first()
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing gid"))?
            .to_owned();
        if let Some(task) = self.tasks.write().await.remove(&gid) {
            task.cancel.store(true, Ordering::Release);
        }
        Ok(Value::String(gid))
    }
}

/// aria2-style HTTP options: `header`, `referer`, `user-agent`, `all-proxy*`.
fn request_options(options: Option<&serde_json::Map<String, Value>>) -> Result<RequestOptions> {
    let mut out = RequestOptions::default();
    let Some(options) = options else {
        return Ok(out);
    };
    let text = |key: &str| {
        options
            .get(key)
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
    };
    match options.get("header") {
        Some(Value::Array(lines)) => {
            for line in lines.iter().filter_map(Value::as_str) {
                push_header(&mut out, line)?;
            }
        }
        Some(Value::String(line)) => push_header(&mut out, line)?,
        _ => {}
    }
    if let Some(referer) = text("referer") {
        out.headers.push(("Referer".into(), referer.into()));
    }
    if let Some(agent) = text("user-agent") {
        out.headers.push(("User-Agent".into(), agent.into()));
    }
    if let Some(url) = text("all-proxy")
        .or_else(|| text("http-proxy"))
        .or_else(|| text("https-proxy"))
    {
        out.proxy = Some(ProxyOptions {
            url: url.into(),
            username: text("all-proxy-user").map(str::to_owned),
            password: text("all-proxy-passwd").map(str::to_owned),
        });
    }
    Ok(out)
}

fn push_header(out: &mut RequestOptions, line: &str) -> Result<()> {
    let (name, value) = line
        .split_once(':')
        .ok_or_else(|| anyhow!("malformed header line"))?;
    if name.trim().is_empty()
        || name.len() > 100
        || value.len() > 16 * 1024
        || out.headers.len() >= 64
    {
        return Err(anyhow!("header line rejected"));
    }
    out.headers
        .push((name.trim().to_owned(), value.trim().to_owned()));
    Ok(())
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
