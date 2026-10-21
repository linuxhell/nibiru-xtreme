use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

pub mod bt;
pub mod ed2k;
pub mod session;

use self::session::{Entry, SessionStore};
use crate::{
    VERSION,
    bittorrent::limiter::RateLimiter,
    engine::{DownloadRequest, Engine, Progress, ProxyOptions, RequestOptions, safe_output_name},
    logging::LogSink,
};

#[derive(Clone)]
struct Task {
    created: std::time::Instant,
    io: Arc<tokio::sync::Mutex<()>>,
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
    /// Options safe to keep in the session file; `None` keeps the task out of it.
    persist: Option<serde_json::Map<String, Value>>,
}

#[derive(Clone)]
pub struct RpcServer {
    engine: Engine,
    log: Arc<LogSink>,
    secret: String,
    default_dir: PathBuf,
    tasks: Arc<RwLock<HashMap<String, Task>>>,
    bt: Option<Arc<bt::BtService>>,
    ed2k: Option<Arc<ed2k::Ed2kService>>,
    /// `max-overall-download-limit` for HTTP/FTP tasks (BitTorrent has its own session limit).
    global_limit: Arc<RateLimiter>,
    session: Option<Arc<SessionStore>>,
    /// Finished results the manager purged; makes `numStoppedTotal` cumulative.
    purged: Arc<AtomicU64>,
    /// Tasks restored from the session file, and entries that could not be.
    restored: Arc<AtomicUsize>,
    skipped: Arc<AtomicUsize>,
}

impl RpcServer {
    pub fn new(engine: Engine, log: Arc<LogSink>, secret: String) -> Self {
        Self {
            engine,
            log,
            secret,
            default_dir: PathBuf::from("."),
            tasks: Arc::new(RwLock::new(HashMap::new())),
            bt: None,
            ed2k: None,
            global_limit: Arc::new(RateLimiter::new(0)),
            session: None,
            purged: Arc::new(AtomicU64::new(0)),
            restored: Arc::new(AtomicUsize::new(0)),
            skipped: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn with_default_dir(mut self, directory: PathBuf) -> Self {
        self.default_dir = directory;
        self
    }

    /// Applies the launcher's speed cap before any restored or new HTTP task starts.
    pub fn with_global_download_limit(self, bytes_per_second: u64) -> Self {
        self.global_limit.set_rate(bytes_per_second);
        self
    }

    /// Keeps unfinished tasks in `path`: restores them (paused, same GIDs) now and
    /// rewrites the file whenever the table changes.
    pub async fn with_session(mut self, path: PathBuf) -> Result<Self> {
        let store = Arc::new(SessionStore::new(path));
        let (entries, skipped) = store.load()?;
        self.session = Some(store.clone());
        if let Some(ed2k) = &self.ed2k {
            let wake = store.clone();
            ed2k.on_change(Arc::new(move || wake.request_flush()));
        }
        self.skipped.store(skipped, Ordering::Release);
        for entry in entries {
            match self.restore_entry(&entry).await {
                Ok(()) => {
                    self.restored.fetch_add(1, Ordering::AcqRel);
                }
                Err(error) => {
                    self.skipped.fetch_add(1, Ordering::AcqRel);
                    self.log.warn(
                        "session.restore_failed",
                        &format!("gid={} error={error}", entry.gid()),
                    );
                }
            }
        }
        self.log.info(
            "session.restored",
            &format!(
                "tasks={} skipped={}",
                self.restored.load(Ordering::Acquire),
                self.skipped.load(Ordering::Acquire)
            ),
        );
        let writer = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = store.flush_requested() => {
                        // Coalesce bursts (a batch of adds) into one write.
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    }
                    // Completions and failures change the table without an RPC call.
                    _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                }
                if let Err(error) = writer.flush_session().await {
                    writer.log.warn("session.write_failed", &error.to_string());
                }
            }
        });
        Ok(self)
    }

    /// Writes the session file now if its content changed.
    pub async fn flush_session(&self) -> Result<bool> {
        let Some(store) = self.session.clone() else {
            return Ok(false);
        };
        let entries = self.snapshot().await;
        tokio::task::spawn_blocking(move || store.save(&entries)).await?
    }

    fn touch(&self) {
        if let Some(store) = &self.session {
            store.request_flush();
        }
    }

    async fn snapshot(&self) -> Vec<Entry> {
        let mut entries = Vec::new();
        for (gid, task) in self.tasks.read().await.iter() {
            if matches!(task.status.as_str(), "complete" | "removed") {
                continue;
            }
            if let Some(options) = &task.persist {
                entries.push(Entry::Direct {
                    gid: gid.clone(),
                    url: task.request.url.clone(),
                    options: options.clone(),
                });
            }
        }
        if let Some(bt) = &self.bt {
            entries.extend(bt.snapshot());
        }
        if let Some(ed2k) = &self.ed2k {
            entries.extend(ed2k.snapshot());
        }
        entries
    }

    async fn restore_entry(&self, entry: &Entry) -> Result<()> {
        match entry {
            Entry::Direct { gid, url, options } => {
                session::validate_gid(gid)?;
                let spec = direct_spec(url, Some(options), &self.default_dir)?;
                let mut tasks = self.tasks.write().await;
                anyhow::ensure!(!tasks.contains_key(gid), "duplicate saved gid");
                tasks.insert(
                    gid.clone(),
                    Task {
                        created: std::time::Instant::now(),
                        io: Arc::new(tokio::sync::Mutex::new(())),
                        status: "paused".into(),
                        total: 0,
                        completed: 0,
                        speed: 0,
                        error: None,
                        cancel: Arc::new(AtomicBool::new(false)),
                        request: spec.request,
                        transport: spec.transport,
                        http: spec.http,
                        limit: Arc::new(RateLimiter::new(spec.limit)),
                        persist: session::persistable_options(Some(url), Some(options)),
                    },
                );
                Ok(())
            }
            Entry::Ed2k { .. } => self
                .ed2k
                .as_ref()
                .ok_or_else(|| anyhow!("ED2K is not enabled"))?
                .restore(entry),
            other => self.bt()?.restore(other).await,
        }
    }

    /// Enables BitTorrent and Magnet downloads through this server.
    pub fn with_bittorrent(mut self, service: Arc<bt::BtService>) -> Self {
        self.bt = Some(service);
        self
    }

    /// Enables `ed2k://` file links (and Kad/server source discovery) through this server.
    pub fn with_ed2k(mut self, service: Arc<ed2k::Ed2kService>) -> Self {
        self.ed2k = Some(service);
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
        let permits = Arc::new(tokio::sync::Semaphore::new(64));
        loop {
            let permit = permits.clone().acquire_owned().await?;
            let (stream, peer) = listener.accept().await?;
            if !peer.ip().is_loopback() {
                continue;
            }
            let server = self.clone();
            tokio::spawn(async move {
                let _permit = permit;
                if let Ok(Err(error)) =
                    tokio::time::timeout(std::time::Duration::from_secs(30), server.handle(stream))
                        .await
                {
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
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length").then_some(value)
            })
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(0);
        if content_length > 1024 * 1024 {
            return Err(anyhow!("RPC body too large"));
        }
        buffer.resize(buffer.len().max(header_end + content_length), 0);
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
        let result = self.dispatch_inner(method, params).await;
        if matches!(
            method,
            "aria2.addUri"
                | "nibiru.addUri"
                | "aria2.addTorrent"
                | "nibiru.addTorrent"
                | "aria2.pause"
                | "aria2.forcePause"
                | "nibiru.pause"
                | "aria2.unpause"
                | "nibiru.resume"
                | "aria2.remove"
                | "aria2.forceRemove"
                | "nibiru.remove"
                | "aria2.removeDownloadResult"
                | "nibiru.removeResult"
                | "aria2.changeOption"
                | "nibiru.changeOption"
        ) {
            self.touch();
        }
        result
    }

    async fn dispatch_inner(&self, method: &str, params: Vec<Value>) -> Result<Value> {
        let gid = params.first().and_then(Value::as_str).unwrap_or_default();
        let bt_gid = self.bt.as_ref().filter(|bt| bt.owns(gid));
        let ed2k_gid = self.ed2k.as_ref().filter(|e| e.owns(gid));
        let options_at = |index: usize| params.get(index).and_then(Value::as_object);
        match method {
            "aria2.getVersion" | "nibiru.getVersion" => {
                let mut features = vec!["HTTP", "HTTPS", "HTTP/2", "WebDAV-GET", "RPC", "Resume"];
                if self.bt.is_some() {
                    features.extend([
                        "BitTorrent",
                        "BitTorrent-v2",
                        "Magnet",
                        "uTP",
                        "MSE",
                        "DHT-IPv4",
                        "DHT-IPv6",
                        "PEX",
                    ]);
                    if self
                        .bt
                        .as_ref()
                        .is_some_and(|bt| bt.session().lsd_enabled())
                    {
                        features.push("LSD");
                    }
                    if self
                        .bt
                        .as_ref()
                        .is_some_and(|bt| bt.session().utp_enabled())
                    {
                        features.push("HolePunch");
                    }
                }
                if self.ed2k.is_some() {
                    features.extend(["ED2K", "Kad2-sources"]);
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
                if let (Some(uri), Some(ed2k)) = (uri, self.ed2k.as_ref())
                    && ed2k::is_ed2k(uri)
                {
                    return Ok(Value::String(ed2k.add(uri, options_at(1))?));
                }
                self.add_uri(params).await
            }
            "aria2.addTorrent" | "nibiru.addTorrent" => {
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
                if let Some(ed2k) = ed2k_gid {
                    let keys: Option<Vec<String>> =
                        params.get(1).and_then(Value::as_array).map(|keys| {
                            keys.iter()
                                .filter_map(Value::as_str)
                                .map(str::to_owned)
                                .collect()
                        });
                    return ed2k.tell_status(gid, keys.as_deref());
                }
                self.tell_status(params).await
            }
            "aria2.pause" | "aria2.forcePause" | "nibiru.pause" => {
                if let Some(bt) = bt_gid {
                    bt.pause(gid)?;
                    return Ok(Value::String(gid.to_owned()));
                }
                if let Some(ed2k) = ed2k_gid {
                    ed2k.pause(gid)?;
                    return Ok(Value::String(gid.to_owned()));
                }
                self.pause(params).await
            }
            "aria2.unpause" | "nibiru.resume" => {
                if let Some(bt) = bt_gid {
                    bt.resume(gid)?;
                    return Ok(Value::String(gid.to_owned()));
                }
                if let Some(ed2k) = ed2k_gid {
                    ed2k.resume(gid)?;
                    return Ok(Value::String(gid.to_owned()));
                }
                self.resume(params).await
            }
            "aria2.remove" | "aria2.forceRemove" | "nibiru.remove" => {
                if let Some(bt) = bt_gid {
                    bt.remove(gid)?;
                    return Ok(Value::String(gid.to_owned()));
                }
                if let Some(ed2k) = ed2k_gid {
                    ed2k.remove(gid)?;
                    return Ok(Value::String(gid.to_owned()));
                }
                self.remove(params).await
            }
            "aria2.removeDownloadResult" | "nibiru.removeResult" => {
                if let Some(bt) = bt_gid {
                    bt.remove_result(gid)?;
                    self.purged.fetch_add(1, Ordering::AcqRel);
                } else if let Some(ed2k) = ed2k_gid {
                    ed2k.remove_result(gid)?;
                    self.purged.fetch_add(1, Ordering::AcqRel);
                } else {
                    let mut tasks = self.tasks.write().await;
                    if let Some(task) = tasks.get(gid) {
                        anyhow::ensure!(
                            matches!(task.status.as_str(), "complete" | "removed" | "error"),
                            "cannot purge an unfinished download"
                        );
                    }
                    if tasks.remove(gid).is_some() {
                        self.purged.fetch_add(1, Ordering::AcqRel);
                    }
                }
                Ok(Value::String("OK".into()))
            }
            "aria2.changeOption" | "nibiru.changeOption" => {
                if let (Some(bt), Some(options)) = (bt_gid, options_at(1)) {
                    bt.change_option(gid, options)?;
                } else if let Some(options) = options_at(1)
                    && let Some(limit) = options
                        .get("max-download-limit")
                        .and_then(Value::as_str)
                        .and_then(bt::parse_size)
                    && let Some(task) = self.tasks.write().await.get_mut(gid)
                {
                    task.limit.set_rate(limit);
                    if let Some(saved) = task.persist.as_mut()
                        && let Some(text) = options.get("max-download-limit")
                    {
                        saved.insert("max-download-limit".into(), text.clone());
                    }
                }
                Ok(Value::String("OK".into()))
            }
            "aria2.changeGlobalOption" | "nibiru.changeGlobalOption" => {
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
            "aria2.getFiles" | "nibiru.getFiles" => match bt_gid {
                Some(bt) => bt.files(gid),
                None => Ok(json!([])),
            },
            "aria2.getPeers" | "nibiru.getPeers" => match bt_gid {
                Some(bt) => bt.peers(gid),
                None => Ok(json!([])),
            },
            "aria2.getGlobalStat" | "nibiru.getGlobalStat" => {
                let (active, down, up) = self.bt.as_ref().map(|bt| bt.totals()).unwrap_or_default();
                let (ed2k_active, ed2k_speed) =
                    self.ed2k.as_ref().map(|e| e.totals()).unwrap_or_default();
                let (ed2k_waiting, ed2k_stopped) =
                    self.ed2k.as_ref().map(|e| e.counts()).unwrap_or_default();
                let http_active = self
                    .tasks
                    .read()
                    .await
                    .values()
                    .filter(|t| t.status == "active")
                    .count();
                let http_speed: u64 = self.tasks.read().await.values().map(|t| t.speed).sum();
                let (bt_waiting, bt_stopped) =
                    self.bt.as_ref().map(|bt| bt.counts()).unwrap_or_default();
                let (http_waiting, http_stopped) = {
                    let tasks = self.tasks.read().await;
                    (
                        tasks
                            .values()
                            .filter(|t| {
                                matches!(t.status.as_str(), "paused" | "pausing" | "waiting")
                            })
                            .count(),
                        tasks
                            .values()
                            .filter(|t| {
                                matches!(t.status.as_str(), "complete" | "error" | "removed")
                            })
                            .count(),
                    )
                };
                let stopped = bt_stopped + http_stopped + ed2k_stopped;
                let bt_waiting = bt_waiting + ed2k_waiting;
                let down = down + ed2k_speed;
                let active = active + ed2k_active;
                Ok(json!({
                    "downloadSpeed": (down + http_speed).to_string(),
                    "uploadSpeed": up.to_string(),
                    "numActive": (active + http_active).to_string(),
                    "numWaiting": (bt_waiting + http_waiting).to_string(),
                    "numStopped": stopped.to_string(),
                    "numStoppedTotal": (stopped as u64 + self.purged.load(Ordering::Acquire)).to_string(),
                }))
            }
            "aria2.tellActive" | "nibiru.tellActive" => self.list_tasks("active", params).await,
            "aria2.tellWaiting" | "nibiru.tellWaiting" => self.list_tasks("waiting", params).await,
            "aria2.tellStopped" | "nibiru.tellStopped" => self.list_tasks("stopped", params).await,
            // Not part of aria2: whether tasks survive a restart and what was restored.
            // Not part of aria2: ED2K listener, server session, obfuscation mode and Kad in one reply.
            "nibiru.getEd2kStatus" => self
                .ed2k
                .as_ref()
                .map(|e| e.status())
                .ok_or_else(|| anyhow!("ED2K is not enabled")),
            // Not part of aria2: state of the engine's own Kad session (absent when ED2K is off).
            "nibiru.getKadStatus" => self
                .ed2k
                .as_ref()
                .and_then(|e| e.kad_status())
                .ok_or_else(|| anyhow!("Kad is not enabled")),
            "nibiru.getSessionInfo" => Ok(json!({
                "persistent": self.session.is_some(),
                "restored": self.restored.load(Ordering::Acquire),
                "skipped": self.skipped.load(Ordering::Acquire),
            })),
            // Not part of aria2: a loopback URL a media player can open to watch while downloading.
            "nibiru.streamUrl" => {
                let file = params.get(1).and_then(Value::as_u64).map(|i| i as usize);
                Ok(Value::String(self.bt()?.stream_url(gid, file)?))
            }
            _ => Err(anyhow!("method not implemented: {method}")),
        }
    }

    async fn list_tasks(&self, kind: &str, params: Vec<Value>) -> Result<Value> {
        let matches = |state: &str| match kind {
            "active" => state == "active",
            "waiting" => matches!(state, "paused" | "pausing" | "waiting"),
            _ => matches!(state, "complete" | "error" | "removed"),
        };
        let mut rows = Vec::new();
        let ids = self
            .tasks
            .read()
            .await
            .iter()
            .map(|(gid, task)| (gid.clone(), task.created))
            .collect::<Vec<_>>();
        for (gid, created) in ids {
            if let Ok(status) = self.tell_status(vec![json!(gid)]).await
                && matches(status["status"].as_str().unwrap_or(""))
            {
                rows.push((created, status));
            }
        }
        if let Some(bt) = &self.bt {
            for gid in bt.list_ids() {
                if let (Some(created), Ok(status)) = (bt.created(&gid), bt.tell_status(&gid, None))
                    && matches(status["status"].as_str().unwrap_or(""))
                {
                    rows.push((created, status));
                }
            }
        }
        if let Some(ed2k) = &self.ed2k {
            for gid in ed2k.list_ids() {
                if let (Some(created), Ok(status)) =
                    (ed2k.created(&gid), ed2k.tell_status(&gid, None))
                    && matches(status["status"].as_str().unwrap_or(""))
                {
                    rows.push((created, status));
                }
            }
        }
        rows.sort_by_key(|(created, _)| *created);
        let (offset, count, keys) = if kind == "active" {
            (0, rows.len(), params.first())
        } else {
            let offset = params
                .first()
                .and_then(Value::as_i64)
                .ok_or_else(|| anyhow!("missing offset"))?;
            let count = params
                .get(1)
                .and_then(Value::as_u64)
                .ok_or_else(|| anyhow!("missing count"))?
                .min(10_000) as usize;
            if offset < 0 {
                rows.reverse();
                (
                    offset.unsigned_abs().saturating_sub(1) as usize,
                    count,
                    params.get(2),
                )
            } else {
                (offset as usize, count, params.get(2))
            }
        };
        let keys = keys
            .and_then(Value::as_array)
            .filter(|keys| !keys.is_empty());
        Ok(json!(
            rows.into_iter()
                .skip(offset)
                .take(count)
                .map(|(_, mut status)| {
                    if let (Some(keys), Value::Object(map)) = (keys, &mut status) {
                        map.retain(|key, _| {
                            key == "gid" || keys.iter().any(|k| k.as_str() == Some(key))
                        });
                    }
                    status
                })
                .collect::<Vec<_>>()
        ))
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
        let spec = direct_spec(&uri, options, &self.default_dir)?;
        let mut persist = session::persistable_options(Some(&uri), options);
        if let Some(saved) = &mut persist {
            saved.insert(
                "dir".into(),
                json!(
                    spec.request
                        .destination
                        .parent()
                        .unwrap_or(std::path::Path::new("."))
                ),
            );
        }
        let limit = Arc::new(RateLimiter::new(spec.limit));
        let io = Arc::new(tokio::sync::Mutex::new(()));
        let gid = uuid::Uuid::new_v4().simple().to_string();
        let cancel = Arc::new(AtomicBool::new(false));
        self.tasks.write().await.insert(
            gid.clone(),
            Task {
                created: std::time::Instant::now(),
                io: io.clone(),
                status: "active".into(),
                total: 0,
                completed: 0,
                speed: 0,
                error: None,
                cancel: cancel.clone(),
                request: spec.request.clone(),
                transport: spec.transport.clone(),
                http: spec.http.clone(),
                limit: limit.clone(),
                persist,
            },
        );
        self.spawn_download(
            gid.clone(),
            spec.request,
            cancel,
            spec.transport,
            spec.http,
            limit,
            io,
        );
        Ok(Value::String(gid))
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_download(
        &self,
        gid: String,
        request: DownloadRequest,
        cancel: Arc<AtomicBool>,
        transport: crate::protocols::TransportOptions,
        http: RequestOptions,
        limit: Arc<RateLimiter>,
        io: Arc<tokio::sync::Mutex<()>>,
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
                        if !Arc::ptr_eq(&task.cancel, &cancel) {
                            return;
                        }
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
            let _lease = io.lock().await;
            let progress_cancel = cancel.clone();
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
                        let progress_cancel = progress_cancel.clone();
                        tokio::spawn(async move {
                            if let Some(task) = tasks.write().await.get_mut(&gid) {
                                if task.status != "active"
                                    || !Arc::ptr_eq(&task.cancel, &progress_cancel)
                                {
                                    return;
                                }
                                task.completed = downloaded;
                                task.total = total.unwrap_or_default();
                                task.speed = speed_bytes_per_second;
                            }
                        });
                    },
                )
                .await;
            let completed_bytes = if result.is_ok() {
                tokio::fs::metadata(&request.destination)
                    .await
                    .ok()
                    .filter(|metadata| metadata.is_file())
                    .map(|metadata| metadata.len())
            } else {
                None
            };
            let mut guard = tasks.write().await;
            if let Some(task) = guard.get_mut(&gid) {
                if !Arc::ptr_eq(&task.cancel, &cancel) {
                    return;
                }
                match result {
                    Ok(()) => {
                        task.speed = 0;
                        if let Some(bytes) = completed_bytes {
                            task.completed = bytes;
                            task.total = bytes;
                            task.status = "complete".into();
                        } else {
                            task.status = "error".into();
                            task.error = Some("completed output is missing".into());
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
        let mut guard = self.tasks.write().await;
        let task = guard.get_mut(gid).ok_or_else(|| anyhow!("unknown gid"))?;
        if task.status == "active" {
            task.status = "pausing".into();
        }
        task.cancel.store(true, Ordering::Release);
        Ok(Value::String(gid.to_owned()))
    }

    async fn resume(&self, params: Vec<Value>) -> Result<Value> {
        let gid = params
            .first()
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing gid"))?
            .to_owned();
        let (request, cancel, transport, http, limit, io) = {
            let mut guard = self.tasks.write().await;
            let task = guard.get_mut(&gid).ok_or_else(|| anyhow!("unknown gid"))?;
            if !matches!(task.status.as_str(), "paused" | "pausing" | "error") {
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
                task.io.clone(),
            )
        };
        self.spawn_download(gid.clone(), request, cancel, transport, http, limit, io);
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

/// What `addUri` builds from a URL and aria2-style options; shared with session restore.
struct DirectSpec {
    request: DownloadRequest,
    transport: crate::protocols::TransportOptions,
    http: RequestOptions,
    limit: u64,
}

fn direct_spec(
    uri: &str,
    options: Option<&serde_json::Map<String, Value>>,
    default_dir: &std::path::Path,
) -> Result<DirectSpec> {
    let value = |key: &str| options.and_then(|m| m.get(key)).and_then(Value::as_str);
    let dir = value("dir")
        .map(PathBuf::from)
        .unwrap_or_else(|| default_dir.to_owned());
    let out = value("out").unwrap_or("download");
    if !safe_output_name(out) {
        return Err(anyhow!("invalid output filename"));
    }
    Ok(DirectSpec {
        request: DownloadRequest {
            url: uri.to_owned(),
            destination: dir.join(out),
        },
        transport: crate::protocols::TransportOptions {
            ca_certificate: value("ca-certificate").map(PathBuf::from),
            ssh_host_key: value("ssh-host-key").map(str::to_owned),
            ssh_identity: value("ssh-identity").map(PathBuf::from),
        },
        http: request_options(options)?,
        limit: value("max-download-limit")
            .and_then(bt::parse_size)
            .unwrap_or(0),
    })
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
    for key in ["split", "max-connection-per-server"] {
        if let Some(value) = text(key) {
            let n = value
                .parse::<usize>()
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| anyhow!("invalid {key}"))?;
            out.max_connections = Some(out.max_connections.map_or(n, |old| old.min(n)));
        }
    }
    if let Some(value) = text("min-split-size") {
        out.min_split_size = Some(
            bt::parse_size(value)
                .filter(|n| *n > 0)
                .ok_or_else(|| anyhow!("invalid min-split-size"))?,
        );
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
