//! ED2K (`ed2k://|file|…|/`) tasks for the aria2-compatible JSON-RPC server.
//!
//! Sources come from, in order: `x-nibiru-ed2k-source=host:port[,host:port]` given with the task,
//! the configured ED2K servers and the engine's own Kad session. Data is verified part by part by
//! the transfer layer, so a restored task re-hashes whatever is already on disk before trusting it.

use super::session::{self, Entry};
use crate::ed2k::{
    CallbackClient, FileLink, FileSource, IncomingListener, ServerConnection, TransferOptions,
    TransferProgress, TransferStage, download_with_options, kad_session::KadSession,
    metadata::Server, obfuscation::Obfuscation,
};
use anyhow::{Result, anyhow, bail, ensure};
use serde_json::{Map, Value, json};
use std::{
    collections::{HashMap, HashSet},
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

const MAX_EXTRA_SOURCES: usize = 64;
const SOURCE_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(25);

pub struct Ed2kConfig {
    pub default_dir: PathBuf,
    /// Our user hash; also what Kad and servers see as our identity.
    pub identity: [u8; 16],
    pub tcp_port: u16,
    pub servers: Vec<Server>,
    pub kad: Option<Arc<KadSession>>,
    pub obfuscation: Obfuscation,
}

struct Task {
    created: Instant,
    link: FileLink,
    dir: PathBuf,
    options: Map<String, Value>,
    extra_sources: Vec<FileSource>,
    obfuscation: Obfuscation,
    status: &'static str,
    total: u64,
    completed: u64,
    speed: u64,
    connections: usize,
    error: Option<String>,
    cancel: Arc<AtomicBool>,
}

pub struct Ed2kService {
    config: Ed2kConfig,
    /// Accepts incoming peers: required for High-ID, server callbacks and (later) uploads.
    listener: std::sync::OnceLock<Arc<IncomingListener>>,
    /// Kad session started after the listener, so it announces the port really in use.
    kad_late: std::sync::OnceLock<Arc<KadSession>>,
    /// The ED2K server session kept open for callbacks; one lookup at a time uses it.
    server: tokio::sync::Mutex<Option<ServerConnection>>,
    tasks: Mutex<HashMap<String, Task>>,
    changed: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

pub fn is_ed2k(uri: &str) -> bool {
    uri.get(..7)
        .is_some_and(|p| p.eq_ignore_ascii_case("ed2k://"))
}

fn parse_sources(options: Option<&Map<String, Value>>) -> Result<Vec<FileSource>> {
    let Some(text) = options
        .and_then(|o| o.get("x-nibiru-ed2k-source"))
        .and_then(Value::as_str)
    else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for item in text.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        // `ip:port` or `ip:port#<32 hex user hash>`; the hash marks a peer that supports obfuscation.
        let (endpoint, hash) = match item.split_once('#') {
            Some((endpoint, hash)) => (endpoint, Some(crate::ed2k::user_hash(hash)?)),
            None => (item, None),
        };
        let addr: SocketAddr = endpoint
            .parse()
            .map_err(|_| anyhow!("invalid ED2K source (expected ipv4:port[#userhash])"))?;
        let SocketAddr::V4(addr) = addr else {
            bail!("ED2K sources must be IPv4");
        };
        ensure!(addr.port() != 0, "invalid ED2K source port");
        if seen.insert(addr) {
            let mut source = FileSource::direct(*addr.ip(), addr.port());
            if let Some(hash) = hash {
                source.user_hash = Some(hash);
                source.crypt = 0x01;
            }
            out.push(source);
        }
    }
    ensure!(out.len() <= MAX_EXTRA_SOURCES, "too many ED2K sources");
    Ok(out)
}

impl Ed2kService {
    pub fn new(config: Ed2kConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            listener: std::sync::OnceLock::new(),
            kad_late: std::sync::OnceLock::new(),
            server: tokio::sync::Mutex::new(None),
            tasks: Mutex::new(HashMap::new()),
            changed: Mutex::new(None),
        })
    }

    /// Opens the incoming ED2K port (falling back to a free one) and returns the port in use.
    pub async fn start_listener(&self) -> Result<u16> {
        if let Some(listener) = self.listener.get() {
            return Ok(listener.status().tcp_port);
        }
        let listener = IncomingListener::bind(self.config.identity, self.config.tcp_port).await?;
        listener.set_obfuscation(self.config.obfuscation);
        let port = listener.status().tcp_port;
        let _ = self.listener.set(Arc::new(listener));
        Ok(port)
    }

    /// The port announced to servers: the real one once the listener is open.
    /// The TCP port peers are told to connect to (the listener's real port once started).
    pub fn announced_port(&self) -> u16 {
        self.listener
            .get()
            .map_or(self.config.tcp_port, |l| l.status().tcp_port)
    }

    /// Everything an operator wants to know about the ED2K side in one object.
    pub fn status(&self) -> Value {
        let server = self.server.try_lock().ok().and_then(|g| {
            g.as_ref().map(|c| {
                json!({"connected": c.is_connected(), "clientId": c.client_id, "lowId": c.is_low_id()})
            })
        });
        json!({
            "obfuscation": self.config.obfuscation,
            "incoming": self.incoming_status(),
            "server": server,
            "kad": self.kad_status(),
        })
    }

    pub fn incoming_status(&self) -> Option<Value> {
        self.listener
            .get()
            .and_then(|l| serde_json::to_value(l.status()).ok())
    }

    pub fn on_change(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        *self.changed.lock().unwrap() = Some(notify);
    }

    fn touch(&self) {
        if let Some(notify) = self.changed.lock().unwrap().clone() {
            notify();
        }
    }

    pub fn kad_status(&self) -> Option<Value> {
        self.kad()
            .and_then(|k| serde_json::to_value(k.status()).ok())
    }

    fn kad(&self) -> Option<Arc<KadSession>> {
        self.config
            .kad
            .clone()
            .or_else(|| self.kad_late.get().cloned())
    }

    /// Joins Kad on the TCP port the listener really has (call after `start_listener`).
    pub async fn start_kad(
        &self,
        seeds: Vec<crate::ed2k::metadata::Node>,
        state: PathBuf,
    ) -> Result<()> {
        let session = KadSession::start(
            self.config.identity,
            self.announced_port(),
            state,
            seeds,
            false,
        )
        .await
        .map_err(|e| anyhow!("cannot start Kad: {e}"))?;
        session.set_obfuscation(self.config.obfuscation != Obfuscation::Disabled);
        let _ = self.kad_late.set(session);
        Ok(())
    }

    pub fn owns(&self, gid: &str) -> bool {
        self.tasks.lock().unwrap().contains_key(gid)
    }

    fn build(&self, link: &str, options: Option<&Map<String, Value>>) -> Result<(FileLink, Task)> {
        let file = FileLink::parse(link)?;
        let dir = options
            .and_then(|o| o.get("dir"))
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .unwrap_or_else(|| self.config.default_dir.clone());
        let extra_sources = parse_sources(options)?;
        let persisted = session::persistable_options(None, options).unwrap_or_default();
        let task = Task {
            created: Instant::now(),
            link: file.clone(),
            dir,
            options: persisted,
            extra_sources,
            obfuscation: options
                .and_then(|o| o.get("x-nibiru-ed2k-obfuscation"))
                .and_then(Value::as_str)
                .map(str::parse)
                .transpose()?
                .unwrap_or(self.config.obfuscation),
            status: "paused",
            total: file.size,
            completed: 0,
            speed: 0,
            connections: 0,
            error: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        Ok((file, task))
    }

    pub fn add(
        self: &Arc<Self>,
        uri: &str,
        options: Option<&Map<String, Value>>,
    ) -> Result<String> {
        let (_, task) = self.build(uri, options)?;
        let gid = uuid::Uuid::new_v4().simple().to_string();
        self.tasks.lock().unwrap().insert(gid.clone(), task);
        self.start(&gid);
        Ok(gid)
    }

    pub fn restore(&self, entry: &Entry) -> Result<()> {
        let Entry::Ed2k { gid, link, options } = entry else {
            bail!("not an ED2K entry");
        };
        session::validate_gid(gid)?;
        let (_, task) = self.build(link, Some(options))?;
        let mut tasks = self.tasks.lock().unwrap();
        ensure!(!tasks.contains_key(gid), "duplicate saved gid");
        tasks.insert(gid.clone(), task);
        Ok(())
    }

    pub fn snapshot(&self) -> Vec<Entry> {
        let mut options_with_dir = Vec::new();
        for (gid, task) in self.tasks.lock().unwrap().iter() {
            if matches!(task.status, "complete" | "removed") {
                continue;
            }
            let mut options = task.options.clone();
            options.insert("dir".into(), json!(task.dir.to_string_lossy()));
            options_with_dir.push(Entry::Ed2k {
                gid: gid.clone(),
                link: task.link.link(),
                options,
            });
        }
        options_with_dir
    }

    /// Launches (or relaunches) the transfer; a previous run is cancelled first.
    fn start(self: &Arc<Self>, gid: &str) {
        let (file, dir, extra, obfuscation, cancel) = {
            let mut tasks = self.tasks.lock().unwrap();
            let Some(task) = tasks.get_mut(gid) else {
                return;
            };
            task.cancel.store(true, Ordering::Release);
            task.cancel = Arc::new(AtomicBool::new(false));
            task.status = "active";
            task.error = None;
            task.speed = 0;
            (
                task.link.clone(),
                task.dir.clone(),
                task.extra_sources.clone(),
                task.obfuscation,
                task.cancel.clone(),
            )
        };
        self.touch();
        let service = self.clone();
        let gid = gid.to_owned();
        tokio::spawn(async move {
            let outcome = service
                .run(&gid, &file, &dir, extra, obfuscation, &cancel)
                .await;
            let mut tasks = service.tasks.lock().unwrap();
            if let Some(task) = tasks.get_mut(&gid) {
                // pause/remove already set their own state; a stale run must not undo it.
                if !Arc::ptr_eq(&task.cancel, &cancel) || task.status != "active" {
                    return;
                }
                task.speed = 0;
                task.connections = 0;
                match outcome {
                    Ok(()) => {
                        task.status = "complete";
                        task.completed = task.total;
                    }
                    Err(_) if cancel.load(Ordering::Acquire) => task.status = "paused",
                    Err(error) => {
                        task.status = "error";
                        task.error = Some(format!("{error:#}"));
                    }
                }
            }
            drop(tasks);
            service.touch();
        });
    }

    /// Sources from the user, the ED2K servers and Kad, plus the server session (kept open) that
    /// a Low-ID source's callback needs.
    async fn discover(
        &self,
        file: &FileLink,
        mut sources: Vec<FileSource>,
    ) -> (Vec<FileSource>, Option<ServerConnection>) {
        let mut seen: HashSet<(Option<Ipv4Addr>, u16)> =
            sources.iter().map(|s| (s.host, s.port)).collect();
        let hash = file.hash_hex();
        let mut found = Vec::new();
        let mut connection = None;
        // Serialised: the server session answers one source lookup at a time.
        let mut guard = self.server.lock().await;
        if guard.as_ref().is_some_and(|c| !c.is_connected()) {
            *guard = None;
        }
        for server in self.config.servers.iter().take(3) {
            let attempt = async {
                let existing = guard.clone();
                let connection = match existing {
                    Some(connection) => connection,
                    None => {
                        ServerConnection::connect_with(
                            server.clone(),
                            self.config.identity,
                            "nibiru-xtreme",
                            self.announced_port(),
                            self.config.obfuscation,
                        )
                        .await?
                    }
                };
                let list = connection.sources(&hash).await?;
                Ok::<_, anyhow::Error>((connection, list))
            };
            match tokio::time::timeout(SOURCE_DISCOVERY_TIMEOUT, attempt).await {
                Ok(Ok((open, list))) => {
                    if let Some(listener) = self.listener.get() {
                        listener.set_client_id(open.client_id);
                    }
                    *guard = Some(open.clone());
                    connection = Some(open);
                    found.extend(list);
                    if !found.is_empty() {
                        break;
                    }
                }
                _ => *guard = None,
            }
        }
        drop(guard);
        if let Some(kad) = self.kad()
            && let Ok(Ok(list)) =
                tokio::time::timeout(SOURCE_DISCOVERY_TIMEOUT, kad.sources(file.hash, file.size))
                    .await
        {
            found.extend(list);
        }
        for source in found {
            if seen.insert((source.host, source.port)) {
                sources.push(source);
            }
        }
        (sources, connection)
    }

    async fn run(
        &self,
        gid: &str,
        file: &FileLink,
        dir: &std::path::Path,
        extra: Vec<FileSource>,
        obfuscation: Obfuscation,
        cancel: &Arc<AtomicBool>,
    ) -> Result<()> {
        let (sources, server) = self.discover(file, extra).await;
        // A Low-ID source is reached by asking the server to make it call us back, which needs our
        // listener and a High-ID server session.
        let callbacks = self.listener.get().and_then(|listener| {
            let registry = listener.callbacks();
            let port = listener.status().tcp_port;
            let by_server = server
                .as_ref()
                .filter(|server| !server.is_low_id())
                .map(|server| CallbackClient::new(server.clone(), registry.clone()));
            match (by_server, self.kad()) {
                (Some(client), Some(kad)) => Some(client.with_kad(kad, port)),
                (Some(client), None) => Some(client),
                (None, Some(kad)) => Some(CallbackClient::kad_only(registry, kad, port)),
                (None, None) => None,
            }
        });
        let client_id = server.as_ref().map_or(0, |s| s.client_id);
        ensure!(
            cancel.load(Ordering::Acquire) || !sources.is_empty(),
            "no ED2K sources found"
        );
        let mut last = (Instant::now(), 0u64);
        let progress_cancel = cancel.clone();
        let result = download_with_options(
            file,
            dir,
            &sources,
            TransferOptions {
                identity: self.config.identity,
                client_id,
                callbacks,
                obfuscation,
            },
            cancel.clone(),
            |p: TransferProgress| {
                let now = Instant::now();
                let mut tasks = self.tasks.lock().unwrap();
                let Some(task) = tasks.get_mut(gid) else {
                    return;
                };
                if !Arc::ptr_eq(&task.cancel, &progress_cancel) || task.status != "active" {
                    return;
                }
                task.total = p.total;
                task.completed = p.verified;
                task.connections =
                    usize::from(p.peer.is_some() && !matches!(p.stage, TransferStage::Completed));
                let elapsed = now.duration_since(last.0);
                if elapsed >= Duration::from_secs(1) {
                    task.speed =
                        (p.verified.saturating_sub(last.1) as f64 / elapsed.as_secs_f64()) as u64;
                    last = (now, p.verified);
                }
            },
        )
        .await;
        result.map(|_| ())
    }

    pub fn pause(&self, gid: &str) -> Result<()> {
        let mut tasks = self.tasks.lock().unwrap();
        let task = tasks.get_mut(gid).ok_or_else(|| anyhow!("unknown gid"))?;
        if task.status == "active" {
            task.cancel.store(true, Ordering::Release);
            task.status = "paused";
            task.speed = 0;
        }
        drop(tasks);
        self.touch();
        Ok(())
    }

    pub fn resume(self: &Arc<Self>, gid: &str) -> Result<()> {
        {
            let tasks = self.tasks.lock().unwrap();
            let task = tasks.get(gid).ok_or_else(|| anyhow!("unknown gid"))?;
            ensure!(
                matches!(task.status, "paused" | "error"),
                "task is not paused"
            );
        }
        self.start(gid);
        Ok(())
    }

    pub fn remove(&self, gid: &str) -> Result<()> {
        let mut tasks = self.tasks.lock().unwrap();
        let task = tasks.get_mut(gid).ok_or_else(|| anyhow!("unknown gid"))?;
        task.cancel.store(true, Ordering::Release);
        task.status = "removed";
        task.speed = 0;
        drop(tasks);
        self.touch();
        Ok(())
    }

    pub fn remove_result(&self, gid: &str) -> Result<()> {
        let mut tasks = self.tasks.lock().unwrap();
        let task = tasks.get(gid).ok_or_else(|| anyhow!("unknown gid"))?;
        ensure!(
            matches!(task.status, "complete" | "error" | "removed"),
            "download is not stopped"
        );
        tasks.remove(gid);
        Ok(())
    }

    pub fn tell_status(&self, gid: &str, keys: Option<&[String]>) -> Result<Value> {
        let tasks = self.tasks.lock().unwrap();
        let task = tasks.get(gid).ok_or_else(|| anyhow!("unknown gid"))?;
        let path = task.dir.join(&task.link.name);
        let mut status = json!({
            "gid": gid,
            "status": task.status,
            "totalLength": task.total.to_string(),
            "completedLength": task.completed.to_string(),
            "downloadSpeed": task.speed.to_string(),
            "uploadSpeed": "0",
            "connections": task.connections.to_string(),
            "errorMessage": task.error.clone().unwrap_or_default(),
            "dir": task.dir.to_string_lossy(),
            "files": [{
                "index": "1",
                "path": path.to_string_lossy(),
                "length": task.total.to_string(),
                "completedLength": task.completed.to_string(),
                "selected": "true",
                "uris": [{"uri": task.link.link(), "status": if task.status == "active" { "used" } else { "waiting" }}]
            }]
        });
        if let (Some(keys), Value::Object(map)) = (keys.filter(|k| !k.is_empty()), &mut status) {
            map.retain(|key, _| key == "gid" || keys.iter().any(|k| k == key));
        }
        Ok(status)
    }

    pub fn list_ids(&self) -> Vec<String> {
        self.tasks.lock().unwrap().keys().cloned().collect()
    }

    pub fn created(&self, gid: &str) -> Option<Instant> {
        self.tasks.lock().unwrap().get(gid).map(|t| t.created)
    }

    pub fn totals(&self) -> (usize, u64) {
        let tasks = self.tasks.lock().unwrap();
        let active = tasks.values().filter(|t| t.status == "active").count();
        (active, tasks.values().map(|t| t.speed).sum())
    }

    pub fn counts(&self) -> (usize, usize) {
        let tasks = self.tasks.lock().unwrap();
        let waiting = tasks.values().filter(|t| t.status == "paused").count();
        let stopped = tasks
            .values()
            .filter(|t| matches!(t.status, "complete" | "error" | "removed"))
            .count();
        (waiting, stopped)
    }

    pub fn shutdown(&self) {
        if let Ok(server) = self.server.try_lock()
            && let Some(server) = server.as_ref()
        {
            server.disconnect();
        }
        for task in self.tasks.lock().unwrap().values() {
            task.cancel.store(true, Ordering::Release);
        }
        if let Some(kad) = self.kad() {
            kad.stop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ed2k::transfer::tests::{fixture, fixture_file};

    fn service(dir: &std::path::Path) -> Arc<Ed2kService> {
        Ed2kService::new(Ed2kConfig {
            default_dir: dir.to_path_buf(),
            identity: [7; 16],
            tcp_port: 4662,
            servers: Vec::new(),
            kad: None,
            obfuscation: Obfuscation::Prefer,
        })
    }
    fn options(dir: &std::path::Path, port: u16) -> Map<String, Value> {
        let mut o = Map::new();
        o.insert("dir".into(), json!(dir.to_string_lossy()));
        o.insert(
            "x-nibiru-ed2k-source".into(),
            json!(format!("127.0.0.1:{port}")),
        );
        o
    }
    async fn wait_for(service: &Arc<Ed2kService>, gid: &str, status: &str) -> Value {
        for _ in 0..200 {
            let s = service.tell_status(gid, None).unwrap();
            if s["status"] == status {
                return s;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!(
            "task never reached {status}: {}",
            service.tell_status(gid, None).unwrap()
        );
    }

    #[tokio::test]
    async fn a_low_id_source_from_the_server_is_reached_through_a_callback() {
        use crate::ed2k::{metadata::Server, peer};
        use tokio::net::{TcpListener, TcpStream};
        let control = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let control_port = control.local_addr().unwrap().port();
        let file = FileLink {
            name: "callback.txt".into(),
            size: 3,
            hash: crate::ed2k::file::digest(b"abc"),
        };
        let served = file.clone();
        let dir = tempfile::tempdir().unwrap();
        let service = Ed2kService::new(Ed2kConfig {
            default_dir: dir.path().to_path_buf(),
            identity: [3; 16],
            tcp_port: 0,
            servers: vec![Server {
                host: "127.0.0.1".into(),
                port: control_port,
                name: "fixture".into(),
                description: String::new(),
            }],
            kad: None,
            obfuscation: Obfuscation::Disabled,
        });
        let port = service.start_listener().await.unwrap();
        assert_ne!(port, 0);
        let server = tokio::spawn(async move {
            let (mut server, _) = control.accept().await.unwrap();
            let (_, _, login) = peer::frame(&mut server).await.unwrap();
            // The login announces the listener port actually in use, not the configured 0.
            assert_eq!(&login[20..22], &port.to_le_bytes());
            peer::send(&mut server, 0xe3, 0x40, &0x04030201u32.to_le_bytes())
                .await
                .unwrap();
            let (_, op, asked) = peer::frame(&mut server).await.unwrap();
            assert_eq!((op, asked), (0x19, served.hash.to_vec()));
            // One Low-ID source: only a callback can reach it.
            let mut sources = served.hash.to_vec();
            sources.push(1);
            sources.extend(123u32.to_le_bytes());
            sources.extend(4662u16.to_le_bytes());
            peer::send(&mut server, 0xe3, 0x42, &sources).await.unwrap();
            let (_, op, id) = peer::frame(&mut server).await.unwrap();
            assert_eq!((op, id), (0x1c, 123u32.to_le_bytes().to_vec()));
            let mut client = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port))
                .await
                .unwrap();
            peer::send(&mut client, 0xe3, 1, &peer::hello([4; 16], 123))
                .await
                .unwrap();
            peer::frame(&mut client).await.unwrap();
            assert_eq!(peer::frame(&mut client).await.unwrap().1, 0x58);
            let mut name = served.hash.to_vec();
            name.extend(1u16.to_le_bytes());
            name.push(b'x');
            peer::send(&mut client, 0xe3, 0x59, &name).await.unwrap();
            assert_eq!(peer::frame(&mut client).await.unwrap().1, 0x54);
            peer::send(&mut client, 0xe3, 0x55, &[]).await.unwrap();
            assert_eq!(peer::frame(&mut client).await.unwrap().1, 0x47);
            let mut data = served.hash.to_vec();
            data.extend(0u32.to_le_bytes());
            data.extend(3u32.to_le_bytes());
            data.extend(b"abc");
            peer::send(&mut client, 0xe3, 0x46, &data).await.unwrap();
            // Keep the server session open until the task is done.
            tokio::time::sleep(Duration::from_secs(2)).await;
        });
        let gid = service.add(&file.link(), None).unwrap();
        wait_for(&service, &gid, "complete").await;
        assert_eq!(
            std::fs::read(dir.path().join("callback.txt")).unwrap(),
            b"abc"
        );
        let status = service.status();
        assert_eq!(status["server"]["clientId"], 0x04030201);
        assert_eq!(status["server"]["lowId"], false);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn a_direct_source_task_downloads_verifies_and_reports_aria2_status() {
        let data: Vec<u8> = (0..1_500_000usize).map(|i| (i % 253) as u8).collect();
        let (file, _) = fixture_file(&data);
        let (source, _, peer) = fixture(data.clone(), false, false).await;
        let dir = tempfile::tempdir().unwrap();
        let service = service(dir.path());
        let gid = service
            .add(&file.link(), Some(&options(dir.path(), source.port)))
            .unwrap();
        let done = wait_for(&service, &gid, "complete").await;
        assert_eq!(done["totalLength"], data.len().to_string());
        assert_eq!(done["completedLength"], data.len().to_string());
        assert_eq!(std::fs::read(dir.path().join(&file.name)).unwrap(), data);
        assert!(
            service.snapshot().is_empty(),
            "finished tasks are not persisted"
        );
        service.remove_result(&gid).unwrap();
        assert!(!service.owns(&gid));
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn without_any_source_the_task_fails_instead_of_hanging() {
        let (file, _) = fixture_file(b"nobody has this");
        let dir = tempfile::tempdir().unwrap();
        let service = service(dir.path());
        let gid = service.add(&file.link(), None).unwrap();
        let failed = wait_for(&service, &gid, "error").await;
        assert!(
            failed["errorMessage"]
                .as_str()
                .unwrap()
                .contains("no ED2K sources")
        );
        assert!(!dir.path().join(&file.name).exists());
    }

    #[tokio::test]
    async fn invalid_links_and_sources_are_rejected_before_a_task_exists() {
        let dir = tempfile::tempdir().unwrap();
        let service = service(dir.path());
        assert!(
            service
                .add(
                    "ed2k://|file|../x|10|00000000000000000000000000000000|/",
                    None
                )
                .is_err()
        );
        let (file, _) = fixture_file(b"abc");
        for bad in ["not-an-address", "[::1]:4662", "127.0.0.1:0"] {
            let mut o = Map::new();
            o.insert("x-nibiru-ed2k-source".into(), json!(bad));
            assert!(service.add(&file.link(), Some(&o)).is_err(), "{bad}");
        }
        assert!(service.list_ids().is_empty());
    }

    #[tokio::test]
    async fn pause_resume_and_restore_keep_the_gid_and_never_trust_old_bytes() {
        let data: Vec<u8> = (0..1_200_000usize).map(|i| (i % 251) as u8).collect();
        let (file, _) = fixture_file(&data);
        let dir = tempfile::tempdir().unwrap();
        // Dead source: the task stays failed, but its identity must survive a restart.
        let first = service(dir.path());
        let opts = options(dir.path(), 9);
        let gid = first.add(&file.link(), Some(&opts)).unwrap();
        wait_for(&first, &gid, "error").await;
        let saved = first.snapshot();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].gid(), gid);

        let second = service(dir.path());
        second.restore(&saved[0]).unwrap();
        assert_eq!(second.tell_status(&gid, None).unwrap()["status"], "paused");
        assert!(
            second.restore(&saved[0]).is_err(),
            "duplicate gid is refused"
        );

        // A real source appears: resume completes the very same task.
        let (source, _, peer) = fixture(data.clone(), false, false).await;
        let third = service(dir.path());
        let mut entry = saved[0].clone();
        if let Entry::Ed2k { options, .. } = &mut entry {
            options.insert(
                "x-nibiru-ed2k-source".into(),
                json!(format!("127.0.0.1:{}", source.port)),
            );
        }
        third.restore(&entry).unwrap();
        third.resume(&gid).unwrap();
        wait_for(&third, &gid, "complete").await;
        assert_eq!(std::fs::read(dir.path().join(&file.name)).unwrap(), data);
        peer.await.unwrap();
        assert!(third.resume(&gid).is_err());
    }

    #[tokio::test]
    async fn remove_stops_an_active_task_and_keeps_it_out_of_the_session() {
        let data = vec![9u8; 600_000];
        let (file, _) = fixture_file(&data);
        // A source that accepts and never answers keeps the task active.
        let hang = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = hang.local_addr().unwrap().port();
        let holder = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((s, _)) = hang.accept().await {
                held.push(s);
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let service = service(dir.path());
        let gid = service
            .add(&file.link(), Some(&options(dir.path(), port)))
            .unwrap();
        wait_for(&service, &gid, "active").await;
        service.pause(&gid).unwrap();
        assert_eq!(service.tell_status(&gid, None).unwrap()["status"], "paused");
        service.resume(&gid).unwrap();
        service.remove(&gid).unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let status = service.tell_status(&gid, None).unwrap();
        assert_eq!(status["status"], "removed");
        assert!(service.snapshot().is_empty());
        holder.abort();
    }
}
