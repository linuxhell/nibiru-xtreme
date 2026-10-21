//! BitTorrent and Magnet support for the aria2-compatible JSON-RPC server.

use super::session::{self, Entry};
use crate::bittorrent::{
    dht::DhtConfig,
    magnet::Magnet,
    metainfo::Torrent,
    session::{Session, SessionConfig},
    stream::StreamServer,
    torrent::{Settings, State, TorrentHandle, TorrentStatus},
};
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use serde_json::{Map, Value, json};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Debug)]
pub struct BtConfig {
    pub session: SessionConfig,
    /// aria2's `--listen-port=6881-6999`: the first free port in the range is used.
    pub port_range: Option<(u16, u16)>,
    pub seed_time: Option<Duration>,
    pub seed_ratio: Option<f64>,
    pub max_peers: usize,
    pub default_dir: PathBuf,
    pub allow_loopback_peers: bool,
    /// Trackerless peer discovery (aria2's `--enable-dht`).
    pub dht: bool,
    pub dht6: bool,
    /// Where known DHT nodes are kept between runs (`--dht-file-path`).
    pub dht_file: Option<PathBuf>,
    pub require_encryption: bool,
    pub enable_tcp: bool,
}

impl Default for BtConfig {
    fn default() -> Self {
        Self {
            session: SessionConfig::default(),
            port_range: None,
            seed_time: None,
            seed_ratio: Some(1.0),
            max_peers: 50,
            default_dir: PathBuf::from("."),
            allow_loopback_peers: false,
            dht: true,
            dht6: true,
            dht_file: None,
            require_encryption: false,
            enable_tcp: true,
        }
    }
}

struct BtTask {
    handle: TorrentHandle,
    metadata_only: bool,
    /// Set once the `.torrent` for a metadata-only task has been written.
    saved: Arc<Mutex<Option<PathBuf>>>,
    removed: bool,
    created: std::time::Instant,
    /// Magnet link the task was added from, until its metadata is known.
    magnet: Option<String>,
    /// Options safe to keep in the session file; `None` keeps the task out of it
    /// (it carries credentials).
    persist: Mutex<Option<Map<String, Value>>>,
}

pub struct BtService {
    session: Arc<Session>,
    stream: Arc<StreamServer>,
    config: BtConfig,
    tasks: Mutex<HashMap<String, BtTask>>,
}

fn gid() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..16].to_owned()
}

fn text<'a>(options: Option<&'a Map<String, Value>>, key: &str) -> Option<&'a str> {
    options?.get(key)?.as_str()
}

fn flag(options: Option<&Map<String, Value>>, key: &str) -> Option<bool> {
    match options?.get(key)? {
        Value::String(s) => Some(s == "true"),
        Value::Bool(b) => Some(*b),
        _ => None,
    }
}

/// `50K`, `1M`, `1024` (aria2 size syntax).
pub fn parse_size(value: &str) -> Option<u64> {
    let value = value.trim();
    let (digits, unit) = match value.char_indices().find(|(_, c)| !c.is_ascii_digit()) {
        Some((at, _)) => value.split_at(at),
        None => (value, ""),
    };
    let base: u64 = digits.parse().ok()?;
    let factor = match unit.to_ascii_uppercase().as_str() {
        "" => 1,
        "K" => 1024,
        "M" => 1024 * 1024,
        "G" => 1024 * 1024 * 1024,
        _ => return None,
    };
    base.checked_mul(factor)
}

pub fn parse_seed_time(value: &str) -> Result<Duration> {
    let minutes = value.parse::<f64>()?;
    anyhow::ensure!(
        minutes.is_finite() && (0.0..=525600.0).contains(&minutes),
        "invalid seed time"
    );
    Ok(Duration::from_secs_f64(minutes * 60.0))
}

/// aria2's 1-based `select-file=1,3-5` into zero-based indexes.
pub fn parse_select_file(value: &str, file_count: usize) -> Result<Vec<usize>> {
    let mut out = Vec::new();
    for part in value.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (from, to) = match part.split_once('-') {
            Some((a, b)) => (a.trim().parse::<usize>()?, b.trim().parse::<usize>()?),
            None => {
                let n = part.parse::<usize>()?;
                (n, n)
            }
        };
        if from == 0 || to < from || to > file_count {
            bail!("select-file index out of range");
        }
        out.extend(from - 1..to);
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

impl BtService {
    pub async fn start(config: BtConfig) -> Result<Arc<Self>> {
        let mut session_config = config.session.clone();
        if config.dht && session_config.dht.is_none() {
            session_config.dht = Some(DhtConfig {
                allow_loopback: config.allow_loopback_peers,
                ..DhtConfig::default()
            });
        }
        if config.dht && config.dht6 && session_config.dht6.is_none() {
            session_config.dht6 = Some(DhtConfig {
                bind: "::".parse().expect("IPv6 unspecified"),
                allow_loopback: config.allow_loopback_peers,
                ..DhtConfig::default()
            });
        }
        let session = match config.port_range {
            Some((first, last)) if session_config.listen_port == 0 => {
                let mut started = None;
                for port in first..=last {
                    session_config.listen_port = port;
                    if let Ok(session) = Session::start(session_config.clone()).await {
                        started = Some(session);
                        break;
                    }
                }
                match started {
                    Some(session) => session,
                    None => {
                        // Every port in the range is busy: any free port still works.
                        session_config.listen_port = 0;
                        Session::start(session_config).await?
                    }
                }
            }
            _ => Session::start(session_config).await?,
        };
        let stream = StreamServer::start(&session).await?;
        let persistence = [
            (session.dht().cloned(), config.dht_file.clone()),
            (
                session.dht6().cloned(),
                config.dht_file.as_ref().map(|p| p.with_extension("ipv6")),
            ),
        ];
        for (dht, file) in persistence {
            if let (Some(dht), Some(file)) = (dht, file) {
                // Known nodes make the next start independent of the public bootstrap routers.
                tokio::spawn(async move {
                    if let Ok(saved) = tokio::fs::read_to_string(&file).await {
                        let nodes: Vec<SocketAddr> = saved
                            .lines()
                            .filter_map(|l| l.trim().parse().ok())
                            .take(300)
                            .collect();
                        dht.add_nodes(&nodes).await;
                    }
                    loop {
                        tokio::time::sleep(Duration::from_secs(600)).await;
                        let nodes = dht.export_nodes();
                        if nodes.is_empty() {
                            continue;
                        }
                        let text: String = nodes.iter().map(|n| format!("{n}\n")).collect();
                        let temp = dht_checkpoint_path(&file);
                        if tokio::fs::write(&temp, text).await.is_ok() {
                            let _ = tokio::fs::rename(&temp, &file).await;
                        }
                    }
                });
            }
        }
        Ok(Arc::new(Self {
            session,
            stream,
            config,
            tasks: Mutex::new(HashMap::new()),
        }))
    }

    pub fn session(&self) -> &Arc<Session> {
        &self.session
    }

    pub fn listen_port(&self) -> u16 {
        self.session.port()
    }

    pub fn owns(&self, gid: &str) -> bool {
        self.tasks
            .lock()
            .map(|t| t.contains_key(gid))
            .unwrap_or(false)
    }

    fn settings(&self, options: Option<&Map<String, Value>>) -> Result<Settings> {
        let dir = text(options, "dir")
            .map(PathBuf::from)
            .unwrap_or_else(|| self.config.default_dir.clone());
        let mut settings = Settings::new(dir);
        settings.max_peers = self.config.max_peers;
        settings.allow_loopback_peers = self.config.allow_loopback_peers;
        settings.seed_time = self.config.seed_time;
        settings.seed_ratio = self.config.seed_ratio.filter(|r| *r > 0.0);
        settings.require_encryption = self.config.require_encryption
            || flag(options, "bt-force-encryption").unwrap_or(false)
            || flag(options, "bt-require-crypto").unwrap_or(false);
        settings.enable_tcp = self.config.enable_tcp;
        settings.enable_utp = self.config.session.enable_utp;
        if let Some(time) = text(options, "seed-time") {
            settings.seed_time = Some(parse_seed_time(time)?);
        }
        if let Some(ratio) = text(options, "seed-ratio") {
            let ratio = ratio.parse::<f64>()?;
            anyhow::ensure!(ratio.is_finite() && ratio >= 0.0, "invalid seed ratio");
            settings.seed_ratio = (ratio > 0.0).then_some(ratio);
        }
        if let Some(n) = text(options, "bt-max-peers").and_then(|v| v.parse::<usize>().ok()) {
            settings.max_peers = if n == 0 { 200 } else { n.min(200) };
        }
        // Prefer the beginning and the end first (what bt-prioritize-piece=head,tail asks for).
        settings.sequential = flag(options, "x-nibiru-sequential").unwrap_or(false);
        Ok(settings)
    }

    fn apply_limits(&self, handle: &TorrentHandle, options: Option<&Map<String, Value>>) {
        if let Some(limit) = text(options, "max-download-limit").and_then(parse_size) {
            handle.set_download_limit(limit);
        }
        if let Some(limit) = text(options, "max-upload-limit").and_then(parse_size) {
            handle.set_upload_limit(limit);
        }
    }

    fn register(
        &self,
        id: Option<String>,
        handle: TorrentHandle,
        metadata_only: bool,
        magnet: Option<String>,
        persist: Option<Map<String, Value>>,
    ) -> String {
        let id = id.unwrap_or_else(gid);
        let saved = Arc::new(Mutex::new(None));
        if let Ok(mut tasks) = self.tasks.lock() {
            tasks.insert(
                id.clone(),
                BtTask {
                    handle,
                    metadata_only,
                    saved,
                    removed: false,
                    created: std::time::Instant::now(),
                    magnet,
                    persist: Mutex::new(persist),
                },
            );
        }
        id
    }

    pub async fn add_magnet(
        &self,
        uri: &str,
        options: Option<&Map<String, Value>>,
    ) -> Result<String> {
        self.add_magnet_as(None, uri, options).await
    }

    async fn add_magnet_as(
        &self,
        id: Option<String>,
        uri: &str,
        options: Option<&Map<String, Value>>,
    ) -> Result<String> {
        let magnet = Magnet::parse(uri)?;
        let mut settings = self.settings(options)?;
        let metadata_only = flag(options, "bt-metadata-only").unwrap_or(false);
        if metadata_only {
            // Fetch the description only; never create or touch payload files.
            settings.selected = Some(Vec::new());
        }
        let dir = settings.directory.clone();
        let handle = self.session.add_magnet(&magnet, settings);
        self.apply_limits(&handle, options);
        // Metadata-only fetches are transient; everything else may be restored.
        let persist = (!metadata_only)
            .then(|| session::persistable_options(Some(uri), options))
            .flatten();
        let id = self.register(
            id,
            handle.clone(),
            metadata_only,
            Some(uri.to_owned()),
            persist,
        );
        if metadata_only {
            let save = flag(options, "bt-save-metadata").unwrap_or(false);
            let saved = self
                .tasks
                .lock()
                .ok()
                .and_then(|t| t.get(&id).map(|t| t.saved.clone()));
            if let Some(saved) = saved {
                tokio::spawn(async move {
                    let Ok(torrent) = handle.wait_metadata(Duration::from_secs(24 * 3600)).await
                    else {
                        return;
                    };
                    let path = if save {
                        match write_metadata(&dir, &torrent).await {
                            Ok(path) => path,
                            Err(_) => return,
                        }
                    } else {
                        dir.join(format!("{}.torrent", hex(&torrent.info_hash)))
                    };
                    handle.shutdown(false);
                    if let Ok(mut slot) = saved.lock() {
                        *slot = Some(path);
                    }
                });
            }
        }
        Ok(id)
    }

    pub async fn add_torrent(
        &self,
        base64_torrent: &str,
        options: Option<&Map<String, Value>>,
    ) -> Result<String> {
        self.add_torrent_as(None, base64_torrent, options).await
    }

    async fn add_torrent_as(
        &self,
        id: Option<String>,
        base64_torrent: &str,
        options: Option<&Map<String, Value>>,
    ) -> Result<String> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(base64_torrent.trim())
            .context("the torrent is not valid base64")?;
        let torrent = Torrent::from_torrent_file(&bytes)?;
        let mut settings = self.settings(options)?;
        if let Some(select) = text(options, "select-file") {
            settings.selected = Some(parse_select_file(select, torrent.files.len())?);
        }
        let handle = self.session.add_torrent(torrent, settings);
        self.apply_limits(&handle, options);
        let persist = session::persistable_options(None, options);
        Ok(self.register(id, handle, false, None, persist))
    }

    /// Recreates a task from the session file under its old GID, paused: the manager
    /// decides when to resume, and resuming re-verifies the payload on disk.
    pub async fn restore(&self, entry: &Entry) -> Result<()> {
        session::validate_gid(entry.gid())?;
        anyhow::ensure!(!self.owns(entry.gid()), "duplicate saved gid");
        let id = match entry {
            Entry::Torrent {
                gid,
                torrent,
                options,
            } => {
                self.add_torrent_as(Some(gid.clone()), torrent, Some(options))
                    .await?
            }
            Entry::Magnet { gid, uri, options } => {
                self.add_magnet_as(Some(gid.clone()), uri, Some(options))
                    .await?
            }
            Entry::Direct { .. } | Entry::Ed2k { .. } => bail!("not a BitTorrent entry"),
        };
        self.pause(&id)
    }

    /// Unfinished, credential-free tasks for the session file.
    pub fn snapshot(&self) -> Vec<Entry> {
        let Ok(tasks) = self.tasks.lock() else {
            return Vec::new();
        };
        let mut entries = Vec::new();
        for (gid, task) in tasks.iter() {
            if task.removed || task.metadata_only {
                continue;
            }
            let Some(options) = task.persist.lock().ok().and_then(|p| p.clone()) else {
                continue;
            };
            let status = task.handle.status();
            let finished = matches!(status.state, State::Complete | State::Seeding)
                || (status.selected_length > 0
                    && status.completed_length >= status.selected_length);
            if finished {
                continue;
            }
            let entry = match (task.handle.torrent(), &task.magnet) {
                (Some(torrent), _) => Entry::Torrent {
                    gid: gid.clone(),
                    torrent: base64::engine::general_purpose::STANDARD
                        .encode(torrent.to_torrent_file()),
                    options,
                },
                (None, Some(uri)) => Entry::Magnet {
                    gid: gid.clone(),
                    uri: uri.clone(),
                    options,
                },
                (None, None) => continue,
            };
            entries.push(entry);
        }
        entries
    }

    /// `(waiting, stopped)` task counts for the global stat call.
    pub fn counts(&self) -> (usize, usize) {
        let Ok(tasks) = self.tasks.lock() else {
            return (0, 0);
        };
        let (mut waiting, mut stopped) = (0, 0);
        for task in tasks.values() {
            let saved = task.saved.lock().ok().and_then(|s| s.clone());
            match aria_status(&task.handle.status(), task, saved.as_deref()) {
                "paused" => waiting += 1,
                "complete" | "error" | "removed" => stopped += 1,
                _ => {}
            }
        }
        (waiting, stopped)
    }

    fn with_task<T>(&self, gid: &str, f: impl FnOnce(&BtTask) -> T) -> Result<T> {
        let tasks = self
            .tasks
            .lock()
            .map_err(|_| anyhow!("task table poisoned"))?;
        tasks
            .get(gid)
            .map(f)
            .ok_or_else(|| anyhow!("GID {gid} is not found"))
    }

    pub fn pause(&self, gid: &str) -> Result<()> {
        self.with_task(gid, |t| t.handle.pause())
    }

    pub fn resume(&self, gid: &str) -> Result<()> {
        self.with_task(gid, |t| t.handle.resume())
    }

    /// Stops the transfer; the entry stays visible as `removed` until `remove_result`.
    pub fn remove(&self, gid: &str) -> Result<()> {
        let mut tasks = self
            .tasks
            .lock()
            .map_err(|_| anyhow!("task table poisoned"))?;
        let task = tasks
            .get_mut(gid)
            .ok_or_else(|| anyhow!("GID {gid} is not found"))?;
        if !task.removed {
            task.removed = true;
            self.session.remove_instance(&task.handle);
        }
        Ok(())
    }

    pub fn remove_result(&self, gid: &str) -> Result<()> {
        let mut tasks = self
            .tasks
            .lock()
            .map_err(|_| anyhow!("task table poisoned"))?;
        if let Some(task) = tasks.get(gid) {
            let saved = task.saved.lock().ok().and_then(|s| s.clone());
            anyhow::ensure!(
                matches!(
                    aria_status(&task.handle.status(), task, saved.as_deref()),
                    "complete" | "removed" | "error"
                ),
                "cannot purge an unfinished torrent"
            );
        }
        if let Some(task) = tasks.remove(gid) {
            self.session.remove_instance(&task.handle);
        }
        Ok(())
    }

    pub fn change_option(&self, gid: &str, options: &Map<String, Value>) -> Result<()> {
        self.with_task(gid, |task| -> Result<()> {
            if let Ok(mut saved) = task.persist.lock()
                && let Some(saved) = saved.as_mut()
            {
                for key in [
                    "max-download-limit",
                    "max-upload-limit",
                    "select-file",
                    "x-nibiru-sequential",
                ] {
                    if let Some(value) = options.get(key).and_then(Value::as_str) {
                        saved.insert(key.to_owned(), Value::String(value.to_owned()));
                    }
                }
            }
            let options = Some(options);
            self.apply_limits(&task.handle, options);
            if let Some(select) = text(options, "select-file") {
                let count = task
                    .handle
                    .status()
                    .files
                    .len()
                    .max(task.handle.torrent().map(|t| t.files.len()).unwrap_or(0));
                task.handle
                    .select_files(Some(parse_select_file(select, count)?));
            }
            if let Some(on) = flag(options, "x-nibiru-sequential") {
                task.handle.set_sequential(on);
            }
            Ok(())
        })?
    }

    pub fn change_global_option(&self, options: &Map<String, Value>) {
        let options = Some(options);
        let down = text(options, "max-overall-download-limit").and_then(parse_size);
        let up = text(options, "max-overall-upload-limit").and_then(parse_size);
        if let Some(limit) = down {
            self.session.set_download_limit(limit);
        }
        if let Some(limit) = up {
            self.session.set_upload_limit(limit);
        }
    }

    pub fn stream_url(&self, gid: &str, file: Option<usize>) -> Result<String> {
        let (hash, status) = self.with_task(gid, |t| (t.handle.info_hash(), t.handle.status()))?;
        let file = match file {
            Some(file) => file,
            // Default to the biggest file: that is the movie in nearly every torrent.
            None => status
                .files
                .iter()
                .max_by_key(|f| f.length)
                .map(|f| f.index)
                .ok_or_else(|| anyhow!("the torrent has no files yet"))?,
        };
        Ok(self.stream.url(&hash, file))
    }

    pub fn tell_status(&self, gid: &str, keys: Option<&[String]>) -> Result<Value> {
        let tasks = self
            .tasks
            .lock()
            .map_err(|_| anyhow!("task table poisoned"))?;
        let task = tasks
            .get(gid)
            .ok_or_else(|| anyhow!("GID {gid} is not found"))?;
        let saved = task.saved.lock().ok().and_then(|s| s.clone());
        let mut value = status_json(gid, task, saved.as_deref());
        if let (Some(keys), Value::Object(map)) = (keys.filter(|keys| !keys.is_empty()), &mut value)
        {
            map.retain(|key, _| keys.iter().any(|k| k == key) || key == "gid");
        }
        Ok(value)
    }

    pub fn files(&self, gid: &str) -> Result<Value> {
        let tasks = self
            .tasks
            .lock()
            .map_err(|_| anyhow!("task table poisoned"))?;
        let task = tasks
            .get(gid)
            .ok_or_else(|| anyhow!("GID {gid} is not found"))?;
        Ok(files_json(&task.handle.status(), task))
    }

    pub fn peers(&self, gid: &str) -> Result<Value> {
        let status = self.with_task(gid, |t| t.handle.status())?;
        Ok(Value::Array(
            status
                .peers
                .iter()
                .map(|p| {
                    let (ip, port) = p.address.rsplit_once(':').unwrap_or((&p.address, "0"));
                    json!({
                        "ip": ip.trim_matches(|c| c == '[' || c == ']'),
                        "port": port,
                        "downloadSpeed": p.download_speed.to_string(),
                        "uploadSpeed": p.upload_speed.to_string(),
                        "seeder": p.seeder.to_string(),
                        "peerChoking": p.choking_us.to_string(),
                    })
                })
                .collect(),
        ))
    }

    /// `(active, speeds)` for the global stat call.
    pub fn totals(&self) -> (usize, u64, u64) {
        let tasks = match self.tasks.lock() {
            Ok(t) => t,
            Err(_) => return (0, 0, 0),
        };
        let mut active = 0;
        let (mut down, mut up) = (0, 0);
        for task in tasks.values().filter(|t| !t.removed) {
            let s = task.handle.status();
            if aria_status(&s, task, None) == "active" {
                active += 1;
            }
            down += s.download_speed;
            up += s.upload_speed;
        }
        (active, down, up)
    }

    pub fn created(&self, gid: &str) -> Option<std::time::Instant> {
        self.tasks.lock().ok()?.get(gid).map(|t| t.created)
    }
    pub fn list_ids(&self) -> Vec<String> {
        self.tasks
            .lock()
            .map(|t| t.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn shutdown(&self) {
        self.session.shutdown();
    }
}

async fn write_metadata(dir: &Path, torrent: &Torrent) -> Result<PathBuf> {
    tokio::fs::create_dir_all(dir).await?;
    let target = dir.join(format!("{}.torrent", hex(&torrent.info_hash)));
    let bytes = torrent.to_torrent_file();
    tokio::task::spawn_blocking(move || {
        use std::io::Write;
        let mut temporary =
            tempfile::NamedTempFile::new_in(target.parent().context("metadata has no parent")?)?;
        temporary.write_all(&bytes)?;
        temporary.as_file().sync_all()?;
        temporary
            .persist(&target)
            .map_err(|error| anyhow!(error.error))?;
        Ok(target)
    })
    .await?
}

fn dht_checkpoint_path(file: &Path) -> PathBuf {
    let mut path = file.as_os_str().to_owned();
    path.push(".tmp");
    PathBuf::from(path)
}

#[cfg(test)]
mod audit_tests {
    use super::*;

    #[test]
    fn ipv4_and_ipv6_dht_checkpoints_never_share_a_temporary_path() {
        let ipv4 = Path::new("state/dht.dat");
        let ipv6 = ipv4.with_extension("ipv6");
        assert_ne!(dht_checkpoint_path(ipv4), dht_checkpoint_path(&ipv6));
        assert_ne!(dht_checkpoint_path(ipv4), ipv4);
    }

    #[tokio::test]
    async fn invalid_rpc_seeding_options_return_an_error_instead_of_panicking() {
        let service = BtService::start(BtConfig {
            dht: false,
            dht6: false,
            ..BtConfig::default()
        })
        .await
        .unwrap();
        let uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
        for options in [
            json!({"seed-time":"inf"}),
            json!({"seed-time":"NaN"}),
            json!({"seed-time":"1e300"}),
            json!({"seed-ratio":"NaN"}),
        ] {
            assert!(service.add_magnet(uri, options.as_object()).await.is_err());
        }
        assert!(service.list_ids().is_empty());
        service.shutdown();
    }
}

fn aria_status(status: &TorrentStatus, task: &BtTask, saved: Option<&Path>) -> &'static str {
    if task.removed {
        return "removed";
    }
    if task.metadata_only {
        return match (status.state, saved) {
            (_, Some(_)) => "complete",
            (State::Error, _) => "error",
            (State::Paused, _) => "paused",
            _ => "active",
        };
    }
    match status.state {
        State::Metadata | State::Checking | State::Downloading | State::Seeding => "active",
        State::Complete => "complete",
        State::Paused => "paused",
        State::Error => "error",
    }
}

fn files_json(status: &TorrentStatus, task: &BtTask) -> Value {
    Value::Array(
        status
            .files
            .iter()
            .map(|f| {
                json!({
                    "index": (f.index + 1).to_string(),
                    "path": task
                        .handle
                        .file_path(f.index)
                        .map(|p| p.to_string_lossy().into_owned())
                        .unwrap_or_else(|| f.path.clone()),
                    "length": f.length.to_string(),
                    "completedLength": f.completed.to_string(),
                    "selected": f.selected.to_string(),
                    "uris": status.web_seeds.iter().filter(|w| !w.failed).map(|w| json!({
                        "uri": w.url,
                        "status": if w.active { "used" } else { "waiting" },
                    })).collect::<Vec<_>>(),
                })
            })
            .collect(),
    )
}

fn status_json(gid: &str, task: &BtTask, saved: Option<&Path>) -> Value {
    let s = task.handle.status();
    let status = aria_status(&s, task, saved);
    let (total, completed) = if task.metadata_only {
        if saved.is_some() {
            let size = task
                .handle
                .torrent()
                .map(|t| t.info_bytes.len() as u64)
                .unwrap_or(0);
            (size, size)
        } else {
            (s.metadata_size, s.metadata_done)
        }
    } else {
        (s.selected_length, s.completed_length)
    };
    let verified = s
        .checking
        .map(|(done, _)| done as u64 * s.piece_length)
        .unwrap_or(0);
    let torrent = task.handle.torrent();
    let announce: Vec<Value> = torrent
        .as_ref()
        .map(|t| t.trackers.iter().map(|tier| json!(tier)).collect())
        .unwrap_or_default();
    let mut value = json!({
        "gid": gid,
        "status": status,
        "totalLength": total.to_string(),
        "completedLength": completed.to_string(),
        "uploadLength": s.uploaded.to_string(),
        "downloadSpeed": s.download_speed.to_string(),
        "uploadSpeed": s.upload_speed.to_string(),
        "connections": s.connections.to_string(),
        "numSeeders": s.seeders.to_string(),
        "seeder": (s.state == State::Seeding).to_string(),
        "infoHash": s.info_hash,
        "errorCode": if status == "error" { "1" } else { "0" },
        "errorMessage": s.error.clone().unwrap_or_default(),
        "dir": s.directory,
        "numPieces": s.pieces_total.to_string(),
        "pieceLength": s.piece_length.to_string(),
        "verifiedLength": verified.to_string(),
        "verifyIntegrityPending": (s.state == State::Checking).to_string(),
        "bittorrent": {
            "mode": if s.files.len() > 1 { "multi" } else { "single" },
            "announceList": announce,
            "info": { "name": s.name.clone().unwrap_or_default() },
        },
        "files": files_json(&s, task),
    });
    if let (Some(path), Value::Object(map)) = (saved, &mut value) {
        map.insert("metadataFile".into(), json!(path.to_string_lossy()));
    }
    value
}
