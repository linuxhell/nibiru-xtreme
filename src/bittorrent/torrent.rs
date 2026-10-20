//! One torrent: shared state, lifecycle tasks (verify, connect, choke, announce, seed) and the
//! public handle used by the session and the RPC layer.

use super::{
    bitfield::Bitfield,
    limiter::{RateLimiter, RateMeter},
    message::{METADATA_PIECE, Message},
    metainfo::{Extras, Torrent},
    picker::{self, Mode, Picker},
    storage::Storage,
    tracker::{AnnounceRequest, Event, TrackerClient},
};
use anyhow::{Result, anyhow, bail};
use serde::Serialize;
use sha1::{Digest, Sha1};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Notify, mpsc};

pub const MAX_METADATA: usize = 8 * 1024 * 1024;
const MAX_CANDIDATES: usize = 400;
const CHOKE_INTERVAL: Duration = Duration::from_secs(10);
const PEX_INTERVAL: Duration = Duration::from_secs(60);
const STRIKES_TO_BAN: u8 = 4;

pub type PeerKey = u64;

#[derive(Clone, Debug)]
pub struct Settings {
    pub directory: PathBuf,
    /// File indexes to download; `None` selects everything.
    pub selected: Option<Vec<usize>>,
    pub sequential: bool,
    pub max_peers: usize,
    pub seed_ratio: Option<f64>,
    pub seed_time: Option<Duration>,
    pub enable_pex: bool,
    /// Accept peers on loopback addresses (tests, local seeders). Link-local is always refused.
    pub allow_loopback_peers: bool,
    /// Extra peers to try immediately (`x.pe`, manual add, tests).
    pub peers: Vec<SocketAddr>,
}

impl Settings {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
            selected: None,
            sequential: false,
            max_peers: 60,
            seed_ratio: Some(1.0),
            seed_time: None,
            enable_pex: true,
            allow_loopback_peers: false,
            peers: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum State {
    Metadata,
    Checking,
    Downloading,
    /// Every selected piece is on disk but we keep serving the swarm.
    Seeding,
    /// Finished and no longer connected (seed limits reached).
    Complete,
    Paused,
    Error,
}

#[derive(Debug)]
pub enum PeerCmd {
    Choke,
    Unchoke,
    Have(u32),
    /// Metadata arrived or verification finished: send our bitfield and recompute interest.
    Ready,
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
    Pex(Arc<Vec<SocketAddr>>),
    Shutdown,
}

pub struct PeerEntry {
    pub addr: SocketAddr,
    pub tx: mpsc::Sender<PeerCmd>,
    pub incoming: bool,
    pub client: Option<String>,
    pub bitfield: Bitfield,
    pub peer_choking: bool,
    pub am_choking: bool,
    pub their_interested: bool,
    pub down: RateMeter,
    pub up: RateMeter,
    pub since: Instant,
}

pub struct MetadataFetch {
    pub size: usize,
    pub pieces: Vec<Option<Vec<u8>>>,
    pub requested: HashMap<u32, (PeerKey, Instant)>,
    pub contributors: HashSet<PeerKey>,
}

pub struct Ready {
    pub torrent: Arc<Torrent>,
    pub storage: Arc<Storage>,
    pub picker: Picker,
    pub file_selected: Vec<bool>,
    pub full: Bitfield,
    pub buffers: HashMap<usize, PieceBuffer>,
}

pub struct PieceBuffer {
    pub data: Vec<u8>,
    pub received: usize,
}

pub enum Phase {
    Metadata(Option<MetadataFetch>),
    Checking { done: usize, total: usize },
    Ready(Box<Ready>),
}

pub struct Core {
    pub phase: Phase,
    pub state: State,
    pub error: Option<String>,
    pub settings: Settings,
    pub name: Option<String>,
    pub extras: Extras,
    pub peers: HashMap<PeerKey, PeerEntry>,
    pub candidates: VecDeque<SocketAddr>,
    pub known: HashSet<SocketAddr>,
    pub connecting: HashSet<SocketAddr>,
    pub strikes: HashMap<IpAddr, u8>,
    pub banned: HashSet<IpAddr>,
    pub contributors: HashMap<usize, Vec<PeerKey>>,
    pub down: RateMeter,
    pub up: RateMeter,
    pub wasted: u64,
    pub started: Instant,
    pub seeding_since: Option<Instant>,
    pub stream_touch: Option<Instant>,
    pub private: bool,
    pub paused: bool,
}

pub struct Shared {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub listen_port: u16,
    pub core: Mutex<Core>,
    /// Pieces completed, state changed: wakes streaming readers and `wait_*`.
    pub progress: Notify,
    /// Wakes the main loop early (new candidates, resume, settings changed).
    pub wake: Notify,
    pub completed_announce: Notify,
    pub stop: AtomicBool,
    pub down_limit: Arc<RateLimiter>,
    pub up_limit: Arc<RateLimiter>,
    pub tracker: TrackerClient,
    next_peer: AtomicU64,
}

impl Shared {
    pub fn lock(&self) -> MutexGuard<'_, Core> {
        self.core.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn next_key(&self) -> PeerKey {
        self.next_peer.fetch_add(1, Ordering::Relaxed)
    }

    pub fn is_stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }
}

impl Core {
    pub fn ready(&self) -> Option<&Ready> {
        match &self.phase {
            Phase::Ready(r) => Some(r),
            _ => None,
        }
    }

    pub fn ready_mut(&mut self) -> Option<&mut Ready> {
        match &mut self.phase {
            Phase::Ready(r) => Some(r),
            _ => None,
        }
    }

    pub fn broadcast(&self, make: impl Fn() -> PeerCmd) {
        for peer in self.peers.values() {
            let _ = peer.tx.try_send(make());
        }
    }

    pub fn add_candidate(&mut self, addr: SocketAddr) {
        if !self.peer_address_ok(&addr)
            || self.known.contains(&addr)
            || self.candidates.len() >= MAX_CANDIDATES
        {
            return;
        }
        self.known.insert(addr);
        self.candidates.push_back(addr);
    }

    pub fn peer_address_ok(&self, addr: &SocketAddr) -> bool {
        if addr.port() == 0 || self.banned.contains(&addr.ip()) {
            return false;
        }
        let ip = addr.ip();
        if ip.is_unspecified() || ip.is_multicast() {
            return false;
        }
        match ip {
            IpAddr::V4(v4) => {
                if v4.is_link_local() || v4.is_broadcast() {
                    return false;
                }
            }
            IpAddr::V6(v6) => {
                if (v6.segments()[0] & 0xffc0) == 0xfe80 {
                    return false;
                }
            }
        }
        self.settings.allow_loopback_peers || !ip.is_loopback()
    }

    pub fn strike(&mut self, ip: IpAddr) {
        let count = self.strikes.entry(ip).or_insert(0);
        *count += 1;
        if *count >= STRIKES_TO_BAN {
            self.banned.insert(ip);
            for peer in self.peers.values().filter(|p| p.addr.ip() == ip) {
                let _ = peer.tx.try_send(PeerCmd::Shutdown);
            }
        }
    }

    pub fn downloaded(&self) -> u64 {
        self.down.total()
    }

    pub fn uploaded(&self) -> u64 {
        self.up.total()
    }

    fn wanted_from_files(torrent: &Torrent, storage: &Storage, files: &[bool]) -> Bitfield {
        let mut wanted = Bitfield::new(torrent.piece_count());
        for (index, on) in files.iter().enumerate() {
            if *on {
                for piece in storage.piece_range_of_file(index) {
                    wanted.set(piece);
                }
            }
        }
        wanted
    }

    /// Re-derives the wanted piece set after `settings.selected` changed.
    pub fn apply_selection(&mut self) {
        let selected = self.settings.selected.clone();
        if let Some(r) = self.ready_mut() {
            r.file_selected = (0..r.torrent.files.len())
                .map(|i| {
                    selected.as_ref().is_none_or(|s| s.contains(&i)) && !r.torrent.files[i].pad
                })
                .collect();
            let wanted = Self::wanted_from_files(&r.torrent, &r.storage, &r.file_selected);
            r.picker.set_wanted(wanted);
        }
        // More (or fewer) pieces are wanted now: a finished torrent may have work again.
        let complete = self.ready().map(|r| r.picker.is_complete());
        match complete {
            Some(false) if matches!(self.state, State::Seeding | State::Complete) => {
                self.state = State::Downloading;
                self.seeding_since = None;
            }
            Some(true) if self.state == State::Downloading => mark_complete(self),
            _ => {}
        }
    }

    pub fn default_mode(&self) -> Mode {
        if self.settings.sequential
            && let Some(r) = self.ready()
        {
            let head = (0..r.picker.have().len())
                .find(|&p| r.picker.wanted().get(p) && !r.picker.have().get(p))
                .unwrap_or(0);
            return Mode::Sequential {
                head,
                window: window_pieces(&r.torrent, 32 * 1024 * 1024),
            };
        }
        Mode::RarestFirst
    }
}

pub fn window_pieces(torrent: &Torrent, bytes: u64) -> usize {
    (bytes / torrent.piece_length.max(1)).max(4) as usize
}

// ---------------------------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileStatus {
    pub index: usize,
    pub path: String,
    pub length: u64,
    pub completed: u64,
    pub selected: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerStatus {
    pub address: String,
    pub client: Option<String>,
    pub download_speed: u64,
    pub upload_speed: u64,
    pub progress: f64,
    pub seeder: bool,
    pub incoming: bool,
    pub choking_us: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentStatus {
    pub info_hash: String,
    pub name: Option<String>,
    pub state: State,
    pub error: Option<String>,
    pub directory: String,
    pub has_metadata: bool,
    pub total_length: u64,
    pub selected_length: u64,
    pub completed_length: u64,
    pub downloaded: u64,
    pub uploaded: u64,
    pub wasted: u64,
    pub download_speed: u64,
    pub upload_speed: u64,
    pub pieces_total: usize,
    pub pieces_done: usize,
    pub piece_length: u64,
    pub connections: usize,
    pub seeders: usize,
    pub known_peers: usize,
    pub sequential: bool,
    pub private: bool,
    pub ratio: f64,
    pub seeding_seconds: u64,
    pub checking: Option<(usize, usize)>,
    pub files: Vec<FileStatus>,
    pub peers: Vec<PeerStatus>,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// ---------------------------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------------------------

#[derive(Clone)]
pub struct TorrentHandle {
    pub(crate) shared: Arc<Shared>,
}

pub struct Start {
    pub torrent: Option<Torrent>,
    pub info_hash: [u8; 20],
    pub name: Option<String>,
    pub extras: Extras,
}

impl TorrentHandle {
    pub(crate) fn spawn(
        start: Start,
        settings: Settings,
        peer_id: [u8; 20],
        listen_port: u16,
        down_limit: Arc<RateLimiter>,
        up_limit: Arc<RateLimiter>,
        tracker: TrackerClient,
    ) -> Self {
        let private = start.torrent.as_ref().is_some_and(|t| t.private);
        let mut core = Core {
            phase: Phase::Metadata(None),
            state: State::Metadata,
            error: None,
            settings: settings.clone(),
            name: start.name.clone(),
            extras: start.extras.clone(),
            peers: HashMap::new(),
            candidates: VecDeque::new(),
            known: HashSet::new(),
            connecting: HashSet::new(),
            strikes: HashMap::new(),
            banned: HashSet::new(),
            contributors: HashMap::new(),
            down: RateMeter::default(),
            up: RateMeter::default(),
            wasted: 0,
            started: Instant::now(),
            seeding_since: None,
            stream_touch: None,
            private,
            paused: false,
        };
        for peer in &settings.peers {
            core.add_candidate(*peer);
        }
        let shared = Arc::new(Shared {
            info_hash: start.info_hash,
            peer_id,
            listen_port,
            core: Mutex::new(core),
            progress: Notify::new(),
            wake: Notify::new(),
            completed_announce: Notify::new(),
            stop: AtomicBool::new(false),
            down_limit,
            up_limit,
            tracker,
            next_peer: AtomicU64::new(1),
        });
        let handle = Self {
            shared: shared.clone(),
        };
        tokio::spawn(main_loop(shared.clone(), start.torrent));
        tokio::spawn(tracker_loop(shared));
        handle
    }

    pub fn info_hash(&self) -> [u8; 20] {
        self.shared.info_hash
    }

    pub fn info_hash_hex(&self) -> String {
        hex(&self.shared.info_hash)
    }

    pub fn torrent(&self) -> Option<Arc<Torrent>> {
        self.shared.lock().ready().map(|r| r.torrent.clone())
    }

    pub fn status(&self) -> TorrentStatus {
        let core = self.shared.lock();
        let mut status = TorrentStatus {
            info_hash: hex(&self.shared.info_hash),
            name: core.name.clone(),
            state: core.state,
            error: core.error.clone(),
            directory: core.settings.directory.display().to_string(),
            has_metadata: false,
            total_length: 0,
            selected_length: 0,
            completed_length: 0,
            downloaded: core.downloaded(),
            uploaded: core.uploaded(),
            wasted: core.wasted,
            download_speed: core.down.rate(),
            upload_speed: core.up.rate(),
            pieces_total: 0,
            pieces_done: 0,
            piece_length: 0,
            connections: core.peers.len(),
            seeders: 0,
            known_peers: core.known.len(),
            sequential: core.settings.sequential,
            private: core.private,
            ratio: 0.0,
            seeding_seconds: core
                .seeding_since
                .map(|t| t.elapsed().as_secs())
                .unwrap_or(0),
            checking: None,
            files: Vec::new(),
            peers: Vec::new(),
        };
        if let Phase::Checking { done, total } = &core.phase {
            status.checking = Some((*done, *total));
        }
        let mut total_pieces = 0;
        if let Some(r) = core.ready() {
            total_pieces = r.torrent.piece_count();
            status.has_metadata = true;
            status.total_length = r.torrent.total_length;
            status.piece_length = r.torrent.piece_length;
            status.pieces_total = total_pieces;
            status.pieces_done = r.picker.have().count();
            for (index, entry) in r.torrent.files.iter().enumerate() {
                if entry.pad {
                    continue;
                }
                let completed = r.storage.completed_bytes(index, r.picker.have());
                let selected = r.file_selected.get(index).copied().unwrap_or(false);
                if selected {
                    status.selected_length += entry.length;
                    status.completed_length += completed;
                }
                status.files.push(FileStatus {
                    index,
                    path: entry.path.join("/"),
                    length: entry.length,
                    completed,
                    selected,
                });
            }
            status.ratio = if status.selected_length > 0 {
                core.uploaded() as f64 / status.selected_length as f64
            } else {
                0.0
            };
        }
        for peer in core.peers.values() {
            let seeder = total_pieces > 0 && peer.bitfield.count() == total_pieces;
            if seeder {
                status.seeders += 1;
            }
            status.peers.push(PeerStatus {
                address: peer.addr.to_string(),
                client: peer.client.clone(),
                download_speed: peer.down.rate(),
                upload_speed: peer.up.rate(),
                progress: if total_pieces > 0 {
                    peer.bitfield.count() as f64 / total_pieces as f64
                } else {
                    0.0
                },
                seeder,
                incoming: peer.incoming,
                choking_us: peer.peer_choking,
            });
        }
        status
    }

    pub fn pause(&self) {
        {
            let mut core = self.shared.lock();
            if core.paused {
                return;
            }
            core.paused = true;
            core.state = State::Paused;
            core.broadcast(|| PeerCmd::Shutdown);
        }
        self.shared.progress.notify_waiters();
    }

    pub fn resume(&self) {
        {
            let mut core = self.shared.lock();
            core.paused = false;
            core.state = state_for(&core);
        }
        self.shared.wake.notify_one();
        self.shared.progress.notify_waiters();
    }

    /// Stops all activity. With `delete_files` the downloaded data is removed too.
    pub fn shutdown(&self, delete_files: bool) {
        self.shared.stop.store(true, Ordering::Relaxed);
        let storage = {
            let core = self.shared.lock();
            core.broadcast(|| PeerCmd::Shutdown);
            core.ready().map(|r| r.storage.clone())
        };
        self.shared.wake.notify_one();
        self.shared.completed_announce.notify_one();
        self.shared.progress.notify_waiters();
        if delete_files && let Some(storage) = storage {
            tokio::task::spawn_blocking(move || storage.delete_files());
        }
    }

    pub fn is_stopped(&self) -> bool {
        self.shared.is_stopped()
    }

    pub fn add_peer(&self, addr: SocketAddr) {
        self.shared.lock().add_candidate(addr);
        self.shared.wake.notify_one();
    }

    /// Changes which files are downloaded. Unselected data already on disk is kept.
    pub fn select_files(&self, files: Option<Vec<usize>>) {
        {
            let mut core = self.shared.lock();
            core.settings.selected = files;
            core.apply_selection();
            if core.state == State::Seeding || core.state == State::Complete {
                core.state = state_for(&core);
            }
            core.broadcast(|| PeerCmd::Ready);
        }
        self.shared.wake.notify_one();
    }

    pub fn set_sequential(&self, on: bool) {
        let mut core = self.shared.lock();
        core.settings.sequential = on;
        let mode = core.default_mode();
        if let Some(r) = core.ready_mut() {
            r.picker.mode = mode;
        }
    }

    pub fn set_seed_limits(&self, ratio: Option<f64>, time: Option<Duration>) {
        let mut core = self.shared.lock();
        core.settings.seed_ratio = ratio;
        core.settings.seed_time = time;
    }

    pub fn file_path(&self, file: usize) -> Option<PathBuf> {
        let core = self.shared.lock();
        let r = core.ready()?;
        (file < r.torrent.files.len()).then(|| r.storage.path_of(file).to_owned())
    }

    /// Waits until the metadata is known (magnet links).
    pub async fn wait_metadata(&self, timeout: Duration) -> Result<Arc<Torrent>> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.shared.progress.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(t) = self.torrent() {
                return Ok(t);
            }
            if self.is_stopped() {
                bail!("torrent was removed");
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                bail!("timed out waiting for torrent metadata");
            }
        }
    }

    /// Waits until every selected piece is on disk.
    pub async fn wait_complete(&self, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.shared.progress.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let core = self.shared.lock();
                if let Some(error) = &core.error {
                    bail!("{error}");
                }
                if core.ready().is_some_and(|r| r.picker.is_complete()) {
                    return Ok(());
                }
            }
            if self.is_stopped() {
                bail!("torrent was removed");
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                bail!("timed out waiting for the download");
            }
        }
    }

    /// Reads part of a file for streaming, prioritising the pieces that cover it and waiting for
    /// them to arrive. Files that were not selected are selected on demand.
    pub async fn read_file(
        &self,
        file: usize,
        offset: u64,
        len: usize,
        patience: Duration,
    ) -> Result<Vec<u8>> {
        let (torrent, storage) = {
            let core = self.shared.lock();
            let r = core
                .ready()
                .ok_or_else(|| anyhow!("metadata not available yet"))?;
            (r.torrent.clone(), r.storage.clone())
        };
        let entry = torrent
            .files
            .get(file)
            .ok_or_else(|| anyhow!("no such file"))?;
        if entry.pad || offset >= entry.length {
            bail!("range outside the file");
        }
        let len = len.min((entry.length - offset) as usize);
        let start = entry.offset + offset;
        let first = (start / torrent.piece_length) as usize;
        let last = ((start + len as u64 - 1) / torrent.piece_length) as usize;
        {
            let mut core = self.shared.lock();
            if core.ready().is_some_and(|r| !r.file_selected[file]) {
                let mut selected = core.settings.selected.clone().unwrap_or_default();
                if core.settings.selected.is_none() {
                    selected = (0..torrent.files.len()).collect();
                }
                selected.push(file);
                core.settings.selected = Some(selected);
                core.apply_selection();
                core.broadcast(|| PeerCmd::Ready);
            }
        }
        let deadline = tokio::time::Instant::now() + patience;
        loop {
            let notified = self.shared.progress.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let missing = {
                let mut core = self.shared.lock();
                let window = window_pieces(&torrent, 24 * 1024 * 1024);
                core.stream_touch = Some(Instant::now());
                let Some(r) = core.ready_mut() else {
                    bail!("torrent was removed");
                };
                let missing = (first..=last).find(|&p| !r.picker.have().get(p));
                // Aim the downloader at the playback position so seeks are served quickly.
                let head = missing.unwrap_or(last);
                r.picker.mode = Mode::Sequential { head, window };
                missing
            };
            if missing.is_none() {
                break;
            }
            self.shared.wake.notify_one();
            if self.is_stopped() {
                bail!("torrent was removed");
            }
            if let Some(error) = self.shared.lock().error.clone() {
                bail!("{error}");
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                bail!("timed out waiting for data");
            }
        }
        tokio::task::spawn_blocking(move || storage.read(start, len as u64)).await?
    }
}

fn state_for(core: &Core) -> State {
    match &core.phase {
        Phase::Metadata(_) => State::Metadata,
        Phase::Checking { .. } => State::Checking,
        Phase::Ready(r) => {
            if r.picker.is_complete() {
                if core.seeding_since.is_some() && core.state == State::Complete {
                    State::Complete
                } else {
                    State::Seeding
                }
            } else {
                State::Downloading
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------------------------

/// Builds storage, verifies existing data and switches the torrent to `Ready`.
pub(crate) async fn install_metadata(shared: &Arc<Shared>, torrent: Torrent) {
    let torrent = Arc::new(torrent);
    let (directory, selected) = {
        let mut core = shared.lock();
        core.name = Some(torrent.name.clone());
        core.private = torrent.private;
        core.phase = Phase::Checking {
            done: 0,
            total: torrent.piece_count(),
        };
        core.state = State::Checking;
        (
            core.settings.directory.clone(),
            core.settings.selected.clone(),
        )
    };
    shared.progress.notify_waiters();
    let storage = Arc::new(Storage::new(&torrent, &directory));
    let check = {
        let storage = storage.clone();
        let shared = shared.clone();
        tokio::task::spawn_blocking(move || {
            let cancel = AtomicBool::new(false);
            storage.check_all(
                |done, total| {
                    shared.lock().phase = Phase::Checking { done, total };
                    if shared.is_stopped() {
                        cancel.store(true, Ordering::Relaxed);
                    }
                },
                &cancel,
            )
        })
        .await
    };
    let have = match check {
        Ok(have) => have,
        Err(error) => return fail(shared, format!("verification failed: {error}")),
    };
    if shared.is_stopped() {
        return;
    }
    let file_selected: Vec<bool> = (0..torrent.files.len())
        .map(|i| selected.as_ref().is_none_or(|s| s.contains(&i)) && !torrent.files[i].pad)
        .collect();
    let wanted = Core::wanted_from_files(&torrent, &storage, &file_selected);
    let picker = Picker::new(torrent.piece_length, torrent.total_length, have, wanted);
    {
        let mut core = shared.lock();
        core.phase = Phase::Ready(Box::new(Ready {
            full: Bitfield::full(torrent.piece_count()),
            torrent: torrent.clone(),
            storage,
            picker,
            file_selected,
            buffers: HashMap::new(),
        }));
        let mode = core.default_mode();
        if let Some(r) = core.ready_mut() {
            r.picker.mode = mode;
        }
        core.extras = Extras {
            trackers: torrent.trackers.clone(),
            web_seeds: torrent.web_seeds.clone(),
        };
        core.state = if core.paused {
            State::Paused
        } else {
            State::Downloading
        };
        if core.ready().is_some_and(|r| r.picker.is_complete()) {
            mark_complete(&mut core);
        }
        core.broadcast(|| PeerCmd::Ready);
    }
    shared.progress.notify_waiters();
    shared.wake.notify_one();
}

fn fail(shared: &Arc<Shared>, message: String) {
    {
        let mut core = shared.lock();
        core.error = Some(message);
        core.state = State::Error;
        core.broadcast(|| PeerCmd::Shutdown);
    }
    shared.progress.notify_waiters();
}

/// Called with the lock held when every wanted piece is present.
pub(crate) fn mark_complete(core: &mut Core) {
    if core.seeding_since.is_none() {
        core.seeding_since = Some(Instant::now());
    }
    if core.state != State::Complete {
        core.state = State::Seeding;
    }
}

/// A fully received piece: verify, write, publish.
pub(crate) async fn finish_piece(shared: &Arc<Shared>, piece: usize, data: Vec<u8>) {
    let Some(storage) = shared.lock().ready().map(|r| r.storage.clone()) else {
        return;
    };
    let offset = piece as u64 * storage_piece_length(&storage);
    let outcome = {
        let storage = storage.clone();
        tokio::task::spawn_blocking(move || -> Result<bool> {
            if !storage.piece_matches(piece, &data) {
                return Ok(false);
            }
            storage.write(offset, &data)?;
            Ok(true)
        })
        .await
    };
    match outcome {
        Ok(Ok(true)) => {
            let mut completed = false;
            {
                let mut core = shared.lock();
                core.contributors.remove(&piece);
                if let Some(r) = core.ready_mut() {
                    r.picker.mark_have(piece);
                    completed = r.picker.is_complete();
                }
                core.broadcast(|| PeerCmd::Have(piece as u32));
                if completed {
                    let was = core.state;
                    mark_complete(&mut core);
                    if was == State::Downloading {
                        shared.completed_announce.notify_one();
                    }
                }
            }
            shared.progress.notify_waiters();
        }
        Ok(Ok(false)) => {
            let mut core = shared.lock();
            let bad = core.contributors.remove(&piece).unwrap_or_default();
            let ips: Vec<IpAddr> = bad
                .iter()
                .filter_map(|key| core.peers.get(key).map(|p| p.addr.ip()))
                .collect();
            for ip in ips {
                core.strike(ip);
            }
            if let Some(r) = core.ready_mut() {
                r.picker.piece_failed(piece);
            }
            drop(core);
            shared.wake.notify_one();
        }
        Ok(Err(error)) => fail(shared, format!("cannot write to disk: {error}")),
        Err(error) => fail(shared, format!("disk task failed: {error}")),
    }
}

fn storage_piece_length(storage: &Storage) -> u64 {
    storage.piece_length()
}

pub(crate) fn complete_metadata(shared: &Arc<Shared>) -> Option<Vec<u8>> {
    let mut core = shared.lock();
    let Phase::Metadata(Some(fetch)) = &mut core.phase else {
        return None;
    };
    if fetch.pieces.iter().any(Option::is_none) {
        return None;
    }
    let bytes: Vec<u8> = fetch.pieces.iter().flatten().flatten().copied().collect();
    if Sha1::digest(&bytes).as_slice() == shared.info_hash {
        Some(bytes)
    } else {
        // Somebody sent garbage: forget everything and strike the senders.
        let senders: Vec<PeerKey> = fetch.contributors.iter().copied().collect();
        core.phase = Phase::Metadata(None);
        let ips: Vec<IpAddr> = senders
            .iter()
            .filter_map(|k| core.peers.get(k).map(|p| p.addr.ip()))
            .collect();
        for ip in ips {
            core.strike(ip);
        }
        None
    }
}

pub(crate) fn metadata_piece_count(size: usize) -> usize {
    size.div_ceil(METADATA_PIECE)
}

struct Timers {
    tick: Instant,
    choke: Instant,
    pex: Instant,
    optimistic: usize,
}

async fn main_loop(shared: Arc<Shared>, initial: Option<Torrent>) {
    if let Some(torrent) = initial {
        install_metadata(&shared, torrent).await;
    }
    let mut timers = Timers {
        tick: Instant::now(),
        choke: Instant::now(),
        pex: Instant::now(),
        optimistic: 0,
    };
    loop {
        if shared.is_stopped() {
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            _ = shared.wake.notified() => {}
        }
        if shared.is_stopped() {
            return;
        }
        let connect = maintain(&mut shared.lock(), &shared, &mut timers);
        for addr in connect {
            tokio::spawn(super::peer::connect(shared.clone(), addr));
        }
    }
}

fn maintain(core: &mut Core, shared: &Arc<Shared>, timers: &mut Timers) -> Vec<SocketAddr> {
    // Streaming stopped: go back to the normal strategy.
    if let Some(touched) = core.stream_touch
        && touched.elapsed() > Duration::from_secs(20)
    {
        core.stream_touch = None;
        let mode = core.default_mode();
        if let Some(r) = core.ready_mut() {
            r.picker.mode = mode;
        }
    }
    // Speed meters advance once per second no matter how often we are woken.
    while timers.tick.elapsed() >= Duration::from_secs(1) {
        timers.tick += Duration::from_secs(1);
        core.down.tick();
        core.up.tick();
        for peer in core.peers.values_mut() {
            peer.down.tick();
            peer.up.tick();
        }
    }
    if core.paused || matches!(core.state, State::Error | State::Complete) {
        return Vec::new();
    }
    // Seed limits.
    if core.state == State::Seeding
        && let Some(since) = core.seeding_since
    {
        let selected_length = core
            .ready()
            .map(|r| {
                r.torrent
                    .files
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| r.file_selected[*i])
                    .map(|(_, f)| f.length)
                    .sum::<u64>()
            })
            .unwrap_or(0);
        let ratio_done = core.settings.seed_ratio.is_some_and(|ratio| {
            selected_length > 0 && core.uploaded() as f64 >= ratio * selected_length as f64
        });
        let time_done = core
            .settings
            .seed_time
            .is_some_and(|limit| since.elapsed() >= limit);
        if ratio_done || time_done {
            core.state = State::Complete;
            core.broadcast(|| PeerCmd::Shutdown);
            shared.progress.notify_waiters();
            return Vec::new();
        }
    }
    if timers.choke.elapsed() >= CHOKE_INTERVAL {
        timers.choke = Instant::now();
        timers.optimistic += 1;
        rechoke(core, timers.optimistic);
    }
    if core.settings.enable_pex && !core.private && timers.pex.elapsed() >= PEX_INTERVAL {
        timers.pex = Instant::now();
        let all: Arc<Vec<SocketAddr>> = Arc::new(core.peers.values().map(|p| p.addr).collect());
        core.broadcast(|| PeerCmd::Pex(all.clone()));
    }
    let mut connect = Vec::new();
    let capacity = core
        .settings
        .max_peers
        .saturating_sub(core.peers.len() + core.connecting.len());
    for _ in 0..capacity.min(8) {
        let Some(addr) = core.candidates.pop_front() else {
            break;
        };
        if core.peers.values().any(|p| p.addr == addr) || !core.peer_address_ok(&addr) {
            continue;
        }
        core.connecting.insert(addr);
        connect.push(addr);
    }
    connect
}

fn rechoke(core: &mut Core, round: usize) {
    let seeding = core.state == State::Seeding;
    let mut interested: Vec<(PeerKey, u64)> = core
        .peers
        .iter()
        .filter(|(_, p)| p.their_interested)
        .map(|(k, p)| (*k, if seeding { p.up.rate() } else { p.down.rate() }))
        .collect();
    interested.sort_by_key(|(_, rate)| std::cmp::Reverse(*rate));
    let mut unchoke: HashSet<PeerKey> = interested.iter().take(4).map(|(k, _)| *k).collect();
    let rest: Vec<PeerKey> = interested.iter().skip(4).map(|(k, _)| *k).collect();
    if !rest.is_empty() {
        unchoke.insert(rest[round % rest.len()]);
    }
    for (key, peer) in core.peers.iter_mut() {
        let want_unchoked = unchoke.contains(key);
        if want_unchoked && peer.am_choking {
            peer.am_choking = false;
            let _ = peer.tx.try_send(PeerCmd::Unchoke);
        } else if !want_unchoked && !peer.am_choking {
            peer.am_choking = true;
            let _ = peer.tx.try_send(PeerCmd::Choke);
        }
    }
}

async fn tracker_loop(shared: Arc<Shared>) {
    let mut first = true;
    let mut tracker_ids: HashMap<String, String> = HashMap::new();
    loop {
        if shared.is_stopped() {
            return;
        }
        // Wait until the torrent is active (metadata not needed for the first announce).
        let (trackers, private, paused, left, event_done) = {
            let core = shared.lock();
            let left = core
                .ready()
                .map(|r| {
                    r.torrent.total_length.saturating_sub(
                        (0..r.torrent.files.len())
                            .filter(|i| r.file_selected[*i])
                            .map(|i| r.storage.completed_bytes(i, r.picker.have()))
                            .sum::<u64>(),
                    )
                })
                .unwrap_or(1);
            (
                core.extras.trackers.clone(),
                core.private,
                core.paused,
                left,
                false,
            )
        };
        let _ = (private, event_done);
        if paused || trackers.is_empty() {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                _ = shared.wake.notified() => {}
            }
            continue;
        }
        let mut interval = Duration::from_secs(1800);
        let event = if first { Event::Started } else { Event::None };
        let request = |event: Event, left: u64, shared: &Arc<Shared>| {
            let core = shared.lock();
            AnnounceRequest {
                info_hash: shared.info_hash,
                peer_id: shared.peer_id,
                port: shared.listen_port,
                uploaded: core.uploaded(),
                downloaded: core.downloaded(),
                left,
                event,
                key: u32::from_be_bytes([
                    shared.peer_id[16],
                    shared.peer_id[17],
                    shared.peer_id[18],
                    shared.peer_id[19],
                ]),
                numwant: 80,
            }
        };
        let mut any_ok = false;
        'tiers: for tier in &trackers {
            for url in tier {
                if shared.is_stopped() {
                    return;
                }
                let req = request(event, left, &shared);
                match shared.tracker.announce(url, &req).await {
                    Ok(response) => {
                        any_ok = true;
                        if let Some(id) = response.tracker_id {
                            tracker_ids.insert(url.clone(), id);
                        }
                        interval = response
                            .interval
                            .max(Duration::from_secs(30))
                            .min(Duration::from_secs(3600));
                        {
                            let mut core = shared.lock();
                            for peer in response.peers {
                                core.add_candidate(peer);
                            }
                        }
                        shared.wake.notify_one();
                        break 'tiers;
                    }
                    Err(_) => continue,
                }
            }
        }
        if any_ok {
            first = false;
        } else {
            interval = Duration::from_secs(30);
        }
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shared.completed_announce.notified() => {
                if shared.is_stopped() {
                    announce_stopped(&shared, &trackers).await;
                    return;
                }
                let req = request(Event::Completed, 0, &shared);
                for tier in &trackers {
                    if let Some(url) = tier.first() {
                        let _ = shared.tracker.announce(url, &req).await;
                    }
                }
            }
            _ = shared.wake.notified() => {
                if shared.is_stopped() {
                    announce_stopped(&shared, &trackers).await;
                    return;
                }
            }
        }
    }
}

async fn announce_stopped(shared: &Arc<Shared>, trackers: &[Vec<String>]) {
    let request = {
        let core = shared.lock();
        AnnounceRequest {
            info_hash: shared.info_hash,
            peer_id: shared.peer_id,
            port: shared.listen_port,
            uploaded: core.uploaded(),
            downloaded: core.downloaded(),
            left: 0,
            event: Event::Stopped,
            key: 0,
            numwant: 0,
        }
    };
    for tier in trackers {
        if let Some(url) = tier.first() {
            let _ = tokio::time::timeout(
                Duration::from_secs(3),
                shared.tracker.announce(url, &request),
            )
            .await;
        }
    }
}

pub fn pipeline_depth(rate: u64) -> usize {
    ((rate / picker::BLOCK as u64) / 2 + 8).clamp(8, 96) as usize
}

pub fn unchoke_message() -> Message {
    Message::Unchoke
}
