//! ED2K upload side: a bounded slot queue, a shared bandwidth limiter and the per-peer
//! uploader. Only files explicitly placed in the [`ShareRegistry`] are ever served.

use super::{ShareRegistry, peer, peer::HelloInfo};
use anyhow::{Result, bail};
use serde::Serialize;
use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};

/// Payload bytes per `OP_SENDINGPART` packet (the size eMule/aMule use).
pub const CHUNK: usize = 10_240;
/// Largest single requested range, and the cap for the three ranges of one request.
const MAX_RANGE: u64 = 256 * 1024;
const MAX_REQUEST: u64 = 3 * MAX_RANGE;

#[derive(Clone, Debug)]
pub struct UploadConfig {
    /// Clients that receive data at the same time.
    pub max_active: usize,
    /// Clients allowed to wait for a slot.
    pub max_waiting: usize,
    /// Total upload rate in bytes per second; `None` is unlimited.
    pub bytes_per_second: Option<u64>,
    /// An active client that requests nothing for this long loses its slot.
    pub active_idle: Duration,
    /// How long a queued client may keep its connection open.
    pub waiting_idle: Duration,
    /// A client that disconnects keeps its queue position for this long.
    pub remember: Duration,
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self {
            max_active: 3,
            max_waiting: 200,
            bytes_per_second: None,
            active_idle: Duration::from_secs(90),
            waiting_idle: Duration::from_secs(900),
            remember: Duration::from_secs(1800),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Key {
    user: [u8; 16],
    ip: IpAddr,
}

struct Active {
    key: Key,
    name: String,
    file: [u8; 16],
    bytes: u64,
    since: Instant,
}

struct Waiting {
    key: Key,
    name: String,
    file: [u8; 16],
    since: Instant,
    connected: bool,
    priority: u8,
}

#[derive(Default)]
struct FileCounters {
    requests: u64,
    accepted: u64,
    uploaded: u64,
}

#[derive(Default)]
struct State {
    active: Vec<Active>,
    waiting: Vec<Waiting>,
    remembered: HashMap<Key, Instant>,
    files: HashMap<[u8; 16], FileCounters>,
    total: u64,
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

struct Inner {
    state: Mutex<State>,
    config: RwLock<UploadConfig>,
    bucket: Mutex<Bucket>,
    changed: watch::Sender<u64>,
}

/// Outcome of asking for an upload slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    Granted,
    /// Waiting; 1 is the next client to be served.
    Queued(u16),
    /// The waiting list is full.
    Refused,
}

#[derive(Clone)]
pub struct UploadManager {
    inner: Arc<Inner>,
}

impl Default for UploadManager {
    fn default() -> Self {
        Self::new(UploadConfig::default())
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadClientInfo {
    pub name: String,
    pub address: String,
    pub file: String,
    pub uploaded: u64,
    pub rank: Option<u32>,
    pub seconds: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileUploadStats {
    pub hash: String,
    pub requests: u64,
    pub accepted: u64,
    pub uploaded: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadSnapshot {
    pub active: Vec<UploadClientInfo>,
    pub waiting: Vec<UploadClientInfo>,
    pub total_uploaded: u64,
    pub max_active: usize,
    pub bytes_per_second: Option<u64>,
    pub files: Vec<FileUploadStats>,
}

fn hex(hash: &[u8; 16]) -> String {
    hash.iter().map(|b| format!("{b:02x}")).collect()
}

impl UploadManager {
    pub fn new(config: UploadConfig) -> Self {
        let (changed, _) = watch::channel(0);
        let rate = config.bytes_per_second.unwrap_or(0) as f64;
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State::default()),
                config: RwLock::new(config),
                bucket: Mutex::new(Bucket {
                    tokens: rate,
                    last: Instant::now(),
                }),
                changed,
            }),
        }
    }

    pub fn config(&self) -> UploadConfig {
        self.inner
            .config
            .read()
            .map(|c| c.clone())
            .unwrap_or_default()
    }

    /// Applies new limits; takes effect for current and future clients.
    pub fn set_config(&self, mut config: UploadConfig) {
        config.max_active = config.max_active.clamp(1, 64);
        config.max_waiting = config.max_waiting.min(5000);
        if let Ok(mut current) = self.inner.config.write() {
            *current = config;
        }
        if let Ok(mut state) = self.inner.state.lock() {
            self.fill(&mut state);
        }
        self.bump();
    }

    fn bump(&self) {
        self.inner.changed.send_modify(|n| *n = n.wrapping_add(1));
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.inner.changed.subscribe()
    }

    /// Moves connected waiting clients into free slots, oldest first.
    fn fill(&self, state: &mut State) {
        let max = self.config().max_active;
        while state.active.len() < max {
            let Some(index) = state.waiting.iter().position(|w| w.connected) else {
                break;
            };
            let w = state.waiting.remove(index);
            state.files.entry(w.file).or_default().accepted += 1;
            state.active.push(Active {
                key: w.key,
                name: w.name,
                file: w.file,
                bytes: 0,
                since: Instant::now(),
            });
        }
    }

    fn request(&self, key: Key, name: &str, file: [u8; 16], priority: u8) -> Admission {
        let config = self.config();
        let Ok(mut state) = self.inner.state.lock() else {
            return Admission::Refused;
        };
        let now = Instant::now();
        state
            .remembered
            .retain(|_, at| now.duration_since(*at) < config.remember);
        state.files.entry(file).or_default().requests += 1;
        if let Some(active) = state.active.iter_mut().find(|a| a.key == key) {
            active.file = file;
            return Admission::Granted;
        }
        if let Some(w) = state.waiting.iter_mut().find(|w| w.key == key) {
            w.file = file;
            w.connected = true;
            w.priority = priority;
        } else {
            if state.waiting.len() >= config.max_waiting {
                return Admission::Refused;
            }
            let since = state.remembered.remove(&key).unwrap_or(now);
            // Higher priority first; within a priority, the longest wait first.
            let position = state
                .waiting
                .iter()
                .position(|w| w.priority < priority || (w.priority == priority && w.since > since))
                .unwrap_or(state.waiting.len());
            state.waiting.insert(
                position,
                Waiting {
                    key,
                    name: name.to_owned(),
                    file,
                    since,
                    connected: true,
                    priority,
                },
            );
        }
        self.fill(&mut state);
        let admission = if state.active.iter().any(|a| a.key == key) {
            Admission::Granted
        } else {
            let rank = state.waiting.iter().position(|w| w.key == key).unwrap_or(0) + 1;
            Admission::Queued(rank.min(u16::MAX as usize) as u16)
        };
        drop(state);
        self.bump();
        admission
    }

    fn status(&self, key: Key) -> Option<Admission> {
        let state = self.inner.state.lock().ok()?;
        if state.active.iter().any(|a| a.key == key) {
            return Some(Admission::Granted);
        }
        state
            .waiting
            .iter()
            .position(|w| w.key == key)
            .map(|i| Admission::Queued((i + 1).min(u16::MAX as usize) as u16))
    }

    /// Frees the client's slot (or queue entry) because it finished or cancelled.
    fn release(&self, key: Key) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.active.retain(|a| a.key != key);
            state.waiting.retain(|w| w.key != key);
            self.fill(&mut state);
        }
        self.bump();
    }

    /// The connection closed: an active client loses its slot, a waiting one keeps its place in line
    /// (it can reconnect and ask again) but is skipped until then.
    fn disconnect(&self, key: Key) {
        if let Ok(mut state) = self.inner.state.lock() {
            let was_active = state.active.iter().any(|a| a.key == key);
            state.active.retain(|a| a.key != key);
            if let Some(index) = state.waiting.iter().position(|w| w.key == key) {
                let w = state.waiting.remove(index);
                state.remembered.insert(key, w.since);
            } else if was_active {
                state.remembered.insert(key, Instant::now());
            }
            self.fill(&mut state);
        }
        self.bump();
    }

    fn account(&self, key: Key, file: [u8; 16], bytes: u64) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.total += bytes;
            state.files.entry(file).or_default().uploaded += bytes;
            if let Some(active) = state.active.iter_mut().find(|a| a.key == key) {
                active.bytes += bytes;
            }
        }
    }

    /// Waits as long as needed to keep the average rate under the configured limit.
    pub async fn throttle(&self, bytes: usize) {
        let Some(rate) = self.config().bytes_per_second.filter(|r| *r > 0) else {
            return;
        };
        let wait = {
            let Ok(mut bucket) = self.inner.bucket.lock() else {
                return;
            };
            let now = Instant::now();
            let refill = now.duration_since(bucket.last).as_secs_f64() * rate as f64;
            bucket.tokens = (bucket.tokens + refill).min(rate as f64);
            bucket.last = now;
            bucket.tokens -= bytes as f64;
            if bucket.tokens < 0.0 {
                Duration::from_secs_f64(-bucket.tokens / rate as f64)
            } else {
                Duration::ZERO
            }
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }

    pub fn snapshot(&self) -> UploadSnapshot {
        let config = self.config();
        let Ok(state) = self.inner.state.lock() else {
            return UploadSnapshot {
                active: vec![],
                waiting: vec![],
                total_uploaded: 0,
                max_active: config.max_active,
                bytes_per_second: config.bytes_per_second,
                files: vec![],
            };
        };
        let now = Instant::now();
        let active = state
            .active
            .iter()
            .map(|a| UploadClientInfo {
                name: a.name.clone(),
                address: a.key.ip.to_string(),
                file: hex(&a.file),
                uploaded: a.bytes,
                rank: None,
                seconds: now.duration_since(a.since).as_secs(),
            })
            .collect();
        let waiting = state
            .waiting
            .iter()
            .enumerate()
            .map(|(i, w)| UploadClientInfo {
                name: w.name.clone(),
                address: w.key.ip.to_string(),
                file: hex(&w.file),
                uploaded: 0,
                rank: Some(i as u32 + 1),
                seconds: now.duration_since(w.since).as_secs(),
            })
            .collect();
        let mut files: Vec<FileUploadStats> = state
            .files
            .iter()
            .map(|(hash, c)| FileUploadStats {
                hash: hex(hash),
                requests: c.requests,
                accepted: c.accepted,
                uploaded: c.uploaded,
            })
            .collect();
        files.sort_by(|a, b| a.hash.cmp(&b.hash));
        UploadSnapshot {
            active,
            waiting,
            total_uploaded: state.total,
            max_active: config.max_active,
            bytes_per_second: config.bytes_per_second,
            files,
        }
    }
}

struct Presence {
    manager: UploadManager,
    key: Key,
}

impl Drop for Presence {
    fn drop(&mut self) {
        self.manager.disconnect(self.key);
    }
}

type Ranges = ([u8; 16], Vec<(u64, u64)>);

fn parse_ranges(wide: bool, data: &[u8]) -> Result<Ranges> {
    let expected = if wide { 16 + 6 * 8 } else { 16 + 6 * 4 };
    if data.len() != expected {
        bail!("invalid part request length");
    }
    let hash: [u8; 16] = data[..16].try_into()?;
    let width = if wide { 8 } else { 4 };
    let number = |index: usize| -> u64 {
        let at = 16 + index * width;
        if wide {
            u64::from_le_bytes(data[at..at + 8].try_into().unwrap())
        } else {
            u32::from_le_bytes(data[at..at + 4].try_into().unwrap()) as u64
        }
    };
    let mut ranges = Vec::new();
    let mut total = 0;
    for i in 0..3 {
        let (from, to) = (number(i), number(3 + i));
        if from == 0 && to == 0 {
            continue;
        }
        if to <= from || to - from > MAX_RANGE {
            bail!("invalid requested range");
        }
        total += to - from;
        ranges.push((from, to));
    }
    if total > MAX_REQUEST {
        bail!("part request too large");
    }
    Ok((hash, ranges))
}

/// Splits `bytes` (file offset `from`) into `OP_SENDINGPART` packets of at most [`CHUNK`] bytes.
pub(crate) fn part_packets(
    hash: &[u8; 16],
    from: u64,
    bytes: &[u8],
    wide: bool,
) -> Vec<(u8, u8, Vec<u8>)> {
    bytes
        .chunks(CHUNK)
        .enumerate()
        .map(|(i, chunk)| {
            let start = from + (i * CHUNK) as u64;
            let end = start + chunk.len() as u64;
            let mut packet = hash.to_vec();
            if wide {
                packet.extend(start.to_le_bytes());
                packet.extend(end.to_le_bytes());
            } else {
                packet.extend((start as u32).to_le_bytes());
                packet.extend((end as u32).to_le_bytes());
            }
            packet.extend(chunk);
            if wide {
                (0xc5, 0xa2, packet)
            } else {
                (0xe3, 0x46, packet)
            }
        })
        .collect()
}

/// eMule-compatible `OP_QUEUERANKING` body: rank followed by ten bytes of padding.
fn queue_ranking(rank: u16) -> Vec<u8> {
    let mut body = rank.to_le_bytes().to_vec();
    body.extend([0; 10]);
    body
}

async fn send_status(
    writer: &mut tokio::io::WriteHalf<super::obfuscation::PeerStream>,
    admission: Admission,
) -> Result<()> {
    match admission {
        Admission::Granted => peer::send(writer, 0xe3, 0x55, &[]).await,
        Admission::Queued(rank) => peer::send(writer, 0xc5, 0x60, &queue_ranking(rank)).await,
        Admission::Refused => peer::send(writer, 0xe3, 0x57, &[]).await,
    }
}

/// Serves one connected peer after the hello exchange.
pub(crate) async fn serve(
    stream: super::obfuscation::PeerStream,
    ip: IpAddr,
    hello: HelloInfo,
    shares: ShareRegistry,
    manager: UploadManager,
) -> Result<()> {
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (frames, mut incoming) = mpsc::channel(8);
    let read_task = tokio::spawn(async move {
        while let Ok(frame) = peer::frame(&mut reader).await {
            if frames.send(frame).await.is_err() {
                break;
            }
        }
    });
    struct StopReader(tokio::task::JoinHandle<()>);
    impl Drop for StopReader {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _stop = StopReader(read_task);
    let key = Key {
        user: hello.user_hash,
        ip,
    };
    let _presence = Presence {
        manager: manager.clone(),
        key,
    };
    let mut changes = manager.subscribe();
    let mut last_sent: Option<Admission> = None;
    let mut deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    loop {
        tokio::select! {
            frame = incoming.recv() => {
                let Some((protocol, op, data)) = frame else { return Ok(()) };
                let config = manager.config();
                match (protocol, op) {
                    (0xc5, 1) => peer::send(&mut writer, 0xc5, 2, &[60, 1, 0, 0, 0, 0]).await?,
                    // The extended payload (part status, complete-source count) is ignored.
                    (0xe3, 0x58) if data.len() >= 16 => {
                        let hash: [u8; 16] = data[..16].try_into()?;
                        if let Some(file) = shares.get(&hash) {
                            let name = file.link.name.as_bytes();
                            if name.len() > u16::MAX as usize {
                                bail!("shared filename too long");
                            }
                            let mut answer = hash.to_vec();
                            answer.extend((name.len() as u16).to_le_bytes());
                            answer.extend(name);
                            peer::send(&mut writer, 0xe3, 0x59, &answer).await?;
                        } else {
                            peer::send(&mut writer, 0xe3, 0x48, &hash).await?;
                        }
                    }
                    (0xe3, 0x4f) if data.len() >= 16 => {
                        let hash: [u8; 16] = data[..16].try_into()?;
                        if shares.get(&hash).is_some() {
                            // A part count of zero means "complete file", as eMule reports it.
                            let mut answer = hash.to_vec();
                            answer.extend(0u16.to_le_bytes());
                            peer::send(&mut writer, 0xe3, 0x50, &answer).await?;
                        } else {
                            peer::send(&mut writer, 0xe3, 0x48, &hash).await?;
                        }
                    }
                    (0xe3, 0x51) if data.len() >= 16 => {
                        let hash: [u8; 16] = data[..16].try_into()?;
                        if let Some(file) = shares.get(&hash) {
                            let mut answer = hash.to_vec();
                            answer.extend((file.part_hashes.len() as u16).to_le_bytes());
                            for part in &file.part_hashes {
                                answer.extend(part);
                            }
                            peer::send(&mut writer, 0xe3, 0x52, &answer).await?;
                        } else {
                            peer::send(&mut writer, 0xe3, 0x48, &hash).await?;
                        }
                    }
                    (0xe3, 0x54) if data.len() >= 16 => {
                        let hash: [u8; 16] = data[..16].try_into()?;
                        let Some(file) = shares.get(&hash) else {
                            peer::send(&mut writer, 0xe3, 0x48, &hash).await?;
                            continue;
                        };
                        {
                            let admission = manager.request(key, &hello.name, hash, file.priority());
                            last_sent = Some(admission);
                            send_status(&mut writer, admission).await?;
                            if admission == Admission::Refused {
                                return Ok(());
                            }
                        }
                    }
                    (0xe3, 0x47) | (0xc5, 0xa3) => {
                        let wide = protocol == 0xc5;
                        let (hash, ranges) = parse_ranges(wide, &data)?;
                        if manager.status(key) != Some(Admission::Granted) {
                            // No slot: eMule ignores such requests; so do we.
                            continue;
                        }
                        let Some(file) = shares.get(&hash) else {
                            peer::send(&mut writer, 0xe3, 0x48, &hash).await?;
                            continue;
                        };
                        for (from, to) in ranges {
                            if to > file.link.size {
                                bail!("requested range is beyond the shared file");
                            }
                            let sending_wide = to > u32::MAX as u64;
                            if sending_wide && !hello.large {
                                bail!("peer does not support 64-bit file offsets");
                            }
                            let reader = file.clone();
                            let bytes = tokio::task::spawn_blocking(move || reader.read_range(from, to)).await??;
                            for (p, o, packet) in part_packets(&hash, from, &bytes, sending_wide) {
                                manager.throttle(packet.len()).await;
                                peer::send(&mut writer, p, o, &packet).await?;
                                manager.account(key, hash, (packet.len() - if sending_wide { 32 } else { 24 }) as u64);
                            }
                        }
                    }
                    (0xe3, 0x4a) => peer::send(&mut writer, 0xe3, 0x4b, &[0; 4]).await?,
                    (0xe3, 0x56 | 0x49) => {
                        manager.release(key);
                        last_sent = None;
                    }
                    _ => {}
                }
                let idle = match manager.status(key) {
                    Some(Admission::Granted) => config.active_idle,
                    Some(Admission::Queued(_)) => config.waiting_idle,
                    _ => Duration::from_secs(30),
                };
                deadline = tokio::time::Instant::now() + idle;
            }
            changed = changes.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
                // A slot opened up, or our place in line moved.
                let now = manager.status(key);
                if let Some(now) = now
                    && last_sent != Some(now)
                {
                    // Only tell a queued client when its place in line improved.
                    if let (Some(Admission::Queued(old)), Admission::Queued(new)) = (last_sent, now)
                        && new >= old
                    {
                        continue;
                    }
                    last_sent = Some(now);
                    send_status(&mut writer, now).await?;
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                if manager.status(key) == Some(Admission::Granted) {
                    let _ = peer::send(&mut writer, 0xe3, 0x57, &[]).await;
                }
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(n: u8) -> Key {
        Key {
            user: [n; 16],
            ip: IpAddr::from([127, 0, 0, n]),
        }
    }

    fn manager(max_active: usize, max_waiting: usize) -> UploadManager {
        UploadManager::new(UploadConfig {
            max_active,
            max_waiting,
            ..UploadConfig::default()
        })
    }

    #[test]
    fn slots_fill_then_queue_in_arrival_order() {
        let m = manager(1, 10);
        assert_eq!(m.request(key(1), "a", [9; 16], 2), Admission::Granted);
        assert_eq!(m.request(key(2), "b", [9; 16], 2), Admission::Queued(1));
        assert_eq!(m.request(key(3), "c", [9; 16], 2), Admission::Queued(2));
        // Asking again does not lose the place.
        assert_eq!(m.request(key(3), "c", [9; 16], 2), Admission::Queued(2));
        m.release(key(1));
        assert_eq!(m.status(key(2)), Some(Admission::Granted));
        assert_eq!(m.status(key(3)), Some(Admission::Queued(1)));
    }

    #[test]
    fn higher_priority_files_are_served_first_and_ties_go_to_the_longest_wait() {
        let m = manager(1, 10);
        m.request(key(1), "a", [1; 16], 2);
        m.request(key(2), "normal-1", [1; 16], 2);
        m.request(key(3), "normal-2", [1; 16], 2);
        // A "release" priority client overtakes both normal ones, but not the active slot.
        assert_eq!(
            m.request(key(4), "release", [2; 16], 4),
            Admission::Queued(1)
        );
        assert_eq!(m.status(key(2)), Some(Admission::Queued(2)));
        // A low priority client goes to the back.
        assert_eq!(m.request(key(5), "low", [3; 16], 0), Admission::Queued(4));
        m.release(key(1));
        assert_eq!(m.status(key(4)), Some(Admission::Granted));
        m.release(key(4));
        assert_eq!(m.status(key(2)), Some(Admission::Granted));
    }

    #[test]
    fn waiting_list_is_bounded() {
        let m = manager(1, 1);
        assert_eq!(m.request(key(1), "a", [1; 16], 2), Admission::Granted);
        assert_eq!(m.request(key(2), "b", [1; 16], 2), Admission::Queued(1));
        assert_eq!(m.request(key(3), "c", [1; 16], 2), Admission::Refused);
    }

    #[test]
    fn a_disconnected_client_keeps_its_place_in_line() {
        let m = manager(1, 10);
        m.request(key(1), "a", [1; 16], 2);
        m.request(key(2), "early", [1; 16], 2);
        m.request(key(3), "late", [1; 16], 2);
        m.disconnect(key(2));
        // The absent client is skipped when the slot frees up...
        m.release(key(1));
        assert_eq!(m.status(key(3)), Some(Admission::Granted));
        m.release(key(3));
        // ...but when it returns it is ahead of anyone who joined after it left.
        assert_eq!(m.request(key(4), "newer", [1; 16], 2), Admission::Granted);
        assert_eq!(
            m.request(key(5), "newest", [1; 16], 2),
            Admission::Queued(1)
        );
        assert_eq!(m.request(key(2), "early", [1; 16], 2), Admission::Queued(1));
        assert_eq!(m.status(key(5)), Some(Admission::Queued(2)));
    }

    #[test]
    fn raising_the_limit_promotes_waiting_clients() {
        let m = manager(1, 10);
        m.request(key(1), "a", [1; 16], 2);
        m.request(key(2), "b", [1; 16], 2);
        m.set_config(UploadConfig {
            max_active: 2,
            ..m.config()
        });
        assert_eq!(m.status(key(2)), Some(Admission::Granted));
        assert_eq!(m.snapshot().active.len(), 2);
    }

    #[test]
    fn statistics_are_tracked_per_file() {
        let m = manager(2, 10);
        m.request(key(1), "a", [7; 16], 2);
        m.account(key(1), [7; 16], 500);
        let s = m.snapshot();
        assert_eq!(s.total_uploaded, 500);
        assert_eq!(s.files.len(), 1);
        assert_eq!(
            (
                s.files[0].requests,
                s.files[0].accepted,
                s.files[0].uploaded
            ),
            (1, 1, 500)
        );
        assert_eq!(s.active[0].uploaded, 500);
    }

    #[tokio::test]
    async fn throttle_enforces_the_configured_rate() {
        let m = UploadManager::new(UploadConfig {
            bytes_per_second: Some(100_000),
            ..UploadConfig::default()
        });
        let start = Instant::now();
        // The bucket starts with one second of burst; the next 50 kB must wait about 0.5 s.
        m.throttle(100_000).await;
        m.throttle(50_000).await;
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(400), "{elapsed:?}");
        assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
    }

    #[test]
    fn ranges_are_validated_and_bounded() {
        let mut request = vec![1u8; 16];
        for v in [10u32, 0, 0, 4106, 0, 0] {
            request.extend(v.to_le_bytes());
        }
        let (hash, ranges) = parse_ranges(false, &request).unwrap();
        assert_eq!(hash, [1; 16]);
        assert_eq!(ranges, vec![(10, 4106)]);
        // end <= start
        let mut bad = vec![1u8; 16];
        for v in [10u32, 0, 0, 10, 0, 0] {
            bad.extend(v.to_le_bytes());
        }
        assert!(parse_ranges(false, &bad).is_err());
        // too large
        let mut big = vec![1u8; 16];
        for v in [0u32, 0, 0, 300_000, 0, 0] {
            big.extend(v.to_le_bytes());
        }
        assert!(parse_ranges(false, &big).is_err());
        // wrong length
        assert!(parse_ranges(true, &request).is_err());
    }

    #[test]
    fn wide_requests_and_packets_use_64_bit_offsets() {
        let base = 5_000_000_000u64;
        let mut request = vec![2u8; 16];
        for v in [base, 0, 0, base + 20_000, 0, 0] {
            request.extend(v.to_le_bytes());
        }
        let (_, ranges) = parse_ranges(true, &request).unwrap();
        assert_eq!(ranges, vec![(base, base + 20_000)]);
        let data = vec![7u8; 20_000];
        let packets = part_packets(&[2; 16], base, &data, true);
        assert_eq!(packets.len(), 2, "20000 bytes split in 10240-byte packets");
        for (protocol, op, body) in &packets {
            assert_eq!((*protocol, *op), (0xc5, 0xa2));
            let start = u64::from_le_bytes(body[16..24].try_into().unwrap());
            let end = u64::from_le_bytes(body[24..32].try_into().unwrap());
            assert_eq!(body.len() - 32, (end - start) as usize);
        }
        let narrow = part_packets(&[2; 16], 100, &data[..5], false);
        assert_eq!((narrow[0].0, narrow[0].1), (0xe3, 0x46));
        assert_eq!(&narrow[0].2[16..20], &100u32.to_le_bytes());
        assert_eq!(&narrow[0].2[20..24], &105u32.to_le_bytes());
    }
}
