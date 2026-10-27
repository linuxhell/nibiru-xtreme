use super::{
    FileLink, FileSource,
    file::{BLOCK_SIZE, PART_SIZE, digest},
    peer::{
        HandshakeTimedOut, KnownHashes, MetadataTimedOut, Peer, PeerLacksFile, QueueWaitTimedOut,
        Tuning, check_cancel,
    },
};
use anyhow::{Context, Result, anyhow, bail};
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferStage {
    Connecting,
    Queued,
    Checking,
    Downloading,
    Completed,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransferProgress {
    pub stage: TransferStage,
    pub verified: u64,
    /// Parts authenticated against the link hashset and flushed to disk in this run.
    /// Consumers must not reconstruct this map from the total verified byte count.
    #[serde(default)]
    pub verified_parts: Vec<bool>,
    pub total: u64,
    pub peer: Option<String>,
    pub queue_rank: Option<u32>,
    /// Bytes received for parts that are still being fetched (not verified yet, so not in
    /// `verified`). Without it a download looks frozen while a slow peer sends one part.
    #[serde(default)]
    pub in_flight: u64,
}

/// What the transfer is doing with each peer, for a log that can explain a stall.
#[derive(Debug, Clone)]
pub enum TransferEvent {
    Connecting {
        peer: String,
    },
    PeerFailed {
        peer: String,
        kind: &'static str,
        error: String,
    },
    PeerReady {
        peer: String,
        offers: usize,
        wanted: usize,
    },
    PeerHasNothingNew {
        peer: String,
    },
    Queued {
        peer: String,
        rank: Option<u32>,
    },
    QueueFailed {
        peer: String,
        kind: &'static str,
        error: String,
    },
    SlotGranted {
        peer: String,
        waited_ms: u64,
    },
    PartStarted {
        peer: String,
        index: usize,
        shadow: bool,
    },
    PartSlow {
        peer: String,
        index: usize,
        received: usize,
        elapsed_ms: u64,
    },
    PartDone {
        peers: usize,
        peer: String,
        index: usize,
        bytes: usize,
        elapsed_ms: u64,
    },
    PartFailed {
        peer: String,
        index: usize,
        error: String,
        elapsed_ms: u64,
    },
}
pub type EventSink = Arc<dyn Fn(TransferEvent) + Send + Sync>;

pub struct TransferOptions {
    /// Sources found while the transfer runs (for example by Kad, which takes seconds): they join
    /// the ones already being tried, and the transfer waits for them when it has none left.
    pub late_sources: Option<tokio::sync::mpsc::UnboundedReceiver<FileSource>>,
    pub identity: [u8; 16],
    pub client_id: u32,
    pub callbacks: Option<super::CallbackClient>,
    pub obfuscation: super::obfuscation::Obfuscation,
    pub events: Option<EventSink>,
}

/// The dominant reason a transfer could not finish in this attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StallReason {
    /// No source could be tried (none given, or none reachable by any route we have).
    NoSources,
    /// Sources were tried but none could be connected.
    Unreachable,
    /// Peers connected but never sent the file information in time.
    MetadataTimeout,
    /// Peers answered, but none of them has the parts that are still missing.
    MissingParts,
    /// Peers have what is missing but kept us waiting in their upload queue.
    QueueTimeout,
    /// Peers refused: they do not share the file or had no slot.
    PeerRefused,
    /// A peer was sending a part and failed or sent bad data.
    PartFailed,
}

/// What the attempt found, source by source. Verified parts are kept; this explains why more could
/// not be fetched now so a caller can wait, re-ask for sources and try again.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Stalled {
    pub reason: StallReason,
    pub sources: usize,
    pub unreachable: usize,
    pub metadata_timeouts: usize,
    pub refused: usize,
    pub lacking_parts: usize,
    pub queue_timeouts: usize,
    pub part_failures: usize,
    pub missing_parts: usize,
    pub total_parts: usize,
    /// The most recent peer error, for logs.
    #[serde(default)]
    pub last_error: Option<String>,
}
impl std::fmt::Display for Stalled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.sources == 0 {
            write!(
                f,
                "no usable ED2K sources; a reachable callback route may be required"
            )?;
        } else {
            let mut found = Vec::new();
            for (count, text) in [
                (
                    self.lacking_parts,
                    "connected but do not have the missing parts",
                ),
                (self.queue_timeouts, "kept us waiting in their queue"),
                (self.part_failures, "failed while sending a part"),
                (
                    self.metadata_timeouts,
                    "did not send the file information in time",
                ),
                (self.refused, "refused or do not share the file"),
                (self.unreachable, "could not be reached"),
            ] {
                if count > 0 {
                    found.push(format!("{count} {text}"));
                }
            }
            write!(f, "{} sources tried: {}", self.sources, found.join(", "))?;
        }
        write!(
            f,
            "; {} of {} parts still missing",
            self.missing_parts, self.total_parts
        )?;
        if let Some(last) = &self.last_error {
            write!(f, " (last peer error: {last})")?;
        }
        Ok(())
    }
}
impl std::error::Error for Stalled {}

/// Download from server-discovered high-ID peers. Successful parts survive
/// interruption. Every resumed part is read and hashed again, without trusting
/// persisted completion flags. The final filename is never overwritten.
pub async fn download_from_sources(
    file: &FileLink,
    directory: &Path,
    sources: &[FileSource],
    identity: [u8; 16],
    client_id: u32,
    cancel: Arc<AtomicBool>,
    progress: impl FnMut(TransferProgress) + Send,
) -> Result<PathBuf> {
    download_with_options(
        file,
        directory,
        sources,
        TransferOptions {
            identity,
            client_id,
            callbacks: None,
            obfuscation: super::obfuscation::Obfuscation::Prefer,
            events: None,
            late_sources: None,
        },
        cancel,
        progress,
    )
    .await
}
pub async fn download_with_options(
    file: &FileLink,
    directory: &Path,
    sources: &[FileSource],
    options: TransferOptions,
    cancel: Arc<AtomicBool>,
    progress: impl FnMut(TransferProgress) + Send,
) -> Result<PathBuf> {
    download_with_tuning(
        file,
        directory,
        sources,
        options,
        Tuning::default(),
        cancel,
        progress,
    )
    .await
}
/// Like [`download_with_options`], with explicit peer time limits.
///
/// An attempt that cannot finish returns an error whose chain contains a [`Stalled`] describing
/// why; the verified parts and the authenticated hashset stay on disk, so the next attempt starts
/// from them without asking a peer for the hashset again.
pub async fn download_with_tuning(
    file: &FileLink,
    directory: &Path,
    sources: &[FileSource],
    mut options: TransferOptions,
    tuning: Tuning,
    cancel: Arc<AtomicBool>,
    mut progress: impl FnMut(TransferProgress) + Send,
) -> Result<PathBuf> {
    let mut late = options.late_sources.take();
    // Revalidate deserialized inputs before using a filename as a path.
    let validated = FileLink::parse(&file.link())?;
    if &validated != file {
        bail!("invalid ED2K file descriptor");
    }
    check_cancel(&cancel)?;
    let target = directory.join(&file.name);
    if tokio::fs::symlink_metadata(&target).await.is_ok() {
        bail!("destination already exists");
    }
    let stage = directory.join(format!(
        ".nibiru-ed2k-{}-{}.part",
        file.hash_hex(),
        file.size
    ));
    if let Ok(metadata) = tokio::fs::symlink_metadata(&stage).await {
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            bail!("partial path is not a regular file");
        }
        if metadata.len() > file.size {
            bail!("partial file exceeds requested size");
        }
    }
    // Lock a separate coordination file: Windows byte-range locks on the data file
    // would also prevent a media player from reading already verified parts.
    let lock_path = stage.with_extension("lock");
    let _writer_guard = tokio::task::spawn_blocking(move || -> Result<std::fs::File> {
        let mut options = std::fs::OpenOptions::new();
        options.create(true).truncate(false).read(true).write(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options
                .share_mode(0)
                .access_mode(0xC0010000)
                .custom_flags(0x04000000); // Delete on close, including errors.
        }
        let guard = options
            .open(lock_path)
            .context("ED2K partial file is already in use")?;
        guard
            .try_lock()
            .context("ED2K partial file is already in use")?;
        Ok(guard)
    })
    .await??;
    // The hashset that authenticated earlier parts is kept next to them (re-validated against the
    // link on every load), so a resume can verify the partial file and skip asking peers for it.
    let hashset_path = stage.with_extension("hashset");
    let saved_hashes = load_hashset(file, &hashset_path).await;
    let stage_clone = stage.clone();
    let disk = tokio::task::spawn_blocking(move || -> Result<std::fs::File> {
        let disk = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(stage_clone)?;
        Ok(disk)
    })
    .await??;
    let disk = tokio::fs::File::from_std(disk);
    let context = Context_ {
        file,
        options: &options,
        cancel: &cancel,
        progress: Mutex::new(&mut progress),
        disk: tokio::sync::Mutex::new(disk),
        scan_gate: tokio::sync::Mutex::new(()),
        changed: tokio::sync::Notify::new(),
        shared: Mutex::new(Shared {
            complete: vec![false; file.part_count()],
            open: std::collections::HashMap::new(),
            solo: std::collections::HashSet::new(),
            verified: 0,
            scanned: false,
            hashes: saved_hashes.map(Arc::new),
            hashset_path,
            outcomes: Outcomes::default(),
            last_error: None,
        }),
        tuning,
    };
    // With a hashset on disk the partial file can be checked before any peer answers, so the
    // progress shown on resume is real from the start.
    let known = context.shared.lock().unwrap().hashes.clone();
    if let Some(hashes) = known {
        context.emit(TransferStage::Checking, &None, None);
        context.scan(&hashes, &None).await?;
        context.emit(TransferStage::Connecting, &None, None);
    }
    // Bounded attempt count avoids unbounded connections to a server-supplied list.
    let usable =
        |s: &FileSource| (s.host.is_some() && !s.requires_callback) || options.callbacks.is_some();
    let key = |s: &FileSource| format!("{:?}:{}:{}", s.host, s.port, s.client_id);
    let mut tried: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut queue: Vec<FileSource> = sources
        .iter()
        .filter(|s| usable(s))
        .take(16)
        .cloned()
        .collect();
    tried.extend(queue.iter().map(key));
    let shared_context = &context;
    for _ in 0..MAX_ROUNDS {
        if context.all_complete() || (queue.is_empty() && late.is_none()) {
            break;
        }
        let mut pending: std::collections::VecDeque<FileSource> = std::mem::take(&mut queue).into();
        let mut running = FuturesUnordered::new();
        // Sources that delivered parts get another connection next round (they may have dropped
        // mid-part); sources that delivered nothing are not retried.
        loop {
            while running.len() < MAX_PARALLEL_PEERS && !context.all_complete() {
                match pending.pop_front() {
                    Some(source) => running.push(shared_context.work_tagged(source)),
                    None => break,
                }
            }
            if running.is_empty() {
                // Nothing is being tried: wait for sources that are still being found.
                let Some(receiver) = late.as_mut() else {
                    break;
                };
                tokio::select! {
                    found = receiver.recv() => match found {
                        Some(source) => {
                            if usable(&source) && tried.len() < 32 && tried.insert(key(&source)) {
                                pending.push_back(source);
                            }
                            continue;
                        }
                        None => {
                            late = None;
                            break;
                        }
                    },
                    _ = wait_cancelled(&cancel) => break,
                }
            }
            tokio::select! {
                finished = running.next() => {
                    let Some((source, result)) = finished else {
                        continue;
                    };
                    let (fetched, retry) = result?;
                    if context.all_complete() {
                        // Everything is verified: stop connecting to the sources still waiting or
                        // being tried.
                        break;
                    }
                    if fetched > 0 || retry {
                        queue.push(source);
                    }
                }
                found = recv_late(&mut late), if late.is_some() => match found {
                    Some(source) => {
                        if usable(&source) && tried.len() < 32 && tried.insert(key(&source)) {
                            pending.push_back(source);
                        }
                    }
                    None => late = None,
                },
            }
        }
    }
    check_cancel(&cancel)?;
    if !context.all_complete() {
        let stalled = context.stalled();
        return Err(anyhow::Error::new(stalled))
            .context("ED2K transfer incomplete; verified parts retained for resume");
    }
    let verified = context.shared.lock().unwrap().verified;
    let context_hashset = context.shared.lock().unwrap().hashset_path.clone();
    let disk = context.disk.into_inner();
    disk.set_len(file.size).await?;
    disk.sync_all().await?;
    let disk = disk.into_std().await;
    let target_clone = target.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let temporary =
            tempfile::NamedTempFile::from_parts(disk, tempfile::TempPath::try_from_path(stage)?);
        match temporary.persist_noclobber(target_clone) {
            Ok(_) => Ok(()),
            Err(error) => {
                let reason = error.error.to_string();
                let _ = error.file.keep(); // Preserve verified parts on finalize failure.
                bail!("cannot finalize ED2K file without overwriting: {reason}");
            }
        }
    })
    .await??;
    let _ = tokio::fs::remove_file(&context_hashset).await;
    progress(TransferProgress {
        stage: TransferStage::Completed,
        verified,
        verified_parts: vec![true; file.part_count()],
        total: file.size,
        peer: None,
        queue_rank: None,
        in_flight: 0,
    });
    Ok(target)
}

const MAX_PARALLEL_PEERS: usize = 8;
/// Blocks asked for in one request (the legacy request carries exactly three).
const BLOCKS_PER_REQUEST: usize = 3;
const MAX_ROUNDS: usize = 6;

/// How each source ended in this attempt.
#[derive(Default)]
struct Outcomes {
    sources: usize,
    unreachable: usize,
    metadata_timeouts: usize,
    refused: usize,
    lacking_parts: usize,
    queue_timeouts: usize,
    part_failures: usize,
}

/// A part being assembled from blocks that may come from different peers.
struct PartBuf {
    data: Vec<u8>,
    have: Vec<bool>,
    /// When each block was asked for (a block nobody has delivered for long is asked again).
    asked_at: Vec<Option<std::time::Instant>>,
    asked_twice: Vec<bool>,
    /// The peer that opened the part takes blocks from the front, helpers from the back.
    opener: String,
    started: std::time::Instant,
    contributors: Vec<String>,
}

/// One block to fetch: part, block number, absolute byte range.
struct Work {
    index: usize,
    block: usize,
    from: u64,
    end: u64,
    /// This peer did not open the part.
    helping: bool,
}

struct Shared {
    complete: Vec<bool>,
    /// Parts being fetched right now, block by block, possibly from several peers at once.
    open: std::collections::HashMap<usize, PartBuf>,
    /// Parts whose blocks came from several peers and failed verification: they are fetched again
    /// by one peer at a time, so the peer that sends bad data can be told apart.
    solo: std::collections::HashSet<usize>,
    verified: u64,
    scanned: bool,
    /// A hashset already authenticated against the link: from disk or from the first peer that sent it.
    hashes: Option<Arc<Vec<[u8; 16]>>>,
    hashset_path: PathBuf,
    outcomes: Outcomes,
    last_error: Option<String>,
}

/// Reads the saved hashset, accepting it only if it still authenticates against the link.
async fn load_hashset(file: &FileLink, path: &Path) -> Option<Vec<[u8; 16]>> {
    if file.size < PART_SIZE {
        return Some(vec![file.hash]);
    }
    let metadata = tokio::fs::symlink_metadata(path).await.ok()?;
    if !metadata.is_file() || metadata.len() > 1 << 20 || metadata.len() % 16 != 0 {
        return None;
    }
    let bytes = tokio::fs::read(path).await.ok()?;
    let values = bytes
        .chunks_exact(16)
        .map(|chunk| <[u8; 16]>::try_from(chunk).ok())
        .collect::<Option<Vec<_>>>()?;
    file.validate_hashset(values).ok()
}

/// Writes the hashset atomically; failure only means the next run asks a peer again.
async fn save_hashset(path: PathBuf, hashes: Vec<[u8; 16]>) {
    let _ = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        use std::io::Write;
        let directory = path.parent().unwrap_or(Path::new("."));
        let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
        temporary.write_all(&hashes.concat())?;
        temporary.as_file().sync_all()?;
        temporary.persist(&path).map_err(|e| e.error)?;
        Ok(())
    })
    .await;
}

/// State shared by the peers working on one file at the same time.
struct Context_<'a> {
    file: &'a FileLink,
    options: &'a TransferOptions,
    cancel: &'a Arc<AtomicBool>,
    progress: Mutex<&'a mut (dyn FnMut(TransferProgress) + Send)>,
    disk: tokio::sync::Mutex<tokio::fs::File>,
    scan_gate: tokio::sync::Mutex<()>,
    shared: Mutex<Shared>,
    /// Signalled when a part is completed or given back, so idle peers re-check what is left.
    changed: tokio::sync::Notify,
    tuning: Tuning,
}

impl Context_<'_> {
    fn all_complete(&self) -> bool {
        self.shared.lock().unwrap().complete.iter().all(|v| *v)
    }
    fn emit(&self, stage: TransferStage, peer: &Option<String>, queue_rank: Option<u32>) {
        let mut progress = self.progress.lock().unwrap();
        let (verified, verified_parts, in_flight) = {
            let shared = self.shared.lock().unwrap();
            let in_flight = shared
                .open
                .values()
                .map(|part| {
                    part.have
                        .iter()
                        .enumerate()
                        .filter(|(_, got)| **got)
                        .map(|(block, _)| block_len(part.data.len(), block))
                        .sum::<usize>() as u64
                })
                .sum();
            (shared.verified, shared.complete.clone(), in_flight)
        };
        progress(TransferProgress {
            stage,
            verified,
            verified_parts,
            total: self.file.size,
            peer: peer.clone(),
            queue_rank,
            in_flight,
        });
    }
    fn soft_error(&self, error: &anyhow::Error) {
        self.shared.lock().unwrap().last_error = Some(format!("{error:#}"));
    }
    /// What this attempt found, once it has nothing more to try.
    fn stalled(&self) -> Stalled {
        let shared = self.shared.lock().unwrap();
        let o = &shared.outcomes;
        let missing = shared.complete.iter().filter(|done| !**done).count();
        let reason = if o.sources == 0 {
            StallReason::NoSources
        } else {
            // The most common cause wins; on a tie the more informative one (listed first).
            let mut best = (0, StallReason::Unreachable);
            for (count, reason) in [
                (o.lacking_parts, StallReason::MissingParts),
                (o.queue_timeouts, StallReason::QueueTimeout),
                (o.part_failures, StallReason::PartFailed),
                (o.metadata_timeouts, StallReason::MetadataTimeout),
                (o.refused, StallReason::PeerRefused),
                (o.unreachable, StallReason::Unreachable),
            ] {
                if count > best.0 {
                    best = (count, reason);
                }
            }
            best.1
        };
        Stalled {
            reason,
            sources: o.sources,
            unreachable: o.unreachable,
            metadata_timeouts: o.metadata_timeouts,
            refused: o.refused,
            lacking_parts: o.lacking_parts,
            queue_timeouts: o.queue_timeouts,
            part_failures: o.part_failures,
            missing_parts: missing,
            total_parts: shared.complete.len(),
            last_error: shared.last_error.clone(),
        }
    }
    /// Re-reads what is already on disk and marks the parts that match the hashset. Done once.
    async fn scan(&self, hashes: &[[u8; 16]], host: &Option<String>) -> Result<()> {
        let _gate = self.scan_gate.lock().await;
        if self.shared.lock().unwrap().scanned {
            return Ok(());
        }
        self.emit(TransferStage::Checking, host, None);
        for (index, expected) in hashes.iter().enumerate().take(self.file.part_count()) {
            check_cancel(self.cancel)?;
            let offset = index as u64 * PART_SIZE;
            let count = self.file.part_len(index);
            let mut disk = self.disk.lock().await;
            if disk.metadata().await?.len() >= offset + count as u64 {
                let mut bytes = vec![0; count];
                disk.seek(std::io::SeekFrom::Start(offset)).await?;
                disk.read_exact(&mut bytes).await?;
                drop(disk);
                if digest(&bytes) == *expected {
                    let mut shared = self.shared.lock().unwrap();
                    shared.complete[index] = true;
                    shared.verified += count as u64;
                }
            }
        }
        self.shared.lock().unwrap().scanned = true;
        Ok(())
    }
    fn note(&self, change: impl FnOnce(&mut Outcomes)) {
        change(&mut self.shared.lock().unwrap().outcomes);
    }
    /// The next block for this peer. It continues the part it opened; otherwise it opens a part
    /// nobody has (first part, last part, then in order); otherwise it helps with a part other
    /// peers are fetching, taking blocks from the back; finally it asks again for a block that has
    /// been outstanding for a long time (the first answer wins).
    fn next_work(&self, peer: &Peer, label: &str) -> Option<Work> {
        let mut shared = self.shared.lock().unwrap();
        let geometry = block_work;
        // Starting the first/tail part is not enough: its owner may be much slower than the
        // other sources. Help these parts before opening or continuing middle parts, otherwise
        // hundreds of megabytes can arrive while the player's header is still unavailable.
        // Take unclaimed blocks from the back; never mix peers after a failed mixed hash.
        let last_len = self.file.part_len(shared.complete.len().saturating_sub(1));
        for index in preview_order(shared.complete.len(), last_len)
            .take(preview_parts(shared.complete.len(), last_len))
        {
            if !peer.has_part(index) || shared.solo.contains(&index) {
                continue;
            }
            let Some(part) = shared.open.get_mut(&index) else {
                continue;
            };
            if part.opener == label {
                continue;
            }
            if let Some(block) = (0..part.have.len())
                .rev()
                .find(|b| !part.have[*b] && part.asked_at[*b].is_none())
            {
                if part.opener.is_empty() {
                    part.opener = label.to_owned();
                }
                part.asked_at[block] = Some(std::time::Instant::now());
                return Some(geometry(index, block, part.data.len(), true));
            }
            if let Some(block) = (0..part.have.len()).rev().find(|b| {
                !part.have[*b]
                    && !part.asked_twice[*b]
                    && part.asked_at[*b].is_some_and(|at| at.elapsed() >= self.tuning.endgame_after)
            }) {
                part.asked_twice[block] = true;
                return Some(geometry(index, block, part.data.len(), true));
            }
        }
        let own = shared
            .open
            .iter()
            .filter(|(i, part)| part.opener == label && peer.has_part(**i))
            .filter_map(|(i, part)| {
                part.have
                    .iter()
                    .zip(&part.asked_at)
                    .position(|(got, asked)| !*got && asked.is_none())
                    .map(|block| (*i, block))
            })
            .min();
        if let Some((index, block)) = own {
            let part = shared.open.get_mut(&index).unwrap();
            part.asked_at[block] = Some(std::time::Instant::now());
            let len = part.data.len();
            return Some(geometry(index, block, len, false));
        }
        // A part whose peer dropped out is picked up before any new part is opened, so the first
        // parts (which a preview needs) are never left behind.
        let orphan = shared
            .open
            .iter()
            .filter(|(i, part)| part.opener.is_empty() && peer.has_part(**i))
            .filter_map(|(i, part)| {
                part.have
                    .iter()
                    .zip(&part.asked_at)
                    .position(|(got, asked)| !*got && asked.is_none())
                    .map(|block| (*i, block))
            })
            .min();
        if let Some((index, block)) = orphan {
            let part = shared.open.get_mut(&index).unwrap();
            part.opener = label.to_owned();
            part.asked_at[block] = Some(std::time::Instant::now());
            let len = part.data.len();
            return Some(geometry(index, block, len, false));
        }
        let last_len = self.file.part_len(shared.complete.len().saturating_sub(1));
        let fresh = preview_order(shared.complete.len(), last_len)
            .find(|i| !shared.complete[*i] && !shared.open.contains_key(i) && peer.has_part(*i));
        if let Some(index) = fresh {
            let len = self.file.part_len(index);
            let blocks = len.div_ceil(BLOCK_SIZE as usize);
            let mut part = PartBuf {
                data: vec![0; len],
                have: vec![false; blocks],
                asked_at: vec![None; blocks],
                asked_twice: vec![false; blocks],
                opener: label.to_owned(),
                started: std::time::Instant::now(),
                contributors: Vec::new(),
            };
            part.asked_at[0] = Some(std::time::Instant::now());
            shared.open.insert(index, part);
            return Some(geometry(index, 0, len, false));
        }
        let help = shared
            .open
            .iter()
            .filter(|(i, _)| peer.has_part(**i) && !shared.solo.contains(*i))
            .filter_map(|(i, part)| {
                part.have
                    .iter()
                    .zip(&part.asked_at)
                    .rposition(|(got, asked)| !*got && asked.is_none())
                    .map(|block| (*i, block))
            })
            .min();
        if let Some((index, block)) = help {
            let part = shared.open.get_mut(&index).unwrap();
            part.asked_at[block] = Some(std::time::Instant::now());
            let len = part.data.len();
            return Some(geometry(index, block, len, true));
        }
        let stale = shared
            .open
            .iter()
            .filter(|(i, _)| peer.has_part(**i) && !shared.solo.contains(*i))
            .filter_map(|(i, part)| {
                (0..part.have.len())
                    .rev()
                    .find(|b| {
                        !part.have[*b]
                            && !part.asked_twice[*b]
                            && part.asked_at[*b]
                                .is_some_and(|at| at.elapsed() >= self.tuning.endgame_after)
                    })
                    .map(|block| (*i, block))
            })
            .min();
        if let Some((index, block)) = stale {
            let part = shared.open.get_mut(&index).unwrap();
            part.asked_twice[block] = true;
            let len = part.data.len();
            return Some(geometry(index, block, len, true));
        }
        None
    }
    /// The next blocks for this peer: the one `next_work` picks and, like eMule and aMule, up to two
    /// more of the same part in the same request (from the front for the part's owner, from the
    /// back for a helper).
    fn next_works(&self, peer: &Peer, label: &str) -> Vec<Work> {
        let Some(first) = self.next_work(peer, label) else {
            return Vec::new();
        };
        let mut works = vec![first];
        let mut shared = self.shared.lock().unwrap();
        if let Some(part) = shared.open.get_mut(&works[0].index) {
            let blocks = part.have.len();
            let order: Vec<usize> = if works[0].helping {
                (0..blocks).rev().collect()
            } else {
                (0..blocks).collect()
            };
            for block in order {
                if works.len() == BLOCKS_PER_REQUEST {
                    break;
                }
                if !part.have[block] && part.asked_at[block].is_none() {
                    part.asked_at[block] = Some(std::time::Instant::now());
                    works.push(block_work(
                        works[0].index,
                        block,
                        part.data.len(),
                        works[0].helping,
                    ));
                }
            }
        }
        works
    }
    /// This peer is done (or gone): the parts it opened lose their owner and any peer may take over.
    fn abandon(&self, label: &str) {
        let mut shared = self.shared.lock().unwrap();
        for part in shared.open.values_mut().filter(|p| p.opener == label) {
            part.opener.clear();
        }
        drop(shared);
        self.changed.notify_waiters();
    }
    /// A block could not be fetched: let someone else ask for it.
    fn release_block(&self, work: &Work) {
        let mut shared = self.shared.lock().unwrap();
        if let Some(part) = shared
            .open
            .get_mut(&work.index)
            .filter(|part| !part.have[work.block])
        {
            part.asked_at[work.block] = None;
            part.asked_twice[work.block] = false;
        }
        drop(shared);
        self.changed.notify_waiters();
    }
    /// Stores a block. When it was the last one, returns the whole part to verify and write.
    fn put_block(&self, work: &Work, bytes: &[u8], label: &str) -> Option<(Vec<u8>, usize, u64)> {
        let mut shared = self.shared.lock().unwrap();
        let part = shared.open.get_mut(&work.index)?;
        if part.have[work.block] || bytes.len() != (work.end - work.from) as usize {
            return None;
        }
        let offset = work.block * BLOCK_SIZE as usize;
        part.data[offset..offset + bytes.len()].copy_from_slice(bytes);
        part.have[work.block] = true;
        if !part.contributors.iter().any(|c| c == label) {
            part.contributors.push(label.to_owned());
        }
        if part.have.iter().all(|got| *got) {
            let part = shared.open.remove(&work.index)?;
            return Some((
                part.data,
                part.contributors.len(),
                part.started.elapsed().as_millis() as u64,
            ));
        }
        None
    }
    /// Whether this peer could still be given work once the blocks being fetched elsewhere land
    /// (they may fail and come back).
    fn waiting_for_others(&self, peer: &Peer) -> bool {
        let shared = self.shared.lock().unwrap();
        shared.open.keys().any(|i| peer.has_part(*i))
    }
    fn event(&self, event: TransferEvent) {
        if let Some(sink) = &self.options.events {
            sink(event);
        }
    }
    async fn work_tagged(&self, source: FileSource) -> (FileSource, Result<(usize, bool)>) {
        let result = self.work(&source).await;
        (source, result)
    }

    /// Talks to one source until it has nothing more to give. Source failures are remembered and
    /// reported as `Ok`; only cancellation and local disk errors are returned as `Err`.
    /// Returns how many parts this source delivered and whether it is worth another try this
    /// attempt (it timed out only because it was asked for a hashset we have since obtained).
    async fn work(&self, source: &FileSource) -> Result<(usize, bool)> {
        check_cancel(self.cancel)?;
        self.note(|o| o.sources += 1);
        let host = source
            .host
            .map(|h| format!("{h}:{}", source.port))
            .or_else(|| Some(format!("ID {}", source.client_id)));
        self.emit(TransferStage::Connecting, &host, None);
        let label = host.clone().unwrap_or_default();
        self.event(TransferEvent::Connecting {
            peer: label.clone(),
        });
        let known: KnownHashes = self.shared.lock().unwrap().hashes.clone();
        let had_known = known.is_some();
        let connected = if source.requires_callback {
            match &self.options.callbacks {
                Some(callbacks) => {
                    callbacks
                        .connect_tuned(source, self.file, known, self.tuning, self.cancel.clone())
                        .await
                }
                None => Err(anyhow!("source needs a callback route")),
            }
        } else {
            Peer::connect_tuned(
                source,
                self.file,
                self.options.identity,
                self.options.client_id,
                self.options.obfuscation,
                known,
                self.tuning,
                self.cancel.clone(),
            )
            .await
        };
        let peer = match connected {
            Ok(peer) => peer,
            Err(error) => {
                self.soft_error(&error);
                let metadata_timeout = error.downcast_ref::<MetadataTimedOut>().is_some();
                self.event(TransferEvent::PeerFailed {
                    peer: label.clone(),
                    kind: if metadata_timeout {
                        "metadata_timeout"
                    } else if error.downcast_ref::<PeerLacksFile>().is_some() {
                        "refused"
                    } else if error.downcast_ref::<HandshakeTimedOut>().is_some() {
                        "handshake_timeout"
                    } else {
                        "unreachable"
                    },
                    error: format!("{error:#}"),
                });
                self.note(|o| {
                    if metadata_timeout {
                        o.metadata_timeouts += 1;
                    } else if error.downcast_ref::<PeerLacksFile>().is_some() {
                        o.refused += 1;
                    } else {
                        // Includes handshake timeouts, refused connections and callbacks that never came.
                        let _ = error.downcast_ref::<HandshakeTimedOut>();
                        o.unreachable += 1;
                    }
                });
                // A peer that only failed to send a hashset can be asked again once another peer
                // (or the disk) has provided one.
                let retry =
                    metadata_timeout && !had_known && self.shared.lock().unwrap().hashes.is_some();
                return Ok((0, retry));
            }
        };
        self.work_peer(peer, &host).await
    }

    async fn work_peer(&self, mut peer: Peer, host: &Option<String>) -> Result<(usize, bool)> {
        // The first authenticated hashset is kept, in memory for the other peers and on disk for
        // the next attempt.
        if self.file.part_count() > 1 {
            let first = {
                let mut shared = self.shared.lock().unwrap();
                if shared.hashes.is_none() {
                    shared.hashes = Some(Arc::new(peer.hashes.clone()));
                    Some(shared.hashset_path.clone())
                } else {
                    None
                }
            };
            if let Some(path) = first {
                save_hashset(path, peer.hashes.clone()).await;
            }
        }
        // Only the first connected peer re-reads what is already on disk (unless a saved hashset
        // already did it before any peer was asked).
        self.scan(&peer.hashes, host).await?;
        if self.all_complete() {
            return Ok((0, false));
        }
        let label = host.clone().unwrap_or_default();
        let (offers, wanted) = {
            let shared = self.shared.lock().unwrap();
            let offers = (0..shared.complete.len())
                .filter(|i| peer.has_part(*i))
                .count();
            let wanted = shared
                .complete
                .iter()
                .enumerate()
                .filter(|(i, done)| !**done && peer.has_part(*i))
                .count();
            (offers, wanted)
        };
        if wanted == 0 {
            self.event(TransferEvent::PeerHasNothingNew {
                peer: label.clone(),
            });
            self.note(|o| o.lacking_parts += 1);
            return Ok((0, false));
        }
        self.event(TransferEvent::PeerReady {
            peer: label.clone(),
            offers,
            wanted,
        });
        self.emit(TransferStage::Queued, host, None);
        let queued_at = std::time::Instant::now();
        let mut last_rank = None;
        if let Err(error) = peer
            .acquire_slot_for(self.tuning.queue_wait, |rank| {
                if last_rank != Some(rank) {
                    last_rank = Some(rank);
                    self.event(TransferEvent::Queued {
                        peer: label.clone(),
                        rank: Some(rank),
                    });
                }
                self.emit(TransferStage::Queued, host, Some(rank))
            })
            .await
        {
            self.soft_error(&error);
            self.event(TransferEvent::QueueFailed {
                peer: label.clone(),
                kind: if error.downcast_ref::<QueueWaitTimedOut>().is_some() {
                    "queue_timeout"
                } else {
                    "refused"
                },
                error: format!("{error:#}"),
            });
            self.note(|o| {
                if error.downcast_ref::<QueueWaitTimedOut>().is_some() {
                    o.queue_timeouts += 1;
                } else {
                    o.refused += 1;
                }
            });
            return Ok((0, false));
        }
        self.event(TransferEvent::SlotGranted {
            peer: label.clone(),
            waited_ms: queued_at.elapsed().as_millis() as u64,
        });
        let mut fetched = 0;
        // What this peer has done for the part it is on now, for the slow-part warning.
        let mut run: Option<(usize, std::time::Instant, usize, bool)> = None;
        loop {
            if self.all_complete() {
                break;
            }
            let wake = self.changed.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            let works = self.next_works(&peer, &label);
            let Some(work) = works.first() else {
                if !self.waiting_for_others(&peer) {
                    break;
                }
                // Every block of the parts this peer has is being fetched elsewhere: if one fails
                // it comes back, and a block outstanding for long is asked again.
                let _ = tokio::time::timeout(Duration::from_millis(250), wake).await;
                check_cancel(self.cancel)?;
                continue;
            };
            let release = |this: &Self| works.iter().for_each(|w| this.release_block(w));
            if let Err(error) = check_cancel(self.cancel) {
                release(self);
                return Err(error);
            }
            if run.is_none_or(|(index, ..)| index != work.index) {
                run = Some((work.index, std::time::Instant::now(), 0, false));
                self.emit(TransferStage::Downloading, host, None);
                self.event(TransferEvent::PartStarted {
                    peer: label.clone(),
                    index: work.index,
                    shadow: work.helping,
                });
            }
            let asked = std::time::Instant::now();
            let ranges: Vec<(u64, u64)> = works.iter().map(|w| (w.from, w.end)).collect();
            let received = match peer.blocks(&ranges).await {
                Ok(received) => received,
                Err(error) => {
                    release(self);
                    self.event(TransferEvent::PartFailed {
                        peer: label.clone(),
                        index: work.index,
                        error: format!("{error:#}"),
                        elapsed_ms: asked.elapsed().as_millis() as u64,
                    });
                    self.soft_error(&error);
                    self.note(|o| o.part_failures += 1);
                    break;
                }
            };
            if let Some((index, since, bytes, flagged)) = run.as_mut() {
                *bytes += received.iter().map(Vec::len).sum::<usize>();
                let elapsed = since.elapsed();
                if !*flagged
                    && elapsed >= Duration::from_secs(20)
                    && (*bytes as f64) / elapsed.as_secs_f64() < 16_384.0
                {
                    *flagged = true;
                    self.event(TransferEvent::PartSlow {
                        peer: label.clone(),
                        index: *index,
                        received: *bytes,
                        elapsed_ms: elapsed.as_millis() as u64,
                    });
                }
            }
            let finished = works
                .iter()
                .zip(&received)
                .find_map(|(w, block)| self.put_block(w, block, &label));
            self.changed.notify_waiters();
            let Some((part, peers, elapsed_ms)) = finished else {
                self.emit(TransferStage::Downloading, host, None);
                continue;
            };
            // The part is only written once it verifies against a hashset whose root matches the
            // user's ED2K link.
            let verified = peer.hashes.get(work.index) == Some(&digest(&part));
            if !verified {
                self.event(TransferEvent::PartFailed {
                    peer: label.clone(),
                    index: work.index,
                    error: format!(
                        "part failed MD4 verification ({peers} peers contributed blocks)"
                    ),
                    elapsed_ms,
                });
                self.soft_error(&anyhow!("ED2K part failed MD4 verification"));
                self.note(|o| o.part_failures += 1);
                if peers > 1 {
                    // Not clear who sent the bad block: fetch the part again, one peer at a time.
                    self.shared.lock().unwrap().solo.insert(work.index);
                    self.changed.notify_waiters();
                    continue;
                }
                break;
            }
            let written = async {
                let mut disk = self.disk.lock().await;
                disk.seek(std::io::SeekFrom::Start(work.index as u64 * PART_SIZE))
                    .await?;
                disk.write_all(&part).await?;
                disk.flush().await?;
                Ok::<_, std::io::Error>(())
            }
            .await;
            if let Err(error) = written {
                return Err(error.into());
            }
            let first = {
                let mut shared = self.shared.lock().unwrap();
                let first = !shared.complete[work.index];
                if first {
                    shared.complete[work.index] = true;
                    shared.verified += part.len() as u64;
                }
                first
            };
            self.changed.notify_waiters();
            if first {
                fetched += 1;
                self.event(TransferEvent::PartDone {
                    peers,
                    peer: label.clone(),
                    index: work.index,
                    bytes: part.len(),
                    elapsed_ms,
                });
            }
            self.emit(TransferStage::Downloading, host, None);
        }
        self.abandon(&label);
        Ok((fetched, false))
    }
}

/// The next late source, or never when the channel is gone.
async fn recv_late(
    late: &mut Option<tokio::sync::mpsc::UnboundedReceiver<FileSource>>,
) -> Option<FileSource> {
    match late {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

/// Resolves when the transfer is cancelled.
async fn wait_cancelled(cancel: &AtomicBool) {
    while !cancel.load(std::sync::atomic::Ordering::Acquire) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The byte range of block `block` in part `index` of `len` bytes.
fn block_work(index: usize, block: usize, len: usize, helping: bool) -> Work {
    let start = index as u64 * PART_SIZE + block as u64 * BLOCK_SIZE;
    let end = (start + BLOCK_SIZE).min(index as u64 * PART_SIZE + len as u64);
    Work {
        index,
        block,
        from: start,
        end,
        helping,
    }
}

/// Length of block `block` in a part of `part_len` bytes.
fn block_len(part_len: usize, block: usize) -> usize {
    (part_len - block * BLOCK_SIZE as usize).min(BLOCK_SIZE as usize)
}

/// The first part, then the last, then the rest in order. Players open a file by reading both ends
/// (an AVI keeps its index at the end, an MP4 often its `moov`), so a preview of a partial download
/// only works once the last part is there too. Like aMule, the part before the last one also comes
/// early when the last one is small (under a third of a part), since the index may span both.
fn preview_parts(parts: usize, last_len: usize) -> usize {
    parts.min(if last_len < PART_SIZE as usize / 3 {
        3
    } else {
        2
    })
}

fn preview_order(parts: usize, last_len: usize) -> impl Iterator<Item = usize> {
    let last = parts.checked_sub(1).filter(|last| *last > 0);
    let before_last = parts
        .checked_sub(2)
        .filter(|before| *before > 0 && last_len < PART_SIZE as usize / 3);
    (0..parts.min(1))
        .chain(last)
        .chain(before_last)
        .chain((1..parts.saturating_sub(1)).filter(move |i| Some(*i) != before_last))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::{peer, server};
    use super::*;
    use std::{
        io::Write,
        sync::{Mutex, atomic::Ordering},
    };

    #[test]
    fn parts_are_fetched_first_then_last_then_in_order() {
        let big = PART_SIZE as usize;
        let order = |parts, last| preview_order(parts, last).collect::<Vec<_>>();
        assert_eq!(order(0, 0), Vec::<usize>::new());
        assert_eq!(order(1, big), vec![0]);
        assert_eq!(order(2, big), vec![0, 1]);
        assert_eq!(order(5, big), vec![0, 4, 1, 2, 3]);
        // A small last part brings the one before it forward too (aMule's rule).
        assert_eq!(order(5, 1000), vec![0, 4, 3, 1, 2]);
        assert_eq!(order(2, 1000), vec![0, 1]);
        let each_once = |parts: usize| {
            let mut all = order(parts, 1000);
            all.sort_unstable();
            all == (0..parts).collect::<Vec<_>>()
        };
        assert!((0..40).all(each_once));
    }

    #[tokio::test]
    async fn preview_map_marks_only_authenticated_readable_parts() {
        use std::io::{Read, Seek};
        let data = vec![42; PART_SIZE as usize + 123];
        let (file, _) = fixture_file(&data);
        let (source, _, server) = fixture(data.clone(), false, false).await;
        let root = tempfile::tempdir().unwrap();
        let stage = root.path().join(format!(
            ".nibiru-ed2k-{}-{}.part",
            file.hash_hex(),
            file.size
        ));
        let mut observed = false;
        download_from_sources(
            &file,
            root.path(),
            &[source],
            [7; 16],
            0,
            Arc::new(AtomicBool::new(false)),
            |progress| {
                if matches!(progress.stage, TransferStage::Completed) {
                    return;
                }
                for (index, verified) in progress.verified_parts.iter().enumerate() {
                    if !verified {
                        continue;
                    }
                    observed = true;
                    let mut disk = std::fs::File::open(&stage).unwrap();
                    disk.seek(std::io::SeekFrom::Start(index as u64 * PART_SIZE))
                        .unwrap();
                    let mut bytes = vec![0; file.part_len(index)];
                    disk.read_exact(&mut bytes).unwrap();
                    assert_eq!(
                        bytes,
                        data[index * PART_SIZE as usize..index * PART_SIZE as usize + bytes.len()]
                    );
                }
            },
        )
        .await
        .unwrap();
        assert!(observed);
        server.await.unwrap();
    }

    pub(crate) fn fixture_file(data: &[u8]) -> (FileLink, Vec<[u8; 16]>) {
        let mut hashes: Vec<_> = data.chunks(PART_SIZE as usize).map(digest).collect();
        if (data.len() as u64).is_multiple_of(PART_SIZE) {
            hashes.push(digest(&[]));
        }
        let hash = if data.len() < PART_SIZE as usize {
            hashes[0]
        } else {
            digest(&hashes.concat())
        };
        (
            FileLink {
                name: "fixture.bin".into(),
                size: data.len() as u64,
                hash,
            },
            hashes,
        )
    }
    pub(crate) async fn fixture(
        data: Vec<u8>,
        corrupt: bool,
        bad_hashset: bool,
    ) -> (
        FileSource,
        Arc<Mutex<Vec<u64>>>,
        tokio::task::JoinHandle<()>,
    ) {
        fixture_with(data, corrupt, bad_hashset, None).await
    }
    /// Like `fixture`, but the peer may advertise only some parts.
    pub(crate) async fn fixture_with(
        data: Vec<u8>,
        corrupt: bool,
        bad_hashset: bool,
        only_parts: Option<Vec<usize>>,
    ) -> (
        FileSource,
        Arc<Mutex<Vec<u64>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let requested = Arc::new(Mutex::new(Vec::new()));
        let requests = requested.clone();
        let (file, hashes) = fixture_file(&data);
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (op, hello) = server::frame(&mut stream).await.unwrap();
            assert_eq!(op, 1);
            assert_eq!(hello[0], 16);
            let mut answer = vec![7; 16];
            answer.extend(123u32.to_le_bytes());
            answer.extend(0u16.to_le_bytes());
            answer.extend(1u32.to_le_bytes());
            answer.extend([3, 1, 0, 0xfe]);
            answer.extend((1u32 << 4).to_le_bytes());
            answer.extend([0; 6]);
            peer::send(&mut stream, 0xe3, 0x4c, &answer).await.unwrap();
            loop {
                let Ok((op, request)) = server::frame(&mut stream).await else {
                    break;
                };
                match op {
                    0x58 => {
                        assert_eq!(request, file.hash);
                        let mut answer = file.hash.to_vec();
                        answer.extend(11u16.to_le_bytes());
                        answer.extend(b"fixture.bin");
                        if peer::send(&mut stream, 0xe3, 0x59, &answer).await.is_err() {
                            break;
                        }
                    }
                    0x4f => {
                        let mut answer = file.hash.to_vec();
                        answer.extend((file.part_count() as u16).to_le_bytes());
                        let mut status = vec![0u8; file.part_count().div_ceil(8)];
                        for part in 0..file.part_count() {
                            if only_parts.as_ref().is_none_or(|p| p.contains(&part)) {
                                status[part / 8] |= 1 << (part % 8);
                            }
                        }
                        answer.extend(status);
                        if peer::send(&mut stream, 0xe3, 0x50, &answer).await.is_err() {
                            break;
                        }
                    }
                    0x51 => {
                        let mut answer = file.hash.to_vec();
                        answer.extend((hashes.len() as u16).to_le_bytes());
                        answer.extend(hashes.concat());
                        if bad_hashset {
                            answer[18] ^= 1;
                        }
                        if peer::send(&mut stream, 0xe3, 0x52, &answer).await.is_err() {
                            break;
                        }
                    }
                    0x54 => {
                        if peer::send(&mut stream, 0xc5, 0x60, &12u16.to_le_bytes())
                            .await
                            .is_err()
                        {
                            break;
                        }
                        if peer::send(&mut stream, 0xe3, 0x55, &[]).await.is_err() {
                            break;
                        }
                    }
                    0x47 => {
                        assert_eq!(request.len(), 40);
                        // Up to three <start,end> pairs; unused pairs are zero.
                        for slot in 0..3 {
                            let from = u32::from_le_bytes(
                                request[16 + 4 * slot..20 + 4 * slot].try_into().unwrap(),
                            ) as usize;
                            let end = u32::from_le_bytes(
                                request[28 + 4 * slot..32 + 4 * slot].try_into().unwrap(),
                            ) as usize;
                            if from == 0 && end == 0 {
                                continue;
                            }
                            assert!(
                                end > from && end - from <= super::super::file::BLOCK_SIZE as usize
                            );
                            requests.lock().unwrap().push(from as u64);
                            let mut bytes = data[from..end].to_vec();
                            if corrupt {
                                bytes[0] ^= 1;
                            }
                            if (from / super::super::file::BLOCK_SIZE as usize).is_multiple_of(2) {
                                // Real split zlib stream across two OP_COMPRESSEDPART frames.
                                let mut z = flate2::write::ZlibEncoder::new(
                                    Vec::new(),
                                    flate2::Compression::fast(),
                                );
                                z.write_all(&bytes).unwrap();
                                let packed = z.finish().unwrap();
                                for chunk in packed.chunks(packed.len().div_ceil(2)) {
                                    let mut answer = file.hash.to_vec();
                                    answer.extend((from as u32).to_le_bytes());
                                    answer.extend((packed.len() as u32).to_le_bytes());
                                    answer.extend(chunk);
                                    if peer::send(&mut stream, 0xc5, 0x40, &answer).await.is_err() {
                                        return;
                                    }
                                }
                            } else {
                                // Non-overlapping fragments intentionally arrive out of order.
                                let mid = from + (end - from) / 2;
                                for (a, b) in [(mid, end), (from, mid)] {
                                    if a == b {
                                        continue;
                                    }
                                    let mut answer = file.hash.to_vec();
                                    answer.extend((a as u32).to_le_bytes());
                                    answer.extend((b as u32).to_le_bytes());
                                    answer.extend(&bytes[a - from..b - from]);
                                    if peer::send(&mut stream, 0xe3, 0x46, &answer).await.is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    _ => panic!("unexpected fixture opcode {op:x}"),
                }
            }
        });
        (
            FileSource {
                client_id: 0x0100007f,
                host: Some(std::net::Ipv4Addr::LOCALHOST),
                port,
                requires_callback: false,
                user_hash: None,
                crypt: 0,
                buddy: None,
            },
            requested,
            handle,
        )
    }
    #[tokio::test]
    async fn real_peer_download_checks_compression_resume_hashes_and_finalization() {
        let data: Vec<u8> = (0..PART_SIZE as usize + 411_001)
            .map(|i| (i % 251) as u8)
            .collect();
        let (file, _) = fixture_file(&data);
        let directory = tempfile::tempdir().unwrap();
        let stage = directory.path().join(format!(
            ".nibiru-ed2k-{}-{}.part",
            file.hash_hex(),
            file.size
        ));
        let mut staged = data.clone();
        staged[PART_SIZE as usize..].fill(0);
        tokio::fs::write(&stage, &staged).await.unwrap();
        let (source, requested, server) = fixture(data.clone(), false, false).await;
        let mut states = Vec::new();
        let target = download_from_sources(
            &file,
            directory.path(),
            &[source],
            [1; 16],
            123,
            Arc::new(AtomicBool::new(false)),
            |p| states.push(p),
        )
        .await
        .unwrap();
        assert_eq!(tokio::fs::read(&target).await.unwrap(), data);
        assert!(!stage.exists());
        assert!(
            requested
                .lock()
                .unwrap()
                .iter()
                .all(|offset| *offset >= PART_SIZE)
        );
        assert!(states.iter().any(|p| p.queue_rank == Some(12)));
        assert_eq!(states.last().unwrap().verified, file.size);
        assert!(matches!(
            states.last().unwrap().stage,
            TransferStage::Completed
        ));
        server.await.unwrap();
        assert!(
            download_from_sources(
                &file,
                directory.path(),
                &[],
                [1; 16],
                123,
                Arc::new(AtomicBool::new(false)),
                |_| {}
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("exists")
        );
    }
    #[tokio::test]
    async fn several_sources_share_one_file_and_each_part_comes_from_a_peer_that_has_it() {
        let data: Vec<u8> = (0..2 * PART_SIZE as usize + 500_000)
            .map(|i| (i % 249) as u8)
            .collect();
        let (file, _) = fixture_file(&data);
        let (a, a_requests, a_server) =
            fixture_with(data.clone(), false, false, Some(vec![0, 2])).await;
        let (b, b_requests, b_server) =
            fixture_with(data.clone(), false, false, Some(vec![1])).await;
        let directory = tempfile::tempdir().unwrap();
        let mut peers = std::collections::HashSet::new();
        let target = download_from_sources(
            &file,
            directory.path(),
            &[a, b],
            [1; 16],
            123,
            Arc::new(AtomicBool::new(false)),
            |p| {
                if let Some(peer) = p.peer {
                    peers.insert(peer);
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(tokio::fs::read(&target).await.unwrap(), data);
        let in_part = |requests: &Arc<Mutex<Vec<u64>>>| {
            requests
                .lock()
                .unwrap()
                .iter()
                .map(|offset| (offset / PART_SIZE) as usize)
                .collect::<std::collections::BTreeSet<_>>()
        };
        assert_eq!(in_part(&a_requests), [0, 2].into());
        assert_eq!(in_part(&b_requests), [1].into());
        assert_eq!(peers.len(), 2);
        a_server.await.unwrap();
        b_server.await.unwrap();
    }

    #[tokio::test]
    async fn a_source_serving_corrupt_data_is_skipped_and_another_source_finishes_the_file() {
        let data: Vec<u8> = (0..PART_SIZE as usize + 100_000)
            .map(|i| (i % 247) as u8)
            .collect();
        let (file, _) = fixture_file(&data);
        let (bad, _, bad_server) = fixture(data.clone(), true, false).await;
        let (good, good_requests, good_server) = fixture(data.clone(), false, false).await;
        let directory = tempfile::tempdir().unwrap();
        let target = download_from_sources(
            &file,
            directory.path(),
            &[bad, good],
            [1; 16],
            123,
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .await
        .unwrap();
        assert_eq!(tokio::fs::read(&target).await.unwrap(), data);
        assert!(!good_requests.lock().unwrap().is_empty());
        let _ = bad_server.await;
        good_server.await.unwrap();
    }

    #[tokio::test]
    async fn corrupt_peer_data_or_hashset_cannot_produce_a_completed_file() {
        for (size, bad_hashset) in [(1025, false), (PART_SIZE as usize + 1, true)] {
            let data = vec![42; size];
            let (file, _) = fixture_file(&data);
            let directory = tempfile::tempdir().unwrap();
            let (source, _, server) = fixture(data, true, bad_hashset).await;
            let result = download_from_sources(
                &file,
                directory.path(),
                &[source],
                [1; 16],
                123,
                Arc::new(AtomicBool::new(false)),
                |_| {},
            )
            .await;
            assert!(result.is_err());
            assert!(!directory.path().join(file.name).exists());
            server.await.unwrap();
        }
    }
    #[tokio::test]
    async fn cancellation_interrupts_stalled_peer_without_exposing_final_file() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let cancel = Arc::new(AtomicBool::new(false));
        let signal = cancel.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            server::frame(&mut stream).await.unwrap();
            signal.store(true, Ordering::Release);
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        });
        let directory = tempfile::tempdir().unwrap();
        let (file, _) = fixture_file(b"abc");
        let source = FileSource {
            client_id: 0x0100007f,
            host: Some(std::net::Ipv4Addr::LOCALHOST),
            port,
            requires_callback: false,
            user_hash: None,
            crypt: 0,
            buddy: None,
        };
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            download_from_sources(
                &file,
                directory.path(),
                &[source],
                [1; 16],
                123,
                cancel,
                |_| {},
            ),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        assert!(!directory.path().join(file.name).exists());
        server.abort();
    }

    #[tokio::test]
    async fn finalization_race_preserves_existing_target_and_verified_partial() {
        let data = b"fixture".to_vec();
        let (file, _) = fixture_file(&data);
        let directory = tempfile::tempdir().unwrap();
        let (source, _, server) = fixture(data, false, false).await;
        let target = directory.path().join(&file.name);
        let result = download_from_sources(
            &file,
            directory.path(),
            &[source],
            [1; 16],
            123,
            Arc::new(AtomicBool::new(false)),
            |p| {
                if p.verified == p.total && matches!(p.stage, TransferStage::Downloading) {
                    std::fs::write(&target, b"existing user file").unwrap();
                }
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(std::fs::read(target).unwrap(), b"existing user file");
        assert!(
            directory
                .path()
                .join(format!(
                    ".nibiru-ed2k-{}-{}.part",
                    file.hash_hex(),
                    file.size
                ))
                .exists()
        );
        server.await.unwrap();
    }

    /// What one connection to a scripted peer does.
    #[derive(Clone, Default)]
    pub(crate) struct Plan {
        /// Never answers the file name / part status request.
        silent_metadata: bool,
        /// Answers the name and status but never the hashset.
        skip_hashset: bool,
        /// Parts it says it has (all when `None`).
        parts: Option<Vec<usize>>,
        /// Pause before each block it sends (a slow peer).
        block_delay: Duration,
        /// Closes the connection after serving this many blocks.
        drop_after_blocks: Option<usize>,
        /// Successful block responses by accepted connection (test observation).
        served_by_connection: Option<Arc<Mutex<Vec<usize>>>>,
    }

    /// A peer that accepts any number of connections; the n-th one follows plan n (the last repeats).
    pub(crate) async fn scripted_peer(
        data: Vec<u8>,
        plans: Vec<Plan>,
    ) -> (FileSource, Arc<Mutex<usize>>, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (file, hashes) = fixture_file(&data);
        let connections = Arc::new(Mutex::new(0usize));
        let counted = connections.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let number = {
                    let mut n = counted.lock().unwrap();
                    *n += 1;
                    *n - 1
                };
                let plan = plans[number.min(plans.len() - 1)].clone();
                let (file, hashes, data) = (file.clone(), hashes.clone(), data.clone());
                tokio::spawn(async move {
                    let mut served_blocks = 0usize;
                    let Ok((op, _)) = server::frame(&mut stream).await else {
                        return;
                    };
                    assert_eq!(op, 1);
                    let mut answer = vec![7; 16];
                    answer.extend(123u32.to_le_bytes());
                    answer.extend(0u16.to_le_bytes());
                    answer.extend(1u32.to_le_bytes());
                    answer.extend([3, 1, 0, 0xfe]);
                    answer.extend((1u32 << 4).to_le_bytes());
                    answer.extend([0; 6]);
                    if peer::send(&mut stream, 0xe3, 0x4c, &answer).await.is_err() {
                        return;
                    }
                    while let Ok((op, request)) = server::frame(&mut stream).await {
                        let sent = match op {
                            0x58 if !plan.silent_metadata => {
                                let mut a = file.hash.to_vec();
                                a.extend(11u16.to_le_bytes());
                                a.extend(b"fixture.bin");
                                peer::send(&mut stream, 0xe3, 0x59, &a).await
                            }
                            0x4f if !plan.silent_metadata => {
                                let mut a = file.hash.to_vec();
                                a.extend((file.part_count() as u16).to_le_bytes());
                                let mut status = vec![0u8; file.part_count().div_ceil(8)];
                                for part in 0..file.part_count() {
                                    if plan.parts.as_ref().is_none_or(|p| p.contains(&part)) {
                                        status[part / 8] |= 1 << (part % 8);
                                    }
                                }
                                a.extend(status);
                                peer::send(&mut stream, 0xe3, 0x50, &a).await
                            }
                            0x51 if !plan.silent_metadata && !plan.skip_hashset => {
                                let mut a = file.hash.to_vec();
                                a.extend((hashes.len() as u16).to_le_bytes());
                                a.extend(hashes.concat());
                                peer::send(&mut stream, 0xe3, 0x52, &a).await
                            }
                            0x54 => {
                                if peer::send(&mut stream, 0xe3, 0x55, &[]).await.is_err() {
                                    return;
                                }
                                Ok(())
                            }
                            0x47 => {
                                let mut sent = Ok(());
                                for slot in 0..3 {
                                    let from = u32::from_le_bytes(
                                        request[16 + 4 * slot..20 + 4 * slot].try_into().unwrap(),
                                    ) as usize;
                                    let end = u32::from_le_bytes(
                                        request[28 + 4 * slot..32 + 4 * slot].try_into().unwrap(),
                                    ) as usize;
                                    if from == 0 && end == 0 {
                                        continue;
                                    }
                                    let mut a = file.hash.to_vec();
                                    a.extend((from as u32).to_le_bytes());
                                    a.extend((end as u32).to_le_bytes());
                                    a.extend(&data[from..end]);
                                    if plan.drop_after_blocks.is_some_and(|k| served_blocks >= k) {
                                        return;
                                    }
                                    served_blocks += 1;
                                    tokio::time::sleep(plan.block_delay).await;
                                    sent = peer::send(&mut stream, 0xe3, 0x46, &a).await;
                                    if sent.is_err() {
                                        break;
                                    }
                                    if let Some(counts) = plan.served_by_connection.as_ref() {
                                        let mut counts = counts.lock().unwrap();
                                        let length = counts.len().max(number + 1);
                                        counts.resize(length, 0);
                                        counts[number] += 1;
                                    }
                                }
                                sent
                            }
                            _ => Ok(()),
                        };
                        if sent.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (
            FileSource {
                client_id: 0x0100007f,
                host: Some(std::net::Ipv4Addr::LOCALHOST),
                port,
                requires_callback: false,
                user_hash: None,
                crypt: 0,
                buddy: None,
            },
            connections,
            handle,
        )
    }

    #[tokio::test]
    async fn preview_parts_get_help_before_fresh_middle_parts() {
        let data: Vec<u8> = (0..PART_SIZE as usize * 5)
            .map(|i| ((i * 13 + i / 71) % 251) as u8)
            .collect();
        let (slow, _, slow_server) = scripted_peer(
            data.clone(),
            vec![Plan {
                block_delay: Duration::from_millis(80),
                ..Plan::default()
            }],
        )
        .await;
        let (fast, _, fast_server) = scripted_peer(data.clone(), vec![Plan::default()]).await;
        let dir = tempfile::tempdir().unwrap();
        let (file, _) = fixture_file(&data);
        let events = Arc::new(Mutex::new(Vec::new()));
        let seen = events.clone();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let late = Mutex::new(Some((sender, fast)));
        let mut opts = options();
        opts.late_sources = Some(receiver);
        opts.events = Some(Arc::new(move |event| {
            if matches!(event, TransferEvent::PartStarted { index: 0, .. })
                && let Some((sender, source)) = late.lock().unwrap().take()
            {
                sender.send(source).unwrap();
            }
            seen.lock().unwrap().push(event);
        }));
        let path = download_with_tuning(
            &file,
            dir.path(),
            &[slow],
            opts,
            quick(),
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .await
        .unwrap();
        assert_eq!(std::fs::read(path).unwrap(), data);
        let events = events.lock().unwrap();
        let helper = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    TransferEvent::PartStarted {
                        index: 0,
                        shadow: true,
                        ..
                    }
                )
            })
            .expect("a fast peer must help with the slow first part");
        let middle = events
            .iter()
            .position(|event| matches!(event, TransferEvent::PartStarted { index: 1..=3, .. }))
            .unwrap();
        assert!(
            helper < middle,
            "middle parts must not delay the preview: {events:?}"
        );
        slow_server.abort();
        fast_server.abort();
    }

    #[tokio::test]
    async fn an_idle_peer_helps_with_the_last_parts_and_the_slow_one_stops() {
        let data = two_part_data();
        // The first connection is slow (about 5 s for part 0 alone), the second is fast.
        let (source, _connections, server) = scripted_peer(
            data.clone(),
            vec![
                Plan {
                    block_delay: Duration::from_millis(100),
                    ..Plan::default()
                },
                Plan::default(),
            ],
        )
        .await;
        let dir = tempfile::tempdir().unwrap();
        let (file, _) = fixture_file(&data);
        let events = Arc::new(Mutex::new(Vec::new()));
        let seen = events.clone();
        let mut opts = options();
        opts.events = Some(Arc::new(move |event| seen.lock().unwrap().push(event)));
        let started = std::time::Instant::now();
        let mut in_flight = 0;
        let path = download_with_tuning(
            &file,
            dir.path(),
            &[source.clone(), source],
            opts,
            quick(),
            Arc::new(AtomicBool::new(false)),
            |progress| in_flight = in_flight.max(progress.in_flight),
        )
        .await
        .unwrap();
        assert!(
            in_flight > 0,
            "bytes of a part in progress were never reported"
        );
        assert_eq!(std::fs::read(path).unwrap(), data);
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "took {:?}: the fast peer should have finished the slow peer's part",
            started.elapsed()
        );
        let events = events.lock().unwrap();
        let done = events
            .iter()
            .filter(|e| matches!(e, TransferEvent::PartDone { .. }))
            .count();
        assert_eq!(done, 2, "{events:?}");
        assert!(
            events
                .iter()
                .any(|e| matches!(e, TransferEvent::PartStarted { .. }))
        );
        server.abort();
    }

    #[tokio::test]
    async fn several_peers_split_the_blocks_of_one_part_and_the_download_stops_when_done() {
        // One part-sized file; each connection sends a block only every 80 ms, so one peer alone
        // would need about 4 s for the 53 blocks.
        let data: Vec<u8> = (0..PART_SIZE as usize - 1000)
            .map(|i| (i % 241) as u8)
            .collect();
        let served = Arc::new(Mutex::new(Vec::new()));
        let slow = Plan {
            served_by_connection: Some(served.clone()),
            block_delay: Duration::from_millis(80),
            ..Plan::default()
        };
        let (source, connections, server) = scripted_peer(data.clone(), vec![slow]).await;
        let (file, _) = fixture_file(&data);
        let dir = tempfile::tempdir().unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let seen = events.clone();
        let mut opts = options();
        opts.events = Some(Arc::new(move |event| seen.lock().unwrap().push(event)));
        let path = tokio::time::timeout(
            Duration::from_secs(30),
            download_with_tuning(
                &file,
                dir.path(),
                &vec![source; 4],
                opts,
                quick(),
                Arc::new(AtomicBool::new(false)),
                |_| {},
            ),
        )
        .await
        .expect("the cooperative transfer must finish")
        .unwrap();
        assert_eq!(std::fs::read(path).unwrap(), data);
        // Verify actual cooperation instead of a wall-clock performance threshold
        // that includes hashing, disk I/O and unrelated load on hosted runners.
        let counts = served.lock().unwrap().clone();
        let blocks = data.len().div_ceil(super::BLOCK_SIZE as usize);
        assert_eq!(counts.len(), 4, "{counts:?}");
        assert!(
            counts.iter().all(|count| *count > 0 && *count < blocks),
            "{counts:?}"
        );
        assert!(counts.iter().sum::<usize>() >= blocks, "{counts:?}");
        assert_eq!(*connections.lock().unwrap(), 4);
        server.abort();
    }

    #[tokio::test]
    async fn the_download_ends_as_soon_as_everything_is_verified() {
        let data = two_part_data();
        let (good, _, good_server) = scripted_peer(data.clone(), vec![Plan::default()]).await;
        let (silent, _, silent_server) = scripted_peer(
            data.clone(),
            vec![Plan {
                silent_metadata: true,
                ..Plan::default()
            }],
        )
        .await;
        let (file, _) = fixture_file(&data);
        let dir = tempfile::tempdir().unwrap();
        let mut sources = vec![good];
        sources.extend(std::iter::repeat_n(silent, 12));
        let verified_at = Arc::new(Mutex::new(None));
        let record = verified_at.clone();
        let mut tuning = quick();
        tuning.metadata_timeout = Duration::from_secs(10);
        let path = download_with_tuning(
            &file,
            dir.path(),
            &sources,
            options(),
            tuning,
            Arc::new(AtomicBool::new(false)),
            move |progress| {
                if progress.verified == progress.total {
                    record
                        .lock()
                        .unwrap()
                        .get_or_insert_with(std::time::Instant::now);
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(std::fs::read(path).unwrap(), data);
        // Measure only the time after the payload was verified. Comparing two entire
        // downloads made CPU/disk contention on shared runners look like a shutdown bug.
        // Silent metadata requests are still pending for 10 seconds at this point.
        let cleanup = verified_at
            .lock()
            .unwrap()
            .expect("no verified progress")
            .elapsed();
        assert!(
            cleanup < Duration::from_millis(450),
            "completion cleanup took {cleanup:?}"
        );
        good_server.abort();
        silent_server.abort();
    }

    #[tokio::test]
    async fn a_part_left_by_a_peer_that_dropped_is_taken_before_new_parts() {
        // Three parts. One peer dies after one block of the part it opened; the other, slow, must take
        // that part over before it opens a new one.
        let data: Vec<u8> = (0..2 * PART_SIZE as usize + 400_000)
            .map(|i| (i % 239) as u8)
            .collect();
        let (dying, _, dying_server) = scripted_peer(
            data.clone(),
            vec![Plan {
                drop_after_blocks: Some(1),
                ..Plan::default()
            }],
        )
        .await;
        let (steady, _, steady_server) = scripted_peer(
            data.clone(),
            vec![Plan {
                block_delay: Duration::from_millis(40),
                ..Plan::default()
            }],
        )
        .await;
        let (file, _) = fixture_file(&data);
        let dir = tempfile::tempdir().unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let seen = events.clone();
        let mut opts = options();
        opts.events = Some(Arc::new(move |event| seen.lock().unwrap().push(event)));
        let path = download_with_tuning(
            &file,
            dir.path(),
            &[dying, steady],
            opts,
            quick(),
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .await
        .unwrap();
        assert_eq!(std::fs::read(path).unwrap(), data);
        let events = events.lock().unwrap();
        // The helper can already be working on the failed part. Whether it starts before
        // or after the failure, it must finish that part before opening fresh work.
        let (failed_at, failed_part) = events
            .iter()
            .enumerate()
            .find_map(|(at, event)| match event {
                TransferEvent::PartFailed { index, .. } => Some((at, *index)),
                _ => None,
            })
            .expect("a part failed");
        let recovered = events[failed_at..]
            .iter()
            .position(|event| {
                matches!(event,
                    TransferEvent::PartDone { index, .. } if *index == failed_part
                )
            })
            .expect("the orphaned part was completed");
        let fresh = events[failed_at..]
            .iter()
            .position(|event| {
                matches!(event,
                    TransferEvent::PartStarted { index, .. } if *index != failed_part
                )
            })
            .unwrap();
        assert!(
            recovered < fresh,
            "fresh work overtook the orphan: {:?}",
            &events[failed_at..]
        );
        dying_server.abort();
        steady_server.abort();
    }

    #[tokio::test]
    async fn sources_found_later_join_a_transfer_that_started_without_them() {
        let data = two_part_data();
        let (good, _, server) = scripted_peer(data.clone(), vec![Plan::default()]).await;
        let (file, _) = fixture_file(&data);
        let dir = tempfile::tempdir().unwrap();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut opts = options();
        opts.late_sources = Some(receiver);
        // The only source shows up half a second after the transfer began (a Kad lookup).
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let _ = sender.send(good);
        });
        let started = std::time::Instant::now();
        let path = download_with_tuning(
            &file,
            dir.path(),
            &[],
            opts,
            quick(),
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .await
        .unwrap();
        assert_eq!(std::fs::read(path).unwrap(), data);
        assert!(started.elapsed() >= Duration::from_millis(500));
        server.abort();
    }

    #[tokio::test]
    async fn a_transfer_with_no_sources_ends_when_the_late_ones_stop_coming() {
        let data = two_part_data();
        let (file, _) = fixture_file(&data);
        let dir = tempfile::tempdir().unwrap();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<FileSource>();
        let mut opts = options();
        opts.late_sources = Some(receiver);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            drop(sender);
        });
        let error = download_with_tuning(
            &file,
            dir.path(),
            &[],
            opts,
            quick(),
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .await
        .unwrap_err();
        assert!(error.downcast_ref::<Stalled>().is_some(), "{error:#}");
    }

    fn quick() -> Tuning {
        Tuning {
            metadata_timeout: Duration::from_millis(700),
            queue_wait: Duration::from_secs(5),
            endgame_after: Duration::from_millis(200),
        }
    }
    fn options() -> TransferOptions {
        TransferOptions {
            identity: [3; 16],
            client_id: 123,
            callbacks: None,
            obfuscation: super::super::obfuscation::Obfuscation::Disabled,
            events: None,
            late_sources: None,
        }
    }
    fn two_part_data() -> Vec<u8> {
        (0..PART_SIZE as usize + 300_000)
            .map(|i| (i % 247) as u8)
            .collect()
    }
    fn stalled(error: &anyhow::Error) -> &Stalled {
        error.downcast_ref::<Stalled>().expect("a stall report")
    }
    async fn attempt(
        file: &FileLink,
        directory: &Path,
        sources: &[FileSource],
    ) -> (Result<PathBuf>, Vec<TransferProgress>) {
        let mut states = Vec::new();
        let result = download_with_tuning(
            file,
            directory,
            sources,
            options(),
            quick(),
            Arc::new(AtomicBool::new(false)),
            |p| states.push(p),
        )
        .await;
        (result, states)
    }

    #[tokio::test]
    async fn a_resume_does_not_need_a_peer_to_send_the_hashset_again() {
        let data = two_part_data();
        let (file, _) = fixture_file(&data);
        let directory = tempfile::tempdir().unwrap();
        // First attempt: the only peer has part 0, so the file stays incomplete.
        let (first, _, first_server) = scripted_peer(
            data.clone(),
            vec![Plan {
                parts: Some(vec![0]),
                ..Plan::default()
            }],
        )
        .await;
        let (result, _) = attempt(&file, directory.path(), &[first]).await;
        let error = result.unwrap_err();
        let report = stalled(&error);
        assert_eq!(report.reason, StallReason::MissingParts);
        assert_eq!((report.missing_parts, report.total_parts), (1, 2));
        first_server.abort();
        let hashset = directory.path().join(format!(
            ".nibiru-ed2k-{}-{}.hashset",
            file.hash_hex(),
            file.size
        ));
        assert!(
            hashset.exists(),
            "the authenticated hashset is kept with the partial file"
        );
        // Second attempt: a peer that has everything but never sends a hashset. Before the hashset was
        // kept this peer timed out ("ED2K peer metadata timed out") and the transfer stayed stuck.
        let (second, _, second_server) = scripted_peer(
            data.clone(),
            vec![Plan {
                skip_hashset: true,
                ..Plan::default()
            }],
        )
        .await;
        let (result, _) = attempt(&file, directory.path(), &[second]).await;
        let target = result.expect("resume completes from the saved hashset");
        assert_eq!(tokio::fs::read(target).await.unwrap(), data);
        assert!(
            !hashset.exists(),
            "the helper file is removed once the file is complete"
        );
        second_server.abort();
    }

    #[tokio::test]
    async fn without_a_saved_hashset_a_silent_peer_is_reported_as_a_metadata_timeout() {
        let data = two_part_data();
        let (file, _) = fixture_file(&data);
        let directory = tempfile::tempdir().unwrap();
        let (peer, _, server) = scripted_peer(
            data,
            vec![Plan {
                skip_hashset: true,
                ..Plan::default()
            }],
        )
        .await;
        let (result, _) = attempt(&file, directory.path(), &[peer]).await;
        let error = result.unwrap_err();
        assert_eq!(stalled(&error).reason, StallReason::MetadataTimeout);
        assert_eq!(stalled(&error).metadata_timeouts, 1);
        let text = format!("{error:#}");
        assert!(
            text.contains("verified parts retained for resume"),
            "{text}"
        );
        assert!(
            text.contains("did not send the file information in time"),
            "{text}"
        );
        server.abort();
    }

    #[tokio::test]
    async fn the_report_names_peers_without_the_missing_parts_instead_of_the_last_timeout() {
        let data = two_part_data();
        let (file, hashes) = fixture_file(&data);
        let directory = tempfile::tempdir().unwrap();
        // Part 0 is already on disk and the hashset is known.
        let stage = directory.path().join(format!(
            ".nibiru-ed2k-{}-{}.part",
            file.hash_hex(),
            file.size
        ));
        let mut staged = data.clone();
        staged[PART_SIZE as usize..].fill(0);
        tokio::fs::write(&stage, &staged).await.unwrap();
        tokio::fs::write(stage.with_extension("hashset"), hashes.concat())
            .await
            .unwrap();
        let (silent, _, silent_server) = scripted_peer(
            data.clone(),
            vec![Plan {
                silent_metadata: true,
                ..Plan::default()
            }],
        )
        .await;
        let (lacking_a, _, a_server) = scripted_peer(
            data.clone(),
            vec![Plan {
                parts: Some(vec![0]),
                ..Plan::default()
            }],
        )
        .await;
        let (lacking_b, _, b_server) = scripted_peer(
            data.clone(),
            vec![Plan {
                parts: Some(vec![0]),
                ..Plan::default()
            }],
        )
        .await;
        let (result, states) =
            attempt(&file, directory.path(), &[silent, lacking_a, lacking_b]).await;
        let error = result.unwrap_err();
        let report = stalled(&error);
        assert_eq!(report.reason, StallReason::MissingParts, "{report}");
        assert_eq!(
            (
                report.sources,
                report.lacking_parts,
                report.metadata_timeouts
            ),
            (3, 2, 1)
        );
        assert_eq!(report.missing_parts, 1);
        // The progress was real from the first moment: the saved hashset checked the partial file
        // before any peer answered.
        let early = states
            .iter()
            .position(|p| matches!(p.stage, TransferStage::Checking))
            .expect("a checking step");
        let after_check = states[early..]
            .iter()
            .find(|p| p.verified > 0)
            .expect("verified bytes");
        assert_eq!(after_check.verified, PART_SIZE);
        assert!(
            after_check.peer.is_none(),
            "reported before any peer was involved"
        );
        for server in [silent_server, a_server, b_server] {
            server.abort();
        }
    }

    #[tokio::test]
    async fn a_peer_that_only_missed_the_hashset_is_tried_again_once_another_peer_provided_it() {
        let data = two_part_data();
        let (file, _) = fixture_file(&data);
        let directory = tempfile::tempdir().unwrap();
        // A has part 0 and sends the hashset; B has part 1 but never sends a hashset.
        let (a, _, a_server) = scripted_peer(
            data.clone(),
            vec![Plan {
                parts: Some(vec![0]),
                ..Plan::default()
            }],
        )
        .await;
        let (b, b_connections, b_server) = scripted_peer(
            data.clone(),
            vec![Plan {
                parts: Some(vec![1]),
                skip_hashset: true,
                ..Plan::default()
            }],
        )
        .await;
        let (result, _) = attempt(&file, directory.path(), &[a, b]).await;
        let target = result.expect("B is asked again with the hashset A provided");
        assert_eq!(tokio::fs::read(target).await.unwrap(), data);
        assert!(
            *b_connections.lock().unwrap() >= 2,
            "B was connected a second time"
        );
        a_server.abort();
        b_server.abort();
    }

    #[tokio::test]
    async fn a_damaged_saved_hashset_is_ignored_and_never_trusted() {
        let data = two_part_data();
        let (file, hashes) = fixture_file(&data);
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(format!(
            ".nibiru-ed2k-{}-{}.hashset",
            file.hash_hex(),
            file.size
        ));
        let mut bad = hashes.concat();
        bad[3] ^= 0xff;
        tokio::fs::write(&path, &bad).await.unwrap();
        assert!(load_hashset(&file, &path).await.is_none());
        tokio::fs::write(&path, &bad[..bad.len() - 5])
            .await
            .unwrap();
        assert!(load_hashset(&file, &path).await.is_none());
        tokio::fs::write(&path, hashes.concat()).await.unwrap();
        assert_eq!(load_hashset(&file, &path).await.unwrap(), hashes);
        // With nothing to try, the report says so and no peer is blamed.
        let (result, _) = attempt(&file, directory.path(), &[]).await;
        let error = result.unwrap_err();
        assert_eq!(stalled(&error).reason, StallReason::NoSources);
    }
}
