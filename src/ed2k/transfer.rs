use super::{
    FileLink, FileSource,
    file::{PART_SIZE, digest},
    peer::{
        HandshakeTimedOut, KnownHashes, MetadataTimedOut, PartSuperseded, Peer, PeerLacksFile,
        QueueWaitTimedOut, Tuning, check_cancel,
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
    PartSuperseded {
        peer: String,
        index: usize,
    },
}
pub type EventSink = Arc<dyn Fn(TransferEvent) + Send + Sync>;

pub struct TransferOptions {
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
    options: TransferOptions,
    tuning: Tuning,
    cancel: Arc<AtomicBool>,
    mut progress: impl FnMut(TransferProgress) + Send,
) -> Result<PathBuf> {
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
            claimed: vec![false; file.part_count()],
            claimed_at: vec![None; file.part_count()],
            shadows: vec![0; file.part_count()],
            in_flight: vec![0; file.part_count()],
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
    let mut queue: Vec<&FileSource> = sources
        .iter()
        .filter(|s| (s.host.is_some() && !s.requires_callback) || options.callbacks.is_some())
        .take(16)
        .collect();
    let shared_context = &context;
    for _ in 0..MAX_ROUNDS {
        if queue.is_empty() || context.all_complete() {
            break;
        }
        let mut pending = std::mem::take(&mut queue).into_iter();
        let mut running = FuturesUnordered::new();
        // Sources that delivered parts get another connection next round (they may have dropped
        // mid-part); sources that delivered nothing are not retried.
        loop {
            while running.len() < MAX_PARALLEL_PEERS {
                match pending.next() {
                    Some(source) => running.push(shared_context.work_tagged(source)),
                    None => break,
                }
            }
            let Some((source, result)) = running.next().await else {
                break;
            };
            let (fetched, retry) = result?;
            if fetched > 0 || retry {
                queue.push(source);
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

const MAX_PARALLEL_PEERS: usize = 4;
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

struct Shared {
    complete: Vec<bool>,
    /// Parts a worker is fetching right now; no other peer is asked for them.
    claimed: Vec<bool>,
    /// When the part was claimed, and how many other peers also fetch it (reta final).
    claimed_at: Vec<Option<std::time::Instant>>,
    shadows: Vec<u8>,
    /// Bytes of each part received so far, before it is verified.
    in_flight: Vec<u64>,
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
                .in_flight
                .iter()
                .zip(&shared.complete)
                .filter(|(_, done)| !**done)
                .map(|(bytes, _)| *bytes)
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
    /// The next part for this peer: first one nobody is fetching; failing that, in the last stretch,
    /// a part another peer has been fetching for a while (both race, the first to finish wins).
    fn claim(&self, peer: &Peer) -> Option<(usize, bool)> {
        let mut shared = self.shared.lock().unwrap();
        let free = preview_order(shared.complete.len())
            .find(|i| !shared.complete[*i] && !shared.claimed[*i] && peer.has_part(*i));
        if let Some(index) = free {
            shared.claimed[index] = true;
            shared.claimed_at[index] = Some(std::time::Instant::now());
            shared.in_flight[index] = 0;
            return Some((index, false));
        }
        let shadow = (0..shared.complete.len()).find(|i| {
            !shared.complete[*i]
                && shared.claimed[*i]
                && shared.shadows[*i] == 0
                && peer.has_part(*i)
                && shared.claimed_at[*i].is_some_and(|at| at.elapsed() >= self.tuning.endgame_after)
        })?;
        shared.shadows[shadow] += 1;
        Some((shadow, true))
    }
    fn release(&self, index: usize, shadow: bool) {
        let mut shared = self.shared.lock().unwrap();
        if shadow {
            shared.shadows[index] = shared.shadows[index].saturating_sub(1);
        } else {
            shared.claimed[index] = false;
            shared.claimed_at[index] = None;
            shared.in_flight[index] = 0;
        }
        drop(shared);
        self.changed.notify_waiters();
    }
    fn event(&self, event: TransferEvent) {
        if let Some(sink) = &self.options.events {
            sink(event);
        }
    }
    fn blocked_by_others(&self, peer: &Peer) -> bool {
        let shared = self.shared.lock().unwrap();
        (0..shared.complete.len())
            .any(|i| !shared.complete[i] && shared.claimed[i] && peer.has_part(i))
    }

    async fn work_tagged<'s>(
        &self,
        source: &'s FileSource,
    ) -> (&'s FileSource, Result<(usize, bool)>) {
        (source, self.work(source).await)
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
        loop {
            let wake = self.changed.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            let Some((index, shadow)) = self.claim(&peer) else {
                if self.all_complete() || !self.blocked_by_others(&peer) {
                    break;
                }
                // Parts this peer has are being fetched elsewhere: if that fails they come back,
                // and after a while this peer helps with them.
                let _ = tokio::time::timeout(Duration::from_millis(250), wake).await;
                check_cancel(self.cancel)?;
                continue;
            };
            if let Err(error) = check_cancel(self.cancel) {
                self.release(index, shadow);
                return Err(error);
            }
            self.emit(TransferStage::Downloading, host, None);
            self.event(TransferEvent::PartStarted {
                peer: label.clone(),
                index,
                shadow,
            });
            let started = std::time::Instant::now();
            let mut flagged_slow = false;
            let result = peer
                .part_with(index, &mut |received| {
                    let elapsed = started.elapsed();
                    {
                        let mut shared = self.shared.lock().unwrap();
                        if shared.complete[index] {
                            return false;
                        }
                        shared.in_flight[index] = shared.in_flight[index].max(received as u64);
                    }
                    if !flagged_slow
                        && elapsed >= Duration::from_secs(20)
                        && (received as f64) / elapsed.as_secs_f64() < 16_384.0
                    {
                        flagged_slow = true;
                        self.event(TransferEvent::PartSlow {
                            peer: label.clone(),
                            index,
                            received,
                            elapsed_ms: elapsed.as_millis() as u64,
                        });
                    }
                    self.emit(TransferStage::Downloading, host, None);
                    true
                })
                .await;
            let elapsed_ms = started.elapsed().as_millis() as u64;
            let bytes = match result {
                Ok(bytes) => bytes,
                Err(error) if error.downcast_ref::<PartSuperseded>().is_some() => {
                    self.release(index, shadow);
                    self.event(TransferEvent::PartSuperseded {
                        peer: label.clone(),
                        index,
                    });
                    continue;
                }
                Err(error) => {
                    self.release(index, shadow);
                    self.event(TransferEvent::PartFailed {
                        peer: label.clone(),
                        index,
                        error: format!("{error:#}"),
                        elapsed_ms,
                    });
                    self.soft_error(&error);
                    self.note(|o| o.part_failures += 1);
                    break;
                }
            };
            // Data reaches disk only after the entire part verifies against
            // a hashset whose root matches the user's ED2K link.
            let written = async {
                let mut disk = self.disk.lock().await;
                disk.seek(std::io::SeekFrom::Start(index as u64 * PART_SIZE))
                    .await?;
                disk.write_all(&bytes).await?;
                disk.flush().await?;
                Ok::<_, std::io::Error>(())
            }
            .await;
            if let Err(error) = written {
                self.release(index, shadow);
                return Err(error.into());
            }
            let first = {
                let mut shared = self.shared.lock().unwrap();
                let first = !shared.complete[index];
                if first {
                    shared.complete[index] = true;
                    shared.verified += bytes.len() as u64;
                }
                shared.claimed[index] = false;
                shared.in_flight[index] = 0;
                first
            };
            self.changed.notify_waiters();
            if first {
                fetched += 1;
                self.event(TransferEvent::PartDone {
                    peer: label.clone(),
                    index,
                    bytes: bytes.len(),
                    elapsed_ms,
                });
            }
            self.emit(TransferStage::Downloading, host, None);
        }
        Ok((fetched, false))
    }
}

/// The first part, then the last, then the rest in order. Players open a file by reading both ends
/// (an AVI keeps its index at the end, an MP4 often its `moov`), so a preview of a partial download
/// only works once the last part is there too.
fn preview_order(parts: usize) -> impl Iterator<Item = usize> {
    let last = parts.checked_sub(1).filter(|last| *last > 0);
    (0..parts.min(1))
        .chain(last)
        .chain(1..parts.saturating_sub(1))
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
        let order = |parts| preview_order(parts).collect::<Vec<_>>();
        assert_eq!(order(0), Vec::<usize>::new());
        assert_eq!(order(1), vec![0]);
        assert_eq!(order(2), vec![0, 1]);
        assert_eq!(order(5), vec![0, 4, 1, 2, 3]);
        let each_once = |parts: usize| {
            let mut all = order(parts);
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
                        let from = u32::from_le_bytes(request[16..20].try_into().unwrap()) as usize;
                        let end = u32::from_le_bytes(request[28..32].try_into().unwrap()) as usize;
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
                                let from = u32::from_le_bytes(request[16..20].try_into().unwrap())
                                    as usize;
                                let end = u32::from_le_bytes(request[28..32].try_into().unwrap())
                                    as usize;
                                let mut a = file.hash.to_vec();
                                a.extend((from as u32).to_le_bytes());
                                a.extend((end as u32).to_le_bytes());
                                a.extend(&data[from..end]);
                                tokio::time::sleep(plan.block_delay).await;
                                peer::send(&mut stream, 0xe3, 0x46, &a).await
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
