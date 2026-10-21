use super::{
    FileLink, FileSource,
    file::{PART_SIZE, digest},
    peer::{Peer, check_cancel},
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
    pub total: u64,
    pub peer: Option<String>,
    pub queue_rank: Option<u32>,
}
pub struct TransferOptions {
    pub identity: [u8; 16],
    pub client_id: u32,
    pub callbacks: Option<super::CallbackClient>,
}

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
    let stage_clone = stage.clone();
    let disk = tokio::task::spawn_blocking(move || -> Result<std::fs::File> {
        let disk = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(stage_clone)?;
        disk.try_lock()
            .context("ED2K partial file is already in use")?;
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
            verified: 0,
            scanned: false,
            last_error: anyhow!(
                "no usable ED2K sources; a reachable callback route may be required"
            ),
        }),
    };
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
            if result? > 0 {
                queue.push(source);
            }
        }
    }
    check_cancel(&cancel)?;
    if !context.all_complete() {
        let error = context.shared.lock().unwrap().take_error();
        return Err(error).context("ED2K transfer incomplete; verified parts retained for resume");
    }
    let verified = context.shared.lock().unwrap().verified;
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
    progress(TransferProgress {
        stage: TransferStage::Completed,
        verified,
        total: file.size,
        peer: None,
        queue_rank: None,
    });
    Ok(target)
}

const MAX_PARALLEL_PEERS: usize = 4;
const MAX_ROUNDS: usize = 6;

struct Shared {
    complete: Vec<bool>,
    /// Parts a worker is fetching right now; no other peer is asked for them.
    claimed: Vec<bool>,
    verified: u64,
    scanned: bool,
    last_error: anyhow::Error,
}
impl Shared {
    fn take_error(&mut self) -> anyhow::Error {
        std::mem::replace(&mut self.last_error, anyhow!("ED2K transfer failed"))
    }
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
}

impl Context_<'_> {
    fn all_complete(&self) -> bool {
        self.shared.lock().unwrap().complete.iter().all(|v| *v)
    }
    fn emit(&self, stage: TransferStage, peer: &Option<String>, queue_rank: Option<u32>) {
        let verified = self.shared.lock().unwrap().verified;
        (self.progress.lock().unwrap())(TransferProgress {
            stage,
            verified,
            total: self.file.size,
            peer: peer.clone(),
            queue_rank,
        });
    }
    fn soft_error(&self, error: anyhow::Error) {
        self.shared.lock().unwrap().last_error = error;
    }
    fn claim(&self, peer: &Peer) -> Option<usize> {
        let mut shared = self.shared.lock().unwrap();
        let index = (0..shared.complete.len())
            .find(|i| !shared.complete[*i] && !shared.claimed[*i] && peer.has_part(*i))?;
        shared.claimed[index] = true;
        Some(index)
    }
    fn release(&self, index: usize) {
        self.shared.lock().unwrap().claimed[index] = false;
        self.changed.notify_waiters();
    }
    fn blocked_by_others(&self, peer: &Peer) -> bool {
        let shared = self.shared.lock().unwrap();
        (0..shared.complete.len())
            .any(|i| !shared.complete[i] && shared.claimed[i] && peer.has_part(i))
    }

    async fn work_tagged<'s>(&self, source: &'s FileSource) -> (&'s FileSource, Result<usize>) {
        (source, self.work(source).await)
    }

    /// Talks to one source until it has nothing more to give. Source failures are remembered and
    /// reported as `Ok`; only cancellation and local disk errors are returned as `Err`.
    /// Returns how many parts this source delivered.
    async fn work(&self, source: &FileSource) -> Result<usize> {
        check_cancel(self.cancel)?;
        let host = source
            .host
            .map(|h| format!("{h}:{}", source.port))
            .or_else(|| Some(format!("ID {}", source.client_id)));
        self.emit(TransferStage::Connecting, &host, None);
        let connected = if source.requires_callback {
            match &self.options.callbacks {
                Some(callbacks) => {
                    callbacks
                        .connect(source, self.file, self.cancel.clone())
                        .await
                }
                None => Err(anyhow!("source needs a callback route")),
            }
        } else {
            Peer::connect(
                source,
                self.file,
                self.options.identity,
                self.options.client_id,
                self.cancel.clone(),
            )
            .await
        };
        let mut peer = match connected {
            Ok(peer) => peer,
            Err(error) => {
                self.soft_error(error);
                return Ok(0);
            }
        };
        {
            // Only the first connected peer re-reads what is already on disk.
            let _gate = self.scan_gate.lock().await;
            if !self.shared.lock().unwrap().scanned {
                self.emit(TransferStage::Checking, &host, None);
                for index in 0..self.file.part_count() {
                    check_cancel(self.cancel)?;
                    let offset = index as u64 * PART_SIZE;
                    let count = self.file.part_len(index);
                    let mut disk = self.disk.lock().await;
                    if disk.metadata().await?.len() >= offset + count as u64 {
                        let mut bytes = vec![0; count];
                        disk.seek(std::io::SeekFrom::Start(offset)).await?;
                        disk.read_exact(&mut bytes).await?;
                        drop(disk);
                        if digest(&bytes) == peer.hashes[index] {
                            let mut shared = self.shared.lock().unwrap();
                            shared.complete[index] = true;
                            shared.verified += count as u64;
                        }
                    }
                }
                self.shared.lock().unwrap().scanned = true;
            }
        }
        if self.all_complete() {
            return Ok(0);
        }
        if !self
            .shared
            .lock()
            .unwrap()
            .complete
            .iter()
            .enumerate()
            .any(|(i, done)| !*done && peer.has_part(i))
        {
            return Ok(0);
        }
        self.emit(TransferStage::Queued, &host, None);
        if let Err(error) = peer
            .acquire_slot(|rank| self.emit(TransferStage::Queued, &host, Some(rank)))
            .await
        {
            self.soft_error(error);
            return Ok(0);
        }
        let mut fetched = 0;
        loop {
            let wake = self.changed.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            let Some(index) = self.claim(&peer) else {
                if self.all_complete() || !self.blocked_by_others(&peer) {
                    break;
                }
                // Parts this peer has are being fetched elsewhere: if that fails they come back.
                let _ = tokio::time::timeout(Duration::from_millis(250), wake).await;
                check_cancel(self.cancel)?;
                continue;
            };
            if let Err(error) = check_cancel(self.cancel) {
                self.release(index);
                return Err(error);
            }
            self.emit(TransferStage::Downloading, &host, None);
            let bytes = match peer.part(index).await {
                Ok(bytes) => bytes,
                Err(error) => {
                    self.release(index);
                    self.soft_error(error);
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
                self.release(index);
                return Err(error.into());
            }
            {
                let mut shared = self.shared.lock().unwrap();
                shared.complete[index] = true;
                shared.claimed[index] = false;
                shared.verified += bytes.len() as u64;
            }
            self.changed.notify_waiters();
            fetched += 1;
            self.emit(TransferStage::Downloading, &host, None);
        }
        Ok(fetched)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::{peer, server};
    use super::*;
    use std::{
        io::Write,
        sync::{Mutex, atomic::Ordering},
    };

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
}
