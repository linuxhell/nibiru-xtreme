use super::{
    FileLink, FileSource,
    file::{PART_SIZE, digest},
    peer::{Peer, check_cancel},
};
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
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
    mut progress: impl FnMut(TransferProgress),
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
    let mut disk = tokio::fs::File::from_std(disk);
    let mut complete = vec![false; file.part_count()];
    let mut scanned = false;
    let mut verified = 0u64;
    let mut last_error =
        anyhow!("no directly reachable ED2K sources; Low-ID callbacks are not implemented yet");
    // Bounded attempt count avoids unbounded connections to a server-supplied list.
    for source in sources
        .iter()
        .filter(|s| s.host.is_some() && !s.requires_callback)
        .take(16)
    {
        check_cancel(&cancel)?;
        let host = source.host.map(|h| format!("{h}:{}", source.port));
        progress(TransferProgress {
            stage: TransferStage::Connecting,
            verified,
            total: file.size,
            peer: host.clone(),
            queue_rank: None,
        });
        let mut peer = match Peer::connect(source, file, identity, client_id, cancel.clone()).await
        {
            Ok(peer) => peer,
            Err(error) => {
                last_error = error;
                continue;
            }
        };
        if !scanned {
            progress(TransferProgress {
                stage: TransferStage::Checking,
                verified,
                total: file.size,
                peer: host.clone(),
                queue_rank: None,
            });
            let len = disk.metadata().await?.len();
            for (index, done) in complete.iter_mut().enumerate() {
                check_cancel(&cancel)?;
                let offset = index as u64 * PART_SIZE;
                let count = file.part_len(index);
                if len >= offset + count as u64 {
                    let mut bytes = vec![0; count];
                    disk.seek(std::io::SeekFrom::Start(offset)).await?;
                    disk.read_exact(&mut bytes).await?;
                    if digest(&bytes) == peer.hashes[index] {
                        *done = true;
                        verified += count as u64;
                    }
                }
            }
            scanned = true;
        }
        if complete.iter().all(|v| *v) {
            break;
        }
        if !complete
            .iter()
            .enumerate()
            .any(|(i, done)| !*done && peer.has_part(i))
        {
            continue;
        }
        progress(TransferProgress {
            stage: TransferStage::Queued,
            verified,
            total: file.size,
            peer: host.clone(),
            queue_rank: None,
        });
        if let Err(error) = peer
            .acquire_slot(|rank| {
                progress(TransferProgress {
                    stage: TransferStage::Queued,
                    verified,
                    total: file.size,
                    peer: host.clone(),
                    queue_rank: Some(rank),
                })
            })
            .await
        {
            last_error = error;
            continue;
        }
        for (index, done) in complete.iter_mut().enumerate() {
            check_cancel(&cancel)?;
            if *done || !peer.has_part(index) {
                continue;
            }
            progress(TransferProgress {
                stage: TransferStage::Downloading,
                verified,
                total: file.size,
                peer: host.clone(),
                queue_rank: None,
            });
            let bytes = match peer.part(index).await {
                Ok(bytes) => bytes,
                Err(error) => {
                    last_error = error;
                    break;
                }
            };
            // Data reaches disk only after the entire part verifies against
            // a hashset whose root matches the user's ED2K link.
            disk.seek(std::io::SeekFrom::Start(index as u64 * PART_SIZE))
                .await?;
            disk.write_all(&bytes).await?;
            disk.flush().await?;
            *done = true;
            verified += bytes.len() as u64;
            progress(TransferProgress {
                stage: TransferStage::Downloading,
                verified,
                total: file.size,
                peer: host.clone(),
                queue_rank: None,
            });
        }
        if complete.iter().all(|v| *v) {
            break;
        }
    }
    check_cancel(&cancel)?;
    if !complete.iter().all(|v| *v) {
        return Err(last_error)
            .context("ED2K transfer incomplete; verified parts retained for resume");
    }
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

#[cfg(test)]
mod tests {
    use super::super::{peer, server};
    use super::*;
    use std::{
        io::Write,
        sync::{Mutex, atomic::Ordering},
    };

    fn fixture_file(data: &[u8]) -> (FileLink, Vec<[u8; 16]>) {
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
    async fn fixture(
        data: Vec<u8>,
        corrupt: bool,
        bad_hashset: bool,
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
                        answer.extend(vec![255; file.part_count().div_ceil(8)]);
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
