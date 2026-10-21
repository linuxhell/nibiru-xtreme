use crate::{engine::Progress, logging::LogSink};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};
use tokio::{
    fs,
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

/// Bytes at the end of a kept partial that are fetched again and compared before resuming.
/// They cover what an interrupted write might not have reached the disk with, and prove the
/// server is really continuing the same content from the offset we asked for.
const RESUME_OVERLAP: u64 = 8 * 1024 * 1024;
const SYNC_EVERY: u64 = 4 * 1024 * 1024;

/// What must be unchanged on the remote side for a kept partial to be continued.
/// `key` never contains credentials or query data.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Validator {
    pub key: String,
    pub size: u64,
    pub mtime: Option<String>,
}

/// The server did not continue the bytes we already hold; the partial was discarded.
#[derive(Debug)]
pub(crate) struct ResumeMismatch;
impl std::fmt::Display for ResumeMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("remote data does not continue the saved partial file")
    }
}
impl std::error::Error for ResumeMismatch {}

struct ResumeFiles {
    part: PathBuf,
    meta: PathBuf,
}

/// Also observes cancellation while connecting, authenticating, or awaiting a reply.
pub(crate) async fn operation<T>(
    cancel: &AtomicBool,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::pin!(future);
    let deadline = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(deadline);
    loop {
        if cancel.load(Ordering::Acquire) {
            bail!("cancelled");
        }
        tokio::select! {
            result = &mut future => return result,
            _ = &mut deadline => bail!("protocol operation timed out"),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
}

/// A completed destination is exposed only after length validation, protocol
/// acknowledgement and durable sync. Existing files are never truncated.
pub(crate) struct StagedFile {
    file: Option<fs::File>,
    part: Option<tempfile::TempPath>,
    destination: PathBuf,
    downloaded: u64,
    total: Option<u64>,
    started: Instant,
    /// Kept across interruptions and continued by `resumable`.
    resume: Option<ResumeFiles>,
    verify: Vec<u8>,
    verified: usize,
    unsynced: u64,
}

impl StagedFile {
    pub(crate) async fn new(destination: &Path, total: Option<u64>) -> Result<Self> {
        if fs::try_exists(destination).await? {
            bail!("destination already exists; choose another output file");
        }
        if let Some(parent) = destination.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent).await?;
        }
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let temporary = tempfile::Builder::new()
            .prefix(".nibiru-")
            .suffix(".part")
            .tempfile_in(parent)?;
        let (file, part) = temporary.into_parts();
        let file = fs::File::from_std(file);
        Ok(Self {
            file: Some(file),
            part: Some(part),
            destination: destination.to_owned(),
            downloaded: 0,
            total,
            started: Instant::now(),
            resume: None,
            verify: Vec::new(),
            verified: 0,
            unsynced: 0,
        })
    }

    /// Like `new`, but an interrupted download leaves a partial that a later call continues, as
    /// long as `validator` still matches. Returns the offset to request from the server.
    pub(crate) async fn resumable(
        destination: &Path,
        validator: &Validator,
    ) -> Result<(Self, u64)> {
        if fs::try_exists(destination).await? {
            bail!("destination already exists; choose another output file");
        }
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).await?;
        let name = destination
            .file_name()
            .context("destination has no file name")?
            .to_string_lossy();
        let files = ResumeFiles {
            part: parent.join(format!(".{name}.nibiru-part")),
            meta: parent.join(format!(".{name}.nibiru-part.json")),
        };
        let saved: Option<Validator> = match fs::read(&files.meta).await {
            Ok(bytes) if bytes.len() <= 4096 => serde_json::from_slice(&bytes).ok(),
            _ => None,
        };
        let length = match fs::symlink_metadata(&files.part).await {
            Ok(m) if m.is_file() => Some(m.len()),
            _ => None,
        };
        let usable = saved.as_ref() == Some(validator)
            && length.is_some_and(|l| l > 0 && l <= validator.size);
        let (file, start, verify) = if usable {
            let length = length.unwrap_or_default();
            let mut file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&files.part)
                .await?;
            let start = length - length.min(RESUME_OVERLAP);
            file.seek(std::io::SeekFrom::Start(start)).await?;
            let mut verify = vec![0; (length - start) as usize];
            file.read_exact(&mut verify).await?;
            file.seek(std::io::SeekFrom::Start(start)).await?;
            (file, start, verify)
        } else {
            let _ = fs::remove_file(&files.part).await;
            let _ = fs::remove_file(&files.meta).await;
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&files.part)
                .await
                .context("cannot create the partial file")?;
            let staged_meta = parent.join(format!(".{name}.nibiru-part.json.new"));
            fs::write(&staged_meta, serde_json::to_vec(validator)?).await?;
            fs::rename(&staged_meta, &files.meta).await?;
            (file, 0, Vec::new())
        };
        let std_file = file.into_std().await;
        let std_file = tokio::task::spawn_blocking(move || {
            std_file
                .try_lock()
                .map(|()| std_file)
                .map_err(|_| anyhow::anyhow!("the partial file is already in use"))
        })
        .await??;
        Ok((
            Self {
                file: Some(fs::File::from_std(std_file)),
                part: None,
                destination: destination.to_owned(),
                downloaded: start,
                total: Some(validator.size),
                started: Instant::now(),
                resume: Some(files),
                verify,
                verified: 0,
                unsynced: 0,
            },
            start,
        ))
    }

    /// The server cannot continue from an offset: start over from byte 0 in the same partial.
    pub(crate) async fn restart(&mut self) -> Result<()> {
        let file = self.file.as_mut().context("staged file is closed")?;
        file.set_len(0).await?;
        file.seek(std::io::SeekFrom::Start(0)).await?;
        self.downloaded = 0;
        self.verify.clear();
        self.verified = 0;
        Ok(())
    }

    async fn discard(&mut self) {
        drop(self.file.take());
        if let Some(files) = self.resume.take() {
            let _ = fs::remove_file(&files.part).await;
            let _ = fs::remove_file(&files.meta).await;
        }
    }

    pub(crate) async fn copy<R, F>(
        &mut self,
        reader: &mut R,
        cancel: &AtomicBool,
        log: &LogSink,
        protocol: &str,
        on_progress: &mut F,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin,
        F: FnMut(Progress) + Send,
    {
        let result = self
            .copy_inner(reader, cancel, log, protocol, on_progress)
            .await;
        if result.is_err()
            && let Some(file) = self.file.as_mut()
        {
            // An interrupted partial must be on disk before the next attempt trusts it.
            let _ = file.sync_data().await;
        }
        result
    }

    async fn copy_inner<R, F>(
        &mut self,
        reader: &mut R,
        cancel: &AtomicBool,
        log: &LogSink,
        protocol: &str,
        on_progress: &mut F,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin,
        F: FnMut(Progress) + Send,
    {
        let mut buffer = vec![0; 256 * 1024];
        let mut last_at = Instant::now();
        let mut last_bytes = 0;
        loop {
            let count = operation(cancel, async { Ok(reader.read(&mut buffer).await?) }).await?;
            if count == 0 {
                break;
            }
            // The bytes already held are re-read from `downloaded`, so count them from there.
            self.downloaded = self
                .downloaded
                .checked_add(count as u64)
                .context("file length overflow")?;
            if self.total.is_some_and(|total| self.downloaded > total) {
                self.discard().await;
                bail!("remote file exceeds advertised length");
            }
            let pending = self.verify.len() - self.verified;
            if pending > 0 {
                let compared = pending.min(count);
                if buffer[..compared] != self.verify[self.verified..self.verified + compared] {
                    self.discard().await;
                    return Err(ResumeMismatch.into());
                }
                self.verified += compared;
            }
            self.file
                .as_mut()
                .context("staged file is closed")?
                .write_all(&buffer[..count])
                .await?;
            self.unsynced += count as u64;
            if self.unsynced >= SYNC_EVERY && self.resume.is_some() {
                self.file
                    .as_mut()
                    .context("staged file is closed")?
                    .sync_data()
                    .await?;
                self.unsynced = 0;
            }
            if last_at.elapsed() >= Duration::from_millis(250) {
                let speed = ((self.downloaded - last_bytes) as f64
                    / last_at.elapsed().as_secs_f64()) as u64;
                on_progress(Progress {
                    downloaded: self.downloaded,
                    total: self.total,
                    speed_bytes_per_second: speed,
                });
                if last_at.elapsed() >= Duration::from_secs(1)
                    || self.downloaded - last_bytes >= 16 * 1024 * 1024
                {
                    log.info("transfer.progress", &format!("protocol={protocol} downloaded={} total={:?} speed_bytes_per_second={speed}", self.downloaded, self.total));
                }
                last_at = Instant::now();
                last_bytes = self.downloaded;
            }
        }
        if self.total.is_some_and(|total| total != self.downloaded) {
            bail!("remote file ended before advertised length");
        }
        Ok(())
    }

    pub(crate) async fn commit<F>(
        mut self,
        cancel: &AtomicBool,
        log: &LogSink,
        protocol: &str,
        on_progress: &mut F,
    ) -> Result<()>
    where
        F: FnMut(Progress) + Send,
    {
        if cancel.load(Ordering::Acquire) {
            bail!("cancelled");
        }
        let file = self.file.take().context("staged file is closed")?;
        file.sync_all().await?;
        drop(file);
        if cancel.load(Ordering::Acquire) {
            bail!("cancelled");
        }
        // persist_noclobber uses the platform's no-overwrite publication primitive.
        // TempPath also cleans up on errors and task cancellation.
        if let Some(files) = self.resume.take() {
            let part = tempfile::TempPath::try_from_path(&files.part)?;
            if let Err(error) = part.persist_noclobber(&self.destination) {
                // Keep the verified partial for another attempt.
                let _ = error.path.keep();
                bail!(
                    "cannot publish completed file without overwriting another file: {}",
                    error.error
                );
            }
            let _ = fs::remove_file(&files.meta).await;
        } else {
            let part = self.part.take().context("staged path is missing")?;
            part.persist_noclobber(&self.destination)
                .context("cannot publish completed file without overwriting another file")?;
        }
        on_progress(Progress {
            downloaded: self.downloaded,
            total: Some(self.downloaded),
            speed_bytes_per_second: (self.downloaded as f64
                / self.started.elapsed().as_secs_f64().max(0.001))
                as u64,
        });
        log.info(
            "transfer.completed",
            &format!(
                "protocol={protocol} bytes={} elapsed_ms={}",
                self.downloaded,
                self.started.elapsed().as_millis()
            ),
        );
        Ok(())
    }
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        drop(self.file.take());
    }
}
