//! Direct-transfer protocols. The existing segmented HTTP scheduler is independent.
pub(crate) mod ftp;
pub(crate) mod http3;
pub(crate) mod sftp;

use crate::{engine::Progress, logging::LogSink};
use anyhow::{Context, Result, bail};
use percent_encoding::percent_decode_str;
use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    fs,
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
};

#[derive(Clone, Default)]
pub struct TransportOptions {
    /// Additional DER root certificate. System verification is never disabled.
    pub ca_certificate: Option<PathBuf>,
    /// An out-of-band verified SHA256 SSH host-key fingerprint.
    pub ssh_host_key: Option<String>,
    pub ssh_identity: Option<PathBuf>,
}

pub(crate) fn decoded(value: &str) -> Result<String> {
    let value = percent_decode_str(value)
        .decode_utf8()
        .context("URL component is not UTF-8")?
        .into_owned();
    if value.chars().any(|c| c.is_control()) {
        bail!("control characters are not allowed in protocol URLs");
    }
    Ok(value)
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
        })
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
        let mut buffer = vec![0; 256 * 1024];
        let mut last_at = Instant::now();
        let mut last_bytes = 0;
        loop {
            let count = operation(cancel, async { Ok(reader.read(&mut buffer).await?) }).await?;
            if count == 0 {
                break;
            }
            self.downloaded = self
                .downloaded
                .checked_add(count as u64)
                .context("file length overflow")?;
            if self.total.is_some_and(|total| self.downloaded > total) {
                bail!("remote file exceeds advertised length");
            }
            self.file
                .as_mut()
                .context("staged file is closed")?
                .write_all(&buffer[..count])
                .await?;
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
        let part = self.part.take().context("staged path is missing")?;
        part.persist_noclobber(&self.destination)
            .context("cannot publish completed file without overwriting another file")?;
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

pub(crate) fn tls_config(extra_ca: Option<&Path>) -> Result<Arc<rustls::ClientConfig>> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(path) = extra_ca {
        roots.add(rustls::pki_types::CertificateDer::from(std::fs::read(
            path,
        )?))?;
    }
    Ok(Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth(),
    ))
}
