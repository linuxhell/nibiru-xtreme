use anyhow::{Context, Result, anyhow, bail};
use futures_util::StreamExt;
use reqwest::{
    Client, StatusCode,
    header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, RANGE},
    redirect::Policy,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::OpenOptions as StdOpenOptions,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
    task::JoinSet,
    time,
};
use url::Url;

use crate::{logging::LogSink, storage};

const MIN_SEGMENTED_SIZE: u64 = 8 * 1024 * 1024;
const RANGE_CHUNK_SIZE: u64 = 4 * 1024 * 1024;
const MAX_HTTP_WORKERS: usize = 16;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Progress {
    pub downloaded: u64,
    pub total: Option<u64>,
    pub speed_bytes_per_second: u64,
}

#[derive(Debug, Clone)]
pub struct DownloadRequest {
    pub url: String,
    pub destination: PathBuf,
}

#[derive(Clone)]
pub struct Engine {
    client: Client,
    log: Arc<LogSink>,
}

impl Engine {
    pub fn new(log: Arc<LogSink>) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("Nibiru-Xtreme/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(10))
            .redirect(Policy::limited(10))
            .https_only(false)
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self { client, log })
    }

    pub async fn download<F>(
        &self,
        request: DownloadRequest,
        cancelled: Arc<AtomicBool>,
        mut on_progress: F,
    ) -> Result<()>
    where
        F: FnMut(Progress) + Send,
    {
        let parsed = Url::parse(&request.url).context("invalid URL")?;
        match parsed.scheme() {
            "http" | "https" => {}
            scheme => bail!("unsupported protocol in bootstrap engine: {scheme}"),
        }

        if let Some(parent) = request.destination.parent() {
            fs::create_dir_all(parent).await?;
        }

        let existing = fs::metadata(&request.destination)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        if existing == 0
            && let Ok((Some(total), ranges)) = self.probe(&request.url).await
            && ranges
            && total >= MIN_SEGMENTED_SIZE
        {
            match self
                .download_segmented(
                    parsed.clone(),
                    &request.destination,
                    total,
                    cancelled.clone(),
                    &mut on_progress,
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(error) if !cancelled.load(Ordering::Acquire) => {
                    self.log.warn(
                        "http.segmented_fallback",
                        &format!("url={} reason={error}", redact_url(&parsed)),
                    );
                }
                Err(error) => return Err(error),
            }
        }

        self.download_single(parsed, request.destination, cancelled, &mut on_progress)
            .await
    }

    async fn download_single<F>(
        &self,
        parsed: Url,
        destination: PathBuf,
        cancelled: Arc<AtomicBool>,
        on_progress: &mut F,
    ) -> Result<()>
    where
        F: FnMut(Progress) + Send,
    {
        let existing = fs::metadata(&destination)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        let mut builder = self.client.get(parsed.clone());
        if existing > 0 {
            builder = builder.header(RANGE, format!("bytes={existing}-"));
        }

        self.log.info(
            "http.request_started",
            &format!(
                "url={} destination={} resume_from={existing} mode=single",
                redact_url(&parsed),
                destination.display()
            ),
        );

        let started = Instant::now();
        let response = builder.send().await.context("HTTP request failed")?;
        let status = response.status();

        if !status.is_success() {
            bail!("HTTP error: {status}");
        }

        let resumed = existing > 0 && status == StatusCode::PARTIAL_CONTENT;
        let response_len = response.content_length();
        let total = response_len.map(|len| if resumed { existing + len } else { len });

        let mut file = if resumed {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&destination)
                .await?
        } else {
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&destination)
                .await?
        };

        let mut downloaded = if resumed { existing } else { 0 };
        let mut last_report = Instant::now();
        let mut last_report_bytes = downloaded;
        let mut stream = response.bytes_stream();

        while let Some(chunk) = stream.next().await {
            if cancelled.load(Ordering::Acquire) {
                file.flush().await?;
                self.log.warn(
                    "http.download_paused",
                    &format!(
                        "destination={} downloaded={downloaded} mode=single",
                        destination.display()
                    ),
                );
                return Err(anyhow!("cancelled"));
            }
            let chunk = chunk.context("HTTP body read failed")?;
            file.write_all(&chunk).await?;
            downloaded = downloaded.saturating_add(chunk.len() as u64);

            let now = Instant::now();
            if now.duration_since(last_report) >= Duration::from_millis(250) {
                let elapsed = now.duration_since(last_report).as_secs_f64().max(0.001);
                let delta = downloaded.saturating_sub(last_report_bytes);
                on_progress(Progress {
                    downloaded,
                    total,
                    speed_bytes_per_second: (delta as f64 / elapsed) as u64,
                });
                last_report = now;
                last_report_bytes = downloaded;
            }
        }

        file.flush().await?;
        on_progress(Progress {
            downloaded,
            total: total.or(Some(downloaded)),
            speed_bytes_per_second: if started.elapsed().as_secs_f64() > 0.0 {
                (downloaded.saturating_sub(if resumed { existing } else { 0 }) as f64
                    / started.elapsed().as_secs_f64()) as u64
            } else {
                0
            },
        });

        self.log.info(
            "http.download_completed",
            &format!(
                "destination={} bytes={} elapsed_ms={} resumed={} mode=single",
                destination.display(),
                downloaded,
                started.elapsed().as_millis(),
                resumed
            ),
        );
        Ok(())
    }

    async fn download_segmented<F>(
        &self,
        parsed: Url,
        destination: &Path,
        total: u64,
        cancelled: Arc<AtomicBool>,
        on_progress: &mut F,
    ) -> Result<()>
    where
        F: FnMut(Progress) + Send,
    {
        let workers = suggested_worker_count(total);
        let part_path = temporary_part_path(destination);
        let _ = fs::remove_file(&part_path).await;

        let file = StdOpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&part_path)?;
        file.set_len(total)?;
        let file = Arc::new(file);

        let next_offset = Arc::new(AtomicU64::new(0));
        let completed = Arc::new(AtomicU64::new(0));
        let mut set = JoinSet::new();

        self.log.info(
            "http.segmented_started",
            &format!(
                "url={} destination={} total={} workers={} chunk_size={}",
                redact_url(&parsed),
                destination.display(),
                total,
                workers,
                RANGE_CHUNK_SIZE
            ),
        );

        for worker_id in 0..workers {
            let client = self.client.clone();
            let url = parsed.clone();
            let file = file.clone();
            let next_offset = next_offset.clone();
            let completed = completed.clone();
            let cancelled = cancelled.clone();
            set.spawn(async move {
                loop {
                    if cancelled.load(Ordering::Acquire) {
                        return Err(anyhow!("cancelled"));
                    }
                    let start = next_offset.fetch_add(RANGE_CHUNK_SIZE, Ordering::AcqRel);
                    if start >= total {
                        break;
                    }
                    let end = start
                        .saturating_add(RANGE_CHUNK_SIZE - 1)
                        .min(total.saturating_sub(1));

                    let response = client
                        .get(url.clone())
                        .header(RANGE, format!("bytes={start}-{end}"))
                        .send()
                        .await
                        .with_context(|| format!("range worker {worker_id} request failed"))?;

                    if response.status() != StatusCode::PARTIAL_CONTENT {
                        bail!(
                            "range worker {worker_id} expected 206, got {}",
                            response.status()
                        );
                    }
                    validate_content_range(
                        response.headers().get(CONTENT_RANGE),
                        start,
                        end,
                        total,
                    )?;

                    let bytes = response
                        .bytes()
                        .await
                        .with_context(|| format!("range worker {worker_id} body failed"))?;
                    let expected = end.saturating_sub(start).saturating_add(1);
                    if bytes.len() as u64 != expected {
                        bail!(
                            "range worker {worker_id} length mismatch expected={expected} got={}",
                            bytes.len()
                        );
                    }

                    storage::write_at(file.clone(), start, bytes.to_vec()).await?;
                    completed.fetch_add(expected, Ordering::AcqRel);
                }
                Ok::<(), anyhow::Error>(())
            });
        }

        let started = Instant::now();
        let mut last_at = Instant::now();
        let mut last_bytes = 0_u64;
        let mut ticker = time::interval(Duration::from_millis(250));

        while !set.is_empty() {
            tokio::select! {
                _ = ticker.tick() => {
                    if cancelled.load(Ordering::Acquire) {
                        set.abort_all();
                        let _ = fs::remove_file(&part_path).await;
                        return Err(anyhow!("cancelled"));
                    }
                    let now = Instant::now();
                    let done = completed.load(Ordering::Acquire).min(total);
                    let elapsed = now.duration_since(last_at).as_secs_f64().max(0.001);
                    let speed = (done.saturating_sub(last_bytes) as f64 / elapsed) as u64;
                    on_progress(Progress {
                        downloaded: done,
                        total: Some(total),
                        speed_bytes_per_second: speed,
                    });
                    last_at = now;
                    last_bytes = done;
                }
                joined = set.join_next() => {
                    match joined {
                        Some(Ok(Ok(()))) => {}
                        Some(Ok(Err(error))) => {
                            set.abort_all();
                            while set.join_next().await.is_some() {}
                            drop(file);
                            let _ = fs::remove_file(&part_path).await;
                            return Err(error);
                        }
                        Some(Err(error)) => {
                            set.abort_all();
                            while set.join_next().await.is_some() {}
                            drop(file);
                            let _ = fs::remove_file(&part_path).await;
                            return Err(anyhow!("range worker panicked: {error}"));
                        }
                        None => break,
                    }
                }
            }
        }

        let done = completed.load(Ordering::Acquire);
        if done != total {
            drop(file);
            let _ = fs::remove_file(&part_path).await;
            bail!("segmented download incomplete expected={total} got={done}");
        }

        file.sync_all()?;
        drop(file);

        if fs::try_exists(destination).await? {
            fs::remove_file(destination).await?;
        }
        fs::rename(&part_path, destination).await?;

        let elapsed = started.elapsed().as_secs_f64().max(0.001);
        on_progress(Progress {
            downloaded: total,
            total: Some(total),
            speed_bytes_per_second: (total as f64 / elapsed) as u64,
        });
        self.log.info(
            "http.download_completed",
            &format!(
                "destination={} bytes={} elapsed_ms={} mode=segmented workers={workers}",
                destination.display(),
                total,
                started.elapsed().as_millis()
            ),
        );
        Ok(())
    }

    pub async fn probe(&self, url: &str) -> Result<(Option<u64>, bool)> {
        let parsed = Url::parse(url)?;
        if !matches!(parsed.scheme(), "http" | "https") {
            bail!("unsupported protocol");
        }

        let head = self.client.head(parsed.clone()).send().await?;
        let total = head
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        let advertised_ranges = head
            .headers()
            .get(ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case("bytes"));

        if total.is_none() {
            return Ok((None, false));
        }

        if advertised_ranges {
            let probe = self
                .client
                .get(parsed)
                .header(RANGE, "bytes=0-0")
                .send()
                .await?;
            if probe.status() == StatusCode::PARTIAL_CONTENT
                && validate_content_range(
                    probe.headers().get(CONTENT_RANGE),
                    0,
                    0,
                    total.unwrap_or_default(),
                )
                .is_ok()
            {
                return Ok((total, true));
            }
        }
        Ok((total, false))
    }
}

fn validate_content_range(
    value: Option<&reqwest::header::HeaderValue>,
    expected_start: u64,
    expected_end: u64,
    expected_total: u64,
) -> Result<()> {
    let raw = value
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| anyhow!("missing Content-Range"))?;
    let rest = raw
        .strip_prefix("bytes ")
        .ok_or_else(|| anyhow!("invalid Content-Range unit"))?;
    let (range, total) = rest
        .split_once('/')
        .ok_or_else(|| anyhow!("invalid Content-Range format"))?;
    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| anyhow!("invalid Content-Range range"))?;
    let start = start.parse::<u64>()?;
    let end = end.parse::<u64>()?;
    let total = total.parse::<u64>()?;
    if start != expected_start || end != expected_end || total != expected_total {
        bail!(
            "unexpected Content-Range expected={expected_start}-{expected_end}/{expected_total} got={raw}"
        );
    }
    Ok(())
}

fn suggested_worker_count(total: u64) -> usize {
    let workers = match total {
        0..=33_554_432 => 2,
        33_554_433..=134_217_728 => 4,
        134_217_729..=536_870_912 => 8,
        536_870_913..=2_147_483_648 => 12,
        _ => 16,
    };
    workers.min(MAX_HTTP_WORKERS)
}

fn temporary_part_path(destination: &Path) -> PathBuf {
    let name = destination
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("download");
    destination.with_file_name(format!(".{name}.nibiru.part"))
}

fn redact_url(url: &Url) -> String {
    let mut copy = url.clone();
    let _ = copy.set_username("");
    let _ = copy.set_password(None);
    copy.set_query(None);
    copy.set_fragment(None);
    copy.to_string()
}

pub fn safe_output_name(value: &str) -> bool {
    let p = Path::new(value);
    p.components().count() == 1
        && !value.is_empty()
        && value != "."
        && value != ".."
        && !value.contains('/')
        && !value.contains('\\')
}

#[cfg(test)]
mod tests {
    use super::{safe_output_name, suggested_worker_count, validate_content_range};
    use reqwest::header::HeaderValue;

    #[test]
    fn rejects_traversal_names() {
        assert!(safe_output_name("file.iso"));
        assert!(!safe_output_name("../file.iso"));
        assert!(!safe_output_name("dir/file.iso"));
        assert!(!safe_output_name("dir\\file.iso"));
    }

    #[test]
    fn scales_workers_with_file_size() {
        assert_eq!(suggested_worker_count(8 * 1024 * 1024), 2);
        assert_eq!(suggested_worker_count(64 * 1024 * 1024), 4);
        assert_eq!(suggested_worker_count(256 * 1024 * 1024), 8);
        assert_eq!(suggested_worker_count(1024 * 1024 * 1024), 12);
        assert_eq!(suggested_worker_count(4 * 1024 * 1024 * 1024), 16);
    }

    #[test]
    fn validates_content_range_exactly() {
        let value = HeaderValue::from_static("bytes 0-1023/4096");
        assert!(validate_content_range(Some(&value), 0, 1023, 4096).is_ok());
        assert!(validate_content_range(Some(&value), 0, 1022, 4096).is_err());
    }
}
