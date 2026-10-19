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
const MAX_HTTP_RANGE_SIZE: u64 = 32 * 1024 * 1024;
const MAX_HTTP_WORKERS: usize = 16;
const HTTP2_CONNECTION_WINDOW: u32 = 16 * 1024 * 1024;

fn claim_range(next: &AtomicU64, total: u64, workers: usize) -> Option<(u64, u64)> {
    loop {
        let start = next.load(Ordering::Acquire);
        if start >= total {
            return None;
        }
        let remaining = total - start;
        let size = if remaining > MAX_HTTP_RANGE_SIZE * workers as u64 {
            MAX_HTTP_RANGE_SIZE
        } else {
            RANGE_CHUNK_SIZE
        };
        let end_exclusive = start + remaining.min(size);
        if next
            .compare_exchange_weak(start, end_exclusive, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Some((start, end_exclusive - 1));
        }
    }
}

async fn write_range_block(
    file: Arc<std::fs::File>,
    offset: u64,
    buffer: Vec<u8>,
    completed: &AtomicU64,
) -> Result<u64> {
    let len = buffer.len() as u64;
    let started = Instant::now();
    storage::write_at(file, offset, buffer).await?;
    completed.fetch_add(len, Ordering::AcqRel);
    Ok(started.elapsed().as_micros() as u64)
}

#[cfg(test)]
#[path = "engine/h2_tests.rs"]
mod h2_tests;

#[derive(Default)]
struct RangeMetrics {
    ranges: AtomicU64,
    http1_ranges: AtomicU64,
    http2_ranges: AtomicU64,
    header_wait_us: AtomicU64,
    body_wait_us: AtomicU64,
    storage_wait_us: AtomicU64,
}

impl RangeMetrics {
    fn report(&self, log: &LogSink) {
        log.info(
            "http.segmented_metrics",
            &format!(
                "ranges={} http1_ranges={} http2_ranges={} header_wait_us_sum={} body_wait_us_sum={} storage_wait_us_sum={} timing_scope=worker_sum",
                self.ranges.load(Ordering::Relaxed),
                self.http1_ranges.load(Ordering::Relaxed),
                self.http2_ranges.load(Ordering::Relaxed),
                self.header_wait_us.load(Ordering::Relaxed),
                self.body_wait_us.load(Ordering::Relaxed),
                self.storage_wait_us.load(Ordering::Relaxed),
            ),
        );
    }
}

struct TransferLog {
    mode: &'static str,
    started: Instant,
    last_at: Instant,
    last_bytes: u64,
}

impl TransferLog {
    fn new(mode: &'static str, started: Instant, initial_bytes: u64) -> Self {
        Self {
            mode,
            started,
            last_at: started,
            last_bytes: initial_bytes,
        }
    }

    fn record(
        &mut self,
        log: &LogSink,
        downloaded: u64,
        total: Option<u64>,
        workers_remaining: usize,
        force: bool,
    ) {
        let now = Instant::now();
        let interval = now.duration_since(self.last_at);
        if !force && interval < Duration::from_secs(1) {
            return;
        }
        let delta = downloaded.saturating_sub(self.last_bytes);
        let speed = (delta as f64 / interval.as_secs_f64().max(0.001)) as u64;
        let total_text = total.map_or_else(|| "unknown".to_owned(), |value| value.to_string());
        let remaining = total.map_or_else(
            || "unknown".to_owned(),
            |value| value.saturating_sub(downloaded).to_string(),
        );
        log.info(
            "http.progress",
            &format!(
                "mode={} downloaded={downloaded} total={total_text} remaining={remaining} interval_bytes={delta} interval_ms={} speed_bytes_per_second={speed} elapsed_ms={} workers_remaining={workers_remaining}",
                self.mode,
                interval.as_millis(),
                now.duration_since(self.started).as_millis(),
            ),
        );
        self.last_at = now;
        self.last_bytes = downloaded;
    }
}

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
        Self::build(log, false)
    }

    pub fn with_http1_only(log: Arc<LogSink>) -> Result<Self> {
        Self::build(log, true)
    }

    fn build(log: Arc<LogSink>, http1_only: bool) -> Result<Self> {
        let client = Self::client_builder(http1_only)
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self { client, log })
    }

    fn client_builder(http1_only: bool) -> reqwest::ClientBuilder {
        let mut builder = Client::builder()
            .user_agent(concat!("Nibiru-Xtreme/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(10))
            .http2_initial_connection_window_size(HTTP2_CONNECTION_WINDOW)
            .http2_initial_stream_window_size(RANGE_CHUNK_SIZE as u32)
            .redirect(Policy::limited(10))
            .https_only(false);
        if http1_only {
            builder = builder.http1_only();
        }
        builder
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
        self.log.info(
            "http.response",
            &format!(
                "mode=single version={:?} status={} url={}",
                response.version(),
                response.status(),
                redact_url(response.url())
            ),
        );
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
        let mut telemetry = TransferLog::new("single", started, downloaded);
        let mut last_report = Instant::now();
        let mut last_report_bytes = downloaded;
        let mut stream = response.bytes_stream();
        let mut log_ticker = time::interval(Duration::from_secs(1));
        log_ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        loop {
            let chunk = tokio::select! {
                chunk = stream.next() => match chunk {
                    Some(chunk) => chunk,
                    None => break,
                },
                _ = log_ticker.tick() => {
                    telemetry.record(&self.log, downloaded, total, 1, false);
                    continue;
                }
            };
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

        telemetry.record(&self.log, downloaded, total, 0, true);
        let transfer_ms = started.elapsed().as_millis();
        self.log.info(
            "http.finalization_started",
            &format!("mode=single downloaded={downloaded} transfer_ms={transfer_ms}"),
        );
        let finalizing = Instant::now();
        file.flush().await?;
        let finalize_ms = finalizing.elapsed().as_millis();
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
                "destination={} bytes={} elapsed_ms={} resumed={} mode=single transfer_ms={transfer_ms} finalize_ms={finalize_ms}",
                destination.display(),
                downloaded,
                started.elapsed().as_millis(),
                resumed,
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
        let started = Instant::now();
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
        let metrics = Arc::new(RangeMetrics::default());
        let mut set = JoinSet::new();

        self.log.info(
            "http.segmented_started",
            &format!(
                "url={} destination={} total={} workers={} chunk_size={} max_range_size={MAX_HTTP_RANGE_SIZE}",
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
            let metrics = metrics.clone();
            let log = self.log.clone();
            set.spawn(async move {
                loop {
                    if cancelled.load(Ordering::Acquire) {
                        return Err(anyhow!("cancelled"));
                    }
                    let Some((start, end)) = claim_range(&next_offset, total, workers) else {
                        break;
                    };

                    let requesting = Instant::now();
                    let response = client
                        .get(url.clone())
                        .header(RANGE, format!("bytes={start}-{end}"))
                        .send()
                        .await
                        .with_context(|| format!("range worker {worker_id} request failed"))?;
                    metrics
                        .header_wait_us
                        .fetch_add(requesting.elapsed().as_micros() as u64, Ordering::Relaxed);
                    metrics.ranges.fetch_add(1, Ordering::Relaxed);
                    match response.version() {
                        reqwest::Version::HTTP_10 | reqwest::Version::HTTP_11 => {
                            metrics.http1_ranges.fetch_add(1, Ordering::Relaxed);
                        }
                        reqwest::Version::HTTP_2 => {
                            metrics.http2_ranges.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {}
                    }
                    if start == 0 {
                        log.info(
                            "http.response",
                            &format!(
                                "mode=segmented version={:?} status={} url={}",
                                response.version(),
                                response.status(),
                                redact_url(response.url())
                            ),
                        );
                    }

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

                    let expected = end.saturating_sub(start).saturating_add(1);
                    let mut body = response.bytes_stream();
                    let mut received = 0_u64;
                    let mut written = 0_u64;
                    let mut body_wait_us = 0_u64;
                    let mut storage_wait_us = 0_u64;
                    let mut buffer = Vec::with_capacity(RANGE_CHUNK_SIZE as usize);
                    loop {
                        let receiving = Instant::now();
                        let chunk = body.next().await;
                        body_wait_us += receiving.elapsed().as_micros() as u64;
                        let Some(chunk) = chunk else {
                            break;
                        };
                        if cancelled.load(Ordering::Acquire) {
                            bail!("cancelled");
                        }
                        let chunk = chunk
                            .with_context(|| format!("range worker {worker_id} body failed"))?;
                        if chunk.len() as u64 > expected.saturating_sub(received) {
                            bail!(
                                "range worker {worker_id} body exceeds expected length {expected}"
                            );
                        }
                        received += chunk.len() as u64;
                        let mut part = chunk.as_ref();
                        while !part.is_empty() {
                            let take = part.len().min(RANGE_CHUNK_SIZE as usize - buffer.len());
                            buffer.extend_from_slice(&part[..take]);
                            part = &part[take..];
                            if buffer.len() == RANGE_CHUNK_SIZE as usize {
                                let block = std::mem::take(&mut buffer);
                                storage_wait_us += write_range_block(
                                    file.clone(),
                                    start + written,
                                    block,
                                    &completed,
                                )
                                .await?;
                                written += RANGE_CHUNK_SIZE;
                                buffer = Vec::with_capacity(RANGE_CHUNK_SIZE as usize);
                            }
                        }
                    }
                    if received != expected {
                        bail!(
                            "range worker {worker_id} length mismatch expected={expected} got={}",
                            received
                        );
                    }
                    if !buffer.is_empty() {
                        storage_wait_us +=
                            write_range_block(file.clone(), start + written, buffer, &completed)
                                .await?;
                    }
                    metrics
                        .body_wait_us
                        .fetch_add(body_wait_us, Ordering::Relaxed);
                    metrics
                        .storage_wait_us
                        .fetch_add(storage_wait_us, Ordering::Relaxed);
                }
                Ok::<(), anyhow::Error>(())
            });
        }

        let mut telemetry = TransferLog::new("segmented", started, 0);
        let mut last_at = Instant::now();
        let mut last_bytes = 0_u64;
        let mut ticker = time::interval(Duration::from_millis(250));
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

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
                    telemetry.record(&self.log, done, Some(total), set.len(), false);
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

        metrics.report(&self.log);
        telemetry.record(&self.log, done, Some(total), 0, true);
        let transfer_ms = started.elapsed().as_millis();
        self.log.info(
            "http.finalization_started",
            &format!("mode=segmented downloaded={done} transfer_ms={transfer_ms}"),
        );
        let finalizing = Instant::now();
        file.sync_all()?;
        drop(file);

        if fs::try_exists(destination).await? {
            fs::remove_file(destination).await?;
        }
        fs::rename(&part_path, destination).await?;
        let finalize_ms = finalizing.elapsed().as_millis();

        let elapsed = started.elapsed().as_secs_f64().max(0.001);
        on_progress(Progress {
            downloaded: total,
            total: Some(total),
            speed_bytes_per_second: (total as f64 / elapsed) as u64,
        });
        self.log.info(
            "http.download_completed",
            &format!(
                "destination={} bytes={} elapsed_ms={} mode=segmented workers={workers} transfer_ms={transfer_ms} finalize_ms={finalize_ms}",
                destination.display(),
                total,
                started.elapsed().as_millis(),
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
    use super::{
        MAX_HTTP_RANGE_SIZE, RANGE_CHUNK_SIZE, claim_range, safe_output_name,
        suggested_worker_count, validate_content_range,
    };
    use reqwest::header::HeaderValue;
    use std::sync::{Arc, atomic::AtomicU64};

    #[test]
    fn concurrent_range_claims_cover_file_once_and_shrink_at_tail() {
        let total = 4 * 1024 * 1024 * 1024 + 321;
        let workers = 16;
        let next = Arc::new(AtomicU64::new(0));
        let tasks: Vec<_> = (0..workers)
            .map(|_| {
                let next = next.clone();
                std::thread::spawn(move || {
                    let mut ranges = Vec::new();
                    while let Some(range) = claim_range(&next, total, workers) {
                        ranges.push(range);
                    }
                    ranges
                })
            })
            .collect();
        let mut ranges: Vec<_> = tasks
            .into_iter()
            .flat_map(|task| task.join().unwrap())
            .collect();
        ranges.sort_unstable();
        let mut position = 0;
        let mut large = false;
        for (start, end) in ranges {
            assert_eq!(start, position);
            assert!(end >= start && end < total);
            let len = end - start + 1;
            assert!(len <= MAX_HTTP_RANGE_SIZE);
            if total - start <= MAX_HTTP_RANGE_SIZE * workers as u64 {
                assert!(len <= RANGE_CHUNK_SIZE);
            }
            large |= len == MAX_HTTP_RANGE_SIZE;
            position = end + 1;
        }
        assert_eq!(position, total);
        assert!(large);
    }

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
