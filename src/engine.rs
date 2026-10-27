use anyhow::{Context, Result, anyhow, bail};
use futures_util::StreamExt;
use reqwest::{
    Client, StatusCode,
    header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, ETAG, IF_RANGE, LAST_MODIFIED, RANGE},
    redirect::Policy,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::OpenOptions as StdOpenOptions,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
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

use crate::{bittorrent::limiter::RateLimiter, logging::LogSink, storage};

mod adaptive;

/// Useful transport diagnostics without copying nested URLs, proxy credentials or headers.
pub(crate) fn download_error_message(error: &anyhow::Error) -> String {
    let causes = error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    let detail = if causes.contains("unknownissuer") {
        Some("TLS certificate validation failed (untrusted issuer)")
    } else if causes.contains("invalid peer certificate")
        || causes.contains("certificate verify failed")
    {
        Some("TLS certificate validation failed")
    } else if let Some(request) = error.downcast_ref::<reqwest::Error>() {
        if request.is_timeout() {
            Some("request timed out")
        } else if request.is_connect() {
            Some("connection failed")
        } else {
            None
        }
    } else {
        None
    };
    detail.map_or_else(|| error.to_string(), |detail| format!("{error}: {detail}"))
}

#[cfg(test)]
mod transport_diagnostic_tests {
    #[test]
    fn certificate_diagnostics_keep_the_cause_without_nested_secrets() {
        let error = anyhow::anyhow!("invalid peer certificate: UnknownIssuer https://user:secret@example.test/?token=private")
            .context("HTTP request failed");
        let message = super::download_error_message(&error);
        assert_eq!(
            message,
            "HTTP request failed: TLS certificate validation failed (untrusted issuer)"
        );
        assert!(!message.contains("secret"));
        assert!(!message.contains("private"));
    }
}

const MIN_SEGMENTED_SIZE: u64 = 8 * 1024 * 1024;
const RANGE_CHUNK_SIZE: u64 = 4 * 1024 * 1024;
const MAX_HTTP_RANGE_SIZE: u64 = 32 * 1024 * 1024;
const MAX_HTTP_WORKERS: usize = 16;
const HTTP2_CONNECTION_WINDOW: u32 = 16 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
struct HttpIdentity {
    origin: String,
    final_url: String,
    etag: Option<String>,
    modified: Option<String>,
    total: Option<u64>,
}

fn url_identity(url: &Url) -> String {
    let digest = Sha256::digest(url.as_str().as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

impl HttpIdentity {
    fn response(origin: &Url, response: &reqwest::Response) -> Self {
        let header = |key| {
            response
                .headers()
                .get(key)
                .and_then(|v| v.to_str().ok())
                .filter(|v| v.len() <= 4096)
                .map(str::to_owned)
        };
        Self {
            origin: url_identity(origin),
            final_url: url_identity(response.url()),
            etag: header(ETAG).filter(|v| !v.starts_with("W/")),
            modified: header(LAST_MODIFIED),
            total: response.content_length(),
        }
    }
    fn matches(&self, origin: &Url, response: &reqwest::Response) -> bool {
        let actual = Self::response(origin, response);
        self.final_url == actual.final_url
            && self.etag.as_ref().map_or_else(
                || self.modified.is_some() && self.modified == actual.modified,
                |_| self.etag == actual.etag,
            )
    }
}
/// The lock that keeps two downloads from writing the same destination. It lives only while the
/// download runs: the file is removed afterwards, so a finished or failed download leaves no
/// `.nibiru-http.lock` next to it. A lock held by another download is never removed.
struct DestinationLock {
    file: Option<std::fs::File>,
    path: PathBuf,
}

impl DestinationLock {
    fn acquire(destination: &Path) -> Result<Self> {
        let mut path = destination.as_os_str().to_owned();
        path.push(".nibiru-http.lock");
        let path = PathBuf::from(path);
        let file = StdOpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        file.try_lock()
            .context("download destination is already in use")?;
        Ok(Self {
            file: Some(file),
            path,
        })
    }
}

impl Drop for DestinationLock {
    fn drop(&mut self) {
        // Close the handle first (Windows cannot delete an open file), then remove the file.
        drop(self.file.take());
        let _ = std::fs::remove_file(&self.path);
    }
}

fn http_identity_path(destination: &Path) -> PathBuf {
    let mut path = destination.as_os_str().to_owned();
    path.push(".nibiru-http.json");
    PathBuf::from(path)
}

/// Hands out the next stretch of missing bytes. Blocks a previous run already wrote (`done`) are
/// skipped; a stretch never spans one, so every request covers only bytes still needed.
fn claim_range(
    next: &AtomicU64,
    total: u64,
    workers: usize,
    done: &[AtomicBool],
) -> Option<(u64, u64)> {
    let is_done = |offset: u64| {
        done.get((offset / RANGE_CHUNK_SIZE) as usize)
            .is_some_and(|block| block.load(Ordering::Acquire))
    };
    loop {
        let start = next.load(Ordering::Acquire);
        if start >= total {
            return None;
        }
        let mut first = start;
        while first < total && is_done(first) {
            first = (first / RANGE_CHUNK_SIZE + 1) * RANGE_CHUNK_SIZE;
        }
        if first != start {
            // Publish the skip so the other workers do not rescan the same blocks.
            let _ = next.compare_exchange_weak(
                start,
                first.min(total),
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            continue;
        }
        let remaining = total - start;
        let size = if remaining > MAX_HTTP_RANGE_SIZE * workers as u64 {
            MAX_HTTP_RANGE_SIZE
        } else {
            RANGE_CHUNK_SIZE
        };
        let mut end_exclusive = start + remaining.min(size);
        let mut probe = start + RANGE_CHUNK_SIZE;
        while probe < end_exclusive {
            if is_done(probe) {
                end_exclusive = probe;
                break;
            }
            probe += RANGE_CHUNK_SIZE;
        }
        if next
            .compare_exchange_weak(start, end_exclusive, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Some((start, end_exclusive - 1));
        }
    }
}

/// Where an unfinished segmented download keeps its bytes and the record of finished blocks.
fn partial_paths(destination: &Path) -> (PathBuf, PathBuf) {
    let mut part = destination.as_os_str().to_owned();
    part.push(".nibiru-http.part");
    let mut state = part.clone();
    state.push(".json");
    (PathBuf::from(part), PathBuf::from(state))
}

/// What a later run needs to trust the partial file: the resource it came from (the URL without
/// its query, so a refreshed signed link still matches) and the blocks that are on disk.
#[derive(Serialize, Deserialize)]
struct PartialState {
    version: u32,
    total: u64,
    block: u64,
    origin: String,
    etag: Option<String>,
    modified: Option<String>,
    done: String,
}

const PARTIAL_STATE_VERSION: u32 = 1;

fn resume_key(url: &Url) -> String {
    let mut stripped = url.clone();
    stripped.set_query(None);
    stripped.set_fragment(None);
    url_identity(&stripped)
}

fn encode_blocks(blocks: &[bool]) -> String {
    let mut bytes = vec![0_u8; blocks.len().div_ceil(8)];
    for (index, done) in blocks.iter().enumerate() {
        if *done {
            bytes[index / 8] |= 1 << (index % 8);
        }
    }
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn decode_blocks(text: &str, count: usize) -> Option<Vec<bool>> {
    if text.len() != count.div_ceil(8) * 2 || !text.is_ascii() {
        return None;
    }
    let mut blocks = Vec::with_capacity(count);
    for index in 0..count {
        let byte = u8::from_str_radix(&text[index / 8 * 2..index / 8 * 2 + 2], 16).ok()?;
        blocks.push(byte & (1 << (index % 8)) != 0);
    }
    Some(blocks)
}

fn block_len(index: usize, total: u64) -> u64 {
    total
        .saturating_sub(index as u64 * RANGE_CHUNK_SIZE)
        .min(RANGE_CHUNK_SIZE)
}

/// Writes the finished-block record. The blocks are noted first and flushed to disk before the
/// record is replaced, so a record never claims bytes that a crash could still lose.
async fn save_partial_state(
    file: Arc<std::fs::File>,
    state_path: PathBuf,
    mut state: PartialState,
    done: Arc<Vec<AtomicBool>>,
) -> Result<()> {
    let blocks: Vec<bool> = done
        .iter()
        .map(|block| block.load(Ordering::Acquire))
        .collect();
    tokio::task::spawn_blocking(move || file.sync_data())
        .await
        .map_err(|error| anyhow!("storage worker failed: {error}"))??;
    state.done = encode_blocks(&blocks);
    let mut temporary = state_path.as_os_str().to_owned();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);
    fs::write(&temporary, serde_json::to_vec(&state)?).await?;
    fs::rename(&temporary, &state_path).await?;
    Ok(())
}

type ResourceIdentity = (String, Option<String>, Option<String>);

/// The record for the current transfer, or `None` when the server gave nothing to validate a
/// later run against (such a download is not resumable).
fn partial_template(
    identity: &std::sync::Mutex<Option<ResourceIdentity>>,
    total: u64,
    origin: &str,
) -> Option<PartialState> {
    let (_, etag, modified) = identity.lock().ok()?.as_ref()?.clone();
    if etag.is_none() && modified.is_none() {
        return None;
    }
    Some(PartialState {
        version: PARTIAL_STATE_VERSION,
        total,
        block: RANGE_CHUNK_SIZE,
        origin: origin.to_owned(),
        etag,
        modified,
        done: String::new(),
    })
}

/// Leaves an interrupted download resumable, or removes the partial file when it cannot be.
async fn suspend_partial(
    file: Arc<std::fs::File>,
    part_path: &Path,
    state_path: &Path,
    state: Option<PartialState>,
    done: Arc<Vec<AtomicBool>>,
) {
    if let Some(state) = state
        && save_partial_state(file, state_path.to_path_buf(), state, done)
            .await
            .is_ok()
    {
        return;
    }
    let _ = fs::remove_file(part_path).await;
    let _ = fs::remove_file(state_path).await;
}

async fn write_range_block(
    file: Arc<std::fs::File>,
    offset: u64,
    buffer: Vec<u8>,
    completed: Arc<AtomicU64>,
    done: Arc<Vec<AtomicBool>>,
) -> Result<u64> {
    let len = buffer.len() as u64;
    let started = Instant::now();
    storage::write_at(file, offset, buffer).await?;
    completed.fetch_add(len, Ordering::AcqRel);
    // Writes start on block boundaries, so each one finishes exactly one block.
    if let Some(block) = done.get((offset / RANGE_CHUNK_SIZE) as usize) {
        block.store(true, Ordering::Release);
    }
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

/// Per-request HTTP settings (what a browser extension or the download manager passes along).
#[derive(Debug, Clone, Default)]
pub struct RequestOptions {
    /// Extra request headers: cookies, referer, user agent, authorization...
    pub headers: Vec<(String, String)>,
    /// `http://`, `https://` or `socks5://` proxy for every request.
    pub proxy: Option<ProxyOptions>,
    /// An upper bound requested by RPC split/max-connection-per-server.
    pub max_connections: Option<usize>,
    pub min_split_size: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ProxyOptions {
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DownloadRequest {
    pub url: String,
    pub destination: PathBuf,
}

#[derive(Clone)]
pub struct Engine {
    client: Client,
    http1_only: bool,
    request_options: RequestOptions,
    limiters: Vec<Arc<RateLimiter>>,
    log: Arc<LogSink>,
    sync_interval_bytes: u64,
    adaptive: bool,
    http3_only: bool,
    transport_options: crate::protocols::TransportOptions,
}

impl Engine {
    fn connection_count(&self, total: u64) -> usize {
        let mut workers = suggested_worker_count(total);
        if let Some(limit) = self.request_options.max_connections {
            workers = workers.min(limit.max(1));
        }
        if let Some(size) = self.request_options.min_split_size.filter(|s| *s > 0) {
            workers = workers.min((total / size).max(1).min(MAX_HTTP_WORKERS as u64) as usize);
        }
        workers
    }
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
        Ok(Self {
            client,
            http1_only,
            request_options: RequestOptions::default(),
            limiters: Vec::new(),
            log,
            sync_interval_bytes: 0,
            adaptive: false,
            http3_only: false,
            transport_options: Default::default(),
        })
    }

    /// Experimental checkpoints for segmented downloads. Zero keeps final-only sync.
    pub fn with_sync_interval_mib(mut self, interval_mib: u32) -> Self {
        self.sync_interval_bytes = u64::from(interval_mib) * 1024 * 1024;
        self
    }

    /// Opt-in, bounded concurrency experiments; the default scheduler is unchanged.
    pub fn with_adaptive(mut self, enabled: bool) -> Self {
        self.adaptive = enabled;
        self
    }

    pub fn with_http3_only(mut self, enabled: bool) -> Self {
        self.http3_only = enabled;
        self
    }

    /// Applies headers and a proxy to every HTTP request of this engine.
    pub fn with_request_options(mut self, options: RequestOptions) -> Result<Self> {
        let mut builder = Self::client_builder(self.http1_only);
        let mut headers = reqwest::header::HeaderMap::new();
        for (name, value) in &options.headers {
            let name = reqwest::header::HeaderName::from_bytes(name.trim().as_bytes())
                .with_context(|| format!("invalid header name {name:?}"))?;
            // Range and framing headers belong to the engine.
            if matches!(
                name.as_str(),
                "range" | "content-length" | "host" | "transfer-encoding"
            ) {
                continue;
            }
            let mut value = reqwest::header::HeaderValue::from_str(value.trim())
                .with_context(|| format!("invalid value for header {name}"))?;
            if matches!(name.as_str(), "cookie" | "authorization") {
                value.set_sensitive(true);
            }
            headers.append(name, value);
        }
        if let Some(agent) = headers.remove(reqwest::header::USER_AGENT) {
            builder = builder.user_agent(agent);
        }
        builder = builder.default_headers(headers);
        if let Some(proxy) = &options.proxy {
            let mut p = reqwest::Proxy::all(&proxy.url).context("invalid proxy URL")?;
            if let (Some(user), Some(password)) = (&proxy.username, &proxy.password) {
                p = p.basic_auth(user, password);
            }
            builder = builder.proxy(p);
        }
        self.client = builder.build().context("failed to build HTTP client")?;
        self.request_options = options;
        Ok(self)
    }

    /// Token buckets every received byte must pass (one per task, one global).
    pub fn with_limiters(mut self, limiters: Vec<Arc<RateLimiter>>) -> Self {
        self.limiters = limiters;
        self
    }

    async fn throttle(limiters: &[Arc<RateLimiter>], bytes: usize) {
        for limiter in limiters {
            limiter.acquire(bytes as u64).await;
        }
    }

    pub fn with_transport_options(mut self, options: crate::protocols::TransportOptions) -> Self {
        self.transport_options = options;
        self
    }

    /// A client configured like the engine's own, for the diagnostics.
    pub fn probe_client() -> Result<Client> {
        Self::client_builder(false)
            .pool_max_idle_per_host(64)
            .build()
            .context("failed to build HTTP client")
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
        mut request: DownloadRequest,
        cancelled: Arc<AtomicBool>,
        mut on_progress: F,
    ) -> Result<()>
    where
        F: FnMut(Progress) + Send,
    {
        // A WebDAV file is downloaded through its HTTP GET representation.
        // No collection traversal or write methods are implied by these aliases.
        if let Some(rest) = request.url.strip_prefix("webdav://") {
            request.url = format!("http://{rest}");
        } else if let Some(rest) = request.url.strip_prefix("webdavs://") {
            request.url = format!("https://{rest}");
        }
        let parsed = Url::parse(&request.url).context("invalid URL")?;
        if let Some(parent) = request.destination.parent() {
            fs::create_dir_all(parent).await?;
        }

        let _destination_lock = DestinationLock::acquire(&request.destination)?;

        #[cfg(any(feature = "ftp", feature = "sftp", feature = "http3"))]
        let context = crate::protocols::TransferContext {
            log: &self.log,
            limiters: &self.limiters,
        };
        if self.http3_only {
            #[cfg(not(feature = "http3"))]
            bail!("HTTP/3 is disabled in this build");
            #[cfg(feature = "http3")]
            return crate::protocols::http3::download(
                &parsed,
                &request.destination,
                &self.transport_options,
                &cancelled,
                &context,
                &mut on_progress,
            )
            .await;
        }
        match parsed.scheme() {
            "http" | "https" => {}
            #[cfg(feature = "ftp")]
            "ftp" | "ftps" | "ftpes" => {
                return crate::protocols::ftp::download(
                    &parsed,
                    &request.destination,
                    &self.transport_options,
                    &cancelled,
                    &context,
                    &mut on_progress,
                )
                .await;
            }
            #[cfg(feature = "sftp")]
            "sftp" => {
                return crate::protocols::sftp::download(
                    &parsed,
                    &request.destination,
                    &self.transport_options,
                    &cancelled,
                    &context,
                    &mut on_progress,
                )
                .await;
            }
            scheme => bail!("unsupported protocol: {scheme}"),
        }

        let existing = fs::metadata(&request.destination)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        if existing == 0
            && let Ok((Some(total), ranges)) = self.probe(&request.url).await
            && ranges
            && total >= MIN_SEGMENTED_SIZE
            && self.connection_count(total) > 1
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
                Err(error) if error.is::<storage::checkpoints::BackgroundSyncError>() => {
                    return Err(error);
                }
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

        let identity_path = http_identity_path(&destination);
        let saved = if existing > 0 {
            anyhow::ensure!(
                fs::metadata(&identity_path)
                    .await
                    .map(|m| m.len() <= 65536)
                    .unwrap_or(false),
                "existing file has no verified HTTP resume identity"
            );
            let mut saved: HttpIdentity = serde_json::from_slice(&fs::read(&identity_path).await?)
                .context("invalid HTTP resume identity")?;
            let expected_origin = url_identity(&parsed);
            if saved.origin == parsed.as_str() {
                // Migrate legacy sidecars that stored URLs in cleartext. Accept them only
                // when the original URL matches exactly, then immediately replace both
                // stored URLs with fingerprints.
                let legacy_final =
                    Url::parse(&saved.final_url).context("invalid legacy HTTP resume identity")?;
                saved.origin = expected_origin.clone();
                saved.final_url = url_identity(&legacy_final);
                let temp = identity_path.with_extension("tmp");
                fs::write(&temp, serde_json::to_vec(&saved)?).await?;
                fs::rename(&temp, &identity_path).await?;
            }
            anyhow::ensure!(
                saved.origin == expected_origin
                    && (saved.etag.is_some() || saved.modified.is_some()),
                "cannot safely resume without the same origin and validator"
            );
            Some(saved)
        } else {
            None
        };
        let mut builder = self.client.get(parsed.clone());
        if let Some(saved) = &saved {
            builder = builder.header(RANGE, format!("bytes={existing}-")).header(
                IF_RANGE,
                saved.etag.as_ref().or(saved.modified.as_ref()).unwrap(),
            );
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
        let response = builder
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .context("HTTP request failed")?;
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

        if status == StatusCode::RANGE_NOT_SATISFIABLE
            && let Some(saved) = &saved
        {
            let head = self
                .client
                .head(parsed.clone())
                .send()
                .await
                .map_err(reqwest::Error::without_url)?;
            if head.status().is_success()
                && saved.matches(&parsed, &head)
                && head
                    .headers()
                    .get(CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    == Some(existing)
                && saved.total == Some(existing)
            {
                let _ = fs::remove_file(&identity_path).await;
                on_progress(Progress {
                    downloaded: existing,
                    total: Some(existing),
                    speed_bytes_per_second: 0,
                });
                return Ok(());
            }
        }
        if !status.is_success() {
            bail!("HTTP error: {status}");
        }
        if let Some(saved) = &saved {
            anyhow::ensure!(
                status == StatusCode::PARTIAL_CONTENT && saved.matches(&parsed, &response),
                "HTTP source changed or ignored validated resume; existing file preserved"
            );
            let content_range = response
                .headers()
                .get(CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .context("resume response lacks Content-Range")?;
            let (bounds, total) = content_range
                .strip_prefix("bytes ")
                .and_then(|v| v.split_once('/'))
                .context("invalid resume Content-Range")?;
            let (start, end) = bounds.split_once('-').context("invalid resume bounds")?;
            let (start, end, total) = (
                start.parse::<u64>()?,
                end.parse::<u64>()?,
                total.parse::<u64>()?,
            );
            anyhow::ensure!(
                start == existing
                    && end.checked_add(1) == Some(total)
                    && saved.total.is_none_or(|n| n == total),
                "resume range/length mismatch"
            );
            anyhow::ensure!(
                response
                    .content_length()
                    .is_none_or(|n| existing.checked_add(n) == Some(total)),
                "resume body length mismatch"
            );
        } else {
            anyhow::ensure!(
                status != StatusCode::PARTIAL_CONTENT,
                "unexpected partial response without Range"
            );
            let identity = HttpIdentity::response(&parsed, &response);
            let temp = identity_path.with_extension("tmp");
            fs::write(&temp, serde_json::to_vec(&identity)?).await?;
            fs::rename(&temp, &identity_path).await?;
        }
        let resumed = existing > 0 && status == StatusCode::PARTIAL_CONTENT;
        let response_len = response.content_length();
        let total = response_len.map(|len| if resumed { existing + len } else { len });

        let mut file = if resumed {
            OpenOptions::new().append(true).open(&destination).await?
        } else {
            OpenOptions::new()
                .create_new(true)
                .write(true)
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
                    if cancelled.load(Ordering::Acquire){file.flush().await?;bail!("cancelled");}
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
            let chunk = chunk
                .map_err(reqwest::Error::without_url)
                .context("HTTP body read failed")?;
            Self::throttle(&self.limiters, chunk.len()).await;
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
        let _ = fs::remove_file(&identity_path).await;
        Ok(())
    }

    /// Reads the record left by an interrupted run and asks the server whether the resource is
    /// still the one it describes. Returns the finished blocks and the resource identity, or
    /// `None` (after deleting the stale files) when the partial cannot be trusted.
    async fn load_partial(
        &self,
        url: &Url,
        part_path: &Path,
        state_path: &Path,
        total: u64,
        block_count: usize,
        origin: &str,
    ) -> Option<(Vec<bool>, ResourceIdentity)> {
        let record = fs::read(state_path).await.ok();
        let part_len = fs::metadata(part_path).await.ok().map(|m| m.len());
        if record.is_none() && part_len.is_none() {
            return None;
        }
        let trusted = async {
            let record = record.filter(|bytes| bytes.len() <= 1 << 20)?;
            let state: PartialState = serde_json::from_slice(&record).ok()?;
            if state.version != PARTIAL_STATE_VERSION
                || state.total != total
                || state.block != RANGE_CHUNK_SIZE
                || state.origin != origin
                || part_len != Some(total)
            {
                return None;
            }
            let blocks = decode_blocks(&state.done, block_count)?;
            let validator = state.etag.clone().or_else(|| state.modified.clone())?;
            let response = self
                .client
                .get(url.clone())
                .header(RANGE, "bytes=0-0")
                .header(IF_RANGE, validator)
                .send()
                .await
                .ok()?;
            let value = |key| {
                response
                    .headers()
                    .get(key)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned)
            };
            let etag = value(ETAG).filter(|etag| !etag.starts_with("W/"));
            let modified = value(LAST_MODIFIED);
            let same = if state.etag.is_some() {
                etag == state.etag
            } else {
                modified.is_some() && modified == state.modified
            };
            if response.status() != StatusCode::PARTIAL_CONTENT
                || !same
                || validate_content_range(response.headers().get(CONTENT_RANGE), 0, 0, total)
                    .is_err()
            {
                return None;
            }
            Some((blocks, (response.url().to_string(), etag, modified)))
        }
        .await;
        match &trusted {
            Some((blocks, _)) => self.log.info(
                "http.resume_partial",
                &format!(
                    "blocks_done={} blocks_total={block_count}",
                    blocks.iter().filter(|done| **done).count()
                ),
            ),
            None => {
                self.log.warn(
                    "http.resume_discarded",
                    "reason=partial_not_verifiable_against_the_server",
                );
                let _ = fs::remove_file(part_path).await;
                let _ = fs::remove_file(state_path).await;
            }
        }
        trusted
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
        let workers = self.connection_count(total);
        let (part_path, state_path) = partial_paths(destination);
        let block_count = total.div_ceil(RANGE_CHUNK_SIZE) as usize;
        let origin = resume_key(&parsed);
        // A partial from an earlier run is reused only after the server confirms it is still the
        // same resource; anything else is discarded and the transfer starts over.
        let resumed = self
            .load_partial(
                &parsed,
                &part_path,
                &state_path,
                total,
                block_count,
                &origin,
            )
            .await;
        let done_blocks: Arc<Vec<AtomicBool>> = Arc::new(
            (0..block_count)
                .map(|index| AtomicBool::new(resumed.as_ref().is_some_and(|r| r.0[index])))
                .collect(),
        );
        let resumed_bytes: u64 = resumed
            .as_ref()
            .map(|r| {
                r.0.iter()
                    .enumerate()
                    .filter(|(_, done)| **done)
                    .map(|(index, _)| block_len(index, total))
                    .sum()
            })
            .unwrap_or(0);
        let identity_seed = resumed.map(|r| r.1);
        if identity_seed.is_none() {
            let _ = fs::remove_file(&state_path).await;
        }
        let file = StdOpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(identity_seed.is_none())
            .open(&part_path)?;
        if identity_seed.is_none() {
            file.set_len(total)?;
        }
        let file = Arc::new(file);
        let mut checkpoints = if self.sync_interval_bytes > 0 {
            Some(storage::checkpoints::BackgroundSync::start(
                file.clone(),
                self.sync_interval_bytes,
                self.log.clone(),
            )?)
        } else {
            None
        };

        let identity = Arc::new(std::sync::Mutex::new(identity_seed));
        let next_offset = Arc::new(AtomicU64::new(0));
        let completed = Arc::new(AtomicU64::new(resumed_bytes));
        let metrics = Arc::new(RangeMetrics::default());
        let mut set = JoinSet::new();
        let (adaptive_limit, _) = tokio::sync::watch::channel(workers);
        let active_ranges = Arc::new(AtomicU32::new(0));
        let mut controller = self.adaptive.then(|| adaptive::Controller::new(workers));
        if self.adaptive {
            self.log.info("http.adaptive_started", &format!(
                "maximum_workers={workers} initial_workers={workers} window_ms=4000 sync_interval_bytes={}",
                self.sync_interval_bytes,
            ));
        }

        self.log.info(
            "http.segmented_started",
            &format!(
                "url={} destination={} total={} workers={} chunk_size={} max_range_size={MAX_HTTP_RANGE_SIZE} sync_interval_bytes={} resumed_bytes={resumed_bytes}",
                redact_url(&parsed),
                destination.display(),
                total,
                workers,
                RANGE_CHUNK_SIZE,
                self.sync_interval_bytes,
            ),
        );

        for worker_id in 0..workers {
            let client = self.client.clone();
            let identity = identity.clone();
            let url = parsed.clone();
            let file = file.clone();
            let next_offset = next_offset.clone();
            let completed = completed.clone();
            let done_blocks = done_blocks.clone();
            let cancelled = cancelled.clone();
            let metrics = metrics.clone();
            let log = self.log.clone();
            let limiters = self.limiters.clone();
            let mut gate = self.adaptive.then(|| adaptive::Gate {
                limit: adaptive_limit.subscribe(),
                wake_all: adaptive_limit.clone(),
                maximum: workers,
                active: active_ranges.clone(),
                worker: worker_id,
            });
            set.spawn(async move {
                loop {
                    if cancelled.load(Ordering::Acquire) {
                        return Err(anyhow!("cancelled"));
                    }
                    if next_offset.load(Ordering::Acquire) >= total {
                        break;
                    }
                    let range_workers = if let Some(gate) = gate.as_mut() {
                        let Some(limit) = gate.enter().await else {
                            break;
                        };
                        limit
                    } else {
                        workers
                    };
                    let Some((start, end)) =
                        claim_range(&next_offset, total, range_workers, &done_blocks)
                    else {
                        break;
                    };
                    if end + 1 == total
                        && let Some(gate) = gate.as_ref()
                    {
                        gate.wake_all.send_replace(gate.maximum);
                    }
                    let _active_range = gate.as_ref().map(adaptive::Gate::track);

                    let requesting = Instant::now();
                    let mut request = client
                        .get(url.clone())
                        .header(RANGE, format!("bytes={start}-{end}"));
                    let validator = {
                        let guard = identity
                            .lock()
                            .map_err(|_| anyhow!("HTTP identity lock poisoned"))?;
                        guard
                            .as_ref()
                            .and_then(|(_, etag, modified)| etag.clone().or(modified.clone()))
                    };
                    if let Some(validator) = validator {
                        request = request.header(IF_RANGE, validator);
                    }
                    let response = request
                        .send()
                        .await
                        .map_err(reqwest::Error::without_url)
                        .with_context(|| format!("range worker {worker_id} request failed"))?;
                    let value = |key| {
                        response
                            .headers()
                            .get(key)
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_owned)
                    };
                    let etag = value(ETAG).filter(|etag| !etag.starts_with("W/"));
                    let actual = (response.url().to_string(), etag, value(LAST_MODIFIED));
                    {
                        let mut expected = identity
                            .lock()
                            .map_err(|_| anyhow!("HTTP identity lock poisoned"))?;
                        if let Some(expected) = expected.as_ref() {
                            anyhow::ensure!(
                                expected == &actual,
                                "HTTP resource identity changed between ranges"
                            );
                        } else {
                            *expected = Some(actual);
                        }
                    }
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
                            .map_err(reqwest::Error::without_url)
                            .with_context(|| format!("range worker {worker_id} body failed"))?;
                        if chunk.len() as u64 > expected.saturating_sub(received) {
                            bail!(
                                "range worker {worker_id} body exceeds expected length {expected}"
                            );
                        }
                        Self::throttle(&limiters, chunk.len()).await;
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
                                    completed.clone(),
                                    done_blocks.clone(),
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
                        storage_wait_us += write_range_block(
                            file.clone(),
                            start + written,
                            buffer,
                            completed.clone(),
                            done_blocks.clone(),
                        )
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

        let mut telemetry = TransferLog::new("segmented", started, resumed_bytes);
        let mut last_at = Instant::now();
        let mut last_bytes = resumed_bytes;
        let mut saver: Option<tokio::task::JoinHandle<Result<()>>> = None;
        let mut ticks = 0_u32;
        let mut ticker = time::interval(Duration::from_millis(250));
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        while !set.is_empty() {
            tokio::select! {
                _ = ticker.tick() => {
                    if cancelled.load(Ordering::Acquire) {
                        set.abort_all();
                        while set.join_next().await.is_some() {}
                        let _ = storage::checkpoints::finish(checkpoints.take()).await;
                        if let Some(saver) = saver.take() {
                            saver.abort();
                            let _ = saver.await;
                        }
                        suspend_partial(
                            file.clone(),
                            &part_path,
                            &state_path,
                            partial_template(&identity, total, &origin),
                            done_blocks.clone(),
                        )
                        .await;
                        drop(file);
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
                    if let Some(controller) = controller.as_mut() {
                        let unclaimed = total.saturating_sub(next_offset.load(Ordering::Acquire));
                        if let Some(decision) = controller.observe(
                            started.elapsed().as_millis() as u64, done,
                            active_ranges.load(Ordering::Acquire), unclaimed,
                        ) {
                            adaptive_limit.send_replace(decision.target);
                            self.log.info("http.adaptive_decision", &format!(
                                "previous_workers={} target_workers={} measured_bytes_per_second={} reference_bytes_per_second={} reason={}",
                                decision.previous, decision.target, decision.measured_bps,
                                decision.reference_bps, decision.reason,
                            ));
                        }
                        // All data is assigned: wake parked workers so they can
                        // observe the exhausted allocator and exit without a request.
                        if unclaimed == 0 { adaptive_limit.send_replace(workers); }
                    }
                    if let Some(worker) = checkpoints.as_mut() {
                        worker.consider(done, total);
                    }
                    ticks = ticks.wrapping_add(1);
                    if ticks.is_multiple_of(8)
                        && saver.as_ref().is_none_or(tokio::task::JoinHandle::is_finished)
                        && let Some(state) = partial_template(&identity, total, &origin)
                    {
                        saver = Some(tokio::spawn(save_partial_state(
                            file.clone(),
                            state_path.clone(),
                            state,
                            done_blocks.clone(),
                        )));
                    }
                    last_at = now;
                    last_bytes = done;
                }
                joined = set.join_next() => {
                    match joined {
                        Some(Ok(Ok(()))) => {}
                        Some(Ok(Err(error))) => {
                            set.abort_all();
                            while set.join_next().await.is_some() {}
                            let checkpoint_result = storage::checkpoints::finish(checkpoints.take()).await;
                            if let Some(saver) = saver.take() {
                                saver.abort();
                                let _ = saver.await;
                            }
                            // A resource that changed under us cannot be resumed; anything else
                            // (a dropped connection) keeps the finished blocks for the next run.
                            let template = (!error.to_string().contains("resource identity changed"))
                                .then(|| partial_template(&identity, total, &origin))
                                .flatten();
                            suspend_partial(file.clone(), &part_path, &state_path, template, done_blocks.clone()).await;
                            drop(file);
                            checkpoint_result?;
                            return Err(error);
                        }
                        Some(Err(error)) => {
                            set.abort_all();
                            while set.join_next().await.is_some() {}
                            let checkpoint_result = storage::checkpoints::finish(checkpoints.take()).await;
                            if let Some(saver) = saver.take() {
                                saver.abort();
                                let _ = saver.await;
                            }
                            suspend_partial(
                                file.clone(),
                                &part_path,
                                &state_path,
                                partial_template(&identity, total, &origin),
                                done_blocks.clone(),
                            )
                            .await;
                            drop(file);
                            checkpoint_result?;
                            return Err(anyhow!("range worker panicked: {error}"));
                        }
                        None => break,
                    }
                }
            }
        }

        if let Some(saver) = saver.take() {
            saver.abort();
            let _ = saver.await;
        }
        let done = completed.load(Ordering::Acquire);
        if done != total {
            let checkpoint_result = storage::checkpoints::finish(checkpoints.take()).await;
            suspend_partial(
                file.clone(),
                &part_path,
                &state_path,
                partial_template(&identity, total, &origin),
                done_blocks.clone(),
            )
            .await;
            drop(file);
            checkpoint_result?;
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
        let checkpoint_stats = match storage::checkpoints::finish(checkpoints.take()).await {
            Ok(stats) => stats,
            Err(error) => {
                suspend_partial(
                    file.clone(),
                    &part_path,
                    &state_path,
                    partial_template(&identity, total, &origin),
                    done_blocks.clone(),
                )
                .await;
                drop(file);
                return Err(error);
            }
        };
        let checkpoint_wait_ms = finalizing.elapsed().as_millis();
        let syncing = Instant::now();
        file.sync_all()?;
        let sync_ms = syncing.elapsed().as_millis();
        let closing = Instant::now();
        drop(file);
        let close_ms = closing.elapsed().as_millis();

        let renaming = Instant::now();
        let published = tempfile::TempPath::try_from_path(&part_path)
            .map_err(|error| (error, None))
            .and_then(|temporary| {
                temporary
                    .persist_noclobber(destination)
                    .map_err(|error| (error.error, Some(error.path)))
            });
        if let Err((error, kept)) = published {
            // A foreign destination must survive. Retain our completed payload too.
            if let Some(path) = kept {
                let _ = path.keep();
            }
            return Err(error)
                .context("cannot publish completed file without overwriting another file");
        }
        let _ = fs::remove_file(&state_path).await;
        let rename_ms = renaming.elapsed().as_millis();
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
                "destination={} bytes={} elapsed_ms={} mode=segmented workers={workers} transfer_ms={transfer_ms} finalize_ms={finalize_ms} sync_ms={sync_ms} close_ms={close_ms} destination_cleanup_ms=0 rename_ms={rename_ms} checkpoint_wait_ms={checkpoint_wait_ms} checkpoint_count={} checkpoint_sync_ms_sum={} adaptive={} adaptive_target_at_end={}",
                destination.display(),
                total,
                started.elapsed().as_millis(),
                checkpoint_stats.count,
                checkpoint_stats.elapsed_ms,
                self.adaptive,
                controller.as_ref().map_or(workers, adaptive::Controller::target),
            ),
        );
        Ok(())
    }

    pub async fn probe(&self, url: &str) -> Result<(Option<u64>, bool)> {
        let parsed = Url::parse(url)?;
        if !matches!(parsed.scheme(), "http" | "https") {
            bail!("unsupported protocol");
        }

        let head = self
            .client
            .head(parsed.clone())
            .send()
            .await
            .map_err(reqwest::Error::without_url)?;
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
            let mut probe = self
                .client
                .get(parsed)
                .header(RANGE, "bytes=0-0")
                .send()
                .await
                .map_err(reqwest::Error::without_url)?;
            if probe.status() == StatusCode::PARTIAL_CONTENT
                && validate_content_range(
                    probe.headers().get(CONTENT_RANGE),
                    0,
                    0,
                    total.unwrap_or_default(),
                )
                .is_ok()
            {
                // Finish the one-byte probe before opening payload connections.
                // A server can send headers before the probe body.
                if probe.content_length() == Some(1)
                    && probe
                        .chunk()
                        .await
                        .map_err(reqwest::Error::without_url)?
                        .is_some_and(|chunk| chunk.len() == 1)
                {
                    return Ok((total, true));
                }
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

fn redact_url(url: &Url) -> String {
    let mut copy = url.clone();
    let _ = copy.set_username("");
    let _ = copy.set_password(None);
    copy.set_query(None);
    copy.set_fragment(None);
    copy.to_string()
}

pub fn safe_output_name(value: &str) -> bool {
    crate::file_names::portable_component(value)
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_HTTP_RANGE_SIZE, RANGE_CHUNK_SIZE, claim_range, safe_output_name,
        suggested_worker_count, url_identity, validate_content_range,
    };
    use reqwest::header::HeaderValue;
    use std::sync::{Arc, atomic::AtomicU64};
    use url::Url;

    #[test]
    fn http_resume_identity_never_stores_url_secrets_in_cleartext() {
        let origin =
            Url::parse("https://user:pass@example.test/file?token=secret#fragment").unwrap();
        let fingerprint = url_identity(&origin);
        assert_eq!(fingerprint.len(), 64);
        assert!(!fingerprint.contains("secret"));
        assert!(!fingerprint.contains("user"));
        assert!(!fingerprint.contains("example.test"));
        assert_eq!(fingerprint, url_identity(&origin));
    }

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
                    while let Some(range) = claim_range(&next, total, workers, &[]) {
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
        assert!(safe_output_name("$ O'Brien's Movie.mkv"));
        for name in [
            "movie ",
            "movie.",
            "movie\u{a0}",
            "CON.mp4",
            "CON .mp4",
            "LPT¹.txt",
            "clip.mkv:stream",
            "bad\0name",
        ] {
            assert!(!safe_output_name(name), "{name:?}");
        }
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
