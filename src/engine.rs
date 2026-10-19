use anyhow::{Context, Result, anyhow, bail};
use futures_util::StreamExt;
use reqwest::{
    Client, StatusCode,
    header::{ACCEPT_RANGES, CONTENT_LENGTH, RANGE},
    redirect::Policy,
};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};
use url::Url;

use crate::logging::LogSink;

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

        let mut builder = self.client.get(parsed.clone());
        if existing > 0 {
            builder = builder.header(RANGE, format!("bytes={existing}-"));
        }

        self.log.info(
            "http.request_started",
            &format!(
                "url={} destination={} resume_from={existing}",
                redact_url(&parsed),
                request.destination.display()
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
                .open(&request.destination)
                .await?
        } else {
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&request.destination)
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
                        "destination={} downloaded={downloaded}",
                        request.destination.display()
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
                "destination={} bytes={} elapsed_ms={} resumed={} accept_ranges={}",
                request.destination.display(),
                downloaded,
                started.elapsed().as_millis(),
                resumed,
                if resumed { "bytes" } else { "unknown" }
            ),
        );
        Ok(())
    }

    pub async fn probe(&self, url: &str) -> Result<(Option<u64>, bool)> {
        let parsed = Url::parse(url)?;
        if !matches!(parsed.scheme(), "http" | "https") {
            bail!("unsupported protocol");
        }
        let response = self.client.head(parsed).send().await?;
        let total = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        let ranges = response
            .headers()
            .get(ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case("bytes"));
        Ok((total, ranges))
    }
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
    use super::safe_output_name;

    #[test]
    fn rejects_traversal_names() {
        assert!(safe_output_name("file.iso"));
        assert!(!safe_output_name("../file.iso"));
        assert!(!safe_output_name("dir/file.iso"));
        assert!(!safe_output_name("dir\\file.iso"));
    }
}
