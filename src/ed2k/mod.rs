//! Native ED2K/Kad building blocks. This module never launches aMule.
//! Implemented independently against the on-wire formats; see docs/protocols.md.
mod file;
mod kad;
pub mod metadata;
mod peer;
mod server;
mod transfer;

pub use file::FileLink;
pub use kad::{BootstrapResult, bootstrap};
pub use server::{FileSource, SearchResult, ServerConnection};
pub use transfer::{TransferProgress, TransferStage, download_from_sources};
pub const SERVER_LIST_URL: &str = "https://upd.emule-security.org/server.met";
pub const NODE_LIST_URL: &str = "https://upd.emule-security.org/nodes.dat";

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use std::time::Duration;

/// Small bounded metadata fetch, with mandatory TLS and HTTPS-only redirects.
pub async fn fetch_metadata(url: &str) -> Result<Vec<u8>> {
    let parsed = url::Url::parse(url).context("invalid metadata URL")?;
    if parsed.scheme() != "https" || !parsed.username().is_empty() || parsed.password().is_some() {
        bail!("metadata requires an HTTPS URL without credentials");
    }
    let client = reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::limited(3))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()?;
    let response = client
        .get(parsed)
        .send()
        .await
        .context("cannot fetch metadata")?
        .error_for_status()?;
    if response
        .content_length()
        .is_some_and(|n| n > metadata::MAX_METADATA as u64)
    {
        bail!("metadata exceeds size limit");
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if bytes.len() + chunk.len() > metadata::MAX_METADATA {
            bail!("metadata exceeds size limit");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
