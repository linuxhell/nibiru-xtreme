//! Native ED2K/Kad building blocks. This module never launches aMule.
//! Implemented independently against the on-wire formats; see docs/protocols.md.
pub(crate) mod file;
pub mod ip_filter;
mod kad;
pub mod kad_crypto;
pub mod kad_session;
mod listener;
pub mod metadata;
pub mod obfuscation;
pub(crate) mod peer;
mod server;
mod share;
pub(crate) mod transfer;
mod upload;

pub use file::FileLink;
pub use kad::{BootstrapResult, bootstrap};
pub use listener::{CallbackClient, CallbackRegistry, IncomingListener, IncomingStatus};
pub use server::{FileSource, OfferedFile, SearchResult, ServerConnection};
pub use share::{CachedShare, RescanReport, ShareRegistry, SharedFileInfo, hash_shared_file};
pub use transfer::{
    TransferOptions, TransferProgress, TransferStage, download_from_sources, download_with_options,
};
pub use upload::{
    Admission, FileUploadStats, UploadClientInfo, UploadConfig, UploadManager, UploadSnapshot,
};
/// Parses a 32-digit hexadecimal ED2K user hash.
pub fn user_hash(value: &str) -> Result<[u8; 16]> {
    server::file_hash(value).map_err(|_| anyhow::anyhow!("invalid ED2K user hash"))
}
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
