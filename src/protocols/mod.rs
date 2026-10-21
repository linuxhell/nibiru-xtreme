//! Optional direct transports and their explicit trust options.
#[cfg(feature = "ftp")]
pub(crate) mod ftp;
#[cfg(feature = "http3")]
pub(crate) mod http3;
#[cfg(feature = "sftp")]
pub(crate) mod sftp;
#[cfg(any(feature = "ftp", feature = "sftp", feature = "http3"))]
mod streaming;
use std::path::PathBuf;
#[cfg(any(feature = "ftp", feature = "sftp", feature = "http3"))]
pub(crate) use streaming::{ResumeMismatch, StagedFile, Validator, operation};

#[derive(Clone, Default)]
pub struct TransportOptions {
    pub ca_certificate: Option<PathBuf>,
    /// A SHA256 SSH host-key fingerprint verified through a trusted channel.
    pub ssh_host_key: Option<String>,
    pub ssh_identity: Option<PathBuf>,
}

#[cfg(any(feature = "ftp", feature = "sftp"))]
pub(crate) fn decoded(value: &str) -> anyhow::Result<String> {
    use anyhow::Context;
    let value = percent_encoding::percent_decode_str(value)
        .decode_utf8()
        .context("URL component is not UTF-8")?
        .into_owned();
    if value.chars().any(|c| c.is_control()) {
        anyhow::bail!("control characters are not allowed in protocol URLs");
    }
    Ok(value)
}
