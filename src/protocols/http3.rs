use super::{StagedFile, TransportOptions, operation};
use crate::engine::Progress;
use anyhow::{Result, bail};
use futures_util::TryStreamExt;
use std::{path::Path, sync::atomic::AtomicBool, time::Duration};
use url::Url;

pub(crate) async fn download<F>(
    url: &Url,
    destination: &Path,
    options: &TransportOptions,
    cancel: &AtomicBool,
    context: &super::TransferContext<'_>,
    on_progress: &mut F,
) -> Result<()>
where
    F: FnMut(Progress) + Send,
{
    if url.scheme() != "https" {
        bail!("HTTP/3 requires HTTPS");
    }
    let mut builder = reqwest::Client::builder()
        .https_only(true)
        .http3_prior_knowledge()
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::limited(3));
    // reqwest binds its QUIC socket to the IPv6 wildcard by default. A host without
    // IPv6 (disabled in the kernel or by policy) then fails every HTTP/3 request with
    // EAFNOSUPPORT even for IPv4 servers, so use the IPv4 wildcard there.
    if std::net::UdpSocket::bind((std::net::Ipv6Addr::UNSPECIFIED, 0)).is_err() {
        builder = builder.local_address(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
    }
    if let Some(path) = &options.ca_certificate {
        builder =
            builder.add_root_certificate(reqwest::Certificate::from_der(&std::fs::read(path)?)?);
    }
    let client = builder.build()?;
    let response = operation(cancel, async {
        Ok(client
            .get(url.clone())
            .version(reqwest::Version::HTTP_3)
            .send()
            .await
            .map_err(reqwest::Error::without_url)?
            .error_for_status()
            .map_err(reqwest::Error::without_url)?)
    })
    .await?;
    if response.version() != reqwest::Version::HTTP_3 {
        bail!("server did not negotiate HTTP/3");
    }
    context
        .log
        .info("transfer.started", "protocol=http3 version=HTTP/3");
    let mut staged = StagedFile::new(destination, response.content_length()).await?;
    let mut reader = tokio_util::io::StreamReader::new(
        response
            .bytes_stream()
            .map_err(|e| std::io::Error::other(e.without_url())),
    );
    staged.set_limiters(context.limiters);
    staged
        .copy(&mut reader, cancel, context.log, "http3", on_progress)
        .await?;
    staged
        .commit(cancel, context.log, "http3", on_progress)
        .await
}
