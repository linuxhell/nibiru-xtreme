use super::{StagedFile, TransportOptions, operation};
use crate::{engine::Progress, logging::LogSink};
use anyhow::{Result, bail};
use futures_util::TryStreamExt;
use std::{path::Path, sync::atomic::AtomicBool, time::Duration};
use url::Url;

pub(crate) async fn download<F>(
    url: &Url,
    destination: &Path,
    options: &TransportOptions,
    cancel: &AtomicBool,
    log: &LogSink,
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
    log.info("transfer.started", "protocol=http3 version=HTTP/3");
    let mut staged = StagedFile::new(destination, response.content_length()).await?;
    let mut reader = tokio_util::io::StreamReader::new(
        response
            .bytes_stream()
            .map_err(|e| std::io::Error::other(e.without_url())),
    );
    staged
        .copy(&mut reader, cancel, log, "http3", on_progress)
        .await?;
    staged.commit(cancel, log, "http3", on_progress).await
}
