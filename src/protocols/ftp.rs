use super::{StagedFile, TransportOptions, decoded, operation};
use crate::{engine::Progress, logging::LogSink};
use anyhow::{Context, Result, bail};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use suppaftp::{
    Mode,
    tokio::{
        AsyncFtpStream, AsyncRustlsConnector, AsyncRustlsFtpStream, ImplAsyncFtpStream,
        TokioTlsStream,
    },
    types::FileType,
};
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
    if url.query().is_some() || url.fragment().is_some() {
        bail!("FTP URLs must not contain a query or fragment");
    }
    let hostname = match url.host().context("FTP host missing")? {
        url::Host::Ipv6(ip) => ip.to_string(),
        host => host.to_string(),
    };
    let host = hostname.as_str();
    let path = decoded(url.path())?;
    if path.is_empty() || path.ends_with('/') {
        bail!("FTP download requires a file path");
    }
    let user = if url.username().is_empty() {
        "anonymous".to_owned()
    } else {
        decoded(url.username())?
    };
    let password = decoded(url.password().unwrap_or("anonymous@"))?;
    let tls = url.scheme() != "ftp";
    let implicit = url.scheme() == "ftps";
    let port = url.port().unwrap_or(if implicit { 990 } else { 21 });
    log.info(
        "transfer.started",
        &format!("protocol={} host={host} port={port}", url.scheme()),
    );
    if tls {
        let connector = AsyncRustlsConnector::from(suppaftp::tokio_rustls::TlsConnector::from(
            tls_config(options.ca_certificate.as_deref())?,
        ));
        let mut ftp = operation(cancel, async {
            if implicit {
                Ok(
                    AsyncRustlsFtpStream::connect_secure_implicit((host, port), connector, host)
                        .await?,
                )
            } else {
                Ok(AsyncRustlsFtpStream::connect((host, port))
                    .await?
                    .into_secure(connector, host)
                    .await?)
            }
        })
        .await
        .context("FTPS connection or certificate validation failed")?;
        ftp.set_mode(if host.contains(':') {
            Mode::ExtendedPassive
        } else {
            Mode::Passive
        });
        transfer(
            &mut ftp,
            &user,
            &password,
            &path,
            destination,
            cancel,
            log,
            on_progress,
        )
        .await
    } else {
        let mut ftp = operation(cancel, async {
            Ok(AsyncFtpStream::connect((host, port)).await?)
        })
        .await
        .context("FTP connection failed")?;
        ftp.set_mode(if host.contains(':') {
            Mode::ExtendedPassive
        } else {
            Mode::Passive
        });
        transfer(
            &mut ftp,
            &user,
            &password,
            &path,
            destination,
            cancel,
            log,
            on_progress,
        )
        .await
    }
}

#[allow(clippy::too_many_arguments)]
async fn transfer<T, F>(
    ftp: &mut ImplAsyncFtpStream<T>,
    user: &str,
    password: &str,
    path: &str,
    destination: &Path,
    cancel: &AtomicBool,
    log: &LogSink,
    on_progress: &mut F,
) -> Result<()>
where
    T: TokioTlsStream + Send + Sync + 'static,
    F: FnMut(Progress) + Send,
{
    // Ignore server-supplied PASV addresses to prevent connections to unrelated hosts.
    ftp.set_passive_nat_workaround(true);
    operation(cancel, async {
        ftp.login(user, password)
            .await
            .context("FTP authentication failed")
    })
    .await?;
    operation(cancel, async {
        Ok(ftp.transfer_type(FileType::Binary).await?)
    })
    .await?;
    let total = operation(cancel, async {
        Ok(ftp.size(path).await.ok().map(|n| n as u64))
    })
    .await?;
    let mut staged = StagedFile::new(destination, total).await?;
    let mut stream = operation(cancel, async { Ok(ftp.retr_as_stream(path).await?) })
        .await
        .context("FTP RETR failed")?;
    staged
        .copy(&mut stream, cancel, log, "ftp", on_progress)
        .await?;
    // EOF on the data socket alone is not proof of successful FTP completion.
    operation(cancel, async { Ok(stream.finish().await?) })
        .await
        .context("FTP transfer completion rejected")?;
    staged.commit(cancel, log, "ftp", on_progress).await?;
    // QUIT is best-effort after data has been acknowledged and committed.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), ftp.quit()).await;
    Ok(())
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
