use super::{ResumeMismatch, StagedFile, TransportOptions, Validator, decoded, operation};
use crate::engine::Progress;
use anyhow::{Context, Result, bail};
use russh::{
    client,
    keys::{HashAlg, PrivateKeyWithHashAlg, PublicKeyOrCertificate},
};
use russh_sftp::client::SftpSession;
use std::{
    path::Path,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use tokio::io::AsyncSeekExt;
use url::Url;

struct VerifyHost {
    fingerprint: String,
}
impl client::Handler for VerifyHost {
    type Error = anyhow::Error;
    async fn check_server_key(&mut self, key: &PublicKeyOrCertificate) -> Result<bool> {
        // SSH certificates need separate CA/principal validation. Do not silently
        // treat their embedded public key as an ordinary pinned host key.
        match key {
            PublicKeyOrCertificate::PublicKey { key, .. } => {
                Ok(key.fingerprint(HashAlg::Sha256).to_string() == self.fingerprint)
            }
            _ => Ok(false),
        }
    }
}

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
    // A server that does not continue a saved partial invalidates it; try once from scratch.
    match download_once(url, destination, options, cancel, context, on_progress).await {
        Err(error) if error.downcast_ref::<ResumeMismatch>().is_some() => {
            context
                .log
                .warn("transfer.resume_rejected", "restarting from the beginning");
            download_once(url, destination, options, cancel, context, on_progress).await
        }
        other => other,
    }
}

async fn download_once<F>(
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
    if url.query().is_some() || url.fragment().is_some() {
        bail!("SFTP URLs must not contain a query or fragment");
    }
    let hostname = match url.host().context("SFTP host missing")? {
        url::Host::Ipv6(ip) => ip.to_string(),
        host => host.to_string(),
    };
    let host = hostname.as_str();
    let port = url.port().unwrap_or(22);
    let user = decoded(url.username())?;
    let path = decoded(url.path())?;
    if user.is_empty() || path.is_empty() || path.ends_with('/') {
        bail!("SFTP requires a username and file path");
    }
    let fingerprint = options
        .ssh_host_key
        .as_ref()
        .filter(|p| p.starts_with("SHA256:"))
        .context("SFTP requires --ssh-host-key SHA256:... from a trusted source")?;
    if options.ssh_identity.is_none() && url.password().is_none() {
        bail!("SFTP requires --ssh-identity or a password in the URL");
    }
    let config = client::Config {
        inactivity_timeout: Some(Duration::from_secs(60)),
        ..Default::default()
    };
    let mut ssh = operation(cancel, async {
        client::connect(
            Arc::new(config),
            (host, port),
            VerifyHost {
                fingerprint: fingerprint.clone(),
            },
        )
        .await
    })
    .await
    .context("SSH connection or host-key validation failed")?;
    let authenticated = if let Some(identity) = &options.ssh_identity {
        let key = russh::keys::load_secret_key(identity, None).context(
            "cannot load SSH identity (encrypted keys require an agent, currently unsupported)",
        )?;
        operation(cancel, async {
            let hash = ssh.best_supported_rsa_hash().await?.flatten();
            Ok(ssh
                .authenticate_publickey(&user, PrivateKeyWithHashAlg::new(Arc::new(key), hash))
                .await?
                .success())
        })
        .await?
    } else {
        let password = decoded(url.password().unwrap_or_default())?;
        operation(cancel, async {
            Ok(ssh.authenticate_password(&user, password).await?.success())
        })
        .await?
    };
    if !authenticated {
        bail!("SSH authentication rejected");
    }
    context.log.info(
        "transfer.started",
        &format!("protocol=sftp host={host} port={port} host_key_verified=true"),
    );
    let sftp = operation(cancel, async {
        let channel = ssh.channel_open_session().await?;
        channel.request_subsystem(true, "sftp").await?;
        Ok(SftpSession::new(channel.into_stream()).await?)
    })
    .await?;
    let mut remote = operation(cancel, async { Ok(sftp.open(path.clone()).await?) }).await?;
    let metadata = operation(cancel, async { Ok(remote.metadata().await?) }).await?;
    if metadata.is_dir() {
        bail!("SFTP directory downloads are not supported");
    }
    let validator = metadata.size.map(|size| Validator {
        key: format!("sftp://{host}:{port}{path}"),
        size,
        mtime: metadata.mtime.map(|t| t.to_string()),
    });
    let (mut staged, offset) = match &validator {
        Some(validator) => StagedFile::resumable(destination, validator).await?,
        None => (StagedFile::new(destination, None).await?, 0),
    };
    if offset > 0 {
        context.log.info(
            "transfer.resuming",
            &format!("protocol=sftp offset={offset}"),
        );
        operation(cancel, async {
            Ok(remote.seek(std::io::SeekFrom::Start(offset)).await?)
        })
        .await?;
    }
    staged.set_limiters(context.limiters);
    staged
        .copy(&mut remote, cancel, context.log, "sftp", on_progress)
        .await?;
    operation(cancel, async { Ok(remote.close().await?) }).await?;
    staged
        .commit(cancel, context.log, "sftp", on_progress)
        .await?;
    let _ = tokio::time::timeout(Duration::from_secs(2), sftp.close()).await;
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        ssh.disconnect(russh::Disconnect::ByApplication, "complete", ""),
    )
    .await;
    Ok(())
}
