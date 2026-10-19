#![forbid(unsafe_code)]

use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand};
use nibiru_xtreme::{
    VERSION,
    engine::{DownloadRequest, Engine},
    logging::LogSink,
    protocols::TransportOptions,
    rpc::RpcServer,
};
use std::{
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
};

#[derive(Parser)]
#[command(name = "nibiru-xtreme", version = VERSION)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    Download {
        url: String,
        #[arg(short, long)]
        out: PathBuf,
        #[arg(long)]
        log: Option<PathBuf>,
        #[arg(long)]
        http1_only: bool,
        /// Require HTTP/3 over QUIC; never silently falls back to TCP.
        #[arg(long, conflicts_with = "http1_only")]
        http3_only: bool,
        /// Experimental background sync interval in MiB; zero keeps final-only sync.
        #[arg(long, default_value_t = 0)]
        sync_interval_mib: u32,
        /// Experiment with bounded request concurrency during segmented downloads.
        #[arg(long)]
        adaptive: bool,
        /// Additional DER root certificate for FTPS.
        #[arg(long)]
        ca_certificate: Option<PathBuf>,
        /// Trusted SSH server fingerprint, e.g. SHA256:...
        #[arg(long)]
        ssh_host_key: Option<String>,
        /// Unencrypted OpenSSH private key for SFTP authentication.
        #[arg(long)]
        ssh_identity: Option<PathBuf>,
    },
    Serve {
        #[arg(long, default_value_t = 6800)]
        port: u16,
        #[arg(long, default_value = "")]
        secret: String,
        #[arg(long)]
        log: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args
        .iter()
        .any(|v| v == "--enable-rpc=true" || v == "--enable-rpc")
    {
        return run_aria2_compatible_rpc(&args).await;
    }

    let cli = Cli::parse();
    match cli.command {
        Some(Command::Download {
            url,
            out,
            log,
            http1_only,
            http3_only,
            sync_interval_mib,
            adaptive,
            ca_certificate,
            ssh_host_key,
            ssh_identity,
        }) => {
            let log = Arc::new(LogSink::new(log.as_deref())?);
            let engine = if http1_only {
                Engine::with_http1_only(log)?
            } else {
                Engine::new(log)?
            }
            .with_sync_interval_mib(sync_interval_mib)
            .with_adaptive(adaptive)
            .with_http3_only(http3_only)
            .with_transport_options(TransportOptions {
                ca_certificate,
                ssh_host_key,
                ssh_identity,
            });
            let cancelled = Arc::new(AtomicBool::new(false));
            engine
                .download(
                    DownloadRequest {
                        url,
                        destination: out,
                    },
                    cancelled,
                    |p| {
                        eprintln!(
                            "downloaded={} total={} speed={}",
                            p.downloaded,
                            p.total.unwrap_or_default(),
                            p.speed_bytes_per_second
                        );
                    },
                )
                .await?;
        }
        Some(Command::Serve { port, secret, log }) => {
            let log = Arc::new(LogSink::new(log.as_deref())?);
            let engine = Engine::new(log.clone())?;
            RpcServer::new(engine, log, secret).serve(port).await?;
        }
        None => {
            println!("Nibiru Xtreme {VERSION}");
        }
    }
    Ok(())
}

async fn run_aria2_compatible_rpc(args: &[String]) -> Result<()> {
    let mut port = 6800u16;
    let mut secret = String::new();
    let mut log_path = None;
    for arg in args {
        if let Some(v) = arg.strip_prefix("--rpc-listen-port=") {
            port = v.parse().map_err(|_| anyhow!("invalid rpc port"))?;
        } else if let Some(v) = arg.strip_prefix("--rpc-secret=") {
            secret = v.to_owned();
        } else if let Some(v) = arg.strip_prefix("--log=") {
            log_path = Some(PathBuf::from(v));
        }
    }
    let log = Arc::new(LogSink::new(log_path.as_deref())?);
    let engine = Engine::new(log.clone())?;
    RpcServer::new(engine, log, secret).serve(port).await
}
