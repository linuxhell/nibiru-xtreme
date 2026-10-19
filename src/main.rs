#![forbid(unsafe_code)]

use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand};
use nibiru_xtreme::{
    VERSION,
    engine::{DownloadRequest, Engine},
    logging::LogSink,
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
        }) => {
            let log = Arc::new(LogSink::new(log.as_deref())?);
            let engine = if http1_only {
                Engine::with_http1_only(log)?
            } else {
                Engine::new(log)?
            };
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
