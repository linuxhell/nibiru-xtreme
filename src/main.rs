#![forbid(unsafe_code)]

use anyhow::{Result, anyhow};
use clap::{Args, Parser, Subcommand};
use nibiru_xtreme::{
    VERSION,
    engine::{DownloadRequest, Engine},
    logging::LogSink,
    protocols::TransportOptions,
    rpc::{
        RpcServer,
        bt::{BtConfig, BtService, parse_size},
    },
};
use std::{
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

#[derive(Parser)]
#[command(name = "nibiru-xtreme", version = VERSION)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Report the native manager contract without starting network services.
    Capabilities {
        #[arg(long)]
        json: bool,
    },
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
    Serve(ServeArgs),
}

#[derive(Args)]
struct ServeArgs {
    #[arg(long, default_value_t = 6800)]
    port: u16,
    #[arg(long, default_value = "")]
    secret: String,
    #[arg(long)]
    log: Option<PathBuf>,
    #[arg(long, default_value = ".")]
    dir: PathBuf,
    #[arg(long, default_value = "6881-6999")]
    listen_port: String,
    #[arg(long, default_value_t = 0.0)]
    seed_time_minutes: f64,
    #[arg(long, default_value_t = 0.0)]
    seed_ratio: f64,
    #[arg(long, default_value_t = 50)]
    bt_max_peers: usize,
    #[arg(long, default_value = "0")]
    max_overall_download_limit: String,
    #[arg(long, default_value = "0")]
    max_overall_upload_limit: String,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    enable_dht: bool,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    enable_dht6: bool,
    #[arg(long)]
    dht_file: Option<PathBuf>,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    enable_utp: bool,
    #[arg(long)]
    bt_require_crypto: bool,
    #[arg(long)]
    allow_loopback_peers: bool,
    #[arg(long)]
    stop_with_process: Option<u32>,
}

impl ServeArgs {
    fn config(&self) -> Result<BtConfig> {
        let (first, last) = self
            .listen_port
            .split_once('-')
            .unwrap_or((&self.listen_port, &self.listen_port));
        let first = first.parse::<u16>()?;
        let last = last.parse::<u16>()?;
        anyhow::ensure!(first > 0 && last >= first, "invalid listening port range");
        anyhow::ensure!(
            self.seed_time_minutes.is_finite()
                && self.seed_time_minutes >= 0.0
                && self.seed_time_minutes <= 525600.0,
            "invalid seed time"
        );
        anyhow::ensure!(
            self.seed_ratio.is_finite() && self.seed_ratio >= 0.0,
            "invalid seed ratio"
        );
        anyhow::ensure!(
            (1..=200).contains(&self.bt_max_peers),
            "peer count must be 1..200"
        );
        let mut config = BtConfig {
            default_dir: self.dir.clone(),
            port_range: Some((first, last)),
            seed_time: Some(Duration::from_secs_f64(self.seed_time_minutes * 60.0)),
            seed_ratio: Some(self.seed_ratio),
            max_peers: self.bt_max_peers,
            dht: self.enable_dht,
            dht6: self.enable_dht6,
            dht_file: self
                .dht_file
                .clone()
                .or_else(|| self.enable_dht.then(|| self.dir.join("dht.dat"))),
            allow_loopback_peers: self.allow_loopback_peers,
            require_encryption: self.bt_require_crypto,
            ..BtConfig::default()
        };
        config.session.enable_utp = self.enable_utp;
        config.session.download_limit = parse_size(&self.max_overall_download_limit)
            .ok_or_else(|| anyhow!("invalid download limit"))?;
        config.session.upload_limit = parse_size(&self.max_overall_upload_limit)
            .ok_or_else(|| anyhow!("invalid upload limit"))?;
        Ok(config)
    }
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
        Some(Command::Capabilities { json: _ }) => {
            println!(
                "{}",
                serde_json::json!({
                    "schema_version": 1, "engine": "nibiru-xtreme", "version": VERSION,
                    "features": {"native_rpc": true, "rpc_namespace": "nibiru",
                        "parent_lifetime": true, "http_resume_identity": true,
                        "bittorrent": true, "ed2k_library": true,
                        "ftp": cfg!(feature = "ftp"), "sftp": cfg!(feature = "sftp"),
                        "http3": cfg!(feature = "http3")}
                })
            );
        }
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
        Some(Command::Serve(args)) => {
            let config = args.config()?;
            std::fs::create_dir_all(&args.dir)?;
            if let Some(pid) = args.stop_with_process {
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        if !process_alive(pid).await {
                            std::process::exit(0);
                        }
                    }
                });
            }
            let log = Arc::new(LogSink::new(args.log.as_deref())?);
            let engine = Engine::new(log.clone())?;
            let service = BtService::start(config).await?;
            RpcServer::new(engine, log, args.secret)
                .with_bittorrent(service)
                .serve(args.port)
                .await?;
        }
        None => {
            println!("Nibiru Xtreme {VERSION}");
        }
    }
    Ok(())
}

/// Legacy third-party launcher compatibility; ADM uses the typed native `serve` command.
async fn run_aria2_compatible_rpc(args: &[String]) -> Result<()> {
    let mut port = 6800u16;
    let mut secret = String::new();
    let mut log_path = None;
    let mut stop_with = None;
    let mut bt = BtConfig::default();
    for arg in args {
        let Some((name, value)) = arg.strip_prefix("--").and_then(|a| a.split_once('=')) else {
            continue;
        };
        match name {
            "rpc-listen-port" => port = value.parse().map_err(|_| anyhow!("invalid rpc port"))?,
            "rpc-secret" => secret = value.to_owned(),
            "log" => log_path = Some(PathBuf::from(value)),
            "stop-with-process" => stop_with = value.parse::<u32>().ok(),
            "listen-port" => {
                let (first, last) = value.split_once('-').unwrap_or((value, value));
                if let (Ok(first), Ok(last)) = (first.trim().parse(), last.trim().parse()) {
                    bt.port_range = Some((first, last));
                }
            }
            // aria2 counts seed-time in minutes; zero disables seeding after completion.
            "seed-time" => {
                bt.seed_time = value
                    .parse::<f64>()
                    .ok()
                    .map(|m| Duration::from_secs_f64(m.max(0.0) * 60.0))
            }
            "seed-ratio" => bt.seed_ratio = value.parse::<f64>().ok(),
            "bt-max-peers" => {
                if let Ok(n) = value.parse::<usize>() {
                    bt.max_peers = if n == 0 { 200 } else { n.min(200) };
                }
            }
            "max-overall-download-limit" => {
                bt.session.download_limit = parse_size(value).unwrap_or(0);
            }
            "max-overall-upload-limit" => {
                bt.session.upload_limit = parse_size(value).unwrap_or(0);
            }
            "dir" => bt.default_dir = PathBuf::from(value),
            "enable-dht" => bt.dht = value == "true",
            "enable-dht6" => bt.dht6 = value == "true",
            "enable-utp" => bt.session.enable_utp = value == "true",
            "x-nibiru-enable-tcp" => bt.enable_tcp = value == "true",
            "bt-force-encryption" | "bt-require-crypto" => bt.require_encryption |= value == "true",
            "dht-file-path" => bt.dht_file = Some(PathBuf::from(value)),
            // Not an aria2 option: lets tests and LAN setups connect to peers on this machine.
            "x-nibiru-allow-loopback-peers" => bt.allow_loopback_peers = value == "true",
            _ => {}
        }
    }
    if bt.dht && bt.dht_file.is_none() {
        bt.dht_file = Some(PathBuf::from("dht.dat"));
    }
    if let Some(pid) = stop_with {
        tokio::spawn(async move {
            // The manager owns this process: exit when it goes away so nothing is left running.
            loop {
                tokio::time::sleep(Duration::from_secs(3)).await;
                if !process_alive(pid).await {
                    std::process::exit(0);
                }
            }
        });
    }
    let log = Arc::new(LogSink::new(log_path.as_deref())?);
    let engine = Engine::new(log.clone())?;
    let service = BtService::start(bt).await?;
    log.info(
        "bittorrent.started",
        &format!("listen_port={}", service.listen_port()),
    );
    RpcServer::new(engine, log, secret)
        .with_bittorrent(service)
        .serve(port)
        .await
}

#[cfg(unix)]
async fn process_alive(pid: u32) -> bool {
    tokio::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(true)
}

#[cfg(windows)]
async fn process_alive(pid: u32) -> bool {
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    let output = tokio::process::Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/NH", "/FO", "CSV"])
        .creation_flags(CREATE_NO_WINDOW)
        .output()
        .await;
    match output {
        Ok(out) => String::from_utf8_lossy(&out.stdout).contains(&format!("\",{pid},\"")),
        Err(_) => true,
    }
}
