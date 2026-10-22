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
        bt::{BtConfig, BtService, parse_seed_time, parse_size},
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
    /// Find out why a download does not hold its top speed: measures the network alone, the disk
    /// alone and the engine on the same link, then compares them.
    Diagnose {
        url: String,
        /// Folder used for the temporary test files (default: the current one).
        #[arg(long, default_value = ".")]
        dir: PathBuf,
        /// Seconds per network test.
        #[arg(long, default_value_t = 10)]
        seconds: u64,
        /// Connection counts for the network tests.
        #[arg(long, value_delimiter = ',', default_value = "1,8,16,32")]
        connections: Vec<usize>,
        /// MiB written by the disk test.
        #[arg(long, default_value_t = 2048)]
        disk_mib: u64,
        #[arg(long)]
        skip_disk: bool,
        /// Seconds the engine runs before it is stopped.
        #[arg(long, default_value_t = 25)]
        engine_seconds: u64,
        /// Cap the engine at this many connections (default: its own choice, 16 for big files).
        #[arg(long)]
        engine_connections: Option<usize>,
        /// Let the engine download the whole file instead of stopping it.
        #[arg(long)]
        full: bool,
        /// Where the report is written.
        #[arg(long, default_value = "nibiru-diagnose-report.txt")]
        report: PathBuf,
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
    /// Local Service Discovery (BEP 14): find peers on the same network via multicast.
    #[arg(long)]
    enable_lpd: bool,
    /// Local address to join/send the LSD multicast group on (default: the OS default route).
    #[arg(long)]
    lpd_interface: Option<std::net::Ipv4Addr>,
    #[arg(long)]
    allow_loopback_peers: bool,
    #[arg(long)]
    stop_with_process: Option<u32>,
    /// Keep unfinished tasks in this file and restore them (paused, same GIDs) on start.
    /// Tasks whose request carries credentials are never written.
    #[arg(long)]
    session_file: Option<PathBuf>,
    /// Ask a UPnP router to forward the listening ports (BitTorrent, ED2K and Kad), renew them
    /// while running and remove them on exit.
    #[arg(long)]
    upnp: bool,
    /// Accept `ed2k://|file|…|/` links (sources: `x-nibiru-ed2k-source`, servers, Kad).
    #[arg(long)]
    ed2k: bool,
    /// ED2K server `host:port` to ask for sources (repeatable).
    #[arg(long = "ed2k-server")]
    ed2k_servers: Vec<String>,
    /// Join Kad2 for source lookup, seeded from this `nodes.dat`.
    #[arg(long)]
    ed2k_nodes_file: Option<PathBuf>,
    /// Where the ED2K identity and Kad routing table are kept (default: `<dir>/.nibiru-ed2k`).
    #[arg(long)]
    ed2k_state_dir: Option<PathBuf>,
    #[arg(long, default_value_t = 4662)]
    ed2k_tcp_port: u16,
    /// Protocol obfuscation for ED2K peers: off, prefer (default) or require.
    #[arg(long, default_value = "prefer")]
    ed2k_obfuscation: nibiru_xtreme::ed2k::obfuscation::Obfuscation,
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
        if self.enable_lpd {
            config.session.lsd = Some(nibiru_xtreme::bittorrent::lsd::LsdConfig {
                interface: self
                    .lpd_interface
                    .unwrap_or(std::net::Ipv4Addr::UNSPECIFIED),
                ..Default::default()
            });
        }
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
                        "session_persistence": true,
                        "bittorrent": true, "ed2k_library": true,
                        "ed2k_rpc": true, "ed2k_multi_source": true, "bt_lsd": true,
                        "bt_holepunch": true,
                        "ftp_resume": cfg!(feature = "ftp"),
                        "sftp_resume": cfg!(feature = "sftp"),
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
            // Ctrl+C pauses: the download stops at a block boundary and keeps what it has, so the
            // same command continues from there. A second Ctrl+C exits at once.
            let interrupt = cancelled.clone();
            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    interrupt.store(true, std::sync::atomic::Ordering::Release);
                    eprintln!(
                        "pausing… run the same command again to continue (Ctrl+C again to exit now)"
                    );
                    if tokio::signal::ctrl_c().await.is_ok() {
                        std::process::exit(130);
                    }
                }
            });
            let result = engine
                .download(
                    DownloadRequest {
                        url,
                        destination: out,
                    },
                    cancelled.clone(),
                    |p| {
                        eprintln!(
                            "downloaded={} total={} speed={}",
                            p.downloaded,
                            p.total.unwrap_or_default(),
                            p.speed_bytes_per_second
                        );
                    },
                )
                .await;
            if cancelled.load(std::sync::atomic::Ordering::Acquire) {
                eprintln!("paused");
                std::process::exit(130);
            }
            result?;
        }
        Some(Command::Diagnose {
            url,
            dir,
            seconds,
            connections,
            disk_mib,
            skip_disk,
            engine_seconds,
            engine_connections,
            full,
            report,
        }) => {
            nibiru_xtreme::diagnose::run(nibiru_xtreme::diagnose::Options {
                url,
                dir,
                seconds,
                connections,
                disk_mib,
                skip_disk,
                engine_seconds,
                engine_connections,
                full,
                report,
            })
            .await?;
        }
        Some(Command::Serve(args)) => {
            let config = args.config()?;
            std::fs::create_dir_all(&args.dir)?;
            let log = Arc::new(LogSink::new(args.log.as_deref())?);
            let engine = Engine::new(log.clone())?;
            let global_download_limit = config.session.download_limit;
            let service = BtService::start(config).await?;
            #[cfg(not(feature = "upnp"))]
            if args.upnp {
                return Err(anyhow!("this build has no UPnP support"));
            }
            #[cfg(feature = "upnp")]
            let mut upnp_ports = Vec::new();
            #[cfg(feature = "upnp")]
            if args.upnp {
                use nibiru_xtreme::upnp::{Mapping, Proto};
                let session = service.session();
                upnp_ports.push(Mapping {
                    proto: Proto::Tcp,
                    port: session.port(),
                });
                upnp_ports.push(Mapping {
                    proto: Proto::Udp,
                    port: session.port(),
                });
                if let Some(dht) = session.dht() {
                    upnp_ports.push(Mapping {
                        proto: Proto::Udp,
                        port: dht.port(),
                    });
                }
            }
            let mut server = RpcServer::new(engine, log.clone(), args.secret.clone())
                .with_default_dir(args.dir.clone())
                .with_global_download_limit(global_download_limit)
                .with_bittorrent(service);
            if args.ed2k {
                let ed2k = start_ed2k(&args, &log).await?;
                #[cfg(feature = "upnp")]
                if args.upnp {
                    use nibiru_xtreme::upnp::{Mapping, Proto};
                    upnp_ports.push(Mapping {
                        proto: Proto::Tcp,
                        port: ed2k.announced_port(),
                    });
                    if let Some(kad) = ed2k.kad_status()
                        && let Some(port) =
                            kad["udp_port"].as_u64().and_then(|p| u16::try_from(p).ok())
                    {
                        upnp_ports.push(Mapping {
                            proto: Proto::Udp,
                            port,
                        });
                    }
                }
                server = server.with_ed2k(ed2k);
            }
            if let Some(path) = args.session_file.clone() {
                server = server.with_session(path).await?;
            }
            #[cfg(feature = "upnp")]
            let upnp = args
                .upnp
                .then(|| nibiru_xtreme::upnp::Upnp::new("Nibiru Xtreme"));
            #[cfg(feature = "upnp")]
            if let Some(upnp) = upnp.clone() {
                // Looking for the router can take seconds: do it without delaying the RPC server.
                let log = log.clone();
                tokio::spawn(async move {
                    upnp.apply(upnp_ports).await;
                    log.info(
                        "upnp.status",
                        &serde_json::to_string(&upnp.status()).unwrap_or_default(),
                    );
                });
            }
            if let Some(pid) = args.stop_with_process {
                let watched = server.clone();
                #[cfg(feature = "upnp")]
                let watched_upnp = upnp.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        if !process_alive(pid).await {
                            // Save the session before the manager's exit takes us down too.
                            let _ = watched.flush_session().await;
                            #[cfg(feature = "upnp")]
                            if let Some(upnp) = watched_upnp {
                                let _ =
                                    tokio::time::timeout(Duration::from_secs(6), upnp.release())
                                        .await;
                            }
                            std::process::exit(0);
                        }
                    }
                });
            }
            let stopping = server.clone();
            #[cfg(feature = "upnp")]
            let stopping_upnp = upnp.clone();
            tokio::spawn(async move {
                wait_for_termination().await;
                // Leave the session file current before exiting.
                let _ = stopping.flush_session().await;
                #[cfg(feature = "upnp")]
                if let Some(upnp) = stopping_upnp {
                    let _ = tokio::time::timeout(Duration::from_secs(6), upnp.release()).await;
                }
                std::process::exit(0);
            });
            server.serve(args.port).await?;
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
    let (mut lpd_enabled, mut lpd_interface) = (false, None::<std::net::Ipv4Addr>);
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
                bt.seed_time = Some(parse_seed_time(value)?);
            }
            "seed-ratio" => {
                let ratio = value.parse::<f64>()?;
                anyhow::ensure!(ratio.is_finite() && ratio >= 0.0, "invalid seed ratio");
                bt.seed_ratio = Some(ratio);
            }
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
            "bt-enable-lpd" => lpd_enabled = value == "true",
            "bt-lpd-interface" => lpd_interface = value.parse().ok(),
            "x-nibiru-enable-tcp" => bt.enable_tcp = value == "true",
            "bt-force-encryption" | "bt-require-crypto" => bt.require_encryption |= value == "true",
            "dht-file-path" => bt.dht_file = Some(PathBuf::from(value)),
            // Not an aria2 option: lets tests and LAN setups connect to peers on this machine.
            "x-nibiru-allow-loopback-peers" => bt.allow_loopback_peers = value == "true",
            _ => {}
        }
    }
    if lpd_enabled {
        bt.session.lsd = Some(nibiru_xtreme::bittorrent::lsd::LsdConfig {
            interface: lpd_interface.unwrap_or(std::net::Ipv4Addr::UNSPECIFIED),
            ..Default::default()
        });
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
    let default_dir = bt.default_dir.clone();
    let global_download_limit = bt.session.download_limit;
    let service = BtService::start(bt).await?;
    log.info(
        "bittorrent.started",
        &format!("listen_port={}", service.listen_port()),
    );
    RpcServer::new(engine, log, secret)
        .with_default_dir(default_dir)
        .with_global_download_limit(global_download_limit)
        .with_bittorrent(service)
        .serve(port)
        .await
}

/// Builds the ED2K service: a persistent random user hash, configured servers and, when a
/// `nodes.dat` is given, a Kad2 session whose routing table survives restarts.
async fn start_ed2k(
    args: &ServeArgs,
    log: &Arc<LogSink>,
) -> Result<Arc<nibiru_xtreme::rpc::ed2k::Ed2kService>> {
    use nibiru_xtreme::{
        ed2k::metadata,
        rpc::ed2k::{Ed2kConfig, Ed2kService},
    };
    let state = args
        .ed2k_state_dir
        .clone()
        .unwrap_or_else(|| args.dir.join(".nibiru-ed2k"));
    std::fs::create_dir_all(&state)?;
    let identity_path = state.join("user-hash");
    let identity: [u8; 16] = match std::fs::read(&identity_path) {
        Ok(bytes) if bytes.len() == 16 && bytes != [0; 16] => bytes.try_into().unwrap(),
        _ => {
            let mut hash = [0u8; 16];
            getrandom::fill(&mut hash).map_err(|e| anyhow::anyhow!("no randomness: {e}"))?;
            hash[5] = 14;
            hash[14] = 111;
            std::fs::write(&identity_path, hash)?;
            hash
        }
    };
    let mut servers = Vec::new();
    for item in &args.ed2k_servers {
        let (host, port) = item
            .rsplit_once(':')
            .ok_or_else(|| anyhow::anyhow!("--ed2k-server expects host:port"))?;
        servers.push(metadata::Server {
            host: host.to_owned(),
            port: port.parse()?,
            name: String::new(),
            description: String::new(),
        });
    }
    let service = Ed2kService::new(Ed2kConfig {
        default_dir: args.dir.clone(),
        identity,
        tcp_port: args.ed2k_tcp_port,
        obfuscation: args.ed2k_obfuscation,
        servers,
        kad: None,
    });
    let port = service.start_listener().await?;
    log.info("ed2k.listening", &format!("tcp_port={port}"));
    if let Some(file) = &args.ed2k_nodes_file {
        let seeds = metadata::parse_nodes(&std::fs::read(file)?)?;
        service
            .start_kad(seeds, state.join("kad-routing.json"))
            .await?;
        log.info("ed2k.kad_started", "joined Kad");
    }
    Ok(service)
}

/// Returns when the process is asked to stop (Ctrl-C everywhere, SIGTERM on Unix).
#[cfg(unix)]
async fn wait_for_termination() {
    use tokio::signal::unix::{SignalKind, signal};
    match signal(SignalKind::terminate()) {
        Ok(mut terminate) => {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
        }
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_termination() {
    let _ = tokio::signal::ctrl_c().await;
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
        Ok(out) if out.status.success() => {
            tasklist_contains_pid(&String::from_utf8_lossy(&out.stdout), pid)
        }
        // A failed inspection must not be mistaken for a dead parent.
        Ok(_) => true,
        Err(_) => true,
    }
}

#[cfg(any(windows, test))]
fn tasklist_contains_pid(output: &str, pid: u32) -> bool {
    output.lines().any(|line| {
        // /FO CSV quotes each field: "image.exe","1234","Console",...
        // Ignore quoted delimiters inside the image name, then parse only the PID.
        line.trim()
            .strip_prefix('"')
            .and_then(|line| line.split_once("\",\""))
            .and_then(|(_, rest)| rest.split_once('"'))
            .and_then(|(field, _)| field.parse::<u32>().ok())
            == Some(pid)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tasklist_reads_the_quoted_pid_field() {
        let csv = "\"ADM.exe\",\"1234\",\"Console\",\"1\",\"12,345 K\"\r\n";
        assert!(tasklist_contains_pid(csv, 1234));
        assert!(!tasklist_contains_pid(csv, 123));
        assert!(!tasklist_contains_pid(csv, 345));
        assert!(!tasklist_contains_pid("INFO: No tasks are running.", 1234));
        assert!(!tasklist_contains_pid("", 1234));
    }

    #[test]
    fn seed_time_rejects_values_that_cannot_be_scheduled() {
        for value in ["NaN", "inf", "-inf", "-1", "1e300", "invalid"] {
            assert!(parse_seed_time(value).is_err(), "accepted {value}");
        }
        assert_eq!(parse_seed_time("0").unwrap(), Duration::ZERO);
        assert_eq!(parse_seed_time("0.5").unwrap(), Duration::from_secs(30));
        assert_eq!(
            parse_seed_time("525600").unwrap(),
            Duration::from_secs(31536000)
        );
    }
}
