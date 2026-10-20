//! Interoperability with a real BitTorrent client (aria2c). Skipped when it is not installed.

use nibiru_xtreme::bittorrent::{
    magnet::Magnet,
    metainfo::{Extras, Torrent, build_info},
    session::{Session, SessionConfig},
    torrent::Settings,
};
use std::{
    net::SocketAddr,
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};

fn aria2c() -> Option<()> {
    Command::new("aria2c")
        .arg("--version")
        .stdout(Stdio::null())
        .status()
        .ok()
        .filter(|s| s.success())
        .map(|_| ())
}

fn sample(len: usize, seed: u64) -> Vec<u8> {
    let mut x = 0x9e3779b97f4a7c15u64 ^ seed;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Killer(Child);
impl Drop for Killer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn torrent(files: &[(Vec<&str>, Vec<u8>)]) -> Torrent {
    let spec: Vec<(&[&str], &[u8])> = files
        .iter()
        .map(|(p, d)| (p.as_slice(), d.as_slice()))
        .collect();
    let info = build_info("interop", 131_072, &spec);
    Torrent::from_info(&info, Extras::default()).unwrap()
}

fn write_files(dir: &Path, files: &[(Vec<&str>, Vec<u8>)]) {
    for (path, data) in files {
        let full = path.iter().fold(dir.join("interop"), |p, c| p.join(c));
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(full, data).unwrap();
    }
}

fn aria2(dir: &Path, torrent_file: &Path, port: u16, extra: &[&str]) -> Killer {
    Killer(
        Command::new("aria2c")
            .args([
                "--enable-dht=false",
                "--enable-dht6=false",
                "--bt-enable-lpd=false",
                "--enable-peer-exchange=false",
                "--bt-require-crypto=false",
                "--seed-ratio=0.0",
                "--allow-overwrite=true",
                "--file-allocation=none",
                "--console-log-level=warn",
                "--bt-stop-timeout=0",
                &format!("--listen-port={port}"),
                &format!("--dir={}", dir.display()),
            ])
            .args(extra)
            .arg(torrent_file)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}

fn files() -> Vec<(Vec<&'static str>, Vec<u8>)> {
    vec![
        (vec!["movie.mkv"], sample(1_500_000, 7)),
        (vec!["docs", "readme.txt"], sample(12_345, 8)),
    ]
}

fn local() -> Settings {
    let mut s = Settings::new(".");
    s.allow_loopback_peers = true;
    s.seed_ratio = None;
    s
}

async fn ours() -> std::sync::Arc<Session> {
    Session::start(SessionConfig {
        bind: "127.0.0.1".parse().unwrap(),
        allow_private_trackers: true,
        ..SessionConfig::default()
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn we_download_from_aria2c() {
    if aria2c().is_none() {
        eprintln!("aria2c not installed; skipping");
        return;
    }
    let files = files();
    let torrent = torrent(&files);
    let seed_dir = tempfile::tempdir().unwrap();
    write_files(seed_dir.path(), &files);
    let meta = tempfile::tempdir().unwrap();
    let torrent_file = meta.path().join("t.torrent");
    std::fs::write(&torrent_file, torrent.to_torrent_file()).unwrap();
    let port = free_port();
    let _seeder = aria2(
        seed_dir.path(),
        &torrent_file,
        port,
        &["--check-integrity=true", "--bt-seed-unverified=true"],
    );
    tokio::time::sleep(Duration::from_secs(2)).await;

    let out = tempfile::tempdir().unwrap();
    let session = ours().await;
    let mut settings = local();
    settings.directory = out.path().to_owned();
    settings
        .peers
        .push(SocketAddr::from(([127, 0, 0, 1], port)));
    let handle = session.add_torrent(torrent, settings);
    handle.wait_complete(Duration::from_secs(60)).await.unwrap();
    for (path, data) in &files {
        let full = path
            .iter()
            .fold(out.path().join("interop"), |p, c| p.join(c));
        assert_eq!(std::fs::read(full).unwrap(), *data);
    }
}

#[tokio::test]
async fn we_fetch_metadata_from_aria2c_via_a_magnet_link() {
    if aria2c().is_none() {
        return;
    }
    let files = files();
    let torrent = torrent(&files);
    let seed_dir = tempfile::tempdir().unwrap();
    write_files(seed_dir.path(), &files);
    let meta = tempfile::tempdir().unwrap();
    let torrent_file = meta.path().join("t.torrent");
    std::fs::write(&torrent_file, torrent.to_torrent_file()).unwrap();
    let port = free_port();
    let _seeder = aria2(
        seed_dir.path(),
        &torrent_file,
        port,
        &["--check-integrity=true", "--bt-seed-unverified=true"],
    );
    tokio::time::sleep(Duration::from_secs(2)).await;

    let out = tempfile::tempdir().unwrap();
    let session = ours().await;
    let hash: String = torrent
        .info_hash
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let magnet =
        Magnet::parse(&format!("magnet:?xt=urn:btih:{hash}&x.pe=127.0.0.1:{port}")).unwrap();
    let mut settings = local();
    settings.directory = out.path().to_owned();
    let handle = session.add_magnet(&magnet, settings);
    let fetched = handle.wait_metadata(Duration::from_secs(30)).await.unwrap();
    assert_eq!(
        fetched.info_bytes, torrent.info_bytes,
        "BEP 9 metadata from a real client"
    );
    handle.wait_complete(Duration::from_secs(60)).await.unwrap();
    assert_eq!(
        std::fs::read(out.path().join("interop/movie.mkv")).unwrap(),
        files[0].1
    );
}

#[tokio::test]
async fn aria2c_downloads_from_us() {
    if aria2c().is_none() {
        return;
    }
    let files = files();
    let torrent = torrent(&files);
    let seed_dir = tempfile::tempdir().unwrap();
    write_files(seed_dir.path(), &files);
    let session = ours().await;
    let mut settings = local();
    settings.directory = seed_dir.path().to_owned();
    let seeder = session.add_torrent(torrent.clone(), settings);
    seeder.wait_complete(Duration::from_secs(20)).await.unwrap();

    let meta = tempfile::tempdir().unwrap();
    let torrent_file = meta.path().join("t.torrent");
    std::fs::write(&torrent_file, torrent.to_torrent_file()).unwrap();
    let out = tempfile::tempdir().unwrap();
    let port = free_port();
    let mut child = aria2(out.path(), &torrent_file, port, &["--seed-time=0"]);
    tokio::time::sleep(Duration::from_secs(1)).await;
    seeder.add_peer(SocketAddr::from(([127, 0, 0, 1], port)));
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success(), "aria2c failed: {status}");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "aria2c did not finish"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    for (path, data) in &files {
        let full = path
            .iter()
            .fold(out.path().join("interop"), |p, c| p.join(c));
        assert_eq!(std::fs::read(full).unwrap(), *data);
    }
    assert!(seeder.status().uploaded >= 1_500_000);
}
