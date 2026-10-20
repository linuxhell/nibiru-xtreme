//! Force each transport independently so successful TCP fallback cannot hide a
//! broken uTP or encrypted path. Every transfer is hash-checked and compared.

use nibiru_xtreme::bittorrent::{
    message::Handshake,
    metainfo::{Extras, Torrent, build_info},
    session::{Session, SessionConfig},
    storage::Storage,
    torrent::Settings,
};
use std::{net::SocketAddr, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn transfer(utp: bool, encrypted: bool) {
    transfer_at(utp, encrypted, "127.0.0.1").await;
}

async fn transfer_at(utp: bool, encrypted: bool, ip: &str) {
    let data: Vec<u8> = (0..600_000).map(|i| (i % 251) as u8).collect();
    let torrent = Torrent::from_info(
        &build_info("payload.bin", 65_536, &[(&["payload.bin"], &data)]),
        Extras::default(),
    )
    .unwrap();
    let seed_dir = tempfile::tempdir().unwrap();
    let storage = Storage::new(&torrent, seed_dir.path());
    storage.write(0, &data).unwrap();
    let config = || SessionConfig {
        bind: ip.parse().unwrap(),
        enable_utp: utp,
        ..Default::default()
    };
    let seeder = Session::start(config()).await.unwrap();
    let leecher = Session::start(config()).await.unwrap();
    let mut settings = Settings::new(seed_dir.path());
    settings.allow_loopback_peers = true;
    settings.require_encryption = encrypted;
    settings.seed_ratio = None;
    let seed = seeder.add_torrent(torrent.clone(), settings);
    seed.wait_complete(Duration::from_secs(10)).await.unwrap();

    let out = tempfile::tempdir().unwrap();
    let mut settings = Settings::new(out.path());
    settings.allow_loopback_peers = true;
    settings.require_encryption = encrypted;
    settings.enable_tcp = !utp;
    settings.enable_utp = utp;
    settings.peers = vec![SocketAddr::new(ip.parse().unwrap(), seeder.port())];
    let download = leecher.add_torrent(torrent.clone(), settings);
    download
        .wait_complete(Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(
        Storage::new(&torrent, out.path())
            .read(0, data.len() as u64)
            .unwrap(),
        data
    );
    download.shutdown(false);
    seed.shutdown(false);
}

#[tokio::test]
async fn encrypted_tcp_transfer() {
    transfer(false, true).await;
}

#[tokio::test]
async fn utp_only_transfer() {
    transfer(true, false).await;
}

#[tokio::test]
async fn encrypted_utp_only_transfer() {
    transfer(true, true).await;
}

#[tokio::test]
async fn encrypted_ipv6_tcp_transfer() {
    transfer_at(false, true, "::1").await;
}

#[tokio::test]
async fn encrypted_ipv6_utp_transfer() {
    transfer_at(true, true, "::1").await;
}

#[tokio::test]
async fn encrypted_only_torrent_rejects_a_plaintext_peer() {
    let dir = tempfile::tempdir().unwrap();
    let torrent = Torrent::from_info(
        &build_info("one.bin", 65_536, &[(&["one.bin"], &[7; 500])]),
        Extras::default(),
    )
    .unwrap();
    let session = Session::start(SessionConfig {
        bind: "127.0.0.1".parse().unwrap(),
        enable_utp: false,
        ..Default::default()
    })
    .await
    .unwrap();
    let mut settings = Settings::new(dir.path());
    settings.allow_loopback_peers = true;
    settings.require_encryption = true;
    let handle = session.add_torrent(torrent.clone(), settings);
    let mut peer = tokio::net::TcpStream::connect(("127.0.0.1", session.port()))
        .await
        .unwrap();
    peer.write_all(&Handshake::new(torrent.info_hash, [9; 20], false).encode())
        .await
        .unwrap();
    let mut buffer = [0; 68];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), peer.read(&mut buffer))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    handle.shutdown(false);
}
