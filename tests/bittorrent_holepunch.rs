//! BEP 55 hole punching between Nibiru sessions on one machine.
//!
//! `emulate_restricted_nat` makes the target drop inbound peers it did not dial recently, like a
//! restricted NAT. The leecher can reach the target only if a relay introduces them and both
//! dial at once.

use nibiru_xtreme::bittorrent::{
    metainfo::{Extras, Torrent, build_info},
    session::{Session, SessionConfig},
    torrent::{Settings, State},
};
use std::{net::SocketAddr, sync::Arc, time::Duration};

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
fn torrent(seed: u64) -> (Torrent, Vec<u8>) {
    let data = sample(400_000, seed);
    let name = format!("punch-{seed}.bin");
    let info = build_info(&name, 65_536, &[(&[name.as_str()], &data)]);
    (Torrent::from_info(&info, Extras::default()).unwrap(), data)
}
fn settings(dir: &std::path::Path) -> Settings {
    let mut settings = Settings::new(dir);
    // Over TCP a peer's address is an ephemeral port nobody can dial back, so BEP 55 is uTP-only.
    settings.enable_tcp = false;
    settings.allow_loopback_peers = true;
    settings.seed_ratio = None;
    settings
}
async fn session(nat: bool) -> Arc<Session> {
    Session::start(SessionConfig {
        bind: "127.0.0.1".parse().unwrap(),
        emulate_restricted_nat: nat,
        dht: None,
        ..SessionConfig::default()
    })
    .await
    .unwrap()
}
fn addr(session: &Session) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], session.port()))
}
async fn until(label: &str, seconds: u64, mut check: impl FnMut() -> bool) {
    for _ in 0..seconds * 20 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out: {label}");
}

/// B (seed, behind the emulated NAT) and R (relay) meet; then A joins through R only.
async fn swarm(
    torrent: &Torrent,
    data: &[u8],
    a_utp: bool,
) -> (
    nibiru_xtreme::bittorrent::torrent::TorrentHandle,
    Vec<Arc<Session>>,
    Vec<tempfile::TempDir>,
) {
    let (relay, seed, leech) = (
        session(false).await,
        session(true).await,
        session(false).await,
    );
    let (relay_dir, seed_dir, leech_dir) = (
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    );
    std::fs::write(seed_dir.path().join(&torrent.name), data).unwrap();

    // The relay wants no data, so it can never become a second source for the leecher.
    let mut relay_settings = settings(relay_dir.path());
    relay_settings.selected = Some(Vec::new());
    let relay_handle = relay.add_torrent(torrent.clone(), relay_settings);

    let mut seed_settings = settings(seed_dir.path());
    seed_settings.peers.push(addr(&relay));
    let seed_handle = seed.add_torrent(torrent.clone(), seed_settings);
    seed_handle
        .wait_complete(Duration::from_secs(20))
        .await
        .unwrap();
    until("relay sees the seed", 20, || {
        relay_handle.status().connections >= 1
    })
    .await;

    let mut leech_settings = settings(leech_dir.path());
    // A leecher that ignores the relay's hole-punch hints (it never learns B can be punched).
    leech_settings.enable_pex = a_utp;
    leech_settings.peers.push(addr(&relay));
    let leech_handle = leech.add_torrent(torrent.clone(), leech_settings);
    (
        leech_handle,
        vec![relay, seed, leech],
        vec![relay_dir, seed_dir, leech_dir],
    )
}

#[tokio::test]
async fn a_peer_behind_a_restricted_nat_is_reached_through_a_relay() {
    let (torrent, data) = torrent(1);
    let (leech, sessions, dirs) = swarm(&torrent, &data, true).await;
    leech.wait_complete(Duration::from_secs(60)).await.unwrap();
    assert_eq!(leech.status().state, State::Seeding);
    assert_eq!(
        std::fs::read(dirs[2].path().join(&torrent.name)).unwrap(),
        data
    );
    // The bytes really came from the seed that nobody could dial directly.
    assert!(sessions[1].list()[0].status().uploaded >= data.len() as u64);
}

#[tokio::test]
async fn without_utp_hints_there_is_no_hole_punching_and_the_nat_wins() {
    let (torrent, data) = torrent(2);
    let (leech, _sessions, _dirs) = swarm(&torrent, &data, false).await;
    assert!(leech.wait_complete(Duration::from_secs(12)).await.is_err());
    assert_ne!(leech.status().state, State::Seeding);
}
