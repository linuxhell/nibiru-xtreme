//! Local Service Discovery (BEP 14) over the real multicast group, on the loopback interface.
//! Interoperability with libtorrent is exercised by tests/interop_libtorrent.py.

use nibiru_xtreme::bittorrent::{
    lsd::{self, LsdConfig},
    magnet::Magnet,
    metainfo::{Extras, Torrent, build_info},
    session::{Session, SessionConfig},
    torrent::{Settings, State},
};
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::net::UdpSocket;

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
fn hex(hash: &[u8; 20]) -> String {
    hash.iter().map(|b| format!("{b:02x}")).collect()
}

fn torrent(seed: u64) -> (Torrent, Vec<u8>) {
    let data = sample(300_000, seed);
    let name = format!("lsd-{seed}.bin");
    let info = build_info(&name, 65_536, &[(&[name.as_str()], &data)]);
    (Torrent::from_info(&info, Extras::default()).unwrap(), data)
}
fn settings(dir: &std::path::Path) -> Settings {
    let mut settings = Settings::new(dir);
    settings.allow_loopback_peers = true;
    settings.seed_ratio = None;
    settings
}
fn config() -> LsdConfig {
    LsdConfig {
        interface: Ipv4Addr::LOCALHOST,
        announce_interval: Duration::from_secs(2),
        ..LsdConfig::default()
    }
}
async fn lsd_session() -> Arc<Session> {
    Session::start(SessionConfig {
        bind: "127.0.0.1".parse().unwrap(),
        lsd: Some(config()),
        dht: None,
        ..SessionConfig::default()
    })
    .await
    .unwrap()
}
async fn seeded(torrent: &Torrent, data: &[u8], session: &Session) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(&torrent.name), data).unwrap();
    let handle = session.add_torrent(torrent.clone(), settings(dir.path()));
    handle.wait_complete(Duration::from_secs(20)).await.unwrap();
    assert_eq!(handle.status().state, State::Seeding);
    dir
}
/// A raw member of the multicast group that records every announcement it sees.
async fn listener() -> UdpSocket {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).unwrap();
    socket.set_reuse_address(true).unwrap();
    #[cfg(unix)]
    socket.set_reuse_port(true).unwrap();
    socket
        .bind(&SocketAddr::from((Ipv4Addr::UNSPECIFIED, lsd::GROUP.port())).into())
        .unwrap();
    socket
        .join_multicast_v4(lsd::GROUP.ip(), &Ipv4Addr::LOCALHOST)
        .unwrap();
    socket.set_nonblocking(true).unwrap();
    UdpSocket::from_std(socket.into()).unwrap()
}
async fn seen_within(socket: &UdpSocket, seconds: u64) -> Vec<lsd::Announcement> {
    let mut found = Vec::new();
    let end = Instant::now() + Duration::from_secs(seconds);
    let mut buffer = [0u8; 2048];
    while let Some(left) = end.checked_duration_since(Instant::now()) {
        if let Ok(Ok((n, _))) = tokio::time::timeout(left, socket.recv_from(&mut buffer)).await
            && let Some(announcement) = lsd::parse(&buffer[..n])
        {
            found.push(announcement);
        }
    }
    found
}

#[tokio::test]
async fn a_magnet_with_no_peers_finds_its_neighbour_and_downloads() {
    let (torrent, data) = torrent(1);
    let seed = lsd_session().await;
    let _seed_dir = seeded(&torrent, &data, &seed).await;

    let out = tempfile::tempdir().unwrap();
    let leech = lsd_session().await;
    let magnet =
        Magnet::parse(&format!("magnet:?xt=urn:btih:{}", hex(&torrent.info_hash))).unwrap();
    assert!(magnet.peers.is_empty() && magnet.trackers.is_empty());
    let handle = leech.add_magnet(&magnet, settings(out.path()));
    handle.wait_metadata(Duration::from_secs(30)).await.unwrap();
    handle.wait_complete(Duration::from_secs(30)).await.unwrap();
    assert_eq!(std::fs::read(out.path().join(&torrent.name)).unwrap(), data);
}

#[tokio::test]
async fn without_lsd_the_same_magnet_finds_nobody() {
    let (torrent, data) = torrent(2);
    let seed = lsd_session().await;
    let _seed_dir = seeded(&torrent, &data, &seed).await;
    let out = tempfile::tempdir().unwrap();
    let plain = Session::start(SessionConfig {
        bind: "127.0.0.1".parse().unwrap(),
        lsd: None,
        ..SessionConfig::default()
    })
    .await
    .unwrap();
    let magnet =
        Magnet::parse(&format!("magnet:?xt=urn:btih:{}", hex(&torrent.info_hash))).unwrap();
    let handle = plain.add_magnet(&magnet, settings(out.path()));
    assert!(handle.wait_metadata(Duration::from_secs(6)).await.is_err());
}

#[tokio::test]
async fn public_torrents_are_announced_with_the_listen_port_and_private_ones_are_not() {
    let (public, public_data) = torrent(3);
    let (mut private, private_data) = torrent(4);
    private.private = true;
    let spy = listener().await;
    let session = lsd_session().await;
    let _a = seeded(&public, &public_data, &session).await;
    let _b = seeded(&private, &private_data, &session).await;
    let announcements = seen_within(&spy, 7).await;
    let public_seen = announcements
        .iter()
        .filter(|a| a.info_hashes.contains(&public.info_hash))
        .collect::<Vec<_>>();
    assert!(
        !public_seen.is_empty(),
        "no announcement for the public torrent"
    );
    assert!(public_seen.iter().all(|a| a.port == session.port()));
    assert!(
        announcements
            .iter()
            .all(|a| !a.info_hashes.contains(&private.info_hash)),
        "a private torrent leaked onto the local network"
    );
    // At most one announce per torrent per interval (2 s here) in a 7 s window.
    assert!(
        public_seen.len() <= 5,
        "{} announcements",
        public_seen.len()
    );
}

async fn announce_as_neighbour(hash: [u8; 20], port: u16) {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )
    .unwrap();
    socket.set_multicast_if_v4(&Ipv4Addr::LOCALHOST).unwrap();
    socket.set_multicast_loop_v4(true).unwrap();
    let packet = lsd::build(lsd::GROUP, port, &[hash], "someone-else");
    socket
        .send_to(&packet, &SocketAddr::from(lsd::GROUP).into())
        .unwrap();
}

#[tokio::test]
async fn announcements_are_only_acted_on_for_public_torrents_we_have() {
    let (mut private, _) = torrent(6);
    private.private = true;
    let (public, _) = torrent(7);
    let session = lsd_session().await;
    // Both are incomplete (nothing on disk), so both would dial a neighbour they accepted.
    let out_private = tempfile::tempdir().unwrap();
    let out_public = tempfile::tempdir().unwrap();
    let _private = session.add_torrent(private.clone(), settings(out_private.path()));
    let _public = session.add_torrent(public.clone(), settings(out_public.path()));

    // Neighbour that would be dialled if the announcement were accepted.
    let for_private = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let for_unknown = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let for_public_leech = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    announce_as_neighbour(private.info_hash, for_private.local_addr().unwrap().port()).await;
    announce_as_neighbour([9; 20], for_unknown.local_addr().unwrap().port()).await;
    announce_as_neighbour(
        public.info_hash,
        for_public_leech.local_addr().unwrap().port(),
    )
    .await;

    // The leeching public torrent dials the announced neighbour...
    tokio::time::timeout(Duration::from_secs(10), for_public_leech.accept())
        .await
        .expect("a public torrent should connect to an LSD neighbour")
        .unwrap();
    // ...the private one (already complete, but the rule is about acceptance) and the unknown hash do not.
    for refused in [&for_private, &for_unknown] {
        assert!(
            tokio::time::timeout(Duration::from_secs(2), refused.accept())
                .await
                .is_err(),
            "connected to a neighbour that should have been ignored"
        );
    }
}
