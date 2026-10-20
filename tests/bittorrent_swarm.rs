//! Real-socket BitTorrent tests: sessions talk over loopback TCP.

use nibiru_xtreme::bittorrent::{
    magnet::Magnet,
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

struct Fixture {
    torrent: Torrent,
    files: Vec<(Vec<String>, Vec<u8>)>,
}

fn fixture() -> Fixture {
    // Awkward sizes: pieces straddle files and the last piece is short.
    let files = vec![
        (vec!["video.mp4".to_owned()], sample(700_000, 1)),
        (
            vec!["extra".to_owned(), "notes.txt".to_owned()],
            sample(90_001, 2),
        ),
        (vec!["tail.bin".to_owned()], sample(333_333, 3)),
    ];
    let spec: Vec<(Vec<&str>, &[u8])> = files
        .iter()
        .map(|(p, d)| (p.iter().map(String::as_str).collect(), d.as_slice()))
        .collect();
    let spec_ref: Vec<(&[&str], &[u8])> = spec.iter().map(|(p, d)| (p.as_slice(), *d)).collect();
    let info = build_info("pack", 65_536, &spec_ref);
    Fixture {
        torrent: Torrent::from_info(&info, Extras::default()).unwrap(),
        files,
    }
}

fn seed_files(dir: &std::path::Path, fixture: &Fixture) {
    for (path, data) in &fixture.files {
        let full = path.iter().fold(dir.join("pack"), |p, c| p.join(c));
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(full, data).unwrap();
    }
}

fn local_settings(dir: &std::path::Path) -> Settings {
    let mut settings = Settings::new(dir);
    settings.allow_loopback_peers = true;
    settings.seed_ratio = None;
    settings
}

async fn session() -> Arc<Session> {
    session_on("127.0.0.1").await
}

async fn session_on(ip: &str) -> Arc<Session> {
    Session::start(SessionConfig {
        bind: ip.parse().unwrap(),
        allow_private_trackers: true,
        ..SessionConfig::default()
    })
    .await
    .unwrap()
}

fn addr(session: &Session) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], session.port()))
}

fn addr_on(ip: [u8; 4], session: &Session) -> SocketAddr {
    SocketAddr::from((ip, session.port()))
}

fn verify(dir: &std::path::Path, fixture: &Fixture) {
    for (path, data) in &fixture.files {
        let full = path.iter().fold(dir.join("pack"), |p, c| p.join(c));
        assert_eq!(std::fs::read(&full).unwrap(), *data, "{}", full.display());
    }
}

async fn seeder(fixture: &Fixture) -> (Arc<Session>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    seed_files(dir.path(), fixture);
    let session = session().await;
    let handle = session.add_torrent(fixture.torrent.clone(), local_settings(dir.path()));
    handle.wait_complete(Duration::from_secs(20)).await.unwrap();
    assert_eq!(handle.status().state, State::Seeding);
    assert_eq!(handle.status().pieces_done, fixture.torrent.piece_count());
    (session, dir)
}

#[tokio::test]
async fn leecher_downloads_a_multi_file_torrent_from_a_seeder() {
    let fx = fixture();
    let (seed, _seed_dir) = seeder(&fx).await;
    let out = tempfile::tempdir().unwrap();
    let leech = session().await;
    let mut settings = local_settings(out.path());
    settings.peers.push(addr(&seed));
    let handle = leech.add_torrent(fx.torrent.clone(), settings);
    handle.wait_complete(Duration::from_secs(30)).await.unwrap();
    verify(out.path(), &fx);
    let status = handle.status();
    assert_eq!(status.completed_length, status.selected_length);
    assert!(status.downloaded >= 1_123_334);
    assert!(
        seed.list()[0].status().uploaded >= 1_123_334,
        "the seeder really uploaded the data"
    );
}

#[tokio::test]
async fn magnet_link_fetches_metadata_from_the_swarm_then_downloads() {
    let fx = fixture();
    let (seed, _seed_dir) = seeder(&fx).await;
    let out = tempfile::tempdir().unwrap();
    let leech = session().await;
    let magnet = Magnet::parse(&format!(
        "magnet:?xt=urn:btih:{}&dn=pack&x.pe={}",
        hex(&fx.torrent.info_hash),
        addr(&seed)
    ))
    .unwrap();
    let handle = leech.add_magnet(&magnet, local_settings(out.path()));
    assert!(!handle.status().has_metadata);
    let torrent = handle.wait_metadata(Duration::from_secs(20)).await.unwrap();
    assert_eq!(torrent.info_hash, fx.torrent.info_hash);
    assert_eq!(torrent.files.len(), 3);
    handle.wait_complete(Duration::from_secs(30)).await.unwrap();
    verify(out.path(), &fx);
}

#[tokio::test]
async fn only_selected_files_are_downloaded() {
    let fx = fixture();
    let (seed, _seed_dir) = seeder(&fx).await;
    let out = tempfile::tempdir().unwrap();
    let leech = session().await;
    let mut settings = local_settings(out.path());
    settings.peers.push(addr(&seed));
    settings.selected = Some(vec![1]);
    let handle = leech.add_torrent(fx.torrent.clone(), settings);
    handle.wait_complete(Duration::from_secs(30)).await.unwrap();
    let notes = out.path().join("pack/extra/notes.txt");
    assert_eq!(std::fs::read(notes).unwrap(), fx.files[1].1);
    let status = handle.status();
    assert_eq!(status.selected_length, 90_001);
    assert!(status.files.iter().filter(|f| f.selected).count() == 1);
    // Pieces shared with neighbouring files may be fetched, but not the whole torrent.
    assert!(
        status.downloaded < 400_000,
        "downloaded {}",
        status.downloaded
    );
}

#[tokio::test]
async fn resume_keeps_verified_pieces_and_fetches_only_the_missing_ones() {
    let fx = fixture();
    let (seed, _seed_dir) = seeder(&fx).await;
    let out = tempfile::tempdir().unwrap();
    // Pre-populate everything, then corrupt the middle of the big file.
    seed_files(out.path(), &fx);
    let video = out.path().join("pack/video.mp4");
    let mut bytes = std::fs::read(&video).unwrap();
    for b in &mut bytes[200_000..200_100] {
        *b ^= 0xff;
    }
    std::fs::write(&video, bytes).unwrap();
    let leech = session().await;
    let mut settings = local_settings(out.path());
    settings.peers.push(addr(&seed));
    let handle = leech.add_torrent(fx.torrent.clone(), settings);
    handle.wait_complete(Duration::from_secs(30)).await.unwrap();
    verify(out.path(), &fx);
    assert!(
        handle.status().downloaded <= 65_536,
        "only the damaged piece is downloaded again, got {}",
        handle.status().downloaded
    );
}

#[tokio::test]
async fn streaming_reads_arrive_in_order_even_after_a_seek_to_the_end() {
    let fx = fixture();
    let (seed, _seed_dir) = seeder(&fx).await;
    let out = tempfile::tempdir().unwrap();
    let leech = session().await;
    let mut settings = local_settings(out.path());
    settings.peers.push(addr(&seed));
    settings.selected = Some(vec![]); // nothing wanted until somebody starts watching
    let handle = leech.add_torrent(fx.torrent.clone(), settings);
    handle.wait_metadata(Duration::from_secs(20)).await.unwrap();
    // A player first probes the end of the file (the mp4 index), then plays from the start.
    let tail = handle
        .read_file(0, 690_000, 10_000, Duration::from_secs(20))
        .await
        .unwrap();
    assert_eq!(tail, fx.files[0].1[690_000..700_000]);
    let head = handle
        .read_file(0, 0, 100_000, Duration::from_secs(20))
        .await
        .unwrap();
    assert_eq!(head, fx.files[0].1[..100_000]);
    let status = handle.status();
    assert!(status.files[0].selected, "watching selects the file");
    assert!(!status.files[2].selected);
}

#[tokio::test]
async fn pause_and_resume_work() {
    let fx = fixture();
    let (seed, _seed_dir) = seeder(&fx).await;
    let out = tempfile::tempdir().unwrap();
    let leech = session().await;
    let mut settings = local_settings(out.path());
    settings.peers.push(addr(&seed));
    let handle = leech.add_torrent(fx.torrent.clone(), settings);
    handle.pause();
    assert_eq!(handle.status().state, State::Paused);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let paused_bytes = handle.status().downloaded;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(handle.status().downloaded, paused_bytes);
    handle.resume();
    handle.wait_complete(Duration::from_secs(30)).await.unwrap();
    verify(out.path(), &fx);
}

#[tokio::test]
async fn a_peer_serving_corrupt_data_is_rejected_and_the_download_still_completes() {
    let fx = fixture();
    let (good, _good_dir) = seeder(&fx).await;
    // A bad seeder claims the same torrent but its files are garbage.
    let bad_dir = tempfile::tempdir().unwrap();
    seed_files(bad_dir.path(), &fx);
    // A different loopback address: bans are per IP and must not hit the honest seeder.
    let bad = session_on("127.0.0.2").await;
    let bad_handle = bad.add_torrent(fx.torrent.clone(), local_settings(bad_dir.path()));
    bad_handle
        .wait_complete(Duration::from_secs(20))
        .await
        .unwrap();
    // Corrupt what it will send after it already verified: flip bytes on disk.
    for (path, data) in &fx.files {
        let full = path
            .iter()
            .fold(bad_dir.path().join("pack"), |p, c| p.join(c));
        let mut garbled = data.clone();
        for b in garbled.iter_mut().step_by(1000) {
            *b = b.wrapping_add(1);
        }
        std::fs::write(full, garbled).unwrap();
    }
    let out = tempfile::tempdir().unwrap();
    let leech = session().await;
    let mut settings = local_settings(out.path());
    settings.peers.push(addr_on([127, 0, 0, 2], &bad));
    settings.peers.push(addr(&good));
    let handle = leech.add_torrent(fx.torrent.clone(), settings);
    handle.wait_complete(Duration::from_secs(60)).await.unwrap();
    verify(out.path(), &fx);
}

#[tokio::test]
async fn adding_the_same_torrent_twice_returns_the_running_one() {
    let fx = fixture();
    let out = tempfile::tempdir().unwrap();
    let s = session().await;
    let a = s.add_torrent(fx.torrent.clone(), local_settings(out.path()));
    let b = s.add_torrent(fx.torrent.clone(), local_settings(out.path()));
    assert_eq!(a.info_hash(), b.info_hash());
    assert_eq!(s.list().len(), 1);
    assert!(s.remove(&a.info_hash(), false));
    assert!(s.list().is_empty());
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// ---- HTTP streaming ---------------------------------------------------------------------------

async fn http(port: u16, request: &str) -> (String, Vec<u8>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    tokio::time::timeout(Duration::from_secs(30), stream.read_to_end(&mut raw))
        .await
        .expect("response in time")
        .unwrap();
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("header end")
        + 4;
    (
        String::from_utf8_lossy(&raw[..split]).into_owned(),
        raw[split..].to_vec(),
    )
}

fn port_of(url: &str) -> u16 {
    url.split("127.0.0.1:")
        .nth(1)
        .unwrap()
        .split('/')
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn http_range_server_streams_a_file_that_is_still_downloading() {
    use nibiru_xtreme::bittorrent::stream::StreamServer;
    let fx = fixture();
    let (seed, _seed_dir) = seeder(&fx).await;
    let out = tempfile::tempdir().unwrap();
    let leech = session().await;
    let mut settings = local_settings(out.path());
    settings.peers.push(addr(&seed));
    settings.selected = Some(vec![]);
    let handle = leech.add_torrent(fx.torrent.clone(), settings);
    handle.wait_metadata(Duration::from_secs(20)).await.unwrap();
    let server = StreamServer::start(&leech).await.unwrap();
    let url = server.url(&handle.info_hash(), 0);
    let port = port_of(&url);
    let path = url
        .split(&format!("127.0.0.1:{port}"))
        .nth(1)
        .unwrap()
        .to_owned();
    let video = &fx.files[0].1;

    // HEAD: size and range support, no body.
    let (head, body) = http(
        port,
        &format!("HEAD {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n"),
    )
    .await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert!(head.contains("Content-Length: 700000"), "{head}");
    assert!(head.contains("Accept-Ranges: bytes") && head.contains("video/mp4"));
    assert!(body.is_empty());

    // The player first asks for the tail (index), then seeks around.
    let (head, body) = http(
        port,
        &format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nRange: bytes=-4096\r\n\r\n"),
    )
    .await;
    assert!(head.starts_with("HTTP/1.1 206"), "{head}");
    assert!(
        head.contains("Content-Range: bytes 695904-699999/700000"),
        "{head}"
    );
    assert_eq!(body, video[695_904..]);

    let (head, body) = http(
        port,
        &format!(
            "GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nRange: bytes=250000-349999\r\n\r\n"
        ),
    )
    .await;
    assert!(head.starts_with("HTTP/1.1 206"), "{head}");
    assert_eq!(body, video[250_000..350_000]);

    // Whole file.
    let (head, body) = http(
        port,
        &format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n"),
    )
    .await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert_eq!(body, *video);

    // Errors.
    let (head, _) = http(
        port,
        &format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nRange: bytes=900000-\r\n\r\n"),
    )
    .await;
    assert!(
        head.starts_with("HTTP/1.1 416") && head.contains("bytes */700000"),
        "{head}"
    );
    let (head, _) = http(
        port,
        &format!(
            "GET {} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n",
            path.replace("/stream/", "/stream/wrong")
        ),
    )
    .await;
    assert!(head.starts_with("HTTP/1.1 403"), "{head}");
    let (head, _) = http(
        port,
        &format!("GET {path} HTTP/1.1\r\nHost: attacker.example:{port}\r\n\r\n"),
    )
    .await;
    assert!(head.starts_with("HTTP/1.1 421"), "{head}");
    let (head, _) = http(
        port,
        &format!("POST {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n"),
    )
    .await;
    assert!(head.starts_with("HTTP/1.1 405"), "{head}");
    let bad = path.rsplit_once('/').unwrap().0.to_owned() + "/9";
    let (head, _) = http(
        port,
        &format!("GET {bad} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n"),
    )
    .await;
    assert!(head.starts_with("HTTP/1.1 404"), "{head}");
    // Streaming selected only the video; the other files were never requested.
    let status = handle.status();
    assert!(status.files[0].selected && !status.files[2].selected);
}
