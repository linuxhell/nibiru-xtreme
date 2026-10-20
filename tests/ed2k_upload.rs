//! Real-socket tests for the ED2K upload side: a Nibiru client downloading from a Nibiru
//! listener, and a raw wire client exercising the queue and the safety limits.

use nibiru_xtreme::ed2k::{
    FileLink, FileSource, IncomingListener, ShareRegistry, UploadConfig, UploadManager,
    download_from_sources,
};
use std::{
    net::Ipv4Addr,
    sync::{Arc, atomic::AtomicBool},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

fn sample(len: usize) -> Vec<u8> {
    // Deterministic, non-repeating-enough content.
    let mut x = 0x9e3779b97f4a7c15u64;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

async fn send(s: &mut TcpStream, protocol: u8, op: u8, body: &[u8]) {
    s.write_u8(protocol).await.unwrap();
    s.write_u32_le(body.len() as u32 + 1).await.unwrap();
    s.write_u8(op).await.unwrap();
    s.write_all(body).await.unwrap();
}

async fn recv(s: &mut TcpStream) -> (u8, u8, Vec<u8>) {
    let protocol = s.read_u8().await.unwrap();
    let len = s.read_u32_le().await.unwrap() as usize;
    let op = s.read_u8().await.unwrap();
    let mut body = vec![0; len - 1];
    s.read_exact(&mut body).await.unwrap();
    (protocol, op, body)
}

async fn recv_within(s: &mut TcpStream, secs: u64) -> Option<(u8, u8, Vec<u8>)> {
    tokio::time::timeout(Duration::from_secs(secs), async {
        let protocol = s.read_u8().await.ok()?;
        let len = s.read_u32_le().await.ok()? as usize;
        let op = s.read_u8().await.ok()?;
        let mut body = vec![0; len - 1];
        s.read_exact(&mut body).await.ok()?;
        Some((protocol, op, body))
    })
    .await
    .ok()
    .flatten()
}

fn hello(user: u8) -> Vec<u8> {
    let mut d = vec![16];
    d.extend([user; 16]);
    d.extend(0x7f000001u32.to_le_bytes());
    d.extend(4662u16.to_le_bytes());
    d.extend(1u32.to_le_bytes());
    let name = format!("client{user}");
    d.extend([2, 1, 0, 1]);
    d.extend((name.len() as u16).to_le_bytes());
    d.extend(name.as_bytes());
    d.extend([0; 6]);
    d
}

/// Connects and completes the hello exchange.
async fn connect(port: u16, user: u8) -> TcpStream {
    let mut s = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
        .await
        .unwrap();
    send(&mut s, 0xe3, 1, &hello(user)).await;
    let (_, op, _) = recv(&mut s).await;
    assert_eq!(op, 0x4c);
    s
}

struct Fixture {
    _dir: tempfile::TempDir,
    path: std::path::PathBuf,
    link: FileLink,
    bytes: Vec<u8>,
    listener: IncomingListener,
    uploads: UploadManager,
}

async fn fixture(len: usize, config: UploadConfig) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shared.bin");
    let bytes = sample(len);
    std::fs::write(&path, &bytes).unwrap();
    let shares = ShareRegistry::default();
    let info = shares.add_file(path.clone()).await.unwrap();
    let uploads = UploadManager::new(config);
    let listener = IncomingListener::bind_with_uploads([5; 16], 0, shares, uploads.clone())
        .await
        .unwrap();
    Fixture {
        link: FileLink::parse(&info.link).unwrap(),
        path,
        bytes,
        listener,
        uploads,
        _dir: dir,
    }
}

fn request_parts(hash: &[u8; 16], from: u32, to: u32) -> Vec<u8> {
    let mut r = hash.to_vec();
    for v in [from, 0, 0, to, 0, 0] {
        r.extend(v.to_le_bytes());
    }
    r
}

#[tokio::test]
async fn nibiru_downloads_a_two_part_file_from_a_nibiru_listener() {
    // Larger than one ED2K part (9,728,000 bytes), so the hashset is exchanged and verified.
    let f = fixture(10_000_000, UploadConfig::default()).await;
    let out = tempfile::tempdir().unwrap();
    let source = FileSource {
        client_id: 0x0100007f,
        host: Some(Ipv4Addr::LOCALHOST),
        port: f.listener.status().tcp_port,
        requires_callback: false,
    };
    let path = download_from_sources(
        &f.link,
        out.path(),
        &[source],
        [2; 16],
        0x0100007f,
        Arc::new(AtomicBool::new(false)),
        |_| {},
    )
    .await
    .unwrap();
    assert_eq!(std::fs::read(path).unwrap(), f.bytes);

    let stats = f.uploads.snapshot();
    assert_eq!(stats.total_uploaded, 10_000_000);
    assert_eq!(stats.files.len(), 1);
    assert_eq!(stats.files[0].uploaded, 10_000_000);
    assert!(stats.files[0].accepted >= 1);
}

#[tokio::test]
async fn extended_requests_and_unknown_hashes_are_answered_like_emule_does() {
    let f = fixture(50_000, UploadConfig::default()).await;
    let mut c = connect(f.listener.status().tcp_port, 7).await;
    // eMule appends part status and a complete-source count to OP_REQUESTFILENAME.
    let mut body = f.link.hash.to_vec();
    body.extend([0, 0, 1, 0]);
    send(&mut c, 0xe3, 0x58, &body).await;
    let (_, op, answer) = recv(&mut c).await;
    assert_eq!(op, 0x59);
    assert_eq!(&answer[..16], &f.link.hash);
    assert_eq!(&answer[18..], b"shared.bin");

    send(&mut c, 0xe3, 0x4f, &f.link.hash).await;
    let (_, op, status) = recv(&mut c).await;
    assert_eq!(op, 0x50);
    assert_eq!(
        &status[16..],
        &[0, 0],
        "a part count of zero means a complete file"
    );

    send(&mut c, 0xe3, 0x54, &[0xab; 16]).await;
    let (_, op, who) = recv(&mut c).await;
    assert_eq!((op, who), (0x48, vec![0xab; 16]));
}

#[tokio::test]
async fn a_full_server_queues_the_second_client_and_promotes_it_when_the_first_leaves() {
    let f = fixture(
        50_000,
        UploadConfig {
            max_active: 1,
            ..UploadConfig::default()
        },
    )
    .await;
    let port = f.listener.status().tcp_port;
    let mut first = connect(port, 1).await;
    let mut second = connect(port, 2).await;

    send(&mut first, 0xe3, 0x54, &f.link.hash).await;
    assert_eq!(recv(&mut first).await.1, 0x55);
    send(&mut second, 0xe3, 0x54, &f.link.hash).await;
    let (protocol, op, rank) = recv(&mut second).await;
    assert_eq!((protocol, op), (0xc5, 0x60));
    assert_eq!(u16::from_le_bytes([rank[0], rank[1]]), 1);
    let snapshot = f.uploads.snapshot();
    assert_eq!((snapshot.active.len(), snapshot.waiting.len()), (1, 1));
    assert_eq!(snapshot.waiting[0].rank, Some(1));

    // A queued client that asks for data without a slot gets nothing.
    send(
        &mut second,
        0xe3,
        0x47,
        &request_parts(&f.link.hash, 0, 1000),
    )
    .await;
    assert!(recv_within(&mut second, 1).await.is_none());

    // The first client cancels: the second one is accepted without asking again.
    send(&mut first, 0xe3, 0x56, &[]).await;
    let (_, op, _) = recv_within(&mut second, 5).await.expect("promotion");
    assert_eq!(op, 0x55);

    // And it can now download.
    send(
        &mut second,
        0xe3,
        0x47,
        &request_parts(&f.link.hash, 0, 25_000),
    )
    .await;
    let mut got = 0u64;
    while got < 25_000 {
        let (_, op, packet) = recv(&mut second).await;
        assert_eq!(op, 0x46);
        let from = u32::from_le_bytes(packet[16..20].try_into().unwrap()) as usize;
        let to = u32::from_le_bytes(packet[20..24].try_into().unwrap()) as usize;
        assert!(to - from <= 10_240, "packets carry at most 10240 bytes");
        assert_eq!(&packet[24..], &f.bytes[from..to]);
        got += (to - from) as u64;
    }
}

#[tokio::test]
async fn a_client_that_drops_frees_its_slot_for_the_next_one() {
    let f = fixture(
        50_000,
        UploadConfig {
            max_active: 1,
            ..UploadConfig::default()
        },
    )
    .await;
    let port = f.listener.status().tcp_port;
    let mut first = connect(port, 1).await;
    let mut second = connect(port, 2).await;
    send(&mut first, 0xe3, 0x54, &f.link.hash).await;
    recv(&mut first).await;
    send(&mut second, 0xe3, 0x54, &f.link.hash).await;
    recv(&mut second).await;
    drop(first);
    let (_, op, _) = recv_within(&mut second, 5)
        .await
        .expect("promotion after disconnect");
    assert_eq!(op, 0x55);
}

#[tokio::test]
async fn invalid_ranges_and_a_file_changed_on_disk_end_the_session() {
    let f = fixture(50_000, UploadConfig::default()).await;
    let port = f.listener.status().tcp_port;

    let mut c = connect(port, 1).await;
    send(&mut c, 0xe3, 0x54, &f.link.hash).await;
    assert_eq!(recv(&mut c).await.1, 0x55);
    // Beyond the end of the file.
    send(&mut c, 0xe3, 0x47, &request_parts(&f.link.hash, 0, 60_000)).await;
    assert!(
        recv_within(&mut c, 2).await.is_none(),
        "connection closed, no data"
    );

    let mut c = connect(port, 2).await;
    send(&mut c, 0xe3, 0x54, &f.link.hash).await;
    assert_eq!(recv(&mut c).await.1, 0x55);
    // Absurdly large single range.
    send(&mut c, 0xe3, 0x47, &request_parts(&f.link.hash, 0, 300_000)).await;
    assert!(recv_within(&mut c, 2).await.is_none());

    // The shared file is modified after it was hashed: nothing is served.
    std::fs::write(&f.path, b"tampered").unwrap();
    let mut c = connect(port, 3).await;
    send(&mut c, 0xe3, 0x54, &f.link.hash).await;
    assert_eq!(recv(&mut c).await.1, 0x55);
    send(&mut c, 0xe3, 0x47, &request_parts(&f.link.hash, 0, 1000)).await;
    assert!(recv_within(&mut c, 2).await.is_none());
}

#[tokio::test]
async fn the_bandwidth_limit_applies_to_real_transfers() {
    let f = fixture(
        1_000_000,
        UploadConfig {
            bytes_per_second: Some(400_000),
            ..UploadConfig::default()
        },
    )
    .await;
    let mut c = connect(f.listener.status().tcp_port, 1).await;
    send(&mut c, 0xe3, 0x54, &f.link.hash).await;
    assert_eq!(recv(&mut c).await.1, 0x55);
    let started = Instant::now();
    let mut got = 0u64;
    let mut next = 0u32;
    while got < 1_000_000 {
        let to = (next + 180_000).min(1_000_000);
        send(&mut c, 0xe3, 0x47, &request_parts(&f.link.hash, next, to)).await;
        let want = (to - next) as u64;
        let mut have = 0;
        while have < want {
            let (_, _, packet) = recv(&mut c).await;
            have += (packet.len() - 24) as u64;
        }
        got += want;
        next = to;
    }
    // 1 MB at 400 kB/s with a one-second burst: well over a second.
    assert!(
        started.elapsed() >= Duration::from_millis(1300),
        "{:?}",
        started.elapsed()
    );
}
