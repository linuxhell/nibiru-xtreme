//! BEP52 fixtures generated independently by libtorrent 2.1.1: an empty file,
//! a short file and a two-piece file, including inter-file and final padding.
use nibiru_xtreme::bittorrent::{
    bencode::{self, Value},
    magnet::Magnet,
    metainfo::Torrent,
    session::{Session, SessionConfig},
    storage::Storage,
    torrent::Settings,
};
use std::{path::Path, time::Duration};

const V2: &[u8] = include_bytes!("fixtures/v2-small.torrent");
const HYBRID: &[u8] = include_bytes!("fixtures/hybrid-small.torrent");

fn seed(directory: &Path) {
    std::fs::create_dir_all(directory.join("root")).unwrap();
    for (name, data) in [
        ("empty.txt", vec![]),
        ("one.bin", vec![b'a'; 20_000]),
        ("two.bin", vec![b'b'; 100_000]),
    ] {
        std::fs::write(directory.join("root").join(name), data).unwrap();
    }
}

#[test]
fn independent_torrents_round_trip_and_corrupt_layers_are_rejected() {
    for bytes in [V2, HYBRID] {
        let torrent = Torrent::from_torrent_file(bytes).unwrap();
        assert!(torrent.wire_v2);
        assert_eq!(torrent.piece_count(), 3);
        assert_eq!(
            (0..3).map(|i| torrent.piece_len(i)).collect::<Vec<_>>(),
            vec![20000, 65536, 34464]
        );
        assert_eq!(
            Torrent::from_torrent_file(&torrent.to_torrent_file()).unwrap(),
            torrent
        );
        let mut root = bencode::decode(bytes).unwrap().value;
        if let Value::Dict(map) = &mut root
            && let Value::Dict(layers) = map.get_mut(b"piece layers".as_slice()).unwrap()
            && let Value::Bytes(hashes) = layers.values_mut().next().unwrap()
        {
            hashes[0] ^= 1;
        }
        assert!(Torrent::from_torrent_file(&bencode::encode(&root)).is_err());
    }
}

#[test]
fn corrupt_merkle_proofs_cannot_enable_a_piece() {
    let full = Torrent::from_torrent_file(V2).unwrap();
    let pending = Torrent::from_info(&full.info_bytes, Default::default()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    seed(directory.path());
    let source = Storage::new(&full, directory.path());
    let target = Storage::new(&pending, directory.path());
    let have = source.check_all(|_, _| {}, &std::sync::atomic::AtomicBool::new(false));
    let v2 = full.v2.as_ref().unwrap();
    let file = &v2.files[1];
    let request = v2.request(file, 0);
    let mut hashes = source.hash_response(&request, &have).unwrap();
    assert!(!target.v2_piece_known(file.first_piece));
    hashes[0][0] ^= 1;
    assert!(target.install_v2_proof(&request, &hashes).is_err());
    assert!(!target.v2_piece_known(file.first_piece));
    hashes[0][0] ^= 1;
    assert_eq!(target.install_v2_proof(&request, &hashes).unwrap().len(), 2);
    assert!(target.check_piece(file.first_piece));
}

async fn transfer(bytes: &[u8], magnet: bool, legacy: bool) {
    let mut torrent = Torrent::from_torrent_file(bytes).unwrap();
    let seed_dir = tempfile::tempdir().unwrap();
    seed(seed_dir.path());
    let config = || SessionConfig {
        bind: "127.0.0.1".parse().unwrap(),
        ..Default::default()
    };
    let seeder = Session::start(config()).await.unwrap();
    let leecher = Session::start(config()).await.unwrap();
    let mut settings = Settings::new(seed_dir.path());
    settings.allow_loopback_peers = true;
    settings.seed_ratio = None;
    let seed = seeder.add_torrent(torrent.clone(), settings);
    seed.wait_complete(Duration::from_secs(5)).await.unwrap();
    if legacy {
        let v1 = torrent.v1_info_hash.unwrap();
        assert_eq!(seeder.get(&v1).unwrap().info_hash(), seed.info_hash());
        torrent.select_wire_hash(v1).unwrap();
    }
    let directory = tempfile::tempdir().unwrap();
    let mut settings = Settings::new(directory.path());
    settings.allow_loopback_peers = true;
    settings.peers.push(([127, 0, 0, 1], seeder.port()).into());
    let download = if magnet {
        let hash = torrent
            .v2
            .as_ref()
            .unwrap()
            .info_hash
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        leecher.add_magnet(
            &Magnet::parse(&format!("magnet:?xt=urn:btmh:1220{hash}")).unwrap(),
            settings,
        )
    } else {
        leecher.add_torrent(torrent, settings)
    };
    download
        .wait_complete(Duration::from_secs(15))
        .await
        .unwrap();
    let metadata = download
        .wait_metadata(Duration::from_secs(2))
        .await
        .unwrap();
    assert!(Torrent::from_torrent_file(&metadata.to_torrent_file()).is_ok());
    assert_eq!(
        std::fs::read(directory.path().join("root/one.bin")).unwrap(),
        vec![b'a'; 20000]
    );
    assert_eq!(
        std::fs::read(directory.path().join("root/two.bin")).unwrap(),
        vec![b'b'; 100000]
    );
    assert_eq!(
        std::fs::metadata(directory.path().join("root/empty.txt"))
            .unwrap()
            .len(),
        0
    );
    download.shutdown(false);
    if legacy {
        assert!(seeder.remove(&metadata.v1_info_hash.unwrap(), false));
        assert!(seeder.list().is_empty());
    }
    seed.shutdown(false);
}

#[tokio::test]
async fn v2_multifile_transfer_with_short_pieces() {
    transfer(V2, false, false).await;
}
#[tokio::test]
async fn v2_magnet_fetches_authenticated_hashes_and_data() {
    transfer(V2, true, false).await;
}
#[tokio::test]
async fn hybrid_transfer_verifies_sha1_and_sha256() {
    transfer(HYBRID, false, false).await;
}
#[tokio::test]
async fn hybrid_magnet_uses_the_v2_swarm() {
    transfer(HYBRID, true, false).await;
}

#[tokio::test]
async fn hybrid_v2_seeder_serves_v1_identity_with_padding() {
    transfer(HYBRID, false, true).await;
}
