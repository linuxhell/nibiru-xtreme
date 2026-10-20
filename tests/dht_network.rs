//! A small DHT on loopback: nodes bootstrap, announce and find each other's peers.

use nibiru_xtreme::bittorrent::dht::{Dht, DhtConfig};
use std::{net::SocketAddr, sync::Arc, time::Duration};

async fn node(bootstrap: Vec<String>) -> Arc<Dht> {
    Dht::start(DhtConfig {
        bind: "127.0.0.1".parse().unwrap(),
        port: 0,
        bootstrap,
        allow_loopback: true,
    })
    .await
    .unwrap()
}

async fn network(size: usize) -> Vec<Arc<Dht>> {
    let first = node(vec![]).await;
    let seed = format!("127.0.0.1:{}", first.port());
    let mut nodes = vec![first];
    for _ in 1..size {
        let n = node(vec![seed.clone()]).await;
        n.bootstrap().await;
        nodes.push(n);
    }
    // A second pass lets early joiners learn about later ones.
    for n in &nodes {
        n.bootstrap().await;
    }
    nodes
}

#[tokio::test]
async fn nodes_bootstrap_and_find_peers_announced_by_others() {
    let nodes = network(14).await;
    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.node_count() >= 3,
            "node {i} knows only {}",
            n.node_count()
        );
    }
    let info_hash = [0x42u8; 20];

    // Node 3 serves the torrent on TCP port 51413 and announces it.
    let lookup = nodes[3].get_peers(info_hash).await;
    assert!(lookup.peers.is_empty());
    assert!(!lookup.tokens.is_empty(), "closest nodes hand out tokens");
    let accepted = nodes[3].announce(info_hash, 51413, &lookup.tokens).await;
    assert!(accepted >= 1, "at least one node accepted the announce");

    // Node 11, which has never heard of it, finds the peer.
    let found = nodes[11].get_peers(info_hash).await;
    assert!(
        found
            .peers
            .contains(&SocketAddr::from(([127, 0, 0, 1], 51413))),
        "peers: {:?}",
        found.peers
    );
}

#[tokio::test]
async fn announces_with_a_bad_token_are_refused() {
    let nodes = network(5).await;
    let hash = [9u8; 20];
    let lookup = nodes[1].get_peers(hash).await;
    assert!(!lookup.tokens.is_empty());
    let forged: Vec<_> = lookup
        .tokens
        .iter()
        .map(|(a, _)| (*a, vec![0xde, 0xad, 0xbe, 0xef]))
        .collect();
    assert_eq!(nodes[1].announce(hash, 6881, &forged).await, 0);
    let found = nodes[2].get_peers(hash).await;
    assert!(found.peers.is_empty(), "nothing was stored");
}

#[tokio::test]
async fn ipv6_nodes_bootstrap_announce_and_find_peers() {
    let start = |bootstrap| async {
        Dht::start(DhtConfig {
            bind: "::1".parse().unwrap(),
            port: 0,
            bootstrap,
            allow_loopback: true,
        })
        .await
        .unwrap()
    };
    let router = start(vec![]).await;
    let seed = start(vec![format!("[::1]:{}", router.port())]).await;
    let client = start(vec![format!("[::1]:{}", router.port())]).await;
    seed.bootstrap().await;
    client.bootstrap().await;
    let hash = [0x39; 20];
    let lookup = seed.get_peers(hash).await;
    assert!(!lookup.tokens.is_empty());
    assert!(seed.announce(hash, 51413, &lookup.tokens).await > 0);
    let found = client.get_peers(hash).await;
    assert!(
        found.peers.contains(&"[::1]:51413".parse().unwrap()),
        "{:?}",
        found.peers
    );
    assert!(found.peers.iter().all(SocketAddr::is_ipv6));
}

#[tokio::test]
async fn silent_or_non_dht_addresses_do_not_hang_or_poison_the_table() {
    let a = node(vec![]).await;
    let silent = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = silent.local_addr().unwrap();
    let started = std::time::Instant::now();
    assert!(!a.ping(addr).await);
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(a.node_count(), 0);
    // Garbage datagrams are ignored and the node keeps working.
    let probe = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    for junk in [
        &b"not bencode"[..],
        b"d1:y1:qe",
        b"d1:t2:aa1:y1:q1:q4:ping1:ad2:id3:abcee",
        &[0u8; 3000][..],
    ] {
        probe.send_to(junk, ("127.0.0.1", a.port())).await.unwrap();
    }
    let b = node(vec![]).await;
    assert!(b.ping(SocketAddr::from(([127, 0, 0, 1], a.port()))).await);
    assert_eq!(b.node_count(), 1);
}
