//! BEP 5 distributed hash table (IPv4): routing table, KRPC server and iterative lookups.
//!
//! This is what lets a bare `magnet:?xt=urn:btih:...` find peers without a tracker. Everything
//! received from the network is bounded: packet size, table size, stored peers, replies per
//! second. Responses are accepted only from the address that was queried, with the transaction
//! id we generated.

use super::bencode::{self, Value, dict};
use anyhow::{Context, Result, bail};
use sha1::{Digest, Sha1};
use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU16, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{net::UdpSocket, sync::oneshot, time::timeout};

pub type NodeId = [u8; 20];

const K: usize = 8;
const MAX_PACKET: usize = 4096;
const QUERY_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_TORRENTS: usize = 2000;
const MAX_PEERS_PER_TORRENT: usize = 100;
const PEER_TTL: Duration = Duration::from_secs(30 * 60);
const SECRET_LIFETIME: Duration = Duration::from_secs(5 * 60);
const MAX_QUERIES_PER_SECOND: u64 = 300;
const ALPHA: usize = 3;
const MAX_LOOKUP_ROUNDS: usize = 10;
const DEFAULT_BOOTSTRAP: [&str; 4] = [
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "router.utorrent.com:6881",
    "dht.libtorrent.org:25401",
];

#[derive(Clone, Debug)]
pub struct DhtConfig {
    pub bind: IpAddr,
    /// 0 picks any free UDP port.
    pub port: u16,
    /// `host:port` entries used when the routing table is empty.
    pub bootstrap: Vec<String>,
    /// Accept nodes on loopback addresses (tests, local clusters).
    pub allow_loopback: bool,
}

impl Default for DhtConfig {
    fn default() -> Self {
        Self {
            bind: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: 0,
            bootstrap: DEFAULT_BOOTSTRAP.iter().map(|s| (*s).to_owned()).collect(),
            allow_loopback: false,
        }
    }
}

#[derive(Clone, Debug)]
struct Node {
    id: NodeId,
    addr: SocketAddr,
    last_seen: Instant,
    failures: u8,
}

struct State {
    buckets: Vec<Vec<Node>>,
    store: HashMap<NodeId, Vec<(SocketAddr, Instant)>>,
    secret: [u8; 16],
    previous_secret: [u8; 16],
    rotated: Instant,
}

pub struct Dht {
    socket: Arc<UdpSocket>,
    id: Mutex<NodeId>,
    ip_votes: Mutex<HashMap<IpAddr, IpAddr>>,
    port: u16,
    allow_loopback: bool,
    ipv6: bool,
    bootstrap: Vec<String>,
    state: Mutex<State>,
    pending: Mutex<HashMap<(SocketAddr, u16), oneshot::Sender<Value>>>,
    next_tid: AtomicU16,
    window_start: Mutex<Instant>,
    window_count: AtomicU64,
    stop: crate::lifetime::Stop,
}

/// Outcome of a `get_peers` lookup.
#[derive(Debug, Default)]
pub struct Lookup {
    pub peers: Vec<SocketAddr>,
    /// Closest nodes that answered, with the token each handed out (needed to announce).
    pub tokens: Vec<(SocketAddr, Vec<u8>)>,
}

pub fn distance(a: &NodeId, b: &NodeId) -> NodeId {
    let mut out = [0u8; 20];
    for i in 0..20 {
        out[i] = a[i] ^ b[i];
    }
    out
}

/// Index of the bucket for `other` seen from `own`: the position of the first differing bit.
fn bucket_index(own: &NodeId, other: &NodeId) -> Option<usize> {
    let d = distance(own, other);
    for (i, byte) in d.iter().enumerate() {
        if *byte != 0 {
            return Some(i * 8 + byte.leading_zeros() as usize);
        }
    }
    None
}

fn random_bytes<const N: usize>() -> [u8; N] {
    let mut out = [0u8; N];
    let mut filled = 0;
    while filled < N {
        let chunk = uuid::Uuid::new_v4();
        let take = (N - filled).min(16);
        out[filled..filled + take].copy_from_slice(&chunk.as_bytes()[..take]);
        filled += take;
    }
    out
}

pub fn encode_peer(addr: &SocketAddr) -> Option<Vec<u8>> {
    if addr.port() == 0 {
        return None;
    }
    let mut out = match addr.ip() {
        IpAddr::V4(ip) => ip.octets().to_vec(),
        IpAddr::V6(ip) => ip.octets().to_vec(),
    };
    out.extend(addr.port().to_be_bytes());
    Some(out)
}

pub fn decode_peer(bytes: &[u8]) -> Option<SocketAddr> {
    let (ip, port) = match bytes.len() {
        6 => (
            IpAddr::V4(std::net::Ipv4Addr::from(
                <[u8; 4]>::try_from(&bytes[..4]).ok()?,
            )),
            u16::from_be_bytes(bytes[4..].try_into().ok()?),
        ),
        18 => (
            IpAddr::V6(std::net::Ipv6Addr::from(
                <[u8; 16]>::try_from(&bytes[..16]).ok()?,
            )),
            u16::from_be_bytes(bytes[16..].try_into().ok()?),
        ),
        _ => return None,
    };
    (port != 0).then(|| SocketAddr::new(ip, port))
}

fn encode_nodes(nodes: &[Node]) -> Vec<u8> {
    let mut out = Vec::new();
    for node in nodes {
        if let Some(peer) = encode_peer(&node.addr) {
            out.extend_from_slice(&node.id);
            out.extend_from_slice(&peer);
        }
    }
    out
}

fn decode_nodes(bytes: &[u8], ipv6: bool) -> Vec<(NodeId, SocketAddr)> {
    let size = if ipv6 { 38 } else { 26 };
    if !bytes.len().is_multiple_of(size) {
        return vec![];
    }
    bytes
        .chunks_exact(size)
        .filter_map(|chunk| Some((chunk[..20].try_into().ok()?, decode_peer(&chunk[20..])?)))
        .collect()
}

impl Dht {
    pub async fn start(config: DhtConfig) -> Result<Arc<Self>> {
        let socket = UdpSocket::bind(SocketAddr::new(config.bind, config.port))
            .await
            .context("cannot bind the DHT UDP socket")?;
        let port = socket.local_addr()?.port();
        let dht = Arc::new(Self {
            socket: Arc::new(socket),
            id: Mutex::new(super::dht_security::bind(config.bind, random_bytes())),
            ip_votes: Mutex::new(HashMap::new()),
            port,
            allow_loopback: config.allow_loopback,
            ipv6: config.bind.is_ipv6(),
            bootstrap: config.bootstrap,
            state: Mutex::new(State {
                buckets: vec![Vec::new(); 160],
                store: HashMap::new(),
                secret: random_bytes(),
                previous_secret: random_bytes(),
                rotated: Instant::now(),
            }),
            pending: Mutex::new(HashMap::new()),
            next_tid: AtomicU16::new(1),
            window_start: Mutex::new(Instant::now()),
            window_count: AtomicU64::new(0),
            stop: crate::lifetime::Stop::default(),
        });
        let weak = Arc::downgrade(&dht);
        let socket = dht.socket.clone();
        let mut stopped = dht.stop.subscribe();
        tokio::spawn(async move {
            let mut buffer = vec![0u8; MAX_PACKET];
            loop {
                let received = tokio::select! {
                    _ = stopped.changed() => return,
                    value = socket.recv_from(&mut buffer) => value,
                };
                let Ok((n, from)) = received else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                };
                let Some(dht) = weak.upgrade() else { return };
                let packet = buffer[..n].to_vec();
                dht.on_packet(&packet, from).await;
            }
        });
        let weak = Arc::downgrade(&dht);
        let mut stopped = dht.stop.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stopped.changed() => return,
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {},
                }
                let Some(dht) = weak.upgrade() else { return };
                dht.maintain().await;
            }
        });
        Ok(dht)
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn id(&self) -> NodeId {
        *self.id.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn observe_ip(&self, reporter: SocketAddr, observed: SocketAddr) {
        use super::dht_security::{bind, local, voting_network};
        if local(reporter.ip()) || local(observed.ip()) || observed.is_ipv6() != self.ipv6 {
            return;
        }
        let enough = {
            let mut votes = self.ip_votes.lock().unwrap_or_else(|e| e.into_inner());
            if votes.len() >= 32 && !votes.contains_key(&voting_network(reporter.ip())) {
                votes.clear();
            }
            votes.insert(voting_network(reporter.ip()), observed.ip());
            votes.values().filter(|ip| **ip == observed.ip()).count() >= 3
        };
        if enough {
            let changed = {
                let mut id = self.id.lock().unwrap_or_else(|e| e.into_inner());
                let new = bind(observed.ip(), *id);
                let changed = *id != new;
                *id = new;
                changed
            };
            if changed {
                // Re-bucket the known nodes around the newly bound identity.
                let nodes = {
                    let mut state = self.state();
                    state
                        .buckets
                        .iter_mut()
                        .flat_map(std::mem::take)
                        .collect::<Vec<_>>()
                };
                for node in nodes {
                    self.insert(node.id, node.addr);
                }
            }
        }
    }

    pub fn node_count(&self) -> usize {
        self.state().buckets.iter().map(Vec::len).sum()
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn address_ok(&self, addr: &SocketAddr) -> bool {
        let ip = addr.ip();
        addr.port() != 0
            && addr.is_ipv6() == self.ipv6
            && !ip.is_unspecified()
            && !ip.is_multicast()
            && (self.allow_loopback || !ip.is_loopback())
            && match ip {
                IpAddr::V4(ip) => !ip.is_broadcast() && !ip.is_link_local(),
                IpAddr::V6(ip) => !ip.is_unicast_link_local() && ip.to_ipv4_mapped().is_none(),
            }
    }

    fn nodes_key(&self) -> &'static str {
        if self.ipv6 { "nodes6" } else { "nodes" }
    }

    // ---- routing table ------------------------------------------------------------------

    fn insert(&self, id: NodeId, addr: SocketAddr) {
        if id == self.id() || !self.address_ok(&addr) || !super::dht_security::valid(addr.ip(), &id)
        {
            return;
        }
        let Some(index) = bucket_index(&self.id(), &id) else {
            return;
        };
        let mut state = self.state();
        let bucket = &mut state.buckets[index];
        if let Some(node) = bucket.iter_mut().find(|n| n.id == id) {
            node.addr = addr;
            node.last_seen = Instant::now();
            node.failures = 0;
            return;
        }
        // One entry per address: a node cannot occupy many slots by changing ids.
        if bucket.iter().any(|n| n.addr == addr) {
            return;
        }
        let node = Node {
            id,
            addr,
            last_seen: Instant::now(),
            failures: 0,
        };
        if bucket.len() < K {
            bucket.push(node);
        } else if let Some(bad) = bucket.iter().position(|n| n.failures >= 2) {
            bucket[bad] = node;
        }
        // Otherwise the bucket is full of good nodes: keep them (long-lived nodes stay up).
    }

    fn record_failure(&self, addr: &SocketAddr) {
        let mut state = self.state();
        for bucket in state.buckets.iter_mut() {
            for node in bucket.iter_mut().filter(|n| n.addr == *addr) {
                node.failures = node.failures.saturating_add(1);
            }
            bucket.retain(|n| n.failures < 4);
        }
    }

    fn closest(&self, target: &NodeId, count: usize) -> Vec<Node> {
        let state = self.state();
        let mut all: Vec<&Node> = state
            .buckets
            .iter()
            .flatten()
            .filter(|n| n.failures < 2)
            .collect();
        all.sort_by_key(|n| distance(&n.id, target));
        all.into_iter().take(count).cloned().collect()
    }

    pub fn export_nodes(&self) -> Vec<SocketAddr> {
        self.state()
            .buckets
            .iter()
            .flatten()
            .map(|n| n.addr)
            .collect()
    }

    // ---- tokens and peer store ----------------------------------------------------------

    fn token_for(secret: &[u8; 16], addr: &SocketAddr) -> Vec<u8> {
        let mut hasher = Sha1::new();
        hasher.update(secret);
        match addr.ip() {
            IpAddr::V4(ip) => hasher.update(ip.octets()),
            IpAddr::V6(ip) => hasher.update(ip.octets()),
        }
        hasher.finalize()[..8].to_vec()
    }

    fn rotate_token_secrets(state: &mut State) {
        let elapsed = state.rotated.elapsed();
        if elapsed >= SECRET_LIFETIME + SECRET_LIFETIME {
            // After two complete windows neither previously issued secret is valid.
            state.previous_secret = random_bytes();
            state.secret = random_bytes();
            state.rotated = Instant::now();
        } else if elapsed >= SECRET_LIFETIME {
            state.previous_secret = state.secret;
            state.secret = random_bytes();
            state.rotated = Instant::now();
        }
    }

    fn make_token(&self, addr: &SocketAddr) -> Vec<u8> {
        let mut state = self.state();
        Self::rotate_token_secrets(&mut state);
        Self::token_for(&state.secret, addr)
    }

    fn token_valid(&self, addr: &SocketAddr, token: &[u8]) -> bool {
        let mut state = self.state();
        Self::rotate_token_secrets(&mut state);
        Self::token_for(&state.secret, addr) == token
            || Self::token_for(&state.previous_secret, addr) == token
    }

    fn store_peer(&self, info_hash: NodeId, peer: SocketAddr) {
        let mut state = self.state();
        if !state.store.contains_key(&info_hash) && state.store.len() >= MAX_TORRENTS {
            return;
        }
        let peers = state.store.entry(info_hash).or_default();
        peers.retain(|(_, at)| at.elapsed() < PEER_TTL);
        if let Some(entry) = peers.iter_mut().find(|(p, _)| *p == peer) {
            entry.1 = Instant::now();
        } else if peers.len() < MAX_PEERS_PER_TORRENT {
            peers.push((peer, Instant::now()));
        }
    }

    fn stored_peers(&self, info_hash: &NodeId) -> Vec<SocketAddr> {
        let mut state = self.state();
        let Some(peers) = state.store.get_mut(info_hash) else {
            return Vec::new();
        };
        peers.retain(|(_, at)| at.elapsed() < PEER_TTL);
        peers.iter().map(|(p, _)| *p).take(50).collect()
    }

    // ---- wire ---------------------------------------------------------------------------

    fn rate_limited(&self) -> bool {
        let mut start = self.window_start.lock().unwrap_or_else(|e| e.into_inner());
        if start.elapsed() >= Duration::from_secs(1) {
            *start = Instant::now();
            self.window_count.store(0, Ordering::Relaxed);
        }
        self.window_count.fetch_add(1, Ordering::Relaxed) >= MAX_QUERIES_PER_SECOND
    }

    async fn send(&self, to: SocketAddr, value: &Value) {
        let _ = self.socket.send_to(&bencode::encode(value), to).await;
    }

    async fn on_packet(&self, packet: &[u8], from: SocketAddr) {
        let Ok(decoded) = bencode::decode(packet) else {
            return;
        };
        let message = decoded.value;
        let tid = message
            .get("t")
            .and_then(Value::bytes)
            .unwrap_or_default()
            .to_vec();
        match message.get("y").and_then(Value::str) {
            Some("q") => {
                if self.rate_limited() || !self.address_ok(&from) {
                    return;
                }
                if let Some(mut reply) = self.answer(&message, &tid, from) {
                    if let Value::Dict(fields) = &mut reply
                        && let Some(ip) = encode_peer(&from)
                    {
                        fields.insert(b"ip".to_vec(), Value::Bytes(ip));
                    }
                    self.send(from, &reply).await;
                }
            }
            Some("r") | Some("e") => {
                let Ok(tid) = <[u8; 2]>::try_from(tid.as_slice()) else {
                    return;
                };
                let key = (from, u16::from_be_bytes(tid));
                let waiter = self.pending.lock().ok().and_then(|mut p| p.remove(&key));
                if let Some(waiter) = waiter {
                    let _ = waiter.send(message);
                }
            }
            _ => {}
        }
    }

    fn error(tid: &[u8], code: i64, text: &str) -> Value {
        dict([
            ("t", Value::Bytes(tid.to_vec())),
            ("y", Value::text("e")),
            ("e", Value::List(vec![Value::Int(code), Value::text(text)])),
        ])
    }

    fn reply(&self, tid: &[u8], mut fields: Vec<(&str, Value)>) -> Value {
        fields.push(("id", Value::Bytes(self.id().to_vec())));
        let r = Value::Dict(
            fields
                .into_iter()
                .map(|(k, v)| (k.as_bytes().to_vec(), v))
                .collect(),
        );
        dict([
            ("t", Value::Bytes(tid.to_vec())),
            ("y", Value::text("r")),
            ("r", r),
        ])
    }

    fn answer(&self, message: &Value, tid: &[u8], from: SocketAddr) -> Option<Value> {
        let args = message.get("a")?;
        let their_id: NodeId = args.get("id")?.bytes()?.try_into().ok()?;
        self.insert(their_id, from);
        let query = message.get("q")?.str()?;
        let target = |key: &str| -> Option<NodeId> { args.get(key)?.bytes()?.try_into().ok() };
        Some(match query {
            "ping" => self.reply(tid, vec![]),
            "find_node" => {
                let Some(target) = target("target") else {
                    return Some(Self::error(tid, 203, "missing target"));
                };
                let nodes = encode_nodes(&self.closest(&target, K));
                self.reply(tid, vec![(self.nodes_key(), Value::Bytes(nodes))])
            }
            "get_peers" => {
                let Some(hash) = target("info_hash") else {
                    return Some(Self::error(tid, 203, "missing info_hash"));
                };
                let mut fields = vec![("token", Value::Bytes(self.make_token(&from)))];
                let peers = self.stored_peers(&hash);
                if peers.is_empty() {
                    fields.push((
                        self.nodes_key(),
                        Value::Bytes(encode_nodes(&self.closest(&hash, K))),
                    ));
                } else {
                    let values = peers
                        .iter()
                        .filter_map(encode_peer)
                        .map(|p| Value::Bytes(p.to_vec()))
                        .collect();
                    fields.push(("values", Value::List(values)));
                }
                self.reply(tid, fields)
            }
            "announce_peer" => {
                let Some(hash) = target("info_hash") else {
                    return Some(Self::error(tid, 203, "missing info_hash"));
                };
                let token = args.get("token").and_then(Value::bytes).unwrap_or_default();
                if !self.token_valid(&from, token) {
                    return Some(Self::error(tid, 203, "bad token"));
                }
                let implied = args.get("implied_port").and_then(Value::int).unwrap_or(0) != 0;
                let port = if implied {
                    from.port()
                } else {
                    args.get("port")
                        .and_then(Value::int)
                        .and_then(|p| u16::try_from(p).ok())
                        .unwrap_or(0)
                };
                if port == 0 {
                    return Some(Self::error(tid, 203, "bad port"));
                }
                self.store_peer(hash, SocketAddr::new(from.ip(), port));
                self.reply(tid, vec![])
            }
            _ => Self::error(tid, 204, "method unknown"),
        })
    }

    /// Sends a query and waits for the matching response (`r` dictionary).
    async fn query(&self, to: SocketAddr, name: &str, args: Vec<(&str, Value)>) -> Result<Value> {
        if !self.address_ok(&to) {
            bail!("refusing to query {to}");
        }
        let tid = self.next_tid.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .map_err(|_| anyhow::anyhow!("poisoned"))?
            .insert((to, tid), tx);
        let mut a: Vec<(Vec<u8>, Value)> = vec![(b"id".to_vec(), Value::Bytes(self.id().to_vec()))];
        a.extend(args.into_iter().map(|(k, v)| (k.as_bytes().to_vec(), v)));
        let message = dict([
            ("t", Value::Bytes(tid.to_be_bytes().to_vec())),
            ("y", Value::text("q")),
            ("q", Value::text(name)),
            ("a", Value::Dict(a.into_iter().collect())),
        ]);
        self.send(to, &message).await;
        let result = timeout(QUERY_TIMEOUT, rx).await;
        if let Ok(mut pending) = self.pending.lock() {
            pending.remove(&(to, tid));
        }
        let response = match result {
            Ok(Ok(response)) => response,
            _ => {
                self.record_failure(&to);
                bail!("no answer from {to}");
            }
        };
        if let Some(observed) = response
            .get("ip")
            .and_then(Value::bytes)
            .and_then(decode_peer)
        {
            self.observe_ip(to, observed);
        }
        if response.get("y").and_then(Value::str) != Some("r") {
            bail!("{to} answered with an error");
        }
        let r = response.get("r").context("response has no body")?.clone();
        if let Some(id) = r
            .get("id")
            .and_then(Value::bytes)
            .and_then(|b| <NodeId>::try_from(b).ok())
        {
            if !super::dht_security::valid(to.ip(), &id) {
                bail!("DHT node ID does not match its address");
            }
            self.insert(id, to);
        } else {
            bail!("DHT reply has no valid node ID");
        }
        Ok(r)
    }

    // ---- client operations --------------------------------------------------------------

    /// Learns about a node (for instance from a BitTorrent `port` message).
    pub async fn ping(&self, addr: SocketAddr) -> bool {
        self.query(addr, "ping", vec![]).await.is_ok()
    }

    pub async fn add_nodes(&self, nodes: &[SocketAddr]) {
        for chunk in nodes.chunks(16) {
            let pings = chunk.iter().map(|addr| self.ping(*addr));
            futures_util::future::join_all(pings).await;
        }
    }

    async fn resolve_bootstrap(&self) -> Vec<SocketAddr> {
        let mut out = Vec::new();
        for entry in &self.bootstrap {
            if let Ok(found) = tokio::net::lookup_host(entry.as_str()).await {
                out.extend(found.filter(|a| a.is_ipv6() == self.ipv6));
            }
        }
        out
    }

    /// Fills the routing table: contact the bootstrap nodes (or saved ones), then look up our own id.
    pub async fn bootstrap(&self) {
        if self.node_count() == 0 {
            let seeds = self.resolve_bootstrap().await;
            self.add_nodes(&seeds).await;
        }
        for _ in 0..2 {
            let _ = self.find_node(self.id()).await;
        }
    }

    pub async fn find_node(&self, target: NodeId) -> Vec<SocketAddr> {
        self.iterate(target, false).await.0
    }

    /// Finds peers for `info_hash`.
    pub async fn get_peers(&self, info_hash: NodeId) -> Lookup {
        let (_, lookup) = self.iterate(info_hash, true).await;
        lookup
    }

    async fn iterate(&self, target: NodeId, peers: bool) -> (Vec<SocketAddr>, Lookup) {
        let mut shortlist: HashMap<SocketAddr, NodeId> = self
            .closest(&target, K * 2)
            .into_iter()
            .map(|n| (n.addr, n.id))
            .collect();
        if shortlist.is_empty() {
            for addr in self.resolve_bootstrap().await {
                shortlist.insert(addr, [0u8; 20]);
            }
        }
        let mut queried: HashSet<SocketAddr> = HashSet::new();
        let mut lookup = Lookup::default();
        let mut found_peers: HashSet<SocketAddr> = HashSet::new();
        for _ in 0..MAX_LOOKUP_ROUNDS {
            let mut candidates: Vec<(SocketAddr, NodeId)> = shortlist
                .iter()
                .filter(|(a, _)| !queried.contains(*a))
                .map(|(a, i)| (*a, *i))
                .collect();
            candidates.sort_by_key(|(_, id)| distance(id, &target));
            candidates.truncate(ALPHA);
            if candidates.is_empty() {
                break;
            }
            let calls = candidates.iter().map(|(addr, _)| {
                queried.insert(*addr);
                let args = if peers {
                    vec![("info_hash", Value::Bytes(target.to_vec()))]
                } else {
                    vec![("target", Value::Bytes(target.to_vec()))]
                };
                async move {
                    (
                        *addr,
                        self.query(*addr, if peers { "get_peers" } else { "find_node" }, args)
                            .await,
                    )
                }
            });
            let mut progressed = false;
            for (addr, outcome) in futures_util::future::join_all(calls).await {
                let Ok(r) = outcome else { continue };
                if let Some(token) = r.get("token").and_then(Value::bytes) {
                    lookup.tokens.push((addr, token.to_vec()));
                }
                if let Some(values) = r.get("values").and_then(Value::list) {
                    for value in values {
                        if let Some(peer) = value.bytes().and_then(decode_peer)
                            && self.address_ok(&peer)
                            && found_peers.insert(peer)
                        {
                            lookup.peers.push(peer);
                        }
                    }
                }
                if let Some(nodes) = r.get(self.nodes_key()).and_then(Value::bytes) {
                    for (id, node_addr) in decode_nodes(nodes, self.ipv6) {
                        if id != self.id()
                            && self.address_ok(&node_addr)
                            && super::dht_security::valid(node_addr.ip(), &id)
                            && shortlist.insert(node_addr, id).is_none()
                        {
                            progressed = true;
                        }
                    }
                }
            }
            if !progressed && peers && !lookup.peers.is_empty() {
                break;
            }
        }
        // Keep only the closest responders for the announce step.
        lookup.tokens.sort_by_key(|(addr, _)| {
            shortlist
                .get(addr)
                .map(|id| distance(id, &target))
                .unwrap_or([0xff; 20])
        });
        lookup.tokens.truncate(K);
        let mut nodes: Vec<(SocketAddr, NodeId)> = shortlist.into_iter().collect();
        nodes.sort_by_key(|(_, id)| distance(id, &target));
        (nodes.into_iter().take(K).map(|(a, _)| a).collect(), lookup)
    }

    /// Tells the nodes closest to `info_hash` that we serve it on `port`.
    pub async fn announce(
        &self,
        info_hash: NodeId,
        port: u16,
        tokens: &[(SocketAddr, Vec<u8>)],
    ) -> usize {
        let calls = tokens.iter().map(|(addr, token)| {
            self.query(
                *addr,
                "announce_peer",
                vec![
                    ("info_hash", Value::Bytes(info_hash.to_vec())),
                    ("port", Value::Int(port as i64)),
                    ("token", Value::Bytes(token.clone())),
                ],
            )
        });
        futures_util::future::join_all(calls)
            .await
            .into_iter()
            .filter(Result::is_ok)
            .count()
    }

    async fn maintain(&self) {
        // Re-learn the neighbourhood and drop dead nodes (failures are counted by `query`).
        let sample: Vec<SocketAddr> = {
            let state = self.state();
            state
                .buckets
                .iter()
                .flatten()
                .filter(|n| n.last_seen.elapsed() > Duration::from_secs(15 * 60))
                .map(|n| n.addr)
                .take(8)
                .collect()
        };
        for addr in sample {
            let _ = self.ping(addr).await;
        }
        if self.node_count() < K {
            self.bootstrap().await;
        } else {
            let _ = self.find_node(random_bytes()).await;
        }
        let mut state = self.state();
        state.store.retain(|_, peers| {
            peers.retain(|(_, at)| at.elapsed() < PEER_TTL);
            !peers.is_empty()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn public_address_needs_three_independent_network_votes() {
        let dht = Dht::start(DhtConfig {
            bind: "127.0.0.1".parse().unwrap(),
            bootstrap: vec![],
            ..Default::default()
        })
        .await
        .unwrap();
        let old = dht.id();
        let observed = "8.8.4.4:6881".parse().unwrap();
        for reporter in ["1.1.1.1:1", "1.1.1.2:2", "9.9.9.9:3"] {
            dht.observe_ip(reporter.parse().unwrap(), observed);
            assert_eq!(dht.id(), old);
        }
        dht.observe_ip("208.67.222.222:4".parse().unwrap(), observed);
        assert!(super::super::dht_security::valid(observed.ip(), &dht.id()));
        assert_ne!(dht.id(), old);
    }

    #[tokio::test]
    async fn announce_tokens_expire_even_without_new_get_peers_requests() {
        let dht = Dht::start(DhtConfig {
            bind: "127.0.0.1".parse().unwrap(),
            bootstrap: vec![],
            allow_loopback: true,
            ..Default::default()
        })
        .await
        .unwrap();
        let addr: SocketAddr = "127.0.0.1:6881".parse().unwrap();

        let token = dht.make_token(&addr);
        {
            let mut state = dht.state();
            state.rotated = Instant::now() - SECRET_LIFETIME - Duration::from_secs(1);
        }
        assert!(
            dht.token_valid(&addr, &token),
            "a token remains valid during the previous-secret window"
        );

        let token = dht.make_token(&addr);
        {
            let mut state = dht.state();
            state.rotated =
                Instant::now() - SECRET_LIFETIME - SECRET_LIFETIME - Duration::from_secs(1);
        }
        assert!(
            !dht.token_valid(&addr, &token),
            "a token must expire after two complete secret windows"
        );
    }

    #[test]
    fn buckets_follow_the_first_differing_bit() {
        let own = [0u8; 20];
        let mut other = [0u8; 20];
        assert_eq!(bucket_index(&own, &other), None);
        other[0] = 0x80;
        assert_eq!(bucket_index(&own, &other), Some(0));
        other[0] = 0x01;
        assert_eq!(bucket_index(&own, &other), Some(7));
        other = [0u8; 20];
        other[19] = 1;
        assert_eq!(bucket_index(&own, &other), Some(159));
    }

    #[test]
    fn compact_encodings_round_trip_and_reject_garbage() {
        let peer: SocketAddr = "10.1.2.3:6881".parse().unwrap();
        assert_eq!(decode_peer(&encode_peer(&peer).unwrap()), Some(peer));
        assert_eq!(decode_peer(&[1, 2, 3, 4, 0, 0]), None, "port zero");
        assert_eq!(decode_peer(&[1, 2, 3]), None);
        let node = Node {
            id: [7; 20],
            addr: peer,
            last_seen: Instant::now(),
            failures: 0,
        };
        let encoded = encode_nodes(&[node]);
        assert_eq!(encoded.len(), 26);
        assert_eq!(decode_nodes(&encoded, false), vec![([7; 20], peer)]);
        assert_eq!(
            decode_nodes(&encoded[..25], false),
            vec![],
            "truncated entries are ignored"
        );
        assert!(
            encode_peer(&"[::1]:80".parse().unwrap()).unwrap().len() == 18,
            "IPv6 compact peers have 18 bytes"
        );
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    #[tokio::test]
    async fn dropping_an_idle_dht_releases_its_udp_socket() {
        let dht = Dht::start(DhtConfig {
            bind: IpAddr::V4(Ipv4Addr::LOCALHOST),
            bootstrap: vec![],
            allow_loopback: true,
            ..DhtConfig::default()
        })
        .await
        .unwrap();
        let address = SocketAddr::from(([127, 0, 0, 1], dht.port()));
        drop(dht);
        timeout(Duration::from_secs(2), async {
            loop {
                if std::net::UdpSocket::bind(address).is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("an idle DHT retained its UDP port after drop");
    }
}
