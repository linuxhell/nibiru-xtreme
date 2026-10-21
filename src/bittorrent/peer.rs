//! A single peer connection: handshake, wire protocol, requesting, uploading, metadata and PEX.

use super::{
    bitfield::Bitfield,
    limiter::RateMeter,
    message::{
        ExtHandshake, Handshake, HashRequest, Holepunch, HolepunchError, LOCAL_UT_HOLEPUNCH,
        LOCAL_UT_METADATA, LOCAL_UT_PEX, MAX_BLOCK, METADATA_PIECE, Message, MetadataMessage, Pex,
        read_handshake, read_message, write_message,
    },
    metainfo::{Extras, Torrent},
    mse::{self, Stream},
    picker::BlockRef,
    torrent::{
        MAX_METADATA, MetadataFetch, PeerCmd, PeerEntry, PeerKey, Phase, PieceBuffer, Shared,
        State, complete_metadata, finish_piece, install_metadata, metadata_piece_count,
        pipeline_depth,
    },
};
use anyhow::{Result, bail};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::mpsc,
    time::timeout,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(8);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const IDLE_TIMEOUT: Duration = Duration::from_secs(180);
const KEEPALIVE: Duration = Duration::from_secs(90);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(45);
pub(crate) const MAX_UPLOAD_QUEUE: usize = 128;
const MAX_PEX_ADDED: usize = 50;
const MAX_HOLEPUNCH_HINTS: usize = 256;
/// Simultaneous hole-punch dials a relay may ask of us.
const MAX_HOLEPUNCH_DIALS: usize = 4;
const HOLEPUNCH_MIN_GAP: Duration = Duration::from_secs(2);

pub async fn connect(shared: Arc<Shared>, addr: SocketAddr) {
    let result = connect_inner(&shared, addr, false).await;
    let mut core = shared.lock();
    core.connecting.remove(&addr);
    if result.is_err() {
        // Let a later tracker/PEX answer offer this address again.
        core.known.remove(&addr);
        // Behind a NAT or firewall? A relay that told us this peer can hole punch may introduce us.
        if let Some(relay) = core.holepunch_hints.remove(&addr)
            && let Some(entry) = core.peers.get(&relay)
            && entry.holepunch
        {
            let _ = entry
                .tx
                .try_send(PeerCmd::Holepunch(Holepunch::Rendezvous(addr)));
        }
    }
}

/// Dials `addr` over uTP right now because a relay told both ends to connect at the same moment.
pub(crate) async fn holepunch_connect(shared: Arc<Shared>, addr: SocketAddr) {
    let result = connect_inner(&shared, addr, true).await;
    let mut core = shared.lock();
    core.holepunching.remove(&addr);
    core.connecting.remove(&addr);
    if result.is_err() {
        core.known.remove(&addr);
    }
}

async fn connect_inner(shared: &Arc<Shared>, addr: SocketAddr, only_utp: bool) -> Result<()> {
    let settings = shared.lock().settings.clone();
    let mut last_error = anyhow::anyhow!("no peer transport enabled");
    for use_utp in [false, true] {
        if (use_utp && (!settings.enable_utp || shared.utp.is_none()))
            || (!use_utp && (!settings.enable_tcp || only_utp))
        {
            continue;
        }
        for wire_hash in shared.wire_hashes() {
            for encrypted in [false, true] {
                if settings.require_encryption && !encrypted {
                    continue;
                }
                if shared.is_stopped() || shared.lock().paused {
                    return Ok(());
                }
                let result = timeout(
                    CONNECT_TIMEOUT + HANDSHAKE_TIMEOUT,
                    open_peer(
                        shared,
                        addr,
                        use_utp,
                        encrypted,
                        settings.require_encryption,
                        wire_hash,
                    ),
                )
                .await;
                match result {
                    Ok(Ok((stream, theirs))) => {
                        shared.lock().connecting.remove(&addr);
                        return run(shared.clone(), stream, addr, false, use_utp, theirs).await;
                    }
                    Ok(Err(e)) => last_error = e,
                    Err(e) => last_error = e.into(),
                }
            }
        }
    }
    Err(last_error)
}

async fn open_peer(
    shared: &Arc<Shared>,
    addr: SocketAddr,
    use_utp: bool,
    encrypted: bool,
    require_rc4: bool,
    wire_hash: [u8; 20],
) -> Result<(Stream, Handshake)> {
    shared.lock().record_dial(addr);
    let mut stream: Stream = if use_utp {
        Box::new(
            timeout(
                CONNECT_TIMEOUT,
                shared.utp.as_ref().expect("uTP checked").connect(addr),
            )
            .await??,
        )
    } else {
        let tcp = timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await??;
        tcp.set_nodelay(true).ok();
        Box::new(tcp)
    };
    if encrypted {
        stream = mse::outgoing(stream, wire_hash, require_rc4).await?;
    }
    let ours = Handshake::new(wire_hash, shared.peer_id, shared.dht_for(addr).is_some());
    stream.write_all(&ours.encode()).await?;
    let theirs = timeout(HANDSHAKE_TIMEOUT, read_handshake(&mut stream)).await??;
    if theirs.info_hash != wire_hash || theirs.peer_id == shared.peer_id {
        bail!("wrong torrent or connected to ourselves");
    }
    Ok((stream, theirs))
}

/// The listener already read (and matched) the remote handshake.
pub async fn accept(
    shared: Arc<Shared>,
    mut stream: Stream,
    addr: SocketAddr,
    theirs: Handshake,
    encrypted: bool,
    utp: bool,
) {
    let allowed = {
        let core = shared.lock();
        core.peer_address_ok(&addr)
            && (!core.settings.require_encryption || encrypted)
            && !core.paused
            && core.peers.len() < core.settings.max_peers + 10
            && !matches!(core.state, State::Error | State::Complete)
    };
    if !allowed || theirs.peer_id == shared.peer_id {
        return;
    }
    let ours = Handshake::new(
        theirs.info_hash,
        shared.peer_id,
        shared.dht_for(addr).is_some(),
    );
    if stream.write_all(&ours.encode()).await.is_err() {
        return;
    }
    let _ = run(shared, stream, addr, true, utp, theirs).await;
}

struct Peer {
    wire_hash: [u8; 20],
    shared: Arc<Shared>,
    key: PeerKey,
    addr: SocketAddr,
    writer: tokio::io::WriteHalf<Stream>,
    fast: bool,
    their_ext: Option<ExtHandshake>,
    /// Their pieces. Empty (len 0) until we know the piece count.
    theirs: Bitfield,
    early: EarlyState,
    peer_choking: bool,
    interested: bool,
    allowed_fast: Bitfield,
    inflight: VecDeque<(BlockRef, Instant)>,
    uploads: VecDeque<(u32, u32, u32)>,
    pex_sent: HashSet<SocketAddr>,
    meta_asked: HashSet<u32>,
    hash_requests: HashMap<HashRequest, Instant>,
    meter: RateMeter,
    last_sent: Instant,
    ext_hello_sent: bool,
    /// When this peer last asked us to relay or connect (BEP 55 requests are rate limited).
    last_holepunch: Option<Instant>,
    /// We told this peer what we have (needs the metadata first).
    announced: bool,
    dht_capable: bool,
}

/// Messages that arrive before we know how many pieces there are.
#[derive(Default)]
struct EarlyState {
    bitfield: Option<Vec<u8>>,
    have_all: bool,
    haves: Vec<u32>,
}

async fn run(
    shared: Arc<Shared>,
    stream: Stream,
    addr: SocketAddr,
    incoming: bool,
    utp: bool,
    theirs: Handshake,
) -> Result<()> {
    // Both ends of a hole punch dial each other at once, so one pair of peers can end up with two
    // connections. Each side keeps the one opened by the peer with the lower peer ID, which both
    // can compute, instead of each dropping the other's.
    let initiator_has_lower_id = if incoming {
        theirs.peer_id < shared.peer_id
    } else {
        shared.peer_id < theirs.peer_id
    };
    let supersedes = {
        let core = shared.lock();
        core.peers
            .values()
            .find(|p| p.addr == addr && p.incoming != incoming)
            .filter(|_| initiator_has_lower_id)
            .map(|p| p.tx.clone())
    };
    if let Some(old) = supersedes {
        let _ = old.try_send(PeerCmd::Shutdown);
        for _ in 0..40 {
            if !shared.lock().peers.values().any(|p| p.addr == addr) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let (read_half, writer) = tokio::io::split(stream);
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<PeerCmd>(64);
    let key = shared.next_key();
    {
        let mut core = shared.lock();
        if core.paused
            || shared.is_stopped()
            || core.peers.len() >= core.settings.max_peers + 10
            || core.peers.values().any(|p| p.addr == addr)
        {
            return Ok(());
        }
        let pieces = core.ready().map(|r| r.torrent.piece_count()).unwrap_or(0);
        core.peers.insert(
            key,
            PeerEntry {
                addr,
                tx: cmd_tx,
                incoming,
                client: None,
                bitfield: Bitfield::new(pieces),
                peer_choking: true,
                am_choking: true,
                their_interested: false,
                down: RateMeter::default(),
                up: RateMeter::default(),
                since: Instant::now(),
                utp,
                holepunch: false,
            },
        );
    }
    let (msg_tx, mut msg_rx) = mpsc::channel::<Message>(128);
    let reader = tokio::spawn(async move {
        let mut reader = BufReader::with_capacity(64 * 1024, read_half);
        loop {
            match timeout(IDLE_TIMEOUT, read_message(&mut reader)).await {
                Ok(Ok(Some(message))) => {
                    if msg_tx.send(message).await.is_err() {
                        break;
                    }
                }
                Ok(Ok(None)) => continue,
                _ => break,
            }
        }
    });
    let mut peer = Peer {
        wire_hash: theirs.info_hash,
        shared: shared.clone(),
        key,
        addr,
        writer,
        fast: theirs.supports_fast(),
        their_ext: None,
        theirs: Bitfield::new(0),
        early: EarlyState::default(),
        peer_choking: true,
        interested: false,
        allowed_fast: Bitfield::new(0),
        inflight: VecDeque::new(),
        uploads: VecDeque::new(),
        pex_sent: HashSet::new(),
        meta_asked: HashSet::new(),
        hash_requests: HashMap::new(),
        meter: RateMeter::default(),
        last_sent: Instant::now(),
        ext_hello_sent: false,
        last_holepunch: None,
        announced: false,
        dht_capable: theirs.supports_dht(),
    };
    let outcome = peer
        .drive(&mut msg_rx, &mut cmd_rx, theirs.supports_extensions())
        .await;
    reader.abort();
    // Cleanup: return blocks to the pool and leave the peer table.
    {
        let mut core = shared.lock();
        let entry = core.peers.remove(&key);
        core.known.remove(&addr);
        if let Some(r) = core.ready_mut() {
            r.picker.release_peer(key);
            if let Some(entry) = &entry {
                r.picker.peer_left(&entry.bitfield);
            }
        }
        if let Phase::Metadata(Some(fetch)) = &mut core.phase {
            fetch.requested.retain(|_, (owner, _)| *owner != key);
        }
    }
    shared.wake.notify_one();
    outcome
}

impl Peer {
    fn our_hello(&self, metadata_size: Option<u32>) -> ExtHandshake {
        let hello = ExtHandshake::ours(metadata_size, self.shared.listen_port);
        let utp = self.shared.utp.is_some() && self.shared.lock().settings.enable_utp;
        if utp { hello.with_holepunch() } else { hello }
    }

    async fn request_hashes(&mut self) -> Result<()> {
        self.hash_requests
            .retain(|_, at| at.elapsed() < Duration::from_secs(20));
        let requests = {
            let core = self.shared.lock();
            if core.paused {
                return Ok(());
            }
            let Some(ready) = core.ready() else {
                return Ok(());
            };
            let Some(v2) = &ready.torrent.v2 else {
                return Ok(());
            };
            let mut requests = vec![];
            for file in &v2.files {
                if file.pieces < 2 {
                    continue;
                }
                let length = file.pieces.next_power_of_two().clamp(2, 128);
                for index in (0..file.pieces).step_by(length) {
                    if self.hash_requests.len() + requests.len() >= 4 {
                        break;
                    }
                    let end = (index + length).min(file.pieces);
                    if (index..end).all(|i| ready.storage.v2_piece_known(file.first_piece + i)) {
                        continue;
                    }
                    let request = v2.request(file, index);
                    if !self.hash_requests.contains_key(&request) {
                        requests.push(request);
                    }
                }
                if self.hash_requests.len() + requests.len() >= 4 {
                    break;
                }
            }
            requests
        };
        for request in requests {
            self.hash_requests.insert(request, Instant::now());
            self.send(&Message::HashRequest(request)).await?;
        }
        Ok(())
    }

    async fn serve_hashes(&mut self, request: HashRequest) -> Result<()> {
        self.shared.hash_query_limit.acquire(1).await;
        let source = {
            let core = self.shared.lock();
            if core.paused {
                None
            } else {
                core.ready()
                    .map(|r| (r.storage.clone(), r.picker.have().clone()))
            }
        };
        let hashes = if let Some((storage, have)) = source {
            tokio::task::spawn_blocking(move || storage.hash_response(&request, &have))
                .await
                .ok()
                .and_then(Result::ok)
        } else {
            None
        };
        match hashes {
            Some(hashes) => self.send(&Message::Hashes { request, hashes }).await,
            None => self.send(&Message::HashReject(request)).await,
        }
    }

    async fn receive_hashes(&mut self, request: HashRequest, hashes: Vec<[u8; 32]>) -> Result<()> {
        if self.hash_requests.remove(&request).is_none() {
            return Ok(());
        }
        let storage = { self.shared.lock().ready().map(|r| r.storage.clone()) };
        let Some(storage) = storage else {
            return Ok(());
        };
        let added = storage.install_v2_proof(&request, &hashes)?;
        // Magnet resume learns the authenticated hashes first, then verifies
        // existing data before asking the swarm to send those pieces again.
        let check = storage.clone();
        let indices = added.clone();
        let verified = tokio::task::spawn_blocking(move || {
            indices
                .into_iter()
                .filter(|i| check.check_piece(*i))
                .collect::<Vec<_>>()
        })
        .await?;
        {
            let mut core = self.shared.lock();
            let complete = if let Some(r) = core.ready_mut() {
                for i in added {
                    r.picker.set_hash_available(i, true);
                }
                for i in verified {
                    r.picker.mark_have(i);
                }
                r.picker.is_complete() && r.storage.metadata_complete()
            } else {
                false
            };
            if complete {
                super::torrent::mark_complete(&mut core);
            }
        }
        self.shared.progress.notify_waiters();
        self.shared.wake.notify_one();
        self.update_interest().await?;
        self.fill_requests().await?;
        self.request_hashes().await
    }

    async fn send(&mut self, message: &Message) -> Result<()> {
        write_message(&mut self.writer, message).await?;
        self.last_sent = Instant::now();
        Ok(())
    }

    async fn drive(
        &mut self,
        messages: &mut mpsc::Receiver<Message>,
        commands: &mut mpsc::Receiver<PeerCmd>,
        extensions: bool,
    ) -> Result<()> {
        if extensions {
            let size = self
                .shared
                .lock()
                .ready()
                .map(|r| r.torrent.info_bytes.len() as u32);
            let hello = self.our_hello(size);
            self.send(&Message::Extended {
                id: 0,
                payload: hello.encode(),
            })
            .await?;
            self.ext_hello_sent = true;
        }
        if self.dht_capable
            && let Some(dht) = self.shared.dht_for(self.addr)
        {
            let port = dht.port();
            self.send(&Message::Port(port)).await?;
        }
        self.send_bitfield().await?;
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            if self.shared.is_stopped() {
                return Ok(());
            }
            tokio::select! {
                command = commands.recv() => {
                    let Some(command) = command else { return Ok(()) };
                    if !self.command(command).await? {
                        return Ok(());
                    }
                }
                message = messages.recv() => {
                    let Some(message) = message else { return Ok(()) };
                    self.message(message).await?;
                }
                _ = tick.tick() => {
                    self.on_tick().await?;
                }
                _ = std::future::ready(()), if !self.uploads.is_empty() => {
                    self.serve_one().await?;
                }
            }
        }
    }

    // ---- bookkeeping --------------------------------------------------------------------

    fn piece_count(&self) -> usize {
        self.shared
            .lock()
            .ready()
            .map(|r| r.torrent.piece_count())
            .unwrap_or(0)
    }

    async fn send_bitfield(&mut self) -> Result<()> {
        let message = {
            let core = self.shared.lock();
            let Some(r) = core.ready() else {
                return Ok(());
            };
            self.announced = true;
            let have = r.picker.have();
            if self.fast && have.count() == have.len() && !have.is_empty() {
                Some(Message::HaveAll)
            } else if self.fast && have.count() == 0 {
                Some(Message::HaveNone)
            } else if have.count() == 0 {
                None
            } else {
                Some(Message::Bitfield(have.as_bytes().to_vec()))
            }
        };
        if let Some(message) = message {
            self.send(&message).await?;
        }
        Ok(())
    }

    /// Converts messages received before the metadata existed, once the piece count is known.
    fn settle_early(&mut self) -> Result<()> {
        let count = self.piece_count();
        if count == 0 || self.theirs.len() == count {
            return Ok(());
        }
        self.theirs = Bitfield::new(count);
        self.allowed_fast = Bitfield::new(count);
        if self.early.have_all {
            self.theirs = Bitfield::full(count);
        } else if let Some(bytes) = self.early.bitfield.take() {
            self.theirs = Bitfield::from_wire(&bytes, count)?;
        }
        for piece in std::mem::take(&mut self.early.haves) {
            if (piece as usize) < count {
                self.theirs.set(piece as usize);
            }
        }
        let mut core = self.shared.lock();
        let key = self.key;
        let theirs = self.theirs.clone();
        if let Some(entry) = core.peers.get_mut(&key) {
            entry.bitfield = theirs.clone();
        }
        if let Some(r) = core.ready_mut() {
            r.picker.peer_joined(&theirs);
        }
        Ok(())
    }

    async fn update_interest(&mut self) -> Result<()> {
        let want = {
            let core = self.shared.lock();
            core.ready().is_some_and(|r| {
                self.theirs.len() == r.torrent.piece_count()
                    && self
                        .theirs
                        .iter_set()
                        .any(|p| r.picker.wanted().get(p) && !r.picker.have().get(p))
            })
        };
        if want != self.interested {
            self.interested = want;
            self.send(&if want {
                Message::Interested
            } else {
                Message::NotInterested
            })
            .await?;
        }
        Ok(())
    }

    async fn fill_requests(&mut self) -> Result<()> {
        if !self.interested || self.theirs.is_empty() {
            return Ok(());
        }
        if self.peer_choking && !self.fast {
            return Ok(());
        }
        let blocks = {
            let mut core = self.shared.lock();
            if core.paused || core.state != State::Downloading {
                return Ok(());
            }
            let rate = self.meter.rate();
            let depth = pipeline_depth(rate);
            let mut out = Vec::new();
            let Some(r) = core.ready_mut() else {
                return Ok(());
            };
            while self.inflight.len() + out.len() < depth {
                let allowed = if self.peer_choking {
                    &self.allowed_fast
                } else {
                    &r.full
                };
                if self.peer_choking && allowed.count() == 0 {
                    break;
                }
                match r.picker.next_block(self.key, &self.theirs, allowed) {
                    Some(block) => out.push(block),
                    None => break,
                }
            }
            out
        };
        for block in blocks {
            self.inflight.push_back((block, Instant::now()));
            self.send(&Message::Request {
                index: block.piece as u32,
                begin: block.offset,
                length: block.length,
            })
            .await?;
        }
        Ok(())
    }

    async fn on_tick(&mut self) -> Result<()> {
        self.meter.tick();
        // Requests that never came back: hand them to other peers.
        let timed_out = self
            .inflight
            .iter()
            .filter(|(_, at)| at.elapsed() > REQUEST_TIMEOUT)
            .count();
        if timed_out > 0 {
            let expired: Vec<BlockRef> = self
                .inflight
                .iter()
                .filter(|(_, at)| at.elapsed() > REQUEST_TIMEOUT)
                .map(|(b, _)| *b)
                .collect();
            self.inflight
                .retain(|(_, at)| at.elapsed() <= REQUEST_TIMEOUT);
            let mut core = self.shared.lock();
            if let Some(r) = core.ready_mut() {
                for block in expired {
                    r.picker.release_block(self.key, block.piece, block.offset);
                }
            }
        }
        if self.last_sent.elapsed() >= KEEPALIVE {
            self.send(&Message::KeepAlive).await?;
        }
        self.request_metadata().await?;
        self.request_hashes().await?;
        self.fill_requests().await?;
        Ok(())
    }

    // ---- commands from the torrent ------------------------------------------------------

    async fn command(&mut self, command: PeerCmd) -> Result<bool> {
        match command {
            PeerCmd::Shutdown => return Ok(false),
            PeerCmd::Choke => {
                self.send(&Message::Choke).await?;
                if self.fast {
                    for (index, begin, length) in std::mem::take(&mut self.uploads) {
                        self.send(&Message::Reject {
                            index,
                            begin,
                            length,
                        })
                        .await?;
                    }
                } else {
                    self.uploads.clear();
                }
            }
            PeerCmd::Unchoke => self.send(&Message::Unchoke).await?,
            PeerCmd::Have(piece) => {
                if self.theirs.len() > piece as usize && !self.theirs.get(piece as usize) {
                    self.send(&Message::Have(piece)).await?;
                }
                self.update_interest().await?;
            }
            PeerCmd::Ready => {
                if !self.announced {
                    if self.ext_hello_sent {
                        // Our metadata size is known now; tell the peer so it can fetch from us.
                        let size = self
                            .shared
                            .lock()
                            .ready()
                            .map(|r| r.torrent.info_bytes.len() as u32);
                        let hello = self.our_hello(size);
                        self.send(&Message::Extended {
                            id: 0,
                            payload: hello.encode(),
                        })
                        .await?;
                    }
                    self.send_bitfield().await?;
                }
                self.settle_early()?;
                self.update_interest().await?;
                self.request_hashes().await?;
                self.fill_requests().await?;
            }
            PeerCmd::Cancel {
                index,
                begin,
                length,
            } => {
                if let Some(pos) = self.inflight.iter().position(|(b, _)| {
                    b.piece == index as usize && b.offset == begin && b.length == length
                }) {
                    self.inflight.remove(pos);
                    self.send(&Message::Cancel {
                        index,
                        begin,
                        length,
                    })
                    .await?;
                }
            }
            PeerCmd::Pex(all) => self.send_pex(&all).await?,
            PeerCmd::Holepunch(message) => {
                if let Some(id) = self
                    .their_ext
                    .as_ref()
                    .and_then(|e| e.extensions.get("ut_holepunch"))
                    .copied()
                {
                    self.send(&Message::Extended {
                        id,
                        payload: message.encode(),
                    })
                    .await?;
                }
            }
        }
        Ok(true)
    }

    async fn send_pex(&mut self, all: &[SocketAddr]) -> Result<()> {
        let Some(id) = self
            .their_ext
            .as_ref()
            .and_then(|e| e.extensions.get("ut_pex"))
            .copied()
        else {
            return Ok(());
        };
        let current: HashSet<SocketAddr> =
            all.iter().copied().filter(|a| *a != self.addr).collect();
        let added: Vec<SocketAddr> = current
            .difference(&self.pex_sent)
            .copied()
            .take(MAX_PEX_ADDED)
            .collect();
        let dropped: Vec<SocketAddr> = self
            .pex_sent
            .difference(&current)
            .copied()
            .take(MAX_PEX_ADDED)
            .collect();
        if added.is_empty() && dropped.is_empty() {
            return Ok(());
        }
        for a in &added {
            self.pex_sent.insert(*a);
        }
        for a in &dropped {
            self.pex_sent.remove(a);
        }
        // Flag the peers that can take part in hole punching, so a receiver knows whom to ask us for.
        let holepunch = {
            let core = self.shared.lock();
            added
                .iter()
                .filter(|a| {
                    core.peers
                        .values()
                        .any(|p| p.addr == **a && p.holepunch && p.utp)
                })
                .copied()
                .collect()
        };
        let payload = Pex {
            added,
            dropped,
            holepunch,
        }
        .encode();
        self.send(&Message::Extended { id, payload }).await
    }

    // ---- messages from the peer ---------------------------------------------------------

    async fn message(&mut self, message: Message) -> Result<()> {
        match message {
            Message::HashRequest(request) => self.serve_hashes(request).await?,
            Message::Hashes { request, hashes } => self.receive_hashes(request, hashes).await?,
            Message::HashReject(request) => {
                if let Some(at) = self.hash_requests.get_mut(&request) {
                    *at = Instant::now();
                }
            }
            Message::KeepAlive | Message::Suggest(_) => {}
            Message::Port(port) => {
                // The peer runs a DHT node: ping it so it can enter our routing table.
                if let Some(dht) = self.shared.dht_for(self.addr)
                    && port != 0
                    && !self.shared.lock().private
                {
                    let node = SocketAddr::new(self.addr.ip(), port);
                    tokio::spawn(async move {
                        dht.ping(node).await;
                    });
                }
            }
            Message::Choke => {
                self.peer_choking = true;
                if !self.fast {
                    // Without the fast extension a choke silently drops our requests.
                    self.drop_inflight();
                }
                if let Some(entry) = self.shared.lock().peers.get_mut(&self.key) {
                    entry.peer_choking = true;
                }
            }
            Message::Unchoke => {
                self.peer_choking = false;
                if let Some(entry) = self.shared.lock().peers.get_mut(&self.key) {
                    entry.peer_choking = false;
                }
                self.fill_requests().await?;
            }
            Message::Interested => {
                if let Some(entry) = self.shared.lock().peers.get_mut(&self.key) {
                    entry.their_interested = true;
                }
                // A seeder with free slots answers immediately instead of waiting for the choker.
                let unchoke_now = {
                    let mut core = self.shared.lock();
                    let unchoked = core.peers.values().filter(|p| !p.am_choking).count();
                    if unchoked < 4 {
                        if let Some(entry) = core.peers.get_mut(&self.key) {
                            entry.am_choking = false;
                        }
                        true
                    } else {
                        false
                    }
                };
                if unchoke_now {
                    self.send(&Message::Unchoke).await?;
                }
            }
            Message::NotInterested => {
                if let Some(entry) = self.shared.lock().peers.get_mut(&self.key) {
                    entry.their_interested = false;
                }
            }
            Message::Have(piece) => {
                self.settle_early()?;
                if self.theirs.is_empty() {
                    if self.early.haves.len() < 100_000 {
                        self.early.haves.push(piece);
                    }
                } else {
                    if piece as usize >= self.theirs.len() {
                        bail!("have for a piece that does not exist");
                    }
                    if !self.theirs.get(piece as usize) {
                        self.theirs.set(piece as usize);
                        let mut core = self.shared.lock();
                        if let Some(entry) = core.peers.get_mut(&self.key) {
                            entry.bitfield.set(piece as usize);
                        }
                        if let Some(r) = core.ready_mut() {
                            r.picker.peer_has(piece as usize);
                        }
                    }
                    self.update_interest().await?;
                    self.fill_requests().await?;
                }
            }
            Message::Bitfield(bytes) => {
                self.settle_early()?;
                if self.theirs.is_empty() {
                    // Metadata is not known yet; applied when it arrives.
                    self.early.bitfield = Some(bytes);
                } else {
                    // Some clients (aria2) announce a bitfield again after the first pieces, so a
                    // later one simply replaces what we knew.
                    let fresh = Bitfield::from_wire(&bytes, self.theirs.len())?;
                    let old = std::mem::replace(&mut self.theirs, fresh.clone());
                    let mut core = self.shared.lock();
                    if let Some(entry) = core.peers.get_mut(&self.key) {
                        entry.bitfield = fresh.clone();
                    }
                    if let Some(r) = core.ready_mut() {
                        r.picker.peer_left(&old);
                        r.picker.peer_joined(&fresh);
                    }
                }
                self.update_interest().await?;
                self.fill_requests().await?;
            }
            Message::HaveAll => {
                self.early.have_all = true;
                self.settle_early()?;
                self.update_interest().await?;
                self.fill_requests().await?;
            }
            Message::HaveNone => {
                self.settle_early()?;
            }
            Message::AllowedFast(piece) => {
                self.settle_early()?;
                if (piece as usize) < self.allowed_fast.len() {
                    self.allowed_fast.set(piece as usize);
                }
            }
            Message::Request {
                index,
                begin,
                length,
            } => {
                if length == 0 || length as usize > MAX_BLOCK {
                    bail!("request for an unreasonable block size");
                }
                if self.uploads.len() >= MAX_UPLOAD_QUEUE {
                    bail!("request queue overflow");
                }
                let am_choking = self
                    .shared
                    .lock()
                    .peers
                    .get(&self.key)
                    .map(|p| p.am_choking)
                    .unwrap_or(true);
                if am_choking {
                    if self.fast {
                        self.send(&Message::Reject {
                            index,
                            begin,
                            length,
                        })
                        .await?;
                    }
                } else {
                    self.uploads.push_back((index, begin, length));
                }
            }
            Message::Cancel {
                index,
                begin,
                length,
            } => {
                self.uploads.retain(|u| *u != (index, begin, length));
            }
            Message::Piece { index, begin, data } => self.on_piece(index, begin, data).await?,
            Message::Reject {
                index,
                begin,
                length,
            } => {
                if let Some(pos) = self.inflight.iter().position(|(b, _)| {
                    b.piece == index as usize && b.offset == begin && b.length == length
                }) {
                    self.inflight.remove(pos);
                    let mut core = self.shared.lock();
                    if let Some(r) = core.ready_mut() {
                        r.picker.release_block(self.key, index as usize, begin);
                    }
                }
            }
            Message::Extended { id, payload } => self.on_extended(id, payload).await?,
        }
        Ok(())
    }

    fn drop_inflight(&mut self) {
        self.inflight.clear();
        let mut core = self.shared.lock();
        if let Some(r) = core.ready_mut() {
            r.picker.release_peer(self.key);
        }
    }

    async fn on_piece(&mut self, index: u32, begin: u32, data: Vec<u8>) -> Result<()> {
        let Some(pos) = self.inflight.iter().position(|(b, _)| {
            b.piece == index as usize && b.offset == begin && b.length as usize == data.len()
        }) else {
            // Late or unsolicited (we may have cancelled it in endgame).
            self.shared.lock().wasted += data.len() as u64;
            return Ok(());
        };
        self.inflight.remove(pos);
        let length = data.len() as u64;
        self.shared.down_limit.acquire(length).await;
        self.shared.own_down.acquire(length).await;
        self.meter.add(length);
        let mut finished = None;
        {
            let mut core = self.shared.lock();
            core.down.add(length);
            if let Some(entry) = core.peers.get_mut(&self.key) {
                entry.down.add(length);
            }
            let key = self.key;
            let piece = index as usize;
            let accepted = match core.ready_mut() {
                Some(r) => r.picker.block_received(key, piece, begin),
                None => None,
            };
            match accepted {
                None => core.wasted += length,
                Some(others) => {
                    let cancels = others;
                    core.contributors.entry(piece).or_default().push(key);
                    let r = core.ready_mut().expect("ready checked above");
                    let piece_len = r.picker.piece_len(piece) as usize;
                    let buffer = r.buffers.entry(piece).or_insert_with(|| PieceBuffer {
                        data: vec![0; piece_len],
                        received: 0,
                    });
                    let start = begin as usize;
                    if start + data.len() <= buffer.data.len() {
                        buffer.data[start..start + data.len()].copy_from_slice(&data);
                        buffer.received += data.len();
                    }
                    if r.picker.piece_has_all_blocks(piece)
                        && let Some(buffer) = r.buffers.remove(&piece)
                    {
                        finished = Some(buffer.data);
                    }
                    for other in &cancels {
                        if let Some(peer) = core.peers.get(other) {
                            let _ = peer.tx.try_send(PeerCmd::Cancel {
                                index,
                                begin,
                                length: data.len() as u32,
                            });
                        }
                    }
                }
            }
        }
        if let Some(buffer) = finished {
            finish_piece(&self.shared, index as usize, buffer).await;
        }
        self.fill_requests().await
    }

    // ---- uploading ----------------------------------------------------------------------

    async fn serve_one(&mut self) -> Result<()> {
        let Some((index, begin, length)) = self.uploads.pop_front() else {
            return Ok(());
        };
        let storage = {
            let core = self.shared.lock();
            let Some(r) = core.ready() else {
                return Ok(());
            };
            let piece = index as usize;
            let piece_len = if r.torrent.v1_info_hash == Some(self.wire_hash) {
                r.torrent
                    .total_length
                    .saturating_sub(piece as u64 * r.torrent.piece_length)
                    .min(r.torrent.piece_length)
            } else {
                r.picker.piece_len(piece)
            };
            if piece >= r.torrent.piece_count()
                || !r.picker.have().get(piece)
                || begin as u64 + length as u64 > piece_len
            {
                None
            } else {
                Some((
                    r.storage.clone(),
                    piece as u64 * r.torrent.piece_length + begin as u64,
                ))
            }
        };
        let Some((storage, offset)) = storage else {
            if self.fast {
                self.send(&Message::Reject {
                    index,
                    begin,
                    length,
                })
                .await?;
            }
            return Ok(());
        };
        let data = tokio::task::spawn_blocking(move || storage.read(offset, length as u64)).await?;
        let Ok(data) = data else {
            if self.fast {
                self.send(&Message::Reject {
                    index,
                    begin,
                    length,
                })
                .await?;
            }
            return Ok(());
        };
        self.shared.up_limit.acquire(length as u64).await;
        self.shared.own_up.acquire(length as u64).await;
        self.send(&Message::Piece { index, begin, data }).await?;
        let mut core = self.shared.lock();
        core.up.add(length as u64);
        if let Some(entry) = core.peers.get_mut(&self.key) {
            entry.up.add(length as u64);
        }
        Ok(())
    }

    // ---- extensions: handshake, metadata, PEX -------------------------------------------

    async fn on_extended(&mut self, id: u8, payload: Vec<u8>) -> Result<()> {
        match id {
            0 => {
                let hello = ExtHandshake::decode(&payload)?;
                if let Some(entry) = self.shared.lock().peers.get_mut(&self.key) {
                    entry.client = hello.client.clone();
                    entry.holepunch = hello.extensions.contains_key("ut_holepunch");
                }
                // BEP 10: a peer that can listen tells us its port; remember it for PEX/reconnects.
                self.their_ext = Some(hello.clone());
                if let Some(size) = hello.metadata_size {
                    self.learn_metadata_size(size as usize);
                }
                self.request_metadata().await?;
                // Tell a new peer who else is here right away (BEP 11), not a minute later.
                let share = {
                    let core = self.shared.lock();
                    (core.settings.enable_pex && !core.private)
                        .then(|| core.peers.values().map(|p| p.addr).collect::<Vec<_>>())
                };
                if let Some(all) = share {
                    self.send_pex(&all).await?;
                }
            }
            LOCAL_UT_METADATA => self.on_metadata(&payload).await?,
            LOCAL_UT_HOLEPUNCH => self.on_holepunch(&payload).await?,
            LOCAL_UT_PEX => {
                let (enabled, private) = {
                    let core = self.shared.lock();
                    (core.settings.enable_pex, core.private)
                };
                if enabled && !private {
                    let pex = Pex::decode(&payload)?;
                    let mut core = self.shared.lock();
                    for addr in pex.added {
                        core.add_candidate(addr);
                    }
                    if core.settings.enable_utp {
                        for addr in pex.holepunch {
                            if core.holepunch_hints.len() >= MAX_HOLEPUNCH_HINTS {
                                break;
                            }
                            core.holepunch_hints.entry(addr).or_insert(self.key);
                        }
                    }
                    drop(core);
                    self.shared.wake.notify_one();
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// BEP 55. As a relay we introduce two of our peers; as an endpoint we dial the address a relay
    /// gives us at the same moment the other side does, so both NATs open a mapping.
    async fn on_holepunch(&mut self, payload: &[u8]) -> Result<()> {
        let message = Holepunch::decode(payload)?;
        if self
            .last_holepunch
            .is_some_and(|at| at.elapsed() < HOLEPUNCH_MIN_GAP)
        {
            return Ok(()); // a peer cannot make us relay or dial in a tight loop
        }
        match message {
            Holepunch::Rendezvous(target) => {
                self.last_holepunch = Some(Instant::now());
                let verdict = {
                    let core = self.shared.lock();
                    if target == self.addr {
                        Err(HolepunchError::NoSelf)
                    } else {
                        match core.peers.values().find(|p| p.addr == target) {
                            None => Err(HolepunchError::NoSuchPeer),
                            Some(p) if !p.holepunch || !p.utp => Err(HolepunchError::NoSupport),
                            Some(p) => Ok(p.tx.clone()),
                        }
                    }
                };
                match verdict {
                    Ok(target_tx) => {
                        // Both ends get the other's address; only addresses of peers that are
                        // connected to us are ever handed out, so this cannot aim traffic elsewhere.
                        let introduced = target_tx
                            .try_send(PeerCmd::Holepunch(Holepunch::Connect(self.addr)))
                            .is_ok();
                        let reply = if introduced {
                            Holepunch::Connect(target)
                        } else {
                            Holepunch::Error(target, HolepunchError::NotConnected)
                        };
                        self.send_holepunch(&reply).await?;
                    }
                    Err(error) => {
                        self.send_holepunch(&Holepunch::Error(target, error))
                            .await?;
                    }
                }
            }
            Holepunch::Connect(target) => {
                self.last_holepunch = Some(Instant::now());
                let allowed = {
                    let mut core = self.shared.lock();
                    let usable = core.settings.enable_utp
                        && self.shared.utp.is_some()
                        && !core.paused
                        && target.port() != 0
                        && !target.ip().is_unspecified()
                        && !target.ip().is_multicast()
                        && core.peer_address_ok(&target)
                        && !core.peers.values().any(|p| p.addr == target)
                        && !core.connecting.contains(&target)
                        && core.holepunching.len() < MAX_HOLEPUNCH_DIALS;
                    if usable {
                        core.holepunching.insert(target);
                        core.connecting.insert(target);
                    }
                    usable
                };
                if allowed {
                    self.shared.lock().holepunch_queue.push_back(target);
                    self.shared.wake.notify_one();
                }
            }
            // The relay could not help; the normal connect path already failed, nothing else to do.
            Holepunch::Error(..) => {}
        }
        Ok(())
    }

    async fn send_holepunch(&mut self, message: &Holepunch) -> Result<()> {
        let Some(id) = self
            .their_ext
            .as_ref()
            .and_then(|e| e.extensions.get("ut_holepunch"))
            .copied()
        else {
            return Ok(());
        };
        self.send(&Message::Extended {
            id,
            payload: message.encode(),
        })
        .await
    }

    fn learn_metadata_size(&mut self, size: usize) {
        let mut core = self.shared.lock();
        if let Phase::Metadata(slot) = &mut core.phase
            && slot.is_none()
            && size > 0
            && size <= MAX_METADATA
        {
            *slot = Some(MetadataFetch {
                size,
                pieces: vec![None; metadata_piece_count(size)],
                requested: Default::default(),
                contributors: Default::default(),
            });
        }
    }

    async fn request_metadata(&mut self) -> Result<()> {
        let Some(id) = self
            .their_ext
            .as_ref()
            .and_then(|e| e.extensions.get("ut_metadata"))
            .copied()
        else {
            return Ok(());
        };
        let wanted: Vec<u32> = {
            let mut core = self.shared.lock();
            let key = self.key;
            let Phase::Metadata(Some(fetch)) = &mut core.phase else {
                return Ok(());
            };
            let mut out = Vec::new();
            for piece in 0..fetch.pieces.len() as u32 {
                if fetch.pieces[piece as usize].is_some() {
                    continue;
                }
                let stale = fetch
                    .requested
                    .get(&piece)
                    .is_none_or(|(_, at)| at.elapsed() > Duration::from_secs(15));
                if stale && out.len() + self.meta_asked.len() < 4 {
                    fetch.requested.insert(piece, (key, Instant::now()));
                    out.push(piece);
                }
            }
            out
        };
        for piece in wanted {
            self.meta_asked.insert(piece);
            self.send(&Message::Extended {
                id,
                payload: MetadataMessage::Request { piece }.encode(),
            })
            .await?;
        }
        Ok(())
    }

    async fn on_metadata(&mut self, payload: &[u8]) -> Result<()> {
        let Some(id) = self
            .their_ext
            .as_ref()
            .and_then(|e| e.extensions.get("ut_metadata"))
            .copied()
        else {
            return Ok(());
        };
        match MetadataMessage::decode(payload)? {
            MetadataMessage::Request { piece } => {
                let reply = {
                    let core = self.shared.lock();
                    core.ready().and_then(|r| {
                        let bytes = &r.torrent.info_bytes;
                        let start = piece as usize * METADATA_PIECE;
                        (start < bytes.len()).then(|| MetadataMessage::Data {
                            piece,
                            total_size: bytes.len() as u32,
                            data: bytes[start..(start + METADATA_PIECE).min(bytes.len())].to_vec(),
                        })
                    })
                };
                let reply = reply.unwrap_or(MetadataMessage::Reject { piece });
                self.send(&Message::Extended {
                    id,
                    payload: reply.encode(),
                })
                .await?;
            }
            MetadataMessage::Reject { piece } => {
                self.meta_asked.remove(&piece);
                let mut core = self.shared.lock();
                if let Phase::Metadata(Some(fetch)) = &mut core.phase {
                    fetch.requested.remove(&piece);
                }
            }
            MetadataMessage::Data {
                piece,
                total_size,
                data,
            } => {
                if !self.meta_asked.remove(&piece) {
                    return Ok(());
                }
                let done = {
                    let mut core = self.shared.lock();
                    let key = self.key;
                    let Phase::Metadata(Some(fetch)) = &mut core.phase else {
                        return Ok(());
                    };
                    if total_size as usize != fetch.size || piece as usize >= fetch.pieces.len() {
                        bail!("inconsistent metadata size");
                    }
                    let expected = if piece as usize + 1 == fetch.pieces.len() {
                        fetch.size - piece as usize * METADATA_PIECE
                    } else {
                        METADATA_PIECE
                    };
                    if data.len() != expected {
                        bail!("metadata piece has the wrong length");
                    }
                    fetch.pieces[piece as usize] = Some(data);
                    fetch.contributors.insert(key);
                    fetch.pieces.iter().all(Option::is_some)
                };
                if done && let Some(info) = complete_metadata(&self.shared) {
                    let extras = self.shared.lock().extras.clone();
                    match Torrent::from_info(&info, Extras { ..extras }) {
                        Ok(torrent) => install_metadata(&self.shared, torrent).await,
                        Err(_) => {
                            self.shared.lock().phase = Phase::Metadata(None);
                        }
                    }
                } else if done {
                    // Hash mismatch: the fetch was reset; the next tick starts over.
                }
            }
        }
        Ok(())
    }
}
