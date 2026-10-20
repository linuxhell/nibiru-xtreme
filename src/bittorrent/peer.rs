//! A single peer connection: handshake, wire protocol, requesting, uploading, metadata and PEX.

use super::{
    bitfield::Bitfield,
    limiter::RateMeter,
    message::{
        ExtHandshake, Handshake, LOCAL_UT_METADATA, LOCAL_UT_PEX, MAX_BLOCK, METADATA_PIECE,
        Message, MetadataMessage, Pex, read_handshake, read_message, write_message,
    },
    metainfo::{Extras, Torrent},
    picker::BlockRef,
    torrent::{
        MAX_METADATA, MetadataFetch, PeerCmd, PeerEntry, PeerKey, Phase, PieceBuffer, Shared,
        State, complete_metadata, finish_piece, install_metadata, metadata_piece_count,
        pipeline_depth,
    },
};
use anyhow::{Result, bail};
use std::{
    collections::{HashSet, VecDeque},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpStream, tcp::OwnedWriteHalf},
    sync::mpsc,
    time::timeout,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(8);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const IDLE_TIMEOUT: Duration = Duration::from_secs(180);
const KEEPALIVE: Duration = Duration::from_secs(90);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(45);
const MAX_UPLOAD_QUEUE: usize = 128;
const MAX_PEX_ADDED: usize = 50;

pub async fn connect(shared: Arc<Shared>, addr: SocketAddr) {
    let result = connect_inner(&shared, addr).await;
    let mut core = shared.lock();
    core.connecting.remove(&addr);
    if result.is_err() {
        // Let a later tracker/PEX answer offer this address again.
        core.known.remove(&addr);
    }
}

async fn connect_inner(shared: &Arc<Shared>, addr: SocketAddr) -> Result<()> {
    let mut stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await??;
    stream.set_nodelay(true).ok();
    let ours = Handshake::new(shared.info_hash, shared.peer_id, shared.dht.is_some());
    stream.write_all(&ours.encode()).await?;
    let theirs = timeout(HANDSHAKE_TIMEOUT, read_handshake(&mut stream)).await??;
    if theirs.info_hash != shared.info_hash || theirs.peer_id == shared.peer_id {
        bail!("wrong torrent or connected to ourselves");
    }
    shared.lock().connecting.remove(&addr);
    run(shared.clone(), stream, addr, false, theirs).await
}

/// The listener already read (and matched) the remote handshake.
pub async fn accept(
    shared: Arc<Shared>,
    mut stream: TcpStream,
    addr: SocketAddr,
    theirs: Handshake,
) {
    let allowed = {
        let core = shared.lock();
        core.peer_address_ok(&addr)
            && !core.paused
            && core.peers.len() < core.settings.max_peers + 10
            && !matches!(core.state, State::Error | State::Complete)
    };
    if !allowed || theirs.peer_id == shared.peer_id {
        return;
    }
    stream.set_nodelay(true).ok();
    let ours = Handshake::new(shared.info_hash, shared.peer_id, shared.dht.is_some());
    if stream.write_all(&ours.encode()).await.is_err() {
        return;
    }
    let _ = run(shared, stream, addr, true, theirs).await;
}

struct Peer {
    shared: Arc<Shared>,
    key: PeerKey,
    addr: SocketAddr,
    writer: OwnedWriteHalf,
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
    meter: RateMeter,
    last_sent: Instant,
    ext_hello_sent: bool,
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
    stream: TcpStream,
    addr: SocketAddr,
    incoming: bool,
    theirs: Handshake,
) -> Result<()> {
    let (read_half, writer) = stream.into_split();
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<PeerCmd>(64);
    let key = shared.next_key();
    {
        let mut core = shared.lock();
        if core.peers.len() >= core.settings.max_peers + 10
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
        meter: RateMeter::default(),
        last_sent: Instant::now(),
        ext_hello_sent: false,
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
            let hello = ExtHandshake::ours(size, self.shared.listen_port);
            self.send(&Message::Extended {
                id: 0,
                payload: hello.encode(),
            })
            .await?;
            self.ext_hello_sent = true;
        }
        if self.dht_capable
            && let Some(dht) = &self.shared.dht
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
                biased;
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
                        let hello = ExtHandshake::ours(size, self.shared.listen_port);
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
        let payload = Pex { added, dropped }.encode();
        self.send(&Message::Extended { id, payload }).await
    }

    // ---- messages from the peer ---------------------------------------------------------

    async fn message(&mut self, message: Message) -> Result<()> {
        match message {
            Message::KeepAlive | Message::Suggest(_) => {}
            Message::Port(port) => {
                // The peer runs a DHT node: ping it so it can enter our routing table.
                if let Some(dht) = self.shared.dht.clone()
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
            if piece >= r.torrent.piece_count()
                || !r.picker.have().get(piece)
                || begin as u64 + length as u64 > r.picker.piece_len(piece)
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
                }
                // BEP 10: a peer that can listen tells us its port; remember it for PEX/reconnects.
                self.their_ext = Some(hello.clone());
                if let Some(size) = hello.metadata_size {
                    self.learn_metadata_size(size as usize);
                }
                self.request_metadata().await?;
            }
            LOCAL_UT_METADATA => self.on_metadata(&payload).await?,
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
                    drop(core);
                    self.shared.wake.notify_one();
                }
            }
            _ => {}
        }
        Ok(())
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
