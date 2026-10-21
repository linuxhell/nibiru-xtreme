use super::kad_session::KadSession;
use super::obfuscation::{self, Obfuscation, PeerStream};
use super::{FileLink, FileSource, ServerConnection, ShareRegistry, UploadManager, peer, upload};
use anyhow::{Context, Result, bail};
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinSet,
};

/// How long to wait for a firewalled source to call back after one callback request.
const CALLBACK_WAIT: Duration = Duration::from_secs(15);

pub(crate) struct IncomingPeer {
    stream: PeerStream,
    large: bool,
}
type PendingCallback = (u64, oneshot::Sender<IncomingPeer>);
#[derive(Clone, Default)]
pub struct CallbackRegistry {
    /// Server callbacks wait for a Low ID...
    pending: Arc<Mutex<HashMap<u32, PendingCallback>>>,
    /// ...Kad callbacks for the user hash the source published.
    pending_hash: Arc<Mutex<HashMap<[u8; 16], PendingCallback>>>,
    revision: Arc<AtomicU64>,
}
enum WaitKey {
    Id(u32),
    Hash([u8; 16]),
}
struct CallbackWaiter {
    key: WaitKey,
    revision: u64,
    registry: CallbackRegistry,
    response: oneshot::Receiver<IncomingPeer>,
}
impl Drop for CallbackWaiter {
    fn drop(&mut self) {
        match &self.key {
            WaitKey::Id(id) => {
                if let Ok(mut pending) = self.registry.pending.lock()
                    && pending
                        .get(id)
                        .is_some_and(|(revision, _)| *revision == self.revision)
                {
                    pending.remove(id);
                }
            }
            WaitKey::Hash(hash) => {
                if let Ok(mut pending) = self.registry.pending_hash.lock()
                    && pending
                        .get(hash)
                        .is_some_and(|(revision, _)| *revision == self.revision)
                {
                    pending.remove(hash);
                }
            }
        }
    }
}
impl CallbackRegistry {
    fn register(&self, id: u32) -> Result<CallbackWaiter> {
        if id == 0 || id >= 0x0100_0000 {
            bail!("callback source must have a Low ID");
        }
        let mut pending = self.pending.lock().map_err(|e| anyhow::anyhow!("{e}"))?;
        if pending.len() >= 32 || pending.contains_key(&id) {
            bail!("ED2K callback already pending or callback limit reached");
        }
        let revision = self.revision.fetch_add(1, Ordering::AcqRel);
        let (sender, response) = oneshot::channel();
        pending.insert(id, (revision, sender));
        Ok(CallbackWaiter {
            key: WaitKey::Id(id),
            revision,
            registry: self.clone(),
            response,
        })
    }
    fn register_hash(&self, hash: [u8; 16]) -> Result<CallbackWaiter> {
        let mut pending = self
            .pending_hash
            .lock()
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        if pending.len() >= 32 || pending.contains_key(&hash) {
            bail!("Kad callback already pending or callback limit reached");
        }
        let revision = self.revision.fetch_add(1, Ordering::AcqRel);
        let (sender, response) = oneshot::channel();
        pending.insert(hash, (revision, sender));
        Ok(CallbackWaiter {
            key: WaitKey::Hash(hash),
            revision,
            registry: self.clone(),
            response,
        })
    }
    fn take_hash(&self, hash: &[u8; 16]) -> Option<oneshot::Sender<IncomingPeer>> {
        self.pending_hash
            .lock()
            .ok()?
            .remove(hash)
            .map(|(_, sender)| sender)
    }
    fn take(&self, id: u32) -> Option<oneshot::Sender<IncomingPeer>> {
        self.pending
            .lock()
            .ok()?
            .remove(&id)
            .map(|(_, sender)| sender)
    }
}
#[derive(Clone)]
pub struct CallbackClient {
    server: Option<ServerConnection>,
    registry: CallbackRegistry,
    /// Kad session and our TCP port, for firewalled Kad sources reached through their buddy.
    kad: Option<(Arc<KadSession>, u16)>,
}
impl CallbackClient {
    pub fn new(server: ServerConnection, registry: CallbackRegistry) -> Self {
        Self {
            server: Some(server),
            registry,
            kad: None,
        }
    }
    /// Callbacks through Kad buddies only (no ED2K server session).
    pub fn kad_only(registry: CallbackRegistry, kad: Arc<KadSession>, tcp_port: u16) -> Self {
        Self {
            server: None,
            registry,
            kad: Some((kad, tcp_port)),
        }
    }
    pub fn with_kad(mut self, kad: Arc<KadSession>, tcp_port: u16) -> Self {
        self.kad = Some((kad, tcp_port));
        self
    }
    async fn connect_through_buddy(
        &self,
        source: &FileSource,
        file: &FileLink,
        cancel: Arc<AtomicBool>,
    ) -> Result<peer::Peer> {
        let (kad, tcp_port) = self
            .kad
            .as_ref()
            .context("a firewalled Kad source needs a running Kad session")?;
        let buddy = source.buddy.as_ref().context("source has no buddy")?;
        let hash = source
            .user_hash
            .context("a Kad callback needs the source's user hash")?;
        // Registered before the request: the firewalled client may connect at once.
        let mut waiter = self.registry.register_hash(hash)?;
        // The buddy hash layout differs between Kad clients: ask once as published, once swapped.
        let mut incoming = None;
        for alternate in [false, true] {
            kad.request_callback(buddy, &file.hash, *tcp_port, alternate)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            tokio::select! {
                result = tokio::time::timeout(CALLBACK_WAIT, &mut waiter.response) => {
                    if let Ok(arrived) = result {
                        incoming = Some(arrived.context("ED2K callback listener closed")?);
                        break;
                    }
                },
                _ = peer::cancelled(&cancel) => bail!("ED2K transfer cancelled"),
            }
        }
        let incoming = incoming.context("Kad callback timed out")?;
        peer::Peer::from_incoming(incoming.stream, file, incoming.large, cancel).await
    }
    pub(crate) async fn connect(
        &self,
        source: &FileSource,
        file: &FileLink,
        cancel: Arc<AtomicBool>,
    ) -> Result<peer::Peer> {
        if source.buddy.is_some() {
            return self.connect_through_buddy(source, file, cancel).await;
        }
        let server = self
            .server
            .as_ref()
            .context("a Low-ID server source needs an ED2K server session")?;
        if server.is_low_id() {
            bail!(
                "Low-ID callbacks require a reachable incoming port and a High-ID server session"
            );
        }
        let mut waiter = self.registry.register(source.client_id)?;
        // Register before asking the server: the incoming peer can arrive at once.
        server.request_callback(source.client_id).await?;
        let incoming = tokio::select! {
            result=tokio::time::timeout(Duration::from_secs(30),&mut waiter.response)=>result.context("ED2K peer callback timed out")?.context("ED2K callback listener closed")?,
            _=peer::cancelled(&cancel)=>bail!("ED2K transfer cancelled"),
        };
        peer::Peer::from_incoming(incoming.stream, file, incoming.large, cancel).await
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct IncomingStatus {
    pub tcp_port: u16,
    pub running: bool,
    pub active_connections: usize,
    pub completed_handshakes: u64,
    /// Of those, how many arrived obfuscated.
    pub obfuscated_handshakes: u64,
}
/// Incoming ED2K hello/probe service. It lets servers test the advertised TCP
/// port. No file is shared implicitly, and unsupported file requests get NoFile.
pub struct IncomingListener {
    port: u16,
    alive: Arc<AtomicBool>,
    client_id: Arc<AtomicU32>,
    active: Arc<AtomicUsize>,
    handshakes: Arc<AtomicU64>,
    obfuscated: Arc<AtomicU64>,
    callbacks: CallbackRegistry,
    uploads: UploadManager,
    obfuscation: Arc<std::sync::atomic::AtomicU8>,
    task: tokio::task::AbortHandle,
}
impl Drop for IncomingListener {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
        self.task.abort();
        if let Ok(mut pending) = self.callbacks.pending.lock() {
            pending.clear();
        }
    }
}
impl IncomingListener {
    pub async fn bind(identity: [u8; 16], preferred_port: u16) -> Result<Self> {
        Self::bind_with_shares(identity, preferred_port, ShareRegistry::default()).await
    }

    pub async fn bind_with_shares(
        identity: [u8; 16],
        preferred_port: u16,
        shares: ShareRegistry,
    ) -> Result<Self> {
        Self::bind_with_uploads(identity, preferred_port, shares, UploadManager::default()).await
    }

    /// Like [`Self::bind_with_shares`], with caller-owned upload limits and statistics.
    pub async fn bind_with_uploads(
        identity: [u8; 16],
        preferred_port: u16,
        shares: ShareRegistry,
        uploads: UploadManager,
    ) -> Result<Self> {
        let listener =
            match TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, preferred_port)).await {
                Ok(listener) => listener,
                Err(e) if e.kind() == std::io::ErrorKind::AddrInUse && preferred_port != 0 => {
                    TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, 0)).await?
                }
                Err(e) => return Err(e).context("cannot open incoming ED2K TCP port"),
            };
        let port = listener.local_addr()?.port();
        let alive = Arc::new(AtomicBool::new(true));
        let live = alive.clone();
        let client_id = Arc::new(AtomicU32::new(0));
        let id = client_id.clone();
        let active = Arc::new(AtomicUsize::new(0));
        let peers = active.clone();
        let handshakes = Arc::new(AtomicU64::new(0));
        let completed = handshakes.clone();
        let obfuscated = Arc::new(AtomicU64::new(0));
        let obfuscated_count = obfuscated.clone();
        let callbacks = CallbackRegistry::default();
        let registry = callbacks.clone();
        let shared = shares.clone();
        let upload_manager = uploads.clone();
        let mode = Arc::new(std::sync::atomic::AtomicU8::new(Obfuscation::Prefer.code()));
        let mode_for_task = mode.clone();
        let task = tokio::spawn(async move {
            struct Offline(Arc<AtomicBool>);
            impl Drop for Offline {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::Release);
                }
            }
            let _offline = Offline(live);
            let mut sessions = JoinSet::new();
            let per_ip: Arc<Mutex<HashMap<std::net::IpAddr, usize>>> = Arc::default();
            loop {
                tokio::select! {
                    accepted=listener.accept()=>{
                        let Ok((stream,remote))=accepted else {break;};
                        if sessions.len()>=MAX_SESSIONS {drop(stream);continue;}
                        let ip = remote.ip();
                        {
                            let Ok(mut counts) = per_ip.lock() else {continue};
                            let count = counts.entry(ip).or_default();
                            if *count >= MAX_PER_IP {drop(stream);continue;}
                            *count += 1;
                        }
                        let active=peers.clone();let id=id.clone();let completed=completed.clone();let registry=registry.clone();let shared=shared.clone();let uploads=upload_manager.clone();let per_ip=per_ip.clone();let mode=mode_for_task.clone();let obfuscated=obfuscated_count.clone();
                        sessions.spawn(async move {
                            struct Active(Arc<AtomicUsize>);impl Drop for Active {fn drop(&mut self){self.0.fetch_sub(1,Ordering::AcqRel);}}
                            struct PerIp(Arc<Mutex<HashMap<std::net::IpAddr, usize>>>, std::net::IpAddr);
                            impl Drop for PerIp {fn drop(&mut self){if let Ok(mut c)=self.0.lock()&&let Some(n)=c.get_mut(&self.1){*n=n.saturating_sub(1);if *n==0{c.remove(&self.1);}}}}
                            active.fetch_add(1,Ordering::AcqRel);let _active=Active(active);let _ip=PerIp(per_ip,ip);
                            let mode = Obfuscation::from_code(mode.load(Ordering::Acquire));
                            let _=session(stream,ip,identity,mode,obfuscated,id,completed,registry,shared,uploads).await;
                        });
                    },
                    _=sessions.join_next(), if !sessions.is_empty()=>{},
                }
            }
        });
        Ok(Self {
            port,
            alive,
            client_id,
            active,
            handshakes,
            obfuscated,
            callbacks,
            uploads,
            obfuscation: mode,
            task: task.abort_handle(),
        })
    }
    pub fn set_client_id(&self, id: u32) {
        self.client_id.store(id, Ordering::Release);
    }
    pub fn callbacks(&self) -> CallbackRegistry {
        self.callbacks.clone()
    }
    /// Upload slots, queue, limits and statistics of this listener.
    /// Whether incoming connections may be plain, obfuscated or must be obfuscated.
    pub fn set_obfuscation(&self, mode: Obfuscation) {
        self.obfuscation.store(mode.code(), Ordering::Release);
    }
    pub fn uploads(&self) -> UploadManager {
        self.uploads.clone()
    }
    pub fn status(&self) -> IncomingStatus {
        IncomingStatus {
            tcp_port: self.port,
            running: self.alive.load(Ordering::Acquire),
            active_connections: self.active.load(Ordering::Acquire),
            completed_handshakes: self.handshakes.load(Ordering::Acquire),
            obfuscated_handshakes: self.obfuscated.load(Ordering::Acquire),
        }
    }
}
const MAX_SESSIONS: usize = 64;
const MAX_PER_IP: usize = 4;

#[allow(clippy::too_many_arguments)]
async fn session(
    stream: TcpStream,
    ip: std::net::IpAddr,
    identity: [u8; 16],
    mode: Obfuscation,
    obfuscated: Arc<AtomicU64>,
    client_id: Arc<AtomicU32>,
    completed: Arc<AtomicU64>,
    callbacks: CallbackRegistry,
    shares: ShareRegistry,
    uploads: UploadManager,
) -> Result<()> {
    // The obfuscation handshake (when the peer starts one) is part of the time-boxed exchange.
    let mut stream = tokio::time::timeout(
        Duration::from_secs(30),
        obfuscation::accept(stream, &identity, mode),
    )
    .await
    .context("incoming peer handshake timed out")??;
    if stream.is_obfuscated() {
        obfuscated.fetch_add(1, Ordering::AcqRel);
    }
    // Only the hello exchange is time-boxed; afterwards the uploader applies its own idle rules.
    let hello = tokio::time::timeout(Duration::from_secs(30), async {
        let (protocol, op, data) = peer::frame(&mut stream).await?;
        if protocol != 0xe3 || op != 1 || data.first() != Some(&16) {
            bail!("incoming peer must begin with an ED2K hello");
        }
        let info = peer::parse_hello(&data[1..])?;
        let answer = peer::hello(identity, client_id.load(Ordering::Acquire));
        peer::send(&mut stream, 0xe3, 0x4c, &answer[1..]).await?;
        Ok::<_, anyhow::Error>(info)
    })
    .await
    .context("incoming peer hello timed out")??;
    completed.fetch_add(1, Ordering::AcqRel);
    if let Some(sender) = callbacks
        .take(hello.client_id)
        .or_else(|| callbacks.take_hash(&hello.user_hash))
    {
        let _ = sender.send(IncomingPeer {
            stream,
            large: hello.large,
        });
        return Ok(());
    }
    upload::serve(stream, ip, hello, shares, uploads).await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn incoming_probe_advertises_assigned_identity_without_sharing_files() {
        let listener = IncomingListener::bind([3; 16], 0).await.unwrap();
        listener.set_client_id(0x04030201);
        let mut client =
            TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, listener.status().tcp_port))
                .await
                .unwrap();
        peer::send(&mut client, 0xe3, 1, &peer::hello([4; 16], 123))
            .await
            .unwrap();
        let (protocol, op, answer) = peer::frame(&mut client).await.unwrap();
        assert_eq!((protocol, op), (0xe3, 0x4c));
        assert_eq!(&answer[..16], &[3; 16]);
        assert_eq!(&answer[16..20], &0x04030201u32.to_le_bytes());
        peer::send(&mut client, 0xe3, 0x58, &[9; 16]).await.unwrap();
        let (_, op, data) = peer::frame(&mut client).await.unwrap();
        assert_eq!(op, 0x48);
        assert_eq!(data, vec![9; 16]);
        peer::send(&mut client, 0xe3, 0x4a, &[]).await.unwrap();
        assert_eq!(peer::frame(&mut client).await.unwrap().2, vec![0; 4]);
        assert_eq!(listener.status().completed_handshakes, 1);
        assert!(listener.status().running);
    }
    #[test]
    fn callback_registration_is_bounded_correlated_and_removed_on_drop() {
        let registry = CallbackRegistry::default();
        let first = registry.register(123).unwrap();
        assert!(registry.register(123).is_err());
        assert!(registry.register(0x01000000).is_err());
        assert!(registry.take(124).is_none());
        drop(first);
        assert!(registry.register(123).is_ok());
    }
    #[tokio::test]
    async fn server_callback_routes_the_matching_low_id_peer_into_a_verified_download() {
        use crate::ed2k::{
            metadata::Server,
            transfer::{TransferOptions, download_with_options},
        };
        let listener = IncomingListener::bind([3; 16], 0).await.unwrap();
        let port = listener.status().tcp_port;
        let control = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let control_port = control.local_addr().unwrap().port();
        let file = FileLink {
            name: "callback.txt".into(),
            size: 3,
            hash: crate::ed2k::file::digest(b"abc"),
        };
        let served = file.clone();
        let fixture = tokio::spawn(async move {
            let (mut server, _) = control.accept().await.unwrap();
            let (_, _, login) = peer::frame(&mut server).await.unwrap();
            assert_eq!(&login[20..22], &port.to_le_bytes());
            peer::send(&mut server, 0xe3, 0x40, &0x04030201u32.to_le_bytes())
                .await
                .unwrap();
            let (_, op, id) = peer::frame(&mut server).await.unwrap();
            assert_eq!(op, 0x1c);
            assert_eq!(id, 123u32.to_le_bytes());
            // A different incoming ID must not steal the pending callback.
            let mut wrong = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port))
                .await
                .unwrap();
            peer::send(&mut wrong, 0xe3, 1, &peer::hello([4; 16], 124))
                .await
                .unwrap();
            peer::frame(&mut wrong).await.unwrap();
            drop(wrong);
            let mut client = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port))
                .await
                .unwrap();
            peer::send(&mut client, 0xe3, 1, &peer::hello([4; 16], 123))
                .await
                .unwrap();
            peer::frame(&mut client).await.unwrap();
            assert_eq!(peer::frame(&mut client).await.unwrap().1, 0x58);
            let mut name = served.hash.to_vec();
            name.extend(1u16.to_le_bytes());
            name.push(b'x');
            peer::send(&mut client, 0xe3, 0x59, &name).await.unwrap();
            assert_eq!(peer::frame(&mut client).await.unwrap().1, 0x54);
            peer::send(&mut client, 0xe3, 0x55, &[]).await.unwrap();
            assert_eq!(peer::frame(&mut client).await.unwrap().1, 0x47);
            let mut data = served.hash.to_vec();
            data.extend(0u32.to_le_bytes());
            data.extend(3u32.to_le_bytes());
            data.extend(b"abc");
            peer::send(&mut client, 0xe3, 0x46, &data).await.unwrap();
        });
        let connection = ServerConnection::connect(
            Server {
                host: "127.0.0.1".into(),
                port: control_port,
                name: "fixture".into(),
                description: String::new(),
            },
            [3; 16],
            "Nibiru",
            port,
        )
        .await
        .unwrap();
        listener.set_client_id(connection.client_id);
        let options = TransferOptions {
            obfuscation: Obfuscation::Disabled,
            identity: [3; 16],
            client_id: connection.client_id,
            callbacks: Some(CallbackClient::new(connection, listener.callbacks())),
        };
        let directory = tempfile::tempdir().unwrap();
        let source = FileSource {
            client_id: 123,
            host: None,
            port: 4662,
            requires_callback: true,
            user_hash: None,
            crypt: 0,
            buddy: None,
        };
        let output = download_with_options(
            &file,
            directory.path(),
            &[source],
            options,
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .await
        .unwrap();
        assert_eq!(tokio::fs::read(output).await.unwrap(), b"abc");
        fixture.await.unwrap();
        assert_eq!(listener.status().completed_handshakes, 2);
    }

    #[tokio::test]
    async fn a_firewalled_kad_source_connects_back_after_a_callback_request_to_its_buddy() {
        use crate::ed2k::{Buddy, TransferOptions, download_with_options, kad_session::KadSession};
        let listener = IncomingListener::bind([3; 16], 0).await.unwrap();
        let port = listener.status().tcp_port;
        let file = FileLink {
            name: "kad.txt".into(),
            size: 3,
            hash: crate::ed2k::file::digest(b"abc"),
        };
        let served = file.clone();
        let buddy_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let buddy_port = buddy_socket.local_addr().unwrap().port();
        let published: [u8; 16] = *b"0123456789abcdef";
        let source_hash = [0x5a; 16];
        let fixture = tokio::spawn(async move {
            let mut buffer = [0u8; 128];
            // The buddy gets the request for the hash as published first.
            let (n, _) = buddy_socket.recv_from(&mut buffer).await.unwrap();
            assert_eq!(n, 2 + 16 + 16 + 2);
            assert_eq!(&buffer[..2], &[0xe4, 0x52]);
            assert_eq!(&buffer[2..18], &published);
            let mut wire_hash = served.hash;
            for chunk in wire_hash.chunks_exact_mut(4) {
                chunk.reverse();
            }
            assert_eq!(&buffer[18..34], &wire_hash);
            assert_eq!(&buffer[34..36], &port.to_le_bytes());
            // ...which tells the firewalled client, which connects to us and says hello.
            let mut client = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port))
                .await
                .unwrap();
            peer::send(&mut client, 0xe3, 1, &peer::hello(source_hash, 5))
                .await
                .unwrap();
            peer::frame(&mut client).await.unwrap();
            assert_eq!(peer::frame(&mut client).await.unwrap().1, 0x58);
            let mut name = served.hash.to_vec();
            name.extend(1u16.to_le_bytes());
            name.push(b'x');
            peer::send(&mut client, 0xe3, 0x59, &name).await.unwrap();
            assert_eq!(peer::frame(&mut client).await.unwrap().1, 0x54);
            peer::send(&mut client, 0xe3, 0x55, &[]).await.unwrap();
            assert_eq!(peer::frame(&mut client).await.unwrap().1, 0x47);
            let mut data = served.hash.to_vec();
            data.extend(0u32.to_le_bytes());
            data.extend(3u32.to_le_bytes());
            data.extend(b"abc");
            peer::send(&mut client, 0xe3, 0x46, &data).await.unwrap();
        });
        let state = tempfile::tempdir().unwrap();
        let kad = KadSession::start(
            [9; 16],
            port,
            state.path().join("routing.json"),
            Vec::new(),
            true,
        )
        .await
        .unwrap();
        let options = TransferOptions {
            obfuscation: Obfuscation::Disabled,
            identity: [3; 16],
            client_id: 0,
            callbacks: Some(CallbackClient::kad_only(
                listener.callbacks(),
                kad.clone(),
                port,
            )),
        };
        let source = FileSource {
            client_id: 1,
            host: None,
            port: 4662,
            requires_callback: true,
            user_hash: Some(source_hash),
            crypt: 0,
            buddy: Some(Buddy {
                ip: std::net::Ipv4Addr::LOCALHOST,
                udp_port: buddy_port,
                id: published,
            }),
        };
        let directory = tempfile::tempdir().unwrap();
        let output = download_with_options(
            &file,
            directory.path(),
            &[source],
            options,
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .await
        .unwrap();
        assert_eq!(tokio::fs::read(output).await.unwrap(), b"abc");
        fixture.await.unwrap();
        kad.stop();
    }
}
