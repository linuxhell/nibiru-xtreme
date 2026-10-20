use super::{FileLink, FileSource, ServerConnection, peer};
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

pub(crate) struct IncomingPeer {
    stream: TcpStream,
    large: bool,
}
type PendingCallback = (u64, oneshot::Sender<IncomingPeer>);
#[derive(Clone, Default)]
pub struct CallbackRegistry {
    pending: Arc<Mutex<HashMap<u32, PendingCallback>>>,
    revision: Arc<AtomicU64>,
}
struct CallbackWaiter {
    id: u32,
    revision: u64,
    registry: CallbackRegistry,
    response: oneshot::Receiver<IncomingPeer>,
}
impl Drop for CallbackWaiter {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.registry.pending.lock()
            && pending
                .get(&self.id)
                .is_some_and(|(revision, _)| *revision == self.revision)
        {
            pending.remove(&self.id);
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
            id,
            revision,
            registry: self.clone(),
            response,
        })
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
    server: ServerConnection,
    registry: CallbackRegistry,
}
impl CallbackClient {
    pub fn new(server: ServerConnection, registry: CallbackRegistry) -> Self {
        Self { server, registry }
    }
    pub(crate) async fn connect(
        &self,
        source: &FileSource,
        file: &FileLink,
        cancel: Arc<AtomicBool>,
    ) -> Result<peer::Peer> {
        if self.server.is_low_id() {
            bail!(
                "Low-ID callbacks require a reachable incoming port and a High-ID server session"
            );
        }
        let mut waiter = self.registry.register(source.client_id)?;
        // Register before asking the server: the incoming peer can arrive at once.
        self.server.request_callback(source.client_id).await?;
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
}
/// Incoming ED2K hello/probe service. It lets servers test the advertised TCP
/// port. No file is shared implicitly, and unsupported file requests get NoFile.
pub struct IncomingListener {
    port: u16,
    alive: Arc<AtomicBool>,
    client_id: Arc<AtomicU32>,
    active: Arc<AtomicUsize>,
    handshakes: Arc<AtomicU64>,
    callbacks: CallbackRegistry,
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
        let callbacks = CallbackRegistry::default();
        let registry = callbacks.clone();
        let task = tokio::spawn(async move {
            struct Offline(Arc<AtomicBool>);
            impl Drop for Offline {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::Release);
                }
            }
            let _offline = Offline(live);
            let mut sessions = JoinSet::new();
            loop {
                tokio::select! {
                    accepted=listener.accept()=>{
                        let Ok((stream,_))=accepted else {break;};
                        if sessions.len()>=16 {drop(stream);continue;}
                        let active=peers.clone();let id=id.clone();let completed=completed.clone();let registry=registry.clone();
                        sessions.spawn(async move {
                            struct Active(Arc<AtomicUsize>);impl Drop for Active {fn drop(&mut self){self.0.fetch_sub(1,Ordering::AcqRel);}}
                            active.fetch_add(1,Ordering::AcqRel);let _active=Active(active);
                            let _=tokio::time::timeout(Duration::from_secs(30),session(stream,identity,id,completed,registry)).await;
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
            callbacks,
            task: task.abort_handle(),
        })
    }
    pub fn set_client_id(&self, id: u32) {
        self.client_id.store(id, Ordering::Release);
    }
    pub fn callbacks(&self) -> CallbackRegistry {
        self.callbacks.clone()
    }
    pub fn status(&self) -> IncomingStatus {
        IncomingStatus {
            tcp_port: self.port,
            running: self.alive.load(Ordering::Acquire),
            active_connections: self.active.load(Ordering::Acquire),
            completed_handshakes: self.handshakes.load(Ordering::Acquire),
        }
    }
}
async fn session(
    mut stream: TcpStream,
    identity: [u8; 16],
    client_id: Arc<AtomicU32>,
    completed: Arc<AtomicU64>,
    callbacks: CallbackRegistry,
) -> Result<()> {
    let (protocol, op, data) = peer::frame(&mut stream).await?;
    if protocol != 0xe3 || op != 1 || data.first() != Some(&16) {
        bail!("incoming peer must begin with an ED2K hello");
    }
    let large = peer::hello_answer(&data[1..])?;
    let remote_id = u32::from_le_bytes(data[17..21].try_into()?);
    let answer = peer::hello(identity, client_id.load(Ordering::Acquire));
    peer::send(&mut stream, 0xe3, 0x4c, &answer[1..]).await?;
    completed.fetch_add(1, Ordering::AcqRel);
    if let Some(sender) = callbacks.take(remote_id) {
        let _ = sender.send(IncomingPeer { stream, large });
        return Ok(());
    }
    for _ in 0..128 {
        let (protocol, op, data) = peer::frame(&mut stream).await?;
        match (protocol, op) {
            (0xc5, 1) => peer::send(&mut stream, 0xc5, 2, &[60, 1, 0, 0, 0, 0]).await?,
            (0xe3, 0x58 | 0x4f | 0x51 | 0x54) => {
                if data.len() != 16 {
                    bail!("invalid incoming file request");
                }
                peer::send(&mut stream, 0xe3, 0x48, &data).await?;
            }
            (0xe3, 0x4a) => peer::send(&mut stream, 0xe3, 0x4b, &[0; 4]).await?,
            (0xe3, 0x56 | 0x49) => return Ok(()),
            _ => {}
        }
    }
    Ok(())
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
}
