//! The BitTorrent session: one listening socket shared by every torrent, global limits and the
//! registry of active torrents.

use super::{
    dht::{Dht, DhtConfig},
    limiter::RateLimiter,
    magnet::Magnet,
    message::read_handshake,
    metainfo::{Extras, Torrent},
    peer,
    torrent::{Environment, Settings, Start, TorrentHandle},
    tracker::TrackerClient,
};
use anyhow::{Context, Result};
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, sync::Semaphore, time::timeout};

#[derive(Clone, Debug)]
pub struct SessionConfig {
    /// 0 picks any free port.
    pub listen_port: u16,
    pub bind: IpAddr,
    pub download_limit: u64,
    pub upload_limit: u64,
    /// Let trackers resolve to private/loopback addresses (LAN trackers, tests).
    pub allow_private_trackers: bool,
    pub max_pending_handshakes: usize,
    /// Enables the DHT (trackerless peer discovery). Its UDP port defaults to the TCP port.
    pub dht: Option<DhtConfig>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            listen_port: 0,
            bind: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            download_limit: 0,
            upload_limit: 0,
            allow_private_trackers: false,
            max_pending_handshakes: 64,
            dht: None,
        }
    }
}

pub struct Session {
    port: u16,
    peer_id: [u8; 20],
    torrents: Mutex<HashMap<[u8; 20], TorrentHandle>>,
    down: Arc<RateLimiter>,
    up: Arc<RateLimiter>,
    tracker: TrackerClient,
    dht: Option<Arc<Dht>>,
}

fn new_peer_id() -> [u8; 20] {
    let mut id = [0u8; 20];
    id[..8].copy_from_slice(b"-NX0200-");
    id[8..].copy_from_slice(&uuid::Uuid::new_v4().as_bytes()[..12]);
    id
}

impl Session {
    pub async fn start(config: SessionConfig) -> Result<Arc<Self>> {
        let listener = TcpListener::bind(SocketAddr::new(config.bind, config.listen_port))
            .await
            .with_context(|| format!("cannot listen on port {}", config.listen_port))?;
        let port = listener.local_addr()?.port();
        let dht = match config.dht.clone() {
            Some(mut dht_config) => {
                if dht_config.port == 0 {
                    dht_config.port = port;
                }
                // The UDP port may be taken even though the TCP one was free; use any port then.
                match Dht::start(dht_config.clone()).await {
                    Ok(dht) => Some(dht),
                    Err(_) => {
                        dht_config.port = 0;
                        Dht::start(dht_config).await.ok()
                    }
                }
            }
            None => None,
        };
        if let Some(dht) = &dht {
            let dht = dht.clone();
            tokio::spawn(async move { dht.bootstrap().await });
        }
        let session = Arc::new(Self {
            port,
            peer_id: new_peer_id(),
            torrents: Mutex::new(HashMap::new()),
            down: Arc::new(RateLimiter::new(config.download_limit)),
            up: Arc::new(RateLimiter::new(config.upload_limit)),
            tracker: TrackerClient {
                allow_private: config.allow_private_trackers,
                ..TrackerClient::default()
            },
            dht,
        });
        let gate = Arc::new(Semaphore::new(config.max_pending_handshakes.max(1)));
        let weak = Arc::downgrade(&session);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, addr)) = listener.accept().await else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                };
                let Some(session) = weak.upgrade() else {
                    return;
                };
                let Ok(permit) = gate.clone().try_acquire_owned() else {
                    continue; // too many unauthenticated connections at once
                };
                tokio::spawn(async move {
                    let hello = timeout(Duration::from_secs(10), read_handshake(&mut stream)).await;
                    drop(permit);
                    let Ok(Ok(hello)) = hello else { return };
                    let handle = session
                        .torrents
                        .lock()
                        .ok()
                        .and_then(|t| t.get(&hello.info_hash).cloned());
                    if let Some(handle) = handle {
                        peer::accept(handle.shared.clone(), stream, addr, hello).await;
                    }
                });
            }
        });
        Ok(session)
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn dht(&self) -> Option<&Arc<Dht>> {
        self.dht.as_ref()
    }

    pub fn set_download_limit(&self, bytes_per_second: u64) {
        self.down.set_rate(bytes_per_second);
    }

    pub fn set_upload_limit(&self, bytes_per_second: u64) {
        self.up.set_rate(bytes_per_second);
    }

    fn register(&self, handle: TorrentHandle) -> TorrentHandle {
        let mut torrents = self.torrents.lock().unwrap_or_else(|e| e.into_inner());
        torrents.entry(handle.info_hash()).or_insert(handle).clone()
    }

    fn existing(&self, info_hash: &[u8; 20]) -> Option<TorrentHandle> {
        self.torrents.lock().ok()?.get(info_hash).cloned()
    }

    fn spawn(&self, start: Start, settings: Settings) -> TorrentHandle {
        TorrentHandle::spawn(
            start,
            settings,
            Environment {
                peer_id: self.peer_id,
                listen_port: self.port,
                down_limit: self.down.clone(),
                up_limit: self.up.clone(),
                tracker: self.tracker.clone(),
                dht: self.dht.clone(),
            },
        )
    }

    /// Adding the same torrent twice returns the one already running.
    pub fn add_torrent(&self, torrent: Torrent, settings: Settings) -> TorrentHandle {
        if let Some(existing) = self.existing(&torrent.info_hash) {
            return existing;
        }
        let start = Start {
            info_hash: torrent.info_hash,
            name: Some(torrent.name.clone()),
            extras: Extras {
                trackers: torrent.trackers.clone(),
                web_seeds: torrent.web_seeds.clone(),
            },
            torrent: Some(torrent),
        };
        let handle = self.spawn(start, settings);
        self.register(handle)
    }

    pub fn add_torrent_file(&self, bytes: &[u8], settings: Settings) -> Result<TorrentHandle> {
        Ok(self.add_torrent(Torrent::from_torrent_file(bytes)?, settings))
    }

    pub fn add_magnet(&self, magnet: &Magnet, mut settings: Settings) -> TorrentHandle {
        if let Some(existing) = self.existing(&magnet.info_hash) {
            return existing;
        }
        settings.peers.extend(magnet.peers.iter().copied());
        let start = Start {
            torrent: None,
            info_hash: magnet.info_hash,
            name: magnet.name.clone(),
            extras: Extras {
                trackers: magnet.trackers.iter().map(|t| vec![t.clone()]).collect(),
                web_seeds: magnet.web_seeds.clone(),
            },
        };
        let handle = self.spawn(start, settings);
        self.register(handle)
    }

    pub fn get(&self, info_hash: &[u8; 20]) -> Option<TorrentHandle> {
        self.existing(info_hash)
    }

    pub fn list(&self) -> Vec<TorrentHandle> {
        self.torrents
            .lock()
            .map(|t| t.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn remove(&self, info_hash: &[u8; 20], delete_files: bool) -> bool {
        let handle = self
            .torrents
            .lock()
            .ok()
            .and_then(|mut t| t.remove(info_hash));
        match handle {
            Some(handle) => {
                handle.shutdown(delete_files);
                true
            }
            None => false,
        }
    }

    pub fn shutdown(&self) {
        let handles: Vec<TorrentHandle> = self.list();
        for handle in handles {
            handle.shutdown(false);
        }
    }
}
