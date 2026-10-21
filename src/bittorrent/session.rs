//! The BitTorrent session: one listening socket shared by every torrent, global limits and the
//! registry of active torrents.

use super::{
    dht::{Dht, DhtConfig},
    limiter::RateLimiter,
    magnet::Magnet,
    metainfo::{Extras, Torrent},
    mse, peer,
    torrent::{Environment, Settings, Start, TorrentHandle},
    tracker::TrackerClient,
};
use anyhow::{Context, Result};
use librqbit_dualstack_sockets::TcpListener;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::Semaphore, time::timeout};

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
    /// Enables IPv4 DHT (trackerless discovery). Falls back to a free UDP port if busy.
    pub dht: Option<DhtConfig>,
    pub dht6: Option<DhtConfig>,
    /// Listen for uTP on the TCP peer port. DHT uses its own UDP socket/port.
    pub enable_utp: bool,
    /// Local Service Discovery (BEP 14) on the multicast group.
    pub lsd: Option<super::lsd::LsdConfig>,
    /// Test aid: behave like a restricted NAT and drop inbound peers we did not dial recently.
    /// Never enabled by the CLI; it exists to exercise BEP 55 hole punching on one machine.
    pub emulate_restricted_nat: bool,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            listen_port: 0,
            bind: IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            download_limit: 0,
            upload_limit: 0,
            allow_private_trackers: false,
            max_pending_handshakes: 64,
            dht: None,
            dht6: None,
            enable_utp: true,
            lsd: None,
            emulate_restricted_nat: false,
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
    dht6: Option<Arc<Dht>>,
    utp: Option<Arc<librqbit_utp::UtpSocketUdp>>,
    lsd: std::sync::OnceLock<super::lsd::Lsd>,
    restricted_nat: bool,
}

fn new_peer_id() -> [u8; 20] {
    let mut id = [0u8; 20];
    id[..8].copy_from_slice(b"-NX0200-");
    id[8..].copy_from_slice(&uuid::Uuid::new_v4().as_bytes()[..12]);
    id
}

impl Session {
    pub async fn start(config: SessionConfig) -> Result<Arc<Self>> {
        let listener = TcpListener::bind_tcp(
            SocketAddr::new(config.bind, config.listen_port),
            Default::default(),
        )
        .or_else(|error| {
            if config.bind == IpAddr::V6(Ipv6Addr::UNSPECIFIED) {
                TcpListener::bind_tcp(
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.listen_port),
                    Default::default(),
                )
            } else {
                Err(error)
            }
        })
        .with_context(|| format!("cannot listen on port {}", config.listen_port))?;
        let port = listener.bind_addr().port();
        let utp = if config.enable_utp {
            let options = librqbit_utp::SocketOpts {
                max_live_vsocks: std::num::NonZeroUsize::new(256),
                remote_inactivity_timeout: Some(Duration::from_secs(180)),
                ..Default::default()
            };
            Some(
                librqbit_utp::UtpSocketUdp::new_udp_with_opts(
                    SocketAddr::new(listener.bind_addr().ip(), port),
                    options,
                    Default::default(),
                )
                .await
                .context("cannot bind uTP peer port")?,
            )
        } else {
            None
        };
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
        let dht6 = if let Some(dht_config) = config.dht6.clone() {
            Dht::start(dht_config).await.ok()
        } else {
            None
        };
        for dht in [dht.as_ref(), dht6.as_ref()].into_iter().flatten() {
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
            dht6,
            utp: utp.clone(),
            lsd: std::sync::OnceLock::new(),
            restricted_nat: config.emulate_restricted_nat,
        });
        if let Some(lsd) = config.lsd.clone() {
            // Best effort: a host without multicast simply runs without LSD.
            if let Ok(lsd) = super::lsd::Lsd::start(lsd, port, Arc::downgrade(&session)) {
                let _ = session.lsd.set(lsd);
            }
        }
        let gate = Arc::new(Semaphore::new(config.max_pending_handshakes.max(1)));
        let weak = Arc::downgrade(&session);
        tokio::spawn(async move {
            loop {
                let Ok((stream, addr)) = listener.accept().await else {
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
                    let hashes = session.accepted_hashes();
                    let hello =
                        timeout(Duration::from_secs(15), mse::incoming(stream, &hashes)).await;
                    drop(permit);
                    let Ok(Ok((stream, hello, encrypted))) = hello else {
                        return;
                    };
                    let handle = session.existing(&hello.info_hash);
                    if let Some(handle) = handle
                        && session.admits(&handle, addr)
                    {
                        peer::accept(handle.shared.clone(), stream, addr, hello, encrypted, false)
                            .await;
                    }
                });
            }
        });
        if let Some(utp) = utp {
            let weak = Arc::downgrade(&session);
            let gate = Arc::new(Semaphore::new(config.max_pending_handshakes.max(1)));
            tokio::spawn(async move {
                loop {
                    let Ok(stream) = utp.accept().await else {
                        return;
                    };
                    let Some(session) = weak.upgrade() else {
                        return;
                    };
                    let Ok(permit) = gate.clone().try_acquire_owned() else {
                        continue;
                    };
                    tokio::spawn(async move {
                        let addr = stream.remote_addr();
                        let hashes = session.accepted_hashes();
                        let hello =
                            timeout(Duration::from_secs(15), mse::incoming(stream, &hashes)).await;
                        drop(permit);
                        let Ok(Ok((stream, hello, encrypted))) = hello else {
                            return;
                        };
                        let handle = session.existing(&hello.info_hash);
                        if let Some(handle) = handle
                            && session.admits(&handle, addr)
                        {
                            peer::accept(
                                handle.shared.clone(),
                                stream,
                                addr,
                                hello,
                                encrypted,
                                true,
                            )
                            .await;
                        }
                    });
                }
            });
        }
        Ok(session)
    }

    /// Whether peers can be reached over uTP (the only transport hole punching works on).
    pub fn utp_enabled(&self) -> bool {
        self.utp.is_some()
    }

    /// Whether Local Service Discovery is running on this session.
    pub fn lsd_enabled(&self) -> bool {
        self.lsd.get().is_some()
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn dht(&self) -> Option<&Arc<Dht>> {
        self.dht.as_ref()
    }

    pub fn dht6(&self) -> Option<&Arc<Dht>> {
        self.dht6.as_ref()
    }

    pub fn set_download_limit(&self, bytes_per_second: u64) {
        self.down.set_rate(bytes_per_second);
    }

    pub fn set_upload_limit(&self, bytes_per_second: u64) {
        self.up.set_rate(bytes_per_second);
    }

    fn register(&self, handle: TorrentHandle) -> TorrentHandle {
        let mut torrents = self.torrents.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = torrents.get(&handle.info_hash()).cloned() {
            // A concurrent add may have spawned the same torrent after the caller's
            // initial existing() check. Stop the losing instance so its background
            // loops cannot continue orphaned after its handle is dropped.
            handle.shutdown(false);
            return existing;
        }
        torrents.insert(handle.info_hash(), handle.clone());
        handle
    }

    /// A neighbour announced `info_hash` over LSD; ignored for private or unknown torrents.
    pub(crate) fn lsd_peer(&self, info_hash: &[u8; 20], addr: SocketAddr) {
        if let Some(handle) = self.existing(info_hash)
            && !handle.is_stopped()
            && !handle.shared.lock().private
        {
            handle.add_peer(addr);
        }
    }

    /// Wire identities of the public torrents whose LSD announcement is due, grouped per torrent.
    pub(crate) fn lsd_due(
        &self,
        last: &mut HashMap<[u8; 20], std::time::Instant>,
        interval: Duration,
    ) -> Vec<Vec<[u8; 20]>> {
        let torrents = self.list();
        last.retain(|hash, _| torrents.iter().any(|t| t.info_hash() == *hash));
        let mut due = Vec::new();
        for handle in torrents {
            // A magnet's privacy is unknown until its metadata arrives: do not announce it yet.
            if handle.is_stopped() || {
                let core = handle.shared.lock();
                core.private || core.ready().is_none()
            } {
                continue;
            }
            let key = handle.info_hash();
            if last.get(&key).is_none_or(|at| at.elapsed() >= interval) {
                last.insert(key, std::time::Instant::now());
                due.push(handle.shared.wire_hashes());
                if due.len() == 10 {
                    break;
                }
            }
        }
        due
    }

    /// Restricted-NAT emulation: an inbound peer is admitted only if we dialed its address lately.
    fn admits(&self, handle: &TorrentHandle, addr: SocketAddr) -> bool {
        !self.restricted_nat || handle.shared.lock().recently_dialed(&addr)
    }

    fn accepted_hashes(&self) -> Vec<[u8; 20]> {
        self.torrents
            .lock()
            .map(|t| t.values().flat_map(|h| h.shared.wire_hashes()).collect())
            .unwrap_or_default()
    }

    fn existing(&self, info_hash: &[u8; 20]) -> Option<TorrentHandle> {
        let torrents = self.torrents.lock().ok()?;
        torrents.get(info_hash).cloned().or_else(|| {
            torrents
                .values()
                .find(|h| h.shared.wire_hashes().contains(info_hash))
                .cloned()
        })
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
                dht6: self.dht6.clone(),
                utp: self.utp.clone(),
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
            v2_info_hash: torrent.v2.as_ref().map(|v| v.info_hash),
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
            v2_info_hash: magnet.v2_info_hash,
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
        let handle = self.torrents.lock().ok().and_then(|mut t| {
            let primary = t.get(info_hash).map(|h| h.info_hash()).or_else(|| {
                t.values()
                    .find(|h| h.shared.wire_hashes().contains(info_hash))
                    .map(|h| h.info_hash())
            })?;
            t.remove(&primary)
        });
        match handle {
            Some(handle) => {
                handle.shutdown(delete_files);
                true
            }
            None => false,
        }
    }

    /// Remove exactly this instance, never a newer transfer with the same hash.
    pub(crate) fn remove_instance(&self, expected: &TorrentHandle) -> bool {
        let removed = self.torrents.lock().ok().and_then(|mut torrents| {
            let key = torrents
                .iter()
                .find(|(_, h)| Arc::ptr_eq(&h.shared, &expected.shared))
                .map(|(key, _)| *key)?;
            torrents.remove(&key)
        });
        if let Some(handle) = removed {
            handle.shutdown(false);
            true
        } else {
            false
        }
    }

    pub fn shutdown(&self) {
        let handles: Vec<TorrentHandle> = self.list();
        for handle in handles {
            handle.shutdown(false);
        }
    }
}

#[cfg(test)]
mod audit_tests {
    use super::*;
    use crate::bittorrent::metainfo::build_info;

    #[tokio::test]
    async fn duplicate_registration_stops_the_losing_spawned_instance() {
        let session = Session::start(SessionConfig {
            bind: "127.0.0.1".parse().unwrap(),
            enable_utp: false,
            ..SessionConfig::default()
        })
        .await
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let hash = [7u8; 20];

        let make_start = || Start {
            torrent: None,
            info_hash: hash,
            v2_info_hash: None,
            name: Some("duplicate".to_owned()),
            extras: Extras::default(),
        };
        let first = session.spawn(make_start(), Settings::new(dir.path()));
        let winner = session.register(first);
        let losing = session.spawn(make_start(), Settings::new(dir.path()));
        let losing_probe = losing.clone();
        let returned = session.register(losing);

        assert!(Arc::ptr_eq(&winner.shared, &returned.shared));
        assert!(
            losing_probe.is_stopped(),
            "the duplicate spawned instance must be shut down"
        );
        session.shutdown();
    }

    #[tokio::test]
    async fn lsd_budget_defers_excess_torrents_without_marking_them_announced() {
        let session = Session::start(SessionConfig {
            bind: "127.0.0.1".parse().unwrap(),
            enable_utp: false,
            ..SessionConfig::default()
        })
        .await
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        for index in 0..11 {
            let name = format!("sample-{index}");
            let info = build_info(&name, 16384, &[(&[name.as_str()], &[1u8])]);
            let torrent = Torrent::from_info(&info, Extras::default()).unwrap();
            std::fs::write(dir.path().join(&name), [1u8]).unwrap();
            let mut settings = Settings::new(dir.path());
            settings.seed_ratio = None;
            session
                .add_torrent(torrent, settings)
                .wait_complete(Duration::from_secs(3))
                .await
                .unwrap();
        }
        let mut last = HashMap::new();
        let first = session.lsd_due(&mut last, Duration::from_secs(300));
        assert_eq!(first.len(), 10);
        assert_eq!(last.len(), 10, "unsent announcements must stay pending");
        let second = session.lsd_due(&mut last, Duration::from_secs(300));
        assert_eq!(second.len(), 1);
        let mut hashes: Vec<_> = first.into_iter().chain(second).flatten().collect();
        hashes.sort();
        hashes.dedup();
        assert_eq!(hashes.len(), 11);
        assert!(
            session
                .lsd_due(&mut last, Duration::from_secs(300))
                .is_empty()
        );
        session.shutdown();
    }
}
