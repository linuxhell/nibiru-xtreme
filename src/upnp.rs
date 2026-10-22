//! UPnP IGD port mapping, so peers behind a home router can reach the listening ports.
//!
//! The caller says which ports it wants reachable (`apply`); this module finds the router, adds
//! the mappings with a lease, renews them while they are wanted and removes them on `release`.
//! A mapping is only ever created for a port the caller listed, and every one is removed again when
//! the caller stops wanting it. Discovery only trusts a router whose service URLs point at the
//! address that answered (igd-next's default), so a rogue SSDP reply cannot redirect requests.
use igd_next::{
    AddPortError, PortMappingProtocol, SearchOptions,
    aio::{
        Gateway,
        tokio::{Tokio, search_gateway},
    },
};
use serde::Serialize;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{sync::Mutex as AsyncMutex, task::JoinHandle};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Proto {
    Tcp,
    Udp,
}

impl Proto {
    fn igd(self) -> PortMappingProtocol {
        match self {
            Self::Tcp => PortMappingProtocol::TCP,
            Self::Udp => PortMappingProtocol::UDP,
        }
    }
}

/// A port to make reachable from the internet (external port = local port).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
pub struct Mapping {
    pub proto: Proto,
    pub port: u16,
}

#[derive(Clone, Debug, Serialize)]
pub struct Status {
    /// `idle` (nothing wanted), `searching`, `mapped`, `partial` or `failed`.
    pub state: &'static str,
    pub gateway: Option<String>,
    pub external_ip: Option<String>,
    pub mappings: Vec<Mapping>,
    pub error: Option<String>,
}

impl Status {
    fn idle() -> Self {
        Self {
            state: "idle",
            gateway: None,
            external_ip: None,
            mappings: Vec::new(),
            error: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Timing {
    /// Lease asked of the router, in seconds. The mapping vanishes by itself after a crash.
    pub lease_secs: u32,
    /// How often the mappings are refreshed (and the router found again if it went away).
    pub renew: Duration,
    /// How long to wait for a router to answer.
    pub search: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            lease_secs: 3600,
            renew: Duration::from_secs(20 * 60),
            search: Duration::from_secs(8),
        }
    }
}

struct Inner {
    gateway: Option<Gateway<Tokio>>,
    mapped: Vec<Mapping>,
    wanted: Vec<Mapping>,
    renewer: Option<JoinHandle<()>>,
}

pub struct Upnp {
    search: Box<dyn Fn() -> SearchOptions + Send + Sync>,
    timing: Timing,
    description: String,
    state: AsyncMutex<Inner>,
    status: Mutex<Status>,
}

fn local_ipv4_toward(gateway: SocketAddr) -> Result<Ipv4Addr, String> {
    let socket =
        std::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).map_err(|e| e.to_string())?;
    socket.connect(gateway).map_err(|e| e.to_string())?;
    match socket.local_addr().map_err(|e| e.to_string())?.ip() {
        IpAddr::V4(ip) if !ip.is_unspecified() => Ok(ip),
        _ => Err("upnp_no_ipv4_route_to_gateway".into()),
    }
}

impl Upnp {
    pub fn new(description: &str) -> Arc<Self> {
        Self::with(
            description,
            Timing::default(),
            Box::new(SearchOptions::default),
        )
    }

    /// Same, with explicit timing and discovery options (tests point discovery at a local fake).
    pub fn with(
        description: &str,
        timing: Timing,
        search: Box<dyn Fn() -> SearchOptions + Send + Sync>,
    ) -> Arc<Self> {
        Arc::new(Self {
            search,
            timing,
            description: description.chars().take(60).collect(),
            state: AsyncMutex::new(Inner {
                gateway: None,
                mapped: Vec::new(),
                wanted: Vec::new(),
                renewer: None,
            }),
            status: Mutex::new(Status::idle()),
        })
    }

    pub fn status(&self) -> Status {
        self.status
            .lock()
            .map(|status| status.clone())
            .unwrap_or_else(|_| Status::idle())
    }

    fn set_status(&self, status: Status) {
        if let Ok(mut current) = self.status.lock() {
            *current = status;
        }
    }

    /// Makes exactly these ports reachable: adds what is missing, removes what is no longer wanted
    /// and keeps renewing. An empty list is the same as [`Self::release`].
    pub async fn apply(self: &Arc<Self>, wanted: Vec<Mapping>) {
        let mut inner = self.state.lock().await;
        let mut wanted = wanted;
        wanted.sort_by_key(|m| (m.proto as u8, m.port));
        wanted.dedup();
        inner.wanted = wanted;
        if inner.wanted.is_empty() {
            self.release_locked(&mut inner).await;
            return;
        }
        if inner.renewer.is_none() {
            let weak: Weak<Self> = Arc::downgrade(self);
            let every = self.timing.renew;
            inner.renewer = Some(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(every).await;
                    let Some(upnp) = weak.upgrade() else { return };
                    let mut inner = upnp.state.lock().await;
                    if inner.wanted.is_empty() {
                        return;
                    }
                    upnp.reconcile(&mut inner).await;
                }
            }));
        }
        self.reconcile(&mut inner).await;
    }

    /// Removes every mapping this module created and stops renewing.
    pub async fn release(&self) {
        let mut inner = self.state.lock().await;
        inner.wanted.clear();
        self.release_locked(&mut inner).await;
    }

    async fn release_locked(&self, inner: &mut Inner) {
        if let Some(renewer) = inner.renewer.take() {
            renewer.abort();
        }
        if let Some(gateway) = inner.gateway.clone() {
            for mapping in std::mem::take(&mut inner.mapped) {
                let _ = tokio::time::timeout(
                    Duration::from_secs(4),
                    gateway.remove_port(mapping.proto.igd(), mapping.port),
                )
                .await;
            }
        }
        inner.mapped.clear();
        self.set_status(Status::idle());
    }

    async fn reconcile(&self, inner: &mut Inner) {
        if inner.gateway.is_none() {
            self.set_status(Status {
                state: "searching",
                ..Status::idle()
            });
            let found =
                tokio::time::timeout(self.timing.search, search_gateway((self.search)())).await;
            match found {
                Ok(Ok(gateway)) => inner.gateway = Some(gateway),
                Ok(Err(error)) => return self.fail(format!("upnp_no_gateway: {error}")),
                Err(_) => return self.fail("upnp_no_gateway: no router answered".into()),
            }
        }
        let Some(gateway) = inner.gateway.clone() else {
            return;
        };
        let local = match local_ipv4_toward(gateway.addr) {
            Ok(ip) => ip,
            Err(error) => return self.fail(error),
        };
        for stale in inner
            .mapped
            .iter()
            .filter(|m| !inner.wanted.contains(m))
            .copied()
            .collect::<Vec<_>>()
        {
            let _ = gateway.remove_port(stale.proto.igd(), stale.port).await;
            inner.mapped.retain(|m| m != &stale);
        }
        let mut errors = Vec::new();
        let mut unreachable = 0;
        for mapping in inner.wanted.clone() {
            let target = SocketAddr::V4(SocketAddrV4::new(local, mapping.port));
            let mut result = gateway
                .add_port(
                    mapping.proto.igd(),
                    mapping.port,
                    target,
                    self.timing.lease_secs,
                    &self.description,
                )
                .await;
            if matches!(result, Err(AddPortError::OnlyPermanentLeasesSupported)) {
                result = gateway
                    .add_port(
                        mapping.proto.igd(),
                        mapping.port,
                        target,
                        0,
                        &self.description,
                    )
                    .await;
            }
            match result {
                Ok(()) => {
                    if !inner.mapped.contains(&mapping) {
                        inner.mapped.push(mapping);
                    }
                }
                Err(error) => {
                    if matches!(error, AddPortError::RequestError(_)) {
                        unreachable += 1;
                    }
                    inner.mapped.retain(|m| m != &mapping);
                    errors.push(format!("{:?} {}: {error}", mapping.proto, mapping.port));
                }
            }
        }
        if unreachable == inner.wanted.len() {
            // The router stopped answering (rebooted, changed address): look for it again next time.
            inner.gateway = None;
        }
        let external_ip = gateway
            .get_external_ip()
            .await
            .ok()
            .map(|ip| ip.to_string());
        self.set_status(Status {
            state: if errors.is_empty() {
                "mapped"
            } else if inner.mapped.is_empty() {
                "failed"
            } else {
                "partial"
            },
            gateway: Some(gateway.addr.ip().to_string()),
            external_ip,
            mappings: inner.mapped.clone(),
            error: (!errors.is_empty()).then(|| errors.join("; ")),
        });
    }

    fn fail(&self, error: String) {
        self.set_status(Status {
            state: "failed",
            error: Some(error),
            ..Status::idle()
        });
    }
}
