//! Local Service Discovery (BEP 14): peers on the same network announce the torrents they have on
//! a multicast group, so a download can start from a neighbour without tracker or DHT.
//!
//! Wire format (verified against libtorrent): an HTTP-like `BT-SEARCH * HTTP/1.1` datagram with
//! `Host`, `Port`, one or more `Infohash` headers and a `cookie` that identifies the sender's
//! session so it can ignore its own announcements.
//!
//! IPv4 only. The group is joined on one interface (`LsdConfig::interface`, the OS default when
//! unspecified). Private torrents are neither announced nor accepted from LSD.

use super::session::Session;
use anyhow::{Context, Result};
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{Arc, Weak},
    time::{Duration, Instant},
};
use tokio::net::UdpSocket;

pub const GROUP: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(239, 192, 152, 143), 6771);
const MAX_DATAGRAM: usize = 1400;
const MAX_INFOHASHES: usize = 4;
/// BEP 14 asks for at most one announce per torrent every five minutes.
const DEFAULT_INTERVAL: Duration = Duration::from_secs(300);
const TICK: Duration = Duration::from_secs(2);
/// Incoming datagrams processed per second; the rest are dropped.
const RECEIVE_BUDGET: u32 = 40;

#[derive(Clone, Debug)]
pub struct LsdConfig {
    /// Local interface address used to join and send (unspecified: the OS default route).
    pub interface: Ipv4Addr,
    pub group: SocketAddrV4,
    pub announce_interval: Duration,
}
impl Default for LsdConfig {
    fn default() -> Self {
        Self {
            interface: Ipv4Addr::UNSPECIFIED,
            group: GROUP,
            announce_interval: DEFAULT_INTERVAL,
        }
    }
}

pub struct Lsd {
    tasks: Vec<tokio::task::JoinHandle<()>>,
}
impl Drop for Lsd {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// What a valid announcement says.
#[derive(Debug, PartialEq, Eq)]
pub struct Announcement {
    pub port: u16,
    pub info_hashes: Vec<[u8; 20]>,
    pub cookie: Option<String>,
}

pub fn build(group: SocketAddrV4, port: u16, hashes: &[[u8; 20]], cookie: &str) -> Vec<u8> {
    let mut text = format!("BT-SEARCH * HTTP/1.1\r\nHost: {group}\r\nPort: {port}\r\n");
    for hash in hashes.iter().take(MAX_INFOHASHES) {
        text.push_str("Infohash: ");
        for byte in hash {
            text.push_str(&format!("{byte:02x}"));
        }
        text.push_str("\r\n");
    }
    text.push_str(&format!("cookie: {cookie}\r\n\r\n\r\n"));
    text.into_bytes()
}

pub fn parse(bytes: &[u8]) -> Option<Announcement> {
    if bytes.len() > MAX_DATAGRAM {
        return None;
    }
    let text = std::str::from_utf8(bytes).ok()?;
    let mut lines = text.split("\r\n");
    if lines.next()? != "BT-SEARCH * HTTP/1.1" {
        return None;
    }
    let (mut port, mut hashes, mut cookie) = (None, Vec::new(), None);
    for line in lines.take(16) {
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':')?;
        let value = value.trim();
        match name.trim().to_ascii_lowercase().as_str() {
            "port" => port = value.parse::<u16>().ok().filter(|p| *p != 0),
            "infohash" => {
                if value.len() == 40 && hashes.len() < MAX_INFOHASHES {
                    let mut hash = [0u8; 20];
                    for (i, byte) in hash.iter_mut().enumerate() {
                        *byte = u8::from_str_radix(value.get(i * 2..i * 2 + 2)?, 16).ok()?;
                    }
                    hashes.push(hash);
                }
            }
            "cookie" => cookie = Some(value.chars().take(64).collect()),
            _ => {}
        }
    }
    if hashes.is_empty() {
        return None;
    }
    Some(Announcement {
        port: port?,
        info_hashes: hashes,
        cookie,
    })
}

/// Multicast never crosses a router, so a sender outside the local network is not a neighbour.
fn is_local(ip: Ipv4Addr) -> bool {
    ip.is_private() || ip.is_loopback() || ip.is_link_local()
}

fn bind(config: &LsdConfig) -> Result<UdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    #[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
    socket.set_reuse_port(true)?;
    socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, config.group.port()).into())?;
    socket
        .join_multicast_v4(config.group.ip(), &config.interface)
        .context("cannot join the LSD multicast group")?;
    socket.set_multicast_if_v4(&config.interface)?;
    socket.set_multicast_loop_v4(true)?;
    socket.set_multicast_ttl_v4(1)?;
    socket.set_nonblocking(true)?;
    Ok(UdpSocket::from_std(socket.into())?)
}

impl Lsd {
    pub fn start(config: LsdConfig, port: u16, session: Weak<Session>) -> Result<Self> {
        let socket = Arc::new(bind(&config)?);
        let mut cookie = [0u8; 4];
        getrandom::fill(&mut cookie).map_err(|e| anyhow::anyhow!("no randomness: {e}"))?;
        let cookie: String = cookie.iter().map(|b| format!("{b:02x}")).collect();

        let receive = {
            let (socket, session, cookie) = (socket.clone(), session.clone(), cookie.clone());
            tokio::spawn(async move {
                let mut buffer = [0u8; 2048];
                let (mut window, mut used) = (Instant::now(), 0u32);
                loop {
                    let Ok((length, from)) = socket.recv_from(&mut buffer).await else {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    };
                    if window.elapsed() >= Duration::from_secs(1) {
                        (window, used) = (Instant::now(), 0);
                    }
                    used += 1;
                    let SocketAddr::V4(from) = from else { continue };
                    if used > RECEIVE_BUDGET || !is_local(*from.ip()) {
                        continue;
                    }
                    let Some(found) = parse(&buffer[..length]) else {
                        continue;
                    };
                    if found.cookie.as_deref() == Some(cookie.as_str()) {
                        continue; // our own announcement, looped back
                    }
                    let Some(session) = session.upgrade() else {
                        return;
                    };
                    for hash in found.info_hashes {
                        session
                            .lsd_peer(&hash, SocketAddr::new(IpAddr::V4(*from.ip()), found.port));
                    }
                }
            })
        };
        let announce = tokio::spawn(async move {
            let mut last: HashMap<[u8; 20], Instant> = HashMap::new();
            let mut ticker = tokio::time::interval(TICK);
            loop {
                ticker.tick().await;
                let Some(session) = session.upgrade() else {
                    return;
                };
                let due = session.lsd_due(&mut last, config.announce_interval);
                drop(session);
                for hashes in due {
                    let packet = build(config.group, port, &hashes, &cookie);
                    let _ = socket.send_to(&packet, config.group).await;
                }
            }
        });
        Ok(Self {
            tasks: vec![receive, announce],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_and_parse_round_trip_with_both_wire_identities() {
        let hashes = [[0xab; 20], [0x01; 20]];
        let packet = build(GROUP, 51413, &hashes, "c0ffee");
        let text = String::from_utf8(packet.clone()).unwrap();
        assert!(
            text.starts_with(
                "BT-SEARCH * HTTP/1.1\r\nHost: 239.192.152.143:6771\r\nPort: 51413\r\n"
            )
        );
        assert!(text.ends_with("cookie: c0ffee\r\n\r\n\r\n"));
        let parsed = parse(&packet).unwrap();
        assert_eq!(parsed.port, 51413);
        assert_eq!(parsed.info_hashes, hashes.to_vec());
        assert_eq!(parsed.cookie.as_deref(), Some("c0ffee"));
    }

    #[test]
    fn parser_accepts_libtorrent_output_and_rejects_malformed_input() {
        let real = b"BT-SEARCH * HTTP/1.1\r\nHost: 239.192.152.143:6771\r\nPort: 38059\r\nInfohash: e556329a32478487d85cd86e6e0f5178f749f190\r\ncookie: 1a9824fa\r\n\r\n\r\n";
        let parsed = parse(real).unwrap();
        assert_eq!(parsed.port, 38059);
        assert_eq!(parsed.info_hashes.len(), 1);
        assert_eq!(parsed.info_hashes[0][0], 0xe5);
        // Header names are case-insensitive; unknown headers are ignored.
        assert!(parse(b"BT-SEARCH * HTTP/1.1\r\nPORT: 7\r\nINFOHASH: 00112233445566778899aabbccddeeff00112233\r\nX-Other: 1\r\n\r\n\r\n").is_some());
        for bad in [
            &b"GET / HTTP/1.1\r\n\r\n"[..],
            b"BT-SEARCH * HTTP/1.1\r\nPort: 0\r\nInfohash: 00112233445566778899aabbccddeeff00112233\r\n\r\n",
            b"BT-SEARCH * HTTP/1.1\r\nPort: 99999\r\nInfohash: 00112233445566778899aabbccddeeff00112233\r\n\r\n",
            b"BT-SEARCH * HTTP/1.1\r\nPort: 7\r\n\r\n",
            b"BT-SEARCH * HTTP/1.1\r\nPort: 7\r\nInfohash: zz112233445566778899aabbccddeeff00112233\r\n\r\n",
            b"BT-SEARCH * HTTP/1.1\r\nPort: 7\r\nInfohash: 0011\r\n\r\n",
            &[0xff, 0xfe, 0x00][..],
        ] {
            assert!(parse(bad).is_none(), "{:?}", String::from_utf8_lossy(bad));
        }
        assert!(parse(&vec![b'a'; MAX_DATAGRAM + 1]).is_none());
        // More infohashes than allowed are capped, not trusted.
        let mut many = String::from("BT-SEARCH * HTTP/1.1\r\nPort: 7\r\n");
        for _ in 0..10 {
            many.push_str("Infohash: 00112233445566778899aabbccddeeff00112233\r\n");
        }
        many.push_str("\r\n");
        assert_eq!(
            parse(many.as_bytes()).unwrap().info_hashes.len(),
            MAX_INFOHASHES
        );
    }

    #[test]
    fn only_neighbouring_addresses_count_as_local() {
        for ip in [
            "10.1.2.3",
            "192.168.0.9",
            "172.16.5.5",
            "127.0.0.1",
            "169.254.1.1",
        ] {
            assert!(is_local(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["8.8.8.8", "1.1.1.1", "203.0.113.5"] {
            assert!(!is_local(ip.parse().unwrap()), "{ip}");
        }
    }
}
