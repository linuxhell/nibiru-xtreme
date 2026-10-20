//! Tracker clients: HTTP/HTTPS (BEP 3, compact peers per BEP 23, IPv6 per BEP 7) and UDP
//! (BEP 15). Tracker URLs come from untrusted torrents, so by default they may not point at
//! loopback, private or link-local addresses (SSRF), redirects are re-checked, and responses
//! are size-bounded.

use super::bencode::{self, Value};
use anyhow::{Context, Result, bail};
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};
use tokio::net::{UdpSocket, lookup_host};

const MAX_RESPONSE: usize = 1024 * 1024;
const MAX_PEERS: usize = 500;
const MAX_REDIRECTS: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    None,
    Started,
    Stopped,
    Completed,
}

#[derive(Clone, Debug)]
pub struct AnnounceRequest {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub event: Event,
    pub key: u32,
    pub numwant: i32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AnnounceResponse {
    pub interval: Duration,
    pub min_interval: Option<Duration>,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
    pub peers: Vec<SocketAddr>,
    pub tracker_id: Option<String>,
    pub warning: Option<String>,
}

#[derive(Clone, Debug)]
pub struct TrackerClient {
    /// Allow trackers on loopback/private networks (LAN trackers, tests).
    pub allow_private: bool,
    /// First UDP retransmission timeout; doubles on every retry (BEP 15 says 15 s).
    pub udp_timeout: Duration,
    pub udp_attempts: u32,
    pub http_timeout: Duration,
    pub user_agent: String,
}

impl Default for TrackerClient {
    fn default() -> Self {
        Self {
            allow_private: false,
            udp_timeout: Duration::from_secs(8),
            udp_attempts: 3,
            http_timeout: Duration::from_secs(20),
            user_agent: concat!("Nibiru-Xtreme/", env!("CARGO_PKG_VERSION")).into(),
        }
    }
}

fn private_v4(ip: Ipv4Addr) -> bool {
    ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_broadcast()
        || ip.is_multicast()
        || ip.octets()[0] == 0
        || (ip.octets()[0] == 100 && (64..128).contains(&ip.octets()[1])) // CGNAT
}

pub(crate) fn is_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => private_v4(v4),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return private_v4(v4);
            }
            let first = v6.segments()[0];
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || first & 0xfe00 == 0xfc00 // unique local
                || first & 0xffc0 == 0xfe80 // link local
        }
    }
}

/// Resolves `host` and keeps only addresses the policy allows.
async fn resolve(host: &str, port: u16, allow_private: bool) -> Result<Vec<SocketAddr>> {
    // `Url::host_str` keeps the brackets of an IPv6 literal; the resolver wants the bare address.
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let addresses: Vec<SocketAddr> = lookup_host((host, port)).await?.collect();
    let allowed: Vec<SocketAddr> = addresses
        .into_iter()
        .filter(|a| allow_private || !is_private(a.ip()))
        .collect();
    if allowed.is_empty() {
        bail!("tracker {host} resolves only to local or private addresses");
    }
    Ok(allowed)
}

fn percent_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 3);
    for &b in bytes {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn compact_v4(bytes: &[u8]) -> Vec<SocketAddr> {
    bytes
        .chunks_exact(6)
        .filter_map(|c| {
            let port = u16::from_be_bytes([c[4], c[5]]);
            (port != 0).then(|| SocketAddr::from((Ipv4Addr::new(c[0], c[1], c[2], c[3]), port)))
        })
        .collect()
}

fn compact_v6(bytes: &[u8]) -> Vec<SocketAddr> {
    bytes
        .chunks_exact(18)
        .filter_map(|c| {
            let octets: [u8; 16] = c[..16].try_into().ok()?;
            let port = u16::from_be_bytes([c[16], c[17]]);
            (port != 0).then(|| SocketAddr::from((Ipv6Addr::from(octets), port)))
        })
        .collect()
}

pub(crate) fn parse_http_response(body: &[u8]) -> Result<AnnounceResponse> {
    let doc = bencode::decode(body)
        .context("tracker sent an invalid response")?
        .value;
    if let Some(reason) = doc.get("failure reason").and_then(Value::str) {
        bail!(
            "tracker refused the announce: {}",
            reason.chars().take(200).collect::<String>()
        );
    }
    let seconds = |key: &str| {
        doc.get(key)
            .and_then(Value::int)
            .filter(|n| *n >= 0)
            .map(|n| Duration::from_secs(n.min(86_400) as u64))
    };
    let count = |key: &str| {
        doc.get(key)
            .and_then(Value::int)
            .and_then(|n| u32::try_from(n).ok())
    };
    let mut peers = match doc.get("peers") {
        Some(Value::Bytes(compact)) => compact_v4(compact),
        Some(Value::List(list)) => list
            .iter()
            .filter_map(|p| {
                let ip: IpAddr = p.get("ip")?.str()?.parse().ok()?;
                let port = u16::try_from(p.get("port")?.int()?).ok()?;
                (port != 0).then(|| SocketAddr::new(ip, port))
            })
            .collect(),
        _ => Vec::new(),
    };
    if let Some(Value::Bytes(v6)) = doc.get("peers6") {
        peers.extend(compact_v6(v6));
    }
    peers.truncate(MAX_PEERS);
    Ok(AnnounceResponse {
        interval: seconds("interval")
            .unwrap_or(Duration::from_secs(1800))
            .max(Duration::from_secs(30)),
        min_interval: seconds("min interval"),
        seeders: count("complete"),
        leechers: count("incomplete"),
        peers,
        tracker_id: doc
            .get("tracker id")
            .and_then(Value::str)
            .map(|s| s.chars().take(64).collect()),
        warning: doc
            .get("warning message")
            .and_then(Value::str)
            .map(|s| s.chars().take(200).collect()),
    })
}

impl TrackerClient {
    pub async fn announce(&self, url: &str, request: &AnnounceRequest) -> Result<AnnounceResponse> {
        let parsed = url::Url::parse(url).context("invalid tracker URL")?;
        match parsed.scheme() {
            "http" | "https" => self.announce_http(parsed, request).await,
            "udp" => self.announce_udp(&parsed, request).await,
            other => bail!("unsupported tracker scheme {other}"),
        }
    }

    fn query(request: &AnnounceRequest) -> String {
        let event = match request.event {
            Event::None => "",
            Event::Started => "&event=started",
            Event::Stopped => "&event=stopped",
            Event::Completed => "&event=completed",
        };
        format!(
            "info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&compact=1&numwant={}&key={:08x}{event}",
            percent_bytes(&request.info_hash),
            percent_bytes(&request.peer_id),
            request.port,
            request.uploaded,
            request.downloaded,
            request.left,
            request.numwant.clamp(0, 200),
            request.key,
        )
    }

    async fn announce_http(
        &self,
        mut url: url::Url,
        request: &AnnounceRequest,
    ) -> Result<AnnounceResponse> {
        for _ in 0..=MAX_REDIRECTS {
            let host = url
                .host_str()
                .context("tracker URL has no host")?
                .to_owned();
            let port = url
                .port_or_known_default()
                .context("tracker URL has no port")?;
            // Pin the connection to the addresses that passed the policy: a later DNS answer
            // cannot redirect the request to an internal host.
            let addresses = resolve(&host, port, self.allow_private).await?;
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(self.http_timeout)
                .user_agent(&self.user_agent)
                .resolve_to_addrs(&host, &addresses)
                .build()?;
            let mut target = url.clone();
            let query = match target.query() {
                Some(existing) if !existing.is_empty() => {
                    format!("{existing}&{}", Self::query(request))
                }
                _ => Self::query(request),
            };
            target.set_query(Some(&query));
            let response = client
                .get(target)
                .send()
                .await
                .context("tracker request failed")?;
            if response.status().is_redirection() {
                let location = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .context("tracker redirect without a location")?;
                url = url.join(location).context("invalid tracker redirect")?;
                if !matches!(url.scheme(), "http" | "https") {
                    bail!("tracker redirected to a non-HTTP URL");
                }
                continue;
            }
            let response = response.error_for_status()?;
            if response
                .content_length()
                .is_some_and(|n| n > MAX_RESPONSE as u64)
            {
                bail!("tracker response is too large");
            }
            let mut body = Vec::new();
            let mut response = response;
            while let Some(chunk) = response.chunk().await? {
                if body.len() + chunk.len() > MAX_RESPONSE {
                    bail!("tracker response is too large");
                }
                body.extend_from_slice(&chunk);
            }
            return parse_http_response(&body);
        }
        bail!("too many tracker redirects")
    }

    async fn announce_udp(
        &self,
        url: &url::Url,
        request: &AnnounceRequest,
    ) -> Result<AnnounceResponse> {
        let host = url.host_str().context("tracker URL has no host")?;
        let port = url.port().context("UDP tracker URL needs a port")?;
        let address = resolve(host, port, self.allow_private).await?[0];
        let socket = UdpSocket::bind(if address.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        })
        .await?;
        socket.connect(address).await?;

        let mut timeout = self.udp_timeout;
        for _ in 0..self.udp_attempts.max(1) {
            // A connection ID is only worth keeping for one announce, so every attempt
            // starts with a fresh connect handshake; either step may time out.
            let transaction: u32 = rand_u32();
            let mut packet = 0x0417_2710_1980u64.to_be_bytes().to_vec();
            packet.extend(0u32.to_be_bytes());
            packet.extend(transaction.to_be_bytes());
            socket.send(&packet).await?;
            let connection_id: [u8; 8] =
                match tokio::time::timeout(timeout, recv_matching(&socket, 0, transaction)).await {
                    Ok(Ok(reply)) if reply.len() >= 16 => reply[8..16].try_into()?,
                    Ok(Ok(_)) => bail!("short UDP tracker connect reply"),
                    Ok(Err(error)) => return Err(error),
                    Err(_) => {
                        timeout *= 2;
                        continue;
                    }
                };
            let transaction: u32 = rand_u32();
            let mut packet = connection_id.to_vec();
            packet.extend(1u32.to_be_bytes());
            packet.extend(transaction.to_be_bytes());
            packet.extend(request.info_hash);
            packet.extend(request.peer_id);
            packet.extend(request.downloaded.to_be_bytes());
            packet.extend(request.left.to_be_bytes());
            packet.extend(request.uploaded.to_be_bytes());
            packet.extend(
                match request.event {
                    Event::None => 0u32,
                    Event::Completed => 1,
                    Event::Started => 2,
                    Event::Stopped => 3,
                }
                .to_be_bytes(),
            );
            packet.extend(0u32.to_be_bytes()); // IP: let the tracker use the source address
            packet.extend(request.key.to_be_bytes());
            packet.extend(request.numwant.clamp(0, 200).to_be_bytes());
            packet.extend(request.port.to_be_bytes());
            socket.send(&packet).await?;
            match tokio::time::timeout(timeout, recv_matching(&socket, 1, transaction)).await {
                Ok(Ok(reply)) => return parse_udp_announce(&reply, address.is_ipv6()),
                Ok(Err(error)) => return Err(error),
                Err(_) => timeout *= 2,
            }
        }
        bail!("UDP tracker did not answer")
    }
}

fn rand_u32() -> u32 {
    let bytes = uuid::Uuid::new_v4();
    u32::from_le_bytes(bytes.as_bytes()[..4].try_into().expect("four bytes"))
}

/// Waits for a datagram with the expected action and transaction ID; tracker errors (action 3)
/// become errors, anything else is ignored.
async fn recv_matching(socket: &UdpSocket, action: u32, transaction: u32) -> Result<Vec<u8>> {
    let mut buffer = vec![0u8; 8192];
    loop {
        let n = socket.recv(&mut buffer).await?;
        if n < 8 {
            continue;
        }
        let got_action = u32::from_be_bytes(buffer[..4].try_into()?);
        let got_transaction = u32::from_be_bytes(buffer[4..8].try_into()?);
        if got_transaction != transaction {
            continue;
        }
        if got_action == 3 {
            bail!(
                "tracker error: {}",
                String::from_utf8_lossy(&buffer[8..n])
                    .chars()
                    .take(200)
                    .collect::<String>()
            );
        }
        if got_action == action {
            return Ok(buffer[..n].to_vec());
        }
    }
}

pub(crate) fn parse_udp_announce(reply: &[u8], v6: bool) -> Result<AnnounceResponse> {
    if reply.len() < 20 {
        bail!("short UDP tracker announce reply");
    }
    let number = |at: usize| u32::from_be_bytes(reply[at..at + 4].try_into().expect("four bytes"));
    let mut peers = if v6 {
        compact_v6(&reply[20..])
    } else {
        compact_v4(&reply[20..])
    };
    peers.truncate(MAX_PEERS);
    Ok(AnnounceResponse {
        interval: Duration::from_secs(number(8).clamp(30, 86_400) as u64),
        leechers: Some(number(12)),
        seeders: Some(number(16)),
        peers,
        ..AnnounceResponse::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn request(event: Event) -> AnnounceRequest {
        AnnounceRequest {
            info_hash: [0xab; 20],
            peer_id: *b"-NX0200-abcdefghijkl",
            port: 6881,
            uploaded: 1,
            downloaded: 2,
            left: 3,
            event,
            key: 0xdeadbeef,
            numwant: 50,
        }
    }

    fn local() -> TrackerClient {
        TrackerClient {
            allow_private: true,
            udp_timeout: Duration::from_millis(300),
            udp_attempts: 2,
            ..TrackerClient::default()
        }
    }

    /// Serves canned HTTP responses; returns the port and a log of request lines.
    type Canned = (u16, Vec<(&'static str, String)>, Vec<u8>);
    async fn http_server(
        responses: Vec<Canned>,
    ) -> (u16, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = log.clone();
        tokio::spawn(async move {
            for (status, headers, body) in responses {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let mut buffer = vec![0u8; 8192];
                let n = stream.read(&mut buffer).await.unwrap_or(0);
                let request = String::from_utf8_lossy(&buffer[..n]).to_string();
                seen.lock()
                    .unwrap()
                    .push(request.lines().next().unwrap_or("").to_string());
                let mut head = format!(
                    "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n",
                    body.len()
                );
                for (k, v) in headers {
                    head.push_str(&format!("{k}: {v}\r\n"));
                }
                head.push_str("\r\n");
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(&body).await;
            }
        });
        (port, log)
    }

    #[test]
    fn the_query_percent_encodes_raw_hash_bytes() {
        let query = TrackerClient::query(&request(Event::Started));
        assert!(query.starts_with("info_hash=%AB%AB%AB%AB%AB%AB%AB%AB%AB%AB%AB%AB%AB%AB%AB%AB%AB%AB%AB%AB&peer_id=-NX0200-abcdefghijkl&port=6881&uploaded=1&downloaded=2&left=3&compact=1&numwant=50&key=deadbeef"), "{query}");
        assert!(query.ends_with("&event=started"));
        assert!(!TrackerClient::query(&request(Event::None)).contains("event="));
    }

    #[test]
    fn parses_compact_dictionary_and_ipv6_peers() {
        let compact = [1, 2, 3, 4, 0x1a, 0xe1, 5, 6, 7, 8, 0, 0];
        let mut v6 = Ipv6Addr::LOCALHOST.octets().to_vec();
        v6.extend(6882u16.to_be_bytes());
        let body = bencode::encode(&bencode::dict([
            ("interval", Value::Int(900)),
            ("complete", Value::Int(7)),
            ("incomplete", Value::Int(3)),
            ("peers", Value::Bytes(compact.to_vec())),
            ("peers6", Value::Bytes(v6)),
            ("tracker id", Value::text("abc")),
        ]));
        let r = parse_http_response(&body).unwrap();
        assert_eq!(r.interval, Duration::from_secs(900));
        assert_eq!((r.seeders, r.leechers), (Some(7), Some(3)));
        assert_eq!(r.peers.len(), 2, "port 0 is dropped");
        assert_eq!(r.peers[0], "1.2.3.4:6881".parse().unwrap());
        assert_eq!(r.peers[1], "[::1]:6882".parse().unwrap());
        assert_eq!(r.tracker_id.as_deref(), Some("abc"));

        let list = bencode::encode(&bencode::dict([
            ("interval", Value::Int(1)),
            (
                "peers",
                Value::List(vec![bencode::dict([
                    ("ip", Value::text("10.0.0.1")),
                    ("port", Value::Int(51413)),
                ])]),
            ),
        ]));
        let r = parse_http_response(&list).unwrap();
        assert_eq!(
            r.peers,
            vec!["10.0.0.1:51413".parse::<SocketAddr>().unwrap()]
        );
        assert_eq!(
            r.interval,
            Duration::from_secs(30),
            "absurdly short intervals are raised"
        );
    }

    #[test]
    fn failure_reasons_and_garbage_are_errors() {
        let failure = bencode::encode(&bencode::dict([(
            "failure reason",
            Value::text("torrent not registered"),
        )]));
        assert!(
            parse_http_response(&failure)
                .unwrap_err()
                .to_string()
                .contains("torrent not registered")
        );
        assert!(parse_http_response(b"<html>").is_err());
    }

    #[tokio::test]
    async fn http_announce_sends_the_expected_request_and_reads_peers() {
        let body = bencode::encode(&bencode::dict([
            ("interval", Value::Int(1800)),
            ("peers", Value::Bytes(vec![127, 0, 0, 1, 0x1a, 0xe1])),
        ]));
        let (port, log) = http_server(vec![(200, vec![], body)]).await;
        let url = format!("http://127.0.0.1:{port}/announce?passkey=secret");
        let r = local()
            .announce(&url, &request(Event::Started))
            .await
            .unwrap();
        assert_eq!(
            r.peers,
            vec!["127.0.0.1:6881".parse::<SocketAddr>().unwrap()]
        );
        let line = log.lock().unwrap()[0].clone();
        assert!(
            line.starts_with("GET /announce?passkey=secret&info_hash=%AB"),
            "{line}"
        );
        assert!(line.contains("&event=started"));
    }

    #[tokio::test]
    async fn local_trackers_are_refused_unless_explicitly_allowed() {
        let (port, log) = http_server(vec![(200, vec![], b"de".to_vec())]).await;
        for host in [
            "127.0.0.1",
            "localhost",
            "[::1]",
            "10.0.0.5",
            "192.168.1.1",
            "169.254.169.254",
        ] {
            let url = format!("http://{host}:{port}/announce");
            let error = TrackerClient::default()
                .announce(&url, &request(Event::None))
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("local or private")
                    || error.to_string().contains("tracker"),
                "{host}: {error}"
            );
        }
        assert!(
            log.lock().unwrap().is_empty(),
            "no request ever reached the local server"
        );
        assert!(
            TrackerClient::default()
                .announce("udp://127.0.0.1:9", &request(Event::None))
                .await
                .is_err()
        );
        assert!(
            TrackerClient::default()
                .announce("file:///etc/passwd", &request(Event::None))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn redirects_are_followed_a_limited_number_of_times_and_revalidated() {
        let ok = bencode::encode(&bencode::dict([("interval", Value::Int(60))]));
        let (port, _) = http_server(vec![
            (302, vec![("Location", "/second".into())], vec![]),
            (200, vec![], ok.clone()),
        ])
        .await;
        let r = local()
            .announce(&format!("http://127.0.0.1:{port}/a"), &request(Event::None))
            .await
            .unwrap();
        assert_eq!(r.interval, Duration::from_secs(60));

        let (port, _) = http_server(vec![(
            302,
            vec![("Location", "file:///etc/passwd".into())],
            vec![],
        )])
        .await;
        assert!(
            local()
                .announce(&format!("http://127.0.0.1:{port}/a"), &request(Event::None))
                .await
                .is_err()
        );

        let loop_forever: Vec<_> = (0..6)
            .map(|_| (302u16, vec![("Location", "/again".to_string())], vec![]))
            .collect();
        let (port, _) = http_server(loop_forever).await;
        let error = local()
            .announce(&format!("http://127.0.0.1:{port}/a"), &request(Event::None))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("redirect"), "{error}");
    }

    #[tokio::test]
    async fn oversized_tracker_responses_are_rejected() {
        let (port, _) = http_server(vec![(200, vec![], vec![b'x'; MAX_RESPONSE + 10])]).await;
        let error = local()
            .announce(&format!("http://127.0.0.1:{port}/a"), &request(Event::None))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("too large"), "{error}");
    }

    async fn udp_tracker(skip_first_connect: bool, answer_error: bool) -> u16 {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = socket.local_addr().unwrap().port();
        tokio::spawn(async move {
            let mut buffer = [0u8; 2048];
            let mut dropped = !skip_first_connect;
            let connection = [9u8, 8, 7, 6, 5, 4, 3, 2];
            loop {
                let Ok((n, from)) = socket.recv_from(&mut buffer).await else {
                    return;
                };
                let action = u32::from_be_bytes(buffer[8..12].try_into().unwrap());
                let transaction = &buffer[12..16];
                if action == 0 && n == 16 {
                    if !dropped {
                        dropped = true; // lose the first connect request
                        continue;
                    }
                    let mut reply = 0u32.to_be_bytes().to_vec();
                    reply.extend(transaction);
                    reply.extend(connection);
                    let _ = socket.send_to(&reply, from).await;
                } else if action == 1 {
                    assert_eq!(
                        &buffer[..8],
                        &connection,
                        "announce must quote the connection ID"
                    );
                    assert_eq!(n, 98);
                    let mut reply = if answer_error { 3u32 } else { 1u32 }
                        .to_be_bytes()
                        .to_vec();
                    reply.extend(transaction);
                    if answer_error {
                        reply.extend(b"unregistered torrent");
                    } else {
                        reply.extend(1800u32.to_be_bytes());
                        reply.extend(4u32.to_be_bytes()); // leechers
                        reply.extend(9u32.to_be_bytes()); // seeders
                        reply.extend([5, 6, 7, 8, 0x1a, 0xe2]);
                    }
                    let _ = socket.send_to(&reply, from).await;
                }
            }
        });
        port
    }

    #[tokio::test]
    async fn udp_announce_connects_then_announces_and_survives_a_lost_packet() {
        let port = udp_tracker(true, false).await;
        let r = local()
            .announce(
                &format!("udp://127.0.0.1:{port}/announce"),
                &request(Event::Completed),
            )
            .await
            .unwrap();
        assert_eq!(r.peers, vec!["5.6.7.8:6882".parse::<SocketAddr>().unwrap()]);
        assert_eq!((r.seeders, r.leechers), (Some(9), Some(4)));
        assert_eq!(r.interval, Duration::from_secs(1800));
    }

    #[tokio::test]
    async fn udp_tracker_errors_and_silence_are_reported() {
        let port = udp_tracker(false, true).await;
        let error = local()
            .announce(&format!("udp://127.0.0.1:{port}"), &request(Event::None))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("unregistered torrent"),
            "{error}"
        );
        let silent = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = silent.local_addr().unwrap().port();
        let error = local()
            .announce(&format!("udp://127.0.0.1:{port}"), &request(Event::None))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("did not answer"), "{error}");
        assert!(
            local()
                .announce("udp://127.0.0.1/noport", &request(Event::None))
                .await
                .is_err()
        );
    }

    #[test]
    fn short_or_hostile_udp_replies_are_rejected() {
        assert!(parse_udp_announce(&[0; 19], false).is_err());
        let mut reply = vec![0u8; 20];
        reply[8..12].copy_from_slice(&0u32.to_be_bytes()); // interval 0 -> clamped
        assert_eq!(
            parse_udp_announce(&reply, false).unwrap().interval,
            Duration::from_secs(30)
        );
    }

    #[test]
    fn address_policy_covers_the_usual_internal_ranges() {
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.0.1",
            "169.254.1.1",
            "0.0.0.0",
            "100.64.0.1",
            "::1",
            "fe80::1",
            "fd00::1",
            "::ffff:127.0.0.1",
            "224.0.0.1",
        ] {
            assert!(is_private(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["8.8.8.8", "93.184.216.34", "2606:4700::1111", "100.63.0.1"] {
            assert!(!is_private(ip.parse().unwrap()), "{ip}");
        }
    }
}
