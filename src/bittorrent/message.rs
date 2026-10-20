//! BitTorrent peer wire codec: handshake (BEP 3), messages including the Fast Extension (BEP 6),
//! and the extension protocol (BEP 10) with ut_metadata (BEP 9) and ut_pex (BEP 11).

use super::bencode::{self, Value, dict};
use anyhow::{Context, Result, bail};
use std::{
    collections::BTreeMap,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const BLOCK_SIZE: u32 = 16 * 1024;
const PROTOCOL: &[u8; 19] = b"BitTorrent protocol";

/// Largest message accepted from a peer. A bitfield for a million pieces is 125 kB; pieces
/// themselves are limited much more tightly below.
pub const MAX_MESSAGE: usize = 2 * 1024 * 1024;
/// A `piece` message may carry at most this many data bytes (the standard block is 16 KiB).
pub const MAX_BLOCK: usize = 32 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HashRequest {
    pub root: [u8; 32],
    pub base_layer: u32,
    pub index: u32,
    pub length: u32,
    pub proof_layers: u32,
}

impl HashRequest {
    fn encode(&self) -> Vec<u8> {
        let mut out = self.root.to_vec();
        for n in [self.base_layer, self.index, self.length, self.proof_layers] {
            out.extend(n.to_be_bytes());
        }
        out
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 48 {
            bail!("invalid hash request length");
        }
        Ok(Self {
            root: bytes[..32].try_into()?,
            base_layer: be32(&bytes[32..]),
            index: be32(&bytes[36..]),
            length: be32(&bytes[40..]),
            proof_layers: be32(&bytes[44..]),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Handshake {
    pub reserved: [u8; 8],
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
}

impl Handshake {
    pub const EXTENSIONS: u8 = 0x10; // reserved[5]
    pub const FAST: u8 = 0x04; // reserved[7]
    pub const DHT: u8 = 0x01; // reserved[7]

    pub fn new(info_hash: [u8; 20], peer_id: [u8; 20], dht: bool) -> Self {
        let mut reserved = [0u8; 8];
        reserved[5] |= Self::EXTENSIONS;
        reserved[7] |= Self::FAST | if dht { Self::DHT } else { 0 };
        Self {
            reserved,
            info_hash,
            peer_id,
        }
    }

    pub fn supports_extensions(&self) -> bool {
        self.reserved[5] & Self::EXTENSIONS != 0
    }

    pub fn supports_fast(&self) -> bool {
        self.reserved[7] & Self::FAST != 0
    }

    pub fn supports_dht(&self) -> bool {
        self.reserved[7] & Self::DHT != 0
    }

    pub fn encode(&self) -> [u8; 68] {
        let mut out = [0u8; 68];
        out[0] = 19;
        out[1..20].copy_from_slice(PROTOCOL);
        out[20..28].copy_from_slice(&self.reserved);
        out[28..48].copy_from_slice(&self.info_hash);
        out[48..68].copy_from_slice(&self.peer_id);
        out
    }

    pub fn decode(bytes: &[u8; 68]) -> Result<Self> {
        if bytes[0] != 19 || &bytes[1..20] != PROTOCOL {
            bail!("not a BitTorrent handshake");
        }
        Ok(Self {
            reserved: bytes[20..28].try_into()?,
            info_hash: bytes[28..48].try_into()?,
            peer_id: bytes[48..68].try_into()?,
        })
    }
}

pub async fn read_handshake<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Handshake> {
    let mut bytes = [0u8; 68];
    reader.read_exact(&mut bytes).await?;
    Handshake::decode(&bytes)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    KeepAlive,
    HashRequest(HashRequest),
    Hashes {
        request: HashRequest,
        hashes: Vec<[u8; 32]>,
    },
    HashReject(HashRequest),
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        data: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
    Port(u16),
    Suggest(u32),
    HaveAll,
    HaveNone,
    Reject {
        index: u32,
        begin: u32,
        length: u32,
    },
    AllowedFast(u32),
    Extended {
        id: u8,
        payload: Vec<u8>,
    },
}

fn be32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes[..4].try_into().expect("four bytes"))
}

impl Message {
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        let id = match self {
            Message::HashRequest(request) => {
                body.extend(request.encode());
                21
            }
            Message::Hashes { request, hashes } => {
                body.extend(request.encode());
                for hash in hashes {
                    body.extend(hash);
                }
                22
            }
            Message::HashReject(request) => {
                body.extend(request.encode());
                23
            }
            Message::KeepAlive => return 0u32.to_be_bytes().to_vec(),
            Message::Choke => 0,
            Message::Unchoke => 1,
            Message::Interested => 2,
            Message::NotInterested => 3,
            Message::Have(i) => {
                body.extend(i.to_be_bytes());
                4
            }
            Message::Bitfield(b) => {
                body.extend(b);
                5
            }
            Message::Request {
                index,
                begin,
                length,
            } => {
                body.extend(index.to_be_bytes());
                body.extend(begin.to_be_bytes());
                body.extend(length.to_be_bytes());
                6
            }
            Message::Piece { index, begin, data } => {
                body.extend(index.to_be_bytes());
                body.extend(begin.to_be_bytes());
                body.extend(data);
                7
            }
            Message::Cancel {
                index,
                begin,
                length,
            } => {
                body.extend(index.to_be_bytes());
                body.extend(begin.to_be_bytes());
                body.extend(length.to_be_bytes());
                8
            }
            Message::Port(p) => {
                body.extend(p.to_be_bytes());
                9
            }
            Message::Suggest(i) => {
                body.extend(i.to_be_bytes());
                13
            }
            Message::HaveAll => 14,
            Message::HaveNone => 15,
            Message::Reject {
                index,
                begin,
                length,
            } => {
                body.extend(index.to_be_bytes());
                body.extend(begin.to_be_bytes());
                body.extend(length.to_be_bytes());
                16
            }
            Message::AllowedFast(i) => {
                body.extend(i.to_be_bytes());
                17
            }
            Message::Extended { id, payload } => {
                body.push(*id);
                body.extend(payload);
                20
            }
        };
        let mut out = Vec::with_capacity(5 + body.len());
        out.extend(((body.len() + 1) as u32).to_be_bytes());
        out.push(id);
        out.extend(body);
        out
    }

    /// Parses one message body (everything after the 4-byte length). Empty means keep-alive.
    pub fn decode(payload: &[u8]) -> Result<Self> {
        let Some((&id, body)) = payload.split_first() else {
            return Ok(Message::KeepAlive);
        };
        let exact = |n: usize| -> Result<()> {
            if body.len() != n {
                bail!("message {id} has {} bytes, expected {n}", body.len());
            }
            Ok(())
        };
        Ok(match id {
            0 => {
                exact(0)?;
                Message::Choke
            }
            1 => {
                exact(0)?;
                Message::Unchoke
            }
            2 => {
                exact(0)?;
                Message::Interested
            }
            3 => {
                exact(0)?;
                Message::NotInterested
            }
            4 => {
                exact(4)?;
                Message::Have(be32(body))
            }
            5 => Message::Bitfield(body.to_vec()),
            6 | 8 | 16 => {
                exact(12)?;
                let (index, begin, length) = (be32(body), be32(&body[4..]), be32(&body[8..]));
                match id {
                    6 => Message::Request {
                        index,
                        begin,
                        length,
                    },
                    8 => Message::Cancel {
                        index,
                        begin,
                        length,
                    },
                    _ => Message::Reject {
                        index,
                        begin,
                        length,
                    },
                }
            }
            7 => {
                if body.len() < 8 || body.len() - 8 > MAX_BLOCK {
                    bail!("piece message has an invalid size");
                }
                Message::Piece {
                    index: be32(body),
                    begin: be32(&body[4..]),
                    data: body[8..].to_vec(),
                }
            }
            9 => {
                exact(2)?;
                Message::Port(u16::from_be_bytes([body[0], body[1]]))
            }
            13 => {
                exact(4)?;
                Message::Suggest(be32(body))
            }
            14 => {
                exact(0)?;
                Message::HaveAll
            }
            15 => {
                exact(0)?;
                Message::HaveNone
            }
            17 => {
                exact(4)?;
                Message::AllowedFast(be32(body))
            }
            20 => {
                if body.is_empty() {
                    bail!("empty extended message");
                }
                Message::Extended {
                    id: body[0],
                    payload: body[1..].to_vec(),
                }
            }
            21 | 23 => {
                let request = HashRequest::decode(body)?;
                if id == 21 {
                    Message::HashRequest(request)
                } else {
                    Message::HashReject(request)
                }
            }
            22 => {
                if body.len() < 48
                    || !(body.len() - 48).is_multiple_of(32)
                    || (body.len() - 48) / 32 > 544
                {
                    bail!("invalid hash response size");
                }
                let request = HashRequest::decode(&body[..48])?;
                let hashes = body[48..]
                    .chunks_exact(32)
                    .map(|h| h.try_into().expect("32 bytes"))
                    .collect();
                Message::Hashes { request, hashes }
            }
            // Unknown message types must be skipped, not treated as fatal.
            other => bail!("unknown message id {other}"),
        })
    }
}

/// Reads one length-prefixed message. Unknown message ids are consumed and reported as
/// `Ok(None)` so the caller can ignore them.
pub async fn read_message<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Message>> {
    let length = reader.read_u32().await? as usize;
    if length > MAX_MESSAGE {
        bail!("peer message of {length} bytes exceeds the limit");
    }
    let mut payload = vec![0u8; length];
    reader.read_exact(&mut payload).await?;
    match Message::decode(&payload) {
        Ok(message) => Ok(Some(message)),
        Err(error) if error.to_string().starts_with("unknown message id") => Ok(None),
        Err(error) => Err(error),
    }
}

pub async fn write_message<W: AsyncWrite + Unpin>(writer: &mut W, message: &Message) -> Result<()> {
    writer.write_all(&message.encode()).await?;
    Ok(())
}

// ---- Extension protocol (BEP 10) ----

/// Extension message IDs we ask peers to use when sending to us.
pub const LOCAL_UT_METADATA: u8 = 1;
pub const LOCAL_UT_PEX: u8 = 2;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExtHandshake {
    /// Extension name -> the ID the *sender* wants us to use when sending that extension.
    pub extensions: BTreeMap<String, u8>,
    pub metadata_size: Option<u32>,
    pub listen_port: Option<u16>,
    pub client: Option<String>,
    pub request_queue: Option<u32>,
}

impl ExtHandshake {
    pub fn ours(metadata_size: Option<u32>, listen_port: u16) -> Self {
        Self {
            extensions: BTreeMap::from([
                ("ut_metadata".to_owned(), LOCAL_UT_METADATA),
                ("ut_pex".to_owned(), LOCAL_UT_PEX),
            ]),
            metadata_size,
            listen_port: Some(listen_port),
            client: Some(format!("Nibiru Xtreme {}", env!("CARGO_PKG_VERSION"))),
            request_queue: Some(super::peer::MAX_UPLOAD_QUEUE as u32),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let m = Value::Dict(
            self.extensions
                .iter()
                .map(|(k, v)| (k.as_bytes().to_vec(), Value::Int(*v as i64)))
                .collect(),
        );
        let mut pairs = vec![("m", m)];
        if let Some(n) = self.metadata_size {
            pairs.push(("metadata_size", Value::Int(n as i64)));
        }
        if let Some(p) = self.listen_port {
            pairs.push(("p", Value::Int(p as i64)));
        }
        if let Some(v) = &self.client {
            pairs.push(("v", Value::text(v)));
        }
        if let Some(q) = self.request_queue {
            pairs.push(("reqq", Value::Int(q as i64)));
        }
        bencode::encode(&Value::Dict(
            pairs
                .into_iter()
                .map(|(k, v)| (k.as_bytes().to_vec(), v))
                .collect(),
        ))
    }

    pub fn decode(payload: &[u8]) -> Result<Self> {
        let doc = bencode::decode(payload)
            .context("invalid extension handshake")?
            .value;
        let mut extensions = BTreeMap::new();
        if let Some(Value::Dict(m)) = doc.get("m") {
            for (name, id) in m {
                if let (Ok(name), Some(id)) = (String::from_utf8(name.clone()), id.int())
                    && (0..=255).contains(&id)
                    && name.len() <= 32
                    && extensions.len() < 32
                {
                    extensions.insert(name, id as u8);
                }
            }
        }
        let number = |key: &str| doc.get(key).and_then(Value::int);
        Ok(Self {
            extensions,
            metadata_size: number("metadata_size").and_then(|n| u32::try_from(n).ok()),
            listen_port: number("p")
                .and_then(|n| u16::try_from(n).ok())
                .filter(|p| *p != 0),
            client: doc
                .get("v")
                .and_then(Value::str)
                .map(|s| s.chars().take(64).collect()),
            request_queue: number("reqq").and_then(|n| u32::try_from(n).ok()),
        })
    }
}

/// BEP 9 metadata exchange messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataMessage {
    Request {
        piece: u32,
    },
    Data {
        piece: u32,
        total_size: u32,
        data: Vec<u8>,
    },
    Reject {
        piece: u32,
    },
}

pub const METADATA_PIECE: usize = 16 * 1024;

impl MetadataMessage {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            MetadataMessage::Request { piece } => bencode::encode(&dict([
                ("msg_type", Value::Int(0)),
                ("piece", Value::Int(*piece as i64)),
            ])),
            MetadataMessage::Reject { piece } => bencode::encode(&dict([
                ("msg_type", Value::Int(2)),
                ("piece", Value::Int(*piece as i64)),
            ])),
            MetadataMessage::Data {
                piece,
                total_size,
                data,
            } => {
                let mut out = bencode::encode(&dict([
                    ("msg_type", Value::Int(1)),
                    ("piece", Value::Int(*piece as i64)),
                    ("total_size", Value::Int(*total_size as i64)),
                ]));
                out.extend(data);
                out
            }
        }
    }

    pub fn decode(payload: &[u8]) -> Result<Self> {
        let (doc, used) = bencode::decode_prefix(payload).context("invalid metadata message")?;
        let piece = u32::try_from(doc.get("piece").and_then(Value::int).context("no piece")?)?;
        match doc.get("msg_type").and_then(Value::int) {
            Some(0) => Ok(MetadataMessage::Request { piece }),
            Some(2) => Ok(MetadataMessage::Reject { piece }),
            Some(1) => {
                let total_size = u32::try_from(
                    doc.get("total_size")
                        .and_then(Value::int)
                        .context("no total_size")?,
                )?;
                let data = payload[used..].to_vec();
                if data.len() > METADATA_PIECE {
                    bail!("metadata piece is larger than 16 KiB");
                }
                Ok(MetadataMessage::Data {
                    piece,
                    total_size,
                    data,
                })
            }
            other => bail!("unknown metadata message type {other:?}"),
        }
    }
}

/// BEP 11 peer exchange.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Pex {
    pub added: Vec<SocketAddr>,
    pub dropped: Vec<SocketAddr>,
}

fn compact(addresses: &[SocketAddr]) -> (Vec<u8>, Vec<u8>) {
    let (mut v4, mut v6) = (Vec::new(), Vec::new());
    for a in addresses {
        match a.ip() {
            std::net::IpAddr::V4(ip) => {
                v4.extend(ip.octets());
                v4.extend(a.port().to_be_bytes());
            }
            std::net::IpAddr::V6(ip) => {
                v6.extend(ip.octets());
                v6.extend(a.port().to_be_bytes());
            }
        }
    }
    (v4, v6)
}

fn uncompact(v4: Option<&[u8]>, v6: Option<&[u8]>, limit: usize) -> Vec<SocketAddr> {
    let mut out = Vec::new();
    for c in v4.unwrap_or_default().chunks_exact(6) {
        let port = u16::from_be_bytes([c[4], c[5]]);
        if port != 0 {
            out.push(SocketAddr::from((
                Ipv4Addr::new(c[0], c[1], c[2], c[3]),
                port,
            )));
        }
    }
    for c in v6.unwrap_or_default().chunks_exact(18) {
        let octets: [u8; 16] = c[..16].try_into().expect("sixteen bytes");
        let port = u16::from_be_bytes([c[16], c[17]]);
        if port != 0 {
            out.push(SocketAddr::from((Ipv6Addr::from(octets), port)));
        }
    }
    out.truncate(limit);
    out
}

impl Pex {
    pub fn encode(&self) -> Vec<u8> {
        let (added, added6) = compact(&self.added);
        let (dropped, dropped6) = compact(&self.dropped);
        let mut pairs = vec![
            ("added", Value::Bytes(added)),
            (
                "added.f",
                Value::Bytes(vec![0; self.added.iter().filter(|a| a.is_ipv4()).count()]),
            ),
            ("dropped", Value::Bytes(dropped)),
        ];
        if !added6.is_empty() {
            pairs.push(("added6", Value::Bytes(added6)));
            pairs.push((
                "added6.f",
                Value::Bytes(vec![0; self.added.iter().filter(|a| a.is_ipv6()).count()]),
            ));
        }
        if !dropped6.is_empty() {
            pairs.push(("dropped6", Value::Bytes(dropped6)));
        }
        bencode::encode(&Value::Dict(
            pairs
                .into_iter()
                .map(|(k, v)| (k.as_bytes().to_vec(), v))
                .collect(),
        ))
    }

    pub fn decode(payload: &[u8]) -> Result<Self> {
        let doc = bencode::decode(payload)
            .context("invalid pex message")?
            .value;
        let bytes = |key: &str| doc.get(key).and_then(Value::bytes);
        Ok(Self {
            added: uncompact(bytes("added"), bytes("added6"), 200),
            dropped: uncompact(bytes("dropped"), bytes("dropped6"), 200),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_round_trips_and_advertises_capabilities() {
        let h = Handshake::new([1; 20], *b"-NX0200-123456789012", true);
        let bytes = h.encode();
        assert_eq!(&bytes[..20], b"\x13BitTorrent protocol");
        let back = Handshake::decode(&bytes).unwrap();
        assert_eq!(back, h);
        assert!(back.supports_extensions() && back.supports_fast() && back.supports_dht());
        let plain = Handshake::new([1; 20], [2; 20], false);
        assert!(!plain.supports_dht());
        let mut bad = bytes;
        bad[3] = b'X';
        assert!(Handshake::decode(&bad).is_err());
    }

    #[test]
    fn every_message_round_trips() {
        let messages = [
            Message::KeepAlive,
            Message::Choke,
            Message::Unchoke,
            Message::Interested,
            Message::NotInterested,
            Message::Have(7),
            Message::Bitfield(vec![0xff, 0x80]),
            Message::Request {
                index: 1,
                begin: 16384,
                length: 16384,
            },
            Message::Piece {
                index: 1,
                begin: 0,
                data: vec![9; 100],
            },
            Message::Cancel {
                index: 1,
                begin: 0,
                length: 16384,
            },
            Message::Port(6881),
            Message::Suggest(3),
            Message::HaveAll,
            Message::HaveNone,
            Message::Reject {
                index: 2,
                begin: 0,
                length: 16384,
            },
            Message::AllowedFast(5),
            Message::Extended {
                id: 1,
                payload: vec![1, 2, 3],
            },
        ];
        for message in messages {
            let wire = message.encode();
            let length = u32::from_be_bytes(wire[..4].try_into().unwrap()) as usize;
            assert_eq!(wire.len(), 4 + length);
            assert_eq!(Message::decode(&wire[4..]).unwrap(), message);
        }
    }

    #[test]
    fn malformed_messages_are_rejected() {
        for bad in [
            &[0u8, 1][..],    // choke with payload
            &[4, 0, 0, 0],    // short have
            &[6, 0, 0, 0, 1], // short request
            &[7, 0, 0, 0, 1], // piece without a begin offset
            &[9, 1],          // short port
            &[20],            // empty extended message
        ] {
            assert!(Message::decode(bad).is_err(), "{bad:?}");
        }
        let oversized = [vec![7u8, 0, 0, 0, 0, 0, 0, 0, 0], vec![0; MAX_BLOCK + 1]].concat();
        assert!(Message::decode(&oversized).is_err());
    }

    #[tokio::test]
    async fn the_stream_reader_enforces_limits_and_skips_unknown_ids() {
        let mut wire = Message::Have(3).encode();
        wire.extend([0, 0, 0, 3, 99, 0, 0]); // unknown id 99 with two bytes of payload
        wire.extend(Message::Interested.encode());
        let mut reader = &wire[..];
        assert_eq!(
            read_message(&mut reader).await.unwrap(),
            Some(Message::Have(3))
        );
        assert_eq!(read_message(&mut reader).await.unwrap(), None);
        assert_eq!(
            read_message(&mut reader).await.unwrap(),
            Some(Message::Interested)
        );
        assert!(read_message(&mut reader).await.is_err(), "end of stream");

        let huge = (MAX_MESSAGE as u32 + 1).to_be_bytes();
        assert!(read_message(&mut &huge[..]).await.is_err());
    }

    #[test]
    fn extension_handshake_round_trips_and_ignores_junk() {
        let ours = ExtHandshake::ours(Some(12345), 6881);
        let back = ExtHandshake::decode(&ours.encode()).unwrap();
        assert_eq!(back.extensions["ut_metadata"], LOCAL_UT_METADATA);
        assert_eq!(back.metadata_size, Some(12345));
        assert_eq!(back.listen_port, Some(6881));
        assert!(back.client.unwrap().starts_with("Nibiru"));

        let junk = bencode::encode(&dict([
            (
                "m",
                dict([
                    ("ut_metadata", Value::Int(300)),
                    ("ok", Value::Int(4)),
                    ("neg", Value::Int(-1)),
                ]),
            ),
            ("p", Value::Int(0)),
            ("metadata_size", Value::Int(-5)),
        ]));
        let parsed = ExtHandshake::decode(&junk).unwrap();
        assert_eq!(parsed.extensions.len(), 1, "out-of-range IDs are dropped");
        assert_eq!(parsed.extensions["ok"], 4);
        assert_eq!(parsed.listen_port, None);
        assert_eq!(parsed.metadata_size, None);
        assert!(ExtHandshake::decode(b"not bencode").is_err());
    }

    #[test]
    fn metadata_messages_carry_raw_data_after_the_dictionary() {
        let request = MetadataMessage::Request { piece: 2 };
        assert_eq!(MetadataMessage::decode(&request.encode()).unwrap(), request);
        let reject = MetadataMessage::Reject { piece: 2 };
        assert_eq!(MetadataMessage::decode(&reject.encode()).unwrap(), reject);
        let data = MetadataMessage::Data {
            piece: 1,
            total_size: 40_000,
            data: vec![0xaa; 1000],
        };
        assert_eq!(MetadataMessage::decode(&data.encode()).unwrap(), data);
        let too_big = MetadataMessage::Data {
            piece: 0,
            total_size: 1,
            data: vec![0; METADATA_PIECE + 1],
        };
        assert!(MetadataMessage::decode(&too_big.encode()).is_err());
        assert!(MetadataMessage::decode(b"d8:msg_typei9e5:piecei0ee").is_err());
    }

    #[test]
    fn pex_round_trips_v4_and_v6_and_drops_port_zero() {
        let pex = Pex {
            added: vec![
                "1.2.3.4:6881".parse().unwrap(),
                "[2001:db8::1]:51413".parse().unwrap(),
            ],
            dropped: vec!["5.6.7.8:6882".parse().unwrap()],
        };
        let back = Pex::decode(&pex.encode()).unwrap();
        assert_eq!(back, pex);
        let zero = bencode::encode(&dict([("added", Value::Bytes(vec![1, 2, 3, 4, 0, 0]))]));
        assert!(Pex::decode(&zero).unwrap().added.is_empty());
    }
}
