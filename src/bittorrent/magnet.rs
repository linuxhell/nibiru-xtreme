//! Magnet links (BEP 9): `magnet:?xt=urn:btih:<hash>&dn=<name>&tr=<tracker>...`.

use super::metainfo::sanitize_component;
use anyhow::{Context, Result, bail};
use std::net::SocketAddr;

const MAX_LINK: usize = 8192;
const MAX_TRACKERS: usize = 64;
const MAX_PEERS: usize = 32;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Magnet {
    pub info_hash: [u8; 20],
    pub v2_info_hash: Option<[u8; 32]>,
    pub name: Option<String>,
    pub trackers: Vec<String>,
    pub web_seeds: Vec<String>,
    /// `x.pe` peer hints.
    pub peers: Vec<SocketAddr>,
}

fn hex_digit(c: u8) -> Option<u8> {
    (c as char).to_digit(16).map(|d| d as u8)
}

fn decode_hex<const N: usize>(text: &str) -> Option<[u8; N]> {
    let bytes = text.as_bytes();
    if bytes.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for (i, pair) in bytes.chunks_exact(2).enumerate() {
        out[i] = hex_digit(pair[0])? << 4 | hex_digit(pair[1])?;
    }
    Some(out)
}

/// RFC 4648 base32 without padding, as used by older magnet links (32 characters).
fn decode_base32(text: &str) -> Option<[u8; 20]> {
    if text.len() != 32 {
        return None;
    }
    let mut out = [0u8; 20];
    let (mut buffer, mut bits, mut index) = (0u32, 0, 0);
    for c in text.bytes() {
        let value = match c.to_ascii_uppercase() {
            c @ b'A'..=b'Z' => c - b'A',
            c @ b'2'..=b'7' => c - b'2' + 26,
            _ => return None,
        } as u32;
        buffer = (buffer << 5) | value;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out[index] = (buffer >> bits) as u8;
            index += 1;
            buffer &= (1 << bits) - 1;
        }
    }
    (index == 20).then_some(out)
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len()
                && hex_digit(bytes[i + 1]).is_some()
                && hex_digit(bytes[i + 2]).is_some() =>
            {
                out.push(hex_digit(bytes[i + 1]).unwrap() << 4 | hex_digit(bytes[i + 2]).unwrap());
                i += 2;
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

impl Magnet {
    pub fn parse(link: &str) -> Result<Self> {
        if link.len() > MAX_LINK {
            bail!("magnet link is too long");
        }
        let link = link.trim();
        let query = link
            .strip_prefix("magnet:?")
            .or_else(|| link.strip_prefix("MAGNET:?"))
            .context("not a magnet link")?;
        let mut magnet = Magnet::default();
        let mut v1_hash = None;
        for pair in query.split('&') {
            let Some((key, value)) = pair.split_once('=') else {
                continue;
            };
            let value = percent_decode(value);
            match key.split('.').next().unwrap_or(key) {
                "xt" if key == "xt" || key.starts_with("xt.") => {
                    if let Some(hash) = value.strip_prefix("urn:btih:") {
                        let parsed = decode_hex(hash).or_else(|| decode_base32(hash));
                        let hash = parsed.context("invalid info hash in magnet link")?;
                        if v1_hash.is_some_and(|old| old != hash) {
                            bail!("conflicting v1 magnet hashes");
                        }
                        v1_hash = Some(hash);
                    } else if let Some(hash) = value.strip_prefix("urn:btmh:1220") {
                        let hash =
                            decode_hex::<32>(hash).context("invalid v2 SHA-256 multihash")?;
                        if magnet.v2_info_hash.is_some_and(|old| old != hash) {
                            bail!("conflicting v2 magnet hashes");
                        }
                        magnet.v2_info_hash = Some(hash);
                    }
                }
                "dn" if key == "dn" => {
                    let name = sanitize_component(value.trim());
                    if !value.trim().is_empty() {
                        magnet.name = Some(name);
                    }
                }
                "tr" if key == "tr" || key.starts_with("tr.") => {
                    let lower = value.to_ascii_lowercase();
                    let ok = ["http://", "https://", "udp://"]
                        .iter()
                        .any(|p| lower.starts_with(p));
                    if ok
                        && value.len() <= 512
                        && magnet.trackers.len() < MAX_TRACKERS
                        && !magnet.trackers.contains(&value)
                    {
                        magnet.trackers.push(value);
                    }
                }
                "ws" if key == "ws" => {
                    let lower = value.to_ascii_lowercase();
                    if (lower.starts_with("http://") || lower.starts_with("https://"))
                        && value.len() <= 512
                        && magnet.web_seeds.len() < 8
                    {
                        magnet.web_seeds.push(value);
                    }
                }
                "x" if key == "x.pe" => {
                    if let Ok(address) = value.parse::<SocketAddr>()
                        && magnet.peers.len() < MAX_PEERS
                        && address.port() != 0
                    {
                        magnet.peers.push(address);
                    }
                }
                _ => {}
            }
        }
        magnet.info_hash = match (v1_hash, magnet.v2_info_hash) {
            (_, Some(hash)) => hash[..20].try_into()?,
            (Some(hash), None) => hash,
            _ => bail!("magnet link has no BitTorrent info hash (btih/btmh)"),
        };
        Ok(magnet)
    }

    pub fn info_hash_hex(&self) -> String {
        self.info_hash.iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEX: &str = "c12fe1c06bba254a9dc9f519b335aa7c1367a88a";

    #[test]
    fn parses_hex_hashes_trackers_names_and_hints() {
        let link = format!(
            "magnet:?xt=urn:btih:{HEX}&dn=Ubuntu+24.04%20ISO&tr=udp%3A%2F%2Ftracker.test%3A6969%2Fannounce&tr=http://t2.test/a&tr=udp%3A%2F%2Ftracker.test%3A6969%2Fannounce&ws=https%3A%2F%2Fmirror.test%2Ff&x.pe=192.0.2.5:6881"
        );
        let m = Magnet::parse(&link).unwrap();
        assert_eq!(m.info_hash_hex(), HEX);
        assert_eq!(m.name.as_deref(), Some("Ubuntu 24.04 ISO"));
        assert_eq!(
            m.trackers,
            vec!["udp://tracker.test:6969/announce", "http://t2.test/a"]
        );
        assert_eq!(m.web_seeds, vec!["https://mirror.test/f"]);
        assert_eq!(
            m.peers,
            vec!["192.0.2.5:6881".parse::<SocketAddr>().unwrap()]
        );
    }

    #[test]
    fn accepts_base32_hashes() {
        // The same 20 bytes in RFC 4648 base32.
        let hash = decode_hex(HEX).unwrap();
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
        let (mut buffer, mut bits, mut text) = (0u32, 0, String::new());
        for byte in hash {
            buffer = (buffer << 8) | byte as u32;
            bits += 8;
            while bits >= 5 {
                bits -= 5;
                text.push(alphabet[((buffer >> bits) & 31) as usize] as char);
            }
        }
        assert_eq!(text.len(), 32);
        let m = Magnet::parse(&format!(
            "magnet:?xt=urn:btih:{}",
            text.to_ascii_lowercase()
        ))
        .unwrap();
        assert_eq!(m.info_hash, hash);
    }

    #[test]
    fn rejects_links_without_a_usable_hash() {
        for bad in [
            "http://example.test/".to_string(),
            "magnet:?dn=only-a-name".into(),
            "magnet:?xt=urn:btih:zzzz".into(),
            format!("magnet:?xt=urn:btih:{}", &HEX[..39]),
            "magnet:?xt=urn:sha1:abcdef".into(),
            format!("magnet:?xt=urn:btih:{HEX}&{}", "a=b&".repeat(3000)),
        ] {
            assert!(Magnet::parse(&bad).is_err(), "{bad:.60}");
        }
    }

    #[test]
    fn untrusted_fields_are_filtered() {
        let m = Magnet::parse(&format!(
            "magnet:?xt=urn:btih:{HEX}&dn=..%2F..%2Fetc&tr=file%3A%2F%2F%2Fetc%2Fpasswd&tr=javascript:1&ws=ftp%3A%2F%2Fx&x.pe=not-an-address&x.pe=1.2.3.4:0"
        ))
        .unwrap();
        assert_eq!(m.name.as_deref(), Some(".._.._etc"));
        assert!(m.trackers.is_empty() && m.web_seeds.is_empty() && m.peers.is_empty());
    }

    #[test]
    fn v2_multihashes_preserve_all_bytes_and_reject_conflicts() {
        let hash = "57".repeat(32);
        let link = format!("magnet:?xt=urn:btih:{HEX}&xt=urn:btmh:1220{hash}");
        let magnet = Magnet::parse(&link).unwrap();
        assert_eq!(magnet.v2_info_hash, Some([0x57; 32]));
        assert_eq!(magnet.info_hash, [0x57; 20]);
        assert!(Magnet::parse(&format!("{link}&xt=urn:btmh:1220{}", "58".repeat(32))).is_err());
        assert!(Magnet::parse(&format!("magnet:?xt=urn:btmh:1220{}", &hash[..62])).is_err());
        assert!(Magnet::parse(&format!("magnet:?xt=urn:btmh:1114{HEX}")).is_err());
    }

    #[test]
    fn tracker_count_is_bounded() {
        let many: String = (0..200)
            .map(|i| format!("&tr=http://t{i}.test/a"))
            .collect();
        let m = Magnet::parse(&format!("magnet:?xt=urn:btih:{HEX}{many}")).unwrap();
        assert_eq!(m.trackers.len(), MAX_TRACKERS);
    }
}
