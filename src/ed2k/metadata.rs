//! Bounded, independently written readers for the ED2K/Kad interchange formats.
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::net::Ipv4Addr;

pub const MAX_METADATA: usize = 8 * 1024 * 1024;
const MAX_RECORDS: u32 = 10_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Server {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub description: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Node {
    pub id: [u8; 16],
    pub host: Ipv4Addr,
    pub udp_port: u16,
    pub tcp_port: u16,
    pub version: u8,
}

pub(crate) struct Reader<'a> {
    pub data: &'a [u8],
    pub pos: usize,
}
impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }
    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .context("metadata length overflow")?;
        let bytes = self.data.get(self.pos..end).context("truncated metadata")?;
        self.pos = end;
        Ok(bytes)
    }
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into()?))
    }
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into()?))
    }
    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into()?))
    }
    pub fn string(&mut self, n: usize) -> Result<String> {
        Ok(String::from_utf8_lossy(self.take(n)?)
            .trim_start_matches('\u{feff}')
            .to_owned())
    }
    pub fn done(&self) -> Result<()> {
        if self.pos != self.data.len() {
            bail!("unexpected trailing metadata");
        }
        Ok(())
    }
}

pub(crate) enum Value {
    Text(String),
    Number(u64),
    Other,
}
pub(crate) fn tag(reader: &mut Reader<'_>) -> Result<(u8, Value)> {
    let raw = reader.u8()?;
    let kind = raw & 0x7f;
    let id = if raw & 0x80 != 0 {
        reader.u8()?
    } else {
        let len = reader.u16()? as usize;
        if len == 1 {
            reader.u8()?
        } else {
            reader.take(len)?;
            0
        }
    };
    let value = match kind {
        1 => {
            reader.take(16)?;
            Value::Other
        }
        2 => {
            let len = reader.u16()? as usize;
            Value::Text(reader.string(len)?)
        }
        3 => Value::Number(reader.u32()? as u64),
        4 => {
            reader.take(4)?;
            Value::Other
        }
        5 | 9 => Value::Number(reader.u8()? as u64),
        6 => {
            let bits = reader.u16()? as usize;
            reader.take(bits / 8 + 1)?;
            Value::Other
        }
        7 => {
            let len = reader.u32()? as usize;
            reader.take(len)?;
            Value::Other
        }
        8 => Value::Number(reader.u16()? as u64),
        10 => {
            let len = reader.u8()? as usize;
            reader.take(len)?;
            Value::Other
        }
        11 => Value::Number(reader.u64()?),
        0x11..=0x26 => Value::Text(reader.string((kind - 0x10) as usize)?),
        _ => bail!("unknown ED2K tag type"),
    };
    Ok((id, value))
}

pub fn parse_servers(bytes: &[u8]) -> Result<Vec<Server>> {
    if bytes.len() > MAX_METADATA {
        bail!("server.met exceeds size limit");
    }
    let mut r = Reader::new(bytes);
    if !matches!(r.u8()?, 0xe0 | 0x0e) {
        bail!("invalid server.met header");
    }
    let count = r.u32()?;
    if count > MAX_RECORDS {
        bail!("too many servers");
    }
    let mut servers = Vec::new();
    for _ in 0..count {
        let ip = Ipv4Addr::from(<[u8; 4]>::try_from(r.take(4)?)?);
        let port = r.u16()?;
        let tags = r.u32()?;
        if tags > 256 {
            bail!("too many server tags");
        }
        let mut server = Server {
            host: ip.to_string(),
            port,
            name: String::new(),
            description: String::new(),
        };
        for _ in 0..tags {
            let (id, value) = tag(&mut r)?;
            if let Value::Text(text) = value {
                match id {
                    1 => server.name = text,
                    0x0b => server.description = text,
                    0x85 if !text.is_empty() => server.host = text,
                    _ => {}
                }
            }
        }
        if port == 0 || server.host.chars().any(char::is_control) || server.host.len() > 253 {
            continue;
        }
        if ip.is_unspecified() && server.host == ip.to_string() {
            continue;
        }
        if server.name.is_empty() {
            server.name = server.host.clone();
        }
        if !servers
            .iter()
            .any(|s: &Server| s.host == server.host && s.port == port)
        {
            servers.push(server);
        }
    }
    r.done()?;
    Ok(servers)
}

pub(crate) fn read_node(r: &mut Reader<'_>) -> Result<Node> {
    let id = r.take(16)?.try_into()?;
    // Kad stores this IPv4 value in network byte order inside a little-endian uint32.
    let host = Ipv4Addr::from(r.u32()?.to_be_bytes());
    Ok(Node {
        id,
        host,
        udp_port: r.u16()?,
        tcp_port: r.u16()?,
        version: r.u8()?,
    })
}

pub fn parse_nodes(bytes: &[u8]) -> Result<Vec<Node>> {
    if bytes.len() > MAX_METADATA {
        bail!("nodes.dat exceeds size limit");
    }
    let mut r = Reader::new(bytes);
    if r.u32()? != 0 {
        bail!("legacy Kad1 nodes.dat is not supported");
    }
    let version = r.u32()?;
    if !(1..=3).contains(&version) {
        bail!("unsupported nodes.dat version");
    }
    let bootstrap = version == 3 && r.u32()? == 1;
    let count = r.u32()?;
    if count > MAX_RECORDS {
        bail!("too many Kad nodes");
    }
    let mut nodes = Vec::new();
    for _ in 0..count {
        let node = read_node(&mut r)?;
        if version >= 2 && !bootstrap {
            r.take(9)?;
        }
        if node.version < 2
            || node.udp_port == 0
            || node.host.is_unspecified()
            || node.host.is_multicast()
        {
            continue;
        }
        if !nodes
            .iter()
            .any(|n: &Node| n.host == node.host && n.udp_port == node.udp_port)
        {
            nodes.push(node);
        }
    }
    r.done()?;
    Ok(nodes)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn metadata_rejects_truncation_and_count_bombs() {
        assert!(parse_servers(&[0xe0, 255, 255, 255, 255]).is_err());
        assert!(parse_nodes(&[0; 9]).is_err());
        let mut valid = vec![0xe0];
        valid.extend(1u32.to_le_bytes());
        valid.extend([1, 2, 3, 4]);
        valid.extend(4661u16.to_le_bytes());
        valid.extend(0u32.to_le_bytes());
        assert_eq!(parse_servers(&valid).unwrap()[0].host, "1.2.3.4");
        for i in 0..valid.len() {
            assert!(parse_servers(&valid[..i]).is_err());
        }
    }
    #[test]
    fn reads_kad2_and_bootstrap_format_with_correct_ipv4_order() {
        for version in [1u32, 2, 3] {
            let mut b = Vec::new();
            b.extend(0u32.to_le_bytes());
            b.extend(version.to_le_bytes());
            if version == 3 {
                b.extend(1u32.to_le_bytes());
            }
            b.extend(1u32.to_le_bytes());
            b.extend([1; 16]);
            b.extend([4, 3, 2, 1]);
            b.extend(4672u16.to_le_bytes());
            b.extend(4662u16.to_le_bytes());
            b.push(9);
            if version == 2 {
                b.extend([0; 9]);
            }
            let nodes = parse_nodes(&b).unwrap();
            assert_eq!(nodes[0].host.to_string(), "1.2.3.4");
            for i in 0..b.len() {
                assert!(parse_nodes(&b[..i]).is_err());
            }
        }
    }
}
