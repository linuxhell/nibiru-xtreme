use super::metadata::{Node, Reader, read_node};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, time::Duration};
use tokio::net::UdpSocket;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BootstrapResult {
    pub responder: Node,
    pub contacts: Vec<Node>,
}
fn response(bytes: &[u8], from: SocketAddr) -> Result<BootstrapResult> {
    if bytes.get(..2) != Some(&[0xe4, 0x09]) {
        bail!("unexpected Kad bootstrap response");
    }
    let mut r = Reader::new(&bytes[2..]);
    let id = r.take(16)?.try_into()?;
    let tcp_port = r.u16()?;
    let version = r.u8()?;
    let count = r.u16()?;
    if count > 1000 {
        bail!("too many bootstrap contacts");
    }
    let host = match from.ip() {
        std::net::IpAddr::V4(ip) => ip,
        _ => bail!("Kad2 bootstrap requires IPv4"),
    };
    let responder = Node {
        id,
        host,
        tcp_port,
        udp_port: from.port(),
        version,
    };
    let mut contacts = Vec::new();
    for _ in 0..count {
        let node = read_node(&mut r)?;
        if node.version >= 2
            && node.udp_port > 0
            && !node.host.is_unspecified()
            && !node.host.is_multicast()
        {
            contacts.push(node);
        }
    }
    r.done()?;
    Ok(BootstrapResult {
        responder,
        contacts,
    })
}
/// Contacts a known Kad2 node and validates its response. This is a bootstrap
/// probe, not yet a persistent Kad routing/publishing session.
pub async fn bootstrap(node: &Node) -> Result<BootstrapResult> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    let target = SocketAddr::from((node.host, node.udp_port));
    socket.connect(target).await?;
    socket.send(&[0xe4, 0x01]).await?;
    let mut bytes = vec![0; 32 * 1024];
    let length = tokio::time::timeout(Duration::from_secs(5), socket.recv(&mut bytes))
        .await
        .context("Kad bootstrap timed out")??;
    response(&bytes[..length], target)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn kad_bootstrap_validates_actual_udp_response() {
        let fixture = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = fixture.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let mut b = [0; 64];
            let (n, peer) = fixture.recv_from(&mut b).await.unwrap();
            assert_eq!(&b[..n], &[0xe4, 1]);
            let mut reply = vec![0xe4, 9];
            reply.extend([1; 16]);
            reply.extend(4662u16.to_le_bytes());
            reply.push(9);
            reply.extend(0u16.to_le_bytes());
            fixture.send_to(&reply, peer).await.unwrap();
        });
        let node = Node {
            id: [1; 16],
            host: std::net::Ipv4Addr::LOCALHOST,
            udp_port: port,
            tcp_port: 4662,
            version: 9,
        };
        let result = bootstrap(&node).await.unwrap();
        assert_eq!(result.responder.id, [1; 16]);
        assert!(result.contacts.is_empty());
        server.await.unwrap();
    }
}
