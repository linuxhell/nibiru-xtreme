//! UPnP port mapping against an in-process router that speaks the real protocol: SSDP discovery
//! answered from loopback, the device and service descriptions over HTTP, and SOAP actions.
#![cfg(feature = "upnp")]

use igd_next::SearchOptions;
use nibiru_xtreme::upnp::{Mapping, Proto, Timing, Upnp};
use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
    task::JoinHandle,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Entry {
    protocol: String,
    external: u16,
    client: String,
    internal: u16,
    lease: u32,
}

struct Router {
    ssdp: SocketAddr,
    entries: Arc<Mutex<Vec<Entry>>>,
    adds: Arc<Mutex<Vec<Entry>>>,
    deletes: Arc<Mutex<Vec<(String, u16)>>>,
    permanent_only: Arc<AtomicBool>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for Router {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

const DEVICE: &str = r#"<?xml version="1.0"?>
<root xmlns="urn:schemas-upnp-org:device-1-0"><specVersion><major>1</major><minor>0</minor></specVersion>
<device><deviceType>urn:schemas-upnp-org:device:InternetGatewayDevice:1</deviceType><friendlyName>Fake router</friendlyName>
<deviceList><device><deviceType>urn:schemas-upnp-org:device:WANDevice:1</deviceType><friendlyName>WAN</friendlyName>
<deviceList><device><deviceType>urn:schemas-upnp-org:device:WANConnectionDevice:1</deviceType><friendlyName>WANConn</friendlyName>
<serviceList><service><serviceType>urn:schemas-upnp-org:service:WANIPConnection:1</serviceType><serviceId>urn:upnp-org:serviceId:WANIPConn1</serviceId><controlURL>/ctl/IPConn</controlURL><eventSubURL>/evt/IPConn</eventSubURL><SCPDURL>/WANIPCn.xml</SCPDURL></service></serviceList>
</device></deviceList></device></deviceList></device></root>"#;

fn argument(name: &str, direction: &str) -> String {
    format!("<argument><name>{name}</name><direction>{direction}</direction></argument>")
}

fn scpd() -> String {
    let add = [
        "NewRemoteHost",
        "NewExternalPort",
        "NewProtocol",
        "NewInternalPort",
        "NewInternalClient",
        "NewEnabled",
        "NewPortMappingDescription",
        "NewLeaseDuration",
    ]
    .map(|n| argument(n, "in"))
    .join("");
    let delete = ["NewRemoteHost", "NewExternalPort", "NewProtocol"]
        .map(|n| argument(n, "in"))
        .join("");
    format!(
        r#"<?xml version="1.0"?><scpd xmlns="urn:schemas-upnp-org:service-1-0"><actionList>
<action><name>AddPortMapping</name><argumentList>{add}</argumentList></action>
<action><name>DeletePortMapping</name><argumentList>{delete}</argumentList></action>
<action><name>GetExternalIPAddress</name><argumentList>{}</argumentList></action>
</actionList></scpd>"#,
        argument("NewExternalIPAddress", "out")
    )
}

fn soap(body: &str) -> String {
    format!(
        r#"<?xml version="1.0"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><s:Body>{body}</s:Body></s:Envelope>"#
    )
}

fn tag(body: &str, name: &str) -> String {
    let open = format!("<{name}>");
    let start = body.find(&open).map(|i| i + open.len()).unwrap_or(0);
    let end = body[start..].find('<').map(|i| start + i).unwrap_or(start);
    body[start..end].to_owned()
}

async fn http(
    mut stream: tokio::net::TcpStream,
    entries: Arc<Mutex<Vec<Entry>>>,
    adds: Arc<Mutex<Vec<Entry>>>,
    deletes: Arc<Mutex<Vec<(String, u16)>>>,
    permanent_only: Arc<AtomicBool>,
) {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let (head, mut body) = loop {
        let Ok(read) = stream.read(&mut chunk).await else {
            return;
        };
        if read == 0 {
            return;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            break (
                String::from_utf8_lossy(&buffer[..end]).into_owned(),
                buffer[end + 4..].to_vec(),
            );
        }
    };
    let length = head
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
        })
        .unwrap_or(0);
    while body.len() < length {
        let Ok(read) = stream.read(&mut chunk).await else {
            return;
        };
        if read == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    let body = String::from_utf8_lossy(&body).into_owned();
    let first = head.lines().next().unwrap_or_default().to_owned();
    let (status, content) = if first.starts_with("GET /rootDesc.xml") {
        ("200 OK", DEVICE.to_owned())
    } else if first.starts_with("GET /WANIPCn.xml") {
        ("200 OK", scpd())
    } else if head.contains("#AddPortMapping") {
        let entry = Entry {
            protocol: tag(&body, "NewProtocol"),
            external: tag(&body, "NewExternalPort").parse().unwrap_or(0),
            client: tag(&body, "NewInternalClient"),
            internal: tag(&body, "NewInternalPort").parse().unwrap_or(0),
            lease: tag(&body, "NewLeaseDuration").parse().unwrap_or(0),
        };
        if permanent_only.load(Ordering::SeqCst) && entry.lease != 0 {
            (
                "500 Internal Server Error",
                soap(
                    r#"<s:Fault><faultcode>s:Client</faultcode><faultstring>UPnPError</faultstring><detail><UPnPError xmlns="urn:schemas-upnp-org:control-1-0"><errorCode>725</errorCode><errorDescription>OnlyPermanentLeasesSupported</errorDescription></UPnPError></detail></s:Fault>"#,
                ),
            )
        } else {
            adds.lock().unwrap().push(entry.clone());
            let mut entries = entries.lock().unwrap();
            entries.retain(|e| !(e.protocol == entry.protocol && e.external == entry.external));
            entries.push(entry);
            (
                "200 OK",
                soap(
                    r#"<u:AddPortMappingResponse xmlns:u="urn:schemas-upnp-org:service:WANIPConnection:1"/>"#,
                ),
            )
        }
    } else if head.contains("#DeletePortMapping") {
        let protocol = tag(&body, "NewProtocol");
        let external: u16 = tag(&body, "NewExternalPort").parse().unwrap_or(0);
        deletes.lock().unwrap().push((protocol.clone(), external));
        entries
            .lock()
            .unwrap()
            .retain(|e| !(e.protocol == protocol && e.external == external));
        (
            "200 OK",
            soap(
                r#"<u:DeletePortMappingResponse xmlns:u="urn:schemas-upnp-org:service:WANIPConnection:1"/>"#,
            ),
        )
    } else if head.contains("#GetExternalIPAddress") {
        (
            "200 OK",
            soap(
                r#"<u:GetExternalIPAddressResponse xmlns:u="urn:schemas-upnp-org:service:WANIPConnection:1"><NewExternalIPAddress>203.0.113.7</NewExternalIPAddress></u:GetExternalIPAddressResponse>"#,
            ),
        )
    } else {
        ("404 Not Found", String::new())
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{content}",
        content.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

/// `location_host` lets a test advertise a different host than the one that answers.
async fn router(location_host: Option<&str>) -> Router {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let http_port = listener.local_addr().unwrap().port();
    let entries = Arc::new(Mutex::new(Vec::new()));
    let adds = Arc::new(Mutex::new(Vec::new()));
    let deletes = Arc::new(Mutex::new(Vec::new()));
    let permanent_only = Arc::new(AtomicBool::new(false));
    let (e, a, d, p) = (
        entries.clone(),
        adds.clone(),
        deletes.clone(),
        permanent_only.clone(),
    );
    let server = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(http(stream, e.clone(), a.clone(), d.clone(), p.clone()));
        }
    });
    let socket = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    let ssdp = socket.local_addr().unwrap();
    let host = location_host.unwrap_or("127.0.0.1").to_owned();
    let responder = tokio::spawn(async move {
        let mut buffer = [0u8; 2048];
        loop {
            let Ok((_, from)) = socket.recv_from(&mut buffer).await else {
                return;
            };
            let reply = format!(
                "HTTP/1.1 200 OK\r\nCACHE-CONTROL: max-age=120\r\nLOCATION: http://{host}:{http_port}/rootDesc.xml\r\nST: urn:schemas-upnp-org:device:InternetGatewayDevice:1\r\nUSN: uuid:fake::urn:schemas-upnp-org:device:InternetGatewayDevice:1\r\n\r\n"
            );
            let _ = socket.send_to(reply.as_bytes(), from).await;
        }
    });
    Router {
        ssdp,
        entries,
        adds,
        deletes,
        permanent_only,
        tasks: vec![server, responder],
    }
}

fn client(ssdp: SocketAddr, renew: Duration) -> Arc<Upnp> {
    Upnp::with(
        "Nibiru test",
        Timing {
            lease_secs: 3600,
            renew,
            search: Duration::from_secs(2),
        },
        Box::new(move || {
            let mut options = SearchOptions::default();
            options.bind_addr = "127.0.0.1:0".parse().unwrap();
            options.broadcast_address = ssdp;
            options.timeout = Some(Duration::from_secs(2));
            options.single_search_timeout = Some(Duration::from_millis(600));
            // igd-next refuses loopback routers unless they are listed; the fake lives there.
            options.allowed_gateway_ips = vec!["127.0.0.1".parse().unwrap()];
            options
        }),
    )
}

fn tcp(port: u16) -> Mapping {
    Mapping {
        proto: Proto::Tcp,
        port,
    }
}
fn udp(port: u16) -> Mapping {
    Mapping {
        proto: Proto::Udp,
        port,
    }
}

#[tokio::test]
async fn wanted_ports_are_mapped_to_this_host_with_a_lease_and_reported() {
    let router = router(None).await;
    let upnp = client(router.ssdp, Duration::from_secs(3600));
    upnp.apply(vec![tcp(4662), udp(4672)]).await;
    let status = upnp.status();
    assert_eq!(status.state, "mapped", "{status:?}");
    assert_eq!(status.external_ip.as_deref(), Some("203.0.113.7"));
    assert_eq!(status.mappings, vec![tcp(4662), udp(4672)]);
    let entries = router.entries.lock().unwrap().clone();
    assert_eq!(entries.len(), 2);
    for entry in &entries {
        assert_eq!(entry.client, "127.0.0.1");
        assert_eq!(
            entry.external, entry.internal,
            "the external port equals the local one"
        );
        assert_eq!(entry.lease, 3600);
    }
    assert!(
        entries
            .iter()
            .any(|e| e.protocol == "TCP" && e.external == 4662)
    );
    assert!(
        entries
            .iter()
            .any(|e| e.protocol == "UDP" && e.external == 4672)
    );
    upnp.release().await;
}

#[tokio::test]
async fn ports_no_longer_wanted_are_removed_and_release_removes_everything() {
    let router = router(None).await;
    let upnp = client(router.ssdp, Duration::from_secs(3600));
    upnp.apply(vec![tcp(4662), udp(4672)]).await;
    upnp.apply(vec![tcp(4662), tcp(6881)]).await;
    assert_eq!(upnp.status().mappings, vec![tcp(4662), tcp(6881)]);
    {
        let entries = router.entries.lock().unwrap();
        assert!(
            !entries.iter().any(|e| e.protocol == "UDP"),
            "the unwanted UDP port was removed"
        );
        assert_eq!(entries.len(), 2);
    }
    upnp.release().await;
    assert!(
        router.entries.lock().unwrap().is_empty(),
        "nothing is left on the router"
    );
    assert_eq!(upnp.status().state, "idle");
    assert!(router.deletes.lock().unwrap().len() >= 3);
    // An empty request is a release too, and releasing twice is harmless.
    upnp.apply(vec![]).await;
    upnp.release().await;
}

#[tokio::test]
async fn mappings_are_renewed_while_they_are_wanted() {
    let router = router(None).await;
    let upnp = client(router.ssdp, Duration::from_millis(250));
    upnp.apply(vec![tcp(4662)]).await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let renewals = router.adds.lock().unwrap().len();
    assert!(
        renewals >= 3,
        "expected the mapping to be refreshed, saw {renewals} adds"
    );
    upnp.release().await;
    let after = router.adds.lock().unwrap().len();
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        router.adds.lock().unwrap().len(),
        after,
        "renewing stops after release"
    );
}

#[tokio::test]
async fn a_router_that_only_allows_permanent_leases_still_gets_the_mapping() {
    let router = router(None).await;
    router.permanent_only.store(true, Ordering::SeqCst);
    let upnp = client(router.ssdp, Duration::from_secs(3600));
    upnp.apply(vec![tcp(4662)]).await;
    assert_eq!(upnp.status().state, "mapped", "{:?}", upnp.status());
    assert_eq!(router.entries.lock().unwrap()[0].lease, 0);
    upnp.release().await;
    assert!(
        router.entries.lock().unwrap().is_empty(),
        "a permanent mapping is still removed"
    );
}

#[tokio::test]
async fn no_router_is_reported_without_creating_anything() {
    // A UDP port nobody answers on.
    let silent = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    let upnp = client(silent.local_addr().unwrap(), Duration::from_secs(3600));
    upnp.apply(vec![tcp(4662)]).await;
    let status = upnp.status();
    assert_eq!(status.state, "failed");
    assert!(status.error.unwrap().starts_with("upnp_no_gateway"));
    assert!(status.mappings.is_empty());
    upnp.release().await;
}

#[tokio::test]
async fn a_router_that_points_elsewhere_is_not_trusted() {
    // The reply comes from loopback but advertises another host for the control URLs.
    let router = router(Some("192.0.2.55")).await;
    let upnp = client(router.ssdp, Duration::from_secs(3600));
    upnp.apply(vec![tcp(4662)]).await;
    assert_eq!(upnp.status().state, "failed");
    assert!(router.adds.lock().unwrap().is_empty());
    upnp.release().await;
}
