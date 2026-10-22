//! Kad UDP obfuscation against a real aMule daemon running Kad on loopback.
//!
//! Skipped (with a message) when `amuled`/`amulecmd` are not installed; CI installs them.
//! Nothing here reaches the public Kad network: the daemon has no bootstrap nodes.

use nibiru_xtreme::ed2k::{kad_session::KadSession, metadata::Node};
use std::{
    net::{Ipv4Addr, UdpSocket},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

fn installed(program: &str) -> bool {
    Command::new(program)
        .arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok()
}
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
struct Daemon(Child);
impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn nibiru_and_a_real_amule_verify_each_other_with_obfuscated_kad_packets() {
    if !installed("amuled") || !installed("amulecmd") {
        eprintln!("skipped: amuled/amulecmd are not installed");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("cfg");
    std::fs::create_dir_all(config.join("Incoming")).unwrap();
    std::fs::create_dir_all(config.join("Temp")).unwrap();
    let (tcp, udp, ec) = (free_port(), free_port(), free_port());
    // md5("pw")
    std::fs::write(
        config.join("amule.conf"),
        format!(
            "[eMule]\nNick=interop\nPort={tcp}\nUDPPort={udp}\nUDPEnable=1\nAutoconnect=0\nReconnect=0\nConnectToKad=1\nConnectToED2K=0\nTempDir={t}\nIncomingDir={i}\n[ExternalConnect]\nAcceptExternalConnections=1\nECAddress=127.0.0.1\nECPort={ec}\nECPassword=8fe4c11451281c094a6578e6ddbf5eed\n[Obfuscation]\nIsClientCryptLayerSupported=1\nIsCryptLayerRequested=1\nIsClientCryptLayerRequired=0\n",
            t = config.join("Temp").display(),
            i = config.join("Incoming").display()
        ),
    )
    .unwrap();
    let log = std::fs::File::create(root.path().join("amuled.log")).unwrap();
    let _daemon = Daemon(
        Command::new("amuled")
            .arg("-c")
            .arg(&config)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap(),
    );
    // Wait for the daemon, then ask it to start Kad.
    let mut started = false;
    for _ in 0..60 {
        // amulecmd exits 0 even when it cannot connect, so look at what it printed.
        let output = Command::new("amulecmd")
            .args([
                "-h",
                "127.0.0.1",
                "-p",
                &ec.to_string(),
                "-P",
                "pw",
                "-c",
                "connect kad",
            ])
            .output();
        if output
            .is_ok_and(|o| String::from_utf8_lossy(&o.stdout).contains("Operation was successful"))
        {
            started = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(started, "aMule never accepted the Kad start command");
    std::thread::sleep(Duration::from_secs(3));

    // Learn aMule's Kad ID the way any node would: from its answer to a plain HELLO.
    let probe = UdpSocket::bind("127.0.0.1:0").unwrap();
    probe
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut hello = vec![0xe4, 0x11];
    hello.extend([9u8; 16]);
    hello.extend(4662u16.to_le_bytes());
    hello.extend([5, 0]);
    let mut buffer = [0u8; 512];
    let mut amule_id = None;
    for _ in 0..20 {
        probe.send_to(&hello, (Ipv4Addr::LOCALHOST, udp)).unwrap();
        if let Ok((n, _)) = probe.recv_from(&mut buffer)
            && n >= 22
            && buffer[..2] == [0xe4, 0x19]
        {
            amule_id = Some(<[u8; 16]>::try_from(&buffer[2..18]).unwrap());
            break;
        }
    }
    let amule_id = amule_id.unwrap_or_else(|| {
        panic!(
            "aMule's Kad did not answer a plain HELLO on UDP {udp}; daemon log:\n{}",
            std::fs::read_to_string(root.path().join("amuled.log")).unwrap_or_default()
        )
    });

    let session = KadSession::start(
        [0x42; 16],
        4662,
        root.path().join("routing.json"),
        vec![Node {
            id: amule_id,
            host: Ipv4Addr::LOCALHOST,
            udp_port: udp,
            tcp_port: tcp,
            version: 8,
        }],
        true,
    )
    .await
    .unwrap();
    assert!(session.status().obfuscation);

    let deadline = Instant::now() + Duration::from_secs(40);
    while Instant::now() < deadline && session.status().verified_contacts == 0 {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let status = session.status();
    assert_eq!(
        status.verified_contacts, 1,
        "aMule was never verified (obfuscated out {}, in {})",
        status.obfuscated_out, status.obfuscated_in
    );
    assert!(
        status.obfuscated_out > 0,
        "we never sent an obfuscated packet"
    );
    assert!(status.obfuscated_in > 0, "aMule never answered obfuscated");
    session.stop();
}
