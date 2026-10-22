#![cfg(feature = "ftp")]
use std::sync::Arc;
use suppaftp::tokio_rustls::TlsAcceptor;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::test]
async fn https_uses_system_trust_and_still_rejects_untrusted_or_wrong_host_certificates() {
    let certificate = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der());
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![certificate.cert.der().clone()], key.into())
    .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let payload = b"PK\x03\x04trusted HTTPS download";
    let server = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let mut tls = match acceptor.accept(socket).await {
                    Ok(tls) => tls,
                    Err(error) => {
                        eprintln!("fixture TLS handshake: {error}");
                        return;
                    }
                };
                let mut request = Vec::new();
                loop {
                    let mut buffer = [0; 1024];
                    let Ok(used) = tls.read(&mut buffer).await else {
                        return;
                    };
                    if used == 0 {
                        return;
                    }
                    request.extend_from_slice(&buffer[..used]);
                    if request.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                    if request.len() > 16 * 1024 {
                        return;
                    }
                }
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    payload.len()
                );
                if tls.write_all(header.as_bytes()).await.is_err() {
                    return;
                }
                if !request.starts_with(b"HEAD ") {
                    let _ = tls.write_all(payload).await;
                }
                let _ = tls.shutdown().await;
            });
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("trusted.pem");
    std::fs::write(&root, certificate.cert.pem()).unwrap();
    let unrelated = rcgen::generate_simple_self_signed(vec!["unrelated.test".into()]).unwrap();
    let other_root = dir.path().join("unrelated.pem");
    std::fs::write(&other_root, unrelated.cert.pem()).unwrap();
    for (name, host, ca, success) in [
        ("untrusted", "127.0.0.1", &other_root, false),
        ("trusted", "127.0.0.1", &root, true),
        ("wrong-host", "localhost", &root, false),
    ] {
        let destination = dir.path().join(format!("{name}.zip"));
        let result = tokio::process::Command::new(env!("CARGO_BIN_EXE_nibiru-xtreme"))
            .args([
                "download",
                &format!("https://{host}:{port}/payload?token=fixture-secret"),
                "--out",
            ])
            .arg(&destination)
            .env_remove("HTTP_PROXY")
            .env_remove("HTTPS_PROXY")
            .env_remove("ALL_PROXY")
            .env_remove("http_proxy")
            .env_remove("https_proxy")
            .env_remove("all_proxy")
            .env("SSL_CERT_FILE", ca)
            .env("SSL_CERT_DIR", dir.path().join("no-additional-roots"))
            .env("NO_PROXY", "*")
            .env("no_proxy", "*")
            .output()
            .await
            .unwrap();
        assert_eq!(
            result.status.success(),
            success,
            "{name}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        if success {
            assert_eq!(std::fs::read(destination).unwrap(), payload);
        } else {
            assert!(!destination.exists());
            assert!(!String::from_utf8_lossy(&result.stderr).contains("fixture-secret"));
        }
    }
    server.abort();
}
