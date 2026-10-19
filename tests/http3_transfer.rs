use nibiru_xtreme::{
    engine::{DownloadRequest, Engine},
    logging::LogSink,
    protocols::TransportOptions,
};
use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

#[tokio::test]
async fn http3_transfers_over_quic_and_rejects_an_untrusted_certificate() {
    for trusted in [true, false] {
        let certified =
            rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "localhost".into()])
                .unwrap();
        let root = tempfile::tempdir().unwrap();
        let ca = root.path().join("ca.der");
        std::fs::write(&ca, certified.cert.der()).unwrap();
        let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![certified.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der())
                .into(),
        )
        .unwrap();
        tls.alpn_protocols = vec![b"h3".to_vec()];
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
        let endpoint = quinn::Endpoint::server(
            quinn::ServerConfig::with_crypto(Arc::new(crypto)),
            "127.0.0.1:0".parse().unwrap(),
        )
        .unwrap();
        let address = endpoint.local_addr().unwrap();
        let data = Arc::new((0..600_003).map(|i| (i % 251) as u8).collect::<Vec<_>>());
        let fixture_data = data.clone();
        let fixture_endpoint = endpoint.clone();
        let fixture = tokio::spawn(async move {
            let connection = match fixture_endpoint.accept().await.unwrap().await {
                Ok(c) => c,
                Err(_) => return,
            };
            let mut server = h3::server::Connection::new(h3_quinn::Connection::new(connection))
                .await
                .unwrap();
            while let Ok(Some(resolver)) = server.accept().await {
                let (request, mut stream) = resolver.resolve_request().await.unwrap();
                assert_eq!(request.uri().path(), "/payload.bin");
                let data = fixture_data.clone();
                tokio::spawn(async move {
                    stream
                        .send_response(
                            http::Response::builder()
                                .status(200)
                                .header("content-length", data.len().to_string())
                                .body(())
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                    stream
                        .send_data(bytes::Bytes::copy_from_slice(&data))
                        .await
                        .unwrap();
                    stream.finish().await.unwrap();
                });
            }
        });
        let destination = root.path().join("result.bin");
        let options = TransportOptions {
            ca_certificate: trusted.then_some(ca),
            ..Default::default()
        };
        let engine = Engine::new(Arc::new(LogSink::new(None).unwrap()))
            .unwrap()
            .with_http3_only(true)
            .with_transport_options(options);
        let result = tokio::time::timeout(
            Duration::from_secs(15),
            engine.download(
                DownloadRequest {
                    url: format!("https://{address}/payload.bin"),
                    destination: destination.clone(),
                },
                Arc::new(AtomicBool::new(false)),
                |_| {},
            ),
        )
        .await
        .unwrap();
        assert_eq!(result.is_ok(), trusted, "{result:?}");
        if trusted {
            assert_eq!(std::fs::read(&destination).unwrap(), *data);
        } else {
            assert!(!destination.exists());
        }
        fixture.abort();
        endpoint.close(0u32.into(), b"test complete");
    }
}
#[tokio::test]
async fn http3_does_not_silently_use_plain_http() {
    let root = tempfile::tempdir().unwrap();
    let output = root.path().join("file");
    let engine = Engine::new(Arc::new(LogSink::new(None).unwrap()))
        .unwrap()
        .with_http3_only(true);
    let error = engine
        .download(
            DownloadRequest {
                url: "http://127.0.0.1:9/file".into(),
                destination: output.clone(),
            },
            Arc::new(AtomicBool::new(false)),
            |_| {},
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("requires HTTPS"));
    assert!(!output.exists());
}
