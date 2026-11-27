#![cfg(feature = "http3")]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_http3_reuses_initial_range_for_the_segmented_payload() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let root = tempfile::tempdir().unwrap();
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let ca = root.path().join("root.der");
    std::fs::write(&ca, certified.cert.der()).unwrap();
    let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![certified.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der()).into(),
    )
    .unwrap();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let endpoint = quinn::Endpoint::server(
        quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
        )),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let address = endpoint.local_addr().unwrap();
    let data = Arc::new(
        (0..9 * 1024 * 1024 + 321)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = requests.clone();
    let source = data.clone();
    let listener = endpoint.clone();
    let fixture = tokio::spawn(async move {
        while let Some(incoming) = listener.accept().await {
            let connection = incoming.await.unwrap();
            let source = source.clone();
            let counted = counted.clone();
            tokio::spawn(async move {
                let mut server = h3::server::Connection::new(h3_quinn::Connection::new(connection))
                    .await
                    .unwrap();
                while let Ok(Some(resolver)) = server.accept().await {
                    let (request, mut stream) = resolver.resolve_request().await.unwrap();
                    let source = source.clone();
                    counted.fetch_add(1, Ordering::Relaxed);
                    tokio::spawn(async move {
                        assert_eq!(request.method(), "GET");
                        let range = request.headers()["range"]
                            .to_str()
                            .unwrap()
                            .strip_prefix("bytes=")
                            .unwrap();
                        let (start, end) = range.split_once('-').unwrap();
                        let start = start.parse::<usize>().unwrap();
                        let end = end.parse::<usize>().unwrap().min(source.len() - 1);
                        let response = http::Response::builder()
                            .status(206)
                            .header(
                                "content-range",
                                format!("bytes {start}-{end}/{}", source.len()),
                            )
                            .header("content-length", end - start + 1)
                            .header("etag", "\"h3-v1\"")
                            .body(())
                            .unwrap();
                        stream.send_response(response).await.unwrap();
                        stream
                            .send_data(bytes::Bytes::copy_from_slice(&source[start..=end]))
                            .await
                            .unwrap();
                        stream.finish().await.unwrap();
                    });
                }
            });
        }
    });
    let destination = root.path().join("result.bin");
    let log = root.path().join("events.jsonl");
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_nibiru-xtreme"))
            .arg("download")
            .arg(format!("https://{address}/payload.bin"))
            .arg("--out")
            .arg(&destination)
            .arg("--ca-certificate")
            .arg(ca)
            .arg("--log")
            .arg(&log)
            .env_remove("HTTP_PROXY")
            .env_remove("HTTPS_PROXY")
            .env_remove("ALL_PROXY")
            .env_remove("http_proxy")
            .env_remove("https_proxy")
            .env_remove("all_proxy")
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(std::fs::read(destination).unwrap(), *data);
    let log = std::fs::read_to_string(log).unwrap();
    assert!(log.contains("protocol=h3"));
    assert!(log.contains("http3_ranges=3"));
    assert_eq!(
        requests.load(Ordering::Relaxed),
        3,
        "the initial winning range must be reused without HEAD or duplicate GET"
    );
    fixture.abort();
    endpoint.close(0u32.into(), b"done");
}
