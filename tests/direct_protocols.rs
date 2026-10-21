#![cfg(feature = "ftp")]
use anyhow::Result;
use nibiru_xtreme::{
    engine::{DownloadRequest, Engine},
    logging::LogSink,
    protocols::TransportOptions,
};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpListener,
};

fn engine(options: TransportOptions) -> Engine {
    Engine::new(Arc::new(LogSink::new(None).unwrap()))
        .unwrap()
        .with_transport_options(options)
}
fn payload() -> Arc<Vec<u8>> {
    Arc::new((0..600_003).map(|i| (i % 251) as u8).collect())
}
trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
type Connection = BufReader<Box<dyn Io>>;

#[derive(Clone, Copy)]
enum Fault {
    None,
    Truncated,
    RejectedCompletion,
    Stall,
    BadPassword,
    NoSize,
    /// Sends this many bytes of the requested range, then fails the transfer (426).
    DropAfter(usize),
    /// Answers REST with 502, so the client has to start over.
    NoRest,
    /// Accepts REST but still sends the file from byte 0.
    IgnoreRest,
}
type FaultCell = Arc<std::sync::Mutex<Fault>>;
struct FtpFixture {
    fault: FaultCell,
    /// Offsets received through REST, in order.
    rests: Arc<std::sync::Mutex<Vec<usize>>>,
    /// Changing it replaces the file served (same name), to simulate a changed remote file.
    data: Arc<std::sync::Mutex<Arc<Vec<u8>>>>,
    address: std::net::SocketAddr,
    cert: PathBuf,
    task: tokio::task::JoinHandle<()>,
    root: tempfile::TempDir,
}
impl Drop for FtpFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn ftp_fixture(secure: bool, implicit: bool, fault: Fault) -> FtpFixture {
    ftp_fixture_with(secure, implicit, fault, payload()).await
}
async fn ftp_fixture_with(
    secure: bool,
    implicit: bool,
    fault: Fault,
    data: Arc<Vec<u8>>,
) -> FtpFixture {
    let fault: FaultCell = Arc::new(std::sync::Mutex::new(fault));
    let rests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let data = Arc::new(std::sync::Mutex::new(data));
    let (fault_cell, rests_cell, data_cell) = (fault.clone(), rests.clone(), data.clone());
    let root = tempfile::tempdir().unwrap();
    let certified =
        rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
    let cert = root.path().join("ca.der");
    std::fs::write(&cert, certified.cert.der()).unwrap();
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
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
    let acceptor = suppaftp::tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let acceptor = acceptor.clone();
            let (fault, rests, data) = (fault_cell.clone(), rests_cell.clone(), data_cell.clone());
            tokio::spawn(async move {
                let _: Result<()> = async {
                    let mut rest = 0usize;
                    let mut conn: Connection = if implicit {
                        BufReader::new(Box::new(acceptor.accept(stream).await?))
                    } else {
                        BufReader::new(Box::new(stream))
                    };
                    conn.write_all(b"220 Test FTP\r\n").await?;
                    let mut passive = None;
                    let mut tls = implicit;
                    loop {
                        let mut line = String::new();
                        if conn.read_line(&mut line).await? == 0 {
                            break;
                        }
                        let cmd = line.trim_end_matches(['\r', '\n']);
                        let (verb, arg) = cmd.split_once(' ').unwrap_or((cmd, ""));
                        let fault_now = *fault.lock().unwrap();
                        let fault = fault_now;
                        let reply = match verb {
                            "AUTH" if secure && !tls => {
                                conn.write_all(b"234 TLS ready\r\n").await?;
                                conn = BufReader::new(Box::new(
                                    acceptor.accept(conn.into_inner()).await?,
                                ));
                                tls = true;
                                continue;
                            }
                            "USER" => "331 Password required\r\n",
                            "PASS" if matches!(fault, Fault::BadPassword) || arg != "p@ss" => {
                                "530 Authentication failed\r\n"
                            }
                            "PASS" => "230 Logged in\r\n",
                            "PBSZ" | "PROT" | "TYPE" => "200 OK\r\n",
                            "SIZE" if matches!(fault, Fault::NoSize) => "502 SIZE unsupported\r\n",
                            "SIZE" => {
                                let length = data.lock().unwrap().len();
                                conn.write_all(format!("213 {length}\r\n").as_bytes())
                                    .await?;
                                continue;
                            }
                            "MDTM" => "213 20260101120000\r\n",
                            "REST" if matches!(fault, Fault::NoRest) => "502 REST unsupported\r\n",
                            "REST" => {
                                rest = arg.parse()?;
                                rests.lock().unwrap().push(rest);
                                "350 Restarting\r\n"
                            }
                            "PASV" => {
                                let data = TcpListener::bind("127.0.0.1:0").await?;
                                let port = data.local_addr()?.port();
                                passive = Some(data);
                                // A bogus remote IP must be ignored in favour of the control peer.
                                conn.write_all(
                                    format!(
                                        "227 Passive Mode (192,0,2,1,{},{})\r\n",
                                        port / 256,
                                        port % 256
                                    )
                                    .as_bytes(),
                                )
                                .await?;
                                continue;
                            }
                            "EPSV" => {
                                let data = TcpListener::bind("127.0.0.1:0").await?;
                                let port = data.local_addr()?.port();
                                passive = Some(data);
                                conn.write_all(
                                    format!("229 Entering Extended Passive Mode (|||{port}|)\r\n")
                                        .as_bytes(),
                                )
                                .await?;
                                continue;
                            }
                            "RETR" => {
                                assert_eq!(arg, "/folder/payload file.bin");
                                conn.write_all(b"150 Opening data channel\r\n").await?;
                                let (channel, _) = passive.take().unwrap().accept().await?;
                                let mut channel: Box<dyn Io> = if secure {
                                    Box::new(acceptor.accept(channel).await?)
                                } else {
                                    Box::new(channel)
                                };
                                if matches!(fault, Fault::Stall) {
                                    tokio::time::sleep(Duration::from_secs(5)).await;
                                }
                                let payload = data.lock().unwrap().clone();
                                let from = if matches!(fault, Fault::IgnoreRest) {
                                    0
                                } else {
                                    rest
                                };
                                rest = 0;
                                let body = match fault {
                                    Fault::Truncated => &payload[..123],
                                    Fault::DropAfter(n) => {
                                        &payload[from..(from + n).min(payload.len())]
                                    }
                                    _ => &payload[from..],
                                };
                                let _ = channel.write_all(body).await;
                                let _ = channel.shutdown().await;
                                if matches!(fault, Fault::RejectedCompletion | Fault::DropAfter(_))
                                {
                                    "426 Transfer failed\r\n"
                                } else {
                                    "226 Transfer complete\r\n"
                                }
                            }
                            "QUIT" => {
                                conn.write_all(b"221 Bye\r\n").await?;
                                break;
                            }
                            _ => "502 Not implemented\r\n",
                        };
                        conn.write_all(reply.as_bytes()).await?;
                    }
                    Ok(())
                }
                .await;
            });
        }
    });
    FtpFixture {
        fault,
        rests,
        data,
        address,
        cert,
        task,
        root,
    }
}

async fn transfer_ftp(
    secure: bool,
    implicit: bool,
    fault: Fault,
    trust: bool,
    cancel: bool,
) -> (Result<()>, bool, Vec<u8>) {
    let fixture = ftp_fixture(secure, implicit, fault).await;
    let destination = fixture.root.path().join("result.bin");
    let signal = Arc::new(AtomicBool::new(false));
    if cancel {
        let signal = signal.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            signal.store(true, Ordering::Release);
        });
    }
    let scheme = if implicit {
        "ftps"
    } else if secure {
        "ftpes"
    } else {
        "ftp"
    };
    let url = format!(
        "{scheme}://user:p%40ss@{}/folder/payload%20file.bin",
        fixture.address
    );
    let options = TransportOptions {
        ca_certificate: trust.then_some(fixture.cert.clone()),
        ..Default::default()
    };
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        engine(options).download(
            DownloadRequest {
                url,
                destination: destination.clone(),
            },
            signal,
            |_| {},
        ),
    )
    .await
    .unwrap();
    let exists = destination.exists();
    let data = tokio::fs::read(&destination).await.unwrap_or_default();
    let remaining: Vec<_> = std::fs::read_dir(fixture.root.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .filter(|n| n.to_string_lossy().ends_with(".part"))
        .collect();
    assert!(
        remaining.is_empty(),
        "orphaned temporary files: {remaining:?}"
    );
    (result, exists, data)
}

#[tokio::test]
async fn ftp_binary_and_unknown_size_are_exact() {
    for fault in [Fault::None, Fault::NoSize] {
        let (result, exists, data) = transfer_ftp(false, false, fault, false, false).await;
        result.unwrap();
        assert!(exists);
        assert_eq!(data, *payload());
    }
}
#[tokio::test]
async fn ftps_explicit_and_implicit_verify_certificates_and_data() {
    for implicit in [false, true] {
        let (result, _, data) = transfer_ftp(true, implicit, Fault::None, true, false).await;
        result.unwrap();
        assert_eq!(data, *payload());
        let (result, exists, _) = transfer_ftp(true, implicit, Fault::None, false, false).await;
        assert!(result.is_err());
        assert!(!exists);
    }
}
#[tokio::test]
async fn ftp_failures_never_publish_a_file() {
    for fault in [
        Fault::Truncated,
        Fault::RejectedCompletion,
        Fault::BadPassword,
    ] {
        let (result, exists, _) = transfer_ftp(false, false, fault, false, false).await;
        assert!(result.is_err());
        assert!(!exists);
    }
}
#[tokio::test]
async fn ftp_cancels_even_when_server_stalls() {
    let (result, exists, _) = transfer_ftp(false, false, Fault::Stall, false, true).await;
    assert!(result.unwrap_err().to_string().contains("cancelled"));
    assert!(!exists);
}
#[tokio::test]
async fn ftp_rejects_command_injection_before_connecting() {
    let root = tempfile::tempdir().unwrap();
    for path in ["payload%0d%0aDELE%20file", "payload%00.bin"] {
        let result = engine(Default::default())
            .download(
                DownloadRequest {
                    url: format!("ftp://127.0.0.1:9/{path}"),
                    destination: root.path().join("file"),
                },
                Arc::new(AtomicBool::new(false)),
                |_| {},
            )
            .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("control characters")
        );
    }
}

const MIB: usize = 1024 * 1024;
fn big_payload(salt: u8) -> Arc<Vec<u8>> {
    Arc::new(
        (0..20 * MIB)
            .map(|i| ((i % 251) as u8).wrapping_add(salt))
            .collect(),
    )
}
fn partial_of(fixture: &FtpFixture) -> (PathBuf, PathBuf) {
    (
        fixture.root.path().join(".result.bin.nibiru-part"),
        fixture.root.path().join(".result.bin.nibiru-part.json"),
    )
}
async fn attempt(fixture: &FtpFixture, scheme: &str, trust: bool) -> Result<()> {
    let options = TransportOptions {
        ca_certificate: trust.then_some(fixture.cert.clone()),
        ..Default::default()
    };
    tokio::time::timeout(
        Duration::from_secs(30),
        engine(options).download(
            DownloadRequest {
                url: format!(
                    "{scheme}://user:p%40ss@{}/folder/payload%20file.bin",
                    fixture.address
                ),
                destination: fixture.root.path().join("result.bin"),
            },
            Arc::new(AtomicBool::new(false)),
            |_| {},
        ),
    )
    .await
    .unwrap()
}
fn set_fault(fixture: &FtpFixture, fault: Fault) {
    *fixture.fault.lock().unwrap() = fault;
}

#[tokio::test]
async fn ftp_and_explicit_ftps_resume_from_the_kept_partial() {
    for (scheme, secure) in [("ftp", false), ("ftpes", true)] {
        let data = big_payload(0);
        let fixture =
            ftp_fixture_with(secure, false, Fault::DropAfter(12 * MIB), data.clone()).await;
        assert!(attempt(&fixture, scheme, secure).await.is_err());
        let (part, meta) = partial_of(&fixture);
        assert_eq!(std::fs::metadata(&part).unwrap().len(), 12 * MIB as u64);
        assert!(!fixture.root.path().join("result.bin").exists());
        let saved = std::fs::read_to_string(&meta).unwrap();
        assert!(
            !saved.contains("p@ss") && !saved.contains("p%40ss") && !saved.contains("user:"),
            "credentials leaked into the sidecar: {saved}"
        );

        set_fault(&fixture, Fault::None);
        attempt(&fixture, scheme, secure).await.unwrap();
        assert_eq!(
            std::fs::read(fixture.root.path().join("result.bin")).unwrap(),
            *data
        );
        // Continues 8 MiB before the end of the kept bytes and compares that overlap.
        assert_eq!(*fixture.rests.lock().unwrap(), vec![4 * MIB]);
        assert!(!part.exists() && !meta.exists());
    }
}

#[tokio::test]
async fn ftp_starts_over_when_the_server_refuses_rest() {
    let data = big_payload(0);
    let fixture = ftp_fixture_with(false, false, Fault::DropAfter(12 * MIB), data.clone()).await;
    assert!(attempt(&fixture, "ftp", false).await.is_err());
    set_fault(&fixture, Fault::NoRest);
    attempt(&fixture, "ftp", false).await.unwrap();
    assert_eq!(
        std::fs::read(fixture.root.path().join("result.bin")).unwrap(),
        *data
    );
    assert!(fixture.rests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn ftp_discards_the_partial_when_the_server_ignores_rest() {
    let data = big_payload(0);
    let fixture = ftp_fixture_with(false, false, Fault::DropAfter(12 * MIB), data.clone()).await;
    assert!(attempt(&fixture, "ftp", false).await.is_err());
    // Says 350 but sends byte 0 onwards: the overlap no longer matches, so the saved bytes are
    // dropped and the file is fetched again completely.
    set_fault(&fixture, Fault::IgnoreRest);
    attempt(&fixture, "ftp", false).await.unwrap();
    assert_eq!(
        std::fs::read(fixture.root.path().join("result.bin")).unwrap(),
        *data
    );
    assert_eq!(*fixture.rests.lock().unwrap(), vec![4 * MIB]);
}

#[tokio::test]
async fn ftp_never_continues_a_partial_into_a_different_remote_file() {
    // Same length (and same fixed MDTM): only the overlap comparison can tell.
    let fixture = ftp_fixture_with(false, false, Fault::DropAfter(12 * MIB), big_payload(0)).await;
    assert!(attempt(&fixture, "ftp", false).await.is_err());
    let replacement = big_payload(1);
    *fixture.data.lock().unwrap() = replacement.clone();
    set_fault(&fixture, Fault::None);
    attempt(&fixture, "ftp", false).await.unwrap();
    assert_eq!(
        std::fs::read(fixture.root.path().join("result.bin")).unwrap(),
        *replacement
    );

    // A different length is rejected before anything is requested with REST.
    let other = ftp_fixture_with(false, false, Fault::DropAfter(12 * MIB), big_payload(0)).await;
    assert!(attempt(&other, "ftp", false).await.is_err());
    let longer: Arc<Vec<u8>> = Arc::new(
        big_payload(0)
            .iter()
            .chain([1u8, 2, 3].iter())
            .copied()
            .collect(),
    );
    *other.data.lock().unwrap() = longer.clone();
    set_fault(&other, Fault::None);
    attempt(&other, "ftp", false).await.unwrap();
    assert_eq!(
        std::fs::read(other.root.path().join("result.bin")).unwrap(),
        *longer
    );
    assert!(other.rests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn ftp_obeys_shared_bandwidth_limits_and_cancels_while_waiting_for_tokens() {
    use nibiru_xtreme::bittorrent::limiter::RateLimiter;
    for cancel_while_throttled in [false, true] {
        let data = Arc::new(vec![23; 300_000]);
        let fixture = ftp_fixture_with(false, false, Fault::None, data.clone()).await;
        let destination = fixture.root.path().join("limited.bin");
        let cancel = Arc::new(AtomicBool::new(false));
        if cancel_while_throttled {
            let signal = cancel.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                signal.store(true, Ordering::Release);
            });
        }
        let limiter = Arc::new(RateLimiter::new(if cancel_while_throttled {
            1
        } else {
            100_000
        }));
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            engine(Default::default())
                .with_limiters(vec![limiter])
                .download(
                    DownloadRequest {
                        url: format!(
                            "ftp://user:p%40ss@{}/folder/payload%20file.bin",
                            fixture.address
                        ),
                        destination: destination.clone(),
                    },
                    cancel,
                    |_| {},
                ),
        )
        .await
        .expect("throttled transfer did not respond");
        if cancel_while_throttled {
            assert!(result.unwrap_err().to_string().contains("cancelled"));
            assert!(started.elapsed() < Duration::from_secs(2));
            assert!(!destination.exists());
        } else {
            result.unwrap();
            assert!(
                started.elapsed() >= Duration::from_millis(1800),
                "FTP ignored the engine limiter: {:?}",
                started.elapsed()
            );
            assert_eq!(std::fs::read(destination).unwrap(), *data);
        }
    }
}
