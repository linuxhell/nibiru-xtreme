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
}
struct FtpFixture {
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
            tokio::spawn(async move {
                let _: Result<()> = async {
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
                                conn.write_all(format!("213 {}\r\n", payload().len()).as_bytes())
                                    .await?;
                                continue;
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
                                let (data, _) = passive.take().unwrap().accept().await?;
                                let mut data: Box<dyn Io> = if secure {
                                    Box::new(acceptor.accept(data).await?)
                                } else {
                                    Box::new(data)
                                };
                                if matches!(fault, Fault::Stall) {
                                    tokio::time::sleep(Duration::from_secs(5)).await;
                                }
                                let payload = payload();
                                let body = if matches!(fault, Fault::Truncated) {
                                    &payload[..123]
                                } else {
                                    &payload[..]
                                };
                                let _ = data.write_all(body).await;
                                let _ = data.shutdown().await;
                                if matches!(fault, Fault::RejectedCompletion) {
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
