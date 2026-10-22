#![cfg(feature = "sftp")]
use anyhow::Result;
use nibiru_xtreme::{
    engine::{DownloadRequest, Engine},
    logging::LogSink,
    protocols::TransportOptions,
};
use russh::{
    Channel, ChannelId,
    server::{Auth, Msg, Session},
};
use russh_sftp::protocol::{Attrs, Data, FileAttributes, Handle, OpenFlags, Status, StatusCode};
use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use tokio::net::TcpListener;

struct Ssh {
    channels: HashMap<ChannelId, Channel<Msg>>,
    payload: Arc<Vec<u8>>,
    truncated: bool,
}
impl russh::server::Handler for Ssh {
    type Error = anyhow::Error;
    async fn auth_password(&mut self, user: &str, password: &str) -> Result<Auth> {
        Ok(if user == "user" && password == "p@ss" {
            Auth::Accept
        } else {
            Auth::Reject {
                proceed_with_methods: None,
                partial_success: false,
            }
        })
    }
    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        reply: russh::server::ChannelOpenHandle,
        _: &mut Session,
    ) -> Result<()> {
        self.channels.insert(channel.id(), channel);
        reply.accept().await;
        Ok(())
    }
    async fn subsystem_request(
        &mut self,
        id: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> Result<()> {
        if name != "sftp" {
            session.channel_failure(id)?;
            return Ok(());
        }
        session.channel_success(id)?;
        let channel = self.channels.remove(&id).unwrap();
        let handler = Files {
            payload: self.payload.clone(),
            truncated: self.truncated,
        };
        tokio::spawn(russh_sftp::server::run(channel.into_stream(), handler));
        Ok(())
    }
}
struct Files {
    payload: Arc<Vec<u8>>,
    truncated: bool,
}
impl russh_sftp::server::Handler for Files {
    type Error = StatusCode;
    fn unimplemented(&self) -> StatusCode {
        StatusCode::OpUnsupported
    }
    async fn open(
        &mut self,
        id: u32,
        filename: String,
        flags: OpenFlags,
        _: FileAttributes,
    ) -> std::result::Result<Handle, StatusCode> {
        if filename != "/payload file.bin" || flags.bits() != OpenFlags::READ.bits() {
            return Err(StatusCode::PermissionDenied);
        }
        Ok(Handle {
            id,
            handle: "file".into(),
        })
    }
    async fn fstat(&mut self, id: u32, _: String) -> std::result::Result<Attrs, StatusCode> {
        Ok(Attrs {
            id,
            attrs: FileAttributes {
                size: Some(self.payload.len() as u64),
                ..Default::default()
            },
        })
    }
    async fn read(
        &mut self,
        id: u32,
        _: String,
        offset: u64,
        len: u32,
    ) -> std::result::Result<Data, StatusCode> {
        let max = if self.truncated {
            123
        } else {
            self.payload.len()
        };
        let start = usize::try_from(offset).map_err(|_| StatusCode::Failure)?;
        if start >= max {
            return Err(StatusCode::Eof);
        }
        Ok(Data {
            id,
            data: self.payload[start..(start + len as usize).min(max)].to_vec(),
        })
    }
    async fn close(&mut self, id: u32, _: String) -> std::result::Result<Status, StatusCode> {
        Ok(Status {
            id,
            status_code: StatusCode::Ok,
            error_message: String::new(),
            language_tag: String::new(),
        })
    }
}

#[tokio::test]
async fn sftp_verifies_host_key_authenticates_and_checks_length() {
    let key =
        russh::keys::PrivateKey::random(&mut rand::rng(), russh::keys::Algorithm::Ed25519).unwrap();
    let fingerprint = key
        .public_key()
        .fingerprint(russh::keys::HashAlg::Sha256)
        .to_string();
    let payload = Arc::new((0..400_007).map(|i| (i % 251) as u8).collect::<Vec<_>>());
    for (trusted, password, truncated, success) in [
        (true, "p%40ss", false, true),
        (false, "p%40ss", false, false),
        (true, "wrong", false, false),
        (true, "p%40ss", true, false),
    ] {
        let config = Arc::new(russh::server::Config {
            keys: vec![key.clone()],
            auth_rejection_time: Duration::ZERO,
            auth_rejection_time_initial: Some(Duration::ZERO),
            ..Default::default()
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let data = payload.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            if let Ok(running) = russh::server::run_stream(
                config,
                stream,
                Ssh {
                    channels: HashMap::new(),
                    payload: data,
                    truncated,
                },
            )
            .await
            {
                let _ = running.await;
            }
        });
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("file.bin");
        let options = TransportOptions {
            ssh_host_key: Some(if trusted {
                fingerprint.clone()
            } else {
                "SHA256:wrong".into()
            }),
            ..Default::default()
        };
        let engine = Engine::new(Arc::new(LogSink::new(None).unwrap()))
            .unwrap()
            .with_transport_options(options);
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            engine.download(
                DownloadRequest {
                    url: format!("sftp://user:{password}@{address}/payload%20file.bin"),
                    destination: destination.clone(),
                },
                Arc::new(AtomicBool::new(false)),
                |_| {},
            ),
        )
        .await
        .unwrap();
        assert_eq!(result.is_ok(), success, "{result:?}");
        assert_eq!(destination.exists(), success);
        if success {
            assert_eq!(tokio::fs::read(&destination).await.unwrap(), *payload);
        }
        assert!(
            std::fs::read_dir(root.path()).unwrap().all(|e| !e
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".part"))
        );
        server.abort();
    }
}

// ---- resume ----

const MIB: usize = 1024 * 1024;
struct Remote {
    payload: std::sync::Mutex<Arc<Vec<u8>>>,
    /// The server reports end-of-file at this offset (simulates a dropped transfer).
    cap: std::sync::atomic::AtomicUsize,
    reads: std::sync::Mutex<Vec<u64>>,
}
struct ResumeSsh {
    channels: HashMap<ChannelId, Channel<Msg>>,
    remote: Arc<Remote>,
}
impl russh::server::Handler for ResumeSsh {
    type Error = anyhow::Error;
    async fn auth_password(&mut self, user: &str, password: &str) -> Result<Auth> {
        Ok(if user == "user" && password == "p@ss" {
            Auth::Accept
        } else {
            Auth::Reject {
                proceed_with_methods: None,
                partial_success: false,
            }
        })
    }
    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        reply: russh::server::ChannelOpenHandle,
        _: &mut Session,
    ) -> Result<()> {
        self.channels.insert(channel.id(), channel);
        reply.accept().await;
        Ok(())
    }
    async fn subsystem_request(
        &mut self,
        id: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> Result<()> {
        if name != "sftp" {
            session.channel_failure(id)?;
            return Ok(());
        }
        session.channel_success(id)?;
        let channel = self.channels.remove(&id).unwrap();
        tokio::spawn(russh_sftp::server::run(
            channel.into_stream(),
            ResumeFiles {
                remote: self.remote.clone(),
            },
        ));
        Ok(())
    }
}
struct ResumeFiles {
    remote: Arc<Remote>,
}
impl russh_sftp::server::Handler for ResumeFiles {
    type Error = StatusCode;
    fn unimplemented(&self) -> StatusCode {
        StatusCode::OpUnsupported
    }
    async fn open(
        &mut self,
        id: u32,
        filename: String,
        _: OpenFlags,
        _: FileAttributes,
    ) -> std::result::Result<Handle, StatusCode> {
        if filename != "/big.bin" {
            return Err(StatusCode::NoSuchFile);
        }
        Ok(Handle {
            id,
            handle: "file".into(),
        })
    }
    async fn fstat(&mut self, id: u32, _: String) -> std::result::Result<Attrs, StatusCode> {
        Ok(Attrs {
            id,
            attrs: FileAttributes {
                size: Some(self.remote.payload.lock().unwrap().len() as u64),
                mtime: Some(1_700_000_000),
                ..Default::default()
            },
        })
    }
    async fn read(
        &mut self,
        id: u32,
        _: String,
        offset: u64,
        len: u32,
    ) -> std::result::Result<Data, StatusCode> {
        self.remote.reads.lock().unwrap().push(offset);
        let payload = self.remote.payload.lock().unwrap().clone();
        let max = payload
            .len()
            .min(self.remote.cap.load(std::sync::atomic::Ordering::Acquire));
        let start = usize::try_from(offset).map_err(|_| StatusCode::Failure)?;
        if start >= max {
            return Err(StatusCode::Eof);
        }
        Ok(Data {
            id,
            data: payload[start..(start + len as usize).min(max)].to_vec(),
        })
    }
    async fn close(&mut self, id: u32, _: String) -> std::result::Result<Status, StatusCode> {
        Ok(Status {
            id,
            status_code: StatusCode::Ok,
            error_message: String::new(),
            language_tag: String::new(),
        })
    }
}

fn big(salt: u8, extra: usize) -> Arc<Vec<u8>> {
    Arc::new(
        (0..20 * MIB + extra)
            .map(|i| ((i % 251) as u8).wrapping_add(salt))
            .collect(),
    )
}

struct ResumeServer {
    address: std::net::SocketAddr,
    fingerprint: String,
    remote: Arc<Remote>,
    task: tokio::task::JoinHandle<()>,
    root: tempfile::TempDir,
}
impl Drop for ResumeServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn resume_server(payload: Arc<Vec<u8>>, cap: usize) -> ResumeServer {
    let key =
        russh::keys::PrivateKey::random(&mut rand::rng(), russh::keys::Algorithm::Ed25519).unwrap();
    let fingerprint = key
        .public_key()
        .fingerprint(russh::keys::HashAlg::Sha256)
        .to_string();
    let config = Arc::new(russh::server::Config {
        keys: vec![key],
        auth_rejection_time: Duration::ZERO,
        auth_rejection_time_initial: Some(Duration::ZERO),
        ..Default::default()
    });
    let remote = Arc::new(Remote {
        payload: std::sync::Mutex::new(payload),
        cap: std::sync::atomic::AtomicUsize::new(cap),
        reads: std::sync::Mutex::new(Vec::new()),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let shared = remote.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let (config, remote) = (config.clone(), shared.clone());
            tokio::spawn(async move {
                if let Ok(running) = russh::server::run_stream(
                    config,
                    stream,
                    ResumeSsh {
                        channels: HashMap::new(),
                        remote,
                    },
                )
                .await
                {
                    let _ = running.await;
                }
            });
        }
    });
    ResumeServer {
        address,
        fingerprint,
        remote,
        task,
        root: tempfile::tempdir().unwrap(),
    }
}
async fn fetch(server: &ResumeServer) -> Result<()> {
    let engine = Engine::new(Arc::new(LogSink::new(None).unwrap()))
        .unwrap()
        .with_transport_options(TransportOptions {
            ssh_host_key: Some(server.fingerprint.clone()),
            ..Default::default()
        });
    tokio::time::timeout(
        Duration::from_secs(60),
        engine.download(
            DownloadRequest {
                url: format!("sftp://user:p%40ss@{}/big.bin", server.address),
                destination: server.root.path().join("result.bin"),
            },
            Arc::new(AtomicBool::new(false)),
            |_| {},
        ),
    )
    .await
    .unwrap()
}
fn first_read(server: &ResumeServer) -> u64 {
    *server.remote.reads.lock().unwrap().first().unwrap()
}

#[tokio::test]
async fn sftp_resumes_from_the_kept_partial_and_keeps_credentials_out_of_it() {
    let data = big(0, 0);
    let server = resume_server(data.clone(), 12 * MIB).await;
    assert!(fetch(&server).await.is_err());
    let part = server.root.path().join(".result.bin.nibiru-part");
    let meta = server.root.path().join(".result.bin.nibiru-part.json");
    assert_eq!(std::fs::metadata(&part).unwrap().len(), 12 * MIB as u64);
    assert!(!server.root.path().join("result.bin").exists());
    let saved = std::fs::read_to_string(&meta).unwrap();
    assert!(
        !saved.contains("p@ss") && !saved.contains("p%40ss") && !saved.contains("user:"),
        "{saved}"
    );

    server.remote.reads.lock().unwrap().clear();
    server
        .remote
        .cap
        .store(usize::MAX, std::sync::atomic::Ordering::Release);
    fetch(&server).await.unwrap();
    assert_eq!(
        std::fs::read(server.root.path().join("result.bin")).unwrap(),
        *data
    );
    // Re-reads the last 8 MiB of what it holds, compares them, then continues.
    assert_eq!(first_read(&server), 4 * MIB as u64);
    assert!(!part.exists() && !meta.exists());
}

#[tokio::test]
async fn sftp_does_not_continue_into_a_changed_remote_file() {
    // Same size and mtime: only the overlap comparison can notice the content changed.
    let server = resume_server(big(0, 0), 12 * MIB).await;
    assert!(fetch(&server).await.is_err());
    let replacement = big(1, 0);
    *server.remote.payload.lock().unwrap() = replacement.clone();
    server
        .remote
        .cap
        .store(usize::MAX, std::sync::atomic::Ordering::Release);
    server.remote.reads.lock().unwrap().clear();
    fetch(&server).await.unwrap();
    assert_eq!(
        std::fs::read(server.root.path().join("result.bin")).unwrap(),
        *replacement
    );
    assert!(server.remote.reads.lock().unwrap().contains(&0));

    // A different size is rejected up front: the very first read is at offset 0.
    let other = resume_server(big(0, 0), 12 * MIB).await;
    assert!(fetch(&other).await.is_err());
    let longer = big(0, 3);
    *other.remote.payload.lock().unwrap() = longer.clone();
    other
        .remote
        .cap
        .store(usize::MAX, std::sync::atomic::Ordering::Release);
    other.remote.reads.lock().unwrap().clear();
    fetch(&other).await.unwrap();
    assert_eq!(first_read(&other), 0);
    assert_eq!(
        std::fs::read(other.root.path().join("result.bin")).unwrap(),
        *longer
    );
}
