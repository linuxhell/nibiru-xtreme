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
