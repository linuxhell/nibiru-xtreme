//! Completion-owned buffers on IOCP/io_uring; portable positional I/O when the
//! OS denies native asynchronous I/O. Network work stays on the Tokio runtime.
use anyhow::{Result, anyhow};
use std::{fs::File, path::Path, sync::Arc};

#[derive(Clone)]
pub(crate) struct Writer {
    file: Arc<File>,
    pending: Arc<Pending>,
    #[cfg(any(target_os = "linux", windows))]
    native: Option<Arc<native::Handle>>,
}

impl Writer {
    pub async fn open(file: Arc<File>, path: &Path, portable: bool) -> Self {
        #[cfg(any(target_os = "linux", windows))]
        let native = if portable {
            None
        } else {
            native::open(file.clone(), path.to_owned())
                .await
                .ok()
                .map(Arc::new)
        };
        #[cfg(not(any(target_os = "linux", windows)))]
        let _ = (path, portable);
        Self {
            file,
            pending: Arc::default(),
            #[cfg(any(target_os = "linux", windows))]
            native,
        }
    }

    pub fn backend(&self) -> &'static str {
        #[cfg(any(target_os = "linux", windows))]
        if self.native.is_some() {
            return if cfg!(windows) { "iocp" } else { "io_uring" };
        }
        "portable"
    }

    pub async fn write(&self, offset: u64, buffer: Vec<u8>) -> Result<Vec<u8>> {
        #[cfg(any(target_os = "linux", windows))]
        if let Some(native) = &self.native {
            return native.write(offset, buffer).await;
        }
        self.pending
            .count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let pending = self.pending.clone();
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || {
            struct Guard(Arc<Pending>);
            impl Drop for Guard {
                fn drop(&mut self) {
                    self.0
                        .count
                        .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                    self.0.changed.notify_waiters();
                }
            }
            let _guard = Guard(pending);
            super::write_all_at(&file, offset, &buffer)?;
            Ok::<_, std::io::Error>(buffer)
        })
        .await
        .map_err(|e| anyhow!("disk worker failed: {e}"))?
        .map_err(Into::into)
    }

    /// A barrier before checkpointing or publishing. Dropping a request does
    /// not cancel an OS write: its buffer and completion must remain alive.
    pub async fn drain(&self) -> Result<()> {
        #[cfg(any(target_os = "linux", windows))]
        if let Some(native) = &self.native {
            native.drain().await?;
        }
        loop {
            let changed = self.pending.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self
                .pending
                .count
                .load(std::sync::atomic::Ordering::Acquire)
                == 0
            {
                break;
            }
            changed.await;
        }
        Ok(())
    }
    pub async fn finish(&self) -> Result<()> {
        self.drain().await?;
        #[cfg(any(target_os = "linux", windows))]
        if let Some(native) = &self.native {
            native.finish().await?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct Pending {
    count: std::sync::atomic::AtomicUsize,
    changed: tokio::sync::Notify,
}

#[cfg(any(target_os = "linux", windows))]
mod native {
    use super::*;
    use compio::{buf::BufResult, io::AsyncWriteAtExt};
    use futures_util::{StreamExt, future::LocalBoxFuture, stream::FuturesUnordered};
    use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

    type Completion = oneshot::Sender<std::io::Result<Vec<u8>>>;
    enum Command {
        Write(u64, Vec<u8>, Completion, OwnedSemaphorePermit),
        Drain(oneshot::Sender<()>),
        Finish(oneshot::Sender<std::io::Result<()>>),
    }
    pub(super) struct Handle {
        sender: mpsc::UnboundedSender<Command>,
        slots: Arc<Semaphore>,
    }

    pub(super) async fn open(original: Arc<File>, path: std::path::PathBuf) -> Result<Handle> {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let (ready, wait) = oneshot::channel();
        std::thread::Builder::new().name("nibiru-disk".into()).spawn(move || {
            let runtime = match compio::runtime::Runtime::new() { Ok(r) => r, Err(e) => { let _ = ready.send(Err(e)); return; } };
            if runtime.driver_type().is_polling() { let _ = ready.send(Err(std::io::Error::other("native I/O unavailable"))); return; }
            runtime.block_on(async move {
                let opened = async {
                    let file = compio::fs::OpenOptions::new().write(true).open(path).await?;
                    #[cfg(unix)]
                    let cloned: File = { use std::os::fd::AsFd; file.as_fd().try_clone_to_owned()?.into() };
                    #[cfg(windows)]
                    let cloned: File = { use std::os::windows::io::AsHandle; file.as_handle().try_clone_to_owned()?.into() };
                    if same_file::Handle::from_file(cloned)? != same_file::Handle::from_file(original.try_clone()?)? {
                        return Err(std::io::Error::other("partial file identity changed"));
                    }
                    Ok::<_, std::io::Error>(file)
                }.await;
                let file = match opened { Ok(file) => file, Err(e) => { let _ = ready.send(Err(e)); return; } };
                if ready.send(Ok(())).is_err() { return; }
                let mut pending: FuturesUnordered<LocalBoxFuture<'_, ()>> = FuturesUnordered::new();
                loop {
                    tokio::select! {
                        command = receiver.recv() => match command {
                            Some(Command::Write(offset, buffer, reply, permit)) => {
                                let mut file = file.clone();
                                pending.push(Box::pin(async move {
                                    let BufResult(result, buffer) = file.write_all_at(buffer, offset).await;
                                    let _ = reply.send(result.map(|()| buffer));
                                    drop(permit);
                                }));
                            }
                            Some(Command::Drain(reply)) => { while pending.next().await.is_some() {} let _ = reply.send(()); }
                            Some(Command::Finish(reply)) => {
                                while pending.next().await.is_some() {}
                                drop(pending);
                                let result = file.close().await;
                                drop(original);
                                let _ = reply.send(result);
                                return;
                            }
                            None => break,
                        },
                        _ = pending.next(), if !pending.is_empty() => {},
                    }
                }
                while pending.next().await.is_some() {}
                drop(file);
                drop(original);
            });
        })?;
        wait.await
            .map_err(|_| anyhow!("native disk worker stopped"))??;
        Ok(Handle {
            sender,
            slots: Arc::new(Semaphore::new(32)),
        })
    }

    impl Handle {
        pub async fn write(&self, offset: u64, buffer: Vec<u8>) -> Result<Vec<u8>> {
            let permit = self.slots.clone().acquire_owned().await?;
            let (reply, wait) = oneshot::channel();
            self.sender
                .send(Command::Write(offset, buffer, reply, permit))
                .map_err(|_| anyhow!("native disk worker stopped"))?;
            Ok(wait
                .await
                .map_err(|_| anyhow!("native disk completion lost"))??)
        }
        pub async fn finish(&self) -> Result<()> {
            let (reply, wait) = oneshot::channel();
            self.sender
                .send(Command::Finish(reply))
                .map_err(|_| anyhow!("native disk worker stopped"))?;
            wait.await
                .map_err(|_| anyhow!("native disk close lost"))??;
            Ok(())
        }
        pub async fn drain(&self) -> Result<()> {
            let (reply, wait) = oneshot::channel();
            self.sender
                .send(Command::Drain(reply))
                .map_err(|_| anyhow!("native disk worker stopped"))?;
            wait.await
                .map_err(|_| anyhow!("native disk barrier lost"))?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn concurrent_native_or_fallback_writes_keep_offsets_and_buffers() {
        for portable in [true, false] {
            let temp = tempfile::NamedTempFile::new().unwrap();
            let writer =
                Writer::open(Arc::new(temp.reopen().unwrap()), temp.path(), portable).await;
            let mut tasks = tokio::task::JoinSet::new();
            for n in 0..16u64 {
                let writer = writer.clone();
                tasks.spawn(async move {
                    let bytes = vec![n as u8; 65536];
                    assert_eq!(writer.write(n * 65536, bytes.clone()).await.unwrap(), bytes);
                });
            }
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
            writer.drain().await.unwrap();
            let bytes = std::fs::read(temp.path()).unwrap();
            for n in 0..16 {
                assert!(
                    bytes[n * 65536..(n + 1) * 65536]
                        .iter()
                        .all(|v| *v == n as u8)
                );
            }
        }
    }
    #[tokio::test]
    async fn reopened_path_cannot_redirect_native_writes() {
        let first = tempfile::NamedTempFile::new().unwrap();
        let second = tempfile::NamedTempFile::new().unwrap();
        let writer = Writer::open(Arc::new(first.reopen().unwrap()), second.path(), false).await;
        assert_eq!(writer.backend(), "portable");
        writer.write(0, b"original".to_vec()).await.unwrap();
        assert_eq!(std::fs::read(first.path()).unwrap(), b"original");
        assert!(std::fs::read(second.path()).unwrap().is_empty());
    }
}
