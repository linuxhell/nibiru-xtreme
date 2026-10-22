use anyhow::Result;
use std::{
    fmt, io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{SyncSender, sync_channel},
    },
    thread::{self, JoinHandle},
    time::Instant,
};

use crate::logging::LogSink;

#[derive(Debug)]
pub(crate) struct BackgroundSyncError(String);

impl fmt::Display for BackgroundSyncError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "background sync failed: {}", self.0)
    }
}

impl std::error::Error for BackgroundSyncError {}

#[derive(Default)]
pub(crate) struct SyncStats {
    pub count: u64,
    pub elapsed_ms: u128,
}

// At most one checkpoint can be queued or running. Progress is a count of
// completed positional writes, NOT a durable or contiguous prefix of the file.
pub(crate) struct BackgroundSync {
    sender: Option<SyncSender<u64>>,
    worker: Option<JoinHandle<Result<SyncStats>>>,
    busy: Arc<AtomicBool>,
    interval: u64,
    next_at: u64,
}

impl BackgroundSync {
    pub fn start(file: Arc<std::fs::File>, interval: u64, log: Arc<LogSink>) -> Result<Self> {
        Self::spawn(interval, log, move || file.sync_all())
    }

    fn spawn<F>(interval: u64, log: Arc<LogSink>, mut sync: F) -> Result<Self>
    where
        F: FnMut() -> io::Result<()> + Send + 'static,
    {
        let (sender, receiver) = sync_channel(1);
        let busy = Arc::new(AtomicBool::new(false));
        let worker_busy = busy.clone();
        let worker = thread::Builder::new()
            .name("nibiru-sync".into())
            .spawn(move || {
                let mut stats = SyncStats::default();
                for observed_written in receiver {
                    let started = Instant::now();
                    if let Err(error) = sync() {
                        log.error("storage.checkpoint_failed", &error.to_string());
                        return Err(BackgroundSyncError(error.to_string()).into());
                    }
                    let elapsed_ms = started.elapsed().as_millis();
                    stats.count += 1;
                    stats.elapsed_ms += elapsed_ms;
                    log.info(
                        "storage.checkpoint_completed",
                        &format!("observed_written={observed_written} elapsed_ms={elapsed_ms}"),
                    );
                    worker_busy.store(false, Ordering::Release);
                }
                Ok(stats)
            })
            .map_err(|error| BackgroundSyncError(error.to_string()))?;
        Ok(Self {
            sender: Some(sender),
            worker: Some(worker),
            busy,
            interval,
            next_at: interval,
        })
    }

    pub fn consider(&mut self, written: u64, total: u64) {
        if written < self.next_at || written >= total {
            return;
        }
        if self
            .busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        if self.sender.as_ref().unwrap().try_send(written).is_ok() {
            self.next_at = written.saturating_add(self.interval);
        } else {
            self.busy.store(false, Ordering::Release);
        }
    }

    pub async fn finish(self) -> Result<SyncStats> {
        tokio::task::spawn_blocking(move || self.join())
            .await
            .map_err(|error| BackgroundSyncError(error.to_string()))?
    }

    fn join(mut self) -> Result<SyncStats> {
        // Closing the only sender lets the worker drain the pending checkpoint
        // and release its file handle before the caller can rename/delete it.
        drop(self.sender.take());
        self.worker
            .take()
            .unwrap()
            .join()
            .map_err(|_| BackgroundSyncError("worker panicked".into()))?
    }
}

impl Drop for BackgroundSync {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(worker) = self.worker.take() {
            // Also reap the OS thread when the download future is dropped.
            let _ = worker.join();
        }
    }
}

pub(crate) async fn finish(checkpoints: Option<BackgroundSync>) -> Result<SyncStats> {
    match checkpoints {
        Some(worker) => worker.finish().await,
        None => Ok(SyncStats::default()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::mpsc, time::Duration};

    #[tokio::test]
    async fn checkpoints_do_not_queue_and_finish_waits_for_active_sync() {
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let mut worker =
            BackgroundSync::spawn(1, Arc::new(LogSink::new(None).unwrap()), move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
            .unwrap();
        worker.consider(1, 100);
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        for n in 2..100 {
            worker.consider(n, 100);
        }
        let finishing = tokio::spawn(worker.finish());
        tokio::task::yield_now().await;
        assert!(!finishing.is_finished());
        release_tx.send(()).unwrap();
        let stats = tokio::time::timeout(Duration::from_secs(5), finishing)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(stats.count, 1);
    }

    #[tokio::test]
    async fn checkpoint_failure_is_returned_to_caller() {
        let mut worker = BackgroundSync::spawn(1, Arc::new(LogSink::new(None).unwrap()), || {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected sync failure",
            ))
        })
        .unwrap();
        worker.consider(1, 100);
        let error = worker.finish().await.err().unwrap();
        assert!(error.is::<BackgroundSyncError>());
        assert!(error.to_string().contains("injected sync failure"));
    }
}
