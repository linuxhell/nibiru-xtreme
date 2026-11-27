//! Happy Eyeballs for safe requests; only a response header winner is chosen.
use anyhow::Result;
use std::{future::Future, time::Duration};

pub(super) async fn compete<T>(
    quic: impl Future<Output = Result<T>>,
    tcp: impl Future<Output = Result<T>>,
) -> Result<(T, bool)> {
    tokio::pin!(quic);
    let first = tokio::select! { response = &mut quic => Some(response), _ = tokio::time::sleep(Duration::from_millis(75)) => None };
    if let Some(Ok(response)) = first {
        return Ok((response, true));
    }
    tokio::pin!(tcp);
    if first.is_some() {
        return tcp.await.map(|r| (r, false));
    }
    tokio::select! {
        response = &mut quic => match response { Ok(r) => Ok((r, true)), Err(_) => tcp.await.map(|r| (r, false)) },
        response = &mut tcp => match response { Ok(r) => Ok((r, false)), Err(e) => quic.await.map(|r| (r, true)).map_err(|_| e) },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    #[tokio::test]
    async fn fast_quic_reuses_the_winner_without_polling_tcp() {
        let polled = AtomicBool::new(false);
        let result = compete(async { Ok(b"winning body".to_vec()) }, async {
            polled.store(true, Ordering::Release);
            Ok(Vec::new())
        })
        .await
        .unwrap();
        assert_eq!(result, (b"winning body".to_vec(), true));
        assert!(!polled.load(Ordering::Acquire));
    }
    #[tokio::test]
    async fn blocked_quic_falls_back_and_releases_the_losing_request() {
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = Dropped(dropped.clone());
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            compete(
                async move {
                    let _guard = guard;
                    std::future::pending::<Result<Vec<u8>>>().await
                },
                async { Ok(b"tcp body".to_vec()) },
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result, (b"tcp body".to_vec(), false));
        assert!(dropped.load(Ordering::Acquire));
    }
    #[tokio::test]
    async fn failed_quic_and_failed_tcp_have_bounded_deterministic_results() {
        assert_eq!(
            compete(async { anyhow::bail!("quic unavailable") }, async { Ok(7) })
                .await
                .unwrap(),
            (7, false)
        );
        assert!(
            compete::<u8>(async { anyhow::bail!("quic unavailable") }, async {
                anyhow::bail!("tcp unavailable")
            })
            .await
            .is_err()
        );
    }
}
