//! Shared bandwidth limiting for HTTP/BitTorrent and BitTorrent speed measurement.

use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

/// Token bucket shared by transfer connections. Zero means unlimited.
pub struct RateLimiter {
    inner: Mutex<Bucket>,
}

struct Bucket {
    rate: u64,
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    pub fn new(bytes_per_second: u64) -> Self {
        Self {
            inner: Mutex::new(Bucket {
                rate: bytes_per_second,
                tokens: bytes_per_second as f64,
                last: Instant::now(),
            }),
        }
    }

    pub fn set_rate(&self, bytes_per_second: u64) {
        if let Ok(mut bucket) = self.inner.lock() {
            bucket.rate = bytes_per_second;
            bucket.tokens = bucket.tokens.min(bytes_per_second as f64);
        }
    }

    pub fn rate(&self) -> u64 {
        self.inner.lock().map(|b| b.rate).unwrap_or(0)
    }

    /// Waits until all `bytes` have been paid for, including requests larger than the
    /// one-second burst budget. No debt is left for a later request to repay.
    pub async fn acquire(&self, bytes: u64) {
        let mut remaining = bytes as f64;
        loop {
            let wait = {
                let Ok(mut bucket) = self.inner.lock() else {
                    return;
                };
                if bucket.rate == 0 {
                    return;
                }
                let now = Instant::now();
                let refill = now.duration_since(bucket.last).as_secs_f64() * bucket.rate as f64;
                bucket.last = now;
                // Allow at most one second of burst.
                bucket.tokens = (bucket.tokens + refill).min(bucket.rate as f64);
                let granted = bucket.tokens.min(remaining);
                bucket.tokens -= granted;
                remaining -= granted;
                if remaining <= 0.0 {
                    return;
                }
                Duration::from_secs_f64((remaining / bucket.rate as f64).clamp(0.005, 1.0))
            };
            tokio::time::sleep(wait).await;
        }
    }
}

/// Bytes-per-second estimate over the last few one-second buckets.
#[derive(Debug, Default, Clone)]
pub struct RateMeter {
    buckets: [u64; 5],
    current: u64,
    total: u64,
}

impl RateMeter {
    pub fn add(&mut self, bytes: u64) {
        self.current += bytes;
        self.total += bytes;
    }

    /// Call once per second.
    pub fn tick(&mut self) {
        self.buckets.rotate_right(1);
        self.buckets[0] = self.current;
        self.current = 0;
    }

    pub fn rate(&self) -> u64 {
        self.buckets.iter().sum::<u64>() / self.buckets.len() as u64
    }

    pub fn total(&self) -> u64 {
        self.total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unlimited_never_waits() {
        let limiter = RateLimiter::new(0);
        let start = Instant::now();
        limiter.acquire(1 << 30).await;
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn limited_transfers_are_paced() {
        let limiter = RateLimiter::new(100_000);
        let start = Instant::now();
        // The first second of tokens is free; the next 100 kB must take about a second.
        for _ in 0..20 {
            limiter.acquire(10_000).await;
        }
        let elapsed = start.elapsed();
        assert!(elapsed > Duration::from_millis(600), "{elapsed:?}");
        assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
    }

    #[tokio::test]
    async fn a_single_large_request_pays_for_every_byte() {
        let limiter = RateLimiter::new(100_000);
        let start = Instant::now();
        // One second of burst is free; the final 200 kB must be paid for here.
        limiter.acquire(300_000).await;
        assert!(start.elapsed() >= Duration::from_secs(2));
    }

    #[tokio::test]
    async fn concurrent_requests_share_the_same_budget() {
        let limiter = RateLimiter::new(100_000);
        let start = Instant::now();
        tokio::join!(limiter.acquire(150_000), limiter.acquire(150_000));
        assert!(start.elapsed() >= Duration::from_secs(2));
    }

    #[tokio::test]
    async fn removing_the_limit_releases_a_waiting_request() {
        let limiter = RateLimiter::new(1);
        let request = limiter.acquire(1_000_000);
        let remove_limit = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            limiter.set_rate(0);
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(request, remove_limit);
        })
        .await
        .expect("removing the limit must release an oversized request");
    }

    #[test]
    fn meter_averages_recent_seconds() {
        let mut meter = RateMeter::default();
        for _ in 0..5 {
            meter.add(1000);
            meter.tick();
        }
        assert_eq!(meter.rate(), 1000);
        for _ in 0..5 {
            meter.tick();
        }
        assert_eq!(meter.rate(), 0);
        assert_eq!(meter.total(), 5000);
    }
}
