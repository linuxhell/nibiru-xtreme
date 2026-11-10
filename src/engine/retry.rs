//! Bounded retries for idempotent range GETs, before inspecting file identity.
use crate::logging::LogSink;
use anyhow::{Context, Result, anyhow};
use reqwest::{RequestBuilder, Response, StatusCode, header};
use std::{
    fmt,
    time::{Duration, SystemTime},
};

const MAX_RETRIES: usize = 5;
const MAX_INLINE_WAIT: Duration = Duration::from_secs(30);

/// Keep throttling distinguishable from unsupported ranges: falling back to a
/// fresh GET would immediately bypass the same server's backoff instruction.
#[derive(Debug)]
pub(super) struct Throttled {
    status: StatusCode,
    wait: Duration,
}

impl fmt::Display for Throttled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HTTP range request refused: {}", self.status)?;
        if self.wait > MAX_INLINE_WAIT {
            write!(
                f,
                "; Retry-After {}s exceeds the inline wait budget",
                self.wait.as_secs()
            )
        } else {
            write!(f, "; retry budget exhausted")
        }
    }
}

impl std::error::Error for Throttled {}

fn delay(headers: &header::HeaderMap, attempt: usize, now: SystemTime) -> Duration {
    if let Some(value) = headers
        .get(header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
    {
        let value = value.trim();
        if let Ok(seconds) = value.parse::<u64>() {
            return Duration::from_secs(seconds);
        }
        if let Ok(at) = httpdate::parse_http_date(value) {
            return at.duration_since(now).unwrap_or(Duration::ZERO);
        }
    }
    Duration::from_millis(250 * (1u64 << attempt.min(MAX_RETRIES)))
}

pub(super) async fn range_response(
    request: RequestBuilder,
    log: &LogSink,
    worker: usize,
) -> Result<Response> {
    for attempt in 0..=MAX_RETRIES {
        let response = request
            .try_clone()
            .ok_or_else(|| anyhow!("range GET cannot be retried"))?
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .with_context(|| format!("range worker {worker} request failed"))?;
        let status = response.status();
        if !matches!(
            status,
            StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
        ) {
            return Ok(response);
        }
        let wait = delay(response.headers(), attempt, SystemTime::now());
        if attempt == MAX_RETRIES || wait > MAX_INLINE_WAIT {
            return Err(Throttled { status, wait }.into());
        }
        log.warn(
            "http.range_retry",
            &format!(
                "worker={worker} status={} retry={} wait_ms={}",
                status.as_u16(),
                attempt + 1,
                wait.as_millis()
            ),
        );
        drop(response);
        // The owning JoinSet aborts this sleep on cancellation, just as it does
        // a pending request. No thread or blocking sleep is retained.
        tokio::time::sleep(wait).await;
    }
    unreachable!("the last refused response returns a throttling error")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_accepts_seconds_dates_and_past_dates() {
        let now = httpdate::parse_http_date("Tue, 10 Nov 2026 20:00:00 GMT").unwrap();
        for (value, expected) in [
            ("2", 2),
            ("Tue, 10 Nov 2026 20:00:03 GMT", 3),
            ("Tue, 10 Nov 2026 19:59:59 GMT", 0),
        ] {
            let mut headers = header::HeaderMap::new();
            headers.insert(header::RETRY_AFTER, value.parse().unwrap());
            assert_eq!(delay(&headers, 0, now), Duration::from_secs(expected));
        }
    }

    #[test]
    fn absent_or_invalid_retry_after_uses_bounded_backoff() {
        let mut headers = header::HeaderMap::new();
        for value in [None, Some("not a date")] {
            if let Some(value) = value {
                headers.insert(header::RETRY_AFTER, value.parse().unwrap());
            }
            assert_eq!(
                delay(&headers, 0, SystemTime::UNIX_EPOCH),
                Duration::from_millis(250)
            );
            assert_eq!(
                delay(&headers, 4, SystemTime::UNIX_EPOCH),
                Duration::from_secs(4)
            );
        }
    }

    #[test]
    fn enormous_waits_are_detected_before_scheduling_a_timer() {
        let mut headers = header::HeaderMap::new();
        headers.insert(header::RETRY_AFTER, u64::MAX.to_string().parse().unwrap());
        assert!(delay(&headers, 0, SystemTime::UNIX_EPOCH) > MAX_INLINE_WAIT);
    }
}
