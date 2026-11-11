//! Temporary origin caps, learned from repeated refusals and actual receiving peers.
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Notify;
const TTL: Duration = Duration::from_secs(600);
const OBSERVATION: Duration = Duration::from_millis(250);
const RECENT_RECEIVING: Duration = Duration::from_secs(1);
const MAX_ORIGINS: usize = 128;
#[derive(Default)]
pub(super) struct OriginLimits(Mutex<HashMap<String, (usize, Instant)>>);
impl OriginLimits {
    fn get(&self, key: &str, maximum: usize, now: Instant) -> usize {
        let mut entries = self.0.lock().unwrap();
        entries.retain(|_, (_, at)| now.saturating_duration_since(*at) < TTL);
        entries
            .get(key)
            .map_or(maximum, |(limit, _)| maximum.min(*limit))
            .max(1)
    }
    fn remember(&self, key: String, limit: usize, now: Instant) {
        let mut entries = self.0.lock().unwrap();
        entries.retain(|_, (_, at)| now.saturating_duration_since(*at) < TTL);
        if entries.len() >= MAX_ORIGINS
            && !entries.contains_key(&key)
            && let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, (_, at))| *at)
                .map(|(key, _)| key.clone())
        {
            entries.remove(&oldest);
        }
        entries.insert(key, (limit.max(1), now));
    }
}
struct State {
    active: usize,
    healthy: usize,
    peak_healthy: usize,
    refusals: usize,
    limit: usize,
    first_receiving: Option<Instant>,
    last_receiving_end: Option<Instant>,
}
pub(super) struct Budget {
    state: Mutex<State>,
    changed: Notify,
    origins: Arc<OriginLimits>,
    key: String,
}
pub(super) struct Permit {
    budget: Arc<Budget>,
    healthy: bool,
}
impl Budget {
    pub fn new(maximum: usize, origins: Arc<OriginLimits>, key: String) -> Arc<Self> {
        let limit = origins.get(&key, maximum, Instant::now());
        Arc::new(Self {
            state: Mutex::new(State {
                active: 0,
                healthy: 0,
                peak_healthy: 0,
                refusals: 0,
                limit,
                first_receiving: None,
                last_receiving_end: None,
            }),
            changed: Notify::new(),
            origins,
            key,
        })
    }
    pub async fn enter(self: &Arc<Self>) -> Permit {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.state.lock().unwrap();
                if state.active < state.limit {
                    state.active += 1;
                    return Permit {
                        budget: self.clone(),
                        healthy: false,
                    };
                }
            }
            changed.await;
        }
    }
    pub fn refused(&self) {
        self.refused_at(Instant::now());
    }
    fn refused_at(&self, now: Instant) {
        let mut state = self.state.lock().unwrap();
        if state.healthy == 0
            && !state
                .last_receiving_end
                .is_some_and(|at| now.saturating_duration_since(at) <= RECENT_RECEIVING)
        {
            state.refusals = 0;
            return;
        }
        state.refusals += 1;
        if state.refusals >= 3
            && state
                .first_receiving
                .is_some_and(|at| now.saturating_duration_since(at) >= OBSERVATION)
        {
            let useful = state.peak_healthy.min(state.limit).max(1);
            if useful < state.limit {
                state.limit = useful;
                self.origins.remember(self.key.clone(), useful, now);
            }
        }
    }
    pub fn limit(&self) -> usize {
        self.state.lock().unwrap().limit
    }
}
impl Permit {
    pub fn accepted(&mut self) {
        if !self.healthy {
            let mut state = self.budget.state.lock().unwrap();
            state.healthy += 1;
            state.peak_healthy = state.peak_healthy.max(state.healthy);
            state.first_receiving.get_or_insert_with(Instant::now);
            self.healthy = true;
        }
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock().unwrap();
        state.active -= 1;
        if self.healthy {
            state.healthy -= 1;
            // A server can refuse the next request between response leases.
            // Retain recently observed receiving peers across that short gap.
            state.last_receiving_end = Some(Instant::now());
        }
        drop(state);
        self.budget.changed.notify_waiters();
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn only_repeated_refusals_with_successful_peers_teach_a_limit() {
        let cache = Arc::new(OriginLimits::default());
        let budget = Budget::new(8, cache.clone(), "origin".into());
        for _ in 0..3 {
            budget.refused();
        }
        assert_eq!(budget.limit(), 8);
        let mut healthy = budget.enter().await;
        healthy.accepted();
        budget.refused();
        assert_eq!(budget.limit(), 8);
        budget.refused();
        budget.refused_at(Instant::now() + OBSERVATION);
        assert_eq!(budget.limit(), 1);
        assert_eq!(Budget::new(8, cache.clone(), "origin".into()).limit(), 1);
        assert_eq!(
            Budget::new(8, cache, "another port or session".into()).limit(),
            8
        );
    }
    #[tokio::test]
    async fn early_refusals_do_not_hide_a_second_useful_connection() {
        let budget = Budget::new(8, Arc::default(), "origin".into());
        let mut first = budget.enter().await;
        first.accepted();
        let began = Instant::now();
        for _ in 0..6 {
            budget.refused_at(began);
        }
        assert_eq!(budget.limit(), 8);
        let mut second = budget.enter().await;
        second.accepted();
        drop(first);
        budget.refused_at(began + OBSERVATION);
        assert_eq!(budget.limit(), 2);
    }
    #[tokio::test]
    async fn a_gap_between_responses_retains_recent_evidence_but_not_stale_evidence() {
        for recent in [true, false] {
            let budget = Budget::new(8, Arc::default(), "origin".into());
            let mut first = budget.enter().await;
            first.accepted();
            let mut second = budget.enter().await;
            second.accepted();
            drop(first);
            drop(second);
            let now = Instant::now()
                + if recent {
                    OBSERVATION
                } else {
                    RECENT_RECEIVING + Duration::from_secs(1)
                };
            for _ in 0..3 {
                budget.refused_at(now);
            }
            assert_eq!(budget.limit(), if recent { 2 } else { 8 });
        }
    }
    #[test]
    fn caps_expire_and_never_raise_the_users_maximum() {
        let cache = OriginLimits::default();
        let now = Instant::now();
        cache.remember("origin".into(), 4, now);
        assert_eq!(cache.get("origin", 1, now), 1);
        assert_eq!(cache.get("origin", 8, now), 4);
        assert_eq!(cache.get("origin", 8, now + TTL), 8);
    }
    #[tokio::test]
    async fn cancelled_waiters_do_not_leak_capacity() {
        let budget = Budget::new(1, Arc::default(), "origin".into());
        let first = budget.enter().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), budget.enter())
                .await
                .is_err()
        );
        drop(first);
        let _next = tokio::time::timeout(Duration::from_secs(1), budget.enter())
            .await
            .unwrap();
    }
}
