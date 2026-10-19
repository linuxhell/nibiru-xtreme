//! Bounded, opt-in concurrency experiments; no learned capacity guarantee.
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use tokio::sync::watch;

const WINDOW_MS: u64 = 4_000;
const HOLD_MS: u64 = 12_000;
const MIN_SAMPLE_BYTES: u64 = 16 * 1024 * 1024;

enum Phase {
    Baseline,
    Trial {
        previous: usize,
        reference: u64,
        increasing: bool,
    },
    Hold {
        until_ms: u64,
    },
    Tail,
}

pub(super) struct Decision {
    pub previous: usize,
    pub target: usize,
    pub measured_bps: u64,
    pub reference_bps: u64,
    pub reason: &'static str,
}

pub(super) struct Controller {
    maximum: usize,
    target: usize,
    phase: Phase,
    window_ms: u64,
    window_bytes: u64,
}

impl Controller {
    pub fn new(maximum: usize) -> Self {
        Self {
            maximum,
            target: maximum,
            phase: Phase::Baseline,
            window_ms: 0,
            window_bytes: 0,
        }
    }

    pub fn target(&self) -> usize {
        self.target
    }

    pub fn observe(
        &mut self,
        now_ms: u64,
        written: u64,
        active: u32,
        unclaimed: u64,
    ) -> Option<Decision> {
        if matches!(self.phase, Phase::Tail) {
            return None;
        }
        if unclaimed <= super::MAX_HTTP_RANGE_SIZE * self.target as u64 {
            let duration = now_ms.saturating_sub(self.window_ms);
            let bytes = written.saturating_sub(self.window_bytes);
            let rate = (u128::from(bytes) * 1_000 / u128::from(duration.max(1)))
                .min(u128::from(u64::MAX)) as u64;
            let allowed = (1_u32 << self.target) - 1;
            let mut next = self.target;
            let mut reference_bps = 0;
            if let Phase::Trial {
                previous,
                reference,
                increasing,
            } = self.phase
            {
                reference_bps = reference;
                // Do not carry an unvalidated or slower probe into the tail.
                if duration < 2_000
                    || bytes < MIN_SAMPLE_BYTES
                    || active & !allowed != 0
                    || !acceptable(rate, reference, increasing)
                {
                    next = previous;
                }
            }
            self.phase = Phase::Tail;
            return Some(self.decision(next, rate, reference_bps, "freeze_at_tail"));
        }
        let allowed = (1_u32 << self.target) - 1;
        // Do not compare a new limit while requests from the old limit drain.
        if now_ms < 2_000
            || active & !allowed != 0
            || (active & allowed).count_ones() as usize + 1 < self.target
        {
            self.reset_window(now_ms, written);
            return None;
        }
        let duration = now_ms.saturating_sub(self.window_ms);
        let bytes = written.saturating_sub(self.window_bytes);
        if duration < WINDOW_MS || bytes < MIN_SAMPLE_BYTES {
            return None;
        }
        let rate =
            (u128::from(bytes) * 1_000 / u128::from(duration)).min(u128::from(u64::MAX)) as u64;
        self.reset_window(now_ms, written);
        match self.phase {
            Phase::Baseline => {
                let next = (self.target / 2).max(2);
                if next >= self.target {
                    self.phase = Phase::Hold { until_ms: u64::MAX };
                    return None;
                }
                self.phase = Phase::Trial {
                    previous: self.target,
                    reference: rate,
                    increasing: false,
                };
                Some(self.decision(next, rate, rate, "probe_fewer"))
            }
            Phase::Trial {
                previous,
                reference,
                increasing,
            } => {
                if !acceptable(rate, reference, increasing) {
                    self.phase = Phase::Hold {
                        until_ms: now_ms.saturating_add(HOLD_MS),
                    };
                    return Some(self.decision(previous, rate, reference, "restore_previous"));
                }
                let next = (self.target / 2).max(2);
                if !increasing && next < self.target {
                    self.phase = Phase::Trial {
                        previous: self.target,
                        reference: rate,
                        increasing: false,
                    };
                    Some(self.decision(next, rate, reference, "keep_gain_probe_fewer"))
                } else {
                    self.phase = Phase::Hold {
                        until_ms: now_ms.saturating_add(HOLD_MS),
                    };
                    Some(self.decision(self.target, rate, reference, "keep_candidate"))
                }
            }
            Phase::Hold { until_ms } if now_ms >= until_ms && self.target < self.maximum => {
                let next = (self.target * 2).min(self.maximum);
                self.phase = Phase::Trial {
                    previous: self.target,
                    reference: rate,
                    increasing: true,
                };
                Some(self.decision(next, rate, rate, "probe_more"))
            }
            Phase::Hold { .. } | Phase::Tail => None,
        }
    }

    fn reset_window(&mut self, now_ms: u64, written: u64) {
        self.window_ms = now_ms;
        self.window_bytes = written;
    }

    fn decision(
        &mut self,
        next: usize,
        rate: u64,
        reference: u64,
        reason: &'static str,
    ) -> Decision {
        let previous = self.target;
        self.target = next;
        Decision {
            previous,
            target: next,
            measured_bps: rate,
            reference_bps: reference,
            reason,
        }
    }
}

fn acceptable(rate: u64, reference: u64, increasing: bool) -> bool {
    if increasing {
        u128::from(rate) * 100 > u128::from(reference) * 108
    } else {
        u128::from(rate) * 100 >= u128::from(reference) * 97
    }
}

pub(super) struct Gate {
    pub limit: watch::Receiver<usize>,
    pub wake_all: watch::Sender<usize>,
    pub maximum: usize,
    pub active: Arc<AtomicU32>,
    pub worker: usize,
}

impl Gate {
    pub async fn enter(&mut self) -> Option<usize> {
        loop {
            let limit = *self.limit.borrow_and_update();
            if self.worker < limit {
                return Some(limit);
            }
            if self.limit.changed().await.is_err() {
                return None;
            }
        }
    }

    pub fn track(&self) -> ActiveRange {
        let bit = 1_u32 << self.worker;
        self.active.fetch_or(bit, Ordering::AcqRel);
        ActiveRange {
            active: self.active.clone(),
            bit,
        }
    }
}

pub(super) struct ActiveRange {
    active: Arc<AtomicU32>,
    bit: u32,
}

impl Drop for ActiveRange {
    fn drop(&mut self) {
        self.active.fetch_and(!self.bit, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const M: u64 = 1024 * 1024;
    const FAR: u64 = 100 * 1024 * M;

    #[test]
    fn reduces_only_after_observing_stable_work_and_restores_a_loss() {
        let mut c = Controller::new(16);
        assert!(c.observe(1_000, 100 * M, 0xffff, FAR).is_none());
        assert_eq!(c.observe(5_000, 500 * M, 0xffff, FAR).unwrap().target, 8);
        // A draining old request must reset the candidate measurement.
        assert!(c.observe(6_000, 600 * M, 0xffff, FAR).is_none());
        assert!(c.observe(8_000, 800 * M, 0xff, FAR).is_none());
        assert_eq!(c.observe(10_000, 1_000 * M, 0xff, FAR).unwrap().target, 4);
        let loss = c.observe(14_000, 1_200 * M, 0xf, FAR).unwrap();
        assert_eq!(loss.reason, "restore_previous");
        assert_eq!(loss.target, 8);
        assert_eq!(c.observe(26_000, 2_400 * M, 0xff, FAR).unwrap().target, 16);
        let gain = c.observe(30_000, 3_000 * M, 0xffff, FAR).unwrap();
        assert_eq!(gain.reason, "keep_candidate");
        assert_eq!(gain.target, 16);
    }

    #[test]
    fn waits_for_bytes_and_freezes_without_ramping_up_at_tail() {
        let mut c = Controller::new(12);
        assert!(c.observe(1_000, 0, 0xfff, FAR).is_none());
        assert!(c.observe(5_000, M, 0xfff, FAR).is_none());
        assert_eq!(c.observe(6_000, 100 * M, 0xfff, FAR).unwrap().target, 6);
        assert_eq!(
            c.observe(9_000, 220 * M, 0x3f, M).unwrap().reason,
            "freeze_at_tail"
        );
        assert!(c.observe(50_000, 120 * M, 0x3f, 0).is_none());
        assert_eq!(c.target(), 6);
    }

    #[test]
    fn tail_restores_an_unvalidated_probe() {
        let mut c = Controller::new(16);
        c.observe(1_000, 100 * M, 0xffff, FAR);
        assert_eq!(c.observe(5_000, 500 * M, 0xffff, FAR).unwrap().target, 8);
        assert_eq!(c.observe(6_000, 510 * M, 0xff, 0).unwrap().target, 16);
    }

    #[tokio::test]
    async fn parked_worker_wakes_and_active_mask_is_cleared_on_drop() {
        let (limit, receiver) = watch::channel(2);
        let active = Arc::new(AtomicU32::new(0));
        let mut gate = Gate {
            limit: receiver,
            wake_all: limit.clone(),
            maximum: 4,
            active: active.clone(),
            worker: 3,
        };
        let task = tokio::spawn(async move {
            assert_eq!(gate.enter().await, Some(4));
            let guard = gate.track();
            assert_eq!(gate.active.load(Ordering::Acquire), 8);
            drop(guard);
        });
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        limit.send_replace(4);
        tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(active.load(Ordering::Acquire), 0);
    }
}
