//! Bounded HTTP ranges with block-aligned handoffs from busy workers to idle ones.
//!
//! `cursor` includes the block currently being received/written. A helper can only
//! take bytes after it, so a handoff never races a positional write or splits a
//! resumable block. At most one mutex operation is needed per completed 4 MiB.
use super::{RANGE_CHUNK_SIZE, claim_range};
use anyhow::{Result, anyhow};
use std::collections::VecDeque;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

#[derive(Clone, Copy)]
struct Lease {
    cursor: u64,
    end: u64, // exclusive; request headers retain the original inclusive end
}

pub(super) struct Scheduler {
    next: AtomicU64,
    state: Mutex<State>,
    handoffs: AtomicU64,
}

struct State {
    active: Vec<Option<Lease>>,
    pending: VecDeque<(u64, u64)>,
}

impl Scheduler {
    pub fn new(workers: usize) -> Self {
        Self {
            next: AtomicU64::new(0),
            state: Mutex::new(State {
                active: vec![None; workers],
                pending: VecDeque::new(),
            }),
            handoffs: AtomicU64::new(0),
        }
    }

    pub fn with_prefix(workers: usize, bytes: u64) -> Self {
        let scheduler = Self::new(workers);
        scheduler.next.store(bytes, Ordering::Release);
        scheduler.state.lock().unwrap().active[0] = Some(Lease {
            cursor: bytes,
            end: bytes,
        });
        scheduler
    }
    pub fn requeue(&self, worker: usize, committed: u64) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("HTTP range scheduler lock poisoned"))?;
        let lease = state.active[worker]
            .take()
            .ok_or_else(|| anyhow!("HTTP worker has no range lease"))?;
        anyhow::ensure!(
            committed <= lease.end && committed.is_multiple_of(RANGE_CHUNK_SIZE),
            "invalid HTTP retry boundary"
        );
        if committed < lease.end {
            state.pending.push_back((committed, lease.end - 1));
        }
        Ok(())
    }
    pub fn next_offset(&self) -> u64 {
        self.next.load(Ordering::Acquire)
    }

    pub fn handoffs(&self) -> u64 {
        self.handoffs.load(Ordering::Relaxed)
    }

    pub fn claim(
        &self,
        worker: usize,
        total: u64,
        workers: usize,
        done: &[AtomicBool],
    ) -> Result<Option<(u64, u64)>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("HTTP range scheduler lock poisoned"))?;
        state.active[worker] = None;
        let range = if let Some(range) = state
            .pending
            .pop_front()
            .or_else(|| claim_range(&self.next, total, workers, done))
        {
            Some(range)
        } else {
            // All queued bytes have an owner. Take half of the largest unreserved
            // tail, leaving the owner's current block entirely untouched.
            let donor = state
                .active
                .iter()
                .enumerate()
                .filter_map(|(id, lease)| lease.map(|lease| (id, lease)))
                .filter(|(_, lease)| lease.end.saturating_sub(lease.cursor) >= RANGE_CHUNK_SIZE)
                .max_by_key(|(_, lease)| lease.end - lease.cursor);
            donor.map(|(id, lease)| {
                let remaining = lease.end - lease.cursor;
                let split = lease.cursor + remaining / 2 / RANGE_CHUNK_SIZE * RANGE_CHUNK_SIZE;
                state.active[id].as_mut().unwrap().end = split;
                self.handoffs.fetch_add(1, Ordering::Relaxed);
                (split, lease.end - 1)
            })
        };
        if let Some((start, end)) = range {
            state.active[worker] = Some(Lease {
                cursor: start.saturating_add(RANGE_CHUNK_SIZE).min(end + 1),
                end: end + 1,
            });
        }
        Ok(range)
    }

    /// Reserve the next block before the worker receives or writes it. False
    /// means this worker reached its current end (possibly shortened by a helper).
    pub fn advance(&self, worker: usize, offset: u64) -> Result<bool> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("HTTP range scheduler lock poisoned"))?;
        let lease = state.active[worker]
            .as_mut()
            .ok_or_else(|| anyhow!("HTTP worker has no range lease"))?;
        anyhow::ensure!(offset <= lease.end, "HTTP write crossed a range handoff");
        lease.cursor = offset.saturating_add(RANGE_CHUNK_SIZE).min(lease.end);
        Ok(offset < lease.end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const B: u64 = RANGE_CHUNK_SIZE;

    #[test]
    fn helpers_split_only_after_the_owners_reserved_block() {
        let s = Scheduler::new(3);
        assert_eq!(s.claim(0, 8 * B, 1, &[]).unwrap(), Some((0, 8 * B - 1)));
        assert_eq!(s.claim(1, 8 * B, 1, &[]).unwrap(), Some((4 * B, 8 * B - 1)));
        assert!(s.advance(1, 6 * B).unwrap());
        assert_eq!(s.claim(2, 8 * B, 1, &[]).unwrap(), Some((2 * B, 4 * B - 1)));
        assert!(s.advance(0, B).unwrap());
        assert!(!s.advance(0, 2 * B).unwrap());
        assert!(s.advance(0, 3 * B).is_err());
        assert_eq!(s.handoffs(), 2);
    }

    #[test]
    fn resume_holes_are_not_crossed_and_the_last_short_block_is_preserved() {
        let done = [false, true, false, false, true, false].map(AtomicBool::new);
        let s = Scheduler::new(2);
        let total = 5 * B + 17;
        assert_eq!(s.claim(0, total, 1, &done).unwrap(), Some((0, B - 1)));
        assert_eq!(
            s.claim(1, total, 1, &done).unwrap(),
            Some((2 * B, 4 * B - 1))
        );
        assert_eq!(
            s.claim(0, total, 1, &done).unwrap(),
            Some((5 * B, total - 1))
        );
        assert!(!s.advance(0, total).unwrap());
        assert_eq!(
            s.claim(0, total, 1, &done).unwrap(),
            Some((3 * B, 4 * B - 1))
        );
        assert!(!s.advance(1, 3 * B).unwrap());
        assert!(s.claim(1, total, 1, &done).unwrap().is_none());
    }

    #[test]
    fn concurrent_handoffs_write_every_block_once() {
        use std::sync::{Arc, atomic::AtomicU8};
        let total = 2048 * B + 17;
        let scheduler = Arc::new(Scheduler::new(16));
        let writes = Arc::new(
            (0..total.div_ceil(B))
                .map(|_| AtomicU8::new(0))
                .collect::<Vec<_>>(),
        );
        let threads = (0..16)
            .map(|worker| {
                let (scheduler, writes) = (scheduler.clone(), writes.clone());
                std::thread::spawn(move || {
                    while let Some((start, end)) = scheduler.claim(worker, total, 16, &[]).unwrap()
                    {
                        let mut offset = start;
                        loop {
                            assert_eq!(
                                writes[(offset / B) as usize].fetch_add(1, Ordering::AcqRel),
                                0
                            );
                            offset = (offset + B).min(end + 1);
                            std::thread::yield_now();
                            if !scheduler.advance(worker, offset).unwrap() {
                                break;
                            }
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(
            writes
                .iter()
                .all(|count| count.load(Ordering::Acquire) == 1)
        );
    }
}
