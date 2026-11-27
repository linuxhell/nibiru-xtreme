//! Small, private origin hints. Hints never override the caller's limits and
//! never authenticate a file or server; TLS and range validators still do that.
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io::Write,
    path::PathBuf,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

const MAX_ENTRIES: usize = 256;
const MAX_BYTES: u64 = 512 * 1024;
const CAPACITY_TTL: u64 = 24 * 3600;
#[cfg(any(feature = "http3", test))]
const PROTOCOL_TTL: u64 = 600;

#[derive(Clone, Default, Serialize, Deserialize)]
struct Hint {
    workers: usize,
    goodput: u64,
    capacity_at: u64,
    protocol: String,
    protocol_at: u64,
}

#[derive(Default, Serialize, Deserialize)]
struct DiskState {
    version: u32,
    entries: BTreeMap<String, Hint>,
}

pub(super) struct Profiles {
    path: Option<PathBuf>,
    state: Mutex<DiskState>,
}

impl Default for Profiles {
    fn default() -> Self {
        Self {
            path: None,
            state: Mutex::new(DiskState {
                version: 1,
                entries: BTreeMap::new(),
            }),
        }
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn recent(at: u64, age: u64, clock: u64) -> bool {
    at > 0 && at <= clock && clock - at < age
}

impl Profiles {
    pub fn open(path: PathBuf) -> Self {
        let mut state = std::fs::metadata(&path)
            .ok()
            .filter(|m| m.len() <= MAX_BYTES)
            .and_then(|_| std::fs::read(&path).ok())
            .and_then(|bytes| serde_json::from_slice::<DiskState>(&bytes).ok())
            .filter(|state| state.version == 1 && state.entries.len() <= MAX_ENTRIES)
            .unwrap_or(DiskState {
                version: 1,
                entries: BTreeMap::new(),
            });
        state.entries.retain(|key, hint| {
            key.len() == 64
                && key.bytes().all(|b| b.is_ascii_hexdigit())
                && hint.workers <= super::MAX_HTTP_WORKERS
                && matches!(hint.protocol.as_str(), "" | "h1" | "h2" | "h3")
        });
        Self {
            path: Some(path),
            state: Mutex::new(state),
        }
    }

    pub fn workers(&self, key: &str, maximum: usize) -> usize {
        let state = self.state.lock().unwrap();
        state
            .entries
            .get(key)
            .filter(|hint| hint.goodput > 0 && recent(hint.capacity_at, CAPACITY_TTL, now()))
            .map_or(maximum, |hint| hint.workers.clamp(1, maximum))
    }

    #[cfg(any(feature = "http3", test))]
    pub fn protocol(&self, key: &str) -> Option<String> {
        self.state
            .lock()
            .unwrap()
            .entries
            .get(key)
            .filter(|hint| {
                recent(hint.protocol_at, PROTOCOL_TTL, now()) && !hint.protocol.is_empty()
            })
            .map(|hint| hint.protocol.clone())
    }

    pub fn remember_protocol(&self, key: String, protocol: &str) {
        if !matches!(protocol, "h1" | "h2" | "h3") {
            return;
        }
        self.change(key, |hint| {
            hint.protocol = protocol.into();
            hint.protocol_at = now();
        });
    }

    /// Learn only from a meaningful completed sample, rather than tiny files,
    /// failed requests, rate-limited downloads or startup bursts.
    pub fn learn(&self, key: String, workers: usize, bytes: u64, millis: u128, limited: bool) {
        if limited
            || bytes < 64 * 1024 * 1024
            || millis < 4_000
            || workers == 0
            || workers > super::MAX_HTTP_WORKERS
        {
            return;
        }
        let rate = (u128::from(bytes) * 1000 / millis).min(u128::from(u64::MAX)) as u64;
        self.change(key, |hint| {
            let expired = !recent(hint.capacity_at, CAPACITY_TTL, now());
            // Within a 3% tie, prefer fewer streams. Slower samples expire and
            // are relearned instead of permanently locking in a bad setting.
            if expired
                || rate >= hint.goodput
                || workers <= hint.workers
                    && u128::from(rate) * 100 >= u128::from(hint.goodput) * 97
            {
                hint.workers = workers;
                hint.goodput = rate;
                hint.capacity_at = now();
            }
        });
    }

    fn change(&self, key: String, change: impl FnOnce(&mut Hint)) {
        let mut state = self.state.lock().unwrap();
        if state.entries.len() >= MAX_ENTRIES
            && !state.entries.contains_key(&key)
            && let Some(oldest) = state
                .entries
                .iter()
                .min_by_key(|(_, hint)| hint.capacity_at.max(hint.protocol_at))
                .map(|(key, _)| key.clone())
        {
            state.entries.remove(&oldest);
        }
        change(state.entries.entry(key).or_default());
        // Serialize writes under this lock: an older flush cannot replace a
        // newer state. Failure only loses a performance hint, never a download.
        if let Some(path) = &self.path {
            let _ = persist(path, &state);
        }
    }
}

fn persist(path: &std::path::Path, state: &DiskState) -> std::io::Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    std::fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer(&mut temporary, state)?;
    temporary.flush()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn key() -> String {
        "a".repeat(64)
    }
    #[test]
    fn useful_capacity_is_persisted_bounded_and_respects_user_limit() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("profiles.json");
        let store = Profiles::open(path.clone());
        store.learn(key(), 8, 128 * 1024 * 1024, 5000, false);
        assert_eq!(store.workers(&key(), 16), 8);
        assert_eq!(store.workers(&key(), 2), 2);
        drop(store);
        assert_eq!(Profiles::open(path).workers(&key(), 16), 8);
    }
    #[test]
    fn tiny_and_throttled_samples_do_not_poison_capacity() {
        let store = Profiles::default();
        store.learn(key(), 2, 1024, 5000, false);
        store.learn(key(), 2, 128 * 1024 * 1024, 5000, true);
        assert_eq!(store.workers(&key(), 16), 16);
        store.learn(key(), 8, 128 * 1024 * 1024, 5000, false);
        store.learn(key(), 2, 64 * 1024 * 1024, 10000, false);
        assert_eq!(store.workers(&key(), 16), 8);
    }
    #[test]
    fn protocols_expire_and_invalid_state_is_ignored() {
        let store = Profiles::default();
        store.remember_protocol(key(), "h3");
        assert_eq!(store.protocol(&key()).as_deref(), Some("h3"));
        store
            .state
            .lock()
            .unwrap()
            .entries
            .get_mut(&key())
            .unwrap()
            .protocol_at = now().saturating_sub(PROTOCOL_TTL);
        assert!(store.protocol(&key()).is_none());
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("profiles.json");
        std::fs::write(&path, b"not json").unwrap();
        assert_eq!(Profiles::open(path).workers(&key(), 4), 4);
    }
}
