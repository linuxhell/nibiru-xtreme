//! Local BLAKE3 block evidence; publisher SHA-256/signatures remain separate.
use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Default)]
pub(crate) struct BlockDigests(Mutex<BTreeMap<usize, String>>);

impl BlockDigests {
    #[cfg(test)]
    pub fn record(&self, index: usize, bytes: &[u8]) {
        self.record_hash(index, blake3::hash(bytes).to_hex().to_string());
    }
    pub fn record_hash(&self, index: usize, hash: String) {
        self.0.lock().unwrap().insert(index, hash);
    }
    pub fn snapshot(&self, completed: &[bool]) -> BTreeMap<usize, String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(index, _)| completed.get(**index) == Some(&true))
            .map(|(index, hash)| (*index, hash.clone()))
            .collect()
    }
    pub fn restored(hashes: BTreeMap<usize, String>, completed: &[bool]) -> Self {
        Self(Mutex::new(
            hashes
                .into_iter()
                .filter(|(index, _)| completed.get(*index) == Some(&true))
                .collect(),
        ))
    }
}

/// Only matching completed blocks are retained. Missing evidence and corrupt
/// blocks are re-fetched; bytes outside the saved bitmap are never trusted.
pub(crate) fn verify(
    path: &Path,
    total: u64,
    block_size: u64,
    completed: &mut [bool],
    hashes: &BTreeMap<usize, String>,
    cancelled: &AtomicBool,
) -> std::io::Result<usize> {
    let mut file = File::open(path)?;
    let mut buffer = vec![0; 128 * 1024];
    let mut rejected = 0;
    for (index, done) in completed.iter_mut().enumerate().filter(|(_, done)| **done) {
        if cancelled.load(Ordering::Acquire) {
            return Err(std::io::ErrorKind::Interrupted.into());
        }
        let offset = (index as u64)
            .checked_mul(block_size)
            .ok_or(std::io::ErrorKind::InvalidData)?;
        let expected = hashes
            .get(&index)
            .and_then(|hash| blake3::Hash::from_hex(hash).ok());
        let mut remaining = total.saturating_sub(offset).min(block_size);
        if expected.is_none() || remaining == 0 {
            *done = false;
            rejected += 1;
            continue;
        }
        file.seek(SeekFrom::Start(offset))?;
        let mut hasher = blake3::Hasher::new();
        while remaining > 0 {
            if cancelled.load(Ordering::Acquire) {
                return Err(std::io::ErrorKind::Interrupted.into());
            }
            let length = remaining.min(buffer.len() as u64) as usize;
            let read = file.read(&mut buffer[..length])?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            remaining -= read as u64;
        }
        if remaining != 0 || Some(hasher.finalize()) != expected {
            *done = false;
            rejected += 1;
        }
    }
    Ok(rejected)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn corrupted_and_unproven_blocks_are_refetched_without_discarding_good_blocks() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("partial");
        std::fs::write(&path, b"aaaaBBBBcccc").unwrap();
        let digests = BlockDigests::default();
        digests.record(0, b"aaaa");
        digests.record(1, b"bbbb");
        let mut completed = vec![true, true, true];
        let hashes = digests.snapshot(&completed);
        assert_eq!(
            verify(
                &path,
                12,
                4,
                &mut completed,
                &hashes,
                &AtomicBool::new(false)
            )
            .unwrap(),
            2
        );
        assert_eq!(completed, vec![true, false, false]);
        assert_eq!(
            BlockDigests::restored(hashes, &completed)
                .snapshot(&completed)
                .len(),
            1
        );
    }
    #[test]
    fn partial_final_block_and_cancellation_are_handled() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("partial");
        std::fs::write(&path, b"aaaabb").unwrap();
        let digests = BlockDigests::default();
        digests.record(1, b"bb");
        let hashes = digests.snapshot(&[false, true]);
        let mut completed = vec![false, true];
        assert_eq!(
            verify(
                &path,
                6,
                4,
                &mut completed,
                &hashes,
                &AtomicBool::new(false)
            )
            .unwrap(),
            0
        );
        assert_eq!(
            verify(&path, 6, 4, &mut completed, &hashes, &AtomicBool::new(true))
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::Interrupted
        );
    }
}
