//! Disk layout for a torrent: maps the contiguous byte space onto files, writes verified
//! pieces, serves reads for uploading, and re-checks existing data when resuming.

use super::{bitfield::Bitfield, metainfo::Torrent};
use anyhow::{Context, Result, bail};
use sha1::{Digest, Sha1};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io,
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

/// Open file handles kept at once; torrents can have thousands of files.
const MAX_OPEN: usize = 64;

#[cfg(unix)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !buf.is_empty() {
        let n = file.read_at(buf, offset)?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        buf = &mut buf[n..];
        offset += n as u64;
    }
    Ok(())
}

#[cfg(unix)]
fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !buf.is_empty() {
        let n = file.write_at(buf, offset)?;
        if n == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        buf = &buf[n..];
        offset += n as u64;
    }
    Ok(())
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        let n = file.seek_read(buf, offset)?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        buf = &mut buf[n..];
        offset += n as u64;
    }
    Ok(())
}

#[cfg(windows)]
fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        let n = file.seek_write(buf, offset)?;
        if n == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        buf = &buf[n..];
        offset += n as u64;
    }
    Ok(())
}

struct Slot {
    path: PathBuf,
    length: u64,
    offset: u64,
    pad: bool,
}

pub struct Storage {
    download_root: PathBuf,
    root: PathBuf,
    slots: Vec<Slot>,
    piece_length: u64,
    total: u64,
    pieces: Vec<[u8; 20]>,
    count: usize,
    v2: Option<super::v2::V2Info>,
    v2_mode: bool,
    v2_hashes: Mutex<Vec<Option<[u8; 32]>>>,
    open: Mutex<HashMap<(usize, bool), std::sync::Arc<File>>>,
    mutation: Mutex<()>,
    deleted: AtomicBool,
}

impl Storage {
    /// `directory` is the download directory; multi-file torrents get a folder named after the torrent.
    pub fn new(torrent: &Torrent, directory: &Path) -> Self {
        let root = if torrent.is_multi_file() {
            directory.join(&torrent.name)
        } else {
            directory.to_owned()
        };
        let slots = torrent
            .files
            .iter()
            .map(|f| Slot {
                path: f.path.iter().fold(root.clone(), |p, c| p.join(c)),
                length: f.length,
                offset: f.offset,
                pad: f.pad,
            })
            .collect();
        Self {
            download_root: directory.to_owned(),
            root,
            slots,
            piece_length: torrent.piece_length,
            total: torrent.total_length,
            pieces: torrent.pieces.clone(),
            count: torrent.piece_count(),
            v2: torrent.v2.clone(),
            v2_mode: torrent.wire_v2,
            v2_hashes: Mutex::new(
                torrent
                    .v2
                    .as_ref()
                    .map(|v| v.hashes.clone())
                    .unwrap_or_default(),
            ),
            open: Mutex::new(HashMap::new()),
            mutation: Mutex::new(()),
            deleted: AtomicBool::new(false),
        }
    }

    /// Empty files carry no blocks and would otherwise never be created.
    pub fn create_empty_files(&self, selected: Option<&[usize]>) -> Result<()> {
        for (i, slot) in self.slots.iter().enumerate() {
            if !slot.pad && slot.length == 0 && selected.is_none_or(|s| s.contains(&i)) {
                self.handle(i, true)?;
            }
        }
        Ok(())
    }

    pub fn piece_length(&self) -> u64 {
        self.piece_length
    }

    pub fn path_of(&self, file: usize) -> &Path {
        &self.slots[file].path
    }

    pub fn piece_len(&self, index: usize) -> u64 {
        if self.v2_mode {
            return self.v2.as_ref().map_or(0, |v| v.piece_len(index));
        }
        self.total
            .saturating_sub(index as u64 * self.piece_length)
            .min(self.piece_length)
    }

    /// Files (index, offset inside the file, length) overlapped by `[start, start + len)`.
    fn spans(&self, start: u64, len: u64) -> Vec<(usize, u64, u64)> {
        let end = start + len;
        // First file whose end is after `start`.
        let first = self.slots.partition_point(|s| s.offset + s.length <= start);
        let mut out = Vec::new();
        for (i, slot) in self.slots.iter().enumerate().skip(first) {
            if slot.offset >= end {
                break;
            }
            if slot.length == 0 {
                continue;
            }
            let from = start.max(slot.offset);
            let to = end.min(slot.offset + slot.length);
            if to > from {
                out.push((i, from - slot.offset, to - from));
            }
        }
        out
    }

    fn validate_path(&self, index: usize, create: bool) -> Result<()> {
        let slot = &self.slots[index];
        if create {
            std::fs::create_dir_all(&self.download_root)?;
        }
        let real_root = std::fs::canonicalize(&self.download_root)?;
        // The configured download folder may itself be a user-chosen alias, but
        // torrent-controlled descendants and file names must never be links.
        for path in slot
            .path
            .ancestors()
            .take_while(|path| *path != self.download_root)
        {
            match std::fs::symlink_metadata(path) {
                Ok(meta) if meta.file_type().is_symlink() => {
                    bail!("torrent path contains a symbolic link")
                }
                Ok(meta) if path == slot.path && !meta.is_file() => {
                    bail!("torrent output is not a regular file")
                }
                Ok(_) => {}
                Err(error) if create && error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        if let Some(parent) = slot.path.parent() {
            if create {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("cannot create {}", parent.display()))?;
            }
            let real_parent = std::fs::canonicalize(parent)?;
            if !real_parent.starts_with(&real_root) {
                bail!("refusing to access outside the download directory");
            }
        }
        Ok(())
    }

    fn handle(&self, index: usize, create: bool) -> Result<std::sync::Arc<File>> {
        let mut open = self.open.lock().map_err(|e| anyhow::anyhow!("{e}"))?;
        if let Some(file) = open.get(&(index, create)) {
            return Ok(file.clone());
        }
        self.validate_path(index, create)?;
        let slot = &self.slots[index];
        let file = if create {
            let existed = slot.path.exists();
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&slot.path)
                .with_context(|| format!("cannot open {}", slot.path.display()))?;
            if !existed || file.metadata()?.len() < slot.length {
                file.set_len(slot.length)?;
            }
            file
        } else {
            OpenOptions::new().read(true).open(&slot.path)?
        };
        if open.len() >= MAX_OPEN
            && let Some(&victim) = open.keys().next()
        {
            open.remove(&victim);
        }
        let file = std::sync::Arc::new(file);
        open.insert((index, create), file.clone());
        Ok(file)
    }

    /// Writes bytes at `offset` in the torrent's byte space (may cross files). Padding is skipped.
    pub fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        if offset
            .checked_add(data.len() as u64)
            .is_none_or(|end| end > self.total)
        {
            bail!("write beyond the end of the torrent");
        }
        let _mutation = self
            .mutation
            .lock()
            .map_err(|_| anyhow::anyhow!("storage mutation lock poisoned"))?;
        if self.deleted.load(Ordering::Acquire) {
            bail!("torrent storage has been deleted");
        }
        let mut consumed = 0usize;
        let mut cursor = offset;
        for (file, inner, len) in self.spans(offset, data.len() as u64) {
            // Bytes before this file (belonging to padding) are skipped.
            let slot_start = self.slots[file].offset + inner;
            consumed += (slot_start - cursor) as usize;
            let chunk = &data[consumed..consumed + len as usize];
            if !self.slots[file].pad {
                write_all_at(&*self.handle(file, true)?, chunk, inner)?;
            }
            consumed += len as usize;
            cursor = slot_start + len;
        }
        Ok(())
    }

    /// Reads `len` bytes at `offset`; padding reads as zeros. Fails if data is missing.
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        if offset.checked_add(len).is_none_or(|end| end > self.total) {
            bail!("read beyond the end of the torrent");
        }
        let mut out = vec![0u8; len as usize];
        for (file, inner, n) in self.spans(offset, len) {
            if self.slots[file].pad {
                continue;
            }
            let at = (self.slots[file].offset + inner - offset) as usize;
            read_exact_at(
                &*self.handle(file, false)?,
                &mut out[at..at + n as usize],
                inner,
            )
            .with_context(|| format!("cannot read {}", self.slots[file].path.display()))?;
        }
        Ok(out)
    }

    pub fn read_piece(&self, index: usize) -> Result<Vec<u8>> {
        self.read(index as u64 * self.piece_length, self.piece_len(index))
    }

    pub fn piece_matches(&self, index: usize, data: &[u8]) -> bool {
        if index >= self.count || data.len() as u64 != self.piece_len(index) {
            return false;
        }
        if !self.pieces.is_empty() {
            let legacy_length = self
                .total
                .saturating_sub(index as u64 * self.piece_length)
                .min(self.piece_length) as usize;
            let matches = if legacy_length == data.len() {
                Sha1::digest(data).as_slice() == self.pieces[index]
            } else {
                let mut legacy = data.to_vec();
                legacy.resize(legacy_length, 0);
                Sha1::digest(&legacy).as_slice() == self.pieces[index]
            };
            if !matches {
                return false;
            }
        }
        if let Some(v2) = &self.v2 {
            let expected = self
                .v2_hashes
                .lock()
                .ok()
                .and_then(|h| h.get(index).copied().flatten());
            let Some(expected) = expected else {
                return !self.v2_mode;
            };
            let length = v2.piece_len(index) as usize;
            if length > data.len()
                || super::v2::piece_root(&data[..length], v2.leaf_capacity(index)).ok()
                    != Some(expected)
            {
                return false;
            }
        }
        true
    }

    pub fn v2_piece_known(&self, index: usize) -> bool {
        !self.v2_mode
            || self.v2.is_none()
            || self
                .v2_hashes
                .lock()
                .ok()
                .and_then(|h| h.get(index).copied().flatten())
                .is_some()
    }

    pub fn v2_hashes(&self) -> Vec<Option<[u8; 32]>> {
        self.v2_hashes.lock().map(|h| h.clone()).unwrap_or_default()
    }

    pub fn metadata_complete(&self) -> bool {
        !self.v2_mode
            || self
                .v2_hashes
                .lock()
                .is_ok_and(|h| h.iter().all(Option::is_some))
    }

    /// The caller supplies a complete authenticated piece layer, never raw
    /// untrusted hashes. Network proof blocks are verified before reaching here.
    pub fn install_v2_layer(&self, root: &[u8; 32], hashes: &[[u8; 32]]) -> Result<()> {
        let v2 = self.v2.as_ref().context("not a v2 torrent")?;
        let file = v2
            .files
            .iter()
            .find(|f| &f.root == root)
            .context("unknown v2 file root")?;
        v2.validate_layer(file, hashes)?;
        let mut known = self
            .v2_hashes
            .lock()
            .map_err(|_| anyhow::anyhow!("hash table poisoned"))?;
        for (i, h) in hashes.iter().enumerate() {
            known[file.first_piece + i] = Some(*h);
        }
        Ok(())
    }

    pub fn install_v2_proof(
        &self,
        request: &super::message::HashRequest,
        hashes: &[[u8; 32]],
    ) -> Result<Vec<usize>> {
        let v2 = self.v2.as_ref().context("not a v2 torrent")?;
        let file = v2
            .files
            .iter()
            .find(|f| f.root == request.root)
            .context("unknown v2 file root")?;
        v2.verify_response(file, request, hashes)?;
        let mut known = self
            .v2_hashes
            .lock()
            .map_err(|_| anyhow::anyhow!("hash table poisoned"))?;
        let mut added = vec![];
        for file in v2.files.iter().filter(|f| f.root == request.root) {
            for (i, hash) in hashes[..request.length as usize].iter().enumerate() {
                let piece = request.index as usize + i;
                if piece >= file.pieces {
                    break;
                }
                let index = file.first_piece + piece;
                if known[index].is_none() {
                    known[index] = Some(*hash);
                    added.push(index);
                }
            }
        }
        Ok(added)
    }

    /// Serve leaf or piece-layer hashes, with proofs, without reading whole
    /// files. Reading data below the piece layer requires a verified piece.
    pub fn hash_response(
        &self,
        request: &super::message::HashRequest,
        have: &Bitfield,
    ) -> Result<Vec<[u8; 32]>> {
        let v2 = self.v2.as_ref().context("not a v2 torrent")?;
        let file = v2
            .files
            .iter()
            .find(|f| f.root == request.root)
            .context("unknown file root")?;
        let (level, proofs) = v2.geometry(file, request)?;
        let known = self.v2_hashes();
        let node = |layer: u32, index: u32| -> Result<[u8; 32]> {
            let base = (v2.piece_length / super::v2::BLOCK).ilog2();
            if layer >= base && file.pieces > 1 {
                let width = 1usize
                    .checked_shl(layer - base)
                    .context("Merkle width overflow")?;
                if width > 1_048_576 {
                    bail!("Merkle proof exceeds bounds");
                }
                let start = (index as usize)
                    .checked_mul(width)
                    .context("Merkle offset overflow")?;
                let padding = super::v2::zero_at_layer(base);
                let mut hashes = Vec::with_capacity(width);
                for i in start..start + width {
                    hashes.push(if i < file.pieces {
                        known[file.first_piece + i].context("piece hash not available")?
                    } else {
                        padding
                    });
                }
                super::v2::root(&hashes, padding)
            } else {
                let bytes = super::v2::BLOCK
                    .checked_shl(layer)
                    .context("Merkle span overflow")?;
                let start = (index as u64)
                    .checked_mul(bytes)
                    .context("Merkle offset overflow")?;
                if start >= file.length {
                    return Ok(super::v2::zero_at_layer(layer));
                }
                let len = bytes.min(file.length - start);
                let first = file.first_piece + (start / v2.piece_length) as usize;
                let last = file.first_piece + ((start + len - 1) / v2.piece_length) as usize;
                if (first..=last).any(|p| !have.get(p)) {
                    bail!("data for hash request is not verified");
                }
                let data = self.read(file.offset + start, len)?;
                super::v2::piece_root(&data, 1usize << layer)
            }
        };
        let mut out = Vec::with_capacity(request.length as usize + proofs as usize);
        for i in request.index..request.index + request.length {
            out.push(node(request.base_layer, i)?);
        }
        let mut index = request.index / request.length;
        for extra in 0..proofs {
            out.push(node(request.base_layer + level + extra, index ^ 1)?);
            index >>= 1;
        }
        Ok(out)
    }

    /// Re-hashes whatever is on disk for `index`. A missing or short file simply means "not have".
    pub fn check_piece(&self, index: usize) -> bool {
        let start = index as u64 * self.piece_length;
        // Quick rejection without reading: every non-pad file region must exist at full size.
        for (file, inner, len) in self.spans(start, self.piece_len(index)) {
            let slot = &self.slots[file];
            if slot.pad {
                continue;
            }
            match std::fs::metadata(&slot.path) {
                Ok(m) if m.is_file() && m.len() >= inner + len => {}
                _ => return false,
            }
        }
        self.read_piece(index)
            .map(|d| self.piece_matches(index, &d))
            .unwrap_or(false)
    }

    /// Checks every piece against the data already on disk. `progress` receives `(checked, total)`.
    pub fn check_all(
        &self,
        mut progress: impl FnMut(usize, usize),
        cancel: &std::sync::atomic::AtomicBool,
    ) -> Bitfield {
        let count = self.count;
        let mut have = Bitfield::new(count);
        for index in 0..count {
            if cancel.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
            if self.check_piece(index) {
                have.set(index);
            }
            if index % 16 == 0 || index + 1 == count {
                progress(index + 1, count);
            }
        }
        have
    }

    /// Removes every file of the torrent (and emptied directories up to the download root).
    pub fn delete_files(&self) {
        // Make deletion terminal before waiting for any in-flight writer.
        self.deleted.store(true, Ordering::Release);
        let Ok(_mutation) = self.mutation.lock() else {
            return;
        };
        if let Ok(mut open) = self.open.lock() {
            open.clear();
        }
        let mut dirs = Vec::new();
        for (index, slot) in self.slots.iter().enumerate() {
            if slot.pad || self.validate_path(index, false).is_err() {
                continue;
            }
            let _ = std::fs::remove_file(&slot.path);
            // Only validated descendants can contribute directories to cleanup.
            if let Some(parent) = slot.path.parent() {
                dirs.extend(
                    parent
                        .ancestors()
                        .take_while(|path| *path != self.download_root)
                        .map(Path::to_path_buf),
                );
            }
        }
        // Remove emptied folders bottom-up, but never the user's download directory itself.
        dirs.sort_by_key(|d| std::cmp::Reverse(d.components().count()));
        dirs.dedup();
        for dir in dirs {
            if dir.starts_with(&self.root) {
                let _ = std::fs::remove_dir(&dir);
            }
        }
    }

    /// Bytes of `file` covered by pieces in `have`.
    pub fn completed_bytes(&self, file: usize, have: &Bitfield) -> u64 {
        let slot = &self.slots[file];
        if slot.length == 0 {
            return 0;
        }
        let first = (slot.offset / self.piece_length) as usize;
        let last = ((slot.offset + slot.length - 1) / self.piece_length) as usize;
        let mut done = 0;
        for piece in first..=last {
            if have.get(piece) {
                let start = (piece as u64 * self.piece_length).max(slot.offset);
                let end = ((piece as u64 + 1) * self.piece_length).min(slot.offset + slot.length);
                done += end - start;
            }
        }
        done
    }

    pub fn piece_range_of_file(&self, file: usize) -> std::ops::Range<usize> {
        let slot = &self.slots[file];
        if slot.length == 0 {
            return 0..0;
        }
        let first = (slot.offset / self.piece_length) as usize;
        let last = ((slot.offset + slot.length - 1) / self.piece_length) as usize;
        first..last + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bittorrent::metainfo::{Extras, build_info};
    use std::sync::atomic::AtomicBool;

    fn data(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
            .collect()
    }

    fn fixture() -> (Torrent, Vec<Vec<u8>>) {
        // Three files of awkward sizes so pieces straddle file boundaries (piece = 16 KiB).
        let parts = vec![data(20_000, 1), data(5_000, 2), data(30_001, 3)];
        let info = build_info(
            "pack",
            16384,
            &[
                (&["a.bin"], &parts[0]),
                (&["sub", "b.bin"], &parts[1]),
                (&["c.bin"], &parts[2]),
            ],
        );
        (Torrent::from_info(&info, Extras::default()).unwrap(), parts)
    }

    fn whole(parts: &[Vec<u8>]) -> Vec<u8> {
        parts.concat()
    }

    #[test]
    fn writing_every_piece_reproduces_the_files_exactly() {
        let (t, parts) = fixture();
        let dir = tempfile::tempdir().unwrap();
        let s = Storage::new(&t, dir.path());
        let all = whole(&parts);
        // Write pieces out of order, as a swarm would.
        for index in (0..t.piece_count()).rev() {
            let start = index * 16384;
            let end = (start + 16384).min(all.len());
            s.write(start as u64, &all[start..end]).unwrap();
        }
        assert_eq!(
            std::fs::read(dir.path().join("pack/a.bin")).unwrap(),
            parts[0]
        );
        assert_eq!(
            std::fs::read(dir.path().join("pack/sub/b.bin")).unwrap(),
            parts[1]
        );
        assert_eq!(
            std::fs::read(dir.path().join("pack/c.bin")).unwrap(),
            parts[2]
        );
        for index in 0..t.piece_count() {
            assert!(s.check_piece(index), "piece {index}");
        }
    }

    #[test]
    fn reads_span_files_and_are_bounds_checked() {
        let (t, parts) = fixture();
        let dir = tempfile::tempdir().unwrap();
        let s = Storage::new(&t, dir.path());
        let all = whole(&parts);
        s.write(0, &all).unwrap();
        assert_eq!(s.read(19_990, 5_020).unwrap(), all[19_990..25_010]);
        assert_eq!(
            s.read_piece(t.piece_count() - 1).unwrap(),
            all[(t.piece_count() - 1) * 16384..]
        );
        assert!(s.read(all.len() as u64 - 1, 2).is_err());
        assert!(s.write(all.len() as u64, &[1]).is_err());
        assert!(s.read(u64::MAX, 2).is_err());
        assert!(s.write(u64::MAX, &[1, 2]).is_err());
    }

    #[test]
    fn check_all_finds_exactly_the_intact_pieces() {
        let (t, parts) = fixture();
        let dir = tempfile::tempdir().unwrap();
        let s = Storage::new(&t, dir.path());
        let all = whole(&parts);
        s.write(0, &all).unwrap();
        // Corrupt one byte inside piece 1 (16384..32768).
        let c = dir.path().join("pack/a.bin");
        let mut bytes = std::fs::read(&c).unwrap();
        bytes[17_000] ^= 0xff;
        std::fs::write(&c, bytes).unwrap();
        // Remove the middle file: pieces that need it fail, others survive.
        std::fs::remove_file(dir.path().join("pack/sub/b.bin")).unwrap();
        let fresh = Storage::new(&t, dir.path());
        let mut last = (0, 0);
        let have = fresh.check_all(|d, n| last = (d, n), &AtomicBool::new(false));
        assert_eq!(last.0, last.1, "progress reaches the end");
        assert!(have.get(0), "piece 0 only covers a.bin and is intact");
        assert!(!have.get(1), "corrupted");
        assert!(
            have.get(2),
            "b.bin ends at 25000, so piece 2 only covers c.bin"
        );
        assert!(have.get(t.piece_count() - 1), "tail only covers c.bin");
        assert!(!have.is_complete());
    }

    #[test]
    fn cancelling_a_check_stops_early() {
        let (t, parts) = fixture();
        let dir = tempfile::tempdir().unwrap();
        let s = Storage::new(&t, dir.path());
        s.write(0, &whole(&parts)).unwrap();
        let have = s.check_all(|_, _| {}, &AtomicBool::new(true));
        assert_eq!(have.count(), 0);
    }

    #[test]
    fn padding_files_are_never_written_and_read_as_zeros() {
        use crate::bittorrent::bencode::{self, Value, dict};
        let a = data(10_000, 5);
        let pad = vec![0u8; 16384 - 10_000];
        let b = data(16384, 6);
        let all = [a.clone(), pad.clone(), b.clone()].concat();
        let pieces: Vec<u8> = all
            .chunks(16384)
            .flat_map(|c| Sha1::digest(c).to_vec())
            .collect();
        let file = |len: usize, path: &str, attr: Option<&str>| {
            let mut pairs = vec![
                ("length", Value::Int(len as i64)),
                ("path", Value::List(vec![Value::text(path)])),
            ];
            if let Some(attr) = attr {
                pairs.push(("attr", Value::text(attr)));
            }
            let mut d = std::collections::BTreeMap::new();
            for (k, v) in pairs {
                d.insert(k.as_bytes().to_vec(), v);
            }
            Value::Dict(d)
        };
        let info = bencode::encode(&dict([
            ("name", Value::text("padded")),
            ("piece length", Value::Int(16384)),
            ("pieces", Value::Bytes(pieces)),
            (
                "files",
                Value::List(vec![
                    file(a.len(), "a", None),
                    file(pad.len(), ".pad", Some("p")),
                    file(b.len(), "b", None),
                ]),
            ),
        ]));
        let t = Torrent::from_info(&info, Extras::default()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let s = Storage::new(&t, dir.path());
        s.write(0, &all).unwrap();
        assert!(!dir.path().join("padded/.pad").exists());
        assert!(s.check_piece(0) && s.check_piece(1));
        assert_eq!(s.read(10_000, 6384).unwrap(), pad);
    }

    #[test]
    fn completed_bytes_and_piece_ranges_follow_file_boundaries() {
        let (t, _) = fixture();
        let dir = tempfile::tempdir().unwrap();
        let s = Storage::new(&t, dir.path());
        assert_eq!(s.piece_range_of_file(0), 0..2);
        assert_eq!(s.piece_range_of_file(1), 1..2);
        let mut have = Bitfield::new(t.piece_count());
        have.set(0);
        assert_eq!(s.completed_bytes(0, &have), 16384);
        have.set(1);
        assert_eq!(s.completed_bytes(0, &have), 20_000);
        assert_eq!(s.completed_bytes(1, &have), 5_000);
    }

    #[test]
    fn delete_removes_files_and_their_folders_but_not_the_download_root() {
        let (t, parts) = fixture();
        let dir = tempfile::tempdir().unwrap();
        let s = Storage::new(&t, dir.path());
        s.write(0, &whole(&parts)).unwrap();
        s.delete_files();
        assert!(!dir.path().join("pack").exists());
        assert!(dir.path().exists());
    }

    #[test]
    fn deletion_is_terminal_and_late_piece_writes_cannot_recreate_files() {
        let (torrent, parts) = fixture();
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(&torrent, dir.path());
        let bytes = whole(&parts);
        storage.write(0, &bytes).unwrap();
        assert!(dir.path().join("pack/a.bin").exists());

        storage.delete_files();
        assert!(!dir.path().join("pack").exists());
        assert!(storage.write(0, &bytes).is_err());
        assert!(
            !dir.path().join("pack").exists(),
            "a late write after delete must not recreate torrent files"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_folder_cannot_redirect_writes_outside_the_download_directory() {
        let (t, parts) = fixture();
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("pack")).unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("pack").join("sub")).unwrap();
        let s = Storage::new(&t, dir.path());
        let all = whole(&parts);
        let result = s.write(20_000, &all[20_000..25_000]);
        assert!(result.is_err(), "write through a symlink must be refused");
        assert!(std::fs::read_dir(outside.path()).unwrap().next().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn torrent_root_and_final_file_symlinks_cannot_escape_for_reads_or_writes() {
        for root_link in [true, false] {
            let (torrent, _) = fixture();
            let dir = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            let foreign = outside.path().join("a.bin");
            std::fs::write(&foreign, b"private data").unwrap();
            if root_link {
                std::os::unix::fs::symlink(outside.path(), dir.path().join("pack")).unwrap();
            } else {
                std::fs::create_dir(dir.path().join("pack")).unwrap();
                std::os::unix::fs::symlink(&foreign, dir.path().join("pack/a.bin")).unwrap();
            }
            let storage = Storage::new(&torrent, dir.path());
            assert!(storage.read(0, 3).is_err());
            assert!(storage.write(0, b"bad").is_err());
            storage.delete_files();
            assert_eq!(std::fs::read(&foreign).unwrap(), b"private data");
        }
    }

    #[cfg(unix)]
    #[test]
    fn refusing_a_symlink_does_not_create_directories_outside_the_download_root() {
        let (mut torrent, _) = fixture();
        torrent.files[1].path.insert(1, "new-directory".into());
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("pack")).unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("pack/sub")).unwrap();
        let storage = Storage::new(&torrent, dir.path());
        assert!(storage.write(20_000, &[0; 5000]).is_err());
        assert!(!outside.path().join("new-directory").exists());
    }
}
