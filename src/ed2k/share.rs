use super::{
    FileLink,
    file::{PART_SIZE, digest},
};
use anyhow::{Context, Result, bail};
use md4::{Digest, Md4};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU8, Ordering},
    },
};

/// eMule-style upload priority: 0 very low, 1 low, 2 normal, 3 high, 4 release.
pub const PRIORITY_NORMAL: u8 = 2;
pub const PRIORITY_MAX: u8 = 4;

#[derive(Clone, Debug)]
pub struct SharedFile {
    pub path: PathBuf,
    pub link: FileLink,
    pub part_hashes: Vec<[u8; 16]>,
    /// Modification time (nanoseconds since the Unix epoch) when the file was hashed.
    pub mtime_ns: u128,
    priority: Arc<AtomicU8>,
}

fn mtime_ns(metadata: &std::fs::Metadata) -> u128 {
    metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Hash results the caller persisted, so unchanged files are not hashed again.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CachedShare {
    pub path: String,
    pub size: u64,
    pub mtime_ns: String,
    pub name: String,
    pub hash: String,
    pub part_hashes: Vec<String>,
}

/// What a rescan found.
#[derive(Clone, Copy, Debug, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RescanReport {
    pub unchanged: usize,
    pub rehashed: usize,
    pub removed: usize,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SharedFileInfo {
    pub path: String,
    pub name: String,
    pub size: u64,
    pub hash: String,
    pub link: String,
    pub parts: usize,
    pub priority: u8,
}

impl From<&SharedFile> for SharedFileInfo {
    fn from(file: &SharedFile) -> Self {
        Self {
            path: file.path.to_string_lossy().into_owned(),
            name: file.link.name.clone(),
            size: file.link.size,
            hash: file.link.hash_hex(),
            link: file.link.link(),
            parts: file.link.part_count(),
            priority: file.priority(),
        }
    }
}

fn validate_regular_file(path: &Path) -> Result<std::fs::Metadata> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("cannot inspect shared file {}", path.display()))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        bail!("shared path must be a regular file");
    }
    if metadata.len() == 0 {
        bail!("empty files are not shared on ED2K");
    }
    Ok(metadata)
}

pub fn hash_shared_file(path: &Path) -> Result<SharedFile> {
    let canonical = std::fs::canonicalize(path)
        .with_context(|| format!("cannot canonicalize shared file {}", path.display()))?;
    let metadata = validate_regular_file(&canonical)?;
    let size = metadata.len();
    if size / PART_SIZE >= u16::MAX as u64 {
        bail!("shared file exceeds supported ED2K hashset bounds");
    }
    let name = canonical
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .context("shared file name is not valid UTF-8")?
        .to_owned();

    let mut input = std::fs::File::open(&canonical)?;
    let mut hashes = Vec::new();
    let mut remaining = size;
    let mut buffer = vec![0_u8; 1024 * 1024];
    while remaining > 0 {
        let part_len = remaining.min(PART_SIZE);
        let mut part_remaining = part_len;
        let mut hasher = Md4::new();
        while part_remaining > 0 {
            let want = buffer.len().min(part_remaining as usize);
            input.read_exact(&mut buffer[..want])?;
            hasher.update(&buffer[..want]);
            part_remaining -= want as u64;
        }
        hashes.push(<[u8; 16]>::from(hasher.finalize()));
        remaining -= part_len;
    }
    if size.is_multiple_of(PART_SIZE) {
        hashes.push(digest(&[]));
    }
    let hash = if size < PART_SIZE {
        hashes[0]
    } else {
        digest(&hashes.concat())
    };
    let link = FileLink { name, size, hash };
    if FileLink::parse(&link.link())? != link {
        bail!("shared file descriptor failed ED2K validation");
    }
    Ok(SharedFile {
        path: canonical,
        link,
        part_hashes: hashes,
        mtime_ns: mtime_ns(&metadata),
        priority: Arc::new(AtomicU8::new(PRIORITY_NORMAL)),
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

impl SharedFile {
    pub fn priority(&self) -> u8 {
        self.priority.load(Ordering::Relaxed)
    }

    fn cache_entry(&self) -> CachedShare {
        CachedShare {
            path: self.path.to_string_lossy().into_owned(),
            size: self.link.size,
            mtime_ns: self.mtime_ns.to_string(),
            name: self.link.name.clone(),
            hash: self.link.hash_hex(),
            part_hashes: self.part_hashes.iter().map(|h| hex(h)).collect(),
        }
    }

    /// Rebuilds a file from persisted hashes. Returns `None` unless the file on disk still has the
    /// recorded size and modification time and the stored hashes are internally consistent.
    fn from_cache(path: &Path, cached: &CachedShare) -> Option<Self> {
        let canonical = std::fs::canonicalize(path).ok()?;
        let metadata = validate_regular_file(&canonical).ok()?;
        if canonical.to_string_lossy() != cached.path
            || metadata.len() != cached.size
            || mtime_ns(&metadata).to_string() != cached.mtime_ns
        {
            return None;
        }
        let part_hashes: Vec<[u8; 16]> = cached
            .part_hashes
            .iter()
            .map(|h| super::server::file_hash(h).ok())
            .collect::<Option<_>>()?;
        let hash = super::server::file_hash(&cached.hash).ok()?;
        let expected_parts = (cached.size / PART_SIZE + 1) as usize;
        let root = if cached.size < PART_SIZE {
            *part_hashes.first()?
        } else {
            digest(&part_hashes.concat())
        };
        if part_hashes.len() != expected_parts || root != hash {
            return None;
        }
        let link = FileLink {
            name: cached.name.clone(),
            size: cached.size,
            hash,
        };
        if FileLink::parse(&link.link()).ok()? != link {
            return None;
        }
        Some(Self {
            path: canonical,
            link,
            part_hashes,
            mtime_ns: mtime_ns(&metadata),
            priority: Arc::new(AtomicU8::new(PRIORITY_NORMAL)),
        })
    }
}

impl SharedFile {
    pub fn read_range(&self, from: u64, to: u64) -> Result<Vec<u8>> {
        if to <= from || to > self.link.size || to - from > 3 * 184_320 {
            bail!("invalid shared-file range");
        }
        let metadata = validate_regular_file(&self.path)?;
        if metadata.len() != self.link.size {
            bail!("shared file changed on disk; rescan required");
        }
        let mut file = std::fs::File::open(&self.path)?;
        file.seek(SeekFrom::Start(from))?;
        let mut bytes = vec![0; (to - from) as usize];
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}

#[derive(Clone, Default)]
pub struct ShareRegistry {
    files: Arc<RwLock<HashMap<[u8; 16], Arc<SharedFile>>>>,
}

impl ShareRegistry {
    pub async fn add_file(&self, path: PathBuf) -> Result<SharedFileInfo> {
        let file = tokio::task::spawn_blocking(move || hash_shared_file(&path)).await??;
        let info = SharedFileInfo::from(&file);
        self.files
            .write()
            .map_err(|error| anyhow::anyhow!("{error}"))?
            .insert(file.link.hash, Arc::new(file));
        Ok(info)
    }

    /// Like [`Self::add_file`], but reuses `cache` when the file is unchanged since it was hashed.
    pub async fn add_file_cached(
        &self,
        path: PathBuf,
        cache: &[CachedShare],
    ) -> Result<SharedFileInfo> {
        let cache: Vec<CachedShare> = cache.to_vec();
        let file = tokio::task::spawn_blocking(move || {
            let canonical = std::fs::canonicalize(&path)?;
            let key = canonical.to_string_lossy().into_owned();
            if let Some(file) = cache
                .iter()
                .find(|c| c.path == key)
                .and_then(|c| SharedFile::from_cache(&canonical, c))
            {
                return Ok(file);
            }
            hash_shared_file(&path)
        })
        .await??;
        let info = SharedFileInfo::from(&file);
        self.files
            .write()
            .map_err(|error| anyhow::anyhow!("{error}"))?
            .insert(file.link.hash, Arc::new(file));
        Ok(info)
    }

    /// Hash results worth persisting, to be passed to [`Self::add_file_cached`] next time.
    pub fn export_cache(&self) -> Vec<CachedShare> {
        let mut entries: Vec<CachedShare> = self
            .files
            .read()
            .map(|files| files.values().map(|f| f.cache_entry()).collect())
            .unwrap_or_default();
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        entries
    }

    /// Sets the upload priority (0..=4) of a shared file.
    pub fn set_priority(&self, hash: [u8; 16], priority: u8) -> Result<bool> {
        if priority > PRIORITY_MAX {
            bail!("priority must be between 0 and {PRIORITY_MAX}");
        }
        let files = self
            .files
            .read()
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        Ok(files
            .get(&hash)
            .map(|f| f.priority.store(priority, Ordering::Relaxed))
            .is_some())
    }

    /// Re-checks every registered file: vanished files are removed, modified ones are hashed again.
    pub async fn rescan(&self) -> Result<RescanReport> {
        let snapshot: Vec<Arc<SharedFile>> = self
            .files
            .read()
            .map_err(|error| anyhow::anyhow!("{error}"))?
            .values()
            .cloned()
            .collect();
        let mut report = RescanReport::default();
        for file in snapshot {
            let current = std::fs::symlink_metadata(&file.path)
                .ok()
                .filter(|m| m.is_file() && !m.file_type().is_symlink() && m.len() > 0);
            match current {
                None => {
                    self.remove(file.link.hash)?;
                    report.removed += 1;
                }
                Some(m) if m.len() == file.link.size && mtime_ns(&m) == file.mtime_ns => {
                    report.unchanged += 1;
                }
                Some(_) => {
                    self.remove(file.link.hash)?;
                    match self.add_file(file.path.clone()).await {
                        Ok(new) => {
                            // Keep the user's priority across a re-hash.
                            if let Ok(hash) = super::server::file_hash(&new.hash) {
                                let _ = self.set_priority(hash, file.priority());
                            }
                            report.rehashed += 1;
                        }
                        Err(_) => report.removed += 1,
                    }
                }
            }
        }
        Ok(report)
    }

    pub async fn add_directory(
        &self,
        directory: PathBuf,
        recursive: bool,
    ) -> Result<Vec<SharedFileInfo>> {
        let paths = tokio::task::spawn_blocking(move || -> Result<Vec<PathBuf>> {
            let root = std::fs::canonicalize(&directory)?;
            if !std::fs::symlink_metadata(&root)?.is_dir() {
                bail!("shared directory is not a directory");
            }
            let mut pending = vec![root];
            let mut files = Vec::new();
            while let Some(dir) = pending.pop() {
                for entry in std::fs::read_dir(dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    let metadata = std::fs::symlink_metadata(&path)?;
                    if metadata.file_type().is_symlink() {
                        continue;
                    }
                    if metadata.is_file() && metadata.len() > 0 {
                        files.push(path);
                        if files.len() > 10_000 {
                            bail!("shared directory contains too many files");
                        }
                    } else if recursive && metadata.is_dir() {
                        pending.push(path);
                    }
                }
            }
            Ok(files)
        })
        .await??;
        let mut added = Vec::new();
        for path in paths {
            if let Ok(info) = self.add_file(path).await {
                added.push(info);
            }
        }
        Ok(added)
    }

    pub fn remove(&self, hash: [u8; 16]) -> Result<bool> {
        Ok(self
            .files
            .write()
            .map_err(|error| anyhow::anyhow!("{error}"))?
            .remove(&hash)
            .is_some())
    }

    pub fn clear(&self) -> Result<()> {
        self.files
            .write()
            .map_err(|error| anyhow::anyhow!("{error}"))?
            .clear();
        Ok(())
    }

    pub fn list(&self) -> Result<Vec<SharedFileInfo>> {
        let mut files = self
            .files
            .read()
            .map_err(|error| anyhow::anyhow!("{error}"))?
            .values()
            .map(|file| SharedFileInfo::from(file.as_ref()))
            .collect::<Vec<_>>();
        files.sort_by(|a, b| {
            a.name
                .to_lowercase()
                .cmp(&b.name.to_lowercase())
                .then(a.hash.cmp(&b.hash))
        });
        Ok(files)
    }

    /// The registered files in the form the server publication expects.
    pub fn offered(&self) -> Vec<super::OfferedFile> {
        let mut files: Vec<_> = self
            .files
            .read()
            .map(|files| {
                files
                    .values()
                    .map(|file| super::OfferedFile {
                        hash: file.link.hash,
                        name: file.link.name.clone(),
                        size: file.link.size,
                    })
                    .collect()
            })
            .unwrap_or_default();
        files.sort_by(|a: &super::OfferedFile, b| a.name.cmp(&b.name).then(a.hash.cmp(&b.hash)));
        files
    }

    pub(crate) fn get(&self, hash: &[u8; 16]) -> Option<Arc<SharedFile>> {
        self.files.read().ok()?.get(hash).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn hashes_and_registers_shared_files_with_ed2k_part_rules() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("hello.txt");
        std::fs::write(&path, b"abc").unwrap();
        let registry = ShareRegistry::default();
        let info = registry.add_file(path.clone()).await.unwrap();
        assert_eq!(info.name, "hello.txt");
        assert_eq!(info.size, 3);
        assert_eq!(info.hash, "a448017aaf21d8525fc10ae87aa6729d");
        assert_eq!(registry.list().unwrap().len(), 1);
        let hash = super::super::server::file_hash(&info.hash).unwrap();
        assert_eq!(
            registry.get(&hash).unwrap().read_range(0, 3).unwrap(),
            b"abc"
        );
        std::fs::write(path, b"changed").unwrap();
        assert!(registry.get(&hash).unwrap().read_range(0, 3).is_err());
    }

    #[tokio::test]
    async fn directory_sharing_skips_symlinks_and_can_recurse() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("sub")).unwrap();
        std::fs::write(root.path().join("a.bin"), b"a").unwrap();
        std::fs::write(root.path().join("sub").join("b.bin"), b"b").unwrap();
        let registry = ShareRegistry::default();
        assert_eq!(
            registry
                .add_directory(root.path().to_path_buf(), false)
                .await
                .unwrap()
                .len(),
            1
        );
        registry.clear().unwrap();
        assert_eq!(
            registry
                .add_directory(root.path().to_path_buf(), true)
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn cached_hashes_are_reused_only_while_the_file_is_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("data.bin");
        std::fs::write(&path, vec![7u8; 100_000]).unwrap();
        let first = ShareRegistry::default();
        let info = first.add_file(path.clone()).await.unwrap();
        let cache = first.export_cache();
        assert_eq!(cache.len(), 1);

        // Unchanged: the cached hashes are accepted without reading the file again. Proving that
        // directly: a cache entry with a wrong-but-consistent size is rejected, a real one is not.
        let second = ShareRegistry::default();
        let again = second.add_file_cached(path.clone(), &cache).await.unwrap();
        assert_eq!(again.hash, info.hash);
        assert_eq!(again.link, info.link);
        assert!(SharedFile::from_cache(&path, &cache[0]).is_some());

        // A tampered cache (hash that does not match the part hashes) is ignored.
        let mut forged = cache[0].clone();
        forged.hash = "00".repeat(16);
        assert!(SharedFile::from_cache(&path, &forged).is_none());
        let third = ShareRegistry::default();
        let rehashed = third
            .add_file_cached(path.clone(), &[forged])
            .await
            .unwrap();
        assert_eq!(
            rehashed.hash, info.hash,
            "falls back to hashing the real content"
        );

        // Modified content (same size, later mtime) invalidates the cache entry.
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(&path, vec![9u8; 100_000]).unwrap();
        assert!(SharedFile::from_cache(&path, &cache[0]).is_none());
        let fourth = ShareRegistry::default();
        let changed = fourth.add_file_cached(path, &cache).await.unwrap();
        assert_ne!(changed.hash, info.hash);
    }

    #[tokio::test]
    async fn rescan_drops_vanished_files_and_rehashes_modified_ones() {
        let root = tempfile::tempdir().unwrap();
        let (keep, edit, gone) = (
            root.path().join("keep.bin"),
            root.path().join("edit.bin"),
            root.path().join("gone.bin"),
        );
        for (p, b) in [(&keep, 1u8), (&edit, 2), (&gone, 3)] {
            std::fs::write(p, vec![b; 5000]).unwrap();
        }
        let registry = ShareRegistry::default();
        for p in [&keep, &edit, &gone] {
            registry.add_file(p.clone()).await.unwrap();
        }
        let edit_hash = registry
            .list()
            .unwrap()
            .into_iter()
            .find(|f| f.name == "edit.bin")
            .unwrap()
            .hash;
        registry
            .set_priority(super::super::server::file_hash(&edit_hash).unwrap(), 4)
            .unwrap();
        assert!(registry.set_priority([0; 16], 9).is_err());
        assert!(!registry.set_priority([0; 16], 1).unwrap());

        std::fs::remove_file(&gone).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(&edit, vec![8u8; 6000]).unwrap();
        let report = registry.rescan().await.unwrap();
        assert_eq!(
            report,
            RescanReport {
                unchanged: 1,
                rehashed: 1,
                removed: 1
            }
        );
        let list = registry.list().unwrap();
        assert_eq!(list.len(), 2);
        let edited = list.iter().find(|f| f.name == "edit.bin").unwrap();
        assert_ne!(edited.hash, edit_hash);
        assert_eq!(edited.size, 6000);
        assert_eq!(edited.priority, 4, "priority survives a re-hash");
    }
}
