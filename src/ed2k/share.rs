use super::{
    FileLink,
    file::{PART_SIZE, digest},
};
use anyhow::{Context, Result, bail};
use md4::{Digest, Md4};
use serde::Serialize;
use std::{
    collections::HashMap,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

#[derive(Clone, Debug)]
pub struct SharedFile {
    pub path: PathBuf,
    pub link: FileLink,
    pub part_hashes: Vec<[u8; 16]>,
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
    })
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

    pub async fn add_directory(&self, directory: PathBuf, recursive: bool) -> Result<Vec<SharedFileInfo>> {
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
        }).await??;
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
        files.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()).then(a.hash.cmp(&b.hash)));
        Ok(files)
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
        assert_eq!(registry.get(&hash).unwrap().read_range(0, 3).unwrap(), b"abc");
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
        assert_eq!(registry.add_directory(root.path().to_path_buf(), false).await.unwrap().len(), 1);
        registry.clear().unwrap();
        assert_eq!(registry.add_directory(root.path().to_path_buf(), true).await.unwrap().len(), 2);
    }
}
