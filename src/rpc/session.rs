//! Durable list of unfinished RPC tasks.
//!
//! The RPC task table lives in memory, so a restart used to forget every GID. With
//! `serve --session-file` the table is written to disk whenever it changes and is
//! restored (paused) on the next start, keeping GIDs stable for the manager.
//!
//! Only data that is safe at rest is stored. A task whose request carries
//! credentials (URL user info, cookies, `Authorization`, proxy passwords, ...) is
//! *not* written: after a restart the manager sees an unknown GID and recreates it
//! with fresh credentials, exactly as it would without persistence. Downloaded bytes
//! are never trusted because of this file: HTTP resume re-checks the saved validator
//! and BitTorrent re-hashes the payload when a restored task is resumed.

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
};
use tokio::sync::Notify;

const FORMAT_VERSION: u32 = 1;
const MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_ENTRIES: usize = 5_000;

/// RPC options that are plain configuration. Everything else is dropped on write.
const KEPT_OPTIONS: &[&str] = &[
    "dir",
    "out",
    "select-file",
    "max-download-limit",
    "max-upload-limit",
    "seed-time",
    "seed-ratio",
    "bt-max-peers",
    "bt-force-encryption",
    "bt-require-crypto",
    "x-nibiru-sequential",
    "split",
    "max-connection-per-server",
    "min-split-size",
    "ca-certificate",
    "ssh-host-key",
    "ssh-identity",
    "x-nibiru-ed2k-source",
    "x-nibiru-ed2k-obfuscation",
    "referer",
    "user-agent",
];
const SAFE_HEADERS: &[&str] = &["accept", "accept-language", "accept-encoding", "origin"];

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Entry {
    Direct {
        gid: String,
        url: String,
        options: Map<String, Value>,
    },
    /// Base64 of the `.torrent` bytes (also used for magnets whose metadata arrived).
    Torrent {
        gid: String,
        torrent: String,
        options: Map<String, Value>,
    },
    Magnet {
        gid: String,
        uri: String,
        options: Map<String, Value>,
    },
    /// Standard `ed2k://|file|…|/` link; downloaded bytes are re-hashed on resume.
    Ed2k {
        gid: String,
        link: String,
        options: Map<String, Value>,
    },
}

impl Entry {
    pub fn gid(&self) -> &str {
        match self {
            Entry::Direct { gid, .. }
            | Entry::Torrent { gid, .. }
            | Entry::Magnet { gid, .. }
            | Entry::Ed2k { gid, .. } => gid,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct SessionFile {
    version: u32,
    tasks: Vec<Entry>,
}

fn has_user_info(raw: &str) -> bool {
    url::Url::parse(raw)
        .map(|url| !url.username().is_empty() || url.password().is_some())
        .unwrap_or(true)
}

/// The options that may be written to disk for this task, or `None` when the task
/// carries anything secret and must stay out of the session file.
pub fn persistable_options(
    url: Option<&str>,
    options: Option<&Map<String, Value>>,
) -> Option<Map<String, Value>> {
    if url.is_some_and(|url| !url.starts_with("magnet:") && has_user_info(url)) {
        return None;
    }
    let mut kept = Map::new();
    for (key, value) in options.into_iter().flatten() {
        match key.as_str() {
            "header" => {
                let lines: Vec<&str> = match value {
                    Value::String(line) => vec![line.as_str()],
                    Value::Array(lines) => lines.iter().filter_map(Value::as_str).collect(),
                    _ => return None,
                };
                if lines.iter().any(|line| {
                    line.split_once(':').is_none_or(|(name, _)| {
                        !SAFE_HEADERS.contains(&name.trim().to_ascii_lowercase().as_str())
                    })
                }) {
                    return None;
                }
                kept.insert(key.clone(), value.clone());
            }
            "all-proxy" | "http-proxy" | "https-proxy" => {
                let proxy = value.as_str()?;
                if !proxy.is_empty() && has_user_info(proxy) {
                    return None;
                }
                kept.insert(key.clone(), value.clone());
            }
            "all-proxy-user" | "all-proxy-passwd" => {
                if value.as_str().is_some_and(|text| !text.is_empty()) {
                    return None;
                }
            }
            other if KEPT_OPTIONS.contains(&other) => {
                if let Some(text) = value.as_str() {
                    kept.insert(key.clone(), Value::String(text.to_owned()));
                }
            }
            _ => {}
        }
    }
    Some(kept)
}

/// Where task state is kept; writes are atomic and skipped when nothing changed.
pub struct SessionStore {
    path: PathBuf,
    last: Mutex<Option<Vec<u8>>>,
    wake: Notify,
}

impl SessionStore {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            last: Mutex::new(None),
            wake: Notify::new(),
        }
    }

    /// Asks the background writer to flush soon (call after a task table change).
    pub fn request_flush(&self) {
        self.wake.notify_one();
    }

    pub async fn flush_requested(&self) {
        self.wake.notified().await;
    }

    /// Reads the saved tasks. A missing file is an empty session; an unreadable or
    /// foreign file is moved aside (never overwritten) and also yields an empty one.
    /// Returns the valid entries and how many were skipped.
    pub fn load(&self) -> Result<(Vec<Entry>, usize)> {
        let metadata = match std::fs::metadata(&self.path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok((Vec::new(), 0));
            }
            Err(error) => return Err(error.into()),
        };
        ensure!(metadata.is_file(), "the session path is not a regular file");
        let parsed = (metadata.len() <= MAX_FILE_BYTES)
            .then(|| std::fs::read(&self.path).ok())
            .flatten()
            .and_then(|bytes| {
                serde_json::from_slice::<Value>(&bytes)
                    .ok()
                    .map(|value| (bytes, value))
            })
            .filter(|(_, value)| value["version"] == FORMAT_VERSION && value["tasks"].is_array());
        let Some((bytes, value)) = parsed else {
            self.set_aside()?;
            return Ok((Vec::new(), 0));
        };
        let rows = value["tasks"].as_array().cloned().unwrap_or_default();
        let total = rows.len();
        let mut entries = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for row in rows.into_iter().take(MAX_ENTRIES) {
            if let Ok(entry) = serde_json::from_value::<Entry>(row)
                && seen.insert(entry.gid().to_owned())
            {
                entries.push(entry);
            }
        }
        let skipped = total - entries.len();
        *self.last.lock().unwrap_or_else(|e| e.into_inner()) = Some(bytes);
        Ok((entries, skipped))
    }

    fn set_aside(&self) -> Result<()> {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let mut name = self
            .path
            .file_name()
            .context("session file has no name")?
            .to_os_string();
        name.push(format!(
            ".corrupt-{stamp}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::rename(&self.path, self.path.with_file_name(name))?;
        Ok(())
    }

    /// Writes the session if it differs from the last write. Returns whether it wrote.
    pub fn save(&self, tasks: &[Entry]) -> Result<bool> {
        ensure!(
            tasks.len() <= MAX_ENTRIES,
            "too many tasks for a session file"
        );
        let mut tasks = tasks.to_vec();
        tasks.sort_by(|a, b| a.gid().cmp(b.gid()));
        let bytes = serde_json::to_vec_pretty(&SessionFile {
            version: FORMAT_VERSION,
            tasks,
        })?;
        ensure!(
            bytes.len() as u64 <= MAX_FILE_BYTES,
            "session exceeds supported size limit"
        );
        {
            let last = self.last.lock().unwrap_or_else(|e| e.into_inner());
            if last.as_deref() == Some(bytes.as_slice()) {
                return Ok(false);
            }
        }
        let parent = self
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::create_dir_all(parent)?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            temporary
                .as_file()
                .set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        temporary.write_all(&bytes)?;
        temporary.as_file().sync_all()?;
        temporary
            .persist(&self.path)
            .map_err(|error| anyhow::anyhow!(error.error))?;
        *self.last.lock().unwrap_or_else(|e| e.into_inner()) = Some(bytes);
        Ok(true)
    }
}

/// Rejects restored values that could not have been produced by this engine.
pub fn validate_gid(gid: &str) -> Result<()> {
    if gid.is_empty() || gid.len() > 32 || !gid.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("invalid saved gid");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn options(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    #[test]
    fn plain_options_are_kept_and_unknown_ones_dropped() {
        let kept = persistable_options(
            Some("https://example.test/file.bin"),
            Some(&options(json!({
                "dir": "/data", "out": "file.bin", "max-download-limit": "1M",
                "referer": "https://example.test/page", "continue": "true",
                "header": ["Accept: */*"]
            }))),
        )
        .unwrap();
        assert_eq!(kept["dir"], "/data");
        assert!(!kept.contains_key("continue"));
        assert_eq!(kept["header"], json!(["Accept: */*"]));
    }

    #[test]
    fn tasks_with_credentials_are_never_persisted() {
        let url = Some("https://example.test/file.bin");
        for secret in [
            json!({"header": ["Cookie: session=1"]}),
            json!({"header": "Authorization: Bearer t"}),
            json!({"header": ["Accept: */*", "X-Api-Key: k"]}),
            json!({"all-proxy": "http://user:pw@proxy.test:8080"}),
            json!({"all-proxy": "http://proxy.test:8080", "all-proxy-passwd": "pw"}),
            json!({"all-proxy-user": "user"}),
        ] {
            assert!(
                persistable_options(url, Some(&options(secret.clone()))).is_none(),
                "{secret}"
            );
        }
        for url in [
            "ftp://user:pw@example.test/file",
            "sftp://user@example.test/file",
            "https://:token@example.test/x",
            "not a url",
        ] {
            assert!(persistable_options(Some(url), None).is_none(), "{url}");
        }
        assert!(persistable_options(Some("magnet:?xt=urn:btih:abc"), None).is_some());
        assert!(
            persistable_options(
                Some("https://proxy-free.test/f"),
                Some(&options(json!({"all-proxy": "http://proxy.test:8080"})))
            )
            .is_some()
        );
    }

    fn direct(gid: &str) -> Entry {
        Entry::Direct {
            gid: gid.into(),
            url: "https://example.test/a".into(),
            options: options(json!({"dir": "/d", "out": "a"})),
        }
    }

    #[test]
    fn save_is_atomic_private_idempotent_and_loads_back() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().join("session.json"));
        assert_eq!(store.load().unwrap().0.len(), 0);
        assert!(store.save(&[direct("b"), direct("a")]).unwrap());
        assert!(
            !store.save(&[direct("a"), direct("b")]).unwrap(),
            "same content is not rewritten"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.path().join("session.json"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        let again = SessionStore::new(dir.path().join("session.json"));
        let (entries, skipped) = again.load().unwrap();
        assert_eq!(skipped, 0);
        assert_eq!(
            entries.iter().map(|e| e.gid()).collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            1,
            "no temporary file is left"
        );
    }

    #[test]
    fn corrupt_or_foreign_files_are_moved_aside_not_overwritten() {
        for content in [
            "not json",
            r#"{"version":99,"tasks":[]}"#,
            r#"{"version":1,"tasks":{}}"#,
            r#"{"version":1}"#,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("session.json");
            std::fs::write(&path, content).unwrap();
            let store = SessionStore::new(path.clone());
            assert!(store.load().unwrap().0.is_empty());
            assert!(!path.exists());
            let kept = std::fs::read_dir(dir.path())
                .unwrap()
                .flatten()
                .find(|e| e.file_name().to_string_lossy().contains(".corrupt-"))
                .unwrap();
            assert_eq!(std::fs::read_to_string(kept.path()).unwrap(), content);
        }
    }

    #[test]
    fn bad_and_duplicate_entries_are_skipped_and_counted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.json");
        let good = serde_json::to_value(direct("a")).unwrap();
        std::fs::write(
            &path,
            json!({"version": 1, "tasks": [good.clone(), good, {"kind":"mystery"}, 7]}).to_string(),
        )
        .unwrap();
        let (entries, skipped) = SessionStore::new(path).load().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(skipped, 3);
    }

    #[test]
    fn saved_gids_must_look_like_engine_gids() {
        assert!(validate_gid("0123456789abcdef").is_ok());
        for bad in ["", "../x", "zz", &"a".repeat(33)] {
            assert!(validate_gid(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn an_oversized_snapshot_never_replaces_the_last_readable_session() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.json");
        let store = SessionStore::new(path.clone());
        store.save(&[direct("a")]).unwrap();
        let before = std::fs::read(&path).unwrap();
        let oversized = Entry::Direct {
            gid: "b".into(),
            url: "x".repeat(MAX_FILE_BYTES as usize),
            options: Map::new(),
        };
        assert!(store.save(&[oversized]).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(SessionStore::new(path).load().unwrap().0[0].gid(), "a");
    }

    #[test]
    fn entries_beyond_the_restore_limit_are_counted_as_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.json");
        let tasks: Vec<_> = (0..MAX_ENTRIES + 3)
            .map(|i| direct(&format!("{i:x}")))
            .collect();
        std::fs::write(
            &path,
            serde_json::to_vec(&SessionFile {
                version: FORMAT_VERSION,
                tasks,
            })
            .unwrap(),
        )
        .unwrap();
        let (entries, skipped) = SessionStore::new(path).load().unwrap();
        assert_eq!(entries.len(), MAX_ENTRIES);
        assert_eq!(skipped, 3);
    }
}
