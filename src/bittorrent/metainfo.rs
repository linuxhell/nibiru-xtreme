//! `.torrent` metainfo (BEP 3, BEP 12, BEP 19, BEP 47): parsing with strict validation of every
//! length and path, since the file comes from an untrusted source.

use super::bencode::{self, Value, dict};
use anyhow::{Context, Result, bail};
use sha1::{Digest, Sha1};

pub const MAX_TORRENT_BYTES: usize = 16 * 1024 * 1024;
const MAX_FILES: usize = 100_000;
const MAX_TOTAL: u64 = 1 << 50;
const MIN_PIECE: u64 = 16 * 1024;
const MAX_PIECE: u64 = 128 * 1024 * 1024;
const MAX_TRACKERS: usize = 64;
const MAX_WEB_SEEDS: usize = 16;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileEntry {
    /// Sanitised relative path components (a single one for single-file torrents).
    pub path: Vec<String>,
    pub length: u64,
    /// Offset of the file's first byte in the torrent's contiguous byte space.
    pub offset: u64,
    /// BEP 47 padding file: exists only to align the next file, never written to disk.
    pub pad: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Torrent {
    pub info_hash: [u8; 20],
    pub name: String,
    pub piece_length: u64,
    pub pieces: Vec<[u8; 20]>,
    pub files: Vec<FileEntry>,
    pub total_length: u64,
    pub private: bool,
    /// Announce URLs in BEP 12 tiers.
    pub trackers: Vec<Vec<String>>,
    pub web_seeds: Vec<String>,
    /// The raw bencoded `info` dictionary, served to peers (BEP 9) and saved with the torrent.
    pub info_bytes: Vec<u8>,
}

/// Trackers and web seeds that live outside the `info` dictionary.
#[derive(Clone, Debug, Default)]
pub struct Extras {
    pub trackers: Vec<Vec<String>>,
    pub web_seeds: Vec<String>,
}

pub fn info_hash(info_bytes: &[u8]) -> [u8; 20] {
    Sha1::digest(info_bytes).into()
}

/// Makes one path component safe on every platform; never returns an empty string, "." or "..".
pub fn sanitize_component(raw: &str) -> String {
    let mut out: String = raw
        .chars()
        .map(|c| {
            if c.is_control() || matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|') {
                '_'
            } else {
                c
            }
        })
        .collect();
    // Windows silently drops trailing dots and spaces, which would alias other names.
    while out.ends_with('.') || out.ends_with(' ') {
        out.pop();
    }
    let out = out.trim_start().to_owned();
    let stem = out.split('.').next().unwrap_or("").to_ascii_uppercase();
    let reserved = matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (stem.len() == 4
            && (stem.starts_with("COM") || stem.starts_with("LPT"))
            && stem.as_bytes()[3].is_ascii_digit());
    if out.is_empty() || reserved {
        format!("_{out}")
    } else {
        out.chars().take(200).collect()
    }
}

fn text(value: &Value) -> Option<String> {
    value.str().map(str::to_owned)
}

fn url_list(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Bytes(b)) => String::from_utf8(b.clone()).into_iter().collect(),
        Some(Value::List(items)) => items.iter().filter_map(text).collect(),
        _ => Vec::new(),
    }
}

fn good_tracker(url: &str) -> bool {
    url.len() <= 512
        && ["http://", "https://", "udp://"]
            .iter()
            .any(|p| url.to_ascii_lowercase().starts_with(p))
}

impl Torrent {
    /// Parses a complete `.torrent` file.
    pub fn from_torrent_file(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_TORRENT_BYTES {
            bail!("torrent file is larger than {MAX_TORRENT_BYTES} bytes");
        }
        let decoded = bencode::decode(bytes).context("invalid torrent file")?;
        let span = decoded
            .info_span
            .context("torrent file has no info dictionary")?;
        let mut extras = Extras::default();
        if let Some(tiers) = decoded.value.get("announce-list").and_then(Value::list) {
            for tier in tiers {
                let urls: Vec<String> = tier
                    .list()
                    .unwrap_or_default()
                    .iter()
                    .filter_map(text)
                    .collect();
                if !urls.is_empty() {
                    extras.trackers.push(urls);
                }
            }
        }
        if extras.trackers.is_empty()
            && let Some(url) = decoded.value.get("announce").and_then(text)
        {
            extras.trackers.push(vec![url]);
        }
        extras.web_seeds = url_list(decoded.value.get("url-list"));
        Self::from_info(&bytes[span], extras)
    }

    /// Builds a torrent from the raw `info` dictionary (for example obtained through BEP 9).
    pub fn from_info(info_bytes: &[u8], extras: Extras) -> Result<Self> {
        let info = bencode::decode(info_bytes)
            .context("invalid info dictionary")?
            .value;
        if info.get("meta version").and_then(Value::int).unwrap_or(1) != 1
            && info.get("pieces").is_none()
        {
            bail!("BitTorrent v2-only torrents are not supported yet");
        }
        let name = sanitize_component(
            info.get("name.utf-8")
                .or_else(|| info.get("name"))
                .and_then(Value::str)
                .context("torrent has no name")?,
        );
        let piece_length = u64::try_from(
            info.get("piece length")
                .and_then(Value::int)
                .context("torrent has no piece length")?,
        )?;
        if !(MIN_PIECE..=MAX_PIECE).contains(&piece_length) || piece_length % MIN_PIECE != 0 {
            bail!("unsupported piece length {piece_length}");
        }
        let blob = info
            .get("pieces")
            .and_then(Value::bytes)
            .context("torrent has no pieces")?;
        if blob.is_empty() || blob.len() % 20 != 0 {
            bail!("pieces is not a list of SHA-1 hashes");
        }
        let pieces: Vec<[u8; 20]> = blob
            .chunks_exact(20)
            .map(|c| c.try_into().expect("chunk of 20"))
            .collect();

        let mut files = Vec::new();
        let mut offset = 0u64;
        let mut used: std::collections::HashSet<Vec<String>> = Default::default();
        if let Some(list) = info.get("files").and_then(Value::list) {
            if list.is_empty() || list.len() > MAX_FILES {
                bail!("unsupported number of files");
            }
            for file in list {
                let length = u64::try_from(
                    file.get("length")
                        .and_then(Value::int)
                        .context("file has no length")?,
                )?;
                let parts = file
                    .get("path.utf-8")
                    .or_else(|| file.get("path"))
                    .and_then(Value::list)
                    .context("file has no path")?;
                if parts.is_empty() || parts.len() > 32 {
                    bail!("invalid file path");
                }
                let pad = file
                    .get("attr")
                    .and_then(Value::str)
                    .is_some_and(|a| a.contains('p'));
                let mut path: Vec<String> = parts
                    .iter()
                    .map(|p| p.str().map(sanitize_component).context("non-text path"))
                    .collect::<Result<_>>()?;
                // Two entries can sanitise to the same name; keep them distinct.
                let mut n = 1;
                while !pad && !used.insert(path.clone()) {
                    let last = path.last_mut().expect("non-empty path");
                    *last = format!(
                        "{}_{n}",
                        last.trim_end_matches(|c: char| c == '_' || c.is_ascii_digit())
                    );
                    n += 1;
                    if n > 1000 {
                        bail!("too many files share a name");
                    }
                }
                files.push(FileEntry {
                    path,
                    length,
                    offset,
                    pad,
                });
                offset = offset
                    .checked_add(length)
                    .filter(|t| *t <= MAX_TOTAL)
                    .context("torrent is too large")?;
            }
        } else {
            let length = u64::try_from(
                info.get("length")
                    .and_then(Value::int)
                    .context("torrent has neither length nor files")?,
            )?;
            files.push(FileEntry {
                path: vec![name.clone()],
                length,
                offset: 0,
                pad: false,
            });
            offset = length;
        }
        let total_length = offset;
        if total_length == 0 || total_length > MAX_TOTAL {
            bail!("torrent has no data or is too large");
        }
        if total_length.div_ceil(piece_length) != pieces.len() as u64 {
            bail!("piece count does not match the total size");
        }

        let mut trackers: Vec<Vec<String>> = extras
            .trackers
            .into_iter()
            .map(|tier| {
                tier.into_iter()
                    .filter(|u| good_tracker(u))
                    .collect::<Vec<_>>()
            })
            .filter(|tier| !tier.is_empty())
            .collect();
        let mut seen = 0;
        trackers.retain_mut(|tier| {
            tier.truncate(MAX_TRACKERS.saturating_sub(seen));
            seen += tier.len();
            !tier.is_empty()
        });
        let web_seeds = extras
            .web_seeds
            .into_iter()
            .filter(|u| {
                let l = u.to_ascii_lowercase();
                u.len() <= 512 && (l.starts_with("http://") || l.starts_with("https://"))
            })
            .take(MAX_WEB_SEEDS)
            .collect();
        Ok(Self {
            info_hash: info_hash(info_bytes),
            name,
            piece_length,
            pieces,
            files,
            total_length,
            private: info.get("private").and_then(Value::int) == Some(1),
            trackers,
            web_seeds,
            info_bytes: info_bytes.to_vec(),
        })
    }

    pub fn piece_count(&self) -> usize {
        self.pieces.len()
    }

    /// Length of piece `index` (the last one may be shorter).
    pub fn piece_len(&self, index: usize) -> u64 {
        let start = index as u64 * self.piece_length;
        self.total_length
            .saturating_sub(start)
            .min(self.piece_length)
    }

    pub fn is_multi_file(&self) -> bool {
        self.files.len() > 1 || self.files[0].path.len() > 1 || self.files[0].path[0] != self.name
    }

    /// Writes a `.torrent` file that contains the exact original `info` bytes, so the info hash
    /// of the saved file equals this torrent's.
    pub fn to_torrent_file(&self) -> Vec<u8> {
        let mut out = b"d".to_vec();
        let mut put = |key: &str, value: &Value| {
            out.extend(format!("{}:{key}", key.len()).as_bytes());
            out.extend(bencode::encode(value));
        };
        if let Some(first) = self.trackers.first().and_then(|t| t.first()) {
            put("announce", &Value::text(first));
        }
        if self.trackers.iter().map(Vec::len).sum::<usize>() > 1 {
            let tiers = self
                .trackers
                .iter()
                .map(|t| Value::List(t.iter().map(|u| Value::text(u)).collect()))
                .collect();
            put("announce-list", &Value::List(tiers));
        }
        out.extend(b"4:info");
        out.extend(&self.info_bytes);
        if !self.web_seeds.is_empty() {
            let list = Value::List(self.web_seeds.iter().map(|u| Value::text(u)).collect());
            let mut tail = Vec::new();
            tail.extend(format!("{}:url-list", "url-list".len()).as_bytes());
            tail.extend(bencode::encode(&list));
            out.extend(tail);
        }
        out.push(b'e');
        out
    }
}

/// Builds a minimal single- or multi-file `info` dictionary. Used by tests and by tools that
/// create torrents; hashes are computed from `data` split into pieces.
pub fn build_info(name: &str, piece_length: u64, files: &[(&[&str], &[u8])]) -> Vec<u8> {
    let data: Vec<u8> = files.iter().flat_map(|(_, d)| d.iter().copied()).collect();
    let pieces: Vec<u8> = data
        .chunks(piece_length as usize)
        .flat_map(|chunk| Sha1::digest(chunk).to_vec())
        .collect();
    let mut info = vec![
        ("name", Value::text(name)),
        ("piece length", Value::Int(piece_length as i64)),
        ("pieces", Value::Bytes(pieces)),
    ];
    if files.len() == 1 && files[0].0.len() == 1 && files[0].0[0] == name {
        info.push(("length", Value::Int(files[0].1.len() as i64)));
    } else {
        let list = files
            .iter()
            .map(|(path, data)| {
                dict([
                    ("length", Value::Int(data.len() as i64)),
                    (
                        "path",
                        Value::List(path.iter().map(|p| Value::text(p)).collect()),
                    ),
                ])
            })
            .collect();
        info.push(("files", Value::List(list)));
    }
    bencode::encode(&Value::Dict(
        info.into_iter()
            .map(|(k, v)| (k.as_bytes().to_vec(), v))
            .collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn torrent_file(info: &[u8], extra: &str) -> Vec<u8> {
        let mut out = b"d".to_vec();
        out.extend(extra.as_bytes());
        out.extend(b"4:info");
        out.extend(info);
        out.push(b'e');
        out
    }

    #[test]
    fn parses_a_single_file_torrent_and_computes_the_info_hash_from_raw_bytes() {
        let data = vec![7u8; 40_000];
        let info = build_info("movie.bin", 16384, &[(&["movie.bin"], &data)]);
        let file = torrent_file(&info, "8:announce23:http://tracker.test/ann");
        let t = Torrent::from_torrent_file(&file).unwrap();
        assert_eq!(t.name, "movie.bin");
        assert_eq!(t.total_length, 40_000);
        assert_eq!(t.piece_count(), 3);
        assert_eq!(t.piece_len(0), 16384);
        assert_eq!(t.piece_len(2), 40_000 - 2 * 16384);
        assert_eq!(t.info_hash, info_hash(&info));
        assert_eq!(
            t.trackers,
            vec![vec!["http://tracker.test/ann".to_string()]]
        );
        assert!(!t.is_multi_file());
        assert!(!t.private);
        assert_eq!(
            t.pieces[0].as_slice(),
            Sha1::digest(&data[..16384]).as_slice()
        );
    }

    #[test]
    fn multi_file_offsets_are_contiguous_and_the_saved_torrent_keeps_its_hash() {
        let (a, b) = (vec![1u8; 20_000], vec![2u8; 13_000]);
        let info = build_info("pack", 16384, &[(&["dir", "a.bin"], &a), (&["b.bin"], &b)]);
        let extras = Extras {
            trackers: vec![
                vec!["udp://t1.test:80".into(), "http://t2.test/a".into()],
                vec!["https://t3.test/a".into()],
            ],
            web_seeds: vec!["https://mirror.test/files/".into()],
        };
        let t = Torrent::from_info(&info, extras).unwrap();
        assert!(t.is_multi_file());
        assert_eq!(t.files[0].offset, 0);
        assert_eq!(t.files[1].offset, 20_000);
        assert_eq!(t.files[0].path, vec!["dir", "a.bin"]);
        let saved = t.to_torrent_file();
        let again = Torrent::from_torrent_file(&saved).unwrap();
        assert_eq!(
            again.info_hash, t.info_hash,
            "info bytes are preserved verbatim"
        );
        assert_eq!(again.trackers, t.trackers);
        assert_eq!(again.web_seeds, t.web_seeds);
        assert_eq!(again.files, t.files);
    }

    #[test]
    fn hostile_paths_are_neutralised() {
        for (raw, safe) in [
            ("..", "_"),
            (".", "_"),
            ("", "_"),
            ("a/b", "a_b"),
            ("..\\..\\evil", ".._.._evil"),
            ("CON", "_CON"),
            ("com1.txt", "_com1.txt"),
            ("trailing. ", "trailing"),
            ("bad:name?.txt", "bad_name_.txt"),
            ("nul\0byte", "nul_byte"),
        ] {
            assert_eq!(sanitize_component(raw), safe, "{raw:?}");
        }
        // A path that tries to escape is rewritten, never honoured.
        let data = vec![1u8; 20_000];
        let info = build_info("x", 16384, &[(&["..", "..", "etc", "passwd"], &data)]);
        let t = Torrent::from_info(&info, Extras::default()).unwrap();
        assert_eq!(t.files[0].path, vec!["_", "_", "etc", "passwd"]);
    }

    #[test]
    fn names_that_collide_after_sanitising_stay_distinct() {
        let (a, b) = (vec![1u8; 10_000], vec![2u8; 10_000]);
        let info = build_info("p", 16384, &[(&["a:b"], &a), (&["a?b"], &b)]);
        let t = Torrent::from_info(&info, Extras::default()).unwrap();
        assert_ne!(t.files[0].path, t.files[1].path);
    }

    #[test]
    fn inconsistent_or_hostile_metadata_is_rejected() {
        let data = vec![1u8; 40_000];
        let good = build_info("f", 16384, &[(&["f"], &data)]);
        assert!(Torrent::from_info(&good, Extras::default()).is_ok());
        // Wrong piece count.
        let short = build_info("f", 16384, &[(&["f"], &data[..20_000])]);
        let mut doc = bencode::decode(&short).unwrap().value;
        if let Value::Dict(d) = &mut doc {
            d.insert(b"length".to_vec(), Value::Int(1_000_000));
        }
        assert!(Torrent::from_info(&bencode::encode(&doc), Extras::default()).is_err());
        // Negative length, odd piece length, empty pieces.
        for (key, value) in [
            ("length", Value::Int(-5)),
            ("piece length", Value::Int(1000)),
            ("piece length", Value::Int(0)),
            ("pieces", Value::Bytes(vec![])),
            ("pieces", Value::Bytes(vec![0; 19])),
        ] {
            let mut doc = bencode::decode(&good).unwrap().value;
            if let Value::Dict(d) = &mut doc {
                d.insert(key.as_bytes().to_vec(), value);
            }
            assert!(
                Torrent::from_info(&bencode::encode(&doc), Extras::default()).is_err(),
                "{key}"
            );
        }
        assert!(Torrent::from_torrent_file(b"d4:spam4:eggse").is_err());
        assert!(Torrent::from_torrent_file(&vec![b'x'; MAX_TORRENT_BYTES + 1]).is_err());
    }

    #[test]
    fn only_web_urls_are_kept_as_trackers_and_web_seeds() {
        let data = vec![1u8; 20_000];
        let info = build_info("f", 16384, &[(&["f"], &data)]);
        let extras = Extras {
            trackers: vec![vec![
                "file:///etc/passwd".into(),
                "ftp://x/y".into(),
                "UDP://ok.test:1".into(),
            ]],
            web_seeds: vec!["file:///x".into(), "http://ok.test/f".into()],
        };
        let t = Torrent::from_info(&info, extras).unwrap();
        assert_eq!(t.trackers, vec![vec!["UDP://ok.test:1".to_string()]]);
        assert_eq!(t.web_seeds, vec!["http://ok.test/f".to_string()]);
    }

    #[test]
    fn announce_list_takes_precedence_and_private_flag_is_read() {
        let data = vec![1u8; 20_000];
        let mut info = bencode::decode(&build_info("f", 16384, &[(&["f"], &data)]))
            .unwrap()
            .value;
        if let Value::Dict(d) = &mut info {
            d.insert(b"private".to_vec(), Value::Int(1));
        }
        let info = bencode::encode(&info);
        let file = torrent_file(
            &info,
            "8:announce16:http://old.test/13:announce-listll17:http://new.test/aee",
        );
        let t = Torrent::from_torrent_file(&file).unwrap();
        assert!(t.private);
        assert_eq!(t.trackers, vec![vec!["http://new.test/a".to_string()]]);
    }
}
