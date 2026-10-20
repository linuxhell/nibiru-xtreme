//! BEP 19 web seeds (GetRight style): HTTP servers that hold the torrent's files.
//!
//! A worker claims a whole piece, fetches it with `Range` requests (one per file the piece
//! touches), and hands it to the normal verification path. Wrong data therefore cannot be
//! stored: the piece hash decides. URLs come from untrusted torrents, so name resolution goes
//! through the same private-address policy as trackers and redirects are re-validated.

use super::{
    metainfo::Torrent,
    torrent::{Shared, State, finish_piece},
    tracker::resolve,
};
use anyhow::{Context, Result, bail};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use std::{sync::Arc, time::Duration};

const WORKERS_PER_SEED: usize = 2;
const MAX_FAILURES: u32 = 8;
const MAX_REDIRECTS: usize = 3;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const PATH_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

pub(crate) fn spawn_workers(shared: &Arc<Shared>) {
    let urls: Vec<String> = shared
        .lock()
        .web_seeds
        .iter()
        .map(|w| w.url.clone())
        .collect();
    for (index, _) in urls.iter().enumerate() {
        for _ in 0..WORKERS_PER_SEED {
            tokio::spawn(worker(shared.clone(), index));
        }
    }
}

/// URL of `file` on a web seed whose base is `base` (BEP 19 naming rules).
pub fn file_url(base: &str, torrent: &Torrent, file: usize) -> String {
    let entry = &torrent.files[file];
    let encode = |part: &str| utf8_percent_encode(part, PATH_SET).to_string();
    if !torrent.is_multi_file() {
        return if base.ends_with('/') {
            format!("{base}{}", encode(&torrent.name))
        } else {
            base.to_owned()
        };
    }
    let mut url = base.to_owned();
    if !url.ends_with('/') {
        url.push('/');
    }
    url.push_str(&encode(&torrent.name));
    for component in &entry.path {
        url.push('/');
        url.push_str(&encode(component));
    }
    url
}

async fn fetch_range(shared: &Shared, url: &str, from: u64, len: u64) -> Result<Vec<u8>> {
    let mut url = url::Url::parse(url).context("invalid web seed URL")?;
    if !matches!(url.scheme(), "http" | "https") {
        bail!("web seeds must be HTTP(S)");
    }
    let allow_private = shared.tracker.allow_private;
    for _ in 0..=MAX_REDIRECTS {
        let host = url
            .host_str()
            .context("web seed URL has no host")?
            .to_owned();
        let port = url
            .port_or_known_default()
            .context("web seed URL has no port")?;
        let addresses = resolve(&host, port, allow_private).await?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(REQUEST_TIMEOUT)
            .user_agent(&shared.tracker.user_agent)
            .resolve_to_addrs(&host, &addresses)
            .build()?;
        let response = client
            .get(url.clone())
            .header("Range", format!("bytes={from}-{}", from + len - 1))
            .send()
            .await?;
        if response.status().is_redirection() {
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .context("redirect without a location")?;
            url = url.join(location)?;
            if !matches!(url.scheme(), "http" | "https") {
                bail!("web seed redirected to a non-HTTP URL");
            }
            continue;
        }
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            // 200 would be the whole file; 404 means the layout is wrong. Neither is usable.
            bail!("web seed answered {}", response.status());
        }
        if response.content_length().is_some_and(|n| n != len) {
            bail!("web seed returned the wrong amount of data");
        }
        let mut body = Vec::with_capacity(len as usize);
        let mut response = response;
        while let Some(chunk) = response.chunk().await? {
            if body.len() + chunk.len() > len as usize {
                bail!("web seed sent more data than requested");
            }
            body.extend_from_slice(&chunk);
        }
        if body.len() as u64 != len {
            bail!("web seed sent less data than requested");
        }
        return Ok(body);
    }
    bail!("too many web seed redirects")
}

async fn fetch_piece(
    shared: &Shared,
    base: &str,
    torrent: &Torrent,
    piece: usize,
) -> Result<Vec<u8>> {
    let start = piece as u64 * torrent.piece_length;
    let length = torrent.piece_len(piece);
    let end = start + length;
    let mut out = vec![0u8; length as usize];
    for (index, file) in torrent.files.iter().enumerate() {
        let from = start.max(file.offset);
        let to = end.min(file.offset + file.length);
        if file.length == 0 || to <= from || file.pad {
            continue; // padding reads as zeros
        }
        let data = fetch_range(
            shared,
            &file_url(base, torrent, index),
            from - file.offset,
            to - from,
        )
        .await?;
        let at = (from - start) as usize;
        out[at..at + data.len()].copy_from_slice(&data);
    }
    Ok(out)
}

async fn worker(shared: Arc<Shared>, seed: usize) {
    let key = shared.next_key();
    let mut backoff = Duration::from_secs(1);
    loop {
        if shared.is_stopped() {
            return;
        }
        let claim = {
            let mut core = shared.lock();
            if core.web_seeds[seed].disabled {
                return;
            }
            if core.paused || core.state != State::Downloading {
                None
            } else {
                let base = core.web_seeds[seed].url.clone();
                core.ready_mut().and_then(|r| {
                    let torrent = r.torrent.clone();
                    r.picker
                        .claim_piece(key)
                        .map(|piece| (base, torrent, piece))
                })
            }
        };
        let Some((base, torrent, piece)) = claim else {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                _ = shared.stop_signal.notified() => {}
            }
            continue;
        };
        shared.lock().web_seeds[seed].active += 1;
        let result = fetch_piece(&shared, &base, &torrent, piece).await;
        match record(&shared, seed, key, result) {
            Some(data) => {
                backoff = Duration::from_secs(1);
                let length = data.len() as u64;
                shared.down_limit.acquire(length).await;
                shared.own_down.acquire(length).await;
                // Verified and written like any other piece; a bad seed just fails the hash.
                finish_piece(&shared, piece, data).await;
                let stored = shared
                    .lock()
                    .ready()
                    .is_some_and(|r| r.picker.have().get(piece));
                let mut core = shared.lock();
                if stored {
                    core.web_seeds[seed].failures = 0;
                } else {
                    // The bytes did not match the piece hash: this server serves wrong data.
                    core.web_seeds[seed].failures += 1;
                    core.web_seeds[seed].disabled |= core.web_seeds[seed].failures >= 3;
                }
            }
            None => {
                shared.wake.notify_one();
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        }
    }
}

/// Bookkeeping after a fetch; returns the data when it should go on to verification.
fn record(shared: &Shared, seed: usize, key: u64, result: Result<Vec<u8>>) -> Option<Vec<u8>> {
    let mut core = shared.lock();
    core.web_seeds[seed].active = core.web_seeds[seed].active.saturating_sub(1);
    match result {
        Ok(data) => {
            let length = data.len() as u64;
            core.web_seeds[seed].downloaded += length;
            core.down.add(length);
            Some(data)
        }
        Err(_) => {
            if let Some(r) = core.ready_mut() {
                r.picker.release_peer(key);
            }
            core.web_seeds[seed].failures += 1;
            if core.web_seeds[seed].failures >= MAX_FAILURES {
                core.web_seeds[seed].disabled = true;
            }
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bittorrent::metainfo::{Extras, build_info};

    #[test]
    fn urls_follow_bep19_naming() {
        let single = Torrent::from_info(
            &build_info("movie.mkv", 16384, &[(&["movie.mkv"], &[1u8; 100])]),
            Extras::default(),
        )
        .unwrap();
        assert_eq!(
            file_url("http://h/files/", &single, 0),
            "http://h/files/movie.mkv"
        );
        assert_eq!(
            file_url("http://h/files/movie.mkv", &single, 0),
            "http://h/files/movie.mkv"
        );
        let multi = Torrent::from_info(
            &build_info(
                "My Show",
                16384,
                &[
                    (&["s 1", "ep#1.mkv"], &[1u8; 100]),
                    (&["b.txt"], &[2u8; 10]),
                ],
            ),
            Extras::default(),
        )
        .unwrap();
        assert_eq!(
            file_url("http://h/f", &multi, 0),
            "http://h/f/My%20Show/s%201/ep%231.mkv"
        );
        assert_eq!(
            file_url("http://h/f/", &multi, 1),
            "http://h/f/My%20Show/b.txt"
        );
    }
}
