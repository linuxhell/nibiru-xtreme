//! A loopback HTTP server that lets a media player watch a torrent while it downloads.
//!
//! `GET /stream/<token>/<info-hash>/<file>` answers like a normal file server (HEAD, Range,
//! 206/416) but every read is served by [`TorrentHandle::read_file`], which steers the downloader
//! toward the pieces the player is asking for and waits for them to arrive. Seeking therefore
//! works, including the "read the end of the file first" probe that MP4 players do.

use super::{session::Session, torrent::TorrentHandle};
use anyhow::{Result, anyhow, bail};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

const CHUNK: usize = 256 * 1024;
const MAX_HEAD: usize = 16 * 1024;
const HEAD_TIMEOUT: Duration = Duration::from_secs(10);
/// How long one chunk may wait for the swarm before the connection is given up.
const CHUNK_PATIENCE: Duration = Duration::from_secs(90);

pub struct StreamServer {
    port: u16,
    token: String,
    stop: crate::lifetime::Stop,
}

impl StreamServer {
    pub async fn start(session: &Arc<Session>) -> Result<Arc<Self>> {
        let listener =
            TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
        let port = listener.local_addr()?.port();
        let token = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let server = Arc::new(Self {
            port,
            token,
            stop: crate::lifetime::Stop::default(),
        });
        let weak: Weak<Session> = Arc::downgrade(session);
        let server_weak = Arc::downgrade(&server);
        let mut stopped = server.stop.subscribe();
        let mut session_stopped = session.stopped();
        tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    _ = stopped.changed() => return,
                    _ = session_stopped.changed() => return,
                    value = listener.accept() => value,
                };
                let Ok((stream, peer)) = accepted else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                };
                if !peer.ip().is_loopback() {
                    continue;
                }
                let Some(session) = weak.upgrade() else {
                    return;
                };
                let Some(guard) = server_weak.upgrade() else {
                    return;
                };
                tokio::spawn(async move {
                    let _ = guard.connection(stream, &session).await;
                });
            }
        });
        Ok(server)
    }

    pub fn url(&self, info_hash: &[u8; 20], file: usize) -> String {
        let hash: String = info_hash.iter().map(|b| format!("{b:02x}")).collect();
        format!(
            "http://127.0.0.1:{}/stream/{}/{hash}/{file}",
            self.port, self.token
        )
    }

    async fn connection(&self, mut stream: TcpStream, session: &Session) -> Result<()> {
        let head = timeout(HEAD_TIMEOUT, read_head(&mut stream)).await??;
        let request = Request::parse(&head)?;
        if !request.host_is_loopback(self.port) {
            // DNS rebinding: a web page must not be able to reach this through a hostname.
            return respond(&mut stream, 421, "Misdirected Request", &[]).await;
        }
        let Some(rest) = request.path.strip_prefix("/stream/") else {
            return respond(&mut stream, 404, "Not Found", &[]).await;
        };
        let mut parts = rest.split('/');
        let (token, hash, index) = (parts.next(), parts.next(), parts.next());
        if token != Some(self.token.as_str()) || parts.next().is_some() {
            return respond(&mut stream, 403, "Forbidden", &[]).await;
        }
        let (Some(hash), Some(index)) = (
            hash.and_then(parse_hash),
            index.and_then(|i| i.parse::<usize>().ok()),
        ) else {
            return respond(&mut stream, 400, "Bad Request", &[]).await;
        };
        let Some(handle) = session.get(&hash) else {
            return respond(&mut stream, 404, "Not Found", &[]).await;
        };
        if request.method != "GET" && request.method != "HEAD" {
            return respond(
                &mut stream,
                405,
                "Method Not Allowed",
                &[("Allow", "GET, HEAD".into())],
            )
            .await;
        }
        serve(
            &mut stream,
            &handle,
            index,
            &request.method,
            request.range.as_deref(),
        )
        .await
    }
}

fn parse_hash(text: &str) -> Option<[u8; 20]> {
    if text.len() != 40 {
        return None;
    }
    let mut out = [0u8; 20];
    for (i, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(out)
}

async fn read_head(stream: &mut TcpStream) -> Result<String> {
    let mut buffer = Vec::new();
    let mut byte = [0u8; 1024];
    loop {
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            bail!("closed before the request was complete");
        }
        buffer.extend_from_slice(&byte[..n]);
        if buffer.len() > MAX_HEAD {
            bail!("request header too large");
        }
        if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            return Ok(String::from_utf8_lossy(&buffer[..end]).into_owned());
        }
    }
}

struct Request {
    method: String,
    path: String,
    host: Option<String>,
    range: Option<String>,
}

impl Request {
    fn parse(head: &str) -> Result<Self> {
        let mut lines = head.lines();
        let first = lines.next().ok_or_else(|| anyhow!("empty request"))?;
        let mut parts = first.split_whitespace();
        let method = parts.next().ok_or_else(|| anyhow!("no method"))?.to_owned();
        let path = parts.next().ok_or_else(|| anyhow!("no path"))?.to_owned();
        let mut host = None;
        let mut range = None;
        for line in lines {
            if let Some((name, value)) = line.split_once(':') {
                match name.trim().to_ascii_lowercase().as_str() {
                    "host" => host = Some(value.trim().to_owned()),
                    "range" => range = Some(value.trim().to_owned()),
                    _ => {}
                }
            }
        }
        Ok(Self {
            method,
            path,
            host,
            range,
        })
    }

    fn host_is_loopback(&self, port: u16) -> bool {
        match self.host.as_deref() {
            Some(host) => [
                format!("127.0.0.1:{port}"),
                format!("localhost:{port}"),
                format!("[::1]:{port}"),
            ]
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(host)),
            None => false,
        }
    }
}

/// `Range: bytes=...` for a resource of `length` bytes. `Ok(None)` means "serve everything".
fn parse_range(header: &str, length: u64) -> Result<Option<(u64, u64)>, ()> {
    let Some(spec) = header.strip_prefix("bytes=") else {
        return Ok(None);
    };
    if spec.contains(',') {
        return Ok(None); // multi-range requests are answered with the whole file
    }
    if length == 0 {
        return Err(());
    }
    let (from, to) = spec.split_once('-').ok_or(())?;
    let (start, end) = match (from.trim(), to.trim()) {
        ("", suffix) => {
            let n: u64 = suffix.parse().map_err(|_| ())?;
            if n == 0 {
                return Err(());
            }
            (length.saturating_sub(n), length - 1)
        }
        (start, "") => (start.parse().map_err(|_| ())?, length.saturating_sub(1)),
        (start, end) => (start.parse().map_err(|_| ())?, end.parse().map_err(|_| ())?),
    };
    if length == 0 || start > end || start >= length {
        return Err(());
    }
    Ok(Some((start, end.min(length - 1))))
}

pub fn content_type(name: &str) -> &'static str {
    let ext = name.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    match ext.as_str() {
        "mp4" | "m4v" => "video/mp4",
        "mkv" => "video/x-matroska",
        "webm" => "video/webm",
        "mov" => "video/quicktime",
        "avi" => "video/x-msvideo",
        "ts" | "m2ts" => "video/mp2t",
        "mpg" | "mpeg" => "video/mpeg",
        "mp3" => "audio/mpeg",
        "m4a" | "aac" => "audio/mp4",
        "flac" => "audio/flac",
        "ogg" | "oga" | "opus" => "audio/ogg",
        "wav" => "audio/wav",
        "srt" | "vtt" | "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

async fn respond<W: AsyncWrite + Unpin>(
    w: &mut W,
    code: u16,
    reason: &str,
    extra: &[(&str, String)],
) -> Result<()> {
    let mut out = format!("HTTP/1.1 {code} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n");
    for (name, value) in extra {
        out.push_str(&format!("{name}: {value}\r\n"));
    }
    out.push_str("\r\n");
    w.write_all(out.as_bytes()).await?;
    Ok(())
}

/// Writes one HTTP response for `file` of `handle`.
pub async fn serve<W: AsyncWrite + Unpin>(
    writer: &mut W,
    handle: &TorrentHandle,
    file: usize,
    method: &str,
    range: Option<&str>,
) -> Result<()> {
    let Some(torrent) = handle.torrent() else {
        return respond(
            writer,
            503,
            "Service Unavailable",
            &[("Retry-After", "2".into())],
        )
        .await;
    };
    let Some(entry) = torrent.files.get(file).filter(|f| !f.pad) else {
        return respond(writer, 404, "Not Found", &[]).await;
    };
    let length = entry.length;
    let name = entry.path.last().map(String::as_str).unwrap_or("file");
    let (start, end, partial) = match range.map(|r| parse_range(r, length)) {
        None | Some(Ok(None)) => (0, length.saturating_sub(1), false),
        Some(Ok(Some((start, end)))) => (start, end, true),
        Some(Err(())) => {
            return respond(
                writer,
                416,
                "Range Not Satisfiable",
                &[("Content-Range", format!("bytes */{length}"))],
            )
            .await;
        }
    };
    let size = if length == 0 { 0 } else { end - start + 1 };
    let mut head = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {size}\r\nAccept-Ranges: bytes\r\n\
         Cache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n",
        if partial {
            "206 Partial Content"
        } else {
            "200 OK"
        },
        content_type(name),
    );
    if partial {
        head.push_str(&format!("Content-Range: bytes {start}-{end}/{length}\r\n"));
    }
    head.push_str("\r\n");
    writer.write_all(head.as_bytes()).await?;
    if method == "HEAD" || size == 0 {
        return Ok(());
    }
    let mut position = start;
    while position <= end {
        let want = (end - position + 1).min(CHUNK as u64) as usize;
        let data = handle
            .read_file(file, position, want, CHUNK_PATIENCE)
            .await?;
        if data.is_empty() {
            bail!("short read");
        }
        writer.write_all(&data).await?;
        position += data.len() as u64;
    }
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_follow_rfc_9110() {
        assert_eq!(parse_range("bytes=0-99", 1000), Ok(Some((0, 99))));
        assert_eq!(parse_range("bytes=900-", 1000), Ok(Some((900, 999))));
        assert_eq!(parse_range("bytes=-100", 1000), Ok(Some((900, 999))));
        assert_eq!(parse_range("bytes=-5000", 1000), Ok(Some((0, 999))));
        assert_eq!(parse_range("bytes=990-5000", 1000), Ok(Some((990, 999))));
        assert_eq!(parse_range("bytes=1000-", 1000), Err(()));
        assert_eq!(parse_range("bytes=50-10", 1000), Err(()));
        assert_eq!(parse_range("bytes=-0", 1000), Err(()));
        assert_eq!(parse_range("bytes=abc", 1000), Err(()));
        assert_eq!(parse_range("bytes=0-1,5-6", 1000), Ok(None));
        assert_eq!(parse_range("items=0-1", 1000), Ok(None));
        assert_eq!(parse_range("bytes=0-", 0), Err(()));
        assert_eq!(parse_range("bytes=-1", 0), Err(()));
        assert_eq!(parse_range("bytes=-5000", 0), Err(()));
    }

    #[test]
    fn host_header_must_be_loopback_with_our_port() {
        let ok = Request::parse("GET /stream/a/b/0 HTTP/1.1\r\nHost: 127.0.0.1:5000").unwrap();
        assert!(ok.host_is_loopback(5000));
        assert!(!ok.host_is_loopback(5001));
        let evil = Request::parse("GET / HTTP/1.1\r\nHost: evil.example:5000").unwrap();
        assert!(!evil.host_is_loopback(5000));
        let none = Request::parse("GET / HTTP/1.1").unwrap();
        assert!(!none.host_is_loopback(5000));
    }

    #[test]
    fn media_types_are_guessed_from_the_extension() {
        assert_eq!(content_type("Movie.MKV"), "video/x-matroska");
        assert_eq!(content_type("a.mp4"), "video/mp4");
        assert_eq!(content_type("unknown"), "application/octet-stream");
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use crate::bittorrent::session::SessionConfig;

    #[tokio::test]
    async fn dropping_an_idle_preview_releases_its_tcp_listener() {
        let session = Session::start(SessionConfig {
            bind: "127.0.0.1".parse().unwrap(),
            enable_utp: false,
            ..SessionConfig::default()
        })
        .await
        .unwrap();
        let preview = StreamServer::start(&session).await.unwrap();
        let address = SocketAddr::from(([127, 0, 0, 1], preview.port));
        drop(preview);
        timeout(Duration::from_secs(2), async {
            loop {
                if std::net::TcpListener::bind(address).is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("preview retained its listener after the owner was dropped");
    }
}
