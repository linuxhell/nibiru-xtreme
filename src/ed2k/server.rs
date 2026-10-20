use super::metadata::{Reader, Server, Value, tag};
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::{
    io::Read,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, oneshot},
};

const MAX_PACKET: usize = 4 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchResult {
    pub name: String,
    pub size: u64,
    pub hash: String,
    pub sources: u64,
    pub link: String,
}

/// A server-advertised source. Low-ID sources need a server callback;
/// their numeric client ID is never interpreted as an IP address.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileSource {
    pub client_id: u32,
    pub host: Option<std::net::Ipv4Addr>,
    pub port: u16,
    pub requires_callback: bool,
}

pub(crate) fn file_hash(value: &str) -> Result<[u8; 16]> {
    if value.len() != 32 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("invalid ED2K file hash");
    }
    let mut hash = [0; 16];
    for (index, byte) in hash.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)?;
    }
    Ok(hash)
}

fn file_sources(data: &[u8], expected: &[u8; 16]) -> Result<Vec<FileSource>> {
    let mut reader = Reader::new(data);
    if reader.take(16)? != expected {
        bail!("ED2K source response has a different file hash");
    }
    let count = reader.u8()? as usize;
    if data.len() != 17 + count * 6 {
        bail!("invalid ED2K source response length");
    }
    let mut sources = Vec::with_capacity(count);
    let mut seen = std::collections::HashSet::new();
    for _ in 0..count {
        let id = reader.u32()?;
        let port = reader.u16()?;
        if id == 0 || port == 0 || !seen.insert((id, port)) {
            continue;
        }
        let low_id = id < 0x0100_0000;
        sources.push(FileSource {
            client_id: id,
            host: (!low_id).then(|| std::net::Ipv4Addr::from(id.to_le_bytes())),
            port,
            requires_callback: low_id,
        });
    }
    Ok(sources)
}

pub(crate) async fn frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<(u8, Vec<u8>)> {
    let protocol = reader.read_u8().await?;
    if !matches!(protocol, 0xe3 | 0xd4 | 0xc5) {
        bail!("unexpected ED2K packet protocol");
    }
    let length = reader.read_u32_le().await? as usize;
    if length == 0 || length > MAX_PACKET {
        bail!("ED2K packet length exceeds limit");
    }
    let opcode = reader.read_u8().await?;
    let mut data = vec![0; length - 1];
    reader.read_exact(&mut data).await?;
    if protocol == 0xd4 {
        let mut decoded = Vec::new();
        flate2::read::ZlibDecoder::new(&data[..])
            .take((MAX_PACKET + 1) as u64)
            .read_to_end(&mut decoded)?;
        if decoded.len() > MAX_PACKET {
            bail!("ED2K decompressed packet exceeds limit");
        }
        data = decoded;
    }
    Ok((opcode, data))
}
pub(crate) async fn send<W: AsyncWrite + Unpin>(
    writer: &mut W,
    opcode: u8,
    data: &[u8],
) -> Result<()> {
    if data.len() >= MAX_PACKET {
        bail!("ED2K packet too large");
    }
    writer.write_u8(0xe3).await?;
    writer.write_u32_le((data.len() + 1) as u32).await?;
    writer.write_u8(opcode).await?;
    writer.write_all(data).await?;
    Ok(())
}
fn string_tag(data: &mut Vec<u8>, id: u8, value: &str) {
    data.push(2);
    data.extend(1u16.to_le_bytes());
    data.push(id);
    data.extend((value.len() as u16).to_le_bytes());
    data.extend(value.as_bytes());
}
fn number_tag(data: &mut Vec<u8>, id: u8, value: u32) {
    data.push(3);
    data.extend(1u16.to_le_bytes());
    data.push(id);
    data.extend(value.to_le_bytes());
}
fn login(hash: [u8; 16], nickname: &str, tcp_port: u16) -> Result<Vec<u8>> {
    if nickname.len() > 128 || nickname.chars().any(char::is_control) {
        bail!("invalid ED2K nickname");
    }
    let mut data = Vec::new();
    data.extend(hash);
    data.extend(0u32.to_le_bytes());
    data.extend(tcp_port.to_le_bytes());
    data.extend(3u32.to_le_bytes());
    string_tag(&mut data, 1, nickname);
    number_tag(&mut data, 0x11, 60);
    // Advertise only implemented tag encoding and Unicode, not obfuscation/AICH.
    number_tag(&mut data, 0x20, 0x18);
    Ok(data)
}
fn results(data: &[u8]) -> Result<Vec<SearchResult>> {
    let mut r = Reader::new(data);
    let count = r.u32()?;
    if count > 10_000 {
        bail!("too many ED2K search results");
    }
    let mut results = Vec::new();
    for _ in 0..count {
        let hash: String = r.take(16)?.iter().map(|b| format!("{b:02x}")).collect();
        r.take(6)?;
        let count = r.u32()?;
        if count > 256 {
            bail!("too many result tags");
        }
        let mut name = String::new();
        let mut size = 0;
        let mut high = 0;
        let mut sources = 0;
        for _ in 0..count {
            match tag(&mut r)? {
                (1, Value::Text(v)) => name = v,
                (2, Value::Number(v)) => size = v,
                (0x3a, Value::Number(v)) => high = v,
                (0x15, Value::Number(v)) => sources = v,
                _ => {}
            }
        }
        size |= high.checked_shl(32).unwrap_or(0);
        if !name.is_empty() {
            let encoded =
                percent_encoding::utf8_percent_encode(&name, percent_encoding::NON_ALPHANUMERIC);
            let link = format!("ed2k://|file|{encoded}|{size}|{hash}|/");
            results.push(SearchResult {
                name,
                size,
                hash,
                sources,
                link,
            });
        }
    }
    // Some servers append a one-byte "more results available" marker.
    if r.data.len() - r.pos > 1 {
        bail!("unexpected search result trailer");
    }
    Ok(results)
}

/// A complete local file offered to the server so other clients can find this one as a source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OfferedFile {
    pub hash: [u8; 16],
    pub name: String,
    pub size: u64,
}

/// Server TCP flags (from `OP_IDCHANGE`).
const SRV_NEWTAGS: u32 = 0x0000_0008;
const SRV_LARGEFILES: u32 = 0x0000_0100;
/// Marker ID/port eMule sends for complete files to servers that understand new tags.
const FILE_COMPLETE_ID: u32 = 0xFBFB_FBFB;
const FILE_COMPLETE_PORT: u16 = 0xFBFB;
/// Files per `OP_OFFERFILES` packet and per connection, so a server is never flooded.
pub const OFFER_BATCH: usize = 50;
pub const OFFER_LIMIT: usize = 5000;

fn file_type(name: &str) -> Option<&'static str> {
    let extension = name.rsplit_once('.')?.1.to_ascii_lowercase();
    Some(match extension.as_str() {
        "mp3" | "flac" | "ogg" | "opus" | "wav" | "m4a" | "aac" | "wma" | "ape" | "mka" => "Audio",
        "avi" | "mkv" | "mp4" | "mov" | "wmv" | "mpg" | "mpeg" | "flv" | "webm" | "m4v" | "ts" => {
            "Video"
        }
        "jpg" | "jpeg" | "png" | "gif" | "bmp" | "webp" | "tif" | "tiff" => "Image",
        "pdf" | "doc" | "docx" | "txt" | "epub" | "rtf" | "odt" | "xls" | "xlsx" | "ppt"
        | "pptx" => "Doc",
        "zip" | "rar" | "7z" | "tar" | "gz" | "bz2" | "xz" => "Arc",
        "iso" | "bin" | "img" | "nrg" | "cue" => "Iso",
        "exe" | "msi" | "apk" | "dmg" | "deb" | "rpm" | "appimage" => "Pro",
        _ => return None,
    })
}

/// Builds the body of `OP_OFFERFILES`. Files that the server cannot represent (over 4 GiB when
/// it lacks large-file support) are left out; the second value is how many were included.
pub(crate) fn offer_packet(
    files: &[OfferedFile],
    client_id: u32,
    port: u16,
    server_flags: u32,
) -> Result<(Vec<u8>, usize)> {
    if files.len() > OFFER_BATCH {
        bail!("too many files in one offer");
    }
    let (id, port) = if server_flags & SRV_NEWTAGS != 0 {
        (FILE_COMPLETE_ID, FILE_COMPLETE_PORT)
    } else if client_id >= 0x0100_0000 {
        (client_id, port)
    } else {
        // A Low-ID client cannot be reached through its ID.
        (0, 0)
    };
    let mut body = Vec::new();
    let mut count = 0u32;
    for file in files {
        if file.name.is_empty()
            || file.name.len() > 255
            || file.name.chars().any(char::is_control)
            || file.size == 0
        {
            continue;
        }
        let large = file.size > u32::MAX as u64;
        if large && server_flags & SRV_LARGEFILES == 0 {
            continue;
        }
        let kind = file_type(&file.name);
        body.extend(file.hash);
        body.extend(id.to_le_bytes());
        body.extend(port.to_le_bytes());
        let tags = 2 + u32::from(large) + u32::from(kind.is_some());
        body.extend(tags.to_le_bytes());
        string_tag(&mut body, 0x01, &file.name);
        number_tag(&mut body, 0x02, file.size as u32);
        if large {
            number_tag(&mut body, 0x3a, (file.size >> 32) as u32);
        }
        if let Some(kind) = kind {
            string_tag(&mut body, 0x03, kind);
        }
        count += 1;
    }
    let mut packet = count.to_le_bytes().to_vec();
    packet.extend(body);
    Ok((packet, count as usize))
}

type SourceReply = oneshot::Sender<Result<Vec<FileSource>>>;
type PendingSources = ([u8; 16], SourceReply);

enum Command {
    Search(String, oneshot::Sender<Result<Vec<SearchResult>>>),
    Sources([u8; 16], SourceReply),
    Callback(u32, oneshot::Sender<Result<()>>),
    Offer(Vec<u8>, oneshot::Sender<Result<()>>),
}
struct Inner {
    commands: mpsc::Sender<Command>,
    alive: Arc<AtomicBool>,
    task: tokio::task::AbortHandle,
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.task.abort();
        self.alive.store(false, Ordering::Release);
    }
}
#[derive(Clone)]
pub struct ServerConnection {
    inner: Arc<Inner>,
    pub server: Server,
    pub client_id: u32,
    /// Capability flags the server reported in `OP_IDCHANGE` (zero when it sent none).
    pub tcp_flags: u32,
    tcp_port: u16,
}
impl ServerConnection {
    pub async fn connect(
        server: Server,
        user_hash: [u8; 16],
        nickname: &str,
        tcp_port: u16,
    ) -> Result<Self> {
        let (stream, client_id, tcp_flags) = tokio::time::timeout(Duration::from_secs(15), async {
            let mut stream = TcpStream::connect((server.host.as_str(), server.port))
                .await
                .context("cannot connect to ED2K server")?;
            stream.set_nodelay(true)?;
            send(&mut stream, 0x01, &login(user_hash, nickname, tcp_port)?).await?;
            for _ in 0..32 {
                let (opcode, bytes) = frame(&mut stream).await?;
                match opcode {
                    0x40 => {
                        let mut reader = Reader::new(&bytes);
                        let id = reader.u32()?;
                        if id == 0 {
                            bail!("ED2K server rejected login");
                        }
                        let flags = reader.u32().unwrap_or(0);
                        return Ok((stream, id, flags));
                    }
                    0x05 => bail!("ED2K server rejected login"),
                    _ => {}
                }
            }
            bail!("ED2K server did not acknowledge login")
        })
        .await
        .context("ED2K login timed out")??;
        let (mut reader, mut writer) = stream.into_split();
        let (commands, mut receiver) = mpsc::channel(4);
        let alive = Arc::new(AtomicBool::new(true));
        let status = alive.clone();
        let task = tokio::spawn(async move {
            struct Offline(Arc<AtomicBool>);
            impl Drop for Offline {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::Release);
                }
            }
            let _offline = Offline(status);
            // One persistent read future avoids losing framing when a command arrives
            // halfway through a packet (AsyncRead::read_exact is not cancel-safe).
            let (packets, mut incoming) = mpsc::channel(8);
            let pump = tokio::spawn(async move {
                loop {
                    let packet = frame(&mut reader).await;
                    let failed = packet.is_err();
                    if packets.send(packet).await.is_err() || failed {
                        break;
                    }
                }
            });
            struct StopPump(tokio::task::AbortHandle);
            impl Drop for StopPump {
                fn drop(&mut self) {
                    self.0.abort();
                }
            }
            let _stop_pump = StopPump(pump.abort_handle());
            let mut pending: Option<oneshot::Sender<Result<Vec<SearchResult>>>> = None;
            let mut pending_sources: Option<PendingSources> = None;
            let mut source_deadline = tokio::time::Instant::now();
            let mut last_sources = None;
            let mut deadline = tokio::time::Instant::now() + Duration::from_secs(3600);
            loop {
                tokio::select! {
                    command = receiver.recv() => match command {
                        Some(Command::Search(query,reply)) => {
                            if pending.is_some() { let _ = reply.send(Err(anyhow!("search already in progress"))); continue; }
                            let mut data = vec![1]; data.extend((query.len() as u16).to_le_bytes()); data.extend(query.as_bytes());
                            if let Err(error) = send(&mut writer,0x16,&data).await { let _ = reply.send(Err(error)); break; }
                            pending=Some(reply); deadline=tokio::time::Instant::now()+Duration::from_secs(20);
                        },
                        Some(Command::Sources(hash,reply)) => {
                            if pending_sources.is_some() { let _ = reply.send(Err(anyhow!("source lookup already in progress"))); continue; }
                            if last_sources.is_some_and(|last: tokio::time::Instant| last.elapsed() < Duration::from_secs(1)) {
                                let _ = reply.send(Err(anyhow!("wait before requesting more sources"))); continue;
                            }
                            last_sources = Some(tokio::time::Instant::now());
                            match tokio::time::timeout(Duration::from_secs(10), send(&mut writer,0x19,&hash)).await {
                                Ok(Ok(())) => {},
                                error => { let _ = reply.send(Err(anyhow!("cannot request ED2K sources: {error:?}"))); break; }
                            }
                            pending_sources=Some((hash,reply));
                            source_deadline=tokio::time::Instant::now()+Duration::from_secs(20);
                        },
                        Some(Command::Callback(id,reply)) => {
                            match tokio::time::timeout(Duration::from_secs(10),send(&mut writer,0x1c,&id.to_le_bytes())).await {
                                Ok(Ok(()))=>{let _=reply.send(Ok(()));},
                                error=>{let _=reply.send(Err(anyhow!("cannot request ED2K callback: {error:?}")));break;},
                            }
                        },
                        Some(Command::Offer(packet,reply)) => {
                            match tokio::time::timeout(Duration::from_secs(10),send(&mut writer,0x15,&packet)).await {
                                Ok(Ok(()))=>{let _=reply.send(Ok(()));},
                                error=>{let _=reply.send(Err(anyhow!("cannot offer files to the ED2K server: {error:?}")));break;},
                            }
                        },
                        None => break,
                    },
                    packet = incoming.recv() => match packet {
                        Some(Ok((0x33,data))) => if let Some(reply) = pending.take() { let _ = reply.send(results(&data)); },
                        Some(Ok((0x42,data))) => {
                            if let Some((hash,_)) = pending_sources.as_ref() {
                                // Ignore unrelated unsolicited packets; never attach another
                                // file's sources to the pending request.
                                if data.get(..16) == Some(hash.as_slice()) {
                                    let (hash,reply) = pending_sources.take().expect("pending source lookup");
                                    let _ = reply.send(file_sources(&data,&hash));
                                }
                            }
                        },
                        Some(Ok(_)) => {},
                        _ => break,
                    },
                    _ = tokio::time::sleep_until(source_deadline), if pending_sources.is_some() => {
                        if let Some((_,reply)) = pending_sources.take() { let _ = reply.send(Err(anyhow!("ED2K source lookup timed out"))); }
                        break;
                    },
                    _ = tokio::time::sleep_until(deadline), if pending.is_some() => {
                        if let Some(reply) = pending.take() { let _ = reply.send(Err(anyhow!("ED2K search timed out"))); }
                        // A late response has no correlation ID; close the session so
                        // it can never be mistaken for a subsequent search result.
                        break;
                    }
                }
            }
            let _ = writer.shutdown().await;
        });
        Ok(Self {
            server,
            client_id,
            tcp_flags,
            tcp_port,
            inner: Arc::new(Inner {
                commands,
                alive,
                task: task.abort_handle(),
            }),
        })
    }
    /// Offers complete local files to the server (`OP_OFFERFILES`), in small batches, so other
    /// clients can find this one as a source. ED2K has no "withdraw" packet: to remove files from
    /// the server's index, reconnect and publish the new list. Returns how many files were sent.
    pub async fn offer_files(&self, files: &[OfferedFile]) -> Result<usize> {
        if !self.is_connected() {
            bail!("ED2K server is disconnected");
        }
        let mut sent = 0;
        for (index, batch) in files
            .iter()
            .take(OFFER_LIMIT)
            .collect::<Vec<_>>()
            .chunks(OFFER_BATCH)
            .enumerate()
        {
            if index > 0 {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            let batch: Vec<OfferedFile> = batch.iter().map(|f| (*f).clone()).collect();
            let (packet, count) =
                offer_packet(&batch, self.client_id, self.tcp_port, self.tcp_flags)?;
            if count == 0 {
                continue;
            }
            let (reply, response) = oneshot::channel();
            self.inner
                .commands
                .send(Command::Offer(packet, reply))
                .await
                .context("ED2K server disconnected")?;
            response
                .await
                .context("ED2K server disconnected while offering files")??;
            sent += count;
        }
        Ok(sent)
    }

    pub fn disconnect(&self) {
        self.inner.alive.store(false, Ordering::Release);
        self.inner.task.abort();
    }

    pub fn is_connected(&self) -> bool {
        self.inner.alive.load(Ordering::Acquire)
    }
    pub fn is_low_id(&self) -> bool {
        self.client_id < 0x0100_0000
    }
    pub async fn request_callback(&self, id: u32) -> Result<()> {
        if id == 0 || id >= 0x0100_0000 || self.is_low_id() {
            bail!("invalid ED2K callback route");
        }
        if !self.is_connected() {
            bail!("ED2K server is disconnected");
        }
        let (reply, response) = oneshot::channel();
        self.inner
            .commands
            .send(Command::Callback(id, reply))
            .await
            .context("ED2K server disconnected")?;
        response
            .await
            .context("ED2K server disconnected during callback")?
    }
    /// Ask the connected server for sources using the baseline OP_GETSOURCES
    /// hash-only packet. This does not connect to peers or request callbacks.
    pub async fn sources(&self, hash: &str) -> Result<Vec<FileSource>> {
        let hash = file_hash(hash)?;
        if !self.is_connected() {
            bail!("ED2K server is disconnected");
        }
        let (reply, response) = oneshot::channel();
        self.inner
            .commands
            .send(Command::Sources(hash, reply))
            .await
            .context("ED2K server disconnected")?;
        response
            .await
            .context("ED2K server disconnected during source lookup")?
    }
    pub async fn search(&self, query: &str) -> Result<Vec<SearchResult>> {
        if query.trim().is_empty() || query.len() > 1024 || query.chars().any(char::is_control) {
            bail!("invalid search query");
        }
        if !self.is_connected() {
            bail!("ED2K server is disconnected");
        }
        let (reply, response) = oneshot::channel();
        self.inner
            .commands
            .send(Command::Search(query.to_owned(), reply))
            .await
            .context("ED2K server disconnected")?;
        response
            .await
            .context("ED2K server disconnected during search")?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn server_login_and_search_use_real_wire_packets() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let fixture = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (opcode, bytes) = frame(&mut stream).await.unwrap();
            assert_eq!(opcode, 1);
            assert_eq!(&bytes[..16], &[7; 16]);
            send(&mut stream, 0x40, &123u32.to_le_bytes())
                .await
                .unwrap();
            let (opcode, bytes) = frame(&mut stream).await.unwrap();
            assert_eq!(opcode, 0x16);
            assert_eq!(&bytes[3..], b"test");
            let mut result = Vec::new();
            result.extend(1u32.to_le_bytes());
            result.extend([8; 16]);
            result.extend([0; 6]);
            result.extend(3u32.to_le_bytes());
            string_tag(&mut result, 1, "example.txt");
            number_tag(&mut result, 2, 1234);
            number_tag(&mut result, 0x15, 5);
            send(&mut stream, 0x33, &result).await.unwrap();
        });
        let server = Server {
            host: "127.0.0.1".into(),
            port: address.port(),
            name: "fixture".into(),
            description: String::new(),
        };
        let connection = ServerConnection::connect(server, [7; 16], "Nibiru", 0)
            .await
            .unwrap();
        assert!(connection.is_low_id());
        let results = connection.search("test").await.unwrap();
        assert_eq!(results[0].size, 1234);
        assert_eq!(results[0].sources, 5);
        assert!(results[0].link.contains("ed2k://|file|example"));
        fixture.await.unwrap();
    }
    /// Parses an OP_OFFERFILES body back into (hash, id, port, name, size, type) rows.
    #[allow(clippy::type_complexity)]
    fn parse_offer(body: &[u8]) -> Vec<([u8; 16], u32, u16, String, u64, Option<String>)> {
        let mut r = Reader::new(body);
        let count = r.u32().unwrap();
        let mut rows = Vec::new();
        for _ in 0..count {
            let hash: [u8; 16] = r.take(16).unwrap().try_into().unwrap();
            let id = r.u32().unwrap();
            let port = r.u16().unwrap();
            let tags = r.u32().unwrap();
            let (mut name, mut size, mut high, mut kind) = (String::new(), 0u64, 0u64, None);
            for _ in 0..tags {
                match tag(&mut r).unwrap() {
                    (1, Value::Text(t)) => name = t,
                    (2, Value::Number(n)) => size = n,
                    (0x3a, Value::Number(n)) => high = n,
                    (3, Value::Text(t)) => kind = Some(t),
                    (id, _) => panic!("unexpected tag {id}"),
                }
            }
            rows.push((hash, id, port, name, size | (high << 32), kind));
        }
        assert_eq!(r.pos, body.len(), "no trailing bytes");
        rows
    }

    fn offered(n: u8, name: &str, size: u64) -> OfferedFile {
        OfferedFile {
            hash: [n; 16],
            name: name.into(),
            size,
        }
    }

    #[test]
    fn offer_packets_use_complete_file_markers_or_the_real_id_by_server_capability() {
        let files = [offered(1, "song.mp3", 5_000_000)];
        // Server with NEWTAGS: complete-file markers.
        let (packet, n) = offer_packet(&files, 0x0200_0001, 4662, SRV_NEWTAGS).unwrap();
        assert_eq!(n, 1);
        let rows = parse_offer(&packet);
        assert_eq!(
            (rows[0].1, rows[0].2),
            (FILE_COMPLETE_ID, FILE_COMPLETE_PORT)
        );
        assert_eq!(
            (rows[0].3.as_str(), rows[0].4, rows[0].5.as_deref()),
            ("song.mp3", 5_000_000, Some("Audio"))
        );
        // Older server, High ID: the real ID and port.
        let rows = parse_offer(&offer_packet(&files, 0x0200_0001, 4662, 0).unwrap().0);
        assert_eq!((rows[0].1, rows[0].2), (0x0200_0001, 4662));
        // Low ID cannot be reached through its ID, so none is advertised.
        let rows = parse_offer(&offer_packet(&files, 1234, 4662, 0).unwrap().0);
        assert_eq!((rows[0].1, rows[0].2), (0, 0));
    }

    #[test]
    fn offers_skip_unrepresentable_files_and_encode_large_sizes() {
        let big = 5_000_000_000u64;
        let files = [
            offered(1, "movie.mkv", big),
            offered(2, "noname", 10),
            offered(3, "", 10),
            offered(4, "bad\nname.txt", 10),
            offered(5, "empty.txt", 0),
            offered(6, &"x".repeat(300), 10),
        ];
        // Without large-file support only the plain file survives.
        let (packet, n) = offer_packet(&files, 0x0200_0001, 1, 0).unwrap();
        let rows = parse_offer(&packet);
        assert_eq!(n, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].3, "noname");
        assert_eq!(rows[0].5, None);
        // With large-file support the 64-bit size is split across two tags.
        let (packet, n) = offer_packet(&files[..1], 0x0200_0001, 1, SRV_LARGEFILES).unwrap();
        assert_eq!(n, 1);
        assert_eq!(parse_offer(&packet)[0].4, big);
        assert!(offer_packet(&vec![offered(1, "a.txt", 1); OFFER_BATCH + 1], 1, 1, 0).is_err());
    }

    #[tokio::test]
    async fn files_are_published_in_batches_on_the_wire() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let fixture = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            assert_eq!(frame(&mut stream).await.unwrap().0, 1);
            // IDCHANGE: High ID plus server flags (NEWTAGS).
            let mut id = 0x0300_0001u32.to_le_bytes().to_vec();
            id.extend(SRV_NEWTAGS.to_le_bytes());
            send(&mut stream, 0x40, &id).await.unwrap();
            let mut seen = Vec::new();
            for _ in 0..3 {
                let (opcode, body) = frame(&mut stream).await.unwrap();
                assert_eq!(opcode, 0x15);
                seen.push(parse_offer(&body).len());
            }
            seen
        });
        let server = Server {
            host: "127.0.0.1".into(),
            port: address.port(),
            name: "fixture".into(),
            description: String::new(),
        };
        let connection = ServerConnection::connect(server, [7; 16], "Nibiru", 4662)
            .await
            .unwrap();
        assert!(!connection.is_low_id());
        assert_eq!(connection.tcp_flags, SRV_NEWTAGS);
        let files: Vec<_> = (0..120u8)
            .map(|n| offered(n, &format!("file{n}.bin"), 100 + n as u64))
            .collect();
        assert_eq!(connection.offer_files(&files).await.unwrap(), 120);
        assert_eq!(fixture.await.unwrap(), vec![50, 50, 20]);
        connection.disconnect();
        assert!(connection.offer_files(&files).await.is_err());
    }

    #[test]
    fn source_packets_validate_hash_length_duplicates_and_low_ids() {
        assert!(file_hash("é").is_err());
        assert!(file_hash(&"g".repeat(32)).is_err());
        assert_eq!(file_hash(&"AB".repeat(16)).unwrap(), [0xab; 16]);
        let mut data = vec![9; 16];
        data.push(4);
        for (id, port) in [(0x04030201u32, 4662u16), (123, 4663), (123, 4663), (0, 0)] {
            data.extend(id.to_le_bytes());
            data.extend(port.to_le_bytes());
        }
        let sources = file_sources(&data, &[9; 16]).unwrap();
        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0].host.unwrap().to_string(), "1.2.3.4");
        assert!(!sources[0].requires_callback);
        assert!(sources[1].host.is_none());
        assert!(sources[1].requires_callback);
        assert!(file_sources(&data, &[8; 16]).is_err());
        data.pop();
        assert!(file_sources(&data, &[9; 16]).is_err());
        data.extend([0, 0]);
        assert!(file_sources(&data, &[9; 16]).is_err());
    }

    #[tokio::test]
    async fn source_lookup_correlates_real_wire_packets_and_disconnect_cancels() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (finish, wait) = oneshot::channel();
        let fixture = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            assert_eq!(frame(&mut stream).await.unwrap().0, 1);
            send(&mut stream, 0x40, &123u32.to_le_bytes())
                .await
                .unwrap();
            let (opcode, hash) = frame(&mut stream).await.unwrap();
            assert_eq!(opcode, 0x19);
            assert_eq!(hash, vec![0xab; 16]);
            let mut response = vec![0xcd; 16];
            response.push(0);
            send(&mut stream, 0x42, &response).await.unwrap();
            response = vec![0xab; 16];
            response.push(1);
            response.extend([1, 2, 3, 4]);
            response.extend(4662u16.to_le_bytes());
            send(&mut stream, 0x42, &response).await.unwrap();
            let _ = wait.await;
        });
        let connection = ServerConnection::connect(
            Server {
                host: "127.0.0.1".into(),
                port: address.port(),
                name: "fixture".into(),
                description: String::new(),
            },
            [7; 16],
            "Nibiru",
            0,
        )
        .await
        .unwrap();
        assert!(connection.sources("invalid").await.is_err());
        let sources = connection.sources(&"ab".repeat(16)).await.unwrap();
        assert_eq!(sources[0].host.unwrap().to_string(), "1.2.3.4");
        assert!(
            connection
                .sources(&"ab".repeat(16))
                .await
                .unwrap_err()
                .to_string()
                .contains("wait")
        );
        connection.disconnect();
        assert!(connection.sources(&"ab".repeat(16)).await.is_err());
        let _ = finish.send(());
        fixture.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_packet_length_bombs_before_allocating() {
        let mut bytes = &[0xe3, 255, 255, 255, 255][..];
        assert!(frame(&mut bytes).await.is_err());
    }
}
