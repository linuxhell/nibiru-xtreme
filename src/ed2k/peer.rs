//! Baseline ED2K peer transport, with bounded frames and verified part hashsets.
use super::obfuscation::{self, Obfuscation, PeerStream};
use super::{
    FileLink, FileSource,
    file::PART_SIZE,
    metadata::{Reader, Value, tag},
};
use anyhow::{Context, Result, bail};
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
};

const MAX_PACKET: usize = 4 * 1024 * 1024;

/// How long a peer may take before it is given up on. The defaults are what the engine always
/// used; callers (and tests) may shorten or lengthen them.
#[derive(Clone, Copy, Debug)]
pub struct Tuning {
    /// Waiting for the peer's file name, part status and, if needed, hashset.
    pub metadata_timeout: Duration,
    /// Waiting in a peer's upload queue for a slot.
    pub queue_wait: Duration,
    /// A part another peer has been fetching this long may be fetched by an idle peer too.
    pub endgame_after: Duration,
}
impl Default for Tuning {
    fn default() -> Self {
        Self {
            metadata_timeout: Duration::from_secs(30),
            queue_wait: Duration::from_secs(300),
            endgame_after: Duration::from_secs(30),
        }
    }
}

/// Why a peer was given up on. These travel inside the error chain (`downcast_ref`), so callers can
/// tell "did not answer" from "does not have the file" without comparing text.
#[derive(Debug)]
pub struct HandshakeTimedOut;
impl std::fmt::Display for HandshakeTimedOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ED2K peer handshake timed out")
    }
}
#[derive(Debug)]
pub struct MetadataTimedOut;
impl std::fmt::Display for MetadataTimedOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ED2K peer metadata timed out")
    }
}
#[derive(Debug)]
pub struct QueueWaitTimedOut;
impl std::fmt::Display for QueueWaitTimedOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("peer queue wait timed out")
    }
}
#[derive(Debug)]
pub struct PeerLacksFile;
impl std::fmt::Display for PeerLacksFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("peer does not share the requested file")
    }
}
impl std::error::Error for HandshakeTimedOut {}
impl std::error::Error for MetadataTimedOut {}
impl std::error::Error for QueueWaitTimedOut {}
impl std::error::Error for PeerLacksFile {}

/// A hashset already authenticated against the file link, so a peer need not send one.
pub(crate) type KnownHashes = Option<Arc<Vec<[u8; 16]>>>;
pub(crate) async fn cancelled(cancel: &AtomicBool) {
    loop {
        if cancel.load(Ordering::Acquire) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
pub(crate) fn check_cancel(cancel: &AtomicBool) -> Result<()> {
    if cancel.load(Ordering::Acquire) {
        bail!("ED2K transfer cancelled");
    }
    Ok(())
}
pub(crate) async fn frame<R: AsyncRead + Unpin>(stream: &mut R) -> Result<(u8, u8, Vec<u8>)> {
    let mut protocol = stream.read_u8().await?;
    if !matches!(protocol, 0xe3 | 0xc5 | 0xd4) {
        bail!("unsupported ED2K peer protocol");
    }
    let len = stream.read_u32_le().await? as usize;
    if len == 0 || len > MAX_PACKET {
        bail!("ED2K peer packet exceeds size limit");
    }
    let opcode = stream.read_u8().await?;
    let mut bytes = vec![0; len - 1];
    stream.read_exact(&mut bytes).await?;
    if protocol == 0xd4 {
        let mut decoded = Vec::new();
        flate2::read::ZlibDecoder::new(&bytes[..])
            .take((MAX_PACKET + 1) as u64)
            .read_to_end(&mut decoded)?;
        if decoded.len() > MAX_PACKET {
            bail!("ED2K peer decompression exceeds limit");
        }
        bytes = decoded;
        protocol = 0xc5;
    }
    Ok((protocol, opcode, bytes))
}
pub(crate) async fn send<W: AsyncWrite + Unpin>(
    stream: &mut W,
    protocol: u8,
    opcode: u8,
    bytes: &[u8],
) -> Result<()> {
    if bytes.len() >= MAX_PACKET {
        bail!("ED2K peer packet exceeds size limit");
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        stream.write_u8(protocol).await?;
        stream.write_u32_le((bytes.len() + 1) as u32).await?;
        stream.write_u8(opcode).await?;
        stream.write_all(bytes).await
    })
    .await
    .context("ED2K peer write timed out")??;
    Ok(())
}
fn number(data: &mut Vec<u8>, id: u8, value: u32) {
    data.extend([3, 1, 0, id]);
    data.extend(value.to_le_bytes());
}
pub(crate) fn hello(hash: [u8; 16], client_id: u32) -> Vec<u8> {
    let mut data = vec![16];
    data.extend(hash);
    data.extend(client_id.to_le_bytes());
    data.extend(0u16.to_le_bytes());
    data.extend(4u32.to_le_bytes());
    let name = b"Apocalipse Nibiru";
    data.extend([2, 1, 0, 1]);
    data.extend((name.len() as u16).to_le_bytes());
    data.extend(name);
    number(&mut data, 0x11, 60);
    // Unicode + compression + no shared-file listing. Do not advertise Kad,
    // secure identification, obfuscation, source exchange or multipackets.
    number(&mut data, 0xfa, (1 << 28) | (1 << 20) | 4);
    number(&mut data, 0xfe, 1 << 4); // 64-bit file offsets.
    data.extend([0; 6]);
    data
}
/// What a peer told us about itself in its hello.
#[derive(Clone, Debug)]
pub(crate) struct HelloInfo {
    pub user_hash: [u8; 16],
    pub client_id: u32,
    pub name: String,
    pub large: bool,
}

/// Parses a hello body that starts at the user hash (after the hash-size byte).
pub(crate) fn parse_hello(data: &[u8]) -> Result<HelloInfo> {
    let mut reader = Reader::new(data);
    let user_hash: [u8; 16] = reader.take(16)?.try_into()?;
    let client_id = reader.u32()?;
    reader.take(2)?;
    let count = reader.u32()?;
    if count > 64 {
        bail!("too many peer hello tags");
    }
    let mut large = false;
    let mut name = String::new();
    for _ in 0..count {
        match tag(&mut reader)? {
            (0xfe, Value::Number(bits)) => large = bits & (1 << 4) != 0,
            (0x01, Value::Text(text)) => name = text.chars().take(64).collect(),
            _ => {}
        }
    }
    reader.take(6)?;
    if data.len() - reader.pos > 4 {
        bail!("invalid peer hello trailer");
    }
    Ok(HelloInfo {
        user_hash,
        client_id,
        name,
        large,
    })
}
pub(crate) fn hello_answer(data: &[u8]) -> Result<bool> {
    Ok(parse_hello(data)?.large)
}

type PartMetadata = (Vec<bool>, Vec<[u8; 16]>);
async fn metadata(
    stream: &mut PeerStream,
    file: &FileLink,
    known: &KnownHashes,
) -> Result<PartMetadata> {
    send(stream, 0xe3, 0x58, &file.hash).await?;
    if file.part_count() > 1 {
        send(stream, 0xe3, 0x4f, &file.hash).await?;
    }
    // A hashset we already trust (kept from an earlier run or sent by another peer) is not asked
    // for again: some peers answer the file name and status but never the hashset.
    if file.size >= PART_SIZE && known.is_none() {
        send(stream, 0xe3, 0x51, &file.hash).await?;
    }
    let mut named = false;
    let mut status = file.part_count() == 1;
    let mut parts = vec![true; file.part_count()];
    let mut hashes = (file.size < PART_SIZE)
        .then(|| vec![file.hash])
        .or_else(|| known.as_ref().map(|hashes| hashes.as_ref().clone()));
    while !named || !status || hashes.is_none() {
        let (protocol, op, data) = frame(stream).await?;
        if protocol == 0xc5 && op == 1 {
            send(stream, 0xc5, 2, &[60, 1, 0, 0, 0, 0]).await?;
            continue;
        }
        if protocol != 0xe3 {
            continue;
        }
        match op {
            0x48 => return Err(PeerLacksFile.into()),
            0x59 => {
                let mut r = Reader::new(&data);
                if r.take(16)? != file.hash {
                    bail!("peer filename response hash mismatch");
                }
                let len = r.u16()? as usize;
                r.take(len)?;
                if r.pos != data.len() {
                    bail!("invalid filename response length");
                }
                named = true;
            }
            0x50 => {
                let mut r = Reader::new(&data);
                if r.take(16)? != file.hash {
                    bail!("peer status response hash mismatch");
                }
                let count = r.u16()? as usize;
                if count != 0 && count != parts.len() {
                    bail!("peer part count does not match file");
                }
                let bits = r.take(count.div_ceil(8))?;
                if r.pos != data.len() {
                    bail!("invalid peer status length");
                }
                if count != 0 {
                    for (i, part) in parts.iter_mut().enumerate() {
                        *part = bits[i / 8] & (1 << (i % 8)) != 0;
                    }
                }
                status = true;
            }
            0x52 => {
                let mut r = Reader::new(&data);
                if r.take(16)? != file.hash {
                    bail!("peer hashset response hash mismatch");
                }
                let count = r.u16()? as usize;
                if data.len() != 18 + count * 16 {
                    bail!("invalid peer hashset length");
                }
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(r.take(16)?.try_into()?);
                }
                hashes = Some(file.validate_hashset(values)?);
            }
            _ => {}
        }
    }
    Ok((parts, hashes.context("missing peer hashset")?))
}

pub(crate) struct Peer {
    stream: PeerStream,
    file: FileLink,
    pub hashes: Vec<[u8; 16]>,
    parts: Vec<bool>,
    large: bool,
    cancel: Arc<AtomicBool>,
}
impl Peer {
    /// Connects, obfuscating when the mode and the source allow it. In `Prefer` mode a failed
    /// obfuscated handshake is retried once as a plain connection (old or misreporting peers).
    #[cfg(test)]
    pub async fn connect(
        source: &FileSource,
        file: &FileLink,
        identity: [u8; 16],
        client_id: u32,
        mode: Obfuscation,
        cancel: Arc<AtomicBool>,
    ) -> Result<Self> {
        Self::connect_tuned(
            source,
            file,
            identity,
            client_id,
            mode,
            None,
            Tuning::default(),
            cancel,
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn connect_tuned(
        source: &FileSource,
        file: &FileLink,
        identity: [u8; 16],
        client_id: u32,
        mode: Obfuscation,
        known: KnownHashes,
        tuning: Tuning,
        cancel: Arc<AtomicBool>,
    ) -> Result<Self> {
        let first = Self::connect_once(
            source,
            file,
            identity,
            client_id,
            mode,
            known.clone(),
            tuning,
            cancel.clone(),
        )
        .await;
        match first {
            Err(error)
                if mode == Obfuscation::Prefer
                    && source.can_obfuscate()
                    && source.crypt & 0x04 == 0
                    && format!("{error:#}").contains("obfuscated handshake failed") =>
            {
                Self::connect_once(
                    source,
                    file,
                    identity,
                    client_id,
                    Obfuscation::Disabled,
                    known,
                    tuning,
                    cancel,
                )
                .await
            }
            other => other,
        }
    }
    #[allow(clippy::too_many_arguments)]
    async fn connect_once(
        source: &FileSource,
        file: &FileLink,
        identity: [u8; 16],
        client_id: u32,
        mode: Obfuscation,
        known: KnownHashes,
        tuning: Tuning,
        cancel: Arc<AtomicBool>,
    ) -> Result<Self> {
        check_cancel(&cancel)?;
        let host = source.host.context("Low-ID peer needs a callback")?;
        if source.requires_callback || source.port == 0 {
            bail!("peer requires an incoming callback");
        }
        // Obfuscation needs the target's user hash as key material, so it is only possible
        // when a server or Kad told us the hash and that the peer supports it.
        let use_crypt = match mode {
            Obfuscation::Disabled => {
                if source.crypt & 0x04 != 0 {
                    bail!("peer requires obfuscation, which is disabled");
                }
                false
            }
            Obfuscation::Prefer => source.can_obfuscate() || source.crypt & 0x04 != 0,
            Obfuscation::Require => true,
        };
        if use_crypt && source.user_hash.is_none() {
            bail!("obfuscation needs the peer's user hash, which is unknown");
        }
        let open = async {
            let stream = TcpStream::connect((host, source.port)).await?;
            stream.set_nodelay(true)?;
            let mut stream = match source.user_hash.filter(|_| use_crypt) {
                Some(hash) => PeerStream::Crypt(Box::new(
                    obfuscation::initiate(stream, &hash)
                        .await
                        .context("obfuscated handshake failed")?,
                )),
                None => PeerStream::Plain(stream),
            };
            send(&mut stream, 0xe3, 1, &hello(identity, client_id)).await?;
            let large = loop {
                let (protocol, op, data) = frame(&mut stream).await?;
                match (protocol, op) {
                    (0xe3, 0x4c) => break hello_answer(&data)?,
                    (0xc5, 1) => send(&mut stream, 0xc5, 2, &[60, 1, 0, 0, 0, 0]).await?,
                    _ => {}
                }
            };
            Ok::<_, anyhow::Error>((stream, large))
        };
        let (stream, large) = tokio::select! {
            result=tokio::time::timeout(Duration::from_secs(30),open)=>result.context(HandshakeTimedOut)??,
            _=cancelled(&cancel)=>bail!("ED2K transfer cancelled"),
        };
        Self::from_incoming_tuned(stream, file, large, known, tuning, cancel).await
    }
    pub(crate) async fn from_incoming_tuned(
        mut stream: PeerStream,
        file: &FileLink,
        large: bool,
        known: KnownHashes,
        tuning: Tuning,
        cancel: Arc<AtomicBool>,
    ) -> Result<Self> {
        let (parts, hashes) = tokio::select! {
            result=tokio::time::timeout(tuning.metadata_timeout,metadata(&mut stream,file,&known))=>result.context(MetadataTimedOut)??,
            _=cancelled(&cancel)=>bail!("ED2K transfer cancelled"),
        };
        Ok(Self {
            stream,
            file: file.clone(),
            large,
            parts,
            hashes,
            cancel,
        })
    }
    async fn receive(&mut self) -> Result<(u8, u8, Vec<u8>)> {
        tokio::select! {
            result=tokio::time::timeout(Duration::from_secs(30),frame(&mut self.stream))=>result.context("ED2K peer read timed out")?,
            _=cancelled(&self.cancel)=>bail!("ED2K transfer cancelled"),
        }
    }
    pub fn has_part(&self, index: usize) -> bool {
        self.parts.get(index).copied().unwrap_or(false)
    }
    #[cfg(test)]
    pub async fn acquire_slot(&mut self, queued: impl FnMut(u32)) -> Result<()> {
        self.acquire_slot_for(Tuning::default().queue_wait, queued)
            .await
    }
    pub async fn acquire_slot_for(
        &mut self,
        queue_wait: Duration,
        mut queued: impl FnMut(u32),
    ) -> Result<()> {
        send(&mut self.stream, 0xe3, 0x54, &self.file.hash).await?;
        // A peer that has no free slot may stay silent while we wait in its queue (aMule makes a new
        // client wait before the first slot), so the usual 30-second read limit does not apply here.
        let deadline = tokio::time::Instant::now() + queue_wait;
        loop {
            let (protocol, op, data) = tokio::select! {
                result = tokio::time::timeout_at(deadline, frame(&mut self.stream)) =>
                    result.context(QueueWaitTimedOut)??,
                _ = cancelled(&self.cancel) => bail!("ED2K transfer cancelled"),
            };
            match (protocol, op) {
                (0xe3, 0x55) => return Ok(()),
                (0xe3, 0x48 | 0x57) => bail!("peer upload slot unavailable"),
                (0xe3, 0x5c) => {
                    let mut r = Reader::new(&data);
                    queued(r.u32()?);
                }
                (0xc5, 0x60) => {
                    let mut r = Reader::new(&data);
                    queued(r.u16()? as u32);
                }
                _ => {}
            }
        }
    }
    /// One block of the file, `[from, end)` in absolute offsets.
    #[cfg(test)]
    pub async fn block(&mut self, from: u64, end: u64) -> Result<Vec<u8>> {
        let mut blocks = self.blocks(&[(from, end)]).await?;
        Ok(blocks.remove(0))
    }

    /// Up to three blocks asked for in one request, like eMule and aMule do (the legacy request
    /// carries exactly three start/end pairs; unused pairs are zero). Each range is
    /// `[from, end)` in absolute offsets and at most one block long. The answers may interleave,
    /// compressed or not; they come back in the order of `ranges`.
    pub async fn blocks(&mut self, ranges: &[(u64, u64)]) -> Result<Vec<Vec<u8>>> {
        check_cancel(&self.cancel)?;
        if ranges.is_empty() || ranges.len() > 3 {
            bail!("a request carries one to three blocks");
        }
        let wide = ranges.iter().any(|(_, end)| *end > u32::MAX as u64);
        if wide && !self.large {
            bail!("peer does not support 64-bit file offsets");
        }
        let mut request = self.file.hash.to_vec();
        let starts = (0..3).map(|i| ranges.get(i).map_or(0, |r| r.0));
        let ends = (0..3).map(|i| ranges.get(i).map_or(0, |r| r.1));
        for offset in starts.chain(ends) {
            if wide {
                request.extend(offset.to_le_bytes());
            } else {
                request.extend((offset as u32).to_le_bytes());
            }
        }
        send(
            &mut self.stream,
            if wide { 0xc5 } else { 0xe3 },
            if wide { 0xa3 } else { 0x47 },
            &request,
        )
        .await?;
        struct Slot {
            from: u64,
            end: u64,
            data: Vec<u8>,
            covered: Vec<bool>,
            remaining: usize,
            compressed: Vec<u8>,
            compressed_size: Option<usize>,
        }
        let mut slots: Vec<Slot> = ranges
            .iter()
            .map(|&(from, end)| Slot {
                from,
                end,
                data: vec![0; (end - from) as usize],
                covered: vec![false; (end - from) as usize],
                remaining: (end - from) as usize,
                compressed: Vec::new(),
                compressed_size: None,
            })
            .collect();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(45 * ranges.len() as u64);
        while slots.iter().any(|slot| slot.remaining > 0) {
            let (protocol, op, data) = tokio::time::timeout_at(deadline, self.receive())
                .await
                .context("peer block timed out")??;
            match (protocol, op) {
                (0xe3, 0x48 | 0x57) => bail!("peer stopped the transfer"),
                (0xe3, 0x46) | (0xc5, 0xa2) => {
                    let mut r = Reader::new(&data);
                    if r.take(16)? != self.file.hash {
                        bail!("peer block hash mismatch");
                    }
                    let (a, b) = if op == 0xa2 {
                        (r.u64()?, r.u64()?)
                    } else {
                        (r.u32()? as u64, r.u32()? as u64)
                    };
                    let slot = slots
                        .iter_mut()
                        .find(|slot| a >= slot.from && b <= slot.end)
                        .filter(|slot| b > a && slot.compressed.is_empty());
                    let Some(slot) = slot else {
                        bail!("peer block exceeds requested range");
                    };
                    if data.len() - r.pos != (b - a) as usize {
                        bail!("peer block exceeds requested range");
                    }
                    let range = (a - slot.from) as usize..(b - slot.from) as usize;
                    if slot.covered[range.clone()].iter().any(|b| *b) {
                        bail!("peer sent overlapping data");
                    }
                    slot.remaining -= range.len();
                    slot.data[range.clone()].copy_from_slice(&data[r.pos..]);
                    slot.covered[range].fill(true);
                }
                (0xc5, 0x40 | 0xa1) => {
                    let mut r = Reader::new(&data);
                    if r.take(16)? != self.file.hash {
                        bail!("compressed peer block hash mismatch");
                    }
                    let offset = if op == 0xa1 {
                        r.u64()?
                    } else {
                        r.u32()? as u64
                    };
                    let total = r.u32()? as usize;
                    let Some(slot) = slots.iter_mut().find(|slot| slot.from == offset) else {
                        bail!("invalid compressed peer block");
                    };
                    if total == 0
                        || total > slot.data.len() + 65536
                        || slot.compressed_size.is_some_and(|n| n != total)
                        || slot.remaining != slot.data.len()
                    {
                        bail!("invalid compressed peer block");
                    }
                    slot.compressed_size = Some(total);
                    if slot.compressed.len() + data.len() - r.pos > total {
                        bail!("compressed peer block exceeds declared length");
                    }
                    slot.compressed.extend(&data[r.pos..]);
                    if slot.compressed.len() == total {
                        let mut decoded = Vec::new();
                        flate2::read::ZlibDecoder::new(&slot.compressed[..])
                            .take(slot.data.len() as u64 + 1)
                            .read_to_end(&mut decoded)?;
                        if decoded.len() != slot.data.len() {
                            bail!("compressed peer block size mismatch");
                        }
                        slot.data = decoded;
                        slot.remaining = 0;
                    }
                }
                _ => {}
            }
        }
        Ok(slots.into_iter().map(|slot| slot.data).collect())
    }

    /// The whole part, verified (the transfer itself assembles parts from blocks of several peers).
    #[cfg(test)]
    pub async fn part(&mut self, index: usize) -> Result<Vec<u8>> {
        check_cancel(&self.cancel)?;
        if !self.has_part(index) {
            bail!("peer does not have this part");
        }
        let start = index as u64 * PART_SIZE;
        let mut part = Vec::with_capacity(self.file.part_len(index));
        while part.len() < self.file.part_len(index) {
            let from = start + part.len() as u64;
            let end =
                (from + super::file::BLOCK_SIZE).min(start + self.file.part_len(index) as u64);
            part.extend(self.block(from, end).await?);
        }
        if super::file::digest(&part) != self.hashes[index] {
            bail!("ED2K part failed MD4 verification");
        }
        Ok(part)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ed2k::file::digest;
    #[tokio::test]
    async fn large_file_peer_uses_64_bit_requests_and_responses() {
        let index = 442usize;
        let offset = index as u64 * PART_SIZE;
        assert!(offset > u32::MAX as u64);
        let bytes = b"abc";
        let mut hashes = vec![[1; 16]; index];
        hashes.push(digest(bytes));
        let file = FileLink {
            name: "large.bin".into(),
            size: offset + 3,
            hash: digest(&hashes.concat()),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let served = file.clone();
        let fixture = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            assert_eq!(frame(&mut stream).await.unwrap().1, 1);
            let answer = hello([7; 16], 123);
            send(&mut stream, 0xe3, 0x4c, &answer[1..]).await.unwrap();
            for _ in 0..3 {
                let (_, op, _) = frame(&mut stream).await.unwrap();
                let mut answer = served.hash.to_vec();
                let reply = match op {
                    0x58 => {
                        answer.extend(1u16.to_le_bytes());
                        answer.push(b'x');
                        0x59
                    }
                    0x4f => {
                        answer.extend((served.part_count() as u16).to_le_bytes());
                        answer.extend(vec![255; served.part_count().div_ceil(8)]);
                        0x50
                    }
                    0x51 => {
                        answer.extend((hashes.len() as u16).to_le_bytes());
                        answer.extend(hashes.concat());
                        0x52
                    }
                    _ => panic!("unexpected peer request"),
                };
                send(&mut stream, 0xe3, reply, &answer).await.unwrap();
            }
            assert_eq!(frame(&mut stream).await.unwrap().1, 0x54);
            send(&mut stream, 0xe3, 0x55, &[]).await.unwrap();
            let (protocol, op, request) = frame(&mut stream).await.unwrap();
            assert_eq!((protocol, op), (0xc5, 0xa3));
            assert_eq!(request.len(), 64);
            assert_eq!(
                u64::from_le_bytes(request[16..24].try_into().unwrap()),
                offset
            );
            assert_eq!(
                u64::from_le_bytes(request[40..48].try_into().unwrap()),
                offset + 3
            );
            let mut answer = served.hash.to_vec();
            answer.extend(offset.to_le_bytes());
            answer.extend((offset + 3).to_le_bytes());
            answer.extend(bytes);
            send(&mut stream, 0xc5, 0xa2, &answer).await.unwrap();
        });
        let source = FileSource {
            client_id: 0x0100007f,
            host: Some(std::net::Ipv4Addr::LOCALHOST),
            port,
            requires_callback: false,
            user_hash: None,
            crypt: 0,
            buddy: None,
        };
        let mut peer = Peer::connect(
            &source,
            &file,
            [2; 16],
            123,
            Obfuscation::Disabled,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();
        peer.acquire_slot(|_| {}).await.unwrap();
        assert_eq!(peer.part(index).await.unwrap(), bytes);
        fixture.await.unwrap();
    }
}
