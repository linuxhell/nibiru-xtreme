//! Message Stream Encryption / Protocol Encryption wire negotiation.
//!
//! MSE is legacy BitTorrent obfuscation, not authenticated encryption. Torrent
//! piece hashes remain mandatory. Padding, initial data and negotiation time are
//! bounded by the caller; no unverified payload bypasses piece verification.

use super::message::{Handshake, read_handshake};
use anyhow::{Context, Result, bail};
use num_bigint::BigUint;
use sha1::{Digest, Sha1};
use std::{
    io,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

const PRIME: &[u8] = b"FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A63A36210000000000090563";
const DH_BYTES: usize = 96;
const MAX_PAD: usize = 512;
const MAX_INITIAL: usize = 68;

pub trait PeerIo: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> PeerIo for T {}
pub type Stream = Box<dyn PeerIo>;

#[derive(Clone)]
struct Rc4 {
    s: [u8; 256],
    i: u8,
    j: u8,
}

impl Rc4 {
    fn new(key: &[u8]) -> Self {
        let mut s = [0; 256];
        for (i, value) in s.iter_mut().enumerate() {
            *value = i as u8;
        }
        let mut j = 0u8;
        for i in 0..256 {
            j = j.wrapping_add(s[i]).wrapping_add(key[i % key.len()]);
            s.swap(i, j as usize);
        }
        Self { s, i: 0, j: 0 }
    }

    fn apply(&mut self, data: &mut [u8]) {
        for b in data {
            self.i = self.i.wrapping_add(1);
            self.j = self.j.wrapping_add(self.s[self.i as usize]);
            self.s.swap(self.i as usize, self.j as usize);
            *b ^= self.s[self.s[self.i as usize].wrapping_add(self.s[self.j as usize]) as usize];
        }
    }

    fn key(label: &[u8], shared: &[u8], hash: &[u8; 20]) -> Self {
        let mut c = Self::new(&digest(&[label, shared, hash]));
        c.apply(&mut [0; 1024]);
        c
    }
}

fn digest(parts: &[&[u8]]) -> [u8; 20] {
    let mut hash = Sha1::new();
    for part in parts {
        hash.update(part);
    }
    hash.finalize().into()
}

fn fixed(value: BigUint) -> Result<[u8; DH_BYTES]> {
    let bytes = value.to_bytes_be();
    if bytes.len() > DH_BYTES {
        bail!("MSE integer too large");
    }
    let mut out = [0; DH_BYTES];
    out[DH_BYTES - bytes.len()..].copy_from_slice(&bytes);
    Ok(out)
}

fn dh() -> Result<(BigUint, [u8; DH_BYTES])> {
    let mut random = [0; 32];
    getrandom::fill(&mut random).map_err(|e| anyhow::anyhow!("MSE random source: {e}"))?;
    random[0] |= 0x80;
    let secret = BigUint::from_bytes_be(&random);
    let p = BigUint::parse_bytes(PRIME, 16).context("MSE modulus")?;
    let public = fixed(BigUint::from(2u8).modpow(&secret, &p))?;
    Ok((secret, public))
}

fn shared_key(secret: &BigUint, peer: &[u8; DH_BYTES]) -> Result<[u8; DH_BYTES]> {
    let p = BigUint::parse_bytes(PRIME, 16).context("MSE modulus")?;
    let y = BigUint::from_bytes_be(peer);
    if y <= BigUint::from(1u8) || y >= &p - BigUint::from(1u8) {
        bail!("invalid MSE public key");
    }
    fixed(y.modpow(secret, &p))
}

async fn sync_marker<S: AsyncRead + Unpin>(io: &mut S, marker: &[u8]) -> Result<()> {
    let mut window = vec![0; marker.len()];
    io.read_exact(&mut window).await?;
    for skipped in 0..=MAX_PAD {
        if window == marker {
            return Ok(());
        }
        if skipped == MAX_PAD {
            break;
        }
        window.rotate_left(1);
        *window.last_mut().expect("nonempty marker") = io.read_u8().await?;
    }
    bail!("MSE padding or synchronization marker is invalid")
}

async fn encrypted_read<S: AsyncRead + Unpin>(
    io: &mut S,
    cipher: &mut Rc4,
    size: usize,
) -> Result<Vec<u8>> {
    let mut data = vec![0; size];
    io.read_exact(&mut data).await?;
    cipher.apply(&mut data);
    Ok(data)
}

async fn padding<S: AsyncRead + Unpin>(io: &mut S, cipher: &mut Rc4, length: &[u8]) -> Result<()> {
    let length = u16::from_be_bytes(length.try_into()?) as usize;
    if length > MAX_PAD {
        bail!("MSE padding exceeds limit");
    }
    encrypted_read(io, cipher, length).await?;
    Ok(())
}

/// Negotiate MSE as initiator, preferring RC4 while accepting header-only mode.
pub async fn outgoing<S: PeerIo + 'static>(
    mut io: S,
    hash: [u8; 20],
    require_rc4: bool,
) -> Result<Stream> {
    let (secret, public) = dh()?;
    io.write_all(&public).await?;
    io.flush().await?;
    let mut peer = [0; DH_BYTES];
    io.read_exact(&mut peer).await?;
    let shared = shared_key(&secret, &peer)?;
    let req1 = digest(&[b"req1", &shared]);
    let req2 = digest(&[b"req2", &hash]);
    let req3 = digest(&[b"req3", &shared]);
    let xor: Vec<_> = req2.iter().zip(req3).map(|(a, b)| a ^ b).collect();
    let mut write = Rc4::key(b"keyA", &shared, &hash);
    let mut read = Rc4::key(b"keyB", &shared, &hash);
    let mut offer = vec![0; 16]; // VC, crypto_provide, len(PadC), len(IA)
    offer[11] = if require_rc4 { 2 } else { 3 };
    write.apply(&mut offer);
    io.write_all(&req1).await?;
    io.write_all(&xor).await?;
    io.write_all(&offer).await?;
    io.flush().await?;
    let mut marker = [0; 8];
    read.apply(&mut marker);
    sync_marker(&mut io, &marker).await?;
    let reply = encrypted_read(&mut io, &mut read, 6).await?;
    let method = u32::from_be_bytes(reply[..4].try_into()?);
    if method != 1 && method != 2 {
        bail!("MSE peer selected unsupported crypto");
    }
    if require_rc4 && method != 2 {
        bail!("peer refused required RC4 encryption");
    }
    padding(&mut io, &mut read, &reply[4..]).await?;
    Ok(Box::new(CipherStream {
        io,
        read: (method == 2).then_some(read),
        write: (method == 2).then_some(write),
        prefix: vec![],
        prefix_at: 0,
        write_cache: None,
    }))
}

/// Accept either a plain handshake or an MSE handshake for an explicitly registered torrent.
pub async fn incoming<S: PeerIo + 'static>(
    mut io: S,
    hashes: &[[u8; 20]],
) -> Result<(Stream, Handshake, bool)> {
    let mut first = [0; 20];
    io.read_exact(&mut first).await?;
    if first[0] == 19 && &first[1..] == b"BitTorrent protocol" {
        let mut bytes = [0; 68];
        bytes[..20].copy_from_slice(&first);
        io.read_exact(&mut bytes[20..]).await?;
        return Ok((Box::new(io), Handshake::decode(&bytes)?, false));
    }
    if hashes.is_empty() {
        bail!("no registered torrents for encrypted peer");
    }
    let mut peer = [0; DH_BYTES];
    peer[..20].copy_from_slice(&first);
    io.read_exact(&mut peer[20..]).await?;
    let (secret, public) = dh()?;
    let shared = shared_key(&secret, &peer)?;
    io.write_all(&public).await?;
    io.flush().await?;
    sync_marker(&mut io, &digest(&[b"req1", &shared])).await?;
    let mut offered = [0; 20];
    io.read_exact(&mut offered).await?;
    let req3 = digest(&[b"req3", &shared]);
    for (b, mask) in offered.iter_mut().zip(req3) {
        *b ^= mask;
    }
    let hash = hashes
        .iter()
        .find(|hash| digest(&[b"req2", &hash[..]]) == offered)
        .context("encrypted peer requested an unknown torrent")?;
    let mut read = Rc4::key(b"keyA", &shared, hash);
    let mut write = Rc4::key(b"keyB", &shared, hash);
    let offer = encrypted_read(&mut io, &mut read, 14).await?;
    if offer[..8] != [0; 8] {
        bail!("invalid MSE verification constant");
    }
    let methods = u32::from_be_bytes(offer[8..12].try_into()?);
    let method = if methods & 2 != 0 {
        2
    } else if methods & 1 != 0 {
        1
    } else {
        bail!("no shared MSE cipher");
    };
    padding(&mut io, &mut read, &offer[12..]).await?;
    let length = encrypted_read(&mut io, &mut read, 2).await?;
    let initial_len = u16::from_be_bytes(length[..].try_into()?) as usize;
    if initial_len > MAX_INITIAL {
        bail!("MSE initial payload exceeds handshake size");
    }
    let initial = encrypted_read(&mut io, &mut read, initial_len).await?;
    let mut reply = vec![0; 14];
    reply[11] = method;
    write.apply(&mut reply);
    io.write_all(&reply).await?;
    io.flush().await?;
    let mut stream: Stream = Box::new(CipherStream {
        io,
        read: (method == 2).then_some(read),
        write: (method == 2).then_some(write),
        prefix: initial,
        prefix_at: 0,
        write_cache: None,
    });
    let handshake = read_handshake(&mut stream).await?;
    if &handshake.info_hash != hash {
        bail!("MSE info hash differs from peer handshake");
    }
    Ok((stream, handshake, method == 2))
}

struct CipherStream<S> {
    io: S,
    read: Option<Rc4>,
    write: Option<Rc4>,
    prefix: Vec<u8>,
    prefix_at: usize,
    /// Cached encryption only, not accepted bytes. Revalidated against the
    /// caller's slice after Pending/cancellation, so another write is safe.
    write_cache: Option<(Vec<u8>, Vec<u8>)>,
}

impl<S: AsyncRead + Unpin> AsyncRead for CipherStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.prefix_at < this.prefix.len() {
            let n = buf.remaining().min(this.prefix.len() - this.prefix_at);
            buf.put_slice(&this.prefix[this.prefix_at..this.prefix_at + n]);
            this.prefix_at += n;
            return Poll::Ready(Ok(()));
        }
        let before = buf.filled().len();
        match Pin::new(&mut this.io).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                if let Some(cipher) = &mut this.read {
                    cipher.apply(&mut buf.filled_mut()[before..]);
                }
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for CipherStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let Some(cipher) = &mut this.write else {
            return Pin::new(&mut this.io).poll_write(cx, buf);
        };
        let matches = this
            .write_cache
            .as_ref()
            .is_some_and(|(plain, _)| buf.starts_with(plain));
        if !matches {
            let plain = buf[..buf.len().min(16 * 1024)].to_vec();
            let mut encrypted = plain.clone();
            cipher.clone().apply(&mut encrypted);
            this.write_cache = Some((plain, encrypted));
        }
        let (plain, encrypted) = this.write_cache.as_mut().expect("write cache initialized");
        match Pin::new(&mut this.io).poll_write(cx, encrypted) {
            Poll::Ready(Ok(n)) => {
                // Commit only bytes accepted by the underlying transport; Pending
                // and partial writes must not advance the keystream prematurely.
                cipher.apply(&mut encrypted[..n]);
                plain.drain(..n);
                encrypted.drain(..n);
                if plain.is_empty() {
                    this.write_cache = None;
                }
                Poll::Ready(Ok(n))
            }
            result => result,
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rc4_known_vector_and_invalid_dh_keys() {
        let mut text = b"Plaintext".to_vec();
        Rc4::new(b"Key").apply(&mut text);
        assert_eq!(text, [0xbb, 0xf3, 0x16, 0xe8, 0xd9, 0x40, 0xaf, 0x0a, 0xd3]);
        assert!(shared_key(&BigUint::from(7u8), &[0; 96]).is_err());
    }

    #[tokio::test]
    async fn encrypted_handshake_and_fragmented_bidirectional_payload() {
        // A tiny duplex capacity forces partial writes and Pending with RC4 active.
        let (a, b) = tokio::io::duplex(23);
        let hash = [4; 20];
        let client = async {
            let mut io = outgoing(a, hash, true).await.unwrap();
            io.write_all(&Handshake::new(hash, [1; 20], false).encode())
                .await
                .unwrap();
            assert_eq!(read_handshake(&mut io).await.unwrap().peer_id, [2; 20]);
            let data = vec![17; 100_000];
            io.write_all(&data).await.unwrap();
            let mut echoed = vec![0; data.len()];
            io.read_exact(&mut echoed).await.unwrap();
            assert_eq!(echoed, data);
        };
        let server = async {
            let (mut io, hello, encrypted) = incoming(b, &[hash]).await.unwrap();
            assert!(encrypted);
            assert_eq!(hello.peer_id, [1; 20]);
            io.write_all(&Handshake::new(hash, [2; 20], false).encode())
                .await
                .unwrap();
            let mut data = vec![0; 100_000];
            io.read_exact(&mut data).await.unwrap();
            assert_eq!(data, vec![17; 100_000]);
            io.write_all(&data).await.unwrap();
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::join!(client, server);
        })
        .await
        .unwrap();
    }
}
