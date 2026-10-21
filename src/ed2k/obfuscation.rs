//! ED2K client↔client protocol obfuscation (the "basic obfuscated handshake").
//!
//! RC4 streams keyed by `MD5(<user hash of the connection's target> <magic byte> <4 random bytes>)`,
//! the first 1024 keystream bytes discarded. Written from the documented wire format and checked
//! against aMule 2.3.3 (tests/interop_amule.py) and an independent Python implementation.
//!
//! Initiator A → responder B (B's user hash is the key material):
//! ```text
//! A: <marker 1 plain><random key part 4 plain> | <magic 4><methods 1><preferred 1><pad len 1><pad>
//! B:                                            | <magic 4><selected 1><pad len 1><pad>
//! ```
//! The part after `|` is RC4 encrypted (A→B with `MD5(hashB|34|rand)`, B→A with `MD5(hashB|203|rand)`)
//! and so is everything that follows on the connection. The marker is any byte that is not an ED2K
//! protocol byte, which is how a listener tells an obfuscated peer from a plain one.

use anyhow::{Result, bail, ensure};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::TcpStream,
};

const MAGIC_SYNC: u32 = 0x835E_6FC4;
const KEY_INITIATOR: u8 = 34;
const KEY_RESPONDER: u8 = 203;
const METHOD_OBFUSCATION: u8 = 0;
const MAX_PADDING: u8 = 128;

#[derive(Clone)]
pub struct Rc4 {
    s: [u8; 256],
    i: u8,
    j: u8,
}
impl Rc4 {
    pub fn new(key: &[u8]) -> Self {
        let mut s = [0u8; 256];
        for (i, v) in s.iter_mut().enumerate() {
            *v = i as u8;
        }
        let mut j = 0u8;
        for i in 0..256 {
            j = j.wrapping_add(s[i]).wrapping_add(key[i % key.len()]);
            s.swap(i, j as usize);
        }
        Self { s, i: 0, j: 0 }
    }
    pub fn apply(&mut self, data: &mut [u8]) {
        for b in data {
            self.i = self.i.wrapping_add(1);
            self.j = self.j.wrapping_add(self.s[self.i as usize]);
            self.s.swap(self.i as usize, self.j as usize);
            let k =
                self.s[(self.s[self.i as usize].wrapping_add(self.s[self.j as usize])) as usize];
            *b ^= k;
        }
    }
    /// ED2K keys discard the first 1024 bytes of keystream.
    fn keyed(material: &[u8; 21]) -> Self {
        let mut rc4 = Self::new(&md5::compute(material).0);
        rc4.apply(&mut [0u8; 1024]);
        rc4
    }
}

/// `(send, receive)` ciphers for one end of a connection.
pub fn ciphers(target_hash: &[u8; 16], random: [u8; 4], initiator: bool) -> (Rc4, Rc4) {
    let mut material = [0u8; 21];
    material[..16].copy_from_slice(target_hash);
    material[17..].copy_from_slice(&random);
    material[16] = KEY_INITIATOR;
    let requester_to_responder = Rc4::keyed(&material);
    material[16] = KEY_RESPONDER;
    let responder_to_requester = Rc4::keyed(&material);
    if initiator {
        (requester_to_responder, responder_to_requester)
    } else {
        (responder_to_requester, requester_to_responder)
    }
}

pub fn is_protocol_byte(byte: u8) -> bool {
    matches!(byte, 0xe3 | 0xc5 | 0xd4)
}

fn random_bytes<const N: usize>() -> Result<[u8; N]> {
    let mut out = [0u8; N];
    getrandom::fill(&mut out).map_err(|e| anyhow::anyhow!("no randomness: {e}"))?;
    Ok(out)
}
fn marker() -> Result<u8> {
    loop {
        let [byte] = random_bytes::<1>()?;
        if !is_protocol_byte(byte) {
            return Ok(byte);
        }
    }
}

/// What to do about protocol obfuscation, for outgoing and incoming connections alike.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Obfuscation {
    /// Plain connections only; obfuscated peers are refused or skipped.
    Disabled,
    /// Obfuscate when the peer is known to support it (and we know its user hash), accept both.
    #[default]
    Prefer,
    /// Never use or accept a plain connection.
    Require,
}
impl std::str::FromStr for Obfuscation {
    type Err = anyhow::Error;
    fn from_str(value: &str) -> Result<Self> {
        match value {
            "off" | "disabled" | "false" => Ok(Self::Disabled),
            "prefer" | "on" | "true" => Ok(Self::Prefer),
            "require" | "required" => Ok(Self::Require),
            other => bail!("unknown obfuscation mode {other:?} (use off, prefer or require)"),
        }
    }
}
impl Obfuscation {
    pub fn code(self) -> u8 {
        self as u8
    }
    pub fn from_code(code: u8) -> Self {
        match code {
            0 => Self::Disabled,
            2 => Self::Require,
            _ => Self::Prefer,
        }
    }
}

/// A peer connection that is either plain or obfuscated.
pub enum PeerStream {
    Plain(TcpStream),
    Crypt(Box<CryptStream<TcpStream>>),
}
impl PeerStream {
    pub fn is_obfuscated(&self) -> bool {
        matches!(self, Self::Crypt(_))
    }
}
impl AsyncRead for PeerStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Crypt(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}
impl AsyncWrite for PeerStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, data),
            Self::Crypt(s) => Pin::new(s.as_mut()).poll_write(cx, data),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Crypt(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Crypt(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Classifies an incoming connection by its first byte and finishes the handshake if needed.
pub async fn accept(
    stream: TcpStream,
    own_hash: &[u8; 16],
    mode: Obfuscation,
) -> Result<PeerStream> {
    let mut first = [0u8; 1];
    // Peek, so a plain connection's first byte stays in the stream for the frame reader.
    let read = stream.peek(&mut first).await?;
    ensure!(read == 1, "peer closed before sending anything");
    if is_protocol_byte(first[0]) {
        ensure!(
            mode != Obfuscation::Require,
            "plain connection refused (obfuscation is required)"
        );
        return Ok(PeerStream::Plain(stream));
    }
    ensure!(
        mode != Obfuscation::Disabled,
        "obfuscated connection refused (obfuscation is disabled)"
    );
    let mut stream = stream;
    let marker = stream.read_u8().await?;
    Ok(PeerStream::Crypt(Box::new(
        respond(stream, marker, own_hash).await?,
    )))
}

/// A byte stream whose reads are decrypted and whose writes are encrypted.
pub struct CryptStream<S> {
    inner: S,
    send: Rc4,
    receive: Rc4,
}
impl<S> CryptStream<S> {
    fn new(inner: S, send: Rc4, receive: Rc4) -> Self {
        Self {
            inner,
            send,
            receive,
        }
    }
    pub fn get_ref(&self) -> &S {
        &self.inner
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for CryptStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let this = &mut *self;
                this.receive.apply(&mut buf.filled_mut()[before..]);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for CryptStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = &mut *self;
        // Encrypt speculatively with a clone. The real RC4 state advances only for bytes
        // that the underlying writer actually accepts, preserving AsyncWrite retry semantics.
        let mut speculative = this.send.clone();
        let mut sealed = data.to_vec();
        speculative.apply(&mut sealed);
        match Pin::new(&mut this.inner).poll_write(cx, &sealed) {
            Poll::Ready(Ok(0)) if !data.is_empty() => {
                Poll::Ready(Err(io::ErrorKind::WriteZero.into()))
            }
            Poll::Ready(Ok(n)) => {
                debug_assert!(n <= data.len());
                let mut accepted = data[..n].to_vec();
                this.send.apply(&mut accepted);
                Poll::Ready(Ok(n))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Opens an obfuscated connection to the client whose user hash is `target_hash`.
pub async fn initiate<S>(mut stream: S, target_hash: &[u8; 16]) -> Result<CryptStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let random = random_bytes::<4>()?;
    let (mut send, mut receive) = ciphers(target_hash, random, true);
    let padding = (random_bytes::<1>()?[0] % (MAX_PADDING + 1)) as usize;
    let mut sealed = Vec::with_capacity(10 + padding);
    sealed.extend(MAGIC_SYNC.to_le_bytes());
    sealed.extend([METHOD_OBFUSCATION, METHOD_OBFUSCATION, padding as u8]);
    let mut filler = vec![0u8; padding];
    getrandom::fill(&mut filler).map_err(|e| anyhow::anyhow!("no randomness: {e}"))?;
    sealed.extend(filler);
    send.apply(&mut sealed);
    let mut request = vec![marker()?];
    request.extend(random);
    request.extend(sealed);
    stream.write_all(&request).await?;

    let mut head = [0u8; 6];
    stream.read_exact(&mut head).await?;
    receive.apply(&mut head);
    ensure!(
        u32::from_le_bytes(head[..4].try_into()?) == MAGIC_SYNC,
        "obfuscation handshake rejected: wrong magic (does the peer use another user hash?)"
    );
    ensure!(
        head[4] == METHOD_OBFUSCATION,
        "peer selected an unsupported obfuscation method"
    );
    let mut skip = vec![0u8; head[5] as usize];
    stream.read_exact(&mut skip).await?;
    receive.apply(&mut skip);
    Ok(CryptStream::new(stream, send, receive))
}

/// Answers an obfuscated connection whose first byte (`marker`) was already read.
pub async fn respond<S>(mut stream: S, marker: u8, own_hash: &[u8; 16]) -> Result<CryptStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    ensure!(!is_protocol_byte(marker), "not an obfuscated connection");
    let mut random = [0u8; 4];
    stream.read_exact(&mut random).await?;
    let (mut send, mut receive) = ciphers(own_hash, random, false);
    let mut head = [0u8; 7];
    stream.read_exact(&mut head).await?;
    receive.apply(&mut head);
    ensure!(
        u32::from_le_bytes(head[..4].try_into()?) == MAGIC_SYNC,
        "obfuscation handshake rejected: wrong magic"
    );
    // head[4] methods supported, head[5] preferred: only basic obfuscation exists.
    if head[4] & (1 << METHOD_OBFUSCATION) == 0 && head[4] != METHOD_OBFUSCATION {
        bail!("peer offers no supported obfuscation method");
    }
    let mut skip = vec![0u8; head[6] as usize];
    stream.read_exact(&mut skip).await?;
    receive.apply(&mut skip);

    let padding = (random_bytes::<1>()?[0] % 16) as usize;
    let mut reply = Vec::with_capacity(6 + padding);
    reply.extend(MAGIC_SYNC.to_le_bytes());
    reply.extend([METHOD_OBFUSCATION, padding as u8]);
    let mut filler = vec![0u8; padding];
    getrandom::fill(&mut filler).map_err(|e| anyhow::anyhow!("no randomness: {e}"))?;
    reply.extend(filler);
    send.apply(&mut reply);
    stream.write_all(&reply).await?;
    Ok(CryptStream::new(stream, send, receive))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rc4_matches_the_published_test_vectors() {
        // RFC 6229-style known answers for the classic vectors.
        let mut data = *b"Plaintext";
        Rc4::new(b"Key").apply(&mut data);
        assert_eq!(data, [0xBB, 0xF3, 0x16, 0xE8, 0xD9, 0x40, 0xAF, 0x0A, 0xD3]);
        let mut data = *b"pedia";
        Rc4::new(b"Wiki").apply(&mut data);
        assert_eq!(data, [0x10, 0x21, 0xBF, 0x04, 0x20]);
    }

    #[test]
    fn keys_follow_the_documented_derivation_and_swap_between_the_ends() {
        let hash = *b"0123456789abcdef";
        let (mut a_send, mut a_recv) = ciphers(&hash, [1, 2, 3, 4], true);
        let (mut b_send, mut b_recv) = ciphers(&hash, [1, 2, 3, 4], false);
        let mut forward = *b"hello B";
        a_send.apply(&mut forward);
        b_recv.apply(&mut forward);
        assert_eq!(&forward, b"hello B");
        let mut backward = *b"hello A";
        b_send.apply(&mut backward);
        a_recv.apply(&mut backward);
        assert_eq!(&backward, b"hello A");
        // The two directions use different keys.
        let (mut x, _) = ciphers(&hash, [1, 2, 3, 4], true);
        let (mut y, _) = ciphers(&hash, [1, 2, 3, 4], false);
        let (mut p, mut q) = ([0u8; 16], [0u8; 16]);
        x.apply(&mut p);
        y.apply(&mut q);
        assert_ne!(p, q);
        // A pinned vector, produced by tests/interop_ed2k_crypto.py from MD5 + RC4 written
        // separately in Python: first 16 keystream bytes after the 1024 discarded.
        let (mut s, _) = ciphers(b"0123456789abcdef", [1, 2, 3, 4], true);
        let mut out = [0u8; 16];
        s.apply(&mut out);
        assert_eq!(out, PINNED_INITIATOR_KEYSTREAM);
    }
    const PINNED_INITIATOR_KEYSTREAM: [u8; 16] = [
        162, 194, 68, 222, 109, 146, 205, 25, 35, 122, 23, 213, 235, 87, 150, 55,
    ];

    #[tokio::test]
    async fn partial_socket_writes_preserve_every_encrypted_byte_under_backpressure() {
        let hash = [0x42; 16];
        let random = [1, 2, 3, 4];
        let (send, receive) = ciphers(&hash, random, true);
        let (_, mut decrypt) = ciphers(&hash, random, false);
        let data: Vec<u8> = (0..8192).map(|index| (index % 251) as u8).collect();
        let expected = data.clone();
        // Forces Ready(partial) followed by Pending during repeated logical writes.
        let (socket, mut remote) = tokio::io::duplex(17);
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            let writer = async move {
                let mut stream = CryptStream::new(socket, send, receive);
                stream.write_all(&data).await.unwrap();
                stream.shutdown().await.unwrap();
            };
            let reader = async {
                let mut wire = Vec::new();
                remote.read_to_end(&mut wire).await.unwrap();
                wire
            };
            let (_, wire) = tokio::join!(writer, reader);
            wire
        })
        .await
        .expect("encrypted write stalled under backpressure");
        let mut plaintext = outcome;
        decrypt.apply(&mut plaintext);
        assert_eq!(plaintext, expected);
    }

    #[tokio::test]
    async fn the_handshake_encrypts_both_directions() {
        let hash = [0x42u8; 16];
        let (a, b) = tokio::io::duplex(65536);
        let responder = tokio::spawn(async move {
            let mut b = b;
            let marker = b.read_u8().await.unwrap();
            let mut stream = respond(b, marker, &hash).await.unwrap();
            let mut got = [0u8; 12];
            stream.read_exact(&mut got).await.unwrap();
            stream.write_all(b"pong from B!").await.unwrap();
            got
        });
        let mut stream = initiate(a, &hash).await.unwrap();
        stream.write_all(b"ping from A!").await.unwrap();
        let mut answer = [0u8; 12];
        stream.read_exact(&mut answer).await.unwrap();
        assert_eq!(&answer, b"pong from B!");
        assert_eq!(&responder.await.unwrap(), b"ping from A!");
    }

    #[tokio::test]
    async fn the_bytes_on_the_wire_carry_no_protocol_marker_or_plaintext() {
        let hash = [0x42u8; 16];
        let (a, mut b) = tokio::io::duplex(65536);
        let initiator = tokio::spawn(async move {
            let mut stream = initiate(a, &hash).await.unwrap();
            stream.write_all(&[0xe3, 1, 0, 0, 0, 1]).await.unwrap();
            stream
        });
        let mut wire = vec![0u8; 5];
        b.read_exact(&mut wire).await.unwrap();
        assert!(!is_protocol_byte(wire[0]));
        // Complete the responder side by hand so the initiator can finish.
        let marker = wire[0];
        let mut random = [0u8; 4];
        random.copy_from_slice(&wire[1..5]);
        let (mut send, mut receive) = ciphers(&hash, random, false);
        let mut head = [0u8; 7];
        b.read_exact(&mut head).await.unwrap();
        receive.apply(&mut head);
        assert_eq!(
            u32::from_le_bytes(head[..4].try_into().unwrap()),
            MAGIC_SYNC
        );
        let mut skip = vec![0u8; head[6] as usize];
        b.read_exact(&mut skip).await.unwrap();
        let mut reply = Vec::new();
        reply.extend(MAGIC_SYNC.to_le_bytes());
        reply.extend([0, 0]);
        send.apply(&mut reply);
        b.write_all(&reply).await.unwrap();
        let _stream = initiator.await.unwrap();
        let mut payload = [0u8; 6];
        b.read_exact(&mut payload).await.unwrap();
        assert_ne!(payload[0], 0xe3, "the first payload byte must be encrypted");
        let _ = marker;
        receive.apply(&mut skip);
        receive.apply(&mut payload);
        assert_eq!(payload, [0xe3, 1, 0, 0, 0, 1]);
    }

    #[tokio::test]
    async fn a_wrong_user_hash_fails_the_handshake_instead_of_producing_garbage() {
        let (a, b) = tokio::io::duplex(65536);
        let responder = tokio::spawn(async move {
            let mut b = b;
            let marker = b.read_u8().await.unwrap();
            respond(b, marker, &[9u8; 16]).await.is_err()
        });
        let outcome =
            tokio::time::timeout(std::time::Duration::from_secs(2), initiate(a, &[1u8; 16])).await;
        assert!(responder.await.unwrap());
        // The initiator sees the connection close without a valid reply.
        assert!(!matches!(outcome, Ok(Ok(_))));
    }

    #[test]
    fn markers_never_look_like_a_plain_protocol_byte() {
        for _ in 0..2000 {
            assert!(!is_protocol_byte(marker().unwrap()));
        }
    }
}
