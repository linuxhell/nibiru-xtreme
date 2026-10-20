# Protocol implementation — 0.2.0 alpha work

The measured 0.1.4 HTTP scheduler remains the default. Adaptive concurrency and
background checkpoints remain opt-in. This branch is development work, not a
complete multiprotocol release.

## Implemented and locally exercised

- Existing HTTP/1.1, HTTPS and HTTP/2 paths and regression tests.
- HTTP/3 over QUIC, explicitly selected with `download ... --http3-only`.
  A local QUIC server transfers and compares binary payloads. An untrusted TLS
  certificate fails. HTTP/3-only never silently switches to HTTP or TCP.
- FTP binary downloads over PASV on IPv4, EPSV on IPv6. Passive addresses are
  replaced with the control peer to prevent FTP bounce/foreign-address access.
- Explicit FTPS (`ftpes://`, default port 21) and implicit FTPS (`ftps://`, 990),
  with certificate and hostname validation on control and data channels.
- SFTP over SSH, with a mandatory out-of-band SHA256 host-key fingerprint;
  password authentication or an unencrypted OpenSSH identity file.
- `webdav://` and `webdavs://` aliases for downloading an individual file via
  HTTP GET. Collection traversal, DAV locks and write operations are not implemented.

New direct protocols use bounded streaming buffers, observe cancellation during
network stalls, verify known lengths and FTP completion replies, sync the output,
and publish without overwriting another destination. Failed/truncated transfers
must not create a completed file. These new paths currently restart instead of
resuming; HTTP retains its existing resume behaviour.

Examples:

```text
nibiru-xtreme download ftpes://user:password@example.test/file.bin --out file.bin
nibiru-xtreme download sftp://user:password@example.test/file.bin --out file.bin --ssh-host-key SHA256:TRUSTED_FINGERPRINT
nibiru-xtreme download https://example.test/file.bin --out file.bin --http3-only
```

Use percent-encoding for reserved characters in URL credentials and paths. Do
not put real secrets in shell history. `--ca-certificate` adds a DER trust root
for FTPS or HTTP/3; it does not disable certificate validation. JSON-RPC addUri
accepts `ssh-host-key`, `ssh-identity`, and `ca-certificate` as per-task options.
Resume options are preserved for the same RPC task.

## BitTorrent transport and BEP 52 work

The engine handles v1, v2 and hybrid metainfo and `urn:btmh:1220…` magnets.
SHA256 piece layers are checked against each file's root; magnet peers supply
correlated BEP 52 hash responses with authenticated Merkle proofs. Missing hashes
keep pieces unavailable to the scheduler. Short final file pieces, padding and
zero-length files are handled separately. Hybrid uploads accept the v1 identity
and its padding geometry; tracker/DHT announcements cover both known identities.

TCP and uTP support plaintext or MSE/PE. `--enable-utp=false` disables uTP;
`--bt-force-encryption=true` requires RC4 negotiation and rejects plaintext peers.
MSE is legacy traffic obfuscation, not peer authentication. The uTP transport
uses the Apache-2.0 Rust `librqbit-utp` crate, not libtorrent. Native engine code
continues to forbid unsafe code.

DHT has separate IPv4/IPv6 routing instances, compact address formats and
IP-bound BEP 42 IDs. A public address change needs observations from three
independent reporter networks. IPv6 is best effort where the OS cannot bind it;
`--enable-dht6=false` disables it. Saved IPv6 nodes use the DHT path's `.ipv6`
extension. DHT can use a different UDP port from the TCP/uTP peer port.

`tests/interop_libtorrent.py` uses libtorrent 2.1.1 only as an independent test
client: v1/v2/hybrid download through metadata exchange and upload, TCP with MSE,
uTP plaintext and uTP with MSE (18 scenarios). Payloads are compared byte for
byte; v2 uses over 128 pieces to exercise multiple hash requests. Rust tests
exercise IPv6 transport/DHT and corrupted Merkle proofs. These loopback tests do
not prove public tracker availability or NAT traversal. Hole punching and local
service discovery remain unimplemented.

## Embedding without unused transports

Cargo features `ftp`, `sftp`, and `http3` are enabled in the standalone build.
An ED2K-only consumer can use `default-features = false` to avoid compiling and
linking the SSH/FTP stacks. Feature-disabled protocols are not advertised by RPC.
The HTTP and ED2K/Kad foundations remain available.

## Native ED2K/Kad foundation

The `ed2k` Rust module implements bounded `server.met` and Kad2 `nodes.dat`
parsers, HTTPS-only list fetching, ED2K login acknowledged by OP_IDCHANGE,
server search, correlated source discovery and a Kad2 UDP bootstrap probe. Tests exchange actual wire packets
with local TCP/UDP fixtures. Connection state is not inferred from a TCP connect
alone. Packet sizes, decompression and result counts have explicit limits.

Native high-ID peer downloads support baseline hello/file/status/hashset/queue
and 32-bit or 64-bit block packets, split compressed blocks, MD4 part verification
and resume by rehashing saved parts. Hashsets must match the ED2K root before
parts are trusted. Files stay under a partial name until sync and no-clobber
finalization. Names are validated as a single Windows-compatible filename.

An incoming TCP service advertises its real bound port. Server-mediated
Low-ID callbacks can feed verified downloads when the requester has a reachable
High-ID session. Pending callbacks are bounded, correlated to the peer client ID,
and removed on cancellation. This does not solve two firewalled peers or NAT
traversal.

### Sharing and uploading

Nothing is shared implicitly. Only files placed in a `ShareRegistry` (explicit
files, or a chosen directory with optional recursion) are ever served: paths are
canonicalised, symlinks and empty files are refused, the ED2K MD4 part hashes
are computed, and every served range re-checks that the file is still the same
regular file with the same size.

- **Uploader.** Answers filename, file status (a part count of zero means a
  complete file, as eMule reports it), hashset and `OP_STARTUPLOADREQ`; accepts
  eMule's extended `OP_REQUESTFILENAME`; serves `OP_REQUESTPARTS` and the 64-bit
  `OP_REQUESTPARTS_I64` in packets of at most 10,240 bytes. Ranges are bounded
  (256 KiB each, 3 per request); ranges past the end of the file or a file
  changed on disk end the session.
- **Queue.** `UploadManager` keeps a configurable number of active slots and a
  bounded waiting list ordered by file priority (0-4) and then by waiting time.
  A queued client receives `OP_QUEUERANKING` and is promoted with
  `OP_ACCEPTUPLOADREQ` when a slot opens; a client that disconnects keeps its
  place for 30 minutes. `OP_CANCELTRANSFER`/`OP_END_OF_DOWNLOAD` and idle timeouts
  free slots. Connections are capped in total and per IP.
- **Bandwidth.** One token-bucket limit is shared by all uploads.
- **Statistics.** Per-client and per-file requests, accepted slots and bytes.
- **Publication.** `ServerConnection::offer_files` sends `OP_OFFERFILES` in
  batches of 50 (at most 5,000 files), using the complete-file markers for
  servers that report new-tag support and the real ID/port otherwise; files over
  4 GiB are only offered to servers that report large-file support. ED2K has no
  withdraw packet: reconnect and publish the new list to remove files.
- **Persistence helpers.** `ShareRegistry::export_cache`/`add_file_cached` reuse
  stored hashes only when path, size and modification time still match and the
  hashes are internally consistent; `rescan` drops vanished files and re-hashes
  modified ones.

Verified with real-socket tests, including Nibiru downloading a two-part file
from a Nibiru listener. Not interoperability-tested against eMule/aMule yet.

Kad bootstrap is **not** persistent Kad routing. Credits, AICH, peer
obfuscation, secure identification, source exchange, Kad lookup/publishing and
messaging are not implemented; the hello deliberately does not advertise them.
The alpha must not be advertised as complete.

## Reference research

Protocol constants and interchange formats were checked against the maintained
[aMule repository](https://github.com/amule-org/amule), release
[3.1.0](https://github.com/amule-org/amule/releases/tag/3.1.0), commit
`909d304d993ee07df6c6f6acf501a6d791d53666` (latest stable observed 2026-10-19).
The earlier `amule-project/amule` repository points to this maintained project.
Relevant reference locations: `src/include/protocol`, `src/include/tags`,
`src/ServerList.cpp`, `src/Preferences.cpp`, and
`src/kademlia/routing/RoutingZone.cpp`. This is an independent Rust implementation,
not a wrapper around aMule and not a translated copy of its C++ implementation.

Default metadata sources match aMule 3.1.0 preferences:

- `https://upd.emule-security.org/server.met`
- `https://upd.emule-security.org/nodes.dat`

Both returned HTTP 200 on 2026-10-19. The downloaded files were accepted by the
native parser (16 servers, 192 Kad2 nodes). This proves format compatibility of
those samples, not availability of every listed server or public-network login.
In this environment, HTTPS downloads can traverse a TLS-intercepting proxy;
Rustls trust checks are not disabled to accommodate it.

Primary implementation API references:
[SuppaFTP](https://docs.rs/suppaftp/), [Russh](https://docs.rs/russh/),
[russh-sftp](https://docs.rs/russh-sftp/), [Reqwest](https://docs.rs/reqwest/).


Peer transfer validation uses real local TCP sockets: compressed and fragmented
blocks, an out-of-order raw response, a resumed multipart file, an independently
verified MD4 test vector, exact ED2K part-boundary hashing, corrupted data and
hashsets, cancellation during a stalled handshake, 64-bit offsets beyond 4 GiB,
and a destination-created-during-finalization race. These fixtures do not prove
interoperability with every public client or server. Transfers currently use
one peer and one bounded block request at a time, rotate across up to 16 supplied
direct sources and retain verified partial data on incomplete attempts.

Incoming TCP tests exercise the advertised identity and no-file responses. A real
TCP server/callback fixture requests a Low-ID source through OP_CALLBACKREQUEST,
rejects routing an unrelated incoming client ID, then completes and verifies the
requested file over the matching incoming connection.
