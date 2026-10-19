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

## Embedding without unused transports

Cargo features `ftp`, `sftp`, and `http3` are enabled in the standalone build.
An ED2K-only consumer can use `default-features = false` to avoid compiling and
linking the SSH/FTP stacks. Feature-disabled protocols are not advertised by RPC.
The HTTP and ED2K/Kad foundations remain available.

## Native ED2K/Kad foundation

The `ed2k` Rust module implements bounded `server.met` and Kad2 `nodes.dat`
parsers, HTTPS-only list fetching, ED2K login acknowledged by OP_IDCHANGE,
server search and a Kad2 UDP bootstrap probe. Tests exchange actual wire packets
with local TCP/UDP fixtures. Connection state is not inferred from a TCP connect
alone. Packet sizes, decompression and result counts have explicit limits.

Kad bootstrap is **not** persistent Kad routing. Peer downloads/uploads, upload
queues, credits, AICH, peer obfuscation, Kad lookup/publishing, messaging and
full aMule parity are not implemented. BitTorrent/Magnet, DHT/PEX/uTP and hole
punching also remain pending. The alpha must not be advertised as complete.

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
