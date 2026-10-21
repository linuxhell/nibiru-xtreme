[Português](README.md) · [English](README.en.md) · [简体中文](README.zh-CN.md)

# Nibiru Xtreme

Rust download engine with a native JSON-RPC contract and an aria2 compatibility subset.

## Status

**0.2.0-alpha.2 is development work, not a complete multiprotocol implementation.** ADM retains its current engine until Xtreme is complete and its integration is validated. No release was published when checked on 2026-10-21; CI artifacts are test candidates.

Code auditing is in progress. “Compiled”, “automatically tested” and “validated in real use” are different states. See the [capability matrix](docs/CAPABILITY-MATRIX.md) for evidence and [protocol documentation](docs/protocols.md) for formats and limitations.

## Protocols and limitations

| Area | Implementation and limitation |
| --- | --- |
| HTTP/1.1, HTTPS, HTTP/2 | Segmentation, identity-checked Range resume, headers/proxy options and HTTP bandwidth limits |
| HTTP/3 | Explicit HTTPS/QUIC through `--http3-only`; no automatic H2/H3 selection |
| FTP/FTPS | Downloads and REST resume; explicit/implicit TLS; implicit resume lacks a dedicated test |
| SFTP | Downloads and seek resume; mandatory SSH fingerprint; password or unencrypted OpenSSH key |
| WebDAV | Individual-file GET aliases; no collections, PROPFIND, locks or writes |
| BitTorrent | v1/v2/hybrid, `btih`/`btmh` magnets, metadata, PEX, trackers, web seeds, selection, seeding and streaming |
| TCP/uTP and MSE/PE | Optional/required obfuscation; MSE does not authenticate peers |
| DHT / LSD | IPv4/IPv6 DHT; opt-in LSD is IPv4-only on one interface |
| Hole punching | BEP 55 over uTP; restricted loopback emulation, no validated real NAT traversal |
| ED2K | MD4 part downloads, up to four simultaneous sources, resume, peer obfuscation and server/buddy callbacks |
| Kad2 | Persistent session, source lookup, contact verification, IP filter and UDP obfuscation; advanced features incomplete |

Explicit sharing, uploads and an ED2K queue exist as library APIs, not a complete RPC/CLI sharing service. Remaining gaps include Metalink, full WebDAV, credits, secure identification, AICH, source exchange, ED2K client-server obfuscation, Kad keyword search/publication, firewall testing and acting as a buddy.

## Usage

```sh
cargo build --release --locked
nibiru-xtreme capabilities --json
nibiru-xtreme download https://example.test/file.bin --out file.bin
nibiru-xtreme serve --port 6800 --secret SECRET --dir downloads --session-file session.json
```

`serve` binds IPv4 loopback. `nibiru.*` is the native contract; `aria2.*` aliases cover only implemented methods. `--session-file` restores unfinished tasks paused with stable GIDs. Tasks carrying recognized credentials are not persisted; the manager must recreate them. Legacy `--input-file` and `--save-session` are not equivalent to native persistence.

For ED2K add `--ed2k`, servers with `--ed2k-server host:port` and optionally `--ed2k-nodes-file nodes.dat`. LSD uses `--enable-lpd [--lpd-interface IP]`. See `--help` and protocol documentation for options and limits. Keep real credentials out of shell history.

## Structure and validation

| Path | Responsibility |
| --- | --- |
| `src/engine.rs`, `src/storage` | HTTP, scheduling and storage |
| `src/protocols` | FTP/FTPS, SFTP and HTTP/3 |
| `src/bittorrent` | BitTorrent, DHT, trackers and transports |
| `src/ed2k` | ED2K, Kad, obfuscation and explicit sharing |
| `src/rpc` and `src/rpc.rs` | Tasks, persistence and JSON-RPC |
| `tests`, `benchmarks` | Regressions, interoperability and measurements |

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
```

Independent tests use libtorrent 2.1.1 and aMule as other clients, not embedded engines; they require separate installation. Tests may skip when external programs or IPv6 are unavailable: inspect the output. This base has no public ED2K/Kad, real NAT or real-world Windows/macOS validation. Fixed HTTP scheduling remains the default; adaptivity and background checkpoints are opt-in. Benchmarks do not establish universal superiority.

## Security, CI and license

Project-owned Rust forbids `unsafe`. TLS validates certificates; SFTP requires a known host key. JSON-RPC has an optional token and connection/read/size limits. Configure a manager secret. JSON Lines logs remove credentials, query and fragment from diagnostic HTTP URLs, including request errors.

`ci.yml` validates Windows, Linux and macOS and tests interoperability on Linux. `release.yml` prepares packages; publication requires explicit authorization. ADM's distribution/signed-manifest contract still needs completion.

MIT. Dependencies retain their licenses; see [LICENSE](LICENSE), [third-party notices](THIRD_PARTY_NOTICES.md) and [Apache 2.0](LICENSE-APACHE-2.0.txt).
