# Nibiru Xtreme

Nibiru Xtreme is a Rust download engine with an aria2-compatible JSON-RPC subset.
It remains a development alpha; ADM keeps its production engine until migration
and parity checks pass.

## Implemented in this branch

- HTTP/HTTPS/HTTP2 downloads, segmentation, verified Range resume and bandwidth limits.
- Explicit HTTP/3, FTP/FTPS and SFTP downloads; SFTP requires a trusted SSH fingerprint.
- WebDAV aliases for individual-file GET, without collection traversal.
- BitTorrent v1/v2/hybrid, `btih` and `btmh` magnets, metadata exchange, Merkle proofs,
  PEX, trackers, verified resume, selective downloading, seeding and preview streaming.
- TCP/uTP and MSE/PE negotiation, including encryption-required mode.
- Separate IPv4/IPv6 DHT instances, BEP 42 IDs and persisted bootstrap nodes.
- Native ED2K search, transfer and explicit sharing foundations; Kad bootstrap only.

Independent libtorrent 2.1.1 tests compare the complete payload in 18 upload and
magnet-download scenarios across v1/v2/hybrid and TCP/uTP/MSE. The engine does
not embed libtorrent. uTP uses the Rust `librqbit-utp` dependency; see
[third-party notices](THIRD_PARTY_NOTICES.md).

## Remaining work

Hole punching, local discovery, persistent Kad routing/search/publication,
ED2K credits/secure identification/obfuscation/AICH, Metalink, full WebDAV,
FTP/SFTP resume and full aria2 RPC/options parity remain incomplete. MSE is
legacy obfuscation, not authenticated encryption. Project-owned Rust forbids
unsafe code; dependencies have their own implementation and licenses.

See [protocol details and limitations](docs/protocols.md).

Português: [README.md](README.md)  
简体中文: [README.zh-CN.md](README.zh-CN.md)
