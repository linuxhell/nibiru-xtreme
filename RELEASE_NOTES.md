# Nibiru Xtreme 0.2.2

- Re-sanitize names after truncation; bound UTF-8 and UTF-16 lengths and preserve normal extensions. Keep legitimate dollar signs and apostrophes.
- Handle case-insensitive torrent filename aliases and reject file/directory collisions before storage is opened.
- Apply common portable component rules to HTTP RPC and ED2K, including Windows device aliases and trailing Unicode whitespace.
- Web seeds retain original network names, sharing a weakly cached mapping across workers.
- Make pause/resume and private Kad handshake fixtures deterministic without changing production timing.


- HTTPS now includes the operating system trust store alongside public roots, retaining certificate and hostname validation. This fixes downloads when the required CA is trusted by the OS.
- RPC errors report TLS certificate trust failures instead of only a generic HTTP request failure; nested secrets remain hidden.
- The same NirSoft URLs reported by the user were downloaded through RPC and their ZIP CRCs verified (98,007 / 2,985 / 2,867 bytes).

- ED2K preview scheduling prioritizes verified header and tail parts, with cooperative requests across up to eight peers. Missing source parts remain pending with explicit diagnostics.
- HTTP destination locks are released when transfers finish. Resume checkpoints are retained where required.
- Idle TCP, uTP, DHT and preview listeners release their ports when their owners are dropped.
- CI tests Windows, Linux and macOS, including independent aMule/libtorrent interoperability on Linux. Release reuses those tested executables.
- Windows test artifacts contain only the EXE.

Release files: Windows x64 EXE, Linux x64 static musl executable and macOS x64 executable, each with SHA-256. Binaries are unsigned. Unix users must grant execute permission after downloading.

ADM 0.7.0 enables ED2K preview after the first MD4-verified part (up to 9,728,000 bytes); MP4/AVI may also need the tail index. The reported partial file had 77/86 parts available from its responding sources; this release cannot create unavailable parts.

Known limitations include full Kad keyword search/publishing, ED2K credits/secure identification, client-to-server ED2K obfuscation and Metalink. See [protocol coverage](docs/protocols.md), [capability matrix](docs/CAPABILITY-MATRIX.md) and [review evidence](docs/AUDIT-2026-10-22.md).

Maintenance after this release focuses on bugs found during everyday use.
