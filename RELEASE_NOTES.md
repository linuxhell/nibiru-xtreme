# Nibiru Xtreme 0.2.0

- ED2K preview scheduling prioritizes verified header and tail parts, with cooperative requests across up to eight peers. Missing source parts remain pending with explicit diagnostics.
- HTTP destination locks are released when transfers finish. Resume checkpoints are retained where required.
- Idle TCP, uTP, DHT and preview listeners release their ports when their owners are dropped.
- CI tests Windows, Linux and macOS, including independent aMule/libtorrent interoperability on Linux. Release reuses those tested executables.
- Windows test artifacts contain only the EXE.

Release files: Windows x64 EXE, Linux x64 static musl executable and macOS x64 executable, each with SHA-256. Binaries are unsigned. Unix users must grant execute permission after downloading.

ADM 0.7.0 enables ED2K preview after the first MD4-verified part (up to 9,728,000 bytes); MP4/AVI may also need the tail index. The reported partial file had 77/86 parts available from its responding sources; this release cannot create unavailable parts.

Known limitations include full Kad keyword search/publishing, ED2K credits/secure identification, client-to-server ED2K obfuscation and Metalink. See [protocol coverage](docs/protocols.md), [capability matrix](docs/CAPABILITY-MATRIX.md) and [review evidence](docs/AUDIT-2026-10-22.md).

Maintenance after this release focuses on bugs found during everyday use.
