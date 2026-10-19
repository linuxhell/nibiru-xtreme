# Nibiru Xtreme

Nibiru Xtreme is a new cross-platform download engine written in Rust. Its goals are a small binary, strong security boundaries, high throughput, resumable transfers, excellent diagnostics, and first-class support for modern download protocols.

## Current status

The repository currently contains the first functional bootstrap engine:

- HTTP/HTTPS downloads;
- HTTP/2-capable Rustls client;
- safe resume using HTTP Range when the server accepts it;
- concise JSON-line diagnostics;
- loopback-only JSON-RPC server;
- an initial aria2-compatible RPC subset used to simplify migration from Apocalipse Download Manager;
- strict path validation for RPC output names;
- no project-owned `unsafe` code.

BitTorrent v1/v2/hybrid, Magnet, DHT, PEX, uTP, HTTP/3/QUIC, FTP/FTPS, SFTP, WebDAV and ED2K/Kad are planned modules and will only be marked stable after interoperability and regression tests.

The ADM will keep its existing production engine until Nibiru reaches feature parity and passes the migration test suite.

Português: [README.md](README.md)  
简体中文: [README.zh-CN.md](README.zh-CN.md)
