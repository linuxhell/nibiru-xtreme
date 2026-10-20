# Nibiru Xtreme — download engine (alpha)

Nibiru Xtreme is a single-binary download engine with an aria2-compatible JSON-RPC interface, built to be
the engine of the Apocalipse Download Manager. **This is an alpha**: it passes its own loopback tests and
interoperability tests against aria2c, but it has had little exposure to the open internet.

## What works

- **HTTP/HTTPS/HTTP2** segmented downloads with resume, custom headers/cookies/referer/user-agent, proxies
  (`all-proxy`) and per-task / global speed limits. FTP/FTPS, SFTP and explicit HTTP/3.
- **BitTorrent** (from the specifications, no libtorrent): `.torrent` files and `magnet:` links,
  metadata exchange (BEP 9), extension protocol (BEP 10), peer exchange (BEP 11), fast extension (BEP 6),
  HTTP/UDP trackers (BEP 3/12/15, SSRF-protected), DHT (BEP 5), web seeds (BEP 19), selective file
  download (`select-file`), verified resume by re-hashing, seeding with ratio/time limits, choking.
- **Watch while downloading**: `nibiru.streamUrl` returns a loopback HTTP URL (token-protected, Range
  support) that a media player can open; seeking steers the downloader to the pieces being played.
- **ED2K** library (hash, search, download, upload queue, sharing) — see `docs/protocols.md` for what is and
  is not implemented. Kad is not complete.
- aria2-compatible startup flags (`--enable-rpc=true --rpc-listen-port=… --rpc-secret=… --stop-with-process=…`)
  and RPC methods used by the manager: `addUri`, `addTorrent`, `tellStatus`, `pause`, `unpause`, `remove`,
  `removeDownloadResult`, `changeOption`, `changeGlobalOption`, `getFiles`, `getPeers`, `getGlobalStat`,
  `tellActive`, `getVersion`.

## Not implemented yet

- uTP (BEP 29), Message Stream Encryption, BitTorrent v2/hybrid (BEP 52) hashing, IPv6 DHT (BEP 32),
  Local Service Discovery, Metalink, secure DHT node IDs (BEP 42).
- Kad persistent sessions, ED2K credits/secure identification/obfuscation.

Peers that *require* encryption or only speak uTP cannot be reached yet.

## Files

`nibiru-xtreme-<version>-windows-x64.exe`, `-linux-x64` (static musl), `-macos-x64`, each with a `.sha256`.
