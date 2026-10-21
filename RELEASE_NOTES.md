# Nibiru Xtreme — download engine (alpha)

## 0.2.0-alpha.2 — PRÉ-RELEASE PARA TESTE (não é a release final)

Esta pré-release existe para o usuário testar o motor. **Não substitui o aria2-ultra no ADM** e não é uma
versão completa: veja `docs/CAPABILITY-MATRIX.md` para o que está pendente.

Novo desde a alpha.1 (testado automaticamente; só em loopback, nada em rede pública):

- RPC com **persistência de tarefas** (`serve --session-file`), `nibiru.getSessionInfo`, contadores reais.
- **ED2K pelo RPC** (`addUri ed2k://…`, `serve --ed2k`), **várias fontes por arquivo**, sessão **Kad2** no
  motor, ofuscação ED2K (TCP) e Kad (UDP), callbacks Low-ID por servidor e por buddy do Kad (solicitante).
  Verificado contra um **aMule 2.3.3** real em loopback (inclusive exigindo ofuscação).
- **Retomada de FTP/FTPS/SFTP**.
- **LSD (BEP 14)** (`--enable-lpd`) e **hole punching (BEP 55)**, verificados contra libtorrent 2.1.1.
- HTTP/3 corrigido em hosts sem IPv6.

Limites conhecidos: nunca testado na rede ED2K/Kad pública, com NAT real ou em LAN com vários hosts;
WebDAV é só GET; sem Metalink; sem ofuscação cliente↔servidor ED2K; sem busca/publicação Kad.
Os binários não são assinados. Confira o SHA-256 do `.sha256` ao lado de cada arquivo.

Nibiru Xtreme is a single-binary download engine with an aria2-compatible JSON-RPC interface, built to be
the engine of the Apocalipse Download Manager. **This is an alpha**: it passes its own loopback tests and
interoperability tests against aria2c, but it has had little exposure to the open internet.

## Bandwidth limit fix

HTTP and BitTorrent now account for every byte before releasing a received block,
including the final block and blocks larger than the one-second burst budget.
Concurrent connections share that budget. Previously, an oversized final block
could finish without waiting for the excess bytes, making limits depend on how
the operating system divided network reads.

Rapidly pausing and resuming a torrent also preserves peers whose old connection
is still closing or whose handshake is pending, so they can reconnect without
waiting for the tracker's next announce interval.

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

## Protocol expansion in development

- TCP/uTP with optional or required MSE/PE, IPv4/IPv6 DHT and BEP 42 node IDs.
- BEP 52 v2/hybrid metainfo, SHA256/Merkle proofs and btmh magnets; hashes are
  authenticated before enabling data requests. Hybrid sessions also accept v1 identities.
- Upload request queue advertisement now matches its bound; fair processing avoids
  starving uploads under a busy request pipeline.
- HTTP RPC split/server/minimum-size options constrain actual payload concurrency;
  the one-byte Range probe finishes before payload connections begin.
- Independent libtorrent 2.1.1: 18 complete, byte-compared loopback transfers.
- User reported a complete public-magnet download and VLC preview on Windows.
  Connections began after disabling a third-party firewall. This is a user report,
  not an automated public-network or firewall compatibility test.

## Not implemented yet

- Local Service Discovery, hole punching and Metalink.
- Kad persistent sessions, ED2K credits/secure identification/obfuscation.


## Files

`nibiru-xtreme-<version>-windows-x64.exe`, `-linux-x64` (static musl), `-macos-x64`, each with a `.sha256`.
