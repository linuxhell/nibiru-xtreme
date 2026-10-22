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
must not create a completed file. FTP/FTPS/SFTP retain validated partials for resume; see the dedicated resume
section below. HTTP retains its separate identity-checked resume path.

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
not prove public tracker availability or NAT traversal. BEP 55 hole punching and BEP 14 local service discovery are implemented; their
loopback evidence and remaining limits are described below.

## RPC task persistence

`serve --session-file=PATH` keeps unfinished tasks across restarts. Without the flag
nothing is written and `nibiru.getSessionInfo` reports `"persistent": false`.

- **What is stored.** A task is written when it is not complete or removed: its GID,
  source (URL, `.torrent` bytes, or magnet link) and non-sensitive configuration options (`dir`,
  `out`, limits, `select-file`, seeding limits, `user-agent`, CA/host-key settings).
  The file is rewritten atomically (temporary file, fsync, rename) shortly after every
  task change and at least every 5 seconds, and on SIGTERM/Ctrl-C and when the parent
  process disappears. Unchanged content is not rewritten. On Unix it is mode `0600`.
- **What is never stored.** Any task whose request carries credentials stays out of the
  file: URL user info, direct-download URLs with a query string, cookies or any header other than
  `Accept*`/`Origin`, proxy user names or passwords, proxy URLs with user info. `Referer` is
  deliberately dropped because it can contain signed URLs or tokens. Downloaded bytes are not trusted
  because of this file either. After a restart the manager sees an unknown GID for such a
  task and recreates it with fresh credentials, exactly as without persistence. HTTP resume sidecars
  store SHA-256 URL fingerprints rather than cleartext request/final URLs; legacy cleartext sidecars are
  accepted only on an exact URL match and are immediately migrated.
- **Restore.** On start the engine recreates the stored tasks under their old GIDs, all
  `paused`; none starts by itself. Resuming a direct task re-checks the saved HTTP
  validator (a changed origin restarts it), and resuming a torrent re-hashes the payload
  on disk. `nibiru.getSessionInfo` returns `persistent`, `restored` and `skipped`.
- **Bad files.** An unreadable file or one with another `version` is moved to
  `<name>.corrupt-<time>` and never overwritten; individual invalid or duplicate entries
  are skipped and counted. At most 5,000 tasks and 16 MiB are read.
- **Counters.** `getGlobalStat` reports real `numWaiting` and `numStopped`; `numStoppedTotal`
  also counts results the manager purged with `removeResult`.
- **Limits.** Completed results are not persisted (the manager keeps its own history).
  Seeding torrents that already finished are not restored. Progress counters are not stored:
  they are recomputed when a task resumes. Metadata-only magnet fetches are transient.

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
from a Nibiru listener. The later independent aMule download and Kad tests are recorded in the capability
matrix; this does not prove every upload/queue feature against an independent client.

The legacy `kad.rs` bootstrap probe is separate from the persistent Kad2 session
in `kad_session.rs` (source lookup, verification, IP filtering and UDP obfuscation).
Peer obfuscation is implemented. Credits, AICH, secure identification, source
exchange, Kad keyword search/publication and messaging remain incomplete.
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
interoperability with every public client or server. Transfers use up to four sources concurrently, with an entire part reserved to
one source. They retain verified partial data on incomplete attempts; dividing
a single part among multiple peers remains unimplemented.

Incoming TCP tests exercise the advertised identity and no-file responses. A real
TCP server/callback fixture requests a Low-ID source through OP_CALLBACKREQUEST,
rejects routing an unrelated incoming client ID, then completes and verifies the
requested file over the matching incoming connection.


## Retomada de FTP, FTPS e SFTP

Uma transferência FTP/FTPS/SFTP interrompida (cancelamento, queda de conexão, resposta truncada,
`pause`) deixa o parcial ao lado do destino e a próxima tentativa o continua:

- Arquivos: `.<nome>.nibiru-part` (dados) e `.<nome>.nibiru-part.json` (validador). O destino só
  aparece quando o arquivo está completo, validado e sincronizado, sem sobrescrever outro arquivo.
- O validador guarda `protocolo://host:porta/caminho`, o tamanho e, quando existe, a data de
  modificação (`MDTM` no FTP, `mtime` no SFTP). **Nunca** guarda usuário, senha, query nem fragmento.
- Só há retomada quando o servidor informa o tamanho. Validador diferente descarta o parcial.
- Para não confiar em bytes que a queda pode ter deixado incompletos, a retomada volta até 8 MiB
  antes do fim do parcial, baixa de novo esse trecho e o **compara byte a byte**. Se diferir (arquivo
  remoto trocado por outro do mesmo tamanho, ou servidor que ignorou o `REST`), o parcial é
  descartado e o arquivo é baixado do início uma vez. O parcial é sincronizado a cada 4 MiB e
  quando a cópia falha.
- FTP: o deslocamento é enviado com `REST`; se o servidor o recusar, a mesma conexão repete o
  `RETR` do byte 0. SFTP: `seek` no handle remoto.
- Limite conhecido: o trecho anterior ao overlap não é recomparado; um servidor que troque o
  conteúdo mantendo tamanho e data e preserve os últimos 8 MiB passaria despercebido. HTTP usa
  ETag/Last-Modified e é tratado à parte.


## Local Service Discovery (BEP 14)

Opt-in (`serve --enable-lpd [--lpd-interface IP]`, ou `--bt-enable-lpd=true` no modo aria2). O
motor entra no grupo multicast `239.192.152.143:6771` (IPv4, TTL 1), anuncia cada torrent
**público com metadados** a cada 5 minutos (primeiro anúncio em até 2 s) e aceita anúncios de
vizinhos para torrents que já tem e que não são privados.

- Formato conferido contra pacotes reais do libtorrent 2.1.1: `BT-SEARCH * HTTP/1.1`, `Host`, `Port`,
  `Infohash` (40 hex, repetível; no máximo 4 aceitos) e `cookie`, que identifica a sessão para ignorar
  o próprio anúncio devolvido pelo loopback multicast.
- Defesas: datagrama até 1400 bytes, limite de 40 pacotes por segundo, só origem privada, loopback ou
  link-local, `Port` 1–65535, cabeçalhos desconhecidos ignorados, torrents privados nunca anunciados
  nem aceitos, magnet sem metadados não é anunciado (ainda não se sabe se é privado).
- Limite: uma interface por vez (a da rota padrão, ou a indicada); sem IPv6 (`ff15::efc0:988f`).


## Hole punching (BEP 55)

Extensão `ut_holepunch`, anunciada quando uTP está ativo. Mensagem binária dentro do envelope BEP 10:
`msg_type` (0 rendezvous, 1 connect, 2 erro), `addr_type` (0 IPv4, 1 IPv6), endereço, porta (big
endian) e, **só nos erros**, um código de 4 bytes (1 sem peer, 2 não conectado, 3 sem suporte, 4 era
ele mesmo). O libtorrent 2.1.1 escreve `connect` com 8 bytes, sem o código; o decodificador aceita
os dois formatos, o codificador segue o do libtorrent.

- **Relay:** só apresenta pares que já estão conectados a ele por uTP e que anunciaram
  `ut_holepunch` (o endereço de um par TCP é uma porta efêmera, inútil para uTP). Manda `connect` aos
  dois lados. Nunca entrega um endereço que não seja de um par conectado, então não serve de refletor.
  No máximo um pedido por par a cada 2 s.
- **Alvo e iniciador:** ao receber `connect`, discam o endereço por uTP no mesmo instante (até 4
  discagens simultâneas por torrent). O iniciador só pede um rendezvous depois que a conexão direta
  falhou e quando um par lhe disse, por PEX (flag 0x08), que o alvo suporta hole punching.
- Conexões simultâneas entre o mesmo par: cada lado mantém a aberta pelo par de **menor peer ID**,
  que os dois calculam igual, em vez de cada um derrubar a do outro.
- Os pares recebem agora a lista de PEX logo depois do handshake de extensão, com as flags de uTP
  (0x04) e hole punching (0x08) dos pares que o relay conhece.
- `SessionConfig::emulate_restricted_nat` (apenas para testes, nunca pela CLI) faz uma sessão só aceitar
  de quem ela discou há menos de 60 s, para exercitar o fluxo numa máquina só.

## Ofuscação ED2K cliente↔cliente

Handshake básico, conforme o formato documentado no código do aMule e conferido contra um aMule 2.3.3
real (o aMule serve só de oráculo de teste; nenhum código dele foi copiado):

1. O iniciador (A) conhece o **user hash do alvo (B)** e sorteia 4 bytes (`random`).
2. Chaves RC4: `MD5(hashB | 34 | random)` para A→B e `MD5(hashB | 203 | random)` para B→A; os
   primeiros 1024 bytes do fluxo de cada chave são descartados.
3. A envia em claro: 1 byte marcador (qualquer byte que **não** seja `0xE3`, `0xC5` ou `0xD4`) e os 4
   bytes `random`; em seguida, cifrado: `magic 0x835E6FC4` (LE), métodos suportados (0), preferido (0),
   tamanho do preenchimento e preenchimento aleatório.
4. B responde cifrado: `magic`, método escolhido (0), tamanho do preenchimento e preenchimento. Daí em
   diante todo o tráfego, nos dois sentidos, segue o mesmo fluxo RC4.
5. O receptor distingue ofuscado de simples pelo primeiro byte: protocolo ED2K conhecido = simples.

O hash que importa é o que o par **anuncia**: o aMule grava um hash em `preferences.dat` e troca dois bytes
marcadores (posições 5 e 14) ao carregá-lo, então a chave usa o hash anunciado, não o do arquivo.

Modos: `off` (só simples; pares que exigem ofuscação são pulados), `prefer` (ofusca quando o hash do par é
conhecido e ele suporta; tenta de novo sem ofuscação se o handshake falhar; aceita os dois), `require`
(nunca usa nem aceita conexão simples). O login no servidor anuncia suporte (0x200) e desejo (0x400) de
ofuscação, exceto em `off`.

## Ofuscação UDP do Kad

Cada datagrama Kad pode ir cifrado: `<marcador 1><parte aleatória 2>` em claro e, cifrado com RC4 **sem
descartar** keystream, `<magic 0x395F2EC1 (LE)><tamanho do preenchimento 1><preenchimento><verify key do
receptor 4 (LE)><verify key do remetente 4 (LE)><pacote Kad>`. Duas chaves: `MD5(<ID Kad do receptor 16><parte
2>)` (bits baixos do marcador `00`) ou `MD5(<verify key do receptor 4 LE><parte 2>)` (bits `10`), para quem não
conhece o ID. O ID Kad é o dos pacotes (16 bytes como trafegam). Um datagrama que começa por `0xE4`, `0xE5`,
`0xC5`, `0xD4`, `0xA3` ou `0xB2` é simples.

- A verify key que damos a cada IP é `MD5(<segredo da sessão><IP>)` reduzida a 32 bits (a derivação é só nossa:
  quem a valida somos nós). O par a devolve no campo "receptor" quando nos responde.
- Pedidos a nós Kad de versão ≥ 6 saem cifrados com o ID deles; respostas a pedidos cifrados saem cifradas com a
  verify key que o remetente anunciou; pedidos simples recebem resposta simples.
- Lição do teste contra o aMule: o magic e as verify keys são **little-endian** na rede (`ENDIAN_SWAP_*` é no-op
  em máquinas little-endian); ler como big-endian dava um pacote que o aMule descartava em silêncio.
- A versão anunciada passou de 5 para 6 (primeira com ofuscação). O ACK com chave da versão 8 não existe.

## UPnP (mapeamento de portas no roteador)

`nibiru_xtreme::upnp::Upnp` (feature `upnp`, ligada por padrão) e `serve --upnp`.

- **O que faz:** descobre um roteador UPnP IGD (SSDP), pede o mapeamento **somente** das portas que o
  chamador listou (porta externa = porta local, IPv4), com concessão de 1 h (renovada a cada 20 min enquanto
  as portas forem desejadas) e remove tudo ao parar (`release`, ou Ctrl+C/SIGTERM/`--stop-with-process` no
  `serve`). Se o roteador só aceita concessão permanente (erro 725), repete com concessão 0 e remove no fim.
  Se o roteador some, procura de novo na renovação seguinte.
- **No `serve --upnp`:** porta TCP e UDP do BitTorrent, UDP do DHT e, com `--ed2k`, a TCP do ED2K e a UDP do
  Kad. O resultado vai para o log (`upnp.status`: `mapped`/`partial`/`failed`, IP externo, erro).
- **Segurança:** só confia num roteador cujos URLs de serviço apontam para o endereço que respondeu ao SSDP
  (padrão do `igd-next`), então uma resposta falsa não redireciona os pedidos; loopback é recusado fora de
  testes. Nada é mapeado sem pedido explícito.
- **Testado (loopback, roteador falso que fala o protocolo):** `tests/upnp.rs` (6): descoberta SSDP, descrição
  do dispositivo e do serviço, `AddPortMapping`/`DeletePortMapping`/`GetExternalIPAddress`; mapeia o IP local e a
  porta certos com a concessão; remove o que deixou de ser desejado; renova; retorna ao estado ocioso e apaga
  tudo na liberação; roteador só-permanente; nenhum roteador; roteador que anuncia outro host (recusado).
- **Não testado:** nenhum roteador real (modelos variam muito: alguns recusam a descrição, exigem porta
  igual, limitam o nome ou ignoram a concessão), CGNAT (UPnP não resolve), IPv6, NAT-PMP/PCP.
