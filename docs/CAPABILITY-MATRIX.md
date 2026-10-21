# Nibiru Xtreme — matriz de capacidades (21/10/2026)

Base inspecionada: branch publicada `work/native-rpc-ed2k` em `8a679994`, mais as
correções desta revisão. O commit `dd92f6ee` citado na retomada **não existe no
remoto**; se ele trouxer trabalho além de `8a679994`, esta matriz não o cobre.
Legenda: **T** = testado automaticamente aqui; **I** = interoperabilidade com cliente
independente aqui; **NT** = existe no código mas não foi exercitado aqui; **—** = ausente.
Nada abaixo foi medido em rede pública, com NAT real, nem em Windows ou macOS.

## Ambiente e gates (Linux x86-64, Rust 1.97, kernel sem IPv6)

| Gate | Resultado |
| --- | --- |
| `cargo fmt --all -- --check` | passou |
| `cargo clippy --locked --all-targets --all-features -- -D warnings` | **falhava** em 3 lints do Rust 1.97 (`src/rpc.rs`); corrigido |
| `cargo test --locked --all-targets --all-features` | 197 passaram, 1 ignorado (experimento de vazão H2, deve rodar em release sem carga), 0 falhas |
| `cargo build --locked --release` | passou |
| `tests/interop_libtorrent.py` (libtorrent 2.1.1.0, binário release) | 18 de 18 cenários passaram |
| `tests/bittorrent_interop.rs` com `aria2c` 1.37.0 | 3 de 3 passaram (sem `aria2c` eles se pulam em silêncio) |

Limites do ambiente que afetam a leitura:

- O kernel deste sandbox não tem IPv6 (`Address family not supported`). Três testes só
  de IPv6 (`encrypted_ipv6_tcp_transfer`, `encrypted_ipv6_utp_transfer`,
  `ipv6_nodes_bootstrap_announce_and_find_peers`) agora **se pulam com mensagem** nesse
  caso, em vez de falhar. Eles contam como "passou" na saída do Cargo mas **não
  exercitaram IPv6 aqui**. Só uma CI com IPv6 loopback prova essa parte.
- O teste HTTP/3 falhava aqui por um defeito real: o reqwest abre o socket QUIC em `[::]`,
  e em hosts sem IPv6 toda requisição HTTP/3 falhava, mesmo para servidor IPv4. O motor
  agora usa o curinga IPv4 nesses hosts. Servidores só IPv6 continuam exigindo IPv6.

## Protocolos de transferência direta

| Item | Situação | Evidência | Lacuna |
| --- | --- | --- | --- |
| HTTP/1.1, HTTPS, HTTP/2 | Existe; segmentação, retomada por Range, identidade (origem/ETag/Last-Modified), 200/206/416, publicação sem sobrescrever | **T** `http_segmented` (6), `http_identity` (5), `rpc_http_options` (5), `h2_tests` | Retomada após reinício do **ADM** depende de o consumidor recriar a tarefa; ver RPC |
| Cabeçalhos, referer, UA, cookies por cabeçalho, proxy HTTP, limite por tarefa e global, `split`/limites por servidor | Existe | **T** `rpc_http_options` | `load-cookies` (arquivo) não suportado |
| Autenticação HTTP (credenciais na URL / `Authorization`) | Aceita pelo cabeçalho `header` do RPC | **NT**: nenhum teste exercita autenticação HTTP (só FTP e SFTP têm testes de senha) | Escrever teste; Digest/NTLM não avaliados |
| HTTP/3 (QUIC) explícito, sem queda silenciosa | Existe | **T** `http3_transfer` (2) com servidor QUIC local e certificado não confiável rejeitado | Sem teste contra servidor HTTP/3 público; escolha automática de protocolo não existe (só `--http3-only`) |
| FTP (PASV/EPSV, proteção contra endereço de terceiros) | Existe | **T** `direct_protocols` (5): exatidão, falhas sem publicar, cancelamento com servidor parado, injeção de comando | **Sem retomada**; cada tentativa recomeça |
| FTPS explícito e implícito, validação de certificado | Existe | **T** `direct_protocols` | Sem retomada; sem teste contra servidor público |
| SFTP com fingerprint obrigatório | Existe | **T** `sftp_transfer` (1) | **Sem retomada**; autenticação só senha ou chave OpenSSH sem senha; sem agente/chave cifrada |
| WebDAV | **Só alias** de GET para `webdav(s)://` | código `engine.rs` | Coleções, locks, PROPFIND e escrita ausentes; documentar como "GET de arquivo único" |

## BitTorrent e peers

| Item | Situação | Evidência | Lacuna |
| --- | --- | --- | --- |
| BitTorrent v1: `.torrent`, magnet `btih`, metadata (BEP 9), PEX, fast, trackers HTTP/UDP, web seeds, seleção de arquivos, seeding, retomada por re-hash | Existe | **T** `bittorrent_swarm` (13), `rpc_bittorrent` (5); **I** aria2c (3) e libtorrent | Sem teste em swarm público |
| BitTorrent v2 / híbrido (BEP 52), magnet `btmh`, provas Merkle | Existe | **T** `bittorrent_v2` (7); **I** libtorrent v2 e híbrido (6 de 18 cenários) | Peça v2 sem hash autenticado fica indisponível (correto) |
| uTP | Existe (crate `librqbit-utp`) | **T** `bittorrent_transports`; **I** libtorrent (12 de 18 cenários) | Sem teste com perda/atraso de rede |
| MSE/PE (obfuscação legada) | Existe | **T**; **I** libtorrent | Não é autenticação |
| DHT IPv4 | Existe | **T** `dht_network` (3); magnet sem tracker via RPC | Sem teste em DHT público |
| DHT IPv6, BEP 42 | Existe no código | IPv6: **NT aqui** (teste se pula sem IPv6); BEP 42: **T** unitário com os vetores oficiais (`dht_security`) | Precisa de CI com IPv6 |
| Hole punching | **—** | `grep` sem ocorrência | Implementar só com interoperabilidade comprovada; sem stub anunciado |
| Descoberta local (LSD, BEP 14) | **—** | `grep` sem ocorrência | Pendente |
| Streaming de torrent (`nibiru.streamUrl`) | Existe | **T** `rpc_bittorrent::stream_url_…` (Range 206, seek) | — |
| Metalink | **—** | — | O ADM expande Metalink por conta própria hoje |

## ED2K e Kad

| Item | Situação | Evidência | Lacuna |
| --- | --- | --- | --- |
| `server.met`/`nodes.dat`, login por `OP_IDCHANGE`, busca e fontes no servidor | Existe | **T** unitários com fixtures TCP/UDP | Sem teste em servidor público |
| Download de peer (MD4 por partes, hashset, comprimido, 32/64 bits, retomada por re-hash, sem sobrescrever) | Existe | **T** `transfer::real_peer_download_…` | **Um peer por vez**; sem coordenação de partes entre fontes |
| Callback Low-ID por servidor | Parcial | **T** unitários | Exige solicitante High-ID e porta acessível |
| Upload, fila, slots, limite, compartilhamento explícito, publicação | Existe | **T** `ed2k_upload` (6) | Sem créditos |
| Créditos persistentes, identificação segura, AICH, ofuscação, source exchange | **—** no Xtreme | — | Filtro de IP (`ipfilter.dat`) agora existe no motor (`src/ed2k/ip_filter.rs`, **T** unitários) e é aplicado pela sessão Kad |
| Kad | **Sessão no motor** (`src/ed2k/kad_session.rs`, portada do ADM): tabela de roteamento limitada, HELLO v5 com desafio, lookup, fontes diretas (tipos 1/4), candidatos persistidos, filtro de IP; `kad.rs` segue como probe de bootstrap | **T** 54 testes ed2k (UDP real em loopback) | Sem ofuscação, sem callbacks de firewall/buddy, sem busca por palavra-chave nem publicação; **não validada contra a rede Kad pública nem contra aMule** |
| ED2K/Kad via RPC | **Existe**: `addUri ed2k://|file|…|/` com `serve --ed2k` (`src/rpc/ed2k.rs`); fontes = `x-nibiru-ed2k-source=ip:porta`, `--ed2k-server` e Kad (`--ed2k-nodes-file`); `tellStatus`/`pause`/`resume`/`remove`/`removeResult`/`tell*`/`getGlobalStat`, `nibiru.getKadStatus`; persistência no `--session-file` (restaura pausada, re-hash na retomada) | **T** `rpc::ed2k` (5) + `rpc_ed2k` (JSON-RPC real, reinício, 2 partes) — uploader é o próprio Nibiru | Um peer por vez; fontes Low-ID não usam callback pelo RPC; **nunca rodou contra rede ED2K/Kad real** |
| Interoperabilidade eMule/aMule | **Não testada** | — | Nenhum teste contra cliente independente de ED2K |

## RPC e contrato nativo

| Item | Situação | Evidência | Lacuna |
| --- | --- | --- | --- |
| `capabilities --json`, `serve`, namespace `nibiru.*`, identidade `nibiru-xtreme`, schema 1 | Existe | **T** `native_contract` (2) | — |
| Métodos: `getVersion`, `addUri`, `addTorrent`, `tellStatus`, `pause`, `resume`, `remove`, `removeResult`, `changeOption`, `changeGlobalOption`, `tellActive/Waiting/Stopped`, `getGlobalStat`, `getFiles`, `getPeers`, `streamUrl` | Existem; aliases `aria2.*` também | **T** `rpc_contract`, `rpc_bittorrent`, `rpc_http_options` | `getGlobalStat` agora devolve `numWaiting`/`numStopped`/`numStoppedTotal` reais (**T**); novo `nibiru.getSessionInfo` |
| Subconjunto usado pelo ADM `main` com aria2 (`addUri`, `addTorrent`, `changeOption`, `changeGlobalOption`, `pause`, `unpause`, `remove`/`forceRemove`, `removeDownloadResult`, `tellStatus`, `getVersion`) | Coberto pelo Xtreme | comparação de `aria2.rs` da `main` com `src/rpc.rs` | O ADM ainda passa opções que o Xtreme ignora (`--input-file`/`--save-session`) |
| Persistência da tabela de tarefas | **Existe** com `serve --session-file`: GIDs estáveis, tarefas restauradas pausadas, sem credenciais em disco, arquivo atômico e 0600 | **T** `rpc_session` (7): HTTP e torrent reiniciados, binário real morto com SIGKILL e recuperado, credenciais ausentes do arquivo, arquivo corrompido preservado, contadores | Só o namespace nativo; `--save-session`/`--input-file` do modo aria2 continuam ignorados; tarefas com credenciais não são restauradas (o ADM recria) |
| Limite de conexões por `split` | Existe | **T** `rpc_http_options::split_and_server_caps_…` | — |
| Encerrar com o processo pai (`--stop-with-process`) | Existe no Unix (`kill -0`) e Windows (`tasklist`) | Unix: **NT** direto; Windows: nunca executado | Validar no Windows |
| Logs JSON Lines sem segredos | Existe `redact_url` (`engine.rs`) | **NT**: nenhum teste verifica a saída de log; não li todos os pontos de log | Teste de ausência de token, senha e query na saída |

## O que vem a seguir (ordem de prioridade da retomada)

1. ~~Persistência real das tarefas no RPC e contadores globais reais~~ (feito nesta revisão, ver `docs/protocols.md`).
2. ~~Mover a sessão Kad para o motor e expor ED2K/Kad no RPC~~ (feito; resta coordenar várias fontes/partes por arquivo e validar contra aMule/rede real).
3. Retomada em FTP/FTPS/SFTP.
4. Hole punching e LSD **somente** com teste de interoperabilidade.
5. CI do Xtreme em Windows e macOS com IPv6; desempenho contra o aria2-ultra que o ADM usa.
