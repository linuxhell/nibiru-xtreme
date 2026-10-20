# Retomada completa — Claude → ChatGPT (Nibiru-Xtreme + ADM + nibiru-m3u8)

Data: 2026-10-20. Objetivo do usuário: terminar o que o ChatGPT começou (ED2K/Kad), tornar o **Nibiru-Xtreme** o motor de download do ADM (HTTP, ED2K e Magnet/BitTorrent) no lugar do aria2-ultra, permitir **assistir vídeo enquanto baixa também em BitTorrent**, e só depois trocar o N_m3u8DL-RE pelo **nibiru-m3u8**. O usuário quer progresso prático, não relatórios; status alpha/beta deve continuar honesto (nada de "completo" sem teste).

## Repositórios e branches

| Repo | Branch de trabalho | Observação |
|---|---|---|
| `linuxhell/nibiru-xtreme` | `claude/amazing-newton-0qce70` | Motor. Nenhum PR aberto (usuário não pediu). |
| `linuxhell/apocalipse-download-manager` | `claude/amazing-newton-0qce70` | ADM (Tauri v2). PR #109 é rascunho do ChatGPT; não mexi nele. |
| `linuxhell/nibiru-m3u8` | `claude/amazing-newton-0qce70` | Release **v0.2.0** já publicada (Win/Linux/macOS x64). Não é o foco agora. |

Regras do ambiente: tag não pode ser enviada por push (proxy bloqueia) → releases saem por `workflow_dispatch` com input `tag` (já implementado nos workflows de release do nibiru-m3u8 e do nibiru-xtreme). Só um clone de repo anexado por vez no sandbox. `sleep` em foreground é bloqueado. Nunca usar `rm -rf *` com glob (bloqueado pelo harness).

## Estado do Nibiru-Xtreme (tudo testado, commitado e enviado)

### ED2K (concluído nesta etapa)
- Upload completo: `UploadManager` (slots, fila por prioridade+tempo, posição lembrada 30 min, limite de banda global, estatísticas por arquivo), `ShareRegistry` (cache de hash validado por caminho/tamanho/mtime, rescan, prioridades 0–4), publicação `OP_OFFERFILES` no servidor, listener entrante (`IncomingListener::bind_with_uploads`).
- Testes de socket real em `tests/ed2k_upload.rs` (6 testes).
- **ADM integrado** (`apps/desktop/src-tauri/src/ed2k_ui.rs`, `ed2k_ui/shares.rs`, UI `ed2k.html/.css/.js` com abas Compartilhados e Uploads, i18n en/pt-BR/zh-CN, `docs/ed2k-kad-native.md`, `tests/ed2k-window.test.cjs`). Nunca compartilha nada implicitamente; caminhos canonicalizados; sem symlink; sem `..`.
- **Faltando no ED2K:** Kad persistente (routing table, lookup, busca, publicação, teste de firewall), créditos/identificação segura, AICH, ofuscação, filtro de IP, mensagens, troca de fontes. Não anunciar capacidades não implementadas. Interoperabilidade real com aMule/eMule ainda **não** foi testada.

### BitTorrent (novo, do zero, sem libtorrent) — `src/bittorrent/`
Módulos: `bencode`, `metainfo` (v1, padding BEP47, sanitização de caminhos), `magnet`, `tracker` (HTTP+UDP, proteção SSRF: rejeita loopback/privado/link-local, DNS fixado, redirecionamento revalidado), `bitfield`, `message` (handshake, mensagens, BEP10 ext, BEP9 metadata, BEP11 PEX), `storage` (multi-arquivo, escrita posicional, anti-symlink, retomada por re-hash, delete), `picker` (rarest-first, janela sequencial para streaming, endgame, `claim_piece` para web seeds), `limiter` (token bucket + medidor), `torrent` (estado, verificação, choker, loop de tracker, loop de DHT, seeding por ratio/tempo, `read_file` para streaming), `peer` (sessão de peer, fast extension, upload, metadata, PEX), `session` (listener único, registro de torrents, limites globais, DHT opcional), `dht` (BEP5 IPv4: tabela de rotas, KRPC, tokens, lookup iterativo, announce, persistência por lista de nós), `webseed` (BEP19 com política SSRF), `stream` (servidor HTTP loopback com token, Range/HEAD/416, checagem do header Host contra DNS rebinding).
- **Assistir enquanto baixa:** `TorrentHandle::read_file` aponta o downloader para as peças pedidas; seek para o fim do arquivo (índice do MP4) funciona; arquivo não selecionado é selecionado ao dar play. Exposto como `nibiru.streamUrl(gid[, fileIndex])` (default: maior arquivo).
- Interoperabilidade **real com aria2c** (apt `aria2`): baixar do aria2c, baixar magnet/BEP9 do aria2c, aria2c baixando de nós (`tests/bittorrent_interop.rs`, pulam se aria2c ausente). Descoberto no teste: aria2 reenvia `bitfield` depois → aceitamos substituição.
- **Faltando:** uTP (BEP29), criptografia MSE/PE, BitTorrent v2/híbrido (BEP52), LSD (BEP14), DHT IPv6 (BEP32), BEP42 (IDs seguros), Metalink. Peers que exigem criptografia ou só falam uTP não são alcançados.

### RPC compatível com aria2 (`src/rpc.rs`, `src/rpc/bt.rs`, `src/main.rs`)
- Modo de lançamento compatível: `nibiru-xtreme --enable-rpc=true --rpc-listen-port=… --rpc-secret=… --stop-with-process=PID --listen-port=6881-6999 --seed-time=0 --seed-ratio=0.0 --bt-max-peers=… --enable-dht=… --dht-file-path=… --max-overall-download-limit=… --log=…` (outros argumentos aria2 são ignorados). `--stop-with-process` encerra o motor quando o ADM morre (`kill -0` no Unix, `tasklist` no Windows — **não compilado/testado no Windows localmente; o CI cobre a compilação**). Flag extra de teste: `--x-nibiru-allow-loopback-peers=true`.
- Métodos: `getVersion` (features incluem BitTorrent/Magnet), `addUri` (HTTP/FTP/SFTP/HTTP3/magnet), `addTorrent` (base64, `select-file` **1-based** como o aria2), `tellStatus` (com filtro de chaves; campos: status, totalLength, completedLength, uploadLength, downloadSpeed, uploadSpeed, connections, numSeeders, seeder, infoHash, errorCode/Message, dir, numPieces, pieceLength, verifiedLength, bittorrent{mode,announceList,info.name}, files[index 1-based, path, length, completedLength, selected "true"/"false", uris com web seeds]), `pause/forcePause/unpause`, `remove/forceRemove` (fica "removed" até `removeDownloadResult`), `changeOption` (max-download-limit, max-upload-limit, select-file), `changeGlobalOption` (max-overall-download/upload-limit), `getFiles`, `getPeers`, `getGlobalStat`, `tellActive`, `tellWaiting/tellStopped` (vazios), `nibiru.streamUrl`.
- Magnet `bt-metadata-only` + `bt-save-metadata`: grava `<infohash>.torrent` no `dir` (via arquivo temporário + rename), status vira `complete`, nenhum arquivo de payload é criado. Fluxo do ADM (metadata → `addTorrent` com select-file) testado de ponta a ponta com o binário real e com os argumentos exatos do ADM (script em `/tmp/e2e3/run.py` na sessão, não versionado).
- **HTTP via RPC** agora aceita `header` (lista), `referer`, `user-agent`, `all-proxy` (+`-user`/`-passwd`), `max-download-limit` por tarefa e global, `out`, `dir`. `split`/`max-connection-per-server`/`min-split-size` são aceitos mas o motor HTTP decide o número de workers sozinho (ainda não limitado pelo valor pedido). `load-cookies` (arquivo) não é suportado.
- ED2K continua **em processo** no ADM (via biblioteca), não via RPC; `addUri ed2k://` não existe no RPC.

### Testes (rodar com `cargo test --no-fail-fast`)
Tudo passa no Linux **exceto** `tests/http3_transfer.rs::http3_transfers_over_quic_and_rejects_an_untrusted_certificate`, que já falhava antes das minhas mudanças por falta de IPv6/QUIC no sandbox (ambiental). Suítes novas: `bittorrent_swarm` (13), `bittorrent_interop` (3), `dht_network` (3), `rpc_bittorrent` (5), `rpc_http_options` (4), `ed2k_upload` (6) + ~115 unitários. O teste do peer corrompido usa `127.0.0.2` e só roda em Linux (`cfg(target_os = "linux")`).

### CI do Nibiru-Xtreme
`ci.yml` roda fmt (Linux), `clippy --locked --all-targets --all-features -D warnings` e testes em ubuntu/windows/macos-15-intel. Falhas já corrigidas: lint novo `chunks_exact_to_as_chunks` (liberado em `src/lib.rs` com `allow(unknown_lints, …)`) e helper de teste `addr_on` morto fora do Linux. **Último run disparado (workflow_dispatch na branch) ainda não confirmado** — conferir com `actions_list` e `get_job_logs failed_only`. `release.yml` (novo): gera `nibiru-xtreme-<versão>-windows-x64.exe`, `-linux-x64` (musl estático), `-macos-x64` + `.sha256` (formato `hash  nome`), cria release com `RELEASE_NOTES.md` quando recebe input `tag` ou tag `v*`. **Ainda não foi executado**; antes de depender dele, rodar o build em PR/dispatch sem tag e confirmar que o musl compila.

## O que falta fazer (ordem sugerida)

1. **Confirmar CI verde nas 3 plataformas** do Nibiru-Xtreme; depois disparar `release.yml` com tag de pré-lançamento (ex.: `v0.2.0-alpha.2`) e verificar os 3 assets + sha256.
2. **Trocar o aria2 pelo Nibiru no ADM** (tarefa #21). Plano já mapeado:
   - `apps/desktop/src-tauri/src/aria2.rs` fala só RPC aria2; **mantém-se** (o Nibiru é compatível). `Runtime::spawn` passa os argumentos aria2 atuais — o Nibiru já os aceita.
   - `main.rs`: default de `aria2_release_repo` → `linuxhell/nibiru-xtreme` (hoje `linuxhell/aria2-ultra`/`FerroDownload/aria2-static-builds`; ajustar `migrate_aria2_release_repo` e o teste `aria2_release_source_migrates_deleted_default_and_preserves_custom_sources`); prefixo do asset no atualizador (`name.starts_with("aria2c-")` → `nibiru-xtreme-`, sufixos `windows-x64.exe`/`linux-x64`/`macos-x64` já coincidem); nome do executável `aria2c(.exe)` → `nibiru-xtreme(.exe)` (≈ linhas 3357, 7510, 10548, 10893, 15598); migração de configuração: se `aria2_path` apontar para `aria2c` e o novo binário existir, trocar (aria2 real ainda funciona como fallback); log `aria2.runtime_spawned` ("backend=aria2-ultra" → nibiru-xtreme).
   - UI (`apps/desktop/ui/app.js`, `index.html`, ~93 menções a aria2) e docs (`README`, `docs/*`): renomear rótulos para "Nibiru-Xtreme" mantendo ids internos.
   - CI/empacotamento do ADM: `.github/workflows/release.yml` (linhas ~30–210) e `test-build.yml` baixam aria2 de `FerroDownload/aria2-static-builds` com SHA-256 e instalam em `data/tools/aria2/aria2c`; trocar por download do release do Nibiru (pin de tag + SHA-256) e instalar em `data/tools/nibiru/nibiru-xtreme(.exe)`; ajustar `initial_settings.aria2_path` (linha ~15598).
   - Reaproveitar o fluxo de testes existentes `direct_download_rpc_flow_sets_destination_and_split_options` e `magnet_metadata_rpc_flow_persists_saved_torrent` (tarefa #14) apontando para o binário Nibiru.
   - Expor "assistir enquanto baixa" de torrents no ADM chamando `nibiru.streamUrl` e abrindo o player/preview com essa URL (o ADM já tem preview de vídeo parcial; ligar a isso).
   - Cuidado: o ChatGPT também edita o ADM — fazer `git fetch` e conferir conflitos antes de mexer em `main.rs`.
3. **Endurecer o Nibiru** (tarefa #19): MSE/PE (RC4 + DH), uTP, LSD, BitTorrent v2, limitar workers HTTP por `split`, `addUri` para `.torrent` por URL (`follow-torrent`), arquivo de sessão (`--save-session`/`--input-file` hoje ignorados; o ADM restaura as próprias tarefas).
4. **Kad** (tarefa #15) e itens restantes do ED2K; teste de interoperabilidade com aMule.
5. **Trocar N_m3u8DL-RE por nibiru-m3u8** no ADM (tarefa #16): o ADM baixa `nilaoda/N_m3u8DL-RE` (ver bloco `"n-m3u8dl-re"` do instalador de ferramentas, ~linha 10566). nibiru-m3u8 v0.2.0 já tem HLS, DASH, SAMPLE-AES, ClearKey, legendas, gravação de live com retomada, modo progressivo e subcomando `convert`; CLI diferente do N_m3u8DL-RE, então escrever o adaptador de argumentos e testes. DRM Widevine/PlayReady/FairPlay (obtenção de chaves) foi deliberadamente recusado como contorno; só chaves fornecidas pelo usuário.

## Restrições do usuário a preservar
Nunca compartilhar implícito; canonicalizar caminhos; sem symlink/`..`; no-clobber; limites de pacote/metadado/fila/timeouts; ADM deve abrir mesmo se ED2K falhar; não declarar "completo" sem teste; documentação em pt-BR, inglês e chinês simplificado onde já existe; avisar quando um release sair.

## Armadilhas aprendidas
- Cargo de teste do CI usa clippy mais novo que o do sandbox — avisos novos aparecem só lá.
- Banimento de peers é por IP; testes com dois "peers" na mesma máquina precisam de IPs de loopback distintos (só Linux).
- `Storage` mantém caches de handle separados para leitura e escrita (um handle somente-leitura em cache quebrava a escrita).
- `notify_one` compartilhado entre loops causava reannounce; há `tracker_wake`, `wake` e `stop_signal` separados.
- Teste de pausa/retomada exigiu devolver endereços de peers a `candidates` ao retomar.

## Monitorar o CI até o fim (instrução permanente)

O ChatGPT deve acompanhar o CI até ficar **verde nas 3 plataformas** e só então seguir para a etapa seguinte; não parar em "enviado".

1. Depois de cada push na branch `claude/amazing-newton-0qce70` do Nibiru-Xtreme, disparar `ci.yml` por `workflow_dispatch` (o push na branch não dispara sozinho; `ci.yml` só reage a `main`, `chatgpt/**` e PRs).
2. Listar o run mais recente da branch; se `in_progress`, esperar (sem `sleep` em foreground — usar lembrete agendado de 8–10 min) e reconsultar até `completed`.
3. Se `failure`: buscar os logs só dos jobs que falharam (`failed_only`), achar a causa real (clippy novo, teste dependente de SO, caminho com `\` no Windows, `127.0.0.2` no macOS/Windows etc.), corrigir, rodar localmente `cargo fmt --all`, `cargo clippy --locked --all-targets --all-features -- -D warnings` e `cargo test`, commitar, dar push e disparar de novo. Repetir até verde. Nunca desabilitar/pular teste só para passar.
4. Falha conhecida e **ambiental** (não bloqueia): `http3_transfers_over_quic_and_rejects_an_untrusted_certificate` no sandbox sem IPv6/QUIC. No CI do GitHub ela deve passar; se falhar lá, investigar.
5. Com o CI verde, disparar `release.yml` (input `tag` pré-lançamento) e conferir os 3 assets `nibiru-xtreme-<versão>-…` + `.sha256`. Só depois mexer no ADM para apontar para esse release.
6. O mesmo vale para o CI do ADM (`test-build.yml`/`release.yml` e testes Rust/Node) após cada mudança de troca de motor: monitorar até o fim e avisar o usuário quando o release do ADM sair.
7. Estado no momento desta nota: último run do Nibiru-Xtreme disparado às 12:33 UTC (commit "Test helper only used on Linux") ainda sem resultado confirmado; o Claude tem lembrete agendado para conferi-lo.
