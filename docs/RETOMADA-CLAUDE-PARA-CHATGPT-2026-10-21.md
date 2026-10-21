# Retomada: o que o Claude fez até aqui, para o ChatGPT assumir (21/10/2026)

Escrito pelo Claude a pedido do usuário (Juliano Vieira de Sá). O ChatGPT assume daqui. Depois o usuário
volta ao Claude, que continua de onde o ChatGPT parou. **Leia tudo antes de editar.** Este arquivo é fiel
ao que foi feito e testado; onde algo **não** foi provado, está dito. Ele não declara nada como "pronto"
além do que as evidências abaixo sustentam.

Vocabulário usado em todo o projeto (não misture):

- **Compilado** = o código compila.
- **Testado automaticamente** = há teste que passa em CI ou localmente (diga onde).
- **Validado em uso real** = rodou com cliente/servidor/rede de verdade. **Quase nada do Xtreme está neste nível.**

---

## Atualização do ChatGPT — auditoria posterior no mesmo dia

A partir da retomada acima, o ChatGPT continuou a revisão na branch
`audit/code-review-2026-10-21`. **Esta branch passa a ser a fonte de verdade para a
auditoria atual**; as referências abaixo à cabeça da branch do Claude são históricas.

Correções publicadas nesta continuação, em ordem lógica:

- `d80ba615`: corrige `CryptStream::poll_write` ED2K sob escrita parcial/backpressure; o estado RC4
  real só avança pelos bytes efetivamente aceitos pelo writer.
- `7dd8422d`: `change_option` valida `select-file` antes de persistir/aplicar mudanças, evitando
  mutação parcial em pedido inválido.
- `0ed80cf0`: fecha a corrida entre `existing()` e `register()`; uma instância duplicada de torrent
  criada em concorrência é explicitamente encerrada.
- `566431cf`: serializa comparação, substituição atômica e cache de `SessionStore::save`, impedindo
  snapshot antigo de vencer corrida de gravação.
- `d91dd1b6`: tokens DHT expiram também em validação, mesmo sem novos `get_peers`.
- `c7d89502`: exclusão de storage BitTorrent passa a ser terminal; writes atrasados não recriam arquivos
  depois de `delete_files()`.
- `a120bca4`: upload ED2K recusa arquivo compartilhado modificado com mesmo tamanho se o mtime mudou.
- `929695e3` e `2f48825d`: `Referer` e URLs diretas com query deixam de ser persistidos no session file.
- `1d9536bc`, `c344e8ea`: sidecar HTTP de retomada grava fingerprints SHA-256 das URLs; sidecars legados
  em claro só são aceitos com correspondência exata e são migrados imediatamente.
- `e0972fbe`, `02d7205c`: GitHub Actions do CI/release fixadas em revisões imutáveis.
- `2f7643ce`: CI cancela execuções superadas da mesma ref.
- `fa167f70`, `0b122c61`, `2dde1ff6`, `10f6cd64`: documentação, release notes e ignore de sidecars
  alinhados ao comportamento auditado.

Evidência importante já observada nesta continuação:

- O head `d91dd1b6` concluiu CI completo com sucesso nas três plataformas.
- A migração HTTP em `c344e8ea` passou `fmt`, Clippy, a suíte Rust no Ubuntu, a interoperabilidade
  BitTorrent e a interoperabilidade ED2K/aMule; no último ponto observado, o release build ainda rodava
  e os jobs Windows/macOS ainda estavam em andamento.
- Uma falha anterior do aMule em `d80ba615` foi intermitente: a mesma correção passou em execução posterior
  e, depois, no CI cumulativo.
- `Cargo.lock` v4 não contém dependências `git+`; builds usam `--locked`.
- A revisão dedicada cobriu todos os módulos de produção Rust listados em `src/`; buscas por
  `TODO`, `FIXME`, `todo!`, `unimplemented!` e `panic!` não encontraram pendências de produção
  explícitas. Isso **não equivale a validar rede pública, NAT real ou uso real Windows/macOS**.

Antes de merge/tag/release, confirme o CI do head atual da branch
`audit/code-review-2026-10-21`. Não houve autorização para merge em `main`, criação de tag ou release.

## 1. Decisão vigente do usuário (prevalece sobre qualquer plano antigo)

1. O **ADM continua como está**. Só o **aria2-ultra** será substituído pelo **Nibiru Xtreme**, e **só depois**
   de o Xtreme estar concluído e validado. **Não existe release do Xtreme** (o repositório não tem nenhuma
   publicada), então a troca **não foi feita** e não deve ser apresentada como feita.
2. **Permanecem como estão** no ADM: **yt-dlp (nightly)**, **N_m3u8DL-RE (v0.6.0-beta)**, **FFmpeg**, **QuickJS
   (`qjs`)**, extrator (7-Zip) e player (mpv). Não substituir por Nibiru M3U8/Power/Colapse. O **Nibiru Colapse
   está encerrado** (guardar como histórico; sem novos motores paralelos). **Não alterar o repositório
   `nibiru-colapse`.**
3. Prioridade: terminar o **Xtreme** (HTTP/1.1/2/3, HTTPS, FTP/FTPS/SFTP, WebDAV, BitTorrent v1/v2/híbrido/magnet,
   DHT v4/v6, PEX, uTP, hole punching quando interoperável, ED2K, Kad), com contrato nativo
   (`capabilities --json`, `serve`, namespace `nibiru.*`).
4. Ordem de execução acordada: **A** recuperar → **B** matriz de capacidades → **C** fechar lacunas →
   **D** testes independentes → **E** interoperabilidade/multiplataforma → **F** trocar o aria2 no ADM →
   **G** regressão + limpeza + entrega reprodutível. Estamos no meio de **C/D/E**.
5. Regras que o usuário repetiu e que valem sempre:
   - Não publicar release final nem anunciar conclusão enquanto houver requisito pendente.
   - Não usar comandos globais como `git clean -fdx`. Preservar downloads reais do usuário (inclusive `.part`,
     parciais, sidecars e checkpoints), histórico, licenças e testes.
   - Limpeza de lixo **comprovado** só em commits separados. Não manter pacotes redundantes por precaução.
   - Prosseguir sem pedir confirmação de rotinas já autorizadas; se faltar algo que realmente não existe
     (código/objeto/credencial), dizer **exatamente** o que falta.
   - Publicação externa (release, tag, comentário em PR) **só com autorização explícita**.
6. Pedido novo do usuário para a aba **Ferramentas do ADM** (ainda **não implementado**), ver seção 6.

---

## 2. Onde está cada coisa

### 2.1 Repositório do motor — `linuxhell/nibiru-xtreme`

- Branch de trabalho do Claude: **`claude/xtreme-completion`** (push feito; cabeça no momento desta escrita:
  `6c351f1` + o commit que adiciona este arquivo). Base publicada anterior: `work/native-rpc-ed2k` em
  `8a679994` (o commit `dd92f6ee` citado numa retomada antiga **não existe no remoto**; se o usuário tiver
  trabalho extra nele, precisa fornecer um bundle/branch).
- Outras branches existentes: `chatgpt/protocol-completion`, `chatgpt/protocols-ed2k-kad`,
  `chatgpt/windows-test-artifacts`, `claude/amazing-newton-0qce70`, `main`. Não verifiquei se divergem do
  que está na minha branch além do já listado; **antes de mesclar, compare**.
- Clone local usado pelo Claude: `/home/user/nibiru-xtreme` (efêmero; o que vale é o remoto).

Commits desta sessão sobre `8a679994`, em ordem (cada um passou fmt/clippy/testes locais antes do push):

| Commit | Assunto |
| --- | --- |
| `cf60d5b` | Corrige 3 lints do clippy (Rust 1.97) |
| `7ba0ba4` | HTTP/3 em host sem IPv6 (corrige bug real: reqwest abria o QUIC em `[::]`); testes só-IPv6 se pulam com mensagem; matriz de capacidades |
| `7e3d53b` | Corrige 3 linhas da matriz que afirmavam mais evidência do que havia |
| `4053d2f` | **Persistência de tarefas no RPC** (`serve --session-file`), contadores globais reais, `nibiru.getSessionInfo` |
| `028682f` | Documentação do contrato de persistência |
| `5593b47` | Teste de reinício HTTP determinístico (origem devolvia 200 a Range impossível; agora 416) |
| `bfe103d` | **Sessão Kad2 + filtro de IP portados do ADM para o motor** (`src/ed2k/kad_session.rs`, `ip_filter.rs`) |
| `62c455c` | **`addUri ed2k://` no RPC** (`src/rpc/ed2k.rs`), persistência de tarefas ED2K, `nibiru.getKadStatus` |
| `892a37f` | **Download ED2K de várias fontes** ao mesmo tempo (até 4 peers; parte reservada a um peer) |
| `c6c0498` | Testes do Kad portáteis para macOS (sem 127.0.0.2, sem datagrama >9216) |
| `b75b982` | **Retomada em FTP/FTPS/SFTP** (parcial + validador, REST/seek, comparação de 8 MiB) |
| `026e681` | **LSD (BEP 14)**, opt-in, verificado contra libtorrent |
| `f0fa7b0` | **Hole punching (BEP 55)** nos 3 papéis, verificado contra libtorrent nos dois sentidos de relay |
| `23d0933` | Corrida no teste do Kad (primeira passada de manutenção) |
| `4ae4fb6` | **Ofuscação ED2K TCP** cliente↔cliente + `tests/interop_amule.py` (aMule real) |
| `1d1314a` | **Ofuscação UDP do Kad** + callbacks Low-ID por servidor no RPC + `tests/kad_amule.rs` |
| `d93ed0a` | Fila de upload do par pode ficar em silêncio (limite de 5 min no total) |
| `d4b63f4` | **Callbacks Kad via buddy** (lado solicitante) |
| `6c351f1` | Ajustes de clippy |

### 2.2 Repositório do aplicativo — `linuxhell/apocalipse-download-manager` (ADM)

- `main` em **`a945667a`** ("Release 0.6.1: signed Firefox XPI 0.3.206…"). Não mudou durante a sessão.
- Branch designada ao Claude: **`claude/optimistic-heisenberg-cozrt2`**, cabeça **`074f7a42`**, 29 commits
  à frente de `main`, baseada no checkpoint `8ababf01` (`work/nibiru-engines-ed2k-audit`). É a **pesquisa/
  auditoria** feita no começo da sessão. **Não serve como base da migração**, porque:
  - tem o composto de 4 motores (Xtreme + Nibiru M3U8/Power/Colapse) e despacho entre eles;
  - **removeu o N_m3u8DL-RE** (commits `3579e23d`, `06439bc9`), contra a diretriz atual.
  Contém, porém, correções **independentes de motor** que valem cherry-pick para uma branch limpa a partir de
  `main` (ver seção 5). Documento-índice: `docs/AUDITORIA-ADM-CONTINUACAO-2026-10-21.md` na branch.
- Branch do ChatGPT para a UI ED2K/Kad do ADM: **`origin/chatgpt/ed2k-kad-client`** (4 commits recentes
  sobre `main`; não revisei o conteúdo a fundo nesta etapa).
- **CI do ADM falha em todas as plataformas** nessa branch por causa **real e documentada**: o `Cargo.lock` e
  `scripts/nibiru-engines.json` fixam revisões de motores (`dd92f6ee` e outras) que **não existem no remoto**.
  Isso some quando o ADM deixar de depender dos motores compostos.
- Clone local: `/home/user/apocalipse-download-manager` (efêmero).

---

## 3. Estado do Xtreme com evidência

A fonte de verdade detalhada é **`docs/CAPABILITY-MATRIX.md`** (linha a linha, com T/I/NT e lacunas) e
**`docs/protocols.md`** (formatos de fio e decisões). Resumo honesto:

### 3.1 O que existe e está **testado automaticamente**

- HTTP/1.1, HTTPS, HTTP/2 (segmentação, retomada por Range e identidade), HTTP/3 explícito (servidor QUIC local).
- **FTP/FTPS (explícito e implícito) e SFTP, agora com retomada**: parcial ao lado do destino
  (`.<nome>.nibiru-part` + `.json` com URL sem credenciais/tamanho/mtime), `REST`/`seek`, relê e compara os
  últimos 8 MiB, recomeça uma vez se o servidor não continuar o mesmo conteúdo.
- BitTorrent v1/v2/híbrido, magnet, DHT v4 (v6 no código, sem prova aqui), PEX (com flags de uTP e hole punching),
  uTP, MSE, web seeds, streaming (`nibiru.streamUrl`).
- **LSD (BEP 14)** opt-in: `--enable-lpd [--lpd-interface IP]`; só IPv4, uma interface.
- **Hole punching (BEP 55)** sobre uTP: relay, iniciador e alvo; desempate de conexões simultâneas pelo menor
  peer ID. Flag de teste `SessionConfig::emulate_restricted_nat` (nunca exposta pela CLI).
- **RPC compatível com aria2** + namespace nativo `nibiru.*` + persistência (`--session-file`), contadores
  reais, `nibiru.getSessionInfo`, `nibiru.getKadStatus`, `nibiru.getEd2kStatus`.
- **ED2K**: download MD4 por partes (hashset, compressão, 32/64 bits, retomada por re-hash, sem sobrescrever),
  **várias fontes por arquivo**, **upload** com fila (biblioteca), servidor (`server.met`, login, busca, fontes
  incl. `OP_FOUNDSOURCES_OBFU`), **ofuscação cliente↔cliente** (modos `off|prefer|require`), **callbacks
  Low-ID por servidor** (RPC) e **por buddy do Kad** (solicitante), sessão **Kad2** no motor com **ofuscação UDP**
  e filtro de IP.

### 3.2 O que foi **validado contra software independente real** (só em loopback)

| O quê | Contra quê | Onde está o teste |
| --- | --- | --- |
| BitTorrent v1/v2/híbrido, TCP/uTP, cifrado ou não, 2 sentidos | libtorrent 2.1.1 | `tests/interop_libtorrent.py` (18 cenários) |
| LSD nos 2 sentidos | libtorrent 2.1.1 | mesmo script (2 cenários) |
| Hole punching: libtorrent como relay → Nibiru disca; Nibiru como relay → libtorrent disca | libtorrent 2.1.1 + par cru em Python | mesmo script (2 cenários) |
| Download ED2K de 10,5 MB em 2 partes, simples e **com o aMule exigindo ofuscação** (conexão simples recusada) | **aMule 2.3.3** (`amuled`) | `tests/interop_amule.py` |
| Sessão Kad verificando o aMule com pacotes **ofuscados nos dois sentidos** | aMule 2.3.3 com Kad ligado em loopback | `tests/kad_amule.rs` |
| BitTorrent contra aria2c 1.37 | `aria2c` (se instalado) | `tests/bittorrent_interop.rs` (se pula em silêncio sem `aria2c`) |

### 3.3 O que **não** foi validado em uso real (não afirme o contrário)

- Nenhum teste tocou a **rede ED2K/Kad pública**, **trackers/DHT públicos**, **NAT real**, **LAN com vários hosts**,
  **servidores FTP/SFTP independentes**, nem rodou **em uso real no Windows/macOS** (CI compila e testa lá, mas
  isso é "testado automaticamente", não "validado em uso real").
- O formato do `KADEMLIA_CALLBACK_REQ` (e a ambiguidade do layout do hash do buddy) vem da **leitura** do código
  do aMule; **nenhum nó real o aceitou**. A mesma ressalva vale para o parser de fontes Kad tipo 3/5.
- O Kad passou a anunciar **versão 6** (primeira com ofuscação UDP). Só foi exercitado com um aMule local.

---

## 4. Como reproduzir o ambiente de teste (o Claude usou isto)

```bash
# gates do motor (Linux)
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test  --locked --all-targets --all-features     # ~280 testes, 1 ignorado (vazão H2)
cargo build --locked --release
cargo check --target x86_64-pc-windows-gnu --all-targets   # verificação cruzada para Windows

# independentes (precisam dos programas instalados)
python -m pip install libtorrent==2.1.1
cargo build --locked --bin nibiru-xtreme
python tests/interop_libtorrent.py --binary target/debug/nibiru-xtreme

sudo apt-get install -y --no-install-recommends amule-daemon amule-utils     # aMule 2.3.3
python tests/interop_amule.py --binary target/debug/nibiru-xtreme
cargo test --locked --test kad_amule -- --nocapture
```

O workflow `.github/workflows/ci.yml` (Ubuntu, Windows, macOS; `workflow_dispatch`) roda os gates e, no Ubuntu,
os três scripts independentes acima. Windows ainda empacota `nibiru-windows-x64` como artefato.

**Estado da CI** (branch `claude/xtreme-completion`):

- Run 84 (`23d0933`): **verde nas 3 plataformas** (inclui LSD, hole punching, retomada FTP/SFTP, várias fontes).
- Runs 81/82/83 falharam por uma corrida num teste do Kad (já corrigida). Run 85 (`4ae4fb6`) falhou só no
  passo do aMule no Ubuntu (timeout de 30 s esperando a fila do aMule; corrigido em `d93ed0a`).
- **Pendente no momento da escrita**: os runs de `d93ed0a` e `6c351f1` (e do commit deste arquivo). **Confira os
  resultados antes de qualquer coisa**, e leia os logs dos jobs que falharem (`get_job_logs failed_only`).

**Armadilhas que custaram tempo (poupe-se delas):**

1. **aMule**: o `preferences.dat` guarda um hash, mas o aMule troca os bytes 5 e 14 em memória; a chave de
   ofuscação usa o hash **anunciado** (peça-o com um hello simples). `amulecmd` retorna exit 0 mesmo sem
   conseguir conectar: confira "Operation was successful" no stdout. A fila do aMule não concede slot a cliente
   novo por ~30 s.
2. **Kad UDP**: `ENDIAN_SWAP_*` do aMule é **no-op em little-endian**, então magic e verify keys vão em
   **little-endian**. Ler como big-endian gera pacote que o aMule descarta em silêncio. Hash do arquivo no Kad
   usa o inverso de cada grupo de 4 bytes.
3. **Rust**: spawnar tarefa de peer dentro de tarefa de peer quebra a prova de `Send` (ciclo de tipos assíncrono);
   o código enfileira em `core.holepunch_queue` e o laço principal faz o `spawn`.
4. **macOS**: não há `127.0.0.2`; datagrama UDP > 9216 B falha no envio. Testes avisam e pulam esses trechos.
5. **Ambiente**: o disco da sessão é uma cota fixa; ao ver "no space left", apague `target/debug/incremental`.
   `rm` com glob relativo depois de `cd` é bloqueado pelo harness.
6. **Nunca** pule/desligue teste para ficar verde. Falha de plataforma real vira **skip explícito com mensagem**.

---

## 5. O que fazer — pendências, em ordem de prioridade

### P0 — Confirmar a base
1. Ver o resultado da CI dos commits `d93ed0a`, `6c351f1` e deste arquivo nas 3 plataformas; corrigir causa real.
2. Comparar `claude/xtreme-completion` com `chatgpt/protocol-completion`, `chatgpt/protocols-ed2k-kad` e
   `work/native-rpc-ed2k` para não perder trabalho de ninguém; mesclar de forma que o usuário aprove.

### P1 — Terminar o Xtreme (lacunas conhecidas)
**ED2K / Kad** (a matriz tem as linhas exatas):
- Ofuscação **cliente↔servidor** (DH 768, `EncryptedStreamSocket` modo servidor) — **não existe**.
- Kad: **busca por palavra-chave**, **publicação** de fontes/arquivos, **ACK com chave (v8)**, **teste de firewall**
  (descobrir se estamos atrás de firewall TCP/UDP), **ser buddy** (aceitar e retransmitir callbacks), servir
  bootstrap (hoje não serve, por risco de reflexão), NAT-T de servidor.
- ED2K: créditos persistentes, identificação segura, AICH, troca de fontes (source exchange), UDP de servidor
  (busca global), **dividir uma parte entre peers** (hoje a menor unidade por peer é a parte inteira), ranking de
  fontes.
- **Compartilhar arquivos pelo RPC/CLI** (`ShareRegistry`/upload existem na biblioteca; o serviço RPC não expõe).
- Validar contra **servidor ED2K e rede Kad reais** (exige rede pública; o usuário precisa autorizar/executar).

**Transferências diretas:**
- Autenticação HTTP (Basic/Digest) **sem teste**; `load-cookies` não suportado; escolha automática HTTP/2↔3 não
  existe (só `--http3-only`).
- **WebDAV é só alias de GET**; coleções/PROPFIND/escrita ausentes. **Metalink ausente** (o ADM hoje expande sozinho).
- Retomada em FTPS **implícito** sem teste próprio; SFTP só senha/chave sem frase (sem agente/chave cifrada).
- Modo aria2 do binário ignora `--input-file`/`--save-session` (o contrato nativo tem `--session-file`).
- Log sem segredos: existe `redact_url`, **falta teste** que prove ausência de token/senha/query na saída.

**BitTorrent:**
- LSD só IPv4/uma interface; hole punching só uTP e nunca atravessou NAT real; papel de iniciador provado
  só entre sessões Nibiru; DHT/IPv6 só com CI que tenha IPv6.

**Plataformas e desempenho:**
- `--stop-with-process` no Windows nunca foi executado de verdade (`tasklist`).
- CI com **IPv6 loopback** (Windows/macOS) para provar DHT v6/uTP v6.
- **Medir desempenho contra o aria2-ultra que o ADM usa** (HTTP segmentado, torrent, ED2K).
- Empacotar **Linux e macOS** (hoje só o Windows x64 sai como artefato).

### P2 — Entrega reprodutível do Xtreme (necessária antes de o ADM poder baixar o motor)
- **Não há release.** O usuário decidiu que ainda não há release completa. Quando houver: tag, binários por
  plataforma (`windows-x86_64`, `linux-x86_64/aarch64`, `macos-x86_64/aarch64`), `SHA-256` por arquivo e
  `tools-manifest.json` assinado compatível com `TRUSTED_TOOL_UPDATE_KEYS` do ADM. Definir **nomes de asset
  estáveis** — a aba Ferramentas do ADM depende deles (seção 6). **Publicar só com autorização do usuário.**
- Atualizar `THIRD_PARTY_NOTICES.md` (crates novas: `md5`, `socket2`) e README.

### P3 — Integração no ADM (depois que o Xtreme estiver concluído)
Passos F e G da ordem acordada. Em resumo:
1. Criar **branch limpa a partir de `main`** (não da branch de pesquisa) — **o usuário ainda não autorizou o
   nome/push**; ele ignorou a pergunta e depois lembrou que não há release. Pedir de novo quando chegar a hora.
2. Trazer por cherry-pick **só** as correções independentes de motor da branch de pesquisa (lista abaixo).
3. Trocar o slot **aria2 → nibiru-xtreme** (RPC, runtime, processo supervisionado, configurações antigas
   migradas sem perder dados), mantendo yt-dlp/N_m3u8DL-RE/FFmpeg/qjs/extrator/player.
4. Auditar chamadas indiretas ao aria2 (yt-dlp `--downloader`, scripts), regressão completa, limpeza em commit
   separado, entrega reprodutível.

Correções da branch de pesquisa que valem cherry-pick (independentes do motor; **conferir um a um**, pois
algumas tocam código que o checkpoint já mudou): perda de arquivo na atualização de ferramentas (SHA-256/identidade/
versões incompatíveis/executável ausente), tolerância a fila e configurações antigas (carregar campo a campo),
Link: fuga por symlink e limitação de tentativas de autenticação, ponte da extensão (uploads abandonados/falhos,
publicação sem sobrescrever, `pause` morto), UI (i18n pt/en/zh, contraste, mensagens), supervisão de processos
(grupo de processos/Job Object), filtro de IP ED2K, ferramentas de evidência de build. **Não** levar: remoção do
N_m3u8DL-RE, despacho M3U8/Power/Colapse, slots Nibiru extras.

---

## 6. Pedido do usuário para a aba Ferramentas do ADM (não implementado)

Diagnóstico feito na `main` (`apps/desktop/src-tauri/src/main.rs`, `get_tool_statuses`, `download_tool`,
`update_tool`, `apps/desktop/ui/app.js`):

- A aba lista ffmpeg, yt-dlp, qjs, n-m3u8dl-re, **aria2**, extractor e player. O slot do **nibiru-xtreme ainda
  não existe na `main`**.
- **Baixar** já baixa o mais recente e **preenche o campo do caminho** (o usuário só clica em Salvar). Nos
  motores em pasta, apaga **só a subpasta daquele motor** (`tools/<id>`), nunca a pasta `tools` inteira.
- **Atualizar** troca só o executável daquele motor (com backup), mas **baixa o arquivo inteiro antes de saber se
  há versão nova** (compara depois) e, **sem motor no caminho**, falha com `tool_target_has_no_directory`.

Comportamento que o usuário pediu:

1. Slot do **nibiru-xtreme no lugar do aria2**; os demais **permanecem como estão**.
2. "Atualizar" deve **realmente verificar** se há versão nova (consultar a tag/versão antes de baixar).
3. Ao atualizar, **remover só aquele motor** (nunca todos da pasta) e colocar o novo no lugar.
4. Se **não houver nenhum motor no caminho**, **baixar o mais recente**, salvar na pasta `tools` e **preencher o
   caminho** automaticamente, bastando o usuário clicar em **Salvar**.

Bloqueios: (a) **não existe release do Xtreme** para baixar; (b) o nome da branch/push para esse trabalho não
foi autorizado. Sugestão do Claude: implementar 2–4 para todos os motores atuais e deixar o slot Xtreme pronto
(apontando para as releases de `linuxhell/nibiru-xtreme`), **mantendo o aria2 como padrão** até existir release
completa; testar com servidor de release simulado.

Observação sobre o `qjs`: o ADM passa `--js-runtimes quickjs:<caminho>` ao yt-dlp (inspeção e download). O yt-dlp
precisa de algum runtime JS para resolver desafios do YouTube; sem ele pode falhar ou vir com menos formatos.
Na **inspeção** o ADM passa o caminho mesmo se o `qjs` não existir; no download só se o arquivo existir.

---

## 7. Mapa do código novo/alterado no Xtreme (para localizar rápido)

| Área | Arquivos |
| --- | --- |
| RPC ED2K, persistência, status | `src/rpc/ed2k.rs`, `src/rpc.rs`, `src/rpc/session.rs` |
| Kad | `src/ed2k/kad_session.rs`, `src/ed2k/kad_crypto.rs` (ofuscação UDP), `src/ed2k/ip_filter.rs`, `src/ed2k/kad.rs` (só bootstrap) |
| ED2K TCP | `src/ed2k/obfuscation.rs`, `peer.rs`, `listener.rs` (callbacks, registro por id e por hash), `transfer.rs` (várias fontes), `server.rs` (fontes `FileSource`, `Buddy`, `OP_FOUNDSOURCES_OBFU`), `upload.rs`, `share.rs` |
| FTP/SFTP retomada | `src/protocols/streaming.rs` (`StagedFile::resumable`), `ftp.rs`, `sftp.rs` |
| BitTorrent | `src/bittorrent/lsd.rs`, `peer.rs` (`on_holepunch`, `holepunch_connect`), `message.rs` (`Holepunch`, PEX com flags), `session.rs`, `torrent.rs` |
| CLI | `src/main.rs` (flags `--ed2k*`, `--enable-lpd`, `--session-file`); `capabilities --json` lista `ed2k_rpc`, `ed2k_multi_source`, `bt_lsd`, `bt_holepunch`, `ftp_resume`, `sftp_resume` |
| Testes novos | `tests/rpc_ed2k.rs`, `bittorrent_lsd.rs`, `bittorrent_holepunch.rs`, `kad_amule.rs`, `direct_protocols.rs`/`sftp_transfer.rs` (retomada), `ed2k_upload.rs` (ofuscação), scripts `interop_*.py` |
| Docs | `docs/CAPABILITY-MATRIX.md`, `docs/protocols.md` (formatos: retomada, LSD, BEP 55, ofuscação TCP e UDP) |

Opções de `serve` relevantes: `--port --secret --dir --session-file`, `--enable-lpd --lpd-interface`,
`--ed2k --ed2k-server host:porta --ed2k-nodes-file nodes.dat --ed2k-state-dir --ed2k-tcp-port
--ed2k-obfuscation off|prefer|require`. Opções por tarefa ED2K: `x-nibiru-ed2k-source=ip:porta[#userhash]`,
`x-nibiru-ed2k-obfuscation`. Exemplo para o usuário testar:

```
nibiru-xtreme serve --port 6800 --secret SENHA --dir C:\Downloads ^
  --session-file C:\Downloads\xtreme-session.json --ed2k --ed2k-obfuscation prefer
```

---

## 8. Decisões que dependem do usuário (não decida por ele)

1. **Branch e push** do trabalho no ADM (Ferramentas + migração): nova branch a partir de `main`, ou reescrever a
   branch designada, ou camadas por cima. (Recomendação do Claude: nova branch a partir de `main`.)
2. **Publicar uma pré-release do Xtreme** (binários da CI + SHA-256) para facilitar o teste dele? Ele disse que
   ainda não há release completa e está ansioso para testar; hoje o caminho é o artefato `nibiru-windows-x64` da
   CI (exige login no GitHub) ou compilar.
3. Fornecer o commit `dd92f6ee` do Xtreme (bundle/branch) se ele tiver trabalho que não está no remoto.
4. Autorizar (ou executar) qualquer teste na **rede pública** (ED2K/Kad/DHT/trackers) — o Claude não fez nenhum.

---

## 9. Como o Claude retoma depois

O usuário volta ao Claude quando o ChatGPT terminar. Para o Claude continuar sem retrabalho, ao final do seu
trabalho **atualize esta seção** com: commits novos (hash + assunto), resultado da CI por plataforma, o que mudou
na matriz, o que ainda está pendente e qualquer decisão do usuário. Se algo ficou pela metade, diga exatamente
em que arquivo/linha e por quê.

Pendências de acompanhamento deixadas pelo Claude (podem ter expirado): verificações agendadas da CI do Xtreme
(runs de `d93ed0a`/`6c351f1`).

*Gerado pelo Claude em 21/10/2026, com o repositório em `claude/xtreme-completion`.*
