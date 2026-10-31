[Português](README.md) · [English](README.en.md) · [简体中文](README.zh-CN.md)

# Nibiru Xtreme

A versão 0.2.3 permite informar o nome exato do arquivo ou da pasta de conteúdo de um torrent por RPC (`dir` + `out`). Isso evita uma segunda pasta com o mesmo nome no ADM e mantém nomes reservados para evitar conflitos. O caminho continua limitado a um nome portável; sem `out`, o layout padrão permanece igual.

A versão 0.2.2 corrige nomes de torrents após truncamento, limita nomes Unicode por bytes e unidades UTF-16, distingue arquivos que diferem apenas em maiúsculas/minúsculas e impede colisões entre arquivo e diretório. ED2K e RPC HTTP recusam nomes não portáveis. `$` e apóstrofos legítimos são preservados. Web seeds continuam usando os nomes originais da rede. Veja a [auditoria de 27/10](docs/AUDIT-2026-10-27.md).

Motor de downloads em Rust, com contrato nativo JSON-RPC e um subconjunto de compatibilidade com aria2.

O HTTPS usa as raízes públicas e os certificados confiáveis do sistema, mantendo a validação TLS. Erros RPC distinguem falhas de confiança no certificado sem expor URLs ou credenciais.

## Estado

**Versão estável 0.2.3.** Motor nativo do Apocalipse Download Manager, distribuído para Windows, Linux e macOS com arquivos SHA-256.

A prévia ED2K não depende de chegar a 30 ou 40 MB no total: o ADM libera o botão quando a primeira parte está autenticada por MD4 (até 9.728.000 bytes). Outros peers ajudam a terminar o início e o fim do arquivo antes das partes do meio, evitando que um peer lento retenha a prévia. MP4/AVI podem precisar também do índice no fim; trechos ainda ausentes aguardam dados, sem fornecer buracos do arquivo ao player.

O teste enviado pelo usuário em 22/10/2026 confirma tráfego ED2K público, UPnP com ID alto e repetição automática das tentativas no ADM. Nesse arquivo, 77 de 86 partes foram verificadas; as fontes consultadas não tinham as nove restantes. Isso não comprova conclusão desse arquivo nem compatibilidade com todos os roteadores. Veja [protocolos](docs/protocols.md) e [matriz de capacidades](docs/CAPABILITY-MATRIX.md) para os limites.

## Protocolos e limites

| Área | Implementação e limite |
| --- | --- |
| HTTP/1.1, HTTPS, HTTP/2 | Segmentação, identidade de retomada por Range, opções de cabeçalhos/proxy e limites de banda HTTP |
| HTTP/3 | HTTPS/QUIC explícito com `--http3-only`; sem seleção automática H2/H3 |
| FTP/FTPS | Download e retomada por REST; TLS explícito e implícito; retomada implícita ainda sem teste específico |
| SFTP | Download e retomada por seek; fingerprint SSH obrigatório; senha ou chave OpenSSH sem frase secreta |
| WebDAV | GET de arquivo individual por alias; sem coleções, PROPFIND, locks ou escrita |
| BitTorrent | v1/v2/híbrido, magnets `btih`/`btmh`, metadata, PEX, trackers, web seeds, seleção, envio e streaming |
| TCP/uTP e MSE/PE | Transportes com ofuscação opcional ou obrigatória; MSE não autentica o par |
| DHT / LSD | DHT IPv4/IPv6; LSD opt-in somente IPv4 e uma interface |
| Hole punching | BEP 55 sobre uTP; testes em loopback com restrição emulada, sem NAT real validado |
| ED2K | Download MD4 por partes, até oito fontes simultâneas, retomada que guarda o hashset autenticado e confere o parcial antes de falar com os pares, relatório do motivo da parada, ofuscação entre clientes e callbacks de servidor/buddy |
| UPnP | Mapeia as portas pedidas no roteador (`serve --upnp`), renova e remove ao sair; testado automaticamente com roteador simulado; log do usuário confirma mapeamento real com ID alto |
| Kad2 | Sessão persistente, lookup de fontes, verificação de contatos, filtro de IP e ofuscação UDP; recursos avançados incompletos |

Compartilhamento explícito, upload e fila ED2K existem como API de biblioteca; não estão expostos como serviço de compartilhamento completo pelo RPC/CLI. Faltam, entre outros: Metalink, WebDAV completo, créditos, identificação segura, AICH, source exchange, ofuscação ED2K cliente-servidor, busca/publicação Kad, teste de firewall e papel de buddy.

## Uso

```sh
cargo build --release --locked
nibiru-xtreme capabilities --json
nibiru-xtreme download https://example.test/file.bin --out file.bin
nibiru-xtreme serve --port 6800 --secret SEGREDO --dir downloads --session-file session.json
```

`serve` usa loopback IPv4. `nibiru.*` oferece o contrato nativo; `aria2.*` mantém apenas os métodos implementados. `--session-file` restaura tarefas inacabadas pausadas com GIDs estáveis. Tarefas com credenciais reconhecidas não são persistidas; o gerenciador deve recriá-las. As opções legadas `--input-file` e `--save-session` não equivalem à persistência nativa.

`nibiru-xtreme diagnose URL --dir PASTA` mede a rede sem disco, o disco e o motor no mesmo link e escreve `nibiru-diagnose-report.txt`. No `download`, Ctrl+C pausa e a mesma linha de comando continua de onde parou.

Para ED2K, adicione `--ed2k`, servidores por `--ed2k-server host:porta` e, opcionalmente, `--ed2k-nodes-file nodes.dat`. LSD usa `--enable-lpd [--lpd-interface IP]`. Use `--help` e a documentação de protocolos para opções e limites. Não grave credenciais reais no histórico do terminal.

## Estrutura e verificação

| Caminho | Responsabilidade |
| --- | --- |
| `src/engine.rs`, `src/storage` | HTTP, agendamento e armazenamento |
| `src/protocols` | FTP/FTPS, SFTP e HTTP/3 |
| `src/bittorrent` | BitTorrent, DHT, trackers e transportes |
| `src/ed2k` | ED2K, Kad, ofuscação e compartilhamento explícito |
| `src/rpc` e `src/rpc.rs` | Tarefas, persistência e JSON-RPC |
| `tests`, `benchmarks` | Regressões, interoperabilidade e medições |

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
```

Os testes independentes usam libtorrent 2.1.1 e aMule como outros clientes; não são motores embutidos. Exigem instalação separada. Testes que dependem de software ausente ou IPv6 indisponível podem ser pulados: confirme a saída. Há log de uso real de ED2K/UPnP no Windows; os testes automatizados de protocolo continuam usando loopback. Não há comprovação de todos os tipos de NAT ou conclusão do arquivo incompleto desse log. O agendador HTTP fixo permanece padrão; adaptatividade e checkpoints em segundo plano são opt-in. Benchmarks não provam superioridade universal.

## Segurança, CI e licença

O código Rust próprio proíbe `unsafe`. TLS valida certificados; SFTP exige chave conhecida. JSON-RPC tem token opcional e limites de conexão, leitura e tamanho. Configure um segredo para o gerenciador. Logs usam JSON Lines; URLs HTTP de diagnóstico removem credenciais, query e fragmento, incluindo erros de requisição.

`ci.yml` testa os três sistemas, verifica interoperabilidade no Linux e gera um artefato contendo somente o executável de cada plataforma. No Windows, o artefato de teste contém apenas o EXE. `release.yml` reutiliza esses binários e publica Windows x64, Linux x64 (musl estático) e macOS x64 com SHA-256 e licenças. Após esta versão, a manutenção acompanha os bugs encontrados no uso diário. Os listeners TCP, uTP, DHT e de prévia são encerrados com seus proprietários.

MIT. Dependências mantêm suas licenças; veja [LICENSE](LICENSE), [avisos de terceiros](THIRD_PARTY_NOTICES.md) e [Apache 2.0](LICENSE-APACHE-2.0.txt).

## Apoie o projeto

O Nibiru Xtreme é gratuito. Se ele for útil para você, pode contribuir pelo PayPal: [doar para jv12802@gmail.com](https://www.paypal.com/cgi-bin/webscr?cmd=_donations&business=jv12802%40gmail.com&currency_code=BRL). A doação é voluntária, não dá acesso a nada além do que já é público e não influencia licença, suporte ou prazos.
