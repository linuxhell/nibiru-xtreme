[Português](README.md) · [English](README.en.md) · [简体中文](README.zh-CN.md)

# Nibiru Xtreme

Motor de downloads em Rust, com contrato nativo JSON-RPC e um subconjunto de compatibilidade com aria2.

## Estado

**0.2.0-alpha.2 é uma versão de desenvolvimento, não uma implementação multiprotocolo concluída.** O ADM continua usando seu motor atual até o Xtreme estar concluído e sua integração validada. Não há release publicada na consulta de 21/10/2026; artefatos da CI são candidatos de teste.

A auditoria do código está em andamento. “Compilado”, “testado automaticamente” e “validado em uso real” são estados diferentes. A documentação de evidências está na [matriz de capacidades](docs/CAPABILITY-MATRIX.md); limitações e formatos estão em [protocolos](docs/protocols.md).

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
| ED2K | Download MD4 por partes, até quatro fontes simultâneas, retomada, ofuscação entre clientes e callbacks de servidor/buddy |
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

Os testes independentes usam libtorrent 2.1.1 e aMule como outros clientes; não são motores embutidos. Exigem instalação separada. Testes que dependem de software ausente ou IPv6 indisponível podem ser pulados: confirme a saída. Não há validação nesta base em rede ED2K/Kad pública, NAT real ou uso real de Windows/macOS. O agendador HTTP fixo permanece padrão; adaptatividade e checkpoints em segundo plano são opt-in. Benchmarks não provam superioridade universal.

## Segurança, CI e licença

O código Rust próprio proíbe `unsafe`. TLS valida certificados; SFTP exige chave conhecida. JSON-RPC tem token opcional e limites de conexão, leitura e tamanho. Configure um segredo para o gerenciador. Logs usam JSON Lines; URLs HTTP de diagnóstico removem credenciais, query e fragmento, incluindo erros de requisição.

`ci.yml` valida Windows, Linux e macOS e testa interoperabilidade no Linux. `release.yml` prepara pacotes e só deve publicar mediante autorização explícita. O contrato de distribuição/manifesto assinado do ADM ainda precisa ser concluído.

MIT. Dependências mantêm suas licenças; veja [LICENSE](LICENSE), [avisos de terceiros](THIRD_PARTY_NOTICES.md) e [Apache 2.0](LICENSE-APACHE-2.0.txt).
