# Nibiru Xtreme

**Nibiru Xtreme** é um novo motor de downloads multiplataforma escrito em Rust, criado do zero para ser pequeno, seguro, poderoso, rápido e fácil de diagnosticar.

## Desenvolvimento multiprotocolo

A branch de desenvolvimento 0.2.0-alpha.1 acrescenta FTP/FTPS, SFTP, HTTP/3
explícito, aliases WebDAV GET e a fundação nativa de ED2K/Kad. Ela **não** é um
release multiprotocolo completo. Veja [suporte e limitações](docs/protocols.md).
O agendador HTTP fixo continua como padrão; os experimentos não foram ativados.

## Estado atual

A primeira fundação funcional já contém:

- download HTTP/HTTPS;
- cliente com HTTP/2 e TLS via Rustls;
- retomada segura por HTTP Range quando o servidor oferece suporte;
- logs estruturados em JSON Lines, focados em eventos importantes;
- servidor JSON-RPC restrito a `127.0.0.1`;
- primeiro subconjunto RPC compatível com aria2 para facilitar a futura migração do Apocalipse Download Manager;
- validação rígida de nomes de saída para impedir path traversal;
- `#![forbid(unsafe_code)]` em todo o código próprio.

> O Nibiru **ainda não substitui o aria2/aria2-ultra no ADM**. A troca definitiva só será feita quando houver paridade funcional, testes multiplataforma e regressões cobertas.

## Protocolos planejados

- HTTP/1.1, HTTP/2 e HTTP/3/QUIC;
- HTTPS;
- FTP e FTPS;
- SFTP;
- WebDAV;
- BitTorrent v1;
- BitTorrent v2 / BEP52;
- torrents híbridos;
- Magnet;
- DHT IPv4/IPv6;
- PEX;
- uTP;
- hole punching quando interoperável;
- ED2K e Kad.

## Objetivos de engenharia

1. **Corretude antes de otimização.**
2. **Medir antes de ajustar.**
3. **Nenhum dado de rede é confiável por padrão.**
4. **Sem código `unsafe` próprio.**
5. **Limites explícitos para memória, mensagens, metadados, redirects e filas.**
6. **Fallback previsível:** uma otimização nunca deve impedir um caminho compatível.
7. **Observabilidade sem log gigante:** INFO registra mudanças de estado e decisões; dados repetitivos não são despejados sem necessidade.
8. **Binário pequeno:** dependências só entram quando justificadas por segurança, interoperabilidade ou desempenho.
9. **Desempenho comprovado:** nenhuma alegação de “mais rápido” será aceita sem benchmark reproduzível.

## RPC e ADM

O Nibiru já aceita o modo de inicialização usado pelo ADM:

```
nibiru-xtreme --enable-rpc=true --rpc-listen-port=6800 --rpc-secret=...
```

O bootstrap implementa inicialmente:

- `aria2.getVersion`;
- `aria2.addUri`;
- `aria2.tellStatus`;
- `aria2.pause`;
- `aria2.unpause`;
- `aria2.remove` / `aria2.forceRemove`;
- `aria2.removeDownloadResult`;
- `aria2.changeOption` / `aria2.changeGlobalOption` como compatibilidade inicial.

BitTorrent ainda não está marcado como pronto nessa camada.

## Segurança

- Rust com `#![forbid(unsafe_code)]`;
- TLS moderno com Rustls;
- RPC somente em loopback por padrão;
- autenticação por token;
- limite de 1 MiB para requisição RPC;
- limite de redirects HTTP;
- credenciais e query string removidas dos URLs gravados em logs;
- validação de saída contra path traversal;
- nenhuma opção de “aceitar qualquer certificado TLS”.

## Logs

Cada linha é um evento JSON independente, por exemplo:

```json
{"ts":0,"level":"INFO","event":"http.download_completed","detail":"..."}
```

Isso permite ao ADM coletar apenas o necessário para suporte sem gerar um arquivo de debug enorme.

## Migração do ADM

O ADM continuará usando o motor atual durante o desenvolvimento. A substituição definitiva acontecerá somente depois que Nibiru suportar e passar testes para:

- downloads diretos;
- pause/resume/cancelamento;
- proxy;
- torrent v1/v2/híbrido;
- Magnet e descoberta de metadata;
- seleção de arquivos;
- limites de banda;
- logs/diagnósticos;
- atualização automática da ferramenta;
- Windows x64, Linux x64 e macOS x64.

## Licença

MIT.

English: [README.en.md](README.en.md)  
简体中文: [README.zh-CN.md](README.zh-CN.md)
