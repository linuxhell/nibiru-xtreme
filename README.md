# Nibiru Xtreme

**Nibiru Xtreme** é um novo motor de downloads multiplataforma escrito em Rust, criado do zero para ser pequeno, seguro, poderoso, rápido e fácil de diagnosticar.

## Desenvolvimento multiprotocolo

A branch de desenvolvimento 0.2.0-alpha.1 acrescenta FTP/FTPS, SFTP, HTTP/3
explícito, aliases WebDAV GET, BitTorrent v1/v2/híbrido, uTP, criptografia MSE,
DHT IPv4/IPv6 e a fundação nativa de ED2K/Kad. Ela **não** é um
release multiprotocolo completo. Veja [suporte e limitações](docs/protocols.md).
O agendador HTTP fixo continua como padrão; os experimentos não foram ativados.

## Estado atual

A versão de desenvolvimento contém:

- download HTTP/HTTPS e os transportes opcionais descritos abaixo;
- BitTorrent v1/v2/híbrido e magnets `btih`/`btmh`, metadata, PEX e pré-visualização;
- TCP e uTP (BEP 29), com negociação MSE/PE e opção de exigir criptografia;
- DHT IPv4 e IPv6, com IDs vinculados ao IP conforme BEP 42;
- cliente com HTTP/2 e TLS via Rustls;
- retomada segura por HTTP Range quando o servidor oferece suporte;
- logs estruturados em JSON Lines, focados em eventos importantes;
- servidor JSON-RPC restrito a `127.0.0.1`;
- primeiro subconjunto RPC compatível com aria2 para facilitar a futura migração do Apocalipse Download Manager;
- validação rígida de nomes de saída para impedir path traversal;
- `#![forbid(unsafe_code)]` em todo o código próprio.

> O Nibiru **ainda não substitui o aria2/aria2-ultra no ADM**. A troca definitiva só será feita quando houver paridade funcional, testes multiplataforma e regressões cobertas.

## Suporte e trabalho restante

| Protocolo | Estado na versão de desenvolvimento |
| --- | --- |
| HTTP/1.1, HTTPS, HTTP/2 | Download, segmentação e retomada |
| HTTP/3/QUIC | Seleção explícita; testes locais com TLS |
| FTP/FTPS e SFTP | Download; SFTP exige fingerprint da chave SSH; ainda sem retomada |
| WebDAV | Arquivo individual via GET; sem navegação de coleções |
| BitTorrent v1/v2/híbrido, Magnet | Download, envio, metadata, hashes e provas Merkle verificados |
| DHT IPv4/IPv6, PEX | Implementados; BEP 42 e persistência separada por família |
| uTP e MSE/PE | TCP/uTP com criptografia opcional ou obrigatória |
| Hole punching e descoberta local | Pendentes |
| ED2K | Fundação nativa de busca, download e compartilhamento explícito |
| Kad | Apenas bootstrap; roteamento persistente, busca e publicação pendentes |

Os testes independentes usam libtorrent **somente como outro cliente**, fora do
motor: 18 cenários de download/envio com bytes comparados, v1/v2/híbrido e
TCP/uTP/MSE. O transporte uTP usa a dependência Rust `librqbit-utp`; consulte
[avisos de terceiros](THIRD_PARTY_NOTICES.md). Nenhum desses resultados torna a
versão estável ou conclui a integração do ADM.

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

O RPC também oferece `addTorrent`, `getFiles`, `getPeers` e `nibiru.streamUrl`.
O conjunto completo de métodos e opções do aria2 ainda não foi implementado.

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
