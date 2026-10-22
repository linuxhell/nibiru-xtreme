# Checklist para publicar o Nibiru Xtreme 1.0

A 1.0 **não** será publicada enquanto os itens bloqueantes abaixo estiverem abertos. `[x]` = feito e com
evidência; `[ ]` = pendente. Publicar release, tag ou PR exige autorização expressa do usuário.

## Bloqueantes

- [ ] **Retomada ED2K em uso real.** O defeito foi reproduzido e corrigido em teste (hashset guardado ao lado do
      parcial, parcial conferido antes dos pares, motivo da parada) — ver `docs/protocols.md`. Falta **provar com
      uma transferência incompleta real** (o caso relatado: 89,5 %, 12 fontes, "metadata timed out"); a causa
      real desse caso **não foi confirmada**.
- [ ] **ADM: a nova espera automática por fontes** (`waiting_sources`) testada no aplicativo real: parar com
      fontes ruins, reconectar ED2K/Kad e ver o download voltar sozinho.
- [ ] **ID baixo após conectar:** conferir no aplicativo real que, com UPnP ligado, o primeiro login já sai com ID
      alto (a porta agora é mapeada **antes** do login; testado só contra roteador simulado).
- [ ] **UPnP em roteador real** (e comportamento sem UPnP / atrás de CGNAT).
- [ ] **CI verde** nas três plataformas no commit da release, incluindo o interop com aMule (falha intermitente
      já vista: `ED2K peer read timed out`; re-executar uma vez antes de tratar como regressão).
- [ ] Pacote do ADM embutindo o Xtreme (hoje o ADM baixa o motor pela aba Ferramentas).

## Já feito (com evidência)

- [x] Retomada de download HTTP segmentado (`tests/http_resume.rs`); uso real no Windows: seis retomadas, uma com
      5,07 GB preservados.
- [x] `diagnose`; medições reais no Windows (ISO de 8,7 GB): 113–115 MB/s com servidor rápido.
- [x] UPnP implementado e testado contra roteador simulado (`tests/upnp.rs`).
- [x] Pré-releases alpha.2 e alpha.3 publicadas com 3 binários e SHA-256.
- [x] Compartilhamento ED2K escolhido pelo usuário, Kad real, ofuscação, UPnP na janela ED2K do ADM.

## Documentação e apresentação

- [x] README em português, inglês e chinês simplificado.
- [x] Descrição do projeto nos três idiomas (`docs/DESCRICAO-DO-PROJETO.md`; o texto do repositório no GitHub
      precisa ser colado por quem tem acesso às configurações).
- [x] Doação PayPal (`jv12802@gmail.com`) nos três READMEs.
- [ ] Notas de release da 1.0 revisadas depois que os bloqueantes fecharem.
- [ ] Versão `1.0.0` em `Cargo.toml`/`Cargo.lock`, `RELEASE_NOTES.md` e título da release (só quando tudo acima estiver
      fechado).

## Faltam no produto (não anunciar como pronto)

Metalink; busca por palavra-chave e publicação no Kad; créditos e identificação segura do ED2K; AICH; ofuscação
cliente↔servidor do ED2K; NAT-PMP/PCP; UPnP no modo compatível com aria2; espera automática por fontes na RPC do
Xtreme (hoje só no ADM); limite de conexões acima de 16.
