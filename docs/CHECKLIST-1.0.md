# Checklist para publicar o Nibiru Xtreme 1.0

A versão estável atual é 0.2.0. Este checklist acompanha uma futura 1.0 e não bloqueia a 0.2.0 autorizada pelo usuário em 22/10/2026. A 1.0 exige revisar os itens abaixo. `[x]` = feito e com
evidência; `[ ]` = pendente. Publicar release, tag ou PR exige autorização expressa do usuário.

## Bloqueantes

- [ ] **Retomada ED2K em uso real.** O defeito foi reproduzido e corrigido em teste (hashset guardado ao lado do
      parcial, parcial conferido antes dos pares, motivo da parada) — ver `docs/protocols.md`. Falta **provar com
      uma transferência incompleta real** (o caso relatado: 89,5 %, 12 fontes, "metadata timed out"); a causa
      real desse caso **não foi confirmada**.
- [x] **ADM: a nova espera automática por fontes** (`waiting_sources`) o log de 22/10/2026 mostra repetição automática após partes indisponíveis. Reconexão explícita ainda precisa de um teste específico.
- [x] **UPnP antes do login e ID alto:** log público do usuário de 22/10/2026 confirma mapeamento TCP 4662 antes do primeiro login e ID alto.
- [x] **UPnP em roteador real:** mapeamento confirmado no log de 22/10/2026. Sem UPnP/CGNAT continua pendente.
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
