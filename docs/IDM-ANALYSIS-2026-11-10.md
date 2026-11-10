# IDM → Nibiru Xtreme: análise e implementação

Data: 10/11/2026. Referência do motor: `f6c41ba5cee2521095130be1b35f3e7f9c5e476c`.

## O que foi efetivamente examinado

Foi feita análise estática do instalador `idman643build18.exe` fornecido pelo
usuário, sem executar o instalador ou os programas internos. O contêiner tem
12.164.608 bytes, um PE32 inicial de 95.232 bytes e 188 registros comprimidos
com zlib, incluindo o manifesto. O registro 5 contém `IDMan.exe`, com
6.517.616 bytes. Os hashes SHA-256 são:

| Arquivo | SHA-256 |
| --- | --- |
| Instalador fornecido | `7346128fab42957b4254eabdb012bfdbd751e9069361e98a6916f26bb03d62ee` |
| IDMan.exe extraído para inspeção | `a705c0154fafc7cd509decf165cb6f6c050b6521b40adb6f730dd827c86051c7` |

O código principal é PE32 nativo. Foram encontrados formatos de cabeçalhos
`Range`, `Content-Range`, `If-Range` e Keep-Alive, referências a `SSL_read` e
`SSL_write`, importações Win32 para gravação/posicionamento/sincronização de
arquivos e mensagens de montagem das partes. O manifesto inclui bibliotecas
TLS e módulos de integração/monitoramento de navegador.

Essas evidências identificam componentes e capacidades, mas não recuperam o
escalonador, seus limiares, o dimensionamento exato dos buffers ou o código-fonte.
Não foi feita comparação executando IDM em Windows, captura de tráfego do IDM
ou medição na conexão do usuário. A implementação abaixo é código próprio para
o Nibiru, baseado em protocolos públicos e nos princípios documentados.

O [inventário reproduzível](idm643build18-static.json) contém metadados,
importações, hashes e indicadores, sem distribuir executáveis ou código do IDM.
Para repetir a inspeção com Python, sem dependências adicionais:

```sh
python tools/inspect_idm_installer.py /caminho/idman643build18.exe --report /tmp/idm-static.json
```

## Por que o IDM pode parecer tão rápido

A [documentação de segmentação do fabricante](https://www.internetdownloadmanager.com/support/segmentation.html)
descreve a divisão dinâmica do maior trecho restante, ajuda de conexões livres,
reutilização de conexões e montagem dos segmentos após a transferência. Também
descreve a gravação periódica das posições para retomada. A [documentação das opções](https://www.internetdownloadmanager.com/support/options.html)
informa que o download pode começar enquanto o usuário ainda escolhe o destino
na janela inicial. Esse início antecipado pode contribuir para a impressão de
velocidade máxima logo que aparece o progresso; não foi observado em execução
nesta análise.

Várias conexões ajudam quando uma só é limitada por latência ou pelo servidor.
Elas não garantem saturação da Internet, nem utilizam necessariamente toda a
capacidade do servidor. A taxa depende também da rota, congestionamento,
limites agregados, TLS/CPU e armazenamento. O limite deve respeitar cada fonte;
adicionar conexões sem medir pode piorar o resultado.

## O que foi aplicado ao HTTP/1.1 e HTTP/2 do Nibiru

| Princípio | Comportamento anterior | Implementação nesta alteração |
| --- | --- | --- |
| Começar os dados mais cedo | HEAD, GET de um byte, depois GETs de dados | Mantém o HEAD, mas usa as respostas reais de dados para verificar faixas; elimina o GET de um byte no início do download segmentado |
| Amortizar latência | Pedidos de 4 MiB quando restavam até 32 MiB por worker | Tamanho calculado com o restante e o número de workers, alinhado a 4 MiB e limitado a 32 MiB |
| Ajudar uma conexão ocupada | Um trecho já atribuído ficava com seu worker até terminar | Um worker livre assume metade da maior cauda ainda não reservada de outro worker |
| Evitar gravações sobrepostas | Alocador somente de trechos ainda não atribuídos | Reservas por bloco protegem o bloco em recepção/gravação; os cortes ocorrem somente depois dele |
| Concluir sem copiar o arquivo inteiro | Gravações posicionais no arquivo parcial, sincronização e publicação | Mantém essa arquitetura: não introduz uma etapa de concatenação como custo obrigatório |
| Distinguir recepção e conclusão | Nem sempre havia atualização após o último bloco, antes do sync | Emite progresso com todos os bytes escritos e velocidade zero antes da sincronização/publicação |

O método público `Engine::probe` continua verificando suporte a Range com um
GET de um byte. A remoção desse pedido se aplica apenas ao início da transferência.
Cada resposta real continua exigindo status 206, Content-Range exato, identidade
consistente e comprimento correto. Um servidor que anuncie faixas mas as ignore
continua podendo cair para o caminho de uma conexão.

As conexões usam o pool do mesmo cliente HTTP dentro da transferência. Ao ceder
uma cauda, o worker original encerra sua resposta na fronteira de bloco; isso
pode fechar aquela conexão HTTP/1.1 ou cancelar o stream HTTP/2. Portanto a
implementação não promete reutilizar cada socket após uma redistribuição.
Pode haver bytes recebidos e descartados durante essa transição, mas não há
dupla publicação/gravação dos blocos. A métrica `http.range_handoffs` registra
quantas redistribuições ocorreram.

Os buffers de montagem dos blocos continuam em 4 MiB por worker, além dos
buffers de transporte. São mantidos o teto de 16 workers, o limite escolhido no
ADM, os limitadores de velocidade, o controle adaptativo opcional, a retomada
por bitmap, os validadores e a publicação que preserva um destino criado por
outro processo. Não houve mudança de formato das parciais existentes.

## Interface Apocalipse

O aplicativo deve fixar a revisão desta alteração tanto na dependência Rust
quanto no backend empacotado. O fluxo de CI já constrói os dois a partir do
mesmo commit fixado em Cargo.lock.

A interface passa a mostrar “Finalizando arquivo…” quando uma transferência
com tamanho conhecido ainda está ativa, mas já recebeu todos os bytes. Só o
estado concluído informado pelo backend confirma o término. Esse rótulo é
inferido dos contadores existentes, não uma medição de porcentagem do fsync;
arquivos muito rápidos podem concluir entre duas atualizações. Gravações ao
vivo e torrents ficam fora dessa inferência. Há traduções em português, inglês
e chinês e informação acessível na barra de progresso.

## Torrent e ed2k: o que já existe e o que HTTP não substitui

| Protocolo | Recursos encontrados e preservados no código atual | Limites relevantes |
| --- | --- | --- |
| BitTorrent | Seleção rarest-first, blocos de 16 KiB, pipeline de 8–96 pedidos conforme a taxa, requisições duplicadas de fim de download com cancelamento, hash de peças, retomada e webseeds | Precisa descobrir metadados/peers, receber unchoke e ter fontes com as peças. A taxa agregada depende dos uploaders |
| ed2k | Partes de 9.728.000 bytes e blocos de 184.320 bytes, ajuda de vários peers na mesma parte, redistribuição de trabalho lento, verificação MD4, retomada e descoberta de fontes | Filas/vagas de upload, disponibilidade das partes e conectividade/ID baixo continuam determinantes |
| HTTP/3, FTP e SFTP | Caminhos específicos existentes e seus testes | O novo escalonador descrito acima pertence ao caminho HTTP/1.1/HTTP/2; não é automaticamente aplicado a esses transportes |

Os conceitos de paralelismo e ajuda no fim já têm equivalentes P2P no Nibiru.
Trocar a lógica P2P por cabeçalhos HTTP não funcionaria. As regras de peças,
verificação e fila precisam ser respeitadas. A [especificação BitTorrent BEP 3](https://www.bittorrent.org/beps/bep_0003.html)
descreve pedidos em pipeline, hashes de peças, choke/unchoke e cancelamento.
Esta alteração não comprova ganho de velocidade em torrent ou ed2k.

## Medição local: início e transferência

Comparação da referência com a candidata, em Linux x64 e Rust 1.97.0, builds
release `--locked --no-default-features`, mesma configuração e máquina.
Fixture HTTP/1.1 em loopback: atraso de 40 ms por requisição e teto agregado
de 100 MiB/s. Cinco repetições por perfil/tamanho/limite, com ordem alternada.
Os limites são tetos de conexões enviados por RPC, não contagens obrigatórias.
Não havia compilação concorrente durante as medições. Todas as 40 transferências
tiveram tamanho e SHA-256 conferidos depois da conclusão.

| Arquivo | Teto de conexões | Referência, mediana | Candidata, mediana |
| --- | ---: | ---: | ---: |
| 128 KiB | 1 | 44,18 ms | 44,33 ms |
| 128 KiB | 8 | 87,16 ms | 87,26 ms |
| 128 MiB | 1 | 1,5054 s | 1,5053 s |
| 128 MiB | 8 | 1,5959 s | 1,3787 s |

O caso segmentado de 128 MiB levou **13,6% menos tempo**, equivalente a cerca de
**15,8% mais vazão média**, neste cenário. O primeiro pedido de dados chegou à
fixture em 82,14 ms na referência e 41,88 ms na candidata; essa medida é chegada
do pedido ao servidor, não tempo até o primeiro byte no cliente. O caso de uma
conexão e os arquivos pequenos não demonstraram ganho material.

As amostras, pedidos e hashes dos dois binários estão no
[JSON de início HTTP](../benchmarks/idm-http-startup-2026-11-10.json).
Não se trata de teste contra o IDM, de uma CDN real, do Windows ou da Internet
do usuário; não estabelece superioridade geral sobre o IDM.

```sh
python benchmarks/http_startup.py \
  --engine baseline=/caminho/nibiru-referencia \
  --engine candidate=/caminho/nibiru-candidato \
  --size-mib 0.125 128 --connections 1 8 \
  --latency-ms 40 --rate-mib 100 --rounds 5 --report resultado.json
```

## Segundo cenário: limite por conexão

Em um segundo cenário, a fixture serviu 256 MiB com atraso de 80 ms por
requisição, teto agregado de 100 MiB/s e teto de 10 MiB/s por conexão. Cinco
repetições alternadas tiveram tamanho e SHA-256 conferidos. A mediana do tempo
total caiu de **4,1723 s para 3,9210 s (6,0%)**; os pedidos GET caíram de
**65 para 21**, e o pico observado continuou em oito pedidos ativos nos dois
perfis. Essas medidas incluem inicialização do processo e consultas HTTP.
As amostras e tempos internos estão no
[JSON com limite por conexão](../benchmarks/idm-http-per-connection-2026-11-10.json).

```sh
python benchmarks/controlled_http.py \
  --engine baseline=/caminho/nibiru-referencia \
  --engine candidate=/caminho/nibiru-candidato \
  --size-mib 256 --latency-ms 80 --rate-mib 100 \
  --connection-rate-mib 10 --rounds 5 --report resultado.json
```

## Verificação de correção

- 355 testes com todos os recursos do motor, mais um teste concorrente adicionado:
  HTTP/1.1, HTTP/2, HTTP/3, TLS,
  retomada, cancelamento, RPC, opções, limites, BitTorrent, ed2k, FTP e SFTP.
- Teste HTTP que bloqueia uma conexão até outra pedir um trecho de sua cauda;
  exige If-Range e confere o arquivo completo sem fallback para conexão única.
- Teste concorrente com 16 workers que verifica uma única gravação por bloco.
- Clippy com `-D warnings`, formatação e análise estática reproduzível.
- 384 testes JavaScript da interface, incluindo limites de inferência da finalização.

O ambiente local não tem os pacotes de desenvolvimento GTK/WebKit necessários
para compilar a janela desktop. A compilação e os testes do aplicativo completo
em Windows, Linux e macOS devem ser confirmados pelo CI dos PRs.
