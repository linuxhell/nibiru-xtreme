# Investigação de desempenho — 19/10/2026

Objetivo no Windows do usuário: sustentar aproximadamente 110 MB/s durante a
transferência da ISO de 8.731.574.272 bytes, sem colapso nos últimos blocos.
Os resultados sintéticos abaixo são de fixtures locais Linux. **Não comprovam
110 MB/s na CDN Microsoft nem equivalência ao IDM.** As execuções reais no
Windows são identificadas separadamente.

Ambiente: Linux x86_64, glibc 2.39, filesystem overlay, 9 CPUs visíveis,
Rust 1.99.1. As dependências usadas foram fixadas em Cargo.lock. Não foram
executados dois benchmarks de throughput simultaneamente. A ordem dos perfis
foi alternada/rotacionada. Todas as transferências foram conferidas integralmente.

## Log em HTTP/1.1

Python ThreadingHTTPServer; 512 MiB; limite agregado de 105 MiB/s; atraso de
40 ms por requisição; três repetições por perfil. O teste valida tamanho e SHA-256
antes de aceitar cada amostra. O tempo de hash não entra na medição.

| Perfil | Tempo mediano | CPU mediana |
|---|---:|---:|
| Motor inicial, commit 5f40629 | 5,1771 s | 0,7551 s |
| Motor com telemetria, commit 5942b1b | 5,1304 s | 0,7115 s |
| Mesmo motor sem arquivo de log | 5,1763 s | 0,8319 s |

A perda observada pelo usuário não se reproduziu nesta fixture. Isso não exclui
custo de console, antivírus ou filesystem no Windows, nem efeitos específicos de
HTTP/2 ou da CDN.

## Janelas de fluxo HTTP/2, antes da mudança de ranges

Fixture Rust/h2, payload de 256 MiB, limitador de envio de até 110 MB/s e proxy
TCP que atrasa frames do cliente, inclusive WINDOW_UPDATE e PING. Cada perfil
teve três repetições. Valores incluem probe, transferência e finalização local.
O limite configurado não garante que a própria fixture consiga produzir essa taxa.

| Janela | RTT simulado de 50 ms: mediana | Sem atraso adicional: mediana |
|---|---:|---:|
| Padrão das dependências: conexão 5 MiB, stream 2 MiB | 4,6300 s / 57,977 MB/s | 2,5425 s / 105,579 MB/s |
| Adaptativa | 3,6879 s / 72,788 MB/s | 2,6482 s / 101,364 MB/s |
| Conexão 16 MiB, stream 4 MiB | 2,8619 s / 93,797 MB/s | 2,5682 s / 104,524 MB/s |

A janela maior limitada foi escolhida: melhorou as três medições com atraso e
ficou próxima do padrão sem atraso adicional. A adaptativa foi avaliada, mas
não virou o padrão desta versão por seu custo de arranque na fixture sem atraso.
No teste Windows posterior, a CDN negociou HTTP/1.1; portanto o ganho observado
nessa execução não pode ser atribuído às janelas HTTP/2.

## Ranges maiores com buffers limitados

Na mesma fixture HTTP/1.1 de 512 MiB, comparando a versão com telemetria anterior
com a candidata que recebe ranges maiores por streaming:

| Perfil | Tempo mediano | CPU mediana | GETs por execução, incluindo probe |
|---|---:|---:|---:|
| Anterior | 5,1755 s | 0,6944 s | 129 |
| Streaming em buffers de 4 MiB | 5,1269 s | 0,4698 s | 73 |

O ganho principal nesta fixture limitada foi menos CPU e menos requisições;
a vazão ficou próxima. Uma simulação do agendamento da ISO do usuário, com
16 workers, prevê 367 ranges de dados em vez de 2.082. Isso é uma contagem do
algoritmo, confirmada posteriormente pelo log de 367 ranges do teste Windows.

A candidata recebe até 32 MiB por requisição, mas escreve a cada 4 MiB, sem
acumular o range inteiro em memória nem fazer a cópia adicional para Vec depois
da coleta do corpo. Os ranges caem para 4 MiB ao entrar na região final do arquivo.
A alocação concorrente por CAS é testada contra sobreposição, lacunas e cauda
indevidamente grande. A escrita só atualiza o progresso após terminar.

Com a combinação final de streaming, uma segunda fixture HTTP/2 de 512 MiB e
50 ms de atraso produziu estas medianas de três execuções:

| Controle de fluxo, mesmo streaming | Tempo mediano | Média |
|---|---:|---:|
| Padrão das dependências | 9,0420 s | 59,376 MB/s |
| Adaptativo | 6,0637 s | 88,538 MB/s |
| Janela limitada escolhida | 5,3451 s | 100,442 MB/s |

Os payloads foram novamente comparados byte a byte. O resultado confirma a
escolha nesta fixture; os números sintéticos não substituem o teste Windows
descrito abaixo.

## ISO no Windows, versão 0.1.1

O teste enviado pelo usuário registrou 8.731.574.272 bytes em 79,5682 s de tempo
total: 109,737 MB/s decimais. A fase de transferência e escrita levou 75,659 s
(115,407 MB/s), seguida de 3,230 s de finalização. Foram 367 ranges HTTP/1.1,
sem fallback. Os últimos 695.287.808 bytes levaram 5,906 s (117,726 MB/s).
O SHA-256 coincidiu com os testes anteriores; não foi feita comparação com um
hash oficial da Microsoft. Uma execução não comprova velocidade constante ou
estabilidade entre execuções. A versão anterior levou 92,8792 s no teste anterior,
mas os ensaios Windows não tiveram ordem alternada nem condições de CDN fixas.

## Investigação de finalização, versão 0.1.2

O teste Windows posterior da 0.1.2 identificou a espera: 3.107 ms em `sync_all`,
de 3.108 ms totais de finalização. Fechamento, limpeza do destino e rename
ficaram abaixo de 1 ms cada. Foram 79,3330 s totais (110,062 MB/s), com
75,992 s de transferência/escrita (114,901 MB/s). O SHA-256 permaneceu igual.
Isso identifica a chamada onde o processo aguarda, não separa os custos internos
de cache, filesystem, driver, antivírus ou dispositivo.

Uma fixture HTTP/1.1 Linux, 1 GiB, 105 MiB/s agregados e 40 ms por requisição,
comparou três perfis em ordem rotacionada, três vezes cada. Todos os nove arquivos
passaram pela validação de tamanho e SHA-256. A sincronização final nunca foi
removida. Resultados completos e hashes dos binários experimentais:
[`finalization-experiment-2026-10-19.json`](finalization-experiment-2026-10-19.json).

| Perfil | Tempo total mediano | Finalização mediana |
|---|---:|---:|
| Referência 0.1.1, commit 0a8bffe7 | 9,8918 s | 29 ms |
| Candidata com sincronização no pool bloqueante | 10,1368 s | 265 ms |
| Mesma candidata e fsync externo a cada 256 MiB de progresso | 9,9917 s | 101 ms |

A transferência ficou em aproximadamente 9,753 s em todos os perfis. O pool não
melhorou a latência total nesta amostra. Os checkpoints reduziram a finalização
em relação à candidata, mas não o tempo total em relação à referência. Há
variabilidade de flush/cache no overlay Linux: isso não demonstra como o disco
Windows se comportará, e os 3,23 s do teste real não foram reproduzidos aqui.

Decisão: **nenhuma dessas duas alternativas virou padrão**. A versão publicada
0.1.2 conserva o caminho de sincronização da 0.1.1 e acrescenta somente os tempos
de sync, fechamento, limpeza do destino e rename. O objetivo é identificar a
origem da espera Windows antes de alterar a política de escrita.

Para reproduzir a candidata em uma cópia separada da 0.1.2, aplique
`git apply benchmarks/sync-pool-experiment.patch` e compile em release. Esse
patch é experimental e não está aplicado no pacote distribuído. Depois execute:

```bash
python benchmarks/controlled_http.py \
  --engine reference=/caminho/nibiru-0.1.1 \
  --engine diagnostics=/caminho/nibiru-sync-pool \
  --engine checkpoint=/caminho/nibiru-sync-pool \
  --checkpoint checkpoint=256 --size-mib 1024 \
  --rate-mib 105 --latency-ms 40 --rounds 3 --report finalization.json
```

O fsync externo usa um descritor separado e observa o progresso gravado em log,
não o tráfego bruto. Há três checkpoints por execução no relatório. O tempo
total inclui esperar essa thread terminar; os tempos nativos vêm do log do
motor. A CPU informada é a dos processos-filho, sem a thread Python de fsync.

## Checkpoints nativos opcionais, versão 0.1.3

Foi implementado um modo experimental com uma thread por download segmentado,
limite de uma operação pendente/em andamento, espera pela thread antes do rename
e `sync_all` final preservado. O padrão permanece zero checkpoints. A opção
`--sync-interval-mib N` e o iniciador `comparar-sync.cmd` permitem medir no Windows
o efeito no tempo total, sem mudar o padrão. O iniciador usa 512 MiB como hipótese
de teste e gera um ZIP dos resultados de duas transferências, sem incluir a ISO.

A fixture local Linux anterior foi repetida com 1 GiB, 105 MiB/s, 40 ms por
requisição e três amostras por perfil, alternando a ordem. Desta vez os dois
perfis usam o mesmo binário e o mesmo caminho de sincronização final; muda apenas
o agendamento de checkpoints nativos de 256 MiB. O motor realizou três checkpoints
por execução experimental. Todos os seis arquivos passaram por tamanho e SHA-256.

| Perfil | Tempo total mediano | Finalização mediana |
|---|---:|---:|
| Final-only, padrão | 9,9068 s | 22 ms |
| Checkpoints nativos a cada 256 MiB de progresso | 9,9365 s | 61 ms |

Não houve vantagem local que justificasse alterar o padrão. Esta fixture não
reproduz os 3.107 ms de sync no Windows. O recurso é entregue **opcional**, para
avaliar essa hipótese no computador do usuário, usando tempo total e hashes,
sem prometer antecipadamente que a sincronização vai ficar mais rápida.
As amostras e os hashes dos binários estão em
[`native-checkpoint-experiment-2026-10-19.json`](native-checkpoint-experiment-2026-10-19.json).

```bash
python benchmarks/controlled_http.py \
  --engine final=/caminho/nibiru-0.1.3 \
  --engine background=/caminho/nibiru-0.1.3 \
  --native-sync background=256 --size-mib 1024 \
  --rate-mib 105 --latency-ms 40 --rounds 3 --report native-sync.json
```

Os novos testes cobrem hash/conteúdo com checkpoints, cancelamento sem publicar
parcial, falha de sincronização e ausência de acúmulo de operações. O teste Windows
executa tanto o script simples quanto a comparação, verifica os hashes iguais,
a ocorrência de checkpoints e a criação do ZIP no Windows PowerShell 5.1.

## Reproduzir

O harness Python usa a biblioteca padrão e `resource` (Linux/macOS):

```bash
python benchmarks/controlled_http.py \
  --engine anterior=/caminho/binario-anterior \
  --engine atual=/caminho/binario-atual \
  --report resultado.json
```

A fixture HTTP/2 usa somente dependências de desenvolvimento Rust:

```bash
NIBIRU_BENCH_RTT_MS=50 cargo test --locked --release --lib compare_h2_flow_control -- --ignored --nocapture --test-threads=1
NIBIRU_BENCH_RTT_MS=0 cargo test --locked --release --lib compare_h2_flow_control -- --ignored --nocapture --test-threads=1
```

`NIBIRU_BENCH_SIZE_MIB` controla o tamanho (256 por padrão). O benchmark imprime
as medições; a CI não impõe um limiar de velocidade instável. A CI verifica os
bytes de HTTP/1.1 e HTTP/2, ranges grandes, a cauda do agendamento e o script real
no Windows PowerShell. O benchmark de throughput é executado explicitamente.

O teste real da ISO neste ambiente Linux foi bloqueado por `UnknownIssuer`
na cadeia TLS do ambiente. A verificação de certificados foi preservada.
