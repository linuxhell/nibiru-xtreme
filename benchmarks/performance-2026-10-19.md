# Investigação de desempenho — 19/10/2026

Objetivo no Windows do usuário: sustentar aproximadamente 110 MB/s durante a
transferência da ISO de 8.731.574.272 bytes, sem colapso nos últimos blocos.
Os resultados abaixo são de fixtures locais Linux. **Não comprovam 110 MB/s na
CDN Microsoft nem equivalência ao IDM.**

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
O protocolo da CDN no Windows ainda precisa ser observado no novo log.

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
algoritmo, não uma medição já realizada no servidor Microsoft.

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
escolha nesta fixture; não significa que a ISO já tenha sido medida no Windows
com esta versão nem que 110 MB/s estejam demonstrados.

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
