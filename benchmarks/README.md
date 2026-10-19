# Benchmark real com ISO do Windows 11

Este benchmark usa uma URL oficial e temporária da Microsoft para medir o Nibiru Xtreme em um arquivo grande.

As URLs geradas pela Microsoft são assinadas e expiram. Por segurança e para evitar manter tokens temporários no histórico Git, a URL **não** deve ser gravada neste repositório.

## Windows

No pacote Windows x64, extraia o ZIP e abra `iniciar-teste.cmd`. Cole a URL quando
aparecer `Url:`. A janela aguarda uma tecla ao terminar, inclusive em caso de erro.
O benchmark remove a ISO de destino existente antes de iniciar um teste novo.

Para compilar e executar a partir do repositório:

Compile em release:

```powershell
cargo build --release
```

Depois execute:

```powershell
.\benchmarks\windows11.ps1 -Url "COLE_AQUI_A_URL_ASSINADA_DA_MICROSOFT"
```

O script registra:

- tamanho final;
- tempo total;
- velocidade média em MiB/s;
- velocidade média em Gbit/s;
- SHA-256 do arquivo baixado.

O pacote salva dois arquivos ao lado do script:

- `nibiru-download.jsonl`: eventos do motor, acrescentados a cada execução;
- `nibiru-benchmark.json`: resultado do último teste, com tempo total, tamanho,
  velocidades, SHA-256 da ISO e SHA-256 do executável usado.

O SHA-256 calculado identifica o arquivo; a verificação de autenticidade depende de
compará-lo com o hash oficial correspondente. A medição de download termina antes
do cálculo dos hashes. MiB/s usa base 1024; Gbit/s usa base decimal.

Para uma comparação justa, use a mesma máquina, mesma rede e URLs válidas do mesmo arquivo. Execute os motores em ordens alternadas para reduzir o efeito de cache/CDN.

## O que observar

Além da média final, os logs do Nibiru devem mostrar quando o modo segmentado foi selecionado, quantos workers foram usados e se ocorreu fallback para transferência simples.

O evento `http.progress` registra aproximadamente uma amostra por segundo, inclusive
quando o corpo HTTP fica parado, e uma última amostra ao terminar. Seus campos
incluem bytes recebidos/gravados, bytes restantes, bytes e duração do intervalo,
velocidade em bytes/s e tempo decorrido. No modo segmentado, o contador avança após
a escrita de cada bloco; por isso as amostras podem oscilar em blocos de 4 MiB.
`workers_remaining` conta tarefas de download ainda não recolhidas pelo coordenador;
não representa o número de sockets TCP abertos.

`http.finalization_started` marca o término da transferência. O evento final
`http.download_completed` inclui `transfer_ms` e `finalize_ms`, separando a etapa de
transferência da finalização local (flush no modo simples; sincronização e rename
no modo segmentado). Esses tempos não incluem o probe HTTP anterior à transferência.
Isso permite examinar a velocidade nos últimos segundos e identificar uma demora
de finalização, sem gravar um evento para cada pacote ou bloco.

O teste completo deve ser complementado por testes controlados locais, pois CDN, rota e horário podem mudar entre execuções.

## Versão 0.1.1 e comparação de transporte

O padrão continua negociando HTTP/1.1 ou HTTP/2. No HTTP/2, a janela de conexão é
16 MiB e a janela por stream é 4 MiB. Requisições de até 32 MiB são recebidas em
buffers de escrita de 4 MiB; perto do fim, o agendamento volta a ranges de até
4 MiB para distribuir o trabalho restante entre os workers.

O evento `http.response` registra a versão HTTP realmente negociada, sem query
string ou credenciais. `http.segmented_metrics` registra quantidade de ranges,
protocolo e tempos acumulados de espera por cabeçalhos, corpo e escrita.
Esses tempos são somas de workers simultâneos: **não devem ser somados como tempo
total do download**. O tempo de escrita inclui espera no pool de tarefas de I/O.

O resumo inclui versão do motor, MB/s decimais e MiB/s, além dos hashes.
Para uma comparação específica em HTTP/1.1, use a mesma versão, URL e máquina:

```powershell
powershell -NoExit -ExecutionPolicy Bypass -File .\windows11.ps1 -Http1Only
```

Esse modo é opcional; o iniciador `iniciar-teste.cmd` usa a negociação automática.
Os resultados locais e os comandos de reprodução estão em
[`performance-2026-10-19.md`](performance-2026-10-19.md).

## Versão 0.1.2: diagnóstico da finalização

A transferência conserva o agendamento, os buffers e a configuração HTTP da 0.1.1.
No modo segmentado, `http.download_completed` agora discrimina `sync_ms`,
`close_ms`, `destination_cleanup_ms` e `rename_ms`. Os valores inteiros em
milissegundos são arredondados para baixo; sua soma pode ser ligeiramente menor
que `finalize_ms`. `sync_ms` mede a chamada síncrona `sync_all`, conservada da 0.1.1.

A sincronização continua sendo obrigatória antes de renomear o arquivo e
informar conclusão. As alternativas de sincronização foram avaliadas localmente,
mas não adotadas por falta de ganho consistente. Essa versão permite diagnosticar
a espera final; não promete aumentar a velocidade da rede.

O harness Unix permite investigar sincronização em segundo plano sem modificar
o motor: `--checkpoint LABEL=MIB` chama `fsync` em um descritor separado do
arquivo parcial a cada MIB de progresso observado. Isso é um experimento local,
não um modo disponível no executável Windows. A sincronização final do motor
permanece ativa, e o tempo do experimento inclui aguardar o sincronizador externo.

## Versão 0.1.3: comparação de sincronização no Windows

Abra **`comparar-sync.cmd`** e cole a URL uma vez. O iniciador executa dois
downloads completos com o mesmo binário: primeiro o modo atual, depois o modo
experimental com sincronização a cada 512 MiB de progresso gravado. Ele substitui
a própria ISO de teste entre as execuções; só uma ISO dessa comparação fica
armazenada. Os arquivos de resultado ficam em uma pasta exclusiva
`comparacao-sync-DATA-ID`, e o ZIP com o mesmo nome inclui somente JSON e JSONL,
sem a ISO. Envie esse ZIP para análise.

O modo experimental usa uma thread de sincronização e permite no máximo uma
operação pendente ou em andamento. Não cria uma fila de flushes. Antes de
renomear o arquivo, aguarda a operação pendente e executa novamente `sync_all`.
Falhas de checkpoint impedem a conclusão e não provocam fallback silencioso.
O cancelamento aguarda o sincronizador e remove o parcial. A sincronização não
altera os bytes nem substitui as validações de ranges e tamanho.

O evento `storage.checkpoint_completed` registra `observed_written` e o tempo
da operação. Esse contador é o progresso observado quando ela foi solicitada,
**não um prefixo contíguo ou uma garantia de bytes persistidos**. No evento de
conclusão, `checkpoint_wait_ms` é a espera final pelo sincronizador;
`checkpoint_count` e `checkpoint_sync_ms_sum` contabilizam as operações
antecipadas. O tempo acumulado de checkpoints pode se sobrepor à transferência
e não deve ser somado ao tempo total. A espera pendente já entra em `finalize_ms`.

`comparacao-sync.json` inclui os dois resultados, tempos nativos, hashes, médias
e diferença de tempo total. Confere tamanho e SHA-256 entre execuções e indica
se houve checkpoints de fato. Um arquivo pequeno pode terminar antes de qualquer
checkpoint. O ganho relevante é reduzir o **tempo total**, não apenas `sync_ms`.
Uma rodada é indicativa, pois CDN e cache podem mudar. Para duas rodadas em
ordem alternada (quatro downloads), execute:

```powershell
powershell -NoExit -ExecutionPolicy Bypass -File .\comparar-sync.ps1 -Rounds 2
```

O modo normal de `iniciar-teste.cmd` continua sem checkpoints. Para testar
manualmente apenas a alternativa, use `windows11.ps1 -SyncIntervalMiB 512`.
No CLI nativo, a opção é `download URL --out ARQUIVO --sync-interval-mib 512`;
zero é o padrão. O recurso vale para downloads segmentados, não para o modo
simples ou retomadas existentes.

A escolha de 512 MiB é uma hipótese de teste, não uma configuração já validada
como superior no Windows. Flushes frequentes também podem reduzir desempenho,
conforme a [documentação da Microsoft](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers).

## Versão 0.1.4: controlador adaptativo opcional

Abra **`comparar-adaptativo.cmd`** para comparar quatro downloads com o mesmo
executável, em duas rodadas com ordem alternada: agendamento fixo e controle
adaptativo. Ambos usam sincronização
somente no final. O ZIP `comparacao-adaptativo-DATA-ID.zip` contém o resumo e os
logs, sem a ISO. O teste de checkpoints é independente e permanece opcional.

O controlador começa com o mesmo teto de workers do modo normal, determinado
pelo tamanho do arquivo (até 16). Observa o progresso após escrita, testa metade
dos workers e só mede a nova configuração depois que requisições antigas acima
do limite terminam. As janelas têm pelo menos quatro segundos e 16 MiB de dados.
Se a vazão cair mais de 3%, restaura o limite anterior. Uma configuração menor
validada pode testar mais workers após um intervalo; o aumento exige ganho maior
que 8%. Isso é uma heurística experimental, sensível a variações de rede e disco.

Requisições em andamento não são canceladas para ajustar o limite. Workers
excedentes aguardam para pedir outro range. O controlador interrompe as tentativas
perto do final e restaura uma tentativa ainda sem validação suficiente. A última
alocação acorda os workers pausados para que encerrem sem pedir dados adicionais.
Os buffers continuam em 4 MiB e o teto de range em 32 MiB; a região de ranges
pequenos no fim acompanha o paralelismo escolhido.

`http.adaptive_started` identifica a ativação. `http.adaptive_decision` registra
limites anterior/novo, vazão observada, referência e motivo. O resumo indica se o
controlador foi ativado e quantas decisões mudaram o limite. Um arquivo curto pode
terminar sem ajuste. Workers são tarefas de requisição, não necessariamente
conexões TCP distintas, especialmente em HTTP/2. O recurso não é um modelo treinado
nem garante capacidade futura do servidor.

O modo normal continua fixo. Para ativar explicitamente: `windows11.ps1 -Adaptive`
ou `download URL --out ARQUIVO --adaptive`. Para duas rodadas em ordem alternada:

```powershell
powershell -NoExit -ExecutionPolicy Bypass -File .\comparar-sync.ps1 -TestAdaptive -Rounds 2
```
