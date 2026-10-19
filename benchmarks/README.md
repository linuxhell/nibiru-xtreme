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
