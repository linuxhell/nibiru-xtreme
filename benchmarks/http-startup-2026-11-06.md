# Início de downloads HTTP — 06/11/2026

O motor fazia HEAD e um GET de um byte antes de avaliar se o download poderia
ser segmentado. Com o limite de uma conexão, essas duas consultas não seriam
usadas. Em arquivos menores que 8 MiB, ou abaixo do tamanho mínimo de divisão,
o teste de faixa também não seria usado.

A versão 0.2.4 começa diretamente pelo GET quando o limite é uma conexão. Nos
outros casos, consulta o tamanho por HEAD e só testa faixas quando pode dividir
o arquivo. O `Engine::probe` público mantém a verificação de faixas em qualquer
tamanho. Retomada, identidade do recurso e validações dos segmentos permanecem.

## Comparação

Fixture HTTP/1.1 local Linux x64, Python 3, Rust 1.97.0, builds release com
`--locked --no-default-features`. Atraso configurado de 40 ms por requisição e
limite agregado de envio de 100 MiB/s; a taxa efetiva pode ser menor por causa
da própria fixture. Não houve compilação concorrente durante as medições.

Cinco repetições por perfil, tamanho e limite de conexões; ordem dos perfis
alternada. As 40 transferências tiveram tamanho e SHA-256 conferidos. A medição
inclui a chamada RPC de início, consultas HTTP, transferência e conclusão; o hash
é calculado depois. O status é consultado a cada 5 ms. Os limites são enviados
pelas mesmas opções RPC usadas pelo ADM; são tetos, não contagens de sockets.

| Arquivo | Limite de conexões | Antes, mediana | Candidata, mediana |
| --- | ---: | ---: | ---: |
| 128 KiB | 1 | 128,6 ms | 46,5 ms |
| 128 KiB | 8 | 124,9 ms | 85,1 ms |
| 128 MiB | 1 | 1,6646 s | 1,5626 s |
| 128 MiB | 8 | 1,6259 s | 1,6240 s |

Em uma conexão, o início deixa de aguardar duas consultas: o primeiro pedido
de dados chegou à fixture em cerca de 1,5–1,8 ms, contra 82–83 ms antes. Isso
antecede o atraso de resposta de 40 ms configurado no servidor; não é uma
medição do primeiro byte recebido pelo cliente.

O ganho demonstrado é reduzir a espera inicial. No arquivo grande com várias
conexões, a diferença total foi de aproximadamente 0,1%, compatível com ruído.
Esses resultados não comprovam ganho de vazão contínua numa CDN, no Windows
ou na conexão do usuário.

Referência: `d44d1d5cd6d8556938d2722ad02bbd417993a86f`. A candidata foi medida com
as alterações de sondagem, antes de mudar a versão declarada para 0.2.4. Os dois
binários, seus hashes, todas as amostras e as medianas estão registrados no
[JSON](http-startup-2026-11-06.json).

## Reproduzir

Compile o commit de referência e a candidata com a mesma toolchain e os mesmos
recursos. Guarde os executáveis em caminhos separados e execute sem builds ou
outros benchmarks simultâneos:

```sh
python benchmarks/http_startup.py \
  --engine baseline=/caminho/nibiru-anterior \
  --engine candidate=/caminho/nibiru-novo \
  --size-mib 0.125 128 --connections 1 8 \
  --latency-ms 40 --rate-mib 100 --rounds 5 --report resultado.json
```

O script usa somente loopback, desativa DHT e uTP nos processos de teste,
verifica o conteúdo completo e remove seus arquivos temporários ao terminar.
