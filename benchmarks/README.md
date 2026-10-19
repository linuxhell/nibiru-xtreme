# Benchmark real com ISO do Windows 11

Este benchmark usa uma URL oficial e temporária da Microsoft para medir o Nibiru Xtreme em um arquivo grande.

As URLs geradas pela Microsoft são assinadas e expiram. Por segurança e para evitar manter tokens temporários no histórico Git, a URL **não** deve ser gravada neste repositório.

## Windows

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

Para uma comparação justa, use a mesma máquina, mesma rede e URLs válidas do mesmo arquivo. Execute os motores em ordens alternadas para reduzir o efeito de cache/CDN.

## O que observar

Além da média final, os logs do Nibiru devem mostrar quando o modo segmentado foi selecionado, quantos workers foram usados e se ocorreu fallback para transferência simples.

O teste completo deve ser complementado por testes controlados locais, pois CDN, rota e horário podem mudar entre execuções.
