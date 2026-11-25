# Comparacao real com IDM no Windows

1. Feche outros downloads, sincronizacoes e atualizacoes. Use a mesma rede e disco para os dois programas.
2. No IDM, escolha **16 conexoes** e desative limites de velocidade. Confira tambem excecoes por servidor. Salve e feche o IDM.
3. No pacote de teste, abra `comparar-idm.cmd` e cole um link valido da ISO quando solicitado. O link fica oculto e nao entra no relatorio.
4. Aguarde as seis transferencias completas. A ordem alterna entre Nibiru/IDM e IDM/Nibiru. Nao abra outro IDM durante o teste.
5. Envie o arquivo `comparacao.json` gerado. Ele contem os tempos individuais, medianas, taxas, hashes dos binarios e SHA-256 da ISO. Os logs do Nibiru ficam nas subpastas.

O IDM precisa estar instalado e disponivel para uso normal, inclusive avaliacao valida. O teste usa somente a linha de comando documentada pelo fabricante. Nao modifica o IDM nem seu licenciamento. Se estiver instalado em outro local, use `--idm CAMINHO` no comando abaixo.

O cronometro inclui inicio do processo, transferencia e finalizacao. Calcular SHA-256 fica fora do tempo medido. Arquivos com tamanho incorreto, processos com erro, tempo excedido ou hashes divergentes invalidam o teste. O script usa uma pasta nova por transferencia e remove apenas a propria ISO depois da verificacao. Nao remove downloads anteriores. Reserve pelo menos duas vezes o tamanho da ISO mais espaco para temporarios do IDM; confirme tambem a pasta temporaria configurada nele.

Uma diferenca de ate 5% entre as medianas recebe o rotulo `within_tolerance`; isso e um criterio operacional deste ensaio, sem demonstracao estatistica de equivalencia. Tres rodadas nao garantem superioridade em outras redes. Um hash igual entre transferencias comprova consistencia; autenticidade exige o SHA-256 oficial correspondente, que pode ser informado com `--expected-sha256`.

Sem o pacote, com Python 3.11 ou superior instalado:

```powershell
py -3 benchmarks/compare_idm.py --nibiru target/release/nibiru-xtreme.exe --idm-connections 16
```

O teste mede o executavel do Nibiru, tambem usado como motor de downloads diretos do ADM. Nao mede a interacao com a interface do ADM. A URL temporaria e solicitada novamente quando o link expirar.

Referencia: [linha de comando oficial do IDM](https://www.internetdownloadmanager.com/support/command_line.html).
