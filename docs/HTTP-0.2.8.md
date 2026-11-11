# HTTP 0.2.8: implementação e limites

Implementação própria Rust/MIT após a [auditoria do MDM](MDM-AUDIT-2026-11-10.md). Não incorpora fonte GPL do outro projeto. HTTP/2, o caminho separado HTTP/3, torrent e ED2K foram preservados.

## Primeiro GET útil

Downloads HTTP novos começam por `GET Range: bytes=0-4194303`, sem HEAD. A resposta estabelece tamanho, URL final e validador. Seu corpo pertence ao worker zero e não é solicitado novamente. Se todo o arquivo couber nessa resposta, inclusive com framing chunked, ela encerra o download. Um servidor que ignora Range e responde 200 tem seu corpo aproveitado em uma conexão. Com uma conexão explicitamente configurada, o primeiro pedido é GET sem Range. Um 416 inicial permite GET normal para arquivos vazios ou servidores que recusam esse range.

A retomada mantém a confirmação da identidade e dos blocos salvos. Cabeçalhos 206, Content-Range, comprimento e identidade são conferidos antes de publicar. O probe explícito mantém HEAD e o teste de um byte.

## Limites, recuperação e buffers

429/503 têm até cinco repetições, respeitando Retry-After em segundos ou data HTTP. Sem instrução válida, usa backoff exponencial. Esperas acima de 30 segundos terminam com erro explícito, sem contornar a instrução com GET integral. Cancelar também cancela esperas de rede e backoff.

Três recusas, enquanto outras respostas recebem dados, podem reduzir a concorrência ao pico de respostas úteis observado. Uma janela inicial de 250 ms evita decidir antes que outras conexões comecem a receber; terminar uma conexão não apaga o pico comprovado. A recepção recente permanece válida por até um segundo entre respostas, evitando perder a observação no intervalo entre leases; evidência antiga ou ausência total de recepção não ensina limite. Uma recusa isolada ou indisponibilidade sem respostas úteis não ensina um teto.

O cache tem até 128 entradas, expira em dez minutos e guarda apenas fingerprints do contexto: esquema, host, porta, credenciais da URL, headers e proxy. Clones do mesmo Engine/RPC compartilham o histórico; processos CLI novos não compartilham. O slider explícito substitui o padrão por tamanho, respeitando blocos disponíveis, tamanho mínimo de divisão e teto de 16. O máximo escolhido nunca é ultrapassado. A CLI oferece `download --connections 1..16`; o padrão por tamanho não foi elevado globalmente.

Ranges com corpo interrompido ou sem uma leitura por 30 segundos podem recuperar blocos ainda não gravados, com até duas recuperações por worker. Esperas por disco ou limitador de banda ficam fora da leitura de rede. Blocos completos continuam marcados; buffers incompletos são descartados. A fila de recuperação compartilha o lock das reservas e handoffs. O esgotamento preserva o parcial verificável e não inicia fallback integral imediato.

Buffers de 4 MiB voltam ao worker depois de a escrita terminar. Não há mutação enquanto uma escrita usa esse buffer. O formato de retomada mantém compatibilidade.

## Arquivos esparsos no Windows

O núcleo mantém `#![forbid(unsafe_code)]`. A pequena dependência local MIT `crates/nibiru-platform` isola a FFI Win32 em uma API segura que só opera arquivos síncronos abertos por ela. Não aceita handles arbitrários/overlapped, não executa PowerShell/fsutil, não exige administrador e não usa SetFileValidData.

FSCTL_SET_SPARSE é aplicado antes de definir o tamanho lógico. Sistemas de arquivos sem suporte continuam com gravação normal e diagnóstico no log. Após escrever e verificar todos os blocos, remove o atributo antes da sincronização/publicação. Uma falha nessa remoção mantém um arquivo esparso válido e gera diagnóstico.

Referência primária: [Microsoft, FSCTL_SET_SPARSE](https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ni-winioctl-fsctl_set_sparse). O teste Windows cria 2 GiB lógicos, escreve fora de ordem e verifica alocação física abaixo de 64 MiB e leitura de buracos como zeros. Depois reduz o arquivo, grava todos os bytes e verifica a remoção do atributo. Os testes HTTP verificam o atributo do arquivo publicado. Linux não mede o ganho NTFS.

## Validação e medição

Regressões reais contra servidores TCP locais cobrem primeiro GET sem HEAD, prefixo reaproveitado, resposta sem Content-Length, servidor sem Range, arquivo vazio, uma conexão, corpos parados/truncados, orçamento esgotado e aprendizagem/isolamento/expiração de limites. O caso de oito workers exige reconhecer duas conexões úteis. Não exige ausência absoluta de 503, pois o servidor pode demorar a liberar uma conexão encerrada.

O CI exige formatação, Clippy estrito, todos os targets/features e interoperabilidade independente libtorrent/aMule. A publicação só ocorre depois das validações. O ADM exige separadamente testes desktop completos, transport lab e extensão nos três sistemas x64.

Os ensaios verificam tamanho e SHA-256 independentemente. Tempo de CLI inclui a espera de polling de conclusão do MDM; o instante de nome/tamanho final é registrado separadamente e não substitui a verificação final. RSS é amostrada em Linux, sem representar uma medição exata de pico. Resultados sintéticos não estabelecem superioridade geral nem aceleração da conexão residencial, torrent ou ED2K.
