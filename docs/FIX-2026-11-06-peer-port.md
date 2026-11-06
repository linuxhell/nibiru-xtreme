# Inicialização de peers no Windows

O [run 42300264106](https://github.com/linuxhell/nibiru-xtreme/actions/runs/42300264106)
passou no Linux e no macOS. No Windows, a etapa **Test** falhou nos três testes
de `tests/bittorrent_interop.rs`, durante `Session::start`, com
`cannot bind uTP peer port` e erro de acesso ao socket **10013**. A publicação
foi corretamente bloqueada pelo requisito de validação.

O motor escolhia uma porta automática pelo listener TCP e depois tentava usar o
mesmo número para UDP/uTP. A disponibilidade da porta TCP não garante a da UDP.
O log não identifica qual reserva ou serviço do Windows recusou o bind; não
permite atribuir a falha a um firewall específico.

Na versão 0.2.5, o motor reserva o socket UDP/uTP antes de abrir TCP no mesmo
número. O socket fica aberto durante essa seleção. Se TCP recusar uma porta
automática por conflito ou permissão, cancela o dispatcher uTP e tenta outra,
com limite de 32 tentativas. A porta explicitamente configurada não é trocada,
e uTP não é desativado como alternativa. A seleção mantém o fallback IPv4 quando
o endereço IPv6 não especificado estiver indisponível.

A guarda de cancelamento também libera o dispatcher se a inicialização for
interrompida antes de registrar a sessão. Os testes novos verificam a reserva
conjunta, conflitos, esgotamento das tentativas, limpeza e portas explícitas.
Os testes existentes com aria2c, magnets e transferências uTP continuam ativos.

Na validação local apareceu ainda uma falha intermitente do teste ED2K que
exigia que o primeiro byte cifrado fosse diferente do original. RC4 aplica XOR;
um byte zero do fluxo de chave permite essa coincidência. O teste agora confere
o ciphertext completo com o estado esperado após handshake e padding, e então
confere a decifragem. A implementação de criptografia não foi alterada.

O ADM passa a incluir esse commit na versão 0.8.11. Os workflows continuam
exigindo a validação de Windows, Linux e macOS x64 antes de publicar os binários.
