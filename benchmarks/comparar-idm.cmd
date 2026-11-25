@echo off
setlocal
cd /d "%~dp0"
echo Configure o IDM com 16 conexoes e limite de velocidade desativado.
echo Feche o IDM depois de salvar as opcoes.
echo A comparacao faz 3 rodadas por programa e confere o SHA-256 da ISO.
echo A ISO e apagada apos cada rodada; os resultados e logs ficam preservados.
pause
if exist "runtime\python.exe" (
  "runtime\python.exe" compare_idm.py --nibiru "nibiru-xtreme.exe" --idm-connections 16
) else (
  py -3 compare_idm.py --nibiru "nibiru-xtreme.exe" --idm-connections 16
)
echo.
echo Teste encerrado. Envie o arquivo comparacao.json da pasta criada.
pause
