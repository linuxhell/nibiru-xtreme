@echo off
setlocal
pushd "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0comparar-sync.ps1" -TestAdaptive -Rounds 2
set "NIBIRU_EXIT_CODE=%ERRORLEVEL%"
echo.
pause
popd
exit /b %NIBIRU_EXIT_CODE%
