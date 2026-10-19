@echo off
setlocal
pushd "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0windows11.ps1"
set "NIBIRU_EXIT_CODE=%ERRORLEVEL%"
echo.
pause
popd
exit /b %NIBIRU_EXIT_CODE%
