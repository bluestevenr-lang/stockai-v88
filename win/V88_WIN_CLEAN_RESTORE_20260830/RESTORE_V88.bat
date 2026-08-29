@echo off
setlocal
set "SCRIPT=%~dp0restore_v88_win.ps1"
if not exist "%SCRIPT%" (
  echo Missing restore_v88_win.ps1
  exit /b 2
)
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT%" %*
set "RC=%ERRORLEVEL%"
echo.
echo V88 restore finished with exit code %RC%.
pause
exit /b %RC%
