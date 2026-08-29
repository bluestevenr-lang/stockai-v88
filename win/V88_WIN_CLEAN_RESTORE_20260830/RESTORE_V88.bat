@echo off
setlocal
fltmc >nul 2>&1
if not "%ERRORLEVEL%"=="0" (
  set "V88_ELEVATE_ARGS=%*"
  powershell.exe -NoLogo -NoProfile -Command "if($env:V88_ELEVATE_ARGS){Start-Process -FilePath '%~f0' -ArgumentList $env:V88_ELEVATE_ARGS -Verb RunAs}else{Start-Process -FilePath '%~f0' -Verb RunAs}"
  exit /b 0
)
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
