@echo off
setlocal
REM Install only the independent health monitor. Never starts the Gateway.
REM Notifications are explicitly enabled; only existing paired recipients are eligible.
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0install_openclaw_watchdog.ps1" -Notify %*
set "V88_HEALTH_EXIT=%ERRORLEVEL%"
echo.
if not "%V88_HEALTH_EXIT%"=="0" echo Installation did not complete. No recovery success is claimed.
pause
exit /b %V88_HEALTH_EXIT%
