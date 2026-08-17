@echo off
REM ============================================================
REM V88 - OpenClaw one-click installer launcher (2026-08-17)
REM House rule: .bat files must be 100 percent ASCII, comments too.
REM Double-click this file. It calls install_openclaw_win.ps1.
REM ============================================================
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0install_openclaw_win.ps1"
echo.
echo Done. Log file is in win\logs\openclaw_install_*.log
pause
