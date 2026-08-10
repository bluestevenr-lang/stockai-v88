@echo off
chcp 65001 >nul
REM ==============================================================
REM  V88 repository mirror resident service (Windows host) - 2026-08-10
REM
REM  Difference from the phone-remote bat (win\ + phone-remote name):
REM    phone-remote bat = manual double-click, has pause, offline when window closes (interactive)
REM    this file        = launched by Task Scheduler, no pause, self-restart on crash (service)
REM
REM  Duties: pull both repos every 10 minutes and keep the Win mirror current.
REM  GPT/Codex owns the active V88 core on Mac/cloud. No AI process runs here.
REM
REM  Log: win\logs\remote_YYYYMMDD.log (ASCII only, avoids console codepage garbage)
REM
REM  ENCODING RULE: this file MUST be pure ASCII, comments included.
REM  Measured 2026-07-30: under "UTF-8 without BOM + chcp 65001", cmd mis-slices lines
REM  containing Chinese bytes. Via the Run key (auto-logon timing) the Chinese REM lines
REM  were executed as commands and "claude" was truncated to "aude"; bridge never started.
REM  Double-click did not reproduce it. Keep every byte in this file ASCII.
REM ==============================================================

set "STOCKAI=%USERPROFILE%\Desktop\StockAI"
set "REPORT=%USERPROFILE%\Desktop\ai-daily-report-v2"
set "LOGDIR=%STOCKAI%\win\logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%" >nul 2>&1

REM -- proxy kept for Git compatibility with the existing host setup --
set "http_proxy=http://127.0.0.1:7897"
set "https_proxy=http://127.0.0.1:7897"
set "HTTP_PROXY=%http_proxy%"
set "HTTPS_PROXY=%https_proxy%"

REM -- git must never go interactive (this one is the jugular) ----
REM  Measured 2026-07-30: pulling the private repo prints
REM  "please complete authentication in your browser".
REM  A background task (S4U, nobody logged on) cannot pop a browser and nobody clicks it
REM  -> git hangs forever -> the whole loop is stuck, log stops right after
REM  "pull ai-daily-report-v2 (private repo)..." with nothing following.
REM  So kill every credential prompt: if it cannot get creds, FAIL IMMEDIATELY, let the
REM  service skip the pull and keep the mirror loop alive.
REM  The cost is only stale data (on-disk snapshot), far better than a dead remote host.
set "GIT_TERMINAL_PROMPT=0"
set "GCM_INTERACTIVE=never"
set "GIT_ASKPASS="
set "SSH_ASKPASS="

:loop
call :stamp
set "LOG=%LOGDIR%\remote_%YMD%.log"

echo [%STAMP%] ---- round start ----------------------->> "%LOG%"

echo [%STAMP%] pull StockAI (public repo)...>> "%LOG%"
call :safepull "%STOCKAI%"
echo [%STAMP%] pull ai-daily-report-v2 (private repo)...>> "%LOG%"
call :safepull "%REPORT%"

REM -- materialize the legacy project instruction filename -------
REM  The filename stays for compatibility; its content now points to GPT/Codex ownership.
copy /Y "%STOCKAI%\win\CLAUDE-win.md" "%STOCKAI%\CLAUDE.md" >nul 2>&1
if errorlevel 1 (
  echo [%STAMP%] WARN: failed to materialize CLAUDE.md from win\CLAUDE-win.md>> "%LOG%"
) else (
  echo [%STAMP%] CLAUDE.md materialized from win\CLAUDE-win.md>> "%LOG%"
)

call :stamp
echo [%STAMP%] mirror sync complete; GPT/Codex is the active owner>> "%LOG%"
timeout /t 600 /nobreak >nul
goto loop


REM ==============================================================
REM  safe pull: on a mirror host, generated-file conflicts always take the remote side.
REM  If upstream leaves an unresolved rebase/merge, every later pull goes fatal and the
REM  service never starts - so clean up the wreckage first, then hard-align to origin/main.
REM ==============================================================
:safepull
set "R=%~1"
if not exist "%R%\.git" (
  echo [%STAMP%]   skip: %R% is not a git repo>> "%LOG%"
  goto :eof
)
git -C "%R%" -c credential.interactive=false pull --rebase --autostash origin main >> "%LOG%" 2>&1
if not errorlevel 1 goto :eof

echo [%STAMP%]   pull failed (auth or conflict), trying to self-heal; will NOT block startup>> "%LOG%"
git -C "%R%" rebase --abort  >> "%LOG%" 2>&1
git -C "%R%" merge  --abort  >> "%LOG%" 2>&1
git -C "%R%" -c credential.interactive=false fetch origin main >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [%STAMP%]   fetch failed too -> keeping on-disk snapshot, continuing anyway>> "%LOG%"
  goto :eof
)
git -C "%R%" reset --hard origin/main >> "%LOG%" 2>&1
goto :eof


REM ==============================================================
REM  timestamp: on Chinese Windows %date% looks like "Thu 2026/07/30" with a Chinese
REM  weekday first, so splitting by delims takes the weekday as the first field and the
REM  date is garbage - use PowerShell instead.
REM ==============================================================
:stamp
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd"') do set "YMD=%%i"
for /f "delims=" %%i in ('powershell -NoProfile -Command "Get-Date -Format \"yyyy-MM-dd HH:mm:ss\""') do set "STAMP=%%i"
goto :eof
