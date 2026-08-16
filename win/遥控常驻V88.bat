@echo off
REM ==============================================================
REM  V88 repository mirror resident service (Windows host) - 2026-08-17
REM
REM  Difference from the phone-remote bat (win\ + phone-remote name):
REM    phone-remote bat = manual double-click, has pause, offline when window closes (interactive)
REM    this file        = launched by Task Scheduler / Run key, no pause, self-restart on crash (service)
REM
REM  Duties: pull both repos every 10 minutes and keep the Win mirror current.
REM  GPT/Codex owns the active V88 core on Mac/cloud. No AI process runs here.
REM
REM  Log: win\logs\remote_YYYYMMDD.log (ASCII only, avoids console codepage garbage)
REM
REM  2026-08-17 change (by Kimi on Win host):
REM    private repo now pulls via scripts/safe_pull.sh (Mac rule: never bare-pull the
REM    private repo; safe_pull self-heals JSON/JSONL and never leaves conflict markers).
REM    PYTHONUTF8=1 fixes the postpull healer crashing on GBK console when stash/commit
REM    messages contain Chinese. Fallback stays "remote wins" mirror semantics.
REM
REM  ENCODING RULE: this file MUST be pure ASCII, comments included.
REM  Measured 2026-07-30: under "UTF-8 without BOM + chcp 65001", cmd mis-slices lines
REM  containing Chinese bytes. Keep every byte in this file ASCII.
REM ==============================================================

REM -- PATH guard: make System32 / PowerShell / Git win over any inherited odd PATH --
REM  (when launched from a stripped shell, powershell was missing and GNU timeout.exe
REM   shadowed Windows timeout -> tight error loop; 2026-08-17 measured)
set "PATH=C:\Windows\System32;C:\Windows;C:\Windows\System32\WindowsPowerShell\v1.0;C:\Program Files\Git\cmd;C:\Program Files\Git\bin;%PATH%"

set "STOCKAI=%USERPROFILE%\Desktop\StockAI"
set "REPORT=%USERPROFILE%\Desktop\ai-daily-report-v2"
set "LOGDIR=%STOCKAI%\win\logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%" >nul 2>&1

REM -- proxy kept for Git compatibility with the existing host setup --
set "http_proxy=http://127.0.0.1:7897"
set "https_proxy=http://127.0.0.1:7897"
set "HTTP_PROXY=%http_proxy%"
set "HTTPS_PROXY=%https_proxy%"

REM -- UTF-8 mode so the safe_pull postpull healer survives Chinese bytes --
set "PYTHONUTF8=1"

REM -- git must never go interactive (this one is the jugular) ----
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
echo [%STAMP%] pull ai-daily-report-v2 (private repo, safe_pull)...>> "%LOG%"
call :safepull_private "%REPORT%"

REM -- materialize the legacy project instruction filename -------
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
REM  safe pull (public repo): generated-file conflicts always take the remote side.
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
REM  safe pull (private repo): route through scripts/safe_pull.sh
REM  (merge drivers + postpull JSON/JSONL self-heal, no bare pull).
REM  On failure keep the old mirror fallback: remote always wins.
REM ==============================================================
:safepull_private
set "R=%~1"
if not exist "%R%\.git" (
  echo [%STAMP%]   skip: %R% is not a git repo>> "%LOG%"
  goto :eof
)
"C:\Program Files\Git\bin\bash.exe" "%R%\scripts\safe_pull.sh" >> "%LOG%" 2>&1
if not errorlevel 1 goto :eof

echo [%STAMP%]   safe_pull failed, fallback: abort + fetch + hard reset (remote wins)>> "%LOG%"
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
REM  timestamp: on Chinese Windows %date% has a Chinese weekday first,
REM  so use PowerShell instead.
REM ==============================================================
:stamp
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd"') do set "YMD=%%i"
for /f "delims=" %%i in ('powershell -NoProfile -Command "Get-Date -Format \"yyyy-MM-dd HH:mm:ss\""') do set "STAMP=%%i"
goto :eof
