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

REM -- one-shot remote recovery for the 2026-08-24 GPT OpenClaw incident --
if exist "%STOCKAI%\win\repair_gpt_openclaw_once.ps1" (
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%STOCKAI%\win\repair_gpt_openclaw_once.ps1" >> "%LOG%" 2>&1
)

echo [%STAMP%] pull ai-daily-report-v2 (private repo, safe_pull)...>> "%LOG%"
call :safepull_private "%REPORT%"

REM -- refresh the privacy-minimized OpenClaw workspace ----------------
set "OCWORK=%USERPROFILE%\.openclaw\workspaces\v88-mobile"
set "OCGPT=%USERPROFILE%\.openclaw\workspaces\v88-gpt"
set "OCPKG=%STOCKAI%\win\openclaw-v88"
if exist "%OCPKG%\sync_v88_projection_win.py" if exist "%REPORT%\data\gpt_verify.json" (
  if not exist "%OCWORK%" mkdir "%OCWORK%" >nul 2>&1
  copy /Y "%OCPKG%\AGENTS.md" "%OCWORK%\AGENTS.md" >nul 2>&1
  python "%OCPKG%\sync_v88_projection_win.py" --source "%REPORT%\data" --dest "%OCWORK%\context" >> "%LOG%" 2>&1
  if errorlevel 1 (
    echo [%STAMP%] WARN: OpenClaw projection refresh failed; keeping last good snapshot>> "%LOG%"
  ) else (
    echo [%STAMP%] OpenClaw privacy projection refreshed>> "%LOG%"
  )
  if exist "%OCGPT%" (
    python "%OCPKG%\sync_v88_projection_win.py" --source "%REPORT%\data" --dest "%OCGPT%\context" >> "%LOG%" 2>&1
    if errorlevel 1 (
      echo [%STAMP%] WARN: GPT OpenClaw projection refresh failed; keeping last good snapshot>> "%LOG%"
    ) else (
      if not exist "%OCGPT%\knowledge\v88-claude-memory" mkdir "%OCGPT%\knowledge\v88-claude-memory" >nul 2>&1
      xcopy /D /E /I /Y "%REPORT%\claude-memory\*" "%OCGPT%\knowledge\v88-claude-memory\" >nul 2>&1
      echo [%STAMP%] GPT OpenClaw projection and sanitized Claude memory refreshed>> "%LOG%"
    )
  )
)

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

echo [%STAMP%]   public pull failed; preserving local commits and last good snapshot>> "%LOG%"
goto :eof


REM ==============================================================
REM  safe pull (private repo): route through scripts/safe_pull.sh
REM  Windows equivalent: Python launcher replaces the missing python3 command.
REM  Never hard-reset this private worktree; preserve local runtime data on failure.
REM ==============================================================
:safepull_private
set "R=%~1"
if not exist "%R%\.git" (
  echo [%STAMP%]   skip: %R% is not a git repo>> "%LOG%"
  goto :eof
)
set "V88PY=%USERPROFILE%\v88env\Scripts\python.exe"
if not exist "%V88PY%" (
  echo [%STAMP%]   private pull skipped: missing %V88PY%>> "%LOG%"
  goto :eof
)
git -C "%R%" config merge.v88json.name "V88 JSON newest-timestamp-wins" >> "%LOG%" 2>&1
git -C "%R%" config merge.v88json.driver "\"C:/Users/admin/v88env/Scripts/python.exe\" scripts/v88_json_merge.py %%O %%A %%B %%P" >> "%LOG%" 2>&1
git -C "%R%" config merge.v88jsonl.name "V88 JSONL lossless-union" >> "%LOG%" 2>&1
git -C "%R%" config merge.v88jsonl.driver "\"C:/Users/admin/v88env/Scripts/python.exe\" scripts/v88_jsonl_merge_driver.py %%O %%A %%B %%P" >> "%LOG%" 2>&1
git -C "%R%" -c credential.interactive=false pull --rebase --autostash origin main >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [%STAMP%]   private pull failed; preserving worktree and last good snapshot>> "%LOG%"
  goto :eof
)
pushd "%R%" >nul 2>&1
"%V88PY%" "scripts\v88_json_merge.py" --postpull >> "%LOG%" 2>&1
set "POSTPULL_RC=%ERRORLEVEL%"
popd >nul 2>&1
if not "%POSTPULL_RC%"=="0" echo [%STAMP%]   private postpull validation failed; preserving last good snapshot>> "%LOG%"
goto :eof


REM ==============================================================
REM  timestamp: on Chinese Windows %date% has a Chinese weekday first,
REM  so use PowerShell instead.
REM ==============================================================
:stamp
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd"') do set "YMD=%%i"
for /f "delims=" %%i in ('powershell -NoProfile -Command "Get-Date -Format \"yyyy-MM-dd HH:mm:ss\""') do set "STAMP=%%i"
goto :eof
