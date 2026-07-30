@echo off
chcp 65001 >nul
REM ==============================================================
REM  V88 remote-control resident service (Windows host) - 2026-07-30
REM
REM  Difference from the phone-remote bat (win\ + phone-remote name):
REM    phone-remote bat = manual double-click, has pause, offline when window closes (interactive)
REM    this file        = launched by Task Scheduler, no pause, self-restart on crash (service)
REM
REM  Duties: 1) pull both repos  2) set proxy  3) start claude remote-control
REM          4) if the process exits, wait 30s, re-pull, restart (same as Mac KeepAlive)
REM
REM  WARNING: two interactive gates must be cleared by hand once (this script cannot answer them):
REM     1) cd StockAI, run claude once, choose "Yes, I trust this folder"
REM     2) run claude remote-control once, answer y to "Enable Remote Control? (y/n)"
REM     Both are remembered; only after that can this service start unattended.
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

REM -- proxy (same Clash mixed port as the launcher / phone-remote bats) --
REM  Claude Code needs api.anthropic.com; domestic data sources are forced direct in code.
set "http_proxy=http://127.0.0.1:7897"
set "https_proxy=http://127.0.0.1:7897"
set "HTTP_PROXY=%http_proxy%"
set "HTTPS_PROXY=%https_proxy%"

REM -- Claude Code native install dir (not added to PATH automatically, especially under Task Scheduler) --
set "PATH=%PATH%;%USERPROFILE%\.local\bin"

REM -- git must never go interactive (this one is the jugular) ----
REM  Measured 2026-07-30: pulling the private repo prints
REM  "please complete authentication in your browser".
REM  A background task (S4U, nobody logged on) cannot pop a browser and nobody clicks it
REM  -> git hangs forever -> the whole loop is stuck, log stops right after
REM  "pull ai-daily-report-v2 (private repo)..." with nothing following.
REM  So kill every credential prompt: if it cannot get creds, FAIL IMMEDIATELY, let the
REM  service skip the pull and still start claude.
REM  The cost is only stale data (on-disk snapshot), far better than a dead remote host.
set "GIT_TERMINAL_PROMPT=0"
set "GCM_INTERACTIVE=never"
set "GIT_ASKPASS="
set "SSH_ASKPASS="

:loop
call :stamp
set "LOG=%LOGDIR%\remote_%YMD%.log"

echo [%STAMP%] ---- round start ----------------------->> "%LOG%"

REM -- resolve the ABSOLUTE path of the claude executable ---------
REM  Measured 2026-07-30: bare "claude" returns 9009 (command not found) under
REM  Task Scheduler (S4U), while the same command works in a manual PowerShell ->
REM  the background environment's PATH / user config is not reliable.
REM  So prefer an absolute path and write diagnostics to disk instead of guessing.
set "CLAUDE="
if exist "%USERPROFILE%\.local\bin\claude.exe"         set "CLAUDE=%USERPROFILE%\.local\bin\claude.exe"
if not defined CLAUDE if exist "%LOCALAPPDATA%\Programs\claude\claude.exe" set "CLAUDE=%LOCALAPPDATA%\Programs\claude\claude.exe"
if not defined CLAUDE if exist "%APPDATA%\npm\claude.cmd"  set "CLAUDE=%APPDATA%\npm\claude.cmd"
if not defined CLAUDE for /f "delims=" %%i in ('where claude 2^>nul') do if not defined CLAUDE set "CLAUDE=%%i"

echo [%STAMP%] diag USERPROFILE=%USERPROFILE%>> "%LOG%"
echo [%STAMP%] diag resolved CLAUDE=%CLAUDE%>> "%LOG%"

if not defined CLAUDE (
  echo [%STAMP%] FATAL: claude not found. Run: irm https://claude.ai/install.ps1 ^| iex >> "%LOG%"
  echo [%STAMP%] FATAL: also check that %%USERPROFILE%%\.local\bin\claude.exe exists >> "%LOG%"
  timeout /t 600 /nobreak >nul
  goto loop
)

echo [%STAMP%] pull StockAI (public repo)...>> "%LOG%"
call :safepull "%STOCKAI%"
echo [%STAMP%] pull ai-daily-report-v2 (private repo)...>> "%LOG%"
call :safepull "%REPORT%"

REM -- materialize CLAUDE.md --------------------------------------
REM  The repo-root CLAUDE.md is excluded by line 68 of StockAI's .gitignore, so it never
REM  syncs to this host and the Win-side Claude cannot read the project rules/boundaries.
REM  So the committed win\CLAUDE-win.md is the single source of truth, copied to the root
REM  CLAUDE.md on every round (Claude reads it automatically).
REM  To change the content, edit win\CLAUDE-win.md.
copy /Y "%STOCKAI%\win\CLAUDE-win.md" "%STOCKAI%\CLAUDE.md" >nul 2>&1
if errorlevel 1 (
  echo [%STAMP%] WARN: failed to materialize CLAUDE.md from win\CLAUDE-win.md>> "%LOG%"
) else (
  echo [%STAMP%] CLAUDE.md materialized from win\CLAUDE-win.md>> "%LOG%"
)

cd /d "%STOCKAI%"
echo [%STAMP%] starting claude remote-control ...>> "%LOG%"

REM  --spawn=same-dir: without it the first run asks interactively about spawn mode
REM  (measured 2026-07-30); nobody can answer in the background and it hangs.
REM  same-dir is mandatory: worktree mode gives each session its own git worktree, but
REM  data/ and .env are gitignored, so a worktree has neither -> no V88 script can run.
REM
REM  COMMAND LINE MUST BE ASCII - lesson measured 2026-07-30:
REM  this line used to carry a long Chinese/full-width --system-prompt, which caused
REM  exit 9009 (cmd did not assemble the command correctly); the absolute path was
REM  verified correct, claude.exe did exist, and it exited instantly with no output
REM  ==> the .bat parser truncated the command line on multi-byte characters under
REM  "UTF-8 without BOM + chcp 65001". Not a claude problem.
REM  The Win host identity/boundaries now live in the repo-root CLAUDE.md (auto-read),
REM  so no Chinese is passed here anymore.
REM  --name: help says "Name for the session (shown in claude.ai/code)" - this is the
REM         right way to tell hosts apart in the phone Code list (ASCII only).
REM  --verbose/--debug-file: measured 2026-07-30, Capacity stayed 0/32 in the background,
REM         even the documented "pre-create one session at startup" never happened, and the
REM         main log had zero errors ==> a debug channel is required.
"%CLAUDE%" remote-control --spawn=same-dir --name "V88-Win-Host" --verbose --debug-file "%LOGDIR%\rc_debug_%YMD%.log" >> "%LOG%" 2>&1

set "RC=%errorlevel%"
call :stamp
echo [%STAMP%] remote-control exited (code=%RC%), retry in 30s>> "%LOG%"
timeout /t 30 /nobreak >nul
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
