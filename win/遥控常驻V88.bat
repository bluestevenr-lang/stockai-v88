@echo off
chcp 65001 >nul
REM ══════════════════════════════════════════════════════════════
REM  V88 遥控常驻服务（Windows 主机版）— 2026-07-30
REM
REM  与 手机遥控V88.bat 的区别：
REM    手机遥控V88.bat = 人手双击、有 pause、关窗即下线（交互版）
REM    本文件          = 任务计划程序调用、无 pause、崩了自己重起（服务版）
REM
REM  职责：① 拉两仓 ② 设代理 ③ 起 claude remote-control
REM        ④ 进程若退出，等 30 秒重新拉代码再起（等价 Mac 的 KeepAlive）
REM
REM  ⚠️ 首次必须先手动过两个交互闸（本脚本无法代答）：
REM     1) cd StockAI 后跑一次 claude，选「Yes, I trust this folder」
REM     2) 跑一次 claude remote-control，对「Enable Remote Control? (y/n)」答 y
REM     两个都会记住，之后本服务才能无人值守启动。
REM
REM  日志：win\logs\remote_YYYYMMDD.log（内容一律 ASCII，避免中文控制台编码乱码）
REM ══════════════════════════════════════════════════════════════

set "STOCKAI=%USERPROFILE%\Desktop\StockAI"
set "REPORT=%USERPROFILE%\Desktop\ai-daily-report-v2"
set "LOGDIR=%STOCKAI%\win\logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%" >nul 2>&1

REM —— 代理（与 启动V88.bat / 手机遥控V88.bat 保持一致的 Clash 混合端口）——
REM  Claude Code 需连 api.anthropic.com；国内源代码里已强制直连，不受影响。
set "http_proxy=http://127.0.0.1:7897"
set "https_proxy=http://127.0.0.1:7897"
set "HTTP_PROXY=%http_proxy%"
set "HTTPS_PROXY=%https_proxy%"

REM —— Claude Code 原生安装目录（装完不会自动进 PATH，任务计划下尤其拿不到）——
set "PATH=%PATH%;%USERPROFILE%\.local\bin"

:loop
call :stamp
set "LOG=%LOGDIR%\remote_%YMD%.log"

echo [%STAMP%] ---- round start ----------------------->> "%LOG%"

REM  claude 缺失就不要死循环刷日志，慢速重试等人来装
where claude >nul 2>&1
if errorlevel 1 (
  echo [%STAMP%] FATAL: claude not found. Run: irm https://claude.ai/install.ps1 ^| iex >> "%LOG%"
  timeout /t 600 /nobreak >nul
  goto loop
)

echo [%STAMP%] pull StockAI (public repo)...>> "%LOG%"
call :safepull "%STOCKAI%"
echo [%STAMP%] pull ai-daily-report-v2 (private repo)...>> "%LOG%"
call :safepull "%REPORT%"

cd /d "%STOCKAI%"
echo [%STAMP%] starting claude remote-control ...>> "%LOG%"

REM  --system-prompt: 手机端 Code 区列表只显示会话标题，无法从名字分辨是哪台机器。
REM  让它开场自报身份，标题就会带上「Win主机」，跟 Mac 的会话区分开。
REM  --spawn=same-dir: 不加这个参数,首次会弹交互问 spawn mode(实测 2026-07-30),后台无人代答会卡死。
REM  必须 same-dir: worktree 模式给每个会话开独立 git worktree,而 data/ 与 .env 都被 gitignore,
REM  worktree 里没有这些文件 -> V88 脚本全跑不起来。
claude remote-control --spawn=same-dir --system-prompt "你运行在【Windows 主机】上(主机名 %COMPUTERNAME%，用户 %USERNAME%，工作目录 %STOCKAI%)，是 V88 的 7x24 常驻遥控终端。会话一开始就先说明你是 Win 主机，并把这次对话的主题定为「Win主机·V88遥控」，便于用户在手机 Code 区从标题分辨机器。注意边界：data/ 与 data/accounts.json 不在本机(只存 Mac)，涉及总资产/仓位占比的判断要说明需回 Mac；本机不跑流水线(云端 Actions 已覆盖)。" >> "%LOG%" 2>&1

set "RC=%errorlevel%"
call :stamp
echo [%STAMP%] remote-control exited (code=%RC%), retry in 30s>> "%LOG%"
timeout /t 30 /nobreak >nul
goto loop


REM ══════════════════════════════════════════════════════════════
REM  安全 pull：镜像端「生成物冲突一律取远端」。
REM  上游若留下未解决的 rebase/merge 冲突，会让之后每一轮 pull 都 fatal，
REM  服务就永远起不来 —— 所以先清残局，再硬对齐 origin/main。
REM ══════════════════════════════════════════════════════════════
:safepull
set "R=%~1"
if not exist "%R%\.git" (
  echo [%STAMP%]   skip: %R% is not a git repo>> "%LOG%"
  goto :eof
)
git -C "%R%" pull --rebase --autostash origin main >> "%LOG%" 2>&1
if not errorlevel 1 goto :eof

echo [%STAMP%]   pull failed, clearing conflict state and hard-aligning to origin/main>> "%LOG%"
git -C "%R%" rebase --abort  >> "%LOG%" 2>&1
git -C "%R%" merge  --abort  >> "%LOG%" 2>&1
git -C "%R%" fetch origin main >> "%LOG%" 2>&1
git -C "%R%" reset --hard origin/main >> "%LOG%" 2>&1
goto :eof


REM ══════════════════════════════════════════════════════════════
REM  时间戳：中文 Windows 的 %date% 形如「周四 2026/07/30」，
REM  按 delims 切会把「周四」当成第一段，日期全乱 —— 所以走 PowerShell 取。
REM ══════════════════════════════════════════════════════════════
:stamp
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd"') do set "YMD=%%i"
for /f "delims=" %%i in ('powershell -NoProfile -Command "Get-Date -Format \"yyyy-MM-dd HH:mm:ss\""') do set "STAMP=%%i"
goto :eof
