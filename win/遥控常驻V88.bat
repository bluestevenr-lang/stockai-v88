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
REM  不要双击本文件。请先用管理员 PowerShell 跑一次 常驻V88.ps1 完成注册。
REM  日志：win\logs\remote_YYYYMMDD.log
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

:loop
call :stamp
set "LOG=%LOGDIR%\remote_%YMD%.log"

echo [%STAMP%] ── 本轮启动 ─────────────────────────>> "%LOG%"

REM  claude 缺失就不要死循环刷日志，慢速重试等人来装
where claude >nul 2>&1
if errorlevel 1 (
  echo [%STAMP%] 致命: 未安装 Claude Code。PowerShell 执行 irm https://claude.ai/install.ps1 ^| iex >> "%LOG%"
  timeout /t 600 /nobreak >nul
  goto loop
)

echo [%STAMP%] 同步公开仓 StockAI...>> "%LOG%"
git -C "%STOCKAI%" pull --rebase --autostash origin main >> "%LOG%" 2>&1
echo [%STAMP%] 同步私仓 ai-daily-report-v2...>> "%LOG%"
git -C "%REPORT%" pull --rebase --autostash origin main >> "%LOG%" 2>&1

cd /d "%STOCKAI%"
echo [%STAMP%] 启动 claude remote-control（手机 Claude App → Code 区可见本机）>> "%LOG%"

REM  前台阻塞运行；正常在线时不会走到下一行
claude remote-control >> "%LOG%" 2>&1

call :stamp
echo [%STAMP%] 遥控进程退出(码=%errorlevel%)，30 秒后重拉代码并重启>> "%LOG%"
timeout /t 30 /nobreak >nul
goto loop

:stamp
for /f "tokens=1-3 delims=/- " %%a in ("%date%") do set "YMD=%%a%%b%%c"
set "YMD=%YMD:~0,8%"
set "STAMP=%date% %time:~0,8%"
goto :eof
