@echo off
chcp 65001 >nul
REM ══════════════════════════════════════════════════════════════
REM  V88 手机遥控主机（Windows）— 2026-07-19
REM  作用：把这台 Win 变成手机可遥控的 Claude Code 主机。
REM  Mac / Win 任何一台开机跑本脚本（或 Mac 端 /remote-control），
REM  手机 Claude App → Code 区即可看到会话（电脑图标+绿点）直接对话。
REM ══════════════════════════════════════════════════════════════

set "STOCKAI=%USERPROFILE%\Desktop\StockAI"
set "REPORT=%USERPROFILE%\Desktop\ai-daily-report-v2"

echo [V88-遥控] 同步两仓（保证遥控会话拿到的是最新代码+数据+记忆副本）...
git -C "%STOCKAI%" pull --rebase --autostash origin main
git -C "%REPORT%" pull --rebase --autostash origin main

REM —— 代理（Clash 混合端口，与 启动V88.bat 保持一致）——
REM  Claude Code 需要连 api.anthropic.com，国内网络必须走代理。
set "http_proxy=http://127.0.0.1:7897"
set "https_proxy=http://127.0.0.1:7897"
set "HTTP_PROXY=%http_proxy%"
set "HTTPS_PROXY=%https_proxy%"

where claude >nul 2>&1
if errorlevel 1 (
  echo [V88-遥控] 未安装 Claude Code。请先在 PowerShell 执行：
  echo    irm https://claude.ai/install.ps1 ^| iex
  echo 然后运行 claude 用与手机相同的账号登录，再重跑本脚本。
  pause
  exit /b 1
)

cd /d "%STOCKAI%"
echo.
echo [V88-遥控] 启动遥控会话中...（保持本窗口开着=遥控在线；关窗=下线）
echo [V88-遥控] 手机 Claude App → Code 区 → 选这台 Win 的会话即可对话。
echo [V88-遥控] Win 端 Claude 开场请说：按 win/README_WIN.md 与私仓 claude-memory/ 接管 V88。
echo.
claude remote-control
