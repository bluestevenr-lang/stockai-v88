@echo off
chcp 65001 >nul
REM ══════════════════════════════════════════════════════════════
REM  V88 Windows 启动器（第四终端）— 2026-07-19
REM  职责：①git pull 两仓同步最新代码与数据 ②设代理 ③启动 Streamlit
REM  全系统更新一致性 = 本脚本每次启动先 pull，无需任何手动同步。
REM ══════════════════════════════════════════════════════════════

set "STOCKAI=%USERPROFILE%\Desktop\StockAI"
set "REPORT=%USERPROFILE%\Desktop\ai-daily-report-v2"

echo [V88-Win] 同步公开仓(代码)...
git -C "%STOCKAI%" pull --rebase --autostash origin main
echo [V88-Win] 同步私仓(数据:日报/持仓/雷达族落盘)...
git -C "%REPORT%" pull --rebase --autostash origin main

REM —— 代理（Clash for Windows 默认混合端口 7890；改成你的端口）——
REM  国内访问 Yahoo 必需；东财/集思录等国内源代码里已强制直连不受影响。
set "http_proxy=http://127.0.0.1:7890"
set "https_proxy=http://127.0.0.1:7890"
set "HTTP_PROXY=%http_proxy%"
set "HTTPS_PROXY=%https_proxy%"

REM —— 停掉旧实例（如有）——
taskkill /f /fi "WINDOWTITLE eq V88-Streamlit*" >nul 2>&1

cd /d "%STOCKAI%"
echo [V88-Win] 启动 V88（端口8501，首次加载约1-2分钟）...
start "V88-Streamlit" /min python -m streamlit run app_v88_integrated.py ^
  --server.address 127.0.0.1 --server.headless true --server.port 8501 ^
  --server.enableCORS false --server.enableXsrfProtection false ^
  --browser.gatherUsageStats false

REM 等服务就绪再开浏览器（最多40秒）
set /a _n=0
:wait
timeout /t 2 /nobreak >nul
set /a _n+=1
curl -s -o nul http://127.0.0.1:8501 && goto open
if %_n% lss 20 goto wait
:open
start http://localhost:8501
echo [V88-Win] 完成。关闭本窗口不影响运行；停止请关闭 V88-Streamlit 窗口。
