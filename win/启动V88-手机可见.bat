@echo off
chcp 65001 >nul
REM ══════════════════════════════════════════════════════════════
REM  V88 启动器·手机可见版（第四终端）— 2026-07-30
REM
REM  与 启动V88.bat 的唯一区别：绑 0.0.0.0 而不是 127.0.0.1，
REM  这样同一 WiFi 下手机浏览器能直接开看板。启动V88.bat 原样保留不动。
REM
REM  ⚠️ 安全边界：这个面板【没有登录保护】，绑 0.0.0.0 = 内网任何设备可见。
REM     只在你自己信任的家庭 WiFi 用。绝对不要在路由器上做端口映射/DDNS
REM     把 8501 暴露到公网——里面有持仓。
REM ══════════════════════════════════════════════════════════════

set "STOCKAI=%USERPROFILE%\Desktop\StockAI"
set "REPORT=%USERPROFILE%\Desktop\ai-daily-report-v2"

echo [V88-Win] 同步公开仓(代码)...
git -C "%STOCKAI%" pull --rebase --autostash origin main
echo [V88-Win] 同步私仓(数据:日报/持仓/雷达族落盘，走 safe_pull 自愈)...
set PYTHONUTF8=1
"C:\Program Files\Git\bin\bash.exe" "%REPORT%\scripts\safe_pull.sh"

REM —— 代理（与 启动V88.bat 一致的 Clash 混合端口）——
set "http_proxy=http://127.0.0.1:7897"
set "https_proxy=http://127.0.0.1:7897"
set "HTTP_PROXY=%http_proxy%"
set "HTTPS_PROXY=%https_proxy%"

REM —— 停掉旧实例（如有）——
taskkill /f /fi "WINDOWTITLE eq V88-Streamlit*" >nul 2>&1

REM —— 放行防火墙（首次需管理员；非管理员会静默失败，手机就连不上）——
netsh advfirewall firewall show rule name="V88-Streamlit-8501" >nul 2>&1
if errorlevel 1 (
  echo [V88-Win] 添加防火墙入站规则 8501（若提示权限不足，请以管理员身份重跑一次本文件）...
  netsh advfirewall firewall add rule name="V88-Streamlit-8501" dir=in action=allow protocol=TCP localport=8501 profile=private >nul 2>&1
)

cd /d "%STOCKAI%"
echo [V88-Win] 启动 V88（端口8501，绑 0.0.0.0，首次加载约1-2分钟）...
start "V88-Streamlit" /min python -m streamlit run app_v88_integrated.py ^
  --server.address 0.0.0.0 --server.headless true --server.port 8501 ^
  --server.enableCORS false --server.enableXsrfProtection false ^
  --browser.gatherUsageStats false

REM 等服务就绪（最多40秒）
set /a _n=0
:wait
timeout /t 2 /nobreak >nul
set /a _n+=1
curl -s -o nul http://127.0.0.1:8501 && goto open
if %_n% lss 20 goto wait
:open

echo.
echo ════════════════════════════════════════════════
echo  手机在同一 WiFi 下，浏览器打开以下任一地址：
for /f "tokens=2 delims=:" %%i in ('ipconfig ^| findstr /c:"IPv4"') do (
  for /f "tokens=1" %%j in ("%%i") do echo    http://%%j:8501
)
echo ════════════════════════════════════════════════
echo  ⚠️ 仅限内网。不要做端口映射到公网（面板无登录保护，含持仓）。
echo ════════════════════════════════════════════════
echo.
start http://localhost:8501
echo [V88-Win] 完成。关闭本窗口不影响运行；停止请关闭 V88-Streamlit 窗口。
