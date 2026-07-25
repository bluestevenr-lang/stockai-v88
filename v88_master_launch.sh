#!/bin/bash

# 1. 停止旧进程（包括所有端口上的实例）
pkill -9 -f 'streamlit run app_v88_integrated.py' 2>/dev/null
pkill -9 -f 'run_v88.sh' 2>/dev/null
for p in $(lsof -ti:8501 -ti:8502 -ti:8503 2>/dev/null); do
    kill -9 "$p" 2>/dev/null
done
sleep 1

# 2. 设置代理环境变量（Clash 127.0.0.1:7897）
#    yfinance 需要代理才能访问 Yahoo Finance（中国被墙）
#    Clash 会自动将国内流量（如东财）直连，海外流量走代理
export http_proxy="http://127.0.0.1:7897"
export https_proxy="http://127.0.0.1:7897"
export ALL_PROXY="http://127.0.0.1:7897"
export HTTP_PROXY="http://127.0.0.1:7897"
export HTTPS_PROXY="http://127.0.0.1:7897"

# 密钥只从未跟踪的 .env 读取，避免进入Git历史。
ENV_FILE="$HOME/Desktop/StockAI/.env"
if [ -f "$ENV_FILE" ]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

# 3. 后台启动 streamlit（绑定 127.0.0.1 避免防火墙弹框，日志写到 /tmp）
cd ~/Desktop/StockAI

# 选择 Python 解释器（优先框架版 3.14，回退到 python3）
PYTHON_BIN="/Library/Frameworks/Python.framework/Versions/3.14/bin/python3.14"
[ -x "$PYTHON_BIN" ] || PYTHON_BIN="$(command -v python3)"

# Apple Silicon 上强制 arm64；其他架构留空
ARCH_PREFIX=""
[ "$(uname -m)" = "arm64" ] && ARCH_PREFIX="arch -arm64"

nohup $ARCH_PREFIX "$PYTHON_BIN" \
    -m streamlit run app_v88_integrated.py \
    --server.address 127.0.0.1 \
    --server.headless true \
    --server.port 8501 \
    --server.enableCORS true \
    --server.enableXsrfProtection true \
    --browser.gatherUsageStats false \
    > /tmp/v88_streamlit.log 2>&1 &

echo $! > /tmp/v88_streamlit.pid

# 4. 等服务就绪后打开/刷新浏览器（最多等 20 秒）
#    【V88定则 2026-07-18】已开着 V88 标签页就在原标签刷新并前置，不再开新标签占资源；
#    刷新=导航到干净首页（洗掉 ?q= 等残留深链参数，防止每次启动都跳回同一只股）；
#    依次找 Chrome / Edge / Safari 里含 8501 的标签，全都没有才 open 新开。
reuse_tab() {
    /usr/bin/osascript <<'OSA' 2>/dev/null
set hit to false
tell application "System Events" to set runningApps to name of every process
if runningApps contains "Google Chrome" then
    tell application "Google Chrome"
        set wIdx to 0
        repeat with w in windows
            set wIdx to wIdx + 1
            set tIdx to 0
            repeat with t in tabs of w
                set tIdx to tIdx + 1
                if (URL of t contains "localhost:8501") or (URL of t contains "127.0.0.1:8501") then
                    set URL of t to "http://localhost:8501"
                    set active tab index of w to tIdx
                    set index of w to 1
                    set hit to true
                    exit repeat
                end if
            end repeat
            if hit then exit repeat
        end repeat
        if hit then activate
    end tell
end if
if (not hit) and (runningApps contains "Microsoft Edge") then
    tell application "Microsoft Edge"
        set wIdx to 0
        repeat with w in windows
            set wIdx to wIdx + 1
            set tIdx to 0
            repeat with t in tabs of w
                set tIdx to tIdx + 1
                if (URL of t contains "localhost:8501") or (URL of t contains "127.0.0.1:8501") then
                    set URL of t to "http://localhost:8501"
                    set active tab index of w to tIdx
                    set index of w to 1
                    set hit to true
                    exit repeat
                end if
            end repeat
            if hit then exit repeat
        end repeat
        if hit then activate
    end tell
end if
if (not hit) and (runningApps contains "Safari") then
    tell application "Safari"
        repeat with w in windows
            set tIdx to 0
            repeat with t in tabs of w
                set tIdx to tIdx + 1
                if (URL of t contains "localhost:8501") or (URL of t contains "127.0.0.1:8501") then
                    set URL of t to "http://localhost:8501"
                    tell w to set current tab to tab tIdx
                    set hit to true
                    exit repeat
                end if
            end repeat
            if hit then exit repeat
        end repeat
        if hit then activate
    end tell
end if
if hit then
    return "reused"
end if
return "none"
OSA
}

open_or_reuse() {
    if [ "$(reuse_tab)" = "reused" ]; then
        return 0
    fi
    open http://localhost:8501
}

for i in $(seq 1 20); do
    sleep 1
    if curl -s http://127.0.0.1:8501 > /dev/null 2>&1; then
        open_or_reuse
        exit 0
    fi
done

# 超时兜底：直接打开/刷新
open_or_reuse
