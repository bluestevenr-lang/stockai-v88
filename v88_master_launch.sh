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
    --server.address 0.0.0.0 \
    --server.headless true \
    --server.port 8501 \
    --server.enableCORS false \
    --server.enableXsrfProtection false \
    --browser.gatherUsageStats false \
    > /tmp/v88_streamlit.log 2>&1 &

echo $! > /tmp/v88_streamlit.pid

# 4. 等服务就绪后自动打开浏览器（最多等 20 秒）
for i in $(seq 1 20); do
    sleep 1
    if curl -s http://127.0.0.1:8501 > /dev/null 2>&1; then
        open http://localhost:8501
        exit 0
    fi
done

# 超时兜底：直接打开
open http://localhost:8501
