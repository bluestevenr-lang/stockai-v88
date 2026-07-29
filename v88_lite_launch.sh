#!/bin/zsh
# ═══════════════════════════════════════════════════════════════
# V88 轻量版启动脚本（手机/云端 24 小时访问）
# 本机: bash v88_lite_launch.sh  → http://<本机IP>:8600
# VPS : 同样运行；确保 8600 端口放行 + 代理(拉美股/港股行情)可用
# ═══════════════════════════════════════════════════════════════
cd "$(dirname "$0")" || exit 1

PORT=8600
# 拉美股/港股行情需 http 代理（本机 Clash 7897；VPS 上改成 VPS 的 http 代理）
PROXY="${V88_HTTP_PROXY:-http://127.0.0.1:7897}"

pkill -9 -f "streamlit run v88_lite.py" 2>/dev/null
for p in $(lsof -ti:$PORT 2>/dev/null); do kill -9 "$p" 2>/dev/null; done
sleep 1

export http_proxy="$PROXY" https_proxy="$PROXY" HTTP_PROXY="$PROXY" HTTPS_PROXY="$PROXY"

PYTHON_BIN="${PYTHON_BIN:-python3}"
nohup "$PYTHON_BIN" -m streamlit run v88_lite.py \
    --server.address 127.0.0.1 \
    --server.port $PORT \
    --server.headless true \
    --server.enableCORS true \
    --server.enableXsrfProtection true \
    --browser.gatherUsageStats false \
    > /tmp/v88_lite.log 2>&1 &

echo "V88 轻量版启动中 (pid=$!)，端口 $PORT"
echo "本机访问: http://127.0.0.1:$PORT"
for i in $(seq 1 15); do
    sleep 1
    curl -s -o /dev/null http://127.0.0.1:$PORT 2>/dev/null && { echo "✅ 已就绪"; break; }
done
