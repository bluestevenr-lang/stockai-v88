#!/bin/bash
echo "🚀 AI皇冠双核 V88 - 启动中..."
echo "================================"

cd "$(dirname "$0")"

# 检查并安装 curl_cffi（新版 yfinance 必须依赖）
python3 -c "import curl_cffi" 2>/dev/null || {
    echo "📦 安装 curl_cffi..."
    pip3 install "curl_cffi>=0.6.0" -q
}

# 后台启动 Streamlit
echo "✅ 启动服务..."
streamlit run app_v88_integrated.py \
    --server.headless true \
    --server.port 8501 \
    --browser.serverAddress localhost &

STREAMLIT_PID=$!

# 等待服务就绪后自动打开浏览器（macOS）
echo "⏳ 等待服务就绪..."
sleep 4
open "http://localhost:8501"

echo ""
echo "================================"
echo "✅ 已在浏览器打开：http://localhost:8501"
echo "   按 Ctrl+C 停止服务"
echo "================================"

# 等待 Streamlit 进程结束
wait $STREAMLIT_PID
