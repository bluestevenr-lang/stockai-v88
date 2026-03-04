#!/bin/bash
echo "🚀 AI皇冠双核 V88 - 启动中..."
echo "================================"

cd "$(dirname "$0")"

# 检查并安装 curl_cffi（yfinance 访问 Yahoo 必需，0.5.10 兼容本地与 Cloud）
python3 -c "import curl_cffi" 2>/dev/null || {
    echo "📦 安装 curl_cffi..."
    pip3 install "curl_cffi==0.5.10" -q
}

# 启动 Streamlit（headless=false 让 Streamlit 自动用系统默认浏览器打开）
echo "✅ 启动服务..."
streamlit run app_v88_integrated.py \
    --server.headless false \
    --server.port 8501 \
    --browser.serverAddress localhost

echo ""
echo "================================"
echo "✅ http://localhost:8501"
echo "   按 Ctrl+C 停止服务"
echo "================================"
