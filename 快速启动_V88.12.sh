#!/bin/bash
echo "🚀 AI皇冠双核 V88 - 启动检查..."
echo "================================"

# 检查并安装 curl_cffi（新版 yfinance 必须依赖）
python3 -c "import curl_cffi" 2>/dev/null || {
    echo "📦 安装 curl_cffi（yfinance 必须）..."
    pip3 install "curl_cffi>=0.6.0" -q
}

# 检查 yfinance 版本
python3 -c "
import yfinance as yf
v = yf.__version__
print(f'✅ yfinance {v}')
" 2>/dev/null || {
    echo "📦 安装 yfinance..."
    pip3 install "yfinance>=0.2.37" -q
}

echo "✅ 依赖检查完成，启动应用..."
echo ""
cd "$(dirname "$0")"
streamlit run app_v88_integrated.py --server.headless true
