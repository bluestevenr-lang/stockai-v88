#!/bin/zsh
# V88 StockAI 本地启动脚本
# 从 .env 加载密钥，然后启动 Streamlit（网页版 + 手机端共用同一 app）
# 用法：bash ~/Desktop/StockAI/run_app.sh

PROJECT_DIR="${HOME}/Desktop/StockAI"
PYTHON_BIN="/Library/Frameworks/Python.framework/Versions/3.14/bin/python3.14"
ENV_FILE="${PROJECT_DIR}/.env"

# ── 加载 .env ────────────────────────────────────────────────────────────────
if [ ! -f "${ENV_FILE}" ]; then
    echo "❌ 未找到 .env 文件"
    echo "   请先执行：cp ${PROJECT_DIR}/.env.example ${PROJECT_DIR}/.env"
    echo "   然后填入真实密钥"
    exit 1
fi

echo "📂 加载 .env 配置..."
set -a
source "${ENV_FILE}"
set +a

# ── 验证必要密钥 ─────────────────────────────────────────────────────────────
missing=0
for required_key in GEMINI_API_KEY TUSHARE_TOKEN; do
    val=$(eval echo "\$$required_key")
    if [[ -z "$val" || "$val" == 你的* ]]; then
        echo "⚠️  $required_key 未填写，请编辑 .env"
        missing=1
    fi
done
[[ $missing -eq 1 ]] && echo "⚠️  部分密钥未配置，功能可能受限"

echo "✅ 密钥加载完成"
echo "🚀 启动 V88 AI 市场简报..."
echo "   网页版：http://localhost:8501"
echo "   手机端：http://$(ipconfig getifaddr en0 2>/dev/null || echo '本机IP'):8501/?mobile=1"
echo ""

cd "${PROJECT_DIR}" || { echo "❌ 无法进入项目目录"; exit 1; }

"${PYTHON_BIN}" -m streamlit run app_v88_integrated.py \
    --server.port 8501 \
    --server.headless false \
    --browser.gatherUsageStats false
