#!/bin/bash
# ================================================================
# AI 皇冠双核 V88 - 守护启动脚本
# 功能：
#   1. 自动检测并安装依赖
#   2. 守护进程模式：进程意外退出后 5 秒自动重拉
#   3. app 内部每 24 小时触发 os.execv 重启，本脚本自动接管
# ================================================================

set -e
cd "$(dirname "$0")"

echo "🚀 AI皇冠双核 V88 - 守护模式启动"
echo "================================================================"

# ── 依赖检查 ─────────────────────────────────────────────────────
python3 -c "import curl_cffi; v=getattr(curl_cffi,'__version__','0'); assert tuple(int(x) for x in v.split('.')[:2]) >= (0,7), f'版本过低 {v}'" 2>/dev/null || {
    echo "📦 安装/升级 curl_cffi..."
    pip3 install "curl_cffi>=0.7.0" -q
}

python3 -c "import flask" 2>/dev/null || {
    echo "📦 安装 flask..."
    pip3 install flask -q
}

python3 -c "import openpyxl" 2>/dev/null || {
    echo "📦 安装 openpyxl..."
    pip3 install openpyxl -q
}

echo "✅ 依赖检查完成"
echo ""

# ── 守护循环 ─────────────────────────────────────────────────────
RESTART_COUNT=0

while true; do
    RESTART_COUNT=$((RESTART_COUNT + 1))
    START_TS=$(date "+%Y-%m-%d %H:%M:%S")

    if [ $RESTART_COUNT -eq 1 ]; then
        echo "▶️  [$START_TS] 首次启动..."
    else
        echo "♻️  [$START_TS] 第 $RESTART_COUNT 次启动（自动重启）..."
    fi

    echo "   访问地址: http://localhost:8501"
    echo "   按 Ctrl+C 停止守护进程"
    echo "----------------------------------------------------------------"

    # 启动 Streamlit（headless=false 让系统自动打开浏览器，仅首次）
    if [ $RESTART_COUNT -eq 1 ]; then
        streamlit run app_v88_integrated.py \
            --server.headless false \
            --server.port 8501 \
            --browser.serverAddress localhost
    else
        # 重启时不再重新打开浏览器
        streamlit run app_v88_integrated.py \
            --server.headless true \
            --server.port 8501
    fi

    EXIT_CODE=$?
    END_TS=$(date "+%Y-%m-%d %H:%M:%S")

    # Ctrl+C (exit code 130) → 用户主动退出，不再重启
    if [ $EXIT_CODE -eq 130 ] || [ $EXIT_CODE -eq 2 ]; then
        echo ""
        echo "⛔ [$END_TS] 用户主动停止，退出守护进程。"
        break
    fi

    echo "⚠️  [$END_TS] 进程退出 (code=$EXIT_CODE)，5 秒后自动重启..."
    sleep 5
done

echo "================================"
echo "V88 守护进程已停止"
echo "================================"
