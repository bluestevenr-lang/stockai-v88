#!/bin/bash
# 【2026-07-27】单页备份版固定8502端口(回滚检查用),永不与主分页版(8501)抢端口
# ================================================================
# AI 皇冠双核 V88 - 守护启动脚本
# 功能：
#   1. 自动检测并安装依赖（失败静默跳过，不影响启动）
#   2. 守护进程模式：进程意外退出后 5 秒自动重拉
#   3. app 内部每天凌晨零点自动清零所有缓存并重载
# 说明：本脚本与 v88_master_launch.sh 使用一致的运行环境
#       （arm64 + 框架版 Python + 代理 + Kimi Code订阅Key）
# ================================================================

# 注意：这里【不能】用 set -e。
# 守护循环依赖捕获 streamlit 的非零退出码后重启，
# set -e 会在 streamlit 非零退出时直接终止脚本，使重启逻辑失效。

cd "$(dirname "$0")"

echo "🚀 AI皇冠双核 V88 - 守护模式启动"
echo "================================================================"

# ── 选择 Python 解释器（优先框架版 3.14，回退到 python3）──────────
PYTHON_BIN="/Library/Frameworks/Python.framework/Versions/3.14/bin/python3.14"
if [ ! -x "$PYTHON_BIN" ]; then
    PYTHON_BIN="$(command -v python3)"
fi

# Apple Silicon 上强制 arm64；其他架构留空
ARCH_PREFIX=""
if [ "$(uname -m)" = "arm64" ]; then
    ARCH_PREFIX="arch -arm64"
fi

RUN_STREAMLIT() {
    # 用 -m streamlit 而非裸 streamlit，避免 Finder 双击时 PATH 缺失
    $ARCH_PREFIX "$PYTHON_BIN" -m streamlit "$@"
}

# ── 代理与 API Key（与 v88_master_launch.sh 保持一致）─────────────
#    yfinance 需要代理才能访问 Yahoo Finance（中国被墙）
#    Clash 会自动将国内流量（如东财）直连，海外流量走代理
export http_proxy="http://127.0.0.1:7897"
export https_proxy="http://127.0.0.1:7897"
export ALL_PROXY="http://127.0.0.1:7897"
export HTTP_PROXY="http://127.0.0.1:7897"
export HTTPS_PROXY="http://127.0.0.1:7897"
ENV_FILE="$HOME/Desktop/StockAI/.env"
if [ -f "$ENV_FILE" ]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

# ── 清理 8502 端口上的旧实例（避免“端口已被占用”启动失败）───────
pkill -9 -f 'streamlit run app_v88_integrated.py' 2>/dev/null
for p in $(lsof -ti:8504 2>/dev/null); do
    # 只清占8502的streamlit自身,不误杀恰好占端口的无关程序(2026-07-25审计修复)
    ps -p "$p" -o command= 2>/dev/null | grep -q "streamlit" && kill "$p" 2>/dev/null
done
sleep 1

# ── 依赖检查（静默安装，pip 失败不影响启动）─────────────────────
_install_if_missing() {
    local pkg=$1 install_spec=${2:-$1}
    "$PYTHON_BIN" -c "import $pkg" 2>/dev/null || {
        echo "📦 安装 $install_spec..."
        "$PYTHON_BIN" -m pip install "$install_spec" -q 2>/dev/null || echo "⚠️  $install_spec 安装失败，已跳过（不影响主功能）"
    }
}
_install_if_missing curl_cffi "curl_cffi>=0.7.0"
_install_if_missing flask flask
_install_if_missing openpyxl openpyxl
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

    echo "   访问地址: http://localhost:8504"
    echo "   按 Ctrl+C 停止守护进程"
    echo "----------------------------------------------------------------"

    # 启动 Streamlit（headless=false 让系统自动打开浏览器，仅首次）
    if [ $RESTART_COUNT -eq 1 ]; then
        RUN_STREAMLIT run app_v88_integrated.py \
            --server.headless false \
            --server.port 8504 \
            --browser.serverAddress localhost
    else
        # 重启时不再重新打开浏览器
        RUN_STREAMLIT run app_v88_integrated.py \
            --server.headless true \
            --server.port 8504
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
