#!/bin/zsh
# V88 钉钉 AI 日报定时推送脚本
# 由 LaunchAgent 每天 07:30（早报）和 20:00（晚报）调用

PROJECT_DIR="${HOME}/Desktop/StockAI"
PYTHON_BIN="/Library/Frameworks/Python.framework/Versions/3.14/bin/python3.14"
LOG_DIR="${PROJECT_DIR}/logs"
mkdir -p "${LOG_DIR}"

LOG_FILE="${LOG_DIR}/autoreporter_$(date '+%Y%m%d_%H%M').log"

{
    echo "=== $(date '+%F %T') 钉钉AI日报启动 ==="

    # ── 从 .env 加载环境变量（密钥不写入脚本）──────────────────────────────
    ENV_FILE="${PROJECT_DIR}/.env"
    if [ -f "${ENV_FILE}" ]; then
        echo "📂 加载 .env 配置..."
        # set -a 让所有赋值自动 export；source 直接执行 .env；set +a 恢复
        set -a
        source "${ENV_FILE}"
        set +a
    else
        echo "⚠️  未找到 .env 文件：${ENV_FILE}"
        echo "   请复制 .env.example 为 .env 并填入密钥"
        exit 1
    fi

    cd "${PROJECT_DIR}" || { echo "❌ 无法进入项目目录"; exit 1; }

    # 确保新版 Gemini SDK 已安装
    "${PYTHON_BIN}" -c "from google import genai" 2>/dev/null || {
        echo "📦 安装 google-genai..."
        "${PYTHON_BIN}" -m pip install -q google-genai
    }

    # 根据当前时间自动决定早报/晚报（auto_reporter.py 内部也有判断，双重保险）
    HOUR=$(date '+%H')
    if [ "${HOUR}" -lt 12 ]; then
        REPORT_TYPE="morning"
    else
        REPORT_TYPE="evening"
    fi

    echo "📰 报告类型: ${REPORT_TYPE}"
    echo "🐍 Python: ${PYTHON_BIN}"

    "${PYTHON_BIN}" "${PROJECT_DIR}/auto_reporter.py" "${REPORT_TYPE}"
    EXIT_CODE=$?

    if [ ${EXIT_CODE} -eq 0 ]; then
        echo "✅ $(date '+%F %T') 推送完成"
    else
        echo "❌ $(date '+%F %T') 推送失败（退出码: ${EXIT_CODE}）"
    fi

    echo "=== $(date '+%F %T') 运行结束 ==="
} 2>&1 | tee -a "${LOG_FILE}"

# 保留最近 30 个日志文件，清理旧日志
ls -t "${LOG_DIR}"/autoreporter_*.log 2>/dev/null | tail -n +31 | xargs rm -f 2>/dev/null
