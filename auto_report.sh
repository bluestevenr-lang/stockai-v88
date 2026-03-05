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

    # 环境变量（来自 .streamlit/secrets.toml 对应值）
    export GEMINI_API_KEY="AIzaSyBMOzUpUngDAnfXIae_VQdz3Gj-xCECR5w"
    export TUSHARE_TOKEN="b59adc9011f54ebdc0e3197d6e6c0a0536a0c31d88d9153d67ac7711"
    export DINGTALK_WEBHOOK="https://oapi.dingtalk.com/robot/send?access_token=3c50ad18b1ce2c55c6e7106a79e7551a37141c87ed399d6a26cd4db6f768ed24"
    export DINGTALK_SECRET="SECc40f49d626b6fa5bfb4f6e8562ecc71124271e566da3e3dcc27821de52be0d0f"
    export DINGTALK_KEYWORD=""

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
