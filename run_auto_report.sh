#!/bin/bash
# run_auto_report.sh — V88 AI 钉钉日报（A+B+C，Part D 已停用）
# 使用 quant_worker venv（含 yfinance / pandas / requests 等）
# 环境变量从 /root/.env.report 读取

LOG="/var/log/quant/auto_report.log"
mkdir -p "$(dirname $LOG)"

echo "=== $(date '+%Y-%m-%d %H:%M:%S') 开始生成日报 ===" >> "$LOG"

# 加载环境变量
ENV_FILE="/root/.env.report"
if [ -f "$ENV_FILE" ]; then
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "⚠️  未找到 $ENV_FILE，跳过" >> "$LOG"
    exit 1
fi

PYTHON="/opt/quant_worker/venv/bin/python3"
SCRIPT="/root/auto_reporter.py"

# 判断早报/晚报
HOUR=$(date +%H)
if [ "$HOUR" -lt 12 ]; then
    TYPE="morning"
else
    TYPE="evening"
fi

"$PYTHON" "$SCRIPT" "$TYPE" >> "$LOG" 2>&1
EXIT_CODE=$?

echo "=== $(date '+%Y-%m-%d %H:%M:%S') 完成 exit=$EXIT_CODE ===" >> "$LOG"
exit $EXIT_CODE
