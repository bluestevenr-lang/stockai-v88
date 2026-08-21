#!/bin/bash
# 监控飞书配对码和消息
OC="/c/Users/admin/AppData/Roaming/npm/openclaw.cmd"
LOG="$HOME/AppData/Local/Temp/openclaw/openclaw-$(date +%Y-%m-%d).log"
REPORT="/c/Users/admin/Desktop/StockAI/win/KIMI_WIN_REPORT2.md"
LAST_POS_FILE="/c/Users/admin/Desktop/StockAI/win/.monitor_last_pos"

touch "$REPORT"
if [ -f "$LAST_POS_FILE" ]; then
  LAST_POS=$(cat "$LAST_POS_FILE")
else
  LAST_POS=0
fi

# 获取当前日志大小
if [ -f "$LOG" ]; then
  CUR_POS=$(stat -c%s "$LOG" 2>/dev/null || echo "$LAST_POS")
else
  CUR_POS=$LAST_POS
fi

# 输出新增日志
if [ -f "$LOG" ] && [ "$CUR_POS" -gt "$LAST_POS" ]; then
  tail -c +$((LAST_POS + 1)) "$LOG" | tee -a /c/Users/admin/Desktop/StockAI/win/feishu_raw_log.jsonl
fi
echo "$CUR_POS" > "$LAST_POS_FILE"
