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

# Never copy raw Feishu/OpenClaw logs into this public repository. Raw logs can
# contain user identifiers, pairing details and operational metadata. Record
# only the byte count in the Windows temp directory.
if [ -f "$LOG" ] && [ "$CUR_POS" -gt "$LAST_POS" ]; then
  DELTA=$((CUR_POS - LAST_POS))
  printf '%s new_bytes=%s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$DELTA" \
    >> "$HOME/AppData/Local/Temp/openclaw/feishu_pairing_monitor.status"
fi
echo "$CUR_POS" > "$LAST_POS_FILE"
