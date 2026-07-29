#!/bin/bash
# quant_vps_diagnose.sh — 在 VPS 上运行，诊断量化页面无数据原因
# 用法：ssh root@107.172.62.217 "bash -s" < scripts/quant_vps_diagnose.sh
# 或先 scp 到 VPS 再执行：scp scripts/quant_vps_diagnose.sh root@107.172.62.217:/opt/quant_worker/ && ssh root@107.172.62.217 "bash /opt/quant_worker/quant_vps_diagnose.sh"

set -e
WORK_DIR="${WORK_DIR:-/opt/quant_worker}"
LOG_DIR="${LOG_DIR:-/var/log/quant}"

echo "════════════════════════════════════════════"
echo "  量化 Worker 诊断 (V88 页面无数据排查)"
echo "════════════════════════════════════════════"
echo ""

echo "▶ 1. 服务器时区（应为 UTC，策略内部用北京时）"
date
echo "   TZ=$TZ"
timedatectl 2>/dev/null || true
echo ""

echo "▶ 2. run_quant.sh 是否存在、是否调用 main.py --once --cloud"
if [ -f "$WORK_DIR/run_quant.sh" ]; then
  echo "   存在。内容摘要："
  grep -E "main\.py|quant_worker\.py|source|\.env" "$WORK_DIR/run_quant.sh" || true
else
  echo "   ❌ 不存在！请重新运行 quant_vps_setup.sh"
fi
echo ""

echo "▶ 3. .env.quant 是否配置 GIST（不打印具体值）"
if [ -f "$WORK_DIR/.env.quant" ]; then
  if grep -q "^GIST_TOKEN=.*" "$WORK_DIR/.env.quant" && ! grep -q "^GIST_TOKEN=your_github_token" "$WORK_DIR/.env.quant" && ! grep -q "^GIST_TOKEN=\s*$" "$WORK_DIR/.env.quant"; then
    echo "   GIST_TOKEN=✅ 已设置"
  else
    echo "   ❌ GIST_TOKEN 未设置或仍是占位符，页面不会更新"
  fi
  if grep -q "^GIST_ID=.*" "$WORK_DIR/.env.quant" && ! grep -q "^GIST_ID=your_gist_id" "$WORK_DIR/.env.quant" && ! grep -q "^GIST_ID=\s*$" "$WORK_DIR/.env.quant"; then
    echo "   GIST_ID=✅ 已设置"
  else
    echo "   ❌ GIST_ID 未设置或仍是占位符"
  fi
else
  echo "   ❌ 不存在 $WORK_DIR/.env.quant"
fi
echo ""

echo "▶ 4. cron 是否安装量化任务"
crontab -l 2>/dev/null | grep -E "quant_worker|run_quant" || echo "   ❌ 未发现量化相关 cron"
echo ""

echo "▶ 5. 最近 30 行 quant_worker 日志"
if [ -f "$LOG_DIR/quant_worker.log" ]; then
  tail -30 "$LOG_DIR/quant_worker.log"
else
  echo "   ❌ 日志文件不存在（可能从未跑过 run_quant.sh）"
fi
echo ""

echo "▶ 6. 手动执行一次扫描（--once --cloud --force）看是否报错并同步 Gist"
cd "$WORK_DIR" || exit 1
if [ -f "$WORK_DIR/venv/bin/python3" ] && [ -f "$WORK_DIR/main.py" ]; then
  set -a
  [ -f .env.quant ] && source .env.quant
  set +a
  echo "   执行: $WORK_DIR/venv/bin/python3 main.py --once --cloud --force"
  "$WORK_DIR/venv/bin/python3" "$WORK_DIR/main.py" --once --cloud --force 2>&1 | tail -50
else
  echo "   ❌ venv 或 main.py 不存在"
fi
echo ""
echo "════════════════════════════════════════════"
echo "  若上面有 [GIST] 状态已同步，说明推送正常；若为「未配置」请填写 .env.quant"
echo "════════════════════════════════════════════"
