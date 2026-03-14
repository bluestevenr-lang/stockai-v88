#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# quant_vps_setup.sh — VPS 量化Worker 一键安装（在VPS上运行）
# 运行方式：bash quant_vps_setup.sh
# ═══════════════════════════════════════════════════════════════
set -e

echo "══════════════════════════════════════════"
echo "  量化模拟 Worker · VPS 环境安装"
echo "══════════════════════════════════════════"

WORK_DIR="/opt/quant_worker"
ENV_FILE="$WORK_DIR/.env.quant"
LOG_DIR="/var/log/quant"
CRON_TAG="# quant_worker_cron"

# ── 1. 安装 Python 依赖 ──────────────────────────────────────
echo ""
echo "▶ [1/5] 安装 Python 依赖..."
apt-get update -qq
apt-get install -y -qq python3 python3-pip python3-venv curl

# 创建虚拟环境（避免与系统包冲突）
if [ ! -d "$WORK_DIR/venv" ]; then
    python3 -m venv "$WORK_DIR/venv"
fi

"$WORK_DIR/venv/bin/pip" install -q --upgrade pip
"$WORK_DIR/venv/bin/pip" install -q \
    "yfinance>=0.2.37" \
    pandas \
    numpy \
    requests \
    "schedule>=1.2.0"

echo "   ✅ Python 环境就绪: $WORK_DIR/venv"

# ── 2. 创建日志目录 ──────────────────────────────────────────
echo ""
echo "▶ [2/5] 创建日志目录..."
mkdir -p "$LOG_DIR"
echo "   ✅ 日志目录: $LOG_DIR/quant_worker.log"

# ── 3. 写入环境变量文件（如果不存在） ────────────────────────
echo ""
echo "▶ [3/5] 检查密钥配置文件..."
if [ ! -f "$ENV_FILE" ]; then
    cat > "$ENV_FILE" << 'ENVEOF'
# ── 量化Worker 密钥配置 ──────────────────────────────────────
# 填入你的真实值，保存后 cron 自动生效

# GitHub Gist（用于在 V88 页面显示结果）
GIST_TOKEN=your_github_token_here
GIST_ID=your_gist_id_here

# 钉钉通知（开仓/平仓实时推送）
DINGTALK_WEBHOOK=https://oapi.dingtalk.com/robot/send?access_token=xxx
DINGTALK_SECRET=SECxxxxxxxxxxx
DINGTALK_KEYWORD=股票行情
ENVEOF
    echo "   ⚠️  已创建密钥文件: $ENV_FILE"
    echo "   ⚠️  请编辑此文件填入你的密钥: nano $ENV_FILE"
else
    echo "   ✅ 密钥文件已存在，跳过覆盖: $ENV_FILE"
fi

# ── 4. 创建运行包装脚本 ──────────────────────────────────────
echo ""
echo "▶ [4/5] 创建运行包装脚本..."

cat > "$WORK_DIR/run_quant.sh" << RUNEOF
#!/bin/bash
# 加载密钥
set -a
source "$ENV_FILE"
set +a

# 切换到工作目录（data/ logs/ 均为相对路径）
cd "$WORK_DIR"

# 运行 Worker（--once 单次扫描，cron 负责调度）
echo "[\$(date '+%Y-%m-%d %H:%M:%S')] 开始扫描..." >> "$LOG_DIR/quant_worker.log"
"$WORK_DIR/venv/bin/python3" "$WORK_DIR/main.py" --once --cloud \
    >> "$LOG_DIR/quant_worker.log" 2>&1
echo "[\$(date '+%Y-%m-%d %H:%M:%S')] 扫描完成" >> "$LOG_DIR/quant_worker.log"
RUNEOF

chmod +x "$WORK_DIR/run_quant.sh"
echo "   ✅ 运行脚本: $WORK_DIR/run_quant.sh"

# ── 5. 安装 cron 任务（覆盖交易三市场） ─────────────────────
echo ""
echo "▶ [5/5] 安装 cron 定时任务..."

# 先删除旧的量化 cron
crontab -l 2>/dev/null | grep -v "$CRON_TAG" | crontab - 2>/dev/null || true

# 写入新 cron（每5分钟，5分钟K线策略，脚本内部判断交易时段自动跳过）
# 覆盖：A股+港股（UTC 01-08）+ 美股（UTC 14-21）+ 跨午夜美股（UTC 22-次日05）
(crontab -l 2>/dev/null; cat << CRONEOF
# ── 量化Worker：每5分钟，5分钟K线策略，脚本内部自动判断是否交易时段 ──
*/5 1-8 * * 1-5 $WORK_DIR/run_quant.sh $CRON_TAG
*/5 14-21 * * 1-5 $WORK_DIR/run_quant.sh $CRON_TAG
*/5 22-23 * * 0-4 $WORK_DIR/run_quant.sh $CRON_TAG
*/5 0-5 * * 2-6 $WORK_DIR/run_quant.sh $CRON_TAG
CRONEOF
) | crontab -

echo "   ✅ cron 任务已安装"
echo ""
echo "══════════════════════════════════════════"
echo "  安装完成！下一步："
echo ""
echo "  1. 填入密钥："
echo "     nano $ENV_FILE"
echo ""
echo "  2. 手动测试一次："
echo "     bash $WORK_DIR/run_quant.sh"
echo ""
echo "  3. 查看实时日志："
echo "     tail -f $LOG_DIR/quant_worker.log"
echo ""
echo "  4. 查看 cron 列表："
echo "     crontab -l"
echo "══════════════════════════════════════════"
