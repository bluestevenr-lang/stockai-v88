#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# deploy_quant.sh — 本地一键部署量化Worker到VPS（在你Mac上运行）
# 用法：bash deploy_quant.sh <VPS_IP>
# 例如：bash deploy_quant.sh 192.168.1.100
# ═══════════════════════════════════════════════════════════════

VPS_IP="${1}"
VPS_USER="root"
WORK_DIR="/opt/quant_worker"

if [ -z "$VPS_IP" ]; then
    echo "❌ 请提供 VPS IP 地址"
    echo "   用法: bash deploy_quant.sh <VPS_IP>"
    exit 1
fi

echo "══════════════════════════════════════════"
echo "  部署量化 Worker 到 VPS: $VPS_IP"
echo "══════════════════════════════════════════"

# ── 1. 在 VPS 上创建工作目录 ─────────────────────────────────
echo ""
echo "▶ [1/3] 创建 VPS 目录..."
ssh "$VPS_USER@$VPS_IP" "mkdir -p $WORK_DIR"

# ── 2. 上传文件 ──────────────────────────────────────────────
echo ""
echo "▶ [2/3] 上传文件..."
scp quant_worker.py         "$VPS_USER@$VPS_IP:$WORK_DIR/"
scp quant_vps_setup.sh      "$VPS_USER@$VPS_IP:$WORK_DIR/"
echo "   ✅ 文件上传完成"

# ── 3. 在 VPS 上执行安装脚本 ─────────────────────────────────
echo ""
echo "▶ [3/3] 远程执行安装脚本..."
ssh "$VPS_USER@$VPS_IP" "bash $WORK_DIR/quant_vps_setup.sh"

echo ""
echo "══════════════════════════════════════════"
echo "  ✅ 部署完成！"
echo ""
echo "  接下来在 VPS 上填入密钥（只需一次）："
echo "  ssh $VPS_USER@$VPS_IP"
echo "  nano $WORK_DIR/.env.quant"
echo "══════════════════════════════════════════"
