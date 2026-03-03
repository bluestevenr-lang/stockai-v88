#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
钉钉对话机器人 - 接收用户指令并更新持仓股
Author: V88团队
Version: 1.0
"""

import os
import sys
import json
import hmac
import time
import base64
import hashlib
import urllib.parse
import urllib.request
import ssl
import pandas as pd
from flask import Flask, request, jsonify
from datetime import datetime

# ═══════════════════════════════════════════════════════════════
# UTF-8 强制编码
# ═══════════════════════════════════════════════════════════════
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
os.environ['PYTHONIOENCODING'] = 'utf-8'

# ═══════════════════════════════════════════════════════════════
# SSL 证书处理
# ═══════════════════════════════════════════════════════════════
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

# ═══════════════════════════════════════════════════════════════
# 配置
# ═══════════════════════════════════════════════════════════════
PORTFOLIO_FILE = "my_portfolio.xlsx"
DINGTALK_WEBHOOK = os.environ.get('DINGTALK_WEBHOOK', '')
DINGTALK_SECRET = os.environ.get('DINGTALK_SECRET', '')

app = Flask(__name__)

# ═══════════════════════════════════════════════════════════════
# 钉钉消息发送
# ═══════════════════════════════════════════════════════════════
def send_to_dingtalk(title, content):
    """发送markdown消息到钉钉"""
    if not DINGTALK_WEBHOOK or not DINGTALK_SECRET:
        print("⚠️  钉钉配置缺失")
        return False
    
    try:
        # 生成签名
        timestamp = str(round(time.time() * 1000))
        secret_enc = DINGTALK_SECRET.encode('utf-8')
        string_to_sign = f'{timestamp}\n{DINGTALK_SECRET}'
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code).decode('ascii'))
        
        # 构建消息（包含关键词"日报"）
        message = {
            "msgtype": "markdown",
            "markdown": {
                "title": f"📢 {title}",
                "text": f"### 📢 {title}\n\n{content}\n\n---\n*V88 AI日报系统*"
            }
        }
        
        # 发送请求
        webhook_url = f"{DINGTALK_WEBHOOK}&timestamp={timestamp}&sign={sign}"
        data = json.dumps(message, ensure_ascii=False).encode('utf-8')
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        req = urllib.request.Request(webhook_url, data=data, headers=headers)
        
        context = ssl._create_unverified_context()
        with urllib.request.urlopen(req, timeout=10, context=context) as response:
            result = json.loads(response.read().decode('utf-8'))
            if result.get('errcode') == 0:
                print(f"✅ 钉钉消息发送成功")
                return True
            else:
                print(f"❌ 钉钉返回错误: {result}")
                return False
                
    except Exception as e:
        print(f"❌ 钉钉发送异常: {e}")
        return False

# ═══════════════════════════════════════════════════════════════
# 持仓股管理
# ═══════════════════════════════════════════════════════════════
def load_portfolio():
    """加载持仓股"""
    try:
        if os.path.exists(PORTFOLIO_FILE):
            df = pd.read_excel(PORTFOLIO_FILE)
            return df
        else:
            # 创建空表
            df = pd.DataFrame(columns=['股票代码', '股票名称', '持仓成本', '持仓数量', '当前价格', '盈亏'])
            df.to_excel(PORTFOLIO_FILE, index=False)
            return df
    except Exception as e:
        print(f"❌ 加载持仓失败: {e}")
        return pd.DataFrame(columns=['股票代码', '股票名称', '持仓成本', '持仓数量', '当前价格', '盈亏'])

def save_portfolio(df):
    """保存持仓股"""
    try:
        df.to_excel(PORTFOLIO_FILE, index=False)
        print(f"✅ 持仓已保存到 {PORTFOLIO_FILE}")
        return True
    except Exception as e:
        print(f"❌ 保存持仓失败: {e}")
        return False

def add_stock(code, name="", cost=0, quantity=0):
    """添加股票到持仓"""
    try:
        df = load_portfolio()
        
        # 检查是否已存在
        if code in df['股票代码'].values:
            return False, f"股票 {code} 已存在持仓中"
        
        # 添加新股票
        new_row = {
            '股票代码': code,
            '股票名称': name or code,
            '持仓成本': cost,
            '持仓数量': quantity,
            '当前价格': 0,
            '盈亏': 0
        }
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        
        if save_portfolio(df):
            return True, f"✅ 已添加 {code} {name}"
        else:
            return False, "保存失败"
            
    except Exception as e:
        return False, f"添加失败: {e}"

def remove_stock(code):
    """从持仓移除股票"""
    try:
        df = load_portfolio()
        
        # 检查是否存在
        if code not in df['股票代码'].values:
            return False, f"股票 {code} 不在持仓中"
        
        # 删除股票
        df = df[df['股票代码'] != code]
        
        if save_portfolio(df):
            return True, f"✅ 已移除 {code}"
        else:
            return False, "保存失败"
            
    except Exception as e:
        return False, f"移除失败: {e}"

def get_portfolio_summary():
    """获取持仓摘要"""
    try:
        df = load_portfolio()
        if df.empty:
            return "📊 当前持仓为空"
        
        summary = "📊 **当前持仓**\n\n"
        for _, row in df.iterrows():
            summary += f"- {row['股票代码']} {row['股票名称']}\n"
        summary += f"\n共 {len(df)} 只股票"
        return summary
        
    except Exception as e:
        return f"❌ 获取持仓失败: {e}"

# ═══════════════════════════════════════════════════════════════
# 消息解析
# ═══════════════════════════════════════════════════════════════
def parse_command(text):
    """解析用户指令
    
    支持格式：
    - 持仓股：增加 AAPL
    - 持仓股：增加 AAPL 苹果 150.5 100
    - 持仓股：删除 AAPL
    - 持仓股：查询
    """
    text = text.strip()
    
    # 增加股票
    if "持仓股：增加" in text or "持仓股:增加" in text:
        parts = text.replace("持仓股：增加", "").replace("持仓股:增加", "").strip().split()
        if len(parts) >= 1:
            code = parts[0]
            name = parts[1] if len(parts) > 1 else ""
            cost = float(parts[2]) if len(parts) > 2 else 0
            quantity = float(parts[3]) if len(parts) > 3 else 0
            return "add", (code, name, cost, quantity)
    
    # 删除股票
    elif "持仓股：删除" in text or "持仓股:删除" in text:
        code = text.replace("持仓股：删除", "").replace("持仓股:删除", "").strip()
        if code:
            return "remove", code
    
    # 查询持仓
    elif "持仓股：查询" in text or "持仓股:查询" in text or "持仓股" in text:
        return "query", None
    
    return None, None

# ═══════════════════════════════════════════════════════════════
# Flask 路由
# ═══════════════════════════════════════════════════════════════
@app.route('/webhook', methods=['POST'])
def webhook():
    """接收钉钉消息"""
    try:
        data = request.get_json()
        print(f"📥 收到钉钉消息: {json.dumps(data, ensure_ascii=False)}")
        
        # 提取消息内容
        if 'text' in data and 'content' in data['text']:
            content = data['text']['content'].strip()
            
            # 解析指令
            cmd, args = parse_command(content)
            
            if cmd == "add":
                success, msg = add_stock(*args)
                send_to_dingtalk("持仓更新日报", msg)
                
            elif cmd == "remove":
                success, msg = remove_stock(args)
                send_to_dingtalk("持仓更新日报", msg)
                
            elif cmd == "query":
                summary = get_portfolio_summary()
                send_to_dingtalk("持仓查询日报", summary)
            
            else:
                print(f"⚠️  未识别的指令: {content}")
        
        return jsonify({"success": True})
        
    except Exception as e:
        print(f"❌ 处理消息异常: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    """健康检查"""
    return jsonify({"status": "ok", "time": datetime.now().isoformat()})

# ═══════════════════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════════════════
if __name__ == '__main__':
    print("🤖 钉钉对话机器人启动中...")
    print(f"📂 持仓文件: {PORTFOLIO_FILE}")
    print(f"🌐 Webhook地址: http://localhost:5000/webhook")
    print("=" * 60)
    
    # 启动Flask服务器
    app.run(host='0.0.0.0', port=5000, debug=False)
