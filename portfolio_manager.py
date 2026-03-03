#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
持仓股管理器 - 通过命令行或钉钉消息更新持仓股
"""

import os
import sys
import pandas as pd
from datetime import datetime

# 强制UTF-8编码（Streamlit 等环境下 stdout 可能已关闭，需容错）
import io
try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
except (ValueError, OSError, AttributeError):
    pass  # 环境不支持时跳过，避免 I/O operation on closed file

PORTFOLIO_FILE = 'my_portfolio.xlsx'

def load_portfolio():
    """加载当前持仓"""
    try:
        df = pd.read_excel(PORTFOLIO_FILE)
        return df
    except Exception as e:
        print(f"❌ 读取持仓失败: {e}")
        return None

def save_portfolio(df):
    """保存持仓到Excel"""
    try:
        df.to_excel(PORTFOLIO_FILE, index=False)
        print(f"✅ 持仓已保存到 {PORTFOLIO_FILE}")
        return True
    except Exception as e:
        print(f"❌ 保存失败: {e}")
        return False

def show_portfolio():
    """显示当前持仓"""
    df = load_portfolio()
    if df is not None:
        print("\n" + "="*60)
        print("📊 当前持仓股")
        print("="*60)
        print(df[['股票代码', '股票名称', '持仓数量', '买入价格']].to_string(index=False))
        print(f"\n共 {len(df)} 只股票")
        print("="*60)
        return df
    return None

def add_stock(code, name, quantity, price, buy_date="", note=""):
    """添加持仓股"""
    df = load_portfolio()
    if df is None:
        return False
    
    # 检查是否已存在
    if code in df['股票代码'].values:
        print(f"⚠️ {code} 已存在，请使用更新功能")
        return False
    
    # 添加新行
    new_row = {
        '股票代码': code,
        '股票名称': name,
        '持仓数量': quantity,
        '买入价格': price,
        '买入日期': buy_date or datetime.now().strftime('%Y-%m-%d'),
        '备注': note
    }
    
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    
    if save_portfolio(df):
        print(f"✅ 已添加: {name}({code}) - {quantity}股 @ {price}")
        return True
    return False

def remove_stock(code):
    """删除持仓股"""
    df = load_portfolio()
    if df is None:
        return False
    
    if code not in df['股票代码'].values:
        print(f"⚠️ {code} 不存在")
        return False
    
    # 获取股票信息
    stock_info = df[df['股票代码'] == code].iloc[0]
    name = stock_info['股票名称']
    
    # 删除
    df = df[df['股票代码'] != code]
    
    if save_portfolio(df):
        print(f"✅ 已删除: {name}({code})")
        return True
    return False

def update_stock(code, quantity=None, price=None):
    """更新持仓股"""
    df = load_portfolio()
    if df is None:
        return False
    
    if code not in df['股票代码'].values:
        print(f"⚠️ {code} 不存在")
        return False
    
    # 更新
    if quantity is not None:
        df.loc[df['股票代码'] == code, '持仓数量'] = quantity
    if price is not None:
        df.loc[df['股票代码'] == code, '买入价格'] = price
    
    if save_portfolio(df):
        updates = []
        if quantity: updates.append(f"数量={quantity}")
        if price: updates.append(f"价格={price}")
        print(f"✅ 已更新 {code}: {', '.join(updates)}")
        return True
    return False

def parse_dingtalk_command(text):
    """
    解析钉钉指令
    
    支持格式：
    1. 添加持仓 AAPL 苹果 100 150
    2. 删除持仓 AAPL
    3. 更新持仓 AAPL 数量=200
    4. 查看持仓
    """
    text = text.strip()
    
    if text == "查看持仓" or text == "持仓":
        return ('show', None)
    
    parts = text.split()
    
    if len(parts) >= 2:
        action = parts[0]
        
        if action == "添加持仓" and len(parts) >= 5:
            # 添加持仓 AAPL 苹果 100 150
            return ('add', {
                'code': parts[1],
                'name': parts[2],
                'quantity': int(parts[3]),
                'price': float(parts[4])
            })
        
        elif action == "删除持仓" and len(parts) >= 2:
            # 删除持仓 AAPL
            return ('remove', {'code': parts[1]})
        
        elif action == "更新持仓" and len(parts) >= 3:
            # 更新持仓 AAPL 数量=200 或 更新持仓 AAPL 价格=160
            result = {'code': parts[1]}
            for param in parts[2:]:
                if '=' in param:
                    key, value = param.split('=')
                    if key == '数量':
                        result['quantity'] = int(value)
                    elif key == '价格':
                        result['price'] = float(value)
            return ('update', result)
    
    return (None, None)

def main():
    """主函数"""
    if len(sys.argv) == 1:
        # 交互模式
        print("\n" + "="*60)
        print("💼 持仓股管理器")
        print("="*60)
        print("\n命令示例：")
        print("  1. 查看持仓")
        print("  2. 添加持仓 AAPL 苹果 100 150")
        print("  3. 删除持仓 AAPL")
        print("  4. 更新持仓 AAPL 数量=200")
        print("  5. 更新持仓 AAPL 价格=160")
        print("\n输入命令（输入 q 退出）：")
        
        while True:
            try:
                command = input("\n> ").strip()
                if command.lower() == 'q':
                    break
                
                action, params = parse_dingtalk_command(command)
                
                if action == 'show':
                    show_portfolio()
                elif action == 'add':
                    add_stock(**params)
                elif action == 'remove':
                    remove_stock(**params)
                elif action == 'update':
                    update_stock(**params)
                else:
                    print("❌ 无效命令")
                    
            except KeyboardInterrupt:
                print("\n\n👋 再见！")
                break
            except Exception as e:
                print(f"❌ 错误: {e}")
    
    else:
        # 命令行模式
        command = ' '.join(sys.argv[1:])
        action, params = parse_dingtalk_command(command)
        
        if action == 'show':
            show_portfolio()
        elif action == 'add':
            add_stock(**params)
        elif action == 'remove':
            remove_stock(**params)
        elif action == 'update':
            update_stock(**params)
        else:
            print("❌ 无效命令")
            print("\n使用示例：")
            print("  python3 portfolio_manager.py 查看持仓")
            print("  python3 portfolio_manager.py 添加持仓 AAPL 苹果 100 150")

if __name__ == "__main__":
    main()
