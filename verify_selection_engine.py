#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
选股引擎验证脚本
运行：python verify_selection_engine.py
输出：子池大小、候选池大小、覆盖率
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def main():
    from modules.stock_pool import init_stock_pools
    from modules.selection_engine import build_daily_candidates, verify_and_print

    # 模拟 fetch_stock_data（简化版，仅用于验证流程）
    try:
        import yfinance as yf
        def _fetch(code):
            try:
                df = yf.Ticker(code).history(period="1y", timeout=5)
                return df
            except Exception:
                return None
    except ImportError:
        print("需要安装 yfinance: pip install yfinance")
        return 1

    print("正在加载股票池...")
    us_pool, hk_pool, cn_pool = init_stock_pools()
    print(f"母池: US={len(us_pool)} HK={len(hk_pool)} CN={len(cn_pool)}")

    print("\n正在构建候选池（684池筛选）...")
    data = build_daily_candidates(us_pool, hk_pool, cn_pool, fetch_fn=_fetch, use_cache=False)

    print("\n" + "=" * 60)
    verify_and_print(data)
    print("\n【与现有 AI 日报选股对比】")
    print("| 项目 | 原逻辑 pool[:15] | 新逻辑 684池筛选 |")
    print("|------|-----------------|------------------|")
    print("| 候选来源 | 每市场前15只固定 | 子池轮换+ST/MT/LT分类 |")
    print("| 候选数量 | 15×3=45 | 60×3×3=540 (每市场ST/MT/LT各60) |")
    print("| 覆盖率 | 无 | 5-7天覆盖全池 |")
    print("| 输出格式 | 每市场3只 | 每市场ST/MT/LT各1只 |")
    print("| Token 控制 | 无 | 紧凑JSON，每只≈35字符 |")
    return 0

if __name__ == "__main__":
    sys.exit(main())
