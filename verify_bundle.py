#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
二层候选验证入口
运行后打印每市场 ST/MT/LT 的 explore/trade 数量与阈值，并能完成一次 Gemini 调用
"""

import os
import sys
from datetime import datetime

# 项目根目录
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def main():
    from modules.stock_pool import init_stock_pools
    from modules.data_fetch import fetch_stock_data
    from modules.selection_engine import (
        build_candidates_bundle,
        verify_bundle_print,
        format_bundle_wsj_candidates,
    )

    print("=" * 60)
    print("【二层候选验证】Explore + Trade")
    print("=" * 60)

    us_pool, hk_pool, cn_pool = init_stock_pools()
    # 快速验证：每市场取前 50 只（完整验证可注释掉下面三行）
    us_pool, hk_pool, cn_pool = us_pool[:50], hk_pool[:50], cn_pool[:50]
    print(f"母池: 美股{len(us_pool)} 港股{len(hk_pool)} A股{len(cn_pool)} (快速模式)")

    date_str = datetime.now().strftime("%Y-%m-%d")
    bundle = build_candidates_bundle(
        us_pool, hk_pool, cn_pool,
        fetch_fn=fetch_stock_data,
        date_str=date_str,
    )
    verify_bundle_print(bundle)

    # 打印 WSJ 格式候选（每市场前 5 只）
    print("\n【WSJ 候选示例】")
    for mkt, pfx in [("US", "$"), ("HK", "HK$"), ("CN", "¥")]:
        cands = format_bundle_wsj_candidates(bundle, mkt, pfx, 5)
        print(f"  {mkt}: {cands}")

    # 可选：Gemini 调用（需配置 GEMINI_API_KEY）
    if os.environ.get("GEMINI_API_KEY"):
        try:
            import google.generativeai as genai
            from zoneinfo import ZoneInfo
            genai.configure(api_key=os.environ["GEMINI_API_KEY"])
            model = genai.GenerativeModel("gemini-2.5-flash")
            today = datetime.now().strftime("%Y年%m月%d日")
            ts = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")
            us_c = format_bundle_wsj_candidates(bundle, "US", "$", 100)
            hk_c = format_bundle_wsj_candidates(bundle, "HK", "HK$", 100)
            cn_c = format_bundle_wsj_candidates(bundle, "CN", "¥", 100)
            prompt = f"""生成 WSJ-style 日报（简化）。日期{today}，校验{ts}。
【候选池】美股：{chr(10).join('- '+c for c in us_c[:20])}
港股：{chr(10).join('- '+c for c in hk_c[:20])}
A股：{chr(10).join('- '+c for c in cn_c[:20])}
【规则】每市场3只：1立即建仓+1中期跟进+1观察。至少2只来自Trade池。
请输出可执行推荐部分即可。"""
            print("\n【Gemini 调用】...")
            resp = model.generate_content(prompt)
            if resp and resp.text:
                print(resp.text[:1500] + "..." if len(resp.text) > 1500 else resp.text)
                print("\n✅ Gemini 调用成功")
            else:
                print("❌ Gemini 返回为空")
        except Exception as e:
            print(f"❌ Gemini 异常: {e}")
    else:
        print("\n⚠️ 未配置 GEMINI_API_KEY，跳过 Gemini 调用")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
