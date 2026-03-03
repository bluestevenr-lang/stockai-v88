#!/usr/bin/env python3
"""
独立脚本：生成 AI 市场简报并打印到终端
用于快速预览新 prompt 的实际输出效果
"""
import os
import sys

# 确保项目根目录在 path 中
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 需要设置环境变量（如果使用代理）
# os.environ.setdefault("HTTP_PROXY", "http://127.0.0.1:1082")
# os.environ.setdefault("HTTPS_PROXY", "http://127.0.0.1:1082")

def main():
    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor, as_completed

    print("正在加载依赖...")
    from app_v88_integrated import (
        fetch_stock_data, init_stock_pools, call_gemini_api,
        ProxyContext, get_proxy_url, GEMINI_MODEL_NAME
    )

    today = datetime.now().strftime("%Y年%m月%d日")
    print(f"\n📅 生成 {today} 市场简报...\n")

    # 获取指数数据
    indices_data = {}
    def _safe_index_change(code, label):
        try:
            _idx_df = fetch_stock_data(code)
            if _idx_df is not None and len(_idx_df) >= 2:
                _last_date = _idx_df.index[-1]
                _prev_date = _idx_df.index[-2]
                _last_close = float(_idx_df['Close'].iloc[-1])
                _prev_close = float(_idx_df['Close'].iloc[-2])
                _chg = ((_last_close - _prev_close) / _prev_close * 100) if _prev_close > 0 else 0
                _last_str = _last_date.strftime('%m/%d') if hasattr(_last_date, 'strftime') else str(_last_date)[-5:]
                _prev_str = _prev_date.strftime('%m/%d') if hasattr(_prev_date, 'strftime') else str(_prev_date)[-5:]
                return f"{label}: {_last_close:.2f}（{_prev_str}→{_last_str} 涨跌 {_chg:+.2f}%）"
        except Exception:
            pass
        return f"{label}: 数据获取中"

    try:
        indices_data['US'] = _safe_index_change("^GSPC", "标普500指数")
        indices_data['HK'] = _safe_index_change("^HSI", "恒生指数")
        indices_data['CN'] = _safe_index_change("000001.SS", "上证综指")
    except Exception:
        pass

    # 获取候选股
    us_pool, hk_pool, cn_pool = init_stock_pools()
    def _get_close_price(yf_code):
        try:
            _df = fetch_stock_data(yf_code)
            if _df is not None and len(_df) > 0:
                return float(_df['Close'].iloc[-1])
        except Exception:
            pass
        return None

    _all_items = (
        [(item, "$") for item in us_pool[:15]] +
        [(item, "HK$") for item in hk_pool[:15]] +
        [(item, "¥") for item in cn_pool[:15]]
    )
    _price_cache = {}
    with ThreadPoolExecutor(max_workers=8) as _exec:
        _futures = {_exec.submit(_get_close_price, it[0][2]): (it[0], it[1]) for it in _all_items}
        for _f in as_completed(_futures):
            _item, _pfx = _futures[_f]
            try:
                _price_cache[(_item[2], _pfx)] = _f.result()
            except Exception:
                _price_cache[(_item[2], _pfx)] = None

    def _fmt_cand(it, pfx):
        p = _price_cache.get((it[2], pfx))
        return f"{it[1]}({it[2]}): 日报价 {pfx}{p:.2f}" if p is not None else f"{it[1]}({it[2]})"

    us_candidates = [_fmt_cand(it, "$") for it in us_pool[:15]]
    hk_candidates = [_fmt_cand(it, "HK$") for it in hk_pool[:15]]
    cn_candidates = [_fmt_cand(it, "¥") for it in cn_pool[:15]]

    prompt = f"""生成今日市场简报，要求专业深度，不要废话称呼。

【日期】{today}

【指数数据】（括号内为数据对比的实际交易日，请严格按此数据引用涨跌幅，不要编造或夸大）
{indices_data.get('US', '美股数据获取中')}
{indices_data.get('HK', '港股数据获取中')}
{indices_data.get('CN', 'A股数据获取中')}

【重要提示】
- 上述涨跌幅是最近两个交易日的对比，不一定是"今天"的数据（可能是昨天或前天收盘）
- 请严格引用上面的数字，不要自己编造涨跌幅
- 如果数据显示"数据获取中"，则该市场部分请写"暂无实时数据"

【⚠️ 推荐个股强制规则】必须严格遵守：
- 美股：只能从以下候选中选择，带「日报价」的必须全文引用：{chr(10).join('- ' + c for c in us_candidates)}
- 港股：只能从以下候选中选择，带「日报价」的必须全文引用：{chr(10).join('- ' + c for c in hk_candidates)}
- A股：只能从以下候选中选择，带「日报价」的必须全文引用：{chr(10).join('- ' + c for c in cn_candidates)}
- 严禁编造股票代码！A股和港股没有字母代码，必须是数字。禁止出现AAAA、BBBB等占位符。
- 每条推荐必须标注「日报价: $/HK$/¥ XX.XX」（与候选数据一致），便于读者识别信息精准。

【⚠️ 日报推荐风格】本简报是「日报」形式，强调事实基本面，推荐理由必须符合日报语境：
- 推荐理由必须以「当日/近期新闻、舆论、热点」为起点，不要写泛泛的长期基本面分析
- 每条推荐要回答：今日/近日有什么新闻或事件，让这只股值得关注？可与上文「重要新闻」呼应
- 可以是短中长期标的，但推荐理由必须新闻驱动、事件驱动、舆情驱动
- 禁止写「核心供应商」「长期战略地位」等空洞官方话术，要具体到「今日/本周发生了什么」

【⚠️ 推荐/WATCH 强制规则】必须严格遵守，让 AI 做事实判断：
- **有近24h/72h触发事件** 且 **能给出三要素** → 输出完整推荐（含目标位、建议）
  - 三要素：① 基本面承接（为何有支撑）② 技术确认（技术面信号）③ 失效条件（什么情况下逻辑失效）
- **缺近24h/72h触发** 或 **三要素任缺一项** → 输出 **WATCH**，禁止给目标涨幅与买入建议
- 每条推荐必须先标注「近24h/72h触发: [具体事件或 无]」，再写推荐理由
- 近24h/72h触发需基于你已知的近期真实新闻、财报、政策等事实，不要编造；若无可靠信息则如实写「无」

请按以下格式输出（不要"尊敬的投资者"等称呼，不要结尾废话）：

📅 **{today} 市场简报**

---

### 📋 市场简报（综合概览）
[2-3句话概括当日美股、港股、A股整体表现，主要驱动因素，资金流向，风险偏好]

---

## 🇺🇸 美股市场

### 📊 市场态势
[当前涨跌情况]，[主要驱动因素2-3句话]

### 📰 重要新闻与影响分析
**新闻1**: [新闻标题或核心内容]
- **影响分析**: [这条新闻对市场的影响，2-3句话]
- **趋势判断**: [短期/中期市场走势预判]

**新闻2**: [新闻标题或核心内容]
- **影响分析**: [这条新闻对市场的影响，2-3句话]
- **趋势判断**: [短期/中期市场走势预判]

### 🎯 推荐个股
1. **[股票名称(代码)]**
   - 日报价: [从候选数据中引用的当日收盘价]
   - 近24h/72h触发: [具体事件，或 无]
   - 推荐理由: 若有触发且三要素齐全 → 写「基本面承接（…）→ 技术确认（…）→ 失效条件（…）」；若无触发或三要素不全 → 写「**状态: WATCH**」+ 简要说明
   - 目标位: [预期涨幅或目标价]（WATCH 时写「无」）
   - 建议: [买入建议]（WATCH 时写「无」）

2. **[股票名称(代码)]**
   - 日报价: [当日收盘价]
   - 近24h/72h触发: [具体事件 或 无]
   - 推荐理由: [同上]
   - 目标位: [或 无]
   - 建议: [或 无]

3. **[股票名称(代码)]**
   - 日报价: [当日收盘价]
   - 近24h/72h触发: [具体事件 或 无]
   - 推荐理由: [同上]
   - 目标位: [或 无]
   - 建议: [或 无]

---

## 🇭🇰 港股市场
[同上格式]

## 🇨🇳 A股市场
[同上格式]

---

**要求**：
- 不要任何称呼和结尾废话
- 新闻分析要深入：新闻内容 + 影响分析 + 趋势判断
- 个股推荐必须从上述候选池选择
- **WATCH 规则**：缺近24h/72h触发 或 三要素任缺一项 → 必须输出 **状态: WATCH**，目标位与建议写「无」
- 内容专业、具体、可操作，参考华尔街日报简报风格"""

    print("正在调用 Gemini API...")
    res = call_gemini_api(prompt)
    print("\n" + "=" * 60)
    print(res)
    print("=" * 60)


if __name__ == "__main__":
    main()
