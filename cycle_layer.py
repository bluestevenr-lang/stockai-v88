"""
cycle_layer.py — 【V88·三层决策·第①层 周期层】(2026-07-29 用户小米月K案定纲)

用户原话（对着小米 8.31→61.45→31.46 的月线图）：
 "一定要站到支撑线上，过去最低的时候…站到支撑线后又大幅下来洗盘，
  这个说实话已经跌幅超过正常止损线，所以很难通过技术划线来判断，
  ai如果这样只从技术角度就会造成这样的问题"

**他是对的。** 技术线有个无法回避的缺陷：**它区分不了「我判断错了」和「主力在洗盘」**
——两者在图上长得一模一样。用技术止损管大级别行情，结果就是在 11 块被洗出去，
然后眼睁睁看它涨到 61。8.31→61.45 是 +640%，中间任何一次技术止损都会让你出局。

**但那张图本身给出了答案**（实测年报，见 cloud_engine.rev_trend）：

    2023 营收 -3.2%  ← 股价在 10~13 反复洗盘一整年（用户指的正是这段）
    2024 营收 +35%   ← 股价开始起飞
    2025 营收 +25%   ← 冲到 61.45

**洗你那一年，基本面还没拐。** 那一年被止损其实不冤——冤的是没有任何东西告诉你
"营收拐点到了，可以回来了"。技术线答不了"去还是留"，它从来就不负责这个。

于是 V88 改成三层，技术降到最底下：

    ①**周期层**（本模块）  要不要有仓位？  营收趋势+估值+长周期结构  → 决定**方向**
    ②**仓位层**            拿多少？        波动率+事件风险            → 决定**大小**
    ③**技术层**            什么时候加/减？ 支撑阻力量能               → **只调节奏，不管去留**

**核心改写：跌破技术线 ≠ 卖出信号，只是「去复核第①层」的闹钟。**
  · 第①层还成立（营收还在涨）→ 跌破了**反而是加仓机会**
  · 第①层破了（营收连续转负）→ **不用等技术线，直接清**

这条其实早就是 V88 的止损哲学（"价格是复核线非自动卖，逻辑破才清仓"），
但一直只写在纲领里、没落进代码——2026-07-29 给浦发写"止损8.78"时又犯了同样的错。
本模块就是把那句纲领变成可执行的判据。

**诚实边界（铁律2）**：年报取不到时一律 stance="数据不足·退回技术层"，
绝不用残缺的季度同比冒充方向判断（那正是小米被误标"营收-11%"的原因）。

产物 data/cycle_layer.json
"""
import json
import logging
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
OUT = BASE / "data" / "cycle_layer.json"
BJT = timezone(timedelta(hours=8))
sys.path.insert(0, str(BASE / "src"))

# —— 结构位置分档（距区间内最高点的回撤）——
# 用"距高点回撤"而不是"52周位置"，因为回撤直接对应"你现在买是在打几折"。
DEEP_DD = -30.0      # ≤-30% 深度回撤 = 黄金坑候选（前提是营收还在涨）
MID_DD = -10.0       # -30%~-10% 中位
# 深度回撤 + 成长 = 越跌越买；深度回撤 + 衰退 = 价值陷阱。**同样的-50%，两个相反动作。**

GROW = ("加速增长", "稳定增长")
SLOW = ("增长放缓", "停滞")
DECAY = ("衰退",)


def _stance(trend: str, dd: float, pe, eg) -> dict:
    """周期层裁决矩阵——**这张表就是"要不要有仓位"的全部**。

    行=营收趋势（年报口径），列=结构位置（距高点回撤）。
    注意最关键的一格：**营收增长 × 深度回撤 = 逢跌加**，
    这一格如果交给技术线，会因为"跌破所有均线"而判清仓——正是小米 2023 的坑。
    """
    if trend == "数据不足":
        return {"stance": "数据不足·退回技术层", "icon": "⚪", "grade": 0,
                "why": "年报营收取不到，周期层无法判方向——此时技术线是**唯一**可用的，"
                       "但也因此只做短线节奏，不做重仓长持",
                "on_break": "按技术止损执行（无周期层背书时，纪律优先）"}
    if trend in GROW:
        if dd <= DEEP_DD:
            return {"stance": "逢跌加", "icon": "🟢", "grade": 3,
                    "why": f"营收{trend}、股价却距高点{dd:.0f}%——**跌的是估值不是生意**，"
                           "这是黄金坑形态（小米2024年前那一年就是这个格）",
                    "on_break": "**跌破技术线不清仓**，复核营收未变则视为加仓机会"}
        if dd <= MID_DD:
            return {"stance": "持有·可加", "icon": "🟢", "grade": 2,
                    "why": f"营收{trend}、回撤{dd:.0f}%属正常波动，趋势未破",
                    "on_break": "跌破技术线只触发复核，营收未变则持有不动"}
        return {"stance": "持有·不加", "icon": "🟡", "grade": 1,
                "why": f"营收{trend}但已在高位（距高点{dd:.0f}%），"
                       + ("估值已贵，" if (pe or 0) > 40 else "") + "上行空间靠业绩兑现",
                "on_break": "跌破技术线减半（高位无安全垫，不赌洗盘）"}
    if trend in SLOW:
        if dd <= DEEP_DD:
            return {"stance": "观察·底仓", "icon": "🟡", "grade": 1,
                    "why": f"营收{trend}（不是衰退但也没动力）、距高点{dd:.0f}%——"
                           "便宜是真的，但**没有让它涨回去的引擎**，只宜底仓等拐点",
                    "on_break": "跌破技术线减半；营收增速转正前不加"}
        return {"stance": "逢反弹减", "icon": "🟠", "grade": -1,
                "why": f"营收{trend}却还在高位（距高点{dd:.0f}%）——**估值在替业绩透支**",
                "on_break": "跌破技术线执行减仓，不等收复"}
    # 衰退
    if dd <= DEEP_DD:
        return {"stance": "价值陷阱·不碰", "icon": "🔴", "grade": -2,
                "why": f"营收**衰退**且距高点{dd:.0f}%——便宜有便宜的原因，"
                       "深跌+衰退是接飞刀，不是抄底（**这一格才是该跑的**）",
                "on_break": "跌破技术线立即清，不适用任何防洗豁免"}
    return {"stance": "逢反弹清", "icon": "🔴", "grade": -2,
            "why": f"营收**衰退**、股价仍在高位（距高点{dd:.0f}%）——最危险的组合",
            "on_break": "跌破技术线立即清"}


def cycle_verdict(code: str, name: str = "", df=None, fund=None) -> dict:
    """第①层裁决：这只票**要不要有仓位**（不回答"今天买不买"，那是第③层的事）。

    三条证据：
      A 营收趋势（年报口径 rev_trend）——决定"是回调还是转折"
      B 估值（PE/PB + 是否用利润换规模）——决定"跌够了没有"
      C 长周期结构（距高点回撤 + 距底部涨幅 + 60/120/250 均线序）——决定"在周期哪一段"

    返回 {stance, icon, grade, why, on_break, evidence{...}, invalid}
      grade: 3逢跌加 / 2持有可加 / 1持有不加 / 0数据不足 / -1逢反弹减 / -2清
      invalid: **第①层自己的失效条件**（这才是真止损，不是价格线）
    """
    out = {"code": code, "name": name, "stance": "数据不足·退回技术层", "icon": "⚪",
           "grade": 0, "why": "", "on_break": "", "evidence": {}, "invalid": ""}
    try:
        import yfinance as yf
        from cloud_engine import _yf_norm, rev_trend
        tk = yf.Ticker(_yf_norm(code))
        if df is None or len(df) < 60:
            df = tk.history(period="3y")
        c = df["Close"].dropna()
        if len(c) < 60:
            out["why"] = "价格数据不足 60 根，无法判周期"
            return out
        px = float(c.iloc[-1])
        hi = float(c.max())
        lo = float(c.min())
        dd = (px / hi - 1) * 100                    # 距区间高点回撤（负数）
        up = (px / lo - 1) * 100 if lo > 0 else 0   # 距区间低点涨幅
        bars = len(c)
        ma = {}
        for n in (60, 120, 250):
            if bars > n:
                ma[n] = float(c.tail(n).mean())
        if len(ma) == 3:
            if ma[60] > ma[120] > ma[250]:
                struct = "多头排列(长周期向上)"
            elif ma[60] < ma[120] < ma[250]:
                struct = "空头排列(长周期向下)"
            else:
                struct = "均线纠缠(方向未定)"
        else:
            struct = f"数据仅{bars}根，长周期结构不可判"

        f = fund
        if f is None:
            try:
                from cloud_engine import fundamentals
                f = fundamentals(code) or {}
            except Exception:
                logging.exception("cycle_verdict fundamentals failed: %s", code)
                f = {}
        rt = (f or {}).get("rev_trend") or rev_trend(code, tk=tk)
        pe, eg = (f or {}).get("pe"), (f or {}).get("earn_growth")

        v = _stance(rt.get("trend", "数据不足"), dd, pe, eg)
        out.update(v)
        out["evidence"] = {
            "rev_trend": rt.get("trend"), "rev_yoy": rt.get("yoy"),
            "rev_src": rt.get("src"), "rev_line": rt.get("line"),
            "pe": pe, "earn_growth": eg,
            "px": round(px, 2), "dd_high": round(dd, 1), "up_from_low": round(up, 1),
            "bars": bars, "struct": struct,
        }
        # —— 第①层的失效条件 = 真止损。**不是价格线，是生意本身。** ——
        if rt.get("src") == "年报" and rt.get("trend") != "数据不足":
            out["invalid"] = ("年报营收增速**连续两年转负**（下一份年报核对），"
                              "或主营逻辑被证伪（如支柱业务停摆/牌照吊销）"
                              "——**只有这个成立才清仓，价格跌多少都不算**")
            if (eg is not None and eg < -0.20 and (rt.get("yoy") or 0) > 0.10):
                out["invalid"] += ("；另注：本票正处「用利润换规模」阶段，"
                                   "**利润下滑不算失效**，但若营收增速掉到10%以下而利润仍塌，"
                                   "则扩张故事证伪，按失效处理")
        else:
            out["invalid"] = "周期层无数据，无第①层失效条件——退回技术止损，且不宜重仓"
    except Exception:
        logging.exception("cycle_verdict failed: %s", code)
        out["why"] = out["why"] or "周期层计算异常（已留痕）"
    return out


def build(codes: dict) -> dict:
    """批量算周期层。codes = {code: name}。"""
    rows = {}
    for code, name in (codes or {}).items():
        try:
            rows[code] = cycle_verdict(code, name)
        except Exception:
            logging.exception("cycle build failed: %s", code)
    out = {"generated_at": datetime.now(BJT).strftime("%Y-%m-%d %H:%M（北京时间）"),
           "note": "第①层周期层：回答『要不要有仓位』。技术线只调节奏不管去留。",
           "rows": rows,
           "counts": {}}
    for r in rows.values():
        k = r.get("stance", "?")
        out["counts"][k] = out["counts"].get(k, 0) + 1
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    return out


if __name__ == "__main__":
    demo = {"1810.HK": "小米集团-W", "600000.SS": "浦发银行", "600938.SS": "中国海油",
            "0027.HK": "银河娱乐", "9988.HK": "阿里巴巴-SW", "AAPL": "苹果"}
    r = build(demo)
    for c, v in r["rows"].items():
        e = v["evidence"]
        print(f"{v['icon']} {v['name']:<12} {v['stance']:<16} "
              f"营收{e.get('rev_trend')}({(e.get('rev_yoy') or 0) * 100:+.0f}%) "
              f"距高{e.get('dd_high')}% {e.get('struct')}")
        print(f"    {v['why']}")
        print(f"    破位时→ {v['on_break']}")
