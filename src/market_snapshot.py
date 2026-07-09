"""
market_snapshot.py - 大盘走势 + 板块轮动量化快照
用 yfinance 抓取三大市场指数与板块行情，计算趋势与轮动信号，
输出 Markdown 段落（附加到日报末尾）+ data/market_snapshot.json。
全部数字来自真实行情计算，不经过大模型，杜绝编造。
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_JSON = BASE_DIR / "data" / "market_snapshot.json"

logger = logging.getLogger("market_snapshot")

# 大盘指数（创业板指 399006 在雅虎数据残缺，用创业板ETF 159915 代理）
INDICES = {
    "美股": [("^GSPC", "标普500"), ("^IXIC", "纳斯达克"), ("^DJI", "道琼斯")],
    "A股": [("000001.SS", "上证指数"), ("399001.SZ", "深证成指"), ("159915.SZ", "创业板(ETF代理)")],
    "港股": [("^HSI", "恒生指数"), ("3033.HK", "恒生科技(ETF代理)")],
}

# 板块代理（美股用SPDR行业ETF；A股用规模最大的行业ETF；港股可选行业ETF有限，取三类风格）
SECTORS = {
    "美股": [
        ("XLK", "科技"), ("XLC", "通信"), ("XLY", "可选消费"), ("XLP", "必选消费"),
        ("XLF", "金融"), ("XLV", "医疗"), ("XLE", "能源"), ("XLI", "工业"),
        ("XLB", "材料"), ("XLU", "公用事业"), ("XLRE", "房地产"),
    ],
    "A股": [
        ("512760.SS", "半导体芯片"), ("515030.SS", "新能源车"), ("512690.SS", "白酒"),
        ("512010.SS", "医药"), ("512880.SS", "证券"), ("512800.SS", "银行"),
        ("512400.SS", "有色金属"), ("512660.SS", "军工"), ("159928.SZ", "消费"),
        ("515220.SS", "煤炭"),
    ],
    "港股": [
        ("3033.HK", "恒生科技"), ("2828.HK", "国企蓝筹"), ("3110.HK", "高股息"),
    ],
}


def _fetch_metrics(symbol: str, name: str) -> dict | None:
    """抓取单个标的 3 个月日线，计算趋势指标。失败返回 None。"""
    try:
        import time as _t
        import yfinance as yf
        # yfinance 对指数符号(^GSPC等)常瞬时限流返回空("possibly delisted")，
        # 加 3 次重试+退避，大幅降低指数行缺失概率（日报/周报共用此函数）
        close = None
        for _attempt in range(3):
            try:
                df = yf.Ticker(symbol).history(period="3mo")
                close = df["Close"].dropna()
                if len(close) >= 25:
                    break
            except Exception:
                close = None
            _t.sleep(1.2 * (_attempt + 1))
        if close is None or len(close) < 25:
            logger.warning(f"[{name} {symbol}] 3次重试后数据仍不足，跳过")
            return None
        # 数据异常防护：指数/行业ETF 单日波动不可能超过 20%，
        # 出现即为份额折算/拆分未复权等脏数据，整只剔除以免假信号进日报
        daily_ret = close.pct_change().dropna().tail(21)
        if len(daily_ret) and float(daily_ret.abs().max()) > 0.20:
            logger.warning(f"[{name} {symbol}] 检测到异常跳空（疑似份额折算/未复权），剔除")
            return None
        last = float(close.iloc[-1])
        chg1d = (last / float(close.iloc[-2]) - 1) * 100
        chg5d = (last / float(close.iloc[-6]) - 1) * 100
        chg20d = (last / float(close.iloc[-21]) - 1) * 100
        # 量能比：5日均量/20日均量（>1 放量，<1 缩量）
        try:
            _vv = df["Volume"].fillna(0)
            vol_ratio = round(float(_vv.tail(5).mean()) / (float(_vv.tail(20).mean()) or 1.0), 2)
        except Exception:
            vol_ratio = 1.0
        ma20 = float(close.rolling(20).mean().iloc[-1])
        ma60 = float(close.rolling(min(60, len(close))).mean().iloc[-1])
        if last > ma20 > ma60:
            trend = "📈 多头排列"
        elif last < ma20 < ma60:
            trend = "📉 空头排列"
        elif last > ma20:
            trend = "🔄 震荡偏强"
        else:
            trend = "🔄 震荡偏弱"
        # 【V88·拐点识别】放量+破趋势=拐点（指数与个股同引擎；量能缺失自动降级价格判定）
        turning, turning_prompt = "", ""
        try:
            try:
                from cloud_engine import turning_point as _tpf
            except ImportError:
                from src.cloud_engine import turning_point as _tpf
            _t = _tpf(df)
            if _t and _t.get("side"):
                turning = _t["brief"]
                turning_prompt = "；".join(_t["signals"]) + "。👉 " + _t["prompt"]
        except Exception:
            pass
        return {
            "symbol": symbol, "name": name, "last": round(last, 2),
            "chg1d": round(chg1d, 2), "chg5d": round(chg5d, 2), "chg20d": round(chg20d, 2),
            "vs_ma20": round((last / ma20 - 1) * 100, 2),
            "vs_ma60": round((last / ma60 - 1) * 100, 2),
            "trend": trend, "vol_ratio": vol_ratio,
            "turning": turning, "turning_prompt": turning_prompt,
        }
    except Exception as e:
        logger.warning(f"[{name} {symbol}] 抓取失败: {type(e).__name__}: {str(e)[:80]}")
        return None


def _fetch_group(pairs: list) -> list:
    with ThreadPoolExecutor(max_workers=6) as ex:
        results = list(ex.map(lambda p: _fetch_metrics(*p), pairs))
    return [r for r in results if r]


def _rotation_hints(sectors: list) -> list[str]:
    """
    板块轮动信号：对比近5日与近20日涨幅排名。
    5日排名显著好于20日 → 资金轮入；反之 → 资金轮出（涨势退潮）。
    """
    if len(sectors) < 4:
        return []
    n = len(sectors)
    rank5 = {s["symbol"]: i for i, s in enumerate(sorted(sectors, key=lambda x: x["chg5d"], reverse=True))}
    rank20 = {s["symbol"]: i for i, s in enumerate(sorted(sectors, key=lambda x: x["chg20d"], reverse=True))}
    hints = []
    jump = max(2, n // 3)  # 排名跃升/滑落超过约1/3视为轮动
    for s in sectors:
        delta = rank20[s["symbol"]] - rank5[s["symbol"]]  # 正=排名上升
        if delta >= jump and s["chg5d"] > 0:
            hints.append(f"🔥 **{s['name']}** 资金轮入：近5日 {s['chg5d']:+.1f}%，排名从第{rank20[s['symbol']]+1}升至第{rank5[s['symbol']]+1}")
        elif delta <= -jump and s["chg20d"] > 0:
            hints.append(f"🧊 **{s['name']}** 涨势退潮：近20日 {s['chg20d']:+.1f}% 但近5日 {s['chg5d']:+.1f}%，排名从第{rank20[s['symbol']]+1}滑至第{rank5[s['symbol']]+1}")
    return hints


def _render_market(market: str, indices: list, sectors: list) -> str:
    lines = [f"### {market}", ""]
    # 大盘走势
    for ix in indices:
        lines.append(
            f"- **{ix['name']}** {ix['last']}（今日 {ix['chg1d']:+.1f}% / 5日 {ix['chg5d']:+.1f}% / 20日 {ix['chg20d']:+.1f}%）"
            f"｜距MA20 {ix['vs_ma20']:+.1f}%｜{ix['trend']}"
            + (f"｜**{ix['turning']}**" if ix.get("turning") else "")
        )
        # 【V88·拐点识别】指数放量破位/放量突破时，给出证据与判断提示词
        if ix.get("turning_prompt"):
            lines.append(f"  - 🔀 拐点详情：{ix['turning_prompt']}")
    # 板块强弱榜
    if sectors:
        # 【V98】板块热度分 0-100 = 动量50% + 量能30% + 月趋势20%（附在每个板块上）
        for s in sectors:
            _vr = s.get("vol_ratio", 1.0)
            s["heat"] = int(max(0, min(100, 0.5 * (50 + s["chg5d"] * 4)
                                       + 0.3 * (50 + (_vr - 1.0) * 150)
                                       + 0.2 * (50 + s["chg20d"] * 2))))
        top = sorted(sectors, key=lambda x: x["chg5d"], reverse=True)
        strongest = top[:3]
        weakest = top[-3:][::-1] if len(top) > 5 else []
        _hot = sorted(sectors, key=lambda x: -x.get("heat", 0))[:3]
        lines.append("")
        lines.append("**板块热度Top3**：" + "、".join(
            f"{s['name']} {s.get('heat', 0)}°(量比{s.get('vol_ratio', 1.0)})" for s in _hot)
            + "　_热度=动量50%+量能30%+月趋势20%，>70=真主线，50-70=轮动补涨，量比<1的冲高多为短命_")
        lines.append("**板块强弱（近5日）**：")
        lines.append("- 领涨：" + "、".join(f"{s['name']} {s['chg5d']:+.1f}%" for s in strongest))
        if weakest:
            lines.append("- 落后：" + "、".join(f"{s['name']} {s['chg5d']:+.1f}%" for s in weakest))
        hints = _rotation_hints(sectors)
        if hints:
            lines.append("")
            lines.append("**轮动信号**：")
            lines.extend(f"- {h}" for h in hints)
        else:
            lines.append("- 轮动信号：近期板块排名稳定，未见明显轮动")
    lines.append("")
    return "\n".join(lines)


def compute_temperature(indices: list, sectors: list) -> dict:
    """
    【V98】市场温度 0-100 = 趋势35%（指数站上MA20/60）+ 宽度35%（板块站上MA20占比）
    + 动量15%（指数5日涨幅）+ 量能15%（指数放量/缩量，放量上涨才是真热）。全实价计算。
    """
    tr = []
    for ix in indices:
        t = (50 if ix.get("vs_ma20", 0) > 0 else 0) + (50 if ix.get("vs_ma60", 0) > 0 else 0)
        tr.append(t)
    trend = sum(tr) / len(tr) if tr else 50.0
    breadth = (100.0 * sum(1 for s in sectors if s.get("vs_ma20", 0) > 0) / len(sectors)) if sectors else 50.0
    mom_raw = sum(ix.get("chg5d", 0) for ix in indices) / len(indices) if indices else 0.0
    momentum = max(0.0, min(100.0, 50 + mom_raw * 10))
    _vrs = [ix.get("vol_ratio", 1.0) for ix in indices if ix.get("vol_ratio")]
    _vr = sum(_vrs) / len(_vrs) if _vrs else 1.0
    vol_heat = max(0.0, min(100.0, 50 + (_vr - 1.0) * 150))
    temp = int(round(0.35 * trend + 0.35 * breadth + 0.15 * momentum + 0.15 * vol_heat))
    if temp >= 75:
        label, pos = "🔥 过热", "6-7成（过热防回撤，触发中线兑现纪律）"
    elif temp >= 60:
        label, pos = "🟢 偏暖", "65-80%"
    elif temp >= 45:
        label, pos = "🟡 中性", "50-65%"
    elif temp >= 30:
        label, pos = "🟠 偏冷", "30-50%"
    else:
        label, pos = "🔵 冰点", "≤30%（仅留核心层大跌加仓弹药）"
    return {"temp": temp, "trend": round(trend), "breadth": round(breadth),
            "momentum": round(momentum), "vol_heat": round(vol_heat), "label": label, "position": pos}


def generate_market_snapshot() -> str:
    """生成完整的大盘+板块轮动 Markdown 段落，并落盘 JSON。失败返回空串。"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    header = [
        "",
        "---",
        "",
        f"## 📈 大盘走势与板块轮动（量化快照 · {now}）",
        "",
        "> 以下数字由真实行情直接计算（指数/行业ETF，5日=一周动量，20日=一月动量），非AI生成。",
        "",
    ]
    payload = {}
    market_blocks = []
    any_data = False
    for market in ("美股", "A股", "港股"):
        indices = _fetch_group(INDICES[market])
        sectors = _fetch_group(SECTORS[market])
        if not indices and not sectors:
            logger.warning(f"[{market}] 指数与板块数据均获取失败，跳过")
            continue
        any_data = True
        market_blocks.append(_render_market(market, indices, sectors))
        payload[market] = {"indices": indices, "sectors": sectors,
                           "temperature": compute_temperature(indices, sectors)}
    if not any_data:
        return ""

    # 🌡 市场温度计（回答"现在市场能不能做"，置于快照最前）
    # 【V98.1】附引擎池宽度详情（上涨家数比/站上MA20比/60日新高数）
    _breadth_ext = {}
    try:
        _er = json.loads((BASE_DIR / "data" / "engine_rank.json").read_text(encoding="utf-8"))
        _breadth_ext = _er.get("breadth") or {}
    except Exception:
        pass
    temp_lines = ["### 🌡 市场温度计（能不能做 · 做多大仓位）", ""]
    _temps = []
    for market in ("美股", "A股", "港股"):
        t = (payload.get(market) or {}).get("temperature")
        if t:
            _temps.append(t["temp"])
            _bx = _breadth_ext.get(market) or {}
            _bx_txt = (f"｜宽度详情: 上涨{_bx.get('up_pct')}%·站上MA20 {_bx.get('above20_pct')}%·"
                       f"60日新高{_bx.get('newhigh60')}只/{_bx.get('total')}" if _bx else "")
            temp_lines.append(
                f"- **{market} {t['temp']}/100** {t['label']}（趋势{t['trend']}/宽度{t['breadth']}/动量{t['momentum']}/量能{t.get('vol_heat','—')}）"
                f"→ 建议仓位 **{t['position']}**{_bx_txt}")
    if _temps:
        _avg = int(round(sum(_temps) / len(_temps)))
        temp_lines.append(f"- **三市场综合 {_avg}/100** ｜ 温度=趋势35%+宽度35%+动量15%+量能15%，全部实价计算")
    temp_lines.append("")
    blocks = header + temp_lines + market_blocks
    try:
        OUTPUT_JSON.write_text(
            json.dumps({"generated_at": now, "markets": payload}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        logger.warning(f"快照JSON写入失败: {e}")
    return "\n".join(blocks)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    md = generate_market_snapshot()
    print(md if md else "（无数据）")
