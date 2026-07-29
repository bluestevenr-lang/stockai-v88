"""
market_snapshot.py - 大盘走势 + 板块轮动量化快照
用 yfinance 抓取三大市场指数与板块行情，计算趋势与轮动信号，
输出 Markdown 段落（附加到日报末尾）+ data/market_snapshot.json。
全部数字来自真实行情计算，不经过大模型，杜绝编造。
"""

import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_JSON = BASE_DIR / "data" / "market_snapshot.json"

logger = logging.getLogger("market_snapshot")

# 大盘指数：创业板优先取真实指数 399006；仅在取数失败时才回退 ETF 159915。
INDICES = {
    "美股": [("^GSPC", "标普500"), ("^IXIC", "纳斯达克"), ("^DJI", "道琼斯")],
    "A股": [("000001.SS", "上证指数"), ("399001.SZ", "深证成指"), ("399006.SZ", "创业板指")],
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


def _em_close_cn(symbol: str):
    """【V88·A股板块东财兜底 2026-07-20 用户抓"看不到军工/银行/芯片板块"】
    根因=yfinance 拉 A股 ETF 极不稳定,半导体/军工/有色常抓失败被跳过→板块源缺数据。
    改用东财日线直连(trust_env=False,与其它国内源一致)。symbol如512760.SS/159928.SZ。
    返回收盘价 pandas.Series 或 None。"""
    import re
    m = re.match(r"(\d{6})\.(SS|SZ)", str(symbol))
    if not m:
        return None
    code, mkt = m.group(1), m.group(2)
    secid = f"{'1' if mkt == 'SS' else '0'}.{code}"
    try:
        import requests
        import pandas as pd
        s = requests.Session()
        s.trust_env = False   # 国内接口直连,不走代理(记忆铁律)
        r = s.get("https://push2his.eastmoney.com/api/qt/stock/kline/get",
                  params={"secid": secid, "fields1": "f1", "fields2": "f51,f53",
                          "klt": "101", "fqt": "1", "end": "20500101", "lmt": "70"},
                  timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        kl = ((r.json() or {}).get("data") or {}).get("klines") or []
        closes = [float(x.split(",")[1]) for x in kl if "," in x]
        return pd.Series(closes) if len(closes) >= 25 else None
    except Exception:
        return None


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
        # A股 ETF yfinance 失败 → 东财直连兜底(半导体/军工/有色/煤炭就是这样被救回)
        if (close is None or len(close) < 25) and str(symbol).endswith((".SS", ".SZ")):
            _em = _em_close_cn(symbol)
            if _em is not None and len(_em) >= 25:
                close = _em
                logger.info(f"[{name} {symbol}] yfinance失败,东财兜底成功({len(close)}根)")
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
        # 【V88·顶底研判层 2026-07-29】价格/成交量分位——量价背离的原料。
        # 旧版只有 vol_ratio(5日/20日均量)，判不出"价在高位而量在低位"这种背离，
        # 港股 trend100/breadth100/vol_heat15 就是被漏掉的教科书级顶背离。
        _pv = {"px_pct": None, "vol_pct": None, "diverge": None}
        try:
            try:
                from regime_topbot import price_volume_percentiles as _pvp
            except ImportError:
                from src.regime_topbot import price_volume_percentiles as _pvp
            # 只有当 volume 与 close 同源同长时才用它——东财兜底路径只返回 close，
            # 此时若误用 yfinance 残留的 df["Volume"]，量分位会建立在另一份数据上。
            _vol_series = None
            try:
                _vv2 = df["Volume"].fillna(0)
                if len(_vv2) == len(close):
                    _vol_series = _vv2
            except Exception:
                _vol_series = None
            _pv = _pvp(close, _vol_series)
        except Exception:
            logger.exception(f"[{name} {symbol}] 价量分位计算失败（不影响其余字段）")
        return {
            "symbol": symbol, "name": name, "last": round(last, 2),
            "chg1d": round(chg1d, 2), "chg5d": round(chg5d, 2), "chg20d": round(chg20d, 2),
            "vs_ma20": round((last / ma20 - 1) * 100, 2),
            "vs_ma60": round((last / ma60 - 1) * 100, 2),
            "trend": trend, "vol_ratio": vol_ratio,
            "px_pct": _pv.get("px_pct"), "vol_pct": _pv.get("vol_pct"),
            "diverge": _pv.get("diverge"),
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
    # 【V88·自动研判】把温度+今日/月涨跌+量能 说成人话结论：
    # 关键区分"见顶派发(高温+高位+滞涨→警惕)" vs "情绪冰点(低温+杀跌→接近弹药、属正常调整)"。
    c1 = sum(ix.get("chg1d", 0) for ix in indices) / len(indices) if indices else 0.0
    _down = c1 <= -1.2          # 今日均跌超1.2%＝明显下杀
    _up = c1 >= 1.2
    if temp >= 75:
        verdict = ("⚠️ 高位过热放量下杀·防趋势反转，触发中线兑现纪律" if _down
                   else "⚠️ 高位过热·放量滞涨防派发，落袋为先" if (vol_heat >= 60 and abs(mom_raw) < 1.5)
                   else "🔥 过热惯性·可持有但设好回撤线、不新追高")
    elif temp < 30:
        verdict = ("🧊 情绪冰点·恐慌杀跌接近弹药区（属正常调整非见顶，核心层分批备弹、不追杀）" if (_down or vol_heat >= 70)
                   else "🧊 情绪冰点·地量磨底（等企稳信号再动，不逆势加主题）")
    elif temp < 45:
        verdict = ("🟠 转弱调整·控节奏（未到弹药区，减主题不加杠杆）" if _down
                   else "🟠 偏冷震荡·轻仓等右侧信号")
    else:
        verdict = ("🟡 获利回吐/正常回调（趋势未破，回踩看支撑再定）" if _down
                   else "🟢 趋势偏暖·持有为主（回踩加、不追高）" if (_up and temp >= 60)
                   else "🟡 中性震荡·跟随主线轮动")
    return {"temp": temp, "trend": round(trend), "breadth": round(breadth),
            "momentum": round(momentum), "vol_heat": round(vol_heat),
            "label": label, "position": pos, "verdict": verdict}




def compute_turn_risk(indices, temp):
    """【B2·市场转向概率】指数拐点+温度+量能合成（规则式，可复算）。
    顶部转向风险：指数出现顶拐/高温/放量滞涨/急跌；底部转机：底拐/冰点/放量回升。"""
    try:
        if not indices:
            return None
        n = len(indices)
        top = sum(1 for ix in indices if str(ix.get("turning", "")).startswith("⚠️"))
        bot = sum(1 for ix in indices if str(ix.get("turning", "")).startswith("🔄"))
        t = float((temp or {}).get("temp", 50) or 50)
        vr = sum(float(ix.get("vol_ratio", 1) or 1) for ix in indices) / n
        chg5 = sum(float(ix.get("chg5d", 0) or 0) for ix in indices) / n
        risk = (35 * min(1.0, top / max(1, n * 0.5))
                + (25 if t >= 70 else (10 if t >= 60 else 0))
                + (15 if (vr >= 1.4 and chg5 < 1) else 0)
                + (15 if chg5 < -2 else 0))
        opp = (35 * min(1.0, bot / max(1, n * 0.5))
               + (25 if t <= 30 else (10 if t <= 40 else 0))
               + (15 if (vr >= 1.3 and chg5 > 0) else 0))
        _lv = lambda x: "高" if x >= 55 else ("中" if x >= 30 else "低")
        return {"top_risk": int(risk), "bottom_opp": int(opp),
                "text": f"顶部转向风险 {_lv(risk)}({int(risk)}/100) ｜ 底部转机信号 {_lv(opp)}({int(opp)}/100)"}
    except Exception:
        return None


def generate_market_snapshot() -> str:
    """生成完整的大盘+板块轮动 Markdown 段落，并落盘 JSON。失败返回空串。"""
    now = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M")
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
        if market == "A股" and not any(x.get("symbol") == "399006.SZ" for x in indices):
            _cyb_etf = _fetch_metrics("159915.SZ", "创业板ETF代理")
            if _cyb_etf:
                _cyb_etf["unit"] = "元"
                indices.append(_cyb_etf)
                logger.warning("[创业板指] 399006 取数失败，已回退到 159915 ETF 代理")
        sectors = _fetch_group(SECTORS[market])
        if not indices and not sectors:
            logger.warning(f"[{market}] 指数与板块数据均获取失败，跳过")
            continue
        any_data = True
        market_blocks.append(_render_market(market, indices, sectors))
        _temp = compute_temperature(indices, sectors)
        payload[market] = {"indices": indices, "sectors": sectors,
                           "temperature": _temp,
                           "turn_risk": compute_turn_risk(indices, _temp)}
    if not any_data:
        return ""

    # 【V88·顶底研判层 2026-07-29 用户定纲"在地顶的时候,对于大盘的判断很重要"】
    # 覆盖 turn_risk 为 v2 版：旧版把35分押在常年为空的 turning 字段、要求"放量"滞涨
    # (顶部实为缩量新高,方向相反)、且把放量急跌算进顶部风险(那是底部特征)。
    _regime = {}
    try:
        try:
            from regime_topbot import build as _regime_build
        except ImportError:
            from src.regime_topbot import build as _regime_build
        _regime = _regime_build(payload)     # 原地给 payload 补 topbot + 覆盖 turn_risk
        (BASE_DIR / "data" / "regime_topbot.json").write_text(
            json.dumps(_regime, ensure_ascii=False, indent=1), encoding="utf-8")
        logger.info("[顶底研判层] 已生成 regime_topbot.json")
    except Exception:
        logger.exception("[顶底研判层] 生成失败——turn_risk 保持旧版，不影响其余快照")

    # 【V88·三层周期概率总览】大盘层：主指数 2/4/8/16/32 周方向分
    # （与桌面首屏/自选决策台同一套 v88_decision_core 口径，供云端/飞书消费）
    _L3_IDX = {"美股": ("标普500", "^GSPC"), "A股": ("上证指数", "000001.SS"),
               "港股": ("恒生指数", "^HSI")}
    for market, (_nm, _sym) in _L3_IDX.items():
        if market not in payload:
            continue
        try:
            import yfinance as yf
            from v88_decision_core import evaluate_decision
            from cloud_engine import analyze_trend_full
            _df = yf.Ticker(_sym).history(period="1y")
            if _df is None or len(_df) < 40:
                continue
            try:
                _full = analyze_trend_full(_df) or {}
            except Exception:
                _full = {}
            _dc = evaluate_decision(_df, _full, name=_nm, code=_sym)
            if _dc.get("error"):
                continue
            _hz = ((_dc.get("facts") or {}).get("horizons") or {})
            _probs = [[_lab, int(round(float((_hz.get(_lab) or {}).get("rule_score"))))]
                      for _lab in ("2周", "4周", "8周", "16周", "32周")
                      if (_hz.get(_lab) or {}).get("rule_score") is not None]
            payload[market]["l3"] = {"name": _nm, "stage": str(_full.get("stage") or "—"),
                                     "action": str(_dc.get("action") or "观察"), "probs": _probs}
        except Exception as _l3e:
            logger.debug(f"[{market}] 三层大盘层计算失败: {_l3e}")

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
            if t.get("verdict"):
                temp_lines.append(f"  - 🧭 研判：**{t['verdict']}**")
            _tr = (payload.get(market) or {}).get("turn_risk")
            if _tr:
                temp_lines.append(f"  - 🔮 {market}转向概率：{_tr['text']}")
            # 【V88·顶底研判层】逐条列命中特征——不给顶底概率(样本<5·铁律2),
            # 只给"命中N/7"与逐条依据,⬜=数据源未接(不算未命中)。用户能自己数、能推翻。
            _cl = (payload.get(market) or {}).get("topbot")
            if _cl:
                _side = "顶" if _cl.get("top_hit", 0) >= _cl.get("bottom_hit", 0) else "底"
                _items = (_cl.get("top") if _side == "顶" else _cl.get("bottom")) or []
                _hit = _cl.get("top_hit" if _side == "顶" else "bottom_hit", 0)
                _na = _cl.get("top_na" if _side == "顶" else "bottom_na", 0)
                _dv = _cl.get("diverge")
                temp_lines.append(
                    f"  - 🌡️ {market}**{_side}部特征 {_hit}/7**"
                    + (f"（{_na}条数据源未接）" if _na else "")
                    + (f"｜价格分位{_cl.get('px_pct')}·量分位{_cl.get('vol_pct')}·背离{_dv:+.1f}"
                       if _dv is not None else ""))
                _on = "；".join(f"{i['key']}({i['text']})" for i in _items if i.get("hit") is True)
                _off = "、".join(i["key"] for i in _items if i.get("hit") is False)
                if _on:
                    temp_lines.append(f"    - ✅ 已命中：{_on}")
                if _off:
                    temp_lines.append(f"    - ▫️ 未命中：{_off}")
            _l3 = (payload.get(market) or {}).get("l3")
            if _l3 and _l3.get("probs"):
                # 【逐点方向符号+现在锚点 2026-07-19】链首「现在」=阶段基准实算,2周箭头相对现在
                _sb = str(_l3.get("stage") or "")
                _base = (45 if any(k in _sb for k in ("蓄势", "底部")) else
                         62 if any(k in _sb for k in ("领涨", "主升", "启动", "延续", "多头")) else
                         55 if any(k in _sb for k in ("派发", "滞涨", "高位")) else
                         38 if any(k in _sb for k in ("退潮", "破位", "转弱", "下跌")) else 50)
                _ch_parts, _pv = [f"现在{_base}"], _base
                for lab, p in _l3["probs"]:
                    _ar = ""
                    if _pv is not None:
                        _ar = "↑" if p - _pv >= 1 else ("↓" if p - _pv <= -1 else "≈")
                    _ch_parts.append(f"{lab}{p}%{_ar}")
                    _pv = p
                _chain = " ".join(_ch_parts)
                temp_lines.append(f"  - 📈 {_l3['name']} {_l3['stage']}·{_l3['action']}｜各周期上行概率：{_chain}（规则情景估计）")
    if _temps:
        _avg = int(round(sum(_temps) / len(_temps)))
        temp_lines.append(f"- **三市场综合 {_avg}/100** ｜ 温度=趋势35%+宽度35%+动量15%+量能15%，全部实价计算")
    # 【V88·三市场相位差】三个市场温度的分化程度本身就是信号：
    # 2026-07-29 实测 港股87 vs A股39 极差48点＝极端分化，
    # 这种时候"大盘"不是一个东西，必须分开说，不能用一句"大盘高位"概括。
    try:
        _ps = (_regime or {}).get("phase_spread") or {}
        if _ps:
            temp_lines.append(f"- 🌐 **三市场相位**：{_ps.get('text')}"
                              "（分化越大＝越不能用一句『大盘如何』概括，需逐市场定策略）")
    except Exception:
        logger.exception("[顶底研判层] 相位差渲染失败")
    temp_lines.append("")
    _rotation_forecast = {}
    _rotation_md = ""
    _cycle_scan = {}
    try:
        _snapshot_core = {"generated_at": now, "markets": payload}
        _canonical = json.dumps(
            _snapshot_core, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        _snapshot_id = "snap-" + hashlib.sha256(_canonical.encode("utf-8")).hexdigest()[:16]
        # 【V88·资金流雷达 2026-07-25 用户批准】主力净流入随行情链一体刷新(时点一致性):
        # 轮动打分要吃它的板块streak,必须先抓;失败不阻断(轮动降级为无资金确认)。
        try:
            from fund_flow_radar import build as _ff_build
            _ff_build()
        except Exception as _ff_exc:
            logger.warning(f"资金流雷达失败,轮动无资金确认: {_ff_exc}")
        # 【V88·下一轮轮转】同一冻结快照先量化筛选，再仅调用一次 thinking-high 联合复核三市场。
        try:
            from rotation_forecast import build_rotation_forecast, render_markdown
            _rotation_forecast = build_rotation_forecast(payload, _snapshot_id, now)
            _rotation_md = render_markdown(_rotation_forecast)
        except Exception as _rf_exc:
            logger.warning(f"轮转预测失败，保留基础快照: {_rf_exc}")
            _rotation_md = ""
        # 【V88·个股周期切换】持仓+自选周期扫描，同 09:00/21:00 节奏（时段闸门内才重算）。
        _cycle_scan = {}
        try:
            from cycle_scan import build_cycle_scan
            _cycle_scan = build_cycle_scan(payload)
        except Exception as _cs_exc:
            logger.warning(f"个股周期扫描失败，保留基础快照: {_cs_exc}")
        OUTPUT_JSON.write_text(
            json.dumps({
                "schema_version": "v88.snapshot/2.0",
                "snapshot_id": _snapshot_id,
                "rotation_forecast": _rotation_forecast,
                "cycle_scan": _cycle_scan,
                **_snapshot_core,
            }, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        logger.warning(f"快照JSON写入失败: {e}")
    # 思维导图紧跟三市场明细，和“大盘走势与板块轮动”保持为同一内容，不另起孤立模块。
    _cycle_md = ""
    try:
        from cycle_scan import render_markdown as _render_cycle_md
        _cycle_md = _render_cycle_md(_cycle_scan)
    except Exception:
        _cycle_md = ""
    blocks = (header + temp_lines + market_blocks
              + ([_rotation_md] if _rotation_md else [])
              + ([_cycle_md] if _cycle_md else []))
    return "\n".join(blocks)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    md = generate_market_snapshot()
    print(md if md else "（无数据）")
