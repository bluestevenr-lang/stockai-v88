"""V88 个股唯一决策底稿（网页、搜索、持仓、预警、深度分析共用）。

这个模块只做可复算的行情/赔率计算。AI、新闻和基本面可以补充解释或触发风险
复核，但不能在不同页面各自重算一套“综合分”。
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd


SCHEMA = "v88.stock-decision/2.0"
SCORE_VERSION = "V88-U2.0"
HORIZONS = (2, 4, 6, 8, 16)
# 个股「交易日」阶梯：5≈1周 / 10≈2周 / 20≈1月 / 60≈1季 / 120≈半年。
# 「当下前瞻」与「锚点复盘」共用同一条，短中长一屏看全（用户 2026-07-16 拍板，改此一行即可调档）。
HORIZON_DAYS = (5, 10, 20, 60, 120)
HORIZON_DAY_WEIGHTS = {5: 0.15, 10: 0.20, 20: 0.25, 60: 0.25, 120: 0.15}
BJT = timezone(timedelta(hours=8))
SCORE_WEIGHTS = {
    "short": 0.20,       # 2周
    "medium": 0.25,      # 4/6/8周均值
    "long": 0.20,        # 16周
    "trend_quality": 0.15,
    "entry_odds": 0.20,
}


def _clip(value, low, high):
    try:
        return max(low, min(high, float(value)))
    except (TypeError, ValueError):
        return float(low)


def _num(value, default=0.0):
    try:
        value = float(value)
        return value if math.isfinite(value) else float(default)
    except (TypeError, ValueError):
        return float(default)


def _series(df, name):
    if df is None or name not in df:
        return pd.Series(dtype=float)
    raw = df[name]
    if isinstance(raw, pd.DataFrame):
        raw = raw.iloc[:, 0]
    return pd.to_numeric(raw, errors="coerce").dropna()


def build_horizon_facts(df, full=None, horizons=HORIZONS, unit="week") -> dict:
    """生成唯一的多周期行情底稿；分数是方向先验，不是胜率。

    unit='week'：档位按周（2/4/6/8/16 周，系统统一评分主口径，全局不变）。
    unit='day' ：档位按交易日（个人决策锚点用 2/5/8/16 交易日）。
    """
    close = _series(df, "Close")
    high = _series(df, "High")
    low = _series(df, "Low")
    volume = _series(df, "Volume")
    if len(close) < 12:
        return {"error": "有效行情不足12个交易日", "horizons": {}}

    last = float(close.iloc[-1])
    stage = str((full or {}).get("stage") or "")
    stage_bias = 6 if any(x in stage for x in ("主升", "启动", "多头", "强势")) else (
        -6 if any(x in stage for x in ("破位", "退潮", "下跌", "转弱")) else 0)
    _by_day = str(unit) == "day"
    _suffix = "日" if _by_day else "周"
    _min_sample = 2 if _by_day else 5
    out = {}
    for period in tuple(horizons or HORIZONS):
        weeks = period                         # 兼容旧字段名；按日口径时其值即交易日数
        target_days = period if _by_day else period * 5
        n = min(target_days, len(close) - 1)
        if n < _min_sample:
            continue
        window = close.iloc[-(n + 1):]
        start = float(window.iloc[0])
        ret = (last / start - 1) * 100 if start else 0.0
        logv = np.log(window.clip(lower=max(last * 1e-6, 1e-9)).to_numpy())
        slope = float(np.polyfit(np.arange(len(logv)), logv, 1)[0]) if len(logv) >= 3 else 0.0
        slope_move = (math.exp(slope * n) - 1) * 100
        ma = float(window.mean())
        ma_bias = (last / ma - 1) * 100 if ma else 0.0
        five_start = float(close.iloc[-min(6, len(close))])
        ret5 = (last / five_start - 1) * 100 if five_start else 0.0
        vw = volume.iloc[-min(n, len(volume)):] if len(volume) else pd.Series(dtype=float)
        vol_ratio = 1.0
        if len(vw) >= 8 and _num(vw.iloc[:-5].mean()) > 0:
            vol_ratio = float(vw.iloc[-5:].mean() / vw.iloc[:-5].mean())
        recent_high = float(high.iloc[-min(n, len(high)):].max()) if len(high) else float(window.max())
        recent_low = float(low.iloc[-min(n, len(low)):].min()) if len(low) else float(window.min())
        drawdown = (last / recent_high - 1) * 100 if recent_high else 0.0
        volume_push = (4.0 if ret5 >= 0 else -4.0) if vol_ratio >= 1.15 else (
            (-1.5 if ret5 >= 0 else 1.5) if vol_ratio <= 0.75 else 0.0)
        score = round(_clip(
            50 + _clip(ret, -20, 20) * 0.75
            + _clip(slope_move, -15, 15) * 0.65
            + _clip(ma_bias, -10, 10) * 0.65
            + volume_push + stage_bias, 15, 85))
        # 【V88·相位一致性】（2026-07-16 用户抓矛盾：礼来周期导图"派发→退潮"，
        # 16周分却77"越远越强"）。远期分公式本质是后视镜动量——过去涨得多分就高；
        # 当下相位不支持时必须回归中性带，全端只许一套嘴：
        # 派发/退潮相位 → 远期(≥8周或≥60日)分封顶55；低位启动相位 → 远期分下限45。
        phase_note = ""
        if full:
            _stg = str(full.get("stage") or "")
            _p52 = _num(full.get("pos52"), 50)
            _hard_down = _stg in ("破位下跌", "趋势转弱")   # 已在下跌趋势→中期档就该收敛、不显看涨红
            _distrib = _stg in ("高位震荡", "放量滞涨")       # 高位派发未破位→仅远档收敛
            _mid = (period >= 20) if _by_day else (period >= 4)
            _far = (period >= 60) if _by_day else (period >= 8)
            if _hard_down and _mid and score > 52:
                score, phase_note = 52, f"{_stg}·中期分收敛至中性（下跌趋势不显看涨）"
            elif _distrib and _far and score > 55:
                score, phase_note = 55, f"派发相位（{_stg}）·远期动量分封顶55"
            elif _stg in ("底部启动", "启动确认") and _p52 <= 50 and _far and score < 45:
                score, phase_note = 45, f"启动相位（{_stg}）·远期分回归中性"
        out[f"{period}{_suffix}"] = {
            "weeks": weeks, "periods": period, "unit": unit, "sample_days": n,
            "return_pct": round(ret, 1), "slope_pct": round(slope_move, 1),
            "ma_bias_pct": round(ma_bias, 1), "ret5_pct": round(ret5, 1),
            "volume_ratio": round(vol_ratio, 2), "drawdown_pct": round(drawdown, 1),
            "support": round(recent_low, 3), "resistance": round(recent_high, 3),
            "rule_score": score,
            "rule_view": "偏涨" if score >= 59 else ("偏跌" if score <= 41 else "震荡"),
            "phase_note": phase_note,
            "rule_confidence": round(_clip(50 + abs(score - 50) * 1.2, 50, 88)),
        }
    raw_sig = {
        "asof": str(close.index[-1]), "last": round(last, 6),
        "tail": [round(float(x), 6) for x in close.tail(8)],
        "volume": [round(float(x), 2) for x in volume.tail(5)],
    }
    atr14 = 0.0
    if len(high) and len(low):
        frame = pd.concat([high.rename("h"), low.rename("l"), close.rename("c")], axis=1).dropna()
        if len(frame) >= 2:
            prev = frame["c"].shift(1)
            tr = pd.concat([(frame["h"] - frame["l"]),
                            (frame["h"] - prev).abs(),
                            (frame["l"] - prev).abs()], axis=1).max(axis=1)
            atr14 = _num(tr.tail(14).mean())
    return {
        "schema": SCHEMA, "asof": str(close.index[-1])[:19],
        "last": round(last, 4), "atr14": round(atr14, 4),
        "stage": stage or "阶段待核",
        "data_signature": hashlib.sha256(
            json.dumps(raw_sig, sort_keys=True).encode("utf-8")).hexdigest()[:12],
        "horizons": out,
    }


def _mean_scores(horizons, labels, default=50.0):
    values = []
    for label in labels:
        value = (horizons.get(label) or {}).get("rule_score")
        try:
            value = float(value)
            if math.isfinite(value):
                values.append(value)
        except (TypeError, ValueError):
            pass
    return sum(values) / len(values) if values else float(default)


def _scenario_prices(full, facts):
    last = _num(facts.get("last") or (full or {}).get("last"))
    h2 = (facts.get("horizons") or {}).get("2周") or {}
    atr = _num((full or {}).get("atr") or facts.get("atr14"))
    resistance = max(_num((full or {}).get("resistance")), _num(h2.get("resistance")))
    stop_candidates = [x for x in (
        _num((full or {}).get("tech_stop") or (full or {}).get("stop")),
        _num(h2.get("support"))) if 0 < x < last]
    stop = max(stop_candidates) if stop_candidates else 0.0
    if last <= 0:
        return 0.0, 0.0, 0.0, 0.0, 0.0
    upside = (resistance / last - 1) * 100 if resistance > last else 0.5
    stop_risk = (1 - stop / last) * 100 if stop else 0.0
    # 技术支撑可能贴着现价，不能据此制造十几倍、几十倍的虚假盈亏比。
    # 至少预留1.5倍ATR（无ATR时2%）作为正常噪声缓冲。
    volatility_floor = atr / last * 150 if atr else 2.0
    downside = max(stop_risk, volatility_floor, 1.5)
    return last, resistance, stop, _clip(upside, 0.5, 40), _clip(downside, 0.5, 30)


def _price_f(x, d=0.0):
    try:
        return float(str(x).replace(",", "").strip())
    except (TypeError, ValueError):
        return d


def entry_timing(full, *, short=50.0, medium=50.0, long_avg=50.0, action="", rr=1.5) -> dict:
    """【V88·入场时机确认】（2026-07-16 用户定纲：短线以交易日数确认，中长线以周数+区间价格确认）

    治的病：系统入场建议永远锚在比现价低的回踩位，从不说"现在可进"，
    导致实际进场时机失准（腾讯案例）。三条铁规：
    ① 现价已在买入区且结构良性 → 明说「现在可进」+ 有效交易日窗 + 作废价；
    ② 等回踩必须给【双路径】：回踩接 或 N个交易日内放量突破改追——两条都是进场路，不再干等；
    ③ 中线=4-8周区间价分批（破MA55失效），长线=16周+区间（破年线才退）。
    纯确定性，只消费 analyze_trend_full 的既有价位，不新造价格。
    """
    full = full or {}
    last = _price_f(full.get("last"))
    if last <= 0:
        return {}
    _bz = str(full.get("buy_zone") or "").split("~")
    buy_lo, buy_hi = _price_f(_bz[0]), _price_f(_bz[-1])
    pullback = _price_f(full.get("pullback"))
    breakout = _price_f(full.get("breakout"))
    stop = _price_f(full.get("stop"))
    stage = str(full.get("stage") or "")
    vp = str(full.get("vp") or "")
    macd = str(full.get("macd_txt") or "")
    ma_txt = str(full.get("ma_txt") or "")
    pos52 = _price_f(full.get("pos52"), 50)
    bias20 = _price_f(full.get("bias20"))
    chg5 = _price_f(full.get("chg5"))
    volr = _price_f(full.get("volr"), 1.0)
    ma = full.get("ma") or {}
    ma20 = _price_f(ma.get(20) or ma.get("20"))
    ma55 = _price_f(ma.get(55) or ma.get("55"))
    ma120 = _price_f(ma.get(120) or ma.get("120"))

    def _bday(n):
        try:
            d = pd.Timestamp(datetime.now(BJT).date()) + pd.tseries.offsets.BDay(n)
            return d.strftime("%m-%d")
        except Exception:
            return f"+{n}交易日"

    benign = stage in ("底部试探", "底部启动", "启动确认", "趋势延续", "主升")
    toxic = (stage in ("破位下跌", "趋势转弱", "放量滞涨")
             or "顶背离" in macd or float(short) <= 42)
    action = str(action or "")
    # 【短线绿灯只看短线的理】2周方向分≥58 + 非防守/冲突动作即可亮灯（2026-07-16 用户定纲：
    # 短线以交易日确认，不再拿16周共振卡1-5日的进场——那是"用长线标准审短线"的错位）。
    # 长线未共振时绿灯降格为"仅短线仓"，止损纪律更硬。
    _blocked = any(k in action for k in ("回避", "保护", "止跌", "仅观察"))
    action_go = float(short) >= 58 and not _blocked and float(rr) >= 1.2
    _short_only = float(long_avg) < 55
    _tag = "（长线未共振·仅短线仓，破线立走）" if _short_only else ""

    if toxic:
        mode, days = "不进", 0
        s_text = f"🚫 短线不进（{stage or '趋势恶化'}·2周方向分{short:.0f}）；放量收复MA20({ma20:g})再评估"
        note = "🚫短线不进"
    elif action_go and buy_lo > 0 and buy_lo <= last <= buy_hi * 1.005 and benign and chg5 < 8 and bias20 <= 6:
        mode, days = "现价可进", 3
        s_text = (f"✅ 现在可进：现价{last:g}已在买入区{buy_lo:g}~{buy_hi:g}内——"
                  f"今日~{_bday(3)}（3个交易日）分批有效；跌破{stop:g}作废{_tag}")
        note = f"✅现价可进·{_bday(3)}前·破{stop:g}废" + ("·仅短线" if _short_only else "")
    elif action_go and pullback > 0 and abs(last - pullback) / pullback <= 0.015 and benign:
        mode, days = "回踩到位", 2
        s_text = (f"✅ 回踩到位：现价{last:g}贴住回踩位{pullback:g}——"
                  f"今日~{_bday(2)}（2个交易日）企稳即接；跌破{stop:g}止损{_tag}")
        note = f"✅回踩到位·{_bday(2)}前·止损{stop:g}" + ("·仅短线" if _short_only else "")
    elif action_go and breakout > 0 and last >= breakout * 0.995 and volr >= 1.15 and "放量" in vp and chg5 < 10:
        mode, days = "突破确认", 2
        s_text = (f"✅ 突破确认：放量站上{breakout:g}——今日~{_bday(2)}（2个交易日）内跟进；"
                  f"收盘跌回{breakout:g}下方作废{_tag}")
        note = f"✅突破确认·{_bday(2)}前有效" + ("·仅短线" if _short_only else "")
    elif any(k in action for k in ("仅观察", "止跌", "回避", "保护")):
        mode, days = "等待", 0
        s_text = (f"⏳ 周期未共振（{action}）：先看回踩{pullback:g}是否企稳、"
                  f"突破{breakout:g}是否放量——触发后重核共振再进，不直接下单")
        note = f"⏳共振不足·先盯{pullback:g}/{breakout:g}"
    elif benign or float(short) >= 55:
        mode, days = "双路径待触发", 5
        s_text = (f"⏳ 双路径（{_bday(5)}前任一触发即进）：①回踩{pullback:g}企稳接；"
                  f"②不回踩而放量站上{breakout:g}→改按突破进，不再等更低价")
        note = f"⏳双路径:回踩{pullback:g}/突破{breakout:g}·{_bday(5)}前"
    else:
        mode, days = "等待", 0
        s_text = f"⏳ 未到进场条件：回踩{pullback:g}或放量突破{breakout:g}，二者都没有就不动"
        note = f"⏳等回踩{pullback:g}或突破{breakout:g}"

    if float(medium) >= 55 and (ma55 > 0 and last > ma55 or "多头排列" in ma_txt):
        _m_lo = round(min(pullback if pullback > 0 else ma20, ma20 * 0.99) or last * 0.97, 2)
        _m_hi = round(max(buy_hi, ma20 * 1.03) or last, 2)
        m_text = f"🎯 中线（4-8周）：区间{_m_lo:g}~{_m_hi:g}分批建仓；收盘连续2日破MA55({ma55:g})失效"
    elif float(medium) >= 48:
        m_text = f"⏳ 中线（4-8周）：待周线站稳MA55({ma55:g})再启动区间建仓"
    else:
        m_text = "🚫 中线结构未修复，不建仓"

    if float(long_avg) >= 55 and pos52 <= 70 and (ma120 <= 0 or last > ma120):
        _l_lo = round((ma55 * 0.95) if ma55 > 0 else last * 0.9, 2)
        _l_hi = round(buy_hi or last, 2)
        l_text = f"🏛 长线（16周+）：区间{_l_lo:g}~{_l_hi:g}分批；跌破年线({ma120:g})且20日收不回才退出"
    elif float(long_avg) >= 50:
        l_text = "🏛 长线（16周+）：方向分未达55，先跟踪不建仓"
    else:
        l_text = "🚫 长线不参与"

    return {"mode": mode, "days_valid": days, "note": note,
            "short_text": s_text, "mid_text": m_text, "long_text": l_text,
            "zone": [buy_lo, buy_hi], "pullback": pullback,
            "breakout": breakout, "stop": stop}


def diagnose_today(*, scope="自选", today_chg=0.0, market_chg=0.0, stage="",
                   broke_stop=False, pos52=50, action="", entry_mode="",
                   pnl_pct=None, name="") -> dict:
    """【V88·今日逐只解读 2026-07-17 用户点单】说清"今天这只能不能动、为什么"——
    区分破位该躲 vs 错杀可低吸，杜绝"永远等回踩"的万能废话。返回 {verdict动作, why人话, kind}。

    scope: 持仓/自选/常搜；today_chg 今日涨跌%；market_chg 对应大盘今日涨跌%；
    stage: analyze_trend_full 阶段；broke_stop: 是否破止损；pos52: 52周分位；pnl_pct: 持仓浮盈亏%。
    """
    _hold = scope == "持仓"
    _rel = today_chg - market_chg          # 相对大盘强弱（>0抗跌/领涨，<0更弱）
    _pos = float(pos52 or 50)
    _down_stage = stage in ("破位下跌", "趋势转弱", "放量滞涨")
    _up_stage = stage in ("底部启动", "启动确认", "趋势延续", "主升阶段")

    # ① 破位型：技术已坏——不是等回踩，是逻辑破了
    if broke_stop or _down_stage:
        if _hold:
            return {"kind": "破位", "verdict": "减仓/离场",
                    "why": f"已{'破止损' if broke_stop else stage}，趋势坏了——这不是回踩是逻辑破了，"
                           f"按纪律减/走，别扛" + (f"（浮亏{pnl_pct:+.0f}%别越亏越拿）" if (pnl_pct or 0) < -3 else "")}
        return {"kind": "破位", "verdict": "回避·不接刀",
                "why": f"今日{today_chg:+.1f}%且已{'破止损位' if broke_stop else stage}——"
                       f"现在买就是接下落的刀，等重新放量站上MA20再看，不是等回踩那种低吸"}

    # ② 个股利空型：跌幅远超大盘（弱于大盘3个点以上）
    if today_chg < -1 and _rel <= -3:
        return {"kind": "个股利空", "verdict": ("先减半·查因" if _hold else "回避·先查因"),
                "why": f"今日{today_chg:+.1f}%、比大盘弱{abs(_rel):.0f}个点——普跌解释不了这个跌幅，"
                       f"多半有个股利空，查明原因再决定，别急着{'扛' if _hold else '抄'}"}

    # ③ 错杀型：大盘普跌拖累，但自身逻辑没破（抗跌或跟跌但阶段良性）
    if market_chg <= -1.2 and (_up_stage or _rel >= -0.5) and not _down_stage:
        _low = _pos <= 30
        if _hold:
            return {"kind": "错杀", "verdict": "拿住·别割",
                    "why": f"大盘{market_chg:+.1f}%普跌拖累，但它{('抗跌' if _rel > 0.5 else '跟跌未破位')}、"
                           f"逻辑没坏——是错杀不是变质，别在恐慌里割在地板上"}
        return {"kind": "错杀", "verdict": ("可低吸·分批" if _low else "观察·等企稳"),
                "why": f"大盘{market_chg:+.1f}%普跌错杀，它{('已在52周低位' + str(int(_pos)) + '%' if _low else '逻辑没破')}——"
                       f"{'今天跌下来反而是低吸机会，分批别一把' if _low else '等缩量企稳信号确认再进，别追跌'}"}

    # ④ 绿灯型：入场时机已达标
    if entry_mode in ("现价可进", "回踩到位", "突破确认"):
        return {"kind": "可进", "verdict": "可进场",
                "why": f"今日{today_chg:+.1f}%、时机已到（{entry_mode}）——{'持仓可加' if _hold else '空仓可按计划分批进'}，仓位看大盘定调"}

    # ⑤ 其余：跟随统一动作，但说人话
    if _hold:
        return {"kind": "持有", "verdict": action or "持有观察",
                "why": f"今日{today_chg:+.1f}%，逻辑未破也未到加仓点——拿住看纪律，破位再走"}
    return {"kind": "观察", "verdict": "观察·等触发",
            "why": f"今日{today_chg:+.1f}%，方向/赔率未同时到位——等回踩企稳或放量突破，二者都没有就不动"}


def build_trade_plan(full, entry_plan=None, forward=None) -> dict:
    """【V88·三段作战计划】（2026-07-16 用户定纲：要明确"未来哪一天进、到什么区间出、什么时段出"）

    组装自现有引擎、不新算价格（一套判断）：
    - 短线：entry_timing 的日期窗进场 + 10日目标价出 + 止损作废（能给具体日期）；
    - 中线：4-8周区间分批 + 60日目标价出（≈1季） + 连续2日破MA55作废；
    - 长线：16周+区间 + 120日目标价出 + 破年线20日收不回退出。
    中长线给"区间+条件+周数"而非拍日历日——中长线进场靠条件确认，拍具体日是伪精确。
    """
    ep = entry_plan or {}
    fwd = forward or {}
    hz = {str(r.get("label")): r for r in (fwd.get("horizons") or [])}

    def _tp(label):
        r = hz.get(label) or {}
        return r.get("target_price"), r.get("risk_price"), r.get("p_up")

    t10, r10, p10 = _tp("10日")
    t60, _, p60 = _tp("60日")
    t120, _, p120 = _tp("120日")
    stop = _price_f(ep.get("stop") or full.get("stop"))
    plan = {}

    def _no_entry(_in):
        # 没进场就没有出场目标——不建仓/不进/不参与时目标价只会误导
        return any(k in str(_in) for k in ("不进", "不建仓", "不参与"))

    _s_in = str(ep.get("short_text") or "").split("；")[0] or "等待触发"
    plan["short"] = {
        "in": _s_in,
        "out": ("——（未进场无出场）" if _no_entry(_s_in) else
                (f"目标{t10:g}（10个交易日内·上行概率{p10}%）" if t10 else "目标待行情足量后给出")),
        "invalid": ("——" if _no_entry(_s_in) else (f"跌破{stop:g}作废" if stop else "破止损作废")),
        "mode": ep.get("mode", ""),
    }
    _m_in = str(ep.get("mid_text") or "中线条件未确认")
    plan["mid"] = {
        "in": _m_in,
        "out": ("——（未建仓无出场）" if _no_entry(_m_in) else
                (f"目标{t60:g}（≈1季·60交易日·上行概率{p60}%）" if t60 else "目标待确认")),
        "invalid": "——" if _no_entry(_m_in) else "收盘连续2日破MA55失效",
    }
    _l_in = str(ep.get("long_text") or "长线条件未确认")
    plan["long"] = {
        "in": _l_in,
        "out": ("——（未建仓无出场）" if _no_entry(_l_in) else
                (f"目标{t120:g}（≈半年·120交易日·上行概率{p120}%）" if t120 else "目标待确认")),
        "invalid": "——" if _no_entry(_l_in) else "跌破年线且20日收不回退出",
    }
    plan["probability_kind"] = "规则情景估计（非回测胜率）"
    return plan


def evaluate_decision(df=None, full=None, *, facts=None, holding=None,
                      action_hint="观察", analysis_time=None, name="", code="") -> dict:
    """返回所有模块必须展示/保存的唯一决策记录。"""
    full = full or {}
    facts = facts or build_horizon_facts(df, full=full)
    horizons = facts.get("horizons") or {}
    if not horizons:
        return {"schema": SCHEMA, "score_version": SCORE_VERSION,
                "error": facts.get("error", "行情不足"), "name": name, "code": code}
    short = _num((horizons.get("2周") or {}).get("rule_score"), 50)
    medium = _mean_scores(horizons, ("4周", "6周", "8周"), short)
    long_ = _num((horizons.get("16周") or {}).get("rule_score"), medium)
    long_avg = _mean_scores(horizons, ("4周", "6周", "8周", "16周"), medium)
    trend_quality = _clip(full.get("total", 50), 0, 100)

    last, resistance, stop, upside, downside = _scenario_prices(full, facts)
    p_up = int(round(_clip(short, 15, 85)))
    p_down = 100 - p_up
    rr = round(upside / downside, 2) if downside else 0.0
    expected = round((p_up / 100) * upside - (p_down / 100) * downside, 1)
    break_even = round(100 / (1 + rr), 1) if rr > 0 else 100.0
    edge = round(p_up - break_even, 1)
    entry_odds = round(_clip(
        50 + _clip(edge, -25, 25) * 1.0
        + _clip((rr - 1) * 14, -18, 24)
        + _clip(expected * 1.8, -15, 18), 0, 100))
    unified = round(sum({
        "short": short, "medium": medium, "long": long_,
        "trend_quality": trend_quality, "entry_odds": entry_odds,
    }[key] * weight for key, weight in SCORE_WEIGHTS.items()))

    short_side = "偏涨" if short >= 58 else ("偏跌" if short <= 42 else "震荡")
    long_side = "偏涨" if long_avg >= 58 else ("偏跌" if long_avg <= 42 else "震荡")
    conflict = ((short_side == "偏涨" and long_avg <= 48) or
                (short_side == "偏跌" and long_avg >= 52))
    protective = str(action_hint) in ("退出", "清仓", "减仓", "评估减仓", "回避")
    holding_like = bool(holding) or protective or "持有" in str(action_hint)
    if protective:
        action, entry_note = str(action_hint), "风险动作优先于周期评分"
    elif conflict:
        action = "仅观察·不追涨" if short_side == "偏涨" else "等待短线止跌"
        entry_note = "短中长期未共振，不建立新仓"
    elif short_side == long_side == "偏跌":
        action = "持仓保护" if holding_like else "回避"
        entry_note = "方向和赔率均不支持新仓"
    elif short_side == long_side == "偏涨":
        if rr >= 1.5 and expected > 1 and edge > 0:
            action = "持有·加仓复核" if holding_like else "多周期共振·试仓复核"
            entry_note = f"标准门槛通过；概率优势{edge:+.1f}点"
        elif p_up >= 65 and rr >= 0.8 and expected >= 2 and edge >= 8:
            action = "持有·小幅加仓复核" if holding_like else "共振·小仓试错"
            entry_note = f"激进门槛通过；概率优势{edge:+.1f}点"
        else:
            action = "持有观察·不加仓" if holding_like else "趋势偏多·等待回踩"
            entry_note = f"方向偏多但当前赔率不足；需上行>{break_even:.1f}%"
    else:
        action = "持有观察" if holding_like else "观察"
        entry_note = "周期未充分共振，等待触发"

    now = analysis_time or datetime.now(BJT).strftime("%Y-%m-%d %H:%M")
    cycle_status = ("周期冲突" if conflict else
                    ("多周期偏涨" if short_side == long_side == "偏涨" else
                     ("多周期偏跌" if short_side == long_side == "偏跌" else "周期未共振")))
    reason = f"{cycle_status}｜赔率{rr:.2f}"[:20]
    # 【V88·入场时机确认】短线=交易日窗口，中长线=周数+区间；等回踩必带突破改追双路径
    entry_plan = {}
    try:
        if full.get("last") and full.get("buy_zone"):
            entry_plan = entry_timing(full, short=short, medium=medium,
                                      long_avg=long_avg, action=action, rr=rr)
            if entry_plan.get("note"):
                entry_note = f"{entry_plan['note']}｜{entry_note}"
    except Exception:
        entry_plan = {}
    return {
        "schema": SCHEMA, "score_version": SCORE_VERSION,
        "score_weights": dict(SCORE_WEIGHTS), "data_signature": facts.get("data_signature", ""),
        "analysis_time": now, "data_asof": facts.get("asof", ""),
        "name": name, "code": code, "last": last,
        "short_score": round(short), "medium_score": round(medium),
        "long_score": round(long_), "trend_quality_score": round(trend_quality),
        "entry_odds_score": entry_odds, "unified_score": unified,
        "p_up": p_up, "p_down": p_down, "long_p_up": round(long_avg),
        "probability_kind": "规则情景估计（非回测胜率）",
        "horizon": "2周",
        "upside_pct": round(upside, 1), "downside_pct": round(downside, 1),
        "rr": rr, "expected_pct": expected, "break_even_p": break_even,
        "probability_edge": edge, "resistance": round(resistance, 3),
        "stop": round(stop, 3), "short_side": short_side, "long_side": long_side,
        "cycle_conflict": conflict, "cycle_status": cycle_status,
        "action": action, "reason": reason, "entry_note": entry_note,
        "entry_plan": entry_plan,
        "cycle_note": f"2周{short_side}{round(short)}%｜4-16周{long_side}{round(long_avg)}%",
        "facts": facts,
    }


def compact_text(card):
    return (f"统一分{card['unified_score']}（短{card['short_score']}/中{card['medium_score']}/长{card['long_score']}）｜"
            f"上{card['p_up']}%/下{card['p_down']}%（规则情景）｜盈亏比{card['rr']:.2f}｜"
            f"期望{card['expected_pct']:+.1f}%｜{card['action']}｜分析{card['analysis_time']}")


def _anchor_frame(df, anchor_time, anchor_price):
    """构建“当时可见”的行情截面；锚点日只使用用户提供的价格，杜绝日内未来函数。"""
    if df is None or len(df) == 0:
        return None, None, "行情为空"
    try:
        ts = pd.Timestamp(anchor_time)
        if ts.tzinfo is not None:
            ts = ts.tz_convert(BJT).tz_localize(None)
        price = float(anchor_price)
    except (TypeError, ValueError, OverflowError):
        return None, None, "分析时间或价格无效"
    if not math.isfinite(price) or price <= 0:
        return None, None, "分析价格必须大于0"

    work = df.copy()
    raw_idx = pd.to_datetime(work.index, errors="coerce")
    if getattr(raw_idx, "tz", None) is not None:
        raw_idx = raw_idx.tz_convert(BJT).tz_localize(None)
    work.index = raw_idx
    work = work[~work.index.isna()].sort_index()
    # 历史日内锚点不能读取该日收盘、高低或成交量；只保留此前交易日，再追加用户价格。
    before = work[work.index.normalize() < ts.normalize()].copy()
    if len(before) < 12:
        return None, ts, "锚点前有效行情不足12个交易日"
    vol = _series(before, "Volume")
    synthetic_volume = float(vol.tail(20).mean()) if len(vol) else 0.0
    row = {col: np.nan for col in before.columns}
    for col in ("Open", "High", "Low", "Close"):
        if col in row:
            row[col] = price
    if "Volume" in row:
        row["Volume"] = synthetic_volume
    anchor_row = pd.DataFrame([row], index=[ts])
    return pd.concat([before, anchor_row]).sort_index(), ts, ""


def _forward_horizon_rows(close, last, base_facts, days_tuple):
    """由收盘序列 + 多周期底稿算出每档「交易日」的概率/上下空间/盈亏比/期望，
    并给加权汇总与总体建议。概率是同源规则情景估计，不是回测胜率。
    「当下前瞻」evaluate_forward_outlook 与「锚点复盘」evaluate_anchor_outlook 共用此核，
    保证个股走势判断只有一套算法。"""
    horizons = base_facts.get("horizons") or {}
    daily_ret = close.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    daily_vol_pct = _num(daily_ret.tail(60).std()) * 100
    atr_pct = _num(base_facts.get("atr14")) / last * 100 if last else 0.0
    rows = []
    for days in days_tuple:
        fact = horizons.get(f"{days}日") or {}
        score = int(round(_clip(fact.get("rule_score", 50), 15, 85)))
        p_up, p_down = score, 100 - score
        trading_days = days
        sigma_move = daily_vol_pct * math.sqrt(trading_days)
        slope = _num(fact.get("slope_pct"))
        resistance = _num(fact.get("resistance"), last)
        support = _num(fact.get("support"), last)
        resistance_gap = max(0.0, (resistance / last - 1) * 100) if last else 0.0
        support_gap = max(0.0, (1 - support / last) * 100) if last else 0.0
        noise = max(sigma_move * 0.65, atr_pct * math.sqrt(max(trading_days, 1) / 14), 1.5)
        upside = _clip(max(resistance_gap, max(slope, 0) * 0.55, noise), 1.0, 40.0)
        downside = _clip(max(support_gap, max(-slope, 0) * 0.55, noise), 1.0, 30.0)
        rr = round(upside / downside, 2) if downside else 0.0
        ev = round(p_up / 100 * upside - p_down / 100 * downside, 1)
        break_even = round(100 / (1 + rr), 1) if rr else 100.0
        view = ("偏涨" if p_up >= 58 and ev > 0 else
                ("偏跌" if p_up <= 42 or ev < -1 else "震荡"))
        rows.append({
            "days": days, "weeks": days, "label": f"{days}日", "score": score,
            "p_up": p_up, "p_down": p_down, "probability_kind": "规则情景估计（非回测胜率）",
            "upside_pct": round(upside, 1), "downside_pct": round(downside, 1),
            "target_price": round(last * (1 + upside / 100), 3),
            "risk_price": round(last * (1 - downside / 100), 3),
            "rr": rr, "expected_pct": ev, "break_even_p": break_even,
            "view": view, "sample_days": fact.get("sample_days", 0),
            "trigger": f"站稳{last:.3f}并突破{max(last, resistance):.3f}",
            "invalid": f"跌破{last * (1 - downside / 100):.3f}后重评",
        })
    raw_w = {d: HORIZON_DAY_WEIGHTS.get(d, 1.0) for d in days_tuple}
    wsum = sum(raw_w.values()) or 1.0
    weights = {d: v / wsum for d, v in raw_w.items()}
    weighted_p = round(sum(r["p_up"] * weights[r["days"]] for r in rows))
    weighted_ev = round(sum(r["expected_pct"] * weights[r["days"]] for r in rows), 1)
    weighted_rr = round(sum(r["rr"] * weights[r["days"]] for r in rows), 2)
    long_rows = [r for r in rows if r["days"] > days_tuple[0]]
    long_bull = bool(long_rows) and all(r["p_up"] >= 55 for r in long_rows)
    long_bear = bool(long_rows) and all(r["p_up"] <= 45 for r in long_rows)
    if weighted_p >= 58 and long_bull and weighted_ev > 0:
        overall = "偏多·等待触发后分批参与"
    elif weighted_p <= 42 and long_bear:
        overall = "偏空·卖出或回避有依据"
    elif (rows[0]["p_up"] >= 58 and rows[-1]["p_up"] <= 48) or (
            rows[0]["p_up"] <= 42 and rows[-1]["p_up"] >= 52):
        overall = "周期分歧·避免一次性决策"
    else:
        overall = "中性·分批处理并等待确认"
    return rows, weighted_p, weighted_ev, weighted_rr, overall


def evaluate_forward_outlook(df, *, name="", code="", analysis_time=None,
                             days_tuple=HORIZON_DAYS, full=None) -> dict:
    """【当下前瞻】用最新收盘价，推算未来 5/10/20/60/120 交易日的上涨/下跌概率、
    盈亏比、目标/风险价，并给一句「拿/加/减/回避」建议。个股主动研判入口。
    概率是同源规则情景估计，不是回测胜率。"""
    close = _series(df, "Close")
    if len(close) < 12:
        return {"schema": "v88.stock-forward/1.0", "score_version": SCORE_VERSION,
                "error": "有效行情不足12个交易日", "name": name, "code": code}
    last = float(close.iloc[-1])
    ma20 = _num(close.tail(20).mean(), last)
    ma60 = _num(close.tail(60).mean(), ma20)
    stage = ("多头趋势" if last > ma20 > ma60 else
             ("转弱下跌" if last < ma20 < ma60 else "震荡整理"))
    base_facts = build_horizon_facts(df, {"stage": stage}, horizons=days_tuple, unit="day")
    if not (base_facts.get("horizons") or {}):
        return {"schema": "v88.stock-forward/1.0", "score_version": SCORE_VERSION,
                "error": base_facts.get("error", "行情不足"), "name": name, "code": code}
    rows, wp, wev, wrr, overall = _forward_horizon_rows(close, last, base_facts, days_tuple)
    if overall.startswith("偏多"):
        suggestion = "持有可继续拿/回踩分批加；空仓等触发再进"
    elif overall.startswith("偏空"):
        suggestion = "持有宜减仓或收紧止损；空仓回避"
    elif overall.startswith("周期分歧"):
        suggestion = "短长背离，别一次性动手，分批并盯失效位"
    else:
        suggestion = "中性观望，等概率与赔率同时转好再动"
    # 【V88·入场时机确认】传入趋势引擎结果时，把"何时进/什么价"落到交易日和区间
    entry_plan = {}
    try:
        if full and full.get("last") and full.get("buy_zone"):
            entry_plan = entry_timing(full, short=wp, medium=wp, long_avg=wp)
            if entry_plan.get("short_text"):
                suggestion = f"{suggestion}｜{entry_plan['short_text']}"
    except Exception:
        entry_plan = {}
    now = analysis_time or datetime.now(BJT).strftime("%Y-%m-%d %H:%M")
    return {
        "schema": "v88.stock-forward/1.0", "score_version": SCORE_VERSION,
        "name": name, "code": code, "last": round(last, 4), "stage": stage,
        "analysis_time": now, "data_asof": base_facts.get("asof", ""),
        "data_signature": base_facts.get("data_signature", ""),
        "weighted_p_up": wp, "weighted_p_down": 100 - wp,
        "weighted_rr": wrr, "weighted_expected_pct": wev,
        "overall_action": overall, "suggestion": suggestion,
        "entry_plan": entry_plan,
        "probability_kind": "规则情景估计（非回测胜率）",
        "horizons": rows,
    }


def evaluate_anchor_outlook(df, anchor_time, anchor_price, *, action="观察",
                            name="", code="", analysis_time=None) -> dict:
    """按个人决策时间/价格推算 5/10/20/60/120 个交易日，并跟踪到期后的真实结果。

    预测段只读取锚点前数据；当前价格和到期收益放在 ``tracking``，不参与旧预测。
    概率是同源规则情景估计，不是已校准的真实胜率。
    """
    hist, ts, error = _anchor_frame(df, anchor_time, anchor_price)
    if error:
        return {"schema": SCHEMA, "score_version": SCORE_VERSION, "error": error,
                "name": name, "code": code}
    close = _series(hist, "Close")
    last = float(anchor_price)
    ma20 = _num(close.tail(20).mean(), last)
    ma60 = _num(close.tail(60).mean(), ma20)
    stage = ("多头趋势" if last > ma20 > ma60 else
             ("转弱下跌" if last < ma20 < ma60 else "震荡整理"))
    base_facts = build_horizon_facts(hist, {"stage": stage}, horizons=HORIZON_DAYS, unit="day")
    horizons = base_facts.get("horizons") or {}
    if not horizons:
        return {"schema": SCHEMA, "score_version": SCORE_VERSION,
                "error": base_facts.get("error", "锚点行情不足"), "name": name, "code": code}

    rows, weighted_p, weighted_ev, weighted_rr, overall = _forward_horizon_rows(
        close, last, base_facts, HORIZON_DAYS)

    action_text = str(action or "观察")
    if any(x in action_text for x in ("卖", "清仓", "减仓")):
        if overall.startswith("偏多"):
            review = "当时不宜一次性清仓；宜分批锁利并保留回补条件"
        elif overall.startswith("偏空"):
            review = "当时卖出具备概率与赔率依据"
        else:
            review = "卖出并非明显错误，但宜分批并预设买回条件"
    elif any(x in action_text for x in ("买", "加仓")):
        review = ("当时买入具备多周期依据" if overall.startswith("偏多") else
                  "当时买入依据不足，需缩小仓位并服从失效位")
    else:
        review = "用 5/10/20/60/120 交易日触发与失效条件继续跟踪"

    # 真实结果只用于复盘，不回填或改写预测。
    work = df.copy()
    idx = pd.to_datetime(work.index, errors="coerce")
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert(BJT).tz_localize(None)
    work.index = idx
    work = work[~work.index.isna()].sort_index()
    after = work[work.index.normalize() > ts.normalize()]
    tracking_rows = []
    for row in rows:
        need = row["days"]
        if len(after) >= need:
            actual_price = _num(_series(after.iloc[:need], "Close").iloc[-1])
            actual_return = round((actual_price / last - 1) * 100, 1) if last else 0.0
            status = "已到期"
        else:
            actual_price, actual_return = None, None
            status = f"待验证（已有{len(after)}/{need}个交易日）"
        tracking_rows.append({"days": row["days"], "weeks": row["days"], "status": status,
                              "actual_price": actual_price, "actual_return_pct": actual_return})
    current_close = _series(work, "Close")
    _latest_market_ts = pd.Timestamp(current_close.index[-1]) if len(current_close) else None
    _market_covers_anchor = bool(
        _latest_market_ts is not None
        and _latest_market_ts.normalize() >= ts.normalize()
    )
    current_price = _num(current_close.iloc[-1]) if _market_covers_anchor else None
    since_anchor_pct = (round((current_price / last - 1) * 100, 1)
                        if current_price is not None and last else None)
    now = analysis_time or datetime.now(BJT).strftime("%Y-%m-%d %H:%M")
    sig_raw = f"{base_facts.get('data_signature')}|{ts.isoformat()}|{last:.6f}|{action_text}"
    return {
        "schema": "v88.personal-anchor/1.0", "score_version": SCORE_VERSION,
        "name": name, "code": code, "anchor_time": ts.strftime("%Y-%m-%d %H:%M"),
        "anchor_price": round(last, 4), "anchor_action": action_text,
        "analysis_time": now, "data_asof": base_facts.get("asof", ""),
        "data_signature": hashlib.sha256(sig_raw.encode("utf-8")).hexdigest()[:12],
        "no_lookahead": True, "stage_at_anchor": stage,
        "weighted_p_up": weighted_p, "weighted_p_down": 100 - weighted_p,
        "weighted_rr": weighted_rr, "weighted_expected_pct": weighted_ev,
        "overall_action": overall, "decision_review": review,
        "horizons": rows,
        "tracking": {"current_price": round(current_price, 4) if current_price is not None else None,
                     "since_anchor_pct": since_anchor_pct,
                     "market_covers_anchor": _market_covers_anchor,
                     "market_asof": (str(_latest_market_ts)[:19]
                                     if _latest_market_ts is not None else ""),
                     "rows": tracking_rows},
    }
