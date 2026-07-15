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


def build_horizon_facts(df, full=None, horizons=HORIZONS) -> dict:
    """生成唯一的2/4/6/8/16周行情底稿；分数是方向先验，不是胜率。"""
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
    out = {}
    for weeks in tuple(horizons or HORIZONS):
        target_days = weeks * 5
        n = min(target_days, len(close) - 1)
        if n < 5:
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
        out[f"{weeks}周"] = {
            "weeks": weeks, "sample_days": n,
            "return_pct": round(ret, 1), "slope_pct": round(slope_move, 1),
            "ma_bias_pct": round(ma_bias, 1), "ret5_pct": round(ret5, 1),
            "volume_ratio": round(vol_ratio, 2), "drawdown_pct": round(drawdown, 1),
            "support": round(recent_low, 3), "resistance": round(recent_high, 3),
            "rule_score": score,
            "rule_view": "偏涨" if score >= 59 else ("偏跌" if score <= 41 else "震荡"),
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


def evaluate_anchor_outlook(df, anchor_time, anchor_price, *, action="观察",
                            name="", code="", analysis_time=None) -> dict:
    """按个人决策时间/价格推算2、5、8、16周，并跟踪到期后的真实结果。

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
    base_facts = build_horizon_facts(hist, {"stage": stage}, horizons=(2, 5, 8, 16))
    horizons = base_facts.get("horizons") or {}
    if not horizons:
        return {"schema": SCHEMA, "score_version": SCORE_VERSION,
                "error": base_facts.get("error", "锚点行情不足"), "name": name, "code": code}

    daily_ret = close.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    daily_vol_pct = _num(daily_ret.tail(60).std()) * 100
    atr_pct = _num(base_facts.get("atr14")) / last * 100 if last else 0.0
    rows = []
    for weeks in (2, 5, 8, 16):
        fact = horizons.get(f"{weeks}周") or {}
        score = int(round(_clip(fact.get("rule_score", 50), 15, 85)))
        p_up, p_down = score, 100 - score
        trading_days = weeks * 5
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
            "weeks": weeks, "label": f"{weeks}周", "score": score,
            "p_up": p_up, "p_down": p_down, "probability_kind": "规则情景估计（非回测胜率）",
            "upside_pct": round(upside, 1), "downside_pct": round(downside, 1),
            "target_price": round(last * (1 + upside / 100), 3),
            "risk_price": round(last * (1 - downside / 100), 3),
            "rr": rr, "expected_pct": ev, "break_even_p": break_even,
            "view": view, "sample_days": fact.get("sample_days", 0),
            "trigger": f"站稳{last:.3f}并突破{max(last, resistance):.3f}",
            "invalid": f"跌破{last * (1 - downside / 100):.3f}后重评",
        })

    weights = {2: .25, 5: .30, 8: .25, 16: .20}
    weighted_p = round(sum(r["p_up"] * weights[r["weeks"]] for r in rows))
    weighted_ev = round(sum(r["expected_pct"] * weights[r["weeks"]] for r in rows), 1)
    weighted_rr = round(sum(r["rr"] * weights[r["weeks"]] for r in rows), 2)
    long_rows = [r for r in rows if r["weeks"] >= 5]
    long_bull = all(r["p_up"] >= 55 for r in long_rows)
    long_bear = all(r["p_up"] <= 45 for r in long_rows)
    if weighted_p >= 58 and long_bull and weighted_ev > 0:
        overall = "偏多·等待触发后分批参与"
    elif weighted_p <= 42 and long_bear:
        overall = "偏空·卖出或回避有依据"
    elif (rows[0]["p_up"] >= 58 and rows[-1]["p_up"] <= 48) or (
            rows[0]["p_up"] <= 42 and rows[-1]["p_up"] >= 52):
        overall = "周期分歧·避免一次性决策"
    else:
        overall = "中性·分批处理并等待确认"

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
        review = "用2/5/8/16周触发与失效条件继续跟踪"

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
        need = row["weeks"] * 5
        if len(after) >= need:
            actual_price = _num(_series(after.iloc[:need], "Close").iloc[-1])
            actual_return = round((actual_price / last - 1) * 100, 1) if last else 0.0
            status = "已到期"
        else:
            actual_price, actual_return = None, None
            status = f"待验证（已有{len(after)}/{need}个交易日）"
        tracking_rows.append({"weeks": row["weeks"], "status": status,
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
