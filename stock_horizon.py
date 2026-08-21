"""V88 个股 2/4/6/8/16 周周期判断。

确定性部分只根据行情计算多周期底稿；Kimi Code订阅K3-256K的reasoning-high
只做证据复核和情景归纳，不得编造价格、新闻或把置信度冒充回测胜率。
"""
from __future__ import annotations

import hashlib
from html import escape
import json
import math
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from kimi_subscription import api_key as kimi_api_key, chat_completion, message_text, model_name


HORIZONS = (2, 4, 6, 8, 16)
BJT = timezone(timedelta(hours=8))
CACHE_DIR = Path.home() / ".cache_v88" / "stock_horizon"
CACHE_TTL = 6 * 3600


def _clip(value, low, high):
    return max(low, min(high, float(value)))


def _series(df, name):
    if df is None or name not in df:
        return pd.Series(dtype=float)
    raw = df[name]
    if isinstance(raw, pd.DataFrame):
        raw = raw.iloc[:, 0]
    return pd.to_numeric(raw, errors="coerce").dropna()


def build_horizon_facts(df, full=None) -> dict:
    """生成五档可审计行情底稿。方向是量化先验，不是未来胜率。"""
    # 唯一实现位于 v88_decision_core；本模块只保留兼容入口与可视化。
    from v88_decision_core import build_horizon_facts as _canonical_facts
    return _canonical_facts(df, full=full)

    # 以下旧实现保留一版仅便于历史审计，不再执行。
    close = _series(df, "Close")
    high = _series(df, "High")
    low = _series(df, "Low")
    volume = _series(df, "Volume")
    if len(close) < 12:
        return {"error": "有效行情不足12个交易日", "horizons": {}}

    last = float(close.iloc[-1])
    stage = str((full or {}).get("stage") or "")
    stage_bias = 0
    if any(x in stage for x in ("主升", "启动", "多头", "强势")):
        stage_bias = 6
    elif any(x in stage for x in ("破位", "退潮", "下跌", "转弱")):
        stage_bias = -6

    out = {}
    for weeks in HORIZONS:
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
        if len(vw) >= 8 and float(vw.iloc[:-5].mean() or 0) > 0:
            vol_ratio = float(vw.iloc[-5:].mean() / vw.iloc[:-5].mean())

        recent_high = float(high.iloc[-min(n, len(high)):].max()) if len(high) else float(window.max())
        recent_low = float(low.iloc[-min(n, len(low)):].min()) if len(low) else float(window.min())
        drawdown = (last / recent_high - 1) * 100 if recent_high else 0.0
        volume_push = 0.0
        if vol_ratio >= 1.15:
            volume_push = 4.0 if ret5 >= 0 else -4.0
        elif vol_ratio <= 0.75:
            volume_push = -1.5 if ret5 >= 0 else 1.5

        score = (50 + _clip(ret, -20, 20) * 0.75
                 + _clip(slope_move, -15, 15) * 0.65
                 + _clip(ma_bias, -10, 10) * 0.65
                 + volume_push + stage_bias)
        score = round(_clip(score, 15, 85))
        view = "偏涨" if score >= 59 else ("偏跌" if score <= 41 else "震荡")
        confidence = round(_clip(50 + abs(score - 50) * 1.2, 50, 88))
        out[f"{weeks}周"] = {
            "weeks": weeks,
            "sample_days": n,
            "return_pct": round(ret, 1),
            "slope_pct": round(slope_move, 1),
            "ma_bias_pct": round(ma_bias, 1),
            "ret5_pct": round(ret5, 1),
            "volume_ratio": round(vol_ratio, 2),
            "drawdown_pct": round(drawdown, 1),
            "support": round(recent_low, 3),
            "resistance": round(recent_high, 3),
            "rule_score": score,
            "rule_view": view,
            "rule_confidence": confidence,
        }
    return {
        "asof": str(close.index[-1])[:19],
        "last": round(last, 4),
        "stage": stage or "阶段待核",
        "horizons": out,
    }


def _brief(value, limit=20):
    return re.sub(r"\s+", " ", str(value or "").strip())[:limit]


def _parse_json(text):
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", str(text or "").strip())
    try:
        return json.loads(text)
    except Exception:
        match = re.search(r"\{.*\}", text, re.S)
        try:
            return json.loads(match.group()) if match else {}
        except Exception:
            return {}


def _cache_key(symbol, facts, context):
    raw = json.dumps({"schema": "v88.stock_horizon/1.1", "symbol": symbol,
                      "facts": facts, "context": context},
                     ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _cache_read(key):
    path = CACHE_DIR / f"{key}.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if time.time() - float(data.get("cached_at", 0)) <= CACHE_TTL:
            data["status"] = "cached"
            return data
    except Exception:
        pass
    return None


def _cache_write(key, data):
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        payload = dict(data, cached_at=time.time())
        (CACHE_DIR / f"{key}.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        pass


def _fallback_reason(label, view, stage):
    """无 AI / 超预算时的兜底：不出现术语，用大白话说清方向与操作。"""
    if view == "偏涨":
        return f"{stage}，{label}方向偏多、上行空间大于回撤风险，可持有或回踩分批跟进"
    if view == "偏跌":
        return f"{stage}，{label}方向偏弱、下行风险大于上行空间，宜观望或减仓等企稳"
    return f"{stage}，{label}多空拉锯、方向未定，等站稳突破或跌破关键价再动"


# 三类对象各自的"人话理由"要融合的三方面（个股/大盘/板块共用一套引擎，口径一致）。
_REASON_KINDS = {
    "个股": ("个股研判官", "基本面 + 个股新闻 + 技术面"),
    "大盘": ("大盘研判官", "宏观政策 + 资金面情绪 + 技术面"),
    "板块": ("板块研判官", "行业景气逻辑 + 板块催化新闻 + 技术面"),
}


def forward_reasons(name, symbol, fwd, context="", api_key="", kind="个股", allow_ai=True) -> dict:
    """给「当下前瞻」的每个周期配一句中文人话理由（按 kind 融合基本面/宏观/行业 + 新闻 + 技术面，
    不出现术语），供阅读者看懂。K3-256K思考生成，订阅不可用/失败时回退到确定性大白话。
    概率仍由确定性引擎给出，这里只负责把理由讲人话，不改概率。个股/大盘/板块共用此函数。

    allow_ai=False 时强制走确定性大白话，即使环境里有 Key 也不调用 AI、不花预算——
    页面「默认规则版、手动才开思考模式」的预算闸门靠它。"""
    role, fuse = _REASON_KINDS.get(kind, _REASON_KINDS["个股"])
    rows = fwd.get("horizons") or []
    stage = str(fwd.get("stage") or "当前")
    fallback = {
        "status": "fallback", "mode": "deterministic",
        "overall": f"{stage}，综合上涨概率{fwd.get('weighted_p_up')}%，{fwd.get('overall_action', '')}",
        "reasons": {r.get("label"): _fallback_reason(r.get("label"), r.get("view"), stage) for r in rows},
    }
    key = kimi_api_key(api_key)
    if not allow_ai or not key or not rows:
        return fallback
    _facts = {r.get("label"): {"方向": r.get("view"), "上涨概率": r.get("p_up"),
                               "上行空间%": r.get("upside_pct"), "下行风险%": r.get("downside_pct"),
                               "目标价": r.get("target_price"), "风险价": r.get("risk_price")}
              for r in rows}
    ckey = _cache_key(f"reason:{kind}:{symbol}", _facts, context)
    cached = _cache_read(ckey)
    if cached and cached.get("reasons"):
        return cached
    labels = "、".join(r.get("label") for r in rows)
    prompt = (
        f"你是V88{role}。请为{name}({symbol})的未来 {labels} 各写一句中文人话，"
        f"说明该周期判断的理由。硬性要求：①每句必须融合『{fuse}』三方面；"
        "②绝对不要出现 RSI、斜率、乖离率、MACD、均线、盈亏比 等术语或数字参数，要像跟朋友解释；"
        "③每句不超过40字，直接说方向和为什么；④不得编造价格、新闻或财报，证据不足就说依据有限；"
        "⑤每句要各不相同，对应不同周期。必须用强思考但只输出严格JSON："
        "{\"overall\":\"一句话总结，不超30字\",\"reasons\":{" +
        "，".join(f'\"{r.get("label")}\":\"…\"' for r in rows) + "}}。\n"
        f"确定性行情事实（只可参考方向，不要照抄数字）：{json.dumps(_facts, ensure_ascii=False)}\n"
        f"趋势阶段：{stage}\n"
        f"参考信息（{fuse}，只可使用其中明确事实，可能为空）：{_brief(context, 1500)}"
    )
    try:
        from v88_ai_budget import reserve, settle
        ticket = reserve(prompt, output_tokens=1400,
                         priority=True, scope="stock-horizon-reasons")
    except Exception:
        ticket = {"id": "untracked", "rmb": 0}
        settle = lambda *a, **k: None
    if not ticket:
        return dict(fallback, status="budget")
    try:
        body = chat_completion(
            [{"role": "user", "content": prompt}], key=key, model=model_name(),
            reasoning_effort="high", temperature=0.3, max_tokens=3000,
            response_format={"type": "json_object"}, timeout=150,
        )
        settle(ticket, body.get("usage"), ok=True)
        parsed = _parse_json(message_text(body)) or {}
        ai_reasons = parsed.get("reasons") or {}
        reasons = {}
        for r in rows:
            lab = r.get("label")
            reasons[lab] = _brief(ai_reasons.get(lab) or fallback["reasons"][lab], 44)
        result = {"status": "completed", "mode": "thinking-high",
                  "model": model_name(),
                  "analysis_time": datetime.now(BJT).strftime("%Y-%m-%d %H:%M（北京时间）"),
                  "overall": _brief(parsed.get("overall") or fallback["overall"], 30),
                  "reasons": reasons}
        _cache_write(ckey, result)
        return result
    except Exception:
        try:
            settle(ticket, ok=False)
        except Exception:
            pass
        return dict(fallback, status="failed")


def thinking_review(name, symbol, facts, context="", api_key="") -> dict:
    """K3-256K reasoning-high复核；失败时由页面继续展示规则底稿。"""
    key = kimi_api_key(api_key)
    if not key:
        return {"status": "no_key", "reason": "Kimi Code订阅未配置", "horizons": {}}
    ckey = _cache_key(symbol, facts, context)
    cached = _cache_read(ckey)
    if cached:
        return cached

    fact_rows = facts.get("horizons") or {}
    prompt = (
        f"你是V88个股周期复核官。请对{name}({symbol})进行2/4/6/8/16周走势复核。"
        "必须使用强思考，但不要输出思维链，只输出严格JSON。规则行情底稿是唯一价格事实，"
        "不得编造价格、新闻、财报或确定性涨跌。置信度表示证据一致性，不是回测胜率。"
        "每个周期都必须独立判断，不得把同一句话复制五遍。JSON格式："
        "{\"summary\":\"最多35字\",\"cycle_phase\":\"蓄势/领涨/派发/退潮/震荡\","
        "\"horizons\":{\"2周\":{\"view\":\"偏涨/震荡/偏跌\",\"confidence\":0-100,"
        "\"reason\":\"不超20字\",\"catalyst\":\"不超20字\",\"risk\":\"不超20字\"},"
        "\"4周\":{},\"6周\":{},\"8周\":{},\"16周\":{}},"
        "\"action\":\"观察/持有/试仓/减仓/回避\",\"invalid_summary\":\"不超25字\"}。"
        "若证据不足就写震荡并降低置信度。\n"
        f"量化行情底稿：{json.dumps(fact_rows, ensure_ascii=False)}\n"
        f"趋势阶段：{facts.get('stage')}；现价：{facts.get('last')}；数据截至：{facts.get('asof')}\n"
        f"补充上下文（可能为空，仅可使用明确事实）：{_brief(context, 1200)}"
    )
    try:
        from v88_ai_budget import reserve, settle
        ticket = reserve(prompt, output_tokens=2200,
                         priority=True, scope="stock-cycle-thinking")
    except Exception:
        ticket = {"id": "untracked", "rmb": 0}
        settle = lambda *a, **k: None
    if not ticket:
        return {"status": "budget", "reason": "网页AI预算闸门或6小时缓存", "horizons": {}}

    try:
        body = chat_completion(
            [{"role": "user", "content": prompt}], key=key, model=model_name(),
            reasoning_effort="high", temperature=0.2, max_tokens=4000,
            response_format={"type": "json_object"}, timeout=150,
        )
        settle(ticket, body.get("usage"), ok=True)
        parsed = _parse_json(message_text(body))
        allowed_views = {"偏涨", "震荡", "偏跌"}
        clean = {}
        for label in (f"{w}周" for w in HORIZONS):
            row = ((parsed.get("horizons") or {}).get(label) or {})
            view = str(row.get("view") or "震荡")
            if view not in allowed_views:
                view = "震荡"
            clean[label] = {
                "view": view,
                "confidence": round(_clip(row.get("confidence") or 45, 20, 90)),
                "reason": _brief(row.get("reason") or "证据不足", 20),
                "catalyst": _brief(row.get("catalyst") or "等待催化", 20),
                "risk": _brief(row.get("risk") or "趋势反转", 20),
            }
        # 失效条件必须锚定真实行情底稿，不允许AI写成日期说明或抽象空话。
        short_fact = fact_rows.get("2周") or next(iter(fact_rows.values()), {})
        support = short_fact.get("support")
        resistance = short_fact.get("resistance")
        view_list = [x.get("view") for x in clean.values()]
        if view_list.count("偏涨") >= 3:
            invalid_summary = f"跌破2周支撑{support}则失效"
        elif view_list.count("偏跌") >= 3:
            invalid_summary = f"站回2周压力{resistance}并放量则重评"
        else:
            invalid_summary = f"突破{resistance}/跌破{support}再评估"
        result = {
            "status": "completed",
            "mode": "thinking-high",
            "model": model_name(),
            "analysis_time": datetime.now(BJT).strftime("%Y-%m-%d %H:%M（北京时间）"),
            "summary": _brief(parsed.get("summary") or "五周期复核完成", 35),
            "cycle_phase": _brief(parsed.get("cycle_phase") or "震荡", 8),
            "action": _brief(parsed.get("action") or "观察", 8),
            "invalid_summary": _brief(invalid_summary, 25),
            "horizons": clean,
        }
        _cache_write(ckey, result)
        return result
    except Exception as exc:
        try:
            settle(ticket, ok=False)
        except Exception:
            pass
        return {"status": "failed", "reason": type(exc).__name__, "horizons": {}}


def analyze(name, symbol, df, full=None, context="", api_key="") -> dict:
    facts = build_horizon_facts(df, full=full)
    review = thinking_review(name, symbol, facts, context=context, api_key=api_key) if facts.get("horizons") else {
        "status": "insufficient", "reason": facts.get("error", "行情不足"), "horizons": {}}
    return {"facts": facts, "review": review}


def _rule_reason(fact: dict) -> str:
    """无 AI 复核时的规则版人话理由（纯确定性拼装）——云端没配 key 也有实质分析,不留"等待AI复核"占位。"""
    _v = str(fact.get("rule_view") or "震荡")
    _ret = float(fact.get("return_pct") or 0)
    _vr = float(fact.get("volume_ratio") or 1)
    _bias = float(fact.get("ma_bias_pct") or 0)
    _parts = []
    if _v == "偏涨":
        _parts.append(f"区间{_ret:+.1f}%走强" if _ret > 0 else "结构偏多")
    elif _v == "偏跌":
        _parts.append(f"区间{_ret:+.1f}%走弱" if _ret < 0 else "结构偏空")
    else:
        _parts.append(f"区间{_ret:+.1f}%横向震荡")
    if _vr >= 1.2:
        _parts.append(f"放量({_vr:.1f}倍)")
    elif _vr <= 0.75:
        _parts.append("缩量")
    if abs(_bias) >= 3:
        _parts.append(f"{'上' if _bias > 0 else '下'}偏均线{abs(_bias):.0f}%")
    return "、".join(_parts) + "（规则底稿）"


def table_rows(result) -> list[dict]:
    facts = (result or {}).get("facts") or {}
    review = (result or {}).get("review") or {}
    ai_rows = review.get("horizons") or {}
    _has_ai = review.get("status") in ("completed", "cached")
    rows = []
    for weeks in HORIZONS:
        label = f"{weeks}周"
        fact = (facts.get("horizons") or {}).get(label) or {}
        if not fact:
            continue
        ai = ai_rows.get(label) or {}
        view = ai.get("view") or fact.get("rule_view") or "震荡"
        confidence = ai.get("confidence") or fact.get("rule_confidence") or 50
        rows.append({
            "周期": label,
            "量化底稿": f"{fact.get('rule_view')}({fact.get('rule_score')}/100)",
            "思考复核": view if _has_ai else "—（未接AI）",
            "综合置信度": f"{int(confidence)}%（非胜率）",
            "历史动量": f"{fact.get('return_pct', 0):+.1f}%",
            "量比": fact.get("volume_ratio"),
            "理由": ai.get("reason") or _rule_reason(fact),
            "催化": ai.get("catalyst") or "—",
            "风险": ai.get("risk") or f"跌破{fact.get('support')}转弱",
            "支撑/压力": f"{fact.get('support')} / {fact.get('resistance')}",
        })
    return rows


def cycle_alignment(facts: dict) -> dict:
    """统一首页决策卡与深度分析的周期方向口径（仅用可审计规则底稿）。"""
    horizons = (facts or {}).get("horizons") or {}
    short = horizons.get("2周") or next(iter(horizons.values()), {})
    short_up = int(round(_clip(short.get("rule_score") or 50, 15, 85)))
    long_scores = [float((horizons.get(f"{w}周") or {}).get("rule_score"))
                   for w in (4, 6, 8, 16)
                   if (horizons.get(f"{w}周") or {}).get("rule_score") is not None]
    long_up = int(round(sum(long_scores) / len(long_scores))) if long_scores else short_up
    short_side = "偏涨" if short_up >= 58 else ("偏跌" if short_up <= 42 else "震荡")
    long_side = "偏涨" if long_up >= 58 else ("偏跌" if long_up <= 42 else "震荡")
    long_tone = ("偏涨" if long_up >= 58 else ("偏强" if long_up >= 52 else
                 ("偏跌" if long_up <= 42 else ("偏弱" if long_up <= 48 else "震荡"))))
    # 不只拦“完全反向”，也拦短期很强但中长线均值已落到50以下的期限错配。
    # 紫金矿业这类2周反弹、4-16周持续转弱必须自动降级，不能继续显示可关注。
    conflict = ((short_side == "偏涨" and long_up <= 48) or
                (short_side == "偏跌" and long_up >= 52))
    if short_side == "偏涨" and long_up <= 48:
        status, action = "短弹长弱", "仅观察·不追涨"
    elif short_side == "偏跌" and long_up >= 52:
        status, action = "短空长修", "等短线止跌"
    elif short_side == long_side == "偏涨":
        status, action = "多周期偏涨", "再核盈亏比"
    elif short_side == long_side == "偏跌":
        status, action = "多周期偏跌", "回避/保护"
    else:
        status, action = "周期未共振", "仅观察"
    return {
        "horizon": "2周",
        "p_up": short_up,
        "p_down": 100 - short_up,
        "long_p_up": long_up,
        "short_side": short_side,
        "long_side": long_side,
        "conflict": conflict,
        "status": status,
        "safe_action": action,
        "note": f"2周{short_side}{short_up}%｜4-16周{long_tone}{long_up}%",
    }


def align_decision_card(card: dict, facts: dict) -> dict:
    """用五周期底稿覆盖首页孤立短线概率；周期冲突拥有最高降级权。"""
    from v88_decision_core import evaluate_decision
    base = dict(card or {})
    synthetic_last = float((facts or {}).get("last") or 100)
    upside = float(base.get("upside_pct") or 0)
    downside = float(base.get("downside_pct") or 0)
    full = {
        "last": synthetic_last,
        "total": base.get("trend_quality_score", 50),
        "resistance": base.get("resistance") or synthetic_last * (1 + upside / 100),
        "stop": base.get("stop") or synthetic_last * (1 - downside / 100),
    }
    hint = base.get("action", "观察")
    # 兼容旧两步调用：旧卡的“回避”只是初筛结果，不是已确认的风险指令。
    if hint == "回避":
        hint = "观察"
    canonical = evaluate_decision(
        full=full, facts=facts,
        holding=base.get("holding"),
        action_hint=hint,
        analysis_time=base.get("analysis_time"),
    )
    if hint in ("退出", "清仓", "减仓", "评估减仓"):
        canonical["entry_note"] = "持仓先执行风险复核"
    base.update(canonical)
    return base

    # 以下旧实现保留一版仅便于历史审计，不再执行。
    out = dict(card or {})
    align = cycle_alignment(facts)
    out.update({
        "horizon": align["horizon"],
        "p_up": align["p_up"],
        "p_down": align["p_down"],
        "long_p_up": align["long_p_up"],
        "cycle_conflict": align["conflict"],
        "cycle_status": align["status"],
        "cycle_note": align["note"],
    })
    upside = float(out.get("upside_pct") or 0)
    downside = float(out.get("downside_pct") or 0)
    out["expected_pct"] = round(
        (align["p_up"] / 100) * upside - (align["p_down"] / 100) * downside, 1)
    rr = float(out.get("rr") or 0)
    break_even_p = round(100 / (1 + rr), 1) if rr > 0 else 100.0
    probability_edge = round(align["p_up"] - break_even_p, 1)
    out["break_even_p"] = break_even_p
    out["probability_edge"] = probability_edge
    original_action = str(out.get("action") or "观察")
    protective_action = original_action in ("退出", "清仓", "减仓", "评估减仓")
    holding_like = protective_action or "持有" in original_action
    if align["conflict"]:
        out["action"] = align["safe_action"]
        out["reason"] = align["note"][:20]
        out["entry_note"] = "周期未共振，不建立新仓"
    elif protective_action:
        # 持仓风险优先级高于看涨周期，避免周期结论掩盖止损/利润保护。
        out["action"] = original_action
        out["reason"] = f"风险动作优先｜{align['note']}"[:20]
        out["entry_note"] = "持仓先执行风险复核"
    elif align["status"] == "多周期偏跌":
        out["action"] = "持仓保护" if holding_like else "回避"
        out["reason"] = align["note"][:20]
        out["entry_note"] = "方向与赔率均不支持新仓"
    elif align["status"] == "多周期偏涨":
        if rr >= 1.5 and out["expected_pct"] > 1:
            out["action"] = ("持有·加仓复核" if holding_like
                             else "多周期共振·试仓复核")
            out["entry_note"] = f"标准门槛通过｜概率优势{probability_edge:+.1f}点"
        elif (align["p_up"] >= 65 and rr >= 0.8 and
              out["expected_pct"] >= 2 and probability_edge >= 8):
            # 个人激进风格的受控入口：仍须多周期共振、正期望和足够概率优势，
            # 但允许盈亏比略低于1时用小仓试错，绝不等同重仓买入。
            out["action"] = ("持有·小幅加仓复核" if holding_like
                             else "共振·小仓试错")
            out["entry_note"] = f"激进门槛通过｜概率优势{probability_edge:+.1f}点"
        else:
            out["action"] = ("持有观察·不加仓" if holding_like
                             else "趋势偏多·等待回踩")
            out["entry_note"] = (f"当前赔率不足｜需上行估计>{break_even_p:.1f}%"
                                 if probability_edge <= 0 else
                                 f"概率有利但赔率不足｜优势{probability_edge:+.1f}点")
        out["reason"] = align["note"][:20]
    else:
        # 未共振时不得因单一正期望升级成买入语言。
        if str(out.get("action") or "") in ("试仓复核", "持有/试仓复核"):
            out["action"] = "仅观察·待共振"
        out["reason"] = align["note"][:20]
        out["entry_note"] = "周期未共振，等待确认"
    return out


def _visual_score(fact: dict, review: dict) -> float:
    """把量化先验与AI方向复核合成仅供画图的热度坐标，不冒充胜率。"""
    rule = _clip(fact.get("rule_score") or 50, 15, 85)
    view = str(review.get("view") or fact.get("rule_view") or "震荡")
    conf = _clip(review.get("confidence") or fact.get("rule_confidence") or 50, 20, 90)
    if view == "偏涨":
        ai = 50 + (conf - 50) * 0.8
    elif view == "偏跌":
        ai = 50 - (conf - 50) * 0.8
    else:
        ai = 50
    return round(_clip(rule * 0.52 + ai * 0.48, 20, 80), 1)


def _turning_candidate(points: list[dict]) -> dict:
    """找第一个斜率反向点；没有反向时明确写趋势延续，不制造拐点。"""
    if len(points) < 2:
        return {"label": "数据不足", "horizon": "", "kind": "等待"}
    diffs = [points[i]["score"] - points[i - 1]["score"] for i in range(1, len(points))]
    for i in range(1, len(diffs)):
        if diffs[i - 1] * diffs[i] < 0 and abs(diffs[i - 1]) + abs(diffs[i]) >= 3:
            kind = "升温转弱" if diffs[i - 1] > 0 else "降温企稳"
            return {"label": f"{points[i]['label']}附近·{kind}",
                    "horizon": points[i]["label"], "kind": kind}
    trend = points[-1]["score"] - points[0]["score"]
    kind = "升温延续" if trend >= 2 else ("退潮延续" if trend <= -2 else "区间震荡")
    return {"label": f"至{points[-1]['label']}·{kind}", "horizon": "", "kind": kind}


def cycle_visual_html(result: dict, name: str, symbol: str,
                      element_id: str = "v88-stock-horizon-visual") -> str:
    """个股深度分析首屏周期图：象限时钟 + 2/4/6/8/16周走向 + 触发/失效。"""
    facts = (result or {}).get("facts") or {}
    review = (result or {}).get("review") or {}
    review_source = "AI" if review.get("status") in ("completed", "cached") else "规则"
    fact_rows = facts.get("horizons") or {}
    ai_rows = review.get("horizons") or {}
    points = []
    for weeks in HORIZONS:
        label = f"{weeks}周"
        fact = fact_rows.get(label) or {}
        if not fact:
            continue
        ai = ai_rows.get(label) or {}
        points.append({
            "label": label,
            # 主轨迹必须与首页卡片完全同源；AI仅作为解释复核，不再改写坐标或动作。
            "score": round(_clip(fact.get("rule_score") or 50, 15, 85), 1),
            "rule_up": int(round(_clip(fact.get("rule_score") or 50, 15, 85))),
            "view": ai.get("view") or fact.get("rule_view") or "震荡",
            "confidence": int(ai.get("confidence") or fact.get("rule_confidence") or 50),
            "reason": _brief(ai.get("reason") or _rule_reason(fact), 24),
            "catalyst": _brief(ai.get("catalyst") or "—", 20),
            "risk": _brief(ai.get("risk") or f"跌破{fact.get('support')}转弱", 20),
            "support": fact.get("support"),
            "resistance": fact.get("resistance"),
        })
    if not points:
        return ""

    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "-", element_id)
    marker_up, marker_down = f"{safe_id}-up", f"{safe_id}-down"
    phase = str(review.get("cycle_phase") or facts.get("stage") or "震荡")
    phase_key = next((key for key in ("蓄势", "领涨", "派发", "退潮") if key in phase), "震荡")
    phase_xy = {
        "蓄势": (-0.62, 0.02), "领涨": (0.02, -0.62),
        "派发": (0.62, 0.02), "退潮": (0.02, 0.62), "震荡": (0.0, 0.0),
    }
    px, py = phase_xy[phase_key]
    # 【V88·今天锚点 2026-07-18 用户点单】走向图必须从"今天"画起，2周前不能是空白。
    # 今天热度=当前相位基准位 + 近5日实际动量微调（相位与左侧象限钟同一口径）。
    _phase_base = {"蓄势": 45, "领涨": 62, "派发": 55, "退潮": 38, "震荡": 50}[phase_key]
    try:
        _ret5_now = float((fact_rows.get("2周") or {}).get("ret5_pct") or 0)
    except (TypeError, ValueError):
        _ret5_now = 0.0
    now_score = round(_clip(_phase_base + _clip(_ret5_now, -6, 6) * 1.2, 20, 80), 1)
    delta = points[-1]["score"] - now_score
    direction = "升温" if delta >= 2 else ("退潮" if delta <= -2 else "震荡")
    arrow_dy = -24 if direction == "升温" else (24 if direction == "退潮" else 0)
    color = "#16a34a" if direction == "升温" else ("#ef4444" if direction == "退潮" else "#64748b")
    turn = _turning_candidate(points)
    alignment = cycle_alignment(facts)

    # 左侧周期象限。
    cw, ch, cx, cy, radius = 430, 224, 154, 112, 76
    mx, my = cx + px * radius, cy + py * radius
    clock = [f'<svg viewBox="0 0 {cw} {ch}" role="img" aria-label="{escape(name)}个股周期轮换象限">']
    clock.append('<defs>'
                 f'<marker id="{marker_up}" markerWidth="7" markerHeight="7" refX="5" refY="3" orient="auto"><path d="M0 0 L6 3 L0 6z" fill="#16a34a"/></marker>'
                 f'<marker id="{marker_down}" markerWidth="7" markerHeight="7" refX="5" refY="3" orient="auto"><path d="M0 0 L6 3 L0 6z" fill="#ef4444"/></marker>'
                 '</defs>')
    clock.append(f'<circle class="grid" cx="{cx}" cy="{cy}" r="{radius}" fill="none"/>')
    clock.append(f'<circle class="grid" cx="{cx}" cy="{cy}" r="38" fill="none" stroke-dasharray="3 4"/>')
    clock.append(f'<line class="axis" x1="{cx}" y1="{cy-radius}" x2="{cx}" y2="{cy+radius}"/>')
    clock.append(f'<line class="axis" x1="{cx-radius}" y1="{cy}" x2="{cx+radius}" y2="{cy}"/>')
    clock.append(f'<text class="mut" x="{cx}" y="24" text-anchor="middle">领涨启动</text>')
    clock.append(f'<text class="mut" x="{cx}" y="210" text-anchor="middle">退潮杀跌</text>')
    clock.append(f'<text class="mut" x="{cx+radius+8}" y="{cy+4}">高位派发</text>')
    clock.append(f'<text class="mut" x="{cx-radius-8}" y="{cy+4}" text-anchor="end">低位蓄势</text>')
    if arrow_dy:
        marker = marker_up if arrow_dy < 0 else marker_down
        clock.append(f'<line x1="{mx:.0f}" y1="{my:.0f}" x2="{mx:.0f}" y2="{my+arrow_dy:.0f}" stroke="{color}" stroke-width="2" marker-end="url(#{marker})"/>')
    clock.append(f'<circle cx="{mx:.0f}" cy="{my:.0f}" r="8" fill="{color}"><title>{escape(name)}·{escape(phase)}·{direction}</title></circle>')
    clock.append(f'<text class="title" x="286" y="58">{escape(name)} · {escape(symbol)}</text>')
    clock.append(f'<text class="value" x="286" y="86">当前相位：{escape(phase)}</text>')
    clock.append(f'<text class="value" x="286" y="110">周期走向：{direction} 今天{now_score:.0f}→{points[-1]["label"]}{points[-1]["score"]:.0f}</text>')
    clock.append(f'<text class="value" x="286" y="134">预计拐点：{escape(turn["label"])}</text>')
    clock.append(f'<text class="note" x="286" y="160">圆点=当前相位 · 箭头=周期方向</text>')
    clock.append('</svg>')

    # 右侧走向轨迹：起点=今天（实算），其后=各周期预测；分数是方向坐标，不称为概率/胜率。
    tw, th, top, bottom = 700, 224, 42, 178
    _n_cols = len(points) + 1
    xs = [74 + i * (580 - 74) / max(1, _n_cols - 1) for i in range(_n_cols)]
    def y_of(score):
        return bottom - (float(score) - 20) / 60 * (bottom - top)
    y60, y45 = y_of(60), y_of(45)
    traj = [f'<svg viewBox="0 0 {tw} {th}" role="img" aria-label="{escape(name)}今天至十六周周期走向">']
    traj.append(f'<rect class="hot" x="54" y="{top}" width="550" height="{y60-top:.0f}"/>')
    traj.append(f'<rect class="warm" x="54" y="{y60:.0f}" width="550" height="{y45-y60:.0f}"/>')
    traj.append(f'<rect class="cold" x="54" y="{y45:.0f}" width="550" height="{bottom-y45:.0f}"/>')
    for label, yy in (("热", (top+y60)/2), ("温", (y60+y45)/2), ("冷", (y45+bottom)/2)):
        traj.append(f'<text class="mut" x="44" y="{yy+4:.0f}" text-anchor="end">{label}</text>')
    # 今天锚点列
    _x0 = xs[0]
    _y_now = y_of(now_score)
    traj.append(f'<line class="grid" x1="{_x0:.0f}" y1="{top}" x2="{_x0:.0f}" y2="{bottom}"/>')
    traj.append(f'<text class="mut" x="{_x0:.0f}" y="26" text-anchor="middle">今天</text>')
    seq = []
    for x, point in zip(xs[1:], points):
        y = y_of(point["score"])
        seq.append((x, y, point))
        traj.append(f'<line class="grid" x1="{x:.0f}" y1="{top}" x2="{x:.0f}" y2="{bottom}"/>')
        traj.append(f'<text class="mut" x="{x:.0f}" y="26" text-anchor="middle">{point["label"]}</text>')
    traj.append('<polyline points="' + f'{_x0:.0f},{_y_now:.0f} '
                + ' '.join(f'{x:.0f},{y:.0f}' for x, y, _ in seq)
                + f'" fill="none" stroke="{color}" stroke-width="3" stroke-linejoin="round"/>')
    traj.append(f'<circle cx="{_x0:.0f}" cy="{_y_now:.0f}" r="6" fill="{color}" stroke="var(--card-bg,#fff)" stroke-width="2">'
                f'<title>今天热度{now_score:.0f}/100（当前相位{escape(phase)}+近5日{_ret5_now:+.1f}%实算，非预测）</title></circle>')
    traj.append(f'<text class="tiny" x="{_x0:.0f}" y="{min(th-8, _y_now+22):.0f}" text-anchor="middle">现在·{escape(phase_key)}</text>')
    for x, y, point in seq:
        ring = point["label"] == turn.get("horizon")
        if ring:
            traj.append(f'<circle cx="{x}" cy="{y:.0f}" r="11" fill="none" stroke="{color}" stroke-width="2" stroke-dasharray="3 2"/>')
        fill = color if point["confidence"] >= 65 else "var(--card-bg,#fff)"
        traj.append(f'<circle cx="{x}" cy="{y:.0f}" r="6" fill="{fill}" stroke="{color}" stroke-width="2"><title>{point["label"]}·{review_source}{point["view"]}·证据一致性{point["confidence"]}%（非胜率）·{escape(point["reason"])}</title></circle>')
        traj.append(f'<text class="tiny" x="{x}" y="{min(th-8, y+22):.0f}" text-anchor="middle">{review_source}{point["view"]}·{point["confidence"]}%</text>')
    traj.append(f'<text class="note" x="54" y="214">横轴=今天→各周期（今天=当前相位+5日动量实算，其后=预测） · 纵轴=综合方向热度 · 虚线环=预计拐点 · 百分比=证据一致性</text>')
    traj.append('</svg>')

    # 每档条件完整保留在紧凑文字区，触发/风险均来自同一轮AI复核。
    detail_rows = []
    for p in points:
        detail_rows.append(
            f'<div class="hz-row"><b>{p["label"]} {review_source}{p["view"]}·一致性{p["confidence"]}%</b>'
            f'<span>规则上行估计：{p["rule_up"]}%</span>'
            f'<span>理由：{escape(p["reason"])}</span><span>触发：{escape(p["catalyst"])}</span>'
            f'<span>风险：{escape(p["risk"])}</span></div>')
    analysis_time = escape(str(review.get("analysis_time") or facts.get("asof") or "时间待更新"))
    model = escape(str(review.get("model") or "量化底稿"))
    review_mode = ("thinking-high" if review.get("status") in ("completed", "cached")
                   else "规则底稿·AI未复核")
    summary = escape(str(review.get("summary") or "五周期量价底稿"))
    decision = (result or {}).get("decision") or {}
    display_action = (str(decision.get("action")) if decision.get("action") else
                      (alignment["safe_action"] if alignment["conflict"]
                       else str(review.get("action") or "观察")))
    action = escape(display_action)
    invalid = escape(str(review.get("invalid_summary") or "突破/跌破关键位重评"))
    css = f'''
<style>
#{safe_id}{{color:var(--foreground,var(--text-color));margin:.18rem 0 .65rem}}
#{safe_id} .head{{display:flex;justify-content:space-between;gap:.6rem;align-items:center;padding:.38rem .55rem;border-radius:8px;background:color-mix(in srgb,var(--primary-color,#2563eb) 9%,transparent);font-size:12px}}
#{safe_id} .head small{{font-size:10px;color:var(--muted-foreground,var(--text-color))}}
#{safe_id} .grid2{{display:grid;grid-template-columns:minmax(320px,.78fr) minmax(480px,1.22fr);gap:.42rem;margin-top:.42rem}}
#{safe_id} .card{{min-width:0;border:1px solid color-mix(in srgb,currentColor 12%,transparent);border-radius:9px;background:color-mix(in srgb,currentColor 4%,transparent);padding:.2rem .35rem}}
#{safe_id} svg{{display:block;width:100%;height:auto;overflow:visible}}
#{safe_id} svg text{{font-family:inherit;font-size:11px;fill:currentColor}}
#{safe_id} svg .title{{font-size:12px;font-weight:700}} #{safe_id} svg .value{{font-size:10.5px}}
#{safe_id} svg .note,#{safe_id} svg .tiny{{font-size:9px;fill:var(--muted-foreground,var(--text-color))}}
#{safe_id} svg .mut{{fill:var(--muted-foreground,var(--text-color))}}
#{safe_id} svg .grid{{stroke:color-mix(in srgb,currentColor 16%,transparent);stroke-width:1}}
#{safe_id} svg .axis{{stroke:color-mix(in srgb,currentColor 36%,transparent);stroke-width:1}}
#{safe_id} svg .hot{{fill:#22c55e;fill-opacity:.11}} #{safe_id} svg .warm{{fill:#f59e0b;fill-opacity:.11}} #{safe_id} svg .cold{{fill:#ef4444;fill-opacity:.10}}
#{safe_id} .summary{{display:flex;gap:.7rem;flex-wrap:wrap;margin:.35rem 0 .2rem;padding:.34rem .5rem;border-left:3px solid var(--primary-color,#2563eb);background:color-mix(in srgb,currentColor 4%,transparent);font-size:11px}}
#{safe_id} .hz-details{{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:.28rem}}
#{safe_id} .hz-row{{min-width:0;padding:.32rem .4rem;border:1px solid color-mix(in srgb,currentColor 11%,transparent);border-radius:7px;font-size:9px}}
#{safe_id} .hz-row b,#{safe_id} .hz-row span{{display:block;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}} #{safe_id} .hz-row span{{color:var(--muted-foreground,var(--text-color));margin-top:.1rem}}
#{safe_id} .foot{{font-size:9px;color:var(--muted-foreground,var(--text-color));margin-top:.28rem}}
@media(max-width:900px){{#{safe_id} .grid2{{grid-template-columns:1fr}}#{safe_id} .hz-details{{grid-template-columns:1fr 1fr}}}}
</style>'''
    return (
        css + f'<div id="{safe_id}" role="figure" aria-label="{escape(name)}个股周期轮换总览">'
        f'<div class="head"><b>🧭 个股周期轮换总览 · 2/4/6/8/16周＋拐点</b>'
        f'<small>🕒 分析于 {analysis_time} · {model} · {review_mode}</small></div>'
        f'<div class="grid2"><div class="card">{"".join(clock)}</div><div class="card">{"".join(traj)}</div></div>'
        f'<div class="summary"><b>{summary}</b><span>{escape(alignment["note"])}</span>'
        f'<span>{"⚠️ 周期冲突·" if alignment["conflict"] else "统一口径·"}综合动作：{action}</span>'
        f'<span>失效：{invalid}</span></div>'
        f'<div class="hz-details">{"".join(detail_rows)}</div>'
        f'<div class="foot">图形用于周期与条件复核；方向热度和置信度均不是历史胜率，也不替代盈亏比与仓位纪律。</div>'
        '</div>'
    )
