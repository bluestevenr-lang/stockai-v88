"""V88 个股过去 2/4/8/16/32 周档的行情证据回看。

确定性部分只根据行情计算多周期底稿；GPT-6 Codex订阅GPT-6 Astra的reasoning-high
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

import pandas as pd
from desktop_gpt_subscription import api_key as gpt_subscription_ready, chat_completion, message_text, model_name


from v88_decision_core import HORIZONS  # One cycle contract for facts, GPT schema and display.
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
    """保留 canonical 数值，补充每个过去观察窗口的实际首末日期。"""
    # 唯一实现位于 v88_decision_core；本模块只保留兼容入口与可视化。
    from v88_decision_core import build_horizon_facts as _canonical_facts
    facts = _canonical_facts(df, full=full)
    close = _series(df, "Close")
    for label, row in (facts.get("horizons") or {}).items():
        n = int(row.get("sample_days") or 0)
        requested = int(row.get("weeks") or 0) * 5
        row.update(evidence_scope="historical-lookback", requested_days=requested,
                   window_complete=n >= requested if requested else False,
                   lookback_start=str(close.index[-(n + 1)])[:10],
                   lookback_end=str(close.index[-1])[:10])
    facts["evidence_scope"] = "historical-lookback"
    return facts


def _brief(value, limit=20):
    return re.sub(r"\s+", " ", str(value or "").strip())[:limit]


def _prompt_context(context, limit):
    """Keep the audited compact projection intact, bound legacy free text."""
    try:
        from deep_prompt_context import model_context
        projected = model_context(context)
        if projected is not None:
            return projected
    except ImportError:
        pass
    return _brief(context, limit)


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
    raw = json.dumps({"schema": "v88.stock_horizon/3.0-historical", "model": model_name(), "symbol": symbol,
                      "facts": facts, "context": context},
                     ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _cache_read(key):
    path = CACHE_DIR / f"{key}.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("model") == model_name() and 0 <= time.time() - float(data.get("cached_at", 0)) <= CACHE_TTL:
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
    """只解释辅助证据，缺少审核时不生成买卖动作。"""
    if view == "偏涨":
        return f"{stage}，{label}规则方向偏强；延续仍须量价与企业证据确认"
    if view == "偏跌":
        return f"{stage}，{label}规则方向偏弱；修复须停止创新低并补齐企业证据"
    return f"{stage}，{label}多空证据分歧，方向仍待结构确认"


# 三类对象各自的"人话理由"要融合的三方面（个股/大盘/板块共用一套引擎，口径一致）。
_REASON_KINDS = {
    "个股": ("个股研判官", "基本面 + 个股新闻 + 技术面"),
    "大盘": ("大盘研判官", "宏观政策 + 资金面情绪 + 技术面"),
    "板块": ("板块研判官", "行业景气逻辑 + 板块催化新闻 + 技术面"),
}


def forward_reasons(name, symbol, fwd, context="", api_key="", kind="个股", allow_ai=True) -> dict:
    """给「当下前瞻」的每个周期配一句中文人话理由（按 kind 融合基本面/宏观/行业 + 新闻 + 技术面，
    不出现术语），供阅读者看懂。GPT-6 Astra思考生成，订阅不可用/失败时回退到确定性大白话。
    规则参考数来自确定性引擎，这里只解释证据，不输出上涨概率或买卖许可。个股/大盘/板块共用此函数。

    allow_ai=False 时强制走确定性大白话，即使环境里有 Key 也不调用 AI、不花预算——
    页面「默认规则版、手动才开思考模式」的预算闸门靠它。"""
    role, fuse = _REASON_KINDS.get(kind, _REASON_KINDS["个股"])
    rows = fwd.get("horizons") or []
    stage = str(fwd.get("stage") or "当前")
    fallback = {
        "status": "fallback", "mode": "deterministic",
        "overall": f"{stage}；各观察档仅作条件研究，仍须核实企业证据与原合同",
        "reasons": {r.get("label"): _fallback_reason(r.get("label"), r.get("view"), stage) for r in rows},
    }
    key = gpt_subscription_ready(api_key)
    if not allow_ai or not key or not rows:
        return fallback
    _facts = {r.get("label"): {"方向": r.get("view"), "规则方向参考数（不是概率）": r.get("p_up"),
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
        "⑤每句要各不相同，对应不同周期；不提供未经中央授权的买卖动作或确定转折时间。必须用强思考但只输出严格JSON："
        "{\"overall\":\"一句话总结，不超30字\",\"reasons\":{" +
        "，".join(f'\"{r.get("label")}\":\"…\"' for r in rows) + "}}。\n"
        f"确定性行情事实（只可参考方向，不要照抄数字）：{json.dumps(_facts, ensure_ascii=False)}\n"
        f"趋势阶段：{stage}\n"
        f"参考信息（{fuse}，只可使用其中明确事实，可能为空）：{_prompt_context(context, 1500)}"
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
            reasons[lab] = _research_text(_brief(ai_reasons.get(lab) or fallback["reasons"][lab], 44))
        result = {"status": "completed", "mode": "thinking-high",
                  "model": model_name(),
                  "analysis_time": datetime.now(BJT).strftime("%Y-%m-%d %H:%M（北京时间）"),
                  "overall": _research_text(_brief(parsed.get("overall") or fallback["overall"], 30)),
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
    """GPT-6 Astra reasoning-high复核；失败时由页面继续展示规则底稿。"""
    key = gpt_subscription_ready(api_key)
    if not key:
        return {"status": "no_key", "reason": "GPT-6 Codex订阅未配置", "horizons": {}}
    ckey = _cache_key(symbol, facts, context)
    cached = _cache_read(ckey)
    if cached:
        return cached

    fact_rows = facts.get("horizons") or {}
    prompt = (
        f"你是V88个股历史窗口复核官。请对{name}({symbol})过去2/4/8/16/32周档的行情证据复核。"
        "必须使用强思考，但不要输出思维链，只输出严格JSON。规则行情底稿是唯一价格事实，"
        "每周档按5个交易日回看，所有窗口终点相同；不得解释成从今天起的未来走势、转折时间或上涨概率。"
        "不得编造价格、新闻、财报或买卖动作。confidence只保留旧接口字段，页面不显示百分比。"
        "每个周期都必须独立判断，不得把同一句话复制五遍。JSON格式："
        "{\"summary\":\"最多35字\",\"cycle_phase\":\"蓄势/领涨/派发/退潮/震荡\","
        "\"horizons\":{\"2周\":{\"view\":\"偏涨/震荡/偏跌\",\"confidence\":0-100,"
        "\"reason\":\"不超20字\",\"catalyst\":\"不超20字\",\"risk\":\"不超20字\"},"
        "\"4周\":{},\"32周\":{},\"8周\":{},\"16周\":{}},"
        "\"action\":\"辅助研究\",\"invalid_summary\":\"不超25字\"}。"
        f"只返回这些周期键：{[str(w)+'周' for w in HORIZONS]}；上方示例键以本列表为准。"
        "经典核对：短期按欧奈尔量价/斯波朗迪止错；中期按E&M趋势确认；长期按林奇盈利/Siegel长期逻辑。"
        "证据不足明确写证据不足，不把不确定性冒充震荡预测。\n"
        f"量化行情底稿：{json.dumps(fact_rows, ensure_ascii=False)}\n"
        f"趋势阶段：{facts.get('stage')}；现价：{facts.get('last')}；数据截至：{facts.get('asof')}\n"
        f"补充上下文（可能为空，仅可使用明确事实）：{_prompt_context(context, 1200)}"
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
        expected = {f"{w}周" for w in HORIZONS}
        if set(parsed.get("horizons") or {}) != expected:
            raise ValueError("GPT-6返回周期不齐或多余，拒绝错位复核")
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
            "action": "辅助研究",
            "evidence_scope": "historical-lookback",
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


def analyze(name, symbol, df, full=None, context="", api_key="", allow_ai=True) -> dict:
    facts = build_horizon_facts(df, full=full)
    review = thinking_review(name, symbol, facts, context=context, api_key=api_key) if allow_ai and facts.get("horizons") else {
        "status": "insufficient", "reason": facts.get("error", "行情不足"), "horizons": {}}
    if not allow_ai and facts.get('horizons'):
        review = {'status': 'deterministic', 'reason': '量价辅助研究；中央GPT双审见上方', 'horizons': {}}
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
    period = (result or {}).get('period_consistency') or {}
    if isinstance(period, dict) and period.get('status') == 'blocked':
        return []
    facts = (result or {}).get("facts") or {}
    review = (result or {}).get("review") or {}
    ai_rows = _review_rows(review)
    rows = []
    for weeks in HORIZONS:
        label = f"{weeks}周"
        fact = (facts.get("horizons") or {}).get(label) or {}
        if not fact:
            continue
        ai = ai_rows.get(label) or {}
        view = _past_view(fact.get("rule_view"))
        score = _score(fact.get('rule_score'))
        score_text = f"{score:g}/100" if score is not None else "缺少分数"
        sample = fact.get('sample_days')
        requested = fact.get('requested_days', weeks * 5)
        incomplete = sample is not None and sample < requested
        rows.append({
            "周期": f"过去{label}档",
            "跨周期关联": _period_status_label(period.get('status')) if isinstance(period, dict) and period.get('status') else '未关联年度条件研判',
            "实际观察范围": _window_text(fact),
            "样本": f"{sample if sample is not None else '未记录'}/{requested}交易日间隔" + (' · 不足整档' if incomplete else ''),
            "量化底稿": f"{view}（{score_text}）",
            "规则方向分": score_text,
            "思考复核": _past_view(ai.get('view')) if ai else "—（无本口径复核）",
            "历史价格变动": f"{fact.get('return_pct', 0):+.1f}%",
            "量比": fact.get("volume_ratio"),
            "理由": _research_text(ai.get("reason")) if ai.get('reason') else _rule_reason(fact),
            "确认条件": _research_text(ai.get("catalyst")) if ai.get('catalyst') else "高低点与量价继续同向才保留结构判断",
            "反证": _research_text(ai.get("risk")) if ai.get('risk') else "结构改变或企业反证出现时重评",
            "历史低/高参考": f"{fact.get('support')} / {fact.get('resistance')}",
        })
    return rows


def cycle_alignment(facts: dict) -> dict:
    """回看档位的一致性。旧 p_up 键仅兼容调用方，数值语义是方向分。"""
    horizons = (facts or {}).get("horizons") or {}
    short = horizons.get("2周") or next(iter(horizons.values()), {})
    short_score = _score(short.get("rule_score"))
    short_up = int(round(short_score if short_score is not None else 50))
    long_scores = [float((horizons.get(f"{w}周") or {}).get("rule_score"))
                   for w in (4, 8, 16, 32)
                   if (horizons.get(f"{w}周") or {}).get("rule_score") is not None]
    long_up = int(round(sum(long_scores) / len(long_scores))) if long_scores else short_up
    short_side = "偏涨" if short_up >= 59 else ("偏跌" if short_up <= 41 else "震荡")
    long_side = "偏涨" if long_up >= 59 else ("偏跌" if long_up <= 41 else "震荡")
    # 不只拦“完全反向”，也拦短期很强但中长线均值已落到50以下的期限错配。
    # 紫金矿业这类2周反弹、4-32周持续转弱必须自动降级，不能继续显示可关注。
    conflict = ((short_side == "偏涨" and long_up <= 48) or
                (short_side == "偏跌" and long_up >= 52))
    if short_side == "偏涨" and long_up <= 48:
        status = "短弹长弱"
    elif short_side == "偏跌" and long_up >= 52:
        status = "短空长修"
    elif short_side == long_side == "偏涨":
        status = "多周期偏涨"
    elif short_side == long_side == "偏跌":
        status = "多周期偏跌"
    else:
        status = "周期未共振"
    return {
        "horizon": "2周",
        "p_up": short_up,
        "p_down": 100 - short_up,
        "long_p_up": long_up,
        "short_side": short_side,
        "long_side": long_side,
        "conflict": conflict,
        "status": status,
        "safe_action": "历史窗口分歧·需复核" if conflict else "辅助研究·核对原合同",
        "short_score": short_up, "long_score": long_up,
        "score_semantics": "historical-direction-score-not-probability",
        "note": f"过去2周{_past_view(short_side)}{short_up}/100｜过去4/8/16/32周规则均分{long_up}/100；观察窗口并列，非未来路径",
    }


def align_decision_card(card: dict, facts: dict) -> dict:
    """保留旧入口并委托 canonical 技术计算；此处不重定义评级或合同。"""
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


def _score(value):
    """Do not clamp or synthesize a display score from confidence."""
    try:
        number = float(value)
        return number if math.isfinite(number) and 0 <= number <= 100 else None
    except (TypeError, ValueError):
        return None


def _past_view(view):
    return {"偏涨": "历史偏强", "偏跌": "历史偏弱", "震荡": "历史震荡"}.get(str(view), "结构待核")


def _window_text(fact):
    begin, end = fact.get('lookback_start'), fact.get('lookback_end')
    return f'{begin} — {end}' if begin and end else '实际首末日期未记录'


def _research_text(value):
    text = str(value or '')
    if re.search(r'买入|卖出|加仓|减仓|试仓|跟进|买点|卖点|必涨|必跌|预计.{0,8}(?:周|日|天).{0,8}(?:拐点|转折)', text):
        return '原说明超出辅助研究范围，请查中央原合同'
    return text


def _review_rows(review):
    # Legacy cache text treated weeks as future horizons. It cannot inherit the
    # new historical scope merely because the symbol/date still match.
    if (review.get('status') in ('completed', 'cached')
            and review.get('evidence_scope') == 'historical-lookback'):
        return review.get('horizons') or {}
    return {}


def _visual_score(fact: dict, review: dict) -> float | None:
    """Compatibility helper: graph uses the unmodified rule score only."""
    return _score(fact.get('rule_score'))


def _turning_candidate(points: list[dict]) -> dict:
    """Different past-window scores do not establish a future turning date."""
    return {'label': '历史窗口不能确定未来转折时间', 'horizon': '', 'kind': '待验证'}


def _period_status_label(status):
    return {'caution':'风险优先', 'divergent':'周期分歧', 'linked':'已关联',
            'limited':'证据不足', 'blocked':'待复核'}.get(status, '待复核')


def _period_consistency_html(result):
    """Render the caller's already-bound conclusion without inferring a grade."""
    supplied = (result or {}).get('period_consistency')
    if not isinstance(supplied, dict):
        return ''
    e = lambda value: escape(str(value if value is not None else '待核'))
    html = '<div class="hz-period-consistency" data-input-id="'+e(supplied.get('input_id', ''))+'" style="padding:8px;border-left:3px solid #d97706;margin:8px 0">'
    if supplied.get('headline'):
        html += '<b>'+e(supplied['headline'])+'</b>'
    if supplied.get('status') is not None:
        html += '<span> · 状态：'+e(_period_status_label(supplied['status']))+'</span>'
    if supplied.get('summary'):
        html += '<div>'+e(supplied['summary'])+'</div>'
    conditions = supplied.get('conditions') or []
    if isinstance(conditions, dict):
        conditions = [f'{key}：{value}' for key, value in conditions.items()]
    elif not isinstance(conditions, list):
        conditions = [conditions]
    for condition in conditions:
        if isinstance(condition, dict):
            condition = '；'.join(f'{key}：{value}' for key, value in condition.items())
        html += '<div>条件：'+e(condition)+'</div>'
    return html+'</div>'


def historical_visual_html(result: dict, name: str, symbol: str,
                      element_id: str = "v88-stock-horizon-visual") -> str:
    """Compare past windows at one snapshot; do not draw a future trajectory."""
    consistency = _period_consistency_html(result)
    period = (result or {}).get('period_consistency') or {}
    if isinstance(period, dict) and period.get('status') == 'blocked':
        return consistency+'<div class="hz-binding-blocked" style="font-size:12px;color:#b45309">跨周期事实待复核；当前图表与表格暂停展示。</div>'
    if not isinstance(period, dict) or not period.get('status'):
        consistency += '<div class="hz-unlinked" style="font-size:11px;color:#64748b">未关联年度条件研判：以下仅为独立历史窗口观察。</div>'
    facts = (result or {}).get('facts') or {}
    review = (result or {}).get('review') or {}
    fact_rows = facts.get('horizons') or {}
    points = []
    for weeks in HORIZONS:
        fact = fact_rows.get(f'{weeks}周') or {}
        if fact:
            points.append({'label': f'过去{weeks}周档', 'weeks': weeks,
                           'score': _visual_score(fact, {}), 'fact': fact})
    if not points:
        return consistency
    e = lambda value: escape(str(value if value is not None else '待核'))
    safe_id = re.sub(r'[^a-zA-Z0-9_-]', '-', element_id) or 'v88-stock-horizon-visual'
    # Independent columns share one score scale. There is no today anchor,
    # inter-window connecting line, future date axis or projected turning ring.
    left, right, top, bottom = 70., 770., 24., 174.
    svg = ['<svg class="hz-history-comparison" viewBox="0 0 850 272" role="img" aria-label="过去2、4、8、16、32周档的规则方向分并列比较">']
    svg.append('<title>'+e(name)+' · 历史窗口规则方向分</title>')
    for score in (0, 25, 50, 75, 100):
        y = bottom-score/100*(bottom-top)
        svg.append(f'<line x1="{left}" y1="{y:.1f}" x2="{right}" y2="{y:.1f}" stroke="#e2e8f0"/>')
        svg.append(f'<text x="58" y="{y+4:.1f}" text-anchor="end" fill="#64748b" font-size="10">{score}</text>')
    for i, point in enumerate(points):
        x = left+(i+.5)*(right-left)/len(points)
        score = point['score']; fact = point['fact']
        fill = '#15803d' if score is not None and score >= 59 else '#b91c1c' if score is not None and score <= 41 else '#64748b'
        if score is not None:
            height = score/100*(bottom-top)
            svg.append(f'<rect class="hz-window-score" x="{x-25:.1f}" y="{bottom-height:.1f}" width="50" height="{height:.1f}" rx="3" fill="{fill}"><title>{e(point["label"])} · {score:g}/100 · {e(_window_text(fact))}</title></rect>')
            svg.append(f'<text x="{x:.1f}" y="{bottom-height-5:.1f}" text-anchor="middle" fill="{fill}" font-size="12">{score:g}/100</text>')
        else:
            svg.append(f'<text x="{x:.1f}" y="150" text-anchor="middle" fill="#64748b" font-size="11">缺少分数</text>')
        svg.append(f'<text x="{x:.1f}" y="194" text-anchor="middle" font-size="12">{e(point["label"])}</text>')
        begin = fact.get('lookback_start') or '起点未记录'
        end = fact.get('lookback_end') or str(facts.get('asof') or '终点未记录')[:10]
        sample = fact.get('sample_days'); requested = fact.get('requested_days', point['weeks']*5)
        suffix = ' · 不足整档' if sample is not None and sample < requested else ''
        svg.append(f'<text x="{x:.1f}" y="211" text-anchor="middle" fill="#64748b" font-size="9">{e(begin)} — {e(end)}</text>')
        svg.append(f'<text x="{x:.1f}" y="227" text-anchor="middle" fill="#64748b" font-size="9">{e(sample if sample is not None else "待核")}/{e(requested)}交易日间隔{suffix}</text>')
    svg.append('<text x="70" y="254" font-size="10" fill="#64748b">横轴为不同历史观察窗口；纵轴为规则方向分 /100。同一终点并列比较，不连成未来走势。</text></svg>')
    rows = table_rows(result)
    detail = '<div class="hz-scroll"><table class="hz-window-evidence"><thead><tr><th>观察档</th><th>历史结构与事实</th><th>确认条件 / 反证</th></tr></thead><tbody>'
    for row in rows:
        detail += '<tr><td><b>'+e(row['周期'])+'</b><div>'+e(row['实际观察范围'])+'</div><div>'+e(row['样本'])+'</div></td>'
        detail += '<td><b>'+e(row['量化底稿'])+'</b><div>历史价格变动 '+e(row['历史价格变动'])+' · 量比 '+e(row['量比'])+'</div><div>'+e(row['理由'])+'</div></td>'
        detail += '<td><div>'+e(row['确认条件'])+'</div><div>反证：'+e(row['反证'])+'</div><div>历史低/高参考 '+e(row['历史低/高参考'])+'</div></td></tr>'
    detail += '</tbody></table></div>'
    current_review = bool(_review_rows(review))
    meta = ('同口径GPT历史证据复核' if current_review else '确定性量价底稿')
    alignment = cycle_alignment(facts)
    summary = '<div class="hz-summary">'+e(alignment['note'])+'</div>'
    if current_review and review.get('summary'):
        summary += '<div class="hz-summary">复核：'+e(_research_text(review['summary']))+'</div>'
    if review.get('status') in ('completed', 'cached') and not current_review:
        summary += '<div class="hz-caption">旧周期复核未声明历史窗口口径，当前采用可复算规则底稿。</div>'
    css = f"""<style>
#{safe_id}{{font-size:12px;line-height:1.55;margin:8px 0;color:var(--text-color,#334155)}}
#{safe_id} .hz-head{{padding:8px 0}}#{safe_id} .hz-caption{{font-size:10px;color:#64748b}}
#{safe_id} .hz-scroll{{max-width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch}}
#{safe_id} svg{{display:block;width:100%;min-width:850px;height:auto;background:#fff;color:#334155}}
#{safe_id} svg text{{font-family:inherit}}#{safe_id} .hz-summary{{margin:6px 0}}
#{safe_id} table{{min-width:900px;width:100%;border-collapse:collapse;font-size:11px}}
#{safe_id} td,#{safe_id} th{{padding:6px;border:1px solid #e2e8f0;text-align:left;vertical-align:top}}
#{safe_id} th{{background:#eff6ff}}#{safe_id} .hz-period-consistency{{overflow-wrap:anywhere}}
</style>"""
    return (css+f'<section id="{safe_id}" class="hz-historical-evidence" aria-label="{e(name)}历史窗口证据比较">'
            '<div class="hz-head"><b>五周期历史证据 · 过去2 / 4 / 8 / 16 / 32周档</b>'
            '<div class="hz-caption">'+e(name)+' · '+e(symbol)+' · 行情截至 '+e(facts.get('asof'))+' · '+meta+'</div></div>'
            '<div class="hz-caption">每周档按5个交易日回看；实际日期见各档。方向分描述已发生的量价结构，不能确定后续涨跌或转折时间；年度方向见同源条件研判。</div>'
            +consistency+'<div class="hz-scroll">'+''.join(svg)+'</div>'+summary+detail+
            '<div class="hz-caption">规则分不是上涨概率、收益预测或审核分。观察档共用同一行情，不构成多份独立验证；原评级与交易合同仍按中央记录。</div></section>')


def cycle_visual_html(result: dict, name: str, symbol: str,
                      element_id: str = "v88-stock-horizon-visual") -> str:
    """Primary future phase/curve; retain past-window calculations as evidence."""
    from future_trend_visual import render
    from modules.utils import to_yf_cn_code
    result = result if isinstance(result, dict) else {}
    future = result.get('future_scenario') if isinstance(result.get('future_scenario'), dict) else {}
    period = result.get('period_consistency') if isinstance(result.get('period_consistency'), dict) else {}
    references = future.get('reference_ids') if isinstance(future.get('reference_ids'), dict) else {}
    identity_ok = (to_yf_cn_code(str(future.get('code') or '').upper())
                   == to_yf_cn_code(str(symbol).upper())
                   == to_yf_cn_code(str(period.get('code') or '').upper()))
    binding_ok = (period.get('input_id') and references.get('period') == period['input_id']
                  and period.get('status') in ('linked', 'caution', 'divergent')
                  and future.get('source_asof') and future['source_asof'] == period.get('source_asof')
                  and future.get('snapshot_signature')
                  and future['snapshot_signature'] == period.get('snapshot_signature')
                  and references.get('annual') and references['annual'] == period.get('annual_input_id')
                  and references.get('synthesis') and references['synthesis'] == period.get('synthesis_input_id'))
    if future.get('status') == 'ready' and not (identity_ok and binding_ok):
        future = {'code': symbol, 'name': name, 'status': 'missing',
                  'headline': '未来趋势与本页事实待重新关联',
                  'gaps': ['个股或跨周期凭据未一致，不能复用其他快照的未来曲线']}
    elif not future:
        future = {'code': symbol, 'name': name, 'status': 'missing',
                  'headline': '未来趋势证据待补齐', 'gaps': ['尚未形成同包年度与跨周期研判']}
    primary = render(future)
    history = historical_visual_html(result, name, symbol, element_id+'-history')
    if history:
        primary += ('<details class="hz-history-archive" style="font-size:11px;color:#64748b;margin:8px 0">'
                    '<summary style="cursor:pointer">▸ 历史量价依据 · 按需展开</summary>'
                    +history+'</details>')
    return primary
