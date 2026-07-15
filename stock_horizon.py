"""V88 个股 2/4/6/8/16 周周期判断。

确定性部分只根据行情计算多周期底稿；DeepSeek V4 Flash 的 thinking-high
只做证据复核和情景归纳，不得编造价格、新闻或把置信度冒充回测胜率。
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests


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


def thinking_review(name, symbol, facts, context="", api_key="") -> dict:
    """DeepSeek thinking-high 复核；失败时返回状态，由页面继续展示规则底稿。"""
    key = str(api_key or os.getenv("DEEPSEEK_API_KEY", "")).strip()
    if not key:
        return {"status": "no_key", "reason": "未配置DeepSeek API Key", "horizons": {}}
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
        ticket = reserve(prompt, output_tokens=2200)
    except Exception:
        ticket = {"id": "untracked", "rmb": 0}
        settle = lambda *a, **k: None
    if not ticket:
        return {"status": "budget", "reason": "网页AI预算闸门或6小时缓存", "horizons": {}}

    try:
        response = requests.post(
            "https://api.deepseek.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={
                "model": os.getenv("DEEPSEEK_REVIEW_MODEL", "deepseek-v4-flash"),
                "messages": [{"role": "user", "content": prompt}],
                "thinking": {"type": "enabled"},
                "reasoning_effort": "high",
                "temperature": 0.2,
                "max_tokens": 4000,
            },
            timeout=150,
        )
        if response.status_code != 200:
            settle(ticket, ok=False)
            return {"status": "failed", "reason": f"HTTP {response.status_code}", "horizons": {}}
        body = response.json()
        settle(ticket, body.get("usage"), ok=True)
        parsed = _parse_json(body["choices"][0]["message"].get("content", ""))
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
            "model": os.getenv("DEEPSEEK_REVIEW_MODEL", "deepseek-v4-flash"),
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


def table_rows(result) -> list[dict]:
    facts = (result or {}).get("facts") or {}
    review = (result or {}).get("review") or {}
    ai_rows = review.get("horizons") or {}
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
            "思考复核": view,
            "综合置信度": f"{int(confidence)}%（非胜率）",
            "历史动量": f"{fact.get('return_pct', 0):+.1f}%",
            "量比": fact.get("volume_ratio"),
            "理由": ai.get("reason") or "等待AI复核",
            "催化": ai.get("catalyst") or "—",
            "风险": ai.get("risk") or "—",
            "支撑/压力": f"{fact.get('support')} / {fact.get('resistance')}",
        })
    return rows
