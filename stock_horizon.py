"""V88 个股 2/4/6/8/16 周周期判断。

确定性部分只根据行情计算多周期底稿；DeepSeek V4 Flash 的 thinking-high
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
            "score": _visual_score(fact, ai),
            "view": ai.get("view") or fact.get("rule_view") or "震荡",
            "confidence": int(ai.get("confidence") or fact.get("rule_confidence") or 50),
            "reason": _brief(ai.get("reason") or "量价底稿判断", 20),
            "catalyst": _brief(ai.get("catalyst") or "等待条件触发", 20),
            "risk": _brief(ai.get("risk") or "趋势反向", 20),
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
    delta = points[-1]["score"] - points[0]["score"]
    direction = "升温" if delta >= 2 else ("退潮" if delta <= -2 else "震荡")
    arrow_dy = -24 if direction == "升温" else (24 if direction == "退潮" else 0)
    color = "#16a34a" if direction == "升温" else ("#ef4444" if direction == "退潮" else "#64748b")
    turn = _turning_candidate(points)

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
    clock.append(f'<text class="value" x="286" y="110">周期走向：{direction} {points[0]["score"]:.0f}→{points[-1]["score"]:.0f}</text>')
    clock.append(f'<text class="value" x="286" y="134">预计拐点：{escape(turn["label"])}</text>')
    clock.append(f'<text class="note" x="286" y="160">圆点=当前相位 · 箭头=周期方向</text>')
    clock.append('</svg>')

    # 右侧五周期热度轨迹；分数是方向坐标，不称为概率/胜率。
    tw, th, top, bottom = 700, 224, 42, 178
    xs = [82, 204, 326, 448, 570][:len(points)]
    def y_of(score):
        return bottom - (float(score) - 20) / 60 * (bottom - top)
    y60, y45 = y_of(60), y_of(45)
    traj = [f'<svg viewBox="0 0 {tw} {th}" role="img" aria-label="{escape(name)}二四六八十六周周期走向">']
    traj.append(f'<rect class="hot" x="54" y="{top}" width="550" height="{y60-top:.0f}"/>')
    traj.append(f'<rect class="warm" x="54" y="{y60:.0f}" width="550" height="{y45-y60:.0f}"/>')
    traj.append(f'<rect class="cold" x="54" y="{y45:.0f}" width="550" height="{bottom-y45:.0f}"/>')
    for label, yy in (("热", (top+y60)/2), ("温", (y60+y45)/2), ("冷", (y45+bottom)/2)):
        traj.append(f'<text class="mut" x="44" y="{yy+4:.0f}" text-anchor="end">{label}</text>')
    seq = []
    for x, point in zip(xs, points):
        y = y_of(point["score"])
        seq.append((x, y, point))
        traj.append(f'<line class="grid" x1="{x}" y1="{top}" x2="{x}" y2="{bottom}"/>')
        traj.append(f'<text class="mut" x="{x}" y="26" text-anchor="middle">{point["label"]}</text>')
    traj.append('<polyline points="' + ' '.join(f'{x},{y:.0f}' for x, y, _ in seq) + f'" fill="none" stroke="{color}" stroke-width="3" stroke-linejoin="round"/>')
    for x, y, point in seq:
        ring = point["label"] == turn.get("horizon")
        if ring:
            traj.append(f'<circle cx="{x}" cy="{y:.0f}" r="11" fill="none" stroke="{color}" stroke-width="2" stroke-dasharray="3 2"/>')
        fill = color if point["confidence"] >= 65 else "var(--card-bg,#fff)"
        traj.append(f'<circle cx="{x}" cy="{y:.0f}" r="6" fill="{fill}" stroke="{color}" stroke-width="2"><title>{point["label"]}·{point["view"]}·置信{point["confidence"]}%（非胜率）·{escape(point["reason"])}</title></circle>')
        traj.append(f'<text class="tiny" x="{x}" y="{min(th-8, y+22):.0f}" text-anchor="middle">{point["view"]}·{point["confidence"]}%</text>')
    traj.append(f'<text class="note" x="54" y="214">横轴=2/4/6/8/16周 · 纵轴=综合方向热度 · 虚线环=预计拐点 · 置信度非胜率</text>')
    traj.append('</svg>')

    # 每档条件完整保留在紧凑文字区，触发/风险均来自同一轮AI复核。
    detail_rows = []
    for p in points:
        detail_rows.append(
            f'<div class="hz-row"><b>{p["label"]} {p["view"]} {p["confidence"]}%</b>'
            f'<span>理由：{escape(p["reason"])}</span><span>触发：{escape(p["catalyst"])}</span>'
            f'<span>风险：{escape(p["risk"])}</span></div>')
    analysis_time = escape(str(review.get("analysis_time") or facts.get("asof") or "时间待更新"))
    model = escape(str(review.get("model") or "量化底稿"))
    review_mode = ("thinking-high" if review.get("status") in ("completed", "cached")
                   else "规则底稿·AI未复核")
    summary = escape(str(review.get("summary") or "五周期量价底稿"))
    action = escape(str(review.get("action") or "观察"))
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
        f'<div class="summary"><b>{summary}</b><span>综合动作：{action}</span><span>失效：{invalid}</span></div>'
        f'<div class="hz-details">{"".join(detail_rows)}</div>'
        f'<div class="foot">图形用于周期与条件复核；方向热度和置信度均不是历史胜率，也不替代盈亏比与仓位纪律。</div>'
        '</div>'
    )
