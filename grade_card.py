"""grade_card.py — 【V88·评级完整输出卡】2026-08-02 用户第十五节全文落地，三端共用渲染。

用户要求每只股票输出：最终等级/子类型/同级排名/同级排序分/机会类型/适用周期/行动状态、
五桶板各自分数与达标状态、风险分/数据完整度/模型置信度、
为什么值得关注/为什么属于这个等级/为什么不是更高等级、缺失桶板/主要短板、
进入触发/升级/降级/失效条件、主要风险。

只用内联 HTML（与 barometer_ui / compare_ui 同一套做法），桌面与云端读同一份 rank_score.json。
"""
from __future__ import annotations

BUCKET_CN = {"long_value": "长期价值", "timing": "当前时机", "catalyst": "机会催化",
             "valuation": "价格估值", "payoff": "获利空间"}
BUCKET_W = {"long_value": 25, "timing": 20, "catalyst": 15, "valuation": 15, "payoff": 25}
TIER_COLOR = {"3A": "#dc2626", "2A": "#ea580c", "1A": "#2563eb",
              "0A": "#94a3b8", "存档": "#64748b"}
# 行动状态统一八种（用户第十五节末）
ACTIONS = ("现在可进", "分批试探", "等待回踩", "等待突破",
           "短线进攻", "低吸埋伏", "继续观察", "风险回避")


def _bar(score: float, ok: bool, w: int) -> str:
    """桶板进度条：达标绿、未达标橙；条长按分数，标签带权重。"""
    c = "#16a34a" if ok else "#ea580c"
    return (f"<div style='display:flex;align-items:center;gap:6px;margin:1px 0'>"
            f"<span style='width:52px;font-size:11px;color:#475569'>{w}%</span>"
            f"<div style='flex:1;height:7px;background:#f1f5f9;border-radius:4px;overflow:hidden'>"
            f"<div style='width:{max(0, min(100, score)):.0f}%;height:100%;background:{c}'></div></div>"
            f"<span style='width:56px;text-align:right;font-size:11px;color:{c};font-weight:700'>"
            f"{score:.0f} {'✓' if ok else '✗'}</span></div>")


def card_html(r: dict, compact: bool = False) -> str:
    """单只股票的完整评级卡。r = rank_score.json 里的一行。"""
    tier = str(r.get("tier") or "")
    col = TIER_COLOR.get(tier, "#64748b")
    head = (
        f"<div style='display:flex;align-items:baseline;gap:8px;flex-wrap:wrap'>"
        f"<span style='background:{col};color:#fff;border-radius:4px;padding:1px 7px;"
        f"font-size:13px;font-weight:800'>{tier}</span>"
        f"<b style='font-size:14px'>{r.get('name')}</b>"
        f"<span style='font-size:11px;color:#94a3b8'>{r.get('code')}</span>"
        f"<span style='font-size:12px;color:{col};font-weight:700'>{r.get('subtype')}</span>"
        f"<span style='font-size:12px;color:#dc2626;font-weight:700'>{r.get('action_state')}</span>"
        f"<span style='font-size:11px;color:#64748b'>同级#{r.get('rank_in_tier', '-')}"
        f"·排序分{r.get('rank_score')}·总#{r.get('rank', '-')}</span>"
        f"<span style='font-size:11px;color:#64748b'>{r.get('opportunity_type')}·"
        f"{r.get('horizon')}</span></div>")
    if compact:
        return f"<div style='padding:5px 8px;border-left:3px solid {col};margin:3px 0'>{head}</div>"

    bks = r.get("buckets") or {}
    bars = "".join(
        f"<div style='font-size:11.5px;color:#334155;margin-top:3px'>"
        f"<b>{BUCKET_CN[k]}</b> "
        f"<span style='color:#94a3b8'>{(bks[k].get('basis') or '')[:56]}</span></div>"
        + _bar(bks[k].get("score", 0), bks[k].get("pass", False), BUCKET_W[k])
        for k in ("long_value", "timing", "catalyst", "valuation", "payoff") if k in bks)

    ind = (f"<div style='font-size:11.5px;margin-top:5px;color:#475569'>"
           f"风险分 <b style='color:{'#dc2626' if (r.get('risk_score') or 0) > 35 else '#16a34a'}'>"
           f"{r.get('risk_score')}</b>(3A限≤35)　"
           f"数据完整度 <b style='color:{'#dc2626' if (r.get('data_completeness') or 0) < 80 else '#16a34a'}'>"
           f"{r.get('data_completeness')}</b>(限≥80)　"
           f"模型置信度 <b>{r.get('model_confidence')}</b></div>")
    risks = r.get("risk_veto") or []
    rf = r.get("risk_flags") or []
    risk_line = ""
    if risks:
        risk_line += (f"<div style='font-size:11.5px;color:#b91c1c;font-weight:600;margin-top:3px'>"
                      f"🚫 风险否决(不可被总分补偿)：{'；'.join(risks)}</div>")
    if rf:
        risk_line += (f"<div style='font-size:11px;color:#b45309'>⚠️ 风险提示：{'、'.join(rf)}</div>")

    tg = r.get("triggers") or {}
    rows = [
        ("当前为什么值得关注", r.get("why_focus") or "—"),
        ("为什么属于这个等级", r.get("why_grade") or "—"),
        ("为什么不是更高等级", r.get("why_not_higher") or "（已是最高级）"),
        ("缺失桶板 / 主要短板", "、".join(r.get("missing") or []) or "无"),
        ("进入触发条件", tg.get("enter") or "—"),
        ("升级条件", tg.get("upgrade") or "—"),
        ("降级条件", tg.get("downgrade") or "—"),
        ("失效条件", tg.get("invalid") or "—"),
    ]
    detail = "".join(
        f"<div style='display:flex;gap:6px;font-size:11.5px;margin:1px 0'>"
        f"<span style='width:118px;color:#94a3b8;flex-shrink:0'>{k}</span>"
        f"<span style='color:#334155'>{v}</span></div>" for k, v in rows)
    ts = (f"<div style='font-size:10.5px;color:#94a3b8;margin-top:4px'>"
          f"数据可得 {r.get('data_available_at') or '?'} · 信号生成 {r.get('signal_generated_at')} · "
          f"最早执行 {r.get('earliest_execution_at')}</div>")
    return (f"<div style='padding:7px 10px;border:1px solid #e2e8f0;border-left:4px solid {col};"
            f"border-radius:0 6px 6px 0;margin:6px 0;background:#fff'>"
            f"{head}{bars}{ind}{risk_line}<div style='margin-top:4px'>{detail}</div>{ts}</div>")


def board_html(data: dict, limit: int = 8, show_detail: int = 3) -> str:
    """行动清单：3A/2A 出完整卡，其余出紧凑行；0A 不进（用户第十节）。"""
    rows = [r for r in (data.get("rows") or []) if str(r.get("tier")) != "0A"][:limit]
    if not rows:
        return ("<div style='font-size:12px;color:#b45309'>今日无 3A/2A/1A 行动标的"
                "（木桶门槛严，不硬凑）</div>")
    out = []
    for i, r in enumerate(rows):
        out.append(card_html(r, compact=(i >= show_detail)))
    n0 = sum(1 for r in (data.get("rows") or []) if str(r.get("tier")) == "0A")
    tail = (f"<div style='font-size:11px;color:#94a3b8;margin-top:4px'>"
            f"另有 {n0} 只判为 0A（无行动价值/风险过高）已移出行动清单；"
            f"权重 长期25/时机20/催化15/估值15/空间25，木桶定级：关键桶板缺一即降级，"
            f"风险否决不可被总分补偿。</div>")
    return "".join(out) + tail


def veto_review_html(data: dict) -> str:
    """待复核否决：被硬闸门拦下但质量分高、或否决前提已变的，必须让人看见来裁。"""
    items = []
    for r in (data.get("rows") or []) + (data.get("archived") or []):
        for v in (r.get("risk_veto") or []):
            if "前提可能已变" in v or r.get("review_needed"):
                items.append((r, v))
                break
    if not items:
        return ""
    body = "".join(
        f"<div style='font-size:12px;color:#b91c1c;margin:2px 0'>⚠️ <b>{r.get('name')}</b>"
        f"（{r.get('code')}）排序分 {r.get('rank_score')} · 五桶"
        f"{sum(1 for b in (r.get('buckets') or {}).values() if b.get('pass'))}/5达标"
        f" → 被拦：{v}</div>" for r, v in items)
    return (f"<div style='border:1px solid #fecaca;background:#fef2f2;border-radius:6px;"
            f"padding:6px 9px;margin:6px 0'>"
            f"<div style='font-size:12.5px;font-weight:700;color:#b91c1c'>🔎 待人工复核的否决"
            f"（{len(items)}只）</div>"
            f"<div style='font-size:11px;color:#64748b;margin-bottom:3px'>"
            f"硬闸门保留（不由系统单方面推翻三方否决），但代价必须看得见——"
            f"否决前提已变或质量分高于中位的，在此列出由你裁定。</div>{body}</div>")
