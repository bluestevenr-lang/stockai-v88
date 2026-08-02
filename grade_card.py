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

# ═══════════════════════════════════════════════════════════════════
# 【-3A 卖出卡】2026-08-02 用户"+3A和-3A都参考这个模式修正"
# 与 card_html 六段严格对称,负半轴同一把尺:
#   ①头行 等级/子类型/同级#/卖出分/门派/周期/行动状态
#   ②三条件条(斯波朗迪1-2-3)+旁路 ← 对称于五桶板
#   ③三独立指标 距止损/一致性/数据 ← 对称于风险/完备/置信
#   ④四问 为什么该卖/为什么这个级别/为什么不是更急/主要短板(触发的那条)
#   ⑤四类条件 卖出区间/升级(转更急)/解除(降级)/**重买条件**(卖出侧独有)
#   ⑥三时点 数据→信号→最早执行(T+1)
# ═══════════════════════════════════════════════════════════════════
SELL_COLOR = {"-3A": "#b91c1c", "-2A": "#dc2626", "-1A": "#ea580c", "0": "#16a34a"}
_C123 = {"c1_trend_break": ("①趋势线跌破", "收盘<MA20"),
         "c2_no_new_high": ("②买方衰竭", "近3日未刷20日高/2B假突破"),
         "c3_low_break": ("③结构破坏", "收盘<前10日低")}


def sell_card_html(r: dict, rebuy: str = "", engine: str = "", compact: bool = False) -> str:
    """单只持仓的 -3A 完整卖出卡(与买入卡同构)。r = sell_grade.json 一行。"""
    lv = str(r.get("level") or "?")
    col = SELL_COLOR.get(lv, "#64748b")
    sc = r.get("sell_score")
    head = (
        f"<div style='display:flex;align-items:baseline;gap:8px;flex-wrap:wrap'>"
        f"<span style='background:{col};color:#fff;border-radius:4px;padding:1px 7px;"
        f"font-size:13px;font-weight:800'>{lv}</span>"
        f"<b style='font-size:14px'>{r.get('name')}</b>"
        f"<span style='font-size:11px;color:#94a3b8'>{r.get('code')}</span>"
        f"<span style='font-size:12px;color:{col};font-weight:700'>{r.get('action')}</span>"
        + (f"<span style='font-size:12px;color:{col}'>卖出分 <b>{sc:.0f}</b></span>"
           if sc is not None else "")
        + f"<span style='font-size:11px;color:#64748b'>门派:{r.get('opp_type') or '未知'}"
        f"·日线级</span></div>")
    if compact:
        return f"<div style='padding:5px 8px;border-left:3px solid {col};margin:3px 0'>{head}</div>"

    # ②三条件条(对称五桶板)
    bars = ""
    for k, (nm, basis) in _C123.items():
        ok = bool(r.get(k))
        c = "#dc2626" if ok else "#94a3b8"
        bars += (f"<div style='font-size:11.5px;color:#334155;margin-top:3px'><b>{nm}</b> "
                 f"<span style='color:#94a3b8'>{basis}</span></div>"
                 f"<div style='display:flex;align-items:center;gap:6px;margin:1px 0'>"
                 f"<span style='width:52px;font-size:11px;color:#475569'>20分</span>"
                 f"<div style='flex:1;height:7px;background:#f1f5f9;border-radius:4px;overflow:hidden'>"
                 f"<div style='width:{100 if ok else 0}%;height:100%;background:{c}'></div></div>"
                 f"<span style='width:56px;text-align:right;font-size:11px;color:{c};"
                 f"font-weight:700'>{'触发' if ok else '未触发'}</span></div>")
    bp = r.get("bypass") or []
    if bp:
        bars += (f"<div style='font-size:11.5px;color:#b91c1c;margin-top:3px'>"
                 f"⚡<b>旁路直达-3A(不等凑齐)</b>: {'；'.join(bp)}</div>")
    for n in (r.get("school_notes") or []):
        bars += f"<div style='font-size:11px;color:#b45309;margin-top:2px'>{n}</div>"

    # ③三独立指标
    d = r.get("dist_stop_pct")
    ind = (f"<div style='font-size:11.5px;margin-top:5px;color:#475569'>"
           f"距止损 <b style='color:{'#b91c1c' if (d is not None and d <= 2) else '#16a34a'}'>"
           f"{f'{d:+.1f}%' if d is not None else '—'}</b>(≤0=已破)　"
           f"现价 <b>{r.get('px')}</b>　MA20 <b>{r.get('ma20')}</b>　"
           f"前10日低 <b>{r.get('prev_low10')}</b>　量比 <b>{r.get('vr') or '—'}</b></div>")

    # ④四问
    cs = "、".join(nm for k, (nm, _) in _C123.items() if r.get(k)) or "无条件触发"
    n = r.get("n123")
    why_lv = f"{lv}：1-2-3 触发 {n}/3（{cs}）" + (f"；旁路{len(bp)}项" if bp else "")
    why_not = ("已是最急档" if lv == "-3A" else
               f"距-3A 还差 {3 - (n or 0)} 个条件（或任一旁路触发）" if lv == "-2A" else
               f"距-2A 还差 {2 - (n or 0)} 个条件")
    cf = r.get("in_out_conflict") or {}
    rows = [("为什么该卖", cs + (f"；{'；'.join(bp)}" if bp else "")),
            ("为什么是这个级别", why_lv),
            ("为什么不是更急", why_not),
            ("主要短板(已破的那条)", cs.split("、")[0] if r.get(k) else "—")]
    if cf:
        rows.append(("⚠️IN/OUT冲突",
                     f"买入侧同时列为 {cf.get('in_tier')}·{cf.get('in_action')}"
                     f"（{cf.get('in_when')}）→ {cf.get('verdict')}"))
    detail = "".join(
        f"<div style='display:flex;gap:6px;font-size:11.5px;margin:1px 0'>"
        f"<span style='width:130px;color:#94a3b8;flex-shrink:0'>{k}</span>"
        f"<span style='color:#334155'>{v}</span></div>" for k, v in rows)

    # ⑤四类条件(含卖出侧独有的重买条件)
    up = ("—" if lv == "-3A" else
          f"再触发1条件→{'-3A' if lv == '-2A' else '-2A'}；或旁路(破止损/高潮放量·限趋势派)")
    tg = [("📉卖出区间", r.get("sell_zone") or "—"),
          ("升级(转更急)", up),
          ("解除(降级)", f"站回MA20({r.get('ma20')})上方且不再创新低 → 逐级降档"),
          ("🔁重买条件", rebuy or "—（缺重买条件=卖出只说一半，须补）")]
    cond = "".join(
        f"<div style='display:flex;gap:6px;font-size:11.5px;margin:1px 0'>"
        f"<span style='width:130px;color:#94a3b8;flex-shrink:0'>{k}</span>"
        f"<span style='color:{'#0891b2' if '重买' in k else '#334155'};"
        f"{'font-weight:600' if '卖出区间' in k else ''}'>{v}</span></div>" for k, v in tg)

    eng = (f"<div style='font-size:11px;color:#64748b;margin-top:3px'>"
           f"引擎卖警对照: {engine or '无'}　·　影子级(攒战绩,不触发交易)</div>")
    return (f"<div style='padding:7px 10px;border:1px solid #fecaca;border-left:4px solid {col};"
            f"border-radius:0 6px 6px 0;margin:6px 0;background:#fff'>"
            f"{head}{bars}{ind}<div style='margin-top:4px'>{detail}</div>"
            f"<div style='margin-top:3px'>{cond}</div>{eng}</div>")
