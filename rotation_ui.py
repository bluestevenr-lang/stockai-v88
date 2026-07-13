"""V88 板块热度与日/周/月轮换思维导图（网页/云端/Lite 共用）。"""
from html import escape
import re


HORIZONS = ("明日", "下周", "半个月")
HORIZON_LABELS = {"明日": "日线·明日", "下周": "周线·下周", "半个月": "月线·半个月"}
MARKET_ICONS = {"美股": "🇺🇸", "A股": "🇨🇳", "港股": "🇭🇰"}
HORIZON_SHORT = {"明日": "明日 (日)", "下周": "下周 (周)", "半个月": "半月 (月)"}
SECTOR_COLORS = ("#3b82f6", "#8b5cf6", "#14b8a6", "#f97316", "#ec4899")

_ROTATION_CSS = """
#__ID__{color:var(--foreground,var(--text-color));margin:.25rem 0 .6rem;
  --rf-hot:#22c55e;--rf-warm:#f59e0b;--rf-cold:#ef4444;--rf-node-bg:var(--background,#fff)}
#__ID__ .rf-meta{display:flex;gap:.7rem;flex-wrap:wrap;color:var(--muted-foreground,var(--text-color));font-size:11px;margin-bottom:.3rem}
#__ID__ .rf-root{width:max-content;max-width:100%;margin:0 auto .5rem;padding:.32rem .8rem;background:color-mix(in srgb,currentColor 9%,transparent);border-radius:7px;text-align:center}
#__ID__ .rf-root small{color:var(--muted-foreground,var(--text-color));margin-left:.4rem}
#__ID__ .rf-card{background:color-mix(in srgb,currentColor 6%,transparent);border-radius:9px;padding:.4rem .6rem .2rem;margin:.42rem 0}
#__ID__ .rf-mkt{display:flex;justify-content:space-between;align-items:baseline;gap:.5rem;font-size:12px;margin-bottom:.2rem}
#__ID__ .rf-mkt span{color:var(--muted-foreground,var(--text-color));font-size:11px}
#__ID__ .rf-heat{height:4px;margin:.15rem 0 .25rem;background:color-mix(in srgb,currentColor 10%,transparent);overflow:hidden;border-radius:4px}
#__ID__ .rf-heat i{display:block;height:100%;background:var(--primary-color,var(--primary,currentColor))}
#__ID__ svg{display:block;width:100%;height:auto;overflow:visible}
#__ID__ svg text{font-size:11px;font-family:inherit}
#__ID__ .b-hot{fill:var(--rf-hot);fill-opacity:.12}
#__ID__ .b-warm{fill:var(--rf-warm);fill-opacity:.12}
#__ID__ .b-cold{fill:var(--rf-cold);fill-opacity:.12}
#__ID__ .grid{stroke:color-mix(in srgb,currentColor 16%,transparent);stroke-width:1}
#__ID__ .axis{stroke:color-mix(in srgb,currentColor 34%,transparent);stroke-width:1}
#__ID__ .ph{fill:var(--muted-foreground,var(--text-color));opacity:.75}
#__ID__ .lbl{fill:currentColor}
#__ID__ .mut{fill:var(--muted-foreground,var(--text-color))}
#__ID__ .rf-warning{font-size:11px;color:var(--destructive,var(--primary-color));margin-top:.3rem}
#__ID__ .rf-foot{font-size:11px;color:var(--muted-foreground,var(--text-color));margin-top:.35rem;display:flex;gap:1rem;flex-wrap:wrap}
@media(max-width:640px){#__ID__ svg text{font-size:10.5px}}
"""


def _phase(strength: float, mom: float) -> str:
    if strength >= 0 and mom >= 0:
        return "领涨启动"
    if strength >= 0:
        return "高位派发"
    if mom >= 0:
        return "低位蓄势"
    return "退潮杀跌"


def _node(x: float, y: float, color: str, conf: str, title: str) -> str:
    tip = f"<title>{escape(title)}</title>"
    if conf == "高":
        return f'<circle cx="{x:.0f}" cy="{y:.0f}" r="5" fill="{color}">{tip}</circle>'
    if conf == "中":
        return f'<circle cx="{x:.0f}" cy="{y:.0f}" r="4.6" fill="{color}" fill-opacity=".82">{tip}</circle>'
    return (f'<circle cx="{x:.0f}" cy="{y:.0f}" r="4.5" fill="var(--rf-node-bg)" '
            f'stroke="{color}" stroke-width="1.7">{tip}</circle>')


def _swimlane_svg(market: str, trajectory: list) -> str:
    if not trajectory:
        return ""
    W, H = 700, 172
    cols = {"明日": 150, "下周": 330, "半个月": 510}
    top, bot = 34, 142
    span = bot - top

    lo, hi = 38.0, 72.0  # 收窄纵轴到真实热度区间，放大板块之间的差异

    def yv(score):
        s = max(lo, min(hi, float(score)))
        return bot - (s - lo) / (hi - lo) * span

    yb1, yb2 = yv(60), yv(48)  # 热≥60 / 温48-60 / 冷<48
    p = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="{escape(market)}板块日周月热度走向泳道">']
    p.append(f'<rect class="b-hot" x="96" y="{top}" width="420" height="{yb1-top:.0f}"/>')
    p.append(f'<rect class="b-warm" x="96" y="{yb1:.0f}" width="420" height="{yb2-yb1:.0f}"/>')
    p.append(f'<rect class="b-cold" x="96" y="{yb2:.0f}" width="420" height="{bot-yb2:.0f}"/>')
    for label, yy in (("热", (top+yb1)/2), ("温", (yb1+yb2)/2), ("冷", (yb2+bot)/2)):
        p.append(f'<text class="ph" x="90" y="{yy+4:.0f}" text-anchor="end">{label}</text>')
    for h, x in cols.items():
        p.append(f'<line class="grid" x1="{x}" y1="{top}" x2="{x}" y2="{bot}"/>')
        p.append(f'<text class="mut" x="{x}" y="22" text-anchor="middle">{HORIZON_SHORT[h]}</text>')
    end_labels = []
    for i, t in enumerate(trajectory):
        color = SECTOR_COLORS[i % len(SECTOR_COLORS)]
        seq = [(cols[h], yv(t["points"][h]["score"]), h) for h in HORIZONS if h in t["points"]]
        if not seq:
            continue
        pts = " ".join(f"{x:.0f},{y:.0f}" for x, y, _ in seq)
        p.append(f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="2" '
                 f'stroke-linejoin="round" stroke-linecap="round"/>')
        for x, y, h in seq:
            pt = t["points"][h]
            title = (f'{t["name"]}·{h}：热度{pt["score"]}/100 · {pt["confidence"]}置信\n'
                     f'触发 {pt.get("trigger","")}｜失效 {pt.get("invalid","")}')
            p.append(_node(x, y, color, pt["confidence"], title))
        end_labels.append([seq[-1][1], seq[-1][0], color, t["name"]])
    # 右端标签防重叠：按 y 排序后强制最小行距
    end_labels.sort()
    min_gap = 14.0
    for j in range(1, len(end_labels)):
        if end_labels[j][0] - end_labels[j-1][0] < min_gap:
            end_labels[j][0] = end_labels[j-1][0] + min_gap
    for ly, lx, color, name in end_labels:
        p.append(f'<text x="{lx+10:.0f}" y="{ly+4:.0f}" fill="{color}">{escape(name)}</text>')
    p.append('</svg>')
    return "".join(p)


def _clock_svg(market: str, trajectory: list, heat: dict) -> str:
    if not trajectory:
        return ""
    W, H = 700, 224
    cx, cy, R = 150, 112, 86
    p = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="{escape(market)}板块轮动时钟与走向">']
    p.append('<defs><marker id="rf-arrow" markerWidth="7" markerHeight="7" refX="5" refY="3" '
             'orient="auto"><path d="M0 0 L6 3 L0 6 Z" fill="context-stroke"/></marker></defs>')
    p.append(f'<circle class="grid" cx="{cx}" cy="{cy}" r="{R}" fill="none"/>')
    p.append(f'<circle class="grid" cx="{cx}" cy="{cy}" r="{R*0.5:.0f}" fill="none" stroke-dasharray="3 4"/>')
    p.append(f'<line class="axis" x1="{cx}" y1="{cy-R}" x2="{cx}" y2="{cy+R}"/>')
    p.append(f'<line class="axis" x1="{cx-R}" y1="{cy}" x2="{cx+R}" y2="{cy}"/>')
    p.append(f'<text class="ph" x="{cx}" y="{cy-R-6}" text-anchor="middle">领涨启动</text>')
    p.append(f'<text class="ph" x="{cx}" y="{cy+R+16}" text-anchor="middle">退潮杀跌</text>')
    p.append(f'<text class="ph" x="{cx+R+6}" y="{cy+4}" text-anchor="start">高位派发</text>')
    p.append(f'<text class="ph" x="{cx-R-6}" y="{cy+4}" text-anchor="end">低位蓄势</text>')
    legend = []
    for i, t in enumerate(trajectory):
        color = SECTOR_COLORS[i % len(SECTOR_COLORS)]
        scores = [t["points"][h]["score"] for h in HORIZONS if h in t["points"]]
        if not scores:
            continue
        avg = sum(scores) / len(scores)
        near = t["points"].get("明日", {}).get("score", avg)
        far = t["points"].get("半个月", {}).get("score", avg)
        strength = max(-1.0, min(1.0, (avg - 50) / 20.0))
        mom = max(-1.0, min(1.0, (far - near) / 12.0))
        x = cx + strength * R * 0.78
        y = cy - mom * R * 0.78
        ay = y - (1 if mom >= 0 else -1) * min(26.0, 11 + abs(mom) * 18)
        p.append(f'<line x1="{x:.0f}" y1="{y:.0f}" x2="{x:.0f}" y2="{ay:.0f}" stroke="{color}" '
                 f'stroke-width="1.6" stroke-dasharray="4 3" marker-end="url(#rf-arrow)"/>')
        p.append(f'<circle cx="{x:.0f}" cy="{y:.0f}" r="5.5" fill="{color}">'
                 f'<title>{escape(t["name"])}</title></circle>')
        phase = _phase(strength, mom)
        ly = 44 + len(legend) * 27
        legend.append(
            f'<rect x="310" y="{ly-9}" width="12" height="12" rx="3" fill="{color}"/>'
            f'<text class="lbl" x="330" y="{ly}">{escape(t["name"])} · {phase}</text>'
            f'<text class="mut" x="330" y="{ly+13}">明日{int(near)} → 半月{int(far)} 热度</text>'
        )
    heat_txt = ""
    if heat:
        hs = heat.get("score")
        heat_txt = f' · 当前热度 {hs}/100 {escape(str(heat.get("label","")))}' if hs is not None else ""
    p.append(f'<text class="lbl" x="310" y="24">{MARKET_ICONS.get(market,"")} {escape(market)}'
             f'<tspan class="mut">{heat_txt}</tspan></text>')
    p.append("".join(legend))
    p.append(f'<text class="mut" x="310" y="{44+len(legend)*27+8:.0f}">'
             f'外圈=一轮周期 · 虚箭头=热度走向（升温↑ / 退潮↓）</text>')
    p.append('</svg>')
    return "".join(p)


def _rich_rotation_html(forecast: dict, element_id: str, focus_market: str) -> str:
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "-", element_id)
    trajectories = forecast.get("trajectories") or {}
    heat_all = forecast.get("market_heat") or {}
    order = [focus_market] + [m for m in ("美股", "A股", "港股") if m != focus_market]
    focus = next((m for m in order if (trajectories.get(m))), None)

    review_status = (forecast.get("strong_review") or {}).get("status")
    review_label = "🧠 最强思考已复核" if review_status == "completed" else "🧠 最强思考待下一轮"
    analysis_time = escape(str(forecast.get("analysis_time") or "未知"))

    clock = _clock_svg(focus, trajectories.get(focus) or [], heat_all.get(focus) or {}) if focus else ""

    cards = []
    for market in ("美股", "A股", "港股"):
        traj = trajectories.get(market) or []
        if not traj:
            continue
        heat = heat_all.get(market) or {}
        hs = heat.get("score")
        try:
            hw = max(0, min(100, int(hs)))
        except (TypeError, ValueError):
            hw = 0
        htext = (f"当前热度 {hs}/100 · {escape(str(heat.get('label','中性')))}"
                 if hs is not None else "热度待更新")
        cards.append(
            '<div class="rf-card">'
            f'<div class="rf-mkt"><b>{MARKET_ICONS[market]} {escape(market)}</b><span>{htext}</span></div>'
            f'<div class="rf-heat" role="img" aria-label="{htext}"><i style="width:{hw}%"></i></div>'
            + _swimlane_svg(market, traj) +
            '</div>'
        )

    warnings = []
    for item in (forecast.get("warnings") or [])[:3]:
        warnings.append(f"{item.get('market')}·{item.get('sector')}：{item.get('reason')}")
    warning_html = (f'<div class="rf-warning">⚠️ {escape("；".join(warnings))}</div>' if warnings else "")

    css = _ROTATION_CSS.replace("__ID__", safe_id)
    clock_block = (f'<div class="rf-card">{clock}</div>' if clock else "")
    return (
        f'<style>{css}</style>'
        f'<div id="{safe_id}" role="figure" aria-label="中美港板块热度轮动时钟与日周月走向泳道">'
        f'<div class="rf-meta"><span>🕒 分析于 {analysis_time}（北京时间）</span>'
        f'<span>{review_label}</span><span>条件成立才升级，失效即撤销</span></div>'
        f'<div class="rf-root"><b>🧠 板块热度 · 轮动时钟与走向</b>'
        f'<small>日线 → 周线 → 月线</small></div>'
        f'{clock_block}{"".join(cards)}{warning_html}'
        f'<div class="rf-foot"><span>● 实心=高置信 ◐ 半实=中 ○ 空心=低</span>'
        f'<span>横轴=明日/下周/半月 · 纵轴热→温→冷</span>'
        f'<span>悬停节点看触发/失效</span></div>'
        f'</div>'
    )


def available_markets(forecast: dict) -> list:
    """有走向数据、可作为时钟焦点的市场列表（保持 美股/A股/港股 顺序）。"""
    traj = (forecast or {}).get("trajectories") or {}
    return [m for m in ("美股", "A股", "港股") if traj.get(m)]


def rotation_map_html(forecast: dict, element_id: str = "v88-rotation-map",
                      focus_market: str = "美股") -> str:
    if not forecast or not forecast.get("markets"):
        return ""
    if forecast.get("trajectories"):
        return _rich_rotation_html(forecast, element_id, focus_market)
    return _legacy_rotation_html(forecast, element_id)


def _legacy_rotation_html(forecast: dict, element_id: str = "v88-rotation-map") -> str:
    if not forecast or not forecast.get("markets"):
        return ""
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "-", element_id)
    reviewed = ((forecast.get("strong_review") or {}).get("focus") or {})
    review_status = (forecast.get("strong_review") or {}).get("status")
    review_label = "🧠 最强思考已复核" if review_status == "completed" else "🧠 最强思考待下一轮"
    branches = []
    for market in ("美股", "A股", "港股"):
        horizons = (forecast.get("markets") or {}).get(market) or {}
        heat = (forecast.get("market_heat") or {}).get(market) or {}
        heat_score = heat.get("score")
        try:
            heat_width = max(0, min(100, int(heat_score)))
        except (TypeError, ValueError):
            heat_width = 0
        heat_text = (f"当前热度 {heat_score}/100 · {heat.get('label', '中性')}"
                     if heat_score is not None else "当前热度待下一轮更新")
        nodes = []
        for horizon in HORIZONS:
            candidates = horizons.get(horizon) or []
            if not candidates:
                nodes.append('<div class="rf-node rf-empty">暂无候选</div>')
                continue
            pick = ((reviewed.get(market) or {}).get(horizon) or candidates[0].get("name"))
            row = next((r for r in candidates if r.get("name") == pick), candidates[0])
            trigger = escape(str(row.get("trigger", "")))
            invalid = escape(str(row.get("invalid", "")))
            nodes.append(
                f'<div class="rf-node" title="触发：{trigger}｜失效：{invalid}">'
                f'<span class="rf-period">{escape(HORIZON_LABELS[horizon])}</span>'
                f'<b>{escape(str(row.get("name", "—")))}</b>'
                f'<span>预测热度 {escape(str(row.get("score", "—")))}/100 · '
                f'{escape(str(row.get("confidence", "—")))}置信</span>'
                f'<small>{escape(str(row.get("reason", "")))}</small>'
                f'<small class="rf-rule">触发：{trigger}</small>'
                f'<small class="rf-rule">失效：{invalid}</small>'
                '</div>'
            )
        branches.append(
            '<section class="rf-branch">'
            f'<div class="rf-market"><b>{MARKET_ICONS[market]} {escape(market)}</b>'
            f'<span>{escape(heat_text)}</span>'
            f'<div class="rf-heat" role="img" aria-label="{escape(heat_text)}">'
            f'<i style="width:{heat_width}%"></i></div></div>'
            '<div class="rf-periods">' + ''.join(nodes) + '</div>'
            '</section>'
        )
    warnings = []
    for item in (forecast.get("warnings") or [])[:3]:
        warnings.append(f"{item.get('market')}·{item.get('sector')}：{item.get('reason')}")
    warning_html = (f'<div class="rf-warning">⚠️ {escape("；".join(warnings))}</div>' if warnings else "")
    analysis_time = escape(str(forecast.get("analysis_time") or "未知"))
    return f'''<style>
#{safe_id}{{color:var(--foreground,var(--text-color));margin:.25rem 0 .55rem}}
#{safe_id} .rf-meta{{display:flex;gap:.75rem;flex-wrap:wrap;color:var(--muted-foreground,var(--text-color));font-size:11px;margin-bottom:.35rem}}
#{safe_id} .rf-root{{width:max-content;max-width:100%;margin:0 auto .65rem;padding:.35rem .8rem;background:color-mix(in srgb,currentColor 9%,transparent);border-radius:7px;text-align:center}}
#{safe_id} .rf-tree{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:.65rem;position:relative}}
#{safe_id} .rf-branch{{min-width:0;position:relative}}
#{safe_id} .rf-branch:before{{content:"";display:block;width:1px;height:.45rem;margin:-.65rem auto .2rem;background:color-mix(in srgb,currentColor 25%,transparent)}}
#{safe_id} .rf-market,#{safe_id} .rf-node{{background:color-mix(in srgb,currentColor 7%,transparent);color:inherit;padding:.4rem .5rem;border-radius:7px;display:flex;flex-direction:column;justify-content:center;min-width:0}}
#{safe_id} .rf-market{{text-align:center;margin-bottom:.35rem}}
#{safe_id} .rf-market span{{font-size:11px;color:var(--muted-foreground,var(--text-color))}}
#{safe_id} .rf-heat{{height:4px;margin-top:.3rem;background:color-mix(in srgb,currentColor 10%,transparent);overflow:hidden;border-radius:4px}}
#{safe_id} .rf-heat i{{display:block;height:100%;background:var(--primary-color,var(--primary,currentColor))}}
#{safe_id} .rf-periods{{display:grid;gap:.3rem;padding-left:.55rem;border-left:1px solid color-mix(in srgb,currentColor 22%,transparent)}}
#{safe_id} .rf-node{{background:color-mix(in srgb,currentColor 4%,transparent)}}
#{safe_id} .rf-node b{{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
#{safe_id} small,#{safe_id} .rf-node span{{font-size:11px;color:var(--muted-foreground,var(--text-color));white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
#{safe_id} .rf-rule{{font-size:11px}}
#{safe_id} .rf-period{{color:var(--foreground,var(--text-color))!important}}
#{safe_id} .rf-warning{{font-size:11px;color:var(--destructive,var(--primary-color));margin-top:.35rem}}
@media(max-width:700px){{#{safe_id} .rf-tree{{grid-template-columns:1fr}}#{safe_id} .rf-branch:before{{display:none}}}}
</style>
<div id="{safe_id}" role="figure" aria-label="中美港板块热度与日周月轮换思维导图">
  <div class="rf-meta"><span>🕒 分析于 {analysis_time}（北京时间）</span><span>{review_label}</span><span>条件成立才升级，失效即撤销</span></div>
  <div class="rf-root"><b>🧠 中美港板块热度与周期预测</b><small>日线 → 周线 → 月线</small></div>
  <div class="rf-tree">{''.join(branches)}</div>
  {warning_html}
</div>'''
