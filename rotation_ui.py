"""V88 下一轮板块轮转导图（网页/云端/Lite 共用）。"""
from html import escape
import re


HORIZONS = ("明日", "下周", "半个月")


def rotation_map_html(forecast: dict, element_id: str = "v88-rotation-map") -> str:
    if not forecast or not forecast.get("markets"):
        return ""
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "-", element_id)
    reviewed = ((forecast.get("strong_review") or {}).get("focus") or {})
    review_status = (forecast.get("strong_review") or {}).get("status")
    review_label = "🧠 最强思考已复核" if review_status == "completed" else "🧠 最强思考待下一轮"
    rows = []
    for market in ("美股", "A股", "港股"):
        horizons = (forecast.get("markets") or {}).get(market) or {}
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
                f'<span class="rf-period">{escape(horizon)}</span>'
                f'<b>{escape(str(row.get("name", "—")))}</b>'
                f'<span>{escape(str(row.get("score", "—")))}·{escape(str(row.get("confidence", "—")))}置信</span>'
                f'<small>{escape(str(row.get("reason", "")))}</small>'
                f'<small class="rf-rule">触发：{trigger}</small>'
                f'<small class="rf-rule">失效：{invalid}</small>'
                '</div>'
            )
        rows.append(
            '<div class="rf-lane">'
            f'<div class="rf-market"><b>{escape(market)}</b><small>当前量化状态</small></div>'
            '<span class="rf-arrow">→</span>' + nodes[0]
            + '<span class="rf-arrow">→</span>' + nodes[1]
            + '<span class="rf-arrow">→</span>' + nodes[2]
            + '</div>'
        )
    warnings = []
    for item in (forecast.get("warnings") or [])[:3]:
        warnings.append(f"{item.get('market')}·{item.get('sector')}：{item.get('reason')}")
    warning_html = (f'<div class="rf-warning">⚠️ {escape("；".join(warnings))}</div>' if warnings else "")
    analysis_time = escape(str(forecast.get("analysis_time") or "未知"))
    return f'''<style>
#{safe_id}{{color:var(--foreground,var(--text-color));margin:.35rem 0 .6rem}}
#{safe_id} .rf-meta{{display:flex;gap:.75rem;flex-wrap:wrap;color:var(--muted-foreground,var(--text-color));font-size:11px;margin-bottom:.35rem}}
#{safe_id} .rf-lane{{display:grid;grid-template-columns:minmax(82px,.7fr) 18px minmax(120px,1fr) 18px minmax(120px,1fr) 18px minmax(120px,1fr);align-items:stretch;gap:.25rem;margin:.3rem 0}}
#{safe_id} .rf-market,#{safe_id} .rf-node{{background:color-mix(in srgb,currentColor 7%,transparent);color:inherit;padding:.45rem .55rem;border-radius:7px;display:flex;flex-direction:column;justify-content:center;min-width:0}}
#{safe_id} .rf-node{{background:color-mix(in srgb,currentColor 4%,transparent)}}
#{safe_id} .rf-node b{{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
#{safe_id} small,#{safe_id} .rf-node span{{font-size:11px;color:var(--muted-foreground,var(--text-color));white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
#{safe_id} .rf-rule{{font-size:11px}}
#{safe_id} .rf-period{{color:var(--foreground,var(--text-color))!important}}
#{safe_id} .rf-arrow{{display:flex;align-items:center;justify-content:center;color:var(--muted-foreground,var(--text-color))}}
#{safe_id} .rf-warning{{font-size:11px;color:var(--destructive,var(--primary-color));margin-top:.35rem}}
@media(max-width:700px){{#{safe_id} .rf-lane{{grid-template-columns:1fr}}#{safe_id} .rf-arrow{{transform:rotate(90deg);height:12px}}}}
</style>
<div id="{safe_id}" role="figure" aria-label="中美港下一轮板块轮转导图">
  <div class="rf-meta"><span>🕒 分析于 {analysis_time}（北京时间）</span><span>{review_label}</span><span>条件成立才升级，失效即撤销</span></div>
  {''.join(rows)}
  {warning_html}
</div>'''
