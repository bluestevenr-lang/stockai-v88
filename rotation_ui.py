"""V88 板块热度与日/周/月轮换思维导图（网页/云端/Lite 共用）。"""
from html import escape
import re


HORIZONS = ("明日", "下周", "半个月")
HORIZON_LABELS = {"明日": "日线·明日", "下周": "周线·下周", "半个月": "月线·半个月"}
MARKET_ICONS = {"美股": "🇺🇸", "A股": "🇨🇳", "港股": "🇭🇰"}


def rotation_map_html(forecast: dict, element_id: str = "v88-rotation-map") -> str:
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
