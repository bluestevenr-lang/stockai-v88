"""Compact, read-only presentation of the shared next-session evidence model.

Pass the returned document to ``st.html``: it is complete HTML, not Markdown.
The model owns signal priority and decisions; this module only groups its rows.
"""
from html import escape
import math

import stock_profile_view


_SECTOR = {
    "aligned": ("aligned", "✓ 板块同向"),
    "opposed": ("opposed", "↔ 板块反向"),
    "mixed": ("mixed", "± 板块分歧"),
    "missing": ("missing", "○ 板块待核"),
}
_DIRECTION = {"down": ("↓", "转弱预警"), "up": ("↗", "转强观察"), "mixed": ("±", "分歧待核")}


def _e(value, fallback=""):
    return escape(str(fallback if value is None or value == "" else value), quote=True)


def _score(value):
    return f"{value:g}/100" if type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 100 else "待核"


def _count(value):
    return str(value) if type(value) is int and value >= 0 else "—"


def _plan_value(value):
    if isinstance(value, (list, tuple)):
        return " ～ ".join(str(v) for v in value) if value else "待核"
    return "待核" if value is None or value == "" else str(value)


def _conditions(row, profiles, *, peers):
    source_status = {"verified": "已核验", "current": "已核验", "missing": "待补当期行情"}.get(
        row.get("source_status"), row.get("source_status") or "待核验")
    parts = [
        ("中央动作", row.get("central_action") or "尚无有效中央结论，等待复核"),
        ("技术触发", row.get("trigger") or "待补齐技术触发条件"),
        ("技术反证", row.get("invalid") or "待核对技术反证条件"),
        ("板块依据", row.get("sector_detail") or "暂无可核验的同口径板块证据"),
        ("数据截至", row.get("source_date") or "未提供"),
        ("数据状态", source_status),
    ]
    plan = row.get("central_plan")
    if row.get("central_current") is True and isinstance(plan, dict) and plan:
        parts[1:1] = [("中央原入场", _plan_value(plan.get("entry_range"))),
                      ("中央原止盈", _plan_value(plan.get("take_profit_range"))),
                      ("中央原失效", _plan_value(plan.get("stop")))]
        ref = row.get("central_ref") or {}
        factpack = ref.get("factpack_id") if isinstance(ref, dict) else None
        if factpack:
            full = str(factpack)
            parts.append(("中央凭据", full[:20] + ("…" if len(full) > 20 else "")))
    body = "".join(f'<dt>{_e(label)}</dt><dd>{_e(value)}</dd>' for label, value in parts)
    peer_html = stock_profile_view.industry_rank_html(row.get("code"), profiles, compact=False) if peers else ""
    if not peer_html and peers:
        peer_html = '<div class="ns-subtle">○ 行业排名及同业前十待核验</div>'
    return ('<details class="ns-evidence"><summary>⌕ 条件与关联依据</summary>'
            f'<dl>{body}</dl>{peer_html}</details>')


def _card(row, profiles, *, compact=False):
    direction = row.get("direction") if row.get("direction") in _DIRECTION else "mixed"
    symbol, _ = _DIRECTION[direction]
    state, sector_text = _SECTOR.get(row.get("sector_state"), _SECTOR["missing"])
    code = row.get("code") or ""
    link = stock_profile_view.link_html(row.get("name"), code, profiles)
    current = row.get("central_current") is True
    tier = _e(row.get("central_tier") or "未评级")
    review = _score(row.get("central_score"))
    central = f"中央 {tier} · {review}" if current else _e(row.get("central_action"), "中央版本待核")
    industry = row.get("industry") or "行业待核"
    sector_label = row.get("sector_label")
    sector = f'{sector_text} · {_e(sector_label)}' if sector_label else sector_text
    return (f'<article class="ns-stock ns-{direction}{" ns-compact" if compact else ""}" data-stock-code="{_e(code)}" '
            f'data-direction="{direction}"><div class="ns-stock-title"><span aria-hidden="true">{symbol}</span> {link}</div>'
            f'<div class="ns-industry">{_e(industry)}</div>'
            f'<div class="ns-signal"><span>个股：{_e(row.get("phase"), "规则状态待核")}</span>'
            f'<span class="ns-rule-score">规则强度 {_score(row.get("rule_score"))}</span></div>'
            '<div class="ns-linkage">'
            f'<span class="ns-badge ns-sector-{state}">{sector}</span>'
            '<span class="ns-link-arrow" aria-hidden="true">→</span>'
            f'<span class="ns-badge {"ns-central" if current else "ns-pending"}">{central}</span></div>'
            f'<p class="ns-action">{_e(row.get("next_action"), "等待原条件与中央复核")}</p>'
            + _conditions(row, profiles, peers=not compact) + '</article>')


def _group(rows, direction, profiles):
    selected = [row for row in rows if row.get("direction") == direction]
    symbol, title = _DIRECTION[direction]
    head = f'<h4 class="ns-group-title ns-text-{direction}">{symbol} {title}<span>{len(selected)}</span></h4>'
    if not selected:
        return head + '<div class="ns-empty">本次无有效信号</div>'
    visible = "".join(_card(row, profiles) for row in selected[:2])
    rest = selected[2:]
    if rest:
        visible += (f'<details class="ns-more"><summary>＋ 其余{len(rest)}只{title}</summary>'
                    + "".join(_card(row, profiles, compact=True) for row in rest) + '</details>')
    return head + visible


_STYLE = """
<style>
#v88-next-session-board{font-family:inherit;color:#18324d;line-height:1.45;margin:12px 0 20px;max-width:100%;overflow-wrap:anywhere}
#v88-next-session-board *{box-sizing:border-box}
#v88-next-session-board .ns-heading{display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;margin-bottom:5px}
#v88-next-session-board h3{font-size:19px;line-height:1.4;margin:0;font-weight:750}
#v88-next-session-board .ns-meta,#v88-next-session-board .ns-subtle{font-size:11px;color:#65778d}
#v88-next-session-board .ns-intro{font-size:12px;color:#52687f;margin:4px 0 10px}
#v88-next-session-board .ns-flow{color:#24628c;font-weight:650}
#v88-next-session-board .ns-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}
#v88-next-session-board .ns-market{background:#f5f8fc;border:1px solid #dbe5f0;border-radius:12px;padding:13px;min-width:0}
#v88-next-session-board .ns-market-header{display:flex;justify-content:space-between;gap:8px;align-items:center}
#v88-next-session-board .ns-market-header h4{font-size:16px;margin:0;font-weight:750}
#v88-next-session-board .ns-session{font-size:11px;color:#536e89}
#v88-next-session-board .ns-market-summary{font-size:11px;color:#62778e;margin:4px 0 10px}
#v88-next-session-board .ns-group-title{font-size:13px;display:flex;justify-content:space-between;align-items:center;margin:12px 0 7px}
#v88-next-session-board .ns-group-title span{font-size:11px;background:#fff;border:1px solid #dde6ee;border-radius:10px;padding:1px 7px}
#v88-next-session-board .ns-text-down{color:#a85c12}
#v88-next-session-board .ns-text-up{color:#16814d}
#v88-next-session-board .ns-stock{background:#fff;border:1px solid #dde6ef;border-left:3px solid #94a3b8;border-radius:8px;padding:10px;margin:7px 0;min-width:0}
#v88-next-session-board .ns-stock.ns-down{border-left-color:#d99026}
#v88-next-session-board .ns-stock.ns-up{border-left-color:#20a067}
#v88-next-session-board .ns-stock-title{font-size:13px;font-weight:700;line-height:1.45}
#v88-next-session-board .ns-stock-title a{color:#183d66!important}
#v88-next-session-board a:focus-visible,#v88-next-session-board summary:focus-visible{outline:2px solid #357cec;outline-offset:3px;border-radius:3px}
#v88-next-session-board .ns-industry{font-size:11px;color:#708297;margin:3px 0 7px}
#v88-next-session-board .ns-signal{font-size:11px;color:#51667d;display:flex;justify-content:space-between;gap:6px;flex-wrap:wrap;margin-bottom:6px}
#v88-next-session-board .ns-rule-score{white-space:nowrap;color:#718299}
#v88-next-session-board .ns-linkage{display:flex;align-items:center;gap:4px;flex-wrap:wrap}
#v88-next-session-board .ns-badge{font-size:10px;line-height:1.5;padding:2px 5px;border-radius:4px;background:#eef2f7;color:#53657b}
#v88-next-session-board .ns-link-arrow{font-size:11px;color:#7d8da3}
#v88-next-session-board .ns-sector-aligned{background:#e8f7ef;color:#14794c}
#v88-next-session-board .ns-sector-opposed,#v88-next-session-board .ns-sector-mixed{background:#fff0db;color:#9b560c}
#v88-next-session-board .ns-central{background:#edf3ff;color:#335ba0}
#v88-next-session-board .ns-pending{background:#f0f2f5;color:#6b7280}
#v88-next-session-board .ns-action{font-size:12px;font-weight:600;color:#354f6c;line-height:1.5;margin:8px 0 5px}
#v88-next-session-board details{font-size:11px;margin:6px 0 0}
#v88-next-session-board summary{cursor:pointer;color:#426784;line-height:1.5}
#v88-next-session-board details[open]>summary{margin-bottom:6px}
#v88-next-session-board .ns-evidence dl{display:grid;grid-template-columns:55px minmax(0,1fr);gap:5px 7px;margin:7px 0 9px;font-size:10px;line-height:1.5}
#v88-next-session-board dt{color:#748397}
#v88-next-session-board dd{margin:0;color:#53677c;white-space:pre-wrap}
#v88-next-session-board .ns-empty{font-size:11px;color:#8190a2;padding:7px 0}
#v88-next-session-board .ns-more{padding:3px 1px}
#v88-next-session-board .ns-mixed{margin-top:10px}
#v88-next-session-board .ns-notes{padding:8px 11px;background:#f8fafc;border-radius:7px;border:1px solid #e5ebf2;margin-top:10px;color:#65778d}
#v88-next-session-board .ns-notes ul{margin:5px 0;padding-left:18px;font-size:10px}
#v88-next-session-board .ns-notes p{margin:4px 0;font-size:10px}
@media(max-width:1000px){#v88-next-session-board .ns-grid{grid-template-columns:1fr}#v88-next-session-board .ns-market{padding:11px}#v88-next-session-board h3{font-size:17px}}
</style>
"""


def render(doc, profiles=None):
    """Render shared rows without making calls, reranking, or granting actions."""
    profiles = stock_profile_view.load() if profiles is None else profiles
    markets = []
    for market in doc.get("markets") or []:
        rows = market.get("rows") or []
        mixed = [row for row in rows if row.get("direction") not in ("up", "down")]
        counts = market.get("counts") or {}
        flags = {"A股": "🇨🇳", "美股": "🇺🇸", "港股": "🇭🇰"}
        market_name = market.get("market") or "市场待核"
        panel = (f'<section class="ns-market" data-market="{_e(market_name)}">'
                 f'<div class="ns-market-header"><h4>{flags.get(market_name, "○")} {_e(market_name)}</h4>'
                 f'<span class="ns-session">关注交易日 {_e(market.get("next_session"), "待核")} · {_e(market.get("market_status"), "待核")}</span></div>'
                 f'<div class="ns-market-summary">↓ {_count(counts.get("down", 0))} · ↗ {_count(counts.get("up", 0))} · ± {_count(counts.get("mixed", 0))}'
                 f' <span>｜完整日线 {_e(market.get("source_date"), "待核")}</span></div>'
                 + _group(rows, "down", profiles) + _group(rows, "up", profiles))
        if mixed:
            panel += (f'<details class="ns-mixed"><summary>± 分歧或待核信号 {len(mixed)}只</summary>'
                      + "".join(_card(row, profiles, compact=True) for row in mixed) + '</details>')
        markets.append(panel + '</section>')
    notes = "".join(f'<li>{_e(note)}</li>' for note in doc.get("notes") or [])
    return (_STYLE + '<section id="v88-next-session-board" aria-label="下一交易日联动观察">'
            '<div class="ns-heading"><h3>◷ 当前交易时段 · 联动观察</h3>'
            f'<span class="ns-meta">观察池 {_count(doc.get("pool_size"))}只 · 信号 {_count(doc.get("signal_count"))}只 · 本地复核 {_count(doc.get("verified_count"))}只</span></div>'
            '<p class="ns-intro"><span class="ns-flow">个股信号 → 板块核验 → 中央结论</span> · '
            '先看中央已评级标的，再按规则强度；交易动作以中央原条件为准。</p>'
            '<div class="ns-grid">' + "".join(markets) + '</div>'
            '<details class="ns-notes"><summary>ⓘ 数据时间与观察口径</summary>'
            f'<p>汇总 {_e(doc.get("generated_at"), "未提供")} · 中央版本 {_e(doc.get("central_version"), "未提供")}</p>'
            f'<p>本地已复核 {_count(doc.get("verified_count"))}只 · 复核时间 {_e(doc.get("verified_at"), "未提供")}</p>'
            '<p>规则强度是信号量表，不是涨跌概率、胜率或中央审核分；板块同向也不直接授予开仓许可。</p>'
            f'<ul>{notes}</ul></details></section>')
