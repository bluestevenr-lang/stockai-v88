"""Pure local rendering of conditional future paths, without trade authority.

The producer owns the scenarios.  This module never extrapolates prices, fills
missing evidence or converts qualitative coordinates into probabilities.
"""
from datetime import date, datetime, timedelta
from hashlib import sha256
from html import escape
import math


WEEKS = (0, 2, 4, 8, 16, 32, 52)
COLORS = {"base": "#2563eb", "up": "#159447", "down": "#d45555"}
CSS = """<style>
.v88-future{color:#17243b;font:13px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;max-width:100%;box-sizing:border-box;margin:10px 0}
.v88-future *{box-sizing:border-box}.v88-future h3{font-size:16px;margin:0 0 3px;font-weight:700;line-height:1.5}
.v88-future .vf-meta,.v88-future .vf-note{font-size:11px;color:#718096;line-height:1.55;margin:3px 0 7px}
.v88-future .vf-headline{border-left:3px solid #3b82f6;background:#eff6ff;border-radius:0 7px 7px 0;padding:7px 10px;font-size:13px;margin:8px 0}
.v88-future .vf-grid{display:grid;grid-template-columns:minmax(235px,27%) minmax(0,1fr);gap:12px;align-items:stretch}
.v88-future .vf-panel{border:1px solid #dce4ef;background:#fbfcff;border-radius:11px;padding:10px;min-width:0}
.v88-future .vf-panel-title{font-size:12px;font-weight:700;display:flex;justify-content:space-between;gap:8px;margin:0 3px 2px}
.v88-future .vf-panel-title small{font-size:10px;color:#718096;font-weight:400}.v88-future .vf-phase svg{width:100%;max-height:265px;display:block}
.v88-future .vf-phase-summary{text-align:center;font-size:12px;margin-top:2px}.v88-future .vf-scroll{overflow-x:auto;max-width:100%;overscroll-behavior-x:contain}
.v88-future .vf-path-svg{display:block;min-width:760px;width:100%;height:auto}
.v88-future .vf-legend{display:flex;gap:12px;flex-wrap:wrap;font-size:11px;color:#52637e;margin:1px 4px 0}
.v88-future .vf-legend i{width:21px;display:inline-block;border-top:2px dashed;vertical-align:middle;margin:0 4px 3px 0}.v88-future .vf-legend .vf-band-key{border:0;background:#647ab41a;height:9px;margin-bottom:1px}
.v88-future details{border:1px solid #e2e8f0;border-radius:7px;margin-top:8px;background:#fff;padding:5px 9px}
.v88-future summary{cursor:pointer;font-size:11px;color:#52637e;line-height:1.7}.v88-future .vf-conditions{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:7px;margin:8px 0}
.v88-future .vf-condition{font-size:11px;padding:7px 9px;background:#f8fafc;border-radius:6px;overflow-wrap:anywhere}.v88-future .vf-condition b{font-size:12px;display:block;color:#344763}.v88-future .vf-condition span{display:block;margin-top:3px}
.v88-future .vf-missing{padding:26px 12px;background:#f8fafc;border:1px dashed #cbd5e1;border-radius:8px;color:#718096;font-size:12px;text-align:center;margin:9px 0}
.v88-future .vf-assumptions{font-size:11px;color:#718096;padding-left:18px;margin:7px 0}.v88-future .vf-gap{color:#b45309;font-size:11px;margin:5px 4px}
.v88-future.vf-compact h3{font-size:14px}.v88-future.vf-compact .vf-phase svg{max-height:230px}
@media(max-width:900px){.v88-future .vf-grid{grid-template-columns:1fr}.v88-future .vf-phase svg{max-height:225px}.v88-future .vf-phase-summary{margin-top:-3px}.v88-future .vf-panel{padding:8px}.v88-future .vf-meta{overflow-wrap:anywhere}}
</style>"""


def _e(value):
    return escape(str(value if value is not None else ""), quote=True)


def _number(value):
    return type(value) in (int, float) and math.isfinite(value) and -2 <= value <= 2


def _date(value):
    if isinstance(value, (date, datetime)):
        return value.date().isoformat() if isinstance(value, datetime) else value.isoformat()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10]).isoformat()
        except ValueError:
            pass
    return "日期待核"


def _direction(value):
    if not _number(value):
        return "待证"
    return "偏强" if value > 0.35 else "偏弱" if value < -0.35 else "震荡 / 待确认"


def _normalize(doc):
    rows = {}
    duplicate = set()
    for row in doc.get("points") or []:
        if not isinstance(row, dict):
            continue
        week = row.get("weeks")
        if type(week) not in (int, float) or week not in WEEKS:
            continue
        if week in rows:
            duplicate.add(week)
        rows[week] = row
    source_date = _date(doc.get("anchor_date") or doc.get("source_asof"))
    result = []
    for week in WEEKS:
        row = rows.get(week) if week not in duplicate else None
        # A future week must have the matching actual date; never relabel an
        # inconsistent producer point to make it appear temporally coherent.
        if row and source_date != "日期待核":
            expected = (date.fromisoformat(source_date)+timedelta(weeks=week)).isoformat()
            if _date(row.get("date")) != expected:
                row = None
        result.append(row)
    return result


def _curve(coords):
    """Endpoint-preserving smooth segments: no overshoot or implied prices."""
    if not coords:
        return ""
    result = [f"M {coords[0][0]:.2f} {coords[0][1]:.2f}"]
    for (x0, y0), (x1, y1) in zip(coords, coords[1:]):
        mid = (x0 + x1) / 2
        result.append(f"C {mid:.2f} {y0:.2f} {mid:.2f} {y1:.2f} {x1:.2f} {y1:.2f}")
    return " ".join(result)


def _segments(rows, fields):
    current = []
    for week, row in zip(WEEKS, rows):
        valid = row and all(_number(row.get(field)) for field in fields)
        if valid and (len(fields) > 1 or fields[0] in ("up", "down")):
            valid = (all(_number(row.get(k)) for k in ("base", "up", "down"))
                     and row["down"] <= row["base"] <= row["up"])
        if valid:
            current.append((week, row))
        else:
            if current:
                yield current
            current = []
    if current:
        yield current


def _phase(doc, uid):
    phase = doc.get("phase") if isinstance(doc.get("phase"), dict) else {}
    ready = doc.get("status") == "ready" and all(_number(phase.get(k)) for k in ("x", "y"))
    s = [f'<svg viewBox="0 0 310 264" role="img" aria-labelledby="{uid}-phase-title">',
         f'<title id="{uid}-phase-title">当前周期相位与条件方向；不是买卖信号</title>',
         '<circle cx="155" cy="128" r="99" fill="#f8fafc" stroke="#cbd5e1" stroke-width="1.5"/>',
         '<circle cx="155" cy="128" r="57" fill="none" stroke="#dbe4ee" stroke-dasharray="3 5"/>',
         '<path d="M56 128H254 M155 29V227" stroke="#b9c6d8"/>',
         '<text x="155" y="16" text-anchor="middle" font-size="12" fill="#40536f">领涨启动</text>',
         '<text x="155" y="250" text-anchor="middle" font-size="12" fill="#40536f">退潮调整</text>',
         '<text x="51" y="116" text-anchor="end" font-size="11" fill="#40536f">低位</text>',
         '<text x="51" y="133" text-anchor="end" font-size="11" fill="#40536f">蓄势</text>',
         '<text x="259" y="116" font-size="11" fill="#40536f">高位</text>',
         '<text x="259" y="133" font-size="11" fill="#40536f">震荡</text>']
    if ready:
        x, y = phase["x"], phase["y"]
        ox, oy = 60*x, -60*y
        radial = math.hypot(ox, oy)
        if radial > 74:
            ox, oy = ox*74/radial, oy*74/radial
        px, py = 155+ox, 128+oy
        color = "#159447" if y > 0.35 else "#d45555" if y < -0.35 else "#2563eb"
        if all(_number(phase.get(k)) for k in ("dx", "dy")):
            arrow_color = "#159447" if phase["dy"] > 0.05 else "#d45555" if phase["dy"] < -0.05 else "#2563eb"
            vx, vy = phase["dx"], -phase["dy"]
            length = math.hypot(vx, vy)
            # Arrow encodes direction only.  A minimum glyph length keeps a
            # small but explicit conditional direction legible beside the dot.
            glyph = max(25, min(43, length*45)) if length else 0
            tx, ty = ox+(vx/length*glyph if length else 0), oy+(vy/length*glyph if length else 0)
            size = math.hypot(tx, ty)
            if size > 88:
                tx, ty = tx*88/size, ty*88/size
            tx, ty = tx+155, ty+128
            if math.hypot(tx-px, ty-py) >= 3:
                s += [f'<defs><marker id="{uid}-arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto" markerUnits="strokeWidth"><path d="M0 0L6 3L0 6Z" fill="{arrow_color}"/></marker></defs>',
                      f'<path class="vf-phase-arrow" d="M{px:.2f} {py:.2f} L{tx:.2f} {ty:.2f}" stroke="{arrow_color}" stroke-width="2.7" stroke-dasharray="4 3" marker-end="url(#{uid}-arrow)" fill="none"><title>箭头表示近期条件成立时的方向；未成立则重新评估</title></path>']
        s.append(f'<circle class="vf-current-phase" cx="{px:.2f}" cy="{py:.2f}" r="7" fill="{color}" stroke="white" stroke-width="2.5"><title>{_e(phase.get("label"))} · 当前相位</title></circle>')
    else:
        s.append('<text x="155" y="121" text-anchor="middle" font-size="13" fill="#718096">○ 相位待证</text><text x="155" y="142" text-anchor="middle" font-size="11" fill="#718096">不补造方向</text>')
    s.append('</svg>')
    caption = _e(phase.get("label") or "当前相位待证") if ready else "○ 当前相位待证"
    return '<div class="vf-panel vf-phase"><div class="vf-panel-title">◉ 当前相位 <small>圆点：当前 · 虚箭头：条件方向</small></div>'+''.join(s)+f'<div class="vf-phase-summary">{caption}</div></div>'


def _paths(doc, uid, rows):
    if doc.get("status") != "ready":
        return '<div class="vf-panel"><div class="vf-panel-title">↗ 未来一年 · 条件路径</div><div class="vf-missing">○ 未来路径等待证据补齐<br>保留当前事实，不补造预测曲线</div></div>'
    # Real elapsed weeks; early observation labels use staggered date rows.
    x = lambda week: 79 + 766*week/52
    y = lambda value: 143 - 47*value
    s = [f'<svg class="vf-path-svg" viewBox="0 0 900 352" role="img" aria-labelledby="{uid}-path-title {uid}-path-desc">',
         f'<title id="{uid}-path-title">未来一年条件情景曲线</title>',
         f'<desc id="{uid}-path-desc">横轴是从当前到五十二周的真实时间。纵轴是定性趋势强弱，不是价格、涨幅或胜率。蓝色虚线表示基准条件路径，绿色和红色为上下行情景，淡色区域是情景范围而非置信区间。</desc>',
         '<rect x="79" y="37" width="766" height="70" fill="#eef8f1"/><rect x="79" y="107" width="766" height="72" fill="#f6f8fc"/><rect x="79" y="179" width="766" height="70" fill="#fff2f2"/>']
    for value, label in ((2, "偏强"), (0, "震荡"), (-2, "偏弱")):
        s.append(f'<line x1="79" x2="845" y1="{y(value)}" y2="{y(value)}" stroke="#d8e1ed"/><text x="61" y="{y(value)+4}" text-anchor="end" font-size="12" fill="#6b7c94">{label}</text>')
    s.append('<text x="79" y="24" font-size="10" fill="#718096">定性强弱 · 非价格轴</text>')
    for segment in _segments(rows, ("base", "up", "down")):
        if len(segment) < 2:
            continue
        polygon = [(x(w), y(r["up"])) for w, r in segment] + [(x(w), y(r["down"])) for w, r in reversed(segment)]
        s.append('<polygon class="vf-scenario-band" points="'+" ".join(f"{a:.2f},{b:.2f}" for a,b in polygon)+'" fill="#647ab4" fill-opacity=".075"><title>条件情景范围；不是置信区间</title></polygon>')
    for field in ("up", "down", "base"):
        for segment in _segments(rows, (field,)):
            if len(segment) < 2:
                continue
            coords = [(x(w), y(r[field])) for w, r in segment]
            weight, opacity, dash = (3.1, 1, "7 5") if field == "base" else (1.65, .62, "3 5")
            s.append(f'<path class="vf-{field}-path" d="{_curve(coords)}" fill="none" stroke="{COLORS[field]}" stroke-width="{weight}" opacity="{opacity}" stroke-dasharray="{dash}"/>')
    gaps = []
    for index, (week, row) in enumerate(zip(WEEKS, rows)):
        px = x(week)
        s.append(f'<line x1="{px:.2f}" x2="{px:.2f}" y1="249" y2="255" stroke="#aebed2"/>')
        label = "起点" if week == 0 else f"{week}周"
        s.append(f'<text x="{px:.2f}" y="274" text-anchor="middle" font-size="11" fill="#40536f">{label}</text>')
        date_text = _date((row or {}).get("date"))
        if date_text != "日期待核":
            date_text = date_text[5:] if week in (2,4,8) else date_text
        date_y = 291 + (index%3)*15 if week <= 8 else 291
        anchor = "start" if week == 0 else "end" if week == 52 else "middle"
        s.append(f'<text x="{px:.2f}" y="{date_y}" text-anchor="{anchor}" font-size="10" fill="#718096">{_e(date_text)}</text>')
        if not row or not _number(row.get("base")):
            gaps.append(label)
            s.append(f'<text x="{px:.2f}" y="237" text-anchor="middle" font-size="10" fill="#ad650d">待证</text>')
            continue
        py = y(row["base"])
        fill = COLORS["base"] if week == 0 else "white"
        title = f'{label} · {_date(row.get("date"))} · {_direction(row["base"])}；{row.get("label") or ""}；条件：{row.get("condition") or "待补"}；失效：{row.get("invalidation") or "待补"}'
        s.append(f'<circle class="vf-future-point" data-weeks="{week}" data-base="{row["base"]}" cx="{px:.2f}" cy="{py:.2f}" r="{5.5 if week else 6}" fill="{fill}" stroke="{COLORS["base"]}" stroke-width="2.1"><title>{_e(title)}</title></circle>')
    s.append('<text x="79" y="342" font-size="10" fill="#718096">时间按实际周数展开 · 近端节点密集，可悬停看条件；手机左右滑动</text></svg>')
    legend = '<div class="vf-legend"><span><i style="color:#2563eb"></i>基准条件路径</span><span><i style="color:#159447"></i>上行情景</span><span><i style="color:#d45555"></i>下行情景</span><span><i class="vf-band-key"></i>条件情景范围</span></div>'
    gap = '<div class="vf-gap">○ '+_e(" / ".join(gaps))+'证据未齐，曲线在缺口处断开；同时核对节点日期。</div>' if gaps else ''
    return '<div class="vf-panel"><div class="vf-panel-title">↗ 未来一年 · 条件路径 <small>起点 → 2 / 4 / 8 / 16 / 32 / 52周</small></div><div class="vf-scroll">'+''.join(s)+'</div>'+legend+gap+'</div>'


def render(doc, compact=False):
    """Render producer-supplied conditional paths; never mutates ``doc``."""
    doc = doc if isinstance(doc, dict) else {}
    uid = "vf-"+sha256(str((doc.get("input_id"), doc.get("code"), doc.get("name"))).encode()).hexdigest()[:16]
    rows = _normalize(doc)
    name = str(doc.get("name") or doc.get("code") or "当前标的")
    code = str(doc.get("code") or "")
    caption = name if name == code or not code else name+" · "+code
    classes = "v88-future vf-compact" if compact else "v88-future"
    s = [CSS, f'<section class="{classes}" data-input-id="{_e(doc.get("input_id"))}" data-status="{_e(doc.get("status") or "missing")}" aria-label="{_e(caption)}未来条件趋势">',
         f'<h3>◉ {_e(caption)} · 未来趋势研判</h3>',
         f'<div class="vf-meta">证据截至 {_e(doc.get("source_asof") or "时间待核")} · 研判起点 {_e(doc.get("anchor_date") or doc.get("source_asof") or "待核")} · 未来条件情景</div>']
    if doc.get("headline"):
        s.append(f'<div class="vf-headline">{_e(doc["headline"])}</div>')
    s += ['<div class="vf-grid">', _phase(doc, uid), _paths(doc, uid, rows), '</div>',
          '<div class="vf-note">未来条件情景 · 非价格 / 胜率；虚线随新证据更新。淡色带为条件情景范围，不是统计置信区间。圆点是当前状态，箭头仅表示条件成立时的方向。</div>']
    cells = []
    for week, row in zip(WEEKS[1:], rows[1:]):
        if not row:
            cells.append(f'<div class="vf-condition"><b>{week}周 · ○ 待补证据</b><span>尚不绘制该节点的方向。</span></div>')
            continue
        branches = row.get("branches") if isinstance(row.get("branches"), dict) else {}
        branches_html = []
        for key, label in (("up", "上行情景"), ("down", "下行情景")):
            branch = branches.get(key)
            if isinstance(branch, dict):
                branches_html.append(f'<span>{label}：{_e(branch.get("label") or "条件待补")}；{_e(branch.get("condition") or "确认条件待补")}；失效：{_e(branch.get("invalidation") or "待补")}</span>')
        cells.append(f'<div class="vf-condition"><b>{week}周 · {_e(row.get("label") or _direction(row.get("base")))}</b><span>↗ 确认条件：{_e(row.get("condition") or "待补证据，不视为已经确认")}</span><span>↘ 失效：{_e(row.get("invalidation") or "待补证据，不视为已经确认")}</span>'+''.join(branches_html)+'</div>')
    gaps = doc.get("gaps") if isinstance(doc.get("gaps"), list) else []
    assumptions = doc.get("assumptions") if isinstance(doc.get("assumptions"), list) else []
    if gaps:
        assumptions = ["证据缺口："+str(item) for item in gaps]+assumptions
    notes = '<ul class="vf-assumptions">'+''.join('<li>'+_e(item)+'</li>' for item in assumptions)+'</ul>' if assumptions else ''
    s.append('<details class="vf-conditions-toggle"><summary>▸ 各阶段确认条件、失效切换与依据</summary><div class="vf-conditions">'+''.join(cells)+'</div>'+notes+'<div class="vf-note">本图不改变中央评级、原入场、止损、目标和期限；绘图本身不调用模型或网络。</div></details></section>')
    return ''.join(s)
