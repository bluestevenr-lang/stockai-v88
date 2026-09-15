"""V88 板块及个股未来周期展望（圆周相位＋证据约束情景曲线）。"""
from html import escape
import re
import math
import hashlib


HORIZONS = ("2周", "5周", "8周", "16周")
LEGACY_HORIZONS = ("明日", "下周", "半个月")
HORIZON_LABELS = {**{h: f"{h}条件档" for h in HORIZONS},
                  "明日": "旧日档", "下周": "旧周档", "半个月": "旧半月档"}
MARKET_ICONS = {"美股": "🇺🇸", "A股": "🇨🇳", "港股": "🇭🇰"}
HORIZON_SHORT = {**{h: h for h in HORIZONS},
                 "明日": "明日 (日)", "下周": "下周 (周)", "半个月": "半月 (月)"}
SECTOR_COLORS = ("#3b82f6", "#8b5cf6", "#14b8a6", "#f97316", "#ec4899")

_ROTATION_CSS = """
#__ID__{font-size:12px;line-height:1.55;margin:6px 0;color:var(--text-color,#334155)}
#__ID__ .rf-meta,#__ID__ .rf-foot{font-size:10px;color:#64748b;margin:4px 0}
#__ID__ .rf-root{margin:5px 0}#__ID__ .rf-root small{display:block;font-size:10px;color:#64748b}
#__ID__ .rf-card{padding:6px;border:1px solid #e2e8f0;border-radius:6px;margin:6px 0}
#__ID__ .rf-scroll{max-width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch}
#__ID__ table{border-collapse:collapse;width:100%;min-width:690px;font-size:11px}
#__ID__ td,#__ID__ th{border:1px solid #e2e8f0;padding:5px;text-align:left;vertical-align:top}
#__ID__ th{background:#eff6ff}#__ID__ .rf-meter{height:3px;background:#e2e8f0;margin-top:3px;min-width:55px}
#__ID__ .rf-meter i{display:block;height:3px;background:#64748b}#__ID__ details{font-size:10px}
#__ID__ .rf-warning{font-size:11px;color:#b45309;margin-top:5px}
"""


def _number(value):
    return float(value) if type(value) in (int,float) and math.isfinite(value) else None


def _score(value):
    number = _number(value)
    return number if number is not None and 0 <= number <= 100 else None


def _research_text(value):
    text = str(value or '')
    if re.search(r'买入|卖出|加仓|减仓|满仓|试仓|跟进|必涨|必跌', text):
        return '原说明含交易动作；本模块仅保留观察，执行仍查中央原合同'
    return text


def _score_cell(value):
    score = _score(value)
    if score is None:
        return '<span class="rf-missing">○ 缺证</span>'
    return f'<span>{score:g}/100</span><div class="rf-meter"><i style="width:{score:g}%"></i></div>'


def _observed_change(facts):
    five, twenty = (_number((facts or {}).get(k)) for k in ('5d','20d'))
    if five is None or twenty is None:
        return '○ 历史动量缺证'
    if five < 0 < twenty:
        return '⚠ 月强周弱'
    if five > 0 and twenty > 0:
        return '↑ 增强·5/20日同涨'
    if five < 0 and twenty < 0:
        return '↓ 减弱·5/20日同跌'
    return '↔ 窗口分歧'

def _phase(strength: float, mom: float) -> str:
    if strength >= 0 and mom >= 0:
        return "领涨启动"
    if strength >= 0:
        return "高位派发"
    if mom >= 0:
        return "低位蓄势"
    return "退潮杀跌"


def _trajectory_horizons(trajectory: list) -> tuple:
    """新版优先；旧缓存尚未被下一轮流水线替换时仍可正常显示。"""
    keys = {h for item in (trajectory or []) for h in (item.get("points") or {})}
    return HORIZONS if any(h in keys for h in HORIZONS) else LEGACY_HORIZONS


def _node(x: float, y: float, color: str, conf: str, title: str) -> str:
    tip = f"<title>{escape(title)}</title>"
    if conf == "高":
        return f'<circle cx="{x:.0f}" cy="{y:.0f}" r="5" fill="{color}">{tip}</circle>'
    if conf == "中":
        return f'<circle cx="{x:.0f}" cy="{y:.0f}" r="4.6" fill="{color}" fill-opacity=".82">{tip}</circle>'
    return (f'<circle cx="{x:.0f}" cy="{y:.0f}" r="4.5" fill="var(--rf-node-bg)" '
            f'stroke="{color}" stroke-width="1.7">{tip}</circle>')


def _swimlane_svg(market: str, trajectory: list) -> str:
    """Compatibility name: separate score cells, never a future price line."""
    if not trajectory:
        return ''
    horizons = _trajectory_horizons(trajectory)
    out = ['<div class="rf-scroll"><table class="rf-scenario-matrix"><thead><tr><th>板块与实际动量</th><th>当前规则分</th>']
    out.extend('<th>'+escape(HORIZON_LABELS[h])+'</th>' for h in horizons)
    out.append('</tr></thead><tbody>')
    for item in trajectory:
        points = item.get('points') or {}; facts = item.get('facts') or {}
        out.append('<tr><td><b>'+escape(str(item.get('name') or '名称待核'))+'</b><div>'+_observed_change(facts)+'</div>')
        if facts:
            out.append('<div>实际5/20日变动 '+escape(str(facts.get('5d', '缺证')))+'% / '+escape(str(facts.get('20d', '缺证')))+'%</div>')
        out.append('</td><td>'+_score_cell(item.get('now'))+'</td>')
        for horizon in horizons:
            point = points.get(horizon) or {}
            out.append('<td>'+_score_cell(point.get('score')))
            if point:
                out.append('<details><summary>条件 / 反证</summary><div>确认：'+escape(_research_text(point.get('trigger')) or '○ 未记录')+'</div><div>失效：'+escape(_research_text(point.get('invalid')) or '○ 未记录')+'</div></details>')
            out.append('</td>')
        out.append('</tr>')
    out.append('</tbody></table></div>')
    return ''.join(out)


def _scenario_id(prefix, name, code='', index=0):
    token = hashlib.sha256(f'{name}|{code}|{index}'.encode()).hexdigest()[:12]
    return re.sub(r'[^a-zA-Z0-9_-]', '-', prefix) + '-' + token


def _strip_styles(content):
    styles = []
    def collect(match):
        styles.append(match.group(1))
        return ''
    body = re.sub(r'<style>(.*?)</style>', collect, content or '', flags=re.S)
    return '\n'.join(styles), body


def _clock_svg(market: str, trajectory: list, heat: dict | None = None) -> str:
    """All objects retain their actual phase; each point opens its own future path.

    The caller supplies phase from evidence or the preserved cycle record. No
    difference between old horizon scores is used to invent a clock position.
    """
    out = ['<div class="rf-clock-main"><svg class="rf-phase-clock" viewBox="0 0 440 310" role="img" aria-label="全部对象当前圆周相位">',
           '<circle cx="220" cy="155" r="108" fill="none" stroke="#cbd5e1"/>',
           '<circle cx="220" cy="155" r="60" fill="none" stroke="#e2e8f0" stroke-dasharray="4 5"/>',
           '<path d="M112 155H328 M220 47V263" stroke="#94a3b8" fill="none"/>',
           '<text x="220" y="26" text-anchor="middle">↑ 领涨启动</text>',
           '<text x="336" y="159">高位转弱</text>',
           '<text x="104" y="159" text-anchor="end">低位修复</text>',
           '<text x="220" y="293" text-anchor="middle">↓ 下行压力</text>']
    missing = []
    for index, row in enumerate(trajectory or []):
        phase = row.get('phase') or {}
        x, y = (_number(phase.get(k)) for k in ('x', 'y')) if isinstance(phase, dict) else (None, None)
        label = str(row.get('label') or row.get('name') or '名称待核')
        target = str(row.get('target') or '')
        href = str(row.get('href') or '#'+target)
        if x is None or y is None:
            missing.append((label,href)); continue
        x, y = max(-1,min(1,x)), max(-1,min(1,y))
        token=int(hashlib.sha256((label+'|'+str(index)).encode()).hexdigest()[:8],16)
        angle=(token%360)*math.pi/180
        x+=math.cos(angle)*.07; y+=math.sin(angle)*.07
        # Radial cap preserves the source quadrant; scatter overlap uses a
        # stable tiny separation purely for visibility, never changes phase.
        radius = math.hypot(x,y)
        if radius > .94:
            x, y = x*.94/radius, y*.94/radius
        cx, cy = 220+x*108, 155-y*108
        color = row.get('color') or SECTOR_COLORS[index % len(SECTOR_COLORS)]
        title = label+' · '+str(phase.get('label') or '相位待核')+'；点击查看未来情景曲线'
        arrow=''
        dx,dy=(_number(phase.get(k)) for k in ('dx','dy'))
        if dx is not None and dy is not None and math.hypot(dx,dy)>.02:
            length=math.hypot(dx,dy); ux,uy=dx/length,-dy/length
            ex,ey=cx+ux*21,cy+uy*21
            arrow=(f'<path d="M{cx:.1f} {cy:.1f} L{ex:.1f} {ey:.1f} '
                   f'M{ex-ux*5-uy*3:.1f} {ey-uy*5+ux*3:.1f} L{ex:.1f} {ey:.1f} '
                   f'L{ex-ux*5+uy*3:.1f} {ey-uy*5-ux*3:.1f}" fill="none" stroke="{color}" stroke-width="1.5" stroke-dasharray="3 2"/>')
        out.append('<a href="'+escape(href,quote=True)+'" aria-label="'+escape(title,quote=True)+'">'
                   +arrow+f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="7" fill="{color}" stroke="white" stroke-width="1.3"><title>'+escape(title)+'</title></circle>'
                   +f'<text x="{cx+9:.1f}" y="{cy-7:.1f}" fill="{color}" font-size="10">{index+1}</text></a>')
    out.append('</svg>')
    if missing:
        out.append('<details class="rf-meta rf-clock-missing"><summary>○ 相位待核 · '+str(len(missing))+'只</summary>'+' · '.join('<a href="'+escape(href,quote=True)+'">'+escape(name)+'</a>' for name,href in missing)+'</details>')
    out.append('</div>')
    return ''.join(out)


def _future_group(items, prefix, market='', compact=False):
    """A script-free selection links every clock point to the matching curve."""
    from future_trend_visual import render
    entries = []
    for i, item in enumerate(items):
        doc = item['doc']
        label = str(item.get('label') or doc.get('name') or doc.get('code') or '名称待核')
        target = _scenario_id(prefix, label, doc.get('code'), i)
        entries.append({**item, 'label': label, 'target': target,
                        'phase': item.get('phase') or doc.get('phase'),
                        'color': item.get('color') or SECTOR_COLORS[i % len(SECTOR_COLORS)]})
    if not entries:
        return '<div class="rf-meta">○ 当前对象资料待补</div>'
    nav = ''.join('<a href="#'+e['target']+'" style="border-color:'+e['color']+'">'
                  +str(i+1)+'. '+escape(e['label'])+'</a>' for i,e in enumerate(entries))
    panels=[]
    for e in entries:
        panels.append('<section class="rf-future-panel" id="'+e['target']+'" tabindex="-1">'
                      +render(e['doc'],compact=compact)+'</section>')
    return ('<div class="rf-future-selector"><div class="rf-clock-wrap">'
            +_clock_svg(market,entries,{})+'<div class="rf-pick-list">'+nav+'</div></div>'
            +'<div class="rf-meta">圆点是当前相位，虚箭头为条件方向；点击圆点或名称切换下方未来曲线。曲线为条件情景，后续按实际证据更新。</div>'
            +'<div class="rf-future-panels">'+''.join(panels)+'</div></div>')


_FUTURE_CSS = """
.rf-future-panels .vf-phase{display:none}.rf-future-panels .v88-future .vf-grid{grid-template-columns:minmax(0,1fr)}
.rf-future-panels .v88-future .vf-path-svg{min-width:600px}.rf-clock-missing{font-size:10px;max-height:120px;overflow:auto}.rf-clock-main{min-width:0}
.rf-phase-clock{width:100%;height:auto;max-height:300px;display:block;color:#475569}
.rf-phase-clock text{font-family:inherit;font-size:11px;fill:currentColor}
.rf-phase-clock a{cursor:pointer}.rf-phase-clock a:hover circle{stroke:#0f172a;stroke-width:3}
.rf-clock-wrap{display:grid;grid-template-columns:minmax(220px,1fr) minmax(130px,.7fr);align-items:center;gap:8px}
.rf-pick-list{display:flex;flex-wrap:wrap;gap:5px;align-content:start;max-height:250px;overflow:auto}
.rf-pick-list a{display:inline-block;border:1px solid #cbd5e1;border-radius:6px;padding:3px 6px;color:inherit;font-size:11px;text-decoration:none}
.rf-pick-list a:hover{background:#eff6ff}.rf-future-panel{display:none;scroll-margin-top:25px}
.rf-future-panel:first-child{display:block}.rf-future-panels:has(>.rf-future-panel:target)>.rf-future-panel:first-child{display:none}
.rf-future-panels>.rf-future-panel:target{display:block!important}
@media(max-width:600px){.rf-clock-wrap{grid-template-columns:1fr}.rf-pick-list{max-height:140px}}
"""


def _rich_rotation_html(forecast: dict, element_id: str, focus_market: str,
                        compact: bool = False) -> str:
    from trend_scenarios import build_sector
    from exchange_sessions import latest_completed
    from datetime import datetime, timezone
    safe_id = re.sub(r'[^a-zA-Z0-9_-]', '-', element_id)
    trajectories = forecast.get('trajectories') or {}
    order = [focus_market]+[m for m in ('美股','A股','港股') if m != focus_market]
    focus = next((m for m in order if trajectories.get(m)), None)
    cards=[]
    for market in ((focus,) if compact and focus else ('美股','A股','港股')):
        rows = trajectories.get(market) or []
        quality = (forecast.get('data_quality') or {}).get(market) or {}
        if not rows and not quality:
            continue
        # Only the explicit source clock is admissible, never analysis_time.
        dates = (forecast.get('source_dates_by_market') or {}).get(market) or []
        source = dates[0] if len(dates)==1 else forecast.get('source_asof') if not dates else None
        required_session=latest_completed(market,datetime.now(timezone.utc)).isoformat()
        entries=[{'doc':build_sector(row,market=market,source_asof=row.get('source_asof') or source or '',required_session=required_session),
                  'label':row.get('name')} for row in rows]
        cards.append('<div class="rf-card"><b>'+MARKET_ICONS.get(market,'')+' '+escape(market)+' · 未来周期展望</b>')
        if quality:
            cards.append('<div class="rf-meta">原板块样本 '+escape(str(quality.get('valid_count','待核')))+' / '+escape(str(quality.get('input_count','待核')))+'；保留原排序</div>')
        cards.append(_future_group(entries,safe_id+'-'+market,market,compact=compact) if rows else '<div>○ 板块数据待补，不能据此判断无机会</div>')
        cards.append('<details class="rf-history"><summary>历史因子与原条件档 · 推演依据</summary>'+_swimlane_svg(market,rows)+'</details></div>')
    warnings=[str(item.get('market',''))+'·'+str(item.get('sector',''))+'：'+_research_text(item.get('reason')) for item in forecast.get('warnings') or []]
    warning='<details class="rf-warning"><summary>⚠ 板块风险与反证</summary>'+escape('；'.join(warnings))+'</details>' if warnings else ''
    body=('<div id="'+safe_id+'" role="figure" aria-label="中美港板块未来周期展望">'
          +'<div class="rf-meta">行情日 '+escape(str(forecast.get('source_asof') or '逐对象核验'))+' · 原计算 '+escape(str(forecast.get('analysis_time') or '未记录'))+'</div>'
          +'<div class="rf-root"><b>圆周相位 ＋ 未来一年变化路径</b><small>基准、改善与恶化三条条件路径；不是承诺涨幅或已验证概率。</small></div>'
          +''.join(cards)+warning+'<div class="rf-foot">未来情景复用原事实，不修改中央评级、原进场/止盈/失效价或期限。本地绘图，新增模型调用 0。</div></div>')
    nested_css,body=_strip_styles(body)
    return '<style>'+_ROTATION_CSS.replace('__ID__',safe_id)+_FUTURE_CSS+nested_css+'</style>'+body


_CYCLE_CSS = """
#__ID__{color:var(--foreground,var(--text-color));margin:.25rem 0 .6rem;--cy-up:#22c55e;--cy-down:#ef4444;--cy-hold:#9aa3b2}
#__ID__ .cy-meta{color:var(--muted-foreground,var(--text-color));font-size:11px;margin-bottom:.3rem}
#__ID__ .cy-card{background:color-mix(in srgb,currentColor 6%,transparent);border-radius:9px;padding:.5rem .6rem;margin:.4rem 0}
#__ID__ svg{display:block;width:100%;height:auto;overflow:visible}
#__ID__ svg text{font-size:11px;font-family:inherit}
#__ID__ .grid{stroke:color-mix(in srgb,currentColor 16%,transparent);stroke-width:1}
#__ID__ .axis{stroke:color-mix(in srgb,currentColor 34%,transparent);stroke-width:1}
#__ID__ .ph{fill:var(--muted-foreground,var(--text-color));opacity:.75}
#__ID__ .cy-list{display:grid;grid-template-columns:1fr 1fr;gap:.5rem;margin-top:.3rem}
/* 【字号统一 2026-07-27】标题12px、行12px(原11px与左侧板块行12px不齐),辅助信息靠颜色分主次 */
#__ID__ .cy-col b{font-size:12.5px}
#__ID__ .cy-row{font-size:12px;color:var(--muted-foreground,var(--text-color));margin:.2rem 0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;line-height:1.5}
#__ID__ .cy-row .cy-nm{font-weight:600}
#__ID__ .cy-cert{display:inline-block;white-space:nowrap;padding:0 3px;line-height:14px;text-align:center;border-radius:3px;font-size:9.5px;font-weight:800;margin-left:3px;vertical-align:middle}
#__ID__ .cy-legend{font-size:11px;color:#94a3b8;margin-top:.35rem;line-height:1.6}
#__ID__ .cy-nm{color:var(--foreground,var(--text-color))}
#__ID__ .cy-up{color:var(--cy-up)}#__ID__ .cy-down{color:var(--cy-down)}
@media(max-width:560px){#__ID__ .cy-list{grid-template-columns:1fr}}
"""


def _cert_mark(code: str = "", name: str = "", cert: dict | None = None,
               std: dict | None = None) -> str:
    """Legacy notes are history, never current GPT or turning-point approval."""
    cert, std = cert or {}, std or {}
    # No bare numeric ticker or name lookup: historical notes must not attach
    # another exchange's security merely because its digits/name look similar.
    key = str(code or '').strip()
    entry = (cert.get('by_code') or {}).get(key) if key else None
    if isinstance(entry, dict):
        at = escape(str(cert.get('asof') or '日期未记录'), quote=True)
        return (f'<span class="cy-cert" title="历史审核备注·{at}；不是当前有效GPT审核，也未验证本次周期拐点" '
                'style="background:#f1f5f9;color:#64748b">旧审核</span>')
    if key and key in (std.get('pass') or {}):
        return ('<span class="cy-cert" title="旧本地规则记录；未核对本次事实和原周期，不是当前审核许可" '
                'style="background:#f1f5f9;color:#64748b">旧规则</span>')
    return ""


def _cycle_record_phase(stock, index=0):
    """Position encodes the preserved phase label, never a future peak date."""
    mapping={'蓄势→领涨':(-.5,.65), '派发→退潮':(.5,-.55),
             '领涨中':(-.15,.78), '退潮中':(.15,-.78),
             '偏多整理':(-.65,.15), '偏空整理':(.65,-.15), '整理':(0,0)}
    label=str(stock.get('phase') or '')
    if stock.get('stale_preserved') or label not in mapping:
        return {'label':label or '相位待核','x':None,'y':None}
    x,y=mapping[label]
    # Small stable visual offsets separate records in the same phase category.
    token=int(hashlib.sha256(str(stock.get('code') or index).encode()).hexdigest()[:8],16)
    angle=(token%360)*math.pi/180; radius=.025+.09*((token//360)%10)/9
    return {'label':label,'x':x+math.cos(angle)*radius,'y':y+math.sin(angle)*radius}


def stock_cycle_html(cycle: dict, element_id: str = "v88-stock-cycle", profiles=None) -> str:
    """Show bounded cycle names while retaining the complete source document."""
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "-", element_id)
    if (cycle or {}).get('status') == 'pending':
        # A pending scan is not an empty opportunity verdict. Only display
        # clocks already present in this document; rendering never renews facts.
        clocks = []
        for key, label in (('source_asof', '原行情日期'), ('analysis_time', '原周期计算')):
            if cycle.get(key):
                clocks.append(f'{label} {escape(str(cycle[key]))}')
        recorded = f'<div class="cy-meta">{" · ".join(clocks)}</div>' if clocks else ''
        css = _CYCLE_CSS.replace('__ID__', safe_id)
        return (f'<style>{css}</style><div id="{safe_id}" role="status">'
                '<div class="cy-meta"><b>个股周期扫描待完成</b></div>'
                '<div class="cy-legend">当前不能据此判断有无观察标的。</div>'
                f'{recorded}</div>')
    from display_limits import market_top
    from grade_focus import canonical, market_of
    original_stocks = (cycle or {}).get('stocks') or []
    stocks = market_top([{**r, 'market': r.get('market') or market_of(canonical(r.get('code')))} for r in original_stocks])
    if not stocks:
        return ''
    from urllib.parse import quote
    from stock_profile_view import display_label, link_html, load as load_profiles
    profiles=load_profiles() if profiles is None else profiles
    entries=[]; rendered=[]; rows=[]; previewed=set(); memo={}
    for i, stock in enumerate(stocks):
        code=str(stock.get('code') or '')
        label=display_label(stock.get('name'),code,profiles)
        deep='/?q='+quote(code,safe='')+'&focus=deep#v88-deep-analysis'
        target=_scenario_id(safe_id+'-future',label,code,i)
        direction=stock.get('direction') if stock.get('direction') in {'up','down','hold'} else 'hold'
        color={'up':'#16a34a','down':'#dc2626','hold':'#64748b'}[direction]
        doc=None
        # One representative per current direction; the complete pool remains
        # clickable without calculating dozens of annual views on every load.
        if code and not stock.get('stale_preserved') and direction not in previewed and len(previewed)<3:
            previewed.add(direction)
            try:
                from stock_future_context import for_stock
                if code not in memo:
                    memo[code]=for_stock(code,label)
                doc=memo[code]
            except Exception:
                # Render the original record even when optional local context
                # fails; do not replace its source date or infer a future curve.
                doc=None
        phase=_cycle_record_phase(stock,i)
        if doc and doc.get('status')=='ready':
            # When a current same-source future view exists, its shared phase
            # is also used here; otherwise retain the dated original record.
            if str(doc.get('source_asof') or '')[:10] == str(stock.get('source_asof') or '')[:10]:
                phase=doc.get('phase') or phase
            else:
                doc=None
        href='#'+target if doc else deep
        entries.append({'label':label,'phase':phase,'target':target,'href':href,'deep':deep,'color':color})
        if doc:
            from future_trend_visual import render
            rendered.append('<section class="rf-future-panel" id="'+target+'" tabindex="-1">'+render(doc,compact=True)+'</section>')
        up,down,position=(_score(stock.get(k)) for k in ('up','down','pos52'))
        tag=('○ 方向分缺证' if up is None or down is None else '↑ 增强线索' if direction=='up' and up>down else
             '↓ 减弱线索' if direction=='down' and down>up else '↔ 方向分歧')
        values=' / '.join(f'{value:g}' if value is not None else '○ 缺证' for value in (up,down))
        position_text=f'{position:g}%' if position is not None else '○ 缺证'
        rows.append('<tr><td>'+link_html(stock.get('name'),code,profiles)+'<div>'+tag+'</div></td><td>'+values+'</td><td>'+position_text+'<div>原历史位置字段，全年窗口未附凭据</div></td><td>'
                    +escape(str(stock.get('source_asof') or '日期未记录'))+'<details><summary>确认 / 失效</summary><div>确认：'+escape(_research_text(stock.get('trigger')) or '○ 未记录')+'</div><div>失效：'+escape(_research_text(stock.get('invalid')) or '○ 未记录')+'</div></details></td></tr>')
    nav=''.join('<span><a href="'+escape(row['deep'],quote=True)+'" style="border-color:'+row['color']+'">'+str(i+1)+'. '+escape(row['label'])+'</a>'
                +('<a href="'+escape(row['href'],quote=True)+'" aria-label="'+escape(row['label']+' · 本页未来曲线',quote=True)+'">↗ 曲线</a>' if row['href'].startswith('#') else '')+'</span>'
                for i,row in enumerate(entries))
    body=(f'<div id="{safe_id}" role="figure" aria-label="个股周期切换扫描">'
          +'<div class="cy-meta">原周期计算 '+escape(str((cycle or {}).get('analysis_time') or '未记录'))+' · 展示 '+str(len(stocks))+' 只（每市场Top5）</div>'
          +'<div class="rf-clock-wrap">'+_clock_svg('',entries,{})+'<div class="rf-pick-list">'+nav+'</div></div>'
          +'<div class="cy-meta">↑ 改善 / ↓ 承压 / ↔ 待确认；圆点表示当前相位。点名称进入深度分析；点圆点或“曲线”看本页预览（至多3只）。</div>'
          +'<div class="rf-future-panels">'+''.join(rendered)+'</div>'
          +'<details class="cy-history"><summary>当前Top名单技术记录与触发条件 · '+str(len(stocks))+' 只</summary><div class="cy-scroll"><table><thead><tr><th>证券</th><th>原上/下方向分 /100</th><th>原历史位置</th><th>行情日与条件</th></tr></thead><tbody>'+''.join(rows)+'</tbody></table></div></details>'
          +'<div class="cy-legend">未来曲线是同源本地条件推演；本次周期拐点没有独立GPT验证凭据。历史周期线索不授予评级或买卖许可，原合同与保护条件保持。</div></div>')
    nested_css,body=_strip_styles(body)
    css=_CYCLE_CSS.replace('__ID__',safe_id)+_FUTURE_CSS+f"""
#{safe_id} .cy-scroll{{max-width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch}}
#{safe_id} table{{width:100%;min-width:650px;border-collapse:collapse;font-size:11px}}
#{safe_id} th,#{safe_id} td{{padding:5px;border:1px solid #e2e8f0;text-align:left;vertical-align:top}}
#{safe_id} th{{background:#eff6ff}}#{safe_id} td div,#{safe_id} details{{font-size:10px;color:#64748b}}"""
    return '<style>'+css+nested_css+'</style>'+body


def available_markets(forecast: dict) -> list:
    """有走向数据、可作为时钟焦点的市场列表（保持 美股/A股/港股 顺序）。"""
    traj = (forecast or {}).get("trajectories") or {}
    return [m for m in ("美股", "A股", "港股") if traj.get(m)]


def rotation_map_html(forecast: dict, element_id: str = "v88-rotation-map",
                      focus_market: str = "美股", compact: bool = False) -> str:
    if not forecast or not forecast.get("markets"):
        return ""
    if forecast.get("trajectories"):
        return _rich_rotation_html(forecast, element_id, focus_market, compact=compact)
    return _legacy_rotation_html(forecast, element_id)


def combined_cycle_dashboard_html(forecast: dict, cycle: dict,
                                  element_id: str = "v88-cycle-dashboard",
                                  focus_market: str = "美股") -> str:
    """把板块轮动与个股周期并排收口为一个紧凑模块；数据内容不删，只压缩视觉层级。"""
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "-", element_id)
    sector_html = (rotation_map_html(forecast, f"{safe_id}-sector", focus_market, compact=True)
                   if forecast else "")
    stock_html = stock_cycle_html(cycle, f"{safe_id}-stock") if cycle else ""

    def _split_style(html: str):
        """抽出纯CSS并合进唯一style，避免Streamlit把第二个style内容显示成正文。"""
        return _strip_styles(html)

    sector_style, sector_body = _split_style(sector_html)
    stock_style, stock_body = _split_style(stock_html)
    panels = []
    if sector_body:
        panels.append('<div class="cc-panel"><div class="cc-title">🧭 板块未来周期 · 圆周＋曲线</div>'
                      + sector_body + '</div>')
    if stock_body:
        panels.append('<div class="cc-panel"><div class="cc-title">🎯 个股周期 · 持仓＋自选</div>'
                      + stock_body + '</div>')
    if not panels:
        return ""
    single = " cc-single" if len(panels) == 1 else ""
    return f'''<style>
#{safe_id}{{margin:.22rem 0 .5rem;color:var(--foreground,var(--text-color))}}
#{safe_id} .cc-head{{display:flex;justify-content:space-between;gap:.5rem;align-items:baseline;
  margin:0 0 .25rem;padding:.26rem .45rem;background:color-mix(in srgb,currentColor 5%,transparent);border-radius:7px}}
#{safe_id} .cc-head b{{font-size:11px}} #{safe_id} .cc-head span{{font-size:8px;color:var(--muted-foreground,var(--text-color))}}
#{safe_id} .cc-grid{{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:.42rem;align-items:start}}
#{safe_id} .cc-grid.cc-single{{grid-template-columns:1fr}}
#{safe_id} .cc-panel{{min-width:0;border:1px solid color-mix(in srgb,currentColor 11%,transparent);border-radius:8px;padding:.28rem .36rem;background:color-mix(in srgb,currentColor 3%,transparent)}}
#{safe_id} .cc-title{{font-size:10px;font-weight:700;margin:0 0 .18rem;color:var(--foreground,var(--text-color))}}
#{safe_id} .cy-meta{{font-size:8px;margin-bottom:.15rem}}
#{safe_id} .cy-card{{padding:.2rem .28rem;margin:.2rem 0}}
#{safe_id} .cy-card svg text{{font-size:8px}}
#{safe_id} .cy-list{{gap:.25rem;margin-top:.15rem;max-height:190px;overflow:auto;padding-right:2px}}
#{safe_id} .cy-scroll{{max-height:340px;overflow:auto}}#{safe_id} .cy-scroll th{{position:sticky;top:0}}
#{safe_id} .cy-col b{{font-size:9px}}
#{safe_id} .cy-row{{font-size:8px;line-height:1.25;margin:.08rem 0}}
@media(max-width:920px){{#{safe_id} .cc-grid{{grid-template-columns:1fr}}}}
{sector_style}
{stock_style}
</style>
<div id="{safe_id}" role="figure" aria-label="板块轮动与个股周期综合总览">
  <div class="cc-head"><b>🧭 周期总览</b><span>左看板块未来路径 · 右看持仓/自选周期 · 点圆点切换曲线</span></div>
  <div class="cc-grid{single}">{''.join(panels)}</div>
</div>'''


def _legacy_rotation_html(forecast: dict, element_id: str = "v88-rotation-map") -> str:
    """Old cache remains visible as conditions, without adopting old review votes."""
    trajectories = {}
    for market, horizons in (forecast.get('markets') or {}).items():
        by_name = {}
        for horizon, candidates in (horizons or {}).items():
            for candidate in candidates or []:
                name = str(candidate.get('name') or '名称待核')
                item = by_name.setdefault(name, {'name':name, 'points':{}, 'facts':candidate.get('facts') or {}})
                item['points'][horizon] = {k:candidate.get(k) for k in ('score','trigger','invalid')}
                if 'now' in candidate:
                    item['now'] = candidate['now']
        trajectories[market] = list(by_name.values())
    return _rich_rotation_html(dict(forecast,trajectories=trajectories),element_id,'美股')
