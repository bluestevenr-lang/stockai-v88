"""Small local evidence visuals. Direction, evidence and execution stay separate."""
from html import escape
import math

STATES = {'up': ('↑', '#15803d', '#f0fdf4'), 'down': ('↓', '#b91c1c', '#fef2f2'),
          'mixed': ('↔', '#7c3aed', '#f5f3ff'), 'risk': ('⚠', '#b45309', '#fffbeb'),
          'missing': ('○', '#64748b', '#f1f5f9'), 'linked': ('✓', '#0369a1', '#f0f9ff'),
          'locked': ('🔒', '#475569', '#f8fafc')}
CSS = '''<style>.v88-evidence-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:7px;margin:8px 0}
.v88-evidence-card{padding:8px;border-radius:6px;border:1px solid #e2e8f0;font-size:12px;line-height:1.45}
.v88-evidence-card small{display:block;font-size:10px;color:#64748b}.v88-evidence-card b{font-size:12px}
.v88-evidence-icon{font-size:19px;font-weight:700;margin-right:6px}.v88-evidence-foot{font-size:10px;color:#64748b;margin:4px 0}
.v88-coverage-track{position:relative;background:#e2e8f0;height:7px;border-radius:5px;margin:7px 0}
.v88-coverage-fill{display:block;height:7px;border-radius:5px}.v88-coverage-target{position:absolute;left:90%;top:-3px;height:13px;border-left:2px solid #475569}
.v88-contract-scroll{overflow-x:auto;max-width:100%}.v88-contract-scroll svg{min-width:630px;width:100%;display:block}
.v88-evidence-domains{display:flex;gap:6px;flex-wrap:wrap;margin:7px 0}.v88-evidence-domain{padding:4px 7px;border-radius:5px;font-size:11px;border:1px solid #e2e8f0}
</style>'''


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def card(label, text, state='missing', detail=''):
    symbol, color, background = STATES.get(state, STATES['missing'])
    e = lambda v: escape(str(v))
    return (f'<div class="v88-evidence-card" data-state="{e(state)}" style="background:{background}">'
            f'<small>{e(label)}</small><span class="v88-evidence-icon" style="color:{color}">{symbol}</span>'
            f'<b style="color:{color}">{e(text)}</b>' + (f'<small>{e(detail)}</small>' if detail else '') + '</div>')


def deep_overview(synthesis, annual, period):
    synthesis, annual, period = synthesis or {}, annual or {}, period or {}
    bound = (bool(period.get('annual_input_id') and period.get('synthesis_input_id')) and period.get('status') not in (None, 'blocked') and
             period.get('annual_input_id') == annual.get('input_id') and
             period.get('synthesis_input_id') == synthesis.get('input_id'))
    regime = annual.get('regime') if bound else None
    direction, state = {'up': ('上升结构', 'up'), 'up_caution': ('上升结构', 'up'),
                        'down': ('下行结构', 'down'), 'range': ('区间选择', 'mixed')}.get(regime, ('待补年度证据', 'missing'))
    ev = annual.get('evidence') or {}
    company = ('有反对证据', 'risk') if ev.get('company_adverse') else ('原周期同包支持', 'linked') if ev.get('company_support') else ('待补企业证据', 'missing')
    if not bound: company = ('待关联复核', 'missing')
    risk = ('转弱预警待解', 'risk') if bound and annual.get('top_warning') else ('周期存在分歧', 'mixed') if period.get('status') == 'divergent' else ('按条件复核', 'locked')
    body = card('年结构 · 历史依据', direction, state, '不表示未来全年单向运行')
    body += card('当前风险', *risk, detail='优先核对反证与失效条件')
    body += card('企业与估值', *company, detail='技术分不代替财务证据')
    body += card('执行', '查中央原合同', 'locked', '图示不新增开仓权限')
    return CSS + '<section class="v88-deep-visual-summary" aria-label="年度、风险、企业与执行四项证据"><div class="v88-evidence-cards">'+body+'</div></section>'


def domain_strip(doc):
    cells = []
    for d in (doc or {}).get('domains') or []:
        votes = [v.get('conclusion') for v in d.get('reviews') or []]
        if d.get('status') == 'unreviewed' or not votes or any(v in ('缺失', '未完成同包复审', None) for v in votes):
            text, state = '待证', 'missing'
        elif d.get('status') == 'conflict' or any(v in ('反对', '阻断') for v in votes):
            text, state = '反证', 'risk'
        elif d.get('status') == 'observed' and len(votes) == 2 and all(v == '支持' for v in votes):
            text, state = '同包支持', 'linked'
        else: text, state = '有分歧/缺口', 'mixed'
        symbol, color, bg = STATES[state]
        title = '；'.join(str(v.get('reason') or '') for v in d.get('reviews') or [])
        cells.append(f'<span class="v88-evidence-domain" title="{escape(title, quote=True)}" style="color:{color};background:{bg}">{symbol} {escape(str(d.get("title") or d.get("domain")))} · {text}</span>')
    return CSS + '<div class="v88-evidence-domains" aria-label="八域证据状态">'+''.join(cells)+'</div>'


def coverage_card(label, count, total, passed=False, detail='', pending_reason=''):
    valid = finite(count) and finite(total) and total > 0 and 0 <= count <= total
    state = 'linked' if valid and passed and count * 10 >= total * 9 else 'risk' if valid else 'missing'
    if pending_reason: state = 'risk' if valid else 'missing'
    symbol, color, bg = STATES[state]
    status_label = pending_reason or ('达标' if state == 'linked' else '待达标' if valid else '待核')
    value = count / total * 100 if valid else None
    text = f'{count:,.0f} / {total:,.0f} · {value:.2f}%' if valid else '数量或时点待核'
    bar = f'<span class="v88-coverage-fill" style="width:{value:.5f}%;background:{color}"></span>' if valid else ''
    return (f'<div class="v88-evidence-card" style="background:{bg}" aria-label="{escape(label)}覆盖：{escape(text)}">'
            f'<b>{symbol} {escape(label)} · {escape(status_label)}</b><div>{text}</div><div class="v88-coverage-track">{bar}<i class="v88-coverage-target" title="90%目标线"></i></div>'
            f'<small>{escape(detail)} · 竖线为90%目标</small></div>')


def contract_strip(plan, current, *, verified=False):
    """Plot existing contract prices on a price axis, never a future time axis."""
    plan = plan or {}; entry = plan.get('entry_range') or []; target = plan.get('take_profit_range') or []
    stop = plan.get('stop')
    if not (verified and len(entry) == len(target) == 2 and all(finite(v) and v > 0 for v in [stop, current, *entry, *target])
            and stop < entry[0] <= entry[1] < target[0] <= target[1]):
        return '<div class="v88-evidence-foot">○ 原合同价格轴待核；不补造入场、止损或目标。</div>'
    lo = min(stop, current); hi = max(current, target[1]); width = hi-lo
    x = lambda v: 55 + 530 * (v-lo)/width
    e = lambda value: escape(str(value))
    svg = ['<svg viewBox="0 0 640 108" role="img" aria-label="原合同价格位置，不是未来走势">',
           '<line x1="55" y1="48" x2="585" y2="48" stroke="#cbd5e1" stroke-width="5"/>']
    for values, color in ((entry, '#2563eb'), (target, '#15803d')):
        svg.append(f'<rect x="{x(values[0]):.1f}" y="41" width="{max(3,x(values[1])-x(values[0])):.1f}" height="14" rx="3" fill="{color}"/>')
    svg.append(f'<line x1="{x(stop):.1f}" x2="{x(stop):.1f}" y1="34" y2="63" stroke="#b91c1c" stroke-width="3"/>')
    svg.append(f'<path d="M{x(current):.1f},36 l-5,-8 h10 Z" fill="#111827"/><text x="{x(current):.1f}" y="19" text-anchor="middle" font-size="12">现 {current:g}</text>')
    for price, label, color, anchor in ((stop,'失效', '#b91c1c','start'),((entry[0]+entry[1])/2,'原入场', '#2563eb','middle'),((target[0]+target[1])/2,'原止盈', '#15803d','end')):
        value = f'{stop:g}' if label == '失效' else '～'.join(f'{v:g}' for v in (entry if label == '原入场' else target))
        svg.append(f'<text x="{x(price):.1f}" y="77" text-anchor="{anchor}" fill="{color}" font-size="11">{label}</text><text x="{x(price):.1f}" y="94" text-anchor="{anchor}" font-size="10">{e(value)}</text>')
    svg.append('</svg>')
    return CSS+'<details class="v88-contract-visual"><summary>↔ 现价与原合同区间 · 价格轴</summary><div class="v88-contract-scroll">'+''.join(svg)+'</div><div class="v88-evidence-foot">仅定位价格，不能把现价落入区间当成已满足全部执行条件。</div></details>'
