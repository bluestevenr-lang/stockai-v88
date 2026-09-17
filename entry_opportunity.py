"""Read-only entry attention filter. No grades, contracts or orders are created.

Only numeric evidence already cited in the frozen reviews is consumed. Unknown
templates/evidence remain in the archive. Keep the desktop copy identical.
"""
from decimal import Decimal
from html import escape
import math
import re

from profit_contract import evaluate

VERSION = 'entry-opportunity-v4-explicit-outcomes'
MIN_NET_RR = 2


def _num(value):
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError('缺少有限数值证据')
    return Decimal(str(value))


def _evidence(card, field):
    values = []
    for result in [card.get('gpt') or {}, *(card.get('review_pair') or {}).values()]:
        for item in result.get('evidence') or []:
            if item.get('field') == field:
                values.append(_num(item.get('value')))
    if not values:
        raise ValueError('当前审核未引用' + field + '，需补证后复核')
    if any(v != values[0] for v in values):
        raise ValueError(field + '在审核证据中冲突，需复核')
    return values[0]


def assess(row, *, now=None, week_doc=None):
    """A feasible conditional path is attention-worthy, never a buy approval.

    For short trend/retest contracts require an overlap between the ORIGINAL
    zone, cited MA20 recovery reference and cost-adjusted RR >= 2 prices. MA20
    is an attention safeguard, not a claimed Sperandeo trendline or signal.
    Medium/long contracts keep their own horizon's book checks; do not apply a
    daily moving average to a fundamental investment thesis.
    """
    plan = row.get('central_trade_plan') or row.get('trade_plan') or {}
    card = row.get('scorecard') or {}
    horizon = plan.get('horizon') or row.get('horizon')
    out = dict(version=VERSION, focus_eligible=False, executable=False,
               state='ARCHIVE', label='🔎 研究跟踪', failure_kind='EVIDENCE', reasons=[],
               original_entry_range=plan.get('entry_range'), feasible_price_band=[],
               original_trigger=plan.get('promotion_trigger'), no_grade_authority=True,
               model_calls=0, minimum_net_rr=MIN_NET_RR)
    try:
        if not all((card.get(k) or {}).get('current') is True and
                   (card.get(k) or {}).get('complete') is True for k in ('gpt', 'books')):
            raise ValueError('当前评级审核缺失或过期，仅保留原研究档案')
        if row.get('tier') not in ('1A', '2A', '3A'):
            out['failure_kind']='REJECTED'
            raise ValueError('双审已完成，但当前结论未达到1A准入条件；查看上方真实评分与逐项审核')
        pc = evaluate(plan, horizon, now=now)
        out['deadline'] = pc.get('thesis_deadline')
        if not pc.get('valid') or not pc.get('eligible'):
            out['failure_kind']='EXPIRED' if any('期限已到' in str(r) for r in pc.get('reasons',[])) else ('NO_PATH' if pc.get('valid') else 'EVIDENCE')
            raise ValueError('原收益合同未通过：' + '；'.join(pc.get('reasons') or []))
        last, stop = _num(plan.get('last')), _num(plan.get('stop'))
        lo, hi = map(_num, plan['entry_range'])
        if last <= stop and row.get('data_fresh', row.get('central_data_fresh')) is not False:
            out['failure_kind']='INVALIDATED'
            raise ValueError('现价已触及原失效线，暂停新开仓并核查原保护合同')
        if row.get('data_fresh', row.get('central_data_fresh')) is False:
            raise ValueError('行情证据过期，需更新同包审核')
        for field in ('risk', 'system'):
            state = (row.get(field) or {}).get('status')
            if state and state != 'PASS':
                raise ValueError('中央' + field + '未放行：' + str(state))
        if (row.get('execution') or {}).get('status') in ('INVALID', 'EXPIRED', 'BLOCKED'):
            raise ValueError('原入场合同已失效或受阻，需重新审核')
        if not str(plan.get('promotion_trigger') or '').strip():
            raise ValueError('缺少明确进场触发条件')
        # Direct decimal arithmetic; never round a failing boundary into a pass.
        target = _num(pc['take_profit_range'][0])
        buy = 1 + _num(pc['buy_cost_pct']) / 100
        sell = 1 - _num(pc['sell_cost_pct']) / 100
        cap = (target + MIN_NET_RR * stop) * sell / ((1 + MIN_NET_RR) * buy)
        floor = lo
        out['net_rr_price_ceiling'] = float(cap)
        if horizon == 'short':
            trend = next((x for x in (card.get('books') or {}).get('checks', []) if x.get('id') == 'trend'), {})
            if trend.get('ok') is not True:
                raise ValueError('原短期趋势/反转书理未通过')
            ma20 = _evidence(card, 'market_evidence.ma20')
            if ma20 <= 0 or _evidence(card, 'last') != last:
                raise ValueError('趋势价格与原合同价格不一致')
            out['trend_reference'] = float(ma20)
            out['trend_condition'] = f'趋势恢复参考MA20 {ma20:g}；还须原量价确认'
            floor = max(floor, ma20)
            trigger = plan['promotion_trigger']
            fixed = re.match(r'收盘重新站上([0-9.]+)且当日量/此前20日均量≥1\.2', trigger)
            if fixed:
                floor = max(floor, Decimal(fixed.group(1)))
                out['entry_branch'] = '原一阶段量价确认'
            elif trigger.startswith('两阶段等待：') and '随后收盘突破前一日最高价' in trigger:
                out['entry_branch'] = '原两阶段企稳后突破'
            else:
                raise ValueError('当前入场模板尚未接入可行性核验')
        top = min(hi, cap)
        out['continuous_floor'], out['continuous_ceiling'] = float(floor), float(top)
        if floor > top:
            out['failure_kind']='NO_PATH'
            raise ValueError(f'趋势/原触发所需价格≥{floor:.4f}，但原区间与净RR≥2允许≤{top:.4f}，无共同价带；需新事实复核，不能追价')
        distance = max(floor / last - 1, last / top - 1, Decimal(0)) * 100
        out['distance_pct'] = float(distance)
        out['feasible_price_band'] = [float(floor), float(top)]
        from entry_week import assess as week_assess
        week = week_assess(row, out['feasible_price_band'], now=now, doc=week_doc)
        out['week_entry'] = week
        if not week['eligible']:
            if week.get('arrival_budget',1)<1 or week.get('reachable_range'):
                out['failure_kind']='OUTSIDE_WINDOW'
            raise ValueError(week['reason'])
        out['week_entry_range'] = week['entry_range']
        floor, top = map(_num, week['entry_range'])
        gaps = (row.get('value_assessment') or {}).get('remaining_conditions') or []
        if not gaps:
            gaps = (card.get('books') or {}).get('missing') or []
        out['maturity_gaps'] = [x.get('title') or x.get('label') or x.get('id') or '待核条件' for x in gaps if isinstance(x, dict)]
        in_band = floor <= last <= top
        central_buy = (row.get('formal_recommendation') is True and row.get('tier') == '3A'
                       and card.get('all_passed') is True
                       and not row.get('action_blocks') and in_band and week.get('capacity_passed') is True
                       and (row.get('execution') or {}).get('triggered') is True)
        out.update(focus_eligible=True, executable=central_buy,
                   state='READY' if central_buy else 'CONDITIONAL',
                   label='中央许可·本周核查成交' if central_buy else
                         '条件观察·已在本周区间' if in_band else '条件观察·本周等价位与确认',
                   reasons=[week['reason']])
    except (ValueError, TypeError, KeyError, ArithmeticError) as exc:
        out['reasons'] = [str(exc)]
    out['action_summary'] = action_summary(row, out)
    return out


def action_summary(row, result):
    """One entry explanation for desktop and digest; never infer a live fill."""
    plan=row.get('central_trade_plan') or row.get('trade_plan') or {}
    last=plan.get('last');band=result.get('week_entry_range') or []
    inside=(len(band)==2 and type(last) in (int,float) and math.isfinite(last)
            and band[0]<=last<=band[1])
    confirmed=(row.get('execution') or {}).get('triggered') is True
    if result.get('executable') is True:
        status,label,reason='READY','🟢入场条件已确认','核查实际成交价仍在原入场带'
    elif result.get('focus_eligible') is not True:
        status=result.get('failure_kind','EVIDENCE')
        label={'REJECTED':'× 审核未通过·本次不推荐',
               'NO_PATH':'× 入场条件无解·本次方案不可执行',
               'EXPIRED':'⌛ 原方案到期·停止等待',
               'INVALIDATED':'🛑 原方案失效·停止等待',
               'OUTSIDE_WINDOW':'↗ 超出本周入场窗口·不列本周推荐'}.get(status,'🔎研究跟踪·证据待补')
        if status=='EVIDENCE':status='RESEARCH'
        reason='；'.join(result.get('reasons') or ['本周入场依据待核'])
    elif not inside:
        status,label,reason='WAIT_PRICE','↔尚未到价','已核价格未在本周入场带；到价后核查原信号'
    elif not confirmed:
        status,label='WAIT_SIGNAL','👁已到观察区·信号未确认'
        reason=('原两阶段信号尚未完成：企稳后突破并放量' if str(plan.get('promotion_trigger') or '').startswith('两阶段等待：')
                else '原一阶段量价信号尚未确认；不额外加等两天')
    else:
        status,label,reason='WAIT_REVIEW','◉信号已确认·准入待核','价格与信号已满足，中央执行审核仍未全部通过'
    warnings=(result.get('week_entry') or {}).get('warnings') or []
    if warnings:reason+='；成交较薄，容量待核'
    dates=(result.get('week_entry') or {}).get('sessions') or []
    deadline=dates[-1] if dates else result.get('deadline')
    if status in {'WAIT_PRICE','WAIT_SIGNAL','WAIT_REVIEW'} and deadline:
        reason+='；本次窗口截至'+str(deadline)[:10]+'，未触发不计买入'
    if status in {'NO_PATH','EXPIRED','INVALIDATED','OUTSIDE_WINDOW','REJECTED'}:
        reason+='；原记录保留，须新事实与复审形成新方案，不能无限等待'
    return {'status':status,'label':label,'reason':reason,'deadline':deadline,
            'original_trigger':plan.get('promotion_trigger'),'price_in_band':inside,
            'signal_confirmed':confirmed,'capacity_passed':(result.get('week_entry') or {}).get('capacity_passed') is True,
            'quote_scope':'按已核行情判断，实际成交前核价','no_order_authority':True}


def html(result):
    esc = lambda value: escape(str(value))
    price = lambda value: f'{value:.4f}'.rstrip('0').rstrip('.')
    band = result.get('feasible_price_band') or []
    band_text = (' ～ '.join(price(x) for x in band)) if len(band) == 2 else '无可核验交集'
    week = result.get('week_entry') or {}
    action = result.get('action_summary') or {}
    sessions = week.get('sessions') or []
    window = (f'📅 {sessions[0]} ～ {sessions[-1]} · {len(sessions)}个交易日' if sessions else '📅 未来5个交易日 · 待核验')
    return (f"<div class='v88-entry-opportunity' data-entry-state='{esc(result.get('state','ARCHIVE'))}' style='font-size:11px;color:#475569'>"
            f"<b style='font-size:13px'>{esc(action.get('label') or result.get('label','研究跟踪'))}</b>"
            + f"<br>{esc(action.get('reason') or '')}<br><small>{esc(window)} · 按已核行情</small>"
            + (f"<br>🎯 {' ～ '.join(price(x) for x in result['week_entry_range'])}" if result.get('focus_eligible') and result.get('week_entry_range') else '')
            +
            f"<details><summary>本周入场核验 · {esc('通过' if week.get('eligible') else '未通过')}</summary>{esc('；'.join((result.get('reasons') or ['周内条件待核验']) + (week.get('warnings') or [])))}<br>"
            + esc(week.get('steps') or '先补齐同源行情与原入场步骤')
            + '<br>可达性依据：近120交易日不重叠5日块，至少20组；按到价预算取收盘幅度中位数。仅为历史波动情景估算，非到价概率；到价不代表成交。'
            + f"<br>同源日线 {esc(week.get('source_asof') or '待核')} · 样本 {esc(week.get('sample_n') or '待补')}组</details>"
            +
            f"<details><summary>进场必要条件与解除条件</summary>"
            f"可行观察价带：{band_text}（连续价格测算，未核交易单位；非新买单）<br>"
            + esc(result.get('trend_condition') or '沿用本周期原书理与入场条件')
            + '<br>净RR≥2按原止盈下沿、原止损及买卖各0.5%费用测算；原合同不改。'
            + '<br>原触发：' + esc(result.get('original_trigger') or '缺失')
            + '<br>审核待补：' + esc('；'.join(result.get('maturity_gaps') or []) or '参见原GPT/书理逐项条件')
            + '<br>仅当前3A许可且全部条件通过才进入可执行区；新事实、新审核或到期后重算。'
            + '<br>原截止：' + esc(result.get('deadline') or '待核') + '</details></div>')


def price_html(result, plan):
    """All consumers show the same bounded entry band; original prices fold away."""
    esc = lambda value: escape(str(value))
    def fmt(xs):
        if isinstance(xs,list) and len(xs)==2 and all(type(x) in (int,float) and math.isfinite(x) for x in xs):
            return ' ～ '.join(f'{x:.4f}'.rstrip('0').rstrip('.') for x in xs)
        return str(xs) if xs is not None else '待核验'
    week = result.get('week_entry') or {}
    dates = week.get('sessions') or []
    valid = result.get('focus_eligible') is True and week.get('eligible') is True
    label = '🎯 本周入场带' if valid else '⏸ 本周无合格入场带'
    return (f"<div class='v88-week-entry-price'><b>{label}</b>"
            + (f"<br><b style='color:#047857'>{esc(fmt(result.get('week_entry_range')))}</b>" if valid else '')
            + (f"<br><small>📅 {esc(dates[0][5:])}～{esc(dates[-1][5:])} · {len(dates)}交易日</small>" if dates else '')
            + f"<details style='font-size:11px'><summary>原研究区间与触发</summary>{esc(fmt(plan.get('entry_range')))}<br>{esc(plan.get('promotion_trigger'))}</details></div>")
