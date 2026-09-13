"""Read-only entry attention filter. No grades, contracts or orders are created.

Only numeric evidence already cited in the frozen reviews is consumed. Unknown
templates/evidence remain in the archive. Keep the desktop copy identical.
"""
from decimal import Decimal
from html import escape
import math
import re

from profit_contract import evaluate

VERSION = 'entry-opportunity-v1'
MAX_DISTANCE_PCT = 10  # Existing weekly attention distance, not a forecast.
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


def assess(row, *, now=None):
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
               state='ARCHIVE', label='暂不推荐·保留跟踪', reasons=[],
               original_entry_range=plan.get('entry_range'), feasible_price_band=[],
               original_trigger=plan.get('promotion_trigger'), no_grade_authority=True,
               model_calls=0, minimum_net_rr=MIN_NET_RR)
    try:
        if row.get('tier') not in ('1A', '2A', '3A') or not all(
                (card.get(k) or {}).get('current') is True and
                (card.get(k) or {}).get('complete') is True for k in ('gpt', 'books')):
            raise ValueError('当前评级审核缺失或过期，仅保留原研究档案')
        pc = evaluate(plan, horizon, now=now)
        out['deadline'] = pc.get('thesis_deadline')
        if not pc.get('valid') or not pc.get('eligible'):
            raise ValueError('原收益合同未通过：' + '；'.join(pc.get('reasons') or []))
        last, stop = _num(plan.get('last')), _num(plan.get('stop'))
        lo, hi = map(_num, plan['entry_range'])
        if last <= stop:
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
            raise ValueError(f'趋势/原触发所需价格≥{floor:.4f}，但原区间与净RR≥2允许≤{top:.4f}，无共同价带；需新事实复核，不能追价')
        distance = max(floor / last - 1, last / top - 1, Decimal(0)) * 100
        out['distance_pct'] = float(distance)
        out['feasible_price_band'] = [float(floor), float(top)]
        if distance > MAX_DISTANCE_PCT:
            raise ValueError(f'距可行观察价带{distance:.2f}%超过10%关注范围，保留长期跟踪')
        gaps = (row.get('value_assessment') or {}).get('remaining_conditions') or []
        if not gaps:
            gaps = (card.get('books') or {}).get('missing') or []
        out['maturity_gaps'] = [x.get('title') or x.get('label') or x.get('id') or '待核条件' for x in gaps if isinstance(x, dict)]
        in_band = floor <= last <= top
        central_buy = (row.get('formal_recommendation') is True and row.get('tier') == '3A'
                       and card.get('all_passed') is True
                       and not row.get('action_blocks') and in_band
                       and (row.get('execution') or {}).get('triggered') is True)
        out.update(focus_eligible=True, executable=central_buy,
                   state='READY' if central_buy else 'CONDITIONAL',
                   label='中央许可·按原合同核查成交' if central_buy else
                         '条件观察·等量价与审核' if in_band else '条件观察·等价位与确认',
                   reasons=['原区间、趋势参考与扣费风险比存在交集；不代表后续一定到价或成交'])
    except (ValueError, TypeError, KeyError, ArithmeticError) as exc:
        out['reasons'] = [str(exc)]
    return out


def html(result):
    esc = lambda value: escape(str(value))
    price = lambda value: f'{value:.4f}'.rstrip('0').rstrip('.')
    band = result.get('feasible_price_band') or []
    band_text = (' ～ '.join(price(x) for x in band)) if len(band) == 2 else '无可核验交集'
    return (f"<div class='v88-entry-opportunity' data-entry-state='{esc(result['state'])}' style='font-size:11px;color:#475569'>"
            f"<b>{esc(result['label'])}</b><br>{esc('；'.join(result['reasons']))}"
            f"<details><summary>进场必要条件与解除条件</summary>"
            f"可行观察价带：{band_text}（连续价格测算，未核交易单位；非新买单）<br>"
            + esc(result.get('trend_condition') or '沿用本周期原书理与入场条件')
            + '<br>净RR≥2按原止盈下沿、原止损及买卖各0.5%费用测算；原合同不改。'
            + '<br>原触发：' + esc(result.get('original_trigger') or '缺失')
            + '<br>审核待补：' + esc('；'.join(result.get('maturity_gaps') or []) or '参见原GPT/书理逐项条件')
            + '<br>仅当前3A许可且全部条件通过才进入可执行区；新事实、新审核或到期后重算。'
            + '<br>原截止：' + esc(result.get('deadline') or '待核') + '</details></div>')
