"""Owner-defined entry windows, separate from holding plans and win-rate goals.

Uses existing completed-session technical facts and execution_readiness. No
forecast, ATR fill-probability estimate, regrading or alteration of old orders.
"""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import math
from zoneinfo import ZoneInfo

from exchange_sessions import is_session, latest_completed, next_session
from execution_readiness import BUY_ACTIONS, readiness, stable_projection
from market_session_view import session_view

VERSION = 'entry-window-policy-2026-11-16-v1'
PUBLIC_ACTIONS = ('研究候选', '策略条件匹配', '等待开市', '等待核价', '等待核验', '等待触发')
RELATIVE_VALIDITY = ('当日有效', '今日有效')
PRICE_PROTECTION_CONDITIONS = ('开盘价高于限价不追', '价格不高于此价', '价格不高于计划入场上沿')


def _price_protection_only(condition):
    if not condition:
        return True
    if not isinstance(condition, str):
        return False
    parts = condition.replace(';', '；').split('；')
    return all(part.strip().rstrip('。') in PRICE_PROTECTION_CONDITIONS for part in parts)


def _number(value):
    return type(value) in (int, float) and math.isfinite(value) and value > 0


def _rising_pivots(structure, asof):
    """Require actual confirmed highs/lows, not an unbacked trend string."""
    if structure.get('trend') != 'higher_highs_higher_lows':
        return False
    for key in ('confirmed_highs', 'confirmed_lows'):
        points = structure.get(key) or []
        if len(points) < 2:
            return False
        prior, current = points[-2:]
        if not all(_number(p.get('price')) and isinstance(p.get('date'), str)
                   and isinstance(p.get('confirmed_at'), str)
                   and p['date'] <= p['confirmed_at'] <= asof for p in (prior, current)):
            return False
        if prior['date'] >= current['date'] or prior['price'] >= current['price']:
            return False
    return True


def trend_assessment(row, now):
    """Medium/long upward evidence from real board or screening structures.

    strategy_board carries crosscheck.technical; screen_history rows carry
    structure.ma20/ma60/ma60_prev10. universe_scan buckets have neither and
    cannot establish this condition from chg60 or an annual-regime label.
    """
    technical = (row.get('crosscheck') or {}).get('technical') or {}
    source = 'crosscheck.technical' if technical else 'structure'
    structure = technical.get('structure') or {} if technical else row.get('structure') or {}
    asof = technical.get('source_asof') or technical.get('asof') if technical else structure.get('source_asof')
    required = latest_completed(row.get('market'), now).isoformat()
    out = {'required': True, 'upward': False, 'status': 'pending', 'source': source,
           'source_asof': asof, 'required_completed_session': required,
           'ma20': structure.get('ma20'), 'ma60': structure.get('ma60'),
           'structure_trend': structure.get('trend'), 'reasons': [],
           'scope': '同一OHLCV的中长期走势描述；不是独立投票或获胜概率'}
    if asof != required or (technical and technical.get('status') != 'OBSERVED'):
        out['reasons'] = ['缺本市场最近完整交易日的个股趋势证据']
        return out
    ma20, ma60 = out['ma20'], out['ma60']
    if not all(_number(v) for v in (ma20, ma60)):
        out['reasons'] = ['缺同口径MA20/MA60，不能由低价或年线标签证明中长期向上']
        return out
    if technical:
        gmma = technical.get('gmma') or {}
        slope = (gmma.get('slopes') or {}).get('long') or {}
        change = slope.get('five_session_change_pct')
        rising_ma = (gmma.get('status') == 'OBSERVED' and slope.get('direction') == 'up'
                     and type(change) in (int, float) and math.isfinite(change) and change > 0)
        out.update(long_group_direction=slope.get('direction'),
                   long_group_change_pct=change, group_relationship=gmma.get('group_relationship'))
        rising_pivots = _rising_pivots(structure, asof)
        # An explicitly declining long ribbon cannot be rescued by a brief
        # pair of higher short-term pivots.
        direction_conflict = slope.get('direction') == 'down'
    else:
        previous = structure.get('ma60_prev10')
        rising_ma = _number(previous) and ma60 > previous
        out.update(ma60_prev10=previous)
        rising_pivots = False
        direction_conflict = _number(previous) and ma60 < previous
    current = row.get('last')
    price_holds_ma60 = _number(current) and current >= ma60
    out.update(ma20_above_ma60=ma20 > ma60, long_average_rising=bool(rising_ma),
               reference_price=current, price_at_or_above_ma60=price_holds_ma60,
               confirmed_structure_rising=rising_pivots)
    out['upward'] = bool(ma20 > ma60 and price_holds_ma60 and not direction_conflict and (rising_ma or rising_pivots))
    out['status'] = 'observed_up' if out['upward'] else 'not_up'
    out['reasons'] = (['MA20高于MA60，现价保持MA60之上，且长期均线组上行或已确认高低点抬升'] if out['upward'] else
                      ['尚未形成MA20高于MA60、现价不低于MA60并有中长期上行依据的结构'])
    return out


def _original(directive):
    out = deepcopy(directive or {})
    # Never unwrap a fresh financial veto such as 不买.
    while out.get('action') in PUBLIC_ACTIONS and isinstance(out.get('signal_plan'), dict):
        out = deepcopy(out['signal_plan'])
    return out


def _deadlines(row, directive, now, zone):
    values = [directive.get('valid_until'), row.get('monthly_entry_deadline'),
              (row.get('window') or {}).get('deadline')]
    days, problems = [], []
    for value in values:
        if value is None or value in RELATIVE_VALIDITY:
            continue
        try:
            if len(str(value)) == 10:
                day = date.fromisoformat(str(value))
            else:
                stamp = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
                if stamp.tzinfo is None or stamp.utcoffset() is None:
                    raise ValueError('timezone missing')
                day = stamp.astimezone(zone).date()
                if stamp <= now:
                    problems.append('原指令已到期，不能延长原有效时间')
            days.append(day)
        except (ValueError, TypeError):
            problems.append('原计划期限无法核对')
    if not days and directive.get('valid_until') not in RELATIVE_VALIDITY:
        problems.append('缺原计划有效期')
    return days, problems


def assess(row, directive, now=None, module='3A'):
    """Eligibility for the main recommendation; exclusions retain full reasons.

    expiry is the entry-window boundary, never a rewritten order deadline.
    Closed-market eligibility is an inactive plan requiring a new quote/order;
    executable_now stays false. The caller retains all excluded research rows.
    """
    now = now or datetime.now(timezone.utc)
    original = _original(directive)
    result = {'version': VERSION, 'module': module, 'eligible': False,
              'label': f'{module}·入场窗口待满足', 'reasons': [],
              'execution_date': None, 'planned_session': None, 'planned_date': None,
              'entry_deadline': None, 'expiry': None,
              'original_valid_until': original.get('valid_until'),
              'executable_now': False, 'requires_new_session_quote': False,
              'requires_new_instruction': False, 'description': '',
              'trend_evidence': {'required': module == '3A', 'upward': None},
              'scope': '入场时间与走势条件，不是成交概率；90%仍为提升目标，不作资格硬门槛'}
    if module not in ('3A', '2A', '1A', 'Astra'):
        result['reasons'] = ['未知模块，不套用其他周期']
        return result
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        result['reasons'] = ['核查时间缺时区']
        return result
    market = row.get('market')
    try:
        view = session_view(market, now)
        today = date.fromisoformat(view['local_date'])
        planned = date.fromisoformat(view['attention_session'])
        zone = ZoneInfo('America/New_York' if market == '美股' else 'Asia/Shanghai')
        if module == '3A':
            start = today if is_session(today, market) else planned
            expiry = next_session(start, market)
        elif module == '2A':
            expiry = today + timedelta(days=6 - today.weekday())
            while not is_session(expiry, market):
                expiry -= timedelta(days=1)
        else:
            expiry = today if module == '1A' else planned
        result.update(execution_date=planned.isoformat(), planned_session=planned.isoformat(),
                      planned_date=planned.isoformat(), expiry=expiry.isoformat(),
                      entry_deadline=expiry.isoformat())
        ready = readiness(row, original, now)
        result['execution_readiness'] = stable_projection(ready)
        if module == '3A':
            result['trend_evidence'] = trend_assessment(row, now)
    except (ValueError, KeyError, TypeError, OverflowError):
        result['reasons'] = ['市场或交易所日历/完整日线证据待核']
        return result

    reasons = result['reasons']
    hold = row.get('max_sessions')
    if hold is None:
        hold = (row.get('ladder') or {}).get('hold_sessions')
    result['hold_sessions'] = hold
    valid_hold = _number(hold) and float(hold).is_integer()
    if module != 'Astra' and not valid_hold:
        reasons.append('缺原计划持有交易日数，不能自动改周期填档位')
    elif module == '3A' and hold < 20:
        reasons.append('3A要求原持有期至少20个交易日')
    elif module == '2A' and not 5 < hold <= 20:
        reasons.append('2A要求原持有期大于5且不超过20个交易日')
    elif module == '1A' and hold > 5:
        reasons.append('1A要求原持有期不超过5个交易日')
    if module == '3A' and not result['trend_evidence']['upward']:
        reasons.extend(result['trend_evidence']['reasons'])

    days, deadline_problems = _deadlines(row, original, now, zone)
    reasons.extend(deadline_problems)
    if days:
        deadline = min(days)
        result['expiry'] = min(expiry, deadline).isoformat() if module != 'Astra' else deadline.isoformat()
        entry_deadline = min(expiry, deadline)
        try:
            while not is_session(entry_deadline, market):
                entry_deadline -= timedelta(days=1)
            result['entry_deadline'] = entry_deadline.isoformat()
        except ValueError:
            reasons.append('入场截止超出已核交易所日历')
        if planned > deadline or (module == 'Astra' and planned >= deadline):
            reasons.append('原绝对截止不覆盖可入场交易日及所需后续期限')
    elif module == 'Astra':
        reasons.append('缺Astra原月度截止，不能推定新的有效期')
    if module in ('3A', '2A') and planned > expiry:
        reasons.append('下一可交易日超出今明窗口' if module == '3A' else '下一可交易日不在本周')

    if original.get('action') not in BUY_ACTIONS:
        reasons.append('原策略没有有效买入计划')
    if (original.get('order') == '条件单' or original.get('guard')
            or not _price_protection_only(original.get('condition'))):
        reasons.append('原条件单尚待触发，不列近期可入场主推荐')
    if original.get('order') != '限价':
        reasons.append('缺可核对的原限价执行方式')
    if any((row.get(key) or {}).get('blocked') for key in ('entry_gate', 'live_gate', 'entry_location')):
        reasons.append('现存入场硬门仍阻断')
    if row.get('profit_gaps'):
        reasons.append('原计划盈利/风险硬缺口未补齐')
    band = row.get('entry_range')
    last, limit = row.get('last'), original.get('price')
    if (not isinstance(band, (list, tuple)) or len(band) != 2
            or not all(_number(v) for v in [*band, last, limit]) or band[0] > band[1]):
        reasons.append('缺有效原入场价带、报价或限价')
    elif not (band[0] <= last <= limit <= band[1]):
        reasons.append('有效报价与原限价未同时落入原入场带；不以ATR距离推断会成交')
    if ready['state'] not in ('ready', 'market_closed'):
        reasons.extend(ready.get('reasons') or ['当前执行证据未满足'])
    if ready['state'] == 'market_closed' and not ready.get('reference_quote_valid'):
        reasons.append('缺最近对应收盘时段的有效参考报价')
    if module == '1A' and not (ready.get('executable_now') and planned == today):
        reasons.append('1A只列今天当前可执行；休市或等待计划留后台')
    if reasons:
        result['reasons'] = list(dict.fromkeys(reasons))
        return result

    result['eligible'] = True
    result['executable_now'] = bool(ready.get('executable_now'))
    if result['executable_now']:
        result['label'] = {'3A': '3A·今明入场窗口', '2A': '2A·本周入场窗口',
                           '1A': '1A·今日当前条件匹配', 'Astra': 'Astra·当前入场条件匹配'}[module]
        result['description'] = '当前时段与报价已核对，按原价带、止损、目标和期限；不等于成交'
    else:
        result['requires_new_session_quote'] = True
        result['requires_new_instruction'] = original.get('valid_until') in RELATIVE_VALIDITY
        result['label'] = f'{module}·' + ('今日恢复交易后核价' if planned == today else '下一交易日待开市核价')
        result['description'] = ('下一交易日开市核价后按计划限价，原指令不自动续期' if planned != today else
                                 '恢复交易后以新报价核对原计划；当前不是即时买入')
    return result
