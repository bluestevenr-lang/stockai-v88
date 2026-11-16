"""Time-specific execution readiness; never grants a strategy grade or edits a plan.

Twenty-minute quote freshness matches strategy_board_state.quote_timing. A valid
prior close remains research data while the exchange is shut; it is not a live
quote. Only already-blocking stock-data freshness gaps are enforced here. Muse,
market/sector disagreement, yearly trend and optional Classics checks are not
new vetoes. ``assessed_at`` is a viewer clock, not an immutable evidence fact.
"""
from copy import deepcopy
from datetime import datetime, time, timedelta, timezone
import math
from zoneinfo import ZoneInfo

from exchange_sessions import HALF
from market_session_view import session_view

VERSION = 'execution-readiness-2026-11-16-v1'
QUOTE_MAX_AGE_SECONDS = 20 * 60
DATA_GAPS = ('最新收盘日线待更新', '多重核查：完整日线待更新')
QUOTE_GAPS = ('最新交易日报价', '入场前更新报价')
BUY_ACTIONS = ('立即买入', '限价买入', '挂单买入')
WAIT_ACTIONS = ('等待开市', '等待核价', '等待核验', '等待触发')
WAIT_BY_STATE = {'market_closed': '等待开市', 'quote_pending': '等待核价',
                 'data_pending': '等待核验', 'waiting_trigger': '等待触发'}


def _number(value):
    return type(value) in (int, float) and math.isfinite(value) and value > 0


def _clock(value):
    stamp = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError('timezone required')
    return stamp


def _close_reference(view, market, zone):
    day = datetime.fromisoformat(view['completed_session']).date()
    minute = 15 * 60 if market == 'A股' else 16 * 60
    if day.strftime('%m-%d') in HALF[market]:
        minute = 13 * 60 if market == '美股' else 12 * 60
    if view['market_status'] == '午间休市':
        day = datetime.fromisoformat(view['local_date']).date()
        minute = 11 * 60 + 30 if market == 'A股' else 12 * 60
    return day.isoformat(), datetime.combine(day, time(minute // 60, minute % 60), zone)


def readiness(row, directive, now=None):
    """Assess the supplied instruction without changing signal, grade, or price.

    ``executable_now`` means the supplied buy limit is marketable using a fresh
    current-session quote during cash trading, with no existing data blocker.
    It is not a fill, broker/account validation, win probability or new approval.
    """
    now = now or datetime.now(timezone.utc)
    directive = directive or {}
    result = {
        'version': VERSION, 'state': 'data_pending', 'label': '数据待核',
        'executable_now': False, 'reasons': [], 'warnings': [],
        'assessed_at': now.isoformat() if isinstance(now, datetime) and now.tzinfo is not None else None,
        'market_status': None, 'attention_session': None, 'completed_session': None,
        'quote_asof': row.get('quote_asof'), 'source_session': row.get('source_session'),
        'quote_age_seconds': None, 'quote_status': 'unknown',
        'quote_fresh_for_execution': False, 'reference_quote_valid': False,
        'quote_max_age_seconds': QUOTE_MAX_AGE_SECONDS,
        'scope': '当前时点的时段/报价/数据核对，不等于成交、收益保证或独立复核通过',
    }
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        result['reasons'] = ['核查时间缺时区，无法核对交易时段']
        return result
    market = row.get('market')
    try:
        view = session_view(market, now)
        zone = ZoneInfo('America/New_York' if market == '美股' else 'Asia/Shanghai')
    except (ValueError, KeyError, TypeError):
        result['reasons'] = ['交易市场或交易所日历待核']
        return result
    result.update({key: view[key] for key in ('market_status', 'attention_session', 'completed_session')})

    # A new quote can refresh readiness but never renew the frozen contract.
    deadlines = [directive.get('valid_until'), row.get('monthly_entry_deadline'),
                 (row.get('window') or {}).get('deadline')]
    for deadline in deadlines:
        try:
            day = datetime.fromisoformat(str(deadline)[:10]).date().isoformat()
        except (ValueError, TypeError):
            continue  # Relative 当日有效 has no inferred or invented origin date.
        if day < view['local_date']:
            result.update(state='expired', label='计划已到期',
                          reasons=[f'原计划截止 {day}，新报价不延长原期限'])
            return result

    missing = [str(value) for value in row.get('missing') or []]
    data_gaps = [value for value in missing if value.startswith(DATA_GAPS)]
    quote_gaps = [value for value in missing if value.startswith(QUOTE_GAPS)]
    # This is the stock's own complete-session evidence, not the minimum date of
    # optional market/sector context used by the disclosure panel.
    cross = row.get('crosscheck') or {}
    if cross.get('version') == 'strategy-crosscheck-v1' and cross.get('asof') != view['completed_session']:
        data_gaps.append('多重核查：完整日线待更新')
    result['warnings'] = [value for value in missing if value not in data_gaps + quote_gaps]
    if row.get('crosscheck_note'):
        result['warnings'].append(str(row['crosscheck_note']))

    try:
        quoted = _clock(row.get('quote_asof'))
        age = (now - quoted).total_seconds()
        result['quote_age_seconds'] = round(age, 3)
        if age < 0:  # Preserve state.view's existing strict future-quote rejection.
            quote_gaps.append('报价时间在未来，须核对真实行情时间')
        elif not _number(row.get('last')):
            quote_gaps.append('缺有效现价')
        elif view['market_status'] == '交易中':
            if row.get('source_session') != view['local_date'] or quoted.astimezone(zone).date().isoformat() != view['local_date']:
                quote_gaps.append('当前交易时段缺本交易日报价')
            elif age > QUOTE_MAX_AGE_SECONDS:
                quote_gaps.append('盘中报价超过20分钟，需刷新后核价')
            else:
                result.update(quote_status='live', reference_quote_valid=True, quote_fresh_for_execution=True)
        else:
            session, close = _close_reference(view, market, zone)
            if (row.get('source_session') == session
                    and quoted.astimezone(zone).date().isoformat() == session
                    and quoted >= close - timedelta(seconds=QUOTE_MAX_AGE_SECONDS)):
                result.update(quote_status='latest_close', reference_quote_valid=True)
            else:
                quote_gaps.append('缺对应最近收盘时段的有效报价，开市前需更新')
    except (ValueError, TypeError, OverflowError):
        quote_gaps.append('报价时间缺失、格式错误或缺时区')

    if data_gaps:
        result['reasons'] = list(dict.fromkeys(data_gaps + quote_gaps))
        return result
    if quote_gaps:
        result.update(state='quote_pending', label='报价待更新', quote_status='stale',
                      quote_fresh_for_execution=False, reasons=list(dict.fromkeys(quote_gaps)))
        return result
    if view['market_status'] != '交易中':
        result.update(state='market_closed', label='待开市核价',
                      reasons=[f'{view["market_status"]}；保留筛选计划，恢复交易后须以新报价核价'])
        return result
    if directive.get('action') not in BUY_ACTIONS:
        result.update(state='not_actionable', label='等待策略条件', reasons=['本条策略指令未给出买入许可'])
        return result
    if directive.get('order') == '条件单' or directive.get('guard'):
        result.update(state='waiting_trigger', label='等待触发', reasons=['条件单尚待触发并核对，不是即时买入'])
        return result
    if not _number(directive.get('price')):
        result.update(state='data_pending', label='数据待核', reasons=['缺有效指令限价'])
        return result
    if directive['price'] < row['last'] - 1e-9:
        result.update(state='waiting_trigger', label='等待触发', reasons=['当前报价高于限价，等待回落，不追价'])
        return result
    result.update(state='ready', label='策略条件匹配', executable_now=True,
                  reasons=['交易时段与本交易日报价已核对；仍按原限价和失效条件执行'])
    return result


def stable_projection(metadata):
    """Keep evidence clocks and states; remove only the moving viewer clock/age."""
    out = deepcopy(metadata or {})
    out.pop('assessed_at', None)
    out.pop('quote_age_seconds', None)
    return out


def apply(row, directive, now=None):
    """Project readiness onto an instruction while preserving its original plan.

    Only this helper's waiting actions may restore ``signal_plan``. A financial
    audit overriding an old waiting instruction to 不买 must stay authoritative.
    The caller owns the strategy audit and immutable publication; this helper
    never mutates either input or extends any price/stop/target/deadline.
    """
    original = deepcopy(directive or {})
    if original.get('action') in WAIT_ACTIONS and isinstance(original.get('signal_plan'), dict):
        original = deepcopy(original['signal_plan'])
    original.pop('execution_readiness', None)
    original.pop('signal_plan', None)
    assessment = readiness(row, original, now)
    projected = deepcopy(original)
    projected['execution_readiness'] = assessment
    if original.get('action') not in BUY_ACTIONS or assessment['executable_now']:
        return projected

    action = WAIT_BY_STATE.get(assessment['state'], '等待核验')
    projected['signal_plan'] = deepcopy(original)
    projected['action'] = action
    projected['why'] = ['原指令保留为研究计划；当前执行前置条件未满足', *assessment['reasons']]
    projected['canon'] = '执行核验前保留原研究计划；不自动改价、顺延期限或把条件匹配当作成交'
    projected['canon_id'] = 'CANON-EXECUTION-WAIT-01'
    parts = [action]
    for field, label in (('price', '研究限价 ≤ '), ('stop', '原止损 '), ('target', '原目标 ')):
        if _number(original.get(field)):
            parts.append(label + str(original[field]))
    if original.get('valid_until'):
        parts.append('原期限 ' + str(original['valid_until']) + '（不自动顺延）')
    if assessment['reasons']:
        parts.append('；'.join(assessment['reasons']))
    projected['text'] = ' · '.join(parts)
    return projected
