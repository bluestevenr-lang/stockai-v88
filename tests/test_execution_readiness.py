from copy import deepcopy
from datetime import datetime

import pytest

from execution_readiness import apply, readiness, stable_projection


def at(value):
    return datetime.fromisoformat(value)


def row(market='A股', quote='2026-11-16T10:00:00+08:00', session='2026-11-16'):
    return {'market': market, 'quote_asof': quote, 'source_session': session, 'last': 18.94,
            'entry_range': [18.7787, 18.94], 'stop_range': [17.7506, 17.8398],
            'missing': [], 'buyable': True, 'strategy_tier': '3A'}


def directive(**changes):
    return {'action': '立即买入', 'order': '限价', 'price': 18.94, 'stop': 17.84, 'target': 21.213, **changes}


@pytest.mark.parametrize('market,quote,session,now,status', [
    ('A股', '2026-11-16T15:41:21+08:00', '2026-11-16', '2026-11-16T16:20:00+08:00', '已收市'),
    ('美股', '2026-11-14T05:00:01+08:00', '2026-11-13', '2026-11-16T16:20:00+08:00', '未开市'),
    ('A股', '2026-11-13T15:00:00+08:00', '2026-11-13', '2026-11-14T10:00:00+08:00', '休市'),
    ('美股', '2026-11-25T16:00:00-05:00', '2026-11-25', '2026-11-26T10:00:00-05:00', '休市'),
    ('A股', '2026-11-16T11:30:00+08:00', '2026-11-16', '2026-11-16T12:00:00+08:00', '午间休市'),
    ('港股', '2026-11-16T12:00:00+08:00', '2026-11-16', '2026-11-16T12:30:00+08:00', '午间休市'),
    ('美股', '2026-11-27T13:00:00-05:00', '2026-11-27', '2026-11-27T13:01:00-05:00', '已收市'),
])
def test_closed_market_preserves_a_valid_reference_without_claiming_immediate_buy(market, quote, session, now, status):
    r = row(market, quote, session); original = deepcopy(r)
    result = readiness(r, directive(), at(now))
    assert result['state'] == 'market_closed'
    assert result['market_status'] == status
    assert result['reference_quote_valid'] and not result['executable_now']
    assert result['quote_asof'] == quote and r == original


@pytest.mark.parametrize('market,quote,now', [
    ('A股', '2026-11-16T10:00:00+08:00', '2026-11-16T10:01:00+08:00'),
    ('港股', '2026-11-16T10:00:00+08:00', '2026-11-16T10:01:00+08:00'),
    ('美股', '2026-11-16T09:31:00-05:00', '2026-11-16T09:32:00-05:00'),
])
def test_active_cash_session_with_fresh_quote_can_preserve_supplied_strategy_permission(market, quote, now):
    result = readiness(row(market, quote), directive(), at(now))
    assert result['state'] == 'ready' and result['executable_now']


def test_twenty_minute_boundary_matches_existing_freshness_policy():
    r = row(); d = directive()
    assert readiness(r, d, at('2026-11-16T10:20:00+08:00'))['executable_now']
    result = readiness(r, d, at('2026-11-16T10:20:01+08:00'))
    assert result['state'] == 'quote_pending' and not result['executable_now']


@pytest.mark.parametrize('quote', [None, 'bad', '2026-11-16T10:00:00', '2026-11-16T10:01:01+08:00', '2026-11-16T10:07:00+08:00'])
def test_missing_naive_or_future_quote_cannot_be_executable(quote):
    result = readiness(row(quote=quote), directive(), at('2026-11-16T10:01:00+08:00'))
    assert result['state'] == 'quote_pending' and not result['executable_now']


def test_prior_session_quote_cannot_become_live_by_its_fresh_capture_timestamp():
    r = row(session='2026-11-13')
    assert readiness(r, directive(), at('2026-11-16T10:01:00+08:00'))['state'] == 'quote_pending'


def test_same_quote_changes_authority_at_close_without_altering_original_plan():
    r = row(quote='2026-11-16T14:59:00+08:00'); d = directive(); original = deepcopy((r, d))
    assert readiness(r, d, at('2026-11-16T14:59:00+08:00'))['executable_now']
    result = readiness(r, d, at('2026-11-16T15:00:00+08:00'))
    assert result['state'] == 'market_closed' and not result['executable_now']
    assert (r, d) == original


def test_existing_stock_daily_freshness_blocker_survives_marketable_limit_order():
    r = row(); r['missing'] = ['多重核查：完整日线待更新']
    result = readiness(r, directive(action='限价买入'), at('2026-11-16T10:01:00+08:00'))
    assert result['state'] == 'data_pending' and not result['executable_now']
    r['missing'] = []
    r['crosscheck'] = {'version': 'strategy-crosscheck-v1', 'asof': '2026-11-12'}
    assert readiness(r, directive(), at('2026-11-16T10:01:00+08:00'))['state'] == 'data_pending'


def test_left_side_and_independent_review_warnings_are_not_new_execution_vetoes():
    r = row(); r.update(annual_regime='down', fundamentals_state='pending',
                       muse={'verdict':'unverified'}, ladder={'weak': True},
                       crosscheck_note='方向分歧；止损锚待核',
                       crosscheck={'version':'strategy-crosscheck-v1','asof':'2026-11-13',
                                   'alignment':{'state':'mixed','asof':'2026-11-10'},
                                   'technical':{'status':'INSUFFICIENT'},
                                   'stop_check':{'anchor_status':'unverified'}})
    result = readiness(r, directive(), at('2026-11-16T10:01:00+08:00'))
    assert result['executable_now'] and result['warnings'] == ['方向分歧；止损锚待核']


@pytest.mark.parametrize('changes', [
    {'action':'不买'}, {'action':'财报后买入'}, {'order':'条件单'},
    {'action':'挂单买入','order':'条件单'}, {'price':18.5}, {'price':None},
])
def test_fresh_quote_does_not_create_strategy_or_trigger_permission(changes):
    assert not readiness(row(), directive(**changes), at('2026-11-16T10:01:00+08:00'))['executable_now']


def test_unknown_calendar_and_naive_clock_are_not_guessed_as_open():
    assert readiness(row(market='未知'), directive(), at('2026-11-16T10:01:00+08:00'))['state'] == 'data_pending'
    assert readiness(row(), directive(), at('2026-11-16T10:01:00'))['state'] == 'data_pending'
    assert readiness(row(), directive(), at('2027-11-16T10:01:00+08:00'))['state'] == 'data_pending'


def test_waiting_projection_restores_same_plan_at_next_open_with_a_new_live_quote():
    r = row(quote='2026-11-16T15:00:00+08:00')
    original = directive(text='立即买入 · 限价 ≤ 18.94 · 当日有效', valid_until='2026-11-20',
                         size='轻仓1/3', size_pct=8.3, condition='不追价', why=['原信号条件匹配'],
                         canon='原研究依据', canon_id='CANON-ENTRY-01')
    untouched = deepcopy((r, original))
    waiting = apply(r, original, at('2026-11-16T16:00:00+08:00'))
    assert waiting['action'] == '等待开市'
    assert '立即买入' not in waiting['text'] and '今日可买' not in waiting['text']
    assert waiting['signal_plan'] == original and (r, original) == untouched
    for field in ('price', 'stop', 'target', 'valid_until', 'size', 'size_pct', 'condition'):
        assert waiting[field] == original[field]
    assert apply(r, waiting, at('2026-11-16T16:00:00+08:00')) == waiting
    fresh = {**r, 'quote_asof': '2026-11-17T09:31:00+08:00', 'source_session': '2026-11-17'}
    opened = apply(fresh, waiting, at('2026-11-17T09:32:00+08:00'))
    assert opened['execution_readiness']['executable_now']
    assert {k: v for k, v in opened.items() if k != 'execution_readiness'} == original
    assert 'signal_plan' not in opened


@pytest.mark.parametrize('action,text', [('不买', '不买：财务反证'), ('财报后买入', '财报后买入，先复核')])
def test_hard_or_earnings_override_never_restores_prior_actionable_plan(action, text):
    r = row(); blocked = directive(action=action, text=text, signal_plan=directive())
    result = apply(r, blocked, at('2026-11-16T10:01:00+08:00'))
    assert result['action'] == action and result['text'] == text
    assert not result['execution_readiness']['executable_now']
    assert apply(r, result, at('2026-11-16T10:01:00+08:00')) == result


@pytest.mark.parametrize('change,expected', [
    ({'quote_asof': None}, '等待核价'),
    ({'missing': ['多重核查：完整日线待更新']}, '等待核验'),
])
def test_waiting_projection_maps_evidence_blockers_without_mutating_numeric_contract(change, expected):
    r = {**row(), **change}; d = directive(valid_until='当日有效', size_notional_usd=1000)
    result = apply(r, d, at('2026-11-16T10:01:00+08:00'))
    assert result['action'] == expected and result['signal_plan'] == d
    for field in ('price', 'stop', 'target', 'valid_until', 'size_notional_usd'):
        assert result[field] == d[field]
    assert '不自动顺延' in result['text']


def test_trigger_projection_can_recheck_the_same_fixed_limit_without_broadening_it():
    r = row(); d = directive(action='限价买入', price=18.5)
    waiting = apply(r, d, at('2026-11-16T10:01:00+08:00'))
    assert waiting['action'] == '等待触发' and waiting['price'] == 18.5
    fresh = {**r, 'last': 18.4, 'quote_asof': '2026-11-16T10:02:00+08:00'}
    opened = apply(fresh, waiting, at('2026-11-16T10:02:00+08:00'))
    assert opened['action'] == '限价买入' and opened['price'] == 18.5


def test_stable_projection_removes_only_ephemeral_assessment_fields():
    first = readiness(row(), directive(), at('2026-11-16T10:01:00+08:00'))
    later = readiness(row(), directive(), at('2026-11-16T10:02:00+08:00'))
    assert first != later and stable_projection(first) == stable_projection(later)
    stable = stable_projection(first)
    assert stable['quote_asof'] == first['quote_asof']
    assert stable['source_session'] == first['source_session']
    assert first['assessed_at'] and first['quote_age_seconds'] == 60
    stale = readiness(row(), directive(), at('2026-11-16T10:21:00+08:00'))
    assert stable_projection(stale) != stable
