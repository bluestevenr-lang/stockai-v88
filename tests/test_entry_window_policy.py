"""Holding horizon, entry timing and research plans remain separate."""
from copy import deepcopy
from datetime import datetime

import pytest

from entry_window_policy import assess
from exchange_sessions import latest_completed


def fixture(now='2026-11-16T10:00:00+08:00', *, hold=40, market='A股', quote=None):
    now = datetime.fromisoformat(now)
    completed = latest_completed(market, now).isoformat()
    quote = quote or '2026-11-16T09:55:00+08:00'
    row = {
        'code': '600001.SS', 'market': market, 'strategy_tier': '3A',
        'max_sessions': hold, 'ladder': {'hold_sessions': hold},
        'last': 100, 'quote_asof': quote, 'source_session': quote[:10],
        'entry_range': [99, 101], 'stop_range': [90, 92], 'take_profit_range': [110, 115],
        'entry_gate': {'blocked': False}, 'profit_gaps': [], 'missing': [],
        'annual_regime': 'range',
        'crosscheck': {'version': 'strategy-crosscheck-v1', 'asof': completed,
                      'technical': {'status': 'OBSERVED', 'asof': completed,
                                    'source_asof': completed,
                                    'structure': {'ma20': 98, 'ma60': 95, 'trend': 'mixed_or_range'},
                                    'gmma': {'status': 'OBSERVED', 'group_relationship': 'short_above_long',
                                             'slopes': {'long': {'direction': 'up', 'five_session_change_pct': 1.2}}}}},
    }
    directive = {'action': '立即买入', 'order': '限价', 'price': 101,
                 'stop': 92, 'target': 110, 'valid_until': '当日有效', 'condition': ''}
    return row, directive, now


def test_current_three_a_uses_real_medium_term_evidence_not_a_yearly_label():
    row, directive, now = fixture()
    before = deepcopy((row, directive))
    result = assess(row, directive, now)
    assert result['eligible'] and result['executable_now']
    assert result['execution_date'] == '2026-11-16'
    assert result['entry_deadline'] == '2026-11-17'
    assert result['trend_evidence']['upward']
    assert result['hold_sessions'] == 40
    assert result['original_valid_until'] == '当日有效'
    assert (row, directive) == before


@pytest.mark.parametrize('changes', ['year_only', 'cheap_below_ma', 'long_declining', 'old_bars', 'future_bars', 'missing_ma'])
def test_three_a_does_not_promote_weak_or_missing_trend_evidence(changes):
    row, directive, now = fixture()
    row['annual_regime'] = 'up'
    t = row['crosscheck']['technical']
    if changes == 'year_only':
        row.pop('crosscheck')
    elif changes == 'cheap_below_ma':
        t['structure'].update(ma20=90, ma60=95)
    elif changes == 'long_declining':
        t['gmma']['slopes']['long'].update(direction='down', five_session_change_pct=-1)
    elif changes == 'old_bars':
        t['source_asof'] = '2026-11-12'
    elif changes == 'future_bars':
        t['source_asof'] = '2026-11-16'
    elif changes == 'missing_ma':
        t['structure'].pop('ma60')
    result = assess(row, directive, now)
    assert not result['eligible']
    assert not result['trend_evidence']['upward']


def test_screen_history_actual_ma_fields_are_an_alternative_dated_source():
    row, directive, now = fixture()
    row.pop('crosscheck')
    row['structure'] = {'source_asof': '2026-11-13', 'ma20': 98, 'ma60': 95,
                        'ma60_prev10': 94, 'annual': {'regime': 'range'}}
    result = assess(row, directive, now)
    assert result['eligible']
    assert result['trend_evidence']['source'] == 'structure'
    row['structure'].pop('ma60_prev10')
    assert not assess(row, directive, now)['eligible']


def test_confirmed_higher_pivots_need_real_dates_and_values():
    row, directive, now = fixture()
    t = row['crosscheck']['technical']
    t['gmma'] = {}
    t['structure'].update(trend='higher_highs_higher_lows',
                          confirmed_highs=[{'price': 103, 'date': '2026-10-27', 'confirmed_at': '2026-10-29'},
                                           {'price': 106, 'date': '2026-11-05', 'confirmed_at': '2026-11-09'}],
                          confirmed_lows=[{'price': 91, 'date': '2026-10-23', 'confirmed_at': '2026-10-27'},
                                          {'price': 94, 'date': '2026-11-02', 'confirmed_at': '2026-11-04'}])
    assert assess(row, directive, now)['eligible']
    t['structure']['confirmed_lows'][-1]['confirmed_at'] = '2026-11-17'
    assert not assess(row, directive, now)['eligible']


@pytest.mark.parametrize('module,hold,expected', [
    ('3A', 20, True), ('3A', 40, True), ('3A', 15, False),
    ('2A', 5, False), ('2A', 6, True), ('2A', 15, True), ('2A', 20, True), ('2A', 40, False),
    ('1A', 1, True), ('1A', 5, True), ('1A', 6, False),
    ('3A', None, False), ('2A', None, False), ('1A', None, False),
])
def test_original_holding_period_is_never_rewritten_to_fill_a_tier(module, hold, expected):
    row, directive, now = fixture(hold=hold)
    assert assess(row, directive, now, module)['eligible'] is expected
    assert row['max_sessions'] == hold


def test_two_a_does_not_inherit_the_three_a_trend_requirement():
    row, directive, now = fixture(hold=15)
    row['crosscheck']['technical']['structure'].update(ma20=90, ma60=95)
    row['crosscheck']['technical']['gmma']['slopes']['long']['direction'] = 'down'
    result = assess(row, directive, now, '2A')
    assert result['eligible'] and result['entry_deadline'] == '2026-11-20'
    assert result['trend_evidence']['required'] is False


def test_three_a_allows_a_pullback_below_ma20_but_not_a_break_below_ma60():
    row, directive, now = fixture()
    row['crosscheck']['technical']['structure']['ma20'] = 102
    assert assess(row, directive, now)['eligible']
    row['crosscheck']['technical']['structure'].update(ma20=104, ma60=101)
    result = assess(row, directive, now)
    assert not result['eligible']
    assert not result['trend_evidence']['price_at_or_above_ma60']


@pytest.mark.parametrize('condition', ['开盘价高于限价不追', '价格不高于此价', '价格不高于计划入场上沿',
                                       '开盘价高于限价不追；价格不高于此价'])
def test_pure_price_protection_is_satisfied_only_within_the_existing_band(condition):
    row, directive, now = fixture()
    directive['condition'] = condition
    assert assess(row, directive, now)['eligible']
    directive['price'] = 99
    assert not assess(row, directive, now)['eligible']


@pytest.mark.parametrize('condition', ['开盘价高于限价不追；等待大盘转好', '价格不高于此价且财报通过',
                                       '等待回调确认', '突破区间下沿触发'])
def test_unknown_extra_trigger_is_never_removed_as_price_protection(condition):
    row, directive, now = fixture()
    directive['condition'] = condition
    assert not assess(row, directive, now)['eligible']


def test_closed_three_a_is_inactive_next_session_plan_not_renewed_day_order():
    row, directive, now = fixture('2026-11-16T16:30:00+08:00', quote='2026-11-16T15:05:00+08:00')
    result = assess(row, directive, now)
    assert result['eligible'] and not result['executable_now']
    assert result['planned_session'] == '2026-11-17'
    assert result['entry_deadline'] == '2026-11-17'
    assert result['requires_new_session_quote'] and result['requires_new_instruction']
    assert result['description'] == '下一交易日开市核价后按计划限价，原指令不自动续期'
    assert directive['valid_until'] == result['original_valid_until'] == '当日有效'


@pytest.mark.parametrize('module,expected', [('3A', True), ('2A', False), ('1A', False)])
def test_friday_close_cannot_call_next_week_this_week_or_today(module, expected):
    row, directive, now = fixture('2026-11-20T16:30:00+08:00', hold={'3A': 40, '2A': 15, '1A': 5}[module],
                                   quote='2026-11-20T15:05:00+08:00')
    result = assess(row, directive, now, module)
    assert result['eligible'] is expected
    assert result['planned_session'] == '2026-11-23'
    assert not result['executable_now']
    assert result['entry_deadline'] == ('2026-11-23' if module == '3A' else '2026-11-20')


def test_monday_before_us_open_uses_friday_close_without_claiming_immediate_execution():
    row, directive, now = fixture('2026-11-16T16:30:00+08:00', market='美股',
                                   quote='2026-11-13T16:05:00-05:00')
    result = assess(row, directive, now)
    assert result['eligible'] and not result['executable_now']
    assert result['execution_date'] == '2026-11-16'
    assert result['entry_deadline'] == '2026-11-17'
    assert '当前不是即时买入' in result['description']


@pytest.mark.parametrize('kind', ['stale_quote', 'missing_quote', 'future_quote', 'old_daily', 'hard_gate',
                                  'overextended', 'profit_gap', 'condition', 'guard', 'far_limit',
                                  'below_band', 'above_band', 'bad_limit', 'financial_veto', 'expired'])
def test_existing_hard_gaps_untriggered_orders_and_unreachable_limits_stay_out(kind):
    row, directive, now = fixture()
    if kind == 'stale_quote': row['quote_asof'] = '2026-11-16T09:30:00+08:00'
    elif kind == 'missing_quote': row.pop('quote_asof')
    elif kind == 'future_quote': row['quote_asof'] = '2026-11-16T10:01:00+08:00'
    elif kind == 'old_daily': row['crosscheck']['asof'] = '2026-11-12'
    elif kind == 'hard_gate': row['entry_gate']['blocked'] = True
    elif kind == 'overextended': row['entry_location'] = {'blocked': True}
    elif kind == 'profit_gap': row['profit_gaps'] = ['目标空间未核']
    elif kind == 'condition': directive.update(action='挂单买入', order='条件单', condition='突破触发')
    elif kind == 'guard': directive['guard'] = '等待大盘'
    elif kind == 'far_limit': directive['price'] = 99
    elif kind == 'below_band': row['last'] = 98
    elif kind == 'above_band': row['last'] = 102
    elif kind == 'bad_limit': directive['price'] = 102
    elif kind == 'financial_veto': directive.update(action='不买', signal_plan=deepcopy(directive))
    elif kind == 'expired': directive['valid_until'] = '2026-11-13'
    before = deepcopy((row, directive))
    result = assess(row, directive, now)
    assert not result['eligible'] and result['reasons']
    assert (row, directive) == before


def test_absolute_deadline_must_cover_next_session_without_rolling_it_forward():
    row, directive, now = fixture('2026-11-16T16:30:00+08:00', quote='2026-11-16T15:05:00+08:00')
    directive['valid_until'] = '2026-11-16'
    assert not assess(row, directive, now)['eligible']
    directive['valid_until'] = '2026-11-17'
    result = assess(row, directive, now)
    assert result['eligible'] and not result['requires_new_instruction']
    assert result['expiry'] == '2026-11-17'


def test_one_a_only_now_and_cannot_wait_through_lunch():
    row, directive, now = fixture('2026-11-16T12:00:00+08:00', hold=5,
                                   quote='2026-11-16T11:25:00+08:00')
    assert not assess(row, directive, now, '1A')['eligible']
    row['max_sessions'] = 15
    result = assess(row, directive, now, '2A')
    assert result['eligible'] and result['requires_new_session_quote']


def test_astra_keeps_original_month_deadline_without_3a_trend_or_horizon_rules():
    row, directive, now = fixture('2026-11-16T16:30:00+08:00', hold=None,
                                   quote='2026-11-16T15:05:00+08:00')
    row.pop('crosscheck')
    row.update(window={'start': '2026-11-10', 'deadline': '2026-11-30', 'sessions': 15},
               monthly_entry_deadline='2026-11-30')
    directive['valid_until'] = '2026-11-30'
    result = assess(row, directive, now, 'Astra')
    assert result['eligible'] and not result['executable_now']
    assert result['expiry'] == '2026-11-30' and result['entry_deadline'] == '2026-11-17'
    assert result['trend_evidence']['required'] is False
    row['monthly_entry_deadline'] = '2026-11-17'
    assert not assess(row, directive, now, 'Astra')['eligible']


def test_unverified_calendar_and_timezone_fail_without_mutating_any_plan():
    row, directive, _ = fixture()
    for now in (datetime(2026, 11, 16), datetime.fromisoformat('2027-01-04T10:00:00+08:00')):
        assert not assess(row, directive, now)['eligible']
