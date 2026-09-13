from copy import deepcopy
from datetime import datetime, timezone
import pandas as pd
import pytest

import annual_outlook
from period_consistency import build, html
from stock_horizon import analyze


def sample(market='A股'):
    from market_data_helper import _core
    _core()
    from history_calendar import coverage
    code = {'A股': '688002.SS', '美股': 'SEPN', '港股': '2696.HK'}[market]
    days = coverage([], market, '2024-09-11', '2026-09-11')['missing_sessions']
    close = [100 * 1.004 ** i for i in range(len(days))]
    close[-1] *= .965
    frame = pd.DataFrame({'Open': close, 'High': [x*1.01 for x in close],
                          'Low': [x*.99 for x in close], 'Close': close,
                          'Volume': [1e6]*(len(days)-1)+[2e6]}, index=pd.to_datetime(days))
    frame.attrs.update(source='verified test', source_asof='2026-09-11', price_basis='split-adjusted OHLC; not total return')
    quality = {'code': code, **frame.attrs, 'snapshot_signature': annual_outlook._signature(frame)}
    context = {'code': code, 'row': {'market': market}, 'selection': {}}
    trend = {'stage': '高位震荡', 'turning': {'side': 'top'}}
    synthesis = {'code': code, 'source_asof': quality['source_asof'],
                 'snapshot_signature': quality['snapshot_signature'], 'turning': trend['turning']}
    annual = annual_outlook.build(context, frame, quality, synthesis=synthesis,
                                  now=datetime(2026,9,13,8,tzinfo=timezone.utc))
    cycles = analyze(code, code, frame, full=trend, allow_ai=False)
    return [code, frame, quality, cycles, trend, annual, synthesis]


@pytest.mark.parametrize('market', ['A股', '美股', '港股'])
def test_positive_eight_week_window_keeps_near_term_warning_and_annual_conditions(market):
    args = sample(market); original = deepcopy(args[2:])
    result = build(*args)
    eight = next(x for x in result['observations'] if x['window'] == '8周')
    assert eight['score'] > 59 and eight['trading_days'] == 40
    assert result['status'] == 'caution' and args[5]['regime'] == 'up_caution'
    assert result['headline'] == args[5]['headline']
    assert result['conditions'] == [args[5]['phases'][0][k] for k in ('confirmation', 'invalidation')]
    assert '不能推出未来8周必涨' in result['summary']
    assert result['model_calls'] == result['network_calls'] == 0
    assert result['no_grade_authority'] and not result['entry_permission']
    assert args[2:] == original


@pytest.mark.parametrize('change', ['annual_identity', 'annual_asof', 'annual_signature', 'annual_basis',
                                  'score', 'lookback_start', 'lookback_end', 'requested_days',
                                  'window_complete', 'evidence_scope', 'early_source_bar'])
def test_different_inputs_cannot_claim_shared_conclusion(change):
    args = sample()
    if change.startswith('annual_'):
        key = {'annual_identity': 'code', 'annual_asof': 'source_asof',
               'annual_signature': 'snapshot_signature', 'annual_basis': 'price_basis'}[change]
        args[5][key] = 'wrong'
    elif change == 'early_source_bar':
        args[1].iloc[0, args[1].columns.get_loc('Close')] *= 1.005
    else:
        key = 'rule_score' if change == 'score' else change
        args[3]['facts']['horizons']['8周'][key] = False if change == 'window_complete' else 'wrong'
    result = build(*args)
    assert result['status'] == 'blocked' and result['gaps']
    assert result['observations'] == result['conditions'] == []
    assert not result['entry_permission']


def test_missing_history_is_a_gap_not_a_neutral_score():
    result = build('GRPN', None, {}, {}, {}, {}, {})
    assert result['status'] == 'blocked' and not result['observations']
    assert not result['entry_permission'] and result['model_calls'] == 0


def test_partial_year_cannot_be_promoted_by_positive_short_windows():
    args = sample(); args[5].update(data_status='limited', valid12month=False, regime='limited')
    result = build(*args)
    assert result['status'] == 'limited' and '年度证据仍待补齐' in result['headline']
    assert not result['entry_permission']


def test_html_escapes_untrusted_evidence_and_keeps_same_input_id():
    result = build(*sample()); result['summary'] = '<script>alert(1)</script>'
    rendered = html(result)
    assert '<script>' not in rendered and '&lt;script&gt;' in rendered
    assert result['input_id'] in rendered
