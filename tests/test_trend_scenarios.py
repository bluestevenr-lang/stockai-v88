import copy
from datetime import date
import json
import math
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from trend_scenarios import WEEKS, build_sector, build_stock


def inputs(code='688002.SS', regime='up_caution', market='A股'):
    phases = [{'outlook': f'阶段{i}原判断', 'confirmation': f'阶段{i}原确认',
               'invalidation': f'阶段{i}原失效'} for i in range(4)]
    annual = {'code': code, 'market': market, 'input_id': 'annual-proof',
              'snapshot_signature': 'series-proof', 'source_asof': '2026-09-11',
              'outlook_start': '2026-09-13', 'data_status': 'complete',
              'valid12month': True, 'regime': regime, 'top_warning': regime == 'up_caution',
              'headline': '原年度条件主线', 'thesis': '原年度判断依据', 'phases': phases,
              'evidence': {'company_support': True, 'company_adverse': False}, 'gaps': []}
    synthesis = {'code': code, 'input_id': 'synthesis-proof', 'snapshot_signature': 'series-proof',
                 'source_asof': '2026-09-11', 'turning': {'side': 'top' if regime == 'up_caution' else ''},
                 'original_plan': {'entry_range': [10, 11], 'stop': 9, 'take_profit_range': [13, 14]},
                 'central_grade': '1A', 'central_audit_score': 70, 'review_blocks': []}
    period = {'code': code, 'input_id': 'period-proof', 'snapshot_signature': 'series-proof',
              'source_asof': '2026-09-11', 'annual_input_id': 'annual-proof',
              'synthesis_input_id': 'synthesis-proof', 'status': 'caution' if regime == 'up_caution' else 'linked',
              'observations': [{'window': '8周', 'score': 81}]}
    return annual, synthesis, period


@pytest.mark.parametrize('code,market', [('688002.SS', 'A股'), ('SEPN', '美股'), ('2696.HK', '港股')])
def test_three_markets_bound_forward_only_and_no_authority(code, market):
    data = inputs(code, market=market)
    before = copy.deepcopy(data)
    result = build_stock(*data, code=code, name='个股')
    assert result['status'] == 'ready'
    assert result['market'] == market
    assert [p['weeks'] for p in result['points']] == list(WEEKS)
    assert all(date.fromisoformat(p['date']) >= date(2026, 9, 13) for p in result['points'])
    assert result['source_asof'] == '2026-09-11'
    assert result['points'][0]['date'] == '2026-09-13'
    assert result['points'][0]['base'] == result['points'][0]['up'] == result['points'][0]['down']
    for p in result['points']:
        assert -2 <= p['down'] <= p['base'] <= p['up'] <= 2
        assert p['condition'] and p['invalidation']
        assert set(p['branches']) == {'up', 'base', 'down'}
    assert result['entry_permission'] is False
    assert result['model_calls'] == result['network_calls'] == 0
    assert data == before
    assert 'original_plan' not in result


@pytest.mark.parametrize('regime', ['up', 'up_caution', 'down', 'range'])
def test_paths_are_ordinal_branch_conditions_not_a_score_or_price(regime):
    result = build_stock(*inputs(regime=regime))
    assert result['status'] == 'ready'
    assert result['scale']['meaning'] == '定性趋势示意，不是价格、涨幅或概率'
    for p in result['points']:
        assert p['down'] <= p['base'] <= p['up']
        assert set(p).isdisjoint({'probability', 'confidence', 'expected_return', 'price'})


def test_top_warning_cannot_schedule_eight_week_rally():
    result = build_stock(*inputs())
    early = [p for p in result['points'] if 0 < p['weeks'] <= 8]
    assert all(p['base'] <= 0 and p['up'] <= .5 for p in early)
    assert all('解除' in p['condition'] for p in early)
    assert result['phase']['dy'] < 0


def test_missing_annual_top_flag_cannot_bypass_bound_synthesis_risk():
    annual, synthesis, period = inputs(regime='up')
    synthesis['turning']['side'] = 'top'
    result = build_stock(annual, synthesis, period)
    assert result['regime'] == 'up_caution'
    assert '风险' in result['headline']
    assert next(p for p in result['points'] if p['weeks'] == 8)['base'] < 0


def test_short_annual_divergence_changes_near_path_not_only_caption():
    annual, synthesis, period = inputs(regime='up')
    linked = build_stock(annual, synthesis, period)
    period['status'] = 'divergent'
    divergent = build_stock(annual, synthesis, period)
    assert divergent['phase']['dy'] < 0
    assert linked['points'][1]['base'] > divergent['points'][1]['base']
    assert all(p['base'] <= 0 and '分歧' in p['condition'] for p in divergent['points'] if 0 < p['weeks'] <= 8)
    assert divergent['points'][0]['base'] == divergent['points'][0]['up'] == divergent['points'][0]['down']


def test_range_top_warning_changes_both_circle_and_forward_path():
    annual, synthesis, period = inputs(regime='range')
    annual['top_warning'] = True
    result = build_stock(annual, synthesis, period)
    assert result['phase']['dy'] < 0 and result['phase']['x'] > 0
    assert all(p['base'] <= -.5 for p in result['points'] if 0 < p['weeks'] <= 8)
    assert '风险' in result['headline']


def test_company_countercase_stays_visible_and_caps_baseline():
    annual, synthesis, period = inputs(regime='up')
    annual['evidence'].update(company_adverse=True, company_support=False)
    result = build_stock(annual, synthesis, period)
    assert '分歧' in result['headline']
    for p in result['points'][1:]:
        assert p['base'] <= 0
        assert '反证' in p['branches']['up']['condition']
        assert '反证' in p['condition']


def test_distinct_company_countercases_preserve_the_actual_bound_reasons():
    annual, synthesis, period = inputs(regime='up')
    annual['evidence'].update(company_adverse=True, company_support=False,
        domains=[{'domain': 'fundamental', 'title': '基本面', 'state': '反对',
                  'reasons': ['原事实：经营现金流持续恶化']},
                 {'domain': 'valuation', 'title': '估值', 'state': '反对',
                  'reasons': ['原事实：估值缺乏盈利兑现支持']}])
    result = build_stock(annual, synthesis, period)
    assert len(result['counterevidence']) == 2
    for point in result['points'][1:]:
        assert '经营现金流持续恶化' in point['branches']['up']['condition']
        assert '估值缺乏盈利兑现支持' in point['branches']['up']['condition']


def test_unreviewed_company_is_a_gap_not_fabricated_forecast_support():
    annual, synthesis, period = inputs(regime='up')
    annual['evidence']['company_support'] = False
    result = build_stock(annual, synthesis, period)
    assert result['status'] == 'ready'
    assert all(p['base'] <= .5 for p in result['points'] if p['weeks'] >= 32)
    assert all('补齐' in p['branches']['up']['condition'] for p in result['points'] if p['weeks'] >= 16)


def test_far_stages_keep_original_conditions_and_cumulative_gates():
    annual, synthesis, period = inputs()
    result = build_stock(annual, synthesis, period)
    for week, stage in [(2, 0), (8, 0), (16, 1), (32, 2), (52, 3)]:
        p = next(p for p in result['points'] if p['weeks'] == week)
        assert p['label'] == annual['phases'][stage]['outlook']
        assert p['invalidation'] == annual['phases'][stage]['invalidation']
        assert all(annual['phases'][i]['confirmation'] in p['condition'] for i in range(stage + 1))


@pytest.mark.parametrize('target,key,value', [
    (0, 'input_id', None), (1, 'input_id', ''), (2, 'input_id', ''),
    (2, 'annual_input_id', 'other'), (2, 'synthesis_input_id', 'other'),
    (0, 'source_asof', '2026-09-10'), (1, 'snapshot_signature', 'other'),
    (0, 'data_status', 'limited'), (0, 'valid12month', False),
    (0, 'regime', 'limited'), (0, 'outlook_start', '2026-09-01'),
    (0, 'phases', []), (0, 'phases', [None] * 4), (2, 'status', 'blocked'),
    (2, 'status', 'limited'), (1, 'code', 'SEPN'), (2, 'code', ''),
])
def test_missing_or_mismatched_evidence_has_no_line(target, key, value):
    data = inputs()
    data[target][key] = value
    result = build_stock(*data)
    assert result['status'] == 'missing'
    assert result['points'] == [] and result['phase']['x'] is None
    assert result['gaps']


def test_no_history_score_curve_or_current_clock_dependency():
    data = inputs()
    result = build_stock(*data)
    data[2]['observations'][0]['score'] = 2
    # The receipt remains unchanged in this fixture. A raw history score is not
    # used to construct future coordinates; real receipts are regenerated upstream.
    assert build_stock(*data)['points'] == result['points']
    assert build_stock(*inputs()) == result


def sector():
    return {'name': '半导体', 'symbol': 'sector-1', 'facts': {'1d': -.5, '5d': 1.5,
            '20d': 8, 'vs_ma20': 2, 'vs_ma60': 5, 'vol_ratio': 1.2},
            'points': {'2周': {'score': 81}, '5周': {'score': 90}}}


@pytest.mark.parametrize('market', ['A股', '港股', '美股'])
def test_sector_three_market_paths_require_real_dated_factors(market):
    row = sector()
    before = copy.deepcopy(row)
    result = build_sector(row, market, '2026-09-11')
    assert result['status'] == 'ready' and result['source_asof'] == '2026-09-11'
    assert result['points'][0]['date'] == '2026-09-11'
    assert result['entry_permission'] is False and result['model_calls'] == 0
    assert any('盈利' in p['condition'] for p in result['points'] if p['weeks'] >= 16)
    assert row == before


@pytest.mark.parametrize('key,value', [('1d', None), ('5d', True), ('20d', math.nan),
    ('vs_ma20', math.inf), ('vs_ma60', '3.5'), ('vol_ratio', -1)])
def test_bad_sector_inputs_never_default_to_neutral(key, value):
    row = sector()
    row['facts'][key] = value
    result = build_sector(row, 'A股', '2026-09-11')
    assert result['status'] == 'missing' and result['points'] == []
    json.dumps(result, allow_nan=False)


def test_generation_time_cannot_supply_source_date():
    row = sector()
    row['generated_at'] = '2026-09-13T12:00:00+08:00'
    assert build_sector(row)['status'] == 'missing'
    row['source_asof'] = '2026-09-10'
    assert build_sector(row, source_asof='2026-09-11')['status'] == 'missing'


@pytest.mark.parametrize('required', ['2026-09-12', 'bad-date', '2026-09-01'])
def test_required_session_rejects_old_future_or_invalid_sector_reference(required):
    result = build_sector(sector(), 'A股', '2026-09-11', required_session=required)
    assert result['status'] == 'missing' and result['points'] == []
    assert result['required_session'] == required


def test_required_session_allows_exact_completed_day_and_same_start_branches():
    result = build_sector(sector(), 'A股', '2026-09-11T15:00:00+08:00', required_session='2026-09-11')
    assert result['status'] == 'ready'
    point = result['points'][0]
    assert point['base'] == point['up'] == point['down']


def test_legacy_sector_scores_turning_week_and_confidence_ignored():
    row = sector()
    result = build_sector(row, 'A股', '2026-09-11')
    row['points']['2周']['score'] = 3
    row['points']['5周']['score'] = 1
    row['confidence'] = '高'
    row['turning'] = {'horizon': '8周', 'type': '见顶'}
    assert build_sector(row, 'A股', '2026-09-11') == result


def test_sector_retreat_risk_changes_path_and_limits_near_up_branch():
    row = sector()
    row['facts'].update({'5d': -2, '20d': 10})
    result = build_sector(row, 'A股', '2026-09-11')
    assert result['regime'] == 'up_caution'
    assert all(p['up'] <= .5 for p in result['points'] if 0 < p['weeks'] <= 8)
    assert all('解除' in p['condition'] for p in result['points'] if 0 < p['weeks'] <= 8)


def test_canonical_cn_hk_identity_aliases_supported():
    annual, synthesis, period = inputs('688002.SS')
    assert build_stock(annual, synthesis, period, code='688002.SH')['status'] == 'ready'
    annual, synthesis, period = inputs('02696.HK', market='港股')
    assert build_stock(annual, synthesis, period, code='2696.HK')['status'] == 'ready'


def test_price_provenance_projects_without_repair_even_when_curve_blocked():
    annual, synthesis, period = inputs()
    annual.update(source='verified provider', price_basis='split-adjusted OHLC; not total return')
    for ready in (True, False):
        if not ready:
            period['status'] = 'blocked'
        result = build_stock(annual, synthesis, period)
        assert result['source'] == annual['source']
        assert result['price_basis'] == annual['price_basis']
        assert result['snapshot_signature'] == annual['snapshot_signature']
        assert result['status'] == ('ready' if ready else 'missing')
        if not ready:
            assert result['points'] == []


def test_non_dict_inputs_missing_safely():
    assert build_stock(None, [], False)['status'] == 'missing'
    assert build_sector(None)['status'] == 'missing'
