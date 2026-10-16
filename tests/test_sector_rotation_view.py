from copy import deepcopy
from datetime import datetime, timezone
import re

import pytest

from sector_rotation_view import sector_rotation_html


NOW = datetime(2026, 10, 13, 10, tzinfo=timezone.utc)


def sample(market="A股"):
    from exchange_sessions import latest_completed
    from sector_rotation import VERSION, BENCHMARKS
    day = latest_completed(market, NOW).isoformat()
    baseline, baseline_name = BENCHMARKS[market]
    rows = []
    for index, state in enumerate(("持续占优", "短期改善", "短期转弱", "持续落后")):
        rows.append({"symbol": f"ETF{index}", "name": f"代理{index}", "market": market,
                     "benchmark_symbol": baseline, "status": "current", "source_asof": day,
                     "benchmark_asof": day, "expected_session": day,
                     "basis": "same_session_summary", "sector_chg5d": -1.1, "sector_chg20d": 2,
                     "rs5_pp": 0.4 if index < 2 else -0.4, "rs20_pp": 1 if index in (0, 2) else -1,
                     "quadrant": state, "transition": {"status": "first_observation", "consecutive_sessions": 1}})
    return {"version": VERSION, "generated_at": "2026-10-13T10:00:00+00:00",
            "markets": {market: {"market": market, "expected_session": day,
                "benchmark": {"symbol": baseline, "name": baseline_name, "source_asof": day},
                "coverage": {"expected": 4, "current": 999}, "rows": rows}}}


def test_four_groups_show_true_returns_basis_and_dates_without_mutating_input():
    doc = sample(); before = deepcopy(doc)
    html = sector_rotation_html(doc, "A股", now=NOW)
    assert all(state in html for state in ("持续占优", "短期改善", "短期转弱", "持续落后"))
    assert '当前有效 4 / 配置 4' in html and '999' not in html
    assert '5日超额 <b>+0.40pp' in html and '代理5日涨跌 -1.10%' in html
    assert '应有完整交易日 2026-10-13' in html
    assert '首次观察 · 待跨日确认' in html
    assert '资金流未核验' in html and '相对占优也可能只是抗跌' in html
    assert doc == before


def test_expired_snapshot_is_historical_even_if_producer_says_current():
    doc = sample()
    html = sector_rotation_html(doc, "A股", now=datetime(2026, 10, 14, 10, tzinfo=timezone.utc))
    assert 'data-current="0"' in html
    assert '历史观察 · 4，不计当前' in html and '历史 2026-10-13' in html
    assert '当前轮动待更新' in html and '应有完整交易日 2026-10-14' in html
    assert '首次观察 · 待跨日确认' not in html


@pytest.mark.parametrize("invalid", [None, {}, {"version": "unknown", "markets": {"A股": {"coverage": {"current": 9}}}}])
def test_missing_or_unknown_snapshot_never_claims_no_opportunities(invalid):
    html = sector_rotation_html(invalid, "A股", now=NOW)
    assert 'data-current="0"' in html and '缺数据不代表没有强势板块' in html
    assert '配置 待核' in html


def test_cross_market_rows_never_enter_current_or_linked_names():
    doc = sample(); rows = doc['markets']['A股']['rows']
    rows[0]['market'] = '港股'; rows[0]['name'] = '错误跨市场'
    rows[1]['linked_stocks'] = [{"code":"US", "name":"不可借用", "market":"美股", "module":"3A"}]
    html = sector_rotation_html(doc, "A股", now=NOW)
    assert '当前有效 3 / 配置 4' in html
    assert '错误跨市场' not in html and '不可借用' not in html


@pytest.mark.parametrize("field,value", [("rs5_pp", float('nan')), ("rs20_pp", None), ("sector_chg5d", True),
                                           ("benchmark_asof", "2026-10-12"), ("benchmark_symbol", "OTHER")])
def test_invalid_or_misaligned_fact_is_pending(field, value):
    doc = sample(); doc['markets']['A股']['rows'][0][field] = value
    html = sector_rotation_html(doc, "A股", now=NOW)
    assert 'data-current="3"' in html and '数据待补 · 1' in html


def test_matching_wrong_benchmark_does_not_self_validate():
    doc = sample(); block = doc['markets']['A股']
    block['benchmark']['symbol'] = '^GSPC'
    block['benchmark']['name'] = '不能借用的美股基准'
    for row in block['rows']: row['benchmark_symbol'] = '^GSPC'
    html = sector_rotation_html(doc, 'A股', now=NOW)
    assert 'data-current="0"' in html and '基准与本市场固定基准不一致' in html
    assert '同市场固定基准 上证指数（000001.SS）' in html
    assert '不能借用的美股基准' not in html


def test_market_envelope_and_row_identity_must_both_match():
    doc = sample(); doc['markets']['A股']['market'] = '美股'
    html = sector_rotation_html(doc, 'A股', now=NOW)
    assert 'data-current="0"' in html and '代理0' not in html


def test_cached_expected_date_does_not_set_current_market_calendar():
    doc = sample(); doc['markets']['A股']['expected_session'] = '2026-10-13'
    html = sector_rotation_html(doc, 'A股', now=datetime(2026, 10, 16, 10, tzinfo=timezone.utc))
    assert 'data-current="0"' in html and '应有完整交易日 2026-10-16' in html
    assert '历史观察 · 4' in html


@pytest.mark.parametrize('now,market', [(datetime(2027, 1, 5, tzinfo=timezone.utc), 'A股'),
                                      (datetime(2026, 10, 13, 10), 'A股'), (NOW, '未知市场')])
def test_calendar_unavailable_fails_closed_without_crashing(now, market):
    html = sector_rotation_html(sample(), market, now=now)
    assert 'data-current="0"' in html and '交易日历未核验' in html
    assert '应有完整交易日 待核' in html


def test_future_source_clock_cannot_be_current_or_historical():
    doc = sample(); row = doc['markets']['A股']['rows'][0]
    row['source_asof'] = row['benchmark_asof'] = '2026-10-14'
    html = sector_rotation_html(doc, 'A股', now=NOW)
    assert 'data-current="3"' in html and '数据待补 · 1' in html
    assert '历史观察' not in html and '原行情 2026-10-14' in html


def test_failed_same_day_revision_keeps_explicit_historical_values():
    doc = sample(); doc['markets']['A股']['rows'][0]['status'] = 'historical'
    html = sector_rotation_html(doc, 'A股', now=NOW)
    assert 'data-current="3"' in html and '历史观察 · 1，不计当前' in html


def test_confirmed_flag_without_cross_day_evidence_cannot_show_confirmation():
    doc = sample(); doc['markets']['A股']['rows'][0]['transition'] = {
        'status':'confirmed','confirmed':True,'consecutive_sessions':2,
        'from_state':'持续落后','to_state':'持续占优','prior_session':'2026-10-13'}
    html = sector_rotation_html(doc, 'A股', now=NOW)
    assert '已跨日确认' not in html and '跨日确认待核' in html


@pytest.mark.parametrize('field,value', [('markets', []), ('markets', {'A股': []})])
def test_malformed_envelope_renders_pending(field, value):
    doc = sample(); doc[field] = value
    html = sector_rotation_html(doc, 'A股', now=NOW)
    assert 'data-current="0"' in html and '当前轮动待更新' in html


def test_duplicate_symbol_does_not_inflate_coverage():
    doc = sample(); doc['markets']['A股']['rows'].append(deepcopy(doc['markets']['A股']['rows'][0]))
    html = sector_rotation_html(doc, 'A股', now=NOW)
    assert '当前有效 4 / 配置 4' in html


def test_xss_all_dynamic_fields_and_deep_link_are_escaped():
    doc = sample(); block = doc['markets']['A股']; row = block['rows'][0]
    row['name'] = '<img src=x onerror=alert(1)>'; block['note'] = '<script>x</script>'
    row['linked_stocks'] = [{"code":'X" onclick="alert(1)', "name":"<svg onload=x>", "market":"A股",
                             "module":"3A", "last":10.2, "currency":"CNY", "quote_asof":"<bad>"}]
    html = sector_rotation_html(doc, 'A股', now=NOW, element_id='a"><script>x</script>')
    assert '<script>' not in html and '<img' not in html and '<svg' not in html
    assert '&lt;img' in html and '&lt;script&gt;' in html
    assert 'q=X%22%20onclick%3D%22alert%281%29&amp;focus=deep' in html
    assert '现 10.20 CNY' in html and '报价 &lt;bad&gt;' in html


def test_nonconsecutive_and_confirmed_transitions_are_explicit():
    doc = sample(); rows = doc['markets']['A股']['rows']
    rows[0]['transition'] = {'status':'nonconsecutive','consecutive_sessions':1,'prior_session':'2026-10-09'}
    rows[1]['transition'] = {'status':'confirmed','confirmed':True,'consecutive_sessions':2,
                             'from_state':'持续落后','to_state':'短期改善','prior_session':'2026-10-12'}
    html = sector_rotation_html(doc, 'A股', now=NOW)
    assert '日期不连续 · 待确认' in html
    assert '持续落后 → 短期改善 · 已跨日确认 · 连续 2 个交易日' in html


def test_small_coverage_and_neutral_do_not_pretend_complete_rotation():
    doc = sample('港股'); block = doc['markets']['港股']; block['rows'] = block['rows'][:1]
    block['rows'][0]['quadrant'] = '中性'; block['coverage']['expected'] = 3
    html = sector_rotation_html(doc, '港股', now=NOW)
    assert '当前有效 1 / 配置 3' in html and '不能视为完整市场轮动' in html
    assert '中性边界 · 1' in html and '数据待补 · 2' in html


def test_compact_narrow_layout_has_no_fixed_width_or_scripts():
    html = sector_rotation_html(sample(), 'A股', now=NOW)
    assert '@media(max-width:520px)' in html and 'grid-template-columns:minmax(0,1fr)' in html
    assert 'overflow-wrap:anywhere' in html
    assert not re.search(r'min-width:\s*\d{3,}px', html)
    assert '<script' not in html


def test_existing_rotation_renderer_leads_with_relative_view_and_folds_old_curves():
    from rotation_ui import rotation_map_html, available_markets
    doc = sample()
    forecast = {'relative_strength':doc, 'markets': {'A股':{}}, 'trajectories':{'A股':[{'name':'旧条件','points':{}}]}}
    html = rotation_map_html(forecast, focus_market='A股')
    assert html.index('板块相对强弱观察') < html.index('原未来条件视图 · 展开')
    assert '<details class="rf-prior-view">' in html and 'rf-phase-clock' in html
    assert html.count('<style>') == 1
    assert available_markets({'relative_strength':doc}) == ['A股']
