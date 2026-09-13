import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from barometer_ui import amount_daily_html


def series(dates):
    return {'series': [{'date': d, 'rel_pct': 0, 'value': 10} for d in dates],
            'vs_ma20_pct': 0, 'latest': 10, 'unit': '亿元', 'label': 'test'}


def test_series_share_real_date_axis_even_when_lengths_differ():
    x = amount_daily_html({'markets': {'中国': series(['2026-09-01', '2026-09-04']),
                                      '港股': series(['2026-09-03', '2026-09-04'])}}, unit='日')
    lines = re.findall(r"<polyline points='([^']+)'", x)
    points = [[float(p.split(',')[0]) for p in line.split()] for line in lines]
    assert points[0][0] < points[1][0]
    assert points[0][-1] == points[1][-1]


def test_unavailable_market_is_explained_when_other_market_renders():
    x = amount_daily_html({'markets': {'中国': series(['2026-09-04']),
                         '港股': {'error': '<missing>', 'latest_date': '2026-08-01'}}}, unit='日')
    assert '&lt;missing&gt;' in x and '2026-08-01' in x


from copy import deepcopy
from datetime import date, timedelta
import pytest
from barometer_ui import breadth_html, _aggregate


def breadth(**changes):
    row = {'adv': 20, 'dec': 10, 'flat': 2, 'n': 32, 'sample_count': 32,
           'ad_ratio': 2, 'index_chg': 1.2, 'median_chg': 0.1, 'divergence': 1.1,
           'source_asof': '2026-09-11', 'source': 'verified_full_market_daily_facts',
           'coverage_note': '有效样本32 / 本次有价行情40只；全交易所覆盖另核',
           'limit_up': 2, 'limit_down': 1,
           'dist_bands': [{'band': '-1~0', 'n': 10}, {'band': '0~1', 'n': 20}],
           'verdict': '普涨(赚钱效应好)；躲为上；个股机会>指数；情绪冰点'}
    row.update(changes)
    return {'markets': {'A股': row}}


def daily(count=65, **changes):
    dates = [(date(2026, 6, 1) + timedelta(days=i)).isoformat() for i in range(count)]
    data = series(dates)
    data.update({'latest_date': dates[-1], 'source': 'test source',
                 'relative_basis': 'rolling20-through-each-date-v1'})
    data.update(changes)
    return {'markets': {'中国': data}}


def test_breadth_uses_sample_scope_approximate_bands_and_source_date():
    html = breadth_html(breadth(), markets=('A股',))
    assert '有效样本32 / 本次有价行情40只' in html
    assert '2026-09-11' in html and 'verified_full_market_daily_facts' in html
    assert '近似档，并非真实涨跌停数量' in html
    assert '全市场逐只统计非抽样' not in html
    assert not any(s in html for s in ['赚钱效应好', '躲为上', '个股机会', '情绪冰点', '权重扛'])
    assert '↑增强' in html and '⚠风险' in html


@pytest.mark.parametrize('ratio,state', [(2, '↑增强'), (1.99, '↔分歧'), (0.51, '↔分歧'), (0.5, '↓减弱')])
def test_breadth_keeps_original_ratio_boundaries(ratio, state):
    assert f'{state}·该日样本' in breadth_html(breadth(ad_ratio=ratio), markets=('A股',))


def test_missing_divergence_is_not_silently_neutral():
    html = breadth_html(breadth(divergence=None), markets=('A股',))
    assert '○缺证·分化度未取得' in html
    assert '指数与样本中位接近' not in html


def test_missing_market_and_failed_refresh_stay_visible_and_escaped():
    doc = breadth()
    doc['markets']['港股'] = {'error': '<source missing>', 'source_asof': '2026-09-10'}
    doc['last_failed_at'] = '2026-09-13 09:00'
    html = breadth_html(doc)
    assert '&lt;source missing&gt;' in html and '美股' in html and '○缺证' in html
    assert '最近刷新失败' in html and '2026-09-11' in html and '2026-09-10' in html


def test_missing_band_count_stays_missing_and_mismatch_is_visible():
    doc = breadth(dist_bands=[{'band': '-1~0', 'n': None}, {'band': '0~1', 'n': 20}])
    html = breadth_html(doc, markets=('A股',))
    assert '○缺证' in html and '分档与涨跌家数未对齐' in html
    assert 'height:0px' in html


def test_window_title_is_not_replaced_by_vertical_axis_scale():
    html = amount_daily_html(daily(), unit='日', span='1年')
    assert '历史窗口=1年（最多250个源交易日' in html
    assert '实际65日' in html
    assert '近65个源交易日中位' in html and '全年中位' not in html and '近4周' not in html
    assert '净资金流向或未来涨跌' in html


@pytest.mark.parametrize('missing', [None, float('nan'), float('inf'), True])
def test_missing_latest_deviation_does_not_crash_or_become_neutral(missing):
    html = amount_daily_html(daily(vs_ma20_pct=missing), unit='日')
    assert '○缺证·最新偏离或基准未核验' in html
    assert '↔分歧·量能未明显偏离基准' not in html
    assert 'nan' not in html and 'inf' not in html


@pytest.mark.parametrize('value,state', [(10, '↑增强'), (9.9, '↔分歧'), (-9.9, '↔分歧'), (-10, '↓减弱')])
def test_amount_retains_original_ten_percent_boundaries(value, state):
    assert f'{state}·量能' in amount_daily_html(daily(vs_ma20_pct=value), unit='日')


def test_short_first_week_is_retained_with_observed_dates_and_count():
    rows = [{'date': '2026-09-04', 'value': 10, 'rel_pct': 0, 'window_n': 1},
            {'date': '2026-09-07', 'value': 20, 'rel_pct': 10, 'window_n': 2}]
    agg = _aggregate(rows, 'W')
    assert len(agg) == 2 and agg[0]['days'] == 1 and agg[0]['date'] == '2026-09-04'
    assert agg[0]['short_baseline_days'] == 1


def test_missing_daily_value_breaks_line_without_filling_zero():
    data = daily(5)
    data['markets']['中国']['series'][2]['rel_pct'] = None
    html = amount_daily_html(data, unit='日')
    lines = re.findall(r"<polyline points='([^']+)'", html)
    assert len(lines) == 2 and all(len(x.split()) == 2 for x in lines)
    assert '1个缺值或缺交易日' in html


def test_recorded_missing_session_breaks_line_even_without_placeholder_row():
    data = {'markets': {'中国': series(['2026-09-01', '2026-09-02', '2026-09-04', '2026-09-07'])}}
    data['markets']['中国']['data_quality'] = {'continuity': {'missing_sessions': ['2026-09-03']}}
    html = amount_daily_html(data, unit='日')
    lines = re.findall(r"<polyline points='([^']+)'", html)
    assert len(lines) == 2 and all(len(x.split()) == 2 for x in lines)
    assert '跨缺口断线' in html


def test_invalid_dates_are_reported_and_no_input_is_mutated():
    data = daily(5)
    data['markets']['中国']['series'][1]['date'] = 'invalid'
    original = deepcopy(data)
    html = amount_daily_html(data, unit='日')
    assert '日期缺失、重复或无序' in html and '<svg' not in html
    assert data == original


def test_single_point_is_visible_and_latest_quality_failure_is_not_drawn():
    data = daily(1)
    assert '<circle' in amount_daily_html(data, unit='日')
    data['markets']['中国']['data_quality'] = {'latest_session_verified': False}
    html = amount_daily_html(data, unit='日')
    assert '最近交易日未核验' in html and '<svg' not in html


def test_amount_legend_can_wrap_on_mobile_without_hiding_source_or_window():
    html = amount_daily_html(daily(), unit='日', span='1年')
    assert "white-space:nowrap" not in html
    assert "min-width:0;max-width:100%;overflow-wrap:anywhere" in html
    assert '源日期' in html and '近65个源交易日中位' in html


def test_amount_endpoint_date_labels_stay_inside_chart_bounds():
    html = amount_daily_html({'markets': {'中国': series(['2026-09-01', '2026-09-04'])}}, unit='日')
    assert "text-anchor='start'>09/01</text>" in html
    assert "text-anchor='end'>09/04</text>" in html
