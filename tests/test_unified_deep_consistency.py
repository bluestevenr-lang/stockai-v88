"""Deep analysis and 3A read one verdict: same regime, same company evidence."""
import copy
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_trend_scenarios import inputs
from trend_scenarios import build_stock

U = {'verdict': '3A·现在可买', 'buyable': True, 'score': 88.2, 'annual_regime': 'up', 'annual_label': '上行结构',
     'annual_metrics': {'top_warning': False, 'basis': 'screening', 'asof': '2026-10-09'},
     'business_supported': True, 'business_status': 'support', 'business_source': '数据商财报',
     'business_reason': '营收同比+30.9%', 'entry_range': [27.49, 27.91], 'stop': 25.68, 'net_rr': 4.06,
     'trend_exit_ma60': 26.05, 'buy_timing': 'next_open', 'last': 27.77, 'quote_asof': '2026-10-09T16:14:30+08:00'}


def test_without_company_support_long_end_is_capped_as_before():
    annual, synthesis, period = inputs(regime='up')
    annual['evidence'] = {'company_support': False, 'company_adverse': False}
    base = [p['base'] for p in build_stock(annual, synthesis, period)['points']]
    assert base[5] == base[6] == .5


def test_unified_buyable_3a_curve_stays_strong_and_ignores_page_only_signals():
    annual, synthesis, period = inputs(regime='up')
    annual['unified'] = U
    synthesis['turning'] = {'side': 'top'}
    period['status'] = 'divergent'
    result = build_stock(annual, synthesis, period)
    base = [p['base'] for p in result['points']]
    assert base == [1] * 7
    assert result['regime'] == 'up' and result['headline'].startswith('3A·现在可买｜')
    assert result['unified'] == U and any('不改变统一年度结论' in a for a in result['assumptions'])


def test_annual_outlook_takes_company_evidence_from_unified_record():
    from annual_outlook import build
    out = build({'code': '002648.SZ'}, None, {}, unified=U)
    assert out['evidence']['company_support'] is True and out['evidence']['company_source'] == '数据商财报'
    assert out['unified'] == U
    adverse = build({'code': '002648.SZ'}, None, {}, unified=dict(U, business_supported=False, business_status='counterevidence'))
    assert adverse['evidence']['company_adverse'] is True and adverse['evidence']['company_support'] is False


def test_lookup_matches_code_forms_and_rejects_old_policy():
    from unified_evaluation import lookup
    doc = {'grade_policy': '2026-10-11-buyable-3a-v4', 'generated_at': 'x',
           'evaluations': {'6.HK': dict(U, name='电能实业'), '600519.SS': U}}
    assert lookup('0006.HK', doc)['code'] == '6.HK'
    assert lookup('600519.SH', doc)['code'] == '600519.SS'
    assert lookup('AAPL', doc) is None
    assert lookup('6.HK', dict(doc, grade_policy='2026-10-09-highest-3a-v3')) is None


def test_verdict_strip_and_render_scope():
    from unified_evaluation import html
    from future_trend_visual import render
    text = html(U)
    assert '3A·现在可买' in text and '60日线' in text and '4.06' in text and '开盘' in text
    assert '未进入本轮统一评估' in html(None)
    waiting = html(dict(U, verdict='3A待买点', buyable=False, trigger={'text': '等年度结构转上行'}))
    assert '买点触发条件：等年度结构转上行' in waiting
    annual, synthesis, period = inputs(regime='up')
    annual['unified'] = U
    stock = build_stock(annual, synthesis, period)
    assert '3A·现在可买' in render(stock)
    sector = copy.deepcopy(stock); sector['scope'] = 'sector'; sector.pop('unified')
    assert 'v88-unified' not in render(sector) and 'v88-unified' in render(stock)
