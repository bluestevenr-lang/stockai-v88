"""Shadow branch arithmetic, missing evidence, authority and real rendering boundary."""
from copy import deepcopy
import ast
import json
from pathlib import Path
import numpy as np
import pandas as pd
import pytest
from deep_analysis_data import snapshot_signature
from research_path_diagnostics import DATA_CHECKS, build, combine
from research_path_view import html


def fixture(n=300):
    values = np.linspace(10, 20, n)
    frame = pd.DataFrame({'Open': values, 'High': values + .2, 'Low': values - .2,
                          'Close': values, 'Volume': np.full(n, 100.)},
                         index=pd.bdate_range(end='2026-09-11', periods=n))
    quality = {'code': '1070.HK', 'source_asof': '2026-09-11', 'price_basis': 'split-adjusted OHLC; not total return',
               'snapshot_signature': snapshot_signature(frame), 'source': 'test'}
    cross = {**quality, 'checks': [{'id': k, 'status': 'pass'} for k in DATA_CHECKS + ('reviews',)]}
    context = {'code': '1070.HK', 'loaded_factpack_id': 'f',
        'row': {'tier': '0A', 'trade_plan': {'entry_range': [15., 16.], 'stop': 14.},
                'reason_codes': ['净收益风险不足', '入场未确认'],
                'action_blocks': ['EXECUTION_NOT_TRIGGERED']},
        'card': {'total': 50, 'gpt': {'total': 60}, 'books': {'total': 50, 'pass_n': 4, 'required': 8,
                  'checks': [{'id': 'entry', 'label': '入场确认', 'ok': False, 'detail': '未确认'},
                             {'id': 'profit', 'label': '净收益风险', 'ok': False, 'detail': '不足'},
                             {'id': 'evidence', 'label': '证据', 'ok': None, 'detail': '缺数据'}]}}}
    return context, frame, quality, cross


def refresh(frame, quality, cross):
    quality['snapshot_signature'] = snapshot_signature(frame)
    cross['snapshot_signature'] = quality['snapshot_signature']


@pytest.mark.parametrize('values,op,result', [
    ([True, None], 'and', None), ([False, None], 'and', False),
    ([False, None], 'or', None), ([True, None], 'or', True),
    ([False, False], 'or', False), ([True, True], 'and', True),
    ([], 'and', None), ([], 'or', None)])
def test_three_valued_logic(values, op, result):
    assert combine(values, op) is result


def test_positive_clue_does_not_promote_zero_grade_or_modify_contract():
    c, f, q, x = fixture()
    before = deepcopy(c)
    report = build(c, f, q, x)
    assert report['data_ready'] and report['local_repair_clue']['value'] is True
    branches = {b['id']: b for b in report['branches']}
    assert branches['trend_continuation']['state'] == 'pass'
    assert branches['trend_repair']['state'] == 'not_applicable'
    assert branches['deep_rebound']['state'] == 'not_applicable'
    assert report['central']['grade'] == '0A' and report['central']['score'] == 50
    assert report['stabilization_complete'] is None
    assert report['entry_permission'] is False and report['decision_weight'] == 0
    assert report['model_calls'] == report['network_calls'] == 0
    assert c == before
    assert [b['id'] for b in report['blockers']] == ['book:entry', 'book:profit', 'book:evidence']
    assert report['central']['reason_codes'] == before['row']['reason_codes']


def test_missing_long_windows_are_unknown_not_shorter_averages():
    c, f, q, x = fixture(61)
    r = build(c, f, q, x)
    assert r['metrics']['ma200'] is None and r['metrics']['ma150'] is None
    assert r['metrics']['drawdown52w_pct'] is None
    assert r['deep_drawdown_hit'] is None  # Short returns fail, 52 weeks unknown.
    assert r['branches'][0]['state'] == 'unknown'
    json.dumps(r, allow_nan=False)


def test_large_drawdown_can_match_without_long_history_and_confirmation_does_not_buy():
    c, f, q, x = fixture(21)
    f.loc[f.index[-1], ['Open', 'High', 'Low', 'Close']] = [10., 10.2, 9.8, 10.]
    refresh(f, q, x)
    r = build(c, f, q, x)
    assert r['deep_drawdown_hit'] is True
    assert r['branches'][2]['state'] == 'unknown'
    assert r['rebound_confirmation_complete'] is False
    assert not r['entry_permission']


def test_returns_highs_and_volume_exclude_today_from_reference_window():
    c, f, q, x = fixture()
    f.loc[f.index[-1], ['Open', 'High', 'Low', 'Close', 'Volume']] = [30., 31., 29., 30., 130.]
    refresh(f, q, x)
    r = build(c, f, q, x)
    m = r['metrics']
    assert m['return5_pct'] == pytest.approx((30/f.Close.iloc[-6]-1)*100)
    assert m['return20_pct'] == pytest.approx((30/f.Close.iloc[-21]-1)*100)
    assert m['prior_high5'] == f.High.iloc[-6:-1].max() < 31.
    assert m['volume_ratio20'] == 1.3
    assert next(v for v in r['branches'][2]['checks'] if v['id'] == 'volume')['value'] is True
    assert r['rebound_confirmation_complete'] is None  # Missing fundamental/event strategy audit.


@pytest.mark.parametrize('problem', ['stale', 'signature', 'cross_signature', 'issuer', 'unfinished_date', 'basis'])
def test_invalid_or_mismatched_evidence_does_not_emit_current_clue(problem):
    c, f, q, x = fixture()
    if problem in ('stale', 'basis'):
        next(v for v in x['checks'] if v['id'] == ('session' if problem == 'stale' else 'basis'))['status'] = 'gap'
    elif problem == 'signature':
        f.loc[f.index[-1], 'Close'] = 99
    elif problem == 'cross_signature':
        x['snapshot_signature'] = 'other'
    elif problem == 'issuer':
        q['code'] = '000100.SZ'
    else:
        q['source_asof'] = '2026-09-13'
    r = build(c, f, q, x)
    assert not r['data_ready'] and r['metrics'] == {}
    assert r['local_repair_clue']['value'] is None


def test_missing_data_and_stale_review_do_not_show_old_score_as_current():
    c, f, q, x = fixture()
    next(v for v in x['checks'] if v['id'] == 'reviews')['status'] = 'gap'
    r = build(c, None, {}, x)
    assert not r['data_ready'] and r['central']['score'] is None
    assert r['central']['original_plan'] == c['row']['trade_plan']
    assert '中央当前审核待核' in html(r)


def test_safe_compact_render_and_shared_routes():
    c, f, q, x = fixture()
    c['row']['reason_codes'].append('<img src=x onerror=alert(1)>')
    r = build(c, f, q, x)
    markup = html(r)
    assert '<img' not in markup and '&lt;img' in markup
    assert '研究线索 ↔ 中央审核' in markup and '15.0' not in markup  # No second trade contract table.
    assert '形态条件满足' in markup and '○待核' in markup
    root = Path(__file__).resolve().parents[1]
    for name in ('focused_deep_view.py', 'app_v88_integrated.py'):
        tree = ast.parse((root/name).read_text())
        assert any(isinstance(v, ast.ImportFrom) and v.module == 'research_path_diagnostics' for v in ast.walk(tree))
    source = ast.parse((root/'research_path_diagnostics.py').read_text())
    banned = {'requests', 'httpx', 'openai', 'subprocess', 'socket'}
    assert not any(isinstance(v, ast.Import) and any(a.name in banned for a in v.names) for v in ast.walk(source))
