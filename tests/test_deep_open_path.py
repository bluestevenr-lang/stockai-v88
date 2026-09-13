"""Cold detail rendering must not start optional providers or model calls."""
import ast
from contextlib import nullcontext
import logging
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'app_v88_integrated.py').read_text()
TREE = ast.parse(SOURCE)


def function(name, env):
    node = next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == name)
    node = ast.parse(ast.unparse(node)).body[0]
    node.decorator_list = []
    exec(compile(ast.Module(body=[node], type_ignores=[]), name, 'exec'), env)
    return env[name]


def bars():
    close = 100 + np.arange(120) * .15 + np.sin(np.arange(120))
    return pd.DataFrame({'Open': close, 'High': close + 1, 'Low': close - 1,
                         'Close': close, 'Volume': np.full(120, 1000.)},
                        index=pd.bdate_range('2026-01-01', periods=120))


def test_local_index_gap_is_explicit_and_receives_no_neutral_bonus():
    calls = []
    env = {'np': np, 'pd': pd, 'logging': logging,
           'fetch_stock_data': lambda *a: calls.append(a),
           'to_yf_cn_code': lambda code: code, '_load_market_temp': lambda: {},
           'identify_kline_pattern': lambda *a: 'test', 'calculate_trade_plan': lambda *a: None,
           'rl_agent': SimpleNamespace(decide=lambda df: ('观察', 'test', .5, []))}
    function('get_benchmark_code', env)
    calc = function('calculate_metrics_all', env)
    frame = bars()
    missing = calc(frame, '3330.HK', benchmark_loader=lambda _: None)
    matched = calc(frame, '3330.HK', benchmark_loader=lambda _: frame)
    assert calls == []
    assert missing['rs20'] is None and matched['rs20'] == pytest.approx(0)
    assert missing['mom_score'] == matched['mom_score']
    assert any('本项不计分' in row['说明'] for row in missing['momentum_rows'])
    assert env['get_benchmark_code']('920718.BJ') == '000001.SS'


def test_risk_gap_does_not_fetch_a_benchmark():
    calls = []
    env = {'np': np, '_safe_print': lambda *a: None,
           'fetch_stock_data': lambda *a: calls.append(a)}
    function('get_benchmark_code', env)
    risk = function('calculate_risk_metrics', env)
    assert risk(bars(), '3330.HK', benchmark_loader=lambda _: None) is None
    assert calls == []


def test_news_failure_is_not_retried_after_timeout_guard():
    accesses = []
    class Ticker:
        @property
        def news(self):
            accesses.append(True)
            raise RuntimeError('offline')
    env = {'HAS_YFINANCE': True, 'to_yf_cn_code': lambda c: c,
           'get_proxy_url': lambda: None, 'ProxyContext': lambda _: nullcontext(),
           'yf': SimpleNamespace(Ticker=lambda _: Ticker()), '_safe_print': lambda *a: None,
           'pd': pd}
    read = function('fetch_news_headlines', env)
    assert read('3330.HK') == []
    assert len(accesses) == 1


def test_fundamentals_renderer_contains_no_implicit_model_request():
    node = next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == 'render_fundamentals_panel')
    calls = {ast.unparse(n.func) for n in ast.walk(node) if isinstance(n, ast.Call)}
    assert 'call_model_api' not in calls and 'call_model_api_stream' not in calls
    assert 'business_summary' in ast.unparse(node)


def test_default_deep_path_does_not_invoke_supplemental_providers():
    first = SOURCE[:SOURCE.index('# ===V88_PAGE_BREAK:RESEARCH===')].count('\n') + 1
    last = SOURCE[:SOURCE.index("st.markdown('<div id=\"v88-strategy-research")].count('\n') + 1
    calls = [n for n in ast.walk(TREE) if isinstance(n, ast.Call) and first < n.lineno < last]
    prohibited = {'fetch_stock_fundamentals', 'fetch_news_headlines', '_price_extremes9', '_ann_fetch9', '_gp9'}
    assert not any(ast.unparse(n.func) in prohibited for n in calls)
    for call in calls:
        if ast.unparse(call.func) in {'calculate_metrics_all', 'calculate_risk_metrics'}:
            assert any(k.arg == 'benchmark_loader' for k in call.keywords)
    first_fetch = min((n for n in calls if ast.unparse(n.func) == '_deep_fetch'), key=lambda n: n.lineno)
    assert any(k.arg == 'allow_network' and isinstance(k.value, ast.Constant) and k.value.value is False
               for k in first_fetch.keywords)


def test_missing_local_series_cannot_start_network(monkeypatch):
    import market_data_helper
    market_data_helper._core()
    import verified_history
    from deep_analysis_data import fetch
    monkeypatch.setattr(verified_history, 'read', lambda *a, **kw: pd.DataFrame())
    import portable_history
    def missing(*a, **kw):raise FileNotFoundError('portable mirror absent')
    monkeypatch.setattr(portable_history, 'read', missing)
    def prohibited(*a, **kw):
        pytest.fail('passive detail read attempted network')
    monkeypatch.setattr(market_data_helper, 'fetch_df', prohibited)
    frame, quality = fetch('3330.HK', allow_network=False)
    assert frame is None and quality['network_requested'] is False


def test_rule_explanation_does_not_implicitly_request_statements():
    node = next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == 'render_readable_reasons')
    assert not any(isinstance(n, ast.Call) and ast.unparse(n.func) == 'fetch_stock_fundamentals'
                   for n in ast.walk(node))


def test_explicit_history_result_expires_at_next_completed_session(monkeypatch):
    import market_data_helper
    from deep_analysis_data import revalidate_cached
    source = bars()
    original_asof = source.index[-1].date().isoformat()
    source.attrs.update(source='verified test', source_asof=original_asof)
    result = (source, {'source_asof': original_asof, 'data_points': len(source)})
    next_session = [False]
    def validate(frame, code, *, source):
        if next_session[0]:
            raise ValueError('缺少最近已完成交易日行情')
        return frame.copy()
    monkeypatch.setattr(market_data_helper, 'validate', validate)
    fresh, _ = revalidate_cached(result, '3330.HK')
    assert fresh is not None
    next_session[0] = True
    stale, quality = revalidate_cached(result, '3330.HK')
    assert stale is None and quality['network_requested'] is False
    assert quality['source_asof'] == original_asof
    assert '缺少最近已完成交易日' in quality['error_detail']
    assert result[0].attrs['source_asof'] == original_asof
