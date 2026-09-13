import ast
import importlib.util
from pathlib import Path
import sys
import types
import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from modules.utils import to_yf_cn_code, parse_market_from_code
from deep_analysis_data import snapshot_signature


@pytest.mark.parametrize('raw,expected,market', [
    ('688002.SH','688002.SS','CN'),('000558','000558.SZ','CN'),
    ('000558.SZ','000558.SZ','CN'),('920001','920001.BJ','CN'),
    ('661.HK','0661.HK','HK'),('00661.HK','0661.HK','HK'),
    ('00700','0700.HK','HK'),('80700','80700.HK','HK'),
    ('grpn','GRPN','US'),('', '', 'UNKNOWN'),
])
def test_market_and_provider_identity(raw,expected,market):
    assert to_yf_cn_code(raw) == expected
    assert parse_market_from_code(raw) == market


def frame():
    close = np.arange(120)+50.
    return pd.DataFrame({'Open':close,'High':close+1,'Low':close-1,'Close':close,'Volume':1e6},
                        index=pd.bdate_range('2026-01-01',periods=120))


def test_snapshot_cache_invalidates_on_past_bar_or_basis_change():
    a=frame(); b=a.copy(); b.iloc[0,b.columns.get_loc('High')]+=1
    assert snapshot_signature(a)!=snapshot_signature(b)
    b=a.copy();b.attrs['price_basis']='different'
    assert snapshot_signature(a)!=snapshot_signature(b)


def test_desktop_adapter_survives_core_module_loaded_first(monkeypatch):
    foreign=types.ModuleType('gpt_subscription')
    monkeypatch.setitem(sys.modules,'gpt_subscription',foreign)
    spec=importlib.util.spec_from_file_location('isolated_desktop_adapter',ROOT/'desktop_gpt_subscription.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    assert module.model_name()=='gpt-6-astra'
    assert callable(module.complete) and callable(module.api_key)
    assert sys.modules['gpt_subscription'] is foreign


def test_symbol_dependency_is_not_inside_optional_modules_branch():
    tree=ast.parse((ROOT/'app_v88_integrated.py').read_text())
    assert any(isinstance(n,ast.ImportFrom) and n.module=='modules.utils'
               and 'to_yf_cn_code' in [a.name for a in n.names] for n in tree.body)


def test_deep_cycle_does_not_call_gpt_on_open(monkeypatch):
    import stock_horizon
    def forbidden(*a,**kw):raise AssertionError('unexpected model call')
    monkeypatch.setattr(stock_horizon,'thinking_review',forbidden)
    result=stock_horizon.analyze('Example','TEST',frame(),allow_ai=False)
    assert result['review']['status']=='deterministic'
    assert len(result['facts']['horizons'])==5


def test_whole_series_failure_returns_diagnostic(monkeypatch):
    import market_data_helper
    market_data_helper._core()
    import verified_history
    from deep_analysis_data import fetch
    def failed(*a,**kw):raise RuntimeError('offline')
    monkeypatch.setattr(verified_history,'read',failed)
    monkeypatch.setattr(market_data_helper,'fetch_df',failed)
    data,quality=fetch('661.HK')
    assert data is None and quality['data_points']==0 and 'offline' in quality['error_detail']


def test_quarantined_series_does_not_retry_same_provider(monkeypatch):
    import market_data_helper
    market_data_helper._core()
    import verified_history
    from deep_analysis_data import fetch
    quarantined=pd.DataFrame();quarantined.attrs['quality_error']='basis mismatch'
    monkeypatch.setattr(verified_history,'read',lambda *a,**kw:quarantined)
    def forbidden(*a,**kw):raise AssertionError('must preserve quarantine')
    monkeypatch.setattr(market_data_helper,'fetch_df',forbidden)
    data,quality=fetch('GRPN')
    assert data is None and quality['source']=='日线已隔离'


def test_monte_carlo_recompute_is_reproducible_without_global_rng():
    import hashlib
    tree=ast.parse((ROOT/'app_v88_integrated.py').read_text())
    fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='monte_carlo_forecast')
    ns={'np':np,'hashlib':hashlib}
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'monte_carlo','exec'),ns)
    first=ns['monte_carlo_forecast'](frame())
    np.random.normal(size=50)
    assert first==ns['monte_carlo_forecast'](frame()) and first['p10']<first['p90']


def test_detail_keeps_research_contract_precision_and_escapes(tmp_path,monkeypatch):
    from market_data_helper import _core
    _core()
    import stock_verdict,review_display
    from deep_analysis_data import report_html
    row={'tier':'1A','trade_plan':{'entry_range':[.1441,.156], 'take_profit_range':[.179,.18],
          'stop':.144, 'promotion_trigger':'<script>unsafe</script>',
          'profit_contract':{'thesis_deadline':'2026-10-11','holding_sessions':10}}}
    monkeypatch.setattr(stock_verdict,'_triad_record',lambda c: ({},row,False))
    monkeypatch.setattr(review_display,'current_scorecard',lambda s,r: {'total':62.5,'gpt':{'current':True,'complete':True},'books':{'current':True,'complete':True}})
    html=report_html('661.HK',tmp_path)
    assert '0.1441 ～ 0.156' in html and '0.179 ～ 0.18' in html
    assert '中央评级 1A' in html and '不可直接执行' in html
    assert '<script>' not in html and '&lt;script&gt;' in html
    assert '2026-10-11' in html
