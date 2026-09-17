"""Focused navigation cannot execute expensive homepage setup or create authority."""
import ast
from copy import deepcopy
from pathlib import Path
import sys
import json
from contextlib import nullcontext
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from focused_deep_view import calculate, navigation, normalize_code


@pytest.mark.parametrize('code', ['3330.HK', '688002.SS', 'GRPN'])
def test_explicit_deep_dispatch_stops_before_all_homepage_imports(monkeypatch, code):
    tree = ast.parse((Path(__file__).resolve().parents[1] / 'app_v88_integrated.py').read_text())
    # Execute the actual app entry through the first heavyweight import. It must
    # stop before that import can run, not merely hide its output in an expander.
    boundary = next(i for i, node in enumerate(tree.body)
                    if isinstance(node, ast.Import) and node.names[0].name == 'pandas')
    calls = []
    class Stopped(Exception):
        pass
    def stop():
        raise Stopped()
    st = SimpleNamespace(query_params={'focus': 'deep', 'q': code}, stop=stop)
    monkeypatch.setitem(sys.modules, 'streamlit', st)
    monkeypatch.setitem(sys.modules, 'focused_deep_view', SimpleNamespace(
        render=lambda actual_st, symbol: calls.append((actual_st, symbol))))
    with pytest.raises(Stopped):
        exec(compile(ast.Module(body=tree.body[:boundary], type_ignores=[]), 'app-route', 'exec'), {})
    assert calls == [(st, code)]


@pytest.mark.parametrize('params', [{}, {'q': '3330.HK'}, {'focus': 'overview', 'q': '3330.HK'}])
def test_overview_navigation_keeps_original_path(monkeypatch, params):
    source = (Path(__file__).resolve().parents[1] / 'app_v88_integrated.py').read_text()
    prefix = source[:source.index('import pandas as pd')]
    def prohibited(*args):
        pytest.fail('ordinary overview was redirected')
    monkeypatch.setitem(sys.modules, 'streamlit', SimpleNamespace(query_params=params, stop=prohibited))
    monkeypatch.setitem(sys.modules, 'focused_deep_view', SimpleNamespace(render=prohibited))
    exec(compile(prefix, 'app-route', 'exec'), {})
    # Original homepage modules remain present beyond this early branch.
    assert 'RAW_US, RAW_HK, RAW_CN_TOP = init_stock_pools()' in source
    assert '# ===V88_PAGE_BREAK:RESEARCH===' in source


@pytest.mark.parametrize('code', ['3330.HK', '688002.SS', 'GRPN'])
def test_focused_technical_result_matches_original_shared_engine(monkeypatch, code):
    from market_data_helper import _core
    _core()
    import cloud_engine
    import stock_horizon
    from deep_analysis_data import snapshot_signature
    from v88_decision_core import evaluate_decision
    def prohibited(*a, **kw):
        pytest.fail('focused calculation attempted model or network')
    monkeypatch.setattr(stock_horizon, 'thinking_review', prohibited)
    monkeypatch.setattr(cloud_engine, 'fetch', prohibited)
    values = np.linspace(10, 15, 220)
    frame = pd.DataFrame({'Open': values, 'High': values + .3, 'Low': values - .3,
                          'Close': values, 'Volume': np.full(220, 1e6)},
                         index=pd.bdate_range(end='2026-09-11', periods=220))
    ctx = {'code': code, 'row': {'code': code, 'trade_plan': {'stop': 9, 'thesis_deadline': '2026-09-30'}},
           'selection': {}, 'card': {}, 'formal': False}
    before = deepcopy(ctx)
    quality = {'code': code, 'source_asof': '2026-09-11', 'snapshot_signature': snapshot_signature(frame)}
    result = calculate(ctx, frame, quality, code, code)
    expected = evaluate_decision(frame, cloud_engine.analyze_trend_full(frame), name=code, code=code)
    assert result['technical'] == expected
    assert result['cycles']['review']['status'] == 'deterministic'
    assert len(result['cycles']['facts']['horizons']) == 5
    assert result['synthesis']['model_calls'] == 0 and not result['synthesis']['entry_permission']
    assert result['annual_outlook']['model_calls'] == 0
    assert result['annual_outlook']['no_grade_authority'] is True
    assert result['annual_outlook']['entry_permission'] is False
    assert len(result['annual_outlook']['phases']) == 4
    assert result['cross']['no_grade_authority'] and ctx == before


def test_missing_history_keeps_original_contract_and_explicit_gaps():
    from market_data_helper import _core
    _core()
    ctx = {'code': 'GRPN', 'row': {'trade_plan': {'stop': 10}}, 'selection': {}, 'card': {}}
    before = deepcopy(ctx)
    result = calculate(ctx, None, {}, 'GRPN', 'GRPN')
    assert result['technical'] == {} and result['cycles'] == {}
    assert result['cross']['status'] == '需复核'
    assert result['cross']['technical_score'] is None
    assert result['cross']['current_price_scenario'] is None and ctx == before
    assert result['annual_outlook']['gaps'] and len(result['annual_outlook']['phases']) == 4
    assert result['annual_outlook']['no_grade_authority'] is True
    assert result['annual_outlook']['entry_permission'] is False


def test_navigation_clears_focus_and_preserves_explicit_full_research_link():
    text = navigation('3330.HK')
    assert 'href="/"' in text and 'href="/?q=3330.HK#v88-deep-analysis"' in text
    assert 'focus=deep' not in text
    assert normalize_code('<script>') is None and normalize_code('../data') is None
    assert normalize_code('  grpn ') == 'GRPN'


def test_renderer_uses_bounded_single_stock_recovery_and_never_imports_main_engine():
    source = (Path(__file__).resolve().parents[1] / 'focused_deep_view.py').read_text()
    tree = ast.parse(source)
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    fetches = [node for node in calls if ast.unparse(node.func) == 'fetch']
    assert len(fetches) == 1
    assert any(k.arg == 'allow_network' and isinstance(k.value, ast.Constant) and k.value.value is True
               for k in fetches[0].keywords)
    assert 'load_for_view(code,' in source and 'max_entries=64' in source and 'ttl=120' in source
    assert not any(ast.unparse(n.func) in {'verdict', 'call_model_api', 'init_stock_pools', 'exec'} for n in calls)
    assert not any(isinstance(n, ast.ImportFrom) and n.module == 'app_v88_integrated' for n in ast.walk(tree))


@pytest.mark.parametrize('row,warning', [
    ({'level': '0', 'action': '持有不动'}, False),
    ({'level': '-1A', 'verification': {'review_ok': False}}, True),
    ({'level': '0', 'bypass': ['破止损']}, True),
])
def test_sell_records_preserve_source_time_without_warning_on_normal_rows(monkeypatch, tmp_path, row, warning):
    import market_data_helper
    from focused_deep_view import _sell_evidence
    (tmp_path / 'data').mkdir()
    doc = {'generated_at': '2026-09-11 18:00', 'rows': [{'code': '3330.HK', **row}]}
    (tmp_path / 'data' / 'sell_grade.json').write_text(json.dumps(doc))
    monkeypatch.setattr(market_data_helper, 'CORE', tmp_path)
    warnings, captions, originals = [], [], []
    st = SimpleNamespace(warning=warnings.append, caption=captions.append, json=originals.append,
                         expander=lambda *a, **kw: nullcontext())
    _sell_evidence(st, '3330.HK')
    assert bool(warnings) == warning and originals == doc['rows']
    assert '2026-09-11 18:00' in captions[0]


def test_annual_plan_is_built_after_synthesis_even_without_history(monkeypatch):
    from market_data_helper import _core
    _core()
    import deep_synthesis
    from focused_deep_view import calculate
    calls=[]
    annual={'version':'annual-test','code':'GRPN','phases':[{'quarter':i} for i in range(1,5)],
            'gaps':['缺完整行情'], 'no_grade_authority':True,'entry_permission':False,'model_calls':0}
    def annual_build(context,frame,quality,synthesis=None,now=None):
        calls.append((context,frame,quality,synthesis))
        return deepcopy(annual)
    import annual_outlook
    monkeypatch.setattr(annual_outlook, 'build', annual_build)
    context={'code':'GRPN','row':{'trade_plan':{'stop':10}},'selection':{},'card':{}}
    before=deepcopy(context);quality={'source':'local','error_detail':'历史待补'}
    result=calculate(context,None,quality,'GRPN','GRPN')
    assert result['annual_outlook']==annual and len(calls)==1
    assert calls[0][0] is context and calls[0][1] is None and calls[0][2] is quality
    assert calls[0][3] is result['synthesis']
    assert context==before and result['cross']['no_grade_authority']
    assert result['annual_outlook']['model_calls']==0


def view_recorder():
    calls=[];downloads=[];plots=[]
    def expander(label,**kw):
        calls.append(('expander',label));return nullcontext()
    st=SimpleNamespace(
        markdown=lambda text,**kw:calls.append(('markdown',text)),
        caption=lambda text:calls.append(('caption',text)),
        warning=lambda text:calls.append(('warning',text)),
        expander=expander,dataframe=lambda *args,**kw:None,
        download_button=lambda label,data,**kw:downloads.append((label,json.loads(data),kw)),
        plotly_chart=lambda fig,**kw:plots.append((fig,kw)))
    return st,calls,downloads,plots


def install_view_engines(monkeypatch):
    monkeypatch.setitem(sys.modules,'annual_outlook',SimpleNamespace(
        html=lambda doc:'<section class="v88-annual-outlook">未来一年：'+str(doc.get('phases'))+'</section>'))
    monkeypatch.setitem(sys.modules,'deep_cross_validation',SimpleNamespace(html=lambda doc:'<p>中央核验</p>'))
    monkeypatch.setitem(sys.modules,'deep_synthesis',SimpleNamespace(html=lambda doc,details=False:'<p>联合判断</p>'))
    monkeypatch.setitem(sys.modules,'cloud_engine',SimpleNamespace(plain_readout=lambda *args:[]))
    monkeypatch.setitem(sys.modules,'stock_horizon',SimpleNamespace(
        cycle_visual_html=lambda *args:'<p>未来圆周与曲线</p>',table_rows=lambda *args:[]))


def test_missing_history_still_displays_annual_gaps_and_exports_full_evidence(monkeypatch):
    from focused_deep_view import _technical_view
    install_view_engines(monkeypatch)
    annual={'phases':[{'quarter':i,'status':'待证据'} for i in range(1,5)],
            'no_grade_authority':True,'entry_permission':False,'gaps':['缺行情']}
    result={'cross':{'no_grade_authority':True,'central_grade':'1A'},'synthesis':{},
            'annual_outlook':annual,'technical':{},'trend':{},'cycles':{}}
    before=deepcopy(result)
    st,calls,downloads,plots=view_recorder()
    _technical_view(st,result,None,{'error_detail':'缺完整日线'},'GRPN','GRPN')
    year_index=next(i for i,v in enumerate(calls) if 'v88-annual-outlook' in str(v))
    missing_index=next(i for i,v in enumerate(calls) if v[0]=='warning')
    assert year_index<missing_index
    assert len(downloads)==1 and downloads[0][1]['annual_outlook']==annual
    assert downloads[0][1]['central_grade']=='1A' and result==before
    assert downloads[0][2]['file_name']=='V88-GRPN-cross-validation.json' and not plots


@pytest.mark.parametrize('timezone',[None,'Asia/Shanghai','America/New_York'])
def test_history_window_uses_one_actual_calendar_year_without_adding_forecast_points(timezone):
    from focused_deep_view import history_window
    frame=pd.DataFrame({'Close':np.arange(600)},index=pd.bdate_range(end='2026-09-11',periods=600,tz=timezone))
    before=frame.copy(deep=True)
    recent=history_window(frame)
    assert recent.index.min()>=frame.index.max()-pd.DateOffset(years=1)
    assert recent.index.max()==frame.index.max() and len(recent)<len(frame)
    pd.testing.assert_frame_equal(recent,frame.loc[recent.index])
    pd.testing.assert_frame_equal(frame,before)
    short=frame.tail(50)
    pd.testing.assert_frame_equal(history_window(short),short)


def test_year_outlook_precedes_technical_sections_without_duplicate_candlesticks(monkeypatch):
    from focused_deep_view import _technical_view,history_window
    install_view_engines(monkeypatch)
    values=np.arange(600)+10
    frame=pd.DataFrame({'Open':values,'High':values+1,'Low':values-1,'Close':values+.5,'Volume':1000},
                       index=pd.bdate_range(end='2026-09-11',periods=600))
    result={'cross':{'no_grade_authority':True},'synthesis':{},'annual_outlook':{'phases':[1,2,3,4]},
            'technical':{'unified_score':60},'trend':{},'cycles':{}}
    st,calls,downloads,plots=view_recorder()
    _technical_view(st,result,frame,{'source_asof':'2026-09-11','source':'verified local'},'测试','TEST')
    annual_index=next(i for i,v in enumerate(calls) if 'v88-annual-outlook' in str(v))
    cross_index=next(i for i,v in enumerate(calls) if '中央核验' in str(v))
    technical_index=next(i for i,v in enumerate(calls) if '量价辅助分' in str(v))
    cycle_index=next(i for i,v in enumerate(calls) if '未来圆周与曲线' in str(v))
    assert cycle_index<annual_index<technical_index
    assert annual_index<cross_index
    assert any(v[0]=='expander' and '历史量价计算明细' in v[1] for v in calls)
    assert any('本地计算与绘图不调用GPT' in str(v) for v in calls)
    assert plots == []
    assert downloads[0][1]['annual_outlook']==result['annual_outlook']
