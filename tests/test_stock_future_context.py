from types import SimpleNamespace
import sys
import pytest


def test_shared_future_uses_deep_calculation_and_disables_network(monkeypatch):
    calls=[];expected={'input_id':'bound','status':'ready'}
    monkeypatch.setitem(sys.modules,'focused_deep_view',SimpleNamespace(
        normalize_code=lambda value:'688002.SS',
        calculate=lambda context,frame,quality,name,code:{'future_scenario':expected}))
    monkeypatch.setitem(sys.modules,'deep_analysis_data',SimpleNamespace(
        fetch=lambda code,**kwargs:(calls.append((code,kwargs)) or ('frame',{'code':code}))))
    monkeypatch.setitem(sys.modules,'deep_cross_validation',SimpleNamespace(load_context=lambda code:{'code':code}))
    import stock_future_context
    assert stock_future_context.for_stock('688002.SH','睿创微纳') is expected
    assert calls==[('688002.SS',{'allow_network':False})]


def test_bad_identity_stops_before_loading_data(monkeypatch):
    monkeypatch.setitem(sys.modules,'focused_deep_view',SimpleNamespace(normalize_code=lambda value:None,calculate=None))
    monkeypatch.setitem(sys.modules,'deep_analysis_data',SimpleNamespace(fetch=lambda *a,**k:(_ for _ in ()).throw(AssertionError('should not fetch'))))
    monkeypatch.setitem(sys.modules,'deep_cross_validation',SimpleNamespace(load_context=lambda *a:(_ for _ in ()).throw(AssertionError('should not load'))))
    import stock_future_context
    doc=stock_future_context.for_stock('../../etc/password')
    assert doc['status']=='missing' and doc['points']==[]


def test_full_context_preserves_original_period_receipt(monkeypatch):
    future={'input_id':'future','status':'ready'}
    period={'input_id':'period','annual_input_id':'annual','synthesis_input_id':'synthesis','code':'TEST'}
    monkeypatch.setitem(sys.modules,'focused_deep_view',SimpleNamespace(
        normalize_code=lambda value:'TEST',
        calculate=lambda *args:{'future_scenario':future,'period_consistency':period}))
    monkeypatch.setitem(sys.modules,'deep_analysis_data',SimpleNamespace(fetch=lambda *a,**k:('frame',{})))
    monkeypatch.setitem(sys.modules,'deep_cross_validation',SimpleNamespace(load_context=lambda code:{'code':code}))
    import stock_future_context
    result=stock_future_context.for_stock_context('TEST')
    assert result['period_consistency'] is period and result['future_scenario'] is future


@pytest.mark.parametrize('error', [ModuleNotFoundError("No module named 'stock_verdict'", name='stock_verdict'),
                                  FileNotFoundError('missing evidence'), PermissionError('denied evidence')])
def test_cloud_missing_local_dependencies_produce_explicit_missing_context(monkeypatch, error):
    calls=[]
    def load(_):
        raise error
    monkeypatch.setitem(sys.modules,'focused_deep_view',SimpleNamespace(
        normalize_code=lambda value:'688002.SS',
        calculate=lambda *args:(_ for _ in ()).throw(AssertionError('must not compute fake future'))))
    monkeypatch.setitem(sys.modules,'deep_analysis_data',SimpleNamespace(
        fetch=lambda *a,**k:calls.append(k)))
    monkeypatch.setitem(sys.modules,'deep_cross_validation',SimpleNamespace(load_context=load))
    import stock_future_context
    result=stock_future_context.for_stock_context('688002.SH','睿创微纳')
    future=result['future_scenario']
    assert result['period_consistency']=={}
    assert future['status']=='missing' and future['points']==[]
    assert future['entry_permission'] is False and future['model_calls']==0
    assert future['gaps'] and ('stock_verdict' in future['gaps'][0] if isinstance(error,ImportError)
                              else type(error).__name__ in future['gaps'][0])
    assert calls==[]


def test_calculation_errors_are_not_silently_reclassified_as_missing_dependencies(monkeypatch):
    monkeypatch.setitem(sys.modules,'focused_deep_view',SimpleNamespace(
        normalize_code=lambda value:'688002.SS',
        calculate=lambda *args:(_ for _ in ()).throw(ValueError('real calculation failure'))))
    monkeypatch.setitem(sys.modules,'deep_analysis_data',SimpleNamespace(fetch=lambda *a,**k:('frame',{})))
    monkeypatch.setitem(sys.modules,'deep_cross_validation',SimpleNamespace(load_context=lambda code:{}))
    import stock_future_context
    with pytest.raises(ValueError,match='real calculation failure'):
        stock_future_context.for_stock_context('688002.SS')
