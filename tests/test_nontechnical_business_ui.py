from copy import deepcopy
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from v88_paths import core_root
sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(core_root()/'src'),str(core_root()/'tests')]
import nontechnical_reason_ui as ui
from bs4 import BeautifulSoup


def pending(code='EXAMPLE'):
    return {'code':code,'status':'pending','has_current_non_technical_support':False,'evidence':[], 'gaps':['正文待核'],'risks':[]}


def supported(code='EXAMPLE'):
    return {**pending(code),'status':'counterevidence','has_current_non_technical_support':True,
      'support_summary':'经营现金流同比增长','risk_summary':'净利润同比下降','risks':['净利润同比下降'],
      'evidence':[{'direction':'support','claim':'经营现金流同比增长','current':True,'source_url':'https://example.org/issuer.pdf','published_at':'2026-08-25','asof':'2026-06-30'}]}


def test_support_and_counter_are_visible_and_source_is_collapsed():
    soup=BeautifulSoup(ui.html(supported()),'html.parser')
    assert soup.select_one('[data-business-support="true"]')
    assert '⊕ 经营现金流同比增长' in soup.get_text()
    assert '⚠ 净利润同比下降' in soup.get_text()
    assert not soup.select_one('details').has_attr('open')
    assert soup.select_one('details a')['href']=='https://example.org/issuer.pdf'


def test_missing_business_support_holds_action_without_mutating_input():
    r=pending();before=deepcopy(r)
    soup=BeautifulSoup(ui.action_html('<b>现在可进</b>',r),'html.parser')
    assert soup.select_one('.v88-business-action-hold')
    original=soup.select_one('details');assert '现在可进' in original.get_text()
    original.decompose();assert '现在可进' not in soup.get_text()
    assert '仅技术观察' in soup.get_text() and r==before


def test_counterevidence_with_verified_support_is_not_mislabeled_missing():
    r=supported()
    assert ui.action_html('<b>等待原条件</b>',r)=='<b>等待原条件</b>'
    assert '经营依据待核' not in ui.html(r)


def test_html_data_escaped_and_nonhttps_sources_omitted():
    r=supported();r['support_summary']='<script>alert(1)</script>'
    r['evidence'][0]['source_url']='javascript:alert(1)'
    soup=BeautifulSoup(ui.html(r),'html.parser')
    assert not soup.select('script') and not soup.select('a')
    assert '<script>alert(1)</script>' in soup.get_text()


def test_read_cache_builds_once_for_many_rows_and_refreshes_for_new_generation(tmp_path,monkeypatch):
    (tmp_path/'data').mkdir();file=tmp_path/'data/nontechnical_primary_reasons.json';file.write_text('{}')
    called=[]
    monkeypatch.setattr(ui,'_api',lambda:(lambda root:called.append(root) or {'records':{}},None))
    ui._CACHE.clear()
    for _ in range(100):ui.load_index(tmp_path)
    assert len(called)==1
    file.write_text('{"changed":true}')
    ui.load_index(tmp_path)
    assert len(called)==2


def test_missing_core_reader_keeps_contract_visible_and_withholds_business_support(monkeypatch):
    def missing():
        raise ImportError('portable core module not synced')
    monkeypatch.setattr(ui,'_api',missing)
    r=ui.for_code('688002.SS',{})
    assert r['status']=='pending' and not ui.has_support(r)
    assert r['no_grade_authority'] is True
    assert '经营依据暂未读取成功' in ui.html(r)
    rendered=ui.action_html('<b>原合同止损 153.41</b>',r)
    assert '原合同止损 153.41' in rendered and '仅技术观察' in rendered


def test_failed_or_malformed_reader_never_reuses_prior_support(monkeypatch):
    def failed(*args):
        raise ValueError('receipt failed')
    for resolver in (failed,lambda *args:None):
        monkeypatch.setattr(ui,'_api',lambda:(None,resolver))
        r=ui.for_code('000333.SZ',{'previous':supported()})
        assert r['status']=='pending' and not ui.has_support(r)
        assert r['evidence']==[]


def test_persistent_table_has_same_reason_and_keeps_original_score(monkeypatch):
    import test_watchlist_view_authority as fixture_module
    from persistent_watchlist_ui import html
    doc,selection=fixture_module.fixture(monkeypatch)
    import grade_focus
    doc['current_focus']['rows'][0]['focus_rank']=1
    monkeypatch.setattr(grade_focus,'build',lambda *a,**kw:{'records':{'EXAMPLE':{
        'central_rank':1,'rank':1,'selected':True,'entry_opportunity':{
            'focus_eligible':False,'state':'WATCH','label':'研究跟踪','reasons':['等待量价确认']}}}})
    original=deepcopy(doc)
    monkeypatch.setattr(ui,'load_index',lambda *a:{})
    monkeypatch.setattr(ui,'for_code',lambda code,*a:supported(code))
    rendered=html(doc,selection,view='current')
    assert 'data-business-support="true"' in rendered
    assert '经营现金流同比增长' in rendered and '净利润同比下降' in rendered
    assert '审核分 70/100' in rendered and '原审核分' not in rendered and doc==original


def test_deep_header_uses_same_missing_reason_without_touching_contract(monkeypatch,tmp_path):
    import deep_analysis_data
    monkeypatch.setattr(ui,'load_index',lambda *a:{})
    monkeypatch.setattr(ui,'for_code',lambda code,*a:pending(code))
    ctx={'selection':{},'row':{'code':'EXAMPLE','name':'Example','tier':'1A','trade_plan':{'entry_range':[10,11],'stop':9,'take_profit_range':[13,14]}},'formal':True,'card':{}}
    before=deepcopy(ctx)
    out=deep_analysis_data.report_html('EXAMPLE',data_dir=tmp_path,context=ctx)
    assert 'data-business-support="false"' in out and '仅技术观察' in out
    assert '10 ～ 11' in out and '失效价 9' in out and ctx==before


def test_deep_header_cannot_render_nan_or_boolean_as_contract_price(monkeypatch,tmp_path):
    import deep_analysis_data
    monkeypatch.setattr(ui,'load_index',lambda *a:{})
    monkeypatch.setattr(ui,'for_code',lambda code,*a:pending(code))
    ctx={'selection':{},'row':{'code':'EXAMPLE','name':'Example','tier':'1A','trade_plan':{
        'entry_range':[False,float('nan')],'stop':float('inf'),
        'take_profit_range':[0,-1],
        'profit_contract':{'net_upside_pct':float('nan'),'net_reward_risk':True}}},
        'formal':False,'card':{}}
    out=deep_analysis_data.report_html('EXAMPLE',data_dir=tmp_path,context=ctx)
    assert '入场 未核实 ～ 未核实' in out and '失效价 未核实' in out
    assert '原合同净空间 未核实' in out and '未核实%' not in out
    assert '净收益风险比 未核实' in out


def test_actual_public_reasons_keep_midea_counter_and_support():
    ui._CACHE.clear();idx=ui.load_index()
    r=ui.for_code('000333.SZ',idx)
    assert r['has_current_non_technical_support'] is True
    assert r['status']=='counterevidence'
    out=ui.html(r)
    assert '3.55%' in out and '25.31%' in out
