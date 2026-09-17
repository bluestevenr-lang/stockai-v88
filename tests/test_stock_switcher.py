from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import json

import stock_switcher as ui
from stock_search_index import build_catalog


def catalog():
    return build_catalog([
        {'code':'688002.SS','name':'睿创微纳','market':'A股'},
        {'code':'700.HK','name':'腾讯控股','market':'港股'},
        {'code':'NVDA','name':'NVIDIA Corporation','market':'美股'},
        {'code':'301001.SZ','name':'同名','market':'A股'},
        {'code':'301002.SZ','name':'同名','market':'A股'},
    ])


def test_choose_changes_only_navigation_and_keeps_comparison_and_trade_history_separate():
    st=SimpleNamespace(session_state={'pk_codes':['OLD'], 'pk_names':['旧'], 'ledger':{'unchanged':1},
                                      'watchlist':['OLD'], 'compare_basket':[('OLD','旧')]},
                       query_params={'q':'OLD','focus':'overview','other':'kept'})
    rows=catalog();before=deepcopy(rows)
    assert ui.choose(st,'00700.HK',rows)
    assert st.query_params=={'q':'0700.HK','focus':'deep','other':'kept'}
    assert st.session_state['scan_selected_code']=='0700.HK'
    assert st.session_state['scan_selected_name']=='腾讯控股'
    assert st.session_state['pk_codes']==[] and st.session_state['pk_names']==[]
    assert st.session_state['ledger']=={'unchanged':1} and st.session_state['watchlist']==['OLD']
    assert st.session_state['compare_basket']==[('OLD','旧')]
    assert st.session_state['_v88_recent_deep_codes']==['0700.HK'] and rows==before


def test_unknown_or_ambiguous_query_never_changes_the_current_stock():
    for value in ['同名','不存在','<script>alert(1)</script>','688002.BAD']:
        st=SimpleNamespace(session_state={'scan_selected_code':'NVDA'},query_params={'q':'NVDA','focus':'deep'})
        assert not ui.choose(st,value,catalog())
        assert st.query_params['q']=='NVDA' and st.session_state['scan_selected_code']=='NVDA'
        assert '唯一匹配' in st.session_state['_v88_stock_search_error']


def test_recent_history_is_bounded_deduplicated_and_saved_history_is_read_only(tmp_path):
    state={}
    for code in ['NVDA','0700.HK','688002.SS','NVDA']:
        ui.remember(state,code)
    file=tmp_path/'history.json';file.write_text(json.dumps({'700.HK':{'name':'腾讯','ts':1},'STALE':{'ts':3}}))
    before=file.read_bytes()
    found=ui.recent_rows(state,catalog(),file)
    assert [r['code'] for r in found]==['NVDA','688002.SS','0700.HK']
    assert file.read_bytes()==before
    for i in range(20):ui.remember(state,str(i))
    assert len(state['_v88_recent_deep_codes'])==6


def test_directory_search_remains_local_and_overview_uses_same_switcher():
    source=(Path(__file__).resolve().parents[1]/'app_v88_integrated.py').read_text()
    block=source.split('def render_cloud_search():',1)[1].split('\ndef render_clickable_table',1)[0]
    assert 'from stock_switcher import render' in block
    assert 'searchapi.eastmoney.com' not in block and '_DIRECT_SESSION.get' not in block
    focused=(Path(__file__).resolve().parents[1]/'focused_deep_view.py').read_text()
    assert focused.index('render_stock_switcher(st,') < focused.index('context = load_context(code)')


def test_dropdown_has_full_catalog_and_selection_alone_navigates():
    from streamlit.testing.v1 import AppTest
    script='''
import streamlit as st
import stock_switcher
import stock_search_index
catalog=stock_search_index.build_catalog([
 {'code':'600176.SS','name':'中国巨石','market':'A股'},
 {'code':'1380.HK','name':'中国金石','market':'港股'},
 *[{'code':f'600{n:03}.SS','name':f'测试公司{n}','market':'A股'} for n in range(20)]])
stock_search_index.load_catalog=lambda:catalog
stock_switcher._render(st,'',key='test_picker')
'''
    app=AppTest.from_string(script).run()
    assert not app.exception and not app.text_input and not app.radio
    picker=app.selectbox[0]
    assert picker.value is None and len(picker.options)==22
    assert len([s for s in picker.options if 'zgjs' in s])==2
    assert all(b.label!='搜索 →' for b in app.button)
    picker.select('600176.SS').run()
    assert not app.exception and app.query_params['q']==['600176.SS']
