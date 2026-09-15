"""Search routing and filtering against real Streamlit widget state."""
from pathlib import Path
from streamlit.testing.v1 import AppTest


def app():
    return AppTest.from_string('''
import streamlit as st
import stock_switcher
from stock_search_index import build_catalog
catalog = build_catalog([
 {'code':'000100.SZ','name':'TCL科技','market':'A股'},
 {'code':'1070.HK','name':'TCL电子','market':'港股'},
 {'code':'NVDA','name':'NVIDIA Corporation','market':'美股'}])
import stock_search_index
from unittest.mock import patch
with patch.object(stock_search_index, 'load_catalog', return_value=catalog), patch.object(stock_switcher, 'recent_rows', return_value=[]):
 stock_switcher.render(st, st.query_params.get('q',''), key='picker')
''').run()


def test_partial_name_results_are_clickable_and_market_filter_does_not_navigate():
    at=app()
    at.text_input[0].input('TCL').run()
    hits=[b for b in at.button if b.key.startswith('picker_hit_')]
    assert len(hits)==2 and not at.query_params
    at.radio[0].set_value('港股').run()
    assert len([b for b in at.button if b.key.startswith('picker_hit_')])==1
    assert not at.query_params
    at.button(key='picker_hit_1070.HK').click().run()
    assert at.query_params['q']==['1070.HK'] and not at.exception
    assert at.radio[0].value=='港股'
    assert len([b for b in at.button if b.key.startswith('picker_hit_')])==1


def test_exact_code_enters_without_refresh_and_unmatched_search_preserves_stock():
    at=app()
    at.text_input[0].input('000100').run()
    assert at.query_params['q']==['000100.SZ']
    at.text_input[0].input('不存在').run()
    assert at.query_params['q']==['000100.SZ']
    assert any('未找到' in msg.value for msg in at.info)
    assert not at.exception


def test_refresh_keeps_current_stock():
    at=app()
    at.text_input[0].input('1070.HK').run()
    at.button(key='picker_refresh').click().run()
    assert at.query_params['q']==['1070.HK'] and not at.exception
