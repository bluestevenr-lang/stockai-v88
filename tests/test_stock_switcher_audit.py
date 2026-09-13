"""Independent render/callback checks; no Streamlit server or market requests."""
from copy import deepcopy
import stock_switcher as ui
from stock_search_index import build_catalog


class FakeSt:
    def __init__(self, state=None, params=None):
        self.session_state = dict(state or {})
        self.query_params = dict(params or {})
        self.widgets = {}
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def container(self, **kwargs): return self
    def columns(self, *args, **kwargs): return self, self
    def markdown(self, *args, **kwargs): pass
    def caption(self, *args, **kwargs): pass
    def warning(self, *args, **kwargs): pass
    def selectbox(self, label, **kwargs): self.widgets[kwargs['key']] = kwargs
    def pills(self, label, **kwargs): self.widgets[kwargs['key']] = kwargs
    def button(self, label, **kwargs): self.widgets[kwargs['key']] = kwargs


def fixture_catalog():
    return build_catalog([
        {'code':'688002.SS', 'name':'睿创微纳', 'market':'A股'},
        {'code':'0700.HK', 'name':'TENCENT', 'market':'港股'},
        {'code':'NVDA', 'name':'NVIDIA Corporation', 'market':'美股'},
    ], {'records':{'700.HK':{'code':'700.HK', 'market':'港股', 'name_zh':'腾讯控股',
                           'name_en':'Tencent Holdings Limited'}}})


def prepare(monkeypatch, state=None, params=None):
    catalog = fixture_catalog()
    import stock_search_index
    monkeypatch.setattr(stock_search_index, 'load_catalog', lambda: catalog)
    monkeypatch.setattr(ui, 'recent_rows', lambda state, _catalog: [
        catalog['by_code'][code] for code in state.get('_v88_recent_deep_codes', [])
        if code in catalog['by_code']])
    return FakeSt(state, params), catalog


def test_url_alias_synchronizes_current_picker_without_touching_financial_state(monkeypatch):
    state={'picker':'NVDA', 'ledger':{'position':7}, 'watchlist':['NVDA'],
           'pending_reviews':{'688002.SS':{'score':70}}}
    st, catalog=prepare(monkeypatch,state,{'q':'00700.HK','focus':'deep'})
    preserved=deepcopy({k:state[k] for k in ('ledger','watchlist','pending_reviews')})
    ui.render(st,'00700.HK',key='picker')
    assert st.session_state['picker']=='0700.HK'
    assert all(st.session_state[k]==v for k,v in preserved.items())
    assert st.widgets['picker']['format_func']('0700.HK').startswith('腾讯控股')
    assert st.widgets['picker']['accept_new_options'] is True
    assert len(st.widgets['picker']['options'])==3


def test_unknown_input_callback_keeps_current_route_and_records_visible_error(monkeypatch):
    st,_=prepare(monkeypatch,{'picker':'NVDA'}, {'q':'NVDA','focus':'deep'})
    ui.render(st,'NVDA',key='picker')
    st.session_state['picker']='不存在的股票'
    widget=st.widgets['picker']
    widget['on_change'](*widget['args'])
    assert st.query_params=={'q':'NVDA','focus':'deep'}
    assert '_v88_stock_search_error' in st.session_state
    ui.render(st,'NVDA',key='picker')
    assert st.session_state['picker']=='NVDA'
    assert '_v88_stock_search_error' in st.session_state


def test_refresh_callback_keeps_exact_stock_and_clears_only_search_error(monkeypatch):
    state={'picker':'0700.HK', '_v88_stock_search_error':'old', 'reviews':{'unchanged':True}}
    st,_=prepare(monkeypatch,state,{'q':'00700.HK','focus':'deep','other':'kept'})
    ui.render(st,'00700.HK',key='picker')
    st.widgets['picker_refresh']['on_click']()
    assert st.query_params=={'q':'0700.HK','focus':'deep','other':'kept'}
    assert st.session_state['reviews']=={'unchanged':True}
    assert '_v88_stock_search_error' not in st.session_state


def test_keyboard_selection_callback_routes_before_deep_calculation(monkeypatch):
    st,_=prepare(monkeypatch,{'picker':'NVDA'}, {'q':'NVDA','focus':'deep'})
    ui.render(st,'NVDA',key='picker')
    st.session_state['picker']='688002.SH'
    w=st.widgets['picker'];w['on_change'](*w['args'])
    assert st.query_params=={'q':'688002.SS','focus':'deep'}
    assert st.session_state['scan_selected_name']=='睿创微纳'
    assert st.session_state['_v88_recent_deep_codes'][0]=='688002.SS'
