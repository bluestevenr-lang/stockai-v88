"""Fast local stock navigation shared by overview and focused deep analysis.

Typing filters a native dropdown in the browser. Only an explicit selection
changes the route. No model, provider, watchlist, ledger or grade writes.
"""
from pathlib import Path
import json


def remember(state, code, *, limit=6):
    previous = state.get('_v88_recent_deep_codes') or []
    state['_v88_recent_deep_codes'] = [code]+[c for c in previous if c != code][:limit-1]


def choose(st, value, catalog):
    from stock_search_index import resolve_exact
    row = resolve_exact(value, catalog)
    if row is None:
        st.session_state['_v88_stock_search_error'] = '没有找到唯一匹配，请输入名称片段后从下拉候选中选择，或核对证券代码。'
        return False
    code = row['code']
    st.session_state.pop('_v88_stock_search_error', None)
    remember(st.session_state, code)
    st.session_state['scan_selected_code'] = code
    st.session_state['scan_selected_name'] = row['name']
    st.session_state['pk_codes'] = []
    st.session_state['pk_names'] = []
    # Callback runs before the app script: its existing early deep dispatch
    # opens the selected stock without rerunning homepage/provider setup.
    st.query_params.update({'q': code, 'focus': 'deep'})
    return True


def recent_rows(state, catalog, history_file=None):
    from stock_search_index import resolve_exact
    found = []
    source = list(state.get('_v88_recent_deep_codes') or [])
    # Reuse old saved navigation history read-only; do not invoke its legacy
    # persistence helper, which also writes the observation/watchlist pools.
    try:
        saved = json.loads(Path(history_file or Path(__file__).with_name('search_history.json')).read_text())
        if isinstance(saved, dict):
            source += [c for c, v in sorted(saved.items(), key=lambda item: -float((item[1] or {}).get('ts') or 0))]
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    for value in source:
        row = resolve_exact(value, catalog)
        if row and all(r['code'] != row['code'] for r in found):
            found.append(row)
        if len(found) == 6:
            break
    return found


def render(st, current_code='', *, key='v88_deep_switch'):
    from stock_search_index import load_catalog, resolve_exact
    catalog = load_catalog()
    rows = catalog['rows']
    by_code = {row['code']: row for row in rows}
    current = resolve_exact(current_code, catalog)
    if current:
        remember(st.session_state, current['code'])
    # URL/history navigation and the visible selected name must stay in sync.
    if current and st.session_state.get(key) != current['code']:
        st.session_state[key] = current['code']
    def select(widget):
        value = st.session_state.get(widget)
        if value:
            choose(st, value, catalog)
    def refresh():
        st.session_state.pop('_v88_stock_search_error', None)
        if current_code:
            st.query_params.update({'q': current['code'] if current else current_code, 'focus': 'deep'})
    def label(value):
        row = by_code.get(value)
        return row['label'] if row else str(value)
    def search_label(value):
        row = by_code.get(value)
        if row and row.get('market') != '美股' and row.get('name_en'):
            return f"{row.get('name_zh') or row['name']} · {row['name_en']} · {row['code']} · {row['market']}"
        return label(value)
    st.markdown('''<style>
    .st-key-v88_deep_searchbar,.st-key-v88_overview_searchbar{padding:9px 12px;border:1px solid #dce6f2;border-left:3px solid #2563eb;border-radius:9px;background:#f5f9ff;margin:5px 0 10px}
    .st-key-v88_deep_searchbar p,.st-key-v88_overview_searchbar p{font-size:12px}
    .st-key-v88_deep_searchbar [data-testid="stCaptionContainer"] p,.st-key-v88_overview_searchbar [data-testid="stCaptionContainer"] p{font-size:11px;color:#64748b}
    </style>''', unsafe_allow_html=True)
    with st.container(key='v88_deep_searchbar' if current_code else 'v88_overview_searchbar'):
        st.markdown('**🔎 搜索并切换个股**')
        left, right = st.columns([5, 1], vertical_alignment='bottom')
        with left:
            st.selectbox('输入名称或代码，选中即分析', options=list(by_code), index=None,
                         format_func=search_label, key=key, on_change=select, args=(key,),
                         placeholder='输入名称 / 代码，选中或按回车即可分析',
                         label_visibility='collapsed', accept_new_options=True)
        with right:
            st.button('↻ 刷新本股', key=key+'_refresh', on_click=refresh,
                      disabled=not bool(current_code), width='stretch',
                      help='重新读取后台最新本地数据，保留当前股票，无需重输名称。')
        if st.session_state.get('_v88_stock_search_error'):
            st.warning(st.session_state['_v88_stock_search_error'])
        recents = recent_rows(st.session_state, catalog)
        if recents:
            recent_key = key+'_recent'
            st.session_state[recent_key] = current['code'] if current else None
            st.pills('最近查看 · 点击切换', options=[r['code'] for r in recents],
                     format_func=label, key=recent_key, on_change=select, args=(recent_key,))
        st.caption(f"名称、英文名或代码均可筛选 · 已下载证券目录 {len(rows):,} 条 · 选中后直接打开分析")
