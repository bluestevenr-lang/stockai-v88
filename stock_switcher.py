"""Fast local stock navigation shared by overview and focused deep analysis.

Local name/code search runs in an isolated UI fragment; market filtering
does not recompute the deep report. Explicit selection changes the route. No model, provider, watchlist, ledger or grade writes.
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
        st.session_state['_v88_stock_search_error'] = '没有找到唯一匹配，请输入名称片段后从匹配结果中选择，或核对证券代码。'
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
    from display_limits import market_top
    return market_top(found)


def render(st, current_code='', *, key='v88_deep_switch'):
    @st.fragment
    def search_panel():
        _render(st, current_code, key=key)
    search_panel()


def _render(st, current_code='', *, key='v88_deep_switch'):
    navigate = st.session_state.pop(key+'_navigate', False)
    from stock_search_index import load_catalog, resolve_exact, dropdown_label
    from html import escape
    catalog = load_catalog()
    current = resolve_exact(current_code, catalog)
    if current:
        remember(st.session_state, current['code'])

    def open_stock(code):
        if choose(st, code, catalog):
            st.session_state[key+'_navigate'] = True

    def select_stock():
        code = st.session_state.get(key+'_picker')
        if code:
            open_stock(code)

    def refresh():
        st.session_state.pop('_v88_stock_search_error', None)
        if current_code:
            st.query_params.update({'q': current['code'] if current else current_code, 'focus': 'deep'})
            st.session_state[key+'_navigate'] = True

    st.markdown("""<style>
    .st-key-v88_deep_searchbar,.st-key-v88_overview_searchbar{padding:16px 20px;border:1px solid #93c5fd;border-top:4px solid #2563eb;border-radius:12px;background:linear-gradient(120deg,#eff6ff,#fff);margin:4px 0 16px}
    [class*="_hit_"] button{justify-content:flex-start;text-align:left}
    .v88-search-title{font-size:22px;font-weight:800;color:#153e75;margin-bottom:4px}
    .v88-search-current{font-size:13px;color:#475569;margin-bottom:10px}
    .st-key-v88_deep_searchbar [data-testid="stTextInput"] input,.st-key-v88_overview_searchbar [data-testid="stTextInput"] input{font-size:17px;min-height:46px;background:white}
    .st-key-v88_deep_searchbar [data-testid="stTextInput"] label p,.st-key-v88_overview_searchbar [data-testid="stTextInput"] label p{font-size:14px;font-weight:650;color:#1e3a5f}
    .st-key-v88_deep_searchbar button p,.st-key-v88_overview_searchbar button p{font-size:14px}
    .st-key-v88_deep_searchbar [data-testid="stCaptionContainer"] p,.st-key-v88_overview_searchbar [data-testid="stCaptionContainer"] p{font-size:12px;color:#64748b}
    </style>""", unsafe_allow_html=True)
    with st.container(key='v88_deep_searchbar' if current_code else 'v88_overview_searchbar'):
        st.markdown('<div class="v88-search-title">🔎 查找个股 · 打开深度分析</div>', unsafe_allow_html=True)
        if current:
            st.markdown('<div class="v88-search-current">当前查看：'+escape(current['label'])+'</div>', unsafe_allow_html=True)
        # Native searchable dropdown filters immediately in the browser.
        # Include the complete catalog: matching symbols are not Top5-truncated.
        labels = {row['code']: dropdown_label(row) for row in catalog['rows']}
        st.selectbox('名称 / 代码 / 拼音简写', options=list(labels), index=None,
                     format_func=lambda code: labels.get(code, code), key=key+'_picker',
                     placeholder='输入 zgjs、rcwn、腾讯或代码，下拉选择个股',
                     on_change=select_stock, width='stretch')
        if st.session_state.get('_v88_stock_search_error'):
            st.warning(st.session_state['_v88_stock_search_error'])
        recents = recent_rows(st.session_state, catalog)
        if recents:
            st.caption('🕘 最近查看 · 点击直达')
            for offset in range(0,len(recents),3):
                columns = st.columns(3)
                for column, row in zip(columns,recents[offset:offset+3]):
                    with column:
                        title = row['name_zh'] or row['name']
                        if row['market']=='美股':
                            title = (row.get('name_en') or row['code']) + (' · '+row['name_zh'] if row['name_zh'] else '')
                        st.button(title+' · '+row['code'], key=key+'_recent_'+row['code'],
                                  on_click=open_stock, args=(row['code'],), width='stretch')
        if current_code:
            st.button('↻ 更新当前分析', key=key+'_refresh', on_click=refresh,
                      help='读取后台最新结果，复用仍有效的评分。')

    # Register widgets before escalating a fragment navigation to a full rerun;
    # aborting earlier makes Streamlit discard the dropdown selection.
    if navigate:
        st.rerun(scope='app')
