"""Display caps retain complete source records and exact individual access."""
from collections import Counter
from copy import deepcopy
import re


def rows():
    return [{'code':f'{600000+i}.SS' if market=='A股' else f'{1000+i}.HK' if market=='港股' else f'CAP{i}',
             'name':f'样本{market}{i}', 'market':market}
            for market in ('A股','港股','美股') for i in range(9)]


def test_discovery_no_long_hidden_list_and_no_source_mutation():
    from discovery_review_ui import render
    source={'rows':rows()};before=deepcopy(source)
    result=render(source)
    assert result.count('<tr style=') == 15
    assert '展开其余' not in result
    assert all(r['name'] in result for r in source['rows'] if int(re.search(r'(\d+)$',r['name'])[0])<5)
    assert all(r['name'] not in result for r in source['rows'] if int(re.search(r'(\d+)$',r['name'])[0])>=5)
    assert source==before


def test_picker_limits_after_matching_but_keeps_full_catalog_and_exact_access():
    from stock_search_index import build_catalog, display_search, resolve_exact, search
    catalog=build_catalog(rows());before=deepcopy(catalog)
    found=display_search('样本',catalog)
    assert Counter(r['market'] for r in found)=={'A股':5,'港股':5,'美股':5}
    assert len(search('样本',catalog,limit=100))==27
    assert resolve_exact('CAP8',catalog)['code']=='CAP8'
    assert display_search('CAP8',catalog)[0]['code']=='CAP8'
    assert catalog==before


def test_grade_legacy_cap_infers_market_without_reordering_or_mutating():
    from grade_card import _market_rows
    source=rows();[r.pop('market') for r in source];before=deepcopy(source)
    result=_market_rows(source)
    assert len(result)==15
    assert Counter(r['market'] for r in result)=={'A股':5,'港股':5,'美股':5}
    assert [r['code'] for r in result]==[r['code'] for r in rows() if int(re.search(r'(\d+)$',r['name'])[0])<5]
    assert source==before


def test_persistent_history_cap_still_allows_single_stock_lookup(monkeypatch):
    from test_persistent_watchlist_ui import fixture
    from persistent_watchlist_ui import html
    import stock_profile_view
    monkeypatch.setattr(stock_profile_view,'load',lambda *args,**kwargs:{})
    selection,doc,_=fixture()
    base=doc['rows'][0]
    doc['rows']=[{**deepcopy(base),**r} for r in rows()]
    before=deepcopy(doc)
    result=html(doc,selection,view='history')
    assert result.count('class="v88-watch-history-row"')==15
    assert 'data-code="CAP8"' not in result
    assert 'data-code="CAP8"' in html(doc,selection,view='history',code='CAP8')
    assert doc==before


def test_reverse_audit_display_caps_all_markets_and_keeps_audit_totals():
    from test_reverse_audit_ui import inputs
    from reverse_audit_ui import html
    doc,status,selection,now=inputs()
    doc['rows']=[{**r,'current_tier':'1A'} for r in rows()]
    before=deepcopy(doc)
    result=html(doc,status,selection,now)
    assert result.count('focus=deep#v88-deep-analysis')==15
    assert '?q=CAP8' not in result
    assert doc==before


def test_validation_caps_single_market_without_changing_blocked_count():
    from research_validation_view import render
    from test_research_validation_view import View
    doc={'blocked_new_contracts':[{'code':f'CAP{i}','reason':'待证据'} for i in range(12)]}
    before=deepcopy(doc)
    view=View();render(view,doc)
    result='\n'.join(view.lines)
    assert '本轮新合同受阻 12 只' in result
    assert result.count('：待证据')==5
    assert 'CAP4：' in result and 'CAP5：' not in result
    assert doc==before
