"""Keep signed current results before discovery/risk and historical content."""
from bs4 import BeautifulSoup


def test_current_board_precedes_alerts_discovery_and_reserves(monkeypatch):
    import grade_card, persistent_watchlist_ui, market_adaptation_ui, market_watch_ui
    markers={'current':'audit-current','tracking':'audit-reserves','history':'audit-archive'}
    monkeypatch.setattr(persistent_watchlist_ui,'html',
        lambda *args,**kwargs:'<div id="'+markers[kwargs['view']]+'"></div>')
    monkeypatch.setattr(market_adaptation_ui,'html',lambda *args:'<section id="v88-market-adaptation"></section>')
    monkeypatch.setattr(market_watch_ui,'html',lambda *args:'<section id="v88-market-watch"></section>')
    html=grade_card.system_table_html({}, {}, {}, {},watchlist={})
    order=['audit-current','v88-market-adaptation','v88-market-watch','audit-reserves','audit-archive']
    soup=BeautifulSoup(html,'html.parser')
    found=[tag.get('id') for tag in soup.find_all(id=True) if tag.get('id') in order]
    assert found==order
    assert [a['href'] for a in soup.select('.v88-section-nav a')]==[
        '#v88-grade-list','#v88-market-adaptation','#v88-market-watch']


def test_rubric_distinguishes_research_windows_from_original_contract_pricing():
    from scorecard_html import rubric_html
    text=BeautifulSoup(rubric_html(),'html.parser').get_text()
    assert '原合同核价口径' in text
    assert '短期0–8周、中期8–24周、长期12–36周' in text
    assert '原短期合同≤30自然日' in text
    assert '原中期合同31–90自然日' in text
    assert '原长期合同91–365自然日' in text
