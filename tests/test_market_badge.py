from html.parser import HTMLParser
import pytest
from market_badge import html

class Nodes(HTMLParser):
    def __init__(self,s):
        super().__init__();self.nodes=[];self.feed(s)
    def handle_starttag(self,t,attrs):self.nodes.append((t,dict(attrs)))

@pytest.mark.parametrize('market',['A股','美股','港股','CN','HK','US'])
def test_svg_market_symbols_need_no_font_or_remote_resource(market):
    s=html(market);nodes=Nodes(s).nodes
    assert sum(t=='svg' for t,a in nodes)==1
    assert {t for t,a in nodes} <= {'span','svg','title','rect','polygon','g','path'}
    assert all(not k.lower().startswith('on') and k not in ('href','src') for t,a in nodes for k in a)
    assert any(t=='svg' and a.get('role')=='img' and a.get('aria-label') for t,a in nodes)

def test_unknown_symbol_is_escaped():
    s=html('<img src=x onerror=bad()>')
    assert Nodes(s).nodes==[] and '&lt;img' in s

@pytest.mark.parametrize('market',['A股','美股','港股'])
def test_native_table_sanitizer_compatible_local_image(market):
    import base64
    from grade_card import _tbl,stock_link
    text=_tbl('<tr><td>'+stock_link('公司','TEST',market)+'</td></tr>')
    images=[a for t,a in Nodes(text).nodes if t=='img']
    assert len(images)==1 and images[0]['src'].startswith('data:image/svg+xml;base64,')
    svg=base64.b64decode(images[0]['src'].split(',',1)[1]).decode()
    assert '<svg' in svg and '<script' not in svg and 'https://' not in svg
