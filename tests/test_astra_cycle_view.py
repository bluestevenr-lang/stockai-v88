from datetime import datetime
from astra_cycle_view import html

NOW=datetime.fromisoformat('2026-09-28T14:00:00+08:00')


def test_cycle_view_keeps_prices_explanation_counts_and_escapes_names():
    from astra_cycle import build
    from test_astra_cycle import q
    quote=q();quote['name']='<script>alert(1)</script>'
    d=build({'rows':[quote]},now=NOW)
    result=html(d,now=NOW)
    for expected in ['研究 1 只','每月1、10、20日重算','2026-10-01','研究入场','止盈','止损','净空间','截止','行业排名','未对账不计零']:
        assert expected in result
    assert '<script>' not in result and '&lt;script&gt;' in result
    assert '0只可买' not in result


def test_old_cycle_not_hidden_or_presented_as_current_executable():
    from astra_cycle import build
    from test_astra_cycle import q
    d=build({'rows':[q()]},now=NOW)
    result=html(d,now=datetime.fromisoformat('2026-10-15T10:00:00+08:00'))
    assert '原轮次跟踪 · 待重算' in result and '条件匹配 0 只' in result and '研究入场' in result
