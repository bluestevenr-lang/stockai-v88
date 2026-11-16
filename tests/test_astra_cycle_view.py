from datetime import datetime
from astra_cycle_view import html
from test_astra_cycle import _bottom_setup_by_default, _low_breadth_by_default

NOW=datetime.fromisoformat('2026-09-28T14:00:00+08:00')


def test_cycle_view_keeps_prices_explanation_counts_and_escapes_names():
    from astra_cycle import build
    from test_astra_cycle import q
    quote=q();quote['name']='<script>alert(1)</script>'
    d=build({'rows':[quote]},now=NOW)
    result=html(d,now=NOW)
    for expected in ['今日可买 1 只','每月1、10、20日开新轮','2026-10-01','新增入场限于 2026-09-30','研究入场','止盈','止损','净空间','截止','行业排名','未对账不计零']:
        assert expected in result
    assert '<script>' not in result and '&lt;script&gt;' in result
    assert '0只可买' not in result


def test_old_cycle_not_hidden_or_presented_as_current_executable():
    from astra_cycle import build
    from test_astra_cycle import q
    d=build({'rows':[q()]},now=NOW)
    result=html(d,now=datetime.fromisoformat('2026-10-15T10:00:00+08:00'))
    assert '原轮次跟踪 · 待重算' in result and '今日可买 0 只' in result and '研究入场' in result


def test_unknown_progress_never_becomes_zero_gap():
    from astra_cycle_view import money_html
    result=money_html({'month_status':{'monthly_target_usd':200,'realized_net_usd':None},'rows':[]},now=NOW)
    assert '实际缺口待对账' in result and '距目标缺口' not in result
