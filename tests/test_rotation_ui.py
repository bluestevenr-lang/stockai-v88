"""周期总览布局回归测试：后续改版不得再拆回两个大模块。"""

from html.parser import HTMLParser

from rotation_ui import combined_cycle_dashboard_html
from rotation_ui import rotation_map_html, stock_cycle_html, _swimlane_svg
from copy import deepcopy
import pytest


@pytest.fixture(autouse=True)
def isolate_stock_future(monkeypatch):
    import stock_future_context
    monkeypatch.setattr(stock_future_context, "for_stock", lambda code,name="": None)


class _LayoutParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.stack = []
        self.root_depth = None
        self.panel_count = 0
        self.sector_inside = False
        self.stock_inside = False
        self.style_inside = False

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        self.stack.append((tag, attrs))
        if attrs.get("aria-label") == "板块轮动与个股周期综合总览":
            self.root_depth = len(self.stack)
        inside = self.root_depth is not None and len(self.stack) > self.root_depth
        if inside and tag == "style":
            self.style_inside = True
        if inside and "cc-panel" in attrs.get("class", "").split():
            self.panel_count += 1
        if inside and attrs.get("aria-label") == "中美港板块未来周期展望":
            self.sector_inside = True
        if inside and attrs.get("aria-label") == "个股周期切换扫描":
            self.stock_inside = True

    def handle_endtag(self, tag):
        if not self.stack:
            return
        if self.root_depth == len(self.stack) and self.stack[-1][1].get("aria-label") == "板块轮动与个股周期综合总览":
            self.root_depth = None
        self.stack.pop()


def _sample_forecast():
    points = {
        horizon: {"score": score, "confidence": "高", "trigger": "量能", "invalid": "破位"}
        for horizon, score in (("2周", 69), ("5周", 64), ("8周", 60), ("16周", 56))
    }
    return {
        "analysis_time": "2026-07-15 12:00",
        "markets": {"美股": {}},
        "market_heat": {"美股": {"score": 59, "label": "中性"}},
        "trajectories": {"美股": [{
            "name": "能源", "points": points,
            "turning": {"horizon": "5周", "type": "顶部转弱", "confidence": "中"},
        }]},
    }


def _sample_cycle():
    return {
        "analysis_time": "2026-07-15 12:00",
        "stocks": [{
            "name": "中微公司", "phase": "低位蓄势", "direction": "up",
            "confidence": 72, "horizon": "明日", "pos52": 20, "up": 60, "down": 10,
        }],
    }


def test_combined_cycle_dashboard_keeps_both_modules_in_compact_grid():
    html = combined_cycle_dashboard_html(_sample_forecast(), _sample_cycle(), "cycle-test", "美股")
    parser = _LayoutParser()
    parser.feed(html)

    assert parser.panel_count == 2
    assert parser.sector_inside
    assert parser.stock_inside
    assert not parser.style_inside
    assert html.count("<style>") == 1
    assert "grid-template-columns:minmax(0,1fr) minmax(0,1fr)" in html
    assert "@media(max-width:920px)" in html
    assert "未来周期" in html and "rf-phase-clock" in html
    assert "预计拐点" not in html and "条件 / 反证" in html


def test_scenario_scores_are_independent_cells_and_never_future_turning_dates():
    forecast=_sample_forecast();forecast['strong_review']={'status':'completed'}
    forecast['trajectories']['美股'][0]['now']=0
    forecast['trajectories']['美股'][0]['points']['2周']['score']=float('nan')
    before=deepcopy(forecast)
    html=rotation_map_html(forecast)
    assert 'rf-scenario-matrix' in html and '0/100' in html and '○ 缺证' in html
    for misleading in ('<polyline','预计拐点','拐点临近','高置信','最强思考已复核','今天 →'):
        assert misleading not in html
    assert forecast['trajectories']['美股'][0]['now']==before['trajectories']['美股'][0]['now']


def test_observed_motion_uses_past_facts_not_scenario_score_slope():
    item=_sample_forecast()['trajectories']['美股'][0]
    item['facts']={'5d':-2,'20d':8}
    for point in item['points'].values():point['score']=95
    html=_swimlane_svg('美股',[item])
    assert '⚠ 月强周弱' in html and '↑ 增强' not in html


def test_stock_cycle_keeps_zero_missing_and_neutral_records_visible(monkeypatch):
    import sys
    from types import SimpleNamespace
    monkeypatch.setitem(sys.modules,'stock_profile_view',SimpleNamespace(
        load=lambda:{},display_label=lambda name,code,profiles:name,
        link_html=lambda name,code,profiles:f'<a href="/?q={code}">{name}</a>'))
    cycle={'stocks':[{'code':'ZERO','name':'实际零','direction':'up','up':60,'down':0,'pos52':0},
                     {'code':'MISS','name':'缺证股','direction':'up','pos52':None},
                     {'code':'NEUT','name':'分歧股','direction':'hold','up':50,'down':50,'pos52':50}]}
    html=stock_cycle_html(cycle,profiles={})
    assert all(x in html for x in ('实际零','缺证股','分歧股','○ 方向分缺证','↔ 方向分歧'))
    assert '<td>0%<div>' in html and '<td>○ 缺证<div>' in html
    assert '即将进入' not in html and '高置信' not in html
    assert '原历史位置字段，全年窗口未附凭据' in html


def test_sector_scores_and_conditions_escape_external_text():
    forecast=_sample_forecast();row=forecast['trajectories']['美股'][0]
    row['name']='<script>alert(1)</script>';row['points']['2周']['trigger']='<img src=x>'
    html=rotation_map_html(forecast)
    assert '<script>' not in html and '<img' not in html and '&lt;script&gt;' in html
