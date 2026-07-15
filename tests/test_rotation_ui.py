"""周期总览布局回归测试：后续改版不得再拆回两个大模块。"""

from html.parser import HTMLParser

from rotation_ui import combined_cycle_dashboard_html


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
        if inside and attrs.get("aria-label") == "中美港板块热度轮动时钟与日周月走向泳道":
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
        for horizon, score in (("明日", 69), ("下周", 64), ("半个月", 56))
    }
    return {
        "analysis_time": "2026-07-15 12:00",
        "markets": {"美股": {}},
        "market_heat": {"美股": {"score": 59, "label": "中性"}},
        "trajectories": {"美股": [{"name": "能源", "points": points}]},
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
    assert "grid-template-columns:minmax(0,1fr) minmax(0,1fr)" in html
    assert "@media(max-width:920px)" in html
