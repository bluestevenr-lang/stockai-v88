from copy import deepcopy
from datetime import date, timedelta
from html.parser import HTMLParser
import math
from pathlib import Path
import re

import pytest
from future_trend_visual import render, WEEKS


def example():
    start = date(2026, 9, 11)
    values = [0.75, -0.4, -0.65, 0.15, 0.8, 1.1, 1.2]
    return {"version": "test/1", "code": "688002.SS", "name": "睿创微纳",
            "source_asof": "2026-09-11", "input_id": "frozen-evidence-123",
            "status": "ready", "headline": "先消化回撤风险，确认修复后再评估延续",
            "phase": {"label": "高位震荡", "x": 1.1, "y": .7, "dx": .1, "dy": -1.1},
            "points": [{"weeks": w, "date": (start+timedelta(weeks=w)).isoformat(),
                        "base": v, "up": min(2, v+.5), "down": max(-2, v-.7),
                        "label": "防回撤" if w < 8 else "等待修复确认",
                        "condition": "价格与量能共同确认", "invalidation": "结构失守或企业证据恶化"}
                       for w, v in zip(WEEKS, values)],
            "assumptions": ["条件路径从同一事实包产生，后续按新证据更新"],
            "entry_permission": False, "model_calls": 0}


class Elements(HTMLParser):
    def __init__(self, text):
        super().__init__(); self.items = []; self.feed(text)

    def handle_starttag(self, tag, attrs):
        self.items.append((tag, dict(attrs)))


def find(text, tag, css):
    return [attrs for kind, attrs in Elements(text).items
            if kind == tag and css in attrs.get("class", "").split()]


def test_future_curve_circle_and_source_are_local_and_no_mutation():
    doc = example(); original = deepcopy(doc)
    html = render(doc)
    assert doc == original
    assert len(find(html, "svg", "vf-path-svg")) == 1
    assert len(find(html, "path", "vf-base-path")) == 1
    assert len(find(html, "circle", "vf-current-phase")) == 1
    assert len(find(html, "path", "vf-phase-arrow")) == 1
    assert "2026-09-11" in html and "2027-09-10" in html
    assert "非价格 / 胜率" in html and "不是统计置信区间" in html
    assert "未来趋势研判" in html and "过去8周档" not in html
    assert "<script" not in html and "http" not in html and "fetch(" not in html
    assert "entry_permission" not in html and "买入" not in html


def test_true_time_scale_and_hollow_future_points():
    points = find(render(example()), "circle", "vf-future-point")
    assert [int(p["data-weeks"]) for p in points] == list(WEEKS)
    xs = [float(p["cx"]) for p in points]
    assert (xs[-1]-xs[0])/(xs[1]-xs[0]) == pytest.approx(26, abs=.015)
    assert points[0]["fill"] == "#2563eb"
    assert all(p["fill"] == "white" for p in points[1:])


@pytest.mark.parametrize("invalid", [None, True, False, math.nan, math.inf, -2.01, 2.01, "1.0"])
def test_missing_or_invalid_base_creates_gap_not_neutral_score(invalid):
    doc = example(); doc["points"][3]["base"] = invalid
    html = render(doc)
    assert len(find(html, "path", "vf-base-path")) == 2
    assert 8 not in [int(p["data-weeks"]) for p in find(html, "circle", "vf-future-point")]
    assert "8周证据未齐" in html and "50/100" not in html


def test_duplicate_or_missing_week_never_connects_across_gap():
    doc = example(); doc["points"].append(deepcopy(doc["points"][3]))
    html = render(doc)
    assert len(find(html, "path", "vf-base-path")) == 2
    assert "8周证据未齐" in html


def test_inconsistent_future_date_is_a_gap_not_a_relabelled_date():
    doc = example(); doc["points"][3]["date"] = "2026-08-01"
    original = deepcopy(doc); html = render(doc)
    assert doc == original
    assert "8周证据未齐" in html and "核对节点日期" in html
    assert 8 not in [int(p["data-weeks"]) for p in find(html, "circle", "vf-future-point")]


def test_model_anchor_can_follow_last_complete_source_session():
    doc = example(); doc['anchor_date'] = '2026-09-13'
    for row in doc['points']:
        row['date'] = (date(2026,9,13)+timedelta(weeks=row['weeks'])).isoformat()
    html = render(doc)
    assert len(find(html, 'circle', 'vf-future-point')) == 7
    assert '证据截至 2026-09-11' in html and '研判起点 2026-09-13' in html


def test_upper_lower_inversion_hidden_without_rewriting_base():
    doc = example()
    for row in doc["points"]:
        row["up"], row["down"] = -1.8, 1.8
    original = deepcopy(doc); html = render(doc)
    assert doc == original
    assert find(html, "path", "vf-base-path")
    assert not find(html, "path", "vf-up-path")
    assert not find(html, "path", "vf-down-path")
    assert not find(html, "polygon", "vf-scenario-band")


@pytest.mark.parametrize("status", [None, "missing", "blocked", "pending"])
def test_unready_document_never_draws_future_or_phase(status):
    doc = example(); doc["status"] = status
    html = render(doc)
    assert "未来路径等待证据补齐" in html
    assert not find(html, "path", "vf-base-path")
    assert not find(html, "circle", "vf-current-phase")


def test_invalid_phase_has_no_arrow_but_independent_path_survives():
    doc = example(); doc["phase"]["x"] = True
    html = render(doc)
    assert "相位待证" in html
    assert not find(html, "circle", "vf-current-phase")
    assert not find(html, "path", "vf-phase-arrow")
    assert find(html, "path", "vf-base-path")


def test_rising_current_phase_with_negative_conditional_arrow_is_red():
    html = render(example())
    assert find(html, "circle", "vf-current-phase")[0]["fill"] == "#159447"
    assert find(html, "path", "vf-phase-arrow")[0]["stroke"] == "#d45555"
    assert "✓ 条件" not in html


def test_small_conditional_direction_remains_visible():
    doc = example(); doc['phase'] = {'x':.55,'y':.2,'dx':0,'dy':-.3,'label':'高位承压'}
    arrow = find(render(doc), 'path', 'vf-phase-arrow')[0]
    coordinates = [float(n) for n in re.findall(r'-?\d+(?:\.\d+)?', arrow['d'])]
    assert math.hypot(coordinates[2]-coordinates[0], coordinates[3]-coordinates[1]) >= 24


def test_escape_all_text_and_attributes():
    doc = example()
    attack = '<script>alert("x")</script><img src=x onerror=evil()>'
    for key in ("input_id", "name", "code", "source_asof", "headline"):
        doc[key] = attack
    doc["phase"]["label"] = attack
    doc["assumptions"] = [attack]
    for row in doc["points"]:
        row.update(label=attack, condition=attack, invalidation=attack, date=attack)
    html = render(doc)
    assert "<script>" not in html and "<img " not in html
    assert "&lt;script&gt;" in html
    assert "日期待核" in html
    ids = [a["id"] for _, a in Elements(html).items if "id" in a]
    assert all(re.fullmatch(r"vf-[a-f0-9]{16}[-a-z]*", value) for value in ids)


def test_conditions_collapsed_and_mobile_keeps_scroll():
    html = render(example(), compact=True)
    details = find(html, "details", "vf-conditions-toggle")
    assert details and "open" not in details[0]
    assert "vf-compact" in html and "overflow-x:auto" in html
    assert len(find(html, "div", "vf-condition")) == 6


def test_empty_doc_has_clear_pending_state():
    html = render(None)
    assert "时间待核" in html and "未来路径等待证据补齐" in html
    assert not find(html, "path", "vf-base-path")


def test_missing_document_preserves_actual_reason_without_fake_curve():
    html = render({'status':'missing','gaps':['原股票代码与当前事实包不一致']})
    assert '证据缺口：原股票代码与当前事实包不一致' in html
    assert not find(html, 'path', 'vf-base-path')


def test_core_copy_matches_desktop():
    desktop = Path(__file__).parents[1] / "future_trend_visual.py"
    core = Path("/Users/bluesteven/Desktop/ai-daily-report-v2/src/future_trend_visual.py")
    assert core.read_bytes() == desktop.read_bytes()
