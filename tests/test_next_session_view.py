from copy import deepcopy
from html.parser import HTMLParser
import math

import pytest

from next_session_view import render


def row(code="688002.SS", direction="down", **extra):
    return {"code": code, "name": "睿创微纳", "direction": direction, "phase": "派发→退潮",
            "rule_score": 78, "central_tier": "1A", "central_score": 70, "central_current": True,
            "central_action": "研究跟踪", "sector_label": "军工电子Ⅱ", "sector_state": "opposed",
            "sector_detail": "板块证据方向与个股不同", "industry": "军工电子Ⅱ", "next_action": "先核对回撤风险，等待原条件",
            "trigger": "价格与量能共同确认", "invalid": "失效价153.41", "source_date": "2026-09-11",
            "source_status": "current", "priority": 0, **extra}


def example(rows=None):
    return {"generated_at": "2026-09-13T12:00:00+08:00", "pool_size": 864, "signal_count": 10,
            "central_version": "frozen-v1", "notes": ["复用已有证据"],
            "markets": [{"market": market, "next_session": "2026-09-14", "source_date": "2026-09-11",
                         "rows": rows if market == "A股" else [], "counts": {"up": 0, "down": len(rows or []), "mixed": 0}}
                        for market in ("A股", "美股", "港股")]}


class Structure(HTMLParser):
    def __init__(self, html):
        super().__init__(); self.elements = []; self.stack = []; self.feed(html)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        self.elements.append((tag, attrs, list(self.stack)))
        if tag not in {"br", "img", "hr", "input", "meta", "link"}:
            self.stack.append((tag, attrs))

    def handle_endtag(self, tag):
        for i in range(len(self.stack)-1, -1, -1):
            if self.stack[i][0] == tag:
                self.stack = self.stack[:i]; break


def articles(html):
    return [(attrs, parents) for tag, attrs, parents in Structure(html).elements if tag == "article"]


def test_three_markets_group_by_direction_keep_model_priority_and_fold_after_two():
    rows = [row(code=f"60000{i}.SS", priority=99-i) for i in range(5)]
    rows += [row(code=f"60001{i}.SS", direction="up", priority=99-i) for i in range(3)]
    rows += [row(code="600020.SS", direction="mixed")]
    html = render(example(rows), profiles={})
    records = articles(html)
    assert [r[0]["data-stock-code"] for r in records] == [r["code"] for r in rows]
    visible = [attrs for attrs, parents in records if not any(tag == "details" for tag, _ in parents)]
    assert [r["data-stock-code"] for r in visible] == ["600000.SS", "600001.SS", "600010.SS", "600011.SS"]
    for market in ("A股", "港股", "美股"):
        assert f'data-market="{market}"' in html
    assert "其余3只转弱预警" in html and "其余1只转强观察" in html and "分歧或待核信号 1只" in html
    assert "grid-template-columns:1fr" in html


def test_read_only_original_conditions_scores_and_shared_links():
    doc = example([row()]); frozen = deepcopy(doc)
    html = render(doc, profiles={})
    assert doc == frozen
    assert "中央 1A · 70/100" in html and "规则强度 78/100" in html
    assert "板块反向" in html and "板块证据方向与个股不同" in html
    assert "失效价153.41" in html and "价格与量能共同确认" in html
    assert '?q=688002.SS&amp;focus=deep#v88-deep-analysis' in html
    assert "不是涨跌概率、胜率或中央审核分" in html
    assert "<script" not in html


@pytest.mark.parametrize("field", ["name", "code", "phase", "central_tier", "central_action", "sector_label",
                                  "sector_detail", "industry", "next_action", "trigger", "invalid", "source_date", "source_status"])
def test_untrusted_row_content_is_text_not_markup(field):
    payload = '\"><img src=x onerror="bad()">中文 & value'
    html = render(example([row(**{field: payload})]), profiles={})
    parsed = Structure(html).elements
    assert not any(tag == "img" or any(k.startswith("on") for k in attrs) for tag, attrs, _ in parsed)
    assert "&lt;img" in html or "%3Cimg" in html


def test_document_text_and_unknown_state_cannot_create_html_or_classes():
    doc = example([row(sector_state='\"><script>', direction='\"><script>')])
    doc.update(generated_at="<b>now</b>", central_version="<script>version</script>", notes=["<img src=x>"])
    doc["markets"][0]["market"] = 'A股" onmouseover="bad'
    html = render(doc, profiles={})
    assert '<script>' not in html and '<img' not in html
    assert '&lt;script&gt;version&lt;/script&gt;' in html
    assert 'ns-sector-missing' in html and 'data-direction="mixed"' in html


@pytest.mark.parametrize("score", [None, True, False, math.nan, math.inf, -1, 101, "70"])
def test_missing_scores_are_not_fabricated_and_stale_central_is_explicit(score):
    html = render(example([row(central_current=False, central_score=score, rule_score=score,
                               central_action="中央版本待核")]), profiles={})
    assert "中央版本待核" in html and "上次 1A" not in html
    assert "规则强度 待核" in html
    assert "50/100" not in html


def test_industry_peers_only_loaded_for_visible_cards(monkeypatch):
    calls = []
    def peers(code, profiles, compact):
        calls.append((code, profiles, compact)); return '<details class="peer-test"><summary>同业前十</summary></details>'
    monkeypatch.setattr("next_session_view.stock_profile_view.industry_rank_html", peers)
    profiles = {}
    html = render(example([row(code=f"60000{i}.SS") for i in range(4)]), profiles=profiles)
    assert calls == [("600000.SS", profiles, False), ("600001.SS", profiles, False)]
    assert html.count('class="peer-test"') == 2


def test_current_original_plan_is_separate_from_technical_evidence():
    current = row(central_plan={"entry_range": [161.232, 164.27], "take_profit_range": [185.96, 185.99], "stop": 153.41},
                  central_ref={"factpack_id": "factpack-0123456789abcdefghij"}, source_status="verified")
    html = render(example([current]), profiles={})
    assert "中央原入场</dt><dd>161.232 ～ 164.27" in html
    assert "中央原止盈</dt><dd>185.96 ～ 185.99" in html
    assert "中央原失效</dt><dd>153.41" in html
    assert "技术触发</dt><dd>价格与量能共同确认" in html and "技术反证" in html
    assert "factpack-0123456789a…" in html
    assert "数据状态</dt><dd>已核验" in html
    current["central_current"] = False; current["central_action"] = "未获当前评级"; current["source_status"] = "missing"
    html = render(example([current]), profiles={})
    assert "中央原入场" not in html and "中央原失效" not in html and "factpack-012" not in html
    assert "未获当前评级" in html and "上次" not in html
    assert "数据状态</dt><dd>待补当期行情" in html


def test_empty_counts_mean_zero_and_local_verification_is_timestamped():
    doc = example([]); doc.update(verified_count=123, verified_at="2026-09-13T14:20:30+08:00")
    for market in doc["markets"]:
        market["counts"] = {}
    html = render(doc, profiles={})
    assert html.count("↓ 0 · ↗ 0 · ± 0") == 3
    assert "本地复核 123只" in html and "本地已复核 123只" in html
    assert "复核时间 2026-09-13T14:20:30+08:00" in html
