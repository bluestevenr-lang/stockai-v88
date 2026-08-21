import os

import numpy as np
import pandas as pd

import stock_horizon


def _frame(direction=1, rows=120):
    idx = pd.date_range("2026-01-01", periods=rows, freq="B")
    close = 100 + direction * np.linspace(0, 30, rows)
    return pd.DataFrame({
        "Open": close - .2,
        "High": close + 1,
        "Low": close - 1,
        "Close": close,
        "Volume": np.linspace(1_000_000, 1_200_000, rows),
    }, index=idx)


def test_all_five_horizons_present():
    facts = stock_horizon.build_horizon_facts(_frame(1), {"stage": "主升阶段"})
    assert list(facts["horizons"]) == ["2周", "4周", "6周", "8周", "16周"]


def test_direction_responds_to_trend():
    up = stock_horizon.build_horizon_facts(_frame(1), {"stage": "主升阶段"})
    down = stock_horizon.build_horizon_facts(_frame(-1), {"stage": "破位转弱"})
    assert up["horizons"]["8周"]["rule_score"] > 50
    assert down["horizons"]["8周"]["rule_score"] < 50


def test_without_key_keeps_deterministic_fallback(monkeypatch):
    monkeypatch.setenv("V88_DISABLE_LLM", "1")
    result = stock_horizon.analyze("测试", "TEST", _frame(1))
    assert result["review"]["status"] == "no_key"
    assert len(stock_horizon.table_rows(result)) == 5


def test_cycle_visual_contains_full_first_screen_content(monkeypatch):
    monkeypatch.setenv("V88_DISABLE_LLM", "1")
    result = stock_horizon.analyze("测试公司", "TEST", _frame(1))
    html = stock_horizon.cycle_visual_html(result, "测试公司", "TEST")
    for text in ("领涨启动", "高位派发", "退潮杀跌", "低位蓄势",
                 "2周", "4周", "6周", "8周", "16周",
                 "预计拐点", "触发", "失效", "分析于"):
        assert text in html


def test_cross_cycle_conflict_forces_safe_action():
    facts = {"horizons": {
        "2周": {"rule_score": 70},
        "4周": {"rule_score": 44},
        "6周": {"rule_score": 38},
        "8周": {"rule_score": 34},
        "16周": {"rule_score": 28},
    }}
    card = {"p_up": 65, "p_down": 35, "upside_pct": 20,
            "downside_pct": 8, "rr": 2.5, "expected_pct": 10,
            "action": "试仓复核", "reason": "短线启动"}
    aligned = stock_horizon.align_decision_card(card, facts)
    assert aligned["p_up"] == 70
    assert aligned["long_p_up"] == 36
    assert aligned["cycle_conflict"] is True
    assert aligned["action"] == "仅观察·不追涨"
    assert "2周偏涨" in aligned["cycle_note"]


def test_short_bullish_long_slightly_weak_is_still_conflict():
    facts = {"horizons": {
        "2周": {"rule_score": 76},
        "4周": {"rule_score": 53},
        "6周": {"rule_score": 44},
        "8周": {"rule_score": 40},
        "16周": {"rule_score": 28},
    }}
    aligned = stock_horizon.align_decision_card(
        {"upside_pct": 25, "downside_pct": 8, "rr": 3.1,
         "expected_pct": 15, "action": "试仓复核"}, facts)
    assert aligned["p_up"] == 76
    assert aligned["long_p_up"] == 41
    assert aligned["cycle_conflict"] is True
    assert aligned["action"] == "仅观察·不追涨"


def test_visual_fallback_labels_rule_not_ai():
    facts = {"asof": "2026-07-15 22:28", "stage": "启动确认", "horizons": {}}
    for weeks, score in ((2, 76), (4, 53), (6, 44), (8, 40), (16, 28)):
        facts["horizons"][f"{weeks}周"] = {
            "rule_score": score, "rule_view": "偏涨" if score >= 58 else "偏跌",
            "rule_confidence": 60, "return_pct": 0, "volume_ratio": 1,
            "support": 1, "resistance": 2,
        }
    html = stock_horizon.cycle_visual_html(
        {"facts": facts, "review": {"status": "failed", "horizons": {}}},
        "紫金矿业", "601899.SS")
    assert "规则偏涨" in html
    assert "AI偏涨" not in html
    assert "规则上行估计：76%" in html


def test_bullish_cycles_bad_price_waits_instead_of_avoid():
    facts = {"horizons": {
        "2周": {"rule_score": 62}, "4周": {"rule_score": 72},
        "6周": {"rule_score": 73}, "8周": {"rule_score": 74},
        "16周": {"rule_score": 73},
    }}
    aligned = stock_horizon.align_decision_card(
        {"upside_pct": .9, "downside_pct": 10, "rr": .09,
         "expected_pct": -2.4, "action": "回避"}, facts)
    assert aligned["cycle_status"] == "多周期偏涨"
    assert aligned["action"] == "趋势偏多·等待回踩"
    assert aligned["break_even_p"] == 91.7
    assert "当前赔率不足" in aligned["entry_note"]


def test_bullish_cycles_allow_controlled_aggressive_entry():
    facts = {"horizons": {
        "2周": {"rule_score": 72}, "4周": {"rule_score": 58},
        "6周": {"rule_score": 59}, "8周": {"rule_score": 58},
        "16周": {"rule_score": 58},
    }}
    aligned = stock_horizon.align_decision_card(
        {"upside_pct": 9, "downside_pct": 10.8, "rr": .83,
         "expected_pct": 3, "action": "观察"}, facts)
    assert aligned["action"] == "共振·小仓试错"
    assert aligned["probability_edge"] >= 8


def test_holding_risk_action_cannot_be_overridden_by_bullish_cycle():
    facts = {"horizons": {
        "2周": {"rule_score": 75}, "4周": {"rule_score": 70},
        "6周": {"rule_score": 68}, "8周": {"rule_score": 66},
        "16周": {"rule_score": 64},
    }}
    aligned = stock_horizon.align_decision_card(
        {"upside_pct": 20, "downside_pct": 8, "rr": 2.5,
         "expected_pct": 12, "action": "评估减仓"}, facts)
    assert aligned["action"] == "评估减仓"
    assert aligned["entry_note"] == "持仓先执行风险复核"
