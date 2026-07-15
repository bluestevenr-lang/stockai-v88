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
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    result = stock_horizon.analyze("测试", "TEST", _frame(1))
    assert result["review"]["status"] == "no_key"
    assert len(stock_horizon.table_rows(result)) == 5


def test_cycle_visual_contains_full_first_screen_content(monkeypatch):
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    result = stock_horizon.analyze("测试公司", "TEST", _frame(1))
    html = stock_horizon.cycle_visual_html(result, "测试公司", "TEST")
    for text in ("领涨启动", "高位派发", "退潮杀跌", "低位蓄势",
                 "2周", "4周", "6周", "8周", "16周",
                 "预计拐点", "触发", "失效", "分析于"):
        assert text in html
