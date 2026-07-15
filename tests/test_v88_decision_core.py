import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

from v88_decision_core import SCORE_VERSION, build_horizon_facts, evaluate_decision


def _prices(direction=1, n=100):
    base = np.linspace(100, 132 if direction > 0 else 72, n)
    close = base + np.sin(np.arange(n) / 4) * 0.5
    idx = pd.date_range("2026-01-01", periods=n, freq="B")
    return pd.DataFrame({
        "Open": close * 0.998, "High": close * 1.012,
        "Low": close * 0.988, "Close": close,
        "Volume": np.linspace(1_000_000, 1_300_000, n),
    }, index=idx)


def test_same_snapshot_is_identical_across_entry_points():
    df = _prices(1)
    full = {"stage": "趋势延续", "total": 72, "last": float(df.Close.iloc[-1]),
            "resistance": 142, "stop": 124}
    home = evaluate_decision(df, full, analysis_time="固定", name="测试", code="T")
    search = evaluate_decision(df, full, analysis_time="固定", name="测试", code="T")
    for key in ("data_signature", "unified_score", "short_score", "medium_score",
                "long_score", "p_up", "p_down", "rr", "expected_pct", "action"):
        assert home[key] == search[key]


def test_score_weights_are_explicit_and_complete():
    row = evaluate_decision(_prices(1), {"stage": "趋势延续", "total": 70})
    assert row["score_version"] == SCORE_VERSION
    assert sum(row["score_weights"].values()) == 1
    expected = round(row["short_score"] * .20 + row["medium_score"] * .25
                     + row["long_score"] * .20 + row["trend_quality_score"] * .15
                     + row["entry_odds_score"] * .20)
    assert abs(row["unified_score"] - expected) <= 1


def test_bullish_but_bad_price_waits_instead_of_avoid():
    df = _prices(1)
    last = float(df.Close.iloc[-1])
    facts = build_horizon_facts(df, {"stage": "趋势延续"})
    for item in facts["horizons"].values():
        item["rule_score"] = 68
    row = evaluate_decision(df, {"stage": "趋势延续", "total": 75,
                                 "resistance": last * 1.005, "stop": last * .90},
                            facts=facts)
    assert row["short_side"] == "偏涨"
    assert row["long_side"] == "偏涨"
    assert row["action"] == "趋势偏多·等待回踩"


def test_holding_risk_action_overrides_score_but_not_score_itself():
    df = _prices(1)
    full = {"stage": "趋势延续", "total": 75, "resistance": 145, "stop": 124}
    plain = evaluate_decision(df, full)
    held = evaluate_decision(df, full, holding={"cost": 100}, action_hint="减仓")
    assert held["action"] == "减仓"
    assert held["unified_score"] == plain["unified_score"]
    assert held["p_up"] == plain["p_up"]


def test_changed_market_data_changes_signature():
    a = build_horizon_facts(_prices(1))
    df = _prices(1)
    df.iloc[-1, df.columns.get_loc("Close")] += 2
    b = build_horizon_facts(df)
    assert a["data_signature"] != b["data_signature"]


def test_cloud_and_web_distribution_are_byte_identical():
    here = Path(__file__).resolve().parents[1] / "v88_decision_core.py"
    cloud = Path.home() / "Desktop" / "ai-daily-report-v2" / "src" / "v88_decision_core.py"
    assert hashlib.sha256(here.read_bytes()).digest() == hashlib.sha256(cloud.read_bytes()).digest()
