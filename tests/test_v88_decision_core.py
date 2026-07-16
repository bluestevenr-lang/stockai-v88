import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

from v88_decision_core import (
    HORIZON_DAYS,
    SCORE_VERSION,
    build_horizon_facts,
    evaluate_anchor_outlook,
    evaluate_decision,
    evaluate_forward_outlook,
)


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


def test_anchor_outlook_is_no_lookahead_and_has_all_horizons():
    df = _prices(1, 180)
    anchor_pos = 100
    anchor_time = df.index[anchor_pos] + pd.Timedelta(hours=9, minutes=45)
    anchor_price = float(df.Close.iloc[anchor_pos])
    first = evaluate_anchor_outlook(
        df, anchor_time, anchor_price,
        action="卖出", name="测试", code="TEST",
        analysis_time="固定",
    )

    changed = df.copy()
    changed.iloc[anchor_pos + 1:, changed.columns.get_loc("Close")] *= 2.5
    changed.iloc[anchor_pos + 1:, changed.columns.get_loc("High")] *= 2.5
    changed.iloc[anchor_pos + 1:, changed.columns.get_loc("Low")] *= 2.5
    second = evaluate_anchor_outlook(
        changed, anchor_time, anchor_price,
        action="卖出", name="测试", code="TEST",
        analysis_time="固定",
    )

    assert first["no_lookahead"] is True
    assert [row["days"] for row in first["horizons"]] == [5, 10, 20, 60, 120]
    # 锚点档位是“交易日”，不是周：5 日档取样窗口应远小于 120 日档。
    _by_days = {row["days"]: row["sample_days"] for row in first["horizons"]}
    assert _by_days[5] <= 6 and _by_days[120] >= 60
    assert first["data_signature"] == second["data_signature"]
    assert first["horizons"] == second["horizons"]
    assert first["overall_action"] == second["overall_action"]
    # 锚点后的真实走势会改变跟踪结果，但不能改写当时预测。
    assert first["tracking"] != second["tracking"]


def test_anchor_outlook_exposes_probability_odds_and_ev():
    df = _prices(1, 180)
    row = evaluate_anchor_outlook(
        df, df.index[120] + pd.Timedelta(hours=10), float(df.Close.iloc[120]),
        action="观察", name="测试", code="TEST",
    )
    assert not row.get("error")
    assert row["weighted_p_up"] + row["weighted_p_down"] == 100
    for horizon in row["horizons"]:
        assert horizon["p_up"] + horizon["p_down"] == 100
        assert horizon["rr"] >= 0
        assert "expected_pct" in horizon
        assert "target_price" in horizon
        assert "risk_price" in horizon
        assert horizon["probability_kind"] == "规则情景估计（非回测胜率）"


def test_anchor_sell_review_is_staged_not_mechanical():
    df = _prices(1, 180)
    row = evaluate_anchor_outlook(
        df, df.index[150] + pd.Timedelta(hours=9, minutes=45),
        float(df.Close.iloc[150]), action="卖出",
    )
    assert any(word in row["decision_review"] for word in ("分批", "卖出具备"))


def test_anchor_newer_than_market_snapshot_does_not_fake_tracking_return():
    df = _prices(1, 120)
    anchor_time = df.index[-1] + pd.Timedelta(days=1, hours=9, minutes=45)
    row = evaluate_anchor_outlook(df, anchor_time, float(df.Close.iloc[-1]), action="卖出")
    assert not row.get("error")
    assert row["tracking"]["market_covers_anchor"] is False
    assert row["tracking"]["current_price"] is None
    assert row["tracking"]["since_anchor_pct"] is None


def test_forward_outlook_gives_live_per_stock_probability_and_odds():
    df = _prices(1, 180)
    row = evaluate_forward_outlook(df, name="腾讯", code="00700.HK")
    assert not row.get("error")
    assert [h["days"] for h in row["horizons"]] == list(HORIZON_DAYS)
    assert row["weighted_p_up"] + row["weighted_p_down"] == 100
    assert row["suggestion"] and row["overall_action"]
    for h in row["horizons"]:
        assert h["p_up"] + h["p_down"] == 100
        assert h["rr"] >= 0
        assert "target_price" in h and "risk_price" in h
        assert h["probability_kind"] == "规则情景估计（非回测胜率）"


def test_forward_outlook_reads_current_price_not_a_past_anchor():
    df = _prices(1, 180)
    row = evaluate_forward_outlook(df, name="测试", code="T")
    assert row["last"] == round(float(df.Close.iloc[-1]), 4)
    assert "tracking" not in row  # 当下前瞻无到期跟踪，纯向前看


def test_cloud_and_web_distribution_are_byte_identical():
    here = Path(__file__).resolve().parents[1] / "v88_decision_core.py"
    cloud = Path.home() / "Desktop" / "ai-daily-report-v2" / "src" / "v88_decision_core.py"
    assert hashlib.sha256(here.read_bytes()).digest() == hashlib.sha256(cloud.read_bytes()).digest()
