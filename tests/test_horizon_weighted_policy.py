from copy import deepcopy
import json

import pytest

from horizon_score_policy import HORIZON_SCORE_WEIGHTS, horizon_score
from v88_decision_core import SCORE_VERSION, evaluate_decision


def facts(**overrides):
    return {"last": 100, "horizons": {label: {"rule_score": overrides.get(label, 60)}
                                     for label in ("2周", "4周", "8周", "16周", "32周")}}


@pytest.mark.parametrize("group,low,high", [("medium", "4周", "8周"), ("long", "16周", "32周"),
                                         ("radar_short", "10日", "5日"), ("radar_long", "60日", "120日")])
def test_higher_weight_factor_changes_score_more_than_lower_weight(group, low, high):
    important_high = horizon_score({low: {"rule_score": 20}, high: {"rule_score": 80}}, group)
    important_low = horizon_score({low: {"rule_score": 80}, high: {"rule_score": 20}}, group)
    assert important_high["score"] == pytest.approx(59)
    assert important_low["score"] == pytest.approx(41)
    assert important_high["weights"][high] > important_high["weights"][low]


@pytest.mark.parametrize("bad", [None, True, False, float("nan"), float("inf"), -1, 101, "invalid"])
def test_invalid_score_is_not_neutral_or_borrowed_from_another_horizon(bad):
    source = facts(**{"8周": bad})
    result = evaluate_decision(facts=source, full={"total": 60}, action_hint="减仓")
    assert result["unified_score"] is None and result["medium_score"] is None
    assert result["score_status"] == "limited" and result["action"] == "减仓"
    component = result["score_aggregation"]["medium"]
    assert component["coverage"] == pytest.approx(.35)
    assert component["missing_components"] == ["8周"]
    assert component["components"]["4周"]["weight"] == .35
    assert component["components"]["8周"]["contribution"] is None
    json.dumps(result["score_aggregation"], allow_nan=False)


def test_deleted_horizon_cannot_inherit_other_period():
    source = facts(); del source["horizons"]["32周"]
    result = evaluate_decision(facts=source)
    assert result["long_score"] is None and result["unified_score"] is None
    assert result["action"] == "数据不足·待复核"


def test_outer_formula_and_original_facts_are_preserved():
    source = facts(**{"4周": 20, "8周": 80, "16周": 20, "32周": 80})
    before = deepcopy(source)
    result = evaluate_decision(facts=source, full={"total": 72, "resistance": 125, "stop": 90})
    assert source == before
    assert result["score_version"] == SCORE_VERSION
    assert result["score_weights"] == {"short": .20, "medium": .25, "long": .20, "trend_quality": .15, "entry_odds": .20}
    assert result["medium_score"] == result["long_score"] == 59
    assert result["unified_score"] == round(sum(result["score_weights"][k] * v for k, v in result["score_terms"].items()))
    assert not result["grade_authority"]


def test_alignment_weights_and_incomplete_raw_window_disclosure():
    horizons = {f"{weeks}周": {"rule_score": score, "sample_days": 90}
                for weeks, score in ((4, 20), (8, 40), (16, 60), (32, 80))}
    result = horizon_score(horizons, "alignment")
    assert result["weights"] == HORIZON_SCORE_WEIGHTS["alignment"]
    assert result["score"] == pytest.approx(60)
    assert result["window_samples"]["32周"]["window_complete"] is False
    assert result["coverage_kind"] == "score-components-not-independent-sources-or-complete-price-history"

    assert result["window_status"] == "limited"
