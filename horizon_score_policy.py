"""Versioned, unequal aggregation of comparable technical direction scores.

Weights are provisional V88 research parameters, not quotations, probabilities,
or calibrated performance. Same-source windows are not independent evidence.
"""
from __future__ import annotations

import math

POLICY_VERSION = "v88-horizon-score-weighting/1.0"
POLICY_DESCRIPTION = (
    "短端重临近观察，较长端重更长结构；仅合成同量纲技术方向分。"
    "权重为V88待验证参数，非书籍原句、胜率或审核授权；同源观察不计独立证据。"
)
HORIZON_SCORE_WEIGHTS = {
    "short": {"2周": 1.0},
    "medium": {"4周": .35, "8周": .65},
    "long": {"16周": .35, "32周": .65},
    "alignment": {"4周": .10, "8周": .20, "16周": .30, "32周": .40},
    "radar_short": {"5日": .65, "10日": .35},
    "radar_long": {"60日": .35, "120日": .65},
}


def finite_score(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) and 0 <= number <= 100 else None
    except (TypeError, ValueError, OverflowError):
        return None


def weighted_score(values, weights, *, description=POLICY_DESCRIPTION):
    """Require every declared component; never fill or renormalize missing data."""
    components = {}
    weight_total = sum(weights.values())
    if not weights or any(not math.isfinite(w) or w <= 0 for w in weights.values()):
        raise ValueError("score weights must be finite and positive")
    for key, weight in weights.items():
        score = finite_score(values.get(key))
        normalized = weight / weight_total
        components[key] = {"score": score, "weight": normalized,
                           "contribution": score * normalized if score is not None else None}
    missing = [key for key, row in components.items() if row["score"] is None]
    return {
        "policy_version": POLICY_VERSION, "description": description,
        "score": sum(row["contribution"] for row in components.values()) if not missing else None,
        "weights": {key: row["weight"] for key, row in components.items()},
        "components": components, "missing_components": missing,
        "coverage": sum(row["weight"] for row in components.values() if row["score"] is not None),
        "status": "complete" if not missing else "limited",
        "missing_policy": "require-all-components-no-imputation-no-renormalization",
        "grade_authority": False,
    }


def horizon_score(horizons, group, *, field="rule_score"):
    weights = HORIZON_SCORE_WEIGHTS[group]
    result = weighted_score({key: (horizons.get(key) or {}).get(field) for key in weights}, weights)
    # Score presence and the actual lookback coverage are distinct facts.
    result["coverage_kind"] = "score-components-not-independent-sources-or-complete-price-history"
    result["window_samples"] = {}
    for key in weights:
        raw = (horizons.get(key) or {}).get("sample_days")
        sample = raw if type(raw) in (float, int) and math.isfinite(raw) and raw >= 0 else None
        requested = int(key[:-1]) * (5 if key.endswith("周") else 1)
        result["window_samples"][key] = {
            "sample_days": sample, "requested_days": requested,
            "window_complete": sample >= requested if sample is not None else None,
        }
    result["window_status"] = "complete" if all(
        row["window_complete"] is True for row in result["window_samples"].values()) else "limited"
    return result
