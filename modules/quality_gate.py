"""
质量闸门 - 钉钉 Score + 阈值 注入 V88
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- 优先使用 metrics.canslim_score（钉钉同源 CANSLIM）
- min_score 阈值：score >= min_score 才入围（默认 40，与钉钉一致）
- trade_candidates 按 score 降序
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from typing import List, Tuple, Dict, Any, Optional
import numpy as np


def compute_quality_score(metrics: Optional[Dict]) -> Optional[float]:
    """
    从 fast_metrics 输出获取质量分。
    优先使用 canslim_score（钉钉同源），否则用轻量 fallback。
    """
    if not metrics:
        return None
    # 优先使用钉钉同源 CANSLIM 分
    cs = metrics.get("canslim_score")
    if cs is not None:
        return float(cs)
    # fallback：轻量计算
    try:
        score = 0
        if metrics.get("above_ma20"): score += 15
        if metrics.get("above_ma60"): score += 15
        if metrics.get("above_ma200"): score += 15
        if metrics.get("rsi", 50) > 50: score += 10
        ret5 = metrics.get("ret5", 0)
        ret20 = metrics.get("ret20", 0)
        if ret5 > 0: score += 10
        if ret20 > 0: score += 10
        vol_ratio = metrics.get("vol_ratio", 1)
        if ret5 > 0 and vol_ratio > 1.1: score += 15
        if vol_ratio > 1.0: score += 5
        return min(100, score)
    except Exception:
        return None


def quality_gate(
    candidates: List[Tuple[Any, float, Optional[Dict]]],
    market: str,
    horizon: str,
    policy: Dict[str, Any],
) -> Tuple[List[Tuple[Any, float, float]], Dict[str, Any]]:
    """
    钉钉 Score + 阈值：min_score 过滤 + 按 score 降序。
    policy.min_score=40 时与钉钉逻辑一致；无 min_score 时用分位数。
    """
    min_score = policy.get("min_score")  # 钉钉阈值，如 40
    min_size_map = policy.get("trade_min_size", {"ST": 30, "MT": 30, "LT": 30})
    quantile_map = policy.get("quantile", {"ST": 0.70, "MT": 0.60, "LT": 0.50})
    fallback_quantiles = [0.60, 0.50, 0.40]
    min_size = min_size_map.get(horizon, 30)
    q_init = quantile_map.get(horizon, 0.60)

    scored: List[Tuple[Any, float, float]] = []
    filter_reasons = {"no_metrics": 0, "below_min_score": 0}

    for item, price, m in candidates:
        score = compute_quality_score(m)
        if score is None:
            filter_reasons["no_metrics"] += 1
            continue
        if min_score is not None and score < min_score:
            filter_reasons["below_min_score"] += 1
            continue
        scored.append((item, price, score))

    if not scored:
        return [], {
            "count": 0,
            "quantile_used": q_init,
            "min_score_used": min_score,
            "filter_reasons": filter_reasons,
            "market": market,
            "horizon": horizon,
            "explore_total": len(candidates),
        }

    scored.sort(key=lambda x: x[2], reverse=True)

    if min_score is not None:
        trade = scored
        quantile_used = None
    else:
        scores = [s[2] for s in scored]
        quantile_used = q_init
        threshold = float(np.quantile(scores, quantile_used))
        trade = [(it, p, sc) for it, p, sc in scored if sc >= threshold]
        trade.sort(key=lambda x: x[2], reverse=True)
        for q_fb in fallback_quantiles:
            if len(trade) >= min_size or q_fb >= quantile_used:
                break
            quantile_used = q_fb
            threshold = float(np.quantile(scores, quantile_used))
            trade = [(it, p, sc) for it, p, sc in scored if sc >= threshold]
            trade.sort(key=lambda x: x[2], reverse=True)

    while len(trade) < min_size and len(scored) > len(trade):
        next_cand = [x for x in scored if x not in trade][:1]
        if next_cand:
            trade.append(next_cand[0])
            trade.sort(key=lambda x: x[2], reverse=True)
        else:
            break

    meta = {
        "count": len(trade),
        "quantile_used": quantile_used,
        "min_score_used": min_score,
        "filter_reasons": filter_reasons,
        "market": market,
        "horizon": horizon,
        "explore_total": len(candidates),
    }
    return trade, meta
