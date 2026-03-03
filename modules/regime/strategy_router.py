# -*- coding: utf-8 -*-
"""
StrategyRouter - 策略路由
输入：regime + feature_vector
输出：regime_adjusted_score, route_reason
规则：
  - BULL：偏进攻，动量权重上调
  - RANGE：偏均衡，回撤质量权重上调
  - BEAR：偏防御，过滤阈值上调
"""

from typing import Dict, Any, Optional
from .market_regime import MarketRegime


class StrategyRouter:
    """策略路由器：根据市场状态调整评分权重"""

    def __init__(self):
        self.regime = None
        self.confidence = 0.0

    def route(self, regime: str, confidence: float,
              feature_vector: Dict[str, Any]) -> Dict[str, Any]:
        """
        根据 regime 调整评分

        Args:
            regime: BULL/RANGE/BEAR
            confidence: 0~1
            feature_vector: {
                "score": 原始综合分,
                "rsi": RSI值,
                "above_ma20": bool,
                "above_ma60": bool,
                "above_ma120": bool,
                "vol_ratio": 成交量/MA20,
                "drawdown_20d": 20日回撤,
                "momentum_5d": 5日涨幅,
            }

        Returns:
            {
                "regime_adjusted_score": 调整后得分,
                "route_reason": "原因说明",
                "weight_momentum": 动量权重,
                "weight_drawdown": 回撤权重,
                "weight_defense": 防御权重,
            }
        """
        self.regime = regime
        self.confidence = confidence

        raw_score = feature_vector.get("score", 50)
        rsi = feature_vector.get("rsi", 50)
        above_ma20 = feature_vector.get("above_ma20", False)
        above_ma60 = feature_vector.get("above_ma60", False)
        momentum_5d = feature_vector.get("momentum_5d", 0)
        drawdown_20d = feature_vector.get("drawdown_20d", 0)

        # 基础分量（0~100）
        momentum_component = 50
        if above_ma20:
            momentum_component += 10
        if above_ma60:
            momentum_component += 10
        if momentum_5d > 0.02:
            momentum_component += 15
        elif momentum_5d > 0:
            momentum_component += 5
        if rsi > 55:
            momentum_component += 5
        momentum_component = min(100, momentum_component)

        drawdown_component = 70  # 回撤小=好
        if drawdown_20d > 0.15:
            drawdown_component -= 40
        elif drawdown_20d > 0.08:
            drawdown_component -= 20
        elif drawdown_20d < 0.03:
            drawdown_component += 15
        drawdown_component = max(0, min(100, drawdown_component))

        defense_component = raw_score  # 直接用原始分作防御

        # 权重随 regime 变化
        if regime == MarketRegime.BULL:
            w_m, w_d, w_def = 0.45, 0.20, 0.35
            route_reason = "牛市：动量权重上调，偏进攻"
        elif regime == MarketRegime.RANGE:
            w_m, w_d, w_def = 0.30, 0.35, 0.35
            route_reason = "震荡：回撤质量权重上调，均衡"
        else:  # BEAR
            w_m, w_d, w_def = 0.20, 0.35, 0.45
            route_reason = "熊市：防御权重上调，过滤阈值提高"

        adjusted = (
            momentum_component * w_m +
            drawdown_component * w_d +
            defense_component * w_def
        )
        adjusted = max(0, min(100, int(adjusted)))

        return {
            "regime_adjusted_score": adjusted,
            "route_reason": route_reason,
            "weight_momentum": w_m,
            "weight_drawdown": w_d,
            "weight_defense": w_def,
            "raw_score": raw_score,
            "momentum_component": momentum_component,
            "drawdown_component": drawdown_component,
        }

    def route_dual_engine(self, regime: str, quality_score: float, potential_score: float) -> Dict[str, Any]:
        """
        双引擎融合：final = w_q * quality + w_p * potential
        BULL: 0.45*quality + 0.55*potential
        RANGE: 0.55*quality + 0.45*potential
        BEAR: 0.70*quality + 0.30*potential
        """
        quality_score = max(0, min(100, quality_score))
        potential_score = max(0, min(100, potential_score))
        if regime == MarketRegime.BULL:
            w_q, w_p = 0.45, 0.55
        elif regime == MarketRegime.RANGE:
            w_q, w_p = 0.55, 0.45
        else:
            w_q, w_p = 0.70, 0.30
        final = w_q * quality_score + w_p * potential_score
        return {
            "final_score": round(min(100, max(0, final)), 1),
            "quality_score": quality_score,
            "potential_score": potential_score,
            "w_quality": w_q,
            "w_potential": w_p,
            "regime": regime,
        }
