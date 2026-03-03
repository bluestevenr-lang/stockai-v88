# -*- coding: utf-8 -*-
"""
ActionEngine - 动作引擎
输出：suggested_position_range, tranche_plan, invalidation_rules, invalidation_price
注意：价格止损仅辅助，不能替代基本面/结构失效
"""

from typing import Dict, Any, Optional
from .opportunity_classifier import OpportunityClassifier


class ActionEngine:
    """动作引擎：仓位、分批、失效条件"""

    def compute(self, action_label: str, risk_probs: Dict[str, float],
                risk_preference: str = "平衡",
                atr: float = 0.0, close: float = 0.0) -> Dict[str, Any]:
        """
        计算执行建议

        Args:
            action_label: BUILD_NOW/FOLLOW_MID/LONG_CORE/FILTERED
            risk_probs: {"p_up_continuation", "p_drawdown", "p_false_breakout"}
            risk_preference: 保守/平衡/进攻
            atr: ATR值
            close: 现价

        Returns:
            {
                "suggested_position_range": "20-30%",
                "tranche_plan": "30/40/30",
                "invalidation_rules": ["基本面恶化", "跌破关键结构"],
                "invalidation_price": 0.0 or float,
                "holding_period": "5-20交易日",
            }
        """
        if action_label == OpportunityClassifier.FILTERED:
            return {
                "suggested_position_range": "0%",
                "tranche_plan": "-",
                "invalidation_rules": ["不满足准入条件"],
                "invalidation_price": 0.0,
                "holding_period": "不建议",
            }

        # 风险偏好系数
        pref_mult = {"保守": 0.7, "平衡": 1.0, "进攻": 1.2}.get(risk_preference, 1.0)

        # 基础仓位 + 【长线法宝】仓位上限与持有期强制绑定
        if action_label == OpportunityClassifier.BUILD_NOW:
            base_range = "20-30%"
            tranche = "30/40/30"
            period = "5-20交易日"
            position_cap = 30
        elif action_label == OpportunityClassifier.FOLLOW_MID:
            base_range = "10-20%"
            tranche = "40/30/30"
            period = "10-30交易日"
            position_cap = 20
        else:
            base_range = "15-25%"
            tranche = "30/35/35"
            period = "20-60交易日"
            position_cap = 25

        # 失效条件（基本面/结构优先）
        invalidation_rules = [
            "基本面恶化（业绩大幅低于预期、重大利空）",
            "跌破关键结构（如MA60/MA120有效跌破）",
            "行业逻辑推翻",
        ]

        # ATR 辅助止损价（可选）
        invalidation_price = 0.0
        if atr > 0 and close > 0:
            invalidation_price = round(close - 2 * atr, 2)
            invalidation_rules.append(f"价格止损辅助：跌破{invalidation_price:.2f}（2ATR）")

        return {
            "suggested_position_range": base_range,
            "tranche_plan": tranche,
            "invalidation_rules": invalidation_rules,
            "invalidation_price": invalidation_price,
            "holding_period": period,
            "position_cap_percent": position_cap,
            "risk_preference_adj": pref_mult,
        }
