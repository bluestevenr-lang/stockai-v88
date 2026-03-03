# -*- coding: utf-8 -*-
"""
OpportunityClassifier - 三池分类
输出标签：BUILD_NOW / FOLLOW_MID / LONG_CORE / FILTERED
每个池子有准入条件 + 剔除条件
"""

from typing import Dict, Any, Optional
from .market_regime import MarketRegime


class OpportunityClassifier:
    """机会分类器：将标的分为三池"""

    BUILD_NOW = "BUILD_NOW"
    FOLLOW_MID = "FOLLOW_MID"
    LONG_CORE = "LONG_CORE"
    FILTERED = "FILTERED"

    # 准入/剔除阈值（随 regime 变化）
    THRESHOLDS = {
        MarketRegime.BULL: {
            "BUILD_NOW": {"min_score": 75, "min_rsi": 45, "max_rsi": 85},
            "FOLLOW_MID": {"min_score": 65, "min_rsi": 35},
            "LONG_CORE": {"min_score": 70, "above_ma120": True},
        },
        MarketRegime.RANGE: {
            "BUILD_NOW": {"min_score": 78, "min_rsi": 45, "max_rsi": 80},
            "FOLLOW_MID": {"min_score": 68, "min_rsi": 35},
            "LONG_CORE": {"min_score": 72, "above_ma120": True},
        },
        MarketRegime.BEAR: {
            "BUILD_NOW": {"min_score": 85, "min_rsi": 40, "max_rsi": 75},
            "FOLLOW_MID": {"min_score": 72, "min_rsi": 30},
            "LONG_CORE": {"min_score": 78, "above_ma120": True},
        },
    }

    def classify(self, regime: str, regime_adjusted_score: int,
                 feature_vector: Dict[str, Any],
                 quality_ok: bool = True,
                 allows_long_core: bool = True) -> Dict[str, Any]:
        """
        分类到三池

        Args:
            regime: BULL/RANGE/BEAR
            regime_adjusted_score: 策略路由后的得分
            feature_vector: 包含 rsi, above_ma20, above_ma60, above_ma120 等
            quality_ok: QualityGuard 校验是否通过

        Returns:
            {
                "action_label": "BUILD_NOW"|"FOLLOW_MID"|"LONG_CORE"|"FILTERED",
                "action_emoji": "⚡"|"🌊"|"💎"|"🚫",
                "classify_reason": "原因",
            }
        """
        if not quality_ok:
            return {
                "action_label": self.FILTERED,
                "action_emoji": "🚫",
                "classify_reason": "数据质量校验未通过",
            }

        th = self.THRESHOLDS.get(regime, self.THRESHOLDS[MarketRegime.RANGE])
        rsi = feature_vector.get("rsi", 50)
        above_ma120 = feature_vector.get("above_ma120", False)

        # 剔除条件
        if rsi > 88 or rsi < 22:
            return {
                "action_label": self.FILTERED,
                "action_emoji": "🚫",
                "classify_reason": f"RSI极端({rsi:.0f})，规避",
            }

        # BUILD_NOW 准入（最高优先级）
        t_bn = th["BUILD_NOW"]
        if (regime_adjusted_score >= t_bn["min_score"] and
                t_bn["min_rsi"] <= rsi <= t_bn.get("max_rsi", 99)):
            return {
                "action_label": self.BUILD_NOW,
                "action_emoji": "⚡",
                "classify_reason": f"评分{regime_adjusted_score}达标，可立即建仓",
            }

        # LONG_CORE 准入【长线法宝】优先于 FOLLOW_MID：必须 allows_long_core + 质量+估值+边际
        t_lc = th["LONG_CORE"]
        if (allows_long_core and regime_adjusted_score >= t_lc["min_score"] and
                (above_ma120 or not t_lc.get("above_ma120", False))):
            return {
                "action_label": self.LONG_CORE,
                "action_emoji": "💎",
                "classify_reason": f"长期配置标的，评分{regime_adjusted_score}",
            }

        # FOLLOW_MID 准入
        t_fm = th["FOLLOW_MID"]
        if regime_adjusted_score >= t_fm["min_score"] and rsi >= t_fm["min_rsi"]:
            return {
                "action_label": self.FOLLOW_MID,
                "action_emoji": "🌊",
                "classify_reason": f"评分{regime_adjusted_score}，中期跟进",
            }

        return {
            "action_label": self.FILTERED,
            "action_emoji": "🚫",
            "classify_reason": f"未达任一池准入(得分{regime_adjusted_score})",
        }
