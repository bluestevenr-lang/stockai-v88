# -*- coding: utf-8 -*-
"""
MarginOfSafetyGate - 安全边际纪律约束层

源自「长线法宝」：高估值分位禁止LONG_CORE，仅「质量达标+估值合理+边际改善」允许长期配置。
作为底层约束，对 ActionEngine 的 LONG_CORE 设为强制准入条件。
"""

from typing import Dict, Any, Optional
import pandas as pd


# 不可投硬过滤清单：价值陷阱信号（优先过滤）
VALUE_TRAP_SIGNALS = [
    "持续阴跌", "量价背离", "业绩暴雷", "商誉减值", "大股东减持",
    "行业景气下行", "技术破位", "流动性枯竭",
]

# 不可投硬过滤清单（供外部引用）
UNINVESTABLE_HARD_FILTER = {
    "value_trap_keywords": VALUE_TRAP_SIGNALS,
    "max_valuation_ratio_long_core": 1.05,
    "min_long_compounder_score": 55,
}


class MarginOfSafetyGate:
    """安全边际纪律：估值分位、边际改善、LONG_CORE 准入"""

    # 高估值分位阈值：价格/MA250 > 1.15 视为高估，禁止 LONG_CORE
    HIGH_VALUATION_RATIO = 1.15
    # 允许 LONG_CORE 的估值上限：价格/MA250 <= 1.05
    LONG_CORE_VALUATION_CAP = 1.05

    def compute(self, df: pd.DataFrame, gap_result: Optional[Dict] = None,
                long_compound_result: Optional[Dict] = None) -> Dict[str, Any]:
        """
        安全边际评估：估值分位、是否允许 LONG_CORE、是否价值陷阱

        Returns:
            {
                "valuation_percentile": 0~1,  # 1=高估
                "valuation_ratio_ma250": float,
                "marginal_improvement": bool,
                "allows_long_core": bool,
                "block_reason": str or None,
                "value_trap_risk": bool,
                "value_trap_signals": [],
            }
        """
        result = {
            "valuation_percentile": 0.5,
            "valuation_ratio_ma250": 1.0,
            "marginal_improvement": False,
            "allows_long_core": False,
            "block_reason": None,
            "value_trap_risk": False,
            "value_trap_signals": [],
        }

        if df is None or len(df) < 60:
            result["block_reason"] = "数据不足"
            return result

        last = df.iloc[-1]
        close = float(last.get("Close", 0))
        ma250 = last.get("MA250", 0) or (df["Close"].rolling(250).mean().iloc[-1] if len(df) >= 250 else 0)

        if ma250 and ma250 > 0:
            ratio = close / ma250
            result["valuation_ratio_ma250"] = round(ratio, 3)
            if ratio > 1.2:
                result["valuation_percentile"] = 0.95
            elif ratio > 1.1:
                result["valuation_percentile"] = 0.8
            elif ratio > 1.0:
                result["valuation_percentile"] = 0.6
            elif ratio > 0.9:
                result["valuation_percentile"] = 0.4
            else:
                result["valuation_percentile"] = 0.2

        # 边际改善：来自 gap_result.delta_score
        if gap_result:
            delta = gap_result.get("delta_score", 0.5)
            result["marginal_improvement"] = delta >= 0.55

        # 质量达标：来自 long_compound_result
        quality_ok = False
        if long_compound_result:
            quality_ok = long_compound_result.get("passes_long_compounder_gate", False)

        # LONG_CORE 准入逻辑：质量达标 + 估值合理 + 边际改善
        ratio = result["valuation_ratio_ma250"]
        if ratio > self.HIGH_VALUATION_RATIO:
            result["allows_long_core"] = False
            result["block_reason"] = f"高估值分位( price/MA250={ratio:.2f} > {self.HIGH_VALUATION_RATIO} )，禁止长期重仓"
        elif ratio > self.LONG_CORE_VALUATION_CAP:
            result["allows_long_core"] = False
            result["block_reason"] = f"估值偏高( price/MA250={ratio:.2f} )，仅允许短线/中线"
        elif not quality_ok:
            result["allows_long_core"] = False
            result["block_reason"] = "质量未达长线复利门槛"
        elif not result["marginal_improvement"]:
            result["allows_long_core"] = False
            result["block_reason"] = "边际改善不足"
        else:
            result["allows_long_core"] = True

        return result

    @staticmethod
    def check_value_trap(industry: str = "", logic: str = "", suggestion: str = "") -> Dict[str, Any]:
        """
        价值陷阱硬过滤：关键词匹配
        Returns: {"is_value_trap": bool, "matched_signals": []}
        """
        text = f"{industry} {logic} {suggestion}".lower()
        matched = []
        for sig in VALUE_TRAP_SIGNALS:
            if sig in text:
                matched.append(sig)
        return {
            "is_value_trap": len(matched) >= 1,
            "matched_signals": matched,
        }
