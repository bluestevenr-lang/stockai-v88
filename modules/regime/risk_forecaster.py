# -*- coding: utf-8 -*-
"""
RiskForecaster - 三概率预测
输出：p_up_continuation, p_drawdown, p_false_breakout, reasons_top3
先做规则+指标版，预留模型升级接口
"""

from typing import Dict, Any, Optional, List
import pandas as pd


class RiskForecaster:
    """风险预测器：规则+指标版三概率"""

    def forecast(self, df, last: dict, regime: str = "RANGE") -> Dict[str, Any]:
        """
        预测未来5-20交易日三概率

        Args:
            df: 日线数据
            last: 最后一行数据
            regime: 市场状态

        Returns:
            {
                "p_up_continuation": 0~1,
                "p_drawdown": 0~1,
                "p_false_breakout": 0~1,
                "reasons_top3": ["原因1", "原因2", "原因3"],
            }
        """
        if df is None or len(df) < 20:
            return {
                "p_up_continuation": 0.5,
                "p_drawdown": 0.5,
                "p_false_breakout": 0.3,
                "reasons_top3": ["数据不足，默认中性"],
            }

        reasons = []
        p_up = 0.5
        p_dd = 0.5
        p_fb = 0.3

        # 基于 RSI
        rsi = last.get("RSI", 50)
        if rsi > 70:
            p_up -= 0.1
            p_dd += 0.15
            p_fb += 0.1
            reasons.append("RSI超买，上行延续概率降")
        elif rsi < 35:
            p_up += 0.1
            p_dd -= 0.05
            reasons.append("RSI超卖，反弹概率升")

        # 基于均线
        close = last.get("Close", 0)
        ma20 = last.get("MA20", close)
        ma60 = last.get("MA60", close)
        if ma20 and ma60 and ma20 > 0 and ma60 > 0:
            if close > ma20 > ma60:
                p_up += 0.1
                reasons.append("均线多头排列，趋势延续")
            elif close < ma20 < ma60:
                p_dd += 0.1
                reasons.append("均线空头，回撤风险升")

        # 基于新高/新低
        if len(df) >= 60:
            high_60 = df["High"].tail(60).max()
            if close >= high_60 * 0.98:
                p_fb += 0.15
                reasons.append("接近前高，警惕假突破")
            low_60 = df["Low"].tail(60).min()
            if close <= low_60 * 1.02:
                p_up += 0.05
                p_dd -= 0.05
                reasons.append("接近前低，反弹空间")

        # regime 微调
        if regime == "BULL":
            p_up = min(0.85, p_up + 0.05)
            p_dd = max(0.15, p_dd - 0.05)
        elif regime == "BEAR":
            p_up = max(0.2, p_up - 0.1)
            p_dd = min(0.85, p_dd + 0.1)

        # 归一化
        p_up = max(0.1, min(0.9, p_up))
        p_dd = max(0.1, min(0.9, p_dd))
        p_fb = max(0.1, min(0.6, p_fb))

        return {
            "p_up_continuation": round(p_up, 3),
            "p_drawdown": round(p_dd, 3),
            "p_false_breakout": round(p_fb, 3),
            "reasons_top3": reasons[:3] if reasons else ["技术面中性"],
        }
