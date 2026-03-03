# -*- coding: utf-8 -*-
"""
ExpectationGapEngine - 预期差潜力引擎

输出 potential_score (0~100) 及 7 子因子：
1) valuation_gap   - 估值分位（proxy: 价格相对MA250）
2) earnings_revision - 盈利预期上修（占位）
3) moat_trend     - 护城河边际（占位）
4) cycle_position - 周期位置（价格相对MA60/120）
5) catalyst_score - 催化剂强度（占位）
6) crowding_score - 拥挤度（占位）
7) delta_score   - 20日边际变化斜率（提前量核心）

MVP：可用数据做 proxy，缺失因子用 0.5 占位
"""

from typing import Dict, Any, Optional, List, Tuple
import pandas as pd
import numpy as np


class ExpectationGapEngine:
    """预期差潜力引擎"""

    def compute(self, df: pd.DataFrame, code: str, sector: str = "") -> Dict[str, Any]:
        """
        计算 potential_score 及 7 子因子

        Args:
            df: 日线数据（含 Close, MA30, MA60, MA120, MA250）
            code: 股票代码
            sector: 行业（可选）

        Returns:
            {
                "potential_score": 0~100,
                "valuation_gap": 0~1,
                "earnings_revision": 0~1,
                "moat_trend": 0~1,
                "cycle_position": 0~1,
                "catalyst_score": 0~1,
                "crowding_score": 0~1,
                "delta_score": 0~1,
                "potential_tags": ["标签1", "标签2", "标签3"],
                "hard_threshold_count": 满足7选4的数量,
                "passes_potential_gate": bool,
            }
        """
        result = {
            "potential_score": 50,
            "valuation_gap": 0.5,
            "earnings_revision": 0.5,
            "moat_trend": 0.5,
            "cycle_position": 0.5,
            "catalyst_score": 0.5,
            "crowding_score": 0.5,
            "delta_score": 0.5,
            "potential_tags": [],
            "hard_threshold_count": 0,
            "passes_potential_gate": False,
        }

        if df is None or len(df) < 20:
            return result

        last = df.iloc[-1]
        close = float(last.get("Close", 0))
        if close <= 0:
            return result

        tags = []
        threshold_hits = 0

        # 1) valuation_gap：价格相对MA250分位，<0.9 视为低估
        ma250 = last.get("MA250", 0) or (df["Close"].rolling(250).mean().iloc[-1] if len(df) >= 250 else 0)
        if ma250 and ma250 > 0:
            ratio = close / ma250
            if ratio < 0.85:
                valuation_gap = 0.9
                tags.append("估值偏低")
                threshold_hits += 1
            elif ratio < 0.95:
                valuation_gap = 0.7
                tags.append("估值合理")
                threshold_hits += 1
            elif ratio < 1.05:
                valuation_gap = 0.5
            else:
                valuation_gap = 0.3
        else:
            valuation_gap = 0.5
        result["valuation_gap"] = round(valuation_gap, 3)

        # 2) earnings_revision：占位（无分析师数据）
        result["earnings_revision"] = 0.5

        # 3) moat_trend：占位（无财务数据）
        result["moat_trend"] = 0.5

        # 4) cycle_position：价格相对MA60/MA120，底部抬升优先
        ma60 = last.get("MA60", 0) or (df["Close"].rolling(60).mean().iloc[-1] if len(df) >= 60 else 0)
        ma120 = last.get("MA120", 0) or (df["Close"].rolling(120).mean().iloc[-1] if len(df) >= 120 else 0)
        if ma60 and ma60 > 0:
            pos_60 = (close - ma60) / ma60
            if pos_60 < -0.05 and ma120 and ma120 > 0 and close > ma120 * 0.9:
                cycle_position = 0.8
                tags.append("周期底部抬升")
                threshold_hits += 1
            elif pos_60 > 0.02:
                cycle_position = 0.65
            else:
                cycle_position = 0.5
        else:
            cycle_position = 0.5
        result["cycle_position"] = round(cycle_position, 3)

        # 5) catalyst_score：占位
        result["catalyst_score"] = 0.5

        # 6) crowding_score：占位（非极端拥挤=0.6）
        result["crowding_score"] = 0.6
        threshold_hits += 1

        # 7) 技术结构未破坏：价格 > MA60×0.9
        if ma60 and ma60 > 0 and close >= ma60 * 0.9:
            threshold_hits += 1

        # delta_score：20日边际变化斜率（提前量核心）
        if len(df) >= 25:
            closes = df["Close"].tail(25).values
            x = np.arange(25)
            if np.std(closes) > 0:
                slope = np.polyfit(x, closes, 1)[0] / np.mean(closes) * 100
                if slope > 0.3:
                    delta_score = min(0.95, 0.5 + slope / 2)
                    tags.append("边际改善")
                elif slope > 0:
                    delta_score = 0.6
                else:
                    delta_score = max(0.2, 0.5 + slope / 2)
            else:
                delta_score = 0.5
        else:
            delta_score = 0.5
        result["delta_score"] = round(delta_score, 3)

        # 综合 potential_score (0~100)
        weights = [0.15, 0.10, 0.10, 0.20, 0.10, 0.10, 0.25]
        factors = [
            result["valuation_gap"],
            result["earnings_revision"],
            result["moat_trend"],
            result["cycle_position"],
            result["catalyst_score"],
            result["crowding_score"],
            result["delta_score"],
        ]
        potential_score = sum(w * f for w, f in zip(weights, factors)) * 100
        result["potential_score"] = round(min(100, max(0, potential_score)), 1)

        result["potential_tags"] = tags[:3]
        result["hard_threshold_count"] = threshold_hits
        result["passes_potential_gate"] = threshold_hits >= 4

        # expectation_gap_score: 估值分位、盈利修正、边际改善、催化剂（与 potential_score 同源，强调预期差维度）
        result["expectation_gap_score"] = round(
            (result["valuation_gap"] * 0.30 + result["earnings_revision"] * 0.20 +
             result["delta_score"] * 0.30 + result["catalyst_score"] * 0.20) * 100, 1
        )

        return result
