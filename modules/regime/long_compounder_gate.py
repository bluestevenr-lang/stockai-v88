# -*- coding: utf-8 -*-
"""
LongCompounderGate - 长线复利框架约束层

源自「长线法宝」：护城河、ROIC、FCF质量、利润率稳定
作为底层约束，对 Regime + Dual Score + ActionEngine 进行硬过滤和仓位约束。
不独立成策，仅作为准入条件。

输出：long_compounder_score (0~100)、passes_long_compounder_gate (bool)
"""

from typing import Dict, Any, Optional
import pandas as pd
import numpy as np


class LongCompounderGate:
    """长线复利框架：护城河、ROIC、FCF质量、利润率稳定"""

    def compute(self, df: pd.DataFrame, code: str, sector: str = "",
                m: Optional[Dict] = None) -> Dict[str, Any]:
        """
        计算 long_compounder_score（0~100）

        数据缺失时用 proxy 占位，优先保证可运行。
        子因子：moat_proxy, roic_proxy, fcf_quality_proxy, margin_stability_proxy

        Returns:
            {
                "long_compounder_score": 0~100,
                "moat_proxy": 0~1,
                "roic_proxy": 0~1,
                "fcf_quality_proxy": 0~1,
                "margin_stability_proxy": 0~1,
                "passes_long_compounder_gate": bool,
                "compound_tags": ["标签1", "标签2"],
            }
        """
        result = {
            "long_compounder_score": 50,
            "moat_proxy": 0.5,
            "roic_proxy": 0.5,
            "fcf_quality_proxy": 0.5,
            "margin_stability_proxy": 0.5,
            "passes_long_compounder_gate": False,
            "compound_tags": [],
        }

        if df is None or len(df) < 60:
            return result

        last = df.iloc[-1]
        close = float(last.get("Close", 0))
        if close <= 0:
            return result

        tags = []
        ma60 = last.get("MA60", 0) or (df["Close"].rolling(60).mean().iloc[-1] if len(df) >= 60 else 0)
        ma120 = last.get("MA120", 0) or (df["Close"].rolling(120).mean().iloc[-1] if len(df) >= 120 else 0)
        ma250 = last.get("MA250", 0) or (df["Close"].rolling(250).mean().iloc[-1] if len(df) >= 250 else 0)

        # 1) moat_proxy：价格稳定性（强势股相对MA120偏离小且趋势向上）
        if ma120 and ma120 > 0:
            dev = abs(close - ma120) / ma120
            if dev < 0.05 and close > ma120:
                moat_proxy = 0.85
                tags.append("护城河代理：趋势稳健")
            elif dev < 0.15:
                moat_proxy = 0.65
            else:
                moat_proxy = max(0.3, 0.7 - dev)
        else:
            moat_proxy = 0.5
        result["moat_proxy"] = round(moat_proxy, 3)

        # 2) roic_proxy：动量与回撤质量（无数据时占位）
        result["roic_proxy"] = 0.5

        # 3) fcf_quality_proxy：成交量/价格关系（量价配合=0.7）
        if len(df) >= 20:
            vol_ma20 = df["Volume"].tail(20).mean()
            last_vol = df["Volume"].iloc[-1]
            if vol_ma20 > 0:
                vol_ratio = last_vol / vol_ma20
                if 0.8 <= vol_ratio <= 1.5 and close > (df["Close"].iloc[-5] if len(df) >= 5 else close):
                    fcf_proxy = 0.7
                    tags.append("量价配合")
                else:
                    fcf_proxy = 0.5
            else:
                fcf_proxy = 0.5
        else:
            fcf_proxy = 0.5
        result["fcf_quality_proxy"] = round(fcf_proxy, 3)

        # 4) margin_stability_proxy：价格波动率（低波动=稳定性高）
        if len(df) >= 30:
            ret = df["Close"].pct_change().tail(30).dropna()
            vol = ret.std() * np.sqrt(252) * 100 if len(ret) > 0 else 25
            if vol < 15:
                margin_proxy = 0.8
                tags.append("波动率低")
            elif vol < 25:
                margin_proxy = 0.65
            else:
                margin_proxy = max(0.3, 0.7 - vol / 80)
        else:
            margin_proxy = 0.5
        result["margin_stability_proxy"] = round(margin_proxy, 3)

        # 综合 long_compounder_score
        weights = [0.25, 0.25, 0.25, 0.25]
        factors = [result["moat_proxy"], result["roic_proxy"], result["fcf_quality_proxy"], result["margin_stability_proxy"]]
        score = sum(w * f for w, f in zip(weights, factors)) * 100
        result["long_compounder_score"] = round(min(100, max(0, score)), 1)
        result["compound_tags"] = tags[:3]
        result["passes_long_compounder_gate"] = result["long_compounder_score"] >= 55

        return result
