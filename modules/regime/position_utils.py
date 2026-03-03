# -*- coding: utf-8 -*-
"""
统一水位计算 - get_position_level_unified
由后端统一计算，禁止前端自行推断
"""

from typing import Dict, Any, Optional, Tuple
import pandas as pd


def get_position_level_unified(df: pd.DataFrame, last_close: float) -> Tuple[str, float]:
    """
    统一计算水位：高/中/低 + 百分位

    Args:
        df: 日线数据
        last_close: 最新收盘价

    Returns:
        (position_level, position_percentile)
        position_level: "高"|"中"|"低"
        position_percentile: 0~100，表示在250日（或可用数据）区间的百分位
    """
    if df is None or len(df) < 5:
        return ("N/A", 0.0)

    n = min(250, len(df))
    low_n = df["Low"].tail(n).min()
    high_n = df["High"].tail(n).max()

    if high_n <= low_n:
        return ("N/A", 50.0)

    percentile = (last_close - low_n) / (high_n - low_n) * 100
    percentile = max(0, min(100, percentile))

    if percentile >= 75:
        level = "高"
    elif percentile >= 35:
        level = "中"
    else:
        level = "低"

    return (level, round(percentile, 1))
