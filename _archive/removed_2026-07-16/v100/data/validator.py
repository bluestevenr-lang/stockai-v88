"""数据验证层：价格合理性检查 + 双源对账"""
import logging
import pandas as pd
from typing import Optional

log = logging.getLogger(__name__)

MAX_DAILY_MOVE = 0.25
MIN_ROWS = 10


def validate_price(df: pd.DataFrame, code: str = "") -> tuple[bool, str]:
    if df is None or df.empty:
        return False, "空数据"
    if len(df) < MIN_ROWS:
        return False, f"数据量不足({len(df)}行)"

    close = df["Close"]
    if (close <= 0).any():
        return False, "存在非正价格"

    daily_ret = close.pct_change().abs().dropna()
    outliers = (daily_ret > MAX_DAILY_MOVE).sum()
    if outliers > 3:
        return False, f"异常波动{outliers}次(>{MAX_DAILY_MOVE*100:.0f}%)"

    return True, "ok"


def cross_validate(df_primary: pd.DataFrame,
                   df_secondary: Optional[pd.DataFrame],
                   tolerance: float = 0.02) -> tuple[bool, str]:
    if df_secondary is None or df_secondary.empty:
        return True, "secondary_unavailable"

    common = df_primary.index.intersection(df_secondary.index)
    if len(common) < 5:
        return True, "overlap_too_small"

    p = df_primary.loc[common, "Close"]
    s = df_secondary.loc[common, "Close"]
    diff = ((p - s).abs() / p).mean()

    if diff > tolerance:
        return False, f"双源价差{diff*100:.1f}% > {tolerance*100:.0f}%"
    return True, f"双源一致(偏差{diff*100:.2f}%)"
