"""
indicators.py — 技术指标计算

所有指标均基于 pandas DataFrame（列名：open/high/low/close/volume）
返回值均为 float 或 bool，方便 scanner 直接判断
"""

from __future__ import annotations
import numpy as np
import pandas as pd


# ─────────────────────────────────────────────
# 基础指标
# ─────────────────────────────────────────────

def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(period).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def macd(series: pd.Series, fast=12, slow=26, signal=9):
    """返回 (macd_line, signal_line, histogram)"""
    fast_ema = ema(series, fast)
    slow_ema = ema(series, slow)
    macd_line = fast_ema - slow_ema
    signal_line = ema(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


def adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    ADX 趋势强度指标（v2.0 新增）
    ADX > 20 认为趋势足够强，可以开仓
    """
    high, low, close = df["high"], df["low"], df["close"]
    prev_high = high.shift(1)
    prev_low = low.shift(1)

    plus_dm = (high - prev_high).clip(lower=0)
    minus_dm = (prev_low - low).clip(lower=0)
    plus_dm[plus_dm < minus_dm] = 0
    minus_dm[minus_dm < plus_dm] = 0

    tr_series = atr(df, 1) * 1  # 单周期 TR
    atr14 = atr(df, period)

    plus_di = 100 * ema(plus_dm, period) / atr14.replace(0, np.nan)
    minus_di = 100 * ema(minus_dm, period) / atr14.replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx_series = ema(dx, period)
    return adx_series


# ─────────────────────────────────────────────
# 信号判断函数（返回 bool）
# ─────────────────────────────────────────────

def is_ema_bullish(df: pd.DataFrame, fast: int = 20, slow: int = 50) -> bool:
    """EMA20 > EMA50（最新bar）"""
    if len(df) < slow + 1:
        return False
    e_fast = ema(df["close"], fast)
    e_slow = ema(df["close"], slow)
    return bool(e_fast.iloc[-1] > e_slow.iloc[-1])


def is_rsi_healthy(df: pd.DataFrame, period: int = 14,
                   low: float = 50.0, high: float = 65.0) -> bool:
    """50 < RSI < 65"""
    if len(df) < period + 5:
        return False
    r = rsi(df["close"], period)
    val = r.iloc[-1]
    return bool(low < val < high)


def is_macd_bullish(df: pd.DataFrame) -> bool:
    """MACD 金叉 且 在零轴上方"""
    if len(df) < 35:
        return False
    m, s, _ = macd(df["close"])
    # 金叉：前一根 MACD < Signal，当前 MACD > Signal
    cross = (m.iloc[-2] < s.iloc[-2]) and (m.iloc[-1] > s.iloc[-1])
    above_zero = m.iloc[-1] > 0
    return bool(cross and above_zero)


def is_volume_confirmed(df: pd.DataFrame, lookback: int = 20,
                         multiplier: float = 1.2) -> bool:
    """
    当前5分钟成交量 > 过去20根5分钟均量的1.2倍（v2.0 新增）
    """
    if len(df) < lookback + 1:
        return False
    avg_vol = df["volume"].iloc[-(lookback + 1):-1].mean()
    cur_vol = df["volume"].iloc[-1]
    return bool(cur_vol > avg_vol * multiplier)


def is_trend_strong(df_daily: pd.DataFrame, period: int = 14,
                     threshold: float = 20.0) -> bool:
    """
    ADX > threshold，趋势强度过滤（v2.0 新增）
    df_daily：日线数据（用日线计算趋势强度更稳定）
    """
    if len(df_daily) < period * 3:
        return False
    adx_series = adx(df_daily, period)
    val = adx_series.iloc[-1]
    if pd.isna(val):
        return False
    return bool(val > threshold)


def is_above_ma200(df_daily: pd.DataFrame) -> bool:
    """大盘价格在200日均线上方"""
    if len(df_daily) < 200:
        return False
    ma = sma(df_daily["close"], 200)
    return bool(df_daily["close"].iloc[-1] > ma.iloc[-1])


def get_atr_stop(df: pd.DataFrame, entry_price: float,
                  period: int = 14, multiplier: float = 2.0) -> float:
    """ATR 动态止损价"""
    atr_val = atr(df, period).iloc[-1]
    return entry_price - multiplier * atr_val


def get_trailing_stop(entry_price: float, peak_price: float,
                       tiers: list) -> float:
    """
    分层追踪止盈（v1.0 保留）
    tiers: [(gain_low, gain_high, drawdown_pct), ...]
    """
    gain = (peak_price - entry_price) / entry_price
    for low, high, dd in tiers:
        if low <= gain < high:
            return peak_price * (1 - dd)
    # 超出所有层级，用最后一层
    return peak_price * (1 - tiers[-1][2])


# ─────────────────────────────────────────────
# 手续费 & 滑点
# ─────────────────────────────────────────────

def net_price_buy(price: float, commission: float, slippage: float) -> float:
    """实际买入成本（含滑点和手续费）"""
    return price * (1 + slippage + commission)


def net_price_sell(price: float, commission: float, slippage: float) -> float:
    """实际卖出到手（扣除滑点和手续费）"""
    return price * (1 - slippage - commission)


def calc_pnl(entry_price: float, exit_price: float, shares: int,
             commission: float, slippage: float) -> float:
    """计算净盈亏（含双边手续费和滑点）"""
    buy_cost = net_price_buy(entry_price, commission, slippage) * shares
    sell_recv = net_price_sell(exit_price, commission, slippage) * shares
    return sell_recv - buy_cost
