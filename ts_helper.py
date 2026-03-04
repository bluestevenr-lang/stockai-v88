"""
ts_helper.py — Tushare A股数据助手（全局共用）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
A股数据优先从 Tushare 获取（全球可用），失败时自动降级 yfinance。
适用于 app_v88_integrated.py / auto_reporter.py / scan_worker.py。

依赖：tushare, pandas, yfinance
Token 来源（优先级）：
    1. 环境变量 TUSHARE_TOKEN
    2. Streamlit st.secrets["TUSHARE_TOKEN"]（可选）
"""

import os
import time
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

# ── Token 读取 ───────────────────────────────────────────────────
def _read_token() -> str:
    token = os.environ.get("TUSHARE_TOKEN", "")
    if not token:
        try:
            import streamlit as st
            token = st.secrets.get("TUSHARE_TOKEN", "")
        except Exception:
            pass
    return token.strip()


# ── Tushare pro_api 单例 ─────────────────────────────────────────
_pro = None
_pro_ok = False   # False = 未初始化；None = 初始化失败

def get_pro():
    """返回 Tushare pro_api 实例，失败返回 None"""
    global _pro, _pro_ok
    if _pro_ok is None:      # 已确认失败，直接返回
        return None
    if _pro is not None:
        return _pro
    token = _read_token()
    if not token:
        _pro_ok = None
        return None
    try:
        import tushare as ts
        ts.set_token(token)
        _pro = ts.pro_api()
        _pro_ok = True
        log.info("Tushare 初始化成功")
        return _pro
    except Exception as e:
        log.warning(f"Tushare 初始化失败: {e}")
        _pro_ok = None
        return None


# ── 代码格式转换 ─────────────────────────────────────────────────
def yf_to_ts(yf_code: str) -> str:
    """600519.SS → 600519.SH，其余不变"""
    return yf_code[:-3] + ".SH" if yf_code.endswith(".SS") else yf_code

def ts_to_yf(ts_code: str) -> str:
    """600519.SH → 600519.SS，其余不变"""
    return ts_code[:-3] + ".SS" if ts_code.endswith(".SH") else ts_code

def is_cn(code: str) -> bool:
    """是否 A 股代码（.SS / .SZ）"""
    return code.endswith(".SS") or code.endswith(".SZ")


# ── 行情数据 ─────────────────────────────────────────────────────
def fetch_daily_tushare(yf_code: str, days: int = 400) -> pd.DataFrame | None:
    """
    用 Tushare 拉取 A 股日线 OHLCV，返回标准 DataFrame（与 yfinance 列名一致）。
    yf_code: 600519.SS 或 000858.SZ
    """
    if not is_cn(yf_code):
        return None
    pro = get_pro()
    if pro is None:
        return None
    try:
        ts_code = yf_to_ts(yf_code)
        end_d   = datetime.now().strftime("%Y%m%d")
        start_d = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        df = pro.daily(ts_code=ts_code, start_date=start_d, end_date=end_d,
                       fields="trade_date,open,high,low,close,vol")
        if df is None or len(df) < 5:
            return None
        df = df.sort_values("trade_date").reset_index(drop=True)
        df.index = pd.to_datetime(df["trade_date"], format="%Y%m%d")
        df = df.rename(columns={"open": "Open", "high": "High", "low": "Low",
                                 "close": "Close", "vol": "Volume"})
        return df[["Open", "High", "Low", "Close", "Volume"]].astype(float)
    except Exception as e:
        log.debug(f"Tushare daily {yf_code}: {e}")
        return None


def fetch_df(yf_code: str, period: str = "1y", timeout: int = 10) -> pd.DataFrame | None:
    """
    统一数据获取接口（全市场）：
    - A 股：优先 Tushare → 降级 yfinance
    - 美股/港股：直接 yfinance
    period: yfinance 格式（1y/6mo/2y/350d…），Tushare 自动换算为天数
    """
    # 换算 period → days
    _period_days = {
        "1d": 3, "5d": 7, "1mo": 35, "3mo": 95,
        "6mo": 185, "1y": 370, "2y": 740, "5y": 1850,
    }
    days = _period_days.get(period, 400)
    if period.endswith("d"):
        try:
            days = int(period[:-1]) + 10
        except ValueError:
            pass

    # A 股：Tushare 优先
    if is_cn(yf_code):
        df = fetch_daily_tushare(yf_code, days=days)
        if df is not None and len(df) >= 5:
            return df
        log.debug(f"Tushare 失败，降级 yfinance: {yf_code}")

    # 其他 / Tushare 失败：yfinance
    try:
        import yfinance as yf
        ticker = yf.Ticker(yf_code)
        df = ticker.history(period=period, timeout=timeout)
        if df is None or df.empty:
            return None
        # 兼容新版 yfinance MultiIndex 列
        if hasattr(df.columns, "levels") and df.columns.nlevels == 2:
            df.columns = [c[0] for c in df.columns]
        return df
    except Exception as e:
        log.debug(f"yfinance {yf_code}: {e}")
        return None


def fetch_latest_price(yf_code: str) -> dict | None:
    """
    获取最新价 + 涨跌幅，返回 {price, change_pct, prev_close}
    A 股优先 Tushare，其余 yfinance
    """
    if is_cn(yf_code):
        pro = get_pro()
        if pro:
            try:
                ts_code = yf_to_ts(yf_code)
                df = pro.daily(ts_code=ts_code, fields="trade_date,close,pct_chg,pre_close")
                if df is not None and len(df) >= 1:
                    row = df.iloc[0]
                    return {
                        "price":      float(row["close"]),
                        "change_pct": float(row["pct_chg"]),
                        "prev_close": float(row["pre_close"]),
                    }
            except Exception as e:
                log.debug(f"Tushare latest {yf_code}: {e}")

    # yfinance fallback
    try:
        import yfinance as yf
        df = yf.Ticker(yf_code).history(period="5d", timeout=10)
        if df is None or len(df) < 2:
            return None
        price  = float(df["Close"].iloc[-1])
        prev   = float(df["Close"].iloc[-2])
        change = (price - prev) / prev * 100 if prev else 0
        return {"price": price, "change_pct": change, "prev_close": prev}
    except Exception:
        return None


# ── 股票池 ───────────────────────────────────────────────────────
def fetch_cn_stock_pool(limit: int = 300) -> list:
    """
    获取 A 股股票池（主板+中小板+创业板+科创板）
    返回 [(code6, name, yf_code), ...]
    """
    pro = get_pro()
    if pro is None:
        return []
    try:
        df = pro.stock_basic(exchange="", list_status="L",
                             fields="ts_code,name,market")
        if df is None or len(df) == 0:
            return []
        df = df[df["market"].isin(["主板", "中小板", "创业板", "科创板"])]
        pool = []
        for _, row in df.iterrows():
            ts_code = str(row["ts_code"])
            name    = str(row["name"])
            yf_code = ts_to_yf(ts_code)
            pool.append((ts_code[:6], name, yf_code))
        log.info(f"Tushare CN 股票池: {len(pool)} 只，返回前 {limit} 只")
        return pool[:limit]
    except Exception as e:
        log.warning(f"Tushare CN 股票池失败: {e}")
        return []
