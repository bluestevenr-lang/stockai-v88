"""V100数据获取层：直接抄V88的东财直连方案，绕过Clash TUN"""
import os
import logging
import urllib3
import requests
import pandas as pd
from typing import Optional
from . import cache

urllib3.disable_warnings()
log = logging.getLogger(__name__)

# 关键：trust_env=False + 手动指定Clash端口（V88同款）
_PROXY_ADDR = "127.0.0.1:7897"
_DIRECT_SESSION = requests.Session()
_DIRECT_SESSION.trust_env = False
_DIRECT_SESSION.verify = False
_DIRECT_SESSION.proxies = {
    "http": f"http://{_PROXY_ADDR}",
    "https": f"http://{_PROXY_ADDR}",
}

_EM_BASE = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
_REQUIRED_COLS = ["Open", "High", "Low", "Close", "Volume"]


def _resolve_secid(symbol: str) -> Optional[str]:
    """根据股票代码生成东财secid"""
    s = symbol.upper()
    if s.endswith(".SS"):
        return f"1.{s.replace('.SS', '')}"
    if s.endswith(".SZ"):
        return f"0.{s.replace('.SZ', '')}"
    if s.endswith(".HK"):
        code = s.replace(".HK", "").zfill(5)
        return f"116.{code}"
    # 美股：尝试 105/106/107 三个市场
    for mkt in ["105", "106", "107"]:
        test_secid = f"{mkt}.{s}"
        try:
            r = _DIRECT_SESSION.get(
                _EM_BASE,
                params={
                    "secid": test_secid,
                    "fields1": "f1,f3",
                    "fields2": "f51,f52",
                    "klt": "101",
                    "fqt": "1",
                    "end": "20500101",
                    "lmt": "1",
                },
                timeout=5,
            )
            if r.status_code == 200:
                d = r.json()
                if d.get("data") and d["data"].get("klines"):
                    return test_secid
        except Exception:
            continue
    return None


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    for col in _REQUIRED_COLS:
        if col not in df.columns:
            df[col] = float("nan")
    return df[_REQUIRED_COLS].dropna(subset=["Close"])


def _period_to_lmt(period: str) -> str:
    return {"1mo": "30", "3mo": "90", "6mo": "130",
            "1y": "252", "2y": "504"}.get(period, "252")


def _fetch_eastmoney(symbol: str, period: str) -> Optional[pd.DataFrame]:
    """东财v8 API直连（V100主力数据源）"""
    secid = _resolve_secid(symbol)
    if not secid:
        log.error(f"resolve secid failed: {symbol}")
        return None
    is_index = secid.startswith("100.") or secid.startswith("124.")
    fqt = "0" if is_index else "1"
    try:
        r = _DIRECT_SESSION.get(
            _EM_BASE,
            params={
                "secid": secid,
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
                "klt": "101",
                "fqt": fqt,
                "end": "20500101",
                "lmt": _period_to_lmt(period),
            },
            timeout=8,
        )
        if r.status_code != 200:
            log.error(f"eastmoney HTTP {r.status_code}: {symbol}")
            return None
        data = r.json()
        if not data.get("data") or not data["data"].get("klines"):
            log.error(f"eastmoney empty data: {symbol}")
            return None
        rows = []
        for line in data["data"]["klines"]:
            parts = line.split(",")
            if len(parts) >= 6:
                rows.append({
                    "Date": parts[0],
                    "Open": float(parts[1]),
                    "Close": float(parts[2]),
                    "High": float(parts[3]),
                    "Low": float(parts[4]),
                    "Volume": float(parts[5]),
                })
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df["Date"] = pd.to_datetime(df["Date"])
        df.set_index("Date", inplace=True)
        log.info(f"eastmoney OK: {symbol} -> {secid} ({len(df)} rows)")
        return _normalize(df)
    except Exception as e:
        log.error(f"eastmoney failed ({symbol}): {e}")
        return None


def fetch(code: str, period: str = "1y") -> Optional[pd.DataFrame]:
    cache_key = f"{code}:{period}"
    cached = cache.get("price", cache_key)
    if cached is not None:
        try:
            return pd.read_json(cached, orient="split")
        except Exception:
            pass

    df = _fetch_eastmoney(code, period)

    if df is not None and not df.empty:
        cache.set("price", cache_key, df.to_json(orient="split"))
    return df


def fetch_spot_price(code: str) -> Optional[float]:
    """实时价：拉最新一根日K线的Close"""
    df = _fetch_eastmoney(code, "1mo")
    if df is None or df.empty:
        return None
    return float(df["Close"].iloc[-1])
