"""
ts_helper.py — Tushare A股数据助手（全局共用）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
A股数据优先从 Tushare 获取（全球可用），失败时自动降级 yfinance。
适用于 app_v88_integrated.py / auto_reporter.py / scan_worker.py。

依赖：tushare, pandas, yfinance
Token 来源（优先级）：
    1. 环境变量 TUSHARE_TOKEN
    2. Streamlit st.secrets["TUSHARE_TOKEN"]（可选）

熔断机制：
    连续 API 调用失败超过 _TS_FAIL_THRESHOLD 次后，自动禁用 Tushare，
    后续 A股数据直接走 yfinance，避免每次都浪费时间重试失效的 API。
    _TS_DISABLED_UNTIL 超时后（默认 30 分钟）自动重置，尝试恢复。
"""

import os
import time
import logging
import pandas as pd
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

# ── 熔断器参数 ────────────────────────────────────────────────────
_TS_FAIL_THRESHOLD  = 5      # 连续失败 N 次后熔断
_TS_COOLDOWN_SEC    = 1800   # 熔断冷却时间（30分钟后自动尝试恢复）
_ts_fail_count      = 0      # 连续 API 调用失败计数
_ts_disabled_until  = 0.0    # 熔断解除时间戳（0 = 未熔断）


def _ts_circuit_ok() -> bool:
    """熔断器状态：True = 可用，False = 熔断中"""
    global _ts_fail_count, _ts_disabled_until
    if _ts_disabled_until > 0:
        if time.time() < _ts_disabled_until:
            return False          # 还在冷却期
        else:
            # 冷却期结束，自动恢复试探
            _ts_disabled_until = 0.0
            _ts_fail_count = 0
            log.info("Tushare 熔断冷却结束，恢复试探...")
    return True


def _ts_record_failure():
    """记录一次 API 调用失败，连续失败达阈值时触发熔断"""
    global _ts_fail_count, _ts_disabled_until
    _ts_fail_count += 1
    if _ts_fail_count >= _TS_FAIL_THRESHOLD:
        _ts_disabled_until = time.time() + _TS_COOLDOWN_SEC
        log.warning(f"Tushare 连续失败 {_ts_fail_count} 次，熔断 {_TS_COOLDOWN_SEC//60} 分钟")


def _ts_record_success():
    """记录一次成功，重置失败计数"""
    global _ts_fail_count
    _ts_fail_count = 0


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
    """返回 Tushare pro_api 实例，失败或熔断中返回 None"""
    global _pro, _pro_ok
    # 熔断期间直接跳过
    if not _ts_circuit_ok():
        return None
    if _pro_ok is None:      # 已确认初始化失败，直接返回
        return None
    if _pro is not None:
        return _pro
    token = _read_token()
    if not token:
        _pro_ok = None
        log.info("Tushare Token 未配置，A股将直接使用 yfinance")
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
        _ts_record_failure()
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


# ── 指数代码识别 ─────────────────────────────────────────────────
_INDEX_PREFIXES = ("000001", "000300", "000016", "000905", "399001",
                   "399006", "399300", "399400", "000852", "000688")

def is_index(yf_code: str) -> bool:
    """是否 A 股指数代码（上证/深证/沪深300/创业板等）"""
    code6 = yf_code.split(".")[0]
    return code6 in _INDEX_PREFIXES


# ── 行情数据 ─────────────────────────────────────────────────────
def fetch_daily_tushare(yf_code: str, days: int = 400) -> pd.DataFrame | None:
    """
    用 Tushare 拉取 A 股日线 OHLCV，返回标准 DataFrame（与 yfinance 列名一致）。
    yf_code: 600519.SS / 000858.SZ / 399006.SZ（指数自动用 index_daily）
    Token 到期/配额耗尽时自动触发熔断，后续请求直接走 yfinance。
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
        if is_index(yf_code):
            df = pro.index_daily(ts_code=ts_code, start_date=start_d, end_date=end_d,
                                 fields="trade_date,open,high,low,close,vol")
        else:
            df = pro.daily(ts_code=ts_code, start_date=start_d, end_date=end_d,
                           fields="trade_date,open,high,low,close,vol")
        if df is None or len(df) < 5:
            _ts_record_failure()
            return None
        df = df.sort_values("trade_date").reset_index(drop=True)
        df.index = pd.to_datetime(df["trade_date"], format="%Y%m%d")
        df = df.rename(columns={"open": "Open", "high": "High", "low": "Low",
                                 "close": "Close", "vol": "Volume"})
        result = df[["Open", "High", "Low", "Close", "Volume"]].astype(float)
        _ts_record_success()
        return result
    except Exception as e:
        err_str = str(e).lower()
        # Token 到期 / 权限不足 / 配额耗尽 → 直接熔断（不需要等待 N 次）
        if any(kw in err_str for kw in ("token", "auth", "权限", "过期", "invalid", "403", "401", "limit")):
            log.warning(f"Tushare Token 无效或已到期: {e}，立即熔断")
            global _ts_fail_count, _ts_disabled_until
            _ts_fail_count = _TS_FAIL_THRESHOLD
            _ts_disabled_until = time.time() + _TS_COOLDOWN_SEC
        else:
            _ts_record_failure()
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
    # 新版 yfinance (>=0.2.37) 需要 curl_cffi session，不能传 requests.Session
    for _attempt in range(2):
        try:
            import yfinance as yf
            # 优先 curl_cffi session（新版 yfinance 推荐）
            _sess = None
            try:
                import curl_cffi.requests as _cffi
                _sess = _cffi.Session(impersonate="chrome120")
            except ImportError:
                pass
            ticker = yf.Ticker(yf_code, session=_sess) if _sess else yf.Ticker(yf_code)
            df = ticker.history(period=period, timeout=timeout)
            if df is None or df.empty:
                time.sleep(1.5)
                continue
            # 兼容新版 yfinance MultiIndex 列
            if hasattr(df.columns, "levels") and df.columns.nlevels == 2:
                df.columns = [c[0] for c in df.columns]
            return df
        except Exception as e:
            log.debug(f"yfinance {yf_code} attempt {_attempt+1}: {e}")
            time.sleep(2)
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
    Token 失效时返回空列表，由上层代码触发备用池。
    """
    pro = get_pro()
    if pro is None:
        return []
    try:
        df = pro.stock_basic(exchange="", list_status="L",
                             fields="ts_code,name,market")
        if df is None or len(df) == 0:
            _ts_record_failure()
            return []
        df = df[df["market"].isin(["主板", "中小板", "创业板", "科创板"])]
        pool = []
        for _, row in df.iterrows():
            ts_code = str(row["ts_code"])
            name    = str(row["name"])
            yf_code = ts_to_yf(ts_code)
            pool.append((ts_code[:6], name, yf_code))
        log.info(f"Tushare CN 股票池: {len(pool)} 只，返回前 {limit} 只")
        _ts_record_success()
        return pool[:limit]
    except Exception as e:
        err_str = str(e).lower()
        if any(kw in err_str for kw in ("token", "auth", "权限", "过期", "invalid", "403", "401", "limit")):
            log.warning(f"Tushare Token 无效或已到期（stock_basic）: {e}，立即熔断")
            global _ts_fail_count, _ts_disabled_until
            _ts_fail_count = _TS_FAIL_THRESHOLD
            _ts_disabled_until = time.time() + _TS_COOLDOWN_SEC
        else:
            _ts_record_failure()
        log.warning(f"Tushare CN 股票池失败: {e}")
        return []


def get_tushare_status() -> dict:
    """返回 Tushare 当前状态（供 UI 诊断用）"""
    ok = _ts_circuit_ok() and _pro_ok is True and _pro is not None
    if _ts_disabled_until > time.time():
        remaining = int(_ts_disabled_until - time.time())
        status = f"熔断中（{remaining//60}分{remaining%60}秒后恢复）"
    elif _pro_ok is None:
        status = "未配置或初始化失败"
    elif _pro_ok is False:
        status = "未初始化"
    else:
        status = "正常"
    return {
        "available": ok,
        "status": status,
        "fail_count": _ts_fail_count,
    }
