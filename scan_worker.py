#!/usr/bin/env python3
"""
scan_worker.py — 后台全策略扫描进程
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
独立运行（无 Streamlit 依赖），由主 App 通过 subprocess 启动。

功能:
  - 扫描三市场（美/港/A股）× 四策略（趋势/蓄势/拐点/启动）
  - 8 线程并发拉取数据，每市场约 2-3 分钟
  - 每 20 只写一次进度到 scan_progress.json
  - 每 20 只检查一次 scan_heartbeat.json；
    若心跳超过 90 秒未更新（页面关闭），自动退出
  - 结果写入 scan_results.json，有效期 4 小时

使用:
  python scan_worker.py                  # 正常启动
  python scan_worker.py --force          # 忽略现有结果，强制重扫
"""

import os
import sys
import json
import time
import signal
import logging
import warnings
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import yfinance as yf
import requests

warnings.filterwarnings("ignore")

# ── 添加项目路径（用于 sector_map / modules）────────────────────
_SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(_SCRIPT_DIR))

# ── 文件路径 ────────────────────────────────────────────────────
CACHE_DIR       = _SCRIPT_DIR / ".cache_brief"
RESULTS_FILE    = CACHE_DIR / "scan_results.json"
PROGRESS_FILE   = CACHE_DIR / "scan_progress.json"
HEARTBEAT_FILE  = CACHE_DIR / "scan_heartbeat.json"
PID_FILE        = CACHE_DIR / "scan_worker.pid"
POOL_CACHE_FILE = CACHE_DIR / "pool_cache.json"

# ── 参数 ────────────────────────────────────────────────────────
SCAN_TTL          = 4 * 3600   # 结果有效期 4 小时
HEARTBEAT_TIMEOUT = 90          # 心跳超时（秒）
MAX_WORKERS       = 8           # 并发线程数
HEARTBEAT_CHECK_INTERVAL = 20   # 每 N 只检查一次心跳

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WORKER] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_FORCE_RESCAN = "--force" in sys.argv


# ═══════════════════════════════════════════════════════════════
# 文件 I/O 工具
# ═══════════════════════════════════════════════════════════════

def _heartbeat_alive() -> bool:
    """主页面心跳是否存活（< 90s）"""
    try:
        data = json.loads(HEARTBEAT_FILE.read_text(encoding="utf-8"))
        age = time.time() - data.get("ts", 0)
        return age < HEARTBEAT_TIMEOUT
    except Exception:
        return True  # 文件不存在时宽松处理（刚启动阶段）


def _results_valid() -> bool:
    """结果文件是否仍在有效期内"""
    try:
        data = json.loads(RESULTS_FILE.read_text(encoding="utf-8"))
        age = time.time() - data.get("timestamp", 0)
        return age < SCAN_TTL
    except Exception:
        return False


def _write_progress(pct: int, status: str, detail: str = ""):
    """写进度文件（status: running / done / aborted / error）"""
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        PROGRESS_FILE.write_text(
            json.dumps({"pct": pct, "status": status, "detail": detail, "ts": time.time()},
                       ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════
# 代码转换工具
# ═══════════════════════════════════════════════════════════════

def _to_yf_code(code: str) -> str:
    """将原始代码转换为 yfinance 格式"""
    if not code:
        return code
    code = code.strip().upper()
    if code.endswith(".SH"):
        return code[:-3] + ".SS"
    if "." in code:
        if code.endswith(".HK"):
            try:
                num = int(code.split(".")[0])
                return f"{num}.HK"
            except Exception:
                pass
        return code
    if code.isdigit():
        if len(code) == 6:
            return f"{code}.SS" if code.startswith("6") else f"{code}.SZ"
        if len(code) in (4, 5):
            try:
                num = int(code)
                return f"{num}.HK"
            except Exception:
                pass
    return code


# ═══════════════════════════════════════════════════════════════
# 股票池获取
# ═══════════════════════════════════════════════════════════════

def _fetch_eastmoney(market: str, limit: int) -> list:
    """从东方财富拉取股票池"""
    url = "http://80.push2.eastmoney.com/api/qt/clist/get"
    fs_map = {
        "us": "m:105,m:106,m:107",
        "hk": "m:128",
        "cn": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
    }
    fs = fs_map.get(market)
    if not fs:
        return []
    all_stocks = []
    pn = 1
    while len(all_stocks) < limit:
        try:
            time.sleep(0.5)
            pz = min(200, limit - len(all_stocks))
            params = {
                "pn": pn, "pz": pz, "fs": fs,
                "fields": "f12,f14,f20",
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            }
            resp = requests.get(url, params=params, timeout=10, verify=False)
            diff = resp.json().get("data", {}).get("diff", [])
            if isinstance(diff, dict):
                diff = list(diff.values())
            page = [
                (x["f12"], x["f14"], _to_yf_code(x["f12"]))
                for x in diff
                if isinstance(x, dict) and x.get("f12") and x.get("f14")
            ]
            if not page:
                break
            all_stocks.extend(page)
            if len(page) < pz:
                break
            pn += 1
        except Exception as e:
            log.warning(f"EastMoney {market} page {pn} 失败: {e}")
            break
    return all_stocks[:limit]


def _fallback_us():
    return [
        ("AAPL","苹果","AAPL"),("MSFT","微软","MSFT"),("GOOGL","谷歌","GOOGL"),
        ("AMZN","亚马逊","AMZN"),("META","Meta","META"),("NVDA","英伟达","NVDA"),
        ("TSLA","特斯拉","TSLA"),("NFLX","奈飞","NFLX"),("JPM","摩根大通","JPM"),
        ("V","Visa","V"),("MA","万事达","MA"),("UNH","联合健康","UNH"),
        ("BABA","阿里巴巴","BABA"),("BIDU","百度","BIDU"),("PDD","拼多多","PDD"),
        ("JD","京东","JD"),("NIO","蔚来","NIO"),("PLTR","Palantir","PLTR"),
        ("CRWD","CrowdStrike","CRWD"),("DDOG","Datadog","DDOG"),
    ]


def _fallback_hk():
    return [
        ("0700","腾讯控股","700.HK"),("0941","中国移动","941.HK"),
        ("1299","友邦保险","1299.HK"),("0005","汇丰控股","5.HK"),
        ("0388","香港交易所","388.HK"),("1398","工商银行","1398.HK"),
        ("3690","美团","3690.HK"),("9988","阿里巴巴","9988.HK"),
        ("0883","中国海洋石油","883.HK"),("1810","小米集团","1810.HK"),
    ]


def _fallback_cn():
    return [
        ("600519","贵州茅台","600519.SS"),("000858","五粮液","000858.SZ"),
        ("601318","中国平安","601318.SS"),("600036","招商银行","600036.SS"),
        ("000001","平安银行","000001.SZ"),("300750","宁德时代","300750.SZ"),
        ("601888","中国中免","601888.SS"),("002594","比亚迪","002594.SZ"),
    ]


def _load_pool_or_fetch() -> tuple:
    """尝试从缓存文件读取池，否则从东财拉取 → (us, hk, cn)"""
    try:
        if POOL_CACHE_FILE.exists():
            data = json.loads(POOL_CACHE_FILE.read_text(encoding="utf-8"))
            if time.time() - data.get("ts", 0) < 6 * 3600:
                us = [tuple(x) for x in data["US"]]
                hk = [tuple(x) for x in data["HK"]]
                cn = [tuple(x) for x in data["CN"]]
                log.info(f"池缓存命中: US={len(us)} HK={len(hk)} CN={len(cn)}")
                return us, hk, cn
    except Exception:
        pass

    log.info("从东财拉取股票池...")
    _write_progress(2, "running", "正在拉取股票池...")
    us = _fetch_eastmoney("us", 350) or _fallback_us()
    hk = _fetch_eastmoney("hk", 200) or _fallback_hk()
    cn = _fetch_eastmoney("cn", 250) or _fallback_cn()
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        POOL_CACHE_FILE.write_text(
            json.dumps({"ts": time.time(), "US": us, "HK": hk, "CN": cn}, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        pass
    log.info(f"股票池获取完成: US={len(us)} HK={len(hk)} CN={len(cn)}")
    return us, hk, cn


# ═══════════════════════════════════════════════════════════════
# 数据获取
# ═══════════════════════════════════════════════════════════════

def _fetch_df(yf_code: str):
    """拉取最近 350 天日线数据"""
    try:
        df = yf.download(yf_code, period="350d", progress=False, auto_adjust=True)
        if df is None or len(df) < 30:
            return None
        # 处理 yfinance ≥ 0.2 的 MultiIndex columns
        if hasattr(df.columns, "levels"):
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
        # 确保必要列存在
        for col in ("Close", "Open", "High", "Low", "Volume"):
            if col not in df.columns:
                return None
        return df
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════
# 评分函数（从 app_v88_integrated.py 复制，已移除 Streamlit 依赖）
# ═══════════════════════════════════════════════════════════════

def _score_top(df) -> dict | None:
    """趋势强势：均线系统 + RSI + 动量 + 成交量"""
    if df is None or len(df) < 50:
        return None
    try:
        close  = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        high   = df["High"].astype(float)
        last_c = float(close.iloc[-1])

        ma20  = float(close.rolling(20).mean().iloc[-1])
        ma50  = float(close.rolling(50).mean().iloc[-1])
        ma200 = float(close.rolling(200).mean().iloc[-1]) if len(df) >= 200 else 0

        delta = close.diff()
        gain  = delta.where(delta > 0, 0).fillna(0)
        loss  = (-delta.where(delta < 0, 0)).fillna(0)
        rsi   = float(100 - 100 / (1 + gain.ewm(com=13).mean().iloc[-1] /
                                   (loss.ewm(com=13).mean().iloc[-1] + 1e-10)))

        ret20 = float(close.iloc[-1] / close.iloc[-21] - 1) * 100 if len(close) >= 21 else 0
        ret60 = float(close.iloc[-1] / close.iloc[-61] - 1) * 100 if len(close) >= 61 else 0

        avg_v20   = float(volume.tail(20).mean())
        last_v    = float(volume.iloc[-1])
        vol_surge = last_v > avg_v20 * 1.2

        # 52 周水位（距高点）
        h52w   = float(high.tail(252).max()) if len(df) >= 252 else float(high.max())
        dist_h = (last_c / h52w - 1) * 100 if h52w > 0 else 0

        score = 0
        signals = []
        if last_c > ma20:                  score += 20; signals.append("✅ 站MA20")
        if last_c > ma50:                  score += 15; signals.append("✅ 站MA50")
        if ma200 > 0 and last_c > ma200:   score += 15; signals.append("🏔 站MA200")
        if 50 < rsi < 75:                  score += 15; signals.append(f"RSI{rsi:.0f}")
        if ret20 > 5:                      score += 15; signals.append(f"20日+{ret20:.1f}%")
        if ret60 > 10:                     score += 10; signals.append(f"60日+{ret60:.1f}%")
        if vol_surge:                      score += 10; signals.append("🔥 放量")

        if score < 40:
            return None
        setup = "强势" if score >= 70 else "偏强"
        return {"score": min(100, score), "signals": signals, "setup": setup,
                "dist_h": dist_h, "rsi": rsi}
    except Exception:
        return None


def _score_coil(df) -> dict | None:
    """蓄势潜伏：量缩价稳 + ATR 收缩 + 贴近均线"""
    if df is None or len(df) < 60:
        return None
    try:
        close  = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        high   = df["High"].astype(float)
        low    = df["Low"].astype(float)

        ma20  = close.rolling(20).mean()
        ma50  = close.rolling(50).mean()
        ma200 = close.rolling(200).mean() if len(df) >= 200 else None

        last_c  = float(close.iloc[-1])
        avg_v60 = float(volume.tail(60).mean())
        atr10   = float((high - low).tail(10).mean())
        atr60   = float((high - low).tail(60).mean())

        atr_contracting = atr10 < atr60 * 0.70
        vol_drying      = float(volume.tail(10).mean()) < avg_v60 * 0.75

        near_ma20    = abs(last_c / float(ma20.iloc[-1]) - 1) < 0.03
        ma20_flat_up = float(ma20.iloc[-1]) >= float(ma20.iloc[-5]) if len(ma20) >= 5 else False
        above_ma50   = last_c > float(ma50.iloc[-1])
        above_ma200  = ma200 is not None and last_c > float(ma200.iloc[-1])

        range60 = float(high.tail(60).max() - low.tail(60).min())
        range20 = float(high.tail(20).max() - low.tail(20).min())
        range_contracting = range20 < range60 * 0.60 if range60 > 0 else False

        h60        = float(high.tail(60).max())
        price_zone = h60 * 0.75 <= last_c <= h60 * 0.95 if h60 > 0 else False

        score = 0
        signals = []
        if atr_contracting:              score += 20; signals.append("🔇 波动收缩")
        if vol_drying:                   score += 20; signals.append("📉 量能萎缩")
        if near_ma20 and ma20_flat_up:   score += 15; signals.append("📐 贴近MA20")
        if above_ma50:                   score += 15; signals.append("✅ 站上MA50")
        if above_ma200:                  score += 15; signals.append("🏔 站上MA200")
        if range_contracting:            score += 10; signals.append("🎯 区间收窄")
        if price_zone:                   score += 5;  signals.append("📍 蓄势区")

        setup = "强蓄势" if score >= 70 else ("蓄势中" if score >= 45 else "弱蓄势")
        return {"score": min(100, score), "signals": signals, "setup": setup}
    except Exception:
        return None


def _score_inflection(df) -> dict | None:
    """拐点通道（赔率）：三关全中才入池"""
    if df is None or len(df) < 40:
        return None
    try:
        close  = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        high   = df["High"].astype(float)
        low    = df["Low"].astype(float)

        delta = close.diff()
        gain  = delta.where(delta > 0, 0).fillna(0)
        loss  = (-delta.where(delta < 0, 0)).fillna(0)
        rsi   = float(100 - 100 / (1 + gain.ewm(com=13).mean().iloc[-1] /
                                   (loss.ewm(com=13).mean().iloc[-1] + 1e-10)))

        last_c = float(close.iloc[-1])
        period = min(126, len(df))
        h6m    = float(high.tail(period).max())
        l6m    = float(low.tail(period).min())
        range6m = h6m - l6m
        pos6m   = (last_c - l6m) / range6m if range6m > 0 else 0.5
        in_bottom_40 = pos6m <= 0.40

        ret5  = float(close.iloc[-1] / close.iloc[-6]  - 1) * 100 if len(close) >= 6  else 0
        ret20 = float(close.iloc[-1] / close.iloc[-21] - 1) * 100 if len(close) >= 21 else 0

        rsi_divergence = (float(close.tail(20).min()) <= last_c * 1.02) and (rsi > 40)
        rebound_signal = (ret5 > 0) and (ret20 < -5)
        gate1 = in_bottom_40 and (rsi_divergence or rebound_signal)

        if len(low) >= 20:
            higher_lows = float(low.iloc[-10:].min()) > float(low.iloc[-20:-10].min())
        else:
            higher_lows = False
        not_new_low = last_c > float(close.tail(20).min()) * 0.99
        gate2 = higher_lows and not_new_low

        recent_10  = df.tail(10).copy()
        up_days    = recent_10[recent_10["Close"] >= recent_10["Open"]]
        down_days  = recent_10[recent_10["Close"] <  recent_10["Open"]]
        avg_vol_up   = float(up_days["Volume"].mean())   if len(up_days)   > 0 else 0
        avg_vol_down = float(down_days["Volume"].mean()) if len(down_days) > 0 else 1
        gate3 = avg_vol_up > avg_vol_down

        if not (gate1 and gate2 and gate3):
            return None

        score = 0
        signals = []
        bottom_score = int((0.40 - pos6m) / 0.40 * 30) if pos6m <= 0.40 else 0
        score += bottom_score
        signals.append(f"📍 底部{pos6m*100:.0f}%位")

        if rebound_signal:  score += 20; signals.append(f"↩️ 5日+{ret5:.1f}%")
        if rsi_divergence:  score += 15; signals.append(f"📈 RSI底背离{rsi:.0f}")
        if higher_lows:     score += 20; signals.append("🔼 高低点抬升")
        vol_ratio = avg_vol_up / avg_vol_down if avg_vol_down > 0 else 1
        score += min(15, int(vol_ratio * 5))
        signals.append(f"💰 买/卖量={vol_ratio:.1f}x")

        setup = "强拐点" if score >= 65 else ("拐点中" if score >= 45 else "弱拐点")
        return {
            "score": min(100, score), "signals": signals, "setup": setup,
            "pos6m": pos6m, "ret5": ret5, "ret20": ret20, "rsi": rsi, "gate3": gate3,
        }
    except Exception:
        return None


def _score_breakout_v2(df, bm_ret5: float = 0.0) -> dict | None:
    """启动通道（胜率）：三信号满足 ≥ 2/3 才入池"""
    if df is None or len(df) < 25:
        return None
    try:
        close  = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        high   = df["High"].astype(float)
        low    = df["Low"].astype(float)

        last_c  = float(close.iloc[-1])
        last_v  = float(volume.iloc[-1])
        avg_v20 = float(volume.tail(20).mean())

        delta = close.diff()
        gain  = delta.where(delta > 0, 0).fillna(0)
        loss  = (-delta.where(delta < 0, 0)).fillna(0)
        rsi   = float(100 - 100 / (1 + gain.ewm(com=13).mean().iloc[-1] /
                                   (loss.ewm(com=13).mean().iloc[-1] + 1e-10)))

        high20_prev = float(close.iloc[-21:-1].max()) if len(close) >= 21 else float(close.iloc[:-1].max())
        s1_breakout = last_c > high20_prev
        s1_margin   = (last_c / high20_prev - 1) * 100 if high20_prev > 0 else 0

        s2_volume = last_v > avg_v20 * 1.5
        s2_ratio  = last_v / avg_v20 if avg_v20 > 0 else 1

        ret5  = float((close.iloc[-1] / close.iloc[-6] - 1) * 100) if len(close) >= 6 else 0
        s3_rs = ret5 > bm_ret5 + 2.0

        met = sum([s1_breakout, s2_volume, s3_rs])
        if met < 2:
            return None

        daily_range  = float(high.iloc[-1] - low.iloc[-1])
        strong_close = ((last_c - float(low.iloc[-1])) / daily_range > 0.70) if daily_range > 0 else False
        rsi_ok = 50 <= rsi <= 78

        score = 0
        signals = []
        if s1_breakout:   score += 35; signals.append(f"🚀 突破+{s1_margin:.1f}%")
        if s2_volume:     score += 30; signals.append(f"🔥 量{s2_ratio:.1f}x")
        if s3_rs:         score += 25; signals.append(f"💪 RS+{ret5-bm_ret5:.1f}%")
        if strong_close:  score += 5;  signals.append("⬆️ 强收盘")
        if rsi_ok:        score += 5;  signals.append(f"RSI{rsi:.0f}")

        setup = "强启动" if score >= 70 else ("启动中" if score >= 50 else "弱启动")
        return {
            "score": min(100, score), "signals": signals, "setup": setup,
            "s1": s1_breakout, "s2": s2_volume, "s3": s3_rs,
            "met": met, "ret5": ret5, "rsi": rsi,
        }
    except Exception:
        return None


def _gen_rationale(df, channel: str, result: dict) -> str:
    """生成一行理由：变量 → 预期差 → 价格位置 → 验证窗口"""
    try:
        close  = df["Close"].astype(float)
        last_c = float(close.iloc[-1])
        ma20   = float(close.rolling(20).mean().iloc[-1])
        h52w   = float(df["High"].tail(252).max()) if len(df) >= 252 else float(df["High"].max())
        dist_h = (last_c / h52w - 1) * 100 if h52w > 0 else 0

        if channel == "INFLECTION":
            pos6m = result.get("pos6m", 0.5)
            ret5  = result.get("ret5", 0)
            rsi   = result.get("rsi", 50)
            var_part = "量能回升+低点抬高" if result.get("gate3") else "结构企稳"
            exp_part = (f"市场仍恐慌RSI{rsi:.0f}，买量已占优"
                        if rsi < 45 else f"底部{pos6m*100:.0f}%位反弹{ret5:+.1f}%")
            return (f"变量:{var_part} → 预期差:{exp_part} → "
                    f"价格:{last_c:.2f}距MA20{(last_c/ma20-1)*100:+.1f}% → 验证:3-5日站MA20")
        else:
            ret5 = result.get("ret5", 0)
            rsi  = result.get("rsi", 60)
            met  = result.get("met", 2)
            sig_n = "三信号共振" if met == 3 else "双信号确认"
            exp_part = f"未追距52W高{dist_h:.1f}%" if dist_h < -5 else "历史高位突破"
            return (f"变量:{sig_n}放量突破 → 预期差:{exp_part} → "
                    f"价格:{last_c:.2f} RSI{rsi:.0f} → 验证:48h维持突破位")
    except Exception:
        return "计算中"


def _get_bm_return(ticker: str, days: int = 5) -> float:
    """拉取基准指数 N 日收益率"""
    try:
        df = yf.download(ticker, period="30d", progress=False, auto_adjust=True)
        if df is None or len(df) < days + 1:
            return 0.0
        closes = df["Close"].dropna()
        if len(closes) < days + 1:
            return 0.0
        return float((closes.iloc[-1] / closes.iloc[-(days + 1)] - 1) * 100)
    except Exception:
        return 0.0


# ═══════════════════════════════════════════════════════════════
# 单股评分（给 ThreadPoolExecutor 调用）
# ═══════════════════════════════════════════════════════════════

def _score_one_stock(args: tuple):
    """拉取数据并运行四策略评分，返回结果 dict 或 None"""
    item, bm_ret5, sector_fn = args
    code    = item[0]
    name    = item[1] if len(item) > 1 else code
    yf_code = item[2] if len(item) > 2 else code

    df = _fetch_df(yf_code)
    if df is None or len(df) < 30:
        return None
    try:
        price = float(df["Close"].iloc[-1])
        if not (0 < price < 1_000_000):
            return None
    except Exception:
        return None

    sector    = sector_fn(code, name) if sector_fn else "其他"
    price_str = f"{price:.2f}"

    out = {}

    top_r  = _score_top(df)
    coil_r = _score_coil(df)
    inf_r  = _score_inflection(df)
    bo_r   = _score_breakout_v2(df, bm_ret5)

    base = {"股票": name, "代码": code, "行业": sector, "现价": price_str}

    if top_r:
        out["top"] = {**base,
                      "得分": top_r["score"], "形态": top_r["setup"],
                      "信号": " ".join(top_r["signals"][:3]),
                      "理由": f"趋势多头·{top_r['setup']}",
                      "建议": top_r["setup"]}

    if coil_r and coil_r["score"] >= 45:
        out["coil"] = {**base,
                       "得分": coil_r["score"], "形态": coil_r["setup"],
                       "信号": " ".join(coil_r["signals"][:3]),
                       "理由": f"蓄势待发·{coil_r['setup']}",
                       "建议": coil_r["setup"]}

    if inf_r:
        out["inflection"] = {**base,
                             "得分": inf_r["score"], "形态": inf_r["setup"],
                             "信号": " ".join(inf_r["signals"][:3]),
                             "理由": _gen_rationale(df, "INFLECTION", inf_r),
                             "建议": inf_r["setup"]}

    if bo_r:
        out["breakout"] = {**base,
                           "得分": bo_r["score"], "形态": bo_r["setup"],
                           "信号": " ".join(bo_r["signals"][:3]),
                           "理由": _gen_rationale(df, "BREAKOUT", bo_r),
                           "建议": bo_r["setup"]}

    return out if out else None


# ═══════════════════════════════════════════════════════════════
# 单市场扫描
# ═══════════════════════════════════════════════════════════════

def _scan_market(pool: list, market_key: str, bm_ticker: str,
                 pct_start: int, pct_end: int) -> dict | None:
    """扫描单一市场，返回四策略 Top 列表，或心跳超时时返回 None"""
    bm_ret5 = _get_bm_return(bm_ticker)
    log.info(f"[{market_key}] 扫描 {len(pool)} 只  基准5日 {bm_ret5:+.2f}%")

    sector_fn = None
    try:
        from modules.sector_map import get_sector  # noqa: PLC0415
        sector_fn = get_sector
    except Exception:
        pass

    top_pool = []
    coil_pool = []
    inf_pool  = []
    bo_pool   = []

    args_list  = [(item, bm_ret5, sector_fn) for item in pool]
    total      = len(pool)
    done_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(_score_one_stock, a): i for i, a in enumerate(args_list)}
        for fut in as_completed(futures):
            done_count += 1

            if done_count % HEARTBEAT_CHECK_INTERVAL == 0 or done_count == total:
                # 写进度
                pct = int(pct_start + (pct_end - pct_start) * done_count / total)
                _write_progress(pct, "running",
                                f"[{market_key}] {done_count}/{total} 只...")
                # 心跳检查
                if not _heartbeat_alive():
                    log.warning(f"[{market_key}] 心跳超时，取消扫描")
                    ex.shutdown(wait=False, cancel_futures=True)
                    return None

            result = fut.result()
            if result is None:
                continue
            if "top"        in result: top_pool.append(result["top"])
            if "coil"       in result: coil_pool.append(result["coil"])
            if "inflection" in result: inf_pool.append(result["inflection"])
            if "breakout"   in result: bo_pool.append(result["breakout"])

    top30  = sorted(top_pool,  key=lambda x: x["得分"], reverse=True)[:30]
    coil30 = sorted(coil_pool, key=lambda x: x["得分"], reverse=True)[:30]
    inf10  = sorted(inf_pool,  key=lambda x: x["得分"], reverse=True)[:10]
    bo10   = sorted(bo_pool,   key=lambda x: x["得分"], reverse=True)[:10]

    log.info(f"[{market_key}] 完成 趋势={len(top30)} 蓄势={len(coil30)} "
             f"拐点={len(inf10)} 启动={len(bo10)}")
    return {"top": top30, "coil": coil30, "inflection": inf10,
            "breakout": bo10, "bm_ret5": bm_ret5}


# ═══════════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════════

def main():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    log.info("scan_worker 启动")

    # 结果仍然有效且不强制重扫时直接退出
    if not _FORCE_RESCAN and _results_valid():
        log.info("结果仍在有效期（4h），无需重扫")
        _write_progress(100, "done", "已有有效缓存")
        return

    # 检查是否已有另一个进程在跑
    if PID_FILE.exists():
        try:
            old_pid = int(PID_FILE.read_text().strip())
            if old_pid != os.getpid():
                os.kill(old_pid, 0)     # 不抛说明进程存活
                log.info(f"进程 {old_pid} 已在运行，退出")
                return
        except (ProcessLookupError, ValueError):
            pass   # 旧进程已死，继续
        except Exception:
            pass

    # 写入 PID
    PID_FILE.write_text(str(os.getpid()), encoding="utf-8")

    # 信号处理：清理 PID 文件
    def _cleanup(signum=None, frame=None):
        PID_FILE.unlink(missing_ok=True)
        _write_progress(0, "aborted", "进程被中断")
        log.info("scan_worker 退出（信号中断）")
        sys.exit(0)

    signal.signal(signal.SIGTERM, _cleanup)
    signal.signal(signal.SIGINT,  _cleanup)

    _write_progress(1, "running", "初始化中...")

    try:
        # 获取股票池
        us_pool, hk_pool, cn_pool = _load_pool_or_fetch()
        log.info(f"股票池就绪: US={len(us_pool)} HK={len(hk_pool)} CN={len(cn_pool)}")

        results = {}
        markets = [
            ("US", us_pool, "SPY",        5,  35),
            ("HK", hk_pool, "^HSI",       35, 65),
            ("CN", cn_pool, "000300.SS",  65, 95),
        ]

        for mkt_key, pool, bm_ticker, pct_start, pct_end in markets:
            if not _heartbeat_alive():
                log.warning("心跳超时，终止")
                _cleanup()
                return

            _write_progress(pct_start, "running",
                            f"正在扫描 {mkt_key} ({len(pool)} 只)...")
            mkt_result = _scan_market(pool, mkt_key, bm_ticker, pct_start, pct_end)

            if mkt_result is None:
                _cleanup()
                return

            results[mkt_key] = mkt_result

        # 写入最终结果
        final = {"timestamp": time.time(), **results}
        RESULTS_FILE.write_text(
            json.dumps(final, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        log.info(f"✅ 扫描完成，结果已写入 {RESULTS_FILE}")
        _write_progress(100, "done", "全市场扫描完成 ✅")

    except Exception as e:
        log.error(f"扫描异常: {e}", exc_info=True)
        _write_progress(0, "error", str(e)[:120])
    finally:
        PID_FILE.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
