#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI股市早晚报 - 钉钉自动推送
- 早报（7:00）：V88 AI 日报，侧重港股+A股
- 晚报（19:00）：V88 AI 日报，侧重美股
- Part A: 基本面+新闻（华尔街日报版）独立 Gemini 调用
- Part B: 可执行推荐（9只：美港A各3只）独立 Gemini 调用
- Part C: 自选股持仓分析 独立 Gemini 调用
  ★ 三部分各自独立调用 Gemini，彻底消灭「切割导致内容缩水」问题
"""

import os
import sys
import re
from pathlib import Path

# ── 优先从 .env 文件加载密钥（Python 解析，避免 shell 编码问题）──────────────
def _load_env_file():
    """
    加载 env 文件，优先级规则：
      1. /root/.env.report（VPS 生产）—— 强制覆盖，确保生产配置永远优先
      2. 脚本同目录的 .env（本地开发 / Streamlit）—— 不覆盖已有值
      3. /root/.env（VPS 通用回退）—— 不覆盖已有值
    """
    def _parse(env_path: Path, force: bool = False):
        try:
            with open(env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#') or '=' not in line:
                        continue
                    key, _, val = line.partition('=')
                    key = key.strip()
                    val = val.strip().strip('"').strip("'")
                    if key and val and (force or key not in os.environ):
                        os.environ[key] = val
        except Exception as e:
            print(f"⚠️  .env 加载失败 [{env_path}]: {e}")

    # 1. VPS 生产配置（强制覆盖，最高优先级）
    _parse(Path('/root/.env.report'), force=True)
    # 2. 本地开发 / Streamlit（不覆盖，补充缺失值）
    _parse(Path(__file__).parent / '.env', force=False)
    # 3. VPS 通用回退（不覆盖）
    _parse(Path('/root/.env'), force=False)

_load_env_file()

# ── 诊断：打印关键 env 变量的前几个字符（不暴露完整值）──────────────────────
def _diag_env():
    for k in ('GEMINI_API_KEY', 'TUSHARE_TOKEN', 'DINGTALK_WEBHOOK', 'DINGTALK_SECRET', 'DINGTALK_KEYWORD'):
        v = os.environ.get(k, '')
        if v:
            preview = v[:6] + '...' if len(v) > 6 else v
            is_ascii = all(ord(c) < 128 for c in v)
            print(f"  [{k}] = {repr(preview)}  ascii_safe={is_ascii}")
        else:
            print(f"  [{k}] = (未设置)")
_diag_env()

import logging
import yfinance as yf
try:
    from news_fetcher import (
        fetch_stock_data as _nf_stock,
        fetch_rss        as _nf_rss,
        fetch_newsapi    as _nf_newsapi,
        build_report_data,
        RSS_SOURCES      as _NF_RSS_SOURCES,
        NEWSAPI_TOPICS   as _NF_NEWSAPI_TOPICS,
    )
    _NEWS_FETCHER_OK = True
except ImportError:
    _NEWS_FETCHER_OK = False

logger = logging.getLogger(__name__)

# 每次运行只采集一次，供 Part A / C / D 共享
_REPORT_DATA_CACHE: dict = {}

try:
    from google import genai as genai
    _GENAI_NEW = True
except ImportError:
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning, module="google.generativeai")
    import google.generativeai as genai  # type: ignore
    _GENAI_NEW = False
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import urllib.request
import urllib.parse
import json
import hmac
import hashlib
import base64
import time
import ssl
import pandas as pd

# Tushare A股数据助手（优先于 yfinance）
try:
    sys.path.insert(0, str(Path(__file__).parent))
    from ts_helper import fetch_df as _ts_fetch_df, fetch_latest_price as _ts_price, is_cn as _ts_is_cn
    _TS_AVAILABLE = True
except Exception:
    _TS_AVAILABLE = False
    def _ts_is_cn(c): return c.endswith(".SS") or c.endswith(".SZ")
    def _ts_fetch_df(c, **kw): return None
    def _ts_price(c): return None

import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

# ─── 配置 ───────────────────────────────────────────────────────────────────
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '')
TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")
GEMINI_MODEL = "gemini-2.5-flash"
USE_GOOGLE_SEARCH_GROUNDING = os.environ.get('USE_GOOGLE_SEARCH_GROUNDING', '0') == '1'
DINGTALK_WEBHOOK = os.environ.get('DINGTALK_WEBHOOK', '')
DINGTALK_SECRET = os.environ.get('DINGTALK_SECRET', '')
DINGTALK_KEYWORD = os.environ.get('DINGTALK_KEYWORD', '股票行情')   # 钉钉机器人安全关键词
DINGTALK_MAX_CONTENT_CHARS = 4800
FEISHU_WEBHOOK = os.environ.get('FEISHU_WEBHOOK', '')              # 飞书机器人 Webhook
PART_A_TARGET_CHARS = 5200
PART_BC_MAX_CHARS = 5200
PORTFOLIO_FILE = "my_portfolio.xlsx"

# ─── 自选股（按中美港划分，来自多账户持仓/自选，可编辑）────────────────────
WATCHLIST = {
    "US": [
        ("ABBV", "艾伯维"), ("ACMR", "ACM Research"), ("NVDA", "英伟达"), ("NVO", "诺和诺德"),
        ("VOO", "标普500ETF"), ("BRK-B", "伯克希尔"), ("QQQM", "纳指100ETF"),
        ("GOOG", "谷歌"), ("PM", "菲利普莫里斯"), ("LLY", "礼来制药"), ("TSM", "台积电"),
        ("TSLA", "特斯拉"),
    ],
    "HK": [
        ("0700.HK", "腾讯控股"), ("0883.HK", "中国海洋石油"), ("1299.HK", "友邦保险"),
        ("0941.HK", "中国移动"),
    ],
    "CN": [
        ("600519.SS", "贵州茅台"), ("688981.SS", "中芯国际"), ("601899.SS", "紫金矿业"),
        ("688008.SS", "澜起科技"), ("600941.SS", "中国移动A"), ("000333.SZ", "美的集团"),
        ("000001.SZ", "平安银行"), ("601669.SS", "中国电建"),
    ],
}

# ─── 工具函数 ────────────────────────────────────────────────────────────────

def _to_yf_cn_code(code):
    """与 V88 modules/utils.to_yf_cn_code 同逻辑：支持 .US 去除、BRK.B、东财格式"""
    if not code:
        return code
    code = str(code).upper().strip()
    if code.endswith(".US"):
        code = code[:-3]
    if code == "BRK.B":
        return "BRK-B"
    if code.endswith(".SS") or code.endswith(".SZ") or code.endswith(".HK"):
        return code
    if code.endswith(".SH"):
        return code[:-3] + ".SS"
    if "." in code and not code.endswith(".HK"):
        part = code.split(".")[-1]
        if part.isalpha() and 2 <= len(part) <= 5:
            return part
    if code.isdigit():
        if len(code) == 5 and code.startswith("0"):
            return code[1:] + ".HK"
        if len(code) == 4:
            return code + ".HK"
        if code.startswith("6") or code.startswith("5"):
            return code + ".SS"
        if code.startswith("0") or code.startswith("3"):
            return code + ".SZ"
    return code


def _v88_fetch_price(code):
    try:
        yf_code = _to_yf_cn_code(code) if not code.endswith(".HK") and not code.endswith(".SS") and not code.endswith(".SZ") else code
        # A股优先 Tushare
        if _TS_AVAILABLE and _ts_is_cn(yf_code):
            r = _ts_price(yf_code)
            if r:
                return r["price"]
        df = yf.Ticker(yf_code).history(period="5d", timeout=10)
        if df is not None and len(df) > 0 and "Close" in df.columns:
            return float(df["Close"].iloc[-1])
    except Exception:
        pass
    return None


def _v88_index_change(code, label):
    try:
        # A股优先 Tushare
        if _TS_AVAILABLE and _ts_is_cn(code):
            r = _ts_price(code)
            if r:
                chg = r["change_pct"]
                p   = r["price"]
                return f"{label}: {p:.2f}（涨跌 {chg:+.2f}%）"
        df = yf.Ticker(code).history(period="5d", timeout=10)
        if df is not None and len(df) >= 2:
            last_close = float(df["Close"].iloc[-1])
            prev_close = float(df["Close"].iloc[-2])
            chg = ((last_close - prev_close) / prev_close * 100) if prev_close > 0 else 0
            last_d, prev_d = df.index[-1], df.index[-2]
            last_str = last_d.strftime("%m/%d") if hasattr(last_d, "strftime") else str(last_d)[-5:]
            prev_str = prev_d.strftime("%m/%d") if hasattr(prev_d, "strftime") else str(prev_d)[-5:]
            return f"{label}: {last_close:.2f}（{prev_str}→{last_str} 涨跌 {chg:+.2f}%）"
    except Exception:
        pass
    return f"{label}: 数据获取中"


def _v88_call_gemini(prompt, use_grounding=None):
    """调用 Gemini 生成内容。use_grounding=True 时启用 Google Search 获取实时新闻"""
    if not GEMINI_API_KEY:
        return "❌ 请配置 GEMINI_API_KEY"
    use_grounding = use_grounding if use_grounding is not None else USE_GOOGLE_SEARCH_GROUNDING
    try:
        if _GENAI_NEW:
            # 新版 SDK: google-genai
            client = genai.Client(api_key=GEMINI_API_KEY)
            if use_grounding:
                from google.genai import types as _gt
                tool = _gt.Tool(google_search=_gt.GoogleSearch())
                response = client.models.generate_content(
                    model=GEMINI_MODEL, contents=prompt,
                    config=_gt.GenerateContentConfig(tools=[tool])
                )
                if (hasattr(response, 'candidates') and response.candidates
                        and hasattr(response.candidates[0], 'grounding_metadata')
                        and response.candidates[0].grounding_metadata):
                    print("  📡 已使用 Google Search grounding 获取实时来源")
            else:
                response = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
        else:
            # 旧版 SDK: google-generativeai（兼容回退）
            genai.configure(api_key=GEMINI_API_KEY)
            model = genai.GenerativeModel(GEMINI_MODEL)
            if use_grounding:
                response = model.generate_content(prompt, tools=["google_search_retrieval"])
                if (hasattr(response, 'candidates') and response.candidates
                        and hasattr(response.candidates[0], 'grounding_metadata')
                        and response.candidates[0].grounding_metadata):
                    print("  📡 已使用 Google Search grounding 获取实时来源")
            else:
                response = model.generate_content(prompt)
        if response and response.text:
            return response.text.strip()
        return "❌ Gemini 返回为空"
    except Exception as e:
        return f"❌ Gemini 调用异常: {str(e)}"


def _get_watchlist_prices():
    """获取自选股现价，供 prompt 注入"""
    out = {"US": [], "HK": [], "CN": []}
    for market, stocks in WATCHLIST.items():
        pfx = "$" if market == "US" else ("HK$" if market == "HK" else "¥")
        for code, name in stocks:
            p = _v88_fetch_price(code)
            s = f"{name}({code}): {pfx}{p:.2f}" if p is not None else f"{name}({code}): 数据获取中"
            out[market].append(s)
    return out


def _get_market_status(date_str):
    """根据日期返回美股/港股/A股 休市状态"""
    try:
        import exchange_calendars as xcals
        xnys = xcals.get_calendar("XNYS")
        xhkg = xcals.get_calendar("XHKG")
        xshg = xcals.get_calendar("XSHG")
        us_open = xnys.is_session(date_str)
        hk_open = xhkg.is_session(date_str)
        cn_open = xshg.is_session(date_str)
        return {
            "US": "开市" if us_open else "休市",
            "HK": "开市" if hk_open else "休市",
            "CN": "开市" if cn_open else "休市",
        }
    except Exception as e:
        print(f"  ⚠️  exchange_calendars 获取失败: {e}，AI 将自主判断")
        return None


def _rotate_pool(pool, seed):
    import random
    rng = random.Random(seed)
    lst = list(pool)
    rng.shuffle(lst)
    return lst

# ─── 股票池（800只：US 350 + HK 200 + CN 250）───────────────────────────────

TARGET_POOL_SIZE = {"US": 350, "HK": 200, "CN": 250}
EASTMONEY_PAGE_SIZE = 200
MAX_WSJ_CANDIDATES = 100

# ─── 跨日去重：与 V88 共享同一历史文件 ──────────────────────────────────────
_REPORTER_SCRIPT_DIR = Path(__file__).parent
_BRIEF_CACHE_DIR  = _REPORTER_SCRIPT_DIR / ".cache_brief"
_BRIEF_HISTORY_FILE = _BRIEF_CACHE_DIR / "brief_history.json"

def _reporter_append_history(content: str):
    """从报告内容提取推荐代码，追加到共享历史文件（7天保留）。"""
    codes = re.findall(r'\*\*[^(（]+[（(]([A-Za-z0-9.]+)[)）]\*\*', content)
    if not codes:
        return
    try:
        _BRIEF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        history = []
        if _BRIEF_HISTORY_FILE.exists():
            history = json.loads(_BRIEF_HISTORY_FILE.read_text(encoding="utf-8"))
        cutoff = time.time() - 7 * 86400
        history = [r for r in history if r.get("ts", 0) > cutoff]
        today_str = datetime.now(TZ_SHANGHAI).strftime("%Y-%m-%d")
        history.append({"date": today_str, "ts": time.time(),
                        "codes": list(set(codes)), "src": "reporter"})
        _BRIEF_HISTORY_FILE.write_text(
            json.dumps(history, ensure_ascii=False), encoding="utf-8")
        print(f"  [历史] 写入推荐记录：{codes}")
    except Exception as e:
        print(f"  [历史] 写入失败: {e}")

def _reporter_get_recent_codes(days: int = 3) -> list:
    """读取最近 N 天的已推荐代码（V88 网页版与钉钉报告共享）。"""
    try:
        if not _BRIEF_HISTORY_FILE.exists():
            return []
        history = json.loads(_BRIEF_HISTORY_FILE.read_text(encoding="utf-8"))
        cutoff = time.time() - days * 86400
        codes = []
        for r in history:
            if r.get("ts", 0) > cutoff:
                codes.extend(r.get("codes", []))
        return list(set(codes))
    except Exception:
        return []


# ─── 双通道评分（拐点 / 启动）────────────────────────────────────────────────

def _score_inflection_r(df) -> dict | None:
    """
    拐点通道：三关全中才入池。
    Gate1 预期上修代理：底部40% + (RSI背离 OR 止跌反弹)
    Gate2 结构不再恶化：10日低点抬升 AND 未破20日低
    Gate3 止跌量能改善：上涨日均量 > 下跌日均量
    """
    if df is None or len(df) < 40 or "Close" not in df.columns:
        return None
    try:
        df = df.copy()
        close  = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        high   = df["High"].astype(float)
        low    = df["Low"].astype(float)
        delta  = close.diff()
        gain   = delta.where(delta > 0, 0).fillna(0)
        loss   = (-delta.where(delta < 0, 0)).fillna(0)
        rsi    = float(100 - 100 / (1 + gain.ewm(com=13).mean().iloc[-1] /
                                    (loss.ewm(com=13).mean().iloc[-1] + 1e-10)))
        last_c = float(close.iloc[-1])
        period = min(126, len(df))
        h6m    = float(high.tail(period).max())
        l6m    = float(low.tail(period).min())
        range6m = h6m - l6m
        pos6m   = (last_c - l6m) / range6m if range6m > 0 else 0.5
        ret5    = float(close.iloc[-1] / close.iloc[-6]  - 1) * 100 if len(close) >= 6  else 0
        ret20   = float(close.iloc[-1] / close.iloc[-21] - 1) * 100 if len(close) >= 21 else 0
        rsi_div = (float(close.tail(20).min()) <= last_c * 1.02) and (rsi > 40)
        rebound = (ret5 > 0) and (ret20 < -5)
        gate1   = (pos6m <= 0.40) and (rsi_div or rebound)
        low10r  = float(low.iloc[-10:].min())
        low10p  = float(low.iloc[-20:-10].min()) if len(low) >= 20 else low10r
        gate2   = (low10r > low10p) and (last_c > float(close.tail(20).min()) * 0.99)
        r10     = df.tail(10).copy()
        up_vol  = float(r10[r10["Close"] >= r10["Open"]]["Volume"].mean() or 0)
        dn_vol  = float(r10[r10["Close"] <  r10["Open"]]["Volume"].mean() or 1)
        gate3   = up_vol > dn_vol
        if not (gate1 and gate2 and gate3):
            return None
        score   = min(100, int((0.40 - pos6m) / 0.40 * 30 if pos6m <= 0.40 else 0)
                      + (20 if rebound else 0) + (15 if rsi_div else 0)
                      + 20 + min(15, int(up_vol / dn_vol * 5 if dn_vol else 5)))
        return {"score": score, "pos6m": pos6m, "ret5": ret5, "ret20": ret20,
                "rsi": rsi, "vol_ratio": up_vol / max(dn_vol, 1)}
    except Exception:
        return None


def _score_breakout_v2_r(df, bm_ret5: float = 0.0) -> dict | None:
    """
    启动通道：三信号满足≥2/3才入池。
    S1 突破20日最高收盘  S2 放量>1.5x  S3 相对强弱>基准+2%
    """
    if df is None or len(df) < 25 or "Close" not in df.columns:
        return None
    try:
        df    = df.copy()
        close = df["Close"].astype(float)
        vol   = df["Volume"].astype(float)
        high  = df["High"].astype(float)
        low   = df["Low"].astype(float)
        last_c  = float(close.iloc[-1])
        last_v  = float(vol.iloc[-1])
        avg_v20 = float(vol.tail(20).mean())
        delta = close.diff()
        gain  = delta.where(delta > 0, 0).fillna(0)
        loss  = (-delta.where(delta < 0, 0)).fillna(0)
        rsi   = float(100 - 100 / (1 + gain.ewm(com=13).mean().iloc[-1] /
                                   (loss.ewm(com=13).mean().iloc[-1] + 1e-10)))
        h20p  = float(close.iloc[-21:-1].max()) if len(close) >= 21 else float(close.iloc[:-1].max())
        s1    = last_c > h20p
        s2    = last_v > avg_v20 * 1.5
        ret5  = float((close.iloc[-1] / close.iloc[-6] - 1) * 100) if len(close) >= 6 else 0
        s3    = ret5 > bm_ret5 + 2.0
        if sum([s1, s2, s3]) < 2:
            return None
        margin = (last_c / h20p - 1) * 100 if h20p > 0 else 0
        score  = (35 if s1 else 0) + (30 if s2 else 0) + (25 if s3 else 0)
        dr = float(high.iloc[-1] - low.iloc[-1])
        if dr > 0 and (last_c - float(low.iloc[-1])) / dr > 0.70:
            score += 5
        if 50 <= rsi <= 78:
            score += 5
        return {"score": min(100, score), "s1": s1, "s2": s2, "s3": s3,
                "margin": margin, "vol_ratio": last_v / max(avg_v20, 1),
                "ret5": ret5, "rsi": rsi}
    except Exception:
        return None


def _get_benchmark_return_r(market: str, days: int = 5) -> float:
    """拉取基准指数N日收益率（SPY/^HSI/000300.SS）。"""
    bm = {"US": "SPY", "HK": "^HSI", "CN": "000300.SS"}
    try:
        df = yf.Ticker(bm.get(market, "SPY")).history(period="20d", timeout=8)
        if df is None or len(df) < days + 1:
            return 0.0
        c = df["Close"].dropna()
        return float((c.iloc[-1] / c.iloc[-(days+1)] - 1) * 100) if len(c) >= days + 1 else 0.0
    except Exception:
        return 0.0


def _fetch_eastmoney_stock_list(market, limit=200):
    """从东方财富API获取股票列表（支持分页）"""
    try:
        base = "http://80.push2.eastmoney.com/api/qt/clist/get"
        page_size = EASTMONEY_PAGE_SIZE
        all_stocks = []
        pn = 1
        while len(all_stocks) < limit:
            time.sleep(0.6)
            pz = min(page_size, limit - len(all_stocks))
            if market == "us":
                params = {"pn": pn, "pz": pz, "fs": "m:105,m:106,m:107", "fields": "f12,f14,f20", "ut": "bd1d9ddb04089700cf9c27f6f7426281"}
            elif market == "hk":
                params = {"pn": pn, "pz": pz, "fs": "m:128", "fields": "f12,f14,f20", "ut": "bd1d9ddb04089700cf9c27f6f7426281"}
            elif market == "cn":
                params = {"pn": pn, "pz": pz, "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23", "fields": "f12,f14,f20", "ut": "bd1d9ddb04089700cf9c27f6f7426281"}
            else:
                return []
            url = f"{base}?{urllib.parse.urlencode(params)}"
            with urllib.request.urlopen(url, timeout=10, context=ssl._create_unverified_context()) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            if not isinstance(data, dict):
                break
            inner = data.get("data")
            if not inner or not isinstance(inner, dict) or not inner.get("diff"):
                break
            diff = inner.get("diff")
            diff_list = diff if isinstance(diff, list) else (list(diff.values()) if isinstance(diff, dict) else [])
            page_stocks = []
            for item in diff_list:
                if not isinstance(item, dict):
                    continue
                code = str(item.get("f12", "") or "").strip()
                name = str(item.get("f14", "") or "").strip()
                if code and name:
                    yf_code = _to_yf_cn_code(code)
                    page_stocks.append((code, name, yf_code))
            if not page_stocks:
                break
            all_stocks.extend(page_stocks)
            if len(page_stocks) < pz:
                break
            pn += 1
            if len(all_stocks) >= limit:
                break
        result = all_stocks[:limit]
        print(f"  [股票池] {market.upper()} 东财API: {len(result)} 只")
        return result
    except Exception as e:
        print(f"  [股票池] {market.upper()} 东财API 失败: {e}")
    return []


def _get_backup_us_pool():
    return [
        ("AAPL", "苹果", "AAPL"), ("MSFT", "微软", "MSFT"), ("GOOGL", "谷歌A", "GOOGL"), ("AMZN", "亚马逊", "AMZN"),
        ("META", "Meta", "META"), ("NVDA", "英伟达", "NVDA"), ("TSLA", "特斯拉", "TSLA"), ("NFLX", "奈飞", "NFLX"),
        ("TSM", "台积电", "TSM"), ("ASML", "阿斯麦", "ASML"), ("AMD", "超微半导体", "AMD"), ("INTC", "英特尔", "INTC"),
        ("QCOM", "高通", "QCOM"), ("AVGO", "博通", "AVGO"), ("CRM", "Salesforce", "CRM"), ("ORCL", "甲骨文", "ORCL"),
        ("ADBE", "Adobe", "ADBE"), ("PLTR", "Palantir", "PLTR"), ("SNOW", "Snowflake", "SNOW"), ("CRWD", "CrowdStrike", "CRWD"),
        ("JPM", "摩根大通", "JPM"), ("BAC", "美国银行", "BAC"), ("V", "Visa", "V"), ("MA", "万事达", "MA"),
        ("UNH", "联合健康", "UNH"), ("JNJ", "强生", "JNJ"), ("LLY", "礼来", "LLY"), ("PFE", "辉瑞", "PFE"),
        ("BABA", "阿里巴巴", "BABA"), ("PDD", "拼多多", "PDD"), ("JD", "京东", "JD"), ("NIO", "蔚来", "NIO"),
        ("XOM", "埃克森美孚", "XOM"), ("CVX", "雪佛龙", "CVX"), ("COST", "好市多", "COST"), ("WMT", "沃尔玛", "WMT"),
        ("DIS", "迪士尼", "DIS"), ("UBER", "Uber", "UBER"), ("COIN", "Coinbase", "COIN"), ("MRNA", "Moderna", "MRNA"),
        ("ABBV", "艾伯维", "ABBV"), ("NVO", "诺和诺德", "NVO"), ("PM", "菲利普莫里斯", "PM"),
        ("ACMR", "ACM Research", "ACMR"), ("BRK-B", "伯克希尔B", "BRK-B"),
    ]


def _get_backup_hk_pool():
    return [
        ("00700", "腾讯控股", "0700.HK"), ("09988", "阿里巴巴-SW", "9988.HK"), ("03690", "美团-W", "3690.HK"),
        ("01810", "小米集团-W", "1810.HK"), ("09618", "京东集团-SW", "9618.HK"), ("09999", "网易-S", "9999.HK"),
        ("01024", "快手-W", "1024.HK"), ("02318", "中国平安", "2318.HK"), ("00941", "中国移动", "0941.HK"),
        ("00883", "中国海洋石油", "0883.HK"), ("00388", "香港交易所", "0388.HK"), ("01299", "友邦保险", "1299.HK"),
        ("00005", "汇丰控股", "0005.HK"), ("00939", "建设银行", "0939.HK"), ("03968", "招商银行", "3968.HK"),
        ("01109", "华润置地", "1109.HK"), ("00857", "中国石油股份", "0857.HK"), ("02020", "安踏体育", "2020.HK"),
        ("01211", "比亚迪股份", "1211.HK"), ("02269", "药明生物", "2269.HK"), ("01801", "信达生物", "1801.HK"),
        ("00728", "中国电信", "0728.HK"), ("00762", "中国联通", "0762.HK"), ("00981", "中芯国际", "0981.HK"),
        ("09868", "小鹏汽车-W", "9868.HK"), ("09866", "蔚来-SW", "9866.HK"), ("02015", "理想汽车-W", "2015.HK"),
    ]


def _get_backup_cn_pool():
    return [
        ("600519", "贵州茅台", "600519.SS"), ("300750", "宁德时代", "300750.SZ"), ("000858", "五粮液", "000858.SZ"),
        ("601318", "中国平安", "601318.SS"), ("000333", "美的集团", "000333.SZ"), ("600036", "招商银行", "600036.SS"),
        ("000001", "平安银行", "000001.SZ"), ("601012", "隆基绿能", "601012.SS"), ("002594", "比亚迪", "002594.SZ"),
        ("300059", "东方财富", "300059.SZ"), ("600276", "恒瑞医药", "600276.SS"), ("000725", "京东方A", "000725.SZ"),
        ("601888", "中国中免", "601888.SS"), ("600030", "中信证券", "600030.SS"), ("002415", "海康威视", "002415.SZ"),
        ("600000", "浦发银行", "600000.SS"), ("600028", "中国石化", "600028.SS"), ("601857", "中国石油", "601857.SS"),
        ("601288", "农业银行", "601288.SS"), ("601398", "工商银行", "601398.SS"), ("601939", "建设银行", "601939.SS"),
        ("000063", "中兴通讯", "000063.SZ"), ("000651", "格力电器", "000651.SZ"), ("002475", "立讯精密", "002475.SZ"),
        ("603259", "药明康德", "603259.SS"), ("601138", "工业富联", "601138.SS"), ("688981", "中芯国际", "688981.SS"),
        ("600887", "伊利股份", "600887.SS"), ("601899", "紫金矿业", "601899.SS"), ("300274", "阳光电源", "300274.SZ"),
    ]


def _init_stock_pools_v88():
    us = _fetch_eastmoney_stock_list("us", TARGET_POOL_SIZE["US"])
    if not us or len(us) < 30:
        us = _get_backup_us_pool()
        print("  [股票池] 美股使用备用池")
    hk = _fetch_eastmoney_stock_list("hk", TARGET_POOL_SIZE["HK"])
    if not hk or len(hk) < 30:
        hk = _get_backup_hk_pool()
        print("  [股票池] 港股使用备用池")
    cn = _fetch_eastmoney_stock_list("cn", TARGET_POOL_SIZE["CN"])
    if not cn or len(cn) < 30:
        cn = _get_backup_cn_pool()
        print("  [股票池] A股使用备用池")
    return us, hk, cn


def _simple_metrics_score(df, code):
    if df is None or df.empty or len(df) < 20 or "Close" not in df.columns:
        return None
    try:
        df = df.apply(pd.to_numeric, errors="coerce").dropna().sort_index()
        if len(df) < 20:
            return None
        df = df.copy()
        for p in [5, 10, 20, 50, 60, 200]:
            if len(df) >= p:
                df[f"MA{p}"] = df["Close"].rolling(p).mean()
        delta = df["Close"].diff()
        gain = delta.where(delta > 0, 0).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)
        rs = gain.ewm(com=13).mean() / (loss.ewm(com=13).mean() + 1e-10)
        df["RSI"] = 100 - (100 / (1 + rs))
        df["RSI"] = df["RSI"].fillna(50)
        last = df.iloc[-1]
        score = 0
        if last["Close"] > last.get("MA50", 0): score += 15
        if last["Close"] > last.get("MA200", 0): score += 15
        if len(df) >= 60 and last["Close"] >= df["High"].tail(60).max() * 0.95: score += 10
        if len(df) >= 20 and last["Volume"] > df["Volume"].tail(20).mean() * 1.2: score += 10
        if last["RSI"] > 50: score += 10
        if last.get("MA5", 0) > last.get("MA10", 0) > last.get("MA20", 0): score += 15
        if len(df) >= 21 and (last["Close"] - df["Close"].iloc[-21]) / (df["Close"].iloc[-21] + 1e-10) > 0: score += 10
        if last["Close"] > last.get("MA60", 0): score += 15
        if len(df) >= 6 and (last["Close"] - df["Close"].iloc[-6]) / (df["Close"].iloc[-6] + 1e-10) > 0.03: score += 15
        return min(100, score)
    except Exception:
        return None


def _classify_term_style(df, last):
    try:
        ret_5d = (last["Close"] - df["Close"].iloc[-6]) / (df["Close"].iloc[-6] + 1e-10) if len(df) >= 6 else 0
        ret_20d = (last["Close"] - df["Close"].iloc[-21]) / (df["Close"].iloc[-21] + 1e-10) if len(df) >= 21 else 0
        above_ma20 = last["Close"] > last.get("MA20", 0)
        above_ma200 = last["Close"] > last.get("MA200", 0)
        vol_20 = df["Volume"].tail(20).std() if len(df) >= 20 else 0
        vol_ratio = vol_20 / (df["Volume"].tail(20).mean() + 1e-10) if len(df) >= 20 else 0
        if ret_5d > 0.03 or (ret_20d > 0.05 and above_ma20):
            return "short"
        if above_ma200 and vol_ratio < 1.5:
            return "long"
        return "mid"
    except Exception:
        return "mid"


def _screened_candidates(pool, min_score, prefix, market_label, max_per_type=40,
                          bm_ret5: float = 0.0):
    """
    筛选候选股。同时运行：
      ① 主力候选（CANSLIM ≥ min_score）→ 传统短中长期分桶
      ② 拐点候选（三关全中，赔率型）
      ③ 启动候选（三中二，胜率型）
    返回 (main_list, inflection_list, breakout_list)
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _worker(item):
        try:
            yf_code = item[2] if len(item) >= 3 else _to_yf_cn_code(item[0])
            # A股优先 Tushare
            df = None
            if _TS_AVAILABLE and _ts_is_cn(yf_code):
                df = _ts_fetch_df(yf_code, period="1y")
            if df is None:
                df = yf.Ticker(yf_code).history(period="1y", timeout=8)
            if df is None or len(df) < 20:
                return None
            df = df.apply(pd.to_numeric, errors="coerce").dropna().sort_index()
            if len(df) < 20:
                return None
            df = df.copy()
            for p in [5, 10, 20, 50, 60, 200]:
                if len(df) >= p:
                    df[f"MA{p}"] = df["Close"].rolling(p).mean()
            delta = df["Close"].diff()
            gain  = delta.where(delta > 0, 0).fillna(0)
            loss  = (-delta.where(delta < 0, 0)).fillna(0)
            rs    = gain.ewm(com=13).mean() / (loss.ewm(com=13).mean() + 1e-10)
            df["RSI"] = (100 - 100 / (1 + rs)).fillna(50)
            last  = df.iloc[-1]
            score = _simple_metrics_score(df, yf_code)
            price = float(df["Close"].iloc[-1]) if "Close" in df.columns else None

            # 主力候选（CANSLIM筛选）
            main_hit = (score is not None and score >= min_score)
            term = _classify_term_style(df, last) if main_hit else None

            # 拐点候选
            inf_r = _score_inflection_r(df) if len(df) >= 40 else None

            # 启动候选
            bo_r  = _score_breakout_v2_r(df, bm_ret5) if len(df) >= 25 else None

            return (item, price, score, term, main_hit, inf_r, bo_r)
        except Exception:
            return None

    results = []
    total = len(pool)
    done  = 0
    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = {ex.submit(_worker, it): it for it in pool}
        for f in as_completed(futures):
            done += 1
            if done % 50 == 0 or done == total:
                print(f"  [筛选] {market_label}: {done}/{total}")
            r = f.result()
            if r:
                results.append(r)

    # ── 主力候选（原有逻辑）──
    main_res = [x for x in results if x[4]]
    short_l = sorted([x for x in main_res if x[3] == "short"], key=lambda x: x[2] or 0, reverse=True)[:max_per_type]
    mid_l   = sorted([x for x in main_res if x[3] == "mid"],   key=lambda x: x[2] or 0, reverse=True)[:max_per_type]
    long_l  = sorted([x for x in main_res if x[3] == "long"],  key=lambda x: x[2] or 0, reverse=True)[:max_per_type]
    used_ids = {id(x[0]) for x in short_l + mid_l + long_l}
    fill = sorted([x for x in main_res if id(x[0]) not in used_ids], key=lambda x: x[2] or 0, reverse=True)
    for lst in (short_l, mid_l, long_l):
        while len(lst) < max_per_type and fill:
            lst.append(fill.pop(0))
    merged = short_l + mid_l + long_l
    out_main = []
    for item, price, _, _, _, _, _ in merged[:MAX_WSJ_CANDIDATES]:
        pstr = f"{prefix}{price:.2f}" if price is not None else "N/A"
        out_main.append((item, pstr))

    # ── 拐点候选 Top10 ──
    inf_hits = [(x[0], x[1], x[5]) for x in results if x[5] is not None]
    inf_hits = sorted(inf_hits, key=lambda x: x[2]["score"], reverse=True)[:10]
    out_inf  = []
    for item, price, ir in inf_hits:
        pstr = f"{prefix}{price:.2f}" if price is not None else "N/A"
        out_inf.append((item, pstr, ir))

    # ── 启动候选 Top10 ──
    bo_hits  = [(x[0], x[1], x[6]) for x in results if x[6] is not None]
    bo_hits  = sorted(bo_hits, key=lambda x: x[2]["score"], reverse=True)[:10]
    out_bo   = []
    for item, price, br in bo_hits:
        pstr = f"{prefix}{price:.2f}" if price is not None else "N/A"
        out_bo.append((item, pstr, br))

    print(f"  [筛选] {market_label}: 主力{len(out_main)} 拐点{len(out_inf)} 启动{len(out_bo)}")
    return out_main, out_inf, out_bo

# ─── 钉钉推送 ────────────────────────────────────────────────────────────────

def send_to_dingtalk(title, content, max_retries=2, part_type="A"):
    """发送到钉钉（支持加签模式和无签名关键词模式）。华尔街日报风格排版，超长自动截断。"""
    if not DINGTALK_WEBHOOK:
        print("⚠️  钉钉配置缺失（需 DINGTALK_WEBHOOK）")
        return False

    if not DINGTALK_SECRET:
        print("ℹ️  DINGTALK_SECRET 未配置，使用无签名模式（需钉钉机器人配置【关键词】安全验证）")

    if content:
        content = _format_dingtalk_wsj(content, part_type)

    if content and len(content) > DINGTALK_MAX_CONTENT_CHARS:
        content = content[:DINGTALK_MAX_CONTENT_CHARS] + "\n\n---\n*(内容过长已截断)*"
        print(f"⚠️  内容超长已截断至 {DINGTALK_MAX_CONTENT_CHARS} 字")

    if "日报" not in title and "日报" not in (content or ""):
        content = f"【AI股市日报】\n\n{content}"

    # 关键词必须出现在消息内（钉钉关键词安全验证），放在 header 和 footer 双保险
    kw = f" · {DINGTALK_KEYWORD}" if DINGTALK_KEYWORD else ""
    header = f"## {title}{kw}\n\n"
    footer = f"\n\n---\n*V88 AI · 机构简报{kw}*"
    body = f"{header}{content}{footer}"

    message = {
        "msgtype": "markdown",
        "markdown": {
            "title": f"📰 {title}",
            "text": body
        }
    }
    data = json.dumps(message, ensure_ascii=False).encode('utf-8')
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    context = ssl._create_unverified_context()

    for attempt in range(max_retries):
        try:
            # 有 SECRET 则加签，否则直接用 webhook URL（关键词安全模式）
            if DINGTALK_SECRET:
                timestamp = str(round(time.time() * 1000))
                secret_enc = DINGTALK_SECRET.encode('utf-8')
                string_to_sign = f'{timestamp}\n{DINGTALK_SECRET}'
                hmac_code = hmac.new(secret_enc, string_to_sign.encode('utf-8'), digestmod=hashlib.sha256).digest()
                sign = urllib.parse.quote_plus(base64.b64encode(hmac_code).decode('ascii'))
                webhook_url = f"{DINGTALK_WEBHOOK}&timestamp={timestamp}&sign={sign}"
            else:
                webhook_url = DINGTALK_WEBHOOK
            req = urllib.request.Request(webhook_url, data=data, headers=headers)
            with urllib.request.urlopen(req, timeout=15, context=context) as response:
                result = json.loads(response.read().decode('utf-8'))
                errcode = result.get('errcode', -1)
                errmsg = result.get('errmsg', '')
                if errcode == 0:
                    print(f"✅ 钉钉推送成功: {title}")
                    return True
                print(f"❌ 钉钉拒绝投递 | errcode={errcode} errmsg={errmsg}")
        except Exception as e:
            print(f"❌ 钉钉推送异常: {e}")
        if attempt < max_retries - 1:
            print(f"🔄 3 秒后重试 ({attempt + 2}/{max_retries})...")
            time.sleep(3)
    return False


# ─── 飞书推送（消息卡片格式）────────────────────────────────────────────────

def send_feishu(content: str, webhook_url: str = "") -> bool:
    """
    发送飞书消息卡片（interactive card）。
    自动解析报告各节，失败时降级为纯文本推送。
    """
    import requests as _req

    url = webhook_url or FEISHU_WEBHOOK
    if not url:
        print("⚠️  飞书配置缺失（需 FEISHU_WEBHOOK 环境变量）")
        return False

    # ── 辅助：提取两个 section 标记之间的内容 ──────────────────────────────
    def _section(text: str, start: str, end: str = None) -> str:
        if end:
            m = re.search(
                re.escape(start) + r"(.*?)" + re.escape(end),
                text, re.DOTALL
            )
        else:
            m = re.search(re.escape(start) + r"(.*?)$", text, re.DOTALL)
        return m.group(1).strip() if m else ""

    # ── 辅助：从三市场文本中提取单个市场块 ────────────────────────────────
    def _market_block(text: str, market: str) -> str:
        lines   = text.split("\n")
        result  = []
        capture = False
        others  = [m for m in ("🇺🇸 美股", "🇭🇰 港股", "🇨🇳 A股", "联动与配置") if market not in m]
        for line in lines:
            if market in line:
                capture = True
                result.append(f"**{market}**")
                continue
            if capture:
                if any(o in line for o in others):
                    break
                result.append(line)
        return "\n".join(result[:10]).strip() or f"（{market} 数据解析中）"

    # ── 提取各节 ─────────────────────────────────────────────────────────
    导语   = _section(content, "## 今日导语",    "## 🔴")
    核心   = _section(content, "## 🔴 核心事件", "## 📊")
    三市场 = _section(content, "## 📊 三市场判断","## 🎯")
    推荐   = _section(content, "## 🎯 精选推荐",  "## 📋")
    持仓   = _section(content, "## 📋 持仓分析",  "## ⚠️")
    风险   = _section(content, "## ⚠️ 风险提示",  None)

    # ── 提取报告时间 ──────────────────────────────────────────────────────
    dm = re.search(r"\d{4}/\d{2}/\d{2} \d{2}:\d{2}", content)
    report_date = dm.group(0) if dm else datetime.now(TZ_SHANGHAI).strftime("%Y/%m/%d %H:%M")

    # ── 构建卡片 JSON ─────────────────────────────────────────────────────
    card = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": f"📈 V88 AI 日报 · {report_date}"
                },
                "template": "blue"
            },
            "elements": [
                # 导语
                {
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"**今日导语**\n{导语}"}
                },
                {"tag": "hr"},

                # 核心事件（最多 2000 字）
                {
                    "tag": "div",
                    "text": {"tag": "lark_md",
                             "content": f"**🔴 核心事件**\n{核心[:2000]}"}
                },
                {"tag": "hr"},

                # 三市场判断 — 三列布局
                {
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": "**📊 三市场判断**"}
                },
                {
                    "tag": "column_set",
                    "flex_mode": "stretch",
                    "background_style": "grey",
                    "columns": [
                        {
                            "tag": "column", "width": "1",
                            "elements": [{"tag": "div", "text": {
                                "tag": "lark_md",
                                "content": _market_block(三市场, "美股")
                            }}]
                        },
                        {
                            "tag": "column", "width": "1",
                            "elements": [{"tag": "div", "text": {
                                "tag": "lark_md",
                                "content": _market_block(三市场, "港股")
                            }}]
                        },
                        {
                            "tag": "column", "width": "1",
                            "elements": [{"tag": "div", "text": {
                                "tag": "lark_md",
                                "content": _market_block(三市场, "A股")
                            }}]
                        },
                    ]
                },
                {"tag": "hr"},

                # 精选推荐（最多 1500 字）
                {
                    "tag": "div",
                    "text": {"tag": "lark_md",
                             "content": f"**🎯 精选推荐**\n{推荐[:1500]}"}
                },
                {"tag": "hr"},

                # 持仓分析（最多 3000 字）
                {
                    "tag": "div",
                    "text": {"tag": "lark_md",
                             "content": f"**📋 持仓分析**\n{持仓[:3000]}"}
                },
                {"tag": "hr"},

                # 风险提示（最多 800 字）
                {
                    "tag": "div",
                    "text": {"tag": "lark_md",
                             "content": f"**⚠️ 风险提示**\n{风险[:800]}"}
                },

                # 底部注脚
                {
                    "tag": "note",
                    "elements": [{
                        "tag": "plain_text",
                        "content": f"V88 AI · 数据来源：Guardian API + yfinance · {report_date}"
                    }]
                }
            ]
        }
    }

    # ── 先尝试卡片格式，失败自动降级纯文本 ──────────────────────────────
    try:
        r_card = _req.post(
            url,
            headers={"Content-Type": "application/json"},
            data=json.dumps(card, ensure_ascii=False).encode("utf-8"),
            timeout=15,
        )
        res_card = r_card.json()
        if res_card.get("code") == 0:
            print(f"✅ 飞书卡片推送成功（约 {len(content)} 字）")
            return True
        print(f"⚠️  飞书卡片推送失败（code={res_card.get('code')}），降级纯文本...")
    except Exception as e:
        print(f"⚠️  飞书卡片请求异常: {e}，降级纯文本...")

    # ── 降级：纯文本（无字数限制，飞书支持约3万字）──────────────────────
    try:
        r_text = _req.post(
            url,
            headers={"Content-Type": "application/json"},
            data=json.dumps(
                {"msg_type": "text", "content": {"text": content}},
                ensure_ascii=False
            ).encode("utf-8"),
            timeout=15,
        )
        res_text = r_text.json()
        if res_text.get("code") == 0:
            print(f"✅ 飞书纯文本推送成功（约 {len(content)} 字）")
            return True
        print(f"❌ 飞书纯文本推送失败: {res_text}")
        return False
    except Exception as e:
        print(f"❌ 飞书推送异常: {e}")
        return False


# ─── 共享市场数据准备（Part A 与 Part B 共用候选池）──────────────────────────

def _prepare_market_data(report_type="evening"):
    """
    准备共享市场数据：日期/时间、市场状态、三地指数、筛选后候选池。
    Part A 和 Part B 调用各自的 Gemini，但共用此数据，避免重复拉取。
    """
    now_sh = datetime.now(TZ_SHANGHAI)
    today = now_sh.strftime("%Y年%m月%d日")
    today_ymd = now_sh.strftime("%Y-%m-%d")
    _ts = now_sh.strftime("%Y-%m-%d %H:%M:%S")

    market_status = _get_market_status(today_ymd)
    if market_status:
        print(f"  📅 市场状态: 美股{market_status['US']} 港股{market_status['HK']} A股{market_status['CN']}")

    indices = {
        "US": _v88_index_change("^GSPC", "标普500指数"),
        "HK": _v88_index_change("^HSI", "恒生指数"),
        "CN": _v88_index_change("000001.SS", "上证综指"),
    }

    if market_status:
        _market_status_block = (
            f"当日（{today}）交易所日历：美股{market_status['US']}、"
            f"港股{market_status['HK']}、A股{market_status['CN']}。"
            f"开市则按正常交易日描述，休市则说明休市原因。严禁写与上述状态相反的内容。"
        )
    else:
        _market_status_block = (
            f"休市状态由 AI 根据【日期】{today} 自行判断。"
            f"若不确定则按开市描述，严禁在非休市日写「休市」。"
        )

    print("📊 正在初始化股票池（800只：350+200+250）...")
    us_pool, hk_pool, cn_pool = _init_stock_pools_v88()

    # 基准5日收益率（用于启动通道相对强弱计算）
    print("📡 拉取基准指数收益率（SPY/HSI/000300）...")
    bm_us = _get_benchmark_return_r("US")
    bm_hk = _get_benchmark_return_r("HK")
    bm_cn = _get_benchmark_return_r("CN")
    print(f"  基准5日：美股{bm_us:+.1f}% 港股{bm_hk:+.1f}% A股{bm_cn:+.1f}%")

    # 跨日去重：读取近3天已推荐代码
    recent_codes = _reporter_get_recent_codes(days=3)
    recent_block = (
        f"【近3日已推荐代码（本次禁止重复）】{', '.join(recent_codes)}\n"
        if recent_codes else ""
    )
    print(f"  近3日已推荐：{recent_codes or '(空)'}")

    print("📈 正在筛选候选股（800池+主力/拐点/启动三通道，每市场最多100只）...")
    us_main, us_inf, us_bo = _screened_candidates(us_pool, 40, "$",   "美股", bm_ret5=bm_us)
    hk_main, hk_inf, hk_bo = _screened_candidates(hk_pool, 40, "HK$", "港股", bm_ret5=bm_hk)
    cn_main, cn_inf, cn_bo = _screened_candidates(cn_pool, 40, "¥",   "A股",  bm_ret5=bm_cn)

    def _fmt_cand(item, ps):
        return f"{item[1]}({item[2]}): 日报价 {ps}"
    def _fmt_inf(item, ps, ir):
        tags = f"底部{ir['pos6m']*100:.0f}% RSI{ir['rsi']:.0f} 5日{ir['ret5']:+.1f}%"
        return f"{item[1]}({item[2]}): 日报价 {ps} [拐点:{tags}]"
    def _fmt_bo(item, ps, br):
        sigs = ("S1✓" if br["s1"] else "") + ("S2✓" if br["s2"] else "") + ("S3✓" if br["s3"] else "")
        tags = f"突破+{br['margin']:.1f}% 量{br['vol_ratio']:.1f}x RS+{br['ret5']:.1f}% {sigs}"
        return f"{item[1]}({item[2]}): 日报价 {ps} [启动:{tags}]"

    us_c   = [_fmt_cand(*x[:2]) for x in us_main]
    hk_c   = [_fmt_cand(*x[:2]) for x in hk_main]
    cn_c   = [_fmt_cand(*x[:2]) for x in cn_main]
    us_inf_c = [_fmt_inf(*x) for x in us_inf]
    hk_inf_c = [_fmt_inf(*x) for x in hk_inf]
    cn_inf_c = [_fmt_inf(*x) for x in cn_inf]
    us_bo_c  = [_fmt_bo(*x)  for x in us_bo]
    hk_bo_c  = [_fmt_bo(*x)  for x in hk_bo]
    cn_bo_c  = [_fmt_bo(*x)  for x in cn_bo]

    if not us_c or not hk_c or not cn_c:
        print("⚠️ 筛选通过数不足，降级为轮换逻辑")
        _seed = int(now_sh.strftime("%Y%m%d"))
        def _fmt(it, pfx):
            p = _v88_fetch_price(it[2])
            return f"{it[1]}({it[2]}): 日报价 {pfx}{p:.2f}" if p is not None else f"{it[1]}({it[2]})"
        us_c = [_fmt(it, "$")    for it in _rotate_pool(us_pool, _seed)[:30]]
        hk_c = [_fmt(it, "HK$") for it in _rotate_pool(hk_pool, _seed)[:30]]
        cn_c = [_fmt(it, "¥")   for it in _rotate_pool(cn_pool, _seed)[:30]]
        us_inf_c = hk_inf_c = cn_inf_c = []
        us_bo_c  = hk_bo_c  = cn_bo_c  = []

    return {
        "today": today, "today_ymd": today_ymd, "_ts": _ts,
        "market_status": market_status, "market_status_block": _market_status_block,
        "indices": indices,
        "us_c": us_c, "hk_c": hk_c, "cn_c": cn_c,
        "us_inf_c": us_inf_c, "hk_inf_c": hk_inf_c, "cn_inf_c": cn_inf_c,
        "us_bo_c":  us_bo_c,  "hk_bo_c":  hk_bo_c,  "cn_bo_c":  cn_bo_c,
        "recent_block": recent_block,
        "bm_us": bm_us, "bm_hk": bm_hk, "bm_cn": bm_cn,
    }

# ─── Part A：基本面 + 新闻（华尔街日报版，独立 Gemini 调用）─────────────────

def generate_part_a_wsj(report_type, data):
    """
    Part A：华尔街日报风格的基本面简报。
    - 聚焦：财报、战争/地缘政治、政治人物言论、三市场事件|变量|资产影响
    - 不含个股推荐（推荐在 Part B 单独输出）
    - 目标篇幅：2000-2500字（精简版，去冗余保核心）
    """
    today = data["today"]
    _ts   = data["_ts"]
    indices = data["indices"]
    _market_status_block = data["market_status_block"]
    focus = (
        "【早报侧重】港股、A股基本面与新闻优先（≥400字/市场）；美股简要。三市场均须输出。"
        if report_type == "morning" else
        "【晚报侧重】美股基本面与新闻优先（≥400字/市场）；港股、A股简要。三市场均须输出。"
    )

    # 构建新闻块（已在 generate_report_final 里调用，这里直接取）
    news_block = data.get("real_news_block") or _build_news_block()

    # 市场指数补充上下文（不作为新闻来源，仅作数字参考）
    index_ctx = (
        f"【指数收盘参考·禁止作为新闻来源引用】\n"
        f"{indices.get('US', '')}\n"
        f"{indices.get('HK', '')}\n"
        f"{indices.get('CN', '')}\n"
        f"休市状态：{_market_status_block}"
    )

    prompt = f"""{news_block}

---
你是一位顶级机构分析师，职责是从上方真实新闻中筛选出
今日最值得关注的市场事件，供专业投资者做决策参考。

{index_ctx}

【早晚报侧重】{focus}
【日期】{today} | 【时间戳】{_ts} (Asia/Shanghai)

【核心事件数量规则】
- 最少输出 3 条核心事件，最多输出 10 条核心事件
- 筛选标准（按优先级）：
  1. 直接涉及持仓标的的新闻（必须包含，不够3条才补其他）
     （ABBV/NVDA/LLY/TSM/GOOG/PM/NVO/BRK-B/VOO/QQQM/
      0700.HK/0883.HK/1299.HK/0941.HK/
      600519.SS/688981.SS/601899.SS）
  2. 涉及持仓所在行业的新闻（医药/半导体/科技/能源/保险/电信）
  3. 影响整体仓位方向的宏观新闻（美联储/央行/地缘政治/大宗商品）
- 只有真正值得关注的事件才输出，宁少勿滥
- 每条事件必须有真实来源URL，没有URL的不算核心事件
- 如果真实新闻中有超过10条值得关注的事件，按以上优先级排序后取前10条
- 如果真实新闻不足3条，直接写"今日真实新闻不足，仅X条核心事件"，不得用模型推断补充

【地区覆盖强制规则】核心事件中：
- 必须至少有1条涉及港股/中国（香港市场/A股/人民币/中国经济/中资企业）
- 必须至少有1条涉及美股（美国经济/美联储/美国科技/标普500）
- 禁止所有核心事件全部来自同一地区
- 若某地区真实新闻不足，在对应位置写明"[港股/A股] 今日无相关真实新闻"

【任务】
从上方真实新闻中，严格按以下结构输出，不得添加任何未出现在新闻中的内容：

---
## 今日导语（100字以内）
用一句话概括今日市场核心情绪和主要驱动力。

---
## 🔴 核心事件（3-10条，直接影响仓位决策，宁少勿滥）

格式：
### 1. 事件标题
- 来源：[媒体名] · 时间 · URL
- 变量：这条新闻的关键变量是什么
- 对美股影响：📈/📉/➡️ + 一句话
- 对港股影响：📈/📉/➡️ + 一句话
- 对A股影响：📈/📉/➡️ + 一句话
- 持仓关联：与持仓哪只标的直接相关（没有则写"无直接持仓关联"）

### 2. （同上格式）
### 3. （同上格式）
...（如有更多值得关注的真实事件，继续输出至多10条）

---
## ⚪ 次要事件（3-8条背景参考，一行一条，不足时写实际数量，不补充）
格式：[来源] 标题 → 影响方向

---
## 三市场动作建议
（须与核心事件逻辑一致，与 Part D 仓位比例保持一致）
美股：成长仓上限X% | 禁止XX | 允许XX
港股：仓位上限X% | XX
A股：仓位上限X% | XX

---
## 明日必看
最多3个，格式：时间 · 事件 · 预期影响

---
## 🔗 衔接 Part B
基于以上宏观环境，当前值得关注的方向（2-3个，供 Part B 选股参考）：
- 方向1: [行业/板块] — 逻辑: [一句话，含具体催化]
- 方向2: [行业/板块] — 逻辑: [一句话]
（Part B 的个股推荐应优先来自上述方向）

---
【次要事件过滤规则】以下类型文章禁止出现在次要事件中：
- 标题包含"最好的X只股票"、"立即买入"、"值得买入"等投资建议性软文
- 来源是个人博客或内容农场性质的文章（如 Seeking Alpha 个人作者文章）
- 与今日市场事件无关的泛泛投资建议
只保留：真实市场事件、公司公告、政策动态、经济数据类新闻

【强制规则】
1. 所有事件必须来自上方真实新闻，每条必须附带来源和链接
2. 所有财务数字必须来自上方真实数据，不得修改或替换
3. 如果某字段为 None 输出"暂无数据"，不得用估算值替代
4. 不得编造任何未出现在上方数据中的内容
5. 严禁使用虚构或示例公司名称（如 Quantum Dynamics Inc、Global Energy Corp、ABC Corp 等均为虚构，一经出现即视为违规）；所有公司名称必须来自上方新闻原文"""

    print("🤖 正在生成 Part A（基本面+新闻，华尔街日报版）...")
    result = _v88_call_gemini(prompt)
    if result and not result.startswith("❌"):
        return result
    print(f"⚠️  Part A 生成失败: {result[:100] if result else 'None'}")
    return None

# ─── Part B：可执行推荐（9只，独立 Gemini 调用）──────────────────────────────

def generate_part_b_recs(report_type, data, part_a_summary: str = ""):
    """
    Part B：可执行推荐，9只个股（美港A各3只）。
    - Card Schema 机构简报：动作标签、触发、机会/风险概率、建仓区间
    - 不含宏观分析（宏观已在 Part A 输出）
    - 每只推荐注入实时现价
    - part_a_summary：由 generate_report_final() 统一提取后传入，BCD 共享
    """
    today    = data["today"]
    _ts      = data["_ts"]
    us_c     = data["us_c"]
    hk_c     = data["hk_c"]
    cn_c     = data["cn_c"]
    us_inf_c = data.get("us_inf_c", [])
    hk_inf_c = data.get("hk_inf_c", [])
    cn_inf_c = data.get("cn_inf_c", [])
    us_bo_c  = data.get("us_bo_c",  [])
    hk_bo_c  = data.get("hk_bo_c",  [])
    cn_bo_c  = data.get("cn_bo_c",  [])
    recent_block = data.get("recent_block", "")
    bm_us    = data.get("bm_us", 0)
    bm_hk    = data.get("bm_hk", 0)
    bm_cn    = data.get("bm_cn", 0)

    focus = (
        "【早报侧重】港股、A股推荐请重点展开，提供更详细的触发条件；美股推荐可简要。"
        if report_type == "morning" else
        "【晚报侧重】美股推荐请重点展开，提供更详细的触发条件；港股、A股推荐可简要。"
    )

    def _pool_block(main, inf, bo, label):
        lines = [f"  {c}" for c in main]
        if inf:
            lines += [f"  [拐点候选·赔率] {c}" for c in inf]
        if bo:
            lines += [f"  [启动候选·胜率] {c}" for c in bo]
        return f"- {label}（主力{len(main)}只 + 拐点{len(inf)}只 + 启动{len(bo)}只）：\n" + "\n".join(lines)

    _fundamentals_b = data.get("fundamentals_block", "")
    _news_summary_b = part_a_summary or "（Part A 摘要未生成）"

    prompt = f"""【今日真实新闻摘要（来自 Part A，必须基于此选股）】
{_news_summary_b}

【真实财务数据】
{_fundamentals_b}

【选股规则】
1. 推荐的每只股票必须与上方至少一条真实新闻有逻辑关联
2. 在"理由"字段中必须引用具体新闻标题
3. 不得推荐与今日新闻完全无关的股票
4. 若今日新闻利空某板块，禁止推荐该板块个股

---
你是 V88 机构交易员。生成今日**可执行推荐（Part B）**。

【职责边界】Part B 仅含 9 只个股推荐——**严禁输出宏观分析**（宏观已在 Part A 输出）。
【与 Part A 的衔接】Part B 的推荐必须优先来自上方新闻摘要中涉及的方向，并在每只股票的"理由"字段中注明与哪条新闻吻合；若某只推荐偏离上方新闻，须在理由中一句话说明原因。

{focus}

【硬性规则】
1) 每市场固定 3 只：1 立即建仓 + 1 中期跟进 + 1 观察
2) 「观察」禁止给目标位和买入建议
3) **跨日去重**：{recent_block}以上代码近3日已推荐，本次9只中禁止再次列为「立即建仓」或「中期跟进」；若无其他合格标的可降级为「观察」
4) **行业多样性**：每市场3只推荐必须覆盖至少2个不同行业/板块，禁止3只全来自科技
5) **优先选用拐点候选或启动候选**：标注[拐点]的为底部反转机会（赔率佳），标注[启动]的为刚突破标的（胜率佳），优先从这两类中选取「立即建仓」

【候选池·三通道】必须从以下选择，日报价必须全文引用，严禁编造代码：
{_pool_block(us_c, us_inf_c, us_bo_c, f'美股（基准5日{bm_us:+.1f}%）')}
{_pool_block(hk_c, hk_inf_c, hk_bo_c, f'港股（基准5日{bm_hk:+.1f}%）')}
{_pool_block(cn_c, cn_inf_c, cn_bo_c, f'A股（基准5日{bm_cn:+.1f}%）')}
A股/港股必须用数字代码（如 600519.SS、0700.HK）。

【Card Schema·机构简报】每只推荐含以下字段：
- **名称(代码)** · **动作标签** · 现价
- 触发: [24h/72h，一句话，含具体催化事件、触发价格/时间点，必须注明来源媒体（如 Bloomberg、Reuters、WSJ、财新、公司公告等）；若无真实新闻则写：无近期相关新闻触发，基于基本面判断]
- 机会/风险: [X%/Y%] · 建仓区间: [必须基于上方候选池传入的日报价（现价）× (1±3%~5%) 计算，严禁使用历史高价或52周高点]
- 理由: 必须包含①**具体数字**（如PE=18x、RSI=42、距MA20=+3%、本季营收预期+15%）②预期差来源 ③验证时间窗口
- 与Part A方向: [吻合/偏离+一句话说明]
**禁止**：模糊描述（"趋势向好""技术良好""基本面扎实"等无数字支撑的表述一律不得出现）。
**禁止输出**：来源、失效条件、仓位上限、R/R、证据状态灯、解释性段落。
**建仓区间硬性约束**：建仓区间下限不得高于日报价的110%，上限不得高于日报价的115%；严禁将历史高价（如52周高点）混入区间计算。

【V2.1 Action Gate】立即建仓 仅当以下全满足，否则自动降级：
a) 触发时效 ≤ 72h
b) 有明确催化（财报/事件/技术突破）
c) R/R ≥ 2.0（内部判定，不输出）

{data.get("fundamentals_block", "")}
【日期】{today} | 【校验时间】{_ts} (Asia/Shanghai)

请严格按以下结构输出（不要称呼和废话）：

---

## 可执行推荐

### 🇺🇸 美股（3只：1 立即建仓 + 1 中期跟进 + 1 观察）

1. **[名称(代码)]** · **立即建仓** · 现价 $X.XX
   - 触发: [24h/72h，含具体催化+触发价格，如"突破$X.XX确认"]
   - 机会/风险: [X%/Y%] · 建仓区间: [$X.XX–$X.XX]
   - 理由: [含具体数字的一句话，如"PE=18x低于行业均值25x，RSI=42未超买，距MA20=+2%，Q1财报预期+15%验证"]
   - 与Part A方向: [吻合/偏离+一句话]

2. **[名称(代码)]** · **中期跟进** · 现价 $X.XX
   - 触发: [一句话，含具体条件]
   - 机会/风险: [X%/Y%] · 建仓区间: [$X.XX–$X.XX]
   - 理由: [含具体数字]
   - 与Part A方向: [一句话]

3. **[名称(代码)]** · **观察** · 现价 $X.XX
   - 升级条件: [具体触发，如"收盘站上$X.XX+成交量放大"]

---

### 🇭🇰 港股（3只：1 立即建仓 + 1 中期跟进 + 1 观察）

1. **[名称(代码)]** · **立即建仓** · 现价 HK$X.XX
   - 触发: [一句话，含具体条件]
   - 机会/风险: [X%/Y%] · 建仓区间: [HK$X.XX–HK$X.XX]
   - 理由: [含具体数字]
   - 与Part A方向: [一句话]

2. **[名称(代码)]** · **中期跟进** · 现价 HK$X.XX
   - 触发: [一句话]
   - 机会/风险: [X%/Y%] · 建仓区间: [HK$X.XX–HK$X.XX]
   - 理由: [含具体数字]
   - 与Part A方向: [一句话]

3. **[名称(代码)]** · **观察** · 现价 HK$X.XX
   - 升级条件: [具体触发]

---

### 🇨🇳 A股（3只：1 立即建仓 + 1 中期跟进 + 1 观察）

1. **[名称(代码)]** · **立即建仓** · 现价 ¥X.XX
   - 触发: [一句话，含具体条件]
   - 机会/风险: [X%/Y%] · 建仓区间: [¥X.XX–¥X.XX]
   - 理由: [含具体数字]
   - 与Part A方向: [一句话]

2. **[名称(代码)]** · **中期跟进** · 现价 ¥X.XX
   - 触发: [一句话]
   - 机会/风险: [X%/Y%] · 建仓区间: [¥X.XX–¥X.XX]
   - 理由: [含具体数字]
   - 与Part A方向: [一句话]

3. **[名称(代码)]** · **观察** · 现价 ¥X.XX
   - 升级条件: [具体触发]

---

*数据: 雅虎财经 | 时间戳: {_ts} (Asia/Shanghai)*"""

    print("🤖 正在生成 Part B（可执行推荐，9只）...")
    result = _v88_call_gemini(prompt)
    if not result or result.startswith("❌"):
        print(f"⚠️  Part B 生成失败: {result[:100] if result else 'None'}")
        return None

    # 注入实时现价（补充 AI 可能未更新的价格）
    try:
        import re
        lines = result.split("\n")
        out = []
        for line in lines:
            m = re.search(r'\(([A-Z0-9\-]{2,10}(?:\.[A-Z]{2})?)\)', line)
            if m and any(kw in line for kw in ["立即建仓", "中期跟进", "观察"]):
                code = m.group(1)
                try:
                    yf_code = _to_yf_cn_code(code)
                    df = yf.Ticker(yf_code).history(period="5d", timeout=8)
                    if df is not None and len(df) > 0 and "Close" in df.columns:
                        p = float(df["Close"].iloc[-1])
                        pfx = "HK$" if ".HK" in code else ("¥" if (".SS" in code or ".SZ" in code) else "$")
                        # 只在行中尚无「现价」时才追加，避免重复
                        if "现价" not in line:
                            line = line.rstrip() + f"  *(现价 {pfx}{p:.2f})*"
                except Exception:
                    pass
            out.append(line)
        result = "\n".join(out)
    except Exception as e:
        print(f"⚠️ 现价注入跳过: {e}")

    # 保存推荐历史（用于跨日去重）
    try:
        _reporter_append_history(result)
    except Exception:
        pass

    return result

# ─── Part C：自选股持仓分析（独立 Gemini 调用）──────────────────────────────

def _get_watchlist_scan_signals():
    """获取自选股在V88扫描中的信号：强势/蓄势/拐点/无，供 Part C 差异化操作建议"""
    scan = _load_scan_results()
    if not scan:
        return {}
    sig = {}
    for mkt in ("US", "HK", "CN"):
        d = scan.get(mkt, {})
        for cat, label in [("top", "强势"), ("coil", "蓄势"), ("breakout", "启动"), ("inflection", "拐点")]:
            for s in d.get(cat, []):
                c = str(s.get("代码", "")).upper().strip()
                if c:
                    sig[c] = (label, s.get("理由", ""), s.get("建议", ""))
    return sig


def generate_watchlist_report(report_type="evening", part_a_summary: str = ""):
    """
    Part C：自选股持仓分析。
    每只持仓一张卡片：事件 | 变量 | 资产影响 | 操作建议（持仓/加仓/减仓/观望）
    注入V88扫描信号，强制差异化操作，禁止全部观望。
    part_a_summary：由 generate_report_final() 统一提取后传入，BCD 共享
    """
    total = sum(len(v) for v in WATCHLIST.values())
    if total == 0:
        return None
    try:
        print(f"📋 正在生成 Part C（自选股持仓分析，{total}只）...")
        _watchlist_prices = _get_watchlist_prices()
        scan_sigs = _get_watchlist_scan_signals()

        # 按市场汇总：进榜股票及其信号
        scan_block_lines = []
        for mkt, stocks in WATCHLIST.items():
            in_scan = []
            for code, name in stocks:
                c = str(code).upper().strip()
                if c in scan_sigs:
                    lbl, reason, _ = scan_sigs[c]
                    in_scan.append(f"{name}({code})【{lbl}】{reason}")
            if in_scan:
                em = "🇺🇸" if mkt == "US" else ("🇭🇰" if mkt == "HK" else "🇨🇳")
                lb = "美股" if mkt == "US" else ("港股" if mkt == "HK" else "A股")
                scan_block_lines.append(f"- {em} {lb}：{'；'.join(in_scan)}")
        scan_block = "\n".join(scan_block_lines) if scan_block_lines else "今日无持仓进榜"

        # 从缓存取基本面 + 财报日（generate_report_final 已采集）
        _cached_stocks   = _REPORT_DATA_CACHE.get("stocks",   {})
        _cached_earnings = _REPORT_DATA_CACHE.get("earnings", {})
        fundamentals_c = _format_fundamentals_from_cache(_cached_stocks, _cached_earnings) if _cached_stocks else ""

        now_sh = datetime.now(TZ_SHANGHAI)
        today = now_sh.strftime("%Y年%m月%d日")
        _ts = now_sh.strftime("%Y-%m-%d %H:%M:%S")
        focus = (
            "早报侧重港股、A股持仓分析；美股持仓可简要。"
            if report_type == "morning" else
            "晚报侧重美股持仓分析；港股、A股持仓可简要。"
        )

        _news_summary_c = part_a_summary or "（Part A 摘要未生成）"

        prompt = f"""{fundamentals_c}
---
【今日宏观背景（来自真实新闻）】
{_news_summary_c}

【持仓分析规则】
1. 每只持仓的"事件"字段必须关联今日真实新闻
2. 若今日有直接涉及该标的的新闻，必须在分析中体现
3. 若今日无涉及该标的的新闻，写"今日无直接相关新闻"
4. 操作建议必须与今日新闻逻辑一致，不得给出与新闻方向相反的建议

---
你是 V88 持仓分析师。生成今日**自选股持仓分析（Part C）**。

【职责边界】每只持仓股出一张独立分析卡片，聚焦「当前该不该动、如何动」——**机构简报思维，零废话**。

【日期】{today} | 【数据截点】{_ts} (Asia/Shanghai)
【侧重】{focus}

【自选股列表】（必须每只都有输出）
- 🇺🇸 美股持仓：{', '.join(f"{n}({c})" for c, n in WATCHLIST.get('US', []))}
- 🇭🇰 港股持仓：{', '.join(f"{n}({c})" for c, n in WATCHLIST.get('HK', []))}
- 🇨🇳 A股持仓：{', '.join(f"{n}({c})" for c, n in WATCHLIST.get('CN', []))}

【自选股现价】（雅虎财经）
- 美股：{chr(10).join('  - ' + s for s in _watchlist_prices.get('US', []))}
- 港股：{chr(10).join('  - ' + s for s in _watchlist_prices.get('HK', []))}
- A股：{chr(10).join('  - ' + s for s in _watchlist_prices.get('CN', []))}

【V88量化扫描信号】以下持仓今日进入扫描榜（强势=趋势向好，蓄势=未启动，拐点=弱势反转）：
{scan_block}

【操作规则】⚠️ 必须差异化，禁止全部或多数为观望：
- 📈加仓：强势进榜+逻辑支持、或蓄势突破+催化明确，至少1-2只
- 📉减仓：拐点进榜、技术破位、估值过高、基本面恶化，至少1只
- 📌持仓：逻辑未变、继续持有
- 🔍观望：短期不明朗、等待信号，不超过半数

【仓位约束规则】⚠️ 加仓数量必须与 Part D 仓位建议逻辑一致：
- 当 Part D 建议美股仓位为X%时，美股加仓标的总数不超过1只（选最强的那只）
- 当 Part D 建议港股仓位为X%时，港股加仓标的总数不超过1只
- 当 Part D 建议A股仓位为X%时，A股加仓标的总数不超过1只
- 其余标的一律维持持仓或观望，不得同时对多只标的给出加仓建议
- 每只给出加仓建议的标的，操作说明后面必须追加一行：
  "⚠️ 注：当前[美股/港股/A股]仓位建议X%，如加仓请相应减少其他持仓"
- 若 Part D 尚未生成或仓位建议不明确，默认按保守原则：每个市场最多1只加仓

【卡片规则】每只股票 = 一张卡片（条件触发格式）：
1) **事件**：该股相关事件（财报/公告/催化/技术信号），≤30字
2) **变量**：关键决策变量（业绩预期/技术位/资金面），≤30字，**必须含具体数字**
3) **资产影响**：对标的的影响结论，≤40字
4) **止损价**：**必填，禁止留空或写 N/A**。基于最近支撑位或均线给出具体价格（如"$X.XX / 若跌破则止损"）；若暂不持仓则写"建仓止损：$X.XX"
5) **为什么是现在**：一句话说明当前时点的特殊性（如"财报窗口前48h""刚突破XX关键位""量化扫描今日进入强势榜"），禁止写"长期看好"类泛泛表述
6) **📌 操作（条件触发）**：格式为——
   「[动作]；触发条件：[具体条件，如价格/事件/时间]；若条件未满足则：[备选动作]」
   示例：「📈加仓；触发条件：明日收盘站稳¥XXX以上；若未满足则：📌持仓」

【排版规则】
- 每只股票前后空两行，用 `---` 分隔卡片
- 股票名、操作建议用 **粗体**；关键变量加粗
- 符号：⭐个股、📌持仓、🔍观望、📈加仓、📉减仓；市场用 🇺🇸🇭🇰🇨🇳
- 禁止大段挤在一行，每块独立成段

请严格按以下结构输出（不要标题和废话，直接进入内容）：

---

### 🇺🇸 美股持仓

---

#### ⭐ 艾伯维(ABBV) | 现价 $X.XX | 📌持仓

**事件** | **变量（含数字）** | **资产影响**
[事件≤30字] | [变量≤30字，必须有具体数字] | [资产影响≤40字]

**🛡 止损价**：$X.XX（[止损逻辑，如"跌破MA50"或"支撑位失守"]）
**为什么是现在**：[一句话，说明当前时点特殊性]
**📌 操作**：[动作]；触发条件：[具体条件]；若未满足则：[备选]

---

（每只按上述格式输出，前后空行分隔）

---

### 🇭🇰 港股持仓

---

#### ⭐ 腾讯控股(0700.HK) | 现价 HK$X.XX | [操作]

**事件** | **变量（含数字）** | **资产影响**
[事件] | [变量，必须有具体数字] | [资产影响]

**🛡 止损价**：HK$X.XX（[止损逻辑]）
**为什么是现在**：[一句话]
**📌 操作**：[动作]；触发条件：[具体条件]；若未满足则：[备选]

---

（每只港股同上格式）

---

### 🇨🇳 A股持仓

---

#### ⭐ 贵州茅台(600519.SS) | 现价 ¥X.XX | [操作]

**事件** | **变量（含数字）** | **资产影响**
[事件] | [变量，必须有具体数字] | [资产影响]

**🛡 止损价**：¥X.XX（[止损逻辑]）
**为什么是现在**：[一句话]
**📌 操作**：[动作]；触发条件：[具体条件]；若未满足则：[备选]

---

（每只A股同上格式）

---

*数据: 雅虎财经 | 时间戳: {_ts} (Asia/Shanghai)*

---
【强制规则】
1. 所有事件必须来自上方真实新闻，每条必须附带来源和链接
2. 所有财务数字必须来自上方真实数据，不得修改或替换
3. 如果某字段为 None 输出"暂无数据"，不得用估算值替代
4. 不得编造任何未出现在上方数据中的内容"""

        report = _v88_call_gemini(prompt)
        if report and not report.startswith("❌"):
            report = _format_watchlist_layout(report)
            report = _strip_redundant_watchlist_header(report)
            report = _simplify_part_c_if_over_limit(report)
            return report
        print(f"⚠️ Part C 生成异常: {report[:60] if report else 'None'}...")
    except Exception as e:
        print(f"⚠️ Part C 异常: {e}")
    return None

# ─── Part C 辅助函数 ─────────────────────────────────────────────────────────

def _simplify_part_c_if_over_limit(text: str, max_chars: int = 5000) -> str:
    if not text or len(text) <= max_chars:
        return text
    text = text[:max_chars] + "\n\n---\n*(Part C 已截断至 5000 字)*"
    print(f"⚠️  Part C 超长，已截断至 {max_chars} 字")
    return text


def _strip_redundant_watchlist_header(text: str) -> str:
    import re
    return re.sub(r'^##\s*📋\s*自选股[^\n]*\n+', '', text, count=1)


def _format_watchlist_layout(text: str) -> str:
    import re
    for pattern in [r'([^\n])(####\s*⭐)', r'([^\n])(⭐\s+[^\n]+[|｜]\s*现价)']:
        text = re.sub(pattern, r'\1\n\n\2', text)
    return text

# ─── 指数速览 ─────────────────────────────────────────────────────────────────

def _build_index_banner():
    """构建指数速览横幅，置于 Part A 最前"""
    indices = {
        "US": _v88_index_change("^GSPC", "标普500指数"),
        "HK": _v88_index_change("^HSI", "恒生指数"),
        "CN": _v88_index_change("000001.SS", "上证综指"),
    }
    return (
        f"🇺🇸 **美股**：{indices.get('US', '数据获取中')}\n"
        f"🇭🇰 **港股**：{indices.get('HK', '数据获取中')}\n"
        f"🇨🇳 **A股**：{indices.get('CN', '数据获取中')}\n\n---\n\n"
    )

# ─── 降级固定模板（Part A/B 生成失败时兜底）────────────────────────────────

def get_portfolio_stocks():
    try:
        if os.path.exists(PORTFOLIO_FILE):
            df = pd.read_excel(PORTFOLIO_FILE)
            us_stocks, hk_stocks, cn_stocks = [], [], []
            for _, row in df.iterrows():
                code = str(row['股票代码'])
                name = str(row['股票名称'])
                if code.endswith('.HK') or len(code) == 5:
                    hk_stocks.append((code, name))
                elif '.' not in code or code.startswith('6') or code.startswith('0') or code.startswith('3'):
                    cn_stocks.append((code, name))
                else:
                    us_stocks.append((code, name))
            return {'us': us_stocks[:5], 'hk': hk_stocks[:5], 'cn': cn_stocks[:5]}
    except Exception:
        pass
    return {'us': [], 'hk': [], 'cn': []}


def _generate_report_fallback(report_type="evening"):
    """固定模板降级方案（Part A/B 全失败时兜底）"""
    now = datetime.now(TZ_SHANGHAI)
    today = now.strftime("%m/%d")
    weekday = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][now.weekday()]

    print("📊 正在获取市场数据（降级模板）...")
    if report_type == "evening":
        symbols = {"道指": "^DJI", "纳指": "^IXIC", "标普": "^GSPC"}
    else:
        symbols = {"恒指": "^HSI", "上证": "000001.SS"}

    market_data = {}
    for name, symbol in symbols.items():
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period="5d")
            if not df.empty:
                price = df['Close'].iloc[-1]
                prev = df['Close'].iloc[-2] if len(df) > 1 else price
                change = ((price - prev) / prev * 100) if prev > 0 else 0
                market_data[name] = {"price": price, "change": change}
                print(f"  ✅ {name}: {price:.2f} ({change:+.2f}%)")
        except Exception:
            market_data[name] = {"price": 0, "change": 0}
            print(f"  ❌ {name}: 获取失败")

    print("📂 正在读取持仓股...")
    portfolio = get_portfolio_stocks()

    print("📈 正在获取推荐股票价格...")
    if report_type == "evening":
        try:
            nvda_price = yf.Ticker("NVDA").history(period="2d")['Close'].iloc[-1]
            pltr_price = yf.Ticker("PLTR").history(period="2d")['Close'].iloc[-1]
            msft_price = yf.Ticker("MSFT").history(period="2d")['Close'].iloc[-1]
        except Exception:
            nvda_price, pltr_price, msft_price = 189, 24, 416
    else:
        try:
            tx_price  = yf.Ticker("0700.HK").history(period="2d")['Close'].iloc[-1]
            ali_price = yf.Ticker("9988.HK").history(period="2d")['Close'].iloc[-1]
            xm_price  = yf.Ticker("1810.HK").history(period="2d")['Close'].iloc[-1]
        except Exception:
            tx_price, ali_price, xm_price = 360, 80, 18
        try:
            # A股优先 Tushare
            def _cn_price(c, fallback):
                if _TS_AVAILABLE:
                    r = _ts_price(c)
                    if r: return r["price"]
                try:
                    return yf.Ticker(c).history(period="2d")['Close'].iloc[-1]
                except Exception:
                    return fallback
            mt_price  = _cn_price("600519.SS", 1650)
            nd_price  = _cn_price("300750.SZ", 165)
            wly_price = _cn_price("000858.SZ", 145)
        except Exception:
            mt_price, nd_price, wly_price = 1650, 165, 145

    print("📝 正在生成降级报告...")

    if report_type == "evening":
        dao_change = market_data.get('道指', {}).get('change', 0)
        na_change  = market_data.get('纳指', {}).get('change', 0)
        sp_change  = market_data.get('标普', {}).get('change', 0)
        holdings_list = [f"{name}({code})" for code, name in portfolio['us']]
        holdings_str = "、".join(holdings_list) if holdings_list else "无持仓"
        nvda_support = nvda_price * 0.98
        pltr_support = pltr_price * 0.94
        msft_support = msft_price * 0.96

        report = f"""**🌙 AI股市日报（晚间）- {today} {weekday}**

---

**📊 美股盘面**
• 道指：{dao_change:+.2f}%
• 纳指：{na_change:+.2f}%
• 标普：{sp_change:+.2f}%

**💼 当前持仓**：{holdings_str}

---

**🎲 V88股票池 Part B（降级模板）**

1. **英伟达(NVDA)** · **立即建仓** · 现价 ${nvda_price:.2f}
   - 触发: AI需求强劲，支撑位 ${nvda_support:.0f} 附近技术反弹机会
   - 机会/风险: 65%/35% · 建仓区间: [${nvda_support:.0f}–${nvda_price:.2f}]

2. **Palantir(PLTR)** · **中期跟进** · 现价 ${pltr_price:.2f}
   - 触发: AI合同增长，突破盘整区间
   - 机会/风险: 60%/40% · 建仓区间: [${pltr_support:.1f}–${pltr_price:.2f}]

3. **微软(MSFT)** · **观察** · 现价 ${msft_price:.2f}
   - 等待回调至 ${msft_support:.0f} 附近确认支撑后建仓

---

*💡 降级模板（AI生成失败），仅供参考，不构成投资建议 | AI股市日报*"""

    else:  # morning
        hsi_change = market_data.get('恒指', {}).get('change', 0)
        sh_change  = market_data.get('上证', {}).get('change', 0)
        tx_support  = tx_price  * 0.97
        ali_support = ali_price * 0.94
        xm_support  = xm_price  * 0.92

        report = f"""**🌅 AI股市日报（早间）- {today} {weekday}**

---

**📊 开盘参考**
• 恒指昨收：{hsi_change:+.2f}%
• 上证昨收：{sh_change:+.2f}%

---

**🎲 V88股票池 Part B（降级模板）**

### 🇭🇰 港股

1. **腾讯控股(0700.HK)** · **立即建仓** · 现价 HK${tx_price:.0f}
   - 触发: AI业务加速，支撑位 HK${tx_support:.0f} 附近
   - 机会/风险: 65%/35% · 建仓区间: [HK${tx_support:.0f}–HK${tx_price:.0f}]

2. **阿里巴巴(9988.HK)** · **中期跟进** · 现价 HK${ali_price:.0f}
   - 触发: 云业务增长+回购持续
   - 机会/风险: 60%/40% · 建仓区间: [HK${ali_support:.0f}–HK${ali_price:.0f}]

3. **小米集团(1810.HK)** · **观察** · 现价 HK${xm_price:.1f}
   - 等待汽车业务盈亏平衡确认信号

---

*💡 降级模板（AI生成失败），仅供参考，不构成投资建议 | AI股市日报*"""

    return report

# ─── 排版辅助 ─────────────────────────────────────────────────────────────────

def _format_dingtalk_wsj(content: str, part_type: str = "A") -> str:
    """钉钉显示：华尔街日报风格排版"""
    import re
    if not content or not content.strip():
        return content
    lines = content.split("\n")
    out = []
    prev_blank = False
    for line in lines:
        s = line.rstrip()
        if re.match(r'^##\s+', line) and not line.startswith("###"):
            if out and not prev_blank:
                out.append("")
            out.append(s)
            out.append("")
            prev_blank = True
            continue
        if re.match(r'^###\s+', line):
            if out and not prev_blank:
                out.append("")
            out.append(s)
            prev_blank = False
            continue
        if re.match(r'^####\s+', line):
            if out and not prev_blank:
                out.append("")
            out.append(s)
            prev_blank = False
            continue
        if s.strip() == "---":
            if out and not prev_blank:
                out.append("")
            out.append("---")
            out.append("")
            prev_blank = True
            continue
        if not s.strip():
            if not prev_blank:
                out.append("")
            prev_blank = True
            continue
        prev_blank = False
        out.append(s)
    result = "\n".join(out).strip()
    result = re.sub(r'\n{4,}', '\n\n\n', result)
    return result


def _strip_part_b_verbose(text: str) -> str:
    """Part B 删除无意义字段：来源、失效条件、仓位上限、R/R、证据状态灯"""
    import re
    patterns = [
        (r'\s*[-·]?\s*来源[：:][^\n]*\n', ''),
        (r'\s*[-·]?\s*失效条件[：:][^\n]*\n', ''),
        (r'\s*[-·]?\s*仓位上限[^\n]*\n', ''),
        (r'\s*[-·]?\s*[Rr]/[Rr][：:][^\n]*\n', ''),
        (r'\s*[-·]?\s*证据状态灯[^\n]*\n', ''),
    ]
    for p, repl in patterns:
        text = re.sub(p, repl, text, flags=re.MULTILINE)
    return text

# ─── 主报告生成入口 ───────────────────────────────────────────────────────────

def _build_news_block() -> str:
    """
    构建注入 Part A 的完整新闻块。
    来源：RSS（9个类别）+ NewsAPI 补充，每条含标题/来源/时间/摘要/URL。
    格式严格对应新版 Part A prompt 的 {news_block} 占位符。
    失败时返回提示字符串（不影响主流程，Part A 会写明"新闻不足"）。
    """
    if not _NEWS_FETCHER_OK:
        return "（news_fetcher 模块未加载，新闻数据不可用）"
    try:
        from datetime import timedelta
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        lines = []
        total = 0

        # ── RSS：按类别采集，每类最多 3 条 ──────────────────────────
        rss_cats = list(_NF_RSS_SOURCES.keys())
        for cat in rss_cats:
            articles = _nf_rss(cat)
            if not articles:
                continue
            lines.append(f"\n📂 {cat}")
            for a in articles[:3]:
                pub  = (a.get("published_at") or "")[:16]
                src  = a.get("source", "")
                ttl  = a.get("title", "").strip()
                desc = (a.get("description") or "").strip()[:120]
                url  = a.get("url", "")
                if not ttl:
                    continue
                lines.append(f"  • [{src}] {pub}")
                lines.append(f"    {ttl}")
                if desc:
                    lines.append(f"    摘要: {desc}")
                lines.append(f"    URL: {url}")
                total += 1

        # ── NewsAPI：补充深度新闻 ─────────────────────────────────
        newsapi_topics = [
            "Federal Reserve interest rate decision",
            "earnings report quarterly results",
            "geopolitical conflict war sanctions",
            "China economy stimulus policy",
        ]
        seen_titles: set = {l.strip() for l in lines if l.strip().startswith("    ") and not l.strip().startswith("摘要") and not l.strip().startswith("URL")}
        for topic in newsapi_topics:
            articles = _nf_newsapi(topic, yesterday)
            for a in articles[:3]:
                ttl = (a.get("title") or "").strip()
                if not ttl or ttl in seen_titles:
                    continue
                seen_titles.add(ttl)
                pub  = (a.get("published_at") or "")[:10]
                src  = a.get("source", "")
                desc = (a.get("description") or "").strip()[:120]
                url  = a.get("url", "")
                lines.append(f"\n📂 NewsAPI · {topic}")
                lines.append(f"  • [{src}] {pub}")
                lines.append(f"    {ttl}")
                if desc:
                    lines.append(f"    摘要: {desc}")
                lines.append(f"    URL: {url}")
                total += 1

        if total == 0:
            return '（今日新闻数据暂无，请 Gemini 基于市场知识生成，并标注"来源：模型推断"）'

        print(f"  📰 新闻块：{total} 条（RSS + NewsAPI）")
        return "\n".join(lines)
    except Exception as e:
        print(f"  ⚠️ 新闻块构建失败（不影响主流程）: {e}")
        return f"（新闻获取异常: {e}）"


def _format_news_from_cache(articles: list) -> str:
    """
    将 build_report_data()["news"] 列表格式化为 Part A prompt 所需文本块。
    格式：=== 头部 === + 每条 [来源] 标题 / 摘要 / 时间 / 链接 / ---
    """
    if not articles:
        return ""
    lines = ["=== 今日真实新闻（来自 NewsAPI，禁止使用以外的任何信息）==="]
    for a in articles:
        ttl  = (a.get("title") or "").strip()
        if not ttl:
            continue
        src  = a.get("source") or "暂无来源"
        desc = (a.get("description") or "").strip()[:200]
        pub  = (a.get("published_at") or "")[:16]
        url  = a.get("url") or "暂无链接"
        lines.append(f"[{src}] {ttl}")
        lines.append(f"摘要：{desc if desc else '暂无数据'}")
        lines.append(f"时间：{pub if pub else '暂无数据'}")
        lines.append(f"链接：{url}")
        lines.append("---")
    return "\n".join(lines)


def _format_fundamentals_from_cache(stocks: dict, earnings: dict) -> str:
    """
    将 build_report_data()["stocks"] + ["earnings"] 格式化为 Part C prompt 所需文本块。
    格式：=== 头部 === + 每只标的3行结构化数据 + ---
    """
    if not stocks:
        return ""
    lines = ["=== 真实财务数据（来自 yfinance）==="]
    for sym, d in list(stocks.items()):
        price   = d.get("price")
        fpe     = d.get("forward_pe")
        feps    = d.get("forward_eps")
        target  = d.get("analyst_target")
        rec     = d.get("recommendation") or "暂无数据"
        div     = d.get("dividend_yield")
        earn_dt = earnings.get(sym) or "暂无数据"
        updated = d.get("updated_at", "")

        # 货币前缀
        if ".HK" in sym:
            pfx = "HK$"
        elif sym.endswith(".SS") or sym.endswith(".SZ"):
            pfx = "¥"
        else:
            pfx = "$"

        def _fmt_yield(val):
            """yfinance 返回小数（0.065=6.5%）或已是百分比（6.5），统一转为 X.X% 显示。"""
            if val is None or val == 0:
                return "暂无数据"
            if val < 1:                     # 小数形式，×100 转换
                return f"{val * 100:.1f}%"
            return f"{val:.1f}%"            # 已是百分比形式

        price_s  = f"{pfx}{price:.2f}"   if price  is not None else "暂无数据"
        feps_s   = f"{pfx}{feps:.2f}"    if feps   is not None else "暂无数据"
        fpe_s    = f"{round(fpe, 1)}x"   if fpe    is not None else "暂无数据"
        div_s    = _fmt_yield(div)
        target_s = f"{pfx}{target:.0f}"  if target is not None else "暂无数据"

        lines.append(f"[{sym}] 现价={price_s} | ForwardEPS={feps_s} | ForwardPE={fpe_s}")
        lines.append(f"       股息率={div_s} | 分析师目标价={target_s} | 评级={rec}")
        lines.append(f"       财报日期={earn_dt} | 数据更新={updated}")
        lines.append("---")

    print(f"  📊 基本面块：{len(stocks)} 只标的（含财报日）")
    return "\n".join(lines) + "\n"


def _build_fundamentals_block(watchlist_symbols: list) -> str:
    """
    从 news_fetcher 拉取自选股真实基本面数据，注入 Part B。
    包含：PE / EPS / 分析师目标价 / 推荐评级 / 52周水位 / 财报日
    """
    if not _NEWS_FETCHER_OK or not watchlist_symbols:
        return ""
    try:
        lines = ["【自选股真实基本面（来自 Yahoo Finance，Part B 理由必须引用这些数字）】"]
        for sym in watchlist_symbols[:12]:  # 最多12只，避免超时
            d = _nf_stock(sym)
            price      = d.get("price")
            fpe        = d.get("forward_pe")
            feps       = d.get("forward_eps")
            target     = d.get("analyst_target")
            rec        = d.get("recommendation", "")
            hi         = d.get("52w_high")
            lo         = d.get("52w_low")
            rev_g      = d.get("revenue_growth")
            earn_g     = d.get("earnings_growth")

            # 52周水位
            water = ""
            if price and hi and lo and hi != lo:
                pct = round((price - lo) / (hi - lo) * 100)
                water = f"52w水位{pct}%"

            # 目标价上涨空间
            upside = ""
            if price and target and price > 0:
                upside = f"目标价↑{round((target - price)/price*100)}%"

            parts = [
                f"现价{price}" if price else "",
                f"ForwardPE={round(fpe,1)}" if fpe else "",
                f"ForwardEPS={round(feps,2)}" if feps else "",
                upside,
                water,
                f"评级:{rec}" if rec else "",
                f"营收增速{round(rev_g*100)}%" if rev_g else "",
                f"盈利增速{round(earn_g*100)}%" if earn_g else "",
            ]
            summary = "  ".join(p for p in parts if p)
            lines.append(f"  {sym}: {summary}")

        print(f"  📊 注入基本面数据：{len(watchlist_symbols)} 只")
        return "\n".join(lines) + "\n"
    except Exception as e:
        print(f"  ⚠️ 基本面注入失败（不影响主流程）: {e}")
        return ""


# ─── 真实新闻报告路径（ai-daily-report-v2 日报，用于约束可执行推荐的触发事件）────────────────
AI_DAILY_REPORT_PATHS = [
    Path("/root/ai-daily-report-v2/data/daily_report.md"),  # VPS 生产
    Path.home() / "Desktop" / "ai-daily-report-v2" / "data" / "daily_report.md",  # Mac 本地
    Path(__file__).parent.parent / "ai-daily-report-v2" / "data" / "daily_report.md",
]
if os.environ.get("AI_DAILY_REPORT_PATH"):
    AI_DAILY_REPORT_PATHS.insert(0, Path(os.environ["AI_DAILY_REPORT_PATH"]))


def _load_real_news_report() -> str:
    """
    读取 ai-daily-report-v2 生成的日报，作为可执行推荐「触发条件」的唯一真实新闻来源。
    若文件不存在或读取失败，返回空字符串（Prompt 中会退化为仅用 news_block 约束）。
    """
    for p in AI_DAILY_REPORT_PATHS:
        try:
            if p.exists():
                content = p.read_text(encoding="utf-8-sig").strip()
                if content and len(content) > 100:
                    print(f"  ✅ 已注入真实新闻报告（{len(content)} 字）: {p}")
                    return content
        except Exception as e:
            logger.debug("读取日报失败 %s: %s", p, e)
    print("  ⚠️ 未找到 ai-daily-report-v2 日报，触发条件将仅基于上方 news_block 约束")
    return ""


# ─── 统一单次 AI 调用：完整报告生成 ──────────────────────────────────────────

def generate_full_report(report_type: str = "evening") -> str:
    """
    一次 Gemini 调用生成完整报告（导语 + 核心事件 + 三市场 + 推荐 + 持仓 + 风险）。
    替代原来的四次独立调用，内容逻辑更统一、不矛盾。
    返回报告全文字符串，失败时返回 None。
    """
    global _REPORT_DATA_CACHE

    now_sh  = datetime.now(TZ_SHANGHAI)
    today   = now_sh.strftime("%Y年%m月%d日")
    _ts     = now_sh.strftime("%Y-%m-%d %H:%M:%S")
    focus   = (
        "早报侧重港股、A股分析；美股简要。三市场均须输出。"
        if report_type == "morning" else
        "晚报侧重美股分析；港股、A股简要。三市场均须输出。"
    )

    # ── 1. 采集新闻 + 基本面（写入全局缓存供后续使用）────────────────────────
    all_symbols = [code for mkt in ("US", "HK", "CN") for code, _ in WATCHLIST.get(mkt, [])]
    print(f"📡 采集真实新闻与基本面数据（{len(all_symbols)}只标的）...")
    try:
        yesterday   = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        report_data = build_report_data(watchlist=all_symbols, date_str=yesterday)
        _REPORT_DATA_CACHE = report_data
        news_block        = _format_news_from_cache(report_data.get("news", []))
        fundamentals_block = _format_fundamentals_from_cache(
            report_data.get("stocks", {}), report_data.get("earnings", {})
        )
        print(f"  ✅ 采集完成：{len(report_data.get('news', []))}条新闻 / {len(all_symbols)}只标的")
    except Exception as e:
        print(f"  ⚠️ 数据采集失败，降级使用旧方式: {e}")
        news_block         = _build_news_block() or "（新闻数据获取失败）"
        fundamentals_block = _build_fundamentals_block(all_symbols) or "（基本面数据获取失败）"

    # ── 2. 市场技术数据 ───────────────────────────────────────────────────────
    print("📊 获取三大市场技术数据...")
    us_data = _fetch_market_technicals("^GSPC",     "标普500")
    hk_data = _fetch_market_technicals("^HSI",      "恒生指数")
    cn_data = _fetch_market_technicals("000001.SS", "上证综指")
    market_data_block = (
        f"美股 · 标普500\n{us_data}\n\n"
        f"港股 · 恒生指数\n{hk_data}\n\n"
        f"A股 · 上证综指\n{cn_data}"
    )

    # ── 3. 持仓列表（逐只枚举，确保 AI 覆盖全部 19 只）────────────────────
    def _wl_lines(mkt, flag, label):
        items = WATCHLIST.get(mkt, [])
        return f"{flag} {label}持仓（{len(items)}只）：" + \
               "、".join(f"{n}({c})" for c, n in items)

    watchlist_block = "\n".join([
        _wl_lines("US", "🇺🇸", "美股"),
        _wl_lines("HK", "🇭🇰", "港股"),
        _wl_lines("CN", "🇨🇳", "A股"),
        f"合计 {sum(len(v) for v in WATCHLIST.values())} 只，持仓分析必须全部覆盖，一只不能遗漏",
    ])

    # ── 3.5 读取真实新闻报告（ai-daily-report-v2 日报，约束可执行推荐的触发事件）────
    real_news_report = _load_real_news_report()

    # ── 4. 构建统一 Prompt ────────────────────────────────────────────────────
    _real_news_block = (
        f"\n\n【真实新闻报告（来自 AI 日报，可执行推荐的触发条件必须来自此处）】\n{real_news_report}\n"
        if real_news_report else ""
    )
    prompt = f"""【今日真实新闻（来自 Guardian + RSS + yfinance，禁止编造）】
{news_block}{_real_news_block}

【真实财务数据（来自 yfinance）】
{fundamentals_block}

【市场技术数据（来自 yfinance 行情）】
{market_data_block}

【持仓标的】
{watchlist_block}

【日期】{today} | 【时间戳】{_ts} (Asia/Shanghai)
【侧重】{focus}

---
你是一位顶级机构分析师，基于以上真实数据，
生成一份完整的今日市场分析报告。

输出格式要求（飞书 Markdown 友好）：
- 标题用 ## 和 ### 层级
- 加粗用 **文字** 格式
- 列表用 - 或 1. 2. 3. 有序列表
- 各节之间用空行分隔，不用 --- 波折号
- 每个核心事件之间用空行分隔
- 方向标签：🟢涨 / 🔴跌 / 🟡震荡
- 操作建议标签：✅持仓 / 📈加仓 / 📉减仓 / 👀观望

## 今日导语
用一句话概括今日市场核心情绪和主要驱动力。（100字以内）

## 🔴 核心事件（3-10条，宁少勿滥）

每条格式：

**N. 事件标题**
- **来源**：[媒体名] · 时间 · URL
- **变量**：关键决策变量
- 对美股影响：📈/📉/➡️ + 一句话
- 对港股影响：📈/📉/➡️ + 一句话
- 对A股影响：📈/📉/➡️ + 一句话
- **持仓关联**：与上方持仓哪只标的直接相关（没有则写"无直接持仓关联"）

## 📊 三市场判断

### 🇺🇸 美股
**方向**：🟢涨 / 🔴跌 / 🟡震荡（选一个）
**概率**：上涨X% / 下跌X%（3-5个交易日）
**关键位**：支撑 XXXX | 压力 XXXX
**均线状态**：MA5/MA20/MA60 多头排列/空头排列/混乱
**RSI**：XX（超买>70 / 超卖<30 / 中性）
**核心逻辑**：2-3句话，必须引用上方至少一条核心事件
**操作建议**：✅持仓 / 📈加仓 / 📉减仓 / 👀观望 + 一句话

### 🇭🇰 港股
（同上格式）

### 🇨🇳 A股
（同上格式）

**联动与配置**
- 三市场联动：强联动 / 弱联动 / 独立走势
- 建议仓位：美X% / 港X% / AX%（最低不得低于10%，除非有明确系统性风险）
- 本周最高优先级：一句话

## 🎯 精选推荐（每个市场1-3只，必须与三市场判断方向一致）

⚠️ **触发条件强制规则（禁止编造新闻）**：
- **所有触发事件必须来自上方【今日真实新闻】或【真实新闻报告】列表**
- **禁止编造任何新闻、事件、公告**。若找不到与标的相关的真实新闻，触发字段必须写：**「无近期相关新闻触发，基于基本面判断」**
- 可执行推荐只能基于真实新闻列表中已有的事件，不得虚构催化、财报、政策等
- **触发事件必须注明具体来源媒体名称**（如 Bloomberg、Reuters、WSJ、FT、财新、公司公告、SEC文件等）；禁止只写"据报道"或不注明来源

每只格式：

**[标的名称(代码)] | 现价 X.XX | 📈加仓/✅持仓**
- **触发条件**：必须引用上方真实新闻中的具体事件，注明具体来源媒体名称（如 Bloomberg/Reuters/公司公告），或写「无近期相关新闻触发，基于基本面判断」
- **机会/风险比**：X:1
- **理由**：必须引用上方核心事件中的至少一条（注明事件标题）
- **一致性**：与[美股/港股/A股]判断[方向]一致

## 📋 持仓分析

⚠️ 必须输出上方【持仓标的】中列出的全部标的，按以下三组分别输出，每组加标题（不加#号，直接输出）：

🇺🇸 美股持仓
（逐一输出 艾伯维ABBV / ACM Research ACMR / 英伟达NVDA / 诺和诺德NVO / 标普500ETF VOO / 伯克希尔BRK-B / 纳指ETF QQQM / 谷歌GOOG / 菲利普莫里斯PM / 礼来LLY / 台积电TSM / 特斯拉TSLA，共12只）

🇭🇰 港股持仓
（逐一输出 腾讯控股0700.HK / 中国海洋石油0883.HK / 友邦保险1299.HK / 中国移动0941.HK，共4只）

🇨🇳 A股持仓
（逐一输出 贵州茅台600519.SS / 中芯国际688981.SS / 紫金矿业601899.SS / 澜起科技688008.SS / 中国移动A 600941.SS / 美的集团000333.SZ / 平安银行000001.SZ / 中国电建601669.SS，共8只）

【持仓操作建议判断规则】— 必须严格按此规则判断，不允许自由发挥：

⚠️ **核心原则**：大盘跌 ≠ 全部观望。综合 相对强弱 + 技术面 + 标的属性 + 行业轮动 四维判断。

---

**一、相对强弱评判**（优先于大盘一刀切）
- 若已知个股今日涨跌与大盘涨跌：**个股跑赢大盘**（今日跌幅 < 大盘跌幅）或**逆势上涨** → 维持持仓
- 仅当 **个股跌幅 > 大盘跌幅 × 1.5** 时，才降级为观望
- 若无个股涨跌数据，综合其他因素判断，不得仅因大盘跌而观望

**二、技术面指标**（若上方【市场技术数据】或标的数据中有 RSI/MA）
- RSI < 30（超卖）→ 考虑加仓，不轻易观望
- RSI > 70（超买）→ 降级为观望
- 价格在 MA20 上方 → 支撑持仓
- 价格在 MA20 下方 **且** 跌破 MA50 → 降级为观望

**三、防御型 vs 进攻型**（按标的属性差异化）
**防御型**（VOO/BRK-B/PM/ABBV/LLY/中国移动0941/友邦保险1299/美的000333/平安银行000001）：
  - 市场下跌时**维持持仓**，不轻易观望
  - 止损条件更宽松，仅重大利空才减仓

**进攻型**（NVDA/TSM/TSLA/ACMR/澜起科技688008/中芯国际688981）：
  - 市场下跌时可降级观望
  - 止损条件更严格，技术破位即减仓

**四、行业轮动逻辑**（在操作建议中注明当前最强板块）
- **能源板块强势**（油价涨/地缘催化）→ 中海油0883/紫金矿业601899 维持持仓
- **科技/AI板块强势** → NVDA/TSM/澜起科技688008 维持持仓
- **医药板块强势** → LLY/ABBV/诺和诺德NVO 维持持仓
- 在每只标的「今日」说明中注明：当前最强板块为 [能源/科技/医药/消费/金融]

---

📈加仓（同时满足以下全部）：
  - 现价 > 止损价
  - 分析师目标价 > 现价 × 1.15（上涨空间 ≥ 15%）
  - 今日有正面相关新闻
  - 对应市场判断为 🟢涨 或 🟡震荡
  - （可选）RSI < 30 超卖时优先考虑加仓

✅持仓（满足以下任一即可）：
  - 现价 > 止损价 且 分析师目标价 > 现价 且 今日无直接负面新闻
  - **今日有正面相关新闻** 且 目标价 ≥ 现价×0.9，**即使大盘🔴跌也持仓**
  - 今日无重大新闻 且 目标价 > 现价×1.05，**即使大盘🔴跌也持仓**
  - **防御型标的** 且 市场跌 → 持仓（不轻易观望）
  - **个股跑赢大盘**（跌幅 < 大盘跌幅）或逆势涨 → 持仓
  - **所属板块当前最强**（能源/科技/医药轮动）→ 持仓

👀观望（满足以下全部才观望，禁止因「大盘跌」而一刀切）：
  - 大盘🔴跌 **且** 今日无正面新闻 **且** 目标价 < 现价×1.05
  - 或：今日有负面相关新闻但尚未触发止损
  - 或：RSI > 70 超买
  - 或：**进攻型** 且 价格跌破 MA20 且 跌破 MA50
  - 或：**个股跌幅 > 大盘跌幅 × 1.5**

📉减仓（满足以下任一）：
  - 分析师目标价 < 现价 × 0.9（低于现价 10% 以上）
  - 今日有直接重大负面新闻 且 市场判断为 🔴跌
  - 现价距止损价 < 3%
  - **进攻型** 且 技术破位（跌破关键均线）

判断示例（TSLA）：进攻型 | 目标价<现价×1.05 | 今日FSD负面 | 市场跌 → 📉减仓
判断示例（GOOG/LLY/TSM/0883）：今日有正面新闻 → **即使大盘跌** → ✅持仓，注明板块
判断示例（VOO/BRK-B/PM/ABBV/LLY/0941/1299）：防御型 → 市场跌时维持 ✅持仓
判断示例（0883/601899）：能源板块强势 → ✅持仓，今日注明「能源强势」

【强制差异化】每市场至少 40% 为 持仓 或 加仓，观望不超过 50%，减仓 1–3 只。禁止满屏观望。

每只格式（严格单行，不换行，不允许任何其他格式变体）：
⭐ 股票名(代码) 现价 | 操作 | 止损X | 今日：[15字内，含板块/相对强弱/新闻要点]

要求：
- 股票名和代码必须同时出现，格式为：名称(代码)
- 货币：美股用$，港股用HK$，A股用¥
- 分组标题直接写文字（不加#号，不加markdown语法）
- 「今日」字段需注明：当前最强板块（能源/科技/医药等）、或个股相对强弱、或新闻要点

输出示例（注意：防御型+板块强势+正面新闻→持仓）：
⭐ 英伟达(NVDA) $180.25 | ✅持仓 | 止损$170 | 今日：科技强势，AI芯片需求强劲
⭐ 礼来(LLY) $920 | ✅持仓 | 止损$900 | 今日：医药强势，减肥药利好
⭐ 台积电(TSM) $334 | ✅持仓 | 止损$320 | 今日：科技强势，AI订单饱满
⭐ 伯克希尔(BRK-B) $481 | ✅持仓 | 止损$470 | 今日：防御型，跑赢大盘
⭐ 中国海洋石油(0883.HK) HK$29.58 | ✅持仓 | 止损HK$28 | 今日：能源强势，油价利好
⭐ 特斯拉(TSLA) $391 | 📉减仓 | 止损$375 | 今日：进攻型破位，FSD调查
⭐ 贵州茅台(600519.SS) ¥1413 | ✅持仓 | 止损¥1350 | 今日：无重大新闻

硬性规则：
- 三组全部输出，总计24只，一只不能遗漏
- 止损价必填，不得写N/A，根据近期支撑位或均线给出具体价格
- 今日事件必须来自上方核心事件，没有则写"无重大新闻"
- 操作建议必须严格按上方判断规则，不允许主观臆断
- **差异化强制**：每市场（美股/港股/A股）至少 40% 为 持仓 或 加仓，观望不超过 50%，减仓 1–3 只。禁止满屏观望。

## ⚠️ 风险提示

基于今日核心事件，列出3条最需要关注的风险（具体到事件，不得写泛泛表述）：

1. [风险1]
2. [风险2]
3. [风险3]

---
【强制规则】
1. 所有事件必须来自上方真实新闻，每条必须附带来源URL
2. **精选推荐的「触发条件」必须来自上方【今日真实新闻】或【真实新闻报告】，且必须注明具体来源媒体名称（如 Bloomberg、Reuters、WSJ、财新、公司公告等），禁止编造。若无相关真实新闻，必须写「无近期相关新闻触发，基于基本面判断」**
3. 精选推荐的理由必须引用上方核心事件，注明事件标题
4. 持仓分析操作建议必须严格遵守【持仓操作建议判断规则】，不允许自由发挥
5. 财务数字来自上方真实数据，不得修改或替换
6. 股息率显示规则：yfinance返回小数（0.065=6.5%，val<1则×100；val≥1则直接显示），禁止显示为655%
   验证：ABBV≈3.2%，中国移动≈6.5%，任何持仓股息率不得超过20%
7. 严禁编造任何公司名称（如Quantum Dynamics Inc等虚构名称均违规）
8. 精选推荐方向必须与三市场判断一致，禁止推荐与看跌市场相反方向的标的
9. 持仓分析目标价规则：
   - 若 分析师目标价 < 现价×0.9（低于现价 10% 以上）→ 不得持仓/加仓，建议减仓
   - 若 分析师目标价 < 现价 但差距 < 10%（目标价 ≥ 现价×0.9），且当日有正面新闻 → 建议持仓，不得减仓
   - 若 分析师目标价 < 现价 且差距 < 10%，且当日无正面新闻 → 观望
10. 差异化强制：大盘跌时不得一刀切全部观望。综合 相对强弱+技术面+防御/进攻型+行业轮动 判断；防御型、板块强势、有正面新闻的标的必须 持仓；每市场至少 40% 持仓/加仓，观望不超过 50%。"""

    print("🤖 正在生成完整报告（单次AI调用）...")
    result = _v88_call_gemini(prompt)
    if result and not result.startswith("❌"):
        print(f"  ✅ 完整报告生成成功（约 {len(result)} 字）")
        return result
    print(f"  ⚠️ 完整报告生成失败: {(result or '')[:80]}")
    return None


# ─── Part A 核心事件摘要提取（模块级，供 BCD 共享）──────────────────────────────

def _extract_part_a_summary(text: str) -> str:
    """
    从 Part A 全文中提取核心事件摘要，格式化为 BCD 共享的简洁摘要块。
    优先提取 🔴 核心事件段落，不足时退化为前 800 字。
    """
    if not text:
        return "（Part A 未生成，无可用事件摘要）"
    # 尝试提取"核心事件"段落
    m = re.search(r"(##\s*🔴\s*核心事件.+?)(?=\n##|\Z)", text, re.S)
    if m:
        snippet = m.group(1).strip()
        return snippet[:1200] if len(snippet) > 1200 else snippet
    # 退而求其次：取前 800 字
    return text[:800].strip() + ("…" if len(text) > 800 else "")


def generate_report_final(report_type="evening"):
    """
    主报告生成：共用一次市场数据准备，然后分别调用 Gemini 生成 Part A 和 Part B。
    - 统一调用 build_report_data() 一次，结果写入 _REPORT_DATA_CACHE 供 Part C / D 复用。
    - Part A 使用 report_data["news"] 格式化的新闻块
    - Part B 使用 report_data["stocks"] + ["earnings"] 格式化的基本面块
    返回 (part_a_content, part_b_content, part_a_summary)，失败时对应项为 None/""。
    """
    global _REPORT_DATA_CACHE
    try:
        print(f"🤖 正在准备市场数据（800池筛选，约 2-5 分钟）...")
        data = _prepare_market_data(report_type)

        # ── 统一数据采集：新闻 + 基本面（一次调用，供 A/B/C/D 共享） ────────────
        if _NEWS_FETCHER_OK:
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            all_symbols = [
                code
                for mkt in ("US", "HK", "CN")
                for code, _ in WATCHLIST.get(mkt, [])
            ]
            print(f"📡 正在采集真实新闻与基本面数据（{len(all_symbols)}只标的）...")
            try:
                report_data = build_report_data(
                    watchlist=all_symbols,
                    date_str=yesterday,
                )
                _REPORT_DATA_CACHE = report_data
                logger.info(
                    f"真实数据注入完成：{len(report_data.get('news', []))}条新闻 / "
                    f"{len(report_data.get('stocks', {}))}只标的"
                )
                print(
                    f"  ✅ 采集完成：{len(report_data.get('news', []))}条新闻 / "
                    f"{len(report_data.get('stocks', {}))}只标的"
                )

                # 将新闻列表格式化成 Part A 所需文本块
                news_block = _format_news_from_cache(report_data.get("news", []))
                if news_block:
                    data["real_news_block"] = news_block

                # 将基本面数据格式化成 Part B 所需文本块
                fundamentals_block = _format_fundamentals_from_cache(
                    report_data.get("stocks", {}),
                    report_data.get("earnings", {}),
                )
                if fundamentals_block:
                    data["fundamentals_block"] = fundamentals_block

            except Exception as e:
                print(f"  ⚠️ 数据采集失败，降级使用旧方式: {e}")
                # 降级：分散调用保证不崩
                news_block = _build_news_block()
                if news_block:
                    data["real_news_block"] = news_block
                fundamentals_block = _build_fundamentals_block(all_symbols)
                if fundamentals_block:
                    data["fundamentals_block"] = fundamentals_block
        else:
            print("  ⚠️ news_fetcher 不可用，跳过新闻/基本面注入")

        index_banner = _build_index_banner()

        part_a = generate_part_a_wsj(report_type, data)
        if part_a:
            part_a = index_banner + part_a

        # ── 提取 Part A 核心事件摘要，供 BCD 共享 ─────────────────────────────
        part_a_summary = _extract_part_a_summary(part_a)
        data["part_a_summary"] = part_a_summary

        time.sleep(4)   # 避免 Gemini API 连续调用触发 429/403
        part_b = generate_part_b_recs(report_type, data, part_a_summary=part_a_summary)
        if part_b:
            part_b = _strip_part_b_verbose(part_b)

        return part_a, part_b, part_a_summary
    except Exception as e:
        print(f"⚠️  主报告生成异常: {e}")
        return None, None, ""

# ─── 精华摘要（富内容版，1次 Gemini 调用）──────────────────────────────────

def _load_scan_results():
    """从 scan_results.json 读取扫描结果，返回各市场 top/coil/breakout 精华"""
    try:
        cache_path = Path(__file__).parent / ".cache_brief" / "scan_results.json"
        if not cache_path.exists():
            return {}
        data = json.loads(cache_path.read_text(encoding="utf-8"))
        out = {}
        for mkt in ("US", "HK", "CN"):
            mdata = data.get(mkt, {})
            out[mkt] = {
                "top":       mdata.get("top", [])[:5],
                "coil":      mdata.get("coil", [])[:3],
                "breakout":  mdata.get("breakout", [])[:3],
                "inflection":mdata.get("inflection", [])[:3],
                "bm_ret5":   mdata.get("bm_ret5", 0),
            }
        return out
    except Exception:
        return {}


def _fmt_stocks(stocks, max_n=5):
    """把股票列表格式化成简短字符串供 prompt 使用"""
    lines = []
    for s in stocks[:max_n]:
        code  = s.get("代码", "")
        name  = s.get("股票", "")
        price = s.get("现价", "N/A")
        score = s.get("得分", "")
        form  = s.get("形态", "")
        reason= s.get("理由", "")
        lines.append(f"  {name}({code}) ¥/$/HK${price} 得分{score} {form} {reason}")
    return "\n".join(lines) if lines else "  暂无"


def _fmt_watchlist_prices():
    """获取自选股现价，格式化成 prompt 段落"""
    lines = []
    for mkt, stocks in WATCHLIST.items():
        pfx = "$" if mkt == "US" else ("HK$" if mkt == "HK" else "¥")
        for code, name in stocks:
            p = _v88_fetch_price(code)
            if p:
                lines.append(f"  {name}({code}): {pfx}{p:.2f}")
    return "\n".join(lines) if lines else "  数据获取中"


def generate_digest(report_type="morning"):
    """
    精华日报：指数 + 市场体制 + Top推荐 + 自选股亮点 + 蓄势/启动精选。
    1次 Gemini 调用，生成约 1800 字的钉钉摘要。
    """
    now_sh  = datetime.now(TZ_SHANGHAI)
    today   = now_sh.strftime("%Y年%m月%d日 %H:%M")
    label   = "早报" if report_type == "morning" else "晚报"

    # ── 三地指数 ──────────────────────────────────────────────────────────────
    us_idx  = _v88_index_change("^GSPC",     "标普500")
    ndx_idx = _v88_index_change("^IXIC",     "纳斯达克")
    hk_idx  = _v88_index_change("^HSI",      "恒生指数")
    cn_idx  = _v88_index_change("000001.SS", "上证综指")
    idx_block = "\n".join([us_idx, ndx_idx, hk_idx, cn_idx])

    # ── 扫描结果 ───────────────────────────────────────────────────────────────
    scan = _load_scan_results()

    def _scan_block(mkt, label_mkt):
        if not scan:
            return f"{label_mkt}：扫描数据未就绪"
        d = scan.get(mkt, {})
        top_str = _fmt_stocks(d.get("top", []), 5)
        coil_str = _fmt_stocks(d.get("coil", []), 3)
        bo_str  = _fmt_stocks(d.get("breakout", []), 3)
        inf_str = _fmt_stocks(d.get("inflection", []), 3)
        bm = d.get("bm_ret5", 0)
        return (
            f"{label_mkt}（基准5日收益{bm:+.1f}%）\n"
            f"  【趋势强势Top5】\n{top_str}\n"
            f"  【蓄势潜伏Top3】\n{coil_str}\n"
            f"  【启动突破Top3】\n{bo_str}\n"
            f"  【拐点反转Top3】\n{inf_str}"
        )

    us_scan  = _scan_block("US", "🇺🇸 美股")
    hk_scan  = _scan_block("HK", "🇭🇰 港股")
    cn_scan  = _scan_block("CN", "🇨🇳 A股")

    # ── 自选股现价 ─────────────────────────────────────────────────────────────
    print("  📋 获取自选股现价...")
    wl_prices = _fmt_watchlist_prices()

    session_hint = (
        "当前为亚市交易时段（港股+A股开市），重点关注港股、A股机会。"
        if report_type == "morning" else
        "当前为美市交易时段，重点关注美股机会。"
    )

    prompt = f"""你是顶级机构宏观策略师兼交易员，当前时间 {today}（{label}）。{session_hint}

=== 三地指数（最新）===
{idx_block}

=== 800只股票量化扫描结果（得分排序）===
{us_scan}

{hk_scan}

{cn_scan}

=== 自选股持仓现价 ===
{wl_prices}

请严格按照以下 Markdown 结构输出一份机构级日报，要求语言精炼深度，像高盛晨报，不说废话，总字数控制在 3200 字以内：

## 一、🌐 市场全局眼
用 3 句话总结今天市场的核心驱动力（每句不超过35字，直接点出关键事件/数据/情绪）。

## 二、📌 核心主线
分别指出三大市场当前最具确定性的逻辑（各一句，30字内）：
- 🇨🇳 A股：
- 🇭🇰 港股：
- 🇺🇸 美股：

## 三、🎯 今日关注个股
从量化扫描高分股中选 3-5 只逻辑最硬的，用表格输出：

| 代码/名称 | 现价 | 核心逻辑 | 催化剂 |
|-----------|------|----------|--------|
（每行不超过60字，逻辑要具体，不说"基本面好"这种废话）

## 四、⚠️ 风险提示
指出当前 1-2 个可能触发回撤的风险点（每条25字内），以及自选股中需要重点注意的持仓。

## 📋 自选股速览
对每一只自选股逐一给出一句精华点评，不得遗漏：
**名称(代码)** 现价xxx｜持有/加仓/减仓/观望 + 核心理由（15字内）

注意：直接输出报告，不要任何前缀或说明。"""

    print("🤖 调用 Gemini 生成精华日报...")
    result = _v88_call_gemini(prompt, use_grounding=False)
    if result and not result.startswith("❌"):
        return result

    print(f"⚠️  Gemini 失败，降级为纯数据: {result}")
    # ── 降级：纯数据（无 Gemini）──────────────────────────────────────────────
    fallback_lines = [f"## 📊 市场数据摘要（{today}）\n", idx_block, ""]
    if scan:
        for mkt, mlabel in [("US","🇺🇸美股"),("HK","🇭🇰港股"),("CN","🇨🇳A股")]:
            tops = scan.get(mkt,{}).get("top",[])[:3]
            if tops:
                fallback_lines.append(f"**{mlabel} Top3强势**")
                for s in tops:
                    fallback_lines.append(f"  {s.get('股票','')}({s.get('代码','')}) {s.get('现价','')}")
    fallback_lines.append("\n> 详细分析请查看 V88 AI 皇冠双核 App")
    return "\n".join(fallback_lines)


# ─── Part D: AI三大市场技术分析（美股/港股/A股 走势预测）─────────────────────

def _fetch_market_technicals(index_code, label):
    """获取指数行情 + 计算技术指标，返回文本摘要"""
    try:
        df = yf.Ticker(index_code).history(period="60d", timeout=15)
        if df is None or len(df) < 5:
            return ""
        last = df.iloc[-1]
        prev = df.iloc[-2]
        chg = (last['Close'] - prev['Close']) / prev['Close'] * 100
        ma5 = df['Close'].rolling(5).mean().iloc[-1]
        ma20 = df['Close'].rolling(20).mean().iloc[-1] if len(df) >= 20 else 0
        ma60 = df['Close'].rolling(60).mean().iloc[-1] if len(df) >= 60 else 0

        delta = df['Close'].diff()
        gain = delta.where(delta > 0, 0).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)
        rs = gain.ewm(com=13).mean() / loss.ewm(com=13).mean()
        rsi = (100 - (100 / (1 + rs))).iloc[-1]

        last5 = df.tail(5)[['Open', 'High', 'Low', 'Close', 'Volume']]
        l5_lines = []
        for idx, row in last5.iterrows():
            d = idx.strftime("%m-%d") if hasattr(idx, "strftime") else str(idx)[:5]
            l5_lines.append(f"  {d} 开{row['Open']:.2f} 高{row['High']:.2f} 低{row['Low']:.2f} 收{row['Close']:.2f}")

        return (f"{label}({index_code}) 最新: {last['Close']:.2f} 涨跌: {chg:+.2f}%\n"
                f"MA5: {ma5:.2f} MA20: {ma20:.2f} MA60: {ma60:.2f} RSI: {rsi:.1f}\n"
                f"最近5日:\n" + "\n".join(l5_lines))
    except Exception as e:
        print(f"  ⚠️ {label} 数据获取失败: {e}")
        return f"{label}: 数据获取失败"


def generate_market_ai_analysis(report_type="evening", part_a_result: str = None, part_a_summary: str = ""):
    """
    Part D: 三大市场 AI 技术分析（走势预测 + 操作建议）
    - market_data_block: 真实行情数据（yfinance）
    - part_a_summary: 由 generate_report_final() 统一提取后传入，BCD 共享
      （part_a_result 保留作向后兼容，优先使用 part_a_summary）
    早报侧重港股A股，晚报侧重美股
    """
    print("📊 Part D: 获取三大市场技术数据...")
    us_data = _fetch_market_technicals("^GSPC", "标普500")
    hk_data = _fetch_market_technicals("^HSI", "恒生指数")
    cn_data = _fetch_market_technicals("000001.SS", "上证综指")

    # ── 构建 market_data_block ────────────────────────────────────────────────
    market_data_block = (
        f"美股 · 标普500\n{us_data}\n\n"
        f"港股 · 恒生指数\n{hk_data}\n\n"
        f"A股 · 上证综指\n{cn_data}"
    )

    # ── 使用统一传入的 part_a_summary（由 generate_report_final 提取，BCD 共享）────
    # 向后兼容：若未传入 part_a_summary 但传入了 part_a_result，则降级提取
    if not part_a_summary and part_a_result:
        m = re.search(r"(##\s*🔴\s*核心事件.+?)(?=\n##|\Z)", part_a_result, re.S)
        if m:
            snippet = m.group(1).strip()
            part_a_summary = snippet[:900] if len(snippet) > 900 else snippet
        else:
            part_a_summary = part_a_result[:800].strip() + ("…" if len(part_a_result) > 800 else "")
    if not part_a_summary:
        part_a_summary = "（Part A 摘要未生成）"

    prompt = f"""【今日核心驱动事件（来自 Part A 真实新闻）】
{part_a_summary}

【技术分析规则】
1. 核心逻辑必须引用今日至少一条真实事件（来自上方摘要）
2. 不得只用技术面解释走势，必须结合新闻基本面
3. 若今日有重大地缘政治或央行事件，必须在对应市场的核心逻辑中体现

---
你是一位专业技术分析师，基于真实行情数据对三个市场做出
清晰、可执行的走势判断。

【真实行情数据（来自 yfinance）】
{market_data_block}

【任务】
对美股、港股、A股分别输出以下结构，每个市场字数控制在150-200字：

---
## 🇺🇸 美股
**方向**：涨 / 跌 / 震荡（选一个）
**概率**：上涨X% / 下跌X%（3-5个交易日）
**关键位**：支撑 XXXX | 压力 XXXX
**均线状态**：MA5 / MA20 / MA60 多头排列 / 空头排列 / 混乱
**RSI**：XX（超买>70 / 超卖<30 / 中性）
**核心逻辑**：2-3句话，说清楚为什么这个方向，必须引用今日至少一条核心事件
**操作建议**：一句话，具体到动作（持仓 / 减仓至X% / 等待X信号）

---
## 🇭🇰 港股
（同上格式）

---
## 🇨🇳 A股
（同上格式）

---
## 联动与配置
- 三市场联动判断：强联动 / 弱联动 / 独立走势
- 建议仓位：美X% / 港X% / AX%（须与 Part A 三市场动作建议中的仓位上限一致）
- 本周最高优先级：一句话

---
【仓位建议规则】
- 仓位建议必须与 Part A 三市场动作建议一致，不得自行推翻
- 最低仓位建议不得低于10%，除非 Part A 明确指出该市场存在系统性风险需要清仓
- 禁止出现 美0% / 港0% / A0% 等清零建议，除非有明确系统性风险事件支撑
- 若 Part A 动作建议不明确，按技术面给出合理区间（如"美30-50%"），不得写0%

【强制规则】
1. 方向判断必须基于上方真实行情数据，不得凭空判断
2. 支撑压力位必须是真实的价格数字，来自行情数据
3. 核心逻辑必须引用今日至少一条真实事件，不得脱离新闻
4. 三市场格式必须统一，不允许某个市场写得特别长或特别短
5. 操作建议必须具体，不允许写"保持观望"或"视情况而定"等模糊表述"""

    print("🤖 Part D: Gemini 分析三大市场走势...")
    result = _v88_call_gemini(prompt)
    if result and not result.startswith("❌"):
        print(f"  ✅ Part D 生成成功（约 {len(result)} 字）")
        return result
    print(f"  ⚠️ Part D 生成失败: {(result or '')[:80]}")
    return None


# ─── 主函数 ───────────────────────────────────────────────────────────────────

def main():
    """主函数：Part A（市场简报）+ Part B（推荐）+ Part C（自选股）
    Part D（AI市场技术分析）已停用，节省 Gemini token。
    进程锁防止 cron/watchdog 短时间内重复触发。
    """
    import fcntl, tempfile
    _lock_path = os.path.join(tempfile.gettempdir(), "auto_reporter.lock")
    _lock_fd = open(_lock_path, "w")
    try:
        fcntl.flock(_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("⚠️  auto_reporter 已在运行（进程锁），本次跳过，避免重复推送")
        _lock_fd.close()
        return

    try:
        _main_body()
    finally:
        fcntl.flock(_lock_fd, fcntl.LOCK_UN)
        _lock_fd.close()


def _main_body():
    """实际报告逻辑（由 main() 在进程锁内调用）。
    ── 新架构：一次 AI 调用生成完整报告，通过飞书推送 ──
    """
    report_type = sys.argv[1] if len(sys.argv) > 1 else (
        "evening" if datetime.now(TZ_SHANGHAI).hour >= 12 else "morning"
    )
    label     = "早报" if report_type == "morning" else "晚报"
    send_time = datetime.now(TZ_SHANGHAI).strftime('%Y/%m/%d %H:%M')

    print(f"\n{'='*60}")
    print(f"🚀 V88 AI 飞书{label}（单次AI调用，完整报告）")
    print(f"{'='*60}\n")

    # ── 单次 AI 调用生成完整报告 ─────────────────────────────────────────────
    report = generate_full_report(report_type)

    if report:
        header  = f"📰 V88 AI {label} · {send_time}\n{'='*50}\n\n"
        payload = header + report
        print(f"📤 飞书推送完整报告（约 {len(payload)} 字）...")
        ok = send_feishu(payload)
    else:
        print("⚠️  报告生成失败，跳过推送")
        ok = False

    print(f"\n{'='*60}")
    print(f"📊 {label}推送结果: {'✅ 成功' if ok else '❌ 失败'}")
    print(f"{'='*60}")

    if not ok:
        print("❌ 推送失败！")
        sys.exit(1)

    # ── 以下为旧版四次调用 + 钉钉推送（已停用，保留供回滚参考）─────────────
    # part_a, part_b, part_a_summary = generate_report_final(report_type)
    # if part_a:
    #     send_to_dingtalk(f"📰 AI{label} Part A · {send_time}", part_a, part_type="A")
    # if part_b:
    #     send_to_dingtalk(f"🎯 AI{label} Part B · {send_time}", part_b, part_type="B")
    # part_c = generate_watchlist_report(report_type, part_a_summary=part_a_summary)
    # if part_c:
    #     send_to_dingtalk(f"📋 AI{label} Part C · {send_time}", part_c, part_type="C")
    # part_d = generate_market_ai_analysis(report_type, part_a_result=part_a, part_a_summary=part_a_summary)
    # if part_d:
    #     send_to_dingtalk(f"📊 AI{label} Part D · {send_time}", part_d, part_type="D")


if __name__ == "__main__":
    main()
