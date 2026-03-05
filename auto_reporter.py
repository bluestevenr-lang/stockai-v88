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
    """读取 .env 文件并写入 os.environ（不覆盖已有环境变量）"""
    env_path = Path(__file__).parent / '.env'
    if not env_path.exists():
        return
    try:
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' not in line:
                    continue
                key, _, val = line.partition('=')  # partition 只拆第一个 =，URL 安全
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:  # 不覆盖系统已有变量
                    os.environ[key] = val
    except Exception as e:
        print(f"⚠️  .env 加载失败: {e}")

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

import yfinance as yf
try:
    from google import genai as genai
    _GENAI_NEW = True
except ImportError:
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning, module="google.generativeai")
    import google.generativeai as genai  # type: ignore
    _GENAI_NEW = False
from datetime import datetime
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
PART_A_TARGET_CHARS = 5200
PART_BC_MAX_CHARS = 5200
PORTFOLIO_FILE = "my_portfolio.xlsx"

# ─── 自选股（按中美港划分，来自多账户持仓/自选，可编辑）────────────────────
WATCHLIST = {
    "US": [
        ("ABBV", "艾伯维"), ("ACMR", "ACM Research"), ("NVDA", "英伟达"), ("NVO", "诺和诺德"),
        ("VOO", "标普500ETF"), ("BRK.B", "伯克希尔"), ("QQQM", "纳指100ETF"),
        ("GOOG", "谷歌"), ("PM", "菲利普莫里斯"), ("LLY", "礼来制药"), ("TSM", "台积电"),
        ("TSLA", "特斯拉"),
    ],
    "HK": [
        ("0700.HK", "腾讯控股"), ("0883.HK", "中国海洋石油"), ("1299.HK", "友邦保险"),
        ("0941.HK", "中国移动"),
    ],
    "CN": [
        ("600519.SS", "贵州茅台"), ("688981.SS", "中芯国际"), ("601899.SS", "紫金矿业"),
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
    - 目标篇幅：4000-5000字
    """
    today = data["today"]
    _ts   = data["_ts"]
    indices = data["indices"]
    _market_status_block = data["market_status_block"]
    focus = (
        "【早报侧重】港股、A股基本面与新闻请充分展开（≥800字/市场）；美股可简要概括。三市场均须完整输出。"
        if report_type == "morning" else
        "【晚报侧重】美股基本面与新闻请充分展开（≥800字/市场）；港股、A股可简要概括。三市场均须完整输出。"
    )

    prompt = f"""你是华尔街日报首席市场记者。生成今日市场**基本面简报（Part A）**。

【职责边界】Part A 专注宏观、事件、新闻、基本面分析——**严禁输出个股推荐**（推荐在 Part B 单独输出）。

{focus}

【机构简报思维】去掉解释性文字，只保留决策变量。信息流：**事件 → 变量 → 资产影响**。

【核心使命】以下三类须按 事件|变量|资产影响 输出，缺一不可：
1) **财报**：事件（公司+日期）| 变量（预期/关键数据）| 资产影响
2) **战争/地缘政治**（**必含，优先级最高**）：美伊以/中东/俄乌/制裁/能源/供应链。若有相关新闻必须输出，不得遗漏。
3) **政治人物言论**：事件（政要/央行表态）| 变量 | 资产影响

【篇幅目标】4000-5000字。每市场 事件+动作+核心变量 合计 ≥800字，充分展开精华决策变量。**禁止解释性文字**。

【休市状态·权威数据】以下为 exchange_calendars 查询结果，**必须严格按此描述**：
{_market_status_block}

【指数数据】（括号内为实际交易日对比，必须严格引用，禁止编造）
{indices.get('US', '美股数据获取中')}
{indices.get('HK', '港股数据获取中')}
{indices.get('CN', 'A股数据获取中')}

【日期】{today} | 【校验时间】{_ts} (Asia/Shanghai)

【阅读友好·华尔街日报风格】
- 段落分开：大块之间空一行，小节之间用 `---` 分隔
- 字体粗细：标题 **粗体**，关键变量/数字加粗强调
- 符号点缀：适当使用 📰📊📈📉⚠️🔍；市场用 🇺🇸🇭🇰🇨🇳
- 层次清晰：二级标题用 ##，三级用 ###；禁止大段挤在一起

请严格按以下结构输出（不要称呼和废话）：

---

## 标题
[一句话概括当日市场核心变化，含日期]

---

## 📰 今日重大事件

**格式：事件 | 变量 | 资产影响**（每行一条，禁止展开解释）

**财报**（至少 2 条）：
- [公司][日期] | [预期/关注变量] | [资产影响]

**战争/地缘政治 + 政治人物言论**（**必含**，美伊以/中东/俄乌等若有新闻必须输出）：
- [事件] | [变量] | [资产影响]

---

## 导语
[1句变量摘要，含时间锚点，禁止展开解释]

---

## 🇺🇸 美股基本面

**📰 事件 | 变量 | 资产影响**（至少 3 条，机构简报，只输出决策变量）：
- [事件] | [变量] | [资产影响]

**📌 动作**：
- 成长仓上限 [%]
- 禁止 [标的/行为]
- 允许 [操作]

**📊 核心变量**：资金风格、核心矛盾、明日观察（禁止解释性文字）

---

## 🇭🇰 港股基本面

**📰 事件 | 变量 | 资产影响**（至少 3 条）：
- [事件] | [变量] | [资产影响]

**📌 动作**：
- [3-5条可执行指令]

**📊 核心变量**：资金风格、核心矛盾、明日观察

---

## 🇨🇳 A股基本面

**📰 事件 | 变量 | 资产影响**（至少 3 条）：
- [事件] | [变量] | [资产影响]

**📌 动作**：
- [3-5条可执行指令]

**📊 核心变量**：资金风格、核心矛盾、明日观察

---

## 📋 明日触发-动作对照
事件A → 动作X
事件B → 动作Y
事件C → 动作Z
（事件|动作，禁止展开）

---

## 数据/时间戳
数据: 雅虎财经收盘价
时间戳: {_ts} (Asia/Shanghai)
数据截点: {_ts}"""

    print("🤖 正在生成 Part A（基本面+新闻，华尔街日报版）...")
    result = _v88_call_gemini(prompt)
    if result and not result.startswith("❌"):
        return result
    print(f"⚠️  Part A 生成失败: {result[:100] if result else 'None'}")
    return None

# ─── Part B：可执行推荐（9只，独立 Gemini 调用）──────────────────────────────

def generate_part_b_recs(report_type, data):
    """
    Part B：可执行推荐，9只个股（美港A各3只）。
    - Card Schema 机构简报：动作标签、触发、机会/风险概率、建仓区间
    - 不含宏观分析（宏观已在 Part A 输出）
    - 每只推荐注入实时现价
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

    prompt = f"""你是 V88 机构交易员。生成今日**可执行推荐（Part B）**。

【职责边界】Part B 仅含 9 只个股推荐——**严禁输出宏观分析**（宏观已在 Part A 输出）。

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
- 触发: [24h/72h，一句话，含催化事件]
- 机会/风险: [X%/Y%] · 建仓区间: [具体价格区间]
- 理由: 变量→预期差→价格位置→验证窗口（一行，30字以内）
**禁止输出**：来源、失效条件、仓位上限、R/R、证据状态灯、解释性段落。

【V2.1 Action Gate】立即建仓 仅当以下全满足，否则自动降级：
a) 触发时效 ≤ 72h
b) 有明确催化（财报/事件/技术突破）
c) R/R ≥ 2.0（内部判定，不输出）

【日期】{today} | 【校验时间】{_ts} (Asia/Shanghai)

请严格按以下结构输出（不要称呼和废话）：

---

## 可执行推荐

### 🇺🇸 美股（3只：1 立即建仓 + 1 中期跟进 + 1 观察）

1. **[名称(代码)]** · **立即建仓** · 现价 $X.XX
   - 触发: [24h/72h 一句话，含催化]
   - 机会/风险: [X%/Y%] · 建仓区间: [$X.XX–$X.XX]

2. **[名称(代码)]** · **中期跟进** · 现价 $X.XX
   - 触发: [一句话]
   - 机会/风险: [X%/Y%] · 建仓区间: [$X.XX–$X.XX]

3. **[名称(代码)]** · **观察** · 现价 $X.XX
   - [升级条件，一句话]

---

### 🇭🇰 港股（3只：1 立即建仓 + 1 中期跟进 + 1 观察）

1. **[名称(代码)]** · **立即建仓** · 现价 HK$X.XX
   - 触发: [一句话]
   - 机会/风险: [X%/Y%] · 建仓区间: [HK$X.XX–HK$X.XX]

2. **[名称(代码)]** · **中期跟进** · 现价 HK$X.XX
   - 触发: [一句话]
   - 机会/风险: [X%/Y%] · 建仓区间: [HK$X.XX–HK$X.XX]

3. **[名称(代码)]** · **观察** · 现价 HK$X.XX
   - [升级条件，一句话]

---

### 🇨🇳 A股（3只：1 立即建仓 + 1 中期跟进 + 1 观察）

1. **[名称(代码)]** · **立即建仓** · 现价 ¥X.XX
   - 触发: [一句话]
   - 机会/风险: [X%/Y%] · 建仓区间: [¥X.XX–¥X.XX]

2. **[名称(代码)]** · **中期跟进** · 现价 ¥X.XX
   - 触发: [一句话]
   - 机会/风险: [X%/Y%] · 建仓区间: [¥X.XX–¥X.XX]

3. **[名称(代码)]** · **观察** · 现价 ¥X.XX
   - [升级条件，一句话]

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

def generate_watchlist_report(report_type="evening"):
    """
    Part C：自选股持仓分析。
    每只持仓一张卡片：事件 | 变量 | 资产影响 | 操作建议（持仓/加仓/减仓/观望）
    """
    total = sum(len(v) for v in WATCHLIST.values())
    if total == 0:
        return None
    try:
        print(f"📋 正在生成 Part C（自选股持仓分析，{total}只）...")
        _watchlist_prices = _get_watchlist_prices()
        now_sh = datetime.now(TZ_SHANGHAI)
        today = now_sh.strftime("%Y年%m月%d日")
        _ts = now_sh.strftime("%Y-%m-%d %H:%M:%S")
        focus = (
            "早报侧重港股、A股持仓分析；美股持仓可简要。"
            if report_type == "morning" else
            "晚报侧重美股持仓分析；港股、A股持仓可简要。"
        )

        prompt = f"""你是 V88 持仓分析师。生成今日**自选股持仓分析（Part C）**。

【职责边界】每只持仓股出一张独立分析卡片，聚焦「当前该不该动、如何动」——**机构简报思维，零废话**。

【日期】{today} | 【数据截点】{_ts} (Asia/Shanghai)
【侧重】{focus}

【自选股列表】（必须每只都有输出）
- 🇺🇸 美股持仓：{', '.join(f"{n}({c})" for c, n in WATCHLIST.get('US', []))}
- 🇭🇰 港股持仓：{', '.join(f"{n}({c})" for c, n in WATCHLIST.get('HK', []))}
- 🇨🇳 A股持仓：{', '.join(f"{n}({c})" for c, n in WATCHLIST.get('CN', []))}

【自选股现价】（雅虎财经，供参考）
- 美股：{chr(10).join('  - ' + s for s in _watchlist_prices.get('US', []))}
- 港股：{chr(10).join('  - ' + s for s in _watchlist_prices.get('HK', []))}
- A股：{chr(10).join('  - ' + s for s in _watchlist_prices.get('CN', []))}

【卡片规则】每只股票 = 一张卡片，格式固定：
1) **事件**：该股相关事件（财报/公告/催化/技术信号），≤30字
2) **变量**：关键决策变量（业绩预期/技术位/资金面），≤30字
3) **资产影响**：对标的的影响结论，≤40字
4) **📌 操作**：四选一且必须明确——📌持仓 | 📈加仓 | 📉减仓 | 🔍观望
5) 技术破位、基本面恶化、风险升温、估值过高时，必须输出 📉减仓
6) 操作建议须保持连续性：无明确新催化，24h内不得频繁切换

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

**事件** | **变量** | **资产影响**
[事件≤30字] | [变量≤30字] | [资产影响≤40字]

---

（每只按上述格式输出，前后空行分隔）

---

### 🇭🇰 港股持仓

---

#### ⭐ 腾讯控股(0700.HK) | 现价 HK$X.XX | [操作]

**事件** | **变量** | **资产影响**
[事件] | [变量] | [资产影响]

---

（每只港股同上格式）

---

### 🇨🇳 A股持仓

---

#### ⭐ 贵州茅台(600519.SS) | 现价 ¥X.XX | [操作]

**事件** | **变量** | **资产影响**
[事件] | [变量] | [资产影响]

---

（每只A股同上格式）

---

*数据: 雅虎财经 | 时间戳: {_ts} (Asia/Shanghai)*"""

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

def generate_report_final(report_type="evening"):
    """
    主报告生成：共用一次市场数据准备，然后分别调用 Gemini 生成 Part A 和 Part B。
    返回 (part_a_content, part_b_content)，失败时对应项为 None。
    """
    try:
        print(f"🤖 正在准备市场数据（800池筛选，约 2-5 分钟）...")
        data = _prepare_market_data(report_type)

        index_banner = _build_index_banner()

        part_a = generate_part_a_wsj(report_type, data)
        if part_a:
            part_a = index_banner + part_a

        time.sleep(4)   # 避免 Gemini API 连续调用触发 429/403
        part_b = generate_part_b_recs(report_type, data)
        if part_b:
            part_b = _strip_part_b_verbose(part_b)

        return part_a, part_b
    except Exception as e:
        print(f"⚠️  主报告生成异常: {e}")
        return None, None

# ─── 精华摘要（富内容版，1次 Gemini 调用）──────────────────────────────────

def _load_scan_results():
    """从 scan_results.json 读取扫描结果，返回各市场完整数据"""
    try:
        cache_path = Path(__file__).parent / ".cache_brief" / "scan_results.json"
        if not cache_path.exists():
            return {}
        data = json.loads(cache_path.read_text(encoding="utf-8"))
        out = {}
        for mkt in ("US", "HK", "CN"):
            mdata = data.get(mkt, {})
            out[mkt] = {
                "top":        mdata.get("top", [])[:5],
                "coil":       mdata.get("coil", [])[:3],
                "breakout":   mdata.get("breakout", [])[:3],
                "inflection": mdata.get("inflection", [])[:3],
                "bm_ret5":    mdata.get("bm_ret5", 0),
            }
        return out
    except Exception:
        return {}


def _sector_stats(scan):
    """统计行业强弱：top+breakout出现频次=连续上升；coil频次=蓄势积累"""
    from collections import Counter
    rising  = Counter()
    coiling = Counter()
    for mkt in ("US", "HK", "CN"):
        d = scan.get(mkt, {})
        for s in d.get("top", []) + d.get("breakout", []):
            ind = s.get("行业", "").strip()
            if ind:
                rising[ind] += 1
        for s in d.get("coil", []):
            ind = s.get("行业", "").strip()
            if ind:
                coiling[ind] += 1
    return rising.most_common(5), coiling.most_common(4)


def _fmt_stock_row(s, pfx=""):
    """格式化单只股票：名称(代码) 现价 | 形态+理由 | 建议"""
    name    = s.get("股票", "")
    code    = s.get("代码", "")
    price   = s.get("现价", "N/A")
    reason  = s.get("理由", "")
    suggest = s.get("建议", "")
    form    = s.get("形态", "")
    return f"**{name}({code})** {pfx}{price}｜{form} {reason}｜{suggest}"


def _fmt_watchlist_with_scan(scan):
    """自选股现价，并标记是否进入V88扫描强势/蓄势榜"""
    scan_index = {}
    for mkt in ("US", "HK", "CN"):
        d = scan.get(mkt, {})
        for cat, cat_label in [("top","强势🔥"),("coil","蓄势⚡"),("breakout","突破🚀"),("inflection","拐点🔄")]:
            for s in d.get(cat, []):
                c = s.get("代码","").upper().strip()
                if c:
                    scan_index[c] = (cat_label, s.get("理由",""), s.get("建议",""))

    lines  = []
    alerts = []
    for mkt, stocks in WATCHLIST.items():
        pfx = "$" if mkt == "US" else ("HK$" if mkt == "HK" else "¥")
        for code, name in stocks:
            p = _v88_fetch_price(code)
            price_str  = f"{pfx}{p:.2f}" if p else "获取中"
            code_upper = code.upper().strip()
            if code_upper in scan_index:
                cat_label, reason, suggest = scan_index[code_upper]
                lines.append(f"  ⚡{name}({code}) {price_str} [{cat_label}] {reason}")
                alerts.append(f"**{name}({code})** {price_str}｜{cat_label}进榜｜{suggest}")
            else:
                lines.append(f"  {name}({code}) {price_str}")
    return "\n".join(lines) if lines else "  数据获取中", alerts


def generate_digest(report_type="morning"):
    """
    5板块精华日报，全部基于V88真实数据：
    ① 主要市场表现  ② 精选行业强弱  ③ Top3强势+蓄势
    ④ 自选股提示筛选  ⑤ AI精选（Gemini提炼）
    """
    now_sh = datetime.now(TZ_SHANGHAI)
    today  = now_sh.strftime("%Y年%m月%d日 %H:%M")
    label  = "早报" if report_type == "morning" else "晚报"
    mkt_focus = "亚市（港股+A股开市）" if report_type == "morning" else "美市（美股开盘）"

    # ── ① 主要市场表现 ────────────────────────────────────────────────────────
    print("  📈 获取指数数据...")
    sp500  = _v88_index_change("^GSPC",     "标普500")
    nasdaq = _v88_index_change("^IXIC",     "纳斯达克")
    hsi    = _v88_index_change("^HSI",      "恒生指数")
    sse    = _v88_index_change("000001.SS", "上证综指")

    sec1 = (
        "## 一、📊 主要市场表现\n"
        f"{sp500}\n{nasdaq}\n{hsi}\n{sse}"
    )

    # ── ② 精选行业强弱 ────────────────────────────────────────────────────────
    print("  🏭 分析行业数据...")
    scan = _load_scan_results()
    if scan:
        rising_sectors, coiling_sectors = _sector_stats(scan)
        rising_str  = "  ".join(f"{ind}({n}只)" for ind, n in rising_sectors[:4]) or "暂无"
        coiling_str = "  ".join(f"{ind}({n}只)" for ind, n in coiling_sectors[:3]) or "暂无"
    else:
        rising_str = coiling_str = "扫描数据未就绪"

    sec2 = (
        "## 二、🏭 精选行业强弱\n"
        f"🔥 **连续上升**：{rising_str}\n"
        f"⚡ **蓄势积累**：{coiling_str}"
    )

    # ── ③ Top3强势 + Top3蓄势（各市场）──────────────────────────────────────
    print("  🎯 整理Top3强势/蓄势...")
    sec3_lines = ["## 三、🎯 Top3强势 & Top3蓄势"]
    for mkt, flag, pfx in [("US","🇺🇸 美股","$"), ("HK","🇭🇰 港股","HK$"), ("CN","🇨🇳 A股","¥")]:
        d   = scan.get(mkt, {}) if scan else {}
        bm  = d.get("bm_ret5", 0)
        top3  = d.get("top", [])[:3]
        coil3 = d.get("coil", [])[:3]
        sec3_lines.append(f"\n{flag}（基准5日{bm:+.1f}%）")
        sec3_lines.append("**强势Top3**")
        for s in top3:
            sec3_lines.append(_fmt_stock_row(s, pfx))
        if not top3:
            sec3_lines.append("  暂无数据")
        sec3_lines.append("**蓄势Top3**")
        for s in coil3:
            sec3_lines.append(_fmt_stock_row(s, pfx))
        if not coil3:
            sec3_lines.append("  暂无数据")
    sec3 = "\n".join(sec3_lines)

    # ── ④ 自选股提示筛选 ──────────────────────────────────────────────────────
    print("  📋 获取自选股现价...")
    wl_all, wl_alerts = _fmt_watchlist_with_scan(scan if scan else {})

    if wl_alerts:
        sec4 = (
            "## 四、⚡ 自选股提示筛选\n"
            "🚨 **进榜提醒** — 以下持仓今日进入V88扫描榜：\n"
            + "\n".join(wl_alerts)
            + "\n\n**全部持仓现价：**\n" + wl_all
        )
    else:
        sec4 = (
            "## 四、📋 自选股持仓现价\n"
            + wl_all
            + "\n（今日无持仓进入强势/蓄势扫描榜）"
        )

    # ── ⑤ AI日报精选（Gemini仅做精华提炼，不编造数据）────────────────────────
    print("🤖 调用 Gemini 提炼AI精选...")
    top_stocks_for_prompt = []
    for mkt, flag in [("US","美股"), ("HK","港股"), ("CN","A股")]:
        d = scan.get(mkt, {}) if scan else {}
        for s in d.get("top", [])[:3]:
            top_stocks_for_prompt.append(
                f"{flag} {s.get('股票','')}({s.get('代码','')}) "
                f"现价{s.get('现价','')} {s.get('理由','')} 建议:{s.get('建议','')}"
            )

    prompt = f"""你是机构策略师，当前时间 {today}（{label}，{mkt_focus}）。
基于以下V88量化扫描真实数据，写"AI日报精选"板块，语言精炼如高盛晨报，禁止编造任何数据：

指数：{sp500} | {nasdaq} | {hsi} | {sse}
连续上升行业：{rising_str}
强势个股（含系统理由）：
{"chr(10)".join(top_stocks_for_prompt) or "暂无"}
自选股进榜：{'、'.join([a.split('｜')[0].replace('**','') for a in wl_alerts]) if wl_alerts else '无'}

严格只输出以下格式，不要任何前缀说明：

## 五、🧠 AI日报精选
**今日核心判断**：（一句话，≤40字，直接点出最关键机会或风险）

**精选3只最强逻辑**（仅从上方数据中选，格式：名称(代码) — 核心逻辑≤20字）：
1. 
2. 
3. 

**风险提示**（2条，每条≤20字，基于数据而非泛泛）：
- 
- """

    ai_section = _v88_call_gemini(prompt, use_grounding=False)
    if not ai_section or ai_section.startswith("❌"):
        ai_section = "## 五、🧠 AI日报精选\n> Gemini 暂时不可用，请查看 V88 App 获取完整分析。"

    # ── 组装完整报告 ──────────────────────────────────────────────────────────
    kw = DINGTALK_KEYWORD or "股票行情"
    report = (
        f"【AI股市{label}】{kw}\n\n"
        f"{sec1}\n\n"
        f"{sec2}\n\n"
        f"{sec3}\n\n"
        f"{sec4}\n\n"
        f"{ai_section}\n\n"
        f"---\n*V88 AI 皇冠双核 · {today} · {kw}*"
    )
    return report


if __name__ == "__main__":
    main()
