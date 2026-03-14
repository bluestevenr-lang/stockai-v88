#!/usr/bin/env python3
"""
quant_worker.py — 兼容性入口（v2.0 shim）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
此文件保留仅为兼容 VPS cron 和 GitHub Actions 的调用命令不变。
策略逻辑已全部迁移至 main.py（量化策略 v2.0 模块化架构）。

运行方式（透传给 main.py）：
  python quant_worker.py --cloud           # VPS cron 云端模式
  python quant_worker.py --once --cloud    # 等价上面
  python quant_worker.py --report --cloud  # 手动日报
  python quant_worker.py --force           # 强制扫描（忽略交易时段）

专业策略:
  进场: EMA20>EMA50(5m) + EMA20>EMA50(1h多周期) + 50<RSI<65 + MACD金叉零轴上
        + 市场环境过滤（大盘在200MA上方）+ 黑名单过滤
  止损: ATR动态止损（2×ATR14）而非固定比例
  止盈: 无固定止盈，让利润奔跑
  追踪: 分层追踪止损（盈利越多，保护越紧）
        0-5%盈利 → 回撤8%止损
        5-15%盈利 → 回撤6%止损
        15-30%盈利 → 回撤5%止损
        30%+盈利  → 回撤4%止损
  风控: 最多5仓 | 单仓18% | 连亏3次黑名单14天

累计学习:
  每笔交易结束后更新统计
  周维度分析：胜率/最优标的/最优时段/参数建议
  每日日报包含学习总结
"""

import os, sys, json, time, logging
import warnings
from pathlib import Path
from datetime import datetime, timezone

import hmac
import base64
import hashlib
import urllib.parse
import urllib.request
import ssl

import pandas as pd
import numpy as np
import yfinance as yf
import requests

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [QUANT] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_CLOUD_MODE    = "--cloud" in sys.argv
_FORCE         = "--force" in sys.argv
_DAILY_REPORT  = "--daily-report" in sys.argv  # 每日日志总结推送钉钉

# ── 路径 ──────────────────────────────────────────────────────────
_DIR        = Path(__file__).parent
_CACHE_DIR  = _DIR / ".cache_brief"
_STATE_FILE = _CACHE_DIR / "quant_state.json"

# ── 参数 ──────────────────────────────────────────────────────────
INITIAL_CAPITAL      = 100_000.0  # 模拟初始资金
MAX_POSITIONS        = 5          # 最多同时持仓
POS_SIZE_PCT         = 0.18       # 单仓占总资金比例
KLINE_PERIOD         = "5d"       # 5分钟K线周期
KLINE_INTERVAL       = "5m"       # K线粒度
KLINE_1H_PERIOD      = "1mo"      # 1小时K线（多周期确认）
KLINE_1H_INTERVAL    = "1h"
REGIME_PERIOD        = "1y"       # 市场环境K线周期（判断大盘趋势）

# ATR 动态止损
ATR_STOP_MULT        = 2.0        # 止损 = 入场价 - ATR × 2.0
ATR_PERIOD           = 14

# 分层追踪止损（盈利越多保护越紧）
TRAIL_TIERS = [
    (0.05, 0.08),          # 盈利 0~5%    → 回撤 8% 止损
    (0.15, 0.06),          # 盈利 5~15%   → 回撤 6% 止损
    (0.30, 0.05),          # 盈利 15~30%  → 回撤 5% 止损
    (float("inf"), 0.04),  # 盈利 30%+    → 回撤 4% 止损
]

# RSI 入场区间
RSI_LOW, RSI_HIGH    = 50, 65

# 黑名单：连亏 N 次 → 冷静 M 天
BLACKLIST_LOSS_COUNT = 3
BLACKLIST_DAYS       = 14

# 市场基准（用于环境过滤）
REGIME_BENCHMARKS = {
    "美股": "SPY",
    "港股": "^HSI",
    "A股":  "000300.SS",
}

# ── 关注股票池（覆盖用户持仓 + 热门标的）────────────────────────────
WATCHLIST = {
    "美股": [
        "NVDA",  # 英伟达
        "AAPL",  # 苹果
        "TSLA",  # 特斯拉
        "AMZN",  # 亚马逊
        "META",  # Meta
        "MSFT",  # 微软
        "GOOGL", # 谷歌
    ],
    "港股": [
        "0700.HK",  # 腾讯
        "9988.HK",  # 阿里巴巴
        "0005.HK",  # 汇丰
        "0836.HK",  # 华润电力
        "2318.HK",  # 中国平安
        "1299.HK",  # 友邦保险
        "3690.HK",  # 美团
    ],
    "A股": [
        "600519.SS",  # 贵州茅台
        "300750.SZ",  # 宁德时代
        "601318.SS",  # 中国平安
        "000858.SZ",  # 五粮液
        "601888.SS",  # 中国中免
        "300014.SZ",  # 亿纬锂能
    ],
}

# 可读名称映射
NAMES = {
    "NVDA": "英伟达",  "AAPL": "苹果",   "TSLA": "特斯拉", "AMZN": "亚马逊",
    "META": "Meta",    "MSFT": "微软",    "GOOGL": "谷歌",
    "0700.HK": "腾讯", "9988.HK": "阿里", "0005.HK": "汇丰",
    "0836.HK": "华润电力", "2318.HK": "中国平安(H)", "1299.HK": "友邦保险",
    "3690.HK": "美团",
    "600519.SS": "贵州茅台", "300750.SZ": "宁德时代", "601318.SS": "中国平安",
    "000858.SZ": "五粮液",   "601888.SS": "中国中免", "300014.SZ": "亿纬锂能",
}

# ═══════════════════════════════════════════════════════════════
# 交易时段检测
# ═══════════════════════════════════════════════════════════════

def _is_trading_time() -> tuple[bool, str]:
    """
    判断当前是否处于三市场任意一个交易时段（北京时间）。
    返回 (is_open, market_name)。
    """
    now_utc = datetime.now(timezone.utc)
    weekday = now_utc.weekday()   # 0=Mon … 4=Fri

    if weekday >= 5:   # 周六/日全部休市
        return False, "周末休市"

    # 转换为北京时间（UTC+8）
    bj_hour   = (now_utc.hour + 8) % 24
    bj_minute = now_utc.minute
    bj_time   = bj_hour * 60 + bj_minute   # 分钟数

    # A股 09:15-11:35 / 13:00-15:05（含集合竞价缓冲）
    cn_am = (9 * 60 + 15) <= bj_time <= (11 * 60 + 35)
    cn_pm = (13 * 60)      <= bj_time <= (15 * 60 + 5)

    # 港股 09:15-12:05 / 13:00-16:10
    hk_am = (9 * 60 + 15)  <= bj_time <= (12 * 60 + 5)
    hk_pm = (13 * 60)       <= bj_time <= (16 * 60 + 10)

    # 美股 22:15-05:15 次日（北京时间跨午夜）
    us_open = bj_time >= (22 * 60 + 15) or bj_time <= (5 * 60 + 15)

    if cn_am or cn_pm:
        return True, "A股"
    if hk_am or hk_pm:
        return True, "港股"
    if us_open:
        return True, "美股"
    return False, "全部休市"


# ═══════════════════════════════════════════════════════════════
# 钉钉通知
# ═══════════════════════════════════════════════════════════════

_DINGTALK_WEBHOOK = os.environ.get("DINGTALK_WEBHOOK", "")
_DINGTALK_SECRET  = os.environ.get("DINGTALK_SECRET", "")
_DINGTALK_KEYWORD = os.environ.get("DINGTALK_KEYWORD", "股票行情")


def _send_dingtalk(title: str, content: str) -> bool:
    """发送 Markdown 消息到钉钉（复用 dingtalk_bot.py 逻辑）"""
    if not _DINGTALK_WEBHOOK:
        return False
    try:
        url = _DINGTALK_WEBHOOK
        if _DINGTALK_SECRET:
            ts   = str(round(time.time() * 1000))
            sign = urllib.parse.quote_plus(
                base64.b64encode(
                    hmac.new(
                        _DINGTALK_SECRET.encode("utf-8"),
                        f"{ts}\n{_DINGTALK_SECRET}".encode("utf-8"),
                        digestmod=hashlib.sha256,
                    ).digest()
                ).decode("ascii")
            )
            url = f"{_DINGTALK_WEBHOOK}&timestamp={ts}&sign={sign}"

        safe_title = title if _DINGTALK_KEYWORD in title else f"{_DINGTALK_KEYWORD} {title}"
        msg = {
            "msgtype": "markdown",
            "markdown": {
                "title": f"🤖 {safe_title}",
                "text": f"### 🤖 {safe_title}\n\n{content}\n\n---\n*量化模拟 · V88*",
            },
        }
        data = json.dumps(msg, ensure_ascii=False).encode("utf-8")
        ctx  = ssl._create_unverified_context()
        req  = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            ok = result.get("errcode") == 0
            log.info(f"钉钉{'✅' if ok else '❌'}: {result}")
            return ok
    except Exception as e:
        log.warning(f"钉钉发送失败: {e}")
        return False


def _notify_open(pos: dict, ind: dict):
    sym    = pos["symbol"]
    name   = pos.get("name", sym)
    market = pos.get("market", "")
    price  = pos["entry_price"]
    qty    = pos["quantity"]
    cost   = pos["cost"]
    stop   = round(price * (1 + STOP_LOSS_PCT), 4)
    tp     = round(price * (1 + TAKE_PROFIT_PCT), 4)
    rsi    = ind.get("rsi", 0)

    content = (
        f"**市场**: {market}  \n"
        f"**标的**: {name}（{sym}）  \n"
        f"**操作**: 🟢 开仓（做多）  \n"
        f"**价格**: {price}  \n"
        f"**数量**: {qty} 股  \n"
        f"**成本**: ¥{cost:,.2f}  \n"
        f"**止损**: {stop}（−{abs(STOP_LOSS_PCT)*100:.0f}%）  \n"
        f"**止盈**: {tp}（+{TAKE_PROFIT_PCT*100:.0f}%）  \n"
        f"**信号**: EMA金叉 · RSI={rsi:.1f} · MACD✓  \n"
        f"**时间**: {_now_str()}"
    )
    _send_dingtalk("量化开仓信号", content)


def _notify_close(pos: dict, exit_price: float, pnl: float, reason: str):
    sym    = pos["symbol"]
    name   = pos.get("name", sym)
    market = pos.get("market", "")
    entry  = pos["entry_price"]
    qty    = pos["quantity"]
    pnl_pct = (exit_price - entry) / entry * 100
    emoji  = "🟢" if pnl >= 0 else "🔴"

    content = (
        f"**市场**: {market}  \n"
        f"**标的**: {name}（{sym}）  \n"
        f"**操作**: {emoji} 平仓（{reason}）  \n"
        f"**入场**: {entry}  \n"
        f"**出场**: {exit_price}  \n"
        f"**数量**: {qty} 股  \n"
        f"**盈亏**: {emoji} ¥{pnl:+,.2f}（{pnl_pct:+.2f}%）  \n"
        f"**时间**: {_now_str()}"
    )
    _send_dingtalk("量化平仓通知", content)


def _notify_summary(state: dict, action_count: int):
    """每次扫描结束后发送状态摘要（仅有动作时发）"""
    if action_count == 0:
        return
    initial  = state.get("initial_capital", INITIAL_CAPITAL)
    cash     = state.get("capital", initial)
    pos_val  = sum(p.get("cost", 0) for p in state.get("positions", []))
    total    = cash + pos_val
    profit   = total - initial
    pct      = profit / initial * 100
    emoji    = "📈" if profit >= 0 else "📉"

    content = (
        f"**本次动作**: {action_count} 条  \n"
        f"**当前持仓**: {len(state.get('positions', []))} 只  \n"
        f"**可用资金**: ¥{cash:,.2f}  \n"
        f"**账户总值**: ¥{total:,.2f}  \n"
        f"**累计盈亏**: {emoji} ¥{profit:+,.2f}（{pct:+.2f}%）  \n"
        f"**时间**: {_now_str()}"
    )
    _send_dingtalk("量化账户快报", content)


def _send_daily_report(state: dict):
    """每晚 21:00 发送当日量化交易日志总结到钉钉"""
    initial   = state.get("initial_capital", INITIAL_CAPITAL)
    cash      = state.get("capital", initial)
    positions = state.get("positions", [])
    trades    = state.get("trades", [])
    equity_h  = state.get("equity_history", [])

    # 账户总值
    pos_val   = sum(p.get("cost", 0) for p in positions)
    total     = cash + pos_val
    profit    = total - initial
    pct       = profit / initial * 100
    emoji_pnl = "📈" if profit >= 0 else "📉"

    # 今日交易记录（今天日期）
    today = datetime.now().strftime("%Y-%m-%d")
    today_trades = [
        t for t in trades
        if t.get("exit_time", "").startswith(today)
        or t.get("entry_time", "").startswith(today)
    ]
    today_closed = [t for t in today_trades if t.get("exit_time", "").startswith(today)]
    today_pnl    = sum(t.get("pnl", 0) for t in today_closed)

    # 净值曲线最近7天
    recent_eq = equity_h[-7:] if len(equity_h) >= 2 else equity_h
    eq_trend  = "  ".join(
        f"{e['date'][5:]}={'📈' if i==0 or e['equity']>=recent_eq[i-1]['equity'] else '📉'}{e['equity']:,.0f}"
        for i, e in enumerate(recent_eq)
    )

    # 当前持仓列表
    if positions:
        pos_lines = "\n".join(
            f"> {p.get('market','')} **{p.get('name', p['symbol'])}** "
            f"入场 {p['entry_price']} · 数量 {p['quantity']}"
            for p in positions
        )
    else:
        pos_lines = "> 暂无持仓"

    # 今日成交列表
    if today_closed:
        trade_lines = "\n".join(
            f"> {'🟢' if t.get('pnl',0)>=0 else '🔴'} "
            f"**{t.get('name', t['symbol'])}** "
            f"{t['entry_price']}→{t['exit_price']} "
            f"¥{t.get('pnl',0):+,.2f}（{t.get('exit_reason','')}）"
            for t in today_closed[:10]
        )
    else:
        trade_lines = "> 今日无平仓记录"

    # 胜率统计
    won  = sum(1 for t in trades if t.get("pnl", 0) > 0)
    wr   = won / len(trades) * 100 if trades else 0

    # 学习摘要
    learning      = state.get("learning", _init_learning())
    learn_summary = _learning_summary(learning)

    content = (
        f"**{today} 量化交易日报**\n\n"
        f"---\n\n"
        f"**📊 账户概览**  \n"
        f"> 总资产：¥{total:,.2f}  \n"
        f"> 累计盈亏：{emoji_pnl} ¥{profit:+,.2f}（{pct:+.2f}%）  \n"
        f"> 今日盈亏：{'🟢' if today_pnl>=0 else '🔴'} ¥{today_pnl:+,.2f}  \n"
        f"> 历史胜率：{wr:.1f}%（共{len(trades)}笔）  \n\n"
        f"---\n\n"
        f"**📂 当前持仓（{len(positions)}只）**  \n"
        f"{pos_lines}  \n\n"
        f"---\n\n"
        f"**📜 今日成交（{len(today_closed)}笔）**  \n"
        f"{trade_lines}  \n\n"
        f"---\n\n"
        f"**📈 近期净值**  \n"
        f"> {eq_trend}  \n\n"
        f"---\n\n"
        f"{learn_summary}\n\n"
        f"---\n"
        f"*策略：5m EMA金叉+1H共振+ATR止损+分层追踪+市场过滤*"
    )
    _send_dingtalk("量化交易日报", content)
    log.info("✅ 量化日报（含学习总结）已发送到钉钉")


# ═══════════════════════════════════════════════════════════════
# 技术指标计算
# ═══════════════════════════════════════════════════════════════

def _ema(series: pd.Series, n: int) -> pd.Series:
    return series.ewm(span=n, adjust=False).mean()

def _rsi(series: pd.Series, n: int = 14) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)
    avg_gain = gain.ewm(com=n - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=n - 1, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def _macd(series: pd.Series):
    fast = _ema(series, 12)
    slow = _ema(series, 26)
    macd = fast - slow
    signal = _ema(macd, 9)
    return macd, signal

def _atr(df: pd.DataFrame, n: int = ATR_PERIOD) -> pd.Series:
    """ATR 真实波幅均值"""
    h = df["High"].squeeze()
    l = df["Low"].squeeze()
    c = df["Close"].squeeze()
    tr = pd.concat([
        h - l,
        (h - c.shift(1)).abs(),
        (l - c.shift(1)).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(com=n - 1, adjust=False).mean()


def analyze(df: pd.DataFrame, realtime_price: float | None = None) -> dict:
    """
    返回最新 bar 的技术指标 + ATR。
    realtime_price: 优先用实时报价替换 K 线末 bar 收盘价。
    """
    close = df["Close"].squeeze()
    if len(close) < 50:
        return {}
    ema20          = _ema(close, 20)
    ema50          = _ema(close, 50)
    rsi            = _rsi(close, 14)
    macd, macd_sig = _macd(close)
    atr_series     = _atr(df)

    price = realtime_price if (realtime_price and realtime_price > 0) else float(close.iloc[-1])

    return {
        "price":        price,
        "price_source": "realtime" if (realtime_price and realtime_price > 0) else "kline",
        "ema20":        float(ema20.iloc[-1]),
        "ema50":        float(ema50.iloc[-1]),
        "rsi":          float(rsi.iloc[-1]),
        "macd":         float(macd.iloc[-1]),
        "macd_sig":     float(macd_sig.iloc[-1]),
        "atr":          float(atr_series.iloc[-1]),
        "prev_close":   float(close.iloc[-2]) if len(close) >= 2 else None,
    }


def analyze_1h(symbol: str) -> dict:
    """获取1小时级别趋势方向（多周期确认）"""
    try:
        tk = yf.Ticker(symbol)
        df = tk.history(period=KLINE_1H_PERIOD, interval=KLINE_1H_INTERVAL, auto_adjust=True)
        if df is None or len(df) < 20:
            return {"trend": "unknown"}
        close  = df["Close"].squeeze()
        ema20h = _ema(close, 20)
        ema50h = _ema(close, 50)
        return {
            "trend":   "up" if ema20h.iloc[-1] > ema50h.iloc[-1] else "down",
            "ema20h":  float(ema20h.iloc[-1]),
            "ema50h":  float(ema50h.iloc[-1]),
        }
    except Exception:
        return {"trend": "unknown"}


def check_market_regime(market: str) -> bool:
    """
    市场环境过滤：大盘在200日均线上方才允许做多。
    unknown → 宽松处理，允许交易。
    """
    benchmark = REGIME_BENCHMARKS.get(market)
    if not benchmark:
        return True
    try:
        tk = yf.Ticker(benchmark)
        df = tk.history(period=REGIME_PERIOD, interval="1d", auto_adjust=True)
        if df is None or len(df) < 200:
            return True
        close  = df["Close"].squeeze()
        ma200  = close.rolling(200).mean()
        is_bull = float(close.iloc[-1]) > float(ma200.iloc[-1])
        log.info(f"  市场环境 [{market}] {benchmark}: {'牛市✅' if is_bull else '熊市⚠️'}")
        return is_bull
    except Exception:
        return True   # 获取失败时宽松处理


# ═══════════════════════════════════════════════════════════════
# 数据获取
# ═══════════════════════════════════════════════════════════════

# Yahoo Finance 请求头（与 V88 app 完全一致，避免被屏蔽）
_YF_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://finance.yahoo.com/",
}


def fetch_realtime_price(symbol: str) -> float | None:
    """
    通过 Yahoo Finance v8 API 获取最新实时报价（与 V88 同源）。
    meta.regularMarketPrice 是当前最新成交价，无需等 5 分钟 bar 收盘。
    """
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        resp = requests.get(
            url,
            params={"interval": "1m", "range": "1d", "includePrePost": "false"},
            headers=_YF_HEADERS,
            timeout=10,
            verify=False,
        )
        data = resp.json()
        result = data.get("chart", {}).get("result")
        if not result:
            return None
        meta = result[0].get("meta", {})
        price = meta.get("regularMarketPrice") or meta.get("previousClose")
        return float(price) if price else None
    except Exception as e:
        log.warning(f"  实时价格获取失败 {symbol}: {e}")
        return None


def fetch_kline(symbol: str) -> pd.DataFrame | None:
    """获取 K 线数据用于指标计算（EMA/RSI/MACD）"""
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=KLINE_PERIOD, interval=KLINE_INTERVAL, auto_adjust=True)
        if df is None or len(df) < 50:
            return None
        return df
    except Exception as e:
        log.warning(f"  K线获取失败 {symbol}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
# Gist 读写
# ═══════════════════════════════════════════════════════════════

def _gist_read() -> dict | None:
    """从 Gist 读取 quant_state.json"""
    gist_id    = os.environ.get("GIST_ID", "")
    gist_token = os.environ.get("GIST_TOKEN", "")
    if not gist_id:
        return None
    try:
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "StockAI-QuantWorker",
        }
        if gist_token:
            headers["Authorization"] = f"token {gist_token}"
        resp = requests.get(
            f"https://api.github.com/gists/{gist_id}",
            headers=headers, timeout=15,
        )
        files = resp.json().get("files", {})
        fdata = files.get("quant_state.json")
        if not fdata:
            return None
        return json.loads(fdata.get("content", "{}"))
    except Exception as e:
        log.warning(f"Gist 读取失败: {e}")
        return None


def _gist_write(state: dict) -> bool:
    """把 quant_state.json 写入 Gist"""
    gist_id    = os.environ.get("GIST_ID", "")
    gist_token = os.environ.get("GIST_TOKEN", "")
    if not gist_id or not gist_token:
        log.warning("GIST_TOKEN / GIST_ID 未配置，跳过上传")
        return False
    try:
        content = json.dumps(state, ensure_ascii=False, default=str)
        resp = requests.patch(
            f"https://api.github.com/gists/{gist_id}",
            headers={
                "Authorization": f"token {gist_token}",
                "Accept": "application/vnd.github+json",
            },
            json={"files": {"quant_state.json": {"content": content}}},
            timeout=30,
        )
        if resp.status_code == 200:
            log.info(f"✅ 量化状态已上传到 Gist")
            return True
        else:
            log.error(f"Gist 写入失败: {resp.status_code}")
            return False
    except Exception as e:
        log.error(f"Gist 写入异常: {e}")
        return False


# ═══════════════════════════════════════════════════════════════
# 状态加载 / 保存
# ═══════════════════════════════════════════════════════════════

def _empty_state() -> dict:
    return {
        "timestamp":       time.time(),
        "capital":         INITIAL_CAPITAL,
        "initial_capital": INITIAL_CAPITAL,
        "positions":       [],      # 当前持仓
        "trades":          [],      # 历史成交记录（最近 100 条）
        "scan_logs":       [],      # 本次扫描行为日志
        "equity_history":  [        # 净值曲线（每次扫描追加）
            {"date": datetime.now().strftime("%Y-%m-%d"), "equity": INITIAL_CAPITAL}
        ],
    }


def load_state() -> dict:
    """优先读 Gist（云端），降级读本地文件，最后用初始状态"""
    state = None

    if _CLOUD_MODE:
        state = _gist_read()
        if state:
            log.info("✅ 从 Gist 加载量化状态")

    if not state and _STATE_FILE.exists():
        try:
            state = json.loads(_STATE_FILE.read_text(encoding="utf-8"))
            log.info("✅ 从本地文件加载量化状态")
        except Exception:
            pass

    if not state:
        log.info("⚪ 初始化全新量化状态")
        state = _empty_state()

    # 字段向后兼容补丁
    state.setdefault("capital", INITIAL_CAPITAL)
    state.setdefault("initial_capital", INITIAL_CAPITAL)
    state.setdefault("positions", [])
    state.setdefault("trades", [])
    state.setdefault("scan_logs", [])
    state.setdefault("equity_history", [])
    return state


def save_state(state: dict):
    state["timestamp"] = time.time()
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, default=str), encoding="utf-8"
    )
    if _CLOUD_MODE:
        _gist_write(state)


# ═══════════════════════════════════════════════════════════════
# 交易逻辑
# ═══════════════════════════════════════════════════════════════

def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def _layered_trail_pct(pnl_pct: float) -> float:
    """根据当前盈利百分比返回对应的追踪止损回撤幅度"""
    for threshold, trail in TRAIL_TIERS:
        if pnl_pct < threshold:
            return trail
    return TRAIL_TIERS[-1][1]


def check_exit(pos: dict, ind: dict) -> str | None:
    """
    专业离场逻辑：
    1. ATR 动态硬止损（绝对保护底线，入场时计算好）
    2. 分层追踪止损（让利润奔跑，无固定止盈上限）
    """
    price  = ind["price"]
    entry  = pos["entry_price"]
    high   = pos.get("highest_price", entry)
    pct    = (price - entry) / entry

    # 更新最高价
    if price > high:
        pos["highest_price"] = price

    # 1. ATR 硬止损（入场时计算并存入 pos）
    atr_stop = pos.get("atr_stop", entry * (1 - 0.08))  # 降级：无ATR时用-8%
    if price <= atr_stop:
        return f"ATR止损(-{(entry-price)/entry*100:.1f}%)"

    # 2. 分层追踪止损（只在盈利时保护）
    if pct > 0.01:   # 至少盈利1%才启动追踪
        trail_pct    = _layered_trail_pct(pct)
        trail_thresh = pos["highest_price"] * (1 - trail_pct)
        if price < trail_thresh:
            return f"追踪止损(回撤{trail_pct*100:.0f}%,盈利{pct*100:.1f}%)"

    return None


def handle_exits(state: dict, logs: list):
    """处理所有持仓的离场逻辑"""
    remaining = []
    for pos in state["positions"]:
        df = fetch_kline(pos["symbol"])
        if df is None or len(df) < 2:
            remaining.append(pos)
            continue
        rt_price = fetch_realtime_price(pos["symbol"])
        ind = analyze(df, rt_price)
        if not ind:
            remaining.append(pos)
            continue

        reason = check_exit(pos, ind)
        if reason:
            price = ind["price"]
            qty   = pos["quantity"]
            pnl   = (price - pos["entry_price"]) * qty
            pnl_pct = (price - pos["entry_price"]) / pos["entry_price"] * 100

            trade_rec = {
                "id":          f"T{int(time.time()*1000)%100000:05d}",
                "symbol":      pos["symbol"],
                "name":        pos.get("name", pos["symbol"]),
                "market":      pos.get("market", ""),
                "entry_price": pos["entry_price"],
                "exit_price":  price,
                "quantity":    qty,
                "entry_time":  pos.get("entry_time", ""),
                "exit_time":   _now_str(),
                "pnl":         round(pnl, 2),
                "pnl_pct":     round(pnl_pct, 2),
                "exit_reason": reason,
            }
            state["trades"].insert(0, trade_rec)
            state["trades"] = state["trades"][:100]
            state["capital"] += pnl
            logs.append({
                "time":   _now_str(),
                "action": "CLOSE",
                "symbol": pos["symbol"],
                "name":   pos.get("name", pos["symbol"]),
                "price":  round(price, 4),
                "pnl":    round(pnl, 2),
                "reason": reason,
            })
            log.info(f"  平仓 {pos['symbol']} @ {price:.4f}  {reason}  PnL={pnl:+.2f}")
            _notify_close(pos, price, pnl, reason)
            # 更新累计学习数据
            update_learning(state, trade_rec)
        else:
            remaining.append(pos)

    state["positions"] = remaining


# ═══════════════════════════════════════════════════════════════
# 累计学习模块
# ═══════════════════════════════════════════════════════════════

def _init_learning() -> dict:
    return {
        "symbol_stats":  {},   # {sym: {trades, wins, total_pnl, avg_hold_hours}}
        "market_stats":  {},   # {market: {trades, wins, total_pnl}}
        "hour_stats":    {},   # {hour_str: {trades, wins}}
        "rsi_stats":     {},   # {rsi_bucket: {trades, wins}}
        "blacklist":     {},   # {sym: {reason, until, count}}
        "consecutive_losses": {},  # {sym: count}
        "last_updated":  "",
        "total_trades":  0,
        "total_wins":    0,
    }


def update_learning(state: dict, trade: dict):
    """每笔交易关闭后更新累计学习数据"""
    learning = state.setdefault("learning", _init_learning())
    sym    = trade.get("symbol", "")
    market = trade.get("market", "")
    pnl    = trade.get("pnl", 0)
    is_win = pnl > 0
    rsi_entry = trade.get("rsi_at_entry", 0)

    # 总计
    learning["total_trades"] = learning.get("total_trades", 0) + 1
    learning["total_wins"]   = learning.get("total_wins", 0) + (1 if is_win else 0)

    # 标的统计
    ss = learning["symbol_stats"].setdefault(sym, {"trades": 0, "wins": 0, "total_pnl": 0.0})
    ss["trades"]    += 1
    ss["wins"]      += 1 if is_win else 0
    ss["total_pnl"] += pnl

    # 市场统计
    ms = learning["market_stats"].setdefault(market, {"trades": 0, "wins": 0, "total_pnl": 0.0})
    ms["trades"]    += 1
    ms["wins"]      += 1 if is_win else 0
    ms["total_pnl"] += pnl

    # 时段统计（入场小时）
    try:
        hour_str = trade.get("entry_time", "")[:13].split(" ")[-1]  # "HH"
        hs = learning["hour_stats"].setdefault(hour_str, {"trades": 0, "wins": 0})
        hs["trades"] += 1
        hs["wins"]   += 1 if is_win else 0
    except Exception:
        pass

    # RSI 区间统计
    if rsi_entry > 0:
        bucket = f"{int(rsi_entry//5)*5}-{int(rsi_entry//5)*5+5}"
        rs = learning["rsi_stats"].setdefault(bucket, {"trades": 0, "wins": 0})
        rs["trades"] += 1
        rs["wins"]   += 1 if is_win else 0

    # 黑名单：连亏处理
    cl = learning.setdefault("consecutive_losses", {})
    if is_win:
        cl[sym] = 0
    else:
        cl[sym] = cl.get(sym, 0) + 1
        if cl[sym] >= BLACKLIST_LOSS_COUNT:
            until = (datetime.now() + __import__("datetime").timedelta(days=BLACKLIST_DAYS)).strftime("%Y-%m-%d")
            learning["blacklist"][sym] = {
                "reason": f"连亏{cl[sym]}次",
                "until":  until,
                "count":  cl[sym],
            }
            log.info(f"  ⚫ {sym} 加入黑名单至 {until}（连亏{cl[sym]}次）")

    learning["last_updated"] = _now_str()


def is_blacklisted(sym: str, learning: dict) -> bool:
    """检查标的是否在黑名单有效期内"""
    bl = learning.get("blacklist", {})
    if sym not in bl:
        return False
    until = bl[sym].get("until", "")
    today = datetime.now().strftime("%Y-%m-%d")
    if today >= until:
        del bl[sym]   # 自动解除
        return False
    return True


def _learning_summary(learning: dict) -> str:
    """生成学习模块文字摘要（用于日报）"""
    total  = learning.get("total_trades", 0)
    wins   = learning.get("total_wins", 0)
    wr     = wins / total * 100 if total > 0 else 0

    lines = [f"**📚 累计学习（共{total}笔交易 · 胜率{wr:.1f}%）**\n"]

    # 最优标的 Top3
    ss = learning.get("symbol_stats", {})
    if ss:
        ranked = sorted(ss.items(), key=lambda x: x[1]["total_pnl"], reverse=True)
        top3   = ranked[:3]
        worst1 = ranked[-1] if len(ranked) >= 1 else None
        lines.append("**🏆 最优标的**")
        for sym, d in top3:
            wr_s = d["wins"] / d["trades"] * 100 if d["trades"] else 0
            lines.append(f"> {NAMES.get(sym, sym)} 胜率{wr_s:.0f}% 累计¥{d['total_pnl']:+,.0f}")
        if worst1 and worst1[1]["total_pnl"] < 0:
            sym, d = worst1
            lines.append(f"**⚠️ 最差标的**: {NAMES.get(sym, sym)} 累计¥{d['total_pnl']:+,.0f}")

    # 最优市场
    ms = learning.get("market_stats", {})
    if ms:
        best_mkt = max(ms.items(), key=lambda x: x[1]["total_pnl"])
        lines.append(f"**🌍 最佳市场**: {best_mkt[0]} 累计¥{best_mkt[1]['total_pnl']:+,.0f}")

    # 最优入场时段
    hs = learning.get("hour_stats", {})
    if hs:
        best_hr = max(hs.items(), key=lambda x: x[1]["wins"] / max(x[1]["trades"], 1))
        wr_h = best_hr[1]["wins"] / best_hr[1]["trades"] * 100 if best_hr[1]["trades"] else 0
        lines.append(f"**⏰ 最佳入场时段**: {best_hr[0]}点 胜率{wr_h:.0f}%")

    # 最优RSI区间
    rs = learning.get("rsi_stats", {})
    if rs:
        best_rsi = max(rs.items(), key=lambda x: x[1]["wins"] / max(x[1]["trades"], 1))
        wr_r = best_rsi[1]["wins"] / best_rsi[1]["trades"] * 100 if best_rsi[1]["trades"] else 0
        lines.append(f"**📊 最佳RSI区间**: RSI {best_rsi[0]} 胜率{wr_r:.0f}%")

    # 黑名单
    bl = learning.get("blacklist", {})
    if bl:
        bl_names = [f"{NAMES.get(s, s)}(至{d['until']})" for s, d in bl.items()]
        lines.append(f"**⚫ 黑名单**: {', '.join(bl_names)}")

    # 建议（基于数据）
    if total >= 10:
        lines.append("\n**💡 系统建议**")
        if wr < 45:
            lines.append("> 整体胜率偏低，建议提高入场RSI阈值至55以上")
        elif wr > 65:
            lines.append("> 胜率良好，可考虑适当扩大仓位比例")
        if len(bl) >= 2:
            lines.append("> 多标的连续亏损，当前市场环境偏弱，建议减少开仓频率")
    elif total > 0:
        lines.append(f"\n> *数据积累中（{total}/10笔），建议积累10笔以上再参考*")
    else:
        lines.append("\n> *暂无交易数据，周一开盘后开始积累*")

    return "\n".join(lines)


def handle_entries(state: dict, logs: list):
    """专业版入场扫描：ATR止损 + 多周期确认 + 市场环境 + 黑名单过滤"""
    open_syms = {p["symbol"] for p in state["positions"]}
    learning  = state.setdefault("learning", _init_learning())

    # 缓存市场环境（每次扫描只查一次）
    regime_cache: dict[str, bool] = {}

    for market, symbols in WATCHLIST.items():
        if len(state["positions"]) >= MAX_POSITIONS:
            break

        # 市场环境检查（牛市才做多）
        if market not in regime_cache:
            regime_cache[market] = check_market_regime(market)
        if not regime_cache[market]:
            log.info(f"  [{market}] 大盘熊市，跳过该市场所有标的")
            continue

        for sym in symbols:
            if sym in open_syms:
                continue
            if len(state["positions"]) >= MAX_POSITIONS:
                break

            # 黑名单过滤
            if is_blacklisted(sym, learning):
                bl_info = learning["blacklist"].get(sym, {})
                log.info(f"  {sym} ⚫黑名单至{bl_info.get('until','?')}，跳过")
                continue

            # 5分钟K线 + 实时价格
            df = fetch_kline(sym)
            if df is None:
                continue
            rt_price = fetch_realtime_price(sym)
            ind = analyze(df, rt_price)
            if not ind:
                continue

            # 5分钟入场条件
            sig_5m = (
                ind["ema20"] > ind["ema50"]
                and RSI_LOW < ind["rsi"] < RSI_HIGH
                and ind["macd"] > ind["macd_sig"]
                and ind["macd"] > 0
            )
            if not sig_5m:
                src = "实时" if ind.get("price_source") == "realtime" else "K线"
                log.info(f"  {sym} 无5m信号  EMA{'↑'if ind['ema20']>ind['ema50'] else '↓'}"
                         f"  RSI={ind['rsi']:.1f}  MACD={'✓'if ind['macd']>0 else '✗'}"
                         f"  价格={ind['price']:.4f}[{src}]")
                continue

            # 1小时多周期确认
            tf1h = analyze_1h(sym)
            if tf1h["trend"] == "down":
                log.info(f"  {sym} 1H趋势向下，跳过（多周期不共振）")
                continue

            # 计算 ATR 动态止损价
            atr_val  = ind.get("atr", ind["price"] * 0.01)
            atr_stop = ind["price"] - ATR_STOP_MULT * atr_val

            # 仓位计算
            budget   = state["capital"] * POS_SIZE_PCT
            price    = ind["price"]
            if price <= 0:
                continue
            quantity = max(1, int(budget / price))
            cost     = quantity * price
            if cost > state["capital"] * 0.9:
                log.info(f"  {sym} 资金不足，跳过")
                continue

            pos = {
                "symbol":        sym,
                "name":          NAMES.get(sym, sym),
                "market":        market,
                "entry_price":   price,
                "entry_time":    _now_str(),
                "quantity":      quantity,
                "cost":          round(cost, 2),
                "highest_price": price,
                "atr_stop":      round(atr_stop, 4),  # ATR动态止损价
                "rsi_at_entry":  round(ind["rsi"], 1),
                "atr_at_entry":  round(atr_val, 4),
            }
            state["positions"].append(pos)
            state["capital"] -= cost
            open_syms.add(sym)
            logs.append({
                "time":   _now_str(),
                "action": "OPEN",
                "symbol": sym,
                "name":   NAMES.get(sym, sym),
                "price":  round(price, 4),
                "reason": f"EMA↑ RSI={ind['rsi']:.0f} MACD✓ 1H共振 ATR止损={atr_stop:.2f}",
            })
            log.info(f"  开仓 {sym} @ {price:.4f}  qty={quantity}  ATR止损={atr_stop:.4f}")
            _notify_open(pos, ind)


def update_equity(state: dict):
    """追加当日净值快照（含持仓市值）"""
    market_val = 0.0
    for pos in state["positions"]:
        df = fetch_kline(pos["symbol"])
        if df is not None and len(df) >= 1:
            ind = analyze(df)
            if ind:
                market_val += ind["price"] * pos["quantity"]
            else:
                market_val += pos["cost"]
        else:
            market_val += pos["cost"]

    total = state["capital"] + market_val
    today = datetime.now().strftime("%Y-%m-%d")

    hist = state["equity_history"]
    if hist and hist[-1]["date"] == today:
        hist[-1]["equity"] = round(total, 2)
    else:
        hist.append({"date": today, "equity": round(total, 2)})

    state["equity_history"] = hist[-180:]   # 保留最近 180 天


# ═══════════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# 兼容性 shim：所有逻辑已迁移至 main.py（量化策略 v2.0 模块化架构）
# VPS cron / GitHub Actions 仍可用原命令调用，此处透传参数到 main.py
# ─────────────────────────────────────────────────────────────────────────────

def main():
    from main import main as _main_v2
    _main_v2()


if __name__ == "__main__":
    main()
