#!/usr/bin/env python3
"""
quant_worker.py — 量化模拟交易后台引擎
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
运行模式:
  python quant_worker.py              # 本地运行（读写本地 quant_state.json）
  python quant_worker.py --cloud      # GitHub Actions 模式（读写 Gist）
  python quant_worker.py --force      # 忽略冷却，强制扫描

策略逻辑:
  进场: EMA20 > EMA50 且 45 < RSI(14) < 68
  离场: 止损 -5% | 止盈 +12% | 追踪止损（最高点回撤 8%）
  风控: 最多 5 个持仓 | 单仓位不超过总资金 20%
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

_CLOUD_MODE = "--cloud" in sys.argv
_FORCE      = "--force" in sys.argv

# ── 路径 ──────────────────────────────────────────────────────────
_DIR        = Path(__file__).parent
_CACHE_DIR  = _DIR / ".cache_brief"
_STATE_FILE = _CACHE_DIR / "quant_state.json"

# ── 参数 ──────────────────────────────────────────────────────────
INITIAL_CAPITAL  = 100_000.0   # 模拟初始资金（人民币等值）
MAX_POSITIONS    = 5           # 最多同时持仓数
POS_SIZE_PCT     = 0.18        # 单仓比例（占总资金）
STOP_LOSS_PCT    = -0.05       # 止损 -5%
TAKE_PROFIT_PCT  = +0.12       # 止盈 +12%
TRAIL_FROM_HIGH  = 0.08        # 追踪止损：最高点回撤 8%
KLINE_PERIOD     = "3mo"       # K 线获取周期
KLINE_INTERVAL   = "1d"        # K 线粒度

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
        "2318.HK",  # 中国平安
        "1299.HK",  # 友邦保险
    ],
    "A股": [
        "600519.SS",  # 贵州茅台
        "300750.SZ",  # 宁德时代
        "601318.SS",  # 中国平安
        "000858.SZ",  # 五粮液
        "601888.SS",  # 中国中免
    ],
}

# 可读名称映射
NAMES = {
    "NVDA": "英伟达",  "AAPL": "苹果",   "TSLA": "特斯拉", "AMZN": "亚马逊",
    "META": "Meta",    "MSFT": "微软",    "GOOGL": "谷歌",
    "0700.HK": "腾讯", "9988.HK": "阿里", "0005.HK": "汇丰",
    "2318.HK": "中国平安(H)", "1299.HK": "友邦保险",
    "600519.SS": "贵州茅台", "300750.SZ": "宁德时代", "601318.SS": "中国平安",
    "000858.SZ": "五粮液",   "601888.SS": "中国中免",
}

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

def analyze(df: pd.DataFrame) -> dict:
    """返回最新 bar 的技术指标"""
    close = df["Close"].squeeze()
    if len(close) < 50:
        return {}
    ema20 = _ema(close, 20)
    ema50 = _ema(close, 50)
    rsi   = _rsi(close, 14)
    macd, macd_sig = _macd(close)

    return {
        "price":     float(close.iloc[-1]),
        "ema20":     float(ema20.iloc[-1]),
        "ema50":     float(ema50.iloc[-1]),
        "rsi":       float(rsi.iloc[-1]),
        "macd":      float(macd.iloc[-1]),
        "macd_sig":  float(macd_sig.iloc[-1]),
        "prev_close": float(close.iloc[-2]) if len(close) >= 2 else None,
    }


# ═══════════════════════════════════════════════════════════════
# 数据获取
# ═══════════════════════════════════════════════════════════════

def fetch_kline(symbol: str) -> pd.DataFrame | None:
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


def check_exit(pos: dict, ind: dict) -> str | None:
    """检查是否触发离场条件，返回离场原因或 None"""
    price  = ind["price"]
    entry  = pos["entry_price"]
    high   = pos.get("highest_price", entry)
    pct    = (price - entry) / entry

    # 更新最高价
    if price > high:
        pos["highest_price"] = price

    if pct <= STOP_LOSS_PCT:
        return "止损"
    if pct >= TAKE_PROFIT_PCT:
        return "止盈"
    # 追踪止损
    trail_thresh = pos["highest_price"] * (1 - TRAIL_FROM_HIGH)
    if price < trail_thresh and pct > 0:
        return "追踪止损"
    return None


def handle_exits(state: dict, logs: list):
    """处理所有持仓的离场逻辑"""
    remaining = []
    for pos in state["positions"]:
        df = fetch_kline(pos["symbol"])
        if df is None or len(df) < 2:
            remaining.append(pos)
            continue
        ind = analyze(df)
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
            state["trades"] = state["trades"][:100]   # 保留最新 100 条
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
        else:
            remaining.append(pos)

    state["positions"] = remaining


def handle_entries(state: dict, logs: list):
    """扫描入场信号"""
    open_syms = {p["symbol"] for p in state["positions"]}

    for market, symbols in WATCHLIST.items():
        if len(state["positions"]) >= MAX_POSITIONS:
            break
        for sym in symbols:
            if sym in open_syms:
                continue
            if len(state["positions"]) >= MAX_POSITIONS:
                break

            df = fetch_kline(sym)
            if df is None:
                continue
            ind = analyze(df)
            if not ind:
                continue

            # 入场条件
            long_ok = (
                ind["ema20"] > ind["ema50"]
                and 45 < ind["rsi"] < 68
                and ind["macd"] > ind["macd_sig"]
            )
            if not long_ok:
                log.info(f"  {sym} 无信号  EMA20{'>'if ind['ema20']>ind['ema50'] else '<'}EMA50"
                         f"  RSI={ind['rsi']:.1f}  MACD={'✓' if ind['macd']>ind['macd_sig'] else '✗'}")
                continue

            # 计算仓位
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
                "reason": f"EMA↑ RSI={ind['rsi']:.0f} MACD✓",
            })
            log.info(f"  开仓 {sym} @ {price:.4f}  qty={quantity}  cost={cost:.2f}")
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

def main():
    log.info(f"quant_worker 启动 ({'云端' if _CLOUD_MODE else '本地'} 模式)")

    state = load_state()
    logs: list[dict] = []

    log.info(f"资金: {state['capital']:.2f}  持仓: {len(state['positions'])}  历史交易: {len(state['trades'])}")

    # Step 1: 检查离场
    log.info("─── 检查离场条件 ───")
    handle_exits(state, logs)

    # Step 2: 扫描入场
    log.info("─── 扫描入场信号 ───")
    handle_entries(state, logs)

    # Step 3: 更新净值曲线
    log.info("─── 更新净值曲线 ───")
    update_equity(state)

    # Step 4: 记录本次扫描日志（追加到全局日志，保留最近 200 条）
    if logs:
        state["scan_logs"] = (logs + state.get("scan_logs", []))[:200]
    else:
        state["scan_logs"] = state.get("scan_logs", [])
        # 无动作时也记录一行
        state["scan_logs"].insert(0, {
            "time":   _now_str(),
            "action": "SCAN",
            "symbol": "—",
            "name":   "本次扫描无信号",
            "price":  0,
            "reason": f"持仓{len(state['positions'])} 可用资金{state['capital']:.0f}",
        })
        state["scan_logs"] = state["scan_logs"][:200]

    # Step 5: 保存
    save_state(state)

    # Step 6: 钉钉摘要（有动作才发，静默扫描不打扰）
    _notify_summary(state, len(logs))

    # 打印统计
    total_eq = state["capital"]
    for p in state["positions"]:
        total_eq += p.get("cost", 0)
    profit = total_eq - INITIAL_CAPITAL
    profit_pct = profit / INITIAL_CAPITAL * 100
    log.info(
        f"✅ 扫描完成 | 总资产={total_eq:.2f} | "
        f"盈亏={profit:+.2f} ({profit_pct:+.2f}%) | "
        f"持仓={len(state['positions'])} | "
        f"本次动作={len(logs)} 条"
    )


if __name__ == "__main__":
    main()
