"""
main.py — 量化策略 v2.1 主程序入口

运行方式：
  python main.py --once          # 扫描一次后退出（VPS cron 调用）
  python main.py --once --cloud  # 扫描一次 + 同步 Gist（VPS 云端模式）
  python main.py --report        # 生成今日日报并发送钉钉
  python main.py --weekly        # 手动生成周报
  python main.py                 # 常驻后台，每5分钟自动扫描

环境变量（在 .env.quant 中配置）：
  GIST_TOKEN          GitHub Token（gist 权限）
  GIST_ID             Gist 文件 ID
  DINGTALK_WEBHOOK    钉钉机器人 webhook
  DINGTALK_SECRET     钉钉加签密钥

v2.1 新增：
  · 每次扫描后将 filter_events 写入 filter_stats.json
  · 每次扫描后将扫描摘要写入 scan_log.json
  · 平仓时保存 mfe_pct / mae_pct 到 trades.json
  · 日报传入 scan_log + filter_stats 用于策略验证统计节
  · 每周日 21:00 自动发送周报
"""

from __future__ import annotations
import argparse
import base64
import hashlib
import hmac
import json
import logging
import os
import time
import urllib.parse
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
import schedule

from config import INITIAL_CAPITAL, SCAN_INTERVAL_SEC, REPORT_TIME, SYMBOL_NAMES
from risk_manager import RiskManager
from scanner import Scanner
from metrics import (
    load_trades, save_trade,
    generate_daily_report, generate_weekly_report,
    # v2.1 新增 I/O
    append_scan_entry, update_filter_stats,
    load_scan_log, load_filter_stats,
)

# ─────────────────────────────────────────────
# 目录 & 日志
# ─────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
Path("data").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(
            f"logs/quant_{date.today()}.log", encoding="utf-8"
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("main")

# ─────────────────────────────────────────────
# 状态文件路径
# ─────────────────────────────────────────────
POSITIONS_FILE  = Path("data/positions.json")
FILTER_LOG_FILE = Path("data/filter_log.json")   # raw event log（兼容旧版）


# ─────────────────────────────────────────────
# 本地 JSON 持久化
# ─────────────────────────────────────────────

def load_positions() -> dict:
    if POSITIONS_FILE.exists():
        return json.loads(POSITIONS_FILE.read_text())
    return {}


def save_positions(positions: dict):
    POSITIONS_FILE.write_text(
        json.dumps(positions, indent=2, ensure_ascii=False)
    )


def load_filter_log() -> list:
    if FILTER_LOG_FILE.exists():
        return json.loads(FILTER_LOG_FILE.read_text())
    return []


def append_filter_log(entry: dict):
    log = load_filter_log()
    log.append(entry)
    FILTER_LOG_FILE.write_text(json.dumps(log[-2000:], indent=2))


# ─────────────────────────────────────────────
# Gist 同步（V88 页面读取用）
# ─────────────────────────────────────────────
GIST_TOKEN = os.environ.get("GIST_TOKEN", "")
GIST_ID    = os.environ.get("GIST_ID", "")


def _gist_push(state: dict):
    if not GIST_TOKEN or not GIST_ID:
        logger.debug("[GIST] 未配置，跳过同步")
        return
    try:
        payload = {
            "files": {
                "quant_state.json": {
                    "content": json.dumps(state, ensure_ascii=False, indent=2)
                }
            }
        }
        resp = requests.patch(
            f"https://api.github.com/gists/{GIST_ID}",
            headers={
                "Authorization": f"token {GIST_TOKEN}",
                "Accept": "application/vnd.github.v3+json",
            },
            json=payload,
            timeout=15,
        )
        resp.raise_for_status()
        logger.info("[GIST] 状态已同步")
    except Exception as e:
        logger.warning(f"[GIST] 同步失败: {e}")


def _build_gist_state(positions: dict, trades: list, risk_summary: dict,
                      scan_log: list = None, filter_stats: dict = None) -> dict:
    """组装兼容 V88 quant_sim.py 的 JSON 格式，v2.1 新增统计字段"""
    pos_list = []
    for sym, p in positions.items():
        pos_list.append({
            "symbol":      sym,
            "name":        SYMBOL_NAMES.get(sym, sym),
            "shares":      p.get("shares", 0),
            "entry_price": p.get("entry_price", 0),
            "entry_time":  p.get("entry_time", ""),
            "peak_price":  p.get("peak_price", p.get("entry_price", 0)),
            "floor_price": p.get("floor_price", p.get("entry_price", 0)),
            "atr_stop":    p.get("atr_stop", 0),
            "market":      p.get("market", ""),
        })

    closed    = [t for t in trades if t.get("closed")]
    total_pnl = sum(t.get("pnl", 0) for t in closed)
    wins      = [t for t in closed if t.get("pnl", 0) > 0]
    win_rate  = round(len(wins) / len(closed) * 100, 1) if closed else 0.0

    # v2.1: 附带信号密度摘要（供 V88 页面展示）
    from metrics import calc_signal_density
    density_summary = {}
    if scan_log:
        d = calc_signal_density(scan_log)
        density_summary = {
            "total_scans":      d.get("total_scans", 0),
            "total_signals":    d.get("total_signals", 0),
            "trigger_rate_pct": d.get("trigger_rate_pct", 0),
            "vacancy_rate_pct": d.get("vacancy_rate_pct", 100),
        }

    return {
        "positions":          pos_list,
        "trades":             trades[-200:],
        "capital":            INITIAL_CAPITAL,
        "total_pnl":          round(total_pnl, 2),
        "win_rate":           win_rate,
        "total_trades":       len(closed),
        "risk_summary":       risk_summary,
        "signal_density":     density_summary,
        "updated_at":         datetime.now().isoformat(),
        "strategy_version":   "v2.1",
    }


# ─────────────────────────────────────────────
# 钉钉通知
# ─────────────────────────────────────────────
DINGTALK_WEBHOOK = os.environ.get("DINGTALK_WEBHOOK", "")
DINGTALK_SECRET  = os.environ.get("DINGTALK_SECRET", "")


def _sign_dingtalk() -> str:
    timestamp  = str(round(time.time() * 1000))
    secret_enc = DINGTALK_SECRET.encode("utf-8")
    hmac_code  = hmac.new(
        secret_enc,
        f"{timestamp}\n{DINGTALK_SECRET}".encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    return f"&timestamp={timestamp}&sign={sign}"


def send_dingtalk(content: str, title: str = "量化策略 v2.1"):
    if not DINGTALK_WEBHOOK:
        logger.debug("[DT] webhook 未配置，跳过")
        return
    try:
        url = DINGTALK_WEBHOOK
        if DINGTALK_SECRET:
            url += _sign_dingtalk()
        body = {
            "msgtype":  "markdown",
            "markdown": {"title": title, "text": content},
        }
        resp = requests.post(url, json=body, timeout=10)
        resp.raise_for_status()
        logger.info(f"[DT] 消息已发送: {title}")
    except Exception as e:
        logger.warning(f"[DT] 推送失败: {e}")


def _notify_open(sig: dict):
    sym      = sig["symbol"]
    name     = SYMBOL_NAMES.get(sym, sym)
    price    = sig["price"]
    stop     = sig["atr_stop"]
    stop_pct = round((price - stop) / price * 100, 2)
    text = (
        f"## 🟢 量化开仓信号\n"
        f"**{name}** ({sym})  \n"
        f"- 市场：{sig['market']}  \n"
        f"- 开仓价：**{price:.4f}**  \n"
        f"- 股数：{sig['shares']} 股  \n"
        f"- ATR止损价：{stop:.4f}（-{stop_pct}%）  \n"
        f"- 信号：L1-L6 全部通过  \n"
        f"- 时间：{sig['timestamp'][:19]}  \n"
    )
    send_dingtalk(text, title=f"开仓 {name}")


def _notify_close(sig: dict, pos: dict):
    sym     = sig["symbol"]
    name    = SYMBOL_NAMES.get(sym, sym)
    price   = sig["price"]
    entry   = pos["entry_price"]
    pnl     = sig.get("pnl", 0.0)
    pnl_pct = round((price - entry) / entry * 100, 2)
    mfe     = sig.get("mfe_pct")
    mae     = sig.get("mae_pct")
    emoji   = "🔴" if pnl < 0 else "💰"

    mfe_line = f"- 期间最大浮盈：{mfe:+.2f}%  \n" if mfe is not None else ""
    mae_line = f"- 期间最大浮亏：{mae:+.2f}%  \n" if mae is not None else ""

    text = (
        f"## {emoji} 量化平仓通知\n"
        f"**{name}** ({sym})  \n"
        f"- 出场原因：**{sig['reason']}**  \n"
        f"- 出场价：{price:.4f}  \n"
        f"- 成本价：{entry:.4f}  \n"
        f"- 收益：**{pnl_pct:+.2f}%** | ¥{pnl:+.0f}  \n"
        f"{mfe_line}"
        f"{mae_line}"
        f"- 时间：{sig['timestamp'][:19]}  \n"
    )
    send_dingtalk(text, title=f"平仓 {name} {pnl_pct:+.1f}%")


# ─────────────────────────────────────────────
# 旧格式数据迁移
# ─────────────────────────────────────────────
OLD_STATE_FILE = Path("data/quant_state.json")


def _migrate_old_state():
    if not OLD_STATE_FILE.exists():
        return
    if POSITIONS_FILE.exists():
        logger.debug("[MIGRATE] v2 positions.json 已存在，跳过")
        return
    try:
        old = json.loads(OLD_STATE_FILE.read_text())
        old_positions = old.get("positions", [])
        old_trades    = old.get("trades",    [])

        new_pos = {}
        for p in old_positions:
            sym = p.get("symbol")
            if sym:
                new_pos[sym] = {
                    "shares":      p.get("shares", 0),
                    "entry_price": p.get("entry_price", 0),
                    "entry_time":  p.get("entry_time", ""),
                    "peak_price":  p.get("peak_price", p.get("entry_price", 0)),
                    "floor_price": p.get("floor_price", p.get("entry_price", 0)),
                    "atr_stop":    p.get("atr_stop", p.get("entry_price", 0) * 0.92),
                    "market":      p.get("market", ""),
                }
        save_positions(new_pos)

        for t in old_trades:
            t.setdefault("closed", bool(t.get("exit_price")))
        from metrics import TRADES_FILE
        TRADES_FILE.parent.mkdir(parents=True, exist_ok=True)
        TRADES_FILE.write_text(json.dumps(old_trades, indent=2, ensure_ascii=False))

        OLD_STATE_FILE.rename(OLD_STATE_FILE.with_suffix(".json.bak"))
        logger.info(f"[MIGRATE] 迁移完成，{len(new_pos)} 持仓 / {len(old_trades)} 历史交易")
    except Exception as e:
        logger.warning(f"[MIGRATE] 失败（继续用新格式）: {e}")


# ─────────────────────────────────────────────
# 核心扫描任务
# ─────────────────────────────────────────────
def run_scan(cloud: bool = False, force: bool = False):
    logger.info("═" * 50)
    logger.info(f"开始扫描 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")

    trades    = load_trades()
    positions = load_positions()
    risk      = RiskManager(INITIAL_CAPITAL)
    scanner   = Scanner(risk, positions, trades)

    signals = scanner.scan_all(force=force)

    # ── v2.1: 扫描完成后立即持久化统计数据 ────
    if scanner._filter_events or scanner._scan_stats:
        # 1. 写入 filter_stats.json（按日聚合）
        update_filter_stats(scanner._filter_events)

        # 2. 写入 scan_log.json（每次扫描一条记录）
        total_scanned = sum(v[0] for v in scanner._scan_stats.values())
        total_signals = sum(v[1] for v in scanner._scan_stats.values())
        append_scan_entry({
            "ts":        datetime.now().isoformat(),
            "date":      str(date.today()),
            "scanned":   total_scanned,
            "signals":   total_signals,
            "by_market": {k: v for k, v in scanner._scan_stats.items()},
        })

    # 3. 写入 market_status.json（指数 vs MA200，供日报大盘状态行读取）
    if scanner._market_status:
        MARKET_STATUS_FILE = Path("data/market_status.json")
        MARKET_STATUS_FILE.write_text(
            json.dumps(
                {"updated_at": datetime.now().isoformat(), "status": scanner._market_status},
                indent=2, ensure_ascii=False,
            )
        )

    # ── 处理开仓 / 平仓信号 ──────────────────
    for sig in signals:
        symbol = sig["symbol"]
        action = sig["action"]

        if action == "BUY":
            positions[symbol] = {
                "shares":      sig["shares"],
                "entry_price": sig["price"],
                "entry_time":  sig["timestamp"],
                "peak_price":  sig["price"],
                "floor_price": sig["price"],    # v2.1: 初始化谷值
                "atr_stop":    sig["atr_stop"],
                "market":      sig["market"],
            }
            logger.info(f"[OPEN] {symbol} {sig['shares']}股 @ {sig['price']:.4f}")
            save_trade({
                "symbol":      symbol,
                "market":      sig["market"],
                "entry_price": sig["price"],
                "shares":      sig["shares"],
                "entry_time":  sig["timestamp"],
                "atr_stop":    sig["atr_stop"],
                "closed":      False,
            })
            _notify_open(sig)

        elif action == "SELL" and symbol in positions:
            pos = positions.pop(symbol)
            pnl = sig.get("pnl", 0.0)
            logger.info(
                f"[CLOSE] {symbol} @ {sig['price']:.4f} | {sig['reason']} | PnL: ¥{pnl:+.0f}"
            )
            _notify_close(sig, pos)

            # 更新 trades.json（加入 mfe_pct / mae_pct / floor_price）
            trades = load_trades()
            for t in reversed(trades):
                if t["symbol"] == symbol and not t.get("closed"):
                    t.update({
                        "exit_price":  sig["price"],
                        "exit_time":   sig["timestamp"],
                        "exit_date":   str(date.today()),
                        "reason":      sig["reason"],
                        "pnl":         round(pnl, 2),
                        "closed":      True,
                        # v2.1: 信号质量追踪
                        "mfe_pct":     sig.get("mfe_pct"),
                        "mae_pct":     sig.get("mae_pct"),
                        "peak_price":  pos.get("peak_price"),
                        "floor_price": pos.get("floor_price"),
                    })
                    break

            from metrics import TRADES_FILE
            TRADES_FILE.write_text(json.dumps(trades, indent=2, ensure_ascii=False))
            risk.record_trade_pnl(pnl, symbol)

    save_positions(positions)

    if positions:
        logger.info(f"当前持仓 {len(positions)} 只: {list(positions.keys())}")
    else:
        logger.info("当前无持仓")

    total_pos = len(positions)
    logger.info(f"扫描完成 | 持仓={total_pos} | 动作={len(signals)} 条")

    if cloud:
        trades       = load_trades()
        risk_summary = risk.daily_summary()
        scan_log     = load_scan_log()
        state        = _build_gist_state(positions, trades, risk_summary, scan_log)
        _gist_push(state)


# ─────────────────────────────────────────────
# 日报任务
# ─────────────────────────────────────────────
def run_daily_report(cloud: bool = False):
    logger.info("生成日报...")
    trades       = load_trades()
    filter_log   = load_filter_log()    # 旧 raw log（兼容）
    scan_log     = load_scan_log()      # v2.1
    filter_stats = load_filter_stats()  # v2.1
    risk         = RiskManager(INITIAL_CAPITAL)

    risk.end_of_day()
    summary = risk.daily_summary()

    report = generate_daily_report(
        trades=trades,
        risk_summary=summary,
        filter_log=filter_log,
        capital=INITIAL_CAPITAL,
        scan_log=scan_log,
        filter_stats=filter_stats,
    )

    print("\n" + report)
    report_path = Path(f"logs/report_{date.today()}.txt")
    report_path.write_text(report, encoding="utf-8")
    logger.info(f"日报已保存至 {report_path}")

    # 主日报（绩效 + 策略验证统计，放入代码块确保格式对齐）
    send_dingtalk(
        f"## 📊 量化策略 v2.1 日报 {date.today()}\n\n```\n{report}\n```",
        title=f"量化日报 {date.today()}",
    )

    if cloud:
        positions = load_positions()
        state     = _build_gist_state(positions, trades, summary, scan_log)
        _gist_push(state)


# ─────────────────────────────────────────────
# 周报任务（每周日 21:00）
# ─────────────────────────────────────────────
def run_weekly_report(cloud: bool = False):
    logger.info("生成周报...")
    trades       = load_trades()
    scan_log     = load_scan_log()
    filter_stats = load_filter_stats()

    report = generate_weekly_report(
        trades=trades,
        scan_log=scan_log,
        filter_stats=filter_stats,
    )

    print("\n" + report)
    today = date.today()
    report_path = Path(f"logs/weekly_{today}.txt")
    report_path.write_text(report, encoding="utf-8")
    logger.info(f"周报已保存至 {report_path}")

    send_dingtalk(
        f"## 📈 量化策略 v2.1 周报\n\n```\n{report}\n```",
        title=f"量化周报 {today}",
    )


# ─────────────────────────────────────────────
# 旧格式数据迁移（quant_state.json → v2 格式）
# ─────────────────────────────────────────────

def _migrate_old_state():
    if not OLD_STATE_FILE.exists():
        return
    if POSITIONS_FILE.exists():
        logger.debug("[MIGRATE] v2 positions.json 已存在，跳过迁移")
        return
    try:
        old = json.loads(OLD_STATE_FILE.read_text())
        old_positions = old.get("positions", [])
        old_trades    = old.get("trades", [])

        new_pos = {}
        for p in old_positions:
            sym = p.get("symbol")
            if sym:
                new_pos[sym] = {
                    "shares":      p.get("shares", 0),
                    "entry_price": p.get("entry_price", 0),
                    "entry_time":  p.get("entry_time", ""),
                    "peak_price":  p.get("peak_price", p.get("entry_price", 0)),
                    "floor_price": p.get("floor_price", p.get("entry_price", 0)),
                    "atr_stop":    p.get("atr_stop", p.get("entry_price", 0) * 0.92),
                    "market":      p.get("market", ""),
                }
        save_positions(new_pos)

        for t in old_trades:
            t.setdefault("closed", bool(t.get("exit_price")))
        from metrics import TRADES_FILE
        TRADES_FILE.parent.mkdir(parents=True, exist_ok=True)
        TRADES_FILE.write_text(json.dumps(old_trades, indent=2, ensure_ascii=False))

        OLD_STATE_FILE.rename(OLD_STATE_FILE.with_suffix(".json.bak"))
        logger.info(f"[MIGRATE] 旧格式迁移完成，{len(new_pos)} 持仓 / {len(old_trades)} 历史交易")
    except Exception as e:
        logger.warning(f"[MIGRATE] 迁移失败: {e}")


# ─────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="量化策略 v2.1")
    parser.add_argument("--report",       action="store_true", help="生成今日日报后退出")
    parser.add_argument("--daily-report", action="store_true", help="同 --report（旧版兼容）")
    parser.add_argument("--weekly",       action="store_true", help="生成本周周报后退出")
    parser.add_argument("--once",         action="store_true", help="扫描一次后退出（cron 模式）")
    parser.add_argument("--cloud",        action="store_true", help="启用 Gist 同步（VPS 云端模式）")
    parser.add_argument("--force",        action="store_true", help="忽略交易时段检查，强制扫描")
    args = parser.parse_args()

    if args.daily_report:
        args.report = True

    _migrate_old_state()

    if args.report:
        run_daily_report(cloud=args.cloud)
        return

    if args.weekly:
        run_weekly_report(cloud=args.cloud)
        return

    if args.once:
        run_scan(cloud=args.cloud, force=args.force)
        return

    # ── 常驻模式 ───────────────────────────────
    logger.info("量化策略 v2.1 启动，常驻模式")
    logger.info(f"扫描间隔: {SCAN_INTERVAL_SEC}s | 日报: {REPORT_TIME} | 周报: 周日 {REPORT_TIME}")

    schedule.every(SCAN_INTERVAL_SEC).seconds.do(run_scan, cloud=args.cloud)
    schedule.every().day.at(REPORT_TIME).do(run_daily_report, cloud=args.cloud)
    # 周日发周报（weekday=6 即周日）
    schedule.every().sunday.at(REPORT_TIME).do(run_weekly_report, cloud=args.cloud)

    run_scan(cloud=args.cloud, force=args.force)   # 启动时立即扫描一次
    while True:
        schedule.run_pending()
        time.sleep(10)


if __name__ == "__main__":
    main()
