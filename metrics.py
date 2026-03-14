"""
metrics.py — 绩效评估 & 日报（v2.0）

统计维度：
  · 总体：胜率、盈亏比、Profit Factor、最大回撤、Sharpe
  · 按市场拆分：A股 / 港股 / 美股
  · 按标的拆分
  · 按时段拆分（早盘/午盘/尾盘/夜盘）
  · 过滤层效果归因（哪一层拦截最多）
"""

from __future__ import annotations
import json
import math
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional


TRADES_FILE = Path("data/trades.json")
METRICS_FILE = Path("data/metrics_history.json")


def load_trades() -> List[dict]:
    if TRADES_FILE.exists():
        return json.loads(TRADES_FILE.read_text())
    return []


def save_trade(trade: dict):
    trades = load_trades()
    trades.append(trade)
    TRADES_FILE.parent.mkdir(parents=True, exist_ok=True)
    TRADES_FILE.write_text(json.dumps(trades, indent=2, ensure_ascii=False))


# ─────────────────────────────────────────────
# 核心统计函数
# ─────────────────────────────────────────────

def calc_metrics(trades: List[dict]) -> dict:
    """
    输入: 已平仓的交易记录列表（含 pnl 字段）
    输出: 完整绩效指标字典
    """
    closed = [t for t in trades if t.get("closed") and "pnl" in t]
    if not closed:
        return {"total_trades": 0}

    pnls = [t["pnl"] for t in closed]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    win_rate = len(wins) / len(pnls) if pnls else 0
    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = sum(losses) / len(losses) if losses else 0  # 负数
    rr = abs(avg_win / avg_loss) if avg_loss != 0 else 0

    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    total_return = sum(pnls)
    max_dd = _max_drawdown(pnls)
    sharpe = _sharpe(pnls)

    return {
        "total_trades":   len(closed),
        "win_trades":     len(wins),
        "loss_trades":    len(losses),
        "win_rate":       round(win_rate * 100, 1),          # %
        "avg_win":        round(avg_win, 2),
        "avg_loss":       round(avg_loss, 2),
        "rr_ratio":       round(rr, 2),
        "profit_factor":  round(profit_factor, 2),
        "total_pnl":      round(total_return, 2),
        "max_drawdown":   round(max_dd * 100, 2),            # %
        "sharpe":         round(sharpe, 3),
    }


def calc_breakdown(trades: List[dict]) -> dict:
    """按市场/标的/时段分别统计"""
    closed = [t for t in trades if t.get("closed") and "pnl" in t]

    by_market = defaultdict(list)
    by_symbol = defaultdict(list)
    by_session = defaultdict(list)

    for t in closed:
        mkt = t.get("market", "US")
        by_market[mkt].append(t["pnl"])
        by_symbol[t["symbol"]].append(t["pnl"])
        session = _get_session(t.get("entry_time", ""))
        by_session[session].append(t["pnl"])

    def summarize(groups: dict) -> dict:
        result = {}
        for k, pnls in groups.items():
            wins = [p for p in pnls if p > 0]
            result[k] = {
                "trades": len(pnls),
                "total_pnl": round(sum(pnls), 2),
                "win_rate": round(len(wins) / len(pnls) * 100, 1) if pnls else 0,
            }
        return dict(sorted(result.items(), key=lambda x: -x[1]["total_pnl"]))

    return {
        "by_market":  summarize(by_market),
        "by_symbol":  summarize(by_symbol),
        "by_session": summarize(by_session),
    }


def calc_filter_stats(filter_log: List[dict]) -> dict:
    """
    统计各过滤层拦截数量（需要在 scanner 记录被哪层拒绝）
    filter_log: [{"layer": "L2", "symbol": "NVDA", "reason": "..."}]
    """
    counts = defaultdict(int)
    for entry in filter_log:
        counts[entry.get("layer", "unknown")] += 1
    total = sum(counts.values())
    return {
        layer: {"count": cnt, "pct": round(cnt / total * 100, 1) if total else 0}
        for layer, cnt in sorted(counts.items())
    }


# ─────────────────────────────────────────────
# 日报生成
# ─────────────────────────────────────────────

def generate_daily_report(
    trades: List[dict],
    risk_summary: dict,
    filter_log: List[dict],
    capital: float,
    report_date: date = None,
) -> str:
    report_date = report_date or date.today()
    today_trades = [t for t in trades
                    if t.get("closed") and t.get("exit_date") == str(report_date)]

    metrics = calc_metrics(today_trades)
    all_metrics = calc_metrics(trades)
    breakdown = calc_breakdown(trades)
    filter_stats = calc_filter_stats(filter_log)

    lines = [
        f"╔══════════════════════════════════════════╗",
        f"║   量化策略 v2.0 日报  {report_date}   ║",
        f"╚══════════════════════════════════════════╝",
        "",
        f"【今日概况】",
        f"  当日交易: {metrics.get('total_trades', 0)} 笔",
        f"  当日盈亏: ¥{risk_summary.get('daily_pnl', 0):+.2f}",
        f"  连续亏损: {risk_summary.get('consec_loss_days', 0)} 天",
        f"  系统状态: {'⏸ 暂停中' if risk_summary.get('paused') else '▶ 运行中'}",
        "",
        f"【累计绩效（全部历史）】",
        f"  总交易数: {all_metrics.get('total_trades', 0)} 笔",
        f"  胜    率: {all_metrics.get('win_rate', 0):.1f}%",
        f"  平均盈利: ¥{all_metrics.get('avg_win', 0):+.2f}",
        f"  平均亏损: ¥{all_metrics.get('avg_loss', 0):+.2f}",
        f"  盈 亏 比: {all_metrics.get('rr_ratio', 0):.2f}",
        f"  PF:       {all_metrics.get('profit_factor', 0):.2f}",
        f"  最大回撤: {all_metrics.get('max_drawdown', 0):.2f}%",
        f"  Sharpe:   {all_metrics.get('sharpe', 0):.3f}",
        f"  累计盈亏: ¥{all_metrics.get('total_pnl', 0):+.2f}",
        "",
        f"【按市场拆分】",
    ]

    for mkt, stats in breakdown.get("by_market", {}).items():
        lines.append(f"  {mkt:6s}: {stats['trades']}笔  PnL ¥{stats['total_pnl']:+.0f}  胜率{stats['win_rate']:.0f}%")

    lines += ["", "【按标的 TOP5 盈亏】"]
    sym_data = breakdown.get("by_symbol", {})
    for sym, stats in list(sym_data.items())[:5]:
        lines.append(f"  {sym:12s}: ¥{stats['total_pnl']:+.0f}  ({stats['trades']}笔 {stats['win_rate']:.0f}%)")

    lines += ["", "【按时段拆分】"]
    for session, stats in breakdown.get("by_session", {}).items():
        lines.append(f"  {session:8s}: {stats['trades']}笔  ¥{stats['total_pnl']:+.0f}")

    lines += ["", "【过滤层拦截统计】（v2.0 新增）"]
    for layer, info in filter_stats.items():
        lines.append(f"  {layer}: 拦截 {info['count']} 次 ({info['pct']:.0f}%)")

    # 黑名单 & 冷静期
    if risk_summary.get("blacklist"):
        lines += ["", "【黑名单标的】"]
        for sym, d in risk_summary["blacklist"].items():
            lines.append(f"  {sym}: 解禁日 {d}")
    if risk_summary.get("cooldown"):
        lines += ["", "【冷静期标的】"]
        for sym, d in risk_summary["cooldown"].items():
            lines.append(f"  {sym}: 可入日 {d}")

    # 参数建议（满10笔后）
    total = all_metrics.get("total_trades", 0)
    if total >= 10:
        lines += ["", "【系统建议】"]
        wr = all_metrics.get("win_rate", 0)
        pf = all_metrics.get("profit_factor", 0)
        if wr < 45:
            lines.append("  ⚠ 胜率偏低(<45%)，建议收紧入场条件，提高 RSI 下限至55")
        if pf < 1.2:
            lines.append("  ⚠ Profit Factor<1.2，考虑放宽追踪止盈空间（允许更多利润奔跑）")
        if pf > 2.5 and wr > 60:
            lines.append("  ✓ 系统表现良好，可考虑小幅提升单仓比例至20%")
        if total >= 50:
            lines.append("  ✓ 已满50笔，Kelly公式已激活，仓位计算将自动优化")

    lines.append("")
    lines.append("━" * 44)
    return "\n".join(lines)


# ─────────────────────────────────────────────
# 内部辅助
# ─────────────────────────────────────────────

def _max_drawdown(pnls: List[float]) -> float:
    """最大回撤（基于盈亏序列的累计曲线）"""
    if not pnls:
        return 0.0
    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    for p in pnls:
        cumulative += p
        if cumulative > peak:
            peak = cumulative
        dd = (peak - cumulative) / max(abs(peak), 1)
        if dd > max_dd:
            max_dd = dd
    return max_dd


def _sharpe(pnls: List[float], risk_free_daily: float = 0.0) -> float:
    if len(pnls) < 2:
        return 0.0
    mean = sum(pnls) / len(pnls)
    variance = sum((p - mean) ** 2 for p in pnls) / len(pnls)
    std = math.sqrt(variance)
    if std == 0:
        return 0.0
    return (mean - risk_free_daily) / std * math.sqrt(252)


def _get_session(entry_time_str: str) -> str:
    """根据入场时间返回时段标签"""
    try:
        dt = datetime.fromisoformat(entry_time_str)
        h = dt.hour
        if 9 <= h < 11:
            return "早盘"
        elif 11 <= h < 14:
            return "午盘"
        elif 14 <= h < 16:
            return "尾盘"
        elif 22 <= h or h < 6:
            return "夜盘(美股)"
        return "其他"
    except Exception:
        return "未知"
