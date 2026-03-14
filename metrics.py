"""
metrics.py — 绩效评估、策略验证统计 & 日报（v2.1）

统计维度：
  · 总体：胜率、盈亏比、Profit Factor、最大回撤、Sharpe
  · 按市场拆分（A股/港股/美股 完整对比）
  · 按标的拆分
  · 按时段拆分
  · 过滤层效果归因（每层拦截次数 / 占比 / 最严格层）
  · 信号质量：MFE（最大浮盈）、MAE（最大浮亏）、持仓时长
  · 信号密度：扫描批次、触发率、空仓率
  · 周度评估报告

v2.1 新增文件：
  data/scan_log.json     — 每次扫描运行的摘要（时间/市场/信号量）
  data/filter_stats.json — 每日过滤层拦截次数聚合
  data/weekly_metrics.json — 周度报告归档
"""

from __future__ import annotations
import json
import math
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ─────────────────────────────────────────────
# 数据文件路径
# ─────────────────────────────────────────────
TRADES_FILE        = Path("data/trades.json")
METRICS_FILE       = Path("data/metrics_history.json")
SCAN_LOG_FILE      = Path("data/scan_log.json")
FILTER_STATS_FILE  = Path("data/filter_stats.json")
WEEKLY_FILE        = Path("data/weekly_metrics.json")

MKT_CN = {"CN": "A股", "HK": "港股", "US": "美股"}

# 过滤层可读名称
LAYER_NAMES: Dict[str, str] = {
    "L0":   "仓位/风控",
    "L1":   "市场环境(MA200)",
    "L2":   "趋势强度(ADX)",
    "L3":   "开仓时段",
    "L4":   "技术信号(5m)",
    "L5":   "成交量确认",
    "L6":   "1h多周期",
    "PASS": "全部通过",
}

LAYER_ORDER = ["L0", "L1", "L2", "L3", "L4", "L5", "L6", "PASS"]


# ─────────────────────────────────────────────
# trades.json I/O
# ─────────────────────────────────────────────

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
# scan_log.json I/O
# ─────────────────────────────────────────────

def load_scan_log() -> List[dict]:
    if SCAN_LOG_FILE.exists():
        return json.loads(SCAN_LOG_FILE.read_text())
    return []


def append_scan_entry(entry: dict):
    """
    entry 格式：
    {
      "ts": "2026-03-15T09:31:00",
      "date": "2026-03-15",
      "scanned": 10,
      "signals": 1,
      "by_market": {"CN": [5, 1], "HK": [5, 0], "US": [0, 0]}
    }
    """
    log = load_scan_log()
    log.append(entry)
    SCAN_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    SCAN_LOG_FILE.write_text(json.dumps(log[-5000:], indent=2))


# ─────────────────────────────────────────────
# filter_stats.json I/O
# ─────────────────────────────────────────────

def load_filter_stats() -> dict:
    if FILTER_STATS_FILE.exists():
        return json.loads(FILTER_STATS_FILE.read_text())
    return {}


def update_filter_stats(events: List[dict], scan_date: str = None):
    """
    events: scanner._filter_events，格式 [{layer, symbol, market, reason}]
    按日期聚合写入 filter_stats.json
    """
    scan_date = scan_date or str(date.today())
    stats = load_filter_stats()

    day = stats.setdefault(scan_date, {"by_layer": {}, "by_market": {}})

    for ev in events:
        layer  = ev.get("layer", "unknown")
        market = ev.get("market", "US")

        day["by_layer"][layer] = day["by_layer"].get(layer, 0) + 1

        mkt_dict = day["by_market"].setdefault(market, {})
        mkt_dict[layer] = mkt_dict.get(layer, 0) + 1

    # 保留最近 90 天
    if len(stats) > 90:
        for k in sorted(stats.keys())[:-90]:
            del stats[k]

    FILTER_STATS_FILE.parent.mkdir(parents=True, exist_ok=True)
    FILTER_STATS_FILE.write_text(json.dumps(stats, indent=2, ensure_ascii=False))


# ─────────────────────────────────────────────
# weekly_metrics.json I/O
# ─────────────────────────────────────────────

def load_weekly_metrics() -> dict:
    if WEEKLY_FILE.exists():
        return json.loads(WEEKLY_FILE.read_text())
    return {}


def save_weekly_metrics(week_key: str, data: dict):
    wm = load_weekly_metrics()
    wm[week_key] = data
    # 保留最近 52 周
    if len(wm) > 52:
        for k in sorted(wm.keys())[:-52]:
            del wm[k]
    WEEKLY_FILE.parent.mkdir(parents=True, exist_ok=True)
    WEEKLY_FILE.write_text(json.dumps(wm, indent=2, ensure_ascii=False))


# ─────────────────────────────────────────────
# 核心统计函数
# ─────────────────────────────────────────────

def calc_metrics(trades: List[dict]) -> dict:
    """总体绩效指标"""
    closed = [t for t in trades if t.get("closed") and "pnl" in t]
    if not closed:
        return {"total_trades": 0}

    pnls   = [t["pnl"] for t in closed]
    wins   = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    win_rate     = len(wins) / len(pnls) if pnls else 0
    avg_win      = sum(wins) / len(wins)   if wins   else 0
    avg_loss     = sum(losses) / len(losses) if losses else 0
    rr           = abs(avg_win / avg_loss)   if avg_loss != 0 else 0
    gross_profit = sum(wins)
    gross_loss   = abs(sum(losses))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    return {
        "total_trades":   len(closed),
        "win_trades":     len(wins),
        "loss_trades":    len(losses),
        "win_rate":       round(win_rate * 100, 1),
        "avg_win":        round(avg_win, 2),
        "avg_loss":       round(avg_loss, 2),
        "rr_ratio":       round(rr, 2),
        "profit_factor":  round(profit_factor, 2),
        "total_pnl":      round(sum(pnls), 2),
        "max_drawdown":   round(_max_drawdown(pnls) * 100, 2),
        "sharpe":         round(_sharpe(pnls), 3),
    }


def calc_market_breakdown_full(trades: List[dict]) -> dict:
    """
    按市场完整拆分统计：
    trades / win_rate / avg_win / avg_loss / rr_ratio / total_pnl / max_dd
    排序按累计盈亏降序
    """
    closed = [t for t in trades if t.get("closed") and "pnl" in t]
    by_market: Dict[str, List] = defaultdict(list)
    for t in closed:
        by_market[t.get("market", "US")].append(t["pnl"])

    result = {}
    for mkt, pnls in by_market.items():
        wins   = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        avg_win  = sum(wins)   / len(wins)   if wins   else 0
        avg_loss = sum(losses) / len(losses) if losses else 0
        rr       = abs(avg_win / avg_loss)   if avg_loss else 0
        result[mkt] = {
            "trades":    len(pnls),
            "win_rate":  round(len(wins) / len(pnls) * 100, 1) if pnls else 0,
            "avg_win":   round(avg_win,  2),
            "avg_loss":  round(avg_loss, 2),
            "rr_ratio":  round(rr, 2),
            "total_pnl": round(sum(pnls), 2),
            "max_dd":    round(_max_drawdown(pnls) * 100, 1),
        }
    return dict(sorted(result.items(), key=lambda x: -x[1]["total_pnl"]))


def calc_breakdown(trades: List[dict]) -> dict:
    """按市场/标的/时段简要统计（兼容旧接口）"""
    closed = [t for t in trades if t.get("closed") and "pnl" in t]

    by_market  = defaultdict(list)
    by_symbol  = defaultdict(list)
    by_session = defaultdict(list)

    for t in closed:
        mkt = t.get("market", "US")
        by_market[mkt].append(t["pnl"])
        by_symbol[t["symbol"]].append(t["pnl"])
        session = _get_session(t.get("entry_time", ""))
        by_session[session].append(t["pnl"])

    def summarize(groups: dict) -> dict:
        res = {}
        for k, pnls in groups.items():
            wins = [p for p in pnls if p > 0]
            res[k] = {
                "trades":    len(pnls),
                "total_pnl": round(sum(pnls), 2),
                "win_rate":  round(len(wins) / len(pnls) * 100, 1) if pnls else 0,
            }
        return dict(sorted(res.items(), key=lambda x: -x[1]["total_pnl"]))

    return {
        "by_market":  summarize(by_market),
        "by_symbol":  summarize(by_symbol),
        "by_session": summarize(by_session),
    }


def calc_signal_quality(trades: List[dict], n: int = 10) -> dict:
    """
    近 n 笔已平仓交易的信号质量统计：
    - 平均最大浮盈（MFE）百分比
    - 平均最大浮亏（MAE）百分比
    - 平均持仓时长（小时）
    - MFE/MAE 比（>1.5 说明持仓期间正收益空间大于负收益空间）
    """
    closed = [t for t in trades if t.get("closed") and "pnl" in t]
    recent = closed[-n:] if len(closed) >= n else closed

    if not recent:
        return {"n_trades": 0, "has_mfe_data": False}

    mfe_list = [t["mfe_pct"] for t in recent if t.get("mfe_pct") is not None]
    mae_list = [t["mae_pct"] for t in recent if t.get("mae_pct") is not None]

    durations: List[float] = []
    for t in recent:
        try:
            entry_dt = datetime.fromisoformat(t["entry_time"])
            exit_dt  = datetime.fromisoformat(t["exit_time"])
            durations.append((exit_dt - entry_dt).total_seconds() / 3600)
        except Exception:
            pass

    avg_mfe = round(sum(mfe_list) / len(mfe_list), 2) if mfe_list else None
    avg_mae = round(sum(mae_list) / len(mae_list), 2) if mae_list else None
    mfe_mae_ratio = round(abs(avg_mfe / avg_mae), 2) if (avg_mfe and avg_mae) else None

    return {
        "n_trades":       len(recent),
        "has_mfe_data":   len(mfe_list) > 0,
        "avg_mfe_pct":    avg_mfe,
        "avg_mae_pct":    avg_mae,
        "mfe_mae_ratio":  mfe_mae_ratio,
        "avg_duration_h": round(sum(durations) / len(durations), 1) if durations else None,
    }


def calc_signal_density(scan_log: List[dict]) -> dict:
    """
    信号密度统计：
    - 总扫描批次 / 评估标的数 / 触发信号数 / 触发率
    - 空仓天数占比
    - 按市场分解触发率
    """
    if not scan_log:
        return {"total_scans": 0}

    total_scanned = sum(e.get("scanned", 0) for e in scan_log)
    total_signals = sum(e.get("signals", 0) for e in scan_log)
    trigger_rate  = round(total_signals / total_scanned * 100, 2) if total_scanned else 0

    # 市场维度
    mkt_acc: Dict[str, list] = defaultdict(lambda: [0, 0])
    for entry in scan_log:
        for mkt, counts in entry.get("by_market", {}).items():
            mkt_acc[mkt][0] += counts[0]
            mkt_acc[mkt][1] += counts[1]

    by_market = {}
    for mkt, (sc, sig) in mkt_acc.items():
        by_market[mkt] = {
            "scanned":      sc,
            "signals":      sig,
            "trigger_rate": round(sig / sc * 100, 2) if sc else 0,
        }

    # 空仓率（有扫描的日子中，信号数为 0 的占比）
    days_with_scan   = len(set(e.get("date", "") for e in scan_log))
    days_with_signal = len(set(e.get("date", "") for e in scan_log if e.get("signals", 0) > 0))
    vacancy_rate     = round((1 - days_with_signal / days_with_scan) * 100, 1) if days_with_scan else 100.0

    return {
        "total_scans":     len(scan_log),
        "total_scanned":   total_scanned,
        "total_signals":   total_signals,
        "trigger_rate_pct": trigger_rate,
        "days_with_scan":  days_with_scan,
        "days_with_signal": days_with_signal,
        "vacancy_rate_pct": vacancy_rate,
        "by_market":       by_market,
    }


def calc_filter_attribution(filter_stats: dict, today_str: str = None) -> dict:
    """
    返回指定日期的过滤层拦截分布，以及最严格层。
    """
    today_str = today_str or str(date.today())
    today     = filter_stats.get(today_str, {})
    by_layer  = today.get("by_layer", {})

    if not by_layer:
        return {"by_layer": {}, "strictest_layer": None, "strictest_name": None, "total": 0}

    total = sum(by_layer.values())
    result = {}
    for layer in LAYER_ORDER:
        cnt = by_layer.get(layer, 0)
        if cnt == 0:
            continue
        result[layer] = {
            "name":  LAYER_NAMES.get(layer, layer),
            "count": cnt,
            "pct":   round(cnt / total * 100, 1) if total else 0,
        }

    non_pass = {k: v for k, v in by_layer.items() if k != "PASS"}
    strictest = max(non_pass, key=lambda k: non_pass[k]) if non_pass else None

    return {
        "by_layer":       result,
        "by_market":      today.get("by_market", {}),
        "strictest_layer": strictest,
        "strictest_name":  LAYER_NAMES.get(strictest, strictest) if strictest else None,
        "total":          total,
    }


def calc_filter_stats(filter_log: List[dict]) -> dict:
    """兼容旧接口（raw event list → layer counts）"""
    counts: Dict[str, int] = defaultdict(int)
    for entry in filter_log:
        counts[entry.get("layer", "unknown")] += 1
    total = sum(counts.values())
    return {
        layer: {"count": cnt, "pct": round(cnt / total * 100, 1) if total else 0}
        for layer, cnt in sorted(counts.items())
    }


# ─────────────────────────────────────────────
# 策略验证统计模块（日报新增节）
# ─────────────────────────────────────────────

def _diag_lines(attr: dict, mkt_stats: dict, density: dict, trades: List[dict]) -> List[str]:
    """生成诊断建议（最多4条）"""
    issues: List[str] = []

    by_layer = attr.get("by_layer", {})
    if by_layer:
        l1_pct = by_layer.get("L1", {}).get("pct", 0)
        l2_pct = by_layer.get("L2", {}).get("pct", 0)
        l4_pct = by_layer.get("L4", {}).get("pct", 0)
        l5_pct = by_layer.get("L5", {}).get("pct", 0)

        if l1_pct > 60:
            issues.append(f"⚠ L1大盘过滤占{l1_pct:.0f}% → 市场持续弱势，策略在保护模式，属正常")
        if l2_pct > 40:
            issues.append(f"⚠ L2 ADX拦截{l2_pct:.0f}% → 趋势强度不足，可考虑降低ADX阈值(当前20)")
        if l4_pct > 50:
            issues.append(f"⚠ L4技术信号拦截{l4_pct:.0f}% → EMA/RSI条件较严，先积累数据再调整")
        if l5_pct > 30:
            issues.append(f"⚠ L5成交量拦截{l5_pct:.0f}% → volume_multiplier=1.2 可能略高")

    if density and density.get("total_scans", 0) > 0:
        tr = density.get("trigger_rate_pct", 0)
        vr = density.get("vacancy_rate_pct", 100)
        if tr < 0.3:
            issues.append(f"⚠ 触发率极低({tr:.2f}%) → 策略过严或市场环境差，建议观察满1周再调参")
        elif tr > 8:
            issues.append(f"⚠ 触发率偏高({tr:.2f}%) → 入场条件可能过松，注意过度交易")
        if vr > 80:
            issues.append(f"⚠ 空仓率达{vr:.0f}% → 当前市场不适合该策略，耐心等待")

    for mkt, s in mkt_stats.items():
        mkt_name = MKT_CN.get(mkt, mkt)
        if s["trades"] >= 5 and s["win_rate"] < 40:
            issues.append(f"⚠ {mkt_name}胜率低({s['win_rate']:.0f}%) → 考虑提高该市场RSI下限")
        if s["trades"] >= 5 and s["rr_ratio"] < 1.0:
            issues.append(f"⚠ {mkt_name}盈亏比<1({s['rr_ratio']:.2f}) → 追踪止盈参数待观察")

    closed = [t for t in trades if t.get("closed")]
    if not closed:
        issues.append("📌 尚无已平仓交易 → 持续积累中，10笔后给出有效建议")

    if not issues:
        issues.append("✓ 暂无明显异常，继续积累数据")

    return issues[:4]


def generate_validation_section(
    trades: List[dict],
    scan_log: List[dict],
    filter_stats: dict,
    today_str: str = None,
) -> str:
    """
    日报中"策略验证统计"节，输出纯文本（适合代码块 / 钉钉 markdown）
    """
    today_str  = today_str or str(date.today())
    today_scans = [e for e in scan_log if e.get("date") == today_str]

    total_scanned_today = sum(e.get("scanned", 0) for e in today_scans)
    total_signals_today = sum(e.get("signals", 0) for e in today_scans)
    scan_batches_today  = len(today_scans)
    trigger_today       = (
        round(total_signals_today / total_scanned_today * 100, 2)
        if total_scanned_today else 0
    )

    mkt_today: Dict[str, list] = defaultdict(lambda: [0, 0])
    for e in today_scans:
        for mkt, counts in e.get("by_market", {}).items():
            mkt_today[mkt][0] += counts[0]
            mkt_today[mkt][1] += counts[1]

    attr      = calc_filter_attribution(filter_stats, today_str)
    mkt_stats = calc_market_breakdown_full(trades)
    density   = calc_signal_density(scan_log)
    sq        = calc_signal_quality(trades, n=10)

    lines = [
        "",
        "━" * 46,
        "【📊 策略验证统计】",
        "",
        "▶ 今日扫描与触发",
        f"  扫描批次: {scan_batches_today}  "
        f"评估标的: {total_scanned_today}  "
        f"触发信号: {total_signals_today}  "
        f"触发率: {trigger_today:.2f}%",
    ]

    for mkt in ["CN", "HK", "US"]:
        if mkt in mkt_today:
            sc, sig = mkt_today[mkt]
            lines.append(f"  {MKT_CN.get(mkt, mkt)}: 评估{sc} 触发{sig}")

    # ── 过滤层归因 ──────────────────────────────
    lines += ["", "▶ 今日过滤层拦截（各层标的数 / 占比）"]
    if attr.get("by_layer"):
        for layer in LAYER_ORDER:
            info = attr["by_layer"].get(layer)
            if not info:
                continue
            bar  = "▓" * min(int(info["pct"] / 5), 16)
            lines.append(
                f"  {layer:<4} {info['name']:<14} {info['count']:3d}次  "
                f"{info['pct']:5.1f}%  {bar}"
            )
        if attr.get("strictest_layer"):
            lines.append(
                f"  ⚠ 最严格层 → {attr['strictest_name']} ({attr['strictest_layer']})"
            )
    else:
        lines.append("  （今日暂无数据）")

    # ── 大盘状态（L1 是否因市场弱还是参数问题）──
    mkt_status_file = Path("data/market_status.json")
    if mkt_status_file.exists():
        try:
            ms_data  = json.loads(mkt_status_file.read_text())
            ms       = ms_data.get("status", {})
            updated  = ms_data.get("updated_at", "")[:16]
            _IDX     = {"US": "SPY", "HK": "恒生指数", "CN": "沪深300"}
            parts    = []
            for mkt in ["US", "HK", "CN"]:
                if mkt not in ms:
                    continue
                s     = ms[mkt]
                arrow = "▲" if s["above_ma200"] else "▼"
                pct   = s["pct_vs_ma200"]
                parts.append(f"{_IDX[mkt]} {arrow}MA200({pct:+.1f}%)")
            if parts:
                lines.append(f"  大盘状态: {' | '.join(parts)}  （更新:{updated}）")
        except Exception:
            pass

    # ── 分市场完整表格 ──────────────────────────
    lines += ["", "▶ 分市场累计绩效"]
    if mkt_stats:
        lines.append(
            f"  {'市场':<5} {'笔':>3} {'胜率':>6} {'均盈':>8} {'均亏':>8} "
            f"{'赔率':>5} {'总PnL':>9} {'最大DD':>7}"
        )
        lines.append("  " + "─" * 55)
        for mkt, s in mkt_stats.items():
            lines.append(
                f"  {MKT_CN.get(mkt, mkt):<5} {s['trades']:>3} "
                f"{s['win_rate']:>5.1f}% "
                f"¥{s['avg_win']:>7.0f} "
                f"¥{s['avg_loss']:>7.0f} "
                f"{s['rr_ratio']:>5.2f} "
                f"¥{s['total_pnl']:>8.0f} "
                f"{s['max_dd']:>6.1f}%"
            )
    else:
        lines.append("  （尚无已平仓交易）")

    # ── 信号质量 ────────────────────────────────
    lines += ["", f"▶ 近10笔信号质量（MFE/MAE）"]
    if sq.get("has_mfe_data"):
        mfe_mae = sq.get("mfe_mae_ratio")
        quality = "✓ 有效" if (mfe_mae and mfe_mae > 1.5) else "⚠ 待观察"
        lines.append(f"  样本笔数: {sq['n_trades']}")
        lines.append(f"  平均最大浮盈(MFE): {sq['avg_mfe_pct']:+.2f}%")
        lines.append(f"  平均最大浮亏(MAE): {sq['avg_mae_pct']:+.2f}%")
        lines.append(
            f"  MFE/MAE 比: {mfe_mae:.2f}x  {quality}"
        )
        if sq.get("avg_duration_h"):
            lines.append(f"  平均持仓时长: {sq['avg_duration_h']:.1f}h")
    else:
        lines.append("  （需要交易数据后可用 | 新增 mfe/mae 追踪从本版本起生效）")

    # ── 信号密度 ────────────────────────────────
    if density.get("total_scans", 0) > 0:
        lines += ["", "▶ 累计信号密度"]
        lines.append(
            f"  总扫描批次: {density['total_scans']}  "
            f"评估标的: {density['total_scanned']}  "
            f"总信号: {density['total_signals']}"
        )
        lines.append(
            f"  整体触发率: {density['trigger_rate_pct']:.2f}%  "
            f"空仓率: {density['vacancy_rate_pct']:.1f}%"
        )
        for mkt in ["CN", "HK", "US"]:
            if mkt in density.get("by_market", {}):
                md = density["by_market"][mkt]
                lines.append(
                    f"  {MKT_CN.get(mkt, mkt)}: 评估{md['scanned']} "
                    f"信号{md['signals']} "
                    f"触发率{md['trigger_rate']:.2f}%"
                )

    # ── 当前最需关注 ────────────────────────────
    lines += ["", "▶ 当前最需关注"]
    for issue in _diag_lines(attr, mkt_stats, density, trades):
        lines.append(f"  {issue}")

    return "\n".join(lines)


# ─────────────────────────────────────────────
# 日报生成
# ─────────────────────────────────────────────

def generate_daily_report(
    trades: List[dict],
    risk_summary: dict,
    filter_log: List[dict],        # 旧接口保留（raw events）
    capital: float,
    report_date: date = None,
    scan_log: List[dict] = None,   # v2.1 新增
    filter_stats: dict = None,     # v2.1 新增
) -> str:
    report_date  = report_date  or date.today()
    scan_log     = scan_log     or []
    filter_stats = filter_stats or {}

    today_trades = [
        t for t in trades
        if t.get("closed") and t.get("exit_date") == str(report_date)
    ]

    metrics     = calc_metrics(today_trades)
    all_metrics = calc_metrics(trades)
    breakdown   = calc_breakdown(trades)

    lines = [
        f"╔══════════════════════════════════════════╗",
        f"║   量化策略 v2.1 日报  {report_date}   ║",
        f"╚══════════════════════════════════════════╝",
        "",
        "【今日概况】",
        f"  当日交易: {metrics.get('total_trades', 0)} 笔",
        f"  当日盈亏: ¥{risk_summary.get('daily_pnl', 0):+.2f}",
        f"  连续亏损: {risk_summary.get('consec_loss_days', 0)} 天",
        f"  系统状态: {'⏸ 暂停中' if risk_summary.get('paused') else '▶ 运行中'}",
        "",
        "【累计绩效（全部历史）】",
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
        "【按市场简览】",
    ]

    for mkt, stats in breakdown.get("by_market", {}).items():
        lines.append(
            f"  {MKT_CN.get(mkt, mkt):<4}: "
            f"{stats['trades']}笔  "
            f"PnL ¥{stats['total_pnl']:+.0f}  "
            f"胜率{stats['win_rate']:.0f}%"
        )

    lines += ["", "【TOP5 标的盈亏】"]
    sym_data = breakdown.get("by_symbol", {})
    for sym, stats in list(sym_data.items())[:5]:
        lines.append(
            f"  {sym:<12}: ¥{stats['total_pnl']:+.0f}  "
            f"({stats['trades']}笔 {stats['win_rate']:.0f}%)"
        )

    lines += ["", "【按时段拆分】"]
    for session, stats in breakdown.get("by_session", {}).items():
        lines.append(f"  {session:<8}: {stats['trades']}笔  ¥{stats['total_pnl']:+.0f}")

    # 黑名单 & 冷静期
    if risk_summary.get("blacklist"):
        lines += ["", "【黑名单标的】"]
        for sym, d in risk_summary["blacklist"].items():
            lines.append(f"  {sym}: 解禁日 {d}")
    if risk_summary.get("cooldown"):
        lines += ["", "【冷静期标的】"]
        for sym, d in risk_summary["cooldown"].items():
            lines.append(f"  {sym}: 可入日 {d}")

    # 参数建议
    total = all_metrics.get("total_trades", 0)
    if total >= 10:
        lines += ["", "【系统建议】"]
        wr = all_metrics.get("win_rate", 0)
        pf = all_metrics.get("profit_factor", 0)
        if wr < 45:
            lines.append("  ⚠ 胜率偏低(<45%)，考虑收紧入场条件")
        if pf < 1.2:
            lines.append("  ⚠ PF<1.2，考虑放宽追踪止盈空间")
        if pf > 2.5 and wr > 60:
            lines.append("  ✓ 系统良好，可考虑小幅提升单仓比例")
        if total >= 50:
            lines.append("  ✓ 已满50笔，Kelly仓位计算已激活")

    # ── v2.1 新增：策略验证统计节 ─────────────
    validation = generate_validation_section(
        trades, scan_log, filter_stats, str(report_date)
    )
    lines.append(validation)

    lines.append("")
    lines.append("━" * 46)
    return "\n".join(lines)


# ─────────────────────────────────────────────
# 周报生成
# ─────────────────────────────────────────────

def generate_weekly_report(
    trades: List[dict],
    scan_log: List[dict],
    filter_stats: dict,
    week_start: date = None,
) -> str:
    """
    周度策略评估报告（每周日 21:00 发送）
    """
    today      = date.today()
    week_start = week_start or (today - timedelta(days=today.weekday()))
    week_end   = week_start + timedelta(days=6)
    week_key   = f"{week_start.isocalendar()[0]}-W{week_start.isocalendar()[1]:02d}"

    # 本周交易
    week_dates  = set(
        str(week_start + timedelta(days=i)) for i in range(7)
    )
    week_trades = [
        t for t in trades
        if t.get("closed") and t.get("exit_date") in week_dates
    ]

    w_metrics    = calc_metrics(week_trades)
    mkt_stats    = calc_market_breakdown_full(week_trades)
    all_mkt      = calc_market_breakdown_full(trades)
    density      = calc_signal_density(scan_log)

    # 本周过滤层聚合
    week_filter: Dict[str, int] = defaultdict(int)
    for d_str, d_data in filter_stats.items():
        if d_str in week_dates:
            for layer, cnt in d_data.get("by_layer", {}).items():
                week_filter[layer] += cnt
    wf_total = sum(week_filter.values())

    lines = [
        f"╔══════════════════════════════════════════╗",
        f"║   量化策略 v2.1 周报  {week_key}        ║",
        f"║   {week_start} ～ {week_end}         ║",
        f"╚══════════════════════════════════════════╝",
        "",
        "【本周交易概览】",
        f"  交易笔数: {w_metrics.get('total_trades', 0)}  "
        f"胜率: {w_metrics.get('win_rate', 0):.1f}%  "
        f"盈亏比: {w_metrics.get('rr_ratio', 0):.2f}",
        f"  累计盈亏: ¥{w_metrics.get('total_pnl', 0):+.2f}  "
        f"PF: {w_metrics.get('profit_factor', 0):.2f}",
        "",
        "【本周各市场表现排名】",
    ]

    ranked = sorted(mkt_stats.items(), key=lambda x: -x[1]["total_pnl"])
    for rank, (mkt, s) in enumerate(ranked, 1):
        lines.append(
            f"  #{rank} {MKT_CN.get(mkt, mkt)}: "
            f"{s['trades']}笔  胜率{s['win_rate']:.0f}%  "
            f"盈亏比{s['rr_ratio']:.2f}  ¥{s['total_pnl']:+.0f}"
        )
    if not ranked:
        lines.append("  （本周暂无已平仓交易）")

    lines += ["", "【本周过滤层拦截占比】"]
    if wf_total > 0:
        for layer in LAYER_ORDER:
            cnt = week_filter.get(layer, 0)
            if cnt == 0:
                continue
            pct = round(cnt / wf_total * 100, 1)
            bar = "▓" * min(int(pct / 5), 16)
            lines.append(
                f"  {layer:<4} {LAYER_NAMES.get(layer, layer):<14} "
                f"{cnt:4d}次  {pct:5.1f}%  {bar}"
            )
    else:
        lines.append("  （本周暂无过滤数据）")

    lines += ["", "【累计各市场对比（全部历史）】"]
    for mkt, s in all_mkt.items():
        lines.append(
            f"  {MKT_CN.get(mkt, mkt):<4}: "
            f"{s['trades']}笔  胜率{s['win_rate']:.0f}%  "
            f"赔率{s['rr_ratio']:.2f}  ¥{s['total_pnl']:+.0f}  "
            f"最大DD {s['max_dd']:.1f}%"
        )

    # 信号密度周总结
    week_scans   = [e for e in scan_log if e.get("date") in week_dates]
    wk_scanned   = sum(e.get("scanned", 0) for e in week_scans)
    wk_signals   = sum(e.get("signals", 0) for e in week_scans)
    wk_trigger   = round(wk_signals / wk_scanned * 100, 2) if wk_scanned else 0
    wk_avg_daily = round(wk_signals / 5, 1) if wk_signals else 0

    lines += [
        "",
        "【本周信号密度】",
        f"  扫描批次: {len(week_scans)}  评估标的: {wk_scanned}  "
        f"信号: {wk_signals}  触发率: {wk_trigger:.2f}%",
        f"  日均信号数: {wk_avg_daily}",
    ]

    # 参数调整建议（只建议，不自动改）
    lines += ["", "【参数调整建议（仅供参考，不自动执行）】"]
    suggestions = _weekly_suggestions(w_metrics, mkt_stats, week_filter, wf_total, wk_trigger)
    for s in suggestions:
        lines.append(f"  {s}")

    lines += ["", "━" * 46]

    # 保存周报数据
    week_data = {
        "generated_at":    datetime.now().isoformat(),
        "week_trades":     w_metrics.get("total_trades", 0),
        "week_win_rate":   w_metrics.get("win_rate", 0),
        "week_pnl":        w_metrics.get("total_pnl", 0),
        "market_ranking":  [m for m, _ in ranked],
        "strictest_layer": max(week_filter, key=lambda k: week_filter[k]) if week_filter else None,
        "avg_trigger_rate": wk_trigger,
        "week_scan_batches": len(week_scans),
    }
    save_weekly_metrics(week_key, week_data)

    return "\n".join(lines)


def _weekly_suggestions(
    w_metrics: dict, mkt_stats: dict,
    week_filter: dict, wf_total: int, wk_trigger: float
) -> List[str]:
    suggestions = []

    wr = w_metrics.get("win_rate", 0)
    pf = w_metrics.get("profit_factor", 0)
    total = w_metrics.get("total_trades", 0)

    if total == 0:
        return ["本周无交易，数据不足，继续观察"]

    if wr < 40:
        suggestions.append(
            "胜率低于40% → 可考虑将RSI下限从50提高至55（保持不动，先观察两周）"
        )
    if pf < 1.0:
        suggestions.append(
            "PF<1，亏损交易金额超过盈利 → 检查追踪止盈是否过早触发"
        )
    if wf_total > 0:
        l2_pct = round(week_filter.get("L2", 0) / wf_total * 100, 1)
        l5_pct = round(week_filter.get("L5", 0) / wf_total * 100, 1)
        if l2_pct > 40:
            suggestions.append(
                f"L2 ADX本周拦截占{l2_pct}% → 可尝试降低 adx_threshold 从20→18"
            )
        if l5_pct > 30:
            suggestions.append(
                f"L5成交量本周拦截占{l5_pct}% → 可尝试降低 volume_multiplier 从1.2→1.1"
            )

    if wk_trigger > 8:
        suggestions.append(
            f"触发率达{wk_trigger:.1f}% → 过度交易风险，考虑收紧入场条件"
        )

    for mkt, s in mkt_stats.items():
        if s["trades"] >= 3 and s["rr_ratio"] < 0.8:
            suggestions.append(
                f"{MKT_CN.get(mkt, mkt)}盈亏比{s['rr_ratio']:.2f} → "
                "降低追踪止盈的第一档回撤容忍度"
            )

    return suggestions if suggestions else ["本周各项指标在合理范围内，维持现有参数"]


# ─────────────────────────────────────────────
# 内部辅助
# ─────────────────────────────────────────────

def _max_drawdown(pnls: List[float]) -> float:
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
    mean     = sum(pnls) / len(pnls)
    variance = sum((p - mean) ** 2 for p in pnls) / len(pnls)
    std      = math.sqrt(variance)
    if std == 0:
        return 0.0
    return (mean - risk_free_daily) / std * math.sqrt(252)


def _get_session(entry_time_str: str) -> str:
    try:
        dt = datetime.fromisoformat(entry_time_str)
        h  = dt.hour
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
