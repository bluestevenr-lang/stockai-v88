"""
weekend_report.py — 周六持仓周报 v1

有持仓时：13 字段单仓快照（持仓天数 + 追踪止盈 + 52周水位 + 均线 + 相对强弱）
空仓时：  4 块丰富周报
  Block 1  市场状态扫描（3市场：涨跌 / 水位 / ADX / MA200 距离 / 市场阶段）
  Block 2  17只标的本周扫描结果（最新层位 + 周涨跌 + 接近程度）
  Block 3  本周系统运行统计（扫描次数 / 层位分布 / 临界信号计数）
  Block 4  下周观察重点（最接近信号 / 市场翻转风险 / 建议）

数据来源：
  - yfinance                       现价 / 周涨跌 / 52周水位 / MA50 / MA200
  - indicators.adx()               ADX 趋势强度（日线数据）
  - data/last_filter_events.json   各标的上次扫描的过滤层位
  - data/scan_log.json             本周扫描运行统计
  - data/filter_stats.json         本周层位拦截分布
  - data/positions.json            当前持仓
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yfinance as yf

from config import SYMBOL_NAMES, SYMBOL_MARKET, WATCHLIST, MARKET_CONFIG
from indicators import get_trailing_stop, adx as calc_adx

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# 文件路径
# ─────────────────────────────────────────────
POSITIONS_FILE        = Path("data/positions.json")
LAST_FILTER_FILE      = Path("data/last_filter_events.json")
SCAN_LOG_FILE         = Path("data/scan_log.json")
FILTER_STATS_FILE     = Path("data/filter_stats.json")
MARKET_STATUS_FILE    = Path("data/market_status.json")

# ─────────────────────────────────────────────
# 常量
# ─────────────────────────────────────────────
BENCHMARK: Dict[str, str] = {"US": "SPY",    "HK": "^HSI",    "CN": "000300.SS"}
INDEX_LABEL: Dict[str, str] = {"US": "SPY",  "HK": "恒生指数", "CN": "沪深300"}
MARKET_CN: Dict[str, str]   = {"US": "美股",  "HK": "港股",    "CN": "A股"}
CURRENCY: Dict[str, str]    = {"US": "$",    "HK": "HK$",     "CN": "¥"}

# 过滤层接近程度（数字越大越接近信号）
LAYER_RANK: Dict[str, int] = {
    "L0": 0, "L1": 1, "L2": 2, "L3": 3, "L4": 4, "L5": 5, "L6": 6, "PASS": 7
}
LAYER_LABEL: Dict[str, str] = {
    "L0": "风控拦截",
    "L1": "大盘过滤",
    "L2": "ADX不足",
    "L3": "时段限制",
    "L4": "EMA/RSI/MACD",
    "L5": "量能不足",
    "L6": "1h不共振",
    "PASS": "全部通过✅",
}
LAYER_EMOJI: Dict[str, str] = {
    "L0": "⛔", "L1": "🌐", "L2": "📊",
    "L3": "🕐", "L4": "🔶", "L5": "🔶", "L6": "🔥", "PASS": "✅"
}


# ─────────────────────────────────────────────
# 数据拉取
# ─────────────────────────────────────────────

def _fetch_ohlcv(symbol: str) -> Optional[dict]:
    """
    拉取 1 年日线，返回：
      price / weekly_ret / water_level / ma50_pct / ma200_pct / adx_val / above_ma200
    """
    try:
        df = yf.download(symbol, period="1y", interval="1d",
                         progress=False, auto_adjust=True)
        if df is None or df.empty or len(df) < 20:
            return None

        if hasattr(df.columns, "get_level_values"):
            df.columns = df.columns.get_level_values(0)
        df.columns = [c.lower() for c in df.columns]

        close = df["close"]
        current = float(close.iloc[-1])

        high_52w = float(df["high"].max())
        low_52w  = float(df["low"].min())
        water_level = (
            round((current - low_52w) / (high_52w - low_52w) * 100, 1)
            if high_52w != low_52w else 50.0
        )

        ma50  = float(close.rolling(50).mean().iloc[-1])  if len(close) >= 50  else None
        ma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else None
        ma50_pct  = round((current - ma50)  / ma50  * 100, 1) if ma50  else None
        ma200_pct = round((current - ma200) / ma200 * 100, 1) if ma200 else None
        above_ma200 = (ma200 is not None and current > ma200)

        weekly_ret: Optional[float] = None
        if len(close) >= 6:
            weekly_ret = round((current - float(close.iloc[-6])) / float(close.iloc[-6]) * 100, 2)

        # ADX（日线，直接复用 indicators.calc_adx）
        adx_val: Optional[float] = None
        if len(df) >= 28:
            try:
                adx_series = calc_adx(df, 14)
                v = float(adx_series.iloc[-1])
                adx_val = round(v, 1) if not np.isnan(v) else None
            except Exception:
                pass

        return {
            "price":       round(current, 4),
            "water_level": water_level,
            "ma50_pct":    ma50_pct,
            "ma200_pct":   ma200_pct,
            "above_ma200": above_ma200,
            "weekly_ret":  weekly_ret,
            "adx_val":     adx_val,
            "high_52w":    round(high_52w, 4),
            "low_52w":     round(low_52w, 4),
        }
    except Exception as exc:
        logger.warning(f"[WEEKEND] 数据拉取失败 {symbol}: {exc}")
        return None


# ─────────────────────────────────────────────
# 辅助计算
# ─────────────────────────────────────────────

def _holding_days(entry_time: Optional[str]) -> str:
    if not entry_time:
        return "N/A"
    try:
        days = (datetime.now() - datetime.fromisoformat(str(entry_time))).days
        return str(days)
    except Exception:
        return "N/A"


def _trailing_stop_info(pos: dict, current_price: float) -> dict:
    entry  = float(pos.get("entry_price", current_price) or current_price)
    peak   = float(pos.get("peak_price",  entry) or entry)
    market = pos.get("market", "US")
    atr_s  = pos.get("atr_stop")

    tiers = MARKET_CONFIG.get(market, MARKET_CONFIG["US"]).trailing_tiers
    trail_price = get_trailing_stop(entry, peak, tiers)
    gain_pct    = (current_price - entry) / entry * 100
    space_pct   = (current_price - trail_price) / current_price * 100
    gain_ratio  = (peak - entry) / entry

    if gain_pct < 0:        status = "保护中"
    elif gain_ratio < 0.05: status = "缓冲区"
    elif gain_ratio < 0.15: status = "追踪中"
    elif gain_ratio < 0.30: status = "强追踪"
    else:                   status = "锁仓"

    atr_price = float(atr_s) if atr_s else None
    atr_space = (
        round((current_price - atr_price) / current_price * 100, 2)
        if atr_price else None
    )
    return {
        "trail_price":  round(trail_price, 4),
        "space_pct":    round(space_pct, 2),
        "gain_pct":     round(gain_pct, 2),
        "trail_status": status,
        "atr_price":    round(atr_price, 4) if atr_price else None,
        "atr_space":    atr_space,
    }


def _status_label(weekly_ret: Optional[float],
                  bench_ret: Optional[float],
                  ma50_pct: Optional[float]) -> str:
    if weekly_ret is None:
        return "─"
    rel = (weekly_ret - bench_ret) if bench_ret is not None else 0.0
    if rel > 1.0 and (ma50_pct is None or ma50_pct > -2):
        return "强"
    if rel < -1.0 or (ma50_pct is not None and ma50_pct < -5):
        return "弱"
    return "中"


def _market_phase(above_ma200: bool, adx_val: Optional[float],
                  water_level: float, weekly_ret: Optional[float]) -> str:
    """根据均线位置、ADX强度、水位判断市场所处阶段。"""
    adx = adx_val or 0.0
    wret = weekly_ret or 0.0
    if above_ma200:
        if adx >= 25:
            return "📈牛市中继"
        elif adx >= 20:
            return "🔼趋势走强"
        else:
            return "〰高位震荡"
    else:
        if wret >= 1.5:
            return "↗熊市反弹"
        elif adx >= 20:
            return "📉下跌趋势"
        elif water_level <= 30:
            return "🔍底部蓄势"
        else:
            return "↘中期调整"


def _fmt_pct(v: Optional[float], sign: bool = True) -> str:
    if v is None:
        return "N/A"
    return f"{v:+.1f}%" if sign else f"{v:.1f}%"


# ─────────────────────────────────────────────
# Block 1：三大市场状态扫描
# ─────────────────────────────────────────────

def _block1_market_scan() -> List[str]:
    lines = ["▌ Block 1  三大市场状态", ""]
    header = f"  {'市场':<8} {'周涨跌':>7}  {'水位':>5}  {'ADX':>5}  {'MA200':>7}  阶段"
    lines.append(header)
    lines.append("  " + "─" * 56)

    for mkt in ("US", "HK", "CN"):
        sym  = BENCHMARK[mkt]
        name = INDEX_LABEL[mkt]
        data = _fetch_ohlcv(sym)
        if not data:
            lines.append(f"  {name:<8}  ⚠ 数据获取失败")
            continue

        wret     = data["weekly_ret"]
        water    = data["water_level"]
        adx_v    = data["adx_val"]
        ma200p   = data["ma200_pct"]
        above    = data["above_ma200"]
        phase    = _market_phase(above, adx_v, water, wret)

        wret_s   = _fmt_pct(wret)
        adx_s    = f"{adx_v:.1f}" if adx_v else "N/A"
        ma200_s  = _fmt_pct(ma200p)
        zone     = "【临界】" if (ma200p is not None and abs(ma200p) <= 1.0) else ""

        lines.append(
            f"  {name:<8} {wret_s:>7}  {water:>4.0f}%  {adx_s:>5}  {ma200_s:>7}{zone}  {phase}"
        )

    lines.append("")
    return lines


# ─────────────────────────────────────────────
# Block 2：17只标的本周扫描结果
# ─────────────────────────────────────────────

def _load_last_filter_events() -> Tuple[List[dict], str]:
    """读取最近一次扫描的 filter_events，返回 (events, updated_at)。"""
    if not LAST_FILTER_FILE.exists():
        return [], "无数据"
    try:
        raw = json.loads(LAST_FILTER_FILE.read_text(encoding="utf-8"))
        return raw.get("events", []), raw.get("updated_at", "")[:16]
    except Exception:
        return [], "读取失败"


def _best_layer_per_symbol(events: List[dict]) -> Dict[str, dict]:
    """
    每只标的取层位最高（最接近信号）的那条记录。
    同一次扫描中一只标的只会有一条记录，但多次扫描会累积。
    这里直接取最高层位对应的记录。
    """
    result: Dict[str, dict] = {}
    for ev in events:
        sym   = ev.get("symbol", "")
        layer = ev.get("layer", "L0")
        rank  = LAYER_RANK.get(layer, 0)
        prev  = result.get(sym)
        if prev is None or rank > LAYER_RANK.get(prev.get("layer", "L0"), 0):
            result[sym] = ev
    return result


def _block2_symbol_scan() -> Tuple[List[str], List[dict]]:
    """
    返回 (display_lines, near_miss_list)
    near_miss_list: 达到 L4 以上的标的，用于 Block 4。
    """
    events, updated = _load_last_filter_events()
    best_layer = _best_layer_per_symbol(events)

    lines = [f"▌ Block 2  全标的扫描结果（数据截至 {updated}）", ""]

    # 先取基准周涨跌（每市场一次）
    bench_ret: Dict[str, Optional[float]] = {}
    for mkt in ("US", "HK", "CN"):
        bd = _fetch_ohlcv(BENCHMARK[mkt])
        bench_ret[mkt] = bd.get("weekly_ret") if bd else None

    near_miss: List[dict] = []

    for mkt in ("US", "HK", "CN"):
        syms = WATCHLIST.get(mkt, [])
        lines.append(f"  ── {MARKET_CN[mkt]} ──────────────────────────────")
        for sym in syms:
            name     = SYMBOL_NAMES.get(sym, sym)
            ev       = best_layer.get(sym)
            layer    = ev.get("layer", "?") if ev else "─"
            reason   = ev.get("reason", "") if ev else "暂无扫描数据"
            lbl      = LAYER_LABEL.get(layer, layer)
            emoji    = LAYER_EMOJI.get(layer, "·")
            rank     = LAYER_RANK.get(layer, -1)

            # 拉周涨跌（轻量：只用于显示，若超时则显示 N/A）
            data = _fetch_ohlcv(sym)
            wret = data.get("weekly_ret") if data else None
            wret_s = _fmt_pct(wret)

            # 临界程度文字
            if rank >= 6:
                proximity = "🔥极度临界"
            elif rank >= 4:
                proximity = "⚡高度临界"
            elif rank >= 2:
                proximity = "·"
            else:
                proximity = "·"

            lines.append(
                f"  {emoji} {sym:<12} {name:<6}  "
                f"本周{wret_s:>7}  卡在 {layer}（{lbl}）  {proximity}"
            )

            if rank >= 4:
                near_miss.append({
                    "symbol": sym, "name": name, "market": mkt,
                    "layer": layer, "rank": rank,
                    "weekly_ret": wret, "reason": reason,
                })
        lines.append("")

    return lines, near_miss


# ─────────────────────────────────────────────
# Block 3：本周系统运行统计
# ─────────────────────────────────────────────

def _this_week_range() -> Tuple[date, date]:
    today = date.today()
    monday = today - timedelta(days=today.weekday())   # 本周一
    return monday, today


def _block3_system_stats() -> Tuple[List[str], int]:
    """
    返回 (lines, total_near_miss_this_week)。
    """
    lines = ["▌ Block 3  本周系统运行统计", ""]
    week_start, week_end = _this_week_range()

    # 读取 scan_log
    scan_log: List[dict] = []
    if SCAN_LOG_FILE.exists():
        try:
            scan_log = json.loads(SCAN_LOG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass

    week_log = [
        e for e in scan_log
        if week_start.isoformat() <= e.get("date", "") <= week_end.isoformat()
    ]

    total_runs    = len(week_log)
    total_scanned = sum(e.get("scanned", 0) for e in week_log)
    total_signals = sum(e.get("signals", 0) for e in week_log)
    trigger_rate  = round(total_signals / total_scanned * 100, 1) if total_scanned else 0.0

    lines.append(f"  扫描轮次: {total_runs} 次  |  累计扫描: {total_scanned} 次·标的  |  信号: {total_signals} 次")
    lines.append(f"  触发率:   {trigger_rate:.1f}%")

    # 读取 filter_stats，聚合本周
    filter_stats: dict = {}
    if FILTER_STATS_FILE.exists():
        try:
            filter_stats = json.loads(FILTER_STATS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass

    week_by_layer: Dict[str, int] = {}
    week_by_market: Dict[str, Dict[str, int]] = {}
    for day_str, day_data in filter_stats.items():
        if not (week_start.isoformat() <= day_str <= week_end.isoformat()):
            continue
        for layer, cnt in day_data.get("by_layer", {}).items():
            week_by_layer[layer] = week_by_layer.get(layer, 0) + cnt
        for mkt, mkt_data in day_data.get("by_market", {}).items():
            if mkt not in week_by_market:
                week_by_market[mkt] = {}
            for layer, cnt in mkt_data.items():
                week_by_market[mkt][layer] = week_by_market[mkt].get(layer, 0) + cnt

    if week_by_layer:
        lines.append("")
        lines.append("  过滤层拦截（本周累计）：")
        sorted_layers = sorted(
            week_by_layer.items(),
            key=lambda x: LAYER_RANK.get(x[0], 0)
        )
        for layer, cnt in sorted_layers:
            bar = "█" * min(cnt, 20) + ("+" if cnt > 20 else "")
            lbl = LAYER_LABEL.get(layer, layer)
            lines.append(f"    {layer} {lbl:<14} {cnt:>4}次  {bar}")

    # 近似临界信号次数（L4/L5/L6 被拦截次数 = 高质量信号但差一步）
    near_miss_cnt = sum(
        week_by_layer.get(l, 0) for l in ("L4", "L5", "L6")
    )
    if near_miss_cnt:
        lines.append(f"\n  📌 临界信号（差一步触发）: 本周累计 {near_miss_cnt} 次")

    lines.append("")
    return lines, near_miss_cnt


# ─────────────────────────────────────────────
# Block 4：下周观察重点
# ─────────────────────────────────────────────

def _block4_next_week(near_miss: List[dict]) -> List[str]:
    """
    综合 Block 2 近似信号 + Block 1 市场状态，生成下周建议。
    near_miss: Block 2 返回的高层位标的列表。
    """
    lines = ["▌ Block 4  下周观察重点", ""]

    # 最接近信号的标的（按层位排序）
    near_miss_sorted = sorted(near_miss, key=lambda x: x["rank"], reverse=True)
    if near_miss_sorted:
        lines.append("  🎯 最接近信号的标的：")
        for item in near_miss_sorted[:5]:
            sym    = item["symbol"]
            name   = item["name"]
            layer  = item["layer"]
            wret   = item.get("weekly_ret")
            wret_s = _fmt_pct(wret)
            tip    = {
                "L6": "仅差 1h 共振，关注隔夜/周一开盘形态",
                "L5": "差量能确认，留意放量突破",
                "L4": "EMA/RSI/MACD 待对齐，关注 5min 信号",
                "PASS": "已通过全部过滤，等待系统开仓",
            }.get(layer, "")
            lines.append(f"    → {sym} {name}  本周{wret_s}  [{layer}]  {tip}")
    else:
        lines.append("  当前无标的进入 L4+ 临界区间，市场整体偏弱或过滤条件偏严。")

    # 市场翻转风险（MA200 距离最小的市场）
    lines.append("")
    lines.append("  📡 市场状态关注：")
    market_ma200: List[Tuple[str, float, bool]] = []
    for mkt in ("US", "HK", "CN"):
        data = _fetch_ohlcv(BENCHMARK[mkt])
        if data and data.get("ma200_pct") is not None:
            market_ma200.append((mkt, abs(data["ma200_pct"]), data["above_ma200"]))
    market_ma200.sort(key=lambda x: x[1])  # 距 MA200 最近的排前

    for mkt, dist, above in market_ma200:
        name  = INDEX_LABEL[mkt]
        pos   = "上方" if above else "下方"
        tip_t = ""
        if dist <= 1.0:
            tip_t = "  ⚠️ 临界区，MA200 支撑/压力关键位"
        elif not above and dist <= 3.0:
            tip_t = "  📌 接近 MA200，若突破将触发 L1 开放"
        lines.append(f"    {name:<8} 距 MA200 {pos} {dist:.1f}%{tip_t}")

    # 综合建议
    lines.append("")
    lines.append("  💡 建议：")
    if near_miss_sorted and near_miss_sorted[0]["rank"] >= 6:
        lines.append("    · 有标的已达 L6，下周一开盘后系统可能快速触发，保持资金充裕。")
    elif near_miss_sorted and near_miss_sorted[0]["rank"] >= 4:
        lines.append("    · 有标的进入 L4/L5 区间，信号正在积累，继续观察日线形态。")
    else:
        lines.append("    · 当前无临界信号，市场可能处于整固期，耐心等待过滤条件改善。")
    lines.append("    · 勿手动干预策略，第一周积累数据期，以观察为主。")
    lines.append("")
    return lines


# ─────────────────────────────────────────────
# 单仓展示（有持仓时使用）
# ─────────────────────────────────────────────

def _position_block(sym: str, pos: dict, data: dict,
                    bench_ret: Optional[float]) -> List[str]:
    name    = SYMBOL_NAMES.get(sym, sym)
    market  = pos.get("market", "US")
    mkt_cn  = MARKET_CN.get(market, market)
    cur_sym = CURRENCY.get(market, "")
    price   = data["price"]

    wret      = data["weekly_ret"]
    water_lvl = data["water_level"]
    ma50_pct  = data["ma50_pct"]
    ma200_pct = data["ma200_pct"]

    hold_d = _holding_days(pos.get("entry_time"))
    trail  = _trailing_stop_info(pos, price)
    rel    = round((wret - bench_ret), 2) if (wret is not None and bench_ret is not None) else None
    label  = _status_label(wret, bench_ret, ma50_pct)
    bench_name = INDEX_LABEL.get(market, "指数")

    emoji = {"强": "🟢", "中": "🟡", "弱": "🔴"}.get(label, "⚪")

    if trail["space_pct"] < 0:
        trail_tag = f"⚠止盈线{cur_sym}{trail['trail_price']:.2f}（已跌破{trail['space_pct']:+.1f}%）"
    else:
        trail_tag = (
            f"追踪止盈:{cur_sym}{trail['trail_price']:.2f}"
            f"（空间:{trail['space_pct']:+.1f}%）{trail['trail_status']}"
        )
    atr_tag = (
        f"ATR底:{cur_sym}{trail['atr_price']:.2f}（{trail['atr_space']:+.1f}%）"
        if trail["atr_price"] else "ATR底:N/A"
    )

    hold_note = ""
    try:
        hd = int(hold_d)
        hold_note = " 老仓" if hd >= 7 else (" 新仓" if hd <= 2 else "")
    except (ValueError, TypeError):
        pass

    return [
        f"  {emoji} {sym}  {name}  {mkt_cn}  "
        f"现价:{cur_sym}{price:.2f}  本周:{_fmt_pct(wret)}  持仓:{hold_d}天{hold_note}",

        f"     水位:{water_lvl:.0f}%  MA50:{_fmt_pct(ma50_pct)}  "
        f"MA200:{_fmt_pct(ma200_pct)}  相对{bench_name}:{_fmt_pct(rel)}  【{label}】",

        f"     {trail_tag}  {atr_tag}",
        "",
    ]


# ─────────────────────────────────────────────
# 主入口
# ─────────────────────────────────────────────

def generate_weekend_holdings_report() -> str:
    """
    生成周六持仓周报 v1。
    有持仓时展示 13 字段单仓快照；
    空仓时展示 4 块丰富分析（市场状态 / 标的扫描 / 系统统计 / 下周重点）。
    """
    positions: dict = {}
    if POSITIONS_FILE.exists():
        try:
            positions = json.loads(POSITIONS_FILE.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning(f"[WEEKEND] positions.json 读取失败: {exc}")

    today = date.today()
    header = [
        f"╔══════════════════════════════════════════╗",
        f"║   量化持仓周报 v1  {today}      ║",
        f"╚══════════════════════════════════════════╝",
        "",
    ]

    # ── 空仓时：4块分析 ────────────────────────────────────────
    if not positions:
        lines = header + ["【当前状态】空仓  系统运行中，等待入场信号\n", "━" * 46, ""]

        # Block 1
        lines += _block1_market_scan()
        lines += ["━" * 46, ""]

        # Block 2
        b2_lines, near_miss = _block2_symbol_scan()
        lines += b2_lines
        lines += ["━" * 46, ""]

        # Block 3
        b3_lines, _ = _block3_system_stats()
        lines += b3_lines
        lines += ["━" * 46, ""]

        # Block 4
        lines += _block4_next_week(near_miss)
        lines += ["━" * 46]
        return "\n".join(lines)

    # ── 有持仓时：13字段单仓快照 ──────────────────────────────
    lines = header + ["【单只持仓状态】", ""]

    markets_used  = set(pos.get("market", "US") for pos in positions.values())
    bench_ret_map: Dict[str, Optional[float]] = {}
    for mkt in markets_used:
        bd = _fetch_ohlcv(BENCHMARK.get(mkt, "SPY"))
        bench_ret_map[mkt] = bd.get("weekly_ret") if bd else None

    at_risk: List[str] = []
    for sym, pos in positions.items():
        market = pos.get("market", "US")
        data   = _fetch_ohlcv(sym)
        if not data:
            lines += [f"  ⚠ {sym}  {SYMBOL_NAMES.get(sym, sym)}  数据拉取失败", ""]
            continue
        lines += _position_block(sym, pos, data, bench_ret_map.get(market))
        t = _trailing_stop_info(pos, data["price"])
        if t["space_pct"] < 0:
            at_risk.append(sym)

    # 持仓汇总
    lines += ["━" * 46, "【持仓汇总】"]
    entry_dates = [
        pos.get("entry_time", "")[:10]
        for pos in positions.values() if pos.get("entry_time")
    ]
    lines.append(f"  持仓数: {len(positions)}  |  最早入场: {min(entry_dates) if entry_dates else 'N/A'}")
    if at_risk:
        lines.append(f"  ⚠ 已跌破追踪止盈线: {', '.join(at_risk)}（预计下周开盘平仓）")

    # 有持仓时也附带 Block 1（市场状态）和 Block 3（系统统计）
    lines += ["", "━" * 46, ""]
    lines += _block1_market_scan()
    lines += ["━" * 46, ""]
    b3_lines, _ = _block3_system_stats()
    lines += b3_lines
    lines += ["━" * 46]
    return "\n".join(lines)
