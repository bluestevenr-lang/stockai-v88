"""
risk_manager.py — 组合级风控（v2.0 新增）

负责：
  · 每日亏损熔断
  · 连续亏损降仓/暂停
  · 相关性集中度限制
  · 止损后冷静期
  · 仓位规模计算（含 Kelly）
"""

from __future__ import annotations
import json
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config import (
    CORR_GROUPS, SYMBOL_MARKET, MARKET_CONFIG,
    MAX_POSITION_PCT, MAX_CORR_GROUP_POSITIONS,
    DAILY_LOSS_HALT_PCT, CONSEC_LOSS_HALF_DAYS, CONSEC_LOSS_PAUSE_DAYS,
    KELLY_MIN_TRADES,
)

logger = logging.getLogger(__name__)
STATE_FILE = Path("data/risk_state.json")


class RiskManager:
    """组合级风控管理器，状态持久化到 risk_state.json"""

    def __init__(self, capital: float):
        self.capital = capital
        self._state = self._load_state()

    # ─────────────────────────────────────────
    # 持久化
    # ─────────────────────────────────────────
    def _load_state(self) -> dict:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text())
        return {
            "daily_pnl": {},          # date_str -> float
            "consec_loss_days": 0,
            "pause_until": None,      # date_str
            "cooldown": {},           # symbol -> date_str (可重入日期)
            "blacklist": {},          # symbol -> date_str (解禁日)
            "consec_loss_per_symbol": {},  # symbol -> int
        }

    def _save(self):
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps(self._state, indent=2, ensure_ascii=False))

    # ─────────────────────────────────────────
    # 每日 P&L 记录
    # ─────────────────────────────────────────
    def record_trade_pnl(self, pnl: float, symbol: str, today: date = None):
        """每笔平仓后调用，更新当日 P&L 和黑名单逻辑"""
        today = today or date.today()
        key = str(today)
        self._state["daily_pnl"][key] = self._state["daily_pnl"].get(key, 0.0) + pnl

        # 单标的连亏计数
        sym_loss = self._state["consec_loss_per_symbol"]
        if pnl < 0:
            sym_loss[symbol] = sym_loss.get(symbol, 0) + 1
            if sym_loss[symbol] >= 3:
                unblock = today + timedelta(days=14)
                self._state["blacklist"][symbol] = str(unblock)
                sym_loss[symbol] = 0
                logger.warning(f"[BLACKLIST] {symbol} 连亏3次，冻结至 {unblock}")
        else:
            sym_loss[symbol] = 0

        self._save()

    def end_of_day(self, today: date = None):
        """每天收盘后调用，更新连续亏损天数"""
        today = today or date.today()
        key = str(today)
        today_pnl = self._state["daily_pnl"].get(key, 0.0)

        if today_pnl < 0:
            self._state["consec_loss_days"] += 1
        else:
            self._state["consec_loss_days"] = 0

        days = self._state["consec_loss_days"]
        if days >= CONSEC_LOSS_PAUSE_DAYS:
            pause_until = today + timedelta(days=2)  # 暂停1个交易日（跳过明天）
            self._state["pause_until"] = str(pause_until)
            self._state["consec_loss_days"] = 0
            logger.warning(f"[CIRCUIT] 连续{days}天亏损，暂停至 {pause_until}")

        self._save()

    # ─────────────────────────────────────────
    # 开仓前检查（返回 True=允许, False=拒绝）
    # ─────────────────────────────────────────
    def can_open(
        self,
        symbol: str,
        positions: Dict[str, dict],
        today: date = None,
    ) -> Tuple[bool, str]:
        """
        综合检查，返回 (allow, reason)
        调用方：scanner.py → 入场过滤第0层（组合级）
        """
        today = today or date.today()

        # 1. 暂停检查
        pause = self._state.get("pause_until")
        if pause and date.fromisoformat(pause) >= today:
            return False, f"组合暂停中，恢复日期 {pause}"

        # 2. 当日亏损熔断
        key = str(today)
        daily_pnl = self._state["daily_pnl"].get(key, 0.0)
        if daily_pnl <= -self.capital * DAILY_LOSS_HALT_PCT:
            return False, f"当日亏损熔断（已亏 {daily_pnl:.0f}）"

        # 3. 黑名单
        unblock = self._state["blacklist"].get(symbol)
        if unblock and date.fromisoformat(unblock) > today:
            return False, f"{symbol} 黑名单中，解禁日 {unblock}"

        # 4. 止损后冷静期
        cooldown = self._state["cooldown"].get(symbol)
        if cooldown and date.fromisoformat(cooldown) > today:
            return False, f"{symbol} 冷静期中，可入日期 {cooldown}"

        # 5. 相关性集中度
        group_name, group_count = self._corr_group_count(symbol, positions)
        if group_count >= MAX_CORR_GROUP_POSITIONS:
            return False, f"相关性组【{group_name}】已持{group_count}只，达上限"

        # 6. 最大持仓数
        if len(positions) >= 5:
            return False, "已达最大持仓5只"

        return True, "OK"

    def _corr_group_count(self, symbol: str, positions: Dict) -> Tuple[str, int]:
        for group_name, members in CORR_GROUPS.items():
            if symbol in members:
                count = sum(1 for s in positions if s in members)
                return group_name, count
        return "无分组", 0

    # ─────────────────────────────────────────
    # 止损后登记冷静期
    # ─────────────────────────────────────────
    def register_stop_loss(self, symbol: str, today: date = None):
        today = today or date.today()
        market = SYMBOL_MARKET.get(symbol, "US")
        cfg = MARKET_CONFIG[market]
        resume = today + timedelta(days=cfg.cooldown_days + 1)
        self._state["cooldown"][symbol] = str(resume)
        self._save()
        logger.info(f"[COOLDOWN] {symbol} 止损出场，冷静期至 {resume}")

    # ─────────────────────────────────────────
    # 仓位大小计算
    # ─────────────────────────────────────────
    def position_size(
        self,
        symbol: str,
        price: float,
        trade_history: List[dict],
    ) -> int:
        """
        返回建议买入股数（手数取整）
        优先用 1/2 Kelly；历史不足时退回固定18%
        """
        market = SYMBOL_MARKET.get(symbol, "US")
        # 连续亏损天数减半
        half_cap = self._state["consec_loss_days"] >= CONSEC_LOSS_HALF_DAYS
        base_pct = MAX_POSITION_PCT * (0.5 if half_cap else 1.0)
        base_capital = self.capital * base_pct

        # Kelly 公式（需要至少50笔历史）
        symbol_trades = [t for t in trade_history if t.get("symbol") == symbol and t.get("closed")]
        if len(symbol_trades) >= KELLY_MIN_TRADES:
            wins = [t for t in symbol_trades if t["pnl"] > 0]
            losses = [t for t in symbol_trades if t["pnl"] <= 0]
            if losses:
                win_rate = len(wins) / len(symbol_trades)
                avg_win = sum(t["pnl"] for t in wins) / max(len(wins), 1)
                avg_loss = abs(sum(t["pnl"] for t in losses) / len(losses))
                rr = avg_win / avg_loss if avg_loss > 0 else 1
                kelly = win_rate - (1 - win_rate) / rr
                half_kelly = max(0.01, min(kelly * 0.5, base_pct))
                base_capital = self.capital * half_kelly
                logger.info(f"[KELLY] {symbol}: win={win_rate:.2f} rr={rr:.2f} kelly={kelly:.3f} → {half_kelly:.3f}")

        shares = int(base_capital / price)
        lot = 100 if market == "CN" else 1  # A股100股一手
        shares = max(lot, (shares // lot) * lot)
        return shares

    # ─────────────────────────────────────────
    # 状态查询
    # ─────────────────────────────────────────
    def daily_summary(self, today: date = None) -> dict:
        today = today or date.today()
        key = str(today)
        return {
            "date": key,
            "daily_pnl": round(self._state["daily_pnl"].get(key, 0.0), 2),
            "consec_loss_days": self._state["consec_loss_days"],
            "paused": self._state.get("pause_until"),
            "blacklist": {k: v for k, v in self._state["blacklist"].items()
                         if date.fromisoformat(v) >= today},
            "cooldown": {k: v for k, v in self._state["cooldown"].items()
                        if date.fromisoformat(v) >= today},
        }
