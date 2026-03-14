"""
scanner.py — 信号扫描主逻辑（v2.1）

入场过滤层（按顺序，任一不满足则跳过）：
  Layer 0：组合级风控（RiskManager）
  Layer 1：市场环境过滤（大盘 MA200）
  Layer 2：趋势强度过滤（ADX > 阈值）
  Layer 3：开仓时间过滤
  Layer 4：5分钟技术信号（EMA+RSI+MACD）
  Layer 5：成交量确认
  Layer 6：1小时多周期共振

出场逻辑：
  · ATR 动态止损 + 止损后冷静期
  · 分层追踪止盈

v2.1 新增：
  · _filter_events: 每次扫描记录每只标的被哪层拦截
  · _scan_stats:    每市场扫描量 / 信号量统计
  · floor_price:    持仓期间最低价（用于 MAE 计算）
  · mfe_pct/mae_pct: 出场信号携带最大浮盈/浮亏百分比
"""

from __future__ import annotations
import logging
from datetime import date, datetime, time
from typing import Dict, List, Optional

import pandas as pd
import requests
import yfinance as yf

from config import (
    WATCHLIST, SYMBOL_MARKET, MARKET_CONFIG,
    PRICE_INTERVAL_5M, PRICE_INTERVAL_1H, PRICE_INTERVAL_1D,
)
from indicators import (
    is_above_ma200, is_trend_strong, is_ema_bullish, is_rsi_healthy,
    is_macd_bullish, is_volume_confirmed, get_atr_stop, get_trailing_stop,
    calc_pnl,
)
from risk_manager import RiskManager

logger = logging.getLogger(__name__)

MARKET_INDEX = {
    "US": "SPY",
    "HK": "^HSI",
    "CN": "000300.SS",
}


class Scanner:
    def __init__(self, risk_mgr: RiskManager, positions: Dict[str, dict],
                 trade_history: List[dict]):
        self.risk = risk_mgr
        self.positions = positions
        self.trade_history = trade_history
        # ── v2.1 统计追踪 ────────────────────────────────
        # 每次 scan_all() 后可被外部读取，写入 filter_stats.json / scan_log.json
        self._filter_events: List[dict] = []    # [{layer, symbol, market, reason}]
        self._scan_stats: Dict[str, list] = {}  # {market: [scanned, signals]}
        self._market_status: Dict[str, dict] = {}  # {market: {above_ma200, pct_vs_ma200, price, ma200}}

    # ─────────────────────────────────────────
    # 主扫描入口
    # ─────────────────────────────────────────
    def scan_all(self, force: bool = False) -> List[dict]:
        """
        扫描所有标的，返回信号列表。
        force=True 时跳过交易时段检查（测试用）。
        扫描完成后 self._filter_events 和 self._scan_stats 可被读取。
        """
        signals = []
        self._filter_events = []
        self._scan_stats = {}
        self._market_status = {}
        now = datetime.now()

        for market, symbols in WATCHLIST.items():
            cfg = MARKET_CONFIG[market]
            if not force and not self._is_trading_session(market, now):
                continue

            idx_sym   = MARKET_INDEX[market]
            idx_daily = self._fetch(idx_sym, PRICE_INTERVAL_1D, period="1y")
            market_ok = is_above_ma200(idx_daily) if idx_daily is not None else False
            trend_ok  = is_trend_strong(idx_daily, cfg.adx_period, cfg.adx_threshold) if idx_daily is not None else False

            # v2.1: 记录指数价格 vs MA200 距离（供日报大盘状态行使用）
            if idx_daily is not None and len(idx_daily) >= 200:
                try:
                    ma200  = float(idx_daily["close"].rolling(200).mean().iloc[-1])
                    price  = float(idx_daily["close"].iloc[-1])
                    self._market_status[market] = {
                        "above_ma200":  market_ok,
                        "pct_vs_ma200": round((price - ma200) / ma200 * 100, 1),
                        "price":        round(price, 2),
                        "ma200":        round(ma200, 2),
                    }
                except Exception:
                    pass

            mkt_scanned = 0
            mkt_signals = 0

            for symbol in symbols:
                try:
                    mkt_scanned += 1
                    sig = self._scan_symbol(symbol, market, cfg, now, market_ok, trend_ok)
                    if sig:
                        signals.append(sig)
                        mkt_signals += 1
                except Exception as e:
                    logger.error(f"[SCAN ERROR] {symbol}: {e}")

            self._scan_stats[market] = [mkt_scanned, mkt_signals]

        return signals

    # ─────────────────────────────────────────
    # 单标的扫描
    # ─────────────────────────────────────────
    def _scan_symbol(self, symbol: str, market: str, cfg,
                     now: datetime, market_ok: bool, trend_ok: bool) -> Optional[dict]:

        # ── 出场检查（已持仓）────────────────
        if symbol in self.positions:
            return self._check_exit(symbol, cfg)

        # ── 入场过滤 ─────────────────────────

        # Layer 0：组合级风控
        allow, reason = self.risk.can_open(symbol, self.positions, date.today())
        if not allow:
            logger.debug(f"[L0 SKIP] {symbol}: {reason}")
            self._filter_events.append(
                {"layer": "L0", "symbol": symbol, "market": market, "reason": reason}
            )
            return None

        # Layer 1：大盘 MA200
        if not market_ok:
            logger.debug(f"[L1 SKIP] {symbol}: 大盘在MA200下方")
            self._filter_events.append(
                {"layer": "L1", "symbol": symbol, "market": market, "reason": "大盘MA200"}
            )
            return None

        # Layer 2：趋势强度 ADX
        if not trend_ok:
            logger.debug(f"[L2 SKIP] {symbol}: 趋势强度不足(ADX<{cfg.adx_threshold})")
            self._filter_events.append(
                {"layer": "L2", "symbol": symbol, "market": market, "reason": "ADX不足"}
            )
            return None

        # Layer 3：开仓时间过滤
        if not self._check_open_time(cfg, now):
            logger.debug(f"[L3 SKIP] {symbol}: 不在允许开仓时段")
            self._filter_events.append(
                {"layer": "L3", "symbol": symbol, "market": market, "reason": "时段限制"}
            )
            return None

        # 拉取 K 线
        df_5m = self._fetch(symbol, PRICE_INTERVAL_5M, period="5d")
        df_1h = self._fetch(symbol, PRICE_INTERVAL_1H, period="60d")
        if df_5m is None or df_1h is None or len(df_5m) < 60:
            return None

        # 优先使用 Yahoo Finance v8 实时报价
        realtime = self._fetch_realtime(symbol)
        price = realtime if realtime else float(df_5m["close"].iloc[-1])

        # Layer 4：5分钟技术信号
        ema_ok  = is_ema_bullish(df_5m, cfg.ema_fast, cfg.ema_slow)
        rsi_ok  = is_rsi_healthy(df_5m, cfg.rsi_period, cfg.rsi_low, cfg.rsi_high)
        macd_ok = is_macd_bullish(df_5m)
        if not (ema_ok and rsi_ok and macd_ok):
            logger.debug(
                f"[L4 SKIP] {symbol}: 5m信号不满足 ema={ema_ok} rsi={rsi_ok} macd={macd_ok}"
            )
            self._filter_events.append({
                "layer": "L4", "symbol": symbol, "market": market,
                "reason": f"ema={ema_ok} rsi={rsi_ok} macd={macd_ok}",
            })
            return None

        # Layer 5：成交量确认
        vol_ok = is_volume_confirmed(df_5m, lookback=20, multiplier=cfg.volume_multiplier)
        if not vol_ok:
            logger.debug(f"[L5 SKIP] {symbol}: 成交量未确认")
            self._filter_events.append(
                {"layer": "L5", "symbol": symbol, "market": market, "reason": "量能不足"}
            )
            return None

        # Layer 6：1小时多周期共振
        h1_ok = is_ema_bullish(df_1h, cfg.h1_ema_fast, cfg.h1_ema_slow)
        if not h1_ok:
            logger.debug(f"[L6 SKIP] {symbol}: 1h EMA不共振")
            self._filter_events.append(
                {"layer": "L6", "symbol": symbol, "market": market, "reason": "1h不共振"}
            )
            return None

        # ── 所有层通过，生成开仓信号 ─────────
        atr_stop = get_atr_stop(df_5m, price, cfg.atr_period, cfg.atr_multiplier)
        shares   = self.risk.position_size(symbol, price, self.trade_history)

        logger.info(f"[SIGNAL BUY] {symbol} @ {price:.4f} | stop={atr_stop:.4f} | qty={shares}")
        self._filter_events.append(
            {"layer": "PASS", "symbol": symbol, "market": market, "reason": "全部通过"}
        )
        return {
            "action":     "BUY",
            "symbol":     symbol,
            "market":     market,
            "price":      price,
            "shares":     shares,
            "atr_stop":   atr_stop,
            "peak_price": price,
            "reason":     "L1-L6全通过",
            "timestamp":  datetime.now().isoformat(),
        }

    # ─────────────────────────────────────────
    # 出场检查
    # ─────────────────────────────────────────
    def _check_exit(self, symbol: str, cfg) -> Optional[dict]:
        pos   = self.positions[symbol]
        df_5m = self._fetch(symbol, PRICE_INTERVAL_5M, period="2d")
        if df_5m is None:
            return None

        realtime = self._fetch_realtime(symbol)
        price    = realtime if realtime else float(df_5m["close"].iloc[-1])

        entry    = pos["entry_price"]
        peak     = max(pos.get("peak_price",  price), price)
        floor    = min(pos.get("floor_price", price), price)   # v2.1: 追踪最低价
        atr_stop = pos["atr_stop"]
        shares   = pos["shares"]
        comm     = cfg.commission_rate
        slip     = cfg.slippage_rate

        # 更新持仓中的峰值 / 谷值
        self.positions[symbol]["peak_price"]  = peak
        self.positions[symbol]["floor_price"] = floor

        # 计算最大浮盈 / 最大浮亏（百分比，用于信号质量统计）
        mfe_pct = round((peak  - entry) / entry * 100, 2)
        mae_pct = round((floor - entry) / entry * 100, 2)

        # ATR 止损
        if price <= atr_stop:
            pnl = calc_pnl(entry, price, shares, comm, slip)
            logger.info(f"[EXIT SL] {symbol} @ {price:.4f} pnl={pnl:.0f}")
            self.risk.register_stop_loss(symbol)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "shares": shares, "reason": "ATR止损", "pnl": pnl,
                "timestamp": datetime.now().isoformat(),
                "mfe_pct": mfe_pct, "mae_pct": mae_pct,
            }

        # 分层追踪止盈
        trail_stop = get_trailing_stop(entry, peak, cfg.trailing_tiers)
        if price <= trail_stop:
            pnl = calc_pnl(entry, price, shares, comm, slip)
            logger.info(f"[EXIT TP] {symbol} @ {price:.4f} pnl={pnl:.0f}")
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "shares": shares, "reason": "追踪止盈", "pnl": pnl,
                "timestamp": datetime.now().isoformat(),
                "mfe_pct": mfe_pct, "mae_pct": mae_pct,
            }

        return None

    # ─────────────────────────────────────────
    # 实时报价（Yahoo Finance v8 直连，与 V88 同源）
    # ─────────────────────────────────────────
    def _fetch_realtime(self, symbol: str) -> Optional[float]:
        try:
            url = (
                "https://query1.finance.yahoo.com/v8/finance/chart/"
                f"{symbol}?interval=1m&range=1d"
            )
            headers = {"User-Agent": "Mozilla/5.0"}
            resp = requests.get(url, headers=headers, timeout=8)
            resp.raise_for_status()
            data  = resp.json()
            price = (
                data["chart"]["result"][0]["meta"]
                .get("regularMarketPrice")
            )
            return float(price) if price else None
        except Exception as e:
            logger.debug(f"[REALTIME FAIL] {symbol}: {e}")
            return None

    # ─────────────────────────────────────────
    # 时间过滤
    # ─────────────────────────────────────────
    def _check_open_time(self, cfg, now: datetime) -> bool:
        t = now.time()
        for start_str, end_str in cfg.open_time_blackout:
            start = time.fromisoformat(start_str)
            end   = time.fromisoformat(end_str)
            if start <= t <= end:
                return False
        return True

    def _is_trading_session(self, market: str, now: datetime) -> bool:
        from config import TRADING_SESSIONS
        session = TRADING_SESSIONS.get(market, {})
        if not session:
            return True
        t     = now.time()
        start = time.fromisoformat(session["start"])
        end   = time.fromisoformat(session["end"])
        if start <= end:
            return start <= t <= end
        return t >= start or t <= end  # 跨午夜（美股）

    # ─────────────────────────────────────────
    # K 线数据拉取
    # ─────────────────────────────────────────
    def _fetch(self, symbol: str, interval: str, period: str) -> Optional[pd.DataFrame]:
        try:
            df = yf.download(symbol, period=period, interval=interval,
                             progress=False, auto_adjust=True)
            if df.empty:
                return None
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df.columns = [c.lower() for c in df.columns]
            return df
        except Exception as e:
            logger.warning(f"[FETCH FAIL] {symbol} {interval}: {e}")
            return None
