"""
scanner.py — 信号扫描主逻辑（v2.0）

入场过滤层（按顺序，任一不满足则跳过）：
  Layer 0：组合级风控（RiskManager）
  Layer 1：市场环境过滤（大盘 MA200）         ← v1.0 保留
  Layer 2：趋势强度过滤（ADX > 阈值）         ← v2.0 新增
  Layer 3：开仓时间过滤                        ← v2.0 新增
  Layer 4：5分钟技术信号（EMA+RSI+MACD）       ← v1.0 保留
  Layer 5：成交量确认                           ← v2.0 新增
  Layer 6：1小时多周期共振                      ← v1.0 保留

出场逻辑：
  · ATR 动态止损 + 止损后冷静期
  · 分层追踪止盈
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

    # ─────────────────────────────────────────
    # 主扫描入口
    # ─────────────────────────────────────────
    def scan_all(self, force: bool = False) -> List[dict]:
        """
        扫描所有标的，返回信号列表
        每个信号: {symbol, action, price, shares, reason, ...}
        force=True 时跳过交易时段检查（测试用）
        """
        signals = []
        now = datetime.now()

        for market, symbols in WATCHLIST.items():
            cfg = MARKET_CONFIG[market]
            if not force and not self._is_trading_session(market, now):
                continue

            idx_sym = MARKET_INDEX[market]
            idx_daily = self._fetch(idx_sym, PRICE_INTERVAL_1D, period="1y")
            market_ok = is_above_ma200(idx_daily) if idx_daily is not None else False
            trend_ok = is_trend_strong(idx_daily, cfg.adx_period, cfg.adx_threshold) if idx_daily is not None else False

            for symbol in symbols:
                try:
                    sig = self._scan_symbol(symbol, market, cfg, now,
                                            market_ok, trend_ok)
                    if sig:
                        signals.append(sig)
                except Exception as e:
                    logger.error(f"[SCAN ERROR] {symbol}: {e}")

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
            return None

        # Layer 1：大盘 MA200
        if not market_ok:
            logger.debug(f"[L1 SKIP] {symbol}: 大盘在MA200下方")
            return None

        # Layer 2：趋势强度 ADX
        if not trend_ok:
            logger.debug(f"[L2 SKIP] {symbol}: 趋势强度不足(ADX<{cfg.adx_threshold})")
            return None

        # Layer 3：开仓时间过滤
        if not self._check_open_time(cfg, now):
            logger.debug(f"[L3 SKIP] {symbol}: 不在允许开仓时段")
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
            logger.debug(f"[L4 SKIP] {symbol}: 5m信号不满足 ema={ema_ok} rsi={rsi_ok} macd={macd_ok}")
            return None

        # Layer 5：成交量确认
        vol_ok = is_volume_confirmed(df_5m, lookback=20, multiplier=cfg.volume_multiplier)
        if not vol_ok:
            logger.debug(f"[L5 SKIP] {symbol}: 成交量未确认")
            return None

        # Layer 6：1小时多周期共振
        h1_ok = is_ema_bullish(df_1h, cfg.h1_ema_fast, cfg.h1_ema_slow)
        if not h1_ok:
            logger.debug(f"[L6 SKIP] {symbol}: 1h EMA不共振")
            return None

        # ── 所有层通过，生成开仓信号 ─────────
        atr_stop = get_atr_stop(df_5m, price, cfg.atr_period, cfg.atr_multiplier)
        shares = self.risk.position_size(symbol, price, self.trade_history)

        logger.info(f"[SIGNAL BUY] {symbol} @ {price:.4f} | stop={atr_stop:.4f} | qty={shares}")
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
        pos = self.positions[symbol]
        df_5m = self._fetch(symbol, PRICE_INTERVAL_5M, period="2d")
        if df_5m is None:
            return None

        realtime = self._fetch_realtime(symbol)
        price = realtime if realtime else float(df_5m["close"].iloc[-1])

        entry    = pos["entry_price"]
        peak     = max(pos.get("peak_price", price), price)
        atr_stop = pos["atr_stop"]
        shares   = pos["shares"]
        market   = SYMBOL_MARKET.get(symbol, "US")
        comm     = cfg.commission_rate
        slip     = cfg.slippage_rate

        self.positions[symbol]["peak_price"] = peak

        # ATR 止损
        if price <= atr_stop:
            pnl = calc_pnl(entry, price, shares, comm, slip)
            logger.info(f"[EXIT SL] {symbol} @ {price:.4f} pnl={pnl:.0f}")
            self.risk.register_stop_loss(symbol)
            return {
                "action": "SELL", "symbol": symbol, "price": price,
                "shares": shares, "reason": "ATR止损", "pnl": pnl,
                "timestamp": datetime.now().isoformat(),
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
            }

        return None

    # ─────────────────────────────────────────
    # 实时报价（Yahoo Finance v8 直连，与 V88 同源）
    # ─────────────────────────────────────────
    def _fetch_realtime(self, symbol: str) -> Optional[float]:
        """
        调用 Yahoo Finance v8 /finance/quote 接口获取最新报价
        失败时静默返回 None，由 scanner 回退到 K 线末根收盘价
        """
        try:
            url = (
                "https://query1.finance.yahoo.com/v8/finance/chart/"
                f"{symbol}?interval=1m&range=1d"
            )
            headers = {"User-Agent": "Mozilla/5.0"}
            resp = requests.get(url, headers=headers, timeout=8)
            resp.raise_for_status()
            data = resp.json()
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
            # 统一列名为小写，兼容 MultiIndex（yfinance 0.2.x）
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df.columns = [c.lower() for c in df.columns]
            return df
        except Exception as e:
            logger.warning(f"[FETCH FAIL] {symbol} {interval}: {e}")
            return None
