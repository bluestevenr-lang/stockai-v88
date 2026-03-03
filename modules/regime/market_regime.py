# -*- coding: utf-8 -*-
"""
MarketRegime - 市场状态判定
输入：趋势 + 广度 + 风险偏好因子
输出：regime, confidence, drivers_top3
规则：不得单因子判定，必须综合多项指标
"""

from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")


class MarketRegime:
    """
    市场状态判定器
    综合趋势、广度、波动率，输出 BULL/RANGE/BEAR + 置信度 + 驱动因素
    """

    BULL = "BULL"
    RANGE = "RANGE"
    BEAR = "BEAR"

    def __init__(self, index_trend: float = 0.0, breadth_pct: float = 0.5,
                 vix_proxy: float = 20.0, ma20_vs_price: float = 0.0):
        """
        Args:
            index_trend: 指数20日收益率 (-1~1 或 百分比/100)
            breadth_pct: 广度指标，0~1，表示成分股中站上MA20的比例
            vix_proxy: 波动率代理（VIX值或ATR比率*100），越高越恐慌
            ma20_vs_price: 价格相对MA20位置，>0表示在MA20上方
        """
        self.index_trend = index_trend
        self.breadth_pct = breadth_pct
        self.vix_proxy = vix_proxy
        self.ma20_vs_price = ma20_vs_price

    def evaluate(self, index_df=None, breadth_above_ma20: int = 0,
                 breadth_total: int = 1) -> Dict[str, Any]:
        """
        评估市场状态

        Args:
            index_df: 指数日线DataFrame（可选），用于计算趋势
            breadth_above_ma20: 站上MA20的股票数
            breadth_total: 总股票数

        Returns:
            {
                "regime": "BULL"|"RANGE"|"BEAR",
                "confidence": 0~1,
                "drivers_top3": ["驱动1", "驱动2", "驱动3"],
                "timestamp": "Asia/Shanghai 秒级",
                "timestamp_epoch": 秒级时间戳
            }
        """
        drivers = []
        bull_score = 0.0
        bear_score = 0.0

        # 1. 指数趋势（若有 index_df）
        if index_df is not None and len(index_df) >= 20:
            last_close = float(index_df["Close"].iloc[-1])
            ma20 = float(index_df["Close"].tail(20).mean())
            if ma20 > 0:
                self.ma20_vs_price = (last_close - ma20) / ma20
            ret_20d = (last_close - float(index_df["Close"].iloc[-21])) / float(index_df["Close"].iloc[-21]) if len(index_df) >= 21 else 0
            self.index_trend = ret_20d

        # 2. 趋势因子
        if self.index_trend > 0.02:
            bull_score += 0.35
            drivers.append(f"指数20日涨幅{self.index_trend*100:.1f}%")
        elif self.index_trend < -0.02:
            bear_score += 0.35
            drivers.append(f"指数20日跌幅{abs(self.index_trend)*100:.1f}%")
        else:
            drivers.append("指数横盘震荡")

        # 3. 广度因子
        if breadth_total > 0:
            self.breadth_pct = breadth_above_ma20 / breadth_total
        if self.breadth_pct > 0.6:
            bull_score += 0.35
            drivers.append(f"广度强({self.breadth_pct*100:.0f}%站上MA20)")
        elif self.breadth_pct < 0.4:
            bear_score += 0.35
            drivers.append(f"广度弱({self.breadth_pct*100:.0f}%站上MA20)")
        else:
            drivers.append(f"广度中性({self.breadth_pct*100:.0f}%)")

        # 4. 波动率/风险因子（VIX proxy）
        if self.vix_proxy > 25:
            bear_score += 0.3
            drivers.append(f"波动率偏高({self.vix_proxy:.1f})")
        elif self.vix_proxy < 15:
            bull_score += 0.2
            drivers.append(f"波动率偏低({self.vix_proxy:.1f})")
        else:
            drivers.append(f"波动率中性({self.vix_proxy:.1f})")

        # 5. 价格相对MA20
        if self.ma20_vs_price > 0.02:
            bull_score += 0.1
        elif self.ma20_vs_price < -0.02:
            bear_score += 0.1

        # 判定 regime
        diff = bull_score - bear_score
        if diff > 0.25:
            regime = self.BULL
            confidence = min(0.95, 0.6 + diff * 0.5)
        elif diff < -0.25:
            regime = self.BEAR
            confidence = min(0.95, 0.6 + abs(diff) * 0.5)
        else:
            regime = self.RANGE
            confidence = min(0.9, 0.5 + (0.25 - abs(diff)) * 1.5)

        ts = datetime.now(SHANGHAI)
        return {
            "regime": regime,
            "confidence": round(confidence, 3),
            "drivers_top3": drivers[:3],
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S %Z"),
            "timestamp_epoch": int(ts.timestamp()),
            "bull_score": round(bull_score, 3),
            "bear_score": round(bear_score, 3),
        }
