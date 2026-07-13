# =============================================================================
# 量化策略 v2.0 — 全局配置
# =============================================================================
from dataclasses import dataclass, field
from typing import Dict, List

# ─────────────────────────────────────────────
# 监控标的 & 分组（V88 持仓列表）
# ─────────────────────────────────────────────
WATCHLIST: Dict[str, List[str]] = {
    "US":  ["ABBV", "ACMR", "NVDA", "NVO", "VOO", "BRK-B", "QQQM", "GOOG", "PM", "LLY", "TSM", "TSLA"],
    "HK":  ["0700.HK", "0883.HK", "1299.HK", "0941.HK"],
    "CN":  ["600519.SS", "688981.SS", "601899.SS", "688008.SS", "600941.SS", "000333.SZ", "000001.SZ"],
}

# 可读名称映射
SYMBOL_NAMES: Dict[str, str] = {
    "ABBV":      "艾伯维",
    "ACMR":      "ACM Research",
    "NVDA":      "英伟达",
    "NVO":       "诺和诺德",
    "VOO":       "标普500ETF",
    "BRK-B":     "伯克希尔",
    "QQQM":      "纳指100ETF",
    "GOOG":      "谷歌",
    "PM":        "菲利普莫里斯",
    "LLY":       "礼来制药",
    "TSM":       "台积电",
    "TSLA":      "特斯拉",
    "0700.HK":   "腾讯控股",
    "0883.HK":   "中国海洋石油",
    "1299.HK":   "友邦保险",
    "0941.HK":   "中国移动",
    "600519.SS": "贵州茅台",
    "688981.SS": "中芯国际",
    "601899.SS": "紫金矿业",
    "688008.SS": "澜起科技",
    "600941.SS": "中国移动",
    "000333.SZ": "美的集团",
    "000001.SZ": "平安银行",
}

# 相关性分组
CORR_GROUPS: Dict[str, List[str]] = {
    "美股医药组":   ["ABBV", "NVO", "LLY"],
    "美股科技组":   ["NVDA", "GOOG", "TSM", "ACMR", "TSLA"],
    "美股ETF组":    ["VOO", "QQQM"],
    "美股其他组":   ["BRK-B", "PM"],
    "港股组":       ["0700.HK", "0883.HK", "1299.HK", "0941.HK"],
    "A股消费组":    ["600519.SS", "601899.SS"],
    "A股半导体组":  ["688981.SS", "688008.SS"],
}

# 标的所属市场映射
SYMBOL_MARKET: Dict[str, str] = {}
for _mkt, _syms in WATCHLIST.items():
    for _s in _syms:
        SYMBOL_MARKET[_s] = _mkt

# ─────────────────────────────────────────────
# 资金 & 仓位
# ─────────────────────────────────────────────
INITIAL_CAPITAL = 100_000
MAX_POSITION_PCT = 0.18
MAX_POSITIONS = 5
MAX_CORR_GROUP_POSITIONS = 2

# ─────────────────────────────────────────────
# 每市场技术参数
# ─────────────────────────────────────────────
@dataclass
class MarketConfig:
    ma_period: int = 200
    adx_period: int = 14
    adx_threshold: float = 20.0
    ema_fast: int = 20
    ema_slow: int = 50
    rsi_period: int = 14
    rsi_low: float = 50.0
    rsi_high: float = 65.0
    volume_multiplier: float = 1.2
    h1_ema_fast: int = 20
    h1_ema_slow: int = 50
    atr_period: int = 14
    atr_multiplier: float = 2.0
    trailing_tiers: list = field(default_factory=lambda: [
        (0.00, 0.05, 0.08),
        (0.05, 0.15, 0.06),
        (0.15, 0.30, 0.05),
        (0.30, 9.99, 0.04),
    ])
    commission_rate: float = 0.001
    slippage_rate: float = 0.0005
    blacklist_days: int = 14
    cooldown_days: int = 1
    open_time_start: str = ""
    open_time_end: str = ""
    open_time_blackout: list = field(default_factory=list)

MARKET_CONFIG: Dict[str, MarketConfig] = {
    "CN": MarketConfig(
        commission_rate=0.0003,
        slippage_rate=0.0003,
        atr_multiplier=2.0,
        rsi_low=50.0,
        rsi_high=65.0,
        adx_threshold=20.0,
        open_time_start="09:45",
        open_time_end="14:45",
        open_time_blackout=[("09:30", "09:45"), ("14:45", "15:00")],
    ),
    "HK": MarketConfig(
        commission_rate=0.0006,
        slippage_rate=0.0005,
        atr_multiplier=2.0,
        rsi_low=50.0,
        rsi_high=65.0,
        adx_threshold=18.0,
        open_time_blackout=[("09:15", "09:35"), ("15:45", "16:00")],
    ),
    "US": MarketConfig(
        commission_rate=0.0002,
        slippage_rate=0.0003,
        atr_multiplier=1.8,
        rsi_low=50.0,
        rsi_high=68.0,
        adx_threshold=20.0,
        open_time_blackout=[("22:15", "22:30")],
    ),
}

# ─────────────────────────────────────────────
# 组合级风控
# ─────────────────────────────────────────────
DAILY_LOSS_HALT_PCT   = 0.02
CONSEC_LOSS_HALF_DAYS = 2
CONSEC_LOSS_PAUSE_DAYS = 3

# ─────────────────────────────────────────────
# 评估 & 日报
# ─────────────────────────────────────────────
REPORT_TIME = "21:00"
MIN_TRADES_FOR_SUGGESTION = 10
KELLY_MIN_TRADES = 50

# ─────────────────────────────────────────────
# 数据源
# ─────────────────────────────────────────────
PRICE_INTERVAL_5M  = "5m"
PRICE_INTERVAL_1H  = "1h"
PRICE_INTERVAL_1D  = "1d"
SCAN_INTERVAL_SEC  = 300

TRADING_SESSIONS = {
    "CN": {"start": "09:15", "end": "15:00"},
    "HK": {"start": "09:15", "end": "16:10"},
    "US": {"start": "22:15", "end": "05:15"},
}
