# =============================================================================
# 量化策略 v2.0 — 全局配置
# =============================================================================
# 所有关键参数集中在此文件，按市场分开配置
# Cursor 提示：修改参数只需改此文件，无需动策略逻辑

from dataclasses import dataclass, field
from typing import Dict, List

# ─────────────────────────────────────────────
# 监控标的 & 分组
# ─────────────────────────────────────────────
WATCHLIST: Dict[str, List[str]] = {
    "US":  ["NVDA", "AAPL", "TSLA", "AMZN", "META", "MSFT", "GOOGL"],
    "HK":  ["0700.HK", "9988.HK", "3690.HK", "0005.HK", "0836.HK"],
    "CN":  ["600519.SS", "300750.SZ", "601318.SS", "000858.SZ", "688008.SS"],
}

# 可读名称映射（用于钉钉通知显示）
SYMBOL_NAMES: Dict[str, str] = {
    "NVDA":      "英伟达",
    "AAPL":      "苹果",
    "TSLA":      "特斯拉",
    "AMZN":      "亚马逊",
    "META":      "Meta",
    "MSFT":      "微软",
    "GOOGL":     "谷歌",
    "0700.HK":   "腾讯",
    "9988.HK":   "阿里",
    "3690.HK":   "美团",
    "0005.HK":   "汇丰",
    "0836.HK":   "华润电力",
    "600519.SS": "贵州茅台",
    "300750.SZ": "宁德时代",
    "601318.SS": "中国平安",
    "000858.SZ": "五粮液",
    "688008.SS": "澜起科技",
}

# 相关性分组（同组最多持仓2只）
CORR_GROUPS: Dict[str, List[str]] = {
    "美股AI组":       ["NVDA", "AAPL", "TSLA", "AMZN", "META", "MSFT", "GOOGL"],
    "港股互联网组":   ["0700.HK", "9988.HK", "3690.HK"],
    "港股金融公用组": ["0005.HK", "0836.HK"],
    "A股消费金融组":  ["600519.SS", "000858.SZ", "601318.SS"],
    "A股新能源组":    ["300750.SZ"],
    "A股半导体组":    ["688008.SS"],
}

# 标的所属市场映射
SYMBOL_MARKET: Dict[str, str] = {}
for _mkt, _syms in WATCHLIST.items():
    for _s in _syms:
        SYMBOL_MARKET[_s] = _mkt

# ─────────────────────────────────────────────
# 资金 & 仓位
# ─────────────────────────────────────────────
INITIAL_CAPITAL = 100_000          # 模拟初始资金（元）
MAX_POSITION_PCT = 0.18            # 单仓最大占比 18%
MAX_POSITIONS = 5                  # 最大同时持仓只数
MAX_CORR_GROUP_POSITIONS = 2       # 同相关性组最多持仓

# ─────────────────────────────────────────────
# 每市场技术参数
# ─────────────────────────────────────────────
@dataclass
class MarketConfig:
    # 趋势过滤
    ma_period: int = 200           # 大盘均线周期
    adx_period: int = 14
    adx_threshold: float = 20.0    # ADX > 此值才认为趋势足够强

    # 入场信号（5分钟K线）
    ema_fast: int = 20
    ema_slow: int = 50
    rsi_period: int = 14
    rsi_low: float = 50.0          # RSI下限
    rsi_high: float = 65.0         # RSI上限（不超买）
    volume_multiplier: float = 1.2 # 当前量 > N倍均量

    # 1小时共振
    h1_ema_fast: int = 20
    h1_ema_slow: int = 50

    # 止损
    atr_period: int = 14
    atr_multiplier: float = 2.0    # 止损 = 入场价 - N * ATR

    # 分层追踪止盈（盈利区间 -> 最高点回撤容忍）
    trailing_tiers: list = field(default_factory=lambda: [
        (0.00, 0.05, 0.08),
        (0.05, 0.15, 0.06),
        (0.15, 0.30, 0.05),
        (0.30, 9.99, 0.04),
    ])

    # 手续费 & 滑点
    commission_rate: float = 0.001  # 手续费率（单边）
    slippage_rate: float = 0.0005   # 滑点（单边）

    # 黑名单冻结天数（连亏3次后）
    blacklist_days: int = 14

    # 止损后冷静期（交易日）
    cooldown_days: int = 1

    # 开仓允许时段（格式 "HH:MM"，空列表=不限制）
    open_time_start: str = ""
    open_time_end: str = ""
    open_time_blackout: list = field(default_factory=list)
    # 格式: [("09:30","09:45"), ("14:45","15:00")]  在此时间段内不允许开仓


MARKET_CONFIG: Dict[str, MarketConfig] = {
    "CN": MarketConfig(
        commission_rate=0.0003,    # A股印花税+佣金约0.03%~0.05%
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
        commission_rate=0.0006,    # 港股佣金+印花税约0.1%
        slippage_rate=0.0005,
        atr_multiplier=2.0,
        rsi_low=50.0,
        rsi_high=65.0,
        adx_threshold=18.0,
        open_time_blackout=[("09:15", "09:35"), ("15:45", "16:00")],
    ),
    "US": MarketConfig(
        commission_rate=0.0002,    # 美股佣金极低
        slippage_rate=0.0003,
        atr_multiplier=1.8,        # 美股流动性好，止损可稍紧
        rsi_low=50.0,
        rsi_high=68.0,
        adx_threshold=20.0,
        open_time_blackout=[("22:15", "22:30")],  # 避开美股开盘前15分钟（北京时间）
    ),
}

# ─────────────────────────────────────────────
# 组合级风控
# ─────────────────────────────────────────────
DAILY_LOSS_HALT_PCT   = 0.02   # 当日亏损 >= 2% 停止开仓
CONSEC_LOSS_HALF_DAYS = 2      # 连续亏损N天后次日仓位减半
CONSEC_LOSS_PAUSE_DAYS = 3     # 连续亏损N天后暂停1交易日

# ─────────────────────────────────────────────
# 评估 & 日报
# ─────────────────────────────────────────────
REPORT_TIME = "21:00"          # 每日日报时间
MIN_TRADES_FOR_SUGGESTION = 10 # 至少交易N笔后给出参数建议
KELLY_MIN_TRADES = 50          # Kelly公式所需最少历史交易笔数

# ─────────────────────────────────────────────
# 数据源
# ─────────────────────────────────────────────
PRICE_INTERVAL_5M  = "5m"
PRICE_INTERVAL_1H  = "1h"
PRICE_INTERVAL_1D  = "1d"
SCAN_INTERVAL_SEC  = 300       # 扫描周期（秒）

# 交易时段（UTC+8）
TRADING_SESSIONS = {
    "CN": {"start": "09:15", "end": "15:00"},
    "HK": {"start": "09:15", "end": "16:10"},
    "US": {"start": "22:15", "end": "05:15"},  # 次日
}
