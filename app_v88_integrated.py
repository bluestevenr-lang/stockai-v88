"""
AI 皇冠双核 V88 - 集成版（模块化架构 + 完整功能）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
版本说明：
  - 基于V87.17的完整功能
  - 集成V88的模块化架构
  - 使用LRU缓存系统
  - Type Hints和统一错误处理
  
核心改进：
  ✅ 模块化架构（8个核心模块）
  ✅ LRU缓存系统（比满则全清更智能）
  ✅ 完整功能100%保留
  ✅ 点击表格行即触发分析
  ✅ 网络重试机制（指数退避）
  ✅ 交易日15分钟/非交易日24小时缓存
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import requests
import os
import time
import json
import urllib3
from datetime import datetime
from pathlib import Path
import pickle
import hashlib
import shutil
import logging

# ── AI市场简报 12小时文件缓存 ──────────────────────────────────────────────
_BRIEF_CACHE_DIR = Path(__file__).parent / ".cache_brief"
_BRIEF_CACHE_FILE = _BRIEF_CACHE_DIR / "daily_brief.json"
_BRIEF_CACHE_TTL = 12 * 3600  # 12小时（由 config.toml [cache].brief_ttl 覆盖，见 Config 初始化后重设）


def _load_brief_cache():
    """加载简报文件缓存，命中（<12h）返回 (content, ts)，否则 (None, None)"""
    try:
        if _BRIEF_CACHE_FILE.exists():
            data = json.loads(_BRIEF_CACHE_FILE.read_text(encoding="utf-8"))
            age = time.time() - data.get("timestamp", 0)
            if age < _BRIEF_CACHE_TTL:
                return data.get("content"), data.get("timestamp")
    except Exception:
        pass
    return None, None


def _save_brief_cache(content: str):
    """将简报内容保存到文件缓存，同时追加历史推荐记录"""
    try:
        _BRIEF_CACHE_DIR.mkdir(exist_ok=True)
        _BRIEF_CACHE_FILE.write_text(
            json.dumps({"content": content, "timestamp": time.time()}, ensure_ascii=False),
            encoding="utf-8",
        )
        # 追加今日推荐记录到历史（用于跨日去重）
        _append_brief_history(content)
    except Exception as _e:
        logging.warning(f"简报缓存写入失败: {_e}")


_BRIEF_HISTORY_FILE = _BRIEF_CACHE_DIR / "brief_history.json"

def _append_brief_history(content: str):
    """从简报内容中提取推荐代码，保存到历史文件（保留最近7天）"""
    import re as _re
    # 匹配 **名称(代码)** 格式，提取括号内的代码
    codes = _re.findall(r'\*\*[^(（]+[（(]([A-Za-z0-9.]+)[)）]\*\*', content)
    if not codes:
        return
    try:
        _BRIEF_CACHE_DIR.mkdir(exist_ok=True)
        history = []
        if _BRIEF_HISTORY_FILE.exists():
            history = json.loads(_BRIEF_HISTORY_FILE.read_text(encoding="utf-8"))
        today_str = __import__("datetime").date.today().isoformat()
        # 去掉7天前的记录
        cutoff = time.time() - 7 * 86400
        history = [r for r in history if r.get("ts", 0) > cutoff]
        history.append({"date": today_str, "ts": time.time(), "codes": list(set(codes))})
        _BRIEF_HISTORY_FILE.write_text(json.dumps(history, ensure_ascii=False), encoding="utf-8")
    except Exception as _e:
        logging.warning(f"历史推荐记录写入失败: {_e}")


def _get_recent_recommended_codes(days: int = 3) -> list:
    """读取最近N天已推荐的股票代码列表（去重）"""
    try:
        if not _BRIEF_HISTORY_FILE.exists():
            return []
        history = json.loads(_BRIEF_HISTORY_FILE.read_text(encoding="utf-8"))
        cutoff = time.time() - days * 86400
        codes = []
        for r in history:
            if r.get("ts", 0) > cutoff:
                codes.extend(r.get("codes", []))
        return list(set(codes))
    except Exception:
        return []
# ─────────────────────────────────────────────────────────────────────────────

def _safe_print(*args, **kwargs):
    """避免 Streamlit 重载时 stdout 关闭导致的 ValueError"""
    try:
        import builtins
        builtins.print(*args, **kwargs)
    except (ValueError, OSError):
        logging.debug(f"_safe_print: {args} {kwargs}")


def _safe_str_for_dom(val):
    """移除控制字符、NaN、Inf 等，防止 InvalidCharacterError。用于 st.metric / st.markdown 等"""
    if val is None:
        return ""
    s = str(val)
    sl = s.lower().strip()
    if sl in ("nan", "inf", "-inf", "infinity", "-infinity"):
        return "N/A"
    if sl.startswith("nan") or sl.startswith("inf") or sl.startswith("-inf"):
        return "N/A"
    try:
        v = float(val)
        if v != v or v == float("inf") or v == float("-inf"):
            return "N/A"
    except (TypeError, ValueError):
        pass
    out = "".join(c for c in s if ord(c) >= 32 or c in "\n\t\r")
    if not out:
        return "N/A"
    return out

# 【V88】导入新模块
try:
    from modules import config as mod_config
    from modules import cache as mod_cache
    from modules import utils as mod_utils
    from modules import data_fetch as mod_data
    from modules import stock_pool as mod_pool
    from modules import analysis_core as mod_analysis
    from modules import ai_engine as mod_ai
    from modules import ui_components as mod_ui
    USE_NEW_MODULES = True
    _safe_print("✅ V88模块已加载（8个模块）")
except ImportError as e:
    USE_NEW_MODULES = False
    _safe_print(f"⚠️  V88模块未找到，使用原版逻辑: {e}")

# 【选股引擎】AI市场日报 684池筛选
try:
    from modules import selection_engine as mod_selection
    SELECTION_ENGINE_AVAILABLE = True
    _safe_print("✅ 选股引擎已加载（684池+ST/MT/LT）")
except ImportError as e:
    SELECTION_ENGINE_AVAILABLE = False
    mod_selection = None
    _safe_print(f"⚠️  选股引擎未找到，日报使用 pool[:15]: {e}")

# 【V89.2】导入机构研究中心
try:
    from institutional_research import InstitutionalResearch
    INSTITUTIONAL_RESEARCH_AVAILABLE = True
    _safe_print("✅ 机构研究中心模块已加载")
except ImportError as e:
    INSTITUTIONAL_RESEARCH_AVAILABLE = False
    _safe_print(f"⚠️  机构研究中心模块未找到: {e}")

# 【V89.3】导入持仓管理
try:
    from portfolio_manager import PortfolioManager
    PORTFOLIO_MANAGER_AVAILABLE = True
    _safe_print("✅ 持仓管理模块已加载")
except ImportError as e:
    PORTFOLIO_MANAGER_AVAILABLE = False
    _safe_print(f"⚠️  持仓管理模块未找到: {e}")

# 【Regime-Adaptive】导入市场状态自适应筛选引擎
try:
    from modules.regime import (
        MarketRegime, StrategyRouter, OpportunityClassifier,
        RiskForecaster, ActionEngine, QualityGuard, ReportComposer,
        LongCompounderGate, MarginOfSafetyGate,
        get_position_level_unified, ExpectationGapEngine,
    )
    REGIME_ENGINE_AVAILABLE = True
    _safe_print("✅ 市场状态自适应筛选引擎已加载")
except ImportError as e:
    REGIME_ENGINE_AVAILABLE = False
    ExpectationGapEngine = None
    LongCompounderGate = None
    MarginOfSafetyGate = None
    _safe_print(f"⚠️  市场状态自适应引擎未找到: {e}")

# 【潜力股双引擎】开关：True=双引擎+三池，False=回滚至原单一质量引擎
USE_POTENTIAL_ENGINE = True

# 【V89.4】导入舆情分析中心
try:
    from sentiment_analyzer import SentimentAnalyzer
    SENTIMENT_ANALYZER_AVAILABLE = True
    _safe_print("✅ 舆情分析中心模块已加载")
except ImportError as e:
    SENTIMENT_ANALYZER_AVAILABLE = False
    _safe_print(f"⚠️  舆情分析中心模块未找到: {e}")

# 【V89.5】导入复制和报告生成工具
try:
    from copy_utils import CopyUtils, ReportGenerator, ShareCardGenerator
    COPY_UTILS_AVAILABLE = True
    _safe_print("✅ 复制和报告生成工具已加载")
except ImportError as e:
    COPY_UTILS_AVAILABLE = False
    _safe_print(f"⚠️  复制和报告生成工具未找到: {e}")

# 【V88.12】导入预测引擎模块
try:
    from prediction_engine import InstitutionalPredictor, analyze_stock_with_predictor
    from market_forecast import MarketForecaster, forecast_all_markets
    HAS_PREDICTION_ENGINE = True
    _safe_print("✅ 前瞻预测引擎已加载")
except ImportError as e:
    HAS_PREDICTION_ENGINE = False
    _safe_print(f"⚠️  预测引擎未找到: {e}")

# 【V87.16 + V88】配置日志系统
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logging.info("=" * 60)
logging.info("🎉 AI 皇冠双核 V88 集成版启动")
logging.info("=" * 60)
if USE_NEW_MODULES:
    logging.info(f"✅ 模块化架构: V{mod_config.APP_VERSION}")
    logging.info(f"✅ LRU缓存系统: {mod_config.CACHE_MAX_SIZE_MB}MB")
    logging.info(f"✅ 缓存TTL: {mod_config.CACHE_TTL_SECONDS}秒")
logging.info("=" * 60)

# 尝试导入 yfinance
try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

# 【V87.11】导入 Google Gemini API
try:
    import google.generativeai as genai
    HAS_GEMINI = True
except ImportError:
    HAS_GEMINI = False
    
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ═══════════════════════════════════════════════════════════════
# 配置中心：先读 config.toml，缺失项用内置默认值
# ═══════════════════════════════════════════════════════════════
def _load_config_toml() -> dict:
    try:
        import tomllib  # Python 3.11+
    except ImportError:
        try:
            import tomli as tomllib  # pip install tomli
        except ImportError:
            return {}
    _p = Path(__file__).parent / "config.toml"
    if _p.exists():
        with open(_p, "rb") as f:
            return tomllib.load(f)
    return {}

_TOML = _load_config_toml()


class Config:
    """全局配置中心 — 优先读 config.toml，缺失项回退到内置默认值"""

    ENABLE_EXPECTATION_LAYER = _TOML.get("features", {}).get("enable_expectation_layer", True)
    ENABLE_PERF_LAYER        = _TOML.get("features", {}).get("enable_perf_layer", True)

    CACHE_TTL       = _TOML.get("cache", {}).get("ttl_daily", 3600)
    RETRY_COUNT     = _TOML.get("data", {}).get("retry_count", 3)
    REQUEST_TIMEOUT = _TOML.get("data", {}).get("request_timeout", 8)

    CACHE_TTL_FAST   = _TOML.get("cache", {}).get("ttl_fast",   900)
    CACHE_TTL_DAILY  = _TOML.get("cache", {}).get("ttl_daily",  3600)
    CACHE_TTL_WEEKLY = _TOML.get("cache", {}).get("ttl_weekly", 21600)

    MAX_WORKERS  = _TOML.get("concurrency", {}).get("max_workers",  8)
    TASK_TIMEOUT = _TOML.get("concurrency", {}).get("task_timeout", 15)

    MACRO_ASSETS = ['SPY', 'TLT', 'GLD', '^VIX', '^TNX', 'DX-Y.NYB']

    TNX_LOOSE = _TOML.get("rates", {}).get("tnx_loose", 3.5)
    TNX_TIGHT = _TOML.get("rates", {}).get("tnx_tight", 4.5)

    DXY_WEAK   = _TOML.get("dollar", {}).get("dxy_weak",   100)
    DXY_STRONG = _TOML.get("dollar", {}).get("dxy_strong", 105)

    MA_SHORT    = _TOML.get("technical", {}).get("ma_short",    50)
    MA_LONG     = _TOML.get("technical", {}).get("ma_long",     200)
    CORR_WINDOW = _TOML.get("technical", {}).get("corr_window", 20)

    VIX_PANIC = _TOML.get("vix", {}).get("panic", 30)
    VIX_HIGH  = _TOML.get("vix", {}).get("high",  20)
    VIX_LOW   = _TOML.get("vix", {}).get("low",   15)

    MACRO_PERIOD = _TOML.get("data", {}).get("macro_period", "1y")

    SMART_CACHE_ENABLED     = True
    CACHE_TTL_WORKDAY       = _TOML.get("cache", {}).get("ttl_workday", 900)
    SCAN_CACHE_TTL          = _TOML.get("cache", {}).get("scan_ttl",    900)
    CACHE_TTL_WEEKEND       = _TOML.get("cache", {}).get("ttl_weekend", 86400)
    CACHE_TTL_TRADING_HOURS = _TOML.get("cache", {}).get("ttl_workday", 900)

    PORTFOLIO_FILE    = 'my_portfolio.xlsx'
    PORTFOLIO_ENABLED = True


# ═══════════════════════════════════════════════════════════════
# 【V89.3 + V91.3】智能缓存 - 交易日15分钟，非交易日24小时
# ═══════════════════════════════════════════════════════════════

def get_smart_cache_ttl(data_type: str = 'daily') -> int:
    """
    智能缓存TTL - 根据是否交易日返回合适的TTL
    
    规则：
    - 交易日（周一到周五）：15分钟
    - 非交易日（周六日）：24小时
    
    参数：
        data_type: 数据类型（'fast'/'daily'/'weekly'）
    
    返回：
        TTL秒数
    """
    if not Config.SMART_CACHE_ENABLED:
        # 如果未启用智能缓存，使用默认配置
        if data_type == 'fast':
            return Config.CACHE_TTL_FAST
        elif data_type == 'weekly':
            return Config.CACHE_TTL_WEEKLY
        else:
            return Config.CACHE_TTL_DAILY
    
    from datetime import datetime
    import pytz
    
    try:
        # 获取当前时间（美东时间，因为美股市场）
        now_et = datetime.now(pytz.timezone('America/New_York'))
        weekday = now_et.weekday()  # 0=周一, 6=周日
        hour = now_et.hour
        
        # 判断是否为非交易日（周六日）
        if weekday >= 5:  # 5=周六, 6=周日
            return Config.CACHE_TTL_WEEKEND  # 24小时
        
        # 交易日（周一至周五）：15分钟
        return Config.CACHE_TTL_WORKDAY
    
    except Exception as e:
        # 异常时使用默认配置
        logging.warning(f"智能缓存TTL计算异常: {e}，使用默认配置")
        return Config.CACHE_TTL_DAILY


# ═══════════════════════════════════════════════════════════════
# 【V91.4】扫描结果文件持久化缓存 - 跨会话/刷新后仍有效（15分钟/24小时）
# ═══════════════════════════════════════════════════════════════

SCAN_CACHE_DIR = Path(__file__).resolve().parent / "scan_cache"

def _scan_cache_key(scan_type: str, scan_market: str, risk_pref: str = None) -> str:
    """生成扫描缓存文件键"""
    key = f"{scan_type}_{scan_market}"
    if scan_type == 'regime' and risk_pref:
        key += f"_{risk_pref}"
    return key.replace(" ", "_")

def _load_scan_cache_from_file(scan_type: str, scan_market: str, risk_pref: str = None):
    """从文件加载扫描缓存，命中则返回结果 dict，否则返回 None"""
    try:
        SCAN_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        ckey = _scan_cache_key(scan_type, scan_market, risk_pref)
        fp = SCAN_CACHE_DIR / f"{ckey}.pkl"
        if not fp.exists():
            return None
        with open(fp, "rb") as f:
            data = pickle.load(f)
        if not isinstance(data, dict):
            return None
        ts = data.get("scan_timestamp", 0)
        ttl = get_smart_cache_ttl("daily")
        if (time.time() - ts) >= ttl:
            return None
        if data.get("type") != scan_type or data.get("scan_market") != scan_market:
            return None
        if scan_type == "regime" and risk_pref and data.get("risk_preference") != risk_pref:
            return None
        return data
    except Exception as e:
        logging.debug(f"加载扫描缓存失败: {e}")
        return None

def _save_scan_cache_to_file(data: dict):
    """将扫描结果保存到文件"""
    try:
        SCAN_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        stype = data.get("type", "")
        mkt = data.get("scan_market", "")
        rpref = data.get("risk_preference") if stype == "regime" else None
        ckey = _scan_cache_key(stype, mkt, rpref)
        fp = SCAN_CACHE_DIR / f"{ckey}.pkl"
        with open(fp, "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as e:
        logging.debug(f"保存扫描缓存失败: {e}")

def _clear_scan_cache_files():
    """清除所有扫描缓存文件（与清除按钮联动）"""
    try:
        if SCAN_CACHE_DIR.exists():
            for f in SCAN_CACHE_DIR.glob("*.pkl"):
                f.unlink()
    except Exception as e:
        logging.debug(f"清除扫描缓存文件失败: {e}")


# ═══════════════════════════════════════════════════════════════
# 【V89 Phase 2】性能优化层 - 分层缓存 + 性能监控
# ═══════════════════════════════════════════════════════════════

class PerformanceMonitor:
    """
    性能监控器 - 记录各阶段耗时和缓存命中率
    目标：可观测性、性能调优、问题定位
    """
    def __init__(self):
        self.metrics = {
            'fetch_time_ms': 0,
            'compute_time_ms': 0,
            'render_time_ms': 0,
            'total_time_ms': 0,
            'cache_hit_count': 0,
            'cache_miss_count': 0,
            'cache_items_count': 0,
            'stale_fallback_count': 0,
            'error_count': 0
        }
        self.start_time = None
    
    def start(self):
        """开始计时"""
        self.start_time = time.time()
    
    def record(self, stage: str, elapsed_ms: float):
        """记录某阶段耗时"""
        key = f"{stage}_time_ms"
        if key in self.metrics:
            self.metrics[key] += elapsed_ms
    
    def cache_hit(self):
        """缓存命中"""
        self.metrics['cache_hit_count'] += 1
    
    def cache_miss(self):
        """缓存未命中"""
        self.metrics['cache_miss_count'] += 1
    
    def stale_fallback(self):
        """使用过期缓存"""
        self.metrics['stale_fallback_count'] += 1
    
    def error(self):
        """记录错误"""
        self.metrics['error_count'] += 1
    
    def get_cache_hit_ratio(self) -> float:
        """计算缓存命中率"""
        total = self.metrics['cache_hit_count'] + self.metrics['cache_miss_count']
        if total == 0:
            return 0.0
        return self.metrics['cache_hit_count'] / total
    
    def finalize(self):
        """结束计时，计算总耗时"""
        if self.start_time:
            self.metrics['total_time_ms'] = (time.time() - self.start_time) * 1000
    
    def get_metrics(self) -> dict:
        """获取所有指标"""
        return self.metrics.copy()
    
    def reset(self):
        """重置所有指标"""
        self.__init__()


class LayeredCacheManager:
    """
    分层缓存管理器 - 按数据类型分配不同TTL
    目标：高频数据快速过期，低频数据长期缓存
    """
    def __init__(self, perf_monitor: PerformanceMonitor = None):
        self._cache = {}  # {key: {'value': data, 'ts': timestamp, 'type': data_type}}
        self.perf = perf_monitor or PerformanceMonitor()
        self.logger = logging.getLogger(__name__)
    
    def _get_ttl(self, data_type: str) -> int:
        """
        根据数据类型返回TTL
        
        【V89.3】使用智能缓存：工作日10分钟，休息日24小时
        """
        if Config.SMART_CACHE_ENABLED:
            # 使用智能缓存TTL
            return get_smart_cache_ttl(data_type)
        else:
            # 使用固定TTL
            ttl_map = {
                'fast': Config.CACHE_TTL_FAST,    # 15分钟
                'daily': Config.CACHE_TTL_DAILY,  # 1小时
                'weekly': Config.CACHE_TTL_WEEKLY  # 6小时
            }
            return ttl_map.get(data_type, Config.CACHE_TTL)
    
    def get(self, key: str, data_type: str = 'daily', force_refresh: bool = False):
        """
        获取缓存
        
        返回：(value, is_stale)
        - value: 缓存值或None
        - is_stale: 是否过期（True=过期但可用，False=新鲜）
        """
        if force_refresh:
            self.logger.info(f"🔄 强制刷新: {key}")
            self.perf.cache_miss()
            return None, False
        
        if key not in self._cache:
            self.perf.cache_miss()
            return None, False
        
        cached = self._cache[key]
        age = time.time() - cached['ts']
        ttl = self._get_ttl(data_type)
        
        if age < ttl:
            # 缓存新鲜
            self.perf.cache_hit()
            self.logger.info(f"✅ 缓存命中: {key} (新鲜度: {int(age)}秒/{ttl}秒)")
            return cached['value'], False
        else:
            # 缓存过期但仍可用
            self.perf.stale_fallback()
            self.logger.warning(f"⚠️  缓存过期: {key} (已过期: {int(age-ttl)}秒)")
            return cached['value'], True
    
    def set(self, key: str, value, data_type: str = 'daily'):
        """设置缓存"""
        self._cache[key] = {
            'value': value,
            'ts': time.time(),
            'type': data_type
        }
        self.logger.info(f"💾 缓存已保存: {key} (类型: {data_type}, TTL: {self._get_ttl(data_type)}秒)")
    
    def clear(self, key: str = None):
        """清除缓存"""
        if key:
            if key in self._cache:
                del self._cache[key]
                self.logger.info(f"🗑️  已清除缓存: {key}")
        else:
            count = len(self._cache)
            self._cache.clear()
            self.logger.info(f"🗑️  已清除所有缓存: {count}项")
    
    def get_stats(self) -> dict:
        """获取缓存统计"""
        return {
            'items_count': len(self._cache),
            'total_size_mb': sum(
                len(str(v['value'])) for v in self._cache.values()
            ) / 1024 / 1024
        }


# 全局实例（用于性能监控）
_perf_monitor = PerformanceMonitor()
_cache_manager = LayeredCacheManager(_perf_monitor)

# ═══════════════════════════════════════════════════════════════

class DataProvider:
    """
    安全数据层 - 容错、缓存兜底、优雅降级
    目标：任何数据获取失败都不会让应用崩溃
    
    【V89 Phase 2】新增：
    - 集成分层缓存管理器
    - 支持force_refresh参数
    - 集成性能监控
    """
    def __init__(self, cache_manager: LayeredCacheManager = None, perf_monitor: PerformanceMonitor = None):
        self._memory_cache = {}  # 保留旧缓存（向后兼容）
        self.cache_mgr = cache_manager or _cache_manager
        self.perf = perf_monitor or _perf_monitor
        self.logger = logging.getLogger(__name__)
    
    def fetch_safe(self, symbol: str, period: str = '1y', data_type: str = 'daily', force_refresh: bool = False, min_rows: int = 20) -> pd.DataFrame:
        """
        安全获取股票数据，带容错和缓存兜底
        
        参数：
            symbol: 股票代码
            period: 数据周期（默认1年）
            data_type: 数据类型（fast/daily/weekly），决定缓存TTL
            force_refresh: 强制刷新，忽略缓存
            min_rows: 最少行数（默认20；补充指标仅需2行可传 min_rows=2）
        
        返回：
            DataFrame or None（失败时返回None，不抛异常）
        """
        cache_key = f"{symbol}_{period}"
        start_time = time.time()
        
        # 1. 检查分层缓存
        cached_value, is_stale = self.cache_mgr.get(cache_key, data_type, force_refresh)
        if cached_value is not None and not is_stale:
            # 缓存新鲜，直接返回
            elapsed = (time.time() - start_time) * 1000
            self.perf.record('fetch', elapsed)
            return cached_value
        
        # 2a. A股：优先 Tushare（全球可用，覆盖率高）
        if symbol.endswith(".SS") or symbol.endswith(".SZ"):
            try:
                from ts_helper import fetch_df as _ts_fetch
                _ts_df = _ts_fetch(symbol, period=period)
                if _ts_df is not None and len(_ts_df) >= min_rows:
                    self.cache_mgr.set(cache_key, _ts_df, data_type)
                    elapsed = (time.time() - start_time) * 1000
                    self.perf.record('fetch', elapsed)
                    self.logger.info(f"✅ Tushare 获取 {symbol}，共 {len(_ts_df)} 条记录")
                    return _ts_df
            except Exception as _e:
                self.logger.debug(f"Tushare {symbol} 失败，降级 yfinance: {_e}")

        # 2b. 尝试从 yfinance 获取（带重试）
        for attempt in range(Config.RETRY_COUNT):
            try:
                self.logger.info(f"📊 正在获取 {symbol} 数据... (尝试 {attempt+1}/{Config.RETRY_COUNT})")
                ticker = yf.Ticker(symbol)
                df = ticker.history(period=period, timeout=Config.REQUEST_TIMEOUT)
                
                if df is not None and not df.empty and len(df) >= min_rows:
                    # 成功获取，更新缓存
                    self.cache_mgr.set(cache_key, df, data_type)
                    elapsed = (time.time() - start_time) * 1000
                    self.perf.record('fetch', elapsed)
                    self.logger.info(f"✅ 成功获取 {symbol} 数据，共 {len(df)} 条记录")
                    return df
                else:
                    self.logger.warning(f"⚠️  {symbol} 数据为空或过少")
            
            except Exception as e:
                self.logger.warning(f"⚠️  {symbol} 获取失败 (尝试 {attempt+1}): {str(e)[:100]}")
                self.perf.error()
                if attempt < Config.RETRY_COUNT - 1:
                    time.sleep(1 * (attempt + 1))  # 递增延迟
        
        # 3. 所有尝试失败，返回过期缓存（如果有）
        if cached_value is not None:
            self.logger.warning(f"⚠️  {symbol} 获取失败，使用过期缓存")
            elapsed = (time.time() - start_time) * 1000
            self.perf.record('fetch', elapsed)
            return cached_value
        
        # 4. 完全失败，返回None
        self.logger.error(f"❌ {symbol} 数据获取完全失败，无可用缓存")
        elapsed = (time.time() - start_time) * 1000
        self.perf.record('fetch', elapsed)
        return None
    
    def fetch_batch_concurrent(self, symbols: list, period: str = '1y', data_type: str = 'daily', force_refresh: bool = False) -> dict:
        """
        【V89 Phase 2】并发批量获取多个标的数据
        
        参数：
            symbols: 股票代码列表
            period: 数据周期
            data_type: 数据类型
            force_refresh: 强制刷新
        
        返回：
            {symbol: DataFrame or None}
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        results = {}
        errors = []
        
        def fetch_one(sym):
            try:
                df = self.fetch_safe(sym, period, data_type, force_refresh)
                return sym, df, None
            except Exception as e:
                return sym, None, str(e)
        
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_one, sym): sym for sym in symbols}
            
            for future in as_completed(futures, timeout=Config.TASK_TIMEOUT * len(symbols)):
                try:
                    sym, df, error = future.result(timeout=Config.TASK_TIMEOUT)
                    results[sym] = df
                    if error:
                        errors.append(f"{sym}: {error}")
                        self.perf.error()
                except Exception as e:
                    sym = futures[future]
                    results[sym] = None
                    errors.append(f"{sym}: 任务超时或异常")
                    self.perf.error()
        
        if errors:
            self.logger.warning(f"⚠️  批量获取部分失败: {'; '.join(errors[:5])}")
        
        return results


class ExpectationLayer:
    """
    宏观预期层 - 基于SPY/TLT/VIX判断市场体制
    目标：Risk On / Risk Off / Neutral 三态裁决
    
    【V89 Phase 2】新增：
    - 支持force_refresh参数
    - 集成性能监控
    - 参数签名机制（增量刷新）
    
    【V89.1 优化】新增：
    - 支持多市场分析（美股/港股/A股）
    - 每个市场独立裁决
    - 综合市场联动分析
    """
    def __init__(self, data_provider: DataProvider, perf_monitor: PerformanceMonitor = None):
        self.dp = data_provider
        self.perf = perf_monitor or _perf_monitor
        self.logger = logging.getLogger(__name__)
        self._last_param_hash = None
        self._last_result = None
        self._last_multi_result = None  # 多市场结果缓存
    
    def _compute_param_hash(self) -> str:
        """计算参数签名（用于增量刷新）"""
        import hashlib
        params = f"{Config.MA_SHORT}_{Config.MA_LONG}_{Config.CORR_WINDOW}_{Config.VIX_PANIC}_{Config.VIX_HIGH}"
        return hashlib.md5(params.encode()).hexdigest()[:8]
    
    def analyze_market_regime(self, force_refresh: bool = False) -> dict:
        """
        分析当前市场体制
        
        【V89 Phase 2】新增参数：
            force_refresh: 强制刷新，忽略缓存和参数签名
        
        返回字典：
            verdict: 'Risk On' / 'Risk Off' / 'Neutral'
            vix_level: VIX数值
            vix_status: VIX状态描述
            correlation: SPY与TLT的相关性
            spy_price: SPY最新价格
            ma50: SPY的50日均线
            ma200: SPY的200日均线
            reason: 裁决理由（中文）
            data_ok: 数据是否完整
        """
        start_time = time.time()
        
        try:
            # 【V89 Phase 2】增量刷新：检查参数签名
            current_hash = self._compute_param_hash()
            if not force_refresh and current_hash == self._last_param_hash and self._last_result is not None:
                self.logger.info(f"✅ 参数未变化，使用缓存结果（签名: {current_hash}）")
                elapsed = (time.time() - start_time) * 1000
                self.perf.record('compute', elapsed)
                return self._last_result
            
            # 1. 获取数据（使用分层缓存）
            self.logger.info("🔍 开始分析宏观市场体制...")
            spy_df = self.dp.fetch_safe('SPY', period=Config.MACRO_PERIOD, data_type='weekly', force_refresh=force_refresh)
            tlt_df = self.dp.fetch_safe('TLT', period=Config.MACRO_PERIOD, data_type='weekly', force_refresh=force_refresh)
            vix_df = self.dp.fetch_safe('^VIX', period=Config.MACRO_PERIOD, data_type='fast', force_refresh=force_refresh)
            
            # 2. 检查数据完整性
            if spy_df is None or len(spy_df) < 200:
                return self._fallback_result("SPY数据不足，无法计算MA200", 'us')
            
            if vix_df is None or vix_df.empty:
                return self._fallback_result("VIX数据不可用", 'us')
            
            # 3. 计算技术指标
            spy_price = float(spy_df['Close'].iloc[-1])
            spy_df['MA50'] = spy_df['Close'].rolling(window=Config.MA_SHORT).mean()
            spy_df['MA200'] = spy_df['Close'].rolling(window=Config.MA_LONG).mean()
            ma50 = float(spy_df['MA50'].iloc[-1])
            ma200 = float(spy_df['MA200'].iloc[-1])
            
            # 4. 计算SPY与TLT的相关性
            correlation = 0.0
            corr_desc = "数据不足"
            if tlt_df is not None and len(tlt_df) >= Config.CORR_WINDOW:
                # 对齐日期
                common_dates = spy_df.index.intersection(tlt_df.index)
                if len(common_dates) >= Config.CORR_WINDOW:
                    spy_aligned = spy_df.loc[common_dates, 'Close']
                    tlt_aligned = tlt_df.loc[common_dates, 'Close']
                    
                    # 计算滚动相关性并取最新值
                    rolling_corr = spy_aligned.rolling(window=Config.CORR_WINDOW).corr(tlt_aligned)
                    correlation = float(rolling_corr.iloc[-1]) if not np.isnan(rolling_corr.iloc[-1]) else 0.0
                    
                    # 相关性解读
                    if correlation > 0.3:
                        corr_desc = "股债同向（宏观冲击主导）"
                    elif correlation < -0.3:
                        corr_desc = "股债跷跷板（避险切换明显）"
                    else:
                        corr_desc = "相关性弱（风格轮动为主）"
            
            # 5. VIX分析
            vix_level = float(vix_df['Close'].iloc[-1])
            vix_prev = float(vix_df['Close'].iloc[-2]) if len(vix_df) >= 2 else vix_level
            vix_change_pct = ((vix_level - vix_prev) / vix_prev * 100) if vix_prev != 0 else 0
            if vix_level > Config.VIX_PANIC:
                vix_status = "⚠️ 极度恐慌（现金为王）"
            elif vix_level > Config.VIX_HIGH:
                vix_status = "📈 高波动（需对冲）"
            elif vix_level < Config.VIX_LOW:
                vix_status = "📉 低波动（趋势延续）"
            else:
                vix_status = "📊 中等波动（均衡应对）"
            
            # 5.1 【V90 新增】10Y美债收益率 (^TNX)
            tnx_yield = 0.0
            tnx_change = 0.0
            tnx_status = "数据不可用"
            try:
                tnx_df = self.dp.fetch_safe('^TNX', period='6mo', data_type='fast', force_refresh=force_refresh)
                if tnx_df is not None and not tnx_df.empty:
                    tnx_yield = float(tnx_df['Close'].iloc[-1])
                    tnx_prev = float(tnx_df['Close'].iloc[-2]) if len(tnx_df) >= 2 else tnx_yield
                    tnx_change = tnx_yield - tnx_prev  # 收益率用绝对变动
                    if tnx_yield < Config.TNX_LOOSE:
                        tnx_status = "🟢 宽松（利好成长股）"
                    elif tnx_yield > Config.TNX_TIGHT:
                        tnx_status = "🔴 偏紧缩（利空高估值）"
                    else:
                        tnx_status = "🟡 中性区间"
            except Exception as e:
                self.logger.warning(f"⚠️ TNX数据获取失败: {e}")
            
            # 5.2 【V90 新增】美元指数 (DX-Y.NYB)
            dxy_level = 0.0
            dxy_change_pct = 0.0
            dxy_status = "数据不可用"
            try:
                dxy_df = self.dp.fetch_safe('DX-Y.NYB', period='6mo', data_type='fast', force_refresh=force_refresh)
                if dxy_df is not None and not dxy_df.empty:
                    dxy_level = float(dxy_df['Close'].iloc[-1])
                    dxy_prev = float(dxy_df['Close'].iloc[-2]) if len(dxy_df) >= 2 else dxy_level
                    dxy_change_pct = ((dxy_level - dxy_prev) / dxy_prev * 100) if dxy_prev != 0 else 0
                    if dxy_level < Config.DXY_WEAK:
                        dxy_status = "🟢 弱美元（利好新兴/大宗）"
                    elif dxy_level > Config.DXY_STRONG:
                        dxy_status = "🔴 强美元（资金回流美国）"
                    else:
                        dxy_status = "🟡 中性区间"
            except Exception as e:
                self.logger.warning(f"⚠️ DXY数据获取失败: {e}")
            
            # 5.3 【V90 新增】黄金 (GLD) 日变动
            gld_price = 0.0
            gld_change_pct = 0.0
            gld_status = "数据不可用"
            try:
                gld_df = self.dp.fetch_safe('GLD', period=Config.MACRO_PERIOD, data_type='weekly', force_refresh=force_refresh)
                if gld_df is not None and not gld_df.empty:
                    gld_price = float(gld_df['Close'].iloc[-1])
                    gld_prev = float(gld_df['Close'].iloc[-2]) if len(gld_df) >= 2 else gld_price
                    gld_change_pct = ((gld_price - gld_prev) / gld_prev * 100) if gld_prev != 0 else 0
                    # 黄金涨+VIX涨=避险情绪浓
                    if gld_change_pct > 1.0:
                        gld_status = "📈 避险需求上升"
                    elif gld_change_pct < -1.0:
                        gld_status = "📉 风险偏好回暖"
                    else:
                        gld_status = "📊 持平"
            except Exception as e:
                self.logger.warning(f"⚠️ GLD数据获取失败: {e}")
            
            # 5.4 SPY/TLT 日变动
            spy_prev = float(spy_df['Close'].iloc[-2]) if len(spy_df) >= 2 else spy_price
            spy_change_pct = ((spy_price - spy_prev) / spy_prev * 100) if spy_prev != 0 else 0
            tlt_price = 0.0
            tlt_change_pct = 0.0
            if tlt_df is not None and not tlt_df.empty:
                tlt_price = float(tlt_df['Close'].iloc[-1])
                tlt_prev = float(tlt_df['Close'].iloc[-2]) if len(tlt_df) >= 2 else tlt_price
                tlt_change_pct = ((tlt_price - tlt_prev) / tlt_prev * 100) if tlt_prev != 0 else 0
            
            # 6. 市场体制裁决（【V90】增强：加入美债+美元因素）
            verdict = "Neutral"
            reason_parts = []
            
            # Risk Off条件
            if vix_level > 25:
                verdict = "Risk Off"
                reason_parts.append(f"VIX={vix_level:.1f}>25（恐慌）")
            elif spy_price < ma200:
                verdict = "Risk Off"
                reason_parts.append(f"SPY({spy_price:.1f}) < MA200({ma200:.1f})")
            
            # Risk On条件
            elif spy_price > ma50 and vix_level < Config.VIX_HIGH:
                verdict = "Risk On"
                reason_parts.append(f"SPY({spy_price:.1f}) > MA50({ma50:.1f})")
                reason_parts.append(f"VIX={vix_level:.1f}<20（低波动）")
            
            # Neutral
            else:
                reason_parts.append(f"SPY在MA50({ma50:.1f})与MA200({ma200:.1f})之间")
                reason_parts.append(f"VIX={vix_level:.1f}（中性）")
            
            # 【V90】美债紧缩警告叠加
            if tnx_yield > Config.TNX_TIGHT:
                reason_parts.append(f"⚠️ 10Y美债{tnx_yield:.2f}%偏高，流动性紧缩")
            if dxy_level > Config.DXY_STRONG:
                reason_parts.append(f"⚠️ 美元指数{dxy_level:.1f}偏强，资金回流美国")
            
            # 【V90】仓位上限建议（基于宏观综合）
            position_cap = 80  # 默认80%
            if verdict == "Risk Off":
                position_cap = 30
            elif verdict == "Neutral":
                position_cap = 60
            if tnx_yield > Config.TNX_TIGHT and position_cap > 60:
                position_cap = 60  # 紧缩环境下降仓位上限
            
            reason = "；".join(reason_parts)
            
            self.logger.info(f"✅ 市场体制分析完成: {verdict} - {reason}")
            
            # 【V89 Phase 2】缓存结果和参数签名
            result = {
                'verdict': verdict,
                'vix_level': vix_level,
                'vix_change_pct': vix_change_pct,
                'vix_status': vix_status,
                'correlation': correlation,
                'corr_desc': corr_desc,
                'spy_price': spy_price,
                'spy_change_pct': spy_change_pct,
                'ma50': ma50,
                'ma200': ma200,
                'tlt_price': tlt_price,
                'tlt_change_pct': tlt_change_pct,
                'tnx_yield': tnx_yield,
                'tnx_change': tnx_change,
                'tnx_status': tnx_status,
                'dxy_level': dxy_level,
                'dxy_change_pct': dxy_change_pct,
                'dxy_status': dxy_status,
                'gld_price': gld_price,
                'gld_change_pct': gld_change_pct,
                'gld_status': gld_status,
                'position_cap': position_cap,
                'reason': reason,
                'data_ok': True
            }
            
            self._last_result = result
            self._last_param_hash = current_hash
            
            elapsed = (time.time() - start_time) * 1000
            self.perf.record('compute', elapsed)
            
            return result
        
        except Exception as e:
            self.logger.error(f"❌ 美股宏观分析异常: {str(e)}")
            import traceback
            traceback.print_exc()
            return self._fallback_result(f"美股分析异常: {str(e)[:50]}", 'us')
    
    def _fallback_result(self, reason: str, market_type: str = 'us') -> dict:
        """
        降级结果 - 数据不足时返回
        
        【V89.6.7 修复】支持不同市场类型
        market_type: 'us' (美股) / 'hk' (港股) / 'cn' (A股)
        """
        if market_type == 'us':
            # 美股降级数据
            return {
                'verdict': 'Unknown',
                'vix_level': 0.0,
                'vix_change_pct': 0.0,
                'vix_status': '数据不可用',
                'correlation': 0.0,
                'corr_desc': '数据不可用',
                'spy_price': 0.0,
                'spy_change_pct': 0.0,
                'ma50': 0.0,
                'ma200': 0.0,
                'tlt_price': 0.0,
                'tlt_change_pct': 0.0,
                'tnx_yield': 0.0,
                'tnx_change': 0.0,
                'tnx_status': '数据不可用',
                'dxy_level': 0.0,
                'dxy_change_pct': 0.0,
                'dxy_status': '数据不可用',
                'gld_price': 0.0,
                'gld_change_pct': 0.0,
                'gld_status': '数据不可用',
                'position_cap': 50,
                'reason': reason,
                'data_ok': False,
                'market_name': '美股'
            }
        elif market_type == 'hk':
            # 港股降级数据
            return {
                'verdict': 'Unknown',
                'index_level': 0.0,
                'index_change_pct': 0.0,
                'volatility': 0.0,
                'vol_status': '数据不可用',
                'ma50': 0.0,
                'ma200': 0.0,
                'reason': reason,
                'data_ok': False,
                'market_name': '港股',
                'hstech_price': 0.0, 'hstech_change_pct': 0.0, 'hstech_use_etf': False,
                'hsce_price': 0.0, 'hsce_change_pct': 0.0,
                'hkd_price': 0.0, 'hkd_change_pct': 0.0,
            }
        else:  # 'cn' - A股
            # A股降级数据
            return {
                'verdict': 'Unknown',
                'index_level': 0.0,
                'index_change_pct': 0.0,
                'volatility': 0.0,
                'vol_status': '数据不可用',
                'ma50': 0.0,
                'ma200': 0.0,
                'reason': reason,
                'data_ok': False,
                'market_name': 'A股',
                'hs300_price': 0.0, 'hs300_change_pct': 0.0,
                'cyb_price': 0.0, 'cyb_change_pct': 0.0,
                'cny_price': 0.0, 'cny_change_pct': 0.0,
            }
    
    def analyze_hk_market_regime(self, force_refresh: bool = False) -> dict:
        """
        【V89.1 新增】分析港股市场体制（基于恒生指数）
        
        返回字典：类似美股，但基于^HSI
        """
        start_time = time.time()
        
        try:
            self.logger.info("🔍 开始分析港股市场体制...")
            
            # 获取恒生指数数据（优先2y以确保足够MA200行数，失败时降级1y）
            hsi_df = None
            for _hsi_period in ['2y', '1y', '6mo']:
                hsi_df = self.dp.fetch_safe('^HSI', period=_hsi_period, data_type='weekly', force_refresh=force_refresh, min_rows=50)
                if hsi_df is not None and len(hsi_df) >= 50:
                    break

            if hsi_df is None or len(hsi_df) < 50:
                return self._fallback_result("恒生指数数据不足，无法分析", 'hk')
            
            # 计算技术指标（MA200不足时用MA50代替）
            hsi_price = float(hsi_df['Close'].iloc[-1])
            hsi_df['MA50'] = hsi_df['Close'].rolling(window=Config.MA_SHORT).mean()
            hsi_df['MA200'] = hsi_df['Close'].rolling(window=Config.MA_LONG).mean()
            ma50 = float(hsi_df['MA50'].iloc[-1])
            _ma200_raw = hsi_df['MA200'].iloc[-1]
            ma200 = float(_ma200_raw) if (_ma200_raw == _ma200_raw and _ma200_raw > 0) else ma50  # NaN时用MA50兜底
            
            # 计算波动率（替代VIX）
            returns = hsi_df['Close'].pct_change().dropna()
            volatility = returns.rolling(window=20).std().iloc[-1] * np.sqrt(252) * 100
            
            # 波动率分级（港股特色）
            if volatility > 35:
                vol_status = "⚠️ 高波动（谨慎）"
            elif volatility > 25:
                vol_status = "📈 中高波动（正常）"
            elif volatility < 15:
                vol_status = "📉 低波动（平稳）"
            else:
                vol_status = "📊 中等波动（均衡）"
            
            # 市场体制裁决
            verdict = "Neutral"
            reason_parts = []
            
            if hsi_price < ma200:
                verdict = "Risk Off"
                reason_parts.append(f"恒指({hsi_price:.0f}) < MA200({ma200:.0f})")
            elif hsi_price > ma50 and volatility < 25:
                verdict = "Risk On"
                reason_parts.append(f"恒指({hsi_price:.0f}) > MA50({ma50:.0f})")
                reason_parts.append(f"波动率={volatility:.1f}%（温和）")
            else:
                reason_parts.append(f"恒指在MA50({ma50:.0f})与MA200({ma200:.0f})之间")
            
            reason = "；".join(reason_parts)
            
            # 日涨跌（用于宏观脉搏展示）
            hsi_prev = float(hsi_df['Close'].iloc[-2]) if len(hsi_df) >= 2 else hsi_price
            hsi_change_pct = ((hsi_price - hsi_prev) / hsi_prev * 100) if hsi_prev != 0 else 0
            
            # 【V91.1】恒生科技/国企指数/港币：6mo日线 + min_rows=2，^HSTECH 失败时用 3033.HK ETF 兜底
            hstech_price, hstech_chg, hstech_use_etf = 0.0, 0.0, False
            hsce_price, hsce_chg = 0.0, 0.0
            hkd_price, hkd_chg = 0.0, 0.0
            for _sym in ['^HSTECH', '3033.HK']:
                hstech_df = self.dp.fetch_safe(_sym, period='6mo', data_type='daily', force_refresh=force_refresh, min_rows=2)
                if hstech_df is not None and len(hstech_df) >= 2:
                    hstech_price = float(hstech_df['Close'].iloc[-1])
                    hstech_prev = float(hstech_df['Close'].iloc[-2])
                    hstech_chg = ((hstech_price - hstech_prev) / hstech_prev * 100) if hstech_prev != 0 else 0
                    hstech_use_etf = (_sym == '3033.HK')
                    break
            try:
                hsce_df = self.dp.fetch_safe('^HSCE', period='6mo', data_type='daily', force_refresh=force_refresh, min_rows=2)
                if hsce_df is not None and len(hsce_df) >= 2:
                    hsce_price = float(hsce_df['Close'].iloc[-1])
                    hsce_prev = float(hsce_df['Close'].iloc[-2])
                    hsce_chg = ((hsce_price - hsce_prev) / hsce_prev * 100) if hsce_prev != 0 else 0
            except Exception:
                pass
            try:
                hkd_df = self.dp.fetch_safe('HKD=X', period='6mo', data_type='fast', force_refresh=force_refresh, min_rows=2)
                if hkd_df is not None and len(hkd_df) >= 2:
                    hkd_price = float(hkd_df['Close'].iloc[-1])
                    hkd_prev = float(hkd_df['Close'].iloc[-2])
                    hkd_chg = ((hkd_price - hkd_prev) / hkd_prev * 100) if hkd_prev != 0 else 0
            except Exception:
                pass
            
            result = {
                'verdict': verdict,
                'index_level': hsi_price,
                'index_change_pct': hsi_change_pct,
                'volatility': volatility,
                'vol_status': vol_status,
                'ma50': ma50,
                'ma200': ma200,
                'reason': reason,
                'data_ok': True,
                'market_name': '港股',
                'hstech_price': hstech_price,
                'hstech_change_pct': hstech_chg,
                'hstech_use_etf': hstech_use_etf,
                'hsce_price': hsce_price,
                'hsce_change_pct': hsce_chg,
                'hkd_price': hkd_price,
                'hkd_change_pct': hkd_chg,
            }
            
            elapsed = (time.time() - start_time) * 1000
            self.perf.record('compute', elapsed)
            
            self.logger.info(f"✅ 港股市场体制分析完成: {verdict} | 恒指={hsi_price:.0f} | MA50={ma50:.0f} | MA200={ma200:.0f} | 波动率={volatility:.1f}%")
            return result
        
        except Exception as e:
            self.logger.error(f"❌ 港股市场分析异常: {str(e)}")
            return self._fallback_result(f"港股分析异常: {str(e)[:50]}", 'hk')
    
    def analyze_cn_market_regime(self, force_refresh: bool = False) -> dict:
        """
        【V89.1 新增】分析A股市场体制（基于上证指数）
        
        返回字典：类似美股，但基于000001.SS
        """
        start_time = time.time()
        
        try:
            self.logger.info("🔍 开始分析A股市场体制...")
            
            # 获取上证指数数据（优先2y确保MA200数据充足，失败时降级）
            sse_df = None
            for _sse_period in ['2y', '1y', '6mo']:
                sse_df = self.dp.fetch_safe('000001.SS', period=_sse_period, data_type='weekly', force_refresh=force_refresh, min_rows=50)
                if sse_df is not None and len(sse_df) >= 50:
                    break

            if sse_df is None or len(sse_df) < 50:
                return self._fallback_result("上证指数数据不足，无法分析", 'cn')
            
            # 计算技术指标（MA200不足时用MA50代替）
            sse_price = float(sse_df['Close'].iloc[-1])
            sse_df['MA50'] = sse_df['Close'].rolling(window=Config.MA_SHORT).mean()
            sse_df['MA200'] = sse_df['Close'].rolling(window=Config.MA_LONG).mean()
            ma50 = float(sse_df['MA50'].iloc[-1])
            _ma200_raw = sse_df['MA200'].iloc[-1]
            ma200 = float(_ma200_raw) if (_ma200_raw == _ma200_raw and _ma200_raw > 0) else ma50
            
            # 计算波动率
            returns = sse_df['Close'].pct_change().dropna()
            volatility = returns.rolling(window=20).std().iloc[-1] * np.sqrt(252) * 100
            
            # 波动率分级（A股特色）
            if volatility > 40:
                vol_status = "⚠️ 高波动（政策敏感期）"
            elif volatility > 30:
                vol_status = "📈 中高波动（活跃）"
            elif volatility < 20:
                vol_status = "📉 低波动（盘整）"
            else:
                vol_status = "📊 中等波动（正常）"
            
            # 市场体制裁决
            verdict = "Neutral"
            reason_parts = []
            
            if sse_price < ma200:
                verdict = "Risk Off"
                reason_parts.append(f"上证({sse_price:.0f}) < MA200({ma200:.0f})")
            elif sse_price > ma50 and volatility < 30:
                verdict = "Risk On"
                reason_parts.append(f"上证({sse_price:.0f}) > MA50({ma50:.0f})")
                reason_parts.append(f"波动率={volatility:.1f}%（温和）")
            else:
                reason_parts.append(f"上证在MA50({ma50:.0f})与MA200({ma200:.0f})之间")
            
            reason = "；".join(reason_parts)
            
            # 日涨跌（用于宏观脉搏展示）
            sse_prev = float(sse_df['Close'].iloc[-2]) if len(sse_df) >= 2 else sse_price
            sse_change_pct = ((sse_price - sse_prev) / sse_prev * 100) if sse_prev != 0 else 0
            
            # 【V91.1】补充指标：沪深300、创业板指、人民币汇率（6mo+min_rows=2 确保能取到）
            hs300_price, hs300_chg = 0.0, 0.0
            cyb_price, cyb_chg = 0.0, 0.0
            cny_price, cny_chg = 0.0, 0.0
            try:
                hs300_df = self.dp.fetch_safe('000300.SS', period='6mo', data_type='daily', force_refresh=force_refresh, min_rows=2)
                if hs300_df is not None and len(hs300_df) >= 2:
                    hs300_price = float(hs300_df['Close'].iloc[-1])
                    hs300_prev = float(hs300_df['Close'].iloc[-2])
                    hs300_chg = ((hs300_price - hs300_prev) / hs300_prev * 100) if hs300_prev != 0 else 0
            except Exception:
                pass
            try:
                cyb_df = self.dp.fetch_safe('399006.SZ', period='6mo', data_type='daily', force_refresh=force_refresh, min_rows=2)
                # 【V91.7】Yahoo 对 399006 不稳定，优先东方财富专用接口（fqt=0），再主 fetch
                if cyb_df is None or len(cyb_df) < 2:
                    try:
                        _cyb_em = fetch_cyb_from_eastmoney()
                        if _cyb_em is not None and len(_cyb_em) >= 2:
                            cyb_df = _cyb_em
                    except Exception:
                        pass
                if cyb_df is None or len(cyb_df) < 2:
                    try:
                        _cyb_from_fetch = fetch_stock_data('399006.SZ')
                        if _cyb_from_fetch is not None and len(_cyb_from_fetch) >= 2:
                            cyb_df = _cyb_from_fetch
                    except Exception:
                        pass
                if cyb_df is not None and len(cyb_df) >= 2:
                    cyb_price = float(cyb_df['Close'].iloc[-1])
                    cyb_prev = float(cyb_df['Close'].iloc[-2])
                    cyb_chg = ((cyb_price - cyb_prev) / cyb_prev * 100) if cyb_prev != 0 else 0
            except Exception:
                pass
            try:
                cny_df = self.dp.fetch_safe('CNY=X', period='6mo', data_type='fast', force_refresh=force_refresh, min_rows=2)
                if cny_df is not None and len(cny_df) >= 2:
                    cny_price = float(cny_df['Close'].iloc[-1])
                    cny_prev = float(cny_df['Close'].iloc[-2])
                    cny_chg = ((cny_price - cny_prev) / cny_prev * 100) if cny_prev != 0 else 0
            except Exception:
                pass
            
            result = {
                'verdict': verdict,
                'index_level': sse_price,
                'index_change_pct': sse_change_pct,
                'volatility': volatility,
                'vol_status': vol_status,
                'ma50': ma50,
                'ma200': ma200,
                'reason': reason,
                'data_ok': True,
                'market_name': 'A股',
                'hs300_price': hs300_price,
                'hs300_change_pct': hs300_chg,
                'cyb_price': cyb_price,
                'cyb_change_pct': cyb_chg,
                'cny_price': cny_price,
                'cny_change_pct': cny_chg,
            }
            
            elapsed = (time.time() - start_time) * 1000
            self.perf.record('compute', elapsed)
            
            self.logger.info(f"✅ A股市场体制分析完成: {verdict} | 上证={sse_price:.0f} | MA50={ma50:.0f} | MA200={ma200:.0f} | 波动率={volatility:.1f}%")
            return result
        
        except Exception as e:
            self.logger.error(f"❌ A股市场分析异常: {str(e)}")
            return self._fallback_result(f"A股分析异常: {str(e)[:50]}", 'cn')
    
    def analyze_all_markets(self, force_refresh: bool = False) -> dict:
        """
        【V89.1 新增】分析所有市场（美股/港股/A股）+ 综合联动
        
        返回字典：
        {
            'us_market': {...},
            'hk_market': {...},
            'cn_market': {...},
            'summary': {...}  # 综合分析
        }
        """
        try:
            # 并发获取三大市场分析
            us_result = self.analyze_market_regime(force_refresh)
            hk_result = self.analyze_hk_market_regime(force_refresh)
            cn_result = self.analyze_cn_market_regime(force_refresh)
            
            # 综合分析
            risk_on_count = sum(1 for r in [us_result, hk_result, cn_result] 
                               if r['data_ok'] and r['verdict'] == 'Risk On')
            risk_off_count = sum(1 for r in [us_result, hk_result, cn_result] 
                                if r['data_ok'] and r['verdict'] == 'Risk Off')
            
            valid_markets = sum(1 for r in [us_result, hk_result, cn_result] if r['data_ok'])
            
            if valid_markets == 0:
                global_verdict = "数据不足"
                global_reason = "所有市场数据均不可用"
            elif risk_on_count >= 2:
                global_verdict = "🟢 全球风险偏好"
                global_reason = f"三大市场中{risk_on_count}个处于Risk On状态"
            elif risk_off_count >= 2:
                global_verdict = "🔴 全球避险模式"
                global_reason = f"三大市场中{risk_off_count}个处于Risk Off状态"
            else:
                global_verdict = "🟡 市场分化"
                global_reason = "各市场体制不一致，结构性行情为主"
            
            summary = {
                'global_verdict': global_verdict,
                'global_reason': global_reason,
                'risk_on_count': risk_on_count,
                'risk_off_count': risk_off_count,
                'valid_markets': valid_markets
            }
            
            result = {
                'us_market': us_result,
                'hk_market': hk_result,
                'cn_market': cn_result,
                'summary': summary
            }
            
            self._last_multi_result = result
            return result
        
        except Exception as e:
            self.logger.error(f"❌ 全市场分析异常: {str(e)}")
            return {
                'us_market': self._fallback_result("美股分析失败", 'us'),
                'hk_market': self._fallback_result("港股分析失败", 'hk'),
                'cn_market': self._fallback_result("A股分析失败", 'cn'),
                'summary': {
                    'global_verdict': "数据不足",
                    'global_reason': "分析异常",
                    'risk_on_count': 0,
                    'risk_off_count': 0,
                    'valid_markets': 0
                }
            }


# 初始化全局实例
_data_provider = DataProvider(_cache_manager, _perf_monitor)
_expectation_layer = ExpectationLayer(_data_provider, _perf_monitor)

# 【V89.2】初始化机构研究中心
if INSTITUTIONAL_RESEARCH_AVAILABLE:
    _institutional_research = InstitutionalResearch(_data_provider, _perf_monitor)
    logging.info("✅ V89.2 机构研究中心初始化完成")
else:
    _institutional_research = None

# 【V89.3】初始化持仓管理器
if PORTFOLIO_MANAGER_AVAILABLE and Config.PORTFOLIO_ENABLED:
    _portfolio_manager = PortfolioManager(Config.PORTFOLIO_FILE)
    logging.info(f"✅ V89.3 持仓管理器初始化完成: {Config.PORTFOLIO_FILE}")
else:
    _portfolio_manager = None

# 【V89.4】初始化舆情分析中心
if SENTIMENT_ANALYZER_AVAILABLE:
    # call_gemini_api函数在后面定义，这里先设为None，后续再绑定
    _sentiment_analyzer = SentimentAnalyzer(gemini_api_caller=None)
    logging.info("✅ V89.4 舆情分析中心初始化完成")
else:
    _sentiment_analyzer = None

logging.info("✅ V89 Phase 1 架构层初始化完成")
logging.info("  - Config: 全局配置中心")
logging.info("  - DataProvider: 安全数据层（容错+缓存）")
logging.info("  - ExpectationLayer: 宏观预期层（Risk On/Off/Neutral）")
logging.info("✅ V89 Phase 2 性能优化层初始化完成")
logging.info("  - PerformanceMonitor: 性能监控器")
logging.info("  - LayeredCacheManager: 分层缓存管理器（Fast/Daily/Weekly）")
logging.info("  - 并发线程池: 最大{}线程".format(Config.MAX_WORKERS))

# ═══════════════════════════════════════════════════════════════

st.set_page_config(layout="wide", page_title="AI 皇冠双核", page_icon="👑", initial_sidebar_state="collapsed")

# ═══════════════════════════════════════════════════════════════
# 【V89.5 修复】提前定义MY_GEMINI_KEY - 避免在全球市场概览中未定义错误
# ═══════════════════════════════════════════════════════════════
try:
    if USE_NEW_MODULES:
        MY_GEMINI_KEY = mod_config.GEMINI_API_KEY
        GEMINI_MODEL_NAME = mod_config.GEMINI_MODEL_NAME
    else:
        MY_GEMINI_KEY = st.secrets.get("GEMINI_API_KEY", os.getenv("GEMINI_API_KEY", ""))
        GEMINI_MODEL_NAME = "gemini-2.5-flash"
    
    # 配置Gemini API
    if HAS_GEMINI and MY_GEMINI_KEY:
        genai.configure(api_key=MY_GEMINI_KEY)
        logging.info(f"✅ Gemini API配置完成: {GEMINI_MODEL_NAME}")
except Exception as e:
    MY_GEMINI_KEY = ""
    GEMINI_MODEL_NAME = "gemini-2.5-flash"
    logging.error(f"⚠️ Gemini API配置失败: {e}")

# 【V91.9】AI分析统一模型说明：所有spinner和报告统一使用
def _ai_model_label(model=None):
    """返回模型显示名称，用于 spinner 和报告底部"""
    m = model or GEMINI_MODEL_NAME
    if USE_NEW_MODULES and hasattr(mod_config, 'GEMINI_MODELS') and m in mod_config.GEMINI_MODELS:
        return mod_config.GEMINI_MODELS[m]
    return m.replace('-', ' ').replace('gemini', 'Gemini').title()


def _load_prompt(name: str, **kwargs) -> str:
    """从 prompts/ 目录加载 prompt 模板，支持 .format() 变量替换"""
    _p = Path(__file__).parent / "prompts" / name
    try:
        tpl = _p.read_text(encoding="utf-8")
        return tpl.format(**kwargs) if kwargs else tpl
    except FileNotFoundError:
        logging.warning(f"Prompt 文件未找到: {_p}")
        return ""
    except KeyError as e:
        logging.warning(f"Prompt 变量替换失败 {name}: {e}")
        return _p.read_text(encoding="utf-8")


# ═══════════════════════════════════════════════════════════════
# 【V92】全量云端搜索 - 从侧边栏移至深度作战室主区域
# ═══════════════════════════════════════════════════════════════
def render_cloud_search():
    """东方财富API全量搜索 - 渲染在主内容区（深度作战室顶部）"""
    st.markdown("""
    <div style="padding: 0.4rem 0 0.2rem 0; margin-bottom: 0.5rem; border-left: 3px solid #00d4aa; padding-left: 0.8rem;">
        <span style="font-size: 13px; font-weight: 700; color: #00d4aa;">🔍 个股搜索</span>
        <span style="font-size: 11px; color: #888; margin-left: 0.6rem;">美股 / 港股 / A股</span>
    </div>
    """, unsafe_allow_html=True)
    col_search, col_filter = st.columns([3, 1])
    with col_search:
        search_input = st.text_input(
            "输入股票名字或代码（全量云端搜索）",
            placeholder="例如：宁波 / 紫金 / 腾讯 / AAPL / NVDA",
            key="stock_search_input",
            label_visibility="collapsed"
        )
    with col_filter:
        search_market_filter = st.selectbox(
            "市场筛选",
            ["全部", "🇺🇸 美股", "🇭🇰 港股", "🇨🇳 A股"],
            key="search_market_filter",
            help="筛选搜索结果为指定市场"
        )
    
    if search_input:
        search_key = search_input.strip()
        
        if search_key:
            _search_prog = st.progress(0)
            _search_status = st.empty()
            _search_status.text("🔍 请求东方财富API... (0%)")
            all_matches = []
            try:
                search_url = f"https://searchapi.eastmoney.com/api/suggest/get"
                params = {
                    "input": search_key,
                    "type": "14",
                    "token": "D43BF722C8E33BDC906FB84D85E326E8",
                    "count": 50
                }
                response = requests.get(search_url, params=params, timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    if data and 'QuotationCodeTable' in data and 'Data' in data['QuotationCodeTable']:
                        results = data['QuotationCodeTable']['Data']
                        for item in results:
                            code_raw = item.get('Code', '')
                            name = item.get('Name', '')
                            market_code = item.get('MktNum', '')
                            yf_code = None
                            if market_code == '1':
                                yf_code = f"{code_raw}.SS"
                            elif market_code == '0':
                                yf_code = f"{code_raw}.SZ"
                            elif market_code == '116':
                                yf_code = f"{code_raw.zfill(5)}.HK"
                            elif market_code == '155':
                                yf_code = code_raw
                            else:
                                yf_code = code_raw
                            if yf_code and name:
                                all_matches.append((yf_code, name))
                        _safe_print(f"[东方财富API] 搜索 '{search_key}' 找到 {len(all_matches)} 个结果")
                        _search_prog.progress(0.5)
                        _search_status.text(f"✅ API返回 {len(all_matches)} 个结果 (50%)")

                if len(all_matches) == 0:
                    _search_prog.progress(0.3)
                    _search_status.text("🔍 API失败，降级到本地索引...")
                    _safe_print("[东方财富API] 失败，降级到本地索引")
                    _idx_total = len(STOCK_NAME_INDEX)
                    for _idx, (code, name) in enumerate(STOCK_NAME_INDEX.items()):
                        if _idx_total > 0 and _idx % 50 == 0:
                            _search_prog.progress(0.3 + 0.6 * (_idx / _idx_total))
                            _search_status.text(f"🔍 本地索引搜索... {_idx}/{_idx_total} ({100*_idx/_idx_total:.0f}%)")
                        if (search_key.upper() in code.upper() or search_key in name or search_key.upper() in code.split('.')[0].upper()):
                            all_matches.append((code, name))
                
            except Exception as e:
                _safe_print(f"[东方财富API] 错误: {e}")
                _search_prog.progress(0.2)
                _search_status.text("🔍 API异常，降级到本地索引...")
                _idx_total = len(STOCK_NAME_INDEX)
                for _idx, (code, name) in enumerate(STOCK_NAME_INDEX.items()):
                    if _idx_total > 0 and _idx % 50 == 0:
                        _search_prog.progress(0.2 + 0.7 * (_idx / _idx_total))
                        _search_status.text(f"🔍 本地索引... {_idx}/{_idx_total} ({100*_idx/_idx_total:.0f}%)")
                    if (search_key.upper() in code.upper() or search_key in name or search_key.upper() in code.split('.')[0].upper()):
                        all_matches.append((code, name))

            _search_prog.progress(1.0)
            _search_status.text(f"✅ 搜索完成，共 {len(all_matches)} 个结果 (100%)")
            time.sleep(0.3)
            _search_prog.empty()
            _search_status.empty()

            if len(all_matches) > 0:
                us_stocks = [(c, n) for c, n in all_matches if "." not in c]
                hk_stocks = [(c, n) for c, n in all_matches if ".HK" in c]
                cn_stocks = [(c, n) for c, n in all_matches if ".SS" in c or ".SZ" in c]
                # 【V93】按市场筛选
                if search_market_filter == "🇺🇸 美股":
                    us_stocks, hk_stocks, cn_stocks = us_stocks, [], []
                elif search_market_filter == "🇭🇰 港股":
                    us_stocks, hk_stocks, cn_stocks = [], hk_stocks, []
                elif search_market_filter == "🇨🇳 A股":
                    us_stocks, hk_stocks, cn_stocks = [], [], cn_stocks
                filtered_count = len(us_stocks) + len(hk_stocks) + len(cn_stocks)
                if filtered_count == 0:
                    st.warning(f"该市场下无匹配结果，请尝试「全部」或切换其他市场")
                else:
                    st.success(f"✅ 找到 {filtered_count} 个结果" + (f"（已筛选 {search_market_filter}）" if search_market_filter != "全部" else ""))
                
                options = ["请选择要分析的股票..."]
                code_map = {}
                
                if us_stocks:
                    options.append("─────── 🇺🇸 美股 ───────")
                    for code, name in us_stocks:
                        option_text = f"🇺🇸 {name} ({code})"
                        options.append(option_text)
                        code_map[option_text] = (code, name)
                
                if hk_stocks:
                    options.append("─────── 🇭🇰 港股 ───────")
                    for code, name in hk_stocks:
                        option_text = f"🇭🇰 {name} ({code})"
                        options.append(option_text)
                        code_map[option_text] = (code, name)
                
                if cn_stocks:
                    options.append("─────── 🇨🇳 A股 ───────")
                    for code, name in cn_stocks:
                        option_text = f"🇨🇳 {name} ({code})"
                        options.append(option_text)
                        code_map[option_text] = (code, name)
                
                selected_option = st.selectbox(
                    "② 从结果中选择股票",
                    options=options,
                    key="stock_select_dropdown"
                )
                
                if filtered_count > 0 and selected_option != "请选择要分析的股票..." and selected_option not in ["─────── 🇺🇸 美股 ───────", "─────── 🇭🇰 港股 ───────", "─────── 🇨🇳 A股 ───────"]:
                    if selected_option in code_map:
                        code, name = code_map[selected_option]
                        
                        if (code, name) not in st.session_state.search_history:
                            st.session_state.search_history.insert(0, (code, name))
                            if len(st.session_state.search_history) > 10:
                                st.session_state.search_history = st.session_state.search_history[:10]
                        
                        _prev_code = st.session_state.get('scan_selected_code')
                        if _prev_code != code:
                            st.session_state.scan_selected_code = code
                            st.session_state.scan_selected_name = name
                            st.session_state.pk_codes = []
                            st.session_state.pk_names = []
                            st.toast(f"✅ 已选中 {name}，正在分析...", icon="🎯")
                            st.rerun()
                        
                        is_in_basket = (code, name) in st.session_state.compare_basket
                        if is_in_basket:
                            st.button("✅ 已在对比篮", key="search_compare", disabled=True, use_container_width=True)
                        else:
                            if st.button("➕ 加入对比篮", key="search_compare", use_container_width=True):
                                st.session_state.compare_basket.append((code, name))
                                st.toast(f"✅ 已加入对比篮: {name}", icon="➕")
                                st.rerun()
            else:
                st.warning("❌ 未找到匹配的股票")
                st.caption("💡 搜索提示：")
                st.caption("• 关键字：宁波、紫金、腾讯")
                st.caption("• 代码：AAPL、02899、600519")
    
    if len(st.session_state.search_history) > 0:
        st.markdown('<p style="font-size: 12px; font-weight: 600; margin-top: 1rem; margin-bottom: 0.3rem;">📜 搜索历史</p>', unsafe_allow_html=True)
        st.caption(f"最近搜索 {len(st.session_state.search_history)} 只")
        
        for i, (code, name) in enumerate(st.session_state.search_history):
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                st.markdown(f"**{name}**")
                st.caption(code)
            
            with col2:
                if st.button("🔍", key=f"hist_analyze_{i}", help="分析", use_container_width=True):
                    st.session_state.scan_selected_code = code
                    st.session_state.scan_selected_name = name
                    st.session_state.pk_codes = []
                    st.session_state.pk_names = []
                    st.rerun()
            
            with col3:
                is_in_basket = (code, name) in st.session_state.compare_basket
                if is_in_basket:
                    st.button("✅", key=f"hist_compare_{i}", disabled=True, use_container_width=True)
                else:
                    if st.button("➕", key=f"hist_compare_{i}", help="加入对比", use_container_width=True):
                        st.session_state.compare_basket.append((code, name))
                        st.toast(f"✅ 已加入对比篮: {name}", icon="➕")
                        st.rerun()
        
        if st.button("🗑️ 清空历史", key="search_clear_history", use_container_width=True):
            st.session_state.search_history = []
            st.rerun()


# ═══════════════════════════════════════════════════════════════
# 【V90.8 关键修复】完整可点击表格 - 含快捷入口、深度分析跳转
# 必须使用此实现，mod_ui 版本无深度作战室逻辑，导致点击无反应
# ═══════════════════════════════════════════════════════════════
def render_clickable_table(df_results, table_key):
    """【V87.7】复选框智能识别 + 加入对比篮 + 快捷入口深度分析"""
    if df_results is None or len(df_results) == 0:
        st.info("暂无数据")
        return
    
    if isinstance(df_results, list):
        df_results = pd.DataFrame(df_results)
    
    if "代码" not in df_results.columns:
        st.dataframe(df_results, use_container_width=True, hide_index=True, key=f"table_plain_{table_key}")
        return
    
    df_display = df_results.copy()
    
    st.markdown("##### 📊 扫描结果")
    st.caption("💡 快捷入口选股点击「深度分析」| 勾选1只=深度分析 | 勾选2只以上=立即对比")
    
    # 【V90.6】快捷入口：选择框+按钮
    stock_options = []
    for _, row in df_display.iterrows():
        code = row.get('代码')
        name = row.get('股票') or row.get('名称') or str(code)
        if code and str(code).strip():
            stock_options.append((str(code).strip(), str(name).strip()))
    if stock_options:
        quick_col1, quick_col2 = st.columns([3, 1])
        with quick_col1:
            quick_choice = st.selectbox("🔍 快捷入口：选择股票查看深度分析", 
                options=["-- 请选择 --"] + [f"{name} ({code})" for code, name in stock_options],
                key=f"quick_select_{table_key}")
        with quick_col2:
            if st.button("⚔️ 深度分析", key=f"quick_btn_{table_key}", type="primary", use_container_width=True):
                if quick_choice and quick_choice != "-- 请选择 --":
                    import re
                    m = re.search(r'\(([^)]+)\)', quick_choice)
                    if m:
                        c, n = m.group(1), quick_choice.split('(')[0].strip()
                        st.session_state.scan_selected_code = c
                        st.session_state.scan_selected_name = n
                        st.session_state.pk_codes = []
                        st.session_state.pk_names = []
                        st.toast(f"✅ 已选中 {n}，跳转深度作战室", icon="🎯")
                        st.rerun()
    
    selection = st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="multi-row",
        key=f"table_{table_key}"
    )
    
    selected_stocks = []
    try:
        if selection is not None:
            if hasattr(selection, 'rows') and selection.rows:
                selected_indices = selection.rows
            elif hasattr(selection, 'selection') and hasattr(selection.selection, 'rows') and selection.selection.rows:
                selected_indices = selection.selection.rows
            else:
                selected_indices = []
            
            for idx in selected_indices:
                try:
                    row = df_display.iloc[idx]
                    code = str(row['代码']).strip()
                    if '股票' in row and row['股票'] and str(row['股票']).strip():
                        name = str(row['股票']).strip()
                    elif '名称' in row and row['名称'] and str(row['名称']).strip():
                        name = str(row['名称']).strip()
                    else:
                        name = code
                    selected_stocks.append((code, name))
                except Exception:
                    pass
    except Exception:
        pass
    
    if len(selected_stocks) == 1:
        code, name = selected_stocks[0]
        st.session_state.scan_selected_code = code
        st.session_state.scan_selected_name = name
        st.session_state.pk_codes = []
        st.session_state.pk_names = []
        st.toast(f"✅ 已选中 {name}，正在跳转深度分析...", icon="🎯")
        st.rerun()
    
    if len(selected_stocks) >= 2:
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button(f"⚔️ 立即对比 {len(selected_stocks)}只", key=f"compare_{table_key}", type="primary", use_container_width=True):
                codes = [s[0] for s in selected_stocks]
                names = [s[1] for s in selected_stocks]
                st.session_state.pk_codes = codes
                st.session_state.pk_names = names
                st.session_state.scan_selected_code = None
                st.session_state.scan_selected_name = None
                st.toast(f"⚔️ 开始对比 {len(selected_stocks)} 只股票", icon="⚔️")
                st.rerun()
        with col2:
            if st.button(f"➕ 加入对比篮 ({len(selected_stocks)}只)", key=f"add_basket_{table_key}", use_container_width=True):
                added_count = 0
                for code, name in selected_stocks:
                    if (code, name) not in st.session_state.compare_basket:
                        st.session_state.compare_basket.append((code, name))
                        added_count += 1
                if added_count > 0:
                    st.toast(f"✅ 已加入 {added_count} 只股票到对比篮", icon="➕")
                    st.rerun()
                else:
                    st.toast("ℹ️ 这些股票已在对比篮中", icon="ℹ️")
        with col3:
            if st.button("🗑️ 清除选择", key=f"clear_{table_key}", use_container_width=True):
                st.rerun()

# ═══════════════════════════════════════════════════════════════
# 模块别名：只保留在 inline def 之前必须确定的 3 个名称
# ProxyContext          — 仅在 modules/data_fetch 中定义
# to_yf_cn_code         — 必须在 get_market_heat() 前映射（~line 2291）
# batch_scan_analysis_concurrent — 仅在 modules/analysis_core 中定义
# ═══════════════════════════════════════════════════════════════
if USE_NEW_MODULES:
    ProxyContext = mod_data.ProxyContext
    to_yf_cn_code = mod_utils.to_yf_cn_code
    batch_scan_analysis_concurrent = mod_analysis.batch_scan_analysis_concurrent
    logging.info("✅ 模块别名映射完成（3项）")

# 无论哪种模式，确保舆情分析器绑定了AI调用函数
try:
    if SENTIMENT_ANALYZER_AVAILABLE and _sentiment_analyzer:
        _sentiment_analyzer.call_ai = call_gemini_api
        logging.info("✅ 舆情分析器已绑定 call_gemini_api")
except NameError:
    logging.warning("⚠️ call_gemini_api 尚未定义，稍后绑定")

# ═══════════════════════════════════════════════════════════════
# Fragment 函数：AI综合分析（局部刷新，按钮交互不触发全页重跑）
# ═══════════════════════════════════════════════════════════════
@st.fragment
def _render_ai_market_analysis():
    from datetime import datetime as _dt_ai
    _all = st.session_state.get('all_markets', {})
    us_result = _all.get('us_market', {'data_ok': False, 'verdict': 'Unknown', 'reason': ''})
    hk_result = _all.get('hk_market', {'data_ok': False, 'verdict': 'Unknown', 'reason': ''})
    cn_result = _all.get('cn_market', {'data_ok': False, 'verdict': 'Unknown', 'reason': ''})

    _has_any_ai = (HAS_PREDICTION_ENGINE or (SENTIMENT_ANALYZER_AVAILABLE and _sentiment_analyzer)) and MY_GEMINI_KEY
    if not _has_any_ai:
        return

    st.markdown(f"### 🤖 AI综合分析 · {_dt_ai.now().strftime('%Y-%m-%d')}")
    st.caption("市场指数走势预测 + 舆情情绪分析 · 点击「一键分析」生成报告")

    if st.button("⚡ 一键分析全市场（美股＋港股＋A股）", key="btn_one_click_all_markets",
                 type="primary", use_container_width=True):
        st.session_state['_one_click_all_markets'] = True
    _trigger_all = st.session_state.get('_one_click_all_markets', False)

    ai_tabs = st.tabs(["🇺🇸 美股", "🇭🇰 港股", "🇨🇳 A股"])

    with ai_tabs[0]:
        _us_pred_key = 'market_ai_us'
        sentiment_us_key = "market_sentiment_us"
        if _trigger_all and 'market_ai_us' not in st.session_state:
            _prog = st.progress(0)
            _stat = st.empty()
            try:
                if HAS_PREDICTION_ENGINE:
                    _stat.info("📊 获取标普500数据...")
                    _prog.progress(0.15)
                    _us_tech_df = fetch_stock_data("^GSPC")
                    _prog.progress(0.3)
                    _stat.info("🔍 技术分析中...")
                    _forecaster = MarketForecaster()
                    _us_tech = _forecaster.analyze_market_technicals(_us_tech_df, '美股') if _us_tech_df is not None and len(_us_tech_df) >= 20 else {}
                    _prog.progress(0.45)
                    _stat.info("🤖 AI预测走势...")
                    _ai_r = _forecaster.call_gemini_market_forecast([_us_tech], MY_GEMINI_KEY)
                    st.session_state[_us_pred_key] = _ai_r.get('美股', 'AI分析结果为空')
                    st.session_state['_us_tech_data'] = _us_tech
                if SENTIMENT_ANALYZER_AVAILABLE and _sentiment_analyzer:
                    _prog.progress(0.6)
                    _stat.info("🤖 分析市场舆情...")
                    _sent_prompt = _sentiment_analyzer.generate_market_sentiment_prompt('美股', us_result)
                    _prog.progress(0.75)
                    _sent_resp = call_gemini_api(_sent_prompt)
                    _sent_metrics = _sentiment_analyzer.parse_sentiment_score(_sent_resp)
                    st.session_state[sentiment_us_key] = {'response': _sent_resp, 'metrics': _sent_metrics}
                _prog.progress(1.0)
                _prog.empty()
                _stat.empty()
                st.success("✅ 美股AI分析完成")
            except Exception as e:
                _prog.empty()
                _stat.empty()
                st.error(f"❌ 分析失败: {str(e)[:80]}")

        if _us_pred_key in st.session_state:
            _us_tech_d = st.session_state.get('_us_tech_data', {})
            if _us_tech_d:
                _tc1, _tc2, _tc3 = st.columns(3)
                with _tc1:
                    st.metric("当前价格", f"{_us_tech_d.get('current_price', 0):.2f}")
                with _tc2:
                    st.metric("技术趋势", _us_tech_d.get('trend', '震荡'))
                with _tc3:
                    st.metric("技术强度", f"{_us_tech_d.get('strength', 50)}/100")
            if COPY_UTILS_AVAILABLE:
                CopyUtils.create_copy_button(st.session_state[_us_pred_key], button_text="📋 复制全文", key="copy_us_pred_full")
                CopyUtils.render_markdown_with_section_copy(st.session_state[_us_pred_key], key_prefix="us_pred")
            else:
                st.markdown(st.session_state[_us_pred_key])
            st.caption(f"📌 AI生成 · 模型: {_ai_model_label()}")
        elif not _trigger_all:
            st.caption("点击「一键分析全市场」生成报告")

        if sentiment_us_key in st.session_state:
            _us_sent_d = st.session_state[sentiment_us_key]
            _us_sent_m = _us_sent_d['metrics']
            with st.expander(f"📰 舆情 | 评分 {_us_sent_m.get('sentiment_score', 50)}/100 · {_us_sent_m.get('sentiment_level', '中性')}", expanded=False):
                if COPY_UTILS_AVAILABLE:
                    CopyUtils.create_copy_button(_us_sent_d['response'], button_text="📋 复制全文", key="copy_us_sent_full")
                    CopyUtils.render_markdown_with_section_copy(_us_sent_d['response'], key_prefix="us_sent")
                else:
                    st.markdown(_us_sent_d['response'])
                st.caption(f"📌 AI生成 · 模型: {_ai_model_label()}")

    with ai_tabs[1]:
        _hk_pred_key = 'market_ai_hk'
        sentiment_hk_key = "market_sentiment_hk"
        if _trigger_all and 'market_ai_hk' not in st.session_state:
            _prog = st.progress(0)
            _stat = st.empty()
            try:
                if HAS_PREDICTION_ENGINE:
                    _stat.info("📊 获取恒指数据...")
                    _prog.progress(0.15)
                    _hk_tech_df = fetch_stock_data("^HSI")
                    _prog.progress(0.3)
                    _stat.info("🔍 技术分析中...")
                    _forecaster = MarketForecaster()
                    _hk_tech = _forecaster.analyze_market_technicals(_hk_tech_df, '港股') if _hk_tech_df is not None and len(_hk_tech_df) >= 20 else {}
                    _prog.progress(0.45)
                    _stat.info("🤖 AI预测走势...")
                    _ai_r = _forecaster.call_gemini_market_forecast([_hk_tech], MY_GEMINI_KEY)
                    st.session_state[_hk_pred_key] = _ai_r.get('港股', 'AI分析结果为空')
                    st.session_state['_hk_tech_data'] = _hk_tech
                if SENTIMENT_ANALYZER_AVAILABLE and _sentiment_analyzer:
                    _prog.progress(0.6)
                    _stat.info("🤖 分析市场舆情...")
                    _sent_prompt = _sentiment_analyzer.generate_market_sentiment_prompt('港股', hk_result)
                    _prog.progress(0.75)
                    _sent_resp = call_gemini_api(_sent_prompt)
                    _sent_metrics = _sentiment_analyzer.parse_sentiment_score(_sent_resp)
                    st.session_state[sentiment_hk_key] = {'response': _sent_resp, 'metrics': _sent_metrics}
                _prog.progress(1.0)
                _prog.empty()
                _stat.empty()
                st.success("✅ 港股AI分析完成")
            except Exception as e:
                _prog.empty()
                _stat.empty()
                st.error(f"❌ 分析失败: {str(e)[:80]}")

        if _hk_pred_key in st.session_state:
            _hk_tech_d = st.session_state.get('_hk_tech_data', {})
            if _hk_tech_d:
                _tc1, _tc2, _tc3 = st.columns(3)
                with _tc1:
                    st.metric("当前价格", f"{_hk_tech_d.get('current_price', 0):.2f}")
                with _tc2:
                    st.metric("技术趋势", _hk_tech_d.get('trend', '震荡'))
                with _tc3:
                    st.metric("技术强度", f"{_hk_tech_d.get('strength', 50)}/100")
            if COPY_UTILS_AVAILABLE:
                CopyUtils.create_copy_button(st.session_state[_hk_pred_key], button_text="📋 复制全文", key="copy_hk_pred_full")
                CopyUtils.render_markdown_with_section_copy(st.session_state[_hk_pred_key], key_prefix="hk_pred")
            else:
                st.markdown(st.session_state[_hk_pred_key])
            st.caption(f"📌 AI生成 · 模型: {_ai_model_label()}")
        elif not _trigger_all:
            st.caption("点击「一键分析全市场」生成报告")

        if sentiment_hk_key in st.session_state:
            _hk_sent_d = st.session_state[sentiment_hk_key]
            _hk_sent_m = _hk_sent_d['metrics']
            with st.expander(f"📰 舆情 | 评分 {_hk_sent_m.get('sentiment_score', 50)}/100 · {_hk_sent_m.get('sentiment_level', '中性')}", expanded=False):
                if COPY_UTILS_AVAILABLE:
                    CopyUtils.create_copy_button(_hk_sent_d['response'], button_text="📋 复制全文", key="copy_hk_sent_full")
                    CopyUtils.render_markdown_with_section_copy(_hk_sent_d['response'], key_prefix="hk_sent")
                else:
                    st.markdown(_hk_sent_d['response'])
                st.caption(f"📌 AI生成 · 模型: {_ai_model_label()}")

    with ai_tabs[2]:
        _cn_pred_key = 'market_ai_cn'
        sentiment_cn_key = "market_sentiment_cn"
        if _trigger_all and 'market_ai_cn' not in st.session_state:
            _prog = st.progress(0)
            _stat = st.empty()
            try:
                if HAS_PREDICTION_ENGINE:
                    _stat.info("📊 获取上证数据...")
                    _prog.progress(0.15)
                    _cn_tech_df = fetch_stock_data("000001.SS")
                    _prog.progress(0.3)
                    _stat.info("🔍 技术分析中...")
                    _forecaster = MarketForecaster()
                    _cn_tech = _forecaster.analyze_market_technicals(_cn_tech_df, 'A股') if _cn_tech_df is not None and len(_cn_tech_df) >= 20 else {}
                    _prog.progress(0.45)
                    _stat.info("🤖 AI预测走势...")
                    _ai_r = _forecaster.call_gemini_market_forecast([_cn_tech], MY_GEMINI_KEY)
                    st.session_state[_cn_pred_key] = _ai_r.get('A股', 'AI分析结果为空')
                    st.session_state['_cn_tech_data'] = _cn_tech
                if SENTIMENT_ANALYZER_AVAILABLE and _sentiment_analyzer:
                    _prog.progress(0.6)
                    _stat.info("🤖 分析市场舆情...")
                    _sent_prompt = _sentiment_analyzer.generate_market_sentiment_prompt('A股', cn_result)
                    _prog.progress(0.75)
                    _sent_resp = call_gemini_api(_sent_prompt)
                    _sent_metrics = _sentiment_analyzer.parse_sentiment_score(_sent_resp)
                    st.session_state[sentiment_cn_key] = {'response': _sent_resp, 'metrics': _sent_metrics}
                _prog.progress(1.0)
                _prog.empty()
                _stat.empty()
                st.success("✅ A股AI分析完成")
                st.session_state.pop('_one_click_all_markets', None)
            except Exception as e:
                _prog.empty()
                _stat.empty()
                st.error(f"❌ 分析失败: {str(e)[:80]}")

        if _cn_pred_key in st.session_state:
            _cn_tech_d = st.session_state.get('_cn_tech_data', {})
            if _cn_tech_d:
                _tc1, _tc2, _tc3 = st.columns(3)
                with _tc1:
                    st.metric("当前价格", f"{_cn_tech_d.get('current_price', 0):.2f}")
                with _tc2:
                    st.metric("技术趋势", _cn_tech_d.get('trend', '震荡'))
                with _tc3:
                    st.metric("技术强度", f"{_cn_tech_d.get('strength', 50)}/100")
            if COPY_UTILS_AVAILABLE:
                CopyUtils.create_copy_button(st.session_state[_cn_pred_key], button_text="📋 复制全文", key="copy_cn_pred_full")
                CopyUtils.render_markdown_with_section_copy(st.session_state[_cn_pred_key], key_prefix="cn_pred")
            else:
                st.markdown(st.session_state[_cn_pred_key])
            st.caption(f"📌 AI生成 · 模型: {_ai_model_label()}")
        elif not _trigger_all:
            st.caption("点击「一键分析全市场」生成报告")

        if sentiment_cn_key in st.session_state:
            _cn_sent_d = st.session_state[sentiment_cn_key]
            _cn_sent_m = _cn_sent_d['metrics']
            with st.expander(f"📰 舆情 | 评分 {_cn_sent_m.get('sentiment_score', 50)}/100 · {_cn_sent_m.get('sentiment_level', '中性')}", expanded=False):
                if COPY_UTILS_AVAILABLE:
                    CopyUtils.create_copy_button(_cn_sent_d['response'], button_text="📋 复制全文", key="copy_cn_sent_full")
                    CopyUtils.render_markdown_with_section_copy(_cn_sent_d['response'], key_prefix="cn_sent")
                else:
                    st.markdown(_cn_sent_d['response'])
                st.caption(f"📌 AI生成 · 模型: {_ai_model_label()}")

    _link_items = []
    for _mk, _mr in [('🇺🇸 美股', us_result), ('🇭🇰 港股', hk_result), ('🇨🇳 A股', cn_result)]:
        if _mr.get('data_ok'):
            _link_items.append(f"{_mk}：{_mr.get('verdict', 'Unknown')}")
    if _link_items and ('market_ai_us' in st.session_state or 'market_ai_hk' in st.session_state or 'market_ai_cn' in st.session_state):
        st.caption("🌐 体制 · " + " | ".join(_link_items))
        _strong = [n for n, d in [('美股', us_result), ('港股', hk_result), ('A股', cn_result)] if d.get('data_ok') and d.get('verdict') == 'Risk On']
        _weak = [n for n, d in [('美股', us_result), ('港股', hk_result), ('A股', cn_result)] if d.get('data_ok') and d.get('verdict') == 'Risk Off']
        if _strong:
            st.success(f"✅ 风险偏好：{', '.join(_strong)}")
        if _weak:
            st.warning(f"⚠️ 避险模式：{', '.join(_weak)}")


# ═══════════════════════════════════════════════════════════════
# 【行业热力】模块级缓存工具 + get_market_heat（12小时，模块级定义）
# 必须在 if Config.ENABLE_EXPECTATION_LAYER: 之前定义，
# 确保每次 Streamlit rerun 都能找到同一个已缓存的函数对象。
# ═══════════════════════════════════════════════════════════════
_HEAT_CACHE_TS_FILE = _BRIEF_CACHE_DIR / "heat_ts.json"


def _heat_save_ts():
    """记录热力图最后一次成功加载的时间戳"""
    try:
        _BRIEF_CACHE_DIR.mkdir(exist_ok=True)
        _HEAT_CACHE_TS_FILE.write_text(
            json.dumps({"ts": time.time()}), encoding="utf-8"
        )
    except Exception:
        pass


def _heat_remaining_seconds() -> int | None:
    """返回热力图缓存剩余秒数；若无记录返回 None"""
    _HEAT_TTL = 12 * 3600
    try:
        data = json.loads(_HEAT_CACHE_TS_FILE.read_text(encoding="utf-8"))
        rem = int(_HEAT_TTL - (time.time() - data.get("ts", 0)))
        return max(0, rem)
    except Exception:
        return None


@st.cache_data(ttl=43200, show_spinner=False)   # 12 小时缓存
def get_market_heat(_cache_ver="v95"):
    """
    【模块级】环球行业热力图 — 12小时 st.cache_data 缓存。
    定义在模块顶层，避免 rerun 时产生新实例导致缓存失效。
    """
    # 使用 Yahoo Finance 直接可用的代码（港股统一用4位补零格式，避免批量下载代码归一化不匹配）
    SECTORS = {
        "科技":       {"US": "NVDA",  "HK": "0700.HK",  "CN": "601138.SS"},
        "健康护理":   {"US": "JNJ",   "HK": "2269.HK",  "CN": "600276.SS"},
        "公用事业":   {"US": "NEE",   "HK": "0003.HK",  "CN": "600900.SS"},  # 0003=中华煤气，比0002稳
        "通信":       {"US": "T",     "HK": "0728.HK",  "CN": "600050.SS"},  # 0728=中国电信
        "金融":       {"US": "JPM",   "HK": "2318.HK",  "CN": "600036.SS"},
        "工业":       {"US": "CAT",   "HK": "1211.HK",  "CN": "601766.SS"},  # 1211=比亚迪HK，流动性好
        "非必需消费": {"US": "TSLA",  "HK": "3690.HK",  "CN": "002594.SZ"},
        "必需消费":   {"US": "WMT",   "HK": "9633.HK",  "CN": "600519.SS"},
        "原材料":     {"US": "LIN",   "HK": "2899.HK",  "CN": "600028.SS"},
        "房地产":     {"US": "PLD",   "HK": "0016.HK",  "CN": "000002.SZ"},
    }

    import yfinance as _yf
    try:
        from ts_helper import fetch_daily_tushare as _ts_daily, is_cn as _is_cn
        _has_ts = True
    except Exception:
        _has_ts = False

    def _calc_ret(series, days):
        try:
            s = series.dropna()
            if len(s) < days + 1:
                return None
            return float((s.iloc[-1] / s.iloc[-(days + 1)] - 1) * 100)
        except Exception:
            return None

    def _fmt(val):
        if val is None:
            return "N/A"
        icon = "↑" if val > 0 else "↓"
        return f"{icon}{val:+.1f}%"

    def _status(val5d):
        if val5d is None:
            return "⚪"
        if val5d > 2:
            return "🟢"
        if val5d > 0:
            return "🟡"
        if val5d > -2:
            return "🟠"
        return "🔴"

    # 收集所有需要下载的代码
    all_codes = []
    for markets in SECTORS.values():
        all_codes.extend(markets.values())
    all_codes = list(dict.fromkeys(all_codes))   # 去重保序

    # 批量下载（一次请求，节省时间）
    try:
        raw = _yf.download(all_codes, period="90d", progress=False,
                           auto_adjust=True, group_by="ticker")
    except Exception:
        raw = None

    # 单独下载缓存（批量失败时的兜底）
    _single_cache = {}

    def _hk_variants(code):
        """生成港股代码补零/去零两种变体，应对 yfinance 批量下载时自动补零"""
        if not code.endswith(".HK"):
            return [code]
        stem = code[:-3]
        variants = [code]
        padded = stem.zfill(4) + ".HK"
        stripped = stem.lstrip("0").rstrip() + ".HK" if stem.lstrip("0") else "0.HK"
        if padded != code:
            variants.append(padded)
        if stripped != code and stripped != ".HK":
            variants.append(stripped)
        return list(dict.fromkeys(variants))

    def _get_close(code):
        # A股：优先 Tushare（90天已足够热力计算）
        if _has_ts and _is_cn(code):
            try:
                s = _ts_daily(code, days=100)
                if s is not None and len(s) >= 5:
                    return s["Close"]
            except Exception:
                pass
        # 先尝试从批量结果取（含港股补零变体匹配）
        try:
            if raw is not None:
                if hasattr(raw.columns, "levels"):
                    lvl0 = set(raw.columns.get_level_values(0))
                    for _c in _hk_variants(code):
                        if _c in lvl0:
                            s = raw[_c]["Close"].dropna()
                            if len(s) >= 5:
                                return s
                elif len(all_codes) == 1:
                    s = raw["Close"].dropna()
                    if len(s) >= 5:
                        return s
        except Exception:
            pass
        # 批量没拿到，逐变体单独下载兜底
        if code in _single_cache:
            return _single_cache[code]
        for _c in _hk_variants(code):
            try:
                df2 = _yf.download(_c, period="90d", progress=False, auto_adjust=True)
                if df2 is None or len(df2) < 5:
                    continue
                # 兼容新版 yfinance 返回 MultiIndex 列
                if hasattr(df2.columns, "levels") and df2.columns.nlevels == 2:
                    df2.columns = [c[0] for c in df2.columns]
                if "Close" not in df2.columns:
                    continue
                s = df2["Close"].dropna()
                if len(s) >= 5:
                    _single_cache[code] = s
                    return s
            except Exception:
                continue
        _single_cache[code] = None
        return None

    results = []
    for sector_name, markets in SECTORS.items():
        row = {"行业": sector_name}
        for mkt_key, col_name in [("US", "🇺🇸 美股 5日 | 30日 | 60日"),
                                   ("HK", "🇭🇰 港股 5日 | 30日 | 60日"),
                                   ("CN", "🇨🇳 A股 5日 | 30日 | 60日")]:
            code = markets.get(mkt_key, "")
            closes = _get_close(code)
            v5  = _calc_ret(closes, 5)  if closes is not None else None
            v30 = _calc_ret(closes, 30) if closes is not None else None
            v60 = _calc_ret(closes, 60) if closes is not None else None
            row[col_name] = f"{_status(v5)} {_fmt(v5)} | {_fmt(v30)} | {_fmt(v60)}"
        results.append(row)

    _heat_save_ts()
    return pd.DataFrame(results)


# ═══════════════════════════════════════════════════════════════
# 全球市场概览
# ═══════════════════════════════════════════════════════════════
if Config.ENABLE_EXPECTATION_LAYER:
    from datetime import datetime as _dt_global
    from zoneinfo import ZoneInfo
    _global_today = _dt_global.now().strftime("%Y-%m-%d")
    _global_weekday_cn = {"Monday": "周一", "Tuesday": "周二", "Wednesday": "周三", "Thursday": "周四", "Friday": "周五", "Saturday": "周六", "Sunday": "周日"}
    _global_weekday = _global_weekday_cn.get(_dt_global.now().strftime("%A"), "")
    _bj_time = _dt_global.now(ZoneInfo("Asia/Shanghai")).strftime("%H:%M")
    _nasdaq_time = _dt_global.now(ZoneInfo("America/New_York")).strftime("%m/%d %H:%M")
    st.markdown(f'<div style="font-family: Georgia, \'Times New Roman\', SimSun, 宋体, serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 0.4rem 1rem; border-radius: 8px; margin-bottom: 1rem; width: 100%;"><div style="font-family: inherit; color: white; margin: 0; text-align: center; font-size: 12px; font-weight: 700;">🌍 全球市场概览 · 实时监控三大市场体制·把握全球资金流向</div><div style="font-family: inherit; color: rgba(255,255,255,0.7); margin: 0.15rem 0 0 0; text-align: center; font-size: 11px;">📅 {_global_today} {_global_weekday} · 北京 {_bj_time} · 纳斯达克 {_nasdaq_time} ET</div></div>', unsafe_allow_html=True)

    try:
        # 检查是否请求强制刷新
        force_refresh = st.session_state.get('force_refresh_requested', False)
        if force_refresh:
            st.session_state['force_refresh_requested'] = False
            st.cache_data.clear()
            try:
                local_cache.clear_all()   # 穿透文件缓存层，彻底刷新
            except Exception:
                pass

        # 启动性能监控
        _perf_monitor.start()

        @st.cache_data(ttl=300, show_spinner=False)
        def _cached_all_markets(_ts=None):
            return _expectation_layer.analyze_all_markets(force_refresh=False)

        _cache_ts = int(time.time() // 300)  # 每5分钟变一次，触发缓存刷新
        all_markets = _cached_all_markets(_ts=_cache_ts)
        
        # 【V89.2】保存到session_state供机构研究中心使用
        st.session_state.all_markets = all_markets
        
        us_result = all_markets['us_market']
        hk_result = all_markets['hk_market']
        cn_result = all_markets['cn_market']
        summary = all_markets['summary']
        
        # 顶部：全球综合状态
        st.markdown("### 🌐 全球市场综合")
        summary_col1, summary_col2, summary_col3 = st.columns([2, 1, 1])
        
        with summary_col1:
            global_verdict = summary['global_verdict']
            _gv = "".join(c for c in str(global_verdict) if ord(c) >= 32 or c in "\n\t\r").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
            _gr = "".join(c for c in str(summary.get("global_reason", "")) if ord(c) >= 32 or c in "\n\t\r").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
            st.markdown(f'<div style="font-family: Georgia, \'Times New Roman\', SimSun, 宋体, serif; background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); padding: 1rem; border-radius: 8px;"><div style="font-family: inherit; color: white; font-size: 12px; font-weight: 600;">{_gv}</div><div style="font-family: inherit; color: rgba(255,255,255,0.9); font-size: 12px; margin-top: 0.5rem;">{_gr}</div></div>', unsafe_allow_html=True)
        
        with summary_col2:
            _r_on = summary.get('risk_on_count', 0)
            st.metric("Risk On市场", _safe_str_for_dom(f"{_r_on}/3"), 
                     delta=_safe_str_for_dom("偏好") if _r_on >= 2 else None)
        
        with summary_col3:
            _r_off = summary.get('risk_off_count', 0)
            st.metric("Risk Off市场", _safe_str_for_dom(f"{_r_off}/3"),
                     delta=_safe_str_for_dom("避险") if _r_off >= 2 else None,
                     delta_color="inverse")
        
        st.divider()
        
        # ═══════════════════════════════════════════════════════════════
        # 【V90】宏观脉搏监控 - 三行布局，每行一个市场，指标横向排列
        # ═══════════════════════════════════════════════════════════════
        st.markdown("### 📡 宏观脉搏监控")
        _macro_fetch_bj = _dt_global.now(ZoneInfo("Asia/Shanghai")).strftime("%m/%d %H:%M")
        _macro_fetch_ny = _dt_global.now(ZoneInfo("America/New_York")).strftime("%H:%M ET")
        st.caption(f"💡 机构交易员的「上帝视角」——在看个股之前，先看天气 · 📅 数据截至最近收盘 · 页面加载于 {_macro_fetch_bj} 北京 / {_macro_fetch_ny}")
        
        # 修复 InvalidCharacterError：移除控制字符、null、无效 Unicode，转义 HTML 特殊字符
        def _sanitize_html(s):
            if s is None: return ""
            s = str(s)
            if s.lower() in ("nan", "inf", "-inf", "infinity", "-infinity"):
                return "N/A"
            s = "".join(c for c in s if ord(c) >= 32 or c in "\n\t\r")
            s = s.replace("\x00", "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
            return s
        def _safe_num(val, fmt="{:.1f}", default="N/A"):
            try:
                v = float(val)
                if v != v or v == float("inf") or v == float("-inf"): return default
                return fmt.format(v)
            except (TypeError, ValueError): return default
        # 英文术语→括号中文（小一号）辅助函数
        def _reason_cn(s):
            s = _sanitize_html(s or "")
            _s = '<span style="font-size: 0.85em;">'
            return (s.replace("SPY在", f'SPY{_s}(标普500ETF)</span>在').replace("SPY(", f'SPY{_s}(标普500ETF)</span>(')
                    .replace("VIX=", f'VIX{_s}(波动率指数)</span>=').replace("VIX(", f'VIX{_s}(波动率指数)</span>(')
                    .replace("MA50(", f'MA50{_s}(50日均线)</span>(').replace("MA200(", f'MA200{_s}(200日均线)</span>('))
        st.markdown("#### 🇺🇸 美国")
        us_cols = st.columns(8)
        with us_cols[0]:
            _v = us_result.get('vix_level', 0)
            _val = _safe_str_for_dom(_safe_num(_v, "{:.1f}") if _v else "N/A")
            _d = _safe_str_for_dom(f"{_safe_num(us_result.get('vix_change_pct', 0), '{:+.1f}')}%" if _v else None) or None
            st.metric("VIX (波动率)", _val, delta=_d, delta_color="inverse")
        with us_cols[1]:
            _v = us_result.get('spy_price', 0)
            _val = _safe_str_for_dom(f"${_safe_num(_v, '{:.1f}')}" if _v and (_v == _v) else "N/A")
            _d = _safe_str_for_dom(f"{_safe_num(us_result.get('spy_change_pct', 0), '{:+.1f}')}%" if _v else None) or None
            st.metric("S&P500", _val, delta=_d)
        with us_cols[2]:
            _v = us_result.get('tlt_price', 0)
            _val = _safe_str_for_dom(f"${_safe_num(_v, '{:.1f}')}" if _v else "N/A")
            _d = _safe_str_for_dom(f"{_safe_num(us_result.get('tlt_change_pct', 0), '{:+.1f}')}%" if _v else None) or None
            st.metric("TLT (美债ETF)", _val, delta=_d)
        with us_cols[3]:
            _v = us_result.get('gld_price', 0)
            _val = _safe_str_for_dom(f"${_safe_num(_v, '{:.1f}')}" if _v else "N/A")
            _d = _safe_str_for_dom(f"{_safe_num(us_result.get('gld_change_pct', 0), '{:+.1f}')}%" if _v else None) or None
            st.metric("GLD (黄金)", _val, delta=_d)
        with us_cols[4]:
            _v = us_result.get('tnx_yield', 0)
            _val = _safe_str_for_dom(f"{_safe_num(_v, '{:.2f}')}%" if _v else "N/A")
            _d = _safe_str_for_dom(_safe_num(us_result.get('tnx_change', 0), "{:+.2f}") if _v else None) or None
            st.metric("10Y美债", _val, delta=_d, delta_color="inverse")
        with us_cols[5]:
            _v = us_result.get('dxy_level', 0)
            _val = _safe_str_for_dom(_safe_num(_v, "{:.1f}") if _v else "N/A")
            _d = _safe_str_for_dom(f"{_safe_num(us_result.get('dxy_change_pct', 0), '{:+.1f}')}%" if _v else None) or None
            st.metric("DXY (美元)", _val, delta=_d, delta_color="inverse")
        with us_cols[6]:
            _corr = us_result.get('correlation', None)
            _corr_val = _safe_str_for_dom(f"{_corr:.2f}" if _corr is not None else "N/A")
            _corr_desc = _sanitize_html(us_result.get('corr_desc', ''))
            st.metric("股债相关性", _corr_val)
            if _corr_desc:
                st.caption(_corr_desc[:20])
        with us_cols[7]:
            _us_v = us_result.get('verdict', 'Unknown')
            _us_color = "#10b981" if _us_v == "Risk On" else ("#ef4444" if _us_v == "Risk Off" else "#f59e0b")
            _us_v_safe = _sanitize_html(_us_v)
            st.markdown(f'<div style="font-family: Georgia, \'Times New Roman\', SimSun, 宋体, serif; background:{_us_color};color:white;padding:0.6rem;border-radius:6px;font-weight:600;text-align:center;margin-top:1.2rem;font-size:12px;">{_us_v_safe}</div>', unsafe_allow_html=True)
        _vix_st = _sanitize_html(us_result.get("vix_status", ""))
        _reason_safe = _sanitize_html(us_result.get("reason", ""))[:120]
        st.caption(f"[美国] {_vix_st} | {_reason_safe}")
        
        # 第二行：A股（6个指标：上证、沪深300、创业板、波动率、人民币、体制）
        st.markdown("#### 🇨🇳 A股")
        cn_cols = st.columns(6)
        _cn_idx = cn_result.get('index_level', 0)
        _cn_chg = cn_result.get('index_change_pct', 0)
        _cn_vol = cn_result.get('volatility', 0)
        _cn_v = cn_result.get('verdict', 'Unknown')
        _cn_color = "#10b981" if _cn_v == "Risk On" else ("#ef4444" if _cn_v == "Risk Off" else "#f59e0b")
        with cn_cols[0]:
            _cn_val = f"{_cn_idx:.0f}" if _cn_idx and _cn_idx == _cn_idx and _cn_idx > 0 else "N/A"
            _cn_d = f"{_cn_chg:+.2f}%" if _cn_idx and _cn_idx > 0 and _cn_chg == _cn_chg else None
            st.metric("上证指数", _safe_str_for_dom(_cn_val), delta=_safe_str_for_dom(_cn_d) if _cn_d else None)
        with cn_cols[1]:
            _hs300 = cn_result.get('hs300_price', 0)
            _hs_val = f"{_hs300:.0f}" if _hs300 and _hs300 > 0 else "N/A"
            _hs_d = f"{cn_result.get('hs300_change_pct', 0):+.2f}%" if _hs300 and _hs300 > 0 else None
            st.metric("沪深300", _safe_str_for_dom(_hs_val), delta=_safe_str_for_dom(_hs_d) if _hs_d else None)
        with cn_cols[2]:
            _cyb = cn_result.get('cyb_price', 0)
            _cy_val = f"{_cyb:.0f}" if _cyb and _cyb > 0 else "N/A"
            _cy_d = f"{cn_result.get('cyb_change_pct', 0):+.2f}%" if _cyb and _cyb > 0 else None
            st.metric("创业板指", _safe_str_for_dom(_cy_val), delta=_safe_str_for_dom(_cy_d) if _cy_d else None)
        with cn_cols[3]:
            st.metric("波动率", _safe_str_for_dom(f"{_cn_vol:.1f}%" if _cn_vol and _cn_vol > 0 else "N/A"))
        with cn_cols[4]:
            _cny = cn_result.get('cny_price', 0)
            _cny_val = f"{_cny:.4f}" if _cny and _cny > 0 else "N/A"
            _cny_d = f"{cn_result.get('cny_change_pct', 0):+.2f}%" if _cny and _cny > 0 else None
            st.metric("人民币", _safe_str_for_dom(_cny_val), delta=_safe_str_for_dom(_cny_d) if _cny_d else None)
        with cn_cols[5]:
            _cn_v_safe = _sanitize_html(_cn_v)
            st.markdown(f'<div style="font-family: Georgia, \'Times New Roman\', SimSun, 宋体, serif; background:{_cn_color};color:white;padding:0.6rem;border-radius:6px;font-weight:600;text-align:center;margin-top:1.2rem;">{_cn_v_safe}</div>', unsafe_allow_html=True)
        _cn_vol_st = _sanitize_html(cn_result.get("vol_status", ""))
        st.markdown(f'<p style="font-size: 12px; color: #666;">[A股] {_cn_vol_st} | {_reason_cn(cn_result.get("reason", ""))[:80]}</p>', unsafe_allow_html=True)
        
        # 第三行：港股（6个指标：恒指、恒生科技、国企指数、波动率、港币、体制）
        st.markdown("#### 🇭🇰 港股")
        hk_cols = st.columns(6)
        _hk_idx = hk_result.get('index_level', 0)
        _hk_chg = hk_result.get('index_change_pct', 0)
        _hk_vol = hk_result.get('volatility', 0)
        _hk_v = hk_result.get('verdict', 'Unknown')
        _hk_color = "#10b981" if _hk_v == "Risk On" else ("#ef4444" if _hk_v == "Risk Off" else "#f59e0b")
        with hk_cols[0]:
            _hk_val = f"{_hk_idx:.0f}" if _hk_idx and _hk_idx == _hk_idx and _hk_idx > 0 else "N/A"
            _hk_d = f"{_hk_chg:+.2f}%" if _hk_idx and _hk_idx > 0 and _hk_chg == _hk_chg else None
            st.metric("恒生指数", _safe_str_for_dom(_hk_val), delta=_safe_str_for_dom(_hk_d) if _hk_d else None)
        with hk_cols[1]:
            _hstech = hk_result.get('hstech_price', 0)
            _use_etf = hk_result.get('hstech_use_etf', False)
            _label = _safe_str_for_dom("恒生科技(ETF)" if _use_etf else "恒生科技")
            _fmt = f"{_hstech:.2f}" if _use_etf else f"{_hstech:.0f}"
            _hst_val = _fmt if _hstech and _hstech == _hstech and _hstech > 0 else "N/A"
            _hst_d = f"{hk_result.get('hstech_change_pct', 0):+.2f}%" if _hstech and _hstech > 0 else None
            st.metric(_label, _safe_str_for_dom(_hst_val), delta=_safe_str_for_dom(_hst_d) if _hst_d else None)
        with hk_cols[2]:
            _hsce = hk_result.get('hsce_price', 0)
            _hsce_val = f"{_hsce:.0f}" if _hsce and _hsce > 0 else "N/A"
            _hsce_d = f"{hk_result.get('hsce_change_pct', 0):+.2f}%" if _hsce and _hsce > 0 else None
            st.metric("国企指数", _safe_str_for_dom(_hsce_val), delta=_safe_str_for_dom(_hsce_d) if _hsce_d else None)
        with hk_cols[3]:
            st.metric("波动率", _safe_str_for_dom(f"{_hk_vol:.1f}%" if _hk_vol and _hk_vol > 0 else "N/A"))
        with hk_cols[4]:
            _hkd = hk_result.get('hkd_price', 0)
            _hkd_val = f"{_hkd:.4f}" if _hkd and _hkd > 0 else "N/A"
            _hkd_d = f"{hk_result.get('hkd_change_pct', 0):+.2f}%" if _hkd and _hkd > 0 else None
            st.metric("港币", _safe_str_for_dom(_hkd_val), delta=_safe_str_for_dom(_hkd_d) if _hkd_d else None)
        with hk_cols[5]:
            _hk_v_safe = _sanitize_html(_hk_v)
            st.markdown(f'<div style="font-family: Georgia, \'Times New Roman\', SimSun, 宋体, serif; background:{_hk_color};color:white;padding:0.6rem;border-radius:6px;font-weight:600;text-align:center;margin-top:1.2rem;">{_hk_v_safe}</div>', unsafe_allow_html=True)
        _hk_vol_st = _sanitize_html(hk_result.get("vol_status", ""))
        st.markdown(f'<p style="font-size: 12px; color: #666;">[港股] {_hk_vol_st} | {_reason_cn(hk_result.get("reason", ""))[:80]}</p>', unsafe_allow_html=True)
        
        # 宏观综合解读条 - 英文术语括号中文小一号
        try:
            _pos_cap = int(float(us_result.get('position_cap', 80)))
            if _pos_cap != _pos_cap or _pos_cap < 0 or _pos_cap > 100: _pos_cap = 80
        except (TypeError, ValueError): _pos_cap = 80
        _macro_reason = _reason_cn(us_result.get('reason', ''))
        _cap_color = "#10b981" if _pos_cap >= 70 else ("#f59e0b" if _pos_cap >= 50 else "#ef4444")
        st.markdown(f'<div style="font-family: Georgia, \'Times New Roman\', SimSun, 宋体, serif; background: linear-gradient(135deg, #1e293b 0%, #334155 100%); padding: 1rem 1.5rem; border-radius: 8px; margin-top: 0.5rem; display: flex; align-items: center; gap: 1rem;"><div style="font-family: inherit; color: white; flex: 1; font-size: 12px;">💡 <b>宏观解读</b>：{_macro_reason}</div><div style="font-family: inherit; background: {_cap_color}; color: white; padding: 0.5rem 1rem; border-radius: 6px; font-weight: 700; font-size: 12px; white-space: nowrap;">仓位上限 {_pos_cap}%</div></div>', unsafe_allow_html=True)
        st.markdown('📖 仓位上限 = 基于VIX<span style="font-size: 0.9em;">(波动率指数)</span> + 美债收益率 + 市场体制的综合建议。Risk Off<span style="font-size: 0.9em;">(风险规避)</span>时建议最多30%仓位，避免满仓硬扛', unsafe_allow_html=True)
        
        st.divider()
        

        # ═══════════════════════════════════════════════════════════════
        # 行业热力
        # ═══════════════════════════════════════════════════════════════
        st.markdown(f"### 🌡️ 行业热力 · {_dt_global.now().strftime('%Y-%m-%d')}")

        # 缓存倒计时
        _heat_remain = _heat_remaining_seconds()
        if _heat_remain is not None and _heat_remain > 0:
            _heat_h, _heat_m = divmod(_heat_remain // 60, 60)
            _heat_countdown = f"⏱ 缓存剩余 {_heat_h}h {_heat_m:02d}m"
        else:
            _heat_countdown = "⏱ 缓存已过期"
        _heat_load_bj = _dt_global.now(ZoneInfo("Asia/Shanghai")).strftime("%H:%M")
        st.caption(f"环球行业资金走向 · 点击行业可一键 AI 分析 · 📊 代表性个股涨跌幅（5/30/60日） · {_heat_countdown} · 加载于 {_heat_load_bj} 北京")

        _heat_rcol = st.columns([5, 1])[1]
        with _heat_rcol:
            if st.button("🔄 强制刷新", key="refresh_heat", help="穿透全部缓存，重新拉取行业数据", use_container_width=True):
                get_market_heat.clear()
                st.cache_data.clear()
                try:
                    local_cache.clear_all()
                except Exception:
                    pass
                # 清除时间戳让倒计时归零
                try:
                    _HEAT_CACHE_TS_FILE.unlink(missing_ok=True)
                except Exception:
                    pass
                st.rerun()

        heat_df = get_market_heat()
        display_cols = ["行业", "🇺🇸 美股 5日 | 30日 | 60日", "🇭🇰 港股 5日 | 30日 | 60日", "🇨🇳 A股 5日 | 30日 | 60日"]
        selected_heat = st.dataframe(
            heat_df[display_cols],
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            key="heat_table"
        )

        if selected_heat and len(selected_heat.selection.rows) > 0:
            selected_idx = selected_heat.selection.rows[0]
            selected_row = heat_df.iloc[selected_idx]
            sector_name = selected_row["行业"]
            st.info(f"📊 已选择 **{sector_name}** 行业")
            if st.button(f"🌍 一键分析全球{sector_name}行业（美股+港股+A股）", key="analyze_global_sector", use_container_width=True, type="primary"):
                st.session_state.sector_analysis_name = sector_name
                st.session_state.sector_analysis_market = "全球"
                st.session_state.sector_analysis_codes = {
                    "us": selected_row["_us_code"],
                    "hk": selected_row["_hk_code"],
                    "cn": selected_row["_cn_code"]
                }
                st.toast(f"🚀 AI分析全球{sector_name}行业中...", icon="🌍")
                st.rerun()

        if 'sector_analysis_name' in st.session_state and st.session_state.sector_analysis_name:
            sector_name_s = st.session_state.sector_analysis_name
            codes_s = st.session_state.sector_analysis_codes

            st.markdown("---")
            st.markdown(f"### 🌍 全球{sector_name_s}行业 AI综合分析")
            st.caption(f"📅 {_dt_global.now().strftime('%Y-%m-%d %A')}")

            if st.button("❌ 关闭", key="close_sector_analysis"):
                st.session_state.sector_analysis_name = None
                st.session_state.sector_analysis_market = None
                st.session_state.sector_analysis_codes = None
                st.rerun()

            if not HAS_GEMINI:
                st.error("❌ 未安装 google-generativeai 库")
            elif not MY_GEMINI_KEY:
                st.error("❌ 未配置 Gemini API Key")
            else:
                from datetime import datetime as _dt_sector
                today_s = _dt_sector.now().strftime("%Y年%m月%d日")

                prompt_s = _load_prompt(
                    "sector_analysis.txt",
                    sector_name=sector_name_s,
                    today=today_s,
                    us_code=codes_s["us"],
                    hk_code=codes_s["hk"],
                    cn_code=codes_s["cn"],
                )
                try:
                    analysis_text_s = st.write_stream(call_gemini_api_stream(prompt_s))
                    if COPY_UTILS_AVAILABLE:
                        CopyUtils.create_copy_button(analysis_text_s, button_text="📋 复制全文", key="copy_global_sector_full")
                    st.caption(f"📌 AI生成 · 模型: {_ai_model_label()}")
                except Exception as e:
                    st.error(f"❌ AI分析失败: {type(e).__name__}: {str(e)}")

        st.divider()

        # AI综合分析（fragment 局部刷新）
        _render_ai_market_analysis()
        
    except Exception as e:
        # 宏观模块异常不影响主应用
        st.warning(f"⚠️  全球市场概览加载异常，主功能不受影响。错误信息: {str(e)[:100]}")
        logging.error(f"宏观仪表盘渲染异常: {e}")
        import traceback
        traceback.print_exc()

    st.markdown("---")  # 分隔线

# 【V90.3】性能监控已移到左侧边栏

st.markdown("""
<style>
    /* 【华尔街日报电子版风格】全站统一衬线体，消除字体参差不齐 */
    :root {
        --nyt-serif: Georgia, "Times New Roman", "SimSun", "宋体", "STSong", "华文宋体", "Songti SC", "Noto Serif SC", serif;
        --nyt-body-size: 13px;
        --nyt-headline-size: 12px;
        --nyt-line-height: 1.6;
    }
    /* 全局强制衬线体 - 覆盖 Streamlit 与自定义内容 */
    html, body, [class*="css"], 
    [data-testid="stMarkdown"] div, [data-testid="stMarkdown"] p, [data-testid="stMarkdown"] span,
    [data-testid="stMarkdown"] h1, [data-testid="stMarkdown"] h2, [data-testid="stMarkdown"] h3,
    [data-testid="stMarkdown"] h4, [data-testid="stMarkdown"] h5, [data-testid="stMarkdown"] h6,
    [data-testid="stMarkdown"] b, [data-testid="stMarkdown"] strong {
        font-family: var(--nyt-serif) !important;
    }
    html, body, [class*="css"] {
        font-size: var(--nyt-body-size) !important;
        line-height: var(--nyt-line-height) !important;
        font-feature-settings: "kern" 1, "liga" 1;
    }
    /* 【V92】全页背景 - 消除大片空白感，浅灰填充 */
    html, body { background: #f1f5f9 !important; min-height: 100vh !important; }
    div[data-testid="stAppViewContainer"] { background: #f1f5f9 !important; }
    section[data-testid="stSidebar"] { background: #f8fafc !important; }
    .block-container { background: transparent !important; }
    /* 标题层级：衬线体加粗 */
    h1, h2, h3, h4, h5, h6, [data-testid="stMarkdown"] strong {
        font-family: var(--nyt-serif) !important;
        font-weight: 700 !important;
    }
    /* st.metric 数值与标签统一衬线体 - 消除数字与文字字体不一致 */
    div[data-testid="stMetric"], div[data-testid="stMetric"] *,
    section[data-testid="stSidebar"] * {
        font-family: var(--nyt-serif) !important;
    }
    /* 数字对齐 - 保持衬线体下的等宽数字 */
    .stMetric, div[data-testid="stDataFrame"], [data-testid="stMarkdown"] {
        font-variant-numeric: tabular-nums;
    }
    
    /* 【修复】容器上边距增加，防止标题被遮挡 */
    .block-container { padding-top: 3rem !important; padding-bottom: 3rem !important; }
    
    /* 按钮样式 */
    div.stButton > button {
        font-family: var(--nyt-serif) !important;
        width: 100%; border: 1px solid #e5e7eb; background-color: #f9fafb; color: #1f2937;
        font-weight: 600; padding: 0.6rem 1rem; border-radius: 8px;
        transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
    }
    div.stButton > button:hover { 
        border-color: #2563eb; color: #2563eb; background-color: #eff6ff; 
        transform: translateY(-1px); box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    
    /* AI 卡片 */
    .ai-card { 
        font-family: var(--nyt-serif) !important;
        background: rgba(255, 255, 255, 0.95); backdrop-filter: blur(10px);
        border: 1px solid #e2e8f0; border-radius: 16px; padding: 24px; margin-bottom: 20px; 
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1); position: relative; overflow: hidden;
    }
    .ai-card::before {
        content: ""; position: absolute; top: 0; left: 0; width: 100%; height: 4px;
        background: linear-gradient(90deg, #3b82f6, #8b5cf6);
    }
    .ai-title { 
        font-family: var(--nyt-serif) !important;
        font-size: var(--nyt-headline-size); font-weight: 700; color: #1e3a8a; margin-bottom: 16px; 
        border-bottom: 1px solid #e5e7eb; padding-bottom: 12px; display: flex; align-items: center; gap: 10px;
    }
    
    /* 【新增】表格行悬停效果 - 提示可点击 */
    div[data-testid="stDataFrame"] tbody tr:hover {
        background-color: #eff6ff !important;
        cursor: pointer !important;
    }
    
    /* 表格选中行高亮 */
    div[data-testid="stDataFrame"] tbody tr.row-selected {
        background-color: #dbeafe !important;
        font-weight: 600;
    }
    
    /* 【NYT 风格】全局正文 13px，表格与指标统一衬线 */
    [data-testid="stMarkdown"] p, [data-testid="stMarkdown"] li, [data-testid="stMarkdown"] span {
        font-family: var(--nyt-serif) !important;
        font-size: var(--nyt-body-size) !important;
    }
    div[data-testid="stDataFrame"] {
        font-family: var(--nyt-serif) !important;
        font-size: var(--nyt-body-size) !important;
    }
    .stCaption {
        font-family: var(--nyt-serif) !important;
        font-size: 12px !important;
    }
    .stMetric {
        font-family: var(--nyt-serif) !important;
        font-size: var(--nyt-body-size) !important;
    }
    /* 侧边栏 */
    [data-testid="stSidebar"] {
        font-family: var(--nyt-serif) !important;
    }
    
    /* 【V92】侧边栏收起按钮 - 提高可见性，便于用户找到 */
    [data-testid="stSidebar"] [data-testid="collapsedControl"],
    button[aria-label*="collapse"], button[aria-label*="Close sidebar"],
    [data-testid="stSidebar"] > div:first-child button {
        opacity: 1 !important;
        z-index: 9999 !important;
    }

    /* 【V91.9】禁用 Streamlit 运行时的灰屏遮罩，保持页面正常亮度 */
    [data-stale="true"], [data-stale="stale"], [stale-data="true"] {
        opacity: 1 !important;
        filter: none !important;
    }
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# 1. 配置常量（V88：使用新模块）
# ═══════════════════════════════════════════════════════════════
# 【V89.5】注释：MY_GEMINI_KEY和GEMINI_MODEL_NAME已在前面定义
if USE_NEW_MODULES:
    # MY_GEMINI_KEY = mod_config.GEMINI_API_KEY  # 已在前面定义
    # GEMINI_MODEL_NAME = mod_config.GEMINI_MODEL_NAME  # 已在前面定义
    CACHE_TTL = mod_config.CACHE_TTL_SECONDS
    CACHE_MAX_SIZE_MB = mod_config.CACHE_MAX_SIZE_MB
    logging.info(f"✅ 使用V88配置模块: Gemini={GEMINI_MODEL_NAME}, 缓存={CACHE_MAX_SIZE_MB}MB")
else:
    # MY_GEMINI_KEY = st.secrets.get("GEMINI_API_KEY", os.getenv("GEMINI_API_KEY", ""))  # 已在前面定义
    # GEMINI_MODEL_NAME = "gemini-2.5-flash"  # 已在前面定义
    CACHE_TTL = 900  # 交易日15分钟
    CACHE_MAX_SIZE_MB = 1500

# 【V87.11】配置 Gemini API（已在前面配置）
# if HAS_GEMINI and MY_GEMINI_KEY:
#     genai.configure(api_key=MY_GEMINI_KEY)

# 【自选股】按中美港划分，与钉钉日报同源，可编辑
WATCHLIST = {
    "US": [
        ("ABBV", "艾伯维"), ("ACMR", "ACM Research"), ("NVDA", "英伟达"), ("NVO", "诺和诺德"),
        ("VOO", "标普500ETF"), ("BRK.B", "伯克希尔"), ("QQQM", "纳指100ETF"),
        ("GOOG", "谷歌"), ("PM", "菲利普莫里斯"), ("LLY", "礼来制药"), ("TSM", "台积电"),
        ("TSLA", "特斯拉"),
    ],
    "HK": [
        ("0700.HK", "腾讯控股"), ("0883.HK", "中国海洋石油"), ("1299.HK", "友邦保险"),
        ("0941.HK", "中国移动"),
    ],
    "CN": [
        ("600519.SS", "贵州茅台"), ("688981.SS", "中芯国际"), ("601899.SS", "紫金矿业"),
    ],
}

# ═══════════════════════════════════════════════════════════════
# 1.5 【V88】本地文件缓存系统（使用新的LRU版本）
# ═══════════════════════════════════════════════════════════════
if USE_NEW_MODULES:
    # 使用新的LRU缓存系统
    logging.info("✅ 使用V88 LRU缓存系统")
    LocalFileCache = mod_cache.LocalFileCache
else:
    # 使用原版缓存系统
    class LocalFileCache:
        """
        本地文件缓存系统
        - 缓存存储在本地文件中，刷新页面不丢失
        - 5分钟过期时间
        - 500MB容量限制，超出自动清理最旧的缓存
        """
        def __init__(self, cache_dir=".cache_stock_data", max_size_mb=500, ttl_seconds=300):
            self.cache_dir = Path(cache_dir)
            self.cache_dir.mkdir(exist_ok=True)
            self.max_size_bytes = max_size_mb * 1024 * 1024
            self.ttl_seconds = ttl_seconds
        
        def _get_cache_key(self, key_str):
            """生成缓存文件名"""
            return hashlib.md5(key_str.encode()).hexdigest()
        
        def _get_cache_path(self, cache_key):
            """获取缓存文件路径"""
            return self.cache_dir / f"{cache_key}.pkl"
        
        def _get_cache_size(self):
            """获取缓存目录总大小（字节）"""
            total_size = 0
            for file in self.cache_dir.glob("*.pkl"):
                try:
                    total_size += file.stat().st_size
                except:
                    pass
            return total_size
        
        def _clean_old_cache(self):
            """清理缓存：满500MB直接清零重新开始"""
            current_size = self._get_cache_size()
            
            if current_size <= self.max_size_bytes:
                return
            
            # 【V87.15改进】达到容量限制，直接清空所有缓存
            _safe_print(f"[缓存清理] ⚠️ 容量已满 ({current_size/1024/1024:.1f}MB / {self.max_size_bytes/1024/1024:.0f}MB)")
            _safe_print(f"[缓存清理] 🗑️ 清空所有缓存，重新开始...")
            
            deleted_count = 0
            deleted_size = 0
            
            for file in self.cache_dir.glob("*.pkl"):
                try:
                    size = file.stat().st_size
                    file.unlink()
                    deleted_count += 1
                    deleted_size += size
                except Exception as e:
                    _safe_print(f"[缓存清理] ❌ 删除失败 {file.name}: {e}")
            
            _safe_print(f"[缓存清理] ✅ 已清空 {deleted_count} 个文件，释放 {deleted_size/1024/1024:.1f}MB")
        
        def get(self, key_str):
            """【V87.16】获取缓存 - 增强错误处理"""
            cache_key = self._get_cache_key(key_str)
            cache_path = self._get_cache_path(cache_key)
            
            if not cache_path.exists():
                return None
            
            try:
                # 检查是否过期
                mtime = cache_path.stat().st_mtime
                age = time.time() - mtime
                
                if age > self.ttl_seconds:
                    # 过期，删除
                    cache_path.unlink()
                    logging.debug(f"缓存过期已删除: {key_str[:50]}... (年龄: {age:.1f}秒)")
                    return None
                
                # 【V87.16】安全的pickle加载
                with open(cache_path, 'rb') as f:
                    data = pickle.load(f)
                
                logging.info(f"✅ 缓存命中: {key_str[:50]}... (年龄: {age:.1f}秒)")
                return data
            
            except (pickle.UnpicklingError, EOFError, ValueError) as e:
                # 【V87.16】pickle损坏，删除并重新获取
                logging.warning(f"⚠️ 缓存文件损坏: {type(e).__name__}, 已删除")
                try:
                    cache_path.unlink()
                except:
                    pass
                return None
            
            except Exception as e:
                logging.error(f"❌ 缓存读取失败: {type(e).__name__}: {str(e)[:100]}")
                try:
                    cache_path.unlink()
                except:
                    pass
                return None
        
        def set(self, key_str, data):
            """设置缓存"""
            cache_key = self._get_cache_key(key_str)
            cache_path = self._get_cache_path(cache_key)
            
            try:
                # 保存缓存
                with open(cache_path, 'wb') as f:
                    pickle.dump(data, f)
                
                # 检查并清理容量
                self._clean_old_cache()
                
                _safe_print(f"[缓存保存] {key_str[:50]}...")
            
            except Exception as e:
                _safe_print(f"[缓存保存失败] {type(e).__name__}: {str(e)[:100]}")
        
        def clear_all(self):
            """清空所有缓存"""
            try:
                shutil.rmtree(self.cache_dir)
                self.cache_dir.mkdir(exist_ok=True)
                _safe_print("[缓存清空] 所有缓存已清除")
            except Exception as e:
                _safe_print(f"[缓存清空失败] {type(e).__name__}: {str(e)[:100]}")
        
        def get_stats(self):
            """获取缓存统计信息"""
            try:
                total_size = self._get_cache_size()
                file_count = len(list(self.cache_dir.glob("*.pkl")))
                
                return {
                    'total_size_mb': total_size / 1024 / 1024,
                    'file_count': file_count,
                    'max_size_mb': self.max_size_bytes / 1024 / 1024,
                    'usage_percent': (total_size / self.max_size_bytes) * 100 if self.max_size_bytes > 0 else 0,
                    'ttl_seconds': self.ttl_seconds
                }
            except:
                return {'total_size_mb': 0, 'file_count': 0, 'max_size_mb': self.max_size_bytes / 1024 / 1024, 'usage_percent': 0, 'ttl_seconds': self.ttl_seconds}

# 【V88】初始化全局缓存实例（使用新的LRU系统）
if USE_NEW_MODULES:
    local_cache = mod_cache.get_cache(
        cache_dir=mod_config.CACHE_DIR,
        max_size_mb=CACHE_MAX_SIZE_MB,
        ttl_seconds=CACHE_TTL
    )
    logging.info(f"✅ V88 LRU缓存已初始化: {CACHE_MAX_SIZE_MB}MB, TTL={CACHE_TTL}s")
else:
    local_cache = LocalFileCache(max_size_mb=CACHE_MAX_SIZE_MB, ttl_seconds=CACHE_TTL)

# 【V87.15】容量评估说明
# 单只股票数据约 0.3-0.5MB（包含DataFrame + 元数据）
# 680只股票池 × 0.4MB = 272MB
# 考虑重复查询、扫描结果等，实际使用约 400-600MB/天
# 1.5GB 容量可支持约 3天的数据缓存

# ═══════════════════════════════════════════════════════════════
# 2. ProxyContext 类（V72 核心技术）
# ═══════════════════════════════════════════════════════════════
class ProxyContext:
    def __init__(self, proxy_url):
        self.proxy_url = proxy_url
        self.old_env = {}
    
    def __enter__(self):
        if self.proxy_url:
            for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY']:
                self.old_env[key] = os.environ.get(key)
                os.environ[key] = self.proxy_url
        return self
    
    def __exit__(self, *args):
        for key, val in self.old_env.items():
            if val is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = val

def get_proxy_url():
    port = st.session_state.get('proxy_port', '1082')
    return f"http://127.0.0.1:{port}"

# ═══════════════════════════════════════════════════════════════
# 3. 数据获取 - 【V75核心】添加重试机制
# ═══════════════════════════════════════════════════════════════
def clean_df(df):
    """清洗数据"""
    if False:  # removed fallback placeholder
        special_codes = {
            'BRK.B': 'BRK-B',
            'BRK.A': 'BRK-A',
            'BF.B': 'BF-B',
            'BF.A': 'BF-A',
        }
        if code in special_codes:
            old_code = code
            code = special_codes[code]
            logging.info(f"📝 特殊代码修正: {old_code} -> {code}")
        
        # 【V87.4 Critical Fix】已经有后缀的需要检查港股前导零问题
        if code.endswith(".SS") or code.endswith(".SZ"): 
            return code
        elif code.endswith(".HK"):
            # 检查港股前导零问题：09992.HK -> 9992.HK
            hk_num = code[:-3]  # 去掉 .HK
            if hk_num.isdigit() and len(hk_num) == 5 and hk_num.startswith('0'):
                # 去掉前导零：09992 -> 9992
                corrected_num = hk_num[1:]
                return f"{corrected_num}.HK"
            else:
                return code
        
        # 沪市改为 .SS
        if code.endswith(".SH"): 
            return code[:-3] + ".SS"
        
        # 纯数字代码判断
        if code.isdigit():
            # 【V75.2 最终修复】港股代码：保留4位数字（去掉最左边的一个0）
            if len(code) == 5:
                # 00700 -> 0700.HK (✅ Yahoo Finance 要求)
                # 02318 -> 2318.HK
                # 09988 -> 9988.HK
                hk_code = code[1:]  # 去掉第一个字符（最左边的0）
                return f"{hk_code}.HK"
            elif len(code) == 4:
                # 已经是4位的直接加后缀
                return f"{code}.HK"
            
            # A股代码（6位）
            if code.startswith("6") or code.startswith("5"): 
                return f"{code}.SS"  # 沪市
            if code.startswith("0") or code.startswith("3"): 
                return f"{code}.SZ"  # 深市
        
        return code

def clean_df(df):
    """清洗数据"""
    if df is None or df.empty: return None
    if isinstance(df.columns, pd.MultiIndex):
        try: df.columns = df.columns.get_level_values(0)
        except: pass
    df = df.rename(columns=lambda x: x.capitalize())
    cols_map = {'Date':'Date','Open':'Open','High':'High','Low':'Low','Close':'Close','Volume':'Volume'}
    df = df.rename(columns=cols_map)
    needed = ['Open', 'High', 'Low', 'Close']
    if not all(c in df.columns for c in needed): return None
    for c in needed: df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.dropna()
    if 'Volume' not in df.columns: df['Volume'] = 0
    return df

# 【V82.13新增】Stooq 数据源（美股/指数备用）
def fetch_from_stooq(symbol: str):
    """
    从 Stooq 获取数据（免费、无需 API Key）
    适用于：美股、指数、ETF
    不适用：港股、A股
    """
    try:
        import ssl
        import urllib.request
        
        # Stooq 需要小写，格式：aapl.us
        if symbol.endswith('.HK') or symbol.endswith('.SS') or symbol.endswith('.SZ'):
            return None  # Stooq 不支持港股和A股
        
        # 转换格式：AAPL -> aapl.us
        base_symbol = symbol.replace('.US', '').replace('.', '')
        stooq_symbol = f"{base_symbol.lower()}.us"
        url = f"https://stooq.com/q/d/l/?s={stooq_symbol}&i=d"
        
        # 创建不验证SSL的上下文（开发环境使用）
        ssl_context = ssl._create_unverified_context()
        
        # 使用pandas读取，但需要先验证URL可访问
        try:
            df = pd.read_csv(url, storage_options={'verify': False} if hasattr(pd, '__version__') and int(pd.__version__.split('.')[0]) >= 2 else {})
        except:
            # 如果上面的方法不行，直接用pandas默认方法
            df = pd.read_csv(url)
        
        if df.empty or "Close" not in df.columns:
            return None
        
        df["Date"] = pd.to_datetime(df["Date"])
        df.set_index("Date", inplace=True)
        df = df.sort_index()
        
        return clean_df(df)
    except Exception as e:
        _safe_print(f"[Stooq] ❌ {symbol} 失败: {type(e).__name__}")
        return None

def fetch_cyb_from_eastmoney():
    """
    【V91.7】创业板指399006专用：东方财富fqt=0（指数不复权）
    Yahoo 对 399006 不稳定，东方财富指数需用 fqt=0
    """
    try:
        em_url = "https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=0.399006&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58&klt=101&fqt=0&end=20500101&lmt=252"
        r = requests.get(em_url, timeout=10, verify=False)
        if r.status_code != 200:
            return None
        data = r.json()
        if not data.get('data') or not data['data'].get('klines'):
            return None
        rows = []
        for line in data['data']['klines']:
            parts = line.split(',')
            if len(parts) >= 6:
                rows.append({'Date': parts[0], 'Open': float(parts[1]), 'Close': float(parts[2]), 'High': float(parts[3]), 'Low': float(parts[4]), 'Volume': float(parts[5])})
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        return df
    except Exception:
        return None

def fetch_stock_data(code, return_source=False, return_quality=False):
    """
    【V87.15】数据获取 + 本地文件缓存（5分钟，500MB限制）
    
    数据源优先级：
    1. 本地文件缓存（5分钟内有效）
    2. yfinance（主力）
    3. Stooq（美股/指数备用）
    4. 东方财富（A股备用）
    
    参数：
        code: 股票代码
        return_source: 是否返回数据源信息
        return_quality: 是否返回数据质量元数据
    
    返回：
        - return_quality=True: (df, data_quality_dict)
        - return_source=True: (df, source_str)
        - 默认: df
    """
    # 【V85 Critical Fix】第一行就强制转换代码格式
    target_code = to_yf_cn_code(code)
    
    # 【V87.15】尝试从本地缓存获取
    cache_key = f"stock_data_{target_code}_{return_source}_{return_quality}"
    cached_result = local_cache.get(cache_key)
    if cached_result is not None:
        _safe_print(f"[fetch] ✅ 缓存命中: {code} -> {target_code}")
        return cached_result
    
    _safe_print(f"[fetch] 代码转换: {code} -> {target_code}")
    proxy_url = get_proxy_url()
    data_source = "无数据"
    
    # 【V83 P0.1】数据质量元数据
    data_quality = {
        'source': '无数据',
        'last_updated': None,
        'is_delayed': True,
        'data_points': 0,
        'date_range': None
    }
    
    # ═══ 1️⃣ 主力：yfinance ═══
    if HAS_YFINANCE:
        param_combinations = [
            {"period": "1y", "auto_adjust": False},
            {"period": "2y", "auto_adjust": True},
            {"period": "6mo", "auto_adjust": False},
            {"period": "max", "auto_adjust": False},
        ]
        
        for idx, params in enumerate(param_combinations):
            for retry in range(3):
                try:
                    with ProxyContext(proxy_url):
                        tk = yf.Ticker(target_code)
                        df = tk.history(**params, timeout=15)
                        cleaned = clean_df(df)
                        if cleaned is not None and len(cleaned) > 0:
                            logging.info(f"✅ {target_code} YFinance成功 (参数{idx+1}, 重试{retry+1}/3)")
                            data_source = "yfinance"
                            
                            # 【V83 P0.1】填充数据质量元数据
                            data_quality['source'] = 'Yahoo Finance'
                            data_quality['last_updated'] = pd.Timestamp.now()
                            data_quality['is_delayed'] = True  # Yahoo Finance免费版有15-20分钟延迟
                            data_quality['data_points'] = len(cleaned)
                            data_quality['date_range'] = f"{cleaned.index[0].date()} 至 {cleaned.index[-1].date()}"
                            
                            # 【V87.15】保存到本地缓存
                            if return_quality:
                                result = (cleaned, data_quality)
                            elif return_source:
                                result = (cleaned, data_source)
                            else:
                                result = cleaned
                            
                            local_cache.set(cache_key, result)
                            return result
                except Exception as e:
                    if retry < 2:
                        # 【V87.16】指数退避重试
                        wait_time = 0.5 * (2 ** retry)  # 0.5s, 1s, 2s
                        logging.warning(f"⚠️ {target_code} YFinance失败 (参数{idx+1}, 重试{retry+1}/3): {type(e).__name__}, 等待{wait_time}s")
                        time.sleep(wait_time)
                        continue
                    else:
                        logging.error(f"❌ {target_code} YFinance参数{idx+1}全部失败")
                        break
        
        _safe_print(f"[fetch] ⚠️ {target_code} YFinance全部尝试失败，尝试备用源...")
    
    # ═══ 2️⃣ 备用：Stooq（仅美股/指数）═══
    if not target_code.endswith('.HK') and not target_code.endswith('.SS') and not target_code.endswith('.SZ'):
        _safe_print(f"[fetch] 🔄 {target_code} 尝试Stooq备用源...")
        df_stooq = fetch_from_stooq(target_code)
        if df_stooq is not None and len(df_stooq) > 0:
            _safe_print(f"[fetch] ✅ {target_code} Stooq成功（备用源）")
            data_source = "stooq(备用)"
            
            # 【V83 P0.1】填充数据质量元数据
            data_quality['source'] = 'Stooq (备用)'
            data_quality['last_updated'] = pd.Timestamp.now()
            data_quality['is_delayed'] = True  # Stooq通常T+1延迟
            data_quality['data_points'] = len(df_stooq)
            data_quality['date_range'] = f"{df_stooq.index[0].date()} 至 {df_stooq.index[-1].date()}"
            
            # 【V87.15】保存到本地缓存
            if return_quality:
                result = (df_stooq, data_quality)
            elif return_source:
                result = (df_stooq, data_source)
            else:
                result = df_stooq
            
            local_cache.set(cache_key, result)
            return result
        else:
            _safe_print(f"[fetch] ❌ {target_code} Stooq也失败")
    
    # ═══ 3️⃣ 【V84.2】第三层备用：东方财富（仅A股）═══
    if target_code.endswith('.SS') or target_code.endswith('.SZ'):
        _safe_print(f"[fetch] 🔄 {target_code} 尝试东方财富备用源...")
        try:
            # 东方财富日线接口（简化版，仅获取基础数据）
            # 【V91.7】创业板指399006等指数需用fqt=0（不复权），fqt=1对指数可能返回空
            secid = f"1.{target_code.replace('.SS', '')}" if target_code.endswith('.SS') else f"0.{target_code.replace('.SZ', '')}"
            is_index = target_code in ('399006.SZ', '000300.SS', '000001.SS', '399001.SZ')  # 创业板指、沪深300、上证、深证
            fqt_val = 0 if is_index else 1
            em_url = f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58&klt=101&fqt={fqt_val}&end=20500101&lmt=252"
            
            response = requests.get(em_url, timeout=10, verify=False)
            if response.status_code == 200:
                data = response.json()
                if data.get('data') and data['data'].get('klines'):
                    klines = data['data']['klines']
                    rows = []
                    for line in klines:
                        parts = line.split(',')
                        if len(parts) >= 6:
                            rows.append({
                                'Date': parts[0],
                                'Open': float(parts[1]),
                                'Close': float(parts[2]),
                                'High': float(parts[3]),
                                'Low': float(parts[4]),
                                'Volume': float(parts[5])
                            })
                    
                    if rows:
                        df_em = pd.DataFrame(rows)
                        df_em['Date'] = pd.to_datetime(df_em['Date'])
                        df_em.set_index('Date', inplace=True)
                        
                        _safe_print(f"[fetch] ✅ {target_code} 东方财富成功（备用源）")
                        data_source = "eastmoney(备用)"
                        
                        # 填充数据质量元数据
                        data_quality['source'] = '东方财富 (备用)'
                        data_quality['last_updated'] = pd.Timestamp.now()
                        data_quality['is_delayed'] = True
                        data_quality['data_points'] = len(df_em)
                        data_quality['date_range'] = f"{df_em.index[0].date()} 至 {df_em.index[-1].date()}"
                        
                        # 【V87.15】保存到本地缓存
                        if return_quality:
                            result = (df_em, data_quality)
                        elif return_source:
                            result = (df_em, data_source)
                        else:
                            result = df_em
                        
                        local_cache.set(cache_key, result)
                        return result
        except Exception as e:
            _safe_print(f"[fetch] ❌ {target_code} 东方财富也失败: {type(e).__name__}")
    
    # ═══ 4️⃣ 【V87.8】所有源失败 - 详细错误信息与建议 ═══
    _safe_print(f"[fetch] ❌❌❌ {target_code} 所有数据源失败 ❌❌❌")
    _safe_print(f"[fetch]     原始代码: {code}")
    _safe_print(f"[fetch]     转换后: {target_code}")
    
    # 【V87.8】提供具体建议
    if target_code.endswith('.HK'):
        _safe_print(f"[fetch] 💡 港股建议:")
        _safe_print(f"[fetch]    1) 检查代码格式是否正确（应为5位数字.HK，如00700.HK）")
        _safe_print(f"[fetch]    2) 股票可能已退市或暂停交易")
        _safe_print(f"[fetch]    3) 尝试在Yahoo Finance网站搜索验证")
    elif target_code.endswith(('.SS', '.SZ')):
        _safe_print(f"[fetch] 💡 A股建议:")
        _safe_print(f"[fetch]    1) 检查网络连接和代理设置（端口{st.session_state.get('proxy_port', '1082')}）")
        _safe_print(f"[fetch]    2) 股票可能停牌或退市")
        _safe_print(f"[fetch]    3) 验证代码格式（沪市.SS，深市.SZ）")
    else:
        _safe_print(f"[fetch] 💡 美股建议:")
        _safe_print(f"[fetch]    1) 验证股票代码是否正确")
        _safe_print(f"[fetch]    2) 股票可能已退市（如ATVI被收购）")
        _safe_print(f"[fetch]    3) 尝试在Yahoo Finance搜索: https://finance.yahoo.com/quote/{target_code}")
    
    # 更新错误元数据
    data_quality['source'] = '无数据'
    data_quality['error_detail'] = f'所有数据源均失败（yfinance/stooq/eastmoney）- 可能已退市或代码错误'
    
    if return_quality:
        return (None, data_quality)
    elif return_source:
        return (None, "无数据")
    else:
        return None

# ═══════════════════════════════════════════════════════════════
# 4. 动态股票池 - 从云端API获取（V87 革命性升级）
# ═══════════════════════════════════════════════════════════════
# 【V87.2】使用更可靠的东方财富行情中心API获取股票列表（支持分页，东财 pz 单页最大约200）
# 【安全策略】总量800只（美350+港200+A250），=800安全线
EASTMONEY_PAGE_SIZE = 200

@st.cache_data(ttl=900, show_spinner=False)  # 【V91.3】交易日15分钟缓存
def fetch_eastmoney_stock_list(market="us", limit=350):
    """
    从东方财富行情中心获取股票列表（支持分页）
    
    参数：
        market: "us" (美股) / "hk" (港股) / "cn" (A股)
        limit: 返回数量
    
    返回：
        [(code, name, yf_code), ...]
    """
    try:
        url = "http://80.push2.eastmoney.com/api/qt/clist/get"
        page_size = EASTMONEY_PAGE_SIZE
        all_stocks = []
        pn = 1
        while len(all_stocks) < limit:
            time.sleep(0.6)
            pz = min(page_size, limit - len(all_stocks))
            if market == "us":
                params = {"pn": pn, "pz": pz, "fs": "m:105,m:106,m:107", "fields": "f12,f14,f20", "ut": "bd1d9ddb04089700cf9c27f6f7426281"}
            elif market == "hk":
                params = {"pn": pn, "pz": pz, "fs": "m:128", "fields": "f12,f14,f20", "ut": "bd1d9ddb04089700cf9c27f6f7426281"}
            elif market == "cn":
                params = {"pn": pn, "pz": pz, "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23", "fields": "f12,f14,f20", "ut": "bd1d9ddb04089700cf9c27f6f7426281"}
            else:
                return []
            response = requests.get(url, params=params, timeout=10, verify=False)
            if response.status_code != 200:
                break
            data = response.json()
            if not data.get('data') or not data['data'].get('diff'):
                break
            diff = data['data']['diff']
            diff_list = diff if isinstance(diff, list) else (list(diff.values()) if isinstance(diff, dict) else [])
            page_stocks = []
            for item in diff_list:
                if not isinstance(item, dict):
                    continue
                code = item.get('f12', '')
                name = item.get('f14', '')
                if code and name:
                    yf_code = to_yf_cn_code(code)
                    page_stocks.append((code, name, yf_code))
            if not page_stocks:
                break
            all_stocks.extend(page_stocks)
            if len(page_stocks) < pz:
                break
            pn += 1
            if len(all_stocks) >= limit:
                break
        result = all_stocks[:limit]
        _safe_print(f"[股票池] ✅ {market.upper()}股池获取成功: {len(result)} 只")
        return result
    except Exception as e:
        _safe_print(f"[股票池] ❌ {market.upper()}股API失败: {type(e).__name__}: {str(e)[:100]}")
        return []

# 【V87.2】初始化股票池（带降级方案 + 安全限流）
def init_stock_pools():
    """
    【V87.2 安全策略】扩大至800安全线内
    - 美股: 350只（标普500代表，东财按市值排序）
    - 港股: 200只（恒生指数 + 国企指数）
    - A股: 250只（沪深300 + 创业板代表）
    - 总计: 800只（= 800安全线）✅
    
    如果云端失败则使用本地备用池
    """
    _safe_print("[股票池] 开始初始化（安全模式：总量800只 = 800安全线）...")
    
    # 1. 尝试从云端获取美股（350只）
    us_pool = fetch_eastmoney_stock_list("us", 350)
    if not us_pool or len(us_pool) < 30:
        _safe_print("[股票池] ⚠️ 美股云端获取失败，使用备用池（标普500+纳斯达克100）")
        # 【V87.3】美股备用池扩展到240只 - 必须足够！
        us_pool = [
            # 科技巨头
            ("AAPL", "苹果", "AAPL"), ("MSFT", "微软", "MSFT"), ("GOOGL", "谷歌A", "GOOGL"), ("GOOG", "谷歌C", "GOOG"),
            ("AMZN", "亚马逊", "AMZN"), ("META", "Meta", "META"), ("NVDA", "英伟达", "NVDA"), ("TSLA", "特斯拉", "TSLA"),
            ("NFLX", "奈飞", "NFLX"), ("DIS", "迪士尼", "DIS"),
            # 半导体
            ("TSM", "台积电", "TSM"), ("ASML", "阿斯麦", "ASML"), ("AMD", "超微半导体", "AMD"), ("INTC", "英特尔", "INTC"),
            ("QCOM", "高通", "QCOM"), ("AVGO", "博通", "AVGO"), ("AMAT", "应用材料", "AMAT"), ("LRCX", "泛林集团", "LRCX"),
            ("KLAC", "科磊", "KLAC"), ("MU", "美光科技", "MU"), ("MRVL", "迈威尔", "MRVL"), ("NXPI", "恩智浦", "NXPI"),
            ("TXN", "德州仪器", "TXN"), ("ADI", "亚德诺", "ADI"), ("ON", "安森美", "ON"),
            # 软件与云
            ("CRM", "Salesforce", "CRM"), ("ORCL", "甲骨文", "ORCL"), ("ADBE", "Adobe", "ADBE"), ("NOW", "ServiceNow", "NOW"),
            ("SNOW", "Snowflake", "SNOW"), ("PLTR", "Palantir", "PLTR"), ("DDOG", "Datadog", "DDOG"), ("CRWD", "CrowdStrike", "CRWD"),
            ("ZS", "Zscaler", "ZS"), ("NET", "Cloudflare", "NET"), ("OKTA", "Okta", "OKTA"),
            # 电商与支付
            ("SHOP", "Shopify", "SHOP"), ("SQ", "Block", "SQ"), ("PYPL", "PayPal", "PYPL"), ("MELI", "MercadoLibre", "MELI"),
            # 中概股
            ("BABA", "阿里巴巴", "BABA"), ("BIDU", "百度", "BIDU"), ("JD", "京东", "JD"), ("PDD", "拼多多", "PDD"),
            ("BILI", "哔哩哔哩", "BILI"), ("NIO", "蔚来汽车", "NIO"), ("LI", "理想汽车", "LI"), ("XPEV", "小鹏汽车", "XPEV"),
            ("TME", "腾讯音乐", "TME"), ("NTES", "网易", "NTES"), ("IQ", "爱奇艺", "IQ"),
            # 金融
            ("JPM", "摩根大通", "JPM"), ("BAC", "美国银行", "BAC"), ("WFC", "富国银行", "WFC"), ("C", "花旗集团", "C"),
            ("GS", "高盛", "GS"), ("MS", "摩根士丹利", "MS"), ("BLK", "贝莱德", "BLK"), ("SCHW", "嘉信理财", "SCHW"),
            ("V", "Visa", "V"), ("MA", "万事达", "MA"), ("AXP", "美国运通", "AXP"), ("COF", "第一资本", "COF"),
            # 医疗健康
            ("JNJ", "强生", "JNJ"), ("UNH", "联合健康", "UNH"), ("PFE", "辉瑞", "PFE"), ("ABBV", "艾伯维", "ABBV"),
            ("TMO", "赛默飞世尔", "TMO"), ("ABT", "雅培", "ABT"), ("LLY", "礼来", "LLY"), ("MRK", "默克", "MRK"),
            ("BMY", "百时美施贵宝", "BMY"), ("AMGN", "安进", "AMGN"), ("GILD", "吉利德", "GILD"), ("CVS", "CVS Health", "CVS"),
            # 消费品
            ("PG", "宝洁", "PG"), ("KO", "可口可乐", "KO"), ("PEP", "百事", "PEP"), ("WMT", "沃尔玛", "WMT"),
            ("COST", "好市多", "COST"), ("HD", "家得宝", "HD"), ("LOW", "劳氏", "LOW"), ("TGT", "塔吉特", "TGT"),
            ("NKE", "耐克", "NKE"), ("SBUX", "星巴克", "SBUX"), ("MCD", "麦当劳", "MCD"), ("CMG", "Chipotle", "CMG"),
            ("YUM", "百胜餐饮", "YUM"),
            # 能源
            ("XOM", "埃克森美孚", "XOM"), ("CVX", "雪佛龙", "CVX"), ("COP", "康菲石油", "COP"), ("SLB", "斯伦贝谢", "SLB"),
            # 工业
            ("BA", "波音", "BA"), ("CAT", "卡特彼勒", "CAT"), ("GE", "通用电气", "GE"), ("HON", "霍尼韦尔", "HON"),
            ("UPS", "联合包裹", "UPS"), ("LMT", "洛克希德马丁", "LMT"), ("RTX", "雷神技术", "RTX"),
            # 通信
            ("T", "AT&T", "T"), ("VZ", "Verizon", "VZ"), ("TMUS", "T-Mobile", "TMUS"), ("CMCSA", "康卡斯特", "CMCSA"),
            # 汽车
            ("F", "福特汽车", "F"), ("GM", "通用汽车", "GM"), ("RIVN", "Rivian", "RIVN"), ("LCID", "Lucid", "LCID"),
            # 科技服务
            ("UBER", "Uber", "UBER"), ("LYFT", "Lyft", "LYFT"), ("ABNB", "Airbnb", "ABNB"), ("DASH", "DoorDash", "DASH"),
            ("COIN", "Coinbase", "COIN"), ("RBLX", "Roblox", "RBLX"), ("U", "Unity", "U"), ("ZM", "Zoom", "ZM"),
            ("DOCU", "DocuSign", "DOCU"), ("TWLO", "Twilio", "TWLO"), ("SPOT", "Spotify", "SPOT"),
            # 其他（移除ATVI-已被微软收购退市）
            ("IBM", "IBM", "IBM"), ("HPQ", "惠普", "HPQ"), ("DELL", "戴尔", "DELL"), ("EA", "艺电", "EA"),
            ("TTWO", "Take-Two", "TTWO"), ("TCEHY", "腾讯ADR", "TCEHY"), ("RBLX", "Roblox", "RBLX"), ("U", "Unity", "U"),
            ("MMM", "3M", "MMM"), ("DD", "杜邦", "DD"), ("DOW", "陶氏化学", "DOW"), ("LIN", "林德", "LIN"),
            ("APD", "空气化工", "APD"), ("ECL", "艺康", "ECL"), ("PPG", "PPG工业", "PPG"),
            ("DHR", "丹纳赫", "DHR"), ("ITW", "伊利诺伊", "ITW"), ("EMR", "艾默生", "EMR"),
            ("FDX", "联邦快递", "FDX"), ("DE", "迪尔", "DE"), ("NSC", "诺福克南方", "NSC"),
            ("UNP", "联合太平洋", "UNP"), ("CSX", "CSX运输", "CSX"), ("DAL", "达美航空", "DAL"),
            ("AAL", "美国航空", "AAL"), ("UAL", "联合航空", "UAL"), ("LUV", "西南航空", "LUV"),
            ("MAR", "万豪国际", "MAR"), ("HLT", "希尔顿", "HLT"), ("MGM", "美高梅", "MGM"),
            ("WYNN", "永利度假", "WYNN"), ("LVS", "金沙集团", "LVS"), ("BKNG", "Booking", "BKNG"),
            ("EXPE", "Expedia", "EXPE"), ("TRIP", "TripAdvisor", "TRIP"), ("ABNB", "Airbnb", "ABNB"),
            ("DG", "Dollar General", "DG"), ("DLTR", "Dollar Tree", "DLTR"), ("FIVE", "Five Below", "FIVE"),
            ("ROST", "Ross Stores", "ROST"), ("TJX", "TJX", "TJX"), ("LULU", "Lululemon", "LULU"),
            ("M", "梅西百货", "M"), ("KSS", "科尔士", "KSS"), ("JWN", "诺德斯特龙", "JWN"),
            ("AZO", "AutoZone", "AZO"), ("ORLY", "O'Reilly", "ORLY"), ("AAP", "Advance Auto", "AAP"),
            ("KMX", "CarMax", "KMX"), ("AN", "AutoNation", "AN"),
            # 【V87.4】扩展备用池到240只 - 新增74只
            # 更多科技股（移除重复的ZM）
            ("CRM", "Salesforce", "CRM"), ("ORCL", "甲骨文", "ORCL"), ("ADBE", "Adobe", "ADBE"), ("NOW", "ServiceNow", "NOW"),
            ("SNOW", "Snowflake", "SNOW"), ("PLTR", "Palantir", "PLTR"), ("OKTA", "Okta", "OKTA"), ("SHOP", "Shopify", "SHOP"),
            ("CRWD", "CrowdStrike", "CRWD"), ("ZS", "Zscaler", "ZS"), ("NET", "Cloudflare", "NET"), ("PANW", "Palo Alto", "PANW"),
            ("DDOG", "Datadog", "DDOG"), ("MDB", "MongoDB", "MDB"), ("SPLK", "Splunk", "SPLK"), ("WDAY", "Workday", "WDAY"),
            # 生物医药
            ("MRNA", "Moderna", "MRNA"), ("BNTX", "BioNTech", "BNTX"), ("REGN", "Regeneron", "REGN"), ("VRTX", "Vertex", "VRTX"),
            ("ILMN", "Illumina", "ILMN"), ("BIIB", "Biogen", "BIIB"), ("AMGN", "安进", "AMGN"), ("CELG", "Celgene", "CELG"),
            ("ISRG", "Intuitive", "ISRG"), ("DXCM", "DexCom", "DXCM"), ("ALGN", "Align", "ALGN"), ("IDXX", "IDEXX", "IDXX"),
            # 新能源与清洁技术
            ("ENPH", "Enphase", "ENPH"), ("SEDG", "SolarEdge", "SEDG"), ("FSLR", "First Solar", "FSLR"), ("RUN", "Sunrun", "RUN"),
            ("PLUG", "Plug Power", "PLUG"), ("FCEL", "FuelCell", "FCEL"), ("BE", "Bloom Energy", "BE"), ("SPWR", "SunPower", "SPWR"),
            # 电动车产业链
            ("NIO", "蔚来", "NIO"), ("XPEV", "小鹏汽车", "XPEV"), ("LI", "理想汽车", "LI"), ("LCID", "Lucid Motors", "LCID"),
            ("RIVN", "Rivian", "RIVN"), ("F", "福特汽车", "F"), ("GM", "通用汽车", "GM"), ("STLA", "Stellantis", "STLA"),
            # 金融科技（移除SQ-已被收购）
            ("PYPL", "PayPal", "PYPL"), ("V", "Visa", "V"), ("MA", "万事达", "MA"), ("COIN", "Coinbase", "COIN"),
            ("AXP", "美国运通", "AXP"), ("COF", "Capital One", "COF"), ("DFS", "Discover", "DFS"), ("SYF", "Synchrony", "SYF"),
            # 消费品牌
            ("NKE", "耐克", "NKE"), ("LULU", "Lululemon", "LULU"), ("ULTA", "Ulta Beauty", "ULTA"), ("EL", "雅诗兰黛", "EL"),
            ("PG", "宝洁", "PG"), ("KO", "可口可乐", "KO"), ("PEP", "百事可乐", "PEP"), ("MCD", "麦当劳", "MCD"),
            # 工业与制造
            ("BA", "波音", "BA"), ("LMT", "洛克希德马丁", "LMT"), ("RTX", "雷神技术", "RTX"), ("NOC", "诺斯罗普", "NOC"),
            ("GE", "通用电气", "GE"), ("MMM", "3M", "MMM"), ("HON", "霍尼韦尔", "HON"), ("UNP", "联合太平洋", "UNP"),
            # 房地产投资信托(REITs)
            ("AMT", "American Tower", "AMT"), ("CCI", "Crown Castle", "CCI"), ("EQIX", "Equinix", "EQIX"), ("DLR", "Digital Realty", "DLR"),
        ]
    
    # 2. 尝试从云端获取港股（200只，保持不变）
    hk_pool = fetch_eastmoney_stock_list("hk", 200)
    if not hk_pool or len(hk_pool) < 30:
        _safe_print("[股票池] ⚠️ 港股云端获取失败，使用备用池（恒生+国企+科技）")
        # 【V87.3】港股备用池扩展到200只
        hk_pool = [
            # 互联网科技 (30只)
            ("00700", "腾讯控股", "0700.HK"), ("09988", "阿里巴巴-SW", "9988.HK"), ("03690", "美团-W", "3690.HK"),
            ("01810", "小米集团-W", "1810.HK"), ("09618", "京东集团-SW", "9618.HK"), ("09999", "网易", "9999.HK"),
            ("09626", "哔哩哔哩", "9626.HK"), ("09888", "百度集团", "9888.HK"), ("01024", "快手", "1024.HK"),
            ("06060", "众安在线", "6060.HK"), ("01833", "平安好医生", "1833.HK"), ("06618", "京东健康", "6618.HK"),
            ("09961", "携程集团-S", "9961.HK"), ("09698", "万国数据-SW", "9698.HK"), ("09999", "网易", "9999.HK"),
            ("09896", "名创优品", "9896.HK"), ("02013", "微盟集团", "2013.HK"), ("00268", "金蝶国际", "0268.HK"),
            ("06690", "海尔智家", "6690.HK"), ("02020", "安踏体育", "2020.HK"), ("01347", "华虹半导体", "1347.HK"),
            ("06618", "京东健康", "6618.HK"), ("09933", "知乎-W", "9933.HK"), ("09999", "网易-S", "9999.HK"),
            ("09991", "宝尊电商-SW", "9991.HK"), ("09901", "新东方-S", "9901.HK"), ("09999", "阅文集团", "0772.HK"),
            ("01717", "澳优乳业", "1717.HK"), ("03900", "绿城中国", "3900.HK"), ("00992", "联想集团", "0992.HK"),
            # 新能源汽车 (15只)
            ("01211", "比亚迪", "1211.HK"), ("02015", "理想汽车-W", "2015.HK"), ("09868", "小鹏汽车-W", "9868.HK"),
            ("09866", "蔚来汽车-SW", "9866.HK"), ("00175", "吉利汽车", "0175.HK"), ("02238", "广汽集团", "2238.HK"),
            ("02460", "宁德时代", "2460.HK"), ("01958", "北京汽车", "1958.HK"), ("02333", "长城汽车", "2333.HK"),
            ("00489", "东风集团股份", "0489.HK"), ("01114", "华晨中国", "1114.HK"), ("00177", "江铃汽车", "0177.HK"),
            ("01122", "庆铃汽车股份", "1122.HK"), ("00038", "第一拖拉机股份", "0038.HK"), ("01053", "重庆长安汽车", "1053.HK"),
            # 金融银行 (40只)
            ("02318", "中国平安", "2318.HK"), ("01299", "友邦保险", "1299.HK"), ("03968", "招商银行", "3968.HK"),
            ("03988", "中国银行", "3988.HK"), ("01398", "工商银行", "1398.HK"), ("01288", "农业银行", "1288.HK"),
            ("00939", "建设银行", "0939.HK"), ("03328", "交通银行", "3328.HK"), ("06818", "中国光大银行", "6818.HK"),
            ("01339", "中国人民保险", "1339.HK"), ("02628", "中国人寿", "2628.HK"), ("01336", "新华保险", "1336.HK"),
            ("02601", "中国太保", "2601.HK"), ("01359", "中国信达", "1359.HK"), ("02799", "中国华融", "2799.HK"),
            ("06066", "中信银行", "6066.HK"), ("01988", "民生银行", "1988.HK"), ("03618", "重庆农村商业银行", "3618.HK"),
            ("01658", "邮储银行", "1658.HK"), ("06196", "浙商银行", "6196.HK"), ("02016", "浙江沪杭甬", "2016.HK"),
            ("06886", "华泰证券", "6886.HK"), ("06881", "中国银河", "6881.HK"), ("06098", "碧桂园服务", "6098.HK"),
            ("01579", "颐海国际", "1579.HK"), ("03799", "达利食品", "3799.HK"), ("01610", "中粮家佳康", "1610.HK"),
            ("02319", "蒙牛乳业", "2319.HK"), ("00291", "华润啤酒", "0291.HK"), ("01876", "百威亚太", "1876.HK"),
            ("01928", "金沙中国", "1928.HK"), ("02388", "中银香港", "2388.HK"), ("02356", "大新银行", "2356.HK"),
            ("02888", "渣打集团", "2888.HK"), ("00005", "汇丰控股", "0005.HK"), ("00011", "恒生银行", "0011.HK"),
            ("01109", "华润置地", "1109.HK"), ("01113", "长实集团", "1113.HK"), ("01997", "九龙仓置业", "1997.HK"),
            ("00016", "新鸿基地产", "0016.HK"), ("00017", "新世界发展", "0017.HK"),
            # 能源资源 (25只)
            ("02899", "紫金矿业", "2899.HK"), ("00883", "中国海洋石油", "0883.HK"), ("00386", "中国石油化工", "0386.HK"),
            ("00857", "中国石油股份", "0857.HK"), ("01088", "中国神华", "1088.HK"), ("01898", "中煤能源", "1898.HK"),
            ("01171", "兖煤澳大利亚", "1171.HK"), ("01772", "赣锋锂业", "1772.HK"), ("02601", "中国铝业", "2601.HK"),
            ("01919", "中远海控", "1919.HK"), ("00358", "江西铜业", "0358.HK"), ("02020", "青岛港", "2020.HK"),
            ("01199", "中远海运港口", "1199.HK"), ("01308", "海丰国际", "1308.HK"), ("00144", "招商局港口", "0144.HK"),
            ("03366", "中兴通讯", "3366.HK"), ("00941", "中国移动", "0941.HK"), ("00728", "中国电信", "0728.HK"),
            ("00762", "中国联通", "0762.HK"), ("06993", "蓝月亮集团", "6993.HK"), ("00688", "中国海外发展", "0688.HK"),
            ("02007", "碧桂园", "2007.HK"), ("01668", "中国建筑国际", "1668.HK"), ("03311", "中国建筑", "3311.HK"),
            ("01800", "中国交建", "1800.HK"), ("01766", "中国中车", "1766.HK"),
            # 医药健康 (20只)
            ("01093", "石药集团", "1093.HK"), ("02269", "药明生物", "2269.HK"), ("06185", "康希诺生物", "6185.HK"),
            ("09889", "药明合联", "9889.HK"), ("02359", "药明康德", "2359.HK"), ("01177", "中国生物制药", "1177.HK"),
            ("01099", "国药控股", "1099.HK"), ("03692", "翰森制药", "3692.HK"), ("00874", "广州白云山医药", "0874.HK"),
            ("02186", "绿叶制药", "2186.HK"), ("06821", "凯莱英", "6821.HK"), ("09969", "诺辉健康", "9969.HK"),
            ("01801", "信达生物", "1801.HK"), ("02162", "康方生物", "2162.HK"), ("09995", "荣昌生物", "9995.HK"),
            ("09996", "沛嘉医疗", "9996.HK"), ("01530", "三生制药", "1530.HK"), ("00347", "鞍钢股份", "0347.HK"),
            ("00902", "华能国际电力", "0902.HK"), ("00966", "中国太平", "0966.HK"),
            # 科技硬件 (15只)
            ("00981", "中芯国际", "0981.HK"), ("02382", "舜宇光学科技", "2382.HK"), ("00992", "联想集团", "0992.HK"),
            ("02018", "瑞声科技", "2018.HK"), ("01285", "比亚迪电子", "1285.HK"), ("06098", "华虹半导体", "1347.HK"),
            ("02007", "康龙化成", "3759.HK"), ("00522", "ASM Pacific", "0522.HK"), ("00966", "华润微电子", "1596.HK"),
            ("01478", "丘钛科技", "1478.HK"), ("09988", "高鑫零售", "6808.HK"), ("00027", "银河娱乐", "0027.HK"),
            ("01128", "永利澳门", "1128.HK"), ("00880", "澳博控股", "0880.HK"), ("00200", "新濠国际", "0200.HK"),
            # 公用事业消费 (30只)
            ("00002", "中电控股", "0002.HK"), ("00006", "电能实业", "0006.HK"), ("00003", "香港中华煤气", "0003.HK"),
            ("00001", "长和", "0001.HK"), ("00012", "恒基地产", "0012.HK"), ("00688", "中国海外发展", "0688.HK"),
            ("01044", "恒安国际", "1044.HK"), ("00179", "德昌电机", "0179.HK"), ("00293", "国泰航空", "0293.HK"),
            ("00066", "港铁公司", "0066.HK"), ("00019", "太古股份公司A", "0019.HK"), ("00330", "思捷环球", "0330.HK"),
            ("00551", "裕元集团", "0551.HK"), ("00709", "佐丹奴国际", "0709.HK"), ("00836", "华润电力", "0836.HK"),
            ("01113", "长江基建", "1113.HK"), ("01177", "中粮糖业", "0506.HK"), ("03396", "联想控股", "3396.HK"),
            ("00384", "中国燃气", "0384.HK"), ("00762", "中国联通", "0762.HK"), ("00576", "浙江沪杭甬", "0576.HK"),
            ("00270", "粤海投资", "0270.HK"), ("01072", "东方海外", "0316.HK"), ("00548", "深圳高速公路", "0548.HK"),
            ("00659", "新创建集团", "0659.HK"), ("00882", "天津发展", "0882.HK"), ("00995", "安徽皖通高速", "0995.HK"),
            ("01052", "越秀交通", "1052.HK"),             ("00363", "上海实业控股", "0363.HK"), ("00737", "湾区发展", "0737.HK"),
            # 【V87.4】扩展港股备用池到200只 - 新增16只
            ("01299", "友邦保险", "1299.HK"), ("02628", "中国人寿", "2628.HK"), ("02318", "中国平安", "2318.HK"), ("01336", "新华保险", "1336.HK"),
            ("00857", "中国石油股份", "0857.HK"), ("00386", "中国石油化工", "0386.HK"), ("00883", "中国海洋石油", "0883.HK"), ("01088", "中国神华", "1088.HK"),
            ("00939", "建设银行", "0939.HK"), ("03988", "中国银行", "3988.HK"), ("01398", "工商银行", "1398.HK"), ("00998", "中信银行", "0998.HK"),
            ("01919", "中远海控", "1919.HK"), ("00753", "中国国航", "0753.HK"), ("00670", "中国东方航空", "0670.HK"), ("01055", "中国南方航空", "1055.HK"),
        ]
    
    # 3. 尝试从云端获取A股（250只）
    cn_pool = fetch_eastmoney_stock_list("cn", 250)
    if not cn_pool or len(cn_pool) < 30:
        _safe_print("[股票池] ⚠️ A股云端获取失败，使用备用池（沪深300+创业板）")
        # 【V87.3】A股备用池扩展到240只
        cn_pool = [
            # 白酒食品
            ("600519", "贵州茅台", "600519.SS"), ("000858", "五粮液", "000858.SZ"),
            ("000568", "泸州老窖", "000568.SZ"), ("600809", "山西汾酒", "600809.SS"),
            ("000799", "酒鬼酒", "000799.SZ"), ("600887", "伊利股份", "600887.SS"),
            ("600132", "重庆啤酒", "600132.SS"),
            # 金融银行
            ("601318", "中国平安", "601318.SS"), ("600036", "招商银行", "600036.SS"),
            ("601398", "工商银行", "601398.SS"), ("601288", "农业银行", "601288.SS"),
            ("601988", "中国银行", "601988.SS"), ("601328", "交通银行", "601328.SS"),
            ("600000", "浦发银行", "600000.SS"), ("600016", "民生银行", "600016.SS"),
            ("601166", "兴业银行", "601166.SS"), ("000001", "平安银行", "000001.SZ"),
            ("002142", "宁波银行", "002142.SZ"), ("601169", "北京银行", "601169.SS"),
            # 证券保险
            ("600030", "中信证券", "600030.SS"), ("601688", "华泰证券", "601688.SS"),
            ("600837", "海通证券", "600837.SS"), ("601788", "光大证券", "601788.SS"),
            ("601628", "中国人寿", "601628.SS"), ("601601", "中国太保", "601601.SS"),
            ("601336", "新华保险", "601336.SS"),
            # 新能源汽车
            ("002594", "比亚迪", "002594.SZ"), ("300750", "宁德时代", "300750.SZ"),
            ("300014", "亿纬锂能", "300014.SZ"), ("002812", "恩捷股份", "002812.SZ"),
            ("603799", "华友钴业", "603799.SS"),
            # 新能源光伏
            ("601012", "隆基绿能", "601012.SS"), ("688005", "容百科技", "688005.SS"),
            ("300124", "汇川技术", "300124.SZ"),
            # 半导体芯片
            ("688981", "中芯国际", "688981.SS"), ("002371", "北方华创", "002371.SZ"),
            ("603501", "韦尔股份", "603501.SS"), ("688008", "澜起科技", "688008.SS"),
            # 消费电子
            ("002475", "立讯精密", "002475.SZ"), ("000333", "美的集团", "000333.SZ"),
            ("000651", "格力电器", "000651.SZ"), ("002008", "大族激光", "002008.SZ"),
            ("002049", "紫光国微", "002049.SZ"),
            # 医药医疗
            ("600276", "恒瑞医药", "600276.SS"), ("000661", "长春高新", "000661.SZ"),
            ("300015", "爱尔眼科", "300015.SZ"), ("300760", "迈瑞医疗", "300760.SZ"),
            ("603259", "药明康德", "603259.SS"), ("688111", "金山办公", "688111.SS"),
            # 互联网传媒
            ("300059", "东方财富", "300059.SZ"), ("002230", "科大讯飞", "002230.SZ"),
            ("300033", "同花顺", "300033.SZ"),
            # 房地产建筑
            ("000002", "万科A", "000002.SZ"), ("601668", "中国建筑", "601668.SS"),
            ("601390", "中国中铁", "601390.SS"), ("601186", "中国铁建", "601186.SS"),
            ("601800", "中国交建", "601800.SS"), ("600585", "海螺水泥", "600585.SS"),
            # 能源资源
            ("601899", "紫金矿业", "601899.SS"), ("600028", "中国石化", "600028.SS"),
            ("601857", "中国石油", "601857.SS"), ("600019", "宝钢股份", "600019.SS"),
            ("601088", "中国神华", "601088.SS"), ("600900", "长江电力", "600900.SS"),
            ("601600", "中国铝业", "601600.SS"), ("601919", "中远海控", "601919.SS"),
            # 消费零售
            ("601888", "中国中免", "601888.SS"), ("601933", "永辉超市", "601933.SS"),
            ("603288", "海天味业", "603288.SS"),
            # 交运物流
            ("601018", "宁波港", "601018.SS"), ("600050", "中国联通", "600050.SS"),
            ("601766", "中国中车", "601766.SS"), ("601111", "中国国航", "601111.SS"),
            ("600029", "南方航空", "600029.SS"), ("601006", "大秦铁路", "601006.SS"),
            ("600018", "上港集团", "600018.SS"),
            # 化工材料
            ("600309", "万华化学", "600309.SS"), ("002756", "永兴材料", "002756.SZ"),
            ("600273", "嘉化能源", "600273.SS"),
            # 机械设备
            ("601989", "中国重工", "601989.SS"), ("600704", "物产中大", "600704.SS"),
            # 农林牧渔
            ("002714", "牧原股份", "002714.SZ"), ("000876", "新希望", "000876.SZ"),
            # 公用事业
            ("600015", "华夏银行", "600015.SS"), ("601818", "光大银行", "601818.SS"),
            # 其他宁波
            ("002805", "丰元股份", "002805.SZ"), ("603088", "宁波精达", "603088.SS"),
            ("301019", "宁波色母", "301019.SZ"), ("600366", "宁波韵升", "600366.SS"),
            ("002048", "宁波华翔", "002048.SZ"), ("600857", "宁波中百", "600857.SS"),
            ("600724", "宁波富达", "600724.SS"), ("600768", "宁波富邦", "600768.SS"),
            # 【V87.3】补充到240只 - 更多优质股票
            ("000063", "中兴通讯", "000063.SZ"), ("002352", "顺丰控股", "002352.SZ"),
            ("000725", "京东方A", "000725.SZ"), ("002415", "海康威视", "002415.SZ"),
            ("002241", "歌尔股份", "002241.SZ"), ("002049", "紫光国微", "002049.SZ"),
            ("300124", "汇川技术", "300124.SZ"), ("300124", "汇川技术", "300124.SZ"),
            ("300496", "中科创达", "300496.SZ"), ("300408", "三环集团", "300408.SZ"),
            ("300750", "宁德时代", "300750.SZ"), ("002129", "TCL中环", "002129.SZ"),
            ("002138", "顺络电子", "002138.SZ"), ("002273", "水晶光电", "002273.SZ"),
            ("002384", "东山精密", "002384.SZ"), ("002456", "欧菲光", "002456.SZ"),
            ("002466", "天齐锂业", "002466.SZ"), ("002497", "雅化集团", "002497.SZ"),
            ("002709", "天赐材料", "002709.SZ"), ("002812", "恩捷股份", "002812.SZ"),
            ("002920", "德赛西威", "002920.SZ"), ("300037", "新宙邦", "300037.SZ"),
            ("300122", "智飞生物", "300122.SZ"), ("300142", "沃森生物", "300142.SZ"),
            ("300274", "阳光电源", "300274.SZ"), ("300316", "晶盛机电", "300316.SZ"),
            ("300347", "泰格医药", "300347.SZ"), ("300408", "三环集团", "300408.SZ"),
            ("300433", "蓝思科技", "300433.SZ"), ("300450", "先导智能", "300450.SZ"),
            ("300496", "中科创达", "300496.SZ"), ("300529", "健帆生物", "300529.SZ"),
            ("300558", "贝达药业", "300558.SZ"), ("300595", "欧普康视", "300595.SZ"),
            ("300628", "亿联网络", "300628.SZ"), ("300763", "锦浪科技", "300763.SZ"),
            ("300782", "卓胜微", "300782.SZ"), ("600031", "三一重工", "600031.SS"),
            ("600048", "保利发展", "600048.SS"), ("600061", "国投资本", "600061.SS"),
            ("600089", "特变电工", "600089.SS"), ("600111", "北方稀土", "600111.SS"),
            ("600115", "中国东航", "600115.SS"), ("600188", "兖矿能源", "600188.SS"),
            ("600201", "生物股份", "600201.SS"), ("600298", "安琪酵母", "600298.SS"),
            ("600309", "万华化学", "600309.SS"), ("600325", "华发股份", "600325.SS"),
            ("600362", "江西铜业", "600362.SS"), ("600383", "金地集团", "600383.SS"),
            ("600436", "片仔癀", "600436.SS"), ("600547", "山东黄金", "600547.SS"),
            ("600570", "恒生电子", "600570.SS"), ("600584", "长电科技", "600584.SS"),
            ("600600", "青岛啤酒", "600600.SS"), ("600606", "绿地控股", "600606.SS"),
            ("600611", "大众交通", "600611.SS"), ("600650", "锦江在线", "600650.SS"),
            ("600703", "三安光电", "600703.SS"), ("600717", "天津港", "600717.SS"),
            ("600867", "通化东宝", "600867.SS"), ("600908", "无锡银行", "600908.SS"),
            ("600919", "江苏银行", "600919.SS"), ("600926", "杭州银行", "600926.SS"),
            ("600958", "东方证券", "600958.SS"), ("600999", "招商证券", "600999.SS"),
            ("601009", "南京银行", "601009.SS"), ("601021", "春秋航空", "601021.SS"),
            ("601066", "中信建投", "601066.SS"), ("601128", "常熟银行", "601128.SS"),
            ("601208", "东材科技", "601208.SS"), ("601225", "陕西煤业", "601225.SS"),
            ("601229", "上海银行", "601229.SS"), ("601298", "青岛港", "601298.SS"),
            ("601377", "兴业证券", "601377.SS"), ("601699", "潞安环能", "601699.SS"),
            ("601789", "宁波建工", "601789.SS"), ("601825", "沪农商行", "601825.SS"),
            ("601865", "福莱特", "601865.SS"), ("601872", "招商轮船", "601872.SS"),
            ("601877", "正泰电器", "601877.SS"), ("601878", "浙商证券", "601878.SS"),
            ("601898", "中煤能源", "601898.SS"), ("601916", "浙商银行", "601916.SS"),
            ("601997", "贵阳银行", "601997.SS"), ("603127", "昭衍新药", "603127.SS"),
            ("603160", "汇顶科技", "603160.SS"), ("603233", "大参林", "603233.SS"),
            ("603288", "海天味业", "603288.SS"), ("603369", "今世缘", "603369.SS"),
            ("603392", "万泰生物", "603392.SS"), ("603589", "口子窖", "603589.SS"),
            ("603659", "璞泰来", "603659.SS"), ("603806", "福斯特", "603806.SS"),
            ("603882", "金域医学", "603882.SS"), ("603986", "兆易创新", "603986.SS"),
            ("688005", "容百科技", "688005.SS"), ("688008", "澜起科技", "688008.SS"),
            ("688012", "中微公司", "688012.SS"), ("688018", "乐鑫科技", "688018.SS"),
            ("688032", "禾迈股份", "688032.SS"), ("688111", "金山办公", "688111.SS"),
            ("688123", "聚和材料", "688123.SS"), ("688126", "沪硅产业", "688126.SS"),
            ("688169", "石头科技", "688169.SS"), ("688256", "寒武纪", "688256.SS"),
            ("688303", "大全能源", "688303.SS"), ("688388", "嘉元科技", "688388.SS"),
            ("688390", "固德威", "688390.SS"), ("688396", "华润微", "688396.SS"),
            ("688599", "天合光能", "688599.SS"), ("688981", "中芯国际", "688981.SS"),
            # 【V87.4】扩展A股备用池到240只 - 新增35只
            # 更多银行股
            ("000001", "平安银行", "000001.SZ"), ("002142", "宁波银行", "002142.SZ"), ("600000", "浦发银行", "600000.SS"), ("601166", "兴业银行", "601166.SS"),
            ("000002", "万科A", "000002.SZ"), ("600048", "保利发展", "600048.SS"), ("001979", "招商蛇口", "001979.SZ"), ("600340", "华夏幸福", "600340.SS"),
            # 更多消费股
            ("600887", "伊利股份", "600887.SS"), ("000895", "双汇发展", "000895.SZ"), ("603288", "海天味业", "603288.SS"), ("000568", "泸州老窖", "000568.SZ"),
            ("600809", "山西汾酒", "600809.SS"), ("000596", "古井贡酒", "000596.SZ"), ("603369", "今世缘", "603369.SS"), ("000799", "酒鬼酒", "000799.SZ"),
            # 更多科技股
            ("002415", "海康威视", "002415.SZ"), ("000063", "中兴通讯", "000063.SZ"), ("002236", "大华股份", "002236.SZ"), ("300059", "东方财富", "300059.SZ"),
            ("300750", "宁德时代", "300750.SZ"), ("002460", "赣锋锂业", "002460.SZ"), ("300014", "亿纬锂能", "300014.SZ"), ("002129", "中环股份", "002129.SZ"),
            # 更多制造业
            ("000858", "五粮液", "000858.SZ"), ("600036", "招商银行", "600036.SS"), ("000725", "京东方A", "000725.SZ"), ("002027", "分众传媒", "002027.SZ"),
            ("600031", "三一重工", "600031.SS"), ("000002", "万科A", "000002.SZ"), ("600519", "贵州茅台", "600519.SS"), ("000001", "平安银行", "000001.SZ"),
            # 新能源汽车产业链
            ("002594", "比亚迪", "002594.SZ"), ("300124", "汇川技术", "300124.SZ"), ("002812", "恩捷股份", "002812.SZ"),
            # 【V87.4】最终补充到680只 - 再添加9只
            ("600585", "海螺水泥", "600585.SS"), ("000876", "新希望", "000876.SZ"), ("002304", "洋河股份", "002304.SZ"),
            ("600276", "恒瑞医药", "600276.SS"), ("300015", "爱尔眼科", "300015.SZ"), ("002142", "宁波银行", "002142.SZ"),
            ("600030", "中信证券", "600030.SS"), ("000776", "广发证券", "000776.SZ"), ("600837", "海通证券", "600837.SS"),
        ]
    
    _safe_print(f"[股票池] ✅ 初始化完成: 美股{len(us_pool)}只 | 港股{len(hk_pool)}只 | A股{len(cn_pool)}只 | 总计{len(us_pool)+len(hk_pool)+len(cn_pool)}只")
    
    return us_pool, hk_pool, cn_pool

def validate_stock_pool_health(pool_sample, pool_name, max_test=5):
    """【V87.4】股票池健康检查 - 检测无效股票代码"""
    _safe_print(f"[健康检查] 正在检查{pool_name}股票池...")
    
    invalid_codes = []
    test_count = min(len(pool_sample), max_test)
    
    for i, item in enumerate(pool_sample[:test_count]):
        code = item[2] if len(item) >= 3 else item[0]  # 使用yfinance格式代码
        
        try:
            df = fetch_stock_data(code)
            if df is None or len(df) == 0:
                invalid_codes.append((item, "无数据"))
                _safe_print(f"[健康检查] ❌ {code} ({item[1]}) - 无法获取数据")
            else:
                _safe_print(f"[健康检查] ✅ {code} ({item[1]}) - {len(df)}条数据")
        except Exception as e:
            invalid_codes.append((item, str(e)[:50]))
            _safe_print(f"[健康检查] ❌ {code} ({item[1]}) - 异常: {type(e).__name__}")
    
    if invalid_codes:
        _safe_print(f"[健康检查] ⚠️ {pool_name}发现{len(invalid_codes)}个问题代码，建议更新股票池")
        for item, error in invalid_codes:
            _safe_print(f"  - {item[0]} ({item[1]}): {error}")
    else:
        _safe_print(f"[健康检查] ✅ {pool_name}股票池健康状况良好")
    
    return invalid_codes

# 【V87】加载股票池（会被缓存24小时）
RAW_US, RAW_HK, RAW_CN_TOP = init_stock_pools()

# ── 写股票池缓存（供 scan_worker.py 直接读取，避免二次拉取）─────────────
try:
    _pool_cache_path = _BRIEF_CACHE_DIR / "pool_cache.json"
    _pool_cache_age  = time.time() - json.loads(_pool_cache_path.read_text()).get("ts", 0) \
                       if _pool_cache_path.exists() else 99999
    if _pool_cache_age > 3600:          # 超过 1 小时才刷写
        _BRIEF_CACHE_DIR.mkdir(exist_ok=True)
        _pool_cache_path.write_text(
            json.dumps({"ts": time.time(),
                        "US": RAW_US, "HK": RAW_HK, "CN": RAW_CN_TOP},
                       ensure_ascii=False),
            encoding="utf-8",
        )
except Exception:
    pass

# 【V82.4】轻量级名称索引 - 仅用于关键字搜索，不存储价格数据
STOCK_NAME_INDEX = {
    # ===== 美股热门 =====
    "AAPL": "苹果", "TSLA": "特斯拉", "NVDA": "英伟达", "MSFT": "微软",
    "GOOGL": "谷歌", "GOOG": "谷歌", "AMZN": "亚马逊", "META": "Meta",
    "BABA": "阿里巴巴", "BIDU": "百度", "JD": "京东", "PDD": "拼多多",
    "TSM": "台积电", "ASML": "阿斯麦", "AMD": "超微半导体", "INTC": "英特尔",
    "TME": "腾讯音乐", "NTES": "网易", "LI": "理想汽车", "XPEV": "小鹏汽车",
    "NIO": "蔚来汽车", "BILI": "哔哩哔哩", "IQ": "爱奇艺",
    
    # ===== 港股热门 =====
    # 互联网科技
    "00700.HK": "腾讯控股", "09988.HK": "阿里巴巴", "03690.HK": "美团",
    "01810.HK": "小米集团", "06618.HK": "京东健康", "01024.HK": "快手",
    "09618.HK": "京东集团", "09999.HK": "网易", "09626.HK": "哔哩哔哩",
    "09888.HK": "百度集团", "06060.HK": "众安在线",
    # 新能源汽车
    "02015.HK": "理想汽车", "09868.HK": "小鹏汽车", "09866.HK": "蔚来汽车",
    "00175.HK": "吉利汽车", "02238.HK": "广汽集团", "01211.HK": "比亚迪",
    "02460.HK": "宁德时代",
    # 地产金融
    "02899.HK": "紫金矿业", "03988.HK": "中国银行", "01398.HK": "工商银行",
    "01288.HK": "农业银行", "03968.HK": "招商银行", "02318.HK": "中国平安",
    "01339.HK": "中国人民保险", "00939.HK": "建设银行",
    # 消费
    "01876.HK": "百威亚太", "02319.HK": "蒙牛乳业", "00291.HK": "华润啤酒",
    
    # ===== A股热门 =====
    # 白酒食品
    "600519.SS": "贵州茅台", "000858.SZ": "五粮液", "000568.SZ": "泸州老窖",
    "600809.SS": "山西汾酒", "000799.SZ": "酒鬼酒", "603589.SS": "口子窖",
    "600887.SS": "伊利股份", "600132.SS": "重庆啤酒",
    # 金融
    "601318.SS": "中国平安", "600036.SS": "招商银行", "601398.SS": "工商银行",
    "601288.SS": "农业银行", "601988.SS": "中国银行", "601328.SS": "交通银行",
    "600000.SS": "浦发银行", "600016.SS": "民生银行", "601166.SS": "兴业银行",
    "000001.SZ": "平安银行", "002142.SZ": "宁波银行",
    "601628.SS": "中国人寿", "601601.SS": "中国太保", "601336.SS": "新华保险",
    "600030.SS": "中信证券", "600837.SS": "海通证券", "601788.SS": "光大证券",
    # 新能源
    "002594.SZ": "比亚迪", "300750.SZ": "宁德时代", "601012.SS": "隆基绿能",
    "688005.SS": "容百科技", "688981.SS": "中芯国际", "300014.SZ": "亿纬锂能",
    # 消费电子
    "002475.SZ": "立讯精密", "000333.SZ": "美的集团", "000651.SZ": "格力电器",
    "002008.SZ": "大族激光",
    # 医药
    "600276.SS": "恒瑞医药", "000661.SZ": "长春高新", "300015.SZ": "爱尔眼科",
    "300760.SZ": "迈瑞医疗", "603259.SS": "药明康德",
    # 地产基建
    "000002.SZ": "万科A", "601668.SS": "中国建筑", "601390.SS": "中国中铁",
    "601186.SS": "中国铁建", "601800.SS": "中国交建",
    # 能源资源
    "601899.SS": "紫金矿业", "600028.SS": "中国石化", "601857.SS": "中国石油",
    "600019.SS": "宝钢股份", "601088.SS": "中国神华", "600900.SS": "长江电力",
    "601600.SS": "中国铝业", "601919.SS": "中远海控",
    # 其他
    "601888.SS": "中国中免", "600050.SS": "中国联通", "601766.SS": "中国中车",
    "601111.SS": "中国国航", "600029.SS": "南方航空", "601006.SS": "大秦铁路",
    "601989.SS": "中国重工", "601818.SS": "光大银行", "600585.SS": "海螺水泥",
    "600018.SS": "上港集团", "600015.SS": "华夏银行",
    
    # ===== 宁波相关（完整版）=====
    "601018.SS": "宁波港", "002142.SZ": "宁波银行", "600366.SS": "宁波韵升",
    "002048.SZ": "宁波华翔", "603088.SS": "宁波精达", "301019.SZ": "宁波色母",
    "600857.SS": "宁波中百", "600724.SS": "宁波富达", "600768.SS": "宁波富邦",
    "600051.SS": "宁波联合", "002667.SZ": "宁波建工", "600452.SS": "涪陵电力",
    "002574.SZ": "明牌珠宝", "600884.SS": "杉杉股份", "002805.SZ": "丰元股份",
    "002756.SZ": "永兴材料", "603799.SS": "华友钴业", "600273.SS": "嘉化能源",
    "601777.SS": "力帆科技", "600704.SS": "物产中大", "600687.SS": "刚泰控股",
    "002098.SZ": "浔兴股份", "002098.SZ": "浔兴股份",
}

# ═══════════════════════════════════════════════════════════════
# 5. K线形态识别（15种）
# ═══════════════════════════════════════════════════════════════
def identify_kline_pattern(row, prev_row):
    """K线形态识别（保留完整15种）"""
    close, open_p, high, low = row['Close'], row['Open'], row['High'], row['Low']
    body = abs(close - open_p)
    total_range = high - low
    if total_range == 0: return "🛑 一字板"
    
    # 十字星家族
    if body <= total_range * 0.15:
        if (high - max(open_p, close)) > total_range * 0.4 and (min(open_p, close) - low) > total_range * 0.4: 
            return "🦵 长腿十字星 (变盘信号)"
        if (high - max(open_p, close)) > total_range * 0.6: 
            return "🪦 墓碑十字线 (顶部反转)"
        if (min(open_p, close) - low) > total_range * 0.6: 
            return "🐉 蜻蜓十字线 (底部反转)"
        return "⚖️ 十字星 (多空平衡)"
    
    upper_shadow = high - max(open_p, close)
    lower_shadow = min(open_p, close) - low
    
    # 锤头线家族
    if lower_shadow >= 2 * body and upper_shadow <= body * 0.3:
        if close > prev_row['Close']: return "🔨 锤头线 (底部看涨)"
        else: return "🪢 吊颈线 (顶部看跌)"
    
    # 倒锤头/射击之星
    if upper_shadow >= 2 * body and lower_shadow <= body * 0.3:
        if close > open_p: return "🛡️ 倒锤头 (底部信号)"
        else: return "🗡️ 射击之星 (顶部信号)"
    
    # 大阳/大阴线
    if body >= total_range * 0.8:
        if close > open_p: return "🔥 大阳线 (强烈看多)"
        else: return "❄️ 大阴线 (强烈看空)"
    
    # 中阳/中阴线
    if body >= total_range * 0.6:
        if close > open_p: return "📈 中阳线 (温和上涨)"
        else: return "📉 中阴线 (温和下跌)"
    
    # 小阳/小阴整理
    if close > open_p:
        return "➚ 小阳推进" if close > prev_row['Close'] else "➿ 小阳整理"
    else:
        return "➘ 小阴下探" if close < prev_row['Close'] else "➿ 小阴整理"

# ═══════════════════════════════════════════════════════════════
# 6. Alpha Matrix Agent（机构决策引擎）
# ═══════════════════════════════════════════════════════════════
class AlphaMatrixAgent:
    def decide(self, df):
        if df is None or len(df) < 20: return ("观察", "数据不足", 0.3, [])
        
        df = df.copy()
        for n in [5, 10, 20, 60, 120]: df[f'MA{n}'] = df['Close'].rolling(n).mean()
        
        last = df.iloc[-1]
        score = 0
        reasons = []
        tags = []
        
        # 趋势判断
        if last['Close'] > last.get('MA60', 0):
            score += 30
            reasons.append("站上季线")
            tags.append("趋势向上")
        
        # 动能判断
        if len(df) > 5:
            ret_5d = (last['Close'] - df['Close'].iloc[-6]) / df['Close'].iloc[-6]
            if ret_5d > 0.03:
                score += 20
                reasons.append("5日涨幅>3%")
                tags.append("动能强劲")
        
        # 量价配合
        if len(df) > 5:
            vol_ma5 = df['Volume'].tail(5).mean()
            if last['Volume'] > vol_ma5 * 1.2:
                score += 15
                reasons.append("放量")
                tags.append("资金活跃")
        
        conf = min(0.95, score / 100)
        
        if conf > 0.7: action = "买入"
        elif conf > 0.5: action = "持有"
        else: action = "观察"
        
        return action, " | ".join(reasons) if reasons else "震荡", conf, tags

rl_agent = AlphaMatrixAgent()

# ═══════════════════════════════════════════════════════════════
# 6.5 Gemini AI API 调用函数（提前定义，供后续所有模块使用）
# ═══════════════════════════════════════════════════════════════
def call_gemini_api(prompt, model_name=None):
    """
    调用Gemini API进行AI分析
    
    参数:
        prompt: 提示词
        model_name: 模型名称（可选，默认使用GEMINI_MODEL_NAME）
    
    返回:
        AI生成的文本响应
    """
    if not MY_GEMINI_KEY:
        return "❌ 请配置 Gemini API Key"
    
    try:
        # 使用代理上下文
        with ProxyContext(get_proxy_url()):
            # 确定使用的模型
            current_model = model_name if model_name else GEMINI_MODEL_NAME
            
            # 构建API请求
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{current_model}:generateContent?key={MY_GEMINI_KEY}"
            headers = {"Content-Type": "application/json"}
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": 8192}
            }
            
            # 发送请求
            r = requests.post(url, headers=headers, json=payload, timeout=60)
            
            if r.status_code == 200:
                return r.json()["candidates"][0]["content"]["parts"][0]["text"]
            else:
                return f"❌ Gemini API失败: {r.status_code} - {r.text}"
    except Exception as e:
        logging.error(f"❌ Gemini API调用异常: {str(e)}")
        return f"❌ Gemini API网络错误: {str(e)}"


def call_gemini_api_stream(prompt, model_name=None):
    """流式调用 Gemini，返回文字块生成器，用于 st.write_stream()"""
    if not MY_GEMINI_KEY or not HAS_GEMINI:
        yield "❌ 请配置 Gemini API Key"
        return
    try:
        m = model_name or GEMINI_MODEL_NAME
        model = genai.GenerativeModel(m)
        gen_cfg = genai.types.GenerationConfig(max_output_tokens=8192)
        response = model.generate_content(prompt, stream=True, generation_config=gen_cfg)
        for chunk in response:
            if chunk.text:
                yield chunk.text
    except Exception as e:
        yield f"❌ AI分析失败: {str(e)}"


# ═══════════════════════════════════════════════════════════════
# 7. CANSLIM + 专业投机原理（完整双核评级）
# ═══════════════════════════════════════════════════════════════
def calculate_metrics_all(df, code):
    """
    【V87.16】完整的双核评级系统 - 增强防御性检查
    即使数据不足,也尽量计算能计算的指标
    """
    # 【V87.16】严格的防御性检查
    if df is None:
        logging.warning(f"⚠️ {code} DataFrame为None")
        return None
    
    if df.empty:
        logging.warning(f"⚠️ {code} DataFrame为空")
        return None
    
    if len(df) < 5:
        logging.warning(f"⚠️ {code} 数据不足5行: {len(df)}")
        return None
    
    if 'Close' not in df.columns:
        logging.error(f"❌ {code} 缺少Close列")
        return None
    
    try:
        df = df.apply(pd.to_numeric, errors='coerce').dropna().sort_index()
        
        if df.empty or len(df) < 5:
            logging.warning(f"⚠️ {code} 清洗后数据不足")
            return None
    
    except Exception as e:
        logging.error(f"❌ {code} 数据清洗失败: {type(e).__name__}")
        return None
    
    # 【V85】计算所有均线,即使数据不足120天也不返回None
    # 只计算数据量允许的均线
    for n in [5, 10, 20, 50, 60, 120, 150, 200, 250]:
        if len(df) >= n:
            df[f'MA{n}'] = df['Close'].rolling(n).mean()
        else:
            # 数据不足时,使用全部数据计算均线
            df[f'MA{n}'] = df['Close'].rolling(min(n, len(df))).mean()
    
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)
    rs = gain.ewm(com=13).mean() / loss.ewm(com=13).mean()
    df['RSI'] = 100 - (100 / (1 + rs))
    df['RSI'] = df['RSI'].fillna(50)
    
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else last
    
    # CANSLIM 7因子
    score_c = 0
    canslim_rows = []
    
    state_c = last['Close'] > last.get('MA50', 0)
    canslim_rows.append({"因子": "C: 当季收益", "状态": "✅" if state_c else "❌", "说明": "股价>MA50"})
    if state_c: score_c += 15
    
    state_a = last['Close'] > last.get('MA200', 0)
    canslim_rows.append({"因子": "A: 年度收益", "状态": "✅" if state_a else "❌", "说明": "股价>年线"})
    if state_a: score_c += 15
    
    l250 = df['Low'].tail(250).min() if len(df) >= 250 else df['Low'].min()
    h250 = df['High'].tail(250).max() if len(df) >= 250 else df['High'].max()
    dist_h = (last['Close'] - h250) / h250 * 100 if h250 > 0 else -100
    state_n = abs(dist_h) < 15
    canslim_rows.append({"因子": "N: 新高附近", "状态": "✅" if state_n else "❌", "说明": f"距前高{abs(dist_h):.1f}%"})
    if state_n: score_c += 15
    
    vol_ma5 = df['Volume'].tail(5).mean() if len(df) >= 5 else df['Volume'].mean()
    price_up = last['Close'] > prev['Close']
    state_s = (last['Volume'] > vol_ma5) and price_up
    canslim_rows.append({"因子": "S: 供需", "状态": "✅" if state_s else "❌", "说明": "放量上涨"})
    if state_s: score_c += 15
    
    state_l = last['RSI'] > 55
    if last['RSI'] > 85:
        score_c -= 10
        canslim_rows.append({"因子": "L: 领头羊", "状态": "⚠️ 过热", "说明": f"RSI={last['RSI']:.1f}"})
    else:
        canslim_rows.append({"因子": "L: 领头羊", "状态": "✅" if state_l else "❌", "说明": f"RSI={last['RSI']:.1f}"})
        if state_l: score_c += 10
    
    ma50 = df.get('MA50')
    s_i = False
    if ma50 is not None and len(ma50) > 5:
        s_i = ma50.iloc[-1] > ma50.iloc[-5]
    canslim_rows.append({"因子": "I: 机构持仓", "状态": "✅" if s_i else "❌", "说明": "MA50向上"})
    if s_i: score_c += 15
    
    state_m = last['Close'] > last.get('MA20', 0)
    canslim_rows.append({"因子": "M: 市场方向", "状态": "✅" if state_m else "❌", "说明": "站上月线"})
    if state_m: score_c += 15
    
    # 专业投机原理 7指标
    score_s = 0
    spec_rows = []
    
    t1 = last['Close'] > last.get('MA200', 0)
    spec_rows.append({"因子":"1. 长期趋势", "状态":"✅" if t1 else "❌", "说明":"当前>年线"})
    if t1: score_s += 10
    
    t2 = last['Close'] > last.get('MA50', 0)
    spec_rows.append({"因子":"2. 中期趋势", "状态":"✅" if t2 else "❌", "说明":"当前>生命线"})
    if t2: score_s += 10
    
    t3 = last['Close'] > last.get('MA20', 0)
    spec_rows.append({"因子":"3. 短期动能", "状态":"✅" if t3 else "❌", "说明":"当前>月线"})
    if t3: score_s += 10
    
    t4 = last['RSI'] > 50
    spec_rows.append({"因子":"4. 相对强度", "状态":"✅" if t4 else "❌", "说明":"RSI>50"})
    if t4: score_s += 10
    
    dev = abs(last['Close'] - last.get('MA20', last['Close'])) / last.get('MA20', 1) if last.get('MA20', 1) > 0 else 0
    if dev > 0.15:
        score_s -= 10
        spec_rows.append({"因子":"5. 波动乖离", "状态":"⚠️ 偏离", "说明": f"乖离{dev*100:.1f}%"})
    else:
        spec_rows.append({"因子":"5. 波动乖离", "状态":"✅ 正常", "说明": f"乖离{dev*100:.1f}%"})
        score_s += 10
    
    t6 = (last['Volume'] > vol_ma5) and price_up
    spec_rows.append({"因子":"6. 量价配合", "状态":"✅" if t6 else "❌", "说明":"放量上涨"})
    if t6: score_s += 20
    
    pos = (last['Close'] - l250) / (h250 - l250 + 0.001) if (h250 - l250) > 0 else 0.5
    t7 = pos > 0.8
    spec_rows.append({"因子":"7. 价格位置", "状态":"✅" if t7 else "❌", "说明":f"位于区间{pos*100:.0f}%处"})
    if t7: score_s += 30
    
    # ═══════════════════════════════════════════════════════════
    # 【V89.7 新增】ESG评分（基于技术面代理指标 + 行业特征）
    # E=环境 S=社会 G=治理，每项0-100分
    # ═══════════════════════════════════════════════════════════
    esg_rows = []
    esg_e_score = 50  # 环境基准50分
    esg_s_score = 50  # 社会基准50分
    esg_g_score = 50  # 治理基准50分
    
    # E-环境：用波动率稳定性代理（低波动=经营稳定=环境风险低）
    if len(df) >= 60:
        _vol_60 = df['Close'].pct_change().tail(60).std() * np.sqrt(252) * 100
        if _vol_60 < 20:
            esg_e_score = 75
            esg_rows.append({"维度": "🌿 E-环境", "评分": f"{esg_e_score}/100", "依据": f"年化波动率{_vol_60:.1f}%，经营稳定", "等级": "✅ 良好"})
        elif _vol_60 < 35:
            esg_e_score = 55
            esg_rows.append({"维度": "🌿 E-环境", "评分": f"{esg_e_score}/100", "依据": f"年化波动率{_vol_60:.1f}%，正常范围", "等级": "🟡 中等"})
        else:
            esg_e_score = 30
            esg_rows.append({"维度": "🌿 E-环境", "评分": f"{esg_e_score}/100", "依据": f"年化波动率{_vol_60:.1f}%，高波动风险", "等级": "❌ 较差"})
    else:
        esg_rows.append({"维度": "🌿 E-环境", "评分": f"{esg_e_score}/100", "依据": "数据不足，使用默认值", "等级": "🟡 中等"})
    
    # S-社会：用成交活跃度代理（高流动性=市场认可=社会关注度高）
    if len(df) >= 20:
        _avg_vol = df['Volume'].tail(20).mean()
        _vol_trend = df['Volume'].tail(5).mean() / _avg_vol if _avg_vol > 0 else 1
        if _vol_trend > 1.3 and price_up:
            esg_s_score = 80
            esg_rows.append({"维度": "👥 S-社会", "评分": f"{esg_s_score}/100", "依据": f"量比{_vol_trend:.2f}，资金积极流入", "等级": "✅ 良好"})
        elif _vol_trend > 0.8:
            esg_s_score = 60
            esg_rows.append({"维度": "👥 S-社会", "评分": f"{esg_s_score}/100", "依据": f"量比{_vol_trend:.2f}，市场关注度正常", "等级": "🟡 中等"})
        else:
            esg_s_score = 35
            esg_rows.append({"维度": "👥 S-社会", "评分": f"{esg_s_score}/100", "依据": f"量比{_vol_trend:.2f}，流动性不足", "等级": "❌ 较差"})
    else:
        esg_rows.append({"维度": "👥 S-社会", "评分": f"{esg_s_score}/100", "依据": "数据不足，使用默认值", "等级": "🟡 中等"})
    
    # G-治理：用价格趋势一致性代理（均线多头排列=管理层执行力强）
    _ma_aligned = 0
    if last['Close'] > last.get('MA20', 0): _ma_aligned += 1
    if last['Close'] > last.get('MA50', 0): _ma_aligned += 1
    if last['Close'] > last.get('MA120', 0): _ma_aligned += 1
    if last['Close'] > last.get('MA200', 0): _ma_aligned += 1
    if last.get('MA50', 0) > last.get('MA200', 0): _ma_aligned += 1
    
    if _ma_aligned >= 4:
        esg_g_score = 85
        esg_rows.append({"维度": "🏛️ G-治理", "评分": f"{esg_g_score}/100", "依据": f"均线{_ma_aligned}/5多头排列，趋势健康", "等级": "✅ 优秀"})
    elif _ma_aligned >= 2:
        esg_g_score = 55
        esg_rows.append({"维度": "🏛️ G-治理", "评分": f"{esg_g_score}/100", "依据": f"均线{_ma_aligned}/5多头，趋势分化", "等级": "🟡 中等"})
    else:
        esg_g_score = 25
        esg_rows.append({"维度": "🏛️ G-治理", "评分": f"{esg_g_score}/100", "依据": f"均线{_ma_aligned}/5多头，趋势恶化", "等级": "❌ 较差"})
    
    # ESG综合分
    esg_total = int(esg_e_score * 0.3 + esg_s_score * 0.3 + esg_g_score * 0.4)
    
    # ESG等级
    if esg_total >= 75:
        esg_grade = "AAA"
        esg_label = "🟢 ESG领先"
    elif esg_total >= 60:
        esg_grade = "AA"
        esg_label = "🟢 ESG良好"
    elif esg_total >= 45:
        esg_grade = "A"
        esg_label = "🟡 ESG中等"
    elif esg_total >= 30:
        esg_grade = "BB"
        esg_label = "🟠 ESG偏弱"
    else:
        esg_grade = "B"
        esg_label = "🔴 ESG较差"
    
    esg_rows.append({"维度": "📊 ESG综合", "评分": f"{esg_total}/100", "依据": f"E×30%+S×30%+G×40%", "等级": f"{esg_label} ({esg_grade})"})
    
    # ═══════════════════════════════════════════════════════════
    # 【V89.7】四维综合评分 = CANSLIM×30% + 专业投机×30% + ESG×20% + 风控×20%
    # ═══════════════════════════════════════════════════════════
    # 风控评分：基于RSI合理性 + 乖离率 + 价格位置
    _risk_control_score = 50
    if 30 < last['RSI'] < 70: _risk_control_score += 20  # RSI适中
    if abs(dev) < 0.05: _risk_control_score += 15  # 乖离小
    if 0.3 < pos < 0.85: _risk_control_score += 15  # 价格位置合理
    _risk_control_score = min(100, _risk_control_score)
    
    final_score = int(score_c * 0.30 + score_s * 0.30 + esg_total * 0.20 + _risk_control_score * 0.20)
    final_score = min(99, max(0, final_score))
    
    # 【V91.0】策略文案差异化，结合RSI/趋势
    rsi_val = last.get('RSI', 50)
    above_ma20 = last['Close'] > last.get('MA20', 0) if last.get('MA20', 0) > 0 else False
    if final_score > 85:
        logic = f"🔥 强力进攻" + ("，均线多头趋势明确" if above_ma20 else "，等待确认突破")
    elif final_score > 60:
        logic = f"🛡️ 稳健持有" + (f"，RSI{rsi_val:.0f}适中" if 40 < rsi_val < 70 else "")
    else:
        logic = "❄️ 弱势回避" + (f"，RSI{rsi_val:.0f}偏离" if rsi_val > 70 or rsi_val < 30 else "")
    
    suggestion = "仅观察"
    if final_score >= 90: suggestion = "积极抢筹"
    elif final_score >= 75: suggestion = "分批建仓"
    elif final_score >= 60: suggestion = "等待确认"
    
    # K线形态识别
    pattern = identify_kline_pattern(last, prev)
    
    ma20 = last.get('MA20', last['Close'])
    bias = (last['Close'] - ma20) / ma20 * 100 if ma20 > 0 else 0
    df['TR'] = np.maximum((df['High'] - df['Low']), np.maximum(abs(df['High'] - df['Close'].shift(1)), abs(df['Low'] - df['Close'].shift(1))))
    atr = df['TR'].rolling(14).mean().iloc[-1] if len(df) >= 14 else 0
    vwap = (df['Close'] * df['Volume']).sum() / df['Volume'].sum() if df['Volume'].sum() > 0 else last['Close']
    
    # 使用Alpha Agent
    action, reason, conf, tags = rl_agent.decide(df)
    kelly = (conf * 2.0 - 1) / 2.0 if conf > 0.5 else 0.0
    kelly = max(0, kelly)
    
    # 【V83 P1.4】机构式交易计划
    trade_plan = calculate_trade_plan(df, code)
    
    # 【V91.0】实战修正：高分股需合理风险收益比，策略文案差异化（避免千篇一律）
    if trade_plan and final_score >= 75:
        risk_pct = trade_plan['risk_per_share'] / last['Close'] * 100 if last['Close'] > 0 else 0
        risk_reward = trade_plan.get('risk_reward', 0)
        rsi_val = last.get('RSI', 50)
        above_ma = last['Close'] > last.get('MA20', 0) if last.get('MA20', 0) > 0 else False
        
        if risk_pct > 20:
            final_score = min(final_score, 74)
            # 差异化文案：结合RSI/趋势
            if rsi_val > 65:
                logic = f"RSI偏高({rsi_val:.0f})，止损{risk_pct:.1f}%过宽，等回调再考虑"
            elif not above_ma:
                logic = f"当前弱于MA20，止损{risk_pct:.1f}%偏大，观望为主"
            else:
                logic = f"趋势向好但止损{risk_pct:.1f}%过宽，建议小仓位试探或等回调"
            suggestion = "观望"
        elif risk_reward < 1.2:
            final_score = min(final_score, 74)
            if risk_reward < 0.8:
                logic = f"盈亏比{risk_reward:.2f}:1严重不足，性价比差，暂不介入"
            elif above_ma:
                logic = f"价格高于MA20，趋势尚可，但盈亏比{risk_reward:.2f}:1偏低，可等更好买点"
            else:
                logic = f"盈亏比{risk_reward:.2f}:1不足，建议等待回调或放量突破"
            suggestion = "观望"
        elif risk_pct > 15:
            logic += f"；止损{risk_pct:.1f}%略宽，建议控制仓位"
    
    return {
        "score": final_score, "logic": logic, "suggestion": suggestion,
        "action": action, "reason": reason, "tags": tags, "kelly": kelly,
        "canslim_rows": canslim_rows, "spec_rows": spec_rows,
        "pattern": pattern, "rsi": last['RSI'], "bias": bias, "atr": atr, "vwap": vwap,
        "last_price": last['Close'], "ma20": ma20, "df": df, "last": last,
        "trade_plan": trade_plan,  # 【V83 P1】新增交易计划
        # 【V89.7】ESG评级数据
        "esg_rows": esg_rows,
        "esg_total": esg_total,
        "esg_grade": esg_grade,
        "esg_label": esg_label,
        "esg_e": esg_e_score,
        "esg_s": esg_s_score,
        "esg_g": esg_g_score,
    }

def calculate_advanced_quant(df):
    """
    【V87.16】量化回测指标 + 高级技术指标
    新增：MACD、Bollinger Bands
    """
    if df is None or len(df) < 20: 
        return {}
    
    # 防御性检查
    if 'Close' not in df.columns:
        logging.error("❌ DataFrame缺少Close列")
        return {}
    
    try:
        # 基础回测指标
        ret = df['Close'].pct_change().dropna()
        rf = 0.03 / 252
        sharpe = (ret.mean() - rf) / ret.std() * np.sqrt(252) if ret.std() > 0 else 0
        cum = (1 + ret).cumprod()
        max_dd = (cum.cummax() - cum).max()
        wins = len(ret[ret > 0])
        win_rate = wins / len(ret) if len(ret) > 0 else 0
        avg_win = ret[ret > 0].mean() if len(ret[ret > 0]) > 0 else 0
        avg_loss = abs(ret[ret < 0].mean()) if len(ret[ret < 0]) > 0 else 1
        pl_ratio = avg_win / avg_loss if avg_loss > 0 else 0
        volatility = ret.std() * np.sqrt(252) if ret.std() > 0 else 0
        
        # 【V87.16】MACD指标 (Fast=12, Slow=26, Signal=9)
        macd_data = {}
        if len(df) >= 26:
            exp1 = df['Close'].ewm(span=12, adjust=False).mean()
            exp2 = df['Close'].ewm(span=26, adjust=False).mean()
            macd = exp1 - exp2
            signal = macd.ewm(span=9, adjust=False).mean()
            histogram = macd - signal
            
            # 判断金叉/死叉
            if len(macd) >= 2:
                prev_diff = macd.iloc[-2] - signal.iloc[-2]
                curr_diff = macd.iloc[-1] - signal.iloc[-1]
                
                if prev_diff < 0 and curr_diff > 0:
                    macd_signal = "🟢 金叉 (看涨)"
                elif prev_diff > 0 and curr_diff < 0:
                    macd_signal = "🔴 死叉 (看跌)"
                elif curr_diff > 0:
                    macd_signal = "🟢 多头 (MACD>Signal)"
                else:
                    macd_signal = "🔴 空头 (MACD<Signal)"
            else:
                macd_signal = "N/A"
            
            macd_data = {
                'macd': f"{macd.iloc[-1]:.2f}",
                'signal': f"{signal.iloc[-1]:.2f}",
                'histogram': f"{histogram.iloc[-1]:.2f}",
                'macd_signal': macd_signal
            }
        
        # 【V87.16】Bollinger Bands (Window=20, Std=2)
        bb_data = {}
        if len(df) >= 20:
            sma20 = df['Close'].rolling(window=20).mean()
            std20 = df['Close'].rolling(window=20).std()
            upper_band = sma20 + (std20 * 2)
            lower_band = sma20 - (std20 * 2)
            
            current_price = df['Close'].iloc[-1]
            bb_width = ((upper_band.iloc[-1] - lower_band.iloc[-1]) / sma20.iloc[-1] * 100) if sma20.iloc[-1] > 0 else 0
            
            # 判断位置
            if current_price > upper_band.iloc[-1]:
                bb_position = "🔴 超买 (价格>上轨)"
            elif current_price < lower_band.iloc[-1]:
                bb_position = "🟢 超卖 (价格<下轨)"
            elif current_price > sma20.iloc[-1]:
                bb_position = "🟡 偏强 (价格>中轨)"
            else:
                bb_position = "🟡 偏弱 (价格<中轨)"
            
            bb_data = {
                'bb_upper': f"{upper_band.iloc[-1]:.2f}",
                'bb_middle': f"{sma20.iloc[-1]:.2f}",
                'bb_lower': f"{lower_band.iloc[-1]:.2f}",
                'bb_width': f"{bb_width:.2f}%",
                'bb_position': bb_position
            }
        
        return {
            "sharpe": f"{sharpe:.2f}",
            "max_dd": f"{max_dd*100:.2f}%",
            "volatility": f"{volatility*100:.1f}%",
            "win_rate": f"{win_rate*100:.1f}%",
            "pl_ratio": f"{pl_ratio:.2f}",
            **macd_data,
            **bb_data
        }
    
    except Exception as e:
        logging.error(f"❌ calculate_advanced_quant失败: {type(e).__name__}: {str(e)}")
        return {}

def monte_carlo_forecast(df, days=10, sims=1000):
    """蒙特卡洛预测"""
    try:
        last_p = df['Close'].iloc[-1]
        ret = df['Close'].pct_change().dropna()
        mu = ret.mean()
        sigma = ret.std()
        final_prices = []
        for _ in range(sims):
            price = last_p * np.exp((mu - 0.5 * sigma**2) * days + sigma * np.sqrt(days) * np.random.normal(0, 1))
            final_prices.append(price)
        p90 = np.percentile(final_prices, 90)
        p50 = np.percentile(final_prices, 50)
        p10 = np.percentile(final_prices, 10)
        return {"p90": p90, "p50": p50, "p10": p10}
    except:
        return None

# ═══════════════════════════════════════════════════════════════
# 【V83 P0.2】基准对比与风险指标
# ═══════════════════════════════════════════════════════════════
def get_benchmark_code(stock_code):
    """根据股票市场自动选择基准指数"""
    if stock_code.endswith('.HK'):
        return '^HSI'  # 恒生指数
    elif stock_code.endswith('.SS') or stock_code.endswith('.SZ'):
        return '000001.SS'  # 上证指数
    else:
        return '^GSPC'  # 标普500

def calculate_risk_metrics(df, stock_code):
    """
    【V83 P0.2】计算风险指标：Beta, Alpha, Correlation, Volatility
    
    参数：
        df: 股票数据DataFrame
        stock_code: 股票代码（用于判断基准）
    
    返回：
        包含 alpha, beta, correlation, volatility 的字典
    """
    try:
        if df is None or len(df) < 60:
            return None
        
        # 获取基准指数
        benchmark_code = get_benchmark_code(stock_code)
        _safe_print(f"[Risk] 获取基准指数: {benchmark_code}")
        
        # 获取基准数据（使用相同时间范围）
        benchmark_df = fetch_stock_data(benchmark_code)
        if benchmark_df is None or len(benchmark_df) < 60:
            _safe_print(f"[Risk] ⚠️ 基准数据获取失败")
            return None
        
        # 对齐日期（取交集）
        common_dates = df.index.intersection(benchmark_df.index)
        if len(common_dates) < 60:
            _safe_print(f"[Risk] ⚠️ 共同日期不足60天")
            return None
        
        stock_aligned = df.loc[common_dates, 'Close']
        benchmark_aligned = benchmark_df.loc[common_dates, 'Close']
        
        # 计算收益率
        stock_ret = stock_aligned.pct_change().dropna()
        benchmark_ret = benchmark_aligned.pct_change().dropna()
        
        # 再次对齐（去除NaN后）
        common_idx = stock_ret.index.intersection(benchmark_ret.index)
        stock_ret = stock_ret.loc[common_idx]
        benchmark_ret = benchmark_ret.loc[common_idx]
        
        if len(stock_ret) < 30:
            _safe_print(f"[Risk] ⚠️ 有效数据点不足30天")
            return None
        
        # 计算指标（年化）
        # Beta: Cov(stock, benchmark) / Var(benchmark)
        covariance = np.cov(stock_ret, benchmark_ret)[0, 1]
        benchmark_variance = np.var(benchmark_ret)
        beta = covariance / benchmark_variance if benchmark_variance > 0 else 1.0
        
        # Alpha: 股票年化收益 - (无风险利率 + Beta * (基准年化收益 - 无风险利率))
        rf_annual = 0.03  # 无风险利率3%
        stock_annual_return = stock_ret.mean() * 252
        benchmark_annual_return = benchmark_ret.mean() * 252
        alpha = stock_annual_return - (rf_annual + beta * (benchmark_annual_return - rf_annual))
        
        # Correlation: 相关系数
        correlation = np.corrcoef(stock_ret, benchmark_ret)[0, 1]
        
        # Volatility: 年化波动率
        volatility = stock_ret.std() * np.sqrt(252)
        
        _safe_print(f"[Risk] ✅ Beta={beta:.2f}, Alpha={alpha*100:.2f}%, Corr={correlation:.2f}, Vol={volatility*100:.1f}%")
        
        return {
            'alpha': alpha,
            'beta': beta,
            'correlation': correlation,
            'volatility': volatility,
            'benchmark': benchmark_code,
            'benchmark_name': '标普500' if benchmark_code == '^GSPC' else ('恒生指数' if benchmark_code == '^HSI' else '上证指数')
        }
    except Exception as e:
        _safe_print(f"[Risk] ❌ 计算失败: {type(e).__name__}: {str(e)[:100]}")
        return None

# ═══════════════════════════════════════════════════════════════
# 【V84 自检与诊断模块】System Self-Diagnostic
# ═══════════════════════════════════════════════════════════════
def run_system_diagnostic():
    """
    【V84.1】系统自检：网络连通性 + 数据源冒烟测试
    
    返回：
        {
            'network': {'status': 'ok'/'error', 'message': str, 'latency': float},
            'data_sources': {
                'us': {'status': 'ok'/'error', 'code': 'AAPL', 'message': str, 'data_points': int},
                'hk': {...},
                'cn': {...}
            },
            'overall': 'healthy'/'warning'/'error'
        }
    """
    result = {
        'network': {},
        'data_sources': {},
        'overall': 'healthy'
    }
    
    # ═══ 1️⃣ 网络连通性测试 ═══
    try:
        start_time = time.time()
        proxy_url = get_proxy_url()
        
        # 测试Google连通性
        test_url = "https://www.google.com"
        if proxy_url:
            proxies = {"http": proxy_url, "https": proxy_url}
            response = requests.get(test_url, proxies=proxies, timeout=5, verify=False)
        else:
            response = requests.get(test_url, timeout=5, verify=False)
        
        latency = (time.time() - start_time) * 1000  # 转换为毫秒
        
        if response.status_code == 200:
            result['network'] = {
                'status': 'ok',
                'message': f'网络连通正常（延迟 {latency:.0f}ms）',
                'latency': latency
            }
        else:
            result['network'] = {
                'status': 'warning',
                'message': f'网络可访问但响应异常（HTTP {response.status_code}）',
                'latency': latency
            }
            result['overall'] = 'warning'
    except requests.exceptions.ProxyError as e:
        result['network'] = {
            'status': 'error',
            'message': f'代理连接失败：{str(e)[:100]}',
            'latency': 0
        }
        result['overall'] = 'error'
    except requests.exceptions.Timeout:
        result['network'] = {
            'status': 'error',
            'message': '网络超时（>5秒）',
            'latency': 5000
        }
        result['overall'] = 'error'
    except Exception as e:
        result['network'] = {
            'status': 'error',
            'message': f'网络测试失败：{type(e).__name__}',
            'latency': 0
        }
        result['overall'] = 'error'
    
    # ═══ 2️⃣ 数据源冒烟测试 ═══
    # 【V85 增强】随机抽取3只港股和3只美股进行测试
    import random
    
    # 固定基础测试
    test_stocks = [
        ('cn', '600519.SS', 'A股（茅台）')
    ]
    
    # 随机抽取3只港股
    hk_codes = [item[2] for item in RAW_HK]  # 使用第3个元素（已经是.HK格式）
    hk_samples = random.sample(hk_codes, min(3, len(hk_codes)))
    for hk_code in hk_samples:
        hk_name = next((item[1] for item in RAW_HK if item[2] == hk_code), hk_code)
        test_stocks.append(('hk', hk_code, f'港股（{hk_name}）'))
    
    # 随机抽取3只美股
    us_codes = [item[0] for item in RAW_US]
    us_samples = random.sample(us_codes, min(3, len(us_codes)))
    for us_code in us_samples:
        us_name = next((item[1] for item in RAW_US if item[0] == us_code), us_code)
        test_stocks.append(('us', us_code, f'美股（{us_name}）'))
    
    for market, code, name in test_stocks:
        try:
            _safe_print(f"[诊断] 测试 {name} ({code})...")
            df = fetch_stock_data(code)
            
            if df is not None and not df.empty and len(df) >= 5:
                result['data_sources'][market] = {
                    'status': 'ok',
                    'code': code,
                    'name': name,
                    'message': f'数据正常（{len(df)} 条记录）',
                    'data_points': len(df),
                    'last_date': df.index[-1].strftime('%Y-%m-%d')
                }
                _safe_print(f"[诊断] ✅ {name} ({code}): {len(df)} 条数据")
            elif df is not None and not df.empty:
                result['data_sources'][market] = {
                    'status': 'warning',
                    'code': code,
                    'name': name,
                    'message': f'数据不足（仅 {len(df)} 条记录，建议>5条）',
                    'data_points': len(df)
                }
                if result['overall'] == 'healthy':
                    result['overall'] = 'warning'
                _safe_print(f"[诊断] ⚠️ {name} ({code}): 仅 {len(df)} 条数据")
            else:
                result['data_sources'][market] = {
                    'status': 'error',
                    'code': code,
                    'name': name,
                    'message': '❌ 数据获取失败（返回 0 行数据） - 代理配置无效或 Yahoo 接口被封',
                    'data_points': 0
                }
                result['overall'] = 'error'
                _safe_print(f"[诊断] ❌ {name} ({code}): 0 条数据 - 接口失败！")
        except Exception as e:
            result['data_sources'][market] = {
                'status': 'error',
                'code': code,
                'name': name,
                'message': f'测试异常：{type(e).__name__} - {str(e)[:80]}',
                'data_points': 0
            }
            result['overall'] = 'error'
    
    return result

# ═══════════════════════════════════════════════════════════════
# 【V83 P0.3】事实新闻源
# ═══════════════════════════════════════════════════════════════
@st.cache_data(ttl=900)  # 【V91.3】交易日15分钟缓存
def fetch_news_headlines(code):
    """
    【V87.5优化】获取真实新闻标题 + 增强多源获取
    
    参数：
        code: 股票代码
    
    返回：
        新闻列表，每条包含 {time, title, source, link, summary}
    """
    try:
        if not HAS_YFINANCE:
            _safe_print(f"[News] ⚠️ yfinance未安装")
            return []
        
        target_code = to_yf_cn_code(code)
        proxy_url = get_proxy_url()
        
        _safe_print(f"[News] 🔍 开始获取 {target_code} 的新闻...")
        
        with ProxyContext(proxy_url):
            ticker = yf.Ticker(target_code)
            # 【V87.5】增加超时控制，避免卡顿
            import signal
            
            def timeout_handler(signum, frame):
                raise TimeoutError("新闻获取超时")
            
            # 设置5秒超时
            try:
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(5)
                news = ticker.news
                signal.alarm(0)  # 取消超时
            except:
                # Windows不支持signal.SIGALRM，直接获取
                news = ticker.news
        
        _safe_print(f"[News] 📊 原始新闻数量: {len(news) if news else 0}")
        
        if not news or len(news) == 0:
            _safe_print(f"[News] ⚠️ 无真实新闻，使用AI生成舆情")
            return []
        
        # 【V87.5】格式化新闻，增加摘要
        formatted_news = []
        for item in news[:8]:  # 【V87.5】增加到8条
            # 提取摘要（如果有）
            summary = ""
            if 'summary' in item:
                summary = item['summary'][:200] + "..." if len(item.get('summary', '')) > 200 else item.get('summary', '')
            
            formatted_news.append({
                'time': pd.Timestamp(item.get('providerPublishTime', 0), unit='s').strftime('%Y-%m-%d %H:%M') if item.get('providerPublishTime') else 'N/A',
                'title': item.get('title', '无标题'),
                'source': item.get('publisher', '未知来源'),
                'link': item.get('link', ''),
                'summary': summary
            })
        
        _safe_print(f"[News] ✅ 成功获取 {len(formatted_news)} 条新闻")
        return formatted_news
        
    except TimeoutError:
        _safe_print(f"[News] ⏱️ 获取超时")
        return []
    except Exception as e:
        _safe_print(f"[News] ❌ 获取失败: {type(e).__name__} - {str(e)}")
        return []

# ═══════════════════════════════════════════════════════════════
# 【V83 P1】交易计划与风险预算
# ═══════════════════════════════════════════════════════════════
def calculate_trade_plan(df, code):
    """
    【V83 P1.4】机构式交易计划
    
    参数：
        df: 股票数据DataFrame
        code: 股票代码
    
    返回：
        包含entry_zone, stop_loss, take_profit, risk_reward, position_size的字典
    """
    try:
        if df is None or len(df) < 50:
            return None
        
        last = df.iloc[-1]
        current_price = last['Close']
        
        # ATR（已在df中计算）
        high_low = df['High'] - df['Low']
        high_close = np.abs(df['High'] - df['Close'].shift())
        low_close = np.abs(df['Low'] - df['Close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        atr = ranges.max(axis=1).rolling(14).mean().iloc[-1]
        
        # MA20和MA50（应该已经在df中）
        ma20 = df['Close'].rolling(20).mean().iloc[-1] if len(df) >= 20 else current_price
        ma50 = df['Close'].rolling(50).mean().iloc[-1] if len(df) >= 50 else current_price
        
        # 1️⃣ 入场区间：MA20 ± ATR * 0.5
        entry_low = ma20 - atr * 0.5
        entry_high = ma20 + atr * 0.5
        
        # 2️⃣ 止损位：MA50 - ATR（或前低）
        recent_low = df['Low'].tail(20).min()
        stop_loss = min(ma50 - atr, recent_low - atr * 0.5)
        
        # 3️⃣ 止盈位：1.5R和2R
        risk = current_price - stop_loss if current_price > stop_loss else atr
        take_profit_15r = current_price + risk * 1.5
        take_profit_2r = current_price + risk * 2.0
        
        # 4️⃣ 盈亏比
        risk_reward = risk / (current_price - stop_loss) if (current_price - stop_loss) > 0 else 0
        
        # 【V83 P1.5】风险预算仓位建议
        total_equity = 100000  # 假设总资金10万
        risk_budget_pct = 0.01  # 单笔风险1%
        risk_amount = total_equity * risk_budget_pct
        max_position = int(risk_amount / (current_price - stop_loss)) if (current_price - stop_loss) > 0 else 0
        position_value = max_position * current_price
        
        return {
            'entry_low': entry_low,
            'entry_high': entry_high,
            'entry_mid': (entry_low + entry_high) / 2,
            'stop_loss': stop_loss,
            'take_profit_15r': take_profit_15r,
            'take_profit_2r': take_profit_2r,
            'risk_per_share': current_price - stop_loss,
            'reward_15r': take_profit_15r - current_price,
            'reward_2r': take_profit_2r - current_price,
            'risk_reward_ratio': (take_profit_15r - current_price) / (current_price - stop_loss) if (current_price - stop_loss) > 0 else 0,
            'current_price': current_price,
            'max_position': max_position,
            'position_value': position_value,
            'risk_budget_pct': risk_budget_pct * 100
        }
    except Exception as e:
        _safe_print(f"[TradePlan] ❌ 计算失败: {type(e).__name__}")
        return None

# ═══════════════════════════════════════════════════════════════
# 7.5 【V87.8】失败详情显示函数
# ═══════════════════════════════════════════════════════════════
def display_scan_failures(all_errors, total_failed):
    """显示扫描失败的详细信息"""
    with st.expander(f"⚠️ 查看失败详情 ({total_failed}只) - 点击展开诊断", expanded=False):
        st.caption("💡 **常见失败原因**：")
        st.caption("1. 股票已退市或被收购（如ATVI被微软收购）")
        st.caption("2. 股票代码格式错误")
        st.caption("3. 网络连接问题或代理设置错误")
        st.caption("4. 数据源暂时不可用")
        st.divider()
        
        # 按市场分组显示
        us_errors = []
        hk_errors = []
        cn_errors = []
        
        for e in all_errors:
            code = e['code']
            if '.HK' in code or (len(code) == 5 and code[0] == '0'):
                hk_errors.append(e)
            elif '.SS' in code or '.SZ' in code or (len(code) == 6 and code[0] in '630'):
                cn_errors.append(e)
            else:
                us_errors.append(e)
        
        if us_errors:
            st.markdown("**🇺🇸 美股失败列表：**")
            for err in us_errors:
                st.caption(f"❌ **{err['name']}** ({err['code']}): {err['error']}")
        
        if hk_errors:
            st.markdown("**🇭🇰 港股失败列表：**")
            for err in hk_errors:
                st.caption(f"❌ **{err['name']}** ({err['code']}): {err['error']}")
        
        if cn_errors:
            st.markdown("**🇨🇳 A股失败列表：**")
            for err in cn_errors:
                st.caption(f"❌ **{err['name']}** ({err['code']}): {err['error']}")

# ═══════════════════════════════════════════════════════════════
# 【V89.7 重构】持仓管理 - 包装为函数，延迟到主内容区渲染
# ═══════════════════════════════════════════════════════════════
def _render_portfolio_section():
  """持仓管理渲染函数 - 在主内容区调用"""
  if not (Config.PORTFOLIO_ENABLED and PORTFOLIO_MANAGER_AVAILABLE and _portfolio_manager):
    return
  try:
    from datetime import datetime as _dt_port
    _port_today = _dt_port.now().strftime("%Y-%m-%d")
    st.markdown(f'<div style="font-family: Georgia, \'Times New Roman\', SimSun, 宋体, serif; background: linear-gradient(135deg, #10b981 0%, #059669 100%); padding: 1.2rem; border-radius: 10px; margin: 0.5rem 0;"><h3 style="font-family: inherit; color: white; margin: 0; text-align: center; font-size: 14px; font-weight: 700;">💼 我的持仓</h3><p style="font-family: inherit; color: rgba(255,255,255,0.85); margin: 0.3rem 0 0 0; text-align: center; font-size: 12px;">Excel数据源 · 实时盈亏 · AI分析</p><p style="font-family: inherit; color: rgba(255,255,255,0.6); margin: 0.2rem 0 0 0; text-align: center; font-size: 12px;">📅 {_port_today}</p></div>', unsafe_allow_html=True)
    
    try:
        # 【V89.6.2】显示文件信息和自动检测文件变更
        file_path = os.path.abspath(Config.PORTFOLIO_FILE)
        file_mtime_str = "未知"  # 默认值
        file_mtime = None
        
        if os.path.exists(file_path):
            import time
            file_mtime = os.path.getmtime(file_path)
            file_mtime_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(file_mtime))
            
            # 【V89.6.2】自动检测文件是否被修改
            if 'portfolio_last_mtime' not in st.session_state:
                st.session_state.portfolio_last_mtime = file_mtime
                logging.info(f"📝 初始化持仓文件修改时间: {file_mtime_str}")
            elif st.session_state.portfolio_last_mtime != file_mtime:
                # 文件已被修改！
                st.info(f"🔔 检测到持仓文件已更新！（{file_mtime_str}）正在自动刷新...")
                st.session_state.portfolio_last_mtime = file_mtime
                logging.info(f"🔄 持仓文件已变更，自动刷新: {file_mtime_str}")
                
                # 清除所有可能的缓存
                if hasattr(_portfolio_manager, '_cached_df'):
                    delattr(_portfolio_manager, '_cached_df')
                if 'portfolio_data_cache' in st.session_state:
                    del st.session_state.portfolio_data_cache
                
                time.sleep(0.5)  # 短暂延迟确保文件写入完成
            
            info_col1, info_col2, info_col3 = st.columns([2, 2, 1])
            with info_col1:
                st.caption(f"📁 文件位置: `{file_path}`")
            with info_col2:
                st.caption(f"🕒 最后修改: {file_mtime_str}")
            with info_col3:
                if st.button("🔄 强制刷新", key="force_reload_portfolio", help="重新加载Excel文件"):
                    # 清除所有可能的缓存
                    if hasattr(_portfolio_manager, '_cached_df'):
                        delattr(_portfolio_manager, '_cached_df')
                    if 'portfolio_data_cache' in st.session_state:
                        del st.session_state.portfolio_data_cache
                    if 'portfolio_last_mtime' in st.session_state:
                        del st.session_state.portfolio_last_mtime
                    st.toast("🔄 正在重新加载Excel...", icon="🔄")
                    st.rerun()
        
        # 【V89.6.2】强制每次都重新读取Excel，不使用任何缓存
        # 先清除 PortfolioManager 内部可能的缓存
        if hasattr(_portfolio_manager, '_cached_df'):
            delattr(_portfolio_manager, '_cached_df')
        
        # 直接读取Excel文件，完全绕过缓存
        try:
            import pandas as pd
            portfolio_df = pd.read_excel(Config.PORTFOLIO_FILE, sheet_name='我的持仓', engine='openpyxl')
            
            # 数据验证和清洗
            if '股票代码' in portfolio_df.columns and len(portfolio_df) > 0:
                original_count = len(portfolio_df)
                
                # 清理空值
                portfolio_df = portfolio_df.dropna(subset=['股票代码'])
                
                # 数据类型转换
                portfolio_df['持仓数量'] = pd.to_numeric(portfolio_df['持仓数量'], errors='coerce')
                portfolio_df['买入价格'] = pd.to_numeric(portfolio_df['买入价格'], errors='coerce')
                
                # 移除无效数据
                portfolio_df = portfolio_df.dropna(subset=['持仓数量', '买入价格'])
                portfolio_df = portfolio_df[portfolio_df['持仓数量'] > 0]
                portfolio_df = portfolio_df[portfolio_df['买入价格'] > 0]
                
                if len(portfolio_df) < original_count:
                    st.caption(f"⚠️ 已过滤 {original_count - len(portfolio_df)} 行无效数据")
                
                logging.info(f"✅ 直接读取Excel成功: {len(portfolio_df)}只股票")
            else:
                portfolio_df = None
                logging.warning("⚠️ Excel文件格式不正确或为空")
                
        except Exception as e:
            logging.error(f"❌ 直接读取Excel失败: {str(e)}")
            # 降级到 PortfolioManager
            portfolio_df = _portfolio_manager.read_portfolio()
        
        # 【V89.6.2】显示读取状态
        if portfolio_df is not None and len(portfolio_df) > 0:
            st.success(f"✅ 成功读取持仓: {len(portfolio_df)}只股票 | 文件: {os.path.basename(Config.PORTFOLIO_FILE)} | 最后修改: {file_mtime_str}")
            
            # 显示读取到的股票名称
            stock_names = ', '.join([f"{row['股票名称']}" for _, row in portfolio_df.head(5).iterrows()])
            if len(portfolio_df) > 5:
                stock_names += f" 等{len(portfolio_df)}只"
            st.caption(f"📋 持仓股票: {stock_names}")
            
            # 【V89.6.7】醒目的价格缓存状态显示
            st.markdown("---")
            cache_status_col1, cache_status_col2, cache_status_col3 = st.columns([1, 2, 1])
            
            with cache_status_col1:
                if 'portfolio_prices_cache' in st.session_state and 'portfolio_prices_timestamp' in st.session_state:
                    cache_age = time.time() - st.session_state['portfolio_prices_timestamp']
                    if cache_age < 86400:
                        st.metric("📦 缓存状态", "✅ 有效")
                    else:
                        st.metric("⏰ 缓存状态", "❌ 已过期")
                else:
                    st.metric("🆕 缓存状态", "无缓存")
            
            with cache_status_col2:
                if 'portfolio_prices_timestamp' in st.session_state:
                    cache_age = time.time() - st.session_state['portfolio_prices_timestamp']
                    remaining_hours = (86400 - cache_age) / 3600
                    if remaining_hours > 0:
                        cache_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(st.session_state['portfolio_prices_timestamp']))
                        st.caption(f"🕐 更新时间: {cache_time}")
                        st.caption(f"⏳ 剩余有效期: {remaining_hours:.1f}小时")
                    else:
                        st.caption(f"⏰ 缓存已过期: {-remaining_hours:.1f}小时前")
                else:
                    st.caption("首次获取价格数据")
            
            with cache_status_col3:
                if 'portfolio_prices_cache' in st.session_state:
                    cached_count = len(st.session_state['portfolio_prices_cache'])
                    st.metric("缓存股票数", f"{cached_count}只")
            
            st.markdown("---")
        
        if portfolio_df is None or len(portfolio_df) == 0:
            st.warning(f"⚠️ 未读取到持仓数据。请检查 **{Config.PORTFOLIO_FILE}** 文件是否有有效数据。")
            
            # 【V89.6】添加调试信息
            with st.expander("🔍 调试信息（如果Excel有数据但不显示，请查看此处）"):
                st.code(f"""
文件路径: {file_path}
文件是否存在: {os.path.exists(file_path)}
文件大小: {os.path.getsize(file_path) if os.path.exists(file_path) else 'N/A'} 字节
最后修改时间: {file_mtime_str}

可能的原因:
1. Excel文件正在被其他程序打开（请关闭Excel后刷新）
2. Excel文件格式不正确（sheet名称必须是"我的持仓"）
3. Excel中没有有效数据（检查必填列: 股票代码、股票名称、持仓数量、买入价格）
4. 数据被清洗过滤掉了（持仓数量和买入价格必须>0）

解决方法:
→ 点击下方"📝 创建持仓模板"重新生成模板
→ 或手动打开Excel文件检查内容
→ 确认保存后点击"🔄 强制刷新"
                """)
            
            col_create, col_open = st.columns(2)
            with col_create:
                if st.button("📝 创建持仓模板", type="primary", use_container_width=True):
                    if _portfolio_manager.create_template():
                        st.success(f"✅ 已创建持仓模板: {Config.PORTFOLIO_FILE}")
                        st.info("💡 请手动编辑Excel文件，添加您的真实持仓数据后刷新页面。")
                    else:
                        st.error("❌ 创建模板失败")
            
            with col_open:
                if st.button("📂 打开Excel编辑", use_container_width=True):
                    import subprocess
                    import platform
                    try:
                        file_path = os.path.abspath(Config.PORTFOLIO_FILE)
                        if platform.system() == 'Darwin':  # macOS
                            subprocess.call(['open', file_path])
                        elif platform.system() == 'Windows':
                            os.startfile(file_path)
                        else:  # Linux
                            subprocess.call(['xdg-open', file_path])
                        st.success(f"✅ 已打开文件: {Config.PORTFOLIO_FILE}")
                        st.info("💡 编辑并保存后，刷新页面即可自动加载新数据")
                        st.caption("⚡ 应用会自动检测文件变化！")
                    except Exception as e:
                        st.error(f"❌ 打开文件失败: {str(e)}")
                        st.caption(f"💡 请手动打开: {os.path.abspath(Config.PORTFOLIO_FILE)}")
        
        else:
            # 【V90.4】直接交互式编辑持仓表格 - 点击即可修改/删除/添加
            st.caption("💡 直接点击单元格修改 | 底部 ➕ 添加新股票 | 勾选左侧复选框后按 Delete 删除")
            
            # 准备编辑用的DataFrame：只保留预期列 + 强制转换类型避免报错
            expected_cols = ['股票代码', '股票名称', '持仓数量', '买入价格', '买入日期', '备注']
            edit_df = portfolio_df.copy()
            
            # 确保所有预期列都存在
            for _ec in expected_cols:
                if _ec not in edit_df.columns:
                    edit_df[_ec] = ""
            
            # 只保留预期列，丢弃多余列
            edit_df = edit_df[expected_cols]
            
            # 【关键修复】强制将文本列转为 str，避免 NaN/float 与 TextColumn 配置冲突
            edit_df['股票代码'] = edit_df['股票代码'].astype(str).replace('nan', '')
            edit_df['股票名称'] = edit_df['股票名称'].astype(str).replace('nan', '')
            edit_df['买入日期'] = edit_df['买入日期'].astype(str).replace('nan', '').replace('NaT', '')
            edit_df['备注'] = edit_df['备注'].astype(str).replace('nan', '')
            
            # 直接用 data_editor 显示，用户可即时编辑
            edited_df = st.data_editor(
                edit_df,
                num_rows="dynamic",
                use_container_width=True,
                column_config={
                    "股票代码": st.column_config.TextColumn(
                        "股票代码",
                        required=True,
                        help="美股: AAPL | 港股: 00700.HK | A股: 600519.SS"
                    ),
                    "股票名称": st.column_config.TextColumn(
                        "股票名称",
                        required=True,
                        help="股票中文名称"
                    ),
                    "持仓数量": st.column_config.NumberColumn(
                        "持仓数量",
                        required=True,
                        min_value=1,
                        help="持有股数（>0）"
                    ),
                    "买入价格": st.column_config.NumberColumn(
                        "买入价格",
                        required=True,
                        min_value=0.01,
                        format="%.2f",
                        help="成本价（>0）"
                    ),
                    "买入日期": st.column_config.TextColumn(
                        "买入日期",
                        help="格式: YYYY-MM-DD"
                    ),
                    "备注": st.column_config.TextColumn(
                        "备注",
                        help="个人备注"
                    ),
                },
                hide_index=True,
                key="portfolio_data_editor"
            )
            
            # 检测是否有修改
            _has_change = False
            if len(edited_df) != len(edit_df):
                _has_change = True
            elif not edited_df.equals(edit_df):
                _has_change = True
            
            # 操作按钮行
            btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 2])
            with btn_col1:
                _save_clicked = st.button("💾 保存修改", type="primary", use_container_width=True, disabled=not _has_change)
            with btn_col2:
                if st.button("📂 打开Excel", use_container_width=True, key="open_excel_btn"):
                    import subprocess
                    import platform
                    try:
                        file_path = os.path.abspath(Config.PORTFOLIO_FILE)
                        if platform.system() == 'Darwin':
                            subprocess.call(['open', file_path])
                        elif platform.system() == 'Windows':
                            os.startfile(file_path)
                        else:
                            subprocess.call(['xdg-open', file_path])
                        st.success(f"✅ 已打开: {Config.PORTFOLIO_FILE}")
                    except Exception as e:
                        st.error(f"❌ 打开失败: {str(e)}")
            with btn_col3:
                if _has_change:
                    if len(edited_df) > len(edit_df):
                        st.info(f"➕ 新增 {len(edited_df) - len(edit_df)} 只股票，点击「保存修改」生效")
                    elif len(edited_df) < len(edit_df):
                        st.warning(f"🗑️ 删除 {len(edit_df) - len(edited_df)} 只股票，点击「保存修改」生效")
                    else:
                        st.info("✏️ 检测到数据修改，点击「保存修改」生效")
            
            # 保存逻辑
            if _save_clicked:
                try:
                    valid_df = edited_df.copy()
                    
                    # 清理空行
                    valid_df = valid_df.dropna(subset=['股票代码', '股票名称'])
                    valid_df = valid_df[valid_df['股票代码'].str.strip() != '']
                    valid_df = valid_df[valid_df['股票名称'].str.strip() != '']
                    
                    # 验证数值
                    valid_df['持仓数量'] = pd.to_numeric(valid_df['持仓数量'], errors='coerce')
                    valid_df['买入价格'] = pd.to_numeric(valid_df['买入价格'], errors='coerce')
                    
                    # 过滤无效数据
                    before_count = len(valid_df)
                    valid_df = valid_df.dropna(subset=['持仓数量', '买入价格'])
                    valid_df = valid_df[valid_df['持仓数量'] > 0]
                    valid_df = valid_df[valid_df['买入价格'] > 0]
                    filtered_count = before_count - len(valid_df)
                    
                    if filtered_count > 0:
                        st.warning(f"⚠️ 已过滤 {filtered_count} 行无效数据")
                    
                    # 保存到Excel
                    valid_df.to_excel(Config.PORTFOLIO_FILE, index=False, sheet_name='我的持仓')
                    
                    # 清除价格缓存
                    if 'portfolio_prices_cache' in st.session_state:
                        del st.session_state['portfolio_prices_cache']
                    if 'portfolio_prices_timestamp' in st.session_state:
                        del st.session_state['portfolio_prices_timestamp']
                    
                    st.success(f"✅ 已保存（共{len(valid_df)}只股票）")
                    time.sleep(0.5)
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ 保存失败: {str(e)}")
                    logging.error(f"保存持仓失败: {str(e)}", exc_info=True)
            
            st.markdown("---")
            
            # 【V89.6.7】获取当前价格 - 【V91.10】统一缓存：交易日15分钟，非交易日24小时
            cache_key = 'portfolio_prices_cache'
            cache_timestamp_key = 'portfolio_prices_timestamp'
            cache_ttl = get_smart_cache_ttl('daily')
            
            current_time = time.time()
            current_prices = None  # 使用None而不是{}，方便判断是否已从缓存加载
            
            # 【V89.6.7】调试信息：检查缓存状态
            force_refresh_price = st.session_state.get('force_refresh_price', False)
            has_cache = cache_key in st.session_state
            has_timestamp = cache_timestamp_key in st.session_state
            
            st.markdown("---")
            st.markdown("### 💰 价格数据")
            
            # 【调试面板】显示当前缓存状态
            with st.expander("🔍 缓存状态（调试）", expanded=False):
                st.code(f"""
强制刷新: {force_refresh_price}
缓存存在: {has_cache}
时间戳存在: {has_timestamp}
当前时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))}
""")
                if has_timestamp:
                    cache_age = current_time - st.session_state[cache_timestamp_key]
                    st.code(f"""
缓存时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(st.session_state[cache_timestamp_key]))}
缓存年龄: {cache_age:.0f}秒 ({cache_age/3600:.2f}小时)
缓存TTL: {cache_ttl}秒 ({cache_ttl/3600:.0f}小时)
是否有效: {cache_age < cache_ttl}
""")
            
            logging.info(f"📊 持仓价格缓存检查: force_refresh={force_refresh_price}, has_cache={has_cache}, has_timestamp={has_timestamp}")
            
            # 尝试使用缓存
            if not force_refresh_price and has_cache and has_timestamp:
                try:
                    cache_age = current_time - st.session_state[cache_timestamp_key]
                    remaining_hours = (cache_ttl - cache_age) / 3600
                    
                    if cache_age < cache_ttl:
                        # 缓存有效 - 直接使用！
                        current_prices = st.session_state[cache_key]
                        cache_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(st.session_state[cache_timestamp_key]))
                        st.success(f"✅ 使用缓存价格数据 | 更新时间: {cache_time_str} | 剩余有效期: {remaining_hours:.1f}小时")
                        logging.info(f"✅ 使用持仓价格缓存，剩余{remaining_hours:.1f}小时")
                    else:
                        # 缓存过期
                        st.info(f"⏰ 价格缓存已过期（{cache_age/3600:.1f}小时前），正在重新获取...")
                        logging.info(f"⏰ 持仓价格缓存过期: {cache_age/3600:.1f}小时")
                except Exception as e:
                    st.warning(f"⚠️ 读取缓存失败: {str(e)}，将重新获取")
                    logging.error(f"读取缓存失败: {str(e)}")
            elif force_refresh_price:
                st.info("🔄 强制刷新模式，忽略缓存")
                logging.info("🔄 强制刷新持仓价格")
            elif not has_cache or not has_timestamp:
                _ttl_hint = f"{cache_ttl//3600}小时" if cache_ttl >= 3600 else f"{cache_ttl//60}分钟"
                st.info(f"🆕 首次获取价格数据，将缓存{_ttl_hint}")
                logging.info("🆕 首次获取持仓价格数据")
            
            # 如果缓存无效（current_prices仍为None），重新获取价格
            if current_prices is None:
                st.markdown("---")
                st.markdown("#### 📡 正在获取最新价格...")
                
                current_prices = {}
                price_progress = st.progress(0)
                price_status = st.empty()
                
                for idx, row in portfolio_df.iterrows():
                    code = str(row['股票代码']).strip()
                    stock_name = row['股票名称']
                    price_status.text(f"正在获取 {stock_name}({code}) 最新价格...")
                    
                    try:
                        # 获取数据
                        df_stock = fetch_stock_data(to_yf_cn_code(code))
                        if df_stock is not None and len(df_stock) > 0:
                            current_prices[code] = float(df_stock['Close'].iloc[-1])
                            logging.info(f"✅ {stock_name}({code}) 当前价格: {current_prices[code]}")
                        else:
                            current_prices[code] = None
                            logging.warning(f"⚠️ {stock_name}({code}) 价格获取失败：数据为空")
                    except Exception as e:
                        current_prices[code] = None
                        logging.error(f"❌ {stock_name}({code}) 价格获取异常: {str(e)}")
                    
                    price_progress.progress((idx + 1) / len(portfolio_df))
                
                price_progress.empty()
                price_status.empty()
                
                # 显示价格获取统计
                success_count = sum(1 for v in current_prices.values() if v is not None)
                st.caption(f"📊 价格获取: {success_count}/{len(portfolio_df)} 成功")
                
                # 【V89.6.7】强制保存到缓存
                try:
                    st.session_state[cache_key] = current_prices
                    st.session_state[cache_timestamp_key] = current_time
                    st.session_state['force_refresh_price'] = False
                    
                    cache_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))
                    st.success(f"✅ 价格数据已更新并缓存（有效期: 24小时） | 缓存时间: {cache_time_str}")
                    logging.info(f"✅ 持仓价格已保存到缓存: {len(current_prices)}只股票, 时间戳: {current_time}")
                except Exception as e:
                    st.error(f"❌ 缓存保存失败: {str(e)}")
                    logging.error(f"缓存保存失败: {str(e)}")
            
            # 添加强制刷新价格按钮
            refresh_col1, refresh_col2, refresh_col3 = st.columns([2, 2, 2])
            with refresh_col1:
                if st.button("🔄 强制刷新价格", key="force_refresh_prices_btn"):
                    st.session_state['force_refresh_price'] = True
                    if cache_key in st.session_state:
                        del st.session_state[cache_key]
                    if cache_timestamp_key in st.session_state:
                        del st.session_state[cache_timestamp_key]
                    st.toast("🔄 价格缓存已清除，正在重新获取...", icon="🔄")
                    st.rerun()
            
            with refresh_col2:
                if cache_timestamp_key in st.session_state:
                    cache_time = time.strftime('%H:%M:%S', time.localtime(st.session_state[cache_timestamp_key]))
                    st.caption(f"⏰ 价格数据时间: {cache_time}")
            
            with refresh_col3:
                st.caption("💡 价格每天自动更新一次")
            
            # 计算持仓指标
            try:
                metrics_df = _portfolio_manager.calculate_portfolio_metrics(portfolio_df, current_prices)
            except Exception as e:
                st.error(f"❌ 计算持仓指标失败: {str(e)}")
                logging.error(f"计算持仓指标失败: {str(e)}", exc_info=True)
                # 使用简化计算
                metrics_df = portfolio_df.copy()
                for code in current_prices:
                    if code in metrics_df['股票代码'].values:
                        idx = metrics_df[metrics_df['股票代码'] == code].index[0]
                        current_price = current_prices[code]
                        if current_price is not None:
                            metrics_df.loc[idx, '当前价格'] = current_price
                            cost = metrics_df.loc[idx, '买入价格']
                            quantity = metrics_df.loc[idx, '持仓数量']
                            metrics_df.loc[idx, '盈亏比例'] = ((current_price - cost) / cost * 100)
                            metrics_df.loc[idx, '盈亏金额'] = (current_price - cost) * quantity
                            metrics_df.loc[idx, '持仓市值'] = current_price * quantity
                        else:
                            metrics_df.loc[idx, '当前价格'] = None
                            metrics_df.loc[idx, '盈亏比例'] = None
                            metrics_df.loc[idx, '盈亏金额'] = None
                            metrics_df.loc[idx, '持仓市值'] = None
            
            # 获取汇总信息
            summary = _portfolio_manager.get_portfolio_summary(metrics_df)
            
            if summary:
                # 显示汇总
                st.markdown("### 📊 持仓汇总")
                sum_col1, sum_col2, sum_col3, sum_col4 = st.columns(4)
                
                with sum_col1:
                    st.metric("总市值", f"¥{summary['total_market_value']:,.2f}")
                
                with sum_col2:
                    st.metric("总成本", f"¥{summary['total_cost']:,.2f}")
                
                with sum_col3:
                    profit_color = "normal" if summary['total_profit'] >= 0 else "inverse"
                    st.metric("总盈亏", f"¥{summary['total_profit']:,.2f}", 
                             delta=f"{summary['total_profit_pct']:.2f}%",
                             delta_color=profit_color)
                
                with sum_col4:
                    st.metric("持仓股票", f"{summary['stock_count']}只",
                             delta=f"盈利{summary['profitable_count']}只")
                
                st.markdown("---")
            
            # 显示持仓明细
            st.markdown("### 💼 持仓明细")
            
            # 格式化显示
            display_df = metrics_df.copy()
            display_df = display_df[['股票代码', '股票名称', '持仓数量', '买入价格', 
                                    '当前价格', '盈亏比例', '盈亏金额', '持仓市值', '备注']]
            
            # 添加样式
            def highlight_profit(row):
                if pd.isna(row['盈亏比例']):
                    return [''] * len(row)
                
                color = ''
                if row['盈亏比例'] > 0:
                    color = 'background-color: #10b98120'
                elif row['盈亏比例'] < 0:
                    color = 'background-color: #ef444420'
                
                return [color] * len(row)
            
            styled_df = display_df.style.apply(highlight_profit, axis=1)
            st.dataframe(styled_df, use_container_width=True, height=400)
            
            # 【V89.5】AI持仓组合分析
            st.markdown("---")
            st.markdown("### 🤖 AI持仓组合分析")
            st.caption("💡 按市场分组分析您的持仓组合（美股/港股/A股）")
            
            # 按市场分组持仓
            def classify_market(code):
                """判断股票所属市场"""
                code_str = str(code).strip()
                if code_str[0].isalpha():  # 以字母开头，美股
                    return "🇺🇸 美股"
                elif len(code_str) == 5 or (len(code_str) >= 4 and code_str[0] == '0' and not code_str.startswith('00')):  # 港股
                    return "🇭🇰 港股"
                elif code_str.startswith('6') or code_str.startswith('0') or code_str.startswith('3'):  # A股
                    return "🇨🇳 A股"
                else:
                    return "❓ 其他"
            
            # 为每只股票分类
            metrics_df['市场'] = metrics_df['股票代码'].apply(classify_market)
            
            # 按市场分组统计
            market_summary = {}
            for market in ["🇺🇸 美股", "🇭🇰 港股", "🇨🇳 A股"]:
                market_stocks = metrics_df[metrics_df['市场'] == market]
                if len(market_stocks) > 0:
                    market_summary[market] = {
                        'count': len(market_stocks),
                        'total_value': market_stocks['持仓市值'].sum(),
                        'total_profit': market_stocks['盈亏金额'].sum(),
                        'stocks': market_stocks,
                        'top_stock': market_stocks.nlargest(1, '持仓市值').iloc[0] if len(market_stocks) > 0 else None
                    }
            
            # 显示市场分组
            if market_summary:
                st.markdown("#### 📊 市场分布")
                market_cols = st.columns(len(market_summary))
                
                for idx, (market, data) in enumerate(market_summary.items()):
                    with market_cols[idx]:
                        profit_pct = (data['total_profit'] / (data['total_value'] - data['total_profit']) * 100) if (data['total_value'] - data['total_profit']) > 0 else 0
                        st.metric(
                            market,
                            f"{data['count']}只",
                            delta=f"{profit_pct:+.2f}%"
                        )
                        st.caption(f"市值: ¥{data['total_value']:,.0f}")
                
                st.markdown("---")
                
                # AI分析选择
                analysis_option = st.radio(
                    "选择分析类型",
                    options=["📊 分市场组合分析", "🎯 单只股票深度分析"],
                    horizontal=True,
                    key="portfolio_analysis_type"
                )
                
                if analysis_option == "📊 分市场组合分析":
                    # 选择要分析的市场
                    available_markets = list(market_summary.keys())
                    selected_market = st.selectbox(
                        "选择市场进行AI组合分析",
                        options=available_markets,
                        key="portfolio_market_select"
                    )
                    
                    if st.button("🚀 启动市场组合分析", type="primary", key="portfolio_market_ai_btn", use_container_width=True):
                        if MY_GEMINI_KEY:
                            with st.spinner(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · {selected_market}持仓组合"):
                                try:
                                    market_data = market_summary[selected_market]
                                    stocks_info = []
                                    
                                    # 收集该市场的所有持仓信息
                                    for _, row in market_data['stocks'].iterrows():
                                        stock_info = {
                                            '代码': row['股票代码'],
                                            '名称': row['股票名称'],
                                            '持仓数量': row['持仓数量'],
                                            '买入价': row['买入价格'],
                                            '当前价': row['当前价格'],
                                            '盈亏': f"{row['盈亏比例']:.2f}%" if not pd.isna(row['盈亏比例']) else 'N/A',
                                            '市值': f"¥{row['持仓市值']:,.0f}",
                                            '市值占比': f"{row['持仓市值'] / market_data['total_value'] * 100:.1f}%"
                                        }
                                        stocks_info.append(stock_info)
                                    
                                    # 生成AI分析提示词
                                    prompt = f"""作为专业投资顾问，请分析以下{selected_market}持仓组合：

【组合概况】
- 持仓股票数: {market_data['count']}只
- 总市值: ¥{market_data['total_value']:,.2f}
- 总盈亏: ¥{market_data['total_profit']:,.2f}
- 盈亏比例: {market_data['total_profit'] / (market_data['total_value'] - market_data['total_profit']) * 100:.2f}%

【持仓明细】
"""
                                    for stock in stocks_info:
                                        prompt += f"\n{stock['名称']}({stock['代码']}): 持仓{stock['持仓数量']}股, 成本{stock['买入价']}, 现价{stock['当前价']}, 盈亏{stock['盈亏']}, 市值{stock['市值']} (占比{stock['市值占比']})"
                                    
                                    prompt += f"""

请从以下维度进行专业分析：

## 📊 组合结构分析
1. 仓位配置是否合理？是否过于集中？
2. 行业分散度如何？（根据股票名称判断）
3. 单只股票占比是否合适？（建议单只不超过20%）

## 💰 盈亏表现分析
1. 整体盈亏情况评价（优秀/良好/一般/较差）
2. 哪些股票贡献了主要收益？
3. 哪些股票拖累了组合表现？

## 🎯 持仓建议
1. 建议增持的股票及理由
2. 建议减持的股票及理由
3. 建议止盈/止损的股票及价位

## ⚖️ 风险评估
1. 组合整体风险等级（低/中/高）
2. 主要风险点
3. 风险控制建议

## 🔮 后市展望
1. {selected_market}市场短期展望（1-2周）
2. 该组合在当前市场环境下的适应性
3. 未来1-2个月的操作策略

请提供专业、具体、可操作的分析建议，字数600-800字。"""
                                    
                                    # 调用Gemini API
                                    ai_response = call_gemini_api(prompt)
                                    
                                    # 显示分析结果
                                    st.success(f"✅ {selected_market}持仓组合分析完成")
                                    
                                    # 显示组合信息
                                    with st.expander("📊 查看持仓明细", expanded=False):
                                        st.dataframe(
                                            pd.DataFrame(stocks_info),
                                            use_container_width=True,
                                            hide_index=True
                                        )
                                    
                                    # 显示AI分析
                                    st.markdown("---")
                                    st.markdown("##### 🤖 AI组合分析报告")
                                    # 【V90.3】段落级复制
                                    if COPY_UTILS_AVAILABLE:
                                        CopyUtils.render_markdown_with_section_copy(ai_response, key_prefix=f"port_{selected_market}")
                                    else:
                                        st.markdown(ai_response)
                                    st.caption(f"📌 本报告由 AI 生成 · 模型: {_ai_model_label()}")
                                
                                except Exception as e:
                                    st.error(f"❌ AI分析失败: {str(e)[:100]}")
                                    logging.error(f"持仓组合AI分析异常: {e}")
                        else:
                            st.warning("⚠️ 请配置Gemini API Key以使用AI分析功能")
                
                else:  # 单只股票深度分析
                    # 原有的单只股票分析
                    analyze_options = [f"{row['股票名称']} ({row['股票代码']})" 
                                      for _, row in portfolio_df.iterrows()]
                    
                    selected_stock = st.selectbox("选择股票进行深度分析", 
                                                 options=analyze_options,
                                                 key="portfolio_single_stock_select")
                    
                    if selected_stock and st.button("🚀 启动深度分析", type="primary", key="portfolio_single_stock_btn", use_container_width=True):
                        # 提取股票代码
                        import re
                        match = re.search(r'\(([^)]+)\)', selected_stock)
                        if match:
                            selected_code = match.group(1)
                            st.session_state.scan_selected_code = selected_code
                            st.session_state.scan_selected_name = selected_stock.split('(')[0].strip()
                            st.toast(f"🎯 已选中: {selected_stock}，请向上滚动查看作战室", icon="🎯")
                            st.info("👆 **请向上滚动到「⚔️ 深度作战室」（模块①）查看完整AI分析报告**")
    
    except Exception as e:
        st.error(f"❌ 持仓管理加载异常: {str(e)[:100]}")
        logging.error(f"持仓管理异常: {e}")
  except Exception as e:
    st.warning(f"⚠️ 持仓模块异常: {str(e)[:80]}")
    logging.error(f"持仓模块渲染异常: {e}")

# 旧位置不再直接渲染，在主内容区域通过 _render_portfolio_section() 调用

# ═══════════════════════════════════════════════════════════════
# 8. 批量扫描（增强版）
# ═══════════════════════════════════════════════════════════════
def _score_coil(df) -> dict:
    """
    潜伏型评分 — 寻找"尚未启动但蓄势待发"的个股。
    核心逻辑：量缩价稳 + 波动率收缩 + 站上关键均线 + 相对强度良好。

    返回 dict: {score(0-100), signals(list), setup(str)}
    """
    if df is None or len(df) < 60 or "Close" not in df.columns:
        return None
    try:
        df = df.copy()
        close = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        high = df["High"].astype(float)
        low  = df["Low"].astype(float)

        # 均线
        ma20  = close.rolling(20).mean()
        ma50  = close.rolling(50).mean()
        ma200 = close.rolling(200).mean() if len(df) >= 200 else None

        last_c  = float(close.iloc[-1])
        last_v  = float(volume.iloc[-1])
        avg_v20 = float(volume.tail(20).mean())
        avg_v60 = float(volume.tail(60).mean()) if len(df) >= 60 else avg_v20

        # ── 信号1：ATR 收缩（近10日波动 < 近60日均值的70%）
        atr10 = float((high - low).tail(10).mean())
        atr60 = float((high - low).tail(60).mean()) if len(df) >= 60 else atr10
        atr_contracting = atr10 < atr60 * 0.70

        # ── 信号2：成交量萎缩（近10日均量 < 60日均量的75%）—— 机构持仓不动
        vol_drying = float(volume.tail(10).mean()) < avg_v60 * 0.75

        # ── 信号3：价格贴近 MA20（±3%）且 MA20 走平或向上
        near_ma20 = abs(last_c / float(ma20.iloc[-1]) - 1) < 0.03 if float(ma20.iloc[-1]) > 0 else False
        ma20_flat_up = float(ma20.iloc[-1]) >= float(ma20.iloc[-5]) if len(ma20) >= 5 else False

        # ── 信号4：站上 MA50
        above_ma50 = last_c > float(ma50.iloc[-1]) if float(ma50.iloc[-1]) > 0 else False

        # ── 信号5：站上 MA200（长期多头结构）
        above_ma200 = (ma200 is not None and last_c > float(ma200.iloc[-1]) and float(ma200.iloc[-1]) > 0)

        # ── 信号6：60日高低点区间收窄（最近20日区间 < 60日区间的60%）
        range60 = float(high.tail(60).max() - low.tail(60).min())
        range20 = float(high.tail(20).max() - low.tail(20).min())
        range_contracting = (range20 < range60 * 0.60) if range60 > 0 else False

        # ── 信号7：价格处于60日高点的75%-95%（不在顶部，但也不离高点太远）
        h60 = float(high.tail(60).max())
        price_zone = (h60 * 0.75 <= last_c <= h60 * 0.95) if h60 > 0 else False

        # ── 评分
        score = 0
        signals = []
        if atr_contracting:   score += 20; signals.append("🔇 波动收缩")
        if vol_drying:        score += 20; signals.append("📉 量能萎缩")
        if near_ma20 and ma20_flat_up: score += 15; signals.append("📐 贴近MA20")
        if above_ma50:        score += 15; signals.append("✅ 站上MA50")
        if above_ma200:       score += 15; signals.append("🏔 站上MA200")
        if range_contracting: score += 10; signals.append("🎯 区间收窄")
        if price_zone:        score += 5;  signals.append("📍 价格蓄势区")

        setup = "强蓄势" if score >= 70 else ("蓄势中" if score >= 45 else "弱蓄势")
        return {"score": min(100, score), "signals": signals, "setup": setup}
    except Exception:
        return None


def _score_breakout(df) -> dict:
    """
    启动型评分 — 寻找"刚刚突破、已开始启动"的个股。
    核心逻辑：放量突破关键阻力 + 价格站稳 + 非超买区间 + 近日创新高。

    返回 dict: {score(0-100), signals(list), setup(str)}
    """
    if df is None or len(df) < 30 or "Close" not in df.columns:
        return None
    try:
        df = df.copy()
        close  = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        high   = df["High"].astype(float)
        low    = df["Low"].astype(float)
        open_  = df["Open"].astype(float)

        last_c  = float(close.iloc[-1])
        last_v  = float(volume.iloc[-1])
        avg_v20 = float(volume.tail(20).mean())

        # 均线
        ma20  = float(close.rolling(20).mean().iloc[-1])
        ma50  = float(close.rolling(50).mean().iloc[-1]) if len(df) >= 50 else 0
        ma200 = float(close.rolling(200).mean().iloc[-1]) if len(df) >= 200 else 0

        # RSI
        delta = close.diff()
        gain = delta.where(delta > 0, 0).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)
        rsi = float(100 - 100 / (1 + gain.ewm(com=13).mean().iloc[-1] /
                                  (loss.ewm(com=13).mean().iloc[-1] + 1e-10)))

        # ── 信号1：放量突破（今日量 > 20日均量 × 1.5）
        volume_surge = last_v > avg_v20 * 1.5

        # ── 信号2：突破20日/50日/60日新高（最近5日内创新高）
        h20_prev = float(high.iloc[-6:-1].max()) if len(df) >= 6 else 0
        new_high_5d = last_c > h20_prev if h20_prev > 0 else False

        # ── 信号3：突破60日高点（更强信号）
        h60_prev = float(high.iloc[-61:-1].max()) if len(df) >= 61 else 0
        breakout_60d = last_c > h60_prev if h60_prev > 0 else False

        # ── 信号4：收盘在今日区间上75%（非假突破）
        daily_range = float(high.iloc[-1] - low.iloc[-1])
        strong_close = ((last_c - float(low.iloc[-1])) / daily_range > 0.75) if daily_range > 0 else False

        # ── 信号5：站上全部关键均线（MA20/MA50/MA200）
        above_all = last_c > ma20 and (ma50 == 0 or last_c > ma50) and (ma200 == 0 or last_c > ma200)

        # ── 信号6：RSI 在健康区间（55-75），有动能但不超买
        rsi_healthy = 55 <= rsi <= 75

        # ── 信号7：近3日涨幅（3%-15%），已启动但未过热
        ret3 = (last_c / float(close.iloc[-4]) - 1) * 100 if len(df) >= 4 else 0
        started_move = 3.0 <= ret3 <= 15.0

        # ── 评分
        score = 0
        signals = []
        if volume_surge:   score += 25; signals.append(f"🔥 放量{last_v/avg_v20:.1f}x")
        if new_high_5d:    score += 20; signals.append("📈 5日新高")
        if breakout_60d:   score += 15; signals.append("🚀 突破60日高")
        if strong_close:   score += 15; signals.append("💪 强势收盘")
        if above_all:      score += 10; signals.append("✅ 站上三线")
        if rsi_healthy:    score += 10; signals.append(f"📊 RSI{rsi:.0f}健康")
        if started_move:   score += 5;  signals.append(f"⚡ 3日+{ret3:.1f}%")

        setup = "强启动" if score >= 70 else ("启动中" if score >= 45 else "弱启动")
        return {"score": min(100, score), "signals": signals, "setup": setup}
    except Exception:
        return None


# ─── 双通道辅助 ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def _get_benchmark_return(market: str, days: int = 5) -> float:
    """
    拉取基准指数N日收益率，用于相对强弱计算。
    market: 'US' → SPY, 'HK' → ^HSI, 'CN' → 000300.SS
    """
    _BM = {"US": "SPY", "HK": "^HSI", "CN": "000300.SS"}
    ticker = _BM.get(market, "SPY")
    try:
        import yfinance as yf
        df = yf.download(ticker, period="30d", progress=False, auto_adjust=True)
        if df is None or len(df) < days + 1:
            return 0.0
        closes = df["Close"].dropna()
        if len(closes) < days + 1:
            return 0.0
        return float((closes.iloc[-1] / closes.iloc[-(days+1)] - 1) * 100)
    except Exception:
        return 0.0


def _score_inflection(df) -> dict | None:
    """
    拐点通道（赔率）— 三关全中才入池。
    寻找「尚在底部但结构开始改善」的标的。

    Gate1 预期上修代理：
        价格处于6个月区间底部40% AND
        (RSI底背离 OR 近5日正收益 & 近20日跌幅>5%)

    Gate2 结构不再恶化：
        近10日最低点 > 前10日最低点（不再创新低）
        AND 今日未破20日最低收盘

    Gate3 止跌量能改善：
        近10日中上涨日的日均成交量 > 下跌日的日均成交量

    全部通过 → 评分（0-100），否则返回 None。
    """
    if df is None or len(df) < 40 or "Close" not in df.columns:
        return None
    try:
        df = df.copy()
        close  = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        high   = df["High"].astype(float)
        low    = df["Low"].astype(float)

        # RSI
        delta = close.diff()
        gain = delta.where(delta > 0, 0).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)
        rsi = float(100 - 100 / (1 + gain.ewm(com=13).mean().iloc[-1] /
                                  (loss.ewm(com=13).mean().iloc[-1] + 1e-10)))

        last_c = float(close.iloc[-1])
        period = min(126, len(df))   # ~6个月

        # ── Gate1：预期上修代理 ──
        h6m = float(high.tail(period).max())
        l6m = float(low.tail(period).min())
        range6m = h6m - l6m
        pos6m = (last_c - l6m) / range6m if range6m > 0 else 0.5
        in_bottom_40 = pos6m <= 0.40

        ret5  = float(close.iloc[-1] / close.iloc[-6] - 1) * 100  if len(close) >= 6  else 0
        ret20 = float(close.iloc[-1] / close.iloc[-21] - 1) * 100 if len(close) >= 21 else 0

        # RSI底背离：近30日价格创新低但RSI > 前低点时的RSI（简化：近低时RSI > 40）
        recent_low_close = float(close.tail(20).min())
        rsi_at_recent_low_approx = rsi   # 当前RSI作为代理（因为close处于低位）
        rsi_divergence = (recent_low_close <= last_c * 1.02) and (rsi > 40)

        rebound_signal = (ret5 > 0) and (ret20 < -5)
        gate1 = in_bottom_40 and (rsi_divergence or rebound_signal)

        # ── Gate2：结构不再恶化 ──
        if len(low) >= 20:
            low10_recent = float(low.iloc[-10:].min())
            low10_prev   = float(low.iloc[-20:-10].min())
            higher_lows  = low10_recent > low10_prev
        else:
            higher_lows = False

        low20_close = float(close.tail(20).min())
        not_new_low = last_c > low20_close * 0.99   # 允许0.1%误差
        gate2 = higher_lows and not_new_low

        # ── Gate3：止跌量能改善 ──
        recent_10 = df.tail(10).copy()
        up_days   = recent_10[recent_10["Close"] >= recent_10["Open"]]
        down_days = recent_10[recent_10["Close"] <  recent_10["Open"]]
        avg_vol_up   = float(up_days["Volume"].mean())   if len(up_days)   > 0 else 0
        avg_vol_down = float(down_days["Volume"].mean()) if len(down_days) > 0 else 1
        gate3 = avg_vol_up > avg_vol_down

        if not (gate1 and gate2 and gate3):
            return None

        # ── 评分（通过三关后按信号强度打分）──
        score   = 0
        signals = []

        # 价格位置越低赔率越好
        bottom_score = int((0.40 - pos6m) / 0.40 * 30) if pos6m <= 0.40 else 0
        score += bottom_score
        signals.append(f"📍 底部{pos6m*100:.0f}%位")

        if rebound_signal:
            score += 20
            signals.append(f"↩️ 5日+{ret5:.1f}% 20日{ret20:.1f}%")
        if rsi_divergence:
            score += 15
            signals.append(f"📈 RSI底背离{rsi:.0f}")
        if higher_lows:
            score += 20
            signals.append("🔼 高低点抬升")
        vol_ratio = avg_vol_up / avg_vol_down if avg_vol_down > 0 else 1
        score += min(15, int(vol_ratio * 5))
        signals.append(f"💰 买量/卖量={vol_ratio:.1f}x")

        setup = "强拐点" if score >= 65 else ("拐点中" if score >= 45 else "弱拐点")
        return {"score": min(100, score), "signals": signals, "setup": setup,
                "gate1": gate1, "gate2": gate2, "gate3": gate3,
                "pos6m": pos6m, "ret5": ret5, "ret20": ret20, "rsi": rsi}
    except Exception:
        return None


def _score_breakout_v2(df, benchmark_ret5: float = 0.0) -> dict | None:
    """
    启动通道（胜率）— 三信号满足≥2/3才入池。

    Signal1 突破关键位：收盘 > 过去20日最高收盘价
    Signal2 量能确认：今日量 > 20日均量 × 1.5
    Signal3 相对强弱转强：个股5日涨幅 > 基准5日涨幅 + 2%
    """
    if df is None or len(df) < 25 or "Close" not in df.columns:
        return None
    try:
        df = df.copy()
        close  = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        high   = df["High"].astype(float)
        low    = df["Low"].astype(float)

        last_c  = float(close.iloc[-1])
        last_v  = float(volume.iloc[-1])
        avg_v20 = float(volume.tail(20).mean())

        # RSI
        delta = close.diff()
        gain = delta.where(delta > 0, 0).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)
        rsi  = float(100 - 100 / (1 + gain.ewm(com=13).mean().iloc[-1] /
                                   (loss.ewm(com=13).mean().iloc[-1] + 1e-10)))

        # Signal1：突破20日最高收盘
        high20_prev = float(close.iloc[-21:-1].max()) if len(close) >= 21 else float(close.iloc[:-1].max())
        s1_breakout = last_c > high20_prev
        s1_margin   = (last_c / high20_prev - 1) * 100 if high20_prev > 0 else 0

        # Signal2：放量
        s2_volume   = last_v > avg_v20 * 1.5
        s2_ratio    = last_v / avg_v20 if avg_v20 > 0 else 1

        # Signal3：相对强弱
        ret5 = float((close.iloc[-1] / close.iloc[-6] - 1) * 100) if len(close) >= 6 else 0
        s3_rs = ret5 > benchmark_ret5 + 2.0

        met = sum([s1_breakout, s2_volume, s3_rs])
        if met < 2:
            return None

        # 强势收盘（加分项）
        daily_range = float(high.iloc[-1] - low.iloc[-1])
        strong_close = ((last_c - float(low.iloc[-1])) / daily_range > 0.70) if daily_range > 0 else False

        # RSI 健康区间
        rsi_ok = 50 <= rsi <= 78

        # 评分
        score   = 0
        signals = []

        if s1_breakout:
            score += 35
            signals.append(f"🚀 突破+{s1_margin:.1f}%")
        if s2_volume:
            score += 30
            signals.append(f"🔥 量{s2_ratio:.1f}x")
        if s3_rs:
            score += 25
            signals.append(f"💪 RS+{ret5-benchmark_ret5:.1f}%")
        if strong_close:
            score += 5
            signals.append("⬆️ 强收盘")
        if rsi_ok:
            score += 5
            signals.append(f"RSI{rsi:.0f}")

        setup = "强启动" if score >= 70 else ("启动中" if score >= 50 else "弱启动")
        return {"score": min(100, score), "signals": signals, "setup": setup,
                "s1": s1_breakout, "s2": s2_volume, "s3": s3_rs,
                "met": met, "ret5": ret5, "rsi": rsi}
    except Exception:
        return None


def _gen_rationale(df, code: str, name: str, channel: str, result: dict) -> str:
    """
    生成每只股票的一行理由：
    变量 → 预期差 → 价格位置 → 验证窗口
    """
    try:
        close = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        last_c = float(close.iloc[-1])
        ma20   = float(close.rolling(20).mean().iloc[-1])
        ma50   = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else 0
        h52w   = float(df["High"].tail(252).max()) if len(df) >= 252 else float(df["High"].max())
        dist_h = (last_c / h52w - 1) * 100 if h52w > 0 else 0

        if channel == "INFLECTION":
            pos6m  = result.get("pos6m", 0.5)
            ret5   = result.get("ret5",  0)
            ret20  = result.get("ret20", 0)
            rsi    = result.get("rsi",   50)
            # 变量
            var_part = "量能回升+低点抬高" if result.get("gate3") else "结构企稳"
            # 预期差
            exp_part = f"市场仍在恐慌（RSI{rsi:.0f}），但买量已>{1:.0f}x卖量" if rsi < 45 else f"底部{pos6m*100:.0f}%位反弹{ret5:+.1f}%"
            # 价格位置
            pos_part = f"现价{last_c:.2f}，距MA20 {(last_c/ma20-1)*100:+.1f}%"
            # 验证窗口
            ver_part = "3-5日内需站上MA20确认"
            return f"变量:{var_part} → 预期差:{exp_part} → 价格:{pos_part} → 验证:{ver_part}"

        else:  # BREAKOUT
            ret5   = result.get("ret5", 0)
            rsi    = result.get("rsi",  60)
            met    = result.get("met",  2)
            sig_n  = "三信号共振" if met == 3 else "双信号确认"
            var_part = sig_n + "放量突破"
            exp_part = f"市场未追，距52周高{dist_h:.1f}%" if dist_h < -5 else "接近历史高位突破"
            pos_part = f"现价{last_c:.2f}，突破后RSI{rsi:.0f}"
            ver_part = "48h内需维持在突破位上方"
            return f"变量:{var_part} → 预期差:{exp_part} → 价格:{pos_part} → 验证:{ver_part}"
    except Exception:
        return "数据计算中"


def batch_scan_dual(pool, market: str = "US", progress_callback=None) -> dict:
    """
    双通道扫描：同时运行拐点通道 + 启动通道，各取Top10。

    返回:
        {
          "inflection": [...top10],   # 拐点Top10
          "breakout":   [...top10],   # 启动Top10
          "stats":      {...}
        }
    """
    bm_ret5 = _get_benchmark_return(market, days=5)

    inflection_pool = []
    breakout_pool   = []
    stats = {"success": 0, "failed": 0, "total": len(pool)}

    for idx, item in enumerate(pool):
        try:
            if progress_callback:
                progress_callback(idx + 1, len(pool), item[1] if len(item) > 1 else "")

            code, name = item[0], item[1] if len(item) > 1 else item[0]
            yf_code = item[2] if len(item) > 2 else code
            c_fixed = to_yf_cn_code(yf_code) if yf_code == code else yf_code

            df = fetch_stock_data(c_fixed)
            if df is None or len(df) < 30 or "Close" not in df.columns:
                stats["failed"] += 1
                continue
            if not (0 < float(df["Close"].iloc[-1]) < 1_000_000):
                stats["failed"] += 1
                continue

            stats["success"] += 1

            # 拐点通道
            inf_r = _score_inflection(df)
            if inf_r:
                rationale = _gen_rationale(df, code, name, "INFLECTION", inf_r)
                inflection_pool.append({
                    "股票": name, "代码": code,
                    "行业": get_sector(code, name),
                    "得分": inf_r["score"],
                    "形态": inf_r["setup"],
                    "信号": " ".join(inf_r["signals"][:3]),
                    "理由": rationale,
                    "现价": f"{float(df['Close'].iloc[-1]):.2f}",
                })

            # 启动通道
            bo_r = _score_breakout_v2(df, benchmark_ret5=bm_ret5)
            if bo_r:
                rationale = _gen_rationale(df, code, name, "BREAKOUT", bo_r)
                breakout_pool.append({
                    "股票": name, "代码": code,
                    "行业": get_sector(code, name),
                    "得分": bo_r["score"],
                    "形态": bo_r["setup"],
                    "信号": " ".join(bo_r["signals"][:3]),
                    "理由": rationale,
                    "现价": f"{float(df['Close'].iloc[-1]):.2f}",
                })

        except Exception as e:
            stats["failed"] += 1
            _safe_print(f"[双通道] ❌ {item[0]}: {type(e).__name__}: {str(e)[:60]}")

    inflection_top10 = sorted(inflection_pool, key=lambda x: x["得分"], reverse=True)[:10]
    breakout_top10   = sorted(breakout_pool,   key=lambda x: x["得分"], reverse=True)[:10]

    _safe_print(f"[双通道] {market} 完成 ✅{stats['success']} ❌{stats['failed']} | 拐点{len(inflection_pool)} 启动{len(breakout_pool)}")
    return {"inflection": inflection_top10, "breakout": breakout_top10,
            "stats": stats, "bm_ret5": bm_ret5}


def batch_scan_analysis(pool, scan_type="TOP", ma_target=None, progress_callback=None):
    """
    批量扫描股票。

    scan_type:
        TOP        — 趋势强势（原有逻辑）
        MA_TOUCH   — 均线回踩（原有逻辑）
        COIL       — 潜伏蓄势（量缩价稳，等待启动）
        BREAKOUT   — 刚刚启动（放量突破，已开始上涨）
    """
    results = []
    stats = {
        'success': 0,
        'failed': 0,
        'errors': []
    }
    
    total_stocks = len(pool)
    
    # 【V91.7】使用统一行业映射模块（sector_map.py），全682只覆盖，单一数据源
    from modules.sector_map import get_sector
    
    for idx, item in enumerate(pool):
        # 【V87.14】调用进度回调
        if progress_callback:
            progress_callback(idx + 1, total_stocks, item[1] if len(item) > 1 else item[0])
        
        # 【V84.3】每个股票都用try-except包裹，防止单个错误中断整个扫描
        try:
            code = item[0]
            name = item[1]
            # 【V82.9关键修复】如果pool有3个元素，直接使用第3个（已经是正确的yfinance格式）
            if len(item) >= 3:
                c_fixed = item[2]
            else:
                c_fixed = to_yf_cn_code(code)
            
            # 【V87.4】优化请求间隔 - 减少延迟提高速度
            if idx > 0 and idx % 20 == 0:  # 改为每20个股票延迟
                time.sleep(0.2)  # 减少延迟时间
            
            df = fetch_stock_data(c_fixed)
            
            # 【V87.8】增强数据验证和详细日志
            if df is None or df.empty:
                stats['failed'] += 1
                # 【V87.8】详细记录失败原因
                error_msg = f'数据获取失败（代码:{c_fixed}, 原始:{code}）'
                stats['errors'].append({
                    'code': code,
                    'name': name,
                    'error': error_msg
                })
                # 【V87.8】打印详细日志帮助诊断
                _safe_print(f"[扫描失败] ❌ {name} ({code}) -> yfinance代码: {c_fixed}")
                _safe_print(f"           原因: 返回空数据或None")
                continue
            
            # 【V87.4】数据质量检查 - 确保有足够的数据点
            if len(df) < 20:  # 至少需要20个交易日的数据
                stats['failed'] += 1
                error_msg = f'数据不足（仅{len(df)}条记录）'
                stats['errors'].append({
                    'code': code,
                    'name': name,
                    'error': error_msg
                })
                continue
            
            # 【V87.4】价格数据合理性检查
            current_price = df['Close'].iloc[-1]
            if current_price <= 0 or current_price > 100000:  # 价格范围检查
                stats['failed'] += 1
                error_msg = f'价格异常（{current_price}）'
                stats['errors'].append({
                    'code': code,
                    'name': name,
                    'error': error_msg
                })
                _safe_print(f"[扫描] ❌ {code} ({name}) {error_msg}")
                continue
            
            if len(df) < 20:
                stats['failed'] += 1
                error_msg = f'数据不足（仅{len(df)}条，需要>20条）'
                stats['errors'].append({
                    'code': code,
                    'name': name,
                    'error': error_msg
                })
                # 【V85】只在控制台打印数据不足信息
                _safe_print(f"[扫描] ⚠️ {code} ({name}) {error_msg}")
                continue
            
            # 数据有效，继续处理
            if df is not None:
                m = calculate_metrics_all(df, c_fixed)
            if m:
                is_hit = False
                
                if scan_type == "TOP":
                    if m['score'] > 40: is_hit = True
                elif scan_type == "COIL":
                    _coil = _score_coil(df)
                    if _coil and _coil['score'] >= 45:
                        m['_special_score'] = _coil['score']
                        m['_special_signals'] = _coil['signals']
                        m['_special_setup']   = _coil['setup']
                        is_hit = True
                elif scan_type == "BREAKOUT":
                    _bo = _score_breakout(df)
                    if _bo and _bo['score'] >= 45:
                        m['_special_score'] = _bo['score']
                        m['_special_signals'] = _bo['signals']
                        m['_special_setup']   = _bo['setup']
                        is_hit = True
                elif scan_type == "MA_TOUCH" and ma_target:
                    # 【V86优化】不同均线使用不同的评分要求和容差
                    # MA30短线：评分>50，容差2%（更严格，只抓真正触碰的）
                    # MA60季线：评分>45，容差3%（中等严格）
                    # MA120半年：评分>40，容差5%（相对宽松）
                    if ma_target == 30:
                        min_score, tolerance = 50, 0.02
                    elif ma_target == 60:
                        min_score, tolerance = 45, 0.03
                    elif ma_target == 120:
                        min_score, tolerance = 40, 0.05
                    else:
                        min_score, tolerance = 45, 0.05
                    
                    if m['score'] > min_score:
                        ma_col = f'MA{ma_target}'
                        if ma_col in m['df'].columns:
                            ma_val = m['df'][ma_col].iloc[-1]
                            last_low = m['last']['Low']
                            last_high = m['last']['High']
                            last_close = m['last']['Close']
                            
                            # 【V86】严格判断：当日K线必须触及均线，或收盘价在容差范围内
                            touched_ma = (last_low <= ma_val <= last_high)  # K线实体触及均线
                            close_to_ma = (abs(last_close - ma_val) / ma_val < tolerance if ma_val > 0 else False)
                            
                            # 【V86】打印调试信息
                            if touched_ma or close_to_ma:
                                distance_pct = abs(last_close - ma_val) / ma_val * 100 if ma_val > 0 else 0
                                _safe_print(f"[MA{ma_target}扫描] ✅ {code} ({name}): 距MA{ma_target}={distance_pct:.2f}%, 评分={m['score']}")
                            
                            if touched_ma or close_to_ma:
                                is_hit = True
                
                if is_hit:
                    # 【V87.12】优化趋势判断 - 结合评分和技术指标
                    score = m['score']
                    ma200 = m['last'].get('MA200', 0)
                    rsi = m['rsi']
                    
                    # 长期趋势：综合评分 + 年线位置
                    if score >= 75 and ma200 > 0 and m['last_price'] > ma200:
                        long_term = "📈 多头"
                    elif score < 50 or (ma200 > 0 and m['last_price'] < ma200 * 0.9):
                        long_term = "📉 空头"
                    else:
                        long_term = "➡️ 震荡"
                    
                    # 短期趋势：综合评分 + RSI
                    if score >= 75 and rsi > 60:
                        short_term = "📈 强势"
                    elif score >= 75 and rsi > 70:
                        short_term = "🔥 超买"
                    elif score < 50 or rsi < 40:
                        short_term = "📉 弱势"
                    elif rsi < 30:
                        short_term = "❄️ 超卖"
                    else:
                        short_term = "➡️ 中性"
                    
                    # 资金状态（根据成交量）
                    if len(m['df']) >= 5:
                        vol_ma5 = m['df']['Volume'].tail(5).mean()
                        last_vol = m['last']['Volume']
                        if last_vol > vol_ma5 * 1.5:
                            capital = "💰 放量"
                        elif last_vol > vol_ma5:
                            capital = "📊 正常"
                        else:
                            capital = "📉 缩量"
                    else:
                        capital = "➖"
                    
                    # 【V82.10新增】水位 - 显示离最高点和最低点的百分比
                    l250 = m['df']['Low'].tail(250).min() if len(m['df']) >= 250 else m['df']['Low'].min()
                    h250 = m['df']['High'].tail(250).max() if len(m['df']) >= 250 else m['df']['High'].max()
                    if h250 > l250:
                        # 离最高点的百分比（负数表示低于最高点）
                        from_high_pct = (m['last_price'] - h250) / h250 * 100
                        # 离最低点的百分比（正数表示高于最低点）
                        from_low_pct = (m['last_price'] - l250) / l250 * 100
                        water_level = f"高{from_high_pct:+.1f}% 低{from_low_pct:+.1f}%"
                    else:
                        water_level = "➖"
                    
                    _display_score = m.get('_special_score', m['score'])
                    _signals_str   = " ".join(m.get('_special_signals', []))
                    _setup_str     = m.get('_special_setup', m['suggestion'])
                    results.append({
                        "股票": name,
                        "代码": code,
                        "行业": get_sector(code, name),
                        "得分": _display_score,
                        "ESG": f"{m.get('esg_total', 0)} ({m.get('esg_grade', 'N/A')})",
                        "长期": long_term,
                        "短期": short_term,
                        "建议": _setup_str if scan_type in ("COIL", "BREAKOUT") else m['suggestion'],
                        "策略": _signals_str if scan_type in ("COIL", "BREAKOUT") else m['logic'],
                        "资金": capital,
                        "水位": water_level,
                        "现价": f"{m['last_price']:.2f}"
                    })
                    stats['success'] += 1
        
        except Exception as e:
            # 【V84.3】捕获异常，记录错误但不中断扫描
            stats['failed'] += 1
            error_msg = f"{type(e).__name__}: {str(e)[:80]}"
            stats['errors'].append({
                'code': item[0] if item else 'Unknown',
                'name': item[1] if len(item) > 1 else 'Unknown',
                'error': error_msg
            })
            _safe_print(f"[扫描] ❌ {item[0]} ({item[1] if len(item) > 1 else ''}) 失败: {error_msg}")
    
    _top_n = 30 if scan_type in ("TOP", "COIL", "BREAKOUT") else 100
    sorted_results = sorted(results, key=lambda x:x['得分'], reverse=True)[:_top_n]
    
    # 【V85】扫描结束后,打印失败统计
    _safe_print(f"[扫描] 扫描完成: ✅ 成功 {stats['success']} 只 | ❌ 失败 {stats['failed']} 只")
    if stats['errors']:
        _safe_print(f"[扫描] 失败详情:")
        for err in stats['errors'][:10]:  # 只打印前10个
            _safe_print(f"  ❌ {err['code']} ({err['name']}): {err['error']}")
    
    return sorted_results, stats


# ═══════════════════════════════════════════════════════════════
# 8b. 【Regime-Adaptive】市场状态自适应扫描
# ═══════════════════════════════════════════════════════════════
def run_regime_scan(pool, use_concurrent, scan_market, risk_preference="平衡", progress_callback=None):
    """
    市场状态自适应筛选：先判 regime，再策略分流，再给动作建议
    返回增强结果：含 动作标签、机会概率、风险概率、建议仓位、失效条件
    progress_callback(current, total, stock_name)：必须有进度百分比
    """
    if not REGIME_ENGINE_AVAILABLE:
        # 降级：使用旧综合评分（带进度）
        res, stats = batch_scan_analysis(pool, scan_type="TOP", ma_target=None, progress_callback=progress_callback)
        return res, stats, None, {"regime": "N/A", "fallback": True}

    results = []
    stats = {'success': 0, 'failed': 0, 'errors': []}
    breadth_above = 0
    breadth_total = 0

    # 1. 获取指数数据，计算 regime
    index_code = "^GSPC" if scan_market == "美股" else ("^HSI" if scan_market == "港股" else "000001.SS")
    index_df = fetch_stock_data(index_code)
    vix_df = fetch_stock_data("^VIX") if scan_market == "美股" else None
    vix_proxy = 20.0
    if vix_df is not None and len(vix_df) > 0:
        vix_proxy = float(vix_df["Close"].iloc[-1])

    mr = MarketRegime(vix_proxy=vix_proxy)
    regime_info = mr.evaluate(index_df, 0, 1)
    regime = regime_info["regime"]

    # 2. get_sector（【V91.7】与 batch_scan_analysis 统一使用 sector_map，全682只覆盖，避免❓其他）
    from modules.sector_map import get_sector

    router = StrategyRouter()
    classifier = OpportunityClassifier()
    risk_fc = RiskForecaster()
    action_eng = ActionEngine()
    quality_guard = QualityGuard()
    composer = ReportComposer()
    gap_engine = ExpectationGapEngine() if (USE_POTENTIAL_ENGINE and ExpectationGapEngine) else None
    long_compound_gate = LongCompounderGate() if LongCompounderGate else None
    margin_gate = MarginOfSafetyGate() if MarginOfSafetyGate else None

    total = len(pool)
    for idx, item in enumerate(pool):
        # 进度回调：必须有百分比
        if progress_callback:
            progress_callback(idx + 1, total, item[1] if len(item) > 1 else item[0])
        try:
            code = item[0]
            name = item[1]
            c_fixed = item[2] if len(item) >= 3 else to_yf_cn_code(code)

            df = fetch_stock_data(c_fixed)
            if df is None or df.empty or len(df) < 20:
                stats['failed'] += 1
                continue

            m = calculate_metrics_all(df, c_fixed)
            score_threshold = 35 if (USE_POTENTIAL_ENGINE and gap_engine) else 40
            if not m or m['score'] <= score_threshold:
                continue

            last = m['last']
            last_price = m['last_price']

            #  breadth 统计
            above_ma20 = last_price > last.get('MA20', 0)
            if above_ma20:
                breadth_above += 1
            breadth_total += 1

            # 水位（统一计算）
            if REGIME_ENGINE_AVAILABLE:
                pos_level, pos_pct = get_position_level_unified(m['df'], last_price)
            else:
                l250 = m['df']['Low'].tail(250).min() if len(m['df']) >= 250 else m['df']['Low'].min()
                h250 = m['df']['High'].tail(250).max() if len(m['df']) >= 250 else m['df']['High'].max()
                pos_pct = (last_price - l250) / (h250 - l250) * 100 if h250 > l250 else 50
                pos_level = "高" if pos_pct >= 75 else ("中" if pos_pct >= 35 else "低")

            # QualityGuard
            qr = quality_guard.validate(
                industry=get_sector(code, name),
                score_total=m['score'],
                position_level=pos_level,
                position_percentile=pos_pct,
            )
            if not qr["pass"] and qr["data_quality_flag"] == "FAIL":
                continue

            # feature_vector
            fv = {
                "score": m['score'],
                "rsi": m['rsi'],
                "above_ma20": last_price > last.get('MA20', 0),
                "above_ma60": last_price > last.get('MA60', 0),
                "above_ma120": last_price > last.get('MA120', 0),
                "vol_ratio": last['Volume'] / m['df']['Volume'].tail(20).mean() if len(m['df']) >= 20 else 1,
                "drawdown_20d": 1 - last_price / m['df']['High'].tail(20).max() if len(m['df']) >= 20 else 0,
                "momentum_5d": (last_price - m['df']['Close'].iloc[-6]) / m['df']['Close'].iloc[-6] if len(m['df']) >= 6 else 0,
            }

            # StrategyRouter（质量引擎）
            route_res = router.route(regime, regime_info["confidence"], fv)
            quality_score = route_res["regime_adjusted_score"]

            # ExpectationGapEngine（潜力引擎，双引擎模式）
            gap_result = None
            sector_raw = get_sector(code, name)
            if gap_engine:
                gap_result = gap_engine.compute(m['df'], c_fixed, sector_raw)

            # 双引擎融合 / 单引擎
            if gap_engine and gap_result:
                dual_res = router.route_dual_engine(regime, quality_score, gap_result["potential_score"])
                final_score = dual_res["final_score"]
            else:
                final_score = quality_score

            # 【长线法宝】LongCompounderGate + MarginOfSafetyGate
            long_compound_result = long_compound_gate.compute(m['df'], c_fixed, sector_raw) if long_compound_gate else {}
            margin_result = margin_gate.compute(m['df'], gap_result, long_compound_result) if margin_gate else {}
            allows_long_core = margin_result.get("allows_long_core", True)

            # 价值陷阱硬过滤
            vt_check = {}
            if margin_gate:
                vt_check = MarginOfSafetyGate.check_value_trap(sector_raw, m.get('logic', ''), m.get('suggestion', ''))
            if vt_check.get("is_value_trap"):
                stats['failed'] += 1
                continue

            # OpportunityClassifier（按质量分做动作分类，LONG_CORE 需 allows_long_core）
            cl_res = classifier.classify(regime, int(quality_score), fv, qr["pass"], allows_long_core=allows_long_core)
            action_label = cl_res["action_label"]
            action_emoji = cl_res["action_emoji"]

            # RiskForecaster
            risk_probs = risk_fc.forecast(m['df'], last, regime)

            # ActionEngine
            df_tr = m['df']
            df_tr['TR'] = np.maximum((df_tr['High'] - df_tr['Low']), 
                np.maximum(abs(df_tr['High'] - df_tr['Close'].shift(1)), abs(df_tr['Low'] - df_tr['Close'].shift(1))))
            atr = float(df_tr['TR'].rolling(14).mean().iloc[-1]) if len(df_tr) >= 14 else 0
            action_res = action_eng.compute(
                action_label, risk_probs, risk_preference,
                atr, last_price
            )

            # 资金状态
            vol_ma5 = m['df']['Volume'].tail(5).mean() if len(m['df']) >= 5 else last['Volume']
            if last['Volume'] > vol_ma5 * 1.5:
                capital = "💰 放量"
            elif last['Volume'] > vol_ma5:
                capital = "📊 正常"
            else:
                capital = "📉 缩量"

            # 长期/短期
            score = m['score']
            ma200 = last.get('MA200', 0)
            if score >= 75 and ma200 > 0 and last_price > ma200:
                long_term = "📈 多头"
            elif score < 50 or (ma200 > 0 and last_price < ma200 * 0.9):
                long_term = "📉 空头"
            else:
                long_term = "➡️ 震荡"

            if score >= 75 and m['rsi'] > 60:
                short_term = "📈 强势"
            elif score < 50 or m['rsi'] < 40:
                short_term = "📉 弱势"
            else:
                short_term = "➡️ 中性"

            # 个股概况硬事实：行业/得分/水位（【V91.0】不再对行业未匹配刷屏WARN，仅显示行业名）
            ff = qr.get("field_flags", {})
            sector_display = sector_raw  # 直接显示行业，不叠加 [WARN]
            water_str = f"{pos_level}-{pos_pct:.1f}%"
            if ff.get("position") == "WARN":
                water_str = f"{water_str} [WARN]"
            qa_parts = []
            # 仅对得分/水位异常标注，行业未匹配不再刷屏
            if ff.get("industry") == "FAIL":
                qa_parts.append("行业缺失")
            if ff.get("score") == "WARN":
                qa_parts.append("得分待核")
            if ff.get("score") == "FAIL":
                qa_parts.append("得分异常")
            if ff.get("position") == "WARN":
                qa_parts.append("水位待核")
            qa_label = " | ".join(qa_parts) if qa_parts else "OK"

            # 三池分类：A=已验证强势 B=预期差潜力 C=左侧观察
            passes_potential = gap_result and gap_result.get("passes_potential_gate", False)
            pot_score = gap_result.get("potential_score", 0) if gap_result else 0
            if passes_potential and action_label in (classifier.BUILD_NOW, classifier.FOLLOW_MID, classifier.LONG_CORE):
                pool_assignment = "B"
            elif passes_potential and action_label == classifier.FILTERED:
                pool_assignment = "C"
            else:
                pool_assignment = "A"

            # 预期差等级 A/B/C
            potential_gap_grade = "A" if pot_score >= 70 else ("B" if pot_score >= 50 else "C")
            potential_tags = (gap_result.get("potential_tags", []) or [])[:3] if gap_result else []

            # 【长线法宝】8项强制解释 + 持有期/仓位上限绑定
            eight_mandatory = {}
            if long_compound_gate and margin_gate and gap_result:
                eight_mandatory = composer.compose_eight_mandatory(
                    gap_result, long_compound_result, margin_result, action_res,
                    name, sector_raw, action_label
                )

            results.append({
                "股票": name, "代码": code, "行业": sector_display,
                "得分": m['score'], "ESG": f"{m.get('esg_total', 0)} ({m.get('esg_grade', 'N/A')})",
                "硬事实校验": qa_label,
                "长期": long_term, "短期": short_term, "建议": m['suggestion'],
                "策略": m['logic'], "资金": capital, "水位": water_str,
                "现价": f"{last_price:.2f}",
                "动作标签": f"{action_emoji} {action_label}",
                "机会概率": f"{risk_probs['p_up_continuation']*100:.0f}%",
                "风险概率": f"{risk_probs['p_drawdown']*100:.0f}%",
                "建议仓位": action_res["suggested_position_range"],
                "分批节奏": action_res["tranche_plan"],
                "持有期": action_res.get("holding_period", "N/A"),
                "仓位上限": f"{action_res.get('position_cap_percent', 0)}%",
                "失效条件": action_res["invalidation_rules"][0] if action_res["invalidation_rules"] else "N/A",
                "regime_adjusted_score": final_score,
                "pool_assignment": pool_assignment,
                "potential_gap_grade": potential_gap_grade,
                "potential_tags": potential_tags,
                "potential_score": pot_score,
                "quality_score": quality_score,
                "long_compounder_score": long_compound_result.get("long_compounder_score", 0),
                "expectation_gap_score": gap_result.get("expectation_gap_score", pot_score) if gap_result else pot_score,
                "eight_mandatory": eight_mandatory,
                "potential_four_sentences": composer.compose_potential_four_sentences(
                    gap_result, name, sector_raw, action_label
                ) if (gap_result and passes_potential) else [],
                "battle_room": composer.compose_battle_room(
                    regime_info, risk_probs, action_label, action_emoji,
                    action_res, qr["data_quality_flag"]
                ),
            })
            stats['success'] += 1
        except Exception as e:
            stats['failed'] += 1
            stats['errors'].append({'code': item[0], 'name': item[1] if len(item) > 1 else '', 'error': str(e)[:80]})

    regime_info["breadth_above"] = breadth_above
    regime_info["breadth_total"] = breadth_total
    regime_info = mr.evaluate(index_df, breadth_above, max(1, breadth_total))

    top_n = 50 if (USE_POTENTIAL_ENGINE and ExpectationGapEngine) else 30
    sorted_results = sorted(results, key=lambda x: x.get('regime_adjusted_score', x['得分']), reverse=True)[:top_n]
    from zoneinfo import ZoneInfo
    ts_str = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S") + " CST"
    return sorted_results, stats, regime_info, {
        "regime": regime,
        "confidence": regime_info["confidence"],
        "scan_timestamp": ts_str,
        "use_potential_engine": USE_POTENTIAL_ENGINE and bool(ExpectationGapEngine),
    }


# ═══════════════════════════════════════════════════════════════
# 8c. 【V91.9】AI选股 - Gemini 筛选短中长期好股，中美港各 Top3
# ═══════════════════════════════════════════════════════════════
def run_ai_stock_selector(progress_callback=None):
    """
    一键AI选股：扫描中美港三市场，取每市场前15只候选，由Gemini选出各市场Top3，
    输出：理由、背景、增长点（短中长期）
    返回: (result_dict, error_msg)
    result_dict: {'us': [], 'hk': [], 'cn': [], 'ai_report': str}
    """
    def _update(msg):
        if progress_callback:
            progress_callback(msg)
    
    result = {'us': [], 'hk': [], 'cn': [], 'ai_report': ''}
    
    # 1. 三市场并行扫描，各取 Top15 候选
    markets_data = [
        ("美股", RAW_US),
        ("港股", RAW_HK),
        ("A股", RAW_CN_TOP),
    ]
    
    all_candidates = {}
    for idx, (market_name, pool) in enumerate(markets_data):
        def _make_progress(mkt):
            def _cb(c, t, name):
                _update(f"正在扫描 {mkt}... {c}/{t} {name[:12]}")
            return _cb
        _update(f"正在扫描 {market_name}...")
        try:
            res, stats = batch_scan_analysis(pool, scan_type="TOP", ma_target=None, progress_callback=_make_progress(market_name))
            # 按得分排序，取前15
            sorted_res = sorted(res, key=lambda x: x.get('得分', 0), reverse=True)[:15]
            all_candidates[market_name] = sorted_res
        except Exception as e:
            logging.error(f"AI选股扫描 {market_name} 失败: {e}")
            all_candidates[market_name] = []
    
    # 2. 构建 Gemini 输入
    _update("正在构建 AI 分析数据...")
    prompt_data = []
    for mkt, candidates in all_candidates.items():
        if not candidates:
            prompt_data.append(f"\n【{mkt}】无有效候选")
            continue
        lines = [f"\n【{mkt}】"]
        for i, r in enumerate(candidates[:15], 1):
            name = r.get('股票', r.get('名称', 'N/A'))
            code = r.get('代码', 'N/A')
            score = r.get('得分', 0)
            sector = r.get('行业', 'N/A')
            suggestion = r.get('建议', '')[:80]
            lines.append(f"  {i}. {name}({code}) 得分:{score} 行业:{sector} 建议:{suggestion}")
        prompt_data.append("\n".join(lines))
    
    input_summary = "\n".join(prompt_data)
    
    # 3. 调用 Gemini
    _update(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · AI选股...")
    prompt = f"""你是顶级量化分析师，根据以下三市场量化扫描候选（每市场前15只，按得分排序），为每个市场选出 **Top 3 最值得关注** 的股票。

【候选数据】
{input_summary}

【任务要求】
对每个市场（美股、港股、A股）各选出 Top 3 只股票，综合短中长期考量。对每只股票必须输出：
1. **选股理由**：为何入选，核心逻辑（1-2句）
2. **背景概况**：公司/行业背景（1-2句）
3. **增长点**：分别说明短期(1-4周)、中期(1-3月)、长期(3-12月)主要增长驱动

【输出格式】（严格按以下 Markdown 结构，便于解析）
## 🇺🇸 美股 Top3
### 1. [股票名](代码)
- **理由**：...
- **背景**：...
- **增长点**：短期... | 中期... | 长期...

### 2. ...
### 3. ...

## 🇭🇰 港股 Top3
### 1. ...
### 2. ...
### 3. ...

## 🇨🇳 A股 Top3
### 1. ...
### 2. ...
### 3. ...

要求：内容专业、具体、可操作，每只股票分析 80-150 字。"""
    
    ai_report = ""
    if MY_GEMINI_KEY:
        try:
            ai_report = call_gemini_api(prompt)
            if ai_report.startswith("❌"):
                return result, ai_report
        except Exception as e:
            err = f"❌ Gemini 调用失败: {type(e).__name__}: {str(e)[:80]}"
            logging.error(err)
            return result, err
    else:
        return result, "❌ 未配置 Gemini API Key"
    
    result['ai_report'] = ai_report or "无输出"
    
    # 4. 简单解析：提取每市场 Top3 代码（用于匹配表格）
    import re
    for mkt_tag, mkt_key in [("美股", "us"), ("港股", "hk"), ("A股", "cn")]:
        candidates = all_candidates.get(mkt_tag, [])
        if not candidates:
            continue
        # 从 AI 报告中提取提到的股票名
        for r in candidates[:5]:  # 只看前5，AI 通常从里面选
            name = r.get('股票', r.get('名称', ''))
            if name and name in ai_report:
                result[mkt_key].append(r)
                if len(result[mkt_key]) >= 3:
                    break
    
    return result, None


# ═══════════════════════════════════════════════════════════════
# 8d. 【自选股分析】按中美港划分，逐只分析：催化、技术面、风险、操作建议
# ═══════════════════════════════════════════════════════════════
def _get_watchlist_price(code):
    """获取自选股现价（fetch_stock_data 内部会做 to_yf_cn_code 转换）"""
    try:
        df = fetch_stock_data(code)
        if df is not None and len(df) > 0 and "Close" in df.columns:
            return float(df["Close"].iloc[-1])
    except Exception:
        pass
    return None

def run_watchlist_analysis(progress_callback=None):
    """
    自选股分析：按中美港划分，对每只逐只给出近期催化、技术面、风险点、操作建议。
    返回: (ai_report_str, error_msg)
    """
    def _update(msg):
        if progress_callback:
            progress_callback(msg)
    
    # 1. 获取现价
    _update("正在获取自选股现价...")
    price_lines = []
    for mkt, pfx, key in [("美股", "$", "US"), ("港股", "HK$", "HK"), ("A股", "¥", "CN")]:
        lines = [f"\n【{mkt}】"]
        for code, name in WATCHLIST.get(key, []):
            p = _get_watchlist_price(code)
            s = f"  {name}({code}): {pfx}{p:.2f}" if p is not None else f"  {name}({code}): 数据获取中"
            lines.append(s)
        price_lines.append("\n".join(lines))
    
    input_data = "\n".join(price_lines)
    
    # 2. 调用 Gemini
    _update(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · 自选股分析...")
    prompt = f"""你是顶级量化分析师，对以下用户的跨账户自选股进行逐只分析。

【自选股及现价】
{input_data}

【任务要求】
按中美港划分，对每只自选股**逐只**给出：
1. **近期催化**：24-72h 内可能影响股价的事件或数据
2. **技术面**：关键支撑/压力、趋势判断
3. **风险点**：1-2 条主要风险
4. **操作建议**：持有/加仓/减仓/观望（简洁可执行）

【输出格式】（严格按以下 Markdown 结构）
## 🇺🇸 美股自选
### 1. [股票名](代码)
- **催化**：...
- **技术面**：...
- **风险**：...
- **建议**：持有/加仓/减仓/观望

### 2. ...
（逐只分析至第11只，含TSLA）

## 🇭🇰 港股自选
### 1. ...
### 2. ...
（逐只分析 4 只）

## 🇨🇳 A股自选
### 1. ...
### 2. ...
（逐只分析 3 只）

要求：每只 2-4 句，简洁可执行，避免空泛套话。"""
    
    if not MY_GEMINI_KEY:
        return "", "❌ 未配置 Gemini API Key"
    
    try:
        ai_report = call_gemini_api(prompt)
        if ai_report.startswith("❌"):
            return "", ai_report
        return ai_report or "无输出", None
    except Exception as e:
        err = f"❌ 自选股分析失败: {type(e).__name__}: {str(e)[:80]}"
        logging.error(err)
        return "", err


# ═══════════════════════════════════════════════════════════════
# 9. 【V89.6.2】注释：call_gemini_api已在前面定义（2815行）
# ═══════════════════════════════════════════════════════════════
# call_gemini_api函数已提前定义，确保所有模块都能正常调用

# 【V89.4】绑定舆情分析器的AI调用函数
if SENTIMENT_ANALYZER_AVAILABLE and _sentiment_analyzer:
    _sentiment_analyzer.call_ai = call_gemini_api

# ═══════════════════════════════════════════════════════════════
# 10. Session State 初始化
# ═══════════════════════════════════════════════════════════════
if 'proxy_port' not in st.session_state: st.session_state.proxy_port = "1082"
if 'scan_selected_code' not in st.session_state: st.session_state.scan_selected_code = None
if 'scan_selected_name' not in st.session_state: st.session_state.scan_selected_name = None
if 'trigger_analysis' not in st.session_state: st.session_state.trigger_analysis = False
# 【V87.7】全局对比篮
if 'compare_basket' not in st.session_state: st.session_state.compare_basket = []  # [(code, name), ...]
if 'search_history' not in st.session_state: st.session_state.search_history = []  # [(code, name), ...]
# 【V87.11】行业分析
if 'sector_analysis_name' not in st.session_state: st.session_state.sector_analysis_name = None
if 'sector_analysis_market' not in st.session_state: st.session_state.sector_analysis_market = None
if 'sector_analysis_codes' not in st.session_state: st.session_state.sector_analysis_codes = None

# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# 11. 侧边栏 - 个股搜索入口
# ═══════════════════════════════════════════════════════════════
with st.sidebar:
    # 【V88】版本标识
    if USE_NEW_MODULES:
        st.markdown('<p style="font-family: Georgia, \'Times New Roman\', SimSun, 宋体, serif; font-size: 12px; font-weight: 700; margin-bottom: 0.5rem;">👑 AI 皇冠双核 V88</p>', unsafe_allow_html=True)
        st.caption("✨ 模块化架构 | LRU缓存")
        st.divider()
    
    # 【V90.3】系统性能与数据刷新（从主区域移到侧边栏）
    if Config.ENABLE_PERF_LAYER and Config.ENABLE_EXPECTATION_LAYER:
        st.markdown('<p style="font-size: 12px; font-weight: 600; margin-bottom: 0.3rem;">⚙️ 系统性能</p>', unsafe_allow_html=True)
        try:
            # 强制刷新按钮
            force_refresh_btn = st.button(
                "🔄 强制刷新",
                key="force_refresh_macro",
                use_container_width=True,
                help="清除所有缓存，重新获取最新市场数据"
            )
            
            if force_refresh_btn:
                st.session_state['force_refresh_requested'] = True
                _cache_manager.clear()
                _perf_monitor.reset()
                st.success("✅ 已触发强制刷新")
                st.rerun()
            
            # 性能监控（折叠）
            with st.expander("📊 性能详情", expanded=False):
                _perf_monitor.finalize()
                metrics = _perf_monitor.get_metrics()
                cache_stats = _cache_manager.get_stats()
                
                st.metric("总耗时", f"{metrics['total_time_ms']:.0f}ms", help="从开始到结束的总耗时")
                st.metric("缓存命中率", f"{_perf_monitor.get_cache_hit_ratio()*100:.1f}%", help="缓存命中次数 / 总请求次数")
                st.metric("缓存项数", f"{cache_stats['items_count']}项", help="当前缓存中的数据项数量")
                
                st.caption(f"💾 缓存大小: {cache_stats['total_size_mb']:.2f} MB")
                st.caption(f"🔍 命中: {metrics['cache_hit_count']}次 | 未命中: {metrics['cache_miss_count']}次")
                
                total_time = metrics['total_time_ms']
                if total_time < 1000:
                    perf_grade = "🟢 极快"
                elif total_time < 3000:
                    perf_grade = "🟡 正常"
                else:
                    perf_grade = "🔴 较慢"
                st.info(f"**评级**: {perf_grade}")
        
        except Exception as e:
            st.warning(f"⚠️ 性能面板异常: {str(e)[:40]}")
            logging.error(f"侧边栏性能面板异常: {e}")
        
        st.divider()
    
    # 【V87.13】缩小侧边栏标题字体
    st.markdown('<p style="font-size: 12px; font-weight: 700; margin-bottom: 1rem;">🛸 指挥控制台</p>', unsafe_allow_html=True)
    
    # 【V87.13】对比篮显示 - 缩小字体
    st.markdown('<p style="font-size: 12px; font-weight: 600; margin-top: 0.5rem; margin-bottom: 0.5rem;">⚔️ 对比篮</p>', unsafe_allow_html=True)
    if len(st.session_state.compare_basket) > 0:
        st.caption(f"📊 已选 {len(st.session_state.compare_basket)} 只股票")
        for i, (code, name) in enumerate(st.session_state.compare_basket):
            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown(f"**{i+1}.** {name} ({code})")
            with col2:
                if st.button("❌", key=f"remove_{code}_{i}", help="移除"):
                    st.session_state.compare_basket.pop(i)
                    st.rerun()
        
        col_compare, col_clear = st.columns(2)
        with col_compare:
            if st.button("⚔️ 开始对比", type="primary", use_container_width=True):
                if len(st.session_state.compare_basket) >= 2:
                    codes = [item[0] for item in st.session_state.compare_basket]
                    names = [item[1] for item in st.session_state.compare_basket]
                    st.session_state.pk_codes = codes
                    st.session_state.pk_names = names
                    st.session_state.scan_selected_code = None
                    st.session_state.scan_selected_name = None
                    st.toast(f"⚔️ 开始对比 {len(codes)} 只股票", icon="⚔️")
                    st.rerun()
                else:
                    st.warning("至少选择2只股票才能对比")
        
        with col_clear:
            if st.button("🗑️ 清空", use_container_width=True):
                st.session_state.compare_basket = []
                st.rerun()
    else:
        st.caption("💡 从搜索或扫描结果中添加股票")
    
    st.markdown("---")
    
    # 【V93】浏览个股历史 - 点击可快速查看历史分析过的股票
    st.markdown('<p style="font-size: 12px; font-weight: 600; margin-top: 0.5rem; margin-bottom: 0.5rem;">📜 浏览个股历史</p>', unsafe_allow_html=True)
    if len(st.session_state.search_history) > 0:
        for i, (code, name) in enumerate(st.session_state.search_history[:8]):
            if st.button(f"🔍 {name} ({code})", key=f"sidebar_hist_{i}_{code}", use_container_width=True, help=f"点击分析 {name}"):
                st.session_state.scan_selected_code = code
                st.session_state.scan_selected_name = name
                st.session_state.pk_codes = []
                st.session_state.pk_names = []
                st.toast(f"✅ 已选中 {name}", icon="🔍")
                st.rerun()
        st.caption(f"共 {len(st.session_state.search_history)} 只，最多显示 8 只")
    else:
        st.caption("💡 搜索股票后将显示在此")
    
    st.markdown("---")
    
    # 【V91.8】AI市场简报快捷入口：做个股分析时也能快速跳转
    st.markdown('[📰 跳转 AI市场简报](#ai-market-brief)')
    st.caption("💡 做个股分析时，点击此处可快速滚动到页面底部")
    
    # 【V92】全量云端搜索已移至主区域「深度作战室」顶部
    st.caption("🔍 股票搜索已移至主区域 → 深度作战室")
    
    # 【V92】侧边栏收起提示：Streamlit 收起按钮在侧边栏与主区域交界处（左上角附近）
    st.caption("💡 收起侧边栏：点击**侧边栏右边缘**或**主区域左上角**的 ◀ 箭头")
    
    st.divider()
    
    # 代理设置
    st.markdown('<p style="font-size: 12px; font-weight: 600; margin-top: 1rem; margin-bottom: 0.3rem;">⚙️ 网络设置</p>', unsafe_allow_html=True)
    proxy_port = st.text_input("本地代理端口", value="1082", key="proxy_port_input")
    st.session_state.proxy_port = (proxy_port or "1082").strip() or "1082"
    
    if st.button("测试连接", use_container_width=True):
        purl = f"http://127.0.0.1:{st.session_state.proxy_port}"
        try:
            with ProxyContext(purl):
                r = requests.get("https://www.google.com", timeout=5, verify=False)
            st.success(f"✅ Google: {r.status_code}")
        except Exception as e:
            st.error(f"❌ 连接失败: {type(e).__name__}")
    
    st.divider()
    
    # 【V88】缓存统计显示
    if USE_NEW_MODULES:
        st.markdown('<p style="font-size: 12px; font-weight: 600; margin-top: 1rem; margin-bottom: 0.3rem;">💾 缓存状态 (V88 LRU)</p>', unsafe_allow_html=True)
        cache_stats = local_cache.get_stats()
        if cache_stats:
            st.metric(
                "缓存使用",
                f"{cache_stats['total_size_mb']:.1f}MB",
                f"{cache_stats['usage_percent']:.1f}%"
            )
            st.caption(f"📁 文件数: {cache_stats['file_count']} | ⏱️ TTL: {cache_stats['ttl_seconds']}s")
            st.caption(f"🔄 策略: LRU淘汰（保持80%容量）")
        st.divider()
    
    # 【V87.8】系统自检和股票池清理
    st.markdown('<p style="font-size: 12px; font-weight: 600; margin-top: 1rem; margin-bottom: 0.3rem;">🛠️ 系统维护</p>', unsafe_allow_html=True)
    st.caption("💡 诊断系统状态")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🛠️ 系统诊断", use_container_width=True, type="secondary"):
            with st.spinner("正在执行系统诊断..."):
                diagnostic_result = run_system_diagnostic()
            
            # 显示结果
            st.markdown("#### 诊断结果")
    
    with col2:
        if st.button("🏥 股票池检查", use_container_width=True, type="secondary"):
            with st.spinner("正在检查股票池健康状况..."):
                us_pool, hk_pool, cn_pool = init_stock_pools()
                
                st.markdown("#### 股票池健康检查结果")
                
                # 检查各市场股票池
                us_invalid = validate_stock_pool_health(us_pool, "美股", max_test=3)
                hk_invalid = validate_stock_pool_health(hk_pool, "港股", max_test=3) 
                cn_invalid = validate_stock_pool_health(cn_pool, "A股", max_test=3)
                
                total_invalid = len(us_invalid) + len(hk_invalid) + len(cn_invalid)
                if total_invalid == 0:
                    st.success("✅ 所有测试的股票代码都能正常获取数据")
                else:
                    st.warning(f"⚠️ 发现 {total_invalid} 个问题代码，建议更新股票池")
    
    # 【V87.15 + V88】缓存管理
    st.markdown('<p style="font-size: 12px; font-weight: 600; margin-top: 1rem; margin-bottom: 0.3rem;">💾 缓存管理</p>', unsafe_allow_html=True)
    
    cache_stats = local_cache.get_stats()
    st.caption(f"📊 缓存使用: {cache_stats['total_size_mb']:.1f}MB / {cache_stats['max_size_mb']:.0f}MB ({cache_stats['usage_percent']:.1f}%)")
    st.caption(f"📁 缓存文件: {cache_stats['file_count']} 个 | ⏱️ 有效期: 交易日15分钟/非交易日24小时")
    
    # 【V87.15】容量警告
    if cache_stats['usage_percent'] > 90:
        st.warning(f"⚠️ 缓存即将满，达到{cache_stats['max_size_mb']:.0f}MB后将自动清零", icon="⚠️")
    
    col_cache1, col_cache2 = st.columns(2)
    with col_cache1:
        if st.button("🗑️ 清空缓存", use_container_width=True, help="清空所有本地缓存文件"):
            local_cache.clear_all()
            st.cache_data.clear()
            st.success("✅ 缓存已清空")
            st.rerun()
    
    with col_cache2:
        if st.button("📋 查看失败详情", use_container_width=True):
            st.session_state.show_failed_stocks = True
            st.rerun()
    
    # 原有的诊断结果显示逻辑
    if 'diagnostic_result' in locals():
        
        # 1. 网络连通性
        net = diagnostic_result['network']
        if net['status'] == 'ok':
            st.success(f"✅ **网络连通性**: {net['message']}")
        elif net['status'] == 'warning':
            st.warning(f"⚠️ **网络连通性**: {net['message']}")
        else:
            st.error(f"❌ **网络连通性**: {net['message']}")
        
        # 2. 数据源测试
        st.markdown("**数据源测试**:")
        for market, result in diagnostic_result['data_sources'].items():
            if result['status'] == 'ok':
                st.success(f"✅ {result['name']}: {result['message']} (最后日期: {result.get('last_date', 'N/A')})")
            elif result['status'] == 'warning':
                st.warning(f"⚠️ {result['name']}: {result['message']}")
            else:
                st.error(f"❌ {result['name']}: {result['message']}")
        
        # 3. 整体评估
        st.divider()
        overall = diagnostic_result['overall']
        if overall == 'healthy':
            st.success("🎉 **系统状态**: 一切正常，可以开始使用！")
        elif overall == 'warning':
            st.warning("⚠️ **系统状态**: 部分功能可能受限，但基本可用")
        else:
            st.error("❌ **系统状态**: 存在严重问题，请检查网络和代理设置")
    
    st.divider()
    
    # 【V87】刷新股票池
    st.markdown("### 🔄 股票池管理")
    col_pool1, col_pool2 = st.columns(2)
    
    with col_pool1:
        if st.button("🔄 刷新股票池", use_container_width=True, help="重新从云端获取最新股票列表"):
            with st.spinner("正在刷新股票池..."):
                # 清除缓存
                st.cache_data.clear()
                # 重新加载
                st.rerun()
    
    with col_pool2:
        if st.button("🗑️ 清除全部缓存", use_container_width=True):
            st.cache_data.clear()
            st.success("✅ 缓存已清除")
            time.sleep(0.5)
            st.rerun()

# ═══════════════════════════════════════════════════════════════
# 12. 主界面 - 标题（修复遮挡）
# ═══════════════════════════════════════════════════════════════
st.markdown(
    '<div style="text-align: center; margin-bottom: 1.5rem;"><h2 style="color: #1e3a8a; font-size: 14px; font-weight: 700; margin: 0;">👑 AI 皇冠双核 V88 (模块化架构)</h2></div>',
    unsafe_allow_html=True
)
# 【V90.7】选中股票后置顶提示
if st.session_state.get('scan_selected_code'):
    st.markdown('<div style="background: linear-gradient(135deg, #10b981 0%, #059669 100%); padding: 1rem; border-radius: 8px; margin-bottom: 1rem; color: white; font-size: 12px; font-weight: 600; text-align: center;">🎯 深度分析报告已生成，请向下滚动查看「⚔️ 深度作战室」完整内容</div>', unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# 【V89.8 布局重构】模块分隔函数
# ═══════════════════════════════════════════════════════════════
def _module_header(icon, title, subtitle="", color_from="#667eea", color_to="#764ba2", compact=False):
    """统一的模块标题样式 - compact=True 时窄边化显示"""
    from datetime import datetime as _dt_hdr
    _weekday_cn = {"Monday": "周一", "Tuesday": "周二", "Wednesday": "周三", "Thursday": "周四", "Friday": "周五", "Saturday": "周六", "Sunday": "周日"}
    _today_display = _dt_hdr.now().strftime("%Y-%m-%d") + " " + _weekday_cn.get(_dt_hdr.now().strftime("%A"), "")
    if compact:
        # 上下变窄：标题+副标题同一行，日期单独一行
        title_line = f"{icon} {title}" + (f" · {subtitle}" if subtitle else "")
        st.markdown(f'''<div style="background: linear-gradient(135deg, {color_from} 0%, {color_to} 100%); 
            padding: 0.4rem 1rem; border-radius: 8px; margin: 1rem 0 0.8rem 0; width: 100%;">
            <div style="color: white; text-align: center; font-size: 12px; font-weight: 700; margin: 0;">{title_line}</div>
            <div style="color: rgba(255,255,255,0.7); text-align: center; font-size: 11px; margin: 0.15rem 0 0 0;">📅 {_today_display}</div>
        </div>''', unsafe_allow_html=True)
    else:
        sub_html = f'<p style="color: rgba(255,255,255,0.85); margin: 0.3rem 0 0 0; text-align: center; font-size: 12px;">{subtitle}</p>' if subtitle else ''
        st.markdown(f'''<div style="background: linear-gradient(135deg, {color_from} 0%, {color_to} 100%); 
            padding: 1.2rem; border-radius: 10px; margin: 1.5rem 0 1rem 0;">
            <h3 style="color: white; margin: 0; text-align: center; font-size: 14px; font-weight: 700;">{icon} {title}</h3>
            {sub_html}
            <p style="color: rgba(255,255,255,0.6); margin: 0.4rem 0 0 0; text-align: center; font-size: 12px;">📅 数据日期: {_today_display}</p>
        </div>''', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════
# 14. 【V78关键修复】深度作战室 - 独立于所有tabs之外
# ═══════════════════════════════════════════════════════════════

# 【V92】全量云端搜索 - 从侧边栏移至主区域，作为作战室入口
render_cloud_search()
st.markdown("---")

# 【V77.1调试】检测点击触发 - 添加详细日志
q_input = None
execute_analysis = False

_safe_print(f"[深度作战室] scan_selected_code = {st.session_state.get('scan_selected_code')}")

if st.session_state.get('scan_selected_code'):
    # 从 session_state 读取选中的股票
    q_input = st.session_state.scan_selected_code
    stock_name = st.session_state.scan_selected_name
    execute_analysis = True
    
    _safe_print(f"[深度作战室] ✅ 检测到选中股票: {stock_name} ({q_input}), execute_analysis = {execute_analysis}")
    
    # 明显的提示
    st.success(f"🎯 已自动选中：**{stock_name}** ({q_input})")
    st.caption('[📰 跳转 AI市场简报](#ai-market-brief)（报告在页面底部）')
    
    # 【V82.9新增】显示扫描分析表格
    st.markdown("#### 📊 扫描结果（勾选2-4只股票进行对比）")
    st.caption("💡 提示：以下是该股票的综合评分和策略建议")
    
    _scan_prog = st.progress(0)
    _scan_status = st.empty()
    _scan_status.text("📊 获取数据... (0%)")
    target_c = to_yf_cn_code(q_input)
    df_temp = fetch_stock_data(target_c)
    _scan_prog.progress(0.4)
    _scan_status.text("📊 计算指标... (40%)")

    if df_temp is not None:
        m = calculate_metrics_all(df_temp, target_c)
        _scan_prog.progress(0.8)
        _scan_status.text("📊 构建表格... (80%)")
        if m:
                # 判断市场（美股/港股/A股）
                if q_input[0].isalpha(): 
                    sector = "美股"
                elif len(q_input) == 5 or (len(q_input) >= 4 and q_input[0] == '0'): 
                    sector = "港股"
                elif q_input.startswith('6') or q_input.startswith('5'): 
                    sector = "A股(沪)"
                elif q_input.startswith('0') or q_input.startswith('3'): 
                    sector = "A股(深)"
                else: 
                    sector = "其他"
                
                # 长期趋势
                ma200 = m['last'].get('MA200', 0)
                if ma200 > 0 and m['last_price'] > ma200:
                    long_term = "📈 多头"
                elif ma200 > 0 and m['last_price'] < ma200 * 0.9:
                    long_term = "📉 空头"
                else:
                    long_term = "➡️ 震荡"
                
                # 短期趋势
                rsi = m['rsi']
                if rsi > 70:
                    short_term = "🔥 超买"
                elif rsi > 50:
                    short_term = "📈 强势"
                elif rsi > 30:
                    short_term = "📉 弱势"
                else:
                    short_term = "❄️ 超卖"
                
                # 资金状态
                if len(m['df']) >= 5:
                    vol_ma5 = m['df']['Volume'].tail(5).mean()
                    last_vol = m['last']['Volume']
                    if last_vol > vol_ma5 * 1.5:
                        capital = "💰 放量"
                    elif last_vol > vol_ma5:
                        capital = "📊 正常"
                    else:
                        capital = "📉 缩量"
                else:
                    capital = "➖"
                
                # 【V82.10新增】水位 - 显示离最高点和最低点的百分比
                l250 = m['df']['Low'].tail(250).min() if len(m['df']) >= 250 else m['df']['Low'].min()
                h250 = m['df']['High'].tail(250).max() if len(m['df']) >= 250 else m['df']['High'].max()
                if h250 > l250:
                    # 离最高点的百分比（负数表示低于最高点）
                    from_high_pct = (m['last_price'] - h250) / h250 * 100
                    # 离最低点的百分比（正数表示高于最低点）
                    from_low_pct = (m['last_price'] - l250) / l250 * 100
                    water_level = f"高{from_high_pct:+.1f}% 低{from_low_pct:+.1f}%"
                else:
                    water_level = "➖"
                
                # 构建扫描结果表格（含ESG）
                _esg_g = m.get('esg_grade', 'N/A')
                _esg_t = m.get('esg_total', 0)
                scan_result = pd.DataFrame([{
                    "代码": q_input,
                    "名称": stock_name,
                    "市场": sector,
                    "得分": m['score'],
                    "ESG": f"{_esg_t} ({_esg_g})",
                    "长期": long_term,
                    "短期": short_term,
                    "建议": "买入" if m['score'] > 70 else ("观望" if m['score'] > 50 else "回避"),
                    "策略": m['logic'],
                    "资金": capital,
                    "水位": water_level,
                    "现价": f"{m['last_price']:.2f}"
                }])
                
                _scan_prog.progress(1.0)
                _scan_status.text("✅ 完成 (100%)")
                time.sleep(0.2)
                _scan_prog.empty()
                _scan_status.empty()

                st.dataframe(
                    scan_result,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "得分": st.column_config.ProgressColumn(
                            "得分",
                            format="%d",
                            min_value=0,
                            max_value=100,
                        ),
                    }
                )
        else:
            _scan_prog.progress(1.0)
            _scan_status.text("❌ 指标计算失败")
            time.sleep(0.3)
            _scan_prog.empty()
            _scan_status.empty()
            # 【V87.4】增强错误提示 - 特别处理已退市股票
            st.error("❌ 无法获取扫描分析数据")
            
            # 检查是否是已知的退市股票
            delisted_stocks = {
                "ATVI": "动视暴雪 - 已被微软收购退市",
                # 可以继续添加其他已知退市股票
            }
            
            stock_code = q_input.upper().strip()
            if stock_code in delisted_stocks:
                st.warning(f"🚨 **{delisted_stocks[stock_code]}**")
                st.info("💡 **建议尝试其他股票：**")
                
                # 根据市场推荐替代股票
                if stock_code.startswith("0") and len(stock_code) == 5:  # 港股
                    suggestions = [
                        ("00700", "腾讯控股", "科技巨头"),
                        ("09988", "阿里巴巴", "电商平台"), 
                        ("03690", "美团", "生活服务"),
                        ("01810", "小米集团", "智能硬件"),
                        ("06618", "京东健康", "医疗健康")
                    ]
                    st.markdown("**🇭🇰 推荐港股：**")
                elif stock_code.isalpha():  # 美股
                    suggestions = [
                        ("AAPL", "苹果", "科技巨头"),
                        ("MSFT", "微软", "软件服务"),
                        ("GOOGL", "谷歌", "互联网"),
                        ("TSLA", "特斯拉", "电动汽车"),
                        ("NVDA", "英伟达", "AI芯片")
                    ]
                    st.markdown("**🇺🇸 推荐美股：**")
                else:  # A股
                    suggestions = [
                        ("600519", "贵州茅台", "白酒龙头"),
                        ("000858", "五粮液", "白酒"),
                        ("300750", "宁德时代", "新能源电池"),
                        ("002594", "比亚迪", "新能源汽车"),
                        ("600036", "招商银行", "银行")
                    ]
                    st.markdown("**🇨🇳 推荐A股：**")
                
                # 显示推荐股票
                for code, name, desc in suggestions:
                    st.markdown(f"- **{code}** ({name}) - {desc}")
                    
            else:
                # 通用错误提示
                st.info("🔍 **可能的原因：**")
                st.markdown("""
                1. **股票代码错误** - 请检查代码格式
                2. **股票已退市** - 该股票可能已从交易所退市
                3. **网络连接问题** - 请检查网络和代理设置
                4. **数据源暂时不可用** - 请稍后重试
                """)
                
                st.info("💡 **建议操作：**")
                st.markdown("""
                1. 使用上方**全量云端搜索**功能查找正确的股票代码
                2. 尝试搜索其他活跃交易的股票
                3. 点击**系统自检**检查网络连接状态
                4. 使用**股票池健康检查**验证数据源状态
                """)
    else:
        _scan_prog.progress(1.0)
        _scan_status.text("❌ 数据获取失败")
        time.sleep(0.3)
        _scan_prog.empty()
        _scan_status.empty()
        st.error("❌ 无法获取股票数据")

    st.markdown("---")

# 【V92】个股搜索统一使用主区域「深度作战室」顶部的全量云端搜索

# 开始执行分析
_safe_print(f"[深度作战室] 准备执行分析: execute_analysis={execute_analysis}, q_input={q_input}")

if execute_analysis and q_input:
    code = q_input.upper().strip()
    target_c = to_yf_cn_code(code)
    
    _safe_print(f"[深度作战室] 🎯 开始分析: {code} -> {target_c}")
    
    st.subheader(f"🎯 {target_c}")
    
    # 【V91.9】深度作战室缓存：K 线点击等 rerun 时复用数据，减少 Running 时长与灰屏
    # 【V91.10】统一缓存：交易日15分钟，非交易日24小时
    _cache_key = f"_warroom_{target_c}"
    _cache_ttl = get_smart_cache_ttl('daily')
    import time as _time_module
    _now = _time_module.time()
    _cached = (_cache_key in st.session_state and
               (_now - st.session_state.get(f"{_cache_key}_ts", 0)) <= _cache_ttl)
    if _cached:
        df, data_quality = st.session_state[_cache_key]
        _safe_print(f"[深度作战室] 使用缓存数据 (剩余 {int(_cache_ttl - (_now - st.session_state[f'{_cache_key}_ts']))}s)")
    else:
        try:
            df, data_quality = fetch_stock_data(target_c, return_quality=True)
            _safe_print(f"[深度作战室] 数据获取: df={'有数据' if df is not None else '无数据'}")
            if df is not None:
                st.session_state[_cache_key] = (df, data_quality or {})
                st.session_state[f"{_cache_key}_ts"] = _now
        except Exception as e:
            df, data_quality = None, {}
            _safe_print(f"[深度作战室] 数据异常: {e}")
    
    # 【V83 P0.1】显示数据质量标签
    if df is not None and data_quality:
        col_src1, col_src2, col_src3 = st.columns([2, 2, 1])
        with col_src1:
            delay_icon = "🟡" if data_quality.get('is_delayed', False) else "🟢"
            st.caption(f"{delay_icon} **数据来源**: {data_quality.get('source', '未知')}")
        with col_src2:
            st.caption(f"📅 **数据范围**: {data_quality.get('date_range', 'N/A')}")
        with col_src3:
            st.caption(f"📊 **数据点**: {data_quality.get('data_points', 0)}")
    
    # 【V87.15修复】数据获取失败的处理
    if df is None:
        _safe_print(f"[深度作战室] ❌ 数据获取失败: {target_c}")
        st.error("❌ 无法获取股票数据")
        
        # 详细错误提示
        st.info("🔍 **可能的原因：**")
        
        # 根据股票代码类型给出针对性建议
        if code.startswith('6') or code.startswith('0') or code.startswith('3') or code.startswith('5'):
            # A股
            st.markdown("""
            **A股数据获取失败：**
            1. 检查代码格式（如：600519 贵州茅台）
            2. 确认股票未停牌或退市
            3. 尝试使用东方财富数据源
            4. 检查网络连接状态
            """)
        elif len(code) == 5 or (len(code) >= 4 and code[0] == '0'):
            # 港股
            st.markdown("""
            **港股数据获取失败：**
            1. 检查代码格式（如：00700 腾讯控股）
            2. 确认使用5位数代码（如：00700，不是700）
            3. 检查代理设置（港股需要代理）
            4. 确认股票未退市
            """)
        else:
            # 美股
            st.markdown("""
            **美股数据获取失败：**
            1. 检查代码格式（如：AAPL 苹果）
            2. 确认股票代码正确（全大写）
            3. 检查代理设置
            4. 确认股票未退市或被收购
            """)
        
        # 推荐测试股票
        st.info("💡 **推荐测试股票：**")
        col_test1, col_test2, col_test3 = st.columns(3)
        with col_test1:
            if st.button("🇺🇸 测试 AAPL", key="test_aapl_error", use_container_width=True):
                st.session_state.scan_selected_code = "AAPL"
                st.session_state.scan_selected_name = "苹果"
                st.rerun()
        with col_test2:
            if st.button("🇭🇰 测试 00700", key="test_hk_error", use_container_width=True):
                st.session_state.scan_selected_code = "00700"
                st.session_state.scan_selected_name = "腾讯控股"
                st.rerun()
        with col_test3:
            if st.button("🇨🇳 测试 600519", key="test_cn_error", use_container_width=True):
                st.session_state.scan_selected_code = "600519"
                st.session_state.scan_selected_name = "贵州茅台"
                st.rerun()
        
        # 不要继续执行后续代码
        st.stop()
    
    if df is not None:
        _computed_key = f"_warroom_computed_{target_c}"
        if _cached and _computed_key in st.session_state:
            metrics = st.session_state[_computed_key].get("metrics")
            quant = st.session_state[_computed_key].get("quant")
            mc = st.session_state[_computed_key].get("mc")
            risk_metrics = st.session_state[_computed_key].get("risk_metrics")
            news_headlines = st.session_state[_computed_key].get("news_headlines")
            _safe_print(f"[深度作战室] 使用缓存指标")
        else:
            try:
                _safe_print(f"[深度作战室] 📊 开始计算指标...")
                metrics = calculate_metrics_all(df, target_c)
                quant = calculate_advanced_quant(df)
                mc = monte_carlo_forecast(df)
                risk_metrics = calculate_risk_metrics(df, target_c)
                news_headlines = fetch_news_headlines(target_c)
                if _cache_key in st.session_state:
                    st.session_state[_computed_key] = {
                        "metrics": metrics, "quant": quant, "mc": mc,
                        "risk_metrics": risk_metrics, "news_headlines": news_headlines,
                    }
            except Exception as e:
                _safe_print(f"[深度作战室] ❌ 指标计算异常: {type(e).__name__}: {str(e)}")
                import traceback
                traceback.print_exc()
                st.error(f"❌ 指标计算失败: {type(e).__name__}")
                st.info(f"错误详情: {str(e)}")
                st.stop()
        
        # ═══════════════════════════════════════════════════════════════
        # 【V90 升级】K线图 + 机构作战层（VWAP + Chandelier Exit）
        # ═══════════════════════════════════════════════════════════════
        
        # 预先计算 VWAP 和 Chandelier Exit（K线图和后续分析共用）
        _chart_predictor = None
        _chart_vwap = None
        _chart_ce = None
        if HAS_PREDICTION_ENGINE:
            try:
                _chart_predictor = InstitutionalPredictor(df, target_c)
                _chart_vwap = _chart_predictor.calculate_vwap(window=20)
                _chart_ce = _chart_predictor.calculate_chandelier_exit()
            except Exception as _ce_err:
                logging.warning(f"K线叠加层计算失败: {_ce_err}")
        
        # K线蜡烛图（基础层）
        fig = go.Figure(data=[go.Candlestick(
            x=df.index,
            open=df['Open'],
            high=df['High'],
            low=df['Low'],
            close=df['Close'],
            name='K线'
        )])
        
        # 叠加 VWAP 金线
        if _chart_vwap is not None and not _chart_vwap.empty:
            fig.add_trace(go.Scatter(
                x=df.index,
                y=_chart_vwap,
                mode='lines',
                name='VWAP(20日) 机构成本线',
                line=dict(color='#FFD700', width=2.5, dash='solid'),
                hovertemplate='VWAP: %{y:.2f}<extra></extra>'
            ))
        
        # 叠加 Chandelier Exit 通道
        if _chart_ce and _chart_ce.get('chandelier_long') is not None:
            _ce_long = _chart_ce['chandelier_long']
            _ce_short = _chart_ce['chandelier_short']
            
            # 多头止损线（绿色虚线）
            fig.add_trace(go.Scatter(
                x=df.index,
                y=_ce_long,
                mode='lines',
                name='Chandelier多头止损',
                line=dict(color='#10b981', width=1.5, dash='dash'),
                hovertemplate='多头止损: %{y:.2f}<extra></extra>'
            ))
            
            # 空头止损线（红色虚线）
            fig.add_trace(go.Scatter(
                x=df.index,
                y=_ce_short,
                mode='lines',
                name='Chandelier空头止损',
                line=dict(color='#ef4444', width=1.5, dash='dash'),
                hovertemplate='空头止损: %{y:.2f}<extra></extra>'
            ))
        
        # 添加可点击的收盘价散点层（用于选点交互）
        fig.add_trace(go.Scatter(
            x=df.index,
            y=df['Close'],
            mode='markers',
            name='收盘价（点击选点）',
            marker=dict(color='rgba(99,102,241,0.4)', size=6, symbol='circle'),
            hovertemplate='<b>%{x|%Y-%m-%d}</b><br>收盘价: %{y:.2f}<br><i>👆 点击此处选定入场点</i><extra></extra>',
            selected=dict(marker=dict(color='#ff6b00', size=14)),
            unselected=dict(marker=dict(opacity=0.3))
        ))
        
        fig.update_layout(
            title="K线图 + 机构作战层 （点击紫色圆点选定入场价位）",
            xaxis_title="日期",
            yaxis_title="价格",
            height=600,
            template="plotly_white",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=0.5,
                font=dict(size=11)
            ),
            # 十字准星 - 光标移动时显示精确价格和日期
            hovermode='x unified',
            xaxis=dict(
                showspikes=True,
                spikecolor='#6366f1',
                spikethickness=1,
                spikedash='dot',
                spikemode='across',
                spikesnap='cursor'
            ),
            yaxis=dict(
                showspikes=True,
                spikecolor='#6366f1',
                spikethickness=1,
                spikedash='dot',
                spikemode='across',
                spikesnap='cursor'
            ),
            # 启用框选模式（用于选点）
            dragmode='select',
            clickmode='event+select'
        )
        
        # 使用 on_select 捕获用户点击
        _kline_event = st.plotly_chart(fig, use_container_width=True, on_select="rerun", key=f"kline_select_{target_c}")
        
        # K线图注释说明
        _chart_note_cols = st.columns(3)
        with _chart_note_cols[0]:
            st.markdown('<p style="font-size:12px;color:#FFD700;font-weight:600;">━━ VWAP(20日) 机构成本线</p>', unsafe_allow_html=True)
            st.markdown('<p style="font-size:12px;color:#888;">📖 成交量加权平均价=机构大资金的平均持仓成本。<b>价格在VWAP上方</b>=机构盈利、多头主导；<b>跌破VWAP</b>=机构被套、可能抛售</p>', unsafe_allow_html=True)
        with _chart_note_cols[1]:
            st.markdown('<p style="font-size:12px;color:#10b981;font-weight:600;">┅┅ Chandelier多头止损线</p>', unsafe_allow_html=True)
            st.markdown('<p style="font-size:12px;color:#888;">📖 22日最高价 - 3×ATR = 动态追踪止损。<b>价格跌破此线</b>=趋势可能反转，多头应离场。比固定止损更科学，随趋势自动上移</p>', unsafe_allow_html=True)
        with _chart_note_cols[2]:
            st.markdown('<p style="font-size:12px;color:#ef4444;font-weight:600;">┅┅ Chandelier空头止损线</p>', unsafe_allow_html=True)
            st.markdown('<p style="font-size:12px;color:#888;">📖 22日最低价 + 3×ATR = 空头追踪止损。<b>价格突破此线</b>=下跌趋势可能结束，空头应离场。两线之间=安全通道</p>', unsafe_allow_html=True)
        
        # Chandelier Exit 当前状态速览
        if _chart_ce and _chart_ce.get('ce_long_latest', 0) > 0:
            _ce_signal = _chart_ce.get('signal', '')
            _ce_long_val = _chart_ce.get('ce_long_latest', 0)
            _ce_short_val = _chart_ce.get('ce_short_latest', 0)
            _curr_price = float(df['Close'].iloc[-1])
            _ce_signal_color = "#ef4444" if "跌破" in _ce_signal else ("#10b981" if "突破" in _ce_signal else "#f59e0b")
            st.markdown(f'<div style="background: {_ce_signal_color}15; border-left: 4px solid {_ce_signal_color}; padding: 0.7rem 1rem; border-radius: 4px; margin: 0.5rem 0;"><span style="font-weight:600;">{_ce_signal}</span> &nbsp;|&nbsp; 当前价 <b>{_curr_price:.2f}</b> &nbsp;|&nbsp; 多头止损 <b style="color:#10b981">{_ce_long_val:.2f}</b> &nbsp;|&nbsp; 空头止损 <b style="color:#ef4444">{_ce_short_val:.2f}</b></div>', unsafe_allow_html=True)
        
        # ═══════════════════════════════════════════════════════════════
        # 【V90 新增】AI入场顾问 - 点击K线选定入场价 → AI给止损止盈
        # ═══════════════════════════════════════════════════════════════
        st.markdown("---")
        st.markdown("### 🎯 AI入场顾问 - 选定价位获取止损止盈建议")
        st.caption("📖 在上方K线图上框选/点击紫色圆点选定入场日期，或在下方手动选择。AI会根据支撑位、压力位、均线和量价结构智能给出止损止盈，不用死板公式")
        
        # 解析图表点击事件
        _selected_entry_date = None
        _selected_entry_price = None
        _selected_candle = None
        
        if _kline_event and hasattr(_kline_event, 'selection') and _kline_event.selection:
            _sel = _kline_event.selection
            _sel_points = _sel.get('points', []) if isinstance(_sel, dict) else (getattr(_sel, 'points', []) if hasattr(_sel, 'points') else [])
            if _sel_points and len(_sel_points) > 0:
                _first_point = _sel_points[0]
                _sel_x = _first_point.get('x', None)
                if _sel_x:
                    try:
                        import pandas as pd
                        _sel_date = pd.Timestamp(_sel_x)
                        # 找到对应日期的数据
                        if _sel_date in df.index:
                            _selected_entry_date = _sel_date
                            _selected_entry_price = float(df.loc[_sel_date, 'Close'])
                            _selected_candle = {
                                'Open': float(df.loc[_sel_date, 'Open']),
                                'High': float(df.loc[_sel_date, 'High']),
                                'Low': float(df.loc[_sel_date, 'Low']),
                                'Close': float(df.loc[_sel_date, 'Close']),
                                'Volume': float(df.loc[_sel_date, 'Volume'])
                            }
                            st.success(f"✅ 已从K线图选中：**{_sel_date.strftime('%Y-%m-%d')}** | 收盘价 **{_selected_entry_price:.2f}**")
                    except Exception as _sel_err:
                        logging.warning(f"选点解析失败: {_sel_err}")
        
        # 手动选择（备用 + 微调）
        with st.expander("📅 手动选择日期 / 微调入场价", expanded=(_selected_entry_date is None)):
            _ea_col1, _ea_col2, _ea_col3 = st.columns([2, 2, 1])
            
            # 日期列表（最近60个交易日倒序）
            _date_options = df.index[-60:].tolist()[::-1]
            _date_labels = [d.strftime('%Y-%m-%d (%a)') if hasattr(d, 'strftime') else str(d) for d in _date_options]
            
            with _ea_col1:
                _default_idx = 0
                if _selected_entry_date and _selected_entry_date in _date_options:
                    _default_idx = _date_options.index(_selected_entry_date)
                _manual_date_label = st.selectbox(
                    "选择交易日",
                    options=_date_labels,
                    index=_default_idx,
                    key=f"entry_date_sel_{target_c}",
                    help="选择你打算入场的交易日"
                )
                _manual_date_idx = _date_labels.index(_manual_date_label)
                _manual_date = _date_options[_manual_date_idx]
                _manual_candle = {
                    'Open': float(df.loc[_manual_date, 'Open']),
                    'High': float(df.loc[_manual_date, 'High']),
                    'Low': float(df.loc[_manual_date, 'Low']),
                    'Close': float(df.loc[_manual_date, 'Close']),
                    'Volume': float(df.loc[_manual_date, 'Volume'])
                }
            
            with _ea_col2:
                _default_price = _selected_entry_price if _selected_entry_price else _manual_candle['Close']
                _manual_price = st.number_input(
                    "入场价格（可微调）",
                    min_value=0.01,
                    value=float(_default_price),
                    step=0.01,
                    format="%.2f",
                    key=f"entry_price_input_{target_c}",
                    help="默认为选定日的收盘价，你可以改为你的实际/计划买入价"
                )
            
            with _ea_col3:
                st.markdown("<br>", unsafe_allow_html=True)
                _use_manual = st.checkbox("使用手动选择", value=(_selected_entry_date is None), key=f"use_manual_{target_c}")
            
            # 显示选定K线信息
            _final_date = _manual_date if _use_manual else (_selected_entry_date or _manual_date)
            _final_price = _manual_price if _use_manual else (_selected_entry_price or _manual_price)
            _final_candle = _manual_candle if _use_manual else (_selected_candle or _manual_candle)
            
            _ohlc_cols = st.columns(5)
            with _ohlc_cols[0]:
                st.metric("开盘", f"{_final_candle['Open']:.2f}")
            with _ohlc_cols[1]:
                st.metric("最高", f"{_final_candle['High']:.2f}")
            with _ohlc_cols[2]:
                st.metric("最低", f"{_final_candle['Low']:.2f}")
            with _ohlc_cols[3]:
                st.metric("收盘", f"{_final_candle['Close']:.2f}")
            with _ohlc_cols[4]:
                st.metric("成交量", f"{_final_candle['Volume']:,.0f}")
        
        # 确认入场信息条
        _final_date_str = _final_date.strftime('%Y-%m-%d') if hasattr(_final_date, 'strftime') else str(_final_date)
        st.markdown(f'<div style="background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%); padding: 0.8rem 1.5rem; border-radius: 8px; display: flex; align-items: center; justify-content: space-between;"><div style="color: white;"><span style="font-size: 12px;">📍 入场点位确认</span><br><span style="font-size: 12px; font-weight: 700;">{target_c} @ {_final_price:.2f}</span><span style="font-size: 12px; margin-left: 12px; opacity: 0.85;">({_final_date_str})</span></div></div>', unsafe_allow_html=True)
        
        # AI分析按钮
        if MY_GEMINI_KEY and HAS_PREDICTION_ENGINE:
            _ea_cache_key = f"entry_advisor_{target_c}_{_final_date_str}_{_final_price:.2f}"
            
            _run_ea = st.button(
                "🤖 AI分析止损止盈",
                key=f"btn_entry_advisor_{target_c}",
                type="primary",
                use_container_width=True,
                help="AI根据支撑位、压力位、均线和量价结构，智能给出止损和多级止盈建议"
            )
            
            if _run_ea:
                _ea_prog = st.progress(0)
                _ea_stat = st.empty()
                
                try:
                    import time as _ea_time
                    _ea_start = _ea_time.time()
                    
                    _ea_stat.text("🤖 AI策略师正在分析你的入场点位... 20%")
                    _ea_prog.progress(0.2)
                    
                    _ea_predictor = _chart_predictor if _chart_predictor else InstitutionalPredictor(df, target_c)
                    _ea_predictor.calculate_alpha_factors()
                    _ea_predictor.calculate_risk_engine()
                    
                    _ea_stat.text("🤖 正在计算止损止盈... 50%")
                    _ea_prog.progress(0.5)
                    
                    _macro_ctx = st.session_state.get('all_markets', {}).get('us_market', {})
                    
                    _ea_result = _ea_predictor.call_gemini_entry_advisor(
                        MY_GEMINI_KEY,
                        entry_price=_final_price,
                        entry_date=_final_date_str,
                        candle_data=_final_candle,
                        model_name=GEMINI_MODEL_NAME,
                        macro_context=_macro_ctx
                    )
                    
                    _ea_elapsed = _ea_time.time() - _ea_start
                    _ea_prog.progress(0.9)
                    _ea_stat.text(f"✅ 分析完成（耗时{_ea_elapsed:.1f}秒）")
                    _ea_time.sleep(0.3)
                    _ea_prog.progress(1.0)
                    _ea_time.sleep(0.3)
                    _ea_prog.empty()
                    _ea_stat.empty()
                    
                    st.session_state[_ea_cache_key] = _ea_result
                
                except Exception as _ea_err:
                    _ea_prog.empty()
                    _ea_stat.empty()
                    st.error(f"❌ AI分析失败: {str(_ea_err)[:80]}")
            
            elif _ea_cache_key in st.session_state:
                _ea_result = st.session_state[_ea_cache_key]
            else:
                _ea_result = None
            
            # 显示AI止损止盈结果
            if _ea_result and _ea_result.get('stop_loss', 0) > 0:
                st.markdown("#### 📊 AI止损止盈方案")
                
                # 入场评分
                _grade = _ea_result.get('entry_grade', '')
                _grade_color = "#10b981" if 'A' in _grade else ("#3b82f6" if 'B' in _grade else ("#f59e0b" if 'C' in _grade else "#ef4444"))
                
                # 价格可视化条
                _sl = _ea_result.get('stop_loss', 0)
                _tp1 = _ea_result.get('take_profit_1', 0)
                _tp2 = _ea_result.get('take_profit_2', 0)
                _sl_pct = ((_final_price - _sl) / _final_price * 100) if _sl > 0 else 0
                _tp1_pct = ((_tp1 - _final_price) / _final_price * 100) if _tp1 > 0 else 0
                _tp2_pct = ((_tp2 - _final_price) / _final_price * 100) if _tp2 > 0 else 0
                
                # 四列展示
                _ea_show_cols = st.columns(4)
                
                with _ea_show_cols[0]:
                    st.markdown(f'<div style="background: {_grade_color}18; border: 2px solid {_grade_color}; padding: 1rem; border-radius: 8px; text-align: center;"><div style="font-size: 12px; color: #888;">入场评分</div><div style="font-size: 12px; font-weight: 800; color: {_grade_color}; margin: 0.3rem 0;">{_grade}</div></div>', unsafe_allow_html=True)
                    st.markdown(f'<p style="font-size:12px;color:#888;">📖 A=绝佳入场点，B=不错可做，C=一般谨慎，D=不建议入场</p>', unsafe_allow_html=True)
                
                with _ea_show_cols[1]:
                    st.markdown(f'<div style="background: #fef2f2; border: 2px solid #ef4444; padding: 1rem; border-radius: 8px; text-align: center;"><div style="font-size: 12px; color: #888;">🔻 止损价</div><div style="font-size: 12px; font-weight: 700; color: #ef4444; margin: 0.3rem 0;">{_sl:.2f}</div><div style="font-size: 12px; color: #ef4444;">-{_sl_pct:.1f}%</div></div>', unsafe_allow_html=True)
                    st.markdown(f'<p style="font-size:12px;color:#888;">📖 {_ea_result.get("stop_loss_reason", "")}</p>', unsafe_allow_html=True)
                
                with _ea_show_cols[2]:
                    st.markdown(f'<div style="background: #f0fdf4; border: 2px solid #10b981; padding: 1rem; border-radius: 8px; text-align: center;"><div style="font-size: 12px; color: #888;">🎯 止盈1（保守）</div><div style="font-size: 12px; font-weight: 700; color: #10b981; margin: 0.3rem 0;">{_tp1:.2f}</div><div style="font-size: 12px; color: #10b981;">+{_tp1_pct:.1f}%</div></div>', unsafe_allow_html=True)
                    st.markdown(f'<p style="font-size:12px;color:#888;">📖 {_ea_result.get("take_profit_1_reason", "")}</p>', unsafe_allow_html=True)
                
                with _ea_show_cols[3]:
                    st.markdown(f'<div style="background: #eff6ff; border: 2px solid #3b82f6; padding: 1rem; border-radius: 8px; text-align: center;"><div style="font-size: 12px; color: #888;">🚀 止盈2（激进）</div><div style="font-size: 12px; font-weight: 700; color: #3b82f6; margin: 0.3rem 0;">{_tp2:.2f}</div><div style="font-size: 12px; color: #3b82f6;">+{_tp2_pct:.1f}%</div></div>', unsafe_allow_html=True)
                    st.markdown(f'<p style="font-size:12px;color:#888;">📖 {_ea_result.get("take_profit_2_reason", "")}</p>', unsafe_allow_html=True)
                
                # 盈亏比可视化
                if _sl_pct > 0 and _tp1_pct > 0:
                    _rr1 = _tp1_pct / _sl_pct
                    _rr2 = _tp2_pct / _sl_pct if _tp2_pct > 0 else 0
                    _rr_color = "#10b981" if _rr1 >= 2 else ("#f59e0b" if _rr1 >= 1.5 else "#ef4444")
                    st.markdown(f'<div style="background: #f8fafc; padding: 0.6rem 1rem; border-radius: 6px; border: 1px solid #e2e8f0; margin-top: 0.5rem;"><span style="font-size:12px;">📐 <b>盈亏比</b>：保守目标 <b style="color:{_rr_color};">{_rr1:.1f}:1</b>{"&nbsp;&nbsp;|&nbsp;&nbsp;激进目标 <b style=" + chr(34) + "color:#3b82f6;" + chr(34) + ">" + f"{_rr2:.1f}:1</b>" if _rr2 > 0 else ""}&nbsp;&nbsp;|&nbsp;&nbsp;持仓周期 <b>{_ea_result.get("hold_period", "")}</b></span></div>', unsafe_allow_html=True)
                    st.caption("📖 盈亏比 = 预期盈利/预期亏损。≥2:1是好交易，<1.5:1不值得冒险")
                
                # 策略总结
                _strategy = _ea_result.get('strategy_summary', '')
                if _strategy:
                    st.markdown(f'<div style="background: linear-gradient(135deg, #1e293b 0%, #334155 100%); padding: 1rem 1.5rem; border-radius: 8px; margin-top: 0.8rem;"><div style="color: #94a3b8; font-size: 12px; margin-bottom: 0.3rem;">📝 AI策略总结</div><div style="color: white; font-size: 12px; line-height: 1.6;">{_strategy}</div></div>', unsafe_allow_html=True)
                    st.caption(f"📌 本报告由 AI 生成 · 模型: {_ai_model_label()}")
            
            elif _ea_result is None:
                st.info("👆 在K线图上选定入场点（框选紫色圆点），或在上方手动选择日期和价格，然后点击按钮获取AI止损止盈建议")
        else:
            if not MY_GEMINI_KEY:
                st.info("💡 配置 Gemini API Key 即可使用AI入场顾问")
        
        # 【V88.12】前瞻预测层 - 机构生命线 + AI预测
        if HAS_PREDICTION_ENGINE:
            st.markdown("---")
            st.markdown("### 🔮 前瞻预测层 - 机构级智能分析")
            
            with st.expander("📊 **机构生命线 & AI预测**", expanded=True):
                try:
                    # 【V90】复用已创建的predictor（避免重复计算）
                    predictor = _chart_predictor if _chart_predictor else InstitutionalPredictor(df, target_c)
                    alpha_factors = predictor.calculate_alpha_factors()
                    risk_metrics = predictor.calculate_risk_engine()
                    
                    # 1. VWAP机构生命线
                    st.markdown("#### 💰 机构生命线 (VWAP)")
                    st.caption("成交量加权平均价 - 机构大单平均成本线")
                    
                    vwap_cols = st.columns([2, 2, 3])
                    with vwap_cols[0]:
                        vwap_val = alpha_factors.get('vwap_20', 0)
                        if vwap_val:
                            st.metric("VWAP(20日)", f"{vwap_val:.2f}")
                            st.markdown('<p style="font-size:12px;color:#888;">📖 过去20天机构大单的平均买入成本</p>', unsafe_allow_html=True)
                    
                    with vwap_cols[1]:
                        vwap_dev = alpha_factors.get('vwap_deviation', 0)
                        st.metric("偏离度", f"{vwap_dev:+.2f}%", 
                                 delta_color="normal" if vwap_dev > 0 else "inverse")
                        _vwap_dev_hint = "价格高于机构成本→多头" if vwap_dev > 0 else "价格低于机构成本→空头"
                        st.markdown(f'<p style="font-size:12px;color:#888;">📖 {_vwap_dev_hint}。偏离>5%=强势但追高风险大</p>', unsafe_allow_html=True)
                    
                    with vwap_cols[2]:
                        st.info(alpha_factors.get('vwap_signal', '⚪ 无信号'))
                        st.markdown('<p style="font-size:12px;color:#888;">📖 VWAP信号：强势=安全持有，偏空=警惕回调，弱势=不建议新仓</p>', unsafe_allow_html=True)
                    
                    st.divider()
                    
                    # 2. Alpha因子矩阵
                    st.markdown("#### 🎯 Alpha因子矩阵")
                    st.caption("📖 Alpha因子 = 超越大盘的收益来源。机构用这些因子寻找「别人看不到的信号」")
                    alpha_cols = st.columns(3)
                    
                    with alpha_cols[0]:
                        st.markdown("**RSI背离**")
                        st.info(alpha_factors.get('rsi_divergence', '⚪ 无数据'))
                        st.markdown('<p style="font-size:12px;color:#888;">📖 RSI=相对强弱指标(0-100)。<b>底背离</b>=价格新低但RSI未新低→反弹信号；<b>顶背离</b>=价格新高但RSI未新高→见顶信号</p>', unsafe_allow_html=True)
                    
                    with alpha_cols[1]:
                        st.markdown("**布林带挤压**")
                        st.info(alpha_factors.get('bb_squeeze', '⚪ 无数据'))
                        st.markdown('<p style="font-size:12px;color:#888;">📖 布林带=价格波动通道。<b>极度挤压</b>=波动率极低，即将爆发大行情（方向不定）；<b>扩张</b>=趋势正在展开</p>', unsafe_allow_html=True)
                    
                    with alpha_cols[2]:
                        st.markdown("**量价背离**")
                        st.info(alpha_factors.get('volume_price_divergence', '⚪ 无数据'))
                        st.markdown('<p style="font-size:12px;color:#888;">📖 价格上涨但成交量下降=上涨无力，假突破风险高；价格下跌但成交量萎缩=抛压减弱，可能见底</p>', unsafe_allow_html=True)
                    
                    st.divider()
                    
                    # 3. 风险引擎
                    st.markdown("#### ⚡ 风险引擎 - 动态止损 & 仓位管理")
                    st.caption("📖 风险引擎 = 机构的「安全气囊」。不是帮你赚钱，而是帮你在撞车时活下来")
                    
                    risk_cols = st.columns(4)
                    with risk_cols[0]:
                        stop_loss = risk_metrics.get('stop_loss', 0)
                        if stop_loss:
                            st.metric("止损价", f"{stop_loss:.2f}")
                            st.caption(f"🛡️ {risk_metrics.get('stop_loss_pct', 0):.2f}% ATR止损")
                            st.markdown('<p style="font-size:12px;color:#888;">📖 止损价=当前价-2.5×ATR。跌到此价必须卖出，不抱幻想。这是机构铁律</p>', unsafe_allow_html=True)
                    
                    with risk_cols[1]:
                        kelly_pos = risk_metrics.get('kelly_position', 5)
                        st.metric("建议仓位", f"{kelly_pos:.1f}%")
                        st.caption("📊 Kelly公式计算")
                        st.markdown('<p style="font-size:12px;color:#888;">📖 Kelly公式=数学最优仓位。基于历史胜率和盈亏比。用0.25倍Kelly（保守策略），防止过度自信</p>', unsafe_allow_html=True)
                    
                    with risk_cols[2]:
                        st.metric("风险评级", risk_metrics.get('risk_grade', '未评级'))
                        st.caption("💎 A级最优")
                        st.markdown('<p style="font-size:12px;color:#888;">📖 A级(&lt;3%)=低风险可重仓；B级(3-5%)=正常仓位；C级(5-8%)=轻仓；D级(&gt;8%)=不建议</p>', unsafe_allow_html=True)
                    
                    with risk_cols[3]:
                        atr = risk_metrics.get('atr', 0)
                        if atr:
                            st.metric("ATR波动率", f"{atr:.2f}")
                            st.caption("📈 14日平均真实范围")
                            st.markdown('<p style="font-size:12px;color:#888;">📖 ATR=平均真实波幅，衡量股票每天的「正常」波动幅度。ATR越大=波动越剧烈=止损要设更宽</p>', unsafe_allow_html=True)
                    
                    st.divider()
                    
                    # 4. AI智能风控预测（合并预测+风控，一键触发）
                    st.markdown("#### 🤖 AI智能风控预测")
                    st.caption("💡 基于VWAP、Alpha因子、风险指标，预测未来3-5日走势 + 开仓前风控评估（潜在风险预判、盈亏比、纪律建议）")
                    
                    if MY_GEMINI_KEY:
                        prediction_cache_key = f"ai_prediction_{target_c}"
                        pm_cache_key = f"pre_mortem_{target_c}"
                        
                        run_combined = st.button(
                            "⚡ 启动AI智能风控预测",
                            key=f"btn_ai_pred_{target_c}",
                            type="primary",
                            use_container_width=True,
                            help="一键获取：AI预测（看涨概率/操作建议）+ 风控评估（三大风险预判 + 盈亏比 + 开仓建议）"
                        )
                        
                        if run_combined:
                            prog = st.progress(0)
                            status = st.empty()
                            try:
                                import time as _t
                                _start = _t.time()
                                status.text(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · 准备数据... 10%")
                                prog.progress(0.1)
                                
                                ai_prediction = predictor.call_gemini_oracle(MY_GEMINI_KEY, GEMINI_MODEL_NAME)
                                st.session_state[prediction_cache_key] = ai_prediction
                                status.text(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · 风控评估... 50%")
                                prog.progress(0.5)
                                
                                _macro_ctx = st.session_state.get('all_markets', {}).get('us_market', {})
                                _pm_predictor = predictor
                                _pm_predictor.calculate_alpha_factors()
                                _pm_predictor.calculate_risk_engine()
                                pm_result = _pm_predictor.call_gemini_pre_mortem(
                                    MY_GEMINI_KEY, GEMINI_MODEL_NAME, macro_context=_macro_ctx
                                )
                                st.session_state[pm_cache_key] = pm_result
                                
                                status.text(f"✅ 分析完成（耗时{_t.time()-_start:.1f}秒）")
                                prog.progress(1.0)
                                _t.sleep(0.5)
                            except Exception as e:
                                st.error(f"❌ AI智能风控预测失败: {str(e)[:100]}")
                                # 清除缓存，避免显示旧结果
                                if prediction_cache_key in st.session_state:
                                    del st.session_state[prediction_cache_key]
                                if pm_cache_key in st.session_state:
                                    del st.session_state[pm_cache_key]
                                ai_prediction = {}
                                pm_result = None
                            finally:
                                prog.empty()
                                status.empty()
                        
                        ai_prediction = st.session_state.get(prediction_cache_key, {})
                        pm_result = st.session_state.get(pm_cache_key)
                        
                        if ai_prediction or pm_result:
                            if ai_prediction:
                                st.markdown("##### 📈 AI预测结果")
                                ai_cols = st.columns([1, 1, 2])
                                with ai_cols[0]:
                                    prob = ai_prediction.get('bullish_prob', 50)
                                    prob_color = "🟢" if prob > 55 else ("🔴" if prob < 45 else "🟡")
                                    st.metric(f"{prob_color} 看涨概率", f"{prob}%")
                                with ai_cols[1]:
                                    st.metric("市场状态", ai_prediction.get('regime', '震荡'))
                                with ai_cols[2]:
                                    st.info(f"**操作建议**: {ai_prediction.get('verdict', '观望')}")
                                st.warning(f"⚠️ **关键风险**: {ai_prediction.get('key_risk', '需关注市场变化')}")
                                st.markdown("---")
                            
                            if pm_result:
                                st.markdown("##### 🛡️ 风控评估 - 潜在风险预判")
                                st.caption("📖 开仓前预判：若这笔交易亏损，最可能的原因")
                                _risk_items = [
                                    ('1', pm_result.get('risk_1', '分析中...')),
                                    ('2', pm_result.get('risk_2', '分析中...')),
                                    ('3', pm_result.get('risk_3', '分析中...'))
                                ]
                                for _ri_num, _ri_text in _risk_items:
                                    st.markdown(f'<div style="background: #fef2f2; border-left: 4px solid #ef4444; padding: 0.8rem 1rem; border-radius: 4px; margin-bottom: 0.5rem;"><span style="font-weight:700; color:#ef4444;">风险 #{_ri_num}</span>：{_ri_text}</div>', unsafe_allow_html=True)
                                st.markdown("##### 🎯 交易纪律")
                                _pm_risk = predictor.risk_metrics if hasattr(predictor, 'risk_metrics') else {}
                                _pm_stop = _pm_risk.get('stop_loss', 0)
                                _pm_kelly = _pm_risk.get('kelly_position', 5)
                                _pm_stop_pct = _pm_risk.get('stop_loss_pct', 0)
                                _rr_ratio = pm_result.get('reward_risk_ratio', 0)
                                _position_amt = 100000 * _pm_kelly / 100 if _pm_kelly else 0
                                _max_loss = _position_amt * _pm_stop_pct / 100 if _pm_stop_pct > 0 else 0
                                disc_cols = st.columns(4)
                                with disc_cols[0]:
                                    st.metric("硬止损价", f"{_pm_stop:.2f}" if _pm_stop > 0 else "N/A")
                                with disc_cols[1]:
                                    st.metric("建议仓位", f"{_pm_kelly:.1f}%")
                                with disc_cols[2]:
                                    st.metric("最大亏损", f"¥{_max_loss:,.0f}" if _max_loss > 0 else "N/A")
                                with disc_cols[3]:
                                    _rr_color = "#10b981" if _rr_ratio >= 2.0 else ("#f59e0b" if _rr_ratio >= 1.5 else "#ef4444")
                                    st.metric("盈亏比", f"{_rr_ratio:.1f}:1" if _rr_ratio > 0 else "N/A")
                                _pm_verdict = pm_result.get('verdict', '分析中...')
                                if '允许开仓' in _pm_verdict:
                                    _verdict_bg = "linear-gradient(135deg, #10b981 0%, #059669 100%)"
                                    _verdict_icon = "🟢"
                                elif '减半' in _pm_verdict:
                                    _verdict_bg = "linear-gradient(135deg, #f59e0b 0%, #d97706 100%)"
                                    _verdict_icon = "🟡"
                                else:
                                    _verdict_bg = "linear-gradient(135deg, #ef4444 0%, #dc2626 100%)"
                                    _verdict_icon = "🔴"
                                st.markdown(f'<div style="background: {_verdict_bg}; padding: 1.2rem 1.5rem; border-radius: 10px; margin-top: 0.5rem;"><div style="color: white; font-size: 12px; font-weight: 700;">{_verdict_icon} 风控判定：{_pm_verdict}</div></div>', unsafe_allow_html=True)
                            st.caption(f"📌 本报告由 AI 生成 · 模型: {_ai_model_label()}")
                        else:
                            st.info("👆 点击上方按钮一键获取AI预测 + 风控评估（预测未来走势 + 开仓前风险预判）")
                    else:
                        st.info("💡 配置 Gemini API Key 即可启用AI智能风控预测")
                
                except Exception as e:
                    st.error(f"预测层加载失败: {str(e)[:100]}")
                    logging.error(f"预测层异常: {e}")
        
        # ═══════════════════════════════════════════════════════════════
        # 【V90 新增】一键生成分享卡片（iPhone 17 尺寸）
        # ═══════════════════════════════════════════════════════════════
        if COPY_UTILS_AVAILABLE:
            st.markdown("---")
            st.markdown("### 📸 一键生成分享卡片")
            st.caption("📖 生成 iPhone 17 尺寸的精美分析卡片，可直接保存到相册分享给朋友")
            
            _card_col1, _card_col2 = st.columns([1, 3])
            with _card_col1:
                _gen_card = st.button("📸 生成卡片", key=f"btn_share_card_{target_c}", type="primary", use_container_width=True)
            with _card_col2:
                st.caption("包含：价格涨跌、核心指标(VWAP/ATR/Kelly)、AI止损止盈、风控官警告、宏观环境")
            
            if _gen_card:
                with st.spinner("正在生成分享卡片..."):
                    try:
                        # 收集当前所有分析数据
                        _card_price = float(df['Close'].iloc[-1])
                        _card_prev = float(df['Close'].iloc[-2]) if len(df) >= 2 else _card_price
                        _card_chg = ((_card_price - _card_prev) / _card_prev * 100) if _card_prev else 0
                        
                        # 从已有的分析结果中提取数据
                        _card_alpha = _chart_predictor.alpha_factors if _chart_predictor and hasattr(_chart_predictor, 'alpha_factors') else {}
                        _card_risk = _chart_predictor.risk_metrics if _chart_predictor and hasattr(_chart_predictor, 'risk_metrics') else {}
                        
                        # 宏观数据
                        _card_macro = st.session_state.get('all_markets', {}).get('us_market', {})
                        _card_macro_v = _card_macro.get('verdict', '') if _card_macro.get('data_ok', False) else ''
                        
                        # AI入场顾问数据（如果有）
                        _card_ea = None
                        for _ck in st.session_state:
                            if _ck.startswith(f"entry_advisor_{target_c}"):
                                _card_ea = st.session_state[_ck]
                                break
                        
                        # Pre-Mortem数据（如果有）
                        _card_pm = None
                        for _pk in st.session_state:
                            if _pk.startswith(f"pre_mortem_{target_c}"):
                                _card_pm = st.session_state[_pk]
                                break
                        
                        _pm_risks = []
                        if _card_pm:
                            for _rk in ['risk_1', 'risk_2', 'risk_3']:
                                _rv = _card_pm.get(_rk, '')
                                if _rv and '分析中' not in _rv:
                                    _pm_risks.append(_rv)
                        
                        # 提取最近30天收盘价用于迷你走势图
                        _recent_closes = []
                        try:
                            _rc_series = df['Close'].tail(30)
                            _recent_closes = [float(v) for v in _rc_series.values if v == v]  # 排除NaN
                        except Exception:
                            _recent_closes = []
                        
                        # 尝试获取AI市场简报（如果有）
                        _card_brief = ""
                        for _brief_key in st.session_state:
                            if _brief_key.startswith("market_brief_") and isinstance(st.session_state[_brief_key], str):
                                _card_brief = st.session_state[_brief_key][:300]  # 限制长度
                                break
                        
                        # 生成卡片
                        _card_bytes = ShareCardGenerator.generate_stock_card(
                            code=target_c,
                            price=_card_price,
                            change_pct=_card_chg,
                            score=int(m.get('score', 0)) if m else 0,
                            suggestion=m.get('suggestion', '') if m else '',
                            vwap=_card_alpha.get('vwap_20', 0),
                            vwap_dev=_card_alpha.get('vwap_deviation', 0),
                            atr=_card_risk.get('atr', 0),
                            stop_loss=_card_risk.get('stop_loss', 0),
                            kelly_pct=_card_risk.get('kelly_position', 0),
                            risk_grade=_card_risk.get('risk_grade', ''),
                            entry_grade=_card_ea.get('entry_grade', '') if _card_ea else '',
                            ai_stop_loss=_card_ea.get('stop_loss', 0) if _card_ea else 0,
                            ai_tp1=_card_ea.get('take_profit_1', 0) if _card_ea else 0,
                            ai_tp2=_card_ea.get('take_profit_2', 0) if _card_ea else 0,
                            ai_strategy=_card_ea.get('strategy_summary', '') if _card_ea else '',
                            macro_verdict=_card_macro_v,
                            position_cap=_card_macro.get('position_cap', 80),
                            pre_mortem_risks=_pm_risks,
                            recent_prices=_recent_closes,
                            market_brief=_card_brief
                        )
                        
                        st.session_state[f"share_card_{target_c}"] = _card_bytes
                        st.success("✅ 分享卡片已生成！")
                    
                    except Exception as _card_err:
                        st.error(f"❌ 卡片生成失败: {str(_card_err)[:80]}")
            
            # 显示和下载卡片
            _card_cache_key = f"share_card_{target_c}"
            if _card_cache_key in st.session_state:
                _card_data = st.session_state[_card_cache_key]
                
                _show_col1, _show_col2 = st.columns([2, 1])
                with _show_col1:
                    st.image(_card_data, caption=f"{target_c} 分析卡片（iPhone 17 尺寸）", use_container_width=True)
                with _show_col2:
                    st.download_button(
                        "📥 保存卡片到本地",
                        data=_card_data,
                        file_name=f"StockAI_{target_c}_{datetime.now().strftime('%Y%m%d_%H%M')}.png",
                        mime="image/png",
                        key=f"download_card_{target_c}",
                        use_container_width=True
                    )
                    st.caption("💡 保存后可直接在微信/钉钉/朋友圈分享")
                    st.caption("📐 卡片尺寸：1170×2532px（iPhone 17 Pro Max）")
        
        # 【V89.2】机构研究中心 - 个股深度分析
        if INSTITUTIONAL_RESEARCH_AVAILABLE and _institutional_research:
            st.markdown("---")
            from datetime import datetime as _dt_research
            st.markdown(f"### 🏦 机构研究报告 · {_dt_research.now().strftime('%Y-%m-%d')}")
            
            with st.expander("📑 **个股深度研究 + 机会风险评估**", expanded=True):
                try:
                    # 【V91.9】机构研究报告缓存：避免每次 rerun 都调 LLM，Running 结束后按钮可正常响应
                    # 【V91.10】统一缓存：交易日15分钟，非交易日24小时
                    _research_cache_key = f"_research_report_{target_c}"
                    _research_ttl = get_smart_cache_ttl('daily')
                    _research_now = _time_module.time()
                    _research_cached = (_research_cache_key in st.session_state and
                        (_research_now - st.session_state.get(f"{_research_cache_key}_ts", 0)) <= _research_ttl)
                    if _research_cached:
                        research_report = st.session_state[_research_cache_key]
                        _safe_print(f"[机构研究] 使用缓存报告")
                    else:
                        # 【V91.10】延迟加载：点击生成才执行，首屏秒开，目标 20 秒内完成
                        _gen_btn_key = f"_research_gen_btn_{target_c}"
                        if st.button("🚀 生成机构研究报告", key=_gen_btn_key, type="primary"):
                            with st.spinner(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · 机构研究报告..."):
                                try:
                                    _all_mkts = st.session_state.get('all_markets', {})
                                    if target_c.endswith('.SS') or target_c.endswith('.SZ'):
                                        market_regime = _all_mkts.get('cn_market', {})
                                    elif target_c.endswith('.HK'):
                                        market_regime = _all_mkts.get('hk_market', {})
                                    else:
                                        market_regime = _all_mkts.get('us_market', {})
                                    _df_r = df.tail(126) if df is not None and len(df) > 126 else df  # 半年数据，加快生成
                                    research_report = _institutional_research.comprehensive_report(
                                        target_c, _df_r, market_regime
                                    )
                                    if research_report and isinstance(research_report, dict) and len(research_report) > 0:
                                        st.session_state[_research_cache_key] = research_report
                                        st.session_state[f"{_research_cache_key}_ts"] = _research_now
                                        st.rerun()
                                    else:
                                        st.error("❌ 报告生成失败（返回为空），请检查数据或稍后重试")
                                        research_report = None
                                except Exception as _re:
                                    st.error(f"❌ 机构研究报告生成异常: {str(_re)[:100]}")
                                    logging.error(f"机构研究报告异常: {_re}")
                                    research_report = None
                        else:
                            research_report = None
                            st.info("👆 点击上方按钮生成机构研究报告（约 20 秒内完成，报告生成后缓存：交易日 15 分钟 / 非交易日 24 小时）")
                    
                    if research_report:
                        # 执行摘要
                        st.markdown("#### 📋 执行摘要")
                        exec_summary = research_report.get('executive_summary', {})
                        
                        summary_cols = st.columns(5)
                        with summary_cols[0]:
                            rating = exec_summary.get('rating', '未评级')
                            rating_color = "#10b981" if '推荐' in rating else "#f59e0b" if '中性' in rating else "#ef4444"
                            st.markdown(f'<div style="background: {rating_color}20; padding: 0.8rem; border-radius: 6px; border-left: 3px solid {rating_color};"><div style="font-size: 12px; color: gray;">综合评级</div><div style="font-size: 12px; font-weight: 600; color: {rating_color}; margin-top: 0.2rem;">{rating}</div></div>', unsafe_allow_html=True)
                        
                        with summary_cols[1]:
                            action = exec_summary.get('action', '观望')
                            st.markdown(f'<div style="background: #3b82f620; padding: 0.8rem; border-radius: 6px; border-left: 3px solid #3b82f6;"><div style="font-size: 12px; color: gray;">操作建议</div><div style="font-size: 12px; font-weight: 600; color: #3b82f6; margin-top: 0.2rem;">{action}</div></div>', unsafe_allow_html=True)
                        
                        with summary_cols[2]:
                            tech_score = exec_summary.get('technical_score', 0)
                            st.metric("技术评分", f"{tech_score}/100", 
                                     delta="优秀" if tech_score >= 70 else "一般" if tech_score >= 50 else "较弱")
                        
                        with summary_cols[3]:
                            opp_score = exec_summary.get('opportunity_score', 0)
                            st.metric("机会评分", f"{opp_score}/100",
                                     delta="高机会" if opp_score >= 70 else "中等" if opp_score >= 50 else "低机会")
                        
                        with summary_cols[4]:
                            risk_score = exec_summary.get('risk_score', 50)
                            st.metric("风险评分", f"{risk_score}/100",
                                     delta="高风险" if risk_score >= 70 else "中等" if risk_score >= 40 else "低风险",
                                     delta_color="inverse")
                        
                        st.divider()
                        
                        # Tab布局：个股研究 | 机会雷达 | 风险预警 | 舆情分析
                        research_tabs = st.tabs(["📊 个股深度研究", "🎯 机会雷达", "⚠️ 风险预警", "📰 舆情分析"])
                        
                        # Tab 1: 个股深度研究
                        with research_tabs[0]:
                            stock_research = research_report.get('stock_research', {})
                            
                            # 趋势分析
                            st.markdown("##### 📈 趋势分析")
                            trend = stock_research.get('trend_analysis', {})
                            trend_col1, trend_col2 = st.columns(2)
                            
                            with trend_col1:
                                st.info(f"**趋势状态**: {trend.get('status', '未知')}")
                                st.metric("趋势评分", f"{trend.get('score', 0)}/100")
                            
                            with trend_col2:
                                st.metric("MA5", f"${trend.get('ma5', 0):.2f}")
                                st.metric("MA20", f"${trend.get('ma20', 0):.2f}")
                                st.metric("MA50", f"${trend.get('ma50', 0):.2f}")
                            
                            st.caption(f"💡 偏离MA20: {trend.get('deviation_from_ma20', 0):+.2f}%")
                            
                            # 动量信号
                            st.markdown("##### ⚡ 动量信号")
                            signals = stock_research.get('momentum_signals', [])
                            if signals:
                                for signal in signals:
                                    if '⚠️' in signal or '🔴' in signal:
                                        st.warning(signal)
                                    elif '✅' in signal or '🟢' in signal:
                                        st.success(signal)
                                    else:
                                        st.info(signal)
                            
                            # 价格目标
                            st.markdown("##### 🎯 价格目标")
                            target = stock_research.get('price_target', {})
                            target_cols = st.columns(4)
                            
                            with target_cols[0]:
                                st.metric("当前价", f"${target.get('current_price', 0):.2f}")
                            with target_cols[1]:
                                st.metric("目标高位", f"${target.get('target_high', 0):.2f}", delta="上涨空间")
                            with target_cols[2]:
                                st.metric("目标低位", f"${target.get('target_low', 0):.2f}", delta="下跌空间", delta_color="inverse")
                            with target_cols[3]:
                                st.metric("止损价", f"${target.get('stop_loss', 0):.2f}", delta="风控线", delta_color="inverse")
                            
                            st.caption(f"⏰ 时间视野: {target.get('time_horizon', '1-2月')}")
                        
                        # Tab 2: 机会雷达
                        with research_tabs[1]:
                            opportunity = research_report.get('opportunity', {})
                            
                            # 机会评分卡
                            st.markdown("##### 🎯 机会评估")
                            opp_col1, opp_col2, opp_col3 = st.columns(3)
                            
                            with opp_col1:
                                opp_level = opportunity.get('opportunity_level', '低')
                                st.markdown(f'<div style="background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%); padding: 1.2rem; border-radius: 8px; color: white;"><div style="font-size: 12px; opacity: 0.9;">机会等级</div><div style="font-size: 12px; font-weight: 600; margin-top: 0.3rem;">{opp_level}</div></div>', unsafe_allow_html=True)
                            
                            with opp_col2:
                                entry_timing = opportunity.get('entry_timing', '观望')
                                st.markdown(f'<div style="background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%); padding: 1.2rem; border-radius: 8px; color: white;"><div style="font-size: 12px; opacity: 0.9;">入场时机</div><div style="font-size: 12px; font-weight: 600; margin-top: 0.3rem;">{entry_timing}</div></div>', unsafe_allow_html=True)
                            
                            with opp_col3:
                                position_size = opportunity.get('position_size_suggestion', '轻仓')
                                st.markdown(f'<div style="background: linear-gradient(135deg, #8b5cf6 0%, #7c3aed 100%); padding: 1.2rem; border-radius: 8px; color: white;"><div style="font-size: 12px; opacity: 0.9;">建议仓位</div><div style="font-size: 12px; font-weight: 600; margin-top: 0.3rem;">{position_size}</div></div>', unsafe_allow_html=True)
                            
                            # 催化剂
                            st.markdown("##### 💡 催化剂分析")
                            catalysts = opportunity.get('catalysts', [])
                            if catalysts:
                                for i, catalyst in enumerate(catalysts, 1):
                                    st.success(f"**{i}.** {catalyst}")
                            else:
                                st.info("暂无明确催化剂")
                            
                            # 最优入场价
                            st.markdown("##### 💰 最优入场价")
                            optimal_price = opportunity.get('optimal_entry_price', 0)
                            st.metric("建议买入价", f"${optimal_price:.2f}")
                            st.caption("💡 根据技术分析和风险收益比计算")
                        
                        # Tab 3: 风险预警
                        with research_tabs[2]:
                            risk_warn = research_report.get('risk_warning', {})
                            
                            # 风险等级
                            st.markdown("##### ⚠️ 风险评估")
                            risk_level = risk_warn.get('risk_level', '中')
                            risk_color = "#ef4444" if '高' in risk_level else "#f59e0b" if '中' in risk_level else "#10b981"
                            
                            risk_col1, risk_col2 = st.columns([1, 2])
                            with risk_col1:
                                st.markdown(f'<div style="background: {risk_color}20; padding: 1.5rem; border-radius: 8px; border: 2px solid {risk_color}; text-align: center;"><div style="font-size: 12px; color: gray;">风险等级</div><div style="font-size: 12px; font-weight: 700; color: {risk_color}; margin-top: 0.5rem;">{risk_level}</div></div>', unsafe_allow_html=True)
                            
                            with risk_col2:
                                st.metric("风险评分", f"{risk_warn.get('risk_score', 50)}/100",
                                         delta="高风险" if risk_warn.get('risk_score', 50) >= 70 else "低风险",
                                         delta_color="inverse")
                                
                                max_dd = risk_warn.get('max_drawdown_tolerance', 0.15)
                                st.metric("最大回撤容忍", f"{max_dd*100:.1f}%")
                            
                            # 风险因素
                            st.markdown("##### 🚨 风险因素")
                            risk_factors = risk_warn.get('risk_factors', [])
                            if risk_factors:
                                for factor in risk_factors:
                                    st.warning(factor)
                            else:
                                st.success("✅ 暂无重大风险因素")
                            
                            # 风险缓释建议
                            st.markdown("##### 🛡️ 风险缓释建议")
                            mitigations = risk_warn.get('risk_mitigation', [])
                            if mitigations:
                                for i, mitigation in enumerate(mitigations, 1):
                                    st.info(f"**{i}.** {mitigation}")
                            
                            # 止损价
                            st.markdown("##### 🔻 止损管理")
                            stop_loss_price = risk_warn.get('stop_loss_price', 0)
                            if stop_loss_price > 0:
                                st.error(f"**严格止损价**: ${stop_loss_price:.2f}")
                                st.caption("⚠️ 跌破该价位应立即止损，不抱幻想")
                        
                        # Tab 4: 舆情分析
                        with research_tabs[3]:
                            st.markdown("##### 📰 AI舆情分析")
                            st.caption("💡 基于最新市场新闻、公司公告、行业动态的综合舆情评估")
                            
                            # 【V89.4】使用舆情分析器生成提示词
                            if SENTIMENT_ANALYZER_AVAILABLE and _sentiment_analyzer and MY_GEMINI_KEY:
                                # 生成按钮（使用session_state缓存）
                                sentiment_cache_key = f"sentiment_{target_c}"
                                
                                run_sentiment = st.button(
                                    "🚀 启动舆情分析", 
                                    key=f"btn_sentiment_{target_c}",
                                    type="primary",
                                    use_container_width=True,
                                    help="AI分析最新新闻、市场情绪、影响预判"
                                )
                                
                                if run_sentiment:
                                    # 【V89.7 修复】根据股票代码选择正确的市场regime
                                    _all_mkts_sent = st.session_state.get('all_markets', {})
                                    if target_c.endswith('.SS') or target_c.endswith('.SZ'):
                                        _sent_regime = _all_mkts_sent.get('cn_market', {})
                                    elif target_c.endswith('.HK'):
                                        _sent_regime = _all_mkts_sent.get('hk_market', {})
                                    else:
                                        _sent_regime = _all_mkts_sent.get('us_market', {})
                                    
                                    prompt = _sentiment_analyzer.generate_stock_sentiment_prompt(
                                        target_c, df, _sent_regime
                                    )
                                    
                                    # 进度显示
                                    sentiment_progress = st.progress(0)
                                    sentiment_status = st.empty()
                                    
                                    try:
                                        sentiment_status.info(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · 舆情分析...")
                                        sentiment_progress.progress(0.2)
                                        
                                        sentiment_status.info(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · 评估市场情绪...")
                                        sentiment_progress.progress(0.4)
                                        
                                        sentiment_status.info(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · 预判影响...")
                                        sentiment_progress.progress(0.6)
                                        
                                        # 调用AI
                                        ai_response = call_gemini_api(prompt)
                                        sentiment_progress.progress(1.0)
                                        
                                        # 【V91.8】API 返回错误时直接提示，不当作成功报告
                                        if isinstance(ai_response, str) and (ai_response.startswith("❌") or "失败" in ai_response[:20]):
                                            sentiment_progress.empty()
                                            sentiment_status.empty()
                                            st.error(ai_response[:150])
                                        else:
                                            # 解析评分
                                            sentiment_metrics = _sentiment_analyzer.parse_sentiment_score(ai_response)
                                            
                                            # 缓存结果
                                            st.session_state[sentiment_cache_key] = {
                                                'response': ai_response,
                                                'metrics': sentiment_metrics
                                            }
                                            
                                            sentiment_progress.empty()
                                            sentiment_status.empty()
                                    
                                    except Exception as e:
                                        sentiment_progress.empty()
                                        sentiment_status.empty()
                                        st.error(f"❌ 舆情分析失败: {str(e)[:80]}")
                                
                                # 显示结果
                                if sentiment_cache_key in st.session_state:
                                    sentiment_data = st.session_state[sentiment_cache_key]
                                    sentiment_metrics = sentiment_data.get('metrics', {})
                                    ai_response = sentiment_data.get('response', '')
                                    
                                    # 显示舆情评分卡
                                    st.markdown("---")
                                    st.markdown("##### 📊 舆情评分卡")
                                    
                                    sent_col1, sent_col2, sent_col3 = st.columns(3)
                                    
                                    with sent_col1:
                                        score = sentiment_metrics.get('sentiment_score', 50)
                                        color = _sentiment_analyzer.get_sentiment_color(score)
                                        icon = _sentiment_analyzer.get_sentiment_icon(score)
                                        st.markdown(f'<div style="background: {color}20; padding: 1rem; border-radius: 8px; border-left: 4px solid {color};"><div style="font-size: 12px; color: gray;">舆情评分</div><div style="font-size: 12px; font-weight: 700; color: {color}; margin-top: 0.3rem;">{icon} {score}/100</div></div>', unsafe_allow_html=True)
                                    
                                    with sent_col2:
                                        level = sentiment_metrics.get('sentiment_level', '中性')
                                        st.metric("舆情等级", level)
                                    
                                    with sent_col3:
                                        impact = sentiment_metrics.get('short_term_impact', '震荡')
                                        impact_icon = "📈" if impact == '上涨' else "📉" if impact == '下跌' else "📊"
                                        st.metric("短期影响", f"{impact_icon} {impact}")
                                    
                                    st.markdown("---")
                                    
                                    # 显示完整报告
                                    st.markdown("##### 📑 完整舆情报告")
                                    st.markdown("""
                                    <style>
                                    .sentiment-report {
                                        font-size: 12px !important;
                                        line-height: 1.8;
                                        color: #374151;
                                        padding: 1rem;
                                        background-color: #f9fafb;
                                        border-radius: 8px;
                                        border-left: 4px solid #3b82f6;
                                    }
                                    .sentiment-report h2 {
                                        font-size: 12px !important;
                                        font-weight: 600 !important;
                                        margin: 1rem 0 0.5rem 0 !important;
                                        color: #1f2937 !important;
                                    }
                                    </style>
                                    """, unsafe_allow_html=True)
                                    
                                    # 【V90.3】段落级复制
                                    if COPY_UTILS_AVAILABLE:
                                        CopyUtils.render_markdown_with_section_copy(ai_response, key_prefix=f"sent_{target_c}")
                                    else:
                                        st.markdown(f'<div class="sentiment-report">{ai_response}</div>', unsafe_allow_html=True)
                                    st.caption(f"📌 本报告由 AI 生成 · 模型: {_ai_model_label()}")
                                else:
                                    st.info("👆 点击上方按钮启动AI舆情分析（分析新闻、市场情绪、影响预判）")
                            
                            elif not MY_GEMINI_KEY:
                                st.info("💡 配置 Gemini API Key 即可启用AI舆情分析功能")
                            else:
                                st.warning("⚠️ 舆情分析模块未加载")
                
                except Exception as e:
                    st.error(f"机构研究报告生成失败: {str(e)[:100]}")
                    logging.error(f"机构研究异常: {e}")
        
        # 【V90.3】独立个股舆情区块已整合到上方「机构研究报告→舆情分析」Tab中，不再重复显示
        
        # 【V89.5】一键生成完整报告
        if COPY_UTILS_AVAILABLE:
            st.markdown("---")
            st.markdown("### 📄 一键生成完整报告")
            st.caption("💡 汇总所有分析结果，生成可复制的完整投资报告")
            
            if st.button("📋 生成完整报告", key="btn_generate_full_report", type="primary", use_container_width=True):
                with st.status(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · 正在生成完整报告", expanded=False):
                    try:
                        # 收集所有数据
                        report_data = {
                            'code': q_input,
                            'name': target_c,
                            'stock_data': df,
                            'quant_metrics': quant,
                            'risk_metrics': risk_metrics,
                            'alpha_factors': alpha_factors,
                        }
                        
                        # 收集机构研究数据（如果存在）
                        if 'research_report' in locals():
                            report_data['institutional_research'] = research_report
                        
                        # 收集舆情分析数据（如果存在）
                        sentiment_cache_key = f"sentiment_{target_c}"
                        if sentiment_cache_key in st.session_state:
                            report_data['sentiment_analysis'] = st.session_state[sentiment_cache_key]
                        
                        # 生成报告
                        full_report = ReportGenerator.generate_stock_summary_report(
                            code=report_data['code'],
                            name=report_data['name'],
                            stock_data=report_data['stock_data'],
                            quant_metrics=report_data.get('quant_metrics'),
                            risk_metrics=report_data.get('risk_metrics'),
                            alpha_factors=report_data.get('alpha_factors'),
                            institutional_research=report_data.get('institutional_research'),
                            sentiment_analysis=report_data.get('sentiment_analysis')
                        )
                        
                        # 显示报告预览
                        st.success("✅ 报告生成成功！")
                        
                        # 使用expander显示报告
                        with st.expander("📖 查看完整报告", expanded=True):
                            st.markdown(full_report)
                            st.caption(f"📌 本报告由 AI 生成 · 模型: {_ai_model_label()}")
                        
                        # 【V90.2】复制按钮
                        if COPY_UTILS_AVAILABLE:
                            CopyUtils.create_copy_button(full_report, button_text="📋 复制完整报告", key=f"copy_full_rpt_{target_c}")
                    
                    except Exception as e:
                        st.error(f"❌ 生成报告失败: {str(e)[:100]}")
                        logging.error(f"生成报告异常: {e}")
            
            st.markdown("---")
        
        # 量化指标
        st.markdown("#### 📊 量化回测指标")
        
        # 【V87.16】显示MACD和Bollinger Bands
        if quant.get('macd_signal'):
            st.markdown("##### 🔧 高级技术指标")
            tech_col1, tech_col2 = st.columns(2)
            
            with tech_col1:
                st.markdown("**MACD指标**")
                st.caption(f"MACD: {quant.get('macd', 'N/A')} | Signal: {quant.get('signal', 'N/A')}")
                st.caption(f"柱状图: {quant.get('histogram', 'N/A')}")
                st.info(quant.get('macd_signal', 'N/A'))
            
            with tech_col2:
                st.markdown("**布林带 (Bollinger Bands)**")
                st.caption(f"上轨: {quant.get('bb_upper', 'N/A')} | 中轨: {quant.get('bb_middle', 'N/A')} | 下轨: {quant.get('bb_lower', 'N/A')}")
                st.caption(f"带宽: {quant.get('bb_width', 'N/A')}")
                st.info(quant.get('bb_position', 'N/A'))
            
            st.divider()
        
        st.markdown("##### 📈 基础回测指标")
        qc1, qc2, qc3, qc4, qc5 = st.columns(5)
        # 【V87.4】量化指标趋势判断
        def get_quant_desc(metric, value):
            if value == 'N/A' or value is None:
                return "数据不足"
            
            try:
                val = float(value.replace('%', '')) if isinstance(value, str) else float(value)
            except:
                return "数据异常"
            
            if metric == "夏普比率":
                if val > 2:
                    return "优秀 - 风险收益极佳"
                elif val > 1:
                    return "良好 - 风险收益不错"
                elif val > 0:
                    return "一般 - 收益覆盖风险"
                else:
                    return "较差 - 风险大于收益"
            elif metric == "最大回撤":
                val = abs(val)  # 确保为正值
                if val < 10:
                    return "优秀 - 回撤很小"
                elif val < 20:
                    return "良好 - 回撤可控"
                elif val < 30:
                    return "一般 - 回撤较大"
                else:
                    return "较差 - 回撤严重"
            elif metric == "胜率":
                if val > 60:
                    return "优秀 - 胜率很高"
                elif val > 50:
                    return "良好 - 胜率过半"
                elif val > 40:
                    return "一般 - 胜率偏低"
                else:
                    return "较差 - 胜率很低"
            elif metric == "盈亏比":
                if val > 2:
                    return "优秀 - 盈利远超亏损"
                elif val > 1.5:
                    return "良好 - 盈利大于亏损"
                elif val > 1:
                    return "一般 - 盈利略大于亏损"
                else:
                    return "较差 - 盈利小于亏损"
            else:
                return "正常范围"
        
        q_labels = [
            ("夏普比率", quant.get('sharpe', 'N/A'), get_quant_desc("夏普比率", quant.get('sharpe', 'N/A'))),
            ("最大回撤", quant.get('max_dd', 'N/A'), get_quant_desc("最大回撤", quant.get('max_dd', 'N/A'))),
            ("年化波动", quant.get('volatility', 'N/A'), "价格波动幅度"),
            ("胜率", quant.get('win_rate', 'N/A'), get_quant_desc("胜率", quant.get('win_rate', 'N/A'))),
            ("盈亏比", quant.get('pl_ratio', 'N/A'), get_quant_desc("盈亏比", quant.get('pl_ratio', 'N/A')))
        ]
        for col, (title, val, desc) in zip([qc1, qc2, qc3, qc4, qc5], q_labels):
            with col:
                st.markdown(
                    f'<div class="ai-card"><div class="ai-title">{title}</div><div style="font-size:12px;font-weight:bold;color:#2563eb;">{val}</div><div style="font-size:12px;color:#666;">{desc}</div></div>',
                    unsafe_allow_html=True
                )
        
        st.divider()
        
        # 【V83 P0.2】基准对比与风险指标
        if risk_metrics:
            # 【修复】安全获取benchmark_name，避免KeyError
            benchmark_name = risk_metrics.get('benchmark_name', '市场基准')
            st.markdown(f"#### 🎯 基准对比分析 (对比{benchmark_name})")
            rc1, rc2, rc3, rc4 = st.columns(4)
            # 【V87.4】增强基准对比指标说明
            alpha_val = risk_metrics.get('alpha', 0)
            beta_val = risk_metrics.get('beta', 1)
            corr_val = risk_metrics.get('correlation', 0)
            vol_val = risk_metrics.get('volatility', 0)
            
            # Alpha说明
            if alpha_val > 0.1:
                alpha_desc = "显著跑赢基准"
            elif alpha_val > 0:
                alpha_desc = "略微跑赢基准"
            elif alpha_val > -0.1:
                alpha_desc = "与基准持平"
            else:
                alpha_desc = "明显跑输基准"
                
            # Beta说明
            if beta_val > 1.3:
                beta_desc = "高风险高收益"
            elif beta_val > 1.1:
                beta_desc = "略高于市场"
            elif beta_val > 0.9:
                beta_desc = "与市场同步"
            elif beta_val > 0.7:
                beta_desc = "相对稳健"
            else:
                beta_desc = "低风险资产"
                
            # 相关系数说明
            if abs(corr_val) > 0.8:
                corr_desc = "高度相关"
            elif abs(corr_val) > 0.5:
                corr_desc = "中度相关"
            else:
                corr_desc = "相关性较弱"
                
            # 波动率说明
            if vol_val > 0.4:
                vol_desc = "波动剧烈"
            elif vol_val > 0.25:
                vol_desc = "波动较大"
            elif vol_val > 0.15:
                vol_desc = "波动适中"
            else:
                vol_desc = "波动较小"
            
            risk_labels = [
                ("Alpha (α)", f"{alpha_val*100:+.2f}%", alpha_desc, "#10b981" if alpha_val > 0 else "#ef4444"),
                ("Beta (β)", f"{beta_val:.2f}", beta_desc, "#6366f1"),
                ("相关系数 (ρ)", f"{corr_val:.2f}", corr_desc, "#8b5cf6"),
                ("波动率 (σ)", f"{vol_val*100:.1f}%", vol_desc, "#f59e0b")
            ]
            for col, (title, val, desc, color) in zip([rc1, rc2, rc3, rc4], risk_labels):
                with col:
                    st.markdown(
                        f'<div class="ai-card"><div class="ai-title">{title}</div><div style="font-size:12px;font-weight:bold;color:{color};">{val}</div><div style="font-size:12px;color:#666;">{desc}</div></div>',
                        unsafe_allow_html=True
                    )
            
            # 解读Alpha和Beta（使用之前已定义的变量）
            alpha_text = "✅ 超越基准" if alpha_val > 0 else "⚠️ 跑输基准"
            beta_text = "高波动" if beta_val > 1.2 else ("低波动" if beta_val < 0.8 else "适中波动")
            st.caption(f"💡 **解读**: {alpha_text}，{beta_text}，与基准相关性{'强' if abs(corr_val) > 0.7 else '中等'}")
            st.divider()
        
        # 综合分析面板
        st.markdown("### 🎯 综合分析")
        kl_col1, kl_col2, kl_col3 = st.columns([1, 1, 1])
        with kl_col1:
            st.metric("综合评分", f"{metrics.get('score', 0)}/100", delta=f"{metrics.get('logic', '计算中')}")
        with kl_col2:
            st.metric("交易建议", metrics.get('suggestion', '观望'))
        with kl_col3:
            st.info(f"**K线形态**\n\n{metrics.get('pattern', '无数据')}")
        
        st.divider()
        
        # 双核评级
        st.markdown("### 🏛️ 三核评级引擎（CANSLIM + 专业投机 + ESG）")
        c_score1, c_score2 = st.columns(2)
        
        with c_score1:
            st.markdown("#### 🦅 CANSLIM 因子")
            st.table(pd.DataFrame(metrics['canslim_rows']))
        
        with c_score2:
            st.markdown("#### 🚀 专业投机原理")
            st.table(pd.DataFrame(metrics['spec_rows']))
        
        # 【V89.7 新增】ESG评级面板
        st.markdown("---")
        _esg_rows = metrics.get('esg_rows', [])
        _esg_total = metrics.get('esg_total', 0)
        _esg_grade = metrics.get('esg_grade', 'N/A')
        _esg_label = metrics.get('esg_label', '')
        _esg_e = metrics.get('esg_e', 0)
        _esg_s = metrics.get('esg_s', 0)
        _esg_g = metrics.get('esg_g', 0)
        
        st.markdown("#### 🌍 ESG 可持续发展评级")
        esg_c1, esg_c2, esg_c3, esg_c4 = st.columns(4)
        with esg_c1:
            st.metric("🌿 环境 (E)", f"{_esg_e}/100", help="基于波动率稳定性评估经营可持续性")
        with esg_c2:
            st.metric("👥 社会 (S)", f"{_esg_s}/100", help="基于成交活跃度评估市场认可度")
        with esg_c3:
            st.metric("🏛️ 治理 (G)", f"{_esg_g}/100", help="基于均线趋势评估管理执行力")
        with esg_c4:
            st.metric("📊 ESG综合", f"{_esg_total}/100 ({_esg_grade})", help="E×30%+S×30%+G×40%")
        
        if _esg_rows:
            st.table(pd.DataFrame(_esg_rows))
        
        # 【V91.10】长线法宝评级（LongCompounder + 安全边际，同CANSLIM/ESG表格形式）
        if LongCompounderGate:
            try:
                lc_gate = LongCompounderGate()
                lc_result = lc_gate.compute(df, target_c)
                lc_rows = []
                lc_score = lc_result.get("long_compounder_score", 50)
                lc_pass = lc_result.get("passes_long_compounder_gate", False)
                for fname, fkey, desc in [
                    ("护城河代理", "moat_proxy", "价格稳定性、趋势稳健"),
                    ("ROIC代理", "roic_proxy", "动量与回撤质量"),
                    ("FCF质量代理", "fcf_quality_proxy", "量价配合"),
                    ("利润率稳定代理", "margin_stability_proxy", "低波动=稳定性高"),
                ]:
                    val = lc_result.get(fkey, 0.5)
                    score_int = int(val * 100)
                    status = "✅" if score_int >= 60 else ("❌" if score_int < 40 else "🟡")
                    lc_rows.append({"要素": fname, "状态": status, "说明": desc, "得分": f"{score_int}/100"})
                lc_rows.append({"要素": "📊 长线法宝综合", "状态": "✅" if lc_pass else "❌", "说明": "护城河+ROIC+FCF+利润率稳定", "得分": f"{lc_score:.1f}/100"})
                st.markdown("#### 📜 长线法宝评级（LongCompounder 框架）")
                st.table(pd.DataFrame(lc_rows))
                st.caption("📖 长线法宝：护城河、ROIC、FCF质量、利润率稳定，≥55分通过长线门槛")
            except Exception as _lc_e:
                st.caption("📖 长线法宝：数据不足时暂不显示")
        
        # 评分权重说明
        st.caption("📐 **四维综合评分** = CANSLIM × 30% + 专业投机 × 30% + ESG × 20% + 风控 × 20%")
        
        st.divider()
        
        # 【V82.10优化】基础指标 - 增加简要说明
        c1, c2, c3, c4 = st.columns(4)
        # 【V87.4】增强指标说明 - 添加趋势判断
        rsi_val = metrics['rsi']
        bias_val = metrics['bias']
        atr_val = metrics['atr']
        
        # RSI判断
        if rsi_val > 70:
            rsi_desc = "超买区间，可能回调"
            rsi_color = "🔴"
        elif rsi_val > 50:
            rsi_desc = "强势区间，趋势向上"
            rsi_color = "🟢"
        elif rsi_val > 30:
            rsi_desc = "弱势区间，趋势向下"
            rsi_color = "🟡"
        else:
            rsi_desc = "超卖区间，可能反弹"
            rsi_color = "🟢"
            
        # 乖离率判断
        if abs(bias_val) > 10:
            bias_desc = "严重偏离，注意风险"
            bias_color = "🔴"
        elif abs(bias_val) > 5:
            bias_desc = "适度偏离，正常波动"
            bias_color = "🟡"
        else:
            bias_desc = "贴近均线，走势平稳"
            bias_color = "🟢"
        
        # 【V87.4】ATR波动幅度判断
        if atr_val > 10:
            atr_desc = "波动极大，高风险高收益"
            atr_color = "🔴"
        elif atr_val > 5:
            atr_desc = "波动较大，注意风险"
            atr_color = "🟡"
        elif atr_val > 2:
            atr_desc = "波动适中，正常范围"
            atr_color = "🟢"
        else:
            atr_desc = "波动较小，相对稳定"
            atr_color = "🟢"
        
        c1.metric("最新价", f"{df['Close'].iloc[-1]:.2f}")
        c2.metric("RSI (相对强弱)", f"{rsi_val:.1f}", help=f"RSI指标：{rsi_color} {rsi_desc}")
        c3.metric("乖离率 (偏离度)", f"{bias_val:.2f}%", help=f"价格偏离度：{bias_color} {bias_desc}")
        c4.metric("ATR (波动幅度)", f"{atr_val:.2f}", help=f"波动性指标：{atr_color} {atr_desc}")
        
        # 【V87.4】在指标下方显示更明显的说明
        st.caption(f"💡 **指标解读**: RSI {rsi_val:.1f} {rsi_color}{rsi_desc} | 乖离率 {bias_val:.2f}% {bias_color}{bias_desc} | ATR {atr_val:.2f} {atr_color}{atr_desc}")
        
        # 蒙特卡洛预测
        if mc:
            st.markdown("#### 🔮 蒙特卡洛推演 (10日)")
            m1, m2, m3 = st.columns(3)
            curr = df['Close'].iloc[-1]
            p90_chg = (mc['p90'] - curr) / curr * 100 if curr > 0 else 0
            p50_chg = (mc['p50'] - curr) / curr * 100 if curr > 0 else 0
            p10_chg = (mc['p10'] - curr) / curr * 100 if curr > 0 else 0
            # 【V87.4】蒙特卡洛预测趋势说明
            def get_trend_desc(change_pct):
                if change_pct > 15:
                    return "大幅上涨"
                elif change_pct > 5:
                    return "温和上涨"
                elif change_pct > -5:
                    return "横盘震荡"
                elif change_pct > -15:
                    return "温和下跌"
                else:
                    return "大幅下跌"
            
            with m1: 
                st.metric("乐观 (P90)", f"{mc['p90']:.2f}", f"{p90_chg:+.1f}%")
                st.caption(f"📈 {get_trend_desc(p90_chg)}")
            with m2: 
                st.metric("中性 (P50)", f"{mc['p50']:.2f}", f"{p50_chg:+.1f}%")
                st.caption(f"📊 {get_trend_desc(p50_chg)}")
            with m3: 
                st.metric("悲观 (P10)", f"{mc['p10']:.2f}", f"{p10_chg:+.1f}%")
                st.caption(f"📉 {get_trend_desc(p10_chg)}")
        
        st.divider()
        
        # AI 分析
        from datetime import datetime as _dt_ai_analyst
        st.markdown(f"#### 💬 AI 智能分析师 · {_dt_ai_analyst.now().strftime('%Y-%m-%d')}")
        tab_daily, tab_weekly, tab_qa = st.tabs(["📅 日线复盘", "🔭 周线波段", "💬 通用问答"])
        
        with tab_daily:
            st.markdown("**今日复盘**：最近5日OHLCV分析")
            if st.button("⚡ 生成", key="btn_daily", type="primary"):
                with st.status(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · 日线复盘", expanded=False):
                    last_5 = df.tail(5)[['Open', 'High', 'Low', 'Close', 'Volume']].to_string()
                    prompt = f"""分析 {target_c} 最近5日数据：
{last_5}

给出：
1. 📊 资金流向
2. 🎯 支撑压力位
3. 📋 明日预案

中文，300字内。"""
                    result = call_gemini_api(prompt)
                    st.success(result)
                    st.caption(f"📌 本报告由 AI 生成 · 模型: {_ai_model_label()}")
                    if COPY_UTILS_AVAILABLE:
                        CopyUtils.create_copy_button(result, button_text="📋 复制", key="copy_daily")
        
        with tab_weekly:
            st.markdown("**周线展望**：最近10周中期趋势")
            if st.button("⚡ 生成", key="btn_weekly", type="primary"):
                with st.status(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · 周线展望", expanded=False):
                    try:
                        df_weekly = df.resample('W').agg({
                            'Open': 'first', 'High': 'max', 'Low': 'min',
                            'Close': 'last', 'Volume': 'sum'
                        }).dropna()
                        if len(df_weekly) < 10:
                            st.warning("数据不足10周")
                        else:
                            last_10w = df_weekly.tail(10)[['Open', 'High', 'Low', 'Close', 'Volume']].to_string()
                            prompt = f"""分析 {target_c} 最近10周数据：
{last_10w}

给出：
1. 📈 中期趋势
2. ⚠️ 周线背离
3. 💰 主力筹码

中文，300字内。"""
                            result = call_gemini_api(prompt)
                            st.success(result)
                            st.caption(f"📌 本报告由 AI 生成 · 模型: {_ai_model_label()}")
                            if COPY_UTILS_AVAILABLE:
                                CopyUtils.create_copy_button(result, button_text="📋 复制", key="copy_weekly")
                    except Exception as e:
                        st.error(f"重采样失败: {e}")
        
        # 【V90.3】tab_news 已删除，舆情功能统一在「机构研究报告→舆情分析」Tab
        with tab_qa:
            st.markdown("**通用问答**：自由提问")
            q = st.text_input("输入问题", placeholder="如：该股适合长期持有吗？", key="qa_input")
            if st.button("🚀 提问", key="btn_qa"):
                if q:
                    with st.status(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · 通用问答", expanded=False):
                        curr_price = df['Close'].iloc[-1]
                        prompt = f"{target_c} 当前价格 {curr_price:.2f}。问题：{q}\n\n简洁回答（200字内）。"
                        result = call_gemini_api(prompt)
                        st.info(result)
                        st.caption(f"📌 本答复由 AI 生成 · 模型: {_ai_model_label()}")
                else:
                    st.warning("请先输入问题")
        
        st.divider()
        
        # 【V83 P1】机构式交易计划与风险预算
        if metrics and metrics.get('trade_plan'):
            trade_plan = metrics['trade_plan']
            st.markdown("### 📋 机构式交易计划")
            st.caption("💡 基于ATR和均线系统计算的专业交易计划，仅供参考，不构成投资建议")
            
            # 交易计划表格
            tp_col1, tp_col2, tp_col3 = st.columns(3)
            
            with tp_col1:
                st.markdown("##### 🎯 入场策略")
                
                # 【V87.4】入场策略趋势判断
                current_price = trade_plan['current_price']
                entry_low = trade_plan['entry_low']
                entry_high = trade_plan['entry_high']
                entry_mid = trade_plan['entry_mid']
                
                # 判断当前价格位置
                if current_price < entry_low:
                    entry_status = "🟢 当前价格偏低，适合买入"
                elif current_price > entry_high:
                    entry_status = "🔴 当前价格偏高，建议等待"
                else:
                    entry_status = "🟡 当前价格在区间内，可考虑"
                
                st.metric("入场区间（低）", f"${entry_low:.2f}")
                st.metric("入场区间（高）", f"${entry_high:.2f}")
                st.metric("最佳入场价", f"${entry_mid:.2f}", 
                         delta=f"{((entry_mid - current_price) / current_price * 100):+.1f}%")
                st.caption(entry_status)
            
            with tp_col2:
                st.markdown("##### 🛡️ 风险控制")
                
                # 【V87.15修复】风险判断结合评分和止损距离
                risk_pct = trade_plan['risk_per_share'] / current_price * 100
                score = metrics.get('score', 50)
                
                # 综合评分和止损距离判断风险
                if score >= 75:
                    # 高分股票：风险较低
                    if risk_pct > 20:
                        risk_status = "🟡 止损较宽，但基本面优秀"
                    else:
                        risk_status = "🟢 风险较低，基本面优秀"
                elif score >= 60:
                    # 中高分股票：风险适中
                    if risk_pct > 15:
                        risk_status = "🟡 风险适中，止损较宽"
                    else:
                        risk_status = "🟢 风险可控，基本面良好"
                else:
                    # 低分股票：风险较高
                    if risk_pct > 15:
                        risk_status = "🔴 风险较高，谨慎操作"
                    elif risk_pct > 10:
                        risk_status = "🟡 风险适中，需谨慎"
                    else:
                        risk_status = "🟢 止损较紧，风险可控"
                
                st.metric("止损位", f"${trade_plan['stop_loss']:.2f}",
                         delta=f"-{trade_plan['risk_per_share']:.2f} ({risk_pct:.1f}%)")
                st.metric("单股风险", f"${trade_plan['risk_per_share']:.2f}")
                st.metric("风险预算", f"{trade_plan['risk_budget_pct']:.1f}%", help="单笔交易风险占总资金比例")
                st.caption(risk_status)
            
            with tp_col3:
                st.markdown("##### 🎁 目标获利")
                
                # 【V87.4】目标获利趋势判断
                risk_reward_ratio = trade_plan['risk_reward_ratio']
                reward_15r_pct = trade_plan['reward_15r'] / current_price * 100
                reward_2r_pct = trade_plan['reward_2r'] / current_price * 100
                
                if risk_reward_ratio >= 2.0:
                    rr_status = "🟢 优秀盈亏比，值得考虑"
                elif risk_reward_ratio >= 1.5:
                    rr_status = "🟡 良好盈亏比，可接受"
                elif risk_reward_ratio >= 1.0:
                    rr_status = "🟡 基本盈亏比，谨慎考虑"
                else:
                    rr_status = "🔴 盈亏比偏低，不建议"
                
                st.metric("目标位1 (1.5R)", f"${trade_plan['take_profit_15r']:.2f}",
                         delta=f"+{trade_plan['reward_15r']:.2f} ({reward_15r_pct:.1f}%)")
                st.metric("目标位2 (2R)", f"${trade_plan['take_profit_2r']:.2f}",
                         delta=f"+{trade_plan['reward_2r']:.2f} ({reward_2r_pct:.1f}%)")
                st.metric("盈亏比", f"{risk_reward_ratio:.2f}:1")
                st.caption(rr_status)
            
            st.divider()
            
            # 【V83 P1.5】风险预算仓位建议
            st.markdown("##### 💰 仓位建议（基于风险预算）")
            st.info(f"""
**建议仓位上限**：{trade_plan['max_position']} 股（约 ${trade_plan['position_value']:,.0f}）

📌 **计算逻辑**：
- 总资金：$100,000（可在代码中调整）
- 风险预算：{trade_plan['risk_budget_pct']:.1f}%（单笔最大亏损 $1,000）
- 单股风险：${trade_plan['risk_per_share']:.2f}（当前价 - 止损价）
- 最大仓位 = 风险预算金额 ÷ 单股风险

⚠️ **注意**：这是理论最大仓位，实际操作应结合资金管理策略分批建仓。
            """)
        
        # 【V80.1修复】添加"清除分析"按钮，不自动清空
        st.markdown("---")
        if st.button("🔄 清除当前分析", key="clear_analysis", use_container_width=True):
            st.session_state.scan_selected_code = None
            st.session_state.scan_selected_name = None
            st.rerun()
    else:
        # 【V87.4】增强深度分析错误提示
        st.error("❌ 数据获取失败，无法进行深度分析")
        
        # 检查是否是已知的退市股票
        delisted_stocks = {
            "ATVI": "动视暴雪 - 已被微软收购退市",
        }
        
        if code in delisted_stocks:
            st.warning(f"🚨 **{delisted_stocks[code]}**")
            st.info("💡 该股票已无法获取历史数据，建议分析其他活跃交易的股票")
        else:
            st.info("🔍 **可能的原因及解决方案：**")
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**📋 检查清单：**")
                st.markdown("""
                - ✅ 股票代码格式是否正确
                - ✅ 股票是否仍在交易
                - ✅ 网络连接是否正常
                - ✅ 代理设置是否有效
                """)
            
            with col2:
                st.markdown("**🛠️ 建议操作：**")
                st.markdown("""
                - 🔍 使用左侧搜索功能查找股票
                - 🛠️ 运行系统自检检查网络
                - 🏥 执行股票池健康检查
                - 🔄 尝试其他股票代码
                """)
        
        # 提供快速测试按钮
        st.markdown("**🚀 快速测试推荐股票：**")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🇺🇸 测试苹果(AAPL)", use_container_width=True):
                st.session_state.scan_selected_code = "AAPL"
                st.session_state.scan_selected_name = "苹果"
                st.rerun()
                
        with col2:
            if st.button("🇭🇰 测试腾讯(00700)", use_container_width=True):
                st.session_state.scan_selected_code = "00700"
                st.session_state.scan_selected_name = "腾讯控股"
                st.rerun()
                
        with col3:
            if st.button("🇨🇳 测试茅台(600519)", use_container_width=True):
                st.session_state.scan_selected_code = "600519"
                st.session_state.scan_selected_name = "贵州茅台"
                st.rerun()


# ═══════════════════════════════════════════════════════════════
# 【模块 ②】我的持仓（标题在 _render_portfolio_section 内部）
# ═══════════════════════════════════════════════════════════════
try:
    _render_portfolio_section()
except Exception as _e_port:
    st.warning(f"⚠️ 持仓模块加载异常: {str(_e_port)[:60]}")


# ═══════════════════════════════════════════════════════════════
# 【模块 ③】深度作战室 + 猎手战位 + Top30（作战室为首 Tab，点击后自动切换）
# ═══════════════════════════════════════════════════════════════
# 【V90.7】深度作战室作为第一个 Tab，解决"点击无反应"——选中后自动显示
tab_warroom, tab_scanner, tab_quant, tab_ai_select, tab_watchlist = st.tabs(["⚔️ 深度作战室", "📡 猎手战位", "🏆 Top30 扫描", "🤖 AI选股", "📋 自选股分析"])

# 【V90.7】深度作战室 Tab - 完整分析内容在顶部区块渲染，此处仅占位
with tab_warroom:
    _warroom_code = st.session_state.get('scan_selected_code')
    if _warroom_code:
        st.success(f"🎯 正在分析：**{st.session_state.get('scan_selected_name', '')}** ({_warroom_code})")
    # 无选中时空白，顶部深度作战室区块会显示分析内容

# 【V89.7】模块独立化 - 各Tab互不影响
# 【V91.8】用 st.fragment 包装猎手战位：缓存命中时仅 fragment 重跑，跳过全局市场分析，10 秒内显示
with tab_scanner:
    @st.fragment
    def _scanner_fragment():
        st.markdown("#### 智能筛选引擎")
        
        # 【V87.1】显示股票池大小和来源（安全限流模式）
        us_count, hk_count, cn_count = len(RAW_US), len(RAW_HK), len(RAW_CN_TOP)
        total_count = us_count + hk_count + cn_count
        
        # 判断是否使用云端数据（阈值降低到50）
        is_cloud_us = us_count >= 50
        is_cloud_hk = hk_count >= 50
        is_cloud_cn = cn_count >= 50
        
        source_icon = "☁️" if (is_cloud_us and is_cloud_hk and is_cloud_cn) else "💾"
        source_text = "云端实时" if (is_cloud_us and is_cloud_hk and is_cloud_cn) else "本地备用"
        
        st.caption(f"{source_icon} **股票池来源**: {source_text} | 美股 {us_count} 只 | 港股 {hk_count} 只 | A股 {cn_count} 只 | 总计 {total_count} 只 | 📦 15分钟缓存")
        
        # 初始化 session_state
        if 'scanner_results' not in st.session_state:
            st.session_state.scanner_results = {}
        # 【NEW V88 Phase 2】初始化取消标志
        if 'cancel_scan' not in st.session_state:
            st.session_state.cancel_scan = {'cancel': False}
        
        # 【V89.6.4 + V91.2】显示扫描缓存状态（10分钟有效）
        if 'scanner_results' in st.session_state and st.session_state.scanner_results:
            if 'scan_timestamp' in st.session_state.scanner_results:
                scan_time = st.session_state.scanner_results['scan_timestamp']
                scan_age = time.time() - scan_time
                ttl = get_smart_cache_ttl('daily')  # 交易日15分钟，非交易日24小时
                remaining_sec = ttl - scan_age
                if remaining_sec > 0:
                    scan_time_str = time.strftime('%H:%M:%S', time.localtime(scan_time))
                    st.info(f"📦 使用缓存扫描结果 | 扫描时间: {scan_time_str} | 剩余 {remaining_sec/60:.1f} 分钟有效（交易日15分钟内不重复扫描）")
                else:
                    _expire_str = f"{ttl//3600}小时" if ttl >= 3600 else f"{ttl//60}分钟"
                    st.warning(f"⏰ 扫描缓存已过期（超过{_expire_str}），请重新扫描")
        
        # 【V89.6.4】添加清除缓存按钮
        clear_col1, clear_col2 = st.columns([3, 1])
        with clear_col2:
            if st.button("🗑️ 清除扫描缓存", help="清除所有扫描结果缓存（含文件持久化）", use_container_width=True):
                st.session_state.scanner_results = {}
                _clear_scan_cache_files()
                st.toast("✅ 扫描缓存已清除", icon="🗑️")
                st.rerun()
        
        # 【NEW V88 Phase 2】扫描辅助函数
        def run_scan(scan_type, ma_target, pool, use_concurrent, scan_name, icon):
            """统一的扫描执行函数"""
            st.session_state.cancel_scan = {'cancel': False}
            scan_mode = "⚡ 并发" if use_concurrent else "🔄 串行"
            st.toast(f"扫描 {scan_name}... ({scan_mode})", icon=icon)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            def update_progress(current, total, stock_name):
                progress = current / total
                progress_bar.progress(progress)
                mode_text = "并发扫描" if use_concurrent else "扫描"
                status_text.text(f"正在{mode_text}{scan_name}... {current}/{total} ({progress*100:.1f}%) - {stock_name}")
            
            if use_concurrent and USE_NEW_MODULES:
                res, stats = batch_scan_analysis_concurrent(
                    pool, scan_type=scan_type, ma_target=ma_target,
                    progress_callback=update_progress, max_workers=6,
                    cancel_flag=st.session_state.cancel_scan
                )
            else:
                res, stats = batch_scan_analysis(
                    pool, scan_type=scan_type, ma_target=ma_target,
                    progress_callback=update_progress
                )
            
            progress_bar.empty()
            status_text.empty()
            
            if stats.get('cancelled', False):
                st.warning("⚠️ 扫描已取消")
            
            st.caption(f"✅ 成功扫描: {stats['success']} 只 | ❌ 失败/无数据: {stats['failed']} 只")
            
            if stats['failed'] > 0:
                display_scan_failures(stats['errors'], stats['failed'])
            
            return res, stats
        
        c_ctrl = st.container()
        with c_ctrl:
            # 【NEW V88 Phase 2】添加并发选项和取消按钮
            col_market, col_concurrent, col_cancel = st.columns([2, 2, 1])
            with col_market:
                scan_market = st.radio("市场", ["美股", "港股", "A股"], horizontal=True, label_visibility="collapsed")
            with col_concurrent:
                use_concurrent = st.checkbox("⚡ 并发扫描（6线程，速度快2-3倍）", value=True, help="默认开启，15分钟内同类型扫描使用缓存不重复执行")
            with col_cancel:
                if st.button("🛑 取消", help="取消当前扫描", use_container_width=True):
                    st.session_state.cancel_scan['cancel'] = True
                    st.toast("正在取消扫描...", icon="🛑")
            
            # 【V82.12 + Regime-Adaptive】专业均线策略 + 市场状态自适应
            st.caption("💡 均线触底反弹策略：股价触及关键均线时，反弹概率大 | 🎯 市场状态自适应：先判状态再分流")
            c_btn1, c_btn2, c_btn3, c_btn4, c_btn5, c_btn6 = st.columns(6)
            
            do_scan_ma30 = c_btn1.button("📊 MA30短线", help="月线支撑，适合短线波段（3-7天）", use_container_width=True)
            do_scan_ma60 = c_btn2.button("📈 MA60季线", help="季线支撑，适合波段交易（1-3周）", use_container_width=True)
            do_scan_ma120 = c_btn3.button("📉 MA120半年", help="半年线支撑，适合中线布局（1-3月）", use_container_width=True)
            do_scan_top = c_btn4.button("🏆 综合评分", help="多维度量化评分，不限均线", use_container_width=True)
            do_scan_regime = c_btn5.button("🎯 市场状态自适应", help="先判 BULL/RANGE/BEAR，再给动作建议", type="primary", use_container_width=True, disabled=not REGIME_ENGINE_AVAILABLE)
            do_scan_safe = c_btn6.button("🛡️ 多重支撑", help="同时靠近多条均线，风险低", use_container_width=True)
            
            risk_preference = st.selectbox("风险偏好（市场状态自适应）", ["保守", "平衡", "进攻"], index=1, key="risk_pref_scanner")
        
        # 【V91.2+V91.3+V91.4】扫描缓存：15分钟内同类型+同市场命中则跳过；支持文件持久化（刷新/新标签页后仍有效）
        def _scan_cache_hit(scan_type: str, risk_pref: str = None) -> bool:
            r = st.session_state.get('scanner_results') or {}
            if r.get('type') == scan_type and r.get('scan_market') == scan_market:
                if scan_type == 'regime' and risk_pref is not None and r.get('risk_preference') != risk_pref:
                    pass  # 继续检查文件
                else:
                    ts = r.get('scan_timestamp', 0)
                    ttl = get_smart_cache_ttl('daily')
                    if (time.time() - ts) < ttl:
                        return True
            # session_state 未命中时，尝试从文件加载（跨会话/刷新后仍有效）
            loaded = _load_scan_cache_from_file(scan_type, scan_market, risk_pref)
            if loaded:
                st.session_state.scanner_results = loaded
                return True
            return False
        
        # 【V82.12 + NEW V88 Phase 2】重构扫描按钮逻辑（使用统一扫描函数）
        if do_scan_ma30:
            if _scan_cache_hit('ma30'):
                st.toast("📦 使用缓存，无需重新扫描", icon="📦")
            else:
                pool = RAW_US if scan_market == "美股" else (RAW_HK if scan_market == "港股" else RAW_CN_TOP)
                res, stats = run_scan("MA_TOUCH", 30, pool, use_concurrent, "MA30 短线反弹", "📊")
                st.session_state.scanner_results = {
                    'type': 'ma30', 'scan_market': scan_market,
                    'title': f"#### 📊 MA30 短线反弹 ({scan_market})",
                    'caption': "💡 适合短线波段交易，持仓3-7天，快进快出捕捉超跌反弹",
                    'data': res, 'stats': stats, 'key': 'ma30_table',
                    'scan_timestamp': time.time(),
                }
                _save_scan_cache_to_file(st.session_state.scanner_results)
        
        if do_scan_ma60:
            if _scan_cache_hit('ma60'):
                st.toast("📦 使用缓存，无需重新扫描", icon="📦")
            else:
                pool = RAW_US if scan_market == "美股" else (RAW_HK if scan_market == "港股" else RAW_CN_TOP)
                res, stats = run_scan("MA_TOUCH", 60, pool, use_concurrent, "MA60 季线机会", "📈")
                st.session_state.scanner_results = {
                    'type': 'ma60', 'scan_market': scan_market,
                    'title': f"#### 📈 MA60 季线机会 ({scan_market})",
                    'caption': "💡 适合波段交易，持仓1-3周，中期趋势确认，胜率更高",
                    'data': res, 'stats': stats, 'key': 'ma60_table',
                    'scan_timestamp': time.time(),
                }
                _save_scan_cache_to_file(st.session_state.scanner_results)
        
        if do_scan_ma120:
            if _scan_cache_hit('ma120'):
                st.toast("📦 使用缓存，无需重新扫描", icon="📦")
            else:
                pool = RAW_US if scan_market == "美股" else (RAW_HK if scan_market == "港股" else RAW_CN_TOP)
                res, stats = run_scan("MA_TOUCH", 120, pool, use_concurrent, "MA120 半年线布局", "📉")
                st.session_state.scanner_results = {
                    'type': 'ma120', 'scan_market': scan_market,
                    'title': f"#### 📉 MA120 半年线布局 ({scan_market})",
                    'caption': "💡 适合价值投资，持仓1-3月，长期支撑位，适合分批建仓",
                    'data': res, 'stats': stats, 'key': 'ma120_table',
                    'scan_timestamp': time.time(),
                }
                _save_scan_cache_to_file(st.session_state.scanner_results)
        
        if do_scan_top:
            if _scan_cache_hit('top'):
                st.toast("📦 使用缓存，无需重新扫描", icon="📦")
            else:
                pool = RAW_US if scan_market == "美股" else (RAW_HK if scan_market == "港股" else RAW_CN_TOP)
                res, stats = run_scan("TOP", None, pool, use_concurrent, "综合评分 Top", "🏆")
                st.session_state.scanner_results = {
                    'type': 'top', 'scan_market': scan_market,
                    'title': f"#### 🏆 综合评分 Top 榜单 ({scan_market})",
                    'caption': "💡 多维度量化评分：CANSLIM + 专业投机原理 + 技术指标，不限均线",
                    'data': res, 'stats': stats, 'key': 'top_table',
                    'scan_timestamp': time.time(),
                }
                _save_scan_cache_to_file(st.session_state.scanner_results)
        
        if do_scan_regime and REGIME_ENGINE_AVAILABLE:
            if _scan_cache_hit('regime', risk_preference):
                st.toast("📦 使用缓存，无需重新扫描", icon="📦")
            else:
                pool = RAW_US if scan_market == "美股" else (RAW_HK if scan_market == "港股" else RAW_CN_TOP)
                st.toast("🎯 市场状态自适应扫描中...", icon="🎯")
                progress_bar = st.progress(0)
                status_text = st.empty()

                def update_regime_progress(current, total, stock_name):
                    pct = current / total
                    progress_bar.progress(pct)
                    status_text.text(f"🎯 市场状态自适应... {current}/{total} ({pct*100:.1f}%) - {stock_name}")

                try:
                    res, stats, regime_info, meta = run_regime_scan(
                        pool, use_concurrent, scan_market, risk_preference,
                        progress_callback=update_regime_progress
                    )
                    progress_bar.empty()
                    status_text.empty()
                    regime_str = meta.get("regime", "N/A")
                    conf = meta.get("confidence", 0)
                    st.caption(f"✅ 成功: {stats['success']} 只 | ❌ 失败: {stats['failed']} 只 | 市场状态: {regime_str} (置信度 {conf:.0%})")
                    if stats.get('failed', 0) > 0 and stats.get('errors'):
                        display_scan_failures(stats['errors'], stats['failed'])
                    _meta = meta or {}
                    _ts = _meta.get("scan_timestamp", "")
                    _dual = _meta.get("use_potential_engine", False)
                    _cap = f"💡 市场状态: {regime_str} | 风险偏好: {risk_preference} | 动作池: BUILD_NOW / FOLLOW_MID / LONG_CORE"
                    if _dual:
                        _cap += f" | 双引擎+三池 | {_ts}"
                    st.session_state.scanner_results = {
                        'type': 'regime', 'scan_market': scan_market, 'risk_preference': risk_preference,
                        'title': f"#### 🎯 市场状态自适应 榜单 ({scan_market})" + (" (Top50 双引擎)" if _dual else ""),
                        'caption': _cap,
                        'data': res, 'stats': stats, 'key': 'regime_table',
                        'scan_timestamp': time.time(),
                        'regime_info': regime_info,
                        'meta': meta,
                    }
                    _save_scan_cache_to_file(st.session_state.scanner_results)
                except Exception as e:
                    st.error(f"❌ 市场状态自适应扫描异常: {str(e)[:100]}")
                    logging.error(f"run_regime_scan error: {e}")
                    status_text.text("⚠️ 降级为综合评分...")
                    res, stats = batch_scan_analysis(pool, scan_type="TOP", ma_target=None, progress_callback=update_regime_progress)
                    progress_bar.empty()
                    status_text.empty()
                    st.session_state.scanner_results = {
                        'type': 'top', 'scan_market': scan_market,
                        'title': f"#### 🏆 综合评分 Top 榜单 ({scan_market})（降级）",
                        'caption': "⚠️ 市场状态引擎异常，已降级为综合评分",
                        'data': res, 'stats': stats, 'key': 'top_table',
                        'scan_timestamp': time.time(),
                    }
                    _save_scan_cache_to_file(st.session_state.scanner_results)
        
        if do_scan_safe:
            if _scan_cache_hit('safe_zone'):
                st.toast("📦 使用缓存，无需重新扫描", icon="📦")
            else:
                st.toast("扫描多重均线支撑标的...", icon="🛡️")
                st.markdown("#### 🛡️ 多重均线支撑 (安全区)")
                st.caption("💡 同时靠近MA30/MA60/MA120，多重支撑共振，风险低，适合保守型投资者")
            
                progress_bar = st.progress(0)
                status_text = st.empty()
            
                res_combined = []
                stats_safe = {'success': 0, 'failed': 0, 'errors': []}
            
                if scan_market == "美股":
                    all_pools = [(RAW_US, "美股")]
                elif scan_market == "港股":
                    all_pools = [(RAW_HK, "港股")]
                elif scan_market == "A股":
                    all_pools = [(RAW_CN_TOP, "A股")]
                else:
                    all_pools = [(RAW_US, "美股"), (RAW_HK, "港股"), (RAW_CN_TOP, "A股")]
            
                total_stocks = sum(len(pool) for pool, _ in all_pools)
                current_idx = 0
            
                for pool, mkt_label in all_pools:
                    for idx, item in enumerate(pool):
                        current_idx += 1
                        progress_pct = current_idx / total_stocks
                        progress_bar.progress(progress_pct)
                        stock_name = item[1] if len(item) > 1 else item[0]
                        status_text.text(f"正在扫描 {stock_name}... ({current_idx}/{total_stocks}, {progress_pct*100:.1f}%)")
                        try:
                            code = item[0]
                            name = item[1]
                            # 【V82.9关键修复】如果pool有3个元素，直接使用第3个
                            if len(item) >= 3:
                                c_fixed = item[2]
                            else:
                                c_fixed = to_yf_cn_code(code)
                            
                            # 【V87.1】添加请求间隔，避免触发API限流（每10个股票延迟0.5秒）
                            if idx > 0 and idx % 10 == 0:
                                time.sleep(0.5)
                            
                            df = fetch_stock_data(c_fixed)
                            
                            # 【V84.3】防御性检查
                            if df is None or df.empty:
                                stats_safe['failed'] += 1
                                continue
                            
                            m = calculate_metrics_all(df, c_fixed)
                            # 【V86修复】多重均线支撑：同时靠近MA30/MA60/MA120
                            # 降低评分要求，放宽容差
                            if m and m['score'] > 35:  # 【V86】从45降到35
                                last_close = m['last']['Close']
                                last_low = m['last']['Low']
                                last_high = m['last']['High']
                                touch_count = 0
                                touch_mas = []
                                
                                for ma_n in [30, 60, 120]:
                                    ma_col = f'MA{ma_n}'
                                    if ma_col in m['df'].columns:
                                        ma_val = m['df'][ma_col].iloc[-1]
                                        # 【V86】放宽容差到8%，或者K线触及均线
                                        touched = (last_low <= ma_val <= last_high)
                                        close_enough = (ma_val > 0 and abs(last_close - ma_val) / ma_val < 0.08)
                                        
                                        if touched or close_enough:
                                            touch_count += 1
                                            distance_pct = abs(last_close - ma_val) / ma_val * 100 if ma_val > 0 else 0
                                            touch_mas.append(f"MA{ma_n}({distance_pct:.1f}%)")
                                
                                # 【V86】打印调试信息
                                if touch_count >= 2:
                                    _safe_print(f"[多重支撑] ✅ {code} ({name}): 触及{touch_count}条均线 - {', '.join(touch_mas)}, 评分={m['score']}")
                                
                                # 只有触及2条或以上均线才算"多重支撑"
                                if touch_count >= 2:
                                    res_combined.append({
                                        "市场": mkt_label, "代码": code, "名称": name,
                                        "评分": m['score'], "策略": m['logic'],
                                        "现价": f"{m['last_price']:.2f}",
                                        "触发": " + ".join(touch_mas)
                                    })
                                    stats_safe['success'] += 1
                        except Exception as e:
                            stats_safe['failed'] += 1
                            error_msg = f"{type(e).__name__}: {str(e)[:80]}"
                            stats_safe['errors'].append({
                                'code': item[0] if item else 'Unknown',
                                'name': item[1] if len(item) > 1 else 'Unknown',
                                'error': error_msg
                            })
            
                # 【V87.17】清除进度条
                progress_bar.empty()
                status_text.empty()
            
                # 【V85】显示统计和失败详情
                st.caption(f"✅ 成功扫描: {stats_safe['success']} 只 | ❌ 失败/无数据: {stats_safe['failed']} 只")
            
                # 【V87.8】如果有失败的股票,显示详情
                if stats_safe['failed'] > 0:
                    display_scan_failures(stats_safe['errors'], stats_safe['failed'])
            
                res_combined = sorted(res_combined, key=lambda x: x['评分'], reverse=True)[:50]
                st.session_state.scanner_results = {
                    'type': 'safe_zone', 'scan_market': scan_market,
                    'title': f"#### 🛡️ 多重均线支撑 (安全区) ({scan_market})",
                    'caption': "💡 同时靠近MA30/MA60/MA120，多重支撑共振，风险低，适合保守型投资者",
                    'data': res_combined,
                    'stats': stats_safe,
                    'key': 'safe_zone',
                    'scan_timestamp': time.time()  # 【V89.6.4】添加扫描时间戳
                }
                _save_scan_cache_to_file(st.session_state.scanner_results)
        
        # 【V82.12】显示保存的扫描结果（支持caption）
        if st.session_state.scanner_results:
            result_info = st.session_state.scanner_results
            
            # 【Regime-Adaptive】市场状态简报（仅 type=regime 时）
            if result_info.get('type') == 'regime' and result_info.get('regime_info'):
                ri = result_info['regime_info']
                st.info(f"📊 **市场状态**: {ri.get('regime', 'N/A')} | 置信度: {ri.get('confidence', 0):.0%} | 驱动: {' '.join(ri.get('drivers_top3', []))}")
            
            # 【NEW V88 Phase 2】标题和导出按钮并排
            col_title, col_export = st.columns([4, 1])
            with col_title:
                st.markdown(result_info['title'])
                if 'caption' in result_info and result_info['caption']:
                    st.caption(result_info['caption'])
            with col_export:
                # 【NEW V88 Phase 2】CSV导出功能
                if result_info['data']:
                    df_export = pd.DataFrame(result_info['data'])
                    csv = df_export.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="📥 导出CSV",
                        data=csv,
                        file_name=f"scan_{result_info['type']}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        help="下载扫描结果为CSV文件",
                        use_container_width=True
                    )
            
            # 【NEW V88 Phase 2】表格筛选功能
            df_results = pd.DataFrame(result_info['data'])
            
            if not df_results.empty:
                # 筛选器
                n_cols = 3 if (result_info.get('type') == 'regime' and 'pool_assignment' in df_results.columns
                    and result_info.get('meta', {}).get('use_potential_engine', False)) else 2
                filter_cols = st.columns(n_cols)
                
                with filter_cols[0]:
                    # 行业筛选（用户要求：板块→行业）
                    col_name = '行业' if '行业' in df_results.columns else '板块'
                    if col_name in df_results.columns:
                        industries = ['全部'] + sorted(df_results[col_name].unique().tolist())
                        selected_industry = st.selectbox("🏷️ 筛选行业", industries, key=f"filter_industry_{result_info['key']}")
                        if selected_industry != '全部':
                            df_results = df_results[df_results[col_name] == selected_industry]
                
                with filter_cols[1]:
                    # 三池筛选（仅双引擎模式显示）
                    if (result_info.get('type') == 'regime' and 'pool_assignment' in df_results.columns
                        and result_info.get('meta', {}).get('use_potential_engine', False)):
                        pool_options = ['全部', 'A-已验证强势', 'B-预期差潜力', 'C-左侧观察']
                        selected_pool = st.selectbox("🏊 三池筛选", pool_options, key=f"filter_pool_{result_info['key']}")
                        if selected_pool != '全部':
                            pool_map = {'A-已验证强势': 'A', 'B-预期差潜力': 'B', 'C-左侧观察': 'C'}
                            df_results = df_results[df_results['pool_assignment'] == pool_map[selected_pool]]
                    elif '得分' in df_results.columns:
                        min_score = st.slider(
                            "📊 最低得分",
                            min_value=0,
                            max_value=100,
                            value=0,
                            step=5,
                            key=f"filter_score_{result_info['key']}",
                            help="只显示得分≥此值的股票"
                        )
                        df_results = df_results[df_results['得分'] >= min_score]
                
                if n_cols >= 3:
                    with filter_cols[2]:
                        if '得分' in df_results.columns:
                            min_score = st.slider(
                                "📊 最低得分",
                                min_value=0,
                                max_value=100,
                                value=0,
                                step=5,
                                key=f"filter_score_{result_info['key']}",
                                help="只显示得分≥此值的股票"
                            )
                            df_results = df_results[df_results['得分'] >= min_score]
                
                # 显示筛选后的结果数量
                if len(df_results) < len(result_info['data']):
                    st.caption(f"🔍 筛选后: {len(df_results)} 只 / 总共 {len(result_info['data'])} 只")
            
            render_clickable_table(df_results, result_info['key'])
        
        st.divider()
        st.markdown("---")
    
    _scanner_fragment()

# ═══════════════════════════════════════════════════════════════
# 【后台扫描工具函数】供 tab_quant 使用
# ═══════════════════════════════════════════════════════════════
import subprocess as _subprocess

_SCAN_RESULTS_FILE  = _BRIEF_CACHE_DIR / "scan_results.json"
_SCAN_PROGRESS_FILE = _BRIEF_CACHE_DIR / "scan_progress.json"
_SCAN_HEARTBEAT_FILE = _BRIEF_CACHE_DIR / "scan_heartbeat.json"
_SCAN_PID_FILE      = _BRIEF_CACHE_DIR / "scan_worker.pid"
_SCAN_WORKER_SCRIPT = Path(__file__).parent / "scan_worker.py"
_SCAN_RESULT_TTL    = 6 * 3600    # 6 小时（匹配 GitHub Actions 每天4次节奏）

# Gist 配置：从 Streamlit Secrets 或环境变量读取
_GIST_ID = (
    st.secrets.get("GIST_ID", "")
    if hasattr(st, "secrets") else ""
) or os.environ.get("GIST_ID", "")
# Gist 本地缓存（避免每 20 秒都请求）
_gist_local_cache: dict = {}
_GIST_LOCAL_TTL  = 600    # 10 分钟内复用，不重复请求
_gist_last_sync_ts: float = 0.0
_gist_last_sync_ok: bool  = False


def _ssl_http_get(url: str, headers: dict | None = None, timeout: int = 12) -> bytes:
    """
    带 macOS SSL fallback 的 GET 请求。
    优先用 requests（SSL 更稳定），其次 urllib + ssl fallback。
    """
    hdrs = headers or {}
    try:
        import requests as _req
        resp = _req.get(url, headers=hdrs, timeout=timeout, verify=True)
        resp.raise_for_status()
        return resp.content
    except ImportError:
        pass
    except Exception:
        pass
    # urllib fallback（附带 macOS SSL 自动修复）
    import urllib.request as _ur
    import ssl as _ssl
    try:
        ctx = _ssl.create_default_context()
        req = _ur.Request(url, headers=hdrs)
        with _ur.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.read()
    except _ssl.SSLError:
        # macOS 本地证书缺失时跳过验证（开发环境兜底）
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode    = _ssl.CERT_NONE
        req = _ur.Request(url, headers=hdrs)
        with _ur.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.read()


def _ssl_http_post(url: str, payload: bytes,
                   headers: dict | None = None, timeout: int = 10) -> bytes:
    """
    带 macOS SSL fallback 的 POST 请求。
    """
    hdrs = {"Content-Type": "application/json", **(headers or {})}
    try:
        import requests as _req
        resp = _req.post(url, data=payload, headers=hdrs, timeout=timeout, verify=True)
        resp.raise_for_status()
        return resp.content
    except ImportError:
        pass
    except Exception:
        pass
    import urllib.request as _ur
    import ssl as _ssl
    try:
        ctx = _ssl.create_default_context()
        req = _ur.Request(url, data=payload, headers=hdrs)
        with _ur.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.read()
    except _ssl.SSLError:
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode    = _ssl.CERT_NONE
        req = _ur.Request(url, data=payload, headers=hdrs)
        with _ur.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.read()


def _scan_fetch_from_gist() -> dict | None:
    """
    用 GitHub API 拉取 Gist 内容（只需 GIST_ID，不需用户名）。
    支持 GIST_TOKEN（可读 secret gist）；带 10 分钟本地短缓存。
    """
    global _gist_local_cache, _gist_last_sync_ts, _gist_last_sync_ok, _gist_last_err
    if not _GIST_ID:
        return None
    # 本地缓存有效则直接返回
    cached = _gist_local_cache
    if cached and time.time() - cached.get("_fetched_at", 0) < _GIST_LOCAL_TTL:
        data = {k: v for k, v in cached.items() if k != "_fetched_at"}
        if time.time() - data.get("timestamp", 0) < _SCAN_RESULT_TTL:
            return data
    # 读取可选的 GIST_TOKEN（用于 secret gist 或提高 API rate limit）
    _gist_token = (
        st.secrets.get("GIST_TOKEN", "") if hasattr(st, "secrets") else ""
    ) or os.environ.get("GIST_TOKEN", "")
    try:
        api_url = f"https://api.github.com/gists/{_GIST_ID}"
        headers = {
            "Accept":     "application/vnd.github+json",
            "User-Agent": "StockAI-V88",
        }
        if _gist_token:
            headers["Authorization"] = f"Bearer {_gist_token}"
        raw = _ssl_http_get(api_url, headers=headers, timeout=12)
        gist_json = json.loads(raw.decode("utf-8"))
        # 从 Gist API 响应里取文件内容
        files = gist_json.get("files", {})
        if not files:
            raise ValueError("Gist 为空（GitHub Actions 可能尚未运行）")
        content = None
        for fname, fdata in files.items():
            if "scan_results" in fname.lower() or fname.endswith(".json"):
                content = fdata.get("content", "")
                break
        if not content:
            raise ValueError(f"Gist 文件中无 scan_results，现有文件: {list(files.keys())}")
        data = json.loads(content)
        if not data.get("timestamp"):
            raise ValueError("Gist 数据无 timestamp 字段")
        # 同步写本地文件备用
        try:
            _BRIEF_CACHE_DIR.mkdir(exist_ok=True)
            _SCAN_RESULTS_FILE.write_text(
                json.dumps(data, ensure_ascii=False), encoding="utf-8"
            )
        except Exception:
            pass
        _gist_local_cache  = {**data, "_fetched_at": time.time()}
        _gist_last_sync_ts = time.time()
        _gist_last_sync_ok = True
        _gist_last_err     = ""
        return data
    except Exception as _e:
        _gist_last_sync_ts = time.time()
        _gist_last_sync_ok = False
        _gist_last_err     = str(_e)[:120]
        return None


_gist_last_err: str = ""


def _gist_sync_status() -> str:
    """返回云端同步状态字符串，用于 UI 展示"""
    if not _GIST_ID:
        return "⚙️ 未配置 GIST_ID（Secrets 里加 GIST_ID = \"...\" 即可）"
    if _gist_last_sync_ts == 0:
        return "🔄 云端尚未同步（页面加载后首次轮询中）"
    ago   = int(time.time() - _gist_last_sync_ts)
    t_str = f"{ago//60}分{ago%60}秒前" if ago >= 60 else f"{ago}秒前"
    if _gist_last_sync_ok:
        return f"☁️ 云端同步成功（{t_str}）"
    hint = ""
    if "尚未运行" in _gist_last_err or "为空" in _gist_last_err:
        hint = " · GitHub Actions 尚未写入数据，可手动触发"
    elif "GIST_ID" in _gist_last_err or "404" in _gist_last_err:
        hint = " · GIST_ID 有误，请检查 Secrets"
    return f"⚠️ 云端读取失败（{t_str}）{hint}"


def _scan_write_heartbeat():
    """更新心跳文件（页面每次 fragment 执行时调用）"""
    try:
        _BRIEF_CACHE_DIR.mkdir(exist_ok=True)
        _SCAN_HEARTBEAT_FILE.write_text(
            json.dumps({"ts": time.time()}), encoding="utf-8"
        )
    except Exception:
        pass


def _scan_read_results() -> dict | None:
    """读取扫描结果：优先 GitHub Gist（云端缓存），其次本地文件"""
    # 1. 先尝试 Gist（GitHub Actions 写入的云端缓存）
    gist_data = _scan_fetch_from_gist()
    if gist_data and time.time() - gist_data.get("timestamp", 0) < _SCAN_RESULT_TTL:
        return gist_data
    # 2. 本地文件（本机手动扫描写入的）
    try:
        data = json.loads(_SCAN_RESULTS_FILE.read_text(encoding="utf-8"))
        if time.time() - data.get("timestamp", 0) < _SCAN_RESULT_TTL:
            return data
    except Exception:
        pass
    return None


def _scan_read_progress() -> dict:
    """读取进度文件；返回 {pct, status, detail, ts} 或默认 idle"""
    try:
        return json.loads(_SCAN_PROGRESS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"pct": 0, "status": "idle", "detail": "", "ts": 0}


def _scan_worker_running() -> bool:
    """检查 scan_worker.py 进程是否存活"""
    try:
        pid = int(_SCAN_PID_FILE.read_text().strip())
        os.kill(pid, 0)     # 不抛异常说明进程存活
        return True
    except Exception:
        return False


def _scan_start_worker(force: bool = False):
    """启动后台扫描进程（非阻塞）"""
    import sys as _sys
    if _scan_worker_running():
        return
    _BRIEF_CACHE_DIR.mkdir(exist_ok=True)
    cmd = [_sys.executable, str(_SCAN_WORKER_SCRIPT)]
    if force:
        cmd.append("--force")
    _subprocess.Popen(
        cmd,
        stdout=open(_BRIEF_CACHE_DIR / "scan_worker.log", "a"),
        stderr=_subprocess.STDOUT,
        close_fds=True,
    )


def _scan_result_remaining() -> int | None:
    """返回结果缓存剩余秒数，无结果返回 None"""
    try:
        data = json.loads(_SCAN_RESULTS_FILE.read_text(encoding="utf-8"))
        rem  = int(_SCAN_RESULT_TTL - (time.time() - data.get("timestamp", 0)))
        return max(0, rem)
    except Exception:
        return None


def _scan_result_label() -> str:
    """返回缓存剩余时间字符串"""
    rem = _scan_result_remaining()
    if rem is None:
        return ""
    if rem <= 0:
        return "⏰ 缓存已过期"
    h, m = divmod(rem // 60, 60)
    return f"⏱ 缓存剩余 {h}h {m:02d}m"


def _scan_force_clear():
    """清除扫描结果，触发重新扫描"""
    for f in (_SCAN_RESULTS_FILE, _SCAN_PROGRESS_FILE):
        try:
            f.unlink(missing_ok=True)
        except Exception:
            pass


# ── 钉钉推送 ──────────────────────────────────────────────────────
def _dingtalk_send(text: str) -> tuple[bool, str]:
    """发送文本到钉钉机器人（支持加签）"""
    webhook = (
        st.secrets.get("DINGTALK_WEBHOOK", "")
        if hasattr(st, "secrets") else ""
    ) or os.environ.get("DINGTALK_WEBHOOK", "")
    secret = (
        st.secrets.get("DINGTALK_SECRET", "")
        if hasattr(st, "secrets") else ""
    ) or os.environ.get("DINGTALK_SECRET", "")

    if not webhook:
        return False, "未配置 DINGTALK_WEBHOOK"
    try:
        import urllib.parse as _up
        url = webhook
        if secret:
            import hmac as _hmac, hashlib as _hs, base64 as _b64
            ts       = str(round(time.time() * 1000))
            sign_str = f"{ts}\n{secret}"
            sig      = _b64.b64encode(
                _hmac.new(secret.encode(), sign_str.encode(), _hs.sha256).digest()
            ).decode()
            url = f"{webhook}&timestamp={ts}&sign={_up.quote_plus(sig)}"
        payload = json.dumps({
            "msgtype": "text",
            "text":    {"content": text},
            "at":      {"isAtAll": False},
        }).encode("utf-8")
        raw    = _ssl_http_post(url, payload=payload, timeout=10)
        result = json.loads(raw.decode("utf-8"))
        if result.get("errcode", -1) == 0:
            return True, "ok"
        return False, result.get("errmsg", "unknown")
    except Exception as e:
        return False, str(e)[:120]


def _dingtalk_push_top30(res: dict | None) -> tuple[bool, str]:
    """把 Top30 趋势榜推送到钉钉（前10条 + 来源时间）"""
    if not res:
        return False, "暂无扫描结果"
    from datetime import datetime as _dt_push
    ts_str = _dt_push.fromtimestamp(res.get("timestamp", 0)).strftime("%m-%d %H:%M")
    lines  = [f"📊 V88 Top30 扫描结果 · {ts_str}\n"]

    mkt_map = {"US": "🇺🇸 美股", "HK": "🇭🇰 港股", "CN": "🇨🇳 A股"}
    for mkt_key, mkt_name in mkt_map.items():
        mkt_data = res.get(mkt_key, {})
        top_list = mkt_data.get("top", [])[:5]   # 每市场取前5
        if not top_list:
            continue
        lines.append(f"\n{mkt_name} 趋势Top5：")
        for item in top_list:
            name  = item.get("name", "")
            code  = item.get("code", "")
            score = item.get("score", 0)
            lines.append(f"  · {name}({code}) 得分{score}")

    lines.append(f"\n🔗 来源：GitHub Actions 云端自动扫描")
    return _dingtalk_send("\n".join(lines))


with tab_quant:

    @st.fragment(run_every=20)
    def _top30_fragment():
        # ── 心跳：告知 scan_worker 页面仍在线 ────────────────────
        _scan_write_heartbeat()

        # ── 读取当前状态 ──────────────────────────────────────────
        _prog   = _scan_read_progress()
        _status = _prog.get("status", "idle")
        _pct    = _prog.get("pct", 0)
        _detail = _prog.get("detail", "")
        _res    = _scan_read_results()           # None → 无有效结果

        # ── 标题行 ────────────────────────────────────────────────
        _h_col1, _h_col2, _h_col3 = st.columns([4, 2, 2])
        with _h_col1:
            st.markdown(
                '<p style="font-size:13px;font-weight:700;margin-bottom:0.2rem;">' +
                '🏆 Top30 后台扫描 · 三市场 × 四策略</p>',
                unsafe_allow_html=True,
            )
        with _h_col3:
            if st.button("📲 推送钉钉", key="top30_dingtalk_push",
                         use_container_width=True,
                         help="一键把 Top30 推荐发送到钉钉群"):
                with st.spinner("推送中..."):
                    _ok, _msg = _dingtalk_push_top30(_res)
                if _ok:
                    st.toast("✅ 已推送到钉钉", icon="📲")
                else:
                    st.toast(f"❌ 推送失败：{_msg}", icon="⚠️")
        with _h_col2:
            _lbl = _scan_result_label()
            if _lbl:
                st.caption(_lbl)

        # ── 进行中：进度条 ────────────────────────────────────────
        if _status == "running" or (_scan_worker_running() and _res is None):
            st.info(f"⏳ 后台扫描中… {_detail}")
            st.progress(min(_pct, 99) / 100)
            st.caption("扫描完成后结果将自动展示，页面将每 20 秒自动刷新")
            return

        # ── 无结果 / 过期：展示启动区域 ───────────────────────────
        if _res is None:
            if _GIST_ID:
                st.info("🔍 尚无扫描结果。云端 GitHub Actions 每天自动扫描4次，结果将自动同步。也可点下方按钮立即在本机后台扫描。")
            else:
                st.info("🔍 尚无扫描结果。点击下方按钮在**后台**启动全市场扫描（约 5-8 分钟），期间可正常使用其他功能。")
            _us_c, _hk_c, _cn_c = len(RAW_US), len(RAW_HK), len(RAW_CN_TOP)
            st.caption(f"扫描池: 美股 {_us_c} + 港股 {_hk_c} + A股 {_cn_c} = {_us_c+_hk_c+_cn_c} 只 · 结果缓存 6 小时")
            _btn_col1, _btn_col2 = st.columns([2, 1])
            with _btn_col1:
                if st.button("🚀 启动后台全市场扫描", type="primary", use_container_width=True, key="top30_start_bg"):
                    _scan_start_worker(force=False)
                    st.toast("✅ 后台扫描已启动，约 5-8 分钟后结果自动刷新", icon="🚀")
                    st.rerun()
            with _btn_col2:
                if st.button("🔄 强制重扫", use_container_width=True, key="top30_force_bg",
                             help="清除缓存并重新扫描"):
                    _scan_force_clear()
                    _scan_start_worker(force=True)
                    st.toast("🔄 已清除缓存，强制重新扫描", icon="🔄")
                    st.rerun()
            return

        # ── 有结果：4-Tab 展示 ────────────────────────────────────
        _ts_str  = datetime.fromtimestamp(_res["timestamp"]).strftime("%m-%d %H:%M")
        _is_gist = _GIST_ID and _gist_local_cache.get("timestamp") == _res.get("timestamp")
        _src_tag = "☁️ 云端缓存" if _is_gist else "💻 本地扫描"
        _sync_st = _gist_sync_status()
        st.caption(f"📅 扫描完成于 {_ts_str}  ·  {_src_tag}  ·  {_scan_result_label()}  ·  20s 自动刷新")
        if _GIST_ID:
            _sync_color = "#10b981" if _gist_last_sync_ok else "#f59e0b"
            st.markdown(f'<p style="font-size:11px;color:{_sync_color};margin:0">{_sync_st}</p>',
                        unsafe_allow_html=True)

        _btn_c1, _btn_c2 = st.columns([8, 1])
        with _btn_c2:
            if st.button("🔄 重扫", key="top30_rescan",
                         help="清除缓存并重新扫描", use_container_width=True):
                _scan_force_clear()
                _scan_start_worker(force=True)
                st.toast("🔄 已触发重新扫描", icon="🔄")
                st.rerun()

        _t1, _t2, _t3, _t4 = st.tabs(
            ["🔥 趋势强势", "🎯 蓄势潜伏", "🎯 拐点Top10（赔率）", "🚀 启动Top10（胜率）"]
        )

        def _render_market_col(col, items, mkt_label, key_prefix):
            """渲染单市场结果列"""
            with col:
                bm = _res.get(mkt_label[-2:] if mkt_label.endswith(("美股","港股","A股")) else mkt_label, {})
                st.markdown(
                    f'<p style="font-size:12px;font-weight:600;margin-bottom:4px;">{mkt_label}</p>',
                    unsafe_allow_html=True,
                )
                if not items:
                    st.caption("暂无符合条件标的")
                    return
                df_show = pd.DataFrame(items)
                show_cols = [c for c in ["股票","代码","得分","形态","信号"] if c in df_show.columns]
                sel = st.dataframe(
                    df_show[show_cols] if show_cols else df_show,
                    use_container_width=True,
                    hide_index=True,
                    height=min(400, 40 + 35 * len(items)),
                    on_select="rerun",
                    selection_mode="single-row",
                    key=f"{key_prefix}_df",
                )
                try:
                    if sel and hasattr(sel, "selection") and sel.selection and sel.selection.rows:
                        idx  = sel.selection.rows[0]
                        row  = items[idx]
                        st.session_state.scan_selected_code = row["代码"]
                        st.session_state.scan_selected_name = row["股票"]
                        st.toast(f"✅ 已选中 {row['股票']}", icon="🎯")
                        st.rerun()
                except Exception:
                    pass

        def _render_dual_col(col, items, title, color, key_prefix):
            """渲染拐点/启动双通道列（含可展开理由）"""
            with col:
                st.markdown(
                    f'<p style="font-size:12px;font-weight:700;color:{color};margin-bottom:4px;">{title}</p>',
                    unsafe_allow_html=True,
                )
                if not items:
                    st.caption("暂无符合条件标的")
                    return
                for i, row in enumerate(items, 1):
                    exp_label = (f"{i}. **{row['股票']}** `{row['代码']}` · "
                                 f"{row.get('形态','')} · 得分 {row['得分']}")
                    with st.expander(exp_label, expanded=(i <= 3)):
                        st.markdown(f"**信号**：{row.get('信号','')}")
                        st.markdown(f"**理由**：{row.get('理由','')}")
                        st.caption(f"行业：{row.get('行业','')} ｜ 现价：{row.get('现价','')}")
                        if st.button(f"🔍 深度分析 {row['股票']}",
                                     key=f"{key_prefix}_{i}_{row['代码']}"):
                            st.session_state.scan_selected_code = row["代码"]
                            st.session_state.scan_selected_name = row["股票"]
                            st.rerun()

        _mkt_cfg = [
            ("US", "🇺🇸 美股"),
            ("HK", "🇭🇰 港股"),
            ("CN", "🇨🇳 A股"),
        ]

        # Tab 1：趋势强势
        with _t1:
            _c1, _c2, _c3 = st.columns(3)
            for (_mkey, _mlabel), _col in zip(_mkt_cfg, [_c1, _c2, _c3]):
                _items = _res.get(_mkey, {}).get("top", [])
                _render_market_col(_col, _items, _mlabel, f"top_{_mkey}")

        # Tab 2：蓄势潜伏
        with _t2:
            _c1, _c2, _c3 = st.columns(3)
            for (_mkey, _mlabel), _col in zip(_mkt_cfg, [_c1, _c2, _c3]):
                _items = _res.get(_mkey, {}).get("coil", [])
                _render_market_col(_col, _items, _mlabel, f"coil_{_mkey}")

        # Tab 3：拐点（赔率）
        with _t3:
            st.caption("三关全中（底部位置 + 结构不创新低 + 止跌买量）· 赔率佳 · 低吸布局窗口")
            for _mkey, _mlabel in _mkt_cfg:
                _mkt_data = _res.get(_mkey, {})
                _bm_r     = _mkt_data.get("bm_ret5", 0)
                st.markdown(f"**{_mlabel}** · 基准5日 {_bm_r:+.1f}%")
                _dc1, _dc2 = st.columns(2)
                _render_dual_col(_dc1, _mkt_data.get("inflection", []),
                                 "🎯 拐点Top10", "#8b5cf6", f"inf_{_mkey}")
                # 占位（右列空）
                with _dc2:
                    st.empty()
                st.divider()

        # Tab 4：启动（胜率）
        with _t4:
            st.caption("三中二（突破20日高 + 放量1.5x + 相对强度领跑）· 胜率佳 · 趋势介入窗口")
            for _mkey, _mlabel in _mkt_cfg:
                _mkt_data = _res.get(_mkey, {})
                _bm_r     = _mkt_data.get("bm_ret5", 0)
                st.markdown(f"**{_mlabel}** · 基准5日 {_bm_r:+.1f}%")
                _dc1, _dc2 = st.columns(2)
                with _dc1:
                    st.empty()
                _render_dual_col(_dc2, _mkt_data.get("breakout", []),
                                 "🚀 启动Top10", "#10b981", f"bo_{_mkey}")
                st.divider()

    _top30_fragment()

# 【V91.9】AI选股 Tab - Gemini 筛选短中长期好股，中美港各 Top3，15分钟缓存
with tab_ai_select:
    st.markdown("#### 🤖 AI 智能选股")
    st.caption("💡 基于量化扫描 + Gemini 分析，筛选短中长期值得关注的好股，中美港各 Top3")
    
    if 'ai_selector_results' not in st.session_state:
        st.session_state.ai_selector_results = None
    
    # 缓存检查（15分钟有效）
    ttl = get_smart_cache_ttl('daily')
    cached = False
    if st.session_state.ai_selector_results:
        ts = st.session_state.ai_selector_results.get('scan_timestamp', 0)
        if (time.time() - ts) < ttl:
            cached = True
            remaining = (ttl - (time.time() - ts)) / 60
            st.info(f"📦 使用缓存 | 剩余 {remaining:.1f} 分钟有效（交易日15分钟内不重复分析）")
    # 文件缓存兜底
    if not cached:
        loaded = _load_scan_cache_from_file('ai_selector', 'all')
        if loaded:
            st.session_state.ai_selector_results = loaded
            cached = True
            st.info("📦 使用缓存（文件持久化）")
    
    if st.button("🚀 一键 AI 选股（中美港 Top3）", type="primary", use_container_width=True):
        if cached:
            st.toast("📦 使用缓存，无需重新分析", icon="📦")
        else:
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            def _status(msg):
                status_text.text(msg)
            
            result, err = run_ai_stock_selector(progress_callback=_status)
            
            progress_bar.progress(1.0)
            progress_bar.empty()
            status_text.empty()
            
            if err:
                st.error(err)
            else:
                st.session_state.ai_selector_results = {
                    'type': 'ai_selector',
                    'scan_market': 'all',
                    'us': result.get('us', []),
                    'hk': result.get('hk', []),
                    'cn': result.get('cn', []),
                    'ai_report': result.get('ai_report', ''),
                    'scan_timestamp': time.time(),
                }
                _save_scan_cache_to_file(st.session_state.ai_selector_results)
                st.toast("✅ AI 选股完成", icon="🤖")
                st.rerun()
    
    # 清除缓存按钮
    if st.button("🗑️ 清除 AI 选股缓存", help="清除 AI 选股结果缓存", use_container_width=True):
        st.session_state.ai_selector_results = None
        ckey = _scan_cache_key('ai_selector', 'all')
        try:
            fp = SCAN_CACHE_DIR / f"{ckey}.pkl"
            if fp.exists():
                fp.unlink()
        except Exception:
            pass
        st.toast("✅ 已清除", icon="🗑️")
        st.rerun()
    
    # 显示结果
    if st.session_state.ai_selector_results:
        res = st.session_state.ai_selector_results
        ai_report = res.get('ai_report', '')
        if ai_report:
            st.markdown("---")
            st.markdown("### 📋 AI 选股报告（理由 · 背景 · 增长点）")
            st.markdown(ai_report)
            st.caption(f"📌 本报告由 AI 生成 · 模型: {_ai_model_label()}")
        
        # 可选：展示匹配的量化数据表格
        for mkt, key in [("美股", "us"), ("港股", "hk"), ("A股", "cn")]:
            arr = res.get(key, [])
            if arr:
                st.markdown(f"#### {mkt} 匹配量化数据")
                df = pd.DataFrame(arr)
                if not df.empty:
                    render_clickable_table(df, f"ai_selector_{key}")

# 【自选股分析】按中美港划分，逐只分析：催化、技术面、风险、操作建议（与钉钉日报同源）
with tab_watchlist:
    st.markdown("#### 📋 自选股分析")
    st.caption("💡 按中美港划分，对每只自选股逐只分析：近期催化、技术面、风险点、操作建议（持有/加仓/减仓/观望）")
    
    # 显示自选股列表
    st.markdown("**当前自选股**（与钉钉日报同源，可在代码中编辑 WATCHLIST）")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("🇺🇸 美股")
        for code, name in WATCHLIST.get("US", []):
            st.caption(f"• {name} ({code})")
    with col2:
        st.markdown("🇭🇰 港股")
        for code, name in WATCHLIST.get("HK", []):
            st.caption(f"• {name} ({code})")
    with col3:
        st.markdown("🇨🇳 A股")
        for code, name in WATCHLIST.get("CN", []):
            st.caption(f"• {name} ({code})")
    
    if 'watchlist_analysis' not in st.session_state:
        st.session_state.watchlist_analysis = None
    
    ttl = get_smart_cache_ttl('daily')
    cached = False
    if st.session_state.watchlist_analysis:
        ts = st.session_state.watchlist_analysis.get('timestamp', 0)
        if (time.time() - ts) < ttl:
            cached = True
            remaining = (ttl - (time.time() - ts)) / 60
            st.info(f"📦 使用缓存 | 剩余 {remaining:.1f} 分钟有效")
    
    if st.button("🚀 一键自选股分析（中美港逐只）", type="primary", use_container_width=True, key="btn_watchlist"):
        if cached:
            st.toast("📦 使用缓存，无需重新分析", icon="📦")
        else:
            progress_bar = st.progress(0)
            status_text = st.empty()
            report, err = run_watchlist_analysis(progress_callback=lambda m: status_text.text(m))
            progress_bar.progress(1.0)
            progress_bar.empty()
            status_text.empty()
            if err:
                st.error(err)
            else:
                st.session_state.watchlist_analysis = {'report': report, 'timestamp': time.time()}
                st.toast("✅ 自选股分析完成", icon="📋")
                st.rerun()
    
    if st.button("🗑️ 清除自选股分析缓存", help="清除自选股分析结果", use_container_width=True, key="btn_watchlist_clear"):
        st.session_state.watchlist_analysis = None
        st.toast("✅ 已清除", icon="🗑️")
        st.rerun()
    
    if st.session_state.watchlist_analysis:
        report = st.session_state.watchlist_analysis.get('report', '')
        if report:
            st.markdown("---")
            st.markdown("### 📋 自选股分析报告")
            st.markdown(report)
            st.caption(f"📌 本报告由 AI 生成 · 模型: {_ai_model_label()}")


# ═══════════════════════════════════════════════════════════════
# 【模块 ④】股票PK对决（仅在有对比股票时显示）
# ═══════════════════════════════════════════════════════════════
if st.session_state.get('pk_codes') and len(st.session_state.pk_codes) >= 2:
    _module_header("⚔️", "股票PK对决", "勾选2-4只股票对比分析", "#f093fb", "#f5576c")
    
    pk_codes = st.session_state.pk_codes
    pk_names = st.session_state.get('pk_names', pk_codes)
    
    st.markdown(f"### 📊 对比：{' vs '.join(pk_names)}")
    
    # 【V87.17】添加进度条
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    pk_results = []
    total_stocks = len(pk_codes)
    
    for idx, code in enumerate(pk_codes):
        name = pk_names[idx] if idx < len(pk_names) else code
        yf_code = to_yf_cn_code(code)
        
        # 【V87.17】更新进度
        progress_pct = (idx + 1) / total_stocks
        progress_bar.progress(progress_pct)
        status_text.text(f"正在获取 {name} 数据... ({idx + 1}/{total_stocks}, {progress_pct*100:.1f}%)")
        
        df_pk = fetch_stock_data(yf_code)
        
        if df_pk is not None and len(df_pk) > 0:
            metrics = calculate_metrics_all(df_pk, yf_code)
            quant = calculate_advanced_quant(df_pk)
            
            # 安全获取metrics数据
            if metrics:
                pk_results.append({
                    "股票": name,
                    "代码": code,
                    "当前价": f"{df_pk['Close'].iloc[-1]:.2f}",
                    "综合评分": metrics.get('score', 0),
                    "建议": metrics.get('suggestion', '观望'),
                    "RSI": f"{metrics.get('rsi', 50):.1f}",
                    "夏普比率": quant.get('sharpe', 'N/A'),
                    "最大回撤": quant.get('max_dd', 'N/A'),
                    "胜率": quant.get('win_rate', 'N/A'),
                    "盈亏比": quant.get('pl_ratio', 'N/A')
                })
    
    # 【V87.17】清除进度条
    progress_bar.empty()
    status_text.empty()
    
    if pk_results:
        # 显示对比表格
        df_pk_display = pd.DataFrame(pk_results)
        st.dataframe(df_pk_display, use_container_width=True, hide_index=True)
        
        # AI 综合点评
        st.markdown("---")
        st.markdown("#### 🤖 AI 综合点评")
        
        col_ai1, col_ai2 = st.columns([1, 4])
        with col_ai1:
            gen_pk_ai = st.button("⚡ 生成分析", key="btn_pk_ai_main", type="primary", use_container_width=True)
        with col_ai2:
            clear_pk = st.button("🔄 清除对比", key="btn_clear_pk", use_container_width=True)
        
        if clear_pk:
            st.session_state.pk_codes = None
            st.session_state.pk_names = None
            st.rerun()
        
        if gen_pk_ai:
            with st.spinner(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · PK对比分析"):
                pk_summary = "\n".join([
                    f"{r['股票']}({r['代码']}): 评分{r['综合评分']}, {r['建议']}, RSI={r['RSI']}, 夏普={r['夏普比率']}"
                    for r in pk_results
                ])
                
                prompt = _load_prompt("pk_analysis.txt", pk_summary=pk_summary)
                result = st.write_stream(call_gemini_api_stream(prompt))
                st.caption(f"📌 AI生成 · 模型: {_ai_model_label()}")
                if COPY_UTILS_AVAILABLE:
                    CopyUtils.create_copy_button(result, button_text="📋 复制", key="copy_pk")


# ═══════════════════════════════════════════════════════════════
# 【V90.3】行业热力已整合到「全球市场概览」第4个Tab
# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# 【模块 ⑤】AI市场简报
# ═══════════════════════════════════════════════════════════════
# 【V91.8】锚点：方便从深度作战室/侧边栏快速跳转
st.markdown('<div id="ai-market-brief"></div>', unsafe_allow_html=True)
_module_header("📰", "AI市场简报", "Gemini实时市场分析", "#3b82f6", "#8b5cf6", compact=True)
# ═══════════════════════════════════════════════════════════════
st.markdown("---")

from datetime import datetime as _dt_brief

# 共用样式常量
_BRIEF_CONTENT_STYLE = """<style>
.news-brief {
    background-color: #f9fafb;
    padding: 1.5rem;
    border-radius: 8px;
    border-left: 4px solid #3b82f6;
    font-size: 14px;
    line-height: 1.8;
    color: #374151;
}
.news-brief h1 { font-size: 20px !important; font-weight: 700 !important; margin: 1.4rem 0 0.6rem 0 !important; color: #111827 !important; }
.news-brief h2 { font-size: 17px !important; font-weight: 700 !important; margin: 1.2rem 0 0.5rem 0 !important; color: #1f2937 !important; border-bottom: 1px solid #e5e7eb; padding-bottom: 0.3rem; }
.news-brief h3 { font-size: 15px !important; font-weight: 600 !important; margin: 0.9rem 0 0.4rem 0 !important; color: #374151 !important; }
.news-brief p  { font-size: 14px !important; margin: 0.5rem 0 !important; }
.news-brief ul, .news-brief ol { font-size: 13px !important; margin: 0.4rem 0 !important; padding-left: 1.5rem !important; }
.news-brief li { margin: 0.3rem 0 !important; }
.news-brief strong { font-weight: 600 !important; color: #1f2937 !important; }
</style>"""

_brief_title_id = "brief-copy-all-btn"
st.markdown(f"""
<div style="display:flex; align-items:center; gap:10px; margin-bottom:1rem;">
  <span style="color:#1f2937; font-size:18px; font-weight:700;">📰 AI市场简报 · {_dt_brief.now().strftime("%Y-%m-%d")}</span>
  <button id="{_brief_title_id}"
    onclick="(function(){{
      var ta = document.getElementById('brief-raw-content');
      var txt = ta ? ta.value : '';
      function _done(){{
        var b=document.getElementById('{_brief_title_id}');
        if(!b) return;
        var orig=b.innerText; b.innerText='✅ 已复制';
        setTimeout(function(){{b.innerText=orig;}},1500);
      }}
      if(navigator.clipboard && txt){{
        navigator.clipboard.writeText(txt).then(_done).catch(function(){{
          var t=document.createElement('textarea');
          t.value=txt; document.body.appendChild(t);
          t.select(); document.execCommand('copy');
          document.body.removeChild(t); _done();
        }});
      }} else {{
        var t=document.createElement('textarea');
        t.value=txt; document.body.appendChild(t);
        t.select(); document.execCommand('copy');
        document.body.removeChild(t); _done();
      }}
    }})();"
    style="padding:2px 8px; font-size:11px; color:#6b7280; background:#f3f4f6;
           border:1px solid #d1d5db; border-radius:4px; cursor:pointer;
           line-height:1.4; white-space:nowrap;">
    📋 复制全文
  </button>
</div>
""", unsafe_allow_html=True)

# 【V92】固定使用 Gemini 2.5 Flash，不再提供模型选择
BRIEF_MODEL = "gemini-2.5-flash"

# ── 12小时文件缓存：打开页面即自动显示，无需点击 ─────────────────────────────
_brief_cached_content, _brief_cached_ts = _load_brief_cache()
if _brief_cached_content and "market_brief_latest" not in st.session_state:
    st.session_state["market_brief_latest"] = _brief_cached_content

_brief_cache_info_col, _brief_btn_col = st.columns([4, 1])
with _brief_cache_info_col:
    if _brief_cached_ts:
        _brief_age_h = (time.time() - _brief_cached_ts) / 3600
        _brief_remain_h = max(0.0, 12.0 - _brief_age_h)
        _brief_remain_m = int(_brief_remain_h * 60)
        _brief_gen_dt = _dt_brief.fromtimestamp(_brief_cached_ts).strftime("%m-%d %H:%M")
        _brief_remain_str = (
            f"{_brief_remain_h:.1f}h" if _brief_remain_h >= 1
            else f"{_brief_remain_m}min"
        )
        st.caption(
            f"📦 缓存简报 · {_brief_gen_dt} 生成 · "
            f"已缓存 {_brief_age_h:.1f}h · ⏳ 剩余 {_brief_remain_str}"
        )
    else:
        st.caption("⏳ 首次加载中，正在自动生成日报...")
with _brief_btn_col:
    # 点击即清除缓存并重新生成，始终是强制刷新
    do_generate = st.button("🔄 刷新简报", key="btn_market_brief", type="primary", use_container_width=True)
    if do_generate:
        try:
            _BRIEF_CACHE_FILE.unlink(missing_ok=True)
        except Exception:
            pass
        st.session_state.pop("market_brief_latest", None)
        st.session_state.pop("_brief_auto_gen_done", None)

# 【自动生成】首次打开页面且无缓存时，自动触发生成，无需手动点击
if (not _brief_cached_content
        and "market_brief_latest" not in st.session_state
        and not st.session_state.get("_brief_auto_gen_done")):
    st.session_state["_brief_auto_gen_done"] = True
    do_generate = True
# ─────────────────────────────────────────────────────────────────────────────

if do_generate:
        with st.spinner("🤖 Gemini 2.5 Flash 分析中..."):
            # 【V87.4】增强市场简报 - 获取实时数据
            us_pool, hk_pool, cn_pool = init_stock_pools()
            
            # 【V90修复】获取代表性指数数据 - 使用真实指数代码 + 标注日期避免误导
            indices_data = {}
            
            def _safe_index_change(code, label):
                """安全获取指数涨跌幅，返回带日期的描述"""
                try:
                    _idx_df = fetch_stock_data(code)
                    if _idx_df is not None and len(_idx_df) >= 2:
                        _last_date = _idx_df.index[-1]
                        _prev_date = _idx_df.index[-2]
                        _last_close = float(_idx_df['Close'].iloc[-1])
                        _prev_close = float(_idx_df['Close'].iloc[-2])
                        _chg = ((_last_close - _prev_close) / _prev_close * 100) if _prev_close > 0 else 0
                        _last_str = _last_date.strftime('%m/%d') if hasattr(_last_date, 'strftime') else str(_last_date)[-5:]
                        _prev_str = _prev_date.strftime('%m/%d') if hasattr(_prev_date, 'strftime') else str(_prev_date)[-5:]
                        return f"{label}: {_last_close:.2f}（{_prev_str}→{_last_str} 涨跌 {_chg:+.2f}%）"
                except Exception as _ie:
                    pass
                return f"{label}: 数据获取中"
            
            try:
                indices_data['US'] = _safe_index_change("^GSPC", "标普500指数")
                indices_data['HK'] = _safe_index_change("^HSI", "恒生指数")
                indices_data['CN'] = _safe_index_change("000001.SS", "上证综指")
            except:
                pass
            
            # 【选股引擎】二层候选：Explore（覆盖广）+ Trade（质量闸门），当天缓存复用
            _date_str = datetime.now().strftime("%Y-%m-%d")
            _cache_key = f"_market_brief_bundle_{_date_str}"
            _sel_data = None
            if SELECTION_ENGINE_AVAILABLE and mod_selection:
                if _cache_key not in st.session_state:
                    with st.spinner("📊 选股引擎：684池二层候选筛选中（Explore+Trade）..."):
                        try:
                            _sel_data = mod_selection.build_candidates_bundle(
                                us_pool, hk_pool, cn_pool,
                                fetch_fn=fetch_stock_data,
                                date_str=_date_str,
                            )
                            st.session_state[_cache_key] = _sel_data
                            mod_selection.verify_bundle_print(_sel_data)
                        except Exception as _e:
                            _safe_print(f"⚠️ 选股引擎异常，降级 pool[:15]: {_e}")
                            st.session_state[_cache_key] = None
                _sel_data = st.session_state.get(_cache_key)
                if _sel_data:
                    us_candidates = mod_selection.format_bundle_wsj_candidates(_sel_data, "US", "$", 100)
                    hk_candidates = mod_selection.format_bundle_wsj_candidates(_sel_data, "HK", "HK$", 100)
                    cn_candidates = mod_selection.format_bundle_wsj_candidates(_sel_data, "CN", "¥", 100)
                    _use_expanded_pool = True
                else:
                    _sel_data = None
                    _use_expanded_pool = False
            else:
                _use_expanded_pool = False
            if not SELECTION_ENGINE_AVAILABLE or not mod_selection or not _sel_data:
                from concurrent.futures import ThreadPoolExecutor, as_completed
                def _get_close_price(yf_code):
                    try:
                        _df = fetch_stock_data(yf_code)
                        if _df is not None and len(_df) > 0:
                            return float(_df['Close'].iloc[-1])
                    except Exception:
                        pass
                    return None
                _all_items = (
                    [(item, "$") for item in us_pool[:15]] +
                    [(item, "HK$") for item in hk_pool[:15]] +
                    [(item, "¥") for item in cn_pool[:15]]
                )
                with ThreadPoolExecutor(max_workers=8) as _exec:
                    _price_cache = {}
                    _futures = {_exec.submit(_get_close_price, it[0][2]): (it[0], it[1]) for it in _all_items}
                    for _f in as_completed(_futures):
                        _item, _pfx = _futures[_f]
                        try:
                            _price_cache[(_item[2], _pfx)] = _f.result()
                        except Exception:
                            _price_cache[(_item[2], _pfx)] = None
                def _fmt_cand(it, pfx):
                    p = _price_cache.get((it[2], pfx))
                    return f"{it[1]}({it[2]}): 日报价 {pfx}{p:.2f}" if p is not None else f"{it[1]}({it[2]})"
                us_candidates = [_fmt_cand(it, "$") for it in us_pool[:15]]
                hk_candidates = [_fmt_cand(it, "HK$") for it in hk_pool[:15]]
                cn_candidates = [_fmt_cand(it, "¥") for it in cn_pool[:15]]
                _use_expanded_pool = False
            
            # 获取当前日期与校验时间（Asia/Shanghai）
            from datetime import datetime
            from zoneinfo import ZoneInfo
            today = datetime.now().strftime("%Y年%m月%d日")
            _ts_shanghai = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")

            # 跨日去重：读取近3天已推荐代码
            _recent_codes = _get_recent_recommended_codes(days=3)
            _recent_block = (
                f"【近3日已推荐代码（禁止重复推荐）】{', '.join(_recent_codes)}\n"
                if _recent_codes else ""
            )

            prompt = f"""生成 WSJ-style 正文格式日报（非聊天体、非社群体、非钉钉模板），专业、客观、可核验。
**硬性要求**：战争/地缘政治（美国、伊朗、以色列、中东冲突、俄乌、制裁等）若有新闻必须输出，不得遗漏。此类敏感新闻对市场影响重大，优先级最高。

【数据口径】雅虎财经收盘价 | 【校验时间】{_ts_shanghai} (Asia/Shanghai 上海时区)

【日期】{today}

【指数数据】（括号内为实际交易日对比，请严格引用，禁止编造）
{indices_data.get('US', '美股数据获取中')}
{indices_data.get('HK', '港股数据获取中')}
{indices_data.get('CN', 'A股数据获取中')}

【候选池】必须从以下选择，日报价必须全文引用。候选池已从684母池筛选扩大，请从以下选择。
- 美股：{chr(10).join('- ' + c for c in us_candidates)}
- 港股：{chr(10).join('- ' + c for c in hk_candidates)}
- A股：{chr(10).join('- ' + c for c in cn_candidates)}
严禁编造代码！A股/港股必须用数字代码（如 600519.SS、00700.HK）。

【硬性规则】
1) 每个市场固定3只：1 立即建仓 + 1 中期跟进 + 1 观察
2) **最终推荐 3 只里至少 2 只必须来自 Trade 池**（优先选质量闸门通过的标的）
3) 每只推荐首行必须写为 **名称(代码)** 格式（如 **苹果(AAPL)**），便于系统标注现价
4) 观察 禁止给目标位和买入建议
5) 每只必须含：证据状态灯、R/R、三类失效、触发、基本面承接、技术确认、动作标签、仓位建议
6) 文末固定输出 数据 与 时间戳（Asia/Shanghai），并标注数据截点
7) **跨日去重**：{_recent_block}上述代码在近3日已推荐，本次9只推荐中禁止出现这些代码；若候选池内无其他合格标的，则可降级为「观察」后选入，但不得再次列为「立即建仓」或「中期跟进」
8) **行业多样性**：每个市场的3只推荐，必须覆盖至少2个不同行业/板块（如科技+医疗、消费+金融），禁止3只均来自同一行业

【V2.1 Action Gate】立即建仓 仅当以下全满足，否则自动降级 中期跟进 或 观察：
a) 证据状态灯 == ✅
b) 触发时效 ≤ 72h
c) 来源 Tier 为 A 或 B（禁止 Tier C）
d) R/R ≥ 2.0
e) 失效条件含 基本面+结构+事件 三类

【Source Tiering 来源分级】
- Tier A：交易所/SEC/公司公告/财报电话会原文/央行与部委官网/统计局
- Tier B：Bloomberg/Reuters/WSJ/FT/财新等权威媒体
- Tier C：未具名消息/二手转述/社媒（立即建仓 禁止 Tier C）

【QA Gate】日报发布前必须通过以下检查：
1) 立即建仓 须满足 Action Gate 全部条件
2) 若「触发=无」则 Action 只能是 观察 或 中期跟进，不得 立即建仓
3) 每只标的必须输出 Card Schema 全部字段
4) 市场驱动、市场格局、催化事件板按 美/港/A 三市场分别输出，不得缺区
5) 催化事件板：每市场「已发生」栏必须有至少1条可验证事件
6) 每段 90-130 字；先事实后影响再动作提示；禁用空泛语句除非给证据
7) 所有时间统一 Asia/Shanghai，并标注数据截点
8) 若任一推荐缺字段或规则冲突，文末输出「日报未通过质检」并列出错误项

【BUILD_NOW 判定器】仅当以下 6 项同时满足，动作标签才允许输出「立即建仓」；任一不满足则输出「中期跟进」或「观察」，严禁给「立即建仓」：
1) 24h/72h 内存在可核验硬催化：来源仅限公司公告/交易所文件/监管公告/财报电话会/权威媒体；若仅为传闻或二手转述，直接降级 中期跟进
2) 事件-利润-估值传导链完整：触发事件 → 关键经营指标变化 → EPS/FCF 修正方向 → 估值中枢影响
3) 预期差成立：说明当前市场共识与本策略判断的差异点（至少1条）
4) 赔率达标：5-20交易日上行概率≥60%、回撤风险概率≤35%、Reward/Risk≥2.0
5) 技术结构未破坏：未出现结构性破位+放量走坏
6) 失效条件可执行：必须给出 基本面失效、结构失效、事件失效 三类触发；缺任一项不得输出 立即建仓

【Card Schema】每只推荐必须含以下字段，缺一不可：
- 代码|名称
- 动作标签（BUILD_NOW/FOLLOW_MID/WATCH）
- 触发（24h/72h）
- 来源（含 tier）
- 机会概率/风险概率
- 建仓区间
- 仓位上限 + 分批节奏（如 40/30/30）
- R/R
- 失效条件（基本面/结构/事件）
- 数据时间戳（Asia/Shanghai）

【Market Section Format】市场驱动/市场格局/催化事件板按美股、港股、A股分别输出，不能缺区。每段限制在 90-130 字，先事实后影响再动作提示。

【Watch Upgrade Logic】观察 标的必须给出：
- 升级条件：满足 2/3 项时升级为 中期跟进
- 降级条件

【写作层硬约束】上半部分采用日报体，专业、有深度：
- 禁用「驱动1/驱动2/状态/主导变量/交易倾向/暂无新增可验证催化」等机器标签词
- 每个段落 90-130 字
- 禁用空泛语句（如「龙头稳固」「资金明显回流」）除非给证据
- 市场驱动、催化事件板：每市场至少1条，当日有新事件可多列
- **战争/地缘政治**：美伊以、中东、俄乌等若有新闻必须在「战争/地缘政治」节输出，并在市场驱动中体现影响
- 市场格局：每市场可写2-4句连贯段落，体现核心矛盾、资金风格、明日观察
- **必须含 AI 点评**：每个市场在事实陈述后，附带「点评」1-2句，有态度、有判断
- 句式有变化，避免重复「今日…受…影响…」模板句
- 催化事件板：每市场「已发生」栏必须有至少1条可验证的已落地事件（政策/数据/财报/公告/宏观数据）；禁止港股、A股「已发生」写「线索仍在形成中」，必须列举具体事件（如央行数据、统计局PMI/CPI、贸易数据、监管政策、行业公告等）；「线索仍在形成中」仅允许用于「待验证」栏

【输出要求】全文使用中文，禁止英文术语（除必要代码如 AAPL、600519.SS）

请按以下结构输出（不要称呼和结尾废话）：

---

## 标题
[一句话概括当日市场核心变化]

---

## 导语
[2-3句新闻导语，概括主要事实与结论，含时间锚点。若有美伊以/中东/俄乌等战争地缘新闻，必须在导语中体现]

---

## 战争/地缘政治（必含）
若有美国、伊朗、以色列、中东冲突、俄乌、制裁、能源供应等新闻，**必须**在本节输出，不得遗漏。格式：事件+来源+对股市/原油/避险资产的影响。

---

## 市场驱动

美/港/A 三市场必须分别输出，不得缺区。每市场至少1条，有新事件可多列。每段 90-130 字，先事实后影响再动作提示。每市场必须附带「点评」。

### 🇺🇸 美股
[短段落或要点，含事实+传导链+市场反应。可2-3句。]
**点评**：[1-2句，有态度、有判断，华尔街日报式编辑点评]

### 🇭🇰 港股
[短段落，可适当展开。]
**点评**：[1-2句，有态度、有判断]

### 🇨🇳 A股
[短段落，可适当展开。]
**点评**：[1-2句，有态度、有判断]

---

## 市场格局

美/港/A 三市场必须分别输出，不得缺区。每段 90-130 字，先事实后影响再动作提示。每市场必须附带「点评」。

### 🇺🇸 美股
[2-4句：核心矛盾→资金风格→明日观察，写成自然判断段。]
**点评**：[1-2句，有态度、有判断]

### 🇭🇰 港股
[同上，2-4句连贯段落。]
**点评**：[1-2句，有态度、有判断]

### 🇨🇳 A股
[同上，2-4句连贯段落。]
**点评**：[1-2句，有态度、有判断]

---

## 催化事件板

美/港/A 三市场必须分别输出，不得缺区。每段 90-130 字，先事实后影响再动作提示。
**硬性要求**：每市场「已发生」栏必须有至少1条可验证的已落地事件（含时间+来源+影响），禁止写「线索仍在形成中」；港股、A股必须列举具体事件（如央行/统计局数据、贸易数据、监管政策、行业公告等），无个股催化可写市场/宏观层面催化。「线索仍在形成中」仅允许用于「待验证」栏。

### 🇺🇸 美股
已发生：[必须1条以上，时间+事件+来源+影响]
待验证：[待确认线索；若无则写「线索仍在形成中」]

### 🇭🇰 港股
已发生：[必须1条以上，禁止「线索仍在形成中」；可写宏观/政策/行业数据]
待验证：[待确认线索；若无则写「线索仍在形成中」]

### 🇨🇳 A股
已发生：[必须1条以上，禁止「线索仍在形成中」；可写宏观/政策/行业数据]
待验证：[待确认线索；若无则写「线索仍在形成中」]

---

## 可执行推荐

每只推荐必须符合 Card Schema，含：代码|名称、动作标签、触发、来源(tier)、机会/风险概率、建仓区间、仓位上限+分批节奏、R/R、失效条件、时间戳。

### 🇺🇸 美股（固定3只：1 立即建仓 + 1 中期跟进 + 1 观察）
1. **[代码|名称]** · **立即建仓**（须满足 Action Gate 全条件）
   - 触发: [24h/72h] [事件] [来源·Tier A/B]
   - 机会概率/风险概率: [%/%]
   - 建仓区间: [区间]
   - 仓位上限 + 分批节奏: [如 40/30/30]
   - R/R: [≥2.0]
   - 失效条件: ① 基本面 ② 结构 ③ 事件
   - 证据状态灯: ✅
   - 数据时间戳: Asia/Shanghai

2. **[代码|名称]** · **中期跟进**
   - [同上 Card Schema 格式]

3. **[代码|名称]** · **观察**
   - [Card Schema 格式，观察 不输出目标位和买入建议]
   - **升级条件**：满足 2/3 项 → 升级为 中期跟进
   - **降级条件**：[具体条件]

### 🇭🇰 港股（固定3只：1 立即建仓 + 1 中期跟进 + 1 观察）
1. **[代码|名称]** · **立即建仓** · [Card Schema 全字段]
2. **[代码|名称]** · **中期跟进** · [Card Schema 全字段]
3. **[代码|名称]** · **观察** · [Card Schema 全字段 + 升级/降级条件]

### 🇨🇳 A股（固定3只：1 立即建仓 + 1 中期跟进 + 1 观察）
1. **[代码|名称]** · **立即建仓** · [Card Schema 全字段]
2. **[代码|名称]** · **中期跟进** · [Card Schema 全字段]
3. **[代码|名称]** · **观察** · [Card Schema 全字段 + 升级/降级条件]

---

## 风险提示
- [风险1]
- [风险2]
- [风险3]

---

## 明日触发-动作对照
若 事件A成立 → 动作X
若 事件B落空 → 动作Y
若 事件C发生 → 动作Z

---

## 数据/时间戳
数据: 雅虎财经收盘价
时间戳: {_ts_shanghai} (Asia/Shanghai 上海时区)
数据截点: {_ts_shanghai}

【QA Checker】若任一推荐缺 Card Schema 字段、或与 Action Gate/Source Tiering 规则冲突，则输出：
日报未通过质检
错误项：[逐条列出具体错误，如「美股推荐1 缺 R/R」「港股推荐1 来源 Tier C 禁止 BUILD_NOW」]"""
            
            _brief_ph = st.empty()
            res = ""
            for _chunk in call_gemini_api_stream(prompt, model_name=BRIEF_MODEL):
                res += _chunk
                _brief_ph.markdown(res + " ▌")
            _brief_ph.empty()
            # 【推荐个股现价】解析报告中的股票代码，拉取现价并标注
            if res and not res.startswith("❌"):
                def _inject_current_prices(text, _fetch_fn):
                    import re
                    lines = text.split("\n")
                    out = []
                    for line in lines:
                        m = re.search(r'\(([A-Z0-9]{2,}\.[A-Z]{2}|[A-Z0-9]{4,5}\.HK|[A-Z]{2,5})\)', line)
                        if m and any(kw in line for kw in ["立即建仓", "中期跟进", "观察"]):
                            code = m.group(1)
                            try:
                                df = _fetch_fn(code)
                                if df is not None and len(df) > 0 and "Close" in df.columns:
                                    p = float(df["Close"].iloc[-1])
                                    if ".HK" in code: pfx = "HK$"
                                    elif ".SS" in code or ".SZ" in code: pfx = "¥"
                                    else: pfx = "$"
                                    line = line.rstrip() + f" 现价 {pfx}{p:.2f}"
                            except Exception:
                                pass
                        out.append(line)
                    return "\n".join(out)
                try:
                    res = _inject_current_prices(res, fetch_stock_data)
                except Exception as _ep:
                    _safe_print(f"⚠️ 现价注入跳过: {_ep}")
            # 【选股引擎】复盘元数据：单独存储，不混入正文
            _meta_html = ""
            if _use_expanded_pool and _sel_data and res and not res.startswith("❌"):
                def _bundle_line(mkt):
                    d = _sel_data.get(mkt, {})
                    s = d.get("subpool_stats", {})
                    parts = [f"母池{s.get('mother_pool_size',0)} 子池{s.get('subpool_size',0)} 覆盖率{s.get('coverage_pct',0):.1f}%"]
                    for h in ["ST","MT","LT"]:
                        hd = d.get(h, {})
                        ex, tr = len(hd.get("explore",[])), len(hd.get("trade",[]))
                        q = hd.get("meta",{}).get("quantile_used",0)
                        parts.append(f"{h}:Ex={ex} Tr={tr}(q={q})")
                    return " ".join(parts)
                _meta_html = (
                    f'<div style="font-size:9px;color:#bbb;margin-top:6px;line-height:1.4;">'
                    f'选股复盘 · 日期:{_sel_data.get("date","")} · '
                    f'美{_bundle_line("US")} · 港{_bundle_line("HK")} · A{_bundle_line("CN")}'
                    f'</div>'
                )
            # 显示日报内容（无段落级复制按钮，全文复制入口在标题旁）
            st.markdown(_BRIEF_CONTENT_STYLE, unsafe_allow_html=True)
            import re as _re
            def _clean_brief(txt):
                # 删除：风险提示、明日触发、数据/时间戳、选股复盘 整段
                for pat in [
                    r'\n?#{1,3}\s*风险提示.*?(?=\n#{1,3}\s|\Z)',
                    r'\n?#{1,3}\s*明日触发.*?(?=\n#{1,3}\s|\Z)',
                    r'\n?#{1,3}\s*数据[/／]时间[戳]?.*?(?=\n#{1,3}\s|\Z)',
                    r'\n?数据[：:][^\n]*时间[戳]?[^\n]*\n?',
                    r'\n?\*数据[：:][^\n]*\n?',
                    r'\n?---\s*\n#{1,3}\s*选股复盘.*',
                    r'\n?#{1,3}\s*选股复盘.*',
                ]:
                    txt = _re.sub(pat, '', txt, flags=_re.DOTALL)
                return txt.rstrip()
            _res_display = _clean_brief(res)
            _res_escaped = _res_display.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
            st.markdown(f'<textarea id="brief-raw-content" style="display:none;">{_res_escaped}</textarea>',
                        unsafe_allow_html=True)
            st.markdown(f'<div class="news-brief">{_res_display}</div>', unsafe_allow_html=True)
            st.caption("📌 本报告由 AI 生成 · Gemini 2.5 Flash")
            st.download_button(
                "📥 下载简报",
                data=res,
                file_name=f"AI市场简报_{datetime.now().strftime('%Y%m%d')}.md",
                mime="text/markdown",
                key="download_market_brief"
            )
            
            # 保存到session_state供分享卡片使用，并写入12小时文件缓存
            st.session_state["market_brief_latest"] = res
            st.session_state["_brief_auto_gen_done"] = True
            _save_brief_cache(res)

# ── 缓存自动展示：刷新页面后无需重新生成，直接显示 ──────────────────────────
elif "market_brief_latest" in st.session_state:
    _auto_res = st.session_state["market_brief_latest"]
    st.markdown(_BRIEF_CONTENT_STYLE, unsafe_allow_html=True)
    import re as _re
    def _clean_brief(txt):
        for pat in [
            r'\n?#{1,3}\s*风险提示.*?(?=\n#{1,3}\s|\Z)',
            r'\n?#{1,3}\s*明日触发.*?(?=\n#{1,3}\s|\Z)',
            r'\n?#{1,3}\s*数据[/／]时间[戳]?.*?(?=\n#{1,3}\s|\Z)',
            r'\n?数据[：:][^\n]*时间[戳]?[^\n]*\n?',
            r'\n?\*数据[：:][^\n]*\n?',
            r'\n?---\s*\n#{1,3}\s*选股复盘.*',
            r'\n?#{1,3}\s*选股复盘.*',
        ]:
            txt = _re.sub(pat, '', txt, flags=_re.DOTALL)
        return txt.rstrip()
    _auto_clean = _clean_brief(_auto_res)
    _auto_escaped = _auto_clean.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    st.markdown(f'<textarea id="brief-raw-content" style="display:none;">{_auto_escaped}</textarea>',
                unsafe_allow_html=True)
    st.markdown(f'<div class="news-brief">{_auto_clean}</div>', unsafe_allow_html=True)
    st.caption("📌 本报告由 AI 生成 · Gemini 2.5 Flash · 来自12小时缓存")
    st.download_button(
        "📥 下载简报",
        data=_auto_res,
        file_name=f"AI市场简报_{datetime.now().strftime('%Y%m%d')}.md",
        mime="text/markdown",
        key="download_market_brief_cached",
    )
# ─────────────────────────────────────────────────────────────────────────────

# ═══════════════════════════════════════════════════════════════
# 【V92】AI 对话区 - 与 Gemini 2.5 Flash 互动
# ═══════════════════════════════════════════════════════════════
st.markdown("---")
_chat_col1, _chat_col2 = st.columns([3, 1])
with _chat_col1:
    st.markdown("### 💬 与 AI 对话")
with _chat_col2:
    if st.button("🗑️ 清空对话", key="clear_brief_chat"):
        st.session_state.brief_chat_messages = []
        st.rerun()
st.caption("向 AI 提问市场、个股、策略等，Gemini 2.5 Flash 实时回答")

if "brief_chat_messages" not in st.session_state:
    st.session_state.brief_chat_messages = []

for msg in st.session_state.brief_chat_messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

if prompt := st.chat_input("输入问题，如：今天美股怎么看？茅台还能买吗？"):
    st.session_state.brief_chat_messages.append({"role": "user", "content": prompt})
    with st.spinner("AI 思考中..."):
        _ctx = ""
        if "market_brief_latest" in st.session_state and st.session_state.market_brief_latest:
            _ctx = f"\n\n【参考：今日简报摘要】\n{st.session_state.market_brief_latest[:1500]}..."
        _chat_prompt = f"""你是资深金融分析师。用户问题：{prompt}
{_ctx}
请简洁专业回答（300字内），可引用上述简报内容。"""
        try:
            reply = call_gemini_api(_chat_prompt, model_name=BRIEF_MODEL)
            reply = reply if reply and not reply.startswith("❌") else "抱歉，当前无法回答，请稍后重试。"
        except Exception as e:
            reply = f"❌ 调用失败: {str(e)[:80]}"
    st.session_state.brief_chat_messages.append({"role": "assistant", "content": reply})
    st.rerun()

# 【V90.3 清理】render_clickable_table 已移至文件顶部（含快捷入口与深度分析）

# EOF - V77 交互彻底重构版