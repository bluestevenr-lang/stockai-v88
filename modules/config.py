"""
配置模块 - 全局常量、API密钥、模型参数
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import os
import streamlit as st
from typing import Dict, Any

# ═══════════════════════════════════════════════════════════════
# 版本信息
# ═══════════════════════════════════════════════════════════════
APP_VERSION = "88.0"
APP_TITLE = "AI 皇冠双核"
APP_ICON = "👑"

# ═══════════════════════════════════════════════════════════════
# API 配置
# ═══════════════════════════════════════════════════════════════
GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY", os.getenv("GEMINI_API_KEY", ""))
GEMINI_MODEL_NAME = "gemini-2.5-flash"  # 保持2.5版本

# Gemini 模型选项（用于下拉框）
GEMINI_MODELS = {
    "gemini-2.5-flash": "Gemini 2.5 Flash（推荐）",
    "gemini-1.5-flash": "Gemini 1.5 Flash",
    "gemini-1.5-pro": "Gemini 1.5 Pro（更强）",
}

# ═══════════════════════════════════════════════════════════════
# 缓存配置
# ═══════════════════════════════════════════════════════════════
CACHE_TTL_SECONDS = 900  # 交易日15分钟缓存（非交易日由智能缓存切24小时）
CACHE_MAX_SIZE_MB = 1500  # 1.5GB缓存，确保3天数据
CACHE_DIR = ".cache_stock_data"

# ═══════════════════════════════════════════════════════════════
# 代理配置
# ═══════════════════════════════════════════════════════════════
PROXY_HOST = "127.0.0.1"
PROXY_PORT = 1082
PROXY_HTTP = f"http://{PROXY_HOST}:{PROXY_PORT}"
PROXY_HTTPS = f"http://{PROXY_HOST}:{PROXY_PORT}"

# ═══════════════════════════════════════════════════════════════
# 数据源配置
# ═══════════════════════════════════════════════════════════════
# yfinance 重试配置
YFINANCE_MAX_RETRIES = 3
YFINANCE_RETRY_DELAY = 0.5  # 初始延迟（秒），指数退避

# 数据质量要求
MIN_DATA_LENGTH = 5  # 最少数据点
REQUIRED_COLUMNS = ['Open', 'High', 'Low', 'Close', 'Volume']

# ═══════════════════════════════════════════════════════════════
# 股票池配置（扩大至800安全线内：美350+港200+A250）
# ═══════════════════════════════════════════════════════════════
TARGET_POOL_SIZE = {
    "US": 350,  # 美股目标数量（标普500代表，东财按市值排序）
    "HK": 200,  # 港股目标数量
    "CN": 250,  # A股目标数量（沪深300+创业板代表）
}

# 东方财富 API 配置
EASTMONEY_API_URL = "https://searchapi.eastmoney.com/api/suggest/get"
EASTMONEY_TOKEN = "D43BF722C8E33BDC906FB84D85E326E8"

# ═══════════════════════════════════════════════════════════════
# 扫描配置
# ═══════════════════════════════════════════════════════════════
SCAN_BATCH_SIZE = 20  # 每批扫描股票数（用于延迟控制）
SCAN_DELAY_SECONDS = 0.2  # 批次间延迟
SCAN_THREAD_POOL_SIZE = 6  # 并发线程数

# ═══════════════════════════════════════════════════════════════
# 技术指标参数
# ═══════════════════════════════════════════════════════════════
MA_PERIODS = {
    "MA5": 5,
    "MA10": 10,
    "MA20": 20,
    "MA30": 30,
    "MA60": 60,
    "MA120": 120,
    "MA200": 200,
}

RSI_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
BOLLINGER_WINDOW = 20
BOLLINGER_STD = 2

# ═══════════════════════════════════════════════════════════════
# 风险管理参数
# ═══════════════════════════════════════════════════════════════
RISK_FREE_RATE = 0.03  # 无风险利率（3%）
TOTAL_EQUITY = 100000  # 假设总资金（10万）
RISK_BUDGET_PCT = 0.01  # 单笔风险预算（1%）

# ═══════════════════════════════════════════════════════════════
# UI 配置
# ═══════════════════════════════════════════════════════════════
# 表格显示列
SCAN_RESULT_COLUMNS = ['股票', '代码', '得分', '板块', '建议']
COMPARISON_COLUMNS = ['股票', '代码', '当前价', '综合评分', '建议', 'RSI', '夏普比率', '最大回撤', '胜率', '盈亏比']

# 颜色配置
COLOR_SCHEME = {
    "primary": "#2563eb",
    "success": "#10b981",
    "warning": "#f59e0b",
    "danger": "#ef4444",
    "info": "#3b82f6",
}

# ═══════════════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════════════
def get_config_dict() -> Dict[str, Any]:
    """获取所有配置的字典表示"""
    return {
        "version": APP_VERSION,
        "cache_ttl": CACHE_TTL_SECONDS,
        "cache_max_size": CACHE_MAX_SIZE_MB,
        "gemini_model": GEMINI_MODEL_NAME,
        "target_pool_size": TARGET_POOL_SIZE,
        "scan_threads": SCAN_THREAD_POOL_SIZE,
    }

def validate_config() -> bool:
    """验证配置的有效性"""
    errors = []
    
    if not GEMINI_API_KEY:
        errors.append("❌ 未配置 Gemini API Key")
    
    if CACHE_TTL_SECONDS < 60:
        errors.append("⚠️ 缓存TTL过短，建议至少60秒")
    
    if SCAN_THREAD_POOL_SIZE > 10:
        errors.append("⚠️ 线程池过大，可能导致API限流")
    
    if errors:
        for error in errors:
            print(error)
        return False
    
    return True
