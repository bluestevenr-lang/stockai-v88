"""
AI 皇冠双核 V88 - 模块化架构
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
模块说明：
  - config.py          : 全局配置、常量、API密钥
  - cache.py           : 本地文件缓存系统（LRU + 大小限制）
  - data_fetch.py      : 数据获取（yfinance + 多源回退）
  - stock_pool.py      : 股票池管理（东财API + 本地备份）
  - analysis_core.py   : 核心分析算法（评分、技术指标、风险指标）
  - ai_engine.py       : AI分析引擎（Gemini API + Prompt管理）
  - ui_components.py   : UI组件（表格、卡片、进度条等）
  - utils.py           : 通用工具函数
  - sector_map.py      : 【V91.7】行业映射（全682只单一数据源，勿改）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

__version__ = "88.0"
__author__ = "AI皇冠双核团队"
