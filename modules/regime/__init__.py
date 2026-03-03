# -*- coding: utf-8 -*-
"""
市场状态自适应筛选引擎 (Regime-Adaptive Screener)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
模块：
  - MarketRegime: 市场状态判定 (BULL/RANGE/BEAR)
  - StrategyRouter: 策略路由与权重调整
  - OpportunityClassifier: 三池分类 (BUILD_NOW/FOLLOW_MID/LONG_CORE/FILTERED)
  - RiskForecaster: 三概率预测
  - ActionEngine: 动作与仓位建议
  - QualityGuard: 强校验
  - ReportComposer: 报告组装
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from .market_regime import MarketRegime
from .strategy_router import StrategyRouter
from .opportunity_classifier import OpportunityClassifier
from .risk_forecaster import RiskForecaster
from .action_engine import ActionEngine
from .quality_guard import QualityGuard
from .report_composer import ReportComposer
from .position_utils import get_position_level_unified
try:
    from .expectation_gap_engine import ExpectationGapEngine
except ImportError:
    ExpectationGapEngine = None
try:
    from .long_compounder_gate import LongCompounderGate
except ImportError:
    LongCompounderGate = None
try:
    from .margin_of_safety_gate import MarginOfSafetyGate, UNINVESTABLE_HARD_FILTER
except ImportError:
    MarginOfSafetyGate = None
    UNINVESTABLE_HARD_FILTER = {}

__all__ = [
    "MarketRegime",
    "StrategyRouter",
    "OpportunityClassifier",
    "RiskForecaster",
    "ActionEngine",
    "QualityGuard",
    "ReportComposer",
    "get_position_level_unified",
    "ExpectationGapEngine",
    "LongCompounderGate",
    "MarginOfSafetyGate",
    "UNINVESTABLE_HARD_FILTER",
]
