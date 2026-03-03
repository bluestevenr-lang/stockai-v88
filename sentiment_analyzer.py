# -*- coding: utf-8 -*-
"""
舆情分析中心 (Sentiment Analysis Center)
V89.4 - 2026-01-25

功能模块：
1. 个股舆情分析（新闻+事件+市场情绪）
2. 市场舆情分析（美股/港股/A股整体情绪）
3. 舆情量化评分（情绪指数0-100）
4. 舆情对价格影响预判

作者：量化团队
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd

class SentimentAnalyzer:
    """
    舆情分析中心 - AI驱动的市场情绪和新闻分析
    
    核心功能：
    - 个股舆情：新闻动态+市场情绪+影响预判
    - 市场舆情：整体市场氛围+资金情绪+风险偏好
    - 量化评分：将定性分析转为量化指标
    """
    
    def __init__(self, gemini_api_caller=None):
        self.logger = logging.getLogger(__name__)
        self.call_ai = gemini_api_caller
    
    def generate_stock_sentiment_prompt(self, code: str, stock_data: pd.DataFrame, 
                                       market_regime: dict = None) -> str:
        """
        生成个股舆情分析提示词
        
        参数：
            code: 股票代码
            stock_data: 股票历史数据
            market_regime: 市场体制数据（可选）
        
        返回：
            完整的AI提示词
        """
        if stock_data is None or len(stock_data) == 0:
            return ""
        
        # 提取关键数据
        current_price = stock_data['Close'].iloc[-1]
        price_1d = ((stock_data['Close'].iloc[-1] - stock_data['Close'].iloc[-2]) / stock_data['Close'].iloc[-2] * 100) if len(stock_data) > 1 else 0
        price_5d = ((stock_data['Close'].iloc[-1] - stock_data['Close'].iloc[-6]) / stock_data['Close'].iloc[-6] * 100) if len(stock_data) > 5 else 0
        price_20d = ((stock_data['Close'].iloc[-1] - stock_data['Close'].iloc[-21]) / stock_data['Close'].iloc[-21] * 100) if len(stock_data) > 20 else 0
        
        # 成交量变化
        avg_vol_20 = stock_data['Volume'].rolling(20).mean().iloc[-1]
        current_vol = stock_data['Volume'].iloc[-1]
        vol_ratio = current_vol / avg_vol_20 if avg_vol_20 > 0 else 1
        
        # 市场环境
        market_context = ""
        if market_regime and market_regime.get('data_ok'):
            verdict = market_regime.get('verdict', 'Unknown')
            market_context = f"\n\n【市场大环境】\n当前市场体制: {verdict}\n市场风险偏好: {'高' if verdict == 'Risk On' else '低' if verdict == 'Risk Off' else '中性'}"
        
        _today_str = datetime.now().strftime("%Y年%m月%d日")
        
        prompt = f"""作为专业金融分析师，对 **{code}** 进行全面舆情分析。

【重要：当前日期是 {_today_str}，所有分析必须基于此日期附近的市场环境，不要使用过时信息】

【当前行情数据】
- 当前价格: {current_price:.2f}
- 近1日涨跌: {price_1d:+.2f}%
- 近5日涨跌: {price_5d:+.2f}%
- 近20日涨跌: {price_20d:+.2f}%
- 成交量变化: {vol_ratio:.2f}倍（相比20日均量）{market_context}

请生成完整的**舆情分析报告**（报告标题需包含日期 {_today_str}，600-800字），必须包含以下结构化内容：

## 📰 近期重要新闻动态（{_today_str} 前后1-2周，3-5条）
列出最近1-2周可能影响该股票的重要新闻或事件：
- 公司公告（财报、业绩、并购、重大合同等）
- 行业新闻（政策、技术突破、竞争格局等）
- 宏观事件（美联储、监管、地缘政治等）

每条新闻简要说明：**事件 + 时间 + 核心要点**

## 📊 新闻解读与影响分析
对核心新闻进行深度解读：
1. **利好因素**：哪些新闻对股价构成支撑
2. **利空因素**：哪些新闻对股价形成压力
3. **中性因素**：市场已消化或影响有限的因素

## 💬 市场情绪评估
- **当前情绪**：正面/中性/负面（给出量化评分0-100）
- **情绪理由**：为什么是这个情绪？结合成交量、涨跌幅、新闻
- **投资者关注焦点**：当前市场最关心什么
- **资金流向**：机构/散户的态度（流入/观望/流出）

## 🎯 舆情对股价的影响预判
- **短期影响（1-2周）**：上涨/下跌/震荡，预期幅度X%
- **中期影响（1-3个月）**：趋势判断和关键驱动因素
- **关键催化剂**：未来1-2个月内可能改变走势的重要事件（财报、会议、新品发布等）

## 🎖️ 舆情综合评级
给出舆情评分（0-100分）和投资建议：
- **90-100分**：舆情极度正面，重大利好
- **70-89分**：舆情正面，建议关注
- **50-69分**：舆情中性，观望为主
- **30-49分**：舆情偏负面，谨慎对待
- **0-29分**：舆情极度负面，建议回避

## ⚠️ 风险提示
列出需要重点关注的风险：
- 政策风险、行业风险、公司风险
- 可能的黑天鹅事件
- 建议的应对策略

---

**格式要求**：
- 使用Markdown格式
- 标题用##
- 重点内容用**加粗**
- 数字要具体（不要模糊表述）
- 每个部分至少50字
- 总字数600-800字"""

        return prompt
    
    def generate_market_sentiment_prompt(self, market_name: str = '美股', 
                                        market_regime: dict = None) -> str:
        """
        生成市场整体舆情分析提示词
        
        参数：
            market_name: 市场名称（'美股'/'港股'/'A股'）
            market_regime: 市场体制数据
        
        返回：
            完整的AI提示词
        """
        # 市场数据
        market_context = ""
        if market_regime and market_regime.get('data_ok'):
            verdict = market_regime.get('verdict', 'Unknown')
            
            if market_name == '美股':
                index_val = market_regime.get('spy_price', 0)
                vix_val = market_regime.get('vix_level', 20)
                market_context = f"""
【当前市场数据】
- SPY指数: {index_val:.2f}
- VIX恐慌指数: {vix_val:.2f}
- 市场体制: {verdict}
- MA50: {market_regime.get('ma50', 0):.2f}
- MA200: {market_regime.get('ma200', 0):.2f}
"""
            elif market_name == '港股':
                index_val = market_regime.get('index_level', 0)
                vol_val = market_regime.get('volatility', 0)
                market_context = f"""
【当前市场数据】
- 恒生指数: {index_val:.0f}
- 年化波动率: {vol_val:.1f}%
- 市场体制: {verdict}
- MA50: {market_regime.get('ma50', 0):.0f}
- MA200: {market_regime.get('ma200', 0):.0f}
"""
            elif market_name == 'A股':
                index_val = market_regime.get('index_level', 0)
                vol_val = market_regime.get('volatility', 0)
                market_context = f"""
【当前市场数据】
- 上证指数: {index_val:.0f}
- 年化波动率: {vol_val:.1f}%
- 市场体制: {verdict}
- MA50: {market_regime.get('ma50', 0):.0f}
- MA200: {market_regime.get('ma200', 0):.0f}
"""
        
        from datetime import datetime as _dt_now
        _today_str = _dt_now.now().strftime("%Y年%m月%d日")
        
        prompt = f"""作为资深金融分析师，对 **{market_name}市场** 进行全面舆情分析。

【重要：当前日期是 {_today_str}，请确保分析基于此日期附近的市场环境，不要编造过时信息】
{market_context}

请生成完整的**市场舆情报告**（报告标题需包含日期 {_today_str}，500-700字），必须包含以下结构化内容：

## 📰 市场重大新闻事件（{_today_str} 前后一周）
列出最近1周影响{market_name}市场的重要事件：
- 宏观政策（央行决议、财政政策、监管政策）
- 地缘政治（国际关系、贸易、冲突）
- 经济数据（GDP、CPI、就业、PMI等）
- 突发事件（金融危机、企业爆雷、系统性风险）

## 💬 市场整体情绪
- **情绪评分**：0-100分（0=极度悲观，50=中性，100=极度乐观）
- **情绪依据**：结合新闻、指数涨跌、成交量、市场体制
- **投资者行为**：机构在加仓/减仓？散户在进场/离场？
- **资金流向**：流入风险资产还是流入避险资产？

## 🔮 市场展望（未来1-2周）
- **短期趋势**：上涨/下跌/震荡，预期幅度
- **关键支撑阻力**：指数的关键技术位
- **关键事件**：未来1-2周可能影响市场的重大事件（会议、数据公布等）

## 🎯 投资策略建议
基于当前舆情，给出具体的投资建议：
- **仓位建议**：重仓/中等/轻仓/空仓
- **板块配置**：重点关注哪些板块，回避哪些板块
- **风控措施**：止损位、仓位管理、对冲策略

## ⚠️ 风险提示
- 当前市场的主要风险点
- 需要警惕的潜在风险
- 黑天鹅事件概率

---

**格式要求**：
- 使用Markdown格式
- 标题用##
- 重点用**加粗**
- 给出具体数字和日期
- 避免模糊表述
- 总字数500-700字"""

        return prompt
    
    def parse_sentiment_score(self, ai_response: str) -> dict:
        """
        从AI响应中提取舆情评分
        
        返回：
            {
                'sentiment_score': 0-100,
                'sentiment_level': '极度正面/正面/中性/负面/极度负面',
                'short_term_impact': '上涨/下跌/震荡',
                'key_catalysts': [...]
            }
        """
        try:
            result = {
                'sentiment_score': 50,
                'sentiment_level': '中性',
                'short_term_impact': '震荡',
                'key_catalysts': []
            }
            
            # 简单的关键词提取（可以后续用正则增强）
            response_lower = ai_response.lower()
            
            # 情绪评分估算
            positive_keywords = ['利好', '正面', '乐观', '上涨', '突破', '强势', '加仓']
            negative_keywords = ['利空', '负面', '悲观', '下跌', '跌破', '弱势', '减仓']
            
            positive_count = sum(1 for kw in positive_keywords if kw in response_lower)
            negative_count = sum(1 for kw in negative_keywords if kw in response_lower)
            
            # 计算情绪评分
            if positive_count > negative_count + 2:
                result['sentiment_score'] = 80
                result['sentiment_level'] = '正面'
            elif positive_count > negative_count:
                result['sentiment_score'] = 65
                result['sentiment_level'] = '偏正面'
            elif negative_count > positive_count + 2:
                result['sentiment_score'] = 30
                result['sentiment_level'] = '负面'
            elif negative_count > positive_count:
                result['sentiment_score'] = 45
                result['sentiment_level'] = '偏负面'
            else:
                result['sentiment_score'] = 50
                result['sentiment_level'] = '中性'
            
            # 短期影响
            if '上涨' in ai_response or '看涨' in ai_response:
                result['short_term_impact'] = '上涨'
            elif '下跌' in ai_response or '看跌' in ai_response:
                result['short_term_impact'] = '下跌'
            else:
                result['short_term_impact'] = '震荡'
            
            return result
        
        except Exception as e:
            self.logger.error(f"解析舆情评分异常: {e}")
            return result
    
    def get_sentiment_color(self, score: int) -> str:
        """根据舆情评分返回颜色"""
        if score >= 70:
            return "#10b981"  # 绿色
        elif score >= 50:
            return "#3b82f6"  # 蓝色
        elif score >= 30:
            return "#f59e0b"  # 橙色
        else:
            return "#ef4444"  # 红色
    
    def get_sentiment_icon(self, score: int) -> str:
        """根据舆情评分返回图标"""
        if score >= 80:
            return "🔥"
        elif score >= 70:
            return "📈"
        elif score >= 50:
            return "📊"
        elif score >= 30:
            return "⚠️"
        else:
            return "🔴"
