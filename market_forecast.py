"""
市场走势预测引擎 - 美股/港股/A股AI分析
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
功能：
  1. 三大市场技术分析（美股/港股/A股）
  2. 市场情绪指标（VIX、融资融券、北向资金等）
  3. Gemini 2.5 Flash AI预测（短期/中期/长期）
  4. 跨市场联动分析
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List
import logging


class MarketForecaster:
    """
    市场预测器 - 三大市场走势分析
    """
    
    def __init__(self):
        """初始化预测器"""
        self.us_analysis = {}
        self.hk_analysis = {}
        self.cn_analysis = {}
        self.cross_market = {}
    
    def analyze_market_technicals(
        self,
        df: pd.DataFrame,
        market_name: str
    ) -> Dict[str, Any]:
        """
        市场技术分析
        
        Args:
            df: 市场指数数据
            market_name: 市场名称（美股/港股/A股）
        
        Returns:
            技术分析结果
        """
        analysis = {
            'market': market_name,
            'current_price': 0,
            'trend': '震荡',
            'strength': 50,
            'support_levels': [],
            'resistance_levels': [],
            'technical_signals': []
        }
        
        try:
            if df is None or df.empty or len(df) < 20:
                analysis['error'] = '数据不足'
                return analysis
            
            current_price = df['Close'].iloc[-1]
            analysis['current_price'] = current_price
            
            # 1. 趋势判断（MA系统）
            if len(df) >= 200:
                ma20 = df['Close'].rolling(window=20).mean().iloc[-1]
                ma60 = df['Close'].rolling(window=60).mean().iloc[-1]
                ma120 = df['Close'].rolling(window=120).mean().iloc[-1]
                ma200 = df['Close'].rolling(window=200).mean().iloc[-1]
                
                # 多头排列：MA20 > MA60 > MA120 > MA200
                if ma20 > ma60 > ma120 > ma200 and current_price > ma20:
                    analysis['trend'] = '强势多头'
                    analysis['strength'] = 80
                elif ma20 > ma60 > ma120 and current_price > ma60:
                    analysis['trend'] = '多头'
                    analysis['strength'] = 65
                elif ma20 < ma60 < ma120 < ma200 and current_price < ma20:
                    analysis['trend'] = '强势空头'
                    analysis['strength'] = 20
                elif ma20 < ma60 < ma120 and current_price < ma60:
                    analysis['trend'] = '空头'
                    analysis['strength'] = 35
                else:
                    analysis['trend'] = '震荡'
                    analysis['strength'] = 50
                
                # 支撑/阻力位
                analysis['support_levels'] = [ma60, ma120, ma200]
                analysis['resistance_levels'] = [
                    current_price * 1.05,
                    current_price * 1.10,
                    df['High'].tail(60).max()
                ]
            
            # 2. 技术信号
            signals = []
            
            # RSI超买超卖
            if len(df) >= 14:
                delta = df['Close'].diff()
                gain = delta.where(delta > 0, 0).rolling(window=14).mean()
                loss = -delta.where(delta < 0, 0).rolling(window=14).mean()
                rs = gain / loss
                rsi = (100 - (100 / (1 + rs))).iloc[-1]
                
                if rsi > 70:
                    signals.append(f"⚠️ RSI超买({rsi:.1f})")
                elif rsi < 30:
                    signals.append(f"✅ RSI超卖({rsi:.1f})")
            
            # MACD信号
            if len(df) >= 26:
                ema12 = df['Close'].ewm(span=12).mean()
                ema26 = df['Close'].ewm(span=26).mean()
                macd_line = ema12 - ema26
                signal_line = macd_line.ewm(span=9).mean()
                
                macd_current = macd_line.iloc[-1]
                signal_current = signal_line.iloc[-1]
                macd_prev = macd_line.iloc[-2]
                signal_prev = signal_line.iloc[-2]
                
                # 金叉/死叉
                if macd_prev < signal_prev and macd_current > signal_current:
                    signals.append("🟢 MACD金叉")
                elif macd_prev > signal_prev and macd_current < signal_current:
                    signals.append("🔴 MACD死叉")
            
            # 成交量异常
            if len(df) >= 20:
                vol_ma20 = df['Volume'].rolling(window=20).mean().iloc[-1]
                vol_current = df['Volume'].iloc[-1]
                
                if vol_current > vol_ma20 * 1.5:
                    signals.append("💰 放量突破")
                elif vol_current < vol_ma20 * 0.5:
                    signals.append("📉 缩量整理")
            
            analysis['technical_signals'] = signals if signals else ['⚪ 无明显信号']
        
        except Exception as e:
            logging.error(f"{market_name}技术分析失败: {e}")
            analysis['error'] = str(e)
        
        return analysis
    
    def analyze_market_sentiment(self, market_name: str) -> Dict[str, Any]:
        """
        市场情绪分析（简化版，实际应接入实时数据）
        
        Args:
            market_name: 市场名称
        
        Returns:
            情绪指标
        """
        sentiment = {
            'market': market_name,
            'fear_greed_index': 50,
            'sentiment_label': '中性',
            'sentiment_notes': []
        }
        
        # 注：实际应用中应接入实时API获取：
        # - 美股：VIX恐慌指数、Put/Call Ratio
        # - A股：融资融券数据、北向资金流向
        # - 港股：沪港通/深港通净买入
        
        sentiment['sentiment_notes'].append("⚠️ 情绪指标需接入实时数据源")
        
        return sentiment
    
    def call_gemini_market_forecast(
        self,
        market_analyses: List[Dict[str, Any]],
        gemini_api_key: str,
        model_name: str = "gemini-2.5-flash"
    ) -> Dict[str, str]:
        """
        调用Gemini进行市场预测
        
        Args:
            market_analyses: 市场分析列表
            gemini_api_key: Gemini API密钥
            model_name: 模型名称
        
        Returns:
            预测结果（美股/港股/A股各一段）
        """
        forecasts = {
            '美股': '分析中...',
            '港股': '分析中...',
            'A股': '分析中...',
            '跨市场联动': '分析中...'
        }
        
        try:
            import google.generativeai as genai
            import json
            
            # 配置API
            genai.configure(api_key=gemini_api_key)
            model = genai.GenerativeModel(model_name)
            
            # 构建输入
            input_data = {
                "timestamp": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"),
                "markets": market_analyses
            }
            
            input_json = json.dumps(input_data, ensure_ascii=False, indent=2)
            
            prompt = f"""你是全球顶级宏观策略分析师，请基于以下三大市场数据进行深度预测：

{input_json}

**任务要求**：
1. **美股预测**（3-5个交易日）：
   - 趋势判断（上涨/下跌/震荡）+ 概率
   - 关键技术位（支撑/阻力）
   - 风险提示

2. **港股预测**（3-5个交易日）：
   - 趋势判断 + 概率
   - 与美股联动分析
   - 风险提示

3. **A股预测**（3-5个交易日）：
   - 趋势判断 + 概率
   - 政策/资金面分析
   - 风险提示

4. **跨市场联动**：
   - 三大市场相关性
   - 风险传染路径
   - 配置建议

**输出格式**（每个市场2-3句话，简洁专业）：

【美股】
[预测内容]

【港股】
[预测内容]

【A股】
[预测内容]

【跨市场联动】
[预测内容]

**注意**：
- 必须给出明确方向和概率
- 风险提示要具体
- 避免模糊表述
"""
            
            # 【V88.12.2】调用Gemini（添加超时控制）
            import signal
            
            def timeout_handler(signum, frame):
                raise TimeoutError("Gemini API调用超时")
            
            # 设置15秒超时（macOS不支持signal.alarm，用try-except包裹）
            try:
                response = model.generate_content(
                    prompt,
                    generation_config={
                        'temperature': 0.7,
                        'top_p': 0.8,
                        'max_output_tokens': 2048,
                    },
                    request_options={'timeout': 15}  # 15秒超时
                )
            except Exception as timeout_err:
                logging.warning(f"Gemini调用超时或失败: {timeout_err}")
                # 快速降级：返回基础分析
                forecasts['美股'] = "AI分析超时，请查看技术指标"
                forecasts['港股'] = "AI分析超时，请查看技术指标"
                forecasts['A股'] = "AI分析超时，请查看技术指标"
                forecasts['跨市场联动'] = "AI分析超时，已提供基础联动分析"
                return forecasts
            
            if response and response.text:
                text = response.text.strip()
                
                # 解析输出（按市场分段）
                sections = text.split('【')
                
                for section in sections:
                    if section.strip():
                        lines = section.split('\n', 1)
                        if len(lines) >= 2:
                            market_name = lines[0].strip().replace('】', '')
                            content = lines[1].strip()
                            
                            if market_name in forecasts:
                                forecasts[market_name] = content
                
                logging.info("Gemini市场预测成功")
            
            else:
                logging.warning("Gemini返回空内容")
        
        except Exception as e:
                logging.error(f"Gemini市场预测失败: {e}")
                forecasts['美股'] = "AI预测暂时不可用"
                forecasts['港股'] = "AI预测暂时不可用"
                forecasts['A股'] = "AI预测暂时不可用"
                forecasts['跨市场联动'] = "AI预测暂时不可用"
        
        return forecasts
    
    def run_full_market_analysis(
        self,
        us_df: Optional[pd.DataFrame] = None,
        hk_df: Optional[pd.DataFrame] = None,
        cn_df: Optional[pd.DataFrame] = None,
        gemini_api_key: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        运行完整市场分析
        
        Args:
            us_df: 美股指数数据（如S&P 500）
            hk_df: 港股指数数据（如恒生指数）
            cn_df: A股指数数据（如上证指数）
            gemini_api_key: Gemini API密钥
        
        Returns:
            完整分析结果
        """
        result = {
            'us_market': {},
            'hk_market': {},
            'cn_market': {},
            'ai_forecasts': {},
            'timestamp': pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # 1. 技术分析
        market_analyses = []
        
        if us_df is not None and not us_df.empty:
            result['us_market'] = self.analyze_market_technicals(us_df, '美股')
            market_analyses.append(result['us_market'])
        
        if hk_df is not None and not hk_df.empty:
            result['hk_market'] = self.analyze_market_technicals(hk_df, '港股')
            market_analyses.append(result['hk_market'])
        
        if cn_df is not None and not cn_df.empty:
            result['cn_market'] = self.analyze_market_technicals(cn_df, 'A股')
            market_analyses.append(result['cn_market'])
        
        # 2. AI预测（如果有API Key）
        if gemini_api_key and market_analyses:
            result['ai_forecasts'] = self.call_gemini_market_forecast(
                market_analyses,
                gemini_api_key
            )
        
        return result


# ═══════════════════════════════════════════════════════════════
# 便捷函数
# ═══════════════════════════════════════════════════════════════

def forecast_all_markets(
    us_df: Optional[pd.DataFrame] = None,
    hk_df: Optional[pd.DataFrame] = None,
    cn_df: Optional[pd.DataFrame] = None,
    gemini_api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    便捷函数：预测所有市场
    
    Args:
        us_df: 美股数据
        hk_df: 港股数据
        cn_df: A股数据
        gemini_api_key: Gemini API密钥
    
    Returns:
        完整预测结果
    """
    forecaster = MarketForecaster()
    return forecaster.run_full_market_analysis(us_df, hk_df, cn_df, gemini_api_key)
