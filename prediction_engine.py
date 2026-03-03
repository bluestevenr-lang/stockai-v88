"""
前瞻预测引擎 - 机构级量化分析 + AI预测
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
功能：
  1. 机构生命线：VWAP (成交量加权平均价) - 识别大资金成本线
  2. 前瞻预测层：基于技术指标 + Gemini 2.5 Flash AI分析
  3. 多时间框架分析：短期/中期/长期趋势预测
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Tuple
import logging


class InstitutionalPredictor:
    """
    机构级预测器
    
    核心功能：
    1. VWAP计算 - 机构生命线
    2. Alpha因子 - 多因子量化分析
    3. 风险引擎 - 动态止损/仓位管理
    4. AI预测 - Gemini 2.5 Flash智能分析
    """
    
    def __init__(self, df: pd.DataFrame, code: str):
        """
        初始化预测器
        
        Args:
            df: 股票历史数据 (必须包含 OHLCV)
            code: 股票代码
        """
        self.df = df.copy()
        self.code = code
        self.alpha_factors = {}
        self.risk_metrics = {}
        self.prediction = {}
    
    def calculate_vwap(self, window: int = 20) -> pd.Series:
        """
        计算VWAP (成交量加权平均价)
        
        机构生命线含义：
        - VWAP是机构大单的平均成本
        - 价格在VWAP之上 → 机构盈利，多头占优
        - 价格在VWAP之下 → 机构亏损，空头占优
        - 突破VWAP → 趋势反转信号
        
        Args:
            window: 计算窗口（天数）
        
        Returns:
            VWAP序列
        """
        try:
            # 典型价格 (HLC3)
            typical_price = (self.df['High'] + self.df['Low'] + self.df['Close']) / 3
            
            # 成交量加权
            vwap = (typical_price * self.df['Volume']).rolling(window=window).sum() / \
                   self.df['Volume'].rolling(window=window).sum()
            
            return vwap.ffill()
        
        except Exception as e:
            logging.error(f"VWAP计算失败: {e}")
            return pd.Series(index=self.df.index, dtype=float)
    
    def calculate_alpha_factors(self) -> Dict[str, Any]:
        """
        计算Alpha因子（机构级量化指标）
        
        包括：
        1. VWAP偏离度 - 当前价格与机构成本线的距离
        2. RSI背离 - 价格与动量的背离（顶部/底部信号）
        3. 布林带挤压 - 波动率压缩（大行情前兆）
        4. 量价背离 - 成交量与价格趋势背离
        
        Returns:
            Alpha因子字典
        """
        factors = {}
        
        try:
            # 1. VWAP因子（20日）
            vwap_20 = self.calculate_vwap(window=20)
            current_price = self.df['Close'].iloc[-1]
            current_vwap = vwap_20.iloc[-1]
            
            if pd.notna(current_vwap) and current_vwap > 0:
                vwap_deviation = ((current_price - current_vwap) / current_vwap) * 100
                factors['vwap_20'] = current_vwap
                factors['vwap_deviation'] = vwap_deviation
                
                # VWAP信号
                if vwap_deviation > 5:
                    factors['vwap_signal'] = "🟢 强势（价格远高于机构成本）"
                elif vwap_deviation > 0:
                    factors['vwap_signal'] = "🟡 偏多（价格略高于机构成本）"
                elif vwap_deviation > -5:
                    factors['vwap_signal'] = "🟡 偏空（价格略低于机构成本）"
                else:
                    factors['vwap_signal'] = "🔴 弱势（价格远低于机构成本）"
            else:
                factors['vwap_20'] = None
                factors['vwap_deviation'] = 0
                factors['vwap_signal'] = "⚪ 无信号"
            
            # 2. RSI背离检测
            if len(self.df) >= 14:
                # 计算RSI
                delta = self.df['Close'].diff()
                gain = delta.where(delta > 0, 0).rolling(window=14).mean()
                loss = -delta.where(delta < 0, 0).rolling(window=14).mean()
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))
                
                # 检测背离（最近20天）
                if len(rsi) >= 20:
                    recent_prices = self.df['Close'].tail(20)
                    recent_rsi = rsi.tail(20)
                    
                    # 顶背离：价格新高，RSI未创新高
                    price_high_idx = recent_prices.idxmax()
                    rsi_high_idx = recent_rsi.idxmax()
                    
                    if price_high_idx > rsi_high_idx and recent_rsi.iloc[-1] < 70:
                        factors['rsi_divergence'] = "🔴 顶背离（潜在顶部）"
                    
                    # 底背离：价格新低，RSI未创新低
                    price_low_idx = recent_prices.idxmin()
                    rsi_low_idx = recent_rsi.idxmin()
                    
                    if price_low_idx > rsi_low_idx and recent_rsi.iloc[-1] > 30:
                        factors['rsi_divergence'] = "🟢 底背离（潜在底部）"
                    
                    if 'rsi_divergence' not in factors:
                        factors['rsi_divergence'] = "⚪ 无背离"
                else:
                    factors['rsi_divergence'] = "⚪ 数据不足"
            else:
                factors['rsi_divergence'] = "⚪ 数据不足"
            
            # 3. 布林带挤压（波动率指标）
            if len(self.df) >= 20:
                ma20 = self.df['Close'].rolling(window=20).mean()
                std20 = self.df['Close'].rolling(window=20).std()
                bb_width = (std20 / ma20 * 100).iloc[-1]
                
                # 历史波动率百分位
                bb_width_series = (std20 / ma20 * 100).dropna()
                if len(bb_width_series) >= 120:
                    bb_percentile = (bb_width_series.iloc[-1] < bb_width_series.tail(120)).sum() / 120 * 100
                    
                    if bb_percentile < 20:
                        factors['bb_squeeze'] = "🔥 极度挤压（6个月最低，大行情酝酿中）"
                    elif bb_percentile < 40:
                        factors['bb_squeeze'] = "🟡 轻度挤压（波动率偏低）"
                    else:
                        factors['bb_squeeze'] = "⚪ 正常波动"
                else:
                    factors['bb_squeeze'] = "⚪ 数据不足"
            else:
                factors['bb_squeeze'] = "⚪ 数据不足"
            
            # 4. 量价背离
            if len(self.df) >= 5:
                price_change_5d = ((self.df['Close'].iloc[-1] - self.df['Close'].iloc[-5]) / 
                                   self.df['Close'].iloc[-5] * 100)
                volume_change_5d = ((self.df['Volume'].iloc[-1] - self.df['Volume'].iloc[-5]) / 
                                    self.df['Volume'].iloc[-5] * 100)
                
                if price_change_5d > 3 and volume_change_5d < -20:
                    factors['volume_price_divergence'] = "⚠️ 价涨量缩（上涨乏力）"
                elif price_change_5d < -3 and volume_change_5d < -20:
                    factors['volume_price_divergence'] = "⚠️ 价跌量缩（杀跌减弱）"
                elif price_change_5d > 3 and volume_change_5d > 20:
                    factors['volume_price_divergence'] = "✅ 价涨量增（上涨有力）"
                elif price_change_5d < -3 and volume_change_5d > 20:
                    factors['volume_price_divergence'] = "⚠️ 价跌量增（杀跌加剧）"
                else:
                    factors['volume_price_divergence'] = "⚪ 量价正常"
            else:
                factors['volume_price_divergence'] = "⚪ 数据不足"
        
        except Exception as e:
            logging.error(f"Alpha因子计算失败: {e}")
        
        self.alpha_factors = factors
        return factors
    
    def calculate_risk_engine(self) -> Dict[str, Any]:
        """
        风险引擎 - 动态止损和仓位管理
        
        包括：
        1. ATR动态止损 - 基于波动率的智能止损
        2. Kelly仓位管理 - 最优仓位计算
        3. 风险评级 - A/B/C/D四级
        
        Returns:
            风险指标字典
        """
        risk = {}
        
        try:
            current_price = self.df['Close'].iloc[-1]
            
            # 1. ATR动态止损（14日ATR）
            if len(self.df) >= 14:
                high_low = self.df['High'] - self.df['Low']
                high_close = abs(self.df['High'] - self.df['Close'].shift(1))
                low_close = abs(self.df['Low'] - self.df['Close'].shift(1))
                
                tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
                atr = tr.rolling(window=14).mean().iloc[-1]
                
                # 止损价格（2.5倍ATR）
                stop_loss = current_price - 2.5 * atr
                stop_loss_pct = ((current_price - stop_loss) / current_price) * 100
                
                risk['atr'] = atr
                risk['stop_loss'] = stop_loss
                risk['stop_loss_pct'] = stop_loss_pct
            else:
                risk['atr'] = None
                risk['stop_loss'] = None
                risk['stop_loss_pct'] = 0
            
            # 2. Kelly仓位计算（简化版）
            if len(self.df) >= 60:
                # 计算历史胜率
                daily_returns = self.df['Close'].pct_change().dropna()
                recent_returns = daily_returns.tail(60)
                
                win_rate = (recent_returns > 0).sum() / len(recent_returns)
                avg_win = recent_returns[recent_returns > 0].mean() if (recent_returns > 0).any() else 0
                avg_loss = abs(recent_returns[recent_returns < 0].mean()) if (recent_returns < 0).any() else 0
                
                if avg_loss > 0:
                    win_loss_ratio = avg_win / avg_loss
                    # Kelly公式: f = (win_rate * win_loss_ratio - (1 - win_rate)) / win_loss_ratio
                    kelly = (win_rate * win_loss_ratio - (1 - win_rate)) / win_loss_ratio
                    kelly = max(0, min(kelly, 1))  # 限制在0-1之间
                    
                    # Kelly建议仓位（保守：0.25倍Kelly）
                    suggested_position = kelly * 0.25 * 100
                    risk['kelly_position'] = min(suggested_position, 20)  # 最高20%
                else:
                    risk['kelly_position'] = 5  # 默认5%
            else:
                risk['kelly_position'] = 5
            
            # 3. 风险评级
            if risk['stop_loss_pct']:
                if risk['stop_loss_pct'] < 3:
                    risk['risk_grade'] = "A级 (低风险)"
                elif risk['stop_loss_pct'] < 5:
                    risk['risk_grade'] = "B级 (中低风险)"
                elif risk['stop_loss_pct'] < 8:
                    risk['risk_grade'] = "C级 (中高风险)"
                else:
                    risk['risk_grade'] = "D级 (高风险)"
            else:
                risk['risk_grade'] = "未评级"
        
        except Exception as e:
            logging.error(f"风险引擎计算失败: {e}")
        
        self.risk_metrics = risk
        return risk
    
    def build_oracle_input_json(self) -> str:
        """
        构建Gemini预测输入JSON（完整上下文）
        
        Returns:
            格式化的JSON字符串
        """
        try:
            current_price = self.df['Close'].iloc[-1]
            price_change_1d = ((self.df['Close'].iloc[-1] - self.df['Close'].iloc[-2]) / 
                               self.df['Close'].iloc[-2] * 100) if len(self.df) >= 2 else 0
            price_change_5d = ((self.df['Close'].iloc[-1] - self.df['Close'].iloc[-5]) / 
                               self.df['Close'].iloc[-5] * 100) if len(self.df) >= 5 else 0
            price_change_20d = ((self.df['Close'].iloc[-1] - self.df['Close'].iloc[-20]) / 
                                self.df['Close'].iloc[-20] * 100) if len(self.df) >= 20 else 0
            
            # 构建输入数据
            input_data = {
                "stock_code": self.code,
                "current_price": f"{current_price:.2f}",
                "price_change_1d": f"{price_change_1d:+.2f}%",
                "price_change_5d": f"{price_change_5d:+.2f}%",
                "price_change_20d": f"{price_change_20d:+.2f}%",
                "alpha_factors": self.alpha_factors,
                "risk_metrics": self.risk_metrics
            }
            
            import json
            return json.dumps(input_data, ensure_ascii=False, indent=2)
        
        except Exception as e:
            logging.error(f"构建预测输入失败: {e}")
            return "{}"
    
    def call_gemini_oracle(self, gemini_api_key: str, model_name: str = "gemini-2.5-flash") -> Dict[str, Any]:
        """
        调用Gemini 2.5 Flash进行AI预测
        
        Args:
            gemini_api_key: Gemini API密钥
            model_name: 模型名称
        
        Returns:
            预测结果字典：
            {
                'bullish_prob': 概率(0-100),
                'regime': '看多/看空/震荡',
                'key_risk': '关键风险因素',
                'verdict': '操作建议'
            }
        """
        prediction = {
            'bullish_prob': 50,
            'regime': '震荡',
            'key_risk': '数据分析中',
            'verdict': '观望为主'
        }
        
        try:
            import google.generativeai as genai
            
            # 配置API
            genai.configure(api_key=gemini_api_key)
            model = genai.GenerativeModel(model_name)
            
            # 构建Prompt
            input_json = self.build_oracle_input_json()
            
            prompt = f"""你是顶级量化分析师，请基于以下数据进行精准预测：

{input_json}

**任务要求**：
1. 给出未来3-5个交易日的看涨概率（0-100）
2. 判断市场状态：看多/看空/震荡
3. 识别关键风险因素（1-2个最重要的）
4. 给出明确操作建议（买入/持有/观望/减仓）

**输出格式**（严格按以下格式，不要额外文字）：
看涨概率: XX%
市场状态: [看多/看空/震荡]
关键风险: [简短描述]
操作建议: [买入/持有/观望/减仓] - [理由一句话]

**注意**：
- VWAP偏离度：>5%强势，<-5%弱势
- RSI背离：顶背离看空，底背离看多
- 布林带挤压：极度挤压预示大行情
- 量价背离：需关注真实买盘力量
"""
            
            # 调用Gemini（60秒超时，避免卡死）
            response = model.generate_content(prompt, request_options={'timeout': 60})
            
            if response and response.text:
                text = response.text.strip()
                
                # 解析输出
                lines = [line.strip() for line in text.split('\n') if line.strip()]
                
                for line in lines:
                    if '看涨概率' in line or 'bullish' in line.lower():
                        try:
                            prob_str = line.split(':')[-1].strip().replace('%', '').replace('概率', '')
                            prediction['bullish_prob'] = int(float(prob_str))
                        except:
                            pass
                    
                    elif '市场状态' in line or 'regime' in line.lower() or '状态' in line:
                        if '看多' in line or '多头' in line:
                            prediction['regime'] = '看多'
                        elif '看空' in line or '空头' in line:
                            prediction['regime'] = '看空'
                        else:
                            prediction['regime'] = '震荡'
                    
                    elif '关键风险' in line or 'risk' in line.lower() or '风险' in line:
                        risk_text = line.split(':')[-1].strip()
                        if len(risk_text) > 5:
                            prediction['key_risk'] = risk_text
                    
                    elif '操作建议' in line or 'verdict' in line.lower() or '建议' in line:
                        prediction['verdict'] = line.split(':')[-1].strip()
                
                logging.info(f"Gemini预测成功: {self.code}")
            
            else:
                logging.warning("Gemini返回空内容")
        
        except Exception as e:
            logging.error(f"Gemini预测失败: {e}")
            prediction['verdict'] = f"AI预测失败: {str(e)[:50]}"
        
        self.prediction = prediction
        return prediction
    
    def calculate_chandelier_exit(self, atr_period: int = 22, atr_multiplier: float = 3.0) -> Dict[str, Any]:
        """
        计算 Chandelier Exit（吊灯止损）
        
        机构用法：
        - Chandelier Exit Long = 22日最高价 - 3×ATR(22) → 多头追踪止损线
        - Chandelier Exit Short = 22日最低价 + 3×ATR(22) → 空头追踪止损线
        - 价格跌破Long线 = 多头离场信号
        - 价格突破Short线 = 空头离场信号
        
        Args:
            atr_period: ATR计算周期（默认22个交易日≈1个月）
            atr_multiplier: ATR倍数（默认3倍，越大止损越宽松）
        
        Returns:
            包含 chandelier_long/short 序列和最新值的字典
        """
        result = {
            'chandelier_long': None,
            'chandelier_short': None,
            'ce_long_latest': 0,
            'ce_short_latest': 0,
            'signal': '无信号',
            'atr_22': 0
        }
        
        try:
            if len(self.df) < atr_period + 1:
                return result
            
            # 计算ATR(22)
            high_low = self.df['High'] - self.df['Low']
            high_close = abs(self.df['High'] - self.df['Close'].shift(1))
            low_close = abs(self.df['Low'] - self.df['Close'].shift(1))
            tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            atr = tr.rolling(window=atr_period).mean()
            
            # Chandelier Exit Long = 22日最高价 - 3×ATR(22)
            highest_high = self.df['High'].rolling(window=atr_period).max()
            chandelier_long = highest_high - atr_multiplier * atr
            
            # Chandelier Exit Short = 22日最低价 + 3×ATR(22)
            lowest_low = self.df['Low'].rolling(window=atr_period).min()
            chandelier_short = lowest_low + atr_multiplier * atr
            
            result['chandelier_long'] = chandelier_long
            result['chandelier_short'] = chandelier_short
            result['ce_long_latest'] = float(chandelier_long.iloc[-1]) if not pd.isna(chandelier_long.iloc[-1]) else 0
            result['ce_short_latest'] = float(chandelier_short.iloc[-1]) if not pd.isna(chandelier_short.iloc[-1]) else 0
            result['atr_22'] = float(atr.iloc[-1]) if not pd.isna(atr.iloc[-1]) else 0
            
            # 信号判定
            current_price = float(self.df['Close'].iloc[-1])
            if result['ce_long_latest'] > 0 and current_price < result['ce_long_latest']:
                result['signal'] = '🔴 跌破多头止损线（离场信号）'
            elif result['ce_short_latest'] > 0 and current_price > result['ce_short_latest']:
                result['signal'] = '🟢 突破空头止损线（反转信号）'
            else:
                result['signal'] = '🟡 在通道内运行（持有观望）'
        
        except Exception as e:
            logging.error(f"Chandelier Exit计算失败: {e}")
        
        return result
    
    def call_gemini_pre_mortem(self, gemini_api_key: str, model_name: str = "gemini-2.5-flash",
                               macro_context: dict = None) -> Dict[str, Any]:
        """
        AI风控官 - 事前验尸分析 (Pre-Mortem)
        
        不是告诉你为什么应该买，而是告诉你为什么可能会亏。
        
        Args:
            gemini_api_key: Gemini API密钥
            model_name: 模型名称
            macro_context: 宏观环境数据（来自ExpectationLayer）
        
        Returns:
            {
                'risk_1': '失败风险1',
                'risk_2': '失败风险2',
                'risk_3': '失败风险3',
                'reward_risk_ratio': 盈亏比(float),
                'verdict': '综合判定',
                'raw_text': '原始AI输出'
            }
        """
        pre_mortem = {
            'risk_1': '分析中...',
            'risk_2': '分析中...',
            'risk_3': '分析中...',
            'reward_risk_ratio': 0.0,
            'verdict': '分析中...',
            'raw_text': ''
        }
        
        try:
            import google.generativeai as genai
            
            genai.configure(api_key=gemini_api_key)
            model = genai.GenerativeModel(model_name)
            
            # 构建数据上下文
            input_json = self.build_oracle_input_json()
            
            # 宏观环境信息
            macro_info = ""
            if macro_context:
                macro_info = f"""
当前宏观环境：
- 市场体制：{macro_context.get('verdict', 'Unknown')}
- VIX恐慌指数：{macro_context.get('vix_level', 0):.1f}
- 10Y美债收益率：{macro_context.get('tnx_yield', 0):.2f}%（{'偏紧缩' if macro_context.get('tnx_yield', 0) > 4.5 else '中性' if macro_context.get('tnx_yield', 0) > 3.5 else '宽松'}）
- 美元指数：{macro_context.get('dxy_level', 0):.1f}（{'强美元' if macro_context.get('dxy_level', 0) > 105 else '中性' if macro_context.get('dxy_level', 0) > 100 else '弱美元'}）
- 建议仓位上限：{macro_context.get('position_cap', 80)}%
"""
            
            # Chandelier Exit 数据
            ce_data = self.calculate_chandelier_exit()
            ce_info = f"""
Chandelier Exit 追踪止损：
- 多头止损线：{ce_data.get('ce_long_latest', 0):.2f}
- 空头止损线：{ce_data.get('ce_short_latest', 0):.2f}
- 信号：{ce_data.get('signal', '无')}
"""
            
            prompt = f"""你是一名冷血风控官（Chief Risk Officer）。你的职责是阻止交易员犯错，而不是鼓励他交易。
你必须假设这笔交易会失败，然后找出最可能的失败原因。

{macro_info}

个股数据：
{input_json}

{ce_info}

**你的任务**：
1. 列出3个最可能导致这笔交易失败的具体原因（必须结合当前数据，不要泛泛而谈）
2. 计算盈亏比（预期盈利空间 / 止损空间，用ATR止损计算）
3. 给出最终判定

**输出格式**（严格遵守，每行一条）：
失败风险1: [具体原因，必须引用数据]
失败风险2: [具体原因，必须引用数据]
失败风险3: [具体原因，必须引用数据]
盈亏比: X.X
综合判定: [允许开仓/减半仓位/不建议开仓] - [一句话理由]

**判定标准**：
- 盈亏比 >= 2.0 且风险可控 → 允许开仓
- 盈亏比 1.5-2.0 或存在重大风险 → 减半仓位
- 盈亏比 < 1.5 或多重风险叠加 → 不建议开仓
"""
            
            response = model.generate_content(prompt, request_options={'timeout': 60})
            
            if response and response.text:
                text = response.text.strip()
                pre_mortem['raw_text'] = text
                
                lines = [line.strip() for line in text.split('\n') if line.strip()]
                risk_idx = 1
                
                for line in lines:
                    if '失败风险1' in line or (risk_idx == 1 and '风险' in line and ':' in line):
                        pre_mortem['risk_1'] = line.split(':', 1)[-1].strip() if ':' in line else line
                        risk_idx = 2
                    elif '失败风险2' in line or (risk_idx == 2 and '风险' in line and ':' in line):
                        pre_mortem['risk_2'] = line.split(':', 1)[-1].strip() if ':' in line else line
                        risk_idx = 3
                    elif '失败风险3' in line or (risk_idx == 3 and '风险' in line and ':' in line):
                        pre_mortem['risk_3'] = line.split(':', 1)[-1].strip() if ':' in line else line
                        risk_idx = 4
                    elif '盈亏比' in line:
                        try:
                            ratio_str = line.split(':')[-1].strip().split(':')[0].strip()
                            # 处理 "2.3:1" 或 "2.3" 格式
                            ratio_str = ratio_str.replace('：', ':')
                            if ':' in ratio_str:
                                ratio_str = ratio_str.split(':')[0]
                            pre_mortem['reward_risk_ratio'] = float(ratio_str)
                        except:
                            pre_mortem['reward_risk_ratio'] = 0.0
                    elif '综合判定' in line or '判定' in line:
                        pre_mortem['verdict'] = line.split(':', 1)[-1].strip() if ':' in line else line
                
                logging.info(f"Pre-Mortem分析完成: {self.code}")
        
        except Exception as e:
            logging.error(f"Pre-Mortem分析失败: {e}")
            pre_mortem['verdict'] = f"分析失败: {str(e)[:50]}"
        
        return pre_mortem
    
    def call_gemini_entry_advisor(self, gemini_api_key: str, entry_price: float, entry_date: str,
                                    candle_data: dict = None, model_name: str = "gemini-2.5-flash",
                                    macro_context: dict = None) -> Dict[str, Any]:
        """
        【V90 新增】AI入场顾问 - 基于用户选定的入场价位，给出止损止盈建议
        
        与公式计算不同，这里完全由AI根据上下文智能判断。
        
        Args:
            gemini_api_key: Gemini API密钥
            entry_price: 用户确认的入场价格
            entry_date: 用户选定的日期
            candle_data: 选定日期的OHLCV数据
            model_name: 模型名称
            macro_context: 宏观环境数据
        
        Returns:
            {
                'stop_loss': 止损价(float),
                'stop_loss_reason': 止损理由,
                'take_profit_1': 第一止盈目标(float),
                'take_profit_1_reason': 第一止盈理由,
                'take_profit_2': 第二止盈目标(float),
                'take_profit_2_reason': 第二止盈理由,
                'hold_period': 建议持仓周期,
                'entry_grade': 入场评分(A/B/C/D),
                'strategy_summary': 策略总结,
                'raw_text': 原始AI输出
            }
        """
        result = {
            'stop_loss': 0.0,
            'stop_loss_reason': '',
            'take_profit_1': 0.0,
            'take_profit_1_reason': '',
            'take_profit_2': 0.0,
            'take_profit_2_reason': '',
            'hold_period': '',
            'entry_grade': '',
            'strategy_summary': '',
            'raw_text': ''
        }
        
        try:
            import google.generativeai as genai
            
            genai.configure(api_key=gemini_api_key)
            model = genai.GenerativeModel(model_name)
            
            current_price = float(self.df['Close'].iloc[-1])
            
            # 构建近期K线数据摘要（最近30个交易日）
            recent_df = self.df.tail(30)
            kline_summary = []
            for idx, row in recent_df.iterrows():
                date_str = idx.strftime('%m/%d') if hasattr(idx, 'strftime') else str(idx)[-5:]
                kline_summary.append(f"{date_str}: O={row['Open']:.2f} H={row['High']:.2f} L={row['Low']:.2f} C={row['Close']:.2f} V={row['Volume']:.0f}")
            kline_text = '\n'.join(kline_summary[-15:])  # 最近15个交易日
            
            # 关键技术位
            _highs = self.df['High'].tail(60)
            _lows = self.df['Low'].tail(60)
            _closes = self.df['Close'].tail(60)
            recent_high = float(_highs.max())
            recent_low = float(_lows.min())
            ma5 = float(_closes.tail(5).mean())
            ma10 = float(_closes.tail(10).mean())
            ma20 = float(_closes.tail(20).mean())
            ma60 = float(_closes.mean())
            
            # VWAP
            vwap_val = self.alpha_factors.get('vwap_20', 0) if self.alpha_factors else 0
            
            # 选定K线信息
            candle_info = ""
            if candle_data:
                candle_info = f"""
用户选定的K线：
- 日期：{entry_date}
- 开盘：{candle_data.get('Open', 0):.2f}
- 最高：{candle_data.get('High', 0):.2f}
- 最低：{candle_data.get('Low', 0):.2f}
- 收盘：{candle_data.get('Close', 0):.2f}
- 成交量：{candle_data.get('Volume', 0):.0f}
"""
            
            # 宏观环境
            macro_info = ""
            if macro_context and macro_context.get('data_ok', False):
                macro_info = f"""
当前宏观环境：{macro_context.get('verdict', 'Unknown')}
VIX：{macro_context.get('vix_level', 0):.1f}
10Y美债：{macro_context.get('tnx_yield', 0):.2f}%
建议仓位上限：{macro_context.get('position_cap', 80)}%
"""
            
            prompt = f"""你是一名顶级交易策略师。用户在K线图上选定了一个价位，打算以此价格入场。
请基于完整的技术分析上下文，给出精准的止损和止盈建议。

【重要】不要使用固定公式（如"ATR×2"），而是根据实际的支撑位、压力位、关键均线、近期量价结构来智能判断。

股票代码：{self.code}
当前最新价：{current_price:.2f}
用户计划入场价：{entry_price:.2f}
{candle_info}
{macro_info}

关键技术位：
- 60日最高价：{recent_high:.2f}
- 60日最低价：{recent_low:.2f}
- MA5：{ma5:.2f}
- MA10：{ma10:.2f}
- MA20：{ma20:.2f}
- MA60：{ma60:.2f}
- VWAP(20日)：{vwap_val:.2f}

最近15个交易日K线：
{kline_text}

请给出以下建议（严格按格式输出）：

止损价: [价格数字]
止损理由: [为什么设在这里，引用具体支撑位/均线/形态]
第一止盈: [价格数字]（保守目标）
第一止盈理由: [为什么设在这里，引用具体压力位/均线/形态]
第二止盈: [价格数字]（激进目标）
第二止盈理由: [条件达成时的更高目标]
持仓周期: [建议持有多长时间，如"3-5个交易日"/"1-2周"/"中线1-2月"]
入场评分: [A/B/C/D]（A=绝佳机会，B=不错，C=一般，D=不建议）
策略总结: [一段话总结完整的交易策略，包含入场/止损/止盈/仓位建议]
"""
            
            response = model.generate_content(prompt)
            
            if response and response.text:
                text = response.text.strip()
                result['raw_text'] = text
                
                lines = [line.strip() for line in text.split('\n') if line.strip()]
                
                for line in lines:
                    line_lower = line.lower()
                    
                    if line.startswith('止损价') and ':' in line:
                        try:
                            val_str = line.split(':')[-1].strip().replace('$', '').replace('¥', '').replace('元', '').replace('美元', '')
                            # 提取第一个数字
                            import re
                            nums = re.findall(r'[\d.]+', val_str)
                            if nums:
                                result['stop_loss'] = float(nums[0])
                        except:
                            pass
                    
                    elif line.startswith('止损理由') and ':' in line:
                        result['stop_loss_reason'] = line.split(':', 1)[-1].strip()
                    
                    elif line.startswith('第一止盈') and ':' in line and '理由' not in line:
                        try:
                            val_str = line.split(':')[-1].strip().replace('$', '').replace('¥', '').replace('元', '').replace('美元', '')
                            import re
                            nums = re.findall(r'[\d.]+', val_str)
                            if nums:
                                result['take_profit_1'] = float(nums[0])
                        except:
                            pass
                    
                    elif line.startswith('第一止盈理由') and ':' in line:
                        result['take_profit_1_reason'] = line.split(':', 1)[-1].strip()
                    
                    elif line.startswith('第二止盈') and ':' in line and '理由' not in line:
                        try:
                            val_str = line.split(':')[-1].strip().replace('$', '').replace('¥', '').replace('元', '').replace('美元', '')
                            import re
                            nums = re.findall(r'[\d.]+', val_str)
                            if nums:
                                result['take_profit_2'] = float(nums[0])
                        except:
                            pass
                    
                    elif line.startswith('第二止盈理由') and ':' in line:
                        result['take_profit_2_reason'] = line.split(':', 1)[-1].strip()
                    
                    elif line.startswith('持仓周期') and ':' in line:
                        result['hold_period'] = line.split(':', 1)[-1].strip()
                    
                    elif line.startswith('入场评分') and ':' in line:
                        result['entry_grade'] = line.split(':', 1)[-1].strip()
                    
                    elif line.startswith('策略总结') and ':' in line:
                        result['strategy_summary'] = line.split(':', 1)[-1].strip()
                
                logging.info(f"AI入场顾问分析完成: {self.code} @ {entry_price}")
        
        except Exception as e:
            logging.error(f"AI入场顾问分析失败: {e}")
            result['strategy_summary'] = f"分析失败: {str(e)[:80]}"
        
        return result
    
    def run_full_analysis(self, gemini_api_key: Optional[str] = None) -> Dict[str, Any]:
        """
        运行完整分析流程
        
        Args:
            gemini_api_key: Gemini API密钥（可选，如果提供则调用AI预测）
        
        Returns:
            完整分析结果
        """
        # 1. 计算Alpha因子
        alpha = self.calculate_alpha_factors()
        
        # 2. 计算风险指标
        risk = self.calculate_risk_engine()
        
        # 3. Chandelier Exit
        chandelier = self.calculate_chandelier_exit()
        
        # 4. AI预测（如果有API Key）
        prediction = {}
        if gemini_api_key:
            prediction = self.call_gemini_oracle(gemini_api_key)
        
        return {
            'alpha_factors': alpha,
            'risk_metrics': risk,
            'chandelier_exit': chandelier,
            'ai_prediction': prediction
        }


# ═══════════════════════════════════════════════════════════════
# 便捷函数
# ═══════════════════════════════════════════════════════════════

def analyze_stock_with_predictor(
    df: pd.DataFrame,
    code: str,
    gemini_api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    便捷函数：使用预测器分析单只股票
    
    Args:
        df: 股票数据
        code: 股票代码
        gemini_api_key: Gemini API密钥
    
    Returns:
        完整分析结果
    """
    predictor = InstitutionalPredictor(df, code)
    return predictor.run_full_analysis(gemini_api_key)
