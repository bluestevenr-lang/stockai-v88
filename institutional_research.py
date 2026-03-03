# -*- coding: utf-8 -*-
"""
机构研究中心 (Institutional Research Center)
专业级投研决策系统 - V89.2

功能模块：
1. 市场前瞻分析（宏观+行业+资金流向）
2. 个股深度研究（技术+基本面+估值）
3. 机会雷达（量化评分+催化剂）
4. 风险预警（多维度风险评估）

作者：量化团队
创建：2026-01-25
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import time

class InstitutionalResearch:
    """
    机构研究中心 - 核心分析引擎
    
    设计理念：
    - 多维度：技术+基本面+市场情绪+资金流向
    - 前瞻性：基于趋势外推+催化剂分析
    - 量化：评分系统+概率分布
    - 专业：机构级分析框架
    """
    
    def __init__(self, data_provider, perf_monitor=None):
        self.dp = data_provider
        self.perf = perf_monitor
        self.logger = logging.getLogger(__name__)
        
        # 分析框架配置
        self.config = {
            'lookback_days': 252,  # 1年历史数据
            'forecast_days': 60,   # 预测60天
            'ma_periods': [5, 10, 20, 50, 200],
            'vol_window': 20,
            'momentum_window': 10,
            'risk_free_rate': 0.03,  # 3%无风险利率
        }
    
    # ═══════════════════════════════════════════════════════════
    # 1. 市场前瞻分析
    # ═══════════════════════════════════════════════════════════
    
    def analyze_market_outlook(self, market_regime: dict, market_name: str = '美股') -> dict:
        """
        市场前瞻分析（基于已有的市场体制+技术分析）
        
        参数：
            market_regime: 来自ExpectationLayer的市场体制分析
            market_name: 市场名称（'美股'/'港股'/'A股'）
        
        返回：
            {
                'market_name': '美股/港股/A股',
                'trend_direction': '上升趋势/下降趋势/震荡',
                'strength_score': 0-100,  # 趋势强度
                'key_levels': {...},  # 关键技术位
                'catalyst': [...],  # 未来催化剂
                'time_horizon': '1-2周/1-2月/3-6月',
                'confidence': 0-100,
                'recommendation': '进攻/防守/观望'
            }
        """
        try:
            result = {
                'market_name': market_name,
                'trend_direction': '未知',
                'strength_score': 0,
                'key_levels': {},
                'catalyst': [],
                'time_horizon': '1-2月',
                'confidence': 0,
                'recommendation': '观望',
                'analysis_time': datetime.now().strftime('%Y-%m-%d %H:%M')
            }
            
            # 基于市场体制判断趋势
            if not market_regime.get('data_ok', False):
                self.logger.warning(f"⚠️ {market_name} 数据不完整，返回默认结果")
                return result
            
            verdict = market_regime.get('verdict', 'Unknown')
            
            # ═════════════════════════════════════════════
            # 【V89.7】根据市场类型获取关键数据（强制float）
            # ═════════════════════════════════════════════
            if market_name == '美股':
                current_price = float(market_regime.get('spy_price', 0))
                ma50 = float(market_regime.get('ma50', 0))
                ma200 = float(market_regime.get('ma200', 0))
                vix = float(market_regime.get('vix_level', 20))
                volatility = vix  # 美股用VIX作为波动指标
            else:  # 港股 / A股
                current_price = float(market_regime.get('index_level', 0))
                ma50 = float(market_regime.get('ma50', 0))
                ma200 = float(market_regime.get('ma200', 0))
                volatility = float(market_regime.get('volatility', 25))
                vix = None
            
            self.logger.info(f"📊 {market_name}: price={current_price:.2f}, ma50={ma50:.2f}, ma200={ma200:.2f}, vol={volatility:.2f}, verdict={verdict}")
            
            # ═════════════════════════════════════════════
            # 【V89.7】动态趋势强度计算（不再用固定值！）
            # 基于价格偏离均线的百分比来动态打分
            # ═════════════════════════════════════════════
            if ma50 > 0 and ma200 > 0:
                # 计算价格偏离MA50的百分比（正=在上方，负=在下方）
                pct_vs_ma50 = (current_price - ma50) / ma50 * 100
                # 计算价格偏离MA200的百分比
                pct_vs_ma200 = (current_price - ma200) / ma200 * 100
                # 计算MA50偏离MA200的百分比（衡量中期趋势）
                ma50_vs_ma200 = (ma50 - ma200) / ma200 * 100
            else:
                pct_vs_ma50 = 0
                pct_vs_ma200 = 0
                ma50_vs_ma200 = 0
            
            self.logger.info(f"  偏离度: vs_MA50={pct_vs_ma50:+.2f}%, vs_MA200={pct_vs_ma200:+.2f}%, MA50vsMA200={ma50_vs_ma200:+.2f}%")
            
            # 动态趋势方向判断
            if current_price > ma50 and ma50 > ma200:
                result['trend_direction'] = '强势上升趋势'
                # 基础分70 + 偏离度加成（最高到95）
                base = 70
                bonus = min(25, pct_vs_ma50 * 3 + ma50_vs_ma200 * 2)
                result['strength_score'] = int(min(95, max(70, base + bonus)))
                
            elif current_price > ma50:
                result['trend_direction'] = '上升趋势'
                base = 55
                bonus = min(14, pct_vs_ma50 * 3)
                result['strength_score'] = int(min(69, max(55, base + bonus)))
                
            elif current_price > ma200 and current_price <= ma50:
                result['trend_direction'] = '震荡偏强'
                # 在MA200之上但MA50之下 - 看距离MA50有多远
                base = 45
                # 越接近MA50分数越高
                gap_ratio = abs(pct_vs_ma50) 
                bonus = max(-10, 5 - gap_ratio * 2)
                result['strength_score'] = int(min(54, max(40, base + bonus)))
                
            elif current_price < ma50 and current_price > ma200:
                result['trend_direction'] = '震荡整理'
                base = 40
                bonus = max(-10, pct_vs_ma200 * 1.5 - abs(pct_vs_ma50) * 2)
                result['strength_score'] = int(min(49, max(30, base + bonus)))
                
            elif current_price < ma200 and ma50 > ma200:
                result['trend_direction'] = '回调下跌'
                base = 30
                bonus = max(-15, pct_vs_ma200 * 2)  # 偏离越多分越低
                result['strength_score'] = int(min(35, max(15, base + bonus)))
                
            elif current_price < ma50 and ma50 < ma200:
                result['trend_direction'] = '弱势下降趋势'
                base = 20
                bonus = max(-15, (pct_vs_ma200 + ma50_vs_ma200) * 1.5)
                result['strength_score'] = int(min(29, max(5, base + bonus)))
            else:
                result['trend_direction'] = '方向不明'
                result['strength_score'] = 50
            
            self.logger.info(f"  趋势: {result['trend_direction']} | 强度: {result['strength_score']}/100")
            
            # ═════════════════════════════════════════════
            # 关键技术位（根据市场类型调整格式）
            # ═════════════════════════════════════════════
            if market_name == '美股':
                result['key_levels'] = {
                    'support_1': f"${ma50:.2f}",
                    'support_2': f"${ma200:.2f}",
                    'resistance_1': f"${current_price * 1.03:.2f}",
                    'resistance_2': f"${current_price * 1.05:.2f}"
                }
            else:  # 港股/A股
                result['key_levels'] = {
                    'support_1': f"{ma50:.0f}点",
                    'support_2': f"{ma200:.0f}点",
                    'resistance_1': f"{current_price * 1.03:.0f}点",
                    'resistance_2': f"{current_price * 1.05:.0f}点"
                }
            
            # ═════════════════════════════════════════════
            # 催化剂分析（每个市场不同）
            # ═════════════════════════════════════════════
            if market_name == '美股':
                if vix and vix < 15:
                    result['catalyst'].append(f'VIX={vix:.1f}，低波动环境利于趋势延续')
                elif vix and vix > 25:
                    result['catalyst'].append(f'VIX={vix:.1f}，高波动警告，关注风险事件')
                else:
                    result['catalyst'].append(f'VIX={vix:.1f}，波动适中')
                result['catalyst'].append(f'SPY偏离MA50: {pct_vs_ma50:+.2f}%')
            elif market_name == '港股':
                result['catalyst'].append(f'波动率{volatility:.1f}%，{"偏高需谨慎" if volatility > 25 else "处于正常区间"}')
                result['catalyst'].append(f'恒指偏离MA50: {pct_vs_ma50:+.2f}%')
            else:  # A股
                result['catalyst'].append(f'波动率{volatility:.1f}%，{"政策敏感期" if volatility > 30 else "市场相对平稳"}')
                result['catalyst'].append(f'上证偏离MA50: {pct_vs_ma50:+.2f}%')
            
            # ═════════════════════════════════════════════
            # 【V89.7】智能操作建议 - 综合verdict+趋势+强度+波动
            # ═════════════════════════════════════════════
            strength = result['strength_score']
            trend = result['trend_direction']
            
            if verdict == 'Risk On':
                result['catalyst'].append(f'{market_name}风险偏好回升，资金流入')
                if strength >= 70:
                    result['recommendation'] = '进攻'
                elif strength >= 50:
                    result['recommendation'] = '持有'
                else:
                    result['recommendation'] = '观望'
                    
            elif verdict == 'Risk Off':
                result['catalyst'].append(f'{market_name}避险情绪升温，建议降仓')
                if strength <= 30:
                    result['recommendation'] = '防守'
                elif strength <= 50:
                    result['recommendation'] = '减仓'
                else:
                    result['recommendation'] = '观望'
                    
            else:  # Neutral
                result['catalyst'].append(f'{market_name}市场方向待定')
                if strength >= 70:
                    result['recommendation'] = '进攻' if '上升' in trend else '减仓'
                elif strength >= 55:
                    result['recommendation'] = '持有' if '上升' in trend or '偏强' in trend else '观望'
                elif strength <= 35:
                    result['recommendation'] = '减仓' if '下降' in trend else '观望'
                else:
                    result['recommendation'] = '观望'
            
            # ═════════════════════════════════════════════
            # 【V89.7】动态置信度 - 基于数据质量+趋势清晰度+波动率
            # ═════════════════════════════════════════════
            base_confidence = 75 if market_regime.get('data_ok') else 30
            
            # 趋势清晰度加成：偏离MA50越远，趋势越清晰
            clarity_bonus = min(10, abs(pct_vs_ma50) * 2)
            
            # 波动率惩罚：波动越大，预测越不确定
            if market_name == '美股':
                vol_penalty = max(-15, -(vix - 15) * 0.5) if vix > 15 else 5
            else:
                vol_penalty = max(-15, -(volatility - 20) * 0.5) if volatility > 20 else 5
            
            # MA50和MA200方向一致性加成
            alignment_bonus = 5 if (pct_vs_ma50 > 0 and ma50_vs_ma200 > 0) or (pct_vs_ma50 < 0 and ma50_vs_ma200 < 0) else -3
            
            result['confidence'] = int(min(95, max(30, base_confidence + clarity_bonus + vol_penalty + alignment_bonus)))
            
            self.logger.info(f"  🎯 {market_name} 最终: {result['trend_direction']}({result['strength_score']}) | {result['recommendation']} | 置信度{result['confidence']}%")
            
            return result
        
        except Exception as e:
            self.logger.error(f"市场前瞻分析异常: {e}")
            return result
    
    # ═══════════════════════════════════════════════════════════
    # 2. 个股深度研究
    # ═══════════════════════════════════════════════════════════
    
    def deep_research_stock(self, stock_data: pd.DataFrame, code: str) -> dict:
        """
        个股深度研究（技术+量价+趋势）
        
        参数：
            stock_data: 股票历史数据（OHLCV）
            code: 股票代码
        
        返回：
            {
                'technical_score': 0-100,
                'trend_analysis': {...},
                'volume_analysis': {...},
                'momentum_signals': [...],
                'price_target': {...},
                'risk_reward_ratio': float,
                'investment_rating': 'A+/A/B+/B/C'
            }
        """
        try:
            if stock_data is None or len(stock_data) < 50:
                return self._empty_research()
            
            result = {
                'code': code,
                'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                'technical_score': 0,
                'trend_analysis': {},
                'volume_analysis': {},
                'momentum_signals': [],
                'price_target': {},
                'risk_reward_ratio': 0,
                'investment_rating': 'C'
            }
            
            # 计算技术指标
            df = stock_data.copy()
            current_price = float(df['Close'].iloc[-1])
            
            # MA系统
            for period in [5, 10, 20, 50, 200]:
                if len(df) >= period:
                    df[f'MA{period}'] = df['Close'].rolling(window=period).mean()
            
            # RSI
            delta = df['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['RSI'] = 100 - (100 / (1 + rs))
            rsi = float(df['RSI'].iloc[-1]) if not pd.isna(df['RSI'].iloc[-1]) else 50
            
            # MACD
            ema12 = df['Close'].ewm(span=12, adjust=False).mean()
            ema26 = df['Close'].ewm(span=26, adjust=False).mean()
            df['MACD'] = ema12 - ema26
            df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
            macd = float(df['MACD'].iloc[-1]) if not pd.isna(df['MACD'].iloc[-1]) else 0
            macd_signal = float(df['MACD_Signal'].iloc[-1]) if not pd.isna(df['MACD_Signal'].iloc[-1]) else 0
            
            # 波动率
            returns = df['Close'].pct_change()
            volatility = returns.rolling(window=20).std().iloc[-1] * np.sqrt(252) * 100
            
            # 1. 趋势分析
            ma5 = df['MA5'].iloc[-1] if 'MA5' in df.columns and not pd.isna(df['MA5'].iloc[-1]) else current_price
            ma20 = df['MA20'].iloc[-1] if 'MA20' in df.columns and not pd.isna(df['MA20'].iloc[-1]) else current_price
            ma50 = df['MA50'].iloc[-1] if 'MA50' in df.columns and not pd.isna(df['MA50'].iloc[-1]) else current_price
            
            trend_score = 0
            if current_price > ma5 > ma20 > ma50:
                trend_status = '强势多头排列'
                trend_score = 90
            elif current_price > ma5 > ma20:
                trend_status = '多头趋势'
                trend_score = 70
            elif current_price < ma5 < ma20 < ma50:
                trend_status = '弱势空头排列'
                trend_score = 20
            elif current_price < ma5 < ma20:
                trend_status = '空头趋势'
                trend_score = 40
            else:
                trend_status = '震荡整理'
                trend_score = 50
            
            result['trend_analysis'] = {
                'status': trend_status,
                'score': trend_score,
                'ma5': round(ma5, 2),
                'ma20': round(ma20, 2),
                'ma50': round(ma50, 2),
                'deviation_from_ma20': round((current_price / ma20 - 1) * 100, 2)
            }
            
            # 2. 量价分析
            avg_volume_20 = df['Volume'].rolling(window=20).mean().iloc[-1]
            current_volume = df['Volume'].iloc[-1]
            volume_ratio = current_volume / avg_volume_20 if avg_volume_20 > 0 else 1
            
            volume_score = 0
            if volume_ratio > 2:
                volume_status = '放量突破'
                volume_score = 85
            elif volume_ratio > 1.5:
                volume_status = '温和放量'
                volume_score = 70
            elif volume_ratio < 0.5:
                volume_status = '缩量整理'
                volume_score = 40
            else:
                volume_status = '量能正常'
                volume_score = 60
            
            result['volume_analysis'] = {
                'status': volume_status,
                'score': volume_score,
                'volume_ratio': round(volume_ratio, 2),
                'avg_volume_20d': int(avg_volume_20)
            }
            
            # 3. 动量信号
            momentum_score = 0
            
            # RSI信号
            if rsi > 70:
                result['momentum_signals'].append('⚠️ RSI超买（>70），注意回调风险')
            elif rsi < 30:
                result['momentum_signals'].append('✅ RSI超卖（<30），可能反弹')
                momentum_score += 20
            elif 40 < rsi < 60:
                result['momentum_signals'].append('📊 RSI中性区域，趋势待确认')
                momentum_score += 10
            else:
                momentum_score += 15
            
            # MACD信号
            if macd > macd_signal and macd > 0:
                result['momentum_signals'].append('🟢 MACD金叉且在零轴上，强势')
                momentum_score += 25
            elif macd > macd_signal:
                result['momentum_signals'].append('📈 MACD金叉，多头信号')
                momentum_score += 20
            elif macd < macd_signal and macd < 0:
                result['momentum_signals'].append('🔴 MACD死叉且在零轴下，弱势')
                momentum_score += 5
            else:
                momentum_score += 10
            
            # 4. 价格目标（基于技术分析）
            atr = (df['High'] - df['Low']).rolling(window=14).mean().iloc[-1]
            
            result['price_target'] = {
                'current_price': round(current_price, 2),
                'target_high': round(current_price + 2 * atr, 2),
                'target_low': round(current_price - 2 * atr, 2),
                'stop_loss': round(current_price - 1.5 * atr, 2),
                'time_horizon': '1-2月'
            }
            
            # 5. 风险收益比
            upside = (result['price_target']['target_high'] - current_price)
            downside = (current_price - result['price_target']['stop_loss'])
            result['risk_reward_ratio'] = round(upside / downside, 2) if downside > 0 else 0
            
            # 6. 综合技术评分
            result['technical_score'] = int((trend_score * 0.4 + volume_score * 0.3 + momentum_score * 0.3))
            
            # 7. 投资评级
            if result['technical_score'] >= 80:
                result['investment_rating'] = 'A+'
            elif result['technical_score'] >= 70:
                result['investment_rating'] = 'A'
            elif result['technical_score'] >= 60:
                result['investment_rating'] = 'B+'
            elif result['technical_score'] >= 50:
                result['investment_rating'] = 'B'
            else:
                result['investment_rating'] = 'C'
            
            return result
        
        except Exception as e:
            self.logger.error(f"个股深度研究异常: {e}")
            return self._empty_research()
    
    # ═══════════════════════════════════════════════════════════
    # 3. 机会雷达
    # ═══════════════════════════════════════════════════════════
    
    def opportunity_radar(self, stock_analysis: dict, market_outlook: dict) -> dict:
        """
        机会雷达 - 综合评分系统
        
        参数：
            stock_analysis: 个股深度研究结果
            market_outlook: 市场前瞻分析结果
        
        返回：
            {
                'opportunity_score': 0-100,
                'opportunity_level': '高/中/低',
                'entry_timing': '立即/等待回调/观望',
                'catalysts': [...],
                'optimal_entry_price': float,
                'position_size_suggestion': '重仓/中等/轻仓'
            }
        """
        try:
            result = {
                'opportunity_score': 0,
                'opportunity_level': '低',
                'entry_timing': '观望',
                'catalysts': [],
                'optimal_entry_price': 0,
                'position_size_suggestion': '轻仓'
            }
            
            if not stock_analysis or not market_outlook:
                return result
            
            # 1. 综合评分（技术40% + 市场环境30% + 风险收益比30%）
            tech_score = stock_analysis.get('technical_score', 0)
            market_score = market_outlook.get('strength_score', 50)
            rr_ratio = stock_analysis.get('risk_reward_ratio', 0)
            rr_score = min(rr_ratio * 20, 100)  # 风险收益比转评分
            
            opp_score = int(tech_score * 0.4 + market_score * 0.3 + rr_score * 0.3)
            result['opportunity_score'] = opp_score
            
            # 2. 机会等级
            if opp_score >= 75:
                result['opportunity_level'] = '🔥 高机会'
                result['entry_timing'] = '立即布局'
                result['position_size_suggestion'] = '重仓（30-40%）'
            elif opp_score >= 60:
                result['opportunity_level'] = '📈 中等机会'
                result['entry_timing'] = '适度参与'
                result['position_size_suggestion'] = '中等仓位（15-25%）'
            elif opp_score >= 45:
                result['opportunity_level'] = '📊 观察机会'
                result['entry_timing'] = '等待回调'
                result['position_size_suggestion'] = '轻仓试探（5-10%）'
            else:
                result['opportunity_level'] = '⚠️ 机会不足'
                result['entry_timing'] = '暂时观望'
                result['position_size_suggestion'] = '空仓等待'
            
            # 3. 催化剂汇总
            # 技术面催化剂
            if tech_score >= 70:
                result['catalysts'].append('技术形态良好，多头排列')
            
            momentum_signals = stock_analysis.get('momentum_signals', [])
            for signal in momentum_signals:
                if '金叉' in signal or 'RSI超卖' in signal:
                    result['catalysts'].append(signal)
            
            # 市场环境催化剂
            market_catalysts = market_outlook.get('catalyst', [])
            result['catalysts'].extend(market_catalysts[:2])  # 取前2个
            
            # 4. 最优入场价
            current_price = stock_analysis.get('price_target', {}).get('current_price', 0)
            trend_analysis = stock_analysis.get('trend_analysis', {})
            ma20 = trend_analysis.get('ma20', current_price)
            
            if opp_score >= 70:
                # 高机会，接受当前价
                result['optimal_entry_price'] = round(current_price, 2)
            else:
                # 等待回调到MA20附近
                result['optimal_entry_price'] = round(ma20 * 1.02, 2)
            
            return result
        
        except Exception as e:
            self.logger.error(f"机会雷达分析异常: {e}")
            return result
    
    # ═══════════════════════════════════════════════════════════
    # 4. 风险预警
    # ═══════════════════════════════════════════════════════════
    
    def risk_warning(self, stock_analysis: dict, market_outlook: dict) -> dict:
        """
        风险预警 - 多维度风险评估
        
        返回：
            {
                'risk_level': '高/中/低',
                'risk_score': 0-100,
                'risk_factors': [...],
                'stop_loss_price': float,
                'max_drawdown_tolerance': float,
                'risk_mitigation': [...]
            }
        """
        try:
            result = {
                'risk_level': '中',
                'risk_score': 50,
                'risk_factors': [],
                'stop_loss_price': 0,
                'max_drawdown_tolerance': 0.15,  # 默认15%
                'risk_mitigation': []
            }
            
            if not stock_analysis or not market_outlook:
                return result
            
            risk_points = 0
            
            # 1. 技术面风险
            tech_score = stock_analysis.get('technical_score', 50)
            if tech_score < 40:
                result['risk_factors'].append('⚠️ 技术形态弱势，趋势不明')
                risk_points += 25
            
            trend = stock_analysis.get('trend_analysis', {}).get('status', '')
            if '空头' in trend:
                result['risk_factors'].append('🔴 空头趋势，不宜逆势操作')
                risk_points += 30
            
            # 2. 动量风险
            momentum_signals = stock_analysis.get('momentum_signals', [])
            for signal in momentum_signals:
                if 'RSI超买' in signal:
                    result['risk_factors'].append('⚠️ RSI超买，回调风险')
                    risk_points += 20
                elif '死叉' in signal:
                    result['risk_factors'].append('🔴 MACD死叉，动能转弱')
                    risk_points += 20
            
            # 3. 市场环境风险
            market_strength = market_outlook.get('strength_score', 50)
            if market_strength < 40:
                result['risk_factors'].append('🌊 市场整体偏弱，系统性风险')
                risk_points += 25
            
            market_rec = market_outlook.get('recommendation', '')
            if market_rec == '防守':
                result['risk_factors'].append('🛡️ 市场建议防守，避险为主')
                risk_points += 20
            
            # 4. 风险收益比风险
            rr_ratio = stock_analysis.get('risk_reward_ratio', 0)
            if rr_ratio < 1.5:
                result['risk_factors'].append('📊 风险收益比不佳（<1.5）')
                risk_points += 15
            
            # 5. 综合风险评分
            result['risk_score'] = min(risk_points, 100)
            
            if result['risk_score'] >= 70:
                result['risk_level'] = '🔴 高风险'
                result['max_drawdown_tolerance'] = 0.08
            elif result['risk_score'] >= 40:
                result['risk_level'] = '🟡 中等风险'
                result['max_drawdown_tolerance'] = 0.15
            else:
                result['risk_level'] = '🟢 低风险'
                result['max_drawdown_tolerance'] = 0.20
            
            # 6. 止损价
            price_target = stock_analysis.get('price_target', {})
            result['stop_loss_price'] = price_target.get('stop_loss', 0)
            
            # 7. 风险缓释建议
            if result['risk_score'] >= 60:
                result['risk_mitigation'].append('降低仓位至10%以下')
                result['risk_mitigation'].append('严格执行止损，不抱幻想')
            elif result['risk_score'] >= 40:
                result['risk_mitigation'].append('控制仓位在20%以内')
                result['risk_mitigation'].append('分批建仓，降低风险')
            else:
                result['risk_mitigation'].append('正常仓位配置')
                result['risk_mitigation'].append('持续跟踪，动态调整')
            
            return result
        
        except Exception as e:
            self.logger.error(f"风险预警分析异常: {e}")
            return result
    
    # ═══════════════════════════════════════════════════════════
    # 辅助方法
    # ═══════════════════════════════════════════════════════════
    
    def _empty_research(self) -> dict:
        """空研究结果"""
        return {
            'technical_score': 0,
            'trend_analysis': {},
            'volume_analysis': {},
            'momentum_signals': ['数据不足'],
            'price_target': {},
            'risk_reward_ratio': 0,
            'investment_rating': 'N/A'
        }
    
    def comprehensive_report(self, code: str, stock_data: pd.DataFrame, 
                           market_regime: dict) -> dict:
        """
        综合研究报告（一站式）
        
        返回完整的机构研究报告，包括：
        - 市场前瞻
        - 个股深度
        - 机会雷达
        - 风险预警
        """
        try:
            # 1. 市场前瞻
            market_outlook = self.analyze_market_outlook(market_regime)
            
            # 2. 个股深度
            stock_research = self.deep_research_stock(stock_data, code)
            
            # 3. 机会雷达
            opportunity = self.opportunity_radar(stock_research, market_outlook)
            
            # 4. 风险预警
            risk_warn = self.risk_warning(stock_research, market_outlook)
            
            # 5. 综合报告
            report = {
                'code': code,
                'report_date': datetime.now().strftime('%Y-%m-%d %H:%M'),
                'market_outlook': market_outlook,
                'stock_research': stock_research,
                'opportunity': opportunity,
                'risk_warning': risk_warn,
                'executive_summary': self._generate_summary(
                    stock_research, opportunity, risk_warn
                )
            }
            
            return report
        
        except Exception as e:
            self.logger.error(f"综合研究报告生成异常: {e}")
            return {}
    
    def _generate_summary(self, research: dict, opportunity: dict, risk: dict) -> dict:
        """生成执行摘要"""
        try:
            tech_score = research.get('technical_score', 0)
            opp_score = opportunity.get('opportunity_score', 0)
            risk_score = risk.get('risk_score', 50)
            
            # 综合评级
            if opp_score >= 70 and risk_score < 40:
                rating = '强烈推荐'
                action = '积极买入'
            elif opp_score >= 60 and risk_score < 50:
                rating = '推荐'
                action = '适度买入'
            elif opp_score >= 50 or risk_score < 60:
                rating = '中性'
                action = '观望为主'
            else:
                rating = '不推荐'
                action = '暂时回避'
            
            return {
                'rating': rating,
                'action': action,
                'technical_score': tech_score,
                'opportunity_score': opp_score,
                'risk_score': risk_score,
                'investment_rating': research.get('investment_rating', 'N/A'),
                'time_horizon': opportunity.get('entry_timing', '观望')
            }
        
        except:
            return {
                'rating': '数据不足',
                'action': '暂无建议',
                'technical_score': 0,
                'opportunity_score': 0,
                'risk_score': 50
            }
