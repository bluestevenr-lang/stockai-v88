"""
核心分析模块 - CANSLIM + 专业投机原理 + 量化回测
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
包含：
  - calculate_metrics_all() - 综合评分系统
  - calculate_advanced_quant() - 高级量化指标
  - calculate_risk_metrics() - 风险指标（Alpha, Beta）
  - calculate_trade_plan() - 交易计划
  - monte_carlo_forecast() - 蒙特卡洛预测
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

注：此模块为简化版，完整实现请参考原app.py
由于代码量大（~800行），这里提供核心框架和关键函数
实际使用时可以从原app.py逐步迁移具体实现
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta

from .config import (
    MA_PERIODS,
    RSI_PERIOD,
    MACD_FAST,
    MACD_SLOW,
    MACD_SIGNAL,
    BOLLINGER_WINDOW,
    BOLLINGER_STD,
    RISK_FREE_RATE,
    TOTAL_EQUITY,
    RISK_BUDGET_PCT,
)


# ═══════════════════════════════════════════════════════════════
# 辅助函数
# ═══════════════════════════════════════════════════════════════

def get_benchmark_symbol(code: str) -> str:
    """
    根据股票代码获取对应的基准指数
    
    Args:
        code: 股票代码
        
    Returns:
        基准指数代码
    """
    if code.endswith('.HK'):
        return '^HSI'  # 恒生指数
    elif code.endswith(('.SS', '.SZ')):
        return '000001.SS'  # 上证指数
    else:
        return '^GSPC'  # 标普500


# ═══════════════════════════════════════════════════════════════
# 核心评分系统
# ═══════════════════════════════════════════════════════════════

def calculate_metrics_all(df: pd.DataFrame, code: str) -> Optional[Dict[str, Any]]:
    """
    完整的双核评级系统（CANSLIM + 专业投机原理）
    
    Args:
        df: 股票数据DataFrame
        code: 股票代码
        
    Returns:
        包含所有指标的字典，失败返回None
    """
    # 防御性检查
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
        # 数据清洗
        df = df.apply(pd.to_numeric, errors='coerce').dropna().sort_index()
        
        if df.empty or len(df) < 5:
            logging.warning(f"⚠️ {code} 清洗后数据不足")
            return None
        
        # 计算均线
        for period in [5, 10, 20, 30, 50, 60, 120, 150, 200, 250]:
            if len(df) >= period:
                df[f'MA{period}'] = df['Close'].rolling(period).mean()
            else:
                df[f'MA{period}'] = df['Close'].rolling(min(period, len(df))).mean()
        
        # 计算RSI
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)
        rs = gain.ewm(com=RSI_PERIOD-1).mean() / loss.ewm(com=RSI_PERIOD-1).mean()
        df['RSI'] = 100 - (100 / (1 + rs))
        df['RSI'] = df['RSI'].fillna(50)
        
        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else last
        
        # CANSLIM评分（简化版）
        score = 0
        
        # C: 当季收益（股价>MA50）
        if last['Close'] > last.get('MA50', 0):
            score += 15
        
        # A: 年度收益（股价>MA200）
        if last['Close'] > last.get('MA200', 0):
            score += 15
        
        # N: 新高突破
        if len(df) >= 60:
            high_60d = df['High'].tail(60).max()
            if last['Close'] >= high_60d * 0.95:
                score += 10
        
        # S: 供需关系（成交量）
        if len(df) >= 20:
            vol_ma20 = df['Volume'].tail(20).mean()
            if last['Volume'] > vol_ma20 * 1.2:
                score += 10
        
        # L: 领导地位（RSI>50）
        if last['RSI'] > 50:
            score += 10
        
        # I: 机构认同（均线多头排列）
        ma_bullish = (last.get('MA5', 0) > last.get('MA10', 0) > last.get('MA20', 0))
        if ma_bullish:
            score += 15
        
        # M: 市场方向（价格趋势）
        if len(df) >= 20:
            ret_20d = (last['Close'] - df['Close'].iloc[-21]) / df['Close'].iloc[-21]
            if ret_20d > 0:
                score += 10
        
        # 专业投机原理评分（简化版）
        spec_score = 0
        
        # 趋势强度
        if last['Close'] > last.get('MA60', 0):
            spec_score += 15
        
        # 动能
        if len(df) >= 5:
            ret_5d = (last['Close'] - df['Close'].iloc[-6]) / df['Close'].iloc[-6]
            if ret_5d > 0.03:
                spec_score += 15
        
        # 波动率适中
        if len(df) >= 20:
            volatility = df['Close'].tail(20).pct_change().std()
            if 0.01 < volatility < 0.05:
                spec_score += 10
        
        # 综合评分
        final_score = min(100, score + spec_score)
        
        # 生成建议
        if final_score >= 75:
            suggestion = "强烈买入"
            logic = "多项指标优秀，趋势强劲"
        elif final_score >= 60:
            suggestion = "买入"
            logic = "整体向好，可以配置"
        elif final_score >= 45:
            suggestion = "观察"
            logic = "震荡整理，等待机会"
        else:
            suggestion = "回避"
            logic = "趋势疲弱，规避风险"
        
        # 长期和短期趋势
        long_term = "多头" if last['Close'] > last.get('MA200', 0) and final_score >= 75 else \
                    "震荡" if 60 <= final_score < 75 else "空头"
        
        short_term = "强势" if last['Close'] > last.get('MA20', 0) and final_score >= 75 else \
                     "中性" if 60 <= final_score < 75 else "弱势"
        
        # 【V87.15】实战修正：高分但风险过高的股票降级
        # 计算止损距离
        if len(df) >= 20:
            low_20d = df['Low'].tail(20).min()
            risk_pct = (last['Close'] - low_20d) / last['Close'] * 100
            risk_reward = (last['Close'] * 1.15 - last['Close']) / (last['Close'] - low_20d) if (last['Close'] - low_20d) > 0 else 0
            
            # 如果止损距离>20%或盈亏比<1.2，降级到74分
            if final_score >= 75 and (risk_pct > 20 or risk_reward < 1.2):
                final_score = 74
                suggestion = "观察"
                logic = "技术面良好但风险收益比不佳，建议等待更好入场点"
                logging.info(f"实战修正: {code} 原评分降级（风险过高）")
        
        return {
            'score': final_score,
            'suggestion': suggestion,
            'logic': logic,
            'long_term': long_term,
            'short_term': short_term,
            'rsi': last['RSI'],
            'last': last,
            'df': df,
            'code': code,
        }
    
    except Exception as e:
        logging.error(f"❌ {code} 指标计算失败: {type(e).__name__}: {str(e)[:100]}")
        return None


def get_position_level_unified(df: pd.DataFrame, last_close: float) -> Tuple[str, float]:
    """
    统一水位计算（强校验字段）
    由后端统一计算，禁止前端自行推断

    Args:
        df: 日线数据
        last_close: 最新收盘价

    Returns:
        (position_level, position_percentile)
        position_level: "高"|"中"|"低"
        position_percentile: 0~100
    """
    if df is None or len(df) < 5:
        return ("N/A", 0.0)

    n = min(250, len(df))
    low_n = df["Low"].tail(n).min()
    high_n = df["High"].tail(n).max()

    if high_n <= low_n:
        return ("N/A", 50.0)

    percentile = (last_close - low_n) / (high_n - low_n) * 100
    percentile = max(0, min(100, percentile))

    if percentile >= 75:
        level = "高"
    elif percentile >= 35:
        level = "中"
    else:
        level = "低"

    return (level, round(percentile, 1))


# ═══════════════════════════════════════════════════════════════
# 高级量化指标
# ═══════════════════════════════════════════════════════════════

def calculate_advanced_quant(df: pd.DataFrame) -> Dict[str, Any]:
    """
    计算高级量化指标
    
    包括：
    - 夏普比率
    - 最大回撤
    - 胜率
    - 盈亏比
    - MACD
    - 布林带
    
    Args:
        df: 股票数据DataFrame
        
    Returns:
        包含量化指标的字典
    """
    result = {
        'sharpe': 'N/A',
        'max_dd': 'N/A',
        'win_rate': 'N/A',
        'pl_ratio': 'N/A',
        'macd': {},
        'bollinger': {},
    }
    
    try:
        if df is None or len(df) < 20:
            return result
        
        # 计算收益率
        returns = df['Close'].pct_change().dropna()
        
        # 夏普比率
        if len(returns) > 0:
            sharpe = (returns.mean() * 252 - RISK_FREE_RATE) / (returns.std() * np.sqrt(252))
            result['sharpe'] = f"{sharpe:.2f}"
        
        # 最大回撤
        cumulative = (1 + returns).cumprod()
        running_max = cumulative.expanding().max()
        drawdown = (cumulative - running_max) / running_max
        max_dd = drawdown.min()
        result['max_dd'] = f"{max_dd*100:.2f}%"
        
        # 胜率和盈亏比
        wins = returns[returns > 0]
        losses = returns[returns < 0]
        
        if len(returns) > 0:
            win_rate = len(wins) / len(returns)
            result['win_rate'] = f"{win_rate*100:.1f}%"
        
        if len(wins) > 0 and len(losses) > 0:
            avg_win = wins.mean()
            avg_loss = abs(losses.mean())
            pl_ratio = avg_win / avg_loss if avg_loss > 0 else 0
            result['pl_ratio'] = f"{pl_ratio:.2f}"
        
        # MACD指标
        if len(df) >= MACD_SLOW:
            exp1 = df['Close'].ewm(span=MACD_FAST, adjust=False).mean()
            exp2 = df['Close'].ewm(span=MACD_SLOW, adjust=False).mean()
            macd = exp1 - exp2
            signal = macd.ewm(span=MACD_SIGNAL, adjust=False).mean()
            histogram = macd - signal
            
            # 判断金叉/死叉
            if len(macd) >= 2:
                macd_cross = "金叉" if macd.iloc[-1] > signal.iloc[-1] and macd.iloc[-2] <= signal.iloc[-2] else \
                             "死叉" if macd.iloc[-1] < signal.iloc[-1] and macd.iloc[-2] >= signal.iloc[-2] else "无"
                
                result['macd'] = {
                    'value': f"{macd.iloc[-1]:.2f}",
                    'signal': f"{signal.iloc[-1]:.2f}",
                    'histogram': f"{histogram.iloc[-1]:.2f}",
                    'cross': macd_cross,
                }
        
        # 布林带
        if len(df) >= BOLLINGER_WINDOW:
            sma = df['Close'].rolling(window=BOLLINGER_WINDOW).mean()
            std = df['Close'].rolling(window=BOLLINGER_WINDOW).std()
            upper = sma + (std * BOLLINGER_STD)
            lower = sma - (std * BOLLINGER_STD)
            
            last_close = df['Close'].iloc[-1]
            last_upper = upper.iloc[-1]
            last_lower = lower.iloc[-1]
            last_sma = sma.iloc[-1]
            
            # 判断位置
            if last_close > last_upper:
                position = "超买"
            elif last_close < last_lower:
                position = "超卖"
            elif last_close > last_sma:
                position = "强势"
            else:
                position = "弱势"
            
            result['bollinger'] = {
                'upper': f"{last_upper:.2f}",
                'middle': f"{last_sma:.2f}",
                'lower': f"{last_lower:.2f}",
                'position': position,
            }
    
    except Exception as e:
        logging.error(f"高级量化指标计算失败: {type(e).__name__}")
    
    return result


# ═══════════════════════════════════════════════════════════════
# 风险指标
# ═══════════════════════════════════════════════════════════════

def calculate_risk_metrics(df: pd.DataFrame, stock_code: str) -> Dict[str, Any]:
    """
    计算风险指标（Beta, Alpha, Correlation, Volatility）
    
    Args:
        df: 股票数据DataFrame
        stock_code: 股票代码
        
    Returns:
        包含风险指标的字典
    """
    result = {
        'beta': 'N/A',
        'alpha': 'N/A',
        'correlation': 'N/A',
        'volatility': 'N/A',
    }
    
    try:
        if df is None or len(df) < 60:
            return result
        
        # 获取基准指数（简化版，实际需要获取真实数据）
        benchmark_symbol = get_benchmark_symbol(stock_code)
        
        # 计算收益率
        stock_returns = df['Close'].pct_change().dropna()
        
        # 波动率
        volatility = stock_returns.std() * np.sqrt(252)
        result['volatility'] = f"{volatility*100:.2f}%"
        
        # 注：Beta和Alpha需要基准指数数据，这里提供框架
        # 实际使用时需要从data_fetch模块获取基准数据
        
        result['beta'] = "1.0"  # 占位
        result['alpha'] = "0.0%"  # 占位
        result['correlation'] = "0.8"  # 占位
    
    except Exception as e:
        logging.error(f"风险指标计算失败: {type(e).__name__}")
    
    return result


# ═══════════════════════════════════════════════════════════════
# 交易计划
# ═══════════════════════════════════════════════════════════════

def calculate_trade_plan(df: pd.DataFrame, code: str) -> Optional[Dict[str, Any]]:
    """
    机构式交易计划
    
    包括：
    - 入场区间
    - 止损位
    - 止盈位
    - 风险预算
    - 仓位建议
    
    Args:
        df: 股票数据DataFrame
        code: 股票代码
        
    Returns:
        交易计划字典，失败返回None
    """
    try:
        if df is None or len(df) < 20:
            return None
        
        current_price = df['Close'].iloc[-1]
        
        # 计算支撑位（20日最低价）
        low_20d = df['Low'].tail(20).min()
        
        # 入场区间（当前价 ± 2%）
        entry_low = current_price * 0.98
        entry_high = current_price * 1.02
        
        # 止损位（支撑位下方1%）
        stop_loss = low_20d * 0.99
        
        # 止盈位（1.5倍风险和2倍风险）
        risk = current_price - stop_loss
        take_profit_15r = current_price + risk * 1.5
        take_profit_2r = current_price + risk * 2
        
        # 盈亏比
        risk_reward_ratio = (take_profit_15r - current_price) / risk if risk > 0 else 0
        
        # 风险预算仓位
        risk_amount = TOTAL_EQUITY * RISK_BUDGET_PCT
        max_position = int(risk_amount / risk) if risk > 0 else 0
        position_value = max_position * current_price
        
        return {
            'entry_low': entry_low,
            'entry_high': entry_high,
            'entry_mid': (entry_low + entry_high) / 2,
            'stop_loss': stop_loss,
            'take_profit_15r': take_profit_15r,
            'take_profit_2r': take_profit_2r,
            'risk_per_share': risk,
            'reward_15r': take_profit_15r - current_price,
            'reward_2r': take_profit_2r - current_price,
            'risk_reward_ratio': risk_reward_ratio,
            'current_price': current_price,
            'max_position': max_position,
            'position_value': position_value,
            'risk_budget_pct': RISK_BUDGET_PCT * 100,
        }
    
    except Exception as e:
        logging.error(f"交易计划计算失败: {type(e).__name__}")
        return None


# ═══════════════════════════════════════════════════════════════
# 蒙特卡洛预测
# ═══════════════════════════════════════════════════════════════

def monte_carlo_forecast(df: pd.DataFrame, days: int = 30, simulations: int = 1000) -> Dict[str, Any]:
    """
    蒙特卡洛模拟预测
    
    Args:
        df: 股票数据DataFrame
        days: 预测天数
        simulations: 模拟次数
        
    Returns:
        预测结果字典
    """
    result = {
        'mean': 'N/A',
        'median': 'N/A',
        'percentile_5': 'N/A',
        'percentile_95': 'N/A',
        'success': False,
    }
    
    try:
        if df is None or len(df) < 30:
            return result
        
        # 计算历史收益率和波动率
        returns = df['Close'].pct_change().dropna()
        mean_return = returns.mean()
        std_return = returns.std()
        
        # 当前价格
        last_price = df['Close'].iloc[-1]
        
        # 蒙特卡洛模拟
        simulation_results = []
        
        for _ in range(simulations):
            price = last_price
            for _ in range(days):
                # 生成随机收益率
                daily_return = np.random.normal(mean_return, std_return)
                price *= (1 + daily_return)
            simulation_results.append(price)
        
        # 统计结果
        simulation_results = np.array(simulation_results)
        
        result = {
            'mean': f"{simulation_results.mean():.2f}",
            'median': f"{np.median(simulation_results):.2f}",
            'percentile_5': f"{np.percentile(simulation_results, 5):.2f}",
            'percentile_95': f"{np.percentile(simulation_results, 95):.2f}",
            'success': True,
        }
    
    except Exception as e:
        logging.error(f"蒙特卡洛预测失败: {type(e).__name__}")
    
    return result


# ═══════════════════════════════════════════════════════════════
# 批量扫描分析
# ═══════════════════════════════════════════════════════════════

def batch_scan_analysis(
    pool: List[Tuple[str, str, str]],
    scan_type: str = "TOP",
    ma_target: Optional[int] = None,
    progress_callback: Optional[callable] = None
) -> Tuple[List[Dict], Dict]:
    """
    批量扫描股票池（完整版，从app.py迁移）
    
    Args:
        pool: 股票池 [(code, name, yf_code), ...]
        scan_type: 扫描类型（TOP/MA_TOUCH/SAFE_ZONE）
        ma_target: 均线目标（30/60/120）
        progress_callback: 进度回调函数
        
    Returns:
        (results, stats) 元组
    """
    from .data_fetch import fetch_stock_data
    import time
    
    results = []
    stats = {
        'success': 0,
        'failed': 0,
        'errors': []
    }
    
    total_stocks = len(pool)
    
    # 【V91.7】使用统一行业映射模块（sector_map.py），全682只覆盖
    from .sector_map import get_sector
    
    for idx, item in enumerate(pool):
        # 进度回调
        if progress_callback:
            progress_callback(idx + 1, total_stocks, item[1] if len(item) > 1 else item[0])
        
        try:
            code = item[0]
            name = item[1]
            c_fixed = item[2] if len(item) >= 3 else code
            
            # 获取数据
            df = fetch_stock_data(c_fixed)
            
            if df is None or df.empty:
                stats['failed'] += 1
                stats['errors'].append({
                    'code': code,
                    'name': name,
                    'error': '数据获取失败'
                })
                continue
            
            # 计算指标
            m = calculate_metrics_all(df, c_fixed)
            
            if m is None:
                stats['failed'] += 1
                continue
            
            # 根据扫描类型筛选
            should_include = False
            
            if scan_type == "TOP":
                should_include = m['score'] >= 35  # Top扫描：评分>35
            elif scan_type == "MA30" and ma_target == 30:
                # MA30扫描：价格靠近MA30
                last_close = m['last']['Close']
                ma30 = m['df']['MA30'].iloc[-1] if 'MA30' in m['df'].columns else 0
                if ma30 > 0:
                    distance = abs(last_close - ma30) / ma30
                    should_include = distance < 0.05  # 5%以内
            
            if should_include:
                results.append({
                    '股票': name,
                    '代码': code,
                    '得分': m['score'],
                    '行业': get_sector(code, name),  # 【V90.9】板块→行业
                    '建议': m['suggestion'],
                })
                stats['success'] += 1
        
        except Exception as e:
            stats['failed'] += 1
            stats['errors'].append({
                'code': item[0] if item else 'Unknown',
                'name': item[1] if len(item) > 1 else 'Unknown',
                'error': f"{type(e).__name__}: {str(e)[:80]}"
            })
    
    return results, stats


def batch_scan_analysis_concurrent(
    pool: List[Tuple[str, str, str]],
    scan_type: str = "TOP",
    ma_target: Optional[int] = None,
    progress_callback: Optional[callable] = None,
    max_workers: int = 6,
    cancel_flag: Optional[Dict] = None
) -> Tuple[List[Dict], Dict]:
    """
    【V88 Phase 2】并发批量扫描股票池（ThreadPoolExecutor）
    
    Args:
        pool: 股票池 [(code, name, yf_code), ...]
        scan_type: 扫描类型（TOP/MA_TOUCH/SAFE_ZONE）
        ma_target: 均线目标（30/60/120）
        progress_callback: 进度回调函数
        max_workers: 最大并发线程数
        cancel_flag: 取消标志 {'cancel': bool}
        
    Returns:
        (results, stats) 元组
    """
    from .data_fetch import fetch_stock_data
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
    import threading
    
    results = []
    stats = {
        'success': 0,
        'failed': 0,
        'errors': [],
        'cancelled': False
    }
    
    total_stocks = len(pool)
    completed_count = 0
    lock = threading.Lock()
    
    # 【V91.7】使用统一行业映射模块（sector_map.py），全682只覆盖
    from .sector_map import get_sector
    
    def _analyze_single_stock(item):
        """单只股票分析任务（线程安全）"""
        try:
            code = item[0]
            name = item[1]
            c_fixed = item[2] if len(item) >= 3 else code
            
            # 获取数据
            df = fetch_stock_data(c_fixed)
            
            if df is None or df.empty:
                return None, {
                    'code': code,
                    'name': name,
                    'error': '数据获取失败'
                }
            
            # 计算指标
            m = calculate_metrics_all(df, c_fixed)
            
            if m is None:
                return None, {
                    'code': code,
                    'name': name,
                    'error': '指标计算失败'
                }
            
            # 根据扫描类型筛选
            should_include = False
            
            if scan_type == "TOP":
                should_include = m['score'] >= 35
            elif scan_type == "MA30" and ma_target == 30:
                last_close = m['last']['Close']
                ma30 = m['df']['MA30'].iloc[-1] if 'MA30' in m['df'].columns else 0
                if ma30 > 0:
                    distance = abs(last_close - ma30) / ma30
                    should_include = distance < 0.05
            
            if should_include:
                return {
                    '股票': name,
                    '代码': code,
                    '得分': m['score'],
                    '行业': get_sector(code, name),  # 【V90.9】板块→行业
                    '建议': m['suggestion'],
                }, None
            else:
                return None, None  # 不满足条件，非错误
        
        except Exception as e:
            return None, {
                'code': item[0] if item else 'Unknown',
                'name': item[1] if len(item) > 1 else 'Unknown',
                'error': f"{type(e).__name__}: {str(e)[:80]}"
            }
    
    # 使用线程池并发执行
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_item = {}
        for item in pool:
            future = executor.submit(_analyze_single_stock, item)
            future_to_item[future] = item
        
        # 收集结果
        for future in as_completed(future_to_item):
            # 检查取消标志
            if cancel_flag and cancel_flag.get('cancel', False):
                stats['cancelled'] = True
                executor.shutdown(wait=False, cancel_futures=True)
                break
            
            item = future_to_item[future]
            
            with lock:
                completed_count += 1
                if progress_callback:
                    progress_callback(
                        completed_count,
                        total_stocks,
                        item[1] if len(item) > 1 else item[0]
                    )
            
            try:
                result, error = future.result(timeout=30)
                
                if error:
                    with lock:
                        stats['failed'] += 1
                        stats['errors'].append(error)
                elif result:
                    with lock:
                        results.append(result)
                        stats['success'] += 1
                else:
                    # 不满足条件，计入failed
                    with lock:
                        stats['failed'] += 1
            except Exception as e:
                with lock:
                    stats['failed'] += 1
                    stats['errors'].append({
                        'code': item[0] if item else 'Unknown',
                        'name': item[1] if len(item) > 1 else 'Unknown',
                        'error': f"Future异常: {type(e).__name__}: {str(e)[:80]}"
                    })
    
    return results, stats
