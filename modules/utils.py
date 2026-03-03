"""
工具模块 - 通用工具函数
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import logging
import traceback
from typing import Any, Callable, Optional, TypeVar, Union
from datetime import datetime, timedelta
from functools import wraps

T = TypeVar('T')


# ═══════════════════════════════════════════════════════════════
# 异常处理装饰器
# ═══════════════════════════════════════════════════════════════

def safe_execute(
    default_return: Any = None,
    log_error: bool = True,
    error_message: str = "执行失败"
) -> Callable:
    """
    安全执行装饰器：捕获异常并返回默认值
    
    Args:
        default_return: 异常时返回的默认值
        log_error: 是否记录错误日志
        error_message: 错误消息前缀
        
    Returns:
        装饰器函数
    """
    def decorator(func: Callable[..., T]) -> Callable[..., Union[T, Any]]:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if log_error:
                    logging.error(f"{error_message} - {func.__name__}: {type(e).__name__}: {str(e)[:200]}")
                    logging.debug(traceback.format_exc())
                return default_return
        return wrapper
    return decorator


def handle_data_error(
    error: Exception,
    context: str = "",
    show_traceback: bool = False
) -> str:
    """
    统一的数据错误处理
    
    Args:
        error: 异常对象
        context: 上下文信息
        show_traceback: 是否显示堆栈跟踪
        
    Returns:
        格式化的错误消息
    """
    error_type = type(error).__name__
    error_msg = str(error)[:200]
    
    message = f"❌ {context}: {error_type}"
    if error_msg:
        message += f" - {error_msg}"
    
    logging.error(message)
    
    if show_traceback:
        logging.debug(traceback.format_exc())
    
    return message


# ═══════════════════════════════════════════════════════════════
# 日期时间工具
# ═══════════════════════════════════════════════════════════════

def format_datetime(dt: datetime, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    格式化日期时间
    
    Args:
        dt: 日期时间对象
        fmt: 格式字符串
        
    Returns:
        格式化的字符串
    """
    try:
        return dt.strftime(fmt)
    except:
        return str(dt)


def get_trading_days_ago(days: int) -> datetime:
    """
    获取N个交易日前的日期（简化版，不考虑节假日）
    
    Args:
        days: 天数
        
    Returns:
        日期时间对象
    """
    return datetime.now() - timedelta(days=days)


def is_trading_time() -> bool:
    """
    判断当前是否为交易时间（简化版）
    
    Returns:
        是否为交易时间
    """
    now = datetime.now()
    hour = now.hour
    weekday = now.weekday()
    
    # 周一到周五
    if weekday >= 5:
        return False
    
    # 9:30 - 15:00（简化）
    if 9 <= hour < 15:
        return True
    
    return False


# ═══════════════════════════════════════════════════════════════
# 数据验证工具
# ═══════════════════════════════════════════════════════════════

def validate_dataframe(df, required_columns: list, min_length: int = 5) -> tuple[bool, str]:
    """
    验证DataFrame的有效性
    
    Args:
        df: DataFrame对象
        required_columns: 必需的列名列表
        min_length: 最小行数
        
    Returns:
        (是否有效, 错误消息)
    """
    if df is None:
        return False, "DataFrame为None"
    
    if df.empty:
        return False, "DataFrame为空"
    
    if len(df) < min_length:
        return False, f"数据点不足（{len(df)} < {min_length}）"
    
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        return False, f"缺少必需列: {', '.join(missing_cols)}"
    
    # 检查价格列是否有效
    for col in ['Open', 'High', 'Low', 'Close']:
        if col in df.columns:
            if (df[col] <= 0).any():
                return False, f"{col}列包含无效值（<=0）"
    
    # 检查成交量
    if 'Volume' in df.columns:
        if (df['Volume'] < 0).any():
            return False, "成交量包含负值"
    
    return True, ""


def clean_dataframe(df):
    """
    清理DataFrame（去除异常值、填充缺失值）
    
    Args:
        df: DataFrame对象
        
    Returns:
        清理后的DataFrame
    """
    if df is None or df.empty:
        return df
    
    # 去除重复索引
    df = df[~df.index.duplicated(keep='first')]
    
    # 填充缺失值（向前填充）
    df = df.fillna(method='ffill')
    
    # 去除仍然存在的NaN
    df = df.dropna()
    
    return df


# ═══════════════════════════════════════════════════════════════
# 格式化工具
# ═══════════════════════════════════════════════════════════════

def format_number(
    value: Union[int, float],
    precision: int = 2,
    percentage: bool = False,
    prefix: str = "",
    suffix: str = ""
) -> str:
    """
    格式化数字
    
    Args:
        value: 数值
        precision: 小数位数
        percentage: 是否为百分比
        prefix: 前缀
        suffix: 后缀
        
    Returns:
        格式化的字符串
    """
    try:
        if percentage:
            value = value * 100
            suffix = "%" + suffix
        
        formatted = f"{value:.{precision}f}"
        return f"{prefix}{formatted}{suffix}"
    
    except:
        return str(value)


def format_large_number(value: Union[int, float]) -> str:
    """
    格式化大数字（K, M, B）
    
    Args:
        value: 数值
        
    Returns:
        格式化的字符串
    """
    try:
        abs_value = abs(value)
        sign = "-" if value < 0 else ""
        
        if abs_value >= 1_000_000_000:
            return f"{sign}{abs_value/1_000_000_000:.2f}B"
        elif abs_value >= 1_000_000:
            return f"{sign}{abs_value/1_000_000:.2f}M"
        elif abs_value >= 1_000:
            return f"{sign}{abs_value/1_000:.2f}K"
        else:
            return f"{sign}{abs_value:.2f}"
    
    except:
        return str(value)


def truncate_string(s: str, max_length: int = 50, suffix: str = "...") -> str:
    """
    截断字符串
    
    Args:
        s: 字符串
        max_length: 最大长度
        suffix: 后缀
        
    Returns:
        截断后的字符串
    """
    if len(s) <= max_length:
        return s
    
    return s[:max_length - len(suffix)] + suffix


# ═══════════════════════════════════════════════════════════════
# 股票代码转换工具
# ═══════════════════════════════════════════════════════════════

def to_yf_cn_code(code: str) -> str:
    """
    转换股票代码为yfinance格式
    
    Args:
        code: 原始股票代码
        
    Returns:
        yfinance格式的代码
    
    Examples:
        00700 -> 0700.HK (港股)
        09992 -> 9992.HK (港股)
        600519 -> 600519.SS (A股沪市)
        000001 -> 000001.SZ (A股深市)
        AAPL -> AAPL (美股)
    """
    if not code:
        return code
    
    code = code.strip().upper()
    
    # 【V88修复】沪市.SH改为.SS（Yahoo Finance要求）
    if code.endswith(".SH"):
        return code[:-3] + ".SS"
    
    # 已经是yfinance格式
    if '.' in code:
        # 港股特殊处理：去除前导0
        if code.endswith('.HK'):
            base_code = code.split('.')[0]
            if len(base_code) == 5 and base_code.startswith('0'):
                # 09992.HK -> 9992.HK
                return base_code.lstrip('0') + '.HK'
        return code
    
    # 纯数字代码判断
    if code.isdigit():
        # 港股代码：5位数字（去掉最左边的一个0）
        if len(code) == 5:
            # 00700 -> 0700.HK
            # 02318 -> 2318.HK
            # 09988 -> 9988.HK
            hk_code = code[1:]  # 去掉第一个字符
            return f"{hk_code}.HK"
        
        # 港股代码：4位数字
        elif len(code) == 4:
            return f"{code}.HK"
        
        # A股代码（6位）
        elif len(code) == 6:
            if code.startswith('6') or code.startswith('5'):
                return f"{code}.SS"  # 沪市
            elif code.startswith('0') or code.startswith('3'):
                return f"{code}.SZ"  # 深市
    
    # 美股：字母开头
    if code and code[0].isalpha():
        return code
    
    return code


def parse_market_from_code(code: str) -> str:
    """
    从股票代码判断市场
    
    Args:
        code: 股票代码
        
    Returns:
        市场标识（US/HK/CN）
    """
    code = code.upper()
    
    if '.SS' in code or '.SZ' in code:
        return "CN"
    
    if '.HK' in code:
        return "HK"
    
    if code[0].isalpha():
        return "US"
    
    if code.startswith('6') or code.startswith('5') or code.startswith('0') or code.startswith('3'):
        return "CN"
    
    if len(code) in [4, 5] and code.isdigit():
        return "HK"
    
    return "UNKNOWN"


# ═══════════════════════════════════════════════════════════════
# 性能监控工具
# ═══════════════════════════════════════════════════════════════

import time
from contextlib import contextmanager


@contextmanager
def timer(name: str = "操作", log_level: int = logging.INFO):
    """
    计时器上下文管理器
    
    Args:
        name: 操作名称
        log_level: 日志级别
        
    Usage:
        with timer("数据获取"):
            fetch_data()
    """
    start_time = time.time()
    try:
        yield
    finally:
        elapsed = time.time() - start_time
        logging.log(log_level, f"{name}耗时: {elapsed:.2f}秒")


# ═══════════════════════════════════════════════════════════════
# 颜色工具
# ═══════════════════════════════════════════════════════════════

def get_trend_color(value: float, threshold_positive: float = 0, threshold_negative: float = 0) -> str:
    """
    根据数值获取趋势颜色
    
    Args:
        value: 数值
        threshold_positive: 正向阈值
        threshold_negative: 负向阈值
        
    Returns:
        颜色代码
    """
    if value > threshold_positive:
        return "#10b981"  # 绿色
    elif value < threshold_negative:
        return "#ef4444"  # 红色
    else:
        return "#6b7280"  # 灰色


def get_score_color(score: float) -> str:
    """
    根据评分获取颜色
    
    Args:
        score: 评分（0-100）
        
    Returns:
        颜色代码
    """
    if score >= 75:
        return "#10b981"  # 绿色
    elif score >= 60:
        return "#3b82f6"  # 蓝色
    elif score >= 45:
        return "#f59e0b"  # 橙色
    else:
        return "#ef4444"  # 红色
