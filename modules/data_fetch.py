"""
数据获取模块 - 多源数据获取（yfinance + Stooq + 东方财富）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
数据源优先级：
  1. 本地文件缓存（10分钟内有效）
  2. yfinance（主力）
  3. Stooq（美股/指数备用）
  4. 东方财富（A股备用）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import os
import time
import logging
import ssl
import urllib.request
import pandas as pd
import requests
from typing import Optional, Tuple, Dict, Any, Union
from contextlib import contextmanager

from .utils import to_yf_cn_code, validate_dataframe, clean_dataframe
from .cache import get_cache
from .config import (
    YFINANCE_MAX_RETRIES,
    YFINANCE_RETRY_DELAY,
    MIN_DATA_LENGTH,
    REQUIRED_COLUMNS,
    PROXY_HTTP,
    PROXY_HTTPS,
)

# 尝试导入yfinance
try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False
    logging.warning("yfinance未安装，数据获取功能受限")


# ═══════════════════════════════════════════════════════════════
# 代理上下文管理器
# ═══════════════════════════════════════════════════════════════

class ProxyContext:
    """
    代理上下文管理器
    
    Usage:
        with ProxyContext(proxy_url):
            # 在此代码块内使用代理
            data = fetch_data()
    """
    
    def __init__(self, proxy_url: Optional[str] = None):
        self.proxy_url = proxy_url
        self.old_env: Dict[str, Optional[str]] = {}
    
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


def get_proxy_url() -> str:
    """
    获取代理URL
    
    Returns:
        代理URL字符串
    """
    # 从session_state获取（Streamlit环境）
    try:
        import streamlit as st
        port = st.session_state.get('proxy_port', '1082')
        return f"http://127.0.0.1:{port}"
    except:
        # 非Streamlit环境，使用默认配置
        return PROXY_HTTP


# ═══════════════════════════════════════════════════════════════
# 数据清洗
# ═══════════════════════════════════════════════════════════════

def clean_df(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """
    清洗DataFrame数据
    
    Args:
        df: 原始DataFrame
        
    Returns:
        清洗后的DataFrame，如果无效则返回None
    """
    if df is None or df.empty:
        return None
    
    # 处理多级列名
    if isinstance(df.columns, pd.MultiIndex):
        try:
            df.columns = df.columns.get_level_values(0)
        except:
            pass
    
    # 标准化列名（首字母大写）
    df = df.rename(columns=lambda x: x.capitalize())
    
    # 标准化列名映射
    cols_map = {
        'Date': 'Date',
        'Open': 'Open',
        'High': 'High',
        'Low': 'Low',
        'Close': 'Close',
        'Volume': 'Volume'
    }
    df = df.rename(columns=cols_map)
    
    # 检查必需列
    needed = ['Open', 'High', 'Low', 'Close']
    if not all(c in df.columns for c in needed):
        return None
    
    # 转换为数值类型
    for c in needed:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    
    # 删除NaN行
    df = df.dropna()
    
    # 确保有Volume列
    if 'Volume' not in df.columns:
        df['Volume'] = 0
    
    return df


# ═══════════════════════════════════════════════════════════════
# 数据源：Stooq（美股/指数备用）
# ═══════════════════════════════════════════════════════════════

def fetch_from_stooq(symbol: str) -> Optional[pd.DataFrame]:
    """
    从Stooq获取数据（免费、无需API Key）
    
    适用于：美股、指数、ETF
    不适用：港股、A股
    
    Args:
        symbol: 股票代码
        
    Returns:
        DataFrame或None
    """
    try:
        # Stooq不支持港股和A股
        if symbol.endswith('.HK') or symbol.endswith('.SS') or symbol.endswith('.SZ'):
            return None
        
        # 转换格式：AAPL->aapl.us, ^VIX->vi.f(CBOE), DX-Y.NYB->dx.f
        _STOOQ_MAP = {
            'DX-Y.NYB': 'dx.f', 'CNY=X': 'cnyusd.fx', 'HKD=X': 'hkdusd.fx',
            '^VIX': 'vi.f', '^TNX': 'tnx.us', '^GSPC': 'sp500.us',
            'SPY': 'spy.us', 'TLT': 'tlt.us', 'GLD': 'gld.us',
            'QQQ': 'qqq.us', 'IWM': 'iwm.us',
        }
        if symbol in _STOOQ_MAP:
            stooq_symbol = _STOOQ_MAP[symbol]
        else:
            base = symbol.replace('^', '').replace('.US', '').replace('.', '').replace('-', '').replace('.NYB', '').replace('=', '')
            stooq_symbol = f"{base.lower()}.us"
        url = f"https://stooq.com/q/d/l/?s={stooq_symbol}&i=d"
        
        # 读取CSV：优先 requests（Cloud 环境 pd.read_csv 直连常失败）
        df = None
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (compatible; StockAI/1.0)'}
            r = requests.get(url, timeout=15, verify=False, headers=headers)
            if r.status_code == 200 and r.text.strip():
                from io import StringIO
                df = pd.read_csv(StringIO(r.text))
        except Exception:
            pass
        if df is None or df.empty:
            try:
                df = pd.read_csv(url, storage_options={'verify': False} if hasattr(pd, '__version__') and int(pd.__version__.split('.')[0]) >= 2 else {})
            except Exception:
                df = pd.read_csv(url)
        
        if df is None or df.empty or "Close" not in df.columns:
            return None
        
        # 处理日期索引
        df["Date"] = pd.to_datetime(df["Date"])
        df.set_index("Date", inplace=True)
        df = df.sort_index()
        
        return clean_df(df)
    
    except Exception as e:
        logging.debug(f"Stooq获取失败 {symbol}: {type(e).__name__}")
        return None


# ═══════════════════════════════════════════════════════════════
# 数据源：东方财富（港股指数备用，yfinance 在 Cloud 常失败）
# ═══════════════════════════════════════════════════════════════

def fetch_hk_index_from_eastmoney(symbol: str) -> Optional[pd.DataFrame]:
    """
    从东方财富获取港股指数数据（^HSI/^HSTECH/^HSCE 等）
    实测 secid: 100.HSI 恒生指数, 124.HSTECH 恒生科技, 100.HSCEI 国企指数
    """
    _EM_HK_MAP = {'^HSI': '100.HSI', '^HSTECH': '124.HSTECH', '^HSCE': '100.HSCEI'}
    secid = _EM_HK_MAP.get(symbol)
    if not secid:
        return None
    try:
        em_url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            'secid': secid,
            'fields1': 'f1,f2,f3,f4,f5,f6',
            'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58',
            'klt': '101',
            'fqt': '0',
            'end': '20500101',
            'lmt': '252'
        }
        r = requests.get(em_url, params=params, timeout=10, verify=False)
        if r.status_code != 200:
            return None
        data = r.json()
        if not data.get('data') or not data['data'].get('klines'):
            return None
        rows = []
        for line in data['data']['klines']:
            parts = line.split(',')
            if len(parts) >= 6:
                rows.append({
                    'Date': parts[0], 'Open': float(parts[1]), 'Close': float(parts[2]),
                    'High': float(parts[3]), 'Low': float(parts[4]), 'Volume': float(parts[5])
                })
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        return clean_df(df)
    except Exception as e:
        logging.debug(f"东财港股指数 {symbol} 失败: {type(e).__name__}")
        return None


# ═══════════════════════════════════════════════════════════════
# 数据源：东方财富（A股备用）
# ═══════════════════════════════════════════════════════════════

def fetch_from_eastmoney(code: str) -> Optional[pd.DataFrame]:
    """
    从东方财富获取A股数据（备用源）
    
    Args:
        code: yfinance格式的股票代码（如600000.SS）
        
    Returns:
        DataFrame或None
    """
    try:
        # 只处理A股
        if not (code.endswith('.SS') or code.endswith('.SZ')):
            return None
        
        # 转换为东财格式
        if code.endswith('.SS'):
            secid = f"1.{code.replace('.SS', '')}"
        else:
            secid = f"0.{code.replace('.SZ', '')}"
        
        # 【V91.7】创业板指399006等指数需用fqt=0（不复权），fqt=1对指数可能返回空
        is_index = code in ('399006.SZ', '000300.SS', '000001.SS', '399001.SZ')
        fqt_val = '0' if is_index else '1'
        
        # 东方财富K线接口
        em_url = f"https://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            'secid': secid,
            'fields1': 'f1,f2,f3,f4,f5,f6',
            'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58',
            'klt': '101',  # 日线
            'fqt': fqt_val,
            'end': '20500101',
            'lmt': '252'   # 最近252个交易日
        }
        
        response = requests.get(em_url, params=params, timeout=10, verify=False)
        
        if response.status_code != 200:
            return None
        
        data = response.json()
        if not data.get('data') or not data['data'].get('klines'):
            return None
        
        # 解析K线数据
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
        
        if not rows:
            return None
        
        # 构建DataFrame
        df = pd.DataFrame(rows)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        
        return df
    
    except Exception as e:
        logging.debug(f"东方财富获取失败 {code}: {type(e).__name__}")
        return None


# ═══════════════════════════════════════════════════════════════
# 主数据获取函数
# ═══════════════════════════════════════════════════════════════

def fetch_stock_data(
    code: str,
    return_source: bool = False,
    return_quality: bool = False,
    use_cache: bool = True
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, str], Tuple[pd.DataFrame, Dict], None]:
    """
    获取股票数据（多源回退 + 缓存）
    
    数据源优先级：
    1. 本地文件缓存（10分钟内有效）
    2. yfinance（主力）
    3. Stooq（美股/指数备用）
    4. 东方财富（A股备用）
    
    Args:
        code: 股票代码
        return_source: 是否返回数据源信息
        return_quality: 是否返回数据质量元数据
        use_cache: 是否使用缓存
        
    Returns:
        - return_quality=True: (df, data_quality_dict)
        - return_source=True: (df, source_str)
        - 默认: df
        - 失败返回None或(None, ...)
    """
    # 转换代码格式
    target_code = to_yf_cn_code(code)
    
    # 数据质量元数据
    data_quality: Dict[str, Any] = {
        'source': '无数据',
        'last_updated': None,
        'is_delayed': True,
        'data_points': 0,
        'date_range': None
    }
    
    # 尝试从缓存获取
    if use_cache:
        cache = get_cache()
        cache_key = f"stock_data_{target_code}_{return_source}_{return_quality}"
        cached_result = cache.get(cache_key)
        
        if cached_result is not None:
            logging.info(f"缓存命中: {code} -> {target_code}")
            return cached_result
    
    logging.info(f"数据获取: {code} -> {target_code}")
    proxy_url = get_proxy_url()
    data_source = "无数据"
    
    # ═══ 1️⃣ 主力：yfinance ═══
    if HAS_YFINANCE:
        param_combinations = [
            {"period": "1y", "auto_adjust": False},
            {"period": "2y", "auto_adjust": True},
            {"period": "6mo", "auto_adjust": False},
            {"period": "max", "auto_adjust": False},
        ]
        
        for idx, params in enumerate(param_combinations):
            for retry in range(YFINANCE_MAX_RETRIES):
                try:
                    with ProxyContext(proxy_url):
                        tk = yf.Ticker(target_code)
                        df = tk.history(**params, timeout=15)
                        cleaned = clean_df(df)
                        
                        if cleaned is not None and len(cleaned) >= MIN_DATA_LENGTH:
                            logging.info(f"✅ {target_code} YFinance成功 (参数{idx+1}, 重试{retry+1}/{YFINANCE_MAX_RETRIES})")
                            data_source = "yfinance"
                            
                            # 填充数据质量元数据
                            data_quality['source'] = 'Yahoo Finance'
                            data_quality['last_updated'] = pd.Timestamp.now()
                            data_quality['is_delayed'] = True
                            data_quality['data_points'] = len(cleaned)
                            data_quality['date_range'] = f"{cleaned.index[0].date()} 至 {cleaned.index[-1].date()}"
                            
                            # 保存到缓存
                            if use_cache:
                                if return_quality:
                                    result = (cleaned, data_quality)
                                elif return_source:
                                    result = (cleaned, data_source)
                                else:
                                    result = cleaned
                                
                                cache.set(cache_key, result)
                            
                            return result if (return_quality or return_source) else cleaned
                
                except Exception as e:
                    if retry < YFINANCE_MAX_RETRIES - 1:
                        # 指数退避重试
                        wait_time = YFINANCE_RETRY_DELAY * (2 ** retry)
                        logging.warning(f"⚠️ {target_code} YFinance失败 (参数{idx+1}, 重试{retry+1}/{YFINANCE_MAX_RETRIES}): {type(e).__name__}, 等待{wait_time}s")
                        time.sleep(wait_time)
                        continue
                    else:
                        logging.error(f"❌ {target_code} YFinance参数{idx+1}全部失败")
                        break
        
        logging.warning(f"{target_code} YFinance全部尝试失败，尝试备用源...")
    
    # ═══ 2️⃣ 备用：Stooq（仅美股/指数）═══
    if not target_code.endswith('.HK') and not target_code.endswith('.SS') and not target_code.endswith('.SZ'):
        logging.info(f"尝试Stooq备用源: {target_code}")
        df_stooq = fetch_from_stooq(target_code)
        
        if df_stooq is not None and len(df_stooq) >= MIN_DATA_LENGTH:
            logging.info(f"✅ {target_code} Stooq成功（备用源）")
            data_source = "stooq(备用)"
            
            # 填充数据质量元数据
            data_quality['source'] = 'Stooq (备用)'
            data_quality['last_updated'] = pd.Timestamp.now()
            data_quality['is_delayed'] = True
            data_quality['data_points'] = len(df_stooq)
            data_quality['date_range'] = f"{df_stooq.index[0].date()} 至 {df_stooq.index[-1].date()}"
            
            # 保存到缓存
            if use_cache:
                if return_quality:
                    result = (df_stooq, data_quality)
                elif return_source:
                    result = (df_stooq, data_source)
                else:
                    result = df_stooq
                
                cache.set(cache_key, result)
            
            return result if (return_quality or return_source) else df_stooq
    
    # ═══ 3️⃣ 备用：东方财富（仅A股）═══
    if target_code.endswith('.SS') or target_code.endswith('.SZ'):
        logging.info(f"尝试东方财富备用源: {target_code}")
        df_em = fetch_from_eastmoney(target_code)
        
        if df_em is not None and len(df_em) >= MIN_DATA_LENGTH:
            logging.info(f"✅ {target_code} 东方财富成功（备用源）")
            data_source = "eastmoney(备用)"
            
            # 填充数据质量元数据
            data_quality['source'] = '东方财富 (备用)'
            data_quality['last_updated'] = pd.Timestamp.now()
            data_quality['is_delayed'] = True
            data_quality['data_points'] = len(df_em)
            data_quality['date_range'] = f"{df_em.index[0].date()} 至 {df_em.index[-1].date()}"
            
            # 保存到缓存
            if use_cache:
                if return_quality:
                    result = (df_em, data_quality)
                elif return_source:
                    result = (df_em, data_source)
                else:
                    result = df_em
                
                cache.set(cache_key, result)
            
            return result if (return_quality or return_source) else df_em
    
    # ═══ 4️⃣ 所有源失败 ═══
    logging.error(f"❌ {target_code} 所有数据源失败")
    logging.error(f"   原始代码: {code}")
    logging.error(f"   转换后: {target_code}")
    
    # 提供具体建议
    if target_code.endswith('.HK'):
        logging.info("💡 港股建议:")
        logging.info("   1) 检查代码格式是否正确（应为4位数字.HK，如0700.HK）")
        logging.info("   2) 股票可能已退市或暂停交易")
        logging.info("   3) 尝试在Yahoo Finance网站搜索验证")
    elif target_code.endswith(('.SS', '.SZ')):
        logging.info("💡 A股建议:")
        logging.info("   1) 检查网络连接和代理设置")
        logging.info("   2) 股票可能停牌或退市")
        logging.info("   3) 验证代码格式（沪市.SS，深市.SZ）")
    else:
        logging.info("💡 美股建议:")
        logging.info("   1) 验证股票代码是否正确")
        logging.info("   2) 股票可能已退市")
        logging.info(f"   3) 尝试在Yahoo Finance搜索: https://finance.yahoo.com/quote/{target_code}")
    
    # 更新错误元数据
    data_quality['source'] = '无数据'
    data_quality['error_detail'] = '所有数据源均失败（yfinance/stooq/eastmoney）- 可能已退市或代码错误'
    
    if return_quality:
        return (None, data_quality)
    elif return_source:
        return (None, "无数据")
    else:
        return None
