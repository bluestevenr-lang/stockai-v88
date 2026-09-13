"""Compatibility names only. All data routes have moved to free sources."""
from market_data_helper import (fetch_df, fetch_latest_price, fetch_cn_stock_pool,
    fetch_cn_top_pool, fetch_daily_free, is_cn, is_index)
fetch_daily_tushare = fetch_daily_free

def get_pro():
    return None

def get_tushare_status():
    return {'available':False, 'status':'已停用；使用免费行情源，无到期重试或付费回退', 'fail_count':0}

def yf_to_ts(code):
    return code[:-3]+'.SH' if code.endswith('.SS') else code

def ts_to_yf(code):
    return code[:-3]+'.SS' if code.endswith('.SH') else code
