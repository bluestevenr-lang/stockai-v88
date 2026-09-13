"""Free market-data adapter for desktop/chart workers; no paid-provider route."""
from v88_paths import core_root
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import logging
import sys
import numpy as np
import pandas as pd

CORE = core_root()
log = logging.getLogger(__name__)
COLUMNS = ['Open', 'High', 'Low', 'Close', 'Volume']
INDEX_CODES = {'000001.SS', '000300.SS', '000016.SS', '000905.SS',
               '399001.SZ', '399006.SZ', '399300.SZ', '399400.SZ', '000852.SS', '000688.SS'}


def _core():
    src = str(CORE/'src')
    if src not in sys.path:
        sys.path.insert(0, src)


def is_cn(code):
    return str(code).endswith(('.SS', '.SH', '.SZ', '.BJ'))


def is_index(code):
    return code in INDEX_CODES


def validate(frame, code, *, source):
    if frame is None or len(frame) < 5 or any(k not in frame for k in COLUMNS):
        raise ValueError('完整行情不足5根')
    out = frame[COLUMNS].astype(float).copy()
    out.index = pd.to_datetime(out.index)
    if not out.index.is_unique or not out.index.is_monotonic_increasing:
        raise ValueError('行情日期重复或无序')
    if not np.isfinite(out.to_numpy()).all() or (out[COLUMNS[:-1]] <= 0).any().any() or (out.Volume < 0).any():
        raise ValueError('行情数值无效')
    if ((out.Low > out[['Open', 'Close']].min(axis=1)) | (out.High < out[['Open', 'Close']].max(axis=1))).any():
        raise ValueError('行情高低价不包围开收盘')
    tz = ZoneInfo('Asia/Shanghai') if is_cn(code) or code.endswith('.HK') else ZoneInfo('America/New_York')
    now = datetime.now(tz)
    if out.index[-1].date() > now.date():
        raise ValueError('行情来自未来日期')
    _core()
    from history_calendar import latest_completed
    market = 'A股' if is_cn(code) else '港股' if code.endswith('.HK') else '美股'
    expected = latest_completed(market, now)
    out = out[[d.date() <= expected for d in out.index]]
    if len(out) < 5 or out.index[-1].date() != expected:
        raise ValueError('缺少最近已完成交易日行情')
    last_day = out.index[-1].date()
    out.attrs.update(frame.attrs)
    out.attrs.update(source=source, source_asof=last_day.isoformat(), is_realtime=False)
    return out


def clip_calendar(frame, days, code):
    tz = ZoneInfo('America/New_York') if not is_cn(code) and not code.endswith('.HK') else ZoneInfo('Asia/Shanghai')
    end = pd.Timestamp(datetime.now(tz).date())
    begin = end - pd.Timedelta(days=days)
    result = frame.copy()
    if result.index.tz is not None:
        result.index = result.index.tz_localize(None)
    result = result.loc[result.index >= begin]
    if result.empty:
        raise ValueError('请求历史区间没有行情')
    _core()
    from history_calendar import coverage
    market = 'A股' if is_cn(code) else '港股' if code.endswith('.HK') else '美股'
    checked = coverage([x.date().isoformat() for x in result.index], market, begin.date().isoformat(), result.index[-1].date().isoformat())
    if not checked['continuity_ok']:
        raise ValueError('请求历史区间未覆盖：缺交易日或出现非交易日')
    result.attrs['history_coverage'] = checked
    result.attrs.update(frame.attrs, requested_calendar_days=days, requested_start=begin.date().isoformat())
    return result


def fetch_daily_free(code, days=400):
    if not is_cn(code):
        return None
    try:
        _core()
        from market_symbols import canonical
        code = canonical(code)
        from verified_history import read as read_verified
        try:
            cached = read_verified(code, (datetime.now() - timedelta(days=days * 2 + 30)).date().isoformat(), datetime.now().date().isoformat())
        except Exception:
            cached = None
        if cached is not None and not cached.empty:
            frame = cached.rename(columns={c: c.title() for c in cached.columns})
            try:
                return clip_calendar(validate(frame, code, source=frame.attrs.get('provider_source') or '同源核验日线'), days, code)
            except ValueError:
                pass
        if is_index(code):
            return None  # Equity endpoint must not impersonate an index endpoint.
        from tencent_history import fetch
        from verified_history import chart_rows
        chart, _ = fetch(code)
        rows = chart_rows(code, chart)
        frame = pd.DataFrame(rows, columns=['code','date','Open','Close','High','Low','Volume','amount']).set_index('date')
        frame.attrs['price_basis'] = chart['chart']['result'][0]['meta']['v88_basis']
        frame.attrs['volume_unit'] = 'shares'
        return clip_calendar(validate(frame, code, source='腾讯整段前复权日线'), days, code)
    except Exception as exc:
        log.warning('免费A股日线不可用 %s: %s', code, type(exc).__name__)
        return None


def clip_sessions(frame, n, code):
    _core()
    from history_calendar import coverage
    end = frame.index[-1].date()
    market = 'A股' if is_cn(code) else '港股' if code.endswith('.HK') else '美股'
    expected = coverage([], market, (end - timedelta(days=n*4+30)).isoformat(), end.isoformat())['missing_sessions'][-n:]
    result = frame.tail(n)
    actual = [x.date().isoformat() for x in result.index]
    if len(expected) != n or actual != expected:
        raise ValueError('最近请求的交易日有缺口，不能用更早日线补足条数')
    result.attrs.update(frame.attrs, requested_sessions=n)
    return result


def fetch_df(code, period='1y', timeout=10):
    tz = ZoneInfo('America/New_York') if not is_cn(code) and not code.endswith('.HK') else ZoneInfo('Asia/Shanghai')
    end = pd.Timestamp(datetime.now(tz).date())
    import re
    span = re.fullmatch(r'(\d+)(mo|y|d)', period)
    if not span:
        return None
    n, unit = int(span[1]), span[2]
    begin = end - (pd.DateOffset(months=n) if unit == 'mo' else pd.DateOffset(years=n) if unit == 'y' else pd.Timedelta(days=n))
    days = (end - begin).days
    if is_cn(code):
        frame = fetch_daily_free(code, max(30, days))
        if frame is not None:
            if period in ('1d', '5d'):
                try:
                    frame = clip_sessions(frame, int(period[:-1]), code)
                except ValueError:
                    return None
            return frame
    try:
        import yfinance as yf
        frame = yf.Ticker(code).history(period='1mo' if period in ('1d', '5d') else period, timeout=timeout)
        if frame is not None and isinstance(frame.columns, pd.MultiIndex):
            frame.columns = frame.columns.get_level_values(0)
        frame = validate(frame, code, source='Yahoo完整日线')
        return clip_sessions(frame, int(period[:-1]), code) if period in ('1d', '5d') else clip_calendar(frame, days, code)
    except Exception as exc:
        log.warning('免费日线不可用 %s: %s', code, type(exc).__name__)
        return None


def fetch_latest_price(code):
    frame = fetch_df(code, period='1mo')
    if frame is None or len(frame) < 2:
        return None
    price, previous = float(frame.Close.iloc[-1]), float(frame.Close.iloc[-2])
    return {'price':price, 'prev_close':previous, 'change_pct':(price/previous-1)*100,
            'source':frame.attrs.get('source'), 'source_asof':frame.attrs.get('source_asof'),
            'is_realtime':False, 'price_type':'最近日线收盘参考'}


def fetch_cn_top_pool(limit=500):
    try:
        _core()
        from free_market_data import code_for, number, source_status
        from market_symbols import canonical
        root = CORE/'data/free_market_data'
        doc = json.loads((root/'cn.json').read_text(encoding='utf-8'))
        if not doc.get('pagination_complete'):
            raise ValueError('报价分页未完成')
        listed = set()
        for name in ['directory_sse.json','directory_star.json','directory_szse.json','directory_bse.json']:
            path = root/name
            if not path.exists():
                raise ValueError('缺少交易所上市名录，不能输出全市场排序')
            directory = json.loads(path.read_text(encoding='utf-8'))
            if directory.get('status') not in ('ok', 'downloaded') or not directory.get('codes'):
                raise ValueError('交易所名录不完整')
            at = datetime.fromisoformat(str(directory.get('fetched_at')).replace('Z', '+00:00'))
            age = (datetime.now(at.tzinfo).date() - at.date()).days
            if not 0 <= age <= 7:
                raise ValueError('交易所名录日期不适用')
            catalog = directory.get('catalog_asof')
            if catalog and catalog != 'not_supplied':
                catalog_at = datetime.fromisoformat(str(catalog)).date()
                if not 0 <= (datetime.now().date() - catalog_at).days <= 7:
                    raise ValueError('交易所名录标注日期不适用')
            listed.update(canonical(c) for c in directory['codes'])
        candidates = {}
        quoted = {canonical(code_for('A股', r)): r for r in doc.get('rows') or []}
        if listed - set(quoted):
            raise ValueError('名录证券缺少报价，不能输出无缺口的全市场排名')
        for code in listed:
            raw = quoted[code]
            name = str(raw.get('f14') or '')
            cap = number(raw.get('f20'))
            if not name or cap is None or cap <= 0 or source_status(raw.get('f124')) != 'within_age_limit':
                raise ValueError('名录证券缺市值或有效时间，不能确认全市场排序')
            if name.upper().startswith(('ST','*ST','S*ST','SST','PT')) or '退' in name:
                continue
            candidates[code] = (cap, (code[:6], name, code))
        ranked = sorted(candidates.values(), key=lambda r:(-r[0],r[1][2]))
        return [r[1] for r in ranked[:max(0,int(limit))]]
    except Exception as exc:
        log.warning('免费A股股票池不完整: %s', type(exc).__name__)
        return []


def fetch_cn_stock_pool(limit=300):
    return fetch_cn_top_pool(limit)
