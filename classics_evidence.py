"""Pure, versioned observations from cached completed OHLCV and supplied facts.

No file/network/model access, company score, grade, order, price target or new
stop. All technical views belong to ONE price_volume evidence family. Numerical
thresholds are engineering conventions, not literal book quotations or a
validated predictive edge. Source authenticity remains the upstream loader's
responsibility; this module checks geometry, dates and session continuity.

API: evaluate(data, *, asof, market, now=None, code=None, source=None,
              basis=None, business=None, validated_stop=None).

Data: screen_history record with bars [date,open,close,high,low,volume]; verified
rows [code,date,open,close,high,low,volume,amount]; named OHLCV dictionaries; or a
DataFrame with date index and provider_source/price_basis attrs. Explicit source
and basis override metadata. Volume can be missing; it never defaults to one.

Business: optional evidence map with recurring_profit, operating_cash_flow,
pe_ttm, pb. Each item is {value, source, asof, period, currency}; common metadata
may be supplied at the top level. Profit/OCF comparisons require the same period
and currency. Raw net profit and cash/liability ratios are NOT recurring profit
or operating cash flow substitutes. Unknown PIT capture/publication is explicit.

validated_stop: {price, validated: True, source, asof, basis[, code]}. It must
already be validated upstream and share the daily series' price basis. Only its
signed close distance/ATR is reported; no stop is constructed or adjusted.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
import re
from statistics import fmean

from exchange_sessions import is_session, latest_completed
from market_symbols import canonical

VERSION = 'classics-evidence-v1-20261016'
MIN_BARS = 120
SHORT_EMAS = (3, 5, 8, 10, 12, 15)
LONG_EMAS = (30, 35, 40, 45, 50, 60)
MARKETS = {'CN':'A股', 'HK':'港股', 'US':'美股', '中国':'A股',
           'A股':'A股', '港股':'港股', '美股':'美股'}
ENGINEERING_RULES = {
    'minimum_completed_bars':120, 'ema_seed':'first close of last 120 complete sessions',
    'gmma_slope_sessions':5, 'flat_slope_pct':0.05,
    'ribbon_compression_ratio':0.75, 'ribbon_expansion_ratio':1.25,
    'hammer_lower_wick_body_min':2.0, 'hammer_upper_wick_body_max':0.5,
    'hammer_body_range_max':0.35, 'candle_context_sessions':5,
    'candle_support_resistance_lookback':20, 'context_atr_distance_max':1.0,
    'candle_confirmation':'next completed close beyond pattern high/low',
    'volume_baseline':'previous 20 complete sessions, excluding current bar',
    'divergence_windows':'last 5 mean volume versus preceding 15; 20-session price return',
    'pivot_left_bars':2, 'pivot_right_confirmation_bars':2,
    'atr14_method':'arithmetic mean of last 14 true ranges', 'near_stop_atr_max':1.0,
    'financial_max_age_days':210, 'valuation_max_age_days':7,
}


def _num(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        value = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def _day(value):
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    value = str(value)
    if len(value) != 10:
        # A daily index may be a midnight Timestamp, never use its clock as
        # quote freshness. Daily date semantics come from the source series.
        value = datetime.fromisoformat(value.replace('Z', '+00:00')).date().isoformat()
    return date.fromisoformat(value).isoformat()


def _clock(value):
    result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('timestamp requires timezone')
    return result


def _known(value):
    return isinstance(value,str) and value.strip().lower() not in {'','unknown','unverified','none','待核','待更新'}


def _period_end(value):
    """Accept explicit report ends and unambiguous annual/half/quarter labels."""
    text=str(value)
    try:return date.fromisoformat(text)
    except ValueError:pass
    match=re.fullmatch(r'(\d{4})-?(FY|H[12]|Q[1-4])',text.upper())
    if not match:raise ValueError('unknown report period')
    year=int(match[1]);part=match[2]
    month=12 if part=='FY' else int(part[1])*6 if part.startswith('H') else int(part[1])*3
    return date(year+1,1,1)-timedelta(days=1) if month==12 else date(year,month+1,1)-timedelta(days=1)


def _block(status, reason=None, family='price_volume', **facts):
    return {'status':status, 'family':family, **({'reason':reason} if reason else {}), **facts}


def _unpack(data):
    metadata = {}
    if hasattr(data, 'iterrows') and hasattr(data, 'attrs'):
        metadata = dict(data.attrs)
        rows = []
        for index, item in data.iterrows():
            row = {str(k).lower():v for k,v in item.items()}
            row.setdefault('date', index)
            rows.append(row)
        return rows, metadata
    if isinstance(data, dict):
        metadata = {k:v for k,v in data.items() if k not in {'bars','rows'}}
        data = data.get('bars', data.get('rows', []))
    return list(data or []), metadata


def _normalize(data, asof, code):
    raw, metadata = _unpack(data)
    rows = []; excluded = 0; volume_gaps = []; identities=set()
    for item in raw:
        if isinstance(item, dict):
            item = {str(k).lower():v for k,v in item.items()}
            day = _day(item.get('date', item.get('session')))
            identity = item.get('code')
            values = [item.get(k) for k in ('open','close','high','low','volume')]
        elif isinstance(item, (list, tuple)) and len(item) in (6, 7, 8):
            prefixed = len(item) == 8
            identity = item[0] if prefixed else None
            day = _day(item[1] if prefixed else item[0])
            values = item[2:7] if prefixed else item[1:6]
        else:
            raise ValueError('unsupported OHLCV record shape')
        if day > asof:
            excluded += 1
            continue  # In-progress/future bars cannot affect validation or any indicator.
        if code and identity and canonical(str(identity)) != canonical(code):
            raise ValueError('bar security identity mismatch')
        if identity:identities.add(canonical(str(identity)))
        if len(identities)>1:raise ValueError('mixed security identities in daily series')
        o,c,h,l = [_num(x) for x in values[:4]]
        if any(v is None or v <= 0 for v in (o,c,h,l)) or not l <= min(o,c) <= max(o,c) <= h:
            raise ValueError('invalid finite positive OHLC geometry at ' + day)
        v = _num(values[4])
        if v is None or v < 0:
            v = None; volume_gaps.append(day)
        rows.append([day,o,c,h,l,v])
    dates = [b[0] for b in rows]
    if dates != sorted(set(dates)):
        raise ValueError('unsorted or duplicate daily sessions')
    return rows, metadata, excluded, volume_gaps


def _ema(values, period):
    alpha = 2 / (period + 1)
    result = [values[0]]
    for value in values[1:]:
        result.append(result[-1] + alpha * (value-result[-1]))
    return result


def _gmma(bars):
    closes = [b[2] for b in bars]
    emas = {p:_ema(closes,p) for p in SHORT_EMAS+LONG_EMAS}
    short = [emas[p][-1] for p in SHORT_EMAS]
    long = [emas[p][-1] for p in LONG_EMAS]
    slopes = {}
    for name, periods in (('short',SHORT_EMAS),('long',LONG_EMAS)):
        before = fmean(emas[p][-6] for p in periods)
        pct = (fmean(emas[p][-1] for p in periods) / before - 1) * 100
        slopes[name] = {'five_session_change_pct':pct,
                        'direction':'flat' if abs(pct)<=.05 else 'up' if pct>0 else 'down'}
    width = lambda offset:(max(emas[p][offset] for p in emas)-min(emas[p][offset] for p in emas))/closes[offset]*100
    current_width = width(-1); prior_width = width(-6)
    ratio = current_width/prior_width if prior_width else None
    compression = ('flat' if current_width==prior_width==0 else
                   'compressed' if ratio is not None and ratio<=.75 else
                   'expanding' if ratio is not None and ratio>=1.25 else 'stable')
    return _block('OBSERVED', short_emas={str(p):emas[p][-1] for p in SHORT_EMAS},
        long_emas={str(p):emas[p][-1] for p in LONG_EMAS}, slopes=slopes,
        group_relationship='short_above_long' if min(short)>max(long) else
                           'short_below_long' if max(short)<min(long) else 'overlapping',
        short_width_pct=(max(short)-min(short))/closes[-1]*100,
        long_width_pct=(max(long)-min(long))/closes[-1]*100,
        ribbon_width_pct=current_width, ribbon_width_5_sessions_ago_pct=prior_width,
        ribbon_width_ratio=ratio, compression=compression)


def _atr(bars):
    return fmean(max(b[3]-b[4],abs(b[3]-a[2]),abs(b[4]-a[2]))
                 for a,b in zip(bars[-15:-1],bars[-14:]))


def _candles(bars):
    patterns = []
    # Only the last bar (awaiting confirmation) and its predecessor (confirmation
    # now observable) are actionable observations; older patterns are not revived.
    for i in (len(bars)-2,len(bars)-1):
        prev, bar = bars[i-1], bars[i]
        o,c,h,l = bar[1:5]; po,pc = prev[1:3]
        body = abs(c-o); span = h-l
        upper = h-max(o,c); lower = min(o,c)-l
        shapes = []
        if pc < po and c > o and o <= pc and c >= po and (o < pc or c > po):
            shapes.append(('bullish_engulfing','up',i-1))
        if pc > po and c < o and o >= pc and c <= po and (o > pc or c < po):
            shapes.append(('bearish_engulfing','down',i-1))
        if body>0 and span>0 and lower>=2*body and upper<=.5*body and body/span<=.35:
            shapes.append(('hammer','up',i))
        for name, direction, start in shapes:
            pattern_high=max(b[3] for b in bars[start:i+1])
            pattern_low=min(b[4] for b in bars[start:i+1])
            prior = bars[start-20:start]
            prior_return = (bars[start-1][2]/bars[start-6][2]-1)*100
            atr = _atr(bars[:start])
            level = min(b[4] for b in prior) if direction=='up' else max(b[3] for b in prior)
            distance = abs((pattern_low if direction=='up' else pattern_high)-level)
            contextual = (prior_return<0 if direction=='up' else prior_return>0) and distance<=atr
            confirming = bars[i+1] if i+1<len(bars) else None
            confirmed = confirming is not None and (confirming[2]>pattern_high if direction=='up' else confirming[2]<pattern_low)
            status = ('CONTEXT_MISSING' if not contextual else 'PENDING_CONFIRMATION' if confirming is None
                      else 'CONFIRMED' if confirmed else 'NOT_CONFIRMED')
            patterns.append({'pattern':name,'direction':direction,'date':bar[0],
                'status':status,'prior_5_session_return_pct':prior_return,
                'prior_support_or_resistance':level,'level_distance_atr':distance/atr if atr>0 else None,
                'pattern_high':pattern_high,'pattern_low':pattern_low,
                'confirmation_date':confirming[0] if confirming else None,
                'confirmation_close':confirming[2] if confirming else None,
                'confirmation_rule':'next completed close above pattern high' if direction=='up' else
                                    'next completed close below pattern low'})
    return _block('OBSERVED', patterns=patterns,
                  interpretation='形态和下一完整日确认是描述性事实，不预测未来、不单独授予交易权限')


def _price_volume(bars):
    recent = bars[-21:]
    if any(b[5] is None for b in recent):
        return _block('MISSING', '最近21个完整日的成交量缺失或无效', volume_ratio20=None,
                      divergence=None, missing_sessions=[b[0] for b in recent if b[5] is None])
    prior20 = fmean(b[5] for b in recent[:-1])
    if prior20<=0:
        return _block('MISSING', '前20日平均成交量为零，量比无定义',volume_ratio20=None,divergence=None)
    current5 = fmean(b[5] for b in bars[-5:]); previous15 = fmean(b[5] for b in bars[-20:-5])
    price_return = (bars[-1][2]/bars[-21][2]-1)*100
    volume_change = (current5/previous15-1)*100 if previous15>0 else None
    divergence = ('unknown' if volume_change is None else
                  'price_up_volume_down' if price_return>0 and volume_change<0 else
                  'price_down_volume_up' if price_return<0 and volume_change>0 else 'not_observed')
    return _block('OBSERVED',volume_ratio20=bars[-1][5]/prior20,latest_volume=bars[-1][5],
                  previous20_mean_volume=prior20,price_return20_pct=price_return,
                  recent5_mean_volume=current5,preceding15_mean_volume=previous15,
                  volume_change_pct=volume_change,divergence=divergence,
                  interpretation='两个非重叠量能窗口与价格方向的描述；背离不等于反转预测')


def _stop_evidence(stop, *, close, atr, basis, asof, now, code):
    if not isinstance(stop,dict):
        return _block('MISSING','未提供已核实的原止损',distance_atr=None)
    price = _num(stop.get('price'))
    try:
        valid_date = _day(stop.get('asof'))<=asof
        if len(str(stop.get('asof')))>10:
            valid_date = valid_date and _clock(stop['asof'])<=now
    except (ValueError,TypeError):
        valid_date = False
    if (price is None or price<=0 or stop.get('validated') is not True or not _known(stop.get('source'))
            or stop.get('basis')!=basis or not valid_date
            or (code and stop.get('code') and canonical(stop['code'])!=canonical(code))):
        return _block('UNVERIFIED','原止损缺核验、来源、同口径、身份或有效时间证据',distance_atr=None)
    distance = (close-price)/atr if atr>0 else None
    return _block('OBSERVED',provided_stop=price,source=stop['source'],source_asof=stop['asof'],
                  distance_price=close-price,distance_atr=distance,
                  proximity='breached' if close<=price else 'unknown' if distance is None else
                            'within_one_atr' if distance<=1 else 'more_than_one_atr',
                  changes_stop=False,interpretation='仅原止损相对已完成收盘的距离；跳空和成交成本未封顶')


def _structure(bars, stop, *, basis, asof, now, code):
    highs=[];lows=[]
    for i in range(2,len(bars)-2):
        high=bars[i][3];low=bars[i][4]
        neighbours=bars[i-2:i]+bars[i+1:i+3]
        if all(high>b[3] for b in neighbours):
            highs.append({'date':bars[i][0],'price':high,'confirmed_at':bars[i+2][0]})
        if all(low<b[4] for b in neighbours):
            lows.append({'date':bars[i][0],'price':low,'confirmed_at':bars[i+2][0]})
    trend='insufficient_confirmed_pivots'
    if len(highs)>=2 and len(lows)>=2:
        up=highs[-1]['price']>highs[-2]['price'] and lows[-1]['price']>lows[-2]['price']
        down=highs[-1]['price']<highs[-2]['price'] and lows[-1]['price']<lows[-2]['price']
        trend='higher_highs_higher_lows' if up else 'lower_highs_lower_lows' if down else 'mixed_or_range'
    atr=_atr(bars)
    return _block('OBSERVED',confirmed_highs=highs[-2:],confirmed_lows=lows[-2:],trend=trend,
        atr14=atr,validated_stop=_stop_evidence(stop,close=bars[-1][2],atr=atr,basis=basis,asof=asof,now=now,code=code),
        interpretation='两侧各2根完整K线确认的结构枢轴；不冒称完整1-2-3/2B，MA20不是原书趋势线')


def _financial_fact(business, name, now, code):
    value=business.get(name)
    item=dict(value) if isinstance(value,dict) else {'value':value}
    for key in ('source','asof','period','period_end','currency','captured_at','published_at','code'):
        item.setdefault(key,business.get(key))
    item['captured_at']=item.get('captured_at') or item.get('fetched_at') or business.get('fetched_at')
    item['published_at']=item.get('published_at') or item.get('notice_date') or business.get('notice_date')
    number=_num(item.get('value'));gaps=[]
    if number is None:gaps.append('missing_finite_value')
    if not _known(item.get('source')):gaps.append('missing_source')
    if code and item.get('code') and canonical(item['code'])!=canonical(code):gaps.append('identity_mismatch')
    at=None
    try:
        at=date.fromisoformat(_day(item.get('asof')))
        age=(now.date()-at).days
        if age<0:gaps.append('future_fact')
        elif age>(7 if name in {'pe_ttm','pb'} else 210):gaps.append('stale_fact')
        if len(str(item.get('asof')))>10 and _clock(item['asof'])>now:gaps.append('future_fact')
    except (ValueError,TypeError):gaps.append('missing_asof')
    # Current caches cannot be reused as historical point-in-time fundamentals.
    for key in ('captured_at','published_at'):
        if item.get(key):
            try:
                if _clock(item[key])>now:gaps.append('future_'+key)
            except (ValueError,TypeError):gaps.append('invalid_'+key)
    if name in {'recurring_profit','operating_cash_flow'}:
        if not item.get('period'):gaps.append('missing_period')
        if not item.get('currency'):gaps.append('missing_currency')
        try:
            end=_period_end(item.get('period_end') or item.get('period'))
            if end>now.date() or (at is not None and end>at):gaps.append('future_report_period')
            elif (now.date()-end).days>210:gaps.append('stale_report_period')
        except (ValueError,TypeError):gaps.append('unverified_report_period')
    return {'status':'MISSING' if number is None else 'UNVERIFIED' if gaps else 'OBSERVED',
            'value':number,'source':item.get('source'),'asof':item.get('asof'),
            'period':item.get('period'),'currency':item.get('currency'),'gaps':gaps,
            'pit_capture_known':bool(item.get('captured_at') and item.get('published_at'))}


def _graham(business, now, code):
    business=business if isinstance(business,dict) else {}
    facts={name:_financial_fact(business,name,now,code) for name in
           ('recurring_profit','operating_cash_flow','pe_ttm','pb')}
    recurring,ocf=facts['recurring_profit'],facts['operating_cash_flow']
    comparable=(recurring['status']==ocf['status']=='OBSERVED'
                and recurring['period']==ocf['period'] and recurring['currency']==ocf['currency'])
    ratio=ocf['value']/recurring['value'] if comparable and recurring['value']>0 else None
    complete=all(x['status']=='OBSERVED' for x in facts.values())
    quality=business.get('earnings_quality') or {}
    supplied_quality=({key:quality.get(key) for key in ('status','entry_verified','score_basis',
                       'recurring_profit_yoy','recurring_roe_annual','gaps') if key in quality}
                      if isinstance(quality,dict) else {})
    return _block('OBSERVED' if complete else 'PARTIAL' if any(x['status']=='OBSERVED' for x in facts.values()) else 'MISSING',
        family='business_financial',facts=facts,
        recurring_profit_positive=recurring['value']>0 if recurring['status']=='OBSERVED' else None,
        operating_cash_flow_positive=ocf['value']>0 if ocf['status']=='OBSERVED' else None,
        cash_to_recurring_profit=ratio,comparable_period_currency=comparable,
        missing=[name for name,fact in facts.items() if fact['status']!='OBSERVED'],
        supplied_earnings_quality=supplied_quality,
        supplied_quality_scope='上游结果原样摘要，不补造源认证、利润金额、现金流或多年质量记录',
        multi_year_quality='NOT_ESTABLISHED',
        multi_year_quality_reason='未核验多年利润、现金流与资产质量序列，单期正利润不代表多年质量已成立',
        safety_margin='NOT_ESTABLISHED',
        interpretation='经常性利润、同报告期现金和估值倍数只列事实；低PE/PB不证明低估，缺内在价值/资产质量证据不声称安全边际')


def evaluate(data, *, asof, market, now=None, code=None, source=None, basis=None,
             business=None, validated_stop=None):
    now=now or datetime.now(timezone.utc)
    if not isinstance(now,datetime) or now.tzinfo is None:
        raise ValueError('now requires timezone')
    out={'version':VERSION,'asof':asof,'generated_at':now.isoformat(),'code':code,
         'market':MARKETS.get(market,market),'engineering_rules':dict(ENGINEERING_RULES),
         'model_calls':0,'no_grade_authority':True,'no_order_authority':True,
         'changes_stop':False,'changes_target':False,
         'evidence_families':{'price_volume':['gmma','candlesticks','price_volume','structure'],
                              'business_financial':['graham']},
         'independence_note':'全部价格技术视角来自同一OHLCV证据族，不按指标或书籍数量独立投票',
         'graham':_graham(business,now,code),'gaps':[]}
    def unavailable(status, reason):
        out.update(status=status,gaps=[reason])
        for name in ('gmma','candlesticks','price_volume','structure'):
            out[name]=_block(status,reason)
        return out
    try:
        asof=_day(asof);market=MARKETS.get(market)
        if market is None:return unavailable('INVALID','unknown_market')
        required=latest_completed(market,now).isoformat()
        out.update(asof=asof,required_completed_session=required)
        if asof!=required:return unavailable('STALE' if asof<required else 'INVALID','asof_not_latest_completed_session')
        bars,metadata,excluded,volume_gaps=_normalize(data,asof,code)
        out['excluded_future_bars']=excluded
        source=source or metadata.get('provider_source') or metadata.get('provider') or metadata.get('source')
        basis=basis or metadata.get('price_basis') or metadata.get('basis')
        out['provenance']={'source':source,'basis':basis,'source_sha256':metadata.get('source_sha256') or metadata.get('raw_sha256'),
            'screening_only':metadata.get('screening_only',True),'cached_bar_count':len(bars),
            'source_authenticity_verified_here':False,'validation_scope':'cached geometry, timestamps and completed-session continuity'}
        if metadata.get('quality_error'):return unavailable('INVALID','upstream_quality_error')
        capture_times={key:metadata[key] for key in ('fetched_at','checked_at','captured_at','published_at') if metadata.get(key)}
        out['provenance'].update(capture_timestamps=capture_times,
            pit_capture_known=bool(metadata.get('fetched_at') or metadata.get('captured_at')),
            point_in_time_verified_here=False)
        for key,value in capture_times.items():
            try:stamp=_clock(value)
            except (ValueError,TypeError):return unavailable('INVALID','invalid_cache_'+key)
            if stamp>now:return unavailable('INVALID','future_cache_'+key)
        if not _known(source) or not _known(basis):return unavailable('MISSING','missing_source_or_price_basis')
        if not bars:return unavailable('MISSING','missing_completed_bars')
        out['source_asof']=bars[-1][0]
        if bars[-1][0]!=asof:return unavailable('STALE','last_bar_not_required_completed_session')
        if len(bars)<MIN_BARS:return unavailable('INSUFFICIENT','requires_at_least_120_clean_completed_bars')
        bars=bars[-MIN_BARS:]
        expected=[];day=date.fromisoformat(bars[0][0]);end=date.fromisoformat(asof)
        while day<=end:
            if is_session(day,market):expected.append(day.isoformat())
            day+=timedelta(days=1)
        if expected!=[b[0] for b in bars]:return unavailable('INVALID','missing_or_non_session_daily_bars')
        out['provenance']['calculation_bar_count']=len(bars)
        out['provenance']['input_sha256']=hashlib.sha256(json.dumps({'bars':bars,'source':source,'basis':basis},
            sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
        out.update(status='OBSERVED',gmma=_gmma(bars),candlesticks=_candles(bars),
            price_volume=_price_volume(bars),structure=_structure(bars,validated_stop,basis=basis,asof=asof,now=now,code=code))
        if out['price_volume']['status']!='OBSERVED':out['gaps'].append('missing_recent_volume_evidence')
        if out['graham']['missing']:out['gaps'].append('business_evidence_incomplete')
        return out
    except (ValueError,TypeError,KeyError,IndexError,OverflowError) as exc:
        return unavailable('INVALID',str(exc))
