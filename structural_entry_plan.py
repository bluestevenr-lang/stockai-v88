"""Production price geometry. Historical cell statistics retain their original exits."""
from math import floor, isfinite

VERSION = 'structure-entry-20261125-v1'
FLOORS = {'3A': 2.0, '2A': 1.5, 'Astra': 2.0}


def active(asof):
    from datetime import timezone, timedelta
    return asof is None or asof.astimezone(timezone(timedelta(hours=8))).date().isoformat() >= '2026-11-25'


def positive(value):
    return type(value) in (float, int) and isfinite(value) and value > 0


def ratio(entry, stop, target, fee):
    if not all(positive(v) for v in (entry, stop, target)) or not stop < entry < target:
        return None
    cost = entry * fee / 100
    return (target - entry - cost) / (entry - stop + cost)


def swing_levels(bars):
    # A pivot is known only after the two subsequent daily bars have closed.
    supports, resistances = [], []
    for i in range(2, len(bars) - 2):
        day, _, _, high, low = bars[i][:5]
        before_after = bars[i-2:i] + bars[i+1:i+3]
        if all(low < x[4] for x in before_after):
            supports.append([day, low])
        if all(high > x[3] for x in before_after):
            resistances.append(high)
    return {'swing_lows': supports, 'swing_highs': resistances}


def build(q, tier, hold, kind, min_stop_pct, max_stop_pct, fee):
    e = q.get('structure') or {}
    pv = e.get('price_volume') or {}
    entry, atr, high52 = q.get('last'), e.get('atr14'), e.get('high52')
    if not all(positive(x) for x in (entry, atr, high52)) or not (
            e.get('basis') and e.get('raw_sha256') and e.get('source_asof')
            and pv.get('asof') == e['source_asof']):
        return None, '缺同口径完整日线、ATR或52周位置'
    anchor = next(((d,v) for d,v in reversed(pv.get('swing_lows', []))
                   if positive(v) and v < entry), None)
    if not anchor:
        return None, '缺已确认的摆动低点'
    day, low = anchor
    stop = round(low - .25 * atr, 4)
    entry = round(entry, 4)
    depth = (1 - stop / entry) * 100
    if not min_stop_pct <= depth <= max_stop_pct or entry - stop < atr:
        return None, '已确认低点的止损距离超出该档风险范围'
    levels = [*pv.get('swing_highs', []), *(e.get(k) for k in ('previous_high20','high60','high120','high52'))]
    # A cleared base permits a measured move only when no closer barrier exists.
    above = [v for v in levels if positive(v) and v > entry]
    if above:
        target = min(above)
        method = '首道上方阻力'
    elif positive(e.get('previous_high20')) and e['previous_high20'] > low and pv.get('reference_close', 0) > e['previous_high20']:
        target = e['previous_high20'] + (e['previous_high20'] - low)
        method = '确认突破后的平台量度涨幅'
    else:
        return None, '缺首道阻力或已突破平台的量度依据'
    target = round(target, 4)
    if entry > .95 * high52 and target > high52:
        return None, '接近52周高点且首目标须创新高'
    rr = ratio(entry, stop, target, fee)
    if rr is None or rr < FLOORS[tier]:
        return None, f'首道目标扣费净盈亏比不足{FLOORS[tier]:g}'
    # Quote only the portion of the entry band that still clears one ATR at its
    # lower edge. Entry ceiling, structural stop and first target stay fixed.
    outside_noise = (floor((stop + atr) * 10000) + 1) / 10000
    lower = round(max(entry * .98, low, outside_noise), 4)
    if not stop < lower < entry or lower-stop <= atr:
        return None, '原买点与结构止损之间缺少波动空间'
    gross = (target / entry - 1) * 100
    return {'tier':tier, 'hold':hold, 'kind':kind, 'entry_range':[lower,entry],
            'stop':stop, 'stop_range':[stop,stop], 'stop_pct':depth,
            'target':target, 'take_profit_range':[target,target], 'target_pct':gross,
            'net_target_return_pct':[round(gross-fee,2),round((target/lower-1)*100-fee,2)],
            'net_rr':round(rr,4), 'price_plan_version':VERSION, 'price_plan_method':method,
            'price_plan_scope':'原形态回测使用旧出口；结构新计划单列验证',
            'stop_anchor':{'type':'已确认摆动低点','date':day,'price':low,'atr14':atr,
                           'stop_level':stop,'validation':'低点两侧各两根完整日线确认','offset_atr':.25,'source_asof':e['source_asof'],'basis':e['basis']},
            'target_evidence':{'status':'SCENARIO','type':method,'target_price':target,'high52':high52,'resistance':min(above) if above else None,
                               'base_high':e.get('previous_high20'),'base_low':low,'source_asof':e['source_asof'],
                               'basis':e['basis'],'source_sha256':e['raw_sha256']}}, None


def review_gaps(row, tier, now):
    """Pre-submission check on the frozen prices; historical contracts stay intact."""
    # Prior dated publications must remain reproducible under their original policy.
    if not active(now) or tier not in FLOORS:
        return []
    gaps = []
    target_fact = row.get('target_evidence') or {}
    anchor = row.get('stop_anchor') or {}
    if row.get('price_plan_version') != VERSION or not (target_fact.get('source_sha256') and anchor.get('date')):
        gaps.append('缺本版结构定价证据')
    entry = row.get('entry_range') or []
    stops = row.get('stop_range') or []
    targets = row.get('take_profit_range') or []
    d = row.get('directive') or {}
    if len(entry)==len(stops)==len(targets)==2 and all(positive(v) for v in [*entry,*stops,*targets]):
        if (target_fact.get('target_price') != targets[0] or anchor.get('stop_level') != stops[0]
                or not row.get('price_plan_basis') or not row.get('price_plan_source_asof')
                or any(proof.get('basis') != row['price_plan_basis'] or
                       proof.get('source_asof') != row['price_plan_source_asof'] for proof in (anchor,target_fact))):
            gaps.append('价带与定价时的结构证据不一致')
        ceiling = max(v for v in (entry[1],d.get('price')) if positive(v))
        stop = min(v for v in (stops[0],d.get('stop')) if positive(v))
        objectives = [targets[0],d.get('target'),target_fact.get('resistance'),
                      row.get('plan_current_nearest_resistance'),row.get('plan_current_structure_target'),
                      (row.get('opportunity') or {}).get('first_objective')]
        obj = min(v for v in objectives if positive(v))
        rr = ratio(ceiling,stop,obj,.5)
        if rr is None or rr < FLOORS[tier]:
            gaps.append(f'首目标按入场上沿及止损下沿扣费后不足{FLOORS[tier]:g}R')
        high=target_fact.get('high52')
        if not positive(high) or not positive(row.get('last')):
            gaps.append('52周位置待核')
        elif row['last'] > .95*high and targets[0] > high:
            gaps.append('近52周高且目标须创新高')
    else:
        gaps.append('缺完整价带')
    tech = (row.get('crosscheck') or {}).get('technical') or {}
    from exchange_sessions import latest_completed
    try:
        required = latest_completed(row.get('market'),now).isoformat()
    except (ValueError,TypeError):
        required = None
    if (not required or tech.get('status')!='OBSERVED' or tech.get('asof')!=required
            or not (tech.get('provenance') or {}).get('input_sha256')):
        gaps.append('最新完整交易日日线待核')
    check=(row.get('crosscheck') or {}).get('stop_check') or {}
    if check.get('anchor_status')!='confirmed' or check.get('status')!='outside_one_atr':
        gaps.append('止损锚或波动距离待核')
    f=(row.get('financial_evidence') or {}).get('growth_check') or {}
    fundamentals=row.get('fundamentals') or {}
    from filing_source_facts import compare
    if (f.get('status')!='matched' or f.get('code')!=row.get('code') or not f.get('source_url')
            or not f.get('period_end') or f['period_end']!=fundamentals.get('period_end')
            or compare(row,f.get('periods') or {},f['source_url'],f.get('published_at'))['status']!='matched'):
        gaps.append('营收及归母净利同比尚未与同期间公告核对')
    return gaps
