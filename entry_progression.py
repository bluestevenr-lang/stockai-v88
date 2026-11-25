"""Completed-bar price/volume progression. Thresholds are V88 policy, not book quotes.

No forecasts, model calls, annual-trend veto or learned-in-place parameters.
The early lane is a timed research alert with a structural invalidation, not 3A.
"""
from math import isfinite

VERSION = 'entry-progression-2026-11-16-v2'


def number(x):
    return type(x) in (int, float) and isfinite(x)


def features(bars):
    """Input [date, open, close, high, low, volume], completed, ordered, one basis."""
    if len(bars) < 61 or any(len(b) < 6 for b in bars):
        return {'version': VERSION, 'state': 'data_pending'}
    b = bars[-61:]
    if any(not all(number(v) and v > 0 for v in x[1:6]) or
           not x[4] <= min(x[1:3]) <= max(x[1:3]) <= x[3] for x in b):
        return {'version': VERSION, 'state': 'data_invalid'}
    if any(a[0] >= z[0] for a, z in zip(b, b[1:])):
        return {'version': VERSION, 'state': 'data_invalid'}
    c = [x[2] for x in b]; v = [x[5] for x in b]
    last = c[-1]; ma5 = sum(c[-5:])/5; ma20 = sum(c[-20:])/20
    atr = sum(max(z[3]-z[4], abs(z[3]-a[2]), abs(z[4]-a[2])) for a,z in zip(b[-15:-1], b[-14:]))/14
    low20 = min(x[4] for x in b[-20:]); low5 = min(x[4] for x in b[-5:])
    # Non-overlapping windows permit a higher low; no near-equality to low20.
    old_low = min(x[4] for x in b[-20:-5]); higher_low = low5 > old_low
    up = [z[5] for a,z in zip(b[-11:-1],b[-10:]) if z[2] > a[2]]
    down = [z[5] for a,z in zip(b[-11:-1],b[-10:]) if z[2] < a[2]]
    balance = (sum(up)/len(up))/(sum(down)/len(down)) if up and down else None
    vol5 = sum(v[-5:])/5; prior5 = sum(v[-10:-5])/5
    vr = v[-1]/(sum(v[-21:-1])/20)
    dd = (last/max(x[3] for x in b[-60:])-1)*100
    reclaim = last > ma5 and last > c[-4]
    demand = bool(balance is not None and balance >= 1.05) or (last > c[-2] and vr >= 1.15)
    drying = vol5 < prior5 and higher_low
    # Observe improvement before the ten-session aggregate becomes positive.
    # Lower selling effort on retests is distinct from strong breakout volume.
    prior_down=[z[5] for a,z in zip(b[-11:-4],b[-10:-3]) if z[2]<a[2]]
    recent_down=[z[5] for a,z in zip(b[-4:-1],b[-3:]) if z[2]<a[2]]
    easing=bool(prior_down and recent_down and sum(recent_down)/len(recent_down)<sum(prior_down)/len(prior_down))
    pivot=min(b[-20:-2],key=lambda x:x[4])
    retest_higher=min(x[4] for x in b[-2:])>pivot[4]
    improving=dd<=-12 and retest_higher and reclaim and easing
    base = dd <= -12 and higher_low and reclaim
    breakout = last > max(x[3] for x in b[-6:-1]) and demand
    distribution = last < c[-2] and vr >= 1.5 and (last-b[-1][4])/(b[-1][3]-b[-1][4] or 1) < .35
    stage = '初升确认' if base and breakout else '左侧拐点' if base and (demand or drying) else '抛压减轻·左侧试探' if improving else '底部蓄势' if dd <= -12 and higher_low and drying else '趋势跟踪'
    from structural_entry_plan import swing_levels
    return {**swing_levels(b), 'version': VERSION, 'state': 'complete', 'asof': b[-1][0], 'reference_close': last,
            'stage': stage, 'early': stage in ('左侧拐点','初升确认','抛压减轻·左侧试探'),
            'supply_easing':easing,'retest_higher':retest_higher,
            'higher_low': higher_low, 'volume_demand': demand, 'volume_drying': drying,
            'up_down_volume_ratio': round(balance,4) if balance is not None else None,
            'volume_ratio20': round(vr,4), 'distribution': distribution,
            'drawdown60_pct': round(dd,2), 'structural_low': low5, 'base_low': low20,
            'ma20': ma20, 'atr14': atr, 'trigger': max(x[3] for x in b[-3:]),
            'reason': '低点抬高；收复短期均线；'+('抛压逐渐减轻' if improving and not demand else '上涨量能改善' if demand else '回撤量能收缩' if drying else '量能确认待补')}


def location(q):
    e = q.get('structure') or q.get('entry_gate') or {}
    pv = e.get('price_volume') or q.get('price_volume') or {}
    last = q.get('last'); ma = e.get('ma20') or pv.get('ma20'); atr = e.get('atr14') or pv.get('atr14')
    if not all(number(x) and x > 0 for x in (last,ma)):
        return {'version': VERSION, 'state': 'data_pending', 'blocked': False}
    ext = (last/ma-1)*100
    ceiling = ma + max(ma*.08, min(ma*.12, 2.5*atr)) if number(atr) and atr > 0 else ma*1.12
    stretched = last > ceiling
    r60 = e.get('return60_pct'); r5 = e.get('return5_pct'); high = e.get('previous_high20')
    fading = (number(r60) and r60 >= 15 and number(high) and high > 0 and last < high*.97
              and ((number(r5) and r5 < 0) or pv.get('distribution')))
    blocked = stretched or fading
    why = (f'距20日均线{ext:.1f}%，等待回踩≤{ceiling:.3f}并重新确认' if stretched else
           '高位回落且动量/量价转弱，等待缩量企稳' if fading else '位置未过度延伸')
    return {'version': VERSION, 'state': 'wait_pullback' if blocked else 'eligible', 'blocked': blocked,
            'extension20_pct': round(ext,2), 'entry_ceiling': round(ceiling,4), 'reason': why}


def early_candidates(source, now):
    """All three markets, independent of old winner-selected horizon cells."""
    from exchange_sessions import latest_completed
    out=[]
    for q in source:
        e=q.get('structure') or {}; pv=e.get('price_volume') or {}
        if not pv.get('early') or pv.get('state') != 'complete' or location(q)['blocked']:
            continue
        if pv.get('asof') != latest_completed(q['market'], now).isoformat():
            continue
        last=q.get('last'); stop=pv['structural_low']*.99
        if not number(last) or not stop < last:
            continue
        # A rebound can be interesting despite weak earnings, but say so.
        risk=(q.get('fundamentals') or {}).get('risk_summary') or ''
        if (q.get('filings') or {}).get('status') == 'conflict':
            continue
        ceiling=stop/.92
        entry=[round(min(last,ceiling)*.99,4),round(min(last,ceiling),4)]
        levels=sorted(x for x in (e.get('previous_high20'),e.get('high60')) if number(x) and x>entry[1])
        out.append({k:q.get(k) for k in ('code','name','market','last','change_pct','quote_asof','source_session','currency')} | {
            'policy_version':VERSION,'stage':pv['stage'],'signal_date':pv['asof'], 'price_volume':pv,
            'entry_range':entry,'trigger':round(pv['trigger'],4),
            'stop':round(stop,4),'target':round(levels[0],4) if levels else None,'max_sessions':5,
            'status':'拐点提醒 · 等待入场核验','buyable':False,'executable':False,
            'reason':pv['reason'],'risk':risk,'evidence_scope':'新规则前瞻跟踪，胜率待积累；不冒充长期价值推荐'})
    return sorted(out,key=lambda r:(r['stage']!='初升确认',-r['price_volume']['drawdown60_pct'],r['code']))
