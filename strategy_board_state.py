"""Read-time source freshness, shared by the cloud, desktop and digest."""
from copy import deepcopy
from datetime import datetime,timezone,timedelta,time,date
from zoneinfo import ZoneInfo
from exchange_sessions import latest_completed

# v4: 3A means buyable now (>=70, business, annual uptrend, net RR>=1.5, in range).
# Rows produced under older rules are never shown under the buyable heading.
# v5: 2A/1A use the same buyable contract with backtested setups.
CURRENT_POLICIES=('2026-10-11-evidence-tiers-v5',)
STILL_VALID={'2026-10-11-buyable-3a-v4':('3A',)}
BUY_FLOOR=70
TIMING_TEXT={'live':'可立即买入（规则条件满足，现价在入场区间内）',
             'next_open':'下一交易时段开盘可买（开盘价仍在入场区间内才成立）',
             'stale':'报价已过时，入场前更新报价'}

def _zone(market):
    return ZoneInfo('America/New_York' if market=='美股' else 'Asia/Shanghai')

def _close_minute(market,day):
    from exchange_sessions import HALF
    if day.strftime('%m-%d') in HALF.get(market,()):return 13*60 if market=='美股' else 12*60
    return 15*60 if market=='A股' else 16*60

def quote_timing(row,now):
    """'live' fresh in session; 'next_open' closing/lunch quote while shut; else 'stale'."""
    from exchange_sessions import session_view
    view=session_view(row['market'],now);state=view['market_status']
    at=datetime.fromisoformat(row['quote_asof'])
    if state=='交易中':
        return 'stale' if (now-at).total_seconds()>20*60 else 'live'
    local=now.astimezone(_zone(row['market']))
    if state=='午间休市':
        day=local.date();minute=690 if row['market']=='A股' else 720
    else:
        day=local.date() if state=='已收市' else date.fromisoformat(view['completed_session'])
        minute=_close_minute(row['market'],day)
    end=datetime.combine(day,time(minute//60,minute%60),_zone(row['market']))
    if row.get('source_session')!=day.isoformat():return 'stale'
    return 'next_open' if at>=end-timedelta(minutes=20) else 'stale'

def view(doc,now=None):
    now=now or datetime.now(timezone.utc);out=deepcopy(doc)
    if out.get('grade_policy') not in CURRENT_POLICIES:
        keep=[];pending=out.setdefault('investment_candidates',[])
        valid=STILL_VALID.get(out.get('grade_policy'),())
        for row in out.get('rows',[]):
            value=(row.get('strategy_score') or {}).get('value',0);tier=row.get('strategy_tier')
            if tier in valid:keep.append(row);continue
            if valid or tier=='3A' or value>=80:
                row['grade_pending']=True
                note='旧规则结果，待按历史验证形态重算' if tier in ('1A','2A') else '旧规则结果，待按“现在可买”规则重算'
                if note not in row.setdefault('missing',[]):row['missing'].append(note)
                pending.append(row)
            else:keep.append(row)
        out['rows']=keep
    for row in out.get('rows',[])+out.get('investment_candidates',[]):
        try:
            zone=_zone(row['market'])
            at=datetime.fromisoformat(row['quote_asof'])
            required=latest_completed(row['market'],now).isoformat()
            row['current_source']=at.tzinfo is not None and at<=now and row.get('source_session') in {required,now.astimezone(zone).date().isoformat()}
            if not row['current_source']:
                row['status']='源行情待更新';row['executable']=False;row['buy_timing']='stale'
                if '最新交易日报价' not in row['missing']:row['missing'].append('最新交易日报价')
                continue
            timing=quote_timing(row,now);row['buy_timing']=timing
            if timing=='stale':
                if row['status']=='条件匹配':row['status']='研究候选'
                if '入场前更新报价' not in row['missing']:row['missing'].append('入场前更新报价')
            else:
                row['missing']=[m for m in row['missing'] if m!='入场前更新报价']
                if not row['missing'] and row['status']=='研究候选' and row.get('buyable'):row['status']='条件匹配'
        except (ValueError,KeyError,TypeError):
            row['current_source']=False;row['status']='源行情待更新';row['executable']=False;row['buy_timing']='stale'
    return out
