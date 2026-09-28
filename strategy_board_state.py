"""Read-time source freshness, shared by the cloud, desktop and digest."""
from copy import deepcopy
from datetime import datetime,timezone
from zoneinfo import ZoneInfo
from exchange_sessions import latest_completed

def view(doc,now=None):
    now=now or datetime.now(timezone.utc);out=deepcopy(doc)
    # Rolling deployments may briefly read an old cached bundle. Never render
    # a 61-point investment lead under the new >=80 highest-grade heading.
    if out.get('grade_policy')!='2026-09-28-highest-3a-v2':
        keep=[];pending=out.setdefault('investment_candidates',[])
        for row in out.get('rows',[]):
            value=(row.get('strategy_score') or {}).get('value',0)
            if (row.get('strategy_tier')=='3A' and (value<80 or not row.get('business_supported'))) or (row.get('strategy_tier')!='3A' and value>=80):
                row['grade_pending']=True;pending.append(row)
            else:keep.append(row)
        out['rows']=keep
    for row in out.get('rows',[])+out.get('investment_candidates',[]):
        try:
            zone=ZoneInfo('America/New_York' if row['market']=='美股' else 'Asia/Shanghai')
            at=datetime.fromisoformat(row['quote_asof'])
            required=latest_completed(row['market'],now).isoformat()
            row['current_source']=at.tzinfo is not None and at<=now and row.get('source_session') in {required,now.astimezone(zone).date().isoformat()}
            if not row['current_source']:
                row['status']='源行情待更新';row['executable']=False
                if '最新交易日报价' not in row['missing']:row['missing'].append('最新交易日报价')
            elif (now-at).total_seconds()>20*60:
                if row['status']=='条件匹配':row['status']='研究候选'
                if '入场前更新报价' not in row['missing']:row['missing'].append('入场前更新报价')
        except (ValueError,KeyError,TypeError):
            row['current_source']=False;row['status']='源行情待更新';row['executable']=False
    return out
