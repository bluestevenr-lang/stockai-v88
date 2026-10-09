"""Read-only audit of published 3A rules, separate from Astra technical replay.

A frozen prospective publication is evidence of what was known, not proof of
predictive performance. Never backfill today's fundamentals into past signals.
"""
from collections import Counter
from datetime import datetime, timezone
from copy import deepcopy
import argparse
import json
from pathlib import Path

VERSION='3a-independent-validation-v1'
NOTE='3A独立验证：尚无成熟样本外结论；Astra技术回放不代表3A胜率，评分权重保持不变。'


def freeze(row):
    """Public research facts only. Stored in the existing append-only journal."""
    return {'version':VERSION, **deepcopy({k:row.get(k) for k in (
        'strategy_score','grade_policy','source_strategy','business_source','fundamentals',
        'filing_check','industry_snapshot','source_sha256','quote_asof','source_session',
        'price_plan_version','price_plan_source_asof','entry_range','stop_range',
        'take_profit_range','net_rr','fee_allowance_pct','resistance_levels',
        'nearest_resistance','target_evidence','missing')})}


def audit(board, profiles=None):
    """Recompute invariants independently from the producer; no policy changes."""
    from industry_diversity import industry
    errors=[]; seen={}; sectors=Counter(); examined=0
    for r in board.get('rows',[]):
        code=r['code'];s=r['strategy_score'];key=(code,s.get('snapshot_id'))
        if key in seen and seen[key]!=s['value']:errors.append({'code':code,'error':'same_snapshot_score_conflict'})
        seen[key]=s['value']
        if r.get('strategy_tier')!='3A':continue
        examined+=1
        if s['value']<80 or not r.get('business_supported'):errors.append({'code':code,'error':'invalid_3a_eligibility'})
        sector=industry(r,profiles)
        if sector:sectors[(r['market'],sector)]+=1
        entry=r.get('entry_range');target=r.get('take_profit_range');stop=r.get('stop_range')
        if not all((entry,target,stop)):continue
        barrier=r.get('nearest_resistance')
        if barrier and target[0]>barrier:errors.append({'code':code,'error':'nearest_resistance_skipped'})
        if not 0<stop[0]<=stop[1]<entry[0]<entry[1]<target[0]<=target[1]:
            errors.append({'code':code,'error':'invalid_price_order'});continue
        fee=r.get('fee_allowance_pct',.5)
        reward=(target[0]/entry[1]-1)*100-fee
        risk=(1-stop[0]/entry[1])*100+fee
        actual=round(reward/risk,2)
        if r.get('net_rr') is None or abs(actual-r['net_rr'])>.011:
            errors.append({'code':code,'error':'net_rr_mismatch','calculated':actual,'published':r.get('net_rr')})
        if r.get('status')=='条件匹配' and actual<1.5:
            errors.append({'code':code,'error':'matched_with_insufficient_rr'})
    for (market,sector),count in sectors.items():
        if count>1:errors.append({'market':market,'industry':sector,'error':'duplicate_industry','count':count})
    return {'version':VERSION,'rows_checked':len(board.get('rows',[])),'three_a_checked':examined,
            'invariants_passed':not errors,'errors':errors,'oos_validated':False,
            'auto_weight_changes':False,'note':NOTE}


def evidence_status(events, revision):
    """Report genuine saved cohorts, never infer missing factors from scores."""
    counts=Counter(); dates=[]
    for event in events:
        if event.get('kind')!='strategy_3a':continue
        for row in event.get('rows',[]):
            proof=row.get('strategy_validation') or {}
            if proof.get('version')!=VERSION:
                counts['legacy_without_frozen_factors']+=1;continue
            if (proof.get('strategy_score') or {}).get('revision')!=revision:continue
            counts['frozen_publications']+=1;dates.append(event['at'])
    return {**counts,'first_capture':min(dates) if dates else None,
            'last_capture':max(dates) if dates else None,'oos_validated':False,
            'reason':'需按原始入选时点积累15/60交易日结果，并完成费用、成交与历史样本口径核验；当前不得用技术回放替代。'}


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--base',type=Path,default=Path(__file__).resolve().parents[1]);parser.add_argument('--out',type=Path)
    args=parser.parse_args()
    from stock_profile_view import load
    board=json.loads((args.base/'data/strategy_board.json').read_text())
    report=audit(board,load(args.base/'data/stock_profiles_pub.json'))
    p=args.base/'data/recommendation_journal_events.json'
    events=json.loads(p.read_text()).get('events',[]) if p.exists() else []
    report['evidence']=evidence_status(events,board.get('scoring_revision'))
    report['checked_at']=datetime.now(timezone.utc).isoformat()
    text=json.dumps(report,ensure_ascii=False,indent=2)
    if args.out:args.out.write_text(text)
    print(text)

if __name__=='__main__':main()
