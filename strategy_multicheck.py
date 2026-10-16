"""Cached, versioned evidence for the selected 3A/Astra names only.

No page-time network, score tuning, contract edits, or invented model votes.
Conflicting/missing cross-market evidence asks for review, not removal from research.
"""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3

VERSION = 'strategy-crosscheck-v1'
LABELS = {'aligned':'大盘·板块·个股同向', 'countertrend':'逆势研究 · 须核催化',
          'mixed':'方向分歧 · 等待复核', 'unknown':'趋势交叉证据待补'}


def digest(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',', ':'),allow_nan=False).encode()).hexdigest()


def read(path):
    try:return json.loads(Path(path).read_text())
    except (OSError, ValueError):return {}


def decision_facts(row):
    return {k:deepcopy(row.get(k)) for k in ('code','name','market','currency','last','quote_asof','source_session',
        'strategy_score','price_plan_basis','entry_range','take_profit_range','stop_range','opportunity','annual_regime',
        'business_reason','business_risk','fundamentals','filing_check','plan_id','window',
        'selection_category','status','buyable','rule_matched','missing','score_current_unknown','research_roster')}


def business_facts(row, raw):
    """Reuse only attributable financial facts; per-share cash is not total cash."""
    out=deepcopy(row.get('fundamentals') or {})
    r=raw.get('recurring') or {}
    if r.get('code')==row.get('code') and r.get('recurring_profit') is not None:
        out['recurring_profit']={'value':r['recurring_profit'],'code':r['code'],
            'source':r.get('source_url'),'period':r.get('period_end'),
            'period_end':r.get('period_end'),'currency':r.get('currency'),
            'asof':r.get('captured_at'),'captured_at':r.get('captured_at'),
            'published_at':raw.get('notice_date')}
    return out


def enrich(doc, source, *, base, now=None, previous=None, module="3A"):
    """Background only; reuse validated daily series from this screening input."""
    from classics_evidence import evaluate as classical
    from market_stock_alignment import evaluate as alignment
    from sector_rotation import build_rotation_observation
    from screen_history import load as screen_load
    from fundamental_snapshot import load as financial_load
    now=now or datetime.now(timezone.utc);base=Path(base)
    snapshot=read(base/'data/market_snapshot.json')
    old_rotation=read(base/'data/rotation_forecast.json').get('relative_strength') or {}
    rotation=build_rotation_observation(snapshot.get('markets') or {},previous=old_rotation,now=now)
    sources={q['code']:q for q in source.get('rows',[])}
    financials=financial_load(base).get('records',{})
    screening=None;db=None
    try:
        if (base/'data/wdata.db').exists():
            db=sqlite3.connect((base/'data/wdata.db').resolve().as_uri()+'?mode=ro',uri=True,timeout=3)
            db.execute('BEGIN')
        for row in doc.get('rows',[]):
            if module=='3A' and row.get('strategy_tier')!='3A':continue
            q=sources.get(row['code']) or {};struct=q.get('structure') or {}
            bars=[];provenance={'source':struct.get('provider_source'),'basis':struct.get('basis'),
                'sha256':struct.get('raw_sha256'),'source_asof':struct.get('source_asof')}
            metadata={'source_sha256':struct.get('raw_sha256'),'screening_only':bool(struct.get('screening_only'))}
            if struct.get('screening_only'):
                if screening is None:screening=screen_load(base).get('records',{})
                record=screening.get(row['code']) or {}
                if record.get('source_sha256') and record.get('source_sha256')==struct.get('raw_sha256'):
                    bars=(record.get('bars') or [])[-300:]
                    metadata.update({k:record[k] for k in ('fetched_at','checked_at','quality_error') if k in record})
            elif struct and db is not None:
                try:
                    meta=db.execute('SELECT sha256 FROM verified_source_receipts WHERE code=?',(row['code'],)).fetchone()
                    if meta and meta[0]==struct.get('raw_sha256'):
                        bars=db.execute('SELECT code,date,open,close,high,low,volume,amount FROM daily_verified WHERE code=? ORDER BY date DESC LIMIT 300',(row['code'],)).fetchall()[::-1]
                except sqlite3.OperationalError:pass
            technical=classical({'bars':bars,**metadata},asof=struct.get('source_asof'),market=row['market'],now=now,
                code=row['code'],source=provenance['source'],basis=provenance['basis'],
                business=business_facts(row,financials.get(row['code']) or {}))
            technical.pop('generated_at',None)  # recomputation time is not a changed market fact
            context=alignment({**row,'nontechnical':q.get('nontechnical') or {}},snapshot,rotation,technical=technical,now=now)
            context.pop('evaluated_at',None)
            data={'version':VERSION,'asof':struct.get('source_asof'),'provenance':provenance,
                  'technical':technical,'alignment':context,'stop_check':stop_quality(row,technical),
                  'price_indicators_are_one_family':True,
                  'model_calls':0,'rules_ready':bool(row.get('buyable') or row.get('rule_matched')),
                  'scope':'工程化证据核查，不是原书逐条复刻、独立多票或胜率证明'}
            # Bind cloud rule reviews to exact input facts and evidence, never an updated clock alone.
            row['crosscheck']=data
            apply_entry_review(row,now)
            data['facts_sha256']=review_hash(row)
            if row['code'] in doc.get('evaluations',{}):
                from strategy_board import compact
                doc['evaluations'][row['code']]=compact(row)
    finally:
        if db is not None:db.close()
    doc['crosscheck_version']=VERSION
    if 'rule_matched_count' in doc:doc['rule_matched_count']=sum(bool(r.get('rule_matched')) for r in doc.get('rows',[]))
    for tier,markets in doc.get('counts',{}).items():
        for market,counts in markets.items():
            counts['matched']=sum(bool(r.get('buyable')) for r in doc.get('rows',[]) if r.get('strategy_tier')==tier and r.get('market')==market)
    return doc


def entry_gaps(row,now=None):
    data=row.get('crosscheck') or {}
    if data.get('version')!=VERSION:return []  # Historical versions retain their own facts.
    from exchange_sessions import latest_completed
    now=now or datetime.now(timezone.utc)
    expected=latest_completed(row['market'],now).isoformat()
    gaps=[];context=data.get('alignment') or {}
    if data.get('asof')!=expected:gaps.append('多重核查：完整日线待更新')
    state=context.get('state','unknown')
    if (context.get('stock_context') or {}).get('direction')=='down':
        gaps.append('多重核查：个股技术结构下行，同向下跌也不构成买入许可')
    if state!='aligned':
        gaps.append('多重核查：'+LABELS.get(state,LABELS['unknown']))
        if state=='countertrend':
            ex=context.get('exception') or {}
            gaps.append('多重核查：已有催化原文待人工/Muse复核，不自动抵消逆势' if ex.get('status')=='documented_for_review' else '多重核查：缺少可追溯的逆势公司催化')
    stop=data.get('stop_check') or {}
    if stop.get('status')=='within_daily_noise':gaps.append('多重核查：原止损距参考入场不超过1个ATR，落在近期日波动内')
    if stop.get('anchor_status')!='confirmed':gaps.append('多重核查：原止损锚尚无已确认结构支撑，不自动放宽止损')
    return list(dict.fromkeys(gaps))


def apply_entry_review(row,now=None):
    row['missing']=[g for g in row.get('missing',[]) if not g.startswith('多重核查：')]
    gaps=entry_gaps(row,now)
    if not gaps:return row
    row['missing']=list(dict.fromkeys((row.get('missing') or [])+gaps))
    row['buyable']=False
    if 'rule_matched' in row:row['rule_matched']=False
    row['status']='研究候选' if not row.get('plan_id') else '原计划研究 · 等待'
    if row.get('strategy_tier')=='3A':row['selection_category']='高价值可跟进 · 重点观察'
    row['readiness_label']='重点观察 · 等待多重核查'
    row['readiness_detail']='；'.join(gaps)
    return row


def brief(row):
    d=row.get('crosscheck') or {};a=d.get('alignment') or {}
    if d.get('version')!=VERSION:return '多重核查：本版待更新，未作验证通过。'
    reasons='；'.join(a.get('reasons') or [])
    title='三层同向下行 · 不构成买入' if a.get('state')=='aligned' and (a.get('stock_context') or {}).get('direction')=='down' else LABELS.get(a.get('state'),LABELS['unknown'])
    return '云端规则复核：'+title+('｜'+reasons[:130] if reasons else '')


def stop_quality(row,technical):
    """Compare the existing plan only; never manufacture a wider stop or real fill."""
    import math
    plan_basis=row.get('price_plan_basis');technical_basis=(technical.get('provenance') or {}).get('basis')
    basis={'price_plan_basis':plan_basis,'technical_basis':technical_basis}
    if not all(isinstance(value,str) and value.strip() for value in (plan_basis,technical_basis)):
        return {**basis,'status':'unknown','anchor_status':'unverified','reason':'原计划或技术日线复权口径缺失，ATR及止损锚不可比较','changes_stop':False}
    if plan_basis!=technical_basis:
        return {**basis,'status':'unknown','anchor_status':'unverified','reason':'原计划与技术日线复权口径不一致，ATR及止损锚不可比较','changes_stop':False}
    structure=technical.get('structure') or {};atr=structure.get('atr14')
    band=row.get('stop_range') or [];entry=row.get('entry_range') or []
    number=lambda v:isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v) and v>0
    if len(band)!=2 or len(entry)!=2 or not all(number(v) for v in [*band,*entry,atr]):
        return {**basis,'status':'unknown','anchor_status':'unverified','reason':'原价带或同口径ATR缺失','changes_stop':False}
    if not band[0]<=band[1]<entry[0]<=entry[1]:
        return {**basis,'status':'invalid','anchor_status':'unverified','reason':'原价带顺序待核','changes_stop':False}
    distance=(entry[0]-band[1])/atr
    anchors=[p for p in structure.get('confirmed_lows',[]) if band[0]<=p['price']<=band[1]]
    return {**basis,'status':'within_daily_noise' if distance<=1 else 'outside_one_atr',
        'anchor_status':'confirmed' if anchors else 'unverified','anchors':anchors,
        'distance_atr':round(distance,4),'atr14':atr,'reference_entry':entry[0],
        'stop_near_edge':band[1],'changes_stop':False,
        'scope':'原入场下沿到止损上沿的保守距离；1ATR为工程风险提示，非保证止损不触发；无持仓量不虚构账户损失'}


def review_hash(row):
    data=row.get('crosscheck') or {}
    return digest({'facts':decision_facts(row),'technical':data.get('technical'),
                   'alignment':data.get('alignment'),'stop_check':data.get('stop_check')})
