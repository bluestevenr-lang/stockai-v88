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
from classics_evidence import _stop_evidence

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
            'published_at':raw.get('notice_date'),
            'publication_timezone':'Asia/Shanghai' if r['code'].endswith(('.SS','.SZ')) else None,
            'publication_scope':'associated_financial_report_notice_date_not_recurring_fact_first_release'}
    return out


def enrich(doc, source, *, base, now=None, previous=None, module="3A", review_entries=True):
    """Background only; reuse validated daily series from this screening input."""
    from classics_evidence import evaluate as classical
    from market_stock_alignment import evaluate as alignment
    from sector_rotation import build_rotation_observation
    from screen_history import load as screen_load
    from screening_financial_cache import records as financial_records
    now=now or datetime.now(timezone.utc);base=Path(base)
    snapshot=read(base/'data/market_snapshot.json')
    old_rotation=read(base/'data/rotation_forecast.json').get('relative_strength') or {}
    rotation=build_rotation_observation(snapshot.get('markets') or {},previous=old_rotation,now=now)
    from market_symbols import canonical
    sources={canonical(q['code']):q for q in source.get('rows',[])}
    financials=financial_records(base,now)
    import time
    filing_deadline = time.monotonic() + 20
    screening=None;db=None
    try:
        if (base/'data/wdata.db').exists():
            db=sqlite3.connect((base/'data/wdata.db').resolve().as_uri()+'?mode=ro',uri=True,timeout=3)
            db.execute('BEGIN')
        for row in doc.get('rows',[]):
            if module=='3A' and row.get('strategy_tier') not in ('3A','2A','1A'):continue
            q=sources.get(canonical(row['code'])) or {};struct=q.get('structure') or {}
            # Retained seats may be absent from today's discovery subset. Read their
            # own cached series, never replace them with another name or an old quote.
            if not struct:
                if screening is None:screening=screen_load(base).get('records',{})
                record=screening.get(canonical(row['code'])) or {}
                if record.get('source_sha256') and record.get('basis') and record.get('bars'):
                    struct={'screening_only':True,'raw_sha256':record['source_sha256'],
                            'basis':record['basis'],'provider_source':'tencent-screen',
                            'source_asof':record['bars'][-1][0]}
            bars=[];provenance={'source':struct.get('provider_source'),'basis':struct.get('basis'),
                'sha256':struct.get('raw_sha256'),'source_asof':struct.get('source_asof')}
            metadata={'source_sha256':struct.get('raw_sha256'),'screening_only':bool(struct.get('screening_only'))}
            if struct.get('screening_only'):
                if screening is None:screening=screen_load(base).get('records',{})
                record=screening.get(canonical(row['code'])) or {}
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
            from exchange_sessions import latest_completed
            required=latest_completed(row['market'],now).isoformat()
            prior=(row.get('crosscheck') or {}).get('technical') or {}
            if (technical.get('status')!='OBSERVED' and prior.get('status')=='OBSERVED'
                    and prior.get('asof')==required
                    and (prior.get('provenance') or {}).get('input_sha256')):
                technical=deepcopy(prior)
                prior_provenance=technical['provenance']
                provenance={'source':prior_provenance.get('source'),'basis':prior_provenance.get('basis'),
                            'sha256':prior_provenance.get('source_sha256'),'source_asof':required}
            quality=stop_quality(row,technical)
            if technical.get('status')=='OBSERVED':
                anchor=next(iter(quality.get('anchors') or []),None)
                validated={'price':(row.get('stop_range') or [None])[0],
                           'validated':quality.get('anchor_status')=='confirmed',
                           'source':provenance['source'],'asof':technical.get('asof'),
                           'basis':row.get('price_plan_basis'),'code':row['code']}
                close=technical['structure'].get('reference_close')
                if close is None and bars:
                    close=bars[-1][2] if struct.get('screening_only') else bars[-1][3]
                technical['structure']['validated_stop']=(_stop_evidence(validated,
                    close=close,atr=technical['structure']['atr14'],basis=provenance['basis'],
                    asof=technical['asof'],now=now,code=row['code']) if close is not None else
                    {'status':'UNVERIFIED','reason':'缓存技术证据缺完整收盘参考值，不补造止损距离'})
                technical['structure']['validated_stop']['anchor']=anchor
            from muse_evidence import financial_facts
            raw_financial=financials.get(canonical(row['code'])) or {}
            expected_period=(row.get('fundamentals') or {}).get('period_end')
            old_financial=row.get('financial_evidence') or {}
            if raw_financial and (not expected_period or raw_financial.get('period_end')==expected_period):
                row['financial_evidence']=financial_facts(row,raw_financial)
            elif expected_period and old_financial.get('period_end')!=expected_period:
                row['financial_evidence']={'status':'STALE','period_end':expected_period,
                    'gaps':['财务缓存与本版报告期不一致，不跨期拼接']}
            from filing_source_facts import reconcile
            from structural_entry_plan import VERSION as structural_version
            fetch_filing = (review_entries and row.get('price_plan_version') == structural_version
                            and technical.get('status') == 'OBSERVED' and time.monotonic() < filing_deadline)
            comparison = reconcile(row,now,allow_fetch=fetch_filing)
            if comparison is None:
                cached = old_financial.get('growth_check') or {}
                if cached.get('code') == row.get('code') and cached.get('period_end') == expected_period and cached.get('periods'):
                    from filing_source_facts import compare
                    comparison = compare(row, cached['periods'], cached.get('source_url'),
                                         cached.get('published_at'), cached.get('sha256'))
            if comparison:
                row['financial_evidence'] = dict(row.get('financial_evidence') or {}, growth_check=comparison)
                row['fundamentals'] = dict(row['fundamentals'],growth_period=comparison['matched_period'],
                                           original_quarter_growth=comparison['periods'].get('单季'))
            context=alignment({**row,'nontechnical':q.get('nontechnical') or {}},snapshot,rotation,technical=technical,now=now)
            context.pop('evaluated_at',None)
            data={'version':VERSION,'asof':technical.get('asof') or struct.get('source_asof'),'provenance':provenance,
                  'technical':technical,'alignment':context,'stop_check':quality,
                  'price_indicators_are_one_family':True,
                  'model_calls':0,'rules_ready':bool(row.get('buyable') or row.get('rule_matched')),
                  'scope':'工程化证据核查，不是原书逐条复刻、独立多票或胜率证明'}
            # Bind cloud rule reviews to exact input facts and evidence, never an updated clock alone.
            row['crosscheck']=data
            if review_entries:apply_entry_review(row,now)
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
    if (context.get('stock_context') or {}).get('direction')=='down':
        gaps.append('多重核查：个股技术结构下行，同向下跌也不构成买入许可')
    # 大盘/板块方向不在这里再判一次：入场门（shadow_screen 环境门+轮转门+逆风试仓）
    # 是同一类价格证据的唯一裁决口径。这里重复用另一套定义（价格<MA60即"逆势"、
    # 缺板块代理即"证据未齐"）去否决，等于一个程序两套逻辑，且把"没有数据"当成
    # "有风险"。方向分歧只作披露，见 alignment_note()。
    stop=data.get('stop_check') or {}
    if stop.get('status')=='within_daily_noise':gaps.append('多重核查：原止损距参考入场不超过1个ATR，落在近期日波动内')
    # 与入场门同一口径：数据口径缺失/不可比（status=unknown）是"算不出"，只披露不拦截；
    # 同口径数据核出止损悬空（低点在止损之下）才是风险，照旧拦截。
    if stop.get('anchor_status')!='confirmed' and stop.get('status')!='unknown':
        gaps.append('多重核查：原止损锚尚无已确认结构支撑，不自动放宽止损')
    return list(dict.fromkeys(gaps))


def alignment_note(row):
    data=row.get('crosscheck') or {}
    if data.get('version')!=VERSION:return None
    context=data.get('alignment') or {};state=context.get('state','unknown')
    if state=='aligned':return None
    note=LABELS.get(state,LABELS['unknown'])
    if state=='countertrend':
        ex=context.get('exception') or {}
        note+='；'+('已有催化原文待人工/Muse复核' if ex.get('status')=='documented_for_review' else '暂无可追溯的逆势公司催化')
    return '方向披露（不作否决，环境由入场门裁决）：'+note


def _evidence_row(row):
    reg=row.get('astra_regime') or {}
    if reg.get('band')!='horizon' or reg.get('grade')!='buy':return False
    from horizon_book import live
    try:return bool(live(reg.get('cell'))[0])
    except Exception:return False


def apply_entry_review(row,now=None):
    row['missing']=[g for g in row.get('missing',[]) if not g.startswith('多重核查：')]
    note=alignment_note(row)
    if note:row['crosscheck_note']=note
    else:row.pop('crosscheck_note',None)
    gaps=entry_gaps(row,now)
    if _evidence_row(row):
        # 证据表买入级行：结构下行/止损在1个ATR内/止损锚未确认三项经全市场回测（同口径 GMMA/结构），
        # 命中子集的规则胜率仍在50–73%，只披露不拦截（2026-11-12 用户：可买概率<50%才是逻辑问题）；
        # 完整日线待更新是数据未到，照拦。
        shown=[g for g in gaps if g!='多重核查：完整日线待更新']
        if shown:
            row['crosscheck_note']='；'.join(([row['crosscheck_note']] if row.get('crosscheck_note') else [])
                                           +[g+'（披露：该规则回测含此情形胜率仍≥50%，不拦截）' for g in shown])
        gaps=[g for g in gaps if g not in shown]
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
    # 整个止损带必须在已确认支撑下方；只检查下沿会把带内悬空误认作有锚。
    # 旧口径要求低点恰好落进不足1%宽的止损带内，几乎从不成立；而"止损紧贴支撑
    # 之下"这一教科书形态反被判为无锚。低点在止损之下的（止损悬空）仍判无锚。
    asof=technical.get('asof')
    anchors=[p for p in structure.get('confirmed_lows',[]) if number(p.get('price'))
             and isinstance(p.get('date'),str) and isinstance(p.get('confirmed_at'),str)
             and isinstance(asof,str) and p['date']<=p['confirmed_at']<=asof
             and band[1]<=p['price']<=entry[0]]
    return {**basis,'status':'within_daily_noise' if distance<=1 else 'outside_one_atr',
        'anchor_status':'confirmed' if anchors else 'unverified','anchors':anchors,
        'distance_atr':round(distance,4),'atr14':atr,'reference_entry':entry[0],
        'stop_near_edge':band[1],'changes_stop':False,'anchor_type':'confirmed_swing_low',
        'validation_rule':'同复权口径；低点两侧各2根完整K线确认且确认日不晚于技术截止日；止损上沿不高于支撑低点，支撑不高于入场下沿',
        'scope':'原入场下沿到止损上沿的保守距离；1ATR为工程风险提示，非保证止损不触发；无持仓量不虚构账户损失'}


def review_hash(row):
    data=row.get('crosscheck') or {}
    return digest({'facts':decision_facts(row),'technical':data.get('technical'),
                   'alignment':data.get('alignment'),'stop_check':data.get('stop_check')})
