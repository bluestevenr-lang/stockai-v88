"""Versioned evidence contributions for observation lists, without grade authority.

The unequal weights and point rules are V88 product parameters, not quotations
from classics, calibrated probabilities, model reviews or signed trade plans.
Missing components stay null and keep their original weight unallocated.
"""
from collections import defaultdict
from datetime import date, datetime, timezone
from html import escape
import math
import re

VERSION='observation-score-v1.0'
WEIGHTS={'capital':25,'business':30,'trend':20,'entry':15,'volume':10}
LABELS={'capital':'资本/结构失效边界','business':'经核非技术经营','trend':'趋势结构',
        'entry':'入场观察时机','volume':'量能'}
RULES={
    'capital':'观察线已触及=0；距观察线≤4%=90、≤8%=75、≤15%=50，其余25。观察线不是合同止损；未核目标不计算RR。',
    'business':'已核经营支持基准80，中性或仅有反证基准50；每项独立反证扣30，最低0。标题、供应商字段和过期报告不计入。',
    'trend':'已核MA20≥MA60且价≥MA20=85，价在MA20下=55；MA20<MA60且价≥MA20=45，否则20。仅企稳窗口：突破75、企稳55、未稳20。',
    'entry':'未过度急涨的突破80、距高点3%内且守MA20为70、贴近MA20为65，其余35；5日涨幅≥12%或单日≥8%为20。仅企稳窗口：突破80、企稳55、未稳20。',
    'volume':'20日量比1.2–3=80，0.8–1.2=60，0.5–0.8=40，低于0.5=20；高于3为40。已核下跌日且量比≥1.2时减至20，放量下跌不算确认优势。',
}
VERIFICATIONS={'issuer_report_verified','primary_document_read_and_capture_rechecked'}
BUSINESS_KINDS={'issuer_financial_observation','primary_earnings','primary_business',
                'primary_capital_return','primary_catalyst','primary_statement'}


def finite(value):
    return type(value) in (int,float) and math.isfinite(value)


def _hash(value):
    return isinstance(value,str) and re.fullmatch('[0-9a-fA-F]{64}',value) is not None


def _date(value):
    try:return date.fromisoformat(str(value)[:10])
    except (ValueError,TypeError):return None


def _identity(row):
    from market_symbols import canonical
    if not isinstance(row.get('code'),str) or not row['code']:return False
    code=canonical(row['code'])
    market=('A股' if re.fullmatch(r'\d{6}\.(?:SS|SZ|BJ)',code) else
            '港股' if re.fullmatch(r'\d{1,5}\.HK',code) else
            '美股' if re.fullmatch(r'[A-Z][A-Z0-9.$_]{0,14}',code) else None)
    return market is not None and row.get('market')==market


def _factor(key,value=None,*,inputs=None,hashes=(),asof=None,missing=(),penalties=()):
    if not finite(value) or not 0<=value<=100:value=None
    weight=WEIGHTS[key]
    return {'id':key,'label':LABELS[key],'weight':weight,'value':value,
            'contribution':round(weight*value/100,4) if value is not None else None,
            'inputs':inputs or {},'source_hashes':sorted(set(h for h in hashes if _hash(h))),
            'source_asof':asof,'missing':list(missing),'penalties':list(penalties),'rule':RULES[key]}


def evidence_from_bars(bars,market,source_hash):
    """Derive common observation metrics from caller-verified, continuous bars.

    Caller must have checked source bytes, identity, basis and DB equality.
    This adds a separate observation projection, never changes the original row.
    """
    from history_calendar import coverage
    if len(bars)<61 or not _hash(source_hash):return None
    recent=bars[-61:]
    if not coverage([b[1] for b in recent],market,recent[0][1],recent[-1][1])['continuity_ok']:return None
    if any(not all(finite(b[i]) and b[i]>0 for i in (3,4,5,6)) for b in recent):return None
    closes=[b[3] for b in recent];average=sum(b[6] for b in recent[-21:-1])/20
    return {'version':VERSION,'verified':True,'last':closes[-1],
            'ma20':sum(closes[-20:])/20,'ma60':sum(closes[-60:])/60,
            'previous_high20':max(b[4] for b in recent[-21:-1]),
            'low5':min(b[5] for b in recent[-5:]),
            'return5_pct':100*(closes[-1]/closes[-6]-1),
            'change_pct':100*(closes[-1]/closes[-2]-1),
            'volume_ratio20':recent[-1][6]/average if average>0 else None,
            'source_asof':recent[-1][1],'raw_sha256':source_hash}


def _technical(row,now):
    from exchange_sessions import latest_completed
    extra=row.get('observation_evidence') or {}
    structure=row.get('structure') or {}
    if extra.get('version')==VERSION and extra.get('verified') is True:
        evidence=extra;verified=True
    elif structure:
        evidence=structure;verified=row.get('structural_verified') is True
    else:
        evidence=row;verified=bool(row.get('signals'))
    source_hash=evidence.get('raw_sha256') or row.get('raw_sha256')
    asof=evidence.get('source_asof') or row.get('source_asof')
    stamp=_date(asof);market=row.get('market')
    try:required=latest_completed(market,now) if market in ('A股','港股','美股') else None
    except (ValueError,TypeError):required=None
    missing=[]
    if not _identity(row):missing.append('证券代码与市场身份待核')
    if not verified or not _hash(source_hash):missing.append('同源完整日线核验凭据待补')
    if stamp is None or stamp!=required or row.get('data_fresh') is False:missing.append('日线未核至最近完整交易日')
    last=row.get('last') if 'last' in row else evidence.get('last')
    if not finite(last) or last<=0:missing.append('有效现价待核')
    if finite(evidence.get('last')) and evidence['last']<=0:missing.append('观察日线价格无效')
    if finite(row.get('last')) and finite(evidence.get('last')) and evidence['last']>0 and abs(row['last']/evidence['last']-1)>.01:
        missing.append('观察价与同源日线价格不一致')
    return evidence,last,source_hash,str(asof or ''),missing


def _business(row,now):
    nt=row.get('nontechnical') or {};verified=[];seen=set()
    from market_symbols import canonical
    if not _identity(row) or not nt.get('code') or canonical(nt['code'])!=canonical(row.get('code') or ''):
        return _factor('business',missing=['企业证据证券身份待核或与本股不一致'])
    for item in nt.get('evidence') or []:
        if not isinstance(item,dict):continue
        if item.get('verification') not in VERIFICATIONS or item.get('eligible_support') is False:continue
        kind=item.get('kind') or ''
        if kind not in BUSINESS_KINDS:continue
        asof=_date(item.get('asof'));published=_date(item.get('published_at'));available=_date(item.get('available_at'))
        if (not _hash(item.get('source_sha256')) or item.get('current') is not True or asof is None
                or not 0<=(now.date()-asof).days<=210 or available is None
                or available>now.date() or available<asof
                or published is not None and (published>now.date() or published<asof)):continue
        try:
            clock=datetime.fromisoformat(str(item['available_at']).replace('Z','+00:00'))
            if clock.tzinfo is None or clock>now:continue
            if item.get('published_at') and len(str(item['published_at']))>10:
                published_clock=datetime.fromisoformat(str(item['published_at']).replace('Z','+00:00'))
                if published_clock.tzinfo is None or published_clock>now:continue
        except (ValueError,TypeError):continue
        direction=item.get('direction')
        if direction not in ('support','neutral','counterevidence'):continue
        numeric=('value','prior_value','yoy_pct')
        if any(k in item and item[k] is not None and not finite(item[k]) for k in numeric):continue
        key=(item.get('source_sha256'),item.get('metric') or item.get('claim'),direction)
        if key in seen:continue
        seen.add(key);verified.append(item)
    if not verified:return _factor('business',missing=['经核且在观察期内的企业经营证据待补；标题/供应商字段不计分'])
    support=[e for e in verified if e['direction']=='support'];counter=[e for e in verified if e['direction']=='counterevidence']
    base=80 if support else 50;deduction=min(base,30*len(counter))
    inputs={'support_count':len(support),'counterevidence_count':len(counter),'base_points':base,
            'counterevidence_deduction':deduction,'search_fresh':nt.get('search_fresh') is True,
            'observations':[{'claim':e.get('claim'),'direction':e['direction'],'metric':e.get('metric'),
                             'value':e.get('value'),'yoy_pct':e.get('yoy_pct'),'asof':e['asof']} for e in verified]}
    gaps=[] if nt.get('search_fresh') is True else ['后续公告待检；已核报告事实未因此作废']
    if any(not e.get('published_at') for e in verified):gaps.append('原公告精确发布时间待核；以真实首次留存时间限定可用性')
    return _factor('business',base-deduction,inputs=inputs,hashes=[e['source_sha256'] for e in verified],
                   asof=max(e['asof'] for e in verified),
                   missing=gaps,
                   penalties=[f'经核反证扣{deduction}分：'+'；'.join(e.get('claim') or '已核反证' for e in counter)] if counter else [])


def assess(row,*,now=None):
    now=now or datetime.now(timezone.utc)
    if now.tzinfo is None:raise ValueError('observation scoring requires an aware clock')
    evidence,last,source_hash,asof,missing=_technical(row,now)
    factors={key:_factor(key,asof=asof,hashes=[source_hash],missing=missing or ['本项已核输入待补'])
             for key in ('capital','trend','entry','volume')}
    if not missing:
        floor=row.get('invalidation') if finite(row.get('invalidation')) else evidence.get('low5')
        if finite(floor) and floor>0:
            distance=(last-floor)/last*100
            value=0 if distance<=0 else 90 if distance<=4 else 75 if distance<=8 else 50 if distance<=15 else 25
            factors['capital']=_factor('capital',value,inputs={'last':last,'observation_floor':floor,'distance_pct':round(distance,4),
                'scope':'观察失效线，非合同止损；无可核目标不计算RR'},asof=asof,hashes=[source_hash],
                penalties=['观察失效线已触及，资本项为0'] if distance<=0 else [])
        ma20,ma60=evidence.get('ma20'),evidence.get('ma60')
        state=row.get('setup_state');states={'breakout_watch':75,'stabilizing':55,'falling':20}
        if finite(ma20) and finite(ma60) and min(ma20,ma60)>0:
            value=(85 if last>=ma20 else 55) if ma20>=ma60 else (45 if last>=ma20 else 20)
            factors['trend']=_factor('trend',value,inputs={'last':last,'ma20':ma20,'ma60':ma60},asof=asof,hashes=[source_hash])
        elif state in states:
            factors['trend']=_factor('trend',states[state],inputs={'setup_state':state,'scope':'已核短线企稳窗口，不代替长期趋势'},asof=asof,hashes=[source_hash])
        pivot=evidence.get('previous_high20');ret=evidence.get('return5_pct');change=row.get('change_pct')
        if finite(pivot) and pivot>0 and finite(ret) and finite(ma20) and ma20>0:
            value=(20 if ret>=12 or finite(change) and change>=8 else
                   80 if last>=pivot else 70 if .97<=last/pivot<1 and last>=ma20 and ret<8 else
                   65 if .99<=last/ma20<=1.025 and ret<=5 else 35)
            factors['entry']=_factor('entry',value,inputs={'last':last,'previous_high20':pivot,'ma20':ma20,'return5_pct':ret},asof=asof,hashes=[source_hash])
        elif state in states and finite(row.get('pivot')) and row['pivot']>0:
            factors['entry']=_factor('entry',{'breakout_watch':80,'stabilizing':55,'falling':20}[state],
                inputs={'last':last,'pivot':row['pivot'],'setup_state':state},asof=asof,hashes=[source_hash])
        ratio=evidence.get('volume_ratio20')
        if ratio is None:ratio=row.get('volume_ratio20')
        daily_change=evidence.get('change_pct') if finite(evidence.get('change_pct')) else row.get('source_day_change_pct')
        if finite(ratio) and ratio>0 and not finite(daily_change):
            factors['volume']=_factor('volume',asof=asof,hashes=[source_hash],missing=['同源完整日线涨跌方向待核；不借盘中涨跌幅推断'])
        if finite(ratio) and ratio>0 and finite(daily_change):
            value=80 if 1.2<=ratio<=3 else 60 if .8<=ratio<1.2 else 40 if ratio>=.5 else 20
            down_day=daily_change<0
            penalties=['异常放量超过3倍，仅计40分，需核事件与持续性'] if ratio>3 else []
            if down_day and ratio>=1.2:
                penalties=[f'已核下跌日放量，量能分项扣{value-20:g}分至20分'];value=20
            factors['volume']=_factor('volume',value,inputs={'volume_ratio20':ratio,'down_day':bool(down_day),'source_day_change_pct':daily_change},asof=asof,hashes=[source_hash],penalties=penalties)
    factors['business']=_business(row,now)
    ordered=[factors[key] for key in WEIGHTS]
    covered=sum(f['weight'] for f in ordered if f['value'] is not None)
    contribution=round(sum(f['contribution'] for f in ordered if f['contribution'] is not None),2)
    return {'version':VERSION,'code':row.get('code'),'market':row.get('market'),
            'status':'complete' if covered==100 else 'stale' if missing and any('最近完整' in m for m in missing) else 'partial',
            'verified_contribution':contribution,'coverage_pct':covered,'total':contribution if covered==100 else None,
            'factors':ordered,'source_asof':asof,'missing':[f['label'] for f in ordered if f['value'] is None],
            'no_grade_authority':True,'model_review':'待真实中央双审','probability':False,
            'formula':'已核贡献=Σ(已核分项×原权重)；缺项不补分、不重新分配权重；100%覆盖才形成复合观察分。',
            'scope':'短期观察；权重与分档为V88参数，经典原则不是数值出处；不替代中央评级、原止损或退出合同。'}


def rank_comparable(rows,*,now=None):
    """Reorder only comparable current-score slots; keep unverified cohorts."""
    result=list(rows);groups=defaultdict(list);scores={}
    for i,row in enumerate(result):
        score=assess(row,now=now);scores[i]=score
        if score['coverage_pct']>0 and score['status']!='stale':
            known=tuple(f['id'] for f in score['factors'] if f['value'] is not None)
            groups[(row.get('market'),score['coverage_pct'],known,score['source_asof'])].append(i)
    for positions in groups.values():
        ordered=sorted(positions,key=lambda i:-scores[i]['verified_contribution'])
        originals=[result[i] for i in ordered]
        for i,row in zip(positions,originals):result[i]=row
    return result


def watch_order(rows,*,now=None):
    """Use the producer's original order as a tie-breaker, never as a score."""
    ordered=sorted(rows,key=lambda row:row['watch_rank'] if finite(row.get('watch_rank')) and row['watch_rank']>0 else math.inf)
    return rank_comparable(ordered,now=now)


def _score(row_or_score,now):
    if row_or_score.get('version')!=VERSION or 'factors' not in row_or_score:return assess(row_or_score,now=now)
    # A saved display object has no review authority. Reject changed weights,
    # contributions, missingness, totals or coverage instead of trusting it.
    factors=row_or_score.get('factors')
    valid=(isinstance(factors,list) and len(factors)==len(WEIGHTS)
           and _identity(row_or_score))
    clock=now or datetime.now(timezone.utc)
    from exchange_sessions import latest_completed
    expected_session=latest_completed(row_or_score.get('market'),clock) if _identity(row_or_score) else None
    seen=set();covered=0;contribution=0
    for factor in factors if isinstance(factors,list) else []:
        if not isinstance(factor,dict):valid=False;continue
        key=factor.get('id');value=factor.get('value');weight=factor.get('weight')
        if key not in WEIGHTS or key in seen or type(weight) is not int or weight!=WEIGHTS.get(key):valid=False;continue
        seen.add(key)
        if value is None:
            if factor.get('contribution') is not None:valid=False
            continue
        if not finite(value) or not 0<=value<=100:valid=False;continue
        asof=_date(factor.get('source_asof'))
        if asof is None or (key!='business' and asof!=expected_session) or (key=='business' and not 0<=(clock.date()-asof).days<=210):valid=False
        expected=round(weight*value/100,4)
        if not finite(factor.get('contribution')) or factor['contribution']!=expected:valid=False
        if not factor.get('source_hashes') or not all(_hash(h) for h in factor['source_hashes']):valid=False
        contribution+=expected;covered+=weight
    contribution=round(contribution,2)
    valid=(valid and seen==set(WEIGHTS) and finite(row_or_score.get('coverage_pct'))
           and row_or_score['coverage_pct']==covered and finite(row_or_score.get('verified_contribution'))
           and row_or_score['verified_contribution']==contribution
           and row_or_score.get('total')==(contribution if covered==100 else None)
           and (row_or_score.get('status')=='complete')==(covered==100))
    return row_or_score if valid else assess({'code':row_or_score.get('code'),'market':row_or_score.get('market')},now=now)


def summary(row_or_score,*,now=None):
    score=_score(row_or_score,now);points=f"{score['verified_contribution']:g}"
    business=next(f for f in score['factors'] if f['id']=='business')
    deduction=(business.get('inputs') or {}).get('counterevidence_deduction') or 0
    penalty=f" · 经营反证扣{deduction*WEIGHTS['business']/100:g}分" if finite(deduction) and deduction>0 else ''
    return (f"复合观察分 {points}/100" if score['total'] is not None else
            f"已核贡献 {points}分 · 覆盖{score['coverage_pct']}%")+penalty+' · 待双审'


def details_html(row_or_score,*,now=None):
    score=_score(row_or_score,now);e=lambda value:escape(str(value),quote=True)
    out=[f"<div class='observation-score'><b>{e(summary(score,now=now))}</b>",
         "<details><summary>观察评分依据 · 权重与缺项</summary>",
         f"<small>{e(score['version'])} · {e(score['formula'])}<br>{e(score['scope'])}</small>",
         "<table style='width:100%;min-width:680px'><thead><tr><th>因子/权重</th><th>分值/贡献</th><th>输入与证据</th></tr></thead><tbody>"]
    for factor in score['factors']:
        value='待核' if factor['value'] is None else f"{factor['value']:g}/100 → {factor['contribution']:g}分"
        labels={'last':'价格','observation_floor':'观察失效线','distance_pct':'距观察线(%)','scope':'适用范围',
                'ma20':'20日均价','ma60':'60日均价','setup_state':'短线阶段','previous_high20':'前20日高点',
                'return5_pct':'5日涨跌幅(%)','pivot':'观察突破位','volume_ratio20':'20日量比',
                'support_count':'已核支持项','counterevidence_count':'独立反证项','base_points':'经营基准分',
                'counterevidence_deduction':'经营分项扣分','search_fresh':'后续公告检索'}
        labels['down_day']='当前完整交易日下跌'
        labels['source_day_change_pct']='同源收盘日涨跌幅(%)'
        stage={'falling':'未确认企稳','stabilizing':'企稳迹象','breakout_watch':'突破待验'}
        inputs=[]
        for key,item in factor['inputs'].items():
            if key=='observations':
                inputs.extend(e(({'support':'支持','counterevidence':'反证','neutral':'中性'}.get(v.get('direction'),'待核'))+'：'+str(v.get('claim') or '')+' · '+str(v.get('asof') or '')) for v in item)
                continue
            shown=(('是' if item else '否') if key=='down_day' else ('已核' if item else '待检')) if isinstance(item,bool) else stage.get(item,item) if isinstance(item,str) else f'{item:g}' if finite(item) else '待核'
            inputs.append(e(labels.get(key,key)+'：'+str(shown)))
        receipts=("<details><summary>来源凭据 · "+e(factor['source_asof'] or '日期待核')+"</summary><small>SHA256<br>"+'<br>'.join(e(h) for h in factor['source_hashes'])+'</small></details>') if factor['source_hashes'] else ''
        details=[e(factor['rule']),'<br>'.join(inputs),e('；'.join(factor['penalties']+factor['missing'])),receipts]
        out.append(f"<tr><td>{e(factor['label'])} {factor['weight']}%</td><td>{value}</td><td style='overflow-wrap:anywhere'>"+'<br>'.join(x for x in details if x)+'</td></tr>')
    out.append('</tbody></table></details></div>')
    return ''.join(out)
