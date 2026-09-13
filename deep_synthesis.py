"""One evidence-bound deep-analysis conclusion; never grants a central grade."""
from html import escape
import hashlib
import json
import math

VERSION='deep-synthesis-v1'
DOMAINS={'technical':'技术量价','sector':'板块轮动','market':'市场环境','news':'新闻催化',
         'flow':'资金流','fundamental':'基本面','valuation':'估值','validation':'历史与前瞻检验'}


def observations(frame):
    out={'bars':0,'source_asof':None,'last':None,'ma200':None,'rsi14':None,
         'return1_pct':None,'volume_ratio20':None,'annual_position':'年线数据不足','momentum':'RSI数据不足'}
    if frame is None or frame.empty or 'Close' not in frame:return out
    c=frame.Close.astype(float)
    if c.isna().any() or not all(math.isfinite(x) and x>0 for x in c):return out
    out.update(bars=len(c),source_asof=str(frame.index[-1])[:10],last=float(c.iloc[-1]))
    if len(c)>=200:
        out['ma200']=float(c.tail(200).mean())
        out['annual_position']='年线上方' if c.iloc[-1]>out['ma200'] else '年线下方' if c.iloc[-1]<out['ma200'] else '年线附近'
    if len(c)>=15:
        delta=c.diff().tail(14);gain=float(delta.clip(lower=0).mean());loss=float(-delta.clip(upper=0).mean())
        out['rsi14']=100*gain/(gain+loss) if gain+loss else 50.
        r=out['rsi14'];out['momentum']='超买' if r>70 else '动量偏强' if r>50 else '动量偏弱' if r>30 else '超卖'
    if len(c)>=2:out['return1_pct']=float((c.iloc[-1]/c.iloc[-2]-1)*100)
    if len(c)>=21 and 'Volume' in frame:
        v=frame.Volume.astype(float);prior=float(v.iloc[-21:-1].mean())
        if prior>0 and math.isfinite(float(v.iloc[-1])):out['volume_ratio20']=float(v.iloc[-1])/prior
    return out


def build(context, frame, quality, technical, trend, cross):
    """Only actual signed domain verdicts may support a company-level claim."""
    row=context.get('row') or {};fact=context.get('fact') or {};selection=context.get('selection') or {}
    obs=observations(frame);turn=trend.get('turning') or {};card=context.get('card') or {}
    checks={x['id']:x['status'] for x in (cross or {}).get('checks',[])}
    price_coherent=all(checks.get(k)=='pass' for k in ('identity','binding','session','basis','facts','quant'))
    from review_contract import known_protocol
    g=(row.get('reviews') or {}).get('gpt') or {};pair=g.get('review_pair') or {}
    annex=row.get('joint_evidence') or {};domains=annex.get('domains') or {}
    reviewed=bool(annex and annex==fact.get('joint_evidence') and known_protocol(g,row)
                  and set(pair)=={'primary','counteraudit'} and (card.get('gpt') or {}).get('current')
                  and (card.get('gpt') or {}).get('complete') and row.get('factpack_id')==selection.get('factpack_id')
                  ==context.get('loaded_factpack_id'))
    table=[];unresolved=[]
    for key,title in DOMAINS.items():
        d=domains.get(key) or {};votes=[]
        for role in ('primary','counteraudit'):
            v=next((v for v in pair.get(role,{}).get('domain_reviews',[]) if v.get('domain')==key),{}) if reviewed else {}
            votes.append({'role':role,'conclusion':v.get('conclusion','未完成同包复审'),
                          'reason':v.get('reason','不能用单项技术分替代该域结论'),'effects':v.get('effects',[])})
        state=d.get('status','missing') if reviewed else 'unreviewed'
        if state in ('missing','limited','conflict','unreviewed') or any(v['conclusion'] in ('反对','阻断','混合','缺失') for v in votes):unresolved.append(title)
        table.append({'domain':key,'title':title,'status':state,'reviews':votes,
                      'limitations':d.get('limitations') or ['尚无同包已审核的本票证据'],
                      'origins':d.get('origins') or []})
    adverse=bool(turn.get('side')=='top' or turn.get('mixed'))
    if adverse:
        short=obs['momentum']+' · 短线转弱预警'
        explanation=f"{obs['annual_position']}、{obs['momentum']}描述较长窗口；最新1–3日出现转弱证据。二者可同时成立，综合结论是短线承压，尚不能确认长期反转。"
    elif turn.get('side')=='bottom':
        short=obs['momentum']+' · 短线转强待确认'
        explanation=f"{obs['annual_position']}、{obs['momentum']}与短线转强信号需分周期核对；转强预警不等于基本面改善或底部确认。"
    else:
        short=obs['momentum'];explanation=f"{obs['annual_position']}、{obs['momentum']}；本组规则未触发短线转折预警，不据此保证趋势延续。"
    finance=next(d for d in table if d['domain']=='fundamental')
    value=next(d for d in table if d['domain']=='valuation')
    firm_confirmed=all(d['status']=='observed' and all(v['conclusion']=='支持' for v in d['reviews']) for d in (finance,value))
    business='基本面与估值已有同包支持证据，仍须对应原合同周期。' if firm_confirmed else '基本面/估值证据未形成完整共同支持；不得将年线上方、RSI或低价位写成企业价值强。'
    blocks=[]
    if not price_coherent:blocks.append('行情、周期或签名交叉核对未通过')
    if technical.get('cycle_conflict'):blocks.append('短中长周期量价判断存在分歧，不能称为全周期强势')
    if adverse:blocks.append('短线转弱预警需复核，不能以趋势分抵消')
    if not reviewed:blocks.append('八域同包联合复审未完整或过期')
    if any(d['status']=='conflict' or any(v['conclusion'] in ('反对','阻断') for v in d['reviews']) for d in table):blocks.append('存在反对或冲突证据，须按原文逐项解决')
    if row.get('horizon') in ('medium','long') and not firm_confirmed:blocks.append('中长期企业价值与估值验证不足')
    if checks.get('invalidation')=='gap':blocks.append('原失效线或合同有效性须先核对')
    status='存在分歧·复核优先' if blocks else '证据已关联·等待原条件' if not context.get('formal') else '证据已关联·仍按中央执行闸'
    result={'version':VERSION,'code':context.get('code'),'factpack_id':selection.get('factpack_id'),
            'source_asof':obs['source_asof'],'snapshot_signature':quality.get('snapshot_signature'),
            'price_coherent':price_coherent,'joint_review_current':reviewed,'observations':obs,
            'technical_cycle':{k:technical.get(k) for k in ('short_score','medium_score','long_score','cycle_conflict','cycle_status','score_version')},
            'annual_label':obs['annual_position'],'momentum_label':short,'turning':turn,
            'status':status,'explanation':explanation,'business_conclusion':business,
            'domains':table,'unresolved_domains':unresolved,'review_blocks':blocks,
            'central_grade':(cross or {}).get('current_grade'),'central_audit_score':(cross or {}).get('audit_score'),
            'original_plan':row.get('trade_plan') or {},'no_grade_authority':True,'model_calls':0,
            'entry_recheck_required':bool(blocks),'entry_permission':False}
    result['input_id']=hashlib.sha256(json.dumps(result,sort_keys=True,ensure_ascii=False,allow_nan=False).encode()).hexdigest()
    return result


def html(doc,*,details=True):
    if not doc:return '<div>联合结论未生成；不输出新的买卖判断。</div>'
    e=lambda v:escape(str(v if v is not None else '未核实'))
    text=f'<b>🧭 统一研判 · {e(doc["status"])}</b><div>{e(doc["explanation"])}</div>'
    from evidence_visuals import domain_strip
    text += domain_strip(doc)
    text+='<details><summary>基本面结论与当前约束</summary><div>'+e(doc['business_conclusion'])+'</div>'
    if doc['review_blocks']:text+='<div>当前约束：'+e('；'.join(doc['review_blocks']))+'</div>'
    text+='</details>'
    if details:
        states={'observed':'🔵 可核对观察','limited':'🟠 有限证据','missing':'⚪ 缺失','conflict':'🔴 冲突','unreviewed':'⚪ 未完成当前复审'}
        text+='<details><summary>技术、基本面与其他证据如何共同影响结论</summary><div style="overflow-x:auto"><table style="min-width:760px;width:100%;font-size:11px"><tr><th>证据域</th><th>来源状态</th><th>主审 / 独立反审及影响</th><th>仍需核实</th></tr>'
        for d in doc['domains']:
            votes='；'.join(('主审' if v['role']=='primary' else '反审')+'：'+v['conclusion']+'，'+v['reason'] for v in d['reviews'])
            text+='<tr>'+''.join('<td style="vertical-align:top;border:1px solid #e2e8f0;padding:5px">'+e(v)+'</td>' for v in (d['title'],states[d['status']],votes,'；'.join(d['limitations'])))+'</tr>'
        text+='</table></div></details>'
    text+=f'<div style="font-size:11px;color:#64748b">行情日 {e(doc["source_asof"])} · 结论 {e(doc["input_id"][:12])} · 本次模型调用0。原目标、失效价及期限不变；预警不等于确认顶部/底部。</div>'
    return '<section class="v88-deep-synthesis" style="font-size:12px;line-height:1.6;padding:9px;margin:8px 0;border:1px solid #cbd5e1;border-radius:6px">'+text+'</section>'
