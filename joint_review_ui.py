"""Use the central signed reviews directly on every research surface."""
from html import escape
from review_contract import JOINT_VERSION,known_protocol

def html(selection,code=None):
    esc=lambda x:escape(str(x if x is not None else '—'))
    rows={r['code']:r for b in ('recommendations','preparations','blocked_3a','conditional','observations','pending','excluded') for r in selection.get(b,[])}
    signed=[r for r in rows.values() if isinstance(r.get('joint_evidence'),dict)]
    if not code:
        return "<div class='v88-joint-summary' style='font-size:12px;color:#475569;margin:5px 0'>联合复审 · "+str(len(signed))+"只已纳入八域事实 · 主审与反审逐项列支持、反对和缺失；详细影响见个股。数据齐全和审核通过分别验收。</div>"
    r=rows.get(code,{})
    if not r.get('joint_evidence'):
        return "<div class='v88-joint-review' style='font-size:12px;color:#b45309'>八域联合复审尚未完成；原分数不是新版联合复审结果。</div>"
    a=r['joint_evidence'];g=(r.get('reviews') or {}).get('gpt') or {};pair=g.get('review_pair') or {}
    valid=known_protocol(g,r) and set(pair)=={'primary','counteraudit'} and all(len(m.get('domain_reviews',[]))==8 for m in pair.values())
    from review_display import fresh
    current=valid and fresh(g.get('at')) and r.get('factpack_id')==selection.get('factpack_id')
    label='八域联合复审 · '+('双审回执完整' if valid else '回执待完成')+('' if current else ' · 当前不得执行')
    status={'observed':'可核对观察','limited':'有限证据','missing':'缺失','conflict':'冲突'}
    names={'facts':'事实','thesis':'逻辑','countercase':'反证','horizon':'周期','risk':'风险'}
    body='<div>两个审核者使用同一冻结事实且互不查看对方意见；来源相同不算独立数据。以下影响已进入各自五项评分，中央仍取保守审核与书理约束。</div>'
    if r.get('research_origin'):body+='<div>'+esc(r['research_origin'].get('meaning'))+'</div>'
    body+="<div style='overflow-x:auto'><table style='font-size:11px;min-width:780px;width:100%;border-collapse:collapse'><tr><th>证据域 / 数据</th><th>主审：判断及影响</th><th>独立反审：判断及影响</th><th>缺口 / 约束</th></tr>"
    for key,d in a['domains'].items():
        cols=[d['title']+' · '+status.get(d['status'],d['status'])]
        for role in ('primary','counteraudit'):
            v=next((x for x in pair.get(role,{}).get('domain_reviews',[]) if x['domain']==key),{})
            cols.append(str(v.get('conclusion','待复核'))+' → '+ '、'.join(names.get(k,k) for k in v.get('effects',[]))+'：'+str(v.get('reason','')))
        cols.append('；'.join(d.get('limitations',[])))
        body+='<tr>'+''.join('<td style="padding:5px;border:1px solid #e2e8f0;vertical-align:top">'+esc(x)+'</td>' for x in cols)+'</tr>'
    body+='</table></div><div>审核时间 '+esc(g.get('at'))+'；协议 '+esc(g.get('review_schema_version'))+'；原价位、失效线和期限不因联合复审自动改变。</div>'
    return "<details class='v88-joint-review' style='font-size:12px;margin:6px 0'><summary>"+esc(label)+'</summary>'+body+'</details>'
