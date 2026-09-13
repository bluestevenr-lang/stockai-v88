"""Independent monthly workspace; all rendering is local and read-only."""
from datetime import datetime, timezone, timedelta
from html import escape
import math
from urllib.parse import urlencode

BJT=timezone(timedelta(hours=8))


def _policy_now(now=None):
    now=now or datetime.now(BJT)
    if now.tzinfo is None or now.utcoffset() is None:return None
    return now.astimezone(BJT)


def text(value):
    return escape(str(value if value is not None and value != '' else '—'))


def number(value, decimals=2):
    return f'{value:,.{decimals}f}' if type(value) in (int,float) and math.isfinite(value) else '未核实'


def band(value):
    return '～'.join(number(v,4).rstrip('0').rstrip('.') for v in value) if isinstance(value,(list,tuple)) and len(value)==2 else '尚缺原合同'


def verified_progress(doc, *, now=None):
    now=_policy_now(now)
    if now is None:return False
    ms=doc.get('month_state') or {};target=(doc.get('policy') or {}).get('monthly_target')
    net=ms.get('realized_net_pnl')
    try:
        stamp=datetime.fromisoformat(str(ms.get('reconciled_at')).replace('Z','+00:00'))
        return (current_document(doc,now=now) and not doc.get('private_redacted') and ms.get('state') in ('OPEN','TARGET_DONE','MONTH_STOPPED','ENTRY_LIMIT','RISK_RESERVED')
                and doc.get('month')==now.strftime('%Y-%m') and stamp.tzinfo is not None
                and timedelta(0)<=now-stamp<=timedelta(hours=24)
                and type(net) in (int,float) and math.isfinite(net)
                and type(target) in (int,float) and math.isfinite(target) and target>0)
    except (ValueError,TypeError):return False


def current_document(doc,*,now=None):
    now=_policy_now(now)
    if now is None:return False
    try:
        stamp=datetime.fromisoformat(str(doc.get('generated_at')).replace('Z','+00:00'))
        return (doc.get('month')==now.strftime('%Y-%m') and stamp.tzinfo is not None
                and timedelta(0)<=now-stamp<=timedelta(hours=24))
    except (ValueError,TypeError):return False


def summary_html(doc):
    p=doc.get('policy') or {};ms=doc.get('month_state') or {};private=doc.get('private_redacted') is True
    target=p.get('monthly_target');net=ms.get('realized_net_pnl') if not private else None
    known=verified_progress(doc)
    pct=max(0,min(100,net/target*100)) if known else 0
    status={'OPEN':'🔎 可继续筛选','LEDGER_UNVERIFIED':'🧾 成交进度待对账','TARGET_DONE':'🏁 本月已锁定收工',
            'MONTH_STOPPED':'⛔ 本月停止新增','ENTRY_LIMIT':'⏸ 已达本月开仓次数','RISK_RESERVED':'🛡 风险预算已占用'}.get(ms.get('state'),'🔎 月度状态待同步')
    if private:status='🔎 公开研究视图 · 实际账户仅私域显示'
    elif not known:status='🧾 成交进度待对账'
    currency=p.get('target_currency') or 'USD'
    prefix='$' if currency=='USD' else text(currency)+' '
    progress_attrs=(' role="progressbar" aria-label="本月已对账净收益目标进度" aria-valuemin="0" aria-valuemax="100" aria-valuenow="'+str(pct)+'"' if known else ' role="img" aria-label="实际净收益尚未核实"')
    return ('<section id="v88-astra-monthly" class="v88-astra-monthly" style="scroll-margin-top:58px;border:1px solid #cbd5e1;border-left:4px solid #4f46e5;border-radius:8px;padding:12px;margin:12px 0;background:#f8faff">'
            '<div style="font-size:16px;font-weight:700;color:#3730a3">🎯 Astra · 月度短线计划</div>'
            '<div style="font-size:11px;color:#64748b;margin-top:4px">'+text(doc.get('month'))+' · GPT-6 Astra + 经典书籍</div>'
            '<div style="display:flex;gap:18px;flex-wrap:wrap;margin:10px 0;font-size:12px">'
            '<div>🎯 本月净利润目标 ('+text(currency)+')<br><b style="font-size:20px;color:#3730a3">'+prefix+number(target,0)+'</b></div>'
            '<div>✅ 已实现净收益<br><b style="font-size:16px;color:'+('#b91c1c' if known and net<0 else '#166534')+'">'+(prefix+number(net) if known else '未核实')+'</b></div>'
            '<div>📍 距离目标<br><b style="font-size:16px">'+(prefix+number(max(0,target-net)) if known else '待成交对账')+'</b></div></div>'
            '<div'+progress_attrs+' style="height:5px;background:#e2e8f0;border-radius:4px"><div style="height:5px;width:'+str(pct)+'%;background:#6366f1;border-radius:4px"></div></div>'
            '<div style="font-size:11px;color:#475569;margin-top:7px">'+text(status)+' · '+('进度 '+number(pct,1)+'%' if known else '未对账不按0收益计算')+'</div>'
            '<div style="font-size:11px;color:#64748b;margin-top:5px">计划更新 '+text(doc.get('generated_at'))+'；行情时点逐股显示。</div></section>')


def render(st, doc, *, stock_link=None, expected_factpack_id=None, allow_trade_recording=False):
    from copy import deepcopy
    doc=deepcopy(doc)
    report=doc.get('research_report') or {}
    report_factpack=report.get('factpack_id')
    current=bool(current_document(doc) and current_document(report)
                 and (not expected_factpack_id or doc.get('factpack_id')==expected_factpack_id)
                 and report_factpack and report_factpack==doc.get('factpack_id'))
    if not current:
        st.warning('Astra报告已过期、跨月或事实版本变化：下列为历史研究，等待后台更新。原持仓保护继续按原合同执行。')
        doc['month_state']={**(doc.get('month_state') or {}),'state':'LEDGER_UNVERIFIED','realized_net_pnl':None}
        doc['monthly_contracts']=[]
        doc['execution_status']='报告时效未通过；不展示为本月可审交易方案'
        doc['advisor']={'current':False,'reason':'历史专项研判不作为本月当前结论。'}
    st.markdown(summary_html(doc),unsafe_allow_html=True)
    p=doc.get('policy') or {};ms=doc.get('month_state') or {}
    report=doc.get('research_report') or {}
    if not doc.get('private_redacted') and ms.get('reason'):
        st.caption(ms['reason'])
    advisor=doc.get('advisor') or report.get('advisor') or {}
    if not current_advisor(advisor,report):
        advisor={**advisor,'current':False,'reason':'专项研判尚未发布、已过期或输入变化；下表保留已有双审和补证任务。'}
    if advisor.get('current') is True:
        analysis=advisor.get('analysis') or {};brief=str(analysis.get('summary') or '')
        st.markdown('<div class="v88-astra-assessment" style="font-size:12px"><b>'+text(analysis.get('assessment'))
                    +'</b> · '+text(brief[:180]+('…' if len(brief)>180 else ''))+'</div>',unsafe_allow_html=True)
    else:
        st.caption(advisor.get('reason') or '月度专项研判待更新。')
    previous=advisor.get('historical_advisor') or {}
    if advisor.get('current') is True or previous.get('historical_input_verified') is True:
        label='🧠 本月专项研判 · 完整依据' if advisor.get('current') is True else '🧠 最近专项研判 · 历史版本'
        with st.expander(label,expanded=False):
            if advisor.get('current') is True:
                st.markdown(advisor_html(advisor, candidates=report.get('candidates') or [],stock_link=stock_link,
                                        include_summary=len(brief)>180),unsafe_allow_html=True)
            else:
                st.markdown(advisor_html(previous,candidates=report.get('candidates') or [],stock_link=stock_link,
                                        historical=True),unsafe_allow_html=True)
    candidates=report.get('candidates') or []
    st.markdown('**🔎 本月短线研究清单**')
    if candidates:
        # Source contract is adapted by research_rows() after module integration.
        st.markdown(research_rows(candidates,stock_link=stock_link,monthly_target=report.get('monthly_target'),target_currency=report.get('target_currency'),
            record_buy=current and allow_trade_recording and not doc.get('private_redacted'),historical=not current),unsafe_allow_html=True)
    else:
        st.caption(report.get('reason') or '没有可核验的当月候选，保留明确补证任务；不填入虚构股票。')
    plans=doc.get('monthly_contracts') or []
    with st.expander(f'📋 本月可审交易方案（{len(plans)}笔）',expanded=bool(plans)):
        if not plans:st.caption(doc.get('execution_status') or '尚无完整可执行条件；研究候选不能当成已批准买单。')
        for row in plans:
            size=row.get('sizing') or {};fit=row.get('target_fit') or {}
            st.write({'代码':row.get('code'),'核算股数':size.get('quantity'),'原入场':row.get('entry_range'),
                      '原失效':row.get('stop'),'原止盈':row.get('take_profit_range'),'月计划截止':row.get('deadline'),
                      '含费目标情景美元':size.get('net_target_scenario'),'实际成交':'尚未成交，不计进度'})
            st.caption(f"本笔目标情景覆盖率 {fit.get('one_plan_target_coverage_pct','未核实')}%；剩余差额 {fit.get('uncovered_target','未核实')}。")
    with st.expander('🔁 周复盘与月度历史',expanded=False):
        st.markdown(history_html(doc.get('monthly_history') or {},private=doc.get('private_redacted') is True),unsafe_allow_html=True)
        for review in doc.get('protection_reviews') or []:
            st.caption(f"{review.get('code') or review.get('trade_id')}：{review.get('reason','利润保护待核实')}")
    with st.expander('📚 计划规则与风险口径',expanded=False):
        st.caption('Astra研究当月短线，3A主榜负责中长期；最多精选3只，优先A股与港股，美股仅作质量更高或缺额时的补充。共用事实和审核，保留各自等级与原合同。')
        st.caption('进度只计已对账、扣费后的本月实际已实现收益；浮盈、目标情景和未成交方案不计入。目标需要检验可行性，不保证每月获利。')
        if not doc.get('private_redacted'):
            m=doc.get('constraints') or {}
            st.caption(f"计划本金基准 ${number((p.get('capital_envelopes') or {}).get('USD'),0)} → 目标 ${number(p.get('monthly_target'),0)}，需要 {number(m.get('required_return_pct'),1)}%；实际余额仍需核实。")
            st.caption(f"月风险预算 ${number(p.get('monthly_risk_cap'))}；单笔 ${number(p.get('per_trade_risk_cap'))}；每月最多{p.get('max_entries_per_month','—')}次开仓，{p.get('max_stops_per_month','—')}次亏损止损后停手。")
            if p.get('one_r_action'):st.caption(p['one_r_action'])
        st.caption('每日检查行情、事件、审核与触发；每周记录验证与证伪；月末按真实净结算复盘。达标停止新增，未达标不加倍、不挪止损；新月重新对账，已有持仓保护继续。')
        for item in p.get('filters') or []:st.caption(f"{item['name']}：{item['required']}")
        st.caption('斯波朗迪《专业投机原理》为核心；萨普补充风险R、仓位和退出，欧奈尔等按适用条件交叉检验。')


def research_rows(candidates,stock_link=None,*,monthly_target=200,target_currency='USD',record_buy=False,historical=False):
    goal=number(monthly_target,0)+' '+text(target_currency)
    out=[('<div style="font-size:11px;color:#92400e">⏸ 历史研究快照：评级、审核与测算仅为原记录，待同版重核；不代表当前开仓许可。</div>' if historical else '')+
         '<div class="v88-astra-research" style="overflow-x:auto"><table style="width:100%;min-width:1350px;border-collapse:collapse;font-size:12px"><thead style="background:#eef2ff"><tr><th>市场·研究候选</th><th>'+('原中央审核' if historical else '中央审核')+'</th><th>原进场 / 止盈 / 失效</th><th>'+('原月度适用性' if historical else '本月适用性')+'</th><th>'+goal+'目标条件测算</th><th>GPT依据与补证</th></tr></thead><tbody>']
    for row in candidates:
        code=row.get('code');name=row.get('name') or code
        link=stock_link(name,code) if stock_link else '<a href="?'+escape(urlencode({'q':code,'focus':'deep'}),quote=True)+'">'+text(name)+'</a>'
        if record_buy and not historical:
            link+='<br><a target="_self" href="?'+escape(urlencode({'astra_record':code}),quote=True)+'#v88-astra-trades" style="display:inline-block;margin-top:6px;color:#4338ca">✍️ 登记买入</a>'
        plan=row.get('original_plan') or {};fit=row.get('monthly_fit') or {};path=row.get('capital_path') or {}
        currency={'A股':'CNY','美股':'USD','港股':'HKD'}.get(row.get('market'),'币种待核')
        checks=row.get('gaps') or []; reviews=row.get('reviews') or {};primary=reviews.get('primary') or {};counter=reviews.get('counteraudit') or {}
        proof=[]
        for label, review in [('主审',primary),('独立反审',counter)]:
            proof.append('<b>'+label+'</b> · '+text(review.get('completed_at')))
            proof.append(text(review.get('why'))+'<br>反证：'+text(review.get('counterargument')))
            for criterion in review.get('criteria') or []:
                proof.append(text(criterion.get('title') or criterion.get('id'))+' '+text(criterion.get('score'))+'/20 · '+text(criterion.get('reason')))
        gaps='<br>'.join('<b>'+text(g.get('title'))+'</b>：'+text(g.get('detail')) for g in checks if isinstance(g,dict))
        reasons=primary.get('why') or primary.get('counterargument') or '请展开查看原审分项与证据缺口。'
        role=row.get('research_role') or ('市场研究序位 '+str(row.get('market_research_rank','—')))
        selected_reason=(row.get('selection_reason') or {}).get('reason')
        if selected_reason:role+='；'+selected_reason
        out.append('<tr data-astra-code="'+text(code)+'" data-astra-market="'+text(row.get('market'))+'" style="border-bottom:1px solid #e2e8f0;vertical-align:top"><td style="padding:8px">'+text(row.get('market'))+'<br>'+link+'<br><small>'+text(code)+'<br>'+text(role)+'</small></td>'
                   '<td style="padding:8px;color:#3730a3">'+('原评级 ' if historical else '')+text(row.get('central_tier') or '未授级')+'<br>'+('原' if historical else '')+('审核分 ' if row.get('central_tier') else '复审分 ')+number(row.get('audit_score'),1)+'<br><small>书理 '+text(row.get('book_pass_n'))+'/'+text(row.get('book_required'))+'<br>仅研究·无新开仓许可</small></td>'
                   '<td style="padding:8px"><b>原价币种 '+currency+'</b><br>进场 '+band(plan.get('entry_range'))+'<br>止盈 '+band(plan.get('take_profit_range'))+'<br>失效 '+number(plan.get('stop'),4)+'<details><summary>原期限与行情时点</summary>原截止 '+text(plan.get('deadline'))+'<br>行情/证据 '+text(plan.get('evidence_asof'))+'<br>原触发 '+text(plan.get('promotion_trigger'))+'</details></td>'
                   '<td style="padding:8px">'+text(fit.get('reason'))+'<br><small>原持有 '+text(plan.get('holding_sessions'))+' 交易日；本月截止 '+text(fit.get('month_end'))+'</small></td>'
                   '<td style="padding:8px">目标所需美元等值本金 $'+number(path.get('nominal_capital_usd_equivalent'))+'<br>对应原合同模型风险 $'+number(path.get('modeled_risk_to_cover_goal_usd'))+'<br>单笔风险预算下目标情景 $'+number(path.get('risk_limited_one_trade_target_scenario_usd'))+'<details><summary>测算假设 · 非实际收益</summary>'+text(path.get('goal_basis'))+'<br>'+text('；'.join(path.get('assumptions') or []))+'<br>'+text(path.get('cost_assumption'))+'</details></td>'
                   '<td style="padding:8px">'+text(str(reasons)[:180])+'<details><summary>'+('原GPT主审 / 反审' if historical else '当前GPT主审 / 反审')+'</summary>'+'<br>'.join(proof)+'</details>'+domains_html(row.get('eight_domains') or [])+'<details><summary>待补 '+str(len(checks))+' 项</summary>'+gaps+'</details></td></tr>')
    out.append('</tbody></table></div>')
    return ''.join(out)


def domains_html(domains):
    rows=[]
    for d in domains:
        opinions=d.get('review_opinions') or {}
        lines=[text(d.get('title'))+' · '+text(d.get('status'))]
        for role,label in [('primary','主审'),('counteraudit','反审')]:
            r=opinions.get(role) or {}
            lines.append(label+'：'+text(r.get('conclusion'))+' · '+text(r.get('reason')))
        lines.append('缺口：'+text('；'.join(str(x) for x in d.get('limitations') or [])))
        rows.append('<p>'+'<br>'.join(lines)+'</p>')
    return '<details style="font-size:11px"><summary>八域交叉证据</summary>'+''.join(rows)+'</details>'


def advisor_html(advisor,*,candidates=(),stock_link=None,historical=False,include_summary=True):
    if advisor.get('current') is not True and not (historical and advisor.get('historical_input_verified') is True):return '<small>本月专项研判待更新</small>'
    analysis=advisor.get('analysis') or {};evidence=advisor.get('evidence') or {}
    names={r.get('code'):r.get('name') or r.get('code') for r in candidates}
    scope='历史输入版本；当前事实已更新，不代表当前审核' if historical else '同版本证据，研究排序不改变中央等级'
    headline='<b>'+text(analysis.get('assessment'))+'</b> · '+text(analysis.get('summary'))+'<br>' if include_summary else ''
    output=['<div class="v88-astra-advisor" style="font-size:12px">'+headline+'<small>GPT-6 Astra · '+text(advisor.get('generated_at'))+' · '+scope+'</small>']
    if advisor.get('validation_status')=='COMPARISON_REVIEWED':
        output.append('<p style="font-size:11px;color:#64748b">跨股比较已单独复核；比较引用不作为本票事实。原回执及引用问题保留，适用边界见每票说明。</p>')
    for n,row in enumerate(analysis.get('research_priority') or [],1):
        stance=row.get('stance');color={'优先研究':'#0369a1','补证后研究':'#92400e','暂停研究':'#64748b'}.get(stance,'#475569')
        name=names.get(row.get('code'),row.get('code'))
        label=stock_link(name,row.get('code')) if stock_link else text(name)
        refs=set(row.get('own_support_ids',row.get('support_ids')) or [])|set(row.get('counter_ids') or [])
        output.append('<details style="margin-top:6px;border-left:3px solid '+color+';padding-left:8px"><summary>'+str(n)+'. '+label+' · '+text(stance)+'</summary><p>依据：'+text(row.get('reason'))+'<br>反证：'+text(row.get('countercase'))+'</p>')
        if row.get('comparison_note'):
            output.append('<p style="color:#92400e">比较的适用边界：'+text(row['comparison_note'])+'</p>')
        for task in row.get('monthly_tasks') or []:
            output.append('<p>核验任务：'+text(task.get('reason'))+'</p>')
            refs.update(task.get('evidence_ids') or [])
        output.append('<details style="font-size:11px"><summary>查看引用的原始证据</summary>')
        import json
        for ref in sorted(refs):
            e=evidence.get(ref) or {}
            output.append('<p><b>'+text(ref)+' · '+text(e.get('field'))+'</b><br>'+text(json.dumps(e.get('value'),ensure_ascii=False))+'</p>')
        for ref in row.get('comparison_refs') or []:
            e=evidence.get(ref) or {}
            output.append('<p><b>跨股比较 · '+text(e.get('code'))+' · '+text(ref)+'</b>（非本票事实）<br>'+text(json.dumps(e.get('value'),ensure_ascii=False))+'</p>')
        output.append('</details></details>')
    return ''.join(output)+'</div>'


def current_advisor(advisor,report,*,now=None):
    now=_policy_now(now)
    if now is None:return False
    try:
        stamp=datetime.fromisoformat(str(advisor.get('generated_at')).replace('Z','+00:00'))
        return (advisor.get('current') is True and stamp.tzinfo is not None
                and timedelta(0)<=now-stamp<timedelta(hours=24)
                and advisor.get('month')==report.get('month')==now.strftime('%Y-%m')
                and bool(report.get('input_id')) and advisor.get('source_input_id')==report['input_id']
                and bool(report.get('factpack_id')) and advisor.get('factpack_id')==report['factpack_id'])
    except (ValueError,TypeError):return False


def history_html(history,*,private=False):
    months=history.get('months') or [];months=list(months.values()) if isinstance(months,dict) else months
    if not months:return '<small>本月首次观察尚未留存。</small>'
    out=['<div class="v88-astra-history" style="font-size:11px">']
    for row in sorted(months,key=lambda r:r.get('month',''),reverse=True):
        p=row.get('current_policy') or {};last=row.get('last_verified_progress') or {}
        result='当月实际结果未核实' if row.get('actual_result_unknown') is not False or not last else '已对账状态；详见本月进度'
        if private:result='实际收益仅在私域记录'
        out.append('<p><b>'+text(row.get('month'))+' · 目标 '+number(p.get('monthly_target'),0)+' '+text(p.get('target_currency'))+'</b><br>'+result+' · 研究记录 '+str(len(row.get('candidates') or []))+' 只<br>最近检查 '+text(row.get('last_checked_at'))+'</p>')
        if not private and last:out.append('<p>历史已核实净收益 $'+number(last.get('realized_net_pnl'))+' · 对账于 '+text(last.get('reconciled_at'))+'（历史记录，不代替本日对账）</p>')
    week=history.get('weekly_observation') or {}
    out.append('<p>近7日：研究名单观察 '+str(len(week.get('research_changes') or []))+' 次；评级演变 '+str(len(week.get('rating_changes') or []))+' 次。记录增减和证伪，不将研究变化计为盈利。</p>')
    for event in (week.get('rating_changes') or [])[:20]:
        out.append('<p>'+text(event.get('at'))+' · '+text(event.get('code'))+' · '+text(event.get('from'))+' → '+text(event.get('to'))+'</p>')
    return ''.join(out)+'</div>'
