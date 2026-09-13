"""Private Astra fill entry and holding monitor; records fills, never sends orders."""
from datetime import date, datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from html import escape
from decimal import Decimal
import json
import sys
import uuid
import sqlite3

BASE=Path(__file__).resolve().parent.parent/'ai-daily-report-v2'
MARKETS={'A股':'CN','港股':'HK','美股':'US','CN':'CN','HK':'HK','US':'US'}
CURRENCIES={'CN':'CNY','HK':'HKD','US':'USD'}
MARKET_LABELS={'CN':'A股','HK':'港股','US':'美股'}


def services():
    source=str(BASE/'src')
    if source not in sys.path:sys.path.insert(0,source)
    import astra_trade_journal as journal
    import astra_trade_monitor as monitor
    return journal,monitor


def number(value,digits=2):
    if value is None:return '未核实'
    try:return f'{Decimal(str(value)):,.{digits}f}'
    except Exception:return '未核实'


def initial_contract(row, *, stop, low, high, deadline, factpack_id, exit_fee_pct=None, exit_fee_fixed=None, stop_policy=None):
    from copy import deepcopy
    original=deepcopy(row.get('original_plan') or {})
    market=MARKETS[row['market']]
    return {'code':row['code'],'name':row.get('name') or row['code'],'market':market,'currency':CURRENCIES[market],
        'stop':stop,'target':([low,high] if low is not None and high is not None else low if low is not None else high),
        'deadline':deadline.isoformat() if deadline else None,
        'price_basis':'raw_at_entry','basis_source':'user_recorded_quote_currency_prices',
        'monitor_levels_confirmed':True,'stop_policy':stop_policy or original.get('stop_policy') or 'close_only',
        'expected_exit_fee_rate':str(Decimal(str(exit_fee_pct))/100) if exit_fee_pct is not None else None,
        'expected_exit_fixed_fee_native':exit_fee_fixed,
        'original_plan':original,'source_ids':{'factpack_id':row.get('record_factpack_id',factpack_id),'research_code':row['code'],
        'evidence_asof':original.get('evidence_asof'),'central_audit_score':row.get('audit_score')},
        'monitoring_meaning':'用户登记的初始持仓监控条件；原研究合同另存，不回改中央评级或原研究价位。'}


def key(st,name):
    k='astra_trade_request_'+name
    if k not in st.session_state:st.session_state[k]=str(uuid.uuid4())
    return st.session_state[k]


def saved(st,name,message,*,reset_keys=()):
    st.session_state.pop('astra_trade_request_'+name,None)
    st.session_state['astra_trade_saved']=message
    st.session_state['astra_trade_reset_keys']=list(reset_keys)
    st.rerun()


def refresh_after_save(monitor,root,now):
    # A derived-monitor failure must never make a committed fill look unsaved.
    try:
        monitor.build(base=root,now=now,write=True)
        return ''
    except Exception as exc:
        return ' 成交已入账，监控刷新待重试（'+type(exc).__name__+'）；无需重复登记。'


def form_number(st,label,value=None,*,key_name,positive=False):
    try: initial=float(value) if value is not None else None
    except (ValueError,TypeError):initial=None
    return st.number_input(label,min_value=0.0,value=initial,step=0.01,format='%.4f',key=key_name)


def monitor_html(doc):
    if doc.get('available') is False:return '<div style="font-size:12px;color:#92400e">监控暂不可用；已录持仓与成交历史仍保留，稍后重试。</div>'
    rows=doc.get('positions') or []
    if not rows:return '<div class="v88-astra-open-empty" style="font-size:12px;color:#64748b">尚无已录持仓。保存买入记录后，会在这里持续监控。</div>'
    out=['<div class="v88-astra-position-monitor" style="overflow-x:auto"><table style="min-width:1050px;width:100%;border-collapse:collapse;font-size:12px"><thead><tr><th>持仓·买入</th><th>剩余数量</th><th>行情·来源</th><th>初始监控条件</th><th>卖出 / 保护提示</th><th>持仓浮盈亏</th></tr></thead><tbody>']
    for r in rows:
        mark=r.get('mark') or {};c=r.get('original_contract') or {};signals=r.get('signals') or []
        color='#b91c1c' if any(s.get('severity')=='action' for s in signals) else '#92400e' if signals else '#334155'
        target=c.get('target') or []
        if not isinstance(target,(list,tuple)):target=[target]
        levels=' / '.join(number(t,4) for t in target) if target else '待填写'
        stop_label='盘中失效 ' if c.get('stop_policy') in ('intraday','touch','hard_stop','intraday_touch') else '收盘失效 '
        messages='<br>'.join(escape(str(s.get('message'))) for s in signals) or escape(str(r.get('action_prompt') or '按初始条件继续监控'))
        out.append('<tr data-astra-lot="'+escape(str(r.get('lot_id')))+'" style="border-top:1px solid #e2e8f0;vertical-align:top">'
            '<td style="padding:8px">'+escape(str(r.get('name') or r.get('code')))+'<br>'+escape(str(r.get('code')))+' · '+escape(str(r.get('currency')))+'<br><small>'+escape(str(r.get('entry_date') or '见成交记录'))+'</small></td>'
            '<td style="padding:8px">'+number(r.get('remaining_quantity'),4)+'</td>'
            '<td style="padding:8px">'+number(mark.get('price'),4)+'<br><small>'+escape(str(mark.get('asof') or '行情待更新'))+'<br>'+escape(str(mark.get('source') or '来源待核'))+'</small></td>'
            '<td style="padding:8px">'+stop_label+number(c.get('stop'),4)+'<br>止盈 '+levels+'<br><small>截止 '+escape(str(c.get('deadline') or '待填写'))+'</small></td>'
            '<td style="padding:8px;color:'+color+'">'+messages+'</td>'
            '<td style="padding:8px">毛浮盈亏 '+number(r.get('unrealized_gross_native'))+'<br>净浮盈亏 '+number(r.get('unrealized_net_native'))+'<br><small>'+escape(str(r.get('unrealized_cost_note') or '未成交不计已实现收益'))+'</small></td></tr>')
    return ''.join(out)+'</tbody></table></div>'


def recordable_stocks(candidates,snapshot):
    rows={r['code']:r for r in candidates if r.get('code') and r.get('market') in MARKETS}
    # A departed candidate still needs correction/additional-fill entry. These
    # private history choices never become new research recommendations.
    for event in reversed(snapshot.get('history') or []):
        if event.get('event_type')!='BUY':continue
        c=(event.get('payload') or {}).get('contract') or {};code=c.get('code')
        if code and code not in rows and c.get('market') in MARKETS:
            source=c.get('source_ids') or {}
            rows[code]={'code':code,'name':c.get('name'),'market':MARKET_LABELS[MARKETS[c['market']]],
                        'original_plan':c.get('original_plan') or {},'audit_score':source.get('central_audit_score'),
                        'record_factpack_id':source.get('factpack_id'),'record_only_historical':True}
    return rows


def render(st,candidates,*,factpack_id=None,root=BASE,now=None):
    journal,monitor=services();root=Path(root)
    now=now or datetime.now(ZoneInfo('Asia/Shanghai'))
    for widget_key in st.session_state.pop('astra_trade_reset_keys',[]):
        st.session_state.pop(widget_key,None)
    snapshot=journal.snapshot(root=root,now=now)
    try:tracking=monitor.build(snapshot,base=root,now=now,write=False)
    except Exception as exc:
        tracking={'positions':[],'available':False}
        st.warning('持仓监控刷新失败（'+type(exc).__name__+'），成交台账仍可记录和查询；请核对行情后处理持仓。')
    st.markdown('<div id="v88-astra-trades" style="scroll-margin-top:65px"></div>',unsafe_allow_html=True)
    st.markdown('**🧾 Astra · 成交记录与持仓监控**')
    st.caption('登记已经成交的买卖；提交只保存记录，不向券商下单。推荐变化不删除持仓，初始条件及更正历史持续保留。后台每5分钟检查已有行情，页面关闭仍继续；行情日期逐笔显示。')
    success=st.session_state.pop('astra_trade_saved',None)
    if success:st.success(success)
    monthly=snapshot.get('monthly') or {}
    native=monthly.get('realized_net_by_currency') or {}
    native_label='；'.join(k+' '+number(v) for k,v in native.items()) or '尚无已录卖出'
    usd=monthly.get('realized_net_usd')
    st.caption('📒 本月已录成交净盈亏：'+native_label+' · 美元口径：'+('$'+number(usd) if usd is not None else '缺汇率或尚无记录')+'。按用户录入统计，不冒充券商对账；浮盈不计入。')
    st.markdown(monitor_html(tracking),unsafe_allow_html=True)
    rows=recordable_stocks(candidates,snapshot)
    requested=str(st.query_params.get('astra_record') or '')
    if requested in rows and st.session_state.get('astra_record_query_seen')!=requested:
        st.session_state['astra_record_stock']=requested;st.session_state['astra_record_query_seen']=requested
        st.session_state['astra_record_mode']='买入记录'
    mode=st.radio('记录类型',['买入记录','卖出记录','历史 / 更正'],horizontal=True,key='astra_record_mode')
    if mode=='买入记录':
        if not rows:
            st.caption('当前没有研究候选可选；已录持仓的卖出和历史入口继续可用。');return
        if st.session_state.get('astra_record_stock') not in rows:st.session_state['astra_record_stock']=next(iter(rows))
        code=st.selectbox('选择 Astra 个股',list(rows),format_func=lambda c:f"{rows[c]['market']} · {rows[c].get('name') or c}（{c}）"+(' · 已录历史' if rows[c].get('record_only_historical') else ''),key='astra_record_stock')
        row=rows[code];market=MARKETS[row['market']];currency=CURRENCIES[market]
        if row.get('record_only_historical'):st.caption('这是已录个股的历史入口，可补录真实成交或更正；不代表本月新增推荐。')
        today=now.astimezone(ZoneInfo('America/New_York' if market=='US' else 'Asia/Hong_Kong')).date()
        plan=row.get('original_plan') or {};target=plan.get('take_profit_range') or []
        if not isinstance(target,list):target=[]
        try:deadline=date.fromisoformat(str(plan.get('deadline'))[:10])
        except (ValueError,TypeError):deadline=None
        with st.form('astra_buy_form_'+code):
            entry_date=st.date_input('买入日期',value=today,max_value=today,key='astra_buy_date_'+code)
            price=form_number(st,'实际买入价格（'+currency+'）',key_name='astra_buy_price_'+code,positive=True)
            quantity=form_number(st,'实际买入数量（股）',key_name='astra_buy_qty_'+code,positive=True)
            fees=form_number(st,'买入费用合计（'+currency+'）',0,key_name='astra_buy_fees_'+code)
            fx=None if currency!='USD' else 1
            if currency!='USD':fx=form_number(st,'买入时 1 '+currency+' 折合美元（选填）',key_name='astra_buy_fx_'+code)
            note=st.text_input('成交单号 / 备注（选填）',key='astra_buy_note_'+code)
            with st.expander('初始持仓监控条件 · 从研究计划带入',expanded=False):
                st.caption('提交即保存本次持仓的初始条件；可核对后填写，原研究合同另行保留。缺失项会提示补齐，不自动产生止损或止盈。')
                stop=form_number(st,'失效价（'+currency+'）',plan.get('stop'),key_name='astra_buy_stop_'+code)
                stop_mode=st.selectbox('止损确认方式',['收盘确认','盘中触价'],index=1 if plan.get('stop_policy') in ('intraday','touch','hard_stop','intraday_touch') else 0,key='astra_buy_stop_mode_'+code)
                low=form_number(st,'止盈区间下沿（'+currency+'）',target[0] if target else None,key_name='astra_buy_low_'+code)
                high=form_number(st,'止盈区间上沿（'+currency+'）',target[-1] if target else None,key_name='astra_buy_high_'+code)
                due=st.date_input('监控截止日（选填）',value=deadline,key='astra_buy_deadline_'+code)
                exit_pct=form_number(st,'预计卖出综合费率（%，选填）',key_name='astra_buy_exit_pct_'+code)
                exit_fixed=form_number(st,'预计卖出固定费用（'+currency+'，选填）',key_name='astra_buy_exit_fixed_'+code)
                st.caption('两项卖费均填写后才估算净浮盈及1R保护提醒；费用未知仍监控原止盈、止损与期限。')
            submitted=st.form_submit_button('保存买入记录并监控',type='primary')
        if submitted:
            try:
                payload={'date':entry_date.isoformat(),'price':price,'quantity':quantity,'fees':fees,'fx_to_usd':fx,'note':note,
                         'contract':initial_contract(row,stop=stop,low=low,high=high,deadline=due,factpack_id=factpack_id,exit_fee_pct=exit_pct,exit_fee_fixed=exit_fixed,stop_policy='intraday' if stop_mode=='盘中触价' else 'close_only')}
                journal.record_buy(payload,root=root,idempotency_key=key(st,'buy'),now=now)
                suffix=refresh_after_save(monitor,root,now)
                saved(st,'buy','买入记录已保存，已计入持仓监控。'+suffix,reset_keys=['astra_buy_'+f+'_'+code for f in ('price','qty','fees','fx','note')])
            except (ValueError,TypeError,sqlite3.Error,OSError) as exc:st.error('记录未保存：'+str(exc))
    elif mode=='卖出记录':
        positions=snapshot.get('open_positions') or []
        if not positions:st.caption('暂无可登记卖出的持仓。请先录入买入成交。');return
        by_id={r['lot_id']:r for r in positions}
        lot=st.selectbox('选择要卖出的持仓',list(by_id),format_func=lambda k:f"{by_id[k].get('name') or by_id[k]['code']}（{by_id[k]['code']}） · 剩余{number(by_id[k]['remaining_quantity'],4)}股 · {by_id[k]['entry_date']}",key='astra_sell_lot')
        position=by_id[lot];currency=position['currency'];market=MARKETS[position['market']]
        today=now.astimezone(ZoneInfo('America/New_York' if market=='US' else 'Asia/Hong_Kong')).date()
        with st.form('astra_sell_form_'+lot):
            sell_date=st.date_input('卖出日期',value=today,min_value=date.fromisoformat(position['entry_date']),max_value=today,key='astra_sell_date_'+lot)
            price=form_number(st,'实际卖出价格（'+currency+'）',key_name='astra_sell_price_'+lot,positive=True)
            quantity=form_number(st,'实际卖出数量（股，可部分卖出）',position['remaining_quantity'],key_name='astra_sell_qty_'+lot,positive=True)
            fees=form_number(st,'卖出费用合计（'+currency+'）',0,key_name='astra_sell_fees_'+lot)
            fx=1 if currency=='USD' else form_number(st,'卖出时 1 '+currency+' 折合美元（选填）',key_name='astra_sell_fx_'+lot)
            reason=st.selectbox('卖出原因',['止盈','止损','期限到期','主动退出','其他'],key='astra_sell_reason_'+lot)
            note=st.text_input('卖出成交单号 / 备注（选填）',key='astra_sell_note_'+lot)
            submitted=st.form_submit_button('保存卖出记录',type='primary')
        if submitted:
            try:
                journal.record_sell({'lot_id':lot,'date':sell_date.isoformat(),'price':price,'quantity':quantity,'fees':fees,'fx_to_usd':fx,'reason':reason,'note':note},root=root,idempotency_key=key(st,'sell'),now=now)
                suffix=refresh_after_save(monitor,root,now)
                saved(st,'sell','卖出记录已保存；剩余持仓及分笔盈亏已更新。'+suffix,reset_keys=['astra_sell_'+f+'_'+lot for f in ('price','qty','fees','fx','note')])
            except (ValueError,TypeError,sqlite3.Error,OSError) as exc:st.error('记录未保存：'+str(exc))
    else:
        events=snapshot.get('history') or []
        if not events:st.caption('尚无成交记录。');return
        labels={'BUY':'买入','SELL':'卖出','VOID':'撤销更正'}
        lots={p['lot_id']:p for p in snapshot.get('positions',[])}
        buys={e['lot_id']:(e.get('payload') or {}).get('contract',{}) for e in events if e.get('event_type')=='BUY'}
        pnl={s['event_id']:s for p in lots.values() for s in p.get('sells',[])}
        table=[]
        for event in reversed(events):
            payload=event.get('payload') or {};contract=payload.get('contract') or buys.get(event.get('lot_id'),{})
            result=pnl.get(event.get('event_id'),{})
            table.append({'记录':labels.get(event.get('event_type'),event.get('event_type')),'股票':(contract.get('name') or '')+' '+(contract.get('code') or ''),
                          '日期':payload.get('date'),'价格':payload.get('price'),'数量':payload.get('quantity'),'费用':payload.get('fees'),
                          '币种':contract.get('currency'),'本笔已实现净盈亏':result.get('realized_net_native'),'本笔美元净盈亏':result.get('realized_net_usd'),
                          '备注':payload.get('note') or payload.get('reason'),'已撤销':bool(event.get('voided')),'记录编号':event.get('event_id')})
        st.dataframe(table,hide_index=True,width='stretch')
        st.download_button('下载成交历史 JSON',json.dumps(snapshot,ensure_ascii=False,indent=2),file_name='astra-trade-history.json',mime='application/json')
        with st.expander('更正误录 · 原记录保留',expanded=False):
            eligible={e['event_id']:e for e in events if e.get('event_type') in ('BUY','SELL') and not e.get('voided')}
            if eligible:
                with st.form('astra_void_form'):
                    chosen=st.selectbox('撤销哪条误录',list(eligible),format_func=lambda k:f"{labels[eligible[k]['event_type']]} · {buys.get(eligible[k].get('lot_id'),{}).get('name','')} · {(eligible[k].get('payload') or {}).get('date')} · {(eligible[k].get('payload') or {}).get('quantity')}股 × {(eligible[k].get('payload') or {}).get('price')} · {k[:8]}")
                    reason=st.text_input('更正原因（必填）')
                    submit=st.form_submit_button('撤销误录并保留历史')
                if submit:
                    try:
                        journal.void_record(chosen,reason,root=root,idempotency_key=key(st,'void'),now=now)
                        suffix=refresh_after_save(monitor,root,now)
                        saved(st,'void','误录已撤销并保留历史，可重新录入正确成交。'+suffix)
                    except (ValueError,TypeError,sqlite3.Error,OSError) as exc:st.error('未能撤销：'+str(exc))
