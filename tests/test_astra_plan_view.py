"""Rendering must preserve monthly research/accounting boundaries."""
import importlib.util
from datetime import datetime, timezone, timedelta
from pathlib import Path

SPEC=importlib.util.spec_from_file_location('astra_plan_view',Path(__file__).resolve().parents[1]/'astra_plan_view.py')
view=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(view)


def document(**state):
    now=datetime.now(timezone(timedelta(hours=8)))
    return {'month':now.strftime('%Y-%m'),'generated_at':now.isoformat(),
            'policy':{'monthly_target':200,'target_currency':'USD'},
            'month_state':{'state':'OPEN','realized_net_pnl':25,'reconciled_at':now.isoformat(),**state}}


def candidate(code='688002.SS',market='A股',tier='1A'):
    return {'code':code,'name':'研究公司','market':market,'central_tier':tier,'audit_score':70,
            'original_plan':{'entry_range':[10.25,10.5],'stop':9.75,'take_profit_range':[12.5,13.5],
                'deadline':'2026-10-11T16:00:00+08:00','holding_sessions':10,'evidence_asof':'2026-09-11T15:00:00+08:00'},
            'monthly_fit':{'reason':'需要本月重审','month_end':'2026-09-30T23:59:59+08:00'},
            'capital_path':{'nominal_capital_usd_equivalent':1000,'modeled_risk_to_cover_goal_usd':100,
                'risk_limited_one_trade_target_scenario_usd':125,'assumptions':['条件成立才可计算']},
            'reviews':{'primary':{'completed_at':'2026-09-12T20:42:00+08:00','why':'同周期研究依据',
                'criteria':[{'id':'facts','score':15,'reason':'字段已核对'}]},
                'counteraudit':{'completed_at':'2026-09-12T20:43:00+08:00','why':'风险复核'}},
            'gaps':[{'title':'原期限跨月','detail':'不得改写原截止日'}]}


def test_unknown_account_never_treats_missing_net_as_zero():
    page=view.summary_html(document(state='LEDGER_UNVERIFIED',realized_net_pnl=None))
    assert '未对账不按0收益计算' in page and '待成交对账' in page
    assert '进度 0.0%' not in page


def test_unverified_state_cannot_reuse_prior_numeric_progress():
    page=view.summary_html(document(state='LEDGER_UNVERIFIED',realized_net_pnl=25))
    assert '未对账不按0收益计算' in page and '进度 12.5%' not in page


def test_public_view_does_not_leak_private_pnl():
    page=view.summary_html({**document(realized_net_pnl=123.45),'private_redacted':True})
    assert '123.45' not in page and '待成交对账' in page


def test_reconciled_negative_net_increases_remaining_goal():
    page=view.summary_html(document(realized_net_pnl=-25))
    assert '$-25.00' in page and '$225.00' in page


def test_ungraded_candidate_has_review_score_without_invented_tier():
    page=view.research_rows([candidate(tier=None)])
    assert '未授级' in page and '复审分 70.0' in page and '审核分 70.0' not in page
    assert '仅研究·无新开仓许可' in page


def test_original_deadline_source_clock_and_monthly_window_all_survive():
    page=view.research_rows([candidate()])
    assert '2026-10-11T16:00:00+08:00' in page
    assert '2026-09-30T23:59:59+08:00' in page
    assert '2026-09-11T15:00:00+08:00' in page
    assert '原截止' in page and '原期限跨月' in page


def test_three_quote_currencies_are_identified_separately_from_usd_math():
    for code,market,currency in [('688002.SS','A股','CNY'),('SEPN','美股','USD'),('3330.HK','港股','HKD')]:
        page=view.research_rows([candidate(code,market)])
        assert currency in page, (market,currency)
        assert '美元等值本金' in page


def test_source_text_is_escaped_and_link_is_canonical_deep_query():
    row=candidate();row['name']='<script>bad()</script>';row['gaps'][0]['detail']='<b>not html</b>'
    page=view.research_rows([row])
    assert '<script>' not in page and '&lt;script&gt;' in page
    assert '&lt;b&gt;not html&lt;/b&gt;' in page
    assert 'q=688002.SS' in page and 'focus=deep' in page


def test_reconciliation_clock_must_be_current_same_month_and_timezone_aware():
    now=datetime.now(timezone(timedelta(hours=8)))
    valid=document()
    assert view.verified_progress(valid,now=now+timedelta(seconds=1))
    for at in [(now-timedelta(hours=25)).isoformat(),(now+timedelta(hours=1)).isoformat(),
               now.replace(tzinfo=None).isoformat(),'bad-date']:
        assert not view.verified_progress(document(reconciled_at=at),now=now)
    older=document();older['month']='1999-01'
    assert not view.verified_progress(older,now=now)


def test_unknown_runtime_state_does_not_show_verified_account_progress():
    now=datetime.now(timezone(timedelta(hours=8)))
    for state in ['FAILED','',None,'UNRECOGNIZED_STATUS']:
        assert not view.verified_progress(document(state=state),now=now+timedelta(seconds=1))


def test_current_summary_rules_and_buy_link_survive_compact_rendering():
    now=datetime.now(timezone(timedelta(hours=8)))
    report,advisor=advisor_fixture(now)
    doc={**document(),'factpack_id':'current-pack','research_report':report,'advisor':advisor}
    capture=StreamlitCapture()
    view.render(capture,doc,expected_factpack_id='current-pack',allow_trade_recording=True)
    all_text='\n'.join(capture.messages)
    front='\n'.join(value for value,path in capture.events if not path)
    rules='\n'.join(value for value,path in capture.events if '📚 计划规则与风险口径' in path)
    assert 'ADVISOR_CURRENT_SENTINEL' in front and all_text.count('ADVISOR_CURRENT_SENTINEL')==1
    assert ('🧠 本月专项研判 · 完整依据',False) in capture.expanders
    assert ('📚 计划规则与风险口径',False) in capture.expanders
    assert '最多精选3只' in rules and '萨普' in rules and '浮盈、目标情景和未成交方案不计入' in rules
    assert '最多精选3只' not in front and '原Fable5计划升级' not in all_text
    assert 'astra_record=688002.SS' in front and '2026-10-11T16:00:00+08:00' in all_text
    assert '进度 12.5%' in front and '$175.00' in front


def test_long_current_summary_is_brief_above_fold_but_full_text_remains_available():
    now=datetime.now(timezone(timedelta(hours=8)))
    report,advisor=advisor_fixture(now)
    advisor['analysis']['summary']='观察依据。'*50+'FULL_SUMMARY_END'
    doc={**document(),'factpack_id':'current-pack','research_report':report,'advisor':advisor}
    capture=StreamlitCapture();view.render(capture,doc)
    front='\n'.join(value for value,path in capture.events if not path)
    folded='\n'.join(value for value,path in capture.events if path)
    assert 'FULL_SUMMARY_END' not in front and '…' in front
    assert 'FULL_SUMMARY_END' in folded


def test_monthly_target_table_heading_uses_passed_policy_not_hardcoded_200():
    html=view.research_rows([candidate()],monthly_target=250,target_currency='USD')
    assert '250 USD目标条件测算' in html and '200目标' not in html and '$200' not in html


def test_stale_or_missing_advisor_never_displays_cached_analysis():
    for state in [False,None]:
        html=view.advisor_html({'current':state,'analysis':{'summary':'STALE_SENTINEL','research_priority':[
            {'code':'ABC','reason':'OLD_INSTRUCTION','stance':'优先研究'}]}})
        assert 'STALE_SENTINEL' not in html and 'OLD_INSTRUCTION' not in html
        assert '待更新' in html


def test_current_advisor_preserves_countercase_and_safe_evidence_references():
    advisor={'current':True,'generated_at':'2026-09-12T23:00:00+08:00',
        'analysis':{'assessment':'研究受限','summary':'只作本月研究',
            'research_priority':[{'code':'ABC','stance':'补证后研究','reason':'SUPPORT_REASON',
                'countercase':'COUNTER_REASON','support_ids':['E1'],'counter_ids':['E2'],
                'monthly_tasks':[{'reason':'TASK_REASON','evidence_ids':['E2']}]}]},
        'evidence':{'E1':{'field':'original_plan.stop','value':9.75},
                    'E2':{'field':'gaps','value':'<script>raw</script>'}}}
    html=view.advisor_html(advisor,candidates=[{'code':'ABC','name':'研究公司'}])
    assert all(t in html for t in ['研究公司','SUPPORT_REASON','COUNTER_REASON','TASK_REASON','original_plan.stop','9.75'])
    assert '<script>' not in html and '&lt;script&gt;' in html
    assert '不改变中央等级' in html


def test_public_history_hides_net_pnl_and_receipt_fields():
    history={'months':[{'month':'2026-09','current_policy':{'monthly_target':200,'target_currency':'USD'},
        'actual_result_unknown':False,'last_verified_progress':{'realized_net_pnl':12345.67,
            'reconciled_at':'PRIVATE_RECEIPT_DATE'},'last_checked_at':'2026-09-12T23:00:00+08:00'}]}
    public=view.history_html(history,private=True)
    assert '12345.67' not in public and '12,345.67' not in public and 'PRIVATE_RECEIPT_DATE' not in public
    assert '实际收益仅在私域记录' in public
    internal=view.history_html(history,private=False)
    assert '12,345.67' in internal and '历史记录，不代替本日对账' in internal


def test_legacy_history_without_reconciliation_is_unknown_not_verified():
    page=view.history_html({'months':[{'month':'2026-09','current_policy':{'monthly_target':200}}]})
    assert '当月实际结果未核实' in page and '已对账状态' not in page


def test_both_review_counterarguments_and_eight_domains_are_folded():
    row=candidate();row['reviews']['primary']['counterargument']='PRIMARY_COUNTER'
    row['reviews']['counteraudit']['counterargument']='SECOND_COUNTER'
    row['eight_domains']=[{'title':'板块轮动','status':'limited','limitations':['未形成独立验证'],
        'review_opinions':{'primary':{'conclusion':'混合','reason':'SECTOR_PRIMARY'},
                          'counteraudit':{'conclusion':'反对','reason':'SECTOR_COUNTER'}}}]
    page=view.research_rows([row])
    for value in ['PRIMARY_COUNTER','SECOND_COUNTER','SECTOR_PRIMARY','SECTOR_COUNTER','八域交叉证据','未形成独立验证']:
        assert value in page
    assert '<details' in page


def test_reviewed_cross_stock_comparisons_are_distinct_from_own_facts():
    advisor={'current':True,'validation_status':'COMPARISON_REVIEWED','analysis':{'research_priority':[
        {'code':'AAA','stance':'优先研究','reason':'比较说明','countercase':'本票仍缺实际验证',
         'support_ids':['OWN','OTHER'],'own_support_ids':['OWN'],'comparison_refs':['OTHER'],
         'comparison_note':'未记载不代表已验证','counter_ids':['OWN'],'monthly_tasks':[]}]},
        'evidence':{'OWN':{'code':'AAA','field':'PLAN','value':'OWN_FACT'},
                    'OTHER':{'code':'BBB','field':'VALIDATION','value':'OTHER_FACT'}}}
    page=view.advisor_html(advisor)
    assert '跨股比较已单独复核' in page and '未记载不代表已验证' in page
    assert '跨股比较 · BBB · OTHER' in page and '（非本票事实）' in page
    assert page.count('OTHER_FACT')==1


def advisor_fixture(now):
    report={'month':now.strftime('%Y-%m'),'generated_at':now.isoformat(),
            'input_id':'research-id','factpack_id':'current-pack','monthly_target':200,
            'target_currency':'USD','candidates':[candidate()]}
    advisor={'current':True,'month':report['month'],'generated_at':now.isoformat(),
             'source_input_id':report['input_id'],'factpack_id':report['factpack_id'],
             'analysis':{'assessment':'有条件研究','summary':'ADVISOR_CURRENT_SENTINEL','research_priority':[]}}
    return report,advisor


def test_current_document_rechecks_persistent_snapshot_time_and_month():
    now=datetime.now(timezone(timedelta(hours=8)))
    doc={**document(),'generated_at':now.isoformat()}
    assert view.current_document(doc,now=now)
    for stamp in [(now-timedelta(hours=24,seconds=1)).isoformat(),
                  (now+timedelta(seconds=1)).isoformat(),now.replace(tzinfo=None).isoformat(),None]:
        assert not view.current_document({**doc,'generated_at':stamp},now=now)
    assert not view.current_document({**doc,'month':'1999-01'},now=now)


def test_current_advisor_rechecks_age_future_month_and_both_input_bindings():
    now=datetime.now(timezone(timedelta(hours=8)))
    report,advisor=advisor_fixture(now)
    assert view.current_advisor(advisor,report,now=now)
    for update in [{'current':False},{'generated_at':(now-timedelta(hours=24)).isoformat()},
        {'generated_at':(now+timedelta(seconds=1)).isoformat()},
        {'generated_at':now.replace(tzinfo=None).isoformat()},
        {'month':'1999-01'},{'source_input_id':'old-research'}, {'factpack_id':'old-pack'}]:
        assert not view.current_advisor({**advisor,**update},report,now=now)
    assert not view.current_advisor(advisor,{**report,'input_id':None},now=now)
    assert not view.current_advisor({**advisor,'factpack_id':None},{**report,'factpack_id':None},now=now)


class StreamlitCapture:
    def __init__(self):self.messages=[];self.writes=[];self.warnings=[];self.expanders=[];self.stack=[];self.events=[]
    def record(self,value):self.messages.append(str(value));self.events.append((str(value),tuple(self.stack)))
    def markdown(self,value,**kwargs):self.record(value)
    def caption(self,value,**kwargs):self.record(value)
    def warning(self,value,**kwargs):self.warnings.append(str(value));self.record(value)
    def write(self,value,**kwargs):self.writes.append(value)
    def expander(self,value,**kwargs):
        self.messages.append(str(value));self.expanders.append((value,kwargs.get('expanded')))
        capture=self
        class Scope:
            def __enter__(self):capture.stack.append(value);return capture
            def __exit__(self,*args):capture.stack.pop();return False
        return Scope()


def test_render_stale_document_hides_orders_and_advisor_without_mutating_original():
    from copy import deepcopy
    now=datetime.now(timezone(timedelta(hours=8)))
    report,advisor=advisor_fixture(now)
    doc={**document(),'generated_at':(now-timedelta(days=2)).isoformat(),
         'factpack_id':'current-pack','research_report':report,'advisor':advisor,
         'monthly_contracts':[{'code':'ORDER_SENTINEL','sizing':{'quantity':123}}]}
    original=deepcopy(doc);capture=StreamlitCapture()
    view.render(capture,doc,expected_factpack_id='current-pack')
    combined='\n'.join(capture.messages)
    assert capture.warnings and not capture.writes and 'ADVISOR_CURRENT_SENTINEL' not in combined
    assert '待成交对账' in combined and doc==original


def test_render_current_live_factpack_blocks_embedded_old_report_advisor():
    now=datetime.now(timezone(timedelta(hours=8)))
    report,advisor=advisor_fixture(now)
    report['factpack_id']=advisor['factpack_id']='old-pack'
    doc={**document(),'factpack_id':'current-pack','research_report':report,'advisor':advisor}
    capture=StreamlitCapture();view.render(capture,doc,expected_factpack_id='current-pack')
    assert 'ADVISOR_CURRENT_SENTINEL' not in '\n'.join(capture.messages)


def test_render_expected_live_factpack_change_hides_old_plan_and_advisor():
    now=datetime.now(timezone(timedelta(hours=8)))
    report,advisor=advisor_fixture(now)
    doc={**document(),'factpack_id':'current-pack','research_report':report,'advisor':advisor,
         'monthly_contracts':[{'code':'ORDER_SENTINEL','sizing':{'quantity':123}}]}
    capture=StreamlitCapture();view.render(capture,doc,expected_factpack_id='new-live-pack')
    assert capture.warnings and not capture.writes
    assert 'ADVISOR_CURRENT_SENTINEL' not in '\n'.join(capture.messages)


def test_stale_research_is_retained_as_history_without_current_grade_or_record_link():
    now=datetime.now(timezone(timedelta(hours=8)))
    report,advisor=advisor_fixture(now)
    doc={**document(),'generated_at':(now-timedelta(days=2)).isoformat(),
         'factpack_id':'current-pack','research_report':report,'advisor':advisor}
    capture=StreamlitCapture()
    view.render(capture,doc,expected_factpack_id='current-pack',allow_trade_recording=True)
    page='\n'.join(capture.messages)
    assert '历史研究快照' in page and '原评级 1A' in page and '原审核分 70.0' in page
    assert '原GPT主审 / 反审' in page and '当前GPT主审 / 反审' not in page
    assert 'astra_record=' not in page and 'q=688002.SS' in page
    assert '2026-10-11T16:00:00+08:00' in page


def test_unknown_account_progress_has_no_accessible_zero_or_open_status():
    page=view.summary_html(document(reconciled_at=None))
    assert 'aria-valuenow' not in page and '实际净收益尚未核实' in page
    assert '可继续筛选' not in page and '成交进度待对账' in page


def test_progress_visually_distinguishes_negative_verified_profit_and_currency():
    page=view.summary_html(document(realized_net_pnl=-25))
    assert 'color:#b91c1c' in page and '目标 (USD)' in page
    assert 'role="progressbar"' in page and '$225.00' in page


def test_fresh_wrapper_cannot_make_stale_or_cross_month_research_current():
    now=datetime.now(timezone(timedelta(hours=8)))
    for change in [{'generated_at':(now-timedelta(days=2)).isoformat()},
                   {'month':'1999-01'},{'generated_at':(now+timedelta(hours=1)).isoformat()}]:
        report,advisor=advisor_fixture(now)
        doc={**document(),'factpack_id':'current-pack','research_report':{**report,**change},'advisor':advisor}
        capture=StreamlitCapture();view.render(capture,doc,allow_trade_recording=True)
        page='\n'.join(capture.messages)
        assert capture.warnings and '历史研究快照' in page and 'astra_record=' not in page
        assert '当前GPT主审 / 反审' not in page


def test_summary_cannot_reuse_new_reconciliation_inside_old_document():
    doc=document();doc['generated_at']=(datetime.now(timezone.utc)-timedelta(days=2)).isoformat()
    page=view.summary_html(doc)
    assert 'aria-valuenow' not in page and '成交进度待对账' in page


def test_month_validation_uses_shanghai_day_for_equivalent_utc_clock():
    local=datetime(2026,10,1,0,30,tzinfo=timezone(timedelta(hours=8)))
    utc=local.astimezone(timezone.utc)
    doc={**document(),'month':'2026-10','generated_at':local.isoformat()}
    doc['month_state']['reconciled_at']=local.isoformat()
    report,advisor=advisor_fixture(local)
    assert view.current_document(doc,now=utc) and view.verified_progress(doc,now=utc)
    assert view.current_advisor(advisor,report,now=utc)
    assert not view.current_document(doc,now=local.replace(tzinfo=None))
    assert not view.verified_progress(doc,now=local.replace(tzinfo=None))
    assert not view.current_advisor(advisor,report,now=local.replace(tzinfo=None))
