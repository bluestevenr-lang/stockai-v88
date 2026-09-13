from datetime import datetime,timedelta,timezone
from weekly_candidates_ui import html

NOW=datetime(2026,9,12,8,tzinfo=timezone.utc)


def fixture():
    row={'code':'TEST','name':'<script>bad</script>','market':'美股','central_tier':'1A','eligible':True,
        'audit_score':65,'screen_score':74.32,'state':'准状态·冲刺2A/3A','last':10,'source_asof':'2026-09-11',
        'trade_plan':{'entry_range':[9.8,10.1],'take_profit_range':[11.5,12],'stop':9.3},
        'profit_contract':{'net_upside_pct':12,'net_reward_risk':2,'holding_sessions':10,'thesis_deadline':'2026-10-11'},
        'history':{'position_bars':252,'position252_pct':20,'low_recovery':True},'gaps':[],
        'book_checks':[],'no_grade_authority':True,'executable':False}
    return {'version':'weekly-candidates-v1','generated_at':NOW.isoformat(),'factpack_id':'pack',
        'policy':{'ranking':'central-five-session-entry-with-weekly-eligibility-v6'},
        'week':{'start':'2026-09-14','end':'2026-09-18'},'rows':[row],
        'market_slots':{'美股':{'selected':1,'qualified_2a_3a':0}}}


def test_research_keeps_original_table_columns_and_separate_score():
    out=html(fixture(),{'factpack_id':'pack'},NOW)
    assert out.count('<th ')==11
    assert '审核 65.00/100' in out and '筛选 74.32/100' in out
    assert '原评级 1A' in out and '缺少同版中央引用' in out and '当前正式2A/3A 0只' in out and '不可执行' in out
    assert '?q=TEST' in out and '<script>bad' not in out


def test_changed_pack_or_expired_snapshot_freezes_instead_of_disappearing():
    for selection,now in (({'factpack_id':'new'},NOW),({'factpack_id':'pack'},NOW+timedelta(days=5))):
        out=html(fixture(),selection,now)
        assert '待当日重核' in out and '原评级 1A' in out and '?q=TEST' in out


def test_old_week_cannot_reopen_from_recent_maintenance_time():
    from copy import deepcopy
    doc,selection,watch,now=linked_fixture()
    old=deepcopy(doc);old['week']={'start':'2026-09-07','end':'2026-09-11'}
    page=html(old,selection,now,watchlist=watch)
    assert '当前评级 1A' not in page and '原评级 1A' in page
    assert '历史周度·不可执行' in page and '同股同分</a>' not in page


def test_stale_weekly_row_never_renders_fresh_entry_instruction(monkeypatch):
    import entry_opportunity
    monkeypatch.setattr(entry_opportunity,'html',lambda *a,**kw:'FRESH_ENTRY_SENTINEL')
    page=html(fixture(),{'factpack_id':'different'},NOW)
    assert 'FRESH_ENTRY_SENTINEL' not in page
    assert '历史周度·不可执行' in page and '?q=TEST' in page


def test_row_data_gap_never_shows_a_current_grade():
    doc=fixture();doc['rows'][0]['eligible']=False
    out=html(doc,{'factpack_id':'pack'},NOW)
    assert '当前评级 1A' not in out and '原评级 1A' in out


def test_low_price_contract_precision_is_not_rounded_into_same_stop_and_entry():
    doc=fixture();r=doc['rows'][0]
    r['trade_plan'].update(entry_range=[.1441,.156],take_profit_range=[.179,.18],stop=.144)
    out=html(doc,{'factpack_id':'pack'},NOW)
    assert '0.1441 ～ 0.156' in out and '0.179 ～ 0.18' in out and '0.144<br>' in out


def test_bound_weekly_row_revalidates_score_contract_and_shared_rank():
    from copy import deepcopy
    from test_grade_card_safety import central_row
    from stock_reference import reference
    import grade_focus
    now=datetime.now(timezone.utc)
    central=central_row('TEST','测试','1A','RESEARCH_1A')
    now=datetime.now(timezone.utc)
    selection={'version':'gpt-classics-selection-v9-tharp','generated_at':now.isoformat(),
               'factpack_id':central['factpack_id'],'observations':[central]}
    focus=grade_focus.build([central]);rec=focus['records']['TEST']
    doc=fixture();doc.update(factpack_id=central['factpack_id'],generated_at=now.isoformat(),
                            central_generated_at=selection['generated_at'],master_focus_version=grade_focus.VERSION)
    r=doc['rows'][0];r.update(audit_score=central['audit_score'],trade_plan=deepcopy(central['trade_plan']),
                            scorecard=central['scorecard'],master_ref=reference(selection,central),
                            master_focus={k:rec.get(k) for k in ('rank','central_rank','selected','tier','market','sort_key')})
    out=html(doc,selection,now)
    assert '当前评级 1A' in out and "data-cross-ok='true'" in out
    assert '本市场1A审核分第1名' in out and '中央同级 Top' not in out
    assert '主榜位置：当前视图未载入' in out and '同股同分</a>' not in out
    for change in ('score','plan','rank','central_rank'):
        bad=deepcopy(doc);row=bad['rows'][0]
        if change=='score':row['audit_score']=99
        elif change=='plan':row['trade_plan']['stop']=1
        elif change=='rank':row['master_focus']['rank']=99
        else:row['master_focus']['central_rank']=99
        out=html(bad,selection,now)
        assert "data-cross-ok='false'" in out and '当前评级 1A' not in out


def linked_fixture():
    from copy import deepcopy
    from test_grade_card_safety import central_row
    from stock_reference import reference
    import grade_focus
    now=datetime.now(timezone.utc)
    central=central_row('TEST','测试','1A','RESEARCH_1A')
    central['factpack_asof']='2026-09-11T16:00:00-04:00'
    # Higher quality rows have no feasible entry path. They must not be
    # confused with the attention ordering shown for the weekly candidate.
    better=[]
    def change_ma20(value):
        if isinstance(value,dict):
            if value.get('field')=='market_evidence.ma20':value['value']=99
            for child in value.values():change_ma20(child)
        elif isinstance(value,list):
            for child in value:change_ma20(child)
    for code in ('AA','BB'):
        row=deepcopy(central);row.update(code=code,name=code)
        change_ma20(row)
        better.append(row)
    now=datetime.now(timezone.utc)
    selection={'version':'gpt-classics-selection-v9-tharp','generated_at':now.isoformat(),
               'factpack_id':central['factpack_id'],'observations':better+[central]}
    focus=grade_focus.build(better+[central],now=now);rec=focus['records']['TEST']
    doc=fixture();doc.update(factpack_id=central['factpack_id'],generated_at=now.isoformat(),
        central_generated_at=selection['generated_at'],master_focus_version=grade_focus.VERSION)
    r=doc['rows'][0];r.update(audit_score=central['audit_score'],trade_plan=deepcopy(central['trade_plan']),
        scorecard=deepcopy(central['scorecard']),master_ref=reference(selection,central),
        master_focus={k:rec.get(k) for k in ('rank','central_rank','selected','tier','market','sort_key')},
        master_watchlist={'watch_rank':3,'seat_kind':'已评级跟踪','current':True})
    seat={**deepcopy(central),'watch_rank':3,'seat_kind':'已评级跟踪',
          'source_asof':central['factpack_asof'],'entry_opportunity':rec['entry_opportunity']}
    watchlist={'generated_at':now.isoformat(),'factpack_id':central['factpack_id'],
        'source_generated_at':selection['generated_at'],'current_focus':{'rows':[seat]}}
    return doc,selection,watchlist,now


def test_weekly_distinguishes_attention_from_quality_and_links_actual_seat():
    doc,selection,watchlist,now=linked_fixture()
    out=html(doc,selection,now,watchlist=watchlist)
    assert '本市场1A审核分第3名' in out
    assert "href='#v88-watch-TEST'" in out and '短期Top3 · 1A同股同分' in out
    assert '当前评级 1A' in out and "data-cross-ok='true'" in out


def test_mismatched_or_missing_current_seat_cannot_claim_weekly_link():
    from copy import deepcopy
    doc,selection,watchlist,now=linked_fixture()
    for change in ('absent','duplicate','rank','score','tier','plan','source','pack','generation','stale','entry'):
        bad=deepcopy(watchlist);seat=bad['current_focus']['rows'][0]
        if change=='absent':bad['current_focus']['rows']=[]
        elif change=='duplicate':bad['current_focus']['rows'].append(deepcopy(seat))
        elif change=='rank':seat['watch_rank']=2
        elif change=='score':seat['audit_score']=99
        elif change=='tier':seat['tier']='3A'
        elif change=='plan':seat['trade_plan']['stop']=1
        elif change=='source':seat['source_asof']='old'
        elif change=='pack':bad['factpack_id']='old'
        elif change=='generation':bad['source_generated_at']='old'
        elif change=='stale':bad['generated_at']=(now-timedelta(minutes=16)).isoformat()
        else:seat['entry_opportunity']['focus_eligible']=False
        out=html(doc,selection,now,watchlist=bad)
        assert '当前评级 1A' not in out,change
        assert '同股同分</a>' not in out,change
        assert '周度与当前主榜席位未形成同版关联' in out,change
        assert '?q=TEST' in out,change


def test_same_factpack_with_changed_publication_does_not_inherit_weekly_link():
    doc,selection,watchlist,now=linked_fixture()
    doc['central_generated_at']='older-publication'
    out=html(doc,selection,now,watchlist=watchlist)
    assert '当前评级 1A' not in out and '同股同分</a>' not in out


def ranked_fixture(tier,position):
    from copy import deepcopy
    from test_grade_card_safety import central_row
    from stock_reference import reference
    import grade_focus
    now=datetime.now(timezone.utc)
    target=central_row('ZZTOP','目标',tier,'RESEARCH_'+tier)
    target['factpack_asof']='2026-09-11T16:00:00-04:00'
    before=[]
    for index in range(position-1):
        row=deepcopy(target);row.update(code=f'AA{index}',name=f'前位{index}')
        before.append(row)
    now=datetime.now(timezone.utc)
    selection={'version':'gpt-classics-selection-v9-tharp','generated_at':now.isoformat(),
        'factpack_id':target['factpack_id'],'observations':before+[target]}
    rec=grade_focus.build(before+[target],now=now)['records']['ZZTOP']
    doc=fixture();doc.update(factpack_id=target['factpack_id'],generated_at=now.isoformat(),
        central_generated_at=selection['generated_at'],master_focus_version=grade_focus.VERSION)
    row=doc['rows'][0];row.update(code='ZZTOP',name='目标',central_tier=tier,
        audit_score=target['audit_score'],trade_plan=deepcopy(target['trade_plan']),
        scorecard=deepcopy(target['scorecard']),master_ref=reference(selection,target),
        master_focus={k:rec.get(k) for k in ('rank','central_rank','selected','tier','market','sort_key')},
        master_watchlist={'watch_rank':position,'seat_kind':'已评级跟踪','current':True})
    watchlist={'generated_at':now.isoformat(),'factpack_id':target['factpack_id'],
        'source_generated_at':selection['generated_at'],'current_focus':{'rows':[{
            **deepcopy(target),'watch_rank':position,'seat_kind':'已评级跟踪',
            'source_asof':target['factpack_asof'],'entry_opportunity':rec['entry_opportunity']}]}}
    return doc,selection,watchlist,now


def test_weekly_cannot_promote_a_score_rank_six_one_a_into_main_top_five():
    doc,selection,watchlist,now=ranked_fixture('1A',6)
    out=html(doc,selection,now,watchlist=watchlist)
    assert '超出当前正式榜本档名额' in out
    assert '当前评级 1A' not in out and '主榜1A第6名 · 同股同分' not in out
    assert '原评级 1A' in out and '?q=ZZTOP' in out


def test_weekly_cannot_reintroduce_fourth_candidate_outside_horizon_top3():
    for tier in ('2A','3A'):
        doc,selection,watchlist,now=ranked_fixture(tier,2)
        out=html(doc,selection,now,watchlist=watchlist)
        assert f'当前评级 {tier}' in out,tier
        assert f'短期Top2 · {tier}同股同分' in out,tier
        doc,selection,watchlist,now=ranked_fixture(tier,4)
        out=html(doc,selection,now,watchlist=watchlist)
        assert '超出当前正式榜本档名额' in out,tier
        assert f'当前评级 {tier}' not in out,tier
        assert '同股同分</a>' not in out,tier


def test_legacy_policy_changed_selection_and_duplicate_identity_cannot_claim_weekly():
    from copy import deepcopy
    for change in ('policy','selected','duplicate'):
        doc,selection,watchlist,now=linked_fixture()
        if change=='policy':doc['policy']['ranking']='central-focus-with-weekly-eligibility-v4-linked'
        elif change=='selected':doc['rows'][0]['master_focus']['selected']=False
        else:selection['excluded']=[deepcopy(selection['observations'][-1])]
        out=html(doc,selection,now,watchlist=watchlist)
        assert '当前评级 1A' not in out,change
        assert '同股同分</a>' not in out,change
        assert '?q=TEST' in out,change


def test_paused_weekly_record_never_claims_top_rank_or_current_order():
    from copy import deepcopy
    doc,selection,watchlist,now=linked_fixture()
    doc['rows'][0].update(eligible=False,central_tier=None)
    doc['rows'][0]['master_focus']['central_rank']=16
    out=html(doc,selection,now,watchlist=watchlist)
    assert '本股已是本市场通过周度条件的最高中央排序' not in out
    assert '本记录当前未通过周度条件' in out
    assert '当前不参与周度名次' in out and '审核分第16名' not in out
    assert '审核 75.00/100' in out and '?q=TEST' in out
