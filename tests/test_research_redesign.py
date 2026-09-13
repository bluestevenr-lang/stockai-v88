from copy import deepcopy
import json
import math
import random
import grade_focus
from test_grade_focus import row


def test_research_windows_are_exact_overlapping_agendas_and_do_not_extend_contracts():
    assert grade_focus.RESEARCH_WEEKS=={'short':(0,8),'medium':(8,24),'long':(12,36)}
    r=row('688002.SS'); original=deepcopy(r)
    window=grade_focus.research_window(r)
    assert window['start_week']==0 and window['end_week']==8
    assert window['contract_max_calendar_days']==r['trade_plan']['profit_inputs']['max_calendar_days']
    assert window['no_contract_extension'] and r==original
    assert grade_focus.research_window({'horizon':'unknown'}) is None


def test_688002_research_survives_entry_conflict_without_becoming_executable():
    r=row('688002.SS')
    for review in [r['scorecard']['gpt'],*r['scorecard']['review_pair'].values()]:
        next(e for e in review['evidence'] if e['field']=='market_evidence.ma20')['value']=99
    before=json.dumps(r,sort_keys=True)
    ref=grade_focus.build([r])['records']['688002.SS']
    assert ref['selected'] and ref['rank']==1 and ref['horizon']=='short'
    assert ref['tier']=='1A' and ref['metrics']['audit_score']==r['audit_score']
    assert not ref['entry_opportunity']['focus_eligible'] and not ref['entry_opportunity']['executable']
    assert '无共同价带' in ' '.join(ref['entry_opportunity']['reasons'])
    assert json.dumps(r,sort_keys=True)==before


def test_reserves_merge_all_sources_before_cap_and_never_invent_scores():
    rows=[]
    for market,prefix in [('A股','CN'),('美股','US'),('港股','HK')]:
        for i in range(12):
            rows.append({'code':prefix+str(i),'market':market,'audit_score':80-i if i<9 else None,'tier':'1A' if i<9 else None})
    before=deepcopy(rows);random.Random(91).shuffle(rows)
    out=grade_focus.ranked_reserves(rows)
    assert len(out)==15 and all(len([r for r in out if r['market']==m])==5 for m in grade_focus.MARKETS)
    for m in grade_focus.MARKETS:
        subset=[r for r in out if r['market']==m]
        assert [r['audit_score'] for r in subset]==[80,79,78,77,76]
        assert [r['reserve_rank'] for r in subset]==[1,2,3,4,5]
    assert grade_focus.ranked_reserves(before)==out
    sparse=grade_focus.ranked_reserves([{'code':'U','market':'美股','audit_score':None},{'code':'S','market':'美股','audit_score':60}])
    assert [r['code'] for r in sparse]==['S','U'] and sparse[-1]['audit_score'] is None


def test_current_lower_score_wins_duplicate_old_higher_score():
    current={'code':'688002.SS','market':'A股','audit_score':60}
    old={**current,'code':'688002.SH','audit_score':95}
    out=grade_focus.ranked_reserves([current,old])
    assert len(out)==1 and out[0]['audit_score']==60


def test_score_first_not_grade_or_entry_first():
    rows=[{'code':'HIGH','market':'美股','horizon':'short','tier':'1A','audit_score':80,'entry_opportunity':{'focus_eligible':False}},
          {'code':'LOW','market':'美股','horizon':'short','tier':'3A','audit_score':75,'entry_opportunity':{'focus_eligible':True}}]
    assert [r['code'] for r in grade_focus.attention_rows(rows)]==['HIGH','LOW']


def test_multi_classics_cannot_hide_risk_or_reuse_evidence_as_new_votes():
    from classics_framework import evaluate,html
    card={'books':{'current':True,'complete':True,'checks':[
        {'id':'trend','ok':True,'evidence':{'close':100}},
        {'id':'entry','ok':True,'evidence':{'close':100}},
        {'id':'stop','ok':False,'evidence':{'stop':0}},
        {'id':'tharp_expectancy','ok':None,'evidence':{'settled_n':None}}]}}
    original=deepcopy(card);r=evaluate(card);domains={x['id']:x for x in r['domains']}
    assert domains['trend']['status']=='支持' and domains['capital']['status']=='反对'
    assert domains['enterprise']['status']=='未覆盖' and domains['validation']['status']=='缺证'
    assert r['unique_observations']==2 and r['reused_observations']==1
    assert r['score_adjustment']==0 and r['no_grade_authority'] and card==original
    assert '设计参考' in html(card) and '专业投机原理' in html(card)
    card['books']['current']=False
    assert all(d['status']=='待重核' for d in evaluate(card)['domains'])


def test_descriptive_history_is_never_oos_certification():
    from classics_framework import evaluate
    r=evaluate({'books':{'current':True,'complete':True,'checks':[{'id':'tharp_expectancy','ok':True,'evidence':{'settled_n':100}}]}})
    assert next(d for d in r['domains'] if d['id']=='validation')['status']=='历史支持·样本外待证'


def test_new_prompt_keeps_exact_legacy_protocol_but_rejects_mismatched_joint_scope():
    from review_contract import profile,known_protocol,LEGACY_PROTOCOLS
    for row in ({},{'joint_evidence':{}}):
        new=profile(row);legacy=LEGACY_PROTOCOLS['joint_evidence' in row]
        assert new!=legacy
        assert known_protocol(dict(zip(('review_schema_version','prompt_hash'),legacy)),row)
        assert known_protocol(dict(zip(('review_schema_version','prompt_hash'),new)),row)
        wrong=LEGACY_PROTOCOLS['joint_evidence' not in row]
        assert not known_protocol(dict(zip(('review_schema_version','prompt_hash'),wrong)),row)
        assert not known_protocol({'review_schema_version':new[0],'prompt_hash':'fabricated'},row)


def test_untriggered_entry_and_insufficient_outcomes_are_not_negative_theses():
    from classics_framework import evaluate
    checks=[{'id':'trend','ok':True,'evidence':{'chg60':10}},
            {'id':'entry','ok':False,'evidence':{'confirmation_stage':0}},
            {'id':'tharp_expectancy','ok':False,'evidence':{'status':'INSUFFICIENT_SAMPLE','settled_n':0}}]
    domains={d['id']:d for d in evaluate({'books':{'current':True,'complete':True,'checks':checks}})['domains']}
    assert domains['trend']['status']=='入场待确认' and domains['trend']['failed']==[]
    assert domains['validation']['status']=='缺证' and domains['validation']['failed']==[]
    checks[-1]['evidence']['raw_status']='NONPOSITIVE_HISTORY'
    assert evaluate({'books':{'current':True,'complete':True,'checks':checks}})['domains'][-1]['status']=='反对'
