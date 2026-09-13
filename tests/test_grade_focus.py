"""Focus ranking changes attention, not authority or historical contracts."""
from copy import deepcopy
import json
import random

import pytest

import grade_focus as focus
from grade_card import system_table_html
from test_grade_card_safety import central_row, central_v2


def row(code, tier='1A', **kw):
    return central_row(code, '测试' + code, tier, tier + '_PREPARE', **kw)


def test_every_grade_has_independent_market_cap():
    rows = [row(code, tier) for tier in focus.GRADES
            for code in [*(f'{i:06}.SZ' for i in range(1,9)),
                         *(f'{i}.HK' for i in range(1,9)),
                         *('US' + chr(65+i) for i in range(8))]]
    # Same stock has one central grade; disjoint identities across grade groups.
    for i, r in enumerate(rows):
        if r['code'].endswith('.SZ'): r['code'] = f'{100000+i}.SZ'
        elif r['code'].endswith('.HK'): r['code'] = f'{100+i}.HK'
        else: r['code'] = 'US' + str(i)
    before = json.dumps(rows, sort_keys=True)
    result = focus.build(rows)
    assert result['selected_count'] == 6
    assert result['reserve_count'] == 66
    assert all(len(lane['selected_codes']) <= 3 for lane in result['by_horizon'].values())
    assert json.dumps(rows, sort_keys=True) == before
    assert result['no_grade_authority'] and result['model_calls'] == 0


def test_missing_hk_slots_do_not_overflow_into_other_markets():
    rows = [row('US'+str(i)) for i in range(12)] + [row('661.HK')]
    result = focus.build(rows)
    assert result['selected_count'] == 3
    assert result['by_horizon']['short']['summary']['1A']['港股']['minimum_gap'] == 0
    assert result['by_horizon']['short']['summary']['1A']['A股']['minimum_gap'] == 0
    assert len(result['records']) == 13
    assert all('保留评级' in r['reason'] for r in result['records'].values() if not r['selected'])


def test_input_shuffle_cannot_change_same_snapshot_order():
    rows = [row('US'+str(i)) for i in range(12)]
    first = focus.build(rows)
    random.Random(77).shuffle(rows)
    assert focus.build(rows) == first
    assert first['selected_codes'] == sorted(r['code'] for r in rows)[:3]


def test_audit_quality_precedes_larger_price_target():
    from profit_fixtures import short_inputs
    from review_scorecard import scorecard
    strong, weak = row('HIGHQUALITY'), row('HIGHTARGET')
    weak['trade_plan']['profit_inputs'] = short_inputs(14,14.5)
    weak['trade_plan']['take_profit_range'] = [14,14.5]
    g, b = weak['reviews']['gpt'], weak['reviews']['classics']
    for review in [g, *g['review_pair'].values()]:
        next(c for c in review['criteria'] if c['id'] == 'countercase')['score'] = 10
        review['verdict'] = review['thesis_verdict'] = '不否定'
    weak['scorecard'] = scorecard(g,b,gpt_current=True,book_current=True)
    weak['audit_score'] = weak['scorecard']['total']
    result = focus.build([weak,strong])
    assert result['selected_codes'] == ['HIGHQUALITY','HIGHTARGET']
    assert result['records']['HIGHTARGET']['metrics']['net_reward_risk'] > result['records']['HIGHQUALITY']['metrics']['net_reward_risk']


@pytest.mark.parametrize('mutate', [
    lambda r: r['scorecard']['gpt'].update(current=False),
    lambda r: r['scorecard'].update(total=float('nan')),
    lambda r: r.update(audit_score=99),
    lambda r: r['scorecard'].update(total=90),
    lambda r: r['trade_plan'].update(stop=999),
    lambda r: r.update(market='港股'),
])
def test_invalid_evidence_stays_visible_without_focus_place(mutate):
    r = row('EXAMPLE'); mutate(r)
    result = focus.build([r])
    assert result['selected_count'] == 0
    assert result['reserve_count'] == 1
    assert result['records']['EXAMPLE']['reason']


def test_alias_duplicates_do_not_receive_two_places():
    result = focus.build([row('601001.SH'),row('601001.SS')])
    assert result['selected_codes'] == []
    assert '重复' in result['records']['601001.SS']['reason']


def test_entry_conflict_cannot_change_formal_grade_order_but_keeps_execution_blocked():
    bad, good = row('CONFLICT'), row('COHERENT')
    # Price is still inside the original range; trend recovery is outside it.
    for result in [bad['scorecard']['gpt'], *bad['scorecard']['review_pair'].values()]:
        next(e for e in result['evidence'] if e['field']=='market_evidence.ma20')['value']=12
    original = deepcopy(bad)
    result = focus.build([bad, good])
    assert result['selected_codes'] == ['COHERENT','CONFLICT']
    assert not result['records']['CONFLICT']['entry_opportunity']['executable']
    assert result['records']['CONFLICT']['tier']=='1A'
    assert '无共同价带' in result['records']['CONFLICT']['entry_opportunity']['reasons'][0]
    assert bad == original and result['reserve_count']==0


def test_shadow_caution_does_not_replace_concrete_entry_explanation():
    triad=central_v2()
    out=system_table_html({}, {'rows':[{'code':'E','gates':{'开仓':'CAUTION'}}]}, {}, {}, triad=triad)
    assert '开仓需谨慎' not in out
    assert '进场必要条件与解除条件' in out and '条件观察' in out


def test_short_and_long_3a_have_independent_caps_and_keep_original_horizons():
    from review_scorecard import scorecard
    rows = [row('LONG'+str(i),'3A') for i in range(4)]
    for i in range(4):
        r = row('SHORT'+str(i),'2A')
        for c in r['reviews']['classics']['checks']: c['ok'] = True
        r['scorecard'] = scorecard(r['reviews']['gpt'],r['reviews']['classics'],gpt_current=True,book_current=True)
        r['audit_score'] = r['scorecard']['total']; r['tier'] = '3A'
        rows.append(r)
    result = focus.build(rows)
    assert result['selected_codes'] == ['SHORT0','SHORT1','SHORT2','LONG0','LONG1','LONG2']
    assert result['by_horizon']['short']['selected_codes']==['SHORT0','SHORT1','SHORT2']
    assert result['by_horizon']['long']['selected_codes']==['LONG0','LONG1','LONG2']
    assert result['records']['LONG0']['rank']==result['records']['SHORT0']['rank']==1
    assert result['records']['SHORT0']['metrics']['horizon'] == 'short'


def test_cross_bucket_limit_and_full_reserve_in_horizontal_renderer():
    triad = central_v2()
    for k in ['recommendations','preparations','blocked_3a','conditional','observations','pending']:
        triad[k] = []
    for i in range(9):
        triad[['recommendations','preparations','blocked_3a'][i % 3]].append(
            row('US'+str(i), '3A', publish=i % 3 == 0))
    original = deepcopy(triad)
    html = system_table_html({}, {}, {}, {}, triad=triad, limit_in=1)
    assert html.count("class='v88-focus-row'") == 3
    assert html.count("data-grade='3A'") == 3
    assert '候补跟踪（5只）' in html
    assert '入选依据与分数分解' in html
    assert 'colspan=\'11\'' in html
    assert triad == original
    for i in range(8):
        assert 'US'+str(i) in html


def test_shared_core_algorithm_is_byte_identical():
    from pathlib import Path
    core = Path('/Users/bluesteven/Desktop/ai-daily-report-v2/src/grade_focus.py')
    if not core.exists(): pytest.skip('separate deployment')
    assert core.read_bytes() == Path(focus.__file__).read_bytes()


def test_three_horizon_top3_independent_and_no_minimum_market_quota():
    rows=[]
    for horizon,base in [('short',65),('medium',75),('long',90)]:
        for i in range(6):
            rows.append({'code':horizon+str(i),'market':['A股','美股','港股'][i%3],
                         'horizon':horizon,'tier':'1A','audit_score':base-i,
                         'central_rank':i+1,'entry_opportunity':{'focus_eligible':True}})
    original=deepcopy(rows)
    out=focus.attention_rows(rows)
    assert len(out)==9 and rows==original
    for horizon in focus.HORIZONS:
        selected=[r for r in out if r['horizon']==horizon]
        assert [r['code'] for r in selected]==[horizon+str(i) for i in range(3)]
        assert [r['watch_rank'] for r in selected]==[1,2,3]
    assert len(focus.attention_rows(rows[:1]))==1
    rows[0]['entry_opportunity']['focus_eligible']=False
    assert 'short0' in [r['code'] for r in focus.attention_rows(rows)]
