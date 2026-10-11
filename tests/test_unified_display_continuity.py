from copy import deepcopy
import pytest
import unified_evaluation as ue
from trend_scenarios import build_stock
from future_trend_visual import render


def evaluation(code='AEM'):
    return {'code':code,'verdict':'3A待买点','score':76,'annual_regime':'up','annual_label':'上行',
            'business_supported':True,'business_source':'数据商财报','market':'美股','last':123.4,
            'quote_asof':'2026-10-09T16:00:00-04:00',
            'tiers':{'2A':{'verdict':'2A·现在可买','buyable':True,'plan_enabled':True,
                           'entry_range':[122,124],'stop':115,'exit_text':'收盘跌破20日线离场','net_rr':1.7}}}


@pytest.mark.parametrize('gap', ['missing_history','mismatched_receipt'])
def test_chart_gap_does_not_erase_matching_board_verdict(gap):
    from test_trend_scenarios import inputs
    annual,synthesis,period=inputs('AEM',market='美股',regime='up')
    annual['unified']=evaluation()
    if gap=='missing_history':annual.update(data_status='limited',valid12month=False)
    else:period['annual_input_id']='different-receipt'
    before=deepcopy((annual,synthesis,period))
    future=build_stock(annual,synthesis,period,code='AEM')
    assert future['status']!='ready' and not future.get('points')
    assert future['unified']==annual['unified']
    markup=render(future)
    assert '2A·现在可买' in markup and '123.4' in markup
    assert '未进入本轮统一评估池' not in markup
    assert (annual,synthesis,period)==before


def test_other_security_verdict_cannot_cross_into_chart():
    from test_trend_scenarios import inputs
    annual,synthesis,period=inputs('AEM',market='美股',regime='up')
    annual['unified']=evaluation('OTHER')
    future=build_stock(annual,synthesis,period,code='AEM')
    assert not future.get('unified')


def test_equal_size_replacement_does_not_reuse_old_symbol_index(monkeypatch):
    # Simulate recycled object IDs after successive published snapshots.
    monkeypatch.setattr(ue,'id',lambda value:7,raising=False)
    ue._INDEX.clear()
    first={'grade_policy':ue.CURRENT_POLICIES[0],'evaluations':{'5.HK':evaluation('5.HK')}}
    second={'grade_policy':ue.CURRENT_POLICIES[0],'evaluations':{'700.HK':evaluation('700.HK')}}
    assert ue.lookup('0005.HK',first)['code']=='5.HK'
    assert ue.lookup('0700.HK',second)['code']=='700.HK'
    assert ue.lookup('0005.HK',second) is None


def test_lookup_returns_detached_record():
    doc={'grade_policy':ue.CURRENT_POLICIES[0],'evaluations':{'AEM':evaluation()}}
    result=ue.lookup('AEM',doc)
    result['tiers']['2A']['entry_range'][0]=0
    assert doc['evaluations']['AEM']['tiers']['2A']['entry_range']==[122,124]


def test_opening_session_invalidates_old_buy_labels_without_changing_score():
    from datetime import datetime,timezone
    record=dict(evaluation(),source_session='2026-10-09',buyable=True,verdict='3A·现在可买')
    doc={'grade_policy':ue.CURRENT_POLICIES[0],'generated_at':'2026-10-11T08:00:00+00:00',
         'market_source_generated_at':{'美股':'2026-10-10T01:00:00+00:00'},'evaluations':{'AEM':record}}
    before=deepcopy(doc)
    weekend=ue.lookup('AEM',doc,now=datetime(2026,10,11,8,tzinfo=timezone.utc))
    assert weekend['buy_timing']=='next_open' and weekend['buyable']
    opened=ue.lookup('AEM',doc,now=datetime(2026,10,12,15,tzinfo=timezone.utc))
    assert not opened['buyable'] and not opened['tiers']['2A']['buyable']
    assert '现在可买' not in ue.html(opened)
    assert opened['score']==record['score']
    assert opened['board_generated_at']=='2026-10-10T01:00:00+00:00'
    assert 'USD' in ue.html(opened)
    assert doc==before
