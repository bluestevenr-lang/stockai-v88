from copy import deepcopy
from datetime import datetime, timezone, timedelta
import pytest
import next_session_model as model

NOW=datetime(2026,9,13,12,tzinfo=timezone(timedelta(hours=8)))

@pytest.fixture(autouse=True)
def profiles(monkeypatch):
    monkeypatch.setattr('stock_profile_view.profile', lambda code,doc,now: {'code':code,'market':model._market(code),
        'profile_fresh':True,'industry':'半导体产品' if model._market(code)=='美股' else '银行'})


def data(direction='up',stock='NVDA'):
    return {'generated_at':'2026-09-13 09:20','scanned':864,'stocks':[{'code':stock,'name':'test',
      'direction':direction,'strength':70,'phase':'切换','trigger':'原技术触发','invalid':'原技术反证',
      'source_status':'verified','source_date':'2026-09-11'}]}


def rotation(week=-3,month=-5):
    return {'snapshot_id':'sector-1','analysis_time':'2026-09-13 05:56',
      'source_dates_by_market':{'美股':['2026-09-11'],'港股':['2026-09-11']},
      'trajectories':{'美股':[{'name':'科技','facts':{'5d':week,'20d':month},
                              'turning':{'status':'unverified','type':'转折时间未核实'}}],
                      '港股':[{'name':'高股息','facts':{'5d':9,'20d':10}}]}}


def central(stock='NVDA'):
    return {'factpack_id':'central-1','source_generated_at':'2026-09-13 09:55（北京时间）',
      'rows':[{'code':stock,'tier':'1A','audit_score':62.5,'trade_plan':{'stop':30,'entry_range':[32,33]},
               'master_ref':{'factpack_id':'central-1'},'entry_opportunity':{'executable':False}}]}


def row(doc):
    return next(r for m in doc['markets'] for r in m['rows'])


def test_opposite_sector_blocks_strength_from_becoming_entry_and_keeps_central_contract():
    inputs=[data(),rotation(),central()];before=deepcopy(inputs)
    result=model.build(inputs[0],inputs[1],central=inputs[2],profiles={},now=NOW);r=row(result)
    assert r['sector_state']=='opposed' and '反向' in r['next_action']
    assert r['central_score']==62.5 and r['rule_score']==70
    assert r['central_plan']==inputs[2]['rows'][0]['trade_plan']
    assert '代理' in r['sector_label'] and '未验证' in r['sector_detail']
    assert result['entry_permission'] is False and inputs==before


def test_same_market_theme_never_implies_industry_membership_and_hk_codes_join():
    result=model.build(data(stock='00358.HK'),rotation(),central=central('358.HK'),profiles={},now=NOW)
    r=row(result)
    assert r['code']=='358.HK' and r['central_tier']=='1A'
    assert r['sector_state']=='missing' and '高股息' not in r['sector_label']


def test_old_scan_time_cannot_renew_old_prices_or_current_signal():
    signals=data();signals['stocks'][0]['source_date']='2026-09-04'
    r=row(model.build(signals,rotation(),central=central(),profiles={},now=NOW))
    assert r['direction']=='mixed' and r['rule_score'] is None
    assert r['next_action'].startswith('先补')


def test_stale_central_version_never_uses_saved_grade_or_score():
    c=central();c['source_generated_at']='2026-09-01 09:55'
    r=row(model.build(data(),rotation(),central=c,profiles={},now=NOW))
    assert not r['central_current'] and r['central_score'] is None and r['central_tier'] is None


def test_stale_sector_source_not_a_no_risk_message():
    rot=rotation();rot['source_dates_by_market']['美股']=['2026-09-04']
    r=row(model.build(data(),rot,central=central(),profiles={},now=NOW))
    assert r['sector_state']=='missing' and '日期待更新' in r['sector_label']
    assert '无顶部' not in r['sector_detail']


def test_disagreement_is_not_silently_averaged():
    r=row(model.build(data(),rotation(5,-2),central=central(),profiles={},now=NOW))
    assert r['sector_state']=='mixed' and '周月' in r['next_action']


def test_all_markets_remain_visible_without_fabricated_rows():
    result=model.build({}, {},central={},profiles={},now=NOW)
    assert [m['market'] for m in result['markets']]==['A股','港股','美股']
    assert all(m['next_session']=='2026-09-14' and not m['rows'] for m in result['markets'])


def test_next_open_respects_local_market_clock_before_open():
    now=datetime(2026,9,14,8,tzinfo=NOW.tzinfo)
    result=model.build({}, {}, central={},profiles={},now=now)
    assert all(m['next_session']=='2026-09-14' for m in result['markets'])
    now=datetime(2026,9,14,22,tzinfo=NOW.tzinfo)
    result=model.build({}, {}, central={},profiles={},now=now)
    assert {m['market']:m['next_session'] for m in result['markets']}=={'A股':'2026-09-15','港股':'2026-09-15','美股':'2026-09-14'}
