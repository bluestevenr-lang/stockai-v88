from copy import deepcopy
import numpy as np
import pandas as pd
import pytest
from deep_synthesis import observations,build,html,DOMAINS


def frame():
 c=np.linspace(80,120,220);c[-1]=c[-2]*.965
 return pd.DataFrame({'Close':c,'Volume':[100.]*219+[190.]},index=pd.bdate_range(end='2026-09-11',periods=220))


def context(monkeypatch,code):
 monkeypatch.setattr('review_contract.known_protocol',lambda *a:True)
 domains={k:{'status':'limited' if k in ('fundamental','valuation','news') else 'observed','limitations':['待核正文']} for k in DOMAINS}
 annex={'domains':domains}
 pair={role:{'domain_reviews':[{'domain':k,'conclusion':'中性','reason':'有限证据不形成支持','effects':['thesis']} for k in DOMAINS]} for role in ('primary','counteraudit')}
 row={'code':code,'tier':'1A','horizon':'short','factpack_id':'pack','joint_evidence':annex,'trade_plan':{'stop':80},'reviews':{'gpt':{'review_pair':pair}}}
 return {'code':code,'row':row,'fact':{'joint_evidence':deepcopy(annex)},'selection':{'factpack_id':'pack'},
         'loaded_factpack_id':'pack','card':{'gpt':{'current':True,'complete':True}},'formal':False}


@pytest.mark.parametrize('code',['688002.SS','GRPN','2696.HK'])
def test_one_snapshot_reconciles_strength_and_daily_reversal_for_all_markets(monkeypatch,code):
 ctx=context(monkeypatch,code);before=deepcopy(ctx)
 cross={'checks':[{'id':k,'status':'pass'} for k in ('identity','binding','session','basis','facts','quant','invalidation')],'current_grade':'1A','audit_score':70}
 turn={'turning':{'side':'top','signals':['单日下跌3.5%且放量']}}
 r=build(ctx,frame(),{'snapshot_signature':'quote'}, {},turn,cross)
 assert r['annual_label']=='年线上方' and '短线转弱预警' in r['momentum_label']
 assert r['status']=='存在分歧·复核优先' and r['entry_recheck_required']
 assert r['joint_review_current'] and len(r['domains'])==8
 assert '未形成完整共同支持' in r['business_conclusion']
 assert not r['entry_permission'] and ctx==before
 assert 'v88-deep-synthesis' in html(r)


def test_insufficient_history_does_not_invent_yearly_strength():
 r=observations(frame().tail(20));assert r['ma200'] is None and r['annual_position']=='年线数据不足'
 r=observations(None);assert r['last'] is None and r['rsi14'] is None


def test_missing_or_mismatched_company_review_cannot_become_fundamental_support(monkeypatch):
 ctx=context(monkeypatch,'TEST');ctx['fact']['joint_evidence']={}
 r=build(ctx,frame(),{}, {},{}, {'checks':[]})
 assert not r['joint_review_current'] and all(d['status']=='unreviewed' for d in r['domains'])
 assert r['entry_recheck_required'] and not r['entry_permission']


def test_volume_window_excludes_today():
 r=observations(frame());assert r['volume_ratio20']==1.9 and r['return1_pct']==pytest.approx(-3.5)


def test_no_signal_does_not_assert_trend_will_continue():
 r=build({},frame(),{}, {},{}, {})
 assert '不据此保证趋势延续' in r['explanation']


def test_turn_warning_does_not_invent_money_flow_or_a_confirmed_top():
 from cloud_engine import turning_point
 df=frame();df['High']=df.Close+1;df['Low']=df.Close-1
 full={'ma':{20:float(df.Close.tail(20).mean())},'macd_txt':'','pos52':70}
 turn=turning_point(df,full)
 assert turn['side']=='top' and turn['confirmed_reversal'] is False
 assert turn['volume_ratio20']==1.9 and '未确认顶部' in turn['label']
 assert not any(x in ''.join(turn['signals']) for x in ('主动抛售','对倒出货','资金回场'))


def test_touching_old_high_does_not_claim_breakout():
 from cloud_engine import turning_point
 df=frame();df['High']=200.;df['Low']=df.Close-1;df['Volume']=100.
 full={'ma':{20:float(df.Close.tail(20).mean())},'macd_txt':'','pos52':70}
 turn=turning_point(df,full)
 assert not any('突破此前60日高点' in x for x in (turn or {}).get('signals',[]))
