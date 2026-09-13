from copy import deepcopy
from datetime import datetime, timezone, timedelta
from html.parser import HTMLParser
import numpy as np
import pandas as pd
import pytest

from deep_analysis_data import snapshot_signature
from deep_cross_validation import reconcile, scenario, html, basis
from market_data_helper import _core
_core()
from recommendation_gate import _trade_plan
from profit_contract import evaluate, VERSION
from v88_decision_core import evaluate_decision

NOW = datetime(2026, 9, 12, 17, tzinfo=timezone(timedelta(hours=8)))


@pytest.fixture
def case():
    close = np.linspace(100, 177, 100)
    df = pd.DataFrame({'Open': close, 'Close': close, 'High': close+2, 'Low': close-2,
                       'Volume': np.full(100, 1e6)}, index=pd.bdate_range(end='2026-09-11', periods=100))
    inp = {'version': VERSION, 'horizon':'short', 'kind':'observed_20day_boundary',
           'max_calendar_days':30, 'holding_sessions':10, 'asof':'2026-09-04T15:00:00+08:00',
           'source_url':'https://example.invalid/test', 'peaks':[{'date':'2026-09-03','high':185.96}, {'date':'2026-09-04','high':185.99}]}
    fact = {'code':'688002.SS', 'market':'A股', 'horizon':'short', 'last':177., 'stop':153.41,
            'entry_range':[161.232,164.27], 'profit_inputs':inp, 'profit_zone':[185.96,185.99],
            'source_timestamps':{'yahoo_daily':'2026-09-11T15:00:00+08:00'},
            'data_provenance':{'daily_date':'2026-09-11','price_basis':'split-adjusted OHLC; not total return'},
            'market_evidence':{'ma20':float(df.Close.tail(20).mean()), 'ma60':float(df.Close.tail(60).mean()),
                               'observed_low10':float(df.Low.tail(10).min()), 'volume_ratio20':1.,
                               'return20_pct':float((df.Close.iloc[-1]/df.Close.iloc[-21]-1)*100)}}
    fact['profit_contract'] = evaluate({'entry_range':fact['entry_range'],'stop':fact['stop'],'profit_inputs':inp}, 'short', now=NOW)
    plan = _trade_plan(fact)
    card = {'total':65, 'gpt':{'current':True,'complete':True}, 'books':{'current':True,'complete':True}}
    ctx = {'code':'688002.SS', 'selection':{'factpack_id':'test-pack'}, 'loaded_factpack_id':'test-pack',
           'row':{'code':'688002.SS','horizon':'short','tier':'1A','audit_score':65,'factpack_id':'test-pack','trade_plan':plan},
           'fact':fact, 'card':card, 'formal':False}
    quality = {'code':'688002.SS','source_asof':'2026-09-11','price_basis':'split-adjusted OHLC; not total return',
               'snapshot_signature':snapshot_signature(df)}
    technical = evaluate_decision(df, {}, code='688002.SS')
    return ctx, df, quality, technical


def status(report, key):
    return next(x['status'] for x in report['checks'] if x['id']==key)


def test_distinct_scores_same_plan_and_current_price_costs(case):
    before = deepcopy(case[0])
    r = reconcile(*case, now=NOW)
    assert r['status']=='一致·等原条件'
    assert r['audit_score']==65 and r['technical_score']!=65
    assert r['original_net_space']==pytest.approx(12.07746499135)
    assert r['original_net_rr']==pytest.approx(1.601719899746)
    assert r['current_price_scenario']['net_upside_pct']==pytest.approx(4.016752396211)
    assert r['current_price_scenario']['net_reward_risk']==pytest.approx(.283067341995)
    assert r['current_price_meets_numeric_hurdles'] is False
    assert r['no_grade_authority'] and not r['independent_data_validation']
    assert r['model_calls']==0 and r['predictive_win_probability'] is None
    assert case[0]==before


@pytest.mark.parametrize('mutation,key', [
    (lambda c: c[2].update(code='GRPN'), 'identity'),
    (lambda c: c[0].update(loaded_factpack_id='different'), 'binding'),
    (lambda c: c[0]['row']['trade_plan'].update(stop=150), 'contract'),
    (lambda c: c[0]['row']['trade_plan']['profit_contract'].update(net_upside_pct=50), 'contract'),
    (lambda c: c[2].update(source_asof='2026-09-10'), 'session'),
    (lambda c: c[2].update(price_basis='unadjusted'), 'basis'),
    (lambda c: c[2].update(price_basis='unknown'), 'basis'),
    (lambda c: c[0]['fact']['market_evidence'].update(ma20=1), 'facts'),
    (lambda c: c[3].update(unified_score=99), 'quant'),
    (lambda c: c[0]['card']['books'].update(current=False), 'reviews'),
    (lambda c: c[0]['row'].update(audit_score=99), 'reviews'),
])
def test_conflict_fails_closed_without_promoting(case, mutation, key):
    mutation(case)
    r = reconcile(*case, now=NOW)
    assert status(r,key)=='gap' and r['status']=='需复核'
    assert r['next_actions'] and r['no_grade_authority']


def test_a_recovered_price_does_not_erase_old_stop_breach(case):
    case[1].loc['2026-09-08','Close']=150
    r = reconcile(*case,now=NOW)
    assert status(r,'invalidation')=='gap'
    assert any('2026-09-08' in x['detail'] for x in r['checks'])


def test_expired_and_new_session_do_not_inherit_score(case):
    r=reconcile(*case,now=NOW+timedelta(days=31))
    assert status(r,'contract')=='gap' and status(r,'session')=='gap'


def test_missing_data_never_outputs_zero_score_or_quote_scenario(case):
    r=reconcile(case[0],None,{}, {},now=NOW)
    assert r['technical_score'] is None and r['current_price_scenario'] is None
    assert r['status']=='需复核'


def test_hash_changes_when_inputs_change_not_just_clock(case):
    a=reconcile(*case,now=NOW); b=reconcile(*case,now=NOW+timedelta(minutes=1))
    assert a['input_id']==b['input_id']
    case[3]['unified_score']+=1
    assert reconcile(*case,now=NOW)['input_id']!=a['input_id']


def test_html_precision_scope_and_escape(case):
    r=reconcile(*case,now=NOW);r['status']='<script>bad</script>'
    body=html(r)
    assert '<script>' not in body and '&lt;script&gt;' in body
    assert '161.232 ～ 164.27' in body and '4.02%' in body
    assert 'overflow-x:auto' in body and '量价辅助分' in body and '中央审核证据分' in body


class VisibleText(HTMLParser):
    """Text outside initially closed details, without relying on CSS rendering."""
    def __init__(self):
        super().__init__();self.detail_stack=[];self.text=[]
    def handle_starttag(self,tag,attrs):
        if tag=='details':self.detail_stack.append('open' not in dict(attrs))
    def handle_endtag(self,tag):
        if tag=='details':self.detail_stack.pop()
    def handle_data(self,value):
        if not any(self.detail_stack):self.text.append(value)


def test_numeric_details_fold_without_hiding_actual_conflict_or_source_date(case):
    case[0]['fact']['market_evidence']['ma20']=1
    report=reconcile(*case,now=NOW);before=deepcopy(report)
    body=html(report);visible=VisibleText();visible.feed(body)
    front=' '.join(visible.text)
    assert '需复核' in front and report['source_asof'] in front
    assert all(c['title'] in front for c in report['checks'] if c['status']=='gap')
    assert '中央审核证据分' not in front and '滚动技术参考' not in front
    assert all(c['detail'] in body for c in report['checks'])
    assert '161.232 ～ 164.27' in body and report==before


def test_consistent_report_does_not_invent_a_visible_gap(case):
    report=reconcile(*case,now=NOW);visible=VisibleText();visible.feed(html(report))
    front=' '.join(visible.text)
    assert report['status'] in front and report['source_asof'] in front
    assert '待核实 ' not in front


@pytest.mark.parametrize('args',[(float('nan'),10,8,.5,.5),(0,10,8,.5,.5),(7,10,8,.5,.5),(9,10,8,-1,.5)])
def test_bad_cost_or_prices_rejected(args):
    assert scenario(*args) is None


def test_basis_does_not_infer_equivalence_from_provider():
    assert basis('Yahoo daily unknown') is None
    assert basis('split-adjusted OHLC; not total return')!=basis('unadjusted')


def test_stored_price_rounding_is_not_a_real_conflict(case):
    case[1].iloc[-1,case[1].columns.get_loc('Close')]=177.000001
    r=reconcile(*case,now=NOW)
    assert status(r,'facts')=='pass'
    case[1].iloc[-1,case[1].columns.get_loc('Close')]=177.001
    assert status(reconcile(*case,now=NOW),'facts')=='gap'


def test_declared_provider_bases_are_explicit_and_not_interchangeable():
    t=basis('腾讯前复权OHLC整段序列；不是含息总回报，不能与Yahoo拆股口径逐行拼接')
    s=basis('Sina CN original OHLC divided by source qfq factor; native unadjusted share volume; not total return')
    assert t and s and t!=s and t!=basis('split-adjusted OHLC; not total return')


def test_unmapped_security_is_not_silently_classified_as_consumer():
    from modules.sector_map import get_sector
    assert get_sector('688002.SS','睿创微纳')=='待核行业'
    assert get_sector('UNMAPPED','')=='待核行业'
    assert get_sector('600519.SS','贵州茅台')=='🛒 消费'


def test_verified_identity_factor_basis_reconciles_without_equating_other_series(case):
    proven='腾讯原始OHLC整段；新浪明确完整前复权因子在所载日期均为1且偏移0，价格无修改；不是含息总回报'
    case[0]['fact']['data_provenance']['price_basis']=proven
    case[2]['price_basis']=proven
    before=deepcopy(case[0])
    result=reconcile(*case,now=NOW)
    assert status(result,'basis')=='pass' and status(result,'facts')=='pass'
    assert case[0]==before and result['no_grade_authority']
    assert basis(proven) not in (None,basis('unadjusted'),basis('qfq'),
        basis('腾讯前复权OHLC整段序列；不是含息总回报，不能与Yahoo拆股口径逐行拼接'))
    assert basis('腾讯原始OHLC整段') is None
    assert basis(proven.replace('均为1且偏移0','可能为1')) is None
    case[2]['price_basis']='unadjusted'
    assert status(reconcile(*case,now=NOW),'basis')=='gap'
