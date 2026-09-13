"""Future first presentation checked with the actual bound scenario producer."""
from copy import deepcopy
from html.parser import HTMLParser

import numpy as np
import pandas as pd
import pytest

from stock_horizon import build_horizon_facts, cycle_visual_html
from trend_scenarios import build_stock


def bound_result(code='688002.SS', name='睿创微纳', market='A股'):
    index = pd.date_range(end='2026-09-11', periods=180, freq='B')
    close = np.linspace(100, 160, len(index))
    frame = pd.DataFrame({'Open': close-.5, 'High': close+2, 'Low': close-2,
                          'Close': close, 'Volume': np.linspace(1e6, 1.5e6, len(index))}, index=index)
    facts = build_horizon_facts(frame)
    sig = facts['data_signature']
    phases = [{'outlook': f'第{i+1}阶段先按条件观察', 'confirmation': f'第{i+1}阶段价格与企业证据确认',
               'invalidation': f'第{i+1}阶段结构失守重新评估'} for i in range(4)]
    common = {'code': code, 'source_asof':'2026-09-11', 'snapshot_signature':sig}
    annual = {**common, 'input_id':'annual-actual-fixture', 'market':market,
              'outlook_start':'2026-09-13', 'data_status':'complete', 'valid12month':True,
              'regime':'up_caution', 'top_warning':True, 'phases':phases,
              'headline':'先消化当前转弱风险，修复后再看延续',
              'evidence':{'company_support':False, 'company_adverse':False}}
    synthesis = {**common, 'input_id':'synthesis-actual-fixture', 'turning':{'side':'top'},
                 'original_plan':{'entry_range':[100,110], 'stop':90, 'take_profit_range':[140,145]},
                 'central_grade':'1A', 'central_audit_score':70, 'review_blocks':[]}
    period = {**common, 'input_id':'period-actual-fixture', 'status':'caution',
              'annual_input_id':annual['input_id'], 'synthesis_input_id':synthesis['input_id'],
              'headline':'先防回撤，再验证条件', 'summary':'同包年度与周期复核'}
    future = build_stock(annual, synthesis, period, code=code, name=name)
    assert future['status'] == 'ready'
    return {'facts':facts, 'review':{'status':'not_requested'}, 'future_scenario':future,
            'period_consistency':period, 'decision':deepcopy(synthesis['original_plan'])}


class Tags(HTMLParser):
    def __init__(self, text):
        super().__init__(); self.items=[]; self.feed(text)

    def handle_starttag(self, tag, attrs):
        self.items.append((tag,dict(attrs)))


def test_bound_future_is_primary_and_history_is_collapsed_without_mutation():
    result = bound_result(); before = deepcopy(result)
    html = cycle_visual_html(result, '睿创微纳', '688002.SS')
    assert result == before
    assert html.index('class="v88-future"') < html.index('class="hz-history-archive"')
    assert 'vf-phase-arrow' in html and 'vf-base-path' in html
    assert html.count('class="vf-future-point"') == 7
    assert '证据截至 2026-09-11' in html and '研判起点 2026-09-13' in html
    assert '2027-09-12' in html and '过去8周档' in html
    history = [attrs for tag,attrs in Tags(html).items
               if tag == 'details' and attrs.get('class') == 'hz-history-archive']
    assert len(history) == 1 and 'open' not in history[0]
    assert result['decision'] == before['decision']


@pytest.mark.parametrize('code,name,market',[
    ('688002.SS','睿创微纳','A股'),
    ('SEPN','Septerna 赛普特纳','美股'),
    ('2696.HK','复宏汉霖','港股')])
def test_chinese_and_english_company_names_survive_actual_producer(code,name,market):
    result=bound_result(code,name,market)
    html=cycle_visual_html(result,name,code)
    assert name+' · '+code+' · 未来趋势研判' in html
    assert html.count('class="vf-future-point"') == 7
    assert '未来趋势与本页事实待重新关联' not in html


def test_cn_exchange_alias_is_the_same_stock():
    result=bound_result()
    html=cycle_visual_html(result,'睿创微纳','688002.SH')
    assert 'vf-base-path' in html


@pytest.mark.parametrize('damage',[
    'wrong_symbol','wrong_future_code','wrong_period_code','wrong_period_id',
    'blocked_period','missing_period','wrong_source_date','wrong_annual_ref','wrong_synthesis_ref',
    'malformed_future','malformed_period','wrong_future_snapshot','missing_future_snapshot',
    'wrong_period_snapshot',
])
def test_inconsistent_current_binding_never_draws_a_future_curve(damage):
    result=bound_result(); symbol='688002.SS'
    if damage=='wrong_symbol': symbol='SEPN'
    elif damage=='wrong_future_code': result['future_scenario']['code']='2696.HK'
    elif damage=='wrong_period_code': result['period_consistency']['code']='SEPN'
    elif damage=='wrong_period_id': result['period_consistency']['input_id']='different-period'
    elif damage=='blocked_period': result['period_consistency']['status']='blocked'
    elif damage=='missing_period': result.pop('period_consistency')
    elif damage=='wrong_source_date': result['period_consistency']['source_asof']='2026-09-10'
    elif damage=='wrong_annual_ref': result['period_consistency']['annual_input_id']='different-annual'
    elif damage=='wrong_synthesis_ref': result['period_consistency']['synthesis_input_id']='different-synthesis'
    elif damage=='wrong_future_snapshot': result['future_scenario']['snapshot_signature']='other-series'
    elif damage=='missing_future_snapshot': result['future_scenario'].pop('snapshot_signature')
    elif damage=='wrong_period_snapshot': result['period_consistency']['snapshot_signature']='other-series'
    elif damage=='malformed_future': result['future_scenario']=['bad-cache']
    elif damage=='malformed_period': result['period_consistency']=['bad-cache']
    before=deepcopy(result)
    html=cycle_visual_html(result,'待核名称',symbol)
    assert result==before
    assert 'class="vf-base-path"' not in html and 'class="vf-current-phase"' not in html
    assert '未来路径等待证据补齐' in html


def test_model_missing_source_evidence_cannot_be_repaired_by_wrapper():
    result=bound_result(); result['future_scenario']['status']='missing'
    result['future_scenario']['points']=[]
    result['future_scenario']['gaps']=['完整年度行情证据不足']
    html=cycle_visual_html(result,'睿创微纳','688002.SS')
    assert '完整年度行情证据不足' in html
    assert 'class="vf-base-path"' not in html
