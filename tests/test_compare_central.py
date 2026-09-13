from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from compare_ui import comparison_records, verdict_html, correlation, family_html, trend_svg
from test_grade_card_safety import central_row, central_v2


def context(code, tier='1A'):
    selection = central_v2()
    row = central_row(code, '测试'+code, tier, tier+'_PREPARE')
    return {'code':code,'selection':selection,'row':row,
            'fact':{'code':code,'horizon':row['horizon'],'trade_plan':deepcopy(row['trade_plan'])},
            'loaded_factpack_id':selection['factpack_id'], 'formal':False}


@pytest.fixture
def contexts(monkeypatch):
    # Test fixtures already hold normalized complete contracts; real live smoke
    # separately uses recommendation_gate._trade_plan on original frozen facts.
    from market_data_helper import _core
    _core()
    monkeypatch.setattr('recommendation_gate._trade_plan',lambda fact:fact['trade_plan'])
    return {'ALOW':context('ALOW'),'ZHIGH':context('ZHIGH','2A')}


def test_technical_winner_cannot_outrank_central_quality(contexts):
    rows=[{'code':'ALOW','score':99,'action':'买入','stop':777},
          {'code':'ZHIGH','score':1,'action':'回避','stop':888}]
    before=deepcopy(contexts)
    output=comparison_records(rows,contexts=contexts)
    assert [r['code'] for r in output]==['ZHIGH','ALOW']
    assert [r['tier'] for r in output]==['2A','1A']
    html=verdict_html(rows,contexts=contexts)
    assert '原失效 9.7' in html
    # Hash references can contain digit runs such as 777; reject them only as prices.
    assert '原失效 777' not in html and '原失效 888' not in html
    assert all(r['trade_plan']['stop'] == 9.7 for r in output)
    assert '全系统同一把尺' not in html and '🥇' not in html
    assert contexts==before


def test_no_context_or_expired_review_never_grants_grade(contexts):
    raw=[{'code':'ALOW','score':100,'tier':'3A','action':'买入'}]
    r=comparison_records(raw)[0]
    assert not r['current'] and r['tier'] is None and r['audit_score'] is None
    ctx=contexts['ALOW']
    ctx['row']['reviews']['gpt']['at']=(datetime.now(timezone.utc)-timedelta(days=10)).isoformat()
    r=comparison_records(raw,contexts=contexts)[0]
    assert not r['current'] and any('过期' in e for e in r['errors'])


@pytest.mark.parametrize('mutate',[
    lambda c:c.update(loaded_factpack_id='different'),
    lambda c:c['fact']['trade_plan'].update(stop=8.8),
    lambda c:c['row'].update(audit_score=99),
    lambda c:c['fact'].update(code='OTHER'),
])
def test_mismatch_is_visible_and_cannot_become_a_current_grade(contexts,mutate):
    mutate(contexts['ALOW'])
    r=comparison_records([{'code':'ALOW','score':99}],contexts=contexts)[0]
    assert not r['current'] and r['errors'] and r['tier'] is None


def test_mixed_publication_invalidates_entire_comparison(contexts):
    contexts['ZHIGH']['selection']['generated_at']='2026-09-01 10:00'
    rows=[{'code':c,'score':99} for c in contexts]
    assert all(not r['current'] for r in comparison_records(rows,contexts=contexts))


def histories():
    start=datetime(2026,7,1)
    a={(start+timedelta(days=i)).date().isoformat():100+i+(i%3) for i in range(40)}
    b={d:v*2 for i,(d,v) in enumerate(a.items()) if i%6!=0}
    # Older unrelated rows must not move a common observation window.
    b['2025-01-01']=900
    return a,b


def test_correlation_aligns_dates_and_does_not_pair_different_holidays():
    a,b=histories()
    assert correlation(a,b)==1.0
    assert correlation(list(a.values()),list(b.values())) is None
    body=family_html({'A':a,'B':b})
    assert '不同市场收盘时刻不同时' in body and '自动要求' in body


def test_trends_share_a_calendar_start_and_reject_undated_cache():
    a,b=histories()
    html=trend_svg({'A':a,'B':b})
    assert '共同起点 2026-07-02' in html and '2025-01-01' not in html
    assert '<svg' not in trend_svg({'A':list(a.values()),'B':list(b.values())})


def test_html_stock_names_and_contract_notes_are_escaped(contexts):
    contexts['ALOW']['row']['name']='<script>alert(1)</script>'
    html=verdict_html([{'code':'ALOW','score':80}],contexts=contexts)
    assert '<script>' not in html and '&lt;script&gt;' in html


def test_old_r1_rank_is_not_mislabelled_as_a_technical_score(contexts):
    rows=[{'code':'ALOW','score':99,'score_kind':'排名分R1'}]
    assert comparison_records(rows,contexts=contexts)[0]['technical_score'] is None
    rows[0]['technical_score']=61
    assert comparison_records(rows,contexts=contexts)[0]['technical_score']==61


def test_boolean_is_not_a_verified_one_unit_price():
    a,b=histories();a[next(iter(a))]=True
    assert correlation(a,b) is None
    assert '<svg' not in trend_svg({'A':a,'B':b})
