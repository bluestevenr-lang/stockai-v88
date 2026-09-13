from copy import deepcopy
import math
import pytest
from evidence_visuals import deep_overview, domain_strip, coverage_card, contract_strip


def test_cross_pack_or_missing_evidence_never_shows_support():
    annual={'input_id':'a','regime':'up','evidence':{'company_support':True},'top_warning':True}
    synthesis={'input_id':'s'}
    for period in ({},{'annual_input_id':'wrong','synthesis_input_id':'s','status':'linked'}, {'annual_input_id':'a','synthesis_input_id':'s','status':'blocked'}):
        h=deep_overview(synthesis,annual,period)
        assert '待补年度证据' in h and '待关联复核' in h and '查中央原合同' in h
        assert 'data-state="up"' not in h


def test_uptrend_and_active_warning_remain_visible_together():
    annual={'input_id':'a','regime':'up_caution','top_warning':True,'evidence':{}}
    h=deep_overview({'input_id':'s'},annual,{'annual_input_id':'a','synthesis_input_id':'s','status':'divergent'})
    assert '上升结构' in h and '转弱预警待解' in h and '待补企业证据' in h


def test_domain_missing_and_opposition_not_green():
    doc={'domains':[{'title':'<script>x</script>','status':'observed','reviews':[{'conclusion':'支持'},{'conclusion':'缺失'}]}, {'title':'财报','status':'conflict','reviews':[{'conclusion':'支持'},{'conclusion':'反对'}]}]}
    h=domain_strip(doc)
    assert '待证' in h and '反证' in h and '同包支持' not in h
    assert '<script>' not in h


@pytest.mark.parametrize('n,total',[(None,100),(100,0),(True,100),(math.nan,100),(101,100),(-1,100)])
def test_invalid_coverage_never_claims_percent(n,total):
    h=coverage_card('港股',n,total,True)
    assert '数量或时点待核' in h and '覆盖-fill' not in h and '· 达标' not in h


def test_coverage_rounding_cannot_pass_the_target():
    h=coverage_card('港股',899999,1000000,True)
    assert '90.00%' in h and '待达标' in h and '· 达标' not in h
    assert '· 达标' in coverage_card('A股',90,100,True)
    assert '待达标' in coverage_card('A股',99,100,False)


def test_existing_price_axis_preserves_contract_and_no_future_axis():
    plan={'stop':90,'entry_range':[100,105],'take_profit_range':[120,125]}; before=deepcopy(plan)
    h=contract_strip(plan,110,verified=True)
    assert '<svg' in h and '不是未来走势' in h and '原止盈' in h and plan==before
    for px in (None,False,math.nan):
        assert '<svg' not in contract_strip(plan,px,verified=True)
    assert '<svg' not in contract_strip(plan,110)
    assert '<svg' not in contract_strip({**plan,'stop':106},110,verified=True)


def test_catalog_pending_separates_directory_from_daily_ratio():
    h=coverage_card('港股',95,100,True,pending_reason='日线比例≥90% · 目录待核')
    assert '95.00%' in h and '目录待核' in h and '待达标' not in h
    assert '#fffbeb' in h
