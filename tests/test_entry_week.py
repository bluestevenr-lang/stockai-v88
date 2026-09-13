"""Behavioral checks for the five-session gate, with no live market requests."""
from copy import deepcopy
from datetime import datetime,timezone
import math

import pytest
import entry_week as week

NOW=datetime(2026,9,13,6,tzinfo=timezone.utc)


def row(code='TEST',triggered=False):
    from profit_fixtures import short_inputs
    return {'code':code,'market':'美股','tier':'1A','audit_id':'audit-1','data_fresh':True,
            'source_timestamps':{'daily':'2026-09-11T16:00:00-04:00'},
            'trade_plan':{'last':100.,'horizon':'short','stop':95.,'entry_range':[98.,101.],
                          'take_profit_range':[120.,121.],
                          'profit_inputs':{**short_inputs(120,121),'asof':'2026-09-11T16:00:00-04:00','peaks':[{'date':'2026-09-10','high':120.},{'date':'2026-09-11','high':121.}]},
                          'promotion_trigger':'收盘重新站上98.0且当日量/此前20日均量≥1.2；下一可交易开盘仍在原区间'},
            'execution':{'triggered':triggered},
            'scorecard':{'gpt':{'complete':True,'current':True,'evidence':[{'field':'last','value':100.},{'field':'market_evidence.ma20','value':98.}]},
                         'books':{'current':True,'complete':True,'checks':[{'id':'trend','ok':True}]}}}


def doc(r,up=2.,down=2.):
    return {'records':{r['code']:{
        'version':week.VERSION,'status':'VERIFIED','binding':week.binding(r),'generated_at':NOW.isoformat(),
        'source_asof':'2026-09-11','last':r['trade_plan']['last'],'sample_n':24,'turnover20_local':100_000_000.,
        'budgets':{str(i):{'up_pct':up,'down_pct':down} for i in range(1,6)}}}}


def test_far_entry_inside_old_ten_percent_rule_is_excluded():
    r=row();original=deepcopy(r)
    result=week.assess(r,[94.,95.],now=NOW,doc=doc(r,down=1))
    assert not result['eligible'] and '到价预算' in result['reason']
    assert result['required_move_pct']==5
    assert r==original


def test_range_intersects_original_trend_and_week_budget_without_changing_contract():
    r=row();result=week.assess(r,[98.,101.],now=NOW,doc=doc(r,up=.5,down=1))
    assert result['eligible'] and result['entry_range']==pytest.approx([99.,100.5])
    assert result['sessions']==['2026-09-14','2026-09-15','2026-09-16','2026-09-17','2026-09-18']
    assert r['trade_plan']['entry_range']==[98.,101.]
    assert result['arrival_budget']==4 and result['confirmation_sessions']==1
    assert result['probability'] is None


def test_two_stage_path_reserves_days_for_confirmation_and_fill():
    r=row();r['trade_plan']['promotion_trigger']='两阶段等待：先回落，再企稳，随后收盘突破前一日最高价'
    d=doc(r);d['records']['TEST']['budgets']['2']['down_pct']=.5
    out=week.assess(r,[98,99],now=NOW,doc=d)
    assert not out['eligible'] and out['arrival_budget']==2 and out['confirmation_sessions']==3
    assert '下一开盘' in out['steps']


@pytest.mark.parametrize('change,reason',[
    (lambda d:d['records'].clear(),'缺少同股'),
    (lambda d:d['records']['TEST'].update(binding='different'),'同原合同'),
    (lambda d:d['records']['TEST'].update(source_asof='2026-09-10'),'过期'),
    (lambda d:d['records']['TEST'].update(generated_at='2026-09-14T00:00:00+00:00'),'时间非法'),
    (lambda d:d['records']['TEST'].update(last=99),'现价'),
    (lambda d:d['records']['TEST'].update(sample_n=19),'不足20'),
    (lambda d:d['records']['TEST']['budgets']['4'].update(up_pct=float('nan')),'数值非法'),
])
def test_missing_stale_changed_or_invalid_evidence_never_passes(change,reason):
    r=row();d=doc(r);change(d)
    result=week.assess(r,[98,101],now=NOW,doc=d)
    assert not result['eligible'] and reason in result['reason']


def test_unknown_template_needs_evidence_instead_of_inventing_steps():
    r=row();r['trade_plan']['promotion_trigger']='长期看好，择机买入'
    assert not week.assess(r,[98,101],now=NOW,doc=doc(r))['eligible']


def test_non_overlapping_blocks_and_an_extreme_day_do_not_expand_median_to_maximum():
    closes=[100+math.sin(i)*.1 for i in range(121)]
    out=week.summarize(closes)
    assert out['sample_n']==24 and out['lookback_sessions']==120
    closes[60]=200
    spike=week.summarize(closes)
    assert spike['budgets']['5']['up_pct']<1
    with pytest.raises(ValueError):week.summarize(closes[:100])
    with pytest.raises(ValueError):week.summarize(closes+[float('nan')])


def test_short_medium_and_long_share_entry_window_without_shortening_holding_contract():
    from profit_fixtures import annual_inputs
    for horizon in ('short','medium','long'):
        r=row();r['trade_plan']['horizon']=horizon
        if horizon!='short':
            r['trade_plan']['profit_inputs']={**annual_inputs(120,121),'horizon':horizon,'asof':'2026-09-11T16:00:00-04:00','max_calendar_days':365 if horizon=='long' else 90}
        result=week.assess(r,[98,101],now=NOW,doc=doc(r))
        assert len(result['sessions'])<=5
        assert r['trade_plan']['horizon']==horizon


def test_entry_gate_and_all_shared_renderers_use_the_bounded_range():
    from entry_opportunity import assess,html,price_html
    r=row();out=assess(r,now=NOW,week_doc=doc(r,up=.5,down=1))
    assert out['focus_eligible'] and not out['executable']
    assert out['week_entry_range']==pytest.approx([99.,100.5])
    assert '99 ～ 100.5' in html(out) and '99 ～ 100.5' in price_html(out,r['trade_plan'])
    assert '原研究区间与触发' in price_html(out,r['trade_plan'])


def test_desktop_and_core_rule_copies_match():
    from pathlib import Path
    root=Path(__file__).resolve().parents[1]
    core=root.parent/'ai-daily-report-v2/src'
    if core.exists():
        for name in ('entry_week.py','entry_opportunity.py','grade_focus.py'):
            assert (root/name).read_bytes()==(core/name).read_bytes()


def test_shared_liquidity_blocks_week_and_main_recommendation_together():
    from entry_opportunity import assess
    r=row();d=doc(r);d['records']['TEST']['turnover20_local']=4_999_999
    result=assess(r,now=NOW,week_doc=d)
    assert not result['focus_eligible'] and '流动性' in result['reasons'][0]
