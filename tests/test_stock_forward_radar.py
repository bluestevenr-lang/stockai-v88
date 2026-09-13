import numpy as np
import pandas as pd
import pytest
from copy import deepcopy

from stock_forward_radar import scan_forward_opportunities, opportunity_score
from v88_decision_core import evaluate_forward_outlook


def _df(direction, n=180):
    if direction > 0:                      # 稳步上行（起步/多头）
        base = np.linspace(100, 150, n)
    elif direction < 0:                    # 下行
        base = np.linspace(150, 100, n)
    else:                                  # 横盘
        base = np.full(n, 120.0)
    close = base + np.sin(np.arange(n) / 4) * 1.2
    idx = pd.date_range("2026-01-01", periods=n, freq="B")
    return pd.DataFrame({"Open": close * .998, "High": close * 1.01,
                         "Low": close * .99, "Close": close,
                         "Volume": np.linspace(1e6, 1.3e6, n)}, index=idx)


def test_radar_ranks_uptrends_above_downtrends_and_tags_sector():
    data = {"UP": _df(1), "DOWN": _df(-1), "FLAT": _df(0)}
    pool = [("UP", "涨股"), ("DOWN", "跌股"), ("FLAT", "横股")]
    sectors = {"UP": "医疗", "DOWN": "地产", "FLAT": "公用"}

    out = scan_forward_opportunities(
        fetch_fn=lambda c: data.get(c),
        get_sector_fn=lambda c, n: sectors[c],
        pool=pool)

    assert out["scanned"] == 3
    # 上行股机会分应最高，排第一
    assert out["stocks"][0]["code"] == "UP"
    assert out["stocks"][0]["opp_score"] > out["stocks"][-1]["opp_score"]
    # 医疗（上行）板块应被判为正在起步/热
    med = next(s for s in out["sectors"] if s["sector"] == "医疗")
    assert med["starting_count"] >= 1 or med["hot"]
    # 概率口径诚实标注
    assert out["probability_kind"] == "未校准规则方向分（非概率或胜率）"


def test_radar_skips_unfetchable_and_short_history():
    data = {"OK": _df(1), "SHORT": _df(1, 10)}
    pool = [("OK", "好"), ("SHORT", "太短"), ("MISS", "拉不到")]
    out = scan_forward_opportunities(
        fetch_fn=lambda c: data.get(c),      # MISS -> None
        get_sector_fn=lambda c, n: "科技",
        pool=pool)
    assert out["scanned"] == 1 and out["skipped"] == 2


def test_opportunity_score_flags_starting_for_bullish():
    fwd = evaluate_forward_outlook(_df(1), name="涨", code="UP")
    opp = opportunity_score(fwd)
    assert opp["long_p"] >= opp["short_p"]        # 长端比短端更偏多 = 趋势在形成
    assert "opp_score" in opp and "starting" in opp


def test_explicit_empty_pool_is_zero_coverage_not_hardcoded_fallback():
    out=scan_forward_opportunities(lambda _:None,lambda c,n:'other',pool=[])
    assert out['pool_size']==0 and not out['coverage_complete']


def test_hk_zero_padding_is_one_security():
    out=scan_forward_opportunities(lambda _: _df(1),lambda c,n:'科技',pool=[('00700.HK','腾讯'),('700.HK','腾讯')])
    assert out['pool_size']==1 and out['scanned']==1


@pytest.mark.parametrize('field', ['weighted_p_up','weighted_rr','weighted_expected_pct'])
@pytest.mark.parametrize('bad', [None,False,float('nan'),float('inf')])
def test_missing_fact_never_defaults_to_neutral_or_generates_rank(field,bad):
    fwd=evaluate_forward_outlook(_df(1),name='涨',code='UP');fwd[field]=bad
    before=deepcopy(fwd);result=opportunity_score(fwd)
    assert result['opp_score'] is None and not result['starting'] and result['gaps']
    assert result['no_grade_authority'] and not result['entry_permission']
    assert set(fwd)==set(before)


def test_incomplete_long_window_is_unranked_and_not_claimed_as_full_pool_coverage():
    result=scan_forward_opportunities(lambda _: _df(1,40),lambda c,n:'科技',pool=[('UP','涨')])
    assert not result['stocks'] and not result['starting_stocks'] and not result['sectors']
    assert not result['coverage_complete'] and len(result['unranked_stocks'])==1
    assert any('完整输入窗口' in g for g in result['unranked_stocks'][0]['gaps'])


def test_action_suggestions_are_not_propagated_from_auxiliary_engine():
    result=scan_forward_opportunities(lambda _: _df(1),lambda c,n:'科技',pool=[('UP','涨')])
    row=result['stocks'][0]
    assert '分批加' not in row['suggestion'] and '持有' not in row['suggestion']
    assert row['direction_score']==row['p_up'] and row['data_signature']
    assert '非全市场' in result['coverage_scope']
