import os
from copy import deepcopy
import re

import numpy as np
import pandas as pd
import pytest

import stock_horizon


def _frame(direction=1, rows=120):
    idx = pd.date_range("2026-01-01", periods=rows, freq="B")
    close = 100 + direction * np.linspace(0, 30, rows)
    return pd.DataFrame({
        "Open": close - .2,
        "High": close + 1,
        "Low": close - 1,
        "Close": close,
        "Volume": np.linspace(1_000_000, 1_200_000, rows),
    }, index=idx)


def test_all_five_horizons_present():
    facts = stock_horizon.build_horizon_facts(_frame(1), {"stage": "主升阶段"})
    assert list(facts["horizons"]) == ["2周", "4周", "8周", "16周", "32周"]


def test_direction_responds_to_trend():
    up = stock_horizon.build_horizon_facts(_frame(1), {"stage": "主升阶段"})
    down = stock_horizon.build_horizon_facts(_frame(-1), {"stage": "破位转弱"})
    assert up["horizons"]["8周"]["rule_score"] > 50
    assert down["horizons"]["8周"]["rule_score"] < 50


def test_without_key_keeps_deterministic_fallback(monkeypatch):
    monkeypatch.setenv("V88_DISABLE_LLM", "1")
    result = stock_horizon.analyze("测试", "TEST", _frame(1))
    assert result["review"]["status"] == "no_key"
    assert len(stock_horizon.table_rows(result)) == 5


def test_cycle_visual_contains_full_first_screen_content(monkeypatch):
    monkeypatch.setenv("V88_DISABLE_LLM", "1")
    result = stock_horizon.analyze("测试公司", "TEST", _frame(1))
    html = stock_horizon.historical_visual_html(result, "测试公司", "TEST")
    for text in ("五周期历史证据", "过去2周档", "过去4周档", "过去32周档", "过去8周档", "过去16周档",
                 "确认条件", "反证", "行情截至", "规则方向分", "不足整档"):
        assert text in html
    for text in ('预计拐点', '今天→', '综合动作', '上行估计', '<polyline', '87%', '置信度'):
        assert text not in html


def test_cross_cycle_conflict_forces_safe_action():
    facts = {"horizons": {
        "2周": {"rule_score": 70},
        "4周": {"rule_score": 44},
        "32周": {"rule_score": 38},
        "8周": {"rule_score": 34},
        "16周": {"rule_score": 28},
    }}
    card = {"p_up": 65, "p_down": 35, "upside_pct": 20,
            "downside_pct": 8, "rr": 2.5, "expected_pct": 10,
            "action": "试仓复核", "reason": "短线启动"}
    aligned = stock_horizon.align_decision_card(card, facts)
    assert aligned["p_up"] == 70
    assert aligned["long_p_up"] == 35
    assert aligned["cycle_conflict"] is True
    assert aligned["action"] == "仅观察·不追涨"
    assert "2周偏涨" in aligned["cycle_note"]


def test_short_bullish_long_slightly_weak_is_still_conflict():
    facts = {"horizons": {
        "2周": {"rule_score": 76},
        "4周": {"rule_score": 53},
        "32周": {"rule_score": 44},
        "8周": {"rule_score": 40},
        "16周": {"rule_score": 28},
    }}
    aligned = stock_horizon.align_decision_card(
        {"upside_pct": 25, "downside_pct": 8, "rr": 3.1,
         "expected_pct": 15, "action": "试仓复核"}, facts)
    assert aligned["p_up"] == 76
    assert aligned["long_p_up"] == 39
    assert aligned["cycle_conflict"] is True
    assert aligned["action"] == "仅观察·不追涨"


def test_visual_fallback_labels_rule_not_ai():
    facts = {"asof": "2026-07-15 22:28", "stage": "启动确认", "horizons": {}}
    for weeks, score in ((2, 76), (4, 53), (32, 44), (8, 40), (16, 28)):
        facts["horizons"][f"{weeks}周"] = {
            "rule_score": score, "rule_view": "偏涨" if score >= 58 else "偏跌",
            "rule_confidence": 60, "return_pct": 0, "volume_ratio": 1,
            "support": 1, "resistance": 2,
        }
    html = stock_horizon.historical_visual_html(
        {"facts": facts, "review": {"status": "failed", "horizons": {}}},
        "紫金矿业", "601899.SS")
    assert "历史偏强" in html
    assert "AI偏涨" not in html
    assert "76/100" in html and "76%" not in html


def test_bullish_cycles_bad_price_waits_instead_of_avoid():
    facts = {"horizons": {
        "2周": {"rule_score": 62}, "4周": {"rule_score": 72},
        "32周": {"rule_score": 73}, "8周": {"rule_score": 74},
        "16周": {"rule_score": 73},
    }}
    aligned = stock_horizon.align_decision_card(
        {"upside_pct": .9, "downside_pct": 10, "rr": .09,
         "expected_pct": -2.4, "action": "回避"}, facts)
    assert aligned["cycle_status"] == "多周期偏涨"
    assert aligned["action"] == "趋势偏多·等待回踩"
    assert aligned["break_even_p"] == 91.7
    assert "当前赔率不足" in aligned["entry_note"]


def test_bullish_cycles_allow_controlled_aggressive_entry():
    facts = {"horizons": {
        "2周": {"rule_score": 72}, "4周": {"rule_score": 58},
        "32周": {"rule_score": 59}, "8周": {"rule_score": 58},
        "16周": {"rule_score": 58},
    }}
    aligned = stock_horizon.align_decision_card(
        {"upside_pct": 9, "downside_pct": 10.8, "rr": .83,
         "expected_pct": 3, "action": "观察"}, facts)
    assert aligned["action"] == "共振·小仓试错"
    assert aligned["probability_edge"] >= 8


def test_holding_risk_action_cannot_be_overridden_by_bullish_cycle():
    facts = {"horizons": {
        "2周": {"rule_score": 75}, "4周": {"rule_score": 70},
        "32周": {"rule_score": 68}, "8周": {"rule_score": 66},
        "16周": {"rule_score": 64},
    }}
    aligned = stock_horizon.align_decision_card(
        {"upside_pct": 20, "downside_pct": 8, "rr": 2.5,
         "expected_pct": 12, "action": "评估减仓"}, facts)
    assert aligned["action"] == "评估减仓"
    assert aligned["entry_note"] == "持仓先执行风险复核"


def test_lookback_metadata_matches_canonical_past_windows_without_altering_scores():
    from v88_decision_core import build_horizon_facts as canonical
    frame = _frame(rows=180); before = frame.copy(deep=True)
    expected = canonical(frame)
    observed = stock_horizon.build_horizon_facts(frame)
    assert observed['data_signature'] == expected['data_signature']
    for weeks in (2, 4, 8, 16, 32):
        label = f'{weeks}周'; fact = observed['horizons'][label]
        assert fact['lookback_start'] == str(frame.index[-(weeks*5+1)])[:10]
        assert fact['lookback_end'] == str(frame.index[-1])[:10]
        assert fact['requested_days'] == weeks*5 and fact['window_complete']
        assert fact['evidence_scope'] == 'historical-lookback'
        for key, value in expected['horizons'][label].items():
            assert fact[key] == value
    pd.testing.assert_frame_equal(frame, before)


def test_short_history_discloses_actual_range_and_missing_window_without_padding():
    frame = _frame(rows=20)
    result = stock_horizon.analyze('Test', 'TEST', frame, allow_ai=False)
    rows = stock_horizon.table_rows(result)
    longest = rows[-1]
    assert '不足整档' in longest['样本'] and '19/160' in longest['样本']
    assert str(frame.index[0])[:10] in longest['实际观察范围']
    assert result['facts']['horizons']['32周']['window_complete'] is False
    html = stock_horizon.historical_visual_html(result, 'Test', 'TEST')
    assert '19/160交易日间隔' in html and '不足整档' in html


def test_score_87_remains_a_score_and_review_confidence_cannot_create_prediction():
    facts = stock_horizon.build_horizon_facts(_frame(rows=180))
    facts['horizons']['8周'].update(rule_score=87, rule_confidence=87)
    review = {'status': 'completed', 'cycle_phase': '领涨', 'action': '可跟进',
              'summary': '预计8周后拐点', 'horizons': {'8周': {'view': '偏涨', 'confidence': 99, 'reason': '买入'}}}
    result = {'facts': facts, 'review': review, 'decision': {'action': '共振·小仓试错'}}
    html = stock_horizon.historical_visual_html(result, 'Test', 'TEST')
    assert '87/100' in html and '87%' not in html and '99%' not in html
    assert html.count('class="hz-window-score"') == 5
    assert '<polyline' not in html and '预计8周后拐点' not in html
    assert '可跟进' not in html and '小仓试错' not in html and '买入' not in html
    assert '旧周期复核未声明历史窗口口径' in html
    assert stock_horizon._visual_score({'rule_score':87}, {'confidence':99, 'view':'偏跌'}) == 87


def test_period_consistency_is_rendered_verbatim_escaped_without_modifying_contract():
    result = stock_horizon.analyze('Test', 'TEST', _frame(rows=180), allow_ai=False)
    result['period_consistency'] = {'headline': '过去偏强但近期须防回撤', 'summary': '同一快照两类证据并存',
        'status': 'caution', 'conditions': ['顶部预警解除后复核', '<script>alert(1)</script>']}
    result['decision'] = {'grade': '1A', 'stop': 100, 'target': 150, 'deadline': '2026-12-01'}
    before = deepcopy(result)
    html = stock_horizon.historical_visual_html(result, '<img src=x onerror=alert(1)>', 'TEST', 'bad"id')
    assert '过去偏强但近期须防回撤' in html and '同一快照两类证据并存' in html
    assert '状态：风险优先' in html and '顶部预警解除后复核' in html
    assert '&lt;script&gt;' in html and '<script>' not in html and '<img src=' not in html
    assert 'overflow-x:auto' in html and 'min-width:850px' in html
    assert result == before
    only_consistency = stock_horizon.historical_visual_html({'period_consistency':result['period_consistency']}, 'Test', 'TEST')
    assert '过去偏强但近期须防回撤' in only_consistency


def test_alignment_names_direction_scores_and_includes_all_four_longer_windows():
    facts = {'horizons': {f'{weeks}周': {'rule_score': score}
                         for weeks, score in ((2, 70), (4, 20), (8, 40), (16, 60), (32, 80))}}
    aligned = stock_horizon.cycle_alignment(facts)
    assert aligned['long_score'] == 60 and aligned['long_p_up'] == 60
    assert aligned['short_score'] == aligned['p_up'] == 70
    assert '/100' in aligned['note'] and '%' not in aligned['note']
    assert aligned['score_semantics'] == 'historical-direction-score-not-probability'
    assert not stock_horizon._turning_candidate([{'score': 80}, {'score':30}, {'score':70}])['horizon']


def test_alignment_missing_direction_score_does_not_become_neutral_or_short_score():
    facts = {'horizons': {'2周': {'rule_score': 80}, '4周': {'rule_score': 80},
                          '8周': {'rule_score': 80}, '16周': {'rule_score': 80}}}
    result = stock_horizon.cycle_alignment(facts)
    assert result['short_score'] == 80 and result['long_score'] is None
    assert result['score_status'] == 'limited' and result['conflict'] is None
    assert result['score_aggregation']['alignment']['coverage'] == pytest.approx(.6)


def test_deterministic_reasons_do_not_create_unreviewed_followup_or_trading_actions():
    result = stock_horizon.forward_reasons('Test', 'TEST', {'stage':'偏强', 'weighted_p_up':87,
        'overall_action':'可跟进', 'horizons':[{'label':'8周','view':'偏涨'}]}, allow_ai=False)
    assert '87%' not in result['overall'] and '可跟进' not in result['overall']
    assert not any(word in result['reasons']['8周'] for word in ('买入', '持有', '分批跟进', '减仓'))


def test_blocked_period_binding_suppresses_all_unverified_chart_and_table_facts():
    result = stock_horizon.analyze('Test', 'TEST', _frame(rows=180), allow_ai=False)
    result['facts']['horizons']['8周'].update(rule_score=99, lookback_start='2099-01-01')
    result['period_consistency'] = {'status':'blocked', 'headline':'跨周期未核对，暂不合并方向',
                                  'summary':'五周期事实与完整行情重算不一致', 'conditions':[]}
    before = deepcopy(result)
    html = stock_horizon.historical_visual_html(result, 'Test', 'TEST')
    assert '五周期事实与完整行情重算不一致' in html and '跨周期事实待复核' in html
    assert '<svg' not in html and '<table' not in html
    assert '99/100' not in html and '2099-01-01' not in html
    assert stock_horizon.table_rows(result) == []
    assert result == before


def test_independent_history_without_period_binding_is_explicitly_unlinked():
    result = stock_horizon.analyze('Test', 'TEST', _frame(rows=180), allow_ai=False)
    html = stock_horizon.historical_visual_html(result, 'Test', 'TEST')
    assert '未关联年度条件研判' in html and '<svg' in html
    rows = stock_horizon.table_rows(result)
    assert len(rows) == 5 and all(row['跨周期关联'] == '未关联年度条件研判' for row in rows)


def test_alignment_direction_boundaries_match_canonical_rule_view():
    for score, expected in ((41, '偏跌'), (42, '震荡'), (58, '震荡'), (59, '偏涨')):
        facts = {'horizons':{f'{w}周':{'rule_score':score, 'rule_view':expected} for w in (2,4,8,16,32)}}
        alignment = stock_horizon.cycle_alignment(facts)
        assert alignment['short_side'] == alignment['long_side'] == expected
        assert alignment['short_score'] == alignment['long_score'] == score


def test_period_display_translates_status_and_does_not_label_invalidation_as_confirmation():
    labels = {'caution':'风险优先', 'divergent':'周期分歧', 'linked':'已关联', 'limited':'证据不足', 'blocked':'待复核'}
    for status, label in labels.items():
        result = {'period_consistency':{'status':status, 'input_id':'abc123',
                                      'conditions':['量价修复后确认', '跌破前低则失效']}}
        html = stock_horizon._period_consistency_html(result)
        assert f'状态：{label}' in html and f'状态：{status}' not in html
        assert 'data-input-id="abc123"' in html
        assert '条件：跌破前低则失效' in html
        assert '确认条件：跌破前低则失效' not in html
