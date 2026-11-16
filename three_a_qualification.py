"""Owner's current 3A definition: upward medium/long trend and near entry.

The owner clarified that 90% is an improvement objective, NOT a grade gate.
Qualification follows the horizon/trend/entry-window contract. Historical group
percentages, Classics agreement and passing code tests remain distinct from a
calibrated per-stock probability; none is manufactured here.
"""
from copy import deepcopy
import math

VERSION = 'three-a-qualification-2026-11-16-v2-entry-window'
TARGET_NET_WIN_RATE = .90
WIN_DEFINITION = '按原退出规则结算，扣除往返费用后收益大于0；未入场和未结算不计胜负'
PUBLIC_LABEL = '未满足3A趋势与近期买点'


def number(value):
    return type(value) in (int, float) and math.isfinite(value) and value > 0


def assess(row, directive, *, pre_requirement_buyable=False, window=None):
    """Describe current evidence without inventing a per-stock probability."""
    rule = row.get('ladder') or {}
    price, stop, target = (directive.get(k) for k in ('price', 'stop', 'target'))
    valid_geometry = all(number(v) for v in (price, stop, target)) and stop < price < target
    fee = .5  # Same round-trip percent as the frozen legacy rule replay.
    reward = (target * .9975 / (price * 1.0025) - 1) * 100 if valid_geometry else None
    risk = (1 - stop * .9975 / (price * 1.0025)) * 100 if valid_geometry else None
    target_assessment = {
        'status': 'scenario_only' if valid_geometry else 'plan_pending',
        'price': price, 'stop': stop, 'target': target,
        'hold_sessions': row.get('max_sessions') or rule.get('hold_sessions'),
        'round_trip_fee_pct': fee,
        'cost_formula': '(exit * (1 - 0.0025) / (entry * (1 + 0.0025)) - 1) * 100',
        'net_target_return_pct': round(reward, 4) if reward is not None else None,
        'net_stop_loss_pct': round(-risk, 4) if risk is not None else None,
        'net_reward_risk': round(reward / risk, 4) if risk else None,
        'target_hit_probability': None,
        'note': '原目标是退出情景；几何盈亏比、ATR距离和经典共振均不证明目标达成概率',
    }
    window = window or {}
    qualified = window.get('eligible') is True
    return {
        'version': VERSION, 'qualified': qualified, 'status': 'qualified_by_trend_and_entry' if qualified else 'entry_window_unmet',
        'public_label': '3A' if qualified else PUBLIC_LABEL, 'target_net_win_rate': TARGET_NET_WIN_RATE,
        'probability_requirement_is_grade_gate': False,
        'qualification_definition': '中长期走势向上，买点在今明两个交易日；90%为提升目标',
        'win_definition': WIN_DEFINITION, 'estimated_net_win_probability': None,
        'pre_requirement_buyable': bool(pre_requirement_buyable),
        'reasons': list(window.get('reasons') or []),
        'probability_note': '现有回放不支持90%；分类与当下执行条件不等于个股获胜概率',
        'historical_evidence': {
            **{k: deepcopy(rule.get(k)) for k in ('win', 'win_halves', 'hold_sessions', 'source', 'evidence')},
            'scope': '选优所用历史规则组统计；不等于该股本次获胜概率或独立样本外认证',
        },
        'target_assessment': target_assessment,
        'learning_cohort': '3A-candidate-v1',
    }


def research_directive(directive, qualification):
    """Keep immutable financial fields while removing unsupported buy language."""
    d = deepcopy(directive)
    source = deepcopy(d.get('signal_plan') or d)
    source.pop('execution_readiness', None)
    source.pop('signal_plan', None)
    d['signal_plan'] = source
    d['action'] = '研究候选'
    d['canon_id'] = 'CANON-3A-ENTRY-WINDOW-01'
    d['canon'] = '中长期走势向上且今明买点才列3A；90%是优化目标，不伪造获胜概率'
    d['why'] = list(qualification['reasons'])
    state = (d.get('execution_readiness') or {}).get('label') or '执行条件待核'
    parts = ['后台研究计划（未满足近期推荐条件）', state]
    for key, label in (('price', '研究限价 ≤ '), ('stop', '原止损 '), ('target', '原目标 ')):
        if number(d.get(key)):
            parts.append(label + str(d[key]))
    if d.get('valid_until'):
        parts.append('原期限 ' + str(d['valid_until']) + '（不自动顺延）')
    d['text'] = ' · '.join(parts)
    return d
