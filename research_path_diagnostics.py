"""Read-only shadow comparison with the 2026-09-13 mobile Stock handoff.

The handoff's numerical screens are custom research hypotheses, not quotations
from classics or calibrated trading rules. No grades, ranking weights, contracts,
review signatures, model calls, orders, or automatic queue admissions are created.
The caller supplies its existing complete-session reconciliation, never live ticks.
"""
from copy import deepcopy
import hashlib
import json
import math

VERSION = 'mobile-stock-path-shadow-v1'
DATA_CHECKS = ('identity', 'binding', 'session', 'basis', 'facts', 'quant')


def combine(values, operator='and'):
    """Three-valued logic: a missing fact is not False or a zero observation."""
    values = list(values)
    if operator not in ('and', 'or'):
        raise ValueError('unknown operator')
    if not values:
        return None
    if operator == 'and':
        return False if False in values else None if None in values else True
    return True if True in values else None if None in values else False


def _number(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def _condition(key, title, value, detail):
    return {'id': key, 'title': title, 'value': value,
            'state': 'unknown' if value is None else 'pass' if value else 'fail',
            'detail': detail}


def _fmt(value):
    return f'{value:.4f}'.rstrip('0').rstrip('.') if value is not None else '待核'


def build(context, frame, quality, cross):
    """Evaluate independent price paths without filtering on central grade."""
    context, quality, cross = context or {}, quality or {}, cross or {}
    checks = {c['id']: c for c in cross.get('checks', [])}
    ready = bool(frame is not None and not frame.empty
                 and all(checks.get(k, {}).get('status') == 'pass' for k in DATA_CHECKS))
    if ready:
        payload = frame[['Open', 'High', 'Low', 'Close', 'Volume']].to_csv()
        payload += json.dumps({k: frame.attrs.get(k) for k in ('source', 'source_asof', 'price_basis')}, sort_keys=True)
        actual_sig = hashlib.sha256(payload.encode()).hexdigest()
        ready = bool(actual_sig == quality.get('snapshot_signature') == cross.get('snapshot_signature')
                     and context.get('code') == quality.get('code') == cross.get('code')
                     and str(frame.index[-1])[:10] == str(quality.get('source_asof'))[:10]
                     == str(cross.get('source_asof'))[:10])
    m = {}
    if ready:
        # Reconciliation covers signed identity, basis, source session, scalar
        # agreement and the exact whole-series hash. Never fill missing windows.
        close = frame.Close
        m['last'] = _number(close.iloc[-1])
        for n in (10, 20, 50, 150, 200):
            m[f'ma{n}'] = _number(close.tail(n).mean()) if len(frame) >= n else None
        for n in (5, 20):
            previous = _number(close.iloc[-n-1]) if len(frame) > n else None
            m[f'return{n}_pct'] = (m['last'] / previous - 1)*100 if previous and m['last'] is not None else None
        m['prior_high5'] = _number(frame.High.iloc[-6:-1].max()) if len(frame) >= 6 else None
        prior_vol = _number(frame.Volume.iloc[-21:-1].mean()) if len(frame) >= 21 else None
        volume = _number(frame.Volume.iloc[-1])
        m['volume_ratio20'] = volume / prior_vol if prior_vol and volume is not None else None
        m['recent_lows'] = [_number(v) for v in frame.Low.tail(3)] if len(frame) >= 3 else []
        m['recent_low_dates'] = [str(v)[:10] for v in frame.index[-3:]] if len(frame) >= 3 else []
        # Calendar 52 weeks, not an arbitrary shorter sample or 252-row proxy.
        import pandas as pd
        start = frame.index[-1] - pd.Timedelta(weeks=52)
        high52 = _number(frame.loc[frame.index > start, 'High'].max()) if frame.index[0] <= start else None
        m['drawdown52w_pct'] = (m['last']/high52-1)*100 if high52 and m['last'] is not None else None
        m['high52w'] = high52

    def compare(left, right, op=lambda a, b: a > b):
        a, b = m.get(left), m.get(right) if isinstance(right, str) else right
        return op(a, b) if a is not None and b is not None else None

    trend_checks = [_condition(f'above_ma{n}', f'收盘高于MA{n}', compare('last', f'ma{n}'),
                              f'收盘 {_fmt(m.get("last"))}；MA{n} {_fmt(m.get(f"ma{n}"))}')
                    for n in (20, 50, 150, 200)]
    for a, b in ((50, 150), (150, 200)):
        trend_checks.append(_condition(f'ma{a}_above_ma{b}', f'MA{a}>MA{b}', compare(f'ma{a}', f'ma{b}'),
                                       f'{_fmt(m.get(f"ma{a}"))} / {_fmt(m.get(f"ma{b}"))}'))
    trend = combine(c['value'] for c in trend_checks)
    lows = m.get('recent_lows') or []
    rising = lows[0] < lows[1] < lows[2] if len(lows) == 3 and all(v is not None for v in lows) else None
    repair_clue = _condition('rising_three_lows', '三日低点逐步抬高', rising,
        ' → '.join(f'{v:.2f}' for v in lows) if rising is not None else '完整三日日线待核')
    # This literal observation does not restore the undefined mobile
    # "no new low" reference window or industry-relative-strength definition.
    stability_checks = [repair_clue,
        _condition('above_ma10', '收盘高于MA10', compare('last', 'ma10'), f'MA10 {_fmt(m.get("ma10"))}'),
        _condition('industry_relative', '5日相对行业表现改善', None, '行业基准与改善口径尚未冻结'),
        _condition('no_new_low_definition', '企稳完整定义', None, '原文“不创新低”的对照窗口待明确；低点抬高仅为局部线索')]
    drawdown_checks = [_condition(key, title, compare(key, threshold, lambda a, b: a <= b),
                                  f'观察 {_fmt(m.get(key))}%；阈值 {threshold}%')
        for key, title, threshold in (('drawdown52w_pct', '距52周高点≤−30%', -30),
                                     ('return20_pct', '20交易日收益≤−15%', -15),
                                     ('return5_pct', '5交易日收益≤−10%', -10))]
    deep = combine((c['value'] for c in drawdown_checks), 'or')
    confirmation = [
        _condition('prior_high5', '收盘突破此前5日最高价', compare('last', 'prior_high5'), f'此前5日高点 {_fmt(m.get("prior_high5"))}（不含本日）'),
        _condition('above_ma20', '收盘高于MA20', compare('last', 'ma20'), f'MA20 {_fmt(m.get("ma20"))}'),
        _condition('volume', '量比≥1.3', compare('volume_ratio20', 1.3, lambda a, b: a >= b),
                   f'本日量/此前20日均量 {_fmt(m.get("volume_ratio20"))}；分母不含本日'),
        _condition('fundamental_event_review', '基本面与事件完整复审', None,
                   '未映射为本影子策略的终审；既有八域资料不等于该策略通过')]

    def branch(key, title, state, summary, parts):
        return {'id': key, 'title': title, 'state': state, 'summary': summary,
                'checks': parts, 'eligible': None, 'decision_weight': 0}

    branches = [
        branch('trend_continuation', '↗ 趋势延续', 'unknown' if trend is None else 'pass' if trend else 'fail',
               '强趋势形态满足' if trend else '强趋势形态未满足' if trend is False else '形态数据待核', trend_checks),
        branch('trend_repair', '↻ 趋势修复补审', 'not_applicable' if trend is True else 'unknown',
               '本次强趋势形态已满足' if trend is True else '有修复线索·待综合复审' if trend is False and rising else '待核对修复证据',
               stability_checks),
        branch('deep_rebound', '↘ 深跌反弹', 'not_applicable' if deep is False else 'unknown',
               '未达深跌条件' if deep is False else '深跌命中·确认待核' if deep else '深跌数据待核',
               drawdown_checks + confirmation),
        branch('long_value', '◇ 长期质量与估值', 'unknown', '对应策略尚待综合复审', [])]

    row, card = context.get('row') or {}, context.get('card') or {}
    current = bool(checks.get('reviews', {}).get('status') == 'pass'
                   and checks.get('binding', {}).get('status') == 'pass')
    book = card.get('books') or {}
    blockers = []
    if current:
        for item in book.get('checks') or []:
            if item.get('required', True) and item.get('ok') is not True:
                blockers.append({'id': 'book:' + str(item.get('id')), 'title': item.get('label'),
                                 'state': 'fail' if item.get('ok') is False else 'unknown',
                                 'detail': item.get('detail'), 'source': '中央书理审核'})
    else:
        blockers.append({'id': 'review_pending', 'title': '中央当前双审待核', 'state': 'unknown',
                         'detail': '原评分不作为当前已审核结论', 'source': '中央交叉核验'})
    for item in cross.get('checks', []):
        if item.get('status') != 'pass':
            blockers.append({'id': 'cross:' + item['id'], 'title': item.get('title'),
                             'state': 'unknown' if item.get('status') == 'gap' else 'fail',
                             'detail': item.get('detail'), 'source': '中央交叉核验'})
    domains = (row.get('joint_evidence') or {}).get('domains') or {}
    result = {'version': VERSION, 'mode': 'shadow', 'code': context.get('code'),
        'source_asof': quality.get('source_asof'), 'source': quality.get('source'),
        'price_basis': quality.get('price_basis'), 'snapshot_signature': quality.get('snapshot_signature'),
        'factpack_id': context.get('loaded_factpack_id'), 'audit_id': row.get('audit_id'),
        'data_ready': ready, 'metrics': m, 'local_repair_clue': repair_clue,
        'branches': branches, 'deep_drawdown_hit': deep,
        'stabilization_complete': combine(c['value'] for c in stability_checks),
        'rebound_confirmation_complete': combine(c['value'] for c in confirmation),
        'central': {'current': current, 'grade': row.get('tier') if current else None,
                    'score': card.get('total') if current else None,
                    'gpt_score': (card.get('gpt') or {}).get('total') if current else None,
                    'book_score': book.get('total') if current else None,
                    'book_pass_n': book.get('pass_n') if current else None,
                    'book_required': book.get('required') if current else None,
                    'reason_codes': deepcopy(row.get('reason_codes') or []),
                    'action_blocks': deepcopy(row.get('action_blocks') or []),
                    'original_plan': deepcopy(row.get('trade_plan') or {})},
        'blockers': blockers,
        'domain_coverage': [{'id': k, 'title': v.get('title'), 'status': v.get('status')}
                            for k, v in domains.items()],
        'scope': '仅逐股影子比较；未接管全市场筛选、排序或入场。不回填历史筛除记录。',
        'no_grade_authority': True, 'entry_permission': False, 'decision_weight': 0,
        'model_calls': 0, 'network_calls': 0, 'predictive_win_probability': None}
    result['input_id'] = hashlib.sha256(json.dumps(result, ensure_ascii=False, sort_keys=True,
                                                 allow_nan=False).encode()).hexdigest()
    return result
