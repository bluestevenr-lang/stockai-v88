"""Shared references into a frozen central stock decision; never a new rating.

Keep the desktop copy identical. Auxiliary metrics retain their own units and
source dates; they must not be treated as extra independent model approvals.
"""
import hashlib
import json
import math
from grade_focus import canonical

VERSION = 'stock-reference-v1'
PLAN_FIELDS = ('horizon', 'last', 'entry_range', 'take_profit_range', 'stop',
               'promotion_trigger', 'invalidation', 'profit_inputs', 'profit_contract')


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def reference(selection, row):
    plan = row.get('trade_plan') or row.get('central_trade_plan') or {}
    contract = {k: plan.get(k) for k in PLAN_FIELDS}
    base = {'version': VERSION, 'code': canonical(row.get('code')),
            'factpack_id': selection.get('factpack_id'),
            'central_generated_at': selection.get('generated_at'),
            'audit_id': row.get('audit_id') or (row.get('scorecard') or {}).get('audit_id'),
            'audit_score': row.get('audit_score', (row.get('scorecard') or {}).get('total')),
            'tier': row.get('tier'), 'horizon': plan.get('horizon') or row.get('horizon'),
            'contract_id': digest(contract)}
    return {**base, 'reference_id': digest(base), 'no_grade_authority': True}


def compare(ref, observation):
    """Compare only fields claiming central authority, not an unrelated factor score."""
    errors = []
    if canonical(observation.get('code')) != ref['code']:
        errors.append('股票身份不一致')
    for field, expected in (('audit_score', ref['audit_score']), ('central_tier', ref['tier']),
                            ('factpack_id', ref['factpack_id'])):
        if field in observation and observation[field] != expected:
            errors.append(field + '与中央主数据不一致')
    if observation.get('master_ref') and observation['master_ref'] != ref:
        errors.append('主数据引用版本不一致')
    plan = observation.get('trade_plan')
    if plan:
        try:
            if digest({k: plan.get(k) for k in PLAN_FIELDS}) != ref['contract_id']:
                errors.append('原交易合同不一致')
        except (ValueError,TypeError,AttributeError):
            errors.append('交易合同结构或数值无效')
    return {'ok': not errors, 'errors': errors, 'reference_id': ref['reference_id']}


def entry_distance(plan):
    last, zone = plan.get('last'), plan.get('entry_range')
    if not isinstance(zone, list) or len(zone) != 2:
        return None
    if any(type(v) not in (int, float) or not math.isfinite(v) or v <= 0 for v in [last, *zone]):
        return None
    return max(zone[0]/last - 1, last/zone[1] - 1, 0) * 100
