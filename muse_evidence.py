"""Attributable review facts, not new prices, trade authority or verification votes."""
from copy import deepcopy
from datetime import datetime
import math

VERSION = 'muse-evidence-v2'


def number(value):
    return type(value) in (int, float) and math.isfinite(value)


def publication_gaps(row):
    gaps = []
    bands = []
    for key in ('entry_range', 'take_profit_range', 'stop_range'):
        values = row.get(key)
        if (not isinstance(values, (list, tuple)) or len(values) != 2
                or not all(number(v) and v > 0 for v in values) or values[0] > values[1]):
            gaps.append(key + ':missing_or_invalid')
        else:
            bands.append(values)
    if len(bands) == 3:
        entry, target, stop = bands
        if not stop[1] < entry[0] <= entry[1] < target[0]:
            gaps.append('bands:invalid_order')
    try:
        stamp = datetime.fromisoformat(str(row.get('quote_asof')).replace('Z', '+00:00'))
        if stamp.tzinfo is None:
            gaps.append('quote_asof:timezone_missing')
    except (ValueError, TypeError):
        gaps.append('quote_asof:missing_or_invalid')
    return gaps


def financial_facts(row, raw):
    """Keep absolute cash separate from cash per share and aggregate one-offs from items."""
    recurring = raw.get('recurring') or {}
    operating = raw.get('operating_check') or {}
    cash = raw.get('cash_flow') or {}
    period = raw.get('period_end')
    currency = raw.get('currency')

    def comparable(record):
        return (record.get('code') == row.get('code') and record.get('period_end') == period
                and bool(currency) and record.get('currency') == currency)

    recurring = recurring if comparable(recurring) else {}
    operating = operating if comparable(operating) else {}
    cash = cash if comparable(cash) else operating
    reported, deducted = recurring.get('reported_profit'), recurring.get('recurring_profit')
    items = [deepcopy(v) for v in raw.get('nonrecurring_items') or []
             if isinstance(v,dict) and v.get('name') and number(v.get('amount'))
             and v.get('period_end')==period and v.get('currency')==currency
             and v.get('source_url')]
    return {
        'code': row.get('code'), 'period_end': period, 'currency': currency,
        'source_type': 'data_vendor', 'provider': 'eastmoney',
        'original_filing_verified': False, 'published_at': raw.get('notice_date'),
        'recurring_profit': deducted, 'recurring_profit_yoy': recurring.get('recurring_profit_yoy'),
        'reported_profit': reported if number(reported) else raw.get('parent_profit'),
        'nonrecurring_net': reported - deducted if number(reported) and number(deducted) else None,
        'nonrecurring_items': items,
        'nonrecurring_items_status': 'PROVIDED' if items else 'MISSING',
        'nonrecurring_scope': '归母减扣非仅为净额对账，不代表已取得一次性项目名称和逐项金额',
        'operating_profit': operating.get('operating_profit'),
        'operating_cash_flow': cash.get('operating_cash_flow'),
        'operating_cash_per_share': raw.get('operating_cash_per_share'),
        'recurring_source': {k: recurring.get(k) for k in ('source_url', 'source_sha256', 'captured_at', 'verification')},
        'operating_source': {k: operating.get(k) for k in ('source_url', 'source_sha256', 'captured_at', 'verification')},
        'cash_source': {k: cash.get(k) for k in ('source_url', 'source_sha256', 'captured_at', 'verification')},
        'gaps': [name for name, value in (
            ('recurring_profit', deducted), ('operating_cash_flow', cash.get('operating_cash_flow')),
            ('nonrecurring_items', items)) if value is None or value == []],
    }


def review_evidence(row):
    cross = row.get('crosscheck') or {}
    technical = cross.get('technical') or {}
    structure = technical.get('structure') or {}
    stop = deepcopy(cross.get('stop_check') or {})
    target = row.get('target_evidence')
    if not target:
        target = {
            'status': 'UNVERIFIED', 'type': 'unattributed_plan',
            'target_range': row.get('take_profit_range'),
            'method': row.get('price_plan_method'), 'explanation': row.get('price_plan_scope'),
            'reason': '未随原计划保存可复算目标锚；不凭目标价反推结构或把风险倍数冒充前高',
        }
    return {
        'version': VERSION,
        'quote': {'asof': row.get('quote_asof'), 'session': row.get('source_session'),
                  'price': row.get('last'), 'currency': row.get('currency'),
                  'completed_session': technical.get('asof'),
                  'scope': '报价时点与技术完整日线时点分别保留；盘中报价不是收盘确认'},
        'validated_stop': deepcopy(structure.get('validated_stop') or {
            'status': 'UNVERIFIED', 'reason': stop.get('reason') or '没有同口径已确认结构锚'}),
        'stop_anchor': stop, 'target_anchor': deepcopy(target),
        'technical': {k: deepcopy(technical.get(k)) for k in (
            'status', 'asof', 'required_completed_session', 'provenance', 'gmma',
            'price_volume', 'structure', 'gaps')},
        'financial': deepcopy(row.get('financial_evidence') or {
            'status': 'MISSING', 'gaps': ['原始同报告期财务数字与来源未随本版提供']}),
        'publication_gaps': publication_gaps(row),
    }


def snapshot_context(snapshot):
    audits = []
    for module, key in (('3A', 'strategy_board'), ('Astra', 'astra_cycle')):
        doc = snapshot.get(key) or {}
        from action_directive import publishable
        shown = publishable(doc, module)
        audit = doc.get('directive_audit') or (doc.get('directive_summary') or {}).get('audit') or {}
        audits.append({'module': module, **deepcopy(audit),
                       'rows_complete': len(audit.get('rows') or []) == audit.get('checked'),
                       'publication_excluded': deepcopy(shown.get('publication_excluded') or [])})
    return {
        'version': VERSION, 'directive_audits': audits,
        'environment': deepcopy(snapshot.get('review_environment') or snapshot.get('free_market_snapshot') or {}),
        'funding': deepcopy(snapshot.get('funding_watch') or {
            'status':'MISSING','reason':'资金采集快照尚未生成；正文六类字段逐项注明未取得'}),
        'scope': '每份草案内嵌机器可读复核全表；缺失项明列，不以通过数冒充证据完整',
    }
