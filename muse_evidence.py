"""Attributable review facts, not new prices, trade authority or verification votes."""
from copy import deepcopy
from datetime import datetime
import math

VERSION = 'muse-evidence-v4'


def stable_review_facts(value):
    """Keep execution evidence, excluding only moving assessment clocks/ages.

    Quote/source timestamps, execution states, reasons and thresholds remain
    signed. Recurse through frozen signal plans without mutating source facts.
    """
    if isinstance(value, dict):
        out = {key: stable_review_facts(item) for key, item in value.items()}
        readiness = out.get('execution_readiness')
        if isinstance(readiness, dict):
            readiness.pop('assessed_at', None)
            readiness.pop('quote_age_seconds', None)
        return out
    if isinstance(value, (list, tuple)):
        return [stable_review_facts(item) for item in value]
    return deepcopy(value)


def review_directive(row, module=None):
    """Use the same recursive privacy boundary as every public instruction."""
    from action_directive import public_directive
    return stable_review_facts(public_directive(row.get('directive') or {}, module))


def review_task():
    """Scope of the independent stock review, not an instruction to endorse V88."""
    return {
        'version': 'selected-stock-review-2026-11-16-v3',
        'task': '逐项复核本包筛选后的股票、买入/等待状态及原价格计划，给出独立结论和可追溯证据。',
        'input': 'items[].facts；最新名单与正式定版各自绑定，不能用运行日志或端口检查代替个股复核。',
        'policy': '先核 facts.decision_contract 与记录的规则版本；旧统一RR/评分门槛不能冒充本版系统规则。'
                  '可以独立反对本版规则，但须注明独立判断及依据，不能仅因系统自检通过而判通过。',
        'evidence': '核对原财务、同复权口径日线、量价拐点、位置追高、止损目标、财报及到期。'
                    '盈亏比请列出实际入场、首目标、止损、每股费用及阻力折价，并给出净收益和净风险。'
                    '若与系统不同，保留双方取价和算式。核对财报时列原公告的单季、累计期间及同期间基数。'
                    '历史回测胜率与本月利润情景不等于当前个股胜率或已实现收益。',
        'qualification': '按facts.entry_window_qualification核对：3A中长期向上且今明两个交易日买点；'
                         '2A中期且本周买点；1A超短且今日即时入场。90%仅是优化目标，非授级门槛或已验证概率。'
                         '胜率按原出口扣费后净收益>0定义，目标命中率另列。不符合窗口的后台研究不补主推荐。',
        'execution': '核对directive.execution_readiness与signal_plan原价格计划；研究、等待开市、'
                     '等待核价或等待触发均不等于当下可买，保留原止损、目标与期限。',
        'data_failure': '无法取得或核实本版数据时保留unverified并指明缺项；传输/端口/工作流问题单列，'
                        '不伪装为个股投资反证。复权或报告期冲突仍属于个股证据问题，必须保留。',
        'output': '每股pass/fail/unverified、简短理由、真实核查时间及证据引用；复制原身份/状态/哈希，'
                  '不重写系统计划、不提交订单、不用旧裁决认证新快照。',
    }


def decision_contract(row, module):
    """Export the applicable rule for current producers; never relabel legacy plans."""
    import action_directive as ad
    from directive_audit import rr_floor, MIN_RR, FEE
    directive = row.get('directive') or {}
    current = directive.get('version') == ad.VERSION
    floor = rr_floor(row, module) if current else None
    specific = floor is not None
    if current and floor is None:
        floor = MIN_RR['Astra' if module == 'Astra' else '3A']
    from grade_ladder import GOOD_FLOOR, LADDER_VERSION
    from horizon_book import stats, SPAN
    graded = current and (row.get('grade_basis') == 'grade-ladder-v1' or module == 'Astra')
    rule_stats = stats((row.get('astra_regime') or {}).get('cell')) if specific else None
    return {
        'version': 'muse-decision-contract-v2', 'module': module,
        'recorded_directive_version': directive.get('version'),
        'producer_version_matches': current,
        'status': 'CURRENT_RULE_EXPORTED' if current else 'LEGACY_OR_UNKNOWN_RULE',
        'rule_cell': deepcopy((row.get('astra_regime') or {}).get('cell')),
        'directive_net_rr_floor': floor,
        'rr_floor_basis': '结构计划按模块计算扣费后首目标/止损' if row.get('price_plan_version')=='structure-entry-20261125-v1' else '命中周期规则的目标/止损/费用几何' if specific else
                         '指令层通用门槛' if current else '未确认历史生成规则，不套用当前门槛',
        'directive_fee_allowance_pct': FEE if current else None,
        'fundamentals_policy_version': LADDER_VERSION if graded else None,
        'business_score_floor': GOOD_FLOOR if graded else None,
        'fundamentals_note': '基本面分只是一个检查项；反证、利润质量、资料待补仍分别核查，不能以过分数代替核验。',
        'rule_reference': {'statistics': rule_stats, 'sample_scope': SPAN,
                           'meaning': '旧出口规则历史样本，结构新版尚待独立结算'} if rule_stats else None,
        'three_a_qualification': deepcopy(row.get('three_a_qualification')),
        'entry_window_qualification': deepcopy(row.get('entry_window_qualification')),
        'scope': '指令价处净RR检查参数，不是胜率或Muse结论；并行保留报价、原计划和全部硬伤。',
    }


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


def review_evidence(row, module=None):
    cross = row.get('crosscheck') or {}
    technical = cross.get('technical') or {}
    structure = technical.get('structure') or {}
    stop = deepcopy(cross.get('stop_check') or {})
    target = row.get('target_evidence')
    directive = review_directive(row, module)
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
        'entry_progression': {k: deepcopy(row.get(k)) for k in (
            'decision_policy', 'entry_location', 'price_volume')},
        'three_a_qualification': deepcopy(row.get('three_a_qualification')),
        'entry_window_qualification': deepcopy(row.get('entry_window_qualification')),
        'execution_readiness': deepcopy(directive.get('execution_readiness')),
        'signal_plan': deepcopy(directive.get('signal_plan')),
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
        'version': VERSION, 'review_task': review_task(), 'directive_audits': audits,
        'environment': deepcopy(snapshot.get('review_environment') or snapshot.get('free_market_snapshot') or {}),
        'funding': deepcopy(snapshot.get('funding_watch') or {
            'status':'MISSING','reason':'资金采集快照尚未生成；正文六类字段逐项注明未取得'}),
        'scope': '每份草案内嵌机器可读复核全表；缺失项明列，不以通过数冒充证据完整',
    }
