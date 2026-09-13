"""Small, read-only model context; full chart observations remain local.

Explicit schemas retain conditions, counterevidence and original execution
constraints. No source is fetched, no model is called, and no input is changed.
Unknown objects are not serialized wholesale into prompts.
"""
import math
import json

VERSION = 'deep-prompt-context-v2'


def _fields(names):
    return {name: None for name in names.split()}


def _project(value, schema):
    if isinstance(schema, tuple):
        return _project(value, schema[1] if isinstance(value, dict) else schema[0])
    if schema is None:
        if value is None or type(value) in (str, bool, int):
            return value
        return value if type(value) is float and math.isfinite(value) else None
    if isinstance(schema, list):
        return [_project(item, schema[0]) for item in value] if isinstance(value, (list, tuple)) else []
    if not isinstance(value, dict):
        return {}
    return {key: _project(value[key], child) for key, child in schema.items() if key in value}


REF = _fields('version code market factpack_id audit_id input_id evidence_id source_id source_sha256 '
              'snapshot_signature series_id source source_url provider source_asof generated_at '
              'price_basis scope')
REF.update(receipt_hex_groups=[None], evidence_ids=[None])
CONDITIONS = _fields('risk reason condition confirmation invalidation next_action description status title')
TEXT_OR_CONDITION = (None, CONDITIONS)
REVIEW = _fields('role conclusion reason')
REVIEW.update(effects=[None], evidence_ids=[None], conditions=[TEXT_OR_CONDITION], risks=[TEXT_OR_CONDITION])
DOMAIN = _fields('domain title status state')
DOMAIN.update(reviews=[REVIEW], reasons=[None], limitations=[None], origins=[REF])
THRESHOLDS = _fields('1A 2A 3A')
PROFIT = _fields('version valid eligible tier_cap net_upside_pct gross_upside_pct cost_assumption '
                 'buy_cost_pct sell_cost_pct formula guaranteed net_reward_risk stop max_calendar_days '
                 'holding_sessions thesis_deadline evidence_asof annual_evidence target_basis '
                 'high_space_label exit_rule next_grade_requirements')
PROFIT.update(entry_range=[None], take_profit_range=[None], reasons=[None], period_thresholds=THRESHOLDS,
              thresholds={h: THRESHOLDS for h in ('short', 'medium', 'long')})
PLAN = _fields('today_type promotion_trigger trigger entry_condition invalidation stop target rr last '
               'horizon position_cap ready deadline thesis_deadline exit_rule risk_note')
PLAN.update(entry_range=[None], take_profit_range=[None], conditions=[TEXT_OR_CONDITION], risks=[TEXT_OR_CONDITION],
            profit_contract=PROFIT,
            required_checks=_fields('today_type entry trigger invalidation stop target_above_last horizon '
                                    'position_cap_at_most_15pct rr_at_least_1_5_recomputed profit_contract_eligible'),
            profit_inputs={**REF, **_fields('horizon kind max_calendar_days holding_sessions asof')})
METRICS = _fields('window_start window_end annual_bars expected_annual_bars calendar_basis last '
                  'annual_complete ma60 ma120 ma200 ma60_slope20_pct ma120_slope20_pct ma200_slope20_pct '
                  'return60_pct high60 low60 return1_pct volume_ratio20 observed_window_high '
                  'observed_window_low annual_high annual_low annual_return_pct annual_max_drawdown_pct '
                  'current_drawdown_from_annual_high_pct annual_position_pct')
ANNUAL = {**REF, **_fields('headline thesis regime data_status valid12month top_warning outlook_start '
                          'outlook_end return_basis structure_scope method_scope no_grade_authority entry_permission')}
ANNUAL.update(metrics=METRICS,
              phases=[_fields('phase start end outlook confirmation invalidation')],
              scenarios=[_fields('scenario path confirmation invalidation')],
              evidence={**_fields('joint_review_current original_horizon central_grade central_audit_score '
                                  'long_review_current annual_gpt_current company_support company_adverse scope'),
                        'domains': [DOMAIN], 'evidence_ids': [None]},
              gaps=[None], risks=[TEXT_OR_CONDITION], risk_conditions=[CONDITIONS], evidence_ids=[None])
OBSERVATIONS = _fields('bars source_asof last ma200 rsi14 return1_pct volume_ratio20 annual_position momentum')
TURNING = _fields('side label score confidence confirmed_reversal mixed volume_ratio20 return1_pct '
                  'invalidation confirmation risk_note')
TURNING.update(signals=[None], risks=[TEXT_OR_CONDITION], conditions=[TEXT_OR_CONDITION])
JOINT = {**REF, **_fields('status price_coherent joint_review_current annual_label momentum_label '
                         'explanation business_conclusion central_grade central_audit_score '
                         'entry_recheck_required entry_permission no_grade_authority')}
JOINT.update(review_blocks=[None], unresolved_domains=[None], domains=[DOMAIN], observations=OBSERVATIONS,
             turning=TURNING, original_plan=PLAN,
             technical_cycle=_fields('short_score medium_score long_score cycle_conflict cycle_status score_version'))
CROSS = {**REF, **_fields('status checked_at current_grade audit_score technical_score score_version horizon '
                         'original_net_space original_net_rr deadline current_price '
                         'current_price_meets_numeric_hurdles no_grade_authority independent_data_validation '
                         'predictive_win_probability')}
CROSS.update(checks=[_fields('id title status detail next_action')],
             indicator_comparisons=[_fields('field central deep matches')], original_plan=PLAN,
             current_price_scenario=_fields('entry target stop net_upside_pct net_reward_risk net_risk_pct'),
             technical_reference=_fields('stop resistance rr action horizon'), next_actions=[None])
PERIOD = {**REF, **_fields('headline summary status explanation conclusion cycle_conflict '
                          'annual_input_id synthesis_input_id no_grade_authority entry_permission')}
PERIOD.update(conditions=[TEXT_OR_CONDITION], risks=[TEXT_OR_CONDITION],
              confirmation=None, invalidation=None, evidence_ids=[None], review_blocks=[None], gaps=[None],
              observations=[_fields('window trading_days complete start end score return_pct meaning')])
CROSS['period_consistency'] = PERIOD
# Only seven conditional checkpoints; no chart markup or OHLCV payload enters
# an explicitly requested model review. The same evidence drives the UI.
FUTURE = {**REF, **_fields('status headline anchor_date regime top_warning company_adverse method no_grade_authority entry_permission')}
FUTURE.update(reference_ids=_fields('annual synthesis period'),
              points=[{**_fields('weeks date base up down label condition invalidation'),
                       'branches': {key:_fields('label condition invalidation') for key in ('base', 'up', 'down')}}],
              counterevidence=[{**_fields('domain title'), 'reasons':[None]}],
              scale=_fields('min max meaning'), assumptions=[None], gaps=[None])
CROSS['future_scenario'] = FUTURE
CENTRAL = {**REF, **_fields('tier executable audit_score horizon state authority')}
CENTRAL.update(trade_plan=PLAN, reason_codes=[None], constraints=[TEXT_OR_CONDITION], risks=[TEXT_OR_CONDITION])


def annual_summary(doc):
    """Retain annual conclusions and provenance, never daily chart arrays."""
    result = _project(doc, ANNUAL)
    if not isinstance(doc, dict):
        return result
    metrics = doc.get('metrics') or {}
    for field in ('missing_sessions', 'unexpected_dates'):
        if isinstance(metrics, dict) and isinstance(metrics.get(field), list):
            result.setdefault('metrics', {})[field + '_count'] = len(metrics[field])
    series = doc.get('history')
    if isinstance(series, dict):
        result['series_evidence'] = _project(series, {**REF,
            **_fields('status annual_complete expected_annual_bars calendar_basis series_basis window_start window_end'),
            'source_range': _fields('start end bars'), 'gaps': [None]})
        for field in ('missing_sessions', 'source_missing_sessions', 'unexpected_dates'):
            if isinstance(series.get(field), list):
                result['series_evidence'][field + '_count'] = len(series[field])
    return result


def build(*, annual_outlook=None, joint_conclusion=None, cross_validation=None, central=None):
    """Whitelist the annual/deep inputs at the two optional GPT boundaries."""
    joint = _project(joint_conclusion, JOINT)
    result = {'prompt_context_version': VERSION,
              'authority': '只解释中央原评级与原合同；缺证或反证不解除前不得新增开仓，不得移动止损、目标或期限。',
              'central_constraints': _project(central, CENTRAL),
              'review_blocks': joint.get('review_blocks', []),
              'annual_outlook': annual_summary(annual_outlook),
              'joint_conclusion': joint,
              'cross_validation': _project(cross_validation, CROSS)}
    return result


def model_context(text):
    """Recognize our structured context and re-project it before lifting a cap.

    A version marker alone is not permission to send unknown JSON or raw chart
    arrays. Ordinary free text and unknown versions retain the caller's limit.
    """
    if not isinstance(text, str):
        return None
    try:
        doc = json.loads(text)
    except (ValueError, TypeError):
        return None
    if not isinstance(doc, dict) or doc.get('prompt_context_version') != VERSION:
        return None
    result = build(annual_outlook=doc.get('annual_outlook'), joint_conclusion=doc.get('joint_conclusion'),
                   cross_validation=doc.get('cross_validation'), central=doc.get('central_constraints'))
    # series_evidence is already the small provenance projection, not a chart.
    annual = doc.get('annual_outlook') or {}
    if isinstance(annual, dict) and isinstance(annual.get('series_evidence'), dict):
        series_schema = {**REF, **_fields('status annual_complete expected_annual_bars calendar_basis '
                         'series_basis window_start window_end missing_sessions_count '
                         'source_missing_sessions_count unexpected_dates_count'),
                         'source_range': _fields('start end bars'), 'gaps': [None]}
        result['annual_outlook']['series_evidence'] = _project(annual['series_evidence'], series_schema)
    metrics = annual.get('metrics') if isinstance(annual, dict) else None
    if isinstance(metrics, dict):
        for key in ('missing_sessions_count', 'unexpected_dates_count'):
            if key in metrics:
                result['annual_outlook'].setdefault('metrics', {})[key] = _project(metrics[key], None)
    return json.dumps(result, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
