"""Evidence-bound future *conditional* paths, never prices or probabilities.

The five ordinal states (-2..2) are drawing coordinates. Path templates encode
the order of conditions in the existing annual outlook, not measured expected
returns, statistical confidence, or a forecast of the week of a turning point.
All functions are pure: no I/O, current-clock lookup, model calls or trade power.
"""
from datetime import date, timedelta
import hashlib
import json
import math

VERSION = 'trend-scenarios-v1'
WEEKS = (0, 2, 4, 8, 16, 32, 52)
_REGIMES = {'up', 'up_caution', 'down', 'range'}
_PATHS = {
    'up': ([1, 1, 1, 1, 1, 1, 1], [1, 1, 1.5, 1.5, 2, 2, 2],
           [1, .5, 0, -.5, -1, -1.5, -2]),
    'up_caution': ([.5, 0, -.5, -.5, 0, .5, .5],
                   [.5, .5, .5, .5, 1, 1.5, 1.5],
                   [.5, 0, -.5, -1, -1.5, -2, -2]),
    'down': ([-1, -1, -1, -1, -1, -.5, -.5],
             [-1, -.5, 0, 0, .5, 1, 1.5],
             [-1, -1, -1.5, -1.5, -2, -2, -2]),
    'range': ([0, 0, 0, 0, 0, 0, 0], [0, .5, .5, 1, 1, 1.5, 1.5],
              [0, -.5, -.5, -1, -1, -1.5, -1.5]),
}
_PHASES = {
    'up': {'label': '上行结构·延续待确认', 'x': -.5, 'y': .65, 'dx': .15, 'dy': .15},
    'up_caution': {'label': '高位承压·先看修复', 'x': .65, 'y': .25, 'dx': .05, 'dy': -.35},
    'down': {'label': '下行结构·修复待确认', 'x': .3, 'y': -.65, 'dx': -.1, 'dy': -.15},
    'range': {'label': '震荡蓄势·方向待确认', 'x': -.65, 'y': 0, 'dx': 0, 'dy': 0},
}


def _finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def _canonical(code):
    code = str(code or '').strip().upper().replace('.SH', '.SS')
    if code.endswith('.HK') and code[:-3].isdigit():
        return str(int(code[:-3])) + '.HK'
    return code


def _date(value):
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _seal(doc):
    doc['input_id'] = hashlib.sha256(json.dumps(doc, ensure_ascii=False, sort_keys=True,
                                                allow_nan=False).encode()).hexdigest()
    return doc


def _base(code, name, market, source_asof, scope):
    return {'version': VERSION, 'code': code, 'name': str(name or code),
            'market': str(market or ''), 'source_asof': source_asof,
            'status': 'missing', 'headline': '未来条件趋势待补证',
            'phase': {'label': '起点待核', 'x': None, 'y': None, 'dx': None, 'dy': None},
            'points': [], 'assumptions': [], 'gaps': [], 'reference_ids': {},
            'scope': scope, 'method': '本地规则情景；未经概率标定',
            'scale': {'min': -2, 'max': 2, 'meaning': '定性趋势示意，不是价格、涨幅或概率',
                      'levels': {-2: '弱', -1: '偏弱', 0: '震荡', 1: '偏强', 2: '强'}},
            'entry_permission': False, 'no_grade_authority': True,
            'model_calls': 0, 'network_calls': 0}


def _paths(regime, *, top=False, company_adverse=False, company_support=False):
    base, up, down = [list(values) for values in _PATHS[regime]]
    if top and regime != 'up_caution':
        for index in (1, 2, 3):
            base[index] = min(base[index], -.5)
            up[index] = min(up[index], .5)
    if company_adverse:
        # A contrary company case is not erased by bullish technical structure.
        for index in range(1, len(base)):
            base[index] = min(base[index], 0)
            up[index] = min(up[index], 1)
    elif not company_support:
        # Long-dated extension remains limited without company/valuation support.
        for index in (5, 6):
            base[index] = min(base[index], .5)
    return base, up, down


def _point(week, anchor, values, label, condition, invalidation, up_condition,
           down_condition, recovery):
    base, up, down = values
    return {'weeks': week, 'date': (anchor + timedelta(weeks=week)).isoformat(),
            'base': base, 'up': up, 'down': down, 'label': label,
            'condition': condition, 'invalidation': invalidation,
            'branches': {
                'base': {'label': '基准条件路径', 'condition': condition, 'invalidation': invalidation},
                'up': {'label': '改善条件路径', 'condition': up_condition, 'invalidation': invalidation},
                'down': {'label': '恶化条件路径', 'condition': down_condition, 'invalidation': recovery},
            }}


def build_stock(annual, synthesis, period, code='', name=''):
    """Derive future scenario paths from the same annual/synthesis/period bundle.

    The existing period receipt establishes the complete OHLCV comparison. This
    consumer additionally requires identity, source date, snapshot and IDs to
    agree; an unknown or incomplete receipt never produces a neutral fake line.
    """
    annual = annual if isinstance(annual, dict) else {}
    synthesis = synthesis if isinstance(synthesis, dict) else {}
    period = period if isinstance(period, dict) else {}
    code = _canonical(code or annual.get('code'))
    out = _base(code, name, annual.get('market'), annual.get('source_asof'), 'stock')
    out.update(snapshot_signature=annual.get('snapshot_signature'), source=annual.get('source'),
               price_basis=annual.get('price_basis'))
    out['reference_ids'] = {'annual': annual.get('input_id'), 'synthesis': synthesis.get('input_id'),
                            'period': period.get('input_id')}
    gaps = []
    if not code or any(_canonical(row.get('code')) != code for row in (annual, synthesis, period)):
        gaps.append('个股身份未一致')
    sig = annual.get('snapshot_signature')
    if not sig or any(row.get('snapshot_signature') != sig for row in (synthesis, period)):
        gaps.append('年度与联合研判快照未一致')
    source = annual.get('source_asof')
    source_day = _date(source)
    if not source_day or any(row.get('source_asof') != source for row in (synthesis, period)):
        gaps.append('原行情时点未一致或缺失')
    if not all(out['reference_ids'].values()) or not (
            period.get('annual_input_id') == annual.get('input_id') and
            period.get('synthesis_input_id') == synthesis.get('input_id')):
        gaps.append('同包关联凭据未通过')
    if period.get('status') not in ('linked', 'caution', 'divergent'):
        gaps.append('跨周期完整核对未通过')
    if annual.get('data_status') != 'complete' or annual.get('valid12month') is not True:
        gaps.append('完整年度行情证据不足')
    regime = annual.get('regime')
    if regime not in _REGIMES:
        gaps.append('年度方向起点未形成')
    anchor = _date(annual.get('outlook_start'))
    if not anchor or (source_day and source_day > anchor):
        gaps.append('未来起点与原行情日期未核实')
    phases = annual.get('phases') or []
    if (not isinstance(phases, list) or len(phases) != 4 or any(
            not isinstance(p, dict) or not all(isinstance(p.get(k), str) and p[k].strip()
                for k in ('outlook', 'confirmation', 'invalidation')) for p in phases)):
        gaps.append('年度四阶段确认与失效条件不完整')
    if gaps:
        out['gaps'] = gaps
        return _seal(out)
    evidence = annual.get('evidence') if isinstance(annual.get('evidence'), dict) else {}
    top = bool(annual.get('top_warning') or (synthesis.get('turning') or {}).get('side') == 'top'
               or (synthesis.get('turning') or {}).get('mixed'))
    if top and regime == 'up':
        regime = 'up_caution'
    adverse = bool(evidence.get('company_adverse'))
    support = bool(evidence.get('company_support'))
    counterevidence = []
    for domain in evidence.get('domains') or []:
        if (isinstance(domain, dict) and domain.get('domain') in ('fundamental', 'valuation')
                and domain.get('state') == '反对'):
            counterevidence.append({'domain': domain['domain'], 'title': domain.get('title') or domain['domain'],
                                    'reasons': [str(reason) for reason in domain.get('reasons') or [] if reason]})
    counter_text = '；'.join(str(d['title'])+'：'+'；'.join(d['reasons']) for d in counterevidence)
    # Interpret only the same-bound annual conclusion; never re-score raw domains.
    base, up, down = _paths(regime, top=top, company_adverse=adverse, company_support=support)
    divergent = period.get('status') == 'divergent'
    if divergent and not top:
        # A short/annual disagreement changes the near branch, not just a footnote.
        # Preserve the annual direction at longer checkpoints, conditional on
        # the original annual confirmation. No new numeric history score enters.
        for index in (1, 2, 3):
            if regime == 'up':
                base[index] = min(base[index], 0)
                up[index] = min(up[index], 1)
            elif regime == 'down':
                base[index] = max(base[index], -.5)
    out.update(status='ready', headline=annual.get('headline') or '未来一年条件趋势',
               phase=dict(_PHASES[regime]), anchor_date=anchor.isoformat(), regime=regime,
               top_warning=top, company_adverse=adverse, counterevidence=counterevidence)
    if top and annual.get('regime') != 'up_caution':
        out['headline'] = '先复核当前转弱风险；'+out['headline']
    if adverse:
        out['headline'] = '结构与企业证据有分歧；'+out['headline']
    if top and regime == 'range':
        out['phase'] = {'label': '震荡承压·风险待解除', 'x': .55, 'y': 0, 'dx': 0, 'dy': -.3}
    if divergent and not top and regime == 'up':
        out['phase'] = {'label': '年结构向上·短期分歧', 'x': -.2, 'y': .4, 'dx': .1, 'dy': -.2}
    elif divergent and not top and regime == 'down':
        out['phase'] = {'label': '年结构偏弱·短期修复', 'x': -.2, 'y': -.4, 'dx': -.1, 'dy': .2}
    out['assumptions'] = [
        '蓝色虚线为基准条件路径，绿/红虚线为改善/恶化分支；所有未来点须逐项兑现条件。',
        '线形按当前结构与风险选择定性条件模板；它不是按周测算出的收益路径。',
        '曲线高低只表示定性方向强弱，不预测价格、涨幅、胜率或拐点日期。',
        '2/4/8/16/32/52周是后续核验时点；时间过去本身不代表条件成立。',
        '依据原行情与同包年度/联合研判，历史窗口分数未连接成未来曲线。',
        '本地规则情景，未经概率标定；不授评级、不修改原交易合同。',
    ]
    if top:
        out['assumptions'].append('近2–8周优先观察回撤与风险解除；8周节点不代表预定见顶或必涨。')
    if adverse:
        out['assumptions'].append('当前企业或估值反证未解除；改善分支额外要求原反证逐项解决。')
        if counter_text:
            out['assumptions'].append('年度同包原反证：'+counter_text)
    elif not support:
        out['assumptions'].append('企业与估值尚无完整同包支持；中长期扩展须补证，不能继承短期GPT评级。')
    if period.get('status') == 'divergent':
        out['assumptions'].append('短期动量与年度结构存在分歧；需先复核跨周期冲突，不能称为全周期同向。')
    for i, week in enumerate(WEEKS):
        stage = 0 if week <= 12 else 1 if week <= 26 else 2 if week <= 39 else 3
        p = phases[stage]
        if not week:
            condition = '已核验行情起点；'+str(annual.get('thesis') or p['outlook'])
            invalidation = '原行情、身份或同包关联失效时停止沿用本次情景'
            up_condition = down_condition = condition
            label = '当前结构'
        else:
            previous = [phases[j]['confirmation'] for j in range(stage)]
            cumulative = ('先核验此前阶段：'+'；'.join(previous)+'。') if previous else ''
            condition = cumulative+p['confirmation']
            invalidation = p['invalidation']
            if top and week <= 8:
                condition = '先解除当前量价转弱预警，确认止跌或收复关键均线；'+condition
            elif divergent and week <= 8:
                condition = '先复核短期动量与年度结构的分歧，确认修复或延续得到同向新证据；'+condition
            up_condition = '改善分支仅在以下条件成立后保留：'+condition
            if adverse:
                condition = '企业/估值反证仍在时维持保守或分歧，不由量价分抵消。'+condition
                up_condition = '先逐项解决当前企业/估值反证，并由新财报/估值证据复核；'+up_condition
                if counter_text:
                    up_condition += ' 原反证待解决：'+counter_text
            elif week >= 16 and not support:
                up_condition = '先补齐同周期基本面与估值支持；'+up_condition
            down_condition = '恶化分支仅在结构或证据失效时启用：'+invalidation
            label = p['outlook']
        out['points'].append(_point(week, anchor, (base[i], up[i], down[i]), label,
            condition, invalidation, up_condition, down_condition,
            '停止恶化并完成原年度修复条件后，重新选择情景；'+p['confirmation']))
    out['gaps'] = list(dict.fromkeys(str(x) for x in (annual.get('gaps') or []) + (synthesis.get('review_blocks') or [])))
    return _seal(out)


def build_sector(row, market='', source_asof='', *, required_session=''):
    """Use dated sector facts to form conditional continuation/repair scenarios.

    Legacy horizon scores/confidence/turning labels are deliberately ignored.
    Long-horizon points are a conditional monitoring framework, with explicit
    requirement for new sector earnings and valuation evidence, not an annual
    company forecast inferred from twenty days of market data.
    """
    row = row if isinstance(row, dict) else {}
    source = source_asof or row.get('source_asof')
    out = _base(str(row.get('symbol') or row.get('name') or ''), row.get('name'), market, source, 'sector')
    out['required_session'] = required_session or None
    facts = row.get('facts') if isinstance(row.get('facts'), dict) else {}
    anchor = _date(source)
    gaps = []
    if not row.get('name'):
        gaps.append('板块名称缺失')
    if not anchor:
        gaps.append('板块原行情时间缺失；生成时间不能替代')
    required = _date(required_session)
    if required_session and (not required or anchor != required):
        gaps.append('板块原行情未匹配最新应有完整交易日，需刷新后再给当前情景')
    if source_asof and row.get('source_asof') and str(source_asof) != str(row['source_asof']):
        gaps.append('板块事实与所传原时点不一致')
    keys = ('1d', '5d', '20d', 'vs_ma20', 'vs_ma60', 'vol_ratio')
    for key in keys:
        if not _finite(facts.get(key)):
            gaps.append(f'板块因子 {key} 缺失或无效')
    if _finite(facts.get('vol_ratio')) and facts['vol_ratio'] < 0:
        gaps.append('板块量比不能为负')
    if gaps:
        out['gaps'] = gaps
        return _seal(out)
    c1, c5, c20, m20, m60, vr = (facts[k] for k in keys)
    up_struct = c20 > 0 and m20 > 0 and m60 > 0
    down_struct = c20 < 0 and m20 < 0 and m60 < 0
    top = bool((c20 > 8 and c5 < 0) or (c1 <= -3 and vr >= 1.5) or (m20 > 10 and vr < 1))
    regime = 'up_caution' if up_struct and top else 'up' if up_struct else 'down' if down_struct else 'range'
    base, up, down = _paths(regime, top=top)
    out.update(status='ready', headline={
        'up': '板块偏强：先验延续，再验盈利兑现',
        'up_caution': '板块高位承压：先看退潮消化，再验修复',
        'down': '板块偏弱：先看止跌，再验趋势修复',
        'range': '板块震荡：等待方向选择与同业确认'}[regime],
        regime=regime, phase=dict(_PHASES[regime]), top_warning=top,
        anchor_date=anchor.isoformat(), fact_inputs={k: facts[k] for k in keys})
    if top and regime == 'range':
        out['phase'] = {'label': '震荡承压·风险待解除', 'x': .55, 'y': 0, 'dx': 0, 'dy': -.3}
    out['assumptions'] = [
        '本地规则情景，未经概率标定；线形是条件顺序示意，不是价格预测。',
        '原2/5/8/16周档分数未连接成未来轨迹，也不据此推算拐点时间。',
        '未来每个节点必须用新行情核验，时间过去本身不代表转强或修复。',
        '16–52周为条件跟踪框架；须新增板块盈利、估值和成分股广度证据，当前短窗数据不证明全年前景。',
        '板块情景不能代替个股年度分析、同包GPT审核或中央进场合同。',
    ]
    if top:
        out['assumptions'].append('当前退潮/量价风险优先；近2–8周改善线仍受风险解除条件约束。')
    for i, week in enumerate(WEEKS):
        if not week:
            condition = '原时点5/20日动量、MA20/60位置与量比已核验'
            invalid = '板块身份、时点或因子数据失效时停止沿用'
            label = '当前板块结构'
        elif week <= 4:
            condition = '后续周动量与量能同向，MA20位置及板块内部上涨广度再次确认'
            invalid = '周动量转弱、跌回MA20且放量，或同业广度背离'
            label = '短期延续 / 退潮核验'
        elif week <= 8:
            condition = '此前条件先兑现，再确认20日动量与MA20/60方向同向、回撤低点不再下移'
            invalid = 'MA20/60方向转弱、修复失败或再破结构低点'
            label = '中期修复 / 方向核验'
        else:
            condition = ('此前量价修复条件持续兑现；新增同周期板块盈利/估值披露与成分股广度共同支持；'
                         '否则不从短窗价格外推长期上升')
            invalid = '趋势低点下移、行业盈利/估值反证出现，或领涨只集中少数成分股'
            label = '盈利兑现 / 长期条件复核'
        if top and 0 < week <= 8:
            condition = '先解除退潮/量价风险并重新确认止跌；'+condition
        out['points'].append(_point(week, anchor, (base[i], up[i], down[i]), label,
            condition, invalid, '改善分支要求：'+condition,
            '恶化分支触发：'+invalid, '风险解除后重新确认量价与同业证据，再选情景'))
    out['gaps'] = ['尚无板块专用完整年度GPT结论；不继承个股评级或短窗置信标签']
    return _seal(out)
