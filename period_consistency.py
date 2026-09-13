"""Bind retrospective windows to one conditional outlook, without trade authority."""
from html import escape
import hashlib
import json

VERSION = 'period-consistency-v1'


def build(code, frame, quality, cycles, trend, annual, synthesis):
    from annual_outlook import _canonical, _signature
    from v88_decision_core import build_horizon_facts
    quality = quality or {}; annual = annual or {}; synthesis = synthesis or {}
    facts = (cycles or {}).get('facts') or {}
    errors = []; observations = []
    sig = quality.get('snapshot_signature'); asof = quality.get('source_asof')
    try:
        if frame is None or frame.empty or not sig or _signature(frame) != sig:
            errors.append('完整行情快照未核实')
        if not code or any(_canonical(d.get('code')) != _canonical(code) for d in (quality, annual, synthesis)):
            errors.append('个股身份未一致')
        if not sig or any(d.get('snapshot_signature') != sig for d in (annual, synthesis)):
            errors.append('年度与联合研判快照不一致')
        if not asof or any(d.get('source_asof') != asof for d in (annual, synthesis)):
            errors.append('年度与联合研判行情日不一致')
        if annual.get('price_basis') != quality.get('price_basis') or annual.get('source') != quality.get('source'):
            errors.append('年度行情来源或价格口径不一致')
        if frame is not None and not frame.empty:
            expected = build_horizon_facts(frame, full=trend)
            # Compare every derived value, not the legacy eight-close digest alone.
            same = all(facts.get(k) == expected.get(k) for k in expected if k != 'horizons')
            for label, values in expected.get('horizons', {}).items():
                actual = (facts.get('horizons') or {}).get(label) or {}
                same = same and all(actual.get(k) == v for k, v in values.items())
                n = values['sample_days']
                display = {'lookback_start': str(frame.index[-n-1])[:10],
                           'lookback_end': str(frame.index[-1])[:10],
                           'requested_days': values['weeks'] * 5,
                           'window_complete': n >= values['weeks'] * 5,
                           'evidence_scope': 'historical-lookback'}
                same = same and all(actual.get(k) == v for k, v in display.items())
                observations.append({'window': label, 'trading_days': n,
                    'complete': n == values['weeks'] * 5,
                    'start': str(frame.index[-n-1])[:10], 'end': str(frame.index[-1])[:10],
                    'score': values['rule_score'], 'return_pct': values['return_pct'],
                    'meaning': '已发生窗口的量价方向分，非未来涨幅或概率'})
            if not same or not expected.get('horizons') or facts.get('evidence_scope') != 'historical-lookback':
                errors.append('五周期事实与完整行情重算不一致')
    except (KeyError, TypeError, ValueError, IndexError):
        errors.append('跨周期输入未通过核对')

    phase = next(iter(annual.get('phases') or []), {})
    conditions = [phase[k] for k in ('confirmation', 'invalidation') if phase.get(k)]
    source_gaps = list(synthesis.get('review_blocks') or []) + list(annual.get('gaps') or [])
    if errors:
        status = 'blocked'; headline = '跨周期未核对，暂不合并方向'
        summary = '；'.join(dict.fromkeys(errors)); observations = []; conditions = []
    elif annual.get('data_status') != 'complete':
        status = 'limited'; headline = '历史窗口可观察，年度证据仍待补齐'
        summary = '回看窗口不能补成一年预测；先补齐完整年资料，再判断全年条件路径。'
    else:
        top = bool(annual.get('top_warning'))
        scores = {v['window']: v['score'] for v in observations if v['complete']}
        short = scores.get('2周'); eight = scores.get('8周')
        regime = annual.get('regime')
        conflict = bool(short is not None and ((short >= 59 and regime == 'down') or
                        (short <= 41 and regime in ('up', 'up_caution'))))
        status = 'caution' if top else 'divergent' if conflict else 'linked'
        headline = annual.get('headline') or '年度条件主线待核'
        eight_text = (f'过去8周窗口量价分 {eight}/100' if eight is not None else '过去8周完整窗口不足')
        summary = eight_text + '；它描述截至当前的历史结构，不能推出未来8周必涨或第8周见顶。'
        if top:
            summary += ' 当前转弱预警优先纳入近端判断：先处理回撤，确认条件兑现后才考虑延续分支。'
        elif conflict:
            summary += ' 短窗动量与年结构存在分歧，先核实修复或失效条件，不能称为全周期同向。'
        else:
            summary += ' 后续方向统一按年度四阶段的确认与失效条件更新。'
        if (annual.get('evidence') or {}).get('company_adverse'):
            summary += ' 基本面或估值的反对证据仍保留，量价分不能抵消。'
    result = {'version': VERSION, 'code': _canonical(code), 'status': status,
              'headline': headline, 'summary': summary, 'conditions': conditions,
              'observations': observations, 'gaps': list(dict.fromkeys(errors + source_gaps)),
              'source_asof': asof, 'snapshot_signature': sig,
              'annual_input_id': annual.get('input_id'), 'synthesis_input_id': synthesis.get('input_id'),
              'model_calls': 0, 'network_calls': 0, 'no_grade_authority': True, 'entry_permission': False}
    result['input_id'] = hashlib.sha256(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False).encode()).hexdigest()
    return result


def html(doc):
    if not doc:
        return ''
    e = lambda value: escape(str(value))
    color = '#b45309' if doc.get('status') in ('blocked', 'caution', 'divergent', 'limited') else '#0369a1'
    return (f'<section class="v88-period-consistency" data-input-id="{e(doc.get("input_id", ""))}" '
            f'style="border-left:3px solid {color};padding:7px 9px;font-size:12px;line-height:1.55">'
            f'<b>🔗 跨周期共同结论 · {e(doc.get("headline", "待核"))}</b>'
            f'<div>{e(doc.get("summary", ""))}</div>'
            '<details><summary style="font-size:11px">共同确认条件与未解决证据</summary>'
            + ''.join(f'<div>{e(x)}</div>' for x in doc.get('conditions', []) + doc.get('gaps', []))
            + f'<div>行情 {e(doc.get("source_asof"))} · 关联 {e(doc.get("input_id", "")[:12])} · 原评级与交易合同仍由中央审核决定。</div></details></section>')
