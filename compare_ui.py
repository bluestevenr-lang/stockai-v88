"""Read-only comparison of current central contracts and dated price evidence.

The compare set never creates a winner, grade, buy permission or replacement
stop. Technical scores remain auxiliary; they cannot reorder central quality.
"""
from __future__ import annotations

from collections import Counter
from datetime import date
from html import escape
import math
from urllib.parse import quote

MAX_COMPARE = 4
VERSION = "central-contract-compare-v2"
_C = ["#2563eb", "#0f766e", "#b45309", "#7c3aed"]
_H = {"short": "短期1–30天", "medium": "中期31–90天", "long": "长期91–365天"}
_MARKETS = ("A股", "美股", "港股")


def _number(value):
    return type(value) in (int, float) and math.isfinite(value)


def _text(value):
    return escape(str(value if value is not None else "未核实"))


def _dated(value):
    """Only genuine date keys can establish a common observation window."""
    if hasattr(value, "to_dict"):
        value = value.to_dict()
    if not isinstance(value, dict):
        return {}
    result = {}
    for key, close in value.items():
        if isinstance(close,bool):
            return {}
        try:
            day = date.fromisoformat(str(key)[:10]).isoformat()
            val = float(close)
        except (TypeError, ValueError):
            return {}
        if day in result or not math.isfinite(val) or val <= 0:
            return {}
        result[day] = val
    return dict(sorted(result.items()))


def _aligned(hist, days):
    series = {str(name): _dated(values) for name, values in hist.items()}
    if not series or any(not values for values in series.values()):
        return {}, []
    dates = sorted(set.intersection(*(set(values) for values in series.values())))[-days:]
    return {name: [values[day] for day in dates] for name, values in series.items()}, dates


def _returns(closes):
    return [closes[i] / closes[i-1] - 1 for i in range(1, len(closes))]


def correlation(a, b):
    """Match calendar observations first; a sequence without dates is unknown."""
    aligned, dates = _aligned({"a": a, "b": b}, 61)
    if len(dates) < 21:
        return None
    ra, rb = _returns(aligned["a"]), _returns(aligned["b"])
    ma, mb = sum(ra)/len(ra), sum(rb)/len(rb)
    va, vb = sum((x-ma)**2 for x in ra), sum((x-mb)**2 for x in rb)
    if va <= 0 or vb <= 0:
        return None
    return round(sum((x-ma)*(y-mb) for x,y in zip(ra, rb))/math.sqrt(va*vb), 2)


def family_html(hist, days=60):
    """Common-date endpoint returns; cross-market closes are not simultaneous."""
    if len(hist) < 2:
        return ""
    aligned, dates = _aligned(hist, days+1)
    if len(dates) < 21:
        return "<div style='font-size:11px;color:#64748b'>关联检验：共同日期不足21个或旧序列缺日期，暂不计算相关性。</div>"
    names = list(aligned)
    pairs = []
    for i, name in enumerate(names):
        for other in names[i+1:]:
            c = correlation(dict(zip(dates, aligned[name])), dict(zip(dates, aligned[other])))
            if c is not None:
                pairs.append((name, other, c))
    if not pairs:
        return "<div style='font-size:11px;color:#64748b'>关联检验：收益率无有效变动，无法计算相关性。</div>"
    high = any(c >= .7 for _,_,c in pairs)
    head = "🟠 同向暴露较高，需组合风险复核" if high else "🔵 样本内相关性供组合研究"
    body = " · ".join(f"{_text(a)}×{_text(b)} <b>{c:+.2f}</b>" for a,b,c in pairs)
    return (f"<div style='font-size:12px'>{head}</div><div style='font-size:11px;color:#475569'>{body}</div>"
            f"<details style='font-size:11px;color:#64748b'><summary>共同日期与解释限制</summary>{dates[0]} 至 {dates[-1]}，"
            f"{len(dates)-1}个共同端点收益区间；休市差异期间按相同端点比较。不同市场收盘时刻不同时，不能当同步因果。"
            "低相关不保证分散风险，高相关不自动要求合并或减半仓位。</details>")


def trend_svg(hist, days=60, height=180):
    series, dates = _aligned(hist, days)
    if len(dates) < 5:
        return "<div style='font-size:11px;color:#64748b'>同期走势：共同日期不足5个或旧序列缺日期，暂不绘制。</div>"
    norm = {name: [(x / values[0]-1)*100 for x in values] for name, values in series.items()}
    vals = [x for values in norm.values() for x in values]
    span = max(5, math.ceil(max(abs(min(vals)),abs(max(vals)))/5)*5)
    width, pad, ph = 620, 40, height-18
    paths, legend = [], []
    for i,(name, values) in enumerate(norm.items()):
        col = _C[i % len(_C)]
        points = " ".join(f"{pad+(width-pad-8)*j/(len(values)-1):.1f},{ph/2-value/span*(ph/2-10):.1f}" for j,value in enumerate(values))
        paths.append(f"<polyline points='{points}' fill='none' stroke='{col}' stroke-width='1.5'/>")
        legend.append(f"<span style='color:{col}'>{_text(name)} {values[-1]:+.1f}%</span>")
    grid = "".join(f"<line x1='{pad}' y1='{ph/2-k*(ph/2-10):.1f}' x2='{width-8}' y2='{ph/2-k*(ph/2-10):.1f}' stroke='#e2e8f0'/>"
                   f"<text x='0' y='{ph/2-k*(ph/2-10)+3:.1f}' font-size='9' fill='#64748b'>{k*span:+.0f}%</text>" for k in (1,0,-1))
    return (f"<div style='font-size:11px;color:#64748b'>共同起点 {dates[0]} → {dates[-1]}；{len(dates)}个共同日期，"
            "本币价格变化，未含汇率、费用及分红；复权口径以各源说明为准。</div>"
            f"<svg viewBox='0 0 {width} {height}' style='width:100%;height:auto'>{grid}{''.join(paths)}</svg>"
            f"<div style='font-size:11px;display:flex;gap:12px;flex-wrap:wrap'>{' '.join(legend)}</div>")


def comparison_records(rows, *, contexts=None, now=None):
    """Validate supplied current contexts; missing linkage fails closed.

    contexts maps code to deep_cross_validation.load_context(code). Its cached
    card is never trusted: current_scorecard is recomputed at display time.
    """
    from grade_focus import canonical, market_of, _metrics
    from review_display import current_scorecard
    from recommendation_gate import _trade_plan
    from stock_reference import reference
    from profit_contract import evaluate
    from datetime import datetime, timezone
    now = now or datetime.now(timezone.utc)
    contexts = {canonical(k): v for k,v in (contexts or {}).items()}
    counts = Counter(canonical(r.get('code')) for r in rows)
    generations = {(c.get('selection',{}).get('factpack_id'), c.get('selection',{}).get('generated_at')) for c in contexts.values() if c.get('row')}
    result = []
    for aux in rows:
        code = canonical(aux.get('code'))
        ctx = contexts.get(code) or {}
        selection, row, fact = (ctx.get(k) or {} for k in ('selection','row','fact'))
        plan = row.get('trade_plan') or {}
        errors, metrics, card, ref = [], None, {}, None
        if len(generations) > 1:
            errors.append('对比读取跨越中央发布，需整体刷新')
        if counts[code] != 1:
            errors.append('重复证券身份')
        if not row or not selection:
            errors.append('尚无中央关联')
        elif code != canonical(row.get('code')) or code != canonical(fact.get('code')):
            errors.append('中央与事实证券身份不一致')
        elif not selection.get('factpack_id') or not selection['factpack_id'] == row.get('factpack_id') == ctx.get('loaded_factpack_id'):
            errors.append('中央与冻结事实版本不一致')
        else:
            try:
                if plan != _trade_plan(fact) or row.get('horizon') != fact.get('horizon'):
                    errors.append('原合同或周期与冻结事实不一致')
                card = current_scorecard(selection, row, now)
                if row.get('tier') not in ('3A','2A','1A'):
                    errors.append('当前未获1A/2A/3A；保留原研究')
                elif not all((card.get(k) or {}).get('current') and (card.get(k) or {}).get('complete') for k in ('gpt','books')):
                    errors.append('中央审核缺失或已过期')
                elif card.get('total') != row.get('audit_score'):
                    errors.append('中央审核分与原分项不一致')
                else:
                    metrics, why = _metrics({**row, 'scorecard': card})
                    if why:
                        errors.append(why)
                pc = evaluate(plan, row.get('horizon'), now=now)
                if not pc.get('valid'):
                    errors.append('原收益合同无效或到期')
                ref = reference(selection, row)
            except (ValueError, TypeError, KeyError, AttributeError):
                errors.append('中央证据结构待核')
        current = not errors and metrics is not None
        period = plan.get('horizon') or row.get('horizon')
        sort_key = ([{'3A':0,'2A':1,'1A':2}[row['tier']], -metrics['audit_score'], -metrics['book_score'],
                     metrics['remaining_count'], -metrics['rr_band'], -metrics['space_band'], code] if current else [9,0,0,0,0,0,code])
        result.append({'code':code, 'name':row.get('name') or aux.get('name') or code,
                       'market':market_of(code), 'horizon':period, 'tier':row.get('tier') if current else None,
                       'audit_score':metrics['audit_score'] if current else None, 'current':current,
                       'errors':errors, 'trade_plan':plan, 'master_ref':ref, 'metrics':metrics,
                       'source_generated_at':selection.get('generated_at'),
                       'technical_score':aux.get('technical_score', aux.get('unified_score')), 'technical_asof':aux.get('data_asof'),
                       'sort_key':sort_key, 'no_grade_authority':True, 'no_order_authority':True})
    return sorted(result, key=lambda r: (_MARKETS.index(r['market']) if r['market'] in _MARKETS else 9,
                                         {'medium':0,'long':1,'short':2}.get(r['horizon'],9), r['sort_key']))


def _span(value):
    return ' ～ '.join(_text(v) for v in value) if isinstance(value,(list,tuple)) and len(value)==2 else '未核实'


def verdict_html(rows, *, contexts=None, now=None):
    if not rows:
        return ''
    records = comparison_records(rows, contexts=contexts, now=now)
    out = ["<div class='v88-compare-central' style='font-size:11px;color:#64748b'>"
           "中央评级与原合同对照；按市场、原周期分组，组内沿用中央等级及审核排序。"
           "量价分不参与评级排序；对比不新增买入许可，也不推荐自动赢家。</div>"]
    group = None
    for r in records:
        new_group = (r['market'],r['horizon'])
        if new_group != group:
            group = new_group
            out.append(f"<div style='font-size:12px;font-weight:600;margin-top:7px'>{_text(r['market'])} · {_H.get(r['horizon'],'原周期待核')}</div>")
        tier = r['tier'] or '未获当前评级'
        col = {'3A':'#15803d','2A':'#b45309','1A':'#2563eb'}.get(r['tier'],'#64748b')
        score = f" · 审核分{r['audit_score']:g}/100" if r['current'] else ''
        plan = r['trade_plan']
        pc = plan.get('profit_contract') or {}
        deadline = pc.get('thesis_deadline') or '未核实'
        contract_title = '中央原研究合同' if r['current'] else '原合同留档·不可据此执行'
        aux = (f"量价辅助分 {_text(r['technical_score'])}/100；仅作同量纲研究，不是审核分或已标定概率。"
               if _number(r['technical_score']) else '量价辅助数据待核。')
        if r['technical_asof']:
            aux += ' 行情日期 '+_text(r['technical_asof'])+'。'
        reason = '；'.join(r['errors']) or '评级与原合同一致；1A/2A仅研究，3A是否可执行须看中央当次执行闸。'
        out.append(f"<div style='border-left:3px solid {col};background:#f8fafc;padding:6px 9px;margin:4px 0;border-radius:4px'>"
                   f"<a href='?q={quote(r['code'],safe='')}&amp;focus=deep#v88-deep-analysis' target='_blank' rel='noopener' style='font-size:12px;color:{col}'>{_text(r['name'])}</a> "
                   f"<span style='font-size:11px'>{_text(r['code'])} · <b style='color:{col}'>{tier}{score}</b></span>"
                   f"<div style='font-size:11px;color:#475569'>{_text(reason)}</div>"
                   f"<details style='font-size:11px;color:#64748b'><summary>{contract_title} / 量价辅助</summary>"
                   f"原进场 {_span(plan.get('entry_range'))}；原止盈 {_span(plan.get('take_profit_range'))}；原失效 {_text(plan.get('stop'))}；截止 {_text(deadline)}。"
                   f"<br>原触发：{_text(plan.get('promotion_trigger'))}<br>原失效依据：{_text(plan.get('invalidation'))}"
                   f"<br>{aux}<br>中央发布 {_text(r['source_generated_at'])}；引用 {_text((r['master_ref'] or {}).get('reference_id','未关联'))}</details></div>")
    return ''.join(out)
