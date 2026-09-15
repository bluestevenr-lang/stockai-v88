"""Read-only reconciliation of signed research and deterministic price analysis.

Scores measure different things. Never average them, issue a new grade, move a
committed stop, or count shared OHLC calculations as independent evidence.
"""
from datetime import datetime, timezone, timedelta
from html import escape
import hashlib
import json
import math
from pathlib import Path
from collections import OrderedDict
from copy import deepcopy
from threading import RLock

from modules.utils import to_yf_cn_code

VERSION = 'deep-cross-validation-v1'
# Frozen numeric facts are rounded to four decimals. This tolerance is only
# for representation agreement, never for grade/profit hurdle comparisons.
FACT_ROUNDING_TOLERANCE = .000051
BJT = timezone(timedelta(hours=8))
HORIZONS = {'short': '短期（1–30天）', 'medium': '中期（31–90天）', 'long': '长期（91–365天）'}
_SNAPSHOTS = OrderedDict()
_SNAPSHOT_LOCK = RLock()
_SELECTION_GROUPS = ('recommendations', 'preparations', 'blocked_3a', 'conditional',
                     'pending', 'observations', 'excluded')


def _file_version(path):
    stat = path.stat()
    return (stat.st_dev, stat.st_ino, stat.st_mtime_ns, stat.st_ctime_ns, stat.st_size)


def read_json_snapshot(path):
    """Borrow immutable source data; callers must copy any returned mutable view.

    Retain one generation per absolute path, never an old good file after a
    missing/broken replacement. Current review/freshness decisions are not cached.
    """
    path = Path(path).resolve()
    with _SNAPSHOT_LOCK:
        try:
            for _ in range(2):
                before = _file_version(path)
                hit = _SNAPSHOTS.get(path)
                if hit and hit[0] == before:
                    _SNAPSHOTS.move_to_end(path)
                    return hit[1]
                doc = json.loads(path.read_text(encoding='utf-8'))
                if _file_version(path) != before:
                    continue
                _SNAPSHOTS[path] = (before, doc)
                _SNAPSHOTS.move_to_end(path)
                while len(_SNAPSHOTS) > 12:
                    _SNAPSHOTS.popitem(last=False)
                return doc
            raise OSError('Snapshot changed while being read')
        except (OSError, ValueError):
            _SNAPSHOTS.pop(path, None)
            raise


def _stock_selection(doc, code):
    """Detached single-stock input to the unchanged central validation helper."""
    from market_symbols import canonical
    target = canonical(code)
    result = {key: deepcopy(doc.get(key)) for key in
              ('version', 'generated_at', 'factpack_id', 'factpack_fresh')}
    for group in _SELECTION_GROUPS:
        result[group] = [deepcopy(row) for row in doc.get(group) or []
                         if canonical(row.get('code')) == target]
    return result


def load_context(code, data_dir=None):
    from market_data_helper import _core, CORE
    _core()
    from stock_verdict import _triad_record
    from review_display import current_scorecard
    root = Path(data_dir or CORE/'data')
    # A publication may change between reads. Retry the entire pair, not one leg.
    for _ in range(2):
        try:
            selection_doc = read_json_snapshot(root/'triad_selection.json')
            pack = read_json_snapshot(root/'review_factpack.json')
        except (OSError, ValueError):
            selection_doc, pack = {}, {}
        selection, row, formal = _triad_record(code, selection=_stock_selection(selection_doc, code))
        if pack.get('factpack_id') == selection.get('factpack_id'):
            break
    fact = next((r for r in pack.get('items', [])
                 if to_yf_cn_code(r.get('code', '')) == to_yf_cn_code(code)), {})
    paired = bool(pack.get('factpack_id') and pack.get('factpack_id') == selection.get('factpack_id'))
    return {'code': to_yf_cn_code(code), 'selection': selection, 'row': row,
            'formal': bool(formal and paired),
            'card': current_scorecard(selection if paired else {}, row) if row else {},
            'fact': deepcopy(fact), 'loaded_factpack_id': pack.get('factpack_id')}


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def near(a, b, tolerance=1e-7):
    return finite(a) and finite(b) and math.isclose(a, b, abs_tol=tolerance, rel_tol=1e-8)


def basis(value):
    # Only explicitly declared equivalent price bases may be compared.
    text = str(value or '').lower()
    if text.startswith('split-adjusted ohlc') or text.startswith('yahoo ohlc拆股口径'):
        return 'split-adjusted OHLC; not total return'
    if text in ('前复权', 'qfq', 'forward-adjusted'):
        return 'forward-adjusted'
    if text in ('不复权', 'none', 'unadjusted'):
        return 'unadjusted'
    if text == 'sina cn original ohlc divided by source qfq factor; native unadjusted share volume; not total return':
        return 'sina-source-qfq'
    if text == '腾讯前复权ohlc整段序列；不是含息总回报，不能与yahoo拆股口径逐行拼接':
        return 'tencent-source-qfq'
    if text == '腾讯原始ohlc整段；新浪明确完整前复权因子在所载日期均为1且偏移0，价格无修改；不是含息总回报':
        # The ingestion adapter emits this exact basis only after checking the
        # complete source adjustment factors. Keep it distinct from generic
        # unadjusted or other provider series; never infer factor=1 here.
        return 'tencent-source-identity-factors-qfq'
    return None


def scenario(entry, target, stop, buy_pct, sell_pct):
    """Reprice the same fixed contract at an alternate entry, without adopting it."""
    if not all(finite(x) for x in (entry, target, stop, buy_pct, sell_pct)):
        return None
    if min(entry, target, stop) <= 0 or not 0 <= buy_pct < 100 or not 0 <= sell_pct < 100:
        return None
    purchase = entry*(1+buy_pct/100)
    proceeds, exit_proceeds = target*(1-sell_pct/100), stop*(1-sell_pct/100)
    risk = purchase-exit_proceeds
    if entry <= stop or risk <= 0:
        return None
    return {'entry': entry, 'target': target, 'stop': stop,
            'net_upside_pct': (proceeds/purchase-1)*100,
            'net_reward_risk': (proceeds-purchase)/risk,
            'net_risk_pct': risk/purchase*100}


def reconcile(context, frame, quality, technical, now=None):
    from market_data_helper import _core
    _core()
    from evidence_freshness import source_times_fresh
    from profit_contract import evaluate, THRESHOLDS
    from recommendation_gate import _trade_plan
    from deep_analysis_data import snapshot_signature
    now = now or datetime.now(BJT)
    row, fact, card = (context.get(k) or {} for k in ('row', 'fact', 'card'))
    selection = context.get('selection') or {}
    plan = row.get('trade_plan') or {}
    saved = plan.get('profit_contract') or {}
    checks = []

    def add(key, title, status, detail, next_action=''):
        checks.append({'id': key, 'title': title, 'status': status, 'detail': detail,
                       'next_action': next_action if status != 'pass' else ''})

    identity = bool(context.get('code') and fact and row and quality.get('code')
                    and all(to_yf_cn_code(x or '') == to_yf_cn_code(context['code']) for x in
                            (row.get('code'), fact.get('code'), quality.get('code'), technical.get('code'))))
    add('identity', '证券身份', 'pass' if identity else 'gap',
        f"中央 {row.get('code')} / 行情 {quality.get('code')} / 技术 {technical.get('code')}",
        '修复代码、市场映射；不能拿其它证券的审核解释本票。')
    bound = bool(fact and selection.get('factpack_id')
                 == context.get('loaded_factpack_id') == row.get('factpack_id')
                 and row.get('horizon') == fact.get('horizon') and plan == _trade_plan(fact))
    add('binding', '同包合同与周期', 'pass' if bound else 'gap',
        f"事实包 {str(selection.get('factpack_id') or '缺失')[:12]}；{HORIZONS.get(row.get('horizon'), '周期缺失')}",
        '重新读取同一发布批次；合同变化须冻结新事实并实际双审，不能沿用旧分数。')
    review_ok = bool(row.get('tier') in ('0A', '1A', '2A', '3A')
                     and all((card.get(k) or {}).get('current') and (card.get(k) or {}).get('complete')
                             for k in ('gpt', 'books'))
                     and finite(card.get('total')) and card['total'] == row.get('audit_score'))
    add('reviews', '中央评级与审核分', 'pass' if review_ok else 'gap',
        f"中央 {row.get('tier', '无当前评级')} / 原审核分 {row.get('audit_score')} / 当前重算 {card.get('total')}",
        '补齐或更新实际GPT主审、反审和适用书理；技术分不能补签。')
    pc = evaluate(plan, row.get('horizon'), now=now)
    contract_ok = bool(bound and pc.get('valid') and saved.get('valid')
                       and all(near(pc.get(k), saved.get(k)) for k in ('net_upside_pct', 'net_reward_risk'))
                       and pc.get('thesis_deadline') == saved.get('thesis_deadline')
                       and pc.get('take_profit_range') == plan.get('take_profit_range'))
    add('contract', '原价位、期限与扣费重算', 'pass' if contract_ok else 'gap',
        '原进场上沿→原止盈下沿，买卖各0.5%；失效价和截止日不滚动。' if contract_ok else '；'.join(pc.get('reasons') or ['原合同数值或绑定不一致']),
        '按原始目标证据修复或到期重审；不得抬目标、放宽止损或续期。')
    usable = frame is not None and not frame.empty and 'Close' in frame
    last = float(frame.Close.iloc[-1]) if usable else None
    day = str(frame.index[-1])[:10] if usable else ''
    provenance = fact.get('data_provenance') or {}
    fact_day = provenance.get('daily_date') or provenance.get('history_last_date')
    fresh = bool(usable and day == str(quality.get('source_asof'))[:10] == fact_day
                 == str(technical.get('data_asof'))[:10] and source_times_fresh(fact, now))
    add('session', '完整交易日与时效', 'pass' if fresh else 'gap',
        f"中央 {fact_day or '缺失'} / 深度 {day or '缺失'} / 技术 {technical.get('data_asof') or '缺失'}",
        '补充最新完整交易日并重建事实；页面刷新时间不能当行情时间。')
    fb = (fact.get('conditional_path_evidence') or {}).get('price_basis') or provenance.get('price_basis')
    basis_ok = bool(basis(fb) and basis(fb) == basis(quality.get('price_basis')))
    add('basis', '价格与复权口径', 'pass' if basis_ok else 'gap',
        f"中央 {fb or '未声明'} / 深度 {quality.get('price_basis') or '未声明'}",
        '核对复权、币种及原始价格，未知口径不做价位一致性认证。')
    measured = {}
    if usable and len(frame) >= 61 and all(k in frame for k in ('Low', 'Volume')):
        measured = {'ma20': float(frame.Close.tail(20).mean()), 'ma60': float(frame.Close.tail(60).mean()),
                    'observed_low10': float(frame.Low.tail(10).min()),
                    'return20_pct': float((frame.Close.iloc[-1]/frame.Close.iloc[-21]-1)*100)}
        prior_vol = float(frame.Volume.iloc[-21:-1].mean())
        if prior_vol > 0:
            measured['volume_ratio20'] = float(frame.Volume.iloc[-1]/prior_vol)
    evidence = fact.get('market_evidence') or {}
    comparisons = [{'field': key, 'central': evidence.get(key), 'deep': value,
                    'matches': near(value, evidence.get(key), FACT_ROUNDING_TOLERANCE)} for key, value in measured.items()
                   if finite(evidence.get(key))]
    prices_ok = bool(identity and fresh and basis_ok and near(last, fact.get('last'), FACT_ROUNDING_TOLERANCE)
                     and near(last, technical.get('last'), FACT_ROUNDING_TOLERANCE) and comparisons and all(r['matches'] for r in comparisons))
    add('facts', '价格与公共指标重算', 'pass' if prices_ok else 'gap',
        f"中央收盘 {fact.get('last')} / 深度收盘 {num(last)}；{sum(r['matches'] for r in comparisons)}/{len(comparisons)} 项指标一致；按冻结事实四位小数核对",
        '逐项检查原始日线、历史修订和指标窗口；不平均冲突的价格或评分。')
    fields = {'short': 'short_score', 'medium': 'medium_score', 'long': 'long_score',
              'trend_quality': 'trend_quality_score', 'entry_odds': 'entry_odds_score'}
    weights = technical.get('score_weights') or {}
    # The engine rounds component displays. Recompute only from its exact terms.
    terms = technical.get('score_terms') or {}
    quant_ok = bool(set(terms) == set(fields) == set(weights)
                    and all(finite(terms[k]) and finite(weights[k]) for k in fields)
                    and near(sum(weights.values()), 1)
                    and round(sum(terms[k]*weights[k] for k in fields)) == technical.get('unified_score')
                    and quality.get('snapshot_signature') == snapshot_signature(frame))
    add('quant', '量价分计算与快照', 'pass' if quant_ok else 'gap',
        f"量价 {technical.get('unified_score')}；短20%＋中25%＋长20%＋趋势15%＋赔率20%；"
        f"精确分项 {len(terms)}/5；行情签名{'一致' if quality.get('snapshot_signature') == snapshot_signature(frame) else '不一致'}",
        '从同一完整日线重算量价分；检查缓存与模型版本，不能补零或拿审核分替代。')
    if usable and finite(plan.get('stop')):
        anchor = str((plan.get('profit_inputs') or {}).get('asof') or '')[:10]
        closes = frame.loc[[str(x)[:10] > anchor for x in frame.index], 'Close'] if anchor else frame.Close.iloc[:0]
        breached = [str(x)[:10] for x in closes[closes <= plan['stop']].index]
    else:
        breached = []
    add('invalidation', '原失效线反查', 'gap' if breached else 'pass' if usable and contract_ok else 'gap',
        '收盘触线：'+', '.join(breached) if breached else '在所读取的原锚点之后日线中未见收盘触及原失效线。',
        '先处理原合同风险并重新审核；滚动技术支撑不能复活失效方案。')
    current_scenario = scenario(last, pc['take_profit_range'][0], plan.get('stop'),
                                pc['buy_cost_pct'], pc['sell_cost_pct']) if contract_ok and prices_ok else None
    entry = plan.get('entry_range') or []
    in_zone = bool(prices_ok and len(entry) == 2 and entry[0] <= last <= entry[1])
    add('entry', '当前价格与原入场区间', 'watch' if not in_zone else 'pass',
        f"收盘 {last}；原区间 {entry}；" + ('价格在区间内，仍须原量价触发和执行闸。' if in_zone else '未在原区间，不把原计划空间当现价收益。'),
        '等待原价格/量能条件；需要新入场方案时以新事实双审，保留旧合同。')
    grade = row.get('tier') if review_ok else None
    hurdle = THRESHOLDS.get(row.get('horizon'), {}).get(grade)
    current_hurdles = bool(current_scenario and hurdle is not None
                           and current_scenario['net_upside_pct'] >= hurdle
                           and current_scenario['net_reward_risk'] >= (2 if grade == '3A' else 1.5))
    gaps = [x for x in checks if x['status'] == 'gap']
    status = '需复核' if gaps else '一致·审核完成未入选' if grade == '0A' else '一致·等原条件' if not context.get('formal') or not in_zone else '一致·仍按中央执行闸'
    result = {'version': VERSION, 'code': context.get('code'), 'checked_at': now.isoformat(),
              'status': status, 'current_grade': grade, 'audit_score': card.get('total') if review_ok else None,
              'technical_score': technical.get('unified_score'), 'score_version': technical.get('score_version'),
              'horizon': row.get('horizon'), 'checks': checks, 'indicator_comparisons': comparisons,
              'original_plan': {k: plan.get(k) for k in ('entry_range', 'take_profit_range', 'stop')},
              'original_net_space': pc.get('net_upside_pct') if contract_ok else None,
              'original_net_rr': pc.get('net_reward_risk') if contract_ok else None,
              'deadline': saved.get('thesis_deadline'), 'current_price': last,
              'current_price_scenario': current_scenario, 'current_price_meets_numeric_hurdles': current_hurdles,
              'technical_reference': {k: technical.get(k) for k in ('stop', 'resistance', 'rr', 'action', 'horizon')},
              'technical_breakdown': {'terms': terms, 'weights': weights},
              'factpack_id': selection.get('factpack_id'), 'audit_id': row.get('audit_id'),
              'snapshot_signature': quality.get('snapshot_signature'), 'source_asof': day,
              'next_actions': list(dict.fromkeys(x['next_action'] for x in checks if x['next_action'])),
              'no_grade_authority': True, 'independent_data_validation': False, 'model_calls': 0,
              'predictive_win_probability': None}
    result['input_id'] = hashlib.sha256(json.dumps({k: result[k] for k in result if k != 'checked_at'},
                                                  sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return result


def num(value):
    return f'{value:.4f}'.rstrip('0').rstrip('.') if finite(value) else '未核实'


def span(value):
    return ' ～ '.join(num(x) for x in value) if isinstance(value, (list, tuple)) and len(value) == 2 else '未核实'


def html(report):
    esc = lambda v: escape(str(v if v is not None else '未核实'))
    original = report['original_plan']
    current = report.get('current_price_scenario') or {}
    technical = report['technical_reference']
    fmt = lambda v: f'{v:.2f}' if finite(v) else '未核实'
    rows = [
        ('中央审核证据分', f"{num(report['audit_score'])}/100", HORIZONS.get(report['horizon'], '未核实'), 'GPT主审/反审＋适用书理，决定评级'),
        ('量价辅助分', f"{num(report['technical_score'])}/100", '2/4/8/16/32周混合', '量价趋势与赔率，仅用于研究，不授级'),
        ('原审核合同', f"入场 {span(original.get('entry_range'))}", f"止盈 {span(original.get('take_profit_range'))} / 失效 {num(original.get('stop'))}",
         f"净空间 {fmt(report['original_net_space'])}% / 净RR {fmt(report['original_net_rr'])}"),
        ('现价代入同一合同', f"收盘 {num(report['current_price'])}", '原目标、原失效价及同一费用假设',
         f"净空间 {fmt(current.get('net_upside_pct'))}% / 净RR {fmt(current.get('net_reward_risk'))}；不构成新入场方案"),
        ('滚动技术参考', f"支撑 {num(technical.get('stop'))} / 压力 {num(technical.get('resistance'))}", '最新技术窗口；不替换原失效价/止盈',
         f"现价毛RR {fmt(technical.get('rr'))}；未扣费用、未验证兑现路径"),
    ]
    table = '<table style="font-size:12px;width:100%;min-width:840px;border-collapse:collapse"><thead><tr>'+''.join('<th>'+x+'</th>' for x in ('项目', '数值', '周期/价位口径', '用途与核验'))+'</tr></thead><tbody>'
    table += ''.join('<tr>'+''.join('<td style="padding:5px;border:1px solid #dce3ed">'+esc(v)+'</td>' for v in r)+'</tr>' for r in rows)+'</tbody></table>'
    details = '<p><b>逐项核验与整改条件</b></p><table style="font-size:12px;min-width:840px">'
    for c in report['checks']:
        details += '<tr>'+''.join('<td style="padding:5px;border:1px solid #dce3ed">'+esc(v)+'</td>' for v in
                                  (c['title'], {'pass':'一致','gap':'需复核','watch':'等条件'}[c['status']], c['detail'], c['next_action']))+'</tr>'
    details += '</table><details><summary>指标原值与计算口径</summary><p>'+esc('公共指标对照：'+json.dumps(report['indicator_comparisons'], ensure_ascii=False))+'</p>'
    details += ('<p>书理分工：斯波朗迪《专业投机原理》核趋势层级与资本保护；萨普《通向财务自由之路》核初始R、仓位与扣费期望；'
                '欧奈尔核量价触发；中长期另按既有模板核盈利、现金流及估值。不同书解释同一价格不算新增独立样本。</p></details>')
    gaps=[c for c in report['checks'] if c['status']=='gap']
    gap_html=('<div class="v88-deep-cross-gaps" style="color:#92400e">待核实 '+str(len(gaps))+' 项：'
              +esc('；'.join(c['title'] for c in gaps))+'</div>') if gaps else ''
    return ('<section class="v88-deep-cross" style="font-size:12px;margin:8px 0;padding:10px;border:1px solid #cbd5e1;border-radius:6px">'
            f'<b>3A ↔ 深度分析交叉核验：{esc(report["status"])}</b> · 当前评级 {esc(report["current_grade"] or "无有效评级")}'
            +gap_html+f'<div style="font-size:11px;color:#64748b">行情日 {esc(report["source_asof"])}</div>'
            '<details class="v88-deep-cross-detail"><summary>查看数值对照与核验明细</summary>'
            '<div>两种分数衡量不同问题，不要求同分、不平均。同一日线的重算属于一致性检查，不是新增独立数据源或第三份GPT审核。</div>'
            '<div style="overflow-x:auto">'+table+details+'</div>'
            '<div>量价支持不能自动升级；量价矛盾进入复核，已承诺失效线优先保护。未来概率须经样本外结算标定。</div>'
            f'<div>核验编号 {esc(report["input_id"][:12])} · 本次模型调用 0</div></details></section>')
