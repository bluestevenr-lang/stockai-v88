"""Completed, validated whole-series data for the stock detail page."""
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import pandas as pd
from modules.utils import to_yf_cn_code


def snapshot_signature(frame):
    if frame is None or frame.empty:
        return 'missing'
    payload = frame[['Open', 'High', 'Low', 'Close', 'Volume']].to_csv()
    payload += json.dumps({k: frame.attrs.get(k) for k in ('source', 'source_asof', 'price_basis')}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


def revalidate_cached(result, code):
    """A session's explicit history fetch cannot extend its source validity."""
    from market_data_helper import validate
    frame, quality = result
    try:
        frame = validate(frame, code, source=frame.attrs.get('source') or '免费完整日线')
        if len(frame) < 61:
            raise ValueError('深度分析完整日线不足61根')
    except (ValueError, TypeError, AttributeError) as exc:
        return None, {**quality, 'source': '缓存日线已失效', 'data_points': 0,
                      'error_detail': str(exc), 'network_requested': False}
    return frame, {**quality, 'source_asof': frame.attrs.get('source_asof'),
                   'data_points': len(frame), 'snapshot_signature': snapshot_signature(frame)}


def fetch(code, *, allow_network=True):
    """Prefer the full-market validated store; never append an intraday quote."""
    from market_data_helper import _core, validate, fetch_df
    _core()
    from verified_history import read
    from market_symbols import canonical
    code = to_yf_cn_code(str(code))
    failures = []
    try:
        frame = read(canonical(code), (datetime.now()-timedelta(days=1100)).date().isoformat(),
                     datetime.now().date().isoformat())
        if frame is None or frame.empty:
            if (getattr(frame, 'attrs', {}) or {}).get('quality_error'):
                return None, {'source': '日线已隔离', 'error_detail': frame.attrs['quality_error'], 'data_points': 0}
            raise ValueError((getattr(frame, 'attrs', {}) or {}).get('quality_error') or '未入核验日线库')
        frame = frame.rename(columns={k: k.title() for k in frame.columns})
        frame = validate(frame, code, source=frame.attrs.get('provider_source') or '核验日线')
        if len(frame) < 61:
            raise ValueError('深度分析完整日线不足61根')
    except Exception as exc:
        failures.append(str(exc)[:180])
        try:
            from portable_history import read as read_portable
            from market_data_helper import CORE
            frame = read_portable(code, (datetime.now()-timedelta(days=1100)).date().isoformat(),
                                  datetime.now().date().isoformat(), path=CORE/'data/portable_history_pub.json')
            frame = frame.rename(columns={k:k.title() for k in frame.columns})
            frame = validate(frame, code, source=frame.attrs.get('provider_source') or '同步核验日线')
            if len(frame) < 61: raise ValueError('同步完整日线不足61根')
        except Exception as mirror_error:
            failures.append(str(mirror_error)[:180])
            if not allow_network:
                return None, {'source': '本地及同步日线暂缺', 'error_detail': '；'.join(failures),
                              'data_points': 0, 'network_requested': False}
            try:
                frame = fetch_df(code, period='2y', timeout=8)
            except Exception as fallback_error:
                failures.append(type(fallback_error).__name__)
                frame = None
    if frame is None or len(frame) < 61:
        return None, {'source': '暂无合格日线', 'error_detail': '；'.join(failures), 'data_points': 0}
    # Revalidate any adapter result rather than trusting a cache timestamp.
    try:
        frame = validate(frame, code, source=frame.attrs.get('source') or '免费完整日线')
    except ValueError as exc:
        return None, {'source': '日线校验失败', 'error_detail': str(exc), 'data_points': 0}
    quality = {'code': code, 'source': frame.attrs.get('source'), 'source_asof': frame.attrs.get('source_asof'),
               'price_basis': frame.attrs.get('price_basis') or '来源未声明', 'is_delayed': True,
               'data_points': len(frame), 'date_range': f'{frame.index[0].date()} 至 {frame.index[-1].date()}',
               'snapshot_signature': snapshot_signature(frame), 'fallback_reasons': failures,
               'is_realtime': False, 'model_calls': 0}
    return frame, quality


def report_html(code, data_dir=None, context=None):
    """Show central grades/contracts before any optional indicator calculation."""
    from html import escape
    from market_data_helper import _core, CORE
    _core()
    from market_symbols import canonical
    from stock_verdict import _triad_record
    from review_display import current_scorecard
    from scorecard_html import gpt_html, books_html
    root = Path(data_dir or CORE/'data')
    selection, row, formal = ((context['selection'], context['row'], context['formal'])
                             if context is not None else _triad_record(code))
    esc = lambda x: escape(str(x if x is not None else '—'))
    price = lambda x: f'{x:.4f}'.rstrip('0').rstrip('.') if isinstance(x, (int,float)) else '未核实'
    span = lambda a: ' ～ '.join(price(x) for x in a) if isinstance(a,list) and len(a)==2 else '未核实'
    card = context['card'] if context is not None else current_scorecard(selection, row) if row else {}
    plan = row.get('trade_plan') or {}
    pc = plan.get('profit_contract') or row.get('profit_contract') or {}
    current = (row.get('tier') in {'1A','2A','3A'} and card.get('total') is not None
               and all((card.get(k) or {}).get('current') and (card.get(k) or {}).get('complete') for k in ('gpt','books')))
    from stock_profile_view import html as profile_html, display_name as profile_name, load as profile_load
    profiles = profile_load(root/'stock_profiles_pub.json')
    content = (f'<b>{esc(profile_name(row.get("name"), code, profiles))} · {esc(code)}</b>'
               + profile_html(code, profiles)
               + f'<b>中央评级 {esc(row.get("tier")) if current else "待重新核验"} · '
               f'审核分 {esc(card.get("total")) if current else "未形成当前分数"}</b> · '
               + ('按中央合同核查执行条件' if formal else '研究观察·不可直接执行'))
    content += (f'<div>入场 {span(plan.get("entry_range"))} ｜ 止盈 {span(plan.get("take_profit_range"))}'
                f' ｜ 失效价 {price(plan.get("stop"))}</div>'
                f'<div>原合同净空间 {esc(round(pc["net_upside_pct"],2)) if isinstance(pc.get("net_upside_pct"),(int,float)) else "未核实"}%'
                f' ｜ 净收益风险比 {esc(round(pc["net_reward_risk"],2)) if isinstance(pc.get("net_reward_risk"),(int,float)) else "未核实"}'
                f'<details><summary>目标依据与费用假设</summary>{esc(pc.get("target_basis"))}<br>{esc(pc.get("cost_assumption"))}</details></div>'
                f'<div>原合同截止 {esc(pc.get("thesis_deadline") or plan.get("thesis_deadline"))}'
                f' ｜ 原持有期 {esc(pc.get("holding_sessions"))} 交易日；不是周内收益承诺。</div>'
                f'<details><summary>入场与失效条件</summary>{esc(plan.get("promotion_trigger") or plan.get("entry_trigger"))}'
                f'<p>{esc(plan.get("invalidation"))}</p></details>'
                '<details><summary>GPT 双审与经典巨著逐项证据</summary>'+gpt_html(card)+books_html(card)+'</details>')
    from entry_opportunity import assess as entry_assess, html as entry_html
    content += entry_html(entry_assess({**row, 'scorecard': card, 'formal_recommendation': formal}))
    try:
        weekly = json.loads((root/'weekly_candidates_pub.json').read_text(encoding='utf-8'))
        item = next((r for r in weekly.get('rows',[]) if canonical(r.get('code'))==canonical(code)), None)
        if item:
            from weekly_candidates_ui import html
            subset = {**weekly, 'rows':[item], 'market_slots':{}, 'sector_rotation':{}, 'context_sources':{},
                      'low_recovery_candidates':{}, 'rotation_candidates':{}, 'funnel':{}, 'events':[]}
            content += ('<details class="v88-deep-weekly" style="font-size:11px;margin:5px 0">'
                        '<summary>周度观察记录 · 短期原合同（展开）</summary>'
                        +html(subset, selection, detail=True)+'</details>')
    except (OSError, ValueError, TypeError):
        content += '<div>周度证据暂未读取成功，中央记录仍保留。</div>'
    content += '<div>下方量价分与情景估计用于辅助研究；不会覆盖中央评级，也不是实测胜率。打开页面复用已有审核，不自动调用模型。</div>'
    from module_relations_ui import html as relations_html
    try:
        relations=json.loads((root/'module_relations_pub.json').read_text(encoding='utf-8'))
    except (OSError,ValueError):
        relations={}
    content += relations_html(relations,selection,code)
    from evolution_learning_ui import read as evolution_read, stock_html as evolution_html, health as evolution_health
    evolution = evolution_read(root)
    try:
        evolution_status=json.loads((root/'evolution_learning_status.json').read_text(encoding='utf-8'))
    except (OSError,ValueError):
        evolution_status={}
    content += '<div class="v88-evolution-health">'+esc(evolution_health(evolution,evolution_status,selection))+' · '+esc(evolution.get('generated_at'))+'</div>'
    content += evolution_html((evolution.get('stocks') or {}).get(canonical(code)), compact=True)
    from persistent_watchlist_ui import read as watch_read, html as watch_html
    content += watch_html(watch_read(root), selection, code=code)
    return '<section class="v88-deep-contract" style="font-size:12px;padding:10px;border:1px solid #cbd5e1;border-radius:6px">'+content+'</section>'
