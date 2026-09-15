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



def observation_html(code, data_dir, *, now=None):
    """Read one stock's existing observation evidence, with the same scoring UI."""
    from datetime import datetime, timezone
    from html import escape
    from market_data_helper import _core
    _core()
    from market_symbols import canonical
    from observation_score import assess, details_html
    from deep_cross_validation import read_json_snapshot
    now = now or datetime.now(timezone.utc)
    root = Path(data_dir)
    candidates = []
    for filename, keys in (("market_watch_pub.json", ("rows",)),
                           ("market_adaptation_pub.json", ("left_entry_watch", "monthly_monitor"))):
        try:
            doc = read_json_snapshot(root / filename)
            at = datetime.fromisoformat(str(doc.get("checked_at") or doc["generated_at"]).replace("Z", "+00:00"))
            if at.tzinfo is None or not 0 <= (now - at).total_seconds() <= 43200:
                continue
            groups = [doc.get("rows") or []] if keys == ("rows",) else [
                (doc.get(key) or {}).get("all_rows") or (doc.get(key) or {}).get("rows") or [] for key in keys]
            for rows in groups:
                for row in rows:
                    if canonical(row.get("code")) == canonical(code):
                        score = assess(row, now=now)
                        candidates.append((score["coverage_pct"], filename, row, score))
        except (OSError, ValueError, KeyError, TypeError):
            continue
    if not candidates:
        return ""
    # On equal coverage the shared market-watch view precedes risk-monitor rows.
    _, source, row, score = max(candidates, key=lambda candidate: candidate[0])
    return ("<section class='v88-deep-observation-score' style='margin:10px 0;padding:12px;background:#f8fafc;border:1px solid #cbd5e1;border-radius:8px'>"
            "<div style='font-weight:700'>🔭 观察证据评分</div>" + details_html(row, now=now)
            + "<small>与视角/风险观察共用分项；中央双审决定审核分与评级。来源："
            + escape("市场视角" if source == "market_watch_pub.json" else "风险观察") + "</small></section>")

def report_html(code, data_dir=None, context=None):
    """Show central grades/contracts before any optional indicator calculation."""
    from html import escape
    from math import isfinite
    from market_data_helper import _core, CORE
    _core()
    from market_symbols import canonical
    from stock_verdict import _triad_record
    from review_display import current_scorecard
    from scorecard_html import gpt_html, books_html, policy_html
    from deep_cross_validation import read_json_snapshot
    from nontechnical_reason_ui import load_index, for_code, html as business_html, action_html
    root = Path(data_dir or CORE/'data')
    selection, row, formal = ((context['selection'], context['row'], context['formal'])
                             if context is not None else _triad_record(code))
    esc = lambda x: escape(str(x if x is not None else '—'))
    number = lambda x: type(x) in (int, float) and isfinite(x)
    price = lambda x: f'{x:.4f}'.rstrip('0').rstrip('.') if number(x) and x > 0 else '未核实'
    span = lambda a: ' ～ '.join(price(x) for x in a) if isinstance(a,list) and len(a)==2 else '未核实'
    card = context['card'] if context is not None else current_scorecard(selection, row) if row else {}
    business=for_code(code,load_index(root.parent),row)
    plan = row.get('trade_plan') or {}
    pc = plan.get('profit_contract') or row.get('profit_contract') or {}
    current = (card.get('total') is not None
               and all((card.get(k) or {}).get('current') and (card.get(k) or {}).get('complete') for k in ('gpt','books')))
    from stock_profile_view import html as profile_html, display_name as profile_name, load as profile_load
    profiles = profile_load(root/'stock_profiles_pub.json')
    content = (f'<b>{esc(profile_name(row.get("name"), code, profiles))} · {esc(code)}</b>'
               + profile_html(code, profiles)
               + f'<b>中央评级 {esc(row.get("tier")) if current else "待重新核验"} · '
               f'加权审核分 {esc(card.get("total")) if current else "未形成当前分数"}</b> · '
               + ('按中央合同核查执行条件' if formal else '研究观察·不可直接执行'))
    # Keep the list's recorded central score visible even when execution is frozen.
    # A stale score is labelled, never promoted into a current recommendation.
    listed_score = row.get('audit_score')
    has_score = type(listed_score) in (int, float) and isfinite(listed_score) and 0 <= listed_score <= 100
    reviewing = context is not None and context.get('review_status') in ('queued', 'running')
    score_text = f'{listed_score:g} / 100' if has_score else ('正在分析…' if reviewing else '暂无评分')
    score_note = ('与3A系统列表同源' if current and listed_score == card.get('total')
                  else '原审核分 · 待重新核验，不代表当前可买入') if has_score else ('正在运行3A审核，完成后自动显示分数' if reviewing else '3A系统尚未给出该股审核分')
    if reviewing and context.get('review_progress'):
        progress = context['review_progress']
        if not has_score:
            score_text = f"分析进度 {progress['percent']}%"
        score_note = progress['label'] + ' · 完成后自动显示中央评分'
    content += ("<section class='v88-deep-3a-score' style='margin:12px 0;padding:12px;background:#eff6ff;border:1px solid #bfdbfe;border-radius:6px'>"
                "<div style='font-weight:700'>⚖️ 3A复合审核评分</div>"
                f"<div style='font-size:28px;font-weight:800;color:#1d4ed8'>{esc(score_text)}</div>"
                f'<div>{esc(score_note)}</div>'
                + ('<details><summary>权重与分项</summary>'+policy_html(card)+'</details>' if card.get('score_policy') else '') + '</section>')
    content += observation_html(code, root)
    content += (f'<div>入场 {span(plan.get("entry_range"))} ｜ 止盈 {span(plan.get("take_profit_range"))}'
                f' ｜ 失效价 {price(plan.get("stop"))}</div>'
                f'<div>原合同净空间 {esc(round(pc["net_upside_pct"],2)) + "%" if number(pc.get("net_upside_pct")) else "未核实"}'
                f' ｜ 净收益风险比 {esc(round(pc["net_reward_risk"],2)) if number(pc.get("net_reward_risk")) else "未核实"}'
                f'<details><summary>目标依据与费用假设</summary>{esc(pc.get("target_basis"))}<br>{esc(pc.get("cost_assumption"))}</details></div>'
                f'<div>原合同截止 {esc(pc.get("thesis_deadline") or plan.get("thesis_deadline"))}'
                f' ｜ 原持有期 {esc(pc.get("holding_sessions"))} 交易日；不是周内收益承诺。</div>'
                f'<details><summary>入场与失效条件</summary>{esc(plan.get("promotion_trigger") or plan.get("entry_trigger"))}'
                f'<p>{esc(plan.get("invalidation"))}</p></details>'
                '<details><summary>GPT 双审与经典巨著逐项证据</summary>'+gpt_html(card)+books_html(card)+'</details>')
    from entry_opportunity import assess as entry_assess, html as entry_html
    content += business_html(business,compact=False)
    content += action_html(entry_html(entry_assess({**row, 'scorecard': card, 'formal_recommendation': formal})),business)
    try:
        weekly = read_json_snapshot(root/'weekly_candidates_pub.json')
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
    content += '<div>下方量价分与情景估计用于辅助研究；不会覆盖中央评级，也不是实测胜率。有效评分直接复用；缺少有效评分时自动进行3A分析。</div>'
    from module_relations_ui import html as relations_html
    try:
        relations=read_json_snapshot(root/'module_relations_pub.json')
    except (OSError,ValueError):
        relations={}
    content += relations_html(relations,selection,code)
    from evolution_learning_ui import stock_html as evolution_html, health as evolution_health
    try:
        evolution = read_json_snapshot(root/'evolution_learning_pub.json')
    except (OSError,ValueError):
        evolution = {}
    try:
        evolution_status=read_json_snapshot(root/'evolution_learning_status.json')
    except (OSError,ValueError):
        evolution_status={}
    content += '<div class="v88-evolution-health">'+esc(evolution_health(evolution,evolution_status,selection))+' · '+esc(evolution.get('generated_at'))+'</div>'
    content += evolution_html((evolution.get('stocks') or {}).get(canonical(code)), compact=True)
    from persistent_watchlist_ui import html as watch_html
    try:
        watchlist = read_json_snapshot(root/'persistent_watchlist_pub.json')
    except (OSError,ValueError):
        watchlist = {}
    content += watch_html(watchlist, selection, code=code)
    return '<section class="v88-deep-contract" style="font-size:12px;padding:10px;border:1px solid #cbd5e1;border-radius:6px">'+content+'</section>'
