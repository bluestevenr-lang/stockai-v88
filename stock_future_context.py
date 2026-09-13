"""Read-only stock future context, shared with the focused deep route.

No cross-request cache: source/session and central signatures are revalidated by
existing loaders on each request. The caller may memoize within one render only.
"""
import hashlib
import json


def _missing_context(code, name, reason):
    from trend_scenarios import build_stock
    future = build_stock({}, {}, {}, code=code, name=name)
    future['headline'] = '本地未来研判证据暂未就绪'
    future['gaps'].insert(0, reason)
    future['input_id'] = hashlib.sha256(json.dumps(
        {k: v for k, v in future.items() if k != 'input_id'}, ensure_ascii=False,
        sort_keys=True, allow_nan=False).encode()).hexdigest()
    return {'future_scenario': future, 'period_consistency': {}}


def for_stock_context(code, name=''):
    """Return the future path AND its original, fully bound period receipt."""
    from focused_deep_view import normalize_code, calculate
    from deep_analysis_data import fetch
    from deep_cross_validation import load_context
    from trend_scenarios import build_stock
    normalized = normalize_code(code)
    if not normalized:
        return {'future_scenario': build_stock({}, {}, {}, code=str(code or ''), name=str(name or '')),
                'period_consistency': {}}
    try:
        context = load_context(normalized)
        frame, quality = fetch(normalized, allow_network=False)
        result = calculate(context, frame, quality, name or normalized, normalized)
    except ImportError as exc:
        return _missing_context(normalized, name or normalized,
            f'本地证据依赖未安装或不可读取（{exc.name or type(exc).__name__}）；没有生成替代曲线')
    except OSError as exc:
        return _missing_context(normalized, name or normalized,
            f'本地证据文件读取失败（{type(exc).__name__}）；请核对数据文件，原合同仍保留')
    # The focused route owns the same future model; no UI-specific grade or
    # independent forecasting formula is introduced here.
    future = result.get('future_scenario') or build_stock(
        result.get('annual_outlook'), result.get('synthesis'),
        result.get('period_consistency'), code=normalized, name=name or normalized)
    return {'future_scenario': future,
            'period_consistency': result.get('period_consistency') or {}}


def for_stock(code, name=''):
    return for_stock_context(code, name)['future_scenario']
