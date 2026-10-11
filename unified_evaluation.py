"""Per-security V88 verdict published by the core board; the deep view never re-grades.

The core writes one mid-term evaluation per screened security (score, annual
regime, company evidence, plan, buy status) into strategy_board.json. Deep
analysis, 3A and the board all read that same record.
"""
from html import escape
from copy import deepcopy
from threading import RLock
from datetime import datetime, timezone

CURRENT_POLICIES = ('2026-10-11-evidence-tiers-v5', '2026-10-11-buyable-3a-v4')
_INDEX = {}
_INDEX_LOCK = RLock()


def _canonical(code):
    code = str(code or '').strip().upper().replace('.SH', '.SS')
    if code.endswith('.HK') and code[:-3].isdigit():
        return str(int(code[:-3])) + '.HK'
    return code


def _doc():
    from v88_paths import DATA
    from module_freshness import read_snapshot
    return read_snapshot(DATA / 'strategy_board.json') or {}


def lookup(code, doc=None, now=None):
    """Return the unified record for ``code`` or None (never a fabricated grade)."""
    doc = _doc() if doc is None else doc
    if doc.get('grade_policy') not in CURRENT_POLICIES:
        return None
    evaluations = doc.get('evaluations') or {}
    # Hold the snapshot itself: Python can recycle an old dictionary's id.
    # Lookup and replacement share a lock across Streamlit sessions.
    with _INDEX_LOCK:
        if _INDEX.get('source') is not evaluations or _INDEX.get('size') != len(evaluations):
            _INDEX.update(source=evaluations, size=len(evaluations),
                          map={_canonical(k): k for k in evaluations})
        raw = _INDEX['map'].get(_canonical(code))
        if not raw or raw not in evaluations:
            return None
        row = deepcopy(evaluations[raw])
    row.update(code=raw, board_generated_at=(doc.get('market_source_generated_at') or {}).get(row.get('market'))
               or doc.get('generated_at'), grade_policy=doc.get('grade_policy'))
    from strategy_board_state import quote_timing
    now = now or datetime.now(timezone.utc)
    try:
        at = datetime.fromisoformat(row['quote_asof'])
        timing = quote_timing(row, now) if at.tzinfo and at <= now else 'stale'
    except (KeyError, TypeError, ValueError):
        timing = 'stale'
    row['buy_timing'] = timing
    for result in [row, *(row.get('tiers') or {}).values()]:
        result['published_buyable'] = bool(result.get('buyable'))
        result['buy_timing'] = timing
        if timing == 'stale' and result.get('buyable'):
            result['buyable'] = False
            result['verdict'] = str(result.get('verdict') or '').replace('现在可买', '报价待更新')
            result['gaps'] = list(dict.fromkeys([*(result.get('gaps') or []), '入场前更新报价']))
    return row


TIMING = {'live': '可立即买入（现价在入场区间内）',
          'next_open': '下一交易时段开盘可买（开盘价仍在入场区间内才成立）',
          'stale': '报价已过时，入场前更新报价'}


def _num(x):
    return f'{x:g}' if isinstance(x, (int, float)) else '待核'


def html(u):
    """Verdict strip shown above the future curve: same numbers as the board."""
    if not isinstance(u, dict):
        return ('<div class="v88-unified missing" style="border:1px solid #d0d7e2;border-radius:10px;padding:10px 14px;'
                'margin:6px 0 10px;color:#5b6577">V88统一评估：本票未进入本轮统一评估池（缺日线结构或企业依据），'
                '下方曲线仅按本页年度行情推演，不构成3A结论。</div>')
    verdict = u.get('verdict') or '未入3A'
    buy = u.get('buyable')
    color = '#0f7b45' if buy else '#b26a00' if verdict.startswith(('3A待买点', '接近3A')) else '#5b6577'
    parts = [f'<b style="color:{color}">{escape(verdict)}</b>', f'统一综合分 {_num(u.get("score"))}',
             f'年度{escape(str(u.get("annual_label") or "待核"))}',
             '企业依据：' + escape(str(u.get('business_source') or ('反证' if u.get('business_status') == 'counterevidence' else '待补')))]
    lines = ['<div>' + ' · '.join(parts) + '</div>']
    if buy:
        lines.append('<div>' + escape(TIMING.get(u.get('buy_timing'), '')) + f'；入场 {escape(str(u.get("entry_range")))} · 止损 {_num(u.get("stop"))}'
                     + f' · 离场：收盘跌破60日线 {_num(u.get("trend_exit_ma60"))} 或触及止损 · 历史同型盈亏比 {_num(u.get("net_rr"))}（回测，非承诺）</div>')
    trig = (u.get('trigger') or {}).get('text')
    if not buy and trig:
        lines.append('<div>买点触发条件：' + escape(trig) + '</div>')
    elif not buy and u.get('gaps'):
        lines.append('<div>未入3A原因：' + escape('；'.join(u['gaps'][:3])) + '</div>')
    for tier in ('2A', '1A'):
        t = (u.get('tiers') or {}).get(tier)
        if not t:
            continue
        text = f'<b>{escape(t.get("verdict") or tier)}</b> · {escape(str(t.get("setup") or ""))}'
        if t.get('buyable'):
            text += ' · ' + escape(TIMING.get(t.get('buy_timing') or u.get('buy_timing'), ''))
        if t.get('plan_enabled'):
            text += (f' · 入场 {escape(str(t.get("entry_range")))} · 止损 {_num(t.get("stop"))} · {escape(str(t.get("exit_text") or ""))}'
                     + (f'（{_num(t.get("exit_ma"))}）' if t.get('exit_ma') else '')
                     + f' · 历史同型盈亏比 {_num(t.get("net_rr"))}（回测，非承诺）')
        if t.get('gaps') and not t.get('buyable'):
            text += ' · ' + escape('；'.join(t['gaps']))
        lines.append('<div>' + text + '</div>')
    if u.get('business_reason'):
        lines.append('<div style="color:#5b6577">' + escape(u['business_reason']) + '</div>')
    currency = u.get('currency') or {'A股':'CNY','港股':'HKD','美股':'USD'}.get(u.get('market'), '')
    lines.append(f'<div style="color:#8a94a6;font-size:12px">行情 {escape(str(u.get("quote_asof") or "待核"))} · 现价 {_num(u.get("last"))} {escape(currency)}'
                 f' · 统一评估生成 {escape(str(u.get("board_generated_at") or "待核"))} · 与策略榜共用评估；年度图表资料不足时另列缺口</div>')
    return ('<div class="v88-unified" style="border:1px solid #c9d6ea;border-left:4px solid ' + color +
            ';border-radius:10px;padding:10px 14px;margin:6px 0 10px;line-height:1.7">' + ''.join(lines) + '</div>')
