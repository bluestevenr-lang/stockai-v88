"""Read-time safety shared by the UI and digest; stale plans remain readable."""
from copy import deepcopy
from datetime import date, datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from strategy_board_state import view as quote_view


def _terminal(row):
    """Only explicit plan termination, never a warning or a target-touch event."""
    state = str(row.get('plan_state') or '').lower()
    status = str(row.get('status') or '')
    return state in {'exited', 'expired', 'stopped', 'stop_loss', 'closed', 'cancelled', 'invalidated'} or status.startswith(
        ('本轮到期复盘', '已退出本轮研究', '已止损', '止损出场', '已平仓', '计划已失效', '已取消', '已终止'))


def _identity(row):
    return row.get('market'), row.get('code'), row.get('plan_id')


def view(doc, now=None):
    now = now or datetime.now(timezone.utc)
    out = deepcopy(doc)
    if not out.get('version'): return out
    terminal = {_identity(row): deepcopy(row) for row in out.get('rows', []) if _terminal(row)}
    rows = quote_view({'grade_policy': '2026-09-28-highest-3a-v2', 'rows': out.get('rows', [])}, now)['rows']
    day = now.astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat()
    old_cycle = day >= str(out.get('next_recalculation') or '')
    for row in rows:
        original_terminal = terminal.get(_identity(row))
        window = row.get('window') or {}
        expired = not window.get('deadline') or day > window['deadline']
        if window.get('start') and window.get('deadline'):
            try:
                from exchange_sessions import latest_completed, is_session
                completed = latest_completed(row['market'], now)
                start = date.fromisoformat(window['start']); end = date.fromisoformat(window['deadline'])
                window['remaining_sessions'] = sum(is_session(start+timedelta(days=n), row['market'])
                    and start+timedelta(days=n)>completed for n in range((end-start).days+1))
                expired = expired or window['remaining_sessions']==0
            except ValueError:
                expired = True
        if original_terminal:
            # quote_view may replace stale-source labels; a finished plan cannot
            # become a new waiting/entry candidate merely because its quote ages.
            row['status'] = original_terminal.get('status') or '原计划已终止 · 留档'
            for key in ('plan_state', 'readiness_label', 'readiness_detail'):
                if key in original_terminal: row[key] = deepcopy(original_terminal[key])
            row.update(rule_matched=False, buyable=False, executable=False)
            continue
        if old_cycle or expired or not row.get('current_source'):
            row['rule_matched'] = False
            row['buyable'] = False
            row['executable'] = False
            row['status'] = '原轮次跟踪 · 待重算' if old_cycle else '本轮到期复盘' if expired else '原研究保留 · 报价待更新'
            # Preserve the governing lifecycle/source state, rather than letting
            # a later entry check relabel it as an active research candidate.
            continue
        elif row.get('missing'):
            row['rule_matched'] = False
            row['status'] = '原计划研究 · 等待' if row.get('plan_id') else '研究候选'
        from strategy_multicheck import apply_entry_review
        apply_entry_review(row,now)
    out['rows'] = rows
    out['rule_matched_count'] = sum(r.get('rule_matched', False) for r in rows)
    return out
