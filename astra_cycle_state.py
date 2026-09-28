"""Read-time safety shared by the UI and digest; stale plans remain readable."""
from copy import deepcopy
from datetime import date, datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from strategy_board_state import view as quote_view


def view(doc, now=None):
    now = now or datetime.now(timezone.utc)
    out = deepcopy(doc)
    if not out.get('version'): return out
    rows = quote_view({'grade_policy': '2026-09-28-highest-3a-v2', 'rows': out.get('rows', [])}, now)['rows']
    day = now.astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat()
    old_cycle = day >= str(out.get('next_recalculation') or '')
    for row in rows:
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
        if old_cycle or expired or not row.get('current_source'):
            row['rule_matched'] = False
            row['status'] = '原轮次跟踪 · 待重算' if old_cycle else '本轮到期复盘' if expired else '原研究保留 · 报价待更新'
        elif row.get('missing'):
            row['rule_matched'] = False
            row['status'] = '研究候选'
    out['rows'] = rows
    out['rule_matched_count'] = sum(r.get('rule_matched', False) for r in rows)
    return out
