"""Project a dated data receipt onto the current completed market session.

Stored observations remain historical evidence when a new session completes.
No capture timestamps, coverage denominators or success receipts are rewritten.
"""
from datetime import datetime, timezone
from exchange_sessions import latest_completed

VERSION = 'dated-session-coverage-v1'


def project(market, entry, now=None):
    now = now or datetime.now(timezone.utc)
    try:
        required = latest_completed(market, now).isoformat()
    except (ValueError, KeyError, TypeError):
        required = None
    daily = entry.get('verified_daily_by_session')
    quotes = entry.get('quote_available_by_session')
    n = entry.get('denominator', 0)
    def valid_counts(counts):
        return (isinstance(counts, dict) and type(n) is int and n >= 0
                and all(isinstance(day, str) and type(value) is int and value >= 0
                        for day, value in counts.items()) and sum(counts.values()) <= n)
    dated = valid_counts(daily) and valid_counts(quotes)
    count = daily.get(required, 0) if dated and required else 0
    quoted = quotes.get(required, 0) if dated and required else 0
    retained = entry.get('retained_history_by_session')
    stored = sum(daily.values()) if dated else entry.get('verified_daily', 0)
    if dated and valid_counts(retained) and all(retained.get(day,0)>=count for day,count in daily.items()):
        stored = sum(retained.values())
    return {'policy': VERSION, 'required_session': required, 'dated_receipt': dated,
            'verified_daily': count, 'quote_available': quoted,
            'stored_verified_daily': stored, 'expired_or_undated_daily': max(0, stored-count),
            'daily_target_met': bool(dated and required and n > 0 and count/n >= .9
                                     and entry.get('directories_complete') is True)}
