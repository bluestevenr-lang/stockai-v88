"""Research evidence clocks, shared by ingestion consumers and the list UI.

Only declared daily-history/paired-quote sources use completed sessions.
This does not certify data quality, extend a review or permit execution.
"""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from exchange_sessions import latest_completed

BJT = timezone(timedelta(hours=8))
DAILY_SOURCES = frozenset({'yahoo_daily', 'tencent_daily', 'sina_daily'})
SESSION_SOURCES = DAILY_SOURCES | {'eastmoney_quote', 'tencent_quote', 'yahoo_quote'}
FRESHNESS_POLICY = 'market-evidence-session-v2-typed-quotes'


def source_time_fresh(source, value, market, now=None):
    now = now or datetime.now(BJT)
    if now.tzinfo is None:
        return False
    now = now.astimezone(BJT)
    raw = str(value or '').replace('（北京时间）', '').strip()
    if len(raw) < 16:
        return False
    try:
        at = datetime.fromisoformat(raw.replace('Z', '+00:00'))
        if source not in SESSION_SOURCES:
            at = at if at.tzinfo else at.replace(tzinfo=BJT)
            hours = 72 if now.weekday() >= 5 else 30
            return timedelta(0) <= now - at <= timedelta(hours=hours)
        # Session-based exceptions require explicitly dated, zoned evidence.
        # Unknown markets/years, missing times and future stamps fail closed.
        if at.tzinfo is None or at > now:
            return False
        zone = ZoneInfo('America/New_York' if market == '美股' else 'Asia/Shanghai')
        day = at.astimezone(zone).date()
        if day != latest_completed(market, now):
            return False
        # A daily bar cannot claim to be complete before its own close.
        # A paired quote may be the last observed trade on that same date;
        # independent-price checks remain the data layer's responsibility.
        return source not in DAILY_SOURCES or latest_completed(market, at) == day
    except (ValueError, TypeError, OverflowError, KeyError):
        return False


def source_times_fresh(row, now=None, *, sources=None, require_all=True):
    """Check each typed source; mixed news/module timestamps keep their TTL."""
    sources = row.get('source_timestamps') if sources is None else sources
    if not isinstance(sources, dict) or not sources:
        return False
    now = now or datetime.now(BJT)
    checks = (source_time_fresh(key, value, row.get('market'), now)
              for key, value in sources.items())
    return all(checks) if require_all else any(checks)
