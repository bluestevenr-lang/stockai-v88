"""Read-only recommendation-day ordinals shared by every calendar.

A security has one recommendation per Beijing calendar date across all modules.
Counts use original publication times, reset each month, and stop at the day
being rendered. Quotes, check-ins, exits and Astra seen/outcome dates never add
recommendations. This module does not alter or persist historical events.
"""
from bisect import bisect_left, bisect_right
from collections import defaultdict
from datetime import date, datetime
from zoneinfo import ZoneInfo

from market_symbols import canonical

BJT = ZoneInfo('Asia/Shanghai')
MARKETS = {'CN': 'A股', 'HK': '港股', 'US': '美股', '中国': 'A股',
           'A股': 'A股', '港股': '港股', '美股': '美股'}
NON_RECOMMENDATIONS = {'check', 'left', 'followup', 'follow_up', 'tracking',
                       'price_update', 'quote_update', 'risk_review', 'exit'}


def security_key(market, code):
    """Keep market/share-class identity while normalizing known symbol aliases."""
    market = MARKETS.get(market)
    if not market or not isinstance(code, str) or not code.strip():
        return None
    symbol = canonical(code.strip())
    if market == '港股' and symbol.isdigit():
        symbol = (symbol.lstrip('0') or '0') + '.HK'
    return market, symbol


def publication_day(value):
    """Return an original aware timestamp's Beijing date, never a quote clock."""
    try:
        stamp = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        if stamp.tzinfo is None:
            return None
        return stamp.astimezone(BJT).date().isoformat()
    except (TypeError, ValueError, OverflowError):
        return None


def is_recommendation(event):
    return (event.get('state') == 'listed'
            and event.get('counts_as_recommendation') is not False
            and event.get('event_type') not in NON_RECOMMENDATIONS)


def recommendation_days(journal=None, astra=None):
    """Index the full source documents BEFORE search, pagination or daily caps."""
    days = defaultdict(set)
    for row in (journal or {}).get('rows', []):
        key = security_key(row.get('market'), row.get('code'))
        if key is None:
            continue
        for events in row.get('days', {}).values():
            for event in events:
                if is_recommendation(event):
                    day = publication_day(event.get('at'))
                    if day:
                        days[key].add(day)
    for record in (astra or {}).get('calendar_records', []):
        row = record.get('row') or {}
        key = security_key(row.get('market'), row.get('code'))
        if key is None or record.get('counts_as_recommendation') is False:
            continue
        if record.get('event_type') in NON_RECOMMENDATIONS:
            continue
        day = publication_day(record.get('at'))
        if day:
            days[key].add(day)
    return {key: tuple(sorted(values)) for key, values in days.items()}


def count_asof(index, market, code, day):
    """Number of distinct recommendation days this month through ``day``."""
    try:
        day = date.fromisoformat(day).isoformat()
    except (TypeError, ValueError):
        return 0
    days = index.get(security_key(market, code), ())
    return bisect_right(days, day) - bisect_left(days, day[:7] + '-01')


def count_label(name, count):
    return f'{name}*{count}' if count > 0 else str(name)


def count_tooltip(count, day, recommended=True):
    if recommended and count > 0:
        return f'本月第{count}个推荐日；同股同日跨模块仅计一次'
    if count > 0:
        return f'截至{day}，本月已推荐{count}个不同日期；本条仅跟踪，不新增推荐'
    return f'截至{day}，本月尚无已留存推荐日；本条仅跟踪，不新增推荐'


def annotated_rows(journal):
    """Attach full-history per-security dates to copies, before any view filter."""
    index = recommendation_days(journal)
    return [{**row, 'monthly_recommendation_days': index.get(security_key(row.get('market'), row.get('code')), ())}
            for row in journal.get('rows', [])]


def row_count(row, day):
    days = row.get('monthly_recommendation_days')
    index = ({security_key(row.get('market'), row.get('code')): days} if days is not None
             else recommendation_days({'rows': [row]}))
    return count_asof(index, row.get('market'), row.get('code'), day)
