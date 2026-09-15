"""Presentation limits only; candidate pools and monitoring history stay complete."""
import math

MARKETS = ('A股', '港股', '美股')


def market_top(rows, per_market=5, rank_key=None):
    """Keep up to five distinct securities per market in existing ranked order.

    Supply rank_key only for an authoritative ascending rank. Callers with a
    score/priority ordering should sort first; this helper does not score stocks.
    """
    limit = max(0, min(5, int(per_market)))
    candidates = list(rows or [])
    if rank_key:
        def rank(row):
            value = row.get(rank_key)
            if isinstance(value, bool):
                return math.inf
            try:
                value = float(value)
                return value if math.isfinite(value) and value > 0 else math.inf
            except (ValueError, TypeError):
                return math.inf
        candidates.sort(key=rank)
    counts = dict.fromkeys(MARKETS, 0)
    aliases = {'CN': 'A股', 'HK': '港股', 'US': '美股'}
    seen = set()
    selected = []
    for row in candidates:
        market = aliases.get(row.get('market'), row.get('market'))
        code = row.get('code') or row.get('symbol')
        if market not in counts or not code:
            continue
        key = (market, code)
        if key in seen or counts[market] >= limit:
            continue
        selected.append(row)
        seen.add(key)
        counts[market] += 1
    return selected
