"""Astra attention/participation limits; never changes security grades or fills."""
import math

VERSION = 'astra-cn-hk-first-us-reference-v1'
TOTAL_LIMIT = 3
US_REFERENCE_LIMIT = 1
US_MIN_AUDIT_SCORE = 85
PREFERRED_MARKETS = ('A股', '港股')
POLICY = {'version': VERSION, 'total_limit': TOTAL_LIMIT,
          'preferred_markets': list(PREFERRED_MARKETS), 'preferred_slots_before_us': 2,
          'us_reference_limit': US_REFERENCE_LIMIT, 'us_min_audit_score': US_MIN_AUDIT_SCORE,
          'us_reference_only': True, 'no_forced_fill': True,
          'score_meaning': '中央当前审核证据分，不是胜率；85分为Astra参考筛选门槛',
          'participation_markets': list(PREFERRED_MARKETS)}


def high_score_us(row):
    score = row.get('audit_score')
    return (row.get('market') == '美股' and row.get('central_tier') in ('1A', '2A', '3A')
            and type(score) in (int, float) and math.isfinite(score) and US_MIN_AUDIT_SCORE <= score <= 100)


def valid_composition(rows):
    """For current consumer inputs only; historical trades remain recordable."""
    us = [r for r in rows if r.get('market') == '美股']
    return (len(rows) <= TOTAL_LIMIT and len(us) <= US_REFERENCE_LIMIT
            and all(high_score_us(r) and r.get('reference_only') is True for r in us)
            and all(r.get('market') in (*PREFERRED_MARKETS, '美股') for r in rows))
