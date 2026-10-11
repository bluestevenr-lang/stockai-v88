"""Deterministic one industry seat per market; reserve records are preserved."""
REASON='同行业已有更高分入选'

def industry(row,profiles=None):
    from stock_profile_view import profile
    p=profile(row['code'],profiles) if profiles and 'records' in profiles else (profiles or {}).get(row['code'],{})
    return str(p.get('industry') or (row.get('industry_snapshot') or {}).get('industry') or row.get('industry') or '').strip()

def selection_industry(row,profiles=None):
    """Concentration family; keep issuer peer taxonomy/ranks unchanged."""
    sector=industry(row,profiles)
    folded=sector.casefold()
    if any(token in folded for token in ('石油','天然气','油气','oil & gas','oil and gas')):
        return '油气产业'
    return sector


def select(rows,limit,profiles=None,*,key=None,reason=REASON):
    chosen=[];reserve=[];occupied=set()
    for row in sorted(rows,key=key or (lambda r:(-r['strategy_score']['value'],r['code']))):
        sector=selection_industry(row,profiles)
        key=(row['market'],sector)
        if sector and key in occupied:
            row=dict(row);row['selection_gap']=reason;reserve.append(row)
        elif len(chosen)<limit:
            chosen.append(row)
            if sector:occupied.add(key)
        else:reserve.append(row)
    return chosen,reserve


def candidate_top3(rows):
    """Presentation-only Top3 per market; never mutate the retained research pool."""
    from math import isfinite
    from market_symbols import canonical
    def score(row):
        try:
            value=float((row.get('strategy_score') or {}).get('value'))
            return value if isfinite(value) else float('-inf')
        except (TypeError, ValueError):
            return float('-inf')
    focused=[]
    for market in ('A股','港股','美股'):
        seen=set()
        pool=sorted((r for r in rows if r.get('market')==market and r.get('code')),
                    key=lambda r:(-score(r),canonical(r['code'])))
        for row in pool:
            code=canonical(row['code'])
            if code in seen:continue
            seen.add(code)
            focused.append(dict(row,rank=len(seen)))
            if len(seen)==3:break
    return focused
