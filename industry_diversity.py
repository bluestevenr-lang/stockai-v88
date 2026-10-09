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
