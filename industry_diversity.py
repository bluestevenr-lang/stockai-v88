"""Deterministic one industry seat per market; reserve records are preserved."""
REASON='同行业已有更高分入选'

def industry(row,profiles=None):
    from stock_profile_view import profile
    p=profile(row['code'],profiles) if profiles and 'records' in profiles else (profiles or {}).get(row['code'],{})
    return str(p.get('industry') or (row.get('industry_snapshot') or {}).get('industry') or row.get('industry') or '').strip()

def select(rows,limit,profiles=None,*,key=None,reason=REASON):
    chosen=[];reserve=[];occupied=set()
    for row in sorted(rows,key=key or (lambda r:(-r['strategy_score']['value'],r['code']))):
        sector=industry(row,profiles)
        key=(row['market'],sector)
        if sector and key in occupied:
            row=dict(row);row['selection_gap']=reason;reserve.append(row)
        elif len(chosen)<limit:
            chosen.append(row)
            if sector:occupied.add(key)
        else:reserve.append(row)
    return chosen,reserve
