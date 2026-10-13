"""Real producer timestamps; reading a view must never advance the screening clock."""
from datetime import datetime,timezone,timedelta
BJT=timezone(timedelta(hours=8))

def display(value):
    try:
        at=value if isinstance(value,datetime) else datetime.fromisoformat(str(value).replace('Z','+00:00'))
        if at.tzinfo is None:return '时间未标时区'
        return at.astimezone(BJT).strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError,TypeError):return '尚无有效筛选时间'

def screened_at(doc):
    return doc.get('screening_finished_at') or doc.get('generated_at')

def market_screened_at(doc, market):
    """Keep the chosen producer's exact clock through publication and merging."""
    return (doc.get('market_source_generated_at') or {}).get(market) or screened_at(doc)

def quote_clocks(doc):
    result={}
    for market in ('A股','港股','美股'):
        values=[]
        # A market can have valid quotes without a selected recommendation.
        try:
            at=datetime.fromisoformat(str((doc.get('market_quote_times') or {}).get(market)).replace('Z','+00:00'))
            if at.tzinfo is not None:values.append(at)
        except (ValueError,TypeError):pass
        for row in doc.get('rows',[])+doc.get('investment_candidates',[]):
            if row.get('market')!=market:continue
            try:
                at=datetime.fromisoformat(row.get('quote_asof','').replace('Z','+00:00'))
                if at.tzinfo is not None:values.append(at)
            except (ValueError,TypeError):pass
        result[market]=display(max(values)) if values else '暂无报价时间'
    return result
