"""Synthetic market evidence for the pre-existing central-row UI fixtures.

Those fixtures mark their sources as {'test': ...} and use factpack p*64.
Provide their missing price-history layer explicitly, without bypassing the
five-session implementation. Real-data and entry-week tests remain unpatched.
"""
from datetime import datetime,timezone
import pytest


@pytest.fixture(autouse=True)
def synthetic_weekly_evidence(monkeypatch):
    import entry_week
    from grade_focus import canonical,market_of
    from exchange_sessions import latest_completed
    original=entry_week.assess
    def assess(row,band,*,now=None,doc=None):
        if doc is None and row.get('factpack_id')=='p'*64 and set(row.get('source_timestamps') or {})=={'test'}:
            now=now or datetime.now(timezone.utc)
            key=canonical(row['code']);plan=row.get('central_trade_plan') or row['trade_plan']
            doc={'records':{key:{'version':entry_week.VERSION,'status':'VERIFIED',
                'binding':entry_week.binding(row),'generated_at':now.isoformat(),
                'source_asof':latest_completed(market_of(key),now).isoformat(),
                'last':plan['last'],'sample_n':24,'turnover20_local':100_000_000.,
                'budgets':{str(i):{'up_pct':1.,'down_pct':1.} for i in range(1,6)}}}}
        return original(row,band,now=now,doc=doc)
    monkeypatch.setattr(entry_week,'assess',assess)
