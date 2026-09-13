"""Synthetic fixtures only: never market recommendations."""
from datetime import datetime, timedelta, timezone
from profit_contract import VERSION

def short_inputs(low=11.4, high=11.5):
    now = datetime.now(timezone(timedelta(hours=8)))
    return {"version":VERSION,"horizon":"short","kind":"observed_20day_boundary",
            "max_calendar_days":30,"holding_sessions":10,"asof":now.isoformat(),
            "source_url":"https://example.invalid/test-bars", "peaks":[
                {"date":(now-timedelta(days=1)).date().isoformat(),"high":low},
                {"date":now.date().isoformat(),"high":high}]}

def annual_inputs(low=18, high=20):
    now=datetime.now(timezone(timedelta(hours=8))).isoformat()
    return {"version":VERSION,"horizon":"long","kind":"annual_dual_valuation",
            "max_calendar_days":365,"asof":now,"valuation":{
                "source_documents":[{"title":"synthetic financial report","url":"https://example.invalid/report","asof":now},
                                    {"title":"synthetic cross check","url":"https://example.invalid/check","asof":now}],
                "forward_eps_range":[1,1],"pe_range":[low,high],
                "forward_fcf_per_share_range":[low*.1,high*.1],"required_fcf_yield_range":[.1,.1],
                **{k:"Synthetic documented hypothesis" for k in ["earnings_bridge","cashflow_bridge","valuation_basis","catalyst","countercase","invalidation"]}}}

def long_facts():
    return {"horizon":"long","entry_range":[9.9,10],"profit_inputs":annual_inputs(),
            "fundamentals":{"eps_growth":10,"rev_growth":8,"fcf":100,"roe":10,"net_margin":5,"pe_ttm":18},
            "long_history":{"history_years":6,"cagr":9},"fundamental_invalidation":{"field":"fcf","threshold":0}}
