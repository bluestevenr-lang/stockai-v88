"""2026 official cash-equity calendars, kept separate by exchange.

Unverified years raise instead of silently claiming a weekday is tradable.
Unexpected closures still require a provider/exchange event update.
"""
from datetime import date,timedelta,datetime
from zoneinfo import ZoneInfo
SOURCES={
 'A股':'https://www.sse.com.cn/disclosure/dealinstruc/closed/',
 '港股':'https://www.hkex.com.hk/-/media/HKEX-Market/Services/Circulars-and-Notices/Participant-and-Members-Circulars/SEHK/2025/ce_SEHK_CT_075_2025.pdf',
 '美股':'https://www.nyse.com/trade/hours-calendars'}
CLOSED={
 'A股':{'01-01','01-02','02-16','02-17','02-18','02-19','02-20','02-23','04-06','05-01','05-04','05-05','06-19','09-25','10-01','10-02','10-05','10-06','10-07'},
 '港股':{'01-01','02-17','02-18','02-19','04-03','04-06','04-07','05-01','05-25','06-19','07-01','10-01','10-19','12-25'},
 '美股':{'01-01','01-19','02-16','04-03','05-25','06-19','07-03','09-07','11-26','12-25'}}
HALF={'A股':set(),'港股':{'02-16','12-24','12-31'},'美股':{'11-27','12-24'}}

def is_session(day,market='A股'):
    day=day.date() if isinstance(day,datetime) else date.fromisoformat(day) if isinstance(day,str) else day
    if day.year!=2026:raise ValueError('该年度交易所日历未核对，不能推断交易日')
    if market not in CLOSED:raise ValueError('unknown market')
    return day.weekday()<5 and day.strftime('%m-%d') not in CLOSED[market]

def next_session(day=None,market='A股'):
    d=(day or date.today())+timedelta(days=1)
    for _ in range(20):
        if is_session(d,market):return d
        d+=timedelta(days=1)
    raise ValueError('future trading calendar unavailable')


def latest_completed(market, now):
    """Latest completed cash session, independent of the caller's time zone."""
    market = 'A股' if market == '中国' else market
    if market not in CLOSED or now.tzinfo is None:
        raise ValueError('完整交易日核验要求已知市场和带时区时间')
    local = now.astimezone(ZoneInfo('America/New_York' if market == '美股' else 'Asia/Shanghai'))
    day = local.date()
    hour = 15 if market == 'A股' else 16
    if day.strftime('%m-%d') in HALF[market]:
        hour = 13 if market == '美股' else 12
    if local.hour < hour:
        day -= timedelta(days=1)
    while not is_session(day, market):
        day -= timedelta(days=1)
    return day

def session_view(market,now=None):
    now=now or datetime.now(ZoneInfo('Asia/Shanghai'))
    if now.tzinfo is None:raise ValueError('market clock requires timezone')
    local=now.astimezone(ZoneInfo('America/New_York' if market=='美股' else 'Asia/Shanghai'))
    day=local.date();minute=local.hour*60+local.minute
    close=15*60 if market=='A股' else 16*60
    if day.strftime('%m-%d') in HALF[market]:close=13*60 if market=='美股' else 12*60
    active=is_session(day,market)
    state=('休市' if not active else '未开市' if minute<570 else
           '已收市' if minute>=close else '午间休市' if market!='美股' and (690 if market=='A股' else 720)<=minute<780 else '交易中')
    attention=day if active and minute<close else next_session(day,market)
    return {'attention_session':attention.isoformat(),'market_status':state,
            'local_date':day.isoformat(),'completed_session':latest_completed(market,now).isoformat()}


def next_labels(day=None):
    if day is not None:return '；'.join(f'{m} {next_session(day,m).isoformat()}' for m in CLOSED)
    return '；'.join(f'{m} {v["attention_session"]}（{v["market_status"]}）' for m in CLOSED for v in [session_view(m)])


def calendar_health(day=None):
    day=day or date.today()
    end=date(2026,12,31)
    left=(end-day).days
    return {"verified_year":2026,"expires_at":end.isoformat(),"days_remaining":max(0,left),
            "status":"EXPIRED" if day.year!=2026 else "REVIEW_NEXT_YEAR" if left<=90 else "CURRENT_VERIFIED_YEAR",
            "note":"未核实年度禁止推断交易日；库提供候选日历，官方核验适配器负责最终边界"}
