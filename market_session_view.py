"""盘中视图（关注交易日/开收市状态），从 exchange_sessions 拆出。

exchange_sessions.py 属于纸面实验的已审核执行源码（replay_compatibility 按字节哈希核验），
任何字节改动都会让既有原合同停在“实现变化未获明确等价验证”；展示类函数因此放在此处。
"""
from datetime import datetime
from zoneinfo import ZoneInfo

from exchange_sessions import CLOSED, HALF, is_session, latest_completed, next_session


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
    from exchange_sessions import next_labels as by_day
    if day is not None:return by_day(day)
    return '；'.join(f'{m} {v["attention_session"]}（{v["market_status"]}）' for m in CLOSED for v in [session_view(m)])
