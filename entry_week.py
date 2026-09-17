"""Five-session entry attention. Estimates restrict attention, never grant grades.

Keep the desktop and core copies identical. Only an identity-bound, locally
verified series summary is consumed; opening a page never downloads bars.
"""
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import json
import math
from pathlib import Path
import statistics
from zoneinfo import ZoneInfo

VERSION = 'entry-week-v2'  # Evidence schema; unchanged source binding.
POLICY_VERSION = 'entry-week-attention-v3-capacity-separated'
MIN_TURNOVER={'A股':20_000_000,'美股':5_000_000,'港股':5_000_000}
SESSIONS = 5
MIN_SAMPLES = 20
LOOKBACK = 120
DEFAULT = ((Path(__file__).resolve().parents[1] if Path(__file__).parent.name == 'src'
            else Path(__file__).resolve().parent.parent/'ai-daily-report-v2')/'data/entry_week_pub.json')


def binding(row):
    # Public/private factpack IDs differ in some exports; bind the actual
    # audit identity, stock, original contract and market-source timestamps.
    from grade_focus import canonical
    value = [canonical(row.get('code')), row.get('audit_id') or (row.get('scorecard') or {}).get('audit_id'),
             row.get('central_trade_plan') or row.get('trade_plan'), row.get('source_timestamps'),
             (row.get('scorecard') or {}).get('score_policy')]
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()


@lru_cache(maxsize=4)
def _read(path, mtime, size):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def load(path=None):
    try:
        path=Path(path or DEFAULT);stat=path.stat()
        return _read(str(path),stat.st_mtime_ns,stat.st_size)
    except (OSError,ValueError):return {}


def summarize(closes):
    """Recent non-overlapping five-session blocks, only fully observed blocks.

    For each 1..5-day arrival budget retain median up/down CLOSE excursion.
    This is a transparent historical scale scenario, not a confidence interval
    or a fitted/validated probability. No maxima or sqrt-time extrapolation.
    """
    if len(closes)<MIN_SAMPLES*SESSIONS+1:raise ValueError('不足101个完整交易日，周内波动依据待补')
    if any(type(x) not in (int,float) or not math.isfinite(x) or x<=0 for x in closes):
        raise ValueError('收盘序列存在非有限值或非正价格')
    closes=closes[-LOOKBACK-1:]
    blocks=[closes[i:i+SESSIONS+1] for i in range(len(closes)-1-SESSIONS,-1,-SESSIONS)]
    return {'sample_n':len(blocks),'lookback_sessions':len(closes)-1,
            'budgets':{str(n):{'down_pct':statistics.median(max(0,(1-min(b[1:n+1])/b[0])*100) for b in blocks),
                              'up_pct':statistics.median(max(0,(max(b[1:n+1])/b[0]-1)*100) for b in blocks)}
                       for n in range(1,SESSIONS+1)}}


def assess(row, band, *, now=None, doc=None):
    from grade_focus import canonical, market_of
    from exchange_sessions import latest_completed, next_session, HALF
    from profit_contract import evaluate
    now=now or datetime.now(timezone.utc)
    result={'version':VERSION,'policy_version':POLICY_VERSION,'capacity_passed':False,'warnings':[],'eligible':False,'entry_sessions':SESSIONS,'entry_range':[],
            'sessions':[],'label':'⏸ 本周入场依据不足','model_calls':0,'probability':None}
    try:
        plan=row.get('central_trade_plan') or row.get('trade_plan') or {}
        key=canonical(row.get('code'));market=market_of(key)
        expected=latest_completed(market,now).isoformat()
        evidence=(doc if doc is not None else load()).get('records',{}).get(key) or {}
        if evidence.get('version')!=VERSION or evidence.get('binding')!=binding(row):
            raise ValueError('缺少同股、同审核、同原合同的5交易日核验，等待后台补齐')
        if evidence.get('status')!='VERIFIED':raise ValueError(evidence.get('reason') or '同源周内波动证据待补')
        if evidence.get('source_asof')!=expected:raise ValueError('周内波动行情过期，等待最新完整交易日')
        at=datetime.fromisoformat(evidence['generated_at'])
        if at.tzinfo is None or at>now:raise ValueError('周内证据时间非法')
        last=plan['last']
        if type(last) not in (int,float) or not math.isfinite(last) or last<=0 or evidence['last']!=last:
            raise ValueError('周内测算现价与中央原合同不一致')
        deadline=datetime.fromisoformat(evaluate(plan,plan.get('horizon') or row.get('horizon'),now=now)['thesis_deadline'])
        zone=ZoneInfo('America/New_York' if market=='美股' else 'Asia/Shanghai')
        day=datetime.fromisoformat(expected).date()
        for _ in range(SESSIONS):
            day=next_session(day,market)
            # A close-confirmed entry needs the session to fit the original deadline.
            hour=15 if market=='A股' else 16
            if day.strftime('%m-%d') in HALF[market]:hour=13 if market=='美股' else 12
            close=datetime(day.year,day.month,day.day,hour,tzinfo=zone)
            if close<=deadline:result['sessions'].append(day.isoformat())
        trigger=str(plan.get('promotion_trigger') or '')
        if (row.get('execution') or {}).get('triggered') is True:
            reserve=0;steps='已确认 → 核查下一实际成交价'
        elif trigger.startswith('两阶段等待：'):
            reserve=3;steps='到区间 → 次日企稳 → 后续突破 → 下一开盘核价'
        elif trigger.startswith('收盘重新站上'):
            reserve=1;steps='到区间并收盘量价确认 → 下一开盘核价'
        else:raise ValueError('原入场模板缺少可核验的周内步骤，补证后再推荐')
        budget=len(result['sessions'])-reserve
        result.update(confirmation_sessions=reserve,arrival_budget=budget,steps=steps,
                      source_asof=expected,original_holding_sessions=(plan.get('profit_contract') or {}).get('holding_sessions'))
        if budget<1:raise ValueError('原截止日前交易日不足以完成入场确认，退出本周候选')
        if evidence.get('sample_n',0)<MIN_SAMPLES:raise ValueError('完整周窗口不足20组，不能推算本周可达范围')
        turnover=evidence.get('turnover20_local')
        if type(turnover) not in (int,float) or not math.isfinite(turnover) or turnover<0:
            raise ValueError('同源20日成交额待核，不能判断入场流动性')
        result.update(turnover20_local=turnover,minimum_turnover20_local=MIN_TURNOVER[market])
        if turnover<=0:raise ValueError('20日成交额为零，无法核查可交易容量')
        result['capacity_passed']=turnover>=MIN_TURNOVER[market]
        if not result['capacity_passed']:
            result['warnings'].append('成交较薄，须核对拟交易金额、买卖价差与盘口容量；不因固定成交额门槛删除观察机会')
        scale=evidence['budgets'][str(budget)]
        if any(type(scale[k]) not in (int,float) or not math.isfinite(scale[k]) or scale[k]<0 for k in ('up_pct','down_pct')):
            raise ValueError('周内波幅数值非法')
        reachable=[last*(1-scale['down_pct']/100),last*(1+scale['up_pct']/100)]
        near=[max(band[0],reachable[0]),min(band[1],reachable[1])]
        result.update(reachable_range=reachable,sample_n=evidence['sample_n'],
                      history_start=evidence.get('history_start'),history_end=expected,
                      required_move_pct=max(band[0]-last,last-band[1],0)/last*100,
                      raw_sha256=evidence.get('raw_sha256'),series_sha256=evidence.get('series_sha256'))
        if near[0]>near[1]:
            raise ValueError(f'原可行区间超出{budget}交易日到价预算；还需{reserve}日确认，不列本周推荐')
        result.update(eligible=True,entry_range=near,label='🟢 已在本周入场带' if near[0]<=last<=near[1] else '🟠 本周条件候选',
                      reason='原区间、趋势与净RR约束，再取周内历史波动情景交集；到价仍须原量价确认')
    except (ValueError,KeyError,TypeError,ArithmeticError) as exc:
        result['reason']=str(exc)
    return result
