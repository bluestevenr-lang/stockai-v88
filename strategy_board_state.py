"""Read-time source freshness, shared by the cloud, desktop and digest."""
from copy import deepcopy
from datetime import datetime,timezone,timedelta,time,date
from zoneinfo import ZoneInfo
from exchange_sessions import latest_completed

# v4: 3A means buyable now (>=70, business, annual uptrend, net RR>=1.5, in range).
# Rows produced under older rules are never shown under the buyable heading.
# v5: 2A/1A use the same buyable contract with backtested setups.
FOLLOWUP_POLICY='2026-10-11-quality-followup-v8'
PLAN_BASIS=('现价/入场上沿至计划目标与计划止损，同一区间口径，扣往返费用；'
            '第一上方结构位单列披露，不假定突破')
STRUCT_BASIS='现价/入场上沿至最近已验证结构位，扣往返费用；无价格计划，不以波动率外推补目标'
RESEARCH_CATEGORY='高价值可跟进 · 重点观察'
MIN_NET_SPACE_PCT=5.0
MIN_CURRENT_RR=2.0
OPPORTUNITY_POLICY=FOLLOWUP_POLICY
CURRENT_POLICIES=(FOLLOWUP_POLICY,'2026-10-11-quality-readiness-v6','2026-10-11-evidence-tiers-v5')
STILL_VALID={'2026-10-11-buyable-3a-v4':('3A',)}
BUY_FLOOR=80
TIMING_TEXT={'live':'买入条件已满足（现价在入场区间内，交易前复核）',
             'next_open':'收盘价有效·下一交易日开盘前复核（开盘价须仍在入场区间内）',
             'stale':'报价已过时，入场前更新报价'}

def _zone(market):
    return ZoneInfo('America/New_York' if market=='美股' else 'Asia/Shanghai')

def _close_minute(market,day):
    from exchange_sessions import HALF
    if day.strftime('%m-%d') in HALF.get(market,()):return 13*60 if market=='美股' else 12*60
    return 15*60 if market=='A股' else 16*60

def quote_timing(row,now):
    """'live' fresh in session; 'next_open' closing/lunch quote while shut; else 'stale'."""
    from market_session_view import session_view
    view=session_view(row['market'],now);state=view['market_status']
    at=datetime.fromisoformat(row['quote_asof'])
    if state=='交易中':
        return 'stale' if (now-at).total_seconds()>20*60 else 'live'
    local=now.astimezone(_zone(row['market']))
    if state=='午间休市':
        day=local.date();minute=690 if row['market']=='A股' else 720
    else:
        day=local.date() if state=='已收市' else date.fromisoformat(view['completed_session'])
        minute=_close_minute(row['market'],day)
    end=datetime.combine(day,time(minute//60,minute%60),_zone(row['market']))
    if row.get('source_session')!=day.isoformat():return 'stale'
    return 'next_open' if at>=end-timedelta(minutes=20) else 'stale'

def opportunity(row, structure=None, *, target_basis='plan'):
    """Current price geometry, not historical payoff or a breakout assumption.

    Conservative entry is the worse of current price and planned entry ceiling.
    Reward and risk are measured over the same horizon: the plan's own target
    against the plan's own stop. Capping reward at the first overhead level while
    keeping risk at the full structural stop is a unit mismatch — it reported a
    plan targeting +19% as "净空间-0.41%" and made 净收益风险比≥2 unreachable for
    every stock in an uptrend, because such a stock always has a recent high just
    overhead. Measured on 30 real ≥80分 stocks, that mixed form yielded a median
    ratio of 0.18 and never once reached 2.
    The nearest overhead level is still computed, published as first_obstacle_pct,
    and still vetoes an entry sitting above it — a breakout is not assumed, it is
    disclosed. Thresholds, stops and holding periods are unchanged.

    target_basis='plan' means take_profit_range is a vetted price plan and may serve
    as the thesis endpoint. target_basis='cap' means it is only an upper bound with
    no structural evidence behind it (structural_opportunity passes 现价+6×ATR as a
    measured-move ceiling); there the nearest evidenced level remains the objective,
    because extrapolating volatility is inventing a target, not measuring one.
    No tuning to fill seats.
    """
    import math
    good=lambda x:isinstance(x,(int,float)) and not isinstance(x,bool) and math.isfinite(x) and x>0
    out={'policy':OPPORTUNITY_POLICY,'min_net_space_pct':MIN_NET_SPACE_PCT,
         'min_current_rr':MIN_CURRENT_RR,'passed':False,'gaps':[]}
    entry=row.get('entry_range');stop=row.get('stop_range');target=row.get('take_profit_range')
    structure=structure or {}
    if not (entry and stop and target and len(entry)==len(stop)==len(target)==2 and
            all(good(x) for x in [*entry,*stop,*target,row.get('last')])):
        out['gaps']=['当前入场、止损或目标结构未核实'];return out
    if not 0<stop[0]<=stop[1]<entry[0]<entry[1]<target[0]<=target[1]:
        out['gaps']=['当前价带顺序不成立'];return out
    ceiling=max(entry[1],row['last'])
    levels=[*(row.get('resistance_levels') or []),row.get('nearest_resistance')]
    levels += [structure.get(k) for k in ('previous_high20','high60','high120','high52')]
    # 以实际参考入场价为界，不是计划入场下沿。价格一旦走过入场区，低于现价的
    # 旧高点仍会落在 entry[0] 之上并被当成"最近目标"——那是脚下的支撑，不是
    # 上方阻力。它会把一只真实还有20%空间的票算成负空间。
    # 取 >=：正好落在参考入场价上的结构位，是"买在阻力上"，必须算作第一障碍。
    levels=sorted({x for x in levels if good(x) and x>=ceiling})
    if not levels:
        out['gaps']=['现价已越过全部可核实结构位，上方无参照目标；不以突破假设补位'];return out
    barrier=levels[0]
    fee=row.get('fee_allowance_pct',.5)
    if not isinstance(fee,(int,float)) or not math.isfinite(fee) or fee<0:
        out['gaps']=['费用假设无效'];return out
    if target_basis!='plan':
        objective=min(target[0],barrier) if target[0]>ceiling else barrier
    elif target[0]<=ceiling:
        out['gaps']=['原计划目标已被现价越过，这份价格计划已兑现；等待新结构目标，不顺延旧目标']
        return out
    else:
        objective=target[0]
    reward=(objective/ceiling-1)*100-fee
    risk=(1-stop[0]/ceiling)*100+fee
    rr=reward/risk if risk>0 else None
    obstacle=(barrier/ceiling-1)*100-fee
    out.update(reference_entry=ceiling,first_objective=objective,nearest_resistance=barrier,
               first_obstacle_pct=round(obstacle,4),
               stop=stop[0],fee_allowance_pct=fee,net_space_pct=round(reward,4),
               risk_pct=round(risk,4),current_net_rr=round(rr,4) if rr is not None else None,
               basis=PLAN_BASIS if target_basis=='plan' else STRUCT_BASIS)
    if reward<MIN_NET_SPACE_PCT:out['gaps'].append(f'当前净空间{reward:.2f}%不足{MIN_NET_SPACE_PCT:g}%')
    if rr is None or rr<MIN_CURRENT_RR:out['gaps'].append(f'当前净收益风险比{rr:.2f}不足{MIN_CURRENT_RR:g}' if rr is not None else '当前风险无法核实')
    # 参考入场价已经站在最近结构位之上：这是追过头，不是空间。
    # 'cap' 口径下 reward 本身就是这段距离，净空间门已覆盖，不重复记一笔。
    if target_basis=='plan' and obstacle<=0:
        out['gaps'].append(f'参考入场价已在最近上方结构位{barrier:g}之上（净距离{obstacle:.2f}%），属追过头而非空间')
    out['passed']=not out['gaps']
    return out

def followup(row, structure=None):
    """Current geometry for readiness; 3A admission itself is decided once by
    strategy_board.apply_entry_gate using this same opportunity() formula."""
    row['opportunity']=opportunity(row,structure)
    row['buyable']=bool(row.get('buyable') and row['opportunity']['passed'])
    row['missing']=list(dict.fromkeys(row.get('missing',[])+row['opportunity']['gaps']))
    row['selection_category']='买入条件已满足' if row['buyable'] else RESEARCH_CATEGORY
    row['status']='条件匹配' if row['buyable'] else '研究候选'
    row.update(readiness(row))
    return row


def readiness(row):
    """Describe observed timing, never predict that a price will enter soon."""
    high=row.get('quality_eligible') or row.get('strategy_tier')=='3A'
    prefix='高分 · ' if high else ''
    if row.get('grade_basis')=='grade-ladder-v1':
        prefix={'good':'好股 · ','pending':'基本面待补 · '}.get((row.get('ladder') or {}).get('fundamentals'),prefix)
    gaps=row.get('missing') or row.get('gaps') or []
    detail='；'.join(gaps)
    roster=row.get('research_roster') or {}
    if roster.get('policy') in ('3a-daily-research-v1','3a-daily-research-v2') and not row.get('buyable'):
        if row.get('score_current_unknown'):
            return {'readiness_label':'日度研究续列 · 当前分待核',
                    'readiness_detail':detail or '保留原研究日期和已核报价；补齐数据后重评，不代表当前可买。'}
        current=(row.get('strategy_score') or {}).get('value')
        if isinstance(current,(int,float)) and current<80:
            return {'readiness_label':'日度研究续列 · 当前分低于80',
                    'readiness_detail':f'选入分{roster.get("selection_score")}，当前统一分{current:g}；本日研究身份续列，下一完整交易日重评；当前不满足3A买入分数门槛。'+detail}
    if row.get('research_retained') or row.get('buy_timing')=='stale':
        return {'readiness_label':prefix+'等待数据更新','readiness_detail':detail or '更新报价和结构后复核'}
    if row.get('buyable'):
        label=prefix+('买入条件已满足' if row.get('buy_timing')=='live' else '收盘条件满足 · 等待开盘复核')
        return {'readiness_label':label,'readiness_detail':TIMING_TEXT.get(row.get('buy_timing'),'交易前复核实际报价')}
    quality_gaps=[g for g in gaps if g.startswith('利润质量：')]
    if quality_gaps:
        return {'readiness_label':prefix+'重点观察 · 利润质量待核',
                'readiness_detail':detail+'；研究名单不等于可买，须先核实经常性利润。'}
    o=row.get('opportunity') or {}
    if high and row.get('grade_policy')==FOLLOWUP_POLICY and not o.get('passed'):
        reason='；'.join(o.get('gaps') or ['当前空间与风险待核'])
        detail='；'.join(g for g in gaps if g not in (o.get('gaps') or []))
        return {'readiness_label':RESEARCH_CATEGORY+' · 等待',
                'readiness_detail':reason+'；待回调改善入场空间，或有效突破后按新结构复核；不追价，不移动原止损。'+detail}
    entry=row.get('entry_range');price=row.get('last')
    if row.get('annual_regime')=='up' and entry and isinstance(price,(int,float)) and price>0:
        price_gap='现价未进入计划区间，待触发且不追价'
        other_gaps=[g for g in gaps if g!=price_gap]
        if (price<entry[0] or price>entry[1]) and not other_gaps:
            edge=entry[0] if price<entry[0] else entry[1]
            distance=round((edge/price-1)*100,2)
            return {'readiness_label':'趋势符合 · 等待进入买入区',
                    'readiness_detail':f'现价需变化{distance:+.2f}%至价带最近边界；进入后仍须复核其余条件。'+detail,
                    'entry_distance_pct':distance}
        if other_gaps and (price<entry[0] or price>entry[1]):
            return {'readiness_label':prefix+'重点观察 · 条件待核',
                    'readiness_detail':detail+'；不只等待价格触发。'}
        return {'readiness_label':prefix+'已在买入区 · 等待核验','readiness_detail':detail or '入场条件尚未全部通过'}
    return {'readiness_label':prefix+'研究观察 · 继续等待','readiness_detail':detail or '趋势与买点待核'}

def view(doc,now=None,*,annotate_directives=True):
    now=now or datetime.now(timezone.utc);out=deepcopy(doc)
    if out.get('grade_policy') not in CURRENT_POLICIES:
        keep=[];pending=out.setdefault('investment_candidates',[])
        valid=STILL_VALID.get(out.get('grade_policy'),())
        for row in out.get('rows',[]):
            value=(row.get('strategy_score') or {}).get('value',0);tier=row.get('strategy_tier')
            if tier in valid:keep.append(row);continue
            if valid or tier=='3A' or value>=80:
                row['grade_pending']=True
                note='旧规则结果，待按历史验证形态重算' if tier in ('1A','2A') else '旧规则结果，待按“现在可买”规则重算'
                if note not in row.setdefault('missing',[]):row['missing'].append(note)
                pending.append(row)
            else:keep.append(row)
        out['rows']=keep
    for row in out.get('rows',[])+out.get('investment_candidates',[]):
        try:
            zone=_zone(row['market'])
            at=datetime.fromisoformat(row['quote_asof'])
            required=latest_completed(row['market'],now).isoformat()
            row['current_source']=at.tzinfo is not None and at<=now and row.get('source_session') in {required,now.astimezone(zone).date().isoformat()}
            if not row['current_source']:
                row['status']='源行情待更新';row['executable']=False;row['buy_timing']='stale'
                if '最新交易日报价' not in row['missing']:row['missing'].append('最新交易日报价')
                continue
            timing=quote_timing(row,now);row['buy_timing']=timing
            if timing=='stale':
                if row['status']=='条件匹配':row['status']='研究候选'
                if '入场前更新报价' not in row['missing']:row['missing'].append('入场前更新报价')
            else:
                row['missing']=[m for m in row['missing'] if m!='入场前更新报价']
                if not row['missing'] and row['status']=='研究候选' and row.get('buyable'):row['status']='条件匹配'
        except (ValueError,KeyError,TypeError):
            row['current_source']=False;row['status']='源行情待更新';row['executable']=False;row['buy_timing']='stale'
    if out.get('grade_policy') in (FOLLOWUP_POLICY,'2026-10-11-quality-readiness-v6'):
        for row in out.get('rows',[]):
            from earnings_quality import entry_gaps
            gaps=entry_gaps(row.get('fundamentals'))
            if row.get('grade_basis')=='grade-ladder-v1':
                # 与分级同一好股门：资料缺失（基本面待补）不是硬伤，不在展示时点再挡一次。
                from grade_ladder import fundamentals_state
                if fundamentals_state(row,now)[0]=='pending':gaps=[]
            row['missing']=list(dict.fromkeys(row.get('missing',[])+gaps))
            row['buyable']=bool(row.get('buyable') and row.get('current_source') and not row.get('research_retained') and row.get('buy_timing')!='stale' and not row.get('missing'))
            if row.get('strategy_tier')=='3A':
                row['selection_category']='买入条件已满足' if row['buyable'] else RESEARCH_CATEGORY if out.get('grade_policy')==FOLLOWUP_POLICY else '最高价值研究'
            row.update(readiness(row))
            from strategy_multicheck import apply_entry_review
            apply_entry_review(row,now)
        for tier,markets in out.get('counts',{}).items():
            for market,counts in markets.items():
                members=[r for r in out['rows'] if r['strategy_tier']==tier and r['market']==market]
                counts['matched']=sum(bool(r.get('buyable')) for r in members)
    # 指令跟随展示时点的状态重算（报价过期/降级后），与页面、飞书看到的是同一条。
    if annotate_directives and out.get('rows'):
        try:
            from action_directive import annotate
            annotate(out,'3A',now=now)
        except Exception:pass
    return out
