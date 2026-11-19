"""Calendar-day research cycles, independent of paid models and private P&L.

Fifteen exchange sessions are a time stop, never a promise of target attainment.
The old account ledger, signed contracts and monthly loss latches are untouched.
"""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import json, os
from zoneinfo import ZoneInfo

VERSION = 'astra-calendar-2026-11-19-v8-native-directive'
BASE = Path(__file__).resolve().parents[1]
BJT = ZoneInfo('Asia/Shanghai')
MARKETS = ('A股', '港股', '美股')
# Monthly goal maths: $200 target, $62.5 risk per trade, at most two entries.
# One target hit at RR>=2 returns >=$125; two hits cover the goal.
MONTHLY_TARGET_USD = 200
RISK_PER_TRADE_USD = 62.5
MONTHLY_RISK_CAP_USD = 125
MIN_NET_TARGET_PCT = 6.0
MIN_NET_RR = 2.0
US_ABSOLUTE_FLOOR = 80
MIN_SCORE = 60
ASTRA_HOLD = 20   # 一个月内回报的轮次长度；证据表命中行的持有天数按规则（10或20日，见 horizon_book）
SELECTION_POLICY = {'version': 'astra-horizon-v4', 'min_score': MIN_SCORE, 'min_net_target_pct': MIN_NET_TARGET_PCT,
                    'position_rule': '买入级只给 horizon_book 周期证据表 Astra 规则（A股低宽度低位企稳20日+10%、缩量回踩前3 20日+8%；美股低宽度低位企稳20日+8%、金叉前3 10日+6%），前后两半段胜率各≥53%；其余观察级',
                    'stop_rule': '止损=max(10日低×0.99, 入场×0.92)，距现价5-8%；证据表命中不另设盈亏比/净空间门槛（回测已按该止损/目标口径核验胜率）',
                    'evidence': '云端日线回测（信号日2026-01-19~11-11），以2026-06-20切前后两半段，两半段胜率各≥53%才入表；当前成分股含幸存者偏差，非收益保证',
                    'time_rule': '目标距入场上沿 ≤ ATR14×√min(20, 本月剩余交易日)——只做本月能兑现的',
                    'price_plan': 'horizon-plan-v1', 'us_rule': '中港优先占满3席；美股最多1只、仅补空位参考。命中周期证据表按同一好股/时机门；未命中另需≥80且为当轮美股前5%',
                    'note': '门槛由月目标反推，用于筛掉"即使止盈也赚不够"的计划；不是胜率或收益保证。'}


def cycle_dates(now):
    if now.tzinfo is None: raise ValueError('timezone required')
    day = now.astimezone(BJT).date()
    anchor = day.replace(day=max(d for d in (1,10,20) if d<=day.day))
    following = (day.replace(day=10) if day.day<10 else day.replace(day=20) if day.day<20
                 else (day.replace(day=28)+timedelta(days=4)).replace(day=1))
    return anchor, following


def window(anchor, market, now, hold=ASTRA_HOLD):
    from exchange_sessions import is_session, latest_completed
    from market_session_view import session_view
    sessions = []
    day = anchor
    try:
        end = (anchor.replace(day=28)+timedelta(days=4)).replace(day=1)-timedelta(days=1)
        while len(sessions) < hold and day <= end:
            if is_session(day, market): sessions.append(day)
            day += timedelta(days=1)
        completed = latest_completed(market, now)
        remaining = [d for d in sessions if d > completed]
        attention = session_view(market, now)['attention_session']
        return {'start': sessions[0].isoformat(), 'deadline': sessions[-1].isoformat(),
                'remaining_sessions': len(remaining), 'sessions': len(sessions), 'month_bound': True,
                'next_entry_session': attention if remaining and attention <= sessions[-1].isoformat() else None,
                'status': 'active' if remaining else 'finished'}
    except ValueError:
        return {'start': None, 'deadline': None, 'remaining_sessions': None,
                'sessions': ASTRA_HOLD, 'status': 'calendar_pending', 'next_entry_session': None}


def month_sessions_left(market, now):
    """本月（北京时间自然月）尚未走完的交易日数：月度200美元目标只算本月能兑现的计划。"""
    from exchange_sessions import is_session, latest_completed
    try:
        completed = latest_completed(market, now)
        today = now.astimezone(BJT).date()
        end = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        day, n = completed + timedelta(days=1), 0
        while day <= end:
            n += bool(is_session(day, market)); day += timedelta(days=1)
        return n
    except (ValueError, TypeError):
        return None


def us_floor(source, now):
    """US reference needs >=80 and the top 5% of current US scored names."""
    from strategy_board import score, business
    values = sorted(score(q, '2A', '')['value'] for q in source.get('rows', [])
                    if q.get('market') == '美股' and business(q) and q.get('structure'))
    if not values: return US_ABSOLUTE_FLOOR
    return max(US_ABSOLUTE_FLOOR, values[min(len(values) - 1, int(len(values) * .95))])


def regime_summary(envs):
    """当前各市场宽度与可推形态，用于"为什么今天没有"的正文说明。"""
    from shadow_screen import breadth_band
    parts = []
    for m in MARKETS:
        b = ((envs or {}).get(m) or {}).get('breadth') or {}
        v = b.get('value'); band = breadth_band(v)
        if band is None: parts.append(f'{m}宽度未知'); continue
        from shadow_screen import playbook
        parts.append(playbook(m, v)['text'])
    return '；'.join(parts)


def horizon_row(row):
    """由周期证据表入选的行：好股只按 grade_ladder.fundamentals_state 判（与3A/2A/1A同门），不再叠加综合分门。"""
    return (row.get('astra_regime') or {}).get('band') == 'horizon' or row.get('fundamentals_state') in ('good', 'pending')


def profit_gaps(row, horizon=None):
    out = []
    net = (row.get('net_target_return_pct') or [None])[0]
    hz = horizon if horizon is not None else horizon_row(row)
    if net is None: out.append('缺中期价格计划（入场/止盈/止损）')
    # 证据表命中：目标%与胜率已按规则回测（如美股+6%/10日规则），不再叠加旧的6%净空间门。
    elif net < MIN_NET_TARGET_PCT and not hz: out.append(f'净目标空间{net:g}%，低于月目标所需{MIN_NET_TARGET_PCT:g}%')
    if not hz and row['strategy_score']['value'] < MIN_SCORE:
        out.append(f'综合分{row["strategy_score"]["value"]:g}，低于Astra质量门槛{MIN_SCORE}')
    return out


def contribution(row):
    """Risk-sized payoff: profit at target = net RR x fixed per-trade risk."""
    rr = row.get('net_rr'); entry = row.get('entry_range'); stop = row.get('stop_range')
    directive = row.get('directive') or {}
    p = directive.get('price') or (entry or [None,None])[-1]
    s = directive.get('stop') or row.get('stop'); t = directive.get('target') or row.get('target')
    if all(isinstance(x,(float,int)) and x > 0 for x in (p,s,t)) and s < p < t:
        rr = ((t/p-1)*100-.5)/((1-s/p)*100+.5)
    scale=1/3 if (row.get('astra_regime') or {}).get('weak') else .5 if row.get('fundamentals_state') == 'pending' else 1
    risk_budget = RISK_PER_TRADE_USD * scale
    per_share = round(entry[1] - stop[0], 4) if entry and stop else None
    profit = round(rr * risk_budget, 1) if rr and rr > 0 else None
    return {'risk_per_trade_usd': risk_budget, 'monthly_target_usd': MONTHLY_TARGET_USD,
            'profit_at_target_usd': profit,
            'target_coverage_pct': round(profit / MONTHLY_TARGET_USD * 100, 1) if profit else None,
            'per_share_risk': per_share,
            'sizing_rule': f'股数 = {RISK_PER_TRADE_USD:g}美元等值风险 ÷ 每股风险（入场上沿−止损下沿）；港股/A股按整手向下取整，不为凑手数移动止损',
            'scope': '连续股数风险情景上限；未核整手、现金、汇率及费用，非可执行股数或已实现收益'}


def month_status(base, now=None):
    plan = read_json(Path(base) / 'data/astra_plan_pub.json')
    policy = plan.get('policy') or {}
    state = {}
    if now:
        from astra_plan import month_state, latch_month
        state = latch_month(month_state(read_json(Path(base)/'data/astra_trade_ledger.json'), now=now),
                            read_json(Path(base)/'data/astra_plan.json'), now.astimezone(BJT).strftime('%Y-%m'))
    return {'monthly_target_usd': policy.get('monthly_target', MONTHLY_TARGET_USD),
            'monthly_risk_cap_usd': MONTHLY_RISK_CAP_USD, 'risk_per_trade_usd': RISK_PER_TRADE_USD,
            'max_new_entries': 2, 'ledger_state': state.get('state') or plan.get('ledger_state') or 'LEDGER_UNVERIFIED',
            'realized_net_usd': state.get('realized_net_pnl'),
            'remaining_entries': max(0,2-state['entry_count']) if state.get('entry_count') is not None else None,
            'remaining_risk_usd': state.get('remaining_risk_budget'),
            'month_sessions_left': {m: month_sessions_left(m, now) for m in MARKETS} if now else None,
            'note': '实际净收益只按已对账成交计入；台账未对账时显示待对账，不计为0。两次止损或月风险用尽即停止新开仓。'}


def build(source, *, now=None, profiles=None, include_pool=False, envs=None):
    from strategy_board import candidate, classify, business, GRADE_POLICY
    from strategy_board_state import view as dated
    now = now or datetime.now(timezone.utc)
    anchor, following = cycle_dates(now)
    windows = {m: window(anchor, m, now) for m in MARKETS}
    floor_us = us_floor(source, now)
    from shadow_screen import entry_timing, plan_geometry, attach_rs, ASTRA_STOP_PCT, ASTRA_MIN_RR, EVIDENCE_BANDS
    envs = envs or {}
    attach_rs(source.get('rows', []))
    # 与 3A/2A/1A 同一好股门、同一张证据表（含排名席：本市场当轮按固定排序键前3）。
    from grade_ladder import fundamentals_state, FUND_DATA_GAPS, PENDING_NOTE
    from horizon_book import rank_pool, SETUP_NAMES
    from entry_progression import location
    rank_pool(source.get('rows', []), ok=lambda q: fundamentals_state(q, now)[0] != 'bad' and not location(q)['blocked'],
              breadth={m: ((v or {}).get('breadth') or {}).get('value') for m, v in envs.items()} if envs else None)
    rows = []
    month_left_by = {}
    for q in source.get('rows', []):
        if q.get('market') not in MARKETS: continue
        if not q.get('structure'): continue
        fstate, _ = fundamentals_state(q, now)
        if fstate == 'bad': continue
        env = envs.get(q['market']) or {}
        breadth = (env.get('breadth') or {}).get('value')
        tm = entry_timing(q, breadth, bool(env.get('bottom_rebound')), tiers=('Astra',))
        # 证据表命中即以形态名为车道；旧 classify 车道只用于未命中证据表的研究候选。
        cell = (tm['regime'].get('cell') or [None] * 4) if tm['ok'] else None
        lanes = [SETUP_NAMES.get(cell[3], cell[3])] if cell else [lane for tier, lane, _ in classify(q)]
        if not lanes: continue
        row = candidate(q, '2A', lanes[0], f'本轮{ASTRA_HOLD}个交易日内；到期复盘', now=now, plan_tier='ASTRA')
        # This is an unfinished Astra candidate, not a 2A execution contract.
        # Only check quote facts here; decide Astra after its own plan and gates.
        row = dated({'grade_policy': GRADE_POLICY, 'rows': [row]}, now,
                    annotate_directives=False)['rows'][0]
        if not row['current_source']: continue
        row['fundamentals_state'] = fstate
        if fstate == 'pending':
            row['missing'] = [m for m in row['missing'] if not str(m).startswith(FUND_DATA_GAPS)]
            row['sizing_note'] = PENDING_NOTE
        row['source_strategy'] = '2A'
        e=q.get('structure') or {}
        row['tracking_anchor']={'session':e.get('source_asof'),'close':e.get('reference_close'),'source':e.get('provider_source'),'basis':e.get('basis')}
        row['role'] = '中港研究' if row['market'] != '美股' else '美股参考'
        row['window'] = windows[row['market']]
        row['industry_snapshot'] = deepcopy((profiles or {}).get(row['code'], {}))
        bottom_gap = None
        if tm['ok']:
            # 与 3A/2A/1A 同一张周期证据表的 Astra 规则计划：止损＝max(10日低×0.99, 入场×0.92)，目标%与持有天数按规则。
            from horizon_book import apply_plan
            apply_plan(row, tm['plan'])
            row['window'] = window(anchor,row['market'],now,hold=tm['plan']['hold'])
            row['period'] = f'{tm["plan"]["hold"]}个交易日内（一个月内回报）'
        # Time feasibility screen: a target outside sqrt(remaining)*ATR is a
        # distant scenario. This is a product heuristic, not a probability.
        atr = (q.get('structure') or {}).get('atr14')
        remaining = row['window']['remaining_sessions']
        month_left = month_left_by.setdefault(row['market'], month_sessions_left(row['market'], now))
        hold = tm['plan']['hold'] if tm['ok'] else ASTRA_HOLD
        horizon = min(x for x in (month_left, hold) if x is not None) if remaining else None
        row['month_sessions_left'] = month_left
        row['time_feasible'] = bool(atr and horizon and row.get('entry_range')
                                   and row.get('target') and row['target']-row['entry_range'][1] <= atr*horizon**.5)
        if not row['time_feasible']:
            row['missing'].append((f'目标须在本月底前可达（本月剩余{month_left}个交易日），当前不满足'
                                   if month_left is not None and remaining and month_left < remaining
                                   else f'{hold}日内目标可达性待证') if remaining else '本轮到期；只复盘，不新增')
        row['profit_gaps'] = profit_gaps(row, horizon=tm['ok']) + ([bottom_gap] if bottom_gap else [])
        pos = tm['position']; regime = tm['regime']
        row['astra_position'] = pos
        row['astra_regime'] = regime
        if regime['grade'] != 'buy':
            row['profit_gaps'].append('环境×形态：' + regime['reason'] + ('' if pos['ok'] else '｜位置：' + pos['reason']))
        fade = tm['fade']
        if fade:
            row['fade_risk'] = fade
            # 低宽度左侧里的"弱势未止跌"放行，但叠加年线下行照拦（shadow_screen.fade_exempt）；高位回落/追涨日照拦。
            if tm['fade_block']:
                row['profit_gaps'].append('推后即跌形态：' + fade)
        geo = plan_geometry(row, q.get('last'))
        if geo:
            stop_pct = geo['stop_pct']; bt_rr = geo['rr_backtest_basis']
            row['astra_stop_check'] = {'stop_pct': stop_pct, 'rr_backtest_basis': bt_rr,
                                       'band_pct': list(ASTRA_STOP_PCT), 'min_rr': ASTRA_MIN_RR}
            lo, hi = ASTRA_STOP_PCT
            if not lo <= stop_pct <= hi:
                row['profit_gaps'].append(f'止损距离{stop_pct:.1f}%不在{lo:g}-{hi:g}%（' + ('过近，落在日常波动内易被扫' if stop_pct < lo else '风险过大') + '）')
            if regime.get('band') not in EVIDENCE_BANDS and (bt_rr is None or bt_rr < ASTRA_MIN_RR):
                row['profit_gaps'].append(f'盈亏比{bt_rr or 0:.2f}<{ASTRA_MIN_RR:g}（现价入场、结构止损口径）')
        else:
            row['profit_gaps'].append('缺结构止损或目标，无法核止损距离与盈亏比')
        if any(m.startswith('近期公告含风险事项') for m in row['missing']): row['profit_gaps'].append('近期公告含风险事项，暂不进入Astra')
        row['profit_gate'] = not row['profit_gaps'] and row['time_feasible']
        e2 = q.get('structure') or {}
        row['setup'] = pos['state']
        row['missing'] = [m for m in row['missing'] if m != '结构净收益风险比不足1.5' and not m.startswith('上方阻力')] + row['profit_gaps']
        row['status'] = '研究候选' if row['missing'] else '条件匹配'
        row['rule_matched'] = not row['missing'] and row['market'] != '美股'
        row['target_contribution'] = contribution(row)
        if row['market'] == '美股' and not tm['ok'] and row['strategy_score']['value'] < floor_us: continue
        rows.append(row)
    def rank(r):
        # 证据表命中优先，信号越多越靠前。
        return (-int(r['rule_matched']), -int(r['profit_gate']), -len((r.get('astra_position') or {}).get('signals') or []),
                -r['strategy_score']['value'], r['code'])
    # Only names whose target, if reached, materially advances the monthly
    # goal take a seat; others are shown as candidates with their gaps.
    watch = sorted((r for r in rows if not r['profit_gate']), key=rank)
    rows = [r for r in rows if r['profit_gate']]
    # Keep a full-pool funnel before seat truncation; zero output must have
    # measurable coverage/filter evidence instead of an unexplained blank.
    from collections import Counter
    diagnostics = {'version': 'astra-funnel-2026-11-19-v1', 'markets': {}}
    for market in MARKETS:
        inputs = [q for q in source.get('rows', []) if q.get('market') == market]
        diagnostics['markets'][market] = {
            'quote_rows': len(inputs), 'with_structure': sum(bool(q.get('structure')) for q in inputs),
            'profit_gate_candidates': sum(r['market'] == market for r in rows),
            'filter_reasons': dict(Counter(g for r in watch if r['market'] == market
                                           for g in r.get('profit_gaps', []))),
            'participation': market != '美股'}
    eligible = deepcopy(rows) if include_pool else None
    from industry_diversity import select
    diverse=[]
    for market in MARKETS:
        included,reserved=select([r for r in rows if r['market']==market],3,profiles,key=rank,reason='同行业已有更优先的Astra候选')
        diverse+=included
        for r in reserved:
            if r.get('selection_gap'):
                r=dict(r,profit_gaps=list(r['profit_gaps'])+[r['selection_gap']]);watch.append(r)
    rows=diverse
    preferred = sorted((r for r in rows if r['market'] != '美股'), key=rank)
    # Prefer representation of both participation markets when available.
    chosen = [next(r for r in preferred if r['market'] == m) for m in ('A股', '港股')
              if any(r['market'] == m for r in preferred)]
    for r in preferred:
        if r not in chosen and len(chosen) < 3: chosen.append(r)
    if len(chosen) < 3:
        chosen += sorted((r for r in rows if r['market'] == '美股'), key=rank)[:1]
    chosen.sort(key=lambda r: (r['market'] == '美股', *rank(r)))
    for r in rows:
        if r not in chosen:
            gap = '本轮Astra 3席已由中港合格者占满（美股仅补空位），条件已全部满足，仅列观察'
            watch.append(dict(r, selection_gap=gap, profit_gaps=list(r['profit_gaps']) + [gap]))
    for n, row in enumerate(chosen, 1): row['rank'] = n
    near = []
    for market in ('A股', '港股', '美股'):
        for r in [r for r in watch if r['market'] == market and r.get('selection_gap')] + [r for r in watch if r['market'] == market and not r.get('selection_gap')][:2]:
            near.append({k: r.get(k) for k in ('code', 'name', 'market', 'last', 'quote_asof', 'strategy_score',
                         'entry_range', 'take_profit_range', 'stop_range', 'net_rr', 'net_target_return_pct',
                         'stop_loss_pct', 'profit_gaps', 'missing', 'business_reason', 'astra_regime', 'stop_anchor', 'astra_stop_check')})
    result = {'version': VERSION, 'generated_at': now.isoformat(), 'cycle_id': anchor.isoformat(),
            'next_recalculation': following.isoformat(), 'holding_sessions': ASTRA_HOLD,
            'recalculation_days': [1,10,20], 'recalculation_timezone': 'Asia/Shanghai',
            'windows': windows, 'rows': chosen, 'model_calls': 0, 'watch': near,
            'selection_policy': SELECTION_POLICY | {'us_floor_this_run': floor_us},
            'rule_matched_count': sum(r['rule_matched'] for r in chosen),
            'status': '研究清单已更新' if chosen else '本轮无买入级候选：' + regime_summary(envs),
            'market_breadth': {m: (envs.get(m) or {}).get('breadth') for m in MARKETS},
            'scope': '每月1、10、20自然日重算，休市也更新，每日复核；规则筛选分≠GPT审核分或胜率。原15交易日目标跟踪窗口独立保留，非盈利承诺。',
            'account_boundary': '月度200美元实际净收益目标及原风险约束独立保留；未对账不计零，不以情景利润当已实现。',
            'new_order_authority': False}
    result['selection_diagnostics'] = diagnostics
    if include_pool: result['_eligible_rows'] = eligible
    return result


def read_json(path):
    try: return json.loads(path.read_text())
    except FileNotFoundError: return {}


def merge_history(left, right):
    """Deploying newer local inputs must not discard cloud-only past cycles."""
    result = deepcopy(left) or {'version': VERSION, 'cycles': {}}
    key = lambda v: datetime.fromisoformat(v)
    for cycle_id, incoming in right.get('cycles', {}).items():
        if cycle_id not in result['cycles']:
            result['cycles'][cycle_id] = deepcopy(incoming)
            continue
        target = result['cycles'][cycle_id]
        incoming_plan = incoming.get('fixed_plan') or {}
        prior_plan = target.get('fixed_plan') or {}
        if incoming_plan and (not prior_plan or key(incoming_plan['updated_at']) > key(prior_plan['updated_at'])):
            target['fixed_plan'] = deepcopy(incoming_plan)
        target['created_at'] = min(target['created_at'], incoming['created_at'], key=key)
        for code, first in incoming.get('first', {}).items():
            prior = target.setdefault('first', {}).get(code)
            if not prior or key(first['at']) < key(prior['at']): target['first'][code] = deepcopy(first)
        for ident, record in incoming.get('recommendations', {}).items():
            prior = target.setdefault('recommendations', {}).get(ident)
            if prior is None: target['recommendations'][ident] = deepcopy(record)
            else:
                if key(record['at']) < key(prior['at']): prior['at'],prior['row'] = record['at'],deepcopy(record['row'])
                prior['seen_dates'] = sorted(set(prior.get('seen_dates',[])+record.get('seen_dates',[])))
                # Desktop merges the archive without rerunning calendar.update.
                # Retain actual publication days, never infer them from seen_dates.
                if 'published_days' in prior or 'published_days' in record:
                    prior['published_days'] = sorted(set(prior.get('published_days', []) + record.get('published_days', [])))
                if key((record.get('outcome') or {}).get('checked_at',record['at'])) > key((prior.get('outcome') or {}).get('checked_at',prior['at'])):
                    prior['outcome'] = deepcopy(record.get('outcome',{}))
        unique = {json.dumps(s, sort_keys=True, ensure_ascii=False): s
                  for s in target.get('snapshots', []) + incoming.get('snapshots', [])}
        target['snapshots'] = sorted(unique.values(), key=lambda s: key(s['generated_at']))
    times = [v for v in (left.get('generated_at'), right.get('generated_at')) if v]
    if times: result['generated_at'] = max(times, key=key)
    # Derived calendar records are rebuilt from the union of immutable snapshots.
    return result


def live_note(row):
    """⚠️ 行与 📌 指令必须同一口径。

    指令层已经给出买入级动作时，逆风/板块轮转只是这张条件单的成交条件
    （已写进指令的 condition），不能同时在旁边说"建议了结/不应续持"——
    一只没有持仓的研究标的更不存在"了结"。只有指令层判不买时，才按原文
    给出不应续持的结论。
    """
    advice = row.get('live_advice') or '建议了结/不应续持'
    if ((row.get('directive') or {}).get('action') or '不买') == '不买':
        return advice
    reasons = (row.get('live_gate') or {}).get('reasons') or []
    return ('新规则·立刻生效（已并入上方指令的成交条件，不是了结建议）：'
            + '；'.join(reasons) if reasons else advice)


def apply_live_gate(doc, source, base, now=None):
    """Astra 立刻生效的更严复核：逆风/板块轮转向下/跌破入选线/计划退化，
    与影子层 shadow_screen 同一套口径。给每只持仓加 live_gate 标记并在建议里点名；
    任何异常都保持原判（fail-open），绝不影响 20 日周期记账。"""
    summary = {'flagged': 0, 'total': 0}

    def block(row, reasons):
        row['live_gate'] = {'hold': False, 'reasons': reasons}
        row['live_advice'] = '建议了结/不应续持（新规则·立刻生效）：' + '；'.join(reasons)
        if row.get('rule_matched'):
            row.update(rule_matched=False, buyable=False, status='原计划研究 · 等待')
            row['missing'] = list(dict.fromkeys([*(row.get('missing') or []), *('入场门：' + x for x in reasons)]))
        summary['flagged'] += 1
    try:
        import shadow_screen as _sh
        from strategy_board import trend_leader, HEADWIND_TRIAL_NOTE
        envs = _sh.market_envs(base)
        quads = _sh.industry_quadrants(source.get('rows', []), base)
        ind_map = _sh.load_industry(base)
        src_by_code = {q.get('code'): q for q in source.get('rows', [])}
        means = _sh._market_r20_means(source.get('rows', []))
        summary['envs'] = {m: (v or {}).get('env') for m, v in envs.items()}
        import earnings_risk as _er
        calendar = _er.load_calendar(base)
        today = (now or datetime.now(timezone.utc)).date()
        trials = []
        headwind_reason = '大盘逆风（指数在MA60下方且MA20下行），环境门不发买入级'
        for row in doc.get('rows', []):
            reasons = []
            code = row.get('code'); market = row.get('market')
            score = (row.get('strategy_score') or {}).get('value')
            env = (envs.get(market) or {}).get('env')
            # 周期证据表已判 buy 的（回测样本已含逆风期），不再由 HEADWIND 另行否决。
            if env == 'headwind' and (row.get('astra_regime') or {}).get('grade') != 'buy':
                reasons.append(headwind_reason)
            if isinstance(score, (int, float)) and score < _sh.ASTRA_MIN_SCORE and not horizon_row(row):
                reasons.append(f'当前分{score:g}<入选线{_sh.ASTRA_MIN_SCORE:g}' + ('，稳定层续持' if row.get('plan_retained') else ''))
            ns = row.get('plan_current_net_space_pct'); rr = row.get('plan_current_net_rr')
            # 不再用 6%/2 的安全边际拦截，只有目标已到（净空间≤0）才算计划退化。
            if isinstance(ns, (int, float)) and round(ns, 2) <= 0:
                reasons.append(f'净空间{ns:.1f}%≤0（目标已到，计划退化）')
            q = src_by_code.get(code)
            eq = None
            if q is not None:
                ind = ind_map.get(code)
                quad = (quads.get((market, ind)) or {}).get('quad')
                if quad == _sh.ROT_BLOCK_QUAD and _sh.near_high(q) is True:
                    reasons.append(f'板块「{ind}」短期转弱且近高位，板块轮转向下')
                eq = _sh.entry_quality(q, env or 'neutral', means.get(market, 0.0))
                if eq['peak_block']:
                    reasons.append('追高熔断：' + '；'.join(eq['peak_reasons']))
            # 财报门（与 3A 同一份已核日历）：20 日波段不跨财报持有——ISRG 盘后跳空就是这个坑。
            verdict = _er.assess(row, today=today, calendar=calendar)
            deadline = _er._as_date((row.get('window') or {}).get('deadline'))
            when = _er._as_date(verdict.get('earnings_date'))
            if when and deadline and today - timedelta(days=1) <= when <= deadline and verdict['state'] != 'blackout':
                verdict['state'] = 'blackout'
                verdict['reasons'] = [f'已核财报日 {when.isoformat()} 落在本轮持有期（截止 {deadline.isoformat()}）内；波段不跨财报持有']
            row['earnings_risk'] = verdict
            if verdict['state'] in ('blackout', 'stale'):
                reasons.append('财报门：' + '；'.join(verdict['reasons'])[:120])
            summary['total'] += 1
            # 与 3A 入场门同一裁决：命中即收回条件匹配（不再只是"建议"而 rule_matched
            # 照旧为真）；只因逆风被拦、且是趋势延续领头股的，按 3A 同一名额规则半仓试仓。
            if (reasons == [headwind_reason] and row.get('rule_matched') and eq is not None
                    and trend_leader(row.get('last'), q, eq)):
                trials.append(row); continue
            if reasons:
                block(row, reasons)
            else:
                row['live_gate'] = {'hold': True, 'reasons': []}
                if verdict['state'] == 'window':
                    row['live_gate']['earnings_half'] = True
        for market in {r['market'] for r in trials}:
            pool = sorted((r for r in trials if r['market'] == market),
                          key=lambda r: -((r.get('strategy_score') or {}).get('value') or 0))
            used = []
            for row in pool:
                ind = ind_map.get(row.get('code'))
                used.append(ind or row.get('code'))
                row['live_gate'] = {'hold': True, 'trend_trial': True, 'reasons': [headwind_reason],
                                    'note': HEADWIND_TRIAL_NOTE}
                row['status'] = '条件匹配'
                summary['trials'] = summary.get('trials', 0) + 1
        doc['rule_matched_count'] = sum(bool(r.get('rule_matched')) for r in doc.get('rows', []))
    except Exception as e:
        summary['error'] = str(e)
    return summary


def refresh(*, base=BASE, now=None, source=None):
    from review_factpack import atomic_json
    from market_watch import build as scan
    from platform_lock import flock, LOCK_EX
    base = Path(base); data = base/'data'; data.mkdir(exist_ok=True, parents=True)
    now = now or datetime.now(timezone.utc)
    source = source if source is not None else scan(base=base, now=now, all_quotes=True)
    board = read_json(data/'strategy_board.json')
    from stock_profile_view import load as load_profiles
    profile_doc=load_profiles(data/'stock_profiles_pub.json')
    profiles=dict(profile_doc.get('records') or {})
    for r in board.get('rows',[]):profiles.setdefault(r['code'],r.get('industry_snapshot',{}))
    from shadow_screen import market_envs
    try: envs = market_envs(base)
    except Exception: envs = {}
    from shadow_screen import market_breadth
    for m, b in market_breadth(source.get('rows'), base).items():
        envs.setdefault(m, {})['breadth'] = b
    doc = build(source, now=now, profiles=profiles, include_pool=True, envs=envs)
    codes = [r['code'] for r in doc['rows'] + doc.get('watch', [])]
    previous = read_json(data/'astra_cycle.json')
    if previous.get('cycle_id') == doc['cycle_id']:
        codes = list(dict.fromkeys(codes + [r['code'] for r in previous.get('rows', [])]))
    if codes and os.environ.get('V88_SKIP_FILING_CHECK') != '1':
        try:
            from filing_check import refresh as check_filings, load as load_filings
            check_filings(codes, base=base, now=now)
            records = load_filings(base).get('records', {})
            for q in source.get('rows', []):
                if q['code'] in records: q['filings'] = records[q['code']]
            doc = build(source, now=now, profiles=profiles, include_pool=True, envs=envs)
        except Exception as exc:
            doc['filing_check_error'] = type(exc).__name__
    doc['month_status'] = month_status(base, now)
    bt = read_json(data/'astra_backtest.json')
    if bt.get('profit_gate_only'):
        doc['backtest'] = {'generated_at': bt.get('generated_at'), 'symbols': bt.get('symbols'),
                           'by_market': bt['profit_gate_only'], 'scope': '仅价格/技术规则回顾（基本面非PIT不参与），当前流动性股票池（含幸存者偏差），约14个月；非样本外证明、非收益预测。'}
    with (data/'.astra_cycle.lock').open('a') as lease:
        flock(lease, LOCK_EX)
        history = read_json(data/'astra_cycle_history.json') or {'version': VERSION, 'cycles': {}}
        cycle = history['cycles'].setdefault(doc['cycle_id'], {'created_at': now.isoformat(), 'snapshots': [], 'first': {}})
        doc['created_at'] = cycle['created_at']
        from astra_cycle_stability import stabilize
        doc['_market_envs'] = envs
        doc = stabilize(doc, cycle, source, now, profiles)
        from strategy_multicheck import enrich
        enrich(doc,source,base=base,now=now,previous=previous,module="Astra")
        doc['live_gate'] = apply_live_gate(doc, source, base, now)
        from astra_cycle_stability import entry_first
        try:
            def gate(d):
                # 候选与在席者走同一套：多重核查 + 实时门，不给换入者开捷径。
                enrich(d, source, base=base, now=now, previous=previous, module="Astra")
                apply_live_gate(d, source, base, now)
            swaps = entry_first(doc, cycle, source, now, profiles, gate)
            if swaps:
                doc['live_gate']['entry_first_swaps'] = swaps
                doc['rule_matched_count'] = sum(bool(r.get('rule_matched')) for r in doc['rows'])
        except Exception as exc:
            doc['live_gate']['entry_first_error'] = f'{type(exc).__name__}: {exc}'
        doc.pop('_astra_pool', None); doc.pop('_market_envs', None)
        doc['market_bottom_rebound'] = {m: bool((v or {}).get('bottom_rebound')) for m, v in envs.items()}
        try:
            from action_directive import annotate
            annotate(doc, 'Astra', now=now)
        except Exception as exc:
            doc['directive_error'] = f'{type(exc).__name__}: {exc}'
        # 日历/记录只收对外真正推出的（今日可买 / 1–3日内可买 / 启动跟踪），在席但被拦的不算推荐。
        try:
            from action_directive import publishable
            from astra_cycle_state import view as _state_view
            shown = {r['code'] for r in publishable(_state_view(deepcopy(doc), now), 'Astra', now=now).get('rows', [])}
        except Exception as exc:
            shown = set(); doc['publish_mark_error'] = f'{type(exc).__name__}: {exc}'
        for row in doc['rows']: row['published'] = row['code'] in shown
        doc['rule_matched_count'] = sum(bool(r.get('rule_matched')) for r in doc['rows'])
        doc['month_status']['published_count'] = len(shown)
        diagnostics = doc['selection_diagnostics']
        diagnostics.update(zero_publication=not bool(shown), entry_first_scan=doc.get('entry_first_scan'),
                           migration_count=sum(bool(r.get('directive_migration')) for r in doc['rows']))
        for market, counts in diagnostics['markets'].items():
            members = [r for r in doc['rows'] if r.get('market') == market]
            counts.update(selected=len(members), published=sum(r['published'] for r in members),
                          exclusion_reasons={r['code']: (r.get('entry_window_qualification') or {}).get('reasons', [])
                                             for r in members if not r['published']})
        for r in doc['rows']:
            r['target_contribution'] = contribution(r)
        # At most two participation entries; US is reference only. No 3-name sum.
        scenarios=sorted(((r.get('target_contribution') or {}).get('profit_at_target_usd') or 0
                          for r in doc['rows'] if r['published'] and r.get('market') != '美股'),reverse=True)
        slots=doc['month_status'].get('remaining_entries')
        slots=2 if slots is None else slots
        upper=round(sum(scenarios[:slots]),1)
        doc['month_status'].update(published_profit_at_target_usd=upper,
            scenario_scope='最多两次开仓的连续股数情景上限，未核整手与可用现金，非收益承诺',
            scenario_shortfall_usd=round(max(0,MONTHLY_TARGET_USD-upper),1))
        for row in doc['rows']:
            first = cycle['first'].setdefault(row['code'], {'at': now.isoformat(), 'row': deepcopy(row)})
            row['first_seen_at'] = first['at']
        snapshot = {k: deepcopy(doc[k]) for k in ('generated_at', 'rows', 'status', 'rule_matched_count')}
        if not cycle['snapshots'] or cycle['snapshots'][-1]['rows'] != snapshot['rows']:
            cycle['snapshots'].append(snapshot)
        from astra_calendar import update as calendar_update
        doc['calendar_records'] = calendar_update(history,base,now)
        history['generated_at'] = now.isoformat()
        atomic_json(data/'astra_cycle_history.json', history)
        doc['history'] = {k: {'created_at': v['created_at'], 'first': v['first'],
                              'updates': len(v['snapshots'])} for k, v in history['cycles'].items()}
        atomic_json(data/'astra_cycle.json', doc)
    return doc


def digest_lines(doc, *, markets=None, now=None):
    from astra_cycle_state import view
    from screening_clock import display
    from action_directive import publishable
    doc = publishable(view(doc, now), 'Astra', now=now)
    scope = ' / '.join(markets) if markets else '全市场'
    if not doc.get('version'):
        return [f'🎯 Astra｜{scope} · 轮次待核 · 研究数量待核 / 条件匹配待核',
                '📊 规则回顾：数据待同步；不能把模块缺数据记为0只。']
    if markets:
        doc['rows'] = [r for r in doc.get('rows',[]) if r.get('market') in markets]
        doc['watch'] = [r for r in doc.get('watch',[]) if r.get('market') in markets]
        doc['calendar_records'] = [r for r in doc.get('calendar_records',[]) if (r.get('row') or {}).get('market') in markets]
    matched = sum(bool(r.get('rule_matched')) for r in doc.get('rows',[]))
    lines = [f'🎯 Astra｜{doc.get("cycle_id", "轮次待核")}轮 · {scope} · 15交易日 · 研究{len(doc.get("rows", []))}只 / 条件匹配{matched}只',
             f'🔄 下轮 {doc.get("next_recalculation", "待核")}；原计划与实际持仓继续跟踪',
             '🧮 Astra筛选：'+display(doc.get('generated_at'))+' 北京时间']
    if doc.get('plan_stability_version'):
        lines.append(f'🔒 本轮固定研究计划 v{doc.get("plan_revision", 0)}；现价与条件更新，原入场/止盈/止损/期限保持。研究退出不代表自动卖出。')
        changes = [c for c in doc.get('plan_changes', []) if c.get('action') in ('added', 'exited')
                   and (not markets or c.get('market') in markets)]
        for change in changes[-3:]:
            lines.append(f'↳ {display(change.get("at"))} {change.get("name", change.get("code"))}：{change.get("reason", "")[:90]}')
    if markets == ('美股',):lines.append('美股为参考计划；本轮无入选也保留轮次与回顾，不借用中港名单。')
    ms = doc.get('month_status') or {}
    if ms:
        realized = '待对账' if ms.get('realized_net_usd') is None else f'{ms["realized_net_usd"]:g}美元'
        lines.append(f'💰 月目标{ms.get("monthly_target_usd",200):g}美元 · 已实现{realized} · 单笔风险{ms.get("risk_per_trade_usd",62.5):g}美元 · 每月最多{ms.get("max_new_entries",2)}次新开仓')
    for market in (markets or ('all',)):
        bt = ((doc.get('backtest') or {}).get('by_market') or {}).get(market) or {}
        label = market if market != 'all' else '全市场'
        if bt.get('trades'):
            lines.append(f'📊 {label}规则回顾{bt["trades"]}次：触及目标{bt["target_hit_pct"]:g}% · 止损{bt["stop_pct"]:g}% · 平均{bt["mean_r"]:+g}R（技术面历史回顾，非样本外证明、非实际盈利）')
        else:lines.append(f'📊 {label}规则回顾：统计待同步；不按0次或0收益处理。')
    audit = ((doc.get('directive_summary') or {}).get('audit') or {}).get('text')
    if audit: lines.append(audit)
    mb = doc.get('market_breadth') or {}
    if mb:
        lines.append('🌡️ 环境（市场宽度=收盘>MA60个股占比）：' + regime_summary({m: {'breadth': v} for m, v in mb.items()})
                     + '｜买入级只给周期证据表Astra规则（前后两半段回测胜率各≥53%），非保证')
    if not doc.get('rows'): lines.append('本轮Astra 0只买入级：' + (regime_summary({m: {'breadth': v} for m, v in mb.items()}) if mb else '市场宽度未取得') + '；见下方候补的具体卡点。')
    for r in doc.get('rows', []):
        c = r.get('target_contribution') or {}
        lines.append(f'{r["name"]} {r["code"]} · 现价{r.get("last")} · {r["strategy_score"]["value"]:g}分 · {r["status"]} · 截止{(r.get("window") or {}).get("deadline") or "待核"}'+(f' · 年度{r["annual_label"]}（20日波段，与3A中长期分开）' if r.get('annual_label') else ''))
        if r.get('buy_window') or r.get('astra_position'):
            lines.append(f"  {r.get('buy_window') or ''}｜{(r.get('astra_position') or {}).get('state', '')}：{(r.get('astra_position') or {}).get('reason', '')}")
        if r.get('astra_regime'):
            lines.append('  🌡️ ' + r['astra_regime'].get('reason', ''))
        sa = r.get('stop_anchor') or {}
        if sa.get('price'):
            sc = r.get('astra_stop_check') or {}
            lines.append(f"  🛑 止损锚：{sa['type']} {sa['price']:g}（{sa.get('date') or '日期待核'}）下方0.25ATR={sa['stop_level']:g}"
                         + (f"，距现价{sc['stop_pct']:g}%、盈亏比{sc.get('rr_backtest_basis')}" if sc.get('stop_pct') is not None else '')
                         + f"｜{sa['validation']}")
        if (r.get('directive') or {}).get('text'):
            from action_directive import directive_label
            lines.append(f'  📌 {directive_label(r)}｜'+r['directive']['text'])
        if (r.get('live_gate') or {}).get('hold') is False:
            lines.append('  ⚠️ '+live_note(r))
        elif (r.get('live_gate') or {}).get('trend_trial'):
            lines.append('  🚀 '+(r['live_gate'].get('note') or '逆风试仓·半仓'))
        lines.append('  币种 '+(r.get('currency') or {'A股':'CNY','港股':'HKD','美股':'USD'}.get(r.get('market'),'待核'))+
                     '｜报价 '+(display(r['quote_asof'])+' 北京时间' if r.get('quote_asof') else '待核；非实时报价')+
                     '｜补发沿用此真实报价时点，不以发送时间替代')
        if r.get('entry_range'):
            lines.append(f'↳ 原计划入场{r["entry_range"]} 止盈{r.get("take_profit_range")} 止损{r.get("stop_range")} · 原计划情景净空间{(r.get("net_target_return_pct") or ["?"])[0]}% · 情景盈亏比{r.get("net_rr")}'
                         + (f' · 止盈约+{c["profit_at_target_usd"]:g}美元（月目标{c["target_coverage_pct"]:g}%）' if c.get('profit_at_target_usd') else ''))
            if r.get('plan_current_net_space_pct') is not None:
                lines.append(f'↳ 原计划目标{r.get("plan_current_structure_target")} · 当前净空间{r["plan_current_net_space_pct"]:g}% · 当前净盈亏比{r.get("plan_current_net_rr")}（与计划止损同口径）｜第一上方结构位{r.get("plan_current_nearest_resistance")}；非原计划止盈承诺')
        from strategy_multicheck import brief
        lines.append('  '+brief(r))
        from muse_review import collect
        collect(r, 'Astra', lines)
    for r in doc.get('watch', [])[:3]:
        lines.append(f'候补 {r["name"]} {r["code"]} {r["strategy_score"]["value"]:g}分：' + '；'.join(r.get('profit_gaps') or r.get('missing') or [])[:60])
    hits=[r for r in doc.get('calendar_records',[]) if r.get('cycle_id')==doc.get('cycle_id') and (r.get('outcome') or {}).get('status')=='target_touched']
    names=list(dict.fromkeys(r['row']['name'] for r in hits))
    lines.append('📅 月度留档 · 20日内触及目标（行情） '+( '、'.join(names[:3]) if names else '暂无已核实记录')+'；非实际盈利')
    return lines


def public_projection(doc, safe):
    keys = ('code','name','market','last','quote_asof','source_session','strategy_score','reason',
            'entry_location','price_volume','decision_policy','monthly_entry_deadline',
            'business_reason','business_risk','business_supported','missing','status','entry_range',
            'crosscheck','financial_evidence','target_evidence','price_plan_method',
            'take_profit_range','stop_range','net_rr','net_target_return_pct','stop_loss_pct',
            'fee_allowance_pct','price_plan_scope','price_plan_source_asof','price_plan_basis','period','role','window',
            'time_feasible','rule_matched','rank','first_seen_at','current_source',
            'profit_gate','profit_gaps','target_contribution','business_source','fundamentals','filing_check','structure_source','setup','annual_regime','annual_label',
            'plan_id','plan_created_at','plan_revision','plan_change_reason','plan_state','plan_retained',
            'plan_recheck_at','plan_waiting_reasons','plan_exit_reason','plan_exited_at',
            'plan_current_net_space_pct','plan_current_net_rr','plan_current_structure_target',
            'plan_current_first_obstacle_pct','plan_current_nearest_resistance',
            'live_gate','live_advice','directive','astra_position','fade_risk','published','month_sessions_left',
            'astra_regime','stop_anchor','astra_stop_check')
    def clean(row):
        out = {k: deepcopy(row[k]) for k in keys if k in row}
        if 'directive' in out:
            from action_directive import public_directive
            out['directive'] = public_directive(out['directive'], 'Astra')
        score = row.get('strategy_score') or {}
        out['strategy_score'] = {k: score[k] for k in ('value','coverage','revision','interaction','penalty','label','model_review','snapshot_id','asof') if k in score}
        out['strategy_score']['factors'] = [{k:f[k] for k in ('factor','weight','value','contribution') if k in f}
                                             for f in score.get('factors',[])]
        if row.get('plan_selection_score'):
            out['plan_selection_score'] = {k:row['plan_selection_score'][k] for k in ('value','revision','asof') if k in row['plan_selection_score']}
        info = row.get('industry_snapshot') or {}
        out['industry_snapshot'] = {k: info[k] for k in ('industry','rank','comparable','source_session','historical') if k in info}
        out['industry_snapshot']['peers'] = [{k:p[k] for k in ('code','name','rank') if k in p}
                                             for p in info.get('peers',[]) if safe(p)]
        return out
    result = {k: deepcopy(doc[k]) for k in ('version','generated_at','created_at','cycle_id',
        'next_recalculation','holding_sessions','recalculation_days','recalculation_timezone','windows','scope','status','model_calls','new_order_authority','selection_policy','month_status','backtest',
        'plan_stability_version','plan_revision','live_gate','market_bottom_rebound','market_breadth') if k in doc}
    result['plan_changes'] = [{k:deepcopy(c[k]) for k in ('at','revision','action','code','name','market','reason','plan_id') if k in c}
                              for c in doc.get('plan_changes', []) if safe(c)]
    result['exited_rows'] = [clean(r) for r in doc.get('exited_rows', []) if safe(r)]
    result['watch'] = [clean(r) for r in doc.get('watch',[]) if safe(r)]
    result['rows'] = [clean(r) for r in doc.get('rows',[]) if safe(r)]
    if result.get('month_status'):
        # The public site cannot receive private account P&L/budget/entry counts.
        for key in ('realized_net_usd','remaining_entries','remaining_risk_usd'):
            result['month_status'][key]=None
        result['month_status']['private_progress_redacted']=True
    from action_directive import public_summary
    summary = public_summary(doc.get('directive_summary'))
    if summary: result['directive_summary'] = summary
    result['rule_matched_count'] = sum(r.get('rule_matched',False) for r in result['rows'])
    result['history'] = {k: {'created_at': v.get('created_at'), 'updates': v.get('updates'),
        'first': {code: {'at': f['at'], 'row': clean(f['row'])} for code,f in v.get('first',{}).items() if safe(f['row'])}}
        for k,v in doc.get('history',{}).items()}
    result['calendar_records'] = []
    for record in doc.get('calendar_records',[]):
        if not safe(record['row']): continue
        from astra_calendar import record_shown
        if not record_shown(record): continue
        outcome=record.get('outcome') or {}
        result['calendar_records'].append({k:deepcopy(record[k]) for k in ('id','cycle_id','at','seen_dates','published_days') if k in record} | {
            'row':clean(record['row']), 'outcome':{k:deepcopy(outcome[k]) for k in ('status','label','checked_at','actual_profit','scope','reason','source_sha256','target','stop','date','observed_high','observed_low','through','recheck_note') if k in outcome}})
    return result
