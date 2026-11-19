"""Keep a round's research contracts stable while rechecking current facts.

This registry changes research membership only. It has no trade/order authority.
"""
from copy import deepcopy
from datetime import datetime
import hashlib
import json
import math
import re

VERSION = 'astra-fixed-cycle-v2-native-directive'
PLAN_FIELDS = ('entry_range', 'take_profit_range', 'stop_range', 'stop', 'target',
               'net_rr', 'net_target_return_pct', 'target_return_pct', 'stop_loss_pct',
               'fee_allowance_pct', 'price_plan_scope', 'price_plan_version',
               'price_plan_method', 'price_plan_source_asof', 'period', 'tracking_anchor',
               'invalidation', 'source_strategy', 'role', 'setup', 'target_contribution')
PLAN_GAPS = {'现价未进入计划区间，待触发且不追价', '入场 / 目标 / 失效结构',
             '结构净收益风险比不足1.5', '止盈 / 止损区间依据', '入场 / 止盈 / 止损区间无可行顺序'}
GOVERNANCE = re.compile(r'立案|调查|处罚|退市|终止上市|风险警示|无法表示意见|保留意见|违约|债务逾期|破产|清盘|'
                        r'investigation|enforcement|delist|bankruptcy|winding.?up|going concern', re.I)


def plan_identity(row, cycle_id):
    facts = {k: row.get(k) for k in PLAN_FIELDS}
    facts.update(code=row['code'], market=row['market'], cycle_id=cycle_id,
                 window={k: (row.get('window') or {}).get(k) for k in ('start', 'deadline', 'sessions')})
    return hashlib.sha256(json.dumps(facts, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:24]


def waiting(row, reason, now):
    row = deepcopy(row)
    row.update(rule_matched=False, buyable=False, executable=False, current_source=False,
               status='原计划保留 · 数据待核', plan_state='waiting', plan_retained=True,
               plan_recheck_at=now.isoformat(), plan_waiting_reasons=[reason])
    row['missing'] = list(dict.fromkeys([*row.get('missing', []), reason]))
    return row, None


def repair_intermediate_directive(row, now):
    """Migrate only the known pre-gate 2A rejection, during full Astra refresh.

    A genuine Astra veto, armed contract, unknown-version decision, or a terminal
    plan remains untouched. This never runs in the read-only publication path.
    Original prices, deadlines, identity and historical snapshots are immutable.
    """
    d = row.get('directive') or {}
    marker = '本轮未经入场门评估（追高/盈亏比/财报窗未核），不给买价'
    allowed = {marker, '结构净收益风险比不足1.5'}
    why = d.get('why') or []
    if not (row.get('source_strategy') == '2A' and row.get('window')
            and row.get('astra_regime') and row.get('plan_state') == 'active'
            and d.get('version') == 'action-directive-2026-11-16-v10-entry-window'
            and d.get('module') == '2A' and d.get('action') == '不买'
            and marker in why and set(why) <= allowed
            and all(d.get(k) is None for k in ('price', 'order', 'valid_until', 'stop', 'target'))):
        return False
    row['directive_migration'] = {'version': VERSION, 'at': now.isoformat(),
        'reason': '未完成Astra候选误带2A前置拒绝；按原Astra价位重走完整审核',
        'original': deepcopy(d)}
    for k in ('directive', 'execution_readiness', 'entry_window_qualification',
              'three_a_qualification', 'buy_window'):
        row.pop(k, None)
    return True


def recheck(original, quote, now, envs=None):
    """Never reuse the new price plan's entry status for the old price bands."""
    from astra_cycle import profit_gaps, contribution, MIN_NET_TARGET_PCT
    from strategy_board import candidate, GRADE_POLICY
    from strategy_board_state import view
    from filing_check import fresh
    from exchange_sessions import latest_completed, is_session
    from market_session_view import session_view
    from datetime import date, timedelta
    if not quote:
        return waiting(original, '本轮原标的缺少最新源数据，保留原计划待核', now)
    try:
        current = candidate(quote, '2A', original.get('lane', ''), original.get('period', ''), now=now, plan_tier='ASTRA')
        current = view({'grade_policy': GRADE_POLICY, 'rows': [current]}, now,
                       annotate_directives=False)['rows'][0]
    except (KeyError, TypeError, ValueError, ZeroDivisionError):
        return waiting(original, '本轮原标的数据不完整，保留原计划待核', now)
    row = deepcopy(original)
    repair_intermediate_directive(row, now)
    # Only facts/diagnostics come from the new candidate, never its computed plan.
    for field in ('name', 'last', 'currency', 'change_pct', 'quote_asof', 'source_session',
                  'source_sha256', 'strategy_score', 'reason', 'business_reason', 'business_risk',
                  'business_source', 'business_supported', 'fundamentals', 'filing_check',
                  'structure_source', 'annual_regime', 'annual_label', 'annual_metrics',
                  'buy_timing', 'current_source', 'model_review', 'applicable_periods'):
        if field in current: row[field] = deepcopy(current[field])
    if quote.get('currency'): row['currency'] = quote['currency']
    missing = [g for g in current.get('missing', [])
               if g not in PLAN_GAPS and not g.startswith('上方阻力')]
    filing = quote.get('filings') or {}
    exit_reason = None
    governance = [h for h in filing.get('hits', []) if GOVERNANCE.search(str(h.get('title', '')))]
    if fresh(filing, now) and filing.get('status') == 'conflict' and governance:
        titles = '；'.join(str(h.get('title', ''))[:40] for h in governance[:2])
        exit_reason = '已核治理风险，退出本轮研究：' + titles
    elif (quote.get('fundamentals') or {}).get('status') == 'counterevidence':
        exit_reason = '最新财报明确企业反证，退出本轮研究；原计划继续留档'
    else:
        # 与入选同一好股门（grade_ladder.fundamentals_state）：资料缺失＝基本面待补，不在复核时再挡一次。
        from astra_cycle import horizon_row
        from grade_ladder import fundamentals_state, FUND_DATA_GAPS
        fstate, fnote = fundamentals_state(quote, now)
        # 公告冲突在上方按治理/非治理细分（治理退出，其余等待），这里只处理财务类硬伤。
        if horizon_row(original) and fstate == 'bad' and fnote != '近期公告含风险事项':
            exit_reason = f'好股门复核未过（{fnote}），退出本轮研究并让出席位；原计划留档，非卖出指令'
        elif horizon_row(original) and fstate != 'bad':
            missing = [g for g in missing if not str(g).startswith(FUND_DATA_GAPS)]
            row['fundamentals_state'] = fstate
        elif not current.get('business_supported'):
            missing.append('原企业依据待复核，缺证不等于企业逻辑已失效')
    # 席位资格逐日复核，与 3A research_roster 同一规则、同一常量：完整数据下当前分
    # 跌破入选线即退出；数据不完整则缓冲 STALE_REVIEW_DAYS 天后让席。否则一只
    # 50 分的旧计划可以整轮占着 60 分线的席位，合格新标的永远进不来。
    from research_roster import _complete, _session_gap, STALE_REVIEW_DAYS
    from astra_cycle import MIN_SCORE, horizon_row
    score = (current.get('strategy_score') or {}).get('value')
    try:
        session = latest_completed(row['market'], now).isoformat()
    except (KeyError, TypeError, ValueError):
        session = None
    if session and _complete(current):
        row['plan_last_complete_review'] = session
        if isinstance(score, (int, float)) and score < MIN_SCORE and not horizon_row(row):
            exit_reason = exit_reason or (f'完整交易日评审：当前统一分{score:g}低于Astra入选线{MIN_SCORE}，退出本轮研究并让出席位'
                                          '（与3A跌破80即退出同一规则）；原计划留档，非卖出指令')
    elif session:
        gap = _session_gap(session, original.get('plan_last_complete_review') or original.get('plan_created_at'))
        if gap > STALE_REVIEW_DAYS:
            exit_reason = exit_reason or (f'连续{gap}天拿不到完整结构数据，无法确认仍达Astra入选线'
                                          f'（残分{score}仅因数据缺失，不作合格或淘汰依据）；席位不被无法验证的标的冻结，原计划留档，非卖出指令')
    window = deepcopy(original.get('window') or {})
    try:
        start, end = date.fromisoformat(window['start']), date.fromisoformat(window['deadline'])
        completed = latest_completed(row['market'], now)
        remaining = sum(is_session(start + timedelta(days=n), row['market']) and start + timedelta(days=n) > completed
                        for n in range((end - start).days + 1))
        window.update(remaining_sessions=remaining, status='active' if remaining else 'finished')
        attention = session_view(row['market'], now)['attention_session']
        window['next_entry_session'] = attention if remaining and attention <= end.isoformat() else None
        if not remaining:
            window['next_entry_session'] = None
            exit_reason = exit_reason or '原计划已到期，退出本轮新增研究；不延长原期限'
    except (KeyError, TypeError, ValueError):
        remaining = None
        missing.append('原计划交易日窗口待核')
    row['window'] = window
    # Preserve the original research contract; new-entry deadline is separately month-bound.
    from entry_progression import location
    row['entry_location'] = location(quote)
    row['price_volume'] = (quote.get('structure') or {}).get('price_volume') or {}
    month_end=(now.astimezone(__import__('zoneinfo').ZoneInfo('Asia/Shanghai')).date().replace(day=28)+timedelta(days=4)).replace(day=1)-timedelta(days=1)
    row['monthly_entry_deadline'] = min(window.get('deadline') or month_end.isoformat(), month_end.isoformat())
    entry, target, stops = row.get('entry_range'), row.get('take_profit_range'), row.get('stop_range')
    price = row.get('last')
    valid = bool(entry and target and stops and len(entry) == len(target) == len(stops) == 2
                 and all(type(x) in (int, float) and math.isfinite(x) and x > 0 for x in [*entry, *target, *stops])
                 and stops[0] <= stops[1] < entry[0] < entry[1] < target[0] <= target[1])
    if not valid: missing.append('原入场 / 止盈 / 止损区间待核')
    elif not isinstance(price, (int, float)) or not entry[0] <= price <= entry[1]:
        missing.append('现价未进入原计划区间，等待触发，不追价')
    if valid and isinstance(price, (int, float)) and price <= stops[0]:
        missing.append('最新报价触及原计划失效位，禁止条件匹配')
        if current.get('current_source') and current.get('buy_timing') != 'stale':
            exit_reason = exit_reason or '最新有效报价触及原计划失效位，退出本轮研究；非自动卖出指令'
    if valid and type(price) in (int, float) and math.isfinite(price) and price > 0:
        ceiling = max(entry[1], price)
        structure = quote.get('structure') or {}
        levels = [structure.get(k) for k in ('previous_high20', 'high60', 'high120', 'high52')]
        # 以实际参考入场价为界，不是现价：低于参考入场价的旧高点是脚下的支撑，
        # 不是上方阻力。
        levels = sorted(x for x in levels if type(x) in (int, float) and math.isfinite(x) and x >= ceiling)
        # 收益腿与风险腿必须同口径，都走完整的原计划区间。把收益腿压到最近一档
        # 阻力、风险腿仍算到完整止损，是主榜 opportunity() 犯过的同一个量纲错误：
        # 上升趋势股头顶永远有近期高点，净盈亏比因此几乎不可能达标，Astra 也就
        # 长期交白卷。最近阻力不丢弃，单列披露并保留"追过头"的否决权。
        objective = target[0]
        fee = row.get('fee_allowance_pct', .5)
        space = (objective / ceiling - 1) * 100 - fee
        risk = (1 - stops[0] / ceiling) * 100 + fee
        rr = round(space / risk, 2) if risk > 0 else None
        obstacle = round((levels[0] / ceiling - 1) * 100 - fee, 2) if levels else None
        row.update(plan_current_net_space_pct=round(space, 2), plan_current_net_rr=rr,
                   plan_current_structure_target=round(objective, 4),
                   plan_current_first_obstacle_pct=obstacle,
                   plan_current_nearest_resistance=levels[0] if levels else None)
        if round(space, 2) < MIN_NET_TARGET_PCT: missing.append('原计划当前净空间不足Astra原门槛，等待复核')
        if obstacle is not None and obstacle <= 0:
            missing.append('现价已站在最近上方结构位之上，属追过头而非空间，等待复核')
    atr = (quote.get('structure') or {}).get('atr14')
    from astra_cycle import month_sessions_left
    month_left = month_sessions_left(row['market'], now)
    horizon = min(x for x in (remaining, month_left) if x is not None) if remaining is not None else None
    row['month_sessions_left'] = month_left
    row['time_feasible'] = bool(valid and atr and horizon and row.get('target')
                               and row['target'] - entry[1] <= atr * horizon ** .5)
    if not row['time_feasible']:
        missing.append(f'目标须在本月底前可达（本月剩余{month_left}个交易日），当前不满足' if month_left is not None and remaining and month_left < remaining
                       else '原计划在剩余窗口内的目标可达性待证')
    # 席位只留「本周能触发、本月能兑现」的计划：最新有效报价下，目标在剩余期内不可达，
    # 或现价距入场带超出 ATR14×√min(5,剩余日)（一周正常波动够不着），即退出让席。
    # 这是波动尺度的可达性筛选，不是概率承诺；过期报价只标注不踢人。
    live = bool(current.get('current_source') and current.get('buy_timing') != 'stale')
    week = min(5, remaining) if remaining else None
    if valid and atr and week and isinstance(price, (int, float)):
        dist = entry[0] - price if price < entry[0] else price - entry[1] if price > entry[1] else 0.0
        reach = atr * week ** .5
        row['week_trigger'] = {'gap': round(dist, 4), 'atr14': atr, 'sessions': week,
                               'reach': round(reach, 4), 'ok': dist <= reach}
        if dist > reach:
            missing.append(f'现价距入场带{dist:.4g}，超出{week}个交易日正常波动（ATR14×√{week}≈{reach:.4g}），本周难触发')
            if live:
                exit_reason = exit_reason or (f'现价距入场带超出{week}个交易日正常波动，本周难以触发；'
                                              '席位让给本周可触发者，原计划留档，非卖出指令')
    if live and valid and atr and horizon and not row['time_feasible']:
        exit_reason = exit_reason or (f'目标在剩余{horizon}个交易日内超出正常波动可达范围，本月难兑现；'
                                      '席位让给可兑现者，原计划留档，非卖出指令')
    row['profit_gaps'] = profit_gaps(row)
    # 入场证据只在入选时裁决一次：按周期证据表（horizon_book）入选的，之后涨向目标属计划兑现，不因形态消失被踢；
    # 旧规则（左侧启动/底部确认/强势延续矩阵）留下的在席股按同一 entry_timing（Astra 口径）复核一次，不合格即退出。
    from shadow_screen import entry_timing, EVIDENCE_BANDS
    env = (envs or {}).get(row['market']) or {}
    if ((original.get('astra_regime') or {}).get('band') not in EVIDENCE_BANDS
            and (env.get('breadth') or {}).get('value') is not None):
        tm = entry_timing(quote, env['breadth']['value'], bool(env.get('bottom_rebound')), tiers=('Astra',))
        row['astra_regime'] = tm['regime']; row['astra_position'] = tm['position']; row['setup'] = tm['position']['state']
        if not tm['ok']:
            row['profit_gaps'].append('周期证据表：' + tm['regime']['reason'])
            exit_reason = exit_reason or (f"周期证据表：{tm['regime']['reason']}；旧规则入选、按统一证据表复核不成立，"
                                          '退出本轮研究，原计划留档；非自动卖出指令')
    # 推后即跌形态每轮按最新行情重算：在席股转入下跌途中/冲高回落即不对外推送（不硬写）。
    from shadow_screen import fade_risk, fade_exempt, annual_down
    fade = fade_risk(quote)
    # 与入选同一规则（fade_exempt 唯一口径）。
    reg = row.get('astra_regime') or {}
    if fade_exempt(fade, reg.get('grade'), annual_down(quote, row), reg.get('band') in EVIDENCE_BANDS):
        fade = None
    if fade:
        row['fade_risk'] = fade
        row['profit_gaps'].append('推后即跌形态：' + fade)
        # 高位回落/弱势未止跌＝启动失败，让出席位给当日合格的新启动股；追涨日只是今天不买。
        if not fade.startswith('追涨日'):
            exit_reason = exit_reason or f'推后即跌形态：{fade}；启动失败，退出本轮研究并让出席位；原计划留档，非自动卖出指令'
    else:
        row.pop('fade_risk', None)
    missing += row['profit_gaps']
    if not current.get('current_source'): missing.append('原计划最新源行情待核')
    row['missing'] = list(dict.fromkeys(missing))
    row['profit_gate'] = bool(not row['profit_gaps'] and row['time_feasible'] and not exit_reason
                              and row.get('plan_current_net_space_pct', float('-inf')) >= MIN_NET_TARGET_PCT)
    row['rule_matched'] = bool(not row['missing'] and not exit_reason and row['market'] != '美股')
    row['buyable'] = row['rule_matched']
    row['executable'] = False
    row['status'] = '条件匹配' if row['rule_matched'] else '原计划研究 · 等待'
    row.update(plan_state='active' if row['rule_matched'] else 'waiting', plan_retained=True,
               plan_recheck_at=now.isoformat(), plan_waiting_reasons=row['missing'])
    row['target_contribution'] = contribution(row)
    return row, exit_reason


def stabilize(doc, cycle, source, now, profiles=None):
    """Fill explicit vacancies; never displace a retained name because of rank."""
    from industry_diversity import selection_industry
    registry = cycle.setdefault('fixed_plan', {'version': VERSION, 'revision': 0,
                                               'active_rows': [], 'changes': [], 'exited_rows': []})
    quotes = {r['code']: r for r in source.get('rows', [])}
    def event(row, action, reason):
        registry['revision'] += 1
        change = {'at': now.isoformat(), 'revision': registry['revision'], 'action': action,
                  'code': row['code'], 'market': row['market'], 'name': row['name'],
                  'reason': reason, 'plan_id': row['plan_id']}
        registry['changes'].append(change)
        row.update(plan_revision=registry['revision'], plan_change_reason=reason)
        return row
    def admit(row, action, reason, at):
        row = deepcopy(row)
        row.update(plan_id=plan_identity(row, doc['cycle_id']), plan_created_at=at,
                   plan_selection_score=deepcopy(row.get('strategy_score')), plan_retained=True)
        return event(row, action, reason)
    # Upgrade takes over the last actually published plan, not the old first
    # lineup or today's newly ranked replacement. Historical proposals stay put.
    if not registry['revision']:
        previous = next((s for s in reversed(cycle.get('snapshots', [])) if s.get('rows')), None)
        if previous:
            registry['active_rows'] = [admit(r, 'established', '接管本轮既有研究计划，原价格与期限保持', previous['generated_at'])
                                       for r in previous['rows']]
    active = []
    for prior in registry['active_rows']:
        row, exit_reason = recheck(prior, quotes.get(prior['code']), now, doc.get('_market_envs'))
        if exit_reason:
            row = event(row, 'exited', exit_reason)
            row.update(plan_state='exited', plan_exit_reason=exit_reason, plan_exited_at=now.isoformat(),
                       status='已退出本轮研究 · 原计划留档', rule_matched=False, buyable=False)
            registry['exited_rows'].append(row)
        else: active.append(row)
    excluded = {r['code'] for r in registry['exited_rows']}
    candidates = doc.pop('_eligible_rows', doc.get('rows', []))
    candidates = sorted(candidates, key=lambda r: (r['market'] == '美股',
                         -int(r['rule_matched']), -len((r.get('astra_position') or {}).get('signals') or []),
                         -r['strategy_score']['value'], r['code']))
    doc['_astra_pool'] = candidates  # 供实时门之后的「入场优先换席」使用；发布前移除
    # If initially empty, preserve the existing CN/HK representation preference.
    if not active:
        preferred = [next((r for r in candidates if r['market'] == market and r['code'] not in excluded), None)
                     for market in ('A股', '港股')]
        candidates = [r for r in preferred if r] + [r for r in candidates if r not in preferred]
    establishing = not registry['revision']
    for candidate in candidates:
        if len(active) >= 3: break
        if candidate['code'] in excluded or any(r['code'] == candidate['code'] for r in active): continue
        if candidate['market'] == '美股' and any(r['market'] == '美股' for r in active): continue
        industry = selection_industry(candidate, profiles)
        if industry and any(r['market'] == candidate['market'] and selection_industry(r, profiles) == industry for r in active): continue
        action = 'established' if establishing else 'added'
        reason = '本轮首次合格研究入选，固定原价格与期限' if action == 'established' else '本轮研究空位获新合格证据，新增独立原计划'
        row = admit(candidate, action, reason, now.isoformat())
        row, exit_reason = recheck(row, quotes.get(row['code']), now, doc.get('_market_envs'))
        # All candidates already passed the admission gate. Recheck defensively
        # before publication; no partial/invalid source creates a new seat.
        if exit_reason:
            registry['changes'].pop(); registry['revision'] -= 1
            continue
        active.append(row)
    for rank, row in enumerate(active, 1): row['rank'] = rank
    registry.update(active_rows=deepcopy(active), updated_at=now.isoformat())
    occupied = {r['code'] for r in active} | excluded
    doc['watch'] = [r for r in doc.get('watch', []) if r['code'] not in occupied]
    doc.update(rows=active, rule_matched_count=sum(bool(r['rule_matched']) for r in active),
               plan_stability_version=VERSION, plan_revision=registry['revision'],
               plan_changes=deepcopy(registry['changes']), exited_rows=deepcopy(registry['exited_rows']))
    if active: doc['status'] = '本轮原研究计划持续跟踪；现价与条件已复核，原价格及期限保持'
    return doc


ENTRY_FIRST_MAX_TRIALS = 6
ENTRY_FIRST_SECONDS = 30.0


def entry_first(doc, cycle, source, now, profiles, gate, *, max_trials=ENTRY_FIRST_MAX_TRIALS,
                seconds=ENTRY_FIRST_SECONDS, clock=None):
    """入场优先换席（与 3A research_roster 同一规则）：席位被实时门判为现在不买
    （逆风/计划退化/追高/跌破入选线），而本轮合格池里有人按原门槛与同一实时门
    现在就条件匹配时，立即让位——否则系统算出了可买却展示不可买。被让位者原
    计划留档并继续计入本轮记录；研究替换不代表卖出。gate(doc) 必须就地施加实时门。"""
    from industry_diversity import selection_industry
    from action_directive import priority, decide_astra
    prio = lambda r: priority(r, 'Astra', now=now)
    registry = cycle.get('fixed_plan') or {}
    pool = doc.get('_astra_pool') or []
    rows = doc.get('rows') or []
    # 席位排序 = 操作指令等级（0 立即买入 … 3 不买），与页面/飞书展示的指令同一口径。
    if not pool or not registry or not any(prio(r) > 0 for r in rows): return []
    quotes = {r['code']: r for r in source.get('rows', [])}
    excluded = {r['code'] for r in registry.get('exited_rows', [])}
    swaps = []
    # 每次试换都要完整跑多重核查＋实时门（较重）；候选池已按质量排序，只试前几名并限时，
    # 以免拖垮整轮刷新（云端步骤 90 秒上限）。未试到的候选仍在观察池，下一轮继续。
    clock = clock or __import__('time').monotonic
    started, tried, stopped = clock(), 0, None
    for candidate in pool:
        weak = [r for r in rows if prio(r) > 0]
        if not weak: break
        code, market = candidate['code'], candidate['market']
        if market == '美股' or code in excluded or any(r['code'] == code for r in rows): continue
        if tried >= max_trials: stopped = 'max_trials'; break
        if clock() - started > seconds: stopped = 'time_budget'; break
        tried += 1
        industry = selection_industry(candidate, profiles)
        same = [r for r in rows if r['market'] == market and industry and selection_industry(r, profiles) == industry]
        trial = deepcopy(candidate)
        trial.update(plan_id=plan_identity(trial, doc['cycle_id']), plan_created_at=now.isoformat(),
                     plan_selection_score=deepcopy(trial.get('strategy_score')), plan_retained=True)
        trial, exit_reason = recheck(trial, quotes.get(code), now, doc.get('_market_envs'))
        if exit_reason: continue
        # 与已在席的条件匹配者一起过实时门：候选必须过，且不得挤掉已在席者的试仓名额。
        matched = [deepcopy(r) for r in rows if r.get('rule_matched')]
        probe = {'rows': matched + [trial]}
        gate(probe)
        if not all(r.get('rule_matched') for r in probe['rows'][:-1]): continue
        trial = probe['rows'][-1]
        rank = prio(trial)
        if rank >= 3: continue
        victims = [r for r in (same or weak) if prio(r) > rank]
        if same and not victims: continue
        if not victims: continue
        victim = max(victims, key=lambda r: (prio(r), -((r.get('strategy_score') or {}).get('value') or 0), r['code']))
        if trial.get('rule_matched'):
            state = '逆风试仓（半仓）' if (trial.get('live_gate') or {}).get('trend_trial') else '条件匹配'
        else:
            state = decide_astra(trial, now=now)['action']
        victim_why = decide_astra(victim, now=now)
        reason = (f"入场优先：{trial.get('name')}（统一分{(trial.get('strategy_score') or {}).get('value')}）"
                  f"按Astra原门槛与实时门现在{state}，原席{victim.get('name')}操作指令更弱"
                  f"（{(victim_why['action'] + '：' + '；'.join(victim_why['why']))[:60]}），"
                  "立即让位；原计划留档，非卖出指令")
        registry['revision'] = registry.get('revision', 0) + 1
        for row, action in ((victim, 'exited'), (trial, 'added')):
            registry.setdefault('changes', []).append({'at': now.isoformat(), 'revision': registry['revision'],
                                                       'action': action, 'code': row['code'], 'market': row['market'],
                                                       'name': row.get('name'), 'reason': reason, 'plan_id': row.get('plan_id')})
            row.update(plan_revision=registry['revision'], plan_change_reason=reason)
        victim.update(plan_state='exited', plan_exit_reason=reason, plan_exited_at=now.isoformat(),
                      status='已退出本轮研究 · 原计划留档', rule_matched=False, buyable=False)
        registry.setdefault('exited_rows', []).append(deepcopy(victim))
        excluded.add(victim['code'])
        rows[rows.index(victim)] = trial
        swaps.append({'exit': victim['code'], 'enter': code, 'reason': reason})
    doc['entry_first_scan'] = {'pool': len(pool), 'tried': tried, 'stopped': stopped,
                               'max_trials': max_trials, 'seconds': seconds}
    if swaps:
        rows.sort(key=lambda r: (prio(r), r['market'] == '美股'))
        for rank, row in enumerate(rows, 1): row['rank'] = rank
        registry.update(active_rows=deepcopy(rows), updated_at=now.isoformat())
        occupied = {r['code'] for r in rows} | excluded
        doc['watch'] = [r for r in doc.get('watch', []) if r['code'] not in occupied]
        doc.update(rows=rows, rule_matched_count=sum(bool(r.get('rule_matched')) for r in rows),
                   plan_revision=registry['revision'], plan_changes=deepcopy(registry['changes']),
                   exited_rows=deepcopy(registry['exited_rows']))
    return swaps
