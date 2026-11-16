"""3A/2A/1A 统一分级：好股 × 进入时机 × 获利机会，按持有周期分档（2026-11-11 用户：3A＝中长期稳定、利润回报稳定，
行情好或拐点多时多；2A＝中短线稳定；1A＝超短线；Astra＝一个月内回报）。

与 Astra 同一入口（shadow_screen.entry_timing → horizon_book 周期证据表），不另设第二套逻辑：
  好股   ：fundamentals_state 唯一口径——明确硬伤（财报反证、基本面分<60、已核利润不为正、近期公告风险）拒绝；
           企业依据＋利润质量认证＋基本面≥60 为"好股"；资料缺失不再当硬伤（2026-11-11 用户：大胆一点，
           A股八成个股缺财报接入，被"企业依据不足"整体挡掉），放行并标"基本面待补"、仓位减半；
  时机   ：命中 horizon_book 证据表——宽度格子或排名席（本市场当轮按规则排序键前3），
           每条规则前后两半段回测胜率各≥53%（2026-11-12 用户：现在可以买入的概率低于50%就是逻辑问题）；
  获利机会：按命中规则的回测口径定价格计划——止损按档：
    3A ＝max(20日低×0.99, 入场×0.92)，5–8%；2A/Astra ＝10日低×0.99，5–8%；1A ＝5日低×0.99，2–8%；
    目标为固定百分比、持有天数按规则（如 A股低宽度低位企稳：3A 40日+12%、2A 15日+10%、1A 5日+5%）。
  同一只股票多档命中取 3A>2A>1A；止损距离不在区间则该档不成立（不硬凑）。
  学习型候选（2026-11-11 用户：每天不能推零只）：某市场正式可买不足3只时补足到3只，轻仓跟踪复盘，不出买入指令。
"""
from collections import Counter
from copy import deepcopy

# 分级行标识（下游指令/审核/日报/验证按此识别，保持不变）；逻辑版本见 LADDER_VERSION。
LADDER_POLICY = 'grade-ladder-v1'
LADDER_VERSION = 'horizon-book-v6-entry-window-before-seats'
GOOD_FLOOR = 60.0
TIER_BUSINESS = {'3A': GOOD_FLOOR, '2A': GOOD_FLOOR, '1A': GOOD_FLOOR}
SEATS = 3
TIERS3 = ('3A', '2A', '1A')
SCOPE = ('云端 screen-history 日线回测（信号日2026-01-19~11-11），以2026-06-20切前后两半段，两半段胜率各≥53%才入表；'
         '当前成分股与优质池含幸存者偏差；基本面无历史时点数据，未参与回测；非收益保证')


def breadth_envs(fresh, base):
    from shadow_screen import market_breadth, market_envs
    try:
        envs = {m: dict(v or {}) for m, v in market_envs(base).items()}
    except Exception:
        envs = {}
    try:
        for m, b in market_breadth(fresh, base).items():
            envs.setdefault(m, {})['breadth'] = b
    except Exception:
        pass
    return envs


RADAR_SEATS = 3
LEARN_SEATS = 3
LEARN_BUSINESS = GOOD_FLOOR


def radar_item(q, env, why):
    """底部雷达：出现底部确认形态但未给买入级的股票（观察，不是推荐），写明差什么。"""
    from shadow_screen import bottom_confirm
    bc = bottom_confirm(q)
    if not bc:
        return None
    return {'code': q['code'], 'name': q.get('name'), 'market': q.get('market'), 'last': q.get('last'),
            'low': bc['low'], 'stop': bc['stop'], 'stop_pct': bc['stop_pct'], 'drawdown_pct': bc['drawdown_pct'],
            'bounce_pct': bc['bounce_pct'], 'age': bc['age'], 'above_ma20': bc['above_ma20'], 'ma20': bc['ma20'],
            'quality': bc['quality'], 'pullback_price': bc['pullback_price'], 'gap': why}


PENDING_NOTE = '基本面待补：无公告原文/数据商财报支持或利润质量未认证，按技术形态＋回测入选，仓位≤正常1/2'
# 资料缺失类缺项（不是硬伤）：基本面待补的行不因这些缺项挡买。
FUND_DATA_GAPS = ('非技术推荐依据', '利润质量：', '最新公告冲突核验')


def fundamentals_state(q, now=None):
    """好股门唯一口径：返回 ('good' | 'pending' | 'bad', 说明)。分级、学习席、Astra、状态重算、独立验证共用。"""
    from business_support import business
    from earnings_quality import certified, VERSION as EQ_VERSION, number
    f = (q or {}).get('fundamentals') or {}
    bs = f.get('business_score')
    eq = f.get('earnings_quality') or {}
    if f.get('status') == 'counterevidence':
        return 'bad', '财报为反证（亏损/大幅下滑）'
    if isinstance(bs, (int, float)) and bs < GOOD_FLOOR:
        return 'bad', f'企业基本面分{bs:g}<{GOOD_FLOOR:g}'
    profit = eq.get('recurring_profit') if eq.get('status') == 'normalized' else eq.get('operating_profit') \
        if eq.get('status') == 'operating_verified' else None
    if eq.get('version') == EQ_VERSION and eq.get('entry_verified') is True and not eq.get('gaps') \
            and number(profit) and profit <= 0:
        return 'bad', '利润质量：已核经常性/经营利润不为正'
    filing = (q or {}).get('filings') or {}
    if filing.get('status') == 'conflict':
        try:
            from filing_check import fresh
            recent = now is None or fresh(filing, now)
        except Exception:
            recent = True
        if recent:
            return 'bad', '近期公告含风险事项'
    if business(q) and certified(eq) and isinstance(bs, (int, float)):
        return 'good', f'企业基本面{bs:g}、利润质量已认证'
    return 'pending', PENDING_NOTE


def grade(q, env, now, calendar=None, tiers=None):
    """单只股票的分级结论；有硬伤或无时机时返回 (None, 原因)。"""
    from strategy_board import candidate, classify
    from shadow_screen import entry_timing, plan_geometry
    from horizon_book import apply_plan, PLAN
    if not q.get('structure'):
        return None, '缺日线结构'
    fstate, fnote = fundamentals_state(q, now)
    if fstate == 'bad':
        return None, fnote
    f = q.get('fundamentals') or {}
    bs = f.get('business_score')
    breadth = (env.get('breadth') or {}).get('value')
    tm = entry_timing(q, breadth, bool(env.get('bottom_rebound')), tiers=tiers)
    if not tm['ok']:
        return None, '时机：' + tm['regime']['reason'][:60]
    lanes = [lane for _, lane, _ in classify(q)]
    r = candidate(q, '2A', lanes[0] if lanes else '质量趋势', '', now=now, plan_tier='ASTRA')
    missing = [m for m in r['missing'] if m != '结构净收益风险比不足1.5' and not m.startswith('上方阻力')]
    if fstate == 'pending':
        missing = [m for m in missing if not m.startswith(FUND_DATA_GAPS)]
    bad = next((m for m in missing if m.startswith(('利润质量', '近期公告含风险事项', '财报为反证'))), None)
    if bad:
        return None, bad[:40]
    hit = tm['hit']
    tier = hit['tier']
    r['missing'] = missing
    apply_plan(r, hit['plan'])
    missing = r['missing']
    if (r.get('entry_location') or {}).get('blocked'):
        missing.append(r['entry_location']['reason'])
    geo = plan_geometry(r, q['last'])
    if not geo or geo['rr_backtest_basis'] is None:
        return None, '缺结构止损或目标'
    notes = (['回测两半段按日t较小者<1.5，证据弱，轻仓（≤正常仓位1/3）'] if hit['weak'] else []) + \
        ([PENDING_NOTE] if fstate == 'pending' else [])
    if calendar is not None:
        try:
            from earnings_risk import assess
            from datetime import timezone
            verdict = assess(r, today=now.astimezone(timezone.utc).date(), calendar=calendar)
            r['earnings_risk'] = verdict
            if verdict['state'] in ('blackout', 'stale'):
                missing += verdict.get('reasons') or ['财报事件窗内']
        except Exception:
            pass
    n, kind = hit['plan']['hold'], PLAN[tier]['kind']
    exit_text = f'触及止损或目标即离场；最长{n}个交易日，到期收盘了结（时间止损）'
    state = tm['position']['state']
    ref, stop_px = geo['ref_price'], geo['stop_price']
    r.update(strategy_tier=tier, source_strategy=tier, lane=state + '·' + kind, tier_setup=state,
             period=f'{n}个交易日{kind}', missing=missing,
             grade_basis=LADDER_POLICY, quality_eligible=tier == '3A' and fstate == 'good', max_sessions=n,
             tier_exit_text=exit_text, exit_rule_text=exit_text,
             astra_position=tm['position'], astra_regime=tm['regime'], fade_risk=tm['fade'],
             astra_stop_check=geo | {'band_pct': [PLAN[tier]['stop_min'], 8.0]},
             ladder={'tier': tier, 'business_score': bs, 'ref_price': round(ref, 4), 'stop_pct': geo['stop_pct'],
                     'rr_backtest_basis': geo['rr_backtest_basis'], 'net_space_pct': geo['net_space_pct'],
                     'timing': tm['regime']['reason'], 'setup': state, 'hold_sessions': n, 'weak': hit['weak'],
                     'downgrade': notes, 'evidence': hit['evidence'], 'fundamentals': fstate,
                     'source': hit.get('source'), 'rank': hit.get('rank'), 't': hit.get('t'), 'rt': hit.get('rt'),
                     'win': hit.get('win'), 'win_halves': hit.get('win_halves'), 'target_pct': hit['plan'].get('target_pct'),
                     'also_tiers': [t for t in ('3A', '2A', '1A') if t != tier and t in tm['horizon']['hits']],
                     'why': (f"好股（{fnote}）" if fstate == 'good' else '基本面待补（轻仓）') +
                            f"＋时机（{tm['regime']['reason']}）"
                            f"＋获利机会（止损{geo['stop_pct']:g}%、目标{r['target']:g}、{n}日内）→ {tier}"})
    r['buyable'] = not missing
    r['status'] = '条件匹配' if r['buyable'] else '研究候选'
    r['selection_category'] = '买入条件已满足' if r['buyable'] else '最高价值研究'
    r['entry_gate'] = {'entry_state': '可入场' if r['buyable'] else '待核', 'stop': stop_px, 'reasons': [],
                       'net_rr': r.get('net_rr'), 'first_objective': r.get('target'),
                       'ma20': q['structure'].get('ma20'), 'atr14': q['structure'].get('atr14'),
                       'blocked': bool((r.get('entry_location') or {}).get('blocked'))}
    r['reason'] = r['ladder']['why']
    return r, None


def playbook_of(m, envs):
    from shadow_screen import playbook
    return playbook(m, ((envs.get(m) or {}).get('breadth') or {}).get('value'))


LEARN_BAD = ('企业基本面分', '利润质量', '近期公告含风险事项', '财报为反证')


def learning(fresh, final, envs, rejected, profiles=None):
    """学习型候选：某市场正式可买不足3只时补足到3只（2026-11-11 用户：每天不能推零只）。
    只取无硬伤的股票（fundamentals_state 非 bad，好股优先）；排序先看证据表形态（任一档命中的回测均值，
    形态命中但当前宽度无格子按0，无形态按-0.5%），兜底取站上MA5且5日上行者（标"无回测形态"）。
    止损按2A口径（10日低×0.99，不深于8%、不浅于5%），+10%目标、15日（与2A规则同口径）；轻仓跟踪复盘，不进入推荐行、不出买入指令。"""
    from strategy_board import MARKETS
    from industry_diversity import select
    from shadow_screen import fade_risk
    from horizon_book import match, setups, SETUP_NAMES, cell_text, HK_NOTE
    out = {}
    held = {r['code'] for r in final}
    for m in MARKETS:
        need = LEARN_SEATS - sum(bool(r.get('buyable')) for r in final if r['market'] == m)
        if need <= 0:
            continue
        breadth = ((envs.get(m) or {}).get('breadth') or {}).get('value')
        pool = []
        for q in fresh:
            if q.get('market') != m or q['code'] in held or not q.get('structure'):
                continue
            state, _ = fundamentals_state(q)
            bs = (q.get('fundamentals') or {}).get('business_score')
            why = rejected.get(q['code']) or ''
            if state == 'bad' or why.startswith(LEARN_BAD):
                continue
            e, last = q['structure'], q.get('last')
            if not isinstance(last, (int, float)) or last <= 0 or not isinstance(e.get('low10'), (int, float)):
                continue
            found = setups(q)
            hits = (match(q, breadth, ('3A', '2A', '1A', 'Astra')) or {}).get('hits') or {}
            best = max(hits.values(), key=lambda h: h['mean'], default=None)
            if not found:
                ma5, r5 = e.get('ma5'), e.get('return5_pct')
                if not (isinstance(ma5, (int, float)) and isinstance(r5, (int, float)) and last > ma5 and r5 > 0) \
                        or fade_risk(q):
                    continue
            stop = round(min(max(e['low10'] * 0.99, last * 0.92), last * 0.95), 4)
            risk = last - stop
            exp = best['mean'] if best else (0.0 if found else -0.5)
            pool.append({
                'code': q['code'], 'name': q.get('name'), 'market': m, 'last': last, 'quote_asof': q.get('quote_asof'),
                'currency': q.get('currency'), 'industry_snapshot': deepcopy((profiles or {}).get(q['code'], {})),
                'industry': q.get('industry'), 'business_score': bs, 'fundamentals': state,
                'pattern': SETUP_NAMES[best['setup']] if best else ('、'.join(SETUP_NAMES[x] for x in found) if found
                                                                   else '无回测形态'),
                'rs20_pct': e.get('rs20_pct'), 'stop': stop, 'stop_pct': round(risk / last * 100, 2),
                'target': round(last * 1.10, 4), 'hold_sessions': 15, 'sizing': '轻仓（≤正常仓位1/3）',
                'expectation': (cell_text(tuple(best['cell'])) if best else
                                '形态在当前宽度无回测格子' if found else '无回测形态，期望≈0') + '；仅学习跟踪，不是推荐',
                'why_not_formal': (HK_NOTE if m == '港股' else why) or '未过正式分级（时机或证据表格子不成立）',
                '_rank': (exp, state == 'good', bs or 0, e.get('rs20_pct') or 0)})
        chosen, _ = select(pool, need, profiles, key=lambda x: (tuple(-v for v in x['_rank']), x['code']))
        for x in chosen:
            x.pop('_rank', None)
            x.pop('industry_snapshot', None)
        if chosen:
            out[m] = chosen
    return out


def rank(r):
    """可买先于研究；好股先于基本面待补；规则两半段按日t较小者高者先；排名席名次靠前者先；再看基本面分、盈亏比。"""
    lad = r['ladder']
    pos = (lad.get('rank') or {}).get('rank') or 9
    return (not r['buyable'], lad.get('fundamentals') != 'good', -(lad.get('rt') or lad.get('t') or 0), pos,
            -(lad.get('business_score') or 0), -lad['rr_backtest_basis'], r['code'])


def window_eligible(row):
    return (row.get('entry_window_qualification') or {}).get('eligible') is True


def apply(doc, fresh, base, now, profiles=None):
    """Qualify each tier's complete pool before assigning its bounded seats.

    Evidence caches are loaded once per tier, including newly regraded overflow.
    A valid next-session plan competes even though it is not executable now.
    Old research rows may remain in the document archive; publication filters
    them independently and never lets them displace a current qualified row.
    """
    from industry_diversity import select
    from strategy_board import compact, MARKETS
    from shadow_screen import attach_rs
    from horizon_book import rank_pool
    attach_rs(fresh)
    envs = breadth_envs(fresh, base)
    from entry_progression import location
    rank_pool(fresh, ok=lambda q: fundamentals_state(q, now)[0] != 'bad' and not location(q)['blocked'],
              breadth={m: ((v or {}).get('breadth') or {}).get('value') for m, v in envs.items()})
    try:
        from earnings_risk import load_calendar
        calendar = load_calendar(base)
    except Exception:
        calendar = None
    graded, skipped, rejected, radar = [], {m: Counter() for m in MARKETS}, {}, {}
    funnel = {m: Counter() for m in MARKETS}
    for q in fresh:
        m = q.get('market')
        if m not in MARKETS:
            continue
        funnel[m]['scanned'] += 1
        try:
            r, why = grade(q, envs.get(m) or {}, now, calendar)
        except Exception as exc:
            r, why = None, f'分级计算异常：{type(exc).__name__}'
        if r is None:
            skipped[m][why] += 1
            rejected[q['code']] = why
        if r is None or not r['buyable']:
            try:
                item = radar_item(q, envs.get(m) or {}, why if r is None else '已分级但未满足：' + '；'.join(r['missing'])[:40])
            except Exception:
                item = None
            if item:
                radar.setdefault(m, []).append(item)
        if r is None:
            continue
        funnel[m]['graded_' + r['strategy_tier']] += 1
        funnel[m]['buyable_' + r['strategy_tier']] += int(r['buyable'])
        graded.append(r)
    seats, watch = {}, []
    by_code = {q.get('code'): q for q in fresh}
    # Preserve eligibility under the unchanged financial/price rules before the
    # time-specific projection turns closed-market buyable flags off. A 3A
    # trend/window failure can still compete under an independently hit 2A plan.
    financial_candidates = {id(r) for r in graded if r.get('buyable')}
    checked = set()
    preselection_errors = []
    for tier in TIERS3:
        pending = [r for r in graded if r['strategy_tier'] == tier and id(r) not in checked]
        if pending:
            batch = {'rows': pending}
            try:
                from strategy_multicheck import enrich
                from action_directive import annotate
                enrich(batch, {'rows': fresh}, base=base, now=now)
                annotate(batch, '3A', now=now, sentry=doc.get('promise_sentry'))
            except Exception as exc:
                reason = '选席前证据核验失败：' + type(exc).__name__
                preselection_errors.append({'tier': tier, 'reason': reason})
                for r in pending:
                    r['buyable'] = False
                    r['entry_window_qualification'] = {'eligible': False, 'label': '数据待核', 'reasons': [reason]}
            checked.update(id(r) for r in pending)
            for r in pending:
                # Raw candidate.buyable can already be false at a closed
                # exchange. Use the independently audited financial plan for
                # overflow, then apply the lower tier's own time/trend policy.
                d=r.get('directive') or {}
                original=d.get('signal_plan') or d
                if original.get('action'):
                    from action_directive import ARMED_ACTIONS, buy_window
                    financial_candidates.discard(id(r))
                    if original['action'] in ARMED_ACTIONS and buy_window(r,original):
                        financial_candidates.add(id(r))
                funnel[r['market']]['window_checked_' + tier] += 1
                funnel[r['market']]['window_eligible_' + tier] += int(window_eligible(r))
        for m in MARKETS:
            pool = [r for r in graded if r['strategy_tier'] == tier and r['market'] == m]
            chosen, rest = select([r for r in pool if window_eligible(r)], SEATS, profiles, key=rank)
            seats[(tier, m)] = chosen
            watch += sorted([r for r in pool if not window_eligible(r)], key=rank)[:3]
            # 高档满席：落选者若本身也命中低档证据（低档规则自己的前3或格子，不是顺延名次），按低档计划重算后进入低档竞争。
            for r in [r for r in pool if id(r) in financial_candidates and r not in chosen]:
                lower = [t for t in ((r.get('ladder') or {}).get('also_tiers') or []) if TIERS3.index(t) > TIERS3.index(tier)]
                if not lower or r['code'] not in by_code:
                    continue
                try:
                    r2, _ = grade(by_code[r['code']], envs.get(m) or {}, now, calendar, tiers=tuple(lower))
                except Exception:
                    r2 = None
                if r2 is not None and r2['strategy_tier'] in lower:
                    r2['ladder']['overflow_from'] = tier
                    graded.append(r2)
                    if r2.get('buyable'):
                        financial_candidates.add(id(r2))
                    funnel[m]['graded_' + r2['strategy_tier']] += 1
                    funnel[m]['buyable_' + r2['strategy_tier']] += int(r2['buyable'])
    taken = {r['code'] for v in seats.values() for r in v}
    now_graded = {}
    for r in graded:
        if r['code'] not in now_graded or window_eligible(r):
            now_graded[r['code']] = r
    rows = [r for r in doc['rows'] if r['code'] not in taken]
    # 一个程序一套逻辑：旧路径的可买行也必须过同一道分级门，否则降为研究行并写明原因。
    demoted = []
    for r in rows:
        if r['strategy_tier'] not in ('3A', '2A', '1A') or r['market'] not in MARKETS:
            continue
        m, tier = r['market'], r['strategy_tier']
        cur = now_graded.get(r['code'])
        why = rejected.get(r['code']) or \
            (('本轮分级未满足：' + '；'.join(cur.get('missing') or (cur.get('entry_window_qualification') or {}).get('reasons') or [])[:80]) if cur is not None and not window_eligible(cur) else None) or \
            (None if r.get('grade_basis') == LADDER_POLICY else '未入周期证据表分级')
        if not why and r.get('buyable'):
            continue
        # 未入分级的研究行（含旧「逆风试仓」放行、旧3A研究身份）一律不出买入指令，只留研究。
        why = why or '未入统一分级（好股×进入时机×获利机会未同时满足）'
        gap = '统一分级门未过：' + why
        was_buyable = bool(r.get('buyable'))
        from strategy_board_state import FOLLOWUP_POLICY, RESEARCH_CATEGORY
        r['missing'] = [gap] + [x for x in (r.get('missing') or []) if x != gap]
        if not was_buyable:
            continue
        r.update(buyable=False, status='研究候选',
                 selection_category=RESEARCH_CATEGORY if doc.get('grade_policy') == FOLLOWUP_POLICY else '最高价值研究')
        demoted.append({'code': r['code'], 'name': r.get('name'), 'tier': tier, 'market': m, 'why': why})
    displaced, final = [], []
    for tier in ('3A', '2A', '1A'):
        for m in MARKETS:
            old = [r for r in rows if r['strategy_tier'] == tier and r['market'] == m]
            mine = seats[(tier, m)]
            merged = mine + [r for r in old if r.get('buyable')] + [r for r in old if not r.get('buyable')]
            keep = merged[:SEATS]
            displaced += [{'code': r['code'], 'name': r.get('name'), 'tier': tier, 'market': m} for r in merged[SEATS:]]
            for k, r in enumerate(keep, 1):
                r['rank'] = k
            final += keep
            c = doc['counts'].setdefault(tier, {}).setdefault(m, {})
            c.update(selected=len(keep), matched=sum(bool(r.get('buyable')) for r in keep), ladder=len(mine),
                     window_eligible=sum(window_eligible(r) for r in keep))
    final += [r for r in rows if r['strategy_tier'] not in ('3A', '2A', '1A')]
    doc['rows'] = final
    for key in ('top_grade_watch', 'investment_candidates'):
        doc[key] = [r for r in doc.get(key, []) if r['code'] not in taken]
    for r in final:
        if r.get('grade_basis') == LADDER_POLICY:
            doc.setdefault('evaluations', {})[r['code']] = compact(r)
    empty = {}
    for tier in ('3A', '2A', '1A'):
        for m in MARKETS:
            if any(window_eligible(r) for r in final if r['strategy_tier'] == tier and r['market'] == m):
                continue
            b = (envs.get(m) or {}).get('breadth') or {}
            from shadow_screen import playbook
            book = playbook(m, b.get('value'), tiers=(tier,))
            upper = [r for r in final if r['market'] == m and window_eligible(r) and
                     tier in ((r.get('ladder') or {}).get('also_tiers') or [])]
            if upper:
                from horizon_book import PLAN
                names = '、'.join(f"{r.get('name') or r['code']}（{r['strategy_tier']}）" for r in upper)
                empty.setdefault(tier, {})[m] = (f'{m}{tier}口径命中的个股已进入更高档：{names}——同一只股票，'
                                                 f'{tier}按{PLAN[tier]["hold"]}个交易日计划同样成立；多档命中取高档，不重复占席')
                continue
            if not book['active']:
                from horizon_book import HK_NOTE
                why = HK_NOTE if m == '港股' else \
                    f'当前宽度区间（{book["band"] or "宽度缺失"}）{m}{tier}没有回测胜率达标的规则（观察·胜率不足50%，不可买）；看学习型候选'
            else:
                top = '；'.join(f'{k}×{v}' for k, v in skipped[m].most_common(3))
                why = (f'{book["text"]}，但本档无合格个股（已分级{funnel[m]["graded_" + tier]}只、可买{funnel[m]["buyable_" + tier]}只）'
                       + (f'；主要淘汰原因：{top}' if top else ''))
            empty.setdefault(tier, {})[m] = why
    doc['ladder'] = {
        'policy': LADDER_POLICY, 'version': LADDER_VERSION,
        'rule': '无硬伤（好股优先，资料缺失标基本面待补、仓位减半）× 进入时机（与Astra同一份周期证据表：宽度格子或每日排名前3）'
                '× 获利机会（按命中规则的回测口径定价格：固定百分比目标、按规则持有天数）；每条规则前后两半段胜率各≥53%，多档命中取高档。',
        'evidence': {m: playbook_of(m, envs)['text'] for m in MARKETS}, 'scope': SCOPE,
        'breadth': {m: (envs.get(m) or {}).get('breadth') for m in MARKETS},
        'funnel': {m: dict(funnel[m]) for m in MARKETS},
        'preselection_errors': preselection_errors,
        'skipped_top': {m: skipped[m].most_common(5) for m in MARKETS},
        'empty_reasons': empty, 'displaced': displaced, 'demoted': demoted,
        'playbook': {m: playbook_of(m, envs) for m in MARKETS},
        'learning': learning(fresh, final, envs, rejected, profiles),
        'bottom_radar': {m: sorted(v, key=lambda x: (not x['quality'], not x['above_ma20'], x['drawdown_pct']))[:RADAR_SEATS]
                         for m, v in radar.items()},
        'watch': [{k: r.get(k) for k in ('code', 'name', 'market', 'strategy_tier', 'last', 'entry_range', 'stop_range',
                                          'take_profit_range', 'missing', 'ladder')} for r in watch],
        'codes': [r['code'] for r in graded]}
    return doc
