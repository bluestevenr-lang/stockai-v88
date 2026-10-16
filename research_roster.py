"""Daily 3A research seats, separate from current scores and entry permission.

The owner-approved daily cadence is a product policy, not a book threshold or
validated return improvement. Historical rows and original plans are untouched.
"""
from copy import deepcopy
from datetime import datetime, date
import math
import re

POLICY = '3a-daily-research-v2'
LEGACY_POLICY = '3a-daily-research-v1'
ACCEPTED_POLICIES = (POLICY, LEGACY_POLICY)
SCORE_MARGIN = 3.0
CONFIRMATION_SESSIONS = 2
GOVERNANCE = re.compile(
    r'立案|被调查|收到.{0,8}调查|调查通知|行政处罚事先告知|拟.{0,8}处罚|'
    r'处罚.{0,12}(未履行|未落实|未完成|尚未)|重大违法|退市|终止上市|风险警示|无法表示意见|否定意见|'
    r'重大问询.{0,12}(未回复|未答复|逾期)|未回复.{0,12}重大问询|'
    r'违约|债务逾期|破产|重整|清盘|investigation|subpoena|restatement|bankruptcy|'
    r'chapter 11|material weakness|SEC (charges|enforcement)', re.I)


def hard_exit(row, now):
    """Missing evidence is not a negative finding; reductions are not governance."""
    if not row:
        return None
    if (row.get('fundamentals') or {}).get('status') == 'counterevidence':
        return '企业财务反证，退出本日重点研究；不代表卖出指令'
    if row.get('market') in ('A股', '港股') and re.match(r'^\*?ST\s*(?=[\u3400-\u9fff])', str(row.get('name') or '').strip(), re.I):
        return '当前风险警示名称，退出重点研究，历史继续跟踪'
    from filing_check import fresh
    filing = row.get('filing_check') or {}
    if filing.get('status') == 'conflict' and fresh(filing, now):
        # A routine issuance inquiry/reply is a document workflow, not proof of
        # unresolved governance. Do not infer clearance of the issuer either.
        hits = [h.get('title', '') for h in filing.get('hits', [])
                if h.get('hard_risk') is True or (
                    GOVERNANCE.search(h.get('title', '')) and
                    not re.search(r'结案|调查终结|处罚.{0,8}(已履行|履行完毕)|撤销风险警示|重大问询.{0,8}(已回复|已答复)|不存在.{0,8}(违法|违规|处罚|立案)|未被立案',
                                  h.get('title', '')))]
        if hits:
            return '已检出治理/偿付风险：' + '；'.join(hits[:2])
    return None


def valid_admission(row, board=None):
    meta = row.get('research_roster') or {}
    value = meta.get('selection_score')
    valid = (meta.get('policy') in ACCEPTED_POLICIES and isinstance(value, (int, float)) and not isinstance(value, bool)
             and math.isfinite(value) and 80 <= value <= 100 and bool(meta.get('selected_at')) and bool(meta.get('review_session')))
    if not valid or board is None:
        return valid
    root = board.get('research_roster') or {}
    market = (root.get('markets') or {}).get(row.get('market')) or {}
    try:
        selected = datetime.fromisoformat(meta['selected_at'])
        checked = datetime.fromisoformat(market['checked_at'])
        timed = selected.tzinfo is not None and checked.tzinfo is not None and selected <= checked
    except (ValueError, TypeError, KeyError):
        return False
    return (root.get('policy') in ACCEPTED_POLICIES and timed and row.get('code') in market.get('selected_codes', [])
            and meta.get('review_session') == market.get('review_session'))


def _complete(row):
    return bool(row and row.get('business_supported') and row.get('structure_source'))


def _session_evidence(row, structure, session):
    """A stale daily bar cannot count as a fresh challenger confirmation."""
    if not _complete(row):
        return False
    daily = (structure or {}).get('source_asof') or (row.get('annual_metrics') or {}).get('asof')
    try:
        return date.fromisoformat(str(daily)[:10]) == date.fromisoformat(session)
    except (ValueError, TypeError):
        return False


def apply(board, current, previous, now, profiles=None, structures=None):
    """Stable research seats, with non-compensatory daily eligibility and risk exits.

    Three points over two consecutive completed sessions is an owner-approved
    roster debounce, not a scoring formula or a calibrated performance claim.
    """
    from exchange_sessions import latest_completed, next_session
    from industry_diversity import selection_industry
    from strategy_board_state import followup, readiness, RESEARCH_CATEGORY
    from strategy_board import compact, trigger

    previous = previous or {}; structures = structures or {}
    policy_ok = (previous.get('grade_policy') == board.get('grade_policy') and
                 previous.get('scoring_revision') == board.get('scoring_revision'))
    previous_meta = previous.get('research_roster') or {}
    active = policy_ok and previous_meta.get('policy') in ACCEPTED_POLICIES
    old_rows = [r for r in previous.get('rows', []) if r.get('strategy_tier') == '3A'] if active else []
    markets = {}; chosen_all = []
    board['instant_top3'] = deepcopy([r for r in board['rows'] if r.get('strategy_tier') == '3A'])
    for market in board['counts']['3A']:
        session = latest_completed(market, now).isoformat()
        prior_meta = (previous_meta.get('markets') or {}).get(market, {}) if active else {}
        old = [r for r in old_rows if r.get('market') == market]
        # A backwards clock never rolls an established edition backwards.
        old_session = prior_meta.get('review_session')
        session = max(session, old_session or session)
        full = {c: r for c, r in current.items() if r.get('market') == market}
        rejected = {c: hard_exit(r, now) for c, r in full.items()}
        pending_codes = [r['code'] for r in old if not rejected.get(r['code']) and not _complete(full.get(r['code']))]
        any_complete = any(_complete(r) for r in full.values())
        review_session = session if any_complete or not old else old_session
        review_deferred = bool(pending_codes)
        eligible = [r for c, r in full.items() if r.get('quality_eligible') and not rejected[c]]
        eligible.sort(key=lambda r: (-r['strategy_score']['value'], r['code']))
        changes = []; chosen = []; occupied = set(); daily_checked = False

        def change(row, action, reason):
            changes.append({'code': row['code'], 'name': row.get('name'), 'action': action,
                            'reason': reason, 'at': now.isoformat(), 'review_session': session})

        def admit(row, reason):
            row = deepcopy(row)
            row['research_roster'] = {
                'policy': POLICY, 'review_session': review_session, 'selection_session': session,
                'last_review_session': session, 'selected_at': now.isoformat(),
                'selection_score': row['strategy_score']['value'], 'current_score': row['strategy_score']['value'],
                'selection_reason': reason, 'status': 'selected', 'current_gaps': list(row.get('missing') or [])}
            row['score_current_unknown'] = False; row['current_quality_eligible'] = True
            return row

        for original in old:
            code = original['code']; latest = full.get(code)
            admission = deepcopy(original.get('research_roster') or {})
            last_review = admission.get('last_review_session') or admission.get('review_session') or old_session
            row_due = bool(not last_review or session > last_review)
            if rejected.get(code):
                change(original, 'exit', rejected[code]); continue
            incomplete = not _complete(latest)
            # Missing data defers this seat alone; complete peers still review.
            if row_due and not incomplete:
                daily_checked = True
                if not latest.get('quality_eligible'):
                    change(original, 'exit', f"完整交易日评审：当前统一分{latest['strategy_score']['value']:g}未达80或企业依据不成立；历史继续跟踪，并非卖出指令")
                    continue
                last_review = session
            row = deepcopy(latest or original)
            row['current_quality_eligible'] = bool(latest and latest.get('quality_eligible'))
            row['quality_eligible'] = True  # Research admission, not current score qualification.
            row['strategy_tier'] = '3A'; row['score_current_unknown'] = incomplete
            row['buyable'] = bool(latest and latest.get('buyable') and row['strategy_score']['value'] >= 80 and not incomplete)
            row['executable'] = False
            if latest is None:
                row['research_retained'] = True
                row['last_evaluated_at'] = original.get('last_evaluated_at') or previous.get('generated_at')
                row['status'] = '源行情待更新'
            note = ('本轮原始数据不完整；保留原研究及原时点，当前分待核' if incomplete else
                    f"本日研究续列；当前分{row['strategy_score']['value']:g}低于80，等待下一完整交易日评审"
                    if row['strategy_score']['value'] < 80 else None)
            if note:
                row['missing'] = list(dict.fromkeys(row.get('missing', []) + [note]))
            admission.setdefault('selection_session', admission.get('review_session'))
            admission.update(policy=POLICY, review_session=review_session, last_review_session=last_review,
                             current_score=None if incomplete else row['strategy_score']['value'],
                             status='data_pending' if incomplete else 'continuing', current_gaps=list(row.get('missing') or []))
            row['research_roster'] = admission
            if latest is not None:
                followup(row, structures.get(code))
            else:
                row['selection_category'] = RESEARCH_CATEGORY
            row['trigger'] = trigger(row); row.update(readiness(row))
            sector = selection_industry(row, profiles)
            if sector and sector in occupied:
                change(row, 'exit', '行业分类更新后同业重复，保留原顺位研究；历史继续跟踪'); continue
            if sector: occupied.add(sector)
            chosen.append(row)

        # First deployment, true exits and empty seats still use current genuine
        # eligible scores. No waiting period is invented for an empty seat.
        for row in eligible:
            if len(chosen) >= 3: break
            if any(r['code'] == row['code'] for r in chosen): continue
            sector = selection_industry(row, profiles)
            if sector and sector in occupied: continue
            reason = '首次日度研究评审' if not active else '空席按当前合格研究分及行业分散补入'
            chosen.append(admit(row, reason))
            if sector: occupied.add(sector)

        prior_challengers = prior_meta.get('challengers') or {}
        challengers = {}; replaced = set()
        # Compare once per completed session for each stable candidate/seat pair.
        # Intraday repeats never build a streak; missing/stale evidence breaks it.
        for candidate in eligible:
            code = candidate['code']
            if len(chosen) < 3 or any(r['code'] == code for r in chosen): continue
            if not _session_evidence(candidate, structures.get(code), session): continue
            sector = selection_industry(candidate, profiles)
            same_sector = [r for r in chosen if sector and selection_industry(r, profiles) == sector]
            targets = same_sector or chosen
            targets = [r for r in targets if r['code'] not in replaced and
                       r['code'] in {x['code'] for x in old} and
                       _session_evidence(full.get(r['code']), structures.get(r['code']), session)]
            if not targets: continue
            target = min(targets, key=lambda r: (r['strategy_score']['value'], r['code']))
            gap = round(candidate['strategy_score']['value'] - target['strategy_score']['value'], 8)
            if gap < SCORE_MARGIN: continue
            prior = prior_challengers.get(code) or {}
            paired = prior.get('against_code') == target['code']
            if paired and prior.get('last_session') == session:
                streak = prior.get('streak', 1); first = prior.get('first_session', session)
            else:
                try:
                    consecutive = paired and next_session(date.fromisoformat(prior['last_session']), market).isoformat() == session
                except (ValueError, TypeError, KeyError): consecutive = False
                streak = prior.get('streak', 0) + 1 if consecutive else 1
                first = prior.get('first_session', session) if consecutive else session
            proof = {'against_code': target['code'], 'first_session': first, 'last_session': session,
                     'streak': streak, 'score_gap': gap, 'candidate_score': candidate['strategy_score']['value'],
                     'incumbent_score': target['strategy_score']['value'], 'checked_at': now.isoformat()}
            if streak < CONFIRMATION_SESSIONS:
                challengers[code] = proof; continue
            reason = f"连续{CONFIRMATION_SESSIONS}个完整交易日领先{target['code']}至少{SCORE_MARGIN:g}分，本次差{gap:g}分；研究替换不代表卖出"
            change(target, 'exit', reason)
            new_row = admit(candidate, reason); new_row['research_roster']['replacement_evidence'] = proof
            chosen[chosen.index(target)] = new_row
            replaced.add(code)

        old_codes = [r['code'] for r in old]; new_codes = [r['code'] for r in chosen]
        for row in chosen:
            if row['code'] not in old_codes:
                change(row, 'enter', row['research_roster']['selection_reason'])
        # A replaced pair may not linger as a ready challenge to a departed seat.
        challengers = {c: v for c, v in challengers.items() if v['against_code'] in new_codes and c not in new_codes}
        for rank, row in enumerate(chosen, 1):
            row['rank'] = rank
            board['evaluations'][row['code']] = compact(row, trig=row.get('trigger'))
        chosen_all.extend(chosen)
        history = deepcopy(prior_meta.get('change_history', prior_meta.get('changes') or []))
        history.extend(changes)
        markets[market] = {
            'review_session': review_session,
            'reviewed_at': now.isoformat() if daily_checked or not active else prior_meta.get('reviewed_at'),
            'review_reason': '逐席日度资格复核，挑战者连续两日领先至少3分才替换；缺源席单独待核',
            'selected_codes': new_codes, 'previous_codes': old_codes, 'changes': changes, 'change_history': history,
            'challengers': challengers, 'checked_at': now.isoformat(), 'review_deferred': review_deferred,
            'pending_codes': pending_codes}
    codes = {r['code'] for r in chosen_all}
    board['rows'] = chosen_all + [r for r in board['rows'] if r['strategy_tier'] != '3A' and r['code'] not in codes]
    for key in ('top_grade_watch', 'investment_candidates'):
        board[key] = [r for r in board.get(key, []) if r['code'] not in codes]
    for tier, by_market in board['counts'].items():
        for market, count in by_market.items():
            rows = [r for r in board['rows'] if r['strategy_tier'] == tier and r['market'] == market]
            count.update(selected=len(rows), matched=sum(bool(r.get('buyable')) for r in rows),
                         retained=sum(bool(r.get('research_retained')) for r in rows))
    original_note = (board.get('score_note') or '').replace(
        '3A≥80分且有企业依据，分买入条件已满足与高价值可跟进·重点观察，两组合计每市场Top3，同业最多1只',
        '3A按入选分≥80与企业依据保留稳定研究席，分买入条件已满足与高价值可跟进·重点观察，两组合计每市场最多3只，同业最多1只')
    board['score_note'] = original_note + (
        ' 3A稳定研究名单：每日逐席复核，当前统一分≥80且企业依据成立的原席优先续列；'
        '挑战者须连续两个完整交易日比可替换席领先至少3分，缺源或断档不累计。'
        '每市场最多3只且行业去重；名单非实时最高分Top3，入选分与当前分分列。'
        '缺源只保留原日期的待核研究，不授可买；真实硬风险即时退出；退榜不等于卖出。'
        '防抖参数为产品规则，未作胜率校准。')
    board['research_roster'] = {'policy': POLICY, 'score_margin': SCORE_MARGIN,
                                'confirmation_sessions': CONFIRMATION_SESSIONS, 'markets': markets,
                                'note': '稳定研究名单，逐日资格复核；不是实时Top3；真实分数及买点独立更新；退榜不等于卖出。'}
    return board
