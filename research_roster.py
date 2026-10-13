"""Daily 3A research seats, separate from current scores and entry permission.

The owner-approved daily cadence is a product policy, not a book threshold or
validated return improvement. Historical rows and original plans are untouched.
"""
from copy import deepcopy
from datetime import datetime
import math
import re

POLICY = '3a-daily-research-v1'
GOVERNANCE = re.compile(
    r'立案|调查|处罚|退市|终止上市|风险警示|问询函|违规|无法表示意见|保留意见|'
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
        hits = [h.get('title', '') for h in filing.get('hits', []) if GOVERNANCE.search(h.get('title', ''))]
        if hits:
            return '已检出治理/偿付风险：' + '；'.join(hits[:2])
    return None


def valid_admission(row, board=None):
    meta = row.get('research_roster') or {}
    value = meta.get('selection_score')
    valid = (meta.get('policy') == POLICY and isinstance(value, (int, float)) and not isinstance(value, bool)
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
    return (root.get('policy') == POLICY and timed and row.get('code') in market.get('selected_codes', [])
            and meta.get('review_session') == market.get('review_session'))


def apply(board, current, previous, now, profiles=None, structures=None):
    """Use full evaluations, never an already truncated Top3 as the candidate pool."""
    from exchange_sessions import latest_completed
    from industry_diversity import selection_industry
    from strategy_board_state import followup
    from strategy_board import compact, trigger

    previous = previous or {}
    policy_ok = (previous.get('grade_policy') == board.get('grade_policy') and
                 previous.get('scoring_revision') == board.get('scoring_revision'))
    previous_meta = previous.get('research_roster') or {}
    active = policy_ok and previous_meta.get('policy') == POLICY
    old_rows = [r for r in previous.get('rows', []) if r.get('strategy_tier') == '3A'] if active else []
    markets = {}; chosen_all = []
    board['instant_top3'] = deepcopy([r for r in board['rows'] if r.get('strategy_tier') == '3A'])
    for market in board['counts']['3A']:
        session = latest_completed(market, now).isoformat()
        prior_meta = (previous_meta.get('markets') or {}).get(market, {}) if active else {}
        old = [r for r in old_rows if r.get('market') == market]
        # A backwards clock cannot roll an established daily edition backwards.
        old_session = prior_meta.get('review_session')
        due = not old_session or session > old_session
        review_session = session if due else old_session
        full = {c: r for c, r in current.items() if r.get('market') == market}
        rejected = {c: hard_exit(r, now) for c, r in full.items()}
        pending_codes = [r['code'] for r in old if not rejected.get(r['code']) and (
            r['code'] not in full or not full[r['code']].get('business_supported') or
            not full[r['code']].get('structure_source'))]
        review_deferred = bool(due and pending_codes)
        if review_deferred:
            # Source gaps are not adverse findings. A partial new-session scan
            # must not silently replace an unobserved incumbent with a peer.
            due = False
            review_session = old_session
        eligible = [r for c, r in full.items() if r.get('quality_eligible') and not rejected[c]]
        eligible.sort(key=lambda r: (-r['strategy_score']['value'], r['code']))
        changes = []; chosen = []; occupied = set()
        if not due:
            for original in old:
                code = original['code']; latest = full.get(code)
                if rejected.get(code):
                    changes.append({'code': code, 'name': original.get('name'), 'action': 'exit',
                                    'reason': rejected[code], 'at': now.isoformat()})
                    continue
                row = deepcopy(latest or original)
                incomplete = (latest is None or not latest.get('business_supported') or
                              not latest.get('structure_source'))
                admission = deepcopy(original.get('research_roster') or {})
                row['current_quality_eligible'] = bool(latest and latest.get('quality_eligible'))
                row['quality_eligible'] = True  # Daily admission, not current score qualification.
                row['strategy_tier'] = '3A'
                row['score_current_unknown'] = incomplete
                row['buyable'] = bool(latest and latest.get('buyable') and
                                      row['strategy_score']['value'] >= 80 and not incomplete)
                row['executable'] = False
                if latest is None:
                    # Retain the entire dated observation, never pair an old
                    # score with an invented latest quote or build timestamp.
                    row['research_retained'] = True
                    row['last_evaluated_at'] = original.get('last_evaluated_at') or previous.get('generated_at')
                    row['status'] = '源行情待更新'
                note = ('本轮原始数据不完整；本日研究身份保留，当前分待核' if incomplete else
                        f"本日研究续列；当前分{row['strategy_score']['value']:g}低于80，等待下一完整交易日评审"
                        if row['strategy_score']['value'] < 80 else None)
                if note:
                    row['missing'] = list(dict.fromkeys(row.get('missing', []) + [note]))
                if latest is not None:
                    followup(row, (structures or {}).get(code))
                else:
                    # No recomputation against a missing source: keep the exact
                    # prior plan/opportunity as a dated, non-buyable observation.
                    from strategy_board_state import readiness, RESEARCH_CATEGORY
                    row['selection_category'] = RESEARCH_CATEGORY
                    row.update(readiness(row))
                row['trigger'] = trigger(row)
                admission.update(policy=POLICY, review_session=review_session,
                                 current_score=None if incomplete else row['strategy_score']['value'],
                                 status='data_pending' if incomplete else 'continuing',
                                 current_gaps=list(row.get('missing') or []))
                row['research_roster'] = admission
                from strategy_board_state import readiness
                row.update(readiness(row))
                sector = selection_industry(row, profiles)
                if sector and sector in occupied:
                    changes.append({'code': code, 'name': row.get('name'), 'action': 'exit',
                                    'reason': '行业分类更新后同业重复，保留原顺位研究；历史继续跟踪', 'at': now.isoformat()})
                    continue
                if sector:
                    occupied.add(sector)
                chosen.append(row)
        # Initial/daily review selects current eligible Top3. Intraday only fills
        # vacancies, never displaces an incumbent for a tiny score advantage.
        for row in eligible:
            if len(chosen) >= 3:
                break
            if any(r['code'] == row['code'] for r in chosen):
                continue
            sector = selection_industry(row, profiles)
            if sector and sector in occupied:
                continue
            row = deepcopy(row)
            reason = '新完整交易日按统一分及行业分散评审' if due and active else '首次日度研究评审' if due else '本日空席按当前合格研究分补入'
            row['research_roster'] = {
                'policy': POLICY, 'review_session': review_session, 'selected_at': now.isoformat(),
                'selection_score': row['strategy_score']['value'], 'current_score': row['strategy_score']['value'],
                'selection_reason': reason, 'status': 'selected', 'current_gaps': list(row.get('missing') or [])}
            row['score_current_unknown'] = False; row['current_quality_eligible'] = True
            if sector:
                occupied.add(sector)
            chosen.append(row)
        old_codes = [r['code'] for r in old]; new_codes = [r['code'] for r in chosen]
        changed = {r['code'] for r in changes}
        for r in old:
            if r['code'] not in new_codes and r['code'] not in changed:
                changes.append({'code': r['code'], 'name': r.get('name'), 'action': 'exit', 'at': now.isoformat(),
                                'reason': rejected.get(r['code']) or '完整交易日评审后未列本期Top3；历史继续跟踪，并非卖出指令'})
        for r in chosen:
            if r['code'] not in old_codes:
                changes.append({'code': r['code'], 'name': r.get('name'), 'action': 'enter', 'at': now.isoformat(),
                                'reason': r['research_roster']['selection_reason']})
        for rank, row in enumerate(chosen, 1):
            row['rank'] = rank
            board['evaluations'][row['code']] = compact(row, trig=row.get('trigger'))
        chosen_all.extend(chosen)
        markets[market] = {
            'review_session': review_session, 'reviewed_at': now.isoformat() if due else prior_meta.get('reviewed_at'),
            'review_reason': '首次评审' if due and not active else '新完整交易日评审' if due else '盘中更新行情与条件，研究席位持续跟踪',
            'selected_codes': new_codes, 'previous_codes': old_codes, 'changes': changes,
            'checked_at': now.isoformat(), 'review_deferred': review_deferred,
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
    board['score_note'] = (board.get('score_note') or '').replace(
        '3A≥80分且有企业依据，分买入条件已满足与高价值可跟进·重点观察，两组合计每市场Top3，同业最多1只',
        '3A为日度评审研究Top3：新完整交易日按当时统一分≥80且有企业依据选入，每市场最多3只、同业最多1只；盘中更新真实现价、当前统一分及买点，保留日度研究席位，非实时Top3；选入分与当前分分列，当前分低于80仅保留本日研究身份、不能标买入条件满足；源缺保留旧日期并标当前分待核，真实企业反证或明确治理风险即时退出研究，退榜不等于卖出。分买入条件已满足与高价值可跟进·重点观察')
    board['research_roster'] = {'policy': POLICY, 'markets': markets,
                                'note': '3A为日度评审研究名单；当前分、行情和买点独立更新，不是实时Top3；退榜不等于卖出。'}
    return board
