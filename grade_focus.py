"""Deterministic attention ranking of CURRENT central grades, never a grade grant.

The caller validates publication identity/freshness first. All pricing, scoring,
and maturity inputs remain from that same frozen contract. No network or writes.
Keep the desktop and core/src copies identical.
"""
from __future__ import annotations

import math
import re
from collections import Counter, defaultdict

from investment_maturity import assess
from review_scorecard import gpt_result

VERSION = "grade-focus-v3-score-quota"
GRADES = ("3A", "2A", "1A")
MARKETS = ("A股", "美股", "港股")
PER_MARKET = 5
GRADE_LIMITS = {'3A': 2, '2A': 2, '1A': 5}
GRADE_MINIMUMS = {'3A': 1, '2A': 1, '1A': 3}
ATTENTION_VERSION = 'central-grade-score-with-market-quota-v2'
RULE = ("正式榜只含当前已评级且有真实审核分的个股：同档同市场按审核分降序；"
        "1A每市场目标3–5只，2A/3A各目标1–2只，不足记录补审缺口，不虚授等级。"
        "进场条件及周度关注只作状态标记，不改变分数顺序；中长期与短期注明原周期。"
        "同档同市场依次比较审核分、书籍通过率、未成熟条件数、净收益风险比档、"
        "本周期净空间倍数档；同档以股票代码稳定排序。"
        "风险比和空间倍数每0.1一档，微小价格差不制造精确排名；排名不是另一个评分或胜率。")


def canonical(code):
    code = str(code or "").strip().upper()
    if re.fullmatch(r"\d{6}\.SH", code):
        return code[:-3] + ".SS"
    if re.fullmatch(r"\d{1,5}\.HK", code):
        return str(int(code[:-3])) + ".HK"
    return code


def market_of(code):
    if re.fullmatch(r"\d{6}\.(SS|SZ|BJ)", code):
        return "A股"
    if re.fullmatch(r"\d{1,5}\.HK", code):
        return "港股"
    if re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,14}", code):
        return "美股"
    return None


def attention_rows(graded, *, weekly_codes=()):
    """Formal graded list, ordered by actual score within each market/grade.

    Weekly flags annotate the SAME ranked rows; they never reserve or reorder.
    Caller must validate current review and contract identity first.
    """
    weekly = set(weekly_codes)
    result = []
    for tier in GRADES:
        for market in MARKETS:
            group = [r for r in graded if r['market'] == market and r.get('tier') == tier
                     and _finite(r.get('audit_score')) and 0 <= r['audit_score'] <= 100]
            group.sort(key=lambda r: (-r['audit_score'], r['central_rank'], r['code']))
            for index, row in enumerate(group[:GRADE_LIMITS[tier]], 1):
                eligible=(row.get('entry_opportunity') or {}).get('focus_eligible') is True
                result.append({**row, 'watch_rank': index,
                    'seat_kind': '条件关注' if eligible else '研究跟踪',
                    'weekly_link': row['code'] in weekly and eligible,
                    'weekly_reserved': False, 'attention_policy': ATTENTION_VERSION})
    return result


def _finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def _metrics(row):
    card = row.get("scorecard") or {}
    plan = row.get("central_trade_plan") or row.get("trade_plan") or {}
    horizon = plan.get("horizon") or row.get("horizon")
    tier = row.get("tier")
    score = card.get("total")
    if not _finite(score) or not 0 <= score <= 100:
        return None, "审核分缺失或非有限值，保留跟踪待核"
    if row.get('audit_score') is not None and row['audit_score'] != score:
        return None, "发布分与审核分不一致，保留跟踪待核"
    value = assess(card, horizon, plan)
    if value["tier"] != tier or not value["value_confirmed"]:
        return None, "当前审核/收益合同与原等级不一致，保留跟踪待核"
    books, gpt = card["books"], card["gpt"]
    if score != min(gpt["total"], books["total"]):
        return None, "审核分与原始分项不一致，保留跟踪待核"
    profit = value["profit_contract"]  # Recompute from inputs, not an unverified saved RR.
    rr, upside = profit["net_reward_risk"], profit["net_upside_pct"]
    floor = profit["period_thresholds"][tier]
    if not all(_finite(v) and v > 0 for v in (rr, upside, floor)):
        return None, "净空间或风险比无效，保留跟踪待核"
    ratio = upside / floor
    return {"audit_score": score, "book_score": books["total"],
            "book_pass_n": books["pass_n"], "book_required": books["required"],
            "review_scores": {k: gpt_result(v)["total"] for k, v in card["review_pair"].items()},
            "selected_review": card.get("selected_review"),
            "remaining_count": len(value["remaining_conditions"]),
            "remaining_conditions": [x["title"] for x in value["remaining_conditions"]],
            "horizon": horizon, "net_reward_risk": rr, "net_upside_pct": upside,
            "period_floor_pct": floor, "space_multiple": ratio,
            "rr_band": math.floor(rr * 10), "space_band": math.floor(ratio * 10),
            "audit_id": card.get("audit_id")}, None


def build(rows, *, now=None):
    """Bound each grade across ALL execution buckets/horizons, preserving reserves.

    No grades/scores are modified. Duplicate identities (including aliases) are
    kept out of the focus list until the upstream publication resolves them.
    """
    current = [r for r in rows if r.get("tier") in GRADES]
    duplicates = Counter(canonical(r.get("code")) for r in current)
    groups, records = defaultdict(list), {}
    for row in current:
        code, tier = canonical(row.get("code")), row["tier"]
        market = market_of(code)
        metrics, error = _metrics(row)
        if duplicates[code] != 1:
            error = "重复股票身份，待核；不重复占用名额"
        if market is None:
            error = "市场身份未确认，待核；不跨市场填额"
        declared = {'CN': 'A股', 'US': '美股', 'HK': '港股'}.get(row.get('market'), row.get('market'))
        if declared in MARKETS and declared != market:
            error = "市场字段与规范代码冲突，待核；不跨市场填额"
        from entry_opportunity import assess as entry_assess
        opportunity = entry_assess(row, now=now)
        rec = {"code": code, "source_code": row.get("code"), "tier": tier,
               "name": row.get("name"), "market": market, "selected": False,
               "rank": None, "central_rank": None, "metrics": metrics, "reason": error,
               "entry_opportunity": opportunity}
        records[code] = rec
        if error is None:
            # Medium/long is the primary 3A product. Original short evidence stays short.
            lane = int(tier == "3A" and metrics["horizon"] == "short")
            rec["sort_key"] = [-metrics["audit_score"], lane, -metrics["book_score"],
                               metrics["remaining_count"], -metrics["rr_band"],
                               -metrics["space_band"], code]
            groups[tier, market].append(rec)
    summary, selected = {}, []
    for tier in GRADES:
        summary[tier] = {}
        for market in MARKETS:
            group = sorted(groups[tier, market], key=lambda r: r["sort_key"])
            eligible_n = 0
            for central_rank, rec in enumerate(group, 1):
                rec['central_rank'] = central_rank
                rec['selected'] = central_rank <= GRADE_LIMITS[tier]
                if rec['selected']:
                    selected.append(rec['code'])
                if not rec['entry_opportunity']['focus_eligible']:
                    rec['reason'] = '；'.join(rec['entry_opportunity']['reasons'])
                    continue
                eligible_n += 1
                rank = eligible_n
                rec.update(rank=rank)
                rec["reason"] = (f"{market}{tier}审核分第{central_rank}名，入选正式榜" if rec['selected'] else
                                 f"{market}{tier}审核分第{central_rank}名，超出本档前{GRADE_LIMITS[tier]}；保留评级和原合同跟踪")
            full_n = sum(r["tier"] == tier and r["market"] == market for r in records.values())
            summary[tier][market] = {"selected": min(GRADE_LIMITS[tier], len(group)),
                                    "eligible": eligible_n, "current": full_n,
                                    "limit": GRADE_LIMITS[tier], "minimum": GRADE_MINIMUMS[tier],
                                    "minimum_gap": max(0,GRADE_MINIMUMS[tier]-len(group)),
                                    "vacancies": max(0, GRADE_LIMITS[tier] - len(group))}
    return {"version": VERSION, "rule": RULE, "per_market_limit": sum(GRADE_LIMITS.values()),
            "per_grade_limit": {t:n*len(MARKETS) for t,n in GRADE_LIMITS.items()}, "per_grade_limits": dict(GRADE_LIMITS),
            "minimums": dict(GRADE_MINIMUMS), "summary": summary,
            "selected_codes": selected, "records": records,
            "current_count": len(records), "selected_count": len(selected),
            "reserve_count": len(records) - len(selected), "no_grade_authority": True,
            "model_calls": 0}
