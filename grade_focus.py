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

VERSION = "grade-focus-v8-market-research-windows"
GRADES = ("3A", "2A", "1A")
HORIZONS = ("short", "medium", "long")
HORIZON_LABELS = {"short":"短期3A · 未来8周", "medium":"中期3A · 8–24周", "long":"长期3A · 12–36周"}
RESEARCH_WEEKS = {"short":(0,8), "medium":(8,24), "long":(12,36)}
RESERVE_PER_MARKET = 5
MARKETS = ("A股", "美股", "港股")
PER_MARKET = 3  # Independent slots in each market within each horizon.
HORIZON_LIMIT = PER_MARKET * len(MARKETS)
GRADE_LIMITS = {t: PER_MARKET for t in GRADES}
GRADE_MINIMUMS = {t: 0 for t in GRADES}
ATTENTION_VERSION = 'central-market-research-top3-v7'
RULE = ("短期3A研究未来8周、中期8–24周、长期12–36周；每个周期A股、美股、港股各自Top3，共9个研究名额。"
        "按当前中央审核分降序；同分比较原书理分、待确认项与原收益风险，股票代码固定打破平分。"
        "研究上榜和当前开仓分开：入场冲突不删除研究价值，未来5交易日入场核验单列。"
        "3A是系统名称，个股保留实际1A/2A/3A审核等级。研究窗口不续签原合同，目标与截止日不延长。"
        "中长期窗口重叠，分别核证；不因短线高分自动复制到中长期。候补各市场最多5只，总计最多15只。")


def research_window(row):
    """A prospective research agenda, never an extension of a signed target.

    Route only the source's explicit strategy horizon, not the entry gate or a
    competing horizon's score. Overlapping agendas need independent evidence;
    absent such a source, never synthesize another rated contract.
    """
    plan=row.get('central_trade_plan') or row.get('trade_plan') or {}
    horizon=plan.get('horizon') or row.get('horizon')
    weeks=RESEARCH_WEEKS.get(horizon)
    if weeks is None:return None
    inputs=plan.get('profit_inputs') or {}
    return {'version':'research-window-v1','horizon':horizon,
            'label':HORIZON_LABELS[horizon],'start_week':weeks[0],'end_week':weeks[1],
            'contract_horizon':horizon,'contract_max_calendar_days':inputs.get('max_calendar_days'),
            'contract_deadline':(plan.get('profit_contract') or {}).get('thesis_deadline') or plan.get('deadline') or plan.get('expires_at'),
            'basis':'沿用当前同周期机会审核；研究窗口独立于单笔交易合同',
            'extension_status':'后续阶段逐次补证复审，不将原目标延伸至整个研究窗口',
            'no_contract_extension':True}


def ranked_reserves(rows, *, exclude=()):
    """Bound ALL reserve sources together; scored first, never invented scores."""
    excluded={canonical(c) for c in exclude};unique={}
    def key(r):
        score=r.get('audit_score');scored=_finite(score) and 0<=score<=100
        return (not scored,-score if scored else 0,
                tuple(r.get('sort_key') or []),canonical(r.get('code')))
    for r in rows:
        code=canonical(r.get('code'))
        if not code or code in excluded or r.get('market') not in MARKETS:continue
        # Current producer rows precede older continuity; no stale score wins a
        # duplicate simply because it was larger in a previous publication.
        unique.setdefault(code,r)
    result=[]
    for market in MARKETS:
        group=sorted((r for r in unique.values() if r['market']==market),key=key)
        result.extend({**r,'watch_rank':i,'reserve_rank':i,'no_rating_authority':True}
                      for i,r in enumerate(group[:RESERVE_PER_MARKET],1))
    return result



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
    """Formal graded list, independent Top3 in each market within each horizon.

    Weekly flags annotate the SAME ranked rows; they never reserve or reorder.
    Caller must validate current review and contract identity first.
    """
    weekly = set(weekly_codes)
    result = []
    for horizon in HORIZONS:
        for market in MARKETS:
            group=[r for r in graded if r.get('market') == market and r.get('tier') in GRADES
                   and (r.get('horizon') or (r.get('trade_plan') or {}).get('horizon') or 'short')==horizon
                   and _finite(r.get('audit_score')) and 0 <= r['audit_score'] <= 100]
            group.sort(key=lambda r:tuple(r.get('sort_key') or [-r['audit_score'],r.get('central_rank') or 999999,r['code']]))
            for index,row in enumerate(group[:PER_MARKET],1):
                result.append({**row,'watch_rank':index,'market_rank':index,'seat_kind':'研究精选','research_window':research_window(row),
                    'weekly_link':row['code'] in weekly,'weekly_reserved':False,'attention_policy':ATTENTION_VERSION})
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
    from review_scorecard import card_valid
    if not card_valid(card):
        return None, "审核分计算策略或贡献与原始分项不一致，保留跟踪待核"
    legacy_projection = bool(card.get('source_audit_id') and card.get('source_audit_id') == row.get('audit_id')
                             and row.get('audit_score') == card.get('legacy_total'))
    if row.get('audit_score') is not None and row['audit_score'] != score and not legacy_projection:
        return None, "发布分与审核分不一致，保留跟踪待核"
    value = assess(card, horizon, plan)
    if value["tier"] != tier or not value["value_confirmed"]:
        return None, "当前审核/收益合同与原等级不一致，保留跟踪待核"
    books, gpt = card["books"], card["gpt"]
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
    """Bound each grade within its own horizon and market, preserving reserves.

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
               "name": row.get("name"), "market": market, "horizon": (row.get("central_trade_plan") or row.get("trade_plan") or {}).get("horizon") or row.get("horizon"), "selected": False,
               "rank": None, "central_rank": None, "metrics": metrics, "reason": error,
               "entry_opportunity": opportunity, "research_window": research_window(row)}
        records[code] = rec
        if error is None:
            rec["sort_key"] = [-metrics["audit_score"], -metrics["book_score"],
                               metrics["remaining_count"], -metrics["rr_band"],
                               -metrics["space_band"], code]
            groups[metrics["horizon"], tier, market].append(rec)
    by_horizon, selected = {}, []
    for horizon in HORIZONS:
        pool=[]
        for tier in GRADES:
            for market in MARKETS:
                group=sorted(groups[horizon,tier,market],key=lambda r:r['sort_key'])
                for central_rank,rec in enumerate(group,1):
                    rec['central_rank']=central_rank
                    pool.append(rec)
        for market in MARKETS:
            group=sorted((r for r in pool if r['market']==market),key=lambda r:r['sort_key'])
            for rank,rec in enumerate(group,1):
                rec.update(rank=rank,market_rank=rank,selected=rank<=PER_MARKET)
                rec['reason']=(f"{HORIZON_LABELS[horizon]} · {market} Top{rank}；原{rec['tier']}审核第{rec['central_rank']}名"
                               if rec['selected'] else f"本周期{market}研究第{rank}，超出本市场Top3；保留评级和原合同跟踪")
                if rec['selected']:selected.append(rec['code'])
        summary={t:{m:{'selected':sum(r['selected'] and r['tier']==t and r['market']==m for r in pool),
            'eligible':sum(r['tier']==t and r['market']==m for r in pool),
            'current':sum(r['horizon']==horizon and r['tier']==t and r['market']==m for r in records.values()),
            'limit':PER_MARKET,'minimum':0,'minimum_gap':0,'vacancies':0}
            for m in MARKETS} for t in GRADES}
        by_horizon[horizon]={'label':HORIZON_LABELS[horizon],'summary':summary,
            'selected_codes':[c for c in selected if records[c]['horizon']==horizon],
            'limit':HORIZON_LIMIT,'minimum':0,'limit_scope':'per_market_in_horizon','per_market_limit':PER_MARKET,
            'research_weeks':list(RESEARCH_WEEKS[horizon]),
            'entry_eligible_count':sum(r['selected'] and r['entry_opportunity']['focus_eligible'] for r in pool),
            'current_count':sum(r['horizon']==horizon for r in records.values())}
    # Compatibility totals are explicit sums; caps apply separately per horizon.
    summary={t:{m:{k:sum(by_horizon[h]['summary'][t][m][k] for h in HORIZONS)
                       for k in ('selected','eligible','current','limit','minimum','minimum_gap','vacancies')}
                for m in MARKETS} for t in GRADES}
    return {"version": VERSION, "rule": RULE, "horizon_limit": HORIZON_LIMIT, "limit_scope": "per_market_in_horizon", "per_market_limit": PER_MARKET,
            "per_grade_limit": dict(GRADE_LIMITS), "per_grade_limits": dict(GRADE_LIMITS),
            "minimums": dict(GRADE_MINIMUMS), "summary": summary, "by_horizon": by_horizon,
            "selected_codes": selected, "records": records,
            "current_count": len(records), "horizons": list(HORIZONS), "selected_count": len(selected),
            "reserve_count": len(records) - len(selected), "no_grade_authority": True,
            "model_calls": 0}
