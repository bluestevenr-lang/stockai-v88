"""1A/2A are evidenced investment ideas awaiting maturity, never missing reviews."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math

VALUE_CHECKS = {
    "short": {"trend"},
    "medium": {"earnings", "cash"},
    "long": {"earnings", "cash", "quality", "history", "invalidation"},
}
VALUE_GPT = {"facts", "thesis", "countercase", "risk"}


def assess(card, horizon, plan=None):
    """Require four GPT value safeguards AND the book's value foundations."""
    g, b = card.get("gpt") or {}, card.get("books") or {}
    scores = {c["id"]: c.get("score") for c in g.get("criteria") or []}
    checks = {c["id"]: c for c in b.get("checks") or []}
    foundations = VALUE_CHECKS.get(horizon, set()) | {"profit", "tharp_risk"}
    if horizon in {"short", "medium"}:
        foundations |= {"stop", "payoff"}
    full = bool(g.get("valid") and b.get("valid") and g.get("current") and b.get("current")
                and g.get("complete") and b.get("complete")
                and card.get("double_audit_complete") is True)
    from review_scorecard import gpt_result
    pair = card.get("review_pair") or {}
    def research_floor(review):
        r = gpt_result(review)
        s = {x["id"]:x.get("score") for x in r["criteria"]}
        return bool(r["valid"] and r["complete"] and all(s.get(k,0) >= (15 if k in {"facts","thesis","risk"} else 10) for k in VALUE_GPT | {"horizon"}))
    value_gpt = bool(full and len(pair)==2 and all(research_floor(r) for r in pair.values()))
    negative_expectancy = (checks.get("tharp_expectancy", {}).get("evidence") or {}).get("status") == "NONPOSITIVE_HISTORY"
    value_books = bool(full and not negative_expectancy and foundations and all(checks.get(k, {}).get("ok") is True for k in foundations))
    geometry = True
    if horizon in {"short", "medium"}:
        evidence = checks.get("payoff", {}).get("evidence") or {}
        target, stop = evidence.get("target"), evidence.get("stop")
        geometry = bool(type(target) in (int, float) and type(stop) in (int, float) and 0 < stop < target)
    if horizon == "long":
        pe = (checks.get("valuation", {}).get("evidence") or {}).get("fundamentals.pe_ttm")
        geometry = bool(type(pe) in (int, float) and math.isfinite(pe) and pe > 0)
    from profit_contract import evaluate
    from review_scorecard import pair_passed
    profit = evaluate(plan or {}, horizon)
    confirmed = bool(value_gpt and value_books and geometry and profit["eligible"] and (card.get("total") or 0) >= 60)
    unmet = [{"id": c["id"], "title": c.get("label"), "condition": c.get("threshold"),
              "observed": c.get("detail"), "book": c.get("book")}
             for c in checks.values() if c.get("ok") is not True]
    value_gaps = []
    if full:
        labels = {"facts": "事实可信", "thesis": "逻辑成立", "risk": "风险与失效"}
        for key, label in labels.items():
            evidence = []
            for role, review in pair.items():
                criterion = next((x for x in gpt_result(review)["criteria"] if x["id"] == key), {})
                score = criterion.get("score")
                if type(score) in (int, float) and score < 15:
                    evidence.append(f"{role}：{score}分；{criterion.get('reason') or '该项未达价值基础底线'}")
            if evidence:
                value_gaps.append(label)
                unmet.append({"id": f"gpt_{key}", "title": f"{label}未达1A价值底线",
                              "condition": "两份GPT审核该项均≥15分", "observed": "；".join(evidence)})
    if scores.get("horizon") == 10:
        unmet.append({"id": "gpt_horizon", "title": "兑现周期仍需确认", "condition": "GPT-6周期匹配项≥15分", "observed": "当前10分"})
    if scores.get("countercase") == 10:
        unmet.append({"id":"gpt_countercase","title":"反证尚待处理","condition":"GPT-6反证项≥15分，明确解决当前反例","observed":"当前10分；只允许研究跟踪"})
    explicit_gpt_failure = bool(g.get('valid') and g.get('current') and g.get('complete')
                                and card.get('double_audit_complete') is True
                                and any(scores.get(k) == 0 for k in VALUE_GPT))
    invalid = bool(explicit_gpt_failure or full and (negative_expectancy or any(scores.get(k) == 0 for k in VALUE_GPT)
                             or any(checks.get(k, {}).get("ok") is False for k in foundations)
                             or not geometry))
    if profit["reasons"]:
        unmet += [{"id": "profit_contract", "title": "收益空间硬门槛", "condition": "同周期明确区间、期限≤365天、净空间≥5%且净收益风险比≥1.5", "observed": ";".join(profit["reasons"])}]
    if confirmed and card.get("all_passed") and profit["tier_cap"] == "3A" and card["total"]>=75:
        tier = "3A"
    elif confirmed:
        tier = "2A" if pair_passed(card) and card["total"]>=75 and len(unmet)<=1 and profit["tier_cap"] in {"2A", "3A"} else "1A"
    elif profit["valid"] and not profit["eligible"]:
        tier = "0A"
    else:
        tier = "0A" if invalid else "PENDING"
    space_excluded = bool(profit["valid"] and not profit["eligible"])
    return {"tier": tier, "assessment_version": "value-maturity-v3-tharp", "value_confirmed": confirmed,
            "value_status": "价值已确认" if confirmed else "收益空间不达标" if space_excluded else "价值条件失效" if invalid else "价值基础尚未通过" if value_gaps else "价值待证",
            "profit_contract": profit, "near_3a": bool(tier == "2A" and profit["tier_cap"] == "3A" and len(unmet) == 1),
            "gpt_value_requirements": sorted(VALUE_GPT), "book_value_requirements": sorted(foundations),
            "basis": "双份GPT完整评分；1A事实/逻辑/风险各≥15，反证/周期各≥10，审核分≥60；2A五项各≥15且审核分≥75、至多一项成熟条件未达；3A双审与全部书理通过。所有档位另过本周期收益空间和资本保护底线",
            "remaining_conditions": unmet,
            "stage_note": {"3A": "本周期净空间达标且GPT双审与书籍全部通过，执行另查触发和限制", "2A": "本周期2A净空间达标且GPT五项全通过；至多等待一项成熟条件",
                           "1A": "本周期1A空间与价值基础达标；反证/周期争议及触发条件列明，保留研究跟踪", "0A": ";".join(profit["reasons"]) if space_excluded else "价值或风险条件不合格，保留退出原因",
                           "PENDING": f"双审已完成，但GPT价值基础未达1A底线：{'、'.join(value_gaps)}；暂不推荐，须补论证" if value_gaps else "审核或事实不完整，不能冒充有价值的1A"}[tier]}


def watch_plan(assessment, plan, horizon, *, now=None):
    now = now or datetime.now(timezone(timedelta(hours=8)))
    remaining = assessment.get("remaining_conditions") or []
    last, stop, target = (plan.get(k) for k in ("last", "stop", "target"))
    ceilings = []
    if all(type(v) in (int, float) and math.isfinite(v) for v in (stop, target)) and 0 < stop < target:
        ceilings.append((target + 2 * stop) / 3)
        if horizon == "short":
            ceilings.append(stop / .92)
    return {"purpose": "时间与空间的跟踪条件；不构成当前买单",
            "current_price": last, "entry_condition": plan.get("promotion_trigger") or "待完善价格触发",
            "price_watch_ceiling": round(min(ceilings), 4) if ceilings else None,
            "price_formula": "收益风险比≥2反推入场上限；短期再取止错距离≤8%的上限；触发及失效线变化后重算",
            "invalidation": plan.get("invalidation") or "待明确失效条件",
            "horizon": horizon, "remaining_conditions": remaining,
            "next_review_due": (now + timedelta(hours=24)).isoformat(),
            "profit_contract": assessment.get("profit_contract"),
            "promotion": "按对应周期1A/2A/3A空间底线升级，另核GPT评分、书理成熟度及净收益风险比；一年50%仅高空间标记，禁止抬目标凑档位",
            "retention": "时间未到、价格未到、扫描落选不移除；新证据导致降级时保留沿革，价值失效进入退出档案"}


def apply_classification(classification, assessment):
    tier = assessment["tier"]
    if tier == "3A":
        return classification
    out = dict(classification)
    out.update(tier=tier, label=assessment["stage_note"],
               state={"2A": "OBSERVE_2A", "1A": "RESEARCH_1A", "PENDING": "PENDING_REVIEW", "0A": "EXCLUDED_0A"}[tier],
               publish_eligible=False, formal_recommendation=False, conditional_eligible=False)
    out["quality"] = {**(out.get("quality") or {}), "tier": tier,
                      "value_confirmed": assessment["value_confirmed"], "subtype": "VALUE_MATURITY", "model_pair_pass": False}
    reasons = [r for r in out.get("reason_codes") or [] if not str(r).startswith("MODEL_OR_CLASSICS_REJECTED:")]
    out["action_blocks"] = [r for r in out.get("action_blocks") or [] if not str(r).startswith("QUALITY_")] + ["VALUE_MATURITY_INCOMPLETE"]
    out["reason_codes"] = list(dict.fromkeys(reasons + [assessment["stage_note"]]
                            + [str(c["title"]) + "：" + str(c["condition"]) for c in assessment["remaining_conditions"]]))
    return out
