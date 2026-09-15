"""Pure, versioned 3A evidence rubric shared by selectors and display consumers."""
from __future__ import annotations

import hashlib
import json
from copy import deepcopy

VERSION = "3a-evidence-v5-tharp"  # Original atomic rubric and signed drafts remain valid.
SCORE_POLICY_VERSION = "3a-weighted-evidence-v1"
GPT_WEIGHTS = {"facts":20, "thesis":25, "countercase":20, "horizon":10, "risk":25}
BOOK_WEIGHTS = {
    "short": {"trend":10, "entry":12, "volume":8, "stop":15, "payoff":12, "profit":15, "tharp_risk":15, "tharp_expectancy":13},
    "medium": {"trend":8, "entry":8, "volume":6, "stop":12, "payoff":10, "earnings":10, "cash":8, "profit":13, "tharp_risk":13, "tharp_expectancy":12},
    "long": {"earnings":14, "cash":12, "quality":10, "valuation":12, "history":6, "invalidation":12, "profit":12, "tharp_risk":12, "tharp_expectancy":10},
}


def score_policy():
    return {"version": SCORE_POLICY_VERSION, "gpt_weights": dict(GPT_WEIGHTS),
            # Classic IDs are values, never private-account-shaped JSON keys.
            "book_weights": {h:[{"id":key,"weight_pct":weight} for key,weight in weights.items()]
                             for h,weights in BOOK_WEIGHTS.items()},
            "formula": "min(保守原稿GPT加权分,适用书理加权分)",
            "missing": "缺项总分为空；保留已核贡献与覆盖率，不重新分配缺项权重",
            "parameter_status": "待校准V88参数；不是书籍原文数字、胜率或收益证明"}

CRITERIA = {
    "facts": ("事实可信", "核对来源、时点、字段冲突和关键数据缺口"),
    "thesis": ("逻辑成立", "说明具体机会或退出逻辑及其因果证据，不能复述旧评级"),
    "countercase": ("反证检验", "提出最强反例，说明什么事实支持或推翻该反例"),
    "horizon": ("周期匹配", "证据对应申报持有周期，不用短期波动代替长期逻辑"),
    "risk": ("风险与失效", "量化损失边界，给出可检验的失效条件和风险处理"),
}
BOOK_IDS = {
    "short": ["trend", "entry", "volume", "stop", "payoff", "profit", "tharp_risk", "tharp_expectancy"],
    "medium": ["trend", "entry", "volume", "stop", "payoff", "earnings", "cash", "profit", "tharp_risk", "tharp_expectancy"],
    "long": ["earnings", "cash", "quality", "valuation", "history", "invalidation", "profit", "tharp_risk", "tharp_expectancy"],
}
RUBRIC = (
    "范K萨普《通向财务自由之路》(Trade Your Way to Financial Freedom)：用初始风险R、R倍数分布、交易期望、仓位与退出评价系统；不得误署《专业投机原理》的作者。"
    "核对tharp_risk及conditional_path_evidence：目标收益风险比不是交易期望；未触发不得算盈利交易，样本不足不能宣称正期望。"
    "conditional_path_evidence是按历史当时可见数据重放、下一开盘条件成交、收盘失效后下一开盘退出、扣双边成本的回顾性统计；不是未来胜率或真正样本外战绩。"
    "收益合同：买侧核对profit_inputs及profit_contract。进场上沿到止盈下沿按买卖各0.5%费用/滑点假设算净空间。"
    "周期必须匹配：短期≤30天，中期31–90天，长期91–365天；1A/2A/3A净空间底线短期5%/8%/10%，中期10%/15%/20%，长期20%/30%/40%。一年50%仅额外高空间标记。"
    "评级和各项评分分开：3A须五项均通过及书理全部通过；1A容许反证与周期仍有非致命争议，但事实、逻辑、风险须证据充分。你应据实给10/15/20分，不能为了授级改变评分锚点。"
    "数值空间不等于可实现预测：必须独立反查等待入场路径、止盈带、中间阻力、未触发及到期失效。若关键路径无依据，周期项不得≥15。"
    "周期项必须核对目标所需涨幅、目标来源/中间阻力、持有交易日与历史路径样本。仅有均线和止损不构成周期印证。统计窗口不是预测胜率；缺乏可达性证据不得给周期15/20分。"
    "五项必须逐项独立评估，每项0/10/15/20分或null。null=必要事实缺失；"
    "0=有明确反证否定本项；10=有支持但关键争议未解决；"
    "15=证据足够、主要反证已处理、失效条件可核验；"
    "20=满足15分并有至少两个不同数值字段交叉印证。"
    "15分及以上必须引用evidence中至少一个字段，20分至少两个不同字段。"
    "每项写具体reason及evidence_fields，不能空话或把评分称为胜率。"
    "只有五项均>=15且总分>=75才能thesis_verdict=通过；"
    "任何0分必须否决；有null或10分只能不否定（仍待补证）。"
    "现在买/卖与等触发单独判断，不因为未触发而否定完整的机会逻辑。"
    "买入逻辑包含可检验的有条件关注价值；只缺价格/量能成熟但有证据支持明确达标路径时，"
    "不要仅因现在不宜买而否定事实、逻辑、反证、风险四项。仍须独立验证路径，不能为了输出等级而抬分。"
)


def gpt_result(rec: dict) -> dict:
    rows = rec.get("criteria")
    valid = isinstance(rows, list) and len(rows) == len(CRITERIA)
    indexed = {}
    evidence = rec.get("evidence") or []
    fields = {e.get("field") for e in evidence if isinstance(e, dict)}
    if valid:
        for row in rows:
            if not isinstance(row, dict) or set(row) != {"id", "score", "reason", "evidence_fields"}:
                valid = False
                break
            key, score, refs = row["id"], row["score"], row["evidence_fields"]
            if (key not in CRITERIA or key in indexed
                    or not (score is None or type(score) is int and score in (0, 10, 15, 20))
                    or not isinstance(row["reason"], str) or not row["reason"].strip()
                    or not isinstance(refs, list) or any(not isinstance(f, str) for f in refs)
                    or len(refs) != len(set(refs)) or not set(refs).issubset(fields)
                    or score is not None and score >= 15 and len(refs) < (2 if score == 20 else 1)
                    or score == 0 and not refs):
                valid = False
                break
            indexed[key] = row
    valid = bool(valid and set(indexed) == set(CRITERIA))
    known = sum(r.get("score") is not None for r in indexed.values()) if valid else 0
    complete = valid and known == len(CRITERIA)
    legacy_total = sum(r["score"] for r in indexed.values()) if complete else None
    total = round(sum(GPT_WEIGHTS[k] * r["score"] / 20 for k,r in indexed.items()), 4) if complete else None
    passed = complete and all(r["score"] >= 15 for r in indexed.values())
    rejected = valid and any(r["score"] == 0 for r in indexed.values())
    expected = "通过" if passed else "否决" if rejected else "不否定"
    valid = bool(valid and (rec.get("thesis_verdict") or rec.get("verdict")) == expected)
    known_contribution = round(sum(GPT_WEIGHTS[k] * r["score"] / 20 for k,r in indexed.items() if r["score"] is not None), 4) if valid else 0
    coverage = sum(GPT_WEIGHTS[k] for k,r in indexed.items() if r["score"] is not None) if valid else 0
    return {"valid": valid, "complete": bool(complete and valid), "total": total if valid else None,
            "legacy_total": legacy_total if valid else None, "score_policy": score_policy(),
            "known_contribution": known_contribution, "coverage_pct": coverage,
            "passed": bool(passed and valid), "known": known if valid else 0,
            "required": len(CRITERIA), "evidence": deepcopy(evidence) if valid else [], "criteria": [
                {"id": key, "title": title, "requirement": requirement,
                 **(deepcopy(indexed[key]) if valid else {}),
                 "score": indexed.get(key, {}).get("score") if valid else None,
                 "weight_pct": GPT_WEIGHTS[key],
                 "contribution": round(GPT_WEIGHTS[key] * indexed[key]["score"] / 20, 4) if valid and indexed[key]["score"] is not None else None,
                 "reason": indexed.get(key, {}).get("reason", "待GPT-6按新版评分表审核") if valid else "待GPT-6按新版评分表审核"}
                for key, (title, requirement) in CRITERIA.items()]}


def book_result(rec: dict) -> dict:
    required = BOOK_IDS.get(rec.get("horizon"), [])
    rows = rec.get("checks") or []
    valid = bool(rec.get("rubric_version") == VERSION and required
                 and isinstance(rows, list) and len(rows) == len(required)
                 and all(isinstance(c, dict) and c.get("ok") in (True, False, None)
                         and (c.get("ok") is None or type(c.get("ok")) is bool)
                         and c.get("label") and c.get("detail") and c.get("book")
                         and c.get("threshold") and c.get("evidence") for c in rows)
                 and {c.get("id") for c in rows} == set(required))
    passed = sum(c["ok"] is True for c in rows) if valid else 0
    missing = sum(c["ok"] is None for c in rows) if valid else len(required)
    failed = len(required) - missing - passed if valid else 0
    complete = bool(valid and not missing)
    weights = BOOK_WEIGHTS.get(rec.get("horizon"), {})
    legacy_total = round(100 * passed / len(required), 1) if complete else None
    contribution = sum(weights[c["id"]] for c in rows if c["ok"] is True) if valid else 0
    coverage = sum(weights[c["id"]] for c in rows if c["ok"] is not None) if valid else 0
    total = contribution if complete else None
    return {"valid": valid, "complete": complete, "total": total,
            "horizon": rec.get("horizon"), "score_policy": score_policy(), "legacy_total": legacy_total,
            "known_contribution": contribution, "coverage_pct": coverage,
            "passed": bool(complete and not failed), "pass_n": passed, "fail_n": failed,
            "missing_n": missing, "required": len(required), "checks": [
                {**deepcopy(c),"weight_pct":weights[c["id"]],"contribution":weights[c["id"]] if c["ok"] is True else 0 if c["ok"] is False else None}
                for c in rows] if valid else [],
            "applicability": deepcopy(rec.get("applicability") or [])}



def choose_conservative(primary, counter, *, weighted=False):
    """Preserve signed envelope selection; weighted=True derives the new score view.

    Original envelopes used equal totals to break ties. Their authentication
    remains unchanged; the scorecard independently selects with weighted totals.
    Both paths choose one actual original, never synthesize or average a review.
    """
    def order(rec):
        result = gpt_result(rec)
        scores = {r["id"]: r.get("score") for r in result["criteria"]}
        value = [scores.get(k) for k in ("facts","thesis","countercase","risk")]
        return ({"否决":0,"不否定":1,"通过":2}.get(rec.get("thesis_verdict"), -1),
                min((-1 if v is None else v) for v in value), result["total" if weighted else "legacy_total"] if result["total"] is not None else -1)
    role, chosen = min((("primary",primary),("counteraudit",counter)), key=lambda pair: order(pair[1]))
    return {**chosen, "review_pair": {"primary":primary,"counteraudit":counter}, "selected_review":role}


def pair_complete(rec):
    pair = rec.get("review_pair") or {}
    return bool(set(pair) == {"primary","counteraudit"} and all(gpt_result(pair[k])["valid"] and gpt_result(pair[k])["complete"] for k in pair))


def pair_valid(rec):
    """A current, authentic draft can explicitly leave evidence incomplete."""
    pair = rec.get("review_pair") or {}
    return bool(set(pair) == {"primary","counteraudit"} and all(gpt_result(pair[k])["valid"] for k in pair))


def pair_passed(rec):
    pair = rec.get("review_pair") or {}
    return bool(set(pair) == {"primary","counteraudit"} and all(gpt_result(pair[k])["passed"] for k in pair))


def _progress(g, b, pair):
    partial = not pair_complete({'review_pair':pair})
    pair_results = [gpt_result(r) for r in pair.values()] if partial and g['current'] else [g]
    known = min(r['known_contribution'] for r in pair_results) if g['current'] else 0
    coverage = min(r['coverage_pct'] for r in pair_results) if g['current'] else 0
    return (min(known,b['known_contribution'] if b['current'] else 0),
            min(coverage,b['coverage_pct'] if b['current'] else 0))


def scorecard(gpt: dict, book: dict, *, gpt_current: bool, book_current: bool, source_audit_id=None) -> dict:
    pair = gpt.get("review_pair") or {}
    chosen = choose_conservative(pair["primary"], pair["counteraudit"], weighted=True) if set(pair) == {"primary","counteraudit"} else gpt
    g, b = gpt_result(chosen), book_result(book)
    g["current"], b["current"] = bool(gpt_current and g["valid"] and pair_valid(gpt)), bool(book_current and b["valid"])
    complete = g["complete"] and b["complete"] and g["current"] and b["current"] and pair_complete(gpt)
    total = min(g["total"], b["total"]) if complete else None
    # Historical comparison is the original envelope's arithmetic, not a new rank.
    original_g = gpt_result(gpt)
    legacy_total = min(original_g["legacy_total"], b["legacy_total"]) if complete and original_g["legacy_total"] is not None else None
    missing = []
    if not g["current"]:
        missing.append("GPT-6新版审核缺失、过期或事实已变化")
    if not b["current"]:
        missing.append("书籍审核缺失、过期或事实已变化")
    if not pair_complete(gpt):
        missing.append("双份GPT原审尚有缺项")
    missing += [c["title"] + "待补证" for c in g["criteria"] if c.get("score") is None]
    missing += [c["label"] + "待补证" for c in b["checks"] if c.get("ok") is None]
    known, coverage = _progress(g,b,pair)
    result = {"version": VERSION, "score_name": "加权审核证据分", "formula": score_policy()["formula"],
              "score_policy": score_policy(), "total": total, "legacy_total": legacy_total,
              "known_contribution": known, "coverage_pct": coverage,
              "gpt": g, "books": b, "missing": missing,
              "all_passed": bool(complete and g["passed"] and b["passed"] and pair_passed(gpt)),
              "review_pair": deepcopy(pair), "selected_review": chosen.get("selected_review"),
              "source_selected_review": gpt.get("selected_review"),
              "source_audit_id": source_audit_id,
              "double_audit_complete": pair_complete(gpt),
              "probability": None, "legacy_score_weight": 0}
    result["audit_id"] = hashlib.sha256(json.dumps(
        {"rubric": result, "sig": gpt.get("sig"), "pack": gpt.get("factpack_id"),
         "at": gpt.get("ts") or gpt.get("at")}, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    return result


def _same(left, right):
    # JSON comparison also rejects bools masquerading as numeric weights/scores.
    return json.dumps(left, sort_keys=True, ensure_ascii=False) == json.dumps(right, sort_keys=True, ensure_ascii=False)


def card_valid(card: dict) -> bool:
    """Recompute policy, contributions and flags; source authenticity is checked upstream.

    Legacy saved cards are historical only. current_scorecard can re-project
    their authentic atomic reviews under this policy without changing originals.
    """
    try:
        if card.get("version") != VERSION or not _same(card.get("score_policy"), score_policy()):
            return False
        g, b = card["gpt"], card["books"]
        pair = card.get("review_pair") or {}
        if set(pair) != {"primary", "counteraudit"}:
            return False
        chosen = choose_conservative(pair["primary"], pair["counteraudit"], weighted=True)
        expected_g = gpt_result(chosen)
        expected_b = book_result({"rubric_version": VERSION, "horizon": b.get("horizon"),
                                  "checks": b.get("checks"), "applicability": b.get("applicability")})
        if not expected_g["valid"] or not expected_b["valid"]:
            return False
        if type(g.get("current")) is not bool or type(b.get("current")) is not bool:
            return False
        if g["current"] and not pair_valid({"review_pair":pair}):
            return False
        if not _same(g, {**expected_g,"current":g["current"]}) or not _same(b, {**expected_b,"current":b["current"]}):
            return False
        complete = expected_g["complete"] and expected_b["complete"] and g["current"] and b["current"] and pair_complete({'review_pair':pair})
        expected_total = min(expected_g["total"], expected_b["total"]) if complete else None
        all_passed = bool(complete and expected_g["passed"] and expected_b["passed"] and pair_passed({"review_pair":pair}))
        known, coverage = _progress(g,b,pair)
        original = choose_conservative(pair["primary"], pair["counteraudit"])
        original_total = gpt_result(original)["legacy_total"]
        expected_legacy = min(original_total,expected_b["legacy_total"]) if complete and original_total is not None else None
        return bool(_same(card.get("total"),expected_total)
                    and card.get("score_name")=="加权审核证据分"
                    and card.get("formula")==score_policy()["formula"]
                    and card.get("probability") is None and type(card.get("legacy_score_weight")) is int and card["legacy_score_weight"]==0
                    and _same(card.get("legacy_total"),expected_legacy)
                    and _same(card.get("all_passed"),all_passed)
                    and _same(card.get("double_audit_complete"),pair_complete({"review_pair":pair}))
                    and card.get("selected_review")==chosen.get("selected_review")
                    and card.get("source_selected_review") in (None,original.get("selected_review"))
                    and _same(card.get("known_contribution"),known) and _same(card.get("coverage_pct"),coverage))
    except (KeyError, TypeError, ValueError):
        return False


def card_passed(card: dict) -> bool:
    return bool(card_valid(card) and card.get("all_passed") is True
                and card.get("gpt",{}).get("current") is True and card.get("books",{}).get("current") is True
                and card.get("total") is not None and card["total"] >= 75)
