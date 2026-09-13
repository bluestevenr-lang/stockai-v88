"""Pure, versioned 3A evidence rubric shared by selectors and display consumers."""
from __future__ import annotations

import hashlib
import json

VERSION = "3a-evidence-v5-tharp"
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
    total = sum(r["score"] for r in indexed.values()) if complete else None
    passed = complete and all(r["score"] >= 15 for r in indexed.values())
    rejected = valid and any(r["score"] == 0 for r in indexed.values())
    expected = "通过" if passed else "否决" if rejected else "不否定"
    valid = bool(valid and (rec.get("thesis_verdict") or rec.get("verdict")) == expected)
    return {"valid": valid, "complete": bool(complete and valid), "total": total if valid else None,
            "passed": bool(passed and valid), "known": known if valid else 0,
            "required": len(CRITERIA), "evidence": evidence if valid else [], "criteria": [
                {"id": key, "title": title, "requirement": requirement,
                 **(indexed[key] if valid else {}),
                 "score": indexed.get(key, {}).get("score") if valid else None,
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
    total = round(100 * passed / len(required), 1) if complete else None
    return {"valid": valid, "complete": complete, "total": total,
            "passed": bool(complete and not failed), "pass_n": passed, "fail_n": failed,
            "missing_n": missing, "required": len(required), "checks": rows if valid else [],
            "applicability": rec.get("applicability") or []}



def choose_conservative(primary, counter):
    """Keep both independent originals; select an actual complete review, never average votes."""
    def order(rec):
        result = gpt_result(rec)
        scores = {r["id"]: r.get("score") for r in result["criteria"]}
        value = [scores.get(k) for k in ("facts","thesis","countercase","risk")]
        return ({"否决":0,"不否定":1,"通过":2}.get(rec.get("thesis_verdict"), -1),
                min((-1 if v is None else v) for v in value), result["total"] if result["total"] is not None else -1)
    role, chosen = min((("primary",primary),("counteraudit",counter)), key=lambda pair: order(pair[1]))
    return {**chosen, "review_pair": {"primary":primary,"counteraudit":counter}, "selected_review":role}


def pair_complete(rec):
    pair = rec.get("review_pair") or {}
    return bool(set(pair) == {"primary","counteraudit"} and all(gpt_result(pair[k])["valid"] and gpt_result(pair[k])["complete"] for k in pair))


def pair_passed(rec):
    pair = rec.get("review_pair") or {}
    return bool(set(pair) == {"primary","counteraudit"} and all(gpt_result(pair[k])["passed"] for k in pair))

def scorecard(gpt: dict, book: dict, *, gpt_current: bool, book_current: bool) -> dict:
    g, b = gpt_result(gpt), book_result(book)
    g["current"], b["current"] = bool(gpt_current and g["valid"] and pair_complete(gpt)), bool(book_current and b["valid"])
    complete = g["complete"] and b["complete"] and g["current"] and b["current"]
    total = min(g["total"], b["total"]) if complete else None
    missing = []
    if not g["current"]:
        missing.append("GPT-6新版审核缺失、过期或事实已变化")
    if not b["current"]:
        missing.append("书籍审核缺失、过期或事实已变化")
    missing += [c["title"] + "待补证" for c in g["criteria"] if c.get("score") is None]
    missing += [c["label"] + "待补证" for c in b["checks"] if c.get("ok") is None]
    result = {"version": VERSION, "score_name": "审核证据分", "formula": "min(GPT五项总分,书籍通过率分)",
              "total": total, "gpt": g, "books": b, "missing": missing,
              "all_passed": bool(complete and g["passed"] and b["passed"] and pair_passed(gpt)),
              "review_pair": gpt.get("review_pair") or {}, "selected_review": gpt.get("selected_review"),
              "double_audit_complete": pair_complete(gpt),
              "probability": None, "legacy_score_weight": 0}
    result["audit_id"] = hashlib.sha256(json.dumps(
        {"rubric": result, "sig": gpt.get("sig"), "pack": gpt.get("factpack_id"),
         "at": gpt.get("ts") or gpt.get("at")}, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    return result


def card_passed(card: dict) -> bool:
    """Consumers recompute arithmetic; a cached green flag is insufficient."""
    g, b = card.get("gpt") or {}, card.get("books") or {}
    rows, checks = g.get("criteria") or [], b.get("checks") or []
    scores = [r.get("score") for r in rows]
    return bool(card.get("version") == VERSION and card.get("all_passed") is True
                and card.get("double_audit_complete") is True
                and all(gpt_result(v)["passed"] for v in (card.get("review_pair") or {}).values())
                and len(card.get("review_pair") or {}) == 2
                and g.get("current") is True and b.get("current") is True
                and len(rows) == 5 and {r.get("id") for r in rows} == set(CRITERIA)
                and all(type(n) is int and n in (15, 20) for n in scores)
                and len(checks) == b.get("required") and len(checks) >= 5
                and len({c.get("id") for c in checks}) == len(checks)
                and all(c.get("ok") is True for c in checks)
                and g.get("total") == sum(scores) and b.get("total") == 100
                and card.get("total") == sum(scores) and sum(scores) >= 75)
