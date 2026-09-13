"""Versioned opportunity hurdle, recomputed from prices, never from a grade.

Projected price upside is conditional, not a promised profit or a probability.
Fees/slippage below are a scenario assumption, not a broker fee quotation.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import math

VERSION = "profit-contract-v2-cycle"
BUY_COST = Decimal("0.005")
SELL_COST = Decimal("0.005")
THRESHOLDS = {"short": {"1A":5, "2A":8, "3A":10},
              "medium": {"1A":10, "2A":15, "3A":20},
              "long": {"1A":20, "2A":30, "3A":40}}
PERIODS = {"short":(1,30),"medium":(31,90),"long":(91,365)}


def number(v):
    if type(v) not in (int, float) or not math.isfinite(v):
        raise ValueError("价格/期限必须为有限数值")
    return Decimal(str(v))


def interval(v):
    if not isinstance(v, (list, tuple)) or len(v) != 2:
        raise ValueError("缺少完整的上下限")
    lo, hi = map(number, v)
    if not 0 < lo <= hi:
        raise ValueError("区间必须为正且下限≤上限")
    return lo, hi


def targets(inputs, horizon):
    """Derive targets from observations or two documented valuation methods."""
    if inputs.get("version") != VERSION or inputs.get("horizon") != horizon:
        raise ValueError("缺少本版同周期目标证据")
    days = inputs.get("max_calendar_days")
    if type(days) is not int or not 1 <= days <= 365:
        raise ValueError("兑现期限必须明确且不超过365天")
    if horizon not in PERIODS or not PERIODS[horizon][0] <= days <= PERIODS[horizon][1]:
        raise ValueError("周期与期限不匹配：短期≤30天，中期31–90天，长期91–365天")
    at = datetime.fromisoformat(str(inputs.get("asof")))
    if at.tzinfo is None:
        raise ValueError("目标证据须带时区日期")
    kind = inputs.get("kind")
    annual = False
    if kind == "observed_20day_boundary" and horizon == "short":
        peaks = inputs.get("peaks") or []
        if (days > 30 or inputs.get("holding_sessions") != 10 or len(peaks) != 2
                or not str(inputs.get("source_url", "")).startswith("https://")):
            raise ValueError("短期目标必须是可追溯的20日边界、10交易日且至多30自然日")
        if len({p.get("date") for p in peaks}) != 2:
            raise ValueError("需要两个不同交易日的实际高点")
        for p in peaks:
            pd = datetime.fromisoformat(str(p["date"])).date()
            if not 0 <= (at.date()-pd).days <= 45:
                raise ValueError("短期高点超出近期窗口")
        lo, hi = sorted(number(p["high"]) for p in peaks)
        if lo <= 0:
            raise ValueError("目标价格无效")
        basis = "近20个完整交易日中最高及次高日高点构成重测止盈带；不能外推一年50%"
    elif kind == "annual_dual_valuation" and horizon in {"medium", "long"}:
        v = inputs.get("valuation") or {}
        documents = v.get("source_documents") or []
        if len({d.get("url") for d in documents}) < 2:
            raise ValueError("年度估值缺少至少两份可追溯原始文档")
        for d in documents:
            if not str(d.get("url", "")).startswith("https://") or not d.get("title"):
                raise ValueError("原始文档须有名称和链接")
            dd = datetime.fromisoformat(str(d.get("asof")))
            if dd.tzinfo is None or not 0 <= (at-dd).total_seconds() <= 120*86400:
                raise ValueError("年度估值源文档超过120天或日期无效")
        if not all(str(v.get(k) or "").strip() for k in
                   ("earnings_bridge", "cashflow_bridge", "valuation_basis", "catalyst", "countercase", "invalidation")):
            raise ValueError("年度估值缺少盈利/现金流桥接、估值依据、催化、反证或失效线")
        eps_lo, eps_hi = interval(v.get("forward_eps_range"))
        pe_lo, pe_hi = interval(v.get("pe_range"))
        cf_lo, cf_hi = interval(v.get("forward_fcf_per_share_range"))
        y_lo, y_hi = interval(v.get("required_fcf_yield_range"))
        if y_hi > 1:
            raise ValueError("现金流收益率须用0到1的小数")
        lo, hi = min(eps_lo*pe_lo, cf_lo/y_hi), min(eps_hi*pe_hi, cf_hi/y_lo)
        basis = "盈利×估值倍数与每股自由现金流÷要求收益率两种方法分别推演，取较低目标；输入预测均是待验证假设"
        annual = horizon == "long"
    else:
        raise ValueError("目标来源未接入可验证模板，禁止用远期高点或愿望收益倒推")
    return lo, hi, days, annual, basis, at


def evaluate(plan, horizon, *, now=None):
    inputs = plan.get("profit_inputs") or {}
    out = {"version": VERSION, "valid": False, "eligible": False,
           "tier_cap": "0A", "net_upside_pct": None, "gross_upside_pct": None,
           "take_profit_range": [], "entry_range": plan.get("entry_range"),
           "cost_assumption": "买入0.5%＋卖出0.5%，合并手续费与滑点假设；不是实际券商报价；未计个人税费、汇率、股息",
           "buy_cost_pct": .5, "sell_cost_pct": .5,
           "formula": "[止盈下沿×(1−0.5%)/(进场上沿×(1＋0.5%))−1]×100",
           "thresholds": THRESHOLDS, "reasons": [], "guaranteed": False}
    try:
        entry_lo, entry_hi = interval(plan.get("entry_range"))
        lo, hi, days, annual, basis, at = targets(inputs, horizon)
        clock = now or datetime.now(at.tzinfo)
        if clock < at or clock > at+timedelta(days=days):
            raise ValueError("目标证据来自未来或本期兑现期限已到，必须重新论证，不能自动续期")
        stop = number(plan.get("stop"))
        if not 0 < stop < entry_lo <= entry_hi < lo <= hi:
            raise ValueError("要求0<失效价<进场下沿≤上沿<止盈下沿≤上沿")
        supplied = plan.get("take_profit_range")
        if supplied is not None and any(abs(a-b) > Decimal("0.0001") for a,b in zip(interval(supplied),(lo,hi))):
            raise ValueError("显示止盈区间与原始证据重算不符")
        purchase = entry_hi*(1+BUY_COST)
        proceeds = lo*(1-SELL_COST)
        loss = purchase-stop*(1-SELL_COST)
        net = (proceeds/purchase-1)*100
        rr = (proceeds-purchase)/loss
        limits = THRESHOLDS[horizon]
        cap = "3A" if net >= limits["3A"] and rr >= 2 else "2A" if net >= limits["2A"] else "1A" if net >= limits["1A"] else "0A"
        reasons = []
        if net < limits["1A"]:
            reasons.append(f"本周期保守净收益空间低于{limits['1A']}%，不予推荐，保留跟踪档案")
        if rr < Decimal("1.5"):
            reasons.append("扣除费用假设后的收益风险比低于1.5，不予推荐")
            cap = "0A"
        out.update(valid=True, eligible=cap!="0A", tier_cap=cap,
                   # Do not round before comparing threshold boundaries.
                   net_upside_pct=float(net), gross_upside_pct=float((lo/entry_hi-1)*100),
                   net_reward_risk=float(rr), take_profit_range=[round(float(lo),4),round(float(hi),4)],
                   stop=float(stop), max_calendar_days=days, holding_sessions=inputs.get("holding_sessions"),
                   thesis_deadline=(at+timedelta(days=days)).isoformat(), evidence_asof=at.isoformat(),
                   annual_evidence=annual, target_basis=basis, reasons=reasons,
                   period_thresholds=limits, high_space_label="一年内净空间≥50%" if annual and net>=50 else None,
                   exit_rule="到达止盈带按卖侧合同处理；到期未兑现转期限到期复核，不自动延长；触及既定失效线先处理风险",
                   next_grade_requirements=f"本周期1A/2A/3A净空间底线为{limits['1A']}%/{limits['2A']}%/{limits['3A']}%；还须满足对应GPT/书理证据成熟度，3A净收益风险比≥2")
    except (ValueError, TypeError, KeyError, InvalidOperation, OverflowError) as exc:
        out["reasons"] = [str(exc)]
    return out


def attach_observed(row, bars):
    """Measured only from complete bars; never change entry to meet a hurdle."""
    peaks = sorted(bars[-20:], key=lambda b: b["high"])[-2:]
    row["profit_inputs"] = {"version": VERSION, "horizon": "short", "kind": "observed_20day_boundary",
                            "max_calendar_days": 30, "holding_sessions": 10,
                            "asof": row["source_timestamps"].get("yahoo_daily") or row["source_timestamps"].get("tencent_daily"),
                            "source_url": row["data_provenance"]["history"],
                            "peaks": [{"date": b["date"], "high": round(b["high"],4)} for b in peaks]}
    contract = evaluate(row, "short")
    row["profit_zone"] = contract["take_profit_range"]
    row["profit_contract"] = contract
    return row
