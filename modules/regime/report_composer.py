# -*- coding: utf-8 -*-
"""
ReportComposer - 报告组装
输出两层结构：
  1) 个股概况（硬字段）
  2) 深度作战室（执行层）
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")


class ReportComposer:
    """报告组装器"""

    def compose_overview(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        个股概况（硬事实面板，12字段）

        核心原则：行业/得分/水位必须准确可追溯；若不确定，宁可标 WARN/FILTERED，也不得臆造。

        字段顺序：1)股票名称 2)代码 3)板块 4)综合得分 5)ESG 6)长期标签 7)短期标签
        8)建议 9)策略摘要 10)资金状态 11)水位 12)现价(附时间戳)
        """
        ts = datetime.now(SHANGHAI)
        return {
            "股票名称": raw.get("股票", raw.get("name", "N/A")),
            "代码": raw.get("代码", raw.get("code", "N/A")),
            "板块": raw.get("板块", raw.get("sector", "N/A")),
            "综合得分": raw.get("得分", raw.get("score", 0)),
            "ESG": raw.get("ESG", raw.get("esg", "N/A")),
            "长期标签": raw.get("长期", raw.get("long_term", "N/A")),
            "短期标签": raw.get("短期", raw.get("short_term", "N/A")),
            "建议": raw.get("建议", raw.get("suggestion", "N/A")),
            "策略摘要": raw.get("策略", raw.get("logic", raw.get("strategy", "N/A"))),
            "资金状态": raw.get("资金", raw.get("capital", "N/A")),
            "水位": raw.get("水位", raw.get("position_level", "N/A")),
            "现价": raw.get("现价", raw.get("close", "N/A")),
            "现价时间戳": ts.strftime("%Y-%m-%d %H:%M:%S %Z"),
        }

    def compose_battle_room(self, regime_info: Dict, risk_probs: Dict,
                            action_label: str, action_emoji: str,
                            action_result: Dict, quality_flag: str) -> Dict[str, Any]:
        """
        深度作战室（六个子模块）

        A) 市场状态简报
        B) 三概率 + Top3原因
        C) 动作标签
        D) 仓位与分批
        E) 失效条件
        F) 执行清单占位
        """
        return {
            "A_市场状态简报": {
                "regime": regime_info.get("regime", "N/A"),
                "confidence": regime_info.get("confidence", 0),
                "drivers_top3": regime_info.get("drivers_top3", []),
            },
            "B_三概率": {
                "p_up_continuation": risk_probs.get("p_up_continuation", 0.5),
                "p_drawdown": risk_probs.get("p_drawdown", 0.5),
                "p_false_breakout": risk_probs.get("p_false_breakout", 0.3),
                "reasons_top3": risk_probs.get("reasons_top3", []),
            },
            "C_动作标签": f"{action_emoji} {action_label}",
            "D_仓位与分批": {
                "suggested_position_range": action_result.get("suggested_position_range", "N/A"),
                "tranche_plan": action_result.get("tranche_plan", "N/A"),
            },
            "E_失效条件": action_result.get("invalidation_rules", []),
            "F_执行清单": "账户/动作/金额或股数/备注（需用户填写）",
            "quality_flag": quality_flag,
        }

    def compose_potential_four_sentences(self, gap_result: Dict[str, Any],
                                          name: str, sector: str,
                                          action_label: str) -> List[str]:
        """
        潜力股必答4句（规则版，无LLM）：
        1) 为什么当前未被充分定价？
        2) 未来1-2季度核心催化剂是什么？
        3) 市场错配点在哪里？
        4) 失效条件是什么？
        """
        tags = gap_result.get("potential_tags", []) or []
        val_gap = gap_result.get("valuation_gap", 0.5)
        cycle = gap_result.get("cycle_position", 0.5)
        delta = gap_result.get("delta_score", 0.5)
        passes = gap_result.get("passes_potential_gate", False)

        s1 = f"当前未被充分定价："
        if val_gap >= 0.7:
            s1 += f"估值相对历史偏低（价格/MA250<0.95），{sector}行业边际改善。"
        elif "估值偏低" in tags or "估值合理" in tags:
            s1 += f"估值处于合理偏低区间，{', '.join(tags[:2])}。"
        else:
            s1 += f"技术结构显示边际改善（delta_score={delta:.2f}），周期位置{cycle:.2f}。"

        s2 = "未来1-2季度催化剂："
        if "周期底部抬升" in tags:
            s2 += "趋势从底部抬升，若放量突破MA60可确认；"
        if "边际改善" in tags:
            s2 += "20日价格斜率转正，动量边际改善；"
        if not any(t in tags for t in ["周期底部抬升", "边际改善"]):
            s2 += "需关注行业政策/订单/财报等催化剂（规则版无数据）。"

        s3 = "市场错配点："
        if passes:
            s3 += f"7选4门槛已过，潜力因子{', '.join(tags) if tags else '技术面'}显示市场尚未充分定价。"
        else:
            s3 += "当前以质量驱动为主，潜力因子待验证。"

        s4 = "失效条件：基本面恶化、跌破关键结构（MA60/MA120）、行业逻辑推翻；价格止损仅辅助。"

        return [s1, s2, s3, s4]

    def compose_eight_mandatory(self, gap_result: Dict[str, Any], long_compound: Dict[str, Any],
                                 margin_result: Dict[str, Any], action_result: Dict[str, Any],
                                 name: str, sector: str, action_label: str) -> Dict[str, str]:
        """
        【长线法宝】每只推荐强制输出8项解释：
        复利逻辑/安全边际/预期差/催化剂/反证/失效条件/仓位节奏/持有期
        """
        tags = gap_result.get("potential_tags", []) or []
        val_gap = gap_result.get("valuation_gap", 0.5)
        delta = gap_result.get("delta_score", 0.5)
        compound_tags = long_compound.get("compound_tags", []) or []
        val_ratio = margin_result.get("valuation_ratio_ma250", 1.0)

        eight = {}

        # 1) 复利逻辑
        if compound_tags:
            eight["复利逻辑"] = f"长期复利框架：{', '.join(compound_tags)}；{sector}行业"
        else:
            eight["复利逻辑"] = f"技术面符合长线条件，{sector}行业边际改善。"

        # 2) 安全边际
        if val_ratio <= 0.95:
            eight["安全边际"] = f"估值合理( price/MA250={val_ratio:.2f} )，具备安全边际"
        elif val_ratio <= 1.05:
            eight["安全边际"] = f"估值中性( price/MA250={val_ratio:.2f} )，谨慎控制仓位"
        else:
            eight["安全边际"] = f"估值偏高( price/MA250={val_ratio:.2f} )，不满足长期重仓安全边际"

        # 3) 预期差
        if tags:
            eight["预期差"] = f"市场未充分定价：{', '.join(tags)}"
        else:
            eight["预期差"] = f"估值分位{val_gap:.2f}，边际斜率{delta:.2f}，存在预期差空间"

        # 4) 催化剂
        if "周期底部抬升" in tags:
            eight["催化剂"] = "趋势从底部抬升，放量突破MA60可确认"
        elif "边际改善" in tags:
            eight["催化剂"] = "20日价格斜率转正，动量边际改善"
        else:
            eight["催化剂"] = "关注行业政策/订单/财报等催化剂（规则版无数据）"

        # 5) 反证
        eight["反证"] = "需警惕：若行业景气下行或技术破位，则逻辑推翻"

        # 6) 失效条件
        inv = action_result.get("invalidation_rules", [])
        eight["失效条件"] = inv[0] if inv else "基本面恶化、跌破关键结构、行业逻辑推翻"

        # 7) 仓位节奏
        eight["仓位节奏"] = action_result.get("tranche_plan", "N/A")

        # 8) 持有期
        eight["持有期"] = action_result.get("holding_period", "N/A")

        return eight

    def compose_long_compounder_eight(self, gap_result: Dict[str, Any], lc_result: Dict[str, Any],
                                      action_res: Dict[str, Any], name: str, sector: str,
                                      action_label: str, valuation_pct: float) -> Dict[str, str]:
        """
        【长线法宝】每只推荐强制输出 8 项解释：
        复利逻辑/安全边际/预期差/催化剂/反证/失效条件/仓位节奏/持有期
        """
        tags = gap_result.get("potential_tags", []) or []
        val_gap = gap_result.get("valuation_gap", 0.5)
        delta = gap_result.get("delta_score", 0.5)
        lc_score = lc_result.get("long_compounder_score", 50)
        inv_rules = action_res.get("invalidation_rules", [])
        tranche = action_res.get("tranche_plan", "-")
        period = action_res.get("holding_period", "N/A")
        cap = action_res.get("position_cap_percent", 25)

        comp_logic = f"复利逻辑：长期复利分{lc_score:.0f}，护城河/ROIC/FCF代理因子综合；{sector}行业。"
        if lc_score >= 65:
            comp_logic += "质量达标，可纳入长期跟踪。"
        else:
            comp_logic += "质量待验证，建议观察。"

        safety = f"安全边际：估值分位proxy={valuation_pct:.2f}；"
        if val_gap >= 0.7:
            safety += "估值合理偏低，安全边际较足。"
        elif valuation_pct >= 0.8:
            safety += "估值偏高，禁止长期重仓。"
        else:
            safety += "估值中性，需结合催化剂。"

        exp_gap = f"预期差：估值分位{val_gap:.2f}、边际改善delta={delta:.2f}；"
        if "边际改善" in tags:
            exp_gap += "技术面边际改善已显现。"
        elif "估值偏低" in tags or "估值合理" in tags:
            exp_gap += "估值端有预期差空间。"
        else:
            exp_gap += "预期差待验证。"

        catalyst = "催化剂："
        if "周期底部抬升" in tags:
            catalyst += "趋势从底部抬升，放量突破MA60可确认；"
        if "边际改善" in tags:
            catalyst += "20日价格斜率转正；"
        if not any(t in tags for t in ["周期底部抬升", "边际改善"]):
            catalyst += "需关注行业政策/订单/财报（规则版无数据）。"

        counter = "反证：若估值持续高企、边际恶化、或行业逻辑推翻，则结论失效。"
        inval = "；".join(inv_rules[:3]) if inv_rules else "基本面恶化、跌破关键结构、行业逻辑推翻"

        return {
            "复利逻辑": comp_logic,
            "安全边际": safety,
            "预期差": exp_gap,
            "催化剂": catalyst,
            "反证": counter,
            "失效条件": inval,
            "仓位节奏": f"{tranche}（单标上限{cap}%）",
            "持有期": period,
        }
