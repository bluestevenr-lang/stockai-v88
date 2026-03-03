# -*- coding: utf-8 -*-
"""
QualityGuard - 强校验（个股概况硬事实面板）

核心原则：宁可标 WARN/FILTERED，也不得臆造。
强校验字段：行业、得分、水位三字段必须准确可追溯；若不确定，必须明确标注。

规则：
  - 主源 > 备源 > 缓存
  - 强字段缺失或冲突时，标记 WARN/FAIL，必要时 FILTERED
  - 禁止臆造字段值
"""

from typing import Dict, Any, Optional, Tuple


class QualityGuard:
    """数据质量守卫"""

    OK = "OK"
    WARN = "WARN"
    FAIL = "FAIL"

    INDUSTRY_UNKNOWN = "❓ 其他"  # 关键词未匹配时返回，不可追溯

    def validate(self, industry: str, score_total: Any,
                 position_level: str, position_percentile: Optional[float],
                 score_breakdown: Optional[Dict] = None) -> Dict[str, Any]:
        """
        强校验：行业、得分、水位三字段必须准确可追溯；若不确定，标 WARN/FILTERED，不得臆造。

        Args:
            industry: 板块/行业（必须来自主源或备源）
            score_total: 综合得分（必须来自 calculate_metrics_all）
            position_level: 水位（必须来自 get_position_level_unified）
            position_percentile: 百分位 0~100
            score_breakdown: C/A/N/S/L/I/M 拆解（可选）

        Returns:
            {
                "data_quality_flag": "OK"|"WARN"|"FAIL",
                "quality_reason": "原因",
                "pass": bool,
                "field_flags": {"industry": ..., "score": ..., "position": ...},
                "field_reasons": {"industry": "原因", ...},
            }
        """
        issues = []
        field_flags = {"industry": self.OK, "score": self.OK, "position": self.OK}
        field_reasons = {"industry": "", "score": "", "position": ""}

        # 1. 行业校验：不得臆造；"❓ 其他"=关键词未匹配→WARN
        if not industry or industry.strip() == "":
            field_flags["industry"] = self.FAIL
            field_reasons["industry"] = "行业缺失（不得臆造）"
            issues.append("行业缺失")
        elif industry == self.INDUSTRY_UNKNOWN or "❓" in industry:
            field_flags["industry"] = self.WARN
            field_reasons["industry"] = "行业未匹配（关键词无命中，不可追溯）"
            issues.append("行业未确认")
        elif any(m in industry for m in ["其它", "未知", "N/A"]):
            field_flags["industry"] = self.WARN
            field_reasons["industry"] = "行业模糊"
            issues.append("行业分类模糊")

        # 2. 得分校验：必须来自 calculate_metrics_all
        try:
            score_val = float(score_total) if score_total is not None else None
        except (TypeError, ValueError):
            score_val = None
        if score_val is None:
            field_flags["score"] = self.FAIL
            field_reasons["score"] = "得分缺失或不可解析"
            issues.append("得分异常")
        elif score_val < 0 or score_val > 100:
            field_flags["score"] = self.FAIL
            field_reasons["score"] = f"得分超出范围({score_val})"
            issues.append("得分超出范围")

        # 3. 水位校验：必须来自 get_position_level_unified
        valid_levels = ("高", "中", "低", "高位", "中位", "低位")
        if not position_level or position_level == "N/A":
            field_flags["position"] = self.WARN
            field_reasons["position"] = "水位未计算（数据不足）"
            issues.append("水位不可用")
        elif position_level not in valid_levels:
            if "高" not in str(position_level) and "中" not in str(position_level) and "低" not in str(position_level):
                field_flags["position"] = self.WARN
                field_reasons["position"] = "水位格式异常"
                issues.append("水位格式异常")
        if position_percentile is not None and (position_percentile < 0 or position_percentile > 100):
            field_flags["position"] = self.WARN
            field_reasons["position"] = "水位百分位超出0-100"
            issues.append("水位百分位异常")

        # 4. 得分拆解（若有）
        if score_breakdown:
            required_keys = ["C", "A", "N", "S", "L", "I", "M"]
            missing = [k for k in required_keys if k not in score_breakdown]
            if missing:
                issues.append(f"得分拆解缺少:{','.join(missing)}")

        # 综合判定：任一强字段 FAIL → 整体 FAIL
        if field_flags["industry"] == self.FAIL or field_flags["score"] == self.FAIL:
            flag = self.FAIL
            pass_ok = False
        elif len(issues) >= 1:
            flag = self.WARN
            pass_ok = True
        else:
            flag = self.OK
            pass_ok = True

        reason = "; ".join(issues) if issues else "校验通过"

        return {
            "data_quality_flag": flag,
            "quality_reason": reason,
            "pass": pass_ok,
            "field_flags": field_flags,
            "field_reasons": field_reasons,
        }
