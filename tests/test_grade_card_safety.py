import unittest

from grade_card import board_html, system_table_html


def blocked_3a_row():
    return {
        "code": "000783.SZ",
        "name": "长江证券",
        "tier": "3A",
        "display_tier": "3A候选",
        "subtype": "完整机会",
        "action_state": "现在可进",
        "display_action": "待复核·不可执行",
        "listable": False,
        "rank_score": 78,
        "rank": 1,
        "verification": {"rules_gate": "gate_pass", "why": "双剑未形成明确共识"},
        "execution_contract": {
            "ready": False,
            "snapshot": {"last": 9.04, "zone": [8.77, 9.04],
                         "mode": "不进", "exec_action": "不进",
                         "data_asof": "2026-08-14"},
        },
        "triggers": {"enter": "禁止执行", "invalid": "跌破止损8.42"},
        "buckets": {},
        "missing": [],
    }


def central_rank_row(code: str, name: str) -> dict:
    return {
        "code": code, "name": name, "tier": "3A", "listable": True,
        "rank_score": 88, "rank": 1, "subtype": "旧分类",
        "action_state": "旧口径现买", "buckets": {}, "missing": [],
        "execution_contract": {
            "ready": True,
            "snapshot": {"last": 1.0, "zone": [1.0, 2.0], "mode": "旧现买"},
        },
        "triggers": {"enter": "旧触发", "invalid": "旧失效"},
    }


def central_row(code: str, name: str, tier: str, state: str,
                *, publish: bool = False) -> dict:
    return {
        "code": code, "name": name, "tier": tier, "state": state,
        "publish_eligible": publish,
        "trade_plan": {
            "ready": True, "last": 10.3, "entry_range": "10.20~10.60",
            "promotion_trigger": "放量站上10.60", "stop": 9.7,
            "target": 12.5, "rr": 2.4, "invalidation": "收盘跌破9.70",
            "horizon": "short", "position_cap": "不超过10%",
        },
    }


def central_v2() -> dict:
    return {
        "version": "gpt-triad-selection-v2-horizon-axes", "factpack_id": "p" * 64,
        "recommendations": [
            central_row("A", "现买股", "3A", "3A_PUBLISHABLE", publish=True)],
        "blocked_3a": [
            central_row("G", "冻结股", "3A", "3A_BLOCKED", publish=True)],
        "preparations": [central_row("B", "准备股", "3A", "3A_PREPARE")],
        "conditional": [central_row("C", "条件股", "2A", "2A_CONDITIONAL")],
        "observations": [
            central_row("D", "分歧股", "2A", "OBSERVE_2A"),
            central_row("E", "研究股", "1A", "RESEARCH_1A"),
        ],
        "pending": [central_row("F", "待审股", "PENDING", "PENDING_REVIEW")],
        "excluded": [],
    }


class GradeCardSafetyTest(unittest.TestCase):
    def test_blocked_bucket_3a_never_enters_action_cards(self):
        html = board_html({"rows": [blocked_3a_row()]})
        self.assertNotIn("长江证券", html)
        self.assertIn("今日无", html)

    def test_blocked_bucket_3a_is_visually_a_non_executable_candidate(self):
        html = system_table_html(
            {"rows": [blocked_3a_row()], "archived": [], "coverage": {},
             "verification": {}},
            {"rows": []}, {}, {},
        )
        self.assertIn("IN: 3A现买×0", html)
        self.assertIn("3A候选", html)
        self.assertIn("待复核·不可执行", html)
        self.assertIn("研究参考·非买单", html)
        self.assertNotIn("IN: 3A现买×1", html)


class GradeCardTriadV2Test(unittest.TestCase):
    def test_v1_with_new_bucket_names_does_not_masquerade_as_v2(self):
        triad = central_v2()
        triad["version"] = "gpt-triad-selection-v1"
        rank = {"rows": [central_rank_row("A", "旧候选")], "archived": [],
                "coverage": {}, "verification": {}}

        html = system_table_html(rank, {"rows": []}, {}, {}, triad=triad)

        self.assertIn("中央 triad_selection v2 未就绪", html)
        self.assertIn("旧口径参考·不可执行", html)
        self.assertIn("3A现买×0", html)

    def test_explicit_v2_schema_is_supported(self):
        triad = central_v2()
        triad["version"] = "custom-producer"
        triad["schema_version"] = 2

        html = system_table_html({"rows": []}, {"rows": []}, {}, {}, triad=triad)

        self.assertIn("3A现买×1", html)
        self.assertNotIn("中央 triad_selection v2 未就绪", html)

    def test_displays_all_states_but_only_recommendation_executes(self):
        triad = central_v2()
        rank = {"rows": [central_rank_row(code, name) for code, name in (
            ("A", "现买股"), ("B", "准备股"), ("C", "条件股"),
            ("D", "分歧股"), ("E", "研究股"), ("F", "待审股"),
        )], "archived": [], "coverage": {}, "verification": {}}

        html = system_table_html(rank, {"rows": []}, {}, {}, triad=triad)

        for text in ("3A现买×1", "3A冻结×1", "3A准备×1", "2A条件×1",
                     "2A分歧×1", "1A研究×1", "PENDING×1"):
            self.assertIn(text, html)
        for label in ("3A现买", "3A冻结", "冻结·不可执行", "3A准备", "2A条件",
                      "2A分歧观察", "1A研究", "PENDING"):
            self.assertIn(label, html)
        self.assertEqual(html.count("现在可进"), 1)
        self.assertIn("研究参考·非买单", html)

    def test_non_publishable_recommendation_is_not_counted_as_current_buy(self):
        triad = central_v2()
        triad["recommendations"][0]["publish_eligible"] = False

        html = system_table_html({"rows": []}, {"rows": []}, {}, {}, triad=triad)

        self.assertIn("3A现买×0", html)
        self.assertNotIn("3A现买×1", html)

    def test_blocked_3a_is_never_executable_and_wins_duplicate_code(self):
        triad = central_v2()
        # 模拟上游异常重复：同一冻结代码又出现在现买桶且 publish=true。
        triad["recommendations"].append(
            central_row("G", "冻结股", "3A", "3A_PUBLISHABLE", publish=True))

        html = system_table_html({"rows": []}, {"rows": []}, {}, {}, triad=triad)

        self.assertIn("3A冻结×1", html)
        self.assertIn("冻结·不可执行", html)
        self.assertIn("3A现买×1", html)  # 只有 A；重复的 G 不得成为第二只现买
        self.assertNotIn("3A现买×2", html)
        self.assertEqual(html.count("冻结股"), 1)

    def test_current_buy_table_precedes_non_execution_research_group(self):
        rank = {"rows": [
            central_rank_row("A", "现买股"), central_rank_row("B", "准备股")],
            "archived": [], "coverage": {}, "verification": {}}

        html = system_table_html(rank, {"rows": []}, {}, {}, triad=central_v2())

        self.assertLess(html.index("现买股"), html.index("非执行研究组"))
        self.assertLess(html.index("非执行研究组"), html.index("准备股"))

    def test_central_trade_plan_overrides_legacy_fields(self):
        rank = {"rows": [central_rank_row("A", "现买股")], "archived": [],
                "coverage": {}, "verification": {}}

        html = system_table_html(rank, {"rows": []}, {}, {}, triad=central_v2())

        self.assertIn("现10.3", html)
        self.assertIn("10.20~10.60", html)
        self.assertIn("放量站上10.60", html)
        self.assertIn("收盘跌破9.70", html)
        self.assertNotIn("旧口径现买", html)
        self.assertNotIn("旧触发", html)

    def test_missing_v2_is_visible_fallback_but_never_current_buy(self):
        rank = {"rows": [central_rank_row("A", "旧候选")], "archived": [],
                "coverage": {}, "verification": {}}

        html = system_table_html(rank, {"rows": []}, {}, {}, triad={})

        self.assertIn("旧口径参考·不可执行", html)
        self.assertIn("不可冒充现买", html)
        self.assertIn("3A现买×0", html)

    def test_sell_header_only_counts_holdings_and_nonholding_is_folded(self):
        sell = {"rows": [
            {"code": "OWN", "name": "持仓卖警", "level": "-2A", "held": True,
             "on_board": True, "board_rank": 1, "sell_score": 80},
            {"code": "WATCH", "name": "非持仓回避", "level": "-3A", "held": False,
             "on_board": True, "board_rank": 2, "sell_score": 90},
        ], "archived": [], "holdings_quiet": {}}

        html = system_table_html({"rows": []}, sell, {}, {}, triad=central_v2())

        self.assertIn("OUT: 持仓卖警×1", html)
        self.assertNotIn("OUT: 持仓卖警×2", html)
        self.assertIn("<details", html)
        self.assertIn("非持仓回避（1只，默认折叠）", html)


if __name__ == "__main__":
    unittest.main()
