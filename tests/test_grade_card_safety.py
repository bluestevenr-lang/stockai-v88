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
        self.assertIn("IN可执行: 3A×0", html)
        self.assertIn("3A候选", html)
        self.assertIn("待复核·不可执行", html)
        self.assertIn("研究参考·非买单", html)
        self.assertNotIn("IN可执行: 3A×1", html)


if __name__ == "__main__":
    unittest.main()
