import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import v88_ai_budget as budget


class WebBudgetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_ledger, self.old_base, self.old_cap = budget.LEDGER, budget.BASE_CAP, budget.CAP
        budget.LEDGER = Path(self.tmp.name) / "web_ai_budget.json"
        budget.BASE_CAP, budget.CAP = 0.0, 0.0
        budget._TRUTH_CACHE.update(ts=10**20, data={})

    def tearDown(self):
        budget.LEDGER, budget.BASE_CAP, budget.CAP = self.old_ledger, self.old_base, self.old_cap
        budget._TRUTH_CACHE.update(ts=0.0, data={})
        self.tmp.cleanup()

    def test_subscription_calls_are_not_cash_capped(self):
        first = budget.reserve("材料", output_tokens=1000)
        budget.settle(first, {"prompt_tokens": 12, "completion_tokens": 34})
        self.assertIsNotNone(budget.reserve(
            "材料", output_tokens=1000, priority=True, scope="stock-cycle-thinking"))
        status = budget.status()
        self.assertEqual("kimi-code-subscription", status["billing_mode"])
        self.assertEqual("k3-256k", status["model"])
        self.assertEqual(0.0, status["cash_rmb"])
        self.assertEqual((12, 34), (status["prompt_tokens"], status["completion_tokens"]))


if __name__ == "__main__":
    unittest.main()
