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
        budget.BASE_CAP, budget.CAP = 0.003, 0.006
        budget._TRUTH_CACHE.update(ts=10**20, data={})

    def tearDown(self):
        budget.LEDGER, budget.BASE_CAP, budget.CAP = self.old_ledger, self.old_base, self.old_cap
        budget._TRUTH_CACHE.update(ts=0.0, data={})
        self.tmp.cleanup()

    def test_extra_budget_requires_priority(self):
        first = budget.reserve("材料", output_tokens=1000)
        budget.settle(first)
        self.assertIsNone(budget.reserve("材料", output_tokens=1000))
        self.assertIsNotNone(budget.reserve(
            "材料", output_tokens=1000, priority=True, scope="stock-cycle-thinking"))


if __name__ == "__main__":
    unittest.main()
