import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("sync_v88_projection_win.py")
SPEC = importlib.util.spec_from_file_location("v88_projection", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


class ProjectionTests(unittest.TestCase):
    def make_source(self, root):
        source = root / "report" / "data"
        write_json(
            source / "gpt_verify.json",
            {
                "generated_at": "2026-08-23 20:00（北京时间）",
                "rows": {
                    "AAPL": {
                        "name": "苹果",
                        "verdict": "通过",
                        "why": "测试事实包",
                        "ts": "2026-08-23 20:00（北京时间）",
                    }
                },
            },
        )
        write_json(
            source / "kimi_verify.json",
            {
                "generated_at": "2026-08-23 20:01（北京时间）",
                "rows": {
                    "AAPL": {
                        "verdict": "通过",
                        "book_verdict": "通过",
                        "why": "测试复核",
                        "ts": "2026-08-23 20:01（北京时间）",
                    }
                },
            },
        )
        write_json(
            source / "three_way_pub.json",
            {
                "generated_at": "2026-08-23 20:02（北京时间）",
                "rows": {"AAPL": {"code": "AAPL", "name": "苹果", "tier": "3A"}},
            },
        )
        write_json(
            source / "why_buy_pub.json",
            {
                "generated_at": "2026-08-23 19:58（北京时间）",
                "sells": {
                    "AAPL": {
                        "name": "苹果",
                        "kind": "贴近止损",
                        "why_now": "距止损2%",
                        "hold": "未破线先持有",
                        "fail": "跌破100失效",
                        "cycle": {
                            "code": "AAPL",
                            "stance": "持有",
                            "why": "趋势未破",
                            "on_break": "跌破减仓",
                        },
                    }
                },
            },
        )
        write_json(
            source / "ai_cert_pub.json",
            {
                "asof": "2026-08-23 20:03（北京时间）",
                "by_code": {
                    "AAPL": {
                        "code": "AAPL",
                        "name": "苹果",
                        "verdict": "通过",
                    }
                },
            },
        )
        write_json(
            source / "exit_ledger.json",
            {
                "days": {
                    "2026-08-23": {
                        "ruleset": "test",
                        "lines": {
                            "AAPL": {
                                "code": "AAPL",
                                "name": "苹果",
                                "last": 105,
                                "take": 120,
                                "stop": 100,
                            }
                        },
                    }
                }
            },
        )
        write_json(
            source.parent / "positions.json",
            {
                "updated_at": "2026-08-23 20:04（北京时间）",
                "accounts": {
                    "secret-broker": {
                        "type": "secret-type",
                        "holdings": [
                            {
                                "code": "AAPL",
                                "name": "苹果",
                                "qty": 999,
                                "cost": 88,
                                "pnl_pct": 4.2,
                            }
                        ],
                    }
                },
            },
        )
        return source

    def test_builds_holding_first_decision_bundle_without_account_data(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = self.make_source(root)
            destination = root / "workspace" / "context"
            self.assertEqual(MODULE.build(source, destination), 1)

            stock = json.loads((destination / "stocks" / "AAPL.json").read_text())
            decision = stock["decision_snapshot"]
            self.assertTrue(decision["is_held"])
            self.assertEqual(decision["position_mode"], "持仓管理")
            self.assertEqual(decision["portfolio_fact"]["pnl_pct"], 4.2)
            self.assertEqual(decision["action_contract"]["guidance"], "未破线先持有")
            self.assertEqual(decision["risk_line"]["stop"], 100)
            self.assertIn("why_buy_pub", decision["module_evidence"])

            portfolio_text = (destination / "modules" / "portfolio_pub.json").read_text()
            self.assertNotIn("secret-broker", portfolio_text)
            self.assertNotIn("secret-type", portfolio_text)
            self.assertNotIn('"qty"', portfolio_text)
            self.assertNotIn('"cost"', portfolio_text)
            self.assertNotIn('"books"', portfolio_text)

    def test_missing_required_source_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "report" / "data"
            write_json(source / "gpt_verify.json", {"rows": {}})
            with self.assertRaises(FileNotFoundError):
                MODULE.build(source, root / "out")


if __name__ == "__main__":
    unittest.main()
