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
            source / "rotation_forecast.json",
            {"generated_at": "2026-08-23 19:57（北京时间）", "markets": {"US": {"top": ["technology"]}}},
        )
        write_json(
            source / "review_factpack.json",
            {"selection_policy": {"shortlist_max": 40}, "coverage": {"market_pool_rows": 2342, "shortlist_rows": 40}},
        )
        write_json(
            source / "dual_cli_status.json",
            {"version": "gpt-led-review-funnel-v2", "generated_at": "2026-08-23 20:05（北京时间）", "funnel": {"k3_shortlist_rows": 20}},
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

            stock = json.loads(
                (destination / "stocks" / "AAPL.json").read_text(encoding="utf-8")
            )
            decision = stock["decision_snapshot"]
            self.assertTrue(decision["is_held"])
            self.assertEqual(decision["position_mode"], "持仓管理")
            self.assertEqual(decision["portfolio_fact"]["pnl_pct"], 4.2)
            self.assertEqual(decision["action_contract"]["guidance"], "未破线先持有")
            self.assertEqual(decision["risk_line"]["stop"], 100)
            self.assertIn("why_buy_pub", decision["module_evidence"])

            overview = json.loads(
                (destination / "overview.json").read_text(encoding="utf-8")
            )
            self.assertEqual(overview["review_funnel"]["coverage"]["market_pool_rows"], 2342)
            self.assertEqual(overview["review_funnel"]["funnel"]["k3_shortlist_rows"], 20)
            self.assertTrue((destination / "modules" / "rotation_forecast.json").is_file())

            portfolio_text = (
                destination / "modules" / "portfolio_pub.json"
            ).read_text(encoding="utf-8")
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

    def test_writes_five_digit_hk_alias_for_unpadded_source_code(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = self.make_source(root)
            for filename in (
                "gpt_verify.json",
                "kimi_verify.json",
                "three_way_pub.json",
            ):
                document = json.loads((source / filename).read_text(encoding="utf-8"))
                document["rows"]["2382.HK"] = {
                    "code": "2382.HK",
                    "name": "舜宇光学科技",
                    "verdict": "通过",
                }
                write_json(source / filename, document)

            destination = root / "workspace" / "context"
            MODULE.build(source, destination)

            canonical = destination / "stocks" / "2382.HK.json"
            padded = destination / "stocks" / "02382.HK.json"
            self.assertTrue(canonical.is_file())
            self.assertTrue(padded.is_file())
            self.assertEqual(
                json.loads(canonical.read_text(encoding="utf-8")),
                json.loads(padded.read_text(encoding="utf-8")),
            )

    def test_cloud_sanitized_portfolio_wins_over_stale_plaintext(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = self.make_source(root)
            write_json(
                source / "portfolio_pub.json",
                {
                    "source_updated_at": "2026-08-25 20:30（北京时间）",
                    "items": [
                        {"code": "MSFT", "name": "微软", "pnl_pct": 8.8},
                    ],
                },
            )
            destination = root / "workspace" / "context"
            MODULE.build(source, destination)

            apple = json.loads(
                (destination / "stocks" / "AAPL.json").read_text(encoding="utf-8")
            )
            microsoft = json.loads(
                (destination / "stocks" / "MSFT.json").read_text(encoding="utf-8")
            )
            portfolio = json.loads(
                (destination / "modules" / "portfolio_pub.json").read_text(encoding="utf-8")
            )
            self.assertFalse(apple["decision_snapshot"]["is_held"])
            self.assertTrue(microsoft["decision_snapshot"]["is_held"])
            self.assertIn("GitHub云端解密后脱敏", portfolio["source"])
            self.assertEqual(portfolio["updated_at"], "2026-08-25 20:30（北京时间）")


if __name__ == "__main__":
    unittest.main()
