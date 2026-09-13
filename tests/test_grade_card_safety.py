import unittest
import pytest

from grade_card import board_html, system_table_html


@pytest.fixture(autouse=True)
def isolated_company_profiles(monkeypatch):
    # A/G are synthetic fixtures here, but also real US symbols. Live company
    # profiles must not replace these test labels or make safety tests depend
    # on the current downloaded catalog; name resolution has its own tests.
    import stock_profile_view
    monkeypatch.setattr(stock_profile_view, 'load', lambda *args, **kwargs: {})


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
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    row = {
        "code": code, "name": name, "tier": tier, "state": state,
        "publish_eligible": publish, "formal_recommendation": publish, "support_count": 2,
        "data_fresh": True, "factpack_id": "p"*64, "horizon": "short",
        "execution": {"triggered": publish, "status": "TRIGGERED" if publish else "PREPARE"},
        "source_timestamps": {"test": now}, "votes": {"gpt": "pass", "classics": "pass"},
        "reviews": {"gpt": {"model": "gpt-6-astra", "review_scope": "buy", "factpack_id": "p"*64, "horizon": "short",
                     "verdict": "通过", "execution_status": "现在买", "risk_veto": "无", "at": now},
                    "classics": {"horizon": "short", "factpack_id": "p"*64, "at": now, "source_timestamps": {"test": now}},
                    "claude_cs": {"verdict": "gate_pass"}},
        "trade_plan": {
            "ready": True, "last": 10.3, "entry_range": "10.2 ～ 10.6",
            "promotion_trigger": "收盘重新站上10.2000且当日量/此前20日均量≥1.2；下一开盘仍在原区间", "stop": 9.7,
            "target": 12.5, "rr": 2.4, "invalidation": "收盘跌破9.70",
            "horizon": "short", "position_cap": "不超过10%",
        },
    }

    from profit_fixtures import annual_inputs, short_inputs
    from profit_contract import evaluate
    horizon = "long" if tier == "3A" else "short"
    row["horizon"] = row["trade_plan"]["horizon"] = horizon
    row["reviews"]["gpt"]["horizon"] = row["reviews"]["classics"]["horizon"] = horizon
    row["trade_plan"]["entry_range"] = [10.2,10.6]
    row["trade_plan"]["profit_inputs"] = annual_inputs() if tier=="3A" else short_inputs(14,14.5) if tier=="2A" else short_inputs(12.4,12.5)
    pc = evaluate(row["trade_plan"],horizon)
    row["trade_plan"]["profit_contract"] = pc
    row["trade_plan"]["take_profit_range"] = pc["take_profit_range"]
    from review_scorecard import scorecard, CRITERIA, VERSION, BOOK_IDS
    gpt = {**row["reviews"]["gpt"], "evidence": [{"field": "last", "value": 10.3}, {"field": "market_evidence.ma20", "value": 10.1}],
           "criteria": [{"id": k, "score": 15, "reason": "已核对测试证据", "evidence_fields": ["last"]} for k in CRITERIA]}
    book = {**row["reviews"]["classics"], "rubric_version": VERSION,
            "checks": [{"id": k, "label": k, "ok": True, "detail": "通过测试证据", "book": "测试书理", "threshold": "测试标准", "evidence": {"test": 1}} for k in BOOK_IDS[horizon]]}
    if horizon=='short':
        next(c for c in book['checks'] if c['id']=='payoff')['evidence'] = {'target':12.5,'stop':9.7}
    else:
        next(c for c in book['checks'] if c['id']=='valuation')['evidence'] = {'fundamentals.pe_ttm':18}
    if tier in {'1A', '2A'}:
        next(c for c in book['checks'] if c['id']=='volume')['ok'] = False
    if tier == '1A':
        next(c for c in book['checks'] if c['id']=='entry')['ok'] = False
    from review_contract import SCHEMA_VERSION, PROMPT_HASH
    gpt.update(review_schema_version=SCHEMA_VERSION,prompt_hash=PROMPT_HASH)
    from copy import deepcopy
    gpt["review_pair"] = {key: deepcopy(gpt) for key in ("primary","counteraudit")}
    row["reviews"]["gpt"] = gpt
    row["reviews"]["classics"] = book
    row["scorecard"] = scorecard(gpt, book, gpt_current=True, book_current=True)
    row["audit_score"] = row["scorecard"]["total"]
    return row


def central_v2() -> dict:
    return {
        "generated_at": __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M"), "factpack_fresh": True,
        "version": "gpt-classics-selection-v9-tharp", "factpack_id": "p" * 64,
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
        self.assertIn("原研究区间与触发", html)
        self.assertNotIn("IN: 3A现买×1", html)


class GradeCardTriadV2Test(unittest.TestCase):
    def test_review_progress_above_list_and_full_explanation_retained(self):
        triad = central_v2()
        triad['pending'][0]['reviews']['gpt'] = {}
        html = system_table_html({'rows': []}, {'rows': []}, {}, {}, triad=triad)
        self.assertIn('全量推演尚未完成', html)
        self.assertIn('当前完整GPT双审', html)
        self.assertIn('空榜不能解释为全市场没有机会', html)
        self.assertLess(html.index('当前完整GPT双审'), html.index('价值分层与条件跟踪'))
        self.assertGreater(html.index('全量推演尚未完成'), html.index('价值分层与条件跟踪'))

    def test_v1_with_new_bucket_names_does_not_masquerade_as_v2(self):
        triad = central_v2()
        triad["version"] = "gpt-triad-selection-v1"
        rank = {"rows": [central_rank_row("A", "旧候选")], "archived": [],
                "coverage": {}, "verification": {}}

        html = system_table_html(rank, {"rows": []}, {}, {}, triad=triad)

        self.assertIn("GPT-6与经典巨著审核尚未就绪", html)
        self.assertIn("旧口径参考·不可执行", html)
        self.assertIn("3A现买×0", html)

    def test_old_v2_schema_cannot_reuse_retired_reviews(self):
        triad = central_v2()
        triad["version"] = "custom-producer"
        triad["schema_version"] = 2

        html = system_table_html({"rows": []}, {"rows": []}, {}, {}, triad=triad)

        self.assertNotIn("3A现买×1", html)
        self.assertIn("GPT-6与经典巨著审核尚未就绪", html)

    def test_displays_all_states_but_only_recommendation_executes(self):
        triad = central_v2()
        rank = {"rows": [central_rank_row(code, name) for code, name in (
            ("A", "现买股"), ("B", "准备股"), ("C", "条件股"),
            ("D", "分歧股"), ("E", "研究股"), ("F", "待审股"),
        )], "archived": [], "coverage": {}, "verification": {}}

        html = system_table_html(rank, {"rows": []}, {}, {}, triad=triad)

        for text in ("3A现买×1", "3A冻结×1", "3A准备×1", "2A条件×1",
                     "2A价值×1", "1A价值×1", "数据处理与审核队列（1只"):
            self.assertIn(text, html)
        for label in ("3A现买", "3A冻结", "冻结·不可执行", "3A准备", "2A条件",
                      "2A·价值机会", "1A价值跟踪", "数据处理与审核队列"):
            self.assertIn(label, html)
        self.assertEqual(html.count("现在可进"), 1)
        self.assertIn("原研究区间与触发", html)

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

        self.assertLess(html.index("现买股"), html.index("价值分层与条件跟踪"))
        self.assertLess(html.index("价值分层与条件跟踪"), html.index("准备股"))

    def test_central_trade_plan_overrides_legacy_fields(self):
        rank = {"rows": [central_rank_row("A", "现买股")], "archived": [],
                "coverage": {}, "verification": {}}

        html = system_table_html(rank, {"rows": []}, {}, {}, triad=central_v2())

        self.assertIn("现10.3", html)
        self.assertIn("10.2 ～ 10.6", html)
        self.assertIn("收盘重新站上10.2000", html)
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


def test_expired_preparation_loses_three_a_label_and_current_score():
    triad = central_v2()
    triad['recommendations'] = []
    triad['blocked_3a'] = []
    row = triad['preparations'][0]
    row['reviews']['gpt']['at'] = '2000-01-01 00:00'
    html = system_table_html({'rows': []}, {'rows': []}, {}, {}, triad=triad)
    assert '3A准备×0' in html
    assert '数据处理与审核队列' in html
    assert '准备股' not in html
    assert '数据处理与审核队列' in html


def test_table_replaces_none_and_old_probability_with_auditable_scores():
    triad = central_v2()
    rank = {'rows': [central_rank_row('A', '现买股')]}
    html = system_table_html(rank, {'rows': []}, {'A': {'p_up': 99}}, {}, triad=triad)
    assert '审核分 75/100' in html
    assert 'GPT‑6逐项评分' in html and '书籍逐项核验' in html
    assert '分None' not in html and '#None' not in html and '风险None' not in html
    assert '2周涨99%' not in html and '分88' not in html
