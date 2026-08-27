import importlib.util
from pathlib import Path

import pytest


MODULE_PATH = (Path(__file__).resolve().parents[1] / "win" / "openclaw-v88" /
               "sync_v88_projection_win.py")
V88CTL_PATH = Path(__file__).resolve().parents[1] / "win" / "v88ctl.ps1"
SPEC = importlib.util.spec_from_file_location("sync_v88_projection_win", MODULE_PATH)
projection = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(projection)


def test_central_v2_buckets_are_indexed_with_state():
    indexed = projection.triad_rows_map({
        "recommendations": [{"code": "AAPL", "tier": "3A"}],
        "preparations": [{"code": "MSFT", "tier": "3A"}],
        "blocked_3a": [{"code": "NVDA", "tier": "3A"}],
        "conditional": [{"code": "0700.HK", "tier": "2A"}],
        "observations": [{"code": "600000.SS", "tier": "1A"}],
        "pending": [{"code": "TSLA", "tier": "PENDING"}],
    })

    assert indexed["AAPL"]["central_bucket"] == "recommendations"
    assert indexed["MSFT"]["central_bucket"] == "preparations"
    assert indexed["NVDA"]["central_bucket"] == "blocked_3a"
    assert indexed["0700.HK"]["central_bucket"] == "conditional"
    assert indexed["600000.SS"]["central_bucket"] == "observations"
    assert indexed["TSLA"]["central_bucket"] == "pending"


def test_projection_allows_new_review_axes():
    record = {
        "verdict": "通过",
        "thesis_verdict": "通过",
        "execution_status": "等触发",
        "risk_veto": "无",
        "horizon": "medium",
        "why": "test",
    }

    projected = projection.pick(record, projection.GPT_FIELDS)

    assert projected["thesis_verdict"] == "通过"
    assert projected["execution_status"] == "等触发"
    assert projected["risk_veto"] == "无"
    assert projected["horizon"] == "medium"


def test_central_projection_is_required(tmp_path):
    with pytest.raises(FileNotFoundError):
        projection.load_central_decision(tmp_path)

    (tmp_path / "triad_selection_pub.json").write_text(
        "{broken", encoding="utf-8")
    (tmp_path / "three_way_pub.json").write_text(
        "{}", encoding="utf-8")
    with pytest.raises(FileNotFoundError):
        projection.load_central_decision(tmp_path)


def test_valid_empty_v2_central_projection_is_accepted(tmp_path):
    expected = {"version": "gpt-triad-selection-v2", "factpack_id": "pack-1",
                "recommendations": [], "pending": []}
    (tmp_path / "triad_selection_pub.json").write_text(
        __import__("json").dumps(expected), encoding="utf-8")
    assert projection.load_central_decision(tmp_path) == expected


def test_win_wrapper_clears_private_memory_and_verifies_real_promotion():
    text = V88CTL_PATH.read_text(encoding="utf-8-sig")
    assert "Remove-Item -LiteralPath $GptKnowledge -Recurse -Force" in text
    assert "Copy-Item -Destination $GptKnowledge" not in text
    for proof in ("$status.ok", "$status.promoted", "$status.kimi_official_promoted",
                  "$status.gpt_reviewed", "$status.k3_reviewed",
                  "$selection.factpack_id", "$status.factpack_id"):
        assert proof in text


def test_win_wrapper_removes_generic_payg_overrides():
    text = V88CTL_PATH.read_text(encoding="utf-8-sig")
    for marker in ("API_KEY", "ACCESS_TOKEN", "AUTH_TOKEN", "BASE_URL",
                   "API_BASE", "ENDPOINT", "V88_DISABLE_LLM", "GITHUB_ACTIONS"):
        assert marker in text
