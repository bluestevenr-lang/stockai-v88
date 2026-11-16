"""Cloud decisions use the published context without reading private files."""
import v88_decision_core as core


def test_published_market_context_controls_environment_without_local_files(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(core, "BASE", tmp_path)
    context = {"snapshot": {"markets": {"港股": {
        "l3": {"probs": [["2周", 60]]}, "temperature": {"verdict": "偏暖"}}}}}
    assert core.market_regime("港股", context=context)["regime"] == "bull"
    context["snapshot"]["markets"]["港股"]["l3"]["probs"] = [["2周", 35]]
    assert core.env_gate("700.HK", "左侧低吸", 2, context=context)["regime"] == "bear"
    assert not caplog.records


def test_explicit_empty_context_does_not_fall_back_to_private_snapshot(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(core, "BASE", tmp_path)
    (tmp_path / "data").mkdir()
    (tmp_path / "data/market_snapshot.json").write_text("private invalid payload")
    assert core.market_regime("港股", context={})["regime"] == "neutral"
    assert not caplog.records


def test_published_top_risk_is_not_overridden_by_warm_probability(tmp_path, monkeypatch):
    monkeypatch.setattr(core, "BASE", tmp_path)
    context = {"snapshot": {"markets": {"港股": {"l3": {"probs": [["2周", 60]]}}}},
               "regime_topbot": {"markets": {"港股": {"turn_risk": {"top_risk": 60}}}}}
    result = core.market_regime("港股", context=context)
    assert result["regime"] == "neutral" and result["topish"] is True


def test_search_decision_passes_published_context_to_entry_plan(tmp_path, monkeypatch):
    from test_v88_decision_core import _prices, _et_full
    monkeypatch.setattr(core, "BASE", tmp_path)
    df = _prices(1)
    full = _et_full(last=float(df.Close.iloc[-1]), total=70, buy_zone="130~135")
    context = {"snapshot": {"markets": {"港股": {"l3": {"probs": [["2周", 35]]}}}}}
    result = core.evaluate_decision(df, full, name="测试", code="700.HK", market_context=context)
    assert result["entry_plan"]["regime"] == "bear"
    assert result["entry_plan"]["mode"] != "左侧低吸"
