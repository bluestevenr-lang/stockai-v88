from datetime import date, datetime, timezone
from threading import Event
from time import monotonic
import json
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

import deep_optional_data as optional


def test_bounded_call_returns_without_waiting_for_worker_shutdown():
    started, release, finished = Event(), Event(), Event()

    def blocked():
        started.set()
        try:
            release.wait(5)
        finally:
            finished.set()

    before = monotonic()
    try:
        with pytest.raises(TimeoutError):
            optional.bounded_call(blocked, timeout=0.03)
        assert started.is_set()
        assert monotonic() - before < 0.5
        assert not finished.is_set()
    finally:
        release.set()
        assert finished.wait(1)


def test_bounded_call_preserves_result_and_does_not_retry_exception():
    payload = {"source": "original"}
    assert optional.bounded_call(lambda: payload) is payload
    calls = []
    failure = TypeError("provider failed")

    def fail():
        calls.append(1)
        raise failure

    with pytest.raises(TypeError) as caught:
        optional.bounded_call(fail)
    assert caught.value is failure
    assert calls == [1]


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_invalid_timeout_never_calls_provider(timeout):
    with pytest.raises(ValueError):
        optional.bounded_call(lambda: pytest.fail("provider called"), timeout=timeout)


def test_session_only_loads_explicitly_once_per_canonical_security():
    state, calls = {}, []
    payload = {"profile": {"ts": 123}, "extremes": None, "announcements": [], "news": [], "errors": []}

    def load():
        calls.append(1)
        return payload

    assert optional.session_once(state, "00700.HK", loader=load) == {}
    assert state == {} and calls == []
    before = datetime.now(timezone.utc)
    first = optional.session_once(state, "00700.HK", requested=True, loader=load)
    after = datetime.now(timezone.utc)
    assert before <= datetime.fromisoformat(first["attempted_at"]) <= after
    assert first["status"] == "complete" and first["code"] == "0700.HK"
    assert first["profile"]["ts"] == 123 and "attempted_at" not in payload
    assert optional.session_once(state, "0700.HK", requested=True, loader=load) is first
    assert optional.session_once(state, "00700", loader=load) is first
    assert calls == [1]
    assert optional.session_once(state, "3330.HK", loader=load) == {}
    second = optional.session_once(state, "3330.HK", requested=True, loader=load)
    assert second["code"] == "3330.HK" and calls == [1, 1]


@pytest.mark.parametrize("failure", [TimeoutError("provider deadline"), ValueError("invalid provider payload")])
def test_session_failure_has_original_attempt_clock_and_never_retries(failure):
    state, calls = {}, []

    def fail():
        calls.append(1)
        raise failure

    first = optional.session_once(state, "600519.SH", requested=True, loader=fail)
    assert first["status"] == "failed" and first["errors"] == [f"{type(failure).__name__}: {failure}"]
    assert first["code"] == "600519.SS" and first["attempted_at"]
    assert optional.session_once(state, "600519.SS", loader=fail) is first
    assert optional.session_once(state, "600519.SS", requested=True, loader=fail) is first
    assert calls == [1]


def test_partial_provider_errors_are_preserved_and_cached():
    state = {}
    first = optional.session_once(state, "GRPN", requested=True,
        loader=lambda: {"profile": {"source": "cache"}, "errors": ["news: timeout"]})
    assert first["status"] == "failed" and first["profile"] == {"source": "cache"}
    assert first["errors"] == ["news: timeout"]
    assert optional.session_once(state, "GRPN", requested=True,
        loader=lambda: pytest.fail("retried partial failure")) is first


def test_invalid_loader_payload_is_cached_as_failure():
    state = {}
    first = optional.session_once(state, "GRPN", requested=True, loader=lambda: [])
    assert first["status"] == "failed" and "dict" in first["errors"][0]
    assert optional.session_once(state, "GRPN", requested=True,
        loader=lambda: pytest.fail("retried invalid payload")) is first


def test_profile_cache_is_canonical_read_only_and_preserves_old_timestamp(tmp_path, monkeypatch):
    root = tmp_path / "cache"
    monkeypatch.setattr(optional, "_PROFILE_CACHE", root)
    assert optional.cached_profile("0700.HK") == {}
    assert not root.exists()
    root.mkdir()
    payload = {"profile": "original text", "source": "original source", "ts": 1}
    path = root / "0700.HK.json"
    path.write_text(json.dumps(payload))
    before = path.read_bytes(), path.stat().st_mtime_ns
    assert optional.cached_profile("00700.HK") == payload
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before
    assert optional.cached_profile("../0700.HK") == {}
    assert optional.cached_profile("3330.HK") == {}


@pytest.mark.parametrize("content", ["broken", "[]", '{"source":"x"}'])
def test_profile_invalid_cache_returns_empty(tmp_path, monkeypatch, content):
    monkeypatch.setattr(optional, "_PROFILE_CACHE", tmp_path)
    (tmp_path / "GRPN.json").write_text(content)
    assert optional.cached_profile("GRPN") == {}


def _bars():
    frame = pd.DataFrame({"open": [10.] * 5, "high": [11.] * 5, "low": [9.] * 5,
                          "close": [10.] * 5, "volume": [100.] * 5},
                         index=pd.bdate_range("2026-09-07", periods=5))
    frame.attrs.update(provider_source="sealed provider", source_asof="2026-09-11", price_basis="original basis")
    return frame


def _stub_benchmark(monkeypatch, read, validate):
    def no_fetch(*args, **kwargs):
        pytest.fail("local benchmark invoked a network fallback")
    monkeypatch.setitem(sys.modules, "verified_history", SimpleNamespace(read=read))
    monkeypatch.setitem(sys.modules, "market_data_helper", SimpleNamespace(
        _core=lambda: None, validate=validate, fetch_df=no_fetch, fetch_daily_free=no_fetch))


@pytest.mark.parametrize("code,read_code,calendar_code", [
    ("000001.SH", "000001.SS", "000001.SS"), ("^HSI", "^HSI", "BENCHMARK.HK"), ("^GSPC", "^GSPC", "^GSPC")])
def test_local_benchmark_keeps_source_identity_and_uses_correct_calendar(monkeypatch, code, read_code, calendar_code):
    calls = []

    def read(key, start, end):
        calls.append((key, start, end))
        return _bars()

    def validate(frame, key, *, source):
        assert key == calendar_code and source == "sealed provider"
        assert list(frame) == ["Open", "High", "Low", "Close", "Volume"]
        frame.attrs["is_realtime"] = False
        return frame

    _stub_benchmark(monkeypatch, read, validate)
    for _ in range(2):
        result = optional.read_local_benchmark(code)
        assert result.attrs["benchmark_code"] == read_code
        assert result.attrs["source_asof"] == "2026-09-11"
        assert result.attrs["price_basis"] == "original basis"
    assert len(calls) == 2  # No cache hides a changed local source.
    assert calls[0][0] == read_code
    assert (date.fromisoformat(calls[0][2]) - date.fromisoformat(calls[0][1])).days == 1100


@pytest.mark.parametrize("missing", [None, pd.DataFrame(), OSError("missing local DB")])
def test_missing_benchmark_has_no_fetch_fallback(monkeypatch, missing):
    def read(*args):
        if isinstance(missing, Exception):
            raise missing
        return missing
    _stub_benchmark(monkeypatch, read, lambda *args, **kwargs: pytest.fail("validated missing source"))
    assert optional.read_local_benchmark("^HSI") is None


def test_invalid_or_stale_benchmark_has_no_fetch_fallback(monkeypatch):
    def invalid(*args, **kwargs):
        raise ValueError("missing latest completed session")
    _stub_benchmark(monkeypatch, lambda *args: _bars(), invalid)
    assert optional.read_local_benchmark("^HSI") is None


@pytest.mark.parametrize("code,last_day,valid", [
    ("^HSI", "2026-09-10", True),
    ("^HSI", "2026-09-09", False),
    ("^GSPC", "2026-09-09", True),
    ("^HSI", "2026-09-11", False),
])
def test_real_validator_observes_hk_close_before_us_close(monkeypatch, code, last_day, valid):
    import market_data_helper
    from exchange_sessions import latest_completed

    class FixedClock(datetime):
        @classmethod
        def now(cls, tz=None):
            instant = datetime(2026, 9, 10, 9, tzinfo=timezone.utc)
            return instant.astimezone(tz) if tz else instant.replace(tzinfo=None)

    # Hong Kong is 17:00; New York is 05:00. Their completed dates differ.
    monkeypatch.setattr(optional, "datetime", FixedClock)
    monkeypatch.setattr(market_data_helper, "datetime", FixedClock)
    monkeypatch.setitem(sys.modules, "history_calendar", SimpleNamespace(latest_completed=latest_completed))
    frame = _bars()
    frame.index = pd.bdate_range(end=last_day, periods=5)
    monkeypatch.setitem(sys.modules, "verified_history", SimpleNamespace(read=lambda *args: frame))
    monkeypatch.setattr(market_data_helper, "fetch_df", lambda *args, **kwargs: pytest.fail("network fallback"))
    result = optional.read_local_benchmark(code)
    if valid:
        assert result is not None
        assert result.index[-1].date().isoformat() == last_day
        assert result.attrs["source_asof"] == last_day
        assert result.attrs["benchmark_code"] == code
        assert result.attrs["is_realtime"] is False
    else:
        assert result is None
