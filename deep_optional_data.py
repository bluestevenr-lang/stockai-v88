"""Explicit optional research reads; importing this module performs no I/O.

Optional providers may keep running after a deadline, so their own network
timeouts remain necessary.  A daemon worker lets the page return promptly; it
does not retry or claim that the provider was cancelled.
"""
from datetime import datetime, timedelta, timezone
import math
from pathlib import Path
from threading import Event, Thread


_SESSION_KEY = "_v88_deep_optional_data"
_PROFILE_CACHE = Path(__file__).resolve().parent / ".cache_profile"


def _canonical(code):
    from modules.utils import to_yf_cn_code
    return to_yf_cn_code(str(code or ""))


def bounded_call(fn, *, timeout=5):
    """Call ``fn()`` once, returning or raising within the wait deadline.

    The worker is a daemon, without an executor context manager or blocking
    shutdown.  Provider exceptions are propagated unchanged, never retried.
    """
    timeout = float(timeout)
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and positive")
    done = Event()
    outcome = []

    def run():
        try:
            outcome.append((True, fn()))
        except BaseException as exc:
            outcome.append((False, exc))
        finally:
            done.set()

    Thread(target=run, name="v88-deep-optional", daemon=True).start()
    if not done.wait(timeout):
        raise TimeoutError(f"optional data exceeded {timeout:g} seconds")
    succeeded, value = outcome[0]
    if succeeded:
        return value
    raise value


def session_once(state, code, *, requested=False, loader=None):
    """Return a security's explicit attempt, without automatic rerun retries.

    ``loader`` is a zero-argument callable returning a flat payload.  Its own
    provider calls should use ``bounded_call``.  The original source clocks in
    that payload remain separate from the actual UTC attempt timestamp here.
    """
    key = _canonical(code)
    attempts = state.get(_SESSION_KEY) or {}
    if key in attempts:
        return attempts[key]
    if not requested or not key:
        return {}
    attempted_at = datetime.now(timezone.utc).isoformat()
    metadata = {"code": key, "attempted_at": attempted_at}
    # Record before invoking the loader: even a nested call or interrupted
    # rerun cannot initiate a second attempt for this security.
    result = dict(metadata, status="failed", errors=["Optional data attempt did not complete."])
    attempts[key] = result
    state[_SESSION_KEY] = attempts
    try:
        if loader is None:
            raise TypeError("an optional data loader is required")
        payload = loader()
        if not isinstance(payload, dict):
            raise TypeError("optional data loader must return a dict")
        result = dict(payload)
        errors = result.get("errors") or []
        result["errors"] = list(errors) if isinstance(errors, (list, tuple)) else [str(errors)]
        result.update(metadata, status="failed" if result["errors"] else "complete")
    except Exception as exc:
        result = dict(metadata, status="failed", errors=[f"{type(exc).__name__}: {exc}"])
    attempts[key] = result
    state[_SESSION_KEY] = attempts
    return result


def cached_profile(code):
    """Read the canonical security's existing cache, preserving source clocks.

    No directory creation, provider import, network fallback or freshness claim.
    """
    import json
    import re
    key = _canonical(code)
    if not re.fullmatch(r"[A-Z0-9^][A-Z0-9.^-]*", key):
        return {}
    try:
        payload = json.loads((_PROFILE_CACHE / f"{key}.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(payload, dict) or not payload.get("profile"):
        return {}
    return {field: payload[field] for field in ("profile", "source", "ts") if field in payload}


def read_local_benchmark(code):
    """Return current completed local benchmark bars, or ``None``.

    This has no data cache: each call reads the current verified local source.
    It never invokes any market-data fetch/fallback function.
    """
    try:
        from zoneinfo import ZoneInfo
        from market_data_helper import _core, validate
        _core()
        from verified_history import read
        key = _canonical(code)
        if not key:
            return None
        eastern = key.endswith((".SS", ".SZ", ".BJ", ".HK")) or key == "^HSI"
        now = datetime.now(ZoneInfo("Asia/Shanghai" if eastern else "America/New_York"))
        frame = read(key, (now.date() - timedelta(days=1100)).isoformat(), now.date().isoformat())
        if frame is None or frame.empty:
            return None
        frame = frame.rename(columns={column: str(column).title() for column in frame.columns})
        source = frame.attrs.get("provider_source") or frame.attrs.get("source") or "同源核验日线"
        # validate uses this key only to select a market's calendar. The actual
        # source was read using ^HSI above; no security/data identity is replaced.
        calendar_code = "BENCHMARK.HK" if key == "^HSI" else key
        result = validate(frame, calendar_code, source=source)
        result.attrs["benchmark_code"] = key
        return result
    except Exception:
        return None
