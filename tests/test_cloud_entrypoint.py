"""Exercise the full cloud page without private modules or live HTTP."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests
import streamlit as st
from streamlit.testing.v1 import AppTest


@pytest.mark.parametrize("has_strategy", [False, True])
@pytest.mark.parametrize("temperature", [{}, {"temp": 45, "label": "中性"}])
def test_cloud_page_starts_with_or_without_current_strategy(monkeypatch, has_strategy, temperature):
    snapshot = {"generated_at": "2026-11-16T03:00:00+00:00",
                "markets": {"A股": {"temperature": temperature}}}
    runtime = {
        "version": "autonomous-public-v1",
        "generated_at": "2026-11-16T04:00:00+00:00",
        "snapshot": snapshot, "watch": {}, "journal": {}, "astra": {},
        "strategy": {"version": "strategy-taxonomy-2026-09-28-v1", "rows": []} if has_strategy else {},
    }

    def get(url, *args, **kwargs):
        value = runtime if "autonomous_runtime.json" in url else snapshot if "market_snapshot.json" in url else {}
        return SimpleNamespace(status_code=200, text=json.dumps(value), json=lambda: value)

    monkeypatch.setattr(requests, "get", get)
    monkeypatch.setenv("V88_DISABLE_LLM", "1")
    monkeypatch.setenv("V88_FEISHU_ALLOW_PUSH", "0")
    st.cache_data.clear()
    app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "streamlit_app.py"),
                            default_timeout=30).run()
    assert not app.exception, [str(error.value) for error in app.exception]
    assert any("V88" in title.value for title in app.title)
    if temperature:
        app.radio(key="_nav").set_value("📈 大盘板块").run()
        assert not app.exception, [str(error.value) for error in app.exception]
