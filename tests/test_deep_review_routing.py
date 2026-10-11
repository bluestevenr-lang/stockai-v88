"""Screening pages never launch historical model reviews implicitly."""
import sys
from contextlib import nullcontext
from types import SimpleNamespace
import pytest
import deep_review_job as jobs

@pytest.mark.parametrize('prior', [{}, {'status': 'failed', 'error': '末根日线未覆盖最近完整交易日'}, {'status': 'complete'}])
def test_open_and_refresh_keep_current_report_without_model_call(monkeypatch, prior):
    monkeypatch.setitem(sys.modules, 'deep_cross_validation', SimpleNamespace(load_context=lambda code: {}))
    monkeypatch.setitem(sys.modules, 'deep_analysis_data', SimpleNamespace(report_html=lambda code, context: '当前分析 88.2 · 27.77 CNY · 2026-10-09'))
    monkeypatch.setattr(jobs, 'read', lambda code: prior)
    monkeypatch.setattr(jobs, 'request', lambda *a, **kw: pytest.fail('page open invoked model review'))
    monkeypatch.setattr(jobs, '_render_legacy', lambda *a: pytest.fail('implicit legacy rendering'))
    output = []
    st = SimpleNamespace(session_state={}, markdown=lambda text, **kw: output.append(text), caption=output.append,
                         expander=lambda *a, **kw: nullcontext(), button=lambda *a, **kw: False)
    jobs.render(st, '002648.SZ');jobs.render(st, '002648.SZ')
    assert output.count('当前分析 88.2 · 27.77 CNY · 2026-10-09') == 2
    if prior.get('error'):
        assert any('上次旧制复审未完成' in s and prior['error'] in s for s in output)

def test_explicit_legacy_review_is_still_available(monkeypatch):
    monkeypatch.setitem(sys.modules, 'deep_cross_validation', SimpleNamespace(load_context=lambda code: {}))
    monkeypatch.setitem(sys.modules, 'deep_analysis_data', SimpleNamespace(report_html=lambda *a, **kw: 'current'))
    monkeypatch.setattr(jobs, 'read', lambda code: {})
    calls = []
    monkeypatch.setattr(jobs, 'request', lambda code, ctx, retry: calls.append((code, retry)) or {'status':'queued'})
    monkeypatch.setattr(jobs, '_render_legacy', lambda st, code: calls.append(code))
    st = SimpleNamespace(session_state={}, markdown=lambda *a, **kw: None, caption=lambda *a: None,
                         expander=lambda *a, **kw: nullcontext(), button=lambda *a, **kw: True)
    jobs.render(st, '002648.SZ')
    assert calls == [('002648.SZ', True), '002648.SZ']
