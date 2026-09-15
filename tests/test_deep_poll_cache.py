"""Version-bound reads and polling never renew evidence or start model work."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path

import pytest

import deep_cross_validation as cross
import deep_review_job as jobs


@pytest.fixture(autouse=True)
def empty_cache():
    cross._SNAPSHOTS.clear()
    yield
    cross._SNAPSHOTS.clear()


def write(path, value):
    path.write_text(json.dumps(value), encoding='utf-8')


def test_same_file_read_once_and_atomic_same_mtime_size_replacement_invalidates(tmp_path, monkeypatch):
    path = tmp_path/'snapshot.json'
    write(path, {'value': 1})
    calls = []
    original = Path.read_text
    def read(self, *args, **kwargs):
        calls.append(self)
        return original(self, *args, **kwargs)
    monkeypatch.setattr(Path, 'read_text', read)
    assert cross.read_json_snapshot(path) == cross.read_json_snapshot(path) == {'value': 1}
    assert len(calls) == 1
    stat = path.stat()
    replacement = tmp_path/'replacement.json'
    write(replacement, {'value': 2})
    os.utime(replacement, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    os.replace(replacement, path)
    assert path.stat().st_size == stat.st_size
    assert cross.read_json_snapshot(path) == {'value': 2}
    assert len(calls) == 2


def test_deleted_or_broken_file_never_returns_last_good_snapshot(tmp_path):
    path = tmp_path/'snapshot.json'
    write(path, {'value': 1})
    cross.read_json_snapshot(path)
    path.unlink()
    with pytest.raises(OSError): cross.read_json_snapshot(path)
    path.write_text('{broken')
    with pytest.raises(ValueError): cross.read_json_snapshot(path)
    assert path.resolve() not in cross._SNAPSHOTS
    write(path, {'value': 3})
    assert cross.read_json_snapshot(path) == {'value': 3}


def test_version_change_during_read_retries_before_caching(tmp_path, monkeypatch):
    path = tmp_path/'snapshot.json'
    write(path, {'value': 1})
    original = Path.read_text
    calls = []
    def read(self, *args, **kwargs):
        raw = original(self, *args, **kwargs)
        calls.append(self)
        if len(calls) == 1:
            write(path, {'value': 2})
        return raw
    monkeypatch.setattr(Path, 'read_text', read)
    assert cross.read_json_snapshot(path) == {'value': 2}
    assert cross.read_json_snapshot(path) == {'value': 2}
    assert len(calls) == 2


@pytest.fixture
def context_files(tmp_path, monkeypatch):
    from market_data_helper import _core
    _core()
    import recommendation_gate as gate
    monkeypatch.setattr(gate, 'current_publishable', lambda selection: [])
    row = {'code': '000333.SZ', 'tier': 'PENDING', 'horizon': 'short',
           'factpack_id': 'pack', 'nested': {'original': 1}}
    selection = {'version': 'gpt-classics-selection-v9-tharp', 'factpack_id': 'pack',
                 'factpack_fresh': True, 'generated_at': '2026-09-15T12:00:00+08:00',
                 'pending': [row, {**deepcopy(row), 'code': '000935.SZ'}]}
    write(tmp_path/'triad_selection.json', selection)
    write(tmp_path/'review_factpack.json', {'factpack_id': 'pack', 'items': [deepcopy(row)]})
    return tmp_path, selection


def test_context_uses_actual_requested_directory_and_detaches_single_stock(context_files, monkeypatch):
    root, _ = context_files
    original = Path.read_text
    calls = []
    def read(self, *args, **kwargs):
        if self.parent == root: calls.append(self.name)
        return original(self, *args, **kwargs)
    monkeypatch.setattr(Path, 'read_text', read)
    first = cross.load_context('000333.SZ', root)
    first['row']['nested']['original'] = 99
    first['fact']['nested']['original'] = 99
    first['selection']['pending'].append({'code': 'MUTATED'})
    first['review_progress'] = {'percent': 100}
    second = cross.load_context('000333.SZ', root)
    assert second['row']['nested']['original'] == second['fact']['nested']['original'] == 1
    assert len(second['selection']['pending']) == 1
    assert 'review_progress' not in second
    assert calls == ['triad_selection.json', 'review_factpack.json']


def test_file_unchanged_still_rechecks_clock_and_central_validity(context_files, monkeypatch):
    import recommendation_gate as gate
    import review_display as display
    root, selection = context_files
    now = [datetime(2026, 9, 15, 13, tzinfo=timezone(timedelta(hours=8)))]
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return now[0].astimezone(tz) if tz else now[0]
    monkeypatch.setattr(display, 'datetime', Clock)
    monkeypatch.setattr(display, 'known_protocol', lambda *a: True)
    monkeypatch.setattr(display, 'source_times_fresh', lambda *a, **kw: True)
    monkeypatch.setattr(display, 'scorecard', lambda g,b,**kw: {
        'gpt': {'current': kw['gpt_current']}, 'books': {'current': kw['book_current']}})
    calls = []
    def publishable(doc):
        calls.append(now[0])
        return doc['pending'] if display.fresh(doc.get('generated_at'), 24) else []
    monkeypatch.setattr(gate, 'current_publishable', publishable)
    row = selection['pending'][0]
    row['reviews'] = {'gpt': {'model': display.MODEL, 'factpack_id': 'pack',
        'review_scope': 'buy', 'horizon': 'short', 'at': selection['generated_at']},
        'classics': {'factpack_id': 'pack', 'horizon': 'short', 'at': selection['generated_at'],
                     'source_timestamps': {'fixture': 'checked'}}}
    write(root/'triad_selection.json', selection)
    first = cross.load_context('000333.SZ', root)
    assert first['formal'] and first['card']['gpt']['current']
    now[0] += timedelta(days=2)
    second = cross.load_context('000333.SZ', root)
    assert not second['formal'] and not second['card']['gpt']['current']
    assert len(calls) == 2


def test_mixed_publication_pair_cannot_grant_current_card(context_files, monkeypatch):
    import review_display as display
    root, _ = context_files
    write(root/'review_factpack.json', {'factpack_id': 'other', 'items': []})
    monkeypatch.setattr(display, 'current_scorecard', lambda selection,row: {'current': bool(selection)})
    result = cross.load_context('000333.SZ', root)
    assert result['loaded_factpack_id'] == 'other'
    assert not result['formal'] and not result['card']['current']


def test_supplied_selection_avoids_second_read_without_mutating_source(context_files, monkeypatch):
    import stock_verdict
    _, selection = context_files
    before = deepcopy(selection)
    monkeypatch.setattr(stock_verdict, '_j', lambda *a: pytest.fail('duplicate central JSON read'))
    result, row, formal = stock_verdict._triad_record('000333.SZ', selection=selection)
    assert result == selection == before and row['code'] == '000333.SZ' and not formal


def test_observation_cache_respects_publication_change_and_clock_expiry(tmp_path):
    from deep_analysis_data import observation_html
    now = datetime(2026, 9, 15, 13, tzinfo=timezone.utc)
    path = tmp_path/'market_watch_pub.json'
    doc = {'generated_at': now.isoformat(), 'rows': [{'code': '000333.SZ', 'market': 'A股'}]}
    write(path, doc)
    assert '观察证据评分' in observation_html('000333.SZ', tmp_path, now=now)
    assert observation_html('000333.SZ', tmp_path, now=now+timedelta(hours=13)) == ''
    write(path, {**doc, 'rows': []})
    assert observation_html('000333.SZ', tmp_path, now=now) == ''


@pytest.mark.parametrize('prior', [
    {'status': 'complete'}, {'status': 'failed', 'requested_at': 0}, {},
])
def test_timer_never_restarts_model_for_missing_expired_or_finished_review(monkeypatch, prior):
    monkeypatch.setattr(jobs, 'read', lambda code: prior)
    monkeypatch.setattr(jobs, 'request', lambda *a, **k: pytest.fail('timer started model'))
    for _ in range(3): assert jobs.poll_job('000333.SZ', {})['status'] == 'failed'
    if prior: assert jobs.poll_job('000333.SZ', {}, start=True)['status'] == 'failed'


class Rerun(Exception): pass


class Streamlit:
    def __init__(self):
        self.session_state = {}
        self.intervals = []
        self.panel = None
        self.reruns = 0
    def fragment(self, *, run_every):
        self.intervals.append(run_every)
        def decorate(fn):
            self.panel = fn
            return fn
        return decorate
    def rerun(self, **kwargs):
        assert not kwargs
        self.reruns += 1
        raise Rerun()
    def progress(self, *a, **k): pass
    def caption(self, *a, **k): pass
    def markdown(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def button(self, *a, **k): return False
    def expander(self, *a, **k): return self
    def __enter__(self): return self
    def __exit__(self, *a): pass


def test_running_to_idle_switches_timer_once_without_duplicate_request(monkeypatch):
    import deep_analysis_data as data
    context = [{}]
    launch = []
    running = {'status': 'running', 'pid': 123, 'stage': 'primary', 'requested_at': 100}
    monkeypatch.setattr(cross, 'load_context', lambda code: deepcopy(context[0]))
    monkeypatch.setattr(data, 'report_html', lambda *a, **k: 'report')
    monkeypatch.setattr(jobs, 'read', lambda code: {})
    monkeypatch.setattr(jobs, 'worker_alive', lambda *a: True)
    monkeypatch.setattr(jobs, 'request', lambda *a, **k: launch.append(1) or running)
    st = Streamlit()
    jobs.render(st, '000333.SZ')
    st.panel()
    assert launch == [1] and st.intervals == [2]
    context[0] = {'card': {'total': 70, 'gpt': {'current': True, 'complete': True},
                           'books': {'current': True, 'complete': True}}}
    with pytest.raises(Rerun): st.panel()
    jobs.render(st, '000333.SZ')
    st.panel()
    assert st.intervals == [2, 30] and st.reruns == 1 and launch == [1]
    context[0] = {}  # Later expiry with unchanged source must not re-launch.
    st.panel()
    assert launch == [1] and st.reruns == 1


def test_explicit_retry_switches_idle_to_fast_once(monkeypatch):
    import deep_analysis_data as data
    running = {'status': 'running', 'pid': 123, 'stage': 'primary'}
    prior = [{'status': 'failed', 'error': 'test source gap'}]
    calls = []
    monkeypatch.setattr(cross, 'load_context', lambda code: {})
    monkeypatch.setattr(data, 'report_html', lambda *a, **k: 'report')
    monkeypatch.setattr(jobs, 'read', lambda code: prior[0])
    monkeypatch.setattr(jobs, 'worker_alive', lambda *a: True)
    def retry(*args, **kwargs):
        calls.append(kwargs)
        prior[0] = running
        return running
    monkeypatch.setattr(jobs, 'request', retry)
    st = Streamlit()
    jobs.render(st, '000333.SZ')
    st.button = lambda *a, **k: True
    with pytest.raises(Rerun): st.panel()
    st.button = lambda *a, **k: False
    jobs.render(st, '000333.SZ')
    st.panel()
    assert st.intervals == [30, 2] and st.reruns == 1
    assert calls == [{'retry': True}]
