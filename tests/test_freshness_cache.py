import json, os, time

import module_freshness as m


def test_generated_stamp_reparses_only_when_file_changes(tmp_path, monkeypatch):
    path = tmp_path / 'x_pub.json'
    path.write_text(json.dumps({'generated_at': '2026-10-09T10:00:00+00:00', 'rows': [1] * 10}))
    calls = []
    real = m.read
    monkeypatch.setattr(m, 'read', lambda p: calls.append(p) or real(p))
    assert m.generated_at(path) == '2026-10-09T10:00:00+00:00'
    assert m.generated_at(path) == '2026-10-09T10:00:00+00:00' and len(calls) == 1
    path.write_text(json.dumps({'generated_at': '2026-10-09T11:00:00+00:00'}))
    os.utime(path, ns=(time.time_ns() + 10**9,) * 2)
    assert m.generated_at(path) == '2026-10-09T11:00:00+00:00' and len(calls) == 2
    path.unlink()
    assert m.generated_at(path) is None and m.record('x_pub.json', base=tmp_path)['generated'] == '未记录'


def test_snapshot_is_shared_until_changed(tmp_path):
    path = tmp_path / 'board.json'
    path.write_text(json.dumps({'version': 1}))
    first = m.read_snapshot(path)
    assert m.read_snapshot(path) is first
    path.write_text(json.dumps({'version': 2, 'pad': 'x'}))
    assert m.read_snapshot(path)['version'] == 2
    assert m.read_snapshot(tmp_path / 'missing.json') == {}
