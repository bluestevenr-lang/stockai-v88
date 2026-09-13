import hashlib
import importlib.util
import json
from pathlib import Path
import zipfile
import pytest

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('windows_update',ROOT/'win/update_v88.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

def bundle(tmp,names):
    (tmp/'sync').mkdir();(tmp/'data').mkdir()
    with zipfile.ZipFile(tmp/'sync/windows_snapshot.zip','w') as z:
        for n,raw in names.items():z.writestr(n,raw)
    manifest={'source_at':'2026-09-13T05:00:00Z','sha256':hashlib.sha256((tmp/'sync/windows_snapshot.zip').read_bytes()).hexdigest(),
              'files':{n:hashlib.sha256(v).hexdigest() for n,v in names.items()}}
    (tmp/'sync/windows_snapshot.json').write_text(json.dumps(manifest))

def test_snapshot_keeps_source_and_local_trades_and_is_idempotent(tmp_path):
    raw=b'{"generated_at":"2026-09-11"}'
    bundle(tmp_path,{'triad_selection.json':raw})
    old=tmp_path/'data/triad_selection.json';old.write_text('{}')
    trades=tmp_path/'data/astra_trade_journal.sqlite';trades.write_bytes(b'private')
    m.install_snapshot(tmp_path)
    assert old.read_bytes()==raw and trades.read_bytes()==b'private'
    assert list((tmp_path/'.v88-work/windows-backups').glob('*/triad_selection.json'))[0].read_text()=='{}'
    old.write_text('{"newer":true}')
    m.install_snapshot(tmp_path)
    assert json.loads(old.read_text())['newer']

@pytest.mark.parametrize('name',['../positions.json','positions.json','astra_trade_monitor.json','.env.json','accounts.json','financial_primary_evidence.json'])
def test_snapshot_rejects_private_or_escaping_members(tmp_path,name):
    bundle(tmp_path,{name:b'{}'})
    with pytest.raises(ValueError):m.install_snapshot(tmp_path)

def test_corrupt_bundle_does_not_touch_current_files(tmp_path):
    bundle(tmp_path,{'triad_selection.json':b'{}'})
    current=tmp_path/'data/triad_selection.json';current.write_text('{"old":1}')
    (tmp_path/'sync/windows_snapshot.zip').write_bytes(b'corrupt')
    with pytest.raises(ValueError):m.install_snapshot(tmp_path)
    assert json.loads(current.read_text())=={'old':1}

def test_malformed_member_is_rejected_before_any_install(tmp_path):
    bundle(tmp_path,{'a.json':b'{}','b.json':b'{bad'})
    with pytest.raises(ValueError):m.install_snapshot(tmp_path)
    assert not (tmp_path/'data/a.json').exists()

def test_partial_install_failure_rolls_back_every_file(tmp_path,monkeypatch):
    bundle(tmp_path,{'a.json':b'{"new":1}','b.json':b'{}'})
    (tmp_path/'data/a.json').write_bytes(b'{"old":1}')
    original=m.os.replace
    def replace(a,b):
        if Path(b).name=='b.json':raise OSError('disk full')
        return original(a,b)
    monkeypatch.setattr(m.os,'replace',replace)
    with pytest.raises(OSError):m.install_snapshot(tmp_path)
    assert (tmp_path/'data/a.json').read_bytes()==b'{"old":1}'
    assert not (tmp_path/'data/b.json').exists()
