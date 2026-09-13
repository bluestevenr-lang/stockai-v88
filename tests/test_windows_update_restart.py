from pathlib import Path
from types import SimpleNamespace
import json
import pytest
from win import update_v88 as updater


def test_foreign_service_is_never_stopped(tmp_path,monkeypatch):
    import runtime_guard
    monkeypatch.setattr(runtime_guard,'listeners',lambda port:[123])
    monkeypatch.setattr(runtime_guard,'owned',lambda pid:False)
    monkeypatch.setattr(updater.subprocess,'Popen',lambda *a,**kw:pytest.fail('foreign service changed'))
    assert updater.restart_updated_ui(tmp_path) is False


def test_exact_owned_service_restarts_once_and_preserves_command(tmp_path,monkeypatch):
    import runtime_guard,psutil
    (tmp_path/'win').mkdir();(tmp_path/'win/release.json').write_text('{"version":"test"}')
    calls=[];current=[123]
    class Process:
        def __init__(self,pid):self.pid=pid
        def create_time(self):return float(self.pid)
        def cmdline(self):return ['python','-m','streamlit','run',str(tmp_path/'app_v88_integrated.py')]
        def cwd(self):return str(tmp_path)
        def terminate(self):calls.append('terminate')
        def wait(self,timeout):calls.append(('wait',timeout))
    def start(cmd,**kw):
        calls.append((cmd,kw));current[:]=[456]
        return SimpleNamespace(pid=456,poll=lambda:None)
    monkeypatch.setattr(runtime_guard,'listeners',lambda port:current)
    monkeypatch.setattr(runtime_guard,'owned',lambda pid:True)
    monkeypatch.setattr(runtime_guard,'health',lambda port:True)
    monkeypatch.setattr(psutil,'Process',Process)
    monkeypatch.setattr(updater.subprocess,'Popen',start)
    assert updater.restart_updated_ui(tmp_path)
    assert calls[0]=='terminate' and calls[1]==('wait',20)
    assert calls[2][0][-1]==str(tmp_path/'app_v88_integrated.py')
    assert calls[2][1]['env']['PYTHONUTF8']=='1'
    assert updater.restart_updated_ui(tmp_path) and len(calls)==3
