"""Native Windows smoke tests; no private repo, market fetch or model calls."""
import ast
import builtins
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest
import runtime_guard as guard
import platform_lock as lock

ROOT=Path(__file__).resolve().parents[1]


def test_current_process_probe_is_read_only_and_rejects_special_pids():
    child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)'])
    try:
        assert guard.process_alive(child.pid)
        assert child.poll() is None
        assert not any(guard.process_alive(pid) for pid in (0,-1,True,None,'123'))
    finally:
        child.terminate();child.wait(timeout=10)
    assert not guard.process_alive(child.pid)


def test_file_lock_contention_and_release_across_real_processes(tmp_path):
    path=tmp_path/'guard.lock'
    code='''import sys, platform_lock as lock
with open(sys.argv[1], 'a') as stream:
    try: lock.flock(stream, lock.LOCK_EX | lock.LOCK_NB)
    except BlockingIOError: sys.exit(2)
    lock.flock(stream, lock.LOCK_UN)
'''
    with path.open('a') as stream:
        lock.flock(stream,lock.LOCK_EX|lock.LOCK_NB)
        result=subprocess.run([sys.executable,'-c',code,str(path)],cwd=ROOT,timeout=10)
        assert result.returncode==2
        lock.flock(stream,lock.LOCK_UN)
    assert subprocess.run([sys.executable,'-c',code,str(path)],cwd=ROOT,timeout=10).returncode==0


def test_missing_resource_module_does_not_break_ui_start(monkeypatch):
    original=builtins.__import__
    def import_module(name,*args,**kwargs):
        if name=='resource':raise ModuleNotFoundError(name)
        return original(name,*args,**kwargs)
    monkeypatch.setattr(builtins,'__import__',import_module)
    assert guard.ensure_file_capacity()=={'changed':False,'error':'ModuleNotFoundError'}


def test_windows_listener_lookup_fails_closed_for_unknown_ownership(monkeypatch):
    monkeypatch.setattr(guard,'os',SimpleNamespace(name='nt'))
    def conn(pid,port=8501,state=guard.psutil.CONN_LISTEN):
        return SimpleNamespace(pid=pid,laddr=SimpleNamespace(port=port),status=state)
    monkeypatch.setattr(guard.psutil,'net_connections',lambda kind:[conn(23),conn(23),conn(42,8502)])
    assert guard.listeners(8501)==[23]
    monkeypatch.setattr(guard.psutil,'net_connections',lambda kind:[conn(None)])
    result=guard.ensure(find=guard.listeners,start=lambda:pytest.fail('must not start'))
    assert result['status']=='listener_lookup_failed'


def test_windows_ownership_requires_exact_script_and_checkout(monkeypatch,tmp_path):
    monkeypatch.setattr(guard,'os',SimpleNamespace(name='nt'))
    monkeypatch.setattr(guard,'ROOT',tmp_path)
    args=['python.exe','-m','streamlit','run','app_v88_integrated.py']
    proc=SimpleNamespace(cmdline=lambda:args,cwd=lambda:str(tmp_path))
    monkeypatch.setattr(guard.psutil,'Process',lambda pid:proc)
    assert guard.owned(12)
    args[-1]='foreign.py'
    assert not guard.owned(12)
    args[:]=['python.exe','-m','streamlit','run',str(tmp_path/'other'/'app_v88_integrated.py')]
    assert not guard.owned(12)


def test_real_page_startup_imports_work_on_this_operating_system():
    from win.verify_runtime import verify_startup_imports
    verify_startup_imports(ROOT)


def test_verifier_detects_missing_startup_dependency_before_success(tmp_path):
    from win.verify_runtime import verify_startup_imports
    (tmp_path/'app_v88_integrated.py').write_text('from nonexistent_v88_startup_module import guard\n')
    with pytest.raises(ModuleNotFoundError):verify_startup_imports(tmp_path)


def test_no_direct_unix_import_or_kill_probe_returns_in_desktop_consumers():
    for name in ('runtime_guard.py','auto_reporter.py','scan_progress_view.py',
                 'scan_worker.py','app_v88_integrated.py'):
        tree=ast.parse((ROOT/name).read_text(encoding='utf-8-sig'))
        for node in ast.walk(tree):
            if isinstance(node,ast.Import):assert all(x.name!='fcntl' for x in node.names),name
            if isinstance(node,ast.ImportFrom):assert node.module!='fcntl',name
            if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute):
                assert not (isinstance(node.func.value,ast.Name) and node.func.value.id=='os'
                            and node.func.attr=='kill'),name


def test_updater_prints_installed_manifest_version(tmp_path,monkeypatch):
    from win import update_v88
    (tmp_path/'win').mkdir()
    (tmp_path/'win/release.json').write_text('{"version":"2026.09.13.2"}')
    monkeypatch.setattr(update_v88,'ROOT',tmp_path)
    assert update_v88.release_version()=='2026.09.13.2'
