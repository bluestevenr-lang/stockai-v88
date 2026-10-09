"""Show persisted data immediately; cloud synchronization never blocks a page."""
from pathlib import Path
import subprocess, sys, time, json, os
from datetime import datetime, timezone
from v88_paths import core_root


def schedule_sync():
    base=core_root();script=base/'src/autonomous_sync.py'
    if not script.exists():return False
    stamp=base/'data/.autonomous_sync_attempt'
    lock=base/'data/.autonomous_sync_ui_lock'
    try:
        # O_EXCL makes simultaneous browser sessions start at most one worker.
        try:fd=os.open(lock,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        except FileExistsError:
            if time.time()-lock.stat().st_mtime>150:lock.unlink(missing_ok=True)
            return False
        os.close(fd)
        if stamp.exists() and time.time()-stamp.stat().st_mtime<60:
            lock.unlink(missing_ok=True);return False
        stamp.touch()
        log=base/'logs/autonomous_sync.log';log.parent.mkdir(exist_ok=True)
        with log.open('a') as out:
            subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'--worker',str(base)],cwd=base,
                stdout=out,stderr=out,start_new_session=True)
        return True
    except OSError:
        lock.unlink(missing_ok=True)
        return False


def _worker(base):
    base=Path(base);state=base/'data/autonomous_sync_ui_status.json'
    def write(status):
        tmp=state.with_suffix('.tmp')
        tmp.write_text(json.dumps({'status':status,'at':datetime.now(timezone.utc).isoformat()}));tmp.replace(state)
    try:
        write('running')
        result=subprocess.run([sys.executable,str(base/'src/autonomous_sync.py')],cwd=base,timeout=120)
        write('complete' if result.returncode==0 else 'failed')
    except (OSError,subprocess.TimeoutExpired):write('failed')
    finally:(base/'data/.autonomous_sync_ui_lock').unlink(missing_ok=True)


def sync_on_open():
    import streamlit as st
    st.session_state.setdefault('_v88_opened_at',datetime.now(timezone.utc).isoformat())
    schedule_sync()
    # The fragment reads small local files only. No network call on the UI thread.
    @st.fragment(run_every=5)
    def sync_status():
        schedule_sync()
        from module_freshness import read,inventory
        base=core_root()/'data'
        state=read(base/'autonomous_sync_ui_status.json')
        receipt=read(base/'autonomous_sync_status.json')
        board=read(base/'strategy_board.json')
        running=(base/'.autonomous_sync_ui_lock').exists()
        from sync_status_view import html as status_card
        st.html(status_card(board,receipt,state,running=running,
            opened=st.session_state['_v88_opened_at'],modules=inventory(),base=base))
    sync_status()

if __name__=='__main__' and len(sys.argv)>2 and sys.argv[1]=='--worker':_worker(sys.argv[2])
