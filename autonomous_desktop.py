"""Non-blocking data-only cloud sync; independent of model availability."""
from pathlib import Path
import subprocess,sys,time
from v88_paths import core_root


def schedule_sync():
    base=core_root();script=base/'src/autonomous_sync.py'
    if not script.exists():return
    stamp=base/'data/.autonomous_sync_attempt'
    try:
        if stamp.exists() and time.time()-stamp.stat().st_mtime<60:return
        stamp.touch()
        log=base/'logs/autonomous_sync.log';log.parent.mkdir(exist_ok=True)
        with log.open('a') as out:
            subprocess.Popen([sys.executable,str(script)],cwd=base,stdout=out,stderr=out,start_new_session=True)
    except OSError:return


def sync_on_open():
    """Check the latest private publication once before a new session reads tables."""
    import streamlit as st
    from datetime import datetime,timezone
    if '_v88_opened_at' in st.session_state:
        schedule_sync()
        return
    st.session_state['_v88_opened_at']=datetime.now(timezone.utc).isoformat()
    base=core_root();script=base/'src/autonomous_sync.py'
    if not script.exists():return
    try:
        with st.spinner('正在核对 GitHub 最新筛选结果…'):
            result=subprocess.run([sys.executable,str(script)],cwd=base,capture_output=True,text=True,timeout=20)
        state='已核对GitHub最新发布' if result.returncode==0 else '云端核对失败，显示本机缓存'
    except (OSError,subprocess.TimeoutExpired):state='云端核对超时，显示本机缓存'
    st.session_state['_v88_open_sync']=state
    try:(base/'data/.autonomous_sync_attempt').touch()
    except OSError:pass
