"""Non-blocking data-only cloud sync; independent of model availability."""
from pathlib import Path
import subprocess,sys,time
from v88_paths import core_root


def schedule_sync():
    base=core_root();script=base/'src/autonomous_sync.py'
    if not script.exists():return
    stamp=base/'data/.autonomous_sync_attempt'
    try:
        if stamp.exists() and time.time()-stamp.stat().st_mtime<300:return
        stamp.touch()
        log=base/'logs/autonomous_sync.log';log.parent.mkdir(exist_ok=True)
        with log.open('a') as out:
            subprocess.Popen([sys.executable,str(script)],cwd=base,stdout=out,stderr=out,start_new_session=True)
    except OSError:return
