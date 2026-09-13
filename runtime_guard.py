"""Idempotent local UI startup. Reuse a healthy V88; never kill by port/pattern."""
import argparse,fcntl,json,os,socket,subprocess,sys,time
from contextlib import contextmanager
from pathlib import Path
from urllib.request import build_opener,ProxyHandler
from datetime import datetime,timezone

ROOT=Path(__file__).resolve().parent

def ensure_file_capacity(target=4096, resource_api=None):
    """Raise only this process's soft limit, bounded by the OS hard limit."""
    try:
        if resource_api is None:
            import resource as resource_api
        soft, hard = resource_api.getrlimit(resource_api.RLIMIT_NOFILE)
        wanted = target if hard == resource_api.RLIM_INFINITY else min(target, hard)
        if soft == resource_api.RLIM_INFINITY or soft >= wanted:
            return {'changed':False,'soft':soft,'hard':hard}
        resource_api.setrlimit(resource_api.RLIMIT_NOFILE,(wanted,hard))
        return {'changed':True,'soft':wanted,'hard':hard}
    except (ImportError,OSError,ValueError) as exc:
        return {'changed':False,'error':type(exc).__name__}

def health(port):
    try:
        with build_opener(ProxyHandler({})).open(f'http://127.0.0.1:{port}/_stcore/health',timeout=3) as r:
            return r.status==200 and r.read(16).strip()==b'ok'
    except (OSError,ValueError):return False

def listeners(port):
    r=subprocess.run(['/usr/sbin/lsof','-nP','-tiTCP:'+str(port),'-sTCP:LISTEN'],capture_output=True,text=True,timeout=5)
    return sorted({int(x) for x in r.stdout.split() if x.isdigit()})

def owned(pid):
    r=subprocess.run(['/bin/ps','-p',str(pid),'-o','command='],capture_output=True,text=True,timeout=5)
    command=r.stdout.strip()
    if ' -m streamlit run ' not in command:return False
    script=command.split(' -m streamlit run ',1)[1].split(' --',1)[0]
    if Path(script).is_absolute():return Path(script).resolve()==ROOT/'app_v88_integrated.py'
    cwd=subprocess.run(['/usr/sbin/lsof','-a','-p',str(pid),'-d','cwd','-Fn'],capture_output=True,text=True,timeout=5)
    return script=='app_v88_integrated.py' and 'n'+str(ROOT) in cwd.stdout.splitlines()

def ensure(port=8501,*,probe=health,find=listeners,identify=owned,start=None):
    pids=find(port)
    if pids:
        if len(pids)!=1 or not identify(pids[0]):return {'ok':False,'status':'port_owned_by_other_service','port':port}
        if probe(port):return {'ok':True,'status':'reused_healthy','pid':pids[0],'port':port}
        return {'ok':False,'status':'existing_v88_unhealthy','pid':pids[0],'port':port,
                'next_step':'Preserve the process and inspect its error log; never kill a shared service blindly.'}
    if start is None:
        import shutil
        if shutil.disk_usage(ROOT).free<512*1024*1024:return {'ok':False,'status':'insufficient_disk_space'}
        log=ROOT/'logs/runtime-ui.log';log.parent.mkdir(exist_ok=True)
        if log.exists() and log.stat().st_size>20*1024*1024:
            log.rename(log.with_name('runtime-ui-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'.log'))
        args=[sys.executable,'-m','streamlit','run',str(ROOT/'app_v88_integrated.py'),'--server.address','0.0.0.0','--server.port',str(port),'--server.headless','true','--server.enableCORS','true','--server.enableXsrfProtection','true','--browser.gatherUsageStats','false']
        def start():
            with log.open('ab') as output:
                return subprocess.Popen(args,cwd=ROOT,stdout=output,stderr=subprocess.STDOUT,start_new_session=True).pid
    pid=start()
    for _ in range(25):
        if probe(port):return {'ok':True,'status':'started_healthy','pid':pid,'port':port}
        time.sleep(.4)
    return {'ok':False,'status':'startup_not_ready','pid':pid,'port':port}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--port',type=int,default=8501);a=p.parse_args()
    if not 1024<=a.port<=65535:p.error('port out of range')
    lock=ROOT/'logs/runtime-ui.lock';lock.parent.mkdir(exist_ok=True)
    with lock.open('a') as f:
        try:fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({'ok':False,'status':'startup_already_running'}));sys.exit(2)
        result=ensure(a.port);print(json.dumps(result));sys.exit(0 if result['ok'] else 1)
