"""Explicit, non-blocking GitHub refresh, including weekends and holidays."""
from datetime import datetime, timezone, timedelta
from pathlib import Path
import json
import os
import subprocess
import sys
import time
import uuid

REPO = 'bluestevenr-lang/stockai-v88'
STATE = 'astra_manual_refresh.json'
LOCK = '.astra_manual_refresh.lock'
ACTIVE = {'requested', 'queued', 'in_progress', 'syncing'}


def read(base):
    try:
        return json.loads((Path(base)/'data'/STATE).read_text())
    except (OSError, ValueError):
        return {}


def write(base, value):
    target = Path(base)/'data'/STATE
    tmp = target.with_suffix('.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False))
    tmp.replace(target)


def busy(base):
    lock = Path(base)/'data'/LOCK
    return lock.exists() and time.time()-lock.stat().st_mtime < 180


def start(base):
    """Only spawn locally; network calls never run on Streamlit's UI thread."""
    base = Path(base)
    (base/'data').mkdir(exist_ok=True)
    lock = base/'data'/LOCK
    if lock.exists():
        if busy(base):
            return False
        lock.unlink(missing_ok=True)
    token = uuid.uuid4().hex
    try:
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        return False
    with os.fdopen(fd, 'w') as out:
        out.write(token)
    write(base, {'status': 'requested', 'requested_at': datetime.now(timezone.utc).isoformat()})
    try:
        subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--worker', str(base), token],
                         cwd=base, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                         start_new_session=True)
    except OSError:
        lock.unlink(missing_ok=True)
        write(base, {'status': 'failed', 'message': '更新进程未能启动，请重试'})
        return False
    return True


def run_cli(args):
    from autonomous_sync import github_cli
    result = subprocess.run([github_cli(), *args], capture_output=True, text=True, timeout=40, check=True)
    return json.loads(result.stdout) if result.stdout.strip() else None


def request_run(cli=run_cli):
    runs = cli(['run', 'list', '-R', REPO, '--workflow', 'autonomous.yml', '--limit', '20',
                '--json', 'databaseId,status,url,createdAt']) or []
    active = [r for r in runs if r['status'] in {'queued', 'in_progress', 'waiting', 'pending'}]
    if active:
        return min(active, key=lambda r: r['createdAt']), True
    # Manual full runs are intentionally NOT gated by exchange working days.
    cli(['workflow', 'run', 'autonomous.yml', '-R', REPO, '-f', 'fast=false', '-f', 'send=false'])
    return None, False


def worker(base, token):
    base = Path(base)
    sys.path.insert(0, str(base/'src'))
    lock = base/'data'/LOCK
    state = read(base)
    try:
        run, reused = request_run()
        state['reused'] = reused
        state['status'] = 'queued'
        write(base, state)
        deadline = time.monotonic()+2400
        while time.monotonic() < deadline:
            lock.touch()
            if run is None:
                runs = run_cli(['run', 'list', '-R', REPO, '--workflow', 'autonomous.yml', '--event',
                                'workflow_dispatch', '--limit', '10', '--json', 'databaseId,status,url,createdAt']) or []
                requested = datetime.fromisoformat(state['requested_at'])-timedelta(seconds=2)
                eligible = [r for r in runs if datetime.fromisoformat(r['createdAt'].replace('Z', '+00:00')) >= requested]
                if eligible:
                    run = min(eligible, key=lambda r: r['createdAt'])
            if run:
                detail = run_cli(['run', 'view', str(run['databaseId']), '-R', REPO,
                                  '--json', 'status,conclusion,jobs,url'])
                state.update(run_id=run['databaseId'], url=detail['url'], status=detail['status'])
                jobs = [j for j in detail.get('jobs', []) if j.get('name') == 'facts']
                steps = jobs[0].get('steps', []) if jobs else []
                state['steps_done'] = sum(s.get('status') == 'completed' for s in steps)
                state['steps_total'] = len(steps)
                current = next((s.get('name') for s in steps if s.get('status') == 'in_progress'), '')
                state['phase'] = current
                if detail['status'] == 'completed':
                    if detail['conclusion'] != 'success':
                        raise RuntimeError('云端运行未成功，保留原记录；可重试或查看运行详情')
                    state['status'] = 'syncing'
                    write(base, state)
                    subprocess.run([sys.executable, str(base/'src/autonomous_sync.py')], cwd=base,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=150, check=True)
                    doc = json.loads((base/'data/astra_cycle.json').read_text())
                    generated = datetime.fromisoformat(doc['generated_at'].replace('Z', '+00:00'))
                    started = datetime.fromisoformat(run['createdAt'].replace('Z', '+00:00'))
                    if generated < started:
                        raise RuntimeError('云端结束，但新 Astra 文件尚未同步；保留原记录，请重试')
                    state.update(status='complete', completed_at=datetime.now(timezone.utc).isoformat(),
                                 screening_at=doc['generated_at'], cycle_id=doc['cycle_id'])
                    write(base, state)
                    return
            write(base, state)
            time.sleep(15)
        raise RuntimeError('云端仍未完成；可查看运行详情，再次点击将接续已有任务')
    except Exception as exc:
        # Do not expose CLI output, tokens, or private file contents in the UI.
        message = str(exc) if isinstance(exc, RuntimeError) else '连接或同步失败，请检查 GitHub 登录与网络后重试；原记录保留'
        state.update(status='failed', message=message)
        write(base, state)
    finally:
        try:
            if lock.read_text() == token:
                lock.unlink(missing_ok=True)
        except OSError:
            pass


def render(base):
    import streamlit as st
    state = read(base)
    running = busy(base)
    completed = state.get('completed_at') if state.get('status') == 'complete' else None
    if completed and st.query_params.get('focus') == 'astra' and st.session_state.get('_astra_displayed_completion') != completed:
        st.session_state['_astra_displayed_completion'] = completed
        st.rerun()
    left, right = st.columns([1, 4])
    with left:
        if st.button('↻ 立即更新 Astra', key='astra_cloud_refresh', disabled=running,
                     help='休市也可更新：GitHub 重新筛选，保留真实行情日和全部历史；不调用模型，不额外推送。'):
            start(base)
            st.rerun(scope='fragment')
    with right:
        status = state.get('status')
        if running:
            done, total = state.get('steps_done', 0), state.get('steps_total', 0)
            label = '同步结果' if status == 'syncing' else ('云端运行' if status == 'in_progress' else '连接 / 排队')
            progress = f' · {done}/{total} 步' if total else ''
            st.caption(f'◌ {label}{progress} · 可以继续浏览')
        elif status == 'complete':
            stamp = datetime.fromisoformat(state['screening_at']).astimezone(timezone(timedelta(hours=8)))
            st.caption(f'✓ 已更新 · 筛选完成 {stamp:%m-%d %H:%M:%S} 北京时间')
        elif status == 'failed':
            st.caption('⚠ '+state.get('message', '更新未完成，可重试'))
        elif status in ACTIVE:
            st.caption('上次连接已中断；点击接续云端任务，原记录保留')
        else:
            st.caption('休市日也可用 · 行情保留真实交易日期')
        if state.get('url', '').startswith('https://github.com/'+REPO+'/actions/runs/'):
            st.caption(f'[查看云端进度]({state["url"]})')


if __name__ == '__main__' and len(sys.argv) == 4 and sys.argv[1] == '--worker':
    worker(sys.argv[2], sys.argv[3])
