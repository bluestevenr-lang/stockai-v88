"""User-requested on-open 3A reviews, using the existing central publisher."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import threading

import psutil
import platform_lock as locking

ROOT = Path(__file__).resolve().parent
JOBS = ROOT / 'logs' / 'deep-reviews'
STEPS = ('facts_done', 'books_done', 'primary_done', 'counteraudit_done', 'published')
LABELS = {'queued': '等待审核通道', 'sources': '核对最新数据与八域证据',
          'facts_done': '核对经典书理', 'books_done': '准备GPT主审',
          'primary': 'GPT主审中', 'primary_done': '主审已完成',
          'counteraudit': 'GPT独立反审中', 'counteraudit_done': '双审已完成',
          'publication_wait': '双审已完成，等待后台提交后发布',
          'publishing': '校验并发布中央评分', 'published': '评分已发布'}


def progress_view(job, *, now=None):
    """Count completed milestones, never elapsed-time or invented model percent."""
    done = set(job.get('completed_steps') or ()) & set(STEPS)
    if job.get('status') == 'complete':
        done = set(STEPS)
    seconds = max(0, (time.time() if now is None else now) - job.get('requested_at', time.time()))
    return {'percent': len(done) * 20, 'completed': len(done),
            'label': LABELS.get(job.get('stage') or job.get('status'), '等待审核结果'),
            'elapsed': f'{int(seconds)//60}分{int(seconds)%60:02d}秒'}


def valid_score(context):
    from math import isfinite
    card = context.get('card') or {}
    score = card.get('total')
    return (type(score) in (int, float) and isfinite(score)
            and 0 <= score <= 100
            and all((card.get(k) or {}).get('current') and
                    (card.get(k) or {}).get('complete') for k in ('gpt', 'books')))


def save(path, value):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False), encoding='utf-8')
    os.replace(tmp, path)


def read(code):
    try:
        return json.loads((JOBS / (code + '.json')).read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {}


def worker_alive(pid, code):
    try:
        p = psutil.Process(pid)
        return (p.status() != psutil.STATUS_ZOMBIE and
                p.cmdline()[-2:] == [str(Path(__file__).resolve()), code])
    except (psutil.Error, ValueError, TypeError):
        return False


def request(code, context, retry=False):
    from focused_deep_view import normalize_code
    if normalize_code(code) != code:
        raise ValueError('invalid stock code')
    if valid_score(context):
        return {'status': 'complete', 'score': context['card']['total']}
    if os.getenv('V88_DISABLE_LLM') == '1' or os.getenv('GITHUB_ACTIONS') == 'true':
        return {'status': 'failed', 'error': '当前环境不允许模型审核，请在本机V88打开个股分析。'}
    JOBS.mkdir(parents=True, exist_ok=True)
    with (JOBS / (code + '.lock')).open('a') as lock:
        locking.flock(lock, locking.LOCK_EX)
        prior = read(code)
        if prior.get('status') in ('queued', 'running'):
            if prior.get('pid') and worker_alive(prior['pid'], code):
                return prior
            prior = {**prior, 'status': 'failed', 'error': '上次审核进程已停止，可以重新分析。'}
        # Do not spend subscription quota repeatedly on refresh or failed facts.
        if prior.get('status') == 'failed' and not retry and time.time() - prior.get('requested_at', 0) < 3600:
            return prior
        job = {'code': code, 'status': 'queued', 'stage': 'queued',
               'completed_steps': [], 'requested_at': time.time(),
               'resume_directory': prior.get('run_directory') or prior.get('resume_directory')}
        path = JOBS / (code + '.json')
        save(path, job)
        try:
            with (JOBS / (code + '.log')).open('ab') as log:
                options = ({'creationflags': subprocess.CREATE_NEW_PROCESS_GROUP}
                           if os.name == 'nt' else {'start_new_session': True})
                p = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), code],
                                     cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, **options)
            job['pid'] = p.pid
            save(path, job)
        except Exception as exc:
            save(path, {**job, 'status': 'failed', 'error': str(exc)})
            raise
        return job


def poll_job(code, context, *, previous=None, start=False):
    """Read progress without letting timer refreshes request another model run."""
    if valid_score(context):
        return {'status': 'complete', 'score': context['card']['total']}
    prior = read(code) or previous or {}
    if prior.get('status') in ('queued', 'running'):
        if prior.get('pid') and worker_alive(prior['pid'], code):
            return prior
        return {**prior, 'status': 'failed', 'error': '上次审核进程已停止，可以重新分析。'}
    if prior.get('status') == 'complete':
        return {**prior, 'status': 'failed', 'error': '审核已结束，当前发布或有效期仍待核对；刷新不会重复启动审核。'}
    if prior.get('status') == 'failed':
        return prior
    if start:
        return request(code, context)
    return {'status': 'failed', 'error': '尚无可用审核结果；可手动重新分析。'}


def poll_interval(job):
    return 2 if job.get('status') in ('queued', 'running') else 30


def worker(code):
    from v88_paths import core_root
    core = core_root()
    sys.path.insert(0, str(core / 'src'))
    from review_pipeline import build
    from interactive_review_queue import pending
    from data_ingest_lock import DataIngestBusy
    path = JOBS / (code + '.json')
    # Wait for the spawning parent to commit the PID before writing status.
    with (JOBS / (code + '.lock')).open('a') as lock:
        locking.flock(lock, locking.LOCK_EX)
        job = {**read(code), 'pid': os.getpid(), 'status': 'queued', 'stage': 'queued'}
        save(path, job)
    queue_path = core / 'data/interactive_review_requests' / (code + '.json')
    queue_path.parent.mkdir(parents=True, exist_ok=True)
    stop = threading.Event()
    def heartbeat():
        while not stop.is_set():
            save(queue_path, {'code': code, 'pid': os.getpid(), 'status': 'running',
                'requested_at': job['requested_at'], 'heartbeat_at': time.time()})
            stop.wait(5)
    pulse = threading.Thread(target=heartbeat, daemon=True)
    pulse.start()
    def progress(stage, **extra):
        done = set(job.get('completed_steps') or ())
        if stage in STEPS:
            done.add(stage)
        job.update(status='running', stage=stage, completed_steps=[s for s in STEPS if s in done],
                   updated_at=time.time(), **extra)
        save(path, job)
    try:
        # Refresh the deterministic module projection if its immutable index advanced.
        import sqlite3
        with sqlite3.connect((core / 'data/full_market_research.sqlite').resolve().as_uri() + '?mode=ro', uri=True) as db:
            active = db.execute('SELECT snapshot FROM active WHERE id=1').fetchone()[0]
        cache = json.loads((core / 'data/value_candidate_facts.json').read_text(encoding='utf-8'))
        if cache.get('full_index_snapshot') != active:
            from module_fact_projection import build as project
            project(core)
        result = {}
        from joint_review_resume import resumable, resume
        resume_directory = job.get('resume_directory')
        for _ in range(900):
            waiting = pending(core)
            if waiting and waiting[0]['code'] != code:
                time.sleep(2)
                continue
            # Resume valid saved facts/primary receipts before creating another
            # prompt; refresh/poll never repeats a completed independent review.
            try:
                if resume_directory and resumable(core, resume_directory):
                    result = resume(resume_directory, base=core, workers=1, limit_batches=1,
                                    on_progress=progress)
                else:
                    result = build(1, trigger='user', include_sell_review=False,
                                   from_full_directory=True, joint_codes=[code], workers=1,
                                   on_progress=progress)
            except DataIngestBusy:
                # The immutable stage and dual receipts survive contention.
                # Re-enter through validated resume, never repeat a model pair.
                resume_directory = job.get('run_directory') or resume_directory
                progress('publication_wait' if 'counteraudit_done' in job.get('completed_steps', ()) else 'sources')
                time.sleep(2)
                continue
            if result.get('status') != 'busy':
                break
            job.update(status='queued', stage='queued', queue_reason=result.get('error'))
            save(path, job)
            time.sleep(2)
        # Publication, not process success, is the authority for the score.
        sys.path.insert(0, str(ROOT))
        from deep_cross_validation import load_context
        context = load_context(code)
        if valid_score(context):
            save(path, {**job, 'status': 'complete', 'stage': 'published', 'completed_steps': list(STEPS), 'score': context['card']['total'],
                        'finished_at': time.time()})
        else:
            details = result.get('error') or '审核未形成完整分数；请查看证据缺口后重新分析。'
            reasons = [str(r['reason']) for r in result.get('refresh_results', [])
                       if r.get('status') == 'blocked' and r.get('reason')]
            if reasons:
                details += '：' + '；'.join(dict.fromkeys(reasons))
            save(path, {**job, 'status': 'failed', 'error': details,
                        'result': result, 'finished_at': time.time()})
    except Exception as exc:
        save(path, {**job, 'status': 'failed', 'error': str(exc), 'finished_at': time.time()})
        raise
    finally:
        stop.set()
        pulse.join(timeout=6)
        queue_path.unlink(missing_ok=True)


def render(st, code):
    """Auto-start once; poll only the small score panel, not the whole page."""
    from deep_cross_validation import load_context
    from deep_analysis_data import report_html

    state_key = 'deep_3a_job_' + code
    requested_key = 'deep_3a_requested_' + code
    initial_context = load_context(code)
    first = not st.session_state.get(requested_key)
    st.session_state[requested_key] = True
    initial_job = poll_job(code, initial_context,
                           previous=st.session_state.get(state_key), start=first)
    st.session_state[state_key] = initial_job
    interval = poll_interval(initial_job)
    first_panel = True

    @st.fragment(run_every=interval)
    def panel():
        nonlocal first_panel, initial_context
        context = initial_context if first_panel else load_context(code)
        job = initial_job if first_panel else poll_job(code, context, previous=st.session_state.get(state_key))
        first_panel = False
        initial_context = None
        st.session_state[state_key] = job
        context['review_status'] = job['status']
        view = progress_view(job)
        context['review_progress'] = view
        marker = (context.get('row') or {}).get('audit_id')
        key = 'deep_3a_published_' + code
        changed = key in st.session_state and st.session_state[key] != marker
        st.session_state[key] = marker
        # Streamlit sends run_every when decorating on a full run. One rerun
        # changes the timer after completion/retry; idle polling stays local.
        if changed or poll_interval(job) != interval:
            st.rerun()
        if job['status'] in ('queued', 'running'):
            st.progress(view['percent'], text=f"3A分析 {view['percent']}% · {view['label']}")
            st.caption(f"已完成 {view['completed']}/5 项 · 已等待 {view['elapsed']} · 进度按完成步骤计算，非模型生成百分比")
            with st.expander('查看分析步骤', expanded=False):
                done = set(job.get('completed_steps') or ())
                st.markdown('　→　'.join(('✅ ' if s in done else '○ ') + label for s,label in zip(
                    STEPS, ('数据核验', '经典书理', 'GPT主审', '独立反审', '中央发布'))))
                if job.get('queue_reason'):
                    st.caption(job['queue_reason'])
        elif job['status'] == 'complete':
            st.caption('✅ 3A分析 100% · 评分已完成，当前有效结果直接复用')
        elif job['status'] == 'failed':
            st.warning('3A分析未完成：' + str(job.get('error', '需要重新分析')))
            if st.button('重新进行3A分析', key='deep_3a_retry_' + code):
                st.session_state[state_key] = request(code, context, retry=True)
                st.rerun()
        st.markdown(report_html(code, context=context), unsafe_allow_html=True)

    panel()


if __name__ == '__main__':
    from focused_deep_view import normalize_code
    code = normalize_code(sys.argv[1])
    if not code:
        raise SystemExit('invalid code')
    worker(code)
