from copy import deepcopy
from types import SimpleNamespace
import pytest
import deep_review_job as jobs


def scored(score=70):
    return {'card': {'total': score, 'gpt': {'current': True, 'complete': True},
                     'books': {'current': True, 'complete': True}}}


def test_valid_score_reuses_real_review_without_launch(monkeypatch):
    monkeypatch.setattr(jobs.subprocess, 'Popen', lambda *a, **k: pytest.fail('duplicate model run'))
    assert jobs.request('000935.SZ', scored()) == {'status': 'complete', 'score': 70}
    for bad in [None, True, float('nan'), -1, 101]:
        assert not jobs.valid_score(scored(bad))
    stale = scored();stale['card']['gpt']['current'] = False
    assert not jobs.valid_score(stale)


def test_repeated_page_poll_starts_exactly_one_worker(monkeypatch, tmp_path):
    monkeypatch.setattr(jobs, 'JOBS', tmp_path)
    monkeypatch.delenv('V88_DISABLE_LLM', raising=False)
    monkeypatch.delenv('GITHUB_ACTIONS', raising=False)
    calls=[]
    monkeypatch.setattr(jobs.subprocess, 'Popen', lambda *a, **k: (calls.append((a,k)) or SimpleNamespace(pid=123)))
    monkeypatch.setattr(jobs, 'worker_alive', lambda pid,code: pid==123)
    first=jobs.request('000935.SZ', {})
    assert jobs.request('000935.SZ', {}) == first
    assert len(calls)==1
    assert calls[0][0][0][-1]=='000935.SZ'
    assert not calls[0][1].get('shell')


def test_failures_do_not_auto_retry_and_cloud_does_not_call(monkeypatch,tmp_path):
    monkeypatch.setattr(jobs,'JOBS',tmp_path)
    monkeypatch.delenv('GITHUB_ACTIONS',raising=False)
    monkeypatch.delenv('V88_DISABLE_LLM',raising=False)
    monkeypatch.setattr(jobs.subprocess,'Popen',lambda *a,**k:pytest.fail('unexpected retry'))
    jobs.save(tmp_path/'000935.SZ.json', {'status':'failed','error':'source gap','requested_at':jobs.time.time()})
    assert jobs.request('000935.SZ',{})['error']=='source gap'
    monkeypatch.setenv('V88_DISABLE_LLM','1')
    assert jobs.request('300760.SZ',{})['status']=='failed'
    with pytest.raises(ValueError):jobs.request('../bad',{})


def test_progress_is_completed_work_not_elapsed_time():
    job={'status':'running','stage':'counteraudit','requested_at':100,
         'completed_steps':['facts_done','books_done','primary_done']}
    assert jobs.progress_view(job,now=110)['percent']==60
    assert jobs.progress_view(job,now=9000)['percent']==60
    assert jobs.progress_view({**job,'status':'failed'},now=9000)['percent']==60
    assert jobs.progress_view({'status':'queued','requested_at':100},now=9000)['percent']==0
    assert jobs.progress_view({**job,'status':'complete'},now=9000)['percent']==100


def test_progress_deduplicates_and_ignores_unverified_stage_names():
    job={'status':'running','completed_steps':['facts_done','facts_done','publishing','made_up']}
    assert jobs.progress_view(job)['percent']==20


def test_zombie_or_reused_pid_does_not_block_retry(monkeypatch):
    expected=[str(jobs.Path(jobs.__file__).resolve()),'1070.HK']
    monkeypatch.setattr(jobs.psutil,'Process',lambda pid:SimpleNamespace(status=lambda:jobs.psutil.STATUS_ZOMBIE,cmdline=lambda:expected))
    assert not jobs.worker_alive(1,'1070.HK')
    monkeypatch.setattr(jobs.psutil,'Process',lambda pid:SimpleNamespace(status=lambda:'running',cmdline=lambda:['python','other.py','1070.HK']))
    assert not jobs.worker_alive(1,'1070.HK')


def test_publication_contention_resumes_receipts_without_second_review(monkeypatch,tmp_path):
    import json,sqlite3,sys
    from types import ModuleType
    root=tmp_path/'core';data=root/'data';data.mkdir(parents=True)
    with sqlite3.connect(data/'full_market_research.sqlite') as db:
        db.execute('CREATE TABLE active (id INTEGER, snapshot TEXT)')
        db.execute("INSERT INTO active VALUES (1,'same')")
    (data/'value_candidate_facts.json').write_text(json.dumps({'full_index_snapshot':'same'}))
    jobdir=tmp_path/'jobs';jobdir.mkdir();monkeypatch.setattr(jobs,'JOBS',jobdir)
    jobs.save(jobdir/'1070.HK.json',{'code':'1070.HK','status':'queued','requested_at':jobs.time.time()})
    def module(name,**attrs):
        m=ModuleType(name);m.__dict__.update(attrs);monkeypatch.setitem(sys.modules,name,m)
    class Busy(RuntimeError): pass
    run=str(data/'full_market_review_runs'/'saved')
    calls=[]
    def build(*args,**kw):
        calls.append('build')
        for stage in jobs.STEPS[:-1]:kw['on_progress'](stage,run_directory=run)
        raise Busy('collector commit')
    def resume(path,**kw):
        calls.append('resume');assert path==run
        assert jobs.read('1070.HK')['completed_steps']==list(jobs.STEPS[:-1])
        return {'published':True}
    module('v88_paths',core_root=lambda:root)
    module('review_pipeline',build=build)
    module('data_ingest_lock',DataIngestBusy=Busy)
    module('interactive_review_queue',pending=lambda root:[])
    module('joint_review_resume',resumable=lambda root,path:path==run,resume=resume)
    module('deep_cross_validation',load_context=lambda code:scored(65))
    monkeypatch.setattr(jobs.time,'sleep',lambda seconds:None)
    jobs.worker('1070.HK')
    assert calls==['build','resume']
    assert jobs.read('1070.HK')['status']=='complete'
    assert jobs.progress_view(jobs.read('1070.HK'))['percent']==100
    assert not (data/'interactive_review_requests'/'1070.HK.json').exists()
