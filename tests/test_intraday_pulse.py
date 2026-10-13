from datetime import datetime
import importlib.util
from pathlib import Path
spec=importlib.util.spec_from_file_location('pulse',Path(__file__).parents[1]/'scripts/intraday_pulse.py')
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)


def test_tick_boundaries_and_next_day():
    for source,expected in [('2026-09-29T11:06:59+08:00','2026-09-29T11:07:00+08:00'),
      ('2026-09-29T11:07:00+08:00','2026-09-29T11:37:00+08:00'),
      ('2026-09-29T23:58:00+08:00','2026-09-30T00:07:00+08:00')]:
        assert p.next_tick(datetime.fromisoformat(source)).isoformat()==expected


def test_dispatch_is_refresh_only_and_uses_action_permission(monkeypatch):
    import json
    seen=[]
    monkeypatch.setenv('GITHUB_REPOSITORY','owner/repo')
    monkeypatch.setenv('GH_TOKEN','test-token')
    class Response:
        status=204
        def __enter__(self):return self
        def __exit__(self,*args):pass
    def accept(request,timeout):
        seen.append(request)
        return Response()
    monkeypatch.setattr(p.urllib.request,'urlopen',accept)
    p.dispatch('autonomous.yml',{'fast':True,'send':False})
    assert json.loads(seen[0].data)=={'ref':'main','inputs':{'fast':True,'send':False}}
    assert seen[0].full_url.endswith('/autonomous.yml/dispatches')


def test_successor_retries_transient_errors_and_stops_after_acceptance(capsys):
    calls=[];waits=[]
    def dispatch(workflow,inputs):
        calls.append((workflow,inputs))
        if len(calls)==1:raise TimeoutError('private credential must not appear')
    assert p.request_successor(dispatcher=dispatch,sleep=waits.append) is True
    assert calls==[('intraday_pulse.yml',{}),('intraday_pulse.yml',{})]
    assert waits==[15]
    output=capsys.readouterr().out
    assert 'successor_requested' in output and 'TimeoutError' in output
    assert 'private credential' not in output


def test_successor_exhaustion_is_bounded_and_never_claims_success(capsys):
    calls=[];waits=[]
    def dispatch(workflow,inputs):
        calls.append((workflow,inputs))
        raise p.urllib.error.URLError('private endpoint')
    assert p.request_successor(dispatcher=dispatch,sleep=waits.append) is False
    assert calls==[('intraday_pulse.yml',{})]*3
    assert waits==[15,30]
    output=capsys.readouterr().out
    assert 'successor_trigger_unconfirmed' in output
    assert 'successor_requested' not in output and 'private endpoint' not in output


def test_latest_review_poll_is_bounded_and_never_calls_model_or_sender(monkeypatch):
    import sys
    from types import SimpleNamespace
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1]/'scripts'))
    monkeypatch.setenv('MUSE_COLLECTOR_SCRIPT','core/src/muse_exchange.py')
    monkeypatch.setenv('MUSE_PRIVATE_TOKEN','not-logged')
    calls=[]
    def run(args,**kwargs):
        calls.append(args)
        assert args[-1]=='core/src/muse_screening_review.py'
        assert kwargs['env']['V88_FEISHU_ALLOW_PUSH']=='0'
        assert kwargs['env']['V88_DISABLE_LLM']=='1' and kwargs['timeout']==25
        return SimpleNamespace(returncode=0,stdout='{"status":"not_found"}')
    monkeypatch.setattr(p.subprocess,'run',run)
    state={}
    p.screening_review_tick(datetime.fromisoformat('2026-10-14T06:18:40+08:00'),state)
    assert calls==[] # Formal deadline's last 90 seconds are protected.
    p.screening_review_tick(datetime.fromisoformat('2026-10-14T06:25:00+08:00'),state)
    p.screening_review_tick(datetime.fromisoformat('2026-10-14T06:25:30+08:00'),state)
    assert len(calls)==1
    p.screening_review_tick(datetime.fromisoformat('2026-10-14T06:27:00+08:00'),state)
    assert len(calls)==2
