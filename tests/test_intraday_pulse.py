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
