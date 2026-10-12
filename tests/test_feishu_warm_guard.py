import importlib.util
from pathlib import Path
from datetime import datetime,timedelta,timezone
p=Path(__file__).resolve().parents[1]/'scripts/feishu_warm_guard.py'
spec=importlib.util.spec_from_file_location('guard',p);guard=importlib.util.module_from_spec(spec);spec.loader.exec_module(guard)

def test_targets_match_beijing_slots_across_utc_midnight():
    now=datetime.fromisoformat('2026-09-28T20:30:00+00:00')
    assert guard.target_time(now,'morning').isoformat()=='2026-09-29T06:30:00+08:00'
    assert guard.target_time(now,'afternoon').hour==12
    assert guard.target_time(now,'evening').minute==1

def test_guard_waits_without_early_release_and_late_start_catches_up():
    target=datetime.fromisoformat('2026-09-29T06:30:00+08:00')
    current=[target-timedelta(seconds=61)];waits=[]
    def sleep(seconds):waits.append(seconds);current[0]+=timedelta(seconds=seconds)
    guard.wait_until(target,clock=lambda:current[0],sleep=sleep)
    assert current[0]==target and waits==[30,30,1]
    guard.wait_until(target,clock=lambda:target+timedelta(minutes=53),sleep=lambda _:(_ for _ in ()).throw(AssertionError('late start must not wait another day')))


def test_warm_runner_collects_without_cron_and_stops_at_cutoff():
    target=datetime.fromisoformat('2026-10-12T19:01:00+08:00')
    current=[target.replace(hour=18,minute=13)];collected=[]
    def sleep(seconds):current[0]+=timedelta(seconds=seconds)
    guard.wait_with_muse(target,'evening',collect=lambda slot:collected.append((slot,current[0])) or 'received',
                         clock=lambda:current[0],sleep=sleep)
    assert current[0]==target
    assert len(collected)>10
    assert all(slot=='evening' and at<target.replace(hour=18,minute=50) for slot,at in collected)


def test_collector_failure_never_blocks_sender_or_calls_after_cutoff():
    target=datetime.fromisoformat('2026-10-12T12:01:00+08:00')
    current=[target.replace(hour=11,minute=54)];attempts=[]
    def fail(slot):attempts.append(slot);raise TimeoutError('bounded request')
    def sleep(seconds):current[0]+=timedelta(seconds=seconds)
    guard.wait_with_muse(target,'afternoon',collect=fail,clock=lambda:current[0],sleep=sleep)
    assert current[0]==target and attempts==['afternoon']
    guard.wait_with_muse(target,'afternoon',collect=fail,clock=lambda:target+timedelta(seconds=1),sleep=sleep)
    assert attempts==['afternoon']


def test_subprocess_collection_has_timeout_and_no_sending(monkeypatch):
    import types
    monkeypatch.setenv('MUSE_COLLECTOR_SCRIPT','collector.py')
    monkeypatch.setenv('MUSE_PRIVATE_TOKEN','test-private-token')
    def run(command,**kwargs):
        assert command[-3:]==['--collect-only','--slot','evening']
        assert kwargs['timeout']==25 and kwargs['capture_output']
        assert kwargs['env']['V88_FEISHU_ALLOW_PUSH']=='0'
        assert kwargs['env']['GH_TOKEN']=='test-private-token'
        assert 'MUSE_PRIVATE_TOKEN' not in kwargs['env']
        return types.SimpleNamespace(returncode=0,stdout='{"status":"received"}')
    monkeypatch.setattr(guard.subprocess,'run',run)
    assert guard.collect_muse('evening')=='received'


def test_warm_window_wakes_missing_draft_without_scheduled_runner():
    target=datetime.fromisoformat('2026-10-13T06:30:00+08:00')
    now=[target.replace(hour=5,minute=40)];wakes=[]
    def sleep(seconds):now[0]+=timedelta(seconds=seconds)
    guard.wait_with_muse(target,'morning',collect=lambda s:'draft_missing' if now[0].minute<48 else 'received',
        clock=lambda:now[0],sleep=sleep,prepare=lambda s:wakes.append(now[0]) or 'dispatched')
    assert wakes[0].hour==5 and wakes[0].minute==40 and len(wakes)==2
    assert now[0]==target
