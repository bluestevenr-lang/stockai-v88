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
