from datetime import datetime
import intraday_schedule as schedule
import importlib.util
from pathlib import Path
spec=importlib.util.spec_from_file_location('preopen_pulse',Path(__file__).parents[1]/'scripts/intraday_pulse.py')
pulse=importlib.util.module_from_spec(spec);spec.loader.exec_module(pulse)

def at(text):return datetime.fromisoformat(text)

def test_seven_am_calendar_and_manual_holiday():
    assert not schedule.preopen_markets(at('2026-10-12T06:59:59+08:00'))
    assert set(schedule.preopen_markets(at('2026-10-12T07:00:00+08:00')))=={'A股','港股','美股'}
    assert not schedule.preopen_markets(at('2026-10-11T07:00:00+08:00'))
    assert schedule.scheduled_due(at('2026-10-12T08:00:00+08:00'),scheduled=True,fast=True,preopen=True)
    assert not schedule.scheduled_due(at('2026-10-11T08:00:00+08:00'),scheduled=True,preopen=True)
    assert schedule.scheduled_due(at('2026-10-11T08:00:00+08:00'),scheduled=False,preopen=True)

def test_clock_wakes_at_seven_and_checks_delayed_runs():
    assert pulse.next_tick(at('2026-10-11T22:59:59+00:00'))==at('2026-10-11T23:00:00+00:00')
    assert pulse.next_tick(at('2026-10-12T06:59:59+08:00'))==at('2026-10-12T07:00:00+08:00')
    now=at('2026-10-12T08:00:00+08:00')
    assert pulse.preopen_needed(now,[])
    assert pulse.preopen_needed(now,[{'created_at':'2026-10-11T22:30:00Z','status':'completed','conclusion':'success'}])
    for state in ['queued','in_progress','waiting']:
        assert not pulse.preopen_needed(now,[{'created_at':'2026-10-11T23:01:00Z','status':state}])
    assert not pulse.preopen_needed(now,[{'created_at':'2026-10-11T23:01:00Z','status':'completed','conclusion':'success'}])
    assert pulse.preopen_needed(now,[{'created_at':'2026-10-11T23:01:00Z','status':'completed','conclusion':'failure'}])
    assert pulse.preopen_needed(now,[{'created_at':'2026-10-11T23:01:00Z','status':'completed','conclusion':'skipped'}])
