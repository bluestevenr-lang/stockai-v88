"""Bounded cloud timer: half-hour dispatch with independent cron recovery."""
from datetime import datetime,timedelta,timezone
import json,os,time,urllib.request,urllib.error
from intraday_schedule import active_markets


def next_tick(now):
    target=now.replace(second=0,microsecond=0)
    while target<=now or target.minute not in (7,37):target+=timedelta(minutes=1)
    return target


def dispatch(workflow,inputs):
    request=urllib.request.Request('https://api.github.com/repos/'+os.environ['GITHUB_REPOSITORY']+'/actions/workflows/'+workflow+'/dispatches',
        data=json.dumps({'ref':'main','inputs':inputs}).encode(),method='POST',
        headers={'Authorization':'Bearer '+os.environ['GH_TOKEN'],'Accept':'application/vnd.github+json'})
    with urllib.request.urlopen(request,timeout=20) as response:
        if response.status!=204:raise RuntimeError('dispatch not accepted')


def main():
    if os.environ.get('PULSE_PRIME')=='true':
        dispatch('autonomous.yml',{'fast':True,'send':False})
        print('initial_refresh_dispatch_accepted',flush=True)
    end=datetime.now(timezone.utc)+timedelta(minutes=325)
    target=next_tick(datetime.now(timezone.utc))
    print('clock_started next_tick='+target.isoformat(),flush=True)
    while target<end:
        while datetime.now(timezone.utc)<target:
            time.sleep(min(30,max(0,(target-datetime.now(timezone.utc)).total_seconds())))
        now=datetime.now(timezone.utc)
        if active_markets(now):
            for attempt in range(3):
                try:dispatch('autonomous.yml',{'fast':True,'send':False});print('accepted_half_hour_refresh',target.isoformat(),flush=True);break
                except (urllib.error.URLError,TimeoutError,RuntimeError) as exc:
                    print('dispatch_failure',type(exc).__name__,flush=True)
                    if attempt==2:raise
                    time.sleep(15)
        else:print('exchange_closed_skip',target.isoformat(),flush=True)
        target=next_tick(datetime.now(timezone.utc))
    # Fresh runner before the hosted job's six-hour execution limit. The shared
    # concurrency group serializes successors and independent recovery cron.
    if datetime.now(timezone.utc).weekday()<5:
        dispatch('intraday_pulse.yml',{});print('successor_requested',flush=True)

if __name__=='__main__':main()
