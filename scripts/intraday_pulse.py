"""Bounded cloud timer: half-hour dispatch with independent cron recovery."""
from datetime import datetime,timedelta,timezone
import json,os,time,urllib.request,urllib.error
from intraday_schedule import active_markets, preopen_markets


def next_tick(now):
    target=now.replace(second=0,microsecond=0)
    while target<=now or not (target.minute in (7,37) or (target.astimezone(timezone.utc).hour==23 and target.minute==0)):target+=timedelta(minutes=1)
    return target


def dispatch(workflow,inputs):
    request=urllib.request.Request('https://api.github.com/repos/'+os.environ['GITHUB_REPOSITORY']+'/actions/workflows/'+workflow+'/dispatches',
        data=json.dumps({'ref':'main','inputs':inputs}).encode(),method='POST',
        headers={'Authorization':'Bearer '+os.environ['GH_TOKEN'],'Accept':'application/vnd.github+json'})
    with urllib.request.urlopen(request,timeout=20) as response:
        if response.status!=204:raise RuntimeError('dispatch not accepted')


def preopen_needed(now,runs):
    """Any successful/running facts pass begun after today's 07:00 covers it.

    Failed/skipped jobs do not satisfy the checkpoint. GitHub's independent
    cron and this already-awake timer can recover each other without Feishu.
    """
    if not preopen_markets(now):return False
    bjt=timezone(timedelta(hours=8))
    since=now.astimezone(bjt).replace(hour=7,minute=0,second=0,microsecond=0)
    for run in runs:
        at=datetime.fromisoformat(run['created_at'].replace('Z','+00:00'))
        if at>=since and (run.get('status') in ('queued','in_progress','waiting','pending') or (run.get('conclusion')=='success' and run.get('data_refreshed') is True)):
            return False
    return True


def preopen_catchup(now):
    if not preopen_markets(now):return False
    url='https://api.github.com/repos/'+os.environ['GITHUB_REPOSITORY']+'/actions/workflows/autonomous.yml/runs?per_page=30'
    req=urllib.request.Request(url,headers={'Authorization':'Bearer '+os.environ['GH_TOKEN'],'Accept':'application/vnd.github+json'})
    with urllib.request.urlopen(req,timeout=20) as response:runs=json.load(response)['workflow_runs']
    since=now.astimezone(timezone(timedelta(hours=8))).replace(hour=7,minute=0,second=0,microsecond=0)
    for run in runs:
        if run.get('conclusion')!='success' or datetime.fromisoformat(run['created_at'].replace('Z','+00:00'))<since:continue
        jobs_req=urllib.request.Request(run['jobs_url'],headers={'Authorization':'Bearer '+os.environ['GH_TOKEN'],'Accept':'application/vnd.github+json'})
        with urllib.request.urlopen(jobs_req,timeout=20) as response:jobs=json.load(response)['jobs']
        run['data_refreshed']=any(step.get('name')=='Validate the replacement rule digest without sending' and step.get('conclusion')=='success'
                                  for job in jobs for step in job.get('steps',[]))
        if run['data_refreshed']:break
    if not preopen_needed(now,runs):return False
    dispatch('autonomous.yml',{'fast':True,'send':False})
    print('accepted_0700_refresh_catchup',now.isoformat(),flush=True)
    return True


def main():
    from feishu_warm_guard import review_tick
    review_state={}
    review_tick(datetime.now(timezone.utc),review_state)
    if os.environ.get('PULSE_PRIME')=='true':
        dispatch('autonomous.yml',{'fast':True,'send':False})
        print('initial_refresh_dispatch_accepted',flush=True)
    try:preopen_catchup(datetime.now(timezone.utc))
    except (urllib.error.URLError,TimeoutError,RuntimeError) as exc:print('preopen_check_failure',type(exc).__name__,flush=True)
    end=datetime.now(timezone.utc)+timedelta(minutes=325)
    target=next_tick(datetime.now(timezone.utc))
    print('clock_started next_tick='+target.isoformat(),flush=True)
    while target<end:
        while datetime.now(timezone.utc)<target:
            review_tick(datetime.now(timezone.utc),review_state)
            time.sleep(min(30,max(0,(target-datetime.now(timezone.utc)).total_seconds())))
        now=datetime.now(timezone.utc)
        try:recovered=preopen_catchup(now)
        except (urllib.error.URLError,TimeoutError,RuntimeError) as exc:
            recovered=False;print('preopen_check_failure',type(exc).__name__,flush=True)
        if not recovered and active_markets(now):
            for attempt in range(3):
                try:dispatch('autonomous.yml',{'fast':True,'send':False});print('accepted_half_hour_refresh',target.isoformat(),flush=True);break
                except (urllib.error.URLError,TimeoutError,RuntimeError) as exc:
                    print('dispatch_failure',type(exc).__name__,flush=True)
                    if attempt==2:
                        print('refresh_trigger_unconfirmed; retain_clock_for_next_tick',flush=True)
                        break
                    time.sleep(15)
        else:print('exchange_closed_skip',target.isoformat(),flush=True)
        target=next_tick(datetime.now(timezone.utc))
    # Fresh runner before the hosted job's six-hour execution limit. The shared
    # concurrency group serializes successors and independent recovery cron.
    # Reports/reviews also run on weekends. Market refresh remains calendar-gated.
    dispatch('intraday_pulse.yml',{});print('successor_requested',flush=True)

if __name__=='__main__':main()
