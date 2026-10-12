"""Wake early, dispatch data refresh, then release the existing deduplicated sender."""
from datetime import datetime,timedelta,timezone
import json,os,time,urllib.request,subprocess,sys
BJT=timezone(timedelta(hours=8))
SLOTS={'morning':(6,30),'afternoon':(12,1),'evening':(19,1)}
CRONS={'30 20 * * *':'morning','1 2 * * *':'afternoon','1 9 * * *':'evening'}
REVIEW_WINDOWS={'morning':((5,40),(6,20)), 'afternoon':((11,30),(11,55)), 'evening':((18,10),(18,50))}

def target_time(now,slot):
    local=now.astimezone(BJT)
    hour,minute=SLOTS[slot]
    return local.replace(hour=hour,minute=minute,second=0,microsecond=0)

def wait_until(target,clock=lambda:datetime.now(timezone.utc),sleep=time.sleep):
    while (remaining:=(target-clock()).total_seconds())>0:
        sleep(min(30,remaining))

def collect_muse(slot):
    script=os.environ.get('MUSE_COLLECTOR_SCRIPT');token=os.environ.get('MUSE_PRIVATE_TOKEN')
    if not script or not token:
        return 'unavailable'
    env={**os.environ,'GH_TOKEN':token,'V88_FEISHU_ALLOW_PUSH':'0','V88_DISABLE_LLM':'1'}
    env.pop('MUSE_PRIVATE_TOKEN',None)
    result=subprocess.run([sys.executable,script,'--collect-only','--slot',slot],env=env,
                          timeout=25,capture_output=True,text=True)
    if result.returncode:return 'collector_failed'
    # Log only a fixed status, never private evidence, credentials or report text.
    status=json.loads(result.stdout).get('status')
    return status if status in {'received','unverified','draft_missing','outside_window'} else 'unknown'

def wait_with_muse(target,slot,collect=collect_muse,clock=lambda:datetime.now(timezone.utc),sleep=time.sleep):
    start,end=REVIEW_WINDOWS[slot]
    opening=target.replace(hour=start[0],minute=start[1],second=0,microsecond=0)
    cutoff=target.replace(hour=end[0],minute=end[1],second=0,microsecond=0)
    next_poll=opening
    while (remaining:=(target-clock()).total_seconds())>0:
        now=clock()
        # Leave time for a bounded request; receipt validation uses actual fetch time.
        if next_poll <= now < cutoff-timedelta(seconds=5):
            try:status=collect(slot)
            except Exception as exc:status=type(exc).__name__
            print(json.dumps({'muse_collection':status,'slot':slot}),flush=True)
            next_poll=clock()+timedelta(seconds=120)
        remaining=(target-clock()).total_seconds()
        if remaining>0:sleep(min(30,remaining))

def main():
    slot=os.environ.get('REPORT_SLOT') or CRONS[os.environ['REPORT_CRON']]
    target=target_time(datetime.now(timezone.utc),slot)
    print(json.dumps({'slot':slot,'target_beijing':target.isoformat(),'mode':'warm_wait'},ensure_ascii=False),flush=True)
    wait_until(target-timedelta(minutes=60))
    token=os.environ['GH_TOKEN'];repo=os.environ['GITHUB_REPOSITORY']
    req=urllib.request.Request(f'https://api.github.com/repos/{repo}/actions/workflows/autonomous.yml/dispatches',
        data=json.dumps({'ref':'main','inputs':{'send':False}}).encode(),method='POST',
        headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json','Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req,timeout=20) as response:print('facts_refresh_dispatched',response.status,flush=True)
    except Exception as exc:print('facts_refresh_dispatch_failed',type(exc).__name__,flush=True)
    wait_with_muse(target,slot)
    print('target_reached_existing_outbox_sender_released',flush=True)

if __name__=='__main__':main()
