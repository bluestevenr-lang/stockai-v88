"""Wake early, dispatch data refresh, then release the existing deduplicated sender."""
from datetime import datetime,timedelta,timezone
import json,os,time,urllib.request
BJT=timezone(timedelta(hours=8))
SLOTS={'morning':(6,30),'afternoon':(12,1),'evening':(19,1)}
CRONS={'30 20 * * *':'morning','1 2 * * *':'afternoon','1 9 * * *':'evening'}

def target_time(now,slot):
    local=now.astimezone(BJT)
    hour,minute=SLOTS[slot]
    return local.replace(hour=hour,minute=minute,second=0,microsecond=0)

def wait_until(target,clock=lambda:datetime.now(timezone.utc),sleep=time.sleep):
    while (remaining:=(target-clock()).total_seconds())>0:
        sleep(min(30,remaining))

def main():
    slot=os.environ.get('REPORT_SLOT') or CRONS[os.environ['REPORT_CRON']]
    target=target_time(datetime.now(timezone.utc),slot)
    print(json.dumps({'slot':slot,'target_beijing':target.isoformat(),'mode':'warm_wait'},ensure_ascii=False),flush=True)
    wait_until(target-timedelta(minutes=20))
    token=os.environ['GH_TOKEN'];repo=os.environ['GITHUB_REPOSITORY']
    req=urllib.request.Request(f'https://api.github.com/repos/{repo}/actions/workflows/autonomous.yml/dispatches',
        data=json.dumps({'ref':'main','inputs':{'send':False}}).encode(),method='POST',
        headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json','Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req,timeout=20) as response:print('facts_refresh_dispatched',response.status,flush=True)
    except Exception as exc:print('facts_refresh_dispatch_failed',type(exc).__name__,flush=True)
    wait_until(target)
    print('target_reached_existing_outbox_sender_released',flush=True)

if __name__=='__main__':main()
