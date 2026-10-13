"""Wake early, dispatch data refresh, then release the existing deduplicated sender."""
from datetime import datetime,timedelta,timezone
import json,os,time,urllib.request,subprocess,sys
BJT=timezone(timedelta(hours=8))
SLOTS={'morning':(6,30),'afternoon':(12,1),'evening':(19,1)}
CRONS={'30 20 * * *':'morning','1 2 * * *':'afternoon','1 9 * * *':'evening'}
REVIEW_WINDOWS={'morning':((5,40),(6,20)), 'afternoon':((11,30),(11,55)), 'evening':((18,10),(18,50))}
REVIEW_COLLECT_TIMEOUT=25

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
                          timeout=REVIEW_COLLECT_TIMEOUT,capture_output=True,text=True)
    if result.returncode:return 'collector_failed'
    # Log only a fixed status, never private evidence, credentials or report text.
    output=json.loads(result.stdout);status=output.get('status')
    if status=='unverified':
        reason=output.get('reason')
        return 'unverified:'+reason if reason in {'not_found','invalid','late','unavailable'} else 'unverified'
    return status if status in {'received','unverified','draft_missing','outside_window'} else 'unknown'

def prepare_muse(slot):
    token=os.environ.get('GH_TOKEN');repo=os.environ.get('GITHUB_REPOSITORY')
    if not token or not repo:return 'unavailable'
    req=urllib.request.Request(f'https://api.github.com/repos/{repo}/actions/workflows/muse_review.yml/dispatches',
        data=json.dumps({'ref':'main','inputs':{'slot':slot}}).encode(),method='POST',
        headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json','Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=10):return 'dispatched'

def review_tick(now,state,collect=collect_muse,prepare=prepare_muse):
    """One bounded review poll for an already-awake cloud runner; never re-draft."""
    local=now.astimezone(BJT)
    for slot,(start,end) in REVIEW_WINDOWS.items():
        opening=local.replace(hour=start[0],minute=start[1],second=0,microsecond=0)
        cutoff=local.replace(hour=end[0],minute=end[1],second=0,microsecond=0)
        # Leave the collector's full budget before cutoff. Its own receive clock
        # remains authoritative; polling earlier never backdates a late verdict.
        if not opening <= local <= cutoff-timedelta(seconds=REVIEW_COLLECT_TIMEOUT):continue
        identity=(str(local.date()),slot)
        entry=state.setdefault(identity,{})
        if local < entry.get('poll',opening):return
        final_window=cutoff-timedelta(seconds=90)
        # Clamp the ordinary poll to the final window, even if the preceding
        # two-minute interval would otherwise skip most of that window.
        entry['poll']=(local+timedelta(seconds=30) if local>=final_window
                       else min(local+timedelta(seconds=120),final_window))
        try:status=collect(slot)
        except Exception as exc:status=type(exc).__name__
        print(json.dumps({'muse_collection':status,'slot':slot}),flush=True)
        # Existing draft is the review contract. Repeated wakeups must not replace it.
        if status=='draft_missing' and local>=entry.get('prepare',opening):
            entry['prepare']=local+timedelta(minutes=5)
            try:prepared=prepare(slot)
            except Exception as exc:prepared=type(exc).__name__
            print(json.dumps({'muse_draft_wakeup':prepared,'slot':slot}),flush=True)
        return status

def wait_with_muse(target,slot,collect=collect_muse,clock=lambda:datetime.now(timezone.utc),sleep=time.sleep,prepare=prepare_muse):
    state={}
    while (remaining:=(target-clock()).total_seconds())>0:
        review_tick(clock(),state,collect,prepare)
        remaining=(target-clock()).total_seconds()
        if remaining>0:sleep(min(30,remaining))

def warm_target(now,slot):
    target=target_time(now,slot)
    # A delayed previous-day cron must not wait 16 hours for today's same slot.
    # Independent reconciliation handles overdue sends; this worker is only prewarming.
    if not target-timedelta(hours=2,minutes=5)<=now<=target+timedelta(minutes=90):return None
    return target

def delivery_output(value):
    if os.environ.get('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'],'a') as out:out.write('deliver='+str(value).lower()+'\n')

def main():
    slot=os.environ.get('REPORT_SLOT') or CRONS[os.environ['REPORT_CRON']]
    target=warm_target(datetime.now(timezone.utc),slot)
    delivery_output(False)
    if target is None:
        print(json.dumps({'slot':slot,'status':'outside_warm_window'}),flush=True)
        return
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
    delivery_output(True)
    print('target_reached_existing_outbox_sender_released',flush=True)

if __name__=='__main__':main()
