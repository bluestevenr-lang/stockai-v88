from copy import deepcopy
from datetime import datetime, timedelta
import json
import pytest
import muse_review as m
import muse_display as ui

NOW = datetime.fromisoformat('2026-10-12T12:05:00+08:00')


def fixture(ready=False, module='3A', vote='pass'):
    row = {'code':'002648.SZ','name':'测试','market':'A股','last':27.7,'buyable':ready,'rule_matched':ready,
           'status':'条件匹配' if ready else '研究候选','selection_category':'买入条件已满足' if ready else '重点观察'}
    bound = m.item(row, module)
    review = {'draft_sha256':'abc','criteria_version':m.CRITERIA,'verdict_blob_sha':'verdict-version',
              'items':[bound], 'accepted_items':{bound['item_id']:{'verdict':vote,'reason':'证据已核',
               'checked_at':'2026-10-12T11:50:00+08:00','evidence_ref':'https://example.org/source'}}}
    record = {'delivery_id':'v88-three-slot:2026-10-12:afternoon','at':NOW.isoformat(),
              'complete':True,'accepted':True,'muse_review':review,'payload_parts':['原文1','原文2']}
    cache = {'sync_status':'complete','synced_at':NOW.isoformat(),'records':ui.project({'deliveries':{'one':record}}),
             'latest_drafts':{record['delivery_id']:{'draft_sha256':'abc'}}}
    return row, record, cache


@pytest.mark.parametrize('ready,expected', [(False,'认可重点观察（不代表可买）'),(True,'认可买入条件')])
def test_bound_badge_meaning_and_metadata(ready,expected):
    row,record,cache=fixture(ready)
    result=ui.current(row,'3A',cache,NOW)
    assert result['state']=='pass' and result['meaning']==expected
    markup=ui.badge(result)
    for token in ['<b>M</b>',expected,m.CRITERIA,'11:50:00','verdict-vers','https://example.org/source','abc']:
        assert token in markup


@pytest.mark.parametrize('mutation', ['price','state','draft','expired','partial','removed','new_report','yesterday'])
def test_old_green_cannot_survive_changes(mutation):
    row,record,cache=fixture()
    if mutation=='price':row['last']=28
    if mutation=='state':row['selection_category']='买入条件已满足'
    if mutation=='draft':cache['latest_drafts'][record['delivery_id']]['draft_sha256']='new'
    if mutation=='expired':cache['synced_at']=(NOW-timedelta(minutes=6)).isoformat()
    if mutation=='partial':cache['sync_status']='partial'
    if mutation=='removed':cache['records'][0]['muse_review']['items']=[]
    if mutation=='new_report':cache['records'].append(dict(cache['records'][0],muse_review={},delivery_id='recovery'))
    if mutation=='yesterday':cache['records'][0]['date']='2026-10-11'
    assert ui.current(row,'3A',cache,NOW)['state']=='unverified'


def test_astra_and_fail_do_not_change_score_or_state():
    row,record,cache=fixture(module='Astra',vote='fail');old=deepcopy(row)
    assert ui.current(row,'Astra',cache,NOW)['state']=='fail'
    assert row==old


def test_history_frozen_when_today_changes_and_debug_recovery_excluded():
    row,record,cache=fixture();review=record['muse_review'];old=deepcopy(review)
    recovery=dict(record,delivery_id='v88-manual-resend:recovery-afternoon',supersedes=record['delivery_id'])
    debug=dict(record,delivery_id='v88-manual-resend:debug-x')
    failed=dict(record,delivery_id='failed',complete=False)
    projected=ui.project({'deliveries':{'o':record,'r':recovery,'d':debug,'f':failed}})
    assert len(projected)==3 and len(ui.effective(projected))==1
    cache['records']=projected;cache['sync_status']='failed'
    markup=ui.panel(cache,NOW)
    assert '测试记录（不计汇总）' in markup and '已被替代' in markup and 'supersedes' in markup
    assert '✅ Muse 认证·通过' in markup and '原文1\n\n原文2' in markup
    assert review==old
    assert ui.current(row,'3A',cache,NOW)['state']=='unverified'


def test_unknown_old_receipt_does_not_fabricate_verification():
    row,record,cache=fixture();record.pop('muse_review')
    cache['records']=ui.project({'deliveries':{'x':record}})
    assert '旧回执无认证字段' in ui.panel(cache,NOW)
    assert ui.current(row,'3A',cache,NOW)['state']=='unverified'


def test_untrusted_text_html_escaped():
    row,record,cache=fixture()
    v=next(iter(record['muse_review']['accepted_items'].values()))
    v.update(reason='<script>bad()</script>',evidence_ref='javascript:bad()<img>')
    text=ui.panel(cache,NOW)
    assert '<script>' not in text and '<img>' not in text and '&lt;script&gt;' in text


def test_daily_history_filter():
    row,record,cache=fixture()
    assert '原文1' in ui.panel(cache,NOW,day='2026-10-12')
    assert '原文1' not in ui.panel(cache,NOW,day='2026-10-11')


def test_fixed_feishu_copy_and_metadata():
    row,_,_=fixture(ready=True)
    bound=dict(m.item(row,'3A'),insert_after=1)
    draft={'items':[bound],'text':'report','delivery_id':'d','draft_sha256':'h','criteria_version':m.CRITERIA,
           'slot':'afternoon','cutoff_at':'2026-10-12T11:55:00+08:00'}
    text,audit=m.merge(draft,{bound['item_id']:{'verdict':'pass','reason':'通过','checked_at':NOW.isoformat(),'evidence_ref':'e'}},verdict_sha='v')
    assert 'Muse 验证（试运行）：✅ 认可买入条件｜通过' in text
    assert '仅针对本版草案，不代表收益保证' in text
    assert audit['slot']=='afternoon' and audit['cutoff_at']==draft['cutoff_at']
    text,_=m.merge(draft,{bound['item_id']:{'verdict':'fail','reason':'治理未出清'}})
    assert '❌ 不认可｜治理未出清' in text


def test_background_sync_preserves_history_on_failure(tmp_path):
    from muse_web_sync import sync
    row,record,cache=fixture()
    def fetch(repo,path,branch,deadline,optional=False):
        if path=='feishu_sync_delivery.json':return {'deliveries':{'r':record}}
        if path.endswith('latest.json'):return {'delivery_id':record['delivery_id'],'draft_sha256':'abc'}
        return None
    assert sync(tmp_path,NOW,fetch)['status']=='complete'
    archive=list((tmp_path/'data/muse_receipt_archive').glob('*.json'));assert len(archive)==1
    def fail(*a,**k):raise RuntimeError('offline')
    assert sync(tmp_path,NOW,fail)['status']=='failed'
    retained=json.loads((tmp_path/'data/muse_review_web.json').read_text())
    assert len(retained['records'])==1 and archive[0].exists()
    assert ui.current(row,'3A',retained,NOW)['state']=='unverified'


def test_legacy_success_receipts_without_id_are_kept():
    rows=ui.project({'deliveries':{'oldsha':{'at':NOW.isoformat(),'complete':True,'accepted':True,'parts':[]}}})
    assert rows[0]['delivery_id']=='legacy-receipt:oldsha'
    assert rows[0]['slot']=='unknown' and ui.counts(rows[0])=={'pass':0,'fail':0,'unverified':0}


def test_current_missing_vote_does_not_reuse_earlier_pass():
    row,record,cache=fixture()
    copy=deepcopy(cache['records'][0]);copy['muse_review']['accepted_items']={}
    copy.update(delivery_id='v88-manual-resend:recovery-afternoon',supersedes=record['delivery_id'])
    cache['records'].append(copy);cache['latest_drafts'][copy['delivery_id']]={'draft_sha256':'abc'}
    assert ui.current(row,'3A',cache,NOW)['state']=='unverified'


def test_verdict_snapshot_never_overrides_receipt():
    row,record,cache=fixture(vote='fail')
    cache['verdict_snapshots']={record['delivery_id']:{'present':True,'snapshot':{'verdict':'pass'}}}
    assert ui.current(row,'3A',cache,NOW)['state']=='fail'


def test_sync_does_not_rewrite_a_frozen_success(tmp_path):
    from muse_web_sync import sync
    row,record,cache=fixture()
    def fetch(repo,path,branch,deadline,optional=False):
        if path=='feishu_sync_delivery.json':return {'deliveries':{'r':record}}
        return None
    sync(tmp_path,NOW,fetch)
    record['muse_review']['accepted_items']={}
    assert sync(tmp_path,NOW,fetch)['status']=='partial'
    cache=json.loads((tmp_path/'data/muse_review_web.json').read_text())
    assert cache['receipt_conflicts']==[record['delivery_id']]
    assert ui.result(cache['records'][0]['muse_review'],cache['records'][0]['muse_review']['items'][0])['state']=='pass'
    assert len(list((tmp_path/'data/muse_receipt_archive').glob('*.json')))==2


def test_missing_vote_explains_no_submission_instead_of_draft_change():
    row,record,cache=fixture()
    cache['records'][0]['muse_review']['accepted_items']={}
    cache['latest_drafts'][record['delivery_id']]['draft_sha256']='changed'
    cache['verdict_snapshots']={record['delivery_id']:{'present':False,'snapshot':None}}
    answer=ui.current(row,'3A',cache,NOW)
    assert answer['state']=='unverified' and '尚未收到本时段' in answer['reason']
    cache['verdict_snapshots'][record['delivery_id']]={'present':True,'snapshot':{'items':[
        {'item_id':m.item(row,'3A')['item_id'],'code':row['code'],'verdict':'pass','reason':'认可观察'}]}}
    answer=ui.current(row,'3A',cache,NOW)
    assert answer['state']=='unverified' and '本次投递未采用' in answer['reason']
    markup=ui.panel(cache,NOW)
    assert '非当前认证' in markup and '认可观察' in markup
    assert '✅ Muse 认证·通过' not in markup
    assert cache['records'][0]['muse_review']['accepted_items']=={}
