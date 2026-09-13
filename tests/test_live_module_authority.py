from datetime import datetime,timezone
import json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'ai-daily-report-v2/src'))
from module_signal_view import legacy_review_text,admitted,admitted_rank,discovery_note,CurrentBuyCodes
from system_diagnostics import inspect_file

NOW=datetime(2026,9,12,tzinfo=timezone.utc)
REF={'reference_id':'current','code':'700.HK','audit_score':80,'tier':'3A','factpack_id':'p','contract_id':'c'}

def test_cached_module_flag_cannot_bypass_current_central_selection():
    r={'code':'00700.HK','push_eligible':True}
    assert not admitted(r,set())
    assert not admitted(r,{'700.HK'})
    assert admitted({**r,'master_ref':REF},CurrentBuyCodes({'700.HK':REF}))
    assert not admitted({**r,'master_ref':{'reference_id':'old'}},CurrentBuyCodes({'700.HK':REF}))
    assert not admitted({**r,'master_ref':REF,'audit_score':99},CurrentBuyCodes({'700.HK':REF}))
    assert not admitted({'code':'700.HK'}, {'700.HK'})


def test_legacy_action_list_cannot_republish_expired_central_grade():
    row={'tier':'3A','listable':True,'formal_recommendation':True}
    assert not admitted_rank(row,'00700.HK',set())
    assert not admitted_rank(row,'00700.HK',{'700.HK'})
    assert admitted_rank({**row,'master_ref':REF},'00700.HK',CurrentBuyCodes({'700.HK':REF}))
    assert not admitted_rank({**row,'formal_recommendation':False},'700.HK',{'700.HK'})

def test_retired_reviewer_cannot_be_relabelled_as_current_approval():
    text=legacy_review_text('GPT+Kimi复核通过')
    assert '旧版审核记录已停用' in text and '重新核验' in text
    assert '复核通过' not in text
    assert legacy_review_text('<script>')=='&lt;script&gt;'

def test_discovery_cannot_echo_unreviewed_enter_now_instruction():
    body=discovery_note('示例','DEMO',lambda n,c:n)
    assert '技术线索' in body and '上方中央列表' in body and '即进' not in body

def test_new_generation_cannot_refresh_old_source(tmp_path):
    p=tmp_path/'example.json'
    p.write_text(json.dumps({'generated_at':NOW.isoformat(),'source_asof':'2026-09-01T15:00:00+08:00','rows':[{'code':'X'}]}))
    r=inspect_file(p,NOW)
    assert r['status']=='记录已过期' and r['clock_kind']=='来源时间' and r['rows']==1

def test_empty_corrupt_future_are_not_healthy(tmp_path):
    p=tmp_path/'example.json'
    for raw,expected in [('{}','空文件'),('{bad','内容损坏'),('[]','结构不符')]:
        p.write_text(raw);assert inspect_file(p,NOW)['status']==expected
    p.write_text(json.dumps({'rows':[],'generated_at':'2026-10-01T10:00:00+08:00'}))
    r=inspect_file(p,NOW);assert r['status']=='未来时间异常' and r['empty']
