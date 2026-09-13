from copy import deepcopy
from pathlib import Path
import sys

sys.path.insert(1,str(Path.home()/'Desktop/ai-daily-report-v2/src'))
import module_relations_ui as ui
from stock_reference import reference


def fixture(monkeypatch):
    import joint_review_ui
    monkeypatch.setattr(joint_review_ui,'html',lambda *a:'')
    row={'code':'TEST','tier':'1A','audit_score':70,'audit_id':'audit','trade_plan':{}}
    selection={'factpack_id':'pack','generated_at':'central-time','observations':[row]}
    doc={'version':'module-relations-v1','generated_at':'2026-09-01T10:00:00+08:00',
        'factpack_id':'pack','central_generated_at':'central-time','summary':{'conflicts':0},
        'stocks':{'TEST':{'master_ref':reference(selection,row),'evidence_links':[
            {'module':'新闻','source_asof':'2026-09-01','metrics':{'score':88},
             'status':'关联辅助证据','file_status':'STALE'}]}}}
    return doc,selection


def test_reference_equality_is_not_current_audit_or_independent_proof(monkeypatch):
    doc,selection=fixture(monkeypatch)
    before=deepcopy(doc);page=ui.html(doc,selection,'TEST')
    assert '关联快照时间 2026-09-01' in page and '捕获时结果' in page
    assert '不代表当前审核有效或独立验证' in page and '模块原始分=88' in page
    assert 'STALE' in page and '快照引用冲突' in page and '当前引用冲突' not in page
    assert doc==before


def test_duplicate_central_identity_cannot_claim_matching_reference(monkeypatch):
    doc,selection=fixture(monkeypatch)
    selection['pending']=[deepcopy(selection['observations'][0])]
    page=ui.html(doc,selection,'TEST')
    assert '与所载中央分数' not in page and '关联缺失或版本待更新' in page


def test_empty_identity_does_not_match_empty_central(monkeypatch):
    doc,selection=fixture(monkeypatch)
    doc['factpack_id']=selection['factpack_id']=None
    assert '旧版关联' in ui.html(doc,selection,'TEST')


def test_core_shared_copy_matches_desktop():
    assert Path(ui.__file__).read_bytes()==(Path.home()/'Desktop/ai-daily-report-v2/src/module_relations_ui.py').read_bytes()
