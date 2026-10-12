from pathlib import Path
import yaml
ROOT=Path(__file__).resolve().parents[1]

def test_draft_restore_has_required_receipt_input_and_cannot_send():
    raw=(ROOT/'.github/workflows/muse_review.yml').read_text()
    doc=yaml.safe_load(raw)
    job=doc['jobs']['exchange']
    sparse=next(s['with']['sparse-checkout'] for s in job['steps'] if s.get('with',{}).get('path')=='state')
    for name in ('/source-bundle.tar.gz','/runtime-bundle.json','/feishu_sync_delivery.json'):
        assert name in sparse.splitlines()
    assert job['env']['V88_FEISHU_ALLOW_PUSH']=='0'
    assert 'FEISHU_WEBHOOK' not in raw and 'upload-artifact' not in raw
    assert doc['concurrency']['group']!='v88-feishu-durable-outbox'


def test_delivery_keeps_original_times_and_feature_flag():
    raw=(ROOT/'.github/workflows/feishu_delivery.yml').read_text()
    assert "'30 22 * * *'" in raw and "'1 4,11 * * *'" in raw
    assert "V88_MUSE_SHADOW: '1'" in raw
