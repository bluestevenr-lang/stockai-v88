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


def test_review_has_independent_wakeups_and_warm_runner_backup():
    raw=(ROOT/'.github/workflows/muse_review.yml').read_text()
    assert 'workflow_run:' in raw and 'Check window before dependencies' in raw
    warm=(ROOT/'.github/workflows/feishu_warm_guard.yml').read_text()
    assert 'MUSE_COLLECTOR_SCRIPT:' in warm and 'MUSE_PRIVATE_TOKEN:' in warm
    assert 'continue-on-error: true' in warm
    assert 'FEISHU_WEBHOOK' not in warm


def test_existing_cloud_clock_collects_reviews_without_sending_credentials():
    doc=yaml.safe_load((ROOT/'.github/workflows/intraday_pulse.yml').read_text())
    job=doc['jobs']['clock']
    assert job['timeout-minutes']<360
    env=job['steps'][-1]['env']
    assert env['MUSE_COLLECTOR_SCRIPT']=='core/src/muse_exchange.py'
    assert 'MUSE_PRIVATE_TOKEN' in env and 'FEISHU_WEBHOOK' not in env
    script=(ROOT/'scripts/intraday_pulse.py').read_text()
    assert 'review_tick(datetime.now(timezone.utc),review_state)' in script
    warm=yaml.safe_load((ROOT/'.github/workflows/feishu_warm_guard.yml').read_text())
    assert warm['jobs']['delivery']['if']=="needs.ready.outputs.deliver == 'true'"
