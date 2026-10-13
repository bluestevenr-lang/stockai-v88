from datetime import datetime, timezone
from unittest.mock import patch
import json
import astra_refresh_control as a


def test_reuses_existing_run_without_duplicate_dispatch():
    calls=[]
    run={'databaseId':123,'status':'in_progress','createdAt':'2026-10-10T00:00:00Z','url':'https://github.com/x'}
    def cli(args):
        calls.append(args)
        return [run]
    assert a.request_run(cli)==(run,True)
    assert len(calls)==1


def test_manual_refresh_is_full_without_sending_or_trading_day_gate():
    calls=[]
    def cli(args):
        calls.append(args)
        return []
    assert a.request_run(cli)==(None,False)
    assert calls[-1]==['workflow','run','autonomous.yml','-R',a.REPO,'-f','fast=false','-f','send=false']


def test_only_one_background_process_and_no_ui_network(tmp_path):
    with patch.object(a.subprocess,'Popen') as spawn,patch.object(a.subprocess,'run',side_effect=AssertionError('UI network')):
        assert a.start(tmp_path)
        assert not a.start(tmp_path)
        spawn.assert_called_once()
    assert a.read(tmp_path)['status']=='requested'


def test_failed_cloud_job_cannot_be_marked_updated(tmp_path):
    data=tmp_path/'data';data.mkdir()
    (data/a.LOCK).write_text('test')
    a.write(tmp_path,{'status':'requested','requested_at':datetime.now(timezone.utc).isoformat()})
    run={'databaseId':123,'createdAt':'2026-10-10T00:00:00Z'}
    with patch.object(a,'request_run',return_value=(run,True)),patch.object(a,'run_cli',return_value={'status':'completed','conclusion':'failure','url':'https://github.com/x'}),patch.object(a.subprocess,'run') as sync:
        a.worker(tmp_path,'test')
        sync.assert_not_called()
    assert a.read(tmp_path)['status']=='failed'
    assert not (data/a.LOCK).exists()


def test_success_requires_fresh_synced_astra_file(tmp_path):
    data=tmp_path/'data';data.mkdir()
    (data/a.LOCK).write_text('test')
    a.write(tmp_path,{'status':'requested','requested_at':'2026-10-10T00:00:00+00:00'})
    (data/'astra_cycle.json').write_text(json.dumps({'generated_at':'2026-10-09T00:00:00+00:00','cycle_id':'2026-10-01'}))
    run={'databaseId':123,'createdAt':'2026-10-10T00:00:00Z'}
    with patch.object(a,'request_run',return_value=(run,True)),patch.object(a,'run_cli',return_value={'status':'completed','conclusion':'success','url':'https://github.com/x'}),patch.object(a.subprocess,'run'):
        a.worker(tmp_path,'test')
    assert a.read(tmp_path)['status']=='failed'
    assert '尚未同步' in a.read(tmp_path)['message']


def test_success_shows_completed_only_after_new_result_is_installed(tmp_path):
    data=tmp_path/'data';data.mkdir()
    (data/a.LOCK).write_text('test')
    a.write(tmp_path,{'status':'requested','requested_at':'2026-10-10T00:00:00+00:00'})
    (data/'astra_cycle.json').write_text(json.dumps({'generated_at':'2026-10-10T00:05:00+00:00','cycle_id':'2026-10-10'}))
    (data/'strategy_board.json').write_text(json.dumps({'generated_at':'2026-10-10T00:04:00+00:00'}))
    (data/'autonomous_status.json').write_text(json.dumps({'generated_at':'2026-10-10T00:06:00+00:00','steps':[{'step':'quotes','ok':True}]}))
    run={'databaseId':123,'createdAt':'2026-10-10T00:00:00Z'}
    with patch.object(a,'request_run',return_value=(run,True)),patch.object(a,'run_cli',return_value={'status':'completed','conclusion':'success','url':'https://github.com/x'}),patch.object(a.subprocess,'run') as sync:
        a.worker(tmp_path,'test')
        sync.assert_called_once()
    assert a.read(tmp_path)['status']=='complete'
    assert a.read(tmp_path)['cycle_id']=='2026-10-10'
    assert not (data/a.LOCK).exists()


def test_header_refresh_fetches_new_quotes_without_sending():
    calls=[]
    def cli(args):
        calls.append(args)
        return []
    assert a.request_run(cli, fast=True)==(None,False)
    assert 'fast=true' in calls[-1] and 'send=false' in calls[-1]


def test_fast_mode_is_persisted_for_background_worker(tmp_path):
    with patch.object(a.subprocess, 'Popen'):
        assert a.start(tmp_path, fast=True)
    assert a.read(tmp_path)['fast'] is True


import pytest
@pytest.mark.parametrize('stale_file', ['strategy_board.json', 'autonomous_status.json', None])
def test_new_astra_is_not_enough_and_partial_sources_remain_visible(tmp_path, stale_file):
    data=tmp_path/'data';data.mkdir()
    (data/a.LOCK).write_text('test')
    a.write(tmp_path,{'status':'requested','fast':True,'requested_at':'2026-10-10T00:00:00+00:00'})
    for name in ['astra_cycle.json','strategy_board.json','autonomous_status.json']:
        (data/name).write_text(json.dumps({'generated_at':'2026-10-09T00:00:00+00:00' if name==stale_file else '2026-10-10T00:05:00+00:00',
             'cycle_id':'2026-10-10','steps':[{'step':'quotes','ok':False}]}))
    run={'databaseId':123,'createdAt':'2026-10-10T00:00:00Z'}
    with patch.object(a,'request_run',return_value=(run,True)),patch.object(a,'run_cli',return_value={'status':'completed','conclusion':'success','url':'https://github.com/x'}),patch.object(a.subprocess,'run'):
        a.worker(tmp_path,'test')
    result=a.read(tmp_path)
    assert result['status']==('failed' if stale_file else 'complete')
    if not stale_file: assert 'quotes' in result['data_warning']


def test_old_failed_manual_run_is_not_current_after_both_modules_refresh(tmp_path):
    import json
    from astra_refresh_control import newer_screening_after_failure
    data=tmp_path/'data';data.mkdir()
    state={'status':'failed','requested_at':'2026-10-11T09:57:29+00:00','message':'old error'}
    (data/'strategy_board.json').write_text(json.dumps({'generated_at':'2026-10-13T16:00:00+00:00'}))
    assert newer_screening_after_failure(tmp_path,state) is None
    (data/'astra_cycle.json').write_text(json.dumps({'generated_at':'2026-10-13T16:01:00+00:00'}))
    assert newer_screening_after_failure(tmp_path,state).isoformat()=='2026-10-14T00:00:00+08:00'
    assert state['status']=='failed' and state['message']=='old error'
    (data/'astra_cycle.json').write_text(json.dumps({'generated_at':'2026-10-13T16:01:00+00:00','refresh_state':'source_unavailable_retained'}))
    assert newer_screening_after_failure(tmp_path,state) is None
