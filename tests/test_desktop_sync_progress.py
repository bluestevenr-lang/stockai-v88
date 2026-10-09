import importlib.util
import json
import os
from datetime import datetime,timezone
from pathlib import Path
from unittest.mock import patch

from autonomous_desktop import schedule_sync
from module_freshness import record,html

def test_refresh_clocks_preserve_source_and_disk(tmp_path):
    path=tmp_path/'strategy_board.json'
    doc={'generated_at':'2026-10-09T01:00:00+00:00','screening_finished_at':'2026-10-09T02:00:00+00:00'}
    path.write_text(json.dumps(doc));os.utime(path,(1791513000,1791513000))
    before=path.stat().st_mtime
    now=datetime(2026,10,9,4,tzinfo=timezone.utc)
    r=record(path.name,base=tmp_path,now=now)
    assert r['generated']=='10-09 10:00:00'
    assert r['age']=='2小时0分钟'
    assert path.stat().st_mtime==before
    assert '缓存写入' in html(path.name,base=tmp_path,now=now)
    assert json.loads(path.read_text())==doc

def test_ui_only_launches_one_background_worker(tmp_path):
    (tmp_path/'src').mkdir();(tmp_path/'data').mkdir()
    (tmp_path/'src/autonomous_sync.py').touch()
    with patch('autonomous_desktop.core_root',return_value=tmp_path),patch('autonomous_desktop.subprocess.Popen') as spawn,patch('autonomous_desktop.subprocess.run',side_effect=AssertionError('UI blocked')):
        assert schedule_sync() is True
        assert schedule_sync() is False
        spawn.assert_called_once()
        assert '--worker' in spawn.call_args.args[0]

def test_recent_attempt_throttles_without_leaking_lock(tmp_path):
    (tmp_path/'src').mkdir();(tmp_path/'data').mkdir()
    (tmp_path/'src/autonomous_sync.py').touch()
    (tmp_path/'data/.autonomous_sync_attempt').touch()
    with patch('autonomous_desktop.core_root',return_value=tmp_path),patch('autonomous_desktop.subprocess.Popen') as spawn:
        assert not schedule_sync()
        assert not (tmp_path/'data/.autonomous_sync_ui_lock').exists()
        spawn.assert_not_called()
