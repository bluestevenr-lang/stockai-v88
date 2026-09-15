"""Exercise source-only Windows packages; no live Windows host or release writes."""
from pathlib import Path
import shutil

import pytest

from win.verify_runtime import SCORING_SHARED_FILES, verify_scoring_dependencies


ROOT=Path(__file__).resolve().parents[1]
CORE=ROOT.parent/'ai-daily-report-v2'


@pytest.fixture
def packaged(tmp_path):
    desktop=tmp_path/'StockAI'
    core=tmp_path/'ai-daily-report-v2'
    desktop.mkdir()
    (core/'src').mkdir(parents=True)
    for source,target in ((ROOT,desktop),(CORE/'src',core/'src')):
        for path in source.glob('*.py'):
            shutil.copy2(path,target/path.name)
    return desktop,core


def test_isolated_source_package_imports_both_orders(packaged):
    result=verify_scoring_dependencies(*packaged)
    assert result['isolated_import_orders']==['desktop','core']
    assert result['shared_files']==9
    assert result['network_calls']==0


@pytest.mark.parametrize('side,name',[
    ('desktop','observation_score.py'),('core','horizon_score_policy.py'),
    ('desktop','profit_contract.py'),('core','investment_maturity.py'),
    ('core','market_symbols.py'),
])
def test_missing_dependency_cannot_fall_back_to_developer_checkout(packaged,side,name):
    desktop,core=packaged
    ((desktop if side=='desktop' else core/'src')/name).unlink()
    with pytest.raises(RuntimeError,match='Missing scoring dependency'):
        verify_scoring_dependencies(desktop,core)


def test_changed_scoring_mirror_fails(packaged):
    desktop,core=packaged
    path=desktop/'observation_score.py'
    path.write_bytes(path.read_bytes()+b'\n# stale desktop copy\n')
    with pytest.raises(RuntimeError,match='Scoring mirror version mismatch: observation_score.py'):
        verify_scoring_dependencies(desktop,core)


def test_windows_line_endings_are_not_a_version_difference(packaged):
    desktop,core=packaged
    for name in SCORING_SHARED_FILES:
        path=desktop/name
        path.write_bytes(path.read_bytes().replace(b'\r\n',b'\n').replace(b'\n',b'\r\n'))
    assert verify_scoring_dependencies(desktop,core)['shared_files']==9
