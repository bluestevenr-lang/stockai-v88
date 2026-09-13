"""The shared desktop/cloud/Lite cycle panel distinguishes pending from empty."""
import copy
from pathlib import Path
import sys
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import rotation_ui


def test_pending_cycle_remains_visible_without_claiming_no_opportunities(monkeypatch):
    monkeypatch.setitem(sys.modules, 'stock_profile_view', SimpleNamespace())
    html = rotation_ui.stock_cycle_html({'status': 'pending', 'snapshot_id': 'snapshot'})
    assert 'role="status"' in html
    assert '个股周期扫描待完成' in html
    assert '当前不能据此判断有无观察标的' in html
    assert '暂无候选' not in html and '<svg' not in html
    assert '原行情日期' not in html and '原周期计算' not in html


def test_pending_cycle_keeps_supplied_clocks_and_input_unchanged():
    cycle = {'status': 'pending', 'source_asof': '2026-09-11',
             'analysis_time': '2026-09-12 16:59',
             'stocks': [{'code': 'OLD', 'source_asof': '2026-09-10'}]}
    original = copy.deepcopy(cycle)
    html = rotation_ui.stock_cycle_html(cycle)
    assert '原行情日期 2026-09-11' in html
    assert '原周期计算 2026-09-12 16:59' in html
    assert '即将进入上行周期' not in html
    assert cycle == original


def test_pending_panel_escapes_dates_and_element_id():
    html = rotation_ui.stock_cycle_html(
        {'status': 'pending', 'analysis_time': '<img src=x onerror=alert(1)>'},
        element_id='cycle"><script>alert(1)</script>')
    assert '<img' not in html and '<script>' not in html
    assert '&lt;img' in html


def test_combined_dashboard_shows_pending_even_without_sector_results():
    html = rotation_ui.combined_cycle_dashboard_html({}, {'status': 'pending'})
    assert '个股周期扫描待完成' in html
    assert '个股周期 · 持仓＋自选' in html
    assert 'cc-single' in html


def test_empty_completed_document_retains_existing_empty_render():
    assert rotation_ui.stock_cycle_html({'stocks': []}) == ''
    assert rotation_ui.stock_cycle_html({}) == ''


def test_completed_cycle_retains_its_original_clock_and_stock_link(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, 'home', classmethod(lambda _: tmp_path))
    monkeypatch.setitem(sys.modules, 'stock_profile_view', SimpleNamespace(
        load=lambda: {},
        display_label=lambda name, code, profiles: name,
        link_html=lambda name, code, profiles: f'<a href="/?q={code}&focus=deep">{name}</a>'))
    cycle = {'analysis_time': '2026-09-12 16:59', 'stocks': [
        {'code': 'OLD', 'name': 'Historical stock', 'direction': 'up',
         'source_asof': '2026-09-11', 'pos52': 40, 'up': 65, 'down': 20}]}
    original = copy.deepcopy(cycle)
    html = rotation_ui.stock_cycle_html(cycle, profiles={})
    assert '2026-09-12 16:59' in html and '/?q=OLD&focus=deep' in html
    assert '扫描待完成' not in html and cycle == original
