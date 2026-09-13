import ast
import copy
from pathlib import Path
import sys
import re
from html import unescape

DESKTOP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DESKTOP))
sys.path.insert(1, str(Path.home() / 'Desktop/ai-daily-report-v2/src'))
from rotation_ui import stock_cycle_html, _cert_mark


def fixture():
    records = {}
    stocks = []
    for code, market, en, zh in [
        ('GRPN', '美股', 'Groupon, Inc.', 'Groupon团购'),
        ('2618.HK', '港股', 'JD Logistics, Inc.', '京东物流'),
        ('6699.HK', '港股', 'ANGELALIGN TECHNOLOGY INC.', '时代天使'),
    ]:
        records[code] = dict(code=code, market=market, name_en=en, name_zh=zh)
        stocks.append(dict(code=code, name=code, phase='低位蓄势', direction='up',
                           headline='观察', up=60, down=40, pos52=30, confidence='中'))
    return {'stocks': stocks}, {'version': 'stock-profiles-v1', 'records': records}


def test_screenshot_missing_names_resolve_in_both_points_and_list_links():
    cycle, profiles = fixture()
    original = copy.deepcopy((cycle, profiles))
    body = stock_cycle_html(cycle, profiles=profiles)
    # Labels must remain correct in the clock/unknown-phase inventory, selector,
    # and folded historical table. Added navigation must not be mistaken for
    # duplicate security identities by counting strings across the entire UI.
    clock = unescape(re.search(r'<div class="rf-clock-main">(.*?)</div>', body, re.S).group(1))
    navigation = unescape(re.search(r'<div class="rf-pick-list">(.*?)</div>', body, re.S).group(1))
    history = unescape(re.search(r'<details class="cy-history">(.*?)</table>', body, re.S).group(1))
    for label, code in [('Groupon, Inc. · Groupon团购（GRPN）', 'GRPN'),
                        ('京东物流（2618.HK）', '2618.HK'), ('时代天使（6699.HK）', '6699.HK')]:
        assert label in clock and label in navigation and label in history
        assert f'?q={code}&focus=deep' in navigation
        assert f'?q={code}&focus=deep#v88-deep-analysis' in history
    assert (cycle, profiles) == original


def test_legacy_receipts_never_become_current_gpt_or_turn_approval():
    cert = {'asof': '2026-07-01', 'by_code': {'GRPN': {'verdict': '一致'}}}
    body = _cert_mark('GRPN', 'Groupon', cert, {})
    assert '旧审核' in body and '2026-07-01' in body
    assert '不是当前有效GPT审核' in body and '>G<' not in body
    rule = _cert_mark('2618.HK', '', {}, {'pass': {'2618.HK': {}}})
    assert '旧规则' in rule and '>R<' not in rule
    assert not _cert_mark('2618.HK', '相同名称', {'by_name': {'相同名称': {'verdict': '一致'}}})
    assert not _cert_mark('2618.HK', '', {'by_code': {'2618': {'verdict': '一致'}}})


def test_cycle_legend_discloses_absent_prediction_receipt():
    cycle, profiles = fixture()
    body = stock_cycle_html(cycle, profiles=profiles)
    assert '本次周期拐点没有独立GPT验证凭据' in body
    assert 'GPT/Codex独立复核通过' not in body


def test_active_app_shared_inline_link_uses_same_label_and_original_symbol(monkeypatch):
    import stock_profile_view
    monkeypatch.setattr(stock_profile_view, 'load', lambda: fixture()[1])
    source = (DESKTOP / 'app_v88_integrated.py').read_text()
    node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == '_stk_link')
    scope = {}
    exec(compile(ast.Module(body=[node], type_ignores=[]), '<stock-link>', 'exec'), scope)
    link = scope['_stk_link'](None, '2618.HK')
    assert '京东物流（2618.HK）' in link
    assert '?q=2618.HK&amp;focus=deep#v88-deep-analysis' in link


def test_action_center_names_are_not_decorated_before_shared_name_resolver():
    tree = ast.parse((DESKTOP / 'app_v88_integrated.py').read_text())
    node = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == '_cb_link9')
    call = next(n.value for n in node.body if isinstance(n, ast.Return))
    assert isinstance(call, ast.Call) and call.func.id == '_nw_link9'
    assert isinstance(call.args[0], ast.Name) and call.args[0].id == '_nm'
