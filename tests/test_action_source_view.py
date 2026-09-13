"""Prevent legacy recommendation HTML from returning through archive drawers."""
from copy import deepcopy
from html.parser import HTMLParser
from pathlib import Path
import ast

import pytest
from action_source_view import html, records


class Page(HTMLParser):
    def __init__(self, body):
        super().__init__()
        self.tags = []
        self.text = []
        self.feed(body)

    def handle_starttag(self, tag, attrs):
        self.tags.append((tag, dict(attrs)))

    def handle_data(self, data):
        self.text.append(data)


@pytest.fixture
def legacy():
    return {'trend_quality.json': {'generated_at': '2026-09-13 13:33', 'rows': [
        {'code': '03396.HK', 'name': '联想控股', 'tier': '1A', 'score': 73.3,
         'trigger_state': '回踩区内·现在分批', 'action': '现在买',
         'entry_pullback': [18.23, 18.96], 'stop': 16.77, 'position': '1批',
         'audit': '<b>GPT全部通过</b>', 'formal_recommendation': True}]}}


def board(code='3396.HK', current='true', css='v88-watch-row'):
    return f'<table><tr id="v88-watch-{code}" class="{css}" data-code="{code}" data-current="{current}"><td>中央</td></tr></table>'


def test_screenshot_legacy_grade_and_instructions_never_reappear(legacy):
    original = deepcopy(legacy)
    body = html(legacy, board(), profiles={})
    for forbidden in ['73.3', '现在买', '现在分批', '18.23', '16.77', '1批', 'GPT全部通过', '1A']:
        assert forbidden not in body
    page = Page(body)
    assert ('a', {'href': '#v88-watch-3396.HK'}) in page.tags
    assert any('q=3396.HK&focus=deep' in attrs.get('href', '') for _, attrs in page.tags)
    assert legacy == original


@pytest.mark.parametrize('central', ['', board(current='false'), board(css='v88-watch-history-row'),
                                    board(css='v88-watch-tracking-row'), board(code='700.HK')])
def test_missing_stale_tracking_or_other_stock_cannot_claim_current_membership(legacy, central):
    body = html(legacy, central, profiles={})
    assert '未列当前重点榜' in body
    assert '定位中央榜行' not in body
    assert '查看中央审核与原合同' in body


def test_source_union_deduplicates_identity_not_independent_review_votes(legacy):
    legacy['value_zone.json'] = {'rows': [{'code': '3396.HK', 'name': '另一名称', 'level': '3A'}]}
    data = records(legacy)
    assert len(data) == 1
    assert len(data[0]['sources']) == 2
    assert set(data[0]) == {'code', 'name', 'sources'}
    body = html(legacy, board(), profiles={})
    assert '技术结构、价值区间' in body and '3A中央重点榜' in body
    assert '独立通过' not in body


def test_auxiliary_focus_rows_cannot_override_the_actual_main_board(legacy):
    extra = board(css='v88-focus-row')
    body = html(legacy, board() + extra, profiles={})
    assert 'href="#v88-watch-3396.HK"' in body
    body = html(legacy, board(current='false') + extra, profiles={})
    assert '未列当前重点榜' in body and '定位中央榜行' not in body


def test_three_markets_always_deep_link_with_original_record_dates():
    docs = {'intraday_decisions.json': {'generated_at': '2026-09-13 10:00', 'rows': [
        {'code': '688002.SH', 'name': '睿创微纳', 'asof': '2026-09-11 15:00'},
        {'code': 'GRPN', 'name': 'Groupon'}, {'code': '0700.HK', 'name': '腾讯控股'}]}}
    body = html(docs, '', profiles={})
    assert '行情 2026-09-11 15:00' in body and '记录 2026-09-13 10:00' in body
    page = Page(body)
    urls = [attrs.get('href', '') for tag, attrs in page.tags if tag == 'a']
    for code in ['688002.SS', 'GRPN', '700.HK']:
        assert any('q=' + code + '&focus=deep' in url for url in urls)
    assert all(market + ' · 1条来源记录' in body for market in ['A股', '港股', '美股'])


def test_malformed_identity_and_html_cannot_create_a_bad_dom_or_order():
    docs = {'trend_shift.json': {'generated_at': '<bad attr>', 'up': [
        {'code': 'BAD"onclick=x', 'name': 'invalid'}, None,
        {'code': 'DEMO', 'name': '<script>alert(1)</script>', 'action': '立即开仓'}]}}
    body = html(docs, '', profiles={})
    page = Page(body)
    assert '<script>' not in body and 'onclick' not in body and '立即开仓' not in body
    assert '&lt;script&gt;' in body and '&lt;bad attr&gt;' in body
    assert len(records(docs)) == 1


def test_old_renderer_is_removed_and_links_consume_the_exact_displayed_board():
    path = Path(__file__).resolve().parents[1] / 'app_v88_integrated.py'
    source = path.read_text()
    tree = ast.parse(source)
    assert not {'_gbadge9', '_row6_9', '_tbl9', '_bsort9'} & {
        node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}
    assert '历史技术观察 ·' not in source and '战术级1A' not in source
    assert 'st.html(_central_board3a)' in source
    assert '{name: _j3a(name) for name in _source_files3a}, _central_board3a)' in source
    assert 'render_tracking(' in source and 'from market_coverage_view import render' in source


def test_out_protection_has_a_real_anchor():
    from grade_card import system_table_html
    body = system_table_html({'rows': []}, {'sell_verification': {'dual_cert': {'candidates': 1, 'passed': 0}}, 'rows': [
        {'code': 'DEMO', 'name': '测试', 'held': True, 'level': '-1A',
         'board_rank': 1, 'on_board': True, 'sell_score': 60, 'px': 10}]}, {}, {})
    assert "id='v88-central-out'" in body
    assert '我的持仓处置' in body and '预承诺止损' in body and 'DEMO' in body
