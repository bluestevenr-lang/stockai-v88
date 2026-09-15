"""Financial prose must never become a tag or attribute on a grade board."""
from copy import deepcopy
from html.parser import HTMLParser

import pytest

import grade_card


PROSE = '价格<value<floor> & "严格"；保留 <目标> 与 A&B 的原意'


class Parsed(HTMLParser):
    def __init__(self, html):
        super().__init__(convert_charrefs=True)
        self.nodes = []
        self.texts = []
        self.feed(html)

    def handle_starttag(self, name, attrs):
        self.nodes.append((name, dict(attrs)))

    def handle_data(self, text):
        self.texts.append(text)


def assert_plain_prose(html, expected=PROSE):
    parsed = Parsed(html)
    known = {'a', 'b', 'br', 'details', 'div', 'img', 'p', 'section', 'small',
             'span', 'style', 'summary', 'table', 'tbody', 'td', 'th', 'thead', 'tr'}
    assert {name for name, _ in parsed.nodes} <= known
    assert all(not any(c in name for c in '<>"\'=/ \n')
               for _, attrs in parsed.nodes for name in attrs)
    assert all(not name.lower().startswith('on') for _, attrs in parsed.nodes for name in attrs)
    assert all(attrs.get('src','').startswith('data:image/svg+xml;base64,')
               for tag, attrs in parsed.nodes if tag=='img')
    assert expected in ''.join(parsed.texts) or any(
        expected in str(value) for _, attrs in parsed.nodes for value in attrs.values())
    return parsed


@pytest.fixture(autouse=True)
def local_only(monkeypatch):
    import scan_progress_view
    import market_adaptation_ui, market_watch_ui
    monkeypatch.setattr(scan_progress_view, 'html', lambda *args, **kwargs: '')
    # These independent panels have their own HTML/identity tests. Live
    # headings and market badge images are not injected financial prose.
    monkeypatch.setattr(market_adaptation_ui, 'html', lambda *args, **kwargs: '')
    monkeypatch.setattr(market_watch_ui, 'html', lambda *args, **kwargs: '')
    monkeypatch.setattr(grade_card, '_book_health', lambda: {})
    monkeypatch.setattr(grade_card, 'profile_name', lambda name, code: name)
    monkeypatch.setattr(grade_card, 'profile_html', lambda code: '')


@pytest.mark.parametrize('note', [PROSE, '复核要求 " > <b"bad> 必须核对', 'P/E<20；量比>1.2'])
def test_badge_title_round_trips_without_creating_attributes_or_tags(note):
    parsed = Parsed(grade_card.cg_badge('G', 'reject', note))
    assert len(parsed.nodes) == 1
    tag, attrs = parsed.nodes[0]
    assert tag == 'span'
    assert set(attrs) == {'style', 'title'}
    assert attrs['title'] == note
    assert ''.join(parsed.texts) == 'G⚠️'


def test_html_table_headers_are_text_but_cell_helpers_remain_html():
    result = grade_card._tbl('<tr><td><b>原合同</b><details><summary>证据</summary>完整</details></td></tr>', [PROSE])
    parsed = assert_plain_prose(result)
    assert any(tag == 'td' and attrs.get('data-label') == PROSE for tag, attrs in parsed.nodes)
    assert {'b', 'details', 'summary'} <= {tag for tag, _ in parsed.nodes}
    assert '&lt;b&gt;' not in result


def legacy_row():
    return {'code': 'TEST', 'name': '测试股', 'tier': '1A', 'display_tier': '1A候选',
            'listable': False, 'action_state': '不可执行', 'buckets': {},
            'central_trade_plan': {'entry_range': '[9.0, 9.4]', 'promotion_trigger': '量价确认'},
            'execution_contract': {'snapshot': {'last': 10, 'stop': 8.5}},
            'triggers': {'enter': '等待确认', 'invalid': '失效条件'}}


@pytest.mark.parametrize('field', ['entry_range', 'promotion_trigger', 'display_tier'])
def test_in_board_prose_cannot_change_table_structure_or_contract(field):
    row = legacy_row()
    if field == 'display_tier':
        row[field] = PROSE
    else:
        row['central_trade_plan'][field] = PROSE
    original = deepcopy(row)
    result = grade_card.system_table_html({'rows': [row]}, {'rows': []}, {}, {})
    assert_plain_prose(result)
    assert row == original
    assert '原研究区间与触发' in result
    assert '卖出分' not in result


@pytest.mark.parametrize('field', [
    'say', 'sell_zone', 'window_note', 'opp_type', 'scope', 'bypass',
    'school_notes', 'gpt_note', 'gate_label', 'gate_value', 'conflict',
    'engine_action', 'rebuy', 'quiet_headline', 'quiet_wording', 'quiet_risk', 'quiet_status',
])
def test_out_board_prose_preserves_text_and_does_not_inject_dom(field):
    row = {'code': 'TEST', 'name': '测试股', 'level': '-1A', 'board_rank': 1,
           'on_board': True, 'sell_score': 60, 'px': 10, 'held': False}
    sg = {'rows': [row]}
    decisions = {}
    why = {}
    if field in {'bypass', 'school_notes'}:
        row[field] = [PROSE]
    elif field == 'gpt_note':
        row['verification'] = {'gpt': 'reject', 'gpt_note': PROSE}
    elif field.startswith('gate_'):
        row['gates'] = {PROSE: 'BLOCK'} if field == 'gate_label' else {'开仓': PROSE}
    elif field == 'conflict':
        row['in_out_conflict'] = {'severity': PROSE, 'in_tier': PROSE,
                                  'in_action': PROSE, 'verdict': PROSE}
    elif field == 'engine_action':
        decisions['TEST'] = {'action': PROSE}
    elif field == 'rebuy':
        why['TEST'] = {'fail': PROSE}
    elif field.startswith('quiet_'):
        quiet = {'rows': [{'code': 'TEST', 'name': '测试股'}]}
        sg['holdings_quiet'] = quiet
        if field == 'quiet_headline':
            quiet['headline'] = PROSE
        elif field == 'quiet_wording':
            quiet['wording_rule'] = PROSE
        elif field == 'quiet_risk':
            quiet['rows'][0]['nearest_risk'] = PROSE
        else:
            quiet['rows'][0].update(vetoed=True, status=PROSE)
    else:
        row[field] = PROSE
    original = deepcopy((sg, decisions, why))
    result = grade_card.system_table_html({'rows': []}, sg, decisions, why)
    assert_plain_prose(result)
    assert (sg, decisions, why) == original
    assert '卖出分60' in ''.join(Parsed(result).texts)


def test_price_fallback_and_external_veto_text_do_not_become_markup():
    assert_plain_prose(grade_card._px_fresh_cell(None, None, fallback_note=PROSE))
    assert_plain_prose(grade_card._px_fresh_cell(PROSE, '2026-09-13 12:00', usable=True))
    assert_plain_prose(grade_card.veto_review_html({'rows': [
        {'name': PROSE, 'code': 'TEST', 'rank_score': 70, 'risk_veto': [PROSE], 'review_needed': True}
    ]}))


def test_shared_deep_verdict_keeps_prose_literal_and_original_decision():
    verdict = {key: PROSE for key in ['name', 'headline', 'source', 'gpt_note', 'gpt_at', 'gpt_model', 'rule']}
    verdict.update(code='TEST', gpt='reject', buy={'final': '1A', 'bucket_tier': '1A',
                   'zone': PROSE, 'missing': [PROSE], 'note': PROSE},
                   sell={'final': '-1A', 'zone': PROSE, 'bypass': [PROSE], 'note': PROSE})
    original = deepcopy(verdict)
    result = grade_card.verdict_html(verdict)
    assert_plain_prose(result)
    assert verdict == original
    assert '最终 1A' in ''.join(Parsed(result).texts)
    assert '最终 -1A' in ''.join(Parsed(result).texts)
    assert '<details><summary>' in result
