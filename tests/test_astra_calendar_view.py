from copy import deepcopy
from datetime import datetime
import json
import re

import astra_calendar_view as calendar_view


NOW = datetime.fromisoformat('2026-10-31T18:00:00+08:00')


def proposal(day='2026-10-09', *, code='144.HK', market='港股', name='招商局港口', score=80, **extra):
    return {
        'id': day + code, 'cycle_id': day[:8] + '01',
        'at': day + 'T12:00:00+08:00', 'seen_dates': [day],
        'row': {'code': code, 'market': market, 'name': name,
                'strategy_score': {'value': score, 'revision': 'original'},
                'entry_range': [16, 17], 'take_profit_range': [18, 19],
                'stop_range': [15, 15.5], 'business_reason': '原企业依据',
                'window': {'deadline': '2026-11-09'}},
        **extra,
    }


def journal_row(days, *, code='00144.HK', market='HK', kind='formal'):
    return {'code': code, 'market': market, 'kind': kind, 'days': {
        day: [{'at': day + 'T09:00:00+08:00', 'state': 'listed'}] for day in days}}


def cell(page, day):
    # Adjacent-month padding can repeat a date, but never contains its events.
    return ''.join(re.findall(r'<td [^>]*data-date="' + day + r'"[^>]*>(.*?)</td>', page, re.S))


def test_recommendation_day_ordinals_use_full_union_and_historical_cutoff():
    first = proposal()
    repeated = proposal(at='2026-10-09T16:00:00+08:00')
    doc = {'calendar_records': [first, repeated, proposal('2026-10-10'), proposal('2026-10-20')]}
    journal = {'rows': [journal_row(['2026-10-08', '2026-10-09']),
                        journal_row(['2026-10-09'], kind='strategy_3a')]}
    before = deepcopy((doc, journal))
    page = calendar_view.html(doc, NOW, journal=journal)
    for day, count in [('2026-10-09', 2), ('2026-10-10', 3), ('2026-10-20', 4)]:
        actual = cell(page, day)
        assert f'招商局港口*{count}' in actual
        assert f'本月第{count}个推荐日' in actual
    assert '招商局港口*4' not in cell(page, '2026-10-09')
    assert '2条记录' in cell(page, '2026-10-09')
    assert (doc, journal) == before


def test_symbol_and_market_aliases_group_without_discarding_original_versions():
    doc = {'calendar_records': [proposal(), proposal(code='00144.HK', market='HK')]}
    page = calendar_view.html(doc, NOW, journal={'rows': [journal_row(['2026-10-08'], code='144')]})
    actual = cell(page, '2026-10-09')
    assert actual.count('<details class="event ') == 1
    assert '招商局港口*2' in actual and '2条记录' in actual
    assert actual.count('原评分 80') == 2
    assert '🇭🇰' in actual


def test_tracking_and_outcomes_do_not_add_recommendations_and_month_resets():
    old = proposal('2026-09-30', seen_dates=['2026-09-30', '2026-10-01', '2026-10-02'],
                   outcome={'date': '2026-10-03', 'status': 'target_touched', 'label': '🎯 目标触及'})
    new = proposal('2026-10-09', seen_dates=['2026-10-09', '2026-10-10'])
    page = calendar_view.html({'calendar_records': [old, new]}, NOW, journal={})
    assert '招商局港口*1' in cell(page, '2026-09-30')
    for day in ('2026-10-01', '2026-10-02', '2026-10-03'):
        actual = cell(page, day)
        assert '招商局港口*' not in actual
        assert '本月尚无已留存推荐日' in actual and '本条仅跟踪，不新增推荐' in actual
    assert '🎯 目标触及（行情）' in cell(page, '2026-10-03')
    assert '招商局港口*1' in cell(page, '2026-10-09')
    assert '招商局港口*1' in cell(page, '2026-10-10')
    assert '截至2026-10-10' in cell(page, '2026-10-10')


def test_explicit_followup_risk_and_exit_records_never_increment_or_claim_recommendation():
    records = [proposal('2026-10-07')]
    records += [proposal(day, event_type=kind) for day, kind in
                [('2026-10-08', 'tracking'), ('2026-10-09', 'risk_review'), ('2026-10-10', 'exit')]]
    records.append(proposal('2026-10-11', counts_as_recommendation=False))
    page = calendar_view.html({'calendar_records': records}, NOW, journal={})
    for day in ('2026-10-08', '2026-10-09', '2026-10-10', '2026-10-11'):
        actual = cell(page, day)
        assert '招商局港口*1' in actual and '本条仅跟踪，不新增推荐' in actual
        assert '📝 推荐记录' not in actual


def test_counting_precedes_daily_rank_and_five_per_market_display_limit():
    records = [proposal('2026-10-01', code=f'{i}.HK', name=f'同市场{i}', score=90-i) for i in range(1, 6)]
    records += [proposal('2026-10-01', name='后续入选', score=1),
                proposal('2026-10-02', name='后续入选', score=90)]
    page = calendar_view.html({'calendar_records': records}, NOW, journal={})
    first = cell(page, '2026-10-01')
    assert first.count('<details class="event ') == 5
    assert '后续入选' not in first and '其余已归档' in first
    assert '后续入选*2' in cell(page, '2026-10-02')


def test_default_read_only_journal_load_adds_prior_recommendation_days(tmp_path, monkeypatch):
    data = tmp_path / 'data'
    data.mkdir()
    path = data / 'recommendation_journal_pub.json'
    path.write_text(json.dumps({'rows': [journal_row(['2026-10-08'])]}))
    original = path.read_bytes()
    monkeypatch.setattr(calendar_view, 'core_root', lambda: tmp_path)
    page = calendar_view.html({'calendar_records': [proposal()]}, NOW)
    assert '招商局港口*2' in cell(page, '2026-10-09')
    assert path.read_bytes() == original


def test_missing_default_journal_keeps_astra_history_without_creating_data(tmp_path, monkeypatch):
    monkeypatch.setattr(calendar_view, 'core_root', lambda: tmp_path)
    page = calendar_view.html({'calendar_records': [proposal()]}, NOW)
    assert '招商局港口*1' in cell(page, '2026-10-09')
    assert not (tmp_path / 'data').exists()


def test_beijing_day_boundary_escapes_name_and_retains_original_plan():
    name = '<script>"港口"</script>'
    utc = proposal('2026-09-30', name=name, at='2026-09-30T16:01:00+00:00', seen_dates=[])
    doc = {'calendar_records': [utc, proposal('2026-10-01', name=name)]}
    page = calendar_view.html(doc, NOW, journal={})
    actual = cell(page, '2026-10-01')
    assert '<script>' not in page
    assert '&lt;script&gt;&quot;港口&quot;&lt;/script&gt;*1' in actual
    assert '本月第1个推荐日' in actual and '*2' not in actual
    assert '入场 16.00～17.00' in actual and '止盈 18.00～19.00' in actual
    assert '2026-09-30T16:01:00+00:00' in actual
