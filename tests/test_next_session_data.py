from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import pandas as pd
import pytest
import next_session_data as data

NOW = datetime(2026, 9, 13, 4, 20, tzinfo=timezone.utc)


@pytest.fixture
def setup(monkeypatch):
    data._CACHE.clear()
    calls = []
    responses = {}
    frame = pd.DataFrame({'Close': [10., 11.]}, index=pd.to_datetime(['2026-09-10', '2026-09-11']))
    current = {'direction': 'up', 'phase': '蓄势→领涨', 'up': 64., 'down': 8.,
               'trigger': '放量站稳 10.50', 'invalid': '跌破 10.50', 'horizon': '日~周', 'confidence': '高'}
    def fetch(code, **kwargs):
        assert kwargs == {'allow_network': False}
        calls.append(code)
        if code in responses:
            return responses[code]
        return frame, {'code': code, 'source_asof': '2026-09-11', 'source': 'verified_daily',
                       'snapshot_signature': 'full-history-signature', 'price_basis': 'source-adjusted'}
    monkeypatch.setattr(data, '_dependencies', lambda: (
        fetch, lambda f: {'frame': f}, lambda f: deepcopy(current),
        lambda market, now: date(2026, 9, 11), lambda code: code.upper()))
    monkeypatch.setattr(data, '_store_stamp', lambda: ('version-a',))
    return calls, responses, frame, current


def source(*codes):
    return {'scanned': 864, 'generated_at': '2026-09-13 09:20',
            'stocks': [{'code': code, 'name': code, 'direction': 'down', 'phase': '派发→退潮',
                        'confidence': '高', 'strength': 99.} for code in codes]}


def test_recheck_retains_all_discoveries_and_uses_real_data_clock(setup):
    original = source('ONE', 'TWO'); before = deepcopy(original)
    result = data.load_signals(original, NOW)
    assert original == before and result['scanned'] == 864
    assert len(result['stocks']) == result['verified_count'] == 2
    assert result['changed_count'] == 2 and result['network_calls'] == result['model_calls'] == 0
    row = result['stocks'][0]
    assert row['source_date'] == '2026-09-11' and row['discovery_generated_at'] == '2026-09-13 09:20'
    assert row['strength'] == 64 and row['direction'] == 'up'
    assert row['trigger'] == '放量站稳 10.50' and row['invalid'] == '跌破 10.50'
    assert row['snapshot_signature'] == 'full-history-signature'
    assert row['entry_permission'] is False and row['no_grade_authority'] is True


@pytest.mark.parametrize('quality', [
    {'code': 'ONE', 'source_asof': '2026-09-04'},
    {'code': 'OTHER', 'source_asof': '2026-09-11'},
    {'code': 'ONE', 'generated_at': '2026-09-13 09:20'},
])
def test_old_missing_or_wrong_security_dates_do_not_pass(setup, quality):
    _, responses, frame, _ = setup
    responses['ONE'] = (frame, quality)
    result = data.load_signals(source('ONE'), NOW)
    row = result['stocks'][0]
    assert result['missing_count'] == 1 and result['verified_count'] == 0
    assert row['source_status'] == 'missing' and row['source_date'] is None
    assert row['strength'] is None and row['confidence'] is None
    assert '原扫描时间不代表行情日期' in row['source_note']


def test_missing_history_preserved_and_does_not_adopt_old_strong_claim(setup):
    _, responses, _, _ = setup
    responses['ONE'] = (None, {'error_detail': '被隔离的价格序列'})
    result = data.load_signals(source('ONE'), NOW)
    assert len(result['stocks']) == 1
    row = result['stocks'][0]
    assert row['source_status'] == 'missing' and '被隔离' in row['source_note']
    assert row['strength'] is None and row['trigger'] == ''


def test_ended_signal_is_visible_as_mixed_instead_of_deleted(setup):
    *_, current = setup
    current.update(direction='hold', phase='整理')
    result = data.load_signals(source('ONE'), NOW)
    assert result['mixed_count'] == result['changed_count'] == 1
    assert result['stocks'][0]['direction'] == 'mixed'
    assert result['stocks'][0]['original_direction'] == 'down'


@pytest.mark.parametrize('value', [None, float('nan'), float('inf'), True])
def test_invalid_strength_never_becomes_high_confidence(setup, value):
    *_, current = setup
    current['up'] = value
    result = data.load_signals(source('ONE'), NOW)
    assert result['verified_count'] == 0 and result['stocks'][0]['strength'] is None


def test_cache_does_not_allow_callers_to_mutate_later_results(setup):
    calls, *_ = setup
    result = data.load_signals(source('ONE'), NOW)
    result['stocks'][0]['direction'] = 'down'
    again = data.load_signals(source('ONE'), NOW + timedelta(seconds=20))
    assert again['stocks'][0]['direction'] == 'up' and calls == ['ONE']


def test_retry_after_database_update_or_minute_changes(setup, monkeypatch):
    calls, responses, *_ = setup
    responses['ONE'] = (None, {'error_detail': '待采集'})
    assert data.load_signals(source('ONE'), NOW)['missing_count'] == 1
    responses.clear()
    monkeypatch.setattr(data, '_store_stamp', lambda: ('version-b',))
    assert data.load_signals(source('ONE'), NOW)['verified_count'] == 1
    data.load_signals(source('ONE'), NOW + timedelta(minutes=1))
    assert calls == ['ONE', 'ONE', 'ONE']


def test_source_change_invalidates_cache_in_same_minute(setup):
    calls, *_ = setup
    data.load_signals(source('ONE'), NOW)
    result = data.load_signals(source('ONE', 'TWO'), NOW)
    assert result['verified_count'] == 2 and len(calls) == 3


def test_naive_clock_rejected(setup):
    with pytest.raises(ValueError, match='zoned'):
        data.load_signals(source('ONE'), NOW.replace(tzinfo=None))
