import copy
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_grade_card_safety import central_v2
from review_display import current_scorecard, actionable, BJT
from investment_maturity import assess

MONDAY = datetime(2026, 9, 7, 4, tzinfo=BJT)


@pytest.mark.parametrize('bucket,index,tier', [('observations', 0, '2A'), ('observations', 1, '1A'), ('recommendations', 0, '3A')])
@pytest.mark.parametrize('quote_source', ['eastmoney_quote', 'tencent_quote', 'yahoo_quote'])
def test_mobile_and_desktop_keep_dated_grades_over_weekend(bucket, index, tier, quote_source):
    snapshot = central_v2()
    snapshot['generated_at'] = '2026-09-06T22:00:00+08:00'
    row = snapshot[bucket][index]
    row['market'] = '美股'
    daily_source = 'tencent_daily' if quote_source == 'yahoo_quote' else 'yahoo_daily'
    sources = {daily_source: '2026-09-04T16:00:00-04:00', quote_source: '2026-09-05T04:00:00+08:00'}
    row['source_timestamps'] = sources
    row['reviews']['classics']['source_timestamps'] = copy.deepcopy(sources)
    for leg in ('gpt', 'classics'):
        row['reviews'][leg]['at'] = snapshot['generated_at']
    for now in (MONDAY, MONDAY.astimezone(timezone.utc)):
        card = current_scorecard(snapshot, row, now)
        assert assess(card, row['horizon'], row['trade_plan'])['tier'] == tier
        assert actionable(snapshot, row, now) == (tier == '3A')
    assert not current_scorecard(snapshot, row, MONDAY + timedelta(hours=25))['gpt']['current']
    row['source_timestamps'][quote_source] = '2026-09-03T16:00:00-04:00'
    assert not current_scorecard(snapshot, row, MONDAY)['books']['current']
    assert not actionable(snapshot, row, MONDAY)
