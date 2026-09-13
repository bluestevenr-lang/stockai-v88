from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pandas as pd
import pytest
import market_data_helper as m
import ts_helper


def frame(market='A股'):
    # Test data must include the actual completed session after a market close.
    # An arbitrary now-minus-two-days fixture silently became stale on Monday.
    m._core()
    from history_calendar import latest_completed, coverage
    end=latest_completed(market,datetime.now(timezone.utc))
    days=coverage([],market,(end-timedelta(days=30)).isoformat(),end.isoformat())['missing_sessions'][-8:]
    dates=pd.to_datetime(days)
    return pd.DataFrame({'Open':10.,'High':12.,'Low':9.,'Close':11.,'Volume':1000.},index=dates)


def test_old_provider_cannot_be_reactivated_by_existing_token(monkeypatch):
    monkeypatch.setenv('TUSHARE_TOKEN','not-a-real-token')
    assert ts_helper.get_pro() is None
    assert ts_helper.get_tushare_status()['available'] is False
    assert ts_helper.fetch_daily_tushare is m.fetch_daily_free


def test_price_units_and_timestamp_are_not_realtime():
    d=m.validate(frame(),'000001.SZ',source='test')
    assert not d.attrs['is_realtime'] and d.attrs['source_asof']
    assert d.Volume.iloc[-1]==1000
    assert not m.is_index('000001.SZ') and m.is_index('000001.SS')


def test_geometry_staleness_and_future_rejected():
    d=frame();d.iloc[-1,d.columns.get_loc('High')]=8
    with pytest.raises(ValueError):m.validate(d,'000001.SZ',source='test')
    d=frame();d.index=d.index-pd.Timedelta(days=60)
    with pytest.raises(ValueError):m.validate(d,'000001.SZ',source='test')
    d=frame();d.index=d.index+pd.Timedelta(days=60)
    with pytest.raises(ValueError):m.validate(d,'000001.SZ',source='test')


def test_calendar_period_clips_by_date_and_rejects_five_year_gap():
    from history_calendar import coverage
    end=pd.Timestamp.now().normalize()-pd.Timedelta(days=2)
    full=coverage([], 'A股', (end-pd.Timedelta(days=600)).date().isoformat(), end.date().isoformat())
    d=frame().reindex(pd.to_datetime(full['missing_sessions'])).ffill().bfill()
    out=m.clip_calendar(d,365,'000001.SZ')
    assert len(out)<=366
    assert out.index[0]>=pd.Timestamp.now().normalize()-pd.Timedelta(days=365)
    with pytest.raises(ValueError):m.clip_calendar(d,1826,'000001.SZ')


def test_one_day_requests_one_session(monkeypatch):
    monkeypatch.setattr(m,'fetch_daily_free',lambda *args:frame())
    assert len(m.fetch_df('000001.SZ','1d'))==1
    assert len(m.fetch_df('000001.SZ','5d'))==5


def test_missing_exchange_directory_never_returns_partial_ranking(tmp_path,monkeypatch):
    import json
    monkeypatch.setattr(m,'CORE',tmp_path)
    data=tmp_path/'data/free_market_data';data.mkdir(parents=True)
    (data/'cn.json').write_text(json.dumps({'pagination_complete':True,'rows':[]}))
    assert m.fetch_cn_top_pool()==[]


def test_yahoo_short_request_fetches_validation_buffer(monkeypatch):
    from unittest import mock
    fake=mock.Mock();fake.Ticker.return_value.history.return_value=frame('美股')
    monkeypatch.setitem(sys.modules,'yfinance',fake)
    out=m.fetch_df('AAPL','1d')
    assert len(out)==1
    assert fake.Ticker.return_value.history.call_args.kwargs['period']=='1mo'


def test_missing_quote_cannot_produce_top_pool(tmp_path,monkeypatch):
    import json
    monkeypatch.setattr(m,'CORE',tmp_path)
    root=tmp_path/'data/free_market_data';root.mkdir(parents=True)
    now=datetime.now().isoformat()
    for name in ['sse','star','szse','bse']:
        (root/f'directory_{name}.json').write_text(json.dumps({'status':'downloaded','fetched_at':now,'codes':['600000.SS']}))
    (root/'cn.json').write_text(json.dumps({'pagination_complete':True,'rows':[]}))
    assert m.fetch_cn_top_pool()==[]


def test_short_history_cannot_fill_a_missing_session_with_older_row():
    d=frame('美股').drop(frame('美股').index[-2])
    with pytest.raises(ValueError,match='缺口'):m.clip_sessions(d,5,'AAPL')
