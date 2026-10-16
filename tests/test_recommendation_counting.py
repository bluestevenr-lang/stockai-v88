from copy import deepcopy

from recommendation_counting import (count_asof, count_label, count_tooltip,
    publication_day, recommendation_days, security_key)


def record(code, market, days, kind='formal'):
    return {'code':code,'market':market,'kind':kind,'days':{
        day:[{'at':day+'T12:00:00+08:00','state':state} for state in states]
        for day,states in days.items()}}


def test_same_day_refresh_republish_and_modules_count_once_by_security():
    doc={'rows':[
        record('00700.HK','港股',{'2026-10-01':['listed','listed'],'2026-10-03':['listed']}),
        record('700.HK','HK',{'2026-10-01':['listed'],'2026-10-02':['listed']},'strategy_3a'),
        record('00700','港股',{'2026-10-03':['listed']},'discovery')]}
    astra={'calendar_records':[{'at':'2026-10-02T07:00:00Z','row':{'code':'700.HK','market':'港股'}}]}
    before=deepcopy((doc,astra));index=recommendation_days(doc,astra)
    assert index=={('港股','700.HK'):('2026-10-01','2026-10-02','2026-10-03')}
    assert [count_asof(index,'HK','00700.HK','2026-10-0'+str(i)) for i in (1,2,3)]==[1,2,3]
    assert (doc,astra)==before


def test_month_reset_historical_cutoff_and_market_identity():
    doc={'rows':[record('A','US',{'2026-09-30':['listed'],'2026-10-02':['listed'],
                                  '2026-10-09':['listed'],'2026-11-01':['listed']}),
                 record('A','CN',{'2026-10-01':['listed']})]}
    index=recommendation_days(doc)
    assert count_asof(index,'US','A','2026-10-01')==0
    assert count_asof(index,'US','A','2026-10-02')==1
    assert count_asof(index,'US','A','2026-10-08')==1
    assert count_asof(index,'US','A','2026-10-09')==2
    assert count_asof(index,'US','A','2026-11-01')==1
    assert count_asof(index,'CN','A','2026-10-09')==1


def test_followups_risk_price_changes_exits_and_astra_seen_dates_never_add_days():
    row=record('A','US',{'2026-10-01':['listed'],'2026-10-02':['check'],
                       '2026-10-03':['left'],'2026-10-04':['listed']})
    row['days']['2026-10-04'][0]['event_type']='quote_update'
    row['days']['2026-10-05']=[{'at':'2026-10-05T01:00:00Z','state':'listed','counts_as_recommendation':False}]
    astra={'calendar_records':[{'at':'2026-10-01T01:00:00Z','row':{'code':'A','market':'US'},
         'seen_dates':['2026-10-02','2026-10-06'],'outcome':{'date':'2026-10-07','status':'target_touched'}},
         {'at':'2026-10-08T01:00:00Z','row':{'code':'A','market':'US'},'event_type':'risk_review'}]}
    index=recommendation_days({'rows':[row]},astra)
    assert count_asof(index,'US','A','2026-10-31')==1
    assert '本条仅跟踪' in count_tooltip(1,'2026-10-07',False)


def test_beijing_publication_date_not_bucket_capture_time_or_quote_date():
    doc={'rows':[{'code':'A','market':'US','days':{'2026-09-30':[
        {'at':'2026-09-30T23:00:00-04:00','captured_at':'2026-10-09T00:00:00Z',
         'source_asof':'2026-09-29','state':'listed'}]}}]}
    index=recommendation_days(doc)
    assert index[('美股','A')]==('2026-10-01',)
    assert count_asof(index,'US','A','2026-09-30')==0
    assert publication_day('2026-10-01T12:00:00') is None
    assert publication_day('invalid') is None


def test_aliases_preserve_share_classes_and_invalid_identity_does_not_count():
    assert security_key('CN','600000.SH')==security_key('A股','600000.SS')
    assert security_key('US','BRK-A')==security_key('美股','BRK.A')
    assert security_key('US','BRK-A')!=security_key('US','BRK-B')
    assert security_key('bad','A') is None
    assert security_key('US','') is None
    assert count_label('A',3)=='A*3' and count_label('A',0)=='A'
    assert count_tooltip(3,'2026-10-16').startswith('本月第3个推荐日')
