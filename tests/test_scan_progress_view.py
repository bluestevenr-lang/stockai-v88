from datetime import datetime, timezone
from scan_progress_view import metrics, html

NOW = datetime(2026, 9, 7, 13, tzinfo=timezone.utc)


def source():
    return {'receipt': 'fixture', 'available_at': NOW.isoformat(), 'daily': {'markets': {
        m: {'denominator': 100, 'directories_complete': True,
            'verified_daily_by_session': {d: n}, 'quote_available_by_session': {d: 100}}
        for m, d, n in [('A股','2026-09-07',92), ('港股','2026-09-07',78), ('美股','2026-09-04',90)]}},
        'counts': {m: {'facts_generated': 80, 'missing_history': 15, 'source_rejected': 3, 'fact_rejected': 2}
                   for m in ('A股','港股','美股')}}


def test_checked_failures_do_not_become_usable_data_or_reviews():
    rows = [{'code': 'AAPL', 'market': '美股', 'scorecard': {'gpt': {'current':True,'complete':True}}}]
    result = metrics(source(), rows+rows, now=NOW)
    assert all(r['checked'] == 100 and r['scan_complete'] for r in result)
    assert all(r['facts'] == 80 for r in result)
    assert [r['valid_daily'] for r in result] == [92,78,90]
    assert [r['reviewed'] for r in result] == [0,0,1]
    assert [r['daily_target_met'] for r in result] == [True,False,True]
    body = html(rows,source=source(),now=NOW)
    assert '已扫描 300 / 300' in body and '生成事实 240' in body
    assert '港股有效日线距90%还差 12' in body


def test_new_close_expires_daily_without_erasing_completed_scan():
    result = metrics(source(), [], now=datetime(2026,9,8,9,tzinfo=timezone.utc))
    assert [r['valid_daily'] for r in result] == [0,0,90]
    assert all(r['checked']==100 and r['scan_complete'] for r in result)


def test_unrecognized_status_or_changed_denominator_never_claims_full_check():
    s = source();s['counts']['港股']['missing_history'] = 14;s['counts']['港股']['unknown'] = 1
    s['daily']['markets']['A股']['denominator'] = 101
    result = metrics(s, [], now=NOW)
    assert not result[0]['scan_complete'] and not result[1]['scan_complete']
    assert result[1]['checked'] == 99
    assert '目录检查尚未齐全' in html([],source=s,now=NOW)


def test_missing_dates_and_absent_source_are_unknown_not_passed(tmp_path):
    s = source();s['daily']['markets']['A股'].pop('verified_daily_by_session')
    result = metrics(s, [], now=NOW)
    assert result[0]['valid_daily'] is None and not result[0]['daily_target_met']
    assert '扫描进度暂不可核对' in html([],core=tmp_path,now=NOW)


def test_all_codes_checked_is_distinct_from_future_catalog_effective_date():
    s=source();s['daily']['catalogs']=[{'market':'港股','time_status':'目录日期在未来，待生效核对',
                                      'conservative_effective_union':True}]
    rows=metrics(s,[],now=NOW)
    assert all(r['rows_checked'] for r in rows) and not rows[1]['scan_complete']
    body=html([],source=s,now=NOW)
    assert '目录逐只检查完成·港股成分时效待核' in body
    assert '目录检查尚未齐全' not in body and '港股有效日线距90%还差 12' in body


def test_expired_or_incomplete_reviews_are_not_counted():
    rows = [{'code':code,'market':'美股','scorecard':{'gpt':{'current':current,'complete':complete}}}
            for code,current,complete in [('A',True,False),('B',False,True),('C',True,True)]]
    assert metrics(source(),rows,now=NOW)[2]['reviewed']==1


def test_staged_progress_does_not_inflate_published_or_whole_market_reviews():
    s=source();s['active_review']={'eligible':127,'complete':23,'reused':11,'new_complete':12,'remaining':104}
    body=html([],source=s,now=NOW)
    assert '当前完整GPT双审 0 只' in body
    assert '本轮可审 127 只' in body and '已完成双审 23' in body and '剩余 104' in body
    assert '暂存审核完成后统一发布' in body


def test_active_stage_counts_only_matching_current_independent_pairs(tmp_path,monkeypatch):
    import json,os,time,hashlib,review_scorecard,review_contract,scan_progress_view
    from scan_progress_view import active_review_progress
    data=tmp_path/'data';run=data/'full_market_review_runs/20260912T054333-4395ea39'
    (run/'data').mkdir(parents=True)
    marker=data/'.gpt_classics_review_running';marker.write_text(str(os.getpid()))
    os.utime(marker,(time.time()-10,time.time()-10))
    (run/'input.json').write_text(json.dumps({'factpack_id':'new'}))
    pp=run/'data/review_factpack.json';pp.write_text(json.dumps({'factpack_id':'new','items':[{'code':c,'sig':c.lower()} for c in 'ABCD']}))
    (run/'review-cohort.json').write_text(json.dumps({'factpack_id':'new','factpack_sha256':hashlib.sha256(pp.read_bytes()).hexdigest(),'eligible':{c:c.lower() for c in 'ABCD'}}))
    def rec(sig,pair=True):return {'sig':sig,'pair':pair,'model':review_contract.MODEL,'review_schema_version':review_contract.SCHEMA_VERSION,'prompt_hash':review_contract.PROMPT_HASH,'review_scope':'buy','ts':'2026-09-07 20:00（北京时间）'}
    (data/'gpt_verify.json').write_text(json.dumps({'rows':{'A':rec('a')}}))
    (run/'data/gpt_verify.json').write_text(json.dumps({'factpack_id':'new','rows':{'B':rec('b'),'C':rec('stale'),'D':rec('d',False)}}))
    monkeypatch.setattr(review_scorecard,'pair_complete',lambda r:r.get('pair') is True)
    monkeypatch.setattr(review_scorecard,'gpt_result',lambda r:{'valid':r.get('pair') is True})
    monkeypatch.setattr(scan_progress_view,'source_times_fresh',lambda r,now:True)
    r=active_review_progress(tmp_path,now=NOW)
    assert (r['eligible'],r['complete'],r['reused'],r['new_complete'],r['remaining'])==(4,2,1,1,2)
    original=pp.read_bytes();pp.write_text('{}')
    assert active_review_progress(tmp_path,now=NOW) is None
    pp.write_bytes(original)
    marker.unlink()
    assert active_review_progress(tmp_path,now=NOW) is None
    marker.write_text(str(os.getpid()));os.utime(marker,(time.time()-10,time.time()-10))
    (run/'result.json').write_text('{}')
    assert active_review_progress(tmp_path,now=NOW) is None
    (run/'result.json').unlink();os.utime(run/'input.json',(time.time()-20,time.time()-20))
    assert active_review_progress(tmp_path,now=NOW) is None


def test_future_directory_cannot_pass_90_percent_even_when_counts_match():
    s=source();s['daily']['markets']['港股']['verified_daily_by_session']['2026-09-07']=100
    s['daily']['catalogs']=[{'market':'港股','time_status':'目录日期在未来，待生效核对'}]
    r=metrics(s,[],now=NOW)[1]
    assert not r['scan_complete'] and not r['daily_target_met'] and r['catalog_future']
    assert '未来生效' in html([],source=s,now=NOW)


def test_same_size_directory_churn_uses_identity_and_retains_old_count(tmp_path):
    import hashlib,json,sqlite3
    from scan_progress_view import snapshot
    data=tmp_path/'data';(data/'universe_manifests').mkdir(parents=True)
    codes={'A股':['NEW.SZ'],'港股':['700.HK'],'美股':['AAPL']}
    frozen={'codes_by_market':codes,'catalogs':[],'excluded_exchange_declared_etf_etp':0,'invalid_entries':[]}
    digest=hashlib.sha256(json.dumps(frozen,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
    path=data/'universe_manifests'/(digest+'.json');path.write_text(json.dumps({**frozen,'sha256_canonical_content':digest}))
    d=source()['daily']
    for m in codes:d['markets'][m]['denominator']=1
    d['universe_manifest']={'path':str(path),'sha256_canonical_content':digest}
    (data/'universe_data_coverage.json').write_text(json.dumps(d))
    db=sqlite3.connect(data/'full_market_research.sqlite')
    db.executescript('CREATE TABLE snapshots(id TEXT,source_available_at TEXT);CREATE TABLE active(id INTEGER,snapshot TEXT);CREATE TABLE members(snapshot TEXT,code TEXT,market TEXT,status TEXT,sources TEXT);')
    db.execute('INSERT INTO snapshots VALUES (?,?)',('s',NOW.isoformat()));db.execute("INSERT INTO active VALUES (1,'s')")
    db.execute("INSERT INTO members VALUES ('s','OLD.SZ','A股','facts_generated','{}')");db.commit();db.close()
    snap=snapshot(tmp_path,now=NOW);assert snap['counts']['A股']=={} and snap['removed_from_directory']['A股']==1
    assert metrics(snap,[],now=NOW)[0]['unscanned']==1
    path.write_text('{}')
    assert '暂不可核对' in html([],core=tmp_path,now=NOW)
