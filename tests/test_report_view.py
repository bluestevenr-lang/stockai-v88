from report_view import load_report


def test_cross_midnight_keeps_authoritative_source_clock():
    manifest={'generated_at':'2026-09-12T23:59:00+08:00','source_snapshot_generated_at':'2026-09-12T22:30:00+08:00',
              'source_sessions':{'A股':'2026-09-11'},'quality':{'status':'plan_b','issues':['news missing']}}
    text,meta=load_report(loader=lambda:('actual report',manifest))
    assert text=='actual report' and meta['plan']=='B'
    assert meta['generated_at']==manifest['generated_at'] and meta['source_sessions']==manifest['source_sessions']


def test_missing_and_legacy_report_cannot_bypass_core_authority():
    for content,manifest in [(None,{'quality':{'status':'plan_b'}}),('old',{'quality':{'status':'legacy'}})]:
        text,meta=load_report(loader=lambda:(content,manifest))
        assert text is None and meta['plan'] is None


def test_failed_loader_does_not_fall_back_to_cached_report():
    def fail():raise ValueError('invalid original')
    text,meta=load_report(loader=fail)
    assert text is None and meta['status']=='missing' and 'ValueError' in meta['issues'][0]
