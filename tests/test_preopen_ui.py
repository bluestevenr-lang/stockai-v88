from autonomous_desktop import refresh_failure_current

def test_previous_failed_sync_no_longer_overrides_newer_real_screening():
    error={'status':'failed','requested_at':'2026-10-11T09:57:29+00:00'}
    assert not refresh_failure_current(error,{'screening_finished_at':'2026-10-12T00:02:38+00:00'})
    assert refresh_failure_current(error,{'screening_finished_at':'2026-10-11T08:02:38+00:00'})
    assert refresh_failure_current(error,{})
