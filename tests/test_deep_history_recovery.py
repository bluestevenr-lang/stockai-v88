"""Missing bars recover without dropping validation or invoking stock reviews."""
import deep_analysis_data as data


def test_valid_local_history_never_calls_recovery(monkeypatch):
    frame=object();quality={'source':'verified local'}
    monkeypatch.setattr(data,'fetch',lambda code,allow_network:(frame,quality))
    def prohibited(code):raise AssertionError('network on healthy local data')
    assert data.load_for_view('600176.SS',prohibited)==(frame,quality)


def test_stale_local_history_recovers_requested_symbol_and_revalidates(monkeypatch):
    calls=[];frame=object();recovered={'source':'free history'}
    def local(code,allow_network):
        assert allow_network is False
        return None,{'source':'本地及同步日线暂缺'}
    monkeypatch.setattr(data,'fetch',local)
    def recover(code):calls.append(code);return frame,recovered
    def validate(result,code):
        assert result==(frame,recovered) and code=='600176.SS'
        return frame,{'source_asof':'2026-09-16','model_calls':0}
    monkeypatch.setattr(data,'revalidate_cached',validate)
    result=data.load_for_view('600176.SS',recover)
    assert calls==['600176.SS'] and result[1]['source_asof']=='2026-09-16'


def test_quarantined_history_is_not_bypassed(monkeypatch):
    quality={'source':'日线已隔离','error_detail':'identity mismatch'}
    monkeypatch.setattr(data,'fetch',lambda *a,**k:(None,quality))
    def prohibited(code):raise AssertionError('quarantine bypass')
    assert data.load_for_view('600176.SS',prohibited)==(None,quality)


def test_failed_and_newly_stale_recovery_cannot_create_chart_data(monkeypatch):
    monkeypatch.setattr(data,'fetch',lambda *a,**k:(None,{'source':'missing'}))
    failed=(None,{'source':'暂无合格日线','error_detail':'network timeout'})
    assert data.load_for_view('600176.SS',lambda c:failed)==failed
    monkeypatch.setattr(data,'revalidate_cached',lambda *a:(None,{'source':'缓存日线已失效'}))
    frame,quality=data.load_for_view('600176.SS',lambda c:(object(),{}))
    assert frame is None and quality['source']=='缓存日线已失效'
