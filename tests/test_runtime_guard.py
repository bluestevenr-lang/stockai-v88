from runtime_guard import ensure

def test_file_capacity_never_changes_hard_limit_or_lowers_soft():
    from runtime_guard import ensure_file_capacity
    class Fake:
        RLIMIT_NOFILE=1
        RLIM_INFINITY=-1
        def __init__(self,limits):self.limits=limits;self.changes=[]
        def getrlimit(self,k):return self.limits
        def setrlimit(self,k,v):self.changes.append(v)
    for initial,expected in [((256,2048),(2048,2048)),((256,-1),(4096,-1)),((8192,-1),None),((-1,-1),None)]:
        fake=Fake(initial);ensure_file_capacity(resource_api=fake)
        assert fake.changes==([expected] if expected else [])


def test_http_sessions_reused_and_do_not_disable_tls():
    from network_resources import http_session
    direct=http_session(True)
    assert direct is http_session(True) and direct.trust_env is False and direct.verify is True
    proxy=http_session(False)
    assert proxy is http_session(False) and proxy is not direct and proxy.trust_env is True

def test_healthy_existing_service_is_not_restarted():
    starts=[]
    r=ensure(probe=lambda p:True,find=lambda p:[123],identify=lambda p:True,start=lambda:starts.append(1))
    assert r['status']=='reused_healthy' and not starts

def test_foreign_or_ambiguous_listener_never_killed_or_replaced():
    for pids in ([123],[123,456]):
        r=ensure(probe=lambda p:True,find=lambda p:pids,identify=lambda p:False,start=lambda:(_ for _ in ()).throw(AssertionError()))
        assert not r['ok'] and r['status']=='port_owned_by_other_service'

def test_absent_service_can_recover_and_unhealthy_process_is_retained():
    r=ensure(probe=lambda p:True,find=lambda p:[],start=lambda:123)
    assert r['status']=='started_healthy'
    r=ensure(probe=lambda p:False,find=lambda p:[123],identify=lambda p:True,start=lambda:None)
    assert r['status']=='existing_v88_unhealthy' and not r['ok']
