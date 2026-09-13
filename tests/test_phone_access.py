from types import SimpleNamespace
import phone_access as phone

def test_current_lan_address_is_discovered_without_fixed_old_ip(monkeypatch):
    monkeypatch.setattr(phone.sys,'platform','darwin')
    monkeypatch.setattr(phone.subprocess,'run',lambda *a,**k:SimpleNamespace(stdout='192.168.5.16\n'))
    assert phone.links()==['http://192.168.5.16:8501/']
    assert '同一 Wi-Fi' in phone.html()

def test_loopback_unspecified_and_broken_interface_cannot_be_phone_url(monkeypatch):
    monkeypatch.setattr(phone.sys,'platform','darwin')
    for value in ['127.0.0.1','0.0.0.0','169.254.1.2','broken']:
        monkeypatch.setattr(phone.subprocess,'run',lambda *a,**k:SimpleNamespace(stdout=value))
        assert not phone.links()
