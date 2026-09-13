"""Current LAN address; no hard-coded DHCP address or public exposure."""
from html import escape
import ipaddress,subprocess,sys


def links():
    if sys.platform!='darwin':return []
    found=[]
    for interface in ('en0','en1'):
        try:
            value=subprocess.run(['/usr/sbin/ipconfig','getifaddr',interface],capture_output=True,text=True,timeout=2).stdout.strip()
            ip=ipaddress.ip_address(value)
            if ip.version==4 and ip.is_private and not (ip.is_loopback or ip.is_unspecified or ip.is_link_local) and value not in found:found.append(value)
        except (OSError,ValueError,subprocess.TimeoutExpired):continue
    return [f'http://{ip}:8501/' for ip in found]


def html():
    urls=links()
    if not urls:return ''
    return '<details id="v88-phone-entry" class="v88-operational-note"><summary>手机同屏访问</summary>同一 Wi-Fi 下打开：'+ ' · '.join(f'<a href="{escape(url,quote=True)}">{escape(url)}</a>' for url in urls)+'。地址随当前网络更新；电脑需保持运行。</details>'
