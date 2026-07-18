"""stock_profile.py — 【V88·公司档案】（2026-07-18 用户点单：像东财一样看到公司简介）

三市场中文公司简介，免费东财F10接口（实测通），统一字段 ORG_PROFILE：
- A股: emweb.securities.eastmoney.com PC_HSF10 CompanySurvey（jbzl[0].ORG_PROFILE）
- 港股: datacenter RPT_HKF10_INFO_ORGPROFILE（SECUCODE=06055.HK）
- 美股: datacenter RPT_USF10_INFO_ORGPROFILE（SECURITY_CODE=OTIS）——东财App同源
兜底: yfinance longBusinessSummary（英文·如实标注）。
文件缓存7天（简介不常变），零AI成本。
"""
from __future__ import annotations

import json
import time
from pathlib import Path

_CACHE_DIR = Path(__file__).parent / ".cache_profile"
_HDR = {"User-Agent": "Mozilla/5.0"}
_TTL = 7 * 86400


def _market_of(code: str) -> str:
    c = str(code or "").upper().strip()
    num = c.split(".")[0]
    if c.endswith((".SS", ".SZ", ".SH")) or (num.isdigit() and len(num) == 6):
        return "CN"
    if c.endswith(".HK") or (num.isdigit() and 1 <= len(num) <= 5):
        return "HK"
    return "US"


def get_profile(code: str) -> dict:
    """{profile: 简介文本, source: 出处}；拿不到返回 {}（调用处如实说明）。"""
    c = str(code or "").upper().strip()
    if not c:
        return {}
    _CACHE_DIR.mkdir(exist_ok=True)
    fp = _CACHE_DIR / (c.replace("/", "_") + ".json")
    try:
        d = json.loads(fp.read_text(encoding="utf-8"))
        if time.time() - float(d.get("ts", 0)) < _TTL and d.get("profile"):
            return d
    except Exception:
        pass

    import requests
    sess = requests.Session()
    sess.trust_env = False          # 东财国内接口直连
    mkt = _market_of(c)
    profile, source = "", ""
    try:
        if mkt == "CN":
            num = c.split(".")[0]
            _pref = "SH" if c.endswith((".SS", ".SH")) or num[0] in "569" else "SZ"
            r = sess.get("https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/PageAjax",
                         params={"code": _pref + num}, headers=_HDR, timeout=12)
            jb = ((r.json().get("jbzl")) or [{}])[0]
            profile = str(jb.get("ORG_PROFILE") or "").strip()
            source = "东财F10(A股)"
        elif mkt == "HK":
            num = c.split(".")[0].zfill(5)
            r = sess.get("https://datacenter.eastmoney.com/securities/api/data/v1/get",
                         headers=_HDR, timeout=12,
                         params={"reportName": "RPT_HKF10_INFO_ORGPROFILE", "columns": "ALL",
                                 "filter": f'(SECUCODE="{num}.HK")', "source": "F10", "client": "PC"})
            rows = ((r.json().get("result") or {}).get("data")) or []
            profile = str((rows[0].get("ORG_PROFILE") if rows else "") or "").strip()
            source = "东财F10(港股)"
        else:
            r = sess.get("https://datacenter.eastmoney.com/securities/api/data/v1/get",
                         headers=_HDR, timeout=12,
                         params={"reportName": "RPT_USF10_INFO_ORGPROFILE", "columns": "ALL",
                                 "filter": f'(SECURITY_CODE="{c.split(".")[0]}")',
                                 "source": "SECURITIES", "client": "PC"})
            rows = ((r.json().get("result") or {}).get("data")) or []
            profile = str((rows[0].get("ORG_PROFILE") if rows else "") or "").strip()
            source = "东财F10(美股·中文)"
    except Exception:
        profile = ""
    if not profile:
        # 兜底:雅虎英文简介——如实标注,不装中文
        try:
            import yfinance as yf
            info = yf.Ticker(c).info or {}
            profile = str(info.get("longBusinessSummary") or "").strip()
            source = "Yahoo(英文原文)"
        except Exception:
            pass
    out = {"ts": time.time(), "profile": profile, "source": source}
    if profile:
        try:
            fp.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass
    return out if profile else {}


if __name__ == "__main__":
    for c in ("OTIS", "300631.SZ", "06055.HK"):
        d = get_profile(c)
        print(c, "→", (d.get("profile") or "")[:50], "|", d.get("source"))
