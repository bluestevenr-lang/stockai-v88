# -*- coding: utf-8 -*-
# k3_quota.py — 查 Moonshot 账户余额 + 本地 K3 调用账本统计
# 直接调 REST 接口查余额，全程不消耗任何大模型 token。
# 密钥运行时从本机 ~/.openclaw/openclaw.json 读取，本文件不含密钥，可安全入库。
import csv
import json
import re
import sys
import urllib.request
import urllib.error
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

LEDGER = Path.home() / ".openclaw" / "k3_usage.csv"


def find_key(cfg: dict):
    mp = cfg.get("models", {}).get("providers", {}).get("moonshot", {})
    if isinstance(mp, dict):
        for cand in (mp.get("apiKey"), (mp.get("auth") or {}).get("apiKey") if isinstance(mp.get("auth"), dict) else None):
            if isinstance(cand, str) and cand.strip().startswith("sk-"):
                return cand.strip()
    m = re.search(r"sk-[A-Za-z0-9]{16,}", json.dumps(cfg, ensure_ascii=False))
    return m.group(0) if m else None


def find_base(cfg: dict):
    mp = cfg.get("models", {}).get("providers", {}).get("moonshot", {})
    if isinstance(mp, dict):
        base = mp.get("baseUrl") or mp.get("baseURL")
        if isinstance(base, str) and base.startswith("http"):
            return base.rstrip("/")
    return "https://api.moonshot.cn/v1"


def show_balance(base, key):
    req = urllib.request.Request(base + "/users/me/balance",
                                 headers={"Authorization": "Bearer " + key})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.load(r)
    d = data.get("data", data)
    print("== 账户余额 ==")
    print("可用余额: %.2f 元" % float(d.get("available_balance", 0)))
    print("  其中现金: %.2f 元" % float(d.get("cash_balance", 0)))
    print("  其中代金券: %.2f 元" % float(d.get("voucher_balance", 0)))
    print("(充值记录明细见 platform.moonshot.cn 控制台；50元充值的剩余 = 当前现金余额)")


def show_ledger():
    print()
    print("== K3 直达调用账本（本机记录）==")
    if not LEDGER.exists():
        print("暂无记录（K3回复功能用过之后才会有）")
        return
    rows = list(csv.DictReader(open(LEDGER, encoding="utf-8")))
    total_in = sum(int(r.get("prompt_tokens") or 0) for r in rows)
    total_out = sum(int(r.get("completion_tokens") or 0) for r in rows)
    print("累计调用: %d 次 | 输入 %d tokens | 输出 %d tokens" % (len(rows), total_in, total_out))
    print("最近5次:")
    for r in rows[-5:]:
        print("  %s | 入%s/出%s | %s" % (r.get("ts", "?"), r.get("prompt_tokens", "?"),
                                      r.get("completion_tokens", "?"), r.get("question", "")[:30]))


def main():
    cfg = json.load(open(Path.home() / ".openclaw" / "openclaw.json", encoding="utf-8"))
    key = find_key(cfg)
    if not key:
        print("ERROR: 未找到 moonshot 密钥")
        sys.exit(1)
    base = find_base(cfg)
    try:
        show_balance(base, key)
    except urllib.error.HTTPError as e:
        print("ERROR: 余额接口 HTTP %s — %s" % (e.code, e.read().decode("utf-8", "replace")[:300]))
    except Exception as e:
        print("ERROR: %s" % e)
    show_ledger()


main()
