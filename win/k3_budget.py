# -*- coding: utf-8 -*-
# k3_budget.py — Moonshot 现金预算哨兵（零 token 确定性脚本，不调大模型）
# 每月现金上限 10 元（2026-08-24 用户定纲）。余额快照落台账，算本月已花；
# 超 8 元 ⚠️ 预警，超 10 元 🚨 报警。充值自动识别（余额跳增不计入花费）。
import json
import re
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

LEDGER = Path.home() / ".openclaw" / "moonshot_balance.jsonl"
USAGE_CSV = Path.home() / ".openclaw" / "k3_usage.csv"
MONTHLY_CAP = 10.0
WARN_AT = 8.0
# K3 按量官价（元/百万 tokens）：输入 20 / 输出 100；K2.7：6.5 / 27
RATE = {"kimi-k3": (20, 100), "k3-256k": (0, 0), "kimi-k2.7-code": (6.5, 27)}


def find_key(cfg):
    mp = cfg.get("models", {}).get("providers", {}).get("moonshot", {})
    if isinstance(mp, dict):
        for c in (mp.get("apiKey"), (mp.get("auth") or {}).get("apiKey") if isinstance(mp.get("auth"), dict) else None):
            if isinstance(c, str) and c.strip().startswith("sk-"):
                return c.strip()
    m = re.search(r"sk-[A-Za-z0-9]{16,}", json.dumps(cfg, ensure_ascii=False))
    return m.group(0) if m else None


def fetch_balance():
    cfg = json.load(open(Path.home() / ".openclaw" / "openclaw.json", encoding="utf-8"))
    key = find_key(cfg)
    if not key:
        return None
    req = urllib.request.Request("https://api.moonshot.cn/v1/users/me/balance",
                                 headers={"Authorization": "Bearer " + key})
    d = json.load(urllib.request.urlopen(req, timeout=20)).get("data", {})
    return {"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "balance": float(d.get("available_balance", 0)),
            "cash": float(d.get("cash_balance", 0)),
            "voucher": float(d.get("voucher_balance", 0))}


def load_ledger():
    rows = []
    if LEDGER.exists():
        for line in LEDGER.read_text(encoding="utf-8").splitlines():
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    return rows


def month_spend(rows):
    """本月花费 = 本月相邻快照的下降量之和（跳增视为充值，不计）。"""
    month = datetime.now().strftime("%Y-%m")
    pts = [r for r in rows if str(r.get("ts", "")).startswith(month)]
    pts.sort(key=lambda r: r["ts"])
    spent = 0.0
    for a, b in zip(pts, pts[1:]):
        delta = float(a["balance"]) - float(b["balance"])
        if delta > 0:
            spent += delta
    return spent, (pts[0]["ts"][:10] if pts else None)


def local_usage_estimate():
    """本地按量账本（k3_usage.csv）本月花费估算——只覆盖 k3ask 一路。"""
    if not USAGE_CSV.exists():
        return 0, 0, 0.0
    month = datetime.now().strftime("%Y-%m")
    n = 0
    cost = 0.0
    tokens = 0
    import csv
    for r in csv.DictReader(open(USAGE_CSV, encoding="utf-8")):
        if not str(r.get("ts", "")).startswith(month):
            continue
        ri, ro = RATE.get(r.get("model", ""), (20, 100))
        try:
            pi, po = int(r["prompt_tokens"]), int(r["completion_tokens"])
        except Exception:
            continue
        n += 1
        tokens += pi + po
        cost += pi / 1e6 * ri + po / 1e6 * ro
    return n, tokens, cost


def main():
    print("== Moonshot 预算哨兵（每月现金上限 %.0f 元）==" % MONTHLY_CAP)
    snap = fetch_balance()
    if snap is None:
        print("🚨 余额查询失败（无密钥或网络异常），无法核对预算")
        sys.exit(1)
    rows = load_ledger()
    # 同一分钟不重复落点
    if not rows or rows[-1].get("ts", "")[:16] != snap["ts"][:16]:
        rows.append(snap)
        LEDGER.parent.mkdir(parents=True, exist_ok=True)
        with open(LEDGER, "a", encoding="utf-8") as f:
            f.write(json.dumps(snap, ensure_ascii=False) + "\n")
    spent, since = month_spend(rows)
    n, tokens, est = local_usage_estimate()

    print("当前余额: %.2f 元（现金 %.2f / 代金券 %.2f）" % (snap["balance"], snap["cash"], snap["voucher"]))
    print("本月已花: %.2f 元（台账起点: %s；跳增自动计为充值）" % (spent, since or "今日"))
    print("本地按量账本: 本月 %d 次调用 / %d tokens / 估算 %.2f 元（仅 k3ask 一路，不含网关与 Mac 端）" % (n, tokens, est))

    if spent > MONTHLY_CAP:
        print("🚨 状态: 超支！本月已花 %.2f 元 > 上限 %.0f 元——立即停止一切按量调用并核查消耗源" % (spent, MONTHLY_CAP))
        sys.exit(2)
    elif spent > WARN_AT:
        print("⚠️ 状态: 预警，本月已花 %.2f 元 > %.0f 元警戒线，逼近上限" % (spent, WARN_AT))
    else:
        print("✅ 状态: 正常（%.2f / %.0f 元）" % (spent, MONTHLY_CAP))


if __name__ == "__main__":
    main()
