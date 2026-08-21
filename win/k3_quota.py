# -*- coding: utf-8 -*-
# k3_quota.py — 显示 Kimi Code 订阅状态 + 本地 K3 调用账本统计
# 订阅共享额度以 Kimi 会员中心为准；本脚本不调用大模型，也不查询按量现金余额。
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from kimi_subscription import configured, model_name

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

LEDGER = Path.home() / ".openclaw" / "k3_usage.csv"


def show_subscription():
    print("== Kimi Code 订阅 ==")
    print("认证状态: %s" % ("已配置" if configured() else "未配置"))
    print("V88默认模型: %s" % model_name())
    print("现金API支出: 0元（本系统不走按量接口）")
    print("共享订阅额度/重置时间: 请以 Kimi 会员中心显示为准")


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
    show_subscription()
    show_ledger()


main()
