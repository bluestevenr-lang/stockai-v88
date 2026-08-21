"""V88网页端Kimi Code订阅用量账本。

会员共享额度由Kimi服务端执行；本地仅记录调用与token，不再按人民币余额拦截。
"""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime
from pathlib import Path

import requests

LEDGER = Path(__file__).resolve().parent / "data" / "web_ai_budget.json"
BASE_CAP = 0.0
CAP = 0.0
_TRUTH_URL = "https://raw.githubusercontent.com/bluestevenr-lang/stockai-v88/data/pub/budget_truth_pub.json"
_TRUTH_CACHE = {"ts": 0.0, "data": {}}


def _load():
    month = datetime.now().strftime("%Y-%m")
    try:
        data = json.loads(LEDGER.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    if data.get("month") != month or data.get("billing_mode") != "kimi-code-subscription":
        data = {"month": month, "billing_mode": "kimi-code-subscription",
                "model": "k3-256k", "spent_rmb": 0.0, "calls": []}
    return data


def _read_truth() -> dict:
    """每10分钟取一次公开脱敏对账；断网时退回随应用发布的本地副本。"""
    now = time.time()
    if now - float(_TRUTH_CACHE.get("ts", 0) or 0) < 600:
        return _TRUTH_CACHE.get("data") or {}
    truth = {}
    try:
        response = requests.get(_TRUTH_URL, timeout=5)
        if response.status_code == 200:
            truth = response.json()
    except Exception:
        try:
            truth = json.loads((LEDGER.parent / "budget_truth_pub.json").read_text(encoding="utf-8"))
        except Exception:
            truth = {}
    _TRUTH_CACHE.update(ts=now, data=truth)
    return truth


def _effective_spent(data: dict, *, sync_truth: bool = False) -> float:
    """官方实付为下限，再叠加本次对账后的网页新增调用。"""
    ledger = float(data.get("spent_rmb", 0) or 0)
    try:
        truth = _read_truth()
        real = float(truth.get("real_month_spent"))
        checked = str(truth.get("checked_at") or "")
        if checked and data.get("truth_seen_at") != checked:
            if sync_truth:
                data["truth_seen_at"] = checked
                data["truth_web_spent_at_check"] = ledger
            checkpoint = ledger
        else:
            checkpoint = float(data.get("truth_web_spent_at_check", ledger) or ledger)
        return max(ledger, real + max(0.0, ledger - checkpoint))
    except Exception:
        return ledger


def reserve(prompt: str, output_tokens=1600, *, scope="web-general", priority=False):
    data = _load()
    ticket = {"id": uuid.uuid4().hex[:10], "rmb": 0.0, "ts": time.time(),
              "scope": str(scope), "priority": bool(priority), "model": "k3-256k",
              "estimated_input_tokens": max(1, int(len(str(prompt)) / 1.5)),
              "estimated_output_tokens": max(1, int(output_tokens))}
    data.setdefault("pending", {})[ticket["id"]] = ticket
    LEDGER.parent.mkdir(exist_ok=True)
    LEDGER.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    return ticket


def settle(ticket, usage=None, ok=True):
    if not ticket:
        return
    data = _load()
    pending = data.setdefault("pending", {}).pop(str(ticket.get("id")), None)
    if pending is None:
        return
    usage = usage or {}
    inp = int(usage.get("prompt_tokens", 0) or 0)
    out = int(usage.get("completion_tokens", 0) or 0)
    data["calls"] = (data.get("calls") or [])[-999:] + [{
        "ts": time.time(), "ok": bool(ok), "scope": ticket.get("scope"),
        "prompt_tokens": inp, "completion_tokens": out, "model": "k3-256k"}]
    data["cap_rmb"] = CAP
    LEDGER.parent.mkdir(exist_ok=True)
    LEDGER.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")


def status():
    data = _load()
    calls = data.get("calls") or []
    good = [x for x in calls if x.get("ok")]
    return {"billing_mode": "kimi-code-subscription", "model": "k3-256k",
            "calls": len(good), "failed_calls": len(calls) - len(good),
            "prompt_tokens": sum(int(x.get("prompt_tokens", 0) or 0) for x in good),
            "completion_tokens": sum(int(x.get("completion_tokens", 0) or 0) for x in good),
            "cash_rmb": 0.0, "spent": 0.0, "ledger_spent": 0.0,
            "base_cap": 0.0, "cap": 0.0, "base_remaining": 0.0, "remaining": 0.0}
