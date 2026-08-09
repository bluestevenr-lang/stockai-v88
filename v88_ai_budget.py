"""V88 网页 AI 预算闸门：全系统基础 7 元，重点思考可用至 10 元。

网页 AI 均由用户主动触发，属于深度复核；实际总消费以云端发布的 DeepSeek
官方余额对账为下限，避免网页/桌面/云端三套本地账本各算各的。
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
BASE_CAP = float(os.getenv("V88_AI_BASE_BUDGET_RMB", "7") or 7)
CAP = float(os.getenv("V88_AI_MONTHLY_BUDGET_RMB", "10") or 10)
_TRUTH_URL = "https://raw.githubusercontent.com/bluestevenr-lang/stockai-v88/data/pub/budget_truth_pub.json"
_TRUTH_CACHE = {"ts": 0.0, "data": {}}


def _load():
    month = datetime.now().strftime("%Y-%m")
    try:
        data = json.loads(LEDGER.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    if data.get("month") != month:
        data = {"month": month, "spent_rmb": 0.0, "calls": []}
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
    est = round((len(str(prompt)) / 1.5 + output_tokens * 2) / 1_000_000, 6)
    limit = CAP if priority else BASE_CAP
    if _effective_spent(data, sync_truth=True) + est > min(CAP, limit):
        return None
    ticket = {"id": uuid.uuid4().hex[:10], "rmb": est, "ts": time.time(),
              "scope": str(scope), "priority": bool(priority)}
    data["spent_rmb"] = round(float(data.get("spent_rmb", 0)) + est, 6)
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
    actual = ((inp + out * 2) / 1_000_000) if (inp or out) else float(ticket["rmb"])
    data["spent_rmb"] = round(max(0.0, float(data.get("spent_rmb", 0))
                                  - float(ticket["rmb"]) + (actual if ok else 0.0)), 6)
    data["calls"] = (data.get("calls") or [])[-99:] + [{"ts": time.time(), "rmb": actual if ok else 0}]
    data["cap_rmb"] = CAP
    LEDGER.parent.mkdir(exist_ok=True)
    LEDGER.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")


def status():
    data = _load()
    ledger = float(data.get("spent_rmb", 0))
    spent = _effective_spent(data)
    return {"spent": spent, "ledger_spent": ledger, "base_cap": BASE_CAP, "cap": CAP,
            "base_remaining": max(0.0, BASE_CAP - spent),
            "remaining": max(0.0, CAP - spent)}
