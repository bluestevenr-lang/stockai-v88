"""网页版AI配额1元/月——共享5元总预算的网页预留部分(主账本4元在私仓ai_budget)。"""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime
from pathlib import Path

LEDGER = Path(__file__).resolve().parent / "data" / "web_ai_budget.json"
CAP = float(os.getenv("V88_WEB_AI_MONTHLY_BUDGET_RMB", "1") or 1)


def _load():
    month = datetime.now().strftime("%Y-%m")
    try:
        data = json.loads(LEDGER.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    if data.get("month") != month:
        data = {"month": month, "spent_rmb": 0.0, "calls": []}
    return data


def reserve(prompt: str, output_tokens=1600):
    data = _load()
    est = round((len(str(prompt)) / 1.5 + output_tokens * 2) / 1_000_000, 6)
    if float(data.get("spent_rmb", 0)) + est > CAP:
        return None
    ticket = {"id": uuid.uuid4().hex[:10], "rmb": est, "ts": time.time()}
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
    spent = float(data.get("spent_rmb", 0))
    return {"spent": spent, "cap": CAP, "remaining": max(0.0, CAP - spent)}
