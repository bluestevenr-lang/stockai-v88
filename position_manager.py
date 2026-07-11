"""
position_manager.py — 【V88·持仓终端】简化输入，三端共用（桌面输入框/云端持仓终端页/CLI）。

语法（一行一条，中英文空格均可）：
  中国海油 18.5 1000            买入/更新（名称|代码 成本 股数，可加账户名、"核心"/"成长"）
  买 海油 18.5 1000 东财-A      同上（"买"可省略；名称支持名录模糊匹配）
  卖 中国海油                   清仓（自动按现价归档已实现盈亏到 journal/trades.json）
  卖 中国海油 500               减仓500股
  查                            列出全部持仓

positions.json 仍是唯一权威底稿；本模块只做结构化读写，不做任何行情判断。
"""
import json
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
POS = BASE / "positions.json"
TRADES = BASE / "journal" / "trades.json"
BJT = timezone(timedelta(hours=8))
DEFAULT_ACCOUNT = "手动录入"

try:
    from src.cloud_engine import search_candidates, name_of
except Exception:
    try:
        from cloud_engine import search_candidates, name_of
    except Exception:
        search_candidates = name_of = None

_CODE_RE = re.compile(r"^[A-Za-z0-9.\-]{1,12}$")


def _resolve(token: str):
    """名称/代码 → (name, yahoo_code)。找不到返回 (token, None)。"""
    token = token.strip()
    if _CODE_RE.match(token) and any(ch.isdigit() for ch in token) or (token.isalpha() and token.isupper()):
        # 看着像代码：6位数字→A股、≤5位→港股、字母→美股（与全局 market_of_code 口径一致）
        t = token.upper()
        if t.replace(".", "").isdigit():
            d = t.split(".")[0]
            if len(d) == 6:
                code = d + (".SS" if d[0] in "569" else ".SZ")
            else:
                code = f"{int(d):04d}.HK"
        else:
            code = t
        nm = name_of(code) if name_of else ""
        return (nm or token), code
    if search_candidates:
        cand = search_candidates(token, limit=1)
        if cand:
            return cand[0][0], cand[0][1]
    return token, None


def _load() -> dict:
    try:
        return json.loads(POS.read_text(encoding="utf-8"))
    except Exception:
        return {"accounts": {}}


def _save(pj: dict):
    pj["updated_at"] = datetime.now(BJT).strftime("%Y-%m-%d %H:%M") + "（持仓终端·北京时间）"
    POS.write_text(json.dumps(pj, ensure_ascii=False, indent=1), encoding="utf-8")


def _iter_holdings(pj):
    for acc, a in (pj.get("accounts") or {}).items():
        for h in (a.get("holdings") or []):
            yield acc, a, h


def _find(pj, name_or_code):
    q = name_or_code.strip()
    # 优先在自己的持仓里匹配（含子串），避免名录歧义（"海油"≠海油工程）
    hits = [(acc, a, h) for acc, a, h in _iter_holdings(pj)
            if q in (h.get("name") or "") or q.upper() == str(h.get("code", "")).upper()]
    if len(hits) == 1:
        return hits[0]
    if len(hits) > 1:  # 多账户同票等歧义：精确名优先，否则取第一个
        exact = [x for x in hits if x[2].get("name") == q]
        return (exact or hits)[0]
    nm, code = _resolve(q)
    for acc, a, h in _iter_holdings(pj):
        if h.get("code") == code or h.get("name") == nm:
            return acc, a, h
    return None, None, None


def _log_trade(rec: dict):
    TRADES.parent.mkdir(exist_ok=True)
    try:
        logs = json.loads(TRADES.read_text(encoding="utf-8"))
    except Exception:
        logs = []
    logs.append(rec)
    TRADES.write_text(json.dumps(logs, ensure_ascii=False, indent=1), encoding="utf-8")


def _live_price(code: str):
    try:
        import yfinance as yf
        try:
            from watch_alerts import _yf_code as _norm
        except Exception:  # 云端同步副本无 watch_alerts，退用 cloud_engine 同款规格化
            from cloud_engine import _yf_norm as _norm
        df = yf.Ticker(_norm(code)).history(period="5d")
        return float(df["Close"].iloc[-1]) if df is not None and len(df) else None
    except Exception:
        return None


def handle(line: str) -> str:
    """处理一行指令，返回给用户看的结果文本。"""
    parts = line.replace("，", " ").replace(",", " ").split()
    if not parts:
        return "空指令。语法：名称 成本 股数 ｜ 卖 名称 [股数] ｜ 查"

    if parts[0] in ("查", "查询", "list"):
        pj = _load()
        rows = [f"{acc}｜{h.get('name')}({h.get('code')}) {h.get('shares')}股 成本{h.get('cost')}"
                for acc, _, h in _iter_holdings(pj)]
        return "\n".join(rows) or "（无持仓）"

    if parts[0] in ("卖", "卖出", "清", "清仓", "sell"):
        if len(parts) < 2:
            return "语法：卖 名称 [股数|全部]"
        pj = _load()
        acc, a, h = _find(pj, parts[1])
        if not h:
            return f"未找到持仓「{parts[1]}」，先用「查」核对名称"
        qty = h.get("shares") or 0
        n = qty if len(parts) < 3 or parts[2] in ("全部", "all") else min(int(float(parts[2])), qty)
        px = _live_price(h["code"]) or h.get("last_seen_price") or 0
        pnl = round((px / h["cost"] - 1) * 100, 2) if h.get("cost") and px else None
        _log_trade({"date": datetime.now(BJT).strftime("%Y-%m-%d %H:%M"), "action": "卖出",
                    "name": h.get("name"), "code": h.get("code"), "shares": n,
                    "cost": h.get("cost"), "sell_price": px, "pnl_pct": pnl, "account": acc})
        if n >= qty:
            a["holdings"] = [x for x in a["holdings"] if x is not h]
            verdict = f"已清仓 {h.get('name')} {qty}股"
        else:
            h["shares"] = qty - n
            verdict = f"已减仓 {h.get('name')} {n}股，剩{h['shares']}股"
        _save(pj)
        tail = f"，卖价{px} 盈亏{pnl:+.1f}%（已记交易日志）" if pnl is not None else "（无实价，日志按0记）"
        return verdict + tail

    # ── 买入/更新：[买] 名称|代码 成本 股数 [账户] [核心|成长]
    if parts[0] in ("买", "买入", "buy"):
        parts = parts[1:]
    if len(parts) < 3:
        return "语法：名称 成本 股数 [账户] [核心|成长]，例：中国海油 18.5 1000"
    nm_in, cost_s, shares_s, *rest = parts
    try:
        cost, shares = float(cost_s), int(float(shares_s))
    except ValueError:
        return f"成本/股数须为数字，收到：{cost_s} / {shares_s}"
    cls = next((x for x in rest if x in ("核心", "成长")), "")
    account = next((x for x in rest if x not in ("核心", "成长")), DEFAULT_ACCOUNT)
    name, code = _resolve(nm_in)
    if not code:
        return f"名录未找到「{nm_in}」——可直接输代码（如 600938 / 0700.HK / NVDA）"
    pj = _load()
    acc0, _, h = _find(pj, code)
    if h:  # 已持有 → 加权平均成本
        tot = h["shares"] + shares
        h["cost"] = round((h["cost"] * h["shares"] + cost * shares) / tot, 4)
        h["shares"] = tot
        if cls:
            h["class"] = cls
        verdict = f"已加仓 {name}({code}) +{shares}股，共{tot}股，摊薄成本{h['cost']}（{acc0}）"
    else:
        a = pj.setdefault("accounts", {}).setdefault(account, {"type": "手动", "holdings": []})
        entry = {"name": name, "code": code, "shares": shares, "cost": cost}
        if cls:
            entry["class"] = cls
        a.setdefault("holdings", []).append(entry)
        verdict = f"已录入 {name}({code}) {shares}股 成本{cost}（{account}）"
    _log_trade({"date": datetime.now(BJT).strftime("%Y-%m-%d %H:%M"), "action": "买入",
                "name": name, "code": code, "shares": shares, "cost": cost, "account": account})
    _save(pj)
    return verdict


if __name__ == "__main__":
    import sys
    print(handle(" ".join(sys.argv[1:]) if len(sys.argv) > 1 else "查"))
