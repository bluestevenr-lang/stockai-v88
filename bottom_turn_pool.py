"""bottom_turn_pool.py — 【V88·触底拐点机会池】恢复+现代化（2026-07-19 用户点单）

前身=「💎深度回调机会池」。2026-07-19 二期（用户点单"要大样本大池+精准高分"）：
- 大池：全市场股票池（美/港/A 约676-1300只，与一键全选同池），并发闸门筛选
- 三闸门：①深水位双口径(距52周高≤-25% 或 52周分位≤20%) ②拐点已现
  (turning底拐信号/底部启动/启动确认——仍在寻底不收) ③机会分≥min_score(高分才配上榜)
- 精准计分：加权上涨概率 + 12×min(盈亏比,2.5) + 2×期望
  + 拐点信号加权(底背离4/收复MA20·放量长阳3/其他2) + 阶段加分(启动确认4/底部启动2)
中美港各Top10。纯确定性零AI；概率=规则情景估计（非回测胜率）。
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed


def _sig_weight(sig: str) -> int:
    s = str(sig)
    if "底背离" in s:
        return 4
    if "收复MA20" in s or "站回MA20" in s or "长阳" in s:
        return 3
    return 2


def _stage1(fetch_fn, analyze_fn, code, name, market, min_bars):
    """闸①②：深水位+拐点已现。返回 survivor dict 或 None。线程内只做纯计算。"""
    try:
        df = fetch_fn(code)
    except Exception:
        return ("err", None)
    if df is None or len(df) < min_bars:
        return ("err", None)
    try:
        close = df["Close"].dropna()
        last = float(close.iloc[-1])
        w52 = close.tail(min(252, len(close)))
        hi52, lo52 = float(w52.max()), float(w52.min())
        dd52 = (last / hi52 - 1) * 100 if hi52 else 0.0
        p52 = (last - lo52) / (hi52 - lo52) * 100 if hi52 > lo52 else 50.0
    except Exception:
        return ("err", None)
    if not (dd52 <= -25 or p52 <= 20):
        return ("shallow", None)
    try:
        full = analyze_fn(df)
    except Exception:
        full = None
    if not full:
        return ("err", None)
    turning = full.get("turning") or {}
    stage = str(full.get("stage") or "")
    bottom_sigs = list(turning.get("signals") or []) if turning.get("side") == "bottom" else []
    turned = bool(bottom_sigs) or any(k in stage for k in ("底部启动", "启动确认"))
    if not turned:
        return ("deep_only", None)
    return ("turned", {"code": code, "name": name, "market": market, "df": df,
                       "full": full, "last": last, "dd52": dd52, "p52": p52,
                       "bottom_sigs": bottom_sigs, "stage": stage})


def scan_bottom_turns(fetch_fn, analyze_fn, forward_fn, pools: dict, *,
                      extremes_fn=None, top_n=10, min_bars=120,
                      max_workers=8, min_score=55, progress_cb=None) -> dict:
    """pools={"美股":{"codes":[(code,name)...]},...}。
    stage1(取数+闸①②)并发；幸存者串行跑前瞻与计分。progress_cb(done,total,turned)在主线程回调。"""
    tasks = [(mk, c, n) for mk, blk in pools.items() for c, n in (blk.get("codes") or [])]
    total = len(tasks)
    out = {"markets": {mk: [] for mk in pools}, "scanned": 0, "deep": 0,
           "turned": 0, "cut_low": 0, "pool_size": total}
    survivors = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_stage1, fetch_fn, analyze_fn, c, n, mk, min_bars): (mk, c)
                for mk, c, n in tasks}
        done = 0
        for fut in as_completed(futs):
            done += 1
            try:
                tag, sv = fut.result()
            except Exception:
                tag, sv = "err", None
            if tag != "err":
                out["scanned"] += 1
            if tag in ("deep_only", "turned"):
                out["deep"] += 1
            if tag == "turned" and sv:
                out["turned"] += 1
                survivors.append(sv)
            if progress_cb and (done % 20 == 0 or done == total):
                try:
                    progress_cb(done, total, out["turned"])
                except Exception:
                    pass

    for sv in survivors:                       # 闸③精准计分（幸存者少，串行安全）
        try:
            fwd = forward_fn(sv["df"], name=sv["name"], code=sv["code"])
            if fwd.get("error"):
                continue
        except Exception:
            continue
        p_up = float(fwd.get("weighted_p_up") or 50)
        rr = float(fwd.get("weighted_rr") or 0)
        ev = float(fwd.get("weighted_expected_pct") or 0)
        stage = sv["stage"]
        score = round(p_up + 12 * min(rr, 2.5) + 2 * ev
                      + sum(_sig_weight(x) for x in sv["bottom_sigs"])
                      + (4 if "启动确认" in stage else (2 if "底部启动" in stage else 0)))
        if score < min_score:
            out["cut_low"] += 1
            continue
        hist_txt = ""
        if extremes_fn:
            try:
                ext = extremes_fn(sv["code"])
                if ext:
                    ddh = (sv["last"] / ext["hist_high"] - 1) * 100
                    hist_txt = f"距历史高{ddh:+.0f}%({str(ext['hist_high_date'])[:7]})"
            except Exception:
                pass
        full, turning = sv["full"], (sv["full"].get("turning") or {})
        out["markets"].setdefault(sv["market"], []).append({
            "code": sv["code"], "name": sv["name"], "market": sv["market"],
            "last": round(sv["last"], 2), "dd52": round(sv["dd52"], 1),
            "p52": round(sv["p52"]), "hist": hist_txt, "stage": stage,
            "turn_label": str(turning.get("label") or stage),
            "turn_sigs": "；".join(str(x) for x in sv["bottom_sigs"][:2]) or "阶段确认",
            "p_up": round(p_up), "rr": round(rr, 2), "ev": round(ev, 1),
            "score": score,
            "support": full.get("support"), "stop": full.get("stop"),
        })
    for mk in out["markets"]:
        out["markets"][mk].sort(key=lambda r: -r["score"])
        out["markets"][mk] = out["markets"][mk][:top_n]
    return out
