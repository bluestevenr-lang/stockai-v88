"""bottom_turn_pool.py — 【V88·触底拐点机会池】恢复+现代化（2026-07-19 用户点单）

前身=「💎深度回调机会池」（瘦身时删了桌面渲染）。按最新计量体系重建：
- 优质池出身：horizon POOLS 116只（中美港各行业流动性龙头）
- 深水位（双口径）：距52周高≤-25% 或 52周分位≤20%；幸存者再补历史口径（距历史高）
- **拐点已现**：turning.side=='bottom'（放量收复MA20/放量长阳/底背离金叉…）
  或 阶段∈(底部启动/启动确认)——仍在寻底的不收（宁缺毋滥，与旧版关键差异）
- 排序=机会分：加权上涨概率 + 12×min(盈亏比,2.5) + 2×期望 + 底拐信号数×2
中美港各Top10。纯确定性零AI；概率=规则情景估计（非回测胜率）。
"""
from __future__ import annotations


def scan_bottom_turns(fetch_fn, analyze_fn, forward_fn, pools: dict, *,
                      extremes_fn=None, top_n=10, min_bars=120) -> dict:
    """pools={"美股":{"codes":[(code,name)...]},...}；返回 {market:[rows], scanned, hits}。"""
    out = {"markets": {}, "scanned": 0, "deep": 0, "turned": 0}
    for market, blk in pools.items():
        rows = []
        for code, name in (blk.get("codes") or []):
            try:
                df = fetch_fn(code)
            except Exception:
                df = None
            if df is None or len(df) < min_bars:
                continue
            out["scanned"] += 1
            try:
                close = df["Close"].dropna()
                last = float(close.iloc[-1])
                w52 = close.tail(min(252, len(close)))
                hi52, lo52 = float(w52.max()), float(w52.min())
                dd52 = (last / hi52 - 1) * 100 if hi52 else 0.0
                p52 = (last - lo52) / (hi52 - lo52) * 100 if hi52 > lo52 else 50.0
            except Exception:
                continue
            if not (dd52 <= -25 or p52 <= 20):
                continue                      # 闸①深水位
            out["deep"] += 1
            try:
                full = analyze_fn(df)
            except Exception:
                full = None
            if not full:
                continue
            turning = full.get("turning") or {}
            stage = str(full.get("stage") or "")
            bottom_sigs = list(turning.get("signals") or []) if turning.get("side") == "bottom" else []
            turned = bool(bottom_sigs) or any(k in stage for k in ("底部启动", "启动确认"))
            if not turned:
                continue                      # 闸②拐点已现（仍在寻底不收）
            out["turned"] += 1
            try:
                fwd = forward_fn(df, name=name, code=code)
                if fwd.get("error"):
                    continue
            except Exception:
                continue
            p_up = float(fwd.get("weighted_p_up") or 50)
            rr = float(fwd.get("weighted_rr") or 0)
            ev = float(fwd.get("weighted_expected_pct") or 0)
            score = round(p_up + 12 * min(rr, 2.5) + 2 * ev + 2 * len(bottom_sigs))
            hist_txt = ""
            if extremes_fn:
                try:
                    ext = extremes_fn(code)
                    if ext:
                        ddh = (last / ext["hist_high"] - 1) * 100
                        hist_txt = f"距历史高{ddh:+.0f}%({str(ext['hist_high_date'])[:7]})"
                except Exception:
                    pass
            rows.append({
                "code": code, "name": name, "market": market, "last": round(last, 2),
                "dd52": round(dd52, 1), "p52": round(p52), "hist": hist_txt,
                "stage": stage,
                "turn_label": str(turning.get("label") or stage),
                "turn_sigs": "；".join(str(x) for x in bottom_sigs[:2]) or "阶段确认",
                "p_up": round(p_up), "rr": round(rr, 2), "ev": round(ev, 1),
                "score": score,
                "support": full.get("support"), "stop": full.get("stop"),
            })
        rows.sort(key=lambda r: -r["score"])
        out["markets"][market] = rows[:top_n]
    return out
