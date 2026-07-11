"""
position_lifecycle.py — 【V88·持仓生命周期管理】三端共用（飞书日报/盘中预警/云端/桌面）。

设计定则（2026-07-11 用户拍板，源于"海油+25%没卖反-20%割肉"案例）：
不预测顶部，只判断"继续持有的胜率变化"。按阶段管理：
  建仓观察期 → 利润保护期(峰值浮盈≥15%) → 减仓评估(浮盈≥20%) ；亏损区单独走纪律。
移动止盈锚定历史最高浮盈；利润回撤超阈值或趋势破坏 → 提醒减仓。
亏损纪律区分 成长股(-10%硬止损) 与 核心资产(-12%且破MA55才退出评估)。
每条持仓的输出必须包含：继续持有条件 / 减仓触发 / 退出触发 + 当下已触发信号。
全部确定性计算，无 LLM。峰值状态持久化 data/position_peaks.json（Actions 每小时回写）。
"""
import json
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
PEAKS = BASE / "data" / "position_peaks.json"

# 利润保护期定则
PROTECT_PEAK = 15.0      # 峰值浮盈≥15% 进入利润保护期
TRAIL_DD = 10.0          # 从峰值回撤超10个百分点 → 锁盈减仓
EVAL_PNL = 20.0          # 浮盈≥20% → 评估减仓（里程碑，每+10pt再提示一次）
FRAME_PNL = 50.0         # FRAMEWORK 硬纪律：+50% 减1/3
# 亏损纪律
LOSS_GROWTH = -10.0      # 成长股硬止损
LOSS_CORE = -12.0        # 核心资产：≤-12% 且破MA55 才进入退出评估

_CORE_HINTS = ("ETF", "标普", "纳指", "指数", "沪深", "中概", "500", "300", "红利",
               "国债", "黄金", "恒生", "科创50", "移动", "石油", "海油", "银行", "神华")


def asset_class(h: dict) -> str:
    """核心资产 or 成长股。持仓可显式写 "class": "核心"/"成长" 覆盖启发式。"""
    c = str(h.get("class", "")).strip()
    if c in ("核心", "成长"):
        return c
    name = str(h.get("name", ""))
    return "核心" if any(k in name for k in _CORE_HINTS) else "成长"


def load_peaks() -> dict:
    try:
        return json.loads(PEAKS.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_peaks(peaks: dict):
    PEAKS.parent.mkdir(exist_ok=True)
    PEAKS.write_text(json.dumps(peaks, ensure_ascii=False, indent=1), encoding="utf-8")


def _trend_broken(f: dict) -> list:
    """趋势破坏信号（利润保护期内任一命中即建议减仓）。f=analyze_trend_full 输出。"""
    sig = []
    t = (f.get("turning") or {})
    if t.get("side") == "top":
        sig.append(f"顶部拐点:{t.get('brief', '')}")
    if f.get("stage") in ("趋势转弱", "破位下跌", "放量滞涨"):
        sig.append(f"阶段={f['stage']}")
    ma = f.get("ma") or {}
    if ma.get(20) and f.get("last") and f["last"] < ma[20] and "放量" in str(f.get("vp", "")):
        sig.append("放量跌破MA20")
    if "死叉" in str(f.get("macd_txt", "")):
        sig.append("MACD死叉")
    return sig


def assess(h: dict, f: dict, peaks: dict = None) -> dict:
    """单条持仓生命周期判定。

    h: positions.json 的 holding（需 cost；shares/name/code 可选）
    f: cloud_engine.analyze_trend_full 输出（含实时 last）
    peaks: load_peaks() 的字典，传入则就地更新峰值/里程碑（调用方负责 save）

    返回 {stage, action, pnl, peak, signals[], hold, reduce, exit}
    action ∈ 持有 / 评估减仓 / 减仓 / 退出
    """
    cost = float(h.get("cost") or 0)
    last = float(f.get("last") or 0)
    if not cost or not last:
        return {}
    code = str(h.get("code", ""))
    cls = asset_class(h)
    pnl = (last / cost - 1) * 100

    st = (peaks if peaks is not None else {}).setdefault(code, {}) if peaks is not None else {}
    peak = max(float(st.get("peak_pnl", pnl)), pnl)
    if peaks is not None:
        st["peak_pnl"] = round(peak, 2)
        st["ts"] = int(time.time())
    milestones = st.get("milestones", []) if st else []

    broken = _trend_broken(f)
    signals, action = [], "持有"

    if peak >= PROTECT_PEAK:                       # ── 利润保护期
        stage = "利润保护期"
        lock_px = round(cost * (1 + (peak - TRAIL_DD) / 100), 3)
        if pnl <= peak - TRAIL_DD:
            action = "减仓"
            signals.append(f"🔒锁盈:浮盈峰值+{peak:.0f}%→现+{pnl:.0f}%,回撤超{TRAIL_DD:.0f}点——别让利润变亏损")
        if broken:
            action = "减仓"
            signals.append("📉趋势破坏:" + "/".join(broken))
        if pnl >= FRAME_PNL and FRAME_PNL not in milestones:
            action = "减仓"
            signals.append(f"🏆+{FRAME_PNL:.0f}%触发框架纪律:减1/3")
            if peaks is not None:
                st.setdefault("milestones", []).append(FRAME_PNL)
        elif pnl >= EVAL_PNL:
            band = int(pnl // 10) * 10             # 20/30/40… 每档提示一次
            if band not in milestones:
                if action == "持有":
                    action = "评估减仓"
                signals.append(f"🎯浮盈+{pnl:.0f}%达评估档{band}%:结合量价/MACD/资金评估是否兑现一部分")
                if peaks is not None:
                    st.setdefault("milestones", []).append(band)
        hold = f"持有条件:不破锁盈线{lock_px}·无顶拐/放量破MA20/MACD死叉"
        reduce = f"减仓触发:回落至{lock_px}(峰值-{TRAIL_DD:.0f}pt)或任一趋势破坏信号"
        exit_ = f"退出触发:跌破成本{cost}或引擎止损{f.get('stop', '—')}"
    elif pnl < 0:                                  # ── 亏损纪律区
        stage = f"亏损区({cls})"
        ma55 = (f.get("ma") or {}).get(55)
        below55 = bool(ma55 and last < ma55)
        if cls == "成长" and pnl <= LOSS_GROWTH:
            action = "退出"
            signals.append(f"❗成长股亏{pnl:.1f}%破{LOSS_GROWTH:.0f}%硬止损——纪律离场,不等反弹")
        elif cls == "核心" and pnl <= LOSS_CORE and below55:
            action = "退出"
            signals.append(f"❗核心资产亏{pnl:.1f}%且破MA55——退出评估(FRAMEWORK纪律)")
        elif broken:
            action = "评估减仓"
            signals.append("📉亏损中趋势破坏:" + "/".join(broken))
        _lim = LOSS_GROWTH if cls == "成长" else LOSS_CORE
        hold = f"持有条件:亏损不超{_lim:.0f}%" + ("且守住MA55" if cls == "核心" else "")
        reduce = "减仓触发:趋势破坏信号出现"
        exit_ = (f"退出触发:亏损≤{_lim:.0f}%" + ("且收盘破MA55" if cls == "核心" else "(硬止损)"))
    else:                                          # ── 建仓观察期
        stage = "建仓观察期"
        if broken:
            action = "评估减仓"
            signals.append("📉趋势破坏:" + "/".join(broken))
        hold = f"持有条件:浮盈滚动·峰值达+{PROTECT_PEAK:.0f}%自动进利润保护期"
        reduce = "减仓触发:趋势破坏信号"
        exit_ = f"退出触发:亏至{LOSS_GROWTH if cls == '成长' else LOSS_CORE:.0f}%纪律线"

    return {"stage": stage, "action": action, "cls": cls,
            "pnl": round(pnl, 1), "peak": round(peak, 1), "signals": signals,
            "hold": hold, "reduce": reduce, "exit": exit_}


def build_section() -> str:
    """「## 💼 持仓生命周期」日报段（💼 前缀=公开版自动脱敏，仅飞书/桌面可见）。"""
    import yfinance as yf
    try:
        from watch_alerts import _yf_code, _atf
    except Exception:
        from src.watch_alerts import _yf_code, _atf
    if _atf is None:
        return ""
    try:
        pj = json.loads((BASE / "positions.json").read_text(encoding="utf-8"))
    except Exception:
        return ""
    peaks = load_peaks()
    rows = []
    for acc, a in (pj.get("accounts") or {}).items():
        for h in (a.get("holdings") or []):
            code = str(h.get("code", ""))
            if not code or "⚠️" in code or not h.get("cost"):
                continue
            try:
                df = yf.Ticker(_yf_code(code)).history(period="6mo")
                f = _atf(df) if df is not None and len(df) >= 35 else None
                if not f:
                    continue
                r = assess(h, f, peaks)
                if not r:
                    continue
                mark = {"退出": "❗", "减仓": "⚠️", "评估减仓": "🎯"}.get(r["action"], "")
                sig = "；".join(r["signals"]) or "—"
                rows.append((0 if r["action"] in ("退出", "减仓") else 1,
                             f"| {h.get('name')}（{code}） | {r['pnl']:+.1f}% | {r['peak']:+.1f}% | "
                             f"{r['stage']} | {mark}{r['action']} | {sig} | {r['reduce']}；{r['exit']} |"))
            except Exception:
                continue
    save_peaks(peaks)
    if not rows:
        return ""
    rows.sort(key=lambda x: x[0])
    return ("## 💼 持仓生命周期（成本实算·移动止盈·不预测顶部只管纪律）\n\n"
            "> 峰值浮盈≥15%进利润保护期；从峰值回撤超10点或趋势破坏→锁盈减仓；"
            "+20%起每档评估兑现；成长股-10%/核心资产-12%破MA55纪律退出。\n\n"
            "| 持仓 | 浮盈 | 峰值 | 阶段 | 动作 | 当前信号 | 减仓/退出触发 |\n"
            "|---|---|---|---|---|---|---|\n" + "\n".join(r[1] for r in rows) + "\n\n---\n")


def append_section() -> dict:
    """幂等插入 data/daily_report.md（「## 五、」之前），与 watch_alerts 同款挂法。"""
    try:
        sec = build_section()
        if not sec:
            return {"ok": True, "rows": 0}
        fp = BASE / "data" / "daily_report.md"
        md = fp.read_text(encoding="utf-8")
        _mark = "## 💼 持仓生命周期"
        while _mark in md:
            i0 = md.find(_mark)
            j0 = md.find("\n## ", i0 + 5)
            md = md[:i0] + (md[j0 + 1:] if j0 > 0 else "")
        i = md.find("## 五、")
        md = (md[:i] + sec + md[i:]) if i > 0 else (md + "\n" + sec)
        fp.write_text(md, encoding="utf-8")
        return {"ok": True, "rows": sec.count("\n| ") - 1}
    except Exception as e:
        return {"ok": False, "error": str(e)[:100]}
