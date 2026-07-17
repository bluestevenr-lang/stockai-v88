"""darkhorse_radar.py — 【V88·黑马漏斗】把各发现模块产出整合，用唯一决策引擎复判出黑马

2026-07-17 用户定纲：系统推荐总在自选池里打转，没有"我不知道的黑马惊喜"；
各功能模块（猎手扫描/云端引擎榜/涨停接力/机会雷达/触底三大线）各自为战，
产出从没被送进"统一评分→入场时机→三段计划"主管道复判。本模块 = 那条漏斗：

  第1层 发现：云端引擎榜(engine_rank tops) ∪ 涨停接力候选 ∪ 机会雷达(会话传入)
  第2层 复判：排除自选/持仓(纯黑马) → 唯一决策引擎(统一分/概率/盈亏比/入场时机/相位闸门)
        触线标注：现价距 MA60/MA120/年线 ≤2% = 触底三大线（漏斗内属性，非独立源）
  第3层 计划：过严门槛者生成三段作战计划；多源共振置顶

用户拍板(2026-07-17)：纯黑马(排除自选持仓)/严门槛宁缺毋滥/今日导航第一屏。
漏斗数字透明：发现N→复判M→达标K，0只时能看到被谁拦下。纯确定性零AI成本。
"""
from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

BJT = timezone(timedelta(hours=8))
# 【两端同源】本文件在 StockAI/(桌面) 与 ai-daily-report-v2/src/(云端流水线) 各一份，
# 必须逐字节一致（decision_core 模式）。路径自适应两种环境：
_HERE = Path(__file__).resolve().parent
REPO = _HERE.parent if _HERE.name == "src" else Path.home() / "Desktop" / "ai-daily-report-v2"
if str(REPO / "src") not in sys.path:
    sys.path.insert(0, str(REPO / "src"))
HORSE_LOG = REPO / "journal" / "darkhorse_signals.json"
OUT = REPO / "data" / "darkhorse.json"
logger = logging.getLogger(__name__)

# 严门槛（用户拍板：宁缺毋滥）
MIN_SHORT = 58        # 2周方向分
MIN_RR = 1.2          # 盈亏比
BAD_STAGES = ("高位震荡", "放量滞涨", "趋势转弱", "破位下跌")
OK_MODES = ("现价可进", "回踩到位", "突破确认", "双路径待触发")
# 分级（2026-07-17 用户定纲：重点/待观察分色标出）
# 🔴重点 = 多源共振(≥2源) 或 (短线分≥65 且 rr≥1.5)；🟡待观察 = 其余达标者


def _grade_of(h: dict) -> str:
    if len(h.get("sources") or []) >= 2 or (
            float(h.get("short_score") or 0) >= 65 and float(h.get("rr") or 0) >= 1.5):
        return "重点"
    return "待观察"


def _canon(code: str) -> str:
    c = str(code or "").upper().split(".")[0]
    return c.lstrip("0") or c


def _market_of(code: str) -> str:
    c = str(code or "").upper()
    if c.endswith(".HK"):
        return "🇭🇰港股"
    if c.endswith((".SS", ".SZ", ".SH")):
        return "🇨🇳A股"
    return "🇺🇸美股"


def collect_candidates(extra: list | None = None) -> list[tuple]:
    """第1层发现：返回 [(yf_code, name, [来源,...]), ...]（同票多源合并）。"""
    from cloud_engine import to_yf
    pool: dict[str, dict] = {}

    def _add(code, name, source):
        if not code:
            return
        k = _canon(code)
        if k not in pool:
            pool[k] = {"code": code, "name": name, "sources": []}
        if source not in pool[k]["sources"]:
            pool[k]["sources"].append(source)

    # ① 云端引擎榜（3市场×3期限 Top5）
    try:
        rank = json.loads((REPO / "data" / "engine_rank.json").read_text(encoding="utf-8"))
        for mkt, horizons in (rank.get("tops") or {}).items():
            for hz, rows in (horizons or {}).items():
                for r in rows or []:
                    tok = str(r.get("token") or "")
                    code = to_yf(tok.split(":", 1)[1]) if ":" in tok else ""
                    _add(code, r.get("name"), f"引擎榜·{mkt}{hz}")
    except Exception as e:
        logger.debug(f"引擎榜源失败: {e}")
    # ② 涨停接力候选（A股短线情绪）
    try:
        zt = json.loads((REPO / "data" / "limit_up_radar.json").read_text(encoding="utf-8"))
        for r in zt.get("relay") or []:
            _add(to_yf(str(r.get("code") or "")), r.get("name"), "涨停接力")
    except Exception as e:
        logger.debug(f"涨停接力源失败: {e}")
    # ③ 全选大池（云端引擎同一池：美50/港30/A36≈116只——用户定纲：池子要足够大）
    try:
        import horizon_rank_cloud as _hrc
        import re as _re
        _pv = None
        for _name in dir(_hrc):
            _obj = getattr(_hrc, _name)
            if isinstance(_obj, dict) and all(isinstance(v, dict) and "codes" in v
                                              for v in _obj.values() or [{}]) and _obj:
                _pv = _obj
                break
        for _mkt, _blk in (_pv or {}).items():
            for _code, _nm in _blk.get("codes") or []:
                _add(_code, _nm, f"全选池·{_mkt}")
    except Exception as e:
        logger.debug(f"全选池源失败: {e}")
    # ④ 财报临近（未来7天美股财报·市值≥百亿）——财报=最硬的预期催化事由
    try:
        import requests
        _hdr = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        for _delta in range(1, 8):
            _d = (datetime.now(BJT) + timedelta(days=_delta))
            if _d.weekday() >= 5:
                continue
            _ds = _d.strftime("%Y-%m-%d")
            r = requests.get(f"https://api.nasdaq.com/api/calendar/earnings?date={_ds}",
                             headers=_hdr, timeout=12)
            for it in ((r.json().get("data") or {}).get("rows")) or []:
                _mc = str(it.get("marketCap") or "").replace("$", "").replace(",", "")
                if _mc.isdigit() and len(_mc) >= 11:      # ≥百亿美元
                    _add(str(it.get("symbol") or ""), it.get("name"),
                         f"财报{_d.strftime('%m-%d')}")
    except Exception as e:
        logger.debug(f"财报日历源失败: {e}")
    # ⑤ 机会雷达等会话内产出（由 app 传入 [(code,name,source),...]）
    for code, name, source in (extra or []):
        try:
            _add(to_yf(str(code)), name, source)
        except Exception:
            continue
    return [(v["code"], v["name"], v["sources"]) for v in pool.values()]


def _touch_lines(full: dict) -> str:
    """触底三大线标注：现价距 MA60/MA120/年线(MA250≈用120近似缺省) ≤2%。"""
    last = float(full.get("last") or 0)
    if not last:
        return ""
    ma = full.get("ma") or {}
    hits = []
    for key, label in ((55, "MA55"), (120, "MA120")):
        v = float(ma.get(key) or ma.get(str(key)) or 0)
        if v > 0 and abs(last - v) / v <= 0.02:
            hits.append(label)
    pos52 = float(full.get("pos52") or 50)
    if pos52 <= 25:
        hits.append(f"52周低位{pos52:.0f}%")
    return "触线:" + "+".join(hits) if hits else ""


def build_darkhorse(exclude_codes: set, extra: list | None = None,
                    max_judge: int = 180) -> dict:
    """整条漏斗。exclude_codes=自选+持仓的 canonical 集（纯黑马）。"""
    from cloud_engine import fetch, analyze_trend_full
    from v88_decision_core import evaluate_decision, evaluate_forward_outlook, build_trade_plan

    cands = collect_candidates(extra)
    found = len(cands)
    fresh = [(c, n, s) for c, n, s in cands if _canon(c) not in exclude_codes]
    excluded = found - len(fresh)
    fresh = fresh[:max_judge]
    horses, judged, blocked = [], 0, {"相位": 0, "方向分": 0, "赔率": 0, "时机": 0, "数据": 0}
    for code, name, sources in fresh:
        try:
            df = fetch(code)
            if df is None or len(df) < 40:
                blocked["数据"] += 1
                continue
            full = analyze_trend_full(df)
            judged += 1
            dc = evaluate_decision(df, full, name=name, code=code)
            if dc.get("error"):
                blocked["数据"] += 1
                continue
            if str(full.get("stage") or "") in BAD_STAGES:
                blocked["相位"] += 1
                continue
            if float(dc.get("short_score") or 0) < MIN_SHORT:
                blocked["方向分"] += 1
                continue
            if float(dc.get("rr") or 0) < MIN_RR:
                blocked["赔率"] += 1
                continue
            ep = dc.get("entry_plan") or {}
            if ep.get("mode") not in OK_MODES:
                blocked["时机"] += 1
                continue
            fwd = evaluate_forward_outlook(df, name=name, code=code, full=full)
            plan = build_trade_plan(full, ep, fwd if not fwd.get("error") else None)
            horses.append({**{k: dc.get(k) for k in
                              ("name", "code", "last", "unified_score", "short_score",
                               "p_up", "p_down", "rr", "expected_pct", "action",
                               "entry_note", "cycle_note", "analysis_time", "facts")},
                           "sources": sources, "touch": _touch_lines(full),
                           "trade_plan": plan, "scope": "黑马",
                           "level": "B", "market": _market_of(code)})
        except Exception:
            blocked["数据"] += 1
            continue
    # 分级 + 排序：🔴重点(多源共振/高分高赔率)在前，组内按短线分
    for h in horses:
        h["grade"] = _grade_of(h)
    horses.sort(key=lambda h: (0 if h.get("grade") == "重点" else 1,
                               -len(h.get("sources") or []),
                               -float(h.get("short_score") or 0)))
    # 🔴重点全部保留（用户反馈推荐不够,不截断重点）,🟡待观察最多8只
    _keys = [h for h in horses if h.get("grade") == "重点"]
    _rest = [h for h in horses if h.get("grade") != "重点"][:8]
    result = {
        "ts": time.time(),
        "generated_at": datetime.now(BJT).strftime("%Y-%m-%d %H:%M"),
        "funnel": {"found": found, "excluded_watch": excluded, "judged": judged,
                   "passed": len(horses), "blocked": blocked},
        "horses": _keys + _rest,
    }
    try:
        OUT.parent.mkdir(exist_ok=True)
        OUT.write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        pass
    _log_horses(result)
    return result


def default_exclude() -> set:
    """云端/无会话环境的排除集：私仓自选 + 持仓（纯黑马定纲）。"""
    excl = set()
    try:
        w = json.loads((REPO / "watchlist_v88.json").read_text(encoding="utf-8"))
        for lst in (w or {}).values():
            for it in lst or []:
                if isinstance(it, (list, tuple)) and it:
                    excl.add(_canon(str(it[0])))
    except Exception:
        pass
    try:
        pj = json.loads((REPO / "positions.json").read_text(encoding="utf-8"))
        for acc in (pj.get("accounts") or {}).values():
            for h in acc.get("holdings") or []:
                excl.add(_canon(str(h.get("code", ""))))
    except Exception:
        pass
    return excl


def refresh(force: bool = False) -> dict:
    """6小时节流：新鲜就读盘，过期才重跑大池漏斗（交易日日报流水线调用）。"""
    try:
        old = json.loads(OUT.read_text(encoding="utf-8"))
        if not force and time.time() - float(old.get("ts", 0)) < 6 * 3600 and old.get("horses") is not None:
            return old
    except Exception:
        pass
    return build_darkhorse(default_exclude())


def build_section() -> str:
    """日报段：🔴重点黑马表 + 🟡待观察行 + 漏斗透明数字。"""
    data = refresh()
    fn = data.get("funnel") or {}
    horses = data.get("horses") or []
    fn_txt = (f"发现{fn.get('found', 0)}→除自选持仓{fn.get('excluded_watch', 0)}"
              f"→复判{fn.get('judged', 0)}→达标{fn.get('passed', 0)}")
    lines = ["## 🐴 黑马雷达（全选池复判 · 纯黑马）", "",
             f"> 🕒 {data.get('generated_at', '')} · 漏斗:{fn_txt} · "
             "严门槛:2周分≥58+盈亏比≥1.2+非派发+时机在窗 · 概率=规则情景估计", ""]
    if not horses:
        blocked = "、".join(f"{k}{v}" for k, v in (fn.get("blocked") or {}).items() if v)
        lines.append(f"- 今日无达标黑马（拦截：{blocked or '候选不足'}）——宁缺毋滥。")
        return "\n".join(lines) + "\n\n---\n"
    keys_ = [h for h in horses if h.get("grade") == "重点"]
    watch_ = [h for h in horses if h.get("grade") != "重点"]
    if keys_:
        lines.append("**🔴 重点黑马**（多源共振或高分高赔率）")
        lines.append("| 黑马 | 来源 | 触线 | 短线分 | 盈亏比 | 短线计划 |")
        lines.append("|---|---|---|---|---|---|")
        for h in keys_[:5]:
            pl = ((h.get("trade_plan") or {}).get("short") or {})
            lines.append(f"| **{h.get('name')}**（{h.get('code')}） | {'＋'.join(h.get('sources') or [])[:28]} "
                         f"| {h.get('touch') or '—'} | {h.get('short_score')} | {h.get('rr')} "
                         f"| {str(pl.get('in', ''))[:46]} |")
        lines.append("")
    if watch_:
        lines.append("**🟡 待观察黑马**：" + "、".join(
            f"{h.get('name')}({h.get('short_score')}分)" for h in watch_[:6]))
    return "\n".join(lines) + "\n\n---\n"


def append_section() -> dict:
    """幂等插入日报（「## 五、」之前）。"""
    try:
        sec = build_section()
        fp = REPO / "data" / "daily_report.md"
        md = fp.read_text(encoding="utf-8")
        _mark = "## 🐴 黑马雷达"
        while _mark in md:
            i0 = md.find(_mark)
            j0 = md.find("\n## ", i0 + 5)
            md = md[:i0] + (md[j0 + 1:] if j0 > 0 else "")
        i = md.find("## 五、")
        md = (md[:i] + sec + md[i:]) if i > 0 else (md + "\n" + sec)
        fp.write_text(md, encoding="utf-8")
        return {"ok": True, "horses": len((json.loads(OUT.read_text(encoding='utf-8'))).get("horses") or [])}
    except Exception as e:
        return {"ok": False, "error": str(e)[:100]}


def weekly_block(days: int = 7) -> str:
    """周报段：下周重点关注黑马 = 近7天信号里出现≥2次的 + 最新一轮🔴重点。"""
    try:
        log = json.loads(HORSE_LOG.read_text(encoding="utf-8"))
    except Exception:
        log = []
    cutoff = (datetime.now(BJT) - timedelta(days=days)).strftime("%Y-%m-%d")
    recent = [r for r in log if str(r.get("date", "")) >= cutoff]
    freq: dict = {}
    for r in recent:
        k = f"{r.get('name')}（{r.get('code')}）"
        freq[k] = freq.get(k, 0) + 1
    repeat = [k for k, v in sorted(freq.items(), key=lambda x: -x[1]) if v >= 2]
    latest_keys = []
    try:
        cur = json.loads(OUT.read_text(encoding="utf-8"))
        latest_keys = [f"{h.get('name')}（{h.get('code')}）"
                       for h in (cur.get("horses") or []) if h.get("grade") == "重点"]
    except Exception:
        pass
    picks = list(dict.fromkeys(repeat + latest_keys))[:6]
    if not picks:
        return "- 🐴 下周重点关注黑马：本周漏斗无重复达标者，等新信号（宁缺毋滥）。"
    lines = ["- 🐴 **下周重点关注黑马**（近7天反复达标🔴优先）："]
    for k in picks:
        n = freq.get(k, 0)
        lines.append(f"  - {k}" + (f" ·本周{n}次达标" if n >= 2 else " ·最新一轮重点"))
    return "\n".join(lines)


def _log_horses(result: dict):
    """黑马也晒战绩：落盘供周报核算事后表现（同代码+日期只记一次）。"""
    try:
        horses = result.get("horses") or []
        if not horses:
            return
        today = datetime.now(BJT).strftime("%Y-%m-%d")
        try:
            log = json.loads(HORSE_LOG.read_text(encoding="utf-8"))
        except Exception:
            log = []
        seen = {r.get("id") for r in log}
        added = 0
        for h in horses:
            sid = f"{h.get('code')}:{today}"
            if sid in seen:
                continue
            log.append({"id": sid, "date": today, "code": h.get("code"),
                        "name": h.get("name"), "last": h.get("last"),
                        "short_score": h.get("short_score"), "rr": h.get("rr"),
                        "sources": h.get("sources")})
            seen.add(sid)
            added += 1
        if added:
            HORSE_LOG.parent.mkdir(exist_ok=True)
            HORSE_LOG.write_text(json.dumps(log, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        pass
