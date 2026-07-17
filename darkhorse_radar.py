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
from datetime import datetime, timedelta, timezone
from pathlib import Path

BJT = timezone(timedelta(hours=8))
REPO = Path.home() / "Desktop" / "ai-daily-report-v2"
HORSE_LOG = REPO / "journal" / "darkhorse_signals.json"
logger = logging.getLogger(__name__)

# 严门槛（用户拍板：宁缺毋滥）
MIN_SHORT = 58        # 2周方向分
MIN_RR = 1.2          # 盈亏比
BAD_STAGES = ("高位震荡", "放量滞涨", "趋势转弱", "破位下跌")
OK_MODES = ("现价可进", "回踩到位", "突破确认", "双路径待触发")


def _canon(code: str) -> str:
    c = str(code or "").upper().split(".")[0]
    return c.lstrip("0") or c


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
    # ③ 机会雷达等会话内产出（由 app 传入 [(code,name,source),...]）
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
                    max_judge: int = 50) -> dict:
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
                           "level": "B", "market": ""})
        except Exception:
            blocked["数据"] += 1
            continue
    # 多源共振置顶，其次 2周方向分
    horses.sort(key=lambda h: (-len(h.get("sources") or []), -float(h.get("short_score") or 0)))
    result = {
        "generated_at": datetime.now(BJT).strftime("%Y-%m-%d %H:%M"),
        "funnel": {"found": found, "excluded_watch": excluded, "judged": judged,
                   "passed": len(horses), "blocked": blocked},
        "horses": horses[:6],
    }
    _log_horses(result)
    return result


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
