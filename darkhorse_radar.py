"""Read-only discovery evidence linked to the sole central 3A authority.

Discovery routes and technical scores nominate research identities only. They
never grant grades, trades, probabilities or targets. UI reads cached evidence;
background collection alone may fetch data. Keep desktop copy byte-identical.
"""
from __future__ import annotations

import json
import math
import copy
import hashlib
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

VERSION = "darkhorse-central-reference-v2"
BUCKETS = ("recommendations", "preparations", "blocked_3a", "conditional",
           "observations", "pending", "excluded")
DISCOVERY_TTL_SECONDS = 6 * 3600


def _canon(code: str) -> str:
    # Do not drop an exchange or a US share class: 000001.SS != 000001.SZ.
    from grade_focus import canonical
    return canonical(code)


def _market_of(code: str) -> str:
    from grade_focus import market_of
    return {"A股": "🇨🇳A股", "港股": "🇭🇰港股", "美股": "🇺🇸美股"}.get(
        market_of(_canon(code)), "身份待核")


def collect_candidates(extra: list | None = None) -> list[tuple]:
    """第1层发现：返回 [(yf_code, name, [来源,...]), ...]（同票多源合并）。"""
    from cloud_engine import to_yf
    pool: dict[str, dict] = {}

    def _add(code, name, source):
        if not code:
            return
        k = _canon(code)
        if _market_of(k) == "身份待核":
            return
        if k not in pool:
            pool[k] = {"code": k, "name": name, "sources": []}
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
    # ④ 已有财报日历的身份线索；不在发现模块联网猜催化。
    try:
        calendar = json.loads((REPO / "data" / "earnings_calendar.json").read_text())
        now = datetime.now(BJT)
        for it in calendar.get("rows") or []:
            at = _stamp(it.get("earnings_date"))
            if at and 0 <= (at.date()-now.date()).days <= 7:
                _add(it.get("code"), it.get("name"), "日历身份线索·需核验公告")
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
    """Observed proximity only; MA120 is never a substitute for an annual line."""
    last = float(full.get("last") or 0)
    if not last:
        return ""
    ma = full.get("ma") or {}
    hits = []
    for key, label in ((55, "MA55"), (120, "MA120")):
        v = float(ma.get(key) or ma.get(str(key)) or 0)
        if v > 0 and abs(last - v) / v <= 0.02:
            hits.append(label)
    pos52 = float(full.get("pos52") if full.get("pos52") is not None else 50)
    if pos52 <= 25:
        hits.append(f"52周低位{pos52:.0f}%")
    return "触线:" + "+".join(hits) if hits else ""


def _number(value):
    return value if type(value) in (int, float) and math.isfinite(value) else None


def _stamp(value):
    from review_display import BJT
    if type(value) in (int, float) and math.isfinite(value):
        try: return datetime.fromtimestamp(value, BJT)
        except (OverflowError, OSError, ValueError): return None
    try:
        at = datetime.fromisoformat(str(value).replace("（北京时间）", "").strip().replace("Z", "+00:00"))
        return at if at.tzinfo else at.replace(tzinfo=BJT)
    except (TypeError, ValueError): return None


def discovery_rows(document):
    """Normalize new and archived discovery payloads without inheriting authority.

    The legacy horses/runners remain readable as observations; their old action,
    trade_plan, grade, target and p_up probability names do not survive this view.
    """
    source = document.get("discovery_candidates")
    if source is None:
        source = list(document.get("horses") or []) + list(document.get("runners") or [])
    found = {}
    for raw in source or []:
        if not isinstance(raw, dict): continue
        code = _canon(raw.get("code"))
        if _market_of(code) == "身份待核": continue
        channels = sorted({str(x) for x in raw.get("discovery_channels", raw.get("sources")) or [] if x})
        aux = raw.get("auxiliary") or {}
        old_horizons = raw.get("horizons") or []
        horizons = aux.get("horizon_direction_scores") or [
            {"label": str(h.get("label")), "score": _number(h.get("p_up"))}
            for h in old_horizons if isinstance(h, dict)]
        row = {"code": code, "name": str(raw.get("name") or code),
               "market": _market_of(code), "last": _number(raw.get("last")),
               "source_asof": raw.get("source_asof"),
               "source_provider": raw.get("source_provider"), "price_basis": raw.get("price_basis"),
               "discovery_generated_at": raw.get("discovery_generated_at") or document.get("generated_at"),
               "discovery_channels": channels,
               "independent_source_count": None,
               "source_independence_note": "发现渠道不是独立数据源；原始供应商独立性未经核实，不计多源通过票。",
               "auxiliary": {
                   "quant_score": _number(aux.get("quant_score", raw.get("unified_score"))),
                   "short_direction_score": _number(aux.get("short_direction_score", raw.get("short_score"))),
                   "technical_gross_rr": _number(aux.get("technical_gross_rr", raw.get("rr"))),
                   "stage": str(aux.get("stage", raw.get("stage")) or ""),
                   "touch": str(aux.get("touch", raw.get("touch")) or ""),
                   "horizon_direction_scores": [{"label": str(h.get("label")), "score": _number(h.get("score"))}
                       for h in horizons if isinstance(h, dict)],
                   "calibrated_probability": False, "decision_weight": 0},
               "legacy_filter_reason": str(raw.get("legacy_filter_reason") or raw.get("why_blocked") or ""),
               "no_grade_authority": True, "executable": False, "push_eligible": False}
        if code in found:
            found[code]["discovery_channels"] = sorted(set(found[code]["discovery_channels"]) | set(channels))
        else: found[code] = row
    return list(found.values())


def nomination_rows(document, *, now=None):
    """Identity-only input to the existing fact-pack queue; no old prices/targets.

    Dates describe the discovery event. Fresh market facts must still be gathered,
    frozen and independently reviewed by the central pipeline before any grade.
    """
    now = now or datetime.now(BJT)
    at = _stamp(document.get("generated_at"))
    if not at or not 0 <= (now-at).total_seconds() <= DISCOVERY_TTL_SECONDS:
        return []
    return [{"code": r["code"], "name": r["name"],
             "market": r["market"].replace("🇨🇳", "").replace("🇺🇸", "").replace("🇭🇰", ""),
             "extra_evidence": "跨模块发现身份；渠道数量不作独立来源计票；原量价评分/旧目标/旧动作不参与中央裁决。"}
            for r in discovery_rows(document)]


def _central_context(base, now):
    """Use the actual central freshness/review checks, never persisted rank flags."""
    import recommendation_gate as gate
    # Gate's cache is rooted in the configured V88 core. Refuse another base;
    # tests of pure projection inject an explicit context below instead.
    if Path(base).resolve() != Path(gate.BASE).resolve():
        raise ValueError("central gate base mismatch")
    def generation():
        stamps=[]
        for name in ('triad_selection.json','review_factpack.json','gpt_verify.json','classics_lens.json'):
            try:
                state=(Path(base)/'data'/name).stat();stamps.append((name,state.st_mtime_ns,state.st_size))
            except OSError:stamps.append((name,None,None))
        return stamps
    before=generation()
    selection = gate._load("triad_selection.json")
    current = gate.selection_runtime_valid(selection, now)
    rows = gate.current_publishable(selection, now) + gate.current_observations(selection, now) if current else []
    if current:
        rows += [r for r in selection.get("preparations", []) if gate.quality_is_current(r, selection, now)]
    if before != generation():raise ValueError('central generation changed during discovery projection')
    return selection, rows, current


def project(document, selection, current_rows, *, now=None, central_current=True):
    """Pure linkage view. A 99-point technical clue cannot outrank a 1A review.

    Current central grades follow grade_focus. Unqualified identities are listed
    by code; no market quota or auxiliary score manufactures a recommendation.
    """
    from grade_focus import build as focus_build, GRADES
    from stock_reference import reference
    from review_display import current_scorecard
    now = now or datetime.now(BJT)
    discovery_at = _stamp(document.get("generated_at"))
    discovery_fresh = bool(discovery_at and 0 <= (now-discovery_at).total_seconds() <= DISCOVERY_TTL_SECONDS)
    live = {_canon(r.get("code")): r for r in current_rows if r.get("tier") in GRADES} if central_current else {}
    focus = focus_build(list(live.values()), now=now)
    by_code = {_canon(r.get("code")): r for bucket in BUCKETS for r in selection.get(bucket) or []}
    out = []
    for observed in discovery_rows(document):
        code = observed["code"]; raw = by_code.get(code); current = live.get(code)
        card = current_scorecard(selection, raw, now=now) if raw and central_current else {}
        complete = bool((card.get("gpt") or {}).get("current") and (card.get("gpt") or {}).get("complete"))
        complete = complete and bool((card.get("books") or {}).get("current") and card.get("double_audit_complete"))
        rank = focus["records"].get(code) or {}
        value = (raw or {}).get("value_assessment") or {}
        gaps = list((raw or {}).get("reason_codes") or [])
        reason = str(value.get("stage_note") or value.get("value_status") or "")
        if current:
            status = "中央" + current["tier"] + "·" + ("重点榜" if rank.get("selected") else "保留跟踪")
            reason = rank.get("reason") or reason
        elif raw and complete:
            status = "中央已复审·未授级"
            reason = reason or "双审后未达到中央价值与合同条件"
        elif raw:
            status = "中央待补证复审·未授级"
            reason = reason or "当前双审缺失、过期或事实不完整"
        else:
            status = "发现线索·中央尚无裁决"
            reason = "不在当前中央裁决列表；按真实索引核对待审状态，不继承辅助评分"
        if not central_current:
            status = "中央证据待更新·未授级"
            reason = "中央快照或同版审核无法核实，保留原合同但不沿用当前等级"
        if not discovery_fresh:
            gaps.insert(0, "发现记录超过6小时或时间未知；仅保留原记录")
        if not observed.get("source_asof"):
            gaps.insert(0, "发现记录未保存行情源日期，不能把计算时间当作行情日期")
        central = {"status": status, "current": bool(current), "fresh": bool(central_current and complete),
                   "tier": current.get("tier") if current else None,
                   "audit_score": current.get("audit_score", (current.get("scorecard") or {}).get("total")) if current else card.get("total") if complete else None,
                   "review_complete": complete, "rank": rank.get("central_rank"),
                   "focus_selected": bool(rank.get("selected")), "reason": reason,
                   "trade_plan": copy.deepcopy((raw or {}).get("trade_plan") or {}),
                   "horizon": (raw or {}).get("horizon"),
                   "master_ref": reference(selection, raw) if raw else None,
                   "reviewed_at": (((raw or {}).get("reviews") or {}).get("gpt") or {}).get("at")}
        out.append({**observed, "name": (raw or {}).get("name") or observed["name"],
                    "central": central, "gaps": list(dict.fromkeys(gaps)),
                    "next_step": "；".join([reason, "发现只提名身份→更新真实事实→冻结事实包→GPT主审与独立反审/经典核验→中央发布；旧分数不继承"]),
                    "central_order_only": True})
    tier_order = {"3A": 0, "2A": 1, "1A": 2}
    out.sort(key=lambda r: (tier_order.get(r["central"]["tier"], 3),
                          r["market"], r["central"]["rank"] or 10**9, r["code"]))
    return {"version": VERSION, "generated_at": now.isoformat(),
            "discovery_generated_at": document.get("generated_at"), "discovery_fresh": discovery_fresh,
            "central_generated_at": selection.get("generated_at"), "factpack_id": selection.get("factpack_id"),
            "central_current": central_current, "funnel": copy.deepcopy(document.get("funnel") or {}), "rows": out,
            "summary": {"discovered": len(out), "central_linked": sum(bool(r["central"]["master_ref"]) for r in out),
                        "current_graded": len([r for r in out if r["central"]["current"]]),
                        "needs_review": sum(not r["central"]["current"] for r in out)},
            "no_grade_authority": True, "model_calls": 0, "network_calls": 0}


def load_projection(base=None, *, now=None):
    """UI entry: local reads only, every call rebinds to the current central pack."""
    base = Path(base or REPO); now = now or datetime.now(BJT)
    path = base / "data" / "darkhorse.json"
    try: document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError): document = {}
    for attempt in range(2):
        try:
            selection, current, valid = _central_context(base, now)
            return project(document, selection, current, now=now, central_current=valid)
        except ValueError as exc:
            if str(exc) != 'central generation changed during discovery projection':raise
            if attempt:
                view=project(document, {}, [], now=now, central_current=False)
                view['projection_error']='中央发布正在更新；已保留发现身份，未沿用旧等级，请稍后复核'
                return view


def _verified_frame(code, *, now=None):
    """Reuse receipt-backed completed daily bars; a local refresh is no new quote."""
    from verified_history import read
    from exchange_sessions import latest_completed
    from grade_focus import market_of
    import pandas as pd
    now = now or datetime.now(BJT)
    frame = read(code, (now-timedelta(days=730)).date().isoformat(), now.date().isoformat(), db=REPO/'data/wdata.db')
    if frame.empty or len(frame) < 40: raise ValueError("缺少已核验完整日线，保留补数队列")
    expected = latest_completed(market_of(_canon(code)), now).isoformat()
    if str(frame.index[-1])[:10] != expected: raise ValueError("已核验日线未到应有收盘日："+expected)
    if not frame.attrs.get('price_basis') or frame.attrs.get('price_basis') == 'unknown':
        raise ValueError("日线复权口径未核实")
    attrs = dict(frame.attrs)
    frame = frame.rename(columns={'open': 'Open', 'close': 'Close', 'high': 'High', 'low': 'Low', 'volume': 'Volume'})
    frame.index = pd.to_datetime(frame.index); frame.attrs.update(attrs)
    return frame


def build_darkhorse(exclude_codes: set, extra: list | None = None,
                    max_judge: int = 240) -> dict:
    """Background-only collection. Counts are a discovery subset, never coverage.

    The old 62-point/two-module/1.2-RR gate had no central review authority and is
    retired. Keep auxiliary observations, not another 'passed horses' leaderboard.
    """
    from cloud_engine import analyze_trend_full
    from v88_decision_core import evaluate_decision
    from review_factpack import atomic_json
    cands = collect_candidates(extra); excludes = {_canon(c) for c in exclude_codes}
    fresh = [(c, n, s) for c, n, s in cands if _canon(c) not in excludes]
    queues = {m: [] for m in ("🇨🇳A股", "🇺🇸美股", "🇭🇰港股")}
    for row in sorted(fresh, key=lambda r: _canon(r[0])):
        queues[_market_of(row[0])].append(row)
    interleaved = []
    for i in range(max([len(q) for q in queues.values()] or [0])):
        interleaved += [q[i] for q in queues.values() if i < len(q)]
    rows = []; errors = []; counts = {m: 0 for m in queues}
    generated_at = datetime.now(BJT).isoformat()
    for code, name, channels in interleaved[:max(0, max_judge)]:
        counts[_market_of(code)] += 1
        try:
            df = _verified_frame(code)
            if df is None or len(df) < 40: raise ValueError("日线不足40根")
            full = analyze_trend_full(df)
            dc = evaluate_decision(df, full, name=name, code=code)
            if dc.get("error"): raise ValueError(str(dc["error"]))
            rows.append({"code": code, "name": name, "last": _number(dc.get("last")),
                "source_asof": str(df.index[-1]), "discovery_generated_at": generated_at,
                "source_provider": df.attrs.get('provider_source'), "price_basis": df.attrs.get('price_basis'),
                "discovery_channels": channels,
                "auxiliary": {"quant_score": _number(dc.get("unified_score")),
                    "short_direction_score": _number(dc.get("short_score")),
                    "technical_gross_rr": _number(dc.get("rr")),
                    "stage": str(full.get("stage") or ""), "touch": _touch_lines(full)},
                "no_grade_authority": True, "executable": False, "push_eligible": False})
        except Exception as exc:
            errors.append({"code": code, "reason": str(exc)[:180]})
    result = {"version": VERSION, "ts": time.time(), "generated_at": generated_at,
              "selection_authority": "triad_selection.json only",
              "scope": "bounded discovery subset; not whole-market coverage",
              "funnel": {"found": len(cands), "excluded_watch": len(cands)-len(fresh),
                  "attempted": sum(counts.values()), "attempted_by_mkt": counts,
                  "judged": len(rows), "judged_by_mkt": {market:sum(_market_of(r['code'])==market for r in rows) for market in counts},
                  "deferred": max(0, len(interleaved)-max(0, max_judge)), "passed": 0,
                  "blocked": {"数据": len(errors)}},
              "discovery_candidates": discovery_rows({"generated_at": generated_at, "discovery_candidates": rows}),
              "errors": errors, "horses": [], "runners": [],
              "no_grade_authority": True, "model_calls": 0}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    if OUT.exists():
        previous = OUT.read_bytes(); archive = OUT.parent / "darkhorse_history" / (hashlib.sha256(previous).hexdigest()+".json")
        if not archive.exists():
            archive.parent.mkdir(parents=True, exist_ok=True); archive.write_bytes(previous)
    atomic_json(OUT, result)
    refresh_public()
    refresh_review_queue()
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
    """Background scheduler entry; UI calls load_projection, never this collector."""
    old = {}
    try:
        old = json.loads(OUT.read_text(encoding="utf-8"))
        if not force and old.get("version") == VERSION and 0 <= time.time()-float(old.get("ts", 0)) < DISCOVERY_TTL_SECONDS:
            return old
    except (OSError, ValueError, TypeError): pass
    retained = [(r['code'], r['name'], '原发现身份·重新核对行情') for r in discovery_rows(old)]
    return build_darkhorse(default_exclude(), extra=retained)


def review_priority_codes(base=None, *, now=None):
    """Review attention only. No fixed quota, model calls or admission mutation."""
    base = Path(base or REPO); now = now or datetime.now(BJT)
    try: doc = json.loads((base/'data/darkhorse.json').read_text())
    except (OSError, ValueError): return set()
    if doc.get('version') != VERSION: return set()
    identities = {r['code'] for r in nomination_rows(doc, now=now)}
    from exchange_sessions import latest_completed
    from grade_focus import market_of
    return {r['code'] for r in discovery_rows(doc) if r['code'] in identities
            and str(r.get('source_asof') or '')[:10] == latest_completed(market_of(r['code']), now).isoformat()}


def refresh_review_queue(base=None, *, now=None):
    """Record actual full-directory queue status; never append placeholder facts."""
    from full_market_research import plan
    from review_factpack import atomic_json
    base = Path(base or REPO); now = now or datetime.now(BJT)
    report, _ = plan(base=base, limit=0, now=now)
    wanted = review_priority_codes(base, now=now)
    rows = [r for r in report['members'] if r['code'] in wanted]
    known = {r['code'] for r in rows}
    rows += [{'code': c, 'status': 'not_in_verified_index', 'no_grade_authority': True} for c in sorted(wanted-known)]
    import recommendation_gate as gate
    from joint_evidence import indexed_fact, due_codes
    due=due_codes(base, max(1000,len(wanted)))
    payloads={}
    for row in rows:
        try:payloads[row['code']]=indexed_fact(base,row['code'])
        except (ValueError,KeyError):continue
    eligible={r['code'] for r in gate.thesis_review_pool({'items':list(payloads.values())},now)}
    for row in rows:
        code=row['code']; row['scheduled_joint_due']=code in due
        row['review_queue_rank']=due.index(code)+1 if code in due else None
        candidate=payloads.get(code)
        if candidate:
            source_ok, source_reason=gate._thesis_source_ready(candidate,now)
            identity_ok, identity_reason, _=gate.identifier_check(candidate.get('code'),candidate.get('name'))
            row['source_ready_for_review']=code in eligible
            row['review_lane_checks']={'source_current':source_ok,'source_reason':source_reason,
                'identity_valid':identity_ok,'identity_reason':identity_reason if not identity_ok else None,
                'instrument_scope_ok':not bool(gate.unsupported_contract(candidate))}
            row['source_asof']=candidate.get('source_generated_at')
        else:
            row['source_ready_for_review']=False
            row['review_lane_checks']={'reason':'缺少可校验的当前索引事实'}
        row['next_step']='按既有预算进入实际双审；已有合同先经保留合同的事实刷新检查' if row['scheduled_joint_due'] else '保留索引身份；需先解决事实/身份或审核冷却条件，不能自动授级'
    result = {'version': VERSION, 'generated_at': now.isoformat(),
        'snapshot_receipt': report['snapshot_receipt'], 'discovery_count': len(wanted), 'rows': rows,
        'model_calls': 0, 'no_grade_authority': True, 'automatic_entry': 'module_refresh.darkhorse → full_market_research.plan / joint_evidence.due_codes → existing bounded real-review budget'}
    atomic_json(base/'data/darkhorse_review_queue.json', result)
    return result


def refresh_public(base=None, *, now=None):
    """Local privacy-safe shared view; no transport or messages."""
    from publish_public import _private_identities, _pub_canon
    from feishu_projection import assert_private_keys_absent
    from review_factpack import atomic_json
    base = Path(base or REPO); result = load_projection(base, now=now)
    private_codes, private_names = _private_identities()
    result['rows'] = [r for r in result['rows'] if _pub_canon(r['code']) not in private_codes and r['name'] not in private_names]
    result['summary'] = {'discovered': len(result['rows']),
        'central_linked': sum(bool(r['central']['master_ref']) for r in result['rows']),
        'current_graded': sum(r['central']['current'] for r in result['rows']),
        'needs_review': sum(not r['central']['current'] for r in result['rows'])}
    assert_private_keys_absent(result)
    atomic_json(base/'data/darkhorse_pub.json', result)
    return result


def build_section() -> str:
    """Reports link the shared authority, never legacy short-term plans."""
    view = load_projection(); rows = view["rows"]
    lines = ["## 🐴 黑马雷达 · 跨模块发现核对", "",
             f"> 发现记录：{view.get('discovery_generated_at') or '未知'}；中央版：{view.get('central_generated_at') or '未知'}。",
             "> 发现分不授级；当前等级、原合同与3A系统相同，详细结果以中央列表为准。", ""]
    for r in rows[:15]:
        c = r["central"]; score = c.get("audit_score")
        lines.append(f"- {r['name']}（{r['code']}）：{c['status']}；审核分 {score if score is not None else '未形成'}。{c['reason']}")
    if len(rows) > 15: lines.append(f"- 其余{len(rows)-15}条发现线索保留在模块明细，未增加推荐名额。")
    if not rows: lines.append("- 暂无可读取的发现线索；中央评级独立保留。")
    return "\n".join(lines) + "\n\n---\n"


def append_section() -> dict:
    """Compatibility hook: reports are now composed by the central contract."""
    return {"ok": True, "appended": False, "reason": "仅由中央报告合同输出推荐；发现模块无追加推荐权限"}


def weekly_block(days: int = 7) -> str:
    # Preserve the historical journal on disk; repeated old signals do not become
    # future recommendations and are not evidence of a verified strategy edge.
    return "- 🐴 跨模块发现记录仅供核对；下周研究候选与原合同统一引用中央周度列表。"
