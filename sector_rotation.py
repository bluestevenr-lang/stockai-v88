"""Dated sector-proxy relative strength; no capital-flow or trading authority.

Only aligned completed sessions are comparable. Daily state observations are
append-only, including same-session corrections, and are never trade signals.
"""
from copy import deepcopy
from datetime import date, datetime, timezone
import hashlib
import json
import math

from exchange_sessions import latest_completed, is_session, next_session

VERSION = "sector-rotation-observation-v1"
BENCHMARKS = {"美股": ("^GSPC", "标普500"), "A股": ("000001.SS", "上证指数"),
              "港股": ("^HSI", "恒生指数")}
PROXIES = {
    "美股": [("XLK", "科技"), ("XLC", "通信"), ("XLY", "可选消费"), ("XLP", "必选消费"),
             ("XLF", "金融"), ("XLV", "医疗"), ("XLE", "能源"), ("XLI", "工业"),
             ("XLB", "材料"), ("XLU", "公用事业"), ("XLRE", "房地产")],
    "A股": [("512760.SS", "半导体芯片"), ("515030.SS", "新能源车"), ("512690.SS", "白酒"),
           ("512010.SS", "医药"), ("512880.SS", "证券"), ("512800.SS", "银行"),
           ("512400.SS", "有色金属"), ("512660.SS", "军工"), ("159928.SZ", "消费"),
           ("515220.SS", "煤炭")],
    "港股": [("3033.HK", "恒生科技"), ("2828.HK", "国企蓝筹"), ("3110.HK", "高股息")],
}
STATES = ("持续占优", "短期改善", "短期转弱", "持续落后", "中性")


def _number(value):
    if value is None or isinstance(value, bool):
        raise ValueError("涨幅缺失或无效")
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("涨幅缺失或无效") from None
    if not math.isfinite(value):
        raise ValueError("涨幅缺失或无效")
    return value


def _day(value):
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value[:10]).isoformat()
    except ValueError:
        return None


def _state(short, medium):
    short, medium = round(short, 6), round(medium, 6)
    if short == 0 or medium == 0:
        return "中性"
    return ("持续占优" if short > 0 else "短期转弱") if medium > 0 else ("短期改善" if short > 0 else "持续落后")


def _daily(series, market):
    if not isinstance(series, list):
        raise ValueError("日线结构无效")
    out = {}
    for point in series:
        if not isinstance(point, dict):
            raise ValueError("日线结构无效")
        day = _day(point.get("date"))
        value = _number(point.get("close"))
        if day is None or day in out or value <= 0 or not is_session(day, market):
            raise ValueError("日线日期、重复记录或价格无效")
        out[day] = value
    return out


def _metrics(sector, benchmark, market, expected):
    """Prefer shared daily windows; otherwise explicitly use same-date summaries."""
    if isinstance(sector.get("daily_closes"), list) and isinstance(benchmark.get("daily_closes"), list):
        left = _daily(sector["daily_closes"], market)
        right = _daily(benchmark["daily_closes"], market)
        common = sorted(set(left) & set(right))
        if left and right and (not common or common[-1] != expected):
            raise ValueError("共同日线截止未到最近完整交易日")
        if len(common) >= 21:
            window = common[-21:]
            if any(next_session(date.fromisoformat(a), market).isoformat() != b
                   for a, b in zip(window, window[1:])):
                raise ValueError("共同日线窗口缺交易日")
            values = [_number((prices[window[-1]] / prices[window[index]] - 1) * 100)
                      for prices in (left, right) for index in (-6, -21)]
            return values, "aligned_daily_closes", expected, expected, window[-6], window[-21]
    source, baseline = _day(sector.get("source_asof")), _day(benchmark.get("source_asof"))
    if source is None or baseline is None:
        raise ValueError("代理或基准行情日期缺失")
    if source != baseline:
        raise ValueError("代理与基准行情日期不一致")
    if source != expected:
        raise ValueError("行情未到最近完整交易日")
    values = [_number(row.get(field)) for row in (sector, benchmark) for field in ("chg5d", "chg20d")]
    return values, "same_session_summary", source, baseline, None, None


def _transition(row, history):
    prior = [r for r in history if r.get("version") == VERSION and r.get("market") == row["market"]
             and r.get("symbol") == row["symbol"] and r.get("benchmark_symbol") == row["benchmark_symbol"]
             and r.get("quadrant") in STATES and _day(r.get("source_asof"))
             and r["source_asof"] < row["source_asof"]]
    before = max(enumerate(prior), key=lambda pair: (pair[1]["source_asof"], pair[0]))[1] if prior else None
    out = {"status": "first_observation", "from_state": None, "to_state": row["quadrant"],
           "prior_session": None, "since_session": row["source_asof"],
           "consecutive_sessions": 1, "confirmed": False}
    if not before:
        return out
    out.update(from_state=before["quadrant"], prior_session=before["source_asof"])
    try:
        consecutive = next_session(date.fromisoformat(before["source_asof"]), row["market"]).isoformat() == row["source_asof"]
    except ValueError:
        consecutive = False
    if not consecutive:
        return dict(out, status="nonconsecutive")
    if before["quadrant"] != row["quadrant"]:
        return dict(out, status="pending_confirmation")
    previous = before.get("transition") or {}
    if previous.get("status") in ("pending_confirmation", "confirmed") and previous.get("from_state") != row["quadrant"]:
        count = int(previous.get("consecutive_sessions") or 1) + 1
        return dict(out, status="confirmed", from_state=previous.get("from_state"),
                    since_session=previous.get("since_session"), consecutive_sessions=count, confirmed=True)
    return dict(out, status="unchanged")


def _latest(history, market, symbol, benchmark):
    rows = [r for r in history if r.get("version") == VERSION and r.get("market") == market
            and r.get("symbol") == symbol and r.get("benchmark_symbol") == benchmark
            and _day(r.get("source_asof")) and r.get("quadrant") in STATES]
    return max(enumerate(rows), key=lambda pair: (pair[1]["source_asof"], pair[0]))[1] if rows else None


ROW_FIELDS = ("symbol", "name", "market", "benchmark_symbol", "status", "reason", "source_asof",
              "benchmark_asof", "expected_session", "basis", "sector_chg5d", "sector_chg20d",
              "benchmark_chg5d", "benchmark_chg20d", "rs5_pp", "rs20_pp", "quadrant",
              "window_start5", "window_start20")
TRANSITION_FIELDS = ("status", "from_state", "to_state", "prior_session", "since_session",
                     "consecutive_sessions", "confirmed")


def _pick(value, keys):
    """Only scalar values at explicitly permitted paths can leave this module."""
    if not isinstance(value, dict):
        return {}
    return {k: value[k] for k in keys if k in value and
            (value[k] is None or isinstance(value[k], (str, bool, int)) or
             isinstance(value[k], float) and math.isfinite(value[k]))}


def _project_row(row, *, historical=False):
    keys = ROW_FIELDS + (("version", "observation_id", "observed_at") if historical else ())
    out = _pick(row, keys)
    out["transition"] = _pick(row.get("transition") if isinstance(row, dict) else {}, TRANSITION_FIELDS)
    return out


def public_projection(doc):
    """Explicit recursive public allowlist; no raw series, holdings or arbitrary dictionaries."""
    if not isinstance(doc, dict):
        return {}
    out = _pick(doc, ("version", "generated_at", "method", "no_grade_authority", "entry_permission", "capital_flow_status"))
    out["markets"] = {}
    for market in BENCHMARKS:
        source = (doc.get("markets") or {}).get(market) if isinstance(doc.get("markets"), dict) else None
        if not isinstance(source, dict):
            continue
        projected = _pick(source, ("market", "expected_session", "proxy_kind", "note"))
        projected["benchmark"] = _pick(source.get("benchmark"), ("symbol", "name", "source_asof", "chg5d", "chg20d"))
        projected["coverage"] = _pick(source.get("coverage"), ("expected", "received", "current", "eligible", "pending", "historical"))
        projected["rows"] = [_project_row(r) for r in source.get("rows", []) if isinstance(r, dict)]
        out["markets"][market] = projected
    out["history"] = [_project_row(r, historical=True) for r in doc.get("history", []) if isinstance(r, dict)]
    return out


def build_rotation_observation(markets, previous=None, now=None):
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("轮动计算时间必须标明时区")
    previous = previous if isinstance(previous, dict) else {}
    # Retain older versions as evidence, but only this version may confirm a change.
    history = deepcopy(previous.get("history")) if isinstance(previous.get("history"), list) else []
    history = [r for r in history if isinstance(r, dict)]
    result = {"version": VERSION, "generated_at": now.isoformat(),
              "method": "同市场同截止日代理5/20交易日涨幅减固定基准，单位百分点；跨日状态变化须连续两个交易日确认。",
              "no_grade_authority": True, "entry_permission": False,
              "capital_flow_status": "not_evaluated", "markets": {}, "history": history}
    for market, (benchmark_symbol, benchmark_name) in BENCHMARKS.items():
        try:
            expected = latest_completed(market, now).isoformat()
        except ValueError:
            expected = None
        source = markets.get(market) or {}
        sectors = [r for r in source.get("sectors", []) if isinstance(r, dict)]
        indices = [r for r in source.get("indices", []) if isinstance(r, dict)]
        benchmarks = [r for r in indices if r.get("symbol") == benchmark_symbol]
        benchmark = benchmarks[0] if len(benchmarks) == 1 else {}
        benchmark_meta = {"symbol": benchmark_symbol, "name": benchmark_name,
                          "source_asof": _day(benchmark.get("source_asof"))}
        for field in ("chg5d", "chg20d"):
            try:
                benchmark_meta[field] = _number(benchmark.get(field))
            except ValueError:
                benchmark_meta[field] = None
        rows = []
        for symbol, name in sorted(PROXIES[market]):
            matches = [r for r in sectors if r.get("symbol") == symbol]
            raw = matches[0] if len(matches) == 1 else {}
            old = _latest(history, market, symbol, benchmark_symbol)
            row = {k: None for k in ROW_FIELDS}
            row.update(symbol=symbol, name=name, market=market, benchmark_symbol=benchmark_symbol,
                       status="pending", expected_session=expected,
                       source_asof=_day(raw.get("source_asof")), benchmark_asof=benchmark_meta["source_asof"])
            try:
                if expected is None:
                    raise ValueError("当前年度交易日历未核对")
                if len(matches) != 1:
                    raise ValueError("代理行情缺失" if not matches else "代理代码重复")
                if len(benchmarks) != 1:
                    raise ValueError("固定基准行情缺失" if not benchmarks else "基准代码重复")
                values, basis, asof, baseline, start5, start20 = _metrics(raw, benchmark, market, expected)
                if old and old["source_asof"] > asof:
                    raise ValueError("行情版本回退，保留较新历史观察")
                s5, s20, b5, b20 = values
                rs5, rs20 = round(s5-b5, 6), round(s20-b20, 6)
                row.update(status="current", reason="当前同日相对强弱；非资金流或交易授权", source_asof=asof,
                           benchmark_asof=baseline, basis=basis, sector_chg5d=round(s5, 6), sector_chg20d=round(s20, 6),
                           benchmark_chg5d=round(b5, 6), benchmark_chg20d=round(b20, 6),
                           rs5_pp=round(rs5, 6), rs20_pp=round(rs20, 6), quadrant=_state(rs5, rs20),
                           window_start5=start5, window_start20=start20)
                row["transition"] = _transition(row, history)
                signature = {k:v for k,v in row.items() if k not in ("reason", "transition")}
                identity = hashlib.sha256(json.dumps(signature, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
                # A same-session correction can later be reverted. Compare with
                # the latest observation, not any earlier identical contents,
                # so tomorrow uses the last actually observed state.
                if not old or old.get("observation_id") != identity:
                    history.append({**deepcopy(row), "version": VERSION, "observation_id": identity, "observed_at": now.isoformat()})
            except (ValueError, TypeError, OverflowError) as exc:
                reason = str(exc)
                if old:
                    row.update({k:old.get(k) for k in ROW_FIELDS if k not in
                                ("status", "reason", "expected_session", "symbol", "name", "market", "benchmark_symbol")})
                    row["status"] = "historical"
                row["reason"] = reason
                row["transition"] = {"status": "source_unavailable", "from_state": row.get("quadrant"),
                                     "to_state": None, "prior_session": row.get("source_asof"),
                                     "since_session": None, "consecutive_sessions": 0, "confirmed": False}
            rows.append(row)
        configured = {s for s, _ in PROXIES[market]}
        received = {r.get("symbol") for r in sectors if r.get("symbol") in configured}
        current = {r.get("symbol") for r in sectors if r.get("symbol") in configured and
                   _day(r.get("source_asof")) == expected and expected is not None}
        current.update(r["symbol"] for r in rows if r["status"] == "current")
        eligible = sum(r["status"] == "current" for r in rows)
        result["markets"][market] = {"market": market, "expected_session": expected, "benchmark": benchmark_meta,
            "proxy_kind": "风格ETF代理" if market == "港股" else "行业ETF代理",
            "note": "固定3类风格ETF代理，非完整行业覆盖。" if market == "港股" else "固定ETF样本的相对价格表现，非全行业广度或资金流。",
            "coverage": {"expected": len(configured), "received": len(received), "current": len(current),
                         "eligible": eligible, "pending": len(configured)-eligible,
                         "historical": sum(r["status"] == "historical" for r in rows)}, "rows": rows}
    return result
