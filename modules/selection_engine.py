"""
AI市场日报 - 选股引擎
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- daily_subpool / build_daily_subpool: 当日轮换子池 = Anchor + Explore
- fast_metrics / calculate_fast_metrics: 轻量指标（ST/MT/LT 分类）
- build_st_pool, build_mt_pool, build_lt_pool: 每套 >= 60 只
- build_candidates_bundle: 二层候选（Explore + Trade）
- build_gemini_payload: 紧凑 JSON，控制 token
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import os
import json
import hashlib
from datetime import datetime
from typing import List, Tuple, Dict, Any, Optional, Callable
from pathlib import Path

import pandas as pd
import yaml

# 配置路径
_CONFIG_PATH = Path(__file__).parent / "config_selection.yaml"


def _load_config() -> dict:
    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}


def _date_seed(date_str: str) -> int:
    """日期字符串转整数种子，可复现"""
    return int(hashlib.md5(date_str.encode()).hexdigest()[:8], 16)


def daily_subpool(
    pool: List[Tuple[str, str, str]],
    date_str: str,
    anchor_ratio: float = 0.15,
    cycle_days: int = 6,
    use_full_for_metrics: bool = True,
) -> Tuple[List[Tuple[str, str, str]], Dict[str, Any]]:
    """
    生成当日轮换子池 = Anchor（固定高频） + Explore（按日期哈希轮换）
    确保 5-7 天覆盖全池一次。
    use_full_for_metrics=True 时返回全池（满足每套>=60只），否则返回 Anchor+Explore 子集。
    """
    n = len(pool)
    anchor_size = max(1, int(n * anchor_ratio))
    explore_pool = pool[anchor_size:]
    explore_total = len(explore_pool)
    explore_per_day = max(1, (explore_total + cycle_days - 1) // cycle_days)

    seed = _date_seed(date_str)
    day_index = seed % cycle_days
    start = (day_index * explore_per_day) % max(1, explore_total)
    explore_indices = [
        (start + i) % explore_total
        for i in range(explore_per_day)
    ]
    explore_items = [explore_pool[i] for i in explore_indices]
    anchor_items = pool[:anchor_size]

    if use_full_for_metrics:
        subpool = list(pool)
    else:
        subpool = anchor_items + explore_items

    coverage_pct = (explore_per_day * cycle_days) / max(1, explore_total) * 100

    stats = {
        "anchor_size": len(anchor_items),
        "explore_size": len(explore_items),
        "subpool_size": len(subpool),
        "mother_pool_size": n,
        "coverage_pct": min(100.0, coverage_pct),
        "day_index": day_index,
        "cycle_days": cycle_days,
    }
    return subpool, stats


def build_daily_subpool(
    pool: List[Tuple[str, str, str]],
    market: str,
    date_str: str,
    anchor_n: Optional[int] = None,
    explore_n: Optional[int] = None,
    seed: Optional[int] = None,
) -> Tuple[List[Tuple[str, str, str]], Dict[str, Any]]:
    """
    当日轮换子池。若指定 anchor_n/explore_n 则用数量；否则用 config 的 anchor_ratio/cycle_days。
    """
    cfg = _load_config()
    sub_cfg = cfg.get("subpool", {})
    anchor_ratio = sub_cfg.get("anchor_ratio", 0.15)
    cycle_days = sub_cfg.get("cycle_days", 6)
    if anchor_n is not None and explore_n is not None:
        n = len(pool)
        anchor_size = min(anchor_n, n)
        explore_pool = pool[anchor_size:]
        explore_total = len(explore_pool)
        explore_per_day = min(explore_n, max(1, (explore_total + cycle_days - 1) // cycle_days))
        _seed = seed if seed is not None else _date_seed(date_str)
        day_index = _seed % cycle_days
        start = (day_index * explore_per_day) % max(1, explore_total)
        explore_indices = [(start + i) % explore_total for i in range(explore_per_day)]
        explore_items = [explore_pool[i] for i in explore_indices]
        anchor_items = pool[:anchor_size]
        subpool = list(pool)
        stats = {
            "anchor_size": len(anchor_items),
            "explore_size": len(explore_items),
            "subpool_size": len(subpool),
            "mother_pool_size": n,
            "coverage_pct": min(100.0, (explore_per_day * cycle_days) / max(1, explore_total) * 100),
            "day_index": day_index,
            "cycle_days": cycle_days,
        }
        return subpool, stats
    return daily_subpool(pool, date_str, anchor_ratio, cycle_days, use_full_for_metrics=True)


def _canslim_score(df: pd.DataFrame) -> Optional[float]:
    """
    钉钉同源 CANSLIM 评分（0-100）。用于 quality_gate 质量闸门。
    与 aihub26/auto_reporter._simple_metrics_score 逻辑一致。
    """
    if df is None or df.empty or len(df) < 20 or "Close" not in df.columns:
        return None
    try:
        df = df.apply(pd.to_numeric, errors="coerce").dropna().sort_index()
        if len(df) < 20:
            return None
        df = df.copy()
        for p in [5, 10, 20, 50, 60, 200]:
            if len(df) >= p:
                df[f"MA{p}"] = df["Close"].rolling(p).mean()
        delta = df["Close"].diff()
        gain = delta.where(delta > 0, 0).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)
        rs = gain.ewm(com=13).mean() / (loss.ewm(com=13).mean() + 1e-10)
        df["RSI"] = 100 - (100 / (1 + rs))
        df["RSI"] = df["RSI"].fillna(50)
        last = df.iloc[-1]
        score = 0
        if last["Close"] > last.get("MA50", 0): score += 15
        if last["Close"] > last.get("MA200", 0): score += 15
        if len(df) >= 60 and last["Close"] >= df["High"].tail(60).max() * 0.95: score += 10
        if len(df) >= 20 and last["Volume"] > df["Volume"].tail(20).mean() * 1.2: score += 10
        if last["RSI"] > 50: score += 10
        if last.get("MA5", 0) > last.get("MA10", 0) > last.get("MA20", 0): score += 15
        if len(df) >= 21 and (last["Close"] - df["Close"].iloc[-21]) / (df["Close"].iloc[-21] + 1e-10) > 0: score += 10
        if last["Close"] > last.get("MA60", 0): score += 15
        if len(df) >= 6 and (last["Close"] - df["Close"].iloc[-6]) / (df["Close"].iloc[-6] + 1e-10) > 0.03: score += 15
        return min(100, score)
    except Exception:
        return None


def fast_metrics(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    轻量指标，用于 ST/MT/LT 分类。含 canslim_score（钉钉同源 CANSLIM 评分）。
    """
    if df is None or df.empty or len(df) < 20 or "Close" not in df.columns:
        return None
    try:
        df = df.apply(pd.to_numeric, errors="coerce").dropna().sort_index()
        if len(df) < 20:
            return None
        last = df.iloc[-1]
        close = last["Close"]
        if close <= 0:
            return None

        # 均线
        ma20 = df["Close"].rolling(20).mean().iloc[-1] if len(df) >= 20 else close
        ma60 = df["Close"].rolling(60).mean().iloc[-1] if len(df) >= 60 else close
        ma200 = df["Close"].rolling(200).mean().iloc[-1] if len(df) >= 200 else close

        ret5 = (close - df["Close"].iloc[-6]) / (df["Close"].iloc[-6] + 1e-10) if len(df) >= 6 else 0
        ret20 = (close - df["Close"].iloc[-21]) / (df["Close"].iloc[-21] + 1e-10) if len(df) >= 21 else 0

        delta = df["Close"].diff()
        gain = delta.where(delta > 0, 0).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)
        rs = gain.ewm(com=13).mean() / (loss.ewm(com=13).mean() + 1e-10)
        rsi = 100 - (100 / (1 + rs))
        rsi_val = float(rsi.iloc[-1]) if hasattr(rsi, "iloc") else 50

        vol_20 = df["Volume"].tail(20).std() if len(df) >= 20 else 0
        vol_mean = df["Volume"].tail(20).mean() if len(df) >= 20 else 1
        vol_ratio = vol_20 / (vol_mean + 1e-10) if vol_mean else 0

        canslim_score = _canslim_score(df)

        return {
            "close": close,
            "ret5": ret5,
            "ret20": ret20,
            "above_ma20": close > ma20,
            "above_ma60": close > ma60,
            "above_ma200": close > ma200,
            "rsi": rsi_val,
            "vol_ratio": vol_ratio,
            "canslim_score": canslim_score,
        }
    except Exception:
        return None


def _classify_term(m: Dict) -> str:
    """ST / MT / LT 分类"""
    if not m:
        return "mt"
    ret5 = m.get("ret5", 0)
    ret20 = m.get("ret20", 0)
    above_ma20 = m.get("above_ma20", False)
    above_ma200 = m.get("above_ma200", False)
    vol_ratio = m.get("vol_ratio", 1)

    if ret5 > 0.02 or (ret20 > 0.05 and above_ma20):
        return "st"
    if above_ma200 and vol_ratio < 1.5:
        return "lt"
    return "mt"


def _classify_term_style(df: pd.DataFrame, last: pd.Series) -> str:
    """钉钉同源：短中长期分类（short/mid/long），与 aihub26/auto_reporter._classify_term_style 一致"""
    try:
        ret_5d = (last["Close"] - df["Close"].iloc[-6]) / (df["Close"].iloc[-6] + 1e-10) if len(df) >= 6 else 0
        ret_20d = (last["Close"] - df["Close"].iloc[-21]) / (df["Close"].iloc[-21] + 1e-10) if len(df) >= 21 else 0
        above_ma20 = last["Close"] > last.get("MA20", 0)
        above_ma60 = last["Close"] > last.get("MA60", 0)
        above_ma200 = last["Close"] > last.get("MA200", 0)
        vol_20 = df["Volume"].tail(20).std() if len(df) >= 20 else 0
        vol_ratio = vol_20 / (df["Volume"].tail(20).mean() + 1e-10) if len(df) >= 20 else 0
        if ret_5d > 0.03 or (ret_20d > 0.05 and above_ma20):
            return "short"
        if above_ma200 and vol_ratio < 1.5:
            return "long"
        return "mid"
    except Exception:
        return "mid"


def screened_candidates_dingtalk_style(
    pool: List[Tuple[str, str, str]],
    min_score: int,
    prefix: str,
    fetch_fn: Callable[[str], Optional[pd.DataFrame]],
    max_per_type: int = 40,
    max_total: int = 100,
    max_workers: int = 16,
) -> Tuple[
    List[Tuple[Tuple[str, str, str], float, float]],  # short -> ST
    List[Tuple[Tuple[str, str, str], float, float]],  # mid -> MT
    List[Tuple[Tuple[str, str, str], float, float]],  # long -> LT
]:
    """
    钉钉筛选逻辑（Score + 阈值）抽象函数：全池并发拉取 → score>=min_score 过滤 → 短中长期分类 → 每类取 max_per_type 只。
    与 aihub26/auto_reporter._screened_candidates 逻辑一致，供 V88 引擎注入使用。
    返回 (short_list, mid_list, long_list)，每项为 [(item, price, score), ...]
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _worker(item):
        try:
            yf_code = item[2] if len(item) >= 3 else item[0]
            df = fetch_fn(yf_code)
            if df is None or len(df) < 20 or "Close" not in df.columns:
                return None
            df = df.apply(pd.to_numeric, errors="coerce").dropna().sort_index()
            if len(df) < 20:
                return None
            df = df.copy()
            for p in [5, 10, 20, 50, 60, 200]:
                if len(df) >= p:
                    df[f"MA{p}"] = df["Close"].rolling(p).mean()
            score = _canslim_score(df)
            if score is None or score < min_score:
                return None
            last = df.iloc[-1]
            term = _classify_term_style(df, last)
            price = float(df["Close"].iloc[-1]) if "Close" in df.columns else 0.0
            return (item, price, score, term)
        except Exception:
            return None

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_worker, it): it for it in pool}
        for f in as_completed(futures):
            try:
                r = f.result()
                if r:
                    results.append(r)
            except Exception:
                pass

    short_l = sorted([x for x in results if x[3] == "short"], key=lambda x: x[2], reverse=True)[:max_per_type]
    mid_l = sorted([x for x in results if x[3] == "mid"], key=lambda x: x[2], reverse=True)[:max_per_type]
    long_l = sorted([x for x in results if x[3] == "long"], key=lambda x: x[2], reverse=True)[:max_per_type]
    used_ids = {id(x[0]) for x in short_l + mid_l + long_l}
    fill = sorted([x for x in results if id(x[0]) not in used_ids], key=lambda x: x[2], reverse=True)
    for lst in (short_l, mid_l, long_l):
        while len(lst) < max_per_type and fill:
            lst.append(fill.pop(0))

    def _to_trade(seq):
        return [(it, p, s) for it, p, s, _ in seq]

    return _to_trade(short_l), _to_trade(mid_l), _to_trade(long_l)


def screened_candidates_wsj_format(
    pool: List[Tuple[str, str, str]],
    min_score: int,
    prefix: str,
    fetch_fn: Callable[[str], Optional[pd.DataFrame]],
    max_per_type: int = 40,
    max_total: int = 100,
) -> List[Tuple[Tuple[str, str, str], str]]:
    """
    钉钉筛选逻辑的 WSJ 格式输出：返回 [(item, price_str), ...]，如 [((code,name,yf), "$150.00"), ...]。
    供钉钉 auto_reporter 直接调用，与 V88 引擎同源。
    """
    short_t, mid_t, long_t = screened_candidates_dingtalk_style(
        pool, min_score, prefix, fetch_fn, max_per_type, max_total
    )
    merged = short_t + mid_t + long_t
    out = []
    for item, price, _ in merged[:max_total]:
        pstr = f"{prefix}{price:.2f}" if price else "N/A"
        out.append((item, pstr))
    return out


def build_st_pool(
    subpool: List[Tuple[str, str, str]],
    metrics_list: List[Optional[Dict]],
    min_size: int = 60,
    max_size: int = 80,
) -> Tuple[List[Tuple[str, str, str, float]], Dict[str, Any]]:
    """构建短期候选池，>= min_size 只"""
    return _build_typed_pool(subpool, metrics_list, "st", min_size, max_size)


def build_mt_pool(
    subpool: List[Tuple[str, str, str]],
    metrics_list: List[Optional[Dict]],
    min_size: int = 60,
    max_size: int = 80,
) -> Tuple[List[Tuple[str, str, str, float]], Dict[str, Any]]:
    """构建中期候选池"""
    return _build_typed_pool(subpool, metrics_list, "mt", min_size, max_size)


def build_lt_pool(
    subpool: List[Tuple[str, str, str]],
    metrics_list: List[Optional[Dict]],
    min_size: int = 60,
    max_size: int = 80,
) -> Tuple[List[Tuple[str, str, str, float]], Dict[str, Any]]:
    """构建长期候选池"""
    return _build_typed_pool(subpool, metrics_list, "lt", min_size, max_size)


def _build_typed_pool(
    subpool: List[Tuple[str, str, str]],
    metrics_list: List[Optional[Dict]],
    term: str,
    min_size: int,
    max_size: int,
) -> Tuple[List[Tuple[str, str, str, float]], Dict[str, Any]]:
    """内部：按 term 分类构建候选池（使用预计算的 metrics_list）"""
    typed = []
    other = []
    for i, item in enumerate(subpool):
        m = metrics_list[i] if i < len(metrics_list) else None
        if not m:
            continue
        t = _classify_term(m)
        price = m.get("close", 0.0)
        entry = (*item, price)
        if t == term:
            typed.append(entry)
        else:
            other.append(entry)

    while len(typed) < min_size and other:
        typed.append(other.pop(0))
    result = typed[:max_size]
    return result, {"count": len(result)}


def _batch_fetch_metrics(
    subpool: List[Tuple[str, str, str]],
    fetch_fn: Callable[[str], Optional[pd.DataFrame]],
    max_workers: int = 8,
) -> List[Optional[Dict]]:
    """并发拉取并计算 fast_metrics"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    results = [None] * len(subpool)
    def _task(i, item):
        yf_code = item[2] if len(item) >= 3 else item[0]
        df = fetch_fn(yf_code)
        return i, fast_metrics(df) if df is not None else None
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_task, i, item) for i, item in enumerate(subpool)]
        for f in as_completed(futures):
            try:
                i, m = f.result()
                results[i] = m
            except Exception:
                pass
    return results


def calculate_fast_metrics(
    subpool: List[Tuple[str, str, str]],
    fetch_fn: Callable[[str], Optional[pd.DataFrame]],
    max_workers: int = 8,
) -> List[Optional[Dict]]:
    """并发拉取并计算 fast_metrics（别名）"""
    return _batch_fetch_metrics(subpool, fetch_fn, max_workers)


def _build_typed_pool_with_metrics(
    subpool: List[Tuple[str, str, str]],
    metrics_list: List[Optional[Dict]],
    term: str,
    min_size: int,
    max_size: int,
) -> List[Tuple[Tuple[str, str, str], float, Dict]]:
    """按 term 分类，返回 [(item, price, metrics), ...]，用于 quality_gate"""
    typed = []
    other = []
    for i, item in enumerate(subpool):
        m = metrics_list[i] if i < len(metrics_list) else None
        if not m:
            continue
        t = _classify_term(m)
        price = m.get("close", 0.0)
        entry = (item, price, m)
        if t == term:
            typed.append(entry)
        else:
            other.append(entry)
    while len(typed) < min_size and other:
        typed.append(other.pop(0))
    return typed[:max_size]


def build_candidates_bundle(
    us_pool: List[Tuple[str, str, str]],
    hk_pool: List[Tuple[str, str, str]],
    cn_pool: List[Tuple[str, str, str]],
    fetch_fn: Callable[[str], Optional[pd.DataFrame]],
    date_str: Optional[str] = None,
) -> Dict[str, Any]:
    """
    二层候选体系：Explore（覆盖广）+ Trade（质量闸门）
    当 min_score 存在时，使用钉钉筛选逻辑（Score+阈值）：全池并发 → score>=min_score → 短中长期分类 → 每类40只。
    返回结构：
    {
      "date": "...",
      "US": { "ST": {explore, trade, meta}, "MT": {...}, "LT": {...}, "subpool_stats": {...} },
      "HK": {...},
      "CN": {...}
    }
    """
    try:
        from . import quality_gate as qg
    except ImportError:
        qg = None
    cfg = _load_config()
    sub_cfg = cfg.get("subpool", {})
    cand_cfg = cfg.get("candidate_pool", {})
    explore_k = cand_cfg.get("explore_k", {"ST": 80, "MT": 80, "LT": 60})
    trade_quantile = cand_cfg.get("trade_quantile", {"ST": 0.70, "MT": 0.60, "LT": 0.50})
    trade_min_size = cand_cfg.get("trade_min_size", {"ST": 30, "MT": 30, "LT": 30})
    min_score = cand_cfg.get("min_score")  # 钉钉同源：40 时使用钉钉式全池筛选
    token_cfg = cfg.get("token_guard", {})
    max_candidates = token_cfg.get("max_candidates_per_market", 100)
    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    policy = {"quantile": trade_quantile, "trade_min_size": trade_min_size, "min_score": min_score}
    result = {"date": date_str, "US": {}, "HK": {}, "CN": {}}
    markets = [
        ("US", us_pool, "$"),
        ("HK", hk_pool, "HK$"),
        ("CN", cn_pool, "¥"),
    ]

    use_dingtalk_screening = min_score is not None

    for market, pool, prefix in markets:
        if use_dingtalk_screening:
            # 钉钉筛选逻辑注入：全池 → score>=min_score → 短中长期分类 → 每类40只
            short_trade, mid_trade, long_trade = screened_candidates_dingtalk_style(
                pool, min_score, prefix, fetch_fn, max_per_type=40, max_total=max_candidates
            )
            sub_stats = {"mother_pool_size": len(pool), "subpool_size": len(pool), "coverage_pct": 100.0}
            market_data = {"subpool_stats": sub_stats}
            for horizon, trade in [("ST", short_trade), ("MT", mid_trade), ("LT", long_trade)]:
                market_data[horizon] = {
                    "explore": [],
                    "trade": trade,
                    "meta": {"count": len(trade), "quantile_used": None, "min_score_used": min_score},
                }
        else:
            subpool, sub_stats = build_daily_subpool(pool, market, date_str)
            metrics_list = _batch_fetch_metrics(subpool, fetch_fn)
            market_data = {"subpool_stats": sub_stats}
            for horizon in ["ST", "MT", "LT"]:
                k = explore_k.get(horizon, 60)
                explore_raw = _build_typed_pool_with_metrics(subpool, metrics_list, horizon.lower(), k, k * 2)
                explore = [(it, p, m) for it, p, m in explore_raw[:max(k, 60)]]
                trade = []
                meta = {}
                if qg:
                    trade, meta = qg.quality_gate(explore, market, horizon, policy)
                else:
                    mn = trade_min_size.get(horizon, 30)
                    trade = [(it, p, 0.0) for it, p, m in explore[:mn]]
                    meta = {"count": len(trade), "quantile_used": 0, "filter_reasons": {}}
                market_data[horizon] = {
                    "explore": explore,
                    "trade": trade,
                    "meta": meta,
                }
        result[market] = market_data
    return result


def build_wsj_format_candidates(
    st_pool: List[Tuple[str, str, str, float]],
    mt_pool: List[Tuple[str, str, str, float]],
    lt_pool: List[Tuple[str, str, str, float]],
    prefix: str = "$",
) -> List[str]:
    """WSJ 格式：名称(代码): 日报价 $xxx，合并 ST+MT+LT 为最大候选池"""
    def _fmt(item):
        name = item[1] if len(item) >= 2 else ""
        code = item[2] if len(item) >= 3 else item[0]
        price = item[3] if len(item) >= 4 else 0
        return f"{name}({code}): 日报价 {prefix}{price:.2f}"
    out = []
    for item in st_pool + mt_pool + lt_pool:
        out.append(_fmt(item))
    return out


def build_gemini_payload(
    st_pool: List[Tuple[str, str, str, float]],
    mt_pool: List[Tuple[str, str, str, float]],
    lt_pool: List[Tuple[str, str, str, float]],
    prefix: str = "$",
    max_chars_per_stock: int = 35,
) -> str:
    """
    生成紧凑 JSON 候选池，每只一行，控制 token。
    格式: {"c":"AAPL","n":"苹果","p":150.2}
    """
    def _row(item):
        code = item[2] if len(item) >= 3 else item[0]
        name = (item[1] or "")[:8]
        price = item[3] if len(item) >= 4 else 0
        s = json.dumps({"c": code, "n": name, "p": round(price, 2)}, ensure_ascii=False)
        return s[:max_chars_per_stock]

    lines = []
    for item in st_pool:
        lines.append(f"ST|{_row(item)}")
    for item in mt_pool:
        lines.append(f"MT|{_row(item)}")
    for item in lt_pool:
        lines.append(f"LT|{_row(item)}")
    return "\n".join(lines)


def build_gemini_payload_from_bundle(
    bundle: Dict[str, Any],
    market: str,
    prefix: str = "$",
    max_chars_per_stock: int = 35,
) -> str:
    """
    从 build_candidates_bundle 输出生成紧凑 JSON。
    格式：每行 ST|{c,n,p} 或 MT|{c,n,p} 或 LT|{c,n,p}，先 explore 后 trade（trade 标记）。
    """
    def _row(item, price):
        code = item[2] if len(item) >= 3 else item[0]
        name = (item[1] or "")[:8]
        s = json.dumps({"c": code, "n": name, "p": round(price, 2)}, ensure_ascii=False)
        return s[:max_chars_per_stock]

    lines = []
    m_data = bundle.get(market, {})
    for horizon in ["ST", "MT", "LT"]:
        h_data = m_data.get(horizon, {})
        explore = h_data.get("explore", [])
        trade = h_data.get("trade", [])
        trade_codes = {t[0][2] if len(t[0]) >= 3 else t[0][0] for t in trade}
        for it, p, _ in explore:
            row = f"{horizon}|{_row(it, p)}"
            if (it[2] if len(it) >= 3 else it[0]) in trade_codes:
                row += "|T"
            lines.append(row)
    return "\n".join(lines)


def format_bundle_wsj_candidates(
    bundle: Dict[str, Any],
    market: str,
    prefix: str = "$",
    max_total: int = 100,
) -> List[str]:
    """WSJ 格式：名称(代码): 日报价 $xxx，合并 explore+trade 去重，供 prompt 使用"""
    def _fmt(item, price):
        name = item[1] if len(item) >= 2 else ""
        code = item[2] if len(item) >= 3 else item[0]
        return f"{name}({code}): 日报价 {prefix}{price:.2f}"

    seen = set()
    out = []
    m_data = bundle.get(market, {})
    for horizon in ["ST", "MT", "LT"]:
        h_data = m_data.get(horizon, {})
        for lst in [h_data.get("trade", []), h_data.get("explore", [])]:
            for entry in lst:
                it = entry[0]
                p = entry[1]
                code = it[2] if len(it) >= 3 else it[0]
                if code in seen:
                    continue
                seen.add(code)
                out.append(_fmt(it, p))
                if len(out) >= max_total:
                    return out
    return out


def get_cached_or_build(
    cache_key: str,
    build_fn: Callable[[], Any],
    cache_dir: Optional[str] = None,
    ttl_hours: int = 24,
) -> Any:
    """当天候选池缓存，多次报告复用"""
    cache_dir = cache_dir or os.path.join(os.path.expanduser("~"), ".cache_stock_selection")
    os.makedirs(cache_dir, exist_ok=True)
    path = os.path.join(cache_dir, f"{cache_key}.json")
    now = datetime.now()
    if os.path.exists(path):
        mtime = datetime.fromtimestamp(os.path.getmtime(path))
        if (now - mtime).total_seconds() < ttl_hours * 3600:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
    result = build_fn()
    if isinstance(result, (dict, list)):
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=0)
        except Exception:
            pass
    return result


def build_daily_candidates(
    us_pool: List[Tuple[str, str, str]],
    hk_pool: List[Tuple[str, str, str]],
    cn_pool: List[Tuple[str, str, str]],
    fetch_fn: Callable[[str], Optional[pd.DataFrame]],
    date_str: Optional[str] = None,
    use_cache: bool = True,
) -> Dict[str, Any]:
    """
    主入口：构建当日三市场 ST/MT/LT 候选池 + Gemini payload + 复盘元数据。
    """
    cfg = _load_config()
    pool_cfg = cfg.get("pool", {})
    sub_cfg = cfg.get("subpool", {})
    cand_cfg = cfg.get("candidate_pool", {})
    token_cfg = cfg.get("token_control", {})

    anchor_ratio = sub_cfg.get("anchor_ratio", 0.15)
    cycle_days = sub_cfg.get("cycle_days", 6)
    min_per = cand_cfg.get("min_per_type", 60)
    max_per = cand_cfg.get("max_per_type", 80)
    max_chars = token_cfg.get("max_chars_per_stock", 35)

    date_str = date_str or datetime.now().strftime("%Y-%m-%d")

    def _build_market(market: str, pool: List, pfx: str):
        subpool, sub_stats = daily_subpool(pool, date_str, anchor_ratio, cycle_days)
        metrics_list = _batch_fetch_metrics(subpool, fetch_fn)
        st_pool, _ = build_st_pool(subpool, metrics_list, min_per, max_per)
        mt_pool, _ = build_mt_pool(subpool, metrics_list, min_per, max_per)
        lt_pool, _ = build_lt_pool(subpool, metrics_list, min_per, max_per)
        payload = build_gemini_payload(st_pool, mt_pool, lt_pool, pfx, max_chars)
        wsj_raw = build_wsj_format_candidates(st_pool, mt_pool, lt_pool, pfx)
        max_wsj = token_cfg.get("max_wsj_candidates", 100)
        wsj_candidates = wsj_raw[:max_wsj]
        return {
            "subpool_stats": sub_stats,
            "st_size": len(st_pool),
            "mt_size": len(mt_pool),
            "lt_size": len(lt_pool),
            "payload": payload,
            "wsj_candidates": wsj_candidates,
            "st_pool": [[x[0], x[1], x[2], x[3]] for x in st_pool],
            "mt_pool": [[x[0], x[1], x[2], x[3]] for x in mt_pool],
            "lt_pool": [[x[0], x[1], x[2], x[3]] for x in lt_pool],
        }

    us_data = _build_market("US", us_pool, "$")
    hk_data = _build_market("HK", hk_pool, "HK$")
    cn_data = _build_market("CN", cn_pool, "¥")

    return {
        "date": date_str,
        "us": us_data,
        "hk": hk_data,
        "cn": cn_data,
        "coverage": {
            "us": us_data["subpool_stats"]["coverage_pct"],
            "hk": hk_data["subpool_stats"]["coverage_pct"],
            "cn": cn_data["subpool_stats"]["coverage_pct"],
        },
        "candidate_sizes": {
            "us": (us_data["st_size"], us_data["mt_size"], us_data["lt_size"]),
            "hk": (hk_data["st_size"], hk_data["mt_size"], hk_data["lt_size"]),
            "cn": (cn_data["st_size"], cn_data["mt_size"], cn_data["lt_size"]),
        },
    }


def verify_and_print(data: Dict[str, Any]) -> None:
    """打印子池大小、候选池大小、覆盖率，用于验证"""
    print("=" * 50)
    print("【选股引擎验证】")
    print(f"日期: {data.get('date', 'N/A')}")
    print("-" * 50)
    for mkt, label in [("us", "美股"), ("hk", "港股"), ("cn", "A股")]:
        d = data.get(mkt, {})
        s = d.get("subpool_stats", {})
        print(f"{label}:")
        print(f"  母池: {s.get('mother_pool_size', 0)} | 子池: {s.get('subpool_size', 0)} (Anchor={s.get('anchor_size', 0)} + Explore={s.get('explore_size', 0)})")
        print(f"  覆盖率: {s.get('coverage_pct', 0):.1f}% | 周期: {s.get('cycle_days', 0)}天")
        print(f"  候选池: ST={d.get('st_size', 0)} MT={d.get('mt_size', 0)} LT={d.get('lt_size', 0)}")
    print("=" * 50)


def verify_bundle_print(bundle: Dict[str, Any]) -> None:
    """打印二层候选：Explore/Trade 数量、分位数阈值、覆盖率"""
    print("=" * 55)
    print("【二层候选验证】Explore + Trade")
    print(f"日期: {bundle.get('date', 'N/A')}")
    print("-" * 55)
    for mkt, label in [("US", "美股"), ("HK", "港股"), ("CN", "A股")]:
        d = bundle.get(mkt, {})
        s = d.get("subpool_stats", {})
        print(f"{label}: 母池{s.get('mother_pool_size', 0)} 子池{s.get('subpool_size', 0)} 覆盖率{s.get('coverage_pct', 0):.1f}%")
        for h in ["ST", "MT", "LT"]:
            hd = d.get(h, {})
            ex = len(hd.get("explore", []))
            tr = len(hd.get("trade", []))
            m = hd.get("meta", {})
            q = m.get("quantile_used", 0)
            print(f"  {h}: Explore={ex} Trade={tr} 分位数={q}")
    print("=" * 55)
