"""V88 全行业机会雷达 —— 用同一套个股前瞻引擎扫全行业，主动发现"起步"的板块与个股。

解决"医疗板块起步我后知后觉"：不再只算自选/持仓，而是对一篮子覆盖各行业（含医疗）
的个股跑 evaluate_forward_outlook（纯确定性、无 AI、不耗预算），按"机会分"排名，
并按板块聚合出"谁在起步"。概率一律是规则情景估计，不是回测胜率。

数据获取与板块归类由调用方注入（app 传 fetch_stock_data / get_sector），本模块只做
可复算的评分与聚合，方便单测。
"""
from __future__ import annotations

from v88_decision_core import evaluate_forward_outlook

# 覆盖主要行业的流动性龙头（含医疗/医药/生物），保证"全行业"且首版够快。
# 板块由 get_sector(code, name) 反推，这里只需 (代码, 名称)。可被外部池覆盖。
DEFAULT_POOL = [
    # 医疗 / 医药 / 生物（用户点名的板块，必须覆盖）
    ("LLY", "礼来"), ("JNJ", "强生"), ("UNH", "联合健康"), ("MRK", "默沙东"),
    ("ABBV", "艾伯维"), ("PFE", "辉瑞"), ("2269.HK", "药明生物"), ("1801.HK", "信达生物"),
    ("300760.SZ", "迈瑞医疗"), ("600276.SS", "恒瑞医药"), ("1093.HK", "石药集团"),
    # 半导体 / 科技
    ("NVDA", "英伟达"), ("AMD", "AMD"), ("AVGO", "博通"), ("TSM", "台积电"),
    ("AAPL", "苹果"), ("MSFT", "微软"), ("GOOG", "谷歌"), ("META", "Meta"),
    ("0700.HK", "腾讯控股"), ("9988.HK", "阿里巴巴"), ("688012.SS", "中微公司"),
    ("688981.SS", "中芯国际"), ("300750.SZ", "宁德时代"),
    # 金融
    ("0005.HK", "汇丰控股"), ("JPM", "摩根大通"), ("BAC", "美国银行"),
    ("601318.SS", "中国平安"), ("600036.SS", "招商银行"),
    # 消费
    ("AMZN", "亚马逊"), ("PG", "宝洁"), ("KO", "可口可乐"),
    ("600519.SS", "贵州茅台"), ("1810.HK", "小米集团"),
    # 能源 / 工业 / 材料
    ("XOM", "埃克森美孚"), ("CAT", "卡特彼勒"), ("601899.SS", "紫金矿业"),
    # 【2026-07-18 用户点单"没有中港板块"】补齐A股/港股各行业龙头,
    # 让板块聚合三个市场都有自己的行（聚合按 市场×板块 拆开,不再混在一起）。
    ("603259.SS", "药明康德"), ("300015.SZ", "爱尔眼科"),          # 医疗 A
    ("9888.HK", "百度集团"), ("3690.HK", "美团"), ("9618.HK", "京东集团"),   # 科技 H
    ("002371.SZ", "北方华创"), ("002594.SZ", "比亚迪"),            # 科技/制造 A
    ("1299.HK", "友邦保险"), ("0388.HK", "香港交易所"),            # 金融 H
    ("601398.SS", "工商银行"),                                     # 金融 A
    ("000333.SZ", "美的集团"), ("600887.SS", "伊利股份"),          # 消费 A
    ("9633.HK", "农夫山泉"),                                       # 消费 H
    ("0883.HK", "中国海洋石油"), ("601088.SS", "中国神华"),        # 能源
    ("600900.SS", "长江电力"), ("600309.SS", "万华化学"),          # 公用/材料
    ("600031.SS", "三一重工"), ("601012.SS", "隆基绿能"),          # 工业/新能源
]


def _market_of(code: str) -> str:
    c = str(code or "").upper()
    if c.endswith(".HK"):
        return "港股"
    if c.endswith((".SS", ".SZ", ".SH")):
        return "A股"
    return "美股"


def _horizon_means(rows):
    """短端(≤10日) / 长端(≥60日) 的平均上涨概率，用于判断'起步'相位。"""
    short = [r["p_up"] for r in rows if r.get("days", 999) <= 10]
    long_ = [r["p_up"] for r in rows if r.get("days", 0) >= 60]
    short_p = round(sum(short) / len(short)) if short else 0
    long_p = round(sum(long_) / len(long_)) if long_ else 0
    return short_p, long_p


def opportunity_score(fwd: dict) -> dict:
    """把一份个股前瞻折算成机会分与'起步'标记（纯确定性）。
    机会分 = 综合上涨概率 + 盈亏比贡献 + 期望贡献；起步 = 长端偏多且短端不破。"""
    rows = fwd.get("horizons") or []
    p_up = float(fwd.get("weighted_p_up", 50))
    rr = float(fwd.get("weighted_rr", 0) or 0)
    ev = float(fwd.get("weighted_expected_pct", 0) or 0)
    short_p, long_p = _horizon_means(rows)
    score = round(p_up + 12 * min(rr, 2.5) + 2 * ev)
    # '起步'：中长期方向已明显偏多、短期未破位、净期望为正（趋势正在形成，越早发现越值钱）。
    # 注意：平滑趋势股的盈亏比天然偏低（阻力贴着现价），所以'起步'看概率与期望，不卡盈亏比。
    starting = bool(long_p >= 60 and short_p >= 52 and ev > 0)
    return {"opp_score": score, "short_p": short_p, "long_p": long_p, "starting": starting}


def scan_forward_opportunities(fetch_fn, get_sector_fn, pool=None, *,
                               forward_fn=evaluate_forward_outlook, min_bars=30,
                               top_n=15) -> dict:
    """对 pool 里每只个股跑前瞻并排名；按板块聚合'起步度'。
    fetch_fn(code)->df(或None)；get_sector_fn(code,name)->板块名。全程无 AI、不耗预算。"""
    pool = pool or DEFAULT_POOL
    rows = []
    errors = 0
    for code, name in pool:
        try:
            df = fetch_fn(code)
        except Exception:
            df = None
        if df is None or len(df) < min_bars:
            errors += 1
            continue
        fwd = forward_fn(df, name=name, code=code)
        if fwd.get("error"):
            errors += 1
            continue
        opp = opportunity_score(fwd)
        rows.append({
            "code": code, "name": name, "market": _market_of(code),
            "sector": get_sector_fn(code, name) if get_sector_fn else "其他",
            "last": fwd.get("last"), "stage": fwd.get("stage"),
            "p_up": fwd.get("weighted_p_up"), "rr": fwd.get("weighted_rr"),
            "ev": fwd.get("weighted_expected_pct"),
            "overall": fwd.get("overall_action"), "suggestion": fwd.get("suggestion"),
            **opp,
        })

    stocks = sorted(rows, key=lambda r: r["opp_score"], reverse=True)

    # 板块聚合：按 市场×板块 拆开（2026-07-18 用户抓"只有美股板块"——混在一起时
    # 中港的预计被美股代表淹没），各市场独立算平均概率与起步只数。
    by_sector = {}
    for r in rows:
        by_sector.setdefault((r["market"], r["sector"]), []).append(r)
    sectors = []
    for (market, sector), items in by_sector.items():
        n = len(items)
        avg_p = round(sum(i["p_up"] for i in items) / n)
        avg_rr = round(sum((i["rr"] or 0) for i in items) / n, 2)
        starting_n = sum(1 for i in items if i["starting"])
        sectors.append({
            "market": market, "sector": sector, "count": n,
            "avg_p_up": avg_p, "avg_rr": avg_rr,
            "starting_count": starting_n,
            "starting_names": [i["name"] for i in items if i["starting"]][:5],
            "hot": bool(avg_p >= 56 and starting_n >= 1),
        })
    sectors.sort(key=lambda s: (s["starting_count"], s["avg_p_up"]), reverse=True)

    return {
        "schema": "v88.forward-radar/1.0",
        "scanned": len(rows), "skipped": errors, "pool_size": len(pool),
        "probability_kind": "规则情景估计（非回测胜率）",
        "stocks": stocks[:top_n],
        "starting_stocks": [r for r in stocks if r["starting"]][:top_n],
        "sectors": sectors,
    }
