"""V88 输入股票池的规则观察雷达，不授予股票或板块交易许可。

解决"医疗板块起步我后知后觉"：不再只算自选/持仓，而是对一篮子覆盖各行业（含医疗）
的个股跑 evaluate_forward_outlook（纯确定性、无 AI、不耗预算），按"机会分"排名，
并按板块聚合规则条件。旧 p_up 字段只是未校准方向分，不是上涨概率。

数据获取与板块归类由调用方注入（app 传 fetch_stock_data / get_sector），本模块只做
可复算的评分与聚合，方便单测。
"""
from __future__ import annotations

from v88_decision_core import evaluate_forward_outlook
import logging
import math

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


def _number(value):
    return float(value) if type(value) in (int, float) and math.isfinite(value) else None


def _horizon_means(rows):
    """短/长观察档的规则方向均分；缺档保留空值。"""
    short = [r["p_up"] for r in rows if r.get("days", 999) <= 10]
    long_ = [r["p_up"] for r in rows if r.get("days", 0) >= 60]
    short_p = round(sum(short) / len(short)) if short else None
    long_p = round(sum(long_) / len(long_)) if long_ else None
    return short_p, long_p


def opportunity_score(fwd: dict) -> dict:
    """保留原公式，缺少任一事实或完整观察档时不生成排名分。"""
    rows = fwd.get("horizons") or []
    p_up = _number(fwd.get("weighted_p_up")); rr = _number(fwd.get("weighted_rr"))
    ev = _number(fwd.get("weighted_expected_pct")); gaps = []
    if p_up is None or not 0 <= p_up <= 100: gaps.append('综合规则方向分缺失或无效')
    if rr is None or rr < 0: gaps.append('规则空间比缺失或无效')
    if ev is None: gaps.append('规则加权空间缺失或无效')
    if not rows: gaps.append('观察档缺失')
    for row in rows:
        days, sample, score = (_number(row.get(k)) for k in ('days','sample_days','p_up'))
        if days is None or days <= 0 or sample is None or sample < days:
            gaps.append(f"{row.get('label') or '观察档'}缺少完整输入窗口")
        if score is None or not 0 <= score <= 100: gaps.append('观察档方向分缺失或无效')
    if gaps:
        return {'opp_score':None, 'short_p':None, 'long_p':None, 'starting':False,
                'status':'limited', 'gaps':list(dict.fromkeys(gaps)), 'no_grade_authority':True, 'entry_permission':False}
    short_p, long_p = _horizon_means(rows)
    if short_p is None or long_p is None:
        return {'opp_score':None, 'short_p':short_p, 'long_p':long_p, 'starting':False,
                'status':'limited', 'gaps':['短端或长端观察档缺失'], 'no_grade_authority':True, 'entry_permission':False}
    score = round(p_up + 12 * min(rr, 2.5) + 2 * ev)
    # '起步'：中长期方向已明显偏多、短期未破位、净期望为正（趋势正在形成，越早发现越值钱）。
    # 注意：平滑趋势股的盈亏比天然偏低（阻力贴着现价），所以'起步'看概率与期望，不卡盈亏比。
    starting = bool(long_p >= 60 and short_p >= 52 and ev > 0)
    return {"opp_score": score, "short_p": short_p, "long_p": long_p, "starting": starting,
            'status':'observed', 'gaps':[], 'no_grade_authority':True, 'entry_permission':False}


def scan_forward_opportunities(fetch_fn, get_sector_fn, pool=None, *,
                               forward_fn=evaluate_forward_outlook, min_bars=30,
                               top_n=15) -> dict:
    """对 pool 里每只个股跑前瞻并排名；按板块聚合'起步度'。
    fetch_fn(code)->df(或None)；get_sector_fn(code,name)->板块名。全程无 AI、不耗预算。"""
    source = "explicit_pool"
    if pool is None:
        try:
            from modules.stock_pool import init_stock_pools
            groups = init_stock_pools()
            pool = [(str(item[2]),str(item[1])) for group in groups for item in group]
            source = "dynamic_stock_pool"
        except Exception:
            logging.exception("动态股票池读取失败，不回退成55只并冒充全市场")
            pool = []
            source = "dynamic_pool_unavailable"
    # Explicit [] means zero coverage. Canonicalize padded HK identities before dedupe.
    unique = {}
    for code, name in pool:
        code = str(code).upper()
        if code.endswith(".HK"): code = str(int(code[:-3])) + ".HK"
        unique.setdefault(code,(code,name))
    pool = list(unique.values())
    rows = []; unranked = []; failures = []
    errors = 0
    for code, name in pool:
        try:
            df = fetch_fn(code)
        except Exception:
            logging.exception("个股前瞻取数失败: %s", code)
            df = None
        if df is None or len(df) < min_bars:
            errors += 1
            failures.append({'code':code,'reason':'行情缺失或小于最小观察样本'})
            continue
        try:
            fwd = forward_fn(df, name=name, code=code)
        except Exception as exc:
            errors += 1; failures.append({'code':code,'reason':'规则计算失败：'+type(exc).__name__})
            continue
        if fwd.get("error"):
            errors += 1
            failures.append({'code':code,'reason':str(fwd['error'])})
            continue
        opp = opportunity_score(fwd)
        if opp['opp_score'] is None:
            unranked.append({'code':code,'name':name,'source_asof':fwd.get('data_asof'),**opp})
            continue
        rows.append({
            "code": code, "name": name, "market": _market_of(code),
            "sector": get_sector_fn(code, name) if get_sector_fn else "其他",
            "last": fwd.get("last"), "stage": fwd.get("stage"),
            "p_up": fwd.get("weighted_p_up"), "rr": fwd.get("weighted_rr"),
            "ev": fwd.get("weighted_expected_pct"),
            "overall": '↑ 规则条件满足' if opp['starting'] else '↔ 条件未同向',
            "suggestion": '只读观察线索；原评级和交易合同以中央审核为准',
            'source_asof':fwd.get('data_asof'), 'data_signature':fwd.get('data_signature'),
            'direction_score':fwd.get('weighted_p_up'), 'score_kind':'未校准规则方向分，非概率',
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
        "schema": "v88.forward-radar/1.2", "pool_source": source,
        "coverage_complete": bool(pool) and errors == 0 and not unranked,
        "scanned": len(rows), "skipped": errors, "pool_size": len(pool),
        "probability_kind": "未校准规则方向分（非概率或胜率）",
        'coverage_scope':'仅本次输入池，非全市场或全行业覆盖',
        'unranked_stocks':unranked, 'failures':failures,
        'no_grade_authority':True, 'entry_permission':False,
        "stocks": stocks[:top_n],
        "starting_stocks": [r for r in stocks if r["starting"]][:top_n],
        "sectors": sectors,
    }
