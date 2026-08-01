"""barometer_ui.py — 【V88·市场晴雨表渲染】三端共用（桌面/云端同源，仿 rotation_ui 的做法）

2026-08-01 用户截图案，两件事：
  ① 市场宽度改**万得式十档柱状图**（横轴 -7 -5 -3 -1 0 1 3 5 7，每档标家数）——
     原来的七档堆叠条里"涨0~5%"一根吞掉 3555 只，看不出是贴着 0 徘徊还是真在涨。
  ② 底部资金走势由**当日分时**改成**每日走势**，并且中美港三市场同图。

实现刻意只用内联 SVG + HTML：桌面端和云端是两套 app，但都吃 unsafe_allow_html，
不引第三方图表库就不会出现"桌面能画云端画不出"的分叉。

调用方只负责把 json 读进来（桌面读本地 data/，云端读 pub/），渲染逻辑只此一份。
"""
from __future__ import annotations

# 跌绿涨红（A股习惯，与截图一致）；由深到浅表示极端到温和
_BAND_COLOR = {
    "≤-7": "#15803d", "-7~-5": "#16a34a", "-5~-3": "#22c55e",
    "-3~-1": "#4ade80", "-1~0": "#86efac",
    "0~1": "#fca5a5", "1~3": "#f87171", "3~5": "#ef4444",
    "5~7": "#dc2626", "≥7": "#b91c1c",
}
_AXIS = ["-7", "-5", "-3", "-1", "0", "1", "3", "5", "7"]
# 2026-08-01 用户指定配色：中国红不变、香港黄、美股亮蓝。
# 黄色在白底上最弱，故三条线统一加粗到 1.9，保证黄线不糊。
_LINE_COLOR = {"中国": "#dc2626", "港股": "#eab308", "美股": "#0ea5e9"}
# 显示窗口：按**周**聚合(用户2026-08-01"横轴变为周为单位")。
# 日线受屏幕限制只能放~120天，改周单位后同样宽度能放下整整一年(~52个点)，
# 手机上每点仍有~7px——"日期尽可能多"和"看得清"这次同时满足。
# 周内取日偏离的均值：因 rel 对量能是线性的，mean(v_i/ma20−1) == mean(v_i)/ma20−1，
# 即"该周平均量能 vs 最近20日常态"，与日线口径完全一致，不是另一把尺子。
SHOW_DAYS = 500
# 三档单位与各自窗口(2026-08-01 用户"可选日和周,像下面的周轮转一样")。
# 窗口按分辨率配:日档看半年、周档看一年、月档看两年——每档都落在"点数够多又看得清"的区间。
# 实测 Kaufman 效率比(净变化/路径总长,越高趋势越干净,季度跨度):
#   日线 0.03~0.06 = 净移动1格线要走30格,基本是噪音；周线 0.12~0.16 好3~5倍；
#   月线 0.38~0.60 最干净但一年12点、转折要1~2月才确认,对1~2周决策节奏太钝。
# 故默认周。(另:实测周内星期几效应只有±3%,可忽略——周聚合的价值在信噪比不在日历效应)
# 【2026-08-01 用户"日期要一致:月线到去年8月,周线日线也要在这个区间内"】
# 原设计每档各带各的窗口(日120交易日/周52周/月24月),结果切换单位连时间段一起变,
# 三张图跨的根本不是同一段行情,没法对照——这是设计错误不是取舍。
# 改成**区间与单位彻底解耦**(专业看盘软件的做法):先选看多长,再选什么颗粒度。
# 区间用交易日计,三市场同一把尺;单位只决定桶宽,不再影响起止日期。
UNITS = {"日": "D", "周": "W", "月": "M"}
SPANS = {"3月": 63, "6月": 126, "1年": 250, "2年": 500}
DEFAULT_UNIT = "周"
DEFAULT_SPAN = "1年"


def breadth_html(bm: dict, markets=("A股", "港股", "美股")) -> str:
    """万得式涨跌分布柱状图 + 指数vs中位对照。bm = barometer.json 解析后的 dict。"""
    mks = bm.get("markets") or {}
    blocks = []
    for mk in markets:
        d = mks.get(mk) or {}
        bands = d.get("dist_bands") or []
        if not bands:
            continue
        peak = max((b.get("n") or 0) for b in bands) or 1
        cols = []
        for b in bands:
            n = b.get("n") or 0
            h = max(2, round(n / peak * 74))          # 柱高按最高档归一
            c = _BAND_COLOR.get(b.get("band"), "#94a3b8")
            cols.append(
                f"<div style='flex:1;display:flex;flex-direction:column;justify-content:flex-end;"
                f"align-items:center;height:92px' title='{b.get('band')}%: {n}只'>"
                f"<div style='font-size:9.5px;color:{c};line-height:1.1;margin-bottom:1px'>{n}</div>"
                f"<div style='width:72%;height:{h}px;background:{c};border-radius:2px 2px 0 0'></div>"
                f"</div>")
        axis = "".join(f"<div style='flex:1;text-align:center'>{a}</div>" for a in _AXIS)
        ix, md, dv = d.get("index_chg"), d.get("median_chg"), d.get("divergence")
        cmp_txt = ""
        if ix is not None and md is not None:
            who = ("个股跑赢指数" if (dv or 0) < -0.3 else
                   ("指数靠权重扛" if (dv or 0) > 0.3 else "指数与个股同步"))
            col = "#16a34a" if (dv or 0) < -0.3 else ("#b45309" if (dv or 0) > 0.3 else "#64748b")
            cmp_txt = (f"　指数<b>{ix:+.2f}%</b> vs 中位<b>{md:+.2f}%</b>"
                       f" <span style='color:{col}'>({who})</span>")
        lu = d.get("limit_up") or 0
        ld = d.get("limit_down") or 0
        lim = f"　涨停<b style='color:#b91c1c'>{lu}</b>/跌停<b style='color:#15803d'>{ld}</b>" if (lu or ld) else ""
        blocks.append(
            f"<div style='margin:8px 0 2px;font-size:12px'><b>{mk}</b> "
            f"跌<b style='color:#16a34a'>{d.get('dec')}</b>家　平{d.get('flat')}家　"
            f"涨<b style='color:#dc2626'>{d.get('adv')}</b>家{lim}{cmp_txt}</div>"
            f"<div style='display:flex;align-items:flex-end;border-bottom:1px solid #cbd5e1'>{''.join(cols)}</div>"
            f"<div style='display:flex;font-size:10px;color:#94a3b8;margin-top:1px'>{axis}</div>"
            f"<div style='font-size:10.5px;color:#64748b;margin:2px 0 6px'>→ {d.get('verdict') or ''}</div>")
    if not blocks:
        return "<div style='font-size:12px;color:#94a3b8'>宽度数据生成中</div>"
    return ("<div style='font-size:11px;color:#94a3b8;margin-bottom:2px'>"
            "横轴=当日涨跌幅(%)分档，纵轴=家数；全市场逐只统计非抽样</div>" + "".join(blocks))


def _aggregate(series: list, mode: str) -> list:
    """日序列 → 日/周/月序列。桶内取 rel_pct 均值。
    因 rel 对量能是线性的，mean(v_i/base−1) == mean(v_i)/base−1，
    即"该周(月)平均量能 vs 同一基准"——三档共用一把尺子，不是三套口径。
    date 记为桶内最后一个交易日，横轴刻度按它标。"""
    if mode == "D":
        return [{"date": str(p.get("date"))[:10], "rel_pct": p.get("rel_pct") or 0,
                 "value": p.get("value"), "days": 1} for p in series
                if p.get("rel_pct") is not None]
    from datetime import date as _d
    buckets: dict = {}
    for p in series:
        raw = str(p.get("date"))[:10]
        try:
            y, m, dd = (int(x) for x in raw.split("-"))
            iso = _d(y, m, dd).isocalendar()
            key = (iso[0], iso[1]) if mode == "W" else (y, m)
        except (ValueError, TypeError):
            continue
        b = buckets.setdefault(key, {"vals": [], "raw": [], "last": "", "n": 0})
        if p.get("rel_pct") is not None:
            b["vals"].append(float(p["rel_pct"]))
        if p.get("value") is not None:
            b["raw"].append(float(p["value"]))
        b["last"] = max(b["last"], raw)
        b["n"] += 1
    out = []
    for key in sorted(buckets):
        b = buckets[key]
        if not b["vals"]:
            continue
        out.append({"date": b["last"], "rel_pct": round(sum(b["vals"]) / len(b["vals"]), 1),
                    "value": (sum(b["raw"]) / len(b["raw"])) if b["raw"] else None,
                    "days": b["n"]})
    # 丢掉不完整的**首**桶：窗口切下来的第一周/月常只剩一两天(港股实测1天)，
    # 一天的均值当一整周画在最左端是纯噪音。末桶不丢——它是"至今"，是真实进度。
    _min = 3 if mode == "W" else (10 if mode == "M" else 1)
    while out and out[0]["days"] < _min:
        out.pop(0)
    return out


def amount_daily_html(ad: dict, height: int = 170, unit: str = DEFAULT_UNIT,
                      span: str = DEFAULT_SPAN) -> str:
    """三市场量能走势（日/周/月三档，相对最近20日均量的偏离%）。

    为什么画相对值而不是绝对值：三市场单位不同（亿元/亿港元/亿股），绝对值同图＝没法比；
    相对基准的偏离才是"钱在进还是在退"的可比刻度。
    """
    mode = UNITS.get(unit, UNITS[DEFAULT_UNIT])
    days = SPANS.get(span, SPANS[DEFAULT_SPAN])
    mks = ad.get("markets") or {}
    usable = {}
    for k, v in mks.items():
        if not v.get("series") or v.get("error"):
            continue
        # 先按**区间**切日线，再按单位聚合——顺序反过来就会出现"周档和日档跨不同时间段"
        agg = _aggregate((v["series"] or [])[-days:], mode)
        if agg:
            usable[k] = {**v, "series": agg, "_raw": v["series"]}
    if not usable:
        errs = "；".join(f"{k}:{v.get('error')}" for k, v in mks.items() if v.get("error"))
        return f"<div style='font-size:12px;color:#b45309'>量能走势不可用（{errs or '无数据'}）</div>"

    W, PAD, LAB = 620, 30, 15
    H, PH = height, height - LAB
    _abs = sorted(abs(p["rel_pct"]) for v in usable.values() for p in v["series"])
    # 纵轴取95分位而非最大值：单根尖峰会把整轴撑满、其余压成直线。超出者截到边界并报数。
    span = max(10, round((_abs[int(len(_abs) * 0.95)] if _abs else 10) / 5) * 5)
    _peak = _abs[-1] if _abs else 0
    _clipped = sum(1 for x in _abs if x > span)
    n_max = max(len(v["series"]) for v in usable.values())

    def xy(i, n, rel):
        x = PAD + (W - PAD - 8) * (i / max(1, n - 1))
        rel = max(-span, min(span, rel))
        return x, PH / 2 - (rel / span) * (PH / 2 - 10)

    import statistics as _st
    paths, legend = [], []
    for mk, v in usable.items():
        s = v["series"]
        c = _LINE_COLOR.get(mk, "#64748b")
        pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in
                       (xy(i, len(s), p["rel_pct"]) for i, p in enumerate(s)))
        paths.append(f"<polyline points='{pts}' fill='none' stroke='{c}' stroke-width='1.2' "
                     f"stroke-linejoin='round'/>")
        for i, p in enumerate(s):
            if abs(p["rel_pct"]) > span:
                x, y = xy(i, len(s), p["rel_pct"])
                paths.append(f"<circle cx='{x:.1f}' cy='{y:.1f}' r='2.2' fill='{c}' "
                             f"stroke='#fff' stroke-width='0.8'><title>{p['date']} "
                             f"{p['rel_pct']:+.1f}%（超出纵轴{span:.0f}%已截顶）</title></circle>")
        vs = v.get("vs_ma20_pct")
        # 【2026-08-01 实测发现】只报 vs20日均会把"最后一天的跳动"读成趋势：
        # 美股 vs20日均+50%看着是大放量，但近4周均量其实比全年中位低11.8%。
        # 故同时给"近4周 vs 全年中位"——一个答当下、一个答水位，缺一会误判。
        _raw = [p["value"] for p in v.get("_raw") or [] if p.get("value")]
        pos = ""
        if len(_raw) >= 60:
            _med = _st.median(_raw[-250:])
            _r4 = sum(_raw[-20:]) / 20
            pos = (f" <span style='color:#64748b'>·近4周vs全年中位"
                   f"{(_r4 / _med - 1) * 100:+.0f}%</span>") if _med else ""
        legend.append(
            f"<span style='white-space:nowrap'>"
            f"<span style='display:inline-block;width:9px;height:9px;background:{c};"
            f"border-radius:2px;margin-right:3px'></span>"
            f"<b>{mk}</b> {v.get('latest')}{v.get('unit')} "
            f"<span style='color:{'#dc2626' if (vs or 0) > 0 else '#16a34a'}'>{vs:+.1f}%</span>"
            f"{pos}</span>")

    grid = "".join(
        f"<line x1='{PAD}' y1='{PH/2 - k*(PH/2-10):.1f}' x2='{W-8}' y2='{PH/2 - k*(PH/2-10):.1f}' "
        f"stroke='#e2e8f0' stroke-width='1' stroke-dasharray='{'0' if k == 0 else '3,3'}'/>"
        f"<text x='0' y='{PH/2 - k*(PH/2-10) + 3:.1f}' font-size='9' fill='#94a3b8'>{k*span:+.0f}%</text>"
        for k in (1, 0.5, 0, -0.5, -1))

    ticks = ""
    ref = max(usable.values(), key=lambda v: len(v["series"]))["series"]
    n = len(ref)
    for i in range(0, n, max(1, (n - 1) // 5)):
        x, _ = xy(i, n, 0)
        d = str(ref[i]["date"])
        # 刻度格式跟**区间**走不跟单位走:跨年的区间标 年/月,一年内标 月/日
        lab = f"{d[2:4]}/{d[5:7]}" if days > 250 else f"{d[5:7]}/{d[8:10]}"
        ticks += (f"<line x1='{x:.1f}' y1='0' x2='{x:.1f}' y2='{PH}' stroke='#f1f5f9'/>"
                  f"<text x='{x:.1f}' y='{H-3}' font-size='9' fill='#94a3b8' "
                  f"text-anchor='middle'>{lab}</text>")

    _first = min(str(v["series"][0]["date"]) for v in usable.values())
    _last = max(str(v["series"][-1]["date"]) for v in usable.values())
    _unit_txt = {"D": "个交易日", "W": "周", "M": "个月"}[mode]
    # 起止日期写进标题:三档切换时你能一眼确认看的是同一段行情
    _span_txt = f"{span}（{_first} ~ {_last}）· {n_max}{_unit_txt}"
    caps = " · ".join(f"{k}={v.get('label')}" for k, v in usable.items())
    return (f"<div style='font-size:11px;color:#94a3b8;margin-bottom:2px'>"
            f"纵轴=每{unit}平均量能相对<b>最近20日均量</b>(固定基准,非滚动)的偏离(%)，"
            f"横轴={_span_txt}；单位各市场不同故只比形状不比绝对值"
            + (f"；<b>{_clipped}点超出纵轴已截顶</b>(最大{_peak:.0f}%)" if _clipped else "")
            + "</div>"
            f"<svg viewBox='0 0 {W} {H}' style='width:100%;height:auto'>{ticks}{grid}{''.join(paths)}</svg>"
            f"<div style='font-size:11px;margin-top:2px;display:flex;flex-wrap:wrap;"
            f"gap:4px 14px'>{''.join(legend)}</div>"
            f"<div style='font-size:10px;color:#94a3b8;margin-top:1px'>口径：{caps}</div>")
