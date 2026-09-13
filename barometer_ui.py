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

from datetime import date
from html import escape
import math

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
# 周/月取各日偏离的均值；各日有自己的滚动基准，不能称为周均量对同一基准。
SHOW_DAYS = 500
# 沿用默认周聚合；它只改变显示颗粒度，不提供未来转折时间或预测精度。
# 【2026-08-01 用户"日期要一致:月线到去年8月,周线日线也要在这个区间内"】
# 原设计每档各带各的窗口(日120交易日/周52周/月24月),结果切换单位连时间段一起变,
# 三张图跨的根本不是同一段行情,没法对照——这是设计错误不是取舍。
# 改成**区间与单位彻底解耦**(专业看盘软件的做法):先选看多长,再选什么颗粒度。
# 区间用交易日计,三市场同一把尺;单位只决定桶宽,不再影响起止日期。
UNITS = {"日": "D", "周": "W", "月": "M"}
SPANS = {"3月": 63, "6月": 126, "1年": 250, "2年": 500}
DEFAULT_UNIT = "周"
DEFAULT_SPAN = "1年"


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _count(value):
    value = _number(value)
    return int(value) if value is not None and value >= 0 and value.is_integer() else None


def _text(value, missing="未提供"):
    return escape(str(value if value is not None and value != "" else missing))


def _day(value):
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except (TypeError, ValueError):
        return None


def _breadth_state(d):
    """保留生产端涨跌比 2/0.5、分化度 0.8 的界限，只描述已发生样本。"""
    ratio = _number(d.get("ad_ratio"))
    if ratio is None or ratio < 0 or _count(d.get("adv")) is None or _count(d.get("dec")) is None:
        return "○缺证·涨跌家数或涨跌比未取得"
    state = ("↑增强·该日样本上涨家数占优" if ratio >= 2 else
             "↓减弱·该日样本下跌家数占优" if ratio <= 0.5 else
             "↔分歧·该日样本涨跌分化")
    divergence = _number(d.get("divergence"))
    if divergence is not None and divergence > 0.8:
        state += "；⚠风险·样本中位涨幅低于指数"
    elif divergence is not None and divergence < -0.8:
        state += "；↔分歧·样本中位涨幅高于指数"
    return state


def breadth_html(bm: dict, markets=("A股", "港股", "美股")) -> str:
    """万得式涨跌分布柱状图 + 指数vs中位对照。bm = barometer.json 解析后的 dict。"""
    mks = bm.get("markets") or {}
    blocks = []
    for mk in markets:
        d = mks.get(mk) or {}
        bands = d.get("dist_bands") or []
        if not bands or d.get("error") or d.get("stale"):
            blocks.append(f"<div style='font-size:12px;color:#b45309'><b>{_text(mk)}</b> "
                          f"○缺证·{_text(d.get('error') or ('数据已标旧' if d.get('stale') else '宽度数据未取得'))}"
                          f"（源日期{_text(d.get('source_asof'))}）</div>")
            continue
        peak = max((_count(b.get("n")) or 0) for b in bands) or 1
        cols = []
        for b in bands:
            n = _count(b.get("n"))
            h = max(2, round(n / peak * 74)) if n else 0
            c = _BAND_COLOR.get(b.get("band"), "#94a3b8")
            cols.append(
                f"<div style='flex:1;display:flex;flex-direction:column;justify-content:flex-end;"
                f"align-items:center;height:92px' title='{_text(b.get('band'))}%: {_text(n, '○缺证')}只'>"
                f"<div style='font-size:9.5px;color:{c};line-height:1.1;margin-bottom:1px'>{_text(n, '○缺证')}</div>"
                f"<div style='width:72%;height:{h}px;background:{c};border-radius:2px 2px 0 0'></div>"
                f"</div>")
        axis = "".join(f"<div style='flex:1;text-align:center'>{a}</div>" for a in _AXIS)
        ix, md, dv = (_number(d.get(k)) for k in ("index_chg", "median_chg", "divergence"))
        cmp_txt = "　○缺证·指数或样本中位涨跌幅未取得"
        if ix is not None and md is not None:
            who = ("○缺证·分化度未取得" if dv is None else
                   "样本中位涨幅高于指数" if dv < -0.3 else
                   "样本中位涨幅低于指数" if dv > 0.3 else "指数与样本中位接近")
            col = "#b45309" if dv is None or abs(dv) > 0.3 else "#64748b"
            cmp_txt = (f"　指数<b>{ix:+.2f}%</b> vs 中位<b>{md:+.2f}%</b>"
                       f" <span style='color:{col}'>({who})</span>")
        lu, ld = _count(d.get("limit_up")), _count(d.get("limit_down"))
        lim = (f"　≥+9.8%档<b style='color:#b91c1c'>{_text(lu, '○缺证')}</b>"
               f"/≤−9.8%档<b style='color:#15803d'>{_text(ld, '○缺证')}</b>"
               "（近似幅度档）") if mk == "A股" else ""
        counts = {k: _count(d.get(k)) for k in ("adv", "dec", "flat")}
        observed = _count(d.get("sample_count", d.get("n")))
        coverage = d.get("coverage_note") or f"有效样本{observed if observed is not None else '未提供'}只；全交易所覆盖另核"
        notes = [f"源日期{_text(d.get('source_asof'))}", _text(coverage),
                 f"来源：{_text(d.get('source'))}"]
        if not _day(d.get("source_asof")):
            notes.append("○缺证·源日期未核验")
        if mk == "A股":
            notes.append("±9.8%为近似档，并非真实涨跌停数量")
        if d.get("dist_band_scope"):
            notes.append(_text(d["dist_band_scope"]))
        if all(counts[k] is not None for k in counts):
            band_counts = [_count(b.get("n")) for b in bands]
            if any(n is None for n in band_counts) or sum(band_counts) != counts["adv"] + counts["dec"]:
                notes.append("⚠风险·分档与涨跌家数未对齐")
        state = _breadth_state(d)
        blocks.append(
            f"<div style='margin:8px 0 2px;font-size:12px'><b>{_text(mk)}</b> "
            f"跌<b style='color:#16a34a'>{_text(counts['dec'], '○缺证')}</b>家　平{_text(counts['flat'], '○缺证')}家　"
            f"涨<b style='color:#dc2626'>{_text(counts['adv'], '○缺证')}</b>家{lim}{cmp_txt}</div>"
            f"<div style='display:flex;align-items:flex-end;border-bottom:1px solid #cbd5e1'>{''.join(cols)}</div>"
            f"<div style='display:flex;font-size:10px;color:#94a3b8;margin-top:1px'>{axis}</div>"
            f"<div style='font-size:10.5px;color:#64748b;margin:2px 0'>{state}</div>"
            f"<div style='font-size:10px;color:#64748b;margin-bottom:6px'>{'；'.join(notes)}</div>")
    if not blocks:
        return "<div style='font-size:12px;color:#94a3b8'>○缺证·宽度数据未取得</div>"
    failed = (f"<div style='font-size:11px;color:#b45309'>⚠风险·最近刷新失败：{_text(bm.get('last_failed_at'))}；"
              "以下保留原源日期的历史样本</div>") if bm.get("last_failed_at") else ""
    return ("<div style='font-size:11px;color:#94a3b8;margin-bottom:2px'>"
            "横轴=源日期涨跌幅(%)分档，纵轴=有效样本家数；平盘单列。"
            "↑增强/↓减弱描述该日样本宽度，单日读数不确认持续趋势。</div>" + failed + "".join(blocks))


def _aggregate(series: list, mode: str) -> list:
    """各日滚动偏离取桶内均值；保留部分桶及缺值，不补零、不删首桶。"""
    buckets = {}
    for p in series:
        raw = _day(p.get("date"))
        if not raw:
            continue
        day = date.fromisoformat(raw)
        iso = day.isocalendar()
        key = raw if mode == "D" else (iso[0], iso[1]) if mode == "W" else (day.year, day.month)
        b = buckets.setdefault(key, {"vals": [], "raw": [], "dates": [], "missing": [], "short": 0})
        rel, value = _number(p.get("rel_pct")), _number(p.get("value"))
        b["dates"].append(raw)
        if rel is None:
            b["missing"].append(raw)
        else:
            b["vals"].append(rel)
        if value is not None and value > 0:
            b["raw"].append(value)
        window = _count(p.get("window_n"))
        if window is not None and window < 20:
            b["short"] += 1
    return [{"date": max(b["dates"]), "start_date": min(b["dates"]),
             "rel_pct": round(sum(b["vals"]) / len(b["vals"]), 1) if b["vals"] else None,
             "value": sum(b["raw"]) / len(b["raw"]) if b["raw"] else None,
             "days": len(b["vals"]), "observed_days": len(b["dates"]),
             "missing_dates": b["missing"], "short_baseline_days": b["short"]}
            for _, b in sorted(buckets.items())]


def amount_daily_html(ad: dict, height: int = 170, unit: str = DEFAULT_UNIT,
                      span: str = DEFAULT_SPAN) -> str:
    """已发生的成交活跃度；量/额和自身滚动基准比较，不推断净资金方向。"""
    mode = UNITS.get(unit, UNITS[DEFAULT_UNIT])
    unit = unit if unit in UNITS else DEFAULT_UNIT
    span_label = span if span in SPANS else DEFAULT_SPAN
    days = SPANS[span_label]
    mks = ad.get("markets") or {}
    usable, notices = {}, []
    for mk, v in mks.items():
        quality = v.get("data_quality") or {}
        source_day = _day(v.get("latest_date"))
        if v.get("error") or v.get("stale") or quality.get("latest_session_verified") is False:
            notices.append(f"{mk}：○缺证·{v.get('error') or '最近交易日未核验'}（数据至{source_day or '未取得'}）")
            continue
        raw = v.get("series") or []
        dates = [_day(p.get("date")) for p in raw]
        if not raw or any(d is None for d in dates) or dates != sorted(set(dates)):
            notices.append(f"{mk}：○缺证·量能日期缺失、重复或无序（数据至{source_day or '未取得'}）")
            continue
        selected = raw[-days:]
        agg = _aggregate(selected, mode)
        if not any(p["rel_pct"] is not None for p in agg):
            notices.append(f"{mk}：○缺证·量能偏离未取得（数据至{source_day or dates[-1]}）")
            continue
        continuity = quality.get("continuity") or {}
        gap_dates = sorted({_day(d) for d in continuity.get("missing_sessions", []) if _day(d)} |
                           {p["date"] for p in selected if _number(p.get("rel_pct")) is None})
        usable[mk] = {**v, "series": agg, "_raw": raw, "_selected": selected,
                      "_gaps": [d for d in gap_dates if dates[-min(days, len(dates))] <= d <= dates[-1]]}
        if v.get("history_warning"):
            notices.append(f"{mk}：○缺证·{v['history_warning']}（数据至{source_day or dates[-1]}）")
        if len(selected) < days:
            notices.append(f"{mk}：○缺证·所选窗口需{days}个源交易日，实际{len(selected)}日（{selected[0]['date']} ~ {selected[-1]['date']}）")
        if usable[mk]["_gaps"]:
            notices.append(f"{mk}：○缺证·窗口内{len(usable[mk]['_gaps'])}个缺值或缺交易日，跨缺口断线")
        if not source_day or source_day != dates[-1]:
            notices.append(f"{mk}：○缺证·最新源日期与序列末日未对齐；序列至{dates[-1]}")
        if v.get("relative_basis") != "rolling20-through-each-date-v1":
            notices.append(f"{mk}：○缺证·滚动基准口径未核验，仅展示原始偏离记录")
    if not usable:
        return ("<div style='font-size:12px;color:#b45309'>○缺证·量能走势不可用（"
                + _text("；".join(notices), "无数据") + "）</div>")

    W, PAD, LAB = 620, 30, 15
    H, PH = max(80, height), max(80, height) - LAB
    magnitudes = sorted(abs(p["rel_pct"]) for v in usable.values() for p in v["series"]
                        if p["rel_pct"] is not None)
    # 保留既有95分位截轴；真实读数在点提示中完整显示。
    axis_span = max(10, round(magnitudes[int(len(magnitudes) * 0.95)] / 5) * 5)
    clipped = sum(x > axis_span for x in magnitudes)
    shared_dates = sorted({p['date'] for v in usable.values() for p in v['series']})
    first_day = date.fromisoformat(shared_dates[0]).toordinal()
    date_span = max(1, date.fromisoformat(shared_dates[-1]).toordinal() - first_day)

    def xy(day, rel):
        position = (date.fromisoformat(day).toordinal() - first_day) / date_span
        x = PAD + (W - PAD - 8) * position
        rel = max(-axis_span, min(axis_span, rel))
        return x, PH / 2 - (rel / axis_span) * (PH / 2 - 10)

    import statistics
    paths, legend, caps = [], [], []
    for mk, v in usable.items():
        s, color = v["series"], _LINE_COLOR.get(mk, "#64748b")
        segments, segment, previous = [], [], None
        for p in s:
            rel = p["rel_pct"]
            crosses_gap = previous and any(previous <= d <= p["date"] for d in v["_gaps"])
            if rel is None or crosses_gap or p["missing_dates"]:
                if segment:
                    segments.append(segment)
                segment = []
            if rel is None:
                previous = None
                continue
            x, y = xy(p["date"], rel)
            segment.append(f"{x:.1f},{y:.1f}")
            title = (f"{mk} {p['start_date']} ~ {p['date']}：偏离{rel:+.1f}%；"
                     f"有效{p['days']}日 / 返回{p['observed_days']}日")
            if p["short_baseline_days"]:
                title += f"；{p['short_baseline_days']}日基准不足20日"
            if abs(rel) > axis_span:
                title += f"（超出纵轴{axis_span:.0f}%已截顶）"
            paths.append(f"<circle cx='{x:.1f}' cy='{y:.1f}' r='2.0' fill='{color}'>"
                         f"<title>{_text(title)}</title></circle>")
            previous = p["date"]
        if segment:
            segments.append(segment)
        for points in segments:
            if len(points) >= 2:
                paths.append(f"<polyline points='{' '.join(points)}' fill='none' stroke='{color}' "
                             "stroke-width='1.9' stroke-linejoin='round'/>")
        vs, latest = _number(v.get("vs_ma20_pct")), _number(v.get("latest"))
        basis_ok = v.get("relative_basis") == "rolling20-through-each-date-v1"
        latest_ok = _day(v.get("latest_date")) == _day(v["_raw"][-1]["date"])
        state = "○缺证·最新偏离或基准未核验"
        if vs is not None and basis_ok and latest_ok:
            state = ("↑增强·量能高于基准" if vs >= 10 else "↓减弱·量能低于基准" if vs <= -10
                     else "↔分歧·量能未明显偏离基准")
        last_window = _count(v["_raw"][-1].get("window_n"))
        if last_window is not None and last_window < 20:
            state += f"；○缺证·20日基准不足，实际{last_window}日"
        raw_values = [_number(p.get("value")) for p in v["_raw"][-250:]]
        valid_values = [x for x in raw_values if x is not None and x > 0]
        pos = ""
        if len(valid_values) == len(raw_values) and len(valid_values) >= 60:
            med = statistics.median(valid_values)
            r20 = sum(valid_values[-20:]) / 20
            pos = (f" · 近20个源交易日均值vs近{len(valid_values)}个源交易日中位"
                   f"{(r20 / med - 1) * 100:+.0f}%")
        latest_text = f"{latest:g}{_text(v.get('unit'))}" if latest is not None else "○缺证·最新量能"
        vs_text = f"{vs:+.1f}%" if vs is not None else "○缺证"
        legend.append(f"<span style='min-width:0;max-width:100%;overflow-wrap:anywhere'><span style='display:inline-block;width:9px;height:9px;"
                      f"background:{color};border-radius:2px;margin-right:3px'></span><b>{_text(mk)}</b> "
                      f"{latest_text} · 源日期{_text(v.get('latest_date'))} · 偏离{vs_text} · {state}{pos}</span>")
        caps.append(f"{mk}={v.get('label') or '样本范围未提供'}；来源{v.get('source') or '未提供'}；"
                    f"所画{len(s)}桶/{len(v['_selected'])}个源交易日；"
                    f"{v['_selected'][0]['date']} ~ {v['_selected'][-1]['date']}")

    grid = "".join(
        f"<line x1='{PAD}' y1='{PH/2 - k*(PH/2-10):.1f}' x2='{W-8}' y2='{PH/2 - k*(PH/2-10):.1f}' "
        f"stroke='#e2e8f0' stroke-width='1' stroke-dasharray='{'0' if k == 0 else '3,3'}'/>"
        f"<text x='0' y='{PH/2 - k*(PH/2-10) + 3:.1f}' font-size='9' fill='#94a3b8'>{k*axis_span:+.0f}%</text>"
        for k in (1, 0.5, 0, -0.5, -1))
    tick_indices = sorted({0, len(shared_dates) - 1} | set(range(0, len(shared_dates), max(1, (len(shared_dates)-1)//5))))
    ticks = ""
    for i in tick_indices:
        d = shared_dates[i]
        x, _ = xy(d, 0)
        lab = f"{d[2:4]}/{d[5:7]}" if days > 250 else f"{d[5:7]}/{d[8:10]}"
        anchor = 'start' if i == 0 else 'end' if i == len(shared_dates) - 1 else 'middle'
        ticks += (f"<line x1='{x:.1f}' y1='0' x2='{x:.1f}' y2='{PH}' stroke='#f1f5f9'/>"
                  f"<text x='{x:.1f}' y='{H-3}' font-size='9' fill='#94a3b8' text-anchor='{anchor}'>{lab}</text>")
    first = min(v["_selected"][0]["date"] for v in usable.values())
    last = max(v["_selected"][-1]["date"] for v in usable.values())
    return (f"<div style='font-size:11px;color:#64748b;margin-bottom:2px'>"
            f"历史窗口={span_label}（最多{days}个源交易日；实际{first} ~ {last}）；按{unit}聚合。"
            "纵轴=各日偏离的桶内均值；已声明滚动口径时，各日相对截至该日20日均量，"
            "不足20日按已有窗口。首末桶保留实际日数；不同市场日期与单位分别披露。"
            "成交额/成交量描述活跃度，不能据此判断净资金流向或未来涨跌。"
            + (f" ⚠风险·{clipped}点超出纵轴已截顶（最大绝对偏离{magnitudes[-1]:.0f}%）" if clipped else "")
            + "</div>"
            f"<svg viewBox='0 0 {W} {H}' style='width:100%;height:auto'>{ticks}{grid}{''.join(paths)}</svg>"
            f"<div style='font-size:11px;margin-top:2px;display:flex;flex-wrap:wrap;gap:4px 14px'>{''.join(legend)}</div>"
            f"<div style='font-size:10px;color:#64748b;margin-top:1px'>口径：{_text(' · '.join(caps))}</div>"
            + (f"<div style='font-size:11px;color:#b45309'>{_text('；'.join(notices))}</div>" if notices else ""))
