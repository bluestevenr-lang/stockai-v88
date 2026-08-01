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
# 显示窗口：用户2026-08-01"日期尽可能多"——120个交易日(约半年)，
# 覆盖一个完整的季度级放量/缩量周期；600px画布放120个点每点仍有5px，线不糊。
# 尖峰压平其它线的问题不靠砍数据解决，改用下面的分位截顶(见 _axis_span)。
SHOW_DAYS = 120


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


def amount_daily_html(ad: dict, height: int = 150, days: int = SHOW_DAYS) -> str:
    """三市场每日量能走势（相对各自20日均量的偏离%）。ad = market_amount_daily.json。

    为什么画相对值而不是绝对值：三市场单位不同（亿元/亿港元/亿股），
    绝对值同图＝没法比；相对20日均量的偏离才是"钱在进还是在退"的可比刻度。
    这也和截图那条 −35%~35% 的曲线口径一致。
    """
    mks = ad.get("markets") or {}
    usable = {k: {**v, "series": (v["series"] or [])[-days:]}
              for k, v in mks.items() if v.get("series") and not v.get("error")}
    if not usable:
        errs = "；".join(f"{k}:{v.get('error')}" for k, v in mks.items() if v.get("error"))
        return f"<div style='font-size:12px;color:#b45309'>量能日线不可用（{errs or '无数据'}）</div>"

    W, H, PAD = 620, height, 26
    # 纵轴按 |偏离| 的95分位取，不按最大值取：单根尖峰(如美股06-23的+106%)会把整根轴撑满、
    # 另两条压成直线。超出的点截到边界并在标题里报数——截了多少必须说，不许悄悄削平。
    _abs = sorted(abs(p.get("rel_pct") or 0)
                  for v in usable.values() for p in v["series"])
    span = max(20, round((_abs[int(len(_abs) * 0.95)] if _abs else 10) / 5) * 5)
    _peak = _abs[-1] if _abs else 0
    _clipped = sum(1 for x in _abs if x > span)
    n_max = max(len(v["series"]) for v in usable.values())

    def xy(i, n, rel):
        x = PAD + (W - PAD - 8) * (i / max(1, n - 1))
        rel = max(-span, min(span, rel))               # 截到边界,不画到画布外
        y = H / 2 - (rel / span) * (H / 2 - 12)
        return f"{x:.1f},{y:.1f}"

    paths, legend = [], []
    for mk, v in usable.items():
        s = v["series"]
        pts = " ".join(xy(i, len(s), p.get("rel_pct") or 0) for i, p in enumerate(s))
        c = _LINE_COLOR.get(mk, "#64748b")
        paths.append(f"<polyline points='{pts}' fill='none' stroke='{c}' stroke-width='1.9' "
                     f"stroke-linejoin='round'/>")
        vs = v.get("vs_ma20_pct")
        legend.append(
            f"<span style='white-space:nowrap'>"
            f"<span style='display:inline-block;width:9px;height:9px;background:{c};"
            f"border-radius:2px;margin-right:3px'></span>"
            f"<b>{mk}</b> {v.get('latest')}{v.get('unit')} "
            f"<span style='color:{'#dc2626' if (vs or 0) > 0 else '#16a34a'}'>{vs:+.1f}%</span>"
            f" <span style='color:#64748b'>{v.get('verdict')}</span></span>")
    grid = "".join(
        f"<line x1='{PAD}' y1='{H/2 - k*(H/2-12)}' x2='{W-8}' y2='{H/2 - k*(H/2-12)}' "
        f"stroke='#e2e8f0' stroke-width='1' stroke-dasharray='{'0' if k == 0 else '3,3'}'/>"
        f"<text x='0' y='{H/2 - k*(H/2-12) + 3}' font-size='9' fill='#94a3b8'>{k*span:+.0f}%</text>"
        for k in (1, 0.5, 0, -0.5, -1))
    caps = " · ".join(f"{k}={v.get('label')}" for k, v in usable.items())
    return (f"<div style='font-size:11px;color:#94a3b8;margin-bottom:2px'>"
            f"纵轴=当日量能相对自身20日均量的偏离(%)，横轴=最近{n_max}个交易日（约"
            f"{n_max // 21}个月）；单位各市场不同故只比形状不比绝对值"
            + (f"；<b>{_clipped}个点超出纵轴已截顶</b>(区间最大{_peak:.0f}%)" if _clipped else "")
            + "</div>"
            f"<svg viewBox='0 0 {W} {H}' style='width:100%;height:auto'>{grid}{''.join(paths)}</svg>"
            f"<div style='font-size:11px;margin-top:2px;display:flex;flex-wrap:wrap;"
            f"gap:4px 14px'>{''.join(legend)}</div>"
            f"<div style='font-size:10px;color:#94a3b8;margin-top:1px'>口径：{caps}</div>")
