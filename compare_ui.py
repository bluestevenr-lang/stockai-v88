"""compare_ui.py — 【V88·个股深度对比】2026-08-01 用户"可对比的个股选项+从历史挑历史个股深度对比,最多四只"

原「⚔️股票PK对决」是一张12列指标表：参数并列 ≠ 深度对比。
用户看完还是得自己心算"那我买哪只"——这正是定纲里被点名的"密而乱=没灵魂"。

本模块让对比回答四个问题（按用户看的顺序排）：
  ① **谁值得买** —— 排序不并列，第一名直接标出来，配明确动词(买/不买/持有)+区间+失效价。
     定纲：禁"观察""评估减仓"这类和稀泥措辞，决策粒度=日。
  ② **它们是不是同一个赌注** —— 实测60日收益率两两相关系数。
     四只票若相关系数都≥0.7，那不是"从四只里挑一只"，是"把同一注下了四遍"。
     这是用户四账户实证里真实亏过钱的模式（科技成长股−12.2%，因为买的是同一个beta）。
     不用行业标签而用实测相关性：行业是代理变量，相关性才是真相。
  ③ **走势谁强** —— 归一化到同一起点的相对走势图，一眼看出谁在领跑谁在拖后腿。
     欧奈尔的相对强度思想：不看绝对涨幅，看同期跑赢跑输。
  ④ **各自在什么位置** —— 52周位/量比/赔率/距止损，同一把尺子横排。

只用内联SVG+HTML，与 barometer_ui 同一套做法，不引图表库，桌面云端都能渲染。
本模块纯计算+渲染，不调AI、不写文件、零token。
"""
from __future__ import annotations

import math

MAX_COMPARE = 4          # 用户2026-08-01定：最多四只（原对比篮上限5，与模块标题"2-4只"自相矛盾）

# 四条线的配色：辨识度优先，避免相邻色相
_C = ["#dc2626", "#0ea5e9", "#eab308", "#7c3aed"]


def _returns(closes: list) -> list:
    return [closes[i] / closes[i - 1] - 1 for i in range(1, len(closes))
            if closes[i - 1]]


def correlation(a: list, b: list) -> float | None:
    """两只股票日收益率的皮尔逊相关系数。用收益率不用价格——
    价格相关是伪相关（两只都长期上涨就高相关），收益率相关才是"是否同涨同跌"。"""
    ra, rb = _returns(a), _returns(b)
    n = min(len(ra), len(rb))
    if n < 20:
        return None
    ra, rb = ra[-n:], rb[-n:]
    ma, mb = sum(ra) / n, sum(rb) / n
    va = sum((x - ma) ** 2 for x in ra)
    vb = sum((x - mb) ** 2 for x in rb)
    if va <= 0 or vb <= 0:
        return None
    cov = sum((ra[i] - ma) * (rb[i] - mb) for i in range(n))
    return round(cov / math.sqrt(va * vb), 2)


def family_html(hist: dict, days: int = 60) -> str:
    """同族敞口检测：两两相关系数矩阵 + 人话结论。
    hist = {name: [close,...]}（按时间升序）。"""
    names = list(hist)
    if len(names) < 2:
        return ""
    pairs, hi = [], []
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            c = correlation(hist[names[i]][-days:], hist[names[j]][-days:])
            if c is None:
                continue
            pairs.append((names[i], names[j], c))
            if c >= 0.7:
                hi.append((names[i], names[j], c))
    if not pairs:
        return ("<div style='font-size:12px;color:#94a3b8'>同族检测：K线样本不足"
                f"({days}日内不足20个收益率点)，本次不下结论</div>")
    rows = "".join(
        f"<span style='white-space:nowrap;margin-right:12px'>{a}×{b} "
        f"<b style='color:{'#dc2626' if c >= 0.7 else ('#b45309' if c >= 0.4 else '#16a34a')}'>"
        f"{c:+.2f}</b></span>" for a, b, c in pairs)
    mid = [x for x in pairs if 0.5 <= x[2] < 0.7]
    if hi:
        who = "、".join(f"{a}×{b}({c:+.2f})" for a, b, c in hi)
        head = (f"<div style='font-size:12.5px;color:#dc2626;font-weight:600'>"
                f"⚠️ 这不是四选一，是同一注下多遍：{who} 相关系数≥0.7</div>"
                f"<div style='font-size:11.5px;color:#64748b'>"
                f"同向标的只该占一个仓位。要分散就换低相关的，别在同一个beta上加倍。</div>")
    elif mid:
        # 【实跑发现 2026-08-01】腾讯×阿里=+0.68 卡在0.7下方，却拿到"✅真正的四选一"的
        # 干净体检报告——0.68 实际已经很同向了。单一硬门槛会在边界上给出误导性结论，
        # 故补中间档：0.5~0.7 点名但不判死，让人自己拿捏。
        who = "、".join(f"{a}×{b}({c:+.2f})" for a, b, c in sorted(mid, key=lambda x: -x[2]))
        head = (f"<div style='font-size:12.5px;color:#b45309;font-weight:600'>"
                f"🟡 部分同向，不是干净的四选一：{who}</div>"
                f"<div style='font-size:11.5px;color:#64748b'>"
                f"这几对同涨同跌的成分不小，同时买等于变相加仓；要么二选一，要么各减半。</div>")
    else:
        head = ("<div style='font-size:12.5px;color:#16a34a;font-weight:600'>"
                "✅ 彼此相关性不高，是真正的四选一（选谁就是选谁，不是加倍下注）</div>")
    return (head + f"<div style='font-size:11px;color:#64748b;margin-top:2px'>"
            f"近{days}日收益率相关：{rows}</div>")


def trend_svg(hist: dict, days: int = 60, height: int = 180) -> str:
    """归一化相对走势：各自起点=0%，看同期谁跑赢。
    比绝对价格图有用——欧奈尔看的是相对强度，不是价格高低。"""
    series = {n: v[-days:] for n, v in hist.items() if len(v) >= 5}
    if not series:
        return "<div style='font-size:12px;color:#94a3b8'>走势样本不足</div>"
    norm = {n: [(x / v[0] - 1) * 100 for x in v] for n, v in series.items() if v[0]}
    if not norm:
        return "<div style='font-size:12px;color:#94a3b8'>走势样本不足</div>"
    allv = [x for v in norm.values() for x in v]
    span = max(5, math.ceil(max(abs(min(allv)), abs(max(allv))) / 5) * 5)
    W, PAD, LAB = 620, 34, 14
    H, PH = height, height - LAB

    def xy(i, n, val):
        x = PAD + (W - PAD - 8) * (i / max(1, n - 1))
        return x, PH / 2 - (val / span) * (PH / 2 - 10)

    paths, legend = [], []
    for k, (nm, v) in enumerate(norm.items()):
        c = _C[k % len(_C)]
        pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in
                       (xy(i, len(v), val) for i, val in enumerate(v)))
        paths.append(f"<polyline points='{pts}' fill='none' stroke='{c}' stroke-width='1.4' "
                     f"stroke-linejoin='round'/>")
        legend.append(
            f"<span style='white-space:nowrap'>"
            f"<span style='display:inline-block;width:9px;height:9px;background:{c};"
            f"border-radius:2px;margin-right:3px'></span><b>{nm}</b> "
            f"<span style='color:{'#dc2626' if v[-1] >= 0 else '#16a34a'}'>{v[-1]:+.1f}%</span></span>")
    grid = "".join(
        f"<line x1='{PAD}' y1='{PH/2 - k*(PH/2-10):.1f}' x2='{W-8}' y2='{PH/2 - k*(PH/2-10):.1f}' "
        f"stroke='#e2e8f0' stroke-dasharray='{'0' if k == 0 else '3,3'}'/>"
        f"<text x='0' y='{PH/2 - k*(PH/2-10)+3:.1f}' font-size='9' fill='#94a3b8'>{k*span:+.0f}%</text>"
        for k in (1, 0.5, 0, -0.5, -1))
    return (f"<div style='font-size:11px;color:#94a3b8;margin-bottom:2px'>"
            f"各自起点归一为0%，看的是<b>同期相对强弱</b>不是绝对价格；横轴=最近{days}个交易日</div>"
            f"<svg viewBox='0 0 {W} {H}' style='width:100%;height:auto'>{grid}{''.join(paths)}</svg>"
            f"<div style='font-size:11.5px;margin-top:2px;display:flex;flex-wrap:wrap;"
            f"gap:4px 14px'>{''.join(legend)}</div>")


# 动词归一：定纲禁"观察/评估减仓"这类和稀泥词，落到明确动作上
_VERB = {"买入": "买", "建仓": "买", "加仓": "买", "试仓": "小批买",
         "持有": "持有不加", "拿住": "持有不加",
         "减仓": "减", "锁盈": "减", "冲高减仓": "减", "评估减仓": "减",
         "卖出": "卖", "清仓": "卖", "退出": "卖", "止损": "卖", "破位离场": "卖",
         "回避": "不买", "观察": "不买"}


def verdict_html(rows: list) -> str:
    """排序结论：按统一分排名，第一名标出来，每只给明确动词。
    rows = [{name, code, score, action, p_up, rr, expected, pos52, vold, last, stop}]
    只重排既有引擎结论，不新造判断——排名靠的是 decision_core 的统一分，不是我另发明的分。"""
    if not rows:
        return ""
    rk = sorted(rows, key=lambda r: (r.get("score") or 0), reverse=True)
    out = []
    for i, r in enumerate(rk):
        verb = _VERB.get(str(r.get("action") or "").strip(), str(r.get("action") or "—"))
        top = i == 0
        col = "#dc2626" if verb.endswith("买") else ("#16a34a" if verb == "卖" else
                                                    ("#ea580c" if verb == "减" else "#2563eb"))
        bits = []
        if r.get("p_up") is not None:
            bits.append(f"2周上涨{r['p_up']}%")
        if r.get("rr"):
            bits.append(f"赔率{r['rr']:.2f}")
        if r.get("pos52") is not None:
            bits.append(f"52周位{r['pos52']:.0f}%")
        if r.get("vold"):
            bits.append(f"量比{r['vold']:.2f}")
        if r.get("expected") is not None:
            bits.append(f"期望{r['expected']:+.1f}%")
        fail = (f"　<span style='color:#64748b'>失效线 {r['stop']}</span>"
                if r.get("stop") else "")
        out.append(
            f"<div style='padding:5px 8px;margin:3px 0;border-left:3px solid {col};"
            f"background:{'#fef2f2' if top else '#f8fafc'};border-radius:0 4px 4px 0'>"
            f"<span style='font-size:13px'>{'🥇' if top else f'{i+1}.'} <b>"
            + (f'<a href="?q={r.get("code")}&focus=deep#v88-deep-analysis" target="_blank" '
               f'rel="noopener" style="color:inherit;text-decoration:underline;'
               f'text-underline-offset:2px">{r.get("name")}</a>' if r.get("code")
               else str(r.get("name") or ""))
            + f"</b> "
            f"<span style='color:#94a3b8;font-size:11px'>{r.get('code')}</span>"
            + (f"　<span style='font-size:10.5px;color:#2563eb'>{r['tier']}</span>"
               if r.get('tier') else "")
            + (f"<span style='font-size:10.5px;color:#64748b'>·{r['when']}</span>"
               if r.get('when') else "") + "　"
            f"<b style='color:{col};font-size:14px'>{verb}</b>"
            f"　<span style='color:#64748b;font-size:11.5px'>统一分{r.get('score')}</span></span>"
            f"<div style='font-size:11.5px;color:#475569;margin-top:1px'>"
            + " · ".join(bits) + fail + "</div></div>")
    _kind = (rk[0].get("score_kind") or "排名分R1") if rk else "排名分R1"
    return (f"<div style='font-size:11px;color:#94a3b8;margin-bottom:2px'>"
            f"按<b>{_kind}</b>排序（赢面45%+催化25%+量能15%+位置15%，全系统同一把尺）；"
            f"级别只表示<b>买入时机</b>不表示质量高低，故1A·中长线可以排在2A前面；"
            f"动词已归一，不出现\"观察/评估\"这类无法执行的措辞</div>" + "".join(out))
