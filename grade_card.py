"""grade_card.py — 【V88·评级完整输出卡】2026-08-02 用户第十五节全文落地，三端共用渲染。

用户要求每只股票输出：最终等级/子类型/同级排名/同级排序分/机会类型/适用周期/行动状态、
五桶板各自分数与达标状态、风险分/数据完整度/模型置信度、
为什么值得关注/为什么属于这个等级/为什么不是更高等级、缺失桶板/主要短板、
进入触发/升级/降级/失效条件、主要风险。

只用内联 HTML（与 barometer_ui / compare_ui 同一套做法），桌面与云端读同一份 rank_score.json。
"""
from __future__ import annotations

BUCKET_CN = {"long_value": "长期价值", "timing": "当前时机", "catalyst": "机会催化",
             "valuation": "价格估值", "payoff": "获利空间"}
BUCKET_W = {"long_value": 25, "timing": 20, "catalyst": 15, "valuation": 15, "payoff": 25}
TIER_COLOR = {"3A": "#dc2626", "2A": "#ea580c", "1A": "#2563eb",
              "0A": "#94a3b8", "存档": "#64748b"}
# 行动状态统一八种（用户第十五节末）
ACTIONS = ("现在可进", "分批试探", "等待回踩", "等待突破",
           "短线进攻", "低吸埋伏", "继续观察", "风险回避")


def _bar(score: float, ok: bool, w: int) -> str:
    """桶板进度条：达标绿、未达标橙；条长按分数，标签带权重。"""
    c = "#16a34a" if ok else "#ea580c"
    return (f"<div style='display:flex;align-items:center;gap:6px;margin:1px 0'>"
            f"<span style='width:52px;font-size:11px;color:#475569'>{w}%</span>"
            f"<div style='flex:1;height:7px;background:#f1f5f9;border-radius:4px;overflow:hidden'>"
            f"<div style='width:{max(0, min(100, score)):.0f}%;height:100%;background:{c}'></div></div>"
            f"<span style='width:56px;text-align:right;font-size:11px;color:{c};font-weight:700'>"
            f"{score:.0f} {'✓' if ok else '✗'}</span></div>")


def card_html(r: dict, compact: bool = False) -> str:
    """单只股票的完整评级卡。r = rank_score.json 里的一行。"""
    tier = str(r.get("tier") or "")
    col = TIER_COLOR.get(tier, "#64748b")
    head = (
        f"<div style='display:flex;align-items:baseline;gap:8px;flex-wrap:wrap'>"
        f"<span style='background:{col};color:#fff;border-radius:4px;padding:1px 7px;"
        f"font-size:13px;font-weight:800'>{tier}</span>"
        f"<b style='font-size:14px'>{r.get('name')}</b>"
        f"<span style='font-size:11px;color:#94a3b8'>{r.get('code')}</span>"
        f"<span style='font-size:12px;color:{col};font-weight:700'>{r.get('subtype')}</span>"
        f"<span style='font-size:12px;color:#dc2626;font-weight:700'>{r.get('action_state')}</span>"
        f"<span style='font-size:11px;color:#64748b'>同级#{r.get('rank_in_tier', '-')}"
        f"·排序分{r.get('rank_score')}·总#{r.get('rank', '-')}</span>"
        f"<span style='font-size:11px;color:#64748b'>{r.get('opportunity_type')}·"
        f"{r.get('horizon')}</span></div>")
    if compact:
        return f"<div style='padding:5px 8px;border-left:3px solid {col};margin:3px 0'>{head}</div>"

    bks = r.get("buckets") or {}
    bars = "".join(
        f"<div style='font-size:11.5px;color:#334155;margin-top:3px'>"
        f"<b>{BUCKET_CN[k]}</b> "
        f"<span style='color:#94a3b8'>{(bks[k].get('basis') or '')[:56]}</span></div>"
        + _bar(bks[k].get("score", 0), bks[k].get("pass", False), BUCKET_W[k])
        for k in ("long_value", "timing", "catalyst", "valuation", "payoff") if k in bks)

    ind = (f"<div style='font-size:11.5px;margin-top:5px;color:#475569'>"
           f"风险分 <b style='color:{'#dc2626' if (r.get('risk_score') or 0) > 35 else '#16a34a'}'>"
           f"{r.get('risk_score')}</b>(3A限≤35)　"
           f"数据完整度 <b style='color:{'#dc2626' if (r.get('data_completeness') or 0) < 80 else '#16a34a'}'>"
           f"{r.get('data_completeness')}</b>(限≥80)　"
           f"模型置信度 <b>{r.get('model_confidence')}</b></div>")
    risks = r.get("risk_veto") or []
    rf = r.get("risk_flags") or []
    risk_line = ""
    if risks:
        risk_line += (f"<div style='font-size:11.5px;color:#b91c1c;font-weight:600;margin-top:3px'>"
                      f"🚫 风险否决(不可被总分补偿)：{'；'.join(risks)}</div>")
    if rf:
        risk_line += (f"<div style='font-size:11px;color:#b45309'>⚠️ 风险提示：{'、'.join(rf)}</div>")

    tg = r.get("triggers") or {}
    rows = [
        ("当前为什么值得关注", r.get("why_focus") or "—"),
        ("为什么属于这个等级", r.get("why_grade") or "—"),
        ("为什么不是更高等级", r.get("why_not_higher") or "（已是最高级）"),
        ("缺失桶板 / 主要短板", "、".join(r.get("missing") or []) or "无"),
        ("进入触发条件", tg.get("enter") or "—"),
        ("升级条件", tg.get("upgrade") or "—"),
        ("降级条件", tg.get("downgrade") or "—"),
        ("失效条件", tg.get("invalid") or "—"),
    ]
    detail = "".join(
        f"<div style='display:flex;gap:6px;font-size:11.5px;margin:1px 0'>"
        f"<span style='width:118px;color:#94a3b8;flex-shrink:0'>{k}</span>"
        f"<span style='color:#334155'>{v}</span></div>" for k, v in rows)
    ts = (f"<div style='font-size:10.5px;color:#94a3b8;margin-top:4px'>"
          f"数据可得 {r.get('data_available_at') or '?'} · 信号生成 {r.get('signal_generated_at')} · "
          f"最早执行 {r.get('earliest_execution_at')}</div>")
    return (f"<div style='padding:7px 10px;border:1px solid #e2e8f0;border-left:4px solid {col};"
            f"border-radius:0 6px 6px 0;margin:6px 0;background:#fff'>"
            f"{head}{bars}{ind}{risk_line}<div style='margin-top:4px'>{detail}</div>{ts}</div>")


def board_html(data: dict, limit: int = 8, show_detail: int = 3) -> str:
    """行动清单：3A/2A 出完整卡，其余出紧凑行；0A 不进（用户第十节）。"""
    rows = [r for r in (data.get("rows") or []) if str(r.get("tier")) != "0A"][:limit]
    if not rows:
        return ("<div style='font-size:12px;color:#b45309'>今日无 3A/2A/1A 行动标的"
                "（木桶门槛严，不硬凑）</div>")
    out = []
    for i, r in enumerate(rows):
        out.append(card_html(r, compact=(i >= show_detail)))
    n0 = sum(1 for r in (data.get("rows") or []) if str(r.get("tier")) == "0A")
    tail = (f"<div style='font-size:11px;color:#94a3b8;margin-top:4px'>"
            f"另有 {n0} 只判为 0A（无行动价值/风险过高）已移出行动清单；"
            f"权重 长期25/时机20/催化15/估值15/空间25，木桶定级：关键桶板缺一即降级，"
            f"风险否决不可被总分补偿。</div>")
    return "".join(out) + tail


def veto_review_html(data: dict) -> str:
    """待复核否决：被硬闸门拦下但质量分高、或否决前提已变的，必须让人看见来裁。"""
    items = []
    for r in (data.get("rows") or []) + (data.get("archived") or []):
        for v in (r.get("risk_veto") or []):
            if "前提可能已变" in v or r.get("review_needed"):
                items.append((r, v))
                break
    if not items:
        return ""
    body = "".join(
        f"<div style='font-size:12px;color:#b91c1c;margin:2px 0'>⚠️ <b>{r.get('name')}</b>"
        f"（{r.get('code')}）排序分 {r.get('rank_score')} · 五桶"
        f"{sum(1 for b in (r.get('buckets') or {}).values() if b.get('pass'))}/5达标"
        f" → 被拦：{v}</div>" for r, v in items)
    return (f"<div style='border:1px solid #fecaca;background:#fef2f2;border-radius:6px;"
            f"padding:6px 9px;margin:6px 0'>"
            f"<div style='font-size:12.5px;font-weight:700;color:#b91c1c'>🔎 待人工复核的否决"
            f"（{len(items)}只）</div>"
            f"<div style='font-size:11px;color:#64748b;margin-bottom:3px'>"
            f"硬闸门保留（不由系统单方面推翻三方否决），但代价必须看得见——"
            f"否决前提已变或质量分高于中位的，在此列出由你裁定。</div>{body}</div>")

# ═══════════════════════════════════════════════════════════════════
# 【3A大系统·自包含表格渲染】2026-08-02 用户"放在大盘和今日之间"
# 前一版把模块塞进买表所在的嵌套作用域,导致它渲染在页面别处、用户看不到。
# 这里做成**模块级自包含函数**:自己产出11列表格HTML(与买表同款列),
# 不依赖 _tbl9/_row6_9 等嵌套闭包,因此可放页面任意位置。
# ═══════════════════════════════════════════════════════════════════
SELL_COLOR = {"-3A": "#b91c1c", "-2A": "#dc2626", "-1A": "#ea580c", "0": "#16a34a"}
# 【2026-08-02 自检修①】列头按 IN/OUT 各自语义命名,不再共用一套含糊标签——
# 自检发现9列里8列两侧装的东西不同(如"状态"IN=风险/完备/置信,OUT=1-2-3+量比),
# 共用列名会让人以为是同一个量。改为两套列头,同位次语义对齐。
_TH_IN = ("名称", "评级·分·五桶板", "现价·数据日", "动作·2周概率", "买入区间·进入时机",
          "失效价", "机会类型·周期", "风险·完备·置信", "缺什么·为什么不更高", "为什么现在")
_TH_OUT = ("名称", "级别·卖出分·门派", "现价·MA20", "动作·紧迫度", "卖出区间·重买条件",
           "止损价", "距止损·量比", "1-2-3·旁路", "冲突·门派仲裁", "引擎对照")
# 【自检修②】市场从**代码形态**推断,不依赖上游 market 字段——
# 实测11处国旗缺失:rank_score.market 仅 trend_quality 来源的行才有,
# intraday_decisions 压根没有该字段。代码后缀是100%可得的事实。
_MKF = {"A股": "🇨🇳", "港股": "🇭🇰", "美股": "🇺🇸"}


def _flag(code: str, market: str = "") -> str:
    c = str(code or "").upper()
    if market in _MKF:
        return _MKF[market]
    if c.endswith(".HK"):
        return "🇭🇰"
    if c.endswith((".SS", ".SZ", ".SH", ".BJ")):
        return "🇨🇳"
    return "🇺🇸" if c else ""


def _tbl(rows_html: str, heads=None) -> str:
    if not rows_html:
        return ""
    th = "".join(f"<th style='padding:3px 5px;text-align:left;font-weight:600;"
                 f"border-bottom:2px solid #cbd5e1;white-space:nowrap'>{h}</th>"
                 for h in (heads or _TH_IN))
    return (f"<div style='overflow-x:auto'><table style='width:100%;border-collapse:collapse;"
            f"font-size:10.5px;line-height:1.3'><thead style='background:#f8fafc;color:#475569'>"
            f"<tr>{th}</tr></thead><tbody>{rows_html}</tbody></table></div>")


def _td(v, style="") -> str:
    return f"<td style='padding:3px 5px;border-bottom:1px solid #f1f5f9;{style}'>{v}</td>"


def system_table_html(rk: dict, sg: dict, dec: dict, why_sells: dict,
                      pool: dict = None, limit_in: int = 6, limit_out: int = 8) -> str:
    """3A大系统完整模块(标题+IN表+OUT表+尾注),一次返回全部HTML。"""
    rows = rk.get("rows") or []
    n3 = sum(1 for r in rows if r.get("tier") == "3A")
    n2 = sum(1 for r in rows if r.get("tier") == "2A")
    n1 = sum(1 for r in rows if r.get("tier") == "1A")
    arch = rk.get("archived") or []
    sgm = {str(x.get("code")): x for x in (sg.get("rows") or [])}
    out_src = sorted([x for x in (sg.get("rows") or [])
                      if x.get("level") in ("-3A", "-2A", "-1A")],
                     key=lambda x: ({"-3A": 0, "-2A": 1, "-1A": 2}[x["level"]],
                                    -(x.get("sell_score") or 0)))[:limit_out]
    head = (f"<div style='background:linear-gradient(90deg,#0ea5e9,#dc2626);color:#fff;"
            f"border-radius:8px;padding:8px 14px;margin:4px 0 6px;text-align:center'>"
            f"<div style='font-size:16px;font-weight:800'>🎯 3A大系统 · IN / OUT</div>"
            f"<div style='font-size:12px;opacity:.95'>"
            + (f"全市场→通道自然产出{len(pool.get('rows') or [])}只→评级{len(rows)}只 ｜ "
               if pool else "")
            + f"IN: 3A×{n3} 2A×{n2} 战术1A×{n1} ｜ OUT: 卖警×{len(out_src)} 否决×{len(arch)}"
            f"</div></div>")

    # ── IN 表 ──
    in_rows = ""
    for r in [x for x in rows if x.get("tier") in ("3A", "2A")][:limit_in]:
        c = str(r.get("code"))
        d = dec.get(c) or {}
        ep = d.get("entry_plan") or {}
        z = ep.get("zone") or []
        zone = (f"{z[0]}~{z[1]}" if len(z) >= 2 else
                (f"回踩{ep.get('pullback')}" if ep.get("pullback") else "—"))
        col = TIER_COLOR.get(str(r.get("tier")), "#64748b")
        cf = (sgm.get(c) or {}).get("in_out_conflict")
        bk = r.get("buckets") or {}
        in_rows += (
            "<tr>"
            + _td(f"{_flag(c, r.get('market'))}<b>{r.get('name')}</b>"
                  f"<br><span style='color:#94a3b8;font-size:9px'>{c}</span>")
            + _td(f"<span style='background:{col};color:#fff;border-radius:3px;padding:0 4px;"
                  f"font-weight:800'>{r.get('tier')}</span>"
                  f"<br><span style='color:#1d4ed8;font-weight:700'>分{r.get('rank_score')}"
                  f"·#{r.get('rank')}</span>"
                  f"<br><span style='color:#94a3b8;font-size:9px'>"
                  + " ".join(f"{BUCKET_CN[k][:2]}{v.get('score'):.0f}"
                             f"{'✓' if v.get('pass') else '✗'}" for k, v in bk.items())
                  + "</span>", "max-width:110px")
            + _td(f"<b>现{d.get('last', '—')}</b>"
                  f"<br><span style='color:#16a34a;font-size:9px'>"
                  f"{str(r.get('data_available_at') or '')[:10]}</span>")
            + _td(f"<b style='color:{col}'>{r.get('subtype')}</b>"
                  f"<br><span style='color:#dc2626'>{r.get('action_state')}</span>"
                  + (f"<br><span style='font-size:9px;color:#64748b'>2周涨"
                     f"{d.get('p_up')}%</span>" if d.get("p_up") else ""))
            + _td(f"<b style='color:#b91c1c'>{zone}</b>"
                  + (f"<br><span style='font-size:9px'>破{ep.get('breakout')}</span>"
                     if ep.get("breakout") else "")
                  + f"<br><span style='font-size:9px;color:#0891b2'>"
                    f"{(r.get('triggers') or {}).get('enter', '—')}</span>")
            + _td(str((r.get("triggers") or {}).get("invalid", "—")).replace("跌破止损", ""))
            + _td(f"{r.get('opportunity_type', '—')}<br>"
                  f"<span style='font-size:9px'>{r.get('horizon', '')}</span>")
            + _td(f"风险{r.get('risk_score')}<br>完备{r.get('data_completeness')}"
                  f"<br>置信{r.get('model_confidence')}", "font-size:9px")
            + _td(f"缺: <b style='color:#b45309'>"
                  f"{'、'.join(r.get('missing') or []) or '无'}</b>"
                  f"<br><span style='font-size:9px'>{r.get('why_not_higher', '')[:34]}</span>"
                  + (f"<br><span style='font-size:9px;color:#b91c1c'>⚠️IN/OUT冲突"
                     f"({cf.get('severity')})</span>" if cf else ""), "max-width:150px")
            + _td("<span style='font-size:9px'>"
                  + (str(r.get("why_focus"))[:46] if r.get("why_focus") else
                     (str(d.get("reason") or "")[:40] or
                      f"{r.get('opportunity_type', '')}·"
                      f"{(r.get('buckets') or {}).get('timing', {}).get('basis', '')[:30]}")
                     + "<span style='color:#b45309'>(池外新入,why_buy未覆盖)</span>")
                  + "</span>", "max-width:170px")
            + "</tr>")

    # ── OUT 表 ──
    out_rows = ""
    for g in out_src:
        c = str(g.get("code"))
        d = dec.get(c) or {}
        lv = str(g.get("level"))
        col = SELL_COLOR.get(lv, "#64748b")
        c123 = "".join(s for s, ok in (("①", g.get("c1_trend_break")),
                                       ("②", g.get("c2_no_new_high")),
                                       ("③", g.get("c3_low_break"))) if ok)
        bp = "；".join(g.get("bypass") or [])
        rb = str((why_sells.get(c) or {}).get("fail") or "")
        cf = g.get("in_out_conflict") or {}
        out_rows += (
            "<tr>"
            + _td(f"{_flag(c, d.get('market'))}<b>{g.get('name')}</b>"
                  f"<br><span style='color:#94a3b8;font-size:9px'>{c}</span>")
            + _td(f"<span style='background:{col};color:#fff;border-radius:3px;padding:0 4px;"
                  f"font-weight:800'>{lv}</span>"
                  f"<br><span style='color:#b91c1c;font-weight:700'>卖出分"
                  f"{g.get('sell_score', '—')}</span>"
                  f"<br><span style='color:#94a3b8;font-size:9px'>"
                  f"{g.get('opp_type') or '门派未知'}</span>", "max-width:110px")
            + _td(f"<b>现{g.get('px', '—')}</b><br>"
                  f"<span style='font-size:9px;color:#64748b'>MA20 {g.get('ma20', '—')}</span>")
            + _td(f"<b style='color:{col}'>{g.get('action')}</b>"
                  + f"<br><span style='font-size:9px;color:#64748b'>紧迫度"
                    f"{'高' if lv == '-3A' else '中' if lv == '-2A' else '低'}</span>")
            + _td(f"<b style='color:#b91c1c'>{g.get('sell_zone', '—')}</b>"
                  + (f"<br><span style='font-size:9px;color:#0891b2'>🔁重买: {rb[:44]}</span>"
                     if rb else "<br><span style='font-size:9px;color:#b45309'>"
                                "⚠️缺重买条件(只说一半)</span>"), "max-width:180px")
            + _td(g.get("stop") or "—")
            + _td(f"{g.get('dist_stop_pct')}%" if g.get("dist_stop_pct") is not None else "—")
            + _td(f"1-2-3: <b>{c123 or '无'}</b>"
                  + (f"<br><span style='font-size:9px;color:#b91c1c'>{bp}</span>" if bp else "")
                  + (f"<br><span style='font-size:9px'>量比{g.get('vr')}</span>"
                     if g.get("vr") else ""), "max-width:130px")
            + _td((f"<span style='font-size:9px;color:#b91c1c'>⚠️IN/OUT冲突"
                   f"({cf.get('severity')}): 买入侧{cf.get('in_tier')}·{cf.get('in_action')}"
                   f"<br>{cf.get('verdict', '')[:40]}</span>" if cf else
                   "<span style='font-size:9px;color:#b45309'>"
                   + "；".join(g.get("school_notes") or []) + "</span>"
                   if g.get("school_notes") else
                   "<span style='font-size:9px;color:#16a34a'>无IN/OUT冲突·"
                   f"门派({g.get('opp_type') or '未定'})内判定一致</span>"), "max-width:150px")
            + _td(f"<span style='font-size:9px'>引擎:{d.get('action', '—')}</span>",
                  "max-width:120px")
            + "</tr>")

    near = ""
    if n3 == 0:
        cand = [r for r in rows if r.get("tier") == "2A"]
        if cand:
            b = min(cand, key=lambda r: len(r.get("missing") or []))
            near = (f"　离3A最近: <b>{b.get('name')}</b> 差「"
                    f"{'、'.join(b.get('missing') or [])}」")
    cons = sg.get("consistency") or {}
    return (head
            + f"<div style='font-size:12.5px;font-weight:700;color:#16a34a;margin:6px 0 2px'>"
              f"🟢 IN · 买入侧（3A/2A 核心推荐）</div>"
            + (f"<div style='font-size:11.5px;background:#eff6ff;border-radius:5px;"
               f"padding:4px 8px;margin-bottom:3px'>今日无 3A（现在可进+长期获益的完整机会），"
               f"不硬凑。{near}</div>" if n3 == 0 else "")
            + (_tbl(in_rows, _TH_IN) or "<div style='font-size:12px;color:#94a3b8'>今日无 3A/2A —— "
                                "现金也是仓位；战术级1A见买表折叠区</div>")
            + f"<div style='font-size:12.5px;font-weight:700;color:#b91c1c;margin:10px 0 2px'>"
              f"🔴 OUT · 卖出侧（-3A 影子分级 · 斯波朗迪1-2-3）</div>"
            + (_tbl(out_rows, _TH_OUT) or "<div style='font-size:12px;color:#16a34a'>持仓无卖出警报 —— "
                                 "无 OUT 信号也是信号</div>")
            + f"<div style='font-size:10.5px;color:#94a3b8;margin-top:4px'>"
              f"OUT为影子级(攒战绩不触发交易)·与引擎卖警一致率{cons.get('rate', '—')}%"
              f"({cons.get('shadow_agrees', '—')}/{cons.get('engine_sell_calls', '—')})"
              f"·08-14起与sell_call核算对照后转正"
            + (f"　🗄已否决{len(arch)}只(不占推荐位)" if arch else "") + "</div>")
