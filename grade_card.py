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
# ══════════ 【V88·语义配色定纲 2026-08-02 用户定纲】 ══════════
# 用户原话："所有系统里的 buy 都用绿色，sell 用红色，其他不一样的操作都需要不同字体，
#           你来设计，都是红色系看的累。"
#
# 铁律：**颜色只表达动作语义，不表达强弱**。强弱用深浅，不换色相。
#   买入族 → 绿   卖出族 → 红   减持(卖但不清) → 赭橙
#   等待/观察 → 蓝   持有不动 → 石板灰   数据质量警告 → 琥珀   未评估 → 浅灰
#
# 为什么"数据质量警告"绝不能用红：它不是卖出信号。红色一旦被滥用到非卖出语境，
# 真正该红的地方就失效了——首版覆盖率红条(深红底#7f1d1d)正是这个错，
# 而且深底小字对比度低,用户第一反应是"好丑,字看不清"。
#
# 注:此处与"中国红=涨"的行情色系**不冲突**——那套用于指数/涨跌幅(见 barometer_ui),
# 本套用于**动作指令**。两者语境不同,不可互相套用。
PALETTE = {
    "buy":      "#15803d",   # 买入·核心(3A)
    "buy2":     "#16a34a",   # 买入·次级(2A)
    "buy3":     "#4d7c0f",   # 买入·战术(1A) 橄榄绿,与2A可辨
    "sell":     "#991b1b",   # 卖出·清仓(-3A)
    "sell2":    "#dc2626",   # 卖出·减仓(-2A)
    "trim":     "#c2410c",   # 分批收回(-1A) 赭橙=卖但不全卖
    "wait":     "#0369a1",   # 等待/回踩/观察
    "hold":     "#475569",   # 持有不动·无动作
    "warn":     "#b45309",   # 数据/质量警告(非卖出)
    "warn_bg":  "#fffbeb",   # 警告底色:浅琥珀+深字,保证可读
    "mute":     "#94a3b8",   # 未评估/无数据
}
TIER_COLOR = {"3A": PALETTE["buy"], "2A": PALETTE["buy2"], "1A": PALETTE["buy3"],
              "0A": PALETTE["mute"], "待评估": PALETTE["mute"],
              "存档": PALETTE["hold"]}
# 行动状态统一八种（用户第十五节末）
ACTIONS = ("现在可进", "分批试探", "等待回踩", "等待突破",
           "短线进攻", "低吸埋伏", "继续观察", "风险回避")


def _bar(score: float, ok: bool, w: int) -> str:
    """桶板进度条：达标绿、未达标橙；条长按分数，标签带权重。"""
    c = PALETTE["wait"] if ok else PALETTE["warn"]
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
        f"<span style='font-size:12px;color:{PALETTE['buy']};font-weight:700'>{r.get('action_state')}</span>"
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
           f"风险分 <b style='color:{PALETTE['warn'] if (r.get('risk_score') or 0) > 35 else PALETTE['hold']}'>"
           f"{r.get('risk_score')}</b>(3A限≤35)　"
           f"数据完整度 <b style='color:{PALETTE['warn'] if (r.get('data_completeness') or 0) < 80 else PALETTE['hold']}'>"
           f"{r.get('data_completeness')}</b>(限≥80)　"
           f"模型置信度 <b>{r.get('model_confidence')}</b></div>")
    risks = r.get("risk_veto") or []
    rf = r.get("risk_flags") or []
    risk_line = ""
    if risks:
        risk_line += (f"<div style='font-size:11.5px;color:{PALETTE['sell']};font-weight:600;margin-top:3px'>"
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
        return (f"<div style='font-size:12px;color:{PALETTE['hold']}'>今日无 3A/2A/1A 行动标的"
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
        f"<div style='font-size:12px;color:{PALETTE['warn']};margin:2px 0'>⚠️ <b>{r.get('name')}</b>"
        f"（{r.get('code')}）排序分 {r.get('rank_score')} · 五桶"
        f"{sum(1 for b in (r.get('buckets') or {}).values() if b.get('pass'))}/5达标"
        f" → 被拦：{v}</div>" for r, v in items)
    return (f"<div style='border:1px solid #fecaca;background:#fef2f2;border-radius:6px;"
            f"padding:6px 9px;margin:6px 0'>"
            f"<div style='font-size:12.5px;font-weight:700;color:{PALETTE['warn']}'>🔎 待人工复核的否决"
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
# 卖出族一律红系,强弱靠深浅;-1A"分批收回"是**卖但不全卖**,用赭橙区分。
# "0"(无卖出信号)改用石板灰而非绿——绿=买入,而"无卖出信号"不是买入信号;
# 且我们自己的措辞铁律写着"不等于安全,也不等于建议继续持有",配绿色是自相矛盾。
SELL_COLOR = {"-3A": PALETTE["sell"], "-2A": PALETTE["sell2"],
              "-1A": PALETTE["trim"], "0": PALETTE["hold"]}
# 【2026-08-02 自检修①】列头按 IN/OUT 各自语义命名,不再共用一套含糊标签——
# 自检发现9列里8列两侧装的东西不同(如"状态"IN=风险/完备/置信,OUT=1-2-3+量比),
# 共用列名会让人以为是同一个量。改为两套列头,同位次语义对齐。
_TH_IN = ("名称", "评级·分", "现价·数据日", "动作·2周概率", "买入区间·进入时机",
          "失效价", "机会类型·周期", "五桶板·风险完备置信", "缺什么·为什么不更高", "为什么现在")
_TH_OUT = ("名称", "级别·卖出分", "现价·MA20", "动作·紧迫度", "卖出/回避区间·重买条件",
           "止损价", "距止损·量比", "1-2-3·旁路·门派", "冲突·仲裁", "持有状态·引擎")
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


def _gate(g: dict | None, key: str) -> str:
    """取某只票某个动作的门票。IN 侧读它**不构成"互为准入"**——门控发生在输出层，
    两个引擎的计算仍互盲(见 sell_grade.py 顶部三方定纲)。"""
    return str(((g or {}).get("gates") or {}).get(key) or "PASS")


def system_table_html(rk: dict, sg: dict, dec: dict, why_sells: dict,
                      pool: dict = None, limit_in: int = 6, limit_out: int = 8) -> str:
    """3A大系统完整模块(标题+IN表+OUT表+尾注),一次返回全部HTML。"""
    rows = rk.get("rows") or []
    n3 = sum(1 for r in rows if r.get("tier") == "3A")
    n2 = sum(1 for r in rows if r.get("tier") == "2A")
    n1 = sum(1 for r in rows if r.get("tier") == "1A")
    arch = rk.get("archived") or []
    sgm = {str(x.get("code")): x for x in (sg.get("rows") or [])}
    # 【2026-08-02 用户定纲】"哪个更接近当前就进入排名,还没到就不进名单"→
    # 持仓/自选/候选**同榜竞争**,不再按持有与否切两段(切两段=让一只离触发12%的持仓
    # 压在离触发1%的自选前面)。优先度已在生产端落成"门槛梯度",此处只按紧迫度排。
    _sig = sorted([x for x in (sg.get("rows") or [])
                   if x.get("level") in ("-3A", "-2A", "-1A")],
                  key=lambda x: ({"-3A": 0, "-2A": 1, "-1A": 2}[x["level"]],
                                 -(x.get("sell_score") or 0)))
    _board = sorted([x for x in _sig if x.get("on_board")],
                    key=lambda x: x.get("board_rank") or 999)
    if not _board:      # 生产端尚未产出榜单字段时退回旧口径,不让模块空白
        _board = _sig[:limit_out]
    out_src = _board
    _bd = sg.get("board") or {}
    # 【P1·2026-08-02 三方裁决】用户"out首先关注的是我的持仓,其次是其他"。
    # GPT B2:"选**改分区**,不改门槛,也不把所有股票混在一个统一榜单中。
    # 门槛梯度解决的是'谁更早获得信号',分区解决的是'用户先看到什么',两者并不冲突。
    # 不建议强行把持仓加排序权重——那会**污染'触发强度'的含义**;
    # 持仓应在展示层优先,而不是通过修改风险分数伪造优先级。"
    # 故:榜内排序(urgency)一个字不改,只在展示层切成 持仓区 / 非持仓区。
    _own = [g for g in _board if g.get("held")]
    _oth = [g for g in _board if not g.get("held")]
    _quiet = sg.get("holdings_quiet") or {}
    _cov = rk.get("coverage") or {}
    # 被 OUT 证据撤销买点的 IN 候选(含未进上表的1A):行不删,但必须点名
    _revoked = [(str(r.get("name")), str(r.get("tier")))
                for r in rows if _gate(sgm.get(str(r.get("code"))), "开仓") == "BLOCK"
                and str(r.get("tier")) in ("3A", "2A", "1A")]
    head = (f"<div style='background:linear-gradient(90deg,#15803d,#991b1b);color:#fff;"
            f"border-radius:8px;padding:8px 14px;margin:4px 0 6px;text-align:center'>"
            f"<div style='font-size:16px;font-weight:800'>🎯 3A大系统 · IN / OUT</div>"
            f"<div style='font-size:12px;opacity:.95'>"
            + (f"全市场→通道自然产出{len(pool.get('rows') or [])}只→评级{len(rows)}只 ｜ "
               if pool else "")
            + f"IN: 3A×{n3} 2A×{n2} 战术1A×{n1} ｜ OUT: 卖警×{len(out_src)} 否决×{len(arch)}"
            f"</div>"
            # 【2026-08-02 用户"3A大系统一定要有标注更新的时间"】买/卖两侧各自落盘,
            # 时间可能不同步——分别标,不合并成一个"最后更新",否则会掩盖某一侧卡住。
            + f"<div style='font-size:10.5px;opacity:.9;margin-top:2px'>"
              f"🕐 IN {str(rk.get('generated_at') or '—')} ｜ OUT {str(sg.get('generated_at') or '—')}"
              f"　<span style='opacity:.8'>每交易日 3 次(05:40/13:00/16:30 北京)</span></div>"
            f"</div>")

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
            # 【2026-08-02 用户"有点堆叠,可以调整"】五桶板缩写原挤在评级列(徽章+分+5项一坨,
            # 窄屏竖排)。移到下方"风险·完备·置信"列——那列只有3个数,有横向空间。
            + _td(f"<span style='background:{col};color:#fff;border-radius:3px;padding:1px 5px;"
                  f"font-weight:800;font-size:12px'>{r.get('tier')}</span>"
                  f"<br><span style='color:#1d4ed8;font-weight:700;font-size:11px'>"
                  f"分{r.get('rank_score')} · #{r.get('rank')}</span>", "white-space:nowrap")
            + _td(f"<b>现{d.get('last', '—')}</b>"
                  f"<br><span style='color:#16a34a;font-size:9px'>"
                  f"{str(r.get('data_available_at') or '')[:10]}</span>")
            # 【2026-08-02 三方定纲】IN 侧有 OUT 风险证据时**不删行**(GPT:原IN结构尚可观察),
            # 而是把"本次入场触发已失效"的门票摆在动作位——机会结构仍在 ≠ 现在能买,
            # 这两件事必须能同时表达。删行=监守自盗的镜像(用一侧结果消音另一侧证据)。
            + _td((f"<div style='background:#7f1d1d;color:#fff;border-radius:3px;"
                   f"padding:1px 4px;font-size:9.5px;font-weight:700;margin-bottom:2px'>"
                   f"⛔本次买点已撤销</div>" if _gate(sgm.get(c), "开仓") == "BLOCK" else
                   f"<div style='background:#fef3c7;color:#92400e;border-radius:3px;"
                   f"padding:1px 4px;font-size:9.5px;margin-bottom:2px'>⚠️开仓需谨慎</div>"
                   if _gate(sgm.get(c), "开仓") == "CAUTION" else "")
                  + f"<b style='color:{col}'>{r.get('subtype')}</b>"
                    f"<br><span style='color:{PALETTE['buy']};font-weight:600'>{r.get('action_state')}</span>"
                  + (f"<br><span style='font-size:9px;color:#64748b'>2周涨"
                     f"{d.get('p_up')}%</span>" if d.get("p_up") else ""))
            + _td(f"<b style='color:{PALETTE['buy']}'>{zone}</b>"
                  + (f"<br><span style='font-size:9px'>破{ep.get('breakout')}</span>"
                     if ep.get("breakout") else "")
                  + f"<br><span style='font-size:9px;color:{PALETTE['buy']}'>"
                    f"{(r.get('triggers') or {}).get('enter', '—')}</span>")
            + _td(str((r.get("triggers") or {}).get("invalid", "—")).replace("跌破止损", ""))
            + _td(f"{r.get('opportunity_type', '—')}<br>"
                  f"<span style='font-size:9px'>{r.get('horizon', '')}</span>")
            + _td("<span style='font-size:9.5px'>"
                  + " ".join(f"<span style='color:{PALETTE['wait'] if v.get('pass') else PALETTE['warn']}'>"
                             f"{BUCKET_CN[k][:2]}{v.get('score'):.0f}</span>"
                             for k, v in bk.items())
                  + f"<br><span style='color:#64748b'>风险{r.get('risk_score')}·"
                    f"完备{r.get('data_completeness')}·置信{r.get('model_confidence')}</span>"
                  + "</span>", "max-width:190px;line-height:1.5")
            + _td(f"缺: <b style='color:#b45309'>"
                  f"{'、'.join(r.get('missing') or []) or '无'}</b>"
                  f"<br><span style='font-size:9px'>{r.get('why_not_higher', '')[:34]}</span>"
                  + (f"<br><span style='font-size:9px;color:{PALETTE['sell']}'>⚠️IN/OUT冲突"
                     f"({cf.get('severity')})</span>" if cf else ""), "max-width:150px")
            + _td("<span style='font-size:9px'>"
                  + (str(r.get("why_focus"))[:46] if r.get("why_focus") else
                     (str(d.get("reason") or "")[:40] or
                      f"{r.get('opportunity_type', '')}·"
                      f"{(r.get('buckets') or {}).get('timing', {}).get('basis', '')[:30]}")
                     + "<span style='color:#b45309'>(池外新入,why_buy未覆盖)</span>")
                  + "</span>", "max-width:170px")
            + "</tr>")

    # ── OUT 表(两段共用同一行构造) ──
    def _out_row(g):
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
        return (
            "<tr>"
            + _td(f"{_flag(c, d.get('market'))}<b>{g.get('name')}</b>"
                  f"<br><span style='color:#94a3b8;font-size:9px'>{c}</span>")
            + _td((f"<span style='color:#94a3b8;font-size:10px;font-weight:700'>"
                   f"#{g.get('board_rank')}</span> " if g.get("board_rank") else "")
                  + f"<span style='background:{col};color:#fff;border-radius:3px;padding:1px 5px;"
                    f"font-weight:800;font-size:12px'>{lv}</span>"
                    f"<br><span style='color:{PALETTE['sell']};font-weight:700;font-size:11px'>卖出分"
                    f"{g.get('sell_score', '—')}</span>", "white-space:nowrap")
            + _td(f"<b>现{g.get('px', '—')}</b><br>"
                  f"<span style='font-size:9px;color:#64748b'>MA20 {g.get('ma20', '—')}</span>")
            # 【三方定纲·第三层】身份决定**措辞**不决定风险等级:同一个-3A,
            # 持仓说"减仓/退出",自选说"回避·禁止开仓",候选说"取消推荐资格"——
            # 这是动作语义转换,不是风险降级(GPT纠正Claude原方案"身份定主答方"之误)。
            + _td((f"<b style='color:{col}'>{g.get('say')}</b><br>" if g.get("say") else "")
                  + f"<span style='font-size:9.5px;color:{col}'>{g.get('action')}</span>"
                  + "<br><span style='font-size:8.5px;color:#475569'>"
                  + " ".join(f"{k}<b style='color:"
                             f"{PALETTE['sell'] if v in ('BLOCK', 'TRIGGERED') else PALETTE['warn'] if v in ('CAUTION', 'WATCH', 'DEMOTE') else PALETTE['wait']}'>"
                             f"{v}</b>" for k, v in (g.get("gates") or {}).items())
                  + "</span>"
                  + f"<br><span style='font-size:9px;color:#64748b'>紧迫度 <b>"
                    f"{g.get('urgency', '—')}</b>"
                    f"{'高' if lv == '-3A' else '中' if lv == '-2A' else '低'}</span>", "max-width:170px")
            + _td(f"<b style='color:{PALETTE['sell']}'>{g.get('sell_zone', '—')}</b>"
                  + (f"<br><span style='font-size:9px;color:{PALETTE['buy']}'>🔁重买: {rb[:44]}</span>"
                     if rb else "<br><span style='font-size:9px;color:#b45309'>"
                                "⚠️缺重买条件(只说一半)</span>")
                  # GPT失效模式⑥:-3A否决的是**当前入场窗口**,不是永久看空。必须绑解除条件,
                  # 否则"暂时否决"会被读成"这票废了"。
                  + (f"<br><span style='font-size:8.5px;color:#64748b'>"
                     f"{g.get('window_note')}</span>" if g.get("window_note") else ""),
                  "max-width:190px")
            + _td(g.get("stop") or "—")
            # 非持仓的"止损价"是按假设持有反推的,距离只作参考——不加这行标注,
            # 会让人以为非持仓票也有真实止损位(-3A新东方距19.9%曾误导排序)
            + _td((f"{g.get('dist_stop_pct')}%"
                   + ("" if g.get("held") else
                      "<br><span style='font-size:8.5px;color:#94a3b8'>参考·未持有</span>"))
                  if g.get("dist_stop_pct") is not None else "—")
            + _td(f"1-2-3: <b>{c123 or '无'}</b>"
                  + (f"<br><span style='font-size:9px;color:{PALETTE['sell']}'>{bp}</span>" if bp else "")
                  + f"<br><span style='font-size:9px;color:#94a3b8'>"
                    f"{g.get('opp_type') or '门派未定'}</span>", "max-width:150px")
            + _td((f"<span style='font-size:9px;color:{PALETTE['sell']}'>⚠️IN/OUT冲突"
                   f"({cf.get('severity')}): 买入侧{cf.get('in_tier')}·{cf.get('in_action')}"
                   f"<br>{cf.get('verdict', '')[:40]}</span>" if cf else
                   "<span style='font-size:9px;color:#b45309'>"
                   + "；".join(g.get("school_notes") or []) + "</span>"
                   if g.get("school_notes") else
                   f"<span style='font-size:9px;color:{PALETTE['hold']}'>无IN/OUT冲突·"
                   f"门派({g.get('opp_type') or '未定'})内判定一致</span>"), "max-width:150px")
            + _td((f"<span style='font-size:9.5px;color:{PALETTE['hold']};font-weight:700'>💼持仓</span>"
                   if g.get("held") else
                   f"<span style='font-size:9.5px;color:{PALETTE['wait']}'>👁"
                   f"{g.get('scope') or '非持仓'}</span>")
                  + f"<br><span style='font-size:9px;color:#64748b'>"
                    f"引擎:{d.get('action') or '—'}</span>", "max-width:120px")
            + "</tr>")

    out_rows = "".join(_out_row(g) for g in _board)
    _bs = _bd.get("by_scope") or {}
    _funnel = ("　".join(f"{k} {v[0]}→<b>{v[1]}</b>" for k, v in _bs.items())
               if _bs else "")

    near = ""
    if n3 == 0:
        cand = [r for r in rows if r.get("tier") == "2A"]
        if cand:
            b = min(cand, key=lambda r: len(r.get("missing") or []))
            near = (f"　离3A最近: <b>{b.get('name')}</b> 差「"
                    f"{'、'.join(b.get('missing') or [])}」")
    cons = sg.get("consistency") or {}
    return (head
            + f"<div style='font-size:13px;font-weight:800;color:{PALETTE['buy']};margin:8px 0 3px;"
              f"border-left:4px solid {PALETTE['buy']};padding-left:6px'>"
              f"🟢 IN · 买入侧（3A/2A 核心推荐）</div>"
            # 【P1·覆盖率门禁上屏】GPT:"关键桶覆盖低于阈值时禁止发布确定性的IN榜,
            # 改报'评估不完整'"。不清空表(那等于另一种隐瞒),而是把"这份名单还不能当结论"
            # 明写在最前面——同类故障曾静默存在整天,就因为界面上没有覆盖率这个数。
            # 数据质量警告=琥珀,不是红(红专属卖出)。浅底深字,保证小字号也读得清。
            + (f"<div style='font-size:12px;background:{PALETTE['warn_bg']};"
               f"border:1px solid #fcd34d;border-left:4px solid {PALETTE['warn']};"
               f"border-radius:6px;padding:7px 10px;margin-bottom:5px;"
               f"color:{PALETTE['warn']};line-height:1.6'>"
               f"⚠️ <b style='font-size:12.5px'>评估不完整 — 本 IN 榜暂不得当作确定性结论</b><br>"
               f"<span style='color:#78350f'>关键桶最低覆盖率 "
               f"<b>{_cov.get('key_min_coverage')}%</b> &lt; 红线 {_cov.get('threshold')}%　"
               + "　".join(
                   f"<span style='color:"
                   f"{PALETTE['hold'] if v >= 85 else PALETTE['warn']}'>"
                   f"{BUCKET_CN.get(k, k)[:2]}<b>{v}%</b></span>"
                   for k, v in (_cov.get("bucket_coverage") or {}).items())
               + "<br>覆盖不足可能是<b>数据管道故障</b>，而非市场真的没机会——"
                 "同族病：某桶大量取同一默认值＝该桶对这批票零区分度。</span></div>"
               if _cov and not _cov.get("publishable") else "")
            + (f"<div style='font-size:11.5px;background:#eff6ff;border-radius:5px;"
               f"padding:4px 8px;margin-bottom:3px'>今日无 3A（现在可进+长期获益的完整机会），"
               f"不硬凑。{near}</div>" if n3 == 0 else "")
            + (_tbl(in_rows, _TH_IN) or "<div style='font-size:12px;color:#94a3b8'>今日无 3A/2A —— "
                                "现金也是仓位；战术级1A见买表折叠区</div>")
            # 【三方定纲·可见性】被 OUT 证据撤销买点的票若不在上表(如1A),必须单独点名——
            # 仲裁若不可见,等于没发生。GPT:"IN候选结构保留,当前买点撤销"要两句都说出来。
            + (f"<div style='font-size:11px;background:#fef2f2;border-left:3px solid {PALETTE['sell']};"
               f"border-radius:4px;padding:4px 8px;margin:4px 0'>"
               f"⛔ <b>本次买点被 OUT 证据撤销 {len(_revoked)} 只</b>："
               + "、".join(f"{n}<span style='color:#94a3b8'>({t})</span>" for n, t in _revoked)
               + "　<span style='color:#64748b'>买入结构可继续观察，但<b>现在不能买</b>"
                 "（解除需:缩量止跌／收复MA20／结构重新确认）。"
                 "两侧计算互盲、都不删——删一侧就成了用结果消音证据。</span></div>"
               if _revoked else "")
            # ══ 第一区：我的持仓（用户定纲"out首先关注的是我的持仓,其次是其他"）══
            + f"<div style='font-size:13px;font-weight:800;color:{PALETTE['sell']};margin:12px 0 2px;"
              f"border-left:4px solid {PALETTE['sell']};padding-left:6px'>"
              f"💼 第一区 · 我的持仓处置"
              f"（{_quiet.get('held_total', len(_own))}只全覆盖：{len(_own)}只有卖出信号 ／ "
              f"{_quiet.get('count', 0)}只无信号）</div>"
            + (_tbl("".join(_out_row(g) for g in _own), _TH_OUT)
               or f"<div style='font-size:12px;color:{PALETTE['hold']}'>持仓本轮无卖出信号</div>")
            # 无信号的持仓：必须交代状态，但绝不说"安全"或"建议继续持有"（GPT B3）
            + (f"<div style='font-size:11.5px;background:#f8fafc;border-left:3px solid {PALETTE['hold']};"
               f"border-radius:4px;padding:5px 8px;margin:4px 0'>"
               f"✅ <b>{_quiet.get('headline')}</b>"
               f"<span style='color:#64748b'>　（{_quiet.get('wording_rule')}）</span><br>"
               + "　".join(
                   f"<span style='display:inline-block;margin:1px 0'>{r.get('name')}"
                   f"<span style='color:#94a3b8;font-size:10px'>"
                   f"[{r.get('nearest_risk')}]</span></span>"
                   for r in (_quiet.get("rows") or []))
               + "</div>" if _quiet.get("rows") else "")
            # ══ 第二区：非持仓 ══
            + f"<div style='font-size:13px;font-weight:800;color:#b45309;margin:12px 0 2px;"
              f"border-left:4px solid #b45309;padding-left:6px'>"
              f"👁 第二区 · 非持仓（{len(_oth)}只 · 自选/池内候选，"
              f"<span style='font-weight:400'>不需要你动作，只是别买</span>）</div>"
            + (f"<div style='font-size:11px;color:#64748b;margin-bottom:3px'>"
               f"漏斗 信号{_bd.get('signals')} → 入榜<b>{_bd.get('on_board')}</b>："
               f"{_funnel}　<span style='color:#94a3b8'>入榜闸=接近度×持有档"
               f"（💼持仓{_bd.get('gates', {}).get('持仓', '')}最松／👁自选"
               f"{_bd.get('gates', {}).get('自选', '')}／🔍候选"
               f"{_bd.get('gates', {}).get('池内候选', '')}最严）。"
               f"排序口径未改（仍按接近触发度）——分区只决定<b>你先看到什么</b>，"
               f"不给持仓伪造风险分。未入榜=<b>还没到</b>，不等于安全。</span></div>" if _bd else "")
            + (_tbl("".join(_out_row(g) for g in _oth), _TH_OUT)
               or f"<div style='font-size:12px;color:{PALETTE['hold']}'>非持仓标的无回避信号 —— "
                                 "无 OUT 信号也是信号</div>")
            + f"<div style='font-size:10.5px;color:#94a3b8;margin-top:4px'>"
              f"OUT为影子级(攒战绩不触发交易)·与引擎卖警一致率{cons.get('rate', '—')}%"
              f"({cons.get('shadow_agrees', '—')}/{cons.get('engine_sell_calls', '—')})"
              f"·08-14起与sell_call核算对照后转正"
            + (f"　🗄已否决{len(arch)}只(不占推荐位)" if arch else "") + "</div>")
