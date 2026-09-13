"""grade_card.py — 【V88·评级完整输出卡】2026-08-02 用户第十五节全文落地，三端共用渲染。

用户要求每只股票输出：最终等级/子类型/同级排名/同级排序分/机会类型/适用周期/行动状态、
五桶板各自分数与达标状态、风险分/数据完整度/模型置信度、
为什么值得关注/为什么属于这个等级/为什么不是更高等级、缺失桶板/主要短板、
进入触发/升级/降级/失效条件、主要风险。

只用内联 HTML（与 barometer_ui / compare_ui 同一套做法），桌面与云端读同一份 rank_score.json。
"""
from __future__ import annotations
from html import escape
from scorecard_html import gpt_html, books_html, score_label, reasons_html, rubric_html, text as audit_text
from module_signal_view import legacy_review_text
import grade_focus
from stock_profile_view import display_name as profile_name, html as profile_html

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
    "buy2":     "#0369a1",   # 买入·次级(2A)
    "buy3":     "#475569",   # 买入·战术(1A) 橄榄绿,与2A可辨
    "sell":     "#991b1b",   # 卖出·清仓(-3A)
    "sell2":    "#dc2626",   # 卖出·减仓(-2A)
    "trim":     "#c2410c",   # 分批收回(-1A) 赭橙=卖但不全卖
    "wait":     "#0369a1",   # 等待/回踩/观察
    "hold":     "#475569",   # 持有不动·无动作
    "warn":     "#b45309",   # 数据/质量警告(非卖出)
    "warn_bg":  "#fffbeb",   # 警告底色:浅琥珀+深字,保证可读
    "mute":     "#94a3b8",   # 未评估/无数据
    # 【2026-08-02 用户"gpt的验证一定要有大写字母G,就像claude的C一样,不同颜色标注"】
    # C/G 是**身份标识**不是动作,所以不进买绿卖红那套语义色,自带固定专色:
    #   C=靛蓝(主脑,不可替代)  G=青(副脑,独立第二意见)
    # 裁决状态由后缀符号表达(✅通过/⚠️否决/—未表态),身份色恒定——
    # 这样一眼能分清"谁说的"与"说了什么"。
    "rules_gate": "#4338ca", # Ⓡ V88规则闸
    "gpt":      "#0d9488",   # legacy key: Ⓒ Codex独立复核
}
TIER_COLOR = {"3A": PALETTE["buy"], "2A": PALETTE["buy2"], "1A": PALETTE["buy3"],
              "0A": PALETTE["mute"], "待评估": PALETTE["mute"],
              "存档": PALETTE["hold"]}
# 行动状态统一八种（用户第十五节末）
ACTIONS = ("现在可进", "分批试探", "等待回踩", "等待突破",
           "短线进攻", "低吸埋伏", "继续观察", "风险回避")


def _text(value) -> str:
    """Escape data at its HTML boundary; rendered helper HTML stays intact."""
    return escape(str(value), quote=True)


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
    """All active cards project the central rubric, including incomplete reviews."""
    from review_scorecard import card_passed
    card = r.get("scorecard") or {}
    grade = str(r.get("tier") or "PENDING")
    if grade == "3A" and not card_passed(card):
        grade = "PENDING"
    return ("<div style='padding:10px;border:1px solid #cbd5e1;border-radius:6px'>"
            + f"<b>{audit_text(r.get('name'))} · {audit_text(grade)} · {score_label(card)}</b>"
            + f"<div>{audit_text(r.get('display_action') or r.get('action_state'))}</div>"
            + gpt_html(card) + books_html(card) + reasons_html(r) + "</div>")


def board_html(data: dict, limit: int = 8, show_detail: int = 3) -> str:
    """行动清单：3A/2A 出完整卡，其余出紧凑行；0A 不进（用户第十节）。"""
    rows = [r for r in (data.get("rows") or [])
            if str(r.get("tier")) != "0A" and r.get("listable") is True][:limit]
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
        f"<div style='font-size:12px;color:{PALETTE['warn']};margin:2px 0'>⚠️ <b>{_text(r.get('name'))}</b>"
        f"（{_text(r.get('code'))}）排序分 {_text(r.get('rank_score'))} · 五桶"
        f"{sum(1 for b in (r.get('buckets') or {}).values() if b.get('pass'))}/5达标"
        f" → 被拦：{_text(v)}</div>" for r, v in items)
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
_TH_IN = ("名称", "评级·分", "现价·数据日", "动作·执行状态", "买入区间·进入时机",
          "止盈区间·净空间·期限", "失效价", "机会类型·周期", "GPT‑6逐项评分", "书籍逐项核验", "判断依据·补证闭环")
_TH_OUT = ("名称", "风险级别·风险分", "现价·MA20", "动作·紧迫度", "卖出/回避区间·重买条件",
           "止损价", "距止损·量比", "1-2-3·旁路·门派", "冲突·仲裁", "规则闸·GPT审核", "持有状态·引擎")
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


def stock_link(name, code, flag: str = "") -> str:
    """【2026-08-02 用户定纲】"大3A系统里筛选出的个股,都可以鼠标点名称进行深度分析,
    全系统出现的个股都可以实现"。
    与站内既有 _nw_link9/_stk_link 同款深链:?q=代码&focus=deep#v88-deep-analysis,
    新标签打开(target=_self 在部分场景会吞掉深链参数,美港股点了没反应——已踩过)。
    无代码的条目(板块名等)原样返回文字,不生成死链。"""
    from html import escape
    from urllib.parse import quote
    c = quote(str(code or "").strip(), safe=".-")
    n = escape(str(profile_name(name, code) or ""))
    if not c:
        return flag + n
    return (f'{flag}<a href="?q={c}&focus=deep#v88-deep-analysis" target="_blank" '
            f'rel="noopener" style="color:inherit;text-decoration:underline;'
            f'text-underline-offset:2px;cursor:pointer">{n}</a>')


def cg_badge(who: str, state: str, note: str = "") -> str:
    """审核徽章。R=规则闸，G=GPT/Codex，书=经典书理。"""
    base = {"R": PALETTE["rules_gate"], "G": PALETTE["gpt"],
            "K": "#7c3aed", "书": "#b45309"}.get(who, PALETTE["gpt"])
    state = {"通过": "pass", "否决": "reject"}.get(state, state)
    # 三级:pass=主动背书(实心✅) / gate_pass=机检达标(空心✓,半亮) / reject=否决 / 未表态=—
    mark, op = ("✅", "1") if state == "pass" else \
               ("✓", ".72") if state == "gate_pass" else \
               ("○", ".68") if state == "不否定" else \
               ("⚠️", "1") if state == "reject" else ("—", ".45")
    t = f" title=\"{_text(note)}\"" if note else ""
    return (f"<span style='background:{base};color:#fff;border-radius:3px;"
            f"padding:0 5px;font-size:9.5px;font-weight:800;opacity:{op};"
            f"margin-left:2px;letter-spacing:.3px'{t}>{_text(who)}{mark}</span>")


def _book_review(code: str):
    try:
        import sys
        from pathlib import Path
        source = str(Path.home() / "Desktop" / "ai-daily-report-v2" / "src")
        if source not in sys.path:
            sys.path.insert(0, source)
        from recommendation_gate import classics_review_for
        return classics_review_for(str(code))
    except Exception:
        return "不可用", ""


def _book_health() -> dict:
    try:
        import json
        from pathlib import Path
        from datetime import datetime
        from review_display import fresh, BJT, source_times_fresh
        from review_scorecard import book_result
        doc = json.loads((Path.home()/"Desktop/ai-daily-report-v2/data/classics_lens.json").read_text())
        rows = list((doc.get("rows") or {}).values())
        now = datetime.now(BJT)
        hours = 72 if now.weekday() >= 5 else 30
        pack = json.loads((Path.home()/"Desktop/ai-daily-report-v2/data/review_factpack.json").read_text())
        items = {r.get("code"): r for r in pack.get("items") or []}
        valid = [r for code, r in (doc.get("rows") or {}).items()
                 if doc.get("factpack_id") == r.get("factpack_id") == pack.get("factpack_id")
                 and fresh(doc.get("generated_at"), hours, now)
                 and source_times_fresh(items.get(code) or {}, now, sources=r.get("source_timestamps") or {})]
        return {"verdicts":len(rows),"fresh_today":len(valid),"book_pass":sum(book_result(r)["passed"] for r in valid),"generated_at":doc.get("generated_at")}
    except Exception:
        return {}


def _nomd(v) -> str:
    """HTML 出口统一清 markdown 星号——落盘文案用 ** 强调是给人读的,
    直接塞进 HTML 会原样显示成星号(用户已抓过两次)。"""
    return str(v or "").replace("**", "")


def _tbl(rows_html: str, heads=None) -> str:
    if not rows_html:
        return ""
    from mobile_view import CSS, label_cells
    rows_html = label_cells(rows_html, heads or _TH_IN)
    th = "".join(f"<th style='padding:3px 5px;text-align:left;font-weight:600;"
                 f"border-bottom:2px solid #cbd5e1;white-space:nowrap'>{_text(h)}</th>"
                 for h in (heads or _TH_IN))
    return (CSS + f"<div class='v88-grade-scroll'><table class='v88-grade-table' style='width:100%;border-collapse:collapse;"
            f"font-size:10.5px;line-height:1.3'><thead style='background:#f8fafc;color:#475569'>"
            f"<tr>{th}</tr></thead><tbody>{rows_html}</tbody></table></div>")


def _td(v, style="") -> str:
    return f"<td style='padding:3px 5px;border-bottom:1px solid #f1f5f9;{style}'>{_nomd(v)}</td>"


def _px_fresh_cell(px, asof, basis="", usable=None, fallback_note="") -> str:
    """现价·时点单元格(2026-07-31 定纲:🟢同日=新鲜/⚠️红=隔日旧数据——
    旧价不许伪装成今天的判断;2026-08-17 用户抓'3A页价格不刷新'补,与行动中心同口径)。
    usable 来自 decisions 行的 px_usable_as_current:False 时强制按旧价标红,
    不信 asof(中海油 08-06 案:文件新、内容旧)。"""
    import datetime as _dt
    _today = _dt.datetime.now().strftime("%Y-%m-%d")
    _d, _t = str(asof or "")[:10], str(asof or "")[11:16]
    if px is None:
        return (f"<b>现—</b><br><span style='color:#dc2626;font-size:9px'>"
                f"⚠️{_text(fallback_note or '无盘中价')}</span>")
    _fresh = (_d == _today) if usable is None else bool(usable)
    if _fresh:
        _lab = "最近收盘·源时区" if _d != _today else "收盘定稿" if "收盘" in basis else "今日"
        return (f"<b>现{_text(px)}</b><br><span style='color:#16a34a;font-size:9px'>"
                f"🟢{_lab} {_text(_d[5:])}{_text((' ' + _t) if _t else '')}</span>")
    return (f"<b>现{_text(px)}</b><br><span style='color:#dc2626;font-size:9px'>"
            f"⚠️旧·{_text(_d[5:] if _d and _d != '' else '时点未知')}{_text((' ' + _t) if _t else '')}</span>")


def _gate(g: dict | None, key: str) -> str:
    """取某只票某个动作的门票。IN 侧读它**不构成"互为准入"**——门控发生在输出层，
    两个引擎的计算仍互盲(见 sell_grade.py 顶部三方定纲)。"""
    return str(((g or {}).get("gates") or {}).get(key) or "PASS")


def pipeline_status() -> dict:
    """读 3A 链路运行状态(在 ai-daily-report-v2/src/memory_audit.py 里),失败返回空。"""
    try:
        import sys as _s
        from pathlib import Path as _P
        _d = str(_P.home() / "Desktop" / "ai-daily-report-v2" / "src")
        if _d not in _s.path:
            _s.path.insert(0, _d)
        from memory_audit import pipeline_status as _ps
        return _ps()
    except Exception:
        return {}


def _run_strip() -> str:
    """3A链路运行状态条。**独立成函数**——上一版直接把 lambda 塞进 head 的
    括号表达式里,把 f-string 隐式拼接切断,整个模块 SyntaxError。
    渲染层的长表达式不要再往里插逻辑,插函数调用。"""
    p = pipeline_status()
    if not p:
        return ""
    if p.get("running"):
        return (f"<br>🟢 <b>正在跑</b>（{p.get('started_at')} 开始，整链约9分钟）")
    out = f"<br>⏸ 上次跑完 {p.get('last_run') or '—'}"
    if p.get("since_min") is not None:
        out += f"（{p['since_min']}分钟前）"
    if p.get("next_slot"):
        out += (f"　下一班 {p['next_slot']}"
                f"（{p['until_min'] // 60}小时{p['until_min'] % 60}分后）")
    return out


def _short_ts(value) -> str:
    """把三端常见时间格式压成分钟；仅作展示，不把发布时间冒充分析时间。"""
    text = str(value or "").strip()
    for suffix in ("（北京时间）", "(北京时间)"):
        text = text.replace(suffix, "").strip()
    return text[:16] if text else "—"


def _three_a_time_strip(rk: dict, sg: dict) -> str:
    """大3A独立时效条：结论时间与证据时间分开，名单不变也能确认已重算。"""
    in_at = _short_ts(rk.get("generated_at"))
    out_at = _short_ts(sg.get("generated_at"))
    evidence_times = [str(row.get("data_available_at") or "").strip()
                      for row in (rk.get("rows") or [])
                      if row.get("data_available_at")]
    in_data_at = _short_ts(max(evidence_times)) if evidence_times else "—"
    recalculated = in_at != "—" and out_at != "—"
    state = "IN/OUT分别按上述时间生成；数据时效逐行判断"
    return (
        "<div style='font-size:10.5px;opacity:.92;margin-top:2px'>"
        f"🕐 结论生成：IN {_text(in_at)} ｜ OUT {_text(out_at)}"
        f"<br>📊 证据可得：IN {_text(in_data_at)} ｜ OUT行情截止未单独落盘"
        f"　{state}</div>"
    )


_TRIAD_BUCKETS = (
    # 冻结态优先去重：即使上游意外把同一代码同时放入 recommendations，
    # 也必须先冻结、绝不能因后续桶重复而恢复执行许可。
    ("blocked_3a", "3A冻结", "冻结·不可执行", False),
    ("recommendations", "3A现买", "现在可进", True),
    ("preparations", "3A准备", "准备买·等待触发", False),
    ("conditional", "2A条件", "条件买·不可直接执行", False),
    ("observations", "", "", False),
    ("pending", "PENDING", "待复核·不是否决", False),
    ("excluded", "不予推荐", "已退出当前推荐·保留档案", False),
)


def _review_gap_strip(rows: list[dict]) -> str:
    """Explain empty grades from current evidence; never imply a market verdict."""
    if not rows:
        return ""
    current = [r for r in rows if (r.get('scorecard') or {}).get('gpt', {}).get('current')
               and (r.get('scorecard') or {}).get('gpt', {}).get('complete')]
    one_a = [r for r in current if r.get('tier') == '1A']
    lack = {}
    for key, label in [('countercase', '反证'), ('horizon', '周期路径')]:
        count = sum(any(c.get('id') == key and type(c.get('score')) in (int, float)
                        and c['score'] < 15 for c in r['scorecard']['gpt'].get('criteria') or [])
                    for r in one_a)
        if count:
            lack[label] = count
    state = '当前评估范围' if len(current) == len(rows) else '全量推演尚未完成'
    detail = ('当前1A中' + '、'.join(f'{k}未达15分{v}只' for k, v in lack.items()) + '。') if lack else ''
    return (f"<details class='v88-review-gap' style='font-size:12px;background:{PALETTE['warn_bg']};"
            f"border-left:3px solid {PALETTE['warn']};padding:5px 8px;margin:3px 0'>"
            f"<summary>{state} · 展开审核缺口与卡级原因</summary>"
            f"本页含跟踪档案 {len(rows)} 只，当前完整GPT双审 {len(current)} 只。"
            f"其余 {len(rows)-len(current)} 只未形成当前完整双审（含缺数据、排除项和风险分流）。"
            f"{detail}空榜不能解释为全市场没有机会；缺审与审核未通过分别留档。</details>")


def _triad_v2_rows(rk: dict, triad: dict | None) -> tuple[list[dict], bool]:
    """Overlay central v2 states on legacy rank evidence.

    Rank rows retain buckets/explanations only.  Tier, action and every trade
    contract field come from triad_selection.  When v2 is absent the legacy
    rows remain visible but are explicitly non-executable.
    """
    legacy = {str(row.get("code") or ""): row for row in (rk.get("rows") or [])}
    triad = triad if isinstance(triad, dict) else {}
    version = str(triad.get("version") or "").strip()
    schema = triad.get("schema_version", triad.get("schema"))
    if isinstance(schema, dict):
        schema = schema.get("version")
    explicit_v2_schema = str(schema or "").strip().lower() in {
        "2", "v2", "triad_selection_v2", "triad-selection-v2",
    }
    is_v2 = bool(
        triad.get("factpack_id")
        and version == "gpt-classics-selection-v9-tharp"
    )
    if not is_v2:
        fallback = []
        for original in (rk.get("rows") or []):
            row = dict(original)
            row.update({
                "listable": False, "formal_recommendation": False,
                "central_bucket": "legacy", "central_state": "LEGACY_FALLBACK",
                "display_action": "旧口径参考·不可执行（待复核·不可执行）",
                "triad_fallback": True,
            })
            fallback.append(row)
        return fallback, False

    rows: list[dict] = []
    seen: set[str] = set()
    for bucket, fixed_tier, fixed_action, executable in _TRIAD_BUCKETS:
        for central in triad.get(bucket) or []:
            code = str(central.get("code") or central.get("canonical_code") or "")
            if not code or code in seen:
                continue
            seen.add(code)
            row = dict(legacy.get(code) or {})
            plan = dict(central.get("trade_plan") or {})
            tier = str(central.get("tier") or "PENDING")
            state = str(central.get("state") or "")
            display_tier = fixed_tier
            display_action = fixed_action
            if bucket == "observations":
                if tier == "1A":
                    display_tier, display_action = "1A价值跟踪", "价值已确认·等待时间与价格"
                else:
                    display_tier, display_action = "2A·价值机会", "本周期2A空间达标·等待入场条件"
            from review_display import actionable, current_scorecard
            from review_scorecard import card_passed
            live_card = current_scorecard(triad, central)
            from investment_maturity import assess
            live_value = assess(live_card, central.get("horizon"), plan)
            expired_quality = (tier == "3A" and (not card_passed(live_card) or live_value["tier"] != "3A")
                               or tier in {"1A", "2A"} and (not live_value["value_confirmed"] or live_value["tier"] != tier))
            if expired_quality:
                tier, display_tier, display_action = "PENDING", "待复核", "双审已过期或不完整·待重新评分"
            publish_eligible = bool(executable and actionable(triad, central))
            if bucket == "recommendations" and not publish_eligible:
                display_tier = "待复核"
                display_action = "当前审核不完整或已过期·不可执行"
            tracking = central.get("tracking") or {}
            if tier == "PENDING" and tracking.get("last_value_tier"):
                display_tier = "原" + tracking["last_value_tier"] + "·复核中"
                display_action = "保留原价值档案·当前审核需更新·暂停执行"
            row.update({
                "code": central.get("code") or code,
                "name": central.get("name") or row.get("name") or code,
                "market": central.get("market") or row.get("market"),
                "opportunity_type": central.get("opportunity_type") or row.get("opportunity_type"),
                "tier": tier, "display_tier": display_tier,
                "display_action": display_action,
                "action_state": display_action,
                "publish_eligible": publish_eligible,
                "listable": publish_eligible,
                "formal_recommendation": publish_eligible,
                "central_bucket": "pending" if expired_quality or bucket == "recommendations" and not publish_eligible else bucket, "central_state": state,
                "central_trade_plan": plan, "triad_fallback": False,
                "scorecard": live_card, "central_reviews": central.get("reviews") or {},
                "master_ref": __import__('stock_reference').reference(triad,central),
                "rank_score": live_card.get("total"), "rank": None,
                "value_assessment": live_value, "watch_plan": central.get("watch_plan") or {}, "tracking": central.get("tracking") or {},
                "execution": central.get("execution") or {}, "risk": central.get("risk") or {},
                "system": central.get("system") or {}, "action_blocks": central.get("action_blocks") or [],
                "subtype": central.get("label") or "待复核",
                "data_available_at": central.get("factpack_asof"),
                "central_price_asof": central.get("factpack_asof"), "central_data_fresh": central.get("data_fresh"),
                "missing": (central.get("scorecard") or {}).get("missing") or [],
                "horizon": {"short": "短期（1–30自然日）", "medium": "中期（31–90自然日）", "long": "长期（91–365自然日）"}.get(central.get("horizon"), "周期待确定"),
                "rr": plan.get("rr") if plan.get("rr") is not None else row.get("rr"),
            })
            reviews = central.get("reviews") or {}
            gpt_review = reviews.get("gpt") or {}
            row["verification"] = {
                "rules_gate": (reviews.get("claude_cs") or {}).get("verdict"),
                "gpt": (central.get("votes") or {}).get("gpt"),
                "book": (central.get("votes") or {}).get("classics"),
                "gpt_note": str(gpt_review.get("why") or "")
                    + "；反证：" + str(gpt_review.get("counterargument") or "未提供")
                    + "；失效：" + str(gpt_review.get("invalidation") or "未提供"),
                "gpt_at": gpt_review.get("at"), "listable": publish_eligible,
                "why": "；".join(central.get("reason_codes") or []),
            }
            triggers = dict(row.get("triggers") or {})
            triggers["enter"] = (plan.get("promotion_trigger") or plan.get("entry_range")
                                 or triggers.get("enter") or "—")
            triggers["invalid"] = (plan.get("invalidation") or plan.get("stop")
                                   or triggers.get("invalid") or "—")
            row["triggers"] = triggers
            old_contract = dict(row.get("execution_contract") or {})
            snapshot = dict(old_contract.get("snapshot") or {})
            for key in ("last", "stop", "target", "rr", "horizon", "position_cap"):
                if plan.get(key) is not None:
                    snapshot[key] = plan.get(key)
            snapshot["entry_range"] = plan.get("entry_range") or ""
            snapshot["mode"] = display_action
            row["execution_contract"] = {
                **old_contract,
                "ready": bool(row["listable"] and plan.get("ready") is True),
                "snapshot": snapshot,
            }
            rows.append(row)
    return rows, True


def system_table_html(rk: dict, sg: dict, dec: dict, why_sells: dict,
                      pool: dict = None, limit_in: int = 12, limit_out: int = 8,
                      triad: dict = None, weekly: dict = None,
                      reverse_audit: dict = None, reverse_status: dict = None,
                      relations: dict = None, watchlist: dict = None) -> str:
    """3A大系统完整模块(标题+IN表+OUT表+尾注),一次返回全部HTML。"""
    rows, triad_v2 = _triad_v2_rows(rk, triad)
    from persistent_watchlist_ui import html as watchlist_html
    persistent_html = watchlist_html(watchlist or {}, triad or {},view='current')
    tracking_html = watchlist_html(watchlist or {}, triad or {},view='tracking')
    fixed_archive = watchlist_html(watchlist or {},triad or {},view='history')
    if fixed_archive:
        paused_count=sum(str(r.get('seat_kind','')).startswith('暂停研究') for r in (watchlist or {}).get('rows',[]))
        fixed_archive=(f"<details class='v88-fixed-period-history' style='font-size:12px;margin:8px 0'><summary>本期固定跟踪档案（{len(watchlist.get('rows',[]))}只 · {paused_count}只当前暂停）· 升降级均保留</summary>"+fixed_archive+"</details>")
    focus = grade_focus.build(rows) if triad_v2 else None
    focus_rows = rows
    if focus is not None:
        # Limit BEFORE separating executable/preparing/frozen or time horizons.
        indexed = {grade_focus.canonical(r.get('code')): r for r in rows}
        focus_rows = [indexed[c] for c in focus['selected_codes']]
    # 页头只统计真正获得执行许可的等级。未通过的桶3A属于候选，不能再显示
    # “IN:3A×1”；这是 2026-08-17 长江证券真实成交事故的直接视觉诱因。
    n3 = sum(1 for r in rows
             if r.get("central_bucket") == "recommendations"
             and r.get("publish_eligible") is True
             and ((focus or {}).get('records', {}).get(grade_focus.canonical(r['code']), {}).get('entry_opportunity') or {}).get('executable') is True)
    n3b = sum(1 for r in rows if r.get("central_bucket") == "blocked_3a")
    n3p = sum(1 for r in rows if r.get("central_bucket") == "preparations")
    n2c = sum(1 for r in rows if r.get("central_bucket") == "conditional")
    n2o = sum(1 for r in rows if r.get("central_bucket") == "observations"
              and r.get("tier") == "2A")
    n1 = sum(1 for r in rows if r.get("central_bucket") == "observations"
             and r.get("tier") == "1A")
    np = sum(1 for r in rows if r.get("central_bucket") == "pending")
    nc = n3b + n3p + n2c + n2o + n1 + np
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
    cycle_summary = " ｜ ".join(
        f"{label}价值分层 {sum(r.get('tier') in {'1A','2A','3A'} and str(r.get('horizon') or '').startswith(label) for r in rows)}只"
        for label in ("短期", "中期", "长期"))
    from scan_progress_view import html as scan_progress_html
    current_review_count = sum(bool((r.get('scorecard') or {}).get('gpt',{}).get('current')) for r in rows)
    empty_cause = ('行情或双审待更新；历史评级保留在下方' if not current_review_count
                   else '尚无同时满足本档证据、收益空间与成熟条件的标的')
    head = (f"<section id='v88-3a-system' class='v88-triad-header' style='color:#1e293b;margin:4px 0 6px'>"
            f"<div style='font-size:16px;font-weight:800'>🎯 3A大系统 · 中长期主线 / 短期跟踪 / OUT</div>"
            + scan_progress_html(rows)
            + f"<div class='v88-grade-counts' style='font-size:13px;padding:6px 0;font-weight:600'>"
            + ((f"当前研究精选 {(watchlist.get('current_focus') or {}).get('total',0)}只 · " + " ｜ ".join(f"{m} {v['selected']}只" for m,v in (watchlist.get('current_focus') or {}).get('markets',{}).items())) if persistent_html else
               "重点榜：" + " ｜ ".join(f"{g} {sum(v['selected'] for v in focus['summary'][g].values())}只" for g in grade_focus.GRADES)
               + f" <span style='font-weight:400'>· 每档最多15只 / 每市场最多5只</span>" if focus else
               f"全部原周期评级：3A {n3+n3b+n3p}只 ｜ 2A {n2c+n2o}只 ｜ 1A {n1}只")
            + f" <span style='font-size:12px;font-weight:400'>｜ 可执行3A {n3}只 · 持仓卖警 {len(_own)}只</span>"
            f"</div></section>")
    guide = ("<details class='v88-list-guide' style='font-size:12px;color:#64748b;margin:3px 0'>"
              "<summary>评级规则、周期与更新时间</summary>"
            + (f"<div style='font-size:12px;color:#64748b'>{cycle_summary}</div>" if triad_v2 else "")
            # 【2026-08-13 用户定纲】结论生成、证据可得、流水线状态必须分开显示；
            # 名单没变化时也明确标注“本班已重算”，避免误判系统停更。
            + "<details style='font-size:12px;color:#64748b'><summary>结论与证据时间</summary>"
            + f"IN: 3A现买×{n3} ｜ 3A冻结×{n3b} ｜ 3A准备×{n3p} ｜ 2A条件×{n2c} "
              f"｜ 2A价值×{n2o} ｜ 1A价值×{n1} ｜ OUT: 持仓卖警×{len(_own)}。"
            + _three_a_time_strip({"rows": rows, "generated_at": triad.get("generated_at")} if triad_v2 else rk, sg)
            + "列表每60秒读取最新已发布结果；行情采集与GPT审核分别运行。</details>"
            + (_review_gap_strip(rows) if triad_v2 else "")
            + rubric_html() + "</details>")

    # ── IN 表 ──
    # 【铁律19·分级验证准入闸】用户:"没有claude验证不能上榜,这是关键,gpt为辅它通过也不行"。
    # 生产端已算好 listable;此处**只放行 listable=True**,被拦的单独点名不隐藏。
    _vf = rk.get("verification") or {}
    if triad_v2:
        _vf = {"stats": {"双审已获3A": n3+n3p+n3b,
                         "待GPT-6新版审核": sum(not ((r.get("scorecard") or {}).get("gpt") or {}).get("current") for r in rows),
                         "书籍尚未全通过": sum(not ((r.get("scorecard") or {}).get("books") or {}).get("passed") for r in rows)}}
    _blk = [(str(r.get("name")), str(r.get("tier")),
             str((r.get("verification") or {}).get("why") or ""))
            for r in rows if r.get("tier") in ("3A", "2A", "1A")
            and r.get("verification") and not r.get("listable")]
    _n_listable = sum(1 for x in rows if x.get("tier") in ("3A", "2A", "1A")
                      and x.get("listable", True))
    # 【用户定纲 2026-08-02】"被拦的也专门是一个被拦的组,就像-3a里的非持仓组别一样"
    # 原先只在漏斗行拖一句"被拦:A、B、C…等N只"——名字挤成一行,看不到为什么被拦、
    # 原判什么档、缺哪一方。改为**独立成组成表**,与 OUT 第二区同规格。
    # 【用户定纲 2026-08-02】"被拦阻里如果有你不同意的、否定的,不能出现"
    # 即:**Claude 已判否的票不进被拦组**。理由成立——被拦组读起来像"差一点就上榜",
    # 把主脑已经判死的票摆在那里,既是噪音又可能诱导操作;它们的正确位置是漏斗计数与尾注。
    # 留在组里的只有**还可能翻身的**:流程没跑到(两方未表态)或仅GPT侧受阻。
    # 与既有铁律一致(claude-standard-gate:"红标不进推荐位")。
    _blocked_rows = [x for x in rows if x.get("central_bucket") in
                     ("blocked_3a", "preparations", "conditional",
                      "observations", "pending", "excluded", "legacy")]
    _data_queue = [r for r in _blocked_rows if r.get("tier") not in {"3A", "2A", "1A"}]
    if triad_v2:
        _blocked_rows = [r for r in _blocked_rows if r.get("tier") in {"3A", "2A", "1A"} or (r.get("tracking") or {}).get("last_value_tier")]
    _current_research_count = sum(r.get("tier") in {"3A", "2A", "1A"} for r in _blocked_rows)
    _research_scope = (f"{_current_research_count}只当前评级 · {len(_blocked_rows)-_current_research_count}只历史跟踪"
                       if triad_v2 else f"{len(_blocked_rows)}只")
    _n_cl_rej = sum(1 for x in rows if x.get("tier") in ("3A", "2A", "1A")
                    and (x.get("verification") or {}).get("rules_gate") == "reject")
    in_rows = ""
    blocked_html = ""
    history_html = ""

    def _in_row(r, blocked: bool = False):
        """IN 行构造(上榜组与被拦组共用同一套列,保证两组可直接对照)。"""
        c = str(r.get("code"))
        d = dec.get(c) or {}
        snap = ((r.get("execution_contract") or {}).get("snapshot") or {})
        # rank 随行快照优先：候选不在 intraday_decisions 覆盖面时，买区也不能消失。
        ep = snap or d.get("entry_plan") or {}
        z = ep.get("zone") or []
        central_range = str((r.get("central_trade_plan") or {}).get("entry_range") or "")
        zone = (central_range if central_range else
                f"{z[0]}~{z[1]}" if len(z) >= 2 else
                (f"回踩{ep.get('pullback')}" if ep.get("pullback") else "—"))
        shown_tier = (str(r.get("display_tier") or
                          (f"{r.get('tier')}候选" if blocked else r.get("tier"))))
        shown_action = str(r.get("display_action") or r.get("action_state") or
                           ("⛔待复核·不可执行" if blocked else ""))
        col = PALETTE["warn"] if blocked else TIER_COLOR.get(str(r.get("tier")), "#64748b")
        cf = (sgm.get(c) or {}).get("in_out_conflict")
        bk = r.get("buckets") or {}
        top = (focus or {}).get('records', {}).get(grade_focus.canonical(c)) or {}
        meta = top.get('metrics') or {}
        from entry_opportunity import assess as entry_assess, html as entry_html
        entry_state = top.get('entry_opportunity') or entry_assess(r)
        focus_detail = ''
        if top.get('selected'):
            scores = meta['review_scores']
            focus_detail = (
                f"<div style='font-size:11px;color:#475569'>净空间 {meta['net_upside_pct']:.2f}%"
                f" · 净收益风险比 {meta['net_reward_risk']:.2f}<br>"
                f"待成熟 {meta['remaining_count']}项</div>"
                f"<details class='v88-focus-evidence'><summary>入选依据与分数分解</summary>"
                f"{audit_text(top['reason'])}<br>主审 {scores.get('primary')}分 / 独立反审 {scores.get('counteraudit')}分；"
                f"采用原保守裁决（{'主审' if meta.get('selected_review') == 'primary' else '独立反审'}），与书理 {meta['book_score']:g}分取低，"
                f"原审核分 {meta['audit_score']:g}，不加排名奖励分。<br>"
                f"本周期本档净空间门槛 {meta['period_floor_pct']:g}%；当前为门槛的 {meta['space_multiple']:.2f}倍。"
                f"风险比排序档 {meta['rr_band']/10:.1f}、空间倍数档 {meta['space_band']/10:.1f}；"
                f"同档技术性代码排序不代表质量差距。<br>待补："
                + audit_text('；'.join(meta['remaining_conditions']) or '书理与双审已通过，另核执行条件')
                + '</details>')
        from scorecard_html import profit_html
        return (
            (f"<tr class='v88-focus-row' data-grade='{audit_text(r.get('tier'))}' data-market='{audit_text(top['market'])}' data-code='{audit_text(c)}'>" if top.get('selected') else "<tr>")
            + _td(f"<b>{stock_link(r.get('name'), c, _flag(c, r.get('market')))}</b>"
                  f"<br><span style='color:#94a3b8;font-size:9px'>{_text(c)}</span>" + profile_html(c))
            # 【2026-08-02 用户"有点堆叠,可以调整"】五桶板缩写原挤在评级列(徽章+分+5项一坨,
            # 窄屏竖排)。移到下方"风险·完备·置信"列——那列只有3个数,有横向空间。
            + _td((f"<div style='font-size:11px;color:#475569'>{audit_text(top['market'])} Top {top['rank']}</div>" if top.get('selected') else '')
                  + f"<span style='background:{col};color:#fff;border-radius:3px;padding:1px 5px;"
                  f"font-weight:800;font-size:12px'>{_text(shown_tier)}</span>"
                  f"<br><span style='color:#1d4ed8;font-weight:700;font-size:11px'>"
                  f"{('上次审核分 '+_text(r['tracking']['last_value_score'])+'/100<br>'+_text(str(r['tracking'].get('last_value_at',''))[:16])) if r.get('tier') in {'PENDING','0A'} and (r.get('tracking') or {}).get('last_value_score') is not None else score_label(r.get('scorecard') or {})}</span>", "min-width:110px;max-width:150px;white-space:normal")
            # 【2026-08-17 用户抓"3A页价格不刷新"】现价列接入新鲜度守卫:
            # 有盘中价→🟢今日/收盘定稿;旧价→⚠️红;无价→明说基期,不再恒绿伪装。
            + _td(_px_fresh_cell(snap.get("last") or d.get("last"), r.get("central_price_asof") or d.get("asof"), "同包收盘价" if r.get("central_price_asof") else d.get("px_basis"),
                                 r.get("central_data_fresh") if "central_data_fresh" in r else d.get("px_usable_as_current"),
                                 f"无盘中价·评级基期{str(snap.get('data_asof') or r.get('data_available_at') or '')[:10][5:]}"))
            # 【2026-08-02 三方定纲】IN 侧有 OUT 风险证据时**不删行**(GPT:原IN结构尚可观察),
            # 而是把"本次入场触发已失效"的门票摆在动作位——机会结构仍在 ≠ 现在能买,
            # 这两件事必须能同时表达。删行=监守自盗的镜像(用一侧结果消音另一侧证据)。
            + _td(entry_html(entry_state)
                  + f"<details><summary>原中央状态</summary>{audit_text(shown_action)}；当前行动以上方进场核验为准。</details>"
                  + f"<details><summary>评级说明</summary>{audit_text(r.get('subtype') or '待复核')}</details>")
            + _td((f"<span style='color:{PALETTE['warn']};font-weight:700'>研究参考·非买单</span><br>"
                   if blocked else "")
                  + f"<b style='color:{PALETTE['warn'] if blocked else PALETTE['buy']}'>{_text(zone)}</b>"
                  + (f"<br><span style='font-size:9px'>破{_text(ep.get('breakout'))}</span>"
                     if ep.get("breakout") else "")
                  + f"<details><summary>入场触发条件</summary>"
                    f"{_text(('待触发·' + str((r.get('central_trade_plan') or {}).get('promotion_trigger') or '缺少有效触发')) if blocked else (r.get('triggers') or {}).get('enter', '—'))}</details>")
            + _td(profit_html(r), "min-width:160px;max-width:230px;line-height:1.5")
            + _td((f"<b>收盘跌破{snap['stop']:g}</b><details><summary>失效依据</summary>"
                   + audit_text((r.get('triggers') or {}).get('invalid')) + '</details>')
                  if type(snap.get('stop')) in (int, float) else audit_text((r.get('triggers') or {}).get('invalid')))
            + _td(f"{audit_text(r.get('horizon'))}<details><summary>策略说明</summary>"
                  f"{audit_text(r.get('opportunity_type'))}</details>")
            + _td(gpt_html(r.get("scorecard") or {}), "min-width:180px;max-width:260px;line-height:1.5")
            + _td(books_html(r.get("scorecard") or {}), "min-width:200px;max-width:280px;line-height:1.5")
            + _td(focus_detail + reasons_html(r), "min-width:190px;max-width:280px;line-height:1.5")
            + "</tr>")

    executable_rows = [x for x in focus_rows if x.get("tier") in ("3A", "2A", "1A") and x.get("listable") is True
                       and (focus['records'][grade_focus.canonical(x['code'])]['entry_opportunity']['executable'] if focus else False)]
    in_rows = "".join(_in_row(r) for r in (executable_rows if triad_v2 else executable_rows[:limit_in]))
    if triad_v2:
        blocks = []
        for grade, title in (("3A", "3A · 双审与书理通过"), ("2A", "2A · 等待成熟条件"), ("1A", "1A · 价值研究")):
            group = [r for r in focus_rows if r.get("tier") == grade and r not in executable_rows]
            total_selected = sum(v['selected'] for v in focus['summary'][grade].values())
            empty_note = ' · '+empty_cause if not total_selected else ''
            allocation = ' ｜ '.join(f"{m} {v['selected']}/{v['current']}" for m, v in focus['summary'][grade].items())
            execution_note = f"；其中{total_selected-len(group)}只许可标的见上表，本表跟踪{len(group)}只" if total_selected > len(group) else ''
            blocks.append(f"<tr><td colspan='11' style='padding:6px;background:#eff6ff'><b>{title} · 重点（{total_selected}只）</b>"
                          f"　<span style='font-size:11px'>{allocation}（重点/当前评级）{execution_note}</span>{empty_note}</td></tr>")
            blocks.extend(_in_row(r, blocked=True) for r in group)
        blocked_html = "".join(blocks)
        history_blocks = []
        retained = [r for r in _blocked_rows if r.get("tier") == "PENDING" and (r.get("tracking") or {}).get("last_value_tier")]
        if retained:
            history_blocks.append(f"<tr><td colspan='11' style='padding:9px;background:#f1f5f9'><b>持续保留的价值档案（{len(retained)}只）· 上次等级与分数注明审核日期，当前暂停执行</b></td></tr>")
            history_blocks.extend(_in_row(r,blocked=True) for r in retained)
        exits = [r for r in _blocked_rows if r.get("tier") == "0A" and (r.get("tracking") or {}).get("last_value_tier")]
        if exits:
            history_blocks.append(f"<tr><td colspan='11' style='padding:9px;background:#fff7ed'><b>已退出当前推荐的原价值档案（{len(exits)}只）· 显示收益门槛/失效原因，原分数仅供追溯</b></td></tr>")
            history_blocks.extend(_in_row(r, blocked=True) for r in exits)
        if history_blocks:
            history_html = (f"<details class='v88-grade-history' style='font-size:12px;color:#64748b;margin:8px 0'>"
                            f"<summary>历史评级与退出档案（{len(retained)+len(exits)}只）· 展开原列表</summary>"
                            + _tbl("".join(history_blocks), _TH_IN) + "</details>")
    else:
        blocked_html = "".join(_in_row(r, blocked=True) for r in _blocked_rows[:10])

    focus_guide = ''
    if focus is not None:
        reserve = [v for v in focus['records'].values() if not v['selected']]
        reserve.sort(key=lambda v: (grade_focus.GRADES.index(v['tier']), str(v['market']), v['rank'] or 99999, v['code']))
        reserve_lines = ''.join('<tr>' + _td(stock_link(v['name'], v['source_code']) + profile_html(v['source_code']))
            + _td(audit_text(v['source_code'])) + _td(audit_text(v['tier']))
            + _td(audit_text(v['reason'])+'<br><small>'+audit_text((indexed.get(grade_focus.canonical(v['code'])) or {}).get('display_action'))+'</small>') + '</tr>' for v in reserve)
        focus_guide = (
            f"<div class='v88-focus-summary' style='font-size:12px;color:#475569;margin:5px 0'>"
            f"重点 {focus['selected_count']}只 / 当前评级 {focus['current_count']}只；"
            f"其余 {focus['reserve_count']}只继续跟踪。不足名额留空；Top落选不等于降级。</div>"
            f"<details class='v88-focus-rules' style='font-size:12px;color:#64748b'><summary>重点榜排序与评分规则</summary>"
            + audit_text(focus['rule']) + '<br>原评分五项与经典书籍证据见每行；审核分不是胜率。'
            + f"<br>规则版本 {focus['version']}；全市场扫描与审核进度单独统计。</details>"
            + (f"<details class='v88-focus-reserve' style='font-size:12px;color:#64748b;margin:6px 0'>"
               f"<summary>候补跟踪（{len(reserve)}只）· 原等级与合同继续保留</summary>"
               + _tbl(reserve_lines, ['名称', '代码', '当前评级', '未入重点榜原因'])
               + '完整区间、分项及升降级沿革见个股深度页和下方持续跟踪台。</details>' if reserve else ''))

    # ── OUT 表(两段共用同一行构造) ──
    _sg_asof = str(sg.get("generated_at") or "")  # sell_grade 批量价的时点(行内无asof)

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
            + _td(f"<b>{stock_link(g.get('name'), c, _flag(c, d.get('market')))}</b>"
                  f"<br><span style='color:#94a3b8;font-size:9px'>{_text(c)}</span>" + profile_html(c))
            + _td((f"<span style='color:#94a3b8;font-size:10px;font-weight:700'>"
                   f"#{_text(g.get('board_rank'))}</span> " if g.get("board_rank") else "")
                  + f"<span style='background:{col};color:#fff;border-radius:3px;padding:1px 5px;"
                    f"font-weight:800;font-size:12px'>{lv}</span>"
                    f"<br><span style='color:{PALETTE['sell']};font-weight:700;font-size:11px'>卖出分"
                    f"{_text(g.get('sell_score', '—'))}</span>", "white-space:nowrap")
            # 【2026-08-17 用户抓"3A页价格不刷新"】OUT 现价同款守卫:
            # 优先 decisions 盘中行(带asof/usable),兜底 sell_grade 批量价(带文件时点)。
            + _td(_px_fresh_cell(d.get("last") if d.get("last") is not None else g.get("px"),
                                 d.get("asof") if d.get("last") is not None else _sg_asof,
                                 d.get("px_basis") if d.get("last") is not None else "",
                                 d.get("px_usable_as_current") if d.get("last") is not None else None,
                                 "无盘中价")
                  + f"<br><span style='font-size:9px;color:#64748b'>MA20 {_text(g.get('ma20', '—'))}</span>")
            # 【三方定纲·第三层】身份决定**措辞**不决定风险等级:同一个-3A,
            # 持仓说"减仓/退出",自选说"回避·禁止开仓",候选说"取消推荐资格"——
            # 这是动作语义转换,不是风险降级(GPT纠正Claude原方案"身份定主答方"之误)。
            + _td((f"<b style='color:{col}'>{_text(g.get('say'))}</b><br>" if g.get("say") else "")
                  + f"<span style='font-size:9.5px;color:{col}'>{legacy_review_text(g.get('action'))}</span>"
                  + "<br><span style='font-size:8.5px;color:#475569'>"
                  + " ".join(f"{_text(k)}<b style='color:"
                             f"{PALETTE['sell'] if v in ('BLOCK', 'TRIGGERED') else PALETTE['warn'] if v in ('CAUTION', 'WATCH', 'DEMOTE') else PALETTE['wait']}'>"
                             f"{_text(v)}</b>" for k, v in (g.get("gates") or {}).items())
                  + "</span>"
                  + f"<br><span style='font-size:9px;color:#64748b'>紧迫度 <b>"
                    f"{_text(g.get('urgency', '—'))}</b>"
                    f"{'高' if lv == '-3A' else '中' if lv == '-2A' else '低'}</span>", "max-width:170px")
            + _td(f"<b style='color:{PALETTE['sell']}'>{_text(g.get('sell_zone', '—'))}</b>"
                  + (f"<br><span style='font-size:9px;color:{PALETTE['buy']}'>🔁重买: {_text(rb[:44])}</span>"
                     if rb else "<br><span style='font-size:9px;color:#b45309'>"
                                "⚠️缺重买条件(只说一半)</span>")
                  # GPT失效模式⑥:-3A否决的是**当前入场窗口**,不是永久看空。必须绑解除条件,
                  # 否则"暂时否决"会被读成"这票废了"。
                  + (f"<br><span style='font-size:8.5px;color:#64748b'>"
                     f"{_text(g.get('window_note'))}</span>" if g.get("window_note") else ""),
                  "max-width:190px")
            + _td(_text(g.get("stop") or "—"))
            # 非持仓的"止损价"是按假设持有反推的,距离只作参考——不加这行标注,
            # 会让人以为非持仓票也有真实止损位(-3A新东方距19.9%曾误导排序)
            + _td((f"{_text(g.get('dist_stop_pct'))}%"
                   + ("" if g.get("held") else
                      "<br><span style='font-size:8.5px;color:#94a3b8'>参考·未持有</span>"))
                  if g.get("dist_stop_pct") is not None else "—")
            + _td(f"1-2-3: <b>{c123 or '无'}</b>"
                  + (f"<br><span style='font-size:9px;color:{PALETTE['sell']}'>{_text(bp)}</span>" if bp else "")
                  + f"<br><span style='font-size:9px;color:#94a3b8'>"
                    f"{_text(g.get('opp_type') or '门派未定')}</span>", "max-width:150px")
            + _td((f"<span style='font-size:9px;color:{PALETTE['sell']}'>⚠️IN/OUT冲突"
                   f"({_text(cf.get('severity'))}): 买入侧{_text(cf.get('in_tier'))}·{_text(cf.get('in_action'))}"
                   f"<br>{_text(cf.get('verdict', '')[:40])}</span>" if cf else
                   "<span style='font-size:9px;color:#b45309'>"
                   + _text("；".join(g.get("school_notes") or [])) + "</span>"
                   if g.get("school_notes") else
                   f"<span style='font-size:9px;color:{PALETTE['hold']}'>无IN/OUT冲突·"
                   f"门派({_text(g.get('opp_type') or '未定')})内判定一致</span>"), "max-width:150px")
            # 卖侧双验证徽章(铁律19v3对称:错误卖出信号直接损失真金)
            + _td((lambda _v: (
                cg_badge("R", str(_v.get("rules_gate") or ""))
                + cg_badge("G", str(_v.get("codex") or _v.get("gpt") or ""), str(_v.get("gpt_note") or ""))
                + (f"<br><span style='font-size:8.5px;color:{PALETTE['warn']}'>"
                   f"{_text(str(_v.get('gpt_note'))[:24])}</span>"
                   if _v.get("gpt") == "reject" and _v.get("gpt_note") else "")
                + (f"<br><span style='font-size:8.5px;color:{PALETTE['mute']}'>"
                   f"{_text(_v.get('level_before_verify'))}→{_text(g.get('level'))} 已降档</span>"
                   if _v.get("level_before_verify") and _v.get("level_before_verify") != g.get("level") else "")
            ))(g.get("verification") or {}), "max-width:150px")
            + _td((f"<span style='font-size:9.5px;color:{PALETTE['hold']};font-weight:700'>💼持仓</span>"
                   if g.get("held") else
                   f"<span style='font-size:9.5px;color:{PALETTE['wait']}'>👁"
                   f"{_text(g.get('scope') or '非持仓')}</span>")
                  + f"<br><span style='font-size:9px;color:#64748b'>"
                    f"引擎:{_text(d.get('action') or '—')}</span>", "max-width:120px")
            + "</tr>")

    out_rows = "".join(_out_row(g) for g in _board)
    _bs = _bd.get("by_scope") or {}
    _funnel = ("　".join(f"{_text(k)} {_text(v[0])}→<b>{_text(v[1])}</b>" for k, v in _bs.items())
               if _bs else "")

    near = ""
    if n3 == 0:
        cand = [r for r in rows if r.get("tier") == "2A" and (r.get("value_assessment") or {}).get("near_3a")]
        if cand:
            b = min(cand, key=lambda r: len(r.get("missing") or []))
            near = (f"　离3A最近: <b>{_text(b.get('name'))}</b> 差「"
                    f"{_text('、'.join(c.get('title','') for c in (b.get('value_assessment') or {}).get('remaining_conditions') or []))}」")
    cons = sg.get("consistency") or {}
    from weekly_candidates_ui import html as weekly_html
    from reverse_audit_ui import html as reverse_html
    from module_relations_ui import html as relations_html
    return (head + relations_html(relations,triad or {}) + reverse_html(reverse_audit, reverse_status, triad)
            + weekly_html(weekly,triad or {},watchlist=watchlist or {})
            + f"<div id='v88-grade-list' style='font-size:13px;font-weight:800;color:{PALETTE['buy']};margin:8px 0 3px;"
              f"border-left:4px solid {PALETTE['buy']};padding-left:6px'>"
              f"IN · 3A / 2A / 1A 重点列表</div>"
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
               if not triad_v2 and _cov and not _cov.get("publishable") else "")
            + (("" if triad_v2 else
                f"<div style='font-size:11.5px;background:{PALETTE['warn_bg']};"
                f"border-left:4px solid {PALETTE['warn']};border-radius:5px;"
                f"padding:5px 8px;margin-bottom:4px;color:{PALETTE['warn']}'>"
                f"⚠️ GPT-6与经典巨著审核尚未就绪：以下仅为旧口径参考，<b>不可冒充现买</b>。</div>"))
            # 行动表必须先于所有研究态：用户第一眼先看唯一可执行的 3A现买，
            # 再看准备/条件/分歧/研究/PENDING，避免研究名单在视觉上盖过买单。
            + ("" if persistent_html else _tbl(in_rows, _TH_IN) or
               (f"<div style='font-size:12px;color:#64748b'>{near}</div>" if near else ""))
            # ══ 被拦组:独立成组成表(用户"就像-3a里的非持仓组别一样") ══
            + persistent_html
            + tracking_html
            + fixed_archive
            + ("<details class='v88-entry-qualified' style='font-size:12px'><summary>进场条件关注子集（不改变当前等级与固定跟踪档案）</summary>" if persistent_html else "")
            + (f"<div style='font-size:12px;font-weight:500;color:#64748b;"
               f"margin:3px 0 5px'>"
               f"⏸ 价值分层与条件跟踪（{'分市场分档限额 · 原周期与执行限制保留' if focus else _research_scope}"
               + ("" if _blocked_rows else
                  "　<span style='font-weight:400'>当前没有非执行研究标的</span>")
               + "）</div>"
               + (_tbl(blocked_html, _TH_IN) if blocked_html else
                  f"<div style='font-size:12px;color:{PALETTE['hold']}'>"
                  f"今日无非执行研究标的</div>")
               if (triad_v2 or _blocked_rows or _n_cl_rej) else "")
            + focus_guide + history_html + guide
            + ("</details>" if persistent_html else "")
            + (f"<details class='v88-list-note' style='font-size:12px;color:{PALETTE['hold']};margin-bottom:3px'>"
               f"<summary>列表说明与退出记录</summary>"
               f"分数与进出场区间直接列示；触发条件、判断依据和审核详情点击展开。"
               + (f"　另有 <b>{_n_cl_rej}</b> 只被 V88规则闸判否，"
                  f"<b>已进入退出/待复核档案</b>——可在下方完整跟踪台查询，"
                  f"只留计数备查。" if _n_cl_rej else "")
               + "</details>" if (triad_v2 or _blocked_rows or _n_cl_rej) else "")
            # 铁律19 验证漏斗:被拦的必须点名,否则"我们还欠多少验证"看不见
            + (f"<details class='v88-review-detail' style='font-size:12px;color:#475569;margin:5px 0'>"
               f"<summary>双审评分与执行检查 · 详细记录</summary>"
               f"<div>3A=GPT-6五项＋书籍全通过＋本周期空间达标；2A等待成熟；1A价值研究；未审=待复核</div>"
               + (lambda _h: (
                   f"<div style='font-size:10.5px;margin-bottom:2px;color:"
                   f"{PALETTE['warn'] if (_h.get('last_error') or (_h.get('max_age_days') or 0) > 1) else PALETTE['hold']}'>"
                   f"⚙️ Codex状态：裁决{_h.get('verdicts', 0)}条"
                   f"（今日新鲜{_h.get('fresh_today', 0)}／最旧{_h.get('max_age_days')}天）"
                   + (f"　⚠️上轮报错：{_text(_h.get('last_error'))}" if _h.get("last_error") else "　上轮正常")
                   + (f"　不可用{_h.get('unavailable')}条" if _h.get("unavailable") else "")
                   + "　<span style='opacity:.85'>裁决比进程活得久：Codex 复核结束后结论仍生效，"
                     "24小时TTL＋判据变化即重判；跑不完时按「这一票能改变什么」排队"
                     "（3A候选＞持仓＞2A＞1A）。</span></div>") if _h else "")(
                   (_vf.get("gpt_health") or {}))
               + (lambda _kh: (
                   f"<div style='font-size:10.5px;margin-bottom:2px;color:#7c3aed'>"
                   f"📚 经典巨著：裁决{_kh.get('verdicts', 0)}条"
                   f"（时效内{_kh.get('fresh_today', 0)}｜书理通过{_kh.get('book_pass', 0)}）"
                   f"｜复核于 {_text(_kh.get('generated_at') or '—')}"
                   f"｜失败关闭：无记录=不可用≠通过</div>") if _kh else "")(_book_health())
               + f"<b>GPT-6与经典巨著独立审核；规则检查只决定执行许可</b>）：上榜 "
               f"<b style='color:{PALETTE['buy']}'>{_n_listable}</b>"
               + "".join(f"　<span style='color:{PALETTE['warn'] if '否决' in k else PALETTE['hold']}'>"
                         f"{_text(k)} {_text(v)}</span>"
                         for k, v in (_vf.get("stats") or {}).items() if v)
               + "</details>" if _vf else "")
            + (f"<details style='margin:8px 0'><summary>数据处理与审核队列（{len(_data_queue)}只，不占1A/2A/3A榜位）</summary>"
               "未完成项在下方完整档案逐只列明缺失字段、原始日期及下一步处理；曾确认的价值等级保留。"
               "本队列数量不代表市场没有机会。</details>" if triad_v2 and _data_queue else "")
            # 【三方定纲·可见性】被 OUT 证据撤销买点的票若不在上表(如1A),必须单独点名——
            # 仲裁若不可见,等于没发生。GPT:"IN候选结构保留,当前买点撤销"要两句都说出来。
            + (f"<div style='font-size:11px;background:#fef2f2;border-left:3px solid {PALETTE['sell']};"
               f"border-radius:4px;padding:4px 8px;margin:4px 0'>"
               f"⛔ <b>本次买点被 OUT 证据撤销 {len(_revoked)} 只</b>："
               + "、".join(f"{_text(_nm)}<span style='color:#94a3b8'>({_text(_t)})</span>"
                           for _nm, _t in _revoked)
               + "　<span style='color:#64748b'>买入结构可继续观察，但<b>现在不能买</b>"
                 "（解除需:缩量止跌／收复MA20／结构重新确认）。"
                 "两侧计算互盲、都不删——删一侧就成了用结果消音证据。</span></div>"
               if _revoked else "")
            # ══ 第一区：我的持仓（用户定纲"out首先关注的是我的持仓,其次是其他"）══
            # 【用户定纲】"-3A系统也要有双剑认证系统,没有双剑认证怎么可以获得-3A立即卖出"
            # 声明必须常驻(不只在有降档时出现)——它是这条榜的准入契约,读者要能随时看到。
            + (lambda _dc: (
                f"<details style='font-size:12px;color:#475569;margin:8px 0 2px'>"
                f"<summary>卖侧审核记录 · 记录时间 {_short_ts(sg.get('generated_at'))}</summary>"
                f"预承诺止损继续有效；模型判断型卖出需当期卖侧审核。"
                f"　原批次候选 <b>{_dc.get('candidates', 0)}</b> 只 → 原批次审核通过 "
                f"<b style='color:{PALETTE['sell'] if _dc.get('passed') else PALETTE['hold']}'>"
                f"{_dc.get('passed', 0)}</b> 只"
                + "".join(f"<br><span style='font-size:10.5px;color:{PALETTE['warn']}'>"
                          f"· {audit_text(x.get('name'))}：{legacy_review_text(x.get('note'))}</span>"
                          for x in (_dc.get("detail") or [])[:4])
                + "</details>") if _dc else "")((sg.get("sell_verification") or {}).get("dual_cert"))
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
               f"✅ <b>{_text(str(_quiet.get('headline')).replace('**', ''))}</b>"
               f"<span style='color:#64748b'>　（"
               + _text(str(_quiet.get('wording_rule') or '').replace('**', ''))
               + "）</span><br>"
               + "　".join(
                   f"<span style='display:inline-block;margin:1px 0'>"
                   f"{stock_link(r.get('name'), r.get('code'))}"
                   f"<span style='color:#94a3b8;font-size:10px'>"
                   f"[{_text(r.get('nearest_risk'))}]</span></span>"
                   for r in (_quiet.get("rows") or []) if not r.get("vetoed"))
               # 被验证压掉的必须与"真没信号"分开列——混在一起就是撒谎:
               # 它们触发了,只是裁决没放行。同"无信号≠安全"的措辞铁律。
               + "".join(
                   f"<div style='font-size:10.5px;color:{PALETTE['warn']};margin-top:2px'>"
                   f"⏸ <b>{stock_link(r.get('name'), r.get('code'))}</b>：{_text(r.get('status'))}</div>"
                   for r in (_quiet.get("rows") or []) if r.get("vetoed"))
               + "</div>" if _quiet.get("rows") else "")
            # ══ 第二区：非持仓 ══
            + f"<details style='margin-top:10px'><summary style='cursor:pointer;font-size:13px;"
              f"font-weight:800;color:#b45309;border-left:4px solid #b45309;padding-left:6px'>"
              f"👁 第二区 · 非持仓回避（{len(_oth)}只，默认折叠）</summary>"
            + (f"<div style='font-size:11px;color:#64748b;margin-bottom:3px'>"
               f"漏斗 信号{_bd.get('signals')} → 入榜<b>{_bd.get('on_board')}</b>："
               f"{_funnel}　<span style='color:#94a3b8'>入榜闸=接近度×持有档"
               f"（💼持仓{_text(_bd.get('gates', {}).get('持仓', ''))}最松／👁自选"
               f"{_text(_bd.get('gates', {}).get('自选', ''))}／🔍候选"
               f"{_text(_bd.get('gates', {}).get('池内候选', ''))}最严）。"
               f"排序口径未改（仍按接近触发度）——分区只决定<b>你先看到什么</b>，"
               f"不给持仓伪造风险分。未入榜=<b>还没到</b>，不等于安全。</span></div>" if _bd else "")
            + (_tbl("".join(_out_row(g) for g in _oth), _TH_OUT)
               or f"<div style='font-size:12px;color:{PALETTE['hold']}'>非持仓标的无回避信号 —— "
                                 "无 OUT 信号也是信号</div>")
            + "</details>"
            + f"<div style='font-size:10.5px;color:#94a3b8;margin-top:4px'>"
              f"OUT为影子级(攒战绩不触发交易)·与引擎卖警一致率{cons.get('rate', '—')}%"
              f"({cons.get('shadow_agrees', '—')}/{cons.get('engine_sell_calls', '—')})"
              f"·08-14起与sell_call核算对照后转正"
            + (f"　🗄已否决{len(arch)}只(不占推荐位)" if arch else "") + "</div>")


def verdict_html(v: dict) -> str:
    """【单票 ±3A 裁决卡】2026-08-02 用户:"个股搜索的深度分析也要和3a系统一样进行买卖分析,
    出来的结果看符合-3a还是+3a,都要有双剑合璧系统的验证"。
    数据来自 ai-daily-report-v2/src/stock_verdict.verdict()——**与 3A 大系统同源**。"""
    if not v:
        return ""
    cl = v.get("rules_gate")
    gp = v.get("codex") or v.get("gpt")

    def _badge(who, st):
        c = (PALETTE["buy"] if st in ("pass", "gate_pass") else
             PALETTE["warn"] if st == "reject" else PALETTE["mute"])
        m = "✅通过" if st == "pass" else "✓规则达标" if st == "gate_pass" else "⚠️否决" if st == "reject" else "—未表态"
        return (f"<span style='background:{c};color:#fff;border-radius:4px;"
                f"padding:1px 7px;font-size:11px;font-weight:700'>{_text(who)} {m}</span>")

    def _side(d, is_buy):
        if not d:
            return ""
        fin = d.get("final")
        col = (PALETTE["buy"] if is_buy else PALETTE["sell"]) if fin else PALETTE["mute"]
        raw = d.get("bucket_tier") if is_buy else d.get("raw_level")
        trail = (f"<span style='color:{PALETTE['mute']};font-size:10.5px'>"
                 f"　原判 {_text(raw)} → 验证上限 {_text(d.get('cap') or '—')} → "
                 f"<b style='color:{col}'>最终 {_text(fin or '不成立')}</b></span>")
        body = []
        if is_buy:
            if d.get("zone") or d.get("when"):
                body.append(f"买入区间/时机：<b>{_text(d.get('zone') or d.get('when'))}</b>")
            if d.get("buckets"):
                body.append("五桶板：" + " ".join(
                    f"{_text(BUCKET_CN.get(k, k)[:2])}{round(x or 0)}" for k, x in d["buckets"].items()))
            if d.get("missing"):
                body.append(f"短板：{_text('、'.join(d['missing']))}")
        else:
            if d.get("zone"):
                body.append(f"卖出区间：<b>{_text(d.get('zone'))}</b>")
            body.append(f"1-2-3：<b>{_text(d.get('c123'))}</b>"
                        + (f"　旁路：{_text('；'.join(d.get('bypass') or []))}" if d.get("bypass") else "")
                        + (f"　止损 {_text(d.get('stop'))}（距 {_text(d.get('dist_stop_pct'))}%）"
                           if d.get("stop") else ""))
        return (f"<div style='border-left:3px solid {col};padding:4px 9px;margin:4px 0;"
                f"background:#f8fafc;border-radius:0 5px 5px 0'>"
                f"<b style='color:{col};font-size:12.5px'>"
                f"{'🟢 买侧' if is_buy else '🔴 卖侧'} {_text(fin or '不成立')}</b>{trail}"
                + "".join(f"<div style='font-size:11px;color:#334155;margin-top:2px'>{x}</div>"
                          for x in body)
                + (f"<div style='font-size:10.5px;color:{PALETTE['warn']};margin-top:2px'>"
                   f"⚔️ {_text(d.get('note'))}</div>" if d.get("note") else "")
                + "</div>")

    return (f"<div style='border:1px solid #e2e8f0;border-radius:7px;padding:8px 11px;"
            f"margin:6px 0;background:#fff'>"
            f"<div style='display:flex;align-items:baseline;gap:8px;flex-wrap:wrap'>"
            f"<b style='font-size:14px'>{_text(v.get('name'))}</b>"
            f"<span style='font-size:11px;color:{PALETTE['mute']}'>{_text(v.get('code'))}</span>"
            f"{_badge('V88规则闸', cl)}{_badge('GPT/Codex', gp)}"
            f"<span style='font-size:10.5px;color:{PALETTE['mute']}'>{_text(v.get('source'))}</span></div>"
            f"<div style='font-size:12.5px;font-weight:700;margin:4px 0;color:#1e293b'>"
            f"{_text(str(v.get('headline')).replace('**', ''))}</div>"
            + (f"<div style='font-size:10.5px;color:"
               f"{PALETTE['warn'] if gp == 'reject' else '#0f766e'};margin-bottom:2px'>"
               f"GPT/Codex复核：<b>{'通过' if gp == 'pass' else '不否定' if gp == '不否定' else '否决' if gp == 'reject' else '待复核'}</b>"
               + (f"｜{_text(v.get('gpt_note'))}" if v.get('gpt_note') else "")
               + (f"｜分析于 {_text(v.get('gpt_at'))}" if v.get('gpt_at') else "")
               + (f"｜{_text(v.get('gpt_model'))}" if v.get('gpt_model') else "")
               + "</div>")
            + gpt_html(v.get("scorecard") or {}) + books_html(v.get("scorecard") or {})
            + reasons_html(v) + _side(v.get("buy"), True) + _side(v.get("sell"), False)
            + f"<div style='font-size:10px;color:{PALETTE['mute']};margin-top:3px'>"
              f"{_text(v.get('rule'))}　｜　3A等级须GPT-6评分及适用书理全通过，执行再核对规则和触发；"
              f"GPT-6/经典巨著未复核或仅不否定时自动降档。</div></div>")


_CERT_CACHE = {"ts": 0, "map": {}}


def cert_map(rank_score: dict | None = None) -> dict:
    """{code: '双'|'单'|'R'|''} —— 作战板用的极简会审口径。
    用户定纲 2026-08-02:"这里面不用去除,这里面只能有两者认证和c认证的两种即可"。
    即:作战板**不做删减**(与推荐位不同),只给每条挂一个认证标记,让人自己掂量。"""
    # 徽章覆盖面必须与验证面一致:黑马票在 watch 段
    rows = ((rank_score or {}).get("rows") or []) + ((rank_score or {}).get("watch") or [])
    m = {}
    for r in rows:
        v = r.get("verification") or {}
        c = v.get("rules_gate")
        g = v.get("codex") or v.get("gpt")
        if c in ("pass", "gate_pass") and g == "pass":
            m[str(r.get("code"))] = "审"
        elif c in ("pass", "gate_pass") and g == "不否定":
            m[str(r.get("code"))] = "缓"
        elif c in ("pass", "gate_pass"):
            m[str(r.get("code"))] = "R"
    return m


def cert_badge(code: str, cmap: dict) -> str:
    """审=规则闸+GPT通过；缓=GPT不否定；R=仅规则闸。"""
    k = cert_map_key(code)
    t = cmap.get(str(code)) or cmap.get(k) or ""
    if t == "审":
        return cg_badge("R", "gate_pass") + cg_badge("G", "pass")
    if t == "缓":
        return cg_badge("R", "gate_pass") + cg_badge("G", "不否定")
    if t == "R":
        return cg_badge("R", "gate_pass") + cg_badge("G", "")
    return (f"<span style='color:{PALETTE['mute']};font-size:9.5px;margin-left:3px'>·待验</span>")


def cert_map_key(code: str) -> str:
    c = str(code or "").upper()
    if c.endswith(".HK"):
        return (c.split(".")[0].lstrip("0") or "0").zfill(5) + ".HK"
    return c
