"""Human-readable projections of the sole central 3A scorecard."""
from html import escape


def text(value, missing="待补证"):
    return escape(str(value if value is not None and value != "" else missing))


def score_label(card):
    total = card.get("total")
    if total is None:
        known, coverage = card.get("known_contribution"), card.get("coverage_pct")
        if type(known) in (int, float) and type(coverage) in (int, float) and coverage > 0:
            return f"审核待补 · 已核贡献 {known:g}分 · 覆盖{coverage:g}%"
        return "加权审核分：待评分"
    return f"加权审核分 {total:g}/100" if card.get("score_policy") else f"原审核分 {total:g}/100"


def gpt_html(card):
    g = card.get("gpt") or {}
    prefix = "GPT‑6 加权" if card.get("score_policy") else "原GPT‑6"
    title = f"{prefix} {g['total']:g}/100" if g.get("total") is not None else f"GPT‑6 已评分 {g.get('known', 0)}/5"
    if not g.get("current"):
        title += " · 待新审"
    values = {e.get("field"): e.get("value") for e in g.get("evidence") or []}
    rows = []
    for c in g.get("criteria") or []:
        score = "待补证" if c.get("score") is None else str(c["score"]) + "/20"
        refs = "、".join(f"{f}={values.get(f)}" for f in c.get("evidence_fields") or []) or "未提供"
        weight, contribution = c.get("weight_pct"), c.get("contribution")
        parts = f"权重 {weight:g}% · 贡献 {contribution:g}分" if weight is not None and contribution is not None else f"权重 {weight:g}% · 待核" if weight is not None else "原审核记录"
        rows.append(f"<div style='margin:8px 0;padding:8px;border-left:3px solid #60a5fa;background:#f8fafc'><b>{text(c.get('title'))}：{score}</b>"
                    f"<br><span style='color:#1d4ed8'>{text(parts)}</span><br>"
                    f"标准：{text(c.get('requirement'))}<br>依据：{text(c.get('reason'))}<br>字段：{text(refs)}</div>")
    pair_html = ""
    from review_scorecard import gpt_result
    for role, review in (card.get("review_pair") or {}).items():
        view = gpt_result(review)
        label = "主审" if role == "primary" else "独立反审"
        score = view.get("total")
        detail = "<br>".join(f"{text(c.get('title'))} {text(c.get('score'))}/20：{text(c.get('reason'))}" for c in view.get("criteria") or [])
        pair_html += f"<details><summary>{label}：{text(score)}分 · {text(review.get('thesis_verdict'))}</summary>{detail}</details>"
    return f"<details><summary><b>{title}</b> · 五项权重与交叉审核</summary>" + policy_html(card) + "".join(rows or ["等待GPT‑6按新版五项评分表审核"]) + pair_html + "</details>"


def books_html(card):
    b = card.get("books") or {}
    title = f"书理 {b['total']:g}分 · {b.get('pass_n', 0)}/{b.get('required', 0)}项通过" if b.get('total') is not None else f"书理待核 · {b.get('pass_n', 0)}/{b.get('required', 0)}项通过"
    if not b.get("current"):
        title += " · 待新审"
    from classics_framework import html as framework_html
    lines = [framework_html(card)]
    for c in b.get("checks") or []:
        status = "通过" if c.get("ok") is True else "未通过" if c.get("ok") is False else "待补证"
        weight, contribution = c.get('weight_pct'), c.get('contribution')
        detail = f"权重 {weight:g}% · 贡献 {contribution:g}分" if weight is not None and contribution is not None else f"权重 {weight:g}% · 待核" if weight is not None else "原审核记录"
        if c.get('ok') is False and weight is not None:
            detail += f" · 扣 {weight:g}分"
        color = '#166534' if c.get('ok') is True else '#be123c' if c.get('ok') is False else '#64748b'
        lines.append(f"<div style='margin:8px 0;padding:8px;border-left:3px solid {color}'><b>{text(c.get('label'))}：{status}</b><br>"
                     f"<span style='color:{color}'>{text(detail)}</span><br>{text(c.get('book'))}<br>标准：{text(c.get('threshold'))}<br>证据：{text(c.get('detail'))}</div>")
    lines += [f"<div>{text(a.get('book'))}：{'适用' if a.get('applicable') else '不适用'}；{text(a.get('reason'))}</div>"
              for a in b.get("applicability") or []]
    return f"<details><summary><b>{title}</b> · 展开书理</summary>" + "".join(lines or ["等待同事实、同周期的书籍核验"]) + "</details>"



def policy_html(card):
    """Explain the bound central calculation; never compute a second UI score."""
    policy = card.get('score_policy')
    if not policy:
        return "<small>原政策历史分，等待同源加权重算。</small>"
    label = policy.get('version', '') if isinstance(policy, dict) else str(policy)
    g, b = card.get('gpt') or {}, card.get('books') or {}
    parts = []
    for name, component in (("GPT", g), ("书理", b)):
        known, coverage = component.get('known_contribution'), component.get('coverage_pct')
        if known is not None and coverage is not None:
            parts.append(f"{name}已核贡献 {known:g}分 / 覆盖 {coverage:g}%")
    prior = card.get('legacy_total')
    if prior is None and g.get('legacy_total') is not None and b.get('legacy_total') is not None:
        prior = min(g['legacy_total'], b['legacy_total'])
    original = f"<br>同一原子证据在旧等权政策下为 {prior:g}分；仅供追溯。" if prior is not None else ""
    return ("<div style='font-size:12px;line-height:1.5;margin:6px 0;color:#475569'>"
            "⚖️ 不等权复合：逻辑25% · 风险25% · 事实20% · 反证20% · 周期10%。"
            "书理按持有周期赋权；取两条证据链较低分，关键条件失败不能靠其他高分抵消。"
            + ("<br>" + text(" · ".join(parts)) if parts else "") + original
            + "<br>策略版本：" + text(label) + " · 权重为V88待校准参数。</div>")


def reasons_html(row):
    card = row.get("scorecard") or {}
    g = (row.get("central_reviews") or {}).get("gpt") or {}
    reasons = list(card.get("missing") or [])
    reasons += [c.get("label", "") + "未达标：" + c.get("detail", "")
                for c in (card.get("books") or {}).get("checks") or [] if c.get("ok") is False]
    reasons += [c.get("title", "") + "：" + str(c.get("score")) + "/20；" + c.get("reason", "")
                for c in (card.get("gpt") or {}).get("criteria") or [] if c.get("score") is not None and c["score"] < 15]
    summary = text(g.get("why"), "尚无当前GPT‑6结论")
    return (f"<details><summary>判断依据</summary><div>{summary}</div></details><details><summary>未达3A / 下一步</summary>"
            + "<br>".join(text(v) for v in reasons or ["双审全部通过；执行须另查触发、风险和数据时效"])
            + f"<br>反证：{text(g.get('counterargument'))}<br>失效：{text(g.get('invalidation'))}"
            + f"<br>审核编号：{text(card.get('audit_id'))}</details>" + maturity_html(row))


def rubric_html():
    return """<details class='v88-rubric' style='font-size:12px;line-height:1.5;background:#f8fafc;border:1px solid #e2e8f0;border-radius:5px;padding:6px 10px;margin:6px 0'>
<summary style='cursor:pointer;font-size:12px;color:#475569'>评级规则 · 3A / 2A / 1A、周期目标与经典书理</summary>
<div style='padding-top:8px'>
<b>原合同核价口径 · 审核前锁定，不随研究窗口延长</b><br>
榜单研究窗口：短期0–8周、中期8–24周、长期12–36周。下列自然日期限用于原合同核价；实际入场、持有与退出期限以每只股票原合同为准。<br>
原短期合同≤30自然日：1A净空间≥5%、2A≥8%、3A≥10%。<br>
原中期合同31–90自然日：1A≥10%、2A≥15%、3A≥20%。<br>
原长期合同91–365自然日：1A≥20%、2A≥30%、3A≥40%；一年≥50%另标高空间。<br>
<b>1A：</b>有依据的价值研究；GPT主审与反审均完整，事实/逻辑/风险各≥15，反证/周期各≥10，审核分≥60；书籍价值基础、资本保护、周期幅度和净空间都通过。允许列明非致命争议，不能当成全部通过。<br>
<b>2A：</b>GPT双审五项各≥15、审核分≥75，至多一项量价成熟条件未达，且本周期2A空间达标。<br>
<b>3A：</b>GPT双审与适用书籍100%通过、审核分≥75、本周期3A空间达标、净收益风险比≥2。准备/冻结与现在可执行分开；只有3A现买获当前执行许可。<br>
<b>加权审核分=min(GPT较保守完整裁决加权分，适用书理加权分)</b>。GPT权重：事实20%、逻辑25%、反证20%、周期10%、风险25%；每项原分0/10/15/20，以原分÷20×权重计贡献。15须充分证据，20须至少两字段交叉印证。短期8项、中期10项、长期9项书理按周期分别赋权；失败扣除该项权重，关键风险仍可否决。缺项不补中性分、不重分配权重。数值是待检验的V88参数；分数不是上涨概率。<br>
<b>净空间=[止盈下沿×0.995÷(进场上沿×1.005)−1]×100%</b>；所有档位净收益风险比至少1.5。买卖各0.5%为手续费与滑点假设，未计个人税费、汇率、股息；不是券商费率。用未四舍五入值过闸。<br>
<b>核心书与多书印证：</b>斯波朗迪《专业投机原理》是重点参考之一；经典按趋势、企业估值、资本风险和实证验证四类分工。更多独立证据用于交叉检验，更多书名不自动提高准确率。冲突列依据和缺口，不按书的数量投票，不把工程阈值当成原著定律。<br>
<b>萨普补充：</b>三个周期均纳入范 K·萨普《通向财务自由之路》（Trade Your Way to Financial Freedom），与斯波朗迪《专业投机原理》分开。初始净R、仓位及退出必须明确；同策略扣费R期望须单独验证。历史不足20笔或描述性保守下界不大于0，不能声称正期望，3A不通过该项。样本充分且净期望非正，不授价值等级。0.5%净值风险预算、10%单票上限为V88研究参数；跳空、停牌及成交限制可能扩大损失。<br>
<b>经典约束：</b>短期依据欧奈尔量价、斯波朗迪资本保护、Edwards与Magee趋势；中期另核林奇的盈利与现金流；长期核盈利、质量、估值、长期历史与逻辑失效。上述百分比为V88参数，不冒称原书统一标准。<br>
<b>目标先有依据：</b>短期止盈带取近20个完整交易日最高与次高日高点；当前短期模板触发后10交易日且本期至多30自然日。中长期要盈利估值和现金流收益率两种推演及原始文档，取较低区间；不得用短线高点冒充一年目标。历史幅度筛选不是胜率。<br>
<b>闭环：</b>全市场发现→同周期事实→GPT双审＋书理→分数与区间→触发/失效/期限→保留升降级与原始合同→前瞻复盘。等待不删除；降级列原因；到期不自动续期；旧目标不追改。缺审不能算1A。
</div></details>"""


def profit_html(row):
    from profit_contract import evaluate
    plan = row.get("central_trade_plan") or row.get("trade_plan") or {}
    inputs = plan.get("profit_inputs") or {}
    result = evaluate(plan, inputs.get("horizon") or plan.get("horizon"))
    zone = result.get("take_profit_range") or []
    if not result["valid"]:
        return "区间尚未验证 · 不推荐<br>" + text("；".join(result["reasons"]))
    lo, hi = zone
    net = result["net_upside_pct"]
    gross = result["gross_upside_pct"]
    holding = (str(result["holding_sessions"])+"交易日；") if result.get("holding_sessions") else ""
    return (f"<b>{lo:g}～{hi:g}</b><br><b>净空间 {net:.2f}%</b> · 毛空间 {gross:.2f}%"
            f"<br>{holding}本期截止 {text(result['thesis_deadline'][:10])}"
            f"<br>净收益风险比 {result['net_reward_risk']:.2f}"
            + ("<br><b>不予推荐："+text("；".join(result["reasons"]))+"</b>" if not result["eligible"] else "")
            + "<details><summary>目标依据与费用假设</summary>"+text(result["target_basis"])
            + "<br>"+text(result["formula"])+"<br>"+text(result["cost_assumption"])
            + "<br>"+text(result["exit_rule"])+"</details>")


def maturity_html(row):
    value, plan, track = (row.get(k) or {} for k in ("value_assessment", "watch_plan", "tracking"))
    if not any((value, plan, track)):
        return ""
    lines = [f"价值状态：{text(value.get('value_status'))}；{text(value.get('stage_note'))}"]
    lines += [f"{text(c.get('title'))}：{text(c.get('condition'))}；当前：{text(c.get('observed'))}" for c in value.get('remaining_conditions') or []]
    lines += [f"价格观察上限：{text(plan.get('price_watch_ceiling'), '尚不能计算')}（需要重新核验触发及失效价）",
              f"原触发：{text(plan.get('entry_condition'))}；失效：{text(plan.get('invalidation'))}",
              f"下次复核期限：{text(plan.get('next_review_due'))}",
              f"首次出现：{text(track.get('first_seen'))}；最近确认价值等级：{text(track.get('last_value_tier'), '尚无')}；状态：{text(track.get('lifecycle'))}"]
    return "<details><summary>价值、时间/价格条件与持续跟踪</summary>" + "<br>".join(lines) + "</details>"
