"""Complete local watch/exit ledger, independent of the daily top-N table."""
from copy import deepcopy
from collections import Counter
from display_limits import market_top

BUCKETS = ("blocked_3a", "recommendations", "preparations", "conditional", "observations", "pending", "excluded")
TIER_LABEL = {"PENDING": "待补证·不评级", "0A": "不予推荐"}
EVENTS = {"ADDED": "首次建档", "PROMOTED": "升级", "DEMOTED": "降级", "UPDATED": "条件/分数更新",
          "REVIEW_STATUS_CHANGED": "审核状态变化", "RETAINED_OUTSIDE_POOL": "扫描落选·继续跟踪",
          "RETURNED_TO_POOL": "重新进入扫描池", "EXITED": "价值失效·进入退出档案",
          "REENTERED": "新证据确认·重新入选", "HORIZON_CHANGED": "审核周期变化"}


def entries(selection, registry, now=None):
    from review_display import current_scorecard
    from investment_maturity import assess
    central = {}
    for bucket in BUCKETS:
        for row in selection.get(bucket) or []:
            central.setdefault(row.get("canonical_code") or row.get("code"), row)
    result = []
    for code in sorted(set(central) | set((registry.get("tracks") or {}))):
        track = (registry.get("tracks") or {}).get(code) or {}
        row = deepcopy(central.get(code) or track.get("latest_snapshot") or {})
        row.setdefault("code", code)
        row["tracking"] = {**track, **(row.get("tracking") or {})}
        card = current_scorecard(selection, row, now=now)
        value = assess(card, row.get("horizon"), row.get("trade_plan") or {})
        # Historical entries never inherit a new pack's lease.
        current_tier = value["tier"] if code in central else "PENDING"
        if track.get("lifecycle") == "EXITED":
            current_tier = "退出档案"
        row.update(scorecard=card, value_assessment=value, current_tier=current_tier,
                   central_reviews=row.get("reviews") or {})
        result.append(row)
    return sorted(result, key=lambda r: ({"3A": 0, "2A": 1, "1A": 2, "PENDING": 3, "0A": 4, "退出档案": 5}.get(r["current_tier"], 9), str(r.get("code"))))


def render_tracking(selection, registry, journal=None):
    import streamlit as st
    from grade_card import stock_link
    from scorecard_html import gpt_html, books_html, reasons_html, score_label, text, profit_html
    rows = entries(selection, registry)
    counts = Counter(r["current_tier"] for r in rows)
    st.markdown("**1A / 2A / 3A 持续跟踪台 · 重点与档案检索**")
    st.caption(f"当前 3A {counts['3A']} · 2A {counts['2A']} · 1A {counts['1A']} · 待复核 {counts['PENDING']} · 档案总数 {len(rows)}。"
               "首次出现、历次等级及原因持续保留；数据或审核过期保留最近确认等级。1A/2A均不可直接执行。")
    contracts = (journal or {}).get('contracts') or []
    with st.expander(f"月度原始研究合同 · {len(contracts)}份 · 原区间与期限不追改", expanded=False):
        st.caption("按首次建档月份留存1A/2A/3A；没有实际入场证据，后来触及止盈位也不计盈利。升降级不改写原始预测。")
        if contracts:
            month = st.selectbox("研究月份", ["全部"] + sorted({c['cohort_month'] for c in contracts}, reverse=True), key="v88_idea_month")
            state_names={'WATCHING':'研究跟踪中','EXPIRED_RESEARCH':'原期限届满','INVALIDATED_RESEARCH':'原失效线已触达'}
            horizon_names={'short':'短期','medium':'中期','long':'长期'}
            st.dataframe([{'名称':c.get('name') or c['code'],'代码':c['code'],'月份':c['cohort_month'],
                '周期':horizon_names.get(c['horizon'],c['horizon']),'原等级':c['original_tier'],'原审核分':c.get('original_score'),
                '原进场区间':str(c['original_plan'].get('entry_range')),
                '原止盈区间':str(c['original_plan'].get('take_profit_range')),
                '原失效价':c['original_plan'].get('stop'),'原截止日':c['original_deadline'],
                '研究状态':state_names.get(c['status'],c['status']),'原审核编号':c.get('original_audit_id')}
                for c in contracts if month=='全部' or c['cohort_month']==month], hide_index=True, use_container_width=True)
        else:
            st.info("尚无符合本版量化与审核要求的研究合同。")
    with st.expander("检索股票、时间/价格条件和等级沿革", expanded=False):
        tier = st.selectbox("当前档位", ["全部", "3A", "2A", "1A", "PENDING", "0A", "退出档案"], format_func=lambda v:TIER_LABEL.get(v,v), key="v88_track_tier")
        market = st.selectbox("市场", ["全部"] + sorted({str(r.get("market") or "未识别") for r in rows}), key="v88_track_market")
        query = st.text_input("按代码或名称查找", key="v88_track_search").strip().casefold()
        visible = [r for r in rows if (tier == "全部" or r["current_tier"] == tier)
                   and (market == "全部" or str(r.get("market") or "未识别") == market)
                   and (not query or query in (str(r.get("code")) + str(r.get("name"))).casefold())]
        matched_count = len(visible)
        visible = market_top(visible)
        st.caption(f"匹配 {matched_count}只，展示 {len(visible)}只 · 每市场Top5，合计至多15只。输入名称或完整代码可查其他档案；个股沿革完整保留。")
        if not visible:
            st.info("本档位当前没有满足条件的股票。缺审核或证据不足的股票仍在待复核档案中，不会补位为1A。")
            return
        table = []
        for r in visible:
            tr = r.get("tracking") or {}
            table.append({"名称": r.get("name") or r["code"], "代码": r["code"], "市场": r.get("market"),
                          "当前档位": TIER_LABEL.get(r["current_tier"],r["current_tier"]), "审核分": score_label(r["scorecard"]),
                          "最近确认价值等级": tr.get("last_value_tier") or "尚无",
                          "首次出现": tr.get("first_seen"), "上次入池": tr.get("last_in_pool"),
                          "等待/变化原因": (r.get("value_assessment") or {}).get("stage_note")})
        st.dataframe(table, hide_index=True, use_container_width=True)
        selected = st.selectbox("选择个股查看完整档案", list(range(len(visible))),
                                format_func=lambda i: f"{visible[i].get('name') or ''} {visible[i]['code']} · {TIER_LABEL.get(visible[i]['current_tier'],visible[i]['current_tier'])}", key="v88_track_stock")
        row = visible[selected]
        st.markdown(stock_link(row.get("name") or row["code"], row["code"]) + f" · <b>{text(TIER_LABEL.get(row['current_tier'],row['current_tier']))} · {score_label(row['scorecard'])}</b>", unsafe_allow_html=True)
        st.markdown(profit_html(row) + gpt_html(row["scorecard"]) + books_html(row["scorecard"]) + reasons_html(row), unsafe_allow_html=True)
        events = (row.get("tracking") or {}).get("events") or []
        st.dataframe([{"时间": e.get("at"), "变化": EVENTS.get(e.get("kind"), e.get("kind")),
                       "原等级": e.get("from"), "新等级": e.get("to"), "分数": e.get("score"),
                       "原因": e.get("reason"), "原条件": str(e.get("previous_conditions") or {}), "新条件": str(e.get("conditions") or {}), "审核编号": e.get("audit_id")} for e in reversed(events)],
                     hide_index=True, use_container_width=True)
