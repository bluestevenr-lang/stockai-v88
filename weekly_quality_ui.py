"""A folded diagnostics panel below the unchanged IN/OUT lists."""
def render(doc,database):
    import streamlit as st
    if not doc:return
    with st.expander('每周3A检查 · 数据覆盖与漏选排查',expanded=False):
        f=doc.get('funnel') or {}
        st.caption(f"本版本已观察 {doc.get('observation_days',0):g} 天；近7天出现合格3A {doc.get('quality_3a_seen',0)}只，"
                   f"其中曾具备执行许可 {doc.get('executable_3a_seen',0)}只。每周至少发现1只为研究目标，不能据此降低授级标准。")
        st.write(' → '.join([f"报价 {f.get('quote_records',0)}",f"报价初筛 {f.get('quote_screen_eligible',0)}",
                            f"当前日线事实 {f.get('fresh_history_facts',0)}",f"当前GPT审核 {f.get('gpt_current',0)}"]))
        if doc.get('problems'):st.warning('；'.join(doc['problems']))
        if doc.get('zero_week_requires_investigation'):st.warning('连续7天无3A：必须复查覆盖、数据时效、被过滤原因和兑现路径，保留排查记录。')
        else:st.caption('不足7天不冒充完整周统计；已有数据缺口立即排查。')
        st.write({'书理未过项计数':doc.get('book_failed_counts'),'GPT保留项计数':doc.get('gpt_reservation_counts')})
        v=database.get('daily_verified') or {};old=database.get('daily') or {}
        st.caption(f"同源库 {v.get('securities',0)}个证券、{v.get('rows',0)}条日线，几何无效 {v.get('invalid_rows','未验')}条；"
                   f"旧库无效 {old.get('invalid_rows','未验')}条，含无效记录的分析区间隔离，原始历史保留。")
