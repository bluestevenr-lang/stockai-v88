"""Public directory search, independent of grades and execution."""
from pathlib import Path
import pandas as pd
from urllib.parse import urlencode
import sqlite3

def research_index_report(core):
    if not (Path(core)/'data/full_market_research.sqlite').exists():
        return None
    from full_market_research import plan
    return plan(base=core, limit=0)[0]

def research_index_caption(report):
    counts = report['status_counts']
    return (f"全目录研究索引：{report['directory_total']:,}只；已有事实进入当前审核池"
            f"{counts.get('in_active_review_lane',0):,}只，等待分批接入{counts.get('queued_for_admission',0):,}只，"
            f"来源过期{counts.get('source_expired',0):,}只。缺历史{counts.get('missing_history',0):,}只，"
            f"事实条件未过{counts.get('fact_rejected',0):,}只。未审不授级，分批数量不是全市场分母。")

def render(st, core):
    from display_limits import market_top
    from market_directory_search import search
    from market_symbols import canonical
    def keep_results_open():
        st.session_state['v88_directory_open']=True
    with st.expander('🔎 全市场证券目录检索 · 可查评级池外证券', expanded=st.session_state.get('v88_directory_open',False)):
        result=search(base=core,limit=1)
        st.caption(f"已下载目录共 {result['directory_total']:,} 个证券代码。"+result['scope'])
        research = None
        try:
            research = research_index_report(core)
            if research:st.caption(research_index_caption(research))
        except (ValueError, OSError, sqlite3.Error) as exc:
            st.warning('全目录研究索引暂不可用：'+str(exc)[:160])
        if result['missing_directories']:
            st.warning('缺少目录：'+ '、'.join(result['missing_directories']))
        with st.form('v88_directory_search_form'):
            market=st.selectbox('目录市场',['全部','A股','B股','港股','美股'],key='v88_directory_market')
            query=st.text_input('证券名称或代码',key='v88_directory_query',placeholder='例如：紫金矿业 / 601899.SH / 00700.HK')
            submit=st.form_submit_button('搜索全部已下载目录',on_click=keep_results_open)
        if submit:
            st.session_state['v88_directory_result']=search(query,market,base=core)
        found=st.session_state.get('v88_directory_result')
        if found is None:return
        st.caption(f"共匹配 {found['match_count']:,} 条；每市场显示前5只、合计至多15只。输入更完整的名称或代码可检索其他证券。")
        states={r['code']:r['status'] for r in (research or {}).get('members',[])}
        labels={'in_active_review_lane':'当前审核池（不等于已审）','queued_for_admission':'待分批接入审核','source_expired':'来源过期·待更新','missing_history':'缺日线资料','fact_rejected':'事实条件未过'}
        records=[{'名称':r['name'],'代码':r['code'],'市场':r['market'],'证券类型':r['security_type'],
                  '目录日期':r['catalog_asof'],'日期校验':r['time_status'],
                  '研究状态':labels.get(states.get(canonical(r['code'])), '当前研究索引未覆盖'),
                  '来源':' | '.join(str(s) for s in r['sources']),
                  '详情':'/?'+urlencode({'q':r['code'],'focus':'deep'})} for r in market_top(found['rows'])]
        if records:
            frame=pd.DataFrame(records)
            st.dataframe(frame,hide_index=True,column_config={'详情':st.column_config.LinkColumn('详情',display_text='查看')})
        st.caption('目录检索不授予1A/2A/3A，不生成行情、胜率或买入建议。未来生效目录不算当前在市证明。')
