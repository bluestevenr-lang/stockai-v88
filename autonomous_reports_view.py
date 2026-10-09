"""Dated free-fact reports remain readable without an AI publication."""
import json
from v88_paths import core_root

def render(st):
    try:doc=json.loads((core_root()/'data/autonomous_report.json').read_text())
    except (OSError,ValueError):return
    if doc.get('version')!='autonomous-reports-v1' or doc.get('model_calls')!=0:return
    with st.expander('📊 行情日报 / 📅 周报 · 无模型额度依赖',expanded=False):
        from module_freshness import html as freshness_html
        st.html(freshness_html('autonomous_report.json',doc))
        st.caption('编制 '+str(doc.get('generated_at',''))+' · 报价日期见各项；AI评级独立审核')
        for tab,key in zip(st.tabs(['📊 日报','📅 周报']),('daily','weekly')):
            with tab:st.markdown(doc.get(key) or '本期数据尚未生成')
