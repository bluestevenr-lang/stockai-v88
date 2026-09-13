"""Live module checks use source clocks, never an old saved green summary."""
import importlib.util
from pathlib import Path


def render(repo):
    import streamlit as st
    from system_diagnostics import build
    try:
        doc=build(Path(repo))
        failures=sum(n for k,n in doc['states'].items() if k not in {'事件档案','结构可读·行级证据另核'})
        with st.expander(f"系统逐模块检查 · {doc['module_count']}个数据域 · {failures}项待处理",expanded=False):
            st.caption(f"实际检查 {doc['checked_at']}。{doc['scope']}")
            st.dataframe([{'模块':m['module'],'数据文件':f['file'],'状态':f['status'],'记录数':f['rows'],
                           '时间类型':f['clock_kind'],'原始记录时间':f['evidence_at']} for m in doc['modules'] for f in m['files']],hide_index=True,width='stretch')
            st.caption(doc['authority'])
    except (OSError,ValueError,TypeError,KeyError) as exc:
        st.caption('系统检查暂不可用：'+type(exc).__name__+'；保留原始数据并等待重检。')
