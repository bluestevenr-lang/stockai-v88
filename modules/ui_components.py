"""
UI组件模块 - 可复用的Streamlit UI组件
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
包含：
  - render_clickable_table() - 可点击表格
  - render_ai_card() - AI卡片组件
  - render_progress_bar() - 进度条
  - render_error_message() - 错误提示
  - render_metric_card() - 指标卡片
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import pandas as pd
import logging
from typing import Optional, List, Dict, Any, Callable

# 导入streamlit
try:
    import streamlit as st
    HAS_STREAMLIT = True
except ImportError:
    HAS_STREAMLIT = False
    logging.warning("streamlit未安装，UI组件将不可用")


# ═══════════════════════════════════════════════════════════════
# 表格组件
# ═══════════════════════════════════════════════════════════════

def render_clickable_table(
    df: pd.DataFrame,
    key: str,
    display_cols: Optional[List[str]] = None,
    on_select: str = "rerun",
    selection_mode: str = "single-row"
):
    """
    渲染可点击的表格
    
    Args:
        df: DataFrame数据
        key: 唯一键
        display_cols: 显示的列
        on_select: 选择时的行为
        selection_mode: 选择模式
    """
    if not HAS_STREAMLIT:
        return
    
    if df is None or df.empty:
        st.info("暂无数据")
        return
    
    # 显示列
    if display_cols:
        df_display = df[display_cols] if all(col in df.columns for col in display_cols) else df
    else:
        df_display = df
    
    # 渲染表格
    selection = st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        on_select=on_select,
        selection_mode=selection_mode,
        key=key
    )
    
    return selection


# ═══════════════════════════════════════════════════════════════
# AI卡片组件
# ═══════════════════════════════════════════════════════════════

def render_ai_card(
    title: str,
    content: str,
    icon: str = "🤖"
):
    """
    渲染AI分析卡片
    
    Args:
        title: 标题
        content: 内容
        icon: 图标
    """
    if not HAS_STREAMLIT:
        return
    
    st.markdown(f"""
    <div class="ai-card">
        <div class="ai-title">{icon} {title}</div>
        <div style="font-size: 14px; line-height: 1.8; color: #374151;">
            {content}
        </div>
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════
# 进度条组件
# ═══════════════════════════════════════════════════════════════

class ProgressBar:
    """
    进度条管理器
    
    功能：
    - 显示进度百分比
    - 显示当前处理项
    - 自动清理
    """
    
    def __init__(self):
        if HAS_STREAMLIT:
            self.progress_bar = st.progress(0)
            self.status_text = st.empty()
        else:
            self.progress_bar = None
            self.status_text = None
    
    def update(self, current: int, total: int, message: str = ""):
        """
        更新进度
        
        Args:
            current: 当前进度
            total: 总数
            message: 状态消息
        """
        if not HAS_STREAMLIT or not self.progress_bar:
            return
        
        progress_pct = current / total if total > 0 else 0
        self.progress_bar.progress(progress_pct)
        
        if self.status_text:
            status_msg = f"{message} ({current}/{total}, {progress_pct*100:.1f}%)"
            self.status_text.text(status_msg)
    
    def clear(self):
        """清除进度条"""
        if not HAS_STREAMLIT:
            return
        
        if self.progress_bar:
            self.progress_bar.empty()
        if self.status_text:
            self.status_text.empty()


# ═══════════════════════════════════════════════════════════════
# 错误提示组件
# ═══════════════════════════════════════════════════════════════

def render_error_message(
    error_type: str,
    error_detail: str,
    suggestions: Optional[List[str]] = None
):
    """
    渲染错误提示
    
    Args:
        error_type: 错误类型
        error_detail: 错误详情
        suggestions: 建议列表
    """
    if not HAS_STREAMLIT:
        return
    
    st.error(f"❌ {error_type}")
    st.caption(error_detail)
    
    if suggestions:
        st.markdown("**💡 建议：**")
        for idx, suggestion in enumerate(suggestions, 1):
            st.caption(f"{idx}. {suggestion}")


def render_data_error(code: str, market: str = ""):
    """
    渲染数据获取失败的错误提示
    
    Args:
        code: 股票代码
        market: 市场类型
    """
    if not HAS_STREAMLIT:
        return
    
    suggestions = []
    
    if market == "HK" or code.endswith('.HK'):
        suggestions = [
            "检查代码格式是否正确（应为4位数字.HK，如0700.HK）",
            "股票可能已退市或暂停交易",
            "尝试在Yahoo Finance网站搜索验证"
        ]
    elif market == "CN" or code.endswith(('.SS', '.SZ')):
        suggestions = [
            "检查网络连接和代理设置",
            "股票可能停牌或退市",
            "验证代码格式（沪市.SS，深市.SZ）"
        ]
    else:
        suggestions = [
            "验证股票代码是否正确",
            "股票可能已退市",
            f"尝试在Yahoo Finance搜索: https://finance.yahoo.com/quote/{code}"
        ]
    
    render_error_message(
        "数据获取失败",
        f"无法获取 {code} 的数据",
        suggestions
    )


# ═══════════════════════════════════════════════════════════════
# 指标卡片组件
# ═══════════════════════════════════════════════════════════════

def render_metric_card(
    label: str,
    value: str,
    delta: Optional[str] = None,
    delta_color: str = "normal"
):
    """
    渲染指标卡片
    
    Args:
        label: 标签
        value: 值
        delta: 变化量
        delta_color: 变化颜色（normal/inverse/off）
    """
    if not HAS_STREAMLIT:
        return
    
    st.metric(
        label=label,
        value=value,
        delta=delta,
        delta_color=delta_color
    )


def render_score_badge(score: int) -> str:
    """
    渲染评分徽章HTML
    
    Args:
        score: 评分（0-100）
        
    Returns:
        HTML字符串
    """
    if score >= 75:
        color = "#10b981"  # 绿色
        text = "优秀"
    elif score >= 60:
        color = "#3b82f6"  # 蓝色
        text = "良好"
    elif score >= 45:
        color = "#f59e0b"  # 橙色
        text = "一般"
    else:
        color = "#ef4444"  # 红色
        text = "较差"
    
    return f"""
    <span style="
        background-color: {color};
        color: white;
        padding: 4px 12px;
        border-radius: 12px;
        font-weight: 600;
        font-size: 14px;
    ">{score}分 · {text}</span>
    """


# ═══════════════════════════════════════════════════════════════
# 扫描失败详情组件
# ═══════════════════════════════════════════════════════════════

def display_scan_failures(
    all_errors: List[Dict[str, str]],
    total_failed: int
):
    """
    显示扫描失败的详细信息
    
    Args:
        all_errors: 错误列表
        total_failed: 失败总数
    """
    if not HAS_STREAMLIT:
        return
    
    with st.expander(f"⚠️ 查看失败详情 ({total_failed}只) - 点击展开诊断", expanded=False):
        st.caption("💡 **常见失败原因**：")
        st.caption("1. 股票已退市或被收购")
        st.caption("2. 股票代码格式错误")
        st.caption("3. 网络连接问题或代理设置错误")
        st.caption("4. 数据源暂时不可用")
        st.divider()
        
        # 按市场分组显示
        us_errors = []
        hk_errors = []
        cn_errors = []
        
        for e in all_errors:
            code = e['code']
            if '.HK' in code or (len(code) == 5 and code[0] == '0'):
                hk_errors.append(e)
            elif '.SS' in code or '.SZ' in code or (len(code) == 6 and code[0] in '630'):
                cn_errors.append(e)
            else:
                us_errors.append(e)
        
        if us_errors:
            st.markdown("**🇺🇸 美股失败列表：**")
            for err in us_errors:
                st.caption(f"❌ **{err['name']}** ({err['code']}): {err['error']}")
        
        if hk_errors:
            st.markdown("**🇭🇰 港股失败列表：**")
            for err in hk_errors:
                st.caption(f"❌ **{err['name']}** ({err['code']}): {err['error']}")
        
        if cn_errors:
            st.markdown("**🇨🇳 A股失败列表：**")
            for err in cn_errors:
                st.caption(f"❌ **{err['name']}** ({err['code']}): {err['error']}")


# ═══════════════════════════════════════════════════════════════
# 导出功能组件
# ═══════════════════════════════════════════════════════════════

def render_export_button(
    df: pd.DataFrame,
    filename: str = "export.csv",
    label: str = "📥 导出CSV"
):
    """
    渲染导出按钮
    
    Args:
        df: 要导出的DataFrame
        filename: 文件名
        label: 按钮标签
    """
    if not HAS_STREAMLIT or df is None or df.empty:
        return
    
    csv = df.to_csv(index=False).encode('utf-8-sig')  # 使用utf-8-sig支持中文
    
    st.download_button(
        label=label,
        data=csv,
        file_name=filename,
        mime='text/csv',
    )


# ═══════════════════════════════════════════════════════════════
# 表格筛选器组件
# ═══════════════════════════════════════════════════════════════

def render_table_filters(
    df: pd.DataFrame,
    filter_columns: List[str]
) -> pd.DataFrame:
    """
    渲染表格筛选器
    
    Args:
        df: DataFrame数据
        filter_columns: 可筛选的列
        
    Returns:
        筛选后的DataFrame
    """
    if not HAS_STREAMLIT or df is None or df.empty:
        return df
    
    filtered_df = df.copy()
    
    st.markdown("**🔍 筛选条件：**")
    cols = st.columns(len(filter_columns))
    
    for idx, col_name in enumerate(filter_columns):
        if col_name not in df.columns:
            continue
        
        with cols[idx]:
            # 根据列类型选择筛选方式
            if df[col_name].dtype in ['int64', 'float64']:
                # 数值列：范围筛选
                min_val = float(df[col_name].min())
                max_val = float(df[col_name].max())
                
                selected_range = st.slider(
                    col_name,
                    min_value=min_val,
                    max_value=max_val,
                    value=(min_val, max_val),
                    key=f"filter_{col_name}"
                )
                
                filtered_df = filtered_df[
                    (filtered_df[col_name] >= selected_range[0]) &
                    (filtered_df[col_name] <= selected_range[1])
                ]
            
            else:
                # 分类列：多选筛选
                unique_values = df[col_name].unique().tolist()
                
                selected_values = st.multiselect(
                    col_name,
                    options=unique_values,
                    default=unique_values,
                    key=f"filter_{col_name}"
                )
                
                if selected_values:
                    filtered_df = filtered_df[filtered_df[col_name].isin(selected_values)]
    
    return filtered_df


# ═══════════════════════════════════════════════════════════════
# CSS样式
# ═══════════════════════════════════════════════════════════════

def inject_custom_css():
    """注入自定义CSS样式"""
    if not HAS_STREAMLIT:
        return
    
    st.markdown("""
    <style>
        /* AI卡片样式 */
        .ai-card { 
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border: 1px solid #e2e8f0;
            border-radius: 16px;
            padding: 24px;
            margin-bottom: 20px; 
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
            position: relative;
            overflow: hidden;
        }
        
        .ai-card::before {
            content: "";
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 4px;
            background: linear-gradient(90deg, #3b82f6, #8b5cf6);
        }
        
        .ai-title { 
            font-size: 1.25rem;
            font-weight: 800;
            color: #1e3a8a;
            margin-bottom: 16px; 
            border-bottom: 1px solid #e5e7eb;
            padding-bottom: 12px;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        /* 表格行悬停效果 */
        div[data-testid="stDataFrame"] tbody tr:hover {
            background-color: #eff6ff !important;
            cursor: pointer !important;
        }
        
        /* 表格选中行高亮 */
        div[data-testid="stDataFrame"] tbody tr.row-selected {
            background-color: #dbeafe !important;
            font-weight: 600;
        }
    </style>
    """, unsafe_allow_html=True)
