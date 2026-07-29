"""V100 主入口：2个Tab — 深度研究 + 自选股"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from ..core import analysis
from ..data import fetcher, cache

st.set_page_config(page_title="StockAI V100", layout="wide")

# ── 侧边栏：搜索 ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("StockAI V100")
    code_input = st.text_input("输入代码", placeholder="AAPL / 00700.HK / 600519.SS")
    name_input = st.text_input("名称（可选）", placeholder="苹果 / 腾讯")
    period = st.selectbox("周期", ["6mo", "1y", "2y"], index=1)
    run_btn = st.button("深度分析", type="primary", use_container_width=True)
    st.divider()
    if st.button("清除过期缓存", use_container_width=True):
        n = cache.clear_expired()
        st.success(f"已清理 {n} 个缓存文件")

tab_research, tab_watchlist = st.tabs(["⚔️ 深度研究", "📋 自选股"])

# ── Tab1: 深度研究 ────────────────────────────────────────────────────────────
with tab_research:
    if run_btn and code_input.strip():
        code = code_input.strip().upper()
        with st.spinner(f"正在分析 {code}..."):
            result = analysis.run(code, name_input.strip(), period)

        if "error" in result:
            st.error(f"数据获取失败：{result['error']}")
        else:
            _render_research(result, code, period)
    else:
        st.info("在左侧输入股票代码，点击「深度分析」开始")


def _render_research(result: dict, code: str, period: str):
    tech = result["metrics"]
    plan = result["trade_plan"]
    name = result["name"]

    st.subheader(f"{name} ({code})")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("当前价", f"{tech['price']:.2f}",
                f"{tech['pct_1d']:+.1f}% 今日")
    col2.metric("RSI14", f"{tech['rsi14']:.1f}")
    col3.metric("成交量比", f"{tech['vol_ratio']:.1f}x")
    col4.metric("20日涨跌", f"{tech['pct_20d']:+.1f}%")

    df = fetcher.fetch(code, period)
    if df is not None:
        st.plotly_chart(_kline_chart(df, tech), use_container_width=True)

    if plan:
        st.subheader("交易计划")
        pcol1, pcol2, pcol3, pcol4 = st.columns(4)
        pcol1.metric("入场价", plan["entry"])
        pcol2.metric("止损价", plan["stop_loss"],
                     f"-{plan['entry']-plan['stop_loss']:.2f}")
        pcol3.metric("目标1", plan["target_1"])
        pcol4.metric("风险收益比", f"{plan['risk_reward']}x")

    if result.get("ai_text"):
        st.subheader("AI 深度诊断")
        st.markdown(result["ai_text"])


def _kline_chart(df: pd.DataFrame, tech: dict) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=df.index, open=df["Open"], high=df["High"],
        low=df["Low"], close=df["Close"], name="K线",
        increasing_line_color="#26a69a", decreasing_line_color="#ef5350",
    ))
    close = df["Close"]
    for n, color in [(20, "#ff9800"), (60, "#2196f3")]:
        ma = close.rolling(n).mean()
        fig.add_trace(go.Scatter(x=df.index, y=ma, name=f"MA{n}",
                                 line=dict(color=color, width=1)))
    fig.update_layout(
        height=420, margin=dict(l=0, r=0, t=20, b=0),
        xaxis_rangeslider_visible=False,
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig


# ── Tab2: 自选股 ──────────────────────────────────────────────────────────────
with tab_watchlist:
    _render_watchlist()


def _render_watchlist():
    st.subheader("自选股监控")

    default_codes = st.session_state.get("watchlist", ["AAPL", "TSLA", "00700.HK"])
    new_code = st.text_input("添加股票代码", key="wl_add")
    if st.button("添加", key="wl_add_btn") and new_code:
        if new_code.upper() not in default_codes:
            default_codes.append(new_code.upper())
            st.session_state["watchlist"] = default_codes

    rows = []
    for c in default_codes:
        df = fetcher.fetch(c, "1mo")
        if df is not None and not df.empty:
            price = float(df["Close"].iloc[-1])
            pct1d = float(df["Close"].pct_change(1).iloc[-1] * 100)
            pct5d = float(df["Close"].pct_change(5).iloc[-1] * 100)
            rows.append({"代码": c, "价格": price,
                         "日涨跌%": round(pct1d, 2), "5日%": round(pct5d, 2)})

    if rows:
        df_wl = pd.DataFrame(rows)
        st.dataframe(
            df_wl.style.applymap(
                lambda v: "color:green" if v > 0 else "color:red",
                subset=["日涨跌%", "5日%"]
            ),
            hide_index=True, use_container_width=True,
        )
    else:
        st.info("暂无数据，请检查网络或代码格式")
