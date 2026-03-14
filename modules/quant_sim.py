"""
modules/quant_sim.py — 量化模拟交易 V88 Tab 渲染模块
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
从 Gist（云端）或本地文件读取 quant_worker 产生的状态，
用 Streamlit 展示：总览、当前持仓、交易历史、净值曲线、扫描日志。
"""

import os, json, time, logging
from datetime import datetime
from pathlib import Path

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import requests

log = logging.getLogger(__name__)

_DIR        = Path(__file__).parent.parent
_CACHE_DIR  = _DIR / ".cache_brief"
_STATE_FILE = _CACHE_DIR / "quant_state.json"

_GIST_CACHE: dict = {}
_GIST_CACHE_TS: float = 0.0
_GIST_CACHE_TTL = 300   # 5 分钟缓存，避免频繁调 GitHub API


# ── 数据加载 ──────────────────────────────────────────────────

def _get_secrets(key: str) -> str:
    try:
        return st.secrets.get(key, "") or os.environ.get(key, "")
    except Exception:
        return os.environ.get(key, "")


def _fetch_from_gist() -> dict | None:
    """从 Gist 读取 quant_state.json（带内存缓存 5 分钟）"""
    global _GIST_CACHE, _GIST_CACHE_TS
    gist_id = _get_secrets("GIST_ID")
    if not gist_id:
        return None

    now = time.time()
    if _GIST_CACHE and now - _GIST_CACHE_TS < _GIST_CACHE_TTL:
        return _GIST_CACHE

    gist_token = _get_secrets("GIST_TOKEN")
    try:
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "StockAI-V88-QuantSim",
        }
        if gist_token:
            headers["Authorization"] = f"Bearer {gist_token}"
        resp = requests.get(
            f"https://api.github.com/gists/{gist_id}",
            headers=headers, timeout=12,
        )
        files = resp.json().get("files", {})
        fdata = files.get("quant_state.json")
        if not fdata:
            return None
        data = json.loads(fdata.get("content", "{}"))
        _GIST_CACHE    = data
        _GIST_CACHE_TS = now
        return data
    except Exception as e:
        log.warning(f"量化状态 Gist 读取失败: {e}")
        return None


def _load_state() -> dict | None:
    """优先 Gist，降级本地文件"""
    state = _fetch_from_gist()
    if state:
        return state
    if _STATE_FILE.exists():
        try:
            return json.loads(_STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return None


# ── 格式化工具 ─────────────────────────────────────────────────

def _ts_to_str(ts: float) -> str:
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "—"


def _pct_color(val: float) -> str:
    if val > 0:
        return "#26a69a"   # 绿
    if val < 0:
        return "#ef5350"   # 红
    return "#888"


def _fmt_pnl(val: float) -> str:
    sign = "▲" if val > 0 else ("▼" if val < 0 else "")
    return f"{sign} {abs(val):,.2f}"


# ── 主渲染入口 ─────────────────────────────────────────────────

def render_quant_sim_tab():
    """在 V88 Streamlit 页面中渲染量化模拟 Tab 的全部内容"""

    st.markdown(
        '<p style="font-size:13px;font-weight:700;margin-bottom:0.4rem;">'
        '🤖 量化模拟交易 · 三市场自动策略</p>',
        unsafe_allow_html=True,
    )

    # ── 加载数据 ─────────────────────────────────────────────
    state = _load_state()

    if state is None:
        st.info(
            "📭 **暂无量化数据**\n\n"
            "GitHub Actions 会在每个交易时段自动运行 `quant_worker.py`，"
            "结果同步至 Gist 后刷新本页即可看到。\n\n"
            "**首次使用**：可手动在 GitHub Actions 页面触发 `Quant Simulation Worker` 工作流。"
        )
        _render_strategy_intro()
        return

    # ── 顶部操作栏 ───────────────────────────────────────────
    col_refresh, col_ts = st.columns([1, 4])
    with col_refresh:
        if st.button("🔄 刷新", key="quant_refresh", help="清除本地缓存，重新从 Gist 拉取"):
            global _GIST_CACHE, _GIST_CACHE_TS
            _GIST_CACHE = {}
            _GIST_CACHE_TS = 0.0
            st.rerun()
    with col_ts:
        last_ts = state.get("timestamp", 0)
        age_min = (time.time() - last_ts) / 60
        age_str = f"{int(age_min)} 分钟前" if age_min < 60 else f"{age_min/60:.1f} 小时前"
        st.caption(f"📡 最后更新: {_ts_to_str(last_ts)}（{age_str}）")

    # ── 总览指标 ─────────────────────────────────────────────
    _render_summary(state)
    st.divider()

    # ── 四栏内容 ─────────────────────────────────────────────
    tab_pos, tab_trades, tab_equity, tab_log = st.tabs(
        ["📂 当前持仓", "📜 交易记录", "📈 净值曲线", "🔍 扫描日志"]
    )
    with tab_pos:
        _render_positions(state)
    with tab_trades:
        _render_trades(state)
    with tab_equity:
        _render_equity(state)
    with tab_log:
        _render_scan_log(state)

    # ── 底部策略说明（可折叠）───────────────────────────────
    with st.expander("📖 策略规则说明", expanded=False):
        _render_strategy_intro()


# ── 总览 ──────────────────────────────────────────────────────

def _render_summary(state: dict):
    initial  = state.get("initial_capital", 100_000)
    cash     = state.get("capital", initial)
    positions = state.get("positions", [])
    trades    = state.get("trades", [])

    # 估算持仓市值（使用入场成本作近似）
    pos_val  = sum(p.get("cost", 0) for p in positions)
    total    = cash + pos_val
    profit   = total - initial
    profit_pct = profit / initial * 100 if initial else 0

    won_trades = [t for t in trades if t.get("pnl", 0) > 0]
    win_rate   = len(won_trades) / len(trades) * 100 if trades else 0

    cols = st.columns(5)
    _metric(cols[0], "初始资金",   f"¥{initial:,.0f}", "")
    _metric(cols[1], "账户总值",   f"¥{total:,.0f}",   f"{profit_pct:+.2f}%",  profit_pct)
    _metric(cols[2], "可用资金",   f"¥{cash:,.0f}",    "")
    _metric(cols[3], "当前持仓数", f"{len(positions)} 只", "")
    _metric(cols[4], "累计交易",   f"{len(trades)} 笔",
            f"胜率 {win_rate:.1f}%" if trades else "")


def _metric(col, label: str, value: str, delta: str = "", delta_val: float = 0):
    color = _pct_color(delta_val) if delta_val != 0 else "#888"
    with col:
        st.markdown(
            f'<div style="background:#1e2230;border-radius:8px;padding:10px 12px;margin-bottom:4px;">'
            f'<div style="font-size:11px;color:#888;margin-bottom:4px;">{label}</div>'
            f'<div style="font-size:18px;font-weight:700;color:#e8eaf6;">{value}</div>'
            f'<div style="font-size:11px;color:{color};margin-top:2px;">{delta}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )


# ── 当前持仓 ──────────────────────────────────────────────────

def _render_positions(state: dict):
    positions = state.get("positions", [])
    if not positions:
        st.info("📭 当前无持仓")
        return

    rows = []
    for p in positions:
        entry = p.get("entry_price", 0)
        cost  = p.get("cost", 0)
        high  = p.get("highest_price", entry)
        trail_stop = high * 0.92
        rows.append({
            "市场":    p.get("market", ""),
            "代码":    p.get("symbol", ""),
            "名称":    p.get("name", ""),
            "入场价":  f"{entry:.4f}",
            "成本":    f"¥{cost:,.2f}",
            "数量":    p.get("quantity", 0),
            "追踪止损": f"{trail_stop:.4f}",
            "开仓时间": p.get("entry_time", ""),
        })
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)


# ── 交易记录 ──────────────────────────────────────────────────

def _render_trades(state: dict):
    trades = state.get("trades", [])
    if not trades:
        st.info("📭 暂无历史交易记录")
        return

    rows = []
    for t in trades[:50]:
        pnl     = t.get("pnl", 0)
        pnl_pct = t.get("pnl_pct", 0)
        rows.append({
            "市场":    t.get("market", ""),
            "名称":    t.get("name", t.get("symbol", "")),
            "入场价":  t.get("entry_price", 0),
            "出场价":  t.get("exit_price", 0),
            "盈亏":    round(pnl, 2),
            "盈亏%":   f"{pnl_pct:+.2f}%",
            "离场原因": t.get("exit_reason", ""),
            "开仓":    t.get("entry_time", ""),
            "平仓":    t.get("exit_time", ""),
        })
    df = pd.DataFrame(rows)

    def _color_pnl(val):
        c = "#26a69a" if val > 0 else ("#ef5350" if val < 0 else "")
        return f"color: {c}" if c else ""

    styled = df.style.map(_color_pnl, subset=["盈亏"])
    st.dataframe(styled, use_container_width=True, hide_index=True)

    # 统计
    total_pnl = sum(t.get("pnl", 0) for t in trades)
    won       = sum(1 for t in trades if t.get("pnl", 0) > 0)
    st.caption(
        f"共 {len(trades)} 笔 | 盈利 {won} 笔 | 亏损 {len(trades)-won} 笔 | "
        f"累计盈亏 ¥{total_pnl:+,.2f}"
    )


# ── 净值曲线 ──────────────────────────────────────────────────

def _render_equity(state: dict):
    hist = state.get("equity_history", [])
    if len(hist) < 2:
        st.info("📈 净值曲线数据不足（运行次数需 ≥ 2）")
        return

    dates   = [h["date"] for h in hist]
    equities = [h["equity"] for h in hist]
    initial = state.get("initial_capital", 100_000)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates, y=equities,
        mode="lines+markers",
        line=dict(color="#7c4dff", width=2),
        marker=dict(size=4),
        fill="tozeroy",
        fillcolor="rgba(124,77,255,0.10)",
        name="账户净值",
    ))
    fig.add_hline(y=initial, line_dash="dot", line_color="#888", annotation_text="初始资金")
    fig.update_layout(
        height=320,
        margin=dict(l=0, r=0, t=20, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, color="#888"),
        yaxis=dict(showgrid=True, gridcolor="#2a2d3a", color="#888"),
        font=dict(size=11),
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)

    # 收益率标注
    latest = equities[-1]
    pnl_pct = (latest - initial) / initial * 100
    color = _pct_color(pnl_pct)
    st.markdown(
        f'<div style="text-align:center;font-size:12px;color:{color};">'
        f'累计收益率 <b>{pnl_pct:+.2f}%</b>（¥{latest-initial:+,.2f}）</div>',
        unsafe_allow_html=True,
    )


# ── 扫描日志 ──────────────────────────────────────────────────

def _render_scan_log(state: dict):
    logs = state.get("scan_logs", [])
    if not logs:
        st.info("📭 暂无扫描日志")
        return

    for entry in logs[:40]:
        action = entry.get("action", "")
        sym    = entry.get("symbol", "")
        name   = entry.get("name", sym)
        price  = entry.get("price", 0)
        reason = entry.get("reason", "")
        ts     = entry.get("time", "")
        pnl    = entry.get("pnl", None)

        if action == "OPEN":
            icon, bg = "🟢", "#1a2e1a"
            text = f"**开仓** {name}（{sym}）@ {price}  _{reason}_"
        elif action == "CLOSE":
            pnl_str = f"  盈亏 ¥{pnl:+,.2f}" if pnl is not None else ""
            icon, bg = ("🔴", "#2e1a1a") if (pnl or 0) < 0 else ("🟡", "#2a2a1a")
            text = f"**平仓** {name}（{sym}）@ {price}  {reason}{pnl_str}"
        else:
            icon, bg = "⚪", "#1e2230"
            text = f"{reason}"

        st.markdown(
            f'<div style="background:{bg};border-radius:6px;padding:6px 10px;'
            f'margin-bottom:4px;font-size:12px;">'
            f'{icon} <span style="color:#888;margin-right:6px;">{ts}</span> {text}</div>',
            unsafe_allow_html=True,
        )


# ── 策略说明 ──────────────────────────────────────────────────

def _render_strategy_intro():
    st.markdown("""
**策略：5分钟 EMA 趋势 + RSI 动量过滤**

| 维度 | 规则 |
|------|------|
| **K线粒度** | 5 分钟线（每 5 分钟扫描一次） |
| **入场** | EMA20 > EMA50（均线多头） AND 50 < RSI(14) < 65 AND MACD 金叉且在零轴上方 |
| **止损** | 入场价 **−1.5%** 硬止损 |
| **止盈** | 入场价 **+3%** 目标 |
| **追踪止损** | 持仓期间最高点回撤 **2%** 自动平仓（锁定利润） |
| **风控** | 最多同时持仓 5 只 · 单仓占总资金 18% |
| **价格来源** | Yahoo Finance v8 实时报价（与 V88 同源） |

**关注标的（17只）**  
美股：英伟达 NVDA、苹果 AAPL、特斯拉 TSLA、亚马逊 AMZN、Meta、微软 MSFT、谷歌 GOOGL  
港股：腾讯 0700.HK、阿里 9988.HK、汇丰 0005.HK、**华润电力 0836.HK**、美团 3690.HK  
A股：贵州茅台 600519、宁德时代 300750、中国平安 601318、五粮液 000858

> ⚠️ 本系统为**模拟盘**，使用虚拟资金 ¥100,000，不涉及真实交易。
    """)
