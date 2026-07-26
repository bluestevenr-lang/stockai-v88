# -*- coding: utf-8 -*-
"""V88·分页演示版(2026-07-26 用户点单"先做下分页我看看效果,确认后再说改不改")
独立小程序·读同一份落盘数据·零计算零AI·不动主程序。跑8502端口。
六页: 总览作战台 / 持仓 / 自选 / 机会雷达 / 研究报告 / 系统
"""
import json
from datetime import datetime
from pathlib import Path

import streamlit as st

st.set_page_config(page_title="V88·分页演示", layout="wide")
REPO = Path.home() / "Desktop" / "ai-daily-report-v2"


def J(name):
    try:
        return json.loads((REPO / "data" / name).read_text(encoding="utf-8"))
    except Exception:
        return {}


def MD(name, n=4000):
    try:
        return (REPO / "data" / name).read_text(encoding="utf-8")[:n]
    except Exception:
        return ""


MKF = {"A股": "🇨🇳", "港股": "🇭🇰", "美股": "🇺🇸"}


def mk_of(code):
    c = str(code or "").upper()
    return ("A股" if c.endswith((".SS", ".SZ", ".SH", ".BJ")) else
            ("港股" if c.endswith(".HK") else "美股"))


PAGES = ["🏠 总览作战台", "💼 持仓", "⭐ 自选", "🛰️ 机会雷达", "📰 研究报告", "⚙️ 系统"]
pg = st.segmented_control("页", PAGES, default=PAGES[0], label_visibility="collapsed") or PAGES[0]

snap = J("market_snapshot.json")
idc = (J("intraday_decisions.json").get("rows") or [])
gate = J("health_gate.json")

if pg == PAGES[0]:
    # ── 总览作战台: 定调条+行动中心+三市场矩阵+拐点+前置——一屏读完 ──
    cells = []
    for mk in ("美股", "A股", "港股"):
        b = (snap.get("markets") or {}).get(mk) or {}
        p = dict((x[0], x[1]) for x in ((b.get("l3") or {}).get("probs") or [])).get("2周")
        t = (b.get("temperature") or {})
        chg = float(((b.get("indices") or [{}])[0] or {}).get("chg1d") or 0)
        cells.append(f"{MKF[mk]}{mk} 日{chg:+.1f}% 周{p}% {t.get('temp')}°")
    st.markdown(f"### 🛡️ 今日总裁决　<span style='font-size:14px;color:#64748b'>{'　'.join(cells)}</span>",
                unsafe_allow_html=True)
    if gate.get("degraded"):
        st.warning("⛔ 数据闸门降级——新买单暂停: " + "；".join(gate.get("reasons") or []))
    buys = [r for r in idc if str((r.get("entry_plan") or {}).get("mode") or "")
            in ("现价可进", "回踩到位", "突破确认")][:5]
    sells = sorted([r for r in idc if r.get("scope") == "持仓" and any(
        k in str(r.get("action", "")) for k in ("减", "退", "清", "止损"))],
        key=lambda r: -(r.get("p_down") or 0))[:5]
    tb, ts2 = st.tabs([f"✅确认买({len(buys)})", f"⚔️卖/减({len(sells)})"])
    with tb:
        st.table([{"名称": f"{MKF[mk_of(r.get('code'))]}{r.get('name')}", "动作": f"买·{r.get('p_up')}%",
                   "模式": (r.get("entry_plan") or {}).get("mode"), "现价": r.get("last")} for r in buys]
                 or [{"提示": "买侧无绿灯——现金也是仓位"}])
    with ts2:
        st.table([{"名称": f"{MKF[mk_of(r.get('code'))]}{r.get('name')}", "动作": f"{r.get('action')}·看跌{r.get('p_down')}%",
                   "现价": r.get("last")} for r in sells] or [{"提示": "无纪律触发"}])
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**⏳ 拐点倒计时**")
        for r in (J("turning_forecast.json").get("rows") or [])[:5]:
            st.markdown(f"{'🔺' if r.get('side') == 'top' else '🌱'} {r.get('name')} "
                        f"**{(r.get('window_days') or ['?', '?'])[0]}~{(r.get('window_days') or ['?', '?'])[1]}日"
                        f"{'见顶' if r.get('side') == 'top' else '见底'}·强度{r.get('prob')}**"
                        f" 确认{r.get('confirm_price')}" + (f" ⚡{r.get('event')[:20]}" if r.get("event") else ""))
    with c2:
        st.markdown("**📡 前置信号 & 事件**")
        ps = J("pre_signals.json")
        for e in (ps.get("earn_gap") or [])[:3]:
            st.markdown(f"📊 {str(e.get('name'))[:22]} · {str(e.get('note'))[:38]}")
        for e in (J("macro_events.json").get("events") or [])[:4]:
            st.markdown(f"📅 {e.get('date', '')[5:]}(周{e.get('dow')}) {e.get('event')}")

elif pg == PAGES[1]:
    st.markdown("### 💼 持仓")
    rows = [r for r in idc if r.get("scope") == "持仓"]
    st.table([{"名称": f"{MKF[mk_of(r.get('code'))]}{r.get('name')}", "现价": r.get("last"),
               "动作": r.get("action"), "涨/跌概率": f"{r.get('p_up')}%/{r.get('p_down')}%",
               "口径": str(r.get("horizon") or "")[:10]} for r in rows])

elif pg == PAGES[2]:
    st.markdown("### ⭐ 自选")
    rows = [r for r in idc if r.get("scope") != "持仓"]
    st.table([{"名称": f"{MKF[mk_of(r.get('code'))]}{r.get('name')}", "现价": r.get("last"),
               "入场模式": (r.get("entry_plan") or {}).get("mode"),
               "涨概率": f"{r.get('p_up')}%"} for r in rows])

elif pg == PAGES[3]:
    st.markdown("### 🛰️ 机会雷达")
    t1, t2, t3, t4 = st.tabs(["🐴 黑马", "🎖️ 五行业代表", "🆕 打新", "🔥 涨停接力"])
    with t1:
        st.table([{"名称": h.get("name"), "分": h.get("p_up"),
                   "模式": ((h.get("trade_plan") or {}).get("short") or {}).get("mode")}
                  for h in (J("darkhorse.json").get("horses") or [])[:10]])
    with t2:
        for mk, grp in (J("sector_reps.json").get("markets") or {}).items():
            st.markdown(f"**{MKF.get(mk, '')}{mk}**")
            st.table([{"行业": s.get("sector"),
                       "代表": (s.get("pick") or s.get("watch") or {}).get("name"),
                       "2周%": (s.get("pick") or s.get("watch") or {}).get("p_up"),
                       "回踩带": "~".join(str(x) for x in ((s.get("pick") or s.get("watch") or {}).get("shallow") or []))}
                      for s in grp])
    with t3:
        st.table([{"新股": i.get("name"), "市场": i.get("market"), "评级": i.get("rating"),
                   "申购日": i.get("apply_date")} for i in (J("ipo_radar.json").get("rows") or [])[:8]])
    with t4:
        st.table([{"名称": r.get("name"), "主线": r.get("theme"), "备注": str(r.get("note"))[:24]}
                  for r in (J("limit_up_radar.json").get("follow") or J("limit_up_radar.json").get("rows") or [])[:8]])

elif pg == PAGES[4]:
    st.markdown("### 📰 研究报告")
    t1, t2, t3 = st.tabs(["🎬 明日预案", "📋 日报", "📅 周报"])
    with t1:
        tp = J("tomorrow_plan.json")
        st.caption(f"for {tp.get('for_date')} · {tp.get('generated_at')}")
        st.markdown(str(tp.get("script") or "").replace("~", "～"))
    with t2:
        st.markdown(MD("daily_report.md", 6000).replace("~", "～"))
    with t3:
        st.markdown(MD("weekly_report.md", 6000).replace("~", "～"))

else:
    st.markdown("### ⚙️ 系统")
    b = J("ai_budget.json")
    sr = (J("success_rates.json").get("types") or {})
    st.markdown(f"**预算**: 主账本 ¥{b.get('spent_rmb', 0):.2f}/4.00 (总5含网页1)")
    st.markdown("**实盘战绩**: " + "｜".join(
        f"{k} {v.get('rate')}%(n{v.get('n')})" for k, v in sr.items()
        if isinstance(v, dict) and v.get("rate") is not None))
    st.markdown(f"**数据闸门**: {'⛔降级 ' + str(gate.get('reasons')) if gate.get('degraded') else '✅健康'} · {gate.get('checked_at', '')}")
    try:
        led = json.loads((REPO / "journal" / "predictions.json").read_text(encoding="utf-8"))
        from collections import Counter
        st.markdown(f"**预测台账**: {len(led)}条 · " + str(dict(Counter(r.get('status') for r in led))))
    except Exception:
        pass

st.caption(f"V88·分页演示版 · 数据与主程序同源(落盘只读) · {datetime.now().strftime('%H:%M')} · 主程序在8501不受影响")
