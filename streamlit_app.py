"""
V88 云端版（Streamlit Community Cloud · 完全免令牌）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
24小时在线，与 Mac 开关机无关。数据从公开分支免令牌读取：
  https://raw.githubusercontent.com/bluestevenr-lang/stockai-v88/data/pub/
GitHub Actions 每交易日 07:00/14:00/21:00 自动发布（已剔除持仓明细，保护隐私）。
用户无需配置任何 Secrets；可选设 APP_PASSWORD 加一道访问密码。
"""
import json
import requests
import streamlit as st

PUB_BASE = "https://raw.githubusercontent.com/bluestevenr-lang/stockai-v88/data/pub"

st.set_page_config(page_title="V88 云端版", page_icon="☁️", layout="centered",
                   initial_sidebar_state="collapsed")

# ── 可选访问密码（数字/文字、带不带引号都兼容）──────────────
_pw = str(st.secrets.get("APP_PASSWORD", "") or "").strip()
if _pw and not st.session_state.get("_auth_ok"):
    st.title("☁️ V88 云端版")
    _in = st.text_input("访问密码", type="password")
    if st.button("进入", type="primary", use_container_width=True):
        if str(_in).strip() == _pw:
            st.session_state["_auth_ok"] = True
            st.rerun()
        else:
            st.error("密码错误")
    st.stop()


@st.cache_data(ttl=300, show_spinner=False)
def pub_text(name: str):
    try:
        r = requests.get(f"{PUB_BASE}/{name}", timeout=12)
        return r.text if r.status_code == 200 else None
    except Exception:
        return None


@st.cache_data(ttl=300, show_spinner=False)
def pub_journal_list():
    # 通过 GitHub API 列 data 分支 pub/journal 目录（公开仓，无需令牌）
    try:
        r = requests.get("https://api.github.com/repos/bluestevenr-lang/stockai-v88/contents/pub/journal?ref=data",
                         timeout=12)
        return sorted([x["name"] for x in r.json() if x["name"].endswith(".json")]) if r.status_code == 200 else []
    except Exception:
        return []


st.title("☁️ V88 云端版")
st.caption("24小时在线 · 数据每交易日 07:00/14:00/21:00 自动更新 · 与 Mac 开关机无关 · 免登录免配置")

c_nav, c_rf = st.columns([5, 1])
with c_rf:
    if st.button("🔄", help="强制刷新"):
        pub_text.clear(); pub_journal_list.clear(); st.rerun()
with c_nav:
    _nav = st.radio("导航", ["🧭 导航", "📊 日报", "📅 周报", "📈 大盘板块", "🔁 复盘"],
                    horizontal=True, label_visibility="collapsed")

_snap_raw = pub_text("market_snapshot.json")
_snap = None
if _snap_raw:
    try:
        _snap = json.loads(_snap_raw)
    except Exception:
        _snap = None

_NOT_READY = "📭 数据生成中（每交易日 07:00/14:00/21:00 自动发布，稍后自动出现，可点右上 🔄 刷新）"

# ── 🧭 导航 ─────────────────────────────────────────────────
if _nav == "🧭 导航":
    _rep = pub_text("daily_report.md") or ""
    st.markdown("#### 🧭 今日导航 · 该关注什么")
    st.caption(f"温度定仓位 → 轮动定板块 → 操作榜定标的 ｜ 数据 {(_snap or {}).get('generated_at', '—')}")
    if _snap and _snap.get("markets"):
        for _mkt in ("美股", "A股", "港股"):
            _t = (_snap["markets"].get(_mkt) or {}).get("temperature")
            if _t:
                st.markdown(f"🌡 **{_mkt} {_t['temp']}/100** {_t['label']} → 仓位 **{_t['position']}**")
        st.markdown("---")
        for _mkt in ("美股", "A股", "港股"):
            _ixs = ((_snap["markets"].get(_mkt) or {}).get("indices")) or []
            if _ixs:
                st.markdown(f"**{_mkt}**：" + " ｜ ".join(
                    f"{ix['trend'].split()[0]}{ix['name']} {ix['chg5d']:+.1f}%" for ix in _ixs[:3]))
        _hints = []
        for _mkt in ("美股", "A股", "港股"):
            _secs = ((_snap["markets"].get(_mkt) or {}).get("sectors")) or []
            if len(_secs) < 4:
                continue
            _r5 = {s["symbol"]: i for i, s in enumerate(sorted(_secs, key=lambda x: -x["chg5d"]))}
            _r20 = {s["symbol"]: i for i, s in enumerate(sorted(_secs, key=lambda x: -x["chg20d"]))}
            _jp = max(2, len(_secs) // 3)
            for s in _secs:
                _d = _r20[s["symbol"]] - _r5[s["symbol"]]
                if _d >= _jp and s["chg5d"] > 0:
                    _hints.append(f"🔥 {_mkt}·{s['name']} 轮入({s['chg5d']:+.1f}%)")
                elif _d <= -_jp and s["chg20d"] > 0:
                    _hints.append(f"🧊 {_mkt}·{s['name']} 退潮")
        if _hints:
            st.markdown("**板块轮动**：" + " ｜ ".join(_hints[:5]))
    else:
        st.info(_NOT_READY)
    _i = _rep.find("## 🎯 今日操作榜")
    if _i > 0:
        _j = _rep.find("## 二、", _i)
        with st.expander("🎯 今日操作榜（中长短×中美港 各Top3·实价校准）", expanded=True):
            st.markdown(_rep[_i:_j if _j > 0 else _i + 3000])
    st.caption("💡 持仓建议为隐私内容，请在飞书推送或 Mac/局域网 V88 查看")

# ── 📊 日报 / 📅 周报 ────────────────────────────────────────
elif _nav in ("📊 日报", "📅 周报"):
    _txt = pub_text("daily_report.md" if _nav == "📊 日报" else "weekly_report.md")
    if _txt:
        st.markdown(_txt)
        st.caption("💡 持仓建议为隐私内容，不在云端公开显示；见飞书或 Mac 版")
    else:
        st.info(_NOT_READY if _nav == "📊 日报" else "📅 周报每周日生成")

# ── 📈 大盘板块 ──────────────────────────────────────────────
elif _nav == "📈 大盘板块":
    st.markdown("#### 📈 大盘走势与板块轮动")
    if _snap:
        st.caption(f"📅 快照生成于 {_snap.get('generated_at', '?')}")
        for mkt, blk in _snap.get("markets", {}).items():
            st.markdown(f"### {mkt}")
            _t = blk.get("temperature")
            if _t:
                st.markdown(f"🌡 温度 **{_t['temp']}/100** {_t['label']}（趋势{_t['trend']}/宽度{_t['breadth']}/动量{_t['momentum']}/量能{_t.get('vol_heat','—')}）→ 仓位 {_t['position']}")
            for ix in blk.get("indices", []):
                st.markdown(f"- **{ix['name']}** {ix['last']}（5日 {ix['chg5d']:+.1f}% / 20日 {ix['chg20d']:+.1f}%）｜{ix['trend']}")
            secs = blk.get("sectors", [])
            if secs:
                top = sorted(secs, key=lambda x: x["chg5d"], reverse=True)
                st.markdown("**板块（近5日）**：领涨 " + "、".join(f"{s['name']} {s['chg5d']:+.1f}%" for s in top[:3])
                            + (" ｜ 落后 " + "、".join(f"{s['name']} {s['chg5d']:+.1f}%" for s in top[-3:][::-1]) if len(top) > 5 else ""))
    else:
        st.info(_NOT_READY)

# ── 🔁 复盘 ─────────────────────────────────────────────────
elif _nav == "🔁 复盘":
    st.markdown("#### 🔁 推荐复盘 · 说话要算数")
    _files = pub_journal_list()
    if not _files:
        st.info("暂无复盘存档（每交易日日报后自动发布）")
    else:
        st.caption(f"已存档 {len(_files)} 天")
        _sel = st.selectbox("选择日期", list(reversed(_files)))
        _raw = pub_text(f"journal/{_sel}")
        if _raw:
            try:
                st.dataframe(json.loads(_raw).get("picks", []), hide_index=True, use_container_width=True)
            except Exception:
                st.code(_raw[:800])
        st.caption("完整收益核算与命中率见每周日推送的周报「🔁 推荐复盘」章节")

st.divider()
st.caption("V88 云端版 · 仅供研究参考，不构成投资建议")
