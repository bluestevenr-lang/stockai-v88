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
    _nav = st.radio("导航", ["🧭 导航", "🔍 个股搜索", "📊 日报", "📅 周报", "📈 大盘板块", "🔁 复盘"],
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

# ── 🔍 个股搜索（云端实时·趋势脉搏）────────────────────────────
elif _nav == "🔍 个股搜索":
    st.markdown("#### 🔍 个股搜索 · 买卖前必看")
    st.caption("中文名（A股全市场5200+/港股/美股离线秒搜）或代码（AAPL｜0700｜600519）· 重名会列出让你选")
    import cloud_engine
    _code = st.text_input("股票代码", value="", placeholder="中微 / 腾讯 / 茅台 / AAPL / 600519",
                          label_visibility="collapsed").strip()
    if st.button("🔎 搜索", type="primary", use_container_width=True) and _code:
        _has_cn = any("一" <= ch <= "鿿" for ch in _code)
        if not _has_cn:  # 代码型输入直接定标的
            _sym0 = cloud_engine.to_yf(_code)
            st.session_state["_sch_cands"] = [(cloud_engine.name_of(_sym0) or _sym0, _sym0, "")]
        else:            # 名字型输入 → 离线名录候选（重名全列）
            st.session_state["_sch_cands"] = cloud_engine.search_candidates(_code)

    _cands = st.session_state.get("_sch_cands")
    _target = None
    if _cands is not None and len(_cands) == 0:
        st.error("没找到匹配。可换个说法(全称/简称)或直接用代码：美股字母(AAPL)、港股数字(0700)、A股6位(600519)。")
    elif _cands and len(_cands) == 1:
        _target = _cands[0]
    elif _cands and len(_cands) > 1:
        st.info(f"找到 {len(_cands)} 个匹配，请选择：")
        _opts = [f"{n}（{c}·{m}）" for n, c, m in _cands]
        _sel = st.selectbox("选择股票", _opts, label_visibility="collapsed")
        if st.button("✅ 就是这只，开始分析", type="primary", use_container_width=True):
            _target = _cands[_opts.index(_sel)]

    if _target:
        _tname, _tsym, _tmkt = _target
        with st.spinner(f"分析 {_tname}（{_tsym}）中..."):
            try:
                _df = cloud_engine.fetch(_tsym)
                _full = cloud_engine.analyze_trend_full(_df) if _df is not None else None
                r = ({"full": _full, "symbol": _tsym, "name": _tname, "asof": str(_df.index[-1])[:10]}
                     if _full else {"error": f"未取到 {_tname}（{_tsym}）的行情，稍后重试或换代码搜索"})
            except Exception as e:
                r = {"error": f"引擎异常：{type(e).__name__}"}
        if r.get("error"):
            st.error(r["error"])
        else:
            # 先看清是哪只（全名+代码），再看分析
            st.markdown(f"## 📌 {r.get('name') or r['symbol']}（{r['symbol']}）")
            f = r["full"]
            _concl_color = {"进攻": "🟢", "试仓": "🧪", "持有": "🔵", "等待": "⏳", "减仓": "🟡", "回避": "🔴"}
            # ① 总分 + 一句话结论
            c1, c2, c3 = st.columns([1, 1, 1])
            c1.metric("趋势总分", f"{f['total']}/100")
            c2.metric("现价", f"{f['last']}")
            c3.metric("RSI", f"{f['rsi']}")
            st.markdown(f"### {_concl_color.get(f['conclusion'],'')} 一句话结论：**{f['conclusion']}**")
            st.success(f"**操作建议：{f['action']}**")

            # ② 六行状态速览
            st.markdown(
                f"- **趋势阶段**：{f['stage']}\n"
                f"- **量价状态**：{f['vp']}\n"
                f"- **水位判断**：{f['water']}（{f['pos52']}%）→ {f['water_adv']}\n"
                f"- **MACD状态**：{f['macd_txt']}\n"
                f"- **均线状态**：{f['ma_state']}（{f['ma_txt']}）\n"
                f"- **失效条件**：{f['invalid']}")

            # ③ 可执行价位
            st.markdown("##### 🎯 可执行价位")
            p1, p2, p3 = st.columns(3)
            p1.metric("买入区间", f['buy_zone'])
            p2.metric("回踩买点", f"{f['pullback']}")
            p3.metric("突破加仓", f"{f['breakout']}")
            p4, p5, p6 = st.columns(3)
            p4.metric("止损位", f"{f['stop']}")
            p5.metric("减仓位", f"{f['reduce']}")
            p6.metric("支撑/压力", f"{f['support']}/{f['resistance']}")

            # ④ 趋势分拆解(8项)
            with st.expander("📐 趋势分拆解（8项子分 → 总分）", expanded=False):
                _bd = f["breakdown"]
                _rows = [{"维度": k, "实际情况": d, "得分": sc, "权重": f"{int(w*100)}%"}
                         for k, (sc, w, d) in _bd.items()]
                st.dataframe(_rows, hide_index=True, use_container_width=True)
                if not f["sector_known"]:
                    st.caption("_板块强度云端按中性50计；资金动向=OBV能量潮真实数据；主判断由价格/均线/MACD/量价/水位驱动_")
            st.caption(f"20日动量 {f['chg20']:+.1f}% ｜ 乖离MA20 {f['bias20']:+.1f}% ｜ 量比 {f['volr']} ｜ 数据截至 {r['asof']}")

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
