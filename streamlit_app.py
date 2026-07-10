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
from datetime import datetime, timezone, timedelta

PUB_BASE = "https://raw.githubusercontent.com/bluestevenr-lang/stockai-v88/data/pub"

# ── 三时段体系（北京时间）：时段一 00-08 / 时段二 08-16 / 时段三 16-24 ──
BJT = timezone(timedelta(hours=8))

def _now_bjt():
    return datetime.now(BJT)

def _parse_ts(s):
    """解析 '2026-07-09 21:33' / ISO 等时间串 → BJT datetime；失败 None"""
    s = str(s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=BJT)
        except Exception:
            pass
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return d.astimezone(BJT) if d.tzinfo else d.replace(tzinfo=BJT)
    except Exception:
        return None

def _period_of(dt):
    return ("🌙 时段一 00-08", "☀️ 时段二 08-16", "🌆 时段三 16-24")[dt.hour // 8]

def _fresh_caption(ts, what="数据"):
    """统一新鲜度标注：生成时间 · 距今 · 所属时段 · 是否本时段"""
    dt = _parse_ts(ts)
    if not dt:
        return f"🕐 {what}时间未知"
    now = _now_bjt()
    age_h = (now - dt).total_seconds() / 3600
    same = _period_of(now) == _period_of(dt) and dt.date() == now.date()
    flag = "✅ 本时段" if same else f"⚠️ 属 {_period_of(dt)}" + ("" if dt.date() == now.date() else f"·{dt.strftime('%m-%d')}")
    return f"🕐 {what}生成于 {dt.strftime('%m-%d %H:%M')} · {age_h:.1f}小时前 · 现在{_period_of(_now_bjt())} · {flag}"

@st.cache_data(ttl=120, show_spinner=False)
def pub_meta() -> dict:
    try:
        r = requests.get(f"{PUB_BASE}/meta.json", timeout=10)
        return r.json() if r.status_code == 200 else {}
    except Exception:
        return {}

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
    _nav = st.radio("导航", ["🧭 导航", "🔥 热点新闻", "🏆 全选榜单", "🔍 个股搜索", "📊 日报", "📅 周报", "📈 大盘板块", "🔁 复盘"],
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
    st.caption("温度定仓位 → 轮动定板块 → 操作榜定标的")
    st.caption(_fresh_caption((_snap or {}).get("generated_at"), "行情快照") + " · 每小时更新")
    _meta0 = pub_meta()
    if _meta0.get("daily_report_ts"):
        st.caption(_fresh_caption(_meta0["daily_report_ts"], "日报/操作榜") + " · 每时段更新（07/14/21点）")
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
    # 🔥 最新热点（直接可见，详情见「🔥 热点新闻」页）
    try:
        _nl0 = json.loads(pub_text("news_live.json") or "{}")
        _its0 = _nl0.get("items") or []
        if _its0:
            st.markdown("**🔥 最新热点**（北京时间·实际发生时间）：")
            for _it0 in _its0[:5]:
                _d0 = _parse_ts(_it0.get("time"))
                _t0 = _d0.strftime("%H:%M") if _d0 else "--"
                st.markdown(f"- `{_t0}` {_it0.get('s','')}｜{str(_it0.get('t',''))[:60]}")
            st.caption("👉 完整实时新闻流见顶部「🔥 热点新闻」页（时段筛选/来源/链接）")
    except Exception:
        pass
    _ipb = _rep.find("## 💎 深度回调机会池")
    if _ipb > 0:
        _jpb = _rep.find("\n## ", _ipb + 5)
        with st.expander("💎 深度回调机会池（优质股·回撤≥30%·企稳信号）", expanded=False):
            st.markdown(_rep[_ipb:_jpb if _jpb > 0 else _ipb + 2500])
    _i = _rep.find("## 🎯 今日操作榜")
    if _i > 0:
        _j = _rep.find("## 二、", _i)
        with st.expander("🎯 今日操作榜（中长短×中美港 各Top3·实价校准）", expanded=True):
            st.markdown(_rep[_i:_j if _j > 0 else _i + 3000])
    st.caption("💡 持仓建议为隐私内容，请在飞书推送或 Mac/局域网 V88 查看")

# ── 🔥 热点新闻（每小时自动更新·每条带实际发生时间·三时段筛选）──────────
elif _nav == "🔥 热点新闻":
    st.markdown("#### 🔥 热点新闻 · 实时流")
    _nl_raw = pub_text("news_live.json")
    _nl = None
    if _nl_raw:
        try:
            _nl = json.loads(_nl_raw)
        except Exception:
            _nl = None
    if not _nl or not _nl.get("items"):
        st.info("📭 新闻流生成中（每小时自动抓取，可点右上 🔄 刷新）")
    else:
        st.caption(_fresh_caption(_nl.get("generated_at"), "新闻流") + " · 每小时自动更新 · 12个中外RSS源")
        _tps = _nl.get("topics") or []
        if _tps:
            st.markdown("**🔥 热点主题**：" + " · ".join(f"`{t['w']}({t['n']})`" for t in _tps))
        # 【V88·复制】新闻清单一键复制（前30条：时间｜来源｜标题）
        with st.popover("📋 复制新闻清单", use_container_width=True):
            _cp_news = "\n".join(f"{(it.get('time') or '')[-11:]}｜{it.get('s','')}｜{it.get('t','')}"
                                  for it in (_nl.get("items") or [])[:30])
            st.code(("🔥 热点主题：" + "、".join(f"{t['w']}({t['n']})" for t in _tps) + "\n\n" if _tps else "")
                    + _cp_news, language=None)
        _po = ["全部（近72小时）", "🌙 今日·时段一 00-08", "☀️ 今日·时段二 08-16", "🌆 今日·时段三 16-24"]
        _psel = st.selectbox("⏱ 时段筛选（北京时间）", _po)
        _today = _now_bjt().date()
        _shown = 0
        for _it in _nl["items"]:
            _dt = _parse_ts(_it.get("time"))
            if _psel != _po[0]:
                if not _dt or _dt.date() != _today:
                    continue
                _pidx = _po.index(_psel) - 1  # 0/1/2
                if _dt.hour // 8 != _pidx:
                    continue
            _tstr = _dt.strftime("%m-%d %H:%M") if _dt else "——"
            _ttl = str(_it.get("t", "")).replace("[", "［").replace("]", "］")
            _url = _it.get("url") or ""
            _line = (f"**`{_tstr}`** ｜ {_it.get('s','')} ｜ [{_ttl}]({_url})" if _url
                     else f"**`{_tstr}`** ｜ {_it.get('s','')} ｜ {_ttl}")
            st.markdown(_line)
            _shown += 1
            if _shown >= 50:
                break
        if _shown == 0:
            st.caption("该时段暂无新闻（时间为新闻实际发生时间·北京时间）")
        else:
            st.caption(f"共 {_shown} 条 · 时间均为新闻实际发生时间（北京时间）· 深度分析见 📊 日报（每时段生成一次）")

# ── 🏆 全选榜单（V88 最近一次「一键全策略」扫描结果，V88是主体·云端跟随）──
elif _nav == "🏆 全选榜单":
    st.markdown("#### 🏆 一键全策略榜单 · 中美港分市场排名")
    _scan_raw = pub_text("scan_latest.json")
    _scan = None
    if _scan_raw:
        try:
            _scan = json.loads(_scan_raw)
        except Exception:
            _scan = None
    if not _scan or not _scan.get("rows"):
        st.info("📭 暂无榜单（V88 桌面端跑过「一键全策略」后自动同步到这里，交易日 09:00/16:00/22:30 自动扫描）")
    else:
        import pandas as pd
        st.caption(_fresh_caption(_scan.get("generated_at"), "扫描榜单") + f"（{_scan.get('scan_market', '')}）· 由 Mac V88 五维引擎产出，云端只展示不重算")
        _df = pd.DataFrame(_scan["rows"])
        if "市场" in _df.columns and "得分" in _df.columns:
            _df["市场排名"] = (_df.groupby("市场")["得分"]
                            .rank(ascending=False, method="first").astype(int))
            _cols = _df.columns.tolist()
            _cols.insert(_cols.index("市场") + 1, _cols.pop(_cols.index("市场排名")))
            _df = _df[_cols]
        c_m, c_s = st.columns(2)
        with c_m:
            _mopts = ["🌍 全部"] + [m for m in ("🇺🇸美股", "🇨🇳A股", "🇭🇰港股")
                                   if "市场" in _df.columns and m in _df["市场"].unique()]
            _mp = st.selectbox("🌏 市场", _mopts)
            if _mp != "🌍 全部":
                _df = _df[_df["市场"] == _mp]
        with c_s:
            if "得分" in _df.columns:
                _sb = st.selectbox("📊 得分", ["全部", "≥70 强势", "≥55 良好", "≥40 及格"])
                _smap = {"≥70 强势": 70, "≥55 良好": 55, "≥40 及格": 40}
                if _sb in _smap:
                    _df = _df[_df["得分"] >= _smap[_sb]]
        st.download_button("📥 导出榜单CSV", data=_df.to_csv(index=False, encoding="utf-8-sig"),
                           file_name="V88全选榜单.csv", mime="text/csv", use_container_width=True)
        st.dataframe(_df, hide_index=True, use_container_width=True, height=560)
        st.caption("💡 得分=五维综合评分 ｜ MACD/量价：明显放量≥+20%·温和放量+8%~20%·持平±8%·明显缩量≤-20% ｜ 操作指引与止损/目标由引擎按实时价确定，与桌面 V88 同源")

# ── 🔍 个股搜索（云端实时·趋势脉搏）────────────────────────────
elif _nav == "🔍 个股搜索":
    st.markdown("#### 🔍 个股搜索 · 买卖前必看")
    st.caption("中文名（A股全市场5200+/港股/美股离线秒搜）或代码（AAPL｜0700｜600519）· 重名会列出让你选")
    import cloud_engine
    if not cloud_engine._load_names():
        st.warning("⚠️ 离线名录加载失败（stock_names.json），中文名搜索暂不可用，请直接输代码或稍后重试")
    _code = st.text_input("股票代码", value="", placeholder="中烟香港 / 腾讯 / 茅台 / AAPL / 600519",
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
            # 【V88·拐点识别】放量+破趋势=拐点，直接亮出证据与判断提示词
            _turn = f.get("turning") or {}
            if _turn.get("side"):
                (st.error if _turn["side"] == "top" else st.info)(
                    f"**{_turn['label']}**：" + "；".join(_turn["signals"]) + f"\n\n👉 {_turn['prompt']}")
            # 【V88·明白话判读】量价/K线/MACD 事实与要点（不是分数）
            _ro = cloud_engine.plain_readout(f, _turn if _turn.get("side") else None)
            if _ro:
                with st.expander("📖 量价判读（事实+要点，你来拍板）", expanded=True):
                    st.markdown("\n".join(f"- {ln}" for ln in _ro))

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
            _pl = cloud_engine.horizon_plans(f, _df)
            if _pl:
                st.markdown("##### ⏱ 分期限剧本（短线做T｜中线锚MA55｜长线锚年线）")
                st.markdown("\n".join(f"- {_pl[k]}" for k in ("short", "mid", "long") if _pl.get(k)))
            # 【V88·复制纪要】整段分析一键复制（st.code 自带复制按钮，微信/笔记直接粘贴）
            _cp_txt = cloud_engine.analysis_text(r.get('name') or r['symbol'], r['symbol'], f, r.get('asof', ''))
            if _cp_txt:
                with st.expander("📋 复制分析纪要（右上角复制按钮）", expanded=False):
                    st.code(_cp_txt, language=None)

# ── 📊 日报 / 📅 周报 ────────────────────────────────────────
elif _nav in ("📊 日报", "📅 周报"):
    _txt = pub_text("daily_report.md" if _nav == "📊 日报" else "weekly_report.md")
    if _txt:
        _meta1 = pub_meta()
        _mk1 = "daily_report_ts" if _nav == "📊 日报" else "weekly_report_ts"
        if _meta1.get(_mk1):
            st.caption(_fresh_caption(_meta1[_mk1], "本报告")
                       + (" · 每时段更新（北京时间07/14/21点）" if _nav == "📊 日报" else " · 每周日更新"))
        # 【V88·复制】下载 md 原文 + 复制全文（st.code 右上角自带复制按钮）
        _cc1, _cc2 = st.columns(2)
        _fn = "V88日报.md" if _nav == "📊 日报" else "V88周报.md"
        _cc1.download_button("📥 下载原文", data=_txt, file_name=_fn,
                             mime="text/markdown", use_container_width=True)
        with _cc2.popover("📋 复制全文", use_container_width=True):
            st.code(_txt[:12000], language=None)
        st.markdown(_txt)
        st.caption("💡 持仓建议为隐私内容，不在云端公开显示；见飞书或 Mac 版")
    else:
        st.info(_NOT_READY if _nav == "📊 日报" else "📅 周报每周日生成")

# ── 📈 大盘板块 ──────────────────────────────────────────────
elif _nav == "📈 大盘板块":
    st.markdown("#### 📈 大盘走势与板块轮动")
    if _snap:
        st.caption(_fresh_caption(_snap.get("generated_at"), "行情快照") + " · 每小时自动更新")
        for mkt, blk in _snap.get("markets", {}).items():
            st.markdown(f"### {mkt}")
            _t = blk.get("temperature")
            if _t:
                st.markdown(f"🌡 温度 **{_t['temp']}/100** {_t['label']}（趋势{_t['trend']}/宽度{_t['breadth']}/动量{_t['momentum']}/量能{_t.get('vol_heat','—')}）→ 仓位 {_t['position']}")
            for ix in blk.get("indices", []):
                st.markdown(f"- **{ix['name']}** {ix['last']}（5日 {ix['chg5d']:+.1f}% / 20日 {ix['chg20d']:+.1f}%）｜{ix['trend']}"
                            + (f"｜**{ix['turning']}**" if ix.get("turning") else ""))
                if ix.get("turning_prompt"):
                    st.caption(f"🔀 拐点详情：{ix['turning_prompt']}")
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
