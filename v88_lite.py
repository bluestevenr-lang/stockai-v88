"""
v88_lite.py — V88 轻量版（手机 / 云端 24 小时可访问）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
设计目标：Mac 关机时也能在手机/Win 上看 V88 主要内容。部署到 VPS 常开。
内容：① 日报/周报  ② 大盘走势+板块轮动  ③ 个股搜索  ④ 猎手战位(一键全策略扫描)
评分引擎：headless 复用桌面版 app_v88_integrated 的真引擎（streamlit 桩 + 引擎守卫），
         保证与桌面版评分/操作指引【完全一致】，不重复实现、不产生分叉。

启动：streamlit run v88_lite.py --server.address 0.0.0.0 --server.port 8600
"""

import os
import sys
import json
import time
import types
import importlib.util
from pathlib import Path

import streamlit as st  # 真 streamlit：给轻量页自己的 UI 用

# ── 路径 ────────────────────────────────────────────────────────
STOCKAI_DIR = Path(__file__).resolve().parent
APP_FILE = STOCKAI_DIR / "app_v88_integrated.py"
# 日报/周报/快照由 ai-daily-report-v2 生成（Mac 与 VPS 路径都试）
REPORT_DIRS = [
    Path.home() / "Desktop" / "ai-daily-report-v2" / "data",
    Path("/root/ai-daily-report-v2/data"),
]


def _report_path(name: str):
    for d in REPORT_DIRS:
        p = d / name
        if p.exists():
            return p
    return None


def _repo_root():
    """ai-daily-report-v2 仓库根（journal/ 复盘存档在根目录）"""
    for d in REPORT_DIRS:
        if d.parent.exists():
            return d.parent
    return None


st.set_page_config(page_title="V88 轻量版", page_icon="📡", layout="centered",
                   initial_sidebar_state="collapsed")


# ═══════════════════════════════════════════════════════════════
# 引擎加载：streamlit 桩 + V88_ENGINE_ONLY 守卫，headless 载入桌面真引擎
# @st.cache_resource：整个服务进程只载一次（约 50 秒），之后秒开
# ═══════════════════════════════════════════════════════════════
class _MockCtx:
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def __call__(self, *a, **k): return self
    def __iter__(self): return iter([_MockCtx() for _ in range(4)])
    def __getattr__(self, n): return _MockCtx()


class _SessionState(dict):
    def __getattr__(self, n): return self.get(n)
    def __setattr__(self, n, v): self[n] = v


def _attach(fn):
    fn.clear = lambda *a, **k: None
    return fn


class _CacheDeco:
    def __call__(self, *a, **k):
        if len(a) == 1 and callable(a[0]) and not k:
            return _attach(a[0])
        return lambda fn: _attach(fn)
    def clear(self, *a, **k): return None


class _MockST(types.ModuleType):
    def __init__(self):
        super().__init__("streamlit")
        self.session_state = _SessionState()
        self.columns = lambda spec, *a, **k: [_MockCtx() for _ in range(spec if isinstance(spec, int) else len(spec))]
        self.tabs = lambda labels, *a, **k: [_MockCtx() for _ in range(len(labels))]
        self.cache_data = _CacheDeco()
        self.cache_resource = _CacheDeco()
        self.fragment = _CacheDeco()
        self.secrets = {}
        # 取值类控件：返回合理默认（避免返回值被当字符串/迭代）
        self.text_input = lambda *a, **k: (k.get("value") or "")
        self.text_area = lambda *a, **k: (k.get("value") or "")
        self.chat_input = lambda *a, **k: ""
        self.number_input = lambda *a, **k: (k.get("value") or 0)
        self.slider = lambda *a, **k: (k.get("value") or 0)
        self.checkbox = lambda *a, **k: bool(k.get("value", False))
        self.toggle = lambda *a, **k: bool(k.get("value", False))
        self.button = lambda *a, **k: False
        self.form_submit_button = lambda *a, **k: False
        self.download_button = lambda *a, **k: False
        self.multiselect = lambda *a, **k: (k.get("default") or [])
        self.date_input = lambda *a, **k: None
        self.time_input = lambda *a, **k: None
        self.file_uploader = lambda *a, **k: None
        self.color_picker = lambda *a, **k: "#000000"
        self.radio = self._pick
        self.selectbox = self._pick

    @staticmethod
    def _pick(label=None, options=None, *a, **k):
        try:
            return list(options)[k.get("index", 0)] if options else None
        except Exception:
            return None

    def __getattr__(self, n):
        return _MockCtx()


@st.cache_resource(show_spinner="⏳ 首次加载评分引擎（约 50 秒，仅一次）...")
def load_engine():
    """用桩 streamlit + 引擎守卫，headless 载入桌面真引擎，返回模块对象。"""
    real_st = sys.modules.get("streamlit")
    sys.modules["streamlit"] = _MockST()
    os.environ["V88_ENGINE_ONLY"] = "1"
    if str(STOCKAI_DIR) not in sys.path:
        sys.path.insert(0, str(STOCKAI_DIR))
    try:
        spec = importlib.util.spec_from_file_location("v88engine", APP_FILE)
        mod = importlib.util.module_from_spec(spec)
        sys.modules["v88engine"] = mod
        try:
            spec.loader.exec_module(mod)
        except Exception as e:
            if type(e).__name__ != "_V88EngineReady":
                raise
        return mod
    finally:
        # 还原真 streamlit 给轻量页自己的 UI；引擎内部仍用桩(其内部 st.* 皆 no-op)
        if real_st is not None:
            sys.modules["streamlit"] = real_st


# ═══════════════════════════════════════════════════════════════
# UI
# ═══════════════════════════════════════════════════════════════
st.title("📡 V88 轻量版")
st.caption("手机 / 云端随时查看 · 评分与桌面版完全一致")

_nav = st.radio("导航", ["🧭 导航", "📊 日报", "🔍 个股搜索", "📡 猎手战位", "🎯 三期限", "📈 大盘板块", "🔁 复盘"],
                horizontal=True, label_visibility="collapsed")


# ── ⓪ 今日导航（与桌面版同款：温度→水位→轮动→操作榜→持仓提醒）──────
if _nav == "🧭 导航":
    _snap = None
    _p = _report_path("market_snapshot.json")
    if _p:
        try:
            _snap = json.loads(_p.read_text(encoding="utf-8"))
        except Exception:
            _snap = None
    _rep = ""
    _rp = _report_path("daily_report.md")
    if _rp:
        _rep = _rp.read_text(encoding="utf-8-sig")

    st.markdown("#### 🧭 今日导航 · 该关注什么")
    st.caption(f"温度定仓位 → 水位定方向 → 轮动定板块 → 操作榜定标的 → 持仓提醒定纪律 ｜ 数据 {(_snap or {}).get('generated_at','—')}")

    if _snap and _snap.get("markets"):
        # 🌡 温度计
        for _mkt in ("美股", "A股", "港股"):
            _t = (_snap["markets"].get(_mkt) or {}).get("temperature")
            if _t:
                st.markdown(f"🌡 **{_mkt} {_t['temp']}/100** {_t['label']} → 仓位 **{_t['position']}**")
        st.markdown("---")
        # 指数水位（每市场一行紧凑）
        for _mkt in ("美股", "A股", "港股"):
            _blk = _snap["markets"].get(_mkt) or {}
            _ixs = _blk.get("indices") or []
            if _ixs:
                _line = " ｜ ".join(f"{ix['trend'].split()[0]}{ix['name']} {ix['chg5d']:+.1f}%" for ix in _ixs[:3])
                st.markdown(f"**{_mkt}**：{_line}")
        # 轮动提醒（5日 vs 20日排名跃迁）
        _hints = []
        for _mkt in ("美股", "A股", "港股"):
            _secs = ((_snap["markets"].get(_mkt) or {}).get("sectors")) or []
            if len(_secs) < 4:
                continue
            _r5 = {s["symbol"]: i for i, s in enumerate(sorted(_secs, key=lambda x: -x["chg5d"]))}
            _r20 = {s["symbol"]: i for i, s in enumerate(sorted(_secs, key=lambda x: -x["chg20d"]))}
            _jump = max(2, len(_secs) // 3)
            for s in _secs:
                _d = _r20[s["symbol"]] - _r5[s["symbol"]]
                if _d >= _jump and s["chg5d"] > 0:
                    _hints.append(f"🔥 {_mkt}·{s['name']} 轮入({s['chg5d']:+.1f}%)")
                elif _d <= -_jump and s["chg20d"] > 0:
                    _hints.append(f"🧊 {_mkt}·{s['name']} 退潮")
        if _hints:
            st.markdown("**板块轮动**：" + " ｜ ".join(_hints[:5]))
        _rot_lite = (_snap or {}).get("rotation_forecast") or {}
        if _rot_lite:
            st.markdown("**🧭 下一轮板块轮转预警（明日 · 下周 · 半个月）**")
            from rotation_ui import rotation_map_html as _rotation_map_html_lite
            st.markdown(_rotation_map_html_lite(_rot_lite, "v88-lite-nav-rotation"), unsafe_allow_html=True)
    else:
        st.info("📭 大盘快照未生成（日报流水线每日 07:00/14:00/21:00 更新）")

    # 操作榜 + 持仓提醒（来自日报）
    _i = _rep.find("## 🎯 今日操作榜")
    if _i > 0:
        _j = _rep.find("## 二、", _i)
        with st.expander("🎯 今日操作榜（短线/长线 Top3 · 价位已实价校准）", expanded=False):
            st.markdown(_rep[_i + len("## 🎯 今日操作榜"):_j if _j > 0 else _i + 2500])
    _k = _rep.find("## 💼 我的持仓·框架化建议")
    if _k > 0:
        _analysis_ts = ""
        try:
            import re as _re_time
            _mt = _re_time.search(r"分析生成(?:时间)?[：:]\s*([^｜\n]+)", _rep[:1200])
            _analysis_ts = (_mt.group(1).replace("**", "").strip() if _mt else "")
        except Exception:
            pass
        _alerts = []
        for _ln in _rep[_k:_k + 2500].splitlines():
            if any(x in _ln for x in ("⚠️", "🛑", "🔔")) and "|" in _ln:
                _pp = [x.strip() for x in _ln.split("|") if x.strip()]
                if len(_pp) >= 7:
                    _alerts.append(f"- **{_pp[0]}**：{_pp[-1]}")  # 最后一列=框架行动,防列数变化
        _time_note = f" · 🕒 分析于 {_analysis_ts}" if _analysis_ts else " · 🕒 分析时间未知"
        st.markdown(("**⚡ 持仓触发提醒**" if _alerts else "**⚡ 持仓触发提醒**：今日无触发 ✅") + _time_note)
        if _alerts:
            st.markdown("\n".join(_alerts[:6]))


# ── ① 日报 / 周报 ────────────────────────────────────────────────
elif _nav == "📊 日报":
    import datetime as _dt
    is_weekend = _dt.date.today().weekday() >= 5
    fname = "weekly_report.md" if is_weekend else "daily_report.md"
    p = _report_path(fname) or _report_path("daily_report.md")
    if p:
        content = p.read_text(encoding="utf-8-sig")
        mtime = time.strftime("%Y-%m-%d %H:%M", time.localtime(p.stat().st_mtime))
        st.caption(f"📅 {'周报' if fname.startswith('weekly') else '日报'} · 更新于 {mtime}")
        st.markdown(content)
    else:
        st.warning("暂无日报文件。请确认日报流水线已运行（VPS 上的 ai-daily-report-v2）。")


# ── ② 个股搜索 ───────────────────────────────────────────────────
elif _nav == "🔍 个股搜索":
    st.markdown("#### 🔍 个股搜索")
    st.caption("输入代码：美股 AAPL｜港股 0700｜A股 600519 / 000001")
    code_in = st.text_input("股票代码", value="", placeholder="AAPL / 0700 / 600519").strip()
    go = st.button("📊 分析", use_container_width=True, type="primary")

    if go and code_in:
        eng = load_engine()
        with st.spinner(f"分析 {code_in} 中..."):
            try:
                c = eng.to_yf_cn_code(code_in)
                df = eng.fetch_stock_data(c)
                if df is None or len(df) < 20:
                    st.error("未取到足够行情数据，请检查代码或稍后重试。")
                else:
                    m = eng.calculate_metrics_all(df, c)
                    if not m:
                        st.error("指标计算失败。")
                    else:
                        score = int(m["score"])
                        last = float(m["last_price"])
                        # 52周位置
                        try:
                            l250 = float(m["df"]["Low"].tail(250).min())
                            h250 = float(m["df"]["High"].tail(250).max())
                            pos_pct = (last - l250) / (h250 - l250) * 100 if h250 > l250 else 50.0
                        except Exception:
                            pos_pct = 50.0
                        action, stop_target = eng.build_action_guidance(
                            score, m.get("rs20"), pos_pct, 0, last, m.get("trade_plan"))

                        c1, c2, c3 = st.columns(3)
                        c1.metric("综合评分", f"{score}/100")
                        c2.metric("现价", f"{last:.2f}")
                        rs = m.get("rs20")
                        c3.metric("RS强度", f"{rs:+.1f}" if rs is not None else "—",
                                  help="近月跑赢大盘幅度，正=领涨")
                        c4, c5, c6 = st.columns(3)
                        c4.metric("RSI", f"{int(m.get('rsi', 0) or 0)}")
                        c5.metric("20日动量", f"{m.get('chg20d', 0) or 0:+.1f}%")
                        c6.metric("ESG", f"{m.get('esg_grade', 'N/A')}")

                        st.markdown(f"### {action}")
                        st.info(f"止损/目标：{stop_target}")

                        # 【V99】综合量价趋势（三端同一 cloud_engine 引擎）
                        try:
                            import cloud_engine as _ce
                            _F = _ce.analyze_trend_full(df)
                            if _F:
                                st.markdown("---")
                                st.markdown(
                                    f"**一句话结论：{_F['conclusion']}** ｜ {_F['action']}\n\n"
                                    f"- 趋势总分 **{_F['total']}/100** ｜ 阶段 {_F['stage']}\n"
                                    f"- 量价 {_F['vp']} ｜ 水位 {_F['water']}({_F['pos52']}%)→{_F['water_adv']}\n"
                                    f"- MACD {_F['macd_txt']} ｜ 均线 {_F['ma_state']}\n"
                                    f"- 买入区 {_F['buy_zone']} ｜ 突破加仓 {_F['breakout']} ｜ 止损 {_F['stop']}\n"
                                    f"- 失效：{_F['invalid']}")
                        except Exception:
                            pass

                        # 五维评分归因（与桌面同源）
                        with st.expander("📐 五维评分归因（点开看分数来源）"):
                            st.caption("CANSLIM 成长因子")
                            st.dataframe(m.get("canslim_rows", []), hide_index=True, use_container_width=True)
                            st.caption("趋势/动能因子（含 MACD/动量/量能/RS）")
                            st.dataframe(m.get("spec_rows", []), hide_index=True, use_container_width=True)
            except Exception as e:
                st.error(f"分析失败：{type(e).__name__}: {str(e)[:120]}")


# ── ③ 猎手战位（一键全策略扫描）──────────────────────────────────
elif _nav == "📡 猎手战位":
    st.markdown("#### 📡 猎手战位 · 一键全策略扫描")
    mkt = st.radio("市场", ["美股", "港股", "A股"], horizontal=True)
    cap = st.select_slider("扫描数量（越多越慢）", options=[60, 100, 150, 200], value=100)
    run = st.button("🔍 开始扫描", use_container_width=True, type="primary")

    _cache_key = f"lite_scan_{mkt}_{cap}"
    if run:
        eng = load_engine()
        pool = {"美股": eng.RAW_US, "港股": eng.RAW_HK, "A股": eng.RAW_CN_TOP}[mkt][:cap]
        prog = st.progress(0.0)
        status = st.empty()

        def _cb(cur, total, name):
            prog.progress(min(cur / total, 1.0))
            status.text(f"扫描中 {cur}/{total} · {name}")

        try:
            rows, stats, meta = eng.run_unified_scan(pool, mkt, progress_callback=_cb)
            prog.empty(); status.empty()
            st.session_state[_cache_key] = {"rows": rows, "meta": meta, "ts": time.time(),
                                            "ok": stats.get("success", 0), "fail": stats.get("failed", 0)}
        except Exception as e:
            prog.empty(); status.empty()
            st.error(f"扫描失败：{type(e).__name__}: {str(e)[:120]}")

    if _cache_key in st.session_state:
        d = st.session_state[_cache_key]
        age = (time.time() - d["ts"]) / 60
        st.caption(f"✅ 成功 {d['ok']} · 失败 {d['fail']} · 市场状态 {d['meta'].get('regime','N/A')} "
                   f"· {age:.0f} 分钟前")
        st.caption("按得分降序；操作指引=可执行动作+价位；RS强度正=领涨")
        st.dataframe(d["rows"], hide_index=True, use_container_width=True,
                     column_config={"得分": st.column_config.ProgressColumn("得分", format="%d", min_value=0, max_value=100)})


# ── ③b 三期限选股（短/中/长线 Top30，与桌面版同一引擎同一公式）────
elif _nav == "🎯 三期限":
    st.markdown("#### 🎯 三期限选股 · 短/中/长线各 Top30")
    st.caption("中美港每市场各10只 · 短线=动能45%+RS30%+综合25% ｜ 中线=综合45%+动能30%+趋势25% ｜ 长线=综合40%+年线30%+低波动30%")
    if st.button("🚀 生成（约400只池，2-3分钟）", use_container_width=True, type="primary"):
        eng = load_engine()
        prog = st.progress(0.0)
        status = st.empty()
        _t0 = time.time()

        def _cb(cur, total, name):
            prog.progress(min(cur / max(1, total), 1.0))
            _el = time.time() - _t0
            _eta = (_el / cur * (total - cur)) if cur > 3 else 0
            status.text(f"⏱ 已用 {_el:.0f}s · 预计剩余 {_eta:.0f}s ｜ {cur}/{total} - {name}")

        try:
            _hz = eng.run_horizon_top10(progress_callback=_cb)
            prog.empty(); status.empty()
            st.session_state["lite_hz"] = {"ts": time.time(), **_hz}
        except Exception as e:
            prog.empty(); status.empty()
            st.error(f"生成失败：{type(e).__name__}: {str(e)[:120]}")

    if "lite_hz" in st.session_state:
        _d = st.session_state["lite_hz"]
        st.caption(f"生成于 {(time.time()-_d['ts'])/60:.0f} 分钟前")
        _ts, _tm, _tl2 = st.tabs(["⚡ 短线Top30", "🚀 中线Top30", "🏛 长线Top30"])
        for _tab, _key in ((_ts, "short"), (_tm, "mid"), (_tl2, "long")):
            with _tab:
                _arr = _d.get(_key, [])
                if _arr:
                    st.dataframe(_arr, hide_index=True, use_container_width=True,
                                 column_config={"期限分": st.column_config.ProgressColumn("期限分", format="%d", min_value=0, max_value=100)})
                else:
                    st.info("暂无合格标的")


# ── ⑤ 复盘（journal/ 存档 + 近7日推荐收益与命中率）────────────────
elif _nav == "🔁 复盘":
    st.markdown("#### 🔁 推荐复盘 · 说话要算数")
    _root = _repo_root()
    _jdir = (_root / "journal") if _root else None
    _files = sorted(_jdir.glob("*.json")) if (_jdir and _jdir.exists()) else []
    if not _files:
        st.info("暂无复盘存档（每个交易日日报生成后自动存档到 journal/）")
    else:
        st.caption(f"已存档 {len(_files)} 天（{_files[0].stem} ~ {_files[-1].stem}）")
        _latest = json.loads(_files[-1].read_text(encoding="utf-8"))
        st.markdown(f"**最新存档 {_latest.get('date')}**（{len(_latest.get('picks', []))} 条推荐）")
        st.dataframe(_latest.get("picks", []), hide_index=True, use_container_width=True)
        if len(_files) > 1 and st.button("📈 计算近7日推荐收益与命中率（拉实时价）", use_container_width=True):
            with st.spinner("拉取实时行情核算中..."):
                try:
                    sys.path.insert(0, str(_root / "src"))
                    from review_log import build_review_section
                    _md = build_review_section(days=7)
                    st.markdown(_md or "近7日暂无可核算的历史推荐（需存档满1天以上）")
                except Exception as e:
                    st.error(f"核算失败：{type(e).__name__}: {str(e)[:120]}")


# ── ④ 大盘板块 ──────────────────────────────────────────────────
elif _nav == "📈 大盘板块":
    st.markdown("#### 📈 大盘走势与板块轮动")
    p = _report_path("market_snapshot.json")
    if p:
        snap = json.loads(p.read_text(encoding="utf-8"))
        st.caption(f"📅 快照生成于 {snap.get('generated_at','?')}")
        if snap.get("rotation_forecast"):
            from rotation_ui import rotation_map_html as _rotation_map_html_lite_market
            st.markdown(_rotation_map_html_lite_market(snap["rotation_forecast"], "v88-lite-market-rotation"), unsafe_allow_html=True)
        for mkt, blk in snap.get("markets", {}).items():
            st.markdown(f"### {mkt}")
            _t = blk.get("temperature")
            if _t:
                st.markdown(f"🌡 温度 **{_t['temp']}/100** {_t['label']}（趋势{_t['trend']}/宽度{_t['breadth']}/动量{_t['momentum']}）→ 仓位 {_t['position']}")
            for ix in blk.get("indices", []):
                st.markdown(f"- **{ix['name']}** {ix['last']}（今日 {ix['chg1d']:+.1f}% / "
                            f"5日 {ix['chg5d']:+.1f}% / 20日 {ix['chg20d']:+.1f}%）｜{ix['trend']}")
            secs = blk.get("sectors", [])
            if secs:
                top = sorted(secs, key=lambda x: x["chg5d"], reverse=True)
                st.markdown("**板块强弱（近5日）**：领涨 "
                            + "、".join(f"{s['name']} {s['chg5d']:+.1f}%" for s in top[:3]))
                if len(top) > 5:
                    st.markdown("落后 " + "、".join(f"{s['name']} {s['chg5d']:+.1f}%" for s in top[-3:][::-1]))
    else:
        st.warning("暂无大盘快照。请确认日报流水线已运行（会生成 market_snapshot.json）。")

st.divider()
st.caption("V88 轻量版 · 数据仅供研究参考，不构成投资建议")
