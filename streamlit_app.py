"""
V88 云端版（Streamlit Community Cloud 专用查看器）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
24小时在线，与 Mac 开关机无关。数据来自私有仓 v88-daily-report
（GitHub Actions 每个交易日 07:00/14:00/21:00 自动更新）。

Secrets 需配置（App Settings → Secrets）:
  GH_TOKEN     = "ghp_xxx"   # 读私有数据仓
  APP_PASSWORD = "xxxx"      # 访问密码（网址泄露也进不来）
"""
import json
import time
import requests
import streamlit as st

DATA_REPO = "bluestevenr-lang/v88-daily-report"

st.set_page_config(page_title="V88 云端版", page_icon="☁️", layout="centered",
                   initial_sidebar_state="collapsed")

# ── 访问密码门 ───────────────────────────────────────────────
# 全部转成字符串再比较：Secrets 里写 5630 不带引号会变成数字，防呆处理
_pw = str(st.secrets.get("APP_PASSWORD", "") or "").strip()
if _pw:
    if not st.session_state.get("_auth_ok"):
        st.title("☁️ V88 云端版")
        _in = st.text_input("访问密码", type="password")
        if st.button("进入", type="primary", use_container_width=True):
            if str(_in).strip() == _pw:
                st.session_state["_auth_ok"] = True
                st.rerun()
            else:
                st.error("密码错误")
        st.stop()

# ── 私有数据仓读取 ───────────────────────────────────────────
# 【V98.2】只缓存成功结果：失败抛异常(st.cache_data 不缓存异常)，
# 避免"文件刚入库但 App 还在缓存 10 分钟前的失败"这种假性未就绪。
class _GhErr(Exception):
    def __init__(self, code):
        self.code = code

import re as _re
def _tok():
    # 只保留合法令牌字符 [A-Za-z0-9_]，自动清掉粘贴时混入的空格/换行/引号/智能引号/零宽字符
    raw = str(st.secrets.get("GH_TOKEN", "") or "")
    return "".join(_re.findall(r"[A-Za-z0-9_]", raw))

@st.cache_data(ttl=600, show_spinner=False)
def _gh_fetch(path: str, raw: bool = True) -> str:
    r = requests.get(f"https://api.github.com/repos/{DATA_REPO}/contents/{path}",
                     headers={"Authorization": f"Bearer {_tok()}",
                              "Accept": "application/vnd.github.raw" if raw else "application/vnd.github+json"},
                     timeout=15)
    if r.status_code != 200:
        raise _GhErr(r.status_code)
    return r.text

def gh_text(path: str):
    try:
        return _gh_fetch(path, True)
    except _GhErr as e:
        st.session_state.setdefault("_gh_errs", {})[path] = e.code
        return None
    except Exception:
        st.session_state.setdefault("_gh_errs", {})[path] = 0
        return None

def gh_listdir(path: str):
    try:
        return [x["name"] for x in json.loads(_gh_fetch(path, False))]
    except _GhErr as e:
        st.session_state.setdefault("_gh_errs", {})[path] = e.code
        return []
    except Exception:
        return []

def gh_diag(path: str, what: str = "数据"):
    """读取失败时给出具体原因+解法，并提供强制刷新（清缓存重拉）。"""
    code = st.session_state.get("_gh_errs", {}).get(path)
    if code in (401, 403):
        st.error(f"🔑 GH_TOKEN 无效或过期（HTTP {code}）。去 App 右下 Manage app → Settings → Secrets 更新 GH_TOKEN。")
    elif code == 404:
        st.info(f"📭 {what}还没入库（每交易日 07:00/14:00/21:00 自动生成，生成后自动出现）")
    else:
        st.warning(f"🌐 {what}读取失败（网络波动），点下方刷新重试")
    if st.button("🔄 强制刷新", key=f"rf_{path}"):
        _gh_fetch.clear()
        st.session_state.pop("_gh_errs", None)
        st.rerun()

st.title("☁️ V88 云端版")
st.caption("24小时在线 · 数据每交易日 07:00/14:00/21:00 自动更新 · 与 Mac 开关机无关")

_t_check = _tok()
# 只在明显没填时拦（<20 字符不可能是真令牌）；其余一律放行，让真实请求当裁判
if len(_t_check) < 20:
    st.error("🔑 GH_TOKEN 未配置或过短。去 Manage app → Settings → Secrets，"
             "把 GH_TOKEN 换成 gho_/ghp_ 开头的真实令牌，保存后等 1 分钟刷新。")
    st.stop()

_nav = st.radio("导航", ["🧭 导航", "📊 日报", "📅 周报", "📈 大盘板块", "💼 持仓", "🔁 复盘"],
                horizontal=True, label_visibility="collapsed")

_snap_raw = gh_text("data/market_snapshot.json")
_snap = None
if _snap_raw:
    try:
        _snap = json.loads(_snap_raw)
    except Exception:
        _snap = None

# ── 🧭 导航 ─────────────────────────────────────────────────
if _nav == "🧭 导航":
    _rep = gh_text("data/daily_report.md") or ""
    st.markdown("#### 🧭 今日导航 · 该关注什么")
    st.caption(f"温度定仓位 → 轮动定板块 → 操作榜定标的 → 持仓提醒定纪律 ｜ 数据 {(_snap or {}).get('generated_at', '—')}")
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
        gh_diag("data/market_snapshot.json", "大盘快照")
    _i = _rep.find("## 🎯 今日操作榜")
    if _i > 0:
        _j = _rep.find("## 二、", _i)
        with st.expander("🎯 今日操作榜（价位已实价校准）", expanded=False):
            st.markdown(_rep[_i + len("## 🎯 今日操作榜"):_j if _j > 0 else _i + 2500])
    _k = _rep.find("## 💼 我的持仓·框架化建议")
    if _k > 0:
        _alerts = []
        for _ln in _rep[_k:_k + 2500].splitlines():
            if any(x in _ln for x in ("⚠️", "🛑", "🔔")) and "|" in _ln:
                _pp = [x.strip() for x in _ln.split("|") if x.strip()]
                if len(_pp) >= 7:
                    _alerts.append(f"- **{_pp[0]}**：{_pp[-1]}")  # 最后一列=框架行动,防列数变化
        st.markdown("**⚡ 持仓触发提醒**" if _alerts else "**⚡ 持仓触发提醒**：今日无触发 ✅")
        if _alerts:
            st.markdown("\n".join(_alerts[:6]))

# ── 📊 日报 / 📅 周报 ────────────────────────────────────────
elif _nav in ("📊 日报", "📅 周报"):
    _f = "data/daily_report.md" if _nav == "📊 日报" else "data/weekly_report.md"
    _txt = gh_text(_f)
    if _txt:
        st.markdown(_txt)
    else:
        gh_diag(_f, "报告")

# ── 📈 大盘板块 ──────────────────────────────────────────────
elif _nav == "📈 大盘板块":
    st.markdown("#### 📈 大盘走势与板块轮动")
    if _snap:
        st.caption(f"📅 快照生成于 {_snap.get('generated_at', '?')}")
        for mkt, blk in _snap.get("markets", {}).items():
            st.markdown(f"### {mkt}")
            _t = blk.get("temperature")
            if _t:
                st.markdown(f"🌡 温度 **{_t['temp']}/100** {_t['label']}（趋势{_t['trend']}/宽度{_t['breadth']}/动量{_t['momentum']}）→ 仓位 {_t['position']}")
            for ix in blk.get("indices", []):
                st.markdown(f"- **{ix['name']}** {ix['last']}（5日 {ix['chg5d']:+.1f}% / 20日 {ix['chg20d']:+.1f}%）｜{ix['trend']}")
            secs = blk.get("sectors", [])
            if secs:
                top = sorted(secs, key=lambda x: x["chg5d"], reverse=True)
                st.markdown("**板块（近5日）**：领涨 " + "、".join(f"{s['name']} {s['chg5d']:+.1f}%" for s in top[:3])
                            + (" ｜ 落后 " + "、".join(f"{s['name']} {s['chg5d']:+.1f}%" for s in top[-3:][::-1]) if len(top) > 5 else ""))
    else:
        gh_diag("data/market_snapshot.json", "大盘快照")

# ── 💼 持仓 ─────────────────────────────────────────────────
elif _nav == "💼 持仓":
    st.markdown("#### 💼 我的持仓")
    _pos_raw = gh_text("positions.json")
    if _pos_raw:
        try:
            _d = json.loads(_pos_raw)
            for _acc, _info in _d.get("accounts", {}).items():
                _hs = _info.get("holdings", [])
                st.markdown(f"**{_acc}**（{len(_hs)}只）")
                st.dataframe([{k: v for k, v in h.items() if not k.startswith("_")} for h in _hs],
                             hide_index=True, use_container_width=True)
        except Exception as e:
            st.error(f"解析失败: {e}")
        _rep = gh_text("data/daily_report.md") or ""
        _k = _rep.find("## 💼 我的持仓·框架化建议")
        if _k > 0:
            st.markdown("---")
            st.markdown(_rep[_k:_k + 3000])
    else:
        gh_diag("positions.json", "持仓数据")

# ── 🔁 复盘 ─────────────────────────────────────────────────
elif _nav == "🔁 复盘":
    st.markdown("#### 🔁 推荐复盘 · 说话要算数")
    _files = sorted([f for f in gh_listdir("journal") if f.endswith(".json")])
    if not _files:
        st.info("暂无复盘存档")
    else:
        st.caption(f"已存档 {len(_files)} 天（{_files[0][:-5]} ~ {_files[-1][:-5]}）")
        _sel = st.selectbox("选择日期", list(reversed(_files)))
        _raw = gh_text(f"journal/{_sel}")
        if _raw:
            try:
                _d = json.loads(_raw)
                st.dataframe(_d.get("picks", []), hide_index=True, use_container_width=True)
            except Exception:
                st.code(_raw[:800])
        st.caption("完整收益核算与命中率见每周日推送的周报「🔁 推荐复盘」章节")

st.divider()
st.caption("V88 云端版 · 仅供研究参考，不构成投资建议")
