"""
V88 云端版（Streamlit Community Cloud · 数据读取免令牌）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
24小时在线，与 Mac 开关机无关。数据从公开分支免令牌读取：
  https://raw.githubusercontent.com/bluestevenr-lang/stockai-v88/data/pub/
GitHub Actions 每交易日 07:00/14:00/21:00 自动发布（已剔除持仓明细，保护隐私）。
用户无需配置任何 Secrets；可选设 APP_PASSWORD 加一道访问密码。
"""
import hashlib
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

def _analysis_label(ts, what="分析"):
    """重点提示统一显示真正的分析生成时间，而不是页面打开/刷新时间。"""
    dt = _parse_ts(ts)
    if not dt:
        return f"🕒 {what}时间未知"
    return f"🕒 {what}于 {dt.strftime('%Y-%m-%d %H:%M')}（北京时间）"

@st.cache_data(ttl=15, show_spinner=False)
def pub_meta() -> dict:
    try:
        r = requests.get(f"{PUB_BASE}/meta.json", timeout=10)
        return r.json() if r.status_code == 200 else {}
    except Exception:
        return {}

st.set_page_config(page_title="V88 云端版", page_icon="☁️", layout="centered",
                   initial_sidebar_state="collapsed")

# 三端统一可读性：示例浅灰、实际值黑；白底按钮蓝字、蓝底按钮白字。
st.markdown("""
<style>
input, textarea { color:#111827!important; -webkit-text-fill-color:#111827!important; }
input::placeholder, textarea::placeholder {
  color:#9ca3af!important; -webkit-text-fill-color:#9ca3af!important; opacity:1!important;
}
[data-baseweb="select"], [data-baseweb="select"] > div { background:#fff!important; }
[data-baseweb="select"] *, [data-testid="stSelectbox"] * { color:#111827!important; }
button[kind="secondary"], button[data-testid="stBaseButton-secondary"] {
  background:#fff!important; border-color:#2563eb!important; color:#1d4ed8!important;
}
button[kind="secondary"] *, button[data-testid="stBaseButton-secondary"] * { color:#1d4ed8!important; }
button[kind="primary"], button[data-testid="stBaseButton-primary"] {
  background:#2563eb!important; border-color:#2563eb!important; color:#fff!important;
}
button[kind="primary"] *, button[data-testid="stBaseButton-primary"] * { color:#fff!important; }
button[kind="primary"]:disabled, button[data-testid="stBaseButton-primary"]:disabled {
  background:#fff!important; border:1px solid #2563eb!important; color:#1d4ed8!important; opacity:.62!important;
}
button[kind="primary"]:disabled *, button[data-testid="stBaseButton-primary"]:disabled * { color:#1d4ed8!important; }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=3600, show_spinner=False)
def _cloud_ath_many(codes: tuple) -> dict:
    """云端个股历史最高水位：距全历史最高百分比及相隔天数。"""
    from concurrent.futures import ThreadPoolExecutor
    import yfinance as yf

    def _one(code):
        raw = str(code).strip().upper()
        yc = raw.replace(".SH", ".SS")
        if yc.isdigit() and len(yc) == 6:
            yc += ".SS" if yc[0] in "569" else ".SZ"
        try:
            close = yf.Ticker(yc).history(period="max")["Close"].dropna()
            if len(close) < 6:
                return raw, "历史水位待核"
            last, ath = float(close.iloc[-1]), float(close.max())
            days = int((close.index[-1] - close.idxmax()).days)
            _w52 = close.tail(min(252, len(close)))
            _lo52, _hi52 = float(_w52.min()), float(_w52.max())
            _p52 = (last - _lo52) / (_hi52 - _lo52) * 100 if _hi52 > _lo52 else 50.0
            return raw, f"距历史最高{(last / ath - 1) * 100:+.1f}%｜高点相隔{days}天·52周{_p52:.0f}%"
        except Exception:
            return raw, "历史水位待核"

    with ThreadPoolExecutor(max_workers=min(8, max(1, len(codes)))) as ex:
        return dict(ex.map(_one, codes))

def _linkify_md(md: str) -> str:
    """【V88·全局个股可点击 v2】两件事：①个股名/token→内联链接（?q=深链）
    ②markdown表格整体转HTML表格——md表格单元格内的HTML前端渲染不可靠，HTML表格100%可点。"""
    import re as _re
    A = '<a href="?q={c}&focus=deep" target="_self" style="color:inherit;text-decoration:underline dotted 1px;">{t}</a>'

    def _link_inline(txt):
        txt = _re.sub(r"`?\[(US|SH|SZ|HK):([A-Za-z0-9\.\-]+)\]`?",
                      lambda m: A.format(c=m.group(2), t=f"[{m.group(1)}:{m.group(2)}]"), txt)
        txt = _re.sub(r"\*\*([\u4e00-\u9fffA-Za-z0-9\-·]{2,14})\*\*[（(]([A-Z0-9]{1,8}(?:\.[A-Z]{2})?)[）)]",
                      lambda m: "<b>" + A.format(c=m.group(2), t=m.group(1)) + f"</b>（{m.group(2)}）", txt)
        txt = _re.sub(r"(?<![>\w])([\u4e00-\u9fffA-Za-z0-9\-·]{2,14})[（(]([A-Z0-9]{1,8}(?:\.[A-Z]{2})?)[）)]",
                      lambda m: A.format(c=m.group(2), t=m.group(1)) + f"（{m.group(2)}）", txt)
        return txt

    def _row_cells(ln):
        return [c.strip() for c in ln.strip().strip("|").split("|")]

    out, i, lines = [], 0, md.splitlines()
    while i < len(lines):
        ln = lines[i]
        # 表格块：表头|分隔|数据行... → HTML表格
        if (ln.strip().startswith("|") and i + 1 < len(lines)
                and _re.match(r"^\s*\|[\s:\-|]+\|\s*$", lines[i + 1])):
            hdr = _row_cells(ln)
            i += 2
            rows = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                cells = _row_cells(lines[i])
                # token行：把名称列也链接化（token列的下一列）
                for k, c in enumerate(cells):
                    mt = _re.fullmatch(r"`?\[(US|SH|SZ|HK):([A-Za-z0-9\.\-]+)\]`?", c)
                    if mt and k + 1 < len(cells) and cells[k + 1] and "<a " not in cells[k + 1]:
                        cells[k + 1] = A.format(c=mt.group(2), t=cells[k + 1])
                rows.append([_link_inline(c) for c in cells])
                i += 1
            _md_b = lambda t: _re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", t)
            html = ['<table style="border-collapse:collapse;width:100%;font-size:0.9em;">',
                    "<tr>" + "".join(f'<th style="border:1px solid #ddd;padding:4px 8px;text-align:left;">{_md_b(h)}</th>' for h in hdr) + "</tr>"]
            for r in rows:
                html.append("<tr>" + "".join(f'<td style="border:1px solid #ddd;padding:4px 8px;">{_md_b(c)}</td>' for c in r) + "</tr>")
            html.append("</table>")
            out.append("".join(html))
            continue
        out.append(_link_inline(ln))
        i += 1
    return "\n".join(out)
def exp_md(title: str, md_text: str, expanded: bool = False):
    """【V88·段落复制】统一段落组件：折叠段右上角带📋复制（st.code自带复制按钮）"""
    with st.expander(title, expanded=expanded):
        _c1, _c2 = st.columns([6, 1])
        with _c2.popover("📋", use_container_width=True):
            st.code(md_text, language=None)
        st.markdown(_linkify_md(md_text), unsafe_allow_html=True)


# ── 【2026-07-13 用户要求·去掉代码密码门】访问控制改由 Streamlit 后台「邮箱授权」
# (share.streamlit.io → Settings → Sharing → Specific people) 承担，更省事。
# 无 PRIVATE_TOKEN 时云端只展示公开脱敏数据(日报/行情/榜单)，去密码不泄露持仓。
# ⚠️ 若要在云端显示持仓(配 PRIVATE_TOKEN)，务必先在后台设好邮箱授权，否则持仓对任何知道网址的人可见。


@st.cache_data(ttl=300, show_spinner=False)
def pub_text(name: str, publish_version: str = "legacy"):
    try:
        # publish_version参与Streamlit缓存键；新发布版本出现时旧缓存立即失效。
        r = requests.get(f"{PUB_BASE}/{name}?v={publish_version}", timeout=12)
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
st.caption("24小时在线 · 日报每交易日07:00/13:00/19:00更新 · 持仓盘中每15分钟风险快扫 · 访问权限由Streamlit部署设置控制")

c_nav, c_rf = st.columns([5, 1])
with c_rf:
    if st.button("🔄", help="强制刷新"):
        pub_meta.clear(); pub_text.clear(); pub_journal_list.clear(); st.rerun()
# 【V88·个股点击】跳转机制：任何页面点股票名→搜索页自动分析；支持 ?q=代码 深链
_jump = st.session_state.pop("_nav_jump", None)
if _jump:
    st.session_state["_nav"] = "🔍 个股搜索"
    st.session_state["_sch_cands"] = [_jump]
try:
    _qp = st.query_params.get("q")
    if _qp and st.session_state.get("_qp_done") != _qp:
        # 记录已处理的具体代码；布尔标记会导致同一会话点击第二只股票时不切换。
        st.session_state["_qp_done"] = _qp
        import cloud_engine as _ceq
        _sym = _ceq.to_yf(_qp)
        st.session_state["_nav"] = "🔍 个股搜索"
        st.session_state["_sch_cands"] = [(_ceq.name_of(_sym) or _qp, _sym, "")]
except Exception:
    pass

def jump_stock(name, code):
    try:
        import cloud_engine as _cej
        code = _cej.to_yf(str(code))  # SH:600519→600519.SS / 06055→6055.HK 等规格化
    except Exception:
        pass
    st.session_state["_nav_jump"] = (name, code, "")
    st.rerun()

with c_nav:
    _nav = st.radio("导航", ["🧭 导航", "🔥 热点新闻", "🏆 全选榜单", "🔍 个股搜索", "📊 日报", "📅 周报", "📈 大盘板块", "🛰️ 雷达族", "🔁 复盘", "💼 持仓终端"],
                    horizontal=True, label_visibility="collapsed", key="_nav")

_pub_state = pub_meta()
_PUB_VERSION = str(_pub_state.get("publish_version") or _pub_state.get("daily_report_ts") or "legacy")
_snap_raw = pub_text("market_snapshot.json", _PUB_VERSION)
_snap = None
if _snap_raw:
    try:
        _snap = json.loads(_snap_raw)
    except Exception:
        _snap = None

# ── 报告协议：日报绑定冻结快照，小时级实时快照可独立前进 ──────────────
def _json_text(name: str, publish_version: str = _PUB_VERSION) -> dict:
    try:
        return json.loads(pub_text(name, publish_version) or "{}")
    except Exception:
        return {}


_report_bundle = _json_text("report_bundle.json")
if (_report_bundle.get("schema_version") == "v88.report.bundle/1.0"
        and _report_bundle.get("report") and _report_bundle.get("manifest")):
    _report_manifest = _report_bundle.get("manifest") or {}
    _report_snapshot = _report_bundle.get("snapshot") or _snap or {}
    _source_ledger = _report_bundle.get("source_ledger") or {}
    _report_text = str(_report_bundle.get("report") or "")
else:
    # 兼容尚未发布原子包的旧数据分支。
    _report_manifest = _json_text("report_manifest.json")
    _report_snapshot = _json_text("report_snapshot.json") or _snap or {}
    _source_ledger = _json_text("source_ledger.json")
    _report_text = pub_text("daily_report.md", _PUB_VERSION) or ""
_contract_available = bool(_report_manifest)
_report_sync_ok = True
_report_block_reason = ""
if _contract_available:
    _quality = (_report_manifest.get("quality") or {}).get("status")
    _manifest_sid = _report_manifest.get("snapshot_id")
    _snapshot_sid = _report_snapshot.get("snapshot_id")
    _manifest_sha = _report_manifest.get("report_sha256")
    _report_sha = hashlib.sha256(_report_text.encode("utf-8")).hexdigest()
    if _quality not in ("passed", "plan_b"):
        _report_sync_ok = False
        _report_block_reason = "权威日报未通过硬质检"
    elif not _manifest_sid or _manifest_sid != _snapshot_sid:
        _report_sync_ok = False
        _report_block_reason = "日报与冻结行情快照仍在同步"
    elif not _manifest_sha or _manifest_sha != _report_sha:
        _report_sync_ok = False
        _report_block_reason = "日报正文与质量清单仍在同步"
else:
    _report_sync_ok = False
    _report_block_reason = "旧版日报缺少硬质检清单"

# 【V88·Plan A/B】Plan B必须是当天新闻+快照+榜单生成的纯观察版，禁止沿用历史日报。
_report_gen_date = str(_report_manifest.get("generated_at") or "")[:10]
_global_analysis_note = _analysis_label(_report_manifest.get("generated_at"), "报告分析")
_today_bj = (__import__("datetime").datetime.now(__import__("zoneinfo").ZoneInfo("Asia/Shanghai"))
            .strftime("%Y-%m-%d"))
if not _contract_available:
    st.warning(f"⚠️ 旧版日报缺少硬质检清单，交易建议暂不展示；下一次日报任务完成后自动升级。 · {_global_analysis_note}")
elif not _report_sync_ok:
    st.warning(f"⚠️ {_report_block_reason}，交易建议暂不展示；实时市场快照仍可查看。 · {_global_analysis_note}")
elif _quality == "plan_b":
    st.warning(f"🟡 Plan B当日安全版 · {_report_gen_date} · Snapshot `{_report_manifest.get('snapshot_id')}` · "
              f"基于今日新闻/行情/榜单生成，所有标的仅观察，并含明日与本周参考。 · {_global_analysis_note}")
elif _report_gen_date and _report_gen_date != _today_bj:
    st.warning(f"⚠️ 公开日报不是今日版本（{_report_gen_date}），暂不作为Plan B展示。 · {_global_analysis_note}")
else:
    st.caption(
        f"✅ 报告硬质检通过(Plan A) · Snapshot `{_report_manifest.get('snapshot_id')}` · "
        f"来源 {_report_manifest.get('source_count', 0)} 条"
    )

_NOT_READY = "📭 数据生成中（每交易日 07:00/14:00/21:00 自动发布，稍后自动出现，可点右上 🔄 刷新）"

# ── 🧭 导航 ─────────────────────────────────────────────────
import re as _re_act
_ACT_COLORS = [
    (_re_act.compile(r"(买入|建仓|加仓|试仓)"), "#dc2626"),      # 买入类=红
    (_re_act.compile(r"(评估减仓|冲高减仓|减仓|锁盈)"), "#ea580c"),  # 减仓类=橙
    (_re_act.compile(r"(卖出|清仓|退出|止损|破位离场)"), "#16a34a"),  # 卖出类=绿
    (_re_act.compile(r"(持有|拿住)"), "#2563eb"),                # 持有=蓝
    (_re_act.compile(r"(回避)"), "#0891b2"),                     # 回避=青
]


def _act_colorize(text: str) -> str:
    """【V88·动作分色】买红/减橙/卖绿/持蓝/避青——与桌面端同一套色规。"""
    for _rx, _col in _ACT_COLORS:
        text = _rx.sub(lambda m: f"<span style='color:{_col};font-weight:700'>{m.group(1)}</span>", text)
    return text


def _linkify_cloud(md: str) -> str:
    """【V88·云端个股可点】把 [US:CODE] token 与 **名称**（CODE） 转成 ?q= 深链（蓝色可点）。"""
    import re as _rc
    _A = '<a href="?q={c}&focus=deep" target="_self" style="color:#1e3a5f;text-decoration:underline;cursor:pointer;font-weight:600">{t}</a>'
    md = _rc.sub(r"`?\[(US|SH|SZ|HK):([A-Za-z0-9\.\-]+)\]`?",
                 lambda m: _A.format(c=m.group(2), t=f"[{m.group(1)}:{m.group(2)}]"), md)
    md = _rc.sub(r"\*\*([一-鿿A-Za-z0-9\-·]{2,14})\*\*[（(]([A-Z0-9]{1,8}(?:\.[A-Z]{2})?)[）)]",
                 lambda m: _A.format(c=m.group(2), t=m.group(1)) + f"（{m.group(2)}）", md)
    # 【V88·动作分色】云端所有 markdown 渲染统一出口上色
    return _act_colorize(md)


# 【V88·非交易日判定】以真实交易日历为准（周末 + 可选 holidays.txt），与桌面端 _v88_is_trading_day 同源。
# 【修复】不再用 meta 的 outlook_ts 判交易日：live_publish 每轮都会把 outlook_ts 盖成当前时间，
# 交易日（如 2026-07-13 周一）也会被误判为非交易日，错误置顶周日的"下一交易日前瞻"。
def _cloud_is_nontrading(meta: dict = None) -> bool:
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    import os as _os
    _d = _dt.now(_tz(_td(hours=8)))
    if _d.weekday() >= 5:            # 周六 / 周日
        return True
    _today = _d.strftime("%Y-%m-%d")
    try:                            # 可选节假日表（与桌面端 holidays.txt 同源；缺失则仅按周末）
        _hf = _os.path.join(_os.path.dirname(__file__), "holidays.txt")
        with open(_hf, encoding="utf-8") as _f:
            _hol = {ln.strip().split()[0] for ln in _f
                    if ln.strip() and not ln.strip().startswith("#")}
        if _today in _hol:
            return True
    except Exception:
        pass
    return False


if _nav == "🧭 导航":
    _rep = _report_text if _report_sync_ok else ""
    _meta_nav = pub_meta()
    _nav_analysis_ts = (_report_manifest.get("generated_at")
                        or _meta_nav.get("daily_report_ts")
                        or (_snap or {}).get("generated_at"))
    _nav_analysis_note = _analysis_label(_nav_analysis_ts)
    # ── 非交易日：置顶「下一交易日前瞻」──
    if _cloud_is_nontrading(_meta_nav):
        _outlook_txt = pub_text("outlook.md") or ""
        if _outlook_txt.strip():
            st.markdown("#### 🔮 下一交易日前瞻 · 非交易日看这里")
            if _meta_nav.get("outlook_ts"):
                st.caption(_fresh_caption(_meta_nav["outlook_ts"], "前瞻") + " · 非交易日生成")
            st.markdown(_linkify_cloud(_outlook_txt), unsafe_allow_html=True)
            with st.popover("📋 复制前瞻"):
                st.code(_outlook_txt, language=None)
            st.divider()
            st.caption("下方为最近交易日的行情快照与温度定位（供延续参考）：")
        else:
            st.info("🔮 下一交易日前瞻生成中，稍后点右上 🔄 刷新。")
    st.markdown("#### 🧭 今日导航 · 该关注什么")
    st.caption("参数白话：上行概率（越大越有利）｜下行概率（越小越有利）｜盈亏比（越大越好，>1才有正向空间）｜期望值（>0才是正期望）｜ATR（越大波动越大）｜历史水位（越接近0%越靠近历史最高点）")
    # 【V88·今日焦点】醒目置顶：重点推荐 + 重点观察触发
    try:
        _fx, _ob = [], []
        _iop = _rep.find("## 🎯 今日操作榜")
        if _iop > 0:
            for _lnf in _rep[_iop:_iop + 4000].splitlines():
                if "买入/建仓" in _lnf and _lnf.strip().startswith("|"):
                    _cf = [x.strip() for x in _lnf.split("|") if x.strip()]
                    if len(_cf) >= 7:
                        _tk = _cf[1].strip("`[] ")
                        _cd = _tk.split(":", 1)[1] if ":" in _tk else _tk
                        _rsn = _cf[6].split("｜")[0].replace("**", "").strip()
                        _fx.append({"n": _cf[2], "c": _cd,
                                    "t": f"{_cf[3].replace('**','')[:40]}\n\n└ {_rsn[:60]}"})
                if len(_fx) >= 3:
                    break
        _iwa = _rep.find("## ⚡ 自选股智能预警")
        if _iwa < 0:
            _iwa = _rep.find("## ⚡ 关注股预警")
        if _iwa > 0:
            for _lnw in _rep[_iwa:_iwa + 2000].splitlines():
                _lnw = _lnw.strip()
                if _lnw.startswith("- ") and any(_lnw[2:].startswith(x) for x in ("❗", "⚠️", "🔄", "📅")):
                    _ob.append(_lnw[2:].replace("**", "")[:60])
                if len(_ob) >= 3:
                    break
        def _lnk(nm, cd):
            return f'<a href="?q={cd}&focus=deep" target="_self" style="color:#1e3a5f;text-decoration:underline;cursor:pointer;font-weight:600">{nm}</a>'
        if not _fx:
            if _quality == "plan_b":
                st.info(f"⭐ **今日策略：不强制给买入指令，转为机会观察＋风险保护**——优先保护仓位、已有利润和整体胜率 · {_nav_analysis_note}")
                _pb_lines = []
                _pb_market = ""
                _pb_start = _rep.find("## 六、🔭 明日与本周参考")
                for _pbl in (_rep[_pb_start:] if _pb_start >= 0 else "").splitlines():
                    if _pbl.startswith("### "):
                        _pb_market = _pbl.replace("### ", "").strip()
                    elif "**观察个股**：" in _pbl:
                        _pb_lines.append(f"<b>{_pb_market}·机会观察</b>：{_pbl.split('：', 1)[-1]}")
                    elif "**风险保护**：" in _pbl:
                        _pb_lines.append(f"<b>{_pb_market}·风险保护</b>：{_pbl.split('：', 1)[-1]}")
                if _pb_lines:
                    st.markdown("<div style='line-height:1.75;font-size:12px'>" + "<br>".join(_pb_lines) + "</div>", unsafe_allow_html=True)
            else:
                st.info(f"⭐ **今日无强制买入信号**（未同时通过75分＋72小时催化）——继续观察并保护仓位，现金也是仓位 · {_nav_analysis_note}")
        if _fx:
            st.success(f"**⭐ 今日重点关注（引擎买入档 Top3）** · 点股票名直接深度分析 · {_nav_analysis_note}")
            _h = "<br>".join(f"🟢 <b>{_lnk(x['n'], x['c'])}</b> {x['t'].replace(chr(10)*2, '<br>&nbsp;&nbsp;')}" for x in _fx)
            st.markdown(f"<div style='line-height:1.9'>{_h}</div>", unsafe_allow_html=True)
        # 【V88·各市场高分】买入档常因缺72h催化而空 → 顶出操作榜各市场Top3，保证美/A/港都露脸（点名分析）
        import re as _rem2
        _rows_mk = []
        if _iop > 0:
            for _lnm in _rep[_iop:_iop + 6000].splitlines():
                if not _lnm.strip().startswith("|"):
                    continue
                _cm = [x.strip() for x in _lnm.split("|") if x.strip()]
                if len(_cm) < 5:
                    continue
                _mm = _rem2.search(r"\[(US|SH|SZ|HK):([A-Za-z0-9\.\-]+)\]", _cm[1])
                _sm = _rem2.search(r"\d+", _cm[4])
                if not _mm or not _sm:
                    continue
                _mk3 = "🇺🇸 美股" if "美股" in _cm[0] else ("🇨🇳 A股" if "A股" in _cm[0] else ("🇭🇰 港股" if "港股" in _cm[0] else None))
                if _mk3:
                    _rows_mk.append((_mk3, int(_sm.group()), _cm[2], _mm.group(2)))
        _mk_html = []
        for _mk3 in ("🇺🇸 美股", "🇨🇳 A股", "🇭🇰 港股"):
            _seen3, _lst3 = set(), []
            for _m3, _s3, _n3, _c3 in sorted([r for r in _rows_mk if r[0] == _mk3], key=lambda x: -x[1]):
                if _c3 in _seen3:
                    continue
                _seen3.add(_c3)
                _lst3.append(f"{_lnk(_n3, _c3)}({_s3})")
                if len(_lst3) >= 3:
                    break
            if _lst3:
                _mk_html.append(f"<b>{_mk3}</b>：" + "、".join(_lst3))
        if _mk_html:
            st.markdown(f"**🌍 各市场引擎高分榜（操作榜 Top3 · 点名直接深度分析）**　<span style='font-size:11px;color:#64748b'>{_nav_analysis_note}</span>", unsafe_allow_html=True)
            st.markdown("<div style='line-height:1.9'>" + "<br>".join(_mk_html) + "</div>", unsafe_allow_html=True)
        if _ob:
            import re as _re9
            _obl = [_re9.sub(r"([\u4e00-\u9fffA-Za-z0-9\-·]+)（([A-Z0-9\.]+)）",
                             lambda m: _lnk(m.group(1), m.group(2)) + f"（{m.group(2)}）", _l9)
                    for _l9 in _ob]
            st.warning(f"**👁 重点观察触发**（点股票名直接分析） · {_nav_analysis_note}")
            st.markdown("<div style='line-height:1.9'>" + "<br>".join(_obl) + "</div>", unsafe_allow_html=True)
    except Exception:
        pass
    st.caption("温度定仓位 → 轮动定板块 → 操作榜定标的")
    st.caption(_fresh_caption((_snap or {}).get("generated_at"), "行情快照") + " · 持仓盘中每15分钟快扫；强思考最多每6小时")
    _meta0 = pub_meta()
    if _meta0.get("daily_report_ts"):
        st.caption(_fresh_caption(_meta0["daily_report_ts"], "日报/操作榜") + " · 每时段更新（07/13/19点）")
    if _snap and _snap.get("markets"):
        for _mkt in ("美股", "A股", "港股"):
            _t = (_snap["markets"].get(_mkt) or {}).get("temperature")
            if _t:
                st.markdown(f"🌡 **{_mkt} {_t['temp']}/100** {_t['label']} → 仓位 **{_t['position']}**")
                if _t.get("verdict"):
                    st.caption(f"🧭 研判：{_t['verdict']}")
        # 【V88·三层周期概率总览】与桌面首屏同源：大盘(快照l3)+板块(轮动轨迹)，自选见预警/日报
        try:
            _traj3 = ((_snap or {}).get("rotation_forecast") or {}).get("trajectories") or {}

            def _pcol3(_p):
                return "#dc2626" if _p >= 55 else ("#16a34a" if _p <= 45 else "#64748b")

            def _chain3(_probs):
                if not _probs:
                    return "<span style='color:#94a3b8'>—</span>"
                _seg = " ".join(f"<span style='color:{_pcol3(int(p))}'>{lab}<b>{int(p)}</b></span>"
                                for lab, p in _probs)
                _d = int(_probs[-1][1]) - int(_probs[0][1])
                _lv = int(_probs[-1][1])
                _arw = ("<b style='color:#dc2626'>↗越远越强</b>" if (_d >= 8 and _lv >= 59) else
                        ("<b style='color:#16a34a'>↘越远越弱</b>" if (_d <= -8 and _lv <= 41) else
                         ("<span style='color:#94a3b8'>↗趋中性</span>" if _d >= 8 else
                          ("<span style='color:#94a3b8'>↘趋中性</span>" if _d <= -8 else
                           "<span style='color:#94a3b8'>→均衡</span>"))))
                return f"{_seg}　{_arw}"

            _cols3 = st.columns(3)
            _any3 = False
            for _ci3, _mkt3 in enumerate(("美股", "A股", "港股")):
                _blk3 = (_snap["markets"].get(_mkt3) or {})
                _l33 = _blk3.get("l3") or {}
                _rows3 = []
                if _l33.get("probs"):
                    _rows3.append(f"<div style='margin-bottom:3px'>📈 <b>{_l33.get('name')}</b> "
                                  f"<span style='color:#475569'>{_l33.get('stage')}</span> · {_l33.get('action')}<br>"
                                  f"<span style='font-size:12px'>{_chain3(_l33['probs'])}</span></div>")
                _tl3 = sorted((_traj3.get(_mkt3) or []),
                              key=lambda t: -((t.get("points") or {}).get("2周") or {}).get("score", 0))
                for _t3, _fl3 in ([(_tl3[0], "🔥")] if _tl3 else []) + \
                                 ([(_tl3[-1], "🧊")] if len(_tl3) > 2 else []):
                    _pts3 = _t3.get("points") or {}
                    _pp3 = [(k, int((_pts3.get(k) or {}).get("score", 0)))
                            for k in ("2周", "5周", "8周", "16周") if _pts3.get(k)]
                    _tg3 = str((_pts3.get("2周") or {}).get("trigger") or _t3.get("reason") or "")[:18]
                    _rows3.append(f"<div style='margin-bottom:3px'>{_fl3} <b>{_t3.get('name')}</b> "
                                  f"<span style='font-size:11px;color:#64748b'>{_tg3}</span><br>"
                                  f"<span style='font-size:12px'>{_chain3(_pp3)}</span></div>")
                if _rows3:
                    _any3 = True
                    with _cols3[_ci3]:
                        st.markdown(
                            f"<div style='border:1px solid #dbe4f0;border-radius:8px;padding:7px 9px;"
                            f"background:#fff;box-shadow:0 1px 2px rgba(15,23,42,.04)'>"
                            f"<div style='font-size:12px;color:#334155;margin-bottom:4px'><b>{_mkt3}</b></div>"
                            + "".join(_rows3) + "</div>", unsafe_allow_html=True)
            if _any3:
                st.markdown("<span style='font-size:11px;color:#64748b'>🧭 三层周期·概率总览｜数字=该周期<b>上涨概率%</b>"
                            "（规则情景估计非胜率）：<span style='color:#dc2626'>红≥55偏涨</span>／"
                            "<span style='color:#16a34a'>绿≤45偏跌</span>／灰=中性。箭头=周期间趋势，"
                            "与左侧「阶段·动作」（现在能不能买）是两件事｜自选层见 ⚡预警与日报</span>",
                            unsafe_allow_html=True)
        except Exception:
            pass
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
        _rot_cloud = (_snap or {}).get("rotation_forecast") or {}
        _cyc_cloud = (_snap or {}).get("cycle_scan") or {}
        if _rot_cloud or _cyc_cloud.get("stocks"):
            st.markdown("**🧭 板块轮动＋个股周期总览（2 / 5 / 8 / 16周＋预计拐点）**")
            from rotation_ui import combined_cycle_dashboard_html as _cycle_board_cloud, available_markets as _am_cloud
            _mk_c = _am_cloud(_rot_cloud)
            _focus_c = (st.radio("时钟聚焦市场", _mk_c, horizontal=True, key="v88_cloud_nav_rot_focus",
                                 label_visibility="collapsed")
                        if len(_mk_c) > 1 else (_mk_c[0] if _mk_c else "美股"))
            st.markdown(_cycle_board_cloud(_rot_cloud, _cyc_cloud, "v88-cloud-nav-cycle-board", _focus_c),
                        unsafe_allow_html=True)
    else:
        st.info(_NOT_READY)
    # 【V88·双门 2026-07-19 用户点单"云端也要有龙虎门/鬼门关"】
    # 龙虎门=公开黑马绿灯(pub安全);鬼门关含持仓名→走PRIVATE_TOKEN私径,无token如实提示。
    try:
        _dh_g9 = {}
        try:
            _dh_g9 = json.loads(pub_text("darkhorse.json", _PUB_VERSION) or "")
        except Exception:
            _dh_g9 = {}
        _gate_go9 = [h for h in (_dh_g9.get("horses") or [])
                     if ((h.get("trade_plan") or {}).get("short") or {}).get("mode")
                     in ("现价可进", "回踩到位", "突破确认")]
        # 【V88·双门卡片化 2026-07-19 用户点单"像自选一样卡片化+16周走势+成功率"】
        # 与桌面同口径mini卡:今天锚点(阶段基准+5日动量)+2/4/8/16/32周逐点箭头链+统一战绩总账。
        try:
            _sr_g9 = (json.loads(pub_text("success_rates.json", _PUB_VERSION) or "{}")
                      .get("types") or {})
        except Exception:
            _sr_g9 = {}

        def _rate_g9(key, label):
            _t = _sr_g9.get(key) or {}
            if not _t:
                return ""
            if _t.get("rate") is None:
                return f"📊 {label}实盘成功率：样本积累中（{_t.get('n', 0)}次·<5不报率）"
            return (f"📊 {label}实盘成功率 {_t['rate']}%（{_t.get('n')}次"
                    + (f"·均{_t['avg']:+.1f}%" if _t.get("avg") is not None else "")
                    + f"·{_t.get('note', '')}·非规则估计）")

        def _now_g9(_d):
            # 今天锚点=阶段基准(与桌面 _v88_stage_base9 同一张表)+5日动量微调(±6clip×1.2),clip 20-80
            _s = str((_d.get("facts") or {}).get("stage") or _d.get("cycle_status") or "")
            _b = (45 if any(k in _s for k in ("蓄势", "底部")) else
                  62 if any(k in _s for k in ("领涨", "主升", "启动", "延续", "多头")) else
                  55 if any(k in _s for k in ("派发", "滞涨", "高位")) else
                  38 if any(k in _s for k in ("退潮", "破位", "转弱", "下跌")) else 50)
            try:
                _r5 = float((((_d.get("facts") or {}).get("horizons") or {})
                             .get("2周") or {}).get("ret5_pct") or 0)
            except (TypeError, ValueError):
                _r5 = 0.0
            return int(max(20, min(80, _b + max(-6.0, min(6.0, _r5)) * 1.2)))

        def _gchain9(_d):
            _hz = (_d.get("facts") or {}).get("horizons") or {}
            _probs = [(k, int(round(float((_hz.get(k) or {}).get("rule_score")))))
                      for k in ("2周", "4周", "8周", "16周", "32周")
                      if (_hz.get(k) or {}).get("rule_score") is not None]
            if not _probs:
                return "<span style='color:#94a3b8'>走势链数据缺失·细节看桌面版</span>"
            _nw = _now_g9(_d)
            _out = [f"<span style='color:#94a3b8'>现在<b>{_nw}</b></span>"]
            _prev = _nw
            for _lab, _p in _probs:
                _ar = ("<b style='color:#dc2626'>↑</b>" if _p - _prev >= 1 else
                       ("<b style='color:#16a34a'>↓</b>" if _p - _prev <= -1 else
                        "<span style='color:#94a3b8'>≈</span>"))
                _pc = "#dc2626" if _p >= 55 else ("#16a34a" if _p <= 45 else "#64748b")
                _out.append(f"{_ar}<span style='color:{_pc}'>{_lab}<b>{_p}</b></span>")
                _prev = _p
            return " ".join(_out)

        def _gcard9(_d, _bc, _bg, _head):
            _rr = _d.get("rr")
            _ex = _d.get("expected_pct")
            _meta = " ｜ ".join(x for x in (
                f"盈亏比<b>{float(_rr):.1f}</b>" if _rr is not None else "",
                f"2周期望<b>{float(_ex):+.1f}%</b>" if _ex is not None else "",
                f"下行<b>{int(_d.get('p_down') or 0)}%</b>" if _d.get("p_down") else "") if x)
            return (f"<div style='border:1px solid {_bc}44;border-left:4px solid {_bc};"
                    f"border-radius:8px;background:{_bg};padding:.45rem .6rem'>"
                    f"<div style='font-size:12px;font-weight:700;color:{_bc}'>{_head}</div>"
                    f"<div style='font-size:13px'><b>{_d.get('name')}</b> "
                    f"<span style='color:#94a3b8;font-size:11px'>{_d.get('code')}</span> "
                    f"<b style='color:{_bc}'>{_d.get('action') or ''}</b></div>"
                    f"<div style='font-size:12px;margin:2px 0'>{_gchain9(_d)}</div>"
                    + (f"<div style='font-size:11px;color:#64748b'>{_meta}</div>" if _meta else "")
                    + "</div>")

        _gate_note9 = ("<div style='font-size:11px;color:#94a3b8;margin:2px 0 6px'>"
                       "↑↓≈=较前一档，链首=今天锚点（阶段+5日动量，与桌面同口径）；"
                       "概率为规则情景估计</div>")
        if _gate_go9:
            st.markdown(f"**🐉 龙虎门 · 上攻关注**（黑马严门槛绿灯 {len(_gate_go9)} 只·出处:黑马漏斗复判）")
            _rl_lh9 = _rate_g9("entry_green", "入场绿灯")
            if _rl_lh9:
                st.caption(_rl_lh9)
            st.markdown(
                "<div style='display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:6px'>"
                + "".join(_gcard9(h, "#dc2626", "#fef2f2",
                                  "🐉 " + str(((h.get('trade_plan') or {}).get('short') or {}).get('mode') or '绿灯'))
                          for h in _gate_go9)
                + "</div>" + _gate_note9, unsafe_allow_html=True)
            st.caption("完整龙虎门(含自选/持仓实时绿灯)在桌面版；此处为公开黑马部分。")
        _tok_g9 = str(st.secrets.get("PRIVATE_TOKEN", "") or "").strip()
        if _tok_g9:
            try:
                import base64 as _b64g9
                _rg9 = requests.get(
                    "https://api.github.com/repos/bluestevenr-lang/v88-daily-report/contents/data/intraday_decisions.json",
                    headers={"Authorization": f"token {_tok_g9}",
                             "Accept": "application/vnd.github+json"}, timeout=12)
                _idc9 = json.loads(_b64g9.b64decode(_rg9.json().get("content") or "").decode("utf-8"))
                _cut_g9 = [r for r in (_idc9.get("rows") or [])
                           if any(k in str(r.get("action", "")) for k in ("减", "退", "清", "止损"))]
                if _cut_g9:
                    st.markdown(f"**⚔️ 鬼门关 · 拐点/破位先躲**（{len(_cut_g9)} 只·盘中落盘·🔒私径）")
                    _rl_gg9 = _rate_g9("gate_guard", "鬼门关警示")
                    st.caption(_rl_gg9 or "📊 鬼门关警示实盘成功率：样本积累中（警示后≥3天下跌=躲对了，反向口径）")
                    st.markdown(
                        "<div style='display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:6px'>"
                        + "".join(_gcard9(r, "#16a34a", "#f0fdf4",
                                          "⚔️ " + str(r.get("reason") or "拐点/破位警示")[:14])
                                  for r in _cut_g9)
                        + "</div>" + _gate_note9, unsafe_allow_html=True)
            except Exception:
                st.caption("⚔️ 鬼门关：私仓数据读取失败，稍后刷新。")
        else:
            st.caption("⚔️ 鬼门关（含持仓，属私域）：需配 PRIVATE_TOKEN 才在云端显示——隐私铁律，桌面/飞书不受限。")
    except Exception:
        pass

    # 🔥 最新热点（直接可见，详情见「🔥 热点新闻」页）
    try:
        _nl0 = json.loads(pub_text("news_live.json") or "{}")
        _its0 = _nl0.get("items") or []
        if _its0:
            st.markdown("**🔥 最新热点**（北京时间·实际发生时间）：")
            for _it0 in _its0[:5]:
                _d0 = _parse_ts(_it0.get("time"))
                _t0 = _d0.strftime("%H:%M") if _d0 else "--"
                _dd0 = int(_it0.get("dir", 0) or 0)
                _dm0 = "🔻" if _dd0 < 0 else ("🔺" if _dd0 > 0 else "")
                st.markdown(f"- {_dm0}`{_t0}` {_it0.get('s','')}｜{str(_it0.get('t',''))[:60]}")
            st.caption("👉 完整实时新闻流见顶部「🔥 热点新闻」页（时段筛选/来源/链接）")
    except Exception:
        pass
    _ipb = _rep.find("## 💎 深度回调机会池")
    if _ipb > 0:
        _jpb = _rep.find("\n## ", _ipb + 5)
        exp_md("💎 深度回调机会池（优质股·回撤≥30%·企稳信号）",
               _rep[_ipb:_jpb if _jpb > 0 else _ipb + 2500])
    _i = _rep.find("## 🎯 今日操作榜")
    if _i > 0:
        _j = _rep.find("## 二、", _i)
        exp_md("🎯 今日操作榜（按门槛入选·允许无机会·实价校准）",
               _rep[_i:_j if _j > 0 else _i + 3000], expanded=True)
    elif not _report_sync_ok:
        st.info("📭 操作榜等待权威日报与冻结快照完成一致性校验。")
    try:
        _navcp = []
        for _mkt in ("美股", "A股", "港股"):
            _b = (_snap or {}).get("markets", {}).get(_mkt) or {}
            _t = _b.get("temperature") or {}
            _tr = _b.get("turn_risk") or {}
            if _t:
                _navcp.append(f"{_mkt} 温度{_t.get('temp','?')}/100 {_t.get('label','')} 仓位{_t.get('position','?')}"
                              + (f"｜{_tr['text']}" if _tr.get('text') else ""))
            for _x in (_b.get("indices") or [])[:3]:
                _navcp.append(f"  {_x['trend']} {_x['name']} {_x['last']}｜5日{_x['chg5d']:+.1f}%"
                              + (f"｜{_x['turning']}" if _x.get('turning') else ""))
        with st.popover("📋 复制本页导航摘要"):
            st.code("\n".join(_navcp) or "无数据", language=None)
    except Exception:
        pass
    st.caption("💡 持仓建议为隐私内容，请在飞书推送或 Mac/局域网 V88 查看")

# ── 🔥 热点新闻（随盘中快照同步·每条带实际发生时间·三时段筛选）──────────
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
        st.info("📭 新闻流生成中（随盘中快照同步抓取，可点右上 🔄 刷新）")
    else:
        st.caption(_fresh_caption(_nl.get("generated_at"), "新闻流") + " · 随三市盘中快照同步 · 12个中外RSS源")
        _tps = _nl.get("topics") or []
        if _tps:
            st.markdown("**🔥 热点主题**：" + " · ".join(f"`{t['w']}({t['n']})`" for t in _tps))
        # 【V88·单条重大通道】补足热点榜漏掉的突发（单条即入，标最小字号，不占版面）
        _mj = _nl.get("major_news") or []
        if _mj:
            _ml = []
            for _m in _mj[:6]:
                _g = "🔻" if _m.get("dir", 0) < 0 else ("🔺" if _m.get("dir", 0) > 0 else "•")
                _rl = "／".join(x.get("n", "") for x in (_m.get("stk") or [])[:2])
                _tt = str(_m.get("t", ""))[:46]
                _uu = _m.get("url") or ""
                _ttl = f"<a href='{_uu}' target='_blank' style='color:inherit'>{_tt}</a>" if _uu else _tt
                _ml.append(f"{_g} {_ttl}" + (f" <span style='opacity:.6'>{_rl}</span>" if _rl else ""))
            st.markdown("<div style='font-size:0.78em;line-height:1.5;opacity:.9'>"
                        "<b>🚨 单条重大</b>　" + "　".join(_ml) + "</div>", unsafe_allow_html=True)
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
            # 【V88·新闻方向标·最小字号】🔻利空/🔺利好，单glyph前置，不占版面
            _d = int(_it.get("dir", 0) or 0)
            _dm = "🔻 " if _d < 0 else ("🔺 " if _d > 0 else "")
            _line = (f"{_dm}**`{_tstr}`** ｜ {_it.get('s','')} ｜ [{_ttl}]({_url})" if _url
                     else f"{_dm}**`{_tstr}`** ｜ {_it.get('s','')} ｜ {_ttl}")
            # 【C1·新闻映射】相关标的（最小字号，弱化不占版面）
            _rel = "／".join(x.get("n", "") for x in (_it.get("stk") or [])[:3])
            if _rel:
                _line += f" <span style='font-size:0.72em;opacity:.6'>📌{_rel}</span>"
            st.markdown(_line, unsafe_allow_html=True)
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
        st.caption("💡 得分=V88唯一统一分（短20%＋中25%＋长20%＋趋势15%＋赔率20%）｜表内同时显示概率、盈亏比与期望值｜口径V88-U2.0")
        import cloud_engine as _ceg
        exp_md("📖 术语速查（数值高低怎么看）", _ceg.GLOSSARY_MD)

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
            from v88_decision_core import evaluate_decision as _evaluate_cloud_decision
            _cloud_decision = _evaluate_cloud_decision(_df, f, name=_tname, code=_tsym)
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("唯一统一分", _cloud_decision["unified_score"])
            c2.metric("短/中/长", f"{_cloud_decision['short_score']}/"
                      f"{_cloud_decision['medium_score']}/{_cloud_decision['long_score']}")
            c3.metric("2周上/下", f"{_cloud_decision['p_up']}%/{_cloud_decision['p_down']}%")
            c4.metric("盈亏比", f"{_cloud_decision['rr']:.2f}")
            c5.metric("情景期望", f"{_cloud_decision['expected_pct']:+.1f}%")
            st.info(f"**统一动作：{_cloud_decision['action']}**｜{_cloud_decision['entry_note']}｜"
                    f"口径{_cloud_decision['score_version']}｜数据签名{_cloud_decision['data_signature']}｜"
                    f"分析{_cloud_decision['analysis_time']}")
            with st.expander("📎 辅助技术底稿（不另行决定买卖）", expanded=False):
                c1a, c2a, c3a = st.columns(3)
                c1a.metric("趋势质量辅助", f"{f['total']}/100")
                c2a.metric("现价", f"{f['last']}")
                c3a.metric("RSI", f"{f['rsi']}")
                st.markdown(f"{_concl_color.get(f['conclusion'],'')} 技术阶段：**{f['conclusion']}**｜{f['action']}")

            # 【V88·云端个股五周期】点击直达后自动显示2/4/8/16/32周，
            # 行情规则先算、DeepSeek thinking-high再复核，失败也不伪装AI结果。
            st.markdown("##### 🧭 个股周期轮换总览（深度分析第一判断）")
            st.caption("先看周期象限与2/4/8/16/32周走向；同一行情快照复用6小时思考缓存。")
            _fu = None
            try:
                import stock_horizon as _stock_horizon_cloud
                _fu = cloud_engine.fundamentals(_tsym)
                _rel_lines = [ln.strip() for ln in str(_report_text or "").splitlines()
                              if (_tsym.split(".")[0] in ln or _tname in ln)][:8]
                _hz_context = ((f"基本面:{(_fu or {}).get('line', '暂无')}；")
                               + f"日报相关:{'；'.join(_rel_lines)[:700]}")
                _hz_bar = st.progress(0, text="正在计算五周期量价底稿…")
                _hz_bar.progress(35, text="DeepSeek思考模式复核中…")
                _hz_result = _stock_horizon_cloud.analyze(
                    _tname, _tsym, _df, full=f, context=_hz_context,
                    api_key=str(st.secrets.get("DEEPSEEK_API_KEY", "") or ""),
                )
                _hz_bar.progress(100, text="五周期走势分析完成")
                _hz_bar.empty()
                _hz_rows = _stock_horizon_cloud.table_rows(_hz_result)
                _hz_align = _stock_horizon_cloud.cycle_alignment(_hz_result.get("facts") or {})
                _hz_action = _cloud_decision["action"]
                _hz_result = dict(_hz_result, decision=_cloud_decision)
                _hz_visual = _stock_horizon_cloud.cycle_visual_html(
                    _hz_result, _tname, _tsym, f"v88-cloud-stock-cycle-{_tsym}")
                if _hz_visual:
                    st.markdown(_hz_visual, unsafe_allow_html=True)
                if _hz_rows:
                    st.dataframe(_hz_rows, hide_index=True, use_container_width=True)
                _hz_review = _hz_result.get("review") or {}
                if _hz_review.get("status") in ("completed", "cached"):
                    st.info(
                        f"🧠 **思考复核**：{_hz_review.get('summary', '五周期复核完成')} ｜ "
                        f"相位：{_hz_review.get('cycle_phase', '震荡')} ｜ "
                        f"周期口径：{_hz_align.get('note', '待核')} ｜ "
                        f"动作：{_hz_action} ｜ "
                        f"失效：{_hz_review.get('invalid_summary', '破位后重评')}"
                    )
                    st.caption(
                        f"模型：{_hz_review.get('model', 'deepseek-v4-flash')} · thinking-high ｜ "
                        f"分析于 {_hz_review.get('analysis_time', '缓存时间待核')}"
                    )
                else:
                    st.caption(
                        "ℹ️ 云端为轻量只读版，个股分析用**确定性规则底稿**（各周期概率/动量/量比/支撑压力"
                        "+规则人话理由，上表已完整）；AI思考复核仅在桌面版按需运行，不在公开云端调用（省钱+防滥用）。"
                    )
            except Exception as _hz_cloud_exc:
                st.warning(f"五周期走势暂不可用：{type(_hz_cloud_exc).__name__}")

            # 【V88·个人决策锚点】云端与网页版调用同一个无未来函数核心。
            st.markdown("##### 🧷 我的决策锚点 · 2/5/8/16周")
            st.caption(
                "输入当时分析/成交的时间与价格；预测仅使用该时点以前的行情。"
                "概率为规则情景估计（非回测胜率），后续实绩只用于复盘。"
            )
            try:
                import pandas as pd
                from v88_decision_core import evaluate_anchor_outlook as _evaluate_cloud_anchor

                _ca_last_date = pd.Timestamp(_df.index[-1]).date()
                _ca_last_price = float(pd.to_numeric(_df["Close"], errors="coerce").dropna().iloc[-1])
                _ca1, _ca2, _ca3, _ca4 = st.columns(4)
                with _ca1:
                    _ca_date = st.date_input(
                        "分析/操作日期", value=_ca_last_date,
                        min_value=pd.Timestamp(_df.index[12]).date(), max_value=_now_bjt().date(),
                        key=f"cloud_anchor_date_{_tsym}")
                with _ca2:
                    _ca_clock = st.time_input(
                        "当时时间", value=datetime.strptime("09:45", "%H:%M").time(),
                        key=f"cloud_anchor_time_{_tsym}")
                with _ca3:
                    _ca_price = st.number_input(
                        "当时价格", min_value=0.0001, value=_ca_last_price, format="%.4f",
                        key=f"cloud_anchor_price_{_tsym}")
                with _ca4:
                    _ca_action = st.selectbox(
                        "当时动作", ["观察", "买入", "加仓", "减仓", "卖出", "清仓"],
                        key=f"cloud_anchor_action_{_tsym}")

                if st.button("🧠 按当时视角推算", type="primary", use_container_width=True,
                             key=f"cloud_anchor_run_{_tsym}"):
                    _ca_bar = st.progress(25, text="正在截断锚点后的行情…")
                    _ca_bar.progress(65, text="正在计算2/5/8/16周概率、赔率和期望…")
                    st.session_state[f"cloud_anchor_result_{_tsym}"] = _evaluate_cloud_anchor(
                        _df, datetime.combine(_ca_date, _ca_clock), _ca_price,
                        action=_ca_action, name=_tname, code=_tsym,
                        analysis_time=_now_bjt().strftime("%Y-%m-%d %H:%M"),
                    )
                    _ca_bar.progress(100, text="当时视角推算完成")
                    _ca_bar.empty()

                _ca_result = st.session_state.get(f"cloud_anchor_result_{_tsym}") or {}
                if _ca_result.get("error"):
                    st.error(_ca_result["error"])
                elif _ca_result:
                    _cm1, _cm2, _cm3, _cm4 = st.columns(4)
                    _cm1.metric("综合上/下", f"{_ca_result.get('weighted_p_up')}%/"
                                f"{_ca_result.get('weighted_p_down')}%")
                    _cm2.metric("综合盈亏比", f"{_ca_result.get('weighted_rr', 0):.2f}")
                    _cm3.metric("综合期望", f"{_ca_result.get('weighted_expected_pct', 0):+.1f}%")
                    _ca_track = _ca_result.get('tracking') or {}
                    _ca_since = _ca_track.get('since_anchor_pct')
                    _cm4.metric("锚点后实绩", (f"{_ca_since:+.1f}%" if _ca_since is not None else "待最新行情"))
                    st.info(f"**当时结论：{_ca_result.get('overall_action')}**｜"
                            f"动作复盘：{_ca_result.get('decision_review')}")
                    _ca_rows = [{
                        "周期": x.get("label"),
                        "上涨/下跌": f"{x.get('p_up')}%/{x.get('p_down')}%",
                        "上涨/下跌空间": f"+{x.get('upside_pct')}% / -{x.get('downside_pct')}%",
                        "目标/风险价": f"{x.get('target_price')} / {x.get('risk_price')}",
                        "盈亏比": x.get("rr"), "期望值": f"{x.get('expected_pct'):+.1f}%",
                        "判断": x.get("view"), "触发": x.get("trigger"), "失效": x.get("invalid"),
                    } for x in (_ca_result.get("horizons") or [])]
                    st.dataframe(_ca_rows, hide_index=True, use_container_width=True)
                    st.caption(f"🔒 无未来函数：是｜口径{_ca_result.get('score_version')}｜"
                               f"预测签名{_ca_result.get('data_signature')}｜"
                               f"生成于{_ca_result.get('analysis_time')}")
            except Exception as _ca_exc:
                st.warning(f"个人决策锚点暂不可用：{type(_ca_exc).__name__}")
            # 【V88·拐点识别】放量+破趋势=拐点，直接亮出证据与判断提示词
            _turn = f.get("turning") or {}
            if _turn.get("side"):
                (st.error if _turn["side"] == "top" else st.info)(
                    f"**{_turn['label']}**：" + "；".join(_turn["signals"]) + f"\n\n👉 {_turn['prompt']}")
            # 【V88·明白话判读】量价/K线/MACD 事实与要点（不是分数）
            _ro = cloud_engine.plain_readout(f, _turn if _turn.get("side") else None)
            if _ro:
                exp_md("📖 量价判读（事实+要点，你来拍板）",
                       "\n".join(f"- {ln}" for ln in _ro), expanded=True)

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
                with st.popover("📋 复制拆解"):
                    st.code("\n".join(f"{k}: {d}｜{sc}分×{int(w*100)}%"
                                       for k, (sc, w, d) in _bd.items()), language=None)
                _rows = [{"维度": k, "实际情况": d, "得分": sc, "权重": f"{int(w*100)}%"}
                         for k, (sc, w, d) in _bd.items()]
                st.dataframe(_rows, hide_index=True, use_container_width=True)
                if not f["sector_known"]:
                    st.caption("_板块强度云端按中性50计；资金动向=OBV能量潮真实数据；主判断由价格/均线/MACD/量价/水位驱动_")
            st.caption(f"20日动量 {f['chg20']:+.1f}% ｜ 乖离MA20 {f['bias20']:+.1f}% ｜ 量比 {f['volr']} ｜ 数据截至 {r['asof']}")
            # 【E1·真实基本面】估值/成长/盈利质量（取不到如实标"—"）
            _fu = _fu if _fu is not None else cloud_engine.fundamentals(_tsym)
            if _fu:
                st.markdown(f"**🧾 基本面**：`{_fu['tag']}`  \n{_fu['line']}")
            _pl = cloud_engine.horizon_plans(f, _df)
            if _pl:
                st.markdown("##### ⏱ 分期限剧本（短线做T｜中线锚MA55｜长线锚年线）")
                st.markdown("\n".join(f"- {_pl[k]}" for k in ("short", "mid", "long") if _pl.get(k)))
            exp_md("📖 术语速查（每个数值高低代表什么，非专业版）", cloud_engine.GLOSSARY_MD)
            # 【V88·复制纪要】整段分析一键复制（st.code 自带复制按钮，微信/笔记直接粘贴）
            _cp_txt = cloud_engine.analysis_text(r.get('name') or r['symbol'], r['symbol'], f, r.get('asof', ''), fund=_fu if '_fu' in dir() else None)
            if _cp_txt:
                with st.expander("📋 复制分析纪要（右上角复制按钮）", expanded=False):
                    st.code(_cp_txt, language=None)

# ── 📊 日报 / 📅 周报 ────────────────────────────────────────
elif _nav in ("📊 日报", "📅 周报"):
    _txt = _report_text if _nav == "📊 日报" else pub_text("weekly_report.md")
    if _nav == "📊 日报" and not _report_sync_ok:
        _txt = None
    if _txt:
        _meta1 = pub_meta()
        _mk1 = "daily_report_ts" if _nav == "📊 日报" else "weekly_report_ts"
        if _meta1.get(_mk1):
            st.caption(_fresh_caption(_meta1[_mk1], "本报告")
                       + (" · 每时段更新（北京时间07/13/19点）" if _nav == "📊 日报" else " · 每周日更新"))
        # 【V88·复制】下载 md 原文 + 复制全文（st.code 右上角自带复制按钮）
        _cc1, _cc2 = st.columns(2)
        _fn = "V88日报.md" if _nav == "📊 日报" else "V88周报.md"
        _cc1.download_button("📥 下载原文", data=_txt, file_name=_fn,
                             mime="text/markdown", use_container_width=True)
        with _cc2.popover("📋 复制全文", use_container_width=True):
            st.code(_txt[:12000], language=None)
        _ledger_marker = "## 🔗 可核验来源台账"
        _ledger_pos = _txt.find(_ledger_marker)
        _report_body = _txt[:_ledger_pos].rstrip() if _ledger_pos >= 0 else _txt
        # 【V88·动作分色】日报正文买红/减橙/卖绿/持蓝/避青（同桌面色规）
        st.markdown(_act_colorize(_report_body), unsafe_allow_html=True)
        if _nav == "📊 日报" and _source_ledger.get("sources"):
            with st.expander("🔗 可核验来源（原文链接）", expanded=False):
                with st.popover("📋 复制来源清单"):
                    st.code(str(_source_ledger)[:6000], language=None)
                for _src in _source_ledger["sources"][:20]:
                    _label = f"[{_src.get('id')}] Tier {_src.get('tier')} · {_src.get('source')}"
                    if _src.get("url"):
                        st.markdown(f"<div style='font-size:9px;line-height:1.3'>• "
                                    f"<a href='{_src.get('url')}' target='_blank'>{_label} · {_src.get('title')}</a></div>",
                                    unsafe_allow_html=True)
                    else:
                        st.markdown(f"<div style='font-size:9px;line-height:1.3'>• {_label} · "
                                    f"{_src.get('title')}（缺原文链接）</div>", unsafe_allow_html=True)
        st.caption("💡 持仓建议为隐私内容，不在云端公开显示；见飞书或 Mac 版")
    else:
        st.info(_NOT_READY if _nav == "📊 日报" else "📅 周报每周日生成")

# ── 📈 大盘板块 ──────────────────────────────────────────────
elif _nav == "📈 大盘板块":
    st.markdown("#### 📈 大盘走势与板块轮动")
    if _snap and _snap.get("rotation_forecast"):
        st.markdown("##### 🧠 板块热度与轮换周期思维导图（日 / 周 / 月）")
        from rotation_ui import rotation_map_html as _rotation_map_html_market
        st.markdown(_rotation_map_html_market(_snap["rotation_forecast"], "v88-cloud-market-rotation"), unsafe_allow_html=True)
    if _snap:
        with st.popover("📋 复制大盘摘要"):
            _mp = []
            for _mk2, _b2 in (_snap.get("markets") or {}).items():
                _t2 = _b2.get("temperature") or {}
                _mp.append(f"{_mk2} 温度{_t2.get('temp','?')}/100 {_t2.get('label','')}")
                for _x2 in (_b2.get("indices") or []):
                    _mp.append(f"  {_x2['name']} {_x2['last']} 5日{_x2['chg5d']:+.1f}% {_x2['trend']}"
                               + (f" {_x2['turning']}" if _x2.get('turning') else ""))
            st.code("\n".join(_mp), language=None)
    if _snap:
        st.caption(_fresh_caption(_snap.get("generated_at"), "行情快照") + " · 交易日盘中每30分钟刷新")
        for mkt, blk in _snap.get("markets", {}).items():
            st.markdown(f"### {mkt}")
            _t = blk.get("temperature")
            if _t:
                st.markdown(f"🌡 温度 **{_t['temp']}/100** {_t['label']}（趋势{_t['trend']}/宽度{_t['breadth']}/动量{_t['momentum']}/量能{_t.get('vol_heat','—')}）→ 仓位 {_t['position']}")
                if _t.get("verdict"):
                    st.markdown(f"🧭 **研判**：{_t['verdict']}")
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
elif _nav == "🛰️ 雷达族":
    # 【V88·三端同步 2026-07-18 用户点单】打新/公告可转债/机构/黑马与桌面同一份落盘数据。
    st.markdown("### 🛰️ 雷达族（打新 / 公告事件·可转债 / 机构风向标 / 黑马）")
    st.caption("与桌面版同一份交易日落盘数据；深度交互分析（实时重算/K线作战层）在桌面版。")

    def _pub_json9(name):
        try:
            return json.loads(pub_text(name, _PUB_VERSION) or "")
        except Exception:
            return {}

    _succ9c = _pub_json9("success_rates.json")

    def _rate9c(key, label):
        _t9c = ((_succ9c.get("types") or {}).get(key)) or {}
        if not _t9c:
            return ""
        if _t9c.get("rate") is None:
            return f"📊 {label}：样本积累中（{_t9c.get('n', 0)}次·<5不报率）"
        return f"📊 {label}实盘 {_t9c.get('rate')}%（{_t9c.get('n')}次·{_t9c.get('note', '')}）"

    _ipo9c = _pub_json9("ipo_radar.json")
    st.markdown("#### 🆕 打新雷达（中美港新股 · Top3优先）")
    _ipo_rows9c = (_ipo9c.get("rows") or [])
    if _ipo_rows9c:
        st.dataframe([{k: r.get(k) for k in ("市场", "名称", "代码", "申购日", "评级", "要点")
                       if k in r} or r for r in _ipo_rows9c[:10]],
                     hide_index=True, use_container_width=True)
        st.caption(f"🕒 {_ipo9c.get('generated_at', '')} · 出处:Tushare/Nasdaq/富途（A/美/港）")
        _ipor9c = _rate9c("ipo_hk", "港股新股首日上涨率")
        if _ipor9c:
            st.caption(_ipor9c)
    else:
        st.info("打新数据待流水线发布（交易日更新）。")

    _annc9 = _pub_json9("announcements.json")
    st.markdown("#### ⚡ 公告事件雷达 · 全市场可转债")
    if _annc9:
        _cbs9c = _annc9.get("cb_calendar") or []
        if _cbs9c:
            st.markdown("**🌐 可转债日历**（出处:东财可转债数据）")
            for _x9c in _cbs9c[:6]:
                st.markdown(f"- **{_x9c.get('bond')}**（正股 {_x9c.get('stock')}）"
                            f"申购日{_x9c.get('apply_date')}·评级{_x9c.get('rating')}"
                            f"·{_x9c.get('scale')}亿——{_x9c.get('note')}"
                            + ("　⭐池内正股·抢权窗口" if _x9c.get("in_pool") else ""))
        _pipe9c = _annc9.get("cb_pipeline") or []
        if _pipe9c:
            st.markdown("**🟢 已注册待发·即将申购储备**（按关注分排序·出处:集思录）")
            for _x9c in _pipe9c[:6]:
                st.markdown(f"- {_x9c.get('tag', '')}{_x9c.get('score', '')}分 **{_x9c.get('stock')}** "
                            f"{str(_x9c.get('stage_date', ''))[5:]}同意注册｜{_x9c.get('why', '')}"
                            + ("　⭐池内" if _x9c.get("in_pool") else ""))
        _evs9c = _annc9.get("events") or {}
        if _evs9c:
            st.markdown("**📌 池内公告事件**（近5日·出处:东财公告库）")
            for _blk9c in list(_evs9c.values())[:8]:
                for _e9c in (_blk9c.get("items") or [])[:2]:
                    st.markdown(f"- {_e9c.get('icon')} **{_blk9c.get('name')}** "
                                f"{_e9c.get('date')}「{str(_e9c.get('title'))[:30]}」——{_e9c.get('note')}")
        st.caption("硬边界：事件只做语境参考，不推翻周期裁决；⚡两面事件若该股被判「回避」，最多短线纪律小仓。")
    else:
        st.info("公告/可转债数据待流水线发布（交易日更新）。")

    _inst9c = _pub_json9("institutional_signals.json")
    st.markdown("#### 🏛️ 机构风向标")
    if _inst9c:
        st.caption(f"🕒 {_inst9c.get('generated_at', '')} · 研报{_inst9c.get('reports_n', 0)}篇 · "
                   "出处:东财研报库(公开评级/目标价/标题)；外资观点=新闻流标题原文 · AI综合仅供参考")
        _aib9c = _inst9c.get("ai_brief") or {}
        if _aib9c:
            st.markdown(f"**🧭 机构综合布局**：主线 **{_aib9c.get('机构共识主线', '—')}** ｜ "
                        f"分歧 {_aib9c.get('机构分歧', '—')}")
            for _k9c in ("明天", "本周", "下周", "本月及下月"):
                if _aib9c.get(_k9c):
                    st.markdown(f"- **{_k9c}**：{_aib9c[_k9c]}")
        for _c9c in (_inst9c.get("consensus") or [])[:6]:
            st.markdown(f"- 📌 **{_c9c.get('stock')}**（{'、'.join((_c9c.get('orgs') or [])[:2])}）："
                        f"{_c9c.get('gist') or '—'}")
    else:
        st.info("机构数据待流水线发布。")

    _intel9c = _pub_json9("intel_feed.json")
    st.markdown("#### 📜 政策直采 · 人气榜")
    if _intel9c:
        for _p9c in (_intel9c.get("policy") or [])[:5]:
            st.markdown(f"- [{_p9c.get('src')}] {_p9c.get('title')}"
                        + (f"（{_p9c.get('date')}）" if _p9c.get("date") else ""))
        _hot9c = _intel9c.get("hot") or []
        if _hot9c:
            st.markdown("**🔥 散户情绪三榜**（反指标：越热闹越要冷静，不是买入榜）")
            st.caption("🇨🇳 东财人气榜：" + "、".join(
                f"#{h.get('rank')} {h.get('code')}"
                + (f"{h.get('chg'):+.1f}%" if isinstance(h.get("chg"), (int, float)) else "")
                + str(h.get("impact") or "").replace("拥挤观察", "")
                + ('🔥🔥雪#' + str(h.get('xq_rank')) if h.get('xq_rank') else '')
                for h in _hot9c[:10]))
        _xq9c = _intel9c.get("hot_xq") or []
        if _xq9c:
            st.caption("❄️ 雪球热股：" + "、".join(
                f"#{x.get('rank')} {x.get('name')}"
                + (f"{x.get('chg'):+.1f}%" if isinstance(x.get("chg"), (int, float)) else "")
                for x in _xq9c[:10]))
        _us9c = _intel9c.get("hot_us") or []
        if _us9c:
            st.caption("🇺🇸 雅虎热搜：" + "、".join(
                f"#{u.get('rank')} {u.get('symbol')}" for u in _us9c[:10]))
        _hd9c = _rate9c("hot_dual", "双榜热股隔日(上涨率低=反指标成立)")
        if _hd9c:
            st.caption(_hd9c)
        st.caption(f"🕒 {_intel9c.get('generated_at', '')} · 出处:发改委/央行官网直采+东财股吧")
    else:
        st.info("政策/人气数据待流水线发布。")

    _dh9c = _pub_json9("darkhorse.json")
    st.markdown("#### 🐴 黑马池（严门槛复判）")
    _horses9c = (_dh9c.get("horses") or [])
    if _horses9c:
        for _h9c in _horses9c[:8]:
            st.markdown(f"- {'🔴' if _h9c.get('grade') == '重点' else '🟡'} "
                        f"**{_h9c.get('name')}**（{_h9c.get('code')}·{_h9c.get('market', '')}）"
                        f"{str(_h9c.get('reason') or _h9c.get('why') or '')[:40]}")
        st.caption(f"🕒 {_dh9c.get('generated_at', '')} · 纯黑马=排除自选/持仓 · 宁缺毋滥")
    else:
        st.info("黑马数据待流水线发布。")

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

elif _nav == "💼 持仓终端":
    # 【V88·持仓终端】简化输入直写私仓 positions.json（PRIVATE_TOKEN），数据不进公开分支。
    # 2026-07-11 用户授权：云端可输入+查看持仓（Streamlit登录+APP_PASSWORD双重门禁）。
    st.markdown("#### 💼 持仓终端")
    _tok = str(st.secrets.get("PRIVATE_TOKEN", "") or "").strip()
    if not _tok:
        st.warning("需在 share.streamlit.io → App settings → Secrets 配置 PRIVATE_TOKEN（私仓读写令牌）后启用")
        st.caption("🔒 隐私铁律：持仓属私域。配 PRIVATE_TOKEN 前，务必先在后台 Settings → Sharing 设「邮箱授权」限定可访问人，"
                   "否则持仓会对任何知道网址的人可见。仅你自己用可放心开。")
    else:
        st.caption("🔒 已启用持仓（私仓读写）。请确认后台已设邮箱授权，此页含真实持仓，勿公开分享网址。")
        import base64 as _b64
        import tempfile as _tf
        from pathlib import Path
        _API = "https://api.github.com/repos/bluestevenr-lang/v88-daily-report/contents"
        _HDR = {"Authorization": f"token {_tok}", "Accept": "application/vnd.github+json"}

        def _priv_get(path):
            try:
                r = requests.get(f"{_API}/{path}", headers=_HDR, timeout=12)
                if r.status_code != 200:
                    return None, None
                j = r.json()
                return _b64.b64decode(j["content"]).decode("utf-8"), j["sha"]
            except Exception:
                return None, None

        def _priv_put(path, text, sha, msg):
            body = {"message": msg, "content": _b64.b64encode(text.encode()).decode()}
            if sha:
                body["sha"] = sha
            r = requests.put(f"{_API}/{path}", headers=_HDR, json=body, timeout=15)
            return r.status_code in (200, 201)

        import position_manager as _pm
        import importlib as _ilc
        _pm = _ilc.reload(_pm)
        _tmp = Path(_tf.mkdtemp())
        _pm.POS, _pm.TRADES = _tmp / "positions.json", _tmp / "journal" / "trades.json"
        _pos_raw, _pos_sha = _priv_get("positions.json")
        _tr_raw, _tr_sha = _priv_get("journal/trades.json")
        if _pos_raw is None:
            st.error("私仓 positions.json 读取失败（令牌权限或网络）")
        else:
            _pm.POS.write_text(_pos_raw, encoding="utf-8")
            if _tr_raw:
                _pm.TRADES.parent.mkdir(exist_ok=True)
                _pm.TRADES.write_text(_tr_raw, encoding="utf-8")
            if st.session_state.get("_ct_flash"):
                st.success(st.session_state.pop("_ct_flash"))
            # ── 结构化录单：显式选择买/卖；买入支持简称联想，卖出从现持仓选择。 ──
            try:
                _ct_pj = json.loads(_pos_raw)
            except Exception:
                _ct_pj = {"accounts": {}}
            _ct_hold_rows = [
                {"账户": acc, "名称": h.get("name"), "代码": h.get("code"),
                 "股数": h.get("shares"), "成本": h.get("cost"),
                 "级别": str(h.get("level") or "B").upper()}
                for acc, a in (_ct_pj.get("accounts") or {}).items()
                for h in (a.get("holdings") or [])
            ]
            _ct_action = st.radio("交易类型", ["买入 / 加仓", "卖出 / 减仓"], horizontal=True,
                                  key="_ctf_action", help="卖出会写入交易日志并计算已实现盈亏")
            _ct_accounts = _pm.account_names()
            from datetime import date as _date9
            _ct_buy, _ct_sell, _ct_qty, _ct_chosen = "", "", "", None
            _ct_date = _date9.today()

            if _ct_action == "买入 / 加仓":
                _f1, _f2, _f3, _f4, _f5, _f6 = st.columns([2.1, 2.25, 1.0, .85, 1.15, .65])
                _ct_name = _f1.text_input("名称/简称/代码", placeholder="中微 / 腾讯 / NVDA", key="_ctf_buy_name")
                _ct_cands = []
                if _ct_name.strip():
                    try:
                        _ct_cands = _pm.candidates_for(_ct_name.strip(), limit=6) or []
                    except Exception:
                        _ct_cands = []
                if _ct_cands:
                    _ct_opts = [f"{n}（{c}·{m}）" for n, c, m in _ct_cands]
                    _ct_pick = _f2.selectbox("简称匹配（请选择）", _ct_opts, key="_ctf_buy_match")
                    _ct_chosen = _ct_cands[_ct_opts.index(_ct_pick)][1]
                else:
                    _f2.text_input("简称匹配", value="输入简称后自动显示全称和代码", disabled=True,
                                   key="_ctf_buy_match_empty")
                _ct_buy = _f3.text_input("买入价", placeholder="469", key="_ctf_buy_price")
                _ct_qty = _f4.text_input("买入股数", placeholder="100", key="_ctf_buy_qty")
                _ct_date = _f5.date_input("成交日期", value=_date9.today(), key="_ctf_buy_date")
                _ct_account = _f6.selectbox("账户", _ct_accounts, key="_ctf_buy_account")
                _ct_level = st.selectbox("关注级别", ["A", "B", "C"], index=1, key="_ctf_buy_level",
                                         help="人工基础级别；持仓风险仍会自动升为A级")
                _ct_token = _ct_name.strip()
                _ct_button = "✅ 确认买入 / 加仓"
            else:
                _ct_sell_map = {
                    f"{r.get('名称')}（{r.get('代码')}｜{r.get('账户')}｜现持{r.get('股数')}股）": r
                    for r in _ct_hold_rows
                }
                if not _ct_sell_map:
                    st.warning("当前没有可卖出的正式持仓")
                    _ct_selected = {}
                else:
                    _s1, _s2, _s3, _s4 = st.columns([3.0, 1.0, 1.05, 1.25])
                    _ct_sell_label = _s1.selectbox("选择要卖出的持仓", list(_ct_sell_map), key="_ctf_sell_holding")
                    _ct_selected = _ct_sell_map[_ct_sell_label]
                    _ct_sell = _s2.text_input("实际卖出价", placeholder="必填", key="_ctf_sell_price")
                    _ct_qty = _s3.text_input("卖出股数", placeholder="留空=全部", key="_ctf_sell_qty")
                    _ct_date = _s4.date_input("成交日期", value=_date9.today(), key="_ctf_sell_date")
                    st.caption(f"将从 {_ct_selected.get('账户')} 卖出 {_ct_selected.get('名称')}；留空股数表示全部清仓。")
                _ct_token = str(_ct_selected.get("代码") or "")
                _ct_account = str(_ct_selected.get("账户") or (_ct_accounts[0] if _ct_accounts else ""))
                _ct_level = str(_ct_selected.get("级别") or "B")
                _ct_button = "🟥 确认卖出 / 减仓"

            _ct_rsn = st.text_input("原因（选填，随日志留档）", placeholder="如：止盈一半 / 逻辑失效 / 情绪操作复盘",
                                     key="_pt_rsn")

            def _pt_exec(_kw, _chosen=None):
                _out, _needs = _pm.record_trade(chosen_code=_chosen, **_kw)
                if _needs:  # 简称多解 → 弹窗确认
                    st.session_state["_ct_pending"] = {"kw": _kw, "cands": _needs}
                    st.rerun()
                if not _out.startswith(("已录入", "已加仓", "已清仓", "已减仓")):
                    st.error(_out)
                    return
                _new_pos = _pm.POS.read_text(encoding="utf-8")
                _ok = True
                if _new_pos != _pos_raw:
                    _ok = _priv_put("positions.json", _new_pos, _pos_sha, f"持仓终端(云端): {_kw.get('token', '')[:40]}")
                if _pm.TRADES.exists() and _pm.TRADES.read_text(encoding="utf-8") != (_tr_raw or ""):
                    _priv_put("journal/trades.json", _pm.TRADES.read_text(encoding="utf-8"), _tr_sha, "持仓终端(云端): 交易日志")
                if _ok:
                    st.session_state["_ct_flash"] = _out + "　✅已落盘私仓"
                    st.cache_data.clear()
                    st.rerun()  # 刷新下方持仓表——所见即所存
                st.error(f"{_out}（⚠️私仓写入失败，请重试）")

            if st.button(_ct_button, type="primary") and _ct_token:
                try:
                    _pt_exec({"token": _ct_token, "shares": _ct_qty or 0,
                              "buy_px": float(_ct_buy) if _ct_buy.strip() else None,
                              "sell_px": float(_ct_sell) if _ct_sell.strip() else None,
                              "date": str(_ct_date), "reason": _ct_rsn.strip(),
                              "account": _ct_account, "level": _ct_level}, _chosen=_ct_chosen)
                except ValueError:
                    st.error("价格/股数须为数字")
            if st.session_state.get("_ct_pending"):
                @st.dialog("该简称有多个匹配，请确认标的")
                def _ct_pick():
                    _pd = st.session_state["_ct_pending"]
                    _opts = [f"{nm}（{cd}·{mk}）" for nm, cd, mk in _pd["cands"]]
                    _sel = st.selectbox("候选", _opts)
                    _d1, _d2 = st.columns(2)
                    if _d1.button("✅ 确认", type="primary"):
                        _code_sel = _pd["cands"][_opts.index(_sel)][1]
                        st.session_state.pop("_ct_pending")
                        _pt_exec(_pd["kw"], _chosen=_code_sel)
                    if _d2.button("✕ 取消"):
                        st.session_state.pop("_ct_pending")
                        st.rerun()
                _ct_pick()
            # ── 持仓总览 + 按需生命周期体检 ──
            try:
                _pj = json.loads(_pos_raw)
                _rows = [{"账户": acc, "名称": h.get("name"), "代码": h.get("code"),
                          "股数": h.get("shares"), "成本": h.get("cost"),
                          "类别": h.get("class", ""), "级别": str(h.get("level") or "B").upper()}
                         for acc, a in (_pj.get("accounts") or {}).items() for h in (a.get("holdings") or [])]
                _hold_codes = tuple(dict.fromkeys(str(r.get("代码", "")).upper() for r in _rows if r.get("代码")))
                _hold_bar = st.progress(0, text="计算持仓历史最高水位…")
                _hold_water = _cloud_ath_many(_hold_codes) if _hold_codes else {}
                _hold_bar.progress(1.0, text="持仓历史最高水位计算完成")
                _hold_bar.empty()
                for _r in _rows:
                    _r["历史水位"] = _hold_water.get(str(_r.get("代码", "")).upper(), "历史水位待核")
                st.dataframe(_rows, hide_index=True, use_container_width=True)
            except Exception:
                pass
            # 【V88·持仓信念文字】≤50字/只（私仓 data/position_conviction.json，随日报生成）
            try:
                _cv_raw, _ = _priv_get("data/position_conviction.json")
                _cv = json.loads(_cv_raw or "{}")
                _cv_items = _cv.get("items") or {}
                if _cv_items:
                    _cv_lines = [f"🧠 **{v.get('name')}**：{v.get('text')}"
                                 for v in _cv_items.values() if v.get("text")]
                    with st.expander(f"🧠 持仓信念速记（{len(_cv_lines)}只 · {_cv.get('generated_at', '')}）",
                                     expanded=True):
                        st.markdown("\n\n".join(_cv_lines))
                        st.caption("≤50字/只·压住情绪用：跌回成本不是卖出理由，破线才是")
            except Exception:
                pass
            try:
                _trs = json.loads(_tr_raw or "[]")[-5:]
                if _trs:
                    st.caption("最近5笔：" + "　".join(
                        f"{t.get('date', '')[:10]} {t.get('action', '')}{t.get('name', '')}{t.get('shares', '')}股"
                        f"@{t.get('sell_price') or t.get('cost', '')}" for t in reversed(_trs)))
            except Exception:
                pass
            # 【V88·自选分级】A=对应市场交易日盘中每3小时 B=每天 C=每周低频
            with st.expander("🏷️ 自选分级管理（A盘中每3小时｜B每天｜C每周低频）"):
                _wl_raw, _wl_sha = _priv_get("watchlist_v88.json")
                _lvl_raw, _lvl_sha = _priv_get("watch_levels.json")
                # 云端也可直接按名称/简称/代码加入自选，与桌面同一私仓底稿。
                _w1, _w2, _w3 = st.columns([2.1, 2.4, 1.1])
                _wl_token = _w1.text_input("新增自选股", placeholder="中微 / 腾讯 / NVDA", key="_ct_wl_add")
                _wl_cands, _wl_choice = [], None
                if _wl_token.strip():
                    try:
                        import cloud_engine as _ce_wl
                        _wl_cands = _ce_wl.search_candidates(_wl_token.strip(), limit=6) or []
                    except Exception:
                        _wl_cands = []
                if _wl_cands:
                    _wl_opts = [f"{n}（{c}·{m}）" for n, c, m in _wl_cands]
                    _wl_pick = _w2.selectbox("简称匹配", _wl_opts, key="_ct_wl_match")
                    _wl_choice = _wl_cands[_wl_opts.index(_wl_pick)]
                else:
                    _w2.text_input("简称匹配", value="输入简称后自动显示全称和代码", disabled=True,
                                   key="_ct_wl_match_empty")
                _wl_level_new = _w3.selectbox("级别", ["A", "B", "C"], index=1, key="_ct_wl_new_level")
                if st.button("＋ 加入自选股", key="_ct_wl_add_btn"):
                    if not _wl_token.strip():
                        st.warning("请先输入股票名称、简称或代码")
                    elif not _wl_choice:
                        st.error("未识别该股票，请换用准确代码")
                    else:
                        try:
                            _wl_obj = json.loads(_wl_raw or "{}") or {"US": [], "HK": [], "CN": []}
                        except Exception:
                            _wl_obj = {"US": [], "HK": [], "CN": []}
                        _wn, _wc = _wl_choice[0], _wl_choice[1]
                        _wm = "HK" if _wc.endswith(".HK") else ("CN" if _wc.endswith((".SS", ".SZ")) else "US")
                        _exists = any(str(c).upper() == str(_wc).upper()
                                      for rows in _wl_obj.values() for c, _n in (rows or []))
                        if _exists:
                            st.info(f"{_wn}（{_wc}）已在自选中")
                        else:
                            _wl_obj.setdefault(_wm, []).append([_wc, _wn])
                            while sum(len(v) for v in _wl_obj.values()) > 20:
                                _big = max(_wl_obj, key=lambda k: len(_wl_obj[k]))
                                if _wl_obj[_big]:
                                    _wl_obj[_big].pop(0)
                            _new_levels = json.loads(_lvl_raw) if _lvl_raw else {}
                            _new_levels[_wc] = _wl_level_new
                            _ok_wl = _priv_put("watchlist_v88.json",
                                               json.dumps(_wl_obj, ensure_ascii=False, indent=1),
                                               _wl_sha, f"新增自选(云端): {_wn}")
                            if _ok_wl:
                                _priv_put("watch_levels.json",
                                          json.dumps(_new_levels, ensure_ascii=False, indent=1),
                                          _lvl_sha, f"自选分级(云端): {_wn}={_wl_level_new}")
                                st.success(f"已加入 {_wn}（{_wc}）· {_wl_level_new}级")
                                st.rerun()
                            else:
                                st.error("自选写入失败，请重试")
                try:
                    _wl_all = [(str(c), n) for lst in (json.loads(_wl_raw or "{}") or {}).values()
                               for c, n in (lst if isinstance(lst, list) else [])]
                except Exception:
                    _wl_all = []
                _lvls = json.loads(_lvl_raw) if _lvl_raw else {}
                if not _wl_all:
                    st.caption("自选池为空或读取失败")
                else:
                    _new_lvls = dict(_lvls)
                    _wl_codes = tuple(dict.fromkeys(str(c).upper() for c, _n in _wl_all))
                    _wl_bar = st.progress(0, text="计算自选股历史最高水位…")
                    _wl_water = _cloud_ath_many(_wl_codes) if _wl_codes else {}
                    _wl_bar.progress(1.0, text="自选股历史最高水位计算完成")
                    _wl_bar.empty()
                    for _wc, _wn in _wl_all[:30]:
                        _l1, _l2 = st.columns([4, 2])
                        _ww = _wl_water.get(str(_wc).upper(), "历史水位待核")
                        _l1.markdown(f"<div style='padding:4px 0 0 0'>• {_wn} <span style='color:#9ca3af'>({_wc})</span>"
                                     f"<div style='font-size:9px;color:#64748b;margin-left:10px'>{_ww}</div></div>",
                                     unsafe_allow_html=True)
                        _new_lvls[_wc] = _l2.selectbox("级别", ["A", "B", "C"],
                                                       index=["A", "B", "C"].index(_lvls.get(_wc, "B")),
                                                       key=f"_ct_lv_{_wc}", label_visibility="collapsed")
                    if st.button("💾 保存分级", key="_ct_lv_save"):
                        _ok_lv = _priv_put("watch_levels.json",
                                           json.dumps({k: v for k, v in sorted(_new_lvls.items())},
                                                      ensure_ascii=False, indent=1),
                                           _lvl_sha, "自选分级调整(云端)")
                        (st.success if _ok_lv else st.error)("已保存——盘中预警只扫A级+持仓，下轮生效" if _ok_lv else "保存失败，请重试")

            if st.button("🔍 生命周期体检（成本实算·移动止盈·约30秒）"):
                import yfinance as _yf
                import position_lifecycle as _pl
                from position_manager import _resolve  # noqa 复用名录
                from cloud_engine import analyze_trend_full as _atf2
                _peaks_raw, _ = _priv_get("data/position_peaks.json")
                _peaks = json.loads(_peaks_raw) if _peaks_raw else {}
                _nmap = _pl._news_dir_map()  # 当日新闻催化方向（并入卖出判断）
                _out_rows, _bar = [], st.progress(0.0)
                _hl = [(acc, h) for acc, a in (json.loads(_pos_raw).get("accounts") or {}).items()
                       for h in (a.get("holdings") or []) if h.get("cost") and "⚠️" not in str(h.get("code", ""))]
                for _i, (acc, h) in enumerate(_hl):
                    _bar.progress((_i + 1) / max(len(_hl), 1))
                    from cloud_engine import _yf_norm as _yfc
                    try:
                        _df = _yf.Ticker(_yfc(str(h["code"]))).history(period="6mo")
                        _f = _atf2(_df) if _df is not None and len(_df) >= 35 else None
                        _nd = _nmap.get(_pl._norm_code(str(h.get("code", ""))), 0)
                        _rp = _pl._risk_probs_from_df(_df) if _f is not None else None
                        _r = _pl.assess(h, _f, _peaks, news_dir=_nd, risk_probs=_rp) if _f else {}
                        if _r:
                            _out_rows.append({"关注": _r.get("urgency", ""),
                                              "持仓": f"{h.get('name')}({h.get('code')})",
                                              "浮盈": f"{_r['pnl']:+.1f}%", "峰值": f"{_r['peak']:+.1f}%",
                                              "阶段": _r["stage"], "动作": _r["action"],
                                              "信号": "；".join(_r["signals"]) or "—",
                                              "减仓/退出": f"{_r['reduce']}；{_r['exit']}"})
                    except Exception:
                        continue
                _bar.empty()
                _urank = {"今日": 0, "明日": 1, "本周": 2, "本月": 3}
                _out_rows.sort(key=lambda x: _urank.get(x.get("关注", ""), 3))
                st.dataframe(_out_rows, hide_index=True, use_container_width=True)
                st.caption("关注紧迫度：今日>明日>本周>本月。规则：峰值浮盈≥15%进利润保护期；回撤超10点或趋势破坏→锁盈减仓；"
                           "+20%起逐档评估；成长-10%/核心-12%破MA55纪律退出。🔻利空催化/回撤概率≥60%→自动收紧动作与紧迫度")

st.divider()
st.caption("V88 云端版 · 仅供研究参考，不构成投资建议")
