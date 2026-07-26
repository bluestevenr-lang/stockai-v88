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
    # 【V88·今日指令牌·云端版 2026-07-25 用户"全系统更新"】与桌面同款开屏三要素:
    # 定调+三市场周概率 / 进攻·防守点名 / 实盘战绩+✨最新升级。全读pub零计算。
    try:
        _bn_cells9c, _bn_chgs9c = [], []
        for _bm9c in ("美股", "A股", "港股"):
            _bb9c = ((_snap or {}).get("markets") or {}).get(_bm9c) or {}
            _bl9c = dict((x[0], x[1]) for x in ((_bb9c.get("l3") or {}).get("probs") or []))
            _bc9c = float(((_bb9c.get("indices") or [{}])[0] or {}).get("chg1d") or 0)
            _bn_chgs9c.append(_bc9c)
            if _bl9c.get("2周") is not None:
                _p9c2 = int(_bl9c["2周"])
                _pc9c2 = "#dc2626" if _p9c2 >= 55 else ("#16a34a" if _p9c2 <= 45 else "#64748b")
                _bn_cells9c.append(f"{_bm9c}周概率<b style='color:{_pc9c2}'>{_p9c2}%</b>")
        _bn_tone9c, _bn_tc9c = (("🛡️ 防守日", "#16a34a") if (_bn_chgs9c and min(_bn_chgs9c) <= -1.5) else
                                (("⚔️ 进攻日", "#dc2626") if (_bn_chgs9c and max(_bn_chgs9c) >= 1.5) else
                                 ("⚖️ 中性日", "#2563eb")))
        _bn_go9c = []
        try:
            for _bh9c in (json.loads(pub_text("darkhorse.json", _PUB_VERSION) or "{}").get("horses") or []):
                if ((_bh9c.get("trade_plan") or {}).get("short") or {}).get("mode") in (
                        "现价可进", "回踩到位", "突破确认"):
                    _bn_go9c.append((_bh9c.get("name"), int(_bh9c.get("p_up") or 0)))
        except Exception:
            pass
        _bn_go9c.sort(key=lambda x: -x[1])
        _bn_go_txt9c = ("、".join(f"{n}{p}%" for n, p in _bn_go9c[:2])
                        + (f" 等{len(_bn_go9c)}只" if len(_bn_go9c) > 2 else "")) if _bn_go9c             else "今日无绿灯(现金也是仓位)"
        _bn_new9c = ""
        try:
            _cl9c = (json.loads(pub_text("v88_changelog.json", _PUB_VERSION) or "{}").get("rows") or [])
            if _cl9c:
                _bn_new9c = ("　<span style='background:#fef9c3;border-radius:4px;padding:0 4px'>✨新:"
                             + str(_cl9c[0].get("t"))[:36] + "</span>")
        except Exception:
            pass
        st.markdown(
            f"<div style='background:linear-gradient(90deg,{_bn_tc9c}11,transparent);"
            f"border:1px solid {_bn_tc9c}44;border-left:5px solid {_bn_tc9c};border-radius:10px;"
            f"padding:.5rem .8rem;margin-bottom:.4rem'>"
            f"<div style='font-size:15px;font-weight:800;color:{_bn_tc9c}'>📣 今日V88 · {_bn_tone9c}"
            f"<span style='font-size:12.5px;font-weight:400;color:#475569'>　{'｜'.join(_bn_cells9c)}</span></div>"
            f"<div style='font-size:13px;margin-top:2px'>🐉 <b style='color:#dc2626'>进攻</b>:{_bn_go_txt9c}"
            f"　⚔️ <b style='color:#16a34a'>防守</b>:见下方地狱门(持仓属私域)"
            + _bn_new9c + "</div>"
            f"<div style='font-size:11px;color:#94a3b8'>与桌面同源(pub快照)·明细在下方双门/关注中心/四档预判</div>"
            "</div>", unsafe_allow_html=True)
    except Exception:
        pass
    # 【V88·时间作战板·云端版 2026-07-25 用户"全系统更新"】与桌面同款六档六问,全读pub;
    # ⑤近档持仓警示属私域(无token如实说明),远档=全池相位转弱公开可显。
    try:
        from datetime import datetime as _dtc9, timedelta as _tdc9
        _is_we9c = _dtc9.now().weekday() >= 5

        def _pubj9(_fn):
            try:
                return json.loads(pub_text(_fn, _PUB_VERSION) or "{}")
            except Exception:
                return {}
        # 【V88·今日确认买单·云端 2026-07-25 用户定纲"要清晰的告知和确认该买了"】
        # pub源=五行业代表+黑马绿灯(受发言权闸);自选/持仓池确认单在桌面/飞书完整版。
        try:
            from datetime import datetime as _dtcb9, timedelta as _tdcb9
            _ntc9 = _dtcb9.now().weekday() >= 5
            _dayc9 = (f"下一交易日(周一{(_dtcb9.now() + _tdcb9(days=7 - _dtcb9.now().weekday())).strftime('%m-%d')})"
                      if _ntc9 else "今日")
            _cbc_rows9 = []
            for _sv9b in (_pubj9("sector_reps.json").get("sectors") or []):
                _pk9b = _sv9b.get("pick") or {}
                _sh9b = _pk9b.get("shallow") or [0, 0]
                _dist9b = ((_pk9b.get("last", 0) / _sh9b[1] - 1) * 100 if _sh9b[1] else 99)
                if str(_pk9b.get("mode")) == "现价可进" and _dist9b <= 5:
                    _cbc_rows9.append(f"✅ <b style='color:#dc2626'>该买:{_pk9b.get('name')}"
                                      f"({_pk9b.get('sym')})</b> 现{_pk9b.get('last')}"
                                      f"·2周涨{_pk9b.get('p_up')}%·回踩带{_sh9b[0]}~{_sh9b[1]}"
                                      f"·失效{_pk9b.get('invalid')}·{_sv9b['sector']}代表")
            if _cbc_rows9:
                st.markdown("<div style='background:#fef2f2;border:2px solid #dc2626;border-radius:10px;"
                            "padding:.4rem .7rem;margin:.3rem 0;font-size:13.5px'>"
                            + f"<b style='color:#dc2626'>🔔 {_dayc9}确认买单(公开源)</b>"
                            + "<span style='font-size:11px;color:#94a3b8'>·自选/持仓池确认单见桌面/飞书"
                            + ("·休市:周一开盘复核价后执行" if _ntc9 else "") + "</span><br>"
                            + "<br>".join(_cbc_rows9) + "</div>", unsafe_allow_html=True)
            else:
                st.caption(f"🔕 {_dayc9}无确认买单(公开源)——现金也是仓位;自选池确认单见桌面/飞书")
        except Exception:
            pass
        # 【V88·七档 2026-07-25 用户定纲】与桌面同源:去明日档,加本季度/下季度(16/32周)
        _TBC9 = {
            "今日": {"mkt_hz": None, "sec_hz": None, "buy": "green", "hz": None, "evt": "明天", "days": (0, 1)},
            "本周": {"mkt_hz": "2周", "sec_hz": "2周", "buy": "green+", "hz": None, "evt": "本周", "days": (0, 7)},
            "下周": {"mkt_hz": "4周", "sec_hz": "5周", "buy": "w4", "hz": "4周", "evt": "下周", "days": (7, 14)},
            "本月": {"mkt_hz": "4周", "sec_hz": "8周", "buy": "w4", "hz": "4周", "evt": "本月及下月", "days": (0, 30)},
            "下月": {"mkt_hz": "8周", "sec_hz": "16周", "buy": "w8", "hz": "8周", "evt": "本月及下月", "days": (30, 60)},
            "本季度": {"mkt_hz": "16周", "sec_hz": "16周", "buy": "w16", "hz": "16周", "evt": "本月及下月", "days": (0, 90)},
            "下季度": {"mkt_hz": "32周", "sec_hz": "16周", "buy": "w32", "hz": "32周", "evt": "本月及下月", "days": (90, 180)},
        }
        with st.expander("⏱ 时间作战板 · 今日→下季度七档切换（与桌面同源·一屏六问）", expanded=True):
            _tt9c = st.radio("时间档", list(_TBC9.keys()), index=(2 if _is_we9c else 0),
                             horizontal=True, key="tb_tier9c", label_visibility="collapsed")
            _cf9c = _TBC9[_tt9c]
            _dh9c2 = _pubj9("darkhorse.json")
            _pt9c2 = _pubj9("phase_turn_full.json")
            # 【V88·明日作战预案·云端脱敏版 2026-07-25】今日/明日档展示if-then剧本(pub版无持仓行)
            try:
                _tpc9 = _pubj9("tomorrow_plan_pub.json")
                _tpfd9 = str(_tpc9.get("for_date") or "")
                if (_tpc9.get("script") and _tt9c in ("今日", "明日") and _tpfd9 and
                        (_tpfd9 >= _dtc9.now().strftime("%Y-%m-%d") if _tt9c == "明日"
                         else _tpfd9 == _dtc9.now().strftime("%Y-%m-%d"))):
                    _tph9 = str(_tpc9["script"])
                    for _sc9, _ic9 in (("## 大盘剧本", "🎬 大盘剧本"), ("## 买", "🐉 买"),
                                       ("## 卖·防", "⚔️ 卖·防"), ("## 准备", "🕐 准备")):
                        _tph9 = _tph9.replace(_sc9, f"**{_ic9}**")
                    st.markdown(f"🎬 **{_tpfd9} 作战预案**（前一晚if-then剧本·持仓行已脱敏,"
                                f"完整版在桌面/飞书·{_tpc9.get('generated_at', '')}）")
                    st.markdown(_tph9)
            except Exception:
                pass
            # 【V88·行动指令·云端 2026-07-25 用户抓"没说该干什么"】脱敏版:宏观事件(pub)+
            # 准备买给计数(自选明细=隐私,完整名单在桌面/飞书)
            try:
                _snpc9i = _pubj9("market_snapshot.json")
                _mev9c = (_pubj9("macro_events_pub.json").get("events") or [])
                from datetime import datetime as _dt9ci
                _d0c, _d1c = _cf9c["days"]
                _tdyc9 = _dt9ci.now().date()
                _evc9 = [e for e in _mev9c if _d0c <= (_dt9ci.strptime(e["date"], "%Y-%m-%d").date()
                                                       - _tdyc9).days <= max(_d1c, 1)]
                _wkc9, _posc9 = [], []
                for _m9ci in ("A股", "港股", "美股"):
                    _b9ci = (_snpc9i.get("markets") or {}).get(_m9ci) or {}
                    _p9ci = dict((x[0], x[1]) for x in ((_b9ci.get("l3") or {}).get("probs") or [])).get(
                        _cf9c.get("mkt_hz") or "明日")
                    if _p9ci is not None:
                        _wkc9.append((_m9ci, int(_p9ci)))
                    _t9ci = (_b9ci.get("temperature") or {})
                    if _t9ci.get("position"):
                        _posc9.append(f"{_m9ci}{str(_t9ci['position']).split('（')[0]}")
                _tk9c = []
                _st9c = [m for m, p in _wkc9 if p >= 55]
                _wk9c2 = [m for m, p in _wkc9 if p <= 45]
                if _st9c:
                    _tk9c.append(f"<b style='color:#dc2626'>{'/'.join(_st9c)}偏涨·持有敢接</b>")
                if _wk9c2:
                    _tk9c.append(f"<b style='color:#16a34a'>{'/'.join(_wk9c2)}偏弱·反弹减仓不追</b>")
                if _evc9:
                    _tk9c.append("📅" + "、".join(f"{e['date'][5:]}(周{e['dow']}){e['event']}"
                                                 for e in _evc9[:3]))
                _tk9c.append("🎯准备买触发单明细在桌面/飞书(自选隐私)")
                st.markdown("<div style='background:#fef9c3;border-left:4px solid #ca8a04;border-radius:6px;"
                            "padding:.35rem .6rem;margin:.2rem 0 .4rem 0;font-size:13px'>"
                            f"<b>📌 {_tt9c}行动指令</b>　仓位:{('·'.join(_posc9[:3])) or '见四档预判'}"
                            f"　{'　'.join(_tk9c)}</div>", unsafe_allow_html=True)
            except Exception:
                pass
            _cL9, _cR9 = st.columns(2)
            with _cL9:
                _ra9 = []
                for _m9c in ("美股", "A股", "港股"):
                    _b9c = ((_snap or {}).get("markets") or {}).get(_m9c) or {}
                    _l9c2 = dict((x[0], x[1]) for x in ((_b9c.get("l3") or {}).get("probs") or []))
                    _t9c2 = _b9c.get("temperature") or {}
                    _cg9c2 = float(((_b9c.get("indices") or [{}])[0] or {}).get("chg1d") or 0)
                    if _cf9c["mkt_hz"] is None:
                        _cc9 = "#dc2626" if _cg9c2 > 0.05 else ("#16a34a" if _cg9c2 < -0.05 else "#64748b")
                        _core9 = (f"今日<b style='color:{_cc9}'>{_cg9c2:+.2f}%</b>·温度{_t9c2.get('temp', '?')}°"
                                  f"→仓位{str(_t9c2.get('position', '?')).split('（')[0]}")
                    else:
                        if _cf9c["mkt_hz"] == "明日":
                            _pv9c = int(round(max(25, min(75, 50 + 2.2 * _cg9c2
                                                          + 0.15 * (float(_t9c2.get('temp') or 50) - 50)))))
                            _tg9c = "(动量+温度估计·低置信)"
                        else:
                            _px9c = _l9c2.get(_cf9c["mkt_hz"])
                            if _px9c is None:
                                continue
                            _pv9c, _tg9c = int(_px9c), f"({_cf9c['mkt_hz']}档)"
                        _pc9c3 = "#dc2626" if _pv9c >= 55 else ("#16a34a" if _pv9c <= 45 else "#64748b")
                        _w9c = ("偏涨·回踩敢接" if _pv9c >= 55 else
                                ("偏弱·反弹先减不追" if _pv9c <= 45 else "震荡·区间对待"))
                        _core9 = (f"{_tt9c}上涨概率<b style='color:{_pc9c3}'>{_pv9c}%</b>→{_w9c}"
                                  f"<span style='font-size:11px;color:#94a3b8'>{_tg9c}</span>")
                    _ra9.append(f"<div style='font-size:13px'><b>{_m9c}</b> {_core9}</div>")
                st.markdown(f"<b style='font-size:13px'>🧭 ① 大盘·{_tt9c}</b>" + "".join(_ra9),
                            unsafe_allow_html=True)
                _rb9 = []
                for _m9c in ("美股", "A股", "港股"):
                    _tl9c = ((_snap or {}).get("rotation_forecast") or {}).get("trajectories", {}).get(_m9c) or []
                    if _cf9c["sec_hz"] is None:
                        _sc9c = ((_snap or {}).get("markets") or {}).get(_m9c, {}).get("sectors") or []
                        if _sc9c:
                            _u9c = sorted(_sc9c, key=lambda s: -(s.get("chg1d") or 0))[:2]
                            _rb9.append(f"<div style='font-size:12.5px'><b>{_m9c}</b> 领涨:"
                                        + "、".join(f"{s['name']}{(s.get('chg1d') or 0):+.1f}%" for s in _u9c)
                                        + "</div>")
                        continue
                    _hz9c = _cf9c["sec_hz"]
                    _tp9c = sorted(_tl9c, key=lambda t: -((t.get("points") or {}).get(_hz9c) or {}).get("score", 0))[:2]
                    _nr9c = [t.get("name") for t in _tl9c
                             if str((t.get("turning") or {}).get("horizon")) == _hz9c]
                    if _tp9c:
                        _rb9.append(f"<div style='font-size:12.5px'><b>{_m9c}</b> {_hz9c}最强:"
                                    + "、".join(f"{t.get('name')}({int(((t.get('points') or {}).get(_hz9c) or {}).get('score', 0))})"
                                                for t in _tp9c)
                                    + (f"　<b style='color:#dc2626'>⏰拐点:{'、'.join(_nr9c)}</b>" if _nr9c else "")
                                    + "</div>")
                st.markdown(f"<b style='font-size:13px'>🔄 ② 板块轮转·{_tt9c}</b>" + "".join(_rb9),
                            unsafe_allow_html=True)
                _up9c2 = sorted([x for x in (_pt9c2.get("stocks") or [])
                                 if x.get("direction") == "up" and x.get("confidence") in ("高", "中")
                                 and float(x.get("pos52") or 50) <= 45],
                                key=lambda x: ({"高": 0, "中": 1}.get(str(x.get("confidence")), 2),
                                               float(x.get("pos52") or 50)))[:8]
                # 【V88·资金流💰标注·云端 2026-07-25】读脱敏版(个股仅全池转强名单,无自选/持仓码)
                _ffc9 = (_pubj9("fund_flow_pub.json").get("stocks") or {})

                def _fftag9c(_cd):
                    _n = (_ffc9.get(str(_cd)) or {}).get("net")
                    return f"·💰主力+{_n:.1f}亿" if (_n is not None and _n >= 0.1) else ""
                st.markdown("<b style='font-size:13px'>🌱 ③ 低位拐点·可注意埋伏</b>"
                            f"<span style='font-size:11px;color:#94a3b8'>（全池{_pt9c2.get('scanned', '?')}只·转强≠立刻买·💰=主力今日净流入）</span>"
                            + ("<div style='font-size:12.5px'>" + "、".join(
                                f"{x.get('name')}(52周{int(float(x.get('pos52') or 0))}%·{x.get('confidence')}{_fftag9c(x.get('code'))})"
                                for x in _up9c2) + "</div>" if _up9c2
                               else "<div style='font-size:12.5px;color:#94a3b8'>全池文件待发布或暂无低位转强</div>"),
                            unsafe_allow_html=True)
                # 【V88·U3拐点倒计时+前置信号·云端 2026-07-26】pub版(大盘/板块级)
                try:
                    _tfc9 = _pubj9("turning_forecast_pub.json")
                    _tfr9 = (_tfc9.get("rows") or [])[:5]
                    if _tfr9:
                        st.markdown("<b style='font-size:13px'>⏳ 拐点倒计时</b>"
                                    "<span style='font-size:11px;color:#94a3b8'>（个股级在桌面/飞书）</span>"
                                    + "".join(f"<div style='font-size:12.5px'>{'🔺' if r.get('side') == 'top' else '🌱'} "
                                              f"{r.get('name')} <b>预计{(r.get('window_days') or ['?', '?'])[0]}～"
                                              f"{(r.get('window_days') or ['?', '?'])[1]}个交易日内"
                                              f"{'可能见顶' if r.get('side') == 'top' else '可能见底'}·强度{r.get('prob')}/100"
                                              f"<span style='font-weight:400;font-size:11px'>({'强预警' if int(r.get('prob') or 0) >= 70 else ('值得留意' if int(r.get('prob') or 0) >= 55 else '弱信号')})</span></b>"
                                              f"·{'跌破' if r.get('side') == 'top' else '站上'}{r.get('confirm_price')}才算数"
                                              + (f"·⚡{str(r.get('event'))[:26]}" if r.get("event") else "")
                                              + "</div>" for r in _tfr9), unsafe_allow_html=True)
                    _psc9 = _pubj9("pre_signals.json")
                    _psl9 = [f"💰{f.get('name')}·{str(f.get('note'))[:30]}" for f in (_psc9.get("fund_lead") or [])[:2]] +                             [f"📊{str(e.get('name'))[:16]}·{str(e.get('note'))[:32]}" for e in (_psc9.get("earn_gap") or [])[:2]]
                    if _psl9:
                        st.markdown("<b style='font-size:13px'>📡 前置信号</b><div style='font-size:12px'>"
                                    + "<br>".join(_psl9) + "</div>", unsafe_allow_html=True)
                except Exception:
                    pass
                # 【V88·五行业代表·云端 2026-07-25】与桌面同源(pub无隐私,候选皆公开大票)
                try:
                    _sruniv9 = _pubj9("sector_reps.json")
                    # 【2026-07-26 三市场版】云端直接显示三市场全部(每市场一组)
                    _srv_grps9 = (_sruniv9.get("markets") or
                                  ({"美股": _sruniv9["sectors"]} if _sruniv9.get("sectors") else {}))
                    if _srv_grps9:
                        _srv_rows9 = []
                        for _mk9v, _fl9v in (("A股", "🇨🇳"), ("港股", "🇭🇰"), ("美股", "🇺🇸")):
                            _grp9v = _srv_grps9.get(_mk9v) or []
                            if not _grp9v:
                                continue
                            _srv_rows9.append(f"<div style='font-size:12px;color:#64748b;margin-top:3px'>"
                                              f"<b>{_fl9v}{_mk9v}</b></div>")
                            for _sv9c in _grp9v:
                                _pk9c = _sv9c.get("pick") or _sv9c.get("watch") or {}
                                _sh9c = _pk9c.get("shallow") or ["?", "?"]
                                _srv_rows9.append(
                                    "<div style='font-size:12.5px'>"
                                    f"<b>{_sv9c['sector']}</b>·{'代表' if _sv9c.get('pick') else '仅观察'} "
                                    f"{_pk9c.get('name')}({_pk9c.get('sym')})"
                                    + (f"<b style='color:#dc2626'>{_pk9c.get('p_up')}%</b>" if _sv9c.get("pick") else "")
                                    + f"<span style='font-size:11.5px;color:#475569'>·现{_pk9c.get('last')}"
                                    f"·回踩带{_sh9c[0]}~{_sh9c[1]}·突破{_pk9c.get('breakout')}"
                                    f"·失效{_pk9c.get('invalid')}</span>"
                                    + (f"<span style='font-size:11px;color:#b45309'>·📅{_pk9c['earn']}财报</span>"
                                       if _pk9c.get("earn") else "") + "</div>")
                        st.markdown("<b style='font-size:13px'>🎖️ 五行业代表·三市场·下周关注</b>"
                                    f"<span style='font-size:11px;color:#94a3b8'>（统一引擎实跑择优·"
                                    f"{str(_sruniv9.get('generated_at'))[5:16]}）</span>"
                                    + "".join(_srv_rows9), unsafe_allow_html=True)
                except Exception:
                    pass
            with _cR9:
                def _hzs9c(_h, _hz):
                    try:
                        return float((((_h.get("facts") or {}).get("horizons") or {}).get(_hz) or {}).get("rule_score"))
                    except (TypeError, ValueError):
                        return None
                # 【V88·发言权规则 2026-07-25 用户批准】黑马实盘<50%(n≥5)→买名单撤出第一屏
                try:
                    _dv9c = ((_pubj9("success_rates.json").get("types") or {}).get("darkhorse") or {})
                    _dh_gate9c = int(_dv9c.get("n") or 0) >= 5 and (_dv9c.get("rate") or 100) < 50
                except Exception:
                    _dh_gate9c = False
                _by9c = []
                for _h9c2 in ([] if _dh_gate9c else (_dh9c2.get("horses") or [])):
                    _md9c = str(((_h9c2.get("trade_plan") or {}).get("short") or {}).get("mode") or "")
                    if _cf9c["buy"] == "green" and _md9c in ("现价可进", "回踩到位", "突破确认"):
                        _by9c.append((_h9c2, int(_h9c2.get("p_up") or 0), _md9c))
                    elif _cf9c["buy"] == "trigger" and _md9c in ("双路径待触发", "回踩到位", "突破确认"):
                        _by9c.append((_h9c2, int(_h9c2.get("p_up") or 0), f"盯触发·{_md9c}"))
                    elif _cf9c["buy"] == "green+" and _md9c in ("现价可进", "回踩到位", "突破确认", "双路径待触发"):
                        _by9c.append((_h9c2, int(_h9c2.get("p_up") or 0), _md9c))
                    elif _cf9c["buy"] in ("w4", "w8", "w16", "w32"):
                        _s9c2 = _hzs9c(_h9c2, _cf9c.get("hz") or "4周")
                        if _s9c2 is not None and _s9c2 >= 58:
                            _by9c.append((_h9c2, int(_s9c2), "周期走强·分批"))
                _by9c.sort(key=lambda x: -x[1])
                st.markdown(f"<b style='font-size:13px'>🐉 ④ {_tt9c}买什么</b>"
                            f"<span style='font-size:11px;color:#94a3b8'>（公开黑马池口径·{len(_by9c)}只）</span>"
                            + ("".join(f"<div style='font-size:12.5px'>{_h9x.get('name')}"
                                       f"<b style='color:#dc2626'>{_p9x}%</b>"
                                       f"<span style='font-size:11.5px;color:#475569'>·{_t9x}"
                                       f"·{str(((_h9x.get('trade_plan') or {}).get('short') or {}).get('in') or '')[:36]}</span></div>"
                                       for _h9x, _p9x, _t9x in _by9c[:6]) if _by9c
                               else ("<div style='font-size:12.5px;color:#94a3b8'>🔇黑马池实盘<50%已降级研究参考"
                                     "——点名暂停,名单仍在下方黑马模块(战绩回升自动恢复)</div>" if _dh_gate9c else
                                     "<div style='font-size:12.5px;color:#94a3b8'>本档暂无达标——空仓等待也是决策</div>")),
                            unsafe_allow_html=True)
                if _tt9c in ("今日", "明日", "本周"):
                    st.markdown("<b style='font-size:13px'>⚔️ ⑤ 卖/持仓要处理</b>"
                                "<div style='font-size:12px;color:#94a3b8'>持仓警示属私域——见桌面/飞书,"
                                "或导航下方地狱门(配PRIVATE_TOKEN显示)</div>", unsafe_allow_html=True)
                else:
                    _ct9c = sorted([x for x in (_pt9c2.get("stocks") or [])
                                    if x.get("direction") == "down" and x.get("confidence") in ("高", "中")],
                                   key=lambda x: -float(x.get("strength") or 0))[:6]
                    st.markdown(f"<b style='font-size:13px'>⚔️ ⑤ {_tt9c}躲什么</b>"
                                "<span style='font-size:11px;color:#94a3b8'>（全池相位转弱·中长线躲避参考）</span>"
                                + ("".join(f"<div style='font-size:12.5px'>{x.get('name')}"
                                           f"<span style='font-size:11.5px;color:#16a34a'>·{x.get('phase')}"
                                           f"·{x.get('confidence')}置信</span></div>" for x in _ct9c) if _ct9c
                                   else "<div style='font-size:12.5px;color:#94a3b8'>暂无高中置信转弱</div>"),
                                unsafe_allow_html=True)
                _ai9c2 = (_pubj9("institutional_signals.json").get("ai_brief") or {})
                _iw9c = []
                for _m9c in ("A股", "港股", "美股"):
                    _sb9c = _ai9c2.get(_m9c) or {}
                    _v9c2 = _sb9c.get(_cf9c["evt"]) if isinstance(_sb9c, dict) else None
                    if _v9c2 and "材料不足" not in str(_v9c2):
                        _iw9c.append(f"{_m9c}:{_v9c2}")
                _ev9c = [f"🆕转债{_c9c.get('bond')}({str(_c9c.get('apply_date'))[5:]}申购)"
                         for _c9c in (_pubj9("announcements.json").get("cb_calendar") or [])[:3]]
                st.markdown(f"<b style='font-size:13px'>📅 ⑥ {_tt9c}事件与消息面</b>"
                            + ("<div style='font-size:12.5px'>" + "　".join(_ev9c) + "</div>" if _ev9c else "")
                            + (f"<div style='font-size:12px;color:#7c3aed'>🏛️机构{_cf9c['evt']}观点:"
                               + "｜".join(_iw9c[:3]) + "</div>" if _iw9c
                               else "<div style='font-size:12px;color:#94a3b8'>本档暂无机构观点材料</div>"),
                            unsafe_allow_html=True)
            _tsl9c = []
            for _lb9t, _sc9t in (("快照", (_snap or {}).get("generated_at")),
                                 ("轮动", ((_snap or {}).get("rotation_forecast") or {}).get("analysis_time")),
                                 ("黑马", _dh9c2.get("generated_at")),
                                 ("全池相位", _pt9c2.get("generated_at"))):
                if _sc9t:
                    _tsl9c.append(f"{_lb9t}{str(_sc9t)[5:16]}")
            st.caption("🕒 分析时间: " + " · ".join(_tsl9c)
                       + f" ｜ 当前档:{_tt9c}·与桌面同源(pub快照)"
                       " ｜ 更新节奏:交易日07/13/19点三班·周末每日09:00一趟(各档同源同步·预算内)"
                       " ｜ 持仓明细守隐私铁律")
    except Exception:
        pass
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
    # 【V88·三档双向关注 2026-07-20 用户定纲】与桌面「关注中心」同口径：
    # ①今日及本周 ②下周 ③本月及下月，每档🐉看涨(龙虎门口径)+⚔️看跌(鬼门关口径)，
    # 每只带导致涨/跌的事+概率标注(对应周期方向分,🎯=概率≥65高把握)。
    # 云端候选=公开黑马池∪涨停接力(pub安全)；持仓/自选实时决策在桌面session不重算,
    # 持仓破位警示属私域→见下方双门模块(PRIVATE_TOKEN私径)。
    try:
        _tw_dh9 = json.loads(pub_text("darkhorse.json", _PUB_VERSION) or "{}")

        def _tw_upside9(_h):
            """上行空间%：阻力位/短线目标/中线目标÷现价。【可买纪律 2026-07-20】<10%或不明→不推。"""
            try:
                _u = float(_h.get("upside_pct") or 0)
                if _u:
                    return _u
            except (TypeError, ValueError):
                pass
            try:
                _last = float(_h.get("last") or 0)
            except (TypeError, ValueError):
                _last = 0.0
            if _last <= 0:
                return None
            import re as _re9t
            _cands = []
            for _leg in ("short", "mid"):
                _m = _re9t.search(r"目标([\d.]+)",
                                  str(((_h.get("trade_plan") or {}).get(_leg) or {}).get("out") or ""))
                if _m:
                    try:
                        _cands.append((float(_m.group(1)) / _last - 1) * 100)
                    except ValueError:
                        continue
            # 取最大者=完整波段空间（短线10日目标常仅5%上下）
            return max(_cands) if _cands else None

        def _tw_hz9(_h, _lab):
            try:
                _v = (((_h.get("facts") or {}).get("horizons") or {}).get(_lab) or {}).get("rule_score")
                return int(round(float(_v))) if _v is not None else None
            except (TypeError, ValueError):
                return None

        def _tw_why9(_h):
            for _t in (str(_h.get("touch") or ""), str(_h.get("cycle_note") or ""),
                       str(_h.get("entry_note") or "")):
                if _t.strip():
                    return _t.strip()[:20]
            return "纯技术驱动"

        _tw_tiers9 = {_k: {"bull": [], "bear": []} for _k in ("t1", "t2", "t3")}
        _tw_skip9 = 0
        for _h in (_tw_dh9.get("horses") or []):
            _md9 = str(((_h.get("trade_plan") or {}).get("short") or {}).get("mode") or "")
            _s2t, _s4t, _s8t = _tw_hz9(_h, "2周"), _tw_hz9(_h, "4周"), _tw_hz9(_h, "8周")
            _pu9t = int(_h.get("p_up") or 0) or (_s2t or 0)
            # 🐉 看涨漏斗（同侧只进最先命中的一档）
            # 【可买纪律 2026-07-20 用户定纲】涨停接力(做T性质)撤出推荐；空间<10%或不明不推
            if _md9 in ("现价可进", "回踩到位", "突破确认", "双路径待触发"):
                _up9t = _tw_upside9(_h)
                if _up9t is None or _up9t < 10:
                    _tw_skip9 += 1
                elif _md9 == "双路径待触发":
                    _tw_tiers9["t1"]["bull"].append((_h, _pu9t or 52, f"本周·双路径待触发·空间约+{_up9t:.0f}%"))
                else:
                    _tw_tiers9["t1"]["bull"].append((_h, _pu9t or 55, f"今日·{_md9}·空间约+{_up9t:.0f}%"))
            elif _s2t is not None and _s2t >= 58:
                _tw_tiers9["t2"]["bull"].append((_h, _s2t, "下周·短周期走强"))
            elif _s4t is not None and _s4t >= 58:
                _tw_tiers9["t3"]["bull"].append((_h, _s4t, "本月·4周周期走强"))
            elif _s8t is not None and _s8t >= 58:
                _tw_tiers9["t3"]["bull"].append((_h, _s8t, "下月·8周周期走强"))
            # ⚔️ 看跌漏斗
            _pd9t = int(_h.get("p_down") or 0)
            if any(_k in str(_h.get("action") or "") for _k in ("减仓", "退出", "清仓", "回避", "止损")):
                _tw_tiers9["t1"]["bear"].append((_h, _pd9t or 55, "今日·风控动作"))
            elif _s2t is not None and _s2t <= 42:
                _tw_tiers9["t1"]["bear"].append((_h, 100 - _s2t, "本周·2周周期转弱"))
            elif _s4t is not None and _s4t <= 42:
                _tw_tiers9["t3"]["bear"].append((_h, 100 - _s4t, "本月·4周周期偏弱"))
            elif _s8t is not None and _s8t <= 42:
                _tw_tiers9["t3"]["bear"].append((_h, 100 - _s8t, "下月·8周周期偏弱"))
        for _k9t in _tw_tiers9:
            for _sd9t in ("bull", "bear"):
                _tw_tiers9[_k9t][_sd9t].sort(key=lambda x: -x[1])
        # 🏛 机构风向标一句话（下周/本月档补充；分市场新结构，某市场无材料如实跳过）
        _tw_ab9 = {}
        try:
            _ab_raw9 = (json.loads(pub_text("institutional_signals.json", _PUB_VERSION) or "{}")
                        .get("ai_brief") or {})
            for _mk9t, _v9t in _ab_raw9.items():
                if isinstance(_v9t, dict):
                    for _kk9t in ("下周", "本月及下月"):
                        _t9t = str(_v9t.get(_kk9t) or "")
                        if _t9t and "无该市场机构材料" not in _t9t:
                            _tw_ab9.setdefault(_kk9t, []).append(f"{_mk9t}:{_t9t[:26]}")
        except Exception:
            pass

        def _tw_item9(_h, _p, _s, _bear):
            _pc9t = "#16a34a" if _bear else "#dc2626"
            return (f"<b>{_h.get('name')}</b><span style='color:#94a3b8;font-size:11px'>"
                    f"{_h.get('code')}</span><span style='font-size:12px;color:#64748b'>"
                    f"[{_s}]·{_tw_why9(_h)}</span>"
                    f"<b style='color:{_pc9t};font-size:12px'>·{'🎯' if _p >= 65 else ''}"
                    f"{'跌' if _bear else '涨'}概率{_p}%</b>")

        # 【V88·计划做T 2026-07-20 用户定纲】做T可以：把握分≥90·最多3只·必标【计划做T】。
        # 把握分与桌面同口径：基45+封单(≥5亿+25/≥2亿+15/≥1亿+8)+换手3~15%+10+主线共振+15
        # +近30日接力命中≥60%再+10；硬校准上限=60+实盘命中率÷2(战绩差的日子凑不出90,如实空档)。
        _t_cells9c = []
        try:
            _zt9c = json.loads(pub_text("limit_up_radar.json", _PUB_VERSION) or "{}")
            _rr9c = ((json.loads(pub_text("success_rates.json", _PUB_VERSION) or "{}")
                      .get("types") or {}).get("relay") or {}).get("rate")
            _cap9c = 60 + (float(_rr9c) / 2 if _rr9c is not None else 0)
            _mains9c = {str(m.get("industry")) for m in (_zt9c.get("mainlines") or [])[:3]}
            _tt9c = []
            for _r9c in (_zt9c.get("relay") or []):
                _sc9c, _wy9c = 45.0, []
                _seal9c = float(_r9c.get("seal_yi") or 0)
                if _seal9c >= 5:
                    _sc9c += 25
                    _wy9c.append(f"封单{_seal9c:.1f}亿·极强")
                elif _seal9c >= 2:
                    _sc9c += 15
                    _wy9c.append(f"封单{_seal9c:.1f}亿·较强")
                elif _seal9c >= 1:
                    _sc9c += 8
                    _wy9c.append(f"封单{_seal9c:.1f}亿")
                _to9c = float(_r9c.get("turnover") or 0)
                if 3 <= _to9c <= 15:
                    _sc9c += 10
                    _wy9c.append(f"换手{_to9c:.0f}%适中")
                if str(_r9c.get("industry") or "") in _mains9c:
                    _sc9c += 15
                    _wy9c.append(f"主线「{_r9c.get('industry')}」共振")
                if _rr9c is not None and float(_rr9c) >= 60:
                    _sc9c += 10
                    _wy9c.append(f"近期接力命中{_rr9c}%")
                _tt9c.append((min(_sc9c, _cap9c), _r9c, "、".join(_wy9c) or "仅入榜无加分项"))
            for _sc9c, _r9c, _wy9c in sorted(_tt9c, key=lambda x: -x[0])[:3]:
                if _sc9c >= 90:
                    _t_cells9c.append(
                        f"<b style='color:#b45309'>【计划做T】{_r9c.get('name')}</b>"
                        f"<span style='color:#94a3b8;font-size:11px'>{_r9c.get('code')}</span>"
                        f"<b style='color:#b45309'>·🎯把握{int(round(_sc9c))}分</b>"
                        f"<span style='font-size:12px;color:#64748b'>·{_wy9c}·当日往返不留仓</span>")
        except Exception:
            pass
        st.markdown("**⭐ 关注中心 · ①今日及本周 ②下周 ③本月及下月**　"
                    "<span style='font-size:12px;color:#94a3b8'>三档双向(看涨/看跌+概率)·与桌面同口径</span>",
                    unsafe_allow_html=True)
        _tw_html9 = ["<div style='font-size:13.5px;line-height:1.8'>"]
        for _k9t, _tt9t, _abk9t in (("t1", "🟢 ① 今日关注 及 本周关注", None),
                                    ("t2", "🗓 ② 下周关注", "下周"),
                                    ("t3", "📆 ③ 本月关注 及 下月关注", "本月及下月")):
            _bl9t = _tw_tiers9[_k9t]["bull"][:5]
            _br9t = _tw_tiers9[_k9t]["bear"][:5]
            _tw_html9.append(f"<div style='margin:3px 0 1px'><b>{_tt9t}</b></div>")
            _bull_cells9t = (list(_t_cells9c) if _k9t == "t1" else []) \
                + [_tw_item9(_h, _p, _s, False) for _h, _p, _s in _bl9t]
            _tw_html9.append(
                "<div style='margin-left:8px'>🐉 <b style='color:#dc2626;font-size:12.5px'>看涨</b>："
                + ("　".join(_bull_cells9t) if _bull_cells9t
                   else "<span style='color:#94a3b8'>本档暂无达标（宁缺毋滥）</span>") + "</div>")
            _tw_html9.append(
                "<div style='margin-left:8px'>⚔️ <b style='color:#16a34a;font-size:12.5px'>看跌</b>："
                + ("　".join(_tw_item9(_h, _p, _s, True) for _h, _p, _s in _br9t) if _br9t
                   else "<span style='color:#94a3b8'>暂无预警（公开池无破位/转弱信号）</span>") + "</div>")
            if _abk9t and _tw_ab9.get(_abk9t):
                _tw_html9.append("<div style='margin-left:8px;font-size:12px;color:#64748b'>🏛 机构风向："
                                 + "｜".join(_tw_ab9[_abk9t][:3]) + "</div>")
        _tw_html9.append("</div>")
        st.markdown("".join(_tw_html9), unsafe_allow_html=True)
        st.caption("🐉看涨=龙虎门口径(上攻)｜⚔️看跌=鬼门关口径(先躲) · 概率=引擎对应周期方向分"
                   "(规则情景估计,非回测真实胜率)，🎯=概率≥65%高把握 · 事由=触发条件/周期备注(无据标纯技术) · "
                   "可买纪律:波段票上行空间≥10%才推"
                   + (f"(已剔除{_tw_skip9}只空间不足/不明的绿灯)" if _tw_skip9 else "")
                   + " · 做T仅收把握分≥90·最多3只·必标【计划做T】·当日往返不留仓"
                   + " · 云端候选=公开黑马池；持仓/自选实时档看桌面版，持仓破位警示见下方双门(私径)")
    except Exception:
        pass
    # 【V88·双门 2026-07-19 用户点单"云端也要有龙虎门/地狱门"】
    # 龙虎门=公开黑马绿灯(pub安全);地狱门含持仓名→走PRIVATE_TOKEN私径,无token如实提示。
    try:
        _dh_g9 = {}
        try:
            _dh_g9 = json.loads(pub_text("darkhorse.json", _PUB_VERSION) or "")
        except Exception:
            _dh_g9 = {}
        _gate_go9 = [h for h in (_dh_g9.get("horses") or [])
                     if ((h.get("trade_plan") or {}).get("short") or {}).get("mode")
                     in ("现价可进", "回踩到位", "突破确认")]
        # 【V88·统一裁决·云端 2026-07-25 用户定纲"逻辑和说明要统一"】与桌面同一把尺:
        # 弱市绿灯⏸️不执行/过热🔶限回踩/中性报领涨引擎——三市场政策一行看清
        try:
            _pol9c = []
            for _mkp9, _bp9 in (json.loads(pub_text("market_snapshot.json", _PUB_VERSION) or "{}")
                                .get("markets") or {}).items():
                _pp9 = dict((x[0], x[1]) for x in ((_bp9.get("l3") or {}).get("probs") or [])).get("2周")
                _tt9 = float((_bp9.get("temperature") or {}).get("temp") or 50)
                _vd9 = str((_bp9.get("temperature") or {}).get("verdict") or "")
                if (_pp9 is not None and int(_pp9) <= 45) or any(
                        k in _vd9 for k in ("转弱", "杀跌", "派发", "偏冷")):
                    _pol9c.append(f"{_mkp9}⏸️拐点/偏弱·买单暂停执行")
                elif _pp9 is not None and int(_pp9) >= 55 and _tt9 >= 75:
                    _pol9c.append(f"{_mkp9}🔶过热·只限回踩不追高")
                elif _pp9 is not None and int(_pp9) >= 55:
                    _pol9c.append(f"{_mkp9}✅良性·按纲领执行")
                else:
                    _pol9c.append(f"{_mkp9}⚖️中性·轻仓试")
            if _pol9c:
                st.caption("🧭 统一裁决(个股服从大盘态): " + "｜".join(_pol9c))
        except Exception:
            pass
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
            # 【V88·周期主判断 2026-07-19 用户定纲】主位=1-2周判断,当日动作降为小字(与桌面卡同口径)
            _pu9 = int(_d.get("p_up") or 0)
            _w2w9 = "偏涨" if _pu9 >= 58 else ("偏跌" if _pu9 <= 42 else "震荡")
            _w2c9 = "#dc2626" if _pu9 >= 58 else ("#16a34a" if _pu9 <= 42 else "#64748b")
            return (f"<div style='border:1px solid {_bc}44;border-left:4px solid {_bc};"
                    f"border-radius:8px;background:{_bg};padding:.45rem .6rem'>"
                    f"<div style='font-size:12px;font-weight:700;color:{_bc}'>{_head}</div>"
                    f"<div style='font-size:13px'><b>{_d.get('name')}</b> "
                    f"<span style='color:#94a3b8;font-size:11px'>{_d.get('code')}</span> "
                    f"<b style='color:{_w2c9}'>1-2周{_w2w9}{_pu9}%</b>"
                    f"<span style='font-size:11px;color:#64748b'>·今日:{_d.get('action') or ''}</span></div>"
                    f"<div style='font-size:12px;margin:2px 0'>{_gchain9(_d)}</div>"
                    + (f"<div style='font-size:11px;color:#64748b'>{_meta}</div>" if _meta else "")
                    + "</div>")

        _gate_note9 = ("<div style='font-size:11px;color:#94a3b8;margin:2px 0 6px'>"
                       "↑↓≈=较前一档，链首=今天锚点（阶段+5日动量，与桌面同口径）；"
                       "概率为规则情景估计</div>")

        # 【V88·双门同一模块 2026-07-19 用户定纲】左半=地狱门(先躲)、右半=龙虎门(上攻)，
        # 各按中美港三部分分列——与桌面「双门决断」模块同构。
        def _gate_mkey9c(_d9g):
            _m9g = str(_d9g.get("market") or "")
            if "美股" in _m9g:
                return "🇺🇸美股"
            if "港股" in _m9g:
                return "🇭🇰港股"
            if "A股" in _m9g:
                return "🇨🇳A股"
            _c9g = str(_d9g.get("code") or "").upper()
            if _c9g.endswith(".HK") or (_c9g.isdigit() and len(_c9g) in (4, 5)):
                return "🇭🇰港股"
            if _c9g.endswith((".SS", ".SZ", ".SH", ".BJ")) or (_c9g.isdigit() and len(_c9g) == 6):
                return "🇨🇳A股"
            return "🇺🇸美股"

        def _gate_grid9c(_pairs9):
            _out9 = []
            for _mk9g in ("🇺🇸美股", "🇭🇰港股", "🇨🇳A股"):
                _its9 = [h for k, h in _pairs9 if k == _mk9g]
                if _its9:
                    _out9.append(f"<div style='font-size:12px;font-weight:700;color:#64748b;"
                                 f"margin:4px 0 2px'>{_mk9g}（{len(_its9)}只）</div>"
                                 "<div style='display:grid;gap:6px'>" + "".join(_its9) + "</div>")
            return "".join(_out9)
        st.markdown("**🚪 双门决断 · ⚔️地狱门（先躲·左） ⟷ 🐉龙虎门（上攻·右）**　"
                    "<span style='font-size:12px;color:#94a3b8'>预测主力模块·中美港分列</span>",
                    unsafe_allow_html=True)
        _colGG9c, _colLH9c = st.columns(2)
        with _colLH9c:
            if _gate_go9:
                st.markdown(f"<div style='background:#fef2f2;border-left:4px solid #dc2626;border-radius:8px;"
                            f"padding:.35rem .5rem'><b style='color:#dc2626'>🐉 龙虎门 · 上攻关注</b>"
                            f"（黑马严门槛绿灯 {len(_gate_go9)} 只）</div>", unsafe_allow_html=True)
                _rl_lh9 = _rate_g9("entry_green", "入场绿灯")
                if _rl_lh9:
                    st.caption(_rl_lh9)
                _lh_pairs9 = [(_gate_mkey9c(h),
                               _gcard9(h, "#dc2626", "#fef2f2",
                                       ("🎯高把握·" if (int(h.get("p_up") or 0) >= 60
                                                     and float(h.get("rr") or 0) >= 1.5) else "")
                                       + "🐉 " + str(((h.get('trade_plan') or {}).get('short') or {})
                                                     .get('mode') or '绿灯')))
                              for h in sorted(_gate_go9,
                                              key=lambda h: -(int(h.get("p_up") or 0)
                                                              + (8 if float(h.get("rr") or 0) >= 1.5 else 0)))]
                st.markdown(_gate_grid9c(_lh_pairs9) + _gate_note9, unsafe_allow_html=True)
                st.caption("完整龙虎门(含自选/持仓实时绿灯)在桌面版；此处为公开黑马部分。")
            else:
                st.caption("🐉 龙虎门：今日无严门槛绿灯——空仓等待也是决策。")
        with _colGG9c:
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
                        st.markdown(f"<div style='background:#f0fdf4;border-left:4px solid #16a34a;"
                                    f"border-radius:8px;padding:.35rem .5rem'><b style='color:#16a34a'>"
                                    f"⚔️ 地狱门 · 拐点/破位先躲</b>（{len(_cut_g9)} 只·盘中落盘·🔒私径）</div>",
                                    unsafe_allow_html=True)
                        _rl_gg9 = _rate_g9("gate_guard", "地狱门警示")
                        st.caption(_rl_gg9 or "📊 地狱门警示实盘成功率：样本积累中（警示后≥3天下跌=躲对了，反向口径）")
                        _gg_pairs9 = [(_gate_mkey9c(r),
                                       _gcard9(r, "#16a34a", "#f0fdf4",
                                               ("🎯高把握·" if int(r.get("p_down") or 0) >= 60 else "")
                                               + "⚔️ " + str(r.get("reason") or "拐点/破位警示")[:14]))
                                      for r in sorted(_cut_g9, key=lambda r: -int(r.get("p_down") or 0))]
                        st.markdown(_gate_grid9c(_gg_pairs9) + _gate_note9, unsafe_allow_html=True)
                    else:
                        st.caption("⚔️ 地狱门：今日无警示名单。")
                except Exception:
                    st.caption("⚔️ 地狱门：私仓数据读取失败，稍后刷新。")
            else:
                st.caption("⚔️ 地狱门（含持仓，属私域）：需配 PRIVATE_TOKEN 才在云端显示——隐私铁律，桌面/飞书不受限。")
    except Exception:
        pass

    # 【V88·云端同步 2026-07-24 用户点单"云端功能不全"】🔮四档预判+⚠️🌱调整提醒+🧬自省
    # 与桌面完全同源(pub数据);明日=动量+温度规则估计;±1σ区间需日线序列,云端不重算,见桌面。
    try:
        _ma9y = {}
        try:
            _ma9y = (json.loads(pub_text("move_attribution.json", _PUB_VERSION) or "{}")
                     .get("reasons") or {})
        except Exception:
            _ma9y = {}

        def _pc9y(_p):
            return "#dc2626" if _p >= 55 else ("#16a34a" if _p <= 45 else "#64748b")
        _fc9y = []
        for _mk9y in ("美股", "A股", "港股"):
            _blk9y = ((_snap or {}).get("markets") or {}).get(_mk9y) or {}
            _l39y = dict((x[0], x[1]) for x in ((_blk9y.get("l3") or {}).get("probs") or []))
            if not _l39y:
                continue
            _t9y = float((_blk9y.get("temperature") or {}).get("temp") or 50)
            _cg9y = float(((_blk9y.get("indices") or [{}])[0] or {}).get("chg1d") or 0)
            _tm9y = int(round(max(25, min(75, 50 + 2.2 * _cg9y + 0.15 * (_t9y - 50)))))
            # 【V88·三市场综述同步 2026-07-25】云端四档行补今日涨跌+领涨领跌(与桌面同款信息密度)
            _cgc9y = "#dc2626" if _cg9y > 0.05 else ("#16a34a" if _cg9y < -0.05 else "#64748b")
            _secs9y = _blk9y.get("sectors") or []
            _lead9y = ""
            if _secs9y:
                _u2y = sorted(_secs9y, key=lambda s: -(s.get("chg1d") or 0))[:2]
                _d2y = sorted(_secs9y, key=lambda s: (s.get("chg1d") or 0))[:2]
                _dnw9y = "领跌" if (_d2y and (_d2y[0].get("chg1d") or 0) < 0) else "较弱"
                _upw9y = "领涨" if (_u2y and (_u2y[0].get("chg1d") or 0) > 0) else "较强"
                _lead9y = (f"<span style='font-size:11px;color:#475569'>　{_upw9y}:"
                           + "、".join(f"{s['name']}{(s.get('chg1d') or 0):+.1f}%" for s in _u2y)
                           + f"｜{_dnw9y}:" + "、".join(f"{s['name']}{(s.get('chg1d') or 0):+.1f}%" for s in _d2y)
                           + "</span>")
            _cells9y = [f"今日<b style='color:{_cgc9y}'>{_cg9y:+.2f}%</b>",
                        f"明日<b style='color:{_pc9y(_tm9y)}'>{_tm9y}%</b>"]
            for _lb9y, _hz9y in (("本周", "2周"), ("本月", "4周"), ("下月", "8周")):
                _p9y = _l39y.get(_hz9y)
                if _p9y is not None:
                    _cells9y.append(f"{_lb9y}<b style='color:{_pc9y(int(_p9y))}'>{int(_p9y)}%</b>")
            _r9y = _ma9y.get(_mk9y) or {}
            _s9y = _r9y.get("src") or {}
            _why9y = ""
            if _r9y.get("why"):
                _why9y = ("<br><span style='font-size:12px;color:#64748b'>└ ❓"
                          + ((f"<a href='{_s9y['u']}' target='_blank' "
                              f"style='color:inherit;text-decoration:underline'>{_r9y['why']}</a>"
                              f"（出处:{_s9y.get('s') or '新闻'}·点击看原文）") if _s9y.get("u")
                             else f"{_r9y['why']}（出处:AI异动归因）") + "</span>")
            _fc9y.append(f"<div style='font-size:12.5px;margin:1px 0'><b>{_mk9y}</b>："
                         + " ｜ ".join(_cells9y) + _lead9y + _why9y + "</div>")
        if _fc9y:
            st.markdown("**🔮 大盘四档预判**"
                        "<span style='font-size:12px;color:#94a3b8'>（明日=动量+温度规则估计·低置信；"
                        "本周/本月/下月=统一引擎2/4/8周分·与桌面同源；±1σ波动区间见桌面版）</span>"
                        + "".join(_fc9y), unsafe_allow_html=True)
    except Exception:
        pass
    try:
        _pt9y = json.loads(pub_text("phase_turn_full.json", _PUB_VERSION) or "{}")
        _st9y = _pt9y.get("stocks") or []
        # 兜底:全池文件未发布时用snapshot周期扫描(持仓自选池),如实标注不空白
        if not _st9y:
            _st9y = ((_snap or {}).get("cycle_scan") or {}).get("stocks") or []
            if _st9y:
                _pt9y = {"scanned": f"持仓自选池{len(_st9y)}",
                         "generated_at": ((_snap or {}).get("cycle_scan") or {}).get("analysis_time", "")}
        _tr9y = ((_snap or {}).get("rotation_forecast") or {}).get("trajectories") or {}

        def _mk9yz(code):
            _c = str(code or "").upper()
            if _c.endswith(".HK") or (_c.isdigit() and len(_c) in (4, 5)):
                return "港股"
            if _c.endswith((".SS", ".SZ", ".SH", ".BJ")) or (_c.isdigit() and len(_c) == 6):
                return "A股"
            return "美股"
        if _st9y:
            for _dir9y, _ttl9y, _kw9y, _col9y in (
                    ("down", "⚠️ 即将转弱 · 顶拐/退潮", "顶部转弱", "#b45309"),
                    ("up", "🌱 即将转强 · 低谷→启动", "底部转强", "#16a34a")):
                _rows9y = []
                for _m9y in ("美股", "A股", "港股"):
                    _ds9y = sorted([x for x in _st9y if x.get("direction") == _dir9y
                                    and _mk9yz(x.get("code")) == _m9y],
                                   key=lambda s: ({"高": 0, "中": 1, "低": 2}.get(str(s.get("confidence")), 3),
                                                  -float(s.get("strength") or 0)))
                    _cells9z = ("、".join(f"{s.get('name')}({s.get('confidence')}置信)" for s in _ds9y[:6])
                                + (f" <span style='color:#94a3b8'>等{len(_ds9y)}只</span>" if len(_ds9y) > 6 else "")
                                if _ds9y else "<span style='color:#94a3b8'>无触发（相位未到不硬报）</span>")
                    _sec9y = [f"{t.get('name')}({(t.get('turning') or {}).get('horizon')})"
                              for t in (_tr9y.get(_m9y) or [])
                              if _kw9y in str((t.get("turning") or {}).get("type"))]
                    _rows9y.append(f"<div style='font-size:12px'><b>{_m9y}</b> {_cells9z}"
                                   + (f"｜板块拐点:{'、'.join(_sec9y)}" if _sec9y else "") + "</div>")
                st.markdown(f"<b style='color:{_col9y};font-size:13px'>{_ttl9y}</b>"
                            f"<span style='font-size:11px;color:#94a3b8'>"
                            f"（全市场大池{_pt9y.get('scanned', '?')}只·{_pt9y.get('generated_at', '')}·与桌面同源）</span>"
                            + "".join(_rows9y), unsafe_allow_html=True)
    except Exception:
        pass
    try:
        _sr9y = json.loads(pub_text("self_review.json", _PUB_VERSION) or "{}")
        _pp9y = _sr9y.get("proposals") or []
        if _pp9y:
            st.caption("🧬 系统自省（战绩到期核算·提案待批,桌面批准后生效）：" + "；".join(
                f"{_p9y.get('signal')}命中{_p9y.get('rate')}%(n{_p9y.get('n')})→{str(_p9y.get('proposal'))[:32]}"
                for _p9y in _pp9y[:2]))
    except Exception:
        pass

    # 【V88·手机研究包·大盘版 2026-07-19 用户点单】Mac/Win关机也能拿系统数据——
    # 把大盘温度+三层链+轮动+龙虎门+实盘对账打包成可复制文本,手机贴给Claude综合研判。
    try:
        _mb_env9 = []
        for _mk9b in ("美股", "A股", "港股"):
            _blk9b = (((_snap or {}).get("markets") or {}).get(_mk9b) or {})
            _t9b = _blk9b.get("temperature") or {}
            _l39b = _blk9b.get("l3") or {}
            _ch9b = " ".join(f"{lab}{int(p)}" for lab, p in (_l39b.get("probs") or []))
            if _t9b or _ch9b:
                _mb_env9.append(f"{_mk9b}：温度{_t9b.get('temp', '?')}°"
                                f"（{str(_t9b.get('position', '')).split('（')[0]}）"
                                + (f"｜{_l39b.get('name')}周期链 {_ch9b}｜阶段{_l39b.get('stage')}"
                                   if _ch9b else ""))
        _mb_rot9 = []
        _traj9b = ((_snap or {}).get("rotation_forecast") or {}).get("trajectories") or {}
        for _mk9b, _tl9b in _traj9b.items():
            _top9b = sorted(_tl9b or [], key=lambda t: -((t.get("points") or {})
                                                         .get("2周") or {}).get("score", 0))
            if _top9b:
                _p29b = ((_top9b[0].get("points") or {}).get("2周") or {}).get("score")
                _mb_rot9.append(f"{_mk9b}最强板块:{_top9b[0].get('name')}(2周{_p29b})")
        # 双门try若早退,_gate_go9/_sr_g9可能未定义——globals兜底,别让研究包整体消失
        _mb_go9 = "、".join(f"{h.get('name')}({h.get('code')})"
                           for h in (globals().get("_gate_go9") or [])) or "今日无绿灯"
        _mb_sr9 = []
        for _k9b, _l9b in (("entry_green", "入场绿灯"), ("darkhorse", "黑马"),
                           ("gate_guard", "地狱门警示"), ("relay", "涨停接力")):
            _v9b = (globals().get("_sr_g9") or {}).get(_k9b) or {}
            if _v9b.get("rate") is not None:
                _mb_sr9.append(f"{_l9b}{_v9b['rate']}%(n{_v9b.get('n')})")
        _mb_txt9 = (
            f"【V88系统研究包·大盘】生成{_now_bjt():%Y-%m-%d %H:%M}北京\n"
            + ("■ 三大市场：\n" + "\n".join("  " + x for x in _mb_env9) + "\n" if _mb_env9 else "")
            + (f"■ 板块轮动：{'｜'.join(_mb_rot9)}\n" if _mb_rot9 else "")
            + f"■ 龙虎门（严门槛绿灯）：{_mb_go9}\n"
            + (f"■ 系统实盘对账（到期核算）：{'·'.join(_mb_sr9)}\n" if _mb_sr9 else "")
            + "■ 口径：温度=水位+情绪合成，越高越该轻仓；周期链数字=该档上涨概率（规则情景估计非胜率）\n"
            + "→ 请结合你能获取的最新宏观/政策/资金面信息，与以上V88数据交叉验证，回答：\n"
              "①明天/下周大盘方向判断与理由；②当前最值得关注的板块与个股逻辑；"
              "③仓位建议；④V88数据与你认知冲突的点。注意生成时间，行情有时效。")
        with st.expander("📱 手机研究包 · 大盘版（复制给任何Claude对话综合研判）", expanded=False):
            st.caption("用法：手机浏览器打开本页→复制下面整段→粘贴到手机Claude对话。"
                       "个股版在「🔍 个股搜索」页搜完自动生成。Mac/Win关机也能用。")
            st.code(_mb_txt9, language=None)
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
            # 【V88·涨跌归因链·云端简版 2026-07-25 用户定纲"不能光说技术面破坏"】
            # 板块联动+市场联动+事件带三层(pub数据);个股新闻层桌面/飞书完整版有。
            try:
                _chg9w = round(float(_df["Close"].iloc[-1] / _df["Close"].iloc[-2] - 1) * 100, 2)
                _dir9w = "跌" if _chg9w < 0 else "涨"
                _mkw9 = ("A股" if str(_tsym).upper().endswith((".SS", ".SZ", ".SH", ".BJ"))
                         else ("港股" if str(_tsym).upper().endswith(".HK") else "美股"))
                _snapw9 = json.loads(pub_text("market_snapshot.json", _PUB_VERSION) or "{}")
                _wl9 = []
                _mbw9 = ((_snapw9.get("markets") or {}).get(_mkw9) or {})
                _mcw9 = float(((_mbw9.get("indices") or [{}])[0] or {}).get("chg1d") or 0)
                if abs(_mcw9) >= 0.8 and ((_mcw9 < 0) == (_dir9w == "跌")):
                    _wl9.append(f"**市场联动**：{_mkw9}大盘当日{_mcw9:+.2f}%,个股难独善其身"
                                "(大盘归因见📰三市场综述why行)。")
                try:
                    for _e9w in (json.loads(pub_text("macro_events_pub.json", _PUB_VERSION) or "{}")
                                 .get("events") or []):
                        _evt9w = str(_e9w.get("event") or "")
                        if str(_tsym).upper().split(".")[0] in _evt9w:
                            _wl9.append(f"**事件带**：{_e9w['date'][5:]}自家财报——博弈期资金先撤是常见连锁。")
                            break
                        if "FOMC" in _evt9w and _mkw9 == "美股":
                            _wl9.append(f"**事件带**：{_e9w['date'][5:]}FOMC——决议前机构降杠杆,高估值先被卖。")
                            break
                except Exception:
                    pass
                if not _wl9:
                    _wl9.append("**归因**：大盘/事件层无显著联动——判定为个股/板块自身波动,"
                                "个股新闻层完整归因在桌面版深度分析。")
                st.markdown(f"##### 🔎 为什么{_dir9w}·归因链（当日{_chg9w:+.2f}%·云端简版）")
                st.markdown("<div style='font-size:13.5px;line-height:1.7'>" + "<br>".join(_wl9)
                            + "</div>", unsafe_allow_html=True)
            except Exception:
                pass
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

            # 【V88·手机研究包 2026-07-19 用户点单】Mac/Win关机也要能拿系统数据——
            # 云端实时引擎算完→打包成一段可复制文本,手机端粘贴给Claude,让它结合
            # V88确定性口径+它自己的最新信息做综合分析。零依赖桌面,纯云端。
            try:
                _mp_hz9 = (_cloud_decision.get("facts") or {}).get("horizons") or {}
                _mp_chain9 = " ".join(
                    f"{_k9m}{int(round(float((_mp_hz9.get(_k9m) or {}).get('rule_score'))))}"
                    for _k9m in ("2周", "4周", "8周", "16周", "32周")
                    if (_mp_hz9.get(_k9m) or {}).get("rule_score") is not None) or "走势链缺失"
                _mp_env9 = []
                try:
                    for _mk9m in ("美股", "A股", "港股"):
                        _t9m = (((_snap or {}).get("markets") or {}).get(_mk9m) or {}).get("temperature") or {}
                        if _t9m:
                            _mp_env9.append(f"{_mk9m}{_t9m.get('temp', '?')}°"
                                            f"({str(_t9m.get('position', '')).split('（')[0]})")
                except Exception:
                    pass
                _mp_sr9 = []
                try:
                    _sr9m = (json.loads(pub_text("success_rates.json", _PUB_VERSION) or "{}")
                             .get("types") or {})
                    for _k9m, _l9m in (("entry_green", "入场绿灯"), ("darkhorse", "黑马"),
                                       ("gate_guard", "地狱门警示")):
                        _v9m = _sr9m.get(_k9m) or {}
                        if _v9m.get("rate") is not None:
                            _mp_sr9.append(f"{_l9m}{_v9m['rate']}%(n{_v9m.get('n')})")
                except Exception:
                    pass
                _mp_txt9 = (
                    f"【V88系统研究包】{_tname}（{_tsym}）· 生成{_now_bjt():%Y-%m-%d %H:%M}北京\n"
                    f"■ 主判断（周期口径·24小时涨跌不改主判断）：1-2周上涨{_cloud_decision.get('p_up')}%"
                    f"/下行{_cloud_decision.get('p_down')}%｜中(4-8周)分{_cloud_decision.get('medium_score')}"
                    f"｜长(16-32周)分{_cloud_decision.get('long_score')}"
                    f"｜统一分{_cloud_decision.get('unified_score')}（短{_cloud_decision.get('short_score')}）\n"
                    f"■ 今日动作（纪律指令，次于主判断）：{_cloud_decision.get('action')}"
                    f"｜盈亏比{_cloud_decision.get('rr')}｜2周期望{_cloud_decision.get('expected_pct'):+.1f}%\n"
                    f"■ 入场/时机：{_cloud_decision.get('entry_note')}\n"
                    f"■ 周期链（各档上涨概率，规则情景估计非胜率）：{_mp_chain9}｜技术阶段：{f.get('conclusion')}\n"
                    f"■ 关键价位：现价{f.get('last')}｜阻力{_cloud_decision.get('resistance')}"
                    f"｜止损{_cloud_decision.get('stop')}\n"
                    + (f"■ 大盘环境：{'｜'.join(_mp_env9)}\n" if _mp_env9 else "")
                    + (f"■ 系统实盘对账（到期核算）：{'·'.join(_mp_sr9)}\n" if _mp_sr9 else "")
                    + "■ 口径：统一分=短20%+中25%+长20%+趋势15%+赔率20%；周期=2/4/8/16/32交易周翻倍律；"
                      "概率为确定性规则情景估计，非回测胜率\n"
                    + "→ 请结合你能获取的最新新闻/基本面/行业信息，与以上V88确定性引擎数据交叉验证，回答：\n"
                      "①同意/不同意系统动作，理由；②该股当前的催化与风险事由（要具体事件，不要泛泛）；"
                      "③给出何时买/何时卖的具体条件（价位或信号）；④指出系统数据与你认知冲突的点。"
                      "注意上面的生成时间，行情有时效。")
                with st.expander("📱 手机研究包 · 复制给任何Claude对话综合分析", expanded=False):
                    st.caption("用法：手机浏览器打开本云端页→搜这只股→复制下面整段→粘贴到手机Claude对话。"
                               "Mac/Win关机也能用（本页数据=云端实时计算+pub快照）。")
                    st.code(_mp_txt9, language=None)
            except Exception:
                pass

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
                exp_md("📖 量价判读（事实+要点·佐证上方结论）",
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
        # 【V88·中签率公示 2026-07-24 用户点单】与桌面同口径:A股ballot/港股luckyRatio/美股如实配售制
        def _lot9c(_r):
            if _r.get("market") == "A股":
                return (f"{float(_r['ballot_pct']):g}%" if _r.get("ballot_pct") else "待披露")
            if _r.get("market") == "港股":
                _lr = _r.get("lucky_ratio")
                return (str(_lr) + ("" if "%" in str(_lr) else "%")) if _lr else "待披露"
            return "配售制·无中签率"
        st.dataframe([{"市场": _r.get("market"), "新股": f"{_r.get('name')}（{_r.get('code')}）",
                       "申购/定价日": _r.get("apply_date"), "中签率": _lot9c(_r),
                       "评级": _r.get("grade", ""),
                       "点评": str(_r.get("ai") or _r.get("why") or "")[:36]}
                      for _r in _ipo_rows9c[:10]],
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
            # 【V88·分市场 2026-07-19】三市场各一栏;兼容旧扁平结构
            _mkts9c = [(_m, _fl) for _m, _fl in (("A股", "🇨🇳"), ("港股", "🇭🇰"), ("美股", "🇺🇸"))
                       if isinstance(_aib9c.get(_m), dict)]
            if _mkts9c:
                for _m, _fl in _mkts9c:
                    _sub = _aib9c[_m]
                    _cells = "｜".join(f"{k}:{_sub[k]}" for k in ("明天", "本周", "下周", "本月及下月")
                                       if _sub.get(k))
                    if _cells:
                        st.markdown(f"- {_fl} **{_m}**：{_cells}")
            else:
                for _k9c in ("明天", "本周", "下周", "本月及下月"):
                    if _aib9c.get(_k9c):
                        st.markdown(f"- **{_k9c}**：{_aib9c[_k9c]}")
            # 【V88·外资投行周月提示 2026-07-24】新闻公开转述·AI综合非原话
            _fb9c = _aib9c.get("外资投行") or {}
            if isinstance(_fb9c, dict) and (_fb9c.get("本周") or _fb9c.get("本月")):
                st.markdown("- 🏦 **外资投行周月提示**："
                            + (f"本周:{_fb9c.get('本周')} " if _fb9c.get("本周") else "")
                            + (f"｜本月:{_fb9c.get('本月')}" if _fb9c.get("本月") else ""))
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
                        _wm = "HK" if _wc.endswith(".HK") else ("CN" if _wc.endswith((".SS", ".SZ", ".SH", ".BJ")) else "US")
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
