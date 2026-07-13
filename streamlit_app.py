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
    A = '<a href="?q={c}" target="_self" style="color:inherit;text-decoration:underline dotted 1px;">{t}</a>'

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
st.caption("24小时在线 · 日报每交易日07:00/14:00/21:00更新 · 三市行情盘中每30分钟刷新 · 访问权限由Streamlit部署设置控制")

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
    if _qp and not st.session_state.get("_qp_done"):
        st.session_state["_qp_done"] = True
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
    _nav = st.radio("导航", ["🧭 导航", "🔥 热点新闻", "🏆 全选榜单", "🔍 个股搜索", "📊 日报", "📅 周报", "📈 大盘板块", "🔁 复盘", "💼 持仓终端"],
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
def _linkify_cloud(md: str) -> str:
    """【V88·云端个股可点】把 [US:CODE] token 与 **名称**（CODE） 转成 ?q= 深链（蓝色可点）。"""
    import re as _rc
    _A = '<a href="?q={c}" target="_self" style="color:#1e3a5f;text-decoration:underline;cursor:pointer;font-weight:600">{t}</a>'
    md = _rc.sub(r"`?\[(US|SH|SZ|HK):([A-Za-z0-9\.\-]+)\]`?",
                 lambda m: _A.format(c=m.group(2), t=f"[{m.group(1)}:{m.group(2)}]"), md)
    md = _rc.sub(r"\*\*([一-鿿A-Za-z0-9\-·]{2,14})\*\*[（(]([A-Z0-9]{1,8}(?:\.[A-Z]{2})?)[）)]",
                 lambda m: _A.format(c=m.group(2), t=m.group(1)) + f"（{m.group(2)}）", md)
    return md


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
            return f'<a href="?q={cd}" target="_self" style="color:#1e3a5f;text-decoration:underline;cursor:pointer;font-weight:600">{nm}</a>'
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
    st.caption(_fresh_caption((_snap or {}).get("generated_at"), "行情快照") + " · 交易日盘中每30分钟刷新")
    _meta0 = pub_meta()
    if _meta0.get("daily_report_ts"):
        st.caption(_fresh_caption(_meta0["daily_report_ts"], "日报/操作榜") + " · 每时段更新（07/14/21点）")
    if _snap and _snap.get("markets"):
        for _mkt in ("美股", "A股", "港股"):
            _t = (_snap["markets"].get(_mkt) or {}).get("temperature")
            if _t:
                st.markdown(f"🌡 **{_mkt} {_t['temp']}/100** {_t['label']} → 仓位 **{_t['position']}**")
                if _t.get("verdict"):
                    st.caption(f"🧭 研判：{_t['verdict']}")
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
        if _rot_cloud:
            st.markdown("**🧭 下一轮板块轮转预警（明日 · 下周 · 半个月）**")
            from rotation_ui import rotation_map_html as _rotation_map_html_cloud
            st.markdown(_rotation_map_html_cloud(_rot_cloud, "v88-cloud-nav-rotation"), unsafe_allow_html=True)
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
        st.caption("💡 得分=五维综合评分 ｜ MACD/量价：明显放量≥+20%·温和放量+8%~20%·持平±8%·明显缩量≤-20% ｜ 操作指引与止损/目标由引擎按实时价确定，与桌面 V88 同源")
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
            _fu = cloud_engine.fundamentals(_tsym)
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
                       + (" · 每时段更新（北京时间07/14/21点）" if _nav == "📊 日报" else " · 每周日更新"))
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
        st.markdown(_report_body)
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
        st.markdown("##### 🧭 下一轮板块轮转预警")
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
            # ── 结构化录单：简称自动识别全称，卖出价留空=买入，每笔带成交日期 ──
            _ct_accounts = _pm.account_names()
            _f1, _f2, _f3, _f4, _f5, _f6, _f7 = st.columns([2.0, 1.05, 1.15, .9, 1.25, 1.2, .65])
            _ct_name = _f1.text_input("名称/简称/代码", placeholder="腾讯 / 海油 / NVDA", key="_ctf_name")
            _ct_buy = _f2.text_input("买入价", placeholder="469", key="_ctf_buy")
            _ct_sell = _f3.text_input("卖出价(空=买入)", placeholder="", key="_ctf_sell")
            _ct_qty = _f4.text_input("股数", placeholder="100", key="_ctf_qty")
            from datetime import date as _date9
            _ct_date = _f5.date_input("成交日期", value=_date9.today(), key="_ctf_date")
            _ct_account = _f6.selectbox("账户", _ct_accounts, key="_ctf_account")
            _ct_level = _f7.selectbox("级别", ["A", "B", "C"], index=1, key="_ctf_level",
                                      help="人工基础级别；持仓风险仍会自动升为A级")
            _ct_rsn = st.text_input("原因(选填,随日志留档)", placeholder="如：回踩买点 / 止盈一半", key="_pt_rsn")

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

            if st.button("▶ 记一笔", type="primary") and _ct_name.strip():
                try:
                    _pt_exec({"token": _ct_name.strip(), "shares": _ct_qty or 0,
                              "buy_px": float(_ct_buy) if _ct_buy.strip() else None,
                              "sell_px": float(_ct_sell) if _ct_sell.strip() else None,
                              "date": str(_ct_date), "reason": _ct_rsn.strip(),
                              "account": _ct_account, "level": _ct_level})
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
                _wl_raw, _ = _priv_get("watchlist_v88.json")
                _lvl_raw, _lvl_sha = _priv_get("watch_levels.json")
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
