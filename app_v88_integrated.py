"""
AI 皇冠双核 V88 - 集成版（模块化架构 + 完整功能）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
版本说明：
  - 基于V87.17的完整功能
  - 集成V88的模块化架构
  - 使用LRU缓存系统
  - Type Hints和统一错误处理
  
核心改进：
  ✅ 模块化架构（8个核心模块）
  ✅ LRU缓存系统（比满则全清更智能）
  ✅ 完整功能100%保留
  ✅ 点击表格行即触发分析
  ✅ 网络重试机制（指数退避）
  ✅ 交易日15分钟/非交易日24小时缓存
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
from v88_paths import core_root

import streamlit as st
# Explicit stock deep links must dispatch before homepage imports, pools and panels.
# Ordinary navigation retains the complete original overview and research tools.
if st.query_params.get("focus") == "deep":
    from focused_deep_view import render as _render_focused_deep
    _render_focused_deep(st, st.query_params.get("q", ""))
    st.stop()

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import requests
import os
import time
import json
import urllib3
from datetime import datetime
from modules.utils import to_yf_cn_code, parse_market_from_code
from runtime_guard import ensure_file_capacity
_V88_FILE_CAPACITY = ensure_file_capacity()

# ══ 前置定义区(2026-07-31 用户抓'_dt_global未定义'·pyflakes全扫修'使用先于定义'家族) ══
# 根因:分页切段搬代码后,多个名字的定义落到了调用之后(NameError被except吞=功能静默死)。
# 修法:把跨段共用的定义统一前移到这里;后文原位置留注释,不留重复定义。
_dt_global = datetime   # 全球概览区时间戳用
_V88_WATCHLIST_UI = False   # 2026-07-31 用户令:自选版面全撤,只留持仓+🏆3A榜(池照扫,3A从池里出)
_V88_GATES_UI = False   # 2026-07-31 用户裁定:双门决断版面撤;地狱门卖警=3A一票压级证据,龙虎门绿灯已并行动中心


def _v88_sentinel9(_repo, module, exc=None):
    """【V88·异常哨兵 2026-07-24 用户批准三层自愈A层】被吞异常落盘显影——
    三档段曾静默崩半天没人知道(病根:except pass)。健康条亮当日计数,
    会话开工必读必修(铁律,已获批:bug发现即修+事后汇报)。保留最近100条。"""
    try:
        import traceback as _tb9
        _fp = _repo / "data" / "render_errors.json"
        try:
            _rows = json.loads(_fp.read_text(encoding="utf-8"))
        except Exception:
            _rows = []
        _rows.append({"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                      "module": str(module)[:40],
                      "err": (str(exc)[:500] if exc is not None else _tb9.format_exc()[-500:])})
        _fp.write_text(json.dumps(_rows[-100:], ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        pass



def call_model_api(prompt, model_name=None, *, priority=False, scope="web-general"):
    """
    V88统一AI调用入口（历史函数名保留；实际使用GPT-6 Codex订阅GPT-6 Astra）
    
    参数:
        prompt: 提示词
        model_name: 模型名称（可选，默认使用GEMINI_MODEL_NAME）
    
    返回:
        AI生成的文本响应
    """
    ticket = None
    try:
        from desktop_gpt_subscription import complete
        from v88_ai_budget import reserve, settle
        ticket = reserve(prompt, output_tokens=8192, priority=priority, scope=scope)
        if not ticket:
            return "❌ GPT-6 Codex订阅调用正在冷却，请稍后重试"
        text, body = complete(prompt, model=model_name or "gpt-6-astra", temperature=0.3,
                              reasoning_effort="high", max_tokens=8192, timeout=150)
        settle(ticket, body.get("usage"), ok=True)
        return text
    except Exception as e:
        if ticket:
            try:
                settle(ticket, ok=False)
            except Exception:
                pass
        logging.error(f"❌ GPT-6 Codex订阅调用异常: {str(e)}")
        return f"❌ GPT-6 Codex订阅调用失败: {str(e)}"


def call_model_api_stream(prompt, model_name=None, max_output_tokens=8192):
    """历史流式入口兼容层；实际固定调用订阅GPT-6 Astra。"""
    yield call_model_api(prompt, model_name=model_name)

# 历史入口兼容一轮；全部仍调用同一GPT-6订阅实现。
call_gemini_api = call_model_api
call_gemini_api_stream = call_model_api_stream



# ═══════════════════════════════════════════════════════════════
# 7. CANSLIM + 专业投机原理（完整双核评级）
# ═══════════════════════════════════════════════════════════════

def _v88_usage9(_repo, event):
    """【V88·点击热力 2026-07-24 用户批准三层自愈C层】本地记录使用事件——
    「🧬系统自省」展示7日热力+主动提减法。只存本机data/(gitignore),不上传。保留30天。"""
    try:
        _fp = _repo / "data" / "usage_log.json"
        try:
            _d = json.loads(_fp.read_text(encoding="utf-8"))
        except Exception:
            _d = {}
        _day = datetime.now().strftime("%Y-%m-%d")
        _d.setdefault(_day, {})
        _d[_day][str(event)] = int(_d[_day].get(str(event), 0)) + 1
        for _k in sorted(_d)[:-30]:
            _d.pop(_k, None)
        _fp.write_text(json.dumps(_d, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        pass



# ══ 前置定义区结束 ══
from pathlib import Path
import pickle
import hashlib
import shutil
import logging
import re

# ── 从 .env 加载密钥（本地开发用；不覆盖已有环境变量）─────────────────────────
def _load_env_file():
    env_path = Path(__file__).parent / '.env'
    if not env_path.exists():
        return
    try:
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, _, v = line.partition('=')
                k = k.strip(); v = v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except Exception:
        pass
_load_env_file()

# ── 强制设置代理环境变量（Clash 127.0.0.1:7897）────────────────────────────
# 使用直接赋值而非 setdefault，防止被 IDE/外部 shell 的残留代理覆盖
_PROXY_ADDR = "127.0.0.1:7897"
for _pk, _pv in [
    ('http_proxy', f'http://{_PROXY_ADDR}'), ('https_proxy', f'http://{_PROXY_ADDR}'),
    ('HTTP_PROXY', f'http://{_PROXY_ADDR}'), ('HTTPS_PROXY', f'http://{_PROXY_ADDR}'),
    # ALL_PROXY 必须走 http：curl_cffi(yfinance) 对 socks5 会报 TLS invalid library
    ('ALL_PROXY', f'http://{_PROXY_ADDR}'),
]:
    os.environ[_pk] = _pv

# ── 启动自检：自动检测关键依赖和数据源 ─────────────────────────────────────────
def _startup_health_check() -> dict:
    """检测关键模块和数据源，返回状态字典"""
    results = {}

    # 1. 关键标准库
    _required_modules = ['re', 'json', 'os', 'time', 'hashlib', 'logging', 'pickle']
    missing = []
    for mod in _required_modules:
        try:
            __import__(mod)
        except ImportError:
            missing.append(mod)
    results['missing_imports'] = missing

    # 2. yfinance 可用性（仅检测模块可导入，不发网络请求——避免启动时触发 rate limit）
    try:
        import yfinance as _yf
        results['yfinance'] = 'ok'
    except ImportError as e:
        results['yfinance'] = f'error:{e}'

    # 3. HK 代码格式自检（核心逻辑验证）
    try:
        from modules.utils import to_yf_cn_code, get_hk_code_variants
        assert to_yf_cn_code("00700") == "0700.HK", "00700 format error"
        assert to_yf_cn_code("00836.HK") == "0836.HK", "00836.HK format error"
        assert to_yf_cn_code("09992.HK") == "9992.HK", "09992.HK format error"
        results['hk_code_fmt'] = 'ok'
    except Exception as e:
        results['hk_code_fmt'] = f'error:{e}'

    # 4. 东方财富搜索 API 连通性（延迟到首次使用时检测，不在启动时阻塞）
    results['eastmoney_api'] = 'deferred'

    return results

if 'startup_health' not in st.session_state:
    st.session_state.startup_health = _startup_health_check()
    _h = st.session_state.startup_health
    _issues = []
    if _h.get('missing_imports'):
        _issues.append(f"缺少模块: {', '.join(_h['missing_imports'])}")
    if _h.get('hk_code_fmt', 'ok') != 'ok':
        _issues.append(f"港股代码格式异常: {_h['hk_code_fmt']}")
    if _h.get('yfinance', 'ok') != 'ok':
        _issues.append(f"yfinance 异常: {_h['yfinance']}")
    if _issues:
        logging.warning(f"[启动自检] 发现问题: {'; '.join(_issues)}")
    else:
        logging.info("[启动自检] ✅ 所有检查通过")

# ── AI市场简报 1小时文件缓存（权威日报迁移前的兼容层）──────────────────────
_BRIEF_CACHE_DIR = Path(__file__).parent / ".cache_brief"
_BRIEF_CACHE_FILE = _BRIEF_CACHE_DIR / "daily_brief.json"
_BRIEF_CACHE_TTL = 3600  # 1小时（由 config.toml [cache].brief_ttl 覆盖；2026-07-09 全模块统一1小时）
_AUTHORITATIVE_REPORT = core_root() / "data" / "daily_report.md"
_AUTHORITATIVE_MANIFEST = _AUTHORITATIVE_REPORT.parent / "report_manifest.json"
_AUTHORITATIVE_SNAPSHOT = _AUTHORITATIVE_REPORT.parent / "market_snapshot.json"
_AUTHORITATIVE_PLAN_B_REPORT = _AUTHORITATIVE_REPORT.parent / "daily_report.plan_b.md"
_AUTHORITATIVE_PLAN_B_MANIFEST = _AUTHORITATIVE_REPORT.parent / "report_manifest.plan_b.json"
_AUTHORITATIVE_BRIEF_META = {}


def _validate_plan_a(report_path, manifest_path, snapshot_path, check_snapshot=True):
    """判定某份报告是否满足硬质检。返回 (content, ts, status_dict)。
    ★关键：质检失败时也把正文交出来（content 非空），由调用方决定是否作为 Plan B 用——
    因为质检失败往往只是"权威新闻来源不足3条A/B级"，而报告里的操作榜/评分/温度是确定性引擎
    今日实算的真实数据，完全可用，不该跟着新闻叙事一起被丢掉。content 仅在文件缺失/读不出时才为 None。
    check_snapshot=False 仅保留为旧协议兼容；生产读取已统一交给report_contract。"""
    if not report_path.exists():
        return None, None, {"status": "missing", "issues": ["报告文件不存在"]}
    ts = report_path.stat().st_mtime
    try:
        raw_content = report_path.read_text(encoding="utf-8-sig")
        content = raw_content.strip()
    except Exception as exc:
        return None, ts, {"status": "failed", "issues": [f"日报读取失败: {exc}"]}
    if not manifest_path.exists():
        return content, ts, {"status": "legacy", "snapshot_id": "", "issues": ["等待下一轮任务生成新版质检清单"]}
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8")) if (check_snapshot and snapshot_path.exists()) else {}
        # Plan B 的 issues 是“Plan A为何降级”的说明，不是 Plan B 自身无效。
        advisory_issues = list((manifest.get("quality") or {}).get("issues") or [])
        validation_issues = []
        _quality_status = (manifest.get("quality") or {}).get("status")
        if _quality_status not in ("passed", "plan_b"):
            return content, ts, {**manifest, "status": "failed", "issues": advisory_issues}
        if snapshot and manifest.get("snapshot_id") != snapshot.get("snapshot_id"):
            validation_issues.append("日报与行情快照版本不一致")
        if hashlib.sha256(raw_content.encode("utf-8")).hexdigest() != manifest.get("report_sha256"):
            validation_issues.append("日报正文校验和不一致")
        if validation_issues:
            return content, ts, {**manifest, "status": "failed", "issues": validation_issues}
        return content, ts, {**manifest, "status": _quality_status, "issues": advisory_issues}
    except Exception as exc:
        return content, ts, {"status": "failed", "issues": [f"质检清单解析失败: {exc}"]}


def _load_report_planab():
    """网页与后台共用原件、中央权限和完整交易日校验，不独立按午夜截断。"""
    global _AUTHORITATIVE_BRIEF_META
    from report_view import load_report
    content, meta = load_report()
    _AUTHORITATIVE_BRIEF_META = meta
    return content, meta


def _load_authoritative_brief():
    """兼容旧调用名：Plan A为硬质检版，Plan B为最近完整交易日观察稿。"""
    content, meta = _load_report_planab()
    return content, meta.get("ts")


_AI_REPORT_CACHE_DIR = _BRIEF_CACHE_DIR  # 复用同一缓存目录
_AI_REPORT_TTL = 3600  # 1小时（全模块统一）
_MARKET_AI_CACHE_TTL = 3 * 3600  # 三市场增强研判：盘中每3小时
_MARKET_AI_MAX_DAILY_RUNS = 3
_MARKET_AI_SCHEDULE_FILE = _AI_REPORT_CACHE_DIR / "market_ai_schedule.json"

def _load_ai_report_cache(report_key: str):
    """加载 AI 报告文件缓存，命中(<1h)返回 (data_dict, ts)，否则 (None, None)"""
    try:
        _f = _AI_REPORT_CACHE_DIR / f"ai_report_{report_key}.json"
        if _f.exists():
            data = json.loads(_f.read_text(encoding="utf-8"))
            age = time.time() - data.get("timestamp", 0)
            # 市场研判在休市期间继续展示最近结果；是否盘中重算由调度器单独判断。
            _ttl = 72 * 3600 if str(report_key).startswith("market_") else _AI_REPORT_TTL
            if age < _ttl:
                return data.get("payload"), data.get("timestamp")
    except Exception:
        pass
    return None, None


def _save_ai_report_cache(report_key: str, payload):
    """保存 AI 报告到文件缓存"""
    try:
        _AI_REPORT_CACHE_DIR.mkdir(exist_ok=True)
        (_AI_REPORT_CACHE_DIR / f"ai_report_{report_key}.json").write_text(
            json.dumps({"payload": payload, "timestamp": time.time()}, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as _e:
        logging.warning(f"AI报告缓存写入失败({report_key}): {_e}")


def _market_ai_schedule_state():
    """返回北京时间当日自动运行次数；日期变化时自动归零。"""
    _today = pd.Timestamp.now(tz="Asia/Shanghai").strftime("%Y-%m-%d")
    try:
        _state = json.loads(_MARKET_AI_SCHEDULE_FILE.read_text(encoding="utf-8"))
    except Exception:
        _state = {}
    if _state.get("date") != _today:
        _state = {"date": _today, "runs": 0, "last_success": 0}
    return _state


def _market_ai_any_market_open():
    """使用交易所官方日历判断美股、港股、A股是否正处于交易时段。"""
    try:
        import exchange_calendars as _xcals
        _now = pd.Timestamp.now(tz="UTC").floor("min")
        return any(_xcals.get_calendar(_code).is_open_on_minute(_now, ignore_breaks=False)
                   for _code in ("XNYS", "XHKG", "XSHG"))
    except Exception as _e:
        logging.warning(f"市场AI交易时段判断失败，停止自动刷新: {_e}")
        return False


def _market_ai_auto_due():
    """仅任一市场盘中、距上次成功≥3小时且北京时间当日少于3次时到期。"""
    _state = _market_ai_schedule_state()
    if int(_state.get("runs", 0)) >= _MARKET_AI_MAX_DAILY_RUNS or not _market_ai_any_market_open():
        return False
    return time.time() - float(_state.get("last_success", 0) or 0) >= _MARKET_AI_CACHE_TTL


def _record_market_ai_auto_success():
    try:
        _state = _market_ai_schedule_state()
        _state["runs"] = min(_MARKET_AI_MAX_DAILY_RUNS, int(_state.get("runs", 0)) + 1)
        _state["last_success"] = time.time()
        _AI_REPORT_CACHE_DIR.mkdir(exist_ok=True)
        _MARKET_AI_SCHEDULE_FILE.write_text(json.dumps(_state, ensure_ascii=False), encoding="utf-8")
    except Exception as _e:
        logging.warning(f"市场AI调度记录失败: {_e}")


# ── 真实新闻报告（ai-daily-report-v2 日报，约束日报触发事件，禁止编造）────────────────
_AI_DAILY_REPORT_PATHS = [
    core_root() / "data" / "daily_report.md",  # Mac 本地优先
    Path("/root/ai-daily-report-v2/data/daily_report.md"),  # VPS 备选
]
if os.environ.get("AI_DAILY_REPORT_PATH"):
    _AI_DAILY_REPORT_PATHS.insert(0, Path(os.environ["AI_DAILY_REPORT_PATH"]))


# 日报最大可用天数：超过该天数视为过期。
# 与 Action Gate「触发时效 ≤ 72h」对齐，过期新闻无法支撑任何「触发」字段，
# 注入只会与【校验时间=今日】产生硬矛盾，导致模型拒绝出报。
_AI_DAILY_REPORT_MAX_AGE_DAYS = int(os.environ.get("AI_DAILY_REPORT_MAX_AGE_DAYS", "3"))


def _parse_daily_report_date(content: str):
    """从日报标题（形如 '# 📊 AI投资日报 — 2026年03月11日'）解析日期，失败返回 None。"""
    import re as _re
    from datetime import date as _date
    m = _re.search(r'(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日', content[:300])
    if not m:
        return None
    try:
        return _date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _load_real_news_report() -> str:
    """
    读取 ai-daily-report-v2 生成的日报，作为可执行推荐「触发」字段的唯一真实新闻来源。
    仅在日报「足够新鲜」（距今 ≤ _AI_DAILY_REPORT_MAX_AGE_DAYS 天）时注入；
    过期日报会被跳过，避免把陈旧新闻当「今日真实新闻」喂给模型而触发硬拒绝。
    若文件不存在、读取失败或全部过期，返回空字符串。
    """
    from datetime import date as _date
    for p in _AI_DAILY_REPORT_PATHS:
        try:
            if p.exists():
                content = p.read_text(encoding="utf-8-sig").strip()
                if content and len(content) > 100:
                    _rep_date = _parse_daily_report_date(content)
                    if _rep_date is not None:
                        _age = (_date.today() - _rep_date).days
                        if _age > _AI_DAILY_REPORT_MAX_AGE_DAYS:
                            _safe_print(
                                f"  ⚠️ 日报已过期（{_rep_date}，距今 {_age} 天 > "
                                f"{_AI_DAILY_REPORT_MAX_AGE_DAYS} 天），跳过注入以免与今日校验时间矛盾: {p}"
                            )
                            continue
                    _safe_print(f"  ✅ 已注入真实新闻报告（{len(content)} 字）: {p}")
                    return content
        except Exception as e:
            logging.debug("读取日报失败 %s: %s", p, e)
    _safe_print("  ⚠️ 未找到新鲜的 ai-daily-report-v2 日报，触发字段将受下方规则约束（基于基本面判断）")
    return ""


_BRIEF_HISTORY_FILE = _BRIEF_CACHE_DIR / "brief_history.json"

def _append_brief_history(content: str):
    """从简报内容中提取推荐代码，保存到历史文件（保留最近7天）"""
    import re as _re
    # 匹配 **名称(代码)** 格式，提取括号内的代码
    codes = _re.findall(r'\*\*[^(（]+[（(]([A-Za-z0-9.]+)[)）]\*\*', content)
    if not codes:
        return
    try:
        _BRIEF_CACHE_DIR.mkdir(exist_ok=True)
        history = []
        if _BRIEF_HISTORY_FILE.exists():
            history = json.loads(_BRIEF_HISTORY_FILE.read_text(encoding="utf-8"))
        today_str = __import__("datetime").date.today().isoformat()
        # 去掉7天前的记录
        cutoff = time.time() - 7 * 86400
        history = [r for r in history if r.get("ts", 0) > cutoff]
        history.append({"date": today_str, "ts": time.time(), "codes": list(set(codes))})
        _BRIEF_HISTORY_FILE.write_text(json.dumps(history, ensure_ascii=False), encoding="utf-8")
    except Exception as _e:
        logging.warning(f"历史推荐记录写入失败: {_e}")


def _get_recent_recommended_codes(days: int = 3) -> list:
    """读取最近N天已推荐的股票代码列表（去重）"""
    try:
        if not _BRIEF_HISTORY_FILE.exists():
            return []
        history = json.loads(_BRIEF_HISTORY_FILE.read_text(encoding="utf-8"))
        cutoff = time.time() - days * 86400
        codes = []
        for r in history:
            if r.get("ts", 0) > cutoff:
                codes.extend(r.get("codes", []))
        return list(set(codes))
    except Exception:
        return []
# ─────────────────────────────────────────────────────────────────────────────


def _safe_print(*args, **kwargs):
    """避免 Streamlit 重载时 stdout 关闭导致的 ValueError"""
    try:
        import builtins
        builtins.print(*args, **kwargs)
    except (ValueError, OSError):
        logging.debug(f"_safe_print: {args} {kwargs}")


# ── 每日凌晨零点自动清零缓存 ────────────────────────────────────
# 用文件记录"今天是否已清过"，网页版和手机端使用同一逻辑。
# 每次页面渲染时检查日期，零点后首次访问触发清零。
from datetime import date as _date_cls
from pathlib import Path as _Path_cls

_DAILY_CLEAR_FLAG: _Path_cls = _Path_cls(".cache_brief/_daily_clear_date.txt")


def _check_daily_cache_clear() -> None:
    """
    每天凌晨零点后首次页面渲染时，自动清零全部缓存并触发重新扫描。

    清零范围（全覆盖）：
      ① Streamlit 内存缓存：st.cache_resource / st.cache_data
      ② st.session_state 数据缓存（宏观风险、简报等会话级缓存）
      ③ .cache_brief/  内所有 JSON/log 文件（简报、宏观、扫描结果、股池等）
      ④ .cache_stock_data/  股票行情 pickle 文件
      ⑤ 触发后台重扫（写入 rescan 标记，下次 fragment 渲染时拾起）
    """
    today_str = str(_date_cls.today())

    # 读取上次清零日期
    try:
        last_clear = _DAILY_CLEAR_FLAG.read_text(encoding="utf-8").strip()
    except Exception:
        last_clear = ""

    if last_clear == today_str:
        return   # 今天已清过，跳过

    _safe_print(f"🌙 [{today_str}] 每日零点全量缓存清零开始...")

    # ① Streamlit 内置内存缓存
    try:
        st.cache_resource.clear()
        st.cache_data.clear()
    except Exception:
        pass

    # ② st.session_state 中的数据缓存键
    _ss_keys_to_clear = [
        "_macro_risk_result",       # 宏观风险评估
        "_brief_content",           # AI 简报内容（旧 key）
        "_brief_timestamp",         # 简报时间戳（旧 key）
        "market_brief_latest",      # AI 简报内容（当前 key）
        "_brief_auto_gen_done",     # 简报自动生成标志
        "_scan_results_cache",      # 扫描结果内存缓存
        "_gist_local_cache",        # Gist 本地缓存
        "_heat_cache",              # 行业热力缓存
        "market_ai_us",             # AI市场分析-美股
        "market_ai_hk",             # AI市场分析-港股
        "market_ai_cn",             # AI市场分析-A股
        "_us_tech_data",            # 美股技术数据
        "_hk_tech_data",            # 港股技术数据
        "_cn_tech_data",            # A股技术数据
        "market_sentiment_us",      # 美股舆情
        "market_sentiment_hk",      # 港股舆情
        "market_sentiment_cn",      # A股舆情
        "_market_ai_auto_done",     # 市场AI自动生成标志
    ]
    for _k in _ss_keys_to_clear:
        try:
            st.session_state.pop(_k, None)
        except Exception:
            pass

    # ③ .cache_brief/ 内所有文件（保留日期标记文件）
    try:
        import shutil as _shutil
        _brief_dir = _Path_cls(".cache_brief")
        _KEEP_FILES = {"_daily_clear_date.txt"}
        if _brief_dir.exists():
            for _f in _brief_dir.iterdir():
                if _f.name in _KEEP_FILES:
                    continue
                try:
                    if _f.is_file():
                        _f.unlink()
                    elif _f.is_dir():
                        _shutil.rmtree(_f)
                except Exception:
                    pass
    except Exception:
        pass

    # ③-b 根目录下的备用 scan_results.json
    try:
        _scan_f = _Path_cls("scan_results.json")
        if _scan_f.exists():
            _scan_f.unlink()
    except Exception:
        pass

    # ④ .cache_stock_data/ 股票行情 pickle 缓存（可能较大，全清保证数据新鲜）
    try:
        _stock_cache_dir = _Path_cls(".cache_stock_data")
        if _stock_cache_dir.exists():
            _cleared = 0
            for _pf in _stock_cache_dir.glob("*.pkl"):
                try:
                    _pf.unlink()
                    _cleared += 1
                except Exception:
                    pass
            _safe_print(f"  ④ 已清除股票行情 pickle 缓存 {_cleared} 个")
    except Exception:
        pass

    # ⑤ 全局 Gist 内存缓存变量重置（防止 Gist 旧结果在内存中滞留）
    try:
        global _gist_local_cache, _gist_last_sync_ts, _gist_last_sync_ok
        _gist_local_cache  = None
        _gist_last_sync_ts = 0
        _gist_last_sync_ok = False
    except Exception:
        pass

    # ⑥ 写入今天日期，防止 rerun 后再次触发
    try:
        _DAILY_CLEAR_FLAG.parent.mkdir(parents=True, exist_ok=True)
        _DAILY_CLEAR_FLAG.write_text(today_str, encoding="utf-8")
    except Exception:
        pass

    _safe_print(f"✅ [{today_str}] 全量缓存清零完成，正在重新加载（将自动触发后台重扫）...")
    st.rerun()


def _safe_str_for_dom(val):
    """移除控制字符、NaN、Inf 等，防止 InvalidCharacterError。用于 st.metric / st.markdown 等"""
    if val is None:
        return ""
    s = str(val)
    sl = s.lower().strip()
    if sl in ("nan", "inf", "-inf", "infinity", "-infinity"):
        return "N/A"
    if sl.startswith("nan") or sl.startswith("inf") or sl.startswith("-inf"):
        return "N/A"
    try:
        v = float(val)
        if v != v or v == float("inf") or v == float("-inf"):
            return "N/A"
    except (TypeError, ValueError):
        pass
    out = "".join(c for c in s if ord(c) >= 32 or c in "\n\t\r")
    if not out:
        return "N/A"
    return out

# 【V88】导入新模块
try:
    from modules import config as mod_config
    from modules import cache as mod_cache
    from modules import utils as mod_utils
    from modules import data_fetch as mod_data
    from modules import stock_pool as mod_pool
    from modules import analysis_core as mod_analysis
    from modules import ai_engine as mod_ai
    from modules import ui_components as mod_ui
    USE_NEW_MODULES = True
    _safe_print("✅ V88模块已加载（8个模块）")
except ImportError as e:
    USE_NEW_MODULES = False
    _safe_print(f"⚠️  V88模块未找到，使用原版逻辑: {e}")

# 【选股引擎】AI市场日报 684池筛选
try:
    from modules import selection_engine as mod_selection
    SELECTION_ENGINE_AVAILABLE = True
    _safe_print("✅ 选股引擎已加载（684池+ST/MT/LT）")
except ImportError as e:
    SELECTION_ENGINE_AVAILABLE = False
    mod_selection = None
    _safe_print(f"⚠️  选股引擎未找到，日报使用 pool[:15]: {e}")

# 【V89.2】导入机构研究中心
try:
    from institutional_research import InstitutionalResearch
    INSTITUTIONAL_RESEARCH_AVAILABLE = True
    _safe_print("✅ 机构研究中心模块已加载")
except ImportError as e:
    INSTITUTIONAL_RESEARCH_AVAILABLE = False
    _safe_print(f"⚠️  机构研究中心模块未找到: {e}")

# 【V89.3】导入持仓管理
try:
    from portfolio_manager import PortfolioManager
    PORTFOLIO_MANAGER_AVAILABLE = True
    _safe_print("✅ 持仓管理模块已加载")
except ImportError as e:
    PORTFOLIO_MANAGER_AVAILABLE = False
    _safe_print(f"⚠️  持仓管理模块未找到: {e}")

# 【Regime-Adaptive】导入市场状态自适应筛选引擎
try:
    from modules.regime import (
        MarketRegime, StrategyRouter, OpportunityClassifier,
        RiskForecaster, ActionEngine, QualityGuard, ReportComposer,
        LongCompounderGate, MarginOfSafetyGate,
        get_position_level_unified, ExpectationGapEngine,
    )
    REGIME_ENGINE_AVAILABLE = True
    _safe_print("✅ 市场状态自适应筛选引擎已加载")
except ImportError as e:
    REGIME_ENGINE_AVAILABLE = False
    ExpectationGapEngine = None
    LongCompounderGate = None
    MarginOfSafetyGate = None
    _safe_print(f"⚠️  市场状态自适应引擎未找到: {e}")

# 【潜力股双引擎】开关：True=双引擎+三池，False=回滚至原单一质量引擎
USE_POTENTIAL_ENGINE = True

# 【V89.4】导入舆情分析中心
try:
    from sentiment_analyzer import SentimentAnalyzer
    SENTIMENT_ANALYZER_AVAILABLE = True
    _safe_print("✅ 舆情分析中心模块已加载")
except ImportError as e:
    SENTIMENT_ANALYZER_AVAILABLE = False
    _safe_print(f"⚠️  舆情分析中心模块未找到: {e}")

# 【V89.5】导入复制和报告生成工具
try:
    from copy_utils import CopyUtils, ReportGenerator, ShareCardGenerator
    COPY_UTILS_AVAILABLE = True
    _safe_print("✅ 复制和报告生成工具已加载")
except ImportError as e:
    COPY_UTILS_AVAILABLE = False
    _safe_print(f"⚠️  复制和报告生成工具未找到: {e}")

# 【V88.12】导入预测引擎模块
try:
    from prediction_engine import InstitutionalPredictor, analyze_stock_with_predictor
    from market_forecast import MarketForecaster, forecast_all_markets
    HAS_PREDICTION_ENGINE = True
    _safe_print("✅ 前瞻预测引擎已加载")
except ImportError as e:
    HAS_PREDICTION_ENGINE = False
    _safe_print(f"⚠️  预测引擎未找到: {e}")

# 【V87.16 + V88】配置日志系统
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logging.info("=" * 60)
logging.info("🎉 AI 皇冠双核 V88 集成版启动")
logging.info("=" * 60)
if USE_NEW_MODULES:
    logging.info(f"✅ 模块化架构: V{mod_config.APP_VERSION}")
    logging.info(f"✅ LRU缓存系统: {mod_config.CACHE_MAX_SIZE_MB}MB")
    logging.info(f"✅ 缓存TTL: {mod_config.CACHE_TTL_SECONDS}秒")
logging.info("=" * 60)

# 尝试导入 yfinance
try:
    import yfinance as yf
    HAS_YFINANCE = True
    # 禁用 yfinance 内部 SQLite 缓存，防止多线程并发时 OperationalError: database is locked
    try:
        from yfinance import cache as _yf_cache
        _yf_cache._TzCacheManager._tz_cache = _yf_cache._TzCacheDummy()
        _yf_cache._CookieCacheManager._cookie_cache = _yf_cache._CookieCacheDummy()
        logging.info("✅ 已禁用 yfinance SQLite 缓存（防止多线程 OperationalError）")
    except Exception as _e:
        logging.debug(f"yfinance 缓存配置跳过: {_e}")
except ImportError:
    HAS_YFINANCE = False

# ── 全局 yfinance OperationalError 熔断器 ──────────────────────────────────
# OperationalError（SQLite锁）连续出现时，直接跳过 yfinance，走备用源
_YF_OPSERR_COUNT = 0
_YF_OPSERR_THRESHOLD = 3
_YF_OPSERR_DISABLED_UNTIL = 0.0
_YF_OPSERR_COOLDOWN = 300  # 5分钟冷却

def _yf_check_operational_error(e: Exception) -> bool:
    """检测是否为 SQLite OperationalError，触发熔断"""
    global _YF_OPSERR_COUNT, _YF_OPSERR_DISABLED_UNTIL
    err_name = type(e).__name__
    if err_name == 'OperationalError' or 'OperationalError' in str(e):
        _YF_OPSERR_COUNT += 1
        if _YF_OPSERR_COUNT >= _YF_OPSERR_THRESHOLD:
            _YF_OPSERR_DISABLED_UNTIL = time.time() + _YF_OPSERR_COOLDOWN
            logging.warning(f"🚫 yfinance OperationalError 连续 {_YF_OPSERR_COUNT} 次，熔断 {_YF_OPSERR_COOLDOWN}s")
        return True
    _YF_OPSERR_COUNT = 0
    return False

def _yf_opserr_blocked() -> bool:
    """yfinance 是否因 OperationalError 被熔断"""
    global _YF_OPSERR_DISABLED_UNTIL, _YF_OPSERR_COUNT
    if _YF_OPSERR_DISABLED_UNTIL > 0 and time.time() < _YF_OPSERR_DISABLED_UNTIL:
        return True
    if _YF_OPSERR_DISABLED_UNTIL > 0:
        _YF_OPSERR_DISABLED_UNTIL = 0.0
        _YF_OPSERR_COUNT = 0
    return False

# ── 全局 yfinance Rate Limit 熔断器 ──────────────────────────────────────
# 一旦触发 rate limit，60 秒内不再向 Yahoo Finance 发请求，直接降级到备用源
_YF_RATE_LIMITED_UNTIL = 0.0        # Unix timestamp，限流解除时间
_YF_RATE_LIMIT_COOLDOWN = 60        # 冷却秒数

def _yf_is_rate_limited() -> bool:
    return time.time() < _YF_RATE_LIMITED_UNTIL

# ── 智能代理端口自动探测 ──────────────────────────────────────────────────
# 自动从 macOS 系统网络设置读取当前代理端口，避免代理软件换端口后整个应用瘫痪
_AUTO_PROXY_PORT = None

def _detect_system_proxy_port() -> str:
    """从 macOS 系统代理设置自动读取当前 HTTP 代理端口"""
    global _AUTO_PROXY_PORT
    if _AUTO_PROXY_PORT is not None:
        return _AUTO_PROXY_PORT
    try:
        import subprocess
        out = subprocess.check_output(["scutil", "--proxy"], timeout=2, text=True)
        port = None
        enabled = False
        for line in out.splitlines():
            line = line.strip()
            if "HTTPEnable" in line and "1" in line:
                enabled = True
            if "HTTPPort" in line:
                port = line.split(":")[-1].strip()
        if enabled and port and port.isdigit():
            _AUTO_PROXY_PORT = port
            logging.info(f"✅ 自动检测系统代理端口: {port}")
            return port
    except Exception:
        pass
    _AUTO_PROXY_PORT = "7897"
    return _AUTO_PROXY_PORT

# ── 全局代理健康检测 ──────────────────────────────────────────────────────
_PROXY_DEAD = False
_PROXY_CHECKED = False

def _check_proxy_health():
    """快速检测代理是否可用（TCP 连接测试，不依赖外部域名）"""
    global _PROXY_DEAD, _PROXY_CHECKED
    if _PROXY_CHECKED:
        return
    _PROXY_CHECKED = True
    _port = _detect_system_proxy_port()
    if not _port:
        _PROXY_DEAD = True
        logging.warning("🚫 未检测到系统代理端口")
        return
    import socket
    try:
        _sock = socket.create_connection(("127.0.0.1", int(_port)), timeout=2)
        _sock.close()
        _PROXY_DEAD = False
        logging.info(f"✅ 代理 127.0.0.1:{_port} 可用（TCP 连接正常）")
    except Exception:
        _PROXY_DEAD = True
        logging.warning(f"🚫 代理 127.0.0.1:{_port} 不可用，数据源将绕过代理直连")

def _is_proxy_dead() -> bool:
    if not _PROXY_CHECKED:
        _check_proxy_health()
    return _PROXY_DEAD

def _yf_mark_rate_limited():
    global _YF_RATE_LIMITED_UNTIL
    _YF_RATE_LIMITED_UNTIL = time.time() + _YF_RATE_LIMIT_COOLDOWN
    logging.warning(f"🚫 Yahoo Finance rate limited，{_YF_RATE_LIMIT_COOLDOWN}s 内跳过 yfinance，使用备用源")

# ── 全局 东方财富 熔断器 ──────────────────────────────────────────────────
# 东财不可达（被封/网络不通/返回空）时，连续失败即熔断；熔断期间所有标的
# 直接跳过东财走 yfinance，避免每个标的都卡满 connect 超时（这是 Top30/选股
# /宏观面板大面积 N/A 和「分析超时」的主因）。源恢复后冷却结束自动重新启用。
_EM_FAIL_COUNT = 0
_EM_FAIL_THRESHOLD = 2
_EM_DISABLED_UNTIL = 0.0
_EM_COOLDOWN = 180  # 3 分钟

def _em_blocked() -> bool:
    global _EM_DISABLED_UNTIL, _EM_FAIL_COUNT
    if _EM_DISABLED_UNTIL > 0 and time.time() < _EM_DISABLED_UNTIL:
        return True
    if _EM_DISABLED_UNTIL > 0:  # 冷却结束，重置重新尝试
        _EM_DISABLED_UNTIL = 0.0
        _EM_FAIL_COUNT = 0
    return False

def _em_mark(ok: bool):
    global _EM_FAIL_COUNT, _EM_DISABLED_UNTIL
    if ok:
        _EM_FAIL_COUNT = 0
        _EM_DISABLED_UNTIL = 0.0
    else:
        _EM_FAIL_COUNT += 1
        if _EM_FAIL_COUNT >= _EM_FAIL_THRESHOLD and _EM_DISABLED_UNTIL == 0.0:
            _EM_DISABLED_UNTIL = time.time() + _EM_COOLDOWN
            logging.warning(f"🚫 东方财富连续 {_EM_FAIL_COUNT} 次失败/空，熔断 {_EM_COOLDOWN}s，期间直接走 yfinance")

# ── 全局 免费行情源 熔断器 ───────────────────────────────────────────────────
# 免费源适配失败仅跳过当前入口；没有Token、积分或收费恢复路径。
_TS_DISABLED = False

def _ts_blocked() -> bool:
    return _TS_DISABLED

def _ts_mark_dead(reason: str = ""):
    global _TS_DISABLED
    if not _TS_DISABLED:
        _TS_DISABLED = True
        logging.warning(f"🚫 免费行情源 不可用（{reason}），本会话跳过 免费行情源，A股直接走 yfinance")

def _normalize_hk_for_yahoo(symbol: str) -> str:
    """雅虎港股代码用 4 位（去前导零后零填充到 4 位）。app 内部用 5 位(00700.HK)
    会被雅虎判为退市，故仅在调用 yfinance 时做格式归一：00700.HK→0700.HK、09988.HK→9988.HK。"""
    if not symbol.endswith('.HK'):
        return symbol
    code = symbol[:-3]
    digits = code.lstrip('0') or '0'
    if digits.isdigit() and len(digits) <= 4:
        return f"{int(digits):04d}.HK"
    return symbol

# 【GPT-6 Codex订阅迁移】
try:
    import desktop_gpt_subscription as _gpt_subscription
    HAS_GEMINI = True                 # 历史变量名，实为“GPT-6订阅调用模块可用”
    genai = None
except ImportError:
    HAS_GEMINI = False
    genai = None
    
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ═══════════════════════════════════════════════════════════════
# 配置中心：先读 config.toml，缺失项用内置默认值
# ═══════════════════════════════════════════════════════════════
def _load_config_toml() -> dict:
    try:
        import tomllib  # Python 3.11+
    except ImportError:
        try:
            import tomli as tomllib  # pip install tomli
        except ImportError:
            return {}
    _p = Path(__file__).parent / "config.toml"
    if _p.exists():
        with open(_p, "rb") as f:
            return tomllib.load(f)
    return {}

_TOML = _load_config_toml()


class Config:
    """全局配置中心 — 优先读 config.toml，缺失项回退到内置默认值"""

    ENABLE_EXPECTATION_LAYER = _TOML.get("features", {}).get("enable_expectation_layer", True)
    ENABLE_PERF_LAYER        = _TOML.get("features", {}).get("enable_perf_layer", True)

    CACHE_TTL       = _TOML.get("cache", {}).get("ttl_daily", 3600)
    RETRY_COUNT     = _TOML.get("data", {}).get("retry_count", 1)
    REQUEST_TIMEOUT = _TOML.get("data", {}).get("request_timeout", 5)

    CACHE_TTL_FAST   = _TOML.get("cache", {}).get("ttl_fast",   900)
    CACHE_TTL_DAILY  = _TOML.get("cache", {}).get("ttl_daily",  3600)
    CACHE_TTL_WEEKLY = _TOML.get("cache", {}).get("ttl_weekly", 21600)

    MAX_WORKERS  = _TOML.get("concurrency", {}).get("max_workers",  8)
    TASK_TIMEOUT = _TOML.get("concurrency", {}).get("task_timeout", 15)

    MACRO_ASSETS = ['SPY', 'QQQ', 'TLT', 'GLD', '^VIX', '^TNX', 'DX-Y.NYB']

    TNX_LOOSE = _TOML.get("rates", {}).get("tnx_loose", 3.5)
    TNX_TIGHT = _TOML.get("rates", {}).get("tnx_tight", 4.5)

    DXY_WEAK   = _TOML.get("dollar", {}).get("dxy_weak",   100)
    DXY_STRONG = _TOML.get("dollar", {}).get("dxy_strong", 105)

    MA_SHORT    = _TOML.get("technical", {}).get("ma_short",    50)
    MA_LONG     = _TOML.get("technical", {}).get("ma_long",     200)
    CORR_WINDOW = _TOML.get("technical", {}).get("corr_window", 20)

    VIX_PANIC = _TOML.get("vix", {}).get("panic", 30)
    VIX_HIGH  = _TOML.get("vix", {}).get("high",  20)
    VIX_LOW   = _TOML.get("vix", {}).get("low",   15)

    MACRO_PERIOD = _TOML.get("data", {}).get("macro_period", "1y")

    SMART_CACHE_ENABLED     = True
    CACHE_TTL_WORKDAY       = _TOML.get("cache", {}).get("ttl_workday", 900)
    SCAN_CACHE_TTL          = _TOML.get("cache", {}).get("scan_ttl",    900)
    CACHE_TTL_WEEKEND       = _TOML.get("cache", {}).get("ttl_weekend", 86400)
    CACHE_TTL_TRADING_HOURS = _TOML.get("cache", {}).get("ttl_workday", 900)

    PORTFOLIO_FILE    = 'my_portfolio.xlsx'
    PORTFOLIO_ENABLED = False  # 已禁用：持仓管理模块存在兼容性问题


# ═══════════════════════════════════════════════════════════════
# 【V89.3 + V91.3 + V99.7】智能缓存 - 全模块统一1小时（2026-07-09 用户要求）
# ═══════════════════════════════════════════════════════════════

def get_smart_cache_ttl(data_type: str = 'daily') -> int:
    """
    智能缓存TTL - 全模块统一1小时（交易日/非交易日同值，由 config.toml 配置）
    
    参数：
        data_type: 数据类型（'fast'/'daily'/'weekly'）
    
    返回：
        TTL秒数
    """
    if not Config.SMART_CACHE_ENABLED:
        # 如果未启用智能缓存，使用默认配置
        if data_type == 'fast':
            return Config.CACHE_TTL_FAST
        elif data_type == 'weekly':
            return Config.CACHE_TTL_WEEKLY
        else:
            return Config.CACHE_TTL_DAILY
    
    from datetime import datetime
    import pytz
    
    try:
        # 获取当前时间（美东时间，因为美股市场）
        now_et = datetime.now(pytz.timezone('America/New_York'))
        weekday = now_et.weekday()  # 0=周一, 6=周日
        hour = now_et.hour
        
        # 判断是否为非交易日（周六日）
        if weekday >= 5:  # 5=周六, 6=周日
            return Config.CACHE_TTL_WEEKEND  # 24小时
        
        # 交易日（周一至周五）：1小时（全模块统一）
        return Config.CACHE_TTL_WORKDAY
    
    except Exception as e:
        # 异常时使用默认配置
        logging.warning(f"智能缓存TTL计算异常: {e}，使用默认配置")
        return Config.CACHE_TTL_DAILY


# ═══════════════════════════════════════════════════════════════
# 【V91.4】扫描结果文件持久化缓存 - 跨会话/刷新后仍有效（15分钟/24小时）
# ═══════════════════════════════════════════════════════════════

SCAN_CACHE_DIR = Path(__file__).resolve().parent / "scan_cache"

def market_of_code(code: str) -> str:
    """按代码判市场（全局唯一口径）。兼容池内无后缀代码：
    ≤5位纯数字=港股(02269)，6位数字开头=A股(688126/600030)，其余=美股。"""
    c = str(code).strip().upper()
    if c.endswith(".HK") or (c.isdigit() and len(c) <= 5):
        return "🇭🇰港股"
    if c.endswith((".SS", ".SZ", ".SH", ".BJ")) or (len(c) >= 6 and c[:6].isdigit()):
        return "🇨🇳A股"
    return "🇺🇸美股"

@st.cache_data(ttl=3600, show_spinner=False)
def _ath_pct(symbol: str):
    """距历史最高点：(水位%, 高点日期, 距今天数, 最新收盘价, 52周分位%)。全量历史真ATH（上证含2007年顶），1小时缓存"""
    try:
        import yfinance as _yfa
        h = _yfa.Ticker(symbol).history(period="max")["Close"].dropna()
        if len(h) < 100:
            return None
        last = float(h.iloc[-1])
        pct = (last / float(h.max()) - 1) * 100
        d_ath = h.idxmax()
        days = (h.index[-1] - d_ath).days
        # 【V88·双水位】另算近52周（约252交易日）分位：现价在一年高低之间的位置
        _w52 = h.tail(min(252, len(h)))
        _lo52, _hi52 = float(_w52.min()), float(_w52.max())
        _p52 = (last - _lo52) / (_hi52 - _lo52) * 100 if _hi52 > _lo52 else 50.0
        return (pct, str(d_ath)[:10], int(days), last, round(_p52))
    except Exception:
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def _price_extremes9(symbol: str):
    """【V88·公司档案 2026-07-18】历史最高/最低+52周高低(全量历史,像东财盘口)。"""
    try:
        import yfinance as _yfe
        h = _yfe.Ticker(symbol).history(period="max")["Close"].dropna()
        if len(h) < 20:
            return None
        last = float(h.iloc[-1])
        w52 = h.tail(min(252, len(h)))
        return {"hist_high": float(h.max()), "hist_high_date": str(h.idxmax())[:10],
                "hist_low": float(h.min()), "hist_low_date": str(h.idxmin())[:10],
                "w52_high": float(w52.max()), "w52_low": float(w52.min()), "last": last}
    except Exception:
        return None


def _yf_norm_code(code: str) -> str:
    """持仓/自选代码 → yfinance 代码（.SH→.SS，6位纯数字补后缀）"""
    _yf = str(code).strip().upper().replace(".SH", ".SS")
    if _yf.isdigit() and len(_yf) == 6:
        _yf += ".SS" if _yf[0] in "569" else ".SZ"
    return _yf


@st.cache_data(ttl=3600, show_spinner=False)
def _last_px_many(codes: tuple) -> dict:
    """批量最新收盘价（复用 _ath_pct 的历史数据缓存，不重复下载）"""
    from concurrent.futures import ThreadPoolExecutor as _PxPool

    def _one(_code):
        _raw = str(_code).strip().upper()
        _r = _ath_pct(_yf_norm_code(_raw))
        return _raw, (float(_r[3]) if _r and len(_r) > 3 else None)

    with _PxPool(max_workers=min(8, max(1, len(codes)))) as _ex:
        return dict(_ex.map(_one, codes))


@st.cache_data(ttl=3600, show_spinner=False)
def _fx_to_cny(pair: str):
    """汇率（如 USDCNY=X / HKDCNY=X）→ float，失败返回 None"""
    try:
        import yfinance as _yfx
        _h = _yfx.Ticker(pair).history(period="5d")["Close"].dropna()
        return float(_h.iloc[-1]) if len(_h) else None
    except Exception:
        return None


def _ath_txt(symbol):
    r = _ath_pct(symbol)
    if r is None:
        return ""
    pct, d, days = r[0], r[1], r[2]
    _p52 = r[4] if len(r) > 4 else None
    _dur = f"{days/365:.1f}年" if days >= 365 else f"{days}天"
    _s52 = f" · 52周位置 {int(_p52)}%" if _p52 is not None else ""
    return f" | 距历史最高 {pct:+.1f}%（{d} 创下·已 {_dur}）{_s52}"


@st.cache_data(ttl=3600, show_spinner=False)
def _ath_many_display(codes: tuple) -> dict:
    """批量个股历史水位：距全历史最高百分比 + 高点相隔自然日数。"""
    from concurrent.futures import ThreadPoolExecutor as _AthPool

    def _one(_code):
        _raw = str(_code).strip().upper()
        _r = _ath_pct(_yf_norm_code(_raw))
        if not _r:
            return _raw, "历史水位待核"
        _pct, _date, _days = _r[0], _r[1], _r[2]
        _p52 = _r[4] if len(_r) > 4 else None
        _s52 = f"·52周{int(_p52)}%" if _p52 is not None else ""
        return _raw, f"距历史最高{float(_pct):+.1f}%｜高点相隔{int(_days)}天{_s52}"

    with _AthPool(max_workers=min(8, max(1, len(codes)))) as _ex:
        return dict(_ex.map(_one, codes))


def _scan_cache_key(scan_type: str, scan_market: str, risk_pref: str = None) -> str:
    """生成扫描缓存文件键"""
    # 评分口径升级必须换缓存命名空间，禁止旧分数在新页面继续展示。
    key = f"v88u2_{scan_type}_{scan_market}"
    if scan_type == 'regime' and risk_pref:
        key += f"_{risk_pref}"
    return key.replace(" ", "_")


# ══════════════════════════════════════════════════════════════════════
# 【V88·使用先于定义 第十例修复 2026-08-15】
# 症状:作战板"④买什么"整块崩溃,render_errors 31条(08-10~08-15,每次开页必崩)。
# 根因:_v88_buy_gate9 在 4957 行被调用,却到 14688 行才 def —— Streamlit 模块级
#      自上而下执行,调用时名字尚未绑定 → NameError,整个 with _tbR9: 块炸掉。
# 同族第九例是 3460 行 _chip9 的 UnboundLocalError(08-06 已修),病根一样:
#      "共享工具函数散落在使用点之后",靠源码顺序碰运气。
# 修法:把作战板买入闸所依赖的**全部**符号(含其运行时才解析的 _canonical_code /
#      _v88_intel9,它们原在 try/except 里 —— 若只搬一半,NameError 会被 except
#      吞掉变成"拥挤闸静默失效",那是铁律21 明令禁止的静默吞错)整体前置到此处。
# 位置约束:必须晚于 market_of_code(1057)、re(147)、json(27)、Path(142),早于 4957。
# 校验:pyflakes undefined-name 归零 + py_compile 通过(铁律6)。
# ══════════════════════════════════════════════════════════════════════

def _canonical_code(code):
    """把同一只票的不同写法归一，只用于去重比较（不改展示代码）。
    港股按数字补零到5位：00700.HK == 0700.HK；A股裸6位补市场后缀；.SH→.SS。"""
    c = str(code).strip().upper()
    if c.endswith(".HK"):
        digits = re.sub(r"\D", "", c[:-3])
        return (digits.lstrip("0").zfill(5) + ".HK") if digits else c
    if c.endswith(".SH"):
        c = c[:-3] + ".SS"
    if c.isdigit() and len(c) == 6:
        c += ".SS" if c[0] in "569" else ".SZ"
    return c


_V88_NEWS_CACHE = {"ts": 0, "news": []}


def _v88_intel9():
    """【V88·情报二期 2026-07-18】政策直采+人气榜(私仓落盘,10分钟缓存)。
    返回 {policy:[...], hot_map:{canon:rank}}"""
    import time as _t
    _c = _V88_NEWS_CACHE.setdefault("_intel", {"ts": 0, "d": {}})
    if _t.time() - _c["ts"] < 600 and _c["d"]:
        return _c["d"]
    try:
        _raw = json.loads((core_root() / "data" /
                           "intel_feed.json").read_text(encoding="utf-8"))
        _c["d"] = {"policy": _raw.get("policy") or [],
                   "hot_map": {str(h.get("canon")): {"rank": int(h.get("rank") or 0),
                                                     "xq": h.get("xq_rank")}
                               for h in (_raw.get("hot") or [])},
                   "xq_map": {str(x.get("canon")): int(x.get("rank") or 0)
                              for x in (_raw.get("hot_xq") or [])},
                   "us_map": {str(u.get("symbol", "")).upper(): int(u.get("rank") or 0)
                              for u in (_raw.get("hot_us") or [])},
                   "hot_raw": _raw.get("hot") or [],
                   "hot_xq_raw": _raw.get("hot_xq") or [],
                   "hot_us_raw": _raw.get("hot_us") or [],
                   "generated_at": _raw.get("generated_at", "")}
    except Exception:
        _c["d"] = {"policy": [], "hot_map": {}}
    _c["ts"] = _t.time()
    return _c["d"]


_V88_MKT_SCORES_CACHE9 = {"ts": 0.0, "scores": {}}


def _v88_market_scores9(_repo):
    """各大盘2周方向分（顺风闸用），10分钟缓存。"""
    import time as _tm
    if _tm.time() - _V88_MKT_SCORES_CACHE9["ts"] < 600 and _V88_MKT_SCORES_CACHE9["scores"]:
        return _V88_MKT_SCORES_CACHE9["scores"]
    _out = {}
    try:
        _s = json.loads((_repo / "data" / "market_snapshot.json").read_text(encoding="utf-8"))
        for _mk, _blk in (_s.get("markets") or {}).items():
            for _lb, _pv in ((_blk.get("l3") or {}).get("probs") or []):
                if _lb == "2周":
                    _out[_mk] = int(_pv)
                    break
    except Exception:
        pass
    _V88_MKT_SCORES_CACHE9.update({"ts": _tm.time(), "scores": _out})
    return _out


def _v88_buy_gate9(_d, _repo):
    """【V88·推送严选五闸 2026-07-20 用户定纲"推送成功率一定要高"】买入推送只留多信号共振：
    ①概率闸 2周上涨概率≥65 ②赔率闸 盈亏比≥1.5 ③周期冲突否决
    ④顺风闸 所属大盘2周分≥45（逆势买入=历史主要亏损源，大盘弱势整市场停推买入）
    ⑤拥挤闸 东财+雪球双榜热股=散户扎堆反指标，硬否决。
    宁可少推不可错推——严选提高的是"推给你的"命中率，完整候选仍在双门/黑马模块可看。
    返回 (通过?, 未过原因)。战绩熔断（类型命中<40%整类停推）由调用方执行。"""
    _pu = int(_d.get("p_up") or 0)
    if _pu < 65:
        return False, ("概率数据缺失" if not _pu else f"概率{_pu}%<65")
    try:
        _rr = float(_d.get("rr") or 0)
    except (TypeError, ValueError):
        _rr = 0.0
    if _rr < 1.5:
        return False, ("赔率数据缺失" if not _rr else f"盈亏比{_rr:.1f}<1.5")
    # 概率与赔率必须共同形成正期望，不能各自过线后仍输出负期望买单。
    _edge9 = (_pu / 100.0) * _rr - (1.0 - _pu / 100.0)
    if _edge9 < 0.35:
        return False, f"期望优势{_edge9:.2f}<0.35"
    if _d.get("px_usable_as_current") is False:
        return False, "行情时点已过期"
    # 名称—代码错配一票否决（03366.HK 被误标中兴通讯事故的永久防线）。
    try:
        import sys as _sys_rg9
        _rgp9 = str(_repo / "src")
        if _rgp9 not in _sys_rg9.path:
            _sys_rg9.path.insert(0, _rgp9)
        from recommendation_gate import identifier_check as _id_check9
        from recommendation_gate import review_for as _review_for9
        from recommendation_gate import review_is_fresh as _review_fresh9
        _id_ok9, _id_why9, _ = _id_check9(_d.get("code"), _d.get("name"))
        if not _id_ok9:
            return False, _id_why9
        _rule9, _gpt9, _gpt_ts9 = _review_for9(_d.get("code"))
        if _rule9 != "gate_pass":
            return False, "V88规则闸未通过"
        if _gpt9 != "通过":
            return False, f"GPT复核{_gpt9 or '缺失'}"
        if not _review_fresh9(_gpt_ts9):
            return False, "GPT复核已过期或时间缺失"
        from recommendation_gate import classics_review_for as _book_for9
        _book9, _book_ts9 = _book_for9(_d.get("code"))
        if _book9 != "通过":
            return False, f"经典巨著复核{_book9 or '缺失'}"

    except Exception:
        return False, "统一复核闸不可用"
    if _d.get("cycle_conflict"):
        return False, "周期冲突"
    _mk = str(_d.get("market") or "")
    if not any(_k in _mk for _k in ("美股", "港股", "A股")):
        try:
            _mk = market_of_code(str(_d.get("code") or "")) or ""
        except Exception:
            _mk = ""
    for _k, _v in _v88_market_scores9(_repo).items():
        if _k in _mk and _v is not None and _v < 45:
            return False, f"{_k}大盘2周分{_v}·逆风停推"
    try:
        _cn = _canonical_code(str(_d.get("code") or ""))
        _hot = (_v88_intel9().get("hot_map") or {}).get(_cn)
        if _hot and _hot.get("xq"):
            return False, "双榜拥挤·反指标"
    except Exception:
        pass
    return True, ""


def _load_scan_cache_from_file(scan_type: str, scan_market: str, risk_pref: str = None):
    """从文件加载扫描缓存，命中则返回结果 dict，否则返回 None"""
    try:
        SCAN_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        ckey = _scan_cache_key(scan_type, scan_market, risk_pref)
        fp = SCAN_CACHE_DIR / f"{ckey}.pkl"
        if not fp.exists():
            return None
        with open(fp, "rb") as f:
            data = pickle.load(f)
        if not isinstance(data, dict):
            return None
        ts = data.get("scan_timestamp", 0)
        ttl = get_smart_cache_ttl("daily")
        if (time.time() - ts) >= ttl:
            return None
        if data.get("type") != scan_type or data.get("scan_market") != scan_market:
            return None
        if scan_type == "regime" and risk_pref and data.get("risk_preference") != risk_pref:
            return None
        return data
    except Exception as e:
        logging.debug(f"加载扫描缓存失败: {e}")
        return None

def _publish_scan_to_cloud(data: dict):
    """【V99.6】把最近一次「一键全策略」榜单发布到公开仓 stockai-v88 data 分支
    pub/scan_latest.json，云端查看器免引擎直接展示最近缓存结果。
    原则：V88 是主体——本地扫出什么，云端就看什么。gh CLI 免密；10分钟节流；
    只发榜单行（无持仓等隐私）。后台线程调用，失败静默不影响本地。"""
    import base64 as _b64
    import subprocess as _sp
    try:
        marker = SCAN_CACHE_DIR / "pub_scan_last.txt"
        if marker.exists():
            try:
                if time.time() - float(marker.read_text(encoding='utf-8').strip() or 0) < 600:
                    return
            except Exception:
                pass
        rows = data.get("data") or []
        if not rows:
            return
        payload = {
            "generated_at": time.strftime(
                "%Y-%m-%d %H:%M:%S",
                time.localtime(data.get("scan_timestamp", time.time()))),
            "scan_market": data.get("scan_market", ""),
            "rows": [{**r, "市场": market_of_code(r.get("代码", ""))} for r in rows],
        }
        content = _b64.b64encode(
            json.dumps(payload, ensure_ascii=False).encode("utf-8")).decode()
        # Streamlit 进程的 PATH 可能不含 homebrew，gh 需绝对路径解析（22:43 静默失败根因）
        import shutil as _sh99
        _gh = (_sh99.which("gh") or next((p for p in ("/opt/homebrew/bin/gh", "/usr/local/bin/gh")
                                          if Path(p).exists()), "gh"))
        _repo_path = "repos/bluestevenr-lang/stockai-v88/contents/pub/scan_latest.json"
        sha = ""
        try:
            sha = _sp.run([_gh, "api", f"{_repo_path}?ref=data", "-q", ".sha"],
                          capture_output=True, text=True, timeout=20).stdout.strip()
        except Exception:
            sha = ""
        cmd = [_gh, "api", "-X", "PUT", _repo_path,
               "-f", "message=publish scan_latest (auto from V88)",
               "-f", "branch=data", "-f", f"content={content}"]
        if sha:
            cmd += ["-f", f"sha={sha}"]
        r = _sp.run(cmd, capture_output=True, text=True, timeout=45)
        if r.returncode == 0:
            marker.write_text(str(time.time()))
            logging.info("☁️ 一键全选榜单已发布到云端 pub/scan_latest.json")
        else:
            logging.warning(f"⚠️ 云端榜单发布失败: {(r.stderr or '')[:200]}")
    except Exception as e:
        logging.warning(f"⚠️ 云端榜单发布异常: {e}")


def _save_scan_cache_to_file(data: dict):
    """将扫描结果保存到文件"""
    try:
        SCAN_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        stype = data.get("type", "")
        mkt = data.get("scan_market", "")
        rpref = data.get("risk_preference") if stype == "regime" else None
        ckey = _scan_cache_key(stype, mkt, rpref)
        fp = SCAN_CACHE_DIR / f"{ckey}.pkl"
        with open(fp, "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
        # 【V99.6】一键全策略结果 → 后台同步到云端查看器（不阻塞UI）
        if stype == "unified":
            try:
                import threading as _th
                _th.Thread(target=_publish_scan_to_cloud, args=(dict(data),),
                           daemon=True).start()
            except Exception:
                pass
    except Exception as e:
        logging.debug(f"保存扫描缓存失败: {e}")

def _clear_scan_cache_files():
    """清除所有扫描缓存文件（与清除按钮联动）"""
    try:
        if SCAN_CACHE_DIR.exists():
            for f in SCAN_CACHE_DIR.glob("*.pkl"):
                f.unlink()
    except Exception as e:
        logging.debug(f"清除扫描缓存文件失败: {e}")


# ═══════════════════════════════════════════════════════════════
# 【V89 Phase 2】性能优化层 - 分层缓存 + 性能监控
# ═══════════════════════════════════════════════════════════════

class PerformanceMonitor:
    """
    性能监控器 - 记录各阶段耗时和缓存命中率
    目标：可观测性、性能调优、问题定位
    """
    def __init__(self):
        self.metrics = {
            'fetch_time_ms': 0,
            'compute_time_ms': 0,
            'render_time_ms': 0,
            'total_time_ms': 0,
            'cache_hit_count': 0,
            'cache_miss_count': 0,
            'cache_items_count': 0,
            'stale_fallback_count': 0,
            'error_count': 0
        }
        self.start_time = None
    
    def start(self):
        """开始计时"""
        self.start_time = time.time()
    
    def record(self, stage: str, elapsed_ms: float):
        """记录某阶段耗时"""
        key = f"{stage}_time_ms"
        if key in self.metrics:
            self.metrics[key] += elapsed_ms
    
    def cache_hit(self):
        """缓存命中"""
        self.metrics['cache_hit_count'] += 1
    
    def cache_miss(self):
        """缓存未命中"""
        self.metrics['cache_miss_count'] += 1
    
    def stale_fallback(self):
        """使用过期缓存"""
        self.metrics['stale_fallback_count'] += 1
    
    def error(self):
        """记录错误"""
        self.metrics['error_count'] += 1
    
    def get_cache_hit_ratio(self) -> float:
        """计算缓存命中率"""
        total = self.metrics['cache_hit_count'] + self.metrics['cache_miss_count']
        if total == 0:
            return 0.0
        return self.metrics['cache_hit_count'] / total
    
    def finalize(self):
        """结束计时，计算总耗时"""
        if self.start_time:
            self.metrics['total_time_ms'] = (time.time() - self.start_time) * 1000
    
    def get_metrics(self) -> dict:
        """获取所有指标"""
        return self.metrics.copy()
    
    def reset(self):
        """重置所有指标"""
        self.__init__()


class LayeredCacheManager:
    """
    分层缓存管理器 - 按数据类型分配不同TTL
    目标：高频数据快速过期，低频数据长期缓存
    """
    def __init__(self, perf_monitor: PerformanceMonitor = None):
        self._cache = {}  # {key: {'value': data, 'ts': timestamp, 'type': data_type}}
        self.perf = perf_monitor or PerformanceMonitor()
        self.logger = logging.getLogger(__name__)
    
    def _get_ttl(self, data_type: str) -> int:
        """
        根据数据类型返回TTL
        
        【V89.3】使用智能缓存：工作日10分钟，休息日24小时
        """
        if Config.SMART_CACHE_ENABLED:
            # 使用智能缓存TTL
            return get_smart_cache_ttl(data_type)
        else:
            # 使用固定TTL
            ttl_map = {
                'fast': Config.CACHE_TTL_FAST,    # 15分钟
                'daily': Config.CACHE_TTL_DAILY,  # 1小时
                'weekly': Config.CACHE_TTL_WEEKLY  # 6小时
            }
            return ttl_map.get(data_type, Config.CACHE_TTL)
    
    def get(self, key: str, data_type: str = 'daily', force_refresh: bool = False):
        """
        获取缓存
        
        返回：(value, is_stale)
        - value: 缓存值或None
        - is_stale: 是否过期（True=过期但可用，False=新鲜）
        """
        if force_refresh:
            self.logger.info(f"🔄 强制刷新: {key}")
            self.perf.cache_miss()
            return None, False
        
        if key not in self._cache:
            self.perf.cache_miss()
            return None, False
        
        cached = self._cache[key]
        age = time.time() - cached['ts']
        ttl = self._get_ttl(data_type)
        
        if age < ttl:
            # 缓存新鲜
            self.perf.cache_hit()
            self.logger.info(f"✅ 缓存命中: {key} (新鲜度: {int(age)}秒/{ttl}秒)")
            return cached['value'], False
        else:
            # 缓存过期但仍可用
            self.perf.stale_fallback()
            self.logger.warning(f"⚠️  缓存过期: {key} (已过期: {int(age-ttl)}秒)")
            return cached['value'], True
    
    def set(self, key: str, value, data_type: str = 'daily'):
        """设置缓存"""
        self._cache[key] = {
            'value': value,
            'ts': time.time(),
            'type': data_type
        }
        self.logger.info(f"💾 缓存已保存: {key} (类型: {data_type}, TTL: {self._get_ttl(data_type)}秒)")
    
    def clear(self, key: str = None):
        """清除缓存"""
        if key:
            if key in self._cache:
                del self._cache[key]
                self.logger.info(f"🗑️  已清除缓存: {key}")
        else:
            count = len(self._cache)
            self._cache.clear()
            self.logger.info(f"🗑️  已清除所有缓存: {count}项")
    
    def get_stats(self) -> dict:
        """获取缓存统计"""
        return {
            'items_count': len(self._cache),
            'total_size_mb': sum(
                len(str(v['value'])) for v in self._cache.values()
            ) / 1024 / 1024
        }


# 全局实例 —— 用 @st.cache_resource 使其在所有 rerun 之间持久化
# 若不持久化，Streamlit 每次 rerun 都会重建空缓存，导致数据每次都重新拉取
@st.cache_resource
def _get_perf_monitor():
    return PerformanceMonitor()

@st.cache_resource
def _get_cache_manager():
    return LayeredCacheManager(_get_perf_monitor())

_perf_monitor = _get_perf_monitor()
_cache_manager = _get_cache_manager()

# ═══════════════════════════════════════════════════════════════
# 东方财富万能数据源（Yahoo 被封时的主力替代）
# 覆盖：美股、港股、A股、全球指数、汇率
# ★ 必须在 DataProvider 之前定义，因为 fetch_safe() 会调用它
# ═══════════════════════════════════════════════════════════════
_EM_SECID_KNOWN = {
    'SPY': '107.SPY', 'QQQ': '105.QQQ', 'TLT': '105.TLT', 'GLD': '107.GLD',
    'IWM': '107.IWM', 'EEM': '107.EEM', 'XLF': '107.XLF', 'VTI': '107.VTI',
    'NVDA': '105.NVDA', 'AAPL': '105.AAPL', 'TSLA': '105.TSLA', 'MSFT': '105.MSFT',
    'GOOGL': '105.GOOGL', 'GOOG': '105.GOOG', 'META': '105.META', 'AMZN': '105.AMZN',
    'NFLX': '105.NFLX', 'AMD': '105.AMD', 'INTC': '105.INTC', 'AVGO': '105.AVGO',
    'JPM': '106.JPM', 'JNJ': '106.JNJ', 'WMT': '106.WMT', 'CAT': '106.CAT',
    'PLD': '106.PLD', 'NEE': '106.NEE', 'LIN': '106.LIN', 'T': '106.T',
    'V': '106.V', 'MA': '106.MA', 'BAC': '106.BAC', 'GS': '106.GS',
    'UNH': '106.UNH', 'HD': '106.HD', 'DIS': '106.DIS', 'KO': '106.KO',
    'PG': '106.PG', 'MRK': '106.MRK', 'ABBV': '106.ABBV', 'PFE': '106.PFE',
    'XOM': '106.XOM', 'CVX': '106.CVX', 'CRM': '106.CRM',
    'NVO': '105.NVO', 'LLY': '106.LLY', 'TSM': '106.TSM', 'PM': '106.PM',
    'ACMR': '105.ACMR', 'BRK-B': '106.BRK.B', 'QQQM': '105.QQQM', 'VOO': '107.VOO',
    '^HSI': '100.HSI', '^HSTECH': '124.HSTECH', '^HSCE': '100.HSCEI',
    '^GSPC': '100.SPX', '^DJI': '100.DJIA', '^IXIC': '100.NDX',
    '^VIX': '100.VIX', '^TNX': '100.UST10Y',
    'DX-Y.NYB': '100.UDI',
    'CNY=X': '119.USDCNH', 'HKD=X': '119.USDHKD',
}

from network_resources import http_session as _shared_http_session
_DIRECT_SESSION = _shared_http_session(direct=True)

# ── Alpha Vantage 数据源（直连，无需代理，东财/雅虎被封时的主力替代）──────────
_AV_KEY = os.getenv("ALPHA_VANTAGE_KEY", "")
_AV_SYMBOL_MAP = {
    "SPY": "SPY", "QQQ": "QQQ", "TLT": "TLT", "GLD": "GLD",
    "^VIX": "VIX", "^TNX": "TNX", "DX-Y.NYB": "DXY",
    "NVDA": "NVDA", "AAPL": "AAPL", "TSLA": "TSLA", "MSFT": "MSFT",
    "ABBV": "ABBV", "LLY": "LLY", "TSM": "TSM", "PM": "PM",
    "NVO": "NVO", "ACMR": "ACMR", "GOOG": "GOOG", "VOO": "VOO",
    "QQQM": "QQQM",
}

def fetch_from_alphavantage(symbol: str, period: str = "1y") -> pd.DataFrame:
    """Alpha Vantage 直连数据源（不需代理），覆盖美股/ETF/指数"""
    if not _AV_KEY:
        return None
    av_sym = _AV_SYMBOL_MAP.get(symbol, symbol)
    # 指数符号处理
    if av_sym.startswith("^"):
        av_sym = av_sym[1:]
    # 只支持美股/ETF，A股和港股跳过
    if symbol.endswith(".SS") or symbol.endswith(".SZ") or symbol.endswith(".HK"):
        return None
    try:
        url = (f"https://www.alphavantage.co/query"
               f"?function=TIME_SERIES_DAILY_ADJUSTED&symbol={av_sym}"
               f"&outputsize=full&apikey={_AV_KEY}")
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return None
        data = r.json()
        ts = data.get("Time Series (Daily)")
        if not ts:
            return None
        rows = []
        for date_str, v in sorted(ts.items()):
            rows.append({
                "Date": pd.Timestamp(date_str),
                "Open":   float(v["1. open"]),
                "High":   float(v["2. high"]),
                "Low":    float(v["3. low"]),
                "Close":  float(v["5. adjusted close"]),
                "Volume": float(v["6. volume"]),
            })
        df = pd.DataFrame(rows).set_index("Date").sort_index()
        _safe_print(f"[AlphaVantage] ✅ {symbol}→{av_sym} {len(df)}行")
        return df
    except Exception as e:
        _safe_print(f"[AlphaVantage] ❌ {symbol}: {type(e).__name__}")
        return None
# 【修复东财被封】让东财请求走 Clash 代理
_PROXY_ADDR = "http://127.0.0.1:7897"
_DIRECT_SESSION.proxies = {"http": _PROXY_ADDR, "https": _PROXY_ADDR}

_EM_BASE = "https://push2his.eastmoney.com/api/qt/stock/kline/get"

def fetch_from_eastmoney_universal(symbol: str, period: str = '1y') -> pd.DataFrame:
    """
    东方财富万能数据源 - 使用 HTTP 直连（不走 HTTPS，彻底避开代理/SSL 拦截）
    trust_env=False 的 Session 彻底绕过所有代理环境变量
    """
    try:
        secid = _EM_SECID_KNOWN.get(symbol)

        if not secid:
            if symbol.endswith('.SS'):
                code = symbol.replace('.SS', '')
                secid = f"1.{code}"
            elif symbol.endswith('.SZ'):
                code = symbol.replace('.SZ', '')
                secid = f"0.{code}"
            elif symbol.endswith('.HK'):
                code = symbol.replace('.HK', '').zfill(5)
                secid = f"116.{code}"
            else:
                for mkt in ['105', '106', '107']:
                    _test_secid = f"{mkt}.{symbol}"
                    try:
                        _tr = _DIRECT_SESSION.get(
                            _EM_BASE,
                            params={'secid': _test_secid, 'fields1': 'f1,f3', 'fields2': 'f51,f52', 'klt': '101', 'fqt': '1', 'end': '20500101', 'lmt': '1'},
                            timeout=1.5)
                        if _tr.status_code == 200:
                            _td = _tr.json()
                            if _td.get('data') and _td['data'].get('klines'):
                                secid = _test_secid
                                break
                    except Exception:
                        continue
                if not secid:
                    return None

        is_index = secid.startswith('100.') or secid.startswith('124.')
        fqt_val = '0' if is_index else '1'
        lmt_map = {'6mo': '130', '1y': '252', '2y': '504', '3y': '756'}
        lmt = lmt_map.get(period, '252')

        r = _DIRECT_SESSION.get(
            _EM_BASE,
            params={
                'secid': secid, 'fields1': 'f1,f2,f3,f4,f5,f6',
                'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58',
                'klt': '101', 'fqt': fqt_val, 'end': '20500101', 'lmt': lmt,
            },
            timeout=(2.5, 4))  # (connect, read)：不可达时快速失败，配合熔断器

        if r.status_code != 200:
            return None
        data = r.json()
        if not data.get('data') or not data['data'].get('klines'):
            return None

        rows = []
        for line in data['data']['klines']:
            parts = line.split(',')
            if len(parts) >= 6:
                rows.append({
                    'Date': parts[0], 'Open': float(parts[1]), 'Close': float(parts[2]),
                    'High': float(parts[3]), 'Low': float(parts[4]), 'Volume': float(parts[5])
                })
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        _safe_print(f"[东财] ✅ {symbol} → {secid}  {len(df)} 行")
        return df
    except Exception as e:
        _safe_print(f"[东财] ❌ {symbol} 失败: {type(e).__name__}: {str(e)[:80]}")
        return None


def _macro_fetch_with_retry(dp, symbol, period='1y', data_type='fast',
                            force_refresh=False, attempts=3, fallback_period='6mo',
                            min_rows=2):
    """
    宏观资产（VIX/TNX/DXY/GLD/QQQ 等）抓取的健壮封装。
    单次请求易受东财/雅虎的突发限流影响 —— 实测中先发出的 SPY/TLT 能成功，
    而紧随其后的同批请求常被限流返回空，从而退化为「数据不可用」。
    本函数：失败时退避重试，并在尝试之间 sleep，自然拉开同批请求间隔以规避限流。
    """
    last = None
    for i in range(max(1, attempts)):
        try:
            _p = period if i == 0 else fallback_period
            last = dp.fetch_safe(symbol, period=_p, data_type=data_type,
                                 force_refresh=force_refresh)
        except Exception:
            last = None
        if last is not None and not last.empty and len(last) >= min_rows:
            return last
        try:
            time.sleep(0.6 * (i + 1))
        except Exception:
            pass
    return last

# ═══════════════════════════════════════════════════════════════

class DataProvider:
    """
    安全数据层 - 容错、缓存兜底、优雅降级
    目标：任何数据获取失败都不会让应用崩溃
    
    【V89 Phase 2】新增：
    - 集成分层缓存管理器
    - 支持force_refresh参数
    - 集成性能监控
    """
    def __init__(self, cache_manager: LayeredCacheManager = None, perf_monitor: PerformanceMonitor = None):
        self._memory_cache = {}  # 保留旧缓存（向后兼容）
        self.cache_mgr = cache_manager or _cache_manager
        self.perf = perf_monitor or _perf_monitor
        self.logger = logging.getLogger(__name__)
    
    def fetch_safe(self, symbol: str, period: str = '1y', data_type: str = 'daily', force_refresh: bool = False, min_rows: int = 20) -> pd.DataFrame:
        """
        安全获取股票数据，带容错和缓存兜底
        
        参数：
            symbol: 股票代码
            period: 数据周期（默认1年）
            data_type: 数据类型（fast/daily/weekly），决定缓存TTL
            force_refresh: 强制刷新，忽略缓存
            min_rows: 最少行数（默认20；补充指标仅需2行可传 min_rows=2）
        
        返回：
            DataFrame or None（失败时返回None，不抛异常）
        """
        cache_key = f"{symbol}_{period}"
        start_time = time.time()
        
        # 1. 检查分层缓存
        # 【2026-08-01 港股"恒生指数数据不足,无法分析"案·通用缺陷】
        # cache_key 只由 symbol+period 组成,**不含 min_rows**;而命中分支原本直接 return,
        # 不校验行数。后果:补充指标那几路用 min_rows=2 取 ^HSI 把一个很短的表存进 ^HSI_2y,
        # 体制分析用 min_rows=50 取同一个键时命中缓存拿到短表 → 误报"数据不足"。
        # 雅虎侧实测 ^HSI 周线105行完全正常,是缓存把宽松调用的结果喂给了严格调用。
        # 修法:命中也要过 min_rows;不够就当未命中重新拉。任何标的都可能中招,不止港股。
        def _cache_ok(_v):
            try:
                return _v is not None and len(_v) >= min_rows
            except TypeError:
                return _v is not None

        cached_value, is_stale = self.cache_mgr.get(cache_key, data_type, force_refresh)
        if cached_value is not None and not is_stale and _cache_ok(cached_value):
            elapsed = (time.time() - start_time) * 1000
            self.perf.record('fetch', elapsed)
            return cached_value
        if cached_value is not None and not _cache_ok(cached_value):
            self.logger.info(f"♻️ {symbol} 缓存仅 {len(cached_value)} 行 < min_rows={min_rows}，"
                             f"按未命中重新拉取（缓存键不含min_rows,宽松调用会污染严格调用）")

        # 1b. Rate limit 冷却期：直接返回过期缓存（有总比没有好）——但同样要够行数,
        # 否则冷却期内会持续误报"数据不足"而不是老实说"取不到"。
        if cached_value is not None and is_stale and _yf_is_rate_limited() and _cache_ok(cached_value):
            self.logger.info(f"⏭️ {symbol} rate limit 期间使用过期缓存")
            elapsed = (time.time() - start_time) * 1000
            self.perf.record('fetch', elapsed)
            return cached_value
        
        # 2-pre-0. Alpha Vantage 直连（东财/雅虎被封时的第一优先）
        if _AV_KEY and not (symbol.endswith('.SS') or symbol.endswith('.SZ') or symbol.endswith('.HK')):
            try:
                _av_df = fetch_from_alphavantage(symbol, period=period)
                if _av_df is not None and len(_av_df) >= min_rows:
                    self.cache_mgr.set(cache_key, _av_df, data_type)
                    return _av_df
            except Exception as _ave:
                pass

        # 2-pre. 东方财富万能源（带熔断：不可达时快速跳过，避免每个标的都卡满超时）
        if not _em_blocked():
            try:
                _em_df = fetch_from_eastmoney_universal(symbol, period=period)
                if _em_df is not None and len(_em_df) >= min_rows:
                    _em_mark(True)
                    self.cache_mgr.set(cache_key, _em_df, data_type)
                    elapsed = (time.time() - start_time) * 1000
                    self.perf.record('fetch', elapsed)
                    self.logger.info(f"✅ 东财万能源获取 {symbol}，共 {len(_em_df)} 条记录 ({elapsed:.0f}ms)")
                    return _em_df
                else:
                    _em_mark(False)
                    self.logger.warning(f"⚠️ 东财万能源 {symbol} 返回空或不足 min_rows={min_rows}")
            except Exception as _e:
                _em_mark(False)
                self.logger.warning(f"⚠️ 东财万能源 {symbol} 异常: {type(_e).__name__}: {str(_e)[:100]}")

        # 2a. A股：优先 免费行情源（同源核验缓存/腾讯日线，失败后尝试Yahoo）
        if (symbol.endswith(".SS") or symbol.endswith(".SZ")) and not _ts_blocked():
            try:
                from market_data_helper import fetch_df as _ts_fetch
                _ts_df = _ts_fetch(symbol, period=period)
                if _ts_df is not None and len(_ts_df) >= min_rows:
                    self.cache_mgr.set(cache_key, _ts_df, data_type)
                    elapsed = (time.time() - start_time) * 1000
                    self.perf.record('fetch', elapsed)
                    self.logger.info(f"✅ 免费行情源 获取 {symbol}，共 {len(_ts_df)} 条记录")
                    return _ts_df
            except Exception as _e:
                _msg = str(_e)
                self.logger.debug(f"免费行情源 {symbol} 失败，降级 yfinance: {_e}")

        # 2b. 尝试从 yfinance 获取（带重试 + rate limit 熔断）
        def _make_yf_session(attempt_idx: int):
            from network_resources import yahoo_session
            return yahoo_session()

        if _yf_is_rate_limited() or _yf_opserr_blocked():
            self.logger.warning(f"⏭️ {symbol} 跳过 yfinance（{'rate limit' if _yf_is_rate_limited() else 'OperationalError'} 冷却中），直接尝试备用源")
        else:
            for attempt in range(Config.RETRY_COUNT):
                try:
                    self.logger.info(f"📊 正在获取 {symbol} 数据... (尝试 {attempt+1}/{Config.RETRY_COUNT})")
                    _sess = _make_yf_session(attempt)
                    _yf_sym = _normalize_hk_for_yahoo(symbol)  # 港股 5 位→雅虎 4 位
                    ticker = yf.Ticker(_yf_sym, session=_sess) if _sess else yf.Ticker(_yf_sym)
                    try:
                        df = ticker.history(period=period, timeout=Config.REQUEST_TIMEOUT)
                    except TypeError:
                        df = ticker.history(period=period)
                    if df is not None and not df.empty and hasattr(df.columns, "levels") and df.columns.nlevels == 2:
                        df.columns = [c[0] for c in df.columns]
                    
                    if df is not None and not df.empty and len(df) >= min_rows:
                        self.cache_mgr.set(cache_key, df, data_type)
                        elapsed = (time.time() - start_time) * 1000
                        self.perf.record('fetch', elapsed)
                        self.logger.info(f"✅ 成功获取 {symbol} 数据，共 {len(df)} 条记录")
                        return df
                    else:
                        self.logger.warning(f"⚠️  {symbol} 数据为空或过少（{len(df) if df is not None else 0} 行），等待后重试...")
                        if attempt < Config.RETRY_COUNT - 1:
                            time.sleep(1 * (attempt + 1))
                
                except Exception as e:
                    _err_str = str(e)
                    self.logger.warning(f"⚠️  {symbol} 获取失败 (尝试 {attempt+1}): {_err_str[:120]}")
                    self.perf.error()
                    if _yf_check_operational_error(e):
                        self.logger.warning(f"⚠️  {symbol} OperationalError（SQLite锁），跳过 yfinance 走备用源")
                        break
                    if 'Rate' in _err_str or 'Too Many' in _err_str or 'RateLimit' in type(e).__name__:
                        _yf_mark_rate_limited()
                        break
                    if attempt < Config.RETRY_COUNT - 1:
                        time.sleep(1 * (attempt + 1))
        
        # 2c. A股：yfinance 失败时尝试东方财富备用（免费行情源 需 token，Cloud 环境常失败）
        if (symbol.endswith('.SS') or symbol.endswith('.SZ')) and USE_NEW_MODULES:
            try:
                _em_cn = mod_data.fetch_from_eastmoney(symbol)
                if _em_cn is not None and len(_em_cn) >= min_rows:
                    self.cache_mgr.set(cache_key, _em_cn, data_type)
                    elapsed = (time.time() - start_time) * 1000
                    self.perf.record('fetch', elapsed)
                    self.logger.info(f"✅ 东财A股备用获取 {symbol}，共 {len(_em_cn)} 条记录")
                    return _em_cn
            except Exception as _e:
                self.logger.debug(f"东财A股 {symbol} 失败: {_e}")
        
        # 2d. yfinance 失败时，美股/指数尝试 Stooq 备用（Streamlit Cloud 等环境 yfinance 常失败）
        if not (symbol.endswith('.HK') or symbol.endswith('.SS') or symbol.endswith('.SZ')):
            try:
                _stooq = mod_data.fetch_from_stooq(symbol) if USE_NEW_MODULES else fetch_from_stooq(symbol)
                if _stooq is not None and len(_stooq) >= min_rows:
                    self.cache_mgr.set(cache_key, _stooq, data_type)
                    elapsed = (time.time() - start_time) * 1000
                    self.perf.record('fetch', elapsed)
                    self.logger.info(f"✅ Stooq 备用获取 {symbol}，共 {len(_stooq)} 条记录")
                    return _stooq
            except Exception as _e:
                self.logger.debug(f"Stooq {symbol} 失败: {_e}")

        # 2d-2. Stooq 也失败时，直连 Yahoo Finance v8 JSON API（绕过 yfinance 封装层）
        if not (symbol.endswith('.HK') or symbol.endswith('.SS') or symbol.endswith('.SZ')):
            try:
                _yv8 = fetch_from_yahoo_direct(symbol, period=period)
                if _yv8 is not None and len(_yv8) >= min_rows:
                    self.cache_mgr.set(cache_key, _yv8, data_type)
                    elapsed = (time.time() - start_time) * 1000
                    self.perf.record('fetch', elapsed)
                    self.logger.info(f"✅ YahooV8直连备用获取 {symbol}，共 {len(_yv8)} 条记录")
                    return _yv8
            except Exception as _e:
                self.logger.debug(f"YahooV8 {symbol} 失败: {_e}")

        # 2e. 港股指数(^HSI/^HSTECH/^HSCE)尝试东方财富备用（yfinance 在 Cloud 常失败）
        if symbol in ('^HSI', '^HSTECH', '^HSCE') and USE_NEW_MODULES:
            try:
                _em = mod_data.fetch_hk_index_from_eastmoney(symbol)
                if _em is not None and len(_em) >= min_rows:
                    self.cache_mgr.set(cache_key, _em, data_type)
                    elapsed = (time.time() - start_time) * 1000
                    self.perf.record('fetch', elapsed)
                    self.logger.info(f"✅ 东财港股指数备用获取 {symbol}，共 {len(_em)} 条记录")
                    return _em
            except Exception as _e:
                self.logger.debug(f"东财港股指数 {symbol} 失败: {_e}")
        
        # 3. 最终兜底：东方财富万能源（熔断期间跳过，避免再次卡超时）
        if not _em_blocked():
            try:
                _em_last = fetch_from_eastmoney_universal(symbol, period=period)
                if _em_last is not None and len(_em_last) >= min_rows:
                    _em_mark(True)
                    self.cache_mgr.set(cache_key, _em_last, data_type)
                    elapsed = (time.time() - start_time) * 1000
                    self.perf.record('fetch', elapsed)
                    self.logger.info(f"✅ 东财兜底获取 {symbol}，共 {len(_em_last)} 条记录")
                    return _em_last
                else:
                    _em_mark(False)
            except Exception as _e:
                _em_mark(False)
                self.logger.debug(f"东财兜底 {symbol} 也失败: {_e}")

        # 4. 所有尝试失败，返回过期缓存（如果有）
        if cached_value is not None:
            self.logger.warning(f"⚠️  {symbol} 获取失败，使用过期缓存")
            elapsed = (time.time() - start_time) * 1000
            self.perf.record('fetch', elapsed)
            return cached_value
        
        # 5. 完全失败，返回None
        self.logger.error(f"❌ {symbol} 数据获取完全失败，无可用缓存")
        elapsed = (time.time() - start_time) * 1000
        self.perf.record('fetch', elapsed)
        return None
    
    def fetch_batch_concurrent(self, symbols: list, period: str = '1y', data_type: str = 'daily', force_refresh: bool = False) -> dict:
        """
        【V89 Phase 2】并发批量获取多个标的数据
        
        参数：
            symbols: 股票代码列表
            period: 数据周期
            data_type: 数据类型
            force_refresh: 强制刷新
        
        返回：
            {symbol: DataFrame or None}
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        results = {}
        errors = []
        
        def fetch_one(sym):
            try:
                df = self.fetch_safe(sym, period, data_type, force_refresh)
                return sym, df, None
            except Exception as e:
                return sym, None, str(e)
        
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_one, sym): sym for sym in symbols}
            
            for future in as_completed(futures, timeout=Config.TASK_TIMEOUT * len(symbols)):
                try:
                    sym, df, error = future.result(timeout=Config.TASK_TIMEOUT)
                    results[sym] = df
                    if error:
                        errors.append(f"{sym}: {error}")
                        self.perf.error()
                except Exception as e:
                    sym = futures[future]
                    results[sym] = None
                    errors.append(f"{sym}: 任务超时或异常")
                    self.perf.error()
        
        if errors:
            self.logger.warning(f"⚠️  批量获取部分失败: {'; '.join(errors[:5])}")
        
        return results


class ExpectationLayer:
    """
    宏观预期层 - 基于SPY/TLT/VIX判断市场体制
    目标：Risk On / Risk Off / Neutral 三态裁决
    
    【V89 Phase 2】新增：
    - 支持force_refresh参数
    - 集成性能监控
    - 参数签名机制（增量刷新）
    
    【V89.1 优化】新增：
    - 支持多市场分析（美股/港股/A股）
    - 每个市场独立裁决
    - 综合市场联动分析
    """
    def __init__(self, data_provider: DataProvider, perf_monitor: PerformanceMonitor = None):
        self.dp = data_provider
        self.perf = perf_monitor or _perf_monitor
        self.logger = logging.getLogger(__name__)
        self._last_param_hash = None
        self._last_result = None
        self._last_multi_result = None  # 多市场结果缓存
    
    def _compute_param_hash(self) -> str:
        """计算参数签名（用于增量刷新）"""
        import hashlib
        # v2: 加入 QQQ 纳斯达克，版本号变更强制旧缓存失效
        params = f"v2_{Config.MA_SHORT}_{Config.MA_LONG}_{Config.CORR_WINDOW}_{Config.VIX_PANIC}_{Config.VIX_HIGH}"
        return hashlib.md5(params.encode()).hexdigest()[:8]
    
    def analyze_market_regime(self, force_refresh: bool = False) -> dict:
        """
        分析当前市场体制
        
        【V89 Phase 2】新增参数：
            force_refresh: 强制刷新，忽略缓存和参数签名
        
        返回字典：
            verdict: 'Risk On' / 'Risk Off' / 'Neutral'
            vix_level: VIX数值
            vix_status: VIX状态描述
            correlation: SPY与TLT的相关性
            spy_price: SPY最新价格
            ma50: SPY的50日均线
            ma200: SPY的200日均线
            reason: 裁决理由（中文）
            data_ok: 数据是否完整
        """
        start_time = time.time()
        
        try:
            # 【V89 Phase 2】增量刷新：检查参数签名
            current_hash = self._compute_param_hash()
            if not force_refresh and current_hash == self._last_param_hash and self._last_result is not None:
                self.logger.info(f"✅ 参数未变化，使用缓存结果（签名: {current_hash}）")
                elapsed = (time.time() - start_time) * 1000
                self.perf.record('compute', elapsed)
                return self._last_result
            
            # 1. 并发获取全部宏观标的（7 个同时发出，总耗时 ≈ 最慢那一个，而非累加）
            self.logger.info("🔍 开始分析宏观市场体制（并发模式）...")
            from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed
            def _fs(sym, period, dtype, min_rows=2):
                return self.dp.fetch_safe(sym, period=period, data_type=dtype,
                                          force_refresh=force_refresh, min_rows=min_rows)
            _macro_tasks = {
                'spy':  ('SPY',       '2y',                  'weekly', 50),
                'tlt':  ('TLT',       '2y',                  'weekly', 20),
                'vix':  ('^VIX',      Config.MACRO_PERIOD,   'fast',    2),
                'tnx':  ('^TNX',      '6mo',                 'fast',    2),
                'dxy':  ('DX-Y.NYB',  '6mo',                 'fast',    2),
                'gld':  ('GLD',       Config.MACRO_PERIOD,   'weekly',  2),
                'qqq':  ('QQQ',       Config.MACRO_PERIOD,   'weekly',  2),
            }
            _macro_results = {}
            with ThreadPoolExecutor(max_workers=7) as _pool:
                _futs = {_pool.submit(_fs, sym, period, dtype, minr): key
                         for key, (sym, period, dtype, minr) in _macro_tasks.items()}
                for fut in _as_completed(_futs):
                    k = _futs[fut]
                    try:
                        _macro_results[k] = fut.result()
                    except Exception:
                        _macro_results[k] = None

            spy_df = _macro_results.get('spy')
            tlt_df = _macro_results.get('tlt')
            vix_df = _macro_results.get('vix')

            # 降级：SPY/TLT 2y 不足时用 1y 重取（已有缓存，基本瞬间）
            if spy_df is None or len(spy_df) < 50:
                spy_df = self.dp.fetch_safe('SPY', period='1y', data_type='weekly', force_refresh=force_refresh)
            if tlt_df is None or len(tlt_df) < 20:
                tlt_df = self.dp.fetch_safe('TLT', period='1y', data_type='weekly', force_refresh=force_refresh)
            
            # 2. 检查数据完整性（放宽：>50 行即可用 MA50 替代 MA200）
            if spy_df is None or len(spy_df) < 50:
                return self._fallback_result("SPY数据不足，无法分析", 'us')
            
            if vix_df is None or vix_df.empty:
                vix_df = _macro_fetch_with_retry(self.dp, '^VIX', period='6mo', data_type='fast', force_refresh=force_refresh)
            
            # 3. 计算技术指标（MA200 不足时用 MA50 代替）
            spy_price = float(spy_df['Close'].iloc[-1])
            spy_df['MA50']  = spy_df['Close'].rolling(window=min(50,  len(spy_df))).mean()
            spy_df['MA200'] = spy_df['Close'].rolling(window=min(200, len(spy_df))).mean()
            ma50  = float(spy_df['MA50'].iloc[-1])
            # MA200 不足时用 MA50 兜底（并在 reason 中说明）
            _ma200_ok = len(spy_df) >= 200
            ma200 = float(spy_df['MA200'].iloc[-1]) if _ma200_ok else ma50
            
            # 4. 计算SPY与TLT的相关性
            correlation = 0.0
            corr_desc = "数据不足"
            if tlt_df is not None and len(tlt_df) >= Config.CORR_WINDOW:
                # 对齐日期
                common_dates = spy_df.index.intersection(tlt_df.index)
                if len(common_dates) >= Config.CORR_WINDOW:
                    spy_aligned = spy_df.loc[common_dates, 'Close']
                    tlt_aligned = tlt_df.loc[common_dates, 'Close']
                    
                    # 计算滚动相关性并取最新值
                    rolling_corr = spy_aligned.rolling(window=Config.CORR_WINDOW).corr(tlt_aligned)
                    correlation = float(rolling_corr.iloc[-1]) if not np.isnan(rolling_corr.iloc[-1]) else 0.0
                    
                    # 相关性解读
                    if correlation > 0.3:
                        corr_desc = "股债同向（宏观冲击主导）"
                    elif correlation < -0.3:
                        corr_desc = "股债跷跷板（避险切换明显）"
                    else:
                        corr_desc = "相关性弱（风格轮动为主）"
            
            # 5. VIX分析（vix_df 两次获取均可能失败，需 None 保护）
            if vix_df is None or vix_df.empty:
                vix_level = 20.0
                vix_change_pct = 0.0
                vix_status = "数据不可用"
                self.logger.warning("⚠️ VIX 数据两次获取均失败，使用默认值 20")
            else:
                vix_level = float(vix_df['Close'].iloc[-1])
                vix_prev = float(vix_df['Close'].iloc[-2]) if len(vix_df) >= 2 else vix_level
                vix_change_pct = ((vix_level - vix_prev) / vix_prev * 100) if vix_prev != 0 else 0
                if vix_level > Config.VIX_PANIC:
                    vix_status = "⚠️ 极度恐慌（现金为王）"
                elif vix_level > Config.VIX_HIGH:
                    vix_status = "📈 高波动（需对冲）"
                elif vix_level < Config.VIX_LOW:
                    vix_status = "📉 低波动（趋势延续）"
                else:
                    vix_status = "📊 中等波动（均衡应对）"
            
            # 5.1–5.5 直接使用并发结果（已在步骤1一次性并发抓完，无需重复抓）
            # ── 10Y 美债 ──
            tnx_yield = 0.0; tnx_change = 0.0; tnx_status = "数据不可用"
            tnx_df = _macro_results.get('tnx')
            if tnx_df is not None and not tnx_df.empty:
                tnx_yield = float(tnx_df['Close'].iloc[-1])
                tnx_prev = float(tnx_df['Close'].iloc[-2]) if len(tnx_df) >= 2 else tnx_yield
                tnx_change = tnx_yield - tnx_prev
                tnx_status = ("🟢 宽松（利好成长股）" if tnx_yield < Config.TNX_LOOSE
                              else "🔴 偏紧缩（利空高估值）" if tnx_yield > Config.TNX_TIGHT
                              else "🟡 中性区间")

            # ── 美元指数 ──
            dxy_level = 0.0; dxy_change_pct = 0.0; dxy_status = "数据不可用"
            dxy_df = _macro_results.get('dxy')
            if dxy_df is not None and not dxy_df.empty:
                dxy_level = float(dxy_df['Close'].iloc[-1])
                dxy_prev = float(dxy_df['Close'].iloc[-2]) if len(dxy_df) >= 2 else dxy_level
                dxy_change_pct = ((dxy_level - dxy_prev) / dxy_prev * 100) if dxy_prev != 0 else 0
                dxy_status = ("🟢 弱美元（利好新兴/大宗）" if dxy_level < Config.DXY_WEAK
                              else "🔴 强美元（资金回流美国）" if dxy_level > Config.DXY_STRONG
                              else "🟡 中性区间")

            # ── 黄金 ──
            gld_price = 0.0; gld_change_pct = 0.0; gld_status = "数据不可用"
            gld_df = _macro_results.get('gld')
            if gld_df is not None and not gld_df.empty:
                gld_price = float(gld_df['Close'].iloc[-1])
                gld_prev = float(gld_df['Close'].iloc[-2]) if len(gld_df) >= 2 else gld_price
                gld_change_pct = ((gld_price - gld_prev) / gld_prev * 100) if gld_prev != 0 else 0
                gld_status = ("📈 避险需求上升" if gld_change_pct > 1.0
                              else "📉 风险偏好回暖" if gld_change_pct < -1.0
                              else "📊 持平")

            # ── SPY/TLT 涨跌 ──
            spy_prev = float(spy_df['Close'].iloc[-2]) if len(spy_df) >= 2 else spy_price
            spy_change_pct = ((spy_price - spy_prev) / spy_prev * 100) if spy_prev != 0 else 0
            tlt_price = 0.0; tlt_change_pct = 0.0
            if tlt_df is not None and not tlt_df.empty:
                tlt_price = float(tlt_df['Close'].iloc[-1])
                tlt_prev = float(tlt_df['Close'].iloc[-2]) if len(tlt_df) >= 2 else tlt_price
                tlt_change_pct = ((tlt_price - tlt_prev) / tlt_prev * 100) if tlt_prev != 0 else 0

            # ── QQQ ──
            qqq_price = 0.0; qqq_change_pct = 0.0
            qqq_df = _macro_results.get('qqq')
            if qqq_df is not None and not qqq_df.empty:
                qqq_price = float(qqq_df['Close'].iloc[-1])
                qqq_prev = float(qqq_df['Close'].iloc[-2]) if len(qqq_df) >= 2 else qqq_price
                qqq_change_pct = ((qqq_price - qqq_prev) / qqq_prev * 100) if qqq_prev != 0 else 0
            
            # 6. 市场体制裁决（【V90】增强：加入美债+美元因素）
            verdict = "Neutral"
            reason_parts = []
            
            # Risk Off条件
            if vix_level > 25:
                verdict = "Risk Off"
                reason_parts.append(f"VIX={vix_level:.1f}>25（恐慌）")
            elif spy_price < ma200:
                verdict = "Risk Off"
                reason_parts.append(f"SPY({spy_price:.1f}) < MA200({ma200:.1f})")
            
            # Risk On条件
            elif spy_price > ma50 and vix_level < Config.VIX_HIGH:
                verdict = "Risk On"
                reason_parts.append(f"SPY({spy_price:.1f}) > MA50({ma50:.1f})")
                reason_parts.append(f"VIX={vix_level:.1f}<20（低波动）")
            
            # Neutral
            else:
                reason_parts.append(f"SPY在MA50({ma50:.1f})与MA200({ma200:.1f})之间")
                reason_parts.append(f"VIX={vix_level:.1f}（中性）")
            
            # 【V90】美债紧缩警告叠加
            if tnx_yield > Config.TNX_TIGHT:
                reason_parts.append(f"⚠️ 10Y美债{tnx_yield:.2f}%偏高，流动性紧缩")
            if dxy_level > Config.DXY_STRONG:
                reason_parts.append(f"⚠️ 美元指数{dxy_level:.1f}偏强，资金回流美国")
            
            # 【V90】仓位上限建议（基于宏观综合）
            position_cap = 80  # 默认80%
            if verdict == "Risk Off":
                position_cap = 30
            elif verdict == "Neutral":
                position_cap = 60
            if tnx_yield > Config.TNX_TIGHT and position_cap > 60:
                position_cap = 60  # 紧缩环境下降仓位上限
            
            reason = "；".join(reason_parts)
            
            self.logger.info(f"✅ 市场体制分析完成: {verdict} - {reason}")
            
            # 【V89 Phase 2】缓存结果和参数签名
            result = {
                'verdict': verdict,
                'vix_level': vix_level,
                'vix_change_pct': vix_change_pct,
                'vix_status': vix_status,
                'correlation': correlation,
                'corr_desc': corr_desc,
                'spy_price': spy_price,
                'spy_change_pct': spy_change_pct,
                'ma50': ma50,
                'ma200': ma200,
                'qqq_price': qqq_price,
                'qqq_change_pct': qqq_change_pct,
                'tlt_price': tlt_price,
                'tlt_change_pct': tlt_change_pct,
                'tnx_yield': tnx_yield,
                'tnx_change': tnx_change,
                'tnx_status': tnx_status,
                'dxy_level': dxy_level,
                'dxy_change_pct': dxy_change_pct,
                'dxy_status': dxy_status,
                'gld_price': gld_price,
                'gld_change_pct': gld_change_pct,
                'gld_status': gld_status,
                'position_cap': position_cap,
                'reason': reason,
                'data_ok': True
            }
            
            self._last_result = result
            self._last_param_hash = current_hash
            
            elapsed = (time.time() - start_time) * 1000
            self.perf.record('compute', elapsed)
            
            return result
        
        except Exception as e:
            self.logger.error(f"❌ 美股宏观分析异常: {str(e)}")
            import traceback
            traceback.print_exc()
            return self._fallback_result(f"美股分析异常: {str(e)[:50]}", 'us')
    
    def _fallback_result(self, reason: str, market_type: str = 'us') -> dict:
        """
        降级结果 - 数据不足时返回
        
        【V89.6.7 修复】支持不同市场类型
        market_type: 'us' (美股) / 'hk' (港股) / 'cn' (A股)
        """
        if market_type == 'us':
            # 美股降级数据
            return {
                'verdict': 'Unknown',
                'vix_level': 0.0,
                'vix_change_pct': 0.0,
                'vix_status': '数据不可用',
                'correlation': 0.0,
                'corr_desc': '数据不可用',
                'spy_price': 0.0,
                'spy_change_pct': 0.0,
                'ma50': 0.0,
                'ma200': 0.0,
                'qqq_price': 0.0,
                'qqq_change_pct': 0.0,
                'tlt_price': 0.0,
                'tlt_change_pct': 0.0,
                'tnx_yield': 0.0,
                'tnx_change': 0.0,
                'tnx_status': '数据不可用',
                'dxy_level': 0.0,
                'dxy_change_pct': 0.0,
                'dxy_status': '数据不可用',
                'gld_price': 0.0,
                'gld_change_pct': 0.0,
                'gld_status': '数据不可用',
                'position_cap': 50,
                'reason': reason,
                'data_ok': False,
                'market_name': '美股'
            }
        elif market_type == 'hk':
            # 港股降级数据
            return {
                'verdict': 'Unknown',
                'index_level': 0.0,
                'index_change_pct': 0.0,
                'volatility': 0.0,
                'vol_status': '数据不可用',
                'ma50': 0.0,
                'ma200': 0.0,
                'reason': reason,
                'data_ok': False,
                'market_name': '港股',
                'hstech_price': 0.0, 'hstech_change_pct': 0.0, 'hstech_use_etf': False,
                'hsce_price': 0.0, 'hsce_change_pct': 0.0,
                'hkd_price': 0.0, 'hkd_change_pct': 0.0,
            }
        else:  # 'cn' - A股
            # A股降级数据
            return {
                'verdict': 'Unknown',
                'index_level': 0.0,
                'index_change_pct': 0.0,
                'volatility': 0.0,
                'vol_status': '数据不可用',
                'ma50': 0.0,
                'ma200': 0.0,
                'reason': reason,
                'data_ok': False,
                'market_name': 'A股',
                'hs300_price': 0.0, 'hs300_change_pct': 0.0,
                'cyb_price': 0.0, 'cyb_change_pct': 0.0,
                'cny_price': 0.0, 'cny_change_pct': 0.0,
            }
    
    def analyze_hk_market_regime(self, force_refresh: bool = False) -> dict:
        """
        【V89.1 新增】分析港股市场体制（基于恒生指数）
        
        返回字典：类似美股，但基于^HSI
        """
        start_time = time.time()
        
        try:
            self.logger.info("🔍 开始分析港股市场体制...")
            
            # 获取恒生指数数据（优先2y以确保足够MA200行数，失败时降级1y）
            hsi_df = None
            for _hsi_period in ['2y', '1y', '6mo']:
                hsi_df = self.dp.fetch_safe('^HSI', period=_hsi_period, data_type='weekly', force_refresh=force_refresh, min_rows=50)
                if hsi_df is not None and len(hsi_df) >= 50:
                    break

            if hsi_df is None or len(hsi_df) < 50:
                return self._fallback_result("恒生指数数据不足，无法分析", 'hk')
            
            # 计算技术指标（MA200不足时用MA50代替）
            hsi_price = float(hsi_df['Close'].iloc[-1])
            hsi_df['MA50'] = hsi_df['Close'].rolling(window=Config.MA_SHORT).mean()
            hsi_df['MA200'] = hsi_df['Close'].rolling(window=Config.MA_LONG).mean()
            ma50 = float(hsi_df['MA50'].iloc[-1])
            _ma200_raw = hsi_df['MA200'].iloc[-1]
            ma200 = float(_ma200_raw) if (_ma200_raw == _ma200_raw and _ma200_raw > 0) else ma50  # NaN时用MA50兜底
            
            # 计算波动率（替代VIX）
            returns = hsi_df['Close'].pct_change().dropna()
            volatility = returns.rolling(window=20).std().iloc[-1] * np.sqrt(252) * 100
            
            # 波动率分级（港股特色）
            if volatility > 35:
                vol_status = "⚠️ 高波动（谨慎）"
            elif volatility > 25:
                vol_status = "📈 中高波动（正常）"
            elif volatility < 15:
                vol_status = "📉 低波动（平稳）"
            else:
                vol_status = "📊 中等波动（均衡）"
            
            # 市场体制裁决
            verdict = "Neutral"
            reason_parts = []
            
            if hsi_price < ma200:
                verdict = "Risk Off"
                reason_parts.append(f"恒指({hsi_price:.0f}) < MA200({ma200:.0f})")
            elif hsi_price > ma50 and volatility < 25:
                verdict = "Risk On"
                reason_parts.append(f"恒指({hsi_price:.0f}) > MA50({ma50:.0f})")
                reason_parts.append(f"波动率={volatility:.1f}%（温和）")
            else:
                reason_parts.append(f"恒指在MA50({ma50:.0f})与MA200({ma200:.0f})之间")
            
            reason = "；".join(reason_parts)
            
            # 日涨跌（用于宏观脉搏展示）
            hsi_prev = float(hsi_df['Close'].iloc[-2]) if len(hsi_df) >= 2 else hsi_price
            hsi_change_pct = ((hsi_price - hsi_prev) / hsi_prev * 100) if hsi_prev != 0 else 0
            
            # 【V91.1】恒生科技/国企指数/港币：^HSTECH 已从 Yahoo Finance 下架，直接用 3033.HK ETF
            hstech_price, hstech_chg, hstech_use_etf = 0.0, 0.0, False
            hsce_price, hsce_chg = 0.0, 0.0
            hkd_price, hkd_chg = 0.0, 0.0
            for _sym in ['3033.HK']:
                hstech_df = self.dp.fetch_safe(_sym, period='6mo', data_type='daily', force_refresh=force_refresh, min_rows=2)
                if hstech_df is not None and len(hstech_df) >= 2:
                    hstech_price = float(hstech_df['Close'].iloc[-1])
                    hstech_prev = float(hstech_df['Close'].iloc[-2])
                    hstech_chg = ((hstech_price - hstech_prev) / hstech_prev * 100) if hstech_prev != 0 else 0
                    hstech_use_etf = (_sym == '3033.HK')
                    break
            try:
                hsce_df = self.dp.fetch_safe('^HSCE', period='6mo', data_type='daily', force_refresh=force_refresh, min_rows=2)
                if hsce_df is not None and len(hsce_df) >= 2:
                    hsce_price = float(hsce_df['Close'].iloc[-1])
                    hsce_prev = float(hsce_df['Close'].iloc[-2])
                    hsce_chg = ((hsce_price - hsce_prev) / hsce_prev * 100) if hsce_prev != 0 else 0
            except Exception:
                pass
            try:
                hkd_df = self.dp.fetch_safe('HKD=X', period='6mo', data_type='fast', force_refresh=force_refresh, min_rows=2)
                if hkd_df is not None and len(hkd_df) >= 2:
                    hkd_price = float(hkd_df['Close'].iloc[-1])
                    hkd_prev = float(hkd_df['Close'].iloc[-2])
                    hkd_chg = ((hkd_price - hkd_prev) / hkd_prev * 100) if hkd_prev != 0 else 0
            except Exception:
                pass
            
            result = {
                'verdict': verdict,
                'index_level': hsi_price,
                'index_change_pct': hsi_change_pct,
                'volatility': volatility,
                'vol_status': vol_status,
                'ma50': ma50,
                'ma200': ma200,
                'reason': reason,
                'data_ok': True,
                'market_name': '港股',
                'hstech_price': hstech_price,
                'hstech_change_pct': hstech_chg,
                'hstech_use_etf': hstech_use_etf,
                'hsce_price': hsce_price,
                'hsce_change_pct': hsce_chg,
                'hkd_price': hkd_price,
                'hkd_change_pct': hkd_chg,
            }
            
            elapsed = (time.time() - start_time) * 1000
            self.perf.record('compute', elapsed)
            
            self.logger.info(f"✅ 港股市场体制分析完成: {verdict} | 恒指={hsi_price:.0f} | MA50={ma50:.0f} | MA200={ma200:.0f} | 波动率={volatility:.1f}%")
            return result
        
        except Exception as e:
            self.logger.error(f"❌ 港股市场分析异常: {str(e)}")
            return self._fallback_result(f"港股分析异常: {str(e)[:50]}", 'hk')
    
    def analyze_cn_market_regime(self, force_refresh: bool = False) -> dict:
        """
        【V89.1 新增】分析A股市场体制（基于上证指数）
        
        返回字典：类似美股，但基于000001.SS
        """
        start_time = time.time()
        
        try:
            self.logger.info("🔍 开始分析A股市场体制...")
            
            # 获取上证指数数据（优先2y确保MA200数据充足，失败时降级）
            sse_df = None
            for _sse_period in ['2y', '1y', '6mo']:
                sse_df = self.dp.fetch_safe('000001.SS', period=_sse_period, data_type='weekly', force_refresh=force_refresh, min_rows=50)
                if sse_df is not None and len(sse_df) >= 50:
                    break

            if sse_df is None or len(sse_df) < 50:
                return self._fallback_result("上证指数数据不足，无法分析", 'cn')
            
            # 计算技术指标（MA200不足时用MA50代替）
            sse_price = float(sse_df['Close'].iloc[-1])
            sse_df['MA50'] = sse_df['Close'].rolling(window=Config.MA_SHORT).mean()
            sse_df['MA200'] = sse_df['Close'].rolling(window=Config.MA_LONG).mean()
            ma50 = float(sse_df['MA50'].iloc[-1])
            _ma200_raw = sse_df['MA200'].iloc[-1]
            ma200 = float(_ma200_raw) if (_ma200_raw == _ma200_raw and _ma200_raw > 0) else ma50
            
            # 计算波动率
            returns = sse_df['Close'].pct_change().dropna()
            volatility = returns.rolling(window=20).std().iloc[-1] * np.sqrt(252) * 100
            
            # 波动率分级（A股特色）
            if volatility > 40:
                vol_status = "⚠️ 高波动（政策敏感期）"
            elif volatility > 30:
                vol_status = "📈 中高波动（活跃）"
            elif volatility < 20:
                vol_status = "📉 低波动（盘整）"
            else:
                vol_status = "📊 中等波动（正常）"
            
            # 市场体制裁决
            verdict = "Neutral"
            reason_parts = []
            
            if sse_price < ma200:
                verdict = "Risk Off"
                reason_parts.append(f"上证({sse_price:.0f}) < MA200({ma200:.0f})")
            elif sse_price > ma50 and volatility < 30:
                verdict = "Risk On"
                reason_parts.append(f"上证({sse_price:.0f}) > MA50({ma50:.0f})")
                reason_parts.append(f"波动率={volatility:.1f}%（温和）")
            else:
                reason_parts.append(f"上证在MA50({ma50:.0f})与MA200({ma200:.0f})之间")
            
            reason = "；".join(reason_parts)
            
            # 日涨跌（用于宏观脉搏展示）
            sse_prev = float(sse_df['Close'].iloc[-2]) if len(sse_df) >= 2 else sse_price
            sse_change_pct = ((sse_price - sse_prev) / sse_prev * 100) if sse_prev != 0 else 0
            
            # 【V91.1】补充指标：沪深300、创业板指、人民币汇率（6mo+min_rows=2 确保能取到）
            hs300_price, hs300_chg = 0.0, 0.0
            cyb_price, cyb_chg = 0.0, 0.0
            cny_price, cny_chg = 0.0, 0.0
            try:
                hs300_df = self.dp.fetch_safe('000300.SS', period='6mo', data_type='daily', force_refresh=force_refresh, min_rows=2)
                if hs300_df is not None and len(hs300_df) >= 2:
                    hs300_price = float(hs300_df['Close'].iloc[-1])
                    hs300_prev = float(hs300_df['Close'].iloc[-2])
                    hs300_chg = ((hs300_price - hs300_prev) / hs300_prev * 100) if hs300_prev != 0 else 0
            except Exception:
                pass
            try:
                cyb_df = self.dp.fetch_safe('399006.SZ', period='6mo', data_type='daily', force_refresh=force_refresh, min_rows=2)
                # 【V91.7】Yahoo 对 399006 不稳定，优先东方财富专用接口（fqt=0），再主 fetch
                if cyb_df is None or len(cyb_df) < 2:
                    try:
                        _cyb_em = fetch_cyb_from_eastmoney()
                        if _cyb_em is not None and len(_cyb_em) >= 2:
                            cyb_df = _cyb_em
                    except Exception:
                        pass
                if cyb_df is None or len(cyb_df) < 2:
                    try:
                        _cyb_from_fetch = fetch_stock_data('399006.SZ')
                        if _cyb_from_fetch is not None and len(_cyb_from_fetch) >= 2:
                            cyb_df = _cyb_from_fetch
                    except Exception:
                        pass
                if cyb_df is not None and len(cyb_df) >= 2:
                    cyb_price = float(cyb_df['Close'].iloc[-1])
                    cyb_prev = float(cyb_df['Close'].iloc[-2])
                    cyb_chg = ((cyb_price - cyb_prev) / cyb_prev * 100) if cyb_prev != 0 else 0
            except Exception:
                pass
            try:
                cny_df = self.dp.fetch_safe('CNY=X', period='6mo', data_type='fast', force_refresh=force_refresh, min_rows=2)
                if cny_df is not None and len(cny_df) >= 2:
                    cny_price = float(cny_df['Close'].iloc[-1])
                    cny_prev = float(cny_df['Close'].iloc[-2])
                    cny_chg = ((cny_price - cny_prev) / cny_prev * 100) if cny_prev != 0 else 0
            except Exception:
                pass
            
            result = {
                'verdict': verdict,
                'index_level': sse_price,
                'index_change_pct': sse_change_pct,
                'volatility': volatility,
                'vol_status': vol_status,
                'ma50': ma50,
                'ma200': ma200,
                'reason': reason,
                'data_ok': True,
                'market_name': 'A股',
                'hs300_price': hs300_price,
                'hs300_change_pct': hs300_chg,
                'cyb_price': cyb_price,
                'cyb_change_pct': cyb_chg,
                'cny_price': cny_price,
                'cny_change_pct': cny_chg,
            }
            
            elapsed = (time.time() - start_time) * 1000
            self.perf.record('compute', elapsed)
            
            self.logger.info(f"✅ A股市场体制分析完成: {verdict} | 上证={sse_price:.0f} | MA50={ma50:.0f} | MA200={ma200:.0f} | 波动率={volatility:.1f}%")
            return result
        
        except Exception as e:
            self.logger.error(f"❌ A股市场分析异常: {str(e)}")
            return self._fallback_result(f"A股分析异常: {str(e)[:50]}", 'cn')
    
    def analyze_all_markets(self, force_refresh: bool = False) -> dict:
        """
        【V89.1 新增】分析所有市场（美股/港股/A股）+ 综合联动
        
        返回字典：
        {
            'us_market': {...},
            'hk_market': {...},
            'cn_market': {...},
            'summary': {...}  # 综合分析
        }
        """
        try:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            _market_fns = {
                'us': lambda: self.analyze_market_regime(force_refresh),
                'hk': lambda: self.analyze_hk_market_regime(force_refresh),
                'cn': lambda: self.analyze_cn_market_regime(force_refresh),
            }
            _market_results = {}
            with ThreadPoolExecutor(max_workers=3) as _pool:
                _futs = {_pool.submit(fn): key for key, fn in _market_fns.items()}
                try:
                    for _f in as_completed(_futs, timeout=45):
                        _k = _futs[_f]
                        try:
                            _market_results[_k] = _f.result(timeout=45)
                        except Exception as _fe:
                            self.logger.error(f"❌ {_k} 市场分析线程异常: {_fe}")
                            _market_results[_k] = self._fallback_result(f"{_k}分析异常", _k)
                except TimeoutError:
                    self.logger.warning("⚠️ 部分市场分析超时，使用已完成的结果")
                    for _f, _k in _futs.items():
                        if _k not in _market_results:
                            _market_results[_k] = self._fallback_result(f"{_k}分析超时", _k)
            us_result = _market_results.get('us', self._fallback_result("美股分析超时", 'us'))
            hk_result = _market_results.get('hk', self._fallback_result("港股分析超时", 'hk'))
            cn_result = _market_results.get('cn', self._fallback_result("A股分析超时", 'cn'))
            
            # 综合分析
            risk_on_count = sum(1 for r in [us_result, hk_result, cn_result] 
                               if r['data_ok'] and r['verdict'] == 'Risk On')
            risk_off_count = sum(1 for r in [us_result, hk_result, cn_result] 
                                if r['data_ok'] and r['verdict'] == 'Risk Off')
            
            valid_markets = sum(1 for r in [us_result, hk_result, cn_result] if r['data_ok'])
            
            if valid_markets == 0:
                global_verdict = "数据不足"
                global_reason = "所有市场数据均不可用"
            elif risk_on_count >= 2:
                global_verdict = "🟢 全球风险偏好"
                global_reason = f"三大市场中{risk_on_count}个处于Risk On状态"
            elif risk_off_count >= 2:
                global_verdict = "🔴 全球避险模式"
                global_reason = f"三大市场中{risk_off_count}个处于Risk Off状态"
            else:
                global_verdict = "🟡 市场分化"
                global_reason = "各市场体制不一致，结构性行情为主"
            
            summary = {
                'global_verdict': global_verdict,
                'global_reason': global_reason,
                'risk_on_count': risk_on_count,
                'risk_off_count': risk_off_count,
                'valid_markets': valid_markets
            }
            
            result = {
                'us_market': us_result,
                'hk_market': hk_result,
                'cn_market': cn_result,
                'summary': summary
            }
            
            self._last_multi_result = result
            return result
        
        except Exception as e:
            self.logger.error(f"❌ 全市场分析异常: {str(e)}")
            return {
                'us_market': self._fallback_result("美股分析失败", 'us'),
                'hk_market': self._fallback_result("港股分析失败", 'hk'),
                'cn_market': self._fallback_result("A股分析失败", 'cn'),
                'summary': {
                    'global_verdict': "数据不足",
                    'global_reason': "分析异常",
                    'risk_on_count': 0,
                    'risk_off_count': 0,
                    'valid_markets': 0
                }
            }


# 初始化全局实例 —— 同样持久化，确保缓存在 rerun 间有效
@st.cache_resource
def _get_data_provider():
    return DataProvider(_get_cache_manager(), _get_perf_monitor())

@st.cache_resource
def _get_expectation_layer():
    # v2: 新增 QQQ 纳斯达克100 指标 —— 此注释变更强制 cache_resource 创建新实例
    return ExpectationLayer(_get_data_provider(), _get_perf_monitor())

_data_provider = _get_data_provider()
_expectation_layer = _get_expectation_layer()


@st.cache_data(ttl=3600, show_spinner=False)
def _cached_expectation_all_markets(_ts: int | None = None):
    """模块级缓存：全球市场宏观分析（避免嵌套函数导致缓存不稳定）"""
    return _expectation_layer.analyze_all_markets(force_refresh=False)


# 【V89.2】初始化机构研究中心
if INSTITUTIONAL_RESEARCH_AVAILABLE:
    _institutional_research = InstitutionalResearch(_data_provider, _perf_monitor)
    logging.info("✅ V89.2 机构研究中心初始化完成")
else:
    _institutional_research = None

# 【V89.3】初始化持仓管理器
if PORTFOLIO_MANAGER_AVAILABLE and Config.PORTFOLIO_ENABLED:
    _portfolio_manager = PortfolioManager(Config.PORTFOLIO_FILE)
    logging.info(f"✅ V89.3 持仓管理器初始化完成: {Config.PORTFOLIO_FILE}")
else:
    _portfolio_manager = None

# 【V89.4】初始化舆情分析中心
if SENTIMENT_ANALYZER_AVAILABLE:
    # call_model_api函数在后面定义，这里先设为None，后续再绑定
    _sentiment_analyzer = SentimentAnalyzer(gemini_api_caller=None)
    logging.info("✅ V89.4 舆情分析中心初始化完成")
else:
    _sentiment_analyzer = None

logging.info("✅ V89 Phase 1 架构层初始化完成")
logging.info("  - Config: 全局配置中心")
logging.info("  - DataProvider: 安全数据层（容错+缓存）")
logging.info("  - ExpectationLayer: 宏观预期层（Risk On/Off/Neutral）")
logging.info("✅ V89 Phase 2 性能优化层初始化完成")
logging.info("  - PerformanceMonitor: 性能监控器")
logging.info("  - LayeredCacheManager: 分层缓存管理器（Fast/Daily/Weekly）")
logging.info("  - 并发线程池: 最大{}线程".format(Config.MAX_WORKERS))

# ═══════════════════════════════════════════════════════════════

st.set_page_config(layout="wide", page_title="V88 · GPT-6与经典巨著", page_icon="👑", initial_sidebar_state="collapsed")

# 【全站字体层级 2026-07-31 用户定纲"主要内容字体大一点,解释说明≤图标字体,不超过现有最大"】
# 主内容(表格数据/矩阵)=13.5px;解释说明/悬停提示行=11px封顶;层级恒定:重要>说明
# 【07-31 用户"字体再缩1/3,阿里那行高度显示两只股"】密度版:主内容10.5>说明8.8,
# 行距/内边距同步压缩——每行高度≈原一半
# 层级铁序(07-31用户抓"名称比时间戳还小,顾此失彼"):
# ①名称(链接)12.5px粗=第一眼 ②关键数字10.5px ③时间戳/说明/身份证8.2px灰=最底层
# 【07-31终局】只留td基线与密度;不再用!important钝器盖子元素——
# 每个元素的字号由生成源头的行内样式精确指定(名称13粗/现价12.5/章12/说明8),行内=唯一裁决
st.markdown("""<style>
table td, table th { font-size: 10.5px !important; padding: 2px 5px !important;
                     line-height: 1.25 !important; }
/* 【2026-08-01 用户"这个不断重复划桨看不出来"】Streamlit 自带的跑步小人是纯循环动画,
   既不表示进度、还很吵。换成 GitHub 式顶部细进度条:一次横扫=一个刷新周期,
   视觉上读作"在推进"而不是"在原地跑"。真正算得出百分比的重活(个股对比/搜索/热力图扫描)
   仍各自用 st.progress 给确定性进度——那才是真进度,这条只是状态指示。 */
[data-testid="stStatusWidget"]{
  position:fixed!important; top:0!important; left:0!important; right:0!important;
  width:100vw!important; height:3px!important; padding:0!important; margin:0!important;
  background:#e5e7eb!important; border-radius:0!important; box-shadow:none!important;
  overflow:hidden!important; z-index:99999!important;
}
[data-testid="stStatusWidget"] > *{ display:none!important; }
[data-testid="stStatusWidget"]::after{
  content:""; position:absolute; top:0; left:-35%; height:100%; width:35%;
  background:linear-gradient(90deg,rgba(37,99,235,0),#2563eb,rgba(37,99,235,0));
  animation:v88bar 1.05s linear infinite;
}
@keyframes v88bar{ from{left:-35%} to{left:100%} }
</style>""", unsafe_allow_html=True)

# 【V88·全局字体系统 2026-07-17 用户定纲】版面字体像 Claude：干净无衬线+克制层级；
# 字号规范：基准=五号(14px)，标题上限≈四号(18px)，全局下限=小五号(12px)——不允许更小的字。
# 重要字眼（动作/涨跌/名字）已由动作分色与红涨绿跌体系区分，这里统一底层字体。
st.markdown("""<style>
html, body, [data-testid="stAppViewContainer"], .stMarkdown, .stMarkdown p, .stMarkdown li,
.stDataFrame, .stCaption, button, input, textarea, select {
  font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "Helvetica Neue",
               "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif !important;
  letter-spacing: .01em;
}
html { font-size: 14px; }                              /* 五号基准 */
.stMarkdown p, .stMarkdown li { font-size: 14px; line-height: 1.65; }
[data-testid="stCaptionContainer"], .stCaption, small { font-size: 12px !important; }  /* 小五下限 */
[data-testid="stAppViewContainer"] [data-testid="stCaptionContainer"] p,
[data-testid="stAppViewContainer"] [data-testid="stCaptionContainer"] li,
[data-testid="stExpander"] > details > summary p {
  font-size:12px!important;line-height:1.45!important;
}
[data-testid="stCaptionContainer"] p{margin-bottom:3px}
.v88-quick-nav{display:flex;gap:16px;align-items:center;padding:3px 0;font-size:12px}
.v88-quick-nav a{color:#334155;text-decoration:none;border-bottom:1px solid #cbd5e1}
.v88-triad-header,.v88-scan,#v88-grade-list{scroll-margin-top:40px}
h1 { font-size: 18px !important; }                     /* 上限≈四号 */
h2 { font-size: 17px !important; }
h3 { font-size: 16px !important; }
h4 { font-size: 14px !important; }
</style>""", unsafe_allow_html=True)

st.markdown('<nav class="v88-quick-nav" aria-label="列表快捷导航">'
            '<a href="#v88-market-scan" target="_self">全市场扫描</a>'
            '<a href="#v88-grade-list" target="_self">3A / 2A / 1A 列表</a>'
            '<a href="#v88-astra-monthly" target="_self">Astra月度</a>'
            '<a href="#v88-deep-analysis" target="_self">个股深度</a>'
            '<a href="#v88-system-check" target="_self">系统检查</a>'
            '</nav>', unsafe_allow_html=True)
from presentation_style import CSS as _professional_style
st.markdown(_professional_style, unsafe_allow_html=True)
from phone_access import html as _phone_access_html
st.markdown(_phone_access_html(), unsafe_allow_html=True)
from runtime_status_ui import html as _runtime_status_html
st.markdown(_runtime_status_html(), unsafe_allow_html=True)
st.markdown('<div id="v88-system-check"></div>', unsafe_allow_html=True)
_v88_system_details = st.expander("⚙️ 系统与数据检查", expanded=False)

# 【V88·页面防跳动 2026-07-20 用户反馈"浏览时页面跳动/点击后跳页"】
# Streamlit 每次 rerun(缓存回填/按钮/自动刷新)都会把滚动位置弹回顶部——
# 用 sessionStorage 记住滚动位置,rerun 后静默恢复;用户一滚动就交还控制权。
# 深链(#锚点)场景跳过恢复,不与"点名进深度分析"的定位打架。
try:
    import streamlit.components.v1 as _components_scroll9
    _components_scroll9.html("""<script>
(function () {
  const w = window.parent;
  const d = w.document;
  if (w.location.hash) return;               // 深链定位优先，不抢滚动
  const KEY = "v88_scroll_y";
  const scroller = d.querySelector('[data-testid="stAppViewContainer"] section')
                || d.querySelector('section.main')
                || d.querySelector('[data-testid="stMain"]')
                || d.scrollingElement;
  const getY = () => (scroller === d.scrollingElement ? w.scrollY : scroller.scrollTop);
  const setY = (y) => { if (scroller === d.scrollingElement) w.scrollTo(0, y); else scroller.scrollTop = y; };
  const saved = parseFloat(w.sessionStorage.getItem(KEY) || "0");
  if (saved > 0) {
    let tries = 0;
    const t = setInterval(() => { setY(saved); if (++tries > 20) clearInterval(t); }, 150);
    const stop = () => clearInterval(t);
    w.addEventListener("wheel", stop, { once: true, passive: true });
    w.addEventListener("touchmove", stop, { once: true, passive: true });
    w.addEventListener("keydown", stop, { once: true });
  }
  let tid = null;
  const save = () => { clearTimeout(tid); tid = setTimeout(() => w.sessionStorage.setItem(KEY, String(getY())), 150); };
  (scroller === d.scrollingElement ? w : scroller).addEventListener("scroll", save, { passive: true });
})();
</script>""", height=0)
except Exception:
    pass

# 首屏占位：定义稍后才可用的今日导航函数后回填到这里，使“大盘/持仓/自选/预警”
# 在视觉顺序上始终排第一，原有后续模块全部保留。
# 【V88·此刻按钮 2026-07-18 用户点单】全站缓存都是固定节奏，右上角给一个总开关：
# 点了才强制用此刻最新数据（清会话缓存+st.cache_data+行情pkl文件缓存后整页重算），
# 不点一律走原缓存零额外流量。AI解读类不受此按钮影响（守预算，仍按各自节流）。
# 【U3.2 2026-07-26 用户点单】全球市场概览置顶(双时钟+三市场体制一眼可见)
# 【2026-07-29 用户抓"为什么又把置顶的时间给我删除了"】双时钟原本在6007行的
# 全球概览块里,而那块被分页切进 LISTS 段——总览页只跑 A 段(到4401行),
# 于是槽位在A段、回填在LISTS段,**总览页的置顶时钟永远是空的**。
# 双时钟只读系统时间(零成本),不该跟着重量级宏观面板一起被切走→单独提到A段常驻。
try:
    from datetime import datetime as _dtc9
    from zoneinfo import ZoneInfo as _zi9
    _wd9 = {"Monday": "周一", "Tuesday": "周二", "Wednesday": "周三", "Thursday": "周四",
            "Friday": "周五", "Saturday": "周六", "Sunday": "周日"}.get(_dtc9.now().strftime("%A"), "")
    _bj9 = _dtc9.now(_zi9("Asia/Shanghai")).strftime("%m-%d %H:%M")
    _ny9 = _dtc9.now(_zi9("America/New_York")).strftime("%m-%d %H:%M")
    # 三市场体制条:全部读落盘(零网络),保住总览页秒开;
    # 带AI解读的完整宏观脉搏面板仍在下方(LISTS段),这里是"永远看得到"的那一层。
    _mk_cells9 = []
    _snapshot_label9 = "快照时间未知"
    try:
        _snap9c = json.loads((core_root() / "data" /
                              "market_snapshot.json").read_text(encoding="utf-8"))
        _snapshot_label9 = "分析快照 " + str(_snap9c.get("generated_at") or "未知")[:16]
        from market_overview_ui import cells as _market_cells, legend as _market_legend
        _mk_cells9 = [_market_cells(_snap9c)]
    except Exception:
        pass
    st.markdown(
        f"<div style='padding:.2rem .45rem;margin:0 0 .3rem;border-bottom:1px solid #e2e8f0;"
        f"font-size:11px;line-height:1.45;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap' class='v88-market-overview'>"
        f"<span><b style='color:#334155'>🌍 全球市场概览</b>{''.join(_mk_cells9)}"
        f"<span style='margin-left:8px;color:#b45309'>{_snapshot_label9}</span></span>"
        f"<span style='color:#1e3a5f;font-weight:600;white-space:nowrap'>"
        f"{_dtc9.now().strftime('%Y-%m-%d')} {_wd9}　🇺🇸纽约 {_ny9}　🇨🇳北京 {_bj9}</span>"
        f"<span style='flex-basis:100%'>{__import__('market_overview_ui').legend()}</span></div>",
        unsafe_allow_html=True)
except Exception as _clk_e9:
    logging.exception(f"[V88] 置顶全球概览渲染失败: {_clk_e9}")
# 扫描与评级紧接置顶行情时钟；完整宏观面板在列表之后。
# 独立常驻容器，不能随宏观槽位的回填/条件而消失。
_three_a_slot = st.container()
# 先渲染本地中央列表；后续行情预载超时不能挡住3A持续跟踪。
with _three_a_slot:
    @st.fragment(run_every=60)
    def _three_a_fragment():
        # ═══════════════════════════════════════════════════════════════
        # 【🎯 3A大系统·常驻模块 2026-08-02 用户"放在大盘和今日之间"·第4版】
        # 前两版分别错在:①做成卡片(用户要图二的11列表格) ②塞进买表嵌套作用域→
        # 渲染在页面别处、用户根本看不到。现改用 grade_card.system_table_html
        # (模块级自包含,自产表格HTML,不依赖任何嵌套闭包),故可挂页面任意位置。
        # 常驻:无3A也在,空态明说并指出离3A最近者。
        # ═══════════════════════════════════════════════════════════════
        try:
            from grade_card import system_table_html as _sys3a
            _d3a = core_root() / "data"

            def _j3a(_f):
                try:
                    return json.loads((_d3a / _f).read_text(encoding="utf-8"))
                except Exception:
                    return {}
            _rk3a = _j3a("rank_score.json")
            if _rk3a.get("rows"):
                # This is complete HTML, not Markdown. Use the native HTML
                # parser + DOMPurify instead of reconstructing every tag in
                # React; audit text remains text and scripts stay disabled.
                _central_board3a = _sys3a(
                    _rk3a, _j3a("sell_grade.json"),
                    {str(x.get("code")): x for x in
                     (_j3a("intraday_decisions.json").get("rows") or [])},
                    (_j3a("why_buy.json").get("sells") or {}),
                    _j3a("market_pool.json"),
                    triad=_j3a("triad_selection.json"), weekly=_j3a("weekly_candidates_pub.json"),
                    reverse_audit=_j3a("reverse_audit_pub.json"), reverse_status=_j3a("reverse_audit_status.json"),
                    relations=_j3a('module_relations_pub.json'), watchlist=_j3a('persistent_watchlist_pub.json'))
                st.html(_central_board3a)
                from action_source_view import html as _source_links3a, SOURCES as _source_files3a
                st.html(_source_links3a(
                    {name: _j3a(name) for name in _source_files3a}, _central_board3a))
            else:
                st.warning("🎯 3A大系统: 评级数据未就绪(模块常驻,数据恢复后自动填充)")
            st.caption("列表读取：" + datetime.now().strftime("%m-%d %H:%M:%S") + " · 每60秒局部更新；源数据日期见表格")
            from investment_tracking_ui import render_tracking
            from evolution_learning_ui import render as render_evolution
            render_evolution(_j3a('evolution_learning_pub.json'), _j3a('evolution_learning_status.json'), _j3a('triad_selection.json'))
            render_tracking(_j3a("triad_selection.json"), _j3a("investment_tracks.json"), _j3a("idea_journal.json"))
            from weekly_quality_ui import render as render_weekly_quality
            render_weekly_quality(_j3a('weekly_quality.json'),_j3a('database_quality.json'))
        except Exception:
            logging.exception("[V88] 3A大系统模块渲染失败")
            st.warning("🎯 3A大系统: 渲染异常(见日志);模块常驻不消失")
    _three_a_fragment()

_macro_top_slot = st.empty()
# 【2026-07-29 用户"版面设计有点浪费"】按钮字号-30%，数据时点从按钮下方挪到右侧同一行——
# 省掉一整行垂直空间。只作用于 .st-key-btn_force_now，不动全站其它按钮。
st.markdown("""<style>
.st-key-btn_force_now button{font-size:11px!important;padding:.18rem .4rem!important;
  min-height:0!important;line-height:1.25!important}
.st-key-btn_force_now button p{font-size:11px!important;margin:0!important}
</style>""", unsafe_allow_html=True)
_now_c1, _now_c2, _now_c3 = st.columns([7.35, 1.35, 1.30])
with _now_c2:
    if st.button("📡 此刻最新", key="btn_force_now", use_container_width=True,
                 help="强制用此刻最新行情重算全页（约30-60秒）；不点则按原缓存节奏。AI解读不重跑、不花预算。"):
        _v88_usage9(core_root(), "此刻最新")   # 【点击热力】
        import time as _tnow9
        if _tnow9.time() - float(st.session_state.get("_force_now_ts") or 0) < 60:
            st.toast("60秒内刚强刷过，当前已是此刻数据", icon="⏳")
        else:
            st.session_state["_force_now_ts"] = _tnow9.time()
            # watch_alerts_v88=自选/持仓/预警/四档推荐共用的一份30分钟会话缓存
            for _k9 in ("watch_alerts_v88", "_fw_panel9"):
                st.session_state.pop(_k9, None)
            try:
                st.cache_data.clear()
            except Exception:
                pass
            try:
                for _f9 in (Path(__file__).parent / ".cache_stock_data").glob("*.pkl"):
                    _f9.unlink()
            except Exception:
                pass
            st.toast("📡 已切换到此刻最新数据，全页重算中…", icon="📡")
            st.rerun()
    # 【2026-07-27 用户点单"增加跳动准确时间"】常显数据时点(不只点击后):
    # 优先显示强刷时刻,否则显示当前行情快照的生成时间——随时知道看的是几点的数据。
    import time as _tnow9b
    from datetime import datetime as _dtn9b
    _fnts9 = float(st.session_state.get("_force_now_ts") or 0)
    if _fnts9:
        _mins9b = int((_tnow9b.time() - _fnts9) / 60)
        _tip9b = (f"⚡{_dtn9b.fromtimestamp(_fnts9).strftime('%H:%M:%S')} 强刷"
                  + (f"·{_mins9b}分钟前" if _mins9b else "·刚刚"))
        _col9b = "#16a34a" if _mins9b < 30 else "#b45309"
    else:
        try:
            _snap_ts9b = str(json.loads((core_root() / "data" /
                                         "market_snapshot.json").read_text(encoding="utf-8")).get("generated_at"))[:16]
            _age9b = (_dtn9b.now() - _dtn9b.strptime(_snap_ts9b, "%Y-%m-%d %H:%M")).total_seconds() / 3600
            _tip9b = f"📊数据{_snap_ts9b[5:]}·{_age9b:.1f}h前"
            _col9b = "#94a3b8" if _age9b < 6 else ("#b45309" if _age9b < 36 else "#dc2626")
        except Exception:
            logging.exception("[V88] 数据时点渲染失败")
            _tip9b = "数据时间未知"
            _col9b = "#94a3b8"
# 【2026-07-29 用户"下面的时间放到此刻刷新的右侧"】时点移出按钮列，与按钮同一行左对齐、
# 垂直居中——原来占一整行，现在并排，置顶区省下一行高度。
with _now_c3:
    st.markdown(f"<div style='font-size:11px;color:{_col9b};line-height:1.2;white-space:nowrap;"
                f"display:flex;align-items:center;height:100%;min-height:26px'>"
                f"{_tip9b}</div>", unsafe_allow_html=True)

# 【V88·今日指令牌 2026-07-24 用户定纲"每天打开=方向+进攻防守+成功率,升级要明显提示"】
# 开屏第一眼三行:①方向(定调+三市场周概率) ②进攻/防守名单点名 ③实盘战绩背书+✨最新升级。
# 全读落盘零重计算,秒开;明细仍在下方各模块,此处只做"今天该干什么"的最短路径。

# 【V88·个股-大盘统一裁决 2026-07-25 用户定纲"逻辑和说明要统一"】一把尺三态:
# 偏弱/拐点(p2w≤45或verdict转弱杀跌派发偏冷)→该市场绿灯⏸️暂不执行留档跟踪;
# 过热分歧(p2w≥55但温度≥75)→🔶只限回踩位不追高仓位减半; 良性/中性→正常(无绿灯时报领涨引擎)。
# 覆巢之下无完卵:危险市场不再出现"逆势增长"执行建议——与温度verdict同一张嘴。
def _v88_nontrade9x():
    """非交易日判定(周六日;按日历判——v88-trading-day定纲)。返回 (是否休市, 下一交易日人话)。"""
    from datetime import datetime as _dt9nt, timedelta as _td9nt
    _now9nt = _dt9nt.now()
    if _now9nt.weekday() < 5:
        return False, "今日"
    _nx9nt = _now9nt + _td9nt(days=(7 - _now9nt.weekday()))
    return True, f"下一交易日(周一{_nx9nt.strftime('%m-%d')})"


def _v88_mkt_gate9x(_repo9x):
    out = {}
    try:
        _mks9x = json.loads((_repo9x / "data" / "market_snapshot.json")
                            .read_text(encoding="utf-8")).get("markets") or {}
    except Exception:
        return out
    for _mk9x, _b9x in _mks9x.items():
        try:
            _p9x = dict((x[0], x[1]) for x in ((_b9x.get("l3") or {}).get("probs") or [])).get("2周")
            _t9x = (_b9x.get("temperature") or {})
            _tv9x, _vd9x = float(_t9x.get("temp") or 50), str(_t9x.get("verdict") or "")
            _weak9x = (_p9x is not None and int(_p9x) <= 45) or any(
                k in _vd9x for k in ("转弱", "杀跌", "派发", "偏冷"))
            _hot9x = (_p9x is not None and int(_p9x) >= 55) and _tv9x >= 75
            _secs9x = sorted([x for x in (_b9x.get("sectors") or []) if isinstance(x, dict)],
                             key=lambda x: -(x.get("chg1d") or x.get("chg") or 0))[:2]
            _lead9x = "、".join(f"{x.get('name')}{(x.get('chg1d') or x.get('chg') or 0):+.1f}%"
                               for x in _secs9x)
            if _weak9x:
                _st9x, _po9x = "weak", "⏸️大盘拐点/偏弱·绿灯暂不执行,留档跟踪(回中性激活)"
            elif _hot9x:
                _st9x, _po9x = "hot", "⚠ 大盘过热·复核回撤与仓位风险"
            elif _p9x is not None and int(_p9x) >= 55:
                _st9x, _po9x = "up", "↑ 大盘规则偏强·核对个股中央原条件"
            else:
                _st9x, _po9x = "mid", "↔ 大盘规则中性·个股执行仍需中央原条件"
            out[_mk9x] = {"state": _st9x, "policy": _po9x, "p2w": _p9x,
                          "temp": int(_tv9x), "leaders": _lead9x}
        except Exception:
            continue
    return out


# 【V88·今日确认买单 2026-07-25 用户定纲"最需要清晰的告知和确认该买了,V88不够清晰"】
# 开屏置顶大字:所有有发言权源的"现价可进"信号汇总成明确告知——该买谁/为什么/买区/止损/
# 概率/裁决仓位;没有确认单=如实说"最近的差什么"(准确认队列),绝不让该买的时刻淹没在小字里。
try:
    _cb_repo9 = core_root()
    # ══ 【2026-08-02 用户"右边全空了""云端能不能和V88页面一样"】 ══
    # 根因:上面是**本机绝对路径**,云端容器里没有 ~/Desktop/ai-daily-report-v2,
    # 于是 _cbj9 对每个文件都返回 {} —— 而且 `except: return {}` **把错误全吞了**,
    # 页面照画空壳:多选对比下拉空、右栏空、组合体检只剩两个空图标。
    # 修:本机读不到 → **兜底读公开仓 pub/**(与 streamlit_app.py 同源),
    # 并且**不再静默吞错**——失败留痕到 _CB_ERR9,页面可显示"数据源不可用"。
    _CB_ERR9 = {}
    _PUB_BASE9 = "https://raw.githubusercontent.com/bluestevenr-lang/stockai-v88/data/pub"

    @st.cache_data(ttl=600, show_spinner=False)
    def _cb_remote9(_fn: str):
        import requests as _rq
        r = _rq.get(f"{_PUB_BASE9}/{_fn}", timeout=12)
        r.raise_for_status()
        return r.json()

    def _cbj9(_fn):
        try:
            return json.loads((_cb_repo9 / "data" / _fn).read_text(encoding="utf-8"))
        except Exception as _e_local:
            try:
                return _cb_remote9(_fn)
            except Exception as _e_pub:
                _CB_ERR9[_fn] = f"本机:{type(_e_local).__name__} 远端:{type(_e_pub).__name__}"
                return {}
    _nwj9 = _cbj9   # 同仓data读取器别名(修使用先于定义:原def在3600行段,调用在3373)

    def _flag9(_cd9f) -> str:
        _c9f = str(_cd9f or "").upper()
        if not _c9f:
            return ""
        return ("🇨🇳" if _c9f.endswith((".SS", ".SZ", ".SH", ".BJ"))
                else ("🇭🇰" if _c9f.endswith(".HK") else "🇺🇸"))

    def _nw_link9(_nm9x, _cd9x):
        # 【2026-07-25 用户抓"只有A股能点"】与全站 _stk_link 完全同款(新标签+下划线)——
        # 原 target=_self 当前页重载在部分场景吞掉深链参数,美港股点了没反应。
        # 【2026-07-27】无代码的条目(板块名如"半导体")不生成死链,原样返回文字。
        if not str(_cd9x or "").strip():
            return str(_nm9x or "")
        from stock_profile_view import display_label as _profile_name9, compact_html as _profile_compact9
        if '<' not in str(_nm9x or ''):
            _nm9x = _profile_name9(_nm9x, _cd9x)
        # 名字前统一挂市场旗(用户"所有股写到一块,是不是美股我都不知道");
        # 已自带旗的调用点(五行业代表)不重复挂
        _fg9x = "" if str(_nm9x or "")[:2] in ("🇨🇳", "🇭🇰", "🇺🇸") else _flag9(_cd9x)
        _nm_html9x = str(_nm9x or "")
        # _cb_nm9 已附带规则/GPT徽章时不要再次追加，避免 R/G/R/G 重复；
        # 名称与徽章一起处于深链内，任何显示档位都能直接进入个股深度分析。
        _cert9x = "" if "title='V88" in _nm_html9x or 'title="V88' in _nm_html9x else _cert_badge9(_cd9x, _nm9x)
        _gpt9x = "" if "title='GPT" in _nm_html9x or 'title="GPT' in _nm_html9x else _gpt_badge9(_cd9x, _nm9x)
        _book9x = "" if "title='经典巨著" in _nm_html9x or 'title="经典巨著' in _nm_html9x else _book_badge9(_cd9x, _nm9x)
        return (f'{_fg9x}<a href="?q={_cd9x}&focus=deep#v88-deep-analysis" target="_blank" rel="noopener" '
                f'style="color:#1e3a5f;text-decoration:underline;cursor:pointer;font-weight:600">{_nm9x}</a>'
                + _cert9x + _gpt9x + _book9x + _profile_compact9(_cd9x))
    _cb_gate9 = _v88_mkt_gate9x(_cb_repo9)
    _cb_nt9, _cb_day9 = _v88_nontrade9x()
    # 【U3⑥数据闸门 2026-07-26 GPT审计采纳】行情异常=degraded→买侧禁发,只留卖警
    _dg9 = _cbj9("health_gate.json")
    _cb_degraded9 = bool(_dg9.get("degraded"))
    # 【铁律17升级 2026-07-31 用户"升级"】按行冻结集:data_gate 判定"价旧于收盘+4h"的代码——
    # 买侧移出确认买(转⏸未达标区)、卖侧动作改🚫冻结,不再只靠整闸degraded一把抓
    _px_stale17 = {str(i.get("code")) for lst in (_dg9.get("px_stale") or {}).values()
                   for i in (lst or [])}
    try:
        # stock_names.json=列表[{n,c,m}] → 归一成 {code: 名};港股键同录去前导零版(2020==02020)
        _cb_names9 = {}
        for _e9n in json.loads((_cb_repo9 / "src" / "stock_names.json").read_text(encoding="utf-8")):
            _c9n = str(_e9n.get("c") or "").upper()
            if _c9n:
                _cb_names9[_c9n] = _e9n.get("n")
                if _c9n.endswith(".HK"):
                    _cb_names9[_c9n.split(".")[0].lstrip("0") + ".HK"] = _e9n.get("n")
    except Exception:
        _cb_names9 = {}

    try:
        _CS9 = json.loads((_cb_repo9 / "data" / "claude_standard.json").read_text(encoding="utf-8"))
    except Exception:
        _CS9 = {}
    # ai_cert 为历史兼容镜像；现役页面只展示 V88规则闸(R/✓) 与 GPT复核(G)。
    try:
        # 【P1·2026-08-02 GPT审计】ai_cert 退出"认证"语义:
        # ①它是**自然语言解析**出来的(把系统异议里的"CS"当成了股票代码,8条里污染1条)
        # ②数据停在07-31,已陈旧 ③它从来不在准入链里(准入走 claude_standard/dual_cert/three_way)
        # 故:保留展示价值(它确实记录了Fable对个股的上下文异议),但**不叫认证、不影响档位**,
        # 且过期3天即不再显示——避免一个陈年结论在界面上冒充"已复核"。
        _CERT9 = json.loads((_cb_repo9 / "data" / "ai_cert.json").read_text(encoding="utf-8"))
        try:
            from datetime import date as _d9c
            _age9c = (_d9c.today() - _d9c.fromisoformat(str(_CERT9.get("asof"))[:10])).days
            if _age9c > 3:
                _CERT9 = {"by_code": {}, "by_name": {}, "_stale": _age9c}
        except Exception:
            pass
        # 过滤非股票键(CS规则名等解析噪音)
        for _seg9c in ("by_code", "by_name"):
            _CERT9[_seg9c] = {k: v for k, v in (_CERT9.get(_seg9c) or {}).items()
                              if not str(k).startswith("CS")}
    except Exception:
        _CERT9 = {}

    def _cert_badge9(_cd, _nm=""):
        """历史函数名保留；现只显示本地 V88 规则闸，不冒充任何AI认证。"""
        _b9 = None
        try:
            for _k9 in (str(_cd or ""), str(_cd or "").split(".")[0], str(_nm or "")):
                if not _k9:
                    continue
                if _k9 in (_CS9.get("pass") or {}):
                    _b9 = {"verdict": "达标", **((_CS9.get("pass") or {}).get(_k9) or {})}
                elif _k9 in (_CS9.get("reject") or {}):
                    _b9 = {"verdict": "否决", **((_CS9.get("reject") or {}).get(_k9) or {})}
        except Exception:
            pass
        _base9 = ("display:inline-block;width:15px;height:15px;line-height:15px;text-align:center;"
                  "border-radius:50%;font-size:10px;font-weight:800;margin-left:3px;"
                  "vertical-align:middle;letter-spacing:-.3px")
        if _b9 and _b9.get("verdict") == "否决":
            return (f"<span title='V88规则闸否决:{'/'.join(_b9.get('rules') or [])}' "
                    f"style='{_base9};background:#fee2e2;color:#b91c1c;border:1px solid #fca5a5'>R̸</span>")
        if _b9:
            return ("<span title='V88本地规则闸达标；这是确定性机检，不是AI背书' "
                    f"style='{_base9};background:#dcfce7;color:#15803d;border:1px solid #86efac'>R</span>")
        return ""      # 无结论=不打标,不再用禁止符污染视觉

    def _gpt_badge9(_cd, _nm=""):
        """【2026-08-02 用户"gpt的验证一定要有大写字母G,就像claude的C一样,不同颜色标注"】
        与 _cert_badge9 同规格(圆形单字符),但**身份色不同**:C 系金/绿,G 系青(#0d9488)。
        身份色恒定、裁决用符号与深浅表达——一眼分清"谁说的"与"说了什么"。
        不推翻既有 C 徽章语义,只是把 GPT 这一方补齐(此前它在站内根本没有可视标识)。"""
        try:
            _raw9 = str(_cd or "").upper()
            _keys9 = [_raw9]
            if _raw9.endswith(".HK"):
                _bare9 = _raw9[:-3].lstrip("0") or "0"
                _keys9 += [_bare9 + ".HK", _bare9.zfill(5) + ".HK"]
            _g9 = next(((_GV9 or {}).get(k) for k in _keys9 if (_GV9 or {}).get(k)), {})
        except Exception:
            _g9 = {}
        try:
            from recommendation_gate import review_for, review_is_fresh
            _, _current_vote9, _current_at9 = review_for(_cd)
            if not review_is_fresh(_current_at9):
                return ""
            _g9 = {**_g9, "verdict": _current_vote9}
        except Exception:
            return ""
        _v9 = str(_g9.get("verdict") or "")
        # 【2026-08-16 界面修复·视觉噪音】无复核记录的票不再挂灰色"G—"徽章——
        # 一屏几十个灰点会淹没真正的红徽章；只保留有记录时的警示/通过标识。
        if not _v9:
            return ""
        # 【2026-08-03 修 NameError】_base9 是 _cert_badge9 的**局部变量**,
        # 我加 G 徽章时抄了样式却没带定义 → NameError 把整个作战板中段(题材榜/龙虎榜)打掉。
        # 圆形单字符样式在此自带一份,不再跨函数借用。
        _base9 = ("display:inline-block;width:15px;height:15px;line-height:15px;"
                  "text-align:center;border-radius:50%;font-size:10px;font-weight:800;"
                  "margin-left:3px;vertical-align:middle;letter-spacing:-.3px")
        _st9 = ("#0d9488", "#fff", "G✓") if _v9 == "通过" else \
               ("#991b1b", "#fff", "G×") if _v9 == "否决" else \
               ("#ccfbf1", "#0f766e", "G?")
        _tip9 = f"GPT独立验证:{_v9}｜{str(_g9.get('why', ''))[:60]}".replace('"', "'")
        _op9 = "1" if _v9 in ("通过", "否决") else ".8"
        return (f"<span title=\"{_tip9}\" style='{_base9};background:{_st9[0]};"
                f"color:{_st9[1]};opacity:{_op9}'>{_st9[2]}</span>")

    try:
        _GV9 = (_nwj9("gpt_verify.json").get("rows") or {})
    except Exception:
        _GV9 = {}
    def _book_badge9(_cd, _nm=""):
        try:
            from recommendation_gate import classics_review_for
            from html import escape
            verdict, at = classics_review_for(_cd)
            if not at:
                return ""
            mark = "✓" if verdict == "通过" else "×" if verdict == "否决" else "?"
            return (f"<span title='{escape('经典巨著：'+verdict+'｜'+at, quote=True)}' "
                    "style='color:#6d28d9;font-weight:800;margin-left:3px'>"
                    f"书{mark}</span>")
        except Exception:
            return ""
    def _cb_nm9(_nm, _cd):
        # 【2026-07-27 统一名字真源】优先私仓 watch_alerts.resolve_name
        # (库内中文名>美股补充表>池名;港股前导零双向归一);不可用回退本地简版。
        try:
            import sys as _sy9n
            if str(_cb_repo9 / "src") not in _sy9n.path:
                _sy9n.path.insert(0, str(_cb_repo9 / "src"))
            from watch_alerts import resolve_name as _rn9c
            _n9 = _rn9c(_cd, _nm)
            return (_n9 + _cert_badge9(_cd, _n9) + _gpt_badge9(_cd, _n9)
                    + _book_badge9(_cd, _n9))
        except Exception:
            pass
        _n = str(_nm or "")
        if _n and _n != str(_cd):
            return (_n + _cert_badge9(_cd, _n) + _gpt_badge9(_cd, _n)
                    + _book_badge9(_cd, _n))
        _k = str(_cd or "").upper()
        _out9 = (_cb_names9.get(_k) or _cb_names9.get(_k.split(".")[0].lstrip("0") + ".HK")
                 or _n or _k)
        return (_out9 + _cert_badge9(_cd, _out9) + _gpt_badge9(_cd, _out9) + _book_badge9(_cd, _out9))

    def _cb_link9(_nm, _cd):
        """V88行动中心唯一股票名出口：先解析正式名称，再生成深度分析链接。

        永久硬规则：推荐、准备买、研究、陷阱、风险、未达标等所有档位只要出现
        个股名称，就必须由此处或同等深链输出；档位改变不得取消可点击能力。
        """
        # Company identity is formatted before badges; prebuilt badge HTML used
        # to bypass the shared English/Chinese/code name overlay entirely.
        return _nw_link9(_nm, _cd)

    def _cb_mk9(_cd):
        _c = str(_cd or "").upper()
        return ("A股" if _c.endswith((".SS", ".SZ", ".SH", ".BJ")) else ("港股" if _c.endswith(".HK") else "美股"))
    _MKFLAG9 = {"A股": "🇨🇳", "港股": "🇭🇰", "美股": "🇺🇸"}
    # The old second action/grade table is retired. Current grades, contract
    # actions and holding protection are rendered once by the central board.
    # Keep independent directory/coverage/system tools available even when no
    # legacy technical row is present.
    from market_coverage_view import render as _render_market_coverage
    _render_market_coverage(st, _cb_repo9)
    from market_directory_view import render as _render_directory_search
    _render_directory_search(st, _cb_repo9)
    from module_health_ui import render as _render_module_health9
    with _v88_system_details:
        _render_module_health9(_cb_repo9)

except Exception:
    try:
        _v88_sentinel9(core_root(), "目录与覆盖检查")
    except Exception:
        pass

# 【V88·U3金字塔导航 2026-07-26】五层阅读顺序显性化:行动→预判→机会→防守→证据

# 【V88·时间作战板 2025-07-25 用户批准"六档一个模块,时间上方选,省版面+每档最优口径"】
# 今日/本周/下周/本月/下月/本季度/下季度 七档切换,六问结构复用:大盘/板块轮转/低位埋伏/买/卖/事件。
# 每档接自己的最优数据口径(见_TB_CFG9),全落盘聚合零新计算;默认档:交易日=今日,周末=下周。
try:
    from datetime import datetime as _dtnw, timedelta as _tdnw
    _is_weekend9 = _dtnw.now().weekday() >= 5
    _nw_repo9 = core_root()

    # (_nwj9 已改为 _cbj9 别名并前移——2026-07-31)

    def _v88_refresh9(_label9r, _est9r, _fn9r, _key9r):
        """【V88·立刻更新按钮 2026-07-25 用户点单"每个用缓存的模块都要能立刻更新"】
        通用强刷:60秒防重+spinner+完成即重跑页面。fn=无参重算函数(内部import私仓模块force跑)。"""
        import time as _t9r
        if st.button(f"🔄 {_label9r}·立刻更新（{_est9r}）", key=_key9r):
            if _t9r.time() - float(st.session_state.get(_key9r + "_ts") or 0) < 60:
                st.toast("60秒内刚更新过，当前已是最新", icon="⏳")
                return
            st.session_state[_key9r + "_ts"] = _t9r.time()
            with st.spinner(f"{_label9r} 重算中（{_est9r}）…完成自动刷新"):
                try:
                    _fn9r()
                    st.toast(f"✅ {_label9r}已用最新数据重算", icon="✅")
                except Exception as _e9r:
                    st.toast(f"❌ {_label9r}更新失败: {str(_e9r)[:40]}", icon="❌")
            st.rerun()

    # 【认证徽章·作战板 2026-07-27 用户抓"图一图二为何没有C验证图标"】
    # 作战板(低位拐点/全市场机会/准备买/卖/五行业代表/拐点倒计时)走的是_nw_link9这条路径,
    # 与行动中心的_cb_nm9不是同一个出口——两条路都必须挂徽章,否则总有半边没有标。
    # 【市场标识 2026-07-27 用户"你都把所有股写到一块,是不是美股我都不知道"】
    # 作战板各名单(低位拐点/全市场机会/拐点倒计时/卖名单/预警兑现)原来只有名字,
    # 中美港混排看不出归属。统一按代码后缀出旗:.SS/.SZ/.SH/.BJ=🇨🇳 .HK=🇭🇰 其余=🇺🇸
    # (_flag9/_nw_link9 已前移至 _cbj9 之后——2026-07-31修使用先于定义)
    # 档位→最优口径: mkt_hz=大盘统一引擎档 / sec_hz=板块轮动点 / buy=买名单口径 / evt=机构简报档
    # 【V88·七档 2026-07-25 用户定纲"今日/本周/下周/本月/下月/本季度/下季度,稳一些"】
    # 明日档删除(触发单归今日/本周;且统一引擎无"明日"键,旧明日档大盘行是暗坑);
    # 季度两档吃16/32周(翻倍律V88-U2.1全五档正好铺满)。hz=买卖名单统一周期分档。
    _TB_CFG9 = {
        "今日":  {"mkt_hz": None,   "sec_hz": None,  "buy": "green",  "hz": None,
                  "evt": "明天", "days": (0, 1)},
        "本周":  {"mkt_hz": "2周",  "sec_hz": "2周", "buy": "green+", "hz": None,
                  "evt": "本周", "days": (0, 7)},
        "下周":  {"mkt_hz": "4周",  "sec_hz": "5周", "buy": "w4",     "hz": None,
                  "evt": "下周", "days": (7, 14)},
        "本月":  {"mkt_hz": "4周",  "sec_hz": "8周", "buy": "hz",     "hz": "4周",
                  "evt": "本月及下月", "days": (0, 30)},
        "下月":  {"mkt_hz": "8周",  "sec_hz": "16周", "buy": "hz",    "hz": "8周",
                  "evt": "本月及下月", "days": (30, 60)},
        "本季度": {"mkt_hz": "16周", "sec_hz": "16周", "buy": "hz",   "hz": "16周",
                  "evt": "本月及下月", "days": (0, 90)},
        "下季度": {"mkt_hz": "32周", "sec_hz": "16周", "buy": "hz",   "hz": "32周",
                  "evt": "本月及下月", "days": (90, 180)},
    }
    # (3A榜已并入上方行动中心确认买表格——2026-07-31 用户'图二整合到图一')
    with st.expander("⏱ 时间作战板 · 今日→下季度七档切换：大盘/轮转/低位埋伏/买/卖/事件（一屏六问）",
                     expanded=False):
        _tbc_t9, _tbc_m9 = st.columns([5.2, 3.8])
        with _tbc_t9:
            _tb_tier9 = st.radio("时间档", list(_TB_CFG9.keys()),
                                 index=(2 if _is_weekend9 else 0), horizontal=True,
                                 key="tb_tier9", label_visibility="collapsed")
        with _tbc_m9:
            # 【V88·市场筛选 2026-07-26 用户抓"都是美股,很少看到中港"】③④⑤名单+五行业代表跟随
            _tb_mkt9 = st.radio("市场", ["全部", "🇨🇳A股", "🇭🇰港股", "🇺🇸美股"], horizontal=True,
                                key="tb_mkt9", label_visibility="collapsed")

        def _mkfit9(_cd9m):
            if _tb_mkt9 == "全部":
                return True
            _c9m = str(_cd9m or "").upper()
            _mk9m = ("🇨🇳A股" if _c9m.endswith((".SS", ".SZ", ".SH", ".BJ")) else
                     ("🇭🇰港股" if _c9m.endswith(".HK") else "🇺🇸美股"))
            return _mk9m == _tb_mkt9
        _cfg9 = _TB_CFG9[_tb_tier9]
        _nw_snap9 = _nwj9("market_snapshot.json")
        _dh9n = _nwj9("darkhorse.json")
        _pt9n = _nwj9("phase_turn_full.json")
        # 【V88·明日作战预案 2026-07-25 用户批准】前一晚写好的if-then剧本:今日/明日两档
        # 且日期匹配时展示。买/卖节价格=真实entry_plan/stop确定性拼装,AI只写大盘与准备节。
        try:
            _tp9 = _nwj9("tomorrow_plan.json")
            _tp_fd9 = str(_tp9.get("for_date") or "")
            _tp_show9 = (_tp9.get("script") and _tb_tier9 == "今日" and _tp_fd9 and
                         (_tp_fd9 >= _dtnw.now().strftime("%Y-%m-%d") if _is_weekend9
                          else _tp_fd9 == _dtnw.now().strftime("%Y-%m-%d")))
            if _tp_show9:
                _tp_html9 = str(_tp9["script"])
                for _sec9t, _ic9t in (("## 大盘剧本", "🎬 大盘剧本"), ("## 买", "🐉 买"),
                                      ("## 卖·防", "⚔️ 卖·防"), ("## 准备", "🕐 准备")):
                    _tp_html9 = _tp_html9.replace(_sec9t, f"**{_ic9t}**")
                st.markdown(f"<div style='background:#fefce8;border:1px solid #eab308;"
                            f"border-radius:8px;padding:.4rem .7rem;margin:.2rem 0;font-size:12.5px'>"
                            f"🎬 <b>{_tp_fd9} 作战预案</b>·前一晚写好的if-then剧本"
                            f"<span style='color:#94a3b8'>（{_tp9.get('generated_at', '')}"
                            f"·买卖价格=系统真实数据,AI只组织大盘与准备节）</span></div>",
                            unsafe_allow_html=True)
                # 【标识统一 2026-07-27 用户"cc我都不知道,统一一下验证标识"】四处病根一次修:
                # ①预案区原用🅒/🚫两个emoji,与作战板的金C/✓绿勾不是一套→统一走_cert_badge9
                # ②"谷歌C"这类名字本身以C结尾,紧跟金C标会看成"CC"→插入前加细空格分隔
                # ③徽章HTML的title里含其它股票名(复核结论原文),按字符串循环替换会命中HTML内部
                #   造成嵌套span乱码→改为"先一次性定位所有名字、再从右往左插入",绝不二次扫描
                # ④被闸掉的票整行剔除:预案是前一晚写死的文本,过不了闸的不该还挂在卖名单里
                try:
                    _names9tp = sorted({str(k) for k in ((_CERT9.get("by_name") or {}).keys())}
                                       | {str(v.get("name") or "") for v in (_CS9.get("pass") or {}).values()},
                                       key=len, reverse=True)
                    _names9tp = [n for n in _names9tp if len(n) >= 2]
                    _rej_nm9 = {str(v.get("name") or "") for v in _cs_rej9.values() if v.get("name")}
                    _code_of9 = {}
                    for _k9c, _v9c in list((_CS9.get("pass") or {}).items()):
                        _code_of9.setdefault(str(_v9c.get("name") or ""), _k9c)
                    for _k9c, _v9c in list((_CERT9.get("by_name") or {}).items()):
                        _code_of9.setdefault(str(_k9c), str(_v9c.get("code") or ""))
                    # 大盘剧本行(A股:/港股:/美股:)沿用同一GPT/Codex复核徽章。
                    _mkt_cert9 = (_CERT9.get("by_market") or {})

                    def _mkt_badge9(_mk9b):
                        _v9b = _mkt_cert9.get(_mk9b)
                        if not _v9b:
                            return ""
                        _tp9b = str(_v9b.get("note") or "").replace('"', "'")
                        _st9b = ("display:inline-block;width:15px;height:15px;line-height:15px;"
                                 "text-align:center;border-radius:50%;font-size:10px;font-weight:800;"
                                 "margin-left:3px;vertical-align:middle")
                        if _v9b.get("verdict") == "一致":
                            return (f"<span title=\"GPT/Codex大盘复核通过:{_tp9b}\" style='{_st9b};"
                                    "background:linear-gradient(145deg,#fde68a,#d97706);color:#4a2c05;"
                                    "box-shadow:0 1px 2px rgba(180,120,0,.45)'>G</span>")
                        if _v9b.get("verdict") == "分歧":
                            return (f"<span title=\"⚡GPT/Codex与引擎读数有差异:{_tp9b}\" style='{_st9b};"
                                    "background:#dc2626;color:#fff'>G̸</span>")
                        return ""
                    _kept9, _dropped9 = [], 0
                    for _ln9tp in _tp_html9.splitlines():
                        for _mk9b in ("A股", "港股", "美股"):
                            if _ln9tp.lstrip("-* ").startswith(_mk9b + "：") or _ln9tp.lstrip("-* ").startswith(_mk9b + ":"):
                                _bd9b = _mkt_badge9(_mk9b)
                                if _bd9b:
                                    _i9b = _ln9tp.find(_mk9b) + len(_mk9b)
                                    _ln9tp = _ln9tp[:_i9b] + _bd9b + _ln9tp[_i9b:]
                                break
                        if any(_r9 and _r9 in _ln9tp for _r9 in _rej_nm9):
                            _dropped9 += 1                     # 未达标的整行不展示
                            continue
                        _hits9 = []                            # [(起点, 终点, 名字)] 互不重叠
                        for _nm9tp in _names9tp:
                            _i9 = _ln9tp.find(_nm9tp)
                            while _i9 >= 0:
                                _j9 = _i9 + len(_nm9tp)
                                if not any(_a9 < _j9 and _i9 < _b9 for _a9, _b9, _ in _hits9):
                                    _hits9.append((_i9, _j9, _nm9tp))
                                    break                      # 同名一行只挂一次(治DRAM双标)
                                _i9 = _ln9tp.find(_nm9tp, _j9)
                        for _a9, _b9, _nm9tp in sorted(_hits9, reverse=True):   # 从右往左插,位置不失效
                            _bd9 = _cert_badge9(_code_of9.get(_nm9tp, ""), _nm9tp)
                            if _bd9:
                                _sep9 = "&nbsp;" if _nm9tp[-1:].upper() == "C" else ""
                                _ln9tp = _ln9tp[:_b9] + _sep9 + _bd9 + _ln9tp[_b9:]
                        _kept9.append(_ln9tp)
                    _tp_html9 = "\n".join(_kept9)
                    if _dropped9:
                        _tp_html9 += (f"\n\n<span style='font-size:11px;color:#94a3b8'>"
                                      f"（另有{_dropped9}条未达V88规则闸已移出,见上方⏸未达标区）</span>")
                except Exception:
                    pass
                # unsafe_allow_html:预案文本由本系统生成,注入的是自家徽章HTML
                st.markdown(_tp_html9.replace('~', '～').replace('\n', '  \n'),
                            unsafe_allow_html=True)   # ~→全角:防markdown把价格区间解析成删除线
        except Exception:
            pass
        # 【V88·行动指令 2026-07-25 用户抓"没有推荐也没说该干什么"】每档开头一句话:
        # 该拿多少仓/主要任务是什么/盯什么事件——防守周也必须有作战任务,拒绝只给数据不给指令。
        try:
            _mi9 = _nwj9("macro_events.json")
            _d0i, _d1i = _cfg9["days"]
            _tdy9i = _dtnw.now().date()
            _evs9i = []
            for _e9i in (_mi9.get("events") or []):
                try:
                    _off9i = (_dtnw.strptime(_e9i["date"], "%Y-%m-%d").date() - _tdy9i).days
                    if _d0i <= _off9i <= max(_d1i, 1):
                        _evs9i.append(_e9i)
                except Exception:
                    continue
            _idc9i = (_nwj9("intraday_decisions.json").get("rows") or [])
            _prep9i = [r for r in _idc9i if str((r.get("entry_plan") or {}).get("mode") or "")
                       in ("现价可进", "回踩到位", "突破确认", "双路径待触发")]
            _cutn9i = len([r for r in _idc9i if r.get("scope") == "持仓" and any(
                k in str(r.get("action", "")) for k in ("减", "退", "清", "止损"))]) \
                if _tb_tier9 in ("今日", "本周") else \
                len([x for x in (_pt9n.get("stocks") or [])
                     if x.get("direction") == "down" and x.get("confidence") in ("高", "中")])
            _wk9i, _pos9i = [], []
            for _m9i in ("A股", "港股", "美股"):
                _b9i = (_nw_snap9.get("markets") or {}).get(_m9i) or {}
                _p9i = dict((x[0], x[1]) for x in ((_b9i.get("l3") or {}).get("probs") or [])).get(
                    _cfg9["mkt_hz"] or "2周")
                if _p9i is not None:
                    _wk9i.append((_m9i, int(_p9i)))
                _t9i = (_b9i.get("temperature") or {})
                if _t9i.get("position"):
                    _pos9i.append(f"{_m9i}{str(_t9i['position']).split('（')[0]}")
            # 统一裁决(与双门同一把尺 _v88_mkt_gate9x):弱市⏸️/过热🔶/良性✅/中性⚖️
            _mg9i = _v88_mkt_gate9x(_nw_repo9)
            _task9i = []
            _wkm9i = [m for m, g in _mg9i.items() if g.get("state") == "weak"]
            _hotm9i = [m for m, g in _mg9i.items() if g.get("state") == "hot"]
            _upm9i = [m for m, g in _mg9i.items() if g.get("state") == "up"]
            if _upm9i:
                _task9i.append(f"<b style='color:#dc2626'>✅{'/'.join(_upm9i)}良性·按纲领执行</b>")
            if _hotm9i:
                _task9i.append(f"<b style='color:#b45309'>🔶{'/'.join(_hotm9i)}过热·只限回踩不追高·仓位减半</b>")
            if _wkm9i:
                _task9i.append(f"<b style='color:#16a34a'>⏸️{'/'.join(_wkm9i)}拐点/偏弱·买单暂停执行·反弹减仓</b>")
            _task9i.append(f"🎯盯{len(_prep9i)}只到价触发单(见④准备买)" if _prep9i
                           else "买侧任务:等③埋伏名单转绿灯")
            if _cutn9i:
                _task9i.append(f"⚔️{_cutn9i}只按⑤纪律处理")
            if _evs9i:
                _top_ev9i = "、".join(f"{_e9i['date'][5:]}(周{_e9i['dow']}){_e9i['event']}"
                                     for _e9i in _evs9i[:3])
                _task9i.append(f"📅事件锚:{_top_ev9i}" + (f"等{len(_evs9i)}件" if len(_evs9i) > 3 else ""))
            st.markdown("<div style='background:#fef9c3;border-left:4px solid #ca8a04;border-radius:6px;"
                        "padding:.35rem .6rem;margin:.2rem 0 .4rem 0;font-size:13px'>"
                        f"<b>📌 {_tb_tier9}行动指令</b>　仓位:{('·'.join(_pos9i[:3])) or '见综述'}"
                        f"　{'　'.join(_task9i)}</div>", unsafe_allow_html=True)
        except Exception:
            try:
                _v88_sentinel9(_nw_repo9, "作战板行动指令")
            except Exception:
                pass
        _tbL9, _tbR9 = st.columns(2)
        with _tbL9:
            # ── ① 大盘{档}会怎样 ──
            _rows_a9 = []
            for _m9n in ("美股", "A股", "港股"):
                _b9n = (_nw_snap9.get("markets") or {}).get(_m9n) or {}
                _l9n = dict((x[0], x[1]) for x in ((_b9n.get("l3") or {}).get("probs") or []))
                _t9n = _b9n.get("temperature") or {}
                _cgn9 = float(((_b9n.get("indices") or [{}])[0] or {}).get("chg1d") or 0)
                if _cfg9["mkt_hz"] is None:          # 今日=实况
                    _cgc9n = "#dc2626" if _cgn9 > 0.05 else ("#16a34a" if _cgn9 < -0.05 else "#64748b")
                    _core9n = (f"今日<b style='color:{_cgc9n}'>{_cgn9:+.2f}%</b>"
                               f"·温度{_t9n.get('temp', '?')}°→仓位{str(_t9n.get('position', '?')).split('（')[0]}")
                else:
                    _pv9x = _l9n.get(_cfg9["mkt_hz"])
                    if _pv9x is None:
                        continue
                    _pv9n, _tag9n = int(_pv9x), (f"<span title='决策窗={_tb_tier9}(日历口径);数字=统一引擎{_cfg9['mkt_hz']}趋势因子分,非日历直译概率——双时间轴口径'>ⓘ</span>")
                    _pc9n = "#dc2626" if _pv9n >= 55 else ("#16a34a" if _pv9n <= 45 else "#64748b")
                    _w9n = ("偏涨·回踩敢接" if _pv9n >= 55 else
                            ("偏弱·反弹先减不追" if _pv9n <= 45 else "震荡·区间对待"))
                    _core9n = (f"{_tb_tier9}上涨概率<b style='color:{_pc9n}'>{_pv9n}%</b>→{_w9n}"
                               f"<span style='font-size:11px;color:#94a3b8'>{_tag9n}</span>")
                _rows_a9.append(f"<div style='font-size:13px'><b>{_m9n}</b> {_core9n}</div>")
            st.markdown(f"<b style='font-size:13px'>🧭 ① 大盘·{_tb_tier9}</b>"
                        "<span style='font-size:11px;color:#94a3b8'>（±1σ区间见🔮四档预判·为什么见📰综述）</span>"
                        + "".join(_rows_a9), unsafe_allow_html=True)
            # ── ② 板块轮转·{档} ──
            _rows_b9 = []
            for _m9n in ("美股", "A股", "港股"):
                _tl9n = ((_nw_snap9.get("rotation_forecast") or {}).get("trajectories") or {}).get(_m9n) or []
                if _cfg9["sec_hz"] is None:          # 今日=快照领涨领跌
                    _sc9n = (_nw_snap9.get("markets") or {}).get(_m9n, {}).get("sectors") or []
                    if _sc9n:
                        _u29 = sorted(_sc9n, key=lambda s: -(s.get("chg1d") or 0))[:2]
                        _d29 = sorted(_sc9n, key=lambda s: (s.get("chg1d") or 0))[:2]
                        _rows_b9.append(
                            f"<div style='font-size:12.5px'><b>{_m9n}</b> 领涨:"
                            + "、".join(f"{s['name']}{(s.get('chg1d') or 0):+.1f}%" for s in _u29)
                            + "｜弱:" + "、".join(f"{s['name']}{(s.get('chg1d') or 0):+.1f}%" for s in _d29)
                            + "</div>")
                    continue
                _hz9b = _cfg9["sec_hz"]
                _top9n = sorted(_tl9n, key=lambda t: -((t.get("points") or {}).get(_hz9b) or {}).get("score", 0))[:2]
                _near9n = [t.get("name") for t in _tl9n
                           if str((t.get("turning") or {}).get("horizon")) == _hz9b]
                if _top9n:
                    _rows_b9.append(
                        f"<div style='font-size:12.5px'><b>{_m9n}</b> {_hz9b}最强:"
                        + "、".join(f"{t.get('name')}({int(((t.get('points') or {}).get(_hz9b) or {}).get('score', 0))})"
                                    for t in _top9n)
                        + (f"　<b style='color:#dc2626'>⏰拐点≈{_hz9b}:{'、'.join(_near9n)}</b>" if _near9n else "")
                        + "</div>")
            st.markdown(f"<b style='font-size:13px'>🔄 ② 板块轮转·{_tb_tier9}</b>"
                        + ("<span style='font-size:11px;color:#b45309'>（轮动引擎最远16周·下季度沿用16周判断,"
                           "越远置信越低——两档板块行相同是引擎上限,非bug）</span>"
                           if _tb_tier9 == "下季度" else "")
                        + "<span style='font-size:11px;color:#94a3b8' title='热度Top2;红字=该档拐点要转向;下季度沿用16周判断(轮动引擎上限)'>ⓘ</span>"
                        + "".join(_rows_b9), unsafe_allow_html=True)
            # ── ③ 低位拐点·可注意埋伏(档感知:近档看日~周,远档看周~月) ──
            _hz_kw9 = ("日", "周") if _tb_tier9 in ("今日", "本周", "下周") else ("周", "月")
            _ups9n = sorted([x for x in (_pt9n.get("stocks") or [])
                             if x.get("direction") == "up" and x.get("confidence") in ("高", "中")
                             and float(x.get("pos52") or 50) <= 45 and _mkfit9(x.get("code"))],
                            key=lambda x: ({"高": 0, "中": 1}.get(str(x.get("confidence")), 2),
                                           float(x.get("pos52") or 50)))[:8]
            # 【V88·资金流💰标注 2026-07-25 用户批准接入③】低位+主力已进=埋伏成色更高(仅A股有数据)
            _ffs9 = (_nwj9("fund_flow.json").get("stocks") or {})

            def _ff_tag9(_cd9f):
                _n9f = (_ffs9.get(str(_cd9f)) or {}).get("net")
                if _n9f is None:
                    return ""
                return (f"<span style='font-size:11px;color:#b45309'>·💰主力+{_n9f:.1f}亿</span>" if _n9f >= 0.1
                        else (f"<span style='font-size:11px;color:#64748b'>·主力{_n9f:+.1f}亿</span>" if _n9f <= -0.1 else ""))
            _up_txt9 = ("、".join(f"{_nw_link9(x.get('name'), x.get('code'))}"
                                 f"<span style='font-size:11px;color:#64748b'>(52周{int(float(x.get('pos52') or 0))}%"
                                 f"·{x.get('confidence')}置信)</span>{_ff_tag9(x.get('code'))}" for x in _ups9n)
                        if _ups9n else "<span style='color:#94a3b8'>全池暂无低位高置信转强(门槛:52周位≤45%+相位向上)</span>")
            st.markdown("<b style='font-size:13px'>🌱 ③ 低位拐点·可注意埋伏</b>"
                        f"<span style='font-size:11px;color:#94a3b8' title='全池{_pt9n.get('scanned', '?')}只扫描;52周低位+相位转强;转强≠立刻买,等时机绿灯;💰=东财主力今日净流入'>ⓘ</span>"
                        f"<div style='font-size:12.5px'>{_up_txt9}</div>", unsafe_allow_html=True)
            # ── ③a 🌍全市场机会(2026-07-27 用户定纲"扩大股票池·熊市也有逆势股") ──
            try:
                _op9 = _nwj9("opportunity_scan.json")
                _op_candidates9 = [r for r in (_op9.get("exec") or []) if _mkfit9(r.get("code"))]
                _op_bk9 = [r for r in (_op9.get("blocked") or []) if _mkfit9(r.get("code"))]
                _op_ex9, _op_rows9 = [], []
                for _op_candidate9 in _op_candidates9:
                    _op_ok9, _op_reason9 = _v88_buy_gate9(_op_candidate9, _nw_repo9)
                    if _op_ok9:
                        _op_ex9.append(_op_candidate9)
                    else:
                        _op_bk9.append({**_op_candidate9, "block_reasons": [_op_reason9 or "GPT-6＋经典巨著未通过"]})
                if _op_ex9 or _op_bk9:
                    _op_rows9 = []
                    for _r9o in _op_ex9[:6]:
                        _exc9 = _r9o.get("exception")
                        _op_rows9.append(
                            f"<div style='font-size:12.5px'>✅ {_nw_link9(_r9o.get('name'), _r9o.get('code'))}"
                            f"<b style='color:#dc2626'>涨{_r9o.get('p_up')}%</b>"
                            f"<span style='font-size:11.5px;color:#475569'>·赔率{_r9o.get('rr')}"
                            f"·52周{int(float(_r9o.get('pos52') or 0))}%"
                            f"({'低位·空间大' if float(_r9o.get('pos52') or 100) <= 30 else '中位'})"
                            f"·买区{_r9o.get('buy_zone')}·止损{_r9o.get('stop')}</span>"
                            + (f"<span style='font-size:11px;color:#b45309'>·⚡逆势通道(≤1/3仓)</span>" if _exc9 else "")
                            + (f"<span style='font-size:11px;color:#b45309'>·{_r9o['crowd']}</span>" if _r9o.get("crowd") else "")
                            + f"<span style='font-size:11px;color:#94a3b8'>·{str(_r9o.get('source'))[:12]}"
                            f"{'·池内' if _r9o.get('in_pool') else '·池外新'}</span></div>")
                    # 【题材热点雷达 2026-07-28 用户"热点掌握不够"】概念板块层——A股的钱按题材走,
                    # 只看行业(电力设备涨2%)根本不知道今天在炒什么。涨幅榜∩资金榜分真假,
                    # 连续在榜天数分主线与一日游(主线第2天上车还有肉,一日游第2天就是接盘)。
                # 【2026-08-02 用户「我让你修改认证的,结果什么都没有了」追出的结构错】
                # 题材榜原先**嵌在 `if _op_ex9 or _op_bk9:` 分支里**——
                # 即机会扫描没结果时,整个题材榜(含蓄势榜/已在涨/GPT主验徽章)一起消失。
                # 而题材榜数据源是 hot_theme,与机会扫描毫无关系,不该受它约束。
                # 用户以为是我把功能删了,我也查了很久——**嵌套错位比逻辑错更难查**,
                # 因为代码本身没毛病,只是长在了错误的分支下。整体外移一级。
                try:
                    _htj9 = _nwj9("hot_theme.json")
                    # 【提前量优先 2026-07-28 用户"不要后知后觉"】蓄势榜排最前:
                    # 钱在进但价没动=还能上车;已在涨的降为副榜并标追高风险
                    # 【2026-08-02 用户"延伸的个股也要实现鼠标点击分析"】
                    # 题材梯队里的个股此前是纯文本,点不动——而它们恰恰是"该看哪只"的落点。
                    # 【2026-08-02 用户"这个深一些claude流量,用gpt为主验证"】
                    # 题材榜的裁决交 GPT 主验(theme_verify.py,一次调用验全部,12秒),
                    # Claude 上下文零消耗。裁决:主线/一日游/不否定。
                    _tv9 = (_nwj9("theme_verify.json").get("rows") or {})

                    def _tv_badge9(_nm):
                        _v = (_tv9.get(str(_nm)) or {})
                        _d = _v.get("verdict")
                        if not _d:
                            return ""
                        # 身份色恒定(G=青#0d9488,与3A模块同一套),裁决用文字区分——
                        # 用户定纲:"gpt的验证一定要有大写字母G,就像claude的C一样"
                        _op = "1" if _d in ("主线", "一日游") else ".45"
                        return (f"<span style='background:#0d9488;color:#fff;"
                                f"border-radius:3px;padding:0 5px;font-size:9.5px;"
                                f"font-weight:800;opacity:{_op};margin-left:3px;"
                                f"letter-spacing:.3px' "
                                f"title='GPT主验:{_v.get(chr(39)+chr(119)+chr(104)+chr(121)+chr(39), '')}'>"
                                f"G·{_d}</span>")

                    def _mem_link9(_m):
                        _c = str(_m.get("code") or "")
                        _t = f'{_m.get("name")}{(_m.get("chg") or 0):+.1f}%'
                        if not _c:
                            return _t
                        return (f'<a href="?q={_c}&focus=deep#v88-deep-analysis" '
                                f'target="_blank" rel="noopener" style="color:inherit;'
                                f'text-decoration:underline;text-underline-offset:2px">{_t}</a>')
                    _brw9 = (_htj9.get("brewing") or [])[:5]
                    if _brw9:
                        _bw_html9 = []
                        for _b9t in _brw9:
                            _mem9b = "、".join(_mem_link9(m)
                                              for m in (_b9t.get("members") or [])[:3])
                            _bw_html9.append(
                                f"<div style='font-size:12px;line-height:1.6'>"
                                f"<b style='color:#16a34a'>{_b9t.get('grade')}</b> "
                                f"<b>{_b9t.get('name')}</b>{_tv_badge9(_b9t.get('name'))}"
                                f"<span style='font-size:11.5px;color:#475569'>·{_b9t.get('why')}</span>"
                                f"<span style='font-size:11px;color:#64748b'>·梯队:{_mem9b}</span></div>")
                        st.markdown(
                            "<b style='font-size:13px'>🌱 ③a0 题材蓄势·钱进价未动(提前量)</b>"
                            "<span style='font-size:11px;color:#94a3b8' title='主力净流入排名靠前但当日涨幅<1.2%"
                            "=钱先进场价还没反应,这是还能上车的地方;连续净流入天数越多=潜伏越久。"
                            "与下方已在涨的副榜相反——那些是追高区'>ⓘ提前量·非追涨</span>"
                            + "".join(_bw_html9), unsafe_allow_html=True)
                    _hts9 = (_htj9.get("themes") or [])[:5]
                    if _hts9:
                        _ht_html9 = []
                        for _h9t in _hts9:
                            _kd9 = str(_h9t.get("kind") or "")
                            _kc9 = ("#dc2626" if "主线" in _kd9 else
                                    ("#b45309" if "真金" in _kd9 else "#94a3b8"))
                            _mem9 = "、".join(
                                _mem_link9(m) for m in (_h9t.get("members") or [])[:3])
                            _ht_html9.append(
                                f"<div style='font-size:12px;line-height:1.6'>"
                                f"<b style='color:{_kc9}'>{_kd9}</b> "
                                f"<b>{_h9t.get('name')}</b>{_tv_badge9(_h9t.get('name'))} "
                                f"<span style='color:#dc2626'>{(_h9t.get('chg') or 0):+.2f}%</span>"
                                + (f"<span style='color:#b45309'>·主力{_h9t['net_yi']:+.1f}亿</span>"
                                   if _h9t.get("net_yi") is not None else "")
                                + f"<span style='font-size:11px;color:#64748b'>·梯队:{_mem9}</span></div>")
                        _mine9 = _htj9.get("mine_in_hot") or []
                        st.markdown(
                            "<b style='font-size:13px'>🔥 ③a0b 已在涨的题材(副榜·追高需谨慎)</b>"
                            "<span style='font-size:11px;color:#94a3b8' title='东财495个概念板块:"
                            "涨幅榜∩主力净流入榜——只涨不进钱=情绪脉冲,涨且进钱=真金白银;"
                            "连续在榜天数区分主线与一日游。梯队=板块内涨幅前三,不只看一只领涨股'>"
                            f"（{str(_htj9.get('asof_note') or _htj9.get('generated_at', ''))[:18]} ⓘ）</span>"
                            + "".join(_ht_html9)
                            + (f"<div style='font-size:11.5px;color:#7c3aed;margin-top:2px'>"
                                   f"📌跟我有关:" + "、".join(
                                   f"{_nw_link9(h.get('name'), h.get('code') or '')}({h.get('theme')}·{h.get('rel')})"
                                   for h in _mine9[:4]) + "</div>" if _mine9 else ""),
                            unsafe_allow_html=True)
                except Exception as _ht_e9:
                    logging.exception(f"[V88] 题材热点渲染失败: {_ht_e9}")
                    # 【2026-08-02 用户"我让你修改认证的,结果什么都没有了"】
                    # 原先只写 logging,页面上一片空白且毫无提示——用户以为功能被删了。
                    # 同"不静默吞错"铁律:渲染失败必须在**页面上**说出来。
                    import traceback as _tb9
                    st.caption(f"⚠️ 题材榜渲染失败：{type(_ht_e9).__name__}: {str(_ht_e9)[:120]}")
                    with st.expander("异常详情（供排查）", expanded=False):
                        st.code(_tb9.format_exc()[-1500:])
                # 【2026-08-03 用户撤单】🐲③a1 龙虎榜已删。
                # 用户:"我没说过要恢复龙虎榜,3a已经能综合替代很多了,和3a重复的全部可以节省资源删掉了"。
                # 背景:我修作战板渲染链时把它一并"恢复"了——那是我自作主张,他从没要过。
                # 席位维度(机构/游资)3A 不含,但用户判定不值这份资源;dragon_board.py 同步下班表。
                # 代码见 git 844d59a^,要回来随时可取。
                # 【2026-08-03 修·同一条链的第二环】🌍全市场机会原写在龙虎榜的 except 体内,
                # 即"龙虎榜挂了才显示全市场机会"。整条链是:题材榜except→龙虎榜→龙虎榜except→全市场机会,
                # 每一段都藏在上一段的错误分支里,谁都不知道自己是靠前一个失败才活着的。
                # 铁律:**except 只放降级/报错,正常内容一律回主干**。
                st.markdown("<b style='font-size:13px'>🌍 ③a 全市场机会</b>"
                            f"<span style='font-size:11px;color:#94a3b8' title='四路候选(全池转强低位/涨停接力/行业代表/黑马)"
                            f"→实跑{_op9.get('studied', '?')}只→严格标准(赔率≥2.0·52周位≤55%·2周概率≥55%·启动型)→过环境闸;"
                            f"逆势通道=深度低位+超宽赔率在弱市可小仓试;全量入台账攒战绩'>"
                            f"（实跑{_op9.get('studied', '?')}只·可执行{len(_op_ex9)}·被闸{len(_op_bk9)} ⓘ）</span>"
                            + ("".join(_op_rows9) if _op_rows9 else
                               "<div style='font-size:12px;color:#94a3b8'>本档无可执行机会</div>")
                            + (f"<div style='font-size:11.5px;color:#94a3b8'>⏸被闸:"
                               + "、".join(f"{_nw_link9(r.get('name'), r.get('code') or '')}(赔率{r.get('rr')}·"
                                          f"{'/'.join((r.get('block_reasons') or ['环境不合格'])[:1])[:18]})"
                                          for r in _op_bk9[:4]) + "</div>" if _op_bk9 else ""),
                            unsafe_allow_html=True)
            except Exception:
                # 【2026-08-02 用户"我让你修改认证的,结果什么都没有了"】
                # 原为裸 `pass`——机会扫描段一旦抛错,**它后面的题材榜/龙虎榜全部静默消失**,
                # 页面上没有任何痕迹。我为此查了一小时,用户以为功能被删了。
                # 这是全系统"静默吞错"里最贵的一处:它吞掉的不是一个值,是整整三段内容。
                import traceback as _tb8
                logging.exception("[V88] 作战板中段渲染失败")
                st.caption("⚠️ 作战板中段渲染失败（题材榜/龙虎榜受影响）")
                with st.expander("异常详情（供排查）", expanded=True):
                    st.code(_tb8.format_exc()[-2000:])
            # ── ③b ⏳拐点倒计时(U3·2026-07-26 用户批准"拐点高低前后的预计") ──
            # 【小米案 2026-07-27】✅预警兑现回响置顶——07-26预警小米见底,07-27+8%兑现,
            # 但用户全程不知道系统说对过。赢了必须让用户看见=事前提醒的证据链。
            try:
                _hitj9 = _nwj9("turning_triggers.json")
                _hits9 = [h for h in (_hitj9.get("rows") or [])
                          if str(h.get("date", "")) >= (_dtnw.now() - _tdnw(days=5)).strftime("%Y-%m-%d")]
                if _hits9:
                    st.markdown(
                        "<div style='background:#f0fdf4;border:1px solid #86efac;border-radius:8px;"
                        "padding:5px 9px;margin:4px 0'><b style='font-size:12.5px;color:#15803d'>✅ 预警兑现"
                        "</b><span style='font-size:10.5px;color:#94a3b8' title='拐点预警的确认价被价格穿越=预判应验;完整战绩窗口期末在预测台账正式核算(含最大有利/不利幅度)'>ⓘ证据链</span>"
                        # 【人话说明 2026-07-27 用户"小米这样标识是什么意思"】一行讲清这卡在说什么
                        "<div style='font-size:11px;color:#64748b;margin:1px 0 3px'>"
                        "＝系统事前预判说对了的存证：某天预警它可能见底/见顶并给出确认价，"
                        "后来价格真的穿过了那个价——第几天兑现、距预警涨跌多少，都记在这里</div>"
                        + "".join(
                            f"<div style='font-size:12px'>{'🌱' if h.get('side') == '见底' else '🔺'}"
                            f"<b>{_nw_link9(h.get('name'), h.get('code') or '')}</b> {h.get('warned')}预警{h.get('side')}"
                            f"(强度{h.get('prob')})→<b style='color:#15803d'>第{h.get('days')}天"
                            f"{'站上' if h.get('side') == '见底' else '跌破'}{h.get('confirm_price')}兑现</b>"
                            + (f"<span style='color:#475569'>·距预警{h.get('move_pct'):+.1f}%</span>"
                               if h.get("move_pct") is not None else "") + "</div>"
                            for h in _hits9[:4]) + "</div>", unsafe_allow_html=True)
            except Exception:
                pass
            try:
                _tfj9 = _nwj9("turning_forecast.json")
                _tf_rows9 = (_tfj9.get("rows") or [])
                _tf_rows9 = [r for r in _tf_rows9 if _mkfit9(r.get("code") or "")] or _tf_rows9[:0]
                # 过闸:被机检/人工否掉的标的不进拐点名单(宽基错杀那批就是从这漏出去的)
                _tf_rows9 = [r for r in _tf_rows9 if str(r.get("code") or "") not in _cs_rej9]
                # 🚀启动候选(底拐≥60+52周≤35)排最前——小米画像不再被挤出名单
                _tf_rows9.sort(key=lambda r: (0 if r.get("launch_candidate") else 1))
                if _tf_rows9:
                    _tf_html9 = []
                    for _r9t in _tf_rows9[:6]:
                        _ic9t = "🔺" if _r9t.get("side") == "top" else ("🚀" if _r9t.get("launch_candidate") else "🌱")
                        _cl9t = "#16a34a" if _r9t.get("side") == "top" else "#dc2626"
                        _tf_html9.append(
                            f"<div style='font-size:12.5px'>{_ic9t} "
                            f"{_nw_link9(_r9t.get('name'), _r9t.get('code') or '')}"
                            + ("<span style='background:#fef3c7;color:#b45309;font-size:10px;"
                               "border-radius:4px;padding:0 4px;margin-right:3px' "
                               "title='底部拐点强度≥60+52周低位≤35%=暴涨候选画像(07-26小米即此形态);站上确认价才算数,买入仍过环境闸'>🚀启动候选</span>"
                               if _r9t.get("launch_candidate") else "")
                            + f"<b style='color:{_cl9t}'>预计{_r9t.get('window_days', ['?', '?'])[0]}～"
                            f"{_r9t.get('window_days', ['?', '?'])[1]}个交易日内"
                            f"{'可能见顶' if _r9t.get('side') == 'top' else '可能见底'}·强度{_r9t.get('prob')}/100"
                            f"<span style='font-weight:400;font-size:11px'>({'强预警' if int(_r9t.get('prob') or 0) >= 70 else ('值得留意' if int(_r9t.get('prob') or 0) >= 55 else '弱信号仅记录')})</span></b>"
                            f"<span style='font-size:11.5px;color:#475569'>·{'跌破' if _r9t.get('side') == 'top' else '站上'}{_r9t.get('confirm_price')}才算数"
                            f"<span style='font-size:11px;color:#94a3b8'>(没{'跌破' if _r9t.get('side') == 'top' else '站上'}=只是提高警惕,不是{'卖' if _r9t.get('side') == 'top' else '买'}信号)</span>"
                            f"{('(' + str(_r9t.get('proxy')) + ')') if _r9t.get('proxy') else ''}"
                            f"<span style='font-size:11px;color:#94a3b8'>·{'涨速在衰减=油门在松' if _r9t.get('side') == 'top' else '跌速在收敛=刹车在踩'}"
                            f"·52周位{int(float(_r9t.get('pos52') or 50))}%"
                            f"({'高位·下行空间大' if float(_r9t.get('pos52') or 50) > 70 else ('低位·上行空间大' if float(_r9t.get('pos52') or 50) < 30 else '中位')})</span>"
                            + (f"<span style='font-size:11px;color:#b45309'>·⚡{_r9t['event'][:30]}</span>"
                               if _r9t.get("event") else "")
                            + ((lambda _ac: f"<span style='font-size:11px;color:{'#b45309' if _ac.startswith('存疑') else '#0891b2'}'>"
                                            f"·🤖AI复核:{_ac}</span>")(str(_r9t.get("ai_check")))
                               if _r9t.get("ai_check") else "") + "</div>")
                    st.markdown("<b style='font-size:13px'>⏳ ③b 拐点倒计时</b>"
                                "<span style='font-size:11px;color:#94a3b8' title='影子模式:不参与买卖名单;数字=信号强度/100非概率(样本≥50报命中率,≥100升概率口径);窗口=交易日;确认价过线才算数;每条已入预测台账到期核算攒样本晋级'>ⓘ影子·强度制</span>"
                                # 【图标说明 2026-07-27 用户"绿叶子图标代表什么也不知道"】图标不配说明=天书
                                "<div style='font-size:11px;color:#94a3b8;margin:1px 0 2px'>"
                                "🌱=可能见底(跌势要停了)　🔺=可能见顶(涨势要停了)　"
                                "🚀=启动候选(底部信号强+位置低,暴涨前兆画像)</div>"
                                + "".join(_tf_html9), unsafe_allow_html=True)
            except Exception:
                pass
            # ── ③c 📡前置信号(U3·钱先动价后动) ──
            try:
                _psj9 = _nwj9("pre_signals.json")
                _ps_html9 = []
                for _f9p in (_psj9.get("fund_lead") or [])[:3]:
                    _ps_html9.append(f"<div style='font-size:12px'>💰蓄势:<b>{_nw_link9(_f9p.get('name'), _f9p.get('code') or '')}</b>"
                                     f"<span style='color:#475569'>·{str(_f9p.get('note'))[:40]}</span>"
                                     f"<span style='font-size:11px;color:#94a3b8'>(钱先进·价未动=好苗头,等启动别追)</span></div>")
                for _e9p in (_psj9.get("earn_gap") or [])[:3]:
                    _cl9p = "#16a34a" if _e9p.get("bias") == "risk" else "#dc2626"
                    _ps_html9.append(f"<div style='font-size:12px'>📊<b style='color:{_cl9p}'>"
                                     f"{str(_e9p.get('name'))[:20]}</b>"
                                     f"<span style='color:#475569'>·{str(_e9p.get('note'))[:42]}</span></div>")
                for _p9p in (_psj9.get("policy") or [])[:2]:
                    _ps_html9.append(f"<div style='font-size:12px'>🏛{_p9p.get('name')}"
                                     f"<span style='color:#475569;font-size:11px'>·{str(_p9p.get('note'))[:36]}</span></div>")
                if _ps_html9:
                    st.markdown("<b style='font-size:13px'>📡 ③c 前置信号</b>"
                                "<span style='font-size:11px;color:#94a3b8' title='影子模式;蓄势=主力资金连续流入但热度未起(仅A股,板块涨幅直读待接);漂移=财报前5日价格位移(真预期差需EPS一致预期/期权隐波,下批);台账攒样本晋级'>ⓘ影子·事前预警</span>" + "".join(_ps_html9), unsafe_allow_html=True)
            except Exception:
                pass
        with _tbR9:
            # ── ④ {档}买什么(每档最优口径) ──
            def _hz_score9(_h, _hz):
                try:
                    return float((((_h.get("facts") or {}).get("horizons") or {}).get(_hz) or {}).get("rule_score"))
                except (TypeError, ValueError):
                    return None
            _buy9n, _buy_note9 = [], ""
            # 【V88·发言权规则 2026-07-25 用户批准】黑马实盘<50%(n≥5)→作战板买名单撤点名
            try:
                _dv9n = ((_nwj9("success_rates.json").get("types") or {}).get("darkhorse") or {})
                _dh_gate9n = int(_dv9n.get("n") or 0) >= 5 and (_dv9n.get("rate") or 100) < 50
            except Exception:
                _dh_gate9n = False
            for _h9n in ([] if _dh_gate9n else (_dh9n.get("horses") or [])):
                if not _mkfit9(_h9n.get("code")):
                    continue
                _md9n = str(((_h9n.get("trade_plan") or {}).get("short") or {}).get("mode") or "")
                # 作战板“买什么”与首页/飞书共用可执行闸，认证徽章只展示、不能替代过滤。
                _qok9n, _qwhy9n = _v88_buy_gate9(_h9n, _nw_repo9)
                if _cfg9["buy"] == "green" and _qok9n and _md9n in ("现价可进", "回踩到位", "突破确认", "左侧低吸"):
                    _buy9n.append((_h9n, int(_h9n.get("p_up") or 0), _md9n))
                elif _cfg9["buy"] == "trigger" and _qok9n and _md9n in ("双路径待触发", "回踩到位", "突破确认"):
                    _buy9n.append((_h9n, int(_h9n.get("p_up") or 0), f"明日盯触发·{_md9n}"))
                elif _cfg9["buy"] == "green+" and _qok9n and _md9n in ("现价可进", "回踩到位", "突破确认", "双路径待触发"):
                    _buy9n.append((_h9n, int(_h9n.get("p_up") or 0), _md9n))
                elif _cfg9["buy"] in ("w4", "hz") and _qok9n:
                    _hz9b = _cfg9.get("hz") or "4周"
                    _s9n = _hz_score9(_h9n, _hz9b)
                    if _s9n is not None and _s9n >= 58:
                        _buy9n.append((_h9n, int(_s9n), f"{_hz9b}分{int(_s9n)}·蓄势"))
            _buy_note9 = {"green": "时机绿灯·盘中可执行", "trigger": "触发型·到价即进不猜",
                          "green+": "绿灯+双路径5日窗", "w4": "4周周期走强·分批布局",
                          "hz": f"{_cfg9.get('hz')}周期走强·左侧分批"}[_cfg9["buy"]]
            _buy9n.sort(key=lambda x: -x[1])
            # 【双剑认证徽章 2026-08-02 用户定纲】作战板与3A大系统此前是两条独立管道:
            # 它只有一道"黑马实盘胜率<50%整体撤名单"的发言权闸,**没有任何双剑验证**。
            # 用户:"也要进行双c认证,只不过这里面不用去除,这里面只能有两者认证和c认证的两种即可"
            # → 不做删减(与推荐位不同),只给每条挂标记:⚔️双认证 / 🅒C认证 / ·待验。
            try:
                from grade_card import cert_map as _cmap_f, cert_badge as _cbadge
                _CM9 = _cmap_f(_nwj9("rank_score.json"))
            except Exception:
                _CM9, _cbadge = {}, (lambda c, m: "")
            _rows_c9 = ["<div style='font-size:12.5px'>"
                        f"{_nw_link9(_h9x.get('name'), _h9x.get('code'))}"
                        f"{_cbadge(_h9x.get('code'), _CM9)}"
                        f"<b style='color:#dc2626'>{_p9x}%</b>"
                        f"<span style='font-size:11.5px;color:#475569'>·{_tag9x}"
                        f"·{str(((_h9x.get('trade_plan') or {}).get('short') or {}).get('in') or '')[:40]}</span></div>"
                        for _h9x, _p9x, _tag9x in _buy9n[:6]]
            # 【V88·准备买 2026-07-25 用户抓"没有推荐"】"现在可买0只"≠没任务——自选池里
            # 挂着触发价的票就是"准备买"名单(到价即动,提前启动)。源=自选决策entry_plan,
            # 挂entry_green台账(积累中=有发言权),不受黑马战绩连坐。
            # 【V88·准备买分档 2026-07-25 用户抓"下周/本月/下月一模一样"】3天触发单只属于
            # 今日~下周档;本月档=4周周期分≥58(mid计划),下月档=长线分≥58(long计划)——档位口径贯穿到底。
            _idc9n_pre = (_nwj9("intraday_decisions.json").get("rows") or [])
            def _rhz9p(_r9z, _hz9z):
                try:
                    return float((((_r9z.get("facts") or {}).get("horizons") or {}).get(_hz9z) or {}).get("rule_score"))
                except (TypeError, ValueError):
                    return None
            _idc9n_pre = [r for r in _idc9n_pre if _mkfit9(r.get("code"))]
            if _cfg9.get("hz"):
                _hz9pp = _cfg9["hz"]
                _tk9p = "mid_text" if _hz9pp in ("4周", "8周") else "_hz_detail"
                _prep9n = sorted([r for r in _idc9n_pre if (_rhz9p(r, _hz9pp) or 0) >= 58],
                                 key=lambda r: -(_rhz9p(r, _hz9pp) or 0))
                _sk9p = "_hz_" + _hz9pp
                _prep_lbl9 = f"{_hz9pp}周期分≥58·左侧分批(区间见明细)"
                # 【V88·季度档明细 2026-07-25 用户抓"本季度和下季度基本一致"】季度两档弃用
                # 只有一种口径的long_text(还曾与85分打架的谎报文本),改用该档horizons档内数据
                # 自拼——支撑/压力/期内涨幅/回撤随档不同,两档文本天然分化;并给双档分对照。
                if _hz9pp in ("16周", "32周"):
                    _other9q = "32周" if _hz9pp == "16周" else "16周"

                    def _hz_detail9(_r9q):
                        _h9q = (((_r9q.get("facts") or {}).get("horizons") or {}).get(_hz9pp) or {})
                        _o9q = _rhz9p(_r9q, _other9q)
                        _bits9q = [str(_h9q.get("rule_view") or "")]
                        if _h9q.get("support") and _h9q.get("resistance"):
                            _bits9q.append(f"支撑{float(_h9q['support']):g}/压力{float(_h9q['resistance']):g}")
                        if _h9q.get("return_pct") is not None:
                            _bits9q.append(f"近{_hz9pp}涨{float(_h9q['return_pct']):+.0f}%")
                        if _h9q.get("drawdown_pct") is not None:
                            _bits9q.append(f"回撤{float(_h9q['drawdown_pct']):+.1f}%")
                        if _h9q.get("rule_confidence"):
                            _bits9q.append(f"置信{_h9q['rule_confidence']}")
                        if _o9q is not None:
                            _bits9q.append(f"(对照{_other9q}分{int(_o9q)})")
                        if _h9q.get("phase_note"):
                            _bits9q.append(str(_h9q["phase_note"]))
                        return "·".join(x for x in _bits9q if x)
            else:
                _prep9n = sorted([r for r in _idc9n_pre
                                  if str((r.get("entry_plan") or {}).get("mode") or "")
                                  in ("现价可进", "回踩到位", "突破确认", "双路径待触发")],
                                 key=lambda r: -(r.get("p_up") or 0))
                _sk9p, _tk9p, _prep_lbl9 = "p_up", "short_text", "自选池触发单·到价即动"
            # “准备买”仍须通过同一可执行闸。未过闸只进入条件观察，不得用准备买措辞。
            _prep_ready9, _prep_pending9 = [], []
            for _r9p in _prep9n:
                _ok9p, _why9p = _v88_buy_gate9(_r9p, _nw_repo9)
                (_prep_ready9 if _ok9p else _prep_pending9).append((_r9p, _why9p))
            # 每票挂所在市场统一裁决标(⏸️弱市触发单=到价也先不执行,等大盘回中性)
            _mg9p = _v88_mkt_gate9x(_nw_repo9)

            def _pol9p(_cd9p):
                _c9p = str(_cd9p or "").upper()
                _mk9p = "A股" if (_c9p.endswith(".SS") or _c9p.endswith(".SZ")) else (
                    "港股" if _c9p.endswith(".HK") else "美股")
                _st9p = (_mg9p.get(_mk9p) or {}).get("state")
                return {"weak": "<span style='font-size:11px;color:#16a34a'>⏸️弱市·到价也等大盘回中性</span>",
                        "hot": "<span style='font-size:11px;color:#b45309'>🔶只限回踩·仓位减半</span>"}.get(_st9p, "")
            _prep_rows9 = ["<div style='font-size:12.5px'>"
                           f"{_nw_link9(_r9p.get('name'), _r9p.get('code'))}"
                           f"{_cbadge(_r9p.get('code'), _CM9)}"
                           f"<b style='color:#dc2626'>{int((_r9p.get(_sk9p) if not _sk9p.startswith('_hz_') else _rhz9p(_r9p, _cfg9.get('hz'))) or 0)}{'%' if _sk9p == 'p_up' else '分'}</b>"
                           f"<span style='font-size:11.5px;color:#475569'>"
                           f"{'·' + str((_r9p.get('entry_plan') or {}).get('mode') or '') if _sk9p == 'p_up' else ''}"
                           f"·{(_hz_detail9(_r9p)[:76] if _tk9p == '_hz_detail' else str((_r9p.get('entry_plan') or {}).get(_tk9p) or '')[2:60])}</span>"
                           f"{_pol9p(_r9p.get('code'))}</div>"
                           for _r9p, _why9p in _prep_ready9[:5]]
            _pending_rows9 = ["<div style='font-size:12px;color:#64748b'>"
                              f"{_nw_link9(_r9p.get('name'), _r9p.get('code'))}"
                              f"<span>·待验证：{str(_why9p)[:48]}</span></div>"
                              for _r9p, _why9p in _prep_pending9[:5]]
            # 【2026-08-16 界面修正·空栏给证据】前置读取,带独立保护——失败只丢数字,
            # 绝不让空栏文案把④区块炸掉。此处早于 _v88_success9 的 def,只能就地读文件。
            try:
                _eg_succ9 = ((json.loads((_cb_repo9 / "data" / "success_rates.json")
                                         .read_text(encoding="utf-8")).get("types") or {})
                             .get("entry_green") or {})
            except Exception:
                _eg_succ9 = {}
            st.markdown(f"<b style='font-size:13px'>🐉 ④ {_tb_tier9}确认买 / 条件观察</b>"
                        f"<span style='font-size:11px;color:#94a3b8' title='{_buy_note9};带买点价'>"
                        f"{'等' + str(len(_buy9n)) + '只' if len(_buy9n) > 6 else str(len(_buy9n)) + '只'} ⓘ</span>"
                        + ("".join(_rows_c9) if _rows_c9
                           else ("<div style='font-size:12.5px;color:#94a3b8'>🔇黑马池实盘<50%已降级研究参考——"
                                 "点名暂停,完整名单在🐴黑马雷达模块(战绩回升自动恢复)</div>" if _dh_gate9n else
                                 # 【2026-08-16 界面修正·空栏给证据】"0只"不是程序坏了:把从严原因和实盘战绩
                                 # 直接写出来,让空仓成为看得见的决策而不是疑心病。数据来自 success_rates.json。
                                 # 注意:此处(约5270行)早于 _v88_success9 的 def(15435行),不能调它——
                                 # 就地读文件(pyflakes undefined-name 会抓,铁律6),读不到就不带数字。
                                 (lambda _eg9: "<div style='font-size:12.5px;color:#94a3b8'>现在可进0只——"
                                    "严选闸正常工作中"
                                    + (f"：近30天绿灯命中率仅{_eg9.get('rate')}%"
                                       f"·平均{_eg9.get('avg')}%" if _eg9.get("n") else "")
                                    + "，故从严。空仓等待也是决策,下面准备买名单到价即动</div>")(_eg_succ9)))
                        + (f"<div style='font-size:12px;margin-top:3px'><b style='color:#b45309'>🎯 准备买"
                           f"({len(_prep_ready9)}只)<span style='font-weight:400;font-size:11px;color:#94a3b8' title='{_prep_lbl9}·已过统一复核闸·到价再确认'>ⓘ</span></b>"
                           + (lambda _ov9q: (f"<span style='font-size:11px;color:#94a3b8'>"
                                             # 【2026-08-16 界面修复·口径一致】分子只数过闸票,分母同步改为过闸数,
                                             # 不再出现"屏上5只却写/12只"的对不上。
                                             f"·与{_ov9q[0]}榜重合{_ov9q[1]}/{len(_prep_ready9)}只——16与32周趋势"
                                             "一致属长周期常态,差异看各档分数/支撑压力对照</span>")
                              if _cfg9.get("hz") in ("16周", "32周") and _ov9q[1] else "")(
                               (("下季度" if _cfg9.get("hz") == "16周" else "本季度"),
                                len([r for r, _ in _prep_ready9 if (_rhz9p(r, "32周" if _cfg9.get("hz") == "16周"
                                                            else "16周") or 0) >= 58])
                                if _cfg9.get("hz") in ("16周", "32周") else 0))
                           + "</div>" + "".join(_prep_rows9)
                           if _prep_rows9 else
                           "<div style='font-size:12px;color:#94a3b8;margin-top:3px'>🎯准备买:当前没有通过统一复核闸的触发单。</div>")
                        + (f"<div style='font-size:12px;margin-top:4px'><b style='color:#64748b'>🔭 条件观察"
                           f"({len(_prep_pending9)}只·不可执行)</b></div>" + "".join(_pending_rows9)
                           if _pending_rows9 else ""),
                        unsafe_allow_html=True)
            # ── ⑤ {档}卖/持仓处理 ──
            _idc9n = _nwj9("intraday_decisions.json")
            if _tb_tier9 in ("今日", "本周"):
                _cut9n = [(r.get("name"), int(r.get("p_down") or 0),
                           str(r.get("reason") or r.get("action") or "")[:14], r.get("code"))
                          for r in (_idc9n.get("rows") or [])
                          if r.get("scope") == "持仓" and _mkfit9(r.get("code"))
                          and any(k in str(r.get("action", "")) for k in ("减", "退", "清", "止损"))]
                _cut_note9 = "盘中落盘减仓警示·卖点价在持仓卡💰行"
            elif _tb_tier9 == "下周":
                _cut9n = [(x.get("name"), int(float(x.get("strength") or 0)),
                           f"{x.get('phase')}·{x.get('confidence')}置信", x.get("code"))
                          for x in (_pt9n.get("stocks") or [])
                          if x.get("direction") == "down" and x.get("confidence") in ("高", "中")
                          and _mkfit9(x.get("code"))]
                _cut_note9 = "全池相位转弱(派发→退潮)·中长线躲避参考"
            else:
                # 【V88·卖侧分档 2026-07-25 七档版】本月4周/下月8周/本季度16周/下季度32周分≤45
                _hz9cc = _cfg9.get("hz") or "4周"
                _cut9n = [(r.get("name"), int(_rhz9p(r, _hz9cc) or 0),
                           f"{_hz9cc}分{int(_rhz9p(r, _hz9cc) or 0)}·周期走弱"
                           + ("·💼持仓" if r.get("scope") == "持仓" else ""), r.get("code"))
                          for r in (_nwj9("intraday_decisions.json").get("rows") or [])
                          if (_rhz9p(r, _hz9cc) or 100) <= 45 and _mkfit9(r.get("code"))]
                _cut_note9 = (f"{_hz9cc}周期分≤45·该档周期逻辑走弱"
                              "(与下周档相位名单口径不同,分数=统一引擎翻倍律)")
            # 【2026-07-27 用户"没通过的还能展示"】作战板卖名单同样过闸:
            # 机检未达标(CS1宽基错杀/CS2涨率过半等)与人工异议的一律不列
            _cut9n = [x for x in _cut9n if str(x[3] or "") not in _cs_rej9]
            # 近档=概率/强度越高越危险(降序);本月/下月=周期分越低越危险(升序)
            _cut9n.sort(key=lambda x: (x[1] if _cfg9.get("hz") else -x[1]))
            _rows_d9 = [f"<div style='font-size:12.5px'>{_nw_link9(_n9n, _c9n4) if _c9n4 else _n9n}"
                        f"<b style='color:#16a34a'>{_p9n}{'%' if _tb_tier9 in ('今日', '明日', '本周') else '强度'}</b>"
                        f"<span style='font-size:11.5px;color:#64748b'>·{_w9n2}</span></div>"
                        for _n9n, _p9n, _w9n2, _c9n4 in _cut9n[:6]]
            st.markdown(f"<b style='font-size:13px'>⚔️ ⑤ {_tb_tier9}卖/持仓要处理</b>"
                        f"<span style='font-size:11px;color:#94a3b8' title='{_cut_note9}'>"
                        f"{'等' + str(len(_cut9n)) + '只' if len(_cut9n) > 6 else str(len(_cut9n)) + '只'} ⓘ</span>"
                        + ("".join(_rows_d9) if _rows_d9
                           else "<div style='font-size:12.5px;color:#94a3b8'>本档暂无警示——按各卡💰卖点纪律执行</div>"),
                        unsafe_allow_html=True)
            # Current industry samples reuse central scores and original contracts.
            try:
                from sector_reps_view import html as _sector_reference_html
                st.markdown(_sector_reference_html(_nwj9("sector_reps.json"),market=_tb_mkt9,
                    factpack_id=(_nwj9("triad_selection_pub.json") or {}).get("factpack_id"),
                    stock_link=_nw_link9),unsafe_allow_html=True)
            except Exception:
                _v88_sentinel9(_nw_repo9,"行业参考样本")
                st.caption("行业样本暂不可用；当前评级与原合同见3A中央列表。")
            # 【V88·上下榜对账 2026-07-25 用户点单"新上榜和下榜要有补充说明"】与绿灯留痕
            # 同款:每档买/卖名单落盘跨日对比——新上榜说"为什么来了",下榜说"为什么走了",
            # 名单变动不许无声蒸发。同日内多次刷新只更新last,不动prev(对账基准=上一个交易日)。
            try:
                _bl_fp9 = _nw_repo9 / "data" / "tb_board_log.json"
                try:
                    _bl9 = json.loads(_bl_fp9.read_text(encoding="utf-8"))
                except Exception:
                    _bl9 = {}
                _tdy9bl = _dtnw.now().strftime("%Y-%m-%d")
                _cur9bl = {"buy": {}, "cut": {}}
                for _h9bl, _s9bl, _t9bl in _buy9n:
                    _cur9bl["buy"][str(_h9bl.get("name"))] = int(_s9bl)
                for _r9bl in _prep9n[:5]:
                    _sv9bl = (_r9bl.get("p_up") if not _sk9p.startswith("_hz_")
                              else _rhz9p(_r9bl, _cfg9.get("hz")))
                    _cur9bl["buy"].setdefault(str(_r9bl.get("name")), int(_sv9bl or 0))
                for _n9bl, _p9bl, _w9bl, _c9bl in _cut9n:
                    _cur9bl["cut"][str(_n9bl)] = int(_p9bl)
                _tier_log9 = _bl9.get(_tb_tier9) or {}
                _last9bl = _tier_log9.get("last") or {}
                if _last9bl.get("date") and _last9bl["date"] != _tdy9bl:
                    _tier_log9["prev"] = _last9bl
                _tier_log9["last"] = {"date": _tdy9bl, **_cur9bl}
                _bl9[_tb_tier9] = _tier_log9
                _bl_fp9.write_text(json.dumps(_bl9, ensure_ascii=False, indent=1), encoding="utf-8")
                _prev9bl = _tier_log9.get("prev") or {}
                if _prev9bl:
                    _chg9bl = []
                    for _kind9, _lbl9b, _thr9b in (("buy", "买榜", "≥58/绿灯"), ("cut", "卖榜", "≤45/警示")):
                        _pv9bl = _prev9bl.get(_kind9) or {}
                        _cv9bl = _cur9bl.get(_kind9) or {}
                        for _n9b2 in list(_cv9bl):
                            if _n9b2 not in _pv9bl:
                                _chg9bl.append(f"🆕{_lbl9b}新上:{_n9b2}(分{_cv9bl[_n9b2]}·新达标{_thr9b})")
                        for _n9b2 in list(_pv9bl):
                            if _n9b2 not in _cv9bl:
                                _chg9bl.append(f"⬇️{_lbl9b}下榜:{_n9b2}(上次分{_pv9bl[_n9b2]}→"
                                               "已不达门槛/窗口关闭,已进场按原计划非翻案)")
                    if _chg9bl:
                        st.markdown("<div style='font-size:12px;color:#475569;background:#f8fafc;"
                                    "border-radius:6px;padding:.25rem .5rem'>"
                                    f"📋 <b>{_tb_tier9}榜单变动</b>(对比{_prev9bl.get('date', '上期')}): "
                                    + "　".join(_chg9bl[:8]) + "</div>", unsafe_allow_html=True)
                    else:
                        st.caption(f"📋 {_tb_tier9}榜单与{_prev9bl.get('date', '上期')}一致·无进出")
            except Exception:
                pass
            # ── ⑥ {档}事件与消息面(财报按档期过滤+机构对应档观点) ──
            _ev9n = []
            _seen_ev9 = set()
            _d0, _d1 = _cfg9["days"]
            _today9e = _dtnw.now().date()
            for _h9n in (_dh9n.get("horses") or []) + (_dh9n.get("runners") or []):
                for _s9n in (_h9n.get("sources") or []):
                    _s9s = str(_s9n)
                    if "财报" in _s9s and _h9n.get("name") not in _seen_ev9:
                        try:
                            _md9e = _s9s.split("财报")[1][:5]
                            _dt9e = _dtnw.strptime(f"{_today9e.year}-{_md9e}", "%Y-%m-%d").date()
                            _off9e = (_dt9e - _today9e).days
                            if not (_d0 <= _off9e <= max(_d1, 1)):
                                continue
                        except Exception:
                            pass
                        _seen_ev9.add(_h9n.get("name"))
                        _ev9n.append(f"📊{_nw_link9(_h9n.get('name'), _h9n.get('code'))}"
                                     f"财报{_s9s.split('财报')[1][:5]}")
            # 【V88·宏观日历 2026-07-25】FOMC/非农/巨头+池内财报置顶——"下周市场在干什么"的骨架
            try:
                for _e9m in reversed(_evs9i if "_evs9i" in dir() else []):
                    _ev9n.insert(0, f"<b style='color:#7c2d12'>🏦{_e9m['date'][5:]}(周{_e9m['dow']})"
                                    f"{_e9m['event']}</b><span style='font-size:11px;color:#64748b'>"
                                    f"·{_e9m['note'][:22]}</span>")
            except Exception:
                pass
            _ann9n = _nwj9("announcements.json")
            for _cb9n in (_ann9n.get("cb_calendar") or [])[:3]:
                # 转债可点:链到正股深度分析(转债价值锚=正股;stock_code字段来自东财转债日历)
                _bond_nm9 = f"转债{_cb9n.get('bond')}"
                _bond_html9 = (_nw_link9(_bond_nm9, _cb9n.get("stock_code"))
                               if _cb9n.get("stock_code") else _bond_nm9)
                _ev9n.append(f"🆕{_bond_html9}({str(_cb9n.get('apply_date'))[5:]}申购"
                             + ("⭐池内" if _cb9n.get("in_pool") else "") + "·点看正股)")
            _ai9n = (_nwj9("institutional_signals.json").get("ai_brief") or {})
            _inst_wk9 = []
            for _m9n in ("A股", "港股", "美股"):
                _sub9n = _ai9n.get(_m9n) or {}
                _v9e = _sub9n.get(_cfg9["evt"]) if isinstance(_sub9n, dict) else None
                if _v9e and "材料不足" not in str(_v9e):
                    _inst_wk9.append(f"{_m9n}:{_v9e}")
            for _p9n2 in (_nwj9("intel_feed.json").get("policy") or [])[:2]:
                _ev9n.append(f"📜{str(_p9n2.get('title'))[:22]}({_p9n2.get('src')})")
            st.markdown(f"<b style='font-size:13px'>📅 ⑥ {_tb_tier9}事件与消息面</b>"
                        "<span style='font-size:11px;color:#94a3b8' title='财报按档期过滤;出处:Nasdaq/东财/发改委/研报库'>ⓘ</span>"
                        + ("<div style='font-size:12.5px'>" + "　".join(_ev9n[:8]) + "</div>" if _ev9n
                           else "<div style='font-size:12.5px;color:#94a3b8'>本档暂无已捕捉硬事件(财报源覆盖未来7天)</div>")
                        + (f"<div style='font-size:12px;color:#7c3aed'>🏛️机构{_cfg9['evt']}观点:"
                           + "｜".join(_inst_wk9[:3]) + "</div>" if _inst_wk9 else ""),
                        unsafe_allow_html=True)
        # 【V88·分析时间+节奏显性化 2026-07-25 用户点单】各数据源时间必须可见;
        # 更新节奏=交易日07/13/19三班流水线(盘中三次)+桌面三时段相位扫描,周末每日09:00一趟——
        # 全跑公共仓Actions+GPT-6订阅既有缓存，不新增按量API费用。
        _ts_line9 = []
        for _lbl9t, _src9t in (("快照", _nw_snap9.get("generated_at")),
                               ("轮动", (_nw_snap9.get("rotation_forecast") or {}).get("analysis_time")),
                               ("黑马", _dh9n.get("generated_at")),
                               ("全池相位", _pt9n.get("generated_at"))):
            if _src9t:
                _ts_line9.append(f"{_lbl9t}{str(_src9t)[5:16]}")
        st.caption("🕒 分析时间: " + " · ".join(_ts_line9)
                   + f" ｜ 当前档:{_tb_tier9}(大盘={_cfg9['mkt_hz'] or '当日实况'}·板块={_cfg9['sec_hz'] or '当日涨跌'}·买={_buy_note9})"
                   " ｜ 更新节奏:交易日07/13/19点三班+盘中三时段相位扫描·周末每日09:00一趟"
                   "(下周/本月/下月档同源同步更新·Actions公共仓免费,预算内)")
        # 【2026-08-16 界面增强·轮动过期警示】轮动是快变量,过期3天的预测约等于作废;
        # 时间戳虽已展示但混在一行里不显眼。落后最近交易日→单独红条点名。
        try:
            import datetime as _dt_rf9
            _rf_at9 = str((_nw_snap9.get("rotation_forecast") or {}).get("analysis_time") or "")[:16]
            if _rf_at9:
                _rf_ts9 = _dt_rf9.datetime.strptime(_rf_at9, "%Y-%m-%d %H:%M")
                _nowr9 = _dt_rf9.datetime.now()
                _ltd9 = _nowr9.replace(hour=0, minute=0, second=0, microsecond=0)
                if _nowr9.weekday() < 5 and _nowr9.hour < 10:
                    _ltd9 -= _dt_rf9.timedelta(days=1)  # 盘前基准上一交易日
                while _ltd9.weekday() >= 5:
                    _ltd9 -= _dt_rf9.timedelta(days=1)
                if _rf_ts9.date() < _ltd9.date():
                    st.markdown(f"<div style='background:#fff7ed;border:1px solid #fdba74;border-radius:6px;"
                                f"padding:5px 10px;font-size:12.5px;color:#9a3412'>⚠️ 板块轮动预测为 "
                                f"<b>{_rf_at9}</b> 版，已落后最近交易日——方向判断仅供参考，"
                                f"等下一个交易日 09:00 盘前刷新。</div>", unsafe_allow_html=True)
        except Exception:
            pass
        # 【V88·立刻更新 2026-07-25 用户点单】四数据源各配强刷按钮(60s防重),不等班车
        _rb1, _rb2, _rb3, _rb4 = st.columns(4)

        def _rf_snap9():
            import sys as _sy9f
            if str(_nw_repo9 / "src") not in _sy9f.path:
                _sy9f.path.insert(0, str(_nw_repo9 / "src"))
            # 全链一致的关键:rotation/cycle各有自己的slot缓存闸,不清掉会在链内被拦
            # (实测周期停昨天)——删缓存文件=强制子链全部用本次新行情重算,时点才真一致。
            for _cf9f in ("rotation_forecast.json", "cycle_scan.json"):
                try:
                    (_nw_repo9 / "data" / _cf9f).unlink()
                except Exception:
                    pass
            from market_snapshot import generate_market_snapshot as _gms9
            _gms9()

        # 【V88·时点一致性 2026-07-25 用户抓"分批更新会不会数据不一样结果不一样"——成立!】
        # 原独立"板块轮动"按钮=拿旧快照行情当输入重算→新时间戳+旧数据,时间戳撒谎,已拆除。
        # 铁律:强刷必须按依赖链闭包——快照→轮动→个股周期一体重算(generate_market_snapshot内嵌),
        # 黑马/全池相位独立实时fetch链可单刷,机构/公告/打新/三榜独立信息源可单刷。

        def _rf_dh9():
            import sys as _sy9f
            if str(_nw_repo9 / "src") not in _sy9f.path:
                _sy9f.path.insert(0, str(_nw_repo9 / "src"))
            import darkhorse_radar as _dhm9f
            _ex9f = set()
            try:
                _w9f = json.loads((_nw_repo9 / "watchlist_v88.json").read_text(encoding="utf-8"))
                for _l9f in (_w9f or {}).values():
                    for _it9f in _l9f or []:
                        if isinstance(_it9f, (list, tuple)) and _it9f:
                            _c9f = str(_it9f[0]).upper().split(".")[0]
                            _ex9f.add(_c9f.lstrip("0") or _c9f)
            except Exception:
                pass
            _dhm9f.build_darkhorse(_ex9f)

        with _rb1:
            _v88_refresh9("快照+轮动+周期(全链一致)", "约2-3分钟", _rf_snap9, "rf_snap9")
        with _rb2:
            st.caption("🛡️ 时点一致原则:行情链(快照/轮动/周期)只许整链重算,不给半截更新按钮——防'新时间戳旧数据'")
        with _rb3:
            _v88_refresh9("黑马池", "约8-15分钟", _rf_dh9, "rf_dh9")
        with _rb4:
            # 全池相位扫描函数定义在本区之后(模块级顺序),此处按钮会NameError——
            # 强刷入口在下方⚠️🌱调整提醒模块(已有),这里指路不重复造
            st.caption("🔄 全池相位·立刻更新→在下方「⚠️🌱调整提醒」模块点扫描按钮(约5-15分钟)")
except Exception:
    import traceback as _tb9e
    try:
        import json as _j9e
        _fp9e = core_root() / "data" / "render_errors.json"
        try:
            _rs9e = _j9e.loads(_fp9e.read_text(encoding="utf-8"))
        except Exception:
            _rs9e = []
        _rs9e.append({"ts": __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                      "module": "时间作战板", "err": _tb9e.format_exc()[-500:]})
        _fp9e.write_text(_j9e.dumps(_rs9e[-100:], ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        pass

# 【V88·宏观置顶 2026-07-17 用户定纲】大盘宏观区(全球概览/宏观脉搏/三层总览)置于页面最顶,
# 先看大势定调再看自己的票。由下方全球概览块通过 slot 回填。
# (U3.2 slot已上移至页首——用户点单'全球概览置顶,看时间方便')

# Fable复核呈现：现役只读 GPT/Codex + 经典书理结果，不展示旧Claude日报。

# ===V88_PAGE_BREAK:LISTS===
_v88_front_decision_slot = st.empty()
# 【V88·搜索前置】搜索框占位排在“大盘/持仓/自选/预警”速览带之下、其余重模块之上，
# 让点开就能秒搜，无需滚到下方深度作战室。真正渲染在自选工具就绪后（约¼页处）回填。
_v88_search_slot = st.empty()

# 【V88·两池归一 2026-07-18 用户定纲】全站个股池只收两个模块：💼持仓 → ⭐自选。
# 散落各处的持仓/自选渲染块全部路由进这两个容器（container对象可多次with追加）；
# 其余主题模块（轮动/新闻/推荐/各雷达）保持领域视角，不再各自另设持仓/自选小灶。
# 【V88·一池归一 2026-07-18 用户定纲二次迭代】持仓+自选融合为一个模块缩小版面：
# 金色名=既是持仓又是自选(双重身份醒目) / 💼=纯持仓 / ⭐=纯自选。
# 两个容器变量保留(散落块的路由不用改),都指向同一个容器。
_v88_hold_slot9 = st.empty()
_v88_hold_mod9 = _v88_hold_slot9.container()
_v88_watch_mod9 = _v88_hold_mod9
with _v88_hold_mod9:
    st.markdown('<div style="font-size:17px;font-weight:800;color:#123a70;'
                'border-left:4px solid #b8860b;padding-left:.5rem;margin:.4rem 0 .05rem">'
                '📊 我的股票池（持仓＋自选） <span style="font-size:12px;color:#94a3b8;font-weight:400">'
                '持仓风险与自选管理</span></div>',
                unsafe_allow_html=True)

# 【V88界面修改原则】默认只新增、压缩与重排；不得删除或隐藏原有内容，
# 除非用户明确提出“删除”。紧凑版必须保留原指标与原功能入口。

# 【用户明确授权】旧侧栏暂时隐藏；代码与状态保留，未来可一键恢复。
st.markdown("""
<style>
section[data-testid="stSidebar"] { display: none !important; }
div[data-testid="stSidebarCollapsedControl"] { display: none !important; }
button[data-testid="stSidebarCollapseButton"] { display: none !important; }
[data-testid="collapsedControl"] { display: none !important; }

/* ═══════ V88·全局字体与配色重构（2026-07-12 用户要求） ═══════ */
/* 字体：Claude 风格 sans-serif，15px 基础字号 */
html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"],
.main, .block-container, .element-container,
[data-testid="stMarkdownContainer"], [data-testid="stCaptionContainer"],
[data-testid="stText"], button, input, select, textarea, label {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue", Arial, "Noto Sans SC", sans-serif !important;
    letter-spacing: -0.01em;
}
/* 【07-31 全天字号大战真凶】原规则把所有span强制14px!important,
   特异性压过一切行内样式与表格规则——span从选择器移除+表格元素豁免,
   行内font-size从此重新成为最终裁决(层级铁序:名称>数字>说明) */
[data-testid="stMarkdownContainer"] p,
[data-testid="stMarkdownContainer"] li,
[data-testid="stText"] { font-size: 14px !important; line-height: 1.6 !important; color: #1a1a2e; }
[data-testid="stMarkdownContainer"] span:not(table span) { font-size: 14px; line-height: 1.6; }
[data-testid="stMarkdownContainer"] table p,
[data-testid="stMarkdownContainer"] table li { font-size: inherit !important; line-height: inherit !important; }
[data-testid="stCaptionContainer"] { font-size: 12px !important; color: #5a6378 !important; }
h1, [data-testid="stHeading"] h1 { font-size: 18px !important; font-weight: 700 !important; color: #1a1a2e !important; }
h2, [data-testid="stHeading"] h2 { font-size: 16px !important; font-weight: 700 !important; color: #1e3a5f !important; }
h3, [data-testid="stHeading"] h3 { font-size: 16px !important; font-weight: 600 !important; color: #2c4a6e !important; }

/* 蓝底主按钮统一白字：覆盖全局 Markdown 深色正文规则。 */
button[kind="primary"], button[data-testid="stBaseButton-primary"],
button[kind="primary"] *, button[data-testid="stBaseButton-primary"] *,
button[kind="primary"] [data-testid="stMarkdownContainer"] p,
button[data-testid="stBaseButton-primary"] [data-testid="stMarkdownContainer"] p {
    color: #ffffff !important;
}
button[kind="primary"], button[data-testid="stBaseButton-primary"] {
    background: #2563eb !important; border-color: #2563eb !important;
}

/* 按钮必须保持可读：白底蓝字、蓝底白字；禁用主按钮也不能白底白字。 */
button[kind="secondary"], button[data-testid="stBaseButton-secondary"] {
    background: #ffffff !important; border-color: #2563eb !important; color: #1d4ed8 !important;
}
button[kind="secondary"] *, button[data-testid="stBaseButton-secondary"] * {
    color: #1d4ed8 !important;
}
button[kind="primary"]:disabled, button[data-testid="stBaseButton-primary"]:disabled {
    background: #ffffff !important; border: 1px solid #2563eb !important;
    color: #1d4ed8 !important; opacity: .62 !important;
}
button[kind="primary"]:disabled *, button[data-testid="stBaseButton-primary"]:disabled * {
    color: #1d4ed8 !important;
}

/* 【2026-07-13 用户要求·撤销深蓝按钮】主按钮深蓝#1e3a5f看不清，移除本次新增的覆盖
   → 回到调整之前：primary 用主题默认亮蓝 #2563eb + 白字，secondary 由下方通用按钮样式接管 */

/* Expander 折叠区：不同内容类型用不同左边框色 */
details[data-testid="stExpander"] {
    border: 1px solid #e2e8f0 !important; border-radius: 10px !important;
    background: #fff !important; margin-bottom: .5rem !important;
}
details[data-testid="stExpander"] summary {
    font-size: 14px !important; font-weight: 600 !important; color: #1e3a5f !important;
}
/* 预警类：左橙边框 */
details[data-testid="stExpander"]:has(summary:first-child) summary:first-child {
    border-left: none;
}

/* Tab 标签：深蓝选中态 */
button[data-baseweb="tab"] { font-size: 13px !important; font-weight: 500 !important; }
button[data-baseweb="tab"][aria-selected="true"] { color: #1e3a5f !important; border-bottom-color: #1e3a5f !important; }

/* 表格/数据行 斑马纹 */
[data-testid="stDataFrame"] tr:nth-child(even) td { background: #f8fafc; }

/* Metric delta 颜色：涨红跌绿（中国惯例） */
[data-testid="stMetricDelta"] svg { display: none; }

/* 分隔线柔和 */
hr, [data-testid="stDivider"] { border-color: #e5e9f0 !important; opacity: .6; }

/* 输入框统一圆角 */
input, textarea, [data-baseweb="select"] { border-radius: 8px !important; }
/* 示例/默认提示浅灰；用户实际输入与下拉已选值保持黑色。 */
input, textarea { color: #111827 !important; -webkit-text-fill-color: #111827 !important; }
input::placeholder, textarea::placeholder {
    color: #9ca3af !important; -webkit-text-fill-color: #9ca3af !important; opacity: 1 !important;
}
[data-baseweb="select"], [data-baseweb="select"] > div { background: #ffffff !important; }
[data-baseweb="select"] *, [data-testid="stSelectbox"] * { color: #111827 !important; }

/* caption / 辅助文字：从灰色升级为蓝灰 */
.stCaption, [data-testid="stCaptionContainer"] span { color: #5a6378 !important; }
</style>
""", unsafe_allow_html=True)

# 【V88·统一运行反馈】未知时长任务使用浏览器端持续动画，Python阻塞时动画仍会运动，
# 明确告诉用户页面没有死机；已知总量的扫描继续使用真实百分比进度条。
from contextlib import contextmanager as _contextmanager

@_contextmanager
def _v88_running(label: str):
    _slot = st.empty()
    _started = time.time()
    _safe_label = str(label).replace("<", "&lt;").replace(">", "&gt;")
    _slot.markdown(f"""
    <style>
    @keyframes v88-running-bar {{
      0% {{left:0;width:8%;}} 50% {{left:38%;width:55%;}} 100% {{left:92%;width:8%;}}
    }}
    .v88-running-box{{background:#eff6ff;border:1px solid #bfdbfe;border-radius:9px;padding:.55rem .75rem;margin:.25rem 0;color:#1d4ed8;font-size:12px}}
    .v88-running-track{{position:relative;height:7px;background:#dbeafe;border-radius:8px;overflow:hidden;margin-top:.4rem}}
    .v88-running-fill{{position:absolute;top:0;height:100%;background:linear-gradient(90deg,#2563eb,#06b6d4);border-radius:8px;animation:v88-running-bar 1.4s ease-in-out infinite}}
    </style>
    <div class="v88-running-box"><b>⏳ {_safe_label}</b>　<span>正在运行，动态条持续移动表示程序正常</span>
      <div class="v88-running-track"><div class="v88-running-fill"></div></div>
    </div>""", unsafe_allow_html=True)
    try:
        yield
    except Exception:
        _elapsed = time.time() - _started
        _slot.error(f"❌ {_safe_label}失败 · 已运行 {_elapsed:.1f} 秒")
        raise
    else:
        # 【2026-07-12 用户要求】完成后不再残留"✅完成·X秒"小绿条，直接清空占位
        _slot.empty()

# ── 每日凌晨零点缓存清零（每次页面渲染时检查日期）──────────────
_check_daily_cache_clear()

# ═══════════════════════════════════════════════════════════════
# 【V89.5 修复】提前定义MY_GEMINI_KEY - 避免在全球市场概览中未定义错误
# ═══════════════════════════════════════════════════════════════
try:
    from desktop_gpt_subscription import api_key as _gpt_subscription_ready, model_name as _gpt_model_name
    GPT_SUBSCRIPTION_READY = _gpt_subscription_ready()
    # 历史Gemini变量只作调用链兼容；生产模型固定为订阅GPT-6 Astra。
    MY_GEMINI_KEY_RAW = ""
    if GPT_SUBSCRIPTION_READY:
        MY_GEMINI_KEY = GPT_SUBSCRIPTION_READY
        AI_PROVIDER = "codex-subscription"
    else:
        MY_GEMINI_KEY = ""
        AI_PROVIDER = "none"
    GEMINI_MODEL_NAME = "gpt-6-astra"
    GPT_MODEL_NAME = _gpt_model_name()
    logging.info("✅ GPT-6 Codex订阅配置完成: %s", GPT_MODEL_NAME)
except Exception as e:
    MY_GEMINI_KEY = ""; GPT_SUBSCRIPTION_READY = ""; GEMINI_MODEL_NAME = "gpt-6-astra"; GPT_MODEL_NAME = "gpt-6-astra"
    AI_PROVIDER = "none"
    logging.error(f"⚠️ AI API配置失败: {e}")

# 【V91.9】AI分析统一模型说明：所有spinner和报告统一使用
def _ai_model_label(model=None):
    """返回模型显示名称，用于 spinner 和报告底部"""
    if AI_PROVIDER == "codex-subscription":
        return "GPT-6 Astra（订阅）"
    m = model or GEMINI_MODEL_NAME
    if USE_NEW_MODULES and hasattr(mod_config, 'GEMINI_MODELS') and m in mod_config.GEMINI_MODELS:
        return mod_config.GEMINI_MODELS[m]
    return m.replace('-', ' ').replace('gemini', 'Gemini').title()


def _load_prompt(name: str, **kwargs) -> str:
    """从 prompts/ 目录加载 prompt 模板，支持 .format() 变量替换"""
    _p = Path(__file__).parent / "prompts" / name
    try:
        tpl = _p.read_text(encoding="utf-8")
        return tpl.format(**kwargs) if kwargs else tpl
    except FileNotFoundError:
        logging.warning(f"Prompt 文件未找到: {_p}")
        return ""
    except KeyError as e:
        logging.warning(f"Prompt 变量替换失败 {name}: {e}")
        return _p.read_text(encoding="utf-8")


# ═══════════════════════════════════════════════════════════════
# 【V92】全量云端搜索 - 从侧边栏移至深度作战室主区域
# ═══════════════════════════════════════════════════════════════
def _local_name_search9(search_key):
    """【2026-07-18修】东财API挂时的本地降级——原直接引用 STOCK_NAME_INDEX(定义在
    4000行之后,脚本自上而下执行到这里还不存在)必 NameError('久吾高科'案发)。
    改为:①cloud_engine 离线名录(8208条,含全量A股) ②内置索引 globals 安全兜底。"""
    _out9 = []
    try:
        import cloud_engine as _ce_srch9
        for _nm9s, _cd9s, _mk9s in (_ce_srch9.search_candidates(search_key, limit=15) or []):
            _out9.append((_cd9s, _nm9s))
    except Exception:
        pass
    if not _out9:
        for code, name in (globals().get("STOCK_NAME_INDEX") or {}).items():
            if (search_key.upper() in code.upper() or search_key in name
                    or search_key.upper() in code.split('.')[0].upper()):
                _out9.append((code, name))
    return _out9


def render_cloud_search():
    """本地输入即筛选；显式选择后直达深度页，共用最近查看和对比篮。"""
    from display_limits import market_top as _display_market_top
    from grade_focus import market_of as _display_market_of, canonical as _display_canonical
    # 【2026-07-18修】本函数经slot提前执行,早于11083行的初始化——先补位防AttributeError
    # 【2026-08-01 连环案②】compare_basket原init在1.3万行处,本函数7495行先用→
    # 历史一恢复就崩'no attribute compare_basket'(以前历史恒空侥幸不触发)。就地init。
    if 'compare_basket' not in st.session_state:
        st.session_state.compare_basket = []
    from compare_ui import MAX_COMPARE as _MAXCMP   # 用户2026-08-01定:最多四只
    if 'search_history' not in st.session_state:
        # 【2026-08-01 用户"搜索历史丢了"】根因:persist落盘了但重启从不回读——
        # 冷启动从 search_history.json 按最近搜索时间取前10只回填
        st.session_state.search_history = []
        try:
            _sh9 = json.loads(_SEARCH_HIST_FILE.read_text(encoding="utf-8"))
            st.session_state.search_history = [
                (c, (v or {}).get("name") or c)
                for c, v in sorted(_sh9.items(), key=lambda kv: -(kv[1] or {}).get("ts", 0))[:10]]
        except Exception:
            pass
    from stock_switcher import render as _render_stock_switcher
    _render_stock_switcher(st, st.session_state.get('scan_selected_code') or '', key='v88_overview_stock_switch')
    _active_code = st.session_state.get('scan_selected_code')
    _active_name = st.session_state.get('scan_selected_name') or _active_code
    if _active_code:
        _active_pair = (_active_code, _active_name)
        if st.button('➕ 当前股票加入对比篮', key='search_compare',
                     disabled=_active_pair in st.session_state.compare_basket):
            if len(st.session_state.compare_basket) < _MAXCMP:
                st.session_state.compare_basket.append(_active_pair)
                st.rerun()
            else:
                st.toast(f'对比篮最多{_MAXCMP}只，请先移出再添加')

    if len(st.session_state.search_history) > 0:
        _visible_search_history = _display_market_top([
            {'code': c, 'name': n, 'market': _display_market_of(_display_canonical(c))}
            for c, n in st.session_state.search_history])
        st.markdown('<p style="font-size: 12px; font-weight: 600; margin-top: 1rem; margin-bottom: 0.3rem;">📜 搜索历史</p>', unsafe_allow_html=True)
        st.caption(f"最近搜索 {len(st.session_state.search_history)} 只")
        # 【2026-08-01 用户"从历史里挑选历史个股进行深度对比,最多四只"】
        # 原来只能一只一只点➕,挑四只要点四次还看不到全貌;这里一次多选直接进对比。
        _hopt9 = {f"{r['name']}（{r['code']}）": (r['code'], r['name']) for r in _visible_search_history}
        _hsel9 = st.multiselect("从搜索历史挑", list(_hopt9), max_selections=_MAXCMP,
                                key="hist_pk_sel", label_visibility="collapsed",
                                placeholder=f"从历史挑2~{_MAXCMP}只做深度对比…")
        if len(_hsel9) >= 2 and st.button(f"⚔️ 深度对比这{len(_hsel9)}只",
                                          key="hist_pk_go", type="primary", width='stretch'):
            st.session_state.pk_codes = [_hopt9[s][0] for s in _hsel9]
            st.session_state.pk_names = [_hopt9[s][1] for s in _hsel9]
            st.session_state.scan_selected_code = None
            st.session_state.scan_selected_name = None
            st.rerun()
        
        # 【2026-08-01 用户"字体好大排版好丑还占版面,10厘米以内"】
        # 原写法:每只票 3列×2行 + 两个满宽按钮 → 10只吃掉20行、约25cm,信息密度极低。
        # 改成一行多只的紧凑 chip:名称+代码同行,点名直达深度分析(?q= 复用表内同一通道);
        # "加入对比"走上方那个多选(已有),不再每只配一个满宽按钮。10只约2~3行≈4cm。
        _hchips9 = "".join(
            f"<a href='?q={_c}&focus=deep' target='_self' style='display:inline-flex;"
            f"align-items:baseline;gap:4px;padding:2px 8px;margin:2px 3px 2px 0;"
            f"border:1px solid #e2e8f0;border-radius:12px;background:#f8fafc;"
            f"text-decoration:none;font-size:12px;color:#1e293b;white-space:nowrap'>"
            f"<b>{_n}</b><span style='font-size:10px;color:#94a3b8'>{_c}</span></a>"
            for _c, _n in ((r['code'], r['name']) for r in _visible_search_history))
        st.markdown(f"<div style='line-height:1.9;margin:2px 0 4px'>{_hchips9}</div>",
                    unsafe_allow_html=True)
        _hc1, _hc2 = st.columns([3, 1])
        with _hc2:
            if st.button("🗑️ 清空", key="search_clear_history"):
                st.session_state.search_history = []
                st.rerun()

    # 【V88·评级榜多选对比 2026-08-01 用户点单"3A/2A/1A多只多选对比,最多5只,简单化"】
    # 直接从评级快照取榜面股,多选≤5一键进PK视图(与对比篮同一通道,零新链路)
    try:
        from feishu_snapshot import build as _pk_selector_snapshot
        _tqc9 = _pk_selector_snapshot()
        _pko9 = {}
        for _r9k in _display_market_top(_tqc9.get('rows') or []):
            if _r9k.get('tier') in ('3A', '2A', '1A'):
                _sc9k = _r9k.get('audit_score')
                _pko9[f"{_r9k['tier']}｜{_r9k.get('name')}（{_r9k.get('code')}）"
                      + (f" 审核分{_sc9k:g}" if isinstance(_sc9k, (int,float)) else '')] = (
                          str(_r9k.get('code')), _r9k.get('name') or str(_r9k.get('code')))
        if _pko9:
            st.markdown('<p style="font-size:12px;font-weight:600;margin:0.6rem 0 0.2rem">'
                        f'⚔️ 评级榜多选对比（3A/2A/1A·最多{_MAXCMP}只）</p>', unsafe_allow_html=True)
            _sel9k = st.multiselect("从评级榜选择", list(_pko9), max_selections=_MAXCMP,
                                    key="grade_pk_sel", label_visibility="collapsed",
                                    placeholder=f"选2~{_MAXCMP}只评级股…")
            if len(_sel9k) >= 2:
                if st.button(f"⚔️ 对比这{len(_sel9k)}只", key="grade_pk_go", type="primary"):
                    st.session_state.pk_codes = [_pko9[s][0] for s in _sel9k]
                    st.session_state.pk_names = [_pko9[s][1] for s in _sel9k]
                    st.session_state.scan_selected_code = None
                    st.session_state.scan_selected_name = None
                    st.rerun()
            elif _sel9k:
                st.caption("再选至少1只(≥2只才能对比)")
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════
# 【V90.8 关键修复】完整可点击表格 - 含快捷入口、深度分析跳转
# 必须使用此实现，mod_ui 版本无深度作战室逻辑，导致点击无反应
# ═══════════════════════════════════════════════════════════════
def render_clickable_table(df_results, table_key):
    """【V87.7】复选框智能识别 + 加入对比篮 + 快捷入口深度分析"""
    if df_results is None or len(df_results) == 0:
        st.info("暂无数据")
        return
    
    if isinstance(df_results, list):
        df_results = pd.DataFrame(df_results)
    
    if "代码" not in df_results.columns:
        st.dataframe(df_results, width='stretch', hide_index=True, key=f"table_plain_{table_key}")
        return
    
    from display_limits import market_top as _display_market_top
    from grade_focus import market_of as _display_market_of, canonical as _display_canonical
    _table_rows = [{'code': str(row['代码']).strip(),
                    'market': row.get('市场') or _display_market_of(_display_canonical(row['代码'])),
                    '_position': i} for i, (_, row) in enumerate(df_results.iterrows())]
    df_display = df_results.iloc[[r['_position'] for r in _display_market_top(_table_rows)]].copy()
    if "得分" in df_display.columns:
        df_display["得分"] = pd.to_numeric(df_display["得分"], errors="coerce").fillna(0).astype(int)
    
    st.markdown("##### 📊 扫描结果")
    st.caption("💡 快捷入口选股点击「深度分析」| 勾选1只=深度分析 | 勾选2只以上=立即对比")
    st.caption(f"展示 {len(df_display)}只 · 每市场Top5，合计至多15只；完整扫描记录保留后台。")
    
    # 【V90.6】快捷入口：选择框+按钮
    stock_options = []
    for _, row in df_display.iterrows():
        code = row.get('代码')
        name = row.get('股票') or row.get('名称') or str(code)
        if code and str(code).strip():
            stock_options.append((str(code).strip(), str(name).strip()))
    if stock_options:
        quick_col1, quick_col2 = st.columns([3, 1])
        with quick_col1:
            quick_choice = st.selectbox("🔍 快捷入口：选择股票查看深度分析", 
                options=["-- 请选择 --"] + [f"{name} ({code})" for code, name in stock_options],
                key=f"quick_select_{table_key}")
        with quick_col2:
            if st.button("⚔️ 深度分析", key=f"quick_btn_{table_key}", type="primary", width='stretch'):
                if quick_choice and quick_choice != "-- 请选择 --":
                    import re
                    m = re.search(r'\(([^)]+)\)', quick_choice)
                    if m:
                        c, n = m.group(1), quick_choice.split('(')[0].strip()
                        st.session_state.scan_selected_code = c
                        st.session_state.scan_selected_name = n
                        st.session_state.pk_codes = []
                        st.session_state.pk_names = []
                        st.toast(f"✅ 已选中 {n}，跳转深度作战室", icon="🎯")
                        st.rerun()
    
    # 【V88·个股可点击】st.dataframe 不渲染HTML，用原生 LinkColumn 让「名称」列可点
    # （点击=新标签打开 ?q=代码 深链→自动深度分析+入观察池；勾选行的老机制照旧可用）
    _link_cfg = {}
    _name_col = "名称" if "名称" in df_display.columns else ("股票" if "股票" in df_display.columns else None)
    _orig_name_series = df_display[_name_col].copy() if _name_col else None  # 【V88·个股可点击】保留原名，供下方 selected_stocks 提取用，避免被URL覆盖污染
    if _name_col and "代码" in df_display.columns:
        try:
            df_display[_name_col] = df_display.apply(
                lambda r: (f"?q={str(r['代码']).strip()}&focus=deep"
                           f"&stk={str(r[_name_col]).strip()}#v88-deep-analysis"), axis=1)
            _link_cfg[_name_col] = st.column_config.LinkColumn(
                _name_col, display_text=r"stk=([^#]+)", help="点击=深度分析并加入重点观察")
        except Exception:
            pass
    selection = st.dataframe(
        df_display,
        width='stretch',
        hide_index=True,
        on_select="rerun",
        selection_mode="multi-row",
        column_config=_link_cfg or None,
        key=f"table_{table_key}"
    )
    
    selected_stocks = []
    try:
        if selection is not None:
            if hasattr(selection, 'rows') and selection.rows:
                selected_indices = selection.rows
            elif hasattr(selection, 'selection') and hasattr(selection.selection, 'rows') and selection.selection.rows:
                selected_indices = selection.selection.rows
            else:
                selected_indices = []
            
            for idx in selected_indices:
                try:
                    row = df_display.iloc[idx]
                    code = str(row['代码']).strip()
                    if '股票' in row and row['股票'] and str(row['股票']).strip() and '股票' != _name_col:
                        name = str(row['股票']).strip()
                    elif _orig_name_series is not None and str(_orig_name_series.iloc[idx]).strip():
                        name = str(_orig_name_series.iloc[idx]).strip()
                    elif '名称' in row and row['名称'] and str(row['名称']).strip():
                        name = str(row['名称']).strip()
                    else:
                        name = code
                    selected_stocks.append((code, name))
                except Exception:
                    pass
    except Exception:
        pass
    
    if len(selected_stocks) == 1:
        code, name = selected_stocks[0]
        st.session_state.scan_selected_code = code
        st.session_state.scan_selected_name = name
        st.session_state.pk_codes = []
        st.session_state.pk_names = []
        st.toast(f"✅ 已选中 {name}，正在跳转深度分析...", icon="🎯")
        st.rerun()
    
    if len(selected_stocks) >= 2:
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button(f"⚔️ 立即对比 {len(selected_stocks)}只", key=f"compare_{table_key}", type="primary", width='stretch'):
                codes = [s[0] for s in selected_stocks]
                names = [s[1] for s in selected_stocks]
                st.session_state.pk_codes = codes
                st.session_state.pk_names = names
                st.session_state.scan_selected_code = None
                st.session_state.scan_selected_name = None
                st.toast(f"⚔️ 开始对比 {len(selected_stocks)} 只股票", icon="⚔️")
                st.rerun()
        with col2:
            if st.button(f"➕ 加入对比篮 ({len(selected_stocks)}只)", key=f"add_basket_{table_key}", width='stretch'):
                added_count = 0
                for code, name in selected_stocks:
                    if (code, name) not in st.session_state.compare_basket:
                        (st.session_state.compare_basket.append((code, name)) if len(st.session_state.compare_basket) < _MAXCMP else st.toast(f'⚠️ 对比篮最多{_MAXCMP}只(先移出再加)', icon='⚠️'))
                        added_count += 1
                if added_count > 0:
                    st.toast(f"✅ 已加入 {added_count} 只股票到对比篮", icon="➕")
                    st.rerun()
                else:
                    st.toast("ℹ️ 这些股票已在对比篮中", icon="ℹ️")
        with col3:
            if st.button("🗑️ 清除选择", key=f"clear_{table_key}", width='stretch'):
                st.rerun()

# ═══════════════════════════════════════════════════════════════
# 模块别名：只保留在 inline def 之前必须确定的 3 个名称
# ProxyContext          — 仅在 modules/data_fetch 中定义
# to_yf_cn_code         — 必须在 get_market_heat() 前映射（~line 2291）
# batch_scan_analysis_concurrent — 仅在 modules/analysis_core 中定义
# ═══════════════════════════════════════════════════════════════
if USE_NEW_MODULES:
    ProxyContext = mod_data.ProxyContext
    batch_scan_analysis_concurrent = mod_analysis.batch_scan_analysis_concurrent
    logging.info("✅ 模块别名映射完成（3项）")

# 无论哪种模式，确保舆情分析器绑定了AI调用函数
try:
    if SENTIMENT_ANALYZER_AVAILABLE and _sentiment_analyzer:
        _sentiment_analyzer.call_ai = call_model_api
        logging.info("✅ 舆情分析器已绑定 call_model_api")
except NameError:
    logging.warning("⚠️ call_model_api 尚未定义，稍后绑定")

# ═══════════════════════════════════════════════════════════════
# Fragment 函数：AI综合分析（局部刷新，按钮交互不触发全页重跑）
# ═══════════════════════════════════════════════════════════════
@st.cache_data(ttl=3600, show_spinner=False)
def _cached_yf_history_for_ai(index_code: str, period: str = "60d"):
    """yfinance 指数日线缓存，避免 AI 分析区重复拉取。"""
    import yfinance as _yf
    try:
        return _yf.Ticker(index_code).history(period=period, timeout=15)
    except TypeError:
        return _yf.Ticker(index_code).history(period=period)


def _build_market_ai_context():
    """【V99.8】AI综合分析的事实上下文：全部来自本地文件（量化快照+真实新闻日报，
    两者已由 launchd/导航兜底保持1小时内新鲜），零额外网络请求。
    返回 (context_text, tech_by_market)——技术指标由快照确定性算出，不劳 LLM。"""
    _repo = core_root()
    ctx_parts, tech = [], {}
    try:
        snap = json.loads((_repo / "data" / "market_snapshot.json").read_text(encoding="utf-8"))
    except Exception:
        snap = {}
    mkts = (snap or {}).get("markets", {})
    for mname in ("美股", "A股", "港股"):
        blk = mkts.get(mname) or {}
        t = blk.get("temperature") or {}
        ixs = blk.get("indices") or []
        if ixs:
            _chg5 = float(ixs[0].get("chg5d", 0) or 0)
            tech[mname] = {
                'current_price': float(ixs[0].get("last", 0) or 0),
                'trend': '上涨' if _chg5 > 0.5 else ('下跌' if _chg5 < -0.5 else '震荡'),
                'strength': int(t.get("temp", 50) or 50),  # 强度=市场温度（趋势+宽度+动量+量能）
            }
        lines = [f"[{mname}] 温度{t.get('temp', '?')}/100 {t.get('label', '')}·建议仓位{t.get('position', '?')}"
                 + (f"·🧭研判{t.get('verdict')}" if t.get('verdict') else "")]
        for ix in ixs[:3]:
            lines.append(f"  {ix.get('name')}: {ix.get('last')}｜5日{float(ix.get('chg5d', 0) or 0):+.1f}%｜距MA20 {float(ix.get('vs_ma20', 0) or 0):+.1f}%｜{ix.get('trend', '')}")
        secs = blk.get("sectors") or []
        if secs:
            _top = sorted(secs, key=lambda x: -float(x.get("chg5d", 0) or 0))
            lines.append("  板块5日: 领涨 " + "、".join(f"{s['name']}{float(s['chg5d']):+.1f}%" for s in _top[:3])
                         + " ｜ 落后 " + "、".join(f"{s['name']}{float(s['chg5d']):+.1f}%" for s in _top[-2:]))
        ctx_parts.append("\n".join(lines))
    try:
        _rep_fp = _repo / "data" / "daily_report.md"
        rep = _rep_fp.read_text(encoding="utf-8")

        def _sec(start, ends, cap=1500):
            i = rep.find(start)
            if i < 0:
                return ""
            _cands = [x for x in (rep.find(e, i + 8) for e in ends) if x > 0]
            return rep[i:min(_cands)][:cap] if _cands else rep[i:i + cap]

        ctx_parts.append("【今日总览·真实新闻日报】\n" + _sec("## 一、", ["## 🎯", "## 二、"]))
        ctx_parts.append("【美股·重点行业与个股·真实新闻】\n" + _sec("## 二、", ["## 三、"]))
        ctx_parts.append("【A股·重点行业与个股·真实新闻】\n" + _sec("## 三、", ["## 四、"]))
        ctx_parts.append("【港股·重点行业与个股·真实新闻】\n" + _sec("## 四、", ["## 五、"]))
        ctx_parts.append("【风险提示】\n" + _sec("## 五、", ["## 📈", "## 六、"], cap=800))
        _age_h = (time.time() - _rep_fp.stat().st_mtime) / 3600
        ctx_parts.append(f"（新闻日报生成于 {_age_h:.1f} 小时前）")
    except Exception:
        ctx_parts.append("【新闻日报缺失：热点只可基于上方量化数据，禁止编造新闻】")
    return "\n\n".join(ctx_parts), tech


def _run_all_markets_ai():
    """【V99.8】一键分析重构：单次 LLM 调用产出三市场精简分析。
    旧版=逐市场串行调 Gemini(直连SDK,key已失效)且只喂5根K线无新闻；
    新版=本地快照+真实新闻拼上下文 → 一次订阅GPT-6 Astra调用（热点有真实依据）。
    返回 {mk: {'pred':…, 'tech':…}}；按【市场】标记切分，解析失败时共享全文兜底。"""
    ctx, tech = _build_market_ai_context()
    prompt = f"""你是买方投资总监。以下材料是唯一事实来源（真实行情快照+当日真实新闻日报），禁止使用材料之外的新闻、数据或价格。

{ctx}

任务：对美股/港股/A股各写一段精简分析，每市场≤150字，严格按以下四行格式（不要寒暄不要免责声明）：
【美股】
研判：趋势判断+关键点位，一句话
热点：今天真正驱动市场的新闻/板块（引用上方材料原文要点；材料里没有就写「今日无显著催化」）
3-5日：偏涨/偏跌/震荡 + 预计波动区间
操作：仓位与策略，一句话
【港股】（同格式）
【A股】（同格式）
【跨市场联动】≤60字：三市场传导关系与今日最重要的一条主线"""
    _llm = globals().get("call_model_api")
    text = ""
    try:
        if callable(_llm):
            text = _llm(prompt) or ""
    except Exception as _e:
        _safe_print(f"[AI市场分析] LLM 调用失败: {type(_e).__name__}: {str(_e)[:100]}")

    out = {}
    key_map = {"美股": "us", "港股": "hk", "A股": "cn"}
    if text and len(text) > 30:
        li = text.find("【跨市场联动】")
        link = ("\n\n---\n🔗 " + text[li:].replace("【跨市场联动】", "**跨市场联动**：").strip()) if li >= 0 else ""
        for mname, mk in key_map.items():
            i = text.find(f"【{mname}】")
            if i < 0:
                seg = text if li < 0 else text[:li]  # 解析失败：共享全文兜底
            else:
                _nxt = [text.find(f"【{m2}】", i + 1) for m2 in key_map if m2 != mname]
                _nxt = [x for x in _nxt if x > i] + ([li] if li > i else [])
                seg = text[i:min(_nxt)] if _nxt else text[i:]
            out[mk] = {'pred': seg.strip() + link, 'tech': tech.get(mname, {})}
    else:
        for mname, mk in key_map.items():
            if tech.get(mname):
                out[mk] = {'pred': "", 'tech': tech[mname]}
    return out


def _load_market_ai_from_cache():
    """页面加载时：从文件缓存恢复 AI 市场分析到 session_state（不调用API）"""
    for mk, ss_pred, ss_tech, ss_sent in [
        ('us', 'market_ai_us', '_us_tech_data', 'market_sentiment_us'),
        ('hk', 'market_ai_hk', '_hk_tech_data', 'market_sentiment_hk'),
        ('cn', 'market_ai_cn', '_cn_tech_data', 'market_sentiment_cn'),
    ]:
        if ss_pred in st.session_state:
            continue
        cached, _ts = _load_ai_report_cache(f"market_{mk}")
        if cached and isinstance(cached, dict):
            if cached.get('pred'):
                st.session_state[ss_pred] = cached['pred']
            if cached.get('tech'):
                st.session_state[ss_tech] = cached['tech']
            if cached.get('sentiment'):
                st.session_state[ss_sent] = cached['sentiment']

# 先从文件缓存恢复（不依赖API函数，可以在模块加载早期执行）
_load_market_ai_from_cache()


def _auto_generate_market_ai():
    """交易日盘中自动生成：三市场单次调用、每3小时一次、每天最多3次。"""
    # 默认关闭首屏自动 Gemini 三连调用，避免长时间空白；用户点击「一键分析全市场」后再生成
    if not st.session_state.get("v88_auto_ai_market", False):
        return
    _markets = [
        ('us', '美股', '^GSPC', 'market_ai_us', '_us_tech_data', 'market_sentiment_us'),
        ('hk', '港股', '^HSI', 'market_ai_hk', '_hk_tech_data', 'market_sentiment_hk'),
        ('cn', 'A股', '000001.SS', 'market_ai_cn', '_cn_tech_data', 'market_sentiment_cn'),
    ]
    if not GPT_SUBSCRIPTION_READY or not _market_ai_auto_due():
        return

    _safe_print("[AI市场分析] 盘中3小时缓存到期，单次调用生成三市场...")
    _res_all = _run_all_markets_ai()
    for mk, mname, mcode, ss_pred, ss_tech, ss_sent in _markets:
        _r = _res_all.get(mk) or {}
        if _r.get('pred'):
            st.session_state[ss_pred] = _r['pred']
            _save_ai_report_cache(f"market_{mk}", _r)
            _safe_print(f"[AI市场分析] ✅ {mname} 完成")
        if _r.get('tech'):
            st.session_state[ss_tech] = _r['tech']
    if any((_res_all.get(_mk) or {}).get('pred') for _mk, *_ in _markets):
        _record_market_ai_auto_success()
        st.session_state['_market_ai_auto_done'] = True




# ═══════════════════════════════════════════════════════════════
# 【V88.13】行业轮动 / 水位 / 量能异常 辅助函数
# ═══════════════════════════════════════════════════════════════

def _calc_sector_rotation_days(sector_close, bench_close, max_lookback=60) -> int:
    """连续相对基准跑赢(+) / 跑输(-) 天数，衡量资金轮动方向。"""
    try:
        common = sector_close.dropna().index.intersection(bench_close.dropna().index)
        if len(common) < 5:
            return 0
        s = sector_close.loc[common].astype(float)
        b = bench_close.loc[common].astype(float)
        rel = s.pct_change() - b.pct_change()
        rel = rel.dropna().tail(max_lookback)
        if rel.empty:
            return 0
        count = 0
        direction = None
        for v in reversed(rel.tolist()):
            if v > 0.0001:
                if direction in (None, 1):
                    count += 1
                    direction = 1
                else:
                    break
            elif v < -0.0001:
                if direction in (None, -1):
                    count -= 1
                    direction = -1
                else:
                    break
            else:
                break
        return count
    except Exception:
        return 0


def _calc_sector_water_level(df) -> tuple:
    """52周(250日)区间水位：(等级, 百分位)"""
    try:
        if df is None or len(df) < 5:
            return ("N/A", 50.0)
        last = float(df["Close"].iloc[-1])
        n = min(250, len(df))
        lo = float(df["Low"].tail(n).min())
        hi = float(df["High"].tail(n).max())
        if hi <= lo:
            return ("N/A", 50.0)
        pct = max(0, min(100, (last - lo) / (hi - lo) * 100))
        level = "高" if pct >= 75 else ("中" if pct >= 35 else "低")
        return (level, round(pct, 1))
    except Exception:
        return ("N/A", 50.0)


def _calc_forum_heat_index(v5, v30, vol_ratio, rot_days, market_key="US") -> dict:
    """
    论坛热度代理指数（0-100）：以量价+轮动合成，模拟雪球/Reddit/WSB 讨论热度。
    无真实论坛 API 时，用可观测的市场行为作大数据代理。
    """
    score = 50.0
    if v5 is not None:
        score += max(-15, min(15, v5 * 2))
    if v30 is not None:
        score += max(-10, min(10, v30 * 0.5))
    if vol_ratio is not None:
        score += max(-10, min(15, (vol_ratio - 1) * 20))
    score += max(-10, min(10, rot_days * 1.5))
    score = max(0, min(100, score))
    if score >= 70:
        level, icon = "🔥 热议", "🔥"
    elif score >= 55:
        level, icon = "🟡 活跃", "🟡"
    elif score >= 40:
        level, icon = "⚪ 平淡", "⚪"
    else:
        level, icon = "❄️ 冷清", "❄️"
    _forum_names = {"US": "Reddit/WSB", "HK": "雪球/富途", "CN": "雪球/东财股吧"}
    return {
        "score": int(score),
        "level": level,
        "icon": icon,
        "forum": _forum_names.get(market_key, "论坛"),
        "label": f"{icon}{int(score)}",
    }


def analyze_volume_anomaly(df) -> dict | None:
    """
    个股交易量异常解读：覆盖放量/缩量 × 涨/跌，体现走势升降信号。
    """
    if df is None or len(df) < 21 or "Volume" not in df.columns:
        return None
    try:
        close = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        last_c = float(close.iloc[-1])
        prev_c = float(close.iloc[-2])
        price_chg = (last_c / prev_c - 1) * 100 if prev_c else 0
        last_v = float(volume.iloc[-1])
        avg_v20 = float(volume.tail(20).mean())
        avg_v5 = float(volume.tail(5).mean())
        vol_ratio = last_v / avg_v20 if avg_v20 > 0 else 1.0
        vol_trend_5d = (avg_v5 / avg_v20 - 1) * 100 if avg_v20 > 0 else 0

        is_up = price_chg > 0.3
        is_down = price_chg < -0.3
        is_surge = vol_ratio >= 1.5
        is_shrink = vol_ratio <= 0.65
        is_extreme = vol_ratio >= 2.5
        is_dry = vol_ratio <= 0.45

        if is_surge and is_up:
            anomaly, signal = "放量上涨", "bullish"
            explain = (
                f"成交量为20日均量 **{vol_ratio:.1f}倍**，当日涨 **{price_chg:+.1f}%**。"
                "多头主动推升，量价配合健康，**趋势有望延续**。"
            )
        elif is_surge and is_down:
            anomaly, signal = "放量下跌", "bearish"
            explain = (
                f"成交量 **{vol_ratio:.1f}倍** 于均量，当日跌 **{price_chg:+.1f}%**。"
                "抛压集中释放，**警惕继续下行**或主力出货。"
            )
        elif is_shrink and is_up:
            anomaly, signal = "缩量上涨", "caution_up"
            explain = (
                f"量仅均量 **{vol_ratio:.1f}倍** 但价涨 **{price_chg:+.1f}%**。"
                "上涨缺乏量能支撑，**追高风险**，需观察后续补量。"
            )
        elif is_shrink and is_down:
            anomaly, signal = "缩量下跌", "caution_down"
            explain = (
                f"量缩至 **{vol_ratio:.1f}倍**，价跌 **{price_chg:+.1f}%**。"
                "抛压减弱，**可能接近短期底部**，关注止跌信号。"
            )
        elif is_extreme:
            anomaly, signal = "天量异动", "turning"
            explain = (
                f"**天量级**成交（{vol_ratio:.1f}x），价变 **{price_chg:+.1f}%**。"
                "主力大幅换手，**关注变盘方向**（突破或见顶）。"
            )
        elif is_dry:
            anomaly, signal = "地量观望", "neutral"
            explain = (
                f"**地量**（{vol_ratio:.1f}x），价变 **{price_chg:+.1f}%**。"
                "市场观望，**静待方向选择**，突破需放量确认。"
            )
        else:
            anomaly, signal = "量能正常", "neutral"
            explain = (
                f"成交量 **{vol_ratio:.1f}倍** 于20日均，价变 **{price_chg:+.1f}%**，"
                "量价配合正常。"
            )

        trend_note = ""
        if vol_trend_5d > 25:
            trend_note = "📈 近5日量能持续放大，市场关注度升高"
        elif vol_trend_5d < -25:
            trend_note = "📉 近5日量能持续萎缩，市场渐冷"

        _sig_colors = {
            "bullish": "#10b981", "bearish": "#ef4444",
            "caution_up": "#f59e0b", "caution_down": "#3b82f6",
            "turning": "#8b5cf6", "neutral": "#6b7280",
        }
        return {
            "vol_ratio": round(vol_ratio, 2),
            "price_chg_1d": round(price_chg, 2),
            "vol_trend_5d": round(vol_trend_5d, 1),
            "anomaly_type": anomaly,
            "signal": signal,
            "color": _sig_colors.get(signal, "#6b7280"),
            "explanation": explain,
            "trend_note": trend_note,
            "label": f"{'🔥' if is_surge else ('📉' if is_shrink else '📊')} {anomaly} ({vol_ratio:.1f}x)",
            "action_hint": {
                "bullish": "✅ 趋势向上，可跟随但设止损",
                "bearish": "⛔ 趋势向下，不宜抄底",
                "caution_up": "⚠️ 无量上涨，等放量确认再追",
                "caution_down": "👀 缩量回调，观察是否止跌",
                "turning": "🔄 天量变盘，等方向明朗",
                "neutral": "➖ 量能正常，按原计划操作",
            }.get(signal, "➖ 观望"),
        }
    except Exception:
        return None


def _interpret_capital_flow(rot_days, v5, v30, vol_ratio, w_level, w_pct) -> dict:
    """行业/板块资金动向 — 白话结论，一眼看懂。"""
    parts, score = [], 0
    if rot_days >= 3:
        parts.append(f"新资金连续{rot_days}日流入")
        score += 22
    elif rot_days <= -3:
        parts.append(f"资金连续{abs(rot_days)}日撤出")
        score -= 22
    elif rot_days > 0:
        parts.append(f"轻微流入{rot_days}日")
        score += 8
    elif rot_days < 0:
        parts.append(f"轻微流出{abs(rot_days)}日")
        score -= 8
    else:
        parts.append("资金方向不明")

    if v5 is not None:
        if v5 > 2:
            parts.append(f"5日涨{v5:+.1f}%")
            score += 12
        elif v5 < -2:
            parts.append(f"5日跌{v5:.1f}%")
            score -= 12
    if vol_ratio >= 1.5:
        parts.append("放量确认")
        score += 10
    elif vol_ratio <= 0.7:
        parts.append("缩量")
        score -= 4
    if w_level == "低":
        parts.append("水位低·机会区")
        score += 6
    elif w_level == "高":
        parts.append("水位高·谨慎")
        score -= 6

    if score >= 28:
        verdict, emoji, color, action = "强势流入", "🟢", "#10b981", "重点跟踪"
    elif score >= 12:
        verdict, emoji, color, action = "温和流入", "🔵", "#3b82f6", "可配置"
    elif score <= -28:
        verdict, emoji, color, action = "明显流出", "🔴", "#ef4444", "回避"
    elif score <= -12:
        verdict, emoji, color, action = "边际流出", "🟠", "#f59e0b", "谨慎"
    else:
        verdict, emoji, color, action = "震荡整理", "⚪", "#6b7280", "观望"

    return {
        "verdict": verdict, "emoji": emoji, "color": color, "action": action,
        "score": score, "summary": " · ".join(parts[:4]),
        "label": f"{emoji} {verdict}",
        "flow_type": "in" if score >= 12 else ("out" if score <= -12 else "flat"),
    }


def _cross_market_leader(v5_map: dict) -> str:
    """三市场强弱对比，一句话。"""
    valid = {k: v for k, v in v5_map.items() if v is not None}
    if len(valid) < 2:
        return "—"
    best = max(valid, key=valid.get)
    worst = min(valid, key=valid.get)
    spread = valid[best] - valid[worst]
    if spread < 1.5:
        return f"三市场均衡"
    return f"强{best}({valid[best]:+.1f}%) 弱{worst}({valid[worst]:+.1f}%)"


def _render_sector_heat_panel(heat_df: pd.DataFrame):
    """行业热力：汇总 + 手机筛选 + 表格/卡片双视图。"""
    if heat_df is None or heat_df.empty:
        return

    # ── 今日资金轮动 TOP 汇总 ──
    _rank_df = heat_df.copy()
    if "_flow_score" in _rank_df.columns:
        _top_in = _rank_df.nlargest(3, "_flow_score")
        _top_out = _rank_df.nsmallest(3, "_flow_score")
        _sum_c1, _sum_c2, _sum_c3 = st.columns(3)
        with _sum_c1:
            _in_txt = " · ".join(
                f"{r['行业']}({r.get('资金动向', '—')})"
                for _, r in _top_in.iterrows()
            ) or "—"
            st.markdown(
                f'<div class="heat-summary-card" style="border-left:4px solid #10b981;">'
                f'<div style="font-weight:700;color:#10b981;font-size:12px;">🟢 资金最强 TOP3</div>'
                f'<div style="font-size:12px;color:#374151;margin-top:4px;">{_in_txt}</div></div>',
                unsafe_allow_html=True,
            )
        with _sum_c2:
            _out_txt = " · ".join(
                f"{r['行业']}({r.get('资金动向', '—')})"
                for _, r in _top_out.iterrows()
            ) or "—"
            st.markdown(
                f'<div class="heat-summary-card" style="border-left:4px solid #ef4444;">'
                f'<div style="font-weight:700;color:#ef4444;font-size:12px;">🔴 资金最弱 TOP3</div>'
                f'<div style="font-size:12px;color:#374151;margin-top:4px;">{_out_txt}</div></div>',
                unsafe_allow_html=True,
            )
        with _sum_c3:
            if "_forum_avg" in _rank_df.columns:
                _hot = _rank_df.nlargest(1, "_forum_avg").iloc[0]
                st.markdown(
                    f'<div class="heat-summary-card" style="border-left:4px solid #8b5cf6;">'
                    f'<div style="font-weight:700;color:#8b5cf6;font-size:12px;">💬 论坛最热</div>'
                    f'<div style="font-size:12px;color:#374151;margin-top:4px;">'
                    f'{_hot["行业"]} · 均热度{_hot["_forum_avg"]}</div></div>',
                    unsafe_allow_html=True,
                )

    st.caption("📖 **轮动天数**=连续跑赢基准(🟢流入) / 跑输(🔴流出) · **水位**=52周位置 · **资金动向**=系统综合判断")

    # ── 筛选栏（手机端友好：下拉代替滑块）──
    _ff1, _ff2, _ff3, _ff4 = st.columns([2, 2, 2, 1])
    with _ff1:
        _f_market = st.selectbox(
            "🌐 看哪个市场", ["全部", "🇺🇸 美股", "🇭🇰 港股", "🇨🇳 A股"],
            key="heat_filter_market", label_visibility="collapsed",
        )
        st.caption("🌐 市场")
    with _ff2:
        _f_flow = st.selectbox(
            "💰 资金方向", ["全部", "🟢 流入", "🔴 流出", "⚪ 震荡"],
            key="heat_filter_flow", label_visibility="collapsed",
        )
        st.caption("💰 资金")
    with _ff3:
        _f_heat = st.selectbox(
            "💬 论坛热度", ["全部", "🔥 热议≥70", "❄️ 冷清<40"],
            key="heat_filter_forum", label_visibility="collapsed",
        )
        st.caption("💬 热度")
    with _ff4:
        _card_mode = st.checkbox("📱卡片", key="heat_card_mode", help="手机推荐开卡片模式")
        st.caption("视图")

    _view_df = heat_df.copy()
    if _f_flow == "🟢 流入" and "_flow_type" in _view_df.columns:
        _view_df = _view_df[_view_df["_flow_type"] == "in"]
    elif _f_flow == "🔴 流出" and "_flow_type" in _view_df.columns:
        _view_df = _view_df[_view_df["_flow_type"] == "out"]
    elif _f_flow == "⚪ 震荡" and "_flow_type" in _view_df.columns:
        _view_df = _view_df[_view_df["_flow_type"] == "flat"]
    if _f_heat == "🔥 热议≥70" and "_forum_avg" in _view_df.columns:
        _view_df = _view_df[_view_df["_forum_avg"] >= 70]
    elif _f_heat == "❄️ 冷清<40" and "_forum_avg" in _view_df.columns:
        _view_df = _view_df[_view_df["_forum_avg"] < 40]

    if len(_view_df) < len(heat_df):
        st.caption(f"🔍 筛选结果：{len(_view_df)} / {len(heat_df)} 个行业")

    st.session_state["_heat_view_df"] = _view_df

    # ── 卡片视图（手机端）──
    if _card_mode:
        for _ci, (_, _row) in enumerate(_view_df.iterrows()):
            _sec = _row["行业"]
            _cross = _row.get("跨市场对比", "—")
            _capital = _row.get("资金动向", "—")
            _action = _row.get("操作建议", "观望")
            _forum = _row.get("💬 论坛热度(大数据)", "—")
            if _f_market == "🇺🇸 美股":
                _mkt_lines = f"🇺🇸 {_row.get('🇺🇸 轮动·水位', '—')} · {_row.get('🇺🇸 美股 5日 | 30日 | 60日', '—')}"
            elif _f_market == "🇭🇰 港股":
                _mkt_lines = f"🇭🇰 {_row.get('🇭🇰 轮动·水位', '—')} · {_row.get('🇭🇰 港股 5日 | 30日 | 60日', '—')}"
            elif _f_market == "🇨🇳 A股":
                _mkt_lines = f"🇨🇳 {_row.get('🇨🇳 轮动·水位', '—')} · {_row.get('🇨🇳 A股 5日 | 30日 | 60日', '—')}"
            else:
                _mkt_lines = (
                    f"🇺🇸 {_row.get('🇺🇸 轮动·水位', '—')}<br>"
                    f"🇭🇰 {_row.get('🇭🇰 轮动·水位', '—')}<br>"
                    f"🇨🇳 {_row.get('🇨🇳 轮动·水位', '—')}"
                )
            st.markdown(
                f'<div class="heat-sector-card">'
                f'<div class="heat-card-title">{_sec} · {_capital}</div>'
                f'<div class="heat-card-sub">📊 {_cross} · 建议：<b>{_action}</b></div>'
                f'<div class="heat-card-body">{_mkt_lines}</div>'
                f'<div class="heat-card-forum">💬 {_forum}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
            if st.button(f"🌍 分析 {_sec}", key=f"heat_card_btn_{_ci}", use_container_width=True):
                st.session_state.sector_analysis_name = _sec
                st.session_state.sector_analysis_market = "全球"
                st.session_state.sector_analysis_codes = {
                    "us": _row["_us_code"], "hk": _row["_hk_code"], "cn": _row["_cn_code"],
                }
                st.session_state.sector_analysis_heat = {
                    "us_rot": _row.get("🇺🇸 轮动·水位", "N/A"),
                    "hk_rot": _row.get("🇭🇰 轮动·水位", "N/A"),
                    "cn_rot": _row.get("🇨🇳 轮动·水位", "N/A"),
                    "forum": _row.get("💬 论坛热度(大数据)", "N/A"),
                    "us_forum": _row.get("_forum_US", {}),
                    "hk_forum": _row.get("_forum_HK", {}),
                    "cn_forum": _row.get("_forum_CN", {}),
                }
                st.rerun()
        return None

    # ── 表格视图 ──
    if _f_market == "🇺🇸 美股":
        _display_cols = ["行业", "资金动向", "操作建议", "跨市场对比",
                         "🇺🇸 美股 5日 | 30日 | 60日", "🇺🇸 轮动·水位", "💬 论坛热度(大数据)"]
    elif _f_market == "🇭🇰 港股":
        _display_cols = ["行业", "资金动向", "操作建议", "跨市场对比",
                         "🇭🇰 港股 5日 | 30日 | 60日", "🇭🇰 轮动·水位", "💬 论坛热度(大数据)"]
    elif _f_market == "🇨🇳 A股":
        _display_cols = ["行业", "资金动向", "操作建议", "跨市场对比",
                         "🇨🇳 A股 5日 | 30日 | 60日", "🇨🇳 轮动·水位", "💬 论坛热度(大数据)"]
    else:
        _display_cols = [
            "行业", "资金动向", "操作建议", "跨市场对比",
            "🇺🇸 美股 5日 | 30日 | 60日", "🇺🇸 轮动·水位",
            "🇭🇰 港股 5日 | 30日 | 60日", "🇭🇰 轮动·水位",
            "🇨🇳 A股 5日 | 30日 | 60日", "🇨🇳 轮动·水位",
            "💬 论坛热度(大数据)",
        ]
    _heat_display = _view_df[[c for c in _display_cols if c in _view_df.columns]]
    return st.dataframe(
        _heat_display,
        width='stretch',
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key="heat_table",
    )


# ═══════════════════════════════════════════════════════════════
# 【行业热力】模块级缓存工具 + get_market_heat（12小时，模块级定义）
# 必须在 if Config.ENABLE_EXPECTATION_LAYER: 之前定义，
# 确保每次 Streamlit rerun 都能找到同一个已缓存的函数对象。
# ═══════════════════════════════════════════════════════════════
_HEAT_CACHE_TS_FILE = _BRIEF_CACHE_DIR / "heat_ts.json"
_HEAT_DF_FILE = _BRIEF_CACHE_DIR / "heat_df.json"
_HEAT_FILE_TTL = 3600  # 1小时（全模块统一，原12小时）


def _load_heat_file_cache():
    """冷启动文件缓存，避免重复拉 33+ 只标的。"""
    try:
        if not _HEAT_DF_FILE.exists() or not _HEAT_CACHE_TS_FILE.exists():
            return None
        ts_data = json.loads(_HEAT_CACHE_TS_FILE.read_text(encoding="utf-8"))
        if time.time() - ts_data.get("ts", 0) > _HEAT_FILE_TTL:
            return None
        df = pd.read_json(_HEAT_DF_FILE, orient="records")
        if df.empty or "_flow_score" not in df.columns:
            return None
        return df
    except Exception:
        return None


def _save_heat_file_cache(df):
    try:
        _BRIEF_CACHE_DIR.mkdir(exist_ok=True)
        df.to_json(_HEAT_DF_FILE, orient="records", force_ascii=False)
    except Exception:
        pass


def _heat_save_ts():
    """记录热力图最后一次成功加载的时间戳"""
    try:
        _BRIEF_CACHE_DIR.mkdir(exist_ok=True)
        _HEAT_CACHE_TS_FILE.write_text(
            json.dumps({"ts": time.time()}), encoding="utf-8"
        )
    except Exception:
        pass


def _heat_remaining_seconds() -> int | None:
    """返回热力图缓存剩余秒数；若无记录返回 None"""
    _HEAT_TTL = 3600  # 1小时（全模块统一，原12小时）
    try:
        data = json.loads(_HEAT_CACHE_TS_FILE.read_text(encoding="utf-8"))
        rem = int(_HEAT_TTL - (time.time() - data.get("ts", 0)))
        return max(0, rem)
    except Exception:
        return None


@st.cache_data(ttl=3600, show_spinner=False)   # 1小时内存缓存（全模块统一）
def get_market_heat(_cache_ver="v98"):
    """
    【模块级】环球行业热力图 — 并行拉取 + 资金动向白话解读。
    """
    _file_cached = _load_heat_file_cache()
    if _file_cached is not None and not _file_cached.empty:
        return _file_cached

    from concurrent.futures import ThreadPoolExecutor, as_completed
    # 使用 Yahoo Finance 直接可用的代码（港股统一用4位补零格式，避免批量下载代码归一化不匹配）
    SECTORS = {
        "科技":       {"US": "NVDA",  "HK": "0700.HK",  "CN": "601138.SS"},
        "健康护理":   {"US": "JNJ",   "HK": "2269.HK",  "CN": "600276.SS"},
        "公用事业":   {"US": "NEE",   "HK": "0003.HK",  "CN": "600900.SS"},
        "通信":       {"US": "T",     "HK": "0728.HK",  "CN": "600050.SS"},
        "金融":       {"US": "JPM",   "HK": "2318.HK",  "CN": "600036.SS"},
        "工业":       {"US": "CAT",   "HK": "1211.HK",  "CN": "601766.SS"},
        "非必需消费": {"US": "TSLA",  "HK": "3690.HK",  "CN": "002594.SZ"},
        "必需消费":   {"US": "WMT",   "HK": "9633.HK",  "CN": "600519.SS"},
        "原材料":     {"US": "LIN",   "HK": "2899.HK",  "CN": "600028.SS"},
        "房地产":     {"US": "PLD",   "HK": "0016.HK",  "CN": "000002.SZ"},
    }
    BENCHMARKS = {"US": "SPY", "HK": "^HSI", "CN": "000300.SS"}

    import yfinance as _yf
    try:
        from market_data_helper import fetch_daily_free as _ts_daily, is_cn as _is_cn
        _has_ts = True
    except Exception:
        _has_ts = False

    def _calc_ret(series, days):
        try:
            s = series.dropna()
            if len(s) < days + 1:
                return None
            return float((s.iloc[-1] / s.iloc[-(days + 1)] - 1) * 100)
        except Exception:
            return None

    def _fmt(val):
        if val is None:
            return "N/A"
        icon = "↑" if val > 0 else "↓"
        return f"{icon}{val:+.1f}%"

    def _status(val5d):
        if val5d is None:
            return "⚪"
        if val5d > 2:
            return "🟢"
        if val5d > 0:
            return "🟡"
        if val5d > -2:
            return "🟠"
        return "🔴"

    def _hk_variants(code):
        if not code.endswith(".HK"):
            return [code]
        stem = code[:-3]
        variants = [code]
        padded = stem.zfill(4) + ".HK"
        stripped = stem.lstrip("0").rstrip() + ".HK" if stem.lstrip("0") else "0.HK"
        if padded != code:
            variants.append(padded)
        if stripped != code and stripped != ".HK":
            variants.append(stripped)
        return list(dict.fromkeys(variants))

    # 并行预取全部标的
    _all_codes = list(dict.fromkeys(
        [c for m in SECTORS.values() for c in m.values()] + list(BENCHMARKS.values())
    ))
    _single_cache = {}

    def _fetch_one(code):
        df_out = None
        if _has_ts and _is_cn(code):
            try:
                s = _ts_daily(code, days=120)
                if s is not None and len(s) >= 5:
                    df_out = s
            except Exception:
                pass
        if df_out is None:
            try:
                _em_df = fetch_from_eastmoney_universal(code, period='6mo')
                if _em_df is not None and len(_em_df) >= 5 and 'Close' in _em_df.columns:
                    df_out = _em_df
            except Exception:
                pass
        if df_out is None and not _yf_opserr_blocked():
            for _c in _hk_variants(code):
                try:
                    df2 = _yf.download(_c, period="90d", progress=False, auto_adjust=True)
                    if df2 is None or len(df2) < 5:
                        continue
                    if hasattr(df2.columns, "levels") and df2.columns.nlevels == 2:
                        df2.columns = [c[0] for c in df2.columns]
                    if "Close" not in df2.columns:
                        continue
                    df_out = df2.dropna(subset=["Close"])
                    if len(df_out) >= 5:
                        break
                except Exception as _e:
                    if _yf_check_operational_error(_e):
                        break
        return df_out

    _workers = min(10, max(4, len(_all_codes)))
    with ThreadPoolExecutor(max_workers=_workers) as _pool:
        _futs = {_pool.submit(_fetch_one, c): c for c in _all_codes}
        for _fut in as_completed(_futs):
            _c = _futs[_fut]
            try:
                _single_cache[_c] = _fut.result()
            except Exception:
                _single_cache[_c] = None

    def _get_df(code):
        return _single_cache.get(code)

    # 预取三大市场基准
    _bench_closes = {}
    for _bk, _bc in BENCHMARKS.items():
        _bdf = _get_df(_bc)
        _bench_closes[_bk] = _bdf["Close"] if _bdf is not None and "Close" in _bdf.columns else None

    def _fmt_rot(days):
        if days == 0:
            return "➖0天"
        if days > 0:
            return f"🟢+{days}天"
        return f"🔴{days}天"

    def _fmt_water(level, pct):
        if level == "N/A":
            return "N/A"
        _icons = {"高": "🔴", "中": "🟡", "低": "🟢"}
        return f"{_icons.get(level, '⚪')}{level}{pct:.0f}%"

    results = []
    for sector_name, markets in SECTORS.items():
        row = {
            "行业": sector_name,
            "_us_code": markets.get("US", ""),
            "_hk_code": markets.get("HK", ""),
            "_cn_code": markets.get("CN", ""),
        }
        _forum_scores = []
        _flow_scores = []
        _v5_map = {}
        _flow_by_mkt = {}
        for mkt_key, ret_col, rot_col in [
            ("US", "🇺🇸 美股 5日 | 30日 | 60日", "🇺🇸 轮动·水位"),
            ("HK", "🇭🇰 港股 5日 | 30日 | 60日", "🇭🇰 轮动·水位"),
            ("CN", "🇨🇳 A股 5日 | 30日 | 60日", "🇨🇳 轮动·水位"),
        ]:
            code = markets.get(mkt_key, "")
            ohlc = _get_df(code)
            closes = ohlc["Close"] if ohlc is not None and "Close" in ohlc.columns else None
            v5  = _calc_ret(closes, 5)  if closes is not None else None
            v30 = _calc_ret(closes, 30) if closes is not None else None
            v60 = _calc_ret(closes, 60) if closes is not None else None
            row[ret_col] = f"{_status(v5)} {_fmt(v5)} | {_fmt(v30)} | {_fmt(v60)}"

            rot = 0
            w_level, w_pct = "N/A", 50.0
            vol_ratio = 1.0
            if ohlc is not None and closes is not None:
                _bench = _bench_closes.get(mkt_key)
                if _bench is not None:
                    rot = _calc_sector_rotation_days(closes, _bench)
                w_level, w_pct = _calc_sector_water_level(ohlc)
                if "Volume" in ohlc.columns and len(ohlc) >= 20:
                    try:
                        vol_ratio = float(ohlc["Volume"].iloc[-1]) / float(ohlc["Volume"].tail(20).mean())
                    except Exception:
                        vol_ratio = 1.0
            row[rot_col] = f"{_fmt_rot(rot)} | {_fmt_water(w_level, w_pct)}"
            _fh = _calc_forum_heat_index(v5, v30, vol_ratio, rot, mkt_key)
            row[f"_forum_{mkt_key}"] = _fh
            _forum_scores.append(_fh["score"])
            _v5_map[{"US": "美股", "HK": "港股", "CN": "A股"}[mkt_key]] = v5
            _cf = _interpret_capital_flow(rot, v5, v30, vol_ratio, w_level, w_pct)
            _flow_by_mkt[mkt_key] = _cf
            _flow_scores.append(_cf["score"])
            row[f"_{mkt_key.lower()}_rot"] = rot

        _avg_forum = int(sum(_forum_scores) / len(_forum_scores)) if _forum_scores else 50
        _avg_flow = int(sum(_flow_scores) / len(_flow_scores)) if _flow_scores else 0
        _best_cf = max(_flow_by_mkt.values(), key=lambda x: x["score"]) if _flow_by_mkt else {}
        row["跨市场对比"] = _cross_market_leader(_v5_map)
        row["资金动向"] = _best_cf.get("label", "⚪ 震荡")
        row["操作建议"] = _best_cf.get("action", "观望")
        row["_flow_score"] = _avg_flow
        row["_flow_type"] = (
            "in" if _avg_flow >= 12 else ("out" if _avg_flow <= -12 else "flat")
        )
        row["_flow_summary"] = _best_cf.get("summary", "")
        _us_f = row.get("_forum_US", {})
        _hk_f = row.get("_forum_HK", {})
        _cn_f = row.get("_forum_CN", {})
        row["💬 论坛热度(大数据)"] = (
            f"🇺🇸{_us_f.get('label', '⚪50')} "
            f"🇭🇰{_hk_f.get('label', '⚪50')} "
            f"🇨🇳{_cn_f.get('label', '⚪50')} "
            f"| 均{_avg_forum}"
        )
        row["_forum_avg"] = _avg_forum
        results.append(row)

    _heat_save_ts()
    _result_df = pd.DataFrame(results)
    _save_heat_file_cache(_result_df)
    return _result_df


# ═══════════════════════════════════════════════════════════════
# 全球市场概览
# ═══════════════════════════════════════════════════════════════
if Config.ENABLE_EXPECTATION_LAYER:
    with _macro_top_slot.container():
        # 【2026-07-29 用户"全球市场概览上面时间两列重复了"】此处原本再画一条
        # "🌍全球市场概览·实时监控三大市场体制"标题 + 同一套 日期/纽约/北京 时钟。
        # 但本块渲染进 _macro_top_slot,位置正好紧贴置顶区(见铁律12,约2716行),
        # 于是同样的标题和同样的时钟连着出现两行——纯冗余,占掉一行高度。
        # **置顶那条是唯一真源**(它还带三市场指数+2周概率+体制裁决),这里只留内容不再重画头。
        try:
            # 检查是否请求强制刷新
            force_refresh = st.session_state.get('force_refresh_requested', False)
            if force_refresh:
                st.session_state['force_refresh_requested'] = False
                st.cache_data.clear()
                try:
                    # 【2026-07-31修使用先于init】此处早于7300行的全局init,现场取同一单例
                    if "local_cache" not in globals() and USE_NEW_MODULES:
                        local_cache = mod_cache.get_cache(
                            cache_dir=mod_config.CACHE_DIR,
                            max_size_mb=getattr(mod_config, "CACHE_MAX_SIZE_MB", 500),
                            ttl_seconds=getattr(mod_config, "CACHE_TTL", 3600))
                    local_cache.clear_all()   # 穿透文件缓存层，彻底刷新
                except Exception:
                    logging.exception("[V88] 强刷穿透文件缓存失败(不阻断)")

            # 启动性能监控
            _perf_monitor.start()

            _cache_ts = int(time.time() // 300)  # 每5分钟变一次，触发缓存刷新
            # 首页只保留宏观脉搏。行业热力首页模块已移除，底层函数仍保留供其他功能复用。
            _overview_mode = "macro"
            st.checkbox(
                "首屏自动请求AI（人工智能）市场分析",
                key="v88_auto_ai_market",
                value=True,   # 节流内建(3h/次·日3次)≈0.3元/月，普通分析计入7元基础额度
                help="已默认开启。仅交易日盘中运行；每3小时一次，每天最多3次；一次请求同时分析三市场；计入7元基础额度。",
            )

            all_markets = None
            heat_df = None
            if _overview_mode in ("macro", "both"):
                with _v88_running("🌐 正在加载全球市场宏观数据…"):
                    all_markets = _cached_expectation_all_markets(_ts=_cache_ts)
                st.session_state.all_markets = all_markets
            else:
                all_markets = st.session_state.get("all_markets")

            if _overview_mode in ("heat", "both"):
                with _v88_running("🌡️ 正在加载行业热力图…"):
                    heat_df = get_market_heat()

            if all_markets is None:
                st.info("📡 **宏观数据未加载。** 请在上方选择「仅宏观脉搏」或「全部加载」后等待片刻。")
            else:
                us_result = all_markets['us_market']
                hk_result = all_markets['hk_market']
                cn_result = all_markets['cn_market']
                summary = all_markets['summary']
                # 指数展示与权威日报共用同一快照；宏观代理指标(VIX/SPY/TLT等)仍来自宏观模块。
                try:
                    _canonical_snapshot = json.loads(_AUTHORITATIVE_SNAPSHOT.read_text(encoding="utf-8"))
                    _cn_ix = (_canonical_snapshot.get("markets", {}).get("A股", {}).get("indices") or [])
                    _hk_ix = (_canonical_snapshot.get("markets", {}).get("港股", {}).get("indices") or [])
                    if _cn_ix:
                        cn_result["index_level"] = _cn_ix[0].get("last")
                        cn_result["index_change_pct"] = _cn_ix[0].get("chg1d")
                    if len(_cn_ix) >= 3:
                        # 宏观模块已优先通过东方财富获取真实创业板指 399006。
                        # 只有真实指数缺失时，才允许快照中的 ETF 代理覆盖。
                        _cyb_snap9 = _cn_ix[2]
                        _cyb_live9 = float(cn_result.get("cyb_price") or 0)
                        _snap_is_etf9 = "ETF" in str(_cyb_snap9.get("name", ""))
                        if _cyb_live9 <= 0 or not _snap_is_etf9:
                            cn_result["cyb_price"] = _cyb_snap9.get("last")
                            cn_result["cyb_change_pct"] = _cyb_snap9.get("chg1d")
                            cn_result["cyb_use_etf"] = _snap_is_etf9
                    if _hk_ix:
                        hk_result["index_level"] = _hk_ix[0].get("last")
                        hk_result["index_change_pct"] = _hk_ix[0].get("chg1d")
                    if len(_hk_ix) >= 2:
                        hk_result["hstech_price"] = _hk_ix[1].get("last")
                        hk_result["hstech_change_pct"] = _hk_ix[1].get("chg1d")
                        hk_result["hstech_use_etf"] = "ETF" in str(_hk_ix[1].get("name", ""))
                except Exception as _snap_sync_error:
                    logging.warning(f"权威快照覆盖宏观指数失败: {_snap_sync_error}")
                def _macro_cn(_text):
                    """宏观区英文术语追加小号中文括注，不改变卡片尺寸。"""
                    _text = str(_text or "")
                    _terms = (
                        ("Risk On", "风险偏好"), ("Risk Off", "风险规避"),
                        ("Neutral", "中性观望"),   # 2026-07-24用户抓漏:裸英文无中文=呈现病
                        ("VIX", "波动率"), ("SPY", "标普500交易型基金"),
                        ("QQQ", "纳指100交易型基金"), ("TLT", "美债交易型基金"),
                        ("DXY", "美元指数"), ("10Y", "十年美债"),
                        ("MA200", "200日均线"), ("MA50", "50日均线"),
                    )
                    for _en, _cn in _terms:
                        _text = _text.replace(_en, f'{_en}<span class="v88-cn-note">（{_cn}）</span>')
                    return _text
                global_verdict = summary['global_verdict']
                _gv = "".join(c for c in str(global_verdict) if ord(c) >= 32 or c in "\n\t\r").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
                _gr = "".join(c for c in str(summary.get("global_reason", "")) if ord(c) >= 32 or c in "\n\t\r").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
                _r_on = summary.get('risk_on_count', 0)
                _r_off = summary.get('risk_off_count', 0)
                _sum_color = "#10b981" if _r_on >= 2 else ("#ef4444" if _r_off >= 2 else "#f59e0b")

            # ═══════════════════════════════════════════════════════════════
            # 【V88·紧凑首页】三市场关键指标一屏展示；完整明细默认折叠。
            def _macro_ai_text(_text, _market):
                _txt = str(_text or "").replace(f"【{_market}】", "").strip()
                _txt = _txt.split("\n\n---", 1)[0].strip()
                _txt = _txt.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                _txt = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", _txt)
                return _macro_cn(_txt).replace("\n", "<br>")

            def _move_note9(_mkt_cn9, _chg9):
                """【V88·异动30字归因 2026-07-17 用户点单】大跌/大涨(|chg|≥1.5%)时用领跌/领涨板块拼原因,零AI。"""
                try:
                    _chg9 = float(_chg9)
                    if abs(_chg9) < 1.5:
                        return ""
                    _secs9 = ((_canonical_snapshot.get("markets", {}).get(_mkt_cn9, {}) or {}).get("sectors")) or []
                    if _chg9 < 0:
                        _t9 = [x for x in sorted(_secs9, key=lambda s: s.get("chg1d", 0))[:2] if x.get("chg1d", 0) < 0]
                        _who9 = "/".join(f"{x['name']}{x.get('chg1d', 0):+.0f}%" for x in _t9)
                        return (f"🔻今日{_chg9:+.1f}% {_who9}领跌" if _who9 else f"🔻今日{_chg9:+.1f}% 普跌")[:30]
                    _t9 = [x for x in sorted(_secs9, key=lambda s: -s.get("chg1d", 0))[:2] if x.get("chg1d", 0) > 0]
                    _who9 = "/".join(f"{x['name']}{x.get('chg1d', 0):+.0f}%" for x in _t9)
                    return (f"🔺今日{_chg9:+.1f}% {_who9}领涨" if _who9 else f"🔺今日{_chg9:+.1f}% 普涨")[:30]
                except Exception:
                    return ""

            def _macro_card(_title, _verdict, _items, _reason, _ai_text="", _market="", _move=""):
                _risk_on = str(_verdict) == "Risk On"
                _accent = "#10b981" if _risk_on else ("#ef4444" if str(_verdict) == "Risk Off" else "#f59e0b")
                _cells = []
                for _lbl, _val, _chg in _items:
                    _chg_text = str(_chg).strip()
                    _mchg = re.match(r"[-+]\d+(?:\.\d+)?", _chg_text)
                    _nchg = float(_mchg.group()) if _mchg else 0.0
                    _chg_cls = "v88-up" if _nchg > 0 else ("v88-down" if _nchg < 0 else "v88-flat")
                    _arrow = "↑ " if _nchg > 0 else ("↓ " if _nchg < 0 else "")
                    _cells.append(
                        f'<div class="v88-macro-kpi"><span>{_macro_cn(_lbl)}</span><b>{_val}</b>'
                        f'<small class="{_chg_cls}">{_arrow}{_macro_cn(_chg)}</small></div>')
                _cells = "".join(_cells)
                _dense_cls = " v88-macro-dense" if len(_items) > 6 else ""
                _ai_cls = " v88-macro-ai-active" if _macro_ai_active else ""
                _ai_html = (_macro_ai_text(_ai_text, _market) if _ai_text
                            else "AI（人工智能）增强：点击上方按钮按需生成")
                st.markdown(
                    f'<div class="v88-macro-card{_dense_cls}{_ai_cls}" style="border-top:3px solid {_accent}">'
                    f'<div class="v88-macro-head"><b>{_title}</b><em style="color:{_accent}">{_macro_cn(_verdict)}</em></div>'
                    f'<div class="v88-macro-grid">{_cells}</div>'
                    f'<div class="v88-macro-ai"><span class="v88-macro-reason-inline">'
                    f'{("<b style=\"color:#b91c1c\">" + _move + "</b> ｜ ") if _move else ""}{_macro_cn(_reason)}</span>'
                    f'<b>🤖 AI（人工智能）增强</b> {_ai_html}</div></div>',
                    unsafe_allow_html=True)

            st.markdown("""<style>
            .v88-macro-title{display:flex;align-items:center;justify-content:space-between;margin:.15rem 0 .45rem}
            .v88-macro-title b{font-size:15px;color:#1e3a5f}.v88-macro-title span{font-size:12px;color:#5a6378}
            .v88-macro-card{background:#fff;border:1px solid #dce3ed;border-radius:10px;padding:.4rem .55rem;min-height:200px;box-shadow:0 1px 3px rgba(30,58,95,.06)}/* 2026-07-18 用户两次抓内容被裁(美股水位行/中港领跌+AI行):死高+overflow对内容不一的卡必裁——改min-height自适应伸展,信息完整优先于严格等高 */
            .v88-macro-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:.4rem}
            .v88-macro-head b{font-size:14px;color:#1a1a2e}.v88-macro-head em{font-style:normal;font-weight:700;font-size:13px}
            .v88-macro-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:.35rem}
            .v88-macro-kpi{min-width:0}.v88-macro-kpi>span{display:block;color:#5a6378;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
            .v88-macro-kpi b{display:block;font-size:14px;line-height:1.2;color:#1a1a2e;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
            .v88-macro-kpi small{font-size:12px}.v88-up{color:#dc2626!important}.v88-down{color:#16a34a!important}.v88-flat{color:#5a6378!important}
            .v88-macro-card .v88-cn-note{display:inline!important;font-size:.72em!important;line-height:1!important;color:#8893a7;font-weight:400;margin-left:1px}.v88-macro-reason{font-size:12px;color:#5a6378;margin-top:.35rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
            .v88-macro-dense{min-height:200px;padding:.38rem .52rem}
            .v88-macro-dense .v88-macro-head{margin-bottom:.2rem}
            .v88-macro-dense .v88-macro-grid{grid-template-columns:repeat(3,minmax(0,1fr));gap:.18rem .3rem}
            .v88-macro-dense .v88-macro-kpi>span{font-size:12px}
            .v88-macro-dense .v88-macro-kpi b{font-size:13px;line-height:1.08}
            .v88-macro-dense .v88-macro-kpi small{display:block;font-size:12px;line-height:1.1;white-space:nowrap}
            .v88-macro-dense .v88-cn-note{font-size:.66em!important}
            .v88-macro-dense .v88-macro-reason{font-size:12px;margin-top:.18rem}
            /* 2026-07-27 用户抓"文字重叠看不清":ai-active死高252px→内容超出后压到下方元素。
               死高必裁铁律再犯——全部改 min-height 自适应,AI段落块级独立不与reason挤在一行。 */
            .v88-macro-ai{font-size:12px;line-height:1.45;color:#3d4f6a;border-top:1px dashed #dce3ed;
                margin-top:.3rem;padding-top:.3rem;display:block;overflow:visible}
            .v88-macro-reason-inline{display:block;font-size:12px;color:#5a6378;white-space:normal;
                margin-bottom:4px;line-height:1.45}
            .v88-macro-ai-active{min-height:252px;height:auto}
            .v88-macro-ai-active .v88-macro-ai{max-height:none;overflow:visible}
            .v88-macro-ai b{display:block;margin-top:3px}
            @media(max-width:900px){.v88-macro-card{min-height:auto}.v88-macro-kpi b{font-size:13px}}
            </style>""", unsafe_allow_html=True)
            # AI 日报增强解读并入三市场卡片：一次请求同时生成三市场，继续复用原文件缓存。
            _auto_generate_market_ai()
            # 【V88·页眉压缩】AI解读状态并入宏观脉搏标题行(图一11px灰字)，标题与两按钮同排一行，两行→一行
            _macro_ai_state = _market_ai_schedule_state()
            _macro_ai_ts = None
            for _macro_mk in ('us', 'hk', 'cn'):
                _, _macro_t = _load_ai_report_cache(f"market_{_macro_mk}")
                if _macro_t:
                    _macro_ai_ts = max(_macro_ai_ts or 0, _macro_t)
            _macro_ai_time_text = (pd.Timestamp.fromtimestamp(_macro_ai_ts, tz="Asia/Shanghai").strftime("%m/%d %H:%M")
                                   if _macro_ai_ts else "尚未生成")
            _mc_title, _mc_b1, _mc_b2 = st.columns([10, 1.5, 0.7])
            with _mc_title:
                st.markdown(f'<div class="v88-macro-title" style="margin:.35rem 0 .2rem"><b>📡 宏观脉搏</b>'
                            f'<span>AI解读 {_macro_ai_time_text} · 今日{_macro_ai_state.get("runs", 0)}/3次 · 盘中每3h ｜ 本次读取 {_dt_global.now().strftime("%m/%d %H:%M")}</span></div>',
                            unsafe_allow_html=True)
            # 【V88·按钮矮化 2026-07-17 用户要求】这两个按钮要和左侧文字行同高，
            # Streamlit 原生按钮默认 35px 高；用 container(key=) 精确限定 CSS 只改这两个按钮
            # （不能全局改 stButton，会误伤全站其它按钮）。
            st.markdown("""<style>
            .st-key-v88-macro-ai-btnbox button, .st-key-v88-macro-ai-btnbox2 button{
              min-height:0!important;height:22px!important;padding:0 8px!important;
              font-size:12px!important;line-height:1!important}
            .st-key-v88-macro-ai-btnbox [data-testid="stTooltipHoverTarget"],
            .st-key-v88-macro-ai-btnbox2 [data-testid="stTooltipHoverTarget"]{display:flex;align-items:center}
            </style>""", unsafe_allow_html=True)
            with _mc_b1:
                with st.container(key="v88-macro-ai-btnbox"):
                    _macro_ai_generate = st.button("⚡ AI解读", key="btn_macro_ai_generate", help="更新AI增强解读", use_container_width=True)
            with _mc_b2:
                with st.container(key="v88-macro-ai-btnbox2"):
                    _macro_ai_refresh = st.button("🔄", key="btn_macro_ai_refresh", help="刷新", use_container_width=True)
            if _macro_ai_refresh:
                for _k in ['market_ai_us', '_us_tech_data', 'market_sentiment_us',
                           'market_ai_hk', '_hk_tech_data', 'market_sentiment_hk',
                           'market_ai_cn', '_cn_tech_data', 'market_sentiment_cn',
                           '_market_ai_auto_done']:
                    st.session_state.pop(_k, None)
                for _mk in ('us', 'hk', 'cn'):
                    try:
                        _rf = _AI_REPORT_CACHE_DIR / f"ai_report_market_{_mk}.json"
                        if _rf.exists():
                            _rf.unlink()
                    except Exception:
                        pass
                _macro_ai_generate = True
            if _macro_ai_generate:
                with _v88_running("AI增强解读 · 单次分析美股、港股、A股"):
                    _macro_ai_results = _run_all_markets_ai()
                _macro_ai_saved = 0
                for _mk, _ss_pred, _ss_tech in (
                    ('us', 'market_ai_us', '_us_tech_data'),
                    ('hk', 'market_ai_hk', '_hk_tech_data'),
                    ('cn', 'market_ai_cn', '_cn_tech_data'),
                ):
                    _mr = _macro_ai_results.get(_mk) or {}
                    if _mr.get('pred'):
                        st.session_state[_ss_pred] = _mr['pred']
                        _save_ai_report_cache(f"market_{_mk}", _mr)
                        _macro_ai_saved += 1
                    if _mr.get('tech'):
                        st.session_state[_ss_tech] = _mr['tech']
                if _macro_ai_saved:
                    st.session_state['_market_ai_auto_done'] = True
                    st.rerun()
                else:
                    st.error("AI增强解读生成失败，请稍后重试")

            _macro_ai_us = st.session_state.get('market_ai_us', '')
            _macro_ai_hk = st.session_state.get('market_ai_hk', '')
            _macro_ai_cn = st.session_state.get('market_ai_cn', '')
            _macro_ai_active = bool(_macro_ai_us or _macro_ai_hk or _macro_ai_cn)
            def _water_text(_symbol):
                _wr = _ath_pct(_symbol)
                if not _wr:
                    return "待核", "距历史高"
                _pct, _ath_date, _days = _wr[0], _wr[1], _wr[2]
                _p52 = _wr[4] if len(_wr) > 4 else None
                _duration = f"{_days / 365:.1f}年前" if _days >= 365 else f"{_days}天前"
                _sub = f"高点{_duration}" + (f" · 52周{int(_p52)}%" if _p52 is not None else "")
                return f"{float(_pct):+.1f}%", _sub

            with _v88_running("计算三市场历史水位"):
                _water_us = _water_text("^GSPC")
                _water_cn = _water_text("000001.SS")
                _water_hk = _water_text("^HSI")
            _mc1, _mc2, _mc3 = st.columns(3)
            with _mc1:
                _macro_card("🇺🇸 美国", us_result.get("verdict", "—"), [
                    ("VIX", f"{float(us_result.get('vix_level') or 0):.1f}", f"{float(us_result.get('vix_change_pct') or 0):+.1f}%"),
                    ("SPY", f"${float(us_result.get('spy_price') or 0):.1f}", f"{float(us_result.get('spy_change_pct') or 0):+.1f}%"),
                    ("QQQ", f"${float(us_result.get('qqq_price') or 0):.1f}", f"{float(us_result.get('qqq_change_pct') or 0):+.1f}%"),
                    ("TLT", f"${float(us_result.get('tlt_price') or 0):.1f}", f"{float(us_result.get('tlt_change_pct') or 0):+.1f}%"),
                    ("黄金", f"${float(us_result.get('gld_price') or 0):.1f}", f"{float(us_result.get('gld_change_pct') or 0):+.1f}%"),
                    ("10Y", f"{float(us_result.get('tnx_yield') or 0):.2f}%", f"{float(us_result.get('tnx_change') or 0):+.2f}"),
                    ("DXY", f"{float(us_result.get('dxy_level') or 0):.1f}", f"{float(us_result.get('dxy_change_pct') or 0):+.1f}%"),
                    ("股债相关", f"{float(us_result.get('correlation') or 0):.2f}", str(us_result.get('corr_desc') or '')[:8]),
                    ("水位", _water_us[0], _water_us[1]),
                ], str(us_result.get("reason", ""))[:44], _macro_ai_us, "美股", _move_note9("美股", us_result.get("spy_change_pct")))
            with _mc2:
                _cyb9 = float(cn_result.get("cyb_price") or 0)
                _cyb_txt9 = f"{_cyb9:.3f}元" if cn_result.get("cyb_use_etf") else f"{_cyb9:.0f}点"
                _macro_card("🇨🇳 A股", cn_result.get("verdict", "—"), [
                    ("上证", f"{float(cn_result.get('index_level') or 0):.0f}", f"{float(cn_result.get('index_change_pct') or 0):+.1f}%"),
                    ("沪深300", f"{float(cn_result.get('hs300_price') or 0):.0f}", f"{float(cn_result.get('hs300_change_pct') or 0):+.1f}%"),
                    ("创业板ETF代理" if cn_result.get("cyb_use_etf") else "创业板", _cyb_txt9, f"{float(cn_result.get('cyb_change_pct') or 0):+.1f}%"),
                    ("波动率", f"{float(cn_result.get('volatility') or 0):.1f}%", "风险温度"),
                    ("人民币", f"{float(cn_result.get('cny_price') or 0):.4f}", f"{float(cn_result.get('cny_change_pct') or 0):+.1f}%"),
                    ("水位", _water_cn[0], _water_cn[1]),
                ], str(cn_result.get("reason", ""))[:44], _macro_ai_cn, "A股", _move_note9("A股", cn_result.get("index_change_pct")))
            with _mc3:
                _macro_card("🇭🇰 港股", hk_result.get("verdict", "—"), [
                    ("恒指", f"{float(hk_result.get('index_level') or 0):.0f}", f"{float(hk_result.get('index_change_pct') or 0):+.1f}%"),
                    ("恒生科技ETF代理" if hk_result.get("hstech_use_etf") else "恒生科技", f"{float(hk_result.get('hstech_price') or 0):.2f}", f"{float(hk_result.get('hstech_change_pct') or 0):+.1f}%"),
                    ("国企指数", f"{float(hk_result.get('hsce_price') or 0):.0f}", f"{float(hk_result.get('hsce_change_pct') or 0):+.1f}%"),
                    ("波动率", f"{float(hk_result.get('volatility') or 0):.1f}%", "风险温度"),
                    ("港币", f"{float(hk_result.get('hkd_price') or 0):.4f}", f"{float(hk_result.get('hkd_change_pct') or 0):+.1f}%"),
                    ("水位", _water_hk[0], _water_hk[1]),
                ], str(hk_result.get("reason", ""))[:44], _macro_ai_hk, "港股", _move_note9("港股", hk_result.get("index_change_pct")))

            _macro_cross = ""
            for _macro_ai_source in (_macro_ai_us, _macro_ai_hk, _macro_ai_cn):
                _macro_link_match = re.search(r"(?:\*\*)?跨市场联动(?:\*\*)?[：:]\s*([^\n]+)", str(_macro_ai_source or ""))
                if _macro_link_match:
                    _macro_cross = _macro_link_match.group(1).strip()
                    break
            if _macro_cross:
                _macro_cross_html = _macro_cn(_macro_cross.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))
                st.markdown(f'<div style="font-size:12px;color:#64748b;margin:.18rem .2rem"><b>🔗 跨市场联动</b>：{_macro_cross_html}</div>', unsafe_allow_html=True)

            # 原宏观解读内容不删除：改为紧凑文字直接展示，不再占用大型指标区。
            try:
                _compact_caps = [int(float(x.get('position_cap', 80))) for x in (us_result, cn_result, hk_result)]
                _compact_cap = min(x for x in _compact_caps if 0 <= x <= 100)
            except Exception:
                _compact_cap = 30 if _r_off >= 2 else 80
            st.markdown(
                f'<div style="font-size:12px;color:#3d4f6a;line-height:1.55;margin:.15rem .2rem .35rem">'
                f'<b style="color:#1e3a5f">宏观解读</b>：{_macro_cn(_gr)}　｜　<b style="color:#1e3a5f">全局仓位上限 {_compact_cap}%</b><br>'
                f'美国：{_macro_cn(us_result.get("reason", ""))}　｜　A股：{_macro_cn(cn_result.get("reason", ""))}　｜　'
                f'港股：{_macro_cn(hk_result.get("reason", ""))}</div>', unsafe_allow_html=True)

            # 【2026-07-12 用户要求】此处原有 st.divider()：下方行业热力已停用(if False)，
            # 连续空分隔线只留末尾一条，删除以压缩版面。
            if 'sector_analysis_name' in st.session_state and st.session_state.sector_analysis_name:
                sector_name_s = st.session_state.sector_analysis_name
                codes_s = st.session_state.sector_analysis_codes

                st.markdown("---")
                st.markdown(f"### 🌍 全球{sector_name_s}行业 AI综合分析")
                st.caption(f"📅 {_dt_global.now().strftime('%Y-%m-%d %A')}")

                if st.button("❌ 关闭", key="close_sector_analysis"):
                    st.session_state.sector_analysis_name = None
                    st.session_state.sector_analysis_market = None
                    st.session_state.sector_analysis_codes = None
                    st.session_state.pop("sector_analysis_heat", None)
                    st.rerun()

                if not GPT_SUBSCRIPTION_READY:
                    st.error("❌ GPT-6 Codex订阅未登录")
                else:
                    from datetime import datetime as _dt_sector
                    today_s = _dt_sector.now().strftime("%Y年%m月%d日")

                    _heat_ctx = st.session_state.get("sector_analysis_heat", {})
                    _us_f = _heat_ctx.get("us_forum", {})
                    _hk_f = _heat_ctx.get("hk_forum", {})
                    _cn_f = _heat_ctx.get("cn_forum", {})
                    prompt_s = _load_prompt(
                        "sector_analysis.txt",
                        sector_name=sector_name_s,
                        today=today_s,
                        us_code=codes_s["us"],
                        hk_code=codes_s["hk"],
                        cn_code=codes_s["cn"],
                        us_rotation=_heat_ctx.get("us_rot", "N/A"),
                        hk_rotation=_heat_ctx.get("hk_rot", "N/A"),
                        cn_rotation=_heat_ctx.get("cn_rot", "N/A"),
                        forum_heat=_heat_ctx.get("forum", "N/A"),
                        us_forum_score=f"{_us_f.get('score', 50)}/100 ({_us_f.get('level', '平淡')})",
                        hk_forum_score=f"{_hk_f.get('score', 50)}/100 ({_hk_f.get('level', '平淡')})",
                        cn_forum_score=f"{_cn_f.get('score', 50)}/100 ({_cn_f.get('level', '平淡')})",
                    )
                    try:
                        analysis_text_s = st.write_stream(call_model_api_stream(prompt_s))
                        if COPY_UTILS_AVAILABLE:
                            CopyUtils.create_copy_button(analysis_text_s, button_text="📋 复制全文", key="copy_global_sector_full")
                        st.caption(f"📌 AI生成 · 模型: {_ai_model_label()}")
                    except Exception as e:
                        st.error(f"❌ AI分析失败: {type(e).__name__}: {str(e)}")

            # 【2026-07-12 用户要求】此处原有 st.divider()，与下方"---"重复，删除以压缩版面。
            # AI增强解读已经并入上方三张宏观卡片，不再重复渲染独立大模块。
        
        except Exception as e:
            # 宏观模块异常不影响主应用
            st.warning(f"⚠️  全球市场概览加载异常，主功能不受影响。错误信息: {str(e)[:100]}")
            logging.error(f"宏观仪表盘渲染异常: {e}")
            import traceback
            traceback.print_exc()

        # 宏观卡片与主功能紧接；不再用高分隔线额外占用纵向空间。
        # 三层周期总览组占位:紧贴宏观脉搏下方(容器内末尾),由今日导航回填
        _l3_group_slot = st.empty()



# 【V90.3】性能监控已移到左侧边栏

st.markdown("""
<style>
    /* 【2026-07-12 用户要求】全站 Claude 风格 sans-serif，替换旧华尔街日报衬线体 */
    :root {
        --v88-sans: -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue", Arial, "PingFang SC", "Noto Sans SC", sans-serif;
        --v88-body-size: 14px;
        --v88-headline-size: 13px;
        --v88-line-height: 1.6;
    }
    html, body, [class*="css"],
    [data-testid="stMarkdown"] div:not(table div), [data-testid="stMarkdown"] p, [data-testid="stMarkdown"] span:not(table *),
    [data-testid="stMarkdown"] h1, [data-testid="stMarkdown"] h2, [data-testid="stMarkdown"] h3,
    [data-testid="stMarkdown"] h4, [data-testid="stMarkdown"] h5, [data-testid="stMarkdown"] h6,
    [data-testid="stMarkdown"] b, [data-testid="stMarkdown"] strong {
        font-family: var(--v88-sans) !important;
    }
    html, body, [class*="css"] {
        font-size: var(--v88-body-size) !important;
        line-height: var(--v88-line-height) !important;
        font-feature-settings: "kern" 1, "liga" 1;
        letter-spacing: -0.01em;
    }
    /* 全页背景 */
    html, body { background: #f8f9fb !important; min-height: 100vh !important; }
    div[data-testid="stAppViewContainer"] { background: #f8f9fb !important; }
    section[data-testid="stSidebar"] { background: #f0f2f6 !important; }
    .block-container { background: transparent !important; }
    /* 标题层级：深蓝色系区分 */
    h1, [data-testid="stMarkdown"] h1 { font-family: var(--v88-sans) !important; font-weight: 700 !important; color: #1a1a2e !important; font-size: 18px !important; }
    h2, [data-testid="stMarkdown"] h2 { font-family: var(--v88-sans) !important; font-weight: 700 !important; color: #1e3a5f !important; font-size: 16px !important; }
    h3, [data-testid="stMarkdown"] h3 { font-family: var(--v88-sans) !important; font-weight: 600 !important; color: #2c4a6e !important; font-size: 16px !important; }
    h4, h5, h6, [data-testid="stMarkdown"] strong { font-family: var(--v88-sans) !important; font-weight: 700 !important; }
    /* st.metric */
    div[data-testid="stMetric"], div[data-testid="stMetric"] *,
    section[data-testid="stSidebar"] * { font-family: var(--v88-sans) !important; }
    .stMetric, div[data-testid="stDataFrame"], [data-testid="stMarkdown"] { font-variant-numeric: tabular-nums; }

    /* 【V88·页眉节省版面】顶栏(absolute 53px)会随滚动移走，顶部留白从3rem收到刚好清顶栏 */
    header[data-testid="stHeader"] { height: 2.6rem !important; background: transparent !important; }
    .block-container { padding-top: 0.9rem !important; padding-bottom: 1.1rem !important; }

    /* 按钮 */
    div.stButton > button {
        font-family: var(--v88-sans) !important;
        width: 100%; border: 1px solid #cbd5e1; background-color: #fff; color: #334155;
        font-weight: 600; padding: 0.6rem 1rem; border-radius: 8px;
        transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.04);
    }
    div.stButton > button:hover {
        border-color: #1e3a5f; color: #1e3a5f; background-color: #f0f4f8;
        transform: translateY(-1px); box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.08);
    }

    /* AI 卡片 */
    .ai-card {
        font-family: var(--v88-sans) !important;
        background: rgba(255, 255, 255, 0.95); backdrop-filter: blur(10px);
        border: 1px solid #dce3ed; border-radius: 12px; padding: 24px; margin-bottom: 20px;
        box-shadow: 0 2px 8px rgba(30, 58, 95, 0.06); position: relative; overflow: hidden;
    }
    .ai-card::before {
        content: ""; position: absolute; top: 0; left: 0; width: 100%; height: 3px;
        background: linear-gradient(90deg, #1e3a5f, #3b82f6);
    }
    .ai-title {
        font-family: var(--v88-sans) !important;
        font-size: var(--v88-headline-size); font-weight: 700; color: #1e3a5f; margin-bottom: 16px;
        border-bottom: 1px solid #e5e9f0; padding-bottom: 12px; display: flex; align-items: center; gap: 10px;
    }

    div[data-testid="stDataFrame"] tbody tr:hover { background-color: #f0f4f8 !important; cursor: pointer !important; }
    div[data-testid="stDataFrame"] tbody tr.row-selected { background-color: #dce3ed !important; font-weight: 600; }

    [data-testid="stMarkdown"] p, [data-testid="stMarkdown"] li, [data-testid="stMarkdown"] span:not(table *) {
        font-family: var(--v88-sans) !important; font-size: var(--v88-body-size) !important; color: #1a1a2e;
    }
    div[data-testid="stDataFrame"] { font-family: var(--v88-sans) !important; font-size: 13px !important; }
    .stCaption { font-family: var(--v88-sans) !important; font-size: 12px !important; color: #5a6378 !important; }
    .stMetric { font-family: var(--v88-sans) !important; font-size: var(--v88-body-size) !important; }
    [data-testid="stSidebar"] { font-family: var(--v88-sans) !important; }
    
    /* 【V92】侧边栏收起按钮 - 提高可见性，便于用户找到 */
    [data-testid="stSidebar"] [data-testid="collapsedControl"],
    button[aria-label*="collapse"], button[aria-label*="Close sidebar"],
    [data-testid="stSidebar"] > div:first-child button {
        opacity: 1 !important;
        z-index: 9999 !important;
    }

    /* 【V91.9】禁用 Streamlit 运行时的灰屏遮罩，保持页面正常亮度 */
    [data-stale="true"], [data-stale="stale"], [stale-data="true"] {
        opacity: 1 !important;
        filter: none !important;
    }

    /* ══════════════════════════════════════════════════
       📱 移动端适配（max-width: 768px）
       ══════════════════════════════════════════════════ */
    @media (max-width: 768px) {
        /* 全局容器：缩减左右边距，给内容更多空间 */
        .block-container {
            padding-top: 1rem !important;
            padding-left: 0.5rem !important;
            padding-right: 0.5rem !important;
            padding-bottom: 2rem !important;
        }

        /* 字体：稍微缩小到移动端友好尺寸 */
        html, body, [class*="css"] {
            font-size: 12px !important;
        }

        /* 标题：防止截断 */
        h1 { font-size: 16px !important; }
        h2 { font-size: 14px !important; }
        h3 { font-size: 13px !important; }

        /* st.metric：数字加大可读性 */
        div[data-testid="stMetric"] [data-testid="stMetricValue"] {
            font-size: 18px !important;
        }
        div[data-testid="stMetric"] [data-testid="stMetricLabel"] {
            font-size: 12px !important;
        }

        /* 表格：允许横向滚动，防止溢出 */
        div[data-testid="stDataFrame"] {
            overflow-x: auto !important;
            -webkit-overflow-scrolling: touch !important;
        }

        /* 按钮：稍大点击区域，更易操作 */
        div.stButton > button {
            padding: 0.7rem 0.8rem !important;
            font-size: 12px !important;
            min-height: 44px !important;
        }

        /* 多列布局：允许 Streamlit columns 在窄屏正常流动 */
        div[data-testid="stHorizontalBlock"] {
            flex-wrap: wrap !important;
        }
        div[data-testid="stColumn"] {
            min-width: 100px !important;
        }

        /* 侧边栏：收起时不占空间 */
        section[data-testid="stSidebar"][aria-expanded="true"] {
            width: 75vw !important;
            min-width: 200px !important;
        }

        /* AI 卡片：减少内边距 */
        .ai-card {
            padding: 14px !important;
            border-radius: 10px !important;
        }

        /* news-brief：移动端字体 */
        .news-brief {
            font-size: 13px !important;
            padding: 1rem !important;
        }
        .news-brief h1 { font-size: 16px !important; }
        .news-brief h2 { font-size: 14px !important; }
        .news-brief p, .news-brief li { font-size: 12px !important; }

        /* 复制按钮：更大点击区域 */
        button[id*="copy"] {
            padding: 6px 12px !important;
            font-size: 12px !important;
            min-height: 36px !important;
        }

        /* 防止长文本溢出 */
        [data-testid="stMarkdown"] p,
        [data-testid="stMarkdown"] span,
        [data-testid="stCaption"] {
            word-break: break-word !important;
            overflow-wrap: break-word !important;
        }

        /* Tab 标签：允许换行防止溢出 */
        [data-testid="stTabs"] [role="tab"] {
            font-size: 12px !important;
            padding: 0.4rem 0.6rem !important;
            white-space: nowrap !important;
        }
        [data-testid="stTabs"] [role="tablist"] {
            overflow-x: auto !important;
            -webkit-overflow-scrolling: touch !important;
        }

        /* 背景色确保对比度（暗色文字 on 浅色背景） */
        html, body,
        div[data-testid="stAppViewContainer"],
        .block-container {
            background-color: #f1f5f9 !important;
            color: #1e293b !important;
        }
        /* inline style 的 div 文字颜色兜底 */
        [data-testid="stMarkdown"] div,
        [data-testid="stMarkdown"] p {
            color: inherit !important;
        }
    }

    /* 超小屏 iPhone SE / 375px */
    @media (max-width: 400px) {
        .block-container {
            padding-left: 0.3rem !important;
            padding-right: 0.3rem !important;
        }
        div[data-testid="stMetric"] [data-testid="stMetricValue"] {
            font-size: 15px !important;
        }
        [data-testid="stTabs"] [role="tab"] {
            font-size: 12px !important;
            padding: 0.3rem 0.4rem !important;
        }
    }

    /* ══════════════════════════════════════════════════
       📱 iPhone / iOS Safari 专项优化
       ══════════════════════════════════════════════════ */

    /* iOS 安全区（刘海屏/动态岛适配） */
    @supports (padding: max(0px)) {
        .block-container {
            padding-left:  max(0.5rem, env(safe-area-inset-left))  !important;
            padding-right: max(0.5rem, env(safe-area-inset-right)) !important;
            padding-bottom:max(2rem,   env(safe-area-inset-bottom)) !important;
        }
    }

    /* 禁止 iOS Safari 双击缩放（保持布局稳定） */
    * { touch-action: manipulation; }

    /* 禁止 iOS 自动调整字体大小 */
    html { -webkit-text-size-adjust: 100% !important; text-size-adjust: 100% !important; }

    /* iOS input 不自动放大（防止点击 input 时页面跳动） */
    input, textarea, select {
        font-size: 16px !important;  /* iOS 不缩放 >= 16px 的 input */
    }

    /* 滚动容器：iOS 弹性滚动 */
    div[data-testid="stDataFrame"],
    [data-testid="stTabs"] [role="tablist"] {
        -webkit-overflow-scrolling: touch !important;
    }

    /* iOS Safari 按钮去掉默认样式 */
    div.stButton > button {
        -webkit-appearance: none !important;
        appearance: none !important;
    }

    /* 移动端隐藏侧边栏展开按钮的遮挡层 */
    @media (max-width: 768px) {
        /* Streamlit 顶部工具栏：简化显示 */
        header[data-testid="stHeader"] {
            background: rgba(241,245,249,0.95) !important;
            backdrop-filter: blur(8px) !important;
            -webkit-backdrop-filter: blur(8px) !important;
        }

        /* 表格文字不换行（横向滑动代替） */
        div[data-testid="stDataFrame"] td,
        div[data-testid="stDataFrame"] th {
            white-space: nowrap !important;
            font-size: 12px !important;
        }

        /* expander 标题：移动端更大点击区 */
        [data-testid="stExpander"] summary {
            padding: 0.8rem 0.6rem !important;
            font-size: 12px !important;
        }

        /* metric 卡片间距收紧 */
        div[data-testid="stMetric"] {
            padding: 0.4rem !important;
        }

        /* caption 字体缩小 */
        [data-testid="stCaptionContainer"] p,
        .stCaption {
            font-size: 12px !important;
        }

        /* Top30 宏观风险面板：移动端紧凑 */
        .macro-risk-panel {
            padding: 8px !important;
        }

        /* 【V88.14】行业热力卡片 — 手机端 */
        .heat-sector-card {
            background: #fff;
            border: 1px solid #e2e8f0;
            border-radius: 10px;
            padding: 12px 14px;
            margin-bottom: 10px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.06);
        }
        .heat-card-title { font-weight: 700; font-size: 13px; color: #1e293b; }
        .heat-card-sub { font-size: 12px; color: #64748b; margin: 4px 0 6px; }
        .heat-card-body { font-size: 12px; color: #374151; line-height: 1.6; }
        .heat-card-forum { font-size: 12px; color: #8b5cf6; margin-top: 6px; }
        .heat-summary-card {
            background: #fff;
            padding: 10px 12px;
            border-radius: 8px;
            margin-bottom: 8px;
            border: 1px solid #e2e8f0;
        }
    }

    /* 热力汇总卡片 — 桌面端也适用 */
    .heat-sector-card {
        background: #fff;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 12px 14px;
        margin-bottom: 10px;
    }
    .heat-card-title { font-weight: 700; font-size: 13px; color: #1e293b; }
    .heat-card-sub { font-size: 12px; color: #64748b; margin: 4px 0 6px; }
    .heat-card-body { font-size: 12px; color: #374151; line-height: 1.6; }
    .heat-card-forum { font-size: 12px; color: #8b5cf6; margin-top: 6px; }
    .heat-summary-card {
        background: #fff;
        padding: 10px 12px;
        border-radius: 8px;
        margin-bottom: 8px;
        border: 1px solid #e2e8f0;
    }
</style>
""", unsafe_allow_html=True)

# 注入 viewport meta + iOS standalone 支持
st.markdown("""
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="theme-color" content="#f1f5f9">
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# 1. 配置常量（V88：使用新模块）
# ═══════════════════════════════════════════════════════════════
# 【V89.5】注释：MY_GEMINI_KEY和GEMINI_MODEL_NAME已在前面定义
if USE_NEW_MODULES:
    # MY_GEMINI_KEY = mod_config.GEMINI_API_KEY  # 已在前面定义
    # GEMINI_MODEL_NAME = mod_config.GEMINI_MODEL_NAME  # 已在前面定义
    CACHE_TTL = mod_config.CACHE_TTL_SECONDS
    CACHE_MAX_SIZE_MB = mod_config.CACHE_MAX_SIZE_MB
    logging.info(f"✅ 使用V88配置模块: Gemini={GEMINI_MODEL_NAME}, 缓存={CACHE_MAX_SIZE_MB}MB")
else:
    # MY_GEMINI_KEY = st.secrets.get("GEMINI_API_KEY", os.getenv("GEMINI_API_KEY", ""))  # 已在前面定义
    # GEMINI_MODEL_NAME = "gpt-6-astra"  # 已在前面定义
    CACHE_TTL = 900  # 交易日15分钟
    CACHE_MAX_SIZE_MB = 1500

# 【V87.11】配置 Gemini API（已在前面配置）
# if HAS_GEMINI and MY_GEMINI_KEY:
#     genai.configure(api_key=MY_GEMINI_KEY)

# 【自选股】按中美港划分，与钉钉日报同源，可编辑
WATCHLIST = {
    "US": [
        ("ABBV", "艾伯维"), ("ACMR", "ACM Research"), ("NVDA", "英伟达"), ("NVO", "诺和诺德"),
        ("VOO", "标普500ETF"), ("BRK.B", "伯克希尔"), ("QQQM", "纳指100ETF"),
        ("GOOG", "谷歌"), ("PM", "菲利普莫里斯"), ("LLY", "礼来制药"), ("TSM", "台积电"),
        ("TSLA", "特斯拉"),
    ],
    "HK": [
        ("0700.HK", "腾讯控股"), ("0883.HK", "中国海洋石油"), ("1299.HK", "友邦保险"),
        ("0941.HK", "中国移动"),
    ],
    "CN": [
        ("600519.SS", "贵州茅台"), ("688981.SS", "中芯国际"), ("601899.SS", "紫金矿业"),
    ],
}

# ═══════════════════════════════════════════════════════════════
# 【V96.1】动态自选股：watchlist.json 持久化 · 搜索过的个股自动加入 · 上限20只
# ═══════════════════════════════════════════════════════════════
_WATCHLIST_FILE = Path(__file__).parent / "watchlist.json"
_SEARCH_HIST_FILE = Path(__file__).parent / "search_history.json"

def _search_history_persist(code, name):
    """【V88·搜索习惯】搜索历史落盘（次数+最近时间）——关注股预警的依据之一"""
    try:
        d = {}
        if _SEARCH_HIST_FILE.exists():
            d = json.loads(_SEARCH_HIST_FILE.read_text(encoding="utf-8"))
        key = _canonical_code(code)  # 归一：搜"腾讯"(0700.HK)与"腾讯控股"(00700.HK)合并同一条
        e = d.get(key) or {"name": name, "n": 0}
        e["n"] = int(e.get("n", 0)) + 1
        e["name"], e["ts"] = name, time.time()
        d[key] = e
        _SEARCH_HIST_FILE.write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        pass

_WATCHLIST_MAX = 20

def _watchlist_save(d):
    # 【V88·重点观察同步】搜索过的个股自动入观察池，镜像到私仓目录随日报提交上云
    try:
        import json as _j
        (core_root() / "watchlist_v88.json").write_text(
            _j.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        pass
    try:
        _WATCHLIST_FILE.write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception as _e:
        logging.warning(f"自选股保存失败: {_e}")

def _watchlist_load():
    try:
        d = json.loads(_WATCHLIST_FILE.read_text(encoding="utf-8"))
        if isinstance(d, dict) and any(k in d for k in ("US", "HK", "CN")):
            # 旧数据曾把裸6位A股代码误放进US；读取时规范代码、按真实市场归类并去重。
            clean = {"US": [], "HK": [], "CN": []}
            seen = set()
            for rows in d.values():
                for row in rows or []:
                    if not isinstance(row, (list, tuple)) or len(row) < 2:
                        continue
                    code, name = str(row[0]).strip().upper(), str(row[1])
                    if code.isdigit() and len(code) == 6:
                        code += ".SS" if code[0] in "569" else ".SZ"
                    if code.endswith(".SH"):
                        code = code[:-3] + ".SS"
                    # 按归一代码去重：00700.HK 与 0700.HK 视为同一只，保留先出现的（名称更规范）。
                    canon = _canonical_code(code)
                    if canon in seen:
                        continue
                    seen.add(canon)
                    clean[_watchlist_market(code)].append((code, name))
            if clean != {k: [tuple(x) for x in d.get(k, [])] for k in ("US", "HK", "CN")}:
                _watchlist_save(clean)
            return clean
    except Exception:
        pass
    d = {k: list(v) for k, v in WATCHLIST.items()}  # 首次从内置初始化
    _watchlist_save(d)
    return d

def _watchlist_market(code):
    c = str(code).upper()
    if c.endswith(".HK"):
        return "HK"
    if c.endswith((".SS", ".SH", ".SZ")) or (c.isdigit() and len(c) == 6):
        return "CN"
    return "US"

def _watchlist_add(code, name):
    """搜索选中的个股自动加入自选股；总量>20 时从最长的市场列表头部淘汰最早的。"""
    d = _watchlist_load()
    mkt = _watchlist_market(code)
    # 归一去重：搜索"腾讯"补进 0700.HK 时，已有 00700.HK 就不再重复加。
    _canon = _canonical_code(code)
    if any(_canonical_code(c) == _canon for c, _ in d.get(mkt, [])):
        return False
    d.setdefault(mkt, []).append((code, name))
    while sum(len(v) for v in d.values()) > _WATCHLIST_MAX:
        big = max(d, key=lambda k: len(d[k]))
        if d[big]:
            d[big].pop(0)
    _watchlist_save(d)
    return True

def _watchlist_remove(code):
    d = _watchlist_load()
    for k in d:
        d[k] = [(c, n) for c, n in d[k] if c != code]
    _watchlist_save(d)

# 动态覆盖内置（Streamlit 每次交互重跑脚本，文件读取保持最新）
WATCHLIST = _watchlist_load()

# 【V88·搜索前置】自选/历史工具就绪后，立刻把个股搜索回填到页顶槽位。
# 比原先渲染在深度作战室（约¾页处）大幅提前，点开首屏即可搜索。
try:
    with _v88_search_slot.container():
        render_cloud_search()
except Exception as _v88_search_slot_err:
    st.caption(f"顶部搜索暂不可用（不影响下方功能）：{str(_v88_search_slot_err)[:60]}")

# 【V88·自选分级弹窗】A=对应市场交易日盘中每3小时 B=每天 C=每周低频
if st.session_state.get("_wl_new_pick"):
    @st.dialog("🏷️ 设置自选关注级别")
    def _wl_pick_level_dialog():
        _cdp, _nmp = st.session_state["_wl_new_pick"]
        st.markdown(f"**{_nmp}**（{_cdp}）已加入自选，选择关注级别：")
        _lv_desc = {"A": "A · 重点关注（对应市场交易日盘中每3小时）", "B": "B · 观察（每天扫描1次,默认）",
                    "C": "C · 长期跟踪（每周低频扫描）"}
        _lv_sel = st.radio("级别", ["A", "B", "C"], index=1,
                           format_func=lambda x: _lv_desc[x], key="_wl_lv_radio")
        if st.button("✅ 确定", type="primary", key="_wl_lv_ok"):
            try:
                import sys as _syslv
                _repo_lv = core_root()
                if str(_repo_lv / "src") not in _syslv.path:
                    _syslv.path.insert(0, str(_repo_lv / "src"))
                from watch_alerts import watch_levels as _lvl, save_watch_levels as _lvs
                _d = _lvl()
                _d[str(_cdp)] = _lv_sel
                _lvs(_d)
                import subprocess as _splv
                _splv.run(["git", "-C", str(_repo_lv), "add", "-f", "watch_levels.json"], capture_output=True)
                _splv.run(["git", "-C", str(_repo_lv), "commit", "-m", f"自选分级: {_nmp}={_lv_sel}"], capture_output=True)
                _splv.Popen(["git", "-C", str(_repo_lv), "push", "origin", "main"],
                            stdout=_splv.DEVNULL, stderr=_splv.DEVNULL)
            except Exception:
                pass
            st.session_state.pop("_wl_new_pick", None)
            st.rerun()
    _wl_pick_level_dialog()

# ═══════════════════════════════════════════════════════════════
# 1.5 【V88】本地文件缓存系统（使用新的LRU版本）
# ═══════════════════════════════════════════════════════════════
if USE_NEW_MODULES:
    # 使用新的LRU缓存系统
    logging.info("✅ 使用V88 LRU缓存系统")
    LocalFileCache = mod_cache.LocalFileCache
else:
    # 使用原版缓存系统
    class LocalFileCache:
        """
        本地文件缓存系统
        - 缓存存储在本地文件中，刷新页面不丢失
        - 5分钟过期时间
        - 500MB容量限制，超出自动清理最旧的缓存
        """
        def __init__(self, cache_dir=".cache_stock_data", max_size_mb=500, ttl_seconds=900):
            self.cache_dir = Path(cache_dir)
            self.cache_dir.mkdir(exist_ok=True)
            self.max_size_bytes = max_size_mb * 1024 * 1024
            self.ttl_seconds = ttl_seconds
        
        def _get_cache_key(self, key_str):
            """生成缓存文件名"""
            return hashlib.md5(key_str.encode()).hexdigest()
        
        def _get_cache_path(self, cache_key):
            """获取缓存文件路径"""
            return self.cache_dir / f"{cache_key}.pkl"
        
        def _get_cache_size(self):
            """获取缓存目录总大小（字节）"""
            total_size = 0
            for file in self.cache_dir.glob("*.pkl"):
                try:
                    total_size += file.stat().st_size
                except:
                    pass
            return total_size
        
        def _clean_old_cache(self):
            """清理缓存：满500MB直接清零重新开始"""
            current_size = self._get_cache_size()
            
            if current_size <= self.max_size_bytes:
                return
            
            # 【V87.15改进】达到容量限制，直接清空所有缓存
            _safe_print(f"[缓存清理] ⚠️ 容量已满 ({current_size/1024/1024:.1f}MB / {self.max_size_bytes/1024/1024:.0f}MB)")
            _safe_print(f"[缓存清理] 🗑️ 清空所有缓存，重新开始...")
            
            deleted_count = 0
            deleted_size = 0
            
            for file in self.cache_dir.glob("*.pkl"):
                try:
                    size = file.stat().st_size
                    file.unlink()
                    deleted_count += 1
                    deleted_size += size
                except Exception as e:
                    _safe_print(f"[缓存清理] ❌ 删除失败 {file.name}: {e}")
            
            _safe_print(f"[缓存清理] ✅ 已清空 {deleted_count} 个文件，释放 {deleted_size/1024/1024:.1f}MB")
        
        def get(self, key_str):
            """【V87.16】获取缓存 - 增强错误处理"""
            cache_key = self._get_cache_key(key_str)
            cache_path = self._get_cache_path(cache_key)
            
            if not cache_path.exists():
                return None
            
            try:
                # 检查是否过期
                mtime = cache_path.stat().st_mtime
                age = time.time() - mtime
                
                if age > self.ttl_seconds:
                    # 过期，删除
                    cache_path.unlink()
                    logging.debug(f"缓存过期已删除: {key_str[:50]}... (年龄: {age:.1f}秒)")
                    return None
                
                # 【V87.16】安全的pickle加载
                with open(cache_path, 'rb') as f:
                    data = pickle.load(f)
                
                logging.info(f"✅ 缓存命中: {key_str[:50]}... (年龄: {age:.1f}秒)")
                return data
            
            except (pickle.UnpicklingError, EOFError, ValueError) as e:
                # 【V87.16】pickle损坏，删除并重新获取
                logging.warning(f"⚠️ 缓存文件损坏: {type(e).__name__}, 已删除")
                try:
                    cache_path.unlink()
                except:
                    pass
                return None
            
            except Exception as e:
                logging.error(f"❌ 缓存读取失败: {type(e).__name__}: {str(e)[:100]}")
                try:
                    cache_path.unlink()
                except:
                    pass
                return None
        
        def set(self, key_str, data):
            """设置缓存"""
            cache_key = self._get_cache_key(key_str)
            cache_path = self._get_cache_path(cache_key)
            
            try:
                # 保存缓存
                with open(cache_path, 'wb') as f:
                    pickle.dump(data, f)
                
                # 检查并清理容量
                self._clean_old_cache()
                
                _safe_print(f"[缓存保存] {key_str[:50]}...")
            
            except Exception as e:
                _safe_print(f"[缓存保存失败] {type(e).__name__}: {str(e)[:100]}")
        
        def clear_all(self):
            """清空所有缓存"""
            try:
                shutil.rmtree(self.cache_dir)
                self.cache_dir.mkdir(exist_ok=True)
                _safe_print("[缓存清空] 所有缓存已清除")
            except Exception as e:
                _safe_print(f"[缓存清空失败] {type(e).__name__}: {str(e)[:100]}")
        
        def get_stats(self):
            """获取缓存统计信息"""
            try:
                total_size = self._get_cache_size()
                file_count = len(list(self.cache_dir.glob("*.pkl")))
                
                return {
                    'total_size_mb': total_size / 1024 / 1024,
                    'file_count': file_count,
                    'max_size_mb': self.max_size_bytes / 1024 / 1024,
                    'usage_percent': (total_size / self.max_size_bytes) * 100 if self.max_size_bytes > 0 else 0,
                    'ttl_seconds': self.ttl_seconds
                }
            except:
                return {'total_size_mb': 0, 'file_count': 0, 'max_size_mb': self.max_size_bytes / 1024 / 1024, 'usage_percent': 0, 'ttl_seconds': self.ttl_seconds}

# 【V88】初始化全局缓存实例（使用新的LRU系统）
if USE_NEW_MODULES:
    local_cache = mod_cache.get_cache(
        cache_dir=mod_config.CACHE_DIR,
        max_size_mb=CACHE_MAX_SIZE_MB,
        ttl_seconds=CACHE_TTL
    )
    logging.info(f"✅ V88 LRU缓存已初始化: {CACHE_MAX_SIZE_MB}MB, TTL={CACHE_TTL}s")
else:
    local_cache = LocalFileCache(max_size_mb=CACHE_MAX_SIZE_MB, ttl_seconds=CACHE_TTL)

# 【V87.15】容量评估说明
# 单只股票数据约 0.3-0.5MB（包含DataFrame + 元数据）
# 680只股票池 × 0.4MB = 272MB
# 考虑重复查询、扫描结果等，实际使用约 400-600MB/天
# 1.5GB 容量可支持约 3天的数据缓存

# ═══════════════════════════════════════════════════════════════
# 2. ProxyContext 类（V72 核心技术）
# ═══════════════════════════════════════════════════════════════
class ProxyContext:
    def __init__(self, proxy_url):
        self.proxy_url = proxy_url
        self.old_env = {}
    
    def __enter__(self):
        if self.proxy_url:
            for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY']:
                self.old_env[key] = os.environ.get(key)
                os.environ[key] = self.proxy_url
        return self
    
    def __exit__(self, *args):
        for key, val in self.old_env.items():
            if val is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = val

def get_proxy_url():
    """
    返回代理 URL。优先级：
    1. secrets.toml 里的 PROXY_URL（云端/生产环境显式配置）
    2. session_state 里用户手动设置的 proxy_port（本地调试）
    3. 环境变量 HTTPS_PROXY（系统级代理）
    4. 无代理（直连）—— Streamlit Cloud 标准情况
    """
    # 1. secrets 里显式配置
    try:
        proxy = st.secrets.get("PROXY_URL", "")
        if proxy:
            return proxy
    except Exception:
        pass
    # 2. Streamlit Cloud / 云端环境：检测到 STREAMLIT_SHARING_MODE 时不用本地代理
    if os.environ.get("STREAMLIT_SHARING_MODE") or os.environ.get("HOME", "").startswith("/home/"):
        return None
    # 3. 代理不可用时直连
    if _is_proxy_dead():
        return None
    # 4. 用户在 session_state 里手动设置了端口，或自动探测系统代理
    port = st.session_state.get("proxy_port", "") or _detect_system_proxy_port()
    if port:
        return f"http://127.0.0.1:{port}"
    # 5. 直连
    return None

# ═══════════════════════════════════════════════════════════════
# 3. 数据获取 - 【V75核心】添加重试机制
# ═══════════════════════════════════════════════════════════════
def clean_df(df):
    """清洗数据"""
    if df is None or df.empty: return None
    if isinstance(df.columns, pd.MultiIndex):
        try: df.columns = df.columns.get_level_values(0)
        except: pass
    df = df.rename(columns=lambda x: x.capitalize())
    cols_map = {'Date':'Date','Open':'Open','High':'High','Low':'Low','Close':'Close','Volume':'Volume'}
    df = df.rename(columns=cols_map)
    needed = ['Open', 'High', 'Low', 'Close']
    if not all(c in df.columns for c in needed): return None
    for c in needed: df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.dropna()
    if 'Volume' not in df.columns: df['Volume'] = 0
    return df

# 【V82.13新增】Stooq 数据源（美股/指数备用）
def fetch_from_stooq(symbol: str):
    """
    从 Stooq 获取数据（免费、无需 API Key）
    适用于：美股、指数、ETF
    不适用：港股、A股
    使用 requests 下载 CSV，比 pd.read_csv(url) 更稳定，支持超时和重试。
    """
    try:
        # Stooq 不支持港股和A股
        if symbol.endswith('.HK') or symbol.endswith('.SS') or symbol.endswith('.SZ'):
            return None

        # 符号映射：^VIX->vi.f, ^TNX->tnx.us, DX-Y.NYB->dx.f 等
        _MAP = {
            'DX-Y.NYB': 'dx.f',
            'CNY=X': 'cnyusd.fx',
            'HKD=X': 'hkdusd.fx',
            '^VIX': 'vi.f',
            '^TNX': 'tnx.us',
            '^GSPC': 'sp500.us',
            'GLD': 'gld.us',
            'SPY': 'spy.us',
            'TLT': 'tlt.us',
            'QQQ': 'qqq.us',
        }
        stooq_sym = _MAP.get(symbol)
        if not stooq_sym:
            raw = symbol.replace('^', '').replace('.', '').replace('-', '').replace('=', '').lower()
            stooq_sym = f"{raw}.us"

        url = f"https://stooq.com/q/d/l/?s={stooq_sym}&i=d"
        _safe_print(f"[Stooq] 请求 {symbol} → {url}")

        resp = _DIRECT_SESSION.get(url, timeout=5,
                            headers={'User-Agent': 'Mozilla/5.0'})
        if resp.status_code != 200 or len(resp.content) < 50:
            _safe_print(f"[Stooq] ❌ {symbol} HTTP {resp.status_code}")
            return None

        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))

        if df.empty or 'Close' not in df.columns:
            _safe_print(f"[Stooq] ❌ {symbol} 返回数据无 Close 列")
            return None

        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        df = df.sort_index()
        _safe_print(f"[Stooq] ✅ {symbol} 获取 {len(df)} 行")
        return clean_df(df)
    except Exception as e:
        _safe_print(f"[Stooq] ❌ {symbol} 失败: {type(e).__name__}: {e}")
        return None


def fetch_from_yahoo_direct(symbol: str, period: str = '1y') -> pd.DataFrame:
    """
    直连 Yahoo Finance v8 chart JSON API（不经过 yfinance 封装）。
    yfinance 在 Streamlit Cloud 上因 IP 限制常失败，
    直连 API 使用自定义 User-Agent 可绕过部分限制。
    适用于：美股、ETF、指数（^VIX、^TNX、SPY 等）
    """
    if _yf_is_rate_limited():
        _safe_print(f"[YahooV8] ⏭️ {symbol} 跳过（Yahoo rate limit 冷却中）")
        return None
    _RANGE_MAP = {
        '6mo': '6mo', '1y': '1y', '2y': '2y',
        '3y': '3y', '5y': '5y',
        '1mo': '1mo', '3mo': '3mo',
    }
    range_str = _RANGE_MAP.get(period, '1y')
    if symbol.endswith('.HK') or symbol.endswith('.SS') or symbol.endswith('.SZ'):
        return None
    try:
        url = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}'
        params = {'interval': '1d', 'range': range_str, 'events': 'div,splits'}
        hdrs = {
            'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                           'AppleWebKit/537.36 (KHTML, like Gecko) '
                           'Chrome/124.0.0.0 Safari/537.36'),
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://finance.yahoo.com/',
        }
        r = _DIRECT_SESSION.get(url, params=params, headers=hdrs, timeout=5)
        if r.status_code != 200:
            _safe_print(f"[YahooV8] ❌ {symbol} HTTP {r.status_code}")
            return None
        data = r.json()
        result = data.get('chart', {}).get('result')
        if not result:
            _safe_print(f"[YahooV8] ❌ {symbol} 无数据")
            return None
        result = result[0]
        timestamps = result.get('timestamp', [])
        quote = result.get('indicators', {}).get('quote', [{}])[0]
        closes  = quote.get('close',  [])
        opens   = quote.get('open',   [])
        highs   = quote.get('high',   [])
        lows    = quote.get('low',    [])
        volumes = quote.get('volume', [])
        if not timestamps or not closes:
            return None
        df = pd.DataFrame({
            'Open': opens, 'High': highs, 'Low': lows,
            'Close': closes, 'Volume': volumes,
        }, index=pd.to_datetime(timestamps, unit='s', utc=True).tz_convert(None))
        df.index.name = 'Date'
        df = df.dropna(subset=['Close'])
        _safe_print(f"[YahooV8] ✅ {symbol} 获取 {len(df)} 行")
        return clean_df(df)
    except Exception as e:
        _safe_print(f"[YahooV8] ❌ {symbol} 失败: {type(e).__name__}: {e}")
        return None


def fetch_cyb_from_eastmoney():
    """
    【V91.7】创业板指399006专用：东方财富fqt=0（指数不复权）
    Yahoo 对 399006 不稳定，东方财富指数需用 fqt=0
    """
    try:
        em_url = "https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=0.399006&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58&klt=101&fqt=0&end=20500101&lmt=252"
        r = _DIRECT_SESSION.get(em_url, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        if not data.get('data') or not data['data'].get('klines'):
            return None
        rows = []
        for line in data['data']['klines']:
            parts = line.split(',')
            if len(parts) >= 6:
                rows.append({'Date': parts[0], 'Open': float(parts[1]), 'Close': float(parts[2]), 'High': float(parts[3]), 'Low': float(parts[4]), 'Volume': float(parts[5])})
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        return df
    except Exception:
        return None

def fetch_stock_data(code, return_source=False, return_quality=False):
    """
    【V87.15】数据获取 + 本地文件缓存（5分钟，500MB限制）
    
    数据源优先级：
    1. 本地文件缓存（5分钟内有效）
    2. yfinance（主力）
    3. Stooq（美股/指数备用）
    4. 东方财富（A股备用）
    
    参数：
        code: 股票代码
        return_source: 是否返回数据源信息
        return_quality: 是否返回数据质量元数据
    
    返回：
        - return_quality=True: (df, data_quality_dict)
        - return_source=True: (df, source_str)
        - 默认: df
    """
    # 【V85 Critical Fix】第一行就强制转换代码格式
    target_code = to_yf_cn_code(code)
    
    # 【V87.15】尝试从本地缓存获取
    cache_key = f"stock_data_{target_code}_{return_source}_{return_quality}"
    cached_result = local_cache.get(cache_key)
    if cached_result is not None:
        _safe_print(f"[fetch] ✅ 缓存命中: {code} -> {target_code}")
        return cached_result
    
    _safe_print(f"[fetch] 代码转换: {code} -> {target_code}")
    proxy_url = get_proxy_url()
    data_source = "无数据"

    # 【V92】A股优先 免费行情源（直连不走代理、~0.2s、稳定）——扫描提速 & 修复 A 股 N/A
    if target_code.endswith('.SS') or target_code.endswith('.SZ'):
        try:
            from market_data_helper import fetch_daily_free as _ts_daily
            _ts_df = _ts_daily(target_code, days=400)
            if _ts_df is not None and len(_ts_df) >= 30:
                _safe_print(f"[fetch] ✅ 免费行情源 获取 {target_code}")
                data_quality = {
                    'source': _ts_df.attrs.get('source', '免费核验日线'), 'last_updated': pd.Timestamp.now(),
                    'is_delayed': True, 'data_points': len(_ts_df),
                    'date_range': f"{_ts_df.index[0].date()} 至 {_ts_df.index[-1].date()}"
                }
                if return_quality:
                    result = (_ts_df, data_quality)
                elif return_source:
                    result = (_ts_df, _ts_df.attrs.get('source', '免费核验日线'))
                else:
                    result = _ts_df
                local_cache.set(cache_key, result)
                return result
        except Exception as _ts_e:
            _safe_print(f"[fetch] 免费行情源 {target_code} 失败: {_ts_e}")

    # 【速度优化】东方财富万能源作为第一数据源（带熔断：不可达时全局跳过，避免每只都卡超时）
    if not _em_blocked():
        try:
            _em_df = fetch_from_eastmoney_universal(target_code, period='1y')
            if _em_df is not None and len(_em_df) > 0:
                _em_mark(True)
                _safe_print(f"[fetch] ✅ 东财万能源获取 {target_code}")
                data_source = "eastmoney"
                data_quality = {
                    'source': '东方财富', 'last_updated': pd.Timestamp.now(),
                    'is_delayed': True, 'data_points': len(_em_df),
                    'date_range': f"{_em_df.index[0].date()} 至 {_em_df.index[-1].date()}" if len(_em_df) > 0 else None
                }
                if return_quality:
                    result = (_em_df, data_quality)
                elif return_source:
                    result = (_em_df, data_source)
                else:
                    result = _em_df
                local_cache.set(cache_key, result)
                return result
            else:
                _em_mark(False)
        except Exception as _em_e:
            _em_mark(False)
            _safe_print(f"[fetch] ⚠️ 东财万能源 {target_code} 失败: {_em_e}")
    
    # 【V83 P0.1】数据质量元数据
    data_quality = {
        'source': '无数据',
        'last_updated': None,
        'is_delayed': True,
        'data_points': 0,
        'date_range': None
    }
    
    # ═══ 1️⃣ 主力：yfinance（rate limit + OperationalError 熔断保护）═══
    if HAS_YFINANCE and not _yf_is_rate_limited() and not _yf_opserr_blocked():
        param_combinations = [
            {"period": "1y", "auto_adjust": False},
            {"period": "6mo", "auto_adjust": False},
        ]
        
        _hit_rate_limit = False
        _hit_opserr = False
        for idx, params in enumerate(param_combinations):
            if _hit_rate_limit or _hit_opserr:
                break
            for retry in range(1):
                try:
                    with ProxyContext(proxy_url):
                        tk = yf.Ticker(_normalize_hk_for_yahoo(target_code))
                        df = tk.history(**params, timeout=5)
                        cleaned = clean_df(df)
                        if cleaned is not None and len(cleaned) > 0:
                            logging.info(f"✅ {target_code} YFinance成功 (参数{idx+1}, 重试{retry+1}/3)")
                            data_source = "yfinance"
                            
                            data_quality['source'] = 'Yahoo Finance'
                            data_quality['last_updated'] = pd.Timestamp.now()
                            data_quality['is_delayed'] = True
                            data_quality['data_points'] = len(cleaned)
                            data_quality['date_range'] = f"{cleaned.index[0].date()} 至 {cleaned.index[-1].date()}"
                            
                            if return_quality:
                                result = (cleaned, data_quality)
                            elif return_source:
                                result = (cleaned, data_source)
                            else:
                                result = cleaned
                            
                            local_cache.set(cache_key, result)
                            return result
                except Exception as e:
                    _err_name = type(e).__name__
                    _err_str = str(e)
                    if _yf_check_operational_error(e):
                        logging.warning(f"⚠️ {target_code} OperationalError，跳过 yfinance 走备用源")
                        _hit_opserr = True
                        break
                    if 'Rate' in _err_str or 'Too Many' in _err_str or 'RateLimit' in _err_name:
                        _yf_mark_rate_limited()
                        _hit_rate_limit = True
                        break
                    if retry < 2:
                        wait_time = 0.5 * (2 ** retry)
                        logging.warning(f"⚠️ {target_code} YFinance失败 (参数{idx+1}, 重试{retry+1}/3): {_err_name}, 等待{wait_time}s")
                        time.sleep(wait_time)
                        continue
                    else:
                        logging.error(f"❌ {target_code} YFinance参数{idx+1}全部失败")
                        break
        
        _safe_print(f"[fetch] ⚠️ {target_code} YFinance全部尝试失败，尝试备用源...")
    elif _yf_is_rate_limited():
        _safe_print(f"[fetch] ⏭️ {target_code} 跳过 yfinance（rate limit 冷却中）")
    elif _yf_opserr_blocked():
        _safe_print(f"[fetch] ⏭️ {target_code} 跳过 yfinance（OperationalError 冷却中）")

    # ═══ 1.5️⃣ 【自动修复】港股代码格式容错：自动尝试所有格式变体 ═══
    if target_code.endswith('.HK'):
        from modules.utils import get_hk_code_variants
        _variants = get_hk_code_variants(target_code)
        _variants = [v for v in _variants if v != target_code]  # 排除已试过的主代码
        if _variants:
            _safe_print(f"[fetch][自动修复] {target_code} 港股格式容错，尝试变体: {_variants}")
            for _alt_code in _variants:
                for _params in [{"period": "1y", "auto_adjust": False}, {"period": "6mo", "auto_adjust": True}]:
                    try:
                        with ProxyContext(proxy_url):
                            _tk = yf.Ticker(_alt_code)
                            _df = _tk.history(**_params, timeout=8)
                            _cleaned = clean_df(_df)
                            if _cleaned is not None and len(_cleaned) > 0:
                                _safe_print(f"[fetch][自动修复] ✅ 港股格式容错成功: {target_code} -> {_alt_code}")
                                data_quality['source'] = f'Yahoo Finance (自动修复:{_alt_code})'
                                data_quality['last_updated'] = pd.Timestamp.now()
                                data_quality['is_delayed'] = True
                                data_quality['data_points'] = len(_cleaned)
                                data_quality['date_range'] = f"{_cleaned.index[0].date()} 至 {_cleaned.index[-1].date()}"
                                result = (_cleaned, data_quality) if return_quality else (_cleaned, f"yfinance(自动修复:{_alt_code})") if return_source else _cleaned
                                local_cache.set(cache_key, result)
                                return result
                    except Exception:
                        continue
            _safe_print(f"[fetch][自动修复] ❌ 港股所有格式变体均失败: {_variants}")

    # ═══ 1.8️⃣ 东方财富万能源（Yahoo 被封时主力替代，带熔断）═══
    if not _em_blocked():
        try:
            _em_univ = fetch_from_eastmoney_universal(target_code)
            if _em_univ is not None and len(_em_univ) > 0:
                _em_mark(True)
                _safe_print(f"[fetch] ✅ {target_code} 东财万能源成功")
                data_quality['source'] = '东方财富'
                data_quality['last_updated'] = pd.Timestamp.now()
                data_quality['is_delayed'] = True
                data_quality['data_points'] = len(_em_univ)
                data_quality['date_range'] = f"{_em_univ.index[0].date()} 至 {_em_univ.index[-1].date()}"
                if return_quality:
                    result = (_em_univ, data_quality)
                elif return_source:
                    result = (_em_univ, "东方财富")
                else:
                    result = _em_univ
                local_cache.set(cache_key, result)
                return result
            else:
                _em_mark(False)
        except Exception as _em_e:
            _em_mark(False)
            _safe_print(f"[fetch] 东财万能源 {target_code} 失败: {_em_e}")

    # ═══ 2️⃣ 备用：Stooq（仅美股/指数）═══
    if not target_code.endswith('.HK') and not target_code.endswith('.SS') and not target_code.endswith('.SZ'):
        _safe_print(f"[fetch] 🔄 {target_code} 尝试Stooq备用源...")
        df_stooq = fetch_from_stooq(target_code)
        if df_stooq is not None and len(df_stooq) > 0:
            _safe_print(f"[fetch] ✅ {target_code} Stooq成功（备用源）")
            data_source = "stooq(备用)"
            
            # 【V83 P0.1】填充数据质量元数据
            data_quality['source'] = 'Stooq (备用)'
            data_quality['last_updated'] = pd.Timestamp.now()
            data_quality['is_delayed'] = True  # Stooq通常T+1延迟
            data_quality['data_points'] = len(df_stooq)
            data_quality['date_range'] = f"{df_stooq.index[0].date()} 至 {df_stooq.index[-1].date()}"
            
            # 【V87.15】保存到本地缓存
            if return_quality:
                result = (df_stooq, data_quality)
            elif return_source:
                result = (df_stooq, data_source)
            else:
                result = df_stooq
            
            local_cache.set(cache_key, result)
            return result
        else:
            _safe_print(f"[fetch] ❌ {target_code} Stooq也失败")
    
    # ═══ 3️⃣ 【V84.2】第三层备用：东方财富（仅A股）═══
    if target_code.endswith('.SS') or target_code.endswith('.SZ'):
        _safe_print(f"[fetch] 🔄 {target_code} 尝试东方财富备用源...")
        try:
            # 东方财富日线接口（简化版，仅获取基础数据）
            # 【V91.7】创业板指399006等指数需用fqt=0（不复权），fqt=1对指数可能返回空
            secid = f"1.{target_code.replace('.SS', '')}" if target_code.endswith('.SS') else f"0.{target_code.replace('.SZ', '')}"
            is_index = target_code in ('399006.SZ', '000300.SS', '000001.SS', '399001.SZ')  # 创业板指、沪深300、上证、深证
            fqt_val = 0 if is_index else 1
            em_url = f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58&klt=101&fqt={fqt_val}&end=20500101&lmt=252"
            
            response = _DIRECT_SESSION.get(em_url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('data') and data['data'].get('klines'):
                    klines = data['data']['klines']
                    rows = []
                    for line in klines:
                        parts = line.split(',')
                        if len(parts) >= 6:
                            rows.append({
                                'Date': parts[0],
                                'Open': float(parts[1]),
                                'Close': float(parts[2]),
                                'High': float(parts[3]),
                                'Low': float(parts[4]),
                                'Volume': float(parts[5])
                            })
                    
                    if rows:
                        df_em = pd.DataFrame(rows)
                        df_em['Date'] = pd.to_datetime(df_em['Date'])
                        df_em.set_index('Date', inplace=True)
                        
                        _safe_print(f"[fetch] ✅ {target_code} 东方财富成功（备用源）")
                        data_source = "eastmoney(备用)"
                        
                        # 填充数据质量元数据
                        data_quality['source'] = '东方财富 (备用)'
                        data_quality['last_updated'] = pd.Timestamp.now()
                        data_quality['is_delayed'] = True
                        data_quality['data_points'] = len(df_em)
                        data_quality['date_range'] = f"{df_em.index[0].date()} 至 {df_em.index[-1].date()}"
                        
                        # 【V87.15】保存到本地缓存
                        if return_quality:
                            result = (df_em, data_quality)
                        elif return_source:
                            result = (df_em, data_source)
                        else:
                            result = df_em
                        
                        local_cache.set(cache_key, result)
                        return result
        except Exception as e:
            _safe_print(f"[fetch] ❌ {target_code} 东方财富也失败: {type(e).__name__}")
    
    # ═══ 4️⃣ 【V87.8】所有源失败 - 详细错误信息与建议 ═══
    _safe_print(f"[fetch] ❌❌❌ {target_code} 所有数据源失败 ❌❌❌")
    _safe_print(f"[fetch]     原始代码: {code}")
    _safe_print(f"[fetch]     转换后: {target_code}")
    
    # 【V87.8】提供具体建议
    if target_code.endswith('.HK'):
        _safe_print(f"[fetch] 💡 港股建议:")
        _safe_print(f"[fetch]    1) 检查代码格式是否正确（应为5位数字.HK，如00700.HK）")
        _safe_print(f"[fetch]    2) 股票可能已退市或暂停交易")
        _safe_print(f"[fetch]    3) 尝试在Yahoo Finance网站搜索验证")
    elif target_code.endswith(('.SS', '.SZ')):
        _safe_print(f"[fetch] 💡 A股建议:")
        _safe_print(f"[fetch]    1) 检查网络连接和代理设置（端口{st.session_state.get('proxy_port', '1082')}）")
        _safe_print(f"[fetch]    2) 股票可能停牌或退市")
        _safe_print(f"[fetch]    3) 验证代码格式（沪市.SS，深市.SZ）")
    else:
        _safe_print(f"[fetch] 💡 美股建议:")
        _safe_print(f"[fetch]    1) 验证股票代码是否正确")
        _safe_print(f"[fetch]    2) 股票可能已退市（如ATVI被收购）")
        _safe_print(f"[fetch]    3) 尝试在Yahoo Finance搜索: https://finance.yahoo.com/quote/{target_code}")
    
    # 更新错误元数据
    data_quality['source'] = '无数据'
    data_quality['error_detail'] = f'所有数据源均失败（yfinance/stooq/eastmoney）- 可能已退市或代码错误'
    
    if return_quality:
        return (None, data_quality)
    elif return_source:
        return (None, "无数据")
    else:
        return None


# ═══════════════════════════════════════════════════════════════
# 【V95.1】当日实时补条：解决"底层时间不一致"
# 根因：东财熔断降级到 免费行情源 后，A股日线只有截至昨日的收盘数据，
# 盘中/收盘后显示的"最新价"实为上一交易日收盘价（用户实测宁德时代偏差~1.5%）。
# 修法：fetch_stock_data 外层包装——A股/港股取完日线后，若最后一根K线不是今天，
# 用腾讯实时接口(qt.gtimg.cn，国内直连极稳)把今天这根补上；若是今天但可能是
# 15分钟旧缓存，也用实时价刷新最后一根。扫描/评分/问答/深度分析全链路统一受益。
# ═══════════════════════════════════════════════════════════════
_TX_QUOTE_CACHE = {}  # {gtimg_sym: (ts, quote_dict)} 60秒微缓存

def _tencent_realtime_quote(yf_code: str):
    """腾讯实时行情。支持 .SS/.SZ/.HK。返回 dict 或 None。"""
    import re as _re
    c = str(yf_code).upper()
    if c.endswith(".SS"):
        sym = "sh" + c[:-3]
    elif c.endswith(".SZ"):
        sym = "sz" + c[:-3]
    elif c.endswith(".HK"):
        sym = "hk" + c[:-3].zfill(5)
    else:
        return None
    now = time.time()
    hit = _TX_QUOTE_CACHE.get(sym)
    if hit and now - hit[0] < 60:
        return hit[1]
    try:
        s = _shared_http_session(direct=True)
        r = s.get(f"https://qt.gtimg.cn/q={sym}", timeout=4)
        f = r.text.split("~")
        if len(f) < 35 or not f[3]:
            return None
        q = {
            "cur": float(f[3]), "prev_close": float(f[4] or 0), "open": float(f[5] or 0),
            "vol": float(f[6] or 0) * (100 if sym[:2] in ("sh", "sz") else 1),
            "ts": str(f[30] or ""), "high": float(f[33] or 0), "low": float(f[34] or 0),
        }
        if q["cur"] <= 0:
            return None
        _TX_QUOTE_CACHE[sym] = (now, q)
        return q
    except Exception:
        return None


def _ensure_today_bar(df, yf_code: str):
    """A股/港股：日线最后一根若不是今天(北京时间,交易时段后)，用实时行情补/刷今天这根。幂等。"""
    try:
        if df is None or len(df) == 0:
            return df
        c = str(yf_code).upper()
        if not (c.endswith(".SS") or c.endswith(".SZ") or c.endswith(".HK")):
            return df
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        bj = _dt.now(_tz(_td(hours=8)))
        if bj.weekday() >= 5 or (bj.hour, bj.minute) < (9, 30):
            return df
        last_date = pd.Timestamp(df.index[-1]).date()
        today = bj.date()
        # 最后一根不是今天→补条；是今天→也刷实时价（覆盖15分钟旧缓存）
        q = _tencent_realtime_quote(c)
        if not q or not q["ts"].startswith(bj.strftime("%Y%m%d")):
            return df  # 无实时数据或时间戳不是今天（停牌/休市），保持原样
        # 成交量单位自适应：免费行情源日线是"手"、腾讯是"股"，混用会让量能因子误判100倍
        _v = q["vol"]
        try:
            _med = float(pd.Series(df["Volume"].tail(5)).median())
            if _med > 0 and _v > 0 and _v / _med > 30:
                _v = _v / 100.0
        except Exception:
            pass
        q = dict(q); q["vol"] = _v
        if last_date < today:
            ts = pd.Timestamp(today)
            if getattr(df.index, "tz", None) is not None:
                ts = ts.tz_localize(df.index.tz)
            row = {col: float("nan") for col in df.columns}
            row.update({"Open": q["open"] or q["cur"], "High": q["high"] or q["cur"],
                        "Low": q["low"] or q["cur"], "Close": q["cur"], "Volume": q["vol"]})
            df = pd.concat([df, pd.DataFrame([row], index=[ts])])
        else:
            i = df.index[-1]
            df.loc[i, "Close"] = q["cur"]
            if q["high"]: df.loc[i, "High"] = max(float(df.loc[i, "High"] or 0), q["high"])
            if q["low"]:  df.loc[i, "Low"] = min(float(df.loc[i, "Low"] or q["low"]), q["low"])
            if q["vol"]:  df.loc[i, "Volume"] = max(float(df.loc[i, "Volume"] or 0), q["vol"])
    except Exception as _e:
        logging.debug(f"当日补条失败 {yf_code}: {_e}")
    return df


_fetch_stock_data_core = fetch_stock_data

def fetch_stock_data(code, return_source=False, return_quality=False):
    """【V95.1】外层包装：核心逻辑不变，A股/港股自动补/刷当日实时K线，保证全链路时间一致。"""
    res = _fetch_stock_data_core(code, return_source, return_quality)
    try:
        yf_code = to_yf_cn_code(code)
        if isinstance(res, tuple):
            df = _ensure_today_bar(res[0], yf_code)
            return (df,) + res[1:]
        return _ensure_today_bar(res, yf_code)
    except Exception:
        return res


# ═══════════════════════════════════════════════════════════════
# 【V97】市场温度+板块轮动读取器（评分环境调整层与今日导航共用，10分钟缓存）
# 数据源=云端快照 market_snapshot.json（温度=趋势40%+宽度40%+动量20%，三端同一数）
# ═══════════════════════════════════════════════════════════════
_MKT_TEMP_CACHE = {"ts": 0.0, "data": None}

def _load_market_temp():
    now = time.time()
    if _MKT_TEMP_CACHE["data"] is not None and now - _MKT_TEMP_CACHE["ts"] < 600:
        return _MKT_TEMP_CACHE["data"]
    out = {}
    try:
        _p = core_root() / "data" / "market_snapshot.json"
        _snap = json.loads(_p.read_text(encoding="utf-8"))
        rot = {}
        for _mk, _blk in (_snap.get("markets") or {}).items():
            if _blk.get("temperature"):
                out[_mk] = _blk["temperature"]
            _secs = _blk.get("sectors") or []
            hot, cold = [], []
            if len(_secs) >= 4:
                _r5 = {s["symbol"]: i for i, s in enumerate(sorted(_secs, key=lambda x: -x["chg5d"]))}
                _r20 = {s["symbol"]: i for i, s in enumerate(sorted(_secs, key=lambda x: -x["chg20d"]))}
                _jump = max(2, len(_secs) // 3)
                for s in _secs:
                    _d = _r20[s["symbol"]] - _r5[s["symbol"]]
                    if _d >= _jump and s["chg5d"] > 0:
                        hot.append(s["name"])
                    elif _d <= -_jump and s["chg20d"] > 0:
                        cold.append(s["name"])
            rot[_mk] = {"hot": hot, "cold": cold}
        out["_rotation"] = rot
    except Exception:
        pass
    _MKT_TEMP_CACHE["ts"] = now
    _MKT_TEMP_CACHE["data"] = out
    return out


# ═══════════════════════════════════════════════════════════════
# 4. 动态股票池 - 从云端API获取（V87 革命性升级）
# ═══════════════════════════════════════════════════════════════
# 【V87.2】使用更可靠的东方财富行情中心API获取股票列表（支持分页，东财 pz 单页最大约200）
# 【安全策略】总量800只（美350+港200+A250），=800安全线
EASTMONEY_PAGE_SIZE = 100  # 东财 clist 接口单页硬上限 100，请求再多也只回 100

@st.cache_data(ttl=3600, show_spinner=False)  # 1小时缓存（全模块统一）
def fetch_eastmoney_stock_list(market="us", limit=350):
    """
    从东方财富行情中心获取股票列表（支持分页）
    
    参数：
        market: "us" (美股) / "hk" (港股) / "cn" (A股)
        limit: 返回数量
    
    返回：
        [(code, name, yf_code), ...]
    """
    try:
        url = "http://80.push2.eastmoney.com/api/qt/clist/get"
        page_size = EASTMONEY_PAGE_SIZE
        all_stocks = []
        pn = 1
        while len(all_stocks) < limit:
            time.sleep(0.6)
            pz = min(page_size, limit - len(all_stocks))
            # po=1 按 f20(总市值) 降序，缺少该参数时东财按代码升序返回，
            # A股会捞到 PT/ST/退市老三板垃圾股（名字全带"A"），必须保留
            if market == "us":
                params = {"pn": pn, "pz": pz, "fs": "m:105,m:106,m:107", "fields": "f12,f14,f20",
                          "ut": "bd1d9ddb04089700cf9c27f6f7426281", "fid": "f20", "po": 1, "type": "rank"}
            elif market == "hk":
                params = {"pn": pn, "pz": pz, "fs": "m:128", "fields": "f12,f14,f20",
                          "ut": "bd1d9ddb04089700cf9c27f6f7426281", "fid": "f20", "po": 1, "type": "rank"}
            elif market == "cn":
                params = {"pn": pn, "pz": pz, "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23", "fields": "f12,f14,f20",
                          "ut": "bd1d9ddb04089700cf9c27f6f7426281", "fid": "f20", "po": 1, "type": "rank"}
            else:
                return []
            response = _DIRECT_SESSION.get(url, params=params, timeout=10)
            if response.status_code != 200:
                break
            data = response.json()
            if not data.get('data') or not data['data'].get('diff'):
                break
            diff = data['data']['diff']
            diff_list = diff if isinstance(diff, list) else (list(diff.values()) if isinstance(diff, dict) else [])
            page_stocks = []
            for item in diff_list:
                if not isinstance(item, dict):
                    continue
                code = item.get('f12', '')
                name = item.get('f14', '')
                if code and name:
                    _nm = str(name).upper()
                    # 只保留个股：按市值排序后 ETF/指数基金会挤占前排（SPY/盈富基金等），全部剔除
                    if any(k in _nm for k in ("ETF", "ETN", "基金", "指数")):
                        continue
                    # A股剔除 ST/*ST/PT/退市股，避免进入扫描推荐池
                    if market == "cn":
                        if _nm.startswith(("ST", "*ST", "PT", "S*ST", "SST")) or "退" in _nm:
                            continue
                    yf_code = to_yf_cn_code(code)
                    page_stocks.append((code, name, yf_code))
            all_stocks.extend(page_stocks)
            # 【V94.3】翻页终止必须看服务端原始条数（diff_list），不能看过滤后的
            # page_stocks：ETF/ST 被剔除后条数必然小于 pz，原判断会在第一页就误停，
            # 导致股池只有 ~100 只/市场
            if len(diff_list) < pz:
                break
            pn += 1
            if pn > 30:  # 安全上限：防止极端情况下整页被过滤导致的无限翻页
                break
            if len(all_stocks) >= limit:
                break
        result = all_stocks[:limit]
        _safe_print(f"[股票池] ✅ {market.upper()}股池获取成功: {len(result)} 只")
        return result
    except Exception as e:
        _safe_print(f"[股票池] ❌ {market.upper()}股API失败: {type(e).__name__}: {str(e)[:100]}")
        return []


# ── 个股财报 & 行业信息获取 ─────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_stock_fundamentals(code: str) -> dict:
    """获取个股完整财报（损益表/资产负债表/现金流量表）+ 行业信息，1小时缓存"""
    try:
        yf_code = to_yf_cn_code(code)
        tk = yf.Ticker(yf_code)
        info = tk.info or {}

        def _safe(key, default=None):
            v = info.get(key, default)
            return v if v is not None else default

        # ── 基本信息 & 估值 ──
        fundamentals = {
            "company_name": _safe("longName") or _safe("shortName", ""),
            "sector": _safe("sector", ""),
            "industry": _safe("industry", ""),
            "business_summary": _safe("longBusinessSummary", ""),
            "market_cap": _safe("marketCap", 0),
            "trailing_pe": _safe("trailingPE", 0),
            "forward_pe": _safe("forwardPE", 0),
            "price_to_book": _safe("priceToBook", 0),
            "dividend_yield": _safe("dividendYield", 0),
            "recommendation": _safe("recommendationKey", ""),
            "target_mean_price": _safe("targetMeanPrice", 0),
            "number_of_analysts": _safe("numberOfAnalystOpinions", 0),
        }

        # ── 损益表（年报，最近4年）──
        def _stmt_to_dict(stmt_df):
            """将 yfinance 财报 DataFrame 转成 {科目: {日期str: 值}} 的 dict"""
            if stmt_df is None or stmt_df.empty:
                return {}
            result = {}
            for row_name in stmt_df.index:
                row_dict = {}
                for col in stmt_df.columns:
                    val = stmt_df.loc[row_name, col]
                    date_key = col.strftime("%Y") if hasattr(col, "strftime") else str(col)[:4]
                    try:
                        row_dict[date_key] = float(val) if pd.notna(val) else None
                    except (ValueError, TypeError):
                        row_dict[date_key] = None
                result[str(row_name)] = row_dict
            return result

        try:
            fundamentals["income_stmt"] = _stmt_to_dict(tk.income_stmt)
        except Exception:
            fundamentals["income_stmt"] = {}
        try:
            fundamentals["balance_sheet"] = _stmt_to_dict(tk.balance_sheet)
        except Exception:
            fundamentals["balance_sheet"] = {}
        try:
            fundamentals["cashflow"] = _stmt_to_dict(tk.cashflow)
        except Exception:
            fundamentals["cashflow"] = {}
        try:
            fundamentals["quarterly_income"] = _stmt_to_dict(tk.quarterly_income_stmt)
        except Exception:
            fundamentals["quarterly_income"] = {}

        return fundamentals
    except Exception as e:
        _safe_print(f"[财报] ❌ {code} 获取失败: {type(e).__name__}: {str(e)[:80]}")
        return {}


def _fmt_fin(val):
    """格式化财报金额（自动亿/万）"""
    if val is None:
        return "-"
    if abs(val) >= 1e12:
        return f"{val/1e12:.2f}万亿"
    if abs(val) >= 1e8:
        return f"{val/1e8:.1f}亿"
    if abs(val) >= 1e4:
        return f"{val/1e4:.0f}万"
    return f"{val:,.0f}"


def _build_fin_table(stmt: dict, items: list, dates: list) -> pd.DataFrame:
    """从财报 dict 构建展示表格。items = [(显示名, 科目key), ...]"""
    rows = []
    for label, key in items:
        row = {"科目": label}
        data = stmt.get(key, {})
        for d in dates:
            v = data.get(d)
            row[d] = _fmt_fin(v) if v is not None else "-"
        # 同比增长（最近两年）
        if len(dates) >= 2:
            v1 = data.get(dates[0])
            v2 = data.get(dates[1])
            if v1 is not None and v2 is not None and v2 != 0:
                yoy = (v1 - v2) / abs(v2) * 100
                row["同比"] = f"{yoy:+.1f}%"
            else:
                row["同比"] = "-"
        rows.append(row)
    return pd.DataFrame(rows)


def render_fundamentals_panel(fundamentals: dict, target_c: str):
    """渲染完整财报面板：损益表 + 资产负债表 + 现金流量表 + 行业背景"""
    if not fundamentals:
        st.caption("⚠️ 财报数据暂不可用（部分 A 股/港股可能无此数据）")
        return

    company_name = fundamentals.get("company_name", target_c)
    sector = fundamentals.get("sector", "")
    industry = fundamentals.get("industry", "")
    mkt_cap = fundamentals.get("market_cap", 0)
    st.caption(f"财报币种：{fundamentals.get('financial_currency') or fundamentals.get('financialCurrency') or '待核'} · 市值币种：{fundamentals.get('currency') or '待核'}；三表日期为报告期，公告时间须另核。")

    # ── 公司头部 ──
    _cap_str = _fmt_fin(mkt_cap) if mkt_cap else "N/A"
    _pe_str = f"{fundamentals.get('trailing_pe', 0):.1f}" if fundamentals.get("trailing_pe") else "N/A"
    _pb_str = f"{fundamentals.get('price_to_book', 0):.2f}" if fundamentals.get("price_to_book") else "N/A"
    if sector or industry:
        st.markdown(
            f'<div style="background:linear-gradient(135deg,#6366f1,#8b5cf6);padding:0.8rem 1.2rem;border-radius:8px;color:white;font-size:13px;margin-bottom:0.8rem;">'
            f'🏢 <b>{company_name}</b> · {sector} · {industry}'
            f'<span style="float:right;">市值 {_cap_str} · P/E {_pe_str} · P/B {_pb_str}</span></div>',
            unsafe_allow_html=True)

    income = fundamentals.get("income_stmt", {})
    balance = fundamentals.get("balance_sheet", {})
    cashflow = fundamentals.get("cashflow", {})
    q_income = fundamentals.get("quarterly_income", {})

    # 获取可用年份（倒序）
    _all_dates = set()
    for stmt in [income, balance, cashflow]:
        for k, v in stmt.items():
            if isinstance(v, dict):
                _all_dates.update(v.keys())
    dates = sorted(_all_dates, reverse=True)[:4]

    if not dates:
        # 没有财报数据，显示基本估值信息
        hc = st.columns(5)
        with hc[0]:
            st.metric("市值", _cap_str)
        with hc[1]:
            st.metric("P/E", _pe_str)
        with hc[2]:
            st.metric("P/B", _pb_str)
        with hc[3]:
            st.metric("股息率", f"{fundamentals.get('dividend_yield', 0)*100:.2f}%" if fundamentals.get("dividend_yield") else "N/A")
        with hc[4]:
            rec = fundamentals.get("recommendation", "")
            rec_cn = {"buy": "买入", "strong_buy": "强买", "hold": "持有", "sell": "卖出"}.get(rec, rec or "N/A")
            st.metric("供应商观点", rec_cn, help="供应商分析师意见，不是V88中央评级或交易许可")
        _biz_cache_key = f"_biz_cn_{target_c}"
        _biz_cn = st.session_state.get(_biz_cache_key, "")
        if not _biz_cn:
            _biz_cn = str(fundamentals.get('business_summary') or '')
        if _biz_cn:
            with st.expander("📖 公司简介与业务概况 · 已有资料", expanded=False):
                st.markdown(f'<p style="font-size:13px;line-height:1.8;color:#374151;">{_biz_cn}</p>', unsafe_allow_html=True)
        return

    # ── 财报三表 Tabs ──
    tab_is, tab_bs, tab_cf = st.tabs(["📋 损益表", "📊 资产负债表", "💵 现金流量表"])

    with tab_is:
        _is_items = [
            ("营业收入", "Total Revenue"),
            ("营业成本", "Cost Of Revenue"),
            ("毛利润", "Gross Profit"),
            ("营业费用", "Operating Expense"),
            ("营业利润", "Operating Income"),
            ("利息费用", "Interest Expense"),
            ("税前利润", "Pretax Income"),
            ("所得税", "Tax Provision"),
            ("净利润", "Net Income"),
            ("基本每股收益", "Basic EPS"),
            ("稀释每股收益", "Diluted EPS"),
            ("EBITDA", "EBITDA"),
        ]
        df_is = _build_fin_table(income, _is_items, dates)
        if not df_is.empty and df_is.shape[1] > 1:
            st.dataframe(df_is, use_container_width=True, hide_index=True)
        else:
            st.caption("⚠️ 损益表数据暂不可用")

        # 季度利润趋势（如有）
        if q_income:
            q_dates = set()
            for v in q_income.values():
                if isinstance(v, dict):
                    q_dates.update(v.keys())
            q_dates = sorted(q_dates, reverse=True)[:8]
            if q_dates:
                with st.expander("📈 最近8季度利润趋势", expanded=False):
                    _q_items = [
                        ("营业收入", "Total Revenue"),
                        ("营业利润", "Operating Income"),
                        ("净利润", "Net Income"),
                    ]
                    df_q = _build_fin_table(q_income, _q_items, q_dates)
                    st.dataframe(df_q, use_container_width=True, hide_index=True)

    with tab_bs:
        _bs_items = [
            ("总资产", "Total Assets"),
            ("流动资产", "Current Assets"),
            ("现金及等价物", "Cash And Cash Equivalents"),
            ("应收账款", "Accounts Receivable"),
            ("存货", "Inventory"),
            ("非流动资产", "Total Non Current Assets"),
            ("固定资产净值", "Net PPE"),
            ("总负债", "Total Liabilities Net Minority Interest"),
            ("流动负债", "Current Liabilities"),
            ("长期负债", "Long Term Debt"),
            ("股东权益", "Stockholders Equity"),
            ("留存收益", "Retained Earnings"),
        ]
        df_bs = _build_fin_table(balance, _bs_items, dates)
        if not df_bs.empty and df_bs.shape[1] > 1:
            st.dataframe(df_bs, use_container_width=True, hide_index=True)
        else:
            st.caption("⚠️ 资产负债表数据暂不可用")

    with tab_cf:
        _cf_items = [
            ("经营活动现金流", "Operating Cash Flow"),
            ("资本开支", "Capital Expenditure"),
            ("自由现金流", "Free Cash Flow"),
            ("投资活动现金流", "Investing Cash Flow"),
            ("筹资活动现金流", "Financing Cash Flow"),
            ("股票回购", "Repurchase Of Capital Stock"),
            ("支付股息", "Cash Dividends Paid"),
            ("现金净增加", "Changes In Cash"),
            ("期末现金", "End Cash Position"),
        ]
        df_cf = _build_fin_table(cashflow, _cf_items, dates)
        if not df_cf.empty and df_cf.shape[1] > 1:
            st.dataframe(df_cf, use_container_width=True, hide_index=True)
        else:
            st.caption("⚠️ 现金流量表数据暂不可用")

    # ── 关键财务比率（一行汇总）──
    # 从损益表计算利润率
    _latest = dates[0] if dates else None
    if _latest and income:
        _rev = (income.get("Total Revenue", {}) or {}).get(_latest)
        _op = (income.get("Operating Income", {}) or {}).get(_latest)
        _ni = (income.get("Net Income", {}) or {}).get(_latest)
        _gp = (income.get("Gross Profit", {}) or {}).get(_latest)
        _ta = (balance.get("Total Assets", {}) or {}).get(_latest)
        _eq = (balance.get("Stockholders Equity", {}) or {}).get(_latest)
        _tl = (balance.get("Total Liabilities Net Minority Interest", {}) or {}).get(_latest)

        st.markdown(f"##### 📐 关键财务比率（{_latest}年报）")
        rc = st.columns(6)
        with rc[0]:
            _gm = f"{_gp/_rev*100:.1f}%" if _rev and _gp else "N/A"
            st.metric("毛利率", _gm)
        with rc[1]:
            _om = f"{_op/_rev*100:.1f}%" if _rev and _op else "N/A"
            st.metric("营业利润率", _om)
        with rc[2]:
            _nm = f"{_ni/_rev*100:.1f}%" if _rev and _ni else "N/A"
            st.metric("净利率", _nm)
        with rc[3]:
            _roe = f"{_ni/_eq*100:.1f}%" if _eq and _ni and _eq != 0 else "N/A"
            st.metric("ROE", _roe)
        with rc[4]:
            _roa = f"{_ni/_ta*100:.1f}%" if _ta and _ni and _ta != 0 else "N/A"
            st.metric("ROA", _roa)
        with rc[5]:
            _de = f"{_tl/_eq:.2f}" if _eq and _tl and _eq != 0 else "N/A"
            st.metric("负债/权益", _de)

    # ── 公司简介原文／既有摘要；渲染不请求模型 ──
    _biz_cache_key = f"_biz_cn_{target_c}"
    _biz_cn = st.session_state.get(_biz_cache_key, "")
    if not _biz_cn:
        _biz_cn = str(fundamentals.get('business_summary') or '')
    if _biz_cn:
        with st.expander("📖 公司简介与业务概况 · 已有资料", expanded=False):
            st.markdown(f'<p style="font-size:13px;line-height:1.8;color:#374151;">{_biz_cn}</p>', unsafe_allow_html=True)
# ─────────────────────────────────────────────────────────────────────────────


# 【V87.2】初始化股票池（带降级方案 + 安全限流）
def fetch_us_pool_sp500(limit=500):
    """
    【V94.3】美股二级云端源：维基百科标普500成分股名单（走代理，稳定可达）。
    东财接口从本机时通时断，不能让股池质量绑死在单一源上。
    返回 [(symbol, name, yf_symbol), ...]
    """
    try:
        import re as _re
        sess = _shared_http_session(direct=False)  # 复用连接，自动用环境代理
        r = sess.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
                     timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        rows = _re.findall(r'<td><a [^>]*>([A-Z][A-Z.\-]{0,5})</a>\s*</td>\s*<td><a [^>]*>([^<]+)</a>', r.text)
        out = []
        for sym, name in rows:
            yf_sym = sym.replace(".", "-")  # BRK.B → BRK-B（yfinance 格式）
            out.append((yf_sym, name, yf_sym))
            if len(out) >= limit:
                break
        if out:
            _safe_print(f"[股票池] ✅ 二级源(标普500名单): {len(out)} 只")
        return out
    except Exception as e:
        _safe_print(f"[股票池] ⚠️ 标普500名单获取失败: {type(e).__name__}: {str(e)[:60]}")
        return []


def init_stock_pools():
    """
    【V94 扩容】总量1300只（美500+港300+A500），配合批量预取+免费行情源直连
    - 美股: 500只（东财按市值降序）
    - 港股: 300只（东财按市值降序）
    - A股: 500只（东财按市值降序，已剔除ST/退市/ETF）
    - 总计: 1300只

    源优先级（【V94.3】三级回退，不绑死单一数据源）：
      美股: 东财 → 维基百科标普500 → 本地备用池
      A股: 东财 → 免费行情源市值榜 → 本地备用池
      港股: 东财 → 本地备用池
    """
    _safe_print("[股票池] 开始初始化（扩容模式：总量1300只 = 美500+港300+A500）...")

    # 1. 尝试从云端获取美股（350只）
    us_pool = fetch_eastmoney_stock_list("us", 500)
    if not us_pool or len(us_pool) < 30:
        # 【V94.3】二级云端源：标普500名单（维基百科，走代理稳定可达）
        us_pool = fetch_us_pool_sp500(500)
    if not us_pool or len(us_pool) < 30:
        _safe_print("[股票池] ⚠️ 美股云端获取失败，使用备用池（标普500+纳斯达克100）")
        # 【V87.3】美股备用池扩展到240只 - 必须足够！
        us_pool = [
            # 科技巨头
            ("AAPL", "苹果", "AAPL"), ("MSFT", "微软", "MSFT"), ("GOOGL", "谷歌A", "GOOGL"), ("GOOG", "谷歌C", "GOOG"),
            ("AMZN", "亚马逊", "AMZN"), ("META", "Meta", "META"), ("NVDA", "英伟达", "NVDA"), ("TSLA", "特斯拉", "TSLA"),
            ("NFLX", "奈飞", "NFLX"), ("DIS", "迪士尼", "DIS"),
            # 半导体
            ("TSM", "台积电", "TSM"), ("ASML", "阿斯麦", "ASML"), ("AMD", "超微半导体", "AMD"), ("INTC", "英特尔", "INTC"),
            ("QCOM", "高通", "QCOM"), ("AVGO", "博通", "AVGO"), ("AMAT", "应用材料", "AMAT"), ("LRCX", "泛林集团", "LRCX"),
            ("KLAC", "科磊", "KLAC"), ("MU", "美光科技", "MU"), ("MRVL", "迈威尔", "MRVL"), ("NXPI", "恩智浦", "NXPI"),
            ("TXN", "德州仪器", "TXN"), ("ADI", "亚德诺", "ADI"), ("ON", "安森美", "ON"),
            # 软件与云
            ("CRM", "Salesforce", "CRM"), ("ORCL", "甲骨文", "ORCL"), ("ADBE", "Adobe", "ADBE"), ("NOW", "ServiceNow", "NOW"),
            ("SNOW", "Snowflake", "SNOW"), ("PLTR", "Palantir", "PLTR"), ("DDOG", "Datadog", "DDOG"), ("CRWD", "CrowdStrike", "CRWD"),
            ("ZS", "Zscaler", "ZS"), ("NET", "Cloudflare", "NET"), ("OKTA", "Okta", "OKTA"),
            # 电商与支付
            ("SHOP", "Shopify", "SHOP"), ("XYZ", "Block", "XYZ"), ("PYPL", "PayPal", "PYPL"), ("MELI", "MercadoLibre", "MELI"),
            # 中概股
            ("BABA", "阿里巴巴", "BABA"), ("BIDU", "百度", "BIDU"), ("JD", "京东", "JD"), ("PDD", "拼多多", "PDD"),
            ("BILI", "哔哩哔哩", "BILI"), ("NIO", "蔚来汽车", "NIO"), ("LI", "理想汽车", "LI"), ("XPEV", "小鹏汽车", "XPEV"),
            ("TME", "腾讯音乐", "TME"), ("NTES", "网易", "NTES"), ("IQ", "爱奇艺", "IQ"),
            # 金融
            ("JPM", "摩根大通", "JPM"), ("BAC", "美国银行", "BAC"), ("WFC", "富国银行", "WFC"), ("C", "花旗集团", "C"),
            ("GS", "高盛", "GS"), ("MS", "摩根士丹利", "MS"), ("BLK", "贝莱德", "BLK"), ("SCHW", "嘉信理财", "SCHW"),
            ("V", "Visa", "V"), ("MA", "万事达", "MA"), ("AXP", "美国运通", "AXP"), ("COF", "第一资本", "COF"),
            # 医疗健康
            ("JNJ", "强生", "JNJ"), ("UNH", "联合健康", "UNH"), ("PFE", "辉瑞", "PFE"), ("ABBV", "艾伯维", "ABBV"),
            ("TMO", "赛默飞世尔", "TMO"), ("ABT", "雅培", "ABT"), ("LLY", "礼来", "LLY"), ("MRK", "默克", "MRK"),
            ("BMY", "百时美施贵宝", "BMY"), ("AMGN", "安进", "AMGN"), ("GILD", "吉利德", "GILD"), ("CVS", "CVS Health", "CVS"),
            # 消费品
            ("PG", "宝洁", "PG"), ("KO", "可口可乐", "KO"), ("PEP", "百事", "PEP"), ("WMT", "沃尔玛", "WMT"),
            ("COST", "好市多", "COST"), ("HD", "家得宝", "HD"), ("LOW", "劳氏", "LOW"), ("TGT", "塔吉特", "TGT"),
            ("NKE", "耐克", "NKE"), ("SBUX", "星巴克", "SBUX"), ("MCD", "麦当劳", "MCD"), ("CMG", "Chipotle", "CMG"),
            ("YUM", "百胜餐饮", "YUM"),
            # 能源
            ("XOM", "埃克森美孚", "XOM"), ("CVX", "雪佛龙", "CVX"), ("COP", "康菲石油", "COP"), ("SLB", "斯伦贝谢", "SLB"),
            # 工业
            ("BA", "波音", "BA"), ("CAT", "卡特彼勒", "CAT"), ("GE", "通用电气", "GE"), ("HON", "霍尼韦尔", "HON"),
            ("UPS", "联合包裹", "UPS"), ("LMT", "洛克希德马丁", "LMT"), ("RTX", "雷神技术", "RTX"),
            # 通信
            ("T", "AT&T", "T"), ("VZ", "Verizon", "VZ"), ("TMUS", "T-Mobile", "TMUS"), ("CMCSA", "康卡斯特", "CMCSA"),
            # 汽车
            ("F", "福特汽车", "F"), ("GM", "通用汽车", "GM"), ("RIVN", "Rivian", "RIVN"), ("LCID", "Lucid", "LCID"),
            # 科技服务
            ("UBER", "Uber", "UBER"), ("LYFT", "Lyft", "LYFT"), ("ABNB", "Airbnb", "ABNB"), ("DASH", "DoorDash", "DASH"),
            ("COIN", "Coinbase", "COIN"), ("RBLX", "Roblox", "RBLX"), ("U", "Unity", "U"), ("ZM", "Zoom", "ZM"),
            ("DOCU", "DocuSign", "DOCU"), ("TWLO", "Twilio", "TWLO"), ("SPOT", "Spotify", "SPOT"),
            # 其他（移除ATVI-已被微软收购退市）
            ("IBM", "IBM", "IBM"), ("HPQ", "惠普", "HPQ"), ("DELL", "戴尔", "DELL"), ("EA", "艺电", "EA"),
            ("TTWO", "Take-Two", "TTWO"), ("TCEHY", "腾讯ADR", "TCEHY"), ("RBLX", "Roblox", "RBLX"), ("U", "Unity", "U"),
            ("MMM", "3M", "MMM"), ("DD", "杜邦", "DD"), ("DOW", "陶氏化学", "DOW"), ("LIN", "林德", "LIN"),
            ("APD", "空气化工", "APD"), ("ECL", "艺康", "ECL"), ("PPG", "PPG工业", "PPG"),
            ("DHR", "丹纳赫", "DHR"), ("ITW", "伊利诺伊", "ITW"), ("EMR", "艾默生", "EMR"),
            ("FDX", "联邦快递", "FDX"), ("DE", "迪尔", "DE"), ("NSC", "诺福克南方", "NSC"),
            ("UNP", "联合太平洋", "UNP"), ("CSX", "CSX运输", "CSX"), ("DAL", "达美航空", "DAL"),
            ("AAL", "美国航空", "AAL"), ("UAL", "联合航空", "UAL"), ("LUV", "西南航空", "LUV"),
            ("MAR", "万豪国际", "MAR"), ("HLT", "希尔顿", "HLT"), ("MGM", "美高梅", "MGM"),
            ("WYNN", "永利度假", "WYNN"), ("LVS", "金沙集团", "LVS"), ("BKNG", "Booking", "BKNG"),
            ("EXPE", "Expedia", "EXPE"), ("TRIP", "TripAdvisor", "TRIP"), ("ABNB", "Airbnb", "ABNB"),
            ("DG", "Dollar General", "DG"), ("DLTR", "Dollar Tree", "DLTR"), ("FIVE", "Five Below", "FIVE"),
            ("ROST", "Ross Stores", "ROST"), ("TJX", "TJX", "TJX"), ("LULU", "Lululemon", "LULU"),
            ("M", "梅西百货", "M"), ("KSS", "科尔士", "KSS"), 
            ("AZO", "AutoZone", "AZO"), ("ORLY", "O'Reilly", "ORLY"), ("AAP", "Advance Auto", "AAP"),
            ("KMX", "CarMax", "KMX"), ("AN", "AutoNation", "AN"),
            # 【V87.4】扩展备用池到240只 - 新增74只
            # 更多科技股（移除重复的ZM）
            ("CRM", "Salesforce", "CRM"), ("ORCL", "甲骨文", "ORCL"), ("ADBE", "Adobe", "ADBE"), ("NOW", "ServiceNow", "NOW"),
            ("SNOW", "Snowflake", "SNOW"), ("PLTR", "Palantir", "PLTR"), ("OKTA", "Okta", "OKTA"), ("SHOP", "Shopify", "SHOP"),
            ("CRWD", "CrowdStrike", "CRWD"), ("ZS", "Zscaler", "ZS"), ("NET", "Cloudflare", "NET"), ("PANW", "Palo Alto", "PANW"),
            ("DDOG", "Datadog", "DDOG"), ("MDB", "MongoDB", "MDB"), ("WDAY", "Workday", "WDAY"),
            # 生物医药
            ("MRNA", "Moderna", "MRNA"), ("BNTX", "BioNTech", "BNTX"), ("REGN", "Regeneron", "REGN"), ("VRTX", "Vertex", "VRTX"),
            ("ILMN", "Illumina", "ILMN"), ("BIIB", "Biogen", "BIIB"), ("AMGN", "安进", "AMGN"),
            ("ISRG", "Intuitive", "ISRG"), ("DXCM", "DexCom", "DXCM"), ("ALGN", "Align", "ALGN"), ("IDXX", "IDEXX", "IDXX"),
            # 新能源与清洁技术
            ("ENPH", "Enphase", "ENPH"), ("SEDG", "SolarEdge", "SEDG"), ("FSLR", "First Solar", "FSLR"), ("RUN", "Sunrun", "RUN"),
            ("PLUG", "Plug Power", "PLUG"), ("FCEL", "FuelCell", "FCEL"), ("BE", "Bloom Energy", "BE"), 
            # 电动车产业链
            ("NIO", "蔚来", "NIO"), ("XPEV", "小鹏汽车", "XPEV"), ("LI", "理想汽车", "LI"), ("LCID", "Lucid Motors", "LCID"),
            ("RIVN", "Rivian", "RIVN"), ("F", "福特汽车", "F"), ("GM", "通用汽车", "GM"), ("STLA", "Stellantis", "STLA"),
            # 金融科技（移除SQ-已被收购）
            ("PYPL", "PayPal", "PYPL"), ("V", "Visa", "V"), ("MA", "万事达", "MA"), ("COIN", "Coinbase", "COIN"),
            ("AXP", "美国运通", "AXP"), ("COF", "Capital One", "COF"), ("SYF", "Synchrony", "SYF"),
            # 消费品牌
            ("NKE", "耐克", "NKE"), ("LULU", "Lululemon", "LULU"), ("ULTA", "Ulta Beauty", "ULTA"), ("EL", "雅诗兰黛", "EL"),
            ("PG", "宝洁", "PG"), ("KO", "可口可乐", "KO"), ("PEP", "百事可乐", "PEP"), ("MCD", "麦当劳", "MCD"),
            # 工业与制造
            ("BA", "波音", "BA"), ("LMT", "洛克希德马丁", "LMT"), ("RTX", "雷神技术", "RTX"), ("NOC", "诺斯罗普", "NOC"),
            ("GE", "通用电气", "GE"), ("MMM", "3M", "MMM"), ("HON", "霍尼韦尔", "HON"), ("UNP", "联合太平洋", "UNP"),
            # 房地产投资信托(REITs)
            ("AMT", "American Tower", "AMT"), ("CCI", "Crown Castle", "CCI"), ("EQIX", "Equinix", "EQIX"), ("DLR", "Digital Realty", "DLR"),
        ]
    
    # 2. 尝试从云端获取港股（200只，保持不变）
    hk_pool = fetch_eastmoney_stock_list("hk", 300)
    if not hk_pool or len(hk_pool) < 30:
        _safe_print("[股票池] ⚠️ 港股云端获取失败，使用备用池（恒生+国企+科技）")
        # 【V87.3】港股备用池扩展到200只
        hk_pool = [
            # 互联网科技 (30只)
            ("00700", "腾讯控股", "0700.HK"), ("09988", "阿里巴巴-SW", "9988.HK"), ("03690", "美团-W", "3690.HK"),
            ("01810", "小米集团-W", "1810.HK"), ("09618", "京东集团-SW", "9618.HK"), ("09999", "网易", "9999.HK"),
            ("09626", "哔哩哔哩", "9626.HK"), ("09888", "百度集团", "9888.HK"), ("01024", "快手", "1024.HK"),
            ("06060", "众安在线", "6060.HK"), ("01833", "平安好医生", "1833.HK"), ("06618", "京东健康", "6618.HK"),
            ("09961", "携程集团-S", "9961.HK"), ("09698", "万国数据-SW", "9698.HK"), ("09999", "网易", "9999.HK"),
            ("09896", "名创优品", "9896.HK"), ("02013", "微盟集团", "2013.HK"), ("00268", "金蝶国际", "0268.HK"),
            ("06690", "海尔智家", "6690.HK"), ("02020", "安踏体育", "2020.HK"), ("01347", "华虹半导体", "1347.HK"),
            ("06618", "京东健康", "6618.HK"), ("09933", "知乎-W", "9933.HK"), ("09999", "网易-S", "9999.HK"),
            ("09991", "宝尊电商-SW", "9991.HK"), ("09901", "新东方-S", "9901.HK"), ("09999", "阅文集团", "0772.HK"),
            ("01717", "澳优乳业", "1717.HK"), ("03900", "绿城中国", "3900.HK"), ("00992", "联想集团", "0992.HK"),
            # 新能源汽车 (15只)
            ("01211", "比亚迪", "1211.HK"), ("02015", "理想汽车-W", "2015.HK"), ("09868", "小鹏汽车-W", "9868.HK"),
            ("09866", "蔚来汽车-SW", "9866.HK"), ("00175", "吉利汽车", "0175.HK"), ("02238", "广汽集团", "2238.HK"),
            ("02460", "宁德时代", "2460.HK"), ("01958", "北京汽车", "1958.HK"), ("02333", "长城汽车", "2333.HK"),
            ("00489", "东风集团股份", "0489.HK"), ("01114", "华晨中国", "1114.HK"), ("00177", "江铃汽车", "0177.HK"),
            ("01122", "庆铃汽车股份", "1122.HK"), ("00038", "第一拖拉机股份", "0038.HK"), ("01053", "重庆长安汽车", "1053.HK"),
            # 金融银行 (40只)
            ("02318", "中国平安", "2318.HK"), ("01299", "友邦保险", "1299.HK"), ("03968", "招商银行", "3968.HK"),
            ("03988", "中国银行", "3988.HK"), ("01398", "工商银行", "1398.HK"), ("01288", "农业银行", "1288.HK"),
            ("00939", "建设银行", "0939.HK"), ("03328", "交通银行", "3328.HK"), ("06818", "中国光大银行", "6818.HK"),
            ("01339", "中国人民保险", "1339.HK"), ("02628", "中国人寿", "2628.HK"), ("01336", "新华保险", "1336.HK"),
            ("02601", "中国太保", "2601.HK"), ("01359", "中国信达", "1359.HK"), ("02799", "中国华融", "2799.HK"),
            ("06066", "中信银行", "6066.HK"), ("01988", "民生银行", "1988.HK"), ("03618", "重庆农村商业银行", "3618.HK"),
            ("01658", "邮储银行", "1658.HK"), ("06196", "浙商银行", "6196.HK"), ("02016", "浙江沪杭甬", "2016.HK"),
            ("06886", "华泰证券", "6886.HK"), ("06881", "中国银河", "6881.HK"), ("06098", "碧桂园服务", "6098.HK"),
            ("01579", "颐海国际", "1579.HK"), ("03799", "达利食品", "3799.HK"), ("01610", "中粮家佳康", "1610.HK"),
            ("02319", "蒙牛乳业", "2319.HK"), ("00291", "华润啤酒", "0291.HK"), ("01876", "百威亚太", "1876.HK"),
            ("01928", "金沙中国", "1928.HK"), ("02388", "中银香港", "2388.HK"), ("02356", "大新银行", "2356.HK"),
            ("02888", "渣打集团", "2888.HK"), ("00005", "汇丰控股", "0005.HK"), ("00011", "恒生银行", "0011.HK"),
            ("01109", "华润置地", "1109.HK"), ("01113", "长实集团", "1113.HK"), ("01997", "九龙仓置业", "1997.HK"),
            ("00016", "新鸿基地产", "0016.HK"), ("00017", "新世界发展", "0017.HK"),
            # 能源资源 (25只)
            ("02899", "紫金矿业", "2899.HK"), ("00883", "中国海洋石油", "0883.HK"), ("00386", "中国石油化工", "0386.HK"),
            ("00857", "中国石油股份", "0857.HK"), ("01088", "中国神华", "1088.HK"), ("01898", "中煤能源", "1898.HK"),
            ("01171", "兖煤澳大利亚", "1171.HK"), ("01772", "赣锋锂业", "1772.HK"), ("02601", "中国铝业", "2601.HK"),
            ("01919", "中远海控", "1919.HK"), ("00358", "江西铜业", "0358.HK"), ("02020", "青岛港", "2020.HK"),
            ("01199", "中远海运港口", "1199.HK"), ("01308", "海丰国际", "1308.HK"), ("00144", "招商局港口", "0144.HK"),
            ("00763", "中兴通讯", "0763.HK"), ("00941", "中国移动", "0941.HK"), ("00728", "中国电信", "0728.HK"),
            ("00762", "中国联通", "0762.HK"), ("06993", "蓝月亮集团", "6993.HK"), ("00688", "中国海外发展", "0688.HK"),
            ("02007", "碧桂园", "2007.HK"), ("01668", "中国建筑国际", "1668.HK"), ("03311", "中国建筑", "3311.HK"),
            ("01800", "中国交建", "1800.HK"), ("01766", "中国中车", "1766.HK"),
            # 医药健康 (20只)
            ("01093", "石药集团", "1093.HK"), ("02269", "药明生物", "2269.HK"), ("06185", "康希诺生物", "6185.HK"),
            ("09889", "药明合联", "9889.HK"), ("02359", "药明康德", "2359.HK"), ("01177", "中国生物制药", "1177.HK"),
            ("01099", "国药控股", "1099.HK"), ("03692", "翰森制药", "3692.HK"), ("00874", "广州白云山医药", "0874.HK"),
            ("02186", "绿叶制药", "2186.HK"), ("06821", "凯莱英", "6821.HK"), ("09969", "诺辉健康", "9969.HK"),
            ("01801", "信达生物", "1801.HK"), ("02162", "康方生物", "2162.HK"), ("09995", "荣昌生物", "9995.HK"),
            ("09996", "沛嘉医疗", "9996.HK"), ("01530", "三生制药", "1530.HK"), ("00347", "鞍钢股份", "0347.HK"),
            ("00902", "华能国际电力", "0902.HK"), ("00966", "中国太平", "0966.HK"),
            # 科技硬件 (15只)
            ("00981", "中芯国际", "0981.HK"), ("02382", "舜宇光学科技", "2382.HK"), ("00992", "联想集团", "0992.HK"),
            ("02018", "瑞声科技", "2018.HK"), ("01285", "比亚迪电子", "1285.HK"), ("06098", "华虹半导体", "1347.HK"),
            ("02007", "康龙化成", "3759.HK"), ("00522", "ASM Pacific", "0522.HK"), ("00966", "华润微电子", "1596.HK"),
            ("01478", "丘钛科技", "1478.HK"), ("09988", "高鑫零售", "6808.HK"), ("00027", "银河娱乐", "0027.HK"),
            ("01128", "永利澳门", "1128.HK"), ("00880", "澳博控股", "0880.HK"), ("00200", "新濠国际", "0200.HK"),
            # 公用事业消费 (30只)
            ("00002", "中电控股", "0002.HK"), ("00006", "电能实业", "0006.HK"), ("00003", "香港中华煤气", "0003.HK"),
            ("00001", "长和", "0001.HK"), ("00012", "恒基地产", "0012.HK"), ("00688", "中国海外发展", "0688.HK"),
            ("01044", "恒安国际", "1044.HK"), ("00179", "德昌电机", "0179.HK"), ("00293", "国泰航空", "0293.HK"),
            ("00066", "港铁公司", "0066.HK"), ("00019", "太古股份公司A", "0019.HK"), ("00330", "思捷环球", "0330.HK"),
            ("00551", "裕元集团", "0551.HK"), ("00709", "佐丹奴国际", "0709.HK"), ("00836", "华润电力", "0836.HK"),
            ("01113", "长江基建", "1113.HK"), ("01177", "中粮糖业", "0506.HK"), ("03396", "联想控股", "3396.HK"),
            ("00384", "中国燃气", "0384.HK"), ("00762", "中国联通", "0762.HK"), ("00576", "浙江沪杭甬", "0576.HK"),
            ("00270", "粤海投资", "0270.HK"), ("01072", "东方海外", "0316.HK"), ("00548", "深圳高速公路", "0548.HK"),
            ("00659", "新创建集团", "0659.HK"), ("00882", "天津发展", "0882.HK"), ("00995", "安徽皖通高速", "0995.HK"),
            ("01052", "越秀交通", "1052.HK"),             ("00363", "上海实业控股", "0363.HK"), ("00737", "湾区发展", "0737.HK"),
            # 【V87.4】扩展港股备用池到200只 - 新增16只
            ("01299", "友邦保险", "1299.HK"), ("02628", "中国人寿", "2628.HK"), ("02318", "中国平安", "2318.HK"), ("01336", "新华保险", "1336.HK"),
            ("00857", "中国石油股份", "0857.HK"), ("00386", "中国石油化工", "0386.HK"), ("00883", "中国海洋石油", "0883.HK"), ("01088", "中国神华", "1088.HK"),
            ("00939", "建设银行", "0939.HK"), ("03988", "中国银行", "3988.HK"), ("01398", "工商银行", "1398.HK"), ("00998", "中信银行", "0998.HK"),
            ("01919", "中远海控", "1919.HK"), ("00753", "中国国航", "0753.HK"), ("00670", "中国东方航空", "0670.HK"), ("01055", "中国南方航空", "1055.HK"),
        ]
    
    # 3. 尝试从云端获取A股（250只）
    cn_pool = fetch_eastmoney_stock_list("cn", 500)
    if not cn_pool or len(cn_pool) < 30:
        # 【V94.3】二级云端源：免费行情源 市值榜（国内直连，稳定；已剔除ST/退市/北交所）
        try:
            from market_data_helper import fetch_cn_top_pool
            cn_pool = fetch_cn_top_pool(500)
            if cn_pool:
                _safe_print(f"[股票池] ✅ 二级源(免费行情源市值榜): {len(cn_pool)} 只")
        except Exception as _tse:
            _safe_print(f"[股票池] ⚠️ 免费行情源市值榜失败: {str(_tse)[:60]}")
    if not cn_pool or len(cn_pool) < 30:
        _safe_print("[股票池] ⚠️ A股云端获取失败，使用备用池（沪深300+创业板）")
        # 【V87.3】A股备用池扩展到240只
        cn_pool = [
            # 白酒食品
            ("600519", "贵州茅台", "600519.SS"), ("000858", "五粮液", "000858.SZ"),
            ("000568", "泸州老窖", "000568.SZ"), ("600809", "山西汾酒", "600809.SS"),
            ("000799", "酒鬼酒", "000799.SZ"), ("600887", "伊利股份", "600887.SS"),
            ("600132", "重庆啤酒", "600132.SS"),
            # 金融银行
            ("601318", "中国平安", "601318.SS"), ("600036", "招商银行", "600036.SS"),
            ("601398", "工商银行", "601398.SS"), ("601288", "农业银行", "601288.SS"),
            ("601988", "中国银行", "601988.SS"), ("601328", "交通银行", "601328.SS"),
            ("600000", "浦发银行", "600000.SS"), ("600016", "民生银行", "600016.SS"),
            ("601166", "兴业银行", "601166.SS"), ("000001", "平安银行", "000001.SZ"),
            ("002142", "宁波银行", "002142.SZ"), ("601169", "北京银行", "601169.SS"),
            # 证券保险
            ("600030", "中信证券", "600030.SS"), ("601688", "华泰证券", "601688.SS"),
            ("601788", "光大证券", "601788.SS"),
            ("601628", "中国人寿", "601628.SS"), ("601601", "中国太保", "601601.SS"),
            ("601336", "新华保险", "601336.SS"),
            # 新能源汽车
            ("002594", "比亚迪", "002594.SZ"), ("300750", "宁德时代", "300750.SZ"),
            ("300014", "亿纬锂能", "300014.SZ"), ("002812", "恩捷股份", "002812.SZ"),
            ("603799", "华友钴业", "603799.SS"),
            # 新能源光伏
            ("601012", "隆基绿能", "601012.SS"), ("688005", "容百科技", "688005.SS"),
            ("300124", "汇川技术", "300124.SZ"),
            # 半导体芯片
            ("688981", "中芯国际", "688981.SS"), ("002371", "北方华创", "002371.SZ"),
            ("603501", "韦尔股份", "603501.SS"), ("688008", "澜起科技", "688008.SS"),
            # 消费电子
            ("002475", "立讯精密", "002475.SZ"), ("000333", "美的集团", "000333.SZ"),
            ("000651", "格力电器", "000651.SZ"), ("002008", "大族激光", "002008.SZ"),
            ("002049", "紫光国微", "002049.SZ"),
            # 医药医疗
            ("600276", "恒瑞医药", "600276.SS"), ("000661", "长春高新", "000661.SZ"),
            ("300015", "爱尔眼科", "300015.SZ"), ("300760", "迈瑞医疗", "300760.SZ"),
            ("603259", "药明康德", "603259.SS"), ("688111", "金山办公", "688111.SS"),
            # 互联网传媒
            ("300059", "东方财富", "300059.SZ"), ("002230", "科大讯飞", "002230.SZ"),
            ("300033", "同花顺", "300033.SZ"),
            # 房地产建筑
            ("000002", "万科A", "000002.SZ"), ("601668", "中国建筑", "601668.SS"),
            ("601390", "中国中铁", "601390.SS"), ("601186", "中国铁建", "601186.SS"),
            ("601800", "中国交建", "601800.SS"), ("600585", "海螺水泥", "600585.SS"),
            # 能源资源
            ("601899", "紫金矿业", "601899.SS"), ("600028", "中国石化", "600028.SS"),
            ("601857", "中国石油", "601857.SS"), ("600019", "宝钢股份", "600019.SS"),
            ("601088", "中国神华", "601088.SS"), ("600900", "长江电力", "600900.SS"),
            ("601600", "中国铝业", "601600.SS"), ("601919", "中远海控", "601919.SS"),
            # 消费零售
            ("601888", "中国中免", "601888.SS"), ("601933", "永辉超市", "601933.SS"),
            ("603288", "海天味业", "603288.SS"),
            # 交运物流
            ("601018", "宁波港", "601018.SS"), ("600050", "中国联通", "600050.SS"),
            ("601766", "中国中车", "601766.SS"), ("601111", "中国国航", "601111.SS"),
            ("600029", "南方航空", "600029.SS"), ("601006", "大秦铁路", "601006.SS"),
            ("600018", "上港集团", "600018.SS"),
            # 化工材料
            ("600309", "万华化学", "600309.SS"), ("002756", "永兴材料", "002756.SZ"),
            ("600273", "嘉化能源", "600273.SS"),
            # 机械设备
            ("601989", "中国重工", "601989.SS"), ("600704", "物产中大", "600704.SS"),
            # 农林牧渔
            ("002714", "牧原股份", "002714.SZ"), ("000876", "新希望", "000876.SZ"),
            # 公用事业
            ("600015", "华夏银行", "600015.SS"), ("601818", "光大银行", "601818.SS"),
            # 其他宁波
            ("002805", "丰元股份", "002805.SZ"), ("603088", "宁波精达", "603088.SS"),
            ("301019", "宁波色母", "301019.SZ"), ("600366", "宁波韵升", "600366.SS"),
            ("002048", "宁波华翔", "002048.SZ"), ("600857", "宁波中百", "600857.SS"),
            ("600724", "宁波富达", "600724.SS"), ("600768", "宁波富邦", "600768.SS"),
            # 【V87.3】补充到240只 - 更多优质股票
            ("000063", "中兴通讯", "000063.SZ"), ("002352", "顺丰控股", "002352.SZ"),
            ("000725", "京东方A", "000725.SZ"), ("002415", "海康威视", "002415.SZ"),
            ("002241", "歌尔股份", "002241.SZ"), ("002049", "紫光国微", "002049.SZ"),
            ("300124", "汇川技术", "300124.SZ"), ("300124", "汇川技术", "300124.SZ"),
            ("300496", "中科创达", "300496.SZ"), ("300408", "三环集团", "300408.SZ"),
            ("300750", "宁德时代", "300750.SZ"), ("002129", "TCL中环", "002129.SZ"),
            ("002138", "顺络电子", "002138.SZ"), ("002273", "水晶光电", "002273.SZ"),
            ("002384", "东山精密", "002384.SZ"), ("002456", "欧菲光", "002456.SZ"),
            ("002466", "天齐锂业", "002466.SZ"), ("002497", "雅化集团", "002497.SZ"),
            ("002709", "天赐材料", "002709.SZ"), ("002812", "恩捷股份", "002812.SZ"),
            ("002920", "德赛西威", "002920.SZ"), ("300037", "新宙邦", "300037.SZ"),
            ("300122", "智飞生物", "300122.SZ"), ("300142", "沃森生物", "300142.SZ"),
            ("300274", "阳光电源", "300274.SZ"), ("300316", "晶盛机电", "300316.SZ"),
            ("300347", "泰格医药", "300347.SZ"), ("300408", "三环集团", "300408.SZ"),
            ("300433", "蓝思科技", "300433.SZ"), ("300450", "先导智能", "300450.SZ"),
            ("300496", "中科创达", "300496.SZ"), ("300529", "健帆生物", "300529.SZ"),
            ("300558", "贝达药业", "300558.SZ"), ("300595", "欧普康视", "300595.SZ"),
            ("300628", "亿联网络", "300628.SZ"), ("300763", "锦浪科技", "300763.SZ"),
            ("300782", "卓胜微", "300782.SZ"), ("600031", "三一重工", "600031.SS"),
            ("600048", "保利发展", "600048.SS"), ("600061", "国投资本", "600061.SS"),
            ("600089", "特变电工", "600089.SS"), ("600111", "北方稀土", "600111.SS"),
            ("600115", "中国东航", "600115.SS"), ("600188", "兖矿能源", "600188.SS"),
            ("600201", "生物股份", "600201.SS"), ("600298", "安琪酵母", "600298.SS"),
            ("600309", "万华化学", "600309.SS"), ("600325", "华发股份", "600325.SS"),
            ("600362", "江西铜业", "600362.SS"), ("600383", "金地集团", "600383.SS"),
            ("600436", "片仔癀", "600436.SS"), ("600547", "山东黄金", "600547.SS"),
            ("600570", "恒生电子", "600570.SS"), ("600584", "长电科技", "600584.SS"),
            ("600600", "青岛啤酒", "600600.SS"), ("600606", "绿地控股", "600606.SS"),
            ("600611", "大众交通", "600611.SS"), ("600650", "锦江在线", "600650.SS"),
            ("600703", "三安光电", "600703.SS"), ("600717", "天津港", "600717.SS"),
            ("600867", "通化东宝", "600867.SS"), ("600908", "无锡银行", "600908.SS"),
            ("600919", "江苏银行", "600919.SS"), ("600926", "杭州银行", "600926.SS"),
            ("600958", "东方证券", "600958.SS"), ("600999", "招商证券", "600999.SS"),
            ("601009", "南京银行", "601009.SS"), ("601021", "春秋航空", "601021.SS"),
            ("601066", "中信建投", "601066.SS"), ("601128", "常熟银行", "601128.SS"),
            ("601208", "东材科技", "601208.SS"), ("601225", "陕西煤业", "601225.SS"),
            ("601229", "上海银行", "601229.SS"), ("601298", "青岛港", "601298.SS"),
            ("601377", "兴业证券", "601377.SS"), ("601699", "潞安环能", "601699.SS"),
            ("601789", "宁波建工", "601789.SS"), ("601825", "沪农商行", "601825.SS"),
            ("601865", "福莱特", "601865.SS"), ("601872", "招商轮船", "601872.SS"),
            ("601877", "正泰电器", "601877.SS"), ("601878", "浙商证券", "601878.SS"),
            ("601898", "中煤能源", "601898.SS"), ("601916", "浙商银行", "601916.SS"),
            ("601997", "贵阳银行", "601997.SS"), ("603127", "昭衍新药", "603127.SS"),
            ("603160", "汇顶科技", "603160.SS"), ("603233", "大参林", "603233.SS"),
            ("603288", "海天味业", "603288.SS"), ("603369", "今世缘", "603369.SS"),
            ("603392", "万泰生物", "603392.SS"), ("603589", "口子窖", "603589.SS"),
            ("603659", "璞泰来", "603659.SS"), ("603806", "福斯特", "603806.SS"),
            ("603882", "金域医学", "603882.SS"), ("603986", "兆易创新", "603986.SS"),
            ("688005", "容百科技", "688005.SS"), ("688008", "澜起科技", "688008.SS"),
            ("688012", "中微公司", "688012.SS"), ("688018", "乐鑫科技", "688018.SS"),
            ("688032", "禾迈股份", "688032.SS"), ("688111", "金山办公", "688111.SS"),
            ("688123", "聚和材料", "688123.SS"), ("688126", "沪硅产业", "688126.SS"),
            ("688169", "石头科技", "688169.SS"), ("688256", "寒武纪", "688256.SS"),
            ("688303", "大全能源", "688303.SS"), ("688388", "嘉元科技", "688388.SS"),
            ("688390", "固德威", "688390.SS"), ("688396", "华润微", "688396.SS"),
            ("688599", "天合光能", "688599.SS"), ("688981", "中芯国际", "688981.SS"),
            # 【V87.4】扩展A股备用池到240只 - 新增35只
            # 更多银行股
            ("000001", "平安银行", "000001.SZ"), ("002142", "宁波银行", "002142.SZ"), ("600000", "浦发银行", "600000.SS"), ("601166", "兴业银行", "601166.SS"),
            ("000002", "万科A", "000002.SZ"), ("600048", "保利发展", "600048.SS"), ("001979", "招商蛇口", "001979.SZ"), ("600340", "华夏幸福", "600340.SS"),
            # 更多消费股
            ("600887", "伊利股份", "600887.SS"), ("000895", "双汇发展", "000895.SZ"), ("603288", "海天味业", "603288.SS"), ("000568", "泸州老窖", "000568.SZ"),
            ("600809", "山西汾酒", "600809.SS"), ("000596", "古井贡酒", "000596.SZ"), ("603369", "今世缘", "603369.SS"), ("000799", "酒鬼酒", "000799.SZ"),
            # 更多科技股
            ("002415", "海康威视", "002415.SZ"), ("000063", "中兴通讯", "000063.SZ"), ("002236", "大华股份", "002236.SZ"), ("300059", "东方财富", "300059.SZ"),
            ("300750", "宁德时代", "300750.SZ"), ("002460", "赣锋锂业", "002460.SZ"), ("300014", "亿纬锂能", "300014.SZ"), ("002129", "中环股份", "002129.SZ"),
            # 更多制造业
            ("000858", "五粮液", "000858.SZ"), ("600036", "招商银行", "600036.SS"), ("000725", "京东方A", "000725.SZ"), ("002027", "分众传媒", "002027.SZ"),
            ("600031", "三一重工", "600031.SS"), ("000002", "万科A", "000002.SZ"), ("600519", "贵州茅台", "600519.SS"), ("000001", "平安银行", "000001.SZ"),
            # 新能源汽车产业链
            ("002594", "比亚迪", "002594.SZ"), ("300124", "汇川技术", "300124.SZ"), ("002812", "恩捷股份", "002812.SZ"),
            # 【V87.4】最终补充到680只 - 再添加9只
            ("600585", "海螺水泥", "600585.SS"), ("000876", "新希望", "000876.SZ"), ("002304", "洋河股份", "002304.SZ"),
            ("600276", "恒瑞医药", "600276.SS"), ("300015", "爱尔眼科", "300015.SZ"), ("002142", "宁波银行", "002142.SZ"),
            ("600030", "中信证券", "600030.SS"), ("000776", "广发证券", "000776.SZ"), ("600837", "海通证券", "600837.SS"),
        ]
    
    _safe_print(f"[股票池] ✅ 初始化完成: 美股{len(us_pool)}只 | 港股{len(hk_pool)}只 | A股{len(cn_pool)}只 | 总计{len(us_pool)+len(hk_pool)+len(cn_pool)}只")
    
    return us_pool, hk_pool, cn_pool

def validate_stock_pool_health(pool_sample, pool_name, max_test=5):
    """【V87.4】股票池健康检查 - 检测无效股票代码"""
    _safe_print(f"[健康检查] 正在检查{pool_name}股票池...")
    
    invalid_codes = []
    test_count = min(len(pool_sample), max_test)
    
    for i, item in enumerate(pool_sample[:test_count]):
        code = item[2] if len(item) >= 3 else item[0]  # 使用yfinance格式代码
        
        try:
            df = fetch_stock_data(code)
            if df is None or len(df) == 0:
                invalid_codes.append((item, "无数据"))
                _safe_print(f"[健康检查] ❌ {code} ({item[1]}) - 无法获取数据")
            else:
                _safe_print(f"[健康检查] ✅ {code} ({item[1]}) - {len(df)}条数据")
        except Exception as e:
            invalid_codes.append((item, str(e)[:50]))
            _safe_print(f"[健康检查] ❌ {code} ({item[1]}) - 异常: {type(e).__name__}")
    
    if invalid_codes:
        _safe_print(f"[健康检查] ⚠️ {pool_name}发现{len(invalid_codes)}个问题代码，建议更新股票池")
        for item, error in invalid_codes:
            _safe_print(f"  - {item[0]} ({item[1]}): {error}")
    else:
        _safe_print(f"[健康检查] ✅ {pool_name}股票池健康状况良好")
    
    return invalid_codes

# 【V87】加载股票池（会被缓存24小时）
def _stratify_pool_by_letter(pool):
    """按首字母 A-Z 轮询排列，避免顺序扫描/yfinance 限流时只覆盖 A 段。"""
    if not pool:
        return pool
    from collections import defaultdict
    buckets = defaultdict(list)
    for it in pool:
        code = str(it[0] if it else "").strip().upper()
        letter = code[0] if code and code[0].isalpha() else "#"
        buckets[letter].append(it)
    letters = sorted(buckets.keys())
    out = []
    while any(buckets[L] for L in letters):
        for L in letters:
            if buckets[L]:
                out.append(buckets[L].pop(0))
    return out


def _pool_letter_count(pool) -> int:
    return len({str(it[0])[0].upper() for it in pool if it and str(it[0]) and str(it[0])[0].isalpha()})


RAW_US, RAW_HK, RAW_CN_TOP = init_stock_pools()

# 【V92】股票池按代码去重（内置/云端池存在重复录入，会导致同一只股票重复占榜）
def _dedup_pool(pool):
    if not pool:
        return pool
    _seen, _out = set(), []
    for it in pool:
        k = str(it[2] if len(it) >= 3 else it[0]).strip().upper()
        if k in _seen:
            continue
        _seen.add(k)
        _out.append(it)
    return _out

RAW_US = _dedup_pool(RAW_US)
RAW_HK = _dedup_pool(RAW_HK)
RAW_CN_TOP = _dedup_pool(RAW_CN_TOP)

# 【V88.14】字母轮询：修复扫描/筛选结果只剩 A 开头的问题
_us_letters = _pool_letter_count(RAW_US)
if _us_letters < 12:
    _safe_print(f"[股票池] ⚠️ 美股池首字母仅 {_us_letters} 种，混入备用池并轮询重排")
    try:
        from modules.stock_pool import get_backup_us_pool
        _bk = get_backup_us_pool()
    except Exception:
        _bk = []
    _seen = {str(x[2] if len(x) >= 3 else x[0]).upper() for x in RAW_US}
    for _s in _bk:
        _k = str(_s[2] if len(_s) >= 3 else _s[0]).upper()
        if _k not in _seen:
            RAW_US.append(_s)
            _seen.add(_k)
    RAW_US = RAW_US[:500]
    try:
        (_BRIEF_CACHE_DIR / "pool_cache.json").unlink(missing_ok=True)
    except Exception:
        pass
RAW_US = _stratify_pool_by_letter(RAW_US)
RAW_HK = _stratify_pool_by_letter(RAW_HK)
RAW_CN_TOP = _stratify_pool_by_letter(RAW_CN_TOP)

# ── 写股票池缓存（供 scan_worker.py 直接读取，避免二次拉取）─────────────
try:
    _pool_cache_path = _BRIEF_CACHE_DIR / "pool_cache.json"
    _pool_cache_age  = time.time() - json.loads(_pool_cache_path.read_text(encoding='utf-8')).get("ts", 0) \
                       if _pool_cache_path.exists() else 99999
    if _pool_cache_age > 3600:          # 超过 1 小时才刷写
        _BRIEF_CACHE_DIR.mkdir(exist_ok=True)
        _pool_cache_path.write_text(
            json.dumps({"ts": time.time(),
                        "US": RAW_US, "HK": RAW_HK, "CN": RAW_CN_TOP},
                       ensure_ascii=False),
            encoding="utf-8",
        )
except Exception:
    pass

# 【V82.4】轻量级名称索引 - 仅用于关键字搜索，不存储价格数据
STOCK_NAME_INDEX = {
    # ===== 美股热门 =====
    "AAPL": "苹果", "TSLA": "特斯拉", "NVDA": "英伟达", "MSFT": "微软",
    "GOOGL": "谷歌", "GOOG": "谷歌", "AMZN": "亚马逊", "META": "Meta",
    "BABA": "阿里巴巴", "BIDU": "百度", "JD": "京东", "PDD": "拼多多",
    "TSM": "台积电", "ASML": "阿斯麦", "AMD": "超微半导体", "INTC": "英特尔",
    "TME": "腾讯音乐", "NTES": "网易", "LI": "理想汽车", "XPEV": "小鹏汽车",
    "NIO": "蔚来汽车", "BILI": "哔哩哔哩", "IQ": "爱奇艺",
    
    # ===== 港股热门 =====
    # 互联网科技
    "00700.HK": "腾讯控股", "09988.HK": "阿里巴巴", "03690.HK": "美团",
    "01810.HK": "小米集团", "06618.HK": "京东健康", "01024.HK": "快手",
    "09618.HK": "京东集团", "09999.HK": "网易", "09626.HK": "哔哩哔哩",
    "09888.HK": "百度集团", "06060.HK": "众安在线",
    # 新能源汽车
    "02015.HK": "理想汽车", "09868.HK": "小鹏汽车", "09866.HK": "蔚来汽车",
    "00175.HK": "吉利汽车", "02238.HK": "广汽集团", "01211.HK": "比亚迪",
    "02460.HK": "宁德时代",
    # 地产金融
    "02899.HK": "紫金矿业", "03988.HK": "中国银行", "01398.HK": "工商银行",
    "01288.HK": "农业银行", "03968.HK": "招商银行", "02318.HK": "中国平安",
    "01339.HK": "中国人民保险", "00939.HK": "建设银行",
    # 消费
    "01876.HK": "百威亚太", "02319.HK": "蒙牛乳业", "00291.HK": "华润啤酒",
    
    # ===== A股热门 =====
    # 白酒食品
    "600519.SS": "贵州茅台", "000858.SZ": "五粮液", "000568.SZ": "泸州老窖",
    "600809.SS": "山西汾酒", "000799.SZ": "酒鬼酒", "603589.SS": "口子窖",
    "600887.SS": "伊利股份", "600132.SS": "重庆啤酒",
    # 金融
    "601318.SS": "中国平安", "600036.SS": "招商银行", "601398.SS": "工商银行",
    "601288.SS": "农业银行", "601988.SS": "中国银行", "601328.SS": "交通银行",
    "600000.SS": "浦发银行", "600016.SS": "民生银行", "601166.SS": "兴业银行",
    "000001.SZ": "平安银行", "002142.SZ": "宁波银行",
    "601628.SS": "中国人寿", "601601.SS": "中国太保", "601336.SS": "新华保险",
    "600030.SS": "中信证券", "600837.SS": "海通证券", "601788.SS": "光大证券",
    # 新能源
    "002594.SZ": "比亚迪", "300750.SZ": "宁德时代", "601012.SS": "隆基绿能",
    "688005.SS": "容百科技", "688981.SS": "中芯国际", "300014.SZ": "亿纬锂能",
    # 消费电子
    "002475.SZ": "立讯精密", "000333.SZ": "美的集团", "000651.SZ": "格力电器",
    "002008.SZ": "大族激光",
    # 医药
    "600276.SS": "恒瑞医药", "000661.SZ": "长春高新", "300015.SZ": "爱尔眼科",
    "300760.SZ": "迈瑞医疗", "603259.SS": "药明康德",
    # 地产基建
    "000002.SZ": "万科A", "601668.SS": "中国建筑", "601390.SS": "中国中铁",
    "601186.SS": "中国铁建", "601800.SS": "中国交建",
    # 能源资源
    "601899.SS": "紫金矿业", "600028.SS": "中国石化", "601857.SS": "中国石油",
    "600019.SS": "宝钢股份", "601088.SS": "中国神华", "600900.SS": "长江电力",
    "601600.SS": "中国铝业", "601919.SS": "中远海控",
    # 其他
    "601888.SS": "中国中免", "600050.SS": "中国联通", "601766.SS": "中国中车",
    "601111.SS": "中国国航", "600029.SS": "南方航空", "601006.SS": "大秦铁路",
    "601989.SS": "中国重工", "601818.SS": "光大银行", "600585.SS": "海螺水泥",
    "600018.SS": "上港集团", "600015.SS": "华夏银行",
    
    # ===== 宁波相关（完整版）=====
    "601018.SS": "宁波港", "002142.SZ": "宁波银行", "600366.SS": "宁波韵升",
    "002048.SZ": "宁波华翔", "603088.SS": "宁波精达", "301019.SZ": "宁波色母",
    "600857.SS": "宁波中百", "600724.SS": "宁波富达", "600768.SS": "宁波富邦",
    "600051.SS": "宁波联合", "002667.SZ": "宁波建工", "600452.SS": "涪陵电力",
    "002574.SZ": "明牌珠宝", "600884.SS": "杉杉股份", "002805.SZ": "丰元股份",
    "002756.SZ": "永兴材料", "603799.SS": "华友钴业", "600273.SS": "嘉化能源",
    "601777.SS": "力帆科技", "600704.SS": "物产中大", "600687.SS": "刚泰控股",
    "002098.SZ": "浔兴股份", "002098.SZ": "浔兴股份",
}

# ═══════════════════════════════════════════════════════════════
# 5. K线形态识别（15种）
# ═══════════════════════════════════════════════════════════════
def identify_kline_pattern(row, prev_row):
    """K线形态识别（保留完整15种）"""
    close, open_p, high, low = row['Close'], row['Open'], row['High'], row['Low']
    body = abs(close - open_p)
    total_range = high - low
    if total_range == 0: return "🛑 一字板"
    
    # 十字星家族
    if body <= total_range * 0.15:
        if (high - max(open_p, close)) > total_range * 0.4 and (min(open_p, close) - low) > total_range * 0.4: 
            return "🦵 长腿十字星 (变盘信号)"
        if (high - max(open_p, close)) > total_range * 0.6: 
            return "🪦 墓碑十字线 (顶部反转)"
        if (min(open_p, close) - low) > total_range * 0.6: 
            return "🐉 蜻蜓十字线 (底部反转)"
        return "⚖️ 十字星 (多空平衡)"
    
    upper_shadow = high - max(open_p, close)
    lower_shadow = min(open_p, close) - low
    
    # 锤头线家族
    if lower_shadow >= 2 * body and upper_shadow <= body * 0.3:
        if close > prev_row['Close']: return "🔨 锤头线 (底部看涨)"
        else: return "🪢 吊颈线 (顶部看跌)"
    
    # 倒锤头/射击之星
    if upper_shadow >= 2 * body and lower_shadow <= body * 0.3:
        if close > open_p: return "🛡️ 倒锤头 (底部信号)"
        else: return "🗡️ 射击之星 (顶部信号)"
    
    # 大阳/大阴线
    if body >= total_range * 0.8:
        if close > open_p: return "🔥 大阳线 (强烈看多)"
        else: return "❄️ 大阴线 (强烈看空)"
    
    # 中阳/中阴线
    if body >= total_range * 0.6:
        if close > open_p: return "📈 中阳线 (温和上涨)"
        else: return "📉 中阴线 (温和下跌)"
    
    # 小阳/小阴整理
    if close > open_p:
        return "➚ 小阳推进" if close > prev_row['Close'] else "➿ 小阳整理"
    else:
        return "➘ 小阴下探" if close < prev_row['Close'] else "➿ 小阴整理"

# ═══════════════════════════════════════════════════════════════
# 6. Alpha Matrix Agent（机构决策引擎）
# ═══════════════════════════════════════════════════════════════
class AlphaMatrixAgent:
    def decide(self, df):
        if df is None or len(df) < 20: return ("观察", "数据不足", 0.3, [])
        
        df = df.copy()
        for n in [5, 10, 20, 60, 120]: df[f'MA{n}'] = df['Close'].rolling(n).mean()
        
        last = df.iloc[-1]
        score = 0
        reasons = []
        tags = []
        
        # 趋势判断
        if last['Close'] > last.get('MA60', 0):
            score += 30
            reasons.append("站上季线")
            tags.append("趋势向上")
        
        # 动能判断
        if len(df) > 5:
            ret_5d = (last['Close'] - df['Close'].iloc[-6]) / df['Close'].iloc[-6]
            if ret_5d > 0.03:
                score += 20
                reasons.append("5日涨幅>3%")
                tags.append("动能强劲")
        
        # 量价配合
        if len(df) > 5:
            vol_ma5 = df['Volume'].tail(5).mean()
            if last['Volume'] > vol_ma5 * 1.2:
                score += 15
                reasons.append("放量")
                tags.append("资金活跃")
        
        conf = min(0.95, score / 100)
        
        if conf > 0.7: action = "买入"
        elif conf > 0.5: action = "持有"
        else: action = "观察"
        
        return action, " | ".join(reasons) if reasons else "震荡", conf, tags

rl_agent = AlphaMatrixAgent()

# ═══════════════════════════════════════════════════════════════
# 6.5 Gemini AI API 调用函数（提前定义，供后续所有模块使用）
# ═══════════════════════════════════════════════════════════════
# (call_model_api / call_model_api_stream 已前移至文件顶部——2026-07-31修使用先于定义)


def calculate_metrics_all(df, code, *, benchmark_loader=None):
    """
    【V87.16】完整的双核评级系统 - 增强防御性检查
    即使数据不足,也尽量计算能计算的指标
    """
    # 【V87.16】严格的防御性检查
    if df is None:
        logging.warning(f"⚠️ {code} DataFrame为None")
        return None
    
    if df.empty:
        logging.warning(f"⚠️ {code} DataFrame为空")
        return None
    
    if len(df) < 5:
        logging.warning(f"⚠️ {code} 数据不足5行: {len(df)}")
        return None
    
    if 'Close' not in df.columns:
        logging.error(f"❌ {code} 缺少Close列")
        return None
    
    try:
        df = df.apply(pd.to_numeric, errors='coerce').dropna().sort_index()
        
        if df.empty or len(df) < 5:
            logging.warning(f"⚠️ {code} 清洗后数据不足")
            return None
    
    except Exception as e:
        logging.error(f"❌ {code} 数据清洗失败: {type(e).__name__}")
        return None
    
    # 【V85】计算所有均线,即使数据不足120天也不返回None
    # 只计算数据量允许的均线
    for n in [5, 10, 20, 50, 60, 120, 150, 200, 250]:
        if len(df) >= n:
            df[f'MA{n}'] = df['Close'].rolling(n).mean()
        else:
            # 数据不足时,使用全部数据计算均线
            df[f'MA{n}'] = df['Close'].rolling(min(n, len(df))).mean()
    
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)
    rs = gain.ewm(com=13).mean() / loss.ewm(com=13).mean()
    df['RSI'] = 100 - (100 / (1 + rs))
    df['RSI'] = df['RSI'].fillna(50)
    
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else last
    
    # CANSLIM 7因子
    score_c = 0
    canslim_rows = []
    
    state_c = last['Close'] > last.get('MA50', 0)
    canslim_rows.append({"因子": "C: 当季收益", "状态": "✅" if state_c else "❌", "说明": "股价>MA50"})
    if state_c: score_c += 15
    
    state_a = last['Close'] > last.get('MA200', 0)
    canslim_rows.append({"因子": "A: 年度收益", "状态": "✅" if state_a else "❌", "说明": "股价>年线"})
    if state_a: score_c += 15
    
    l250 = df['Low'].tail(250).min() if len(df) >= 250 else df['Low'].min()
    h250 = df['High'].tail(250).max() if len(df) >= 250 else df['High'].max()
    dist_h = (last['Close'] - h250) / h250 * 100 if h250 > 0 else -100
    state_n = abs(dist_h) < 15
    canslim_rows.append({"因子": "N: 新高附近", "状态": "✅" if state_n else "❌", "说明": f"距前高{abs(dist_h):.1f}%"})
    if state_n: score_c += 15
    
    vol_ma5 = df['Volume'].tail(5).mean() if len(df) >= 5 else df['Volume'].mean()
    price_up = last['Close'] > prev['Close']
    state_s = (last['Volume'] > vol_ma5) and price_up
    canslim_rows.append({"因子": "S: 供需", "状态": "✅" if state_s else "❌", "说明": "放量上涨"})
    if state_s: score_c += 15
    
    state_l = last['RSI'] > 55
    if last['RSI'] > 85:
        score_c -= 10
        canslim_rows.append({"因子": "L: 领头羊", "状态": "⚠️ 过热", "说明": f"RSI={last['RSI']:.1f}"})
    else:
        canslim_rows.append({"因子": "L: 领头羊", "状态": "✅" if state_l else "❌", "说明": f"RSI={last['RSI']:.1f}"})
        if state_l: score_c += 10
    
    ma50 = df.get('MA50')
    s_i = False
    if ma50 is not None and len(ma50) > 5:
        s_i = ma50.iloc[-1] > ma50.iloc[-5]
    canslim_rows.append({"因子": "I: 机构持仓", "状态": "✅" if s_i else "❌", "说明": "MA50向上"})
    if s_i: score_c += 15
    
    state_m = last['Close'] > last.get('MA20', 0)
    canslim_rows.append({"因子": "M: 市场方向", "状态": "✅" if state_m else "❌", "说明": "站上月线"})
    if state_m: score_c += 15
    
    # 专业投机原理 7指标
    score_s = 0
    spec_rows = []
    
    t1 = last['Close'] > last.get('MA200', 0)
    spec_rows.append({"因子":"1. 长期趋势", "状态":"✅" if t1 else "❌", "说明":"当前>年线"})
    if t1: score_s += 10
    
    t2 = last['Close'] > last.get('MA50', 0)
    spec_rows.append({"因子":"2. 中期趋势", "状态":"✅" if t2 else "❌", "说明":"当前>生命线"})
    if t2: score_s += 10
    
    t3 = last['Close'] > last.get('MA20', 0)
    spec_rows.append({"因子":"3. 短期动能", "状态":"✅" if t3 else "❌", "说明":"当前>月线"})
    if t3: score_s += 10
    
    t4 = last['RSI'] > 50
    spec_rows.append({"因子":"4. 相对强度", "状态":"✅" if t4 else "❌", "说明":"RSI>50"})
    if t4: score_s += 10
    
    dev = abs(last['Close'] - last.get('MA20', last['Close'])) / last.get('MA20', 1) if last.get('MA20', 1) > 0 else 0
    if dev > 0.15:
        score_s -= 10
        spec_rows.append({"因子":"5. 波动乖离", "状态":"⚠️ 偏离", "说明": f"乖离{dev*100:.1f}%"})
    else:
        spec_rows.append({"因子":"5. 波动乖离", "状态":"✅ 正常", "说明": f"乖离{dev*100:.1f}%"})
        score_s += 10
    
    t6 = (last['Volume'] > vol_ma5) and price_up
    spec_rows.append({"因子":"6. 量价配合", "状态":"✅" if t6 else "❌", "说明":"放量上涨"})
    if t6: score_s += 20
    
    pos = (last['Close'] - l250) / (h250 - l250 + 0.001) if (h250 - l250) > 0 else 0.5
    t7 = pos > 0.8
    spec_rows.append({"因子":"7. 价格位置", "状态":"✅" if t7 else "❌", "说明":f"位于区间{pos*100:.0f}%处"})
    if t7: score_s += 30
    
    # ═══════════════════════════════════════════════════════════
    # 【V89.7 新增】ESG评分（基于技术面代理指标 + 行业特征）
    # E=环境 S=社会 G=治理，每项0-100分
    # ═══════════════════════════════════════════════════════════
    esg_rows = []
    esg_e_score = 50  # 环境基准50分
    esg_s_score = 50  # 社会基准50分
    esg_g_score = 50  # 治理基准50分
    
    # E-环境：用波动率稳定性代理（低波动=经营稳定=环境风险低）
    if len(df) >= 60:
        _vol_60 = df['Close'].pct_change().tail(60).std() * np.sqrt(252) * 100
        if _vol_60 < 20:
            esg_e_score = 75
            esg_rows.append({"维度": "🌿 E-环境", "评分": f"{esg_e_score}/100", "依据": f"年化波动率{_vol_60:.1f}%，经营稳定", "等级": "✅ 良好"})
        elif _vol_60 < 35:
            esg_e_score = 55
            esg_rows.append({"维度": "🌿 E-环境", "评分": f"{esg_e_score}/100", "依据": f"年化波动率{_vol_60:.1f}%，正常范围", "等级": "🟡 中等"})
        else:
            esg_e_score = 30
            esg_rows.append({"维度": "🌿 E-环境", "评分": f"{esg_e_score}/100", "依据": f"年化波动率{_vol_60:.1f}%，高波动风险", "等级": "❌ 较差"})
    else:
        esg_rows.append({"维度": "🌿 E-环境", "评分": f"{esg_e_score}/100", "依据": "数据不足，使用默认值", "等级": "🟡 中等"})
    
    # S-社会：用成交活跃度代理（高流动性=市场认可=社会关注度高）
    if len(df) >= 20:
        _avg_vol = df['Volume'].tail(20).mean()
        _vol_trend = df['Volume'].tail(5).mean() / _avg_vol if _avg_vol > 0 else 1
        if _vol_trend > 1.3 and price_up:
            esg_s_score = 80
            esg_rows.append({"维度": "👥 S-社会", "评分": f"{esg_s_score}/100", "依据": f"量比{_vol_trend:.2f}，资金积极流入", "等级": "✅ 良好"})
        elif _vol_trend > 0.8:
            esg_s_score = 60
            esg_rows.append({"维度": "👥 S-社会", "评分": f"{esg_s_score}/100", "依据": f"量比{_vol_trend:.2f}，市场关注度正常", "等级": "🟡 中等"})
        else:
            esg_s_score = 35
            esg_rows.append({"维度": "👥 S-社会", "评分": f"{esg_s_score}/100", "依据": f"量比{_vol_trend:.2f}，流动性不足", "等级": "❌ 较差"})
    else:
        esg_rows.append({"维度": "👥 S-社会", "评分": f"{esg_s_score}/100", "依据": "数据不足，使用默认值", "等级": "🟡 中等"})
    
    # G-治理：用价格趋势一致性代理（均线多头排列=管理层执行力强）
    _ma_aligned = 0
    if last['Close'] > last.get('MA20', 0): _ma_aligned += 1
    if last['Close'] > last.get('MA50', 0): _ma_aligned += 1
    if last['Close'] > last.get('MA120', 0): _ma_aligned += 1
    if last['Close'] > last.get('MA200', 0): _ma_aligned += 1
    if last.get('MA50', 0) > last.get('MA200', 0): _ma_aligned += 1
    
    if _ma_aligned >= 4:
        esg_g_score = 85
        esg_rows.append({"维度": "🏛️ G-治理", "评分": f"{esg_g_score}/100", "依据": f"均线{_ma_aligned}/5多头排列，趋势健康", "等级": "✅ 优秀"})
    elif _ma_aligned >= 2:
        esg_g_score = 55
        esg_rows.append({"维度": "🏛️ G-治理", "评分": f"{esg_g_score}/100", "依据": f"均线{_ma_aligned}/5多头，趋势分化", "等级": "🟡 中等"})
    else:
        esg_g_score = 25
        esg_rows.append({"维度": "🏛️ G-治理", "评分": f"{esg_g_score}/100", "依据": f"均线{_ma_aligned}/5多头，趋势恶化", "等级": "❌ 较差"})
    
    # ESG综合分
    esg_total = int(esg_e_score * 0.3 + esg_s_score * 0.3 + esg_g_score * 0.4)
    
    # ESG等级
    if esg_total >= 75:
        esg_grade = "AAA"
        esg_label = "🟢 ESG领先"
    elif esg_total >= 60:
        esg_grade = "AA"
        esg_label = "🟢 ESG良好"
    elif esg_total >= 45:
        esg_grade = "A"
        esg_label = "🟡 ESG中等"
    elif esg_total >= 30:
        esg_grade = "BB"
        esg_label = "🟠 ESG偏弱"
    else:
        esg_grade = "B"
        esg_label = "🔴 ESG较差"
    
    esg_rows.append({"维度": "📊 ESG综合", "评分": f"{esg_total}/100", "依据": f"E×30%+S×30%+G×40%", "等级": f"{esg_label} ({esg_grade})"})
    
    # ═══════════════════════════════════════════════════════════
    # 【V94 新增】第五维：动能与相对强度（MACD + 20日动量 + 量能趋势 + 相对大盘RS）
    # RS 用于区分「领涨股」与「跟着大盘涨的跟风股」，是分数参考性的关键增量
    # ═══════════════════════════════════════════════════════════
    momentum_rows = []
    mom_score = 0

    # 1. MACD 动能（12/26/9）：DIF>DEA 为多头，柱体走强为动能增强
    try:
        _ema12 = df['Close'].ewm(span=12, adjust=False).mean()
        _ema26 = df['Close'].ewm(span=26, adjust=False).mean()
        _dif = _ema12 - _ema26
        _dea = _dif.ewm(span=9, adjust=False).mean()
        _hist = _dif - _dea
        _macd_bull = float(_dif.iloc[-1]) > float(_dea.iloc[-1])
        _hist_rising = len(_hist) >= 3 and float(_hist.iloc[-1]) > float(_hist.iloc[-3])
        if _macd_bull: mom_score += 20
        if _hist_rising: mom_score += 10
        momentum_rows.append({"因子": "8. MACD动能", "状态": "✅" if _macd_bull else "❌",
                              "说明": ("DIF>DEA多头" if _macd_bull else "DIF<DEA空头") + ("，柱体走强" if _hist_rising else "")})
    except Exception:
        momentum_rows.append({"因子": "8. MACD动能", "状态": "—", "说明": "计算失败"})

    # 2. 20日动量
    chg20d = 0.0
    try:
        if len(df) >= 21:
            chg20d = (float(last['Close']) / float(df['Close'].iloc[-21]) - 1) * 100
        if chg20d > 0: mom_score += 15
        if chg20d > 5: mom_score += 5
        momentum_rows.append({"因子": "9. 20日动量", "状态": "✅" if chg20d > 0 else "❌", "说明": f"{chg20d:+.1f}%"})
    except Exception:
        pass

    # 3. 量能趋势：5日均量/20日均量 温和放量（1~2.5倍）且月线为正，
    #    比单日放量更能代表资金持续流入；>2.5倍多为消息脉冲，不加分
    try:
        _v20 = float(df['Volume'].tail(20).mean())
        _vratio = float(df['Volume'].tail(5).mean()) / _v20 if _v20 > 0 else 1.0
        _vol_ok = 1.0 <= _vratio <= 2.5 and chg20d > 0
        if _vol_ok: mom_score += 15
        # 【V99.6】量能变化明示：不写模糊的"增长"，直接说明显放量/温和放量/持平/明显缩量
        _vp_pct = (_vratio - 1) * 100
        _vp_lbl = ("明显放量" if _vp_pct >= 20 else "温和放量" if _vp_pct >= 8 else
                   "明显缩量" if _vp_pct <= -20 else "温和缩量" if _vp_pct <= -8 else "量能持平")
        momentum_rows.append({"因子": "10. 量能趋势", "状态": "✅" if _vol_ok else "❌",
                              "说明": f"{_vp_lbl}（5日均量较20日{_vp_pct:+.0f}%·量比{_vratio:.2f}）"})
    except Exception:
        pass

    # 4. 相对大盘强度 RS（20日超额收益）：指数数据走缓存，整批扫描只取一次
    rs20 = None
    try:
        _tc = to_yf_cn_code(code)
        _idx_code = get_benchmark_code(_tc)
        _idx_df = (benchmark_loader or fetch_stock_data)(_idx_code)
        if _idx_df is not None and len(_idx_df) >= 21:
            _idx_chg = (float(_idx_df['Close'].iloc[-1]) / float(_idx_df['Close'].iloc[-21]) - 1) * 100
            rs20 = chg20d - _idx_chg
            if rs20 > 0: mom_score += 20
            if rs20 > 5: mom_score += 15
            momentum_rows.append({"因子": "11. 相对强度RS", "状态": "✅" if rs20 > 0 else "❌",
                                  "说明": f"20日跑{'赢' if rs20 >= 0 else '输'}大盘 {abs(rs20):.1f}%"})
    except Exception:
        pass
    if rs20 is None:
        momentum_rows.append({"因子": "11. 相对强度RS", "状态": "—", "说明": "缺少同日合格指数数据，本项不计分；不授级"})

    mom_score = min(100, mom_score)
    # 附在专业投机因子表之后，深度作战室现有表格直接可见，无需新增UI
    spec_rows.extend(momentum_rows)

    # ═══════════════════════════════════════════════════════════
    # 【V94】五维综合评分 = CANSLIM×25% + 专业投机×25% + 动能RS×20% + ESG×15% + 风控×15%
    # ═══════════════════════════════════════════════════════════
    # 风控评分：基于RSI合理性 + 乖离率 + 价格位置
    _risk_control_score = 50
    if 30 < last['RSI'] < 70: _risk_control_score += 20  # RSI适中
    if abs(dev) < 0.05: _risk_control_score += 15  # 乖离小
    if 0.3 < pos < 0.85: _risk_control_score += 15  # 价格位置合理
    _risk_control_score = min(100, _risk_control_score)

    final_score = int(score_c * 0.25 + score_s * 0.25 + mom_score * 0.20
                      + esg_total * 0.15 + _risk_control_score * 0.15)
    # 【V94.1】RS连续微调 ±3分：因子都是5/10/15的整数档，容易同分扎堆；
    # 用相对大盘超额收益做连续调整，同档股票中领涨股自然排前
    if rs20 is not None:
        final_score = int(round(final_score + max(-3.0, min(3.0, rs20 / 3.0))))
    final_score = min(99, max(0, final_score))

    # 【V97】市场环境×行业强度调整层（七权重口径MVP：环境系数0.85~1.10 + 行业轮动±3）
    env_coef, sector_adj = 1.0, 0
    try:
        _cu = str(code).upper()
        _mk_t = "A股" if _cu.endswith((".SS", ".SZ", ".SH", ".BJ")) else ("港股" if _cu.endswith(".HK") else "美股")
        _mt = _load_market_temp()
        _ti = (_mt or {}).get(_mk_t) or {}
        if _ti.get("temp") is not None:
            env_coef = 0.85 + float(_ti["temp"]) / 100.0 * 0.25  # 冰点×0.85 ←→ 过热×1.10
        _rot = ((_mt or {}).get("_rotation") or {}).get(_mk_t) or {}
        _sec_nm = ""
        try:
            from modules.sector_map import get_sector as _gs97
            _sec_nm = str(_gs97(code, "") or "")
        except Exception:
            pass
        if _sec_nm:
            def _sec_hit(names):
                for _n in names:
                    _n2 = _n.replace("芯片", "").replace("ETF", "")
                    if _n in _sec_nm or _sec_nm in _n or (_n2 and _n2 in _sec_nm):
                        return True
                return False
            if _sec_hit(_rot.get("hot", [])):
                sector_adj = 3    # 所属板块资金轮入
            elif _sec_hit(_rot.get("cold", [])):
                sector_adj = -3   # 所属板块涨势退潮
        final_score = int(round(final_score * env_coef)) + sector_adj
        final_score = min(99, max(0, final_score))
    except Exception:
        pass
    
    # 【V91.0】策略文案差异化，结合RSI/趋势
    rsi_val = last.get('RSI', 50)
    above_ma20 = last['Close'] > last.get('MA20', 0) if last.get('MA20', 0) > 0 else False
    if final_score > 85:
        logic = f"🔥 强力进攻" + ("，均线多头趋势明确" if above_ma20 else "，等待确认突破")
    elif final_score > 60:
        logic = f"🛡️ 稳健持有" + (f"，RSI{rsi_val:.0f}适中" if 40 < rsi_val < 70 else "")
    else:
        logic = "❄️ 弱势回避" + (f"，RSI{rsi_val:.0f}偏离" if rsi_val > 70 or rsi_val < 30 else "")
    
    suggestion = "仅观察"
    if final_score >= 90: suggestion = "积极抢筹"
    elif final_score >= 75: suggestion = "分批建仓"
    elif final_score >= 60: suggestion = "等待确认"
    
    # K线形态识别
    pattern = identify_kline_pattern(last, prev)
    
    ma20 = last.get('MA20', last['Close'])
    bias = (last['Close'] - ma20) / ma20 * 100 if ma20 > 0 else 0
    df['TR'] = np.maximum((df['High'] - df['Low']), np.maximum(abs(df['High'] - df['Close'].shift(1)), abs(df['Low'] - df['Close'].shift(1))))
    atr = df['TR'].rolling(14).mean().iloc[-1] if len(df) >= 14 else 0
    vwap = (df['Close'] * df['Volume']).sum() / df['Volume'].sum() if df['Volume'].sum() > 0 else last['Close']
    
    # 使用Alpha Agent
    action, reason, conf, tags = rl_agent.decide(df)
    kelly = (conf * 2.0 - 1) / 2.0 if conf > 0.5 else 0.0
    kelly = max(0, kelly)
    
    # 【V83 P1.4】机构式交易计划
    trade_plan = calculate_trade_plan(df, code)
    
    # 【V91.0】实战修正：高分股需合理风险收益比，策略文案差异化（避免千篇一律）
    if trade_plan and final_score >= 75:
        risk_pct = trade_plan['risk_per_share'] / last['Close'] * 100 if last['Close'] > 0 else 0
        # 【V94.2】修复字段名错误：原来读不存在的 'risk_reward' 永远得 0，
        # 导致所有高分股被误判"盈亏比不足"集体降档（满屏74分的另一半根因）
        risk_reward = trade_plan.get('risk_reward_ratio', 0)
        rsi_val = last.get('RSI', 50)
        above_ma = last['Close'] > last.get('MA20', 0) if last.get('MA20', 0) > 0 else False
        
        # 【V94.1】降档但保序：原逻辑 min(score,74) 会把所有被降档的高分股
        # 钉死在同一个 74 分，导致榜单大量并列、头部排序失真。
        # 改为线性映射 75~99 → 60~74（原分越高映射后仍越高），
        # 再按缺陷严重程度追加 0~6 分惩罚，语义不变（仍低于75的建仓线）。
        def _demote(s):
            return 60 + (min(99, s) - 75) * 14 // 24

        if risk_pct > 20:
            final_score = max(40, _demote(final_score) - min(6, int((risk_pct - 20) * 0.3)))
            # 差异化文案：结合RSI/趋势
            if rsi_val > 65:
                logic = f"RSI偏高({rsi_val:.0f})，止损{risk_pct:.1f}%过宽，等回调再考虑"
            elif not above_ma:
                logic = f"当前弱于MA20，止损{risk_pct:.1f}%偏大，观望为主"
            else:
                logic = f"趋势向好但止损{risk_pct:.1f}%过宽，建议小仓位试探或等回调"
            suggestion = "观望"
        elif risk_reward < 1.2:
            final_score = max(40, _demote(final_score) - min(6, int((1.2 - risk_reward) * 5)))
            if risk_reward < 0.8:
                logic = f"盈亏比{risk_reward:.2f}:1严重不足，性价比差，暂不介入"
            elif above_ma:
                logic = f"价格高于MA20，趋势尚可，但盈亏比{risk_reward:.2f}:1偏低，可等更好买点"
            else:
                logic = f"盈亏比{risk_reward:.2f}:1不足，建议等待回调或放量突破"
            suggestion = "观望"
        elif risk_pct > 15:
            logic += f"；止损{risk_pct:.1f}%略宽，建议控制仓位"

    # 【V88·时机闸门｜用户定则】可买性时机修正（最终闸门）：高分=现在值得买。
    # 五维分只量"质量/动能"，不看位置与时机——曾出现"74分但趋势引擎判减仓"的
    # 表述矛盾（CRWD：87%高位+顶背离+价涨量跌）。现在用三端共用趋势引擎的结论
    # 强制压分：减仓≤58 / 回避≤45 / 等待≤64 / 试仓≤68；进攻/持有不干预。
    # 持仓者的"冲高减仓"提示由操作指引单独给出，评分只回答"现在能不能买"。
    trend_full, timing_note = None, ""
    try:
        # 提速：低分股（<45）不跑趋势引擎——反正上不了榜、指引也是回避；
        # 候选股（≥45）才做时机闸门+拐点识别（22:43实测全量跑13分钟的主因）
        from cloud_engine import analyze_trend_full as _atf100
        trend_full = _atf100(df) if final_score >= 45 else None
        if trend_full:
            _caps100 = {"回避": 45, "减仓": 58, "等待": 64, "试仓": 68}
            _cap100 = _caps100.get(trend_full.get("conclusion", ""))
            if _cap100 is not None and final_score > _cap100:
                timing_note = (f"{trend_full.get('stage', '')}·{trend_full.get('conclusion', '')}"
                               f"：评分 {final_score}→{_cap100}")
                final_score = _cap100
                suggestion = "持仓减仓" if trend_full["conclusion"] == "减仓" else "观望"
                logic = f"⏳ 质量强但时机差：{trend_full.get('stage', '')}·{trend_full.get('conclusion', '')}"
            spec_rows.append({
                "因子": "12. 时机修正", "状态": "⚠️降分" if timing_note else "✅",
                "说明": timing_note or f"{trend_full.get('stage', '')}·{trend_full.get('conclusion', '')}·时机不减分"})
    except Exception:
        pass

    return {
        "score": final_score, "logic": logic, "suggestion": suggestion,
        "trend_full": trend_full, "timing_note": timing_note,
        "action": action, "reason": reason, "tags": tags, "kelly": kelly,
        "canslim_rows": canslim_rows, "spec_rows": spec_rows,
        "pattern": pattern, "rsi": last['RSI'], "bias": bias, "atr": atr, "vwap": vwap,
        "last_price": last['Close'], "ma20": ma20, "df": df, "last": last,
        "trade_plan": trade_plan,  # 【V83 P1】新增交易计划
        # 【V89.7】ESG评级数据
        "esg_rows": esg_rows,
        "esg_total": esg_total,
        "esg_grade": esg_grade,
        "esg_label": esg_label,
        "esg_e": esg_e_score,
        "esg_s": esg_s_score,
        "esg_g": esg_g_score,
        # 【V94】动能与相对强度维度
        "mom_score": mom_score,
        "chg20d": chg20d,
        "rs20": rs20,
        "momentum_rows": momentum_rows,
    }

def calculate_advanced_quant(df):
    """
    【V87.16】量化回测指标 + 高级技术指标
    新增：MACD、Bollinger Bands
    """
    if df is None or len(df) < 20: 
        return {}
    
    # 防御性检查
    if 'Close' not in df.columns:
        logging.error("❌ DataFrame缺少Close列")
        return {}
    
    try:
        # 基础回测指标
        ret = df['Close'].pct_change().dropna()
        rf = 0.03 / 252
        sharpe = (ret.mean() - rf) / ret.std() * np.sqrt(252) if ret.std() > 0 else 0
        cum = (1 + ret).cumprod()
        max_dd = (cum.cummax() - cum).max()
        wins = len(ret[ret > 0])
        win_rate = wins / len(ret) if len(ret) > 0 else 0
        avg_win = ret[ret > 0].mean() if len(ret[ret > 0]) > 0 else 0
        avg_loss = abs(ret[ret < 0].mean()) if len(ret[ret < 0]) > 0 else 1
        pl_ratio = avg_win / avg_loss if avg_loss > 0 else 0
        volatility = ret.std() * np.sqrt(252) if ret.std() > 0 else 0
        
        # 【V87.16】MACD指标 (Fast=12, Slow=26, Signal=9)
        macd_data = {}
        if len(df) >= 26:
            exp1 = df['Close'].ewm(span=12, adjust=False).mean()
            exp2 = df['Close'].ewm(span=26, adjust=False).mean()
            macd = exp1 - exp2
            signal = macd.ewm(span=9, adjust=False).mean()
            histogram = macd - signal
            
            # 判断金叉/死叉
            if len(macd) >= 2:
                prev_diff = macd.iloc[-2] - signal.iloc[-2]
                curr_diff = macd.iloc[-1] - signal.iloc[-1]
                
                if prev_diff < 0 and curr_diff > 0:
                    macd_signal = "🟢 金叉 (看涨)"
                elif prev_diff > 0 and curr_diff < 0:
                    macd_signal = "🔴 死叉 (看跌)"
                elif curr_diff > 0:
                    macd_signal = "🟢 多头 (MACD>Signal)"
                else:
                    macd_signal = "🔴 空头 (MACD<Signal)"
            else:
                macd_signal = "N/A"
            
            macd_data = {
                'macd': f"{macd.iloc[-1]:.2f}",
                'signal': f"{signal.iloc[-1]:.2f}",
                'histogram': f"{histogram.iloc[-1]:.2f}",
                'macd_signal': macd_signal
            }
        
        # 【V87.16】Bollinger Bands (Window=20, Std=2)
        bb_data = {}
        if len(df) >= 20:
            sma20 = df['Close'].rolling(window=20).mean()
            std20 = df['Close'].rolling(window=20).std()
            upper_band = sma20 + (std20 * 2)
            lower_band = sma20 - (std20 * 2)
            
            current_price = df['Close'].iloc[-1]
            bb_width = ((upper_band.iloc[-1] - lower_band.iloc[-1]) / sma20.iloc[-1] * 100) if sma20.iloc[-1] > 0 else 0
            
            # 判断位置
            if current_price > upper_band.iloc[-1]:
                bb_position = "🔴 超买 (价格>上轨)"
            elif current_price < lower_band.iloc[-1]:
                bb_position = "🟢 超卖 (价格<下轨)"
            elif current_price > sma20.iloc[-1]:
                bb_position = "🟡 偏强 (价格>中轨)"
            else:
                bb_position = "🟡 偏弱 (价格<中轨)"
            
            bb_data = {
                'bb_upper': f"{upper_band.iloc[-1]:.2f}",
                'bb_middle': f"{sma20.iloc[-1]:.2f}",
                'bb_lower': f"{lower_band.iloc[-1]:.2f}",
                'bb_width': f"{bb_width:.2f}%",
                'bb_position': bb_position
            }
        
        return {
            "sharpe": f"{sharpe:.2f}",
            "max_dd": f"{max_dd*100:.2f}%",
            "volatility": f"{volatility*100:.1f}%",
            "win_rate": f"{win_rate*100:.1f}%",
            "pl_ratio": f"{pl_ratio:.2f}",
            **macd_data,
            **bb_data
        }
    
    except Exception as e:
        logging.error(f"❌ calculate_advanced_quant失败: {type(e).__name__}: {str(e)}")
        return {}

def monte_carlo_forecast(df, days=10, sims=1000):
    """蒙特卡洛预测"""
    try:
        last_p = df['Close'].iloc[-1]
        ret = df['Close'].pct_change().dropna()
        mu = ret.mean()
        sigma = ret.std()
        # 同一行情与参数复算必须一致，不因点击/刷新随机改写情景区间。
        seed_payload = df['Close'].to_csv() + f"|{days}|{sims}"
        rng = np.random.default_rng(int(hashlib.sha256(seed_payload.encode()).hexdigest()[:16], 16))
        final_prices = last_p * np.exp((mu - 0.5 * sigma**2) * days
                                      + sigma * np.sqrt(days) * rng.normal(0, 1, sims))
        p90 = np.percentile(final_prices, 90)
        p50 = np.percentile(final_prices, 50)
        p10 = np.percentile(final_prices, 10)
        return {"p90": p90, "p50": p50, "p10": p10}
    except:
        return None

# ═══════════════════════════════════════════════════════════════
# 【V83 P0.2】基准对比与风险指标
# ═══════════════════════════════════════════════════════════════
def get_benchmark_code(stock_code):
    """根据股票市场自动选择基准指数"""
    if stock_code.endswith('.HK'):
        return '^HSI'  # 恒生指数
    elif stock_code.endswith(('.SS', '.SZ', '.BJ')):
        return '000001.SS'  # 上证指数
    else:
        return '^GSPC'  # 标普500

def calculate_risk_metrics(df, stock_code, *, benchmark_loader=None):
    """
    【V83 P0.2】计算风险指标：Beta, Alpha, Correlation, Volatility
    
    参数：
        df: 股票数据DataFrame
        stock_code: 股票代码（用于判断基准）
    
    返回：
        包含 alpha, beta, correlation, volatility 的字典
    """
    try:
        if df is None or len(df) < 60:
            return None
        
        # 获取基准指数
        benchmark_code = get_benchmark_code(stock_code)
        _safe_print(f"[Risk] 获取基准指数: {benchmark_code}")
        
        # 获取基准数据（使用相同时间范围）
        benchmark_df = (benchmark_loader or fetch_stock_data)(benchmark_code)
        if benchmark_df is None or len(benchmark_df) < 60:
            _safe_print(f"[Risk] ⚠️ 基准数据获取失败")
            return None
        
        # 对齐日期（取交集）
        common_dates = df.index.intersection(benchmark_df.index)
        if len(common_dates) < 60:
            _safe_print(f"[Risk] ⚠️ 共同日期不足60天")
            return None
        
        stock_aligned = df.loc[common_dates, 'Close']
        benchmark_aligned = benchmark_df.loc[common_dates, 'Close']
        
        # 计算收益率
        stock_ret = stock_aligned.pct_change().dropna()
        benchmark_ret = benchmark_aligned.pct_change().dropna()
        
        # 再次对齐（去除NaN后）
        common_idx = stock_ret.index.intersection(benchmark_ret.index)
        stock_ret = stock_ret.loc[common_idx]
        benchmark_ret = benchmark_ret.loc[common_idx]
        
        if len(stock_ret) < 30:
            _safe_print(f"[Risk] ⚠️ 有效数据点不足30天")
            return None
        
        # 计算指标（年化）
        # Beta: Cov(stock, benchmark) / Var(benchmark)
        covariance = np.cov(stock_ret, benchmark_ret)[0, 1]
        benchmark_variance = np.var(benchmark_ret)
        beta = covariance / benchmark_variance if benchmark_variance > 0 else 1.0
        
        # Alpha: 股票年化收益 - (无风险利率 + Beta * (基准年化收益 - 无风险利率))
        rf_annual = 0.03  # 无风险利率3%
        stock_annual_return = stock_ret.mean() * 252
        benchmark_annual_return = benchmark_ret.mean() * 252
        alpha = stock_annual_return - (rf_annual + beta * (benchmark_annual_return - rf_annual))
        
        # Correlation: 相关系数
        correlation = np.corrcoef(stock_ret, benchmark_ret)[0, 1]
        
        # Volatility: 年化波动率
        volatility = stock_ret.std() * np.sqrt(252)
        
        _safe_print(f"[Risk] ✅ Beta={beta:.2f}, Alpha={alpha*100:.2f}%, Corr={correlation:.2f}, Vol={volatility*100:.1f}%")
        
        return {
            'alpha': alpha,
            'beta': beta,
            'correlation': correlation,
            'volatility': volatility,
            'benchmark': benchmark_code,
            'benchmark_name': '标普500' if benchmark_code == '^GSPC' else ('恒生指数' if benchmark_code == '^HSI' else '上证指数')
        }
    except Exception as e:
        _safe_print(f"[Risk] ❌ 计算失败: {type(e).__name__}: {str(e)[:100]}")
        return None

# ═══════════════════════════════════════════════════════════════
# 【V84 自检与诊断模块】System Self-Diagnostic
# ═══════════════════════════════════════════════════════════════
def run_system_diagnostic():
    """
    【V84.1】系统自检：网络连通性 + 数据源冒烟测试
    
    返回：
        {
            'network': {'status': 'ok'/'error', 'message': str, 'latency': float},
            'data_sources': {
                'us': {'status': 'ok'/'error', 'code': 'AAPL', 'message': str, 'data_points': int},
                'hk': {...},
                'cn': {...}
            },
            'overall': 'healthy'/'warning'/'error'
        }
    """
    result = {
        'network': {},
        'data_sources': {},
        'overall': 'healthy'
    }
    
    # ═══ 1️⃣ 网络连通性测试 ═══
    try:
        start_time = time.time()
        proxy_url = get_proxy_url()
        
        # 测试Google连通性
        test_url = "https://www.google.com"
        if proxy_url:
            proxies = {"http": proxy_url, "https": proxy_url}
            response = requests.get(test_url, proxies=proxies, timeout=5, verify=True)
        else:
            response = requests.get(test_url, timeout=5, verify=True)
        
        latency = (time.time() - start_time) * 1000  # 转换为毫秒
        
        if response.status_code == 200:
            result['network'] = {
                'status': 'ok',
                'message': f'网络连通正常（延迟 {latency:.0f}ms）',
                'latency': latency
            }
        else:
            result['network'] = {
                'status': 'warning',
                'message': f'网络可访问但响应异常（HTTP {response.status_code}）',
                'latency': latency
            }
            result['overall'] = 'warning'
    except requests.exceptions.ProxyError as e:
        result['network'] = {
            'status': 'error',
            'message': f'代理连接失败：{str(e)[:100]}',
            'latency': 0
        }
        result['overall'] = 'error'
    except requests.exceptions.Timeout:
        result['network'] = {
            'status': 'error',
            'message': '网络超时（>5秒）',
            'latency': 5000
        }
        result['overall'] = 'error'
    except Exception as e:
        result['network'] = {
            'status': 'error',
            'message': f'网络测试失败：{type(e).__name__}',
            'latency': 0
        }
        result['overall'] = 'error'
    
    # ═══ 2️⃣ 数据源冒烟测试 ═══
    # 【V85 增强】随机抽取3只港股和3只美股进行测试
    import random
    
    # 固定基础测试
    test_stocks = [
        ('cn', '600519.SS', 'A股（茅台）')
    ]
    
    # 随机抽取3只港股
    hk_codes = [item[2] for item in RAW_HK]  # 使用第3个元素（已经是.HK格式）
    hk_samples = random.sample(hk_codes, min(3, len(hk_codes)))
    for hk_code in hk_samples:
        hk_name = next((item[1] for item in RAW_HK if item[2] == hk_code), hk_code)
        test_stocks.append(('hk', hk_code, f'港股（{hk_name}）'))
    
    # 随机抽取3只美股
    us_codes = [item[0] for item in RAW_US]
    us_samples = random.sample(us_codes, min(3, len(us_codes)))
    for us_code in us_samples:
        us_name = next((item[1] for item in RAW_US if item[0] == us_code), us_code)
        test_stocks.append(('us', us_code, f'美股（{us_name}）'))
    
    for market, code, name in test_stocks:
        try:
            _safe_print(f"[诊断] 测试 {name} ({code})...")
            df = fetch_stock_data(code)
            
            if df is not None and not df.empty and len(df) >= 5:
                result['data_sources'][market] = {
                    'status': 'ok',
                    'code': code,
                    'name': name,
                    'message': f'数据正常（{len(df)} 条记录）',
                    'data_points': len(df),
                    'last_date': df.index[-1].strftime('%Y-%m-%d')
                }
                _safe_print(f"[诊断] ✅ {name} ({code}): {len(df)} 条数据")
            elif df is not None and not df.empty:
                result['data_sources'][market] = {
                    'status': 'warning',
                    'code': code,
                    'name': name,
                    'message': f'数据不足（仅 {len(df)} 条记录，建议>5条）',
                    'data_points': len(df)
                }
                if result['overall'] == 'healthy':
                    result['overall'] = 'warning'
                _safe_print(f"[诊断] ⚠️ {name} ({code}): 仅 {len(df)} 条数据")
            else:
                result['data_sources'][market] = {
                    'status': 'error',
                    'code': code,
                    'name': name,
                    'message': '❌ 数据获取失败（返回 0 行数据） - 代理配置无效或 Yahoo 接口被封',
                    'data_points': 0
                }
                result['overall'] = 'error'
                _safe_print(f"[诊断] ❌ {name} ({code}): 0 条数据 - 接口失败！")
        except Exception as e:
            result['data_sources'][market] = {
                'status': 'error',
                'code': code,
                'name': name,
                'message': f'测试异常：{type(e).__name__} - {str(e)[:80]}',
                'data_points': 0
            }
            result['overall'] = 'error'
    
    return result

# ═══════════════════════════════════════════════════════════════
# 【V83 P0.3】事实新闻源
# ═══════════════════════════════════════════════════════════════
@st.cache_data(ttl=900)  # 【V91.3】交易日15分钟缓存
def fetch_news_headlines(code):
    """
    【V87.5优化】获取真实新闻标题 + 增强多源获取
    
    参数：
        code: 股票代码
    
    返回：
        新闻列表，每条包含 {time, title, source, link, summary}
    """
    try:
        if not HAS_YFINANCE:
            _safe_print(f"[News] ⚠️ yfinance未安装")
            return []
        
        target_code = to_yf_cn_code(code)
        proxy_url = get_proxy_url()
        
        _safe_print(f"[News] 🔍 开始获取 {target_code} 的新闻...")
        
        with ProxyContext(proxy_url):
            ticker = yf.Ticker(target_code)
            # Streamlit executes on a worker thread where SIGALRM is invalid.
            # A timeout must return once, never retry the same unbounded call.
            from deep_optional_data import bounded_call
            news = bounded_call(lambda: ticker.news, timeout=5)
        
        _safe_print(f"[News] 📊 原始新闻数量: {len(news) if news else 0}")
        
        if not news or len(news) == 0:
            _safe_print(f"[News] ⚠️ 无真实新闻，使用AI生成舆情")
            return []
        
        # 【V87.5】格式化新闻，增加摘要
        # 兼容新旧两种 yfinance 结构：新版字段嵌在 item['content'] 里
        formatted_news = []
        for item in news[:8]:  # 【V87.5】增加到8条
            content = item.get('content') if isinstance(item.get('content'), dict) else item

            summary = content.get('summary') or content.get('description') or ""
            if len(summary) > 200:
                summary = summary[:200] + "..."

            title = content.get('title') or item.get('title') or '无标题'

            # 时间：旧版 providerPublishTime(秒级时间戳)，新版 pubDate(ISO字符串)
            news_time = 'N/A'
            if item.get('providerPublishTime'):
                news_time = pd.Timestamp(item['providerPublishTime'], unit='s').strftime('%Y-%m-%d %H:%M')
            elif content.get('pubDate'):
                try:
                    news_time = pd.Timestamp(content['pubDate']).strftime('%Y-%m-%d %H:%M')
                except Exception:
                    pass

            source = item.get('publisher') or (content.get('provider') or {}).get('displayName', '未知来源')
            link = item.get('link') or (content.get('canonicalUrl') or {}).get('url', '')

            formatted_news.append({
                'time': news_time,
                'title': title,
                'source': source,
                'link': link,
                'summary': summary
            })
        
        _safe_print(f"[News] ✅ 成功获取 {len(formatted_news)} 条新闻")
        return formatted_news
        
    except TimeoutError:
        _safe_print(f"[News] ⏱️ 获取超时")
        return []
    except Exception as e:
        _safe_print(f"[News] ❌ 获取失败: {type(e).__name__} - {str(e)}")
        return []

def _fetch_deep_supplement(code):
    """Optional facts only; each provider wait is bounded and never calls a model."""
    from deep_optional_data import bounded_call
    from stock_profile import get_profile
    from announcement_radar import fetch_for, DIR_TXT
    result = {'errors': [], 'announcement_directions': dict(DIR_TXT)}
    for key, provider in (
        ('profile', lambda: get_profile(code)),
        ('extremes', lambda: _price_extremes9(code)),
        ('announcements', lambda: fetch_for(code)),
        ('news', lambda: fetch_news_headlines(code)),
        ('fundamentals', lambda: fetch_stock_fundamentals(code)),
    ):
        try:
            result[key] = bounded_call(provider, timeout=4)
        except Exception as exc:
            result['errors'].append(f'{key}: {type(exc).__name__}')
    return result


# ═══════════════════════════════════════════════════════════════
# 【V88·人话理由（预算自适应，个股/大盘/板块共用）】
# 默认规则版（不烧钱）；手动点按钮才切思考模式并立刻出结果；预算用满自动关、按钮禁用。
# ═══════════════════════════════════════════════════════════════
_REASON_FUSE_LABEL = {"个股": "基本面＋新闻＋技术面",
                      "大盘": "宏观＋资金＋技术面",
                      "板块": "行业＋催化＋技术面"}


@st.fragment
def render_readable_reasons(fwd, *, kind, symbol, name, context="", key_prefix=""):
    """在 fwd（含 horizons: label/view/p_up…）下方渲染每周期一句人话理由。
    预算自适应：默认规则版；点按钮切思考模式；预算到底自动关。个股/大盘/板块通用。
    【V88·原地交互铁律 2026-07-18 用户点单】st.fragment=点按钮只重跑本块：
    页面不滚动不整页刷新，精讲内容原地出现——治"点了像刷新页面,要找半天"。"""
    try:
        from stock_horizon import forward_reasons as _forward_reasons
    except Exception as _e:
        st.caption(f"判断理由暂不可用：{type(_e).__name__}")
        return
    _exhausted = not bool(GPT_SUBSCRIPTION_READY)
    _sk = f"_reason_think_{key_prefix}_{symbol}"
    _think_on = bool(st.session_state.get(_sk, False))

    st.markdown(f"#### 🗣️ 各周期判断理由（人话版：{_REASON_FUSE_LABEL.get(kind, '')}）")
    if _exhausted:
        st.button("🧠 开启思考模式精讲", key=f"btn{_sk}", disabled=True,
                  help="GPT-6 Codex订阅不可用，思考模式已自动关闭")
        st.caption("💤 GPT-6 Codex订阅不可用，当前为规则版；登录订阅后可开启。")
        _think_on = False
        st.session_state[_sk] = False
    else:
        _lab = "🔄 重新用思考模式精讲" if _think_on else "🧠 开启GPT-6 Astra思考精讲（使用订阅额度）"
        if st.button(_lab, key=f"btn{_sk}"):
            _think_on = True
            st.session_state[_sk] = True

    if _think_on and not _exhausted:
        with st.spinner("🧠 GPT-6 Astra思考：把每档判断讲成人话…"):
            _out = _forward_reasons(
                name, symbol, fwd, context=context, kind=kind, allow_ai=True,
                api_key=GPT_SUBSCRIPTION_READY)
    else:
        # 默认规则版：allow_ai=False 强制不调用 AI、不花预算（即使环境有 Key）。
        _out = _forward_reasons(name, symbol, fwd, context=context, kind=kind, allow_ai=False)

    # 【V88·基本面+新闻必到场 2026-07-18 用户点单】标着"基本面+新闻+技术面"就必须真有：
    # 🏢这是家什么公司 + 📰近日什么消息导致判断——规则版也要有,不许只剩技术套话。
    if kind == "个股":
        _prof9r = ""
        try:
            _fn0r = (st.session_state.get(f'_fundamentals_{symbol}') or {})
            from deep_optional_data import session_once as _read_optional
            _fn0r = _read_optional(st.session_state, symbol).get('fundamentals') or _fn0r
            _pb9r = []
            for _k9r, _lab9r in (("sector", "行业"), ("industry", "细分")):
                if _fn0r.get(_k9r):
                    _pb9r.append(f"{_lab9r} {_fn0r[_k9r]}")
            for _k9r, _lab9r in (("trailing_pe", "市盈率"), ("price_to_book", "市净率")):
                try:
                    if _fn0r.get(_k9r):
                        _pb9r.append(f"{_lab9r}{float(_fn0r[_k9r]):.1f}")
                except (TypeError, ValueError):
                    pass
            _prof9r = " · ".join(_pb9r)
        except Exception:
            _prof9r = ""
        _nws9r = []
        try:
            _na0r = json.loads((core_root() / "data" /
                                "news_analyzed.json").read_text(encoding="utf-8"))
            _bc9r = str(symbol).split(".")[0].lstrip("0")
            from news_evidence import current_news, news_note
            for _n9r in current_news(_na0r):
                _blob9r = str(_n9r.get("title", "")) + str(_n9r.get("affected_tickers", ""))
                if ((len(str(name)) >= 2 and str(name) in _blob9r)
                        or (_bc9r and len(_bc9r) >= 4 and _bc9r in str(_n9r.get("affected_tickers", "")))):
                    _dir9r = str(_n9r.get("impact_direction") or "")
                    _tag9r = ("🔴利好" if "好" in _dir9r else ("🟢利空" if "空" in _dir9r else "⚪中性"))
                    # 【2026-07-18 出处铁律】消息必带媒体名
                    _src9r = str(_n9r.get("source") or "").split(" - ")[0].strip()[:16] or "新闻流"
                    _nws9r.append(f"{str(_n9r.get('title'))[:42]}（{_tag9r}·{_src9r}；{news_note(_n9r)}）")
                if len(_nws9r) >= 2:
                    break
        except Exception:
            pass
        if _prof9r:
            st.markdown(f"🏢 **这是家什么公司**：{_prof9r}")
        # 【V88·基本面必带定性 2026-07-18 全系统】有研报覆盖就给行业优势句(带出处);
        # 消息面下方已有专行,这里只取机构研报,不重复。
        _eg9r = _v88_fund_edge(name, inst_only=True)
        if _eg9r:
            st.markdown(_eg9r)
        if _nws9r:
            st.markdown("📰 **近期关联线索（不证明因果）**：" + "；".join(_nws9r))
        else:
            st.caption("📰 近3日新闻流中无该股直接消息——本次判断以技术面与估值为主（如实说明，不编事由）。")
    if _out.get("overall"):
        st.success(f"**一句话：{_out.get('overall')}**")
    _m = _out.get("reasons") or {}
    for _r in fwd.get("horizons") or []:
        _lb = _r.get("label")
        st.markdown(f"- **{_lb}**（规则方向分 {_r.get('p_up')}/100）：{_m.get(_lb) or '—'}")
    _stt = _out.get("status")
    if _stt in ("completed", "cached"):
        st.caption(f"🧠 {_out.get('model', 'gpt-6-astra')} · 思考模式 ｜ 生成于 {_out.get('analysis_time', '—')}")
    elif _stt == "budget":
        st.caption("ℹ️ GPT-6订阅额度/限流触发，已回退规则版。仅保留原规则分与技术盈亏比，不作为已标定概率。")
    else:
        st.caption("ℹ️ 当前为规则版大白话理由（点上方按钮用思考模式精讲）。仅保留原规则分与技术盈亏比，不作为已标定概率。")

# ═══════════════════════════════════════════════════════════════
# 【V83 P1】交易计划与风险预算
# ═══════════════════════════════════════════════════════════════
def calculate_trade_plan(df, code):
    """
    【V83 P1.4】机构式交易计划
    
    参数：
        df: 股票数据DataFrame
        code: 股票代码
    
    返回：
        包含entry_zone, stop_loss, take_profit, risk_reward, position_size的字典
    """
    try:
        if df is None or len(df) < 50:
            return None
        
        last = df.iloc[-1]
        current_price = last['Close']
        
        # ATR（已在df中计算）
        high_low = df['High'] - df['Low']
        high_close = np.abs(df['High'] - df['Close'].shift())
        low_close = np.abs(df['Low'] - df['Close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        atr = ranges.max(axis=1).rolling(14).mean().iloc[-1]
        
        # MA20和MA50（应该已经在df中）
        ma20 = df['Close'].rolling(20).mean().iloc[-1] if len(df) >= 20 else current_price
        ma50 = df['Close'].rolling(50).mean().iloc[-1] if len(df) >= 50 else current_price
        
        # 1️⃣ 入场区间：MA20 ± ATR * 0.5
        entry_low = ma20 - atr * 0.5
        entry_high = ma20 + atr * 0.5
        
        # 2️⃣ 止损位：MA50 - ATR（或前低）
        recent_low = df['Low'].tail(20).min()
        stop_loss = min(ma50 - atr, recent_low - atr * 0.5)
        
        # 3️⃣ 止盈位：1.5R和2R
        risk = current_price - stop_loss if current_price > stop_loss else atr
        take_profit_15r = current_price + risk * 1.5
        take_profit_2r = current_price + risk * 2.0

        # 4️⃣ 【V94.2】真实盈亏比：目标位取实际阻力，而非"现价+1.5R"的恒等式
        #    - 已在52周高点附近（突破形态）：目标 = 现价 + 2×ATR
        #    - 近60日高点在上方2%以上：目标 = 60日高点（第一阻力）
        #    - 否则：目标 = 52周高点（主阻力）
        h60 = float(df['High'].tail(60).max())
        h250 = float(df['High'].tail(250).max()) if len(df) >= 250 else float(df['High'].max())
        if current_price >= h250 * 0.98:
            target = current_price + 2 * atr
        elif h60 > current_price * 1.02:
            target = h60
        else:
            target = h250
        real_rr = (target - current_price) / risk if risk > 0 else 0
        
        # 【V83 P1.5】风险预算仓位建议
        total_equity = 100000  # 假设总资金10万
        risk_budget_pct = 0.01  # 单笔风险1%
        risk_amount = total_equity * risk_budget_pct
        max_position = int(risk_amount / (current_price - stop_loss)) if (current_price - stop_loss) > 0 else 0
        position_value = max_position * current_price
        
        return {
            'entry_low': entry_low,
            'entry_high': entry_high,
            'entry_mid': (entry_low + entry_high) / 2,
            'stop_loss': stop_loss,
            'take_profit_15r': take_profit_15r,
            'take_profit_2r': take_profit_2r,
            'risk_per_share': current_price - stop_loss,
            'reward_15r': take_profit_15r - current_price,
            'reward_2r': take_profit_2r - current_price,
            'target': target,
            'risk_reward_ratio': real_rr,
            'current_price': current_price,
            'max_position': max_position,
            'position_value': position_value,
            'risk_budget_pct': risk_budget_pct * 100
        }
    except Exception as e:
        _safe_print(f"[TradePlan] ❌ 计算失败: {type(e).__name__}")
        return None

# ═══════════════════════════════════════════════════════════════
# 7.5 【V87.8】失败详情显示函数
# ═══════════════════════════════════════════════════════════════
def display_scan_failures(all_errors, total_failed):
    """显示扫描失败的详细信息"""
    with st.expander(f"⚠️ 查看失败详情 ({total_failed}只) - 点击展开诊断", expanded=False):
        st.caption("💡 **常见失败原因**：")
        st.caption("1. 股票已退市或被收购（如ATVI被微软收购）")
        st.caption("2. 股票代码格式错误")
        st.caption("3. 网络连接问题或代理设置错误")
        st.caption("4. 数据源暂时不可用")
        st.divider()
        
        # 按市场分组显示
        us_errors = []
        hk_errors = []
        cn_errors = []
        
        for e in all_errors:
            code = e['code']
            if '.HK' in code or (len(code) == 5 and code[0] == '0'):
                hk_errors.append(e)
            elif '.SS' in code or '.SZ' in code or (len(code) == 6 and code[0] in '630'):
                cn_errors.append(e)
            else:
                us_errors.append(e)
        
        if us_errors:
            st.markdown("**🇺🇸 美股失败列表：**")
            for err in us_errors:
                st.caption(f"❌ **{err['name']}** ({err['code']}): {err['error']}")
        
        if hk_errors:
            st.markdown("**🇭🇰 港股失败列表：**")
            for err in hk_errors:
                st.caption(f"❌ **{err['name']}** ({err['code']}): {err['error']}")
        
        if cn_errors:
            st.markdown("**🇨🇳 A股失败列表：**")
            for err in cn_errors:
                st.caption(f"❌ **{err['name']}** ({err['code']}): {err['error']}")

# ═══════════════════════════════════════════════════════════════
# 【V89.7 重构】持仓管理 - 包装为函数，延迟到主内容区渲染
# ═══════════════════════════════════════════════════════════════

# 持仓统一在当前风险中心与Astra成交记录中呈现。

# ═══════════════════════════════════════════════════════════════
# 8. 批量扫描（增强版）
# ═══════════════════════════════════════════════════════════════
def _score_coil(df) -> dict:
    """
    潜伏型评分 — 寻找"尚未启动但蓄势待发"的个股。
    核心逻辑：量缩价稳 + 波动率收缩 + 站上关键均线 + 相对强度良好。

    返回 dict: {score(0-100), signals(list), setup(str)}
    """
    if df is None or len(df) < 60 or "Close" not in df.columns:
        return None
    try:
        df = df.copy()
        close = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        high = df["High"].astype(float)
        low  = df["Low"].astype(float)

        # 均线
        ma20  = close.rolling(20).mean()
        ma50  = close.rolling(50).mean()
        ma200 = close.rolling(200).mean() if len(df) >= 200 else None

        last_c  = float(close.iloc[-1])
        last_v  = float(volume.iloc[-1])
        avg_v20 = float(volume.tail(20).mean())
        avg_v60 = float(volume.tail(60).mean()) if len(df) >= 60 else avg_v20

        # ── 信号1：ATR 收缩（近10日波动 < 近60日均值的70%）
        atr10 = float((high - low).tail(10).mean())
        atr60 = float((high - low).tail(60).mean()) if len(df) >= 60 else atr10
        atr_contracting = atr10 < atr60 * 0.70

        # ── 信号2：成交量萎缩（近10日均量 < 60日均量的75%）—— 机构持仓不动
        vol_drying = float(volume.tail(10).mean()) < avg_v60 * 0.75

        # ── 信号3：价格贴近 MA20（±3%）且 MA20 走平或向上
        near_ma20 = abs(last_c / float(ma20.iloc[-1]) - 1) < 0.03 if float(ma20.iloc[-1]) > 0 else False
        ma20_flat_up = float(ma20.iloc[-1]) >= float(ma20.iloc[-5]) if len(ma20) >= 5 else False

        # ── 信号4：站上 MA50
        above_ma50 = last_c > float(ma50.iloc[-1]) if float(ma50.iloc[-1]) > 0 else False

        # ── 信号5：站上 MA200（长期多头结构）
        above_ma200 = (ma200 is not None and last_c > float(ma200.iloc[-1]) and float(ma200.iloc[-1]) > 0)

        # ── 信号6：60日高低点区间收窄（最近20日区间 < 60日区间的60%）
        range60 = float(high.tail(60).max() - low.tail(60).min())
        range20 = float(high.tail(20).max() - low.tail(20).min())
        range_contracting = (range20 < range60 * 0.60) if range60 > 0 else False

        # ── 信号7：价格处于60日高点的75%-95%（不在顶部，但也不离高点太远）
        h60 = float(high.tail(60).max())
        price_zone = (h60 * 0.75 <= last_c <= h60 * 0.95) if h60 > 0 else False

        # ── 评分
        score = 0
        signals = []
        if atr_contracting:   score += 20; signals.append("🔇 波动收缩")
        if vol_drying:        score += 20; signals.append("📉 量能萎缩")
        if near_ma20 and ma20_flat_up: score += 15; signals.append("📐 贴近MA20")
        if above_ma50:        score += 15; signals.append("✅ 站上MA50")
        if above_ma200:       score += 15; signals.append("🏔 站上MA200")
        if range_contracting: score += 10; signals.append("🎯 区间收窄")
        if price_zone:        score += 5;  signals.append("📍 价格蓄势区")

        setup = "强蓄势" if score >= 70 else ("蓄势中" if score >= 45 else "弱蓄势")
        return {"score": min(100, score), "signals": signals, "setup": setup}
    except Exception:
        return None


def _score_breakout(df) -> dict:
    """
    启动型评分 — 寻找"刚刚突破、已开始启动"的个股。
    核心逻辑：放量突破关键阻力 + 价格站稳 + 非超买区间 + 近日创新高。

    返回 dict: {score(0-100), signals(list), setup(str)}
    """
    if df is None or len(df) < 30 or "Close" not in df.columns:
        return None
    try:
        df = df.copy()
        close  = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        high   = df["High"].astype(float)
        low    = df["Low"].astype(float)
        open_  = df["Open"].astype(float)

        last_c  = float(close.iloc[-1])
        last_v  = float(volume.iloc[-1])
        avg_v20 = float(volume.tail(20).mean())

        # 均线
        ma20  = float(close.rolling(20).mean().iloc[-1])
        ma50  = float(close.rolling(50).mean().iloc[-1]) if len(df) >= 50 else 0
        ma200 = float(close.rolling(200).mean().iloc[-1]) if len(df) >= 200 else 0

        # RSI
        delta = close.diff()
        gain = delta.where(delta > 0, 0).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)
        rsi = float(100 - 100 / (1 + gain.ewm(com=13).mean().iloc[-1] /
                                  (loss.ewm(com=13).mean().iloc[-1] + 1e-10)))

        # ── 信号1：放量突破（今日量 > 20日均量 × 1.5）
        volume_surge = last_v > avg_v20 * 1.5

        # ── 信号2：突破20日/50日/60日新高（最近5日内创新高）
        h20_prev = float(high.iloc[-6:-1].max()) if len(df) >= 6 else 0
        new_high_5d = last_c > h20_prev if h20_prev > 0 else False

        # ── 信号3：突破60日高点（更强信号）
        h60_prev = float(high.iloc[-61:-1].max()) if len(df) >= 61 else 0
        breakout_60d = last_c > h60_prev if h60_prev > 0 else False

        # ── 信号4：收盘在今日区间上75%（非假突破）
        daily_range = float(high.iloc[-1] - low.iloc[-1])
        strong_close = ((last_c - float(low.iloc[-1])) / daily_range > 0.75) if daily_range > 0 else False

        # ── 信号5：站上全部关键均线（MA20/MA50/MA200）
        above_all = last_c > ma20 and (ma50 == 0 or last_c > ma50) and (ma200 == 0 or last_c > ma200)

        # ── 信号6：RSI 在健康区间（55-75），有动能但不超买
        rsi_healthy = 55 <= rsi <= 75

        # ── 信号7：近3日涨幅（3%-15%），已启动但未过热
        ret3 = (last_c / float(close.iloc[-4]) - 1) * 100 if len(df) >= 4 else 0
        started_move = 3.0 <= ret3 <= 15.0

        # ── 评分
        score = 0
        signals = []
        if volume_surge:   score += 25; signals.append(f"🔥 放量{last_v/avg_v20:.1f}x")
        if new_high_5d:    score += 20; signals.append("📈 5日新高")
        if breakout_60d:   score += 15; signals.append("🚀 突破60日高")
        if strong_close:   score += 15; signals.append("💪 强势收盘")
        if above_all:      score += 10; signals.append("✅ 站上三线")
        if rsi_healthy:    score += 10; signals.append(f"📊 RSI{rsi:.0f}健康")
        if started_move:   score += 5;  signals.append(f"⚡ 3日+{ret3:.1f}%")

        setup = "强启动" if score >= 70 else ("启动中" if score >= 45 else "弱启动")
        return {"score": min(100, score), "signals": signals, "setup": setup}
    except Exception:
        return None


# ─── 双通道辅助 ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _get_benchmark_return(market: str, days: int = 5) -> float:
    """
    拉取基准指数N日收益率，用于相对强弱计算。
    market: 'US' → SPY, 'HK' → ^HSI, 'CN' → 000300.SS
    """
    _BM = {"US": "SPY", "HK": "^HSI", "CN": "000300.SS"}
    ticker = _BM.get(market, "SPY")
    try:
        import yfinance as yf
        df = yf.download(ticker, period="30d", progress=False, auto_adjust=True)
        if df is None or len(df) < days + 1:
            return 0.0
        closes = df["Close"].dropna()
        if len(closes) < days + 1:
            return 0.0
        return float((closes.iloc[-1] / closes.iloc[-(days+1)] - 1) * 100)
    except Exception:
        return 0.0


def _score_inflection(df) -> dict | None:
    """
    拐点通道（赔率）— 三关全中才入池。
    寻找「尚在底部但结构开始改善」的标的。

    Gate1 预期上修代理：
        价格处于6个月区间底部40% AND
        (RSI底背离 OR 近5日正收益 & 近20日跌幅>5%)

    Gate2 结构不再恶化：
        近10日最低点 > 前10日最低点（不再创新低）
        AND 今日未破20日最低收盘

    Gate3 止跌量能改善：
        近10日中上涨日的日均成交量 > 下跌日的日均成交量

    全部通过 → 评分（0-100），否则返回 None。
    """
    if df is None or len(df) < 40 or "Close" not in df.columns:
        return None
    try:
        df = df.copy()
        close  = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        high   = df["High"].astype(float)
        low    = df["Low"].astype(float)

        # RSI
        delta = close.diff()
        gain = delta.where(delta > 0, 0).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)
        rsi = float(100 - 100 / (1 + gain.ewm(com=13).mean().iloc[-1] /
                                  (loss.ewm(com=13).mean().iloc[-1] + 1e-10)))

        last_c = float(close.iloc[-1])
        period = min(126, len(df))   # ~6个月

        # ── Gate1：预期上修代理 ──
        h6m = float(high.tail(period).max())
        l6m = float(low.tail(period).min())
        range6m = h6m - l6m
        pos6m = (last_c - l6m) / range6m if range6m > 0 else 0.5
        in_bottom_50 = pos6m <= 0.50

        ret5  = float(close.iloc[-1] / close.iloc[-6] - 1) * 100  if len(close) >= 6  else 0
        ret20 = float(close.iloc[-1] / close.iloc[-21] - 1) * 100 if len(close) >= 21 else 0

        recent_low_close = float(close.tail(20).min())
        rsi_divergence = (recent_low_close <= last_c * 1.03) and (rsi > 35)

        rebound_signal = (ret5 > 0) and (ret20 < -3)
        gate1 = in_bottom_50 and (rsi_divergence or rebound_signal)

        # ── Gate2：结构不再恶化 ──
        if len(low) >= 20:
            low10_recent = float(low.iloc[-10:].min())
            low10_prev   = float(low.iloc[-20:-10].min())
            higher_lows  = low10_recent > low10_prev * 0.99
        else:
            higher_lows = False

        low20_close = float(close.tail(20).min())
        not_new_low = last_c > low20_close * 0.98
        gate2 = higher_lows and not_new_low

        # ── Gate3：止跌量能改善 ──
        recent_10 = df.tail(10).copy()
        up_days   = recent_10[recent_10["Close"] >= recent_10["Open"]]
        down_days = recent_10[recent_10["Close"] <  recent_10["Open"]]
        avg_vol_up   = float(up_days["Volume"].mean())   if len(up_days)   > 0 else 0
        avg_vol_down = float(down_days["Volume"].mean()) if len(down_days) > 0 else 1
        gate3 = avg_vol_up > avg_vol_down

        gates_met = sum([gate1, gate2, gate3])
        if gates_met < 2:
            return None

        # ── 评分（满足2关以上按信号强度打分）──
        score   = 0
        signals = []

        bottom_score = int((0.50 - pos6m) / 0.50 * 30) if pos6m <= 0.50 else 0
        score += bottom_score
        signals.append(f"📍 底部{pos6m*100:.0f}%位")

        if rebound_signal:
            score += 20
            signals.append(f"↩️ 5日+{ret5:.1f}% 20日{ret20:.1f}%")
        if rsi_divergence:
            score += 15
            signals.append(f"📈 RSI底背离{rsi:.0f}")
        if higher_lows:
            score += 20
            signals.append("🔼 高低点抬升")
        vol_ratio = avg_vol_up / avg_vol_down if avg_vol_down > 0 else 1
        score += min(15, int(vol_ratio * 5))
        signals.append(f"💰 买量/卖量={vol_ratio:.1f}x")
        if gates_met == 3:  score += 10

        setup = "强拐点" if score >= 65 else ("拐点中" if score >= 40 else "拐点")
        return {"score": min(100, score), "signals": signals, "setup": setup,
                "gate1": gate1, "gate2": gate2, "gate3": gate3,
                "pos6m": pos6m, "ret5": ret5, "ret20": ret20, "rsi": rsi}
    except Exception:
        return None


def _score_breakout_v2(df, benchmark_ret5: float = 0.0) -> dict | None:
    """
    启动通道（胜率）— 三信号满足≥1/3即入池，满足越多分越高。

    Signal1 突破关键位：收盘 > 过去20日最高收盘价
    Signal2 量能确认：今日量 > 20日均量 × 1.3
    Signal3 相对强弱转强：个股5日涨幅 > 基准5日涨幅 + 1.5%
    """
    if df is None or len(df) < 25 or "Close" not in df.columns:
        return None
    try:
        df = df.copy()
        close  = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        high   = df["High"].astype(float)
        low    = df["Low"].astype(float)

        last_c  = float(close.iloc[-1])
        last_v  = float(volume.iloc[-1])
        avg_v20 = float(volume.tail(20).mean())

        delta = close.diff()
        gain = delta.where(delta > 0, 0).fillna(0)
        loss = (-delta.where(delta < 0, 0)).fillna(0)
        rsi  = float(100 - 100 / (1 + gain.ewm(com=13).mean().iloc[-1] /
                                   (loss.ewm(com=13).mean().iloc[-1] + 1e-10)))

        high20_prev = float(close.iloc[-21:-1].max()) if len(close) >= 21 else float(close.iloc[:-1].max())
        s1_breakout = last_c > high20_prev
        s1_margin   = (last_c / high20_prev - 1) * 100 if high20_prev > 0 else 0

        s2_volume   = last_v > avg_v20 * 1.3
        s2_ratio    = last_v / avg_v20 if avg_v20 > 0 else 1

        ret5 = float((close.iloc[-1] / close.iloc[-6] - 1) * 100) if len(close) >= 6 else 0
        s3_rs = ret5 > benchmark_ret5 + 1.5

        met = sum([s1_breakout, s2_volume, s3_rs])
        if met < 1:
            return None

        daily_range = float(high.iloc[-1] - low.iloc[-1])
        strong_close = ((last_c - float(low.iloc[-1])) / daily_range > 0.70) if daily_range > 0 else False
        rsi_ok = 45 <= rsi <= 80

        score   = 0
        signals = []

        if s1_breakout:
            score += 35
            signals.append(f"🚀 突破+{s1_margin:.1f}%")
        if s2_volume:
            score += 30
            signals.append(f"🔥 量{s2_ratio:.1f}x")
        if s3_rs:
            score += 25
            signals.append(f"💪 RS+{ret5-benchmark_ret5:.1f}%")
        if strong_close:
            score += 5
            signals.append("⬆️ 强收盘")
        if rsi_ok:
            score += 5
            signals.append(f"RSI{rsi:.0f}")

        setup = "强启动" if score >= 70 else ("启动中" if score >= 45 else "弱启动")
        return {"score": min(100, score), "signals": signals, "setup": setup,
                "s1": s1_breakout, "s2": s2_volume, "s3": s3_rs,
                "met": met, "ret5": ret5, "rsi": rsi}
    except Exception:
        return None


def _gen_rationale(df, code: str, name: str, channel: str, result: dict) -> str:
    """
    生成每只股票的一行理由：
    变量 → 预期差 → 价格位置 → 验证窗口
    """
    try:
        close = df["Close"].astype(float)
        volume = df["Volume"].astype(float)
        last_c = float(close.iloc[-1])
        ma20   = float(close.rolling(20).mean().iloc[-1])
        ma50   = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else 0
        h52w   = float(df["High"].tail(252).max()) if len(df) >= 252 else float(df["High"].max())
        dist_h = (last_c / h52w - 1) * 100 if h52w > 0 else 0

        if channel == "INFLECTION":
            pos6m  = result.get("pos6m", 0.5)
            ret5   = result.get("ret5",  0)
            ret20  = result.get("ret20", 0)
            rsi    = result.get("rsi",   50)
            # 变量
            var_part = "量能回升+低点抬高" if result.get("gate3") else "结构企稳"
            # 预期差
            exp_part = f"市场仍在恐慌（RSI{rsi:.0f}），但买量已>{1:.0f}x卖量" if rsi < 45 else f"底部{pos6m*100:.0f}%位反弹{ret5:+.1f}%"
            # 价格位置
            pos_part = f"现价{last_c:.2f}，距MA20 {(last_c/ma20-1)*100:+.1f}%"
            # 验证窗口
            ver_part = "3-5日内需站上MA20确认"
            return f"变量:{var_part} → 预期差:{exp_part} → 价格:{pos_part} → 验证:{ver_part}"

        else:  # BREAKOUT
            ret5   = result.get("ret5", 0)
            rsi    = result.get("rsi",  60)
            met    = result.get("met",  2)
            sig_n  = "三信号共振" if met == 3 else "双信号确认"
            var_part = sig_n + "放量突破"
            exp_part = f"市场未追，距52周高{dist_h:.1f}%" if dist_h < -5 else "接近历史高位突破"
            pos_part = f"现价{last_c:.2f}，突破后RSI{rsi:.0f}"
            ver_part = "48h内需维持在突破位上方"
            return f"变量:{var_part} → 预期差:{exp_part} → 价格:{pos_part} → 验证:{ver_part}"
    except Exception:
        return "数据计算中"


def batch_scan_dual(pool, market: str = "US", progress_callback=None) -> dict:
    from modules.sector_map import get_sector   # 2026-07-31修:本作用域内使用先于导入
    """
    双通道扫描：同时运行拐点通道 + 启动通道，各取Top10。

    返回:
        {
          "inflection": [...top10],   # 拐点Top10
          "breakout":   [...top10],   # 启动Top10
          "stats":      {...}
        }
    """
    bm_ret5 = _get_benchmark_return(market, days=5)

    inflection_pool = []
    breakout_pool   = []
    stats = {"success": 0, "failed": 0, "total": len(pool)}

    # 【V92 修复】扫描前预热行情：美股/港股批量下载(避免限流只取到A段)，A股 免费行情源
    if len(pool) > 1:
        _prefetch_pool(pool, progress_callback)

    for idx, item in enumerate(pool):
        try:
            if progress_callback:
                progress_callback(idx + 1, len(pool), item[1] if len(item) > 1 else "")

            code, name = item[0], item[1] if len(item) > 1 else item[0]
            yf_code = item[2] if len(item) > 2 else code
            c_fixed = to_yf_cn_code(yf_code) if yf_code == code else yf_code

            df = fetch_stock_data(c_fixed)
            if df is None or len(df) < 30 or "Close" not in df.columns:
                stats["failed"] += 1
                continue
            if not (0 < float(df["Close"].iloc[-1]) < 1_000_000):
                stats["failed"] += 1
                continue

            stats["success"] += 1

            # 拐点通道
            inf_r = _score_inflection(df)
            if inf_r:
                rationale = _gen_rationale(df, code, name, "INFLECTION", inf_r)
                inflection_pool.append({
                    "股票": name, "代码": code,
                    "行业": get_sector(code, name),
                    "得分": inf_r["score"],
                    "形态": inf_r["setup"],
                    "信号": " ".join(inf_r["signals"][:3]),
                    "理由": rationale,
                    "现价": f"{float(df['Close'].iloc[-1]):.2f}",
                })

            # 启动通道
            bo_r = _score_breakout_v2(df, benchmark_ret5=bm_ret5)
            if bo_r:
                rationale = _gen_rationale(df, code, name, "BREAKOUT", bo_r)
                breakout_pool.append({
                    "股票": name, "代码": code,
                    "行业": get_sector(code, name),
                    "得分": bo_r["score"],
                    "形态": bo_r["setup"],
                    "信号": " ".join(bo_r["signals"][:3]),
                    "理由": rationale,
                    "现价": f"{float(df['Close'].iloc[-1]):.2f}",
                })

        except Exception as e:
            stats["failed"] += 1
            _safe_print(f"[双通道] ❌ {item[0]}: {type(e).__name__}: {str(e)[:60]}")

    inflection_top10 = sorted(inflection_pool, key=lambda x: x["得分"], reverse=True)[:10]
    breakout_top10   = sorted(breakout_pool,   key=lambda x: x["得分"], reverse=True)[:10]

    _safe_print(f"[双通道] {market} 完成 ✅{stats['success']} ❌{stats['failed']} | 拐点{len(inflection_pool)} 启动{len(breakout_pool)}")
    return {"inflection": inflection_top10, "breakout": breakout_top10,
            "stats": stats, "bm_ret5": bm_ret5}


def batch_scan_analysis(pool, scan_type="TOP", ma_target=None, progress_callback=None):
    """
    批量扫描股票。

    scan_type:
        TOP        — 趋势强势（原有逻辑）
        MA_TOUCH   — 均线回踩（原有逻辑）
        COIL       — 潜伏蓄势（量缩价稳，等待启动）
        BREAKOUT   — 刚刚启动（放量突破，已开始上涨）
    """
    results = []
    stats = {
        'success': 0,
        'failed': 0,
        'errors': []
    }
    
    total_stocks = len(pool)
    
    # 【V91.7】使用统一行业映射模块（sector_map.py），全682只覆盖，单一数据源
    from modules.sector_map import get_sector

    # 【V92 修复】扫描前预热行情：美股/港股批量下载(避免限流只取到A段)，A股 免费行情源
    if total_stocks > 1:
        _prefetch_pool(pool, progress_callback)

    for idx, item in enumerate(pool):
        # 【V87.14】调用进度回调
        if progress_callback:
            progress_callback(idx + 1, total_stocks, item[1] if len(item) > 1 else item[0])
        
        # 【V84.3】每个股票都用try-except包裹，防止单个错误中断整个扫描
        try:
            code = item[0]
            name = item[1]
            # 【V82.9关键修复】如果pool有3个元素，直接使用第3个（已经是正确的yfinance格式）
            if len(item) >= 3:
                c_fixed = item[2]
            else:
                c_fixed = to_yf_cn_code(code)
            
            # 【V87.4】优化请求间隔 - 减少延迟提高速度
            if idx > 0 and idx % 20 == 0:  # 改为每20个股票延迟
                time.sleep(0.2)  # 减少延迟时间
            
            df = fetch_stock_data(c_fixed)
            
            # 【V87.8】增强数据验证和详细日志
            if df is None or df.empty:
                stats['failed'] += 1
                # 【V87.8】详细记录失败原因
                error_msg = f'数据获取失败（代码:{c_fixed}, 原始:{code}）'
                stats['errors'].append({
                    'code': code,
                    'name': name,
                    'error': error_msg
                })
                # 【V87.8】打印详细日志帮助诊断
                _safe_print(f"[扫描失败] ❌ {name} ({code}) -> yfinance代码: {c_fixed}")
                _safe_print(f"           原因: 返回空数据或None")
                continue
            
            # 【V87.4】数据质量检查 - 确保有足够的数据点
            if len(df) < 20:  # 至少需要20个交易日的数据
                stats['failed'] += 1
                error_msg = f'数据不足（仅{len(df)}条记录）'
                stats['errors'].append({
                    'code': code,
                    'name': name,
                    'error': error_msg
                })
                continue
            
            # 【V87.4】价格数据合理性检查
            current_price = df['Close'].iloc[-1]
            if current_price <= 0 or current_price > 100000:  # 价格范围检查
                stats['failed'] += 1
                error_msg = f'价格异常（{current_price}）'
                stats['errors'].append({
                    'code': code,
                    'name': name,
                    'error': error_msg
                })
                _safe_print(f"[扫描] ❌ {code} ({name}) {error_msg}")
                continue
            
            if len(df) < 20:
                stats['failed'] += 1
                error_msg = f'数据不足（仅{len(df)}条，需要>20条）'
                stats['errors'].append({
                    'code': code,
                    'name': name,
                    'error': error_msg
                })
                # 【V85】只在控制台打印数据不足信息
                _safe_print(f"[扫描] ⚠️ {code} ({name}) {error_msg}")
                continue
            
            # 数据有效，继续处理
            if df is not None:
                m = calculate_metrics_all(df, c_fixed)
            if m:
                from v88_decision_core import evaluate_decision as _evaluate_batch_decision
                _decision = _evaluate_batch_decision(
                    df, m.get('trend_full') or {}, name=name, code=code)
                if _decision.get("error"):
                    continue
                _unified_score = int(_decision["unified_score"])
                is_hit = False
                
                if scan_type == "TOP":
                    if _unified_score > 40: is_hit = True
                elif scan_type == "COIL":
                    _coil = _score_coil(df)
                    if _coil and _coil['score'] >= 45:
                        m['_special_score'] = _coil['score']
                        m['_special_signals'] = _coil['signals']
                        m['_special_setup']   = _coil['setup']
                        is_hit = True
                elif scan_type == "BREAKOUT":
                    _bo = _score_breakout(df)
                    if _bo and _bo['score'] >= 45:
                        m['_special_score'] = _bo['score']
                        m['_special_signals'] = _bo['signals']
                        m['_special_setup']   = _bo['setup']
                        is_hit = True
                elif scan_type == "MA_TOUCH" and ma_target:
                    # 【V86优化】不同均线使用不同的评分要求和容差
                    # MA30短线：评分>50，容差2%（更严格，只抓真正触碰的）
                    # MA60季线：评分>45，容差3%（中等严格）
                    # MA120半年：评分>40，容差5%（相对宽松）
                    if ma_target == 30:
                        min_score, tolerance = 50, 0.02
                    elif ma_target == 60:
                        min_score, tolerance = 45, 0.03
                    elif ma_target == 120:
                        min_score, tolerance = 40, 0.05
                    else:
                        min_score, tolerance = 45, 0.05
                    
                    if _unified_score > min_score:
                        ma_col = f'MA{ma_target}'
                        if ma_col in m['df'].columns:
                            ma_val = m['df'][ma_col].iloc[-1]
                            last_low = m['last']['Low']
                            last_high = m['last']['High']
                            last_close = m['last']['Close']
                            
                            # 【V86】严格判断：当日K线必须触及均线，或收盘价在容差范围内
                            touched_ma = (last_low <= ma_val <= last_high)  # K线实体触及均线
                            close_to_ma = (abs(last_close - ma_val) / ma_val < tolerance if ma_val > 0 else False)
                            
                            # 【V86】打印调试信息
                            if touched_ma or close_to_ma:
                                distance_pct = abs(last_close - ma_val) / ma_val * 100 if ma_val > 0 else 0
                                _safe_print(f"[MA{ma_target}扫描] ✅ {code} ({name}): 距MA{ma_target}={distance_pct:.2f}%, 评分={m['score']}")
                            
                            if touched_ma or close_to_ma:
                                is_hit = True
                
                if is_hit:
                    # 【V87.12】优化趋势判断 - 结合评分和技术指标
                    score = _unified_score
                    ma200 = m['last'].get('MA200', 0)
                    rsi = m['rsi']
                    
                    # 长期趋势：综合评分 + 年线位置
                    if score >= 75 and ma200 > 0 and m['last_price'] > ma200:
                        long_term = "📈 多头"
                    elif score < 50 or (ma200 > 0 and m['last_price'] < ma200 * 0.9):
                        long_term = "📉 空头"
                    else:
                        long_term = "➡️ 震荡"
                    
                    # 短期趋势：综合评分 + RSI
                    if score >= 75 and rsi > 60:
                        short_term = "📈 强势"
                    elif score >= 75 and rsi > 70:
                        short_term = "🔥 超买"
                    elif score < 50 or rsi < 40:
                        short_term = "📉 弱势"
                    elif rsi < 30:
                        short_term = "❄️ 超卖"
                    else:
                        short_term = "➡️ 中性"
                    
                    # 资金状态（根据成交量）
                    if len(m['df']) >= 5:
                        vol_ma5 = m['df']['Volume'].tail(5).mean()
                        last_vol = m['last']['Volume']
                        if last_vol > vol_ma5 * 1.5:
                            capital = "💰 放量"
                        elif last_vol > vol_ma5:
                            capital = "📊 正常"
                        else:
                            capital = "📉 缩量"
                    else:
                        capital = "➖"
                    
                    # 【V82.10新增】水位 - 显示离最高点和最低点的百分比
                    l250 = m['df']['Low'].tail(250).min() if len(m['df']) >= 250 else m['df']['Low'].min()
                    h250 = m['df']['High'].tail(250).max() if len(m['df']) >= 250 else m['df']['High'].max()
                    if h250 > l250:
                        # 离最高点的百分比（负数表示低于最高点）
                        from_high_pct = (m['last_price'] - h250) / h250 * 100
                        # 离最低点的百分比（正数表示高于最低点）
                        from_low_pct = (m['last_price'] - l250) / l250 * 100
                        water_level = f"高{from_high_pct:+.1f}% 低{from_low_pct:+.1f}%"
                    else:
                        water_level = "➖"
                    
                    _display_score = _unified_score
                    _signals_str   = " ".join(m.get('_special_signals', []))
                    _setup_str     = m.get('_special_setup', m['suggestion'])
                    results.append({
                        "股票": name,
                        "代码": code,
                        "行业": get_sector(code, name),
                        "得分": _display_score,
                        "口径": _decision.get("score_version"),
                        "短/中/长": f"{_decision['short_score']}/{_decision['medium_score']}/{_decision['long_score']}",
                        "上/下估计": f"{_decision['p_up']}%/{_decision['p_down']}%",
                        "盈亏比": f"{_decision['rr']:.2f}",
                        "期望值": f"{_decision['expected_pct']:+.1f}%",
                        "ESG": f"{m.get('esg_total', 0)} ({m.get('esg_grade', 'N/A')})",
                        "长期": long_term,
                        "短期": short_term,
                        "建议": _decision['action'],
                        "策略": _signals_str if scan_type in ("COIL", "BREAKOUT") else m['logic'],
                        "资金": capital,
                        "水位": water_level,
                        "现价": f"{m['last_price']:.2f}"
                    })
                    stats['success'] += 1
        
        except Exception as e:
            # 【V84.3】捕获异常，记录错误但不中断扫描
            stats['failed'] += 1
            error_msg = f"{type(e).__name__}: {str(e)[:80]}"
            stats['errors'].append({
                'code': item[0] if item else 'Unknown',
                'name': item[1] if len(item) > 1 else 'Unknown',
                'error': error_msg
            })
            _safe_print(f"[扫描] ❌ {item[0]} ({item[1] if len(item) > 1 else ''}) 失败: {error_msg}")
    
    _top_n = 30 if scan_type in ("TOP", "COIL", "BREAKOUT") else 100
    sorted_results = sorted(results, key=lambda x:x['得分'], reverse=True)[:_top_n]
    
    # 【V85】扫描结束后,打印失败统计
    _safe_print(f"[扫描] 扫描完成: ✅ 成功 {stats['success']} 只 | ❌ 失败 {stats['failed']} 只")
    if stats['errors']:
        _safe_print(f"[扫描] 失败详情:")
        for err in stats['errors'][:10]:  # 只打印前10个
            _safe_print(f"  ❌ {err['code']} ({err['name']}): {err['error']}")
    
    return sorted_results, stats


# ═══════════════════════════════════════════════════════════════
# 8a1. 【V92 关键修复】扫描行情预热：美股/港股批量下载（避免逐只触发 Yahoo 限流
#       导致只取到字母A段、后面全无数据），A股走 免费行情源 直连。
# ═══════════════════════════════════════════════════════════════
def _batch_prefetch_yf(yf_codes, period='1y', on_each=None) -> int:
    """用 yf.download 批量下载美股/港股，复用单会话/crumb，避免逐只请求触发 429。
    结果按 fetch_stock_data 的缓存键写入 local_cache，供扫描秒级命中。返回成功条数。"""
    if not HAS_YFINANCE or not yf_codes:
        return 0
    targets, seen = [], set()
    for c in yf_codes:
        tc = to_yf_cn_code(c)
        if tc.endswith('.SS') or tc.endswith('.SZ'):
            continue
        y = _normalize_hk_for_yahoo(tc)
        if y in seen:
            continue
        seen.add(y)
        targets.append((tc, y))   # (缓存键代码, 雅虎代码)
    if not targets:
        return 0
    proxy_url = get_proxy_url()
    n_ok = 0
    CH = 25   # 小批次降低 Yahoo 429 限流，避免只取到 A 段
    import time as _t_pf
    for i in range(0, len(targets), CH):
        batch = targets[i:i + CH]
        ylist = [y for _, y in batch]
        cmap = {y: tc for tc, y in batch}
        data = None
        try:
            with ProxyContext(proxy_url):
                data = yf.download(ylist, period=period, group_by='ticker',
                                   auto_adjust=False, threads=True, progress=False)
        except Exception as e:
            logging.warning(f"⚠️ yfinance 批量下载块失败({i//CH}): {str(e)[:80]}")
        for y in ylist:
            sub = None
            try:
                if data is not None and not data.empty:
                    if len(ylist) == 1:
                        sub = data
                    elif isinstance(data.columns, pd.MultiIndex) and y in data.columns.get_level_values(0):
                        sub = data[y]
                if sub is not None:
                    cleaned = clean_df(sub.copy())
                    if cleaned is not None and len(cleaned) > 0:
                        local_cache.set(f"stock_data_{cmap[y]}_False_False", cleaned)
                        n_ok += 1
            except Exception:
                pass
            if on_each:
                on_each(1)
        if i + CH < len(targets):
            _t_pf.sleep(0.25)
    logging.info(f"📦 批量下载完成：{n_ok}/{len(targets)} 只美股/港股已写入缓存")
    return n_ok


def _prefetch_pool(pool, progress_callback=None):
    """扫描前预热行情缓存：美股/港股批量下载，A股 免费行情源 并发逐只。"""
    if not pool:
        return
    cn, ovs = [], []
    for it in pool:
        c = it[2] if len(it) >= 3 else to_yf_cn_code(it[0])
        tc = to_yf_cn_code(c)
        if tc.endswith('.SS') or tc.endswith('.SZ'):
            cn.append(c)
        else:
            ovs.append(c)
    total = max(1, len(cn) + len(ovs))
    cnt = {'n': 0}
    def _bump(k=1, label="⚡预取行情"):
        cnt['n'] += k
        if progress_callback:
            progress_callback(min(cnt['n'], total), total, label)
    try:
        if ovs:
            _batch_prefetch_yf(ovs, on_each=lambda k=1: _bump(k, "⚡批量预取(美/港)"))
    except Exception as e:
        logging.warning(f"⚠️ 美股/港股批量预取异常：{e}")
    try:
        if cn:
            from concurrent.futures import ThreadPoolExecutor as _TPE, as_completed as _ac
            with _TPE(max_workers=min(12, max(2, Config.MAX_WORKERS))) as ex:
                _futs = [ex.submit(fetch_stock_data, c) for c in cn]
                for _ in _ac(_futs):
                    _bump(1, "⚡预取(A股·免费行情源)")
    except Exception as e:
        logging.warning(f"⚠️ A股预取异常：{e}")
    # 补缺：批量 yfinance 未命中的标的，逐只走东财/yfinance（避免只剩 A 段有数据）
    if ovs:
        missed = []
        for c in ovs:
            tc = to_yf_cn_code(c)
            if local_cache.get(f"stock_data_{tc}_False_False") is None:
                missed.append(tc)
        if missed:
            logging.info(f"📦 批量预取后补缺 {len(missed)} 只...")
            try:
                from concurrent.futures import ThreadPoolExecutor as _TPE2, as_completed as _ac2
                with _TPE2(max_workers=min(12, max(2, Config.MAX_WORKERS))) as ex2:
                    _futs2 = [ex2.submit(fetch_stock_data, c) for c in missed]
                    for _ in _ac2(_futs2):
                        _bump(1, "⚡补缺预取")
            except Exception as _me:
                logging.warning(f"⚠️ 补缺预取异常：{_me}")


# ═══════════════════════════════════════════════════════════════
# 8a2. 【V92 一页全策略】单次取数+单次评分，一张表看全所有策略关键信息
# ═══════════════════════════════════════════════════════════════
def _mk_reason(m, score):
    """【V88·推荐理由】一句话说清基本面+技术面优势（全部来自真实计算，无编造）"""
    try:
        _tf = m.get('trend_full') or {}
        _rs = m.get('rs20')
        _rs_txt = (f"RS{_rs:+.0f}领跑" if (_rs or 0) > 3 else ("跑输大盘" if (_rs or 0) < -3 else "RS中性"))
        _vp0 = str(_tf.get('vp', '')).split('·')[0]
        tech = f"技术:{_tf.get('stage', '—')}·{_vp0}·{_rs_txt}" if _tf else f"技术:{_rs_txt}"
        fund = f"基本面:ESG {m.get('esg_grade', '—')}级·综合{score}分"
        return f"{tech}｜{fund}"
    except Exception:
        return ""


def build_action_guidance(score, rs20, pos_pct, touch_count, last_close, trade_plan, regime_str="N/A", trend=None):
    """
    【V94.3】统一操作指引：猎手战位一键筛选与个股搜索共用同一套决策逻辑，
    保证同一只股票在任何入口看到的动作、价位、口径完全一致。
    【V99.9】trend=cloud_engine.analyze_trend_full 结果（可选）：
    五维评分只回答"公司质量/动能强不强"，趋势引擎回答"现在位置/时机好不好"。
    引擎结论为减仓/回避时，指引强制对齐，杜绝"74分高分却让减仓"的表述矛盾——
    高分+高位 = 好票不等于好买点。
    返回 (操作指引文本, 止损/目标文本)
    """
    _bear = str(regime_str).upper().startswith("BEAR")
    _tp = trade_plan
    _rs = rs20
    _rr = _tp.get('risk_reward_ratio', 0) if _tp else 0

    def _fp(v):
        return f"{v:.0f}" if v >= 100 else f"{v:.2f}"

    if score < 55:
        action = "⚪ 回避：评分弱，不参与"
    elif _rs is not None and _rs < -3:
        action = f"⚪ 回避：跑输大盘{abs(_rs):.0f}%，非主线"
    elif _bear and score < 70:
        action = "⚪ 空头市：持币等右侧信号"
    elif pos_pct >= 88 and (_rs is None or _rs < 5):
        action = (f"🟡 高位滞涨别追：回踩 {_fp(_tp['entry_high'])} 下方再考虑" if _tp
                  else "🟡 高位滞涨别追，等回调")
    elif _tp and _rr < 1.0 and score >= 70:
        action = f"🟡 临近阻力 {_fp(_tp['target'])}：突破跟进，不破不追"
    elif score >= 70:
        if _tp and last_close <= _tp['entry_high']:
            action = f"🟢 买入区 {_fp(_tp['entry_low'])}~{_fp(_tp['entry_high'])}：现价可分批"
        elif _tp:
            action = f"🟢 强势：回调 {_fp(_tp['entry_high'])} 附近接"
        else:
            action = "🟢 强势可分批建仓"
    elif touch_count >= 2 and _tp:
        action = f"🟡 双支撑试探：小仓位，破 {_fp(_tp['stop_loss'])} 止损"
    elif score >= 62:
        action = "🔵 持有跟随：不加仓不清仓"
    else:
        action = "⚪ 观望：等评分上70或缩量回踩支撑"

    # 【V99.9】趋势引擎一致性对齐：引擎判减仓/回避时，指引不得再喊买入/持有/跟进
    if trend and isinstance(trend, dict):
        _concl = trend.get('conclusion', '')
        _stage = trend.get('stage', '')
        if _concl == '减仓' and action[:1] in ('🟢', '🔵', '🟡', '⚪'):
            action = f"🟡 {_stage}：持有者冲高减仓，空仓者不追（已时机降分）"
        elif _concl == '回避' and action[:1] in ('🟢', '🔵', '🟡'):
            action = f"⚪ {_stage}：趋势破坏回避（已时机降分）"

    stop_target = (f"损{_fp(_tp['stop_loss'])} → 标{_fp(_tp['target'])}（盈亏比{_rr:.1f}）" if _tp else "—")
    return action, stop_target


def analyze_trend_pulse(df, code=None):
    """
    【V98·MVP】市场热度与趋势判断模块 —— 个股「趋势脉搏」
    确定性综合判断：趋势阶段(7档) + 量价关系 + 明确动作(8种) + 支撑/压力 +
    失效条件 + 原因清单 + 趋势分0-100 + 数据可信度。三端共用(V88/轻量版/问答)。
    """
    try:
        if df is None or len(df) < 30:
            return None
        c = df["Close"].dropna()
        v = df["Volume"].fillna(0)
        hi, lo = df["High"], df["Low"]
        last = float(c.iloc[-1])
        prev = float(c.iloc[-2])

        ma = {n: float(c.rolling(min(n, len(c))).mean().iloc[-1]) for n in (5, 10, 20, 55, 120)}
        ma20_up = float(c.rolling(20).mean().iloc[-1]) > float(c.rolling(20).mean().iloc[-5]) if len(c) >= 25 else True

        # MACD
        dif = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
        dea = dif.ewm(span=9, adjust=False).mean()
        hist = dif - dea
        macd_gold = float(dif.iloc[-1]) > float(dea.iloc[-1])
        hist_rising = len(hist) >= 3 and float(hist.iloc[-1]) > float(hist.iloc[-3])

        # RSI(14)
        delta = c.diff()
        rs_ = delta.clip(lower=0).ewm(com=13).mean() / (-delta.clip(upper=0)).ewm(com=13).mean()
        rsi = float((100 - 100 / (1 + rs_)).iloc[-1])

        # 量能
        v5 = float(v.tail(5).mean())
        v20 = float(v.tail(20).mean()) or 1.0
        volr = v5 / v20                      # 5日/20日量比(持续性)
        volr_d = float(v.iloc[-1]) / v20 if v20 else 1.0   # 当日量比
        chg5 = (last / float(c.iloc[-6]) - 1) * 100 if len(c) >= 6 else 0.0
        chg20 = (last / float(c.iloc[-21]) - 1) * 100 if len(c) >= 21 else 0.0
        bias20 = (last / ma[20] - 1) * 100 if ma[20] else 0.0

        # 位置与关键位
        h60 = float(hi.tail(60).max())
        l20 = float(lo.tail(20).min())
        l250 = float(lo.tail(min(250, len(lo))).min())
        h250 = float(hi.tail(min(250, len(hi))).max())
        pos52 = (last - l250) / (h250 - l250) * 100 if h250 > l250 else 50.0
        new_high_60 = float(hi.iloc[-1]) >= h60 * 0.995
        support = max(ma[20], l20) if last > ma[20] else max(ma[55], l20)
        resistance = h60 if last < h60 * 0.99 else h250

        reasons = []

        # ── 量价关系判定 ─────────────────────────────
        if chg5 > 1.5 and volr >= 1.1:
            vp, vp_good = "📈 放量上涨·量价健康", 2
            reasons.append(f"5日+{chg5:.1f}%且量比{volr:.2f}放大，资金持续进场")
        elif chg5 > 1.5 and volr < 0.85:
            vp, vp_good = "⚠️ 缩量上涨·上攻乏力", 1
            reasons.append(f"上涨但量比仅{volr:.2f}，追高动能存疑")
        elif chg5 < -1.5 and volr < 0.9:
            vp, vp_good = "🔄 缩量回调·抛压有限", 1
            reasons.append(f"回调{chg5:.1f}%但缩量({volr:.2f})，属正常回踩概率大")
        elif chg5 < -1.5 and volr >= 1.2:
            vp, vp_good = "🚨 放量下跌·出货嫌疑", 0
            reasons.append(f"下跌{chg5:.1f}%且放量({volr:.2f})，主动抛压明显")
        elif volr >= 1.5 and abs(chg5) < 1.5:
            vp, vp_good = "⚠️ 放量滞涨·分歧加大", 0
            reasons.append(f"量比{volr:.2f}大幅放大但价格滞涨，多空分歧加剧")
        else:
            vp, vp_good = "➖ 量价中性", 1

        # ── 趋势阶段判定(7档,优先级从坏到好) ──────────
        if last < ma[20] < ma[55] and chg5 < 0 and (vp_good == 0 or last < ma[120]):
            stage = "🔴 破位下跌"
            reasons.append(f"价({last:.2f})<MA20({ma[20]:.2f})<MA55({ma[55]:.2f})，均线空头")
        elif last < ma[20] and (not ma20_up or not macd_gold):
            stage = "🟠 趋势转弱"
            reasons.append(f"跌破MA20({ma[20]:.2f})" + ("且MACD死叉" if not macd_gold else "且MA20走平向下"))
        elif volr >= 1.5 and abs(chg5) < 1.5 and pos52 > 70:
            stage = "🟡 放量滞涨"
        elif pos52 > 80 and abs(chg5) < 3 and not new_high_60:
            stage = "🟡 高位震荡"
            reasons.append(f"52周高位({pos52:.0f}%)横盘，未创新高")
        elif last > ma[5] > ma[20] and new_high_60 and macd_gold:
            stage = "🚀 主升阶段"
            reasons.append(f"多头排列+创60日新高+MACD金叉")
        elif last > ma[20] > ma[55] and macd_gold:
            stage = "🟢 趋势确认"
            reasons.append(f"站稳MA20/MA55多头排列，MACD金叉")
        elif last > ma[20] and pos52 < 45 and volr > 1.05:
            stage = "🌱 底部启动"
            reasons.append(f"低位({pos52:.0f}%)放量站上MA20，疑似启动")
        else:
            stage = "➖ 震荡整理"

        # ── 明确动作(8种) ────────────────────────────
        if stage == "🔴 破位下跌":
            action = "🛑 趋势破坏，剔除/离场"
            invalid = f"重新站上MA20({ma[20]:.2f})且缩量企稳3日，才可重新评估"
        elif stage == "🟠 趋势转弱":
            action = "🛑 持有者跌破止损离场；空仓者回避"
            invalid = f"收复MA20({ma[20]:.2f})并放量收阳"
        elif stage == "🟡 放量滞涨":
            action = "📉 冲高减仓（先落袋一部分）"
            invalid = f"缩量整理后再放量突破{resistance:.2f}"
        elif stage == "🟡 高位震荡":
            action = "✋ 不追高；持有者可持有但设好止损"
            invalid = f"跌破MA20({ma[20]:.2f})即减仓"
        elif stage == "🚀 主升阶段":
            if bias20 > 8 or rsi > 75:
                action = f"✋ 短线过热(乖离{bias20:+.1f}%/RSI{rsi:.0f})·不追高，等回踩MA10({ma[10]:.2f})"
                reasons.append(f"乖离率{bias20:+.1f}%、RSI{rsi:.0f}，短线透支")
            else:
                action = f"🟢 继续持有；新买回踩MA10({ma[10]:.2f})附近分批"
            invalid = f"收盘跌破MA20({ma[20]:.2f})且放量，主升结束"
        elif stage == "🟢 趋势确认":
            action = (f"🟢 可以买：{ma[20]:.2f}~{last:.2f}区间分批" if vp_good >= 1
                      else f"⏳ 等待回踩MA20({ma[20]:.2f})企稳再买")
            invalid = f"收盘跌破 {ma[55]:.2f}，趋势失效"   # 价格化定纲:主文案不提均线名
        elif stage == "🌱 底部启动":
            action = f"🧪 只能试仓(≤半仓位)，止损{l20:.2f}"
            invalid = f"跌回启动前低点{l20:.2f}，启动失败"
        else:
            action = f"⏳ 观望/等待：站稳MA20({ma[20]:.2f})+放量再介入"
            invalid = "—"

        # ── 趋势分 0-100 ─────────────────────────────
        align = sum([last > ma[5], ma[5] > ma[20], ma[20] > ma[55], last > ma[120]])
        score = (align * 7.5                                  # 均线排列 30
                 + (10 if macd_gold else 0) + (10 if hist_rising else 0)   # MACD 20
                 + vp_good * 10                               # 量价 20
                 + max(0, min(15, 7.5 + chg20 * 0.75))        # 动量 15
                 + (15 if (45 <= rsi <= 70 and abs(bias20) < 8) else (7 if rsi < 80 else 0)))  # 健康度 15
        score = int(max(0, min(100, score)))

        # ── 数据可信度 ───────────────────────────────
        try:
            from datetime import datetime as _dt, timezone as _tz, timedelta as _td
            _bj_today = _dt.now(_tz(_td(hours=8))).date()
            _fresh = pd.Timestamp(df.index[-1]).date() >= _bj_today - _td(days=3)
        except Exception:
            _fresh = True
        conf = "已核验(含当日)" if _fresh and len(c) >= 120 else ("单源待核验" if len(c) >= 60 else "数据偏短·参考")

        return {
            "stage": stage, "vp": vp, "action": action, "score": score,
            "support": round(support, 2), "resistance": round(resistance, 2),
            "invalid": invalid, "reasons": reasons[:4], "confidence": conf,
            "rsi": round(rsi, 0), "bias20": round(bias20, 1), "volr": round(volr, 2),
            "chg5": round(chg5, 1), "chg20": round(chg20, 1),
            "ma": {k: round(v_, 2) for k, v_ in ma.items()},
            "macd_gold": macd_gold, "pos52": round(pos52, 0),
        }
    except Exception as _e:
        logging.debug(f"trend_pulse失败 {code}: {_e}")
        return None


def render_trend_pulse_md(tp: dict, name: str = "") -> str:
    """趋势脉搏 → Markdown（V88/轻量版/AI问答共用同一份文案）"""
    if not tp:
        return ""
    L = [f"**{name} 趋势脉搏** ｜ 趋势分 **{tp['score']}/100** ｜ {tp['stage']} ｜ {tp['vp']}",
         f"**动作：{tp['action']}**",
         f"支撑 {tp['support']} ｜ 压力 {tp['resistance']} ｜ RSI {tp['rsi']:.0f} ｜ 乖离20 {tp['bias20']:+.1f}% ｜ 量比 {tp['volr']}",
         f"失效条件：{tp['invalid']}",
         "依据：" + "；".join(tp["reasons"]) if tp.get("reasons") else "",
         f"_数据可信度：{tp['confidence']}_"]
    return "\n\n".join(x for x in L if x)


def run_unified_scan(pool, scan_market, risk_preference="平衡", use_concurrent=True, progress_callback=None):
    """
    一页全策略扫描：每只股票只取一次数据、算一次指标，同时得出它在
    MA30短线 / MA60季线 / MA120半年线 / 综合评分 / 多重支撑 / 市场状态动作
    上的关键信息，汇成一张可点击表格。比逐个按钮点 6 次快 ~6 倍。

    返回: (rows:list[dict], stats:dict, meta:dict)
    """
    from modules.sector_map import get_sector

    # ── 市场状态（整盘判一次）──────────────────────────────────────
    regime_str, regime_conf = "N/A", 0.0
    try:
        if REGIME_ENGINE_AVAILABLE:
            index_code = "^GSPC" if scan_market == "美股" else ("^HSI" if scan_market == "港股" else "000001.SS")
            _idx_df = fetch_stock_data(index_code)
            _vix_df = fetch_stock_data("^VIX") if scan_market == "美股" else None
            _vix = float(_vix_df["Close"].iloc[-1]) if (_vix_df is not None and len(_vix_df) > 0) else 20.0
            _ri = MarketRegime(vix_proxy=_vix).evaluate(_idx_df, 0, 1)
            regime_str, regime_conf = _ri.get("regime", "N/A"), _ri.get("confidence", 0.0)
    except Exception as _e:
        logging.debug(f"unified regime 计算失败: {_e}")

    total = len(pool)

    # ── 预热缓存：美股/港股批量下载（避免限流只取到A段），A股 免费行情源 ──
    if total > 1:
        _prefetch_pool(pool, progress_callback)
    # 三大指数先取一次入缓存（个股RS都要用，避免并发时挤在第一只上）
    for _ic in ("^GSPC", "^HSI", "000001.SS"):
        try:
            fetch_stock_data(_ic)
        except Exception:
            pass

    rows = []
    stats = {'success': 0, 'failed': 0, 'errors': []}

    def _eval_one(item):
        """单只评估：返回 ('ok', row) / ('skip', None) / ('err', 错误dict)。
        行情已预取入缓存，这里主要是指标计算，线程安全（纯pandas逐只独立）。"""
        try:
            code = item[0]
            name = item[1] if len(item) > 1 else code
            c_fixed = item[2] if len(item) >= 3 else to_yf_cn_code(code)

            df = fetch_stock_data(c_fixed)
            if df is None or df.empty or len(df) < 20:
                return ('err', None)

            m = calculate_metrics_all(df, c_fixed)
            if not m:
                return ('skip', None)

            from v88_decision_core import evaluate_decision as _evaluate_scan_decision
            _decision = _evaluate_scan_decision(
                df, m.get('trend_full') or {}, name=name, code=code)
            if _decision.get("error") or _decision.get("unified_score", 0) <= 35:
                return ('skip', None)

            mdf = m['df']
            last_close = float(m['last']['Close'])
            last_low = float(m['last']['Low'])
            last_high = float(m['last']['High'])

            # 各均线触及情况（一次算全）。注意 calculate_metrics_all 不算 MA30，
            # 故这里直接用 Close 现算 MA30/MA60/MA120，保证三列都有值。
            _close = mdf['Close']
            ma_disp = {}
            touch_count = 0
            touch_list = []
            for ma_n in (30, 60, 120):
                col = f'MA{ma_n}'
                ma_val = 0.0
                if col in mdf.columns:
                    try:
                        ma_val = float(mdf[col].iloc[-1])
                    except Exception:
                        ma_val = 0.0
                if not ma_val or ma_val <= 0:
                    _win = ma_n if len(_close) >= ma_n else len(_close)
                    try:
                        ma_val = float(_close.rolling(_win).mean().iloc[-1])
                    except Exception:
                        ma_val = 0.0
                if ma_val and ma_val > 0:
                    dist = (last_close - ma_val) / ma_val * 100
                    touched = (last_low <= ma_val <= last_high) or abs(dist) < 8
                    if touched:
                        touch_count += 1
                        touch_list.append(f"MA{ma_n}")
                        ma_disp[ma_n] = "✅触及"
                    else:
                        ma_disp[ma_n] = f"{dist:+.1f}%"
                else:
                    ma_disp[ma_n] = "—"

            # 52周(250日)高低点水位：pos_pct 0~100（0=贴52周低,100=贴52周高）；
            # pos52 居中化为 -100~+100（正=偏高/近高点，负=偏低/近低点，0=中位）
            try:
                l250 = float(mdf['Low'].tail(250).min())
                h250 = float(mdf['High'].tail(250).max())
                pos_pct = (last_close - l250) / (h250 - l250) * 100 if h250 > l250 else 50.0
            except Exception:
                pos_pct = 50.0
            pos52 = max(-100, min(100, round((pos_pct - 50) * 2)))

            score = int(_decision['unified_score'])
            _bear = str(regime_str).upper().startswith("BEAR")

            # 【V94.3】操作指引与止损/目标：统一决策函数（与个股搜索共用，口径一致）
            # 【V88·时机闸门】趋势结论复用评分内核已算好的（评分本身已做时机压分）
            action = _decision.get("action", "观察")
            stop_target = (f"防守{_decision.get('stop') or '—'} → 压力"
                           f"{_decision.get('resistance') or '—'}（盈亏比{_decision.get('rr', 0):.2f}）")

            # 【V99.6】MACD/量价列：量能变化必须明示方向与幅度，不写模糊的"增长"。
            # 阈值：5日均量较20日均量 ≥+20% 明显放量 / +8%~+20% 温和放量 /
            # ±8% 持平 / -8%~-20% 温和缩量 / ≤-20% 明显缩量
            try:
                _e12 = _close.ewm(span=12, adjust=False).mean()
                _e26 = _close.ewm(span=26, adjust=False).mean()
                _dif = _e12 - _e26
                _dea = _dif.ewm(span=9, adjust=False).mean()
                _hst = _dif - _dea
                _gold = float(_dif.iloc[-1]) > float(_dea.iloc[-1])
                _red = float(_hst.iloc[-1]) > 0
                _hexp = len(_hst) >= 3 and abs(float(_hst.iloc[-1])) > abs(float(_hst.iloc[-3]))
                _macd_txt = (("金叉" if _gold else "死叉")
                             + ("·红柱扩大" if (_red and _hexp) else ("·红柱缩小" if _red else
                                ("·绿柱扩大" if _hexp else "·绿柱缩小"))))
                _v20m = float(mdf['Volume'].tail(20).mean()) or 1.0
                _vpct = (float(mdf['Volume'].tail(5).mean()) / _v20m - 1) * 100
                _vol_txt = (("🔺明显放量" if _vpct >= 20 else
                             "↗温和放量" if _vpct >= 8 else
                             "🔻明显缩量" if _vpct <= -20 else
                             "↘温和缩量" if _vpct <= -8 else
                             "→量能持平") + f"{_vpct:+.0f}%")
                macd_vp = f"{_macd_txt}｜{_vol_txt}"
            except Exception:
                macd_vp = "—"

            row = {
                "代码": code,
                "名称": name,
                "行业": get_sector(code, name),
                "现价": f"{m['last_price']:.2f}",
                "得分": score,
                "口径": _decision.get("score_version"),
                "短/中/长": (f"{_decision.get('short_score')}/"
                             f"{_decision.get('medium_score')}/{_decision.get('long_score')}"),
                "上/下估计": f"{_decision.get('p_up')}%/{_decision.get('p_down')}%",
                "盈亏比": f"{_decision.get('rr', 0):.2f}",
                "期望值": f"{_decision.get('expected_pct', 0):+.1f}%",
                "RSI": int(m.get('rsi', 0) or 0),
                "20日动量": f"{m.get('chg20d', 0) or 0:+.1f}%",
                "RS强度": (f"{m['rs20']:+.1f}" if m.get('rs20') is not None else "—"),
                "52周位置": f"{pos52:+d}",
                "MA30短线": ma_disp[30],
                "MA60季线": ma_disp[60],
                "MA120半年": ma_disp[120],
                "多重支撑": (f"✅×{touch_count}" if touch_count >= 2 else ""),
                "MACD/量价": macd_vp,
                "拐点": ((m.get('trend_full') or {}).get('turning') or {}).get('brief', ''),
                # 【V88·当日买入区间】推荐可买(≥62分且指引非回避)才给区间，其余留空
                "买入区间": (f"{m['trade_plan']['entry_low']:.2f}~{m['trade_plan']['entry_high']:.2f}"
                          if m.get('trade_plan') and score >= 62 and action[:1] in ('🟢', '🔵', '🟡') else ""),
                "推荐理由": _decision.get("entry_note", "等待确认"),
                "操作指引": action,
                "止损/目标": stop_target,
            }
            return ('ok', row)
        except Exception as e:
            return ('err', {
                'code': item[0] if item else 'Unknown',
                'name': item[1] if len(item) > 1 else 'Unknown',
                'error': f"{type(e).__name__}: {str(e)[:60]}",
            })

    # 【V99.6 提速】行情预取完后，指标计算也并发跑（此前 use_concurrent 只管预取，
    # 565只逐只串行算指标是主要耗时）。进度回调始终在主线程调用，Streamlit 安全。
    if use_concurrent and total > 20:
        from concurrent.futures import ThreadPoolExecutor as _UTPE, as_completed as _uac
        with _UTPE(max_workers=min(12, max(4, Config.MAX_WORKERS))) as _uex:
            _futs = {_uex.submit(_eval_one, it): it for it in pool}
            _done = 0
            for _fu in _uac(_futs):
                _done += 1
                _it = _futs[_fu]
                if progress_callback:
                    progress_callback(_done, total, _it[1] if len(_it) > 1 else _it[0])
                try:
                    _st_, _payload = _fu.result()
                except Exception:
                    _st_, _payload = ('err', None)
                if _st_ == 'ok':
                    rows.append(_payload)
                    stats['success'] += 1
                elif _st_ == 'err':
                    stats['failed'] += 1
                    if isinstance(_payload, dict):
                        stats['errors'].append(_payload)
    else:
        for idx, item in enumerate(pool):
            if progress_callback:
                progress_callback(idx + 1, total, item[1] if len(item) > 1 else item[0])
            _st_, _payload = _eval_one(item)
            if _st_ == 'ok':
                rows.append(_payload)
                stats['success'] += 1
            elif _st_ == 'err':
                stats['failed'] += 1
                if isinstance(_payload, dict):
                    stats['errors'].append(_payload)

    # 去重：先按得分降序，再按「代码」+「公司名(去A/B/C/H股别后缀)」只保留最高分一条，
    # 避免同一只股票（池内重复录入）或同公司多股别（谷歌A/谷歌C）重复占榜。
    import re as _re

    # 【V94.1】同分时按 RS强度 二次排序，领涨股排前
    def _rs_key(r):
        try:
            return float(str(r.get('RS强度', '')).replace('—', 'x'))
        except Exception:
            return -999.0

    rows.sort(key=lambda x: (x['得分'], _rs_key(x)), reverse=True)
    _seen_code, _seen_name, _dedup = set(), set(), []
    for r in rows:
        code_k = str(r.get('代码', '')).strip().upper()
        name_k = _re.sub(r'[ABCH类]+$', '', str(r.get('名称', '')).strip())
        if (code_k and code_k in _seen_code) or (name_k and name_k in _seen_name):
            continue
        if code_k:
            _seen_code.add(code_k)
        if name_k:
            _seen_name.add(name_k)
        _dedup.append(r)
    # 截断 80→150：日报精选池推荐的标的（如 MU）分数可能在 60-63 档，
    # 80 条截断会让"日报有推荐、全选搜不到"——放宽保证两端口径互相可见
    rows = _dedup[:150]
    return rows, stats, {'regime': regime_str, 'confidence': regime_conf}


# ═══════════════════════════════════════════════════════════════
# 8a3.【V96】三期限选股：短/中/长线各 Top10（中美港混排，确定性打分）
# ═══════════════════════════════════════════════════════════════
def run_horizon_top10(progress_callback=None):
    """
    短线(1-5日)=动能与RS主导 | 中线(1-3月)=综合评分+趋势排列 | 长线(6月+)=质量+年线+低波动
    全部由五维引擎指标确定性计算，无LLM参与，结果可复算。
    返回 {"short":[...], "mid":[...], "long":[...]}，各≤10条（同公司去重）。
    """
    pool = []
    for src, mkt in ((RAW_US[:150], "美股"), (RAW_HK[:100], "港股"), (RAW_CN_TOP[:150], "A股")):
        for it in src:
            pool.append((it, mkt))
    _prefetch_pool([p[0] for p in pool], progress_callback)

    rows = []
    total = len(pool)
    for i, (it, mkt) in enumerate(pool):
        if progress_callback:
            progress_callback(i + 1, total, it[1] if len(it) > 1 else it[0])
        try:
            code = it[0]
            name = it[1] if len(it) > 1 else code
            cfix = it[2] if len(it) >= 3 else to_yf_cn_code(code)
            df = fetch_stock_data(cfix)
            if df is None or len(df) < 60:
                continue
            m = calculate_metrics_all(df, cfix)
            if not m:
                continue
            mdf = m['df']
            last = float(m['last_price'])
            score = int(m['score'])
            rs = m.get('rs20')

            # 三期限榜也只读唯一决策底稿，不再使用另一套 horizon_scores。
            from v88_decision_core import evaluate_decision as _evaluate_top_decision
            _dc101 = _evaluate_top_decision(mdf, m.get('trend_full') or {}, name=name, code=code)
            if _dc101.get("error"):
                continue

            l250 = float(mdf['Low'].tail(250).min())
            h250 = float(mdf['High'].tail(250).max())
            pos = (last - l250) / (h250 - l250) * 100 if h250 > l250 else 50.0
            score = int(_dc101["unified_score"])
            act = _dc101["action"]
            stp = (f"防守{_dc101.get('stop') or '—'}→压力"
                   f"{_dc101.get('resistance') or '—'}")

            rows.append({
                "市场": mkt, "代码": code, "名称": name,
                "现价": f"{last:.2f}", "综合分": score,
                "RS强度": (f"{rs:+.1f}" if rs is not None else "—"),
                "20日动量": f"{m.get('chg20d', 0) or 0:+.1f}%",
                "操作指引": act, "止损/目标": stp,
                "口径": _dc101.get("score_version"),
                "上/下估计": f"{_dc101['p_up']}%/{_dc101['p_down']}%",
                "盈亏比": f"{_dc101['rr']:.2f}",
                "期望值": f"{_dc101['expected_pct']:+.1f}%",
                "_s": _dc101['short_score'], "_m": _dc101['medium_score'], "_l": _dc101['long_score'],
                "_why_s": f"2周方向分{_dc101['short_score']}｜{_dc101['entry_note']}",
                "_why_m": f"4/6/8周均分{_dc101['medium_score']}｜{_dc101['entry_note']}",
                "_why_l": f"16周方向分{_dc101['long_score']}｜{_dc101['entry_note']}",
                "_plan_s": _dc101['action'],
                "_plan_m": _dc101['action'],
                "_plan_l": _dc101['action'],
            })
        except Exception:
            continue

    import re as _re2
    def _top(key, per_market=10):
        """每市场各取 per_market 只（保证中美港都有），合并后按期限分降序 → Top30"""
        seen, out = set(), []
        for _mkt in ("美股", "港股", "A股"):
            cnt = 0
            for r in sorted([x for x in rows if x["市场"] == _mkt], key=lambda x: -x[key]):
                nk = _re2.sub(r'[ABCH类]+$', '', str(r["名称"]).strip())
                if nk in seen:
                    continue
                seen.add(nk)
                row = {k: v for k, v in r.items() if not k.startswith("_")}
                row["期限分"] = int(r[key])
                # 【V88·三期限引擎】逻辑链全透明：该期限每个因子拿了多少分、依据是什么
                row["操作剧本"] = r.get("_plan" + key, "")
                row["入选逻辑"] = r.get("_why" + key, "")
                out.append(row)
                cnt += 1
                if cnt >= per_market:
                    break
        out.sort(key=lambda r: -r["期限分"])
        for _i, r in enumerate(out):
            r["排名"] = _i + 1
        return out

    return {"short": _top("_s"), "mid": _top("_m"), "long": _top("_l")}


# ═══════════════════════════════════════════════════════════════
# 8b. 【Regime-Adaptive】市场状态自适应扫描
# ═══════════════════════════════════════════════════════════════
def run_regime_scan(pool, use_concurrent, scan_market, risk_preference="平衡", progress_callback=None):
    """
    市场状态自适应筛选：先判 regime，再策略分流，再给动作建议
    返回增强结果：含 动作标签、机会概率、风险概率、建议仓位、失效条件
    progress_callback(current, total, stock_name)：必须有进度百分比
    """
    if not REGIME_ENGINE_AVAILABLE:
        # 降级：使用旧综合评分（带进度）
        res, stats = batch_scan_analysis(pool, scan_type="TOP", ma_target=None, progress_callback=progress_callback)
        return res, stats, None, {"regime": "N/A", "fallback": True}

    results = []
    stats = {'success': 0, 'failed': 0, 'errors': []}
    breadth_above = 0
    breadth_total = 0

    # 1. 获取指数数据，计算 regime
    index_code = "^GSPC" if scan_market == "美股" else ("^HSI" if scan_market == "港股" else "000001.SS")
    index_df = fetch_stock_data(index_code)
    vix_df = fetch_stock_data("^VIX") if scan_market == "美股" else None
    vix_proxy = 20.0
    if vix_df is not None and len(vix_df) > 0:
        vix_proxy = float(vix_df["Close"].iloc[-1])

    mr = MarketRegime(vix_proxy=vix_proxy)
    regime_info = mr.evaluate(index_df, 0, 1)
    regime = regime_info["regime"]

    # 2. get_sector（【V91.7】与 batch_scan_analysis 统一使用 sector_map，全682只覆盖，避免❓其他）
    from modules.sector_map import get_sector

    router = StrategyRouter()
    classifier = OpportunityClassifier()
    risk_fc = RiskForecaster()
    action_eng = ActionEngine()
    quality_guard = QualityGuard()
    composer = ReportComposer()
    gap_engine = ExpectationGapEngine() if (USE_POTENTIAL_ENGINE and ExpectationGapEngine) else None
    long_compound_gate = LongCompounderGate() if LongCompounderGate else None
    margin_gate = MarginOfSafetyGate() if MarginOfSafetyGate else None

    total = len(pool)

    # 【V92 修复】扫描前预热行情：美股/港股批量下载(避免限流只取到A段)，A股 免费行情源
    if total > 1:
        _prefetch_pool(pool, progress_callback)

    for idx, item in enumerate(pool):
        # 进度回调：必须有百分比
        if progress_callback:
            progress_callback(idx + 1, total, item[1] if len(item) > 1 else item[0])
        try:
            code = item[0]
            name = item[1]
            c_fixed = item[2] if len(item) >= 3 else to_yf_cn_code(code)

            df = fetch_stock_data(c_fixed)
            if df is None or df.empty or len(df) < 20:
                stats['failed'] += 1
                continue

            m = calculate_metrics_all(df, c_fixed)
            score_threshold = 35 if (USE_POTENTIAL_ENGINE and gap_engine) else 40
            if not m:
                continue
            from v88_decision_core import evaluate_decision as _evaluate_regime_decision
            _decision = _evaluate_regime_decision(
                df, m.get('trend_full') or {}, name=name, code=code)
            if _decision.get("error") or _decision.get("unified_score", 0) <= score_threshold:
                continue

            last = m['last']
            last_price = m['last_price']

            #  breadth 统计
            above_ma20 = last_price > last.get('MA20', 0)
            if above_ma20:
                breadth_above += 1
            breadth_total += 1

            # 水位（统一计算）
            if REGIME_ENGINE_AVAILABLE:
                pos_level, pos_pct = get_position_level_unified(m['df'], last_price)
            else:
                l250 = m['df']['Low'].tail(250).min() if len(m['df']) >= 250 else m['df']['Low'].min()
                h250 = m['df']['High'].tail(250).max() if len(m['df']) >= 250 else m['df']['High'].max()
                pos_pct = (last_price - l250) / (h250 - l250) * 100 if h250 > l250 else 50
                pos_level = "高" if pos_pct >= 75 else ("中" if pos_pct >= 35 else "低")

            # QualityGuard
            qr = quality_guard.validate(
                industry=get_sector(code, name),
                score_total=m['score'],
                position_level=pos_level,
                position_percentile=pos_pct,
            )
            # 旧质量门只作为辅助告警，不得再让同一只股票在不同模块被无声剔除。

            # feature_vector
            fv = {
                "score": m['score'],
                "rsi": m['rsi'],
                "above_ma20": last_price > last.get('MA20', 0),
                "above_ma60": last_price > last.get('MA60', 0),
                "above_ma120": last_price > last.get('MA120', 0),
                "vol_ratio": last['Volume'] / m['df']['Volume'].tail(20).mean() if len(m['df']) >= 20 else 1,
                "drawdown_20d": 1 - last_price / m['df']['High'].tail(20).max() if len(m['df']) >= 20 else 0,
                "momentum_5d": (last_price - m['df']['Close'].iloc[-6]) / m['df']['Close'].iloc[-6] if len(m['df']) >= 6 else 0,
            }

            # StrategyRouter（质量引擎）
            route_res = router.route(regime, regime_info["confidence"], fv)
            quality_score = route_res["regime_adjusted_score"]

            # ExpectationGapEngine（潜力引擎，双引擎模式）
            gap_result = None
            sector_raw = get_sector(code, name)
            if gap_engine:
                gap_result = gap_engine.compute(m['df'], c_fixed, sector_raw)

            # 双引擎融合 / 单引擎
            if gap_engine and gap_result:
                dual_res = router.route_dual_engine(regime, quality_score, gap_result["potential_score"])
                final_score = dual_res["final_score"]
            else:
                final_score = quality_score

            # 【长线法宝】LongCompounderGate + MarginOfSafetyGate
            long_compound_result = long_compound_gate.compute(m['df'], c_fixed, sector_raw) if long_compound_gate else {}
            margin_result = margin_gate.compute(m['df'], gap_result, long_compound_result) if margin_gate else {}
            allows_long_core = margin_result.get("allows_long_core", True)

            # 价值陷阱硬过滤
            vt_check = {}
            if margin_gate:
                vt_check = MarginOfSafetyGate.check_value_trap(sector_raw, m.get('logic', ''), m.get('suggestion', ''))
            # 价值陷阱保留为风险证据，由唯一动作引擎降级；不在本模块另行删除标的。

            # OpportunityClassifier（按质量分做动作分类，LONG_CORE 需 allows_long_core）
            cl_res = classifier.classify(regime, int(quality_score), fv, qr["pass"], allows_long_core=allows_long_core)
            action_label = cl_res["action_label"]
            action_emoji = cl_res["action_emoji"]

            # RiskForecaster
            risk_probs = risk_fc.forecast(m['df'], last, regime)

            # ActionEngine
            df_tr = m['df']
            df_tr['TR'] = np.maximum((df_tr['High'] - df_tr['Low']), 
                np.maximum(abs(df_tr['High'] - df_tr['Close'].shift(1)), abs(df_tr['Low'] - df_tr['Close'].shift(1))))
            atr = float(df_tr['TR'].rolling(14).mean().iloc[-1]) if len(df_tr) >= 14 else 0
            action_res = action_eng.compute(
                action_label, risk_probs, risk_preference,
                atr, last_price
            )

            # 资金状态
            vol_ma5 = m['df']['Volume'].tail(5).mean() if len(m['df']) >= 5 else last['Volume']
            if last['Volume'] > vol_ma5 * 1.5:
                capital = "💰 放量"
            elif last['Volume'] > vol_ma5:
                capital = "📊 正常"
            else:
                capital = "📉 缩量"

            # 长期/短期
            score = _decision['unified_score']
            ma200 = last.get('MA200', 0)
            if score >= 75 and ma200 > 0 and last_price > ma200:
                long_term = "📈 多头"
            elif score < 50 or (ma200 > 0 and last_price < ma200 * 0.9):
                long_term = "📉 空头"
            else:
                long_term = "➡️ 震荡"

            if score >= 75 and m['rsi'] > 60:
                short_term = "📈 强势"
            elif score < 50 or m['rsi'] < 40:
                short_term = "📉 弱势"
            else:
                short_term = "➡️ 中性"

            # 个股概况硬事实：行业/得分/水位（【V91.0】不再对行业未匹配刷屏WARN，仅显示行业名）
            ff = qr.get("field_flags", {})
            sector_display = sector_raw  # 直接显示行业，不叠加 [WARN]
            water_str = f"{pos_level}-{pos_pct:.1f}%"
            if ff.get("position") == "WARN":
                water_str = f"{water_str} [WARN]"
            qa_parts = []
            # 仅对得分/水位异常标注，行业未匹配不再刷屏
            if ff.get("industry") == "FAIL":
                qa_parts.append("行业缺失")
            if ff.get("score") == "WARN":
                qa_parts.append("得分待核")
            if ff.get("score") == "FAIL":
                qa_parts.append("得分异常")
            if ff.get("position") == "WARN":
                qa_parts.append("水位待核")
            qa_label = " | ".join(qa_parts) if qa_parts else "OK"

            # 三池分类：A=已验证强势 B=预期差潜力 C=左侧观察
            passes_potential = gap_result and gap_result.get("passes_potential_gate", False)
            pot_score = gap_result.get("potential_score", 0) if gap_result else 0
            if passes_potential and action_label in (classifier.BUILD_NOW, classifier.FOLLOW_MID, classifier.LONG_CORE):
                pool_assignment = "B"
            elif passes_potential and action_label == classifier.FILTERED:
                pool_assignment = "C"
            else:
                pool_assignment = "A"

            # 预期差等级 A/B/C
            potential_gap_grade = "A" if pot_score >= 70 else ("B" if pot_score >= 50 else "C")
            potential_tags = (gap_result.get("potential_tags", []) or [])[:3] if gap_result else []

            # 【长线法宝】8项强制解释 + 持有期/仓位上限绑定
            eight_mandatory = {}
            if long_compound_gate and margin_gate and gap_result:
                eight_mandatory = composer.compose_eight_mandatory(
                    gap_result, long_compound_result, margin_result, action_res,
                    name, sector_raw, action_label
                )

            results.append({
                "股票": name, "代码": code, "行业": sector_display,
                "得分": _decision['unified_score'],
                "口径": _decision['score_version'],
                "短/中/长": f"{_decision['short_score']}/{_decision['medium_score']}/{_decision['long_score']}",
                "盈亏比": f"{_decision['rr']:.2f}", "期望值": f"{_decision['expected_pct']:+.1f}%",
                "ESG": f"{m.get('esg_total', 0)} ({m.get('esg_grade', 'N/A')})",
                "硬事实校验": qa_label,
                "长期": long_term, "短期": short_term, "建议": m['suggestion'],
                "策略": m['logic'], "资金": capital, "水位": water_str,
                "现价": f"{last_price:.2f}",
                "动作标签": _decision['action'],
                "机会概率": f"{_decision['p_up']}%",
                "风险概率": f"{_decision['p_down']}%",
                "建议仓位": action_res["suggested_position_range"],
                "分批节奏": action_res["tranche_plan"],
                "持有期": action_res.get("holding_period", "N/A"),
                "仓位上限": f"{action_res.get('position_cap_percent', 0)}%",
                "失效条件": action_res["invalidation_rules"][0] if action_res["invalidation_rules"] else "N/A",
                "regime_adjusted_score": final_score,
                "pool_assignment": pool_assignment,
                "potential_gap_grade": potential_gap_grade,
                "potential_tags": potential_tags,
                "potential_score": pot_score,
                "quality_score": quality_score,
                "long_compounder_score": long_compound_result.get("long_compounder_score", 0),
                "expectation_gap_score": gap_result.get("expectation_gap_score", pot_score) if gap_result else pot_score,
                "eight_mandatory": eight_mandatory,
                "potential_four_sentences": composer.compose_potential_four_sentences(
                    gap_result, name, sector_raw, action_label
                ) if (gap_result and passes_potential) else [],
                "battle_room": composer.compose_battle_room(
                    regime_info, risk_probs, action_label, action_emoji,
                    action_res, qr["data_quality_flag"]
                ),
            })
            stats['success'] += 1
        except Exception as e:
            stats['failed'] += 1
            stats['errors'].append({'code': item[0], 'name': item[1] if len(item) > 1 else '', 'error': str(e)[:80]})

    regime_info["breadth_above"] = breadth_above
    regime_info["breadth_total"] = breadth_total
    regime_info = mr.evaluate(index_df, breadth_above, max(1, breadth_total))

    top_n = 50 if (USE_POTENTIAL_ENGINE and ExpectationGapEngine) else 30
    sorted_results = sorted(results, key=lambda x: x['得分'], reverse=True)[:top_n]
    from zoneinfo import ZoneInfo
    ts_str = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S") + " CST"
    return sorted_results, stats, regime_info, {
        "regime": regime,
        "confidence": regime_info["confidence"],
        "scan_timestamp": ts_str,
        "use_potential_engine": USE_POTENTIAL_ENGINE and bool(ExpectationGapEngine),
    }


# ═══════════════════════════════════════════════════════════════
# 8c. 【V91.9】AI选股 - Gemini 筛选短中长期好股，中美港各 Top3
# ═══════════════════════════════════════════════════════════════
def run_ai_stock_selector(progress_callback=None):
    """Retired ungrounded selector; only signed central reviews grant grades."""
    return {'us': [], 'hk': [], 'cn': [], 'ai_report': ''}, '旧独立AI选股已停用；请使用3A中央评级与固定跟踪席位。'


# ═══════════════════════════════════════════════════════════════
# 8d. 【自选股分析】按中美港划分，逐只分析：催化、技术面、风险、操作建议（与钉钉日报同源）
# ═══════════════════════════════════════════════════════════════
def _get_watchlist_price(code):
    """获取自选股现价（fetch_stock_data 内部会做 to_yf_cn_code 转换）"""
    try:
        df = fetch_stock_data(code)
        if df is not None and len(df) > 0 and "Close" in df.columns:
            return float(df["Close"].iloc[-1])
    except Exception:
        pass
    return None


def _get_watchlist_scan_signals():
    """获取自选股在V88扫描中的信号：强势/蓄势/拐点，供差异化操作建议（与钉钉日报同源）"""
    try:
        _path = _BRIEF_CACHE_DIR / "scan_results.json"
        if not _path.exists():
            return {}
        data = json.loads(_path.read_text(encoding="utf-8"))
        sig = {}
        for mkt in ("US", "HK", "CN"):
            d = data.get(mkt, {})
            for cat, label in [("top", "强势"), ("coil", "蓄势"), ("breakout", "启动"), ("inflection", "拐点")]:
                for s in d.get(cat, []):
                    c = str(s.get("代码", "")).upper().strip()
                    if c:
                        sig[c] = (label, s.get("理由", ""), s.get("建议", ""))
        return sig
    except Exception:
        return {}


def run_watchlist_analysis(progress_callback=None):
    """Retired legacy force-trade entry; current research uses central reviews."""
    return '', '此旧自选点评入口已停用；请通过3A中央评级或个股深度分析查看当前GPT双审、证据与原合同。'


# ═══════════════════════════════════════════════════════════════
# 9. 【V89.6.2】注释：call_model_api已在前面定义（2815行）
# ═══════════════════════════════════════════════════════════════
# call_model_api函数已提前定义，确保所有模块都能正常调用

# 【V89.4】绑定舆情分析器的AI调用函数
if SENTIMENT_ANALYZER_AVAILABLE and _sentiment_analyzer:
    _sentiment_analyzer.call_ai = call_model_api

# ═══════════════════════════════════════════════════════════════
# 10. Session State 初始化
# ═══════════════════════════════════════════════════════════════
if 'proxy_port' not in st.session_state: st.session_state.proxy_port = _detect_system_proxy_port()
if 'scan_selected_code' not in st.session_state: st.session_state.scan_selected_code = None
if 'scan_selected_name' not in st.session_state: st.session_state.scan_selected_name = None
if 'trigger_analysis' not in st.session_state: st.session_state.trigger_analysis = False
# 【V87.7】全局对比篮
if 'compare_basket' not in st.session_state: st.session_state.compare_basket = []  # [(code, name), ...]
from compare_ui import MAX_COMPARE as _MAXCMP   # 用户2026-08-01定:最多四只(原为5,与模块标题"2-4只"自相矛盾)
if 'search_history' not in st.session_state: st.session_state.search_history = []  # [(code, name), ...]
# 【V87.11】行业分析
if 'sector_analysis_name' not in st.session_state: st.session_state.sector_analysis_name = None
if 'sector_analysis_market' not in st.session_state: st.session_state.sector_analysis_market = None
if 'sector_analysis_codes' not in st.session_state: st.session_state.sector_analysis_codes = None

# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# 11. 侧边栏 - 个股搜索入口
# ═══════════════════════════════════════════════════════════════
with st.sidebar:
    # 【V88】版本标识
    if USE_NEW_MODULES:
        st.markdown('<p style="font-family: inherit; font-size: 12px; font-weight: 700; margin-bottom: 0.5rem;">👑 AI 皇冠双核 V88</p>', unsafe_allow_html=True)
        st.caption("✨ 模块化架构 | LRU缓存")
    
    # 【V90.3】系统性能与数据刷新（从主区域移到侧边栏）
    if Config.ENABLE_PERF_LAYER and Config.ENABLE_EXPECTATION_LAYER:
        st.markdown('<p style="font-size: 12px; font-weight: 600; margin-bottom: 0.3rem;">⚙️ 系统性能</p>', unsafe_allow_html=True)
        try:
            # 强制刷新按钮
            force_refresh_btn = st.button(
                "🔄 强制刷新",
                key="force_refresh_macro",
                width='stretch',
                help="清除所有缓存，重新获取最新市场数据"
            )
            
            if force_refresh_btn:
                st.session_state['force_refresh_requested'] = True
                _cache_manager.clear()
                _perf_monitor.reset()
                st.success("✅ 已触发强制刷新")
                st.rerun()
            
            # 性能监控（折叠）
            with st.expander("📊 性能详情", expanded=False):
                _perf_monitor.finalize()
                metrics = _perf_monitor.get_metrics()
                cache_stats = _cache_manager.get_stats()
                
                st.metric("总耗时", f"{metrics['total_time_ms']:.0f}ms", help="从开始到结束的总耗时")
                st.metric("缓存命中率", f"{_perf_monitor.get_cache_hit_ratio()*100:.1f}%", help="缓存命中次数 / 总请求次数")
                st.metric("缓存项数", f"{cache_stats['items_count']}项", help="当前缓存中的数据项数量")
                
                st.caption(f"💾 缓存大小: {cache_stats['total_size_mb']:.2f} MB")
                st.caption(f"🔍 命中: {metrics['cache_hit_count']}次 | 未命中: {metrics['cache_miss_count']}次")
                
                total_time = metrics['total_time_ms']
                if total_time < 1000:
                    perf_grade = "🟢 极快"
                elif total_time < 3000:
                    perf_grade = "🟡 正常"
                else:
                    perf_grade = "🔴 较慢"
                st.info(f"**评级**: {perf_grade}")
        
        except Exception as e:
            st.warning(f"⚠️ 性能面板异常: {str(e)[:40]}")
            logging.error(f"侧边栏性能面板异常: {e}")
        
        st.divider()
    
    # 【V87.13】缩小侧边栏标题字体
    st.markdown('<p style="font-size: 12px; font-weight: 700; margin-bottom: 1rem;">🛸 指挥控制台</p>', unsafe_allow_html=True)
    
    # 【V87.13】对比篮显示 - 缩小字体
    st.markdown('<p style="font-size: 12px; font-weight: 600; margin-top: 0.5rem; margin-bottom: 0.5rem;">⚔️ 对比篮</p>', unsafe_allow_html=True)
    if len(st.session_state.compare_basket) > 0:
        st.caption(f"📊 已选 {len(st.session_state.compare_basket)} 只股票")
        for i, (code, name) in enumerate(st.session_state.compare_basket):
            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown(f"**{i+1}.** {name} ({code})")
            with col2:
                if st.button("❌", key=f"remove_{code}_{i}", help="移除"):
                    st.session_state.compare_basket.pop(i)
                    st.rerun()
        
        col_compare, col_clear = st.columns(2)
        with col_compare:
            if st.button("⚔️ 开始对比", type="primary", width='stretch'):
                if len(st.session_state.compare_basket) >= 2:
                    codes = [item[0] for item in st.session_state.compare_basket]
                    names = [item[1] for item in st.session_state.compare_basket]
                    st.session_state.pk_codes = codes
                    st.session_state.pk_names = names
                    st.session_state.scan_selected_code = None
                    st.session_state.scan_selected_name = None
                    st.toast(f"⚔️ 开始对比 {len(codes)} 只股票", icon="⚔️")
                    st.rerun()
                else:
                    st.warning("至少选择2只股票才能对比")
        
        with col_clear:
            if st.button("🗑️ 清空", width='stretch'):
                st.session_state.compare_basket = []
                st.rerun()
    else:
        st.caption("💡 从搜索或扫描结果中添加股票")
    
    st.markdown("---")
    
    # 【V93】浏览个股历史 - 点击可快速查看历史分析过的股票
    st.markdown('<p style="font-size: 12px; font-weight: 600; margin-top: 0.5rem; margin-bottom: 0.5rem;">📜 浏览个股历史</p>', unsafe_allow_html=True)
    if len(st.session_state.search_history) > 0:
        for i, (code, name) in enumerate(st.session_state.search_history[:8]):
            if st.button(f"🔍 {name} ({code})", key=f"sidebar_hist_{i}_{code}", width='stretch', help=f"点击分析 {name}"):
                st.session_state.scan_selected_code = code
                st.session_state.scan_selected_name = name
                st.session_state.pk_codes = []
                st.session_state.pk_names = []
                st.toast(f"✅ 已选中 {name}", icon="🔍")
                st.rerun()
        st.caption(f"共 {len(st.session_state.search_history)} 只，最多显示 8 只")
    else:
        st.caption("💡 搜索股票后将显示在此")
    
    st.markdown("---")
    
    # 【V91.8】AI市场简报快捷入口：做个股分析时也能快速跳转
    st.caption("💡 做个股分析时，点击此处可快速滚动到页面底部")
    
    # 【V92】全量云端搜索已移至主区域「深度作战室」顶部
    st.caption("🔍 股票搜索已移至主区域 → 深度作战室")
    
    # 【V92】侧边栏收起提示：Streamlit 收起按钮在侧边栏与主区域交界处（左上角附近）
    st.caption("💡 收起侧边栏：点击**侧边栏右边缘**或**主区域左上角**的 ◀ 箭头")
    
    st.divider()
    
    # 代理设置
    st.markdown('<p style="font-size: 12px; font-weight: 600; margin-top: 1rem; margin-bottom: 0.3rem;">⚙️ 网络设置</p>', unsafe_allow_html=True)
    _default_port = _detect_system_proxy_port()
    proxy_port = st.text_input("本地代理端口", value=_default_port, key="proxy_port_input")
    st.session_state.proxy_port = (proxy_port or _default_port).strip() or _default_port
    
    if st.button("测试连接", width='stretch'):
        purl = f"http://127.0.0.1:{st.session_state.proxy_port}"
        try:
            with ProxyContext(purl):
                r = requests.get("https://www.google.com", timeout=5, verify=True)
            st.success(f"✅ Google: {r.status_code}")
        except Exception as e:
            st.error(f"❌ 连接失败: {type(e).__name__}")
    
    st.divider()
    
    # 【V88】缓存统计显示
    if USE_NEW_MODULES:
        st.markdown('<p style="font-size: 12px; font-weight: 600; margin-top: 1rem; margin-bottom: 0.3rem;">💾 缓存状态 (V88 LRU)</p>', unsafe_allow_html=True)
        cache_stats = local_cache.get_stats()
        if cache_stats:
            st.metric(
                "缓存使用",
                f"{cache_stats['total_size_mb']:.1f}MB",
                f"{cache_stats['usage_percent']:.1f}%"
            )
            st.caption(f"📁 文件数: {cache_stats['file_count']} | ⏱️ TTL: {cache_stats['ttl_seconds']}s")
            st.caption(f"🔄 策略: LRU淘汰（保持80%容量）")
        st.divider()
    
    # 【V87.8】系统自检和股票池清理
    st.markdown('<p style="font-size: 12px; font-weight: 600; margin-top: 1rem; margin-bottom: 0.3rem;">🛠️ 系统维护</p>', unsafe_allow_html=True)
    st.caption("💡 诊断系统状态")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🛠️ 系统诊断", width='stretch', type="secondary"):
            with _v88_running("正在执行系统诊断..."):
                diagnostic_result = run_system_diagnostic()
            
            # 显示结果
            st.markdown("#### 诊断结果")
    
    with col2:
        if st.button("🏥 股票池检查", width='stretch', type="secondary"):
            with _v88_running("正在检查股票池健康状况..."):
                us_pool, hk_pool, cn_pool = init_stock_pools()
                
                st.markdown("#### 股票池健康检查结果")
                
                # 检查各市场股票池
                us_invalid = validate_stock_pool_health(us_pool, "美股", max_test=3)
                hk_invalid = validate_stock_pool_health(hk_pool, "港股", max_test=3) 
                cn_invalid = validate_stock_pool_health(cn_pool, "A股", max_test=3)
                
                total_invalid = len(us_invalid) + len(hk_invalid) + len(cn_invalid)
                if total_invalid == 0:
                    st.success("✅ 所有测试的股票代码都能正常获取数据")
                else:
                    st.warning(f"⚠️ 发现 {total_invalid} 个问题代码，建议更新股票池")
    
    # 【V87.15 + V88】缓存管理
    st.markdown('<p style="font-size: 12px; font-weight: 600; margin-top: 1rem; margin-bottom: 0.3rem;">💾 缓存管理</p>', unsafe_allow_html=True)
    
    cache_stats = local_cache.get_stats()
    st.caption(f"📊 缓存使用: {cache_stats['total_size_mb']:.1f}MB / {cache_stats['max_size_mb']:.0f}MB ({cache_stats['usage_percent']:.1f}%)")
    st.caption(f"📁 缓存文件: {cache_stats['file_count']} 个 | ⏱️ 有效期: 1小时（全模块统一）")
    
    # 【V87.15】容量警告
    if cache_stats['usage_percent'] > 90:
        st.warning(f"⚠️ 缓存即将满，达到{cache_stats['max_size_mb']:.0f}MB后将自动清零", icon="⚠️")
    
    col_cache1, col_cache2 = st.columns(2)
    with col_cache1:
        if st.button("🗑️ 清空缓存", width='stretch', help="清空所有本地缓存文件"):
            local_cache.clear_all()
            st.cache_data.clear()
            st.success("✅ 缓存已清空")
            st.rerun()
    
    with col_cache2:
        if st.button("📋 查看失败详情", width='stretch'):
            st.session_state.show_failed_stocks = True
            st.rerun()
    
    # 原有的诊断结果显示逻辑
    if 'diagnostic_result' in locals():
        
        # 1. 网络连通性
        net = diagnostic_result['network']
        if net['status'] == 'ok':
            st.success(f"✅ **网络连通性**: {net['message']}")
        elif net['status'] == 'warning':
            st.warning(f"⚠️ **网络连通性**: {net['message']}")
        else:
            st.error(f"❌ **网络连通性**: {net['message']}")
        
        # 2. 数据源测试
        st.markdown("**数据源测试**:")
        for market, result in diagnostic_result['data_sources'].items():
            if result['status'] == 'ok':
                st.success(f"✅ {result['name']}: {result['message']} (最后日期: {result.get('last_date', 'N/A')})")
            elif result['status'] == 'warning':
                st.warning(f"⚠️ {result['name']}: {result['message']}")
            else:
                st.error(f"❌ {result['name']}: {result['message']}")
        
        # 3. 整体评估
        st.divider()
        overall = diagnostic_result['overall']
        if overall == 'healthy':
            st.success("🎉 **系统状态**: 一切正常，可以开始使用！")
        elif overall == 'warning':
            st.warning("⚠️ **系统状态**: 部分功能可能受限，但基本可用")
        else:
            st.error("❌ **系统状态**: 存在严重问题，请检查网络和代理设置")
    
    st.divider()
    

# ═══════════════════════════════════════════════════════════════
# 12. 主界面 - 标题（修复遮挡）
# ═══════════════════════════════════════════════════════════════
st.markdown(
    '''<style>
    div[data-testid="stElementContainer"]:has(.v88-mini-brand){
        margin-top:-1.75rem!important;margin-bottom:-.55rem!important;min-height:10px!important;
    }
    .v88-mini-brand{text-align:left;margin:0!important;padding:0!important;color:#7b8798;
        font-size:12px!important;line-height:1!important;white-space:nowrap;}
    </style><div class="v88-mini-brand">👑 V88 · 同源行情 / AI日报 / 持仓决策</div>''',
    unsafe_allow_html=True
)
# 【V90.7】选中股票后置顶提示
if st.session_state.get('scan_selected_code'):
    st.markdown('<div style="background: linear-gradient(135deg, #10b981 0%, #059669 100%); padding: 1rem; border-radius: 8px; margin-bottom: 1rem; color: white; font-size: 12px; font-weight: 600; text-align: center;">🎯 深度分析报告已生成，请向下滚动查看「⚔️ 深度作战室」完整内容</div>', unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# 【V89.8 布局重构】模块分隔函数
# ═══════════════════════════════════════════════════════════════
def _module_header(icon, title, subtitle='', color_from='#2563eb', color_to='#e0f2fe', compact=False):
    """Compact section label; render date must never masquerade as data date."""
    from html import escape as _header_escape
    detail = ('<details style="font-size:11px;color:#64748b;margin-top:3px"><summary>查看说明</summary>'
              + _header_escape(subtitle) + '</details>') if subtitle else ''
    st.markdown('<div style="border-left:3px solid #2563eb;background:#eff6ff;padding:7px 10px;'
                'border-radius:5px;margin:10px 0"><div style="font-size:14px;font-weight:600;color:#1e40af">'
                + _header_escape(str(icon)+' '+str(title)) + '</div>'+detail+'</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════
# 14. 【V78关键修复】深度作战室 - 独立于所有tabs之外
# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# 【V96】首页·今日导航：回答"我今天该关注/买什么"
# 数据全部来自本地日报/快照文件（每日3次自动更新），零网络请求、秒开、无闪烁
# ═══════════════════════════════════════════════════════════════


_ACT_COLORS9 = [
    (re.compile(r"(买入|建仓|加仓|试仓)"), "#dc2626"),      # 买入类=红（进攻）
    (re.compile(r"(评估减仓|冲高减仓|减仓|锁盈)"), "#ea580c"),  # 减仓类=橙（部分退出）
    (re.compile(r"(卖出|清仓|退出|止损|破位离场)"), "#16a34a"),  # 卖出类=绿（离场）
    (re.compile(r"(持有|拿住)"), "#2563eb"),                # 持有=蓝
    (re.compile(r"(回避)"), "#0891b2"),                     # 回避=青
]


def _act_colorize9(text: str) -> str:
    """【V88·动作分色】（2026-07-16 用户定则：买入/卖出/减仓等动作词全系统不同颜色）
    买入类=红 / 减仓类=橙 / 卖出退出类=绿 / 持有=蓝 / 回避=青。用于 markdown/HTML 渲染处。"""
    for _rx9, _col9 in _ACT_COLORS9:
        text = _rx9.sub(lambda m: f"<span style='color:{_col9};font-weight:700'>{m.group(1)}</span>", text)
    return text


def _act_color_of9(action: str) -> str:
    """取动作字符串的主色（卡片单动作标签用）。"""
    for _rx9, _col9 in _ACT_COLORS9:
        if _rx9.search(str(action or "")):
            return _col9
    return "#334155"


def _linkify_md(md: str) -> str:
    """【V88·全局个股可点击 v2】两件事：①个股名/token→内联链接（?q=深链）
    ②markdown表格整体转HTML表格——md表格单元格内的HTML前端渲染不可靠，HTML表格100%可点。"""
    import re as _re
    # 【V88·点击不跳页 2026-07-20 用户反馈】深度分析改新标签页打开——主页面停在原位不被带走
    A = ('<a href="?q={c}&focus=deep#v88-deep-analysis" target="_blank" rel="noopener" '
         'style="color:#1e3a5f;text-decoration:underline;cursor:pointer;font-weight:600">{t}</a>')

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
    # 【V88·动作分色】所有 markdown 渲染统一出口上色（买红/减橙/卖绿/持蓝/避青）
    return _act_colorize9("\n".join(out))
def _stk_link(name, code):
    """【V88·内联可点个股】不改字体字号，名字即链接（?q=深链→自动深度分析+入观察池）。
    【V88·点击不跳页 2026-07-20 用户反馈】新标签页打开——主页面停在原位不被带走。"""
    from stock_profile_view import link_html as _profile_link, compact_html as _profile_compact
    return (_profile_link(name, code,
            style='color:#1e3a5f;text-decoration:underline;cursor:pointer;font-weight:600')
            + _profile_compact(code))


# 【V88·决策卡·共用】自选决策台与持仓决策台共用同一张卡片（短/中/长/16周概率走势条+盈亏比+期望）。
# 2026-07-16 用户要求持仓也变成自选那样的概率决策台，故把卡片渲染提为模块级复用。
_V88_CARD_CSS = """
<style>
.v88-watch-shell{border:1px solid #bfdbfe;border-radius:12px;background:#f8fbff;padding:10px 11px 8px;margin:2px 0 10px}
.v88-watch-title{display:flex;align-items:flex-end;justify-content:space-between;gap:10px;margin-bottom:8px}
.v88-watch-title h3{margin:0;color:#123a70;font-size:18px;line-height:1.25}
.v88-watch-title p{margin:0;color:#64748b;font-size:12px;text-align:right}
.v88-watch-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;align-items:start}
.v88-watch-market{min-width:0}
.v88-watch-market h4{margin:0 0 5px;padding:4px 7px;border-radius:6px;background:#eaf2ff;color:#173b68;font-size:13px}
.v88-watch-market h4 span{float:right;color:#64748b;font-size:12px;font-weight:500}
.v88-watch-card{background:#fff;border:1px solid #dbe4f0;border-radius:8px;padding:6px 7px;margin-bottom:5px;box-shadow:0 1px 2px rgba(15,23,42,.04);min-height:266px;display:flex;flex-direction:column}/* 右上角周期主判断3行后改min-height自适应(铁律:内容行数会变的卡禁fixed height裁剪) */
.v88-watch-conflict{border:2px solid #f59e0b;background:#fffdf5}
.v88-watch-pending{border-style:dashed;background:#fafbfc;opacity:.85;height:252px}
.v88-cycle-warn{color:#b45309;font-weight:700}
.v88-watch-card-head{display:flex;justify-content:space-between;align-items:center;gap:5px;font-size:14px;line-height:1.3}
.v88-name-line{min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.v88-watch-card-head a{color:#173b68!important;text-decoration:none!important;font-weight:700!important}
.v88-watch-card-head a.v88-dual{color:#b8860b!important;font-weight:800!important}/* 一池归一:金名=持仓∩自选,压过上行!important */
.v88-code{color:#94a3b8;font-size:12px;margin-left:3px}
.v88-action{color:#1d4ed8;font-size:13px;white-space:nowrap}
.v88-level{display:inline-block;padding:1px 4px;border-radius:4px;font-size:12px;margin-right:3px;color:#fff}
.v88-level-A{background:#dc2626} .v88-level-B{background:#2563eb} .v88-level-C{background:#64748b}
.v88-cyc-head{display:flex;justify-content:space-between;align-items:center;margin-top:6px;font-size:12px;color:#64748b}
.v88-cyc-trend{font-weight:700;color:#334155}
.v88-cyc-strip{display:flex;gap:4px;margin-top:3px}
.v88-cyc{flex:1;min-width:0;text-align:center;border:1px solid #e5eaf1;border-radius:5px;padding:3px 1px;background:#fff}
.v88-cyc i{display:block;font-style:normal;font-size:12px;color:#94a3b8;line-height:1.2}
.v88-cyc b{font-size:15px;line-height:1.2}
.v88-spark-wrap{margin-top:2px;line-height:0}
.v88-spark{width:100%;height:auto;max-height:58px;display:block}
.v88-rrline{margin-top:3px;font-size:14px;color:#64748b;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.v88-rrline b{font-size:15px} .v88-rrline em{font-style:normal;font-size:12px}
.v88-watch-foot{display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;gap:2px 8px;margin-top:3px;color:#475569;font-size:14px;line-height:1.35;overflow:hidden}
.v88-watch-foot span{margin-right:8px}
@media(max-width:1100px){.v88-watch-grid{grid-template-columns:1fr}}
</style>
"""


def _v88_rr_state9(_rr9):
    _v9 = float(_rr9 or 0)
    if _v9 >= 2:
        return "优秀·上行空间足", "#1d4ed8", "#dbeafe"
    if _v9 >= 1.5:
        return "可关注", "#2563eb", "#eff6ff"
    if _v9 >= 1:
        return "偏低", "#b45309", "#fffbeb"
    return "不合格", "#b91c1c", "#fef2f2"


def _v88_decision_card(_d9):
    """一张决策卡的 HTML：多周期上涨概率走势条 + 盈亏比 + 期望 + 动作。自选/持仓共用。"""
    if _d9.get("_pending"):
        return f"""
    <div class="v88-watch-card v88-watch-pending">
      <div class="v88-watch-card-head">
        <div>{_stk_link(_d9.get('name') or _d9.get('code'), _d9.get('code'))}
        <span class="v88-code">{_d9.get('code')}</span></div>
        <b class="v88-action">计算中</b>
      </div>
      <div class="v88-watch-foot"><span>信号计算中或数据暂缺，稍后自动刷新；点名称可先看深度分析</span></div>
    </div>"""
    if _d9.get("_short_history"):
        # 【V88·新股如实卡 2026-07-18 SPCX案发】历史不足35根K线,多周期概率算不动——
        # 如实说明并给现价/近5日/上市以来三个真实数字,不给假信号不装"计算中"。
        _c5x = float(_d9.get("chg5") or 0)
        _cix = float(_d9.get("ipo_chg") or 0)
        return f"""
    <div class="v88-watch-card v88-watch-pending">
      <div class="v88-watch-card-head">
        <div>{_stk_link(_d9.get('name') or _d9.get('code'), _d9.get('code'))}
        <span class="v88-code">{_d9.get('code')}</span></div>
        <b class="v88-action" style="color:#b45309">新股·数据积累中</b>
      </div>
      <div style="font-size:12px;color:#64748b;line-height:1.4">上市仅{_d9.get('bars')}个交易日，
      多周期概率需≥35根K线——历史不足不给假信号（如实说明）</div>
      <div class="v88-rrline">现价 <b>{_d9.get('last')}</b>
        ｜近5日<b style="color:{'#dc2626' if _c5x >= 0 else '#16a34a'}">{_c5x:+.1f}%</b>
        ｜上市以来<b style="color:{'#dc2626' if _cix >= 0 else '#16a34a'}">{_cix:+.1f}%</b></div>
      <div class="v88-watch-foot"><span>新股波动大、无均线结构，只按纪律小仓参与；点名称看K线与深度分析</span></div>
    </div>"""
    _rr9 = float(_d9.get("rr") or 0)
    _rr_txt9, _rr_color9, _rr_bg9 = _v88_rr_state9(_rr9)
    _up9, _down9 = int(_d9.get("p_up") or 0), int(_d9.get("p_down") or 0)
    _exp9 = float(_d9.get("expected_pct") or 0)
    _exp_color9 = "#dc2626" if _exp9 > 0 else ("#16a34a" if _exp9 < 0 else "#64748b")
    _level9 = str(_d9.get("level") or "B")
    _action9 = str(_d9.get("action") or "观察")
    # 【V88·动作语境化 2026-07-17 用户抓矛盾】"长期看好却评估减仓"是两层信号的呈现问题：
    # 风控动作=当日风险指令(非长期观点)→前缀"今日风控·"；时机绿灯却大字"观察"→升级显示可进。
    _ep_mode9 = str((_d9.get("entry_plan") or {}).get("mode") or "")
    _action_disp9 = _action9
    if any(k in _action9 for k in ("减仓", "退出", "回避", "清仓")) and float(_d9.get("expected_pct") or 0) > 1:
        _action_disp9 = f"今日风控·{_action9}"
    elif _ep_mode9 in ("现价可进", "回踩到位", "突破确认", "左侧低吸") and any(
            k in _action9 for k in ("观察", "等待回踩", "持有观察")):
        _action_disp9 = f"⏱{_ep_mode9}·仅短线"
    _at9 = str(_d9.get("analysis_time") or "时间未知")
    _score9 = int(_d9.get("unified_score") or 0)
    _medium9 = int(_d9.get("medium_score") or 0)
    _long_score9 = int(_d9.get("long_score") or 0)
    _entry9 = str(_d9.get("entry_note") or "入场条件待核")
    # 【V88·决策粒度=日 2026-07-28 用户定纲】"这就跟场外基金操作一样,今天买还是不买,
    # 而不是今天的某个价格买还是不买"——用户白天在忙,打开V88时"现价可进X.XX"已脱节数小时。
    # 故大字一律先答「今天是什么日+明确动词(买/卖/不动)」,价格区间降级为副行参考;
    # 上面那些"⏱现价可进·仅短线""持有观察·不加仓"的点位/和稀泥措辞由 day_call 覆盖。
    _dc9 = _d9.get("day_call") or (_d9.get("entry_plan") or {}).get("day_call") or {}
    if _dc9.get("day"):
        _vb9 = str(_dc9.get("verb") or "")
        _icon9 = {"买": "🟢", "卖": "🔴"}.get(_vb9, "⏸")
        _action_disp9 = f"{_icon9}今日{_dc9['day']}·{_vb9}"
        _bits9 = []
        # 【双基准 2026-07-28】confirm 已合并"日+区间是否齐备"的裁决,优先显示它;
        # 没有 confirm(老数据)才退回裸区间。
        if _dc9.get("confirm") and _dc9["confirm"] != "—":
            _bits9.append(str(_dc9["confirm"]))
        elif _dc9.get("zone_text"):
            _bits9.append(f"{_dc9.get('zone_kind') or '区间'}{_dc9['zone_text']}")
        if _dc9.get("why"):
            _bits9.append(str(_dc9["why"]))
        if _dc9.get("flip"):
            _bits9.append("翻转:" + "／".join(list(_dc9["flip"])[:2]))
        if _bits9:
            _entry9 = "｜".join(_bits9 + [_entry9])
    # 【V88·买卖视角分离 2026-07-17 用户定纲】自选=买入视角(有上升通道必给"何时/何区间进")；
    # 持仓=卖出视角(盈利奔跑的关键是何时下车:冲高减/目标位/破位全走三线)。
    _scope9x = str(_d9.get("scope") or "")
    _plan9x = _d9.get("trade_plan") or {}

    def _pnum9x(_v):
        try:
            return f"{float(_v):g}"
        except (TypeError, ValueError):
            return ""
    if _scope9x == "持仓":
        _t60m = re.search(r"目标([\d.]+)", str((_plan9x.get("mid") or {}).get("out") or ""))
        _res9x, _stop9x = _pnum9x(_d9.get("resistance")), _pnum9x(_d9.get("stop"))
        # 【V88·动态奔跑线 2026-07-17 用户批准】trailing stop 做活：与生命周期同口径
        # (TRAIL_DD=10：峰值浮盈回撤超10个百分点→锁盈)，换算成价格画在卡上。
        # 峰值浮盈≤10点时锁盈线无意义，退回静态"冲阻力减半"。
        _first9x = f"冲{_res9x}减半" if _res9x else ""
        try:
            _pk9x = float(_d9.get("peak_pnl") or 0)
            _cost9x = float(_d9.get("hold_cost") or 0)
            if _pk9x > 10 and _cost9x > 0:
                _run9x = _cost9x * (1 + (_pk9x - 10) / 100)
                _first9x = f"奔跑线{_run9x:.2f}(峰盈{_pk9x:.0f}%回撤10点锁盈)"
        except (TypeError, ValueError):
            pass
        _parts9x = [x for x in (
            _first9x,
            f"目标{_t60m.group(1)}(60日)" if _t60m else "",
            f"破{_stop9x}全走" if _stop9x else "") if x]
        if _parts9x:
            _entry9 = "<b style='color:#ea580c'>💰卖点</b>:" + "·".join(_parts9x)
            # 【V88·止盈止损带概率+战绩+why 2026-07-21 用户定纲"持仓操作和止盈止损一定要
            # 概率和成功率的标注和why"】下行概率+预计跌幅空间(引擎situational)+同类警示实盘
            # 战绩(到期核算)+一句why(优先消息归因)——卖点不再是裸价位。
            try:
                _gg_t9x = (_v88_success9().get("types") or {}).get("gate_guard") or {}
                _gg_txt9x = (f"同类警示实盘{_gg_t9x['rate']}%(n{_gg_t9x.get('n', 0)})"
                             if _gg_t9x.get("rate") is not None else "警示战绩积累中")
                _dnsp9x = abs(float(_d9.get("downside_pct") or 0))
                _why_sell9x = str(_d9.get("move_reason") or _d9.get("diag_why")
                                  or _d9.get("stage") or (_d9.get("facts") or {}).get("stage")
                                  or "趋势与位置实算")[:16]
                _entry9 += (f"<br><span style='font-size:11px;color:#64748b'>└ 下行概率{_down9}%"
                            + (f"·预计跌幅约-{_dnsp9x:.0f}%" if _dnsp9x else "")
                            + f"·{_gg_txt9x}·why:{_why_sell9x}</span>")
            except Exception:
                pass
    elif ("不进" in _entry9 or "等" in _entry9) and (float(_d9.get("medium_score") or 0) >= 58
                                                    or float(_d9.get("long_score") or 0) >= 58):
        _mid_in9x = str((_plan9x.get("mid") or {}).get("in") or "")
        _zone9x = re.search(r"区间([\d.]+~[\d.]+)", _mid_in9x)
        _inv9x = re.search(r"MA55\(([\d.]+)\)", _mid_in9x)
        if _zone9x:
            _entry9 = (f"<b style='color:#dc2626'>🎯中线买点</b>:区间{_zone9x.group(1)}分批(4-8周)"
                       + (f"·破{_inv9x.group(1)}废" if _inv9x else ""))

    def _side_word9(_v):
        _v = float(_v or 50)
        return (f"<b style='color:#dc2626'>偏强{_v:.0f}</b>" if _v >= 58 else
                (f"<b style='color:#16a34a'>偏弱{_v:.0f}</b>" if _v <= 42 else f"中性{_v:.0f}"))
    _mlabel9 = _side_word9(_medium9)
    _llabel9 = _side_word9(_long_score9)
    _conflict9 = bool(_d9.get("cycle_conflict"))
    # 【V88·盈亏比语境化 2026-07-17 用户抓矛盾】"优秀却减仓"——盈亏比=若进场的赔率结构,
    # 不是"现在该买"。风控/等待状态下明说关系,不让用户猜。
    if any(k in _action9 for k in ("减仓", "退出", "回避", "清仓")) and _rr9 >= 1.5:
        _rr_txt9, _rr_color9, _rr_bg9 = "赔率优·但风控优先", "#b45309", "#fffbeb"
    elif _conflict9:
        _rr_txt9, _rr_color9, _rr_bg9 = "短期赔率≠可买", "#b45309", "#fffbeb"
    elif "小仓试错" in _action9:
        _rr_txt9, _rr_color9, _rr_bg9 = "概率补偿·仅小仓", "#2563eb", "#eff6ff"
    elif "试仓复核" in _action9:
        _rr_txt9, _rr_color9, _rr_bg9 = "入场门槛通过", "#1d4ed8", "#dbeafe"
    elif "等待回踩" in _action9:
        _rr_txt9, _rr_color9, _rr_bg9 = "当前价不划算", "#b45309", "#fffbeb"
    _exp_suffix9 = "（周期冲突，不升级）" if _conflict9 else ""
    _facts_h9 = ((_d9.get("facts") or {}).get("horizons") or {})
    _cyc_probs9 = []
    for _lab9 in ("2周", "4周", "8周", "16周", "32周"):
        _rs9 = (_facts_h9.get(_lab9) or {}).get("rule_score")
        if _rs9 is not None:
            _cyc_probs9.append((_lab9, int(round(float(_rs9)))))
    if len(_cyc_probs9) < 4:
        _cyc_probs9 = [("2周", _up9), ("4周", _medium9), ("8周", _medium9),
                       ("16周", _long_score9), ("32周", _long_score9)]

    def _cyc_col9(_p):
        return "#dc2626" if _p >= 55 else ("#16a34a" if _p <= 45 else "#64748b")
    _c_first9, _c_last9 = _cyc_probs9[0][1], _cyc_probs9[-1][1]
    _trend9 = ("↗ 越远越强" if (_c_last9 - _c_first9 >= 8 and _c_last9 >= 59) else
               ("↘ 越远越弱" if (_c_first9 - _c_last9 >= 8 and _c_last9 <= 41) else
                ("↗ 趋中性" if _c_last9 - _c_first9 >= 8 else
                 ("↘ 趋中性" if _c_first9 - _c_last9 >= 8 else "→ 各周期均衡"))))
    _trend_col9 = ("#dc2626" if _c_last9 - _c_first9 >= 8 else
                   ("#16a34a" if _c_first9 - _c_last9 >= 8 else "#94a3b8"))
    # 【V88·今天锚点 2026-07-18 用户点单】卡片走势条同样从"今天"画起，2周前不是空白——
    # 今天=当前阶段基准位(蓄势/底部45·领涨/主升62·派发/滞涨55·退潮/破位38·其余50,
    # 与深度页象限口径一致)+近5日实际动量微调；空心点=实算起点,实心点=各周期预测。
    _stage9x = str((_d9.get("facts") or {}).get("stage") or _d9.get("stage") or "")
    _pb9x = _v88_stage_base9(_stage9x)
    try:
        _r5x9 = float((_facts_h9.get("2周") or {}).get("ret5_pct") or 0)
    except (TypeError, ValueError):
        _r5x9 = 0.0
    _now9x = int(round(max(20, min(80, _pb9x + max(-6.0, min(6.0, _r5x9)) * 1.2))))
    # 【V88·卡片瘦身 2026-07-17 用户反馈"板块太大"】走势图压扁(viewBox更宽更矮)——
    # svg width:100% 会随卡宽等比放大,加宽viewBox即让实际高度和字号显著变小,更精致。
    _spk_src9 = [("今天", _now9x)] + _cyc_probs9
    _spk_w9, _spk_h9, _sn9 = 320, 34, len(_spk_src9)

    def _spk_y9(_p):
        return round(12 + 13 * (1 - (min(90, max(20, _p)) - 20) / 70), 1)
    _pts9 = [(round(16 + _i9 * (_spk_w9 - 32) / max(1, _sn9 - 1), 1),
              _spk_y9(_p9x), _p9x, _l9x)
             for _i9, (_l9x, _p9x) in enumerate(_spk_src9)]
    _base_y9 = _spk_y9(50)
    _poly9 = " ".join(f"{x},{y}" for x, y, _, _ in _pts9)
    # 【V88·逐点方向符号 2026-07-19 用户点单】每个%后跟↑↓(相对前一点;2周的箭头=相对今天锚点)
    _marks_parts9 = []
    _prev_p9m = None
    for x, y, p, lab in _pts9:
        if lab == "今天":
            _marks_parts9.append(
                f'<circle cx="{x}" cy="{y}" r="2.3" fill="var(--card-bg,#fff)" stroke="#475569" stroke-width="1.1"/>'
                f'<text x="{x}" y="7" text-anchor="middle" font-size="7.5" font-weight="700" fill="#475569">现在</text>'
                f'<text x="{x}" y="33" text-anchor="middle" font-size="6.5" fill="#94a3b8">今天</text>')
        else:
            _ar9m = ""
            if _prev_p9m is not None:
                _dd9m = p - _prev_p9m
                _ar9m = "↑" if _dd9m >= 1 else ("↓" if _dd9m <= -1 else "≈")
            _marks_parts9.append(
                f'<circle cx="{x}" cy="{y}" r="2.1" fill="{_cyc_col9(p)}"/>'
                f'<text x="{x}" y="7" text-anchor="middle" font-size="7.5" font-weight="700" '
                f'fill="{_cyc_col9(p)}">{p}%{_ar9m}</text>'
                f'<text x="{x}" y="33" text-anchor="middle" font-size="6.5" fill="#94a3b8">{lab}</text>')
        _prev_p9m = p
    _marks9 = "".join(_marks_parts9)
    _spark9 = (
        f'<svg class="v88-spark" viewBox="0 0 {_spk_w9} {_spk_h9}">'
        f'<line x1="10" y1="{_base_y9}" x2="{_spk_w9 - 10}" y2="{_base_y9}" stroke="#e2e8f0" '
        f'stroke-width="1" stroke-dasharray="3 3"/>'
        f'<polyline points="{_poly9}" fill="none" stroke="{_trend_col9}" stroke-width="1.6" '
        f'stroke-linejoin="round" stroke-linecap="round"/>{_marks9}</svg>')
    # 【V88·凭什么铁律 2026-07-18 用户定纲】每张卡必有一句定性——有据(研报/公告/新闻)
    # 或明示"纯技术驱动",不许只有数字。
    _edge_s9 = _v88_fund_edge_short(_d9.get("name") or "")
    # 【V88·双榜拥挤 2026-07-18】东财人气榜+雪球热股(匿名token已通):双榜同上=更硬反指标
    _intel_d9 = _v88_intel9()
    _cn_card9 = _canonical_code(str(_d9.get("code") or ""))
    _hot9 = (_intel_d9.get("hot_map") or {}).get(_cn_card9)
    _xq_only9 = None if _hot9 else (_intel_d9.get("xq_map") or {}).get(_cn_card9)
    if _hot9 and _hot9.get("xq"):
        _hot_txt9 = f' <b style="color:#dc2626">🔥🔥双榜拥挤 东#{_hot9["rank"]}·雪#{_hot9["xq"]}（散户扎堆·反指标更硬）</b>'
    elif _hot9:
        _hot_txt9 = f' <b style="color:#b45309">🔥东财人气榜#{_hot9["rank"]}·散户扎堆拥挤提示</b>'
    elif _xq_only9:
        _hot_txt9 = f' <b style="color:#b45309">🔥雪球热股#{_xq_only9}·散户扎堆拥挤提示</b>'
    else:
        _hot_txt9 = ''
    _edge_html9 = ('<div style="font-size:12px;color:#475569;white-space:nowrap;overflow:hidden;'
                   'text-overflow:ellipsis;margin-top:1px">💡'
                   + (_edge_s9 or "无研报/新闻定性——纯技术驱动（如实说明）")
                   + _hot_txt9 + '</div>')
    _dgk9 = str(_d9.get("diag_kind") or "")
    _dg_col9 = {"破位": "#dc2626", "个股利空": "#b91c1c", "错杀": "#16a34a",
                "可进": "#16a34a", "持有": "#2563eb"}.get(_dgk9, "#64748b")
    _diag_html9 = ((f'<div style="font-size:12px;color:{_dg_col9};white-space:nowrap;overflow:hidden;'
                    f'text-overflow:ellipsis;margin-top:1px">🩺{_d9.get("diag_verdict") or _dgk9}·'
                    + str(_d9.get("diag_why") or "")[:34] + '</div>') if _dgk9 else '')
    # 【V88·乱码修复 2026-07-18】归因行并到上一行尾:独立行在无归因时会生成"缩进+空"的
    # 空白行→Markdown按"HTML块遇空行终止"把后续卡片/市场列全打成源代码(用户抓的乱码+只剩美股)。
    _mv_html9 = ((f'<div style="font-size:12px;color:#b91c1c;line-height:1.3;margin-top:2px;'
                  f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis">📌'
                  + str(_d9.get('move_reason')) + '</div>') if _d9.get('move_reason') else '')
    # 【一池归一 2026-07-18】双重身份金色名(#b8860b)醒目;纯持仓💼/纯自选⭐小标
    _is_hold9 = str(_d9.get("scope") or "") == "持仓"
    _is_watch9 = bool(_d9.get("in_watchlist")) or str(_d9.get("scope") or "") == "自选"
    _dual9 = _is_hold9 and _is_watch9
    _pool_tag9 = "" if _dual9 else ("💼" if _is_hold9 else ("⭐" if _is_watch9 else ""))
    # class方案压过 .v88-watch-card-head a 的 !important(内联style会被它盖掉,实机案发)
    _name_html9 = (f'<a class="v88-dual" href="?q={_d9.get("code")}&focus=deep#v88-deep-analysis" '
                   f'target="_blank" rel="noopener" style="cursor:pointer">'
                   f'{_d9.get("name") or _d9.get("code")}</a>' if _dual9
                   else _stk_link(_d9.get('name') or _d9.get('code'), _d9.get('code')))
    # 【V88·右上角周期主判断 2026-07-19 用户定纲】"明天"这种24小时判断不是主角——
    # 右上角主位=1-2周区间判断(方向概率+大概率波动区间,明天大涨大跌也先看本周整体),
    # 次行=中长期周数判断,当日动作降级为末行小字(纪律指令仍在,只是不再当主角)。
    _wk2_word9 = "偏涨" if _up9 >= 58 else ("偏跌" if _up9 <= 42 else "震荡")
    _wk2_col9 = "#dc2626" if _up9 >= 58 else ("#16a34a" if _up9 <= 42 else "#64748b")
    _updn9h = ""
    try:
        _u9h = float(_d9.get("upside_pct") or 0)
        _dn9h = float(_d9.get("downside_pct") or 0)
        if _u9h or _dn9h:
            _updn9h = f"区间-{abs(_dn9h):.0f}%~+{_u9h:.0f}%｜"
    except (TypeError, ValueError):
        pass
    _head_right9 = (
        f'<div style="text-align:right;line-height:1.3;flex-shrink:0">'
        f'<b class="v88-action" style="color:{_wk2_col9}">1-2周{_wk2_word9}{_up9}%</b>'
        f'<div style="font-size:10.5px;color:#475569">{_updn9h}中(4-8周){_mlabel9}·长(16-32周){_llabel9}</div>'
        f'<div style="font-size:10.5px;color:{_act_color_of9(_action_disp9)}">今日动作:{_action_disp9}</div></div>')
    return f"""
    <div class="v88-watch-card{' v88-watch-conflict' if _conflict9 else ''}">
      <div class="v88-watch-card-head">
        <div class="v88-name-line"><span class="v88-level v88-level-{_level9}">{_level9}级</span>
        {_pool_tag9}{_name_html9}
        <span class="v88-code">{_d9.get('code')}</span></div>
        {_head_right9}
      </div>{_mv_html9}{_edge_html9}{_diag_html9}
      <div class="v88-cyc-head">
        <span>今天→各周期上涨概率（红涨绿跌；↑↓≈=较前一档升/降/平,2周较「今天」）</span>
        <span class="v88-cyc-trend">{_trend9}</span>
      </div>
      <div class="v88-spark-wrap">{_spark9}</div>
      <div class="v88-rrline">关键位盈亏比 <b style="color:{_rr_color9}">{_rr9:.2f}</b>
        <em style="color:{_rr_color9}">（{_rr_txt9}）</em></div>
      <div style="font-size:12px;line-height:1.3;margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{_entry9}</div>
      <div class="v88-watch-foot"><span>短(2周)期望<b style="color:{_exp_color9}">{_exp9:+.1f}%</b>{_exp_suffix9}</span>
      <span>中(4-8周){_mlabel9}</span><span>长(16-32周){_llabel9}</span>
      <span><b>统一分{_score9}</b></span><span>🕒 {_at9}</span></div>
    </div>"""


def _v88_attach_diag9(_wa, _live_chg):
    """【V88·一票一卡 2026-07-18 用户抓模块内重复】把 diagnose_today 结论写进
    每条决策(diag_kind/verdict/why)由决策卡🩺行呈现——逐只不再独立成区，
    同一只票在一个模块里只出现一次。"""
    try:
        from v88_decision_core import diagnose_today as _diag9
    except Exception:
        return
    _mkt_chg = {"🇺🇸美股": _live_chg.get("美股", 0), "🇭🇰港股": _live_chg.get("港股", 0),
                "🇨🇳A股": _live_chg.get("A股", 0)}
    for _d in (_wa or {}).get("decisions") or []:
        if _d.get("_pending") or _d.get("_short_history"):
            continue
        try:
            _mk = _d.get("market") or market_of_code(_d.get("code", ""))
            _dg = _diag9(scope=_d.get("scope", "自选"),
                         today_chg=float(_d.get("today_chg") or 0),
                         market_chg=float(_mkt_chg.get(_mk, 0) or 0),
                         stage=str(_d.get("stage") or ""),
                         broke_stop=bool(_d.get("broke_stop")),
                         pos52=_d.get("pos52") or 50,
                         action=str(_d.get("action") or ""),
                         entry_mode=((_d.get("entry_plan") or {}).get("mode") or ""),
                         name=_d.get("name", ""))
            _d["diag_kind"] = _dg.get("kind")
            _d["diag_verdict"] = _dg.get("verdict")
            _d["diag_why"] = _dg.get("why")
        except Exception:
            continue




def _next_trading_day9(_repo):
    """Shared navigation uses the earliest exchange session; each market is labelled separately."""
    from exchange_sessions import next_session
    from datetime import date
    d=min(next_session(date.today(),m) for m in ('A股','港股','美股'))
    return d,f"{d.strftime('%m-%d')}周{'一二三四五六日'[d.weekday()]}"


def _v88_upside_pct9(_d):
    """可买标的的上行空间%。用户纪律(2026-07-20)：短线推荐上行空间<10%不值得动、不推。
    实时决策有 upside_pct；黑马落盘从 阻力位/短线目标价/中线目标价 ÷ 现价推算；
    都没有→None（如实=空间不明，同样不进可买名单）。"""
    try:
        _u = float(_d.get("upside_pct") or 0)
        if _u:
            return _u
    except (TypeError, ValueError):
        pass
    try:
        _last = float(_d.get("last") or 0)
    except (TypeError, ValueError):
        _last = 0.0
    if _last <= 0:
        return None
    _cands = []
    try:
        _res = float(_d.get("resistance") or 0)
        if _res > 0:
            _cands.append((_res / _last - 1) * 100)
    except (TypeError, ValueError):
        pass
    for _leg in ("short", "mid"):
        _m = re.search(r"目标([\d.]+)",
                       str(((_d.get("trade_plan") or {}).get(_leg) or {}).get("out") or ""))
        if _m:
            try:
                _cands.append((float(_m.group(1)) / _last - 1) * 100)
            except ValueError:
                continue
    # 取各口径最大者=完整波段空间（短线10日目标常仅5%上下，不代表全部空间）
    return max(_cands) if _cands else None


def _v88_gate_breaker9(_type_key="entry_green"):
    """战绩熔断：该信号类型近30日实盘命中<40% → 整类停推（返回熔断说明，None=不熔断）。"""
    try:
        _t = (_v88_success9().get("types") or {}).get(_type_key) or {}
        if _t.get("rate") is not None and int(_t["rate"]) < 40:
            return (f"该类型近30日实盘命中仅{_t['rate']}%（n={_t.get('n', 0)}）<40%——"
                    "战绩熔断，整类停推直至命中回升（说话要算数：打不准就先闭嘴）")
    except Exception:
        pass
    return None


_V88_T_RULE9 = ("计划做T：竞价弱转强/开盘回封确认再接，当日冲高即了结不恋战，破昨收即止损，不转波段")


def _v88_t_plan9(_repo, _max_n=3):
    """【V88·计划做T 2026-07-20 用户定纲】做T可以做，但只推把握分≥90的、最多3只，必须明标"计划做T"。
    把握分(0-100·可解释零AI)：基45 ＋封单(≥5亿+25/≥2亿+15/≥1亿+8) ＋换手甜区3~15%+10
    ＋涨停主线板块共振+15 ＋近30日接力实盘命中≥60%再+10；
    硬校准：上限=60+实盘命中率÷2——战绩差时系统性压分(当前命中22%→上限71，
    意味着接力环境差的日子永远凑不出90分，如实空档宁缺毋滥，不硬凑3只)。
    返回 (入选列表≤3只, 一句口径说明)。"""
    import json as _jt
    try:
        _zt = _jt.loads((_repo / "data" / "limit_up_radar.json").read_text(encoding="utf-8"))
    except Exception:
        return [], "接力雷达数据缺失——今日无做T档"
    _rate = None
    try:
        _rt = (_jt.loads((_repo / "data" / "success_rates.json").read_text(encoding="utf-8"))
               .get("types") or {}).get("relay") or {}
        _rate = _rt.get("rate")
    except Exception:
        pass
    _cap = 60 + (float(_rate) / 2 if _rate is not None else 0)
    _mains = {str(m.get("industry")) for m in (_zt.get("mainlines") or [])[:3]}
    _out = []
    for _r in (_zt.get("relay") or []):
        _sc = 45.0
        _why = []
        try:
            _seal = float(_r.get("seal_yi") or 0)
        except (TypeError, ValueError):
            _seal = 0.0
        if _seal >= 5:
            _sc += 25
            _why.append(f"封单{_seal:.1f}亿·极强")
        elif _seal >= 2:
            _sc += 15
            _why.append(f"封单{_seal:.1f}亿·较强")
        elif _seal >= 1:
            _sc += 8
            _why.append(f"封单{_seal:.1f}亿")
        try:
            _to = float(_r.get("turnover") or 0)
        except (TypeError, ValueError):
            _to = 0.0
        if 3 <= _to <= 15:
            _sc += 10
            _why.append(f"换手{_to:.0f}%适中")
        _ind = str(_r.get("industry") or "")
        if _ind and _ind in _mains:
            _sc += 15
            _why.append(f"主线「{_ind}」共振")
        if _rate is not None and float(_rate) >= 60:
            _sc += 10
            _why.append(f"近期接力命中{_rate}%")
        _sc = min(_sc, _cap)
        _out.append({"name": _r.get("name"), "code": _r.get("code"),
                     "score": int(round(_sc)), "why": "、".join(_why) or "仅入榜无加分项"})
    _out = [x for x in sorted(_out, key=lambda x: -x["score"]) if x["score"] >= 90][:_max_n]
    _note = (f"准入:把握分≥90·最多3只·近30日接力实盘命中{_rate}%(把握分上限{_cap:.0f})"
             if _rate is not None else "准入:把握分≥90·最多3只·接力战绩样本积累中")
    return _out, _note





# 【V88·异动归因引擎 2026-07-17 用户定纲】大盘/板块/个股任一当天大异动→一句为什么。
# 三层共用同一套:新闻方向匹配(跌找利空/涨找利好)+带因果词优先+命中名称(市场/板块/个股)+中文。零AI。
_V88_CAUSE_WORDS = ("因", "由于", "受", "预期", "担忧", "导致", "引发", "拖累", "冲击",
                    "下调", "上调", "抛售", "避险", "回吐", "获利", "政策", "加息", "降息",
                    "利空", "利好", "财报", "业绩", "订单", "涨价", "减产", "制裁", "合作")


def _v88_fast_news9():
    """【V88·情报采集层 2026-07-18 用户点单"爬虫是我们的优势"】东财7x24快讯
    (免费JSON,机构观点/政策/公司急事,盘中机器速度)——30分钟缓存,国内直连零代理。
    并入桌面新闻池:凭什么定性/消息归因自动受益,出处标'东财7x24快讯'。"""
    import time as _t
    _c = _V88_NEWS_CACHE.setdefault("_fast", {"ts": 0, "rows": []})
    if _t.time() - _c["ts"] < 1800 and _c["rows"]:
        return _c["rows"]
    try:
        import requests as _rqf
        _sf = _shared_http_session(direct=True)
        _r = _sf.get("https://np-listapi.eastmoney.com/comm/web/getFastNewsList", timeout=10,
                     headers={"User-Agent": "Mozilla/5.0", "Referer": "https://kuaixun.eastmoney.com/"},
                     params={"client": "web", "biz": "web_724", "fastColumn": "102",
                             "sortEnd": "", "pageSize": 40, "req_trace": "1"})
        _rows = ((_r.json().get("data") or {}).get("fastNewsList")) or []
        _c["rows"] = [{"title": str(it.get("title") or it.get("summary") or ""),
                       "cleaned_title": str(it.get("title") or it.get("summary") or ""),
                       "analysis_summary": str(it.get("summary") or ""),
                       "source": "东财7x24快讯", "impact_direction": "",
                       "affected_tickers": "", "affected_sectors": "", "market_scope": ""}
                      for it in _rows if (it.get("title") or it.get("summary"))]
        _c["ts"] = _t.time()
    except Exception:
        _c["rows"] = _c.get("rows") or []
        _c["ts"] = _t.time()
    return _c["rows"]


def _v88_load_news():
    import time as _t
    if _t.time() - _V88_NEWS_CACHE["ts"] < 300 and _V88_NEWS_CACHE["news"]:
        return _V88_NEWS_CACHE["news"]
    try:
        _na = json.loads((core_root() / "data" /
                          "news_analyzed.json").read_text(encoding="utf-8"))
        # 近期消息必须保留原发布时间；缓存刷新不使旧闻变新。
        from news_evidence import current_news
        _V88_NEWS_CACHE["news"] = current_news(_na) + current_news({"news": _v88_fast_news9()})
        _V88_NEWS_CACHE["ts"] = _t.time()
    except Exception:
        _V88_NEWS_CACHE["news"] = []
    return _V88_NEWS_CACHE["news"]


def _v88_rating_moves():
    """近3日机构评级动作缓存（异动归因最高优先源）。"""
    import time as _t
    _c = _V88_NEWS_CACHE.setdefault("_ratings", {"ts": 0, "rows": []})
    if _t.time() - _c["ts"] < 600 and _c["rows"]:
        return _c["rows"]
    try:
        _d = json.loads((core_root() / "data" /
                         "institutional_signals.json").read_text(encoding="utf-8"))
        _c["rows"] = [r for r in ((_d.get("reports")) or _d.get("all_reports") or [])
                      if r.get("stock")]
        # 兼容:reports_n 版落盘无全量列表时,用共识+共振里的条目
        if not _c["rows"]:
            _c["rows"] = []
        _c["ts"] = _t.time()
    except Exception:
        _c["rows"] = []
    return _c["rows"]


def _v88_announcements():
    """【V88·公告事件雷达 2026-07-18】私仓落盘的自选/持仓池公告事件（10分钟内存缓存）。
    {canon_code: {name, code, items:[{date,title,dir,note,icon}]}}"""
    import time as _t
    _c = _V88_NEWS_CACHE.setdefault("_ann", {"ts": 0, "d": {}})
    if _t.time() - _c["ts"] < 600 and _c["d"]:
        return _c["d"]
    try:
        _c["d"] = (json.loads((core_root() / "data" /
                               "announcements.json").read_text(encoding="utf-8")).get("events")) or {}
    except Exception:
        _c["d"] = {}
    _c["ts"] = _t.time()
    return _c["d"]


def _v88_move_reason(chg, *, names=(), scope_hint="", max_len=34, require_name=False):
    """异动一句原因。chg=今日涨跌%; names=命中优先的名字(个股名/板块名/市场名);
    scope_hint=market_scope 关键词(A股/港股/美股)。|chg|<1.5 返回空。
    优先级:①机构评级变动(下调致跌/上调首予致涨=最精准因果)②新闻匹配。"""
    try:
        chg = float(chg)
    except (TypeError, ValueError):
        return ""
    if abs(chg) < 1.5:
        return ""
    # 【V88·评级变动归因 2026-07-18 用户批准】跌配下调、涨配上调/首次买入
    _nm_list = [str(n) for n in names if n and len(str(n)) >= 2]
    if _nm_list:
        for _r in _v88_rating_moves():
            if str(_r.get("stock")) not in _nm_list:
                continue
            _ch, _rt = str(_r.get("change") or ""), str(_r.get("rating") or "")
            if chg < 0 and (_ch == "下调" or any(k in _rt for k in ("减持", "卖出", "中性"))
                            and _ch in ("下调",)):
                return (f"{_r.get('org')}下调评级"
                        + (f"{_r.get('last_rating')}→{_rt}" if _r.get("last_rating") else f"至{_rt}")
                        + f"({_r.get('date', '')[5:]})所致")[:max_len]
            if chg > 0 and (_ch in ("上调", "首次") and any(k in _rt for k in ("买", "增持", "Buy"))):
                _tg = f"·目标{_r.get('target'):g}" if _r.get("target") else ""
                return (f"{_r.get('org')}{_ch}{_rt}评级{_tg}({_r.get('date', '')[5:]})提振")[:max_len]
    # 【V88·公告源 2026-07-18 久吾高科案】评级之后、媒体新闻之前——公司公告是强因果：
    # 方向事件须与涨跌同向(回购配涨/减持配跌)；⚡两面事件(可转债等)涨跌都可标注。
    if _nm_list:
        for _blk in _v88_announcements().values():
            if str(_blk.get("name")) not in _nm_list:
                continue
            for _a in (_blk.get("items") or []):
                _ad = _a.get("dir")
                if ((chg > 0 and _ad == "bull") or (chg < 0 and _ad == "bear")
                        or _ad == "event"):
                    return (f"{_a.get('icon', '')}公告:{str(_a.get('note') or _a.get('title'))[:14]}"
                            f"({str(_a.get('date', ''))[5:]}·出处:公司公告)")[:max_len]
    _want = "空" if chg < 0 else "好"
    _names = [str(n) for n in names if n]
    _best, _bsc = "", 0
    for _n in _v88_load_news():
        if _want not in str(_n.get("impact_direction") or ""):
            continue
        _txt = str(_n.get("cleaned_title") or _n.get("title") or "")
        if sum(1 for c in _txt if "一" <= c <= "鿿") < 4:
            _txt = str(_n.get("analysis_summary") or _txt)
        if not _txt:
            continue
        _blob = _txt + str(_n.get("affected_sectors") or "") + str(_n.get("affected_tickers") or "")
        _sc = 0
        _name_hit = bool(_names and any(_nm in _blob for _nm in _names))
        # 【V88·相关性硬门槛 2026-07-18 用户抓错配】个股/板块归因必须名称真命中——
        # 否则"中烟香港配全球科技股抛售"这类市场级新闻会被错当个股原因。命不中宁缺毋滥。
        if require_name and not _name_hit:
            continue
        if _name_hit:
            _sc += 6                          # 命中个股/板块名=最相关
        if scope_hint and scope_hint in str(_n.get("market_scope") or ""):
            _sc += 5
        elif ("综合" in str(_n.get("market_scope") or "") or "宏观" in str(_n.get("market_scope") or "")):
            _sc += 1
        if _sc == 0:
            continue                          # 与该标的/市场无关的不用
        _sc += (3 if "高" in str(_n.get("impact_level") or "")
                else (1 if "中" in str(_n.get("impact_level") or "") else 0))
        if any(_w in _txt for _w in _V88_CAUSE_WORDS):
            _sc += 3                          # 带因果="为什么"
        if sum(1 for c in _txt if "一" <= c <= "鿿") >= 4:
            _sc += 2
        if _sc > _bsc:
            _best, _bsc = _txt[:max_len], _sc
    return _best


def _v88_fund_edge(name, *, inst_only=False, max_len=34):
    """【V88·基本面必带定性 2026-07-18 用户点单·全系统】PE/PB数字之外必须有一句
    "凭什么"的行业优势或消息匹配，且机构信息必标出处：
    ①近3日机构研报标题（机构名+日期，出处=东财研报库）
    ②名称硬命中的近日新闻（带媒体名出处）
    ③都没有→如实说明，不编。inst_only=True 只回①（调用处已有自己的新闻行时用）。"""
    _nm = str(name or "").strip()
    if len(_nm) < 2:
        return ""
    try:
        for _rp in _v88_rating_moves():
            _st = str(_rp.get("stock") or "")
            if _st and (_st in _nm or _nm in _st):
                return (f"🏭 机构定性：{_rp.get('org')}「{str(_rp.get('title') or '')[:max_len]}」"
                        f"·评级{_rp.get('rating') or '—'}"
                        f"（{str(_rp.get('date') or '')[5:]}·出处:东财研报库）")
    except Exception:
        pass
    if inst_only:
        return ""
    try:
        # 名称命中里挑最可读的一条：中文文本>纯英文、标题直接命中>仅在关联标的里、方向明确加分
        _best, _bsc = None, 0
        for _n in _v88_load_news():
            _txt = str(_n.get("cleaned_title") or _n.get("title") or "")
            if sum(1 for c in _txt if "一" <= c <= "鿿") < 4:
                _alt = str(_n.get("analysis_summary") or "")
                if sum(1 for c in _alt if "一" <= c <= "鿿") >= 4:
                    _txt = _alt
            _blob = (_txt + str(_n.get("title") or "") + str(_n.get("affected_tickers") or "")
                     + str(_n.get("affected_sectors") or ""))
            if _nm not in _blob:
                continue
            _sc = 1
            if _nm in _txt:
                _sc += 4
            if sum(1 for c in _txt if "一" <= c <= "鿿") >= 4:
                _sc += 3
            _dir = str(_n.get("impact_direction") or "")
            if ("好" in _dir) or ("空" in _dir):
                _sc += 1
            if _sc > _bsc:
                _best, _bsc = (_txt, _dir, _n), _sc
        if _best:
            _txt, _dir, _n = _best
            _tag = "🔴利好" if "好" in _dir else ("🟢利空" if "空" in _dir else "⚪中性")
            _src = str(_n.get("source") or "").split(" - ")[0].strip()[:16] or "新闻流"
            return f"📰 消息定性：{_txt[:max_len]}（{_tag}·出处:{_src}）"
    except Exception:
        pass
    return "🏭 近3日无该股研报/直接新闻——行业优势以数字与走势为准（如实说明，不编）"


def _v88_stage_base9(stage):
    """【V88·今天锚点统一口径】阶段→现在热度基准位(与深度页象限/决策卡同一张表)。"""
    _s = str(stage or "")
    if any(k in _s for k in ("蓄势", "底部")):
        return 45
    if any(k in _s for k in ("领涨", "主升", "启动", "延续", "多头")):
        return 62
    if any(k in _s for k in ("派发", "滞涨", "高位")):
        return 55
    if any(k in _s for k in ("退潮", "破位", "转弱", "下跌")):
        return 38
    return 50


def _v88_fund_edge_short(name, max_len=22):
    """【V88·凭什么铁律 2026-07-18 用户定纲】任何数字结论旁的一句定性(≤22字):
    ①机构研报标题(出处:研报库) ②公告事件 ③名称命中新闻——都没有返回空,
    调用处必须明示"纯技术驱动"。零AI,全本地json。"""
    _nm = str(name or "").strip()
    if len(_nm) < 2:
        return ""
    try:
        for _rp in _v88_rating_moves():
            _st = str(_rp.get("stock") or "")
            if _st and (_st in _nm or _nm in _st):
                return f"{_rp.get('org')}:{str(_rp.get('title') or '')[:max_len]}(研报)"
    except Exception:
        pass
    try:
        for _blk in _v88_announcements().values():
            if str(_blk.get("name")) in _nm or _nm in str(_blk.get("name") or "×"):
                for _a in (_blk.get("items") or [])[:1]:
                    return f"{_a.get('icon', '')}{str(_a.get('note') or '')[:max_len]}(公告)"
    except Exception:
        pass
    try:
        for _n in _v88_load_news():
            _txt = str(_n.get("cleaned_title") or _n.get("title") or "")
            if _nm not in (_txt + str(_n.get("affected_tickers") or "")):
                continue
            if sum(1 for c in _txt if "一" <= c <= "鿿") < 4:
                _txt = str(_n.get("analysis_summary") or "")
            if sum(1 for c in _txt if "一" <= c <= "鿿") >= 4:   # 只要中文可读句,英文截断不如"纯技术"诚实
                return f"{_txt[:max_len]}(新闻)"
    except Exception:
        pass
    return ""


def _v88_success9():
    """【V88·统一战绩总账 2026-07-19 用户点单】各推荐类型的实盘成功率(私仓落盘,10分钟缓存)。"""
    import time as _t
    _c = _V88_NEWS_CACHE.setdefault("_succ", {"ts": 0, "d": {}})
    if _t.time() - _c["ts"] < 600 and _c["d"]:
        return _c["d"]
    try:
        _c["d"] = json.loads((core_root() / "data" /
                              "success_rates.json").read_text(encoding="utf-8"))
    except Exception:
        _c["d"] = {}
    _c["ts"] = _t.time()
    return _c["d"]


def _v88_rate_line9(key, label):
    """一句可挂在模块头的实盘成功率(诚实版:样本<5报积累中)。key不存在返回空。"""
    _t9 = (_v88_success9().get("types") or {}).get(key) or {}
    if not _t9:
        return ""
    _n9 = int(_t9.get("n") or 0)
    _r9 = _t9.get("rate")
    _avg9 = _t9.get("avg")
    if _r9 is None:
        return f"📊 {label}实盘成功率：样本积累中（{_n9}次·<5不报率，防小样本噪音）"
    # 【V88·发言权规则 2026-07-25 用户批准】<50%(n≥5)→模块降级"研究参考"标注,点名撤出第一屏
    _demote9 = ("　🔇已降级研究参考(战绩<50%·点名撤出第一屏,到期核算回升自动恢复)"
                if (_n9 >= 5 and int(_r9) < 50 and key != "hot_dual") else "")
    return (f"📊 {label}实盘成功率 {_r9}%（{_n9}次"
            + (f"·均{_avg9:+.1f}%" if _avg9 is not None else "")
            + f"·{_t9.get('note', '')}·非规则估计）" + _demote9)


def _v88_hot_note9(code):
    """【V88·推荐×热议点拨 2026-07-19 用户点单】推荐股恰好在热榜→括号就地点拨。
    双榜>雪球>东财>雅虎;语义=情绪助燃但拥挤,提醒纪律,不是加分项。"""
    try:
        _d = _v88_intel9()
        _cn = _canonical_code(str(code or ""))
        _h = (_d.get("hot_map") or {}).get(_cn)
        if _h and _h.get("xq"):
            return f"（🔥🔥东雪双榜热议一致·东#{_h['rank']}雪#{_h['xq']}——情绪助燃但拥挤,严守作废价）"
        _xq = (_d.get("xq_map") or {}).get(_cn)
        if _xq:
            return f"（🔥雪球热议一致·雪#{_xq}——散户关注升温,防冲高回落）"
        if _h:
            return f"（🔥东财人气一致·东#{_h['rank']}）"
        _us = (_d.get("us_map") or {}).get(str(code or "").upper().split(".")[0])
        if _us:
            return f"（🔥雅虎热搜一致·#{_us}）"
    except Exception:
        pass
    return ""


def _v88_market_edge(market):
    """大盘级定性:近3日策略/宏观研报标题按市场关键词命中(出处:东财研报库),
    命不中如实空——调用处明示"纯量价驱动"。10分钟缓存。"""
    import time as _t
    _c = _V88_NEWS_CACHE.setdefault("_mkt_edge", {"ts": 0, "d": {}})
    if _t.time() - _c["ts"] > 600:
        try:
            _c["d"] = {"titles": (json.loads(
                (core_root() / "data" /
                 "institutional_signals.json").read_text(encoding="utf-8"))
                .get("strategy_titles")) or []}
        except Exception:
            _c["d"] = {"titles": []}
        _c["ts"] = _t.time()
    _kw = {"美股": ("美股", "纳指", "标普", "美联储", "降息", "美国", "海外"),
           "A股": ("A股", "政策", "央行", "财政", "两市", "国内", "中国资产"),
           "港股": ("港股", "恒生", "中概", "香港")}.get(str(market)[-2:], ())
    for _t9 in _c["d"].get("titles") or []:
        if any(k in str(_t9) for k in _kw):
            return f"🏛️{str(_t9)[:30]}（出处:东财研报库·近3日策略报告）"
    # 【情报二期】策略研报没提→政策原文兜底(中国市场;美股不适用)
    if str(market)[-2:] in ("A股", "港股"):
        for _p9 in (_v88_intel9().get("policy") or []):
            if _p9.get("src") == "发改委":
                return (f"📜政策:{str(_p9.get('title'))[:26]}"
                        f"（{str(_p9.get('date'))[5:]}·出处:发改委官网直采）")
    return ""


def _v88_mkt_why9(mk, _repo):
    """【V88·大盘why带出处 2026-07-21 用户点单"原因要下划线可点出自的新闻链接,美股港股也要有"】
    返回可点击的"今天为什么"HTML片段,三级兜底保证三市场都有：
    ①AI异动归因(src_idx回填新闻→下划线链接) ②新闻直配(该市场高影响Top1,带链接,明标非AI)
    ③策略研报/政策定性 ④如实"无显著消息"。10分钟缓存。"""
    import time as _t
    _c = _V88_NEWS_CACHE.setdefault("_mkt_why", {"ts": 0, "d": {}})
    if _t.time() - _c["ts"] > 600:
        _c["d"], _c["ts"] = {}, _t.time()
    if mk in _c["d"]:
        return _c["d"][mk]
    _out = ""
    _ma, _r = {}, {}
    try:
        _ma = json.loads((_repo / "data" / "move_attribution.json").read_text(encoding="utf-8"))
        _r = ((_ma.get("reasons") or {}).get(mk) or {}) if _ma.get("status") == "completed" else {}
        if _r.get("why"):
            _src = _r.get("src") or {}
            if _src.get("u"):
                _out = (f"<a href='{_src['u']}' target='_blank' "
                        f"style='color:inherit;text-decoration:underline'>{str(_r['why'])[:40]}</a>"
                        f"<span style='font-size:11px;color:#94a3b8'>"
                        f"（出处:{_src.get('s') or '新闻'}·点击看原文）</span>")
            else:
                _out = (f"{str(_r['why'])[:40]}"
                        f"<span style='font-size:11px;color:#94a3b8'>（出处:历史异动归因记录·需核源时点）</span>")
    except Exception:
        pass
    if not _out:
        try:   # ②新闻直配:该市场scope命中+高影响的最新一条,带原文链接,明标非AI归因
            from news_evidence import current_news, news_note_html
            _ns = current_news(json.loads((_repo / "data" / "news_analyzed.json").read_text(encoding="utf-8")))
            for _n in _ns:
                _sc = str(_n.get("market_scope") or "")
                if (mk in _sc or "宏观" in _sc or "综合" in _sc) and "高" in str(_n.get("impact_level") or ""):
                    _tt = str(_n.get("cleaned_title") or _n.get("title") or "")[:36]
                    _uu = str(_n.get("link") or "")
                    if _tt:
                        _out = ((f"<a href='{_uu}' target='_blank' "
                                 f"style='color:inherit;text-decoration:underline'>{_tt}</a>" if _uu else _tt)
                                + f"<span style='font-size:11px;color:#94a3b8'>"
                                  f"（新闻关联·非因果·{str(_n.get('source') or '')[:12]}；{news_note_html(_n)}）</span>")
                        break
        except Exception:
            pass
    if not _out:
        _out = _v88_market_edge(mk) or ""
    if not _out:
        _out = "尚无足够新鲜的消息证据；量价分不能证明消息驱动"
    if _out and _ma.get("status") == "completed" and _r.get("why"):
        _out = "历史归因线索 · " + str(_ma.get("generated_at") or "原时间待核") + " · " + _out
    _c["d"][mk] = _out
    return _out


# (_v88_sentinel9 已前移至文件顶部——2026-07-31修:A段调用D段定义,分页版哨兵永久NameError)


# (_v88_usage9 已前移至文件顶部前置定义区——2026-07-31修使用先于定义)

def _v88_phase_slot9():
    """【V88·三时段换算 2026-07-24 用户点单"一天盘中三次刷新,注意中美港时段不同"】
    北京时间三槽:09:15后=cn-am(A/港早盘) 14:30后=cn-pm(A/港尾盘·港still盘中)
    21:45后=us(美股盘,跨午夜00:00-09:15归前一天us槽)。槽变=该刷新。"""
    from datetime import timedelta as _td9s
    _now = datetime.now()
    _hm = _now.hour * 60 + _now.minute
    if _hm < 9 * 60 + 15:
        return (_now - _td9s(days=1)).strftime("%Y-%m-%d") + "-us"
    if _hm < 14 * 60 + 30:
        return _now.strftime("%Y-%m-%d") + "-cn-am"
    if _hm < 21 * 60 + 45:
        return _now.strftime("%Y-%m-%d") + "-cn-pm"
    return _now.strftime("%Y-%m-%d") + "-us"


def _v88_phase_turn_full9(_repo, force=False):
    """【V88·全池双向相位扫描 2026-07-24 用户点单"要在最全股票池960只里筛选+要有低谷转强"】
    扫 RAW_US+RAW_HK+RAW_CN_TOP 全市场大池,找相位切换股:direction=down(顶拐/退潮·转弱)
    + direction=up(低谷→启动·转强)。8线程并发,落盘 data/phase_turn_full.json。
    缓存口径=三时段槽(09:15/14:30/21:45北京,中美港各自盘中一刷)——槽没变就用缓存。
    force=False 只读缓存(秒开,无缓存返回None);force=True 实扫(首扫5-15分钟)。"""
    _fp = _repo / "data" / "phase_turn_full.json"
    _cached = None
    try:
        _cached = json.loads(_fp.read_text(encoding="utf-8"))
    except Exception:
        _cached = None
    if not force:
        return _cached      # 槽新旧由调用处判定(旧槽→起后台线程刷,页面先显旧数据不卡)
    import sys as _sy9p
    if str(_repo / "src") not in _sy9p.path:
        _sy9p.path.insert(0, str(_repo / "src"))
    from cloud_engine import fetch as _fetch9p, analyze_trend_full as _atf9p
    from stock_cycle import cycle_phase as _cph9p
    from concurrent.futures import ThreadPoolExecutor as _TPE9p
    _pool9p, _seen9p = [], set()
    for _grp9p in (RAW_US, RAW_HK, RAW_CN_TOP):
        for _it9p in _grp9p:
            _c9p = _it9p[2] if len(_it9p) > 2 else _it9p[0]
            if _c9p and _c9p not in _seen9p:
                _seen9p.add(_c9p)
                _pool9p.append((_c9p, _it9p[1]))

    def _one9p(_item9p):
        _c9p, _n9p = _item9p
        try:
            _df9p = _fetch9p(_c9p)
            if _df9p is None or len(_df9p) < 60:
                return None
            _r9p = _cph9p(_atf9p(_df9p))
            if not _r9p or _r9p.get("direction") not in ("up", "down"):
                return None
            return {"code": _c9p, "name": _n9p, "direction": _r9p.get("direction"),
                    "phase": _r9p.get("phase"), "confidence": _r9p.get("confidence"),
                    "pos52": _r9p.get("pos52"),
                    "strength": round(max(float(_r9p.get("up") or 0),
                                          float(_r9p.get("down") or 0)), 1)}
        except Exception:
            return None
    _out9p = []
    with _TPE9p(max_workers=8) as _ex9p:
        for _r9p in _ex9p.map(_one9p, _pool9p):
            if _r9p:
                _out9p.append(_r9p)
    _d9p = {"ts": time.time(), "scanned": len(_pool9p),
            "slot": _v88_phase_slot9(),
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "stocks": _out9p}
    try:
        _fp.write_text(json.dumps(_d9p, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass
    return _d9p


def _v88_sector_edge9(label):
    """【V88·板块定性 2026-07-18 用户点单"轮动全是数学模型"】复用私仓 rotation_forecast.
    sector_edge(行业研报>板块新闻,带出处);30分钟缓存;空=调用处标'纯量价模型'。"""
    import time as _t
    _c = _V88_NEWS_CACHE.setdefault("_sec_edge", {"ts": 0, "d": {}})
    if _t.time() - _c["ts"] > 1800:
        _c["d"], _c["ts"] = {}, _t.time()
    _k = str(label)
    if _k in _c["d"]:
        return _c["d"][_k]
    try:
        import sys as _sy9e
        _p9e = str(core_root() / "src")
        if _p9e not in _sy9e.path:
            _sy9e.path.insert(0, _p9e)
        from rotation_forecast import sector_edge as _se9e
        # 标签形如"医药·A/科技·美"——取·前主体再查(私仓别名表按纯板块名收录)
        _c["d"][_k] = _se9e(_k.strip("🇺🇸🇨🇳🇭🇰 ").split("·")[0].strip())
    except Exception:
        _c["d"][_k] = ""
    return _c["d"][_k]


def _render_today_verdict(_snap, _repo):
    """Read the linked observation panel; no hidden legacy decision engine."""
    # Published discovery -> verified local prices -> sector context -> central
    # authority. Reading this panel never launches market scans or model calls.
    try:
        from next_session_data import load_signals as _ns_load
        from next_session_model import build as _ns_build
        from next_session_view import render as _ns_render
        from stock_profile_view import load as _ns_profiles_load
        from scanner_central import _load_current_snapshot as _ns_central_load
        from datetime import timezone as _ns_timezone
        _ns_now = datetime.now(_ns_timezone.utc)
        _ns_phase = _v88_phase_turn_full9(_repo) or {}
        if not _ns_phase.get("stocks"):
            _ns_phase = dict((_snap or {}).get("cycle_scan") or {})
            _ns_phase["generated_at"] = _ns_phase.get("analysis_time")
            _ns_phase["scanned"] = len(_ns_phase.get("stocks") or [])
        _ns_signals = _ns_load(_ns_phase, now=_ns_now)
        _ns_profiles = _ns_profiles_load()
        try:
            _ns_central = _ns_central_load(_ns_now)
        except Exception:
            logging.warning("联动观察无法核实中央发布", exc_info=True)
            _ns_central = {}
        _ns_doc = _ns_build(_ns_signals, (_snap or {}).get("rotation_forecast") or {},
                            central=_ns_central, profiles=_ns_profiles, now=_ns_now)
        st.html(_ns_render(_ns_doc, _ns_profiles))
    except Exception:
        logging.exception("下一交易日联动观察加载失败")
        st.warning("联动观察暂未完成数据核验，请查看3A中央列表；原评级和交易条件继续保留。")


def _render_l3_cycle_board(_snap, _is_trading):
    """【V88·三层周期概率总览】大盘→板块同屏（自选=上方决策台，共三层）。

    北极星定纲(2026-07-16)：三层都要看到轮转周期与下一周期方向概率（中美港）。
    口径：大盘=统一引擎 2/4/8/16/32 周方向分（与自选决策台完全同源）；
    板块=rotation_forecast 轨迹 2/5/8/16 周（09:00/21:00 交易日更新）。
    纯确定性引擎，零 LLM 成本；概率=规则情景估计（非回测胜率）。
    """
    _rot9 = (_snap or {}).get("rotation_forecast") or {}
    _traj9 = _rot9.get("trajectories") or {}
    _key9, _ttl9 = "_l3_board9", (1800 if _is_trading else 3600)
    _cache9 = st.session_state.get(_key9) or {}
    if not _cache9.get("mkts") or time.time() - _cache9.get("ts", 0) > _ttl9:
        _idx_map9 = {"美股": ("标普500", "^GSPC"), "A股": ("上证指数", "000001.SS"),
                     "港股": ("恒生指数", "^HSI")}
        _mkts9 = {}
        try:
            from v88_decision_core import evaluate_decision as _ed9
            import cloud_engine as _ce9
            for _mk9, (_nm9, _sym9) in _idx_map9.items():
                try:
                    _df9 = fetch_stock_data(_sym9)
                    if _df9 is None or len(_df9) < 40:
                        continue
                    try:
                        _full9 = _ce9.analyze_trend_full(_df9) or {}
                    except Exception:
                        _full9 = {}
                    _dc9 = _ed9(_df9, _full9, name=_nm9, code=_sym9)
                    if _dc9.get("error"):
                        continue
                    _hz9 = ((_dc9.get("facts") or {}).get("horizons") or {})
                    _probs9 = [(_lab9, int(round(float((_hz9.get(_lab9) or {}).get("rule_score")))))
                               for _lab9 in ("2周", "4周", "8周", "16周", "32周")
                               if (_hz9.get(_lab9) or {}).get("rule_score") is not None]
                    _mkts9[_mk9] = {"name": _nm9, "stage": str(_full9.get("stage") or "—"),
                                    "action": str(_dc9.get("action") or "观察"), "probs": _probs9}
                except Exception:
                    continue
        except Exception:
            logging.debug("三层总览大盘层计算失败", exc_info=True)
        if _mkts9:
            _cache9 = {"ts": time.time(), "mkts": _mkts9}
            st.session_state[_key9] = _cache9
    _mkts9 = _cache9.get("mkts") or {}
    if not _mkts9 and not _traj9:
        return

    def _pcol9(_p):
        return "#dc2626" if _p >= 55 else ("#16a34a" if _p <= 45 else "#64748b")

    def _chain9(_probs, _now9c=None):
        if not _probs:
            return "<span style='color:#94a3b8'>计算中</span>"
        # 【V88·逐点方向符号+现在锚点 2026-07-19 用户点单】链首灰「现在」=阶段基准+动量
        # 实算起点(非预测),2周箭头相对现在;之后每档相对前一档标↑↓≈。
        _parts9c = []
        _prev9c = None
        if _now9c is not None:
            _parts9c.append(f"<span style='color:#94a3b8'>现在<b>{int(_now9c)}</b></span>")
            _prev9c = _now9c
        for lab, p in _probs:
            _ar9c = ""
            if _prev9c is not None:
                _dd9c = p - _prev9c
                _ar9c = ("<span style='color:#dc2626'>↑</span>" if _dd9c >= 1 else
                         ("<span style='color:#16a34a'>↓</span>" if _dd9c <= -1 else
                          "<span style='color:#94a3b8'>≈</span>"))
            _parts9c.append(f"<span style='color:{_pcol9(p)}'>{lab}<b>{p}</b></span>{_ar9c}")
            _prev9c = p
        _seg9 = " ".join(_parts9c)
        # 【V88·相位一致性】"越远越强"必须末档真强（≥59）才说，末档在中性带只说趋中性
        _d9 = _probs[-1][1] - _probs[0][1]
        _last9v = _probs[-1][1]
        _arrow9 = ("<b style='color:#dc2626'>↗越远越强</b>" if (_d9 >= 8 and _last9v >= 59) else
                   ("<b style='color:#16a34a'>↘越远越弱</b>" if (_d9 <= -8 and _last9v <= 41) else
                    ("<span style='color:#94a3b8'>↗趋中性</span>" if _d9 >= 8 else
                     ("<span style='color:#94a3b8'>↘趋中性</span>" if _d9 <= -8 else
                      "<span style='color:#94a3b8'>→均衡</span>"))))
        return f"{_seg9}　{_arrow9}"

    # 【V88·大盘异动即时横幅 2026-07-17 用户点单】大跌日必须一眼看到"跌了多少+为什么"。
    # 今日涨跌独立于30分钟卡片缓存,每次渲染现算(fetch有15分钟缓存+60秒实时价,准实时零流量)。
    _live_chg9 = {}
    try:
        for _mk9x, (_nm9x, _sym9x) in {"美股": ("标普500", "^GSPC"), "A股": ("上证指数", "000001.SS"),
                                       "港股": ("恒生指数", "^HSI")}.items():
            try:
                _dfx9 = fetch_stock_data(_sym9x)
                _cx9 = _dfx9["Close"]
                _live_chg9[_mk9x] = (float(_cx9.iloc[-1]) / float(_cx9.iloc[-2]) - 1) * 100
            except Exception:
                continue
        # 【V88·三市场今日综述 2026-07-25 用户点单"只有A股没营养——中美港都要今日+四档+原因写清"】
        # 常显(不再只异动日才有):每市场=今日涨跌+领涨领跌板块+❓为什么(带原文链接三级兜底永不空)
        # +明日/本周/本月/下月四档概率(与🔮四档预判同口径)。异动市场行底色高亮。
        _repo9s = core_root()

        def _pc9s(_p):
            return "#dc2626" if _p >= 55 else ("#16a34a" if _p <= 45 else "#64748b")
        _sum_rows9 = []
        for _mk9y in ("美股", "A股", "港股"):
            _blk9s = ((_snap or {}).get("markets") or {}).get(_mk9y) or {}
            _cg9s = _live_chg9.get(_mk9y)
            if _cg9s is None:
                continue
            _cgc9s = "#dc2626" if _cg9s > 0.05 else ("#16a34a" if _cg9s < -0.05 else "#64748b")
            _secs9s = _blk9s.get("sectors") or []
            _lead_txt9 = ""
            if _secs9s:
                _up2s = sorted(_secs9s, key=lambda s: -(s.get("chg1d") or 0))[:2]
                _dn2s = sorted(_secs9s, key=lambda s: (s.get("chg1d") or 0))[:2]
                _dn_word9 = "领跌" if (_dn2s and (_dn2s[0].get("chg1d") or 0) < 0) else "较弱"
                _up_word9 = "领涨" if (_up2s and (_up2s[0].get("chg1d") or 0) > 0) else "较强"
                _lead_txt9 = (f"{_up_word9}:" + "、".join(f"{s['name']}{(s.get('chg1d') or 0):+.1f}%" for s in _up2s)
                              + f"｜{_dn_word9}:" + "、".join(f"{s['name']}{(s.get('chg1d') or 0):+.1f}%" for s in _dn2s))
            _l39s = dict((x[0], x[1]) for x in ((_blk9s.get("l3") or {}).get("probs") or []))
            _t9s = float((_blk9s.get("temperature") or {}).get("temp") or 50)
            _tm9s = int(round(max(25, min(75, 50 + 2.2 * _cg9s + 0.15 * (_t9s - 50)))))
            _fc_cells9 = [f"明日<b style='color:{_pc9s(_tm9s)}'>{_tm9s}%</b>"]
            for _lb9s, _hz9s in (("本周", "2周"), ("本月", "4周"), ("下月", "8周")):
                _p9s = _l39s.get(_hz9s)
                if _p9s is not None:
                    _fc_cells9.append(f"{_lb9s}<b style='color:{_pc9s(int(_p9s))}'>{int(_p9s)}%</b>")
            _hl9s = abs(_cg9s) >= 1.5
            _rowbg9 = ("background:#f0fdf4;" if (_hl9s and _cg9s < 0) else
                       ("background:#fef2f2;" if _hl9s else ""))
            # 【合并2026-07-25】原①大盘怎么看的独有信息并入:温度→仓位/阶段→短期结论/转向风险
            _tp9s = _blk9s.get("temperature") or {}
            _pos_txt9 = (f"温度{_tp9s.get('temp', '?')}°({str(_tp9s.get('label', '')).strip()})"
                         f"→仓位{str(_tp9s.get('position', '?')).split('（')[0]}")
            _l3n9s = _blk9s.get("l3") or {}
            _p29w = _l39s.get("2周")
            _word9w = ("短期偏涨" if (_p29w or 50) >= 58 else
                       ("短期偏弱·反弹先看反抽不追" if (_p29w or 50) <= 42 else "短期震荡·区间对待"))
            _wc9w = ("#dc2626" if (_p29w or 50) >= 58 else
                     ("#16a34a" if (_p29w or 50) <= 42 else "#64748b"))
            _stage_txt9 = (f"｜{_l3n9s.get('name') or '指数'}「{_l3n9s.get('stage') or '—'}」"
                           f"→<b style='color:{_wc9w}'>{_word9w}</b>")
            _tr9w = _blk9s.get("turn_risk") or {}
            _turn9w = ""
            if int(_tr9w.get("top_risk") or 0) >= 40:
                _turn9w = f"　<b style='color:#b45309'>⚠️顶部转向风险{_tr9w['top_risk']}/100·冲高别加仓</b>"
            elif int(_tr9w.get("bottom_opp") or 0) >= 40:
                _turn9w = f"　<b style='color:#2563eb'>🔭底部转机{_tr9w['bottom_opp']}/100·留意右侧企稳</b>"
            _sum_rows9.append(
                f"<div style='{_rowbg9}border-radius:6px;padding:2px 6px;margin:1px 0;font-size:13px'>"
                f"{'🚨' if _hl9s else '📊'} <b>{_mk9y}</b> <b style='color:{_cgc9s}'>{_cg9s:+.2f}%</b>"
                f"<span style='font-size:12px;color:#475569'>·{_pos_txt9}{_stage_txt9}</span>"
                f"<br><span style='font-size:12px;color:#475569'>{_lead_txt9}</span>"
                f"　{' '.join(_fc_cells9)}{_turn9w}"
                f"<br><span style='font-size:12px;color:#64748b'>└ ❓今日为什么:"
                + _v88_mkt_why9(_mk9y, _repo9s) + "</span></div>")
        if _sum_rows9:
            st.markdown(
                "<div style='border:1px solid #dbe4f0;border-left:4px solid #2563eb;border-radius:8px;"
                "padding:.5rem .7rem;margin-bottom:.4rem'>"
                "<b style='font-size:13px'>📰 三市场今日综述</b>"
                "<span style='font-size:11px;color:#94a3b8'>（今日涨跌+领涨领跌+为什么·下划线点击看原文"
                "+明日/本周/本月/下月概率·与🔮四档预判同口径）</span>"
                + "".join(_sum_rows9) + "</div>", unsafe_allow_html=True)
    except Exception:
        _v88_sentinel9(core_root(), "三市场今日综述")

    st.markdown(
        "<span style='font-size:12px;color:#64748b'>"
        "数字=该周期<b>上涨概率%</b>（规则情景估计，非胜率）：<span style='color:#dc2626'>红≥55偏涨</span>／"
        "<span style='color:#16a34a'>绿≤45偏跌</span>／<span style='color:#64748b'>灰=中性</span>。"
        "链首灰字「现在」=当前阶段基准+近日动量<b>实算起点</b>（非预测），2周的箭头=相对「现在」；"
        "数字后小箭头=<b>较前一档</b>升↑/降↓/平≈（看每段涨落）；末尾大箭头=整条链总结（越远越强/弱/趋中性），"
        "与左侧「阶段·动作」是两件事：动作说<b>现在能不能买</b>、"
        "箭头说<b>越往后越强还是越弱</b></span>",
        unsafe_allow_html=True)
    _cols9 = st.columns(3)
    for _ci9, _mk9 in enumerate(("美股", "A股", "港股")):
        with _cols9[_ci9]:
            _rows9 = []
            _m9 = _mkts9.get(_mk9)
            if _m9:
                _lc9 = _live_chg9.get(_mk9)
                _lc_html9 = ""
                if _lc9 is not None:
                    _lc_col9 = "#dc2626" if _lc9 > 0.05 else ("#16a34a" if _lc9 < -0.05 else "#64748b")
                    _lc_html9 = f" <b style='color:{_lc_col9}'>今日{_lc9:+.2f}%</b>"
                _rows9.append(
                    f"<div style='margin-bottom:3px'>📈 <b>{_m9['name']}</b>{_lc_html9} "
                    f"<span style='color:#475569'>{_m9['stage']}</span> · {_m9['action']}<br>"
                    f"<span style='font-size:12px'>{_chain9(_m9['probs'], max(20, min(80, _v88_stage_base9(_m9.get('stage')) + max(-6.0, min(6.0, float(_lc9 or 0))) * 1.2)))}</span></div>")
            _tl9 = sorted((_traj9.get(_mk9) or []),
                          key=lambda t: -((t.get("points") or {}).get("2周") or {}).get("score", 0))
            for _t9, _flag9 in ([(_tl9[0], "🔥")] if _tl9 else []) + \
                               ([(_tl9[-1], "🧊")] if len(_tl9) > 2 else []):
                _pts9 = _t9.get("points") or {}
                _probs9 = [(k, int((_pts9.get(k) or {}).get("score", 0)))
                           for k in ("2周", "5周", "8周", "16周") if _pts9.get(k)]
                _trg9 = str((_pts9.get("2周") or {}).get("trigger") or _t9.get("reason") or "")[:18]
                # 【V88·板块异动归因】板块今日大异动时,用新闻替动量trigger说"为什么"
                _sec_chg9 = float((_t9.get("facts") or {}).get("1d", 0) or 0)
                _sec_reason9 = _v88_move_reason(_sec_chg9, names=[_t9.get("name", "")], max_len=22, require_name=True)
                _trg_disp9 = (f"📌{_sec_reason9}" if _sec_reason9 else _trg9)
                _rows9.append(
                    f"<div style='margin-bottom:3px'>{_flag9} <b>{_t9.get('name')}</b> "
                    f"<span style='font-size:12px;color:{'#b91c1c' if _sec_reason9 else '#64748b'}'>{_trg_disp9}</span><br>"
                    f"<span style='font-size:12px'>{_chain9(_probs9, _t9.get('now'))}</span></div>")
            if _rows9:
                st.markdown(
                    f"<div style='border:1px solid #dbe4f0;border-radius:8px;padding:7px 9px;"
                    f"background:#fff;box-shadow:0 1px 2px rgba(15,23,42,.04)'>"
                    f"<div style='font-size:12px;color:#334155;margin-bottom:4px'><b>{_mk9}</b></div>"
                    + "".join(_rows9) + "</div>", unsafe_allow_html=True)
    _ts9 = _rot9.get("analysis_time") or _cache9.get("ts")
    try:
        _ts_txt9 = (datetime.fromtimestamp(float(_ts9)).strftime("%H:%M") if isinstance(_ts9, (int, float))
                    else str(_ts9)[:16])
    except Exception:
        _ts_txt9 = "—"
    st.caption(f"🕒 大盘层实时算（{'30分钟' if _is_trading else '1小时'}缓存）｜板块层随轮动预测更新（{_ts_txt9}）")


def _render_today_nav():
    _repo = core_root()
    # ?q= 深链：点击任何内联个股名到达这里
    try:
        _q0 = st.query_params.get("q")
        _focus0 = st.query_params.get("focus")
        if _q0 and _focus0 == "deep":
            # 同一只股票再次点击也必须重新定位；该请求只消费一次，避免
            # 用户在深度报告内操作组件时页面反复跳回报告顶部。
            st.session_state["_deep_scroll_pending"] = str(_q0)
            try:
                del st.query_params["focus"]
            except Exception:
                pass
        if _q0 and st.session_state.get("_q_done") != _q0:
            st.session_state["_q_done"] = _q0
            _nm0 = _q0
            try:
                import cloud_engine as _ceq0
                _nm0 = _ceq0.name_of(_ceq0.to_yf(_q0)) or _q0
            except Exception:
                pass
            _search_history_persist(_q0, _nm0)
            _watchlist_add(_q0, _nm0)
            st.session_state.scan_selected_code = _q0
            st.session_state.scan_selected_name = _nm0
            st.toast(f"🔍 {_nm0} 深度分析中（已入重点观察）", icon="⭐")
        # 【2026-07-18 用户抓bug】?q= 消费后必须从地址栏删掉（focus早就删了,q漏了）——
        # 否则每次刷新/快捷方式重开都是新会话,_q_done清零,残留的 ?q=SO 反复触发
        # 同一只股的深度分析("怎么老跳SO")。深链=一次性指令,用完即焚。
        if _q0:
            try:
                del st.query_params["q"]
            except Exception:
                pass
    except Exception:
        pass
    try:
        _snap = json.loads((_repo / "data" / "market_snapshot.json").read_text(encoding="utf-8"))
    except Exception:
        _snap = None
    # 【V88·全站统一Plan A/B】不再绕过质检直读文件——首页/简报/云端三处共用同一份数据+同一套状态。
    # 之前的bug：这里曾直接 read_text 无视质检结果，简报模块却拦截未过质检的报告，
    # 导致"简报说数据源不足停止展示"而首页仍在显示同一份报告里的评分——现在统一为同一数据源。
    _rep, _rep_planab_meta = _load_report_planab()
    _rep = _rep or ""

    def _analysis_label9(_ts, _what="分析"):
        """重点提示统一显示分析发生时间；缓存展示时绝不冒充当前刷新时间。"""
        try:
            if isinstance(_ts, (int, float)):
                _dt9 = datetime.fromtimestamp(float(_ts))
            else:
                _raw9 = str(_ts or "").strip().replace("Z", "+00:00")
                _dt9 = datetime.fromisoformat(_raw9) if _raw9 else None
                if _dt9 and _dt9.tzinfo:
                    from datetime import timezone as _tz9, timedelta as _td9
                    _dt9 = _dt9.astimezone(_tz9(_td9(hours=8))).replace(tzinfo=None)
            return (f"🕒 {_what}于 {_dt9.strftime('%Y-%m-%d %H:%M')}（北京时间）"
                    if _dt9 else f"🕒 {_what}时间未知")
        except Exception:
            return f"🕒 {_what}时间未知"

    _report_analysis_ts9 = (_rep_planab_meta.get("generated_at")
                            or (_snap or {}).get("generated_at")
                            or _rep_planab_meta.get("ts"))
    _report_analysis_note9 = _analysis_label9(_report_analysis_ts9)

    # 【V88·非交易日判定】周末/节假日：无"今日盘中"，改看"下一交易日前瞻"
    def _v88_is_trading_day(_d=None):
        from exchange_sessions import is_session
        from datetime import date
        try:return any(is_session(_d or date.today(),market) for market in ('A股','港股','美股'))
        except ValueError:return False

    _is_trading = _v88_is_trading_day()
    try:
        from exchange_sessions import next_labels
        st.caption("三地关注交易日（当地日期）："+next_labels())
    except ValueError:
        st.caption("新年度交易所日历待核对；不推断交易日期。")

    # 【V88·防跳动 2026-07-21 用户点单"页面老跳动"】原30分钟自动整页重跑=阅读中突然跳回顶部。
    # 改为：数据过期时 fragment 原地亮一个"点此更新"按钮（只动这一小块，不打断阅读），
    # 用户点了才整页刷新。非交易日不查（省流量定则 2026-07-16）。
    @st.fragment(run_every=300)
    def _auto_refresh_tick9():
        try:
            if not _is_trading:
                return
            _wa0 = st.session_state.get("watch_alerts_v88") or {}
            if _wa0.get("ts") and time.time() - float(_wa0["ts"]) > 30 * 60:
                _age_m9 = int((time.time() - float(_wa0["ts"])) / 60)
                if st.button(f"🔄 数据已 {_age_m9} 分钟未更新 · 点此刷新（防跳动：不再自动整页重跑）",
                             key="btn_stale_refresh9", type="secondary"):
                    st.rerun(scope="app")
        except Exception:
            pass
    _auto_refresh_tick9()

    # Viewing/refreshing a page reads published results only. Scheduled
    # maintenance owns collection/review; never launch the legacy report/push
    # shell from a stale file mtime or a browser session.
    _stale_note = ""

    # The linked observation board supplies its own single section title.



    # 【V88·轮动挂钩仓位】持仓/自选正好踩今日涨停主线 → 导航顶部醒目高亮（2026-07-16 用户点单）
    try:
        _zt_hits9 = (json.loads((_repo / "data" / "limit_up_radar.json").read_text(encoding="utf-8"))
                     .get("watch_hits") or [])
        if _zt_hits9:
            _hl9 = "；".join(
                f"{'💼' if h['source']=='持仓' else '👁'}{h['name']}属主线**{h['industry']}**"
                f"({h['count']}只涨停·{h['max_boards']}板)" for h in _zt_hits9[:4])
            st.markdown(
                f"<div style='background:#fef3c7;border-left:4px solid #f59e0b;border-radius:8px;"
                f"padding:.6rem .8rem;font-size:13px;margin:.3rem 0'>⭐ <b>你的仓位正踩今日主线</b>："
                f"{_linkify_md(_hl9)}　<span style='color:#92400e;font-size:12px'>"
                f"轮动共振=短线动能，但别因涨停情绪改变原有纪律</span></div>",
                unsafe_allow_html=True)
    except Exception:
        pass

    # 【V88·今日总决断】第一眼给"今天该干什么"——定调/可进/持仓要处理/纪律,治"满屏观察=没推荐"
    try:
        _render_today_verdict(_snap, _repo)
    except Exception:
        logging.debug("今日总决断渲染失败", exc_info=True)

    # 【V88·第一屏自选决策台】先占住标题下方的位置，扫描完成后再回填。
    # 这样无需重复请求行情，也能让“我的自选”真正排在大盘、日报和持仓之前。
    # 【两池归一 2026-07-18】原第一屏独立slot废弃,自选台直接进⭐自选模块容器

    def _render_front_watch_add9():
        """置顶自选台直接录入；名称/简称/代码均可，并让多解结果先选择再加入。"""
        _wa1, _wa2, _wa3 = st.columns([4.2, 3.4, 1.25])
        _token9 = _wa1.text_input(
            "新增自选股", placeholder="输入名称/简称/代码，如：中微 / 腾讯 / NVDA",
            key="_front_wl_add", label_visibility="collapsed")
        _cands9, _chosen9 = [], None
        if _token9.strip():
            try:
                import cloud_engine as _ce_add9
                _cands9 = _ce_add9.search_candidates(_token9.strip(), limit=6) or []
            except Exception:
                _cands9 = []
        if _cands9:
            _opts9 = [f"{n}（{c}·{m}）" for n, c, m in _cands9]
            _pick9 = _wa2.selectbox("简称匹配", _opts9, key="_front_wl_candidate",
                                    label_visibility="collapsed")
            _chosen9 = _cands9[_opts9.index(_pick9)]
        else:
            _wa2.caption("输入简称后，这里会显示匹配的全称与代码")
        if _wa3.button("＋ 加入自选", key="_front_wl_add_btn", use_container_width=True):
            if not _token9.strip():
                st.warning("请先输入股票名称、简称或代码")
            else:
                try:
                    import cloud_engine as _ce_add9
                    if _chosen9:
                        _nm_add9, _cd_add9 = _chosen9[0], _chosen9[1]
                    else:
                        _cd_add9 = _ce_add9.to_yf(_token9.strip())
                        _nm_add9 = _ce_add9.name_of(_cd_add9) or _token9.strip()
                    if _watchlist_add(_cd_add9, _nm_add9):
                        # 新增后立刻让第一屏概率台重扫，不能继续展示15分钟旧缓存。
                        st.session_state.pop("watch_alerts_v88", None)
                        st.session_state["_wl_new_pick"] = (_cd_add9, _nm_add9)
                        st.toast(f"已加入自选：{_nm_add9}（{_cd_add9}）", icon="⭐")
                        st.rerun()
                    else:
                        st.info(f"{_nm_add9}（{_cd_add9}）已在自选中")
                except Exception as _add_e9:
                    st.error(f"未识别该股票，请换用准确代码：{str(_add_e9)[:60]}")
        st.caption("💡 搜名称/简称会列出候选后加入；直接输代码（如 NVDA、00700.HK、600519.SS）可一步加入。")
        # 删除自选：完整可管理。
        try:
            _wl_now9 = _watchlist_load()
            _all_pairs9 = [(c, n, k) for k in ("US", "HK", "CN") for c, n in (_wl_now9.get(k) or [])]
            if _all_pairs9:
                with st.expander("🗑️ 管理 / 删除自选", expanded=False):
                    _rm_opts9 = [f"{n}（{c}）" for c, n, _k in _all_pairs9]
                    _rm_pick9 = st.multiselect("选择要删除的自选股", _rm_opts9, key="_front_wl_remove")
                    if st.button("删除选中", key="_front_wl_remove_btn") and _rm_pick9:
                        for _sel9 in _rm_pick9:
                            _code_rm9 = _all_pairs9[_rm_opts9.index(_sel9)][0]
                            _watchlist_remove(_code_rm9)
                        st.session_state.pop("watch_alerts_v88", None)
                        st.toast(f"已删除 {len(_rm_pick9)} 只自选", icon="🗑️")
                        st.rerun()
        except Exception:
            pass

    def _render_front_watch_board9(_wa9, _time_note9):
        """把规则概率与盈亏比做成第一屏主视觉；详细预警仍在原模块保留。"""
        _all9 = list((_wa9 or {}).get("decisions") or [])
        # 【一池归一 2026-07-18】全池board:持仓+自选都进同一张卡片网格
        _watch9 = [d for d in _all9 if d.get("in_watchlist")
                   or d.get("scope") in ("自选", "持仓")]
        if not _watch9:
            with _v88_watch_mod9:
                st.info("⭐ 我的自选股决策台正在计算；完成后将在这里显示规则方向分（非概率）与盈亏比。")
                _render_front_watch_add9()
            return

        _card9 = _v88_decision_card

        _market_order9 = ("🇺🇸美股", "🇭🇰港股", "🇨🇳A股")
        _groups9 = {m: [] for m in _market_order9}
        for _d9 in _watch9:
            _m9 = str(_d9.get("market") or market_of_code(_d9.get("code", "")))
            _groups9.setdefault(_m9, []).append(_d9)

        # 【所有自选都要在】把还没算出信号的自选股补一张"计算中"占位卡，保证全都在场。
        _mkt_map9 = {"US": "🇺🇸美股", "HK": "🇭🇰港股", "CN": "🇨🇳A股"}
        _seen_canon9 = {_canonical_code(str(d.get("code") or "")) for d in _watch9}
        try:
            _wl_all9 = _watchlist_load()
        except Exception:
            _wl_all9 = {}
        _pending9 = {}
        for _mk_key9, _mk_label9 in _mkt_map9.items():
            for _c_wl9, _n_wl9 in (_wl_all9.get(_mk_key9) or []):
                if _canonical_code(_c_wl9) not in _seen_canon9:
                    _groups9.setdefault(_mk_label9, []).append(
                        {"code": _c_wl9, "name": _n_wl9, "_pending": True})
                    _pending9[_c_wl9] = True

        for _m9 in _groups9:
            _groups9[_m9].sort(key=lambda d: (
                1 if d.get("_pending") else 0,   # 占位卡沉底
                0 if d.get("level") == "A" else (1 if d.get("level") == "B" else 2),
                -float(d.get("p_up") or 0), -float(d.get("rr") or 0)))  # 上涨概率高的靠前

        _cols_html9 = []
        for _m9 in _market_order9:
            _rows9 = _groups9.get(_m9) or []
            if not _rows9:
                continue
            _cols_html9.append(
                f'<section class="v88-watch-market"><h4>{_m9} <span>{len(_rows9)}只</span></h4>'
                + "".join(_card9(d) for d in _rows9) + "</section>")

        with _v88_watch_mod9:
            import textwrap as _textwrap9
            # 先对不含动态卡片的模板去缩进，再替换动态内容；否则卡片中的零缩进行
            # 会让 Markdown 把最外层 div 误判成代码块。
            # 【V88·模块可折叠 2026-07-17 用户定纲】功能区顶部统一折叠开关（默认展开）；
            # 标题上提到 expander label，模板内不再重复大标题。
            _board_tpl9 = _textwrap9.dedent("""
            <div class="v88-watch-shell">
              <div class="v88-watch-title">
              <p>统一分=短20%＋中25%＋长20%＋趋势15%＋赔率20%｜点击股票名进入深度分析｜__TIME__</p></div>
              <div class="v88-watch-grid">__COLUMNS__</div>
            </div>
            """).replace("__TIME__", _time_note9).replace("__COLUMNS__", "".join(_cols_html9))
            if _V88_WATCHLIST_UI:   # 2026-07-31 用户令:删自选版面(数据池保留,仅撤显示)
                with st.expander(f"⭐ 我的自选股 · V88唯一评分决策台（{len(_watch9)}只）", expanded=True):
                    st.markdown(_V88_CARD_CSS + _board_tpl9, unsafe_allow_html=True)
                    _render_front_watch_add9()

        # 【V88·Plan A/B统一标注】与下方AI简报模块共用同一状态，避免"这里显示分数、简报说数据不可信"的割裂
    _pab_status = _rep_planab_meta.get("status")
    if _pab_status == "plan_b":
        _pab_ts = _rep_planab_meta.get("ts")
        _pab_str = datetime.fromtimestamp(_pab_ts).strftime("%m-%d %H:%M") if _pab_ts else "—"
        _pab_issues = _rep_planab_meta.get("today_issues") or []
        _pab_sessions = " / ".join(f"{k} {v}" for k, v in (_rep_planab_meta.get("source_sessions") or {}).items()) or "见原件时间"
        st.caption(f"🟡 最近完整交易日观察稿 · {_pab_str}生成 · 行情：{_pab_sessions}")
        with st.expander("观察稿来源与限制", expanded=False):
            st.caption(f"{'；'.join(_pab_issues) or '叙事证据尚未齐备'}。快照生成于 {_rep_planab_meta.get('source_snapshot_generated_at') or '见原件'}；标的仅作观察，生成报告不授予交易权限。{_report_analysis_note9}")
    elif _pab_status == "missing":
        st.error(f"📭 当前报告尚未通过来源与交易日核验，观察摘要暂不展示；中央评级以3A列表为准。 · {_report_analysis_note9}")

    # 【V88·非交易日前瞻置顶】把 outlook.md 前瞻正文醒目展示（个股名可点深度分析）
    if not _is_trading:
        _outlook_md = ""
        try:
            _outlook_md = (_repo / "data" / "outlook.md").read_text(encoding="utf-8")
        except Exception:
            _outlook_md = ""
        if _outlook_md.strip():
            _outlook_current9 = datetime.now().strftime("%Y-%m-%d") in _outlook_md[:700]
            with st.expander("🔮 下一交易日前瞻" if _outlook_current9 else "📁 历史前瞻·日期见原文·暂停执行用途", expanded=_outlook_current9):
                st.markdown(_linkify_md(_outlook_md), unsafe_allow_html=True)
                with st.popover("📋 复制前瞻"):
                    st.code(_outlook_md, language=None)
        else:
            st.info("暂无可展示的下一交易日前瞻；最近扫描进度见3A列表顶部。")
        st.divider()
        st.caption("下方为最近交易日的行情快照与温度定位（供延续参考）：")
    # 【V88·今日焦点】醒目置顶：重点推荐（引擎买入档）+ 重点观察（搜索过的个股）
    # 【模块可折叠】今日焦点整块折叠（默认展开）
        # 【模块可折叠】使用指引与AI预算收进折叠区（默认收起）
    with st.expander("📖 使用指引 · 参数白话 / AI预算", expanded=False):
        _gen = (_snap or {}).get("generated_at", "")
        st.caption(f"💡 不知道买什么先看这里：温度定仓位 → 水位定方向 → 轮动定板块 → 操作榜定标的 → 持仓提醒定纪律 ｜ 数据时间 {_gen}{_stale_note}")
        st.caption("参数白话：上行概率（越大越有利）｜下行概率（越小越有利）｜盈亏比（越大越好，>1才有正向空间）｜期望值（>0才是正期望）｜ATR（越大波动越大）｜历史水位（越接近0%越靠近历史最高点）")

        # ChatGPT订阅共享额度以会员中心为准；页面仅展示本机调用统计，不臆测剩余额度。
        try:
            import v88_ai_budget as _wb9
            _web9 = _wb9.status()
            st.caption(f"🧮 AI：GPT-6 Codex订阅 / {_web9.get('model', 'gpt-6-astra')}｜"
                       f"本月本机记录{int(_web9.get('calls', 0))}次｜"
                       "共享额度与限流以ChatGPT订阅额度页面为准")
        except Exception:
            pass

    # 🌡 市场温度计（能不能做 · 做多大仓位）
    if _snap and _snap.get("markets"):
        _tl = []
        for _mkt in ("美股", "A股", "港股"):
            _t = (_snap["markets"].get(_mkt) or {}).get("temperature")
            if _t:
                _pos_short = str(_t.get("position", "")).split("（")[0]
                _tl.append(f"{_mkt} <b>{_t['temp']}</b>/100 {_t['label']}·仓位{_pos_short}")
        if _tl:
            st.markdown("🌡 **市场温度**：" + " ｜ ".join(_tl)
                        + "　<span style='font-size:12px;color:#6b7280'>温度=趋势40%+宽度40%+动量20%，全实价计算</span>",
                        unsafe_allow_html=True)

    # 【V88·大盘宏观归位 2026-07-17 用户点单】三层总览+大盘前瞻+大盘环境 用slot传送到
    # 「全球市场概览/宏观脉搏」正下方,让同属大盘宏观的两块上下相邻(逻辑归并)。
    with _l3_group_slot.container():
        # 【V88·三层周期概率总览】北极星定纲：大盘/板块/自选三层同屏看周期+下一周期概率
        # 【模块可折叠】顶部折叠开关，默认展开
        try:
            with st.expander("🧭 三层周期·概率总览（大盘/板块/自选）", expanded=False):
                _render_l3_cycle_board(_snap, _is_trading)
        except Exception:
            logging.debug("三层周期总览渲染失败", exc_info=True)

        # ═══════════════════════════════════════════════════════════════
        # 【V88·大盘&板块前瞻】与个股同一套引擎(evaluate_forward_outlook)+同一条5/10/20/60/120日阶梯，
        # 给大盘指数、板块代表ETF 的 概率+盈亏比+每周期人话理由（预算自适应）。选一个才算，不拖首屏。
        # ═══════════════════════════════════════════════════════════════
        with st.expander("🎯 大盘 & 板块 前瞻 · 概率＋盈亏比（5/10/20/60/120日 · 免选直出）", expanded=False):
            # 【V88·免手选可视化 2026-07-18 用户点单】不再下拉选择:左=中美港大盘,右=板块,
            # 同一套 evaluate_forward_outlook 引擎同一缓存(30分钟)——左右逻辑共联。
            _FW_IDX9 = (("美股·标普500", "^GSPC"), ("A股·上证指数", "000001.SS"), ("港股·恒生指数", "^HSI"))
            _FW_SEC9 = (("科技·美", "XLK"), ("半导体·美", "SOXX"), ("医疗·美", "XLV"),
                        ("金融·美", "XLF"), ("能源·美", "XLE"), ("消费·美", "XLY"),
                        ("医药·A", "512010.SS"), ("券商·A", "512880.SS"), ("新能源·A", "516160.SS"))
            _fwp9 = st.session_state.get("_fw_panel9")
            if not _fwp9 or time.time() - _fwp9.get("ts", 0) > 1800:
                _fwp9 = {"ts": time.time(), "idx": [], "sec": []}
                with st.spinner("同引擎计算中美港大盘与九大板块前瞻…"):
                    try:
                        from v88_decision_core import evaluate_forward_outlook as _efo_p9
                        for _lb9, _sy9 in _FW_IDX9 + _FW_SEC9:
                            try:
                                _dfp9 = fetch_stock_data(_sy9)
                                if _dfp9 is None or len(_dfp9) < 30:
                                    continue
                                _fw9 = _efo_p9(_dfp9, name=_lb9, code=_sy9)
                                if _fw9.get("error"):
                                    continue
                                try:
                                    _cl9p = _dfp9["Close"].dropna()
                                    _r5p9 = (float(_cl9p.iloc[-1]) / float(_cl9p.iloc[-min(6, len(_cl9p))]) - 1) * 100
                                except Exception:
                                    _r5p9 = 0.0
                                _row9 = {"label": _lb9, "p_up": _fw9.get("weighted_p_up"),
                                         "rr": _fw9.get("weighted_rr"), "ev": _fw9.get("weighted_expected_pct"),
                                         "stage": _fw9.get("stage"), "act": _fw9.get("overall_action"),
                                         "now": round(max(20, min(80, _v88_stage_base9(_fw9.get("stage"))
                                                                   + max(-6.0, min(6.0, _r5p9)) * 1.2))),
                                         "chain": [(r.get("label"), r.get("p_up"))
                                                   for r in (_fw9.get("horizons") or [])]}
                                (_fwp9["idx"] if any(_sy9 == s2 for _, s2 in _FW_IDX9)
                                 else _fwp9["sec"]).append(_row9)
                            except Exception:
                                continue
                    except Exception:
                        pass
                st.session_state["_fw_panel9"] = _fwp9
            _colL9, _colR9 = st.columns([1, 1.3])

            def _fw_chain9(_ch9, _now9f=None):
                # 【逐点方向符号+现在锚点 2026-07-19】链首灰「现在」,2周箭头相对现在
                _out9f, _pv9f = [], None
                if _now9f is not None:
                    _out9f.append(f"<span style='color:#94a3b8'>现在<b>{int(_now9f)}</b></span>")
                    _pv9f = _now9f
                for lb, p in (_ch9 or []):
                    _c9f = '#dc2626' if (p or 0) >= 55 else ('#16a34a' if (p or 0) <= 45 else '#64748b')
                    _ar9f = ""
                    if _pv9f is not None and p is not None:
                        _dd9f = p - _pv9f
                        _ar9f = ("<span style='color:#dc2626'>↑</span>" if _dd9f >= 1 else
                                 ("<span style='color:#16a34a'>↓</span>" if _dd9f <= -1 else
                                  "<span style='color:#94a3b8'>≈</span>"))
                    _out9f.append(f"<span style='color:{_c9f}'>{lb}<b>{p}%</b></span>{_ar9f}")
                    _pv9f = p
                return " ".join(_out9f)
            with _colL9:
                st.markdown("**🌍 大盘（中美港）**")
                for _r9 in _fwp9.get("idx") or []:
                    _evc9 = "#dc2626" if (_r9.get("ev") or 0) > 0 else "#16a34a"
                    st.markdown(
                        f"<div style='border:1px solid #dbe4f0;border-radius:8px;padding:6px 8px;margin-bottom:5px'>"
                        f"<b>{_r9['label']}</b> <span style='color:#475569'>{_r9.get('stage', '')}</span> · {_r9.get('act', '')}<br>"
                        f"<span style='font-size:13px'>上涨<b>{_r9.get('p_up')}%</b> ｜ 盈亏比<b>{(_r9.get('rr') or 0):.2f}</b>"
                        f" ｜ 期望<b style='color:{_evc9}'>{(_r9.get('ev') or 0):+.1f}%</b></span><br>"
                        f"<span style='font-size:12px'>{_fw_chain9(_r9.get('chain'), _r9.get('now'))}</span>"
                        + ((lambda _me9: f"<br><span style='font-size:12px;color:#64748b'>{_me9}</span>"
                            if _me9 else "<br><span style='font-size:12px;color:#94a3b8'>"
                            "近3日无该市场策略研报——纯量价驱动（如实说明）</span>")(
                            _v88_market_edge(_r9.get('label', ''))))
                        + "</div>",
                        unsafe_allow_html=True)
            with _colR9:
                st.markdown("**🧩 板块（代表ETF·同引擎）**")
                _sec_rows9 = sorted(_fwp9.get("sec") or [], key=lambda r: -(r.get("p_up") or 0))
                _sec_html9 = ["<div style='font-size:13px;line-height:1.75'>"]
                for _r9 in _sec_rows9:
                    _pc9 = "#dc2626" if (_r9.get("p_up") or 0) >= 55 else ("#16a34a" if (_r9.get("p_up") or 0) <= 45 else "#64748b")
                    _sedge9 = _v88_sector_edge9(_r9.get('label', ''))
                    _sec_html9.append(
                        f"<div style='border-bottom:1px solid #eef2f7;padding:2px 0'>"
                        f"<b>{_r9['label']}</b> 上涨<b style='color:{_pc9}'>{_r9.get('p_up')}%</b>"
                        f" · 期望{(_r9.get('ev') or 0):+.1f}% · {_r9.get('act', '')[:10]}"
                        f"　<span style='font-size:12px'>{_fw_chain9((_r9.get('chain') or [])[:3], _r9.get('now'))}</span>"
                        + (f"<br><span style='font-size:12px;color:#64748b'>🏛️{_sedge9}</span>" if _sedge9 else "")
                        + "</div>")
                _sec_html9.append("</div>")
                _n_noedge9 = sum(1 for _r9x in _sec_rows9 if not _v88_sector_edge9(_r9x.get('label', '')))
                if _n_noedge9:
                    _sec_html9.append(f"<div style='font-size:12px;color:#94a3b8'>"
                                      f"未标🏛️的{_n_noedge9}个板块=近3日无行业研报/板块新闻——纯量价模型信号（如实说明）</div>")
                st.markdown("".join(_sec_html9), unsafe_allow_html=True)
            st.caption("大盘与板块同一套前瞻引擎、同一缓存时间——左右口径天然一致 · 30分钟刷新 · "
                       "概率=规则情景估计 · 数字后↑↓≈=较前一档升/降/平（每段涨落，非对当天涨跌的预测）")

        # 【V88·研究数据层可见化 2026-08-01 用户三问"升级的我都没看到"】
        # 后台四层(宽度/预期差/财务体检/财报日历)原本只喂引擎,用户无法验收→合成一个展开块,
        # 放在大盘环境正上方(视线第一落点)。只读data下的json,不新增计算、不花token。
        try:
            _rl9 = core_root() / "data"
            def _rlj9(_f):
                try:
                    return json.loads((_rl9 / _f).read_text(encoding="utf-8"))
                except Exception:
                    return {}
            _bm9 = _rlj9("barometer.json"); _fq9 = _rlj9("fin_quality.json")
            _ex9 = _rlj9("expectation_proxy.json"); _ec9 = _rlj9("earnings_calendar.json")
            _n_risk9 = len([r for r in (_fq9.get("rows") or []) if (r.get("risk") or 0) >= 25])
            _n_urg9 = len(_ec9.get("urgent_holdings") or [])
            _ttl9 = (f"🔬 研究数据层 · 市场宽度/财务体检/预期差/财报日历"
                     f"（{('⚠️%d只财务风险 · ' % _n_risk9) if _n_risk9 else ''}"
                     f"{('📅%d只持仓临近财报' % _n_urg9) if _n_urg9 else '无临近财报'}）")
            with st.expander(_ttl9, expanded=bool(_n_urg9)):
                st.caption(f"2026-08-01上线 · 全免费源零成本 · 更新 {str(_bm9.get('generated_at'))[:16]}"
                           " · 这四层是引擎的证据来源,此处只做可见化")
                _c9a, _c9b = st.columns(2)
                with _c9a:
                    st.markdown("**🌡️ 市场宽度**（万得式涨跌分布 + 指数对照）")
                    # 【2026-08-01 用户截图案】七档堆叠条→万得式十档柱状图(横轴-7~7,每档标家数);
                    # 渲染逻辑搬进 barometer_ui.py 由桌面/云端共用,不再两端各写一份。
                    try:
                        from barometer_ui import breadth_html as _bhtml9
                        st.markdown(_bhtml9(_bm9), unsafe_allow_html=True)
                    except Exception:
                        logging.exception("[V88] 宽度柱状图渲染失败")
                        st.markdown("<div style='font-size:12px;color:#b45309'>宽度图渲染失败(见日志)</div>",
                                    unsafe_allow_html=True)
                    st.markdown("**📅 美股财报窗**（持仓7日内=降险）")
                    if _n_urg9:
                        for _u9 in (_ec9.get("urgent_holdings") or [])[:5]:
                            st.markdown(f"<div style='font-size:12px;color:#dc2626'>🔴 {_linkify_md(str(_u9))}"
                                        "→财报前不加仓</div>", unsafe_allow_html=True)
                    else:
                        st.markdown("<div style='font-size:12px;color:#64748b'>持仓7日内无财报</div>",
                                    unsafe_allow_html=True)
                with _c9b:
                    st.markdown("**🧪 财务体检**（A股池·质量分/暴雷风险分,可解释）")
                    _fr9 = [r for r in (_fq9.get("rows") or []) if (r.get("risk") or 0) > 0][:6]
                    if _fr9:
                        for _r9 in _fr9:
                            _cl9 = "#dc2626" if _r9["risk"] >= 25 else "#b45309"
                            st.markdown(
                                f"<div style='font-size:12px;margin:1px 0'>{_stk_link(_r9.get('name'), _r9.get('code'))} "
                                f"质量<b>{_r9['quality']}</b>/风险<b style='color:{_cl9}'>{_r9['risk']}</b> "
                                f"<span style='color:#64748b'>{'；'.join((_r9.get('risk_reasons') or [])[:2])}</span></div>",
                                unsafe_allow_html=True)
                    else:
                        st.markdown("<div style='font-size:12px;color:#64748b'>池内暂无显性财务风险</div>",
                                    unsafe_allow_html=True)
                    st.markdown("**📈 预期差**（免费代理分,非万得一致预期）")
                    _er9 = (_ex9.get("rows") or [])[:4]
                    if _er9:
                        for _r9 in _er9:
                            st.markdown(
                                f"<div style='font-size:12px;margin:1px 0'>{_stk_link(_r9.get('name'), _r9.get('code'))} "
                                f"<b>{_r9.get('expectation_proxy_score'):+d}</b> "
                                f"<span style='color:#64748b'>{'；'.join((_r9.get('components') or [])[:2])}</span></div>",
                                unsafe_allow_html=True)
                    else:
                        st.markdown("<div style='font-size:12px;color:#64748b'>"
                                    "池内暂无预告/快报材料(A股7月为披露空窗,8月底半年报密集)</div>",
                                    unsafe_allow_html=True)
                    st.caption("⚠️ 预期差原始分经RankIC回测判否决(高增幅预告后反跑输),"
                               "此处仅语境展示,不进评级权重")
                # 【2026-08-01 用户"资金走势图从每天的小时走势变成每日走势,中美港"】
                # 东财那条是当日分时,看不出趋势;换成每日线才答得了"钱在持续进场还是退潮"。
                st.markdown("**💰 三市场量能走势**（替代当日分时）")
                try:
                    from barometer_ui import (amount_daily_html as _adhtml9, UNITS as _AU9,
                                              SPANS as _AS9, DEFAULT_UNIT as _AD9,
                                              DEFAULT_SPAN as _ADS9)
                    # 【2026-08-01 用户"日期要一致"】区间与单位解耦:先选看多长,再选颗粒度。
                    # 默认1年+周:实测Kaufman效率比日线0.03~0.06≈噪音、周线0.12~0.16、
                    # 月线0.38~0.60最干净但转折要1~2月才确认,对1~2周决策太钝。
                    _cs9a, _cs9b = st.columns(2)
                    with _cs9a:
                        _asp9 = st.radio("区间", list(_AS9), horizontal=True,
                                         index=list(_AS9).index(_ADS9), key="_amt_span9",
                                         label_visibility="collapsed")
                    with _cs9b:
                        _au9 = st.radio("单位", list(_AU9), horizontal=True,
                                        index=list(_AU9).index(_AD9), key="_amt_unit9",
                                        label_visibility="collapsed")
                    _amount_meta9 = _rlj9("market_amount_daily.json")
                    _amount_dates9 = "；".join(f"{k} {v.get('latest_date') or '未标明'}" for k,v in (_amount_meta9.get('markets') or {}).items())
                    st.caption("量能源数据截至：" + _amount_dates9 + "；图表末日未更新时仅保留历史走势，不作为当前资金信号。")
                    st.markdown(_adhtml9(_amount_meta9,
                                         unit=_au9, span=_asp9), unsafe_allow_html=True)
                except Exception:
                    logging.exception("[V88] 量能日线渲染失败")
                    st.markdown("<div style='font-size:12px;color:#b45309'>量能日线渲染失败(见日志)</div>",
                                unsafe_allow_html=True)
        except Exception:
            logging.exception("[V88] 研究数据层区块渲染失败")

        # 【V88·版面梳理 2026-07-17】水位/轮动提醒/周期导图/复制摘要合并收进「大盘环境」
        # （三层总览已给概览,这里是细节层,默认收起减乱）
        # ═══════════════════════════════════════════════════════════════
        with st.expander("🌍 大盘环境 · 指数水位/板块轮动/周期导图", expanded=False):
            # ① 三大市场指数水位
            if _snap and _snap.get("markets"):
                _cols = st.columns(3)
                for _ci, _mkt in enumerate(("美股", "A股", "港股")):
                    _blk = _snap["markets"].get(_mkt) or {}
                    with _cols[_ci]:
                        st.markdown(f"**{_mkt}**")
                        for _ix in (_blk.get("indices") or [])[:3]:
                            _tn99 = _ix.get("turning") or ""
                            st.markdown(
                                f"<div style='font-size:13px;line-height:1.7'>{_ix['trend']} {_ix['name']} "
                                f"<b>{_ix['last']}</b>｜5日{_ix['chg5d']:+.1f}%｜距MA20 {_ix['vs_ma20']:+.1f}%"
                                + (f"｜<b style='color:#dc2626'>{_tn99}</b>" if _tn99.startswith("⚠️")
                                   else (f"｜<b style='color:#16a34a'>{_tn99}</b>" if _tn99 else ""))
                                + "</div>", unsafe_allow_html=True)
                            if _ix.get("turning_prompt"):
                                st.caption(f"🔀 {_ix['name']}拐点：{_ix['turning_prompt']}")
                # ② 板块轮动提醒（用快照数据重算 5日vs20日 排名跃迁）
                _hints = []
                for _mkt in ("美股", "A股", "港股"):
                    _secs = ((_snap["markets"].get(_mkt) or {}).get("sectors")) or []
                    if len(_secs) < 4:
                        continue
                    _n = len(_secs)
                    _r5 = {s["symbol"]: i for i, s in enumerate(sorted(_secs, key=lambda x: -x["chg5d"]))}
                    _r20 = {s["symbol"]: i for i, s in enumerate(sorted(_secs, key=lambda x: -x["chg20d"]))}
                    _jump = max(2, _n // 3)
                    for s in _secs:
                        _d = _r20[s["symbol"]] - _r5[s["symbol"]]
                        if _d >= _jump and s["chg5d"] > 0:
                            _hints.append(f"🔥 {_mkt}·**{s['name']}** 轮入（5日{s['chg5d']:+.1f}%，排名{_r20[s['symbol']]+1}→{_r5[s['symbol']]+1}）")
                        elif _d <= -_jump and s["chg20d"] > 0:
                            _hints.append(f"🧊 {_mkt}·**{s['name']}** 退潮（20日{s['chg20d']:+.1f}%但5日{s['chg5d']:+.1f}%）")
                if _hints:
                    st.markdown("**板块轮动**：" + " ｜ ".join(_hints[:5]))
                # ③+④ 周期总览：板块轮动与个股切换合并为同一行双栏，信息保留、字号压小。
                _rot_forecast9 = (_snap or {}).get("rotation_forecast") or {}
                _cyc9 = (_snap or {}).get("cycle_scan") or {}
                if _rot_forecast9 or _cyc9.get("stocks") or _cyc9.get("status") == "pending":
                    st.markdown("**🧭 板块与个股 · 未来趋势圆周与曲线**")
                    try:
                        from rotation_ui import combined_cycle_dashboard_html as _cycle_board9, available_markets as _am9
                        _mk9 = _am9(_rot_forecast9)
                        _focus9 = (st.radio("时钟聚焦市场", _mk9, horizontal=True, key="v88_nav_rot_focus",
                                            label_visibility="collapsed")
                                   if len(_mk9) > 1 else (_mk9[0] if _mk9 else "美股"))
                        st.markdown(_cycle_board9(_rot_forecast9, _cyc9, "v88-nav-cycle-board", _focus9),
                                    unsafe_allow_html=True)
                    except Exception as _rot_ui_e9:
                        logging.debug(f"周期总览渲染失败: {_rot_ui_e9}")
                # 【V88·复制】今日导航摘要一键复制（温度/指数/拐点/轮动）
                try:
                    _cpn = [f"🧭 V88今日导航 {_gen}"]
                    for _mkt in ("美股", "A股", "港股"):
                        _b9 = _snap["markets"].get(_mkt) or {}
                        _t9 = _b9.get("temperature") or {}
                        if _t9:
                            _cpn.append(f"{_mkt} 温度{_t9.get('temp','?')}/100 {_t9.get('label','')} 仓位{_t9.get('position','?')}")
                        for _x9 in (_b9.get("indices") or [])[:3]:
                            _cpn.append(f"  {_x9['trend']} {_x9['name']} {_x9['last']}｜5日{_x9['chg5d']:+.1f}%"
                                        + (f"｜{_x9['turning']}" if _x9.get('turning') else ""))
                    if _hints:
                        _cpn.append("板块轮动：" + "；".join(h.replace('**', '') for h in _hints[:5]))
                    with st.popover("📋 复制导航摘要"):
                        st.code("\n".join(_cpn), language=None)
                except Exception:
                    pass
            else:
                st.info("📭 大盘快照尚未生成（每日07:00/14:00/21:00自动更新）")

    # 【V88·关注股预警】底层仍统一扫描自选+常搜+持仓；页面展示分流：
    # 持仓风险只进入下方“持仓决策中心”，自选预警不再重复罗列正式持仓。
    _critical9, _wa = [], {}
    try:
        _wa = st.session_state.get('watch_alerts_v88')
        if not _wa or _wa.get('rule_version') != 9 or time.time() - _wa.get('ts', 0) > 15 * 60:
            _pool_wa = {}
            _watch_codes_wa = set()
            _holds_wa = set()
            _claims_wa = {}
            _hold_map_wa = {}
            try:
                for _mk9, _lst9 in (_watchlist_load() or {}).items():
                    # 每个市场的自选全部扫描；不得以10只截断或因同时持仓而漏显。
                    for _c9, _n9 in list(_lst9):
                        _c9 = str(_c9)
                        _watch_codes_wa.add(_c9)
                        _pool_wa[_c9] = _n9
            except Exception:
                pass
            try:
                _pcj9 = json.loads((_repo / "position_claims.json").read_text(encoding="utf-8"))
                _claims_wa = _pcj9.get("claims", _pcj9) if isinstance(_pcj9, dict) else {}
                for _cc9, _cv9 in _claims_wa.items():
                    _pool_wa.setdefault(str(_cc9), (_cv9 or {}).get("name", str(_cc9)))
            except Exception:
                _claims_wa = {}
            try:
                if _SEARCH_HIST_FILE.exists():
                    _sh9 = json.loads(_SEARCH_HIST_FILE.read_text(encoding="utf-8"))
                    for _c9, _e9 in sorted(_sh9.items(), key=lambda x: -x[1].get("n", 0))[:8]:
                        _pool_wa.setdefault(_c9, _e9.get("name", _c9))
            except Exception:
                pass
            try:
                _pj9 = json.loads((_repo / "positions.json").read_text(encoding="utf-8"))
                for _acc9 in (_pj9.get("accounts") or {}).values():
                    for _h9 in (_acc9.get("holdings") or []):
                        if _h9.get("code") and "⚠️" not in str(_h9["code"]):
                            _hc9 = str(_h9["code"])
                            _holds_wa.add(_hc9)
                            _hold_map_wa[_hc9] = _h9
                            _pool_wa.setdefault(_hc9, _h9.get("name", ""))
            except Exception:
                pass
            # 【V88·动态奔跑线】峰值浮盈数据（盘中Actions每小时回写），供持仓卡换算锁盈价
            try:
                _peaks_wa = json.loads((_repo / "data" / "position_peaks.json").read_text(encoding="utf-8"))
            except Exception:
                _peaks_wa = {}
            _risk_holds_wa = _holds_wa | set(_claims_wa)
            import cloud_engine as _ce_wa
            import sys as _sys_wa
            if str(_repo / "src") not in _sys_wa.path:
                _sys_wa.path.insert(0, str(_repo / "src"))
            from watch_alerts import sharp_drop_signal as _sharp_wa, market_change_for as _mchg_wa, watch_levels as _levels_fn_wa
            from position_lifecycle import dynamic_priority as _dyn_level_wa, load_peaks as _load_peaks_wa
            from v88_decision_core import evaluate_decision as _evaluate_decision_wa, compact_text as _compact_decision_wa
            _levels_wa = _levels_fn_wa()
            _peaks_wa = _load_peaks_wa()
            try:  # 【行业热度维度】与飞书/云端同一份冻结快照
                _mkts9 = json.loads((_repo / "data" / "market_snapshot.json").read_text(encoding="utf-8")).get("markets") or {}
            except Exception:
                _mkts9 = {}
            _pool_wa = dict(sorted(_pool_wa.items(), key=lambda kv: (
                0 if kv[0] in _risk_holds_wa else (1 if _levels_wa.get(kv[0], "B") == "A" else 2))))
            _alerts9 = []
            _decisions9 = []
            _holding_levels9 = {}
            # 【V88·禁静默截断 2026-07-18】持仓+自选必须全算（"所有自选都要在场"），
            # 只允许截"常搜"尾部——否则超60只时尾部票永远"计算中"却谎称稍后刷新。
            _core_scan9 = [(c, n) for c, n in _pool_wa.items()
                           if c in _risk_holds_wa or c in _watch_codes_wa]
            _rest_scan9 = [(c, n) for c, n in _pool_wa.items()
                           if not (c in _risk_holds_wa or c in _watch_codes_wa)]
            _scan_items9 = _core_scan9 + _rest_scan9[:max(0, 60 - len(_core_scan9))]
            _scan_prog9 = st.progress(0)
            _scan_status9 = st.empty()
            for _idx9, (_c9, _n9) in enumerate(_scan_items9, 1):
                _scan_prog9.progress(_idx9 / max(1, len(_scan_items9)))
                _scan_status9.caption(f"⏳ 正在计算持仓/自选概率 {_idx9}/{len(_scan_items9)}：{_n9}")
                try:
                    _df9 = fetch_stock_data(to_yf_cn_code(_c9))
                    _f9 = _ce_wa.analyze_trend_full(_df9)
                    if not _f9:
                        # 【V88·新股如实卡 2026-07-18 SPCX案发】历史<35根K线引擎算不动——
                        # 原来静默continue→占位卡永远"计算中"撒谎。改为落一条简版记录：
                        # 现价/近5日/上市以来涨跌，明说"历史不足不给假信号"。
                        try:
                            _bars9 = 0 if _df9 is None else len(_df9)
                            if _df9 is not None and _bars9 >= 2:
                                _clo9 = _df9["Close"].dropna()
                                _lastp9 = float(_clo9.iloc[-1])
                                _decisions9.append({
                                    "code": _c9, "name": _n9, "_short_history": True,
                                    "in_watchlist": _c9 in _watch_codes_wa,
                                    "scope": ("持仓" if _c9 in _risk_holds_wa else
                                              ("自选" if _c9 in _watch_codes_wa else "常搜")),
                                    "level": _levels_wa.get(_c9, "B"),
                                    "market": market_of_code(_c9),
                                    "bars": _bars9, "last": round(_lastp9, 2),
                                    "chg5": round((_lastp9 / float(_clo9.iloc[-min(6, len(_clo9))]) - 1) * 100, 1),
                                    "ipo_chg": round((_lastp9 / float(_clo9.iloc[0]) - 1) * 100, 1),
                                })
                        except Exception:
                            pass
                        continue
                    _last9 = _f9["last"]
                    _sharp9 = _sharp_wa(_df9, _f9, holding=_c9 in _risk_holds_wa,
                                        level=_levels_wa.get(_c9, "B"),
                                        market_chg=_mchg_wa(_c9, _mkts9))
                    _dyn9, _dyn_reason9 = _levels_wa.get(_c9, "B"), ""
                    if _c9 in _holds_wa:
                        _dyn9, _dyn_reason9 = _dyn_level_wa(
                            _hold_map_wa[_c9], _f9,
                            peak_pnl=((_peaks_wa.get(_c9) or {}).get("peak_pnl")),
                            sharp=bool(_sharp9))
                        _holding_levels9[_c9] = {"level": _dyn9, "reason": _dyn_reason9}
                    _hint9 = ("评估减仓" if (_sharp9 or (_c9 in _holds_wa and _dyn9 == "A"))
                              else ("持有" if _c9 in _holds_wa else "观察"))
                    # 首页、搜索、持仓、预警、深度分析全部只读唯一决策引擎。
                    _dc9 = _evaluate_decision_wa(
                        _df9, _f9, holding=_hold_map_wa.get(_c9), action_hint=_hint9,
                        analysis_time=datetime.now().strftime("%m-%d %H:%M"),
                        name=_n9, code=_c9)
                    # 【V88·三段作战计划】顺手组装（前瞻日阶梯纯确定性，几毫秒/只）
                    _plan9 = {}
                    try:
                        from v88_decision_core import (evaluate_forward_outlook as _efo_wa,
                                                       build_trade_plan as _btp_wa)
                        _fwd_wa9 = _efo_wa(_df9, name=_n9, code=_c9, full=_f9)
                        if not _fwd_wa9.get("error"):
                            _plan9 = _btp_wa(_f9, _dc9.get("entry_plan"), _fwd_wa9)
                    except Exception:
                        _plan9 = {}
                    # 【V88·今日逐只解读所需】顺手存今日涨跌/阶段/破位（数据在手，零额外成本）
                    try:
                        _tcc9 = _df9["Close"]
                        _today_chg9 = ((float(_tcc9.iloc[-1]) / float(_tcc9.iloc[-2]) - 1) * 100
                                       if len(_tcc9) >= 2 else 0.0)
                    except Exception:
                        _today_chg9 = 0.0
                    _decisions9.append({"code": _c9, "name": _n9,
                                        "in_watchlist": _c9 in _watch_codes_wa,
                                        "scope": ("持仓" if _c9 in _risk_holds_wa else
                                                  ("自选" if _c9 in _watch_codes_wa else "常搜")),
                                        "level": (_dyn9 if _c9 in _holds_wa else _levels_wa.get(_c9, "B")),
                                        "market": market_of_code(_c9),
                                        "trade_plan": _plan9,
                                        "today_chg": round(_today_chg9, 2),
                                        "stage": _f9.get("stage"),
                                        "broke_stop": bool(_last9 < _f9.get("stop", 0)),
                                        "pos52": _f9.get("pos52"),
                                        "peak_pnl": (_peaks_wa.get(_c9) or {}).get("peak_pnl"),
                                        "hold_cost": (_hold_map_wa.get(_c9) or {}).get("cost"),
                                        "move_reason": _v88_move_reason(
                                            _today_chg9, names=[_n9],
                                            scope_hint=market_of_code(_c9)[-2:],
                                            require_name=True),
                                        **_dc9})
                    if _last9 < _f9["stop"]:
                        if _c9 in _claims_wa:
                            _alerts9.append(f"❗ [已确认持仓·资料待补录] **{_n9}**({_c9})：现价{_last9}已破技术防守位{_f9['stop']}——立即复核仓位｜{_compact_decision_wa(_dc9)}｜成本/股数待补录")
                        else:
                            _alerts9.append(f"❗ [持仓·A自动] **{_n9}**({_c9})：现价{_last9}已破止损位{_f9['stop']}——纪律复核离场/减仓｜{_compact_decision_wa(_dc9)}")
                    else:
                        if _sharp9:
                            _who9 = "正式持仓" if _c9 in _holds_wa else ("已确认持仓" if _c9 in _claims_wa else "A级重点")
                            _level_tag9 = (f"[持仓·{_dyn9}自动]" if _c9 in _holds_wa else
                                           ("[已确认持仓·资料待补录]" if _c9 in _claims_wa else "[A级重点]"))
                            _alerts9.insert(0, f"{_sharp9['severity']} {_level_tag9} **{_n9}**({_c9})：{_who9}急跌预警｜"
                                            + "＋".join(_sharp9["facts"]) + f"｜{_sharp9['action']}"
                                            + f"｜{_compact_decision_wa(_dc9)}"
                                            + ("｜成本/股数待补录" if _c9 in _claims_wa else ""))
                            continue
                        # 【V88·多因子共振】买入/减仓须≥2维度（技术/量价/消息）共振，单指标不触发（与云端/飞书同源）
                        _sw9 = _ce_wa.smart_watch_signal(_f9, sector_heat=_ce_wa.sector_heat_of(_c9, _n9, _mkts9))
                        if _sw9:
                            _ic9 = "🛒" if _sw9["side"] == "buy" else "⚠️"
                            _hd9 = "触发条件" if _sw9["side"] == "buy" else "风险原因"
                            _alerts9.append(f"{_ic9} **{_n9}**({_c9})：**{_sw9['action']}**｜{_hd9}："
                                            + "＋".join(_sw9["conditions"][:4]) + f"｜{_sw9['zone']}｜{_compact_decision_wa(_dc9)}")
                        elif _c9 in _holds_wa and _dyn9 == "A":
                            _alerts9.insert(0, f"⚠️ [持仓·A自动] **{_n9}**({_c9})：{_dyn_reason9}｜"
                                                "优先复核减仓/止损与利润保护")
                except Exception:
                    continue
            _scan_prog9.empty()
            _scan_status9.empty()
            # 【2026-07-18修】新股简版记录(_short_history)没有p_down——裸取键曾KeyError
            # 炸掉整个扫描段(自选台/预警/逐只全灭),一律.get兜底。
            _decisions9.sort(key=lambda x: (
                0 if x.get("scope") == "自选" else (1 if x.get("scope") == "持仓" else 2),
                0 if x.get("level") == "A" else (1 if x.get("level") == "B" else 2),
                -float(x.get("p_down") or 0)))
            _wa = {"ts": time.time(), "alerts": _alerts9, "decisions": _decisions9,
                   "n": len(_pool_wa), "rule_version": 9, "holding_levels": _holding_levels9}
            st.session_state['watch_alerts_v88'] = _wa
        _alert_analysis_note9 = _analysis_label9(_wa.get("ts"), "预警分析")
        # 【V88·一票一卡 2026-07-18 用户抓重复】逐只诊断先附着到决策,由卡片🩺行呈现,
        # 不再另开"逐只怎么办"独立区(同一票模块内只出现一次)。
        try:
            _mkchg9 = {}
            for _mk9m, _sym9m in {"美股": "^GSPC", "A股": "000001.SS", "港股": "^HSI"}.items():
                try:
                    _c9m = fetch_stock_data(_sym9m)["Close"]
                    _mkchg9[_mk9m] = (float(_c9m.iloc[-1]) / float(_c9m.iloc[-2]) - 1) * 100
                except Exception:
                    _mkchg9[_mk9m] = 0.0
            _v88_attach_diag9(_wa, _mkchg9)
        except Exception:
            logging.debug("逐只诊断附着失败", exc_info=True)
        _render_front_watch_board9(_wa, _alert_analysis_note9)

        # Discovery contributes evidence; the central 3A record is the only rating authority.
        try:
            from darkhorse_radar import load_projection as _load_discovery_projection
            from discovery_review_ui import render as _render_discovery_review
            _discovery_projection = _load_discovery_projection()
            with st.expander("🔎 跨模块发现与3A核对 · 精选", expanded=False):
                st.markdown(_render_discovery_review(_discovery_projection, stock_link=_stk_link), unsafe_allow_html=True)
        except Exception:
            logging.exception("发现线索中央核对暂不可用")
            st.caption("🔎 发现线索核对暂不可用；请以3A主榜的当前审核与原合同为准。")
        if _wa.get("alerts"):
            _critical9 = [a for a in _wa["alerts"]
                          if "[持仓" in a or "[已确认持仓" in a]
            # 【V88·持仓归位持仓台 2026-07-16】持仓票(含smart_watch减仓信号，此前无"[持仓"标记
            # 会漏进图二)一律移出预警文本列表——持仓改由下方「💼持仓·概率决策台」卡片呈现。
            _hold_codes9x = {str(d.get("code")) for d in (_wa.get("decisions") or [])
                             if d.get("scope") == "持仓"}
            _watch_only9 = [a for a in _wa["alerts"]
                            if a not in _critical9 and "[持仓" not in a
                            and not any(f"({_hc})" in a for _hc in _hold_codes9x)]
            if _watch_only9:
                if _V88_WATCHLIST_UI:   # 2026-07-31 用户'这个模块也可以删了':预警文字墙撤——买入类归行动中心/3A表,减仓类归卖减tab+地狱门否决链,推送走飞书,引擎照跑
                    with _v88_watch_mod9, st.expander(f"⚡ 预警触发（自选/常搜 · 多因子共振 · {len(_watch_only9)}条） · {_alert_analysis_note9}", expanded=False):
                        st.markdown("\n".join(f"- {_linkify_md(a)}" for a in _watch_only9), unsafe_allow_html=True)
                        with st.popover("📋 复制预警"):
                            st.code(_alert_analysis_note9 + "\n" + "\n".join(a.replace("**", "") for a in _watch_only9), language=None)
            elif not _critical9:
                with _v88_watch_mod9:
                    st.caption(f"⚡ 预警：{_wa.get('n', 0)}只关注股暂无触发（持仓15分钟｜A盘中3小时｜B每天｜C每周）")
        else:
            with _v88_watch_mod9:
                st.caption(f"⚡ 预警：{_wa.get('n', 0)}只关注股暂无触发（持仓15分钟｜A盘中3小时｜B每天｜C每周）")
    except Exception as _we9:
        logging.debug(f"关注股预警异常: {_we9}", exc_info=True)

    # 【V88·持仓决策中心】唯一持仓展示：完整日报分析 + 实时风险 + 可修改底稿。
    with _v88_hold_mod9, st.expander("💼 持仓风险与记录", expanded=True):
        import sys as _sysf
        if str(_repo / "src") not in _sysf.path:
            _sysf.path.insert(0, str(_repo / "src"))
        import position_manager as _pmf
        # Streamlit 热更新不会自动重载已 import 的模块；升级持仓字段后立即使用新接口，无需重启 V88。
        import importlib as _ilf9
        _pmf = _ilf9.reload(_pmf)
        if st.session_state.get("_pt_flash"):
            st.success(st.session_state.pop("_pt_flash"))
        _claim_rows9 = _pmf.claimed_holding_rows()
        if _claim_rows9:
            _claim_names9 = "、".join(f"{r['名称']}({r['代码']})" for r in _claim_rows9)
            st.warning(f"⚠️ 已确认持仓·资料待补录：{_claim_names9}。已按持仓优先预警；补齐账户、股数和成本后自动启用浮盈/峰值回撤/个性化止损。 · {_analysis_label9((_wa or {}).get('ts'), '持仓识别分析')}")
        _rows_pt = _pmf.holdings_rows()

        if (_repo / "positions.json").is_file():
            with st.expander("📒 已登记持仓与历史成交", expanded=False):
                st.caption("通用持仓账本；Astra成交记录在Astra模块独立管理。")
                from portfolio_archive_view import records_html as _portfolio_records_html9
                def _archive_table9(rows):
                    return _portfolio_records_html9(rows, stock_link=_stk_link)
                if _rows_pt:
                    st.markdown(_archive_table9(_rows_pt), unsafe_allow_html=True)
                else:
                    st.caption("暂无已登记持仓。")
                _legacy_trade_path9 = _repo / "journal" / "trades.json"
                if _legacy_trade_path9.is_file():
                    try:
                        _legacy_trades9 = json.loads(_legacy_trade_path9.read_text(encoding="utf-8"))
                        if not isinstance(_legacy_trades9, list):
                            raise ValueError("历史成交格式不正确")
                        st.caption(f"历史成交 {len(_legacy_trades9)} 条；显示最近20条原记录，不计为已核验实盘业绩。")
                        if _legacy_trades9:
                            st.markdown(_archive_table9(list(reversed(_legacy_trades9[-20:]))), unsafe_allow_html=True)
                    except Exception:
                        st.caption("历史成交暂不可读，原文件保留。")

        # ── 唯一持仓分析：复用日报最完整的“基本面+技术面+新闻面+综合建议”，按市场拆分。 ──
        if _critical9:
            # 【V88·一票一卡 2026-07-18 用户抓重复】原文本墙每行重复卡片同源数字(统一分/
            # 概率/盈亏比/期望)——压成一行名单,细节看下方各卡;原文收进popover备查。
            _names_r9, _seen_r9 = [], set()
            for _a9 in _critical9:
                _m9r = re.search(r"\*\*(.+?)\*\*", str(_a9))
                _nm9r = _m9r.group(1) if _m9r else str(_a9)[:8]
                if _nm9r not in _seen_r9:
                    _seen_r9.add(_nm9r)
                    _names_r9.append(_nm9r)
            _risk_time9 = _analysis_label9(_wa.get("ts"), "预警分析")
            st.markdown(
                f'<div style="background:#fee2e2;border-left:4px solid #ef4444;border-radius:8px;'
                f'padding:.5rem .8rem;color:#7f1d1d;font-size:13px"><b>🚨 持仓风险优先 {len(_names_r9)}只</b>：'
                + "、".join(_names_r9[:12])
                + f'<span style="font-size:12px;color:#991b1b">——急跌/破位请核对原退出条件；预警原文可展开 · {_risk_time9}</span></div>',
                unsafe_allow_html=True)
            with st.popover("📄 预警原文备查"):
                st.markdown("\n".join(f"- {a}" for a in _critical9[:10]))

        # 【V88·持仓概率决策台 2026-07-16】持仓也变成自选那样的卡片：中/短/长/16周上涨概率走势条+盈亏比+期望。
        # 数据用预警扫描的同一份唯一决策(scope=持仓)，与自选决策台一字同源。
        try:
            _hold_dec9 = [d for d in (_wa.get("decisions") or []) if d.get("scope") == "持仓"]
            if _hold_dec9:
                # 【V88·持仓组合体检】把持仓当一盘棋的思考模式判断（每天一次,读落盘零成本）
                _pcx_html9 = ""
                try:
                    _pcx9 = json.loads((_repo / "data" / "portfolio_checkup.json").read_text(encoding="utf-8"))
                    if _pcx9.get("status") == "completed":
                        _chain_txt9 = "　".join(
                            f"🔗<b>{c.get('链名')}</b>({len(c.get('成员') or [])}只:{c.get('敞口说明', '')[:24]})"
                            for c in (_pcx9.get("chains") or [])[:3])
                        _pcx_html9 = (
                            f"<div style='background:#f5f3ff;border-left:4px solid #7c3aed;border-radius:8px;"
                            f"padding:.5rem .7rem;font-size:12px;margin-bottom:6px'>"
                            f"<b style='color:#7c3aed'>🧩 组合体检（强思考·{_pcx9.get('generated_at', '')[:16]}）</b><br>"
                            f"{_chain_txt9}<br>"
                            f"⚠️ <b>{_pcx9.get('top_risk', '')}</b><br>"
                            f"💡 {_pcx9.get('advice', '')}</div>")
                except Exception:
                    _pcx_html9 = ""
                # 【一池归一 2026-07-18】持仓概率卡已并入上方"我的股票池"网格(金名=双重身份,
                # 💼=纯持仓)——此处只留独有的组合体检,重复网格撤掉缩小版面。
                if _pcx_html9:
                    st.markdown(_pcx_html9, unsafe_allow_html=True)

        except Exception as _hpe9:
            logging.debug(f"持仓决策台渲染失败: {_hpe9}")



try:
    with _v88_front_decision_slot.container():
        _render_today_nav()

except Exception as _nav_e:
    st.caption(f"今日导航暂不可用: {str(_nav_e)[:50]}")
st.markdown("---")

# Astra monthly research and actual trade journal share one workspace.
# Independent monthly workspace; general release diagnostics remain separate.
try:
    from astra_plan_view import render as _render_astra_monthly
    _astra_doc = _cbj9('astra_plan.json') or _cbj9('astra_plan_pub.json') or {}
    _astra_private_records = bool(_astra_doc) and not _astra_doc.get('private_redacted') and (_cb_repo9/'data/astra_plan.json').is_file()
    _render_astra_monthly(st, _astra_doc, stock_link=_stk_link,
                          expected_factpack_id=(_cbj9('triad_selection_pub.json') or {}).get('factpack_id'),
                          allow_trade_recording=_astra_private_records)
    if _astra_private_records:
        try:
            from astra_trade_entry_view import render as _render_astra_records
            _render_astra_records(st, (_astra_doc.get('research_report') or {}).get('candidates') or [],
                                  factpack_id=_astra_doc.get('factpack_id'))
        except Exception as _astra_record_error:
            st.error(f'Astra成交记录暂不可用：{type(_astra_record_error).__name__}；请稍后重试，原记录保留')
except Exception as _astra_error:
    st.error(f'Astra月度模块读取失败：{type(_astra_error).__name__}；保留原合同，不新增交易')

with _v88_system_details, st.expander('🔬 审核与验证进度', expanded=False):
    try:
        _notice_doc = _cbj9("notification_plan.json") or {}
        if _notice_doc:
            with st.expander("🔔 行动 / 准备 / 观察 · 通知状态", expanded=False):
                st.info(_notice_doc.get("status_text", "通知计划待生成"))
                st.caption(_notice_doc.get("delivery", ""))
                st.caption("1A/2A到区间仅提示准备复核，不增加买入权限；未发送和失败不能算送达。")
                st.caption(_notice_doc.get("coverage_note", ""))
        _upgrade_doc = _cbj9("upgrade_acceptance.json") or {}
        if _upgrade_doc:
            _ug2 = _upgrade_doc.get("2a", {})
            _ugc = _upgrade_doc.get("coverage", {})
            _ugs = _upgrade_doc.get("samples", {})
            st.info(f"量化改进验收：合格2A {_ug2.get('current', 0)}/{_ug2.get('target_minimum', 1)}；"
                    f"固定批次累计双审 {_ugc.get('cumulative_completed', 0)}/{_ugc.get('fixed_cohort', 0)}；"
                    f"合规已结算回放 {_ugs.get('qualified_settled', 0)}/{_ugs.get('target', 3000)}。"
                    f"{_upgrade_doc.get('status', '')}")
            st.caption(f"当前有效双审 {_ugc.get('current_valid_complete', 0)} 只；累计审核不因过期删除，当前有效数另算。"
                       "2A数量和3000笔目标不改变评分；未触发、重复、缺历史成分证明的回放不计入。")
            _ugb = _upgrade_doc.get("one_a_bottlenecks", {})
            _ugg = _ugb.get("gaps", {})
            st.caption(f"1A升级卡点（可重叠）：反证 {_ugg.get('gpt_countercase', 0)} 只；"
                       f"兑现周期 {_ugg.get('gpt_horizon', 0)} 只；同策略净期望 {_ugg.get('tharp_expectancy', 0)} 只；"
                       f"入场确认 {_ugg.get('entry', 0)} 只。数据和策略证据覆盖未完成，不能据空榜断定全市场没有机会。")

        with st.expander("🔬 前瞻验证 · 原始凭据、模拟结算与实际履历", expanded=False):
            from research_validation_view import render as _render_research_validation
            _render_research_validation(st, _cbj9("research_validation.json") or {})

        _grade_acceptance = _cbj9("grade_acceptance.json") or {}
        if _grade_acceptance:
            st.info(f"新增评级验收：合格2A {_grade_acceptance.get('qualified_2a', 0)}只；"
                    f"合格3A {_grade_acceptance.get('qualified_3a', 0)}只"
                    f"（当前可执行 {_grade_acceptance.get('executable_3a', 0)}只）。"
                    f"{_grade_acceptance.get('status', '待核验')}")
            _grade_run = _grade_acceptance.get("review_run", {})
            st.caption(f"当前事实包完整双审 {_grade_run.get('complete_pairs_in_current_pack', 0)}只；"
                       f"{'复核仍在运行' if _grade_run.get('in_progress') else '本轮复核已结束'}。"
                       "双审完成不等于评级通过；本轮有限批次不等于全市场审核完成。")
            _grade_bounds = _grade_acceptance.get('deterministic_review_bounds', {})
            st.caption(f"当前有源复核队列 {_grade_acceptance.get('review_queue_count', 0)}只；"
                       f"书理与空间允许进一步审核2A/3A的候选 "
                       f"{_grade_bounds.get('2A', 0)+_grade_bounds.get('3A', 0)}只。"
                       "这是复核顺序依据，不是预先授级；不满足条件时验收明确不通过。")

    except Exception as _acceptance_error:
        st.caption(f'系统验收记录暂不可用：{type(_acceptance_error).__name__}')


# Full-market coverage and directory search are rendered once in the main action area.

_v88_history_details = st.expander("📁 历史与规则档案", expanded=False)
with _v88_history_details, st.expander("📚 原Fable5计划与持仓保护", expanded=False):
    try:
        st.caption("原Fable5已由Astra月度计划接替；以下为历史参数和既有持仓保护，不形成新的开仓指令。")
        _fp9 = _cbj9("fable_plan.json") or _cbj9("fable_plan_pub.json") or {}
        _fmonths9 = _fp9.get("months") or {}
        _fmonth_options9 = sorted(_fmonths9, reverse=True)
        _fmonth_default9 = next((m for m in _fmonth_options9 if (_fmonths9[m] or {}).get("plan")), None)
        _fmonth9 = st.selectbox("历史计划月份", _fmonth_options9,
                               index=_fmonth_options9.index(_fmonth_default9) if _fmonth_default9 else 0,
                               key="v88_fable_archive_month") if _fmonth_options9 else None
        _fpm9 = _fmonths9.get(_fmonth9) or {}
        _fpp9 = _fpm9.get("plan") or {}
        if not _fpp9:
            if _fp9.get("private_redacted"):
                _fps9 = _fp9.get("plan_summary") or {}
                st.info(f"{_fps9.get('state') or 'Fable计划状态已隐私保护'}｜"
                        f"本月计划{_fps9.get('trade_count', 0)}笔｜"
                        f"{_fps9.get('current_action') or '完整计划仅在桌面/飞书私域显示'}")
                st.caption(_fps9.get("meaning") or
                           "云端公开版不展示具体仓位与交易参数；完整 Fable 计划仅在桌面/飞书私域显示。")
            else:
                st.caption(_fpm9.get("why") or "该月份没有已发布计划。")
        else:
            st.caption(f"发布 {_fpp9.get('issued_at')}｜规则 {_fpp9.get('ruleset')}｜"
                       f"目标 ${_fpp9.get('target_usd')}｜{_fpp9.get('governance')}")
            for _t9f in (_fpp9.get("trades") or []):
                st.markdown(
                    f"**{_stk_link(_t9f.get('name'), _t9f.get('code'))}** `{_t9f.get('code')}`　【{_t9f.get('school')}】"
                    f"　计划赔率 {_t9f.get('plan_rr')}　最大亏损 ${_t9f.get('max_loss_usd')}\n"
                    f"- ① 买入：**限价 {_t9f.get('entry_limit')} × {_t9f.get('shares')}股**"
                    f"　{_t9f.get('entry_rule')}\n"
                    f"- ② 止盈：**{_t9f.get('take_profit')}**　{_t9f.get('tp_rule')}\n"
                    f"- ③ 止损：**收盘 < {_t9f.get('stop_close')}**　{_t9f.get('sl_rule')}\n"
                    f"- ④ 时间闸：**{_t9f.get('time_gate')}**　{_t9f.get('tg_rule')}\n"
                    f"- 诚实赔率：{_t9f.get('honest_odds')}", unsafe_allow_html=True)
            for _r9f in (_fpm9.get("status_rows") or []):
                for _a9f in (_r9f.get("alerts") or []):
                    st.markdown(
                        f"<div style='background:#fff7ed;color:#9a3412;border-radius:6px;padding:7px 10px'>"
                        f"{_stk_link(_r9f.get('name'), _r9f.get('code'))}：{_a9f}</div>",
                        unsafe_allow_html=True)
            _wc9f = _fpp9.get("watch_conditional") or []
            if _wc9f:
                st.markdown("<span style='font-size:12px;color:#64748b'>条件单（触发才动）：" + "；".join(
                    f"{_stk_link(w.get('name'), w.get('code'))}·{w.get('trigger')}" for w in _wc9f)
                    + "</span>", unsafe_allow_html=True)
            st.caption("硬约束：单月风险敞口≤$125｜无合格候选=空仓待机（合格结果）｜"
                       "**禁止为凑目标下调门槛或放大仓位**｜miss不加倍追｜财报窗强平")
        _ftr9 = _fpm9.get("triad_review") or _fp9.get("triad_review") or {}
        if _ftr9:
            _fcs9 = _ftr9.get("consensus") or {}
            _fst9 = str(_fcs9.get("state") or "未完成")
            if "通过" in _fst9:
                st.success(f"双层复核：{_fst9}｜分析 {_ftr9.get('reviewed_at') or _ftr9.get('checked_at') or '?'}")
            elif "分歧" in _fst9:
                st.error(f"双层复核：{_fst9}｜{_fcs9.get('action','')}｜不取消已发布计划的止损/止盈/时间闸")
            else:
                st.warning(f"双层复核：{_fst9}｜{_fcs9.get('action','')}")
            _fcols9 = st.columns(2)
            for _fc9, _frv9 in zip(_fcols9, _ftr9.get("reviews") or []):
                _fc9.caption(
                    f"**{_frv9.get('party')}** · {_frv9.get('verdict')}\n\n"
                    f"{_frv9.get('reason')}\n\n分析 {_frv9.get('analysis_at','?')}")
    except Exception as _e9fp:
        st.caption(f"⚠️ Fable计划渲染失败：{type(_e9fp).__name__}: {str(_e9fp)[:110]}")


# ═══════════════════════════════════════════════════════════════
# 【V88·出口清单 + 三方台账 + 规则版本】2026-08-06 用户批准接入
# 三个模块建好后页面一个都没接（grep=0）—— 又是「写了不等于生效」。
# 出口清单=以始为终（每只票同时带进场与出场，出口是价格不是文字）；
# 三方台账=默默统计四方胜率（只记录不参与决策）；规则版本=答案变时先比版本号。
# ═══════════════════════════════════════════════════════════════
with _v88_hold_mod9, st.expander("🛡️ 原退出条件", expanded=False):
    try:
        import importlib, sys as _sy8
        _rp8 = str(core_root() / "src")
        if _rp8 not in _sy8.path:
            _sy8.path.insert(0, _rp8)
        _ep8 = importlib.import_module("exit_plan")
        _eo8 = _cbj9("exit_plan.json") or {}
        if not _eo8:
            raise ValueError("退出计划尚未生成，等待后台更新")
        _es8 = _eo8.get("stats") or {}
        st.caption(f"覆盖 {_es8.get('total')} 只｜门派 {_es8.get('by_school')}｜"
                   f"有止盈线 {_es8.get('has_take_profit')}｜有止损线 {_es8.get('has_stop')}"
                   f"｜规则 {_eo8.get('ruleset_version')}")
        _t8 = st.tabs(["💼 持仓", "📊 评级榜", "⛔ 赔率挡下"])
        for _tb8, _sc8 in zip(_t8, ("持仓", "评级榜", "赔率挡下")):
            with _tb8:
                st.markdown(_ep8.render_md(_eo8, _sc8))
    except Exception as _e8x:
        st.caption(f"⚠️ 出口清单渲染失败：{type(_e8x).__name__}: {str(_e8x)[:120]}")

with _v88_history_details, st.expander("⚖️ 历史审核记录", expanded=False):
    try:
        _lg8 = _cbj9("three_way_ledger.json") or {}
        _sm8 = _lg8.get("summary") or {}
        if not _sm8:
            st.caption("尚未核算（T+1 才有结果）。已留档日期："
                       + "、".join(sorted(_lg8.get("days") or {})))
        else:
            st.markdown("**累计**（弃权不计入分母，但单独统计表态率——"
                        "一个从不表态的一方，胜率再高也没有价值）")
            st.dataframe([{"方": k, **v} for k, v in (_sm8.get("cumulative") or {}).items()],
                         hide_index=True, width='stretch')
            _rk8 = _sm8.get("rescue_kill") or {}
            if _rk8:
                st.caption(f"🛟救援 {_rk8.get('救援')}｜🔪误杀 {_rk8.get('误杀')}"
                           f"｜净值 {_rk8.get('净值')} —— {_rk8.get('结论')}"
                           "　（救援/误杀比才是「合议值不值」的真指标，胜率不是）")
    except Exception as _e8y:
        st.caption(f"⚠️ 三方台账渲染失败：{type(_e8y).__name__}: {str(_e8y)[:120]}")

with _v88_system_details, st.expander("📐 规则版本与生效记录", expanded=False):
    try:
        _rs8 = _cbj9("ruleset.json") or {}
        st.caption(f"**{_rs8.get('version')}**　冻结={_rs8.get('frozen')}　"
                   f"自检={'✅' if (_rs8.get('self_check') or {}).get('ok') else '❌'}　"
                   f"{_rs8.get('declaration','')}")
        st.dataframe([{"闸": g.get("id"), "名称": g.get("name"),
                       "上线": str(g.get("since"))[:16], "影响": g.get("affects")}
                      for g in (_rs8.get("gates") or [])], hide_index=True, width='stretch')
        _cl8 = (_rs8.get("changelog") or [{}])[0]
        st.markdown(f"**最近变更 {_cl8.get('version')}** @ {_cl8.get('at')}")
        for _c8 in (_cl8.get("changes") or []):
            st.markdown(f"- {_c8}")
        st.caption(f"翻转：{_cl8.get('flip')}")
        _ds8 = _cbj9("dropout_sentry.json") or {}
        if _ds8:
            _dst8 = _ds8.get("stats") or {}
            st.caption(f"🕳 静默掉队哨兵：🔴{_dst8.get('silent_dropout')} "
                       f"🟠未进评级段{_dst8.get('missing_from_grade')} —— "
                       "区分『评估后不合格』(正常)与『从未被评估』(bug)")
    except Exception as _e8z:
        st.caption(f"⚠️ 规则版本渲染失败：{type(_e8z).__name__}: {str(_e8z)[:120]}")

# ═══════════════════════════════════════════════════════════════
# 【V88·机构风向标 2026-07-18 用户点单】权威机构研报评级/外资观点综合分析推荐池,
# AI按时间档(明天/本周/下周/本月下月)给布局。私仓流水线生成,这里零网络秒开。
# ═══════════════════════════════════════════════════════════════
with st.expander("🏛️ 机构风向标 · 权威研报评级×系统推荐池（明天/本周/下周布局）", expanded=False):
    # 【V88·立刻更新 2026-07-25】缓存模块强刷按钮(60s防重,通用helper)
    def _rf_inst9():
        import sys as _s9z
        _p9z = str(core_root() / "src")
        if _p9z not in _s9z.path:
            _s9z.path.insert(0, _p9z)
        import importlib as _il9z
        import institutional_signals as _m9z
        _il9z.reload(_m9z)
        _m9z.build(force=True)
    try:
        _v88_refresh9("机构风向标", "约30-60秒·AI档12h节流仍生效", _rf_inst9, "rf_inst9")
    except Exception:
        pass

    try:
        _inst9 = json.loads((core_root() / "data" /
                             "institutional_signals.json").read_text(encoding="utf-8"))
        # 【2026-07-18 出处铁律】机构信息(尤其带评级/家数得分的)必须写明出自哪里
        st.caption(f"🕒 {_inst9.get('generated_at', '')} · 近3日研报{_inst9.get('reports_n', 0)}篇 · "
                   "📚 出处：券商研报评级/目标价/标题=东财研报库(公开数据)；外资观点=新闻流标题原文。"
                   "研报全文为付费品,此处为公开精华 · AI综合仅供参考")
        _aib9 = _inst9.get("ai_brief") or {}
        if _aib9:
            st.markdown(f"**🧭 机构综合布局**：主线 **{_aib9.get('机构共识主线', '—')}** ｜ "
                        f"分歧 {_aib9.get('机构分歧', '—')}")
            # 【V88·分市场 2026-07-19 用户点单"没说明中美港哪个市场"】三市场各一栏;
            # 兼容旧扁平结构(无市场键→单栏原样,不炸)。
            _mkts9x = [(_m9, _fl9) for _m9, _fl9 in (("A股", "🇨🇳"), ("港股", "🇭🇰"), ("美股", "🇺🇸"))
                       if isinstance(_aib9.get(_m9), dict)]
            if _mkts9x:
                _cols9x = st.columns(len(_mkts9x))
                for _ci9, (_m9, _fl9) in enumerate(_mkts9x):
                    _sub9 = _aib9[_m9]
                    _rows9x = "".join(
                        f"<div style='padding:1px 0'><b style='color:#7c3aed'>{_k9}</b>：{_sub9[_k9]}</div>"
                        for _k9 in ("明天", "本周", "下周", "本月及下月") if _sub9.get(_k9))
                    with _cols9x[_ci9]:
                        st.markdown(f"<div style='background:#f8fafc;border-left:3px solid #7c3aed;"
                                    f"border-radius:6px;padding:.4rem .6rem;font-size:12px'>"
                                    f"<div style='font-weight:800;margin-bottom:2px'>{_fl9} {_m9}</div>"
                                    f"{_rows9x}</div>", unsafe_allow_html=True)
                st.caption("出处：AI综合自东财研报库近3日研报+新闻流外资观点，按三市场分别归纳，非任何机构原话")
            else:
                _tl9x = "".join(f"<div style='padding:2px 0'><b>{_k9}</b>：{_aib9[_k9]}</div>"
                                for _k9 in ("明天", "本周", "下周", "本月及下月") if _aib9.get(_k9))
                st.markdown(f"<div style='background:#f8fafc;border-left:3px solid #7c3aed;"
                            f"border-radius:6px;padding:.4rem .7rem;font-size:13px'>{_tl9x}"
                            f"<div style='padding:2px 0;color:#94a3b8'>出处：AI综合自上述东财研报库近3日"
                            f"研报+新闻流外资观点，非任何机构原话（下轮流水线升级为三市场分列）</div></div>",
                            unsafe_allow_html=True)
        _res9x = _inst9.get("resonance") or []
        if _res9x:
            st.markdown("**🤝 机构×系统共振**（机构覆盖且在你的池/持仓/自选——双重背书·"
                        "评级与目标价出处:东财研报库,机构名见「覆盖机构」列）")
            st.dataframe([{"股票": x["stock"], "在系统": x["source"],
                           "覆盖机构": "、".join(x["orgs"][:3]) + (f" 等{len(x['orgs'])}家" if len(x["orgs"]) > 3 else ""),
                           "看多家数": x["buy_n"],
                           "最高目标价": (str(max(x["targets"])) if x.get("targets") else "—"),
                           "研报精华": x.get("gist") or "—"}
                          for x in _res9x], hide_index=True, use_container_width=True)
        _cons9 = _inst9.get("consensus") or []
        if _cons9:
            # 【2026-07-18 用户点单】共识股不只给家数,每只附≤30字研报精华+机构名出处
            st.markdown("**📌 机构共识股**（≥2家覆盖·附研报精华·出处:东财研报库）")
            st.markdown("<div style='font-size:13px;line-height:1.6'>" + "".join(
                f"<div>· {_stk_link(c['stock'], c['stock'])}"
                f"（{'、'.join((c.get('orgs') or [])[:2])}"
                f"{('等' + str(len(c['orgs'])) + '家') if len(c.get('orgs') or []) > 2 else ''}）："
                f"<span style='color:#475569'>{c.get('gist') or '—'}</span></div>"
                for c in _cons9[:8]) + "</div>", unsafe_allow_html=True)
        # 【V88·外资投行观点层 2026-07-24 用户点单"高级金融机构日报→周/月提示"】
        # ①AI周月提示行(只依据外资材料) ②观点列表带银行名+原文链接。
        # 如实边界:投行原始日报为付费品,此处为新闻公开转述(财联社等)。
        _fb9 = (_aib9 or {}).get("外资投行") or {}
        if isinstance(_fb9, dict) and (_fb9.get("本周") or _fb9.get("本月")):
            st.markdown(f"<div style='background:#faf5ff;border-left:3px solid #9333ea;border-radius:6px;"
                        f"padding:.35rem .6rem;font-size:13px'><b>🏦 外资投行周月提示</b>"
                        f"（高盛/大摩/汇丰等·出处:新闻公开转述·AI综合非原话）："
                        + (f"<b>本周</b>:{_fb9.get('本周')} " if _fb9.get("本周") else "")
                        + (f"｜<b>本月</b>:{_fb9.get('本月')}" if _fb9.get("本月") else "")
                        + "</div>", unsafe_allow_html=True)
        if _inst9.get("org_news"):
            def _orgline9(_t):
                if isinstance(_t, dict):
                    _ttl9 = str(_t.get("title") or "")
                    _lk9 = str(_t.get("link") or "")
                    _bk9 = str(_t.get("bank") or "")
                    _body9 = (f"<a href='{_lk9}' target='_blank' "
                              f"style='color:inherit;text-decoration:underline'>{_ttl9}</a>"
                              if _lk9 else _ttl9)
                    return f"· <b style='color:#9333ea'>{_bk9}</b> {_body9}"
                return f"· {_t}"
            st.markdown("**🌐 外资/权威观点**（出处:新闻流标题原文·下划线可点原文）：" + "<br>".join(
                _orgline9(_t) for _t in _inst9["org_news"][:6]), unsafe_allow_html=True)
    except Exception:
        st.info("机构风向标数据随日报流水线生成（交易日07/13/19点），稍后刷新。")

# 【V88·公告事件雷达常驻板块 2026-07-18 用户点单】"未来可以提醒或主动"——
# 自选+持仓的公司公告(可转债/回购/减持…)主动摆上台面;盘中新事件另有飞书预警。
with st.expander("⚡ 公告事件雷达 · 自选+持仓公司公告（可转债/回购/减持·主动提醒）", expanded=False):
    # 【V88·立刻更新 2026-07-25】缓存模块强刷按钮(60s防重,通用helper)
    def _rf_ann9():
        import sys as _s9z
        _p9z = str(core_root() / "src")
        if _p9z not in _s9z.path:
            _s9z.path.insert(0, _p9z)
        import importlib as _il9z
        import announcement_radar as _m9z
        _il9z.reload(_m9z)
        _m9z.build(force=True)
    try:
        _v88_refresh9("公告事件雷达", "约20-40秒", _rf_ann9, "rf_ann9")
    except Exception:
        pass

    try:
        _annj9 = json.loads((core_root() / "data" /
                             "announcements.json").read_text(encoding="utf-8"))
        st.caption(f"🕒 {_annj9.get('generated_at', '')} · 池{_annj9.get('pool_n', 0)}只 · "
                   "出处:东财公告库（A股+港股；美股8-K不覆盖，如实说明） · "
                   "来源时点见原公告；后台状态与飞书送达以运行监控及发送回执为准")
        # 【V88·全市场可转债日历 2026-07-18 用户点单】低频事件全市场一网打尽,
        # 申购/上市提前预警;⭐=正股在你的池内(抢权窗口重点)。
        _cbs9 = _annj9.get("cb_calendar") or []
        if _cbs9:
            _tdy9 = datetime.now().strftime("%Y-%m-%d")
            st.markdown("**🌐 全市场可转债日历**（近期申购/上市·出处:东财可转债数据）")
            st.markdown("<div style='font-size:13px;line-height:1.7'>" + "".join(
                f"<div>· <b>{_x9.get('bond')}</b>（正股 {_stk_link(_x9.get('stock'), _x9.get('stock_code'))}）"
                + (f"申购日<b>{_x9.get('apply_date')}</b>" if str(_x9.get('apply_date') or '') >= _tdy9
                   else f"已申购·上市日{_x9.get('list_date') or '待定'}")
                + f"·评级{_x9.get('rating')}·{_x9.get('scale')}亿"
                f"<span style='color:#64748b'>——{_x9.get('note')}</span>"
                + ("<b style='color:#b45309'> ⭐池内正股·抢权窗口</b>" if _x9.get('in_pool') else "")
                + "</div>"
                for _x9 in _cbs9[:8]) + "</div>", unsafe_allow_html=True)
            st.caption("申购及上市日期须核对原公告；提醒是否送达以回执为准，"
                       "配售权、摊薄和价格回落应合并核验；此日历不授予交易权限。")
        # 【V88·即将申购储备 2026-07-18 用户点单"已申购的没操作空间"】同意注册=
        # 申购日随时公告(一般数周内),提前进入观察;池内正股注册当轮盘中就会推飞书。
        _pipe9 = _annj9.get("cb_pipeline") or []
        if _pipe9:
            _pc9 = _annj9.get("cb_pipe_counts") or {}
            # 【2026-07-18 用户点单"看不出重点"】按确定性关注分排序+分色,裸数值带说明
            st.markdown("**🟢 已注册待发·即将申购储备**（申购日随时公告·按关注分排序·出处:集思录）")
            st.markdown("<div style='font-size:13px;line-height:1.7'>" + "".join(
                f"<div>· <b style='color:"
                + ("#dc2626" if _x9.get('tag') == '🔴重点' else
                   ("#b45309" if _x9.get('tag') == '🟡关注' else "#94a3b8"))
                + f"'>{_x9.get('tag', '')}{_x9.get('score', '')}分</b> "
                f"{_stk_link(_x9.get('stock'), _x9.get('stock_code'))} "
                f"{str(_x9.get('stage_date'))[5:]}同意注册"
                f"<span style='color:#64748b'>｜{_x9.get('why', '')}</span>"
                + ("<b style='color:#b45309'> ⭐池内正股</b>" if _x9.get('in_pool') else "")
                + "</div>"
                for _x9 in _pipe9[:6]) + "</div>", unsafe_allow_html=True)
            st.caption(f"关注分=确定性规则（转股价值≥100含权足+30 / 规模≤5亿稀缺+20 / PB>1可下修+10 "
                       f"/ 注册≤30天+10 / 池内正股+15 / 底分15），非AI非收益预测；"
                       f"转股价值=正股价÷转股价×100，≥100=转债一上市就有股性支撑。"
                       f"注册待发共{_pc9.get('reg', len(_pipe9))}只｜过会排队{_pc9.get('passed', 0)}只"
                       f"·更早期(受理/股东大会/预案){_pc9.get('early', 0)}只——注册批文一般12个月内有效，"
                       "公告申购日后自动进上方日历并推提醒。")
        _evs_all9 = _annj9.get("events") or {}
        if not _evs_all9:
            st.info("当前缓存没有可展示的公告事件；需核对采集状态与日期，不能据此认定近5日没有公告。")
        else:
            _grp9 = {}
            for _blk9 in _evs_all9.values():
                for _e9 in (_blk9.get("items") or []):
                    _grp9.setdefault(str(_e9.get("dir")), []).append((_blk9, _e9))
            for _dk9, _hd9 in (("event", "⚡ 事件博弈（两面：抢权/输血 vs 摊薄——只语境化不翻案）"),
                               ("bull", "🔴 偏多公告"), ("bear", "🟢 偏空公告")):
                if _grp9.get(_dk9):
                    st.markdown(f"**{_hd9}**")
                    st.markdown("<div style='font-size:13px;line-height:1.7'>" + "".join(
                        f"<div>· {_stk_link(_b9.get('name'), _b9.get('code'))} "
                        f"{str(_e9.get('date'))[5:]}「{str(_e9.get('title'))[:30]}」"
                        f"<span style='color:#64748b'>——{_e9.get('note')}</span></div>"
                        for _b9, _e9 in _grp9[_dk9][:8]) + "</div>", unsafe_allow_html=True)
            st.caption("公告是复核证据，不能单独授权开仓；中央处于回避或未满足原条件时，事件机会也不可执行。")
    except Exception:
        st.info("公告证据读取失败或尚未生成，请查数据与后台状态；当前无法判断是否存在新公告。")

# 【V88·散户情绪三榜 2026-07-19 用户问"热榜细节在哪"】此前只做池内命中提示,
# 这里给完整三榜详情——反指标语义:越热闹越要冷静,不是买入榜。
with st.expander("🔥 散户情绪三榜 · 东财人气/雪球热股/雅虎热搜（反指标·拥挤观察）", expanded=False):
    # 【V88·立刻更新 2026-07-25】缓存模块强刷按钮(60s防重,通用helper)
    def _rf_intel9():
        import sys as _s9z
        _p9z = str(core_root() / "src")
        if _p9z not in _s9z.path:
            _s9z.path.insert(0, _p9z)
        import importlib as _il9z
        import intel_feed as _m9z
        _il9z.reload(_m9z)
        _m9z.build(force=True)
    try:
        _v88_refresh9("散户情绪三榜", "约20-40秒", _rf_intel9, "rf_intel9")
    except Exception:
        pass

    try:
        _i3h9 = _v88_intel9()
        _c1h9, _c2h9, _c3h9 = st.columns(3)
        with _c1h9:
            st.markdown("**🇨🇳 东财股吧人气榜**")
            _rows_e9 = []
            for _h9x in (_i3h9.get("hot_raw") or [])[:10]:
                _nm9x = str(_h9x.get("canon") or _h9x.get("code"))
                try:
                    import cloud_engine as _ce_h9
                    _sc9x = str(_h9x.get("code") or "")
                    _yf9x = (_sc9x[2:] + (".SS" if _sc9x[:2] == "SH" else ".SZ")) if _sc9x[:2] in ("SH", "SZ") else _sc9x
                    _nm9x = _ce_h9.name_of(_yf9x) or _nm9x
                except Exception:
                    pass
                _dual9x = "🔥🔥" if _h9x.get("xq_rank") else ""
                # 【热度影响 2026-07-19】当日涨跌+确定性判读:热了会怎样一眼可见
                _chg9x = _h9x.get("chg")
                _chg_html9x = (f"<b style='color:{'#dc2626' if _chg9x > 0 else '#16a34a'}'>{_chg9x:+.1f}%</b>"
                               if isinstance(_chg9x, (int, float)) else "")
                _imp9x = str(_h9x.get("impact") or "")
                _imp_html9x = (f"<span style='font-size:12px;color:"
                               + ("#dc2626" if "过热" in _imp9x else ("#b45309" if ("冰点" in _imp9x or "蹿升" in _imp9x) else "#94a3b8"))
                               + f"'>{_imp9x}</span>") if _imp9x else ""
                _rows_e9.append(f"<div>#{_h9x.get('rank')} {_stk_link(_nm9x, _yf9x if 'SH' in str(_h9x.get('code', '')) or 'SZ' in str(_h9x.get('code', '')) else _h9x.get('code'))}"
                                f" {_chg_html9x} {_imp_html9x}"
                                f" {_dual9x}{('<span style=\'color:#94a3b8;font-size:12px\'>雪#' + str(_h9x.get('xq_rank')) + '</span>') if _h9x.get('xq_rank') else ''}</div>")
            st.markdown("<div style='font-size:13px;line-height:1.7'>" + "".join(_rows_e9) + "</div>",
                        unsafe_allow_html=True)
        with _c2h9:
            st.markdown("**❄️ 雪球热股榜**")
            st.markdown("<div style='font-size:13px;line-height:1.7'>" + "".join(
                f"<div>#{_x9h.get('rank')} {_stk_link(_x9h.get('name'), _x9h.get('code'))} "
                + (f"<b style='color:{'#dc2626' if _x9h.get('chg', 0) > 0 else '#16a34a'}'>{_x9h.get('chg'):+.1f}%</b> "
                   if isinstance(_x9h.get("chg"), (int, float)) else "")
                + (f"<span style='font-size:12px;color:#94a3b8'>热度{'+' if (_x9h.get('heat_chg') or 0) >= 0 else ''}{_x9h.get('heat_chg')}</span>"
                   if _x9h.get("heat_chg") is not None else "")
                + f"</div>"
                for _x9h in (_i3h9.get("hot_xq_raw") or [])[:10]) + "</div>", unsafe_allow_html=True)
        with _c3h9:
            st.markdown("**🇺🇸 雅虎美股热搜**")
            # 【V88·美股榜补文字说明 2026-07-24 用户抓"美股压根没有文字说明"】
            # 涨跌%+判读与东财同口径:🔴热+涨≥3%=过热追高险 🟠热+跌≤-3%=情绪冰点查错杀 其余=拥挤观察
            def _us_note9(_u):
                _cg = _u.get("chg")
                if _cg is None:
                    return "<span style='font-size:12px;color:#94a3b8'>拥挤观察(涨跌待更新)</span>"
                _cgc = "#dc2626" if _cg >= 0 else "#16a34a"
                _tag = ("🔴过热·追高险" if _cg >= 3 else
                        ("🟠情绪冰点·查错杀" if _cg <= -3 else "拥挤观察"))
                _tc = "#dc2626" if _cg >= 3 else ("#b45309" if _cg <= -3 else "#94a3b8")
                return (f"<b style='color:{_cgc}'>{_cg:+.1f}%</b> "
                        f"<span style='font-size:12px;color:{_tc}'>{_tag}</span>")
            st.markdown("<div style='font-size:13px;line-height:1.7'>" + "".join(
                f"<div>#{_u9h.get('rank')} {_stk_link(_u9h.get('name') or _u9h.get('symbol'), _u9h.get('symbol'))} "
                + _us_note9(_u9h) + "</div>"
                for _u9h in (_i3h9.get("hot_us_raw") or [])[:10]) + "</div>", unsafe_allow_html=True)
        _hd9v = _v88_rate_line9("hot_dual", "双榜热股隔日")
        st.caption(f"🕒 {_i3h9.get('generated_at', '')} · 出处:东财股吧/雪球(匿名token)/雅虎trending · "
                   "🔥🔥=东财雪球双榜同上(拥挤信号更硬) · 判读:🔴热+大涨=过热追高险 🟠热+大跌=情绪冰点查错杀"
                   " ⚡排名蹿升=情绪突变 · 交易日随流水线6小时更新 · "
                   "纪律:人气榜=散户扎堆反指标——你的票冲上榜是提醒冷静的钟,不是加仓的号"
                   + (f"　｜　{_hd9v}（上涨率低=反指标被实盘证实）" if _hd9v else ""))
    except Exception:
        st.info("情绪榜数据待流水线生成。")

# ===V88_PAGE_BREAK:RADAR===
with st.expander("🆕 打新雷达 · 中美港新股申购（提前布局）", expanded=False):
    # 【V88·立刻更新 2026-07-25】缓存模块强刷按钮(60s防重,通用helper)
    def _rf_ipo9():
        import sys as _s9z
        _p9z = str(core_root() / "src")
        if _p9z not in _s9z.path:
            _s9z.path.insert(0, _p9z)
        import importlib as _il9z
        import ipo_radar as _m9z
        _il9z.reload(_m9z)
        _m9z.build_radar(force=True)
    try:
        _v88_refresh9("打新雷达", "约20-40秒", _rf_ipo9, "rf_ipo9")
    except Exception:
        pass

    try:
        _ipo9 = json.loads((core_root() / "data" / "ipo_radar.json")
                           .read_text(encoding="utf-8"))
        _ipo_rows9 = _ipo9.get("rows") or []
        _cn_ipo_status9 = _ipo9.get('cn_source') or {}
        if _cn_ipo_status9:
            st.caption('A股申购源：东方财富公开沪深/北交所日历 · 新增费用0元 · ' +
                       ('分页核对完成' if _cn_ipo_status9.get('complete') else '部分来源缺失，不能视为无新股'))
        if _ipo_rows9:
            st.caption(f"🕒 {_ipo9.get('generated_at', '')} · 日历观察与规则风险提示 · 不代表3A评级或申购指令 · 以正式公告为准")

            def _ipo_tb9(_lst9):
                _out9 = []
                for _r9x in _lst9:
                    if _r9x.get("market") == "A股":
                        _d9x = str(_r9x.get("apply_date") or "")
                        _d9x = f"{_d9x[:4]}-{_d9x[4:6]}-{_d9x[6:]}" if len(_d9x) == 8 else _d9x
                        _px9 = (f"{_r9x.get('price')}元/PE" + (f"{_r9x['pe']:g}" if _r9x.get("pe") is not None else "未披露") if _r9x.get("price") else "未披露")
                        _sz9 = f"募{_r9x.get('funds_yi'):g}亿" if _r9x.get("funds_yi") else (f"{_r9x['amount_wan']:g}万股" if _r9x.get("amount_wan") is not None else "规模未披露")
                    elif _r9x.get("market") == "港股":
                        _d9x = str(_r9x.get("apply_date") or "")
                        _px9 = _r9x.get("price_range") or "未披露"
                        _sz9 = f"入场费{_r9x.get('entrance_fee')}港元" if _r9x.get("entrance_fee") else "—"
                    else:
                        _d9x = str(_r9x.get("apply_date") or "")
                        _px9 = _r9x.get("price_range") or "未披露"
                        _sz9 = f"募{_r9x.get('raise_usd', 0) / 1e8:.1f}亿$" if _r9x.get("raise_usd") else "—"
                    # 【V88·中签率公示 2026-07-24 用户点单】A股=东方财富 ONLINE_ISSUE_LWR,港股=富途luckyRatio,
                    # 美股=配售制无中签率(如实);未披露标"待披露"不留空。
                    if _r9x.get("market") == "A股":
                        _bl9 = _r9x.get("ballot_pct")
                        _lot9 = f"{float(_bl9):g}%" if _bl9 else "待披露"
                    elif _r9x.get("market") == "港股":
                        _lr9 = _r9x.get("lucky_ratio")
                        _lot9 = (f"{_lr9}" + ("" if "%" in str(_lr9) else "%")) if _lr9 else "待披露"
                    else:
                        _lot9 = "配售制·无中签率"
                    _out9.append({"市场": _r9x.get("market"), "新股": f"{_r9x.get('name')}（{_r9x.get('code')}）",
                                  "申购/定价日": _d9x, "价格/PE": _px9, "规模": _sz9, "中签率": _lot9,
                                  "日历规则标签": _r9x.get("grade", ""), "规则风险说明": _r9x.get("why", "")})
                return _out9

            # 日历沿用Top3；完整原始日历保留后台。
            _rank9 = {"✅ 值得打": 0, "🔎 重点关注": 1, "🔎 关注": 2, "✅ 常规打": 3,
                      "⚪ 一般": 4, "⚠️ 谨慎": 5, "🚫 回避": 6}
            _picks9 = sorted(_ipo_rows9, key=lambda r: (_rank9.get(r.get("grade", ""), 9),
                                                        str(r.get("apply_date") or "")))[:3]
            st.markdown("**📅 IPO日历关注 Top3**")
            with st.expander('📊 历史首日观察（分市场，非未来胜率）', expanded=False):
                from ipo_radar import refresh_history_labels as _ipo_history_labels
                _ipo_history9 = _ipo_history_labels(_ipo9)
                for _history_market in ('A股','港股','美股'):
                    _hist_rows9 = [r for r in _ipo_history9.get('rows',[]) if r.get('market') == _history_market]
                    _line9 = next((r.get('odds_line') for r in _hist_rows9 if r.get('odds_line')), None)
                    st.caption(_line9 or f'{_history_market}：暂无可核实的首日历史观察；不表示获配率或未来收益。')
            st.dataframe(_ipo_tb9(_picks9), hide_index=True, use_container_width=True)
            _rest9 = [r for r in _ipo_rows9 if r not in _picks9]
            if _rest9:
                st.caption(f"后台另保留 {len(_rest9)} 只原始日历记录。")
            st.caption("A股/美股=申购或定价日；港股=招股截止日、规模列为入场费/手（数据源：免费行情源/Nasdaq/富途）。")
            _listed9 = _ipo9.get("hk_listed") or []
            if _listed9:
                from display_limits import market_top as _ipo_market_top
                _listed_display9 = _ipo_market_top([{**r, 'market': '港股'} for r in _listed9])
                _win9 = sum(1 for x in _listed9 if str(x.get("first_day", "")).startswith("+"))
                with st.expander(f"📉 近期港股打新回看（{len(_listed9)}只上市·首日红盘{_win9}只，验证打新性价比）"):
                    st.dataframe([{"新股": f"{x.get('name')}（{x.get('code')}）",
                                   "暗盘": x.get("dark") or "—", "首日": x.get("first_day") or "—",
                                   "较发行价累计": x.get("cum") or "—"} for x in _listed_display9],
                                 hide_index=True, use_container_width=True)
        else:
            st.info("未来窗口内暂无披露的新股申购/定价（随日报每日更新）。")
    except Exception:
        st.info("打新雷达数据待今日日报生成（07/13/19点自动更新）。")

# ═══════════════════════════════════════════════════════════════
# 【V88·涨停接力雷达】A股涨停主线+连板梯队+接力候选（2026-07-16 用户点单：市场天天有涨停，
# 系统不该说"今天无推荐"）。数据由私仓日报流水线生成，这里零网络秒开。
# ═══════════════════════════════════════════════════════════════

    # ═══════════════════════════════════════════════════════════════
    # 【瘦身2026-07-27用户批准】全行业机会雷达已删(手动90秒扫描,被五行业代表+全市场机会扫描+涨停接力全面覆盖)
    # 【V88·触底拐点机会池 2026-07-19 用户点单恢复+现代化】前身"深度回调机会池"。
    # 优质池116只里"跌得深(52周双口径)且拐点已现"的中美港各Top10——仍在寻底的不收。
# 新的左侧观察已统一四连跌、52周/波段/已覆盖历史低位和经营依据。
# 旧机会分/概率池退出当前推荐展示；原始信号与研究记录保留下载。
with _v88_history_details, st.expander("📁 旧低位池档案 · 已合并至左侧观察", expanded=False):
    st.caption("当前低位与企稳提醒统一见上方左侧观察；旧机会分与概率不再作为当前推荐展示。")
    st.markdown('[打开统一左侧观察 ↗](#v88-left-entry-watch)')
    _legacy_bottom_path = core_root() / 'data' / 'bottom_turn_pool.json'
    if _legacy_bottom_path.is_file():
        st.download_button('下载旧版研究原始记录', _legacy_bottom_path.read_bytes(),
                           file_name='v88-legacy-bottom-turn.json', mime='application/json',
                           key='v88_legacy_bottom_download')
    else:
        st.caption('暂无旧版记录；当前提醒按新的核验结果展示。')

# 用户明确要求删除重复持仓展示：旧 Excel“我的持仓/AI组合分析”不再渲染；
# 数据与函数保留兼容，唯一入口为上方“持仓决策中心”。

# ═══════════════════════════════════════════════════════════════
# 【V90.3】行业热力已整合到「全球市场概览」第4个Tab
# ═══════════════════════════════════════════════════════════════

# 【瘦身2026-07-27用户批准】旧版AI市场简报已删(07-17收纳观察期过,被权威日报+AI宏观卡取代)
# ===V88_PAGE_BREAK:RESEARCH===
# 【V88·个股直达】所有个股点击统一落到这里，不再让用户在长页面中寻找。
st.markdown('<div id="v88-deep-analysis"></div>', unsafe_allow_html=True)
_deep_code_now = st.session_state.get("scan_selected_code")
if (_deep_code_now and
        st.session_state.get("_deep_scroll_seen_code") != str(_deep_code_now)):
    st.session_state["_deep_scroll_pending"] = str(_deep_code_now)
    st.session_state["_deep_scroll_seen_code"] = str(_deep_code_now)

_deep_scroll_code = st.session_state.pop("_deep_scroll_pending", None)
if _deep_scroll_code:
    try:
        import streamlit.components.v1 as _components_deep
        _components_deep.html(
            """<script>
            (() => {
              let tries = 0;
              let stable = 0;
              const jump = () => {
                const el = parent.document.getElementById('v88-deep-analysis');
                if (el) {
                  el.scrollIntoView({behavior: 'instant', block: 'start'});
                  const top = el.getBoundingClientRect().top;
                  stable = Math.abs(top) < 12 ? stable + 1 : 0;
                  return true;
                }
                stable = 0;
                return false;
              };
              // Streamlit会分批把首页、持仓和深度报告插入DOM。首次找到锚点就停止
              // 会在后续模块完成后发生位置漂移，因此持续校正，直到稳定约2秒。
              jump();
              const timer = setInterval(() => {
                tries += 1;
                jump();
                if ((stable >= 8 && tries >= 8) || tries > 180) clearInterval(timer);
              }, 250);
            })();
            </script>""",
            height=0,
        )
    except Exception:
        pass

if _deep_code_now:
    st.markdown("### ⚔️ 个股深度分析")
    # 【板块轮转定位 2026-07-31 用户抓"云端有网页版没有"(ARXS案)】深度页开门先答:
    # 这票所属板块在2/5/8/16周轮转里处于什么位置——三端同源rotation_forecast
    try:
        _dp_code9 = (st.session_state.get("scan_selected_code")
                     or (dict(st.query_params).get("q") if hasattr(st, "query_params") else None))
        if _dp_code9:
            _dp_code9 = str(_dp_code9)
            from stock_profile_view import profile as _dp_profile_read9, compact_html as _dp_profile_compact9
            _dp_company9 = _dp_profile_read9(_dp_code9)
            try:
                from modules.sector_map import get_sector as _gs9dp
                _dp_sec9 = _gs9dp(_dp_code9, str(st.session_state.get("scan_selected_name") or ""))
                if _dp_sec9 == '待核行业':
                    _dp_sec9 = ''
            except Exception:
                _dp_sec9 = ""
            _mk9dp = ("A股" if _dp_code9.endswith((".SS", ".SZ", ".SH", ".BJ"))
                      else "港股" if _dp_code9.endswith(".HK") else "美股")
            _blk9dp = ((_nwj9("rotation_forecast.json").get("markets") or {}).get(_mk9dp) or {})
            _hits9dp = []
            for _hz9dp in ("2周", "5周", "8周", "16周"):
                for _s9dp in (_blk9dp.get(_hz9dp) or []):
                    if _dp_sec9 and (_dp_sec9 in str(_s9dp.get("name", ""))
                                     or str(_s9dp.get("name", "")) in _dp_sec9):
                        _hits9dp.append(f"<b>{_hz9dp}</b> {_s9dp.get('name')} 分{_s9dp.get('score')}"
                                        f"<span style='font-size:8px;color:#94a3b8'>·{str(_s9dp.get('reason'))[:26]}"
                                        f"·失效:{str(_s9dp.get('invalid'))[:16]}</span>")
            st.markdown("<div class='v88-deep-sector' style='font-size:12px;background:#f0f9ff;border-left:3px solid #0369a1;"
                        "padding:.35rem .6rem;border-radius:6px'>🧭 <b>板块轮转定位</b>"
                        + (_dp_profile_compact9(_dp_code9) if _dp_company9.get('industry') else " · 公司行业待核")
                        + (f"｜轮动映射[{_dp_sec9}]："+"；".join(_hits9dp) if _hits9dp else
                           "｜暂无匹配的强板块轮动证据；不改变公司行业与审核分。")
                        + "</div>", unsafe_allow_html=True)
    except Exception:
        logging.exception("[V88] 深度页板块轮转段失败")

# 【V92→V88】全量云端搜索已前置到页顶槽位（_v88_search_slot），此处不再重复渲染，
# 避免 stock_search_input 组件键重复。深度分析仍从 session_state 读取选中股票。
st.markdown("---")

# 【V77.1调试】检测点击触发 - 添加详细日志
q_input = None
execute_analysis = False

_safe_print(f"[深度作战室] scan_selected_code = {st.session_state.get('scan_selected_code')}")

if st.session_state.get('scan_selected_code'):
    # 从 session_state 读取选中的股票
    q_input = st.session_state.scan_selected_code
    stock_name = st.session_state.scan_selected_name
    # 统一名称真源：港股搜索/深链常把 6699.HK 补成 06699.HK，旧代码又把
    # 数字代码当作名称写进 session。先从3A底稿/GPT审核/名录纠正，再渲染表格。
    try:
        import sys as _name_sys9
        _name_src9 = str(core_root() / "src")
        if _name_src9 not in _name_sys9.path:
            _name_sys9.path.insert(0, _name_src9)
        from stock_verdict import resolve_name as _resolve_sv_name9
        stock_name = _resolve_sv_name9(str(q_input), str(stock_name or ""))
        st.session_state.scan_selected_name = stock_name
    except Exception:
        stock_name = stock_name or q_input
    execute_analysis = True
    
    _safe_print(f"[深度作战室] ✅ 检测到选中股票: {stock_name} ({q_input}), execute_analysis = {execute_analysis}")
    
    # 明显的提示
    from stock_profile_view import display_name as _profile_name
    stock_name = _profile_name(stock_name, q_input)
    st.success(f"🎯 已自动选中：**{stock_name}** ({q_input})")
    
    # 【V82.9新增】显示扫描分析表格
    st.markdown("#### 📊 扫描结果（勾选2-4只股票进行对比）")
    st.caption("中央审核分与原3A列表一致；量价辅助分和情景估计另列，不作为授级依据。")
    
    # 【V96】闪烁修复：页面任何交互都会触发 rerun 并重跑本块，原进度条每次
    # 挂载/卸载+sleep(0.2) 造成"不断闪现搜索/字符波动"。15分钟内同一代码
    # 已算过 → 进度UI全部替换为无操作对象（计算照走，fetch有缓存很快）。
    class _NoopProg:
        def progress(self, *a, **k): pass
        def text(self, *a, **k): pass
        def empty(self, *a, **k): pass
    _wr_ts_key = f"warroom_done_{q_input}"
    _wr_fresh = (time.time() - st.session_state.get(_wr_ts_key, 0)) < 900
    _scan_prog = _NoopProg() if _wr_fresh else st.progress(0)
    _scan_status = _NoopProg() if _wr_fresh else st.empty()
    _scan_status.text("📊 获取数据... (0%)")
    target_c = to_yf_cn_code(q_input)
    from deep_analysis_data import fetch as _deep_fetch, report_html as _deep_report_html, snapshot_signature as _deep_signature
    from deep_cross_validation import load_context as _deep_context_load, reconcile as _deep_reconcile, html as _deep_cross_html, span as _deep_span
    _deep_context = None
    _deep_cross = None
    _deep_synthesis = {}
    _deep_annual = {}
    _search_dc = {}
    _deep_trend = {}
    try:
        _deep_context = _deep_context_load(target_c)
        from deep_review_job import render as _render_3a_review
        _render_3a_review(st, target_c)
    except Exception as _deep_report_error:
        logging.exception("中央深度报告读取失败")
        st.warning("中央报告暂未读取成功；下方技术研究不授予评级。")
    from deep_optional_data import read_local_benchmark as _deep_read_benchmark
    _deep_benchmark_frames = {}
    def _deep_benchmark(index_code):
        if index_code not in _deep_benchmark_frames:
            _deep_benchmark_frames[index_code] = _deep_read_benchmark(index_code)
        return _deep_benchmark_frames[index_code]
    _deep_extras = {}
    df_temp, _deep_quality = _deep_fetch(target_c, allow_network=False)
    if df_temp is None:
        st.caption('本地完整日线暂缺，中央原合同仍保留；补查不会改动评级或执行权限。')
        from deep_optional_data import session_once as _deep_once, bounded_call as _deep_bounded
        _deep_history_state = st.session_state.setdefault('_deep_history_requests', {})
        _deep_history_saved = _deep_once(_deep_history_state, target_c)
        _deep_retry = st.button('补查完整日线（免费来源）', key=f'deep_history_request_{target_c}',
                                disabled=bool(_deep_history_saved))
        _deep_retry_data = _deep_once(_deep_history_state, target_c, requested=_deep_retry,
            loader=lambda: {'result': _deep_bounded(lambda: _deep_fetch(target_c), timeout=10)})
        if _deep_retry_data.get('result'):
            from deep_analysis_data import revalidate_cached as _deep_revalidate_cached
            df_temp, _deep_quality = _deep_revalidate_cached(_deep_retry_data['result'], target_c)
        if _deep_retry_data and df_temp is None:
            st.caption('本会话已补查，日线仍未通过当前完整交易日核验：'
                       + str(_deep_quality.get('error_detail') or '；'.join(_deep_retry_data.get('errors') or [])))
    _deep_annual_slot = st.empty()
    _deep_cross_slot = st.empty()
    from annual_outlook import build as _annual_build, html as _annual_html
    # A rolling twelve-month view has its own evidence scope; MA200 is history,
    # and a signed short contract cannot become an annual target by relabeling.
    _deep_annual = _annual_build(_deep_context or {'code': target_c}, df_temp, _deep_quality)
    _deep_annual_slot.markdown(_annual_html(_deep_annual), unsafe_allow_html=True)
    _scan_prog.progress(0.4)
    _scan_status.text("📊 计算指标... (40%)")

    if df_temp is not None:
        m = calculate_metrics_all(df_temp, target_c, benchmark_loader=_deep_benchmark)
        if m and m.get('rs20') is None:
            st.caption('指数辅助资料未覆盖最近完整交易日：RS、Beta与Alpha留空，不按中性补分；中央评级仍查原审核。')
        _scan_prog.progress(0.8)
        _scan_status.text("📊 构建表格... (80%)")
        if m:
                # 后缀先于前导数字，避免 000558.SZ / 0661.HK 互相串市场。
                sector = {"CN":"A股", "HK":"港股", "US":"美股"}.get(parse_market_from_code(target_c), "未知市场")

                # 统一研判在同一行情快照完成后填入；禁止单项指标先授强弱结论。
                long_term = short_term = "等待同源研判"
                # 资金状态
                if len(m['df']) >= 5:
                    vol_ma5 = m['df']['Volume'].tail(5).mean()
                    last_vol = m['last']['Volume']
                    if last_vol > vol_ma5 * 1.5:
                        capital = "💰 放量"
                    elif last_vol > vol_ma5:
                        capital = "📊 正常"
                    else:
                        capital = "📉 缩量"
                else:
                    capital = "➖"
                
                # 【V82.10新增】水位 - 显示离最高点和最低点的百分比
                l250 = m['df']['Low'].tail(250).min() if len(m['df']) >= 250 else m['df']['Low'].min()
                h250 = m['df']['High'].tail(250).max() if len(m['df']) >= 250 else m['df']['High'].max()
                if h250 > l250:
                    # 离最高点的百分比（负数表示低于最高点）
                    from_high_pct = (m['last_price'] - h250) / h250 * 100
                    # 离最低点的百分比（正数表示高于最低点）
                    from_low_pct = (m['last_price'] - l250) / l250 * 100
                    water_level = f"高{from_high_pct:+.1f}% 低{from_low_pct:+.1f}%"
                else:
                    water_level = "➖"
                
                # 【V94.3】与猎手战位一键筛选完全一致的口径：
                # 同一评分、同一操作指引、同一止损/目标（共用 build_action_guidance）
                _esg_g = m.get('esg_grade', 'N/A')
                _esg_t = m.get('esg_total', 0)
                _mdf = m['df']
                _close_s = _mdf['Close']
                _t_low = float(m['last'].get('Low', m['last_price']))
                _t_high = float(m['last'].get('High', m['last_price']))
                _touch = 0
                for _n in (30, 60, 120):
                    try:
                        _mav = float(_close_s.rolling(min(_n, len(_close_s))).mean().iloc[-1])
                    except Exception:
                        continue
                    if _mav > 0:
                        _d = (m['last_price'] - _mav) / _mav * 100
                        if (_t_low <= _mav <= _t_high) or abs(_d) < 8:
                            _touch += 1
                _pos_pct = (m['last_price'] - l250) / (h250 - l250) * 100 if h250 > l250 else 50.0
                from v88_decision_core import evaluate_decision as _evaluate_search_decision
                import cloud_engine as _deep_ce
                _deep_trend = _deep_ce.analyze_trend_full(df_temp) or {}
                _search_dc = _evaluate_search_decision(
                    df_temp, _deep_trend, name=stock_name, code=target_c)
                try:
                    _deep_cross = _deep_reconcile(_deep_context or {}, df_temp, _deep_quality, _search_dc)
                    from deep_synthesis import build as _synth_build, html as _synth_html
                    _deep_synthesis = _synth_build(_deep_context or {}, df_temp, _deep_quality, _search_dc, _deep_trend, _deep_cross)
                    _deep_cross['joint_conclusion'] = _deep_synthesis
                    _deep_annual = _annual_build(_deep_context or {'code': target_c}, df_temp, _deep_quality, synthesis=_deep_synthesis)
                    _deep_cross['annual_outlook'] = _deep_annual
                    from stock_horizon import analyze as _period_analyze
                    from period_consistency import build as _period_build, html as _period_html
                    _deep_period = _period_build(target_c, df_temp, _deep_quality,
                        _period_analyze(stock_name, target_c, df_temp, full=_deep_trend, allow_ai=False),
                        _deep_trend, _deep_annual, _deep_synthesis)
                    _deep_cross['period_consistency'] = _deep_period
                    from trend_scenarios import build_stock as _future_build
                    from future_trend_visual import render as _future_render
                    _deep_future = _future_build(_deep_annual, _deep_synthesis, _deep_period, code=target_c, name=stock_name)
                    _deep_cross['future_scenario'] = _deep_future
                    from research_path_diagnostics import build as _research_paths_build
                    from research_path_view import html as _research_paths_html
                    _deep_paths = _research_paths_build(_deep_context or {}, df_temp, _deep_quality, _deep_cross)
                    _deep_cross['research_paths'] = _deep_paths
                    from evidence_visuals import deep_overview as _deep_visual_overview, contract_strip as _contract_visual
                    with _deep_annual_slot.container():
                        st.markdown(_research_paths_html(_deep_paths), unsafe_allow_html=True)
                        st.markdown(_deep_visual_overview(_deep_synthesis, _deep_annual, _deep_period), unsafe_allow_html=True)
                        st.markdown(_future_render(_deep_future), unsafe_allow_html=True)
                        with st.expander('未来一年分阶段条件与年度依据', expanded=False):
                            st.markdown(_annual_html(_deep_annual), unsafe_allow_html=True)
                            st.markdown(_period_html(_deep_period), unsafe_allow_html=True)
                    _deep_cross_slot.markdown(_deep_cross_html(_deep_cross)+_synth_html(_deep_synthesis, details=False)
                        +_contract_visual(_deep_synthesis.get('original_plan'), (_deep_synthesis.get('observations') or {}).get('last'),
                            verified=_deep_cross.get('status') in {'一致·等原条件', '一致·仍按中央执行闸'}), unsafe_allow_html=True)
                    long_term = _deep_synthesis['annual_label']
                    short_term = _deep_synthesis['momentum_label']
                except Exception:
                    logging.exception('深度交叉核验失败')
                    _deep_cross_slot.warning('交叉核验暂不可用；保留中央原合同，技术研究不提供执行许可。')
                _central_plan = ((_deep_context or {}).get('row') or {}).get('trade_plan') or {}
                _action = ('发现差异·先复核' if not _deep_cross or _deep_cross['status'] == '需复核' else
                           '仅按中央执行闸与原合同' if (_deep_context or {}).get('formal') else '仅研究·等待原条件')
                _stop_target = (f"失效{_central_plan.get('stop') or '未核实'} → 止盈"
                                f"{_deep_span(_central_plan.get('take_profit_range'))}")
                scan_result = pd.DataFrame([{
                    "代码": q_input,
                    "名称": stock_name,
                    "市场": sector,
                    "中央评级·审核分": (f"{_deep_cross.get('current_grade') or '无当前评级'} / {_deep_cross.get('audit_score') if _deep_cross.get('audit_score') is not None else '未形成当前分数'}" if _deep_cross else '核验未完成'),
                    "量价辅助分": _search_dc.get('unified_score'),
                    "短/中/长": (f"{_search_dc.get('short_score', 0)}/"
                                  f"{_search_dc.get('medium_score', 0)}/"
                                  f"{_search_dc.get('long_score', 0)}"),
                    "2周上/下估计": f"{_search_dc.get('p_up', 0)}%/{_search_dc.get('p_down', 0)}%",
                    "技术毛RR": f"{_search_dc.get('rr', 0):.2f}",
                    "规则情景值": f"{_search_dc.get('expected_pct', 0):+.1f}%",
                    "评分口径": _search_dc.get('score_version', 'V88-U2.0'),
                    "20日动量": f"{m.get('chg20d', 0) or 0:+.1f}%",
                    "RS强度": (f"{m['rs20']:+.1f}" if m.get('rs20') is not None else "—"),
                    "ESG": f"{_esg_t} ({_esg_g})",
                    "年线位置（200日）": long_term,
                    "未来一年条件主线": _deep_annual.get('headline', '年度研判待核'),
                    "RSI动量 / 转弱反证": short_term,
                    "成交量（非资金净流）": capital,
                    "水位": water_level,
                    "联合结论": _deep_synthesis.get("status", "研判未完成"),
                    "中央动作约束": _action,
                    "原合同失效/止盈": _stop_target,
                    "现价": f"{m['last_price']:.2f}"
                }])
                
                _scan_prog.progress(1.0)
                _scan_status.text("✅ 完成 (100%)")
                if not _wr_fresh:
                    time.sleep(0.2)
                st.session_state[_wr_ts_key] = time.time()
                _scan_prog.empty()
                _scan_status.empty()

                st.dataframe(
                    scan_result,
                    width='stretch',
                    hide_index=True,
                    column_config={
                        "量价辅助分": st.column_config.ProgressColumn(
                            "量价辅助分",
                            format="%d",
                            min_value=0,
                            max_value=100,
                        ),
                    }
                )
                if _deep_cross:
                    st.download_button('下载本票交叉核验凭据',
                        data=json.dumps(_deep_cross, ensure_ascii=False, indent=2),
                        file_name=f'V88-{target_c}-cross-validation.json', mime='application/json',
                        key=f'deep_cross_download_{target_c}')

                # 中央买侧评级与合同已在页首先行展示；这里仅补充独立卖侧保护。
                try:
                    from stock_verdict import verdict as _sv_verdict
                    _sv = _sv_verdict(str(target_c), stock_name)
                    if _sv.get("sell"):
                        with st.expander("独立卖侧风险证据", expanded=False):
                            from grade_card import verdict_html as _sv_html
                            st.markdown(_sv_html(_sv), unsafe_allow_html=True)
                except Exception:
                    logging.exception("独立卖侧风险证据读取失败")

                # 【V99】综合量价趋势判断（8分拆解/9态量价/9段趋势/6级水位/全价位）
                # 复用 cloud_engine（三端同一套引擎），桌面版注入真实板块强度
                try:
                    import cloud_engine as _ce
                    # Same trend term as the upper 5-component technical score.
                    # A guessed sector must not silently add another score here.
                    _F = _deep_trend
                    if _F:
                        # 【V88·拐点识别】放量+破趋势=拐点，卡片最顶端直接亮出来
                        _turn99 = _F.get("turning") or {}
                        if _turn99.get("side"):
                            (st.error if _turn99["side"] == "top" else st.success)(
                                f"**{_turn99['label']}**：" + "；".join(_turn99["signals"])
                                + "\n\n这是技术反证提示，需结合中央原合同复核；不自动改动失效价、止盈或交易动作。")
                        with st.expander(f"趋势分项拆解 · {_F['stage']}（占量价辅助分15%，不另授级）", expanded=False):
                            st.markdown(
                                f"**技术状态：{_F['stage']}**；以下为量价参考，执行以页首原合同与独立卖侧风控为准。\n\n"
                                f"- 趋势分项：**{_F['total']}/100**（在上方量价辅助分中占15%）\n"
                                f"- 趋势阶段：{_F['stage']}\n"
                                f"- 量价状态：{_F['vp']}\n"
                                f"- 水位判断：{_F['water']}（{_F['pos52']}%）→ {_F['water_adv']}\n"
                                f"- MACD状态：{_F['macd_txt']}\n"
                                f"- 均线状态：{_F['ma_state']}（{_F['ma_txt']}）\n"
                                f"- 技术支撑区：{_F['buy_zone']} ｜ 回踩参考：{_F['pullback']} ｜ 突破参考：{_F['breakout']}\n"
                                f"- 结构风险参考：{_F['stop']} ｜ 压力参考：{_F['reduce']}\n"
                                f"- 原合同失效条件：{_central_plan.get('invalidation') or '尚无已审核合同'}")
                            # 【V88·U3全局时间轴 2026-07-26】深度分析跟随作战板档位:当前档周期分+区间
                            try:
                                _gt9 = str(st.session_state.get("tb_tier9") or "今日")
                                _gh9 = {"本周": "2周", "下周": "4周", "本月": "4周", "下月": "8周",
                                        "本季度": "16周", "下季度": "32周"}.get(_gt9)
                                if _gh9:
                                    from v88_decision_core import build_horizon_facts as _bhf9
                                    _dhz9 = (_bhf9(df_temp) or {}).get("horizons") or {}
                                    _gsc9 = (_dhz9.get(_gh9) or {}).get("rule_score")
                                    if _gsc9 is not None:
                                        _gsp9 = (_dhz9.get(_gh9) or {})
                                        st.markdown(f"<div style='background:#eff6ff;border-radius:6px;padding:.3rem .6rem;"
                                                    f"font-size:13px'>🧭 <b>当前档({_gt9}={_gh9})判断</b>: "
                                                    f"周期分<b>{int(_gsc9)}</b>·{_gsp9.get('rule_view', '')}"
                                                    f"·支撑{_gsp9.get('support', '?')}/压力{_gsp9.get('resistance', '?')}"
                                                    f"·置信{_gsp9.get('rule_confidence', '?')}"
                                                    "<span style='font-size:11px;color:#94a3b8'>　与作战板时间档联动(U3全局时间轴)</span></div>",
                                                    unsafe_allow_html=True)
                            except Exception:
                                pass
                            # 【V88·明白话判读】量价/K线/MACD 的事实与判断要点（不是分数）
                            _ro99 = _ce.plain_readout(_F, _turn99 if _turn99.get("side") else None)
                            if _ro99:
                                st.markdown("##### 📖 量价判读（事实+要点·佐证上方结论）")
                                st.markdown("\n".join(f"- {ln}" for ln in _ro99))
                            # Closed technical details only read already stored facts.
                            # Supplemental providers require one explicit request per session/security.
                            from deep_optional_data import session_once as _deep_once, cached_profile as _deep_cached_profile
                            _extras_saved9 = _deep_once(st.session_state, target_c)
                            _extras_go9 = st.button('按需补充外部资料（公司、三表、新闻）',
                                key=f'deep_supplement_{target_c}', disabled=bool(_extras_saved9))
                            _deep_extras = _deep_once(st.session_state, target_c, requested=_extras_go9,
                                                     loader=lambda: _fetch_deep_supplement(target_c))
                            _prof9d = _deep_extras.get('profile') or _deep_cached_profile(target_c)
                            if _prof9d.get('profile'):
                                st.markdown('**📇 公司档案**  \n' + str(_prof9d['profile'])[:180])
                                st.caption(f"来源：{_prof9d.get('source') or '未注明'}；缓存采集："
                                           + (datetime.fromtimestamp(float(_prof9d['ts'])).strftime('%Y-%m-%d %H:%M')
                                              if _prof9d.get('ts') else '未注明'))
                            else:
                                st.caption('公司简介尚未覆盖；行业和原审核资料见页首。')
                            st.caption('股息率：当前已发布公司资料未覆盖。')
                            _ext9d = _deep_extras.get('extremes')
                            if _ext9d:
                                st.markdown(f"补充历史收盘区间：{_ext9d['hist_low']:.2f}～{_ext9d['hist_high']:.2f}；"
                                            f"52周：{_ext9d['w52_low']:.2f}～{_ext9d['w52_high']:.2f}（Yahoo，独立资料，不替换原合同）")
                            st.markdown("**基本面与技术面交叉结论**：" + _deep_synthesis.get('business_conclusion', '同包证据待核'))
                            for _ev9 in (_deep_extras.get('announcements') or [])[:3]:
                                st.markdown(f"{_ev9.get('icon', '')} {_ev9.get('date', '')}「{_ev9.get('title', '')}」——{_ev9.get('note', '')}")
                            if _deep_extras.get('announcements'):
                                st.caption('出处：东财公告库。公告是待核事件线索，不生成交易许可。')
                            if _deep_extras:
                                st.caption(f"本会话已补查：{_deep_extras.get('attempted_at')}；后续页面交互复用该次结果。")
                                if _deep_extras.get('errors'):
                                    st.caption('补充资料部分未完成：' + '；'.join(_deep_extras['errors']))
                            else:
                                st.caption('补充资料尚未请求；当前技术分项使用本票同一份已核验日线。')
                            st.caption('分期限技术情景见下方；实际研究进场、止盈和失效沿用页首同周期中央合同。')
                            with st.expander("📖 术语速查（每个数值高低代表什么，非专业版）"):
                                st.markdown(_ce.GLOSSARY_MD)
                            # 【V88·复制纪要】整段分析一键复制（与云端同格式）
                            _cp99 = (f"{stock_name} {target_c}\n中央审核与技术研究分开；以原合同和当前执行闸为准。\n"
                                     + json.dumps(_deep_cross or {'status':'核验暂不可用'}, ensure_ascii=False, indent=2))
                            if _cp99:
                                if COPY_UTILS_AVAILABLE:
                                    CopyUtils.create_copy_button(_cp99, button_text="📋 复制分析纪要",
                                                                 key=f"copy_trend_{target_c}")
                                else:
                                    with st.expander("📋 复制分析纪要", expanded=False):
                                        st.code(_cp99, language=None)
                            _bd99 = _F["breakdown"]
                            st.dataframe([{"维度": k, "实际情况": d, "得分": sc, "权重": f"{int(w*100)}%"}
                                          for k, (sc, w, d) in _bd99.items()],
                                         hide_index=True, width='stretch')
                except Exception as _e99:
                    # 【2026-08-03】这一处与龙虎榜那条链**不同**:完整评分挂了退回趋势脉搏简版,
                    # 是正当的降级兜底。但降级必须**说出来**——否则用户看到简版,
                    # 会以为这就是全部,不知道完整分析其实失败了。
                    logging.exception(f"[V88] 完整评分失败,降级趋势脉搏: {_e99}")
                    st.caption(f"⚠️ 完整评分失败，已降级简版：{type(_e99).__name__}: {str(_e99)[:100]}")
                    _tp = analyze_trend_pulse(df_temp, target_c)
                    if _tp:
                        st.caption(f"技术简版状态：{_tp['stage']}；完整分项未完成，不能代替量价分或中央审核。")
        else:
            _scan_prog.progress(1.0)
            _scan_status.text("❌ 指标计算失败")
            time.sleep(0.3)
            _scan_prog.empty()
            _scan_status.empty()
            # 【V87.4】增强错误提示 - 特别处理已退市股票
            st.error("❌ 无法获取扫描分析数据")
            
            # 检查是否是已知的退市股票
            delisted_stocks = {
                "ATVI": "动视暴雪 - 已被微软收购退市",
                # 可以继续添加其他已知退市股票
            }
            
            stock_code = q_input.upper().strip()
            if stock_code in delisted_stocks:
                st.warning(f"🚨 **{delisted_stocks[stock_code]}**")
                st.info("💡 **建议尝试其他股票：**")
                
                # 根据市场推荐替代股票
                if stock_code.startswith("0") and len(stock_code) == 5:  # 港股
                    suggestions = [
                        ("00700", "腾讯控股", "科技巨头"),
                        ("09988", "阿里巴巴", "电商平台"), 
                        ("03690", "美团", "生活服务"),
                        ("01810", "小米集团", "智能硬件"),
                        ("06618", "京东健康", "医疗健康")
                    ]
                    st.markdown("**🇭🇰 推荐港股：**")
                elif stock_code.isalpha():  # 美股
                    suggestions = [
                        ("AAPL", "苹果", "科技巨头"),
                        ("MSFT", "微软", "软件服务"),
                        ("GOOGL", "谷歌", "互联网"),
                        ("TSLA", "特斯拉", "电动汽车"),
                        ("NVDA", "英伟达", "AI芯片")
                    ]
                    st.markdown("**🇺🇸 推荐美股：**")
                else:  # A股
                    suggestions = [
                        ("600519", "贵州茅台", "白酒龙头"),
                        ("000858", "五粮液", "白酒"),
                        ("300750", "宁德时代", "新能源电池"),
                        ("002594", "比亚迪", "新能源汽车"),
                        ("600036", "招商银行", "银行")
                    ]
                    st.markdown("**🇨🇳 推荐A股：**")
                
                # 显示推荐股票
                for code, name, desc in suggestions:
                    st.markdown(f"- **{code}** ({name}) - {desc}")
                    
            else:
                # 通用错误提示
                st.info("🔍 **可能的原因：**")
                st.markdown("""
                1. **股票代码错误** - 请检查代码格式
                2. **股票已退市** - 该股票可能已从交易所退市
                3. **网络连接问题** - 请检查网络和代理设置
                4. **数据源暂时不可用** - 请稍后重试
                """)
                
                st.info("💡 **建议操作：**")
                st.markdown("""
                1. 使用上方**全量云端搜索**功能查找正确的股票代码
                2. 尝试搜索其他活跃交易的股票
                3. 点击**系统自检**检查网络连接状态
                4. 使用**股票池健康检查**验证数据源状态
                """)
    else:
        _scan_prog.progress(1.0)
        _scan_status.text("❌ 数据获取失败")
        time.sleep(0.3)
        _scan_prog.empty()
        _scan_status.empty()
        st.error("❌ 无法获取股票数据")

    st.markdown("---")

# 【V92】个股搜索统一使用主区域「深度作战室」顶部的全量云端搜索

# 开始执行分析
_safe_print(f"[深度作战室] 准备执行分析: execute_analysis={execute_analysis}, q_input={q_input}")

if execute_analysis and q_input:
    code = q_input.upper().strip()
    target_c = to_yf_cn_code(code)
    
    _safe_print(f"[深度作战室] 🎯 开始分析: {code} -> {target_c}")
    
    st.subheader(f"🎯 {target_c}")
    
    # 预览与详情复用同一份完整日线；缓存身份包含全部OHLCV和来源。
    df, data_quality = df_temp, _deep_quality
    _cache_key = f"_warroom_local_refs_v1_{target_c}"
    _deep_sig = _deep_signature(df)
    _benchmark_sig = _deep_signature(_deep_benchmark(get_benchmark_code(target_c)))
    _computed_sig = f'{_deep_sig}:{_benchmark_sig}'
    _cached = st.session_state.get(f"{_cache_key}_signature") == _computed_sig
    st.session_state[f"{_cache_key}_signature"] = _computed_sig

    # 【V83 P0.1】显示数据质量标签
    if df is not None and data_quality:
        col_src1, col_src2, col_src3 = st.columns([2, 2, 1])
        with col_src1:
            delay_icon = "🟡" if data_quality.get('is_delayed', False) else "🟢"
            st.caption(f"{delay_icon} **数据来源**: {data_quality.get('source', '未知')}")
        with col_src2:
            st.caption(f"📅 **数据范围**: {data_quality.get('date_range', 'N/A')}")
        with col_src3:
            st.caption(f"📊 **数据点**: {data_quality.get('data_points', 0)}")
        st.caption(f"完整日线截至 {data_quality.get('source_asof', '未知')} · 价格口径：{data_quality.get('price_basis', '未声明')}；不拼接盘中报价。")
    
    # 【V87.15修复】数据获取失败的处理
    if df is None:
        _safe_print(f"[深度作战室] ❌ 数据获取失败: {target_c}")
        st.error("❌ 无法获取股票数据")
        
        # 详细错误提示
        st.info("🔍 **可能的原因：**")
        
        # 根据股票代码类型给出针对性建议
        if code.startswith('6') or code.startswith('0') or code.startswith('3') or code.startswith('5'):
            # A股
            st.markdown("""
            **A股数据获取失败：**
            1. 检查代码格式（如：600519 贵州茅台）
            2. 确认股票未停牌或退市
            3. 尝试使用东方财富数据源
            4. 检查网络连接状态
            """)
        elif len(code) == 5 or (len(code) >= 4 and code[0] == '0'):
            # 港股
            st.markdown("""
            **港股数据获取失败：**
            1. 检查代码格式（如：00700 腾讯控股）
            2. 确认使用5位数代码（如：00700，不是700）
            3. 检查代理设置（港股需要代理）
            4. 确认股票未退市
            """)
        else:
            # 美股
            st.markdown("""
            **美股数据获取失败：**
            1. 检查代码格式（如：AAPL 苹果）
            2. 确认股票代码正确（全大写）
            3. 检查代理设置
            4. 确认股票未退市或被收购
            """)
        
        # 推荐测试股票
        st.info("💡 **推荐测试股票：**")
        col_test1, col_test2, col_test3 = st.columns(3)
        with col_test1:
            if st.button("🇺🇸 测试 AAPL", key="test_aapl_error", width='stretch'):
                st.session_state.scan_selected_code = "AAPL"
                st.session_state.scan_selected_name = "苹果"
                st.rerun()
        with col_test2:
            if st.button("🇭🇰 测试 00700", key="test_hk_error", width='stretch'):
                st.session_state.scan_selected_code = "00700"
                st.session_state.scan_selected_name = "腾讯控股"
                st.rerun()
        with col_test3:
            if st.button("🇨🇳 测试 600519", key="test_cn_error", width='stretch'):
                st.session_state.scan_selected_code = "600519"
                st.session_state.scan_selected_name = "贵州茅台"
                st.rerun()
        
        # 不要继续执行后续代码
        st.stop()
    
    if df is not None:
        _computed_key = f"_warroom_computed_local_refs_v1_{target_c}"
        news_headlines = _deep_extras.get('news') or []
        if _cached and _computed_key in st.session_state:
            metrics = st.session_state[_computed_key].get("metrics")
            quant = st.session_state[_computed_key].get("quant")
            mc = st.session_state[_computed_key].get("mc")
            risk_metrics = st.session_state[_computed_key].get("risk_metrics")
            _safe_print(f"[深度作战室] 使用缓存指标")
        else:
            try:
                _safe_print(f"[深度作战室] 📊 开始计算指标...")
                metrics = m or calculate_metrics_all(df, target_c, benchmark_loader=_deep_benchmark)
                if not metrics:
                    st.warning("完整行情已读取，但技术指标不足；上方中央合同和审核证据仍可查看。")
                    st.stop()
                quant = calculate_advanced_quant(df)
                mc = monte_carlo_forecast(df)
                risk_metrics = calculate_risk_metrics(df, target_c, benchmark_loader=_deep_benchmark)
                if df is not None:
                    st.session_state[_computed_key] = {
                        "metrics": metrics, "quant": quant, "mc": mc,
                        "risk_metrics": risk_metrics, "news_headlines": news_headlines,
                    }
            except Exception as e:
                _safe_print(f"[深度作战室] ❌ 指标计算异常: {type(e).__name__}: {str(e)}")
                import traceback
                traceback.print_exc()
                st.error(f"❌ 指标计算失败: {type(e).__name__}")
                st.info(f"错误详情: {str(e)}")
                st.stop()

        # ═══════════════════════════════════════════════════════════════
        # 【V88·个股当下前瞻】用最新价，主动给这只票未来 5/10/20/60/120 交易日的
        # 规则方向分（非概率） + 盈亏比 + 一句拿/加/减/回避。用户最需要的"个股走概率"入口。
        # 放在最前、纯确定性计算，不等 AI，秒出——解决"系统不及时给信号"。
        # ═══════════════════════════════════════════════════════════════
        try:
            from v88_decision_core import evaluate_forward_outlook as _evaluate_forward_outlook
            # 【V88·入场时机确认】传入趋势引擎价位（买入区/回踩/突破/止损），
            # 让前瞻给出"现在可进/双路径"的交易日窗口，不再只会说等更低价
            _fwd_full9 = _deep_trend
            _fwd = _evaluate_forward_outlook(
                df, name=(st.session_state.get("scan_selected_name") or target_c),
                code=target_c, full=_fwd_full9)
            if _fwd.get("error"):
                st.info(f"个股前瞻暂不可用：{_fwd['error']}")
            else:
                st.markdown("### 🎯 量价情景研究 · 5 / 10 / 20 / 60 / 120 交易日")
                st.caption(
                    "使用同一完整日线计算各期限的技术情景，不能替代上方同周期GPT与书理审核。"
                    "规则情景值不是经标定概率或回测胜率；技术价位不替换原合同。"
                )
                _fm1, _fm2, _fm3, _fm4 = st.columns(4)
                _fm1.metric("规则方向分", f"{_fwd['weighted_p_up']} / 100",
                            help="5档规则加权，未经概率标定，不是上涨概率")
                _fm2.metric("综合盈亏比", f"{_fwd.get('weighted_rr', 0):.2f}",
                            help="上涨空间÷下跌空间，越大越好，≥2优秀")
                _fm3.metric("规则情景值", f"{_fwd.get('weighted_expected_pct', 0):+.1f}%",
                            help="规则分加权的技术空间，不是统计期望收益")
                _fm4.metric("阶段", _fwd.get("stage", "—"),
                            help="多头/震荡/转弱，由最新价与MA20/MA60关系判定")
                st.caption('执行与仓位只查中央原合同；下表保留各周期技术反证，不另生成买单或移动止损。')
                _fwd_rows = []
                for _fr in _fwd.get("horizons") or []:
                    _fwd_rows.append({
                        "周期(交易日)": _fr.get("label"),
                        "方向参考分/100": _fr.get('p_up'),
                        "上涨空间": f"+{_fr.get('upside_pct')}%",
                        "下跌风险": f"-{_fr.get('downside_pct')}%",
                        "技术上界/下界": f"{_fr.get('target_price')} / {_fr.get('risk_price')}",
                        "盈亏比(越大越好)": _fr.get("rr"),
                        "规则加权空间": f"{_fr.get('expected_pct'):+.1f}%",
                        "判断": _fr.get("view"),
                    })
                st.dataframe(_fwd_rows, hide_index=True, use_container_width=True)

                # 【V88·每档判断理由讲人话】基本面+个股新闻+技术面融合成一句中文，不出现术语。
                # 预算自适应：默认规则版，手动点按钮切思考模式，预算到底自动关（render_readable_reasons）。
                from deep_prompt_context import build as _deep_prompt_build
                _fwd_ctx = json.dumps(_deep_prompt_build(
                    joint_conclusion=_deep_synthesis, annual_outlook=_deep_annual,
                    cross_validation=_deep_cross,
                    central={'tier': _deep_synthesis.get('central_grade'), 'executable': False,
                             'trade_plan': _deep_synthesis.get('original_plan')}),
                    ensure_ascii=False, separators=(',', ':'))
                render_readable_reasons(
                    _fwd, kind="个股", symbol=target_c,
                    name=st.session_state.get("scan_selected_name") or target_c,
                    context=_fwd_ctx, key_prefix="stock")

                st.caption(
                    f"🔒 口径{_fwd.get('score_version')}｜数据签名{_fwd.get('data_signature')}｜"
                    f"行情截至{_fwd.get('data_asof') or '未知'}｜生成于{_fwd.get('analysis_time')}"
                )
        except Exception as _fwd_exc:
            logging.exception("个股当下前瞻失败")
            st.warning(f"个股前瞻暂不可用：{type(_fwd_exc).__name__}")

        # ═══════════════════════════════════════════════════════════════
        # 【V88·个股五周期】2/4/8/16/32周量化底稿 + GPT-6 Astra high复核
        # 点击任一个股均自动执行；同一行情快照缓存6小时，节省会员共享额度。
        # ═══════════════════════════════════════════════════════════════
        with st.expander("历史量价计算明细 · 按需查看", expanded=False):
            st.markdown("### 🧭 历史量价窗口 · 2 / 4 / 8 / 16 / 32周")
            st.caption("各档回看截至同一行情日的历史结构；后续方向统一参照年度条件主线。")
            _hz_align = {}
            _hz_decision = {}
            try:
                import stock_horizon as _stock_horizon
                # 与首页自选卡使用同一套趋势阶段，避免“启动确认”在首页加权、
                # 深度分析却漏传阶段而产生同股同周期分差。
                _hz_full = _deep_trend
                # 不能只按交易日期缓存：盘中价格/成交量已变化时，继续复用旧底稿会让
                # 首页概率与深度分析互相打架。用末价+末量组成同源行情签名。
                _hz_last_px = float(pd.to_numeric(df["Close"], errors="coerce").dropna().iloc[-1])
                _hz_last_vol = (float(pd.to_numeric(df["Volume"], errors="coerce").dropna().iloc[-1])
                                if "Volume" in df and not pd.to_numeric(df["Volume"], errors="coerce").dropna().empty else 0.0)
                _hz_stage = str((_hz_full or {}).get("stage") or "阶段待核")
                _hz_last = f"{str(df.index[-1])[:19]}_{_hz_last_px:.4f}_{_hz_last_vol:.0f}_{len(df)}_{_hz_stage}"
                _hz_cache_key = f"_stock_horizon_cross_v2_historical_{target_c}_{_deep_sig}_{_hz_last}"
                if _hz_cache_key not in st.session_state:
                    _hz_bar = st.progress(0, text="正在计算五周期量价底稿…")
                    _hz_bar.progress(35, text="计算五周期辅助研究；复用上方中央双审…")
                    _hz_news_parts = []
                    for _hz_news in (news_headlines or [])[:8]:
                        if isinstance(_hz_news, dict):
                            _hz_news_parts.append(str(_hz_news.get("title") or _hz_news.get("headline") or ""))
                        else:
                            _hz_news_parts.append(str(_hz_news))
                    _hz_context = ("本区只输出量价研究；评级和执行合同由中央GPT双审与书理决定；"
                                   f"近期新闻:{'；'.join(x for x in _hz_news_parts if x)[:700]}")
                    _hz_result = _stock_horizon.analyze(
                        st.session_state.get("scan_selected_name") or target_c,
                        target_c,
                        df,
                        full=_hz_full,
                        context=_hz_context,
                        allow_ai=False,
                    )
                    st.session_state[_hz_cache_key] = _hz_result
                    _hz_bar.progress(100, text="五周期走势分析完成")
                    _hz_bar.empty()
                else:
                    _hz_result = st.session_state[_hz_cache_key]

                _hz_review = _hz_result.get("review") or {}
                _hz_align = _stock_horizon.cycle_alignment(_hz_result.get("facts") or {})
                # Reuse exactly the same complete-series computation as the upper list.
                _hz_decision = dict(_search_dc)
                _hz_action = _hz_decision.get("action", "观察")
                from period_consistency import build as _period_build
                _hz_period = _period_build(target_c, df, _deep_quality, _hz_result,
                                          _hz_full, _deep_annual, _deep_synthesis)
                _hz_result = dict(_hz_result, decision=_hz_decision, period_consistency=_hz_period)
                _ud1, _ud2, _ud3, _ud4, _ud5 = st.columns(5)
                _ud1.metric("量价辅助分", _hz_decision.get("unified_score", "—"),
                            help="与上方量价表复用同一计算；短20%＋中25%＋长20%＋趋势15%＋赔率20%，不是中央审核分")
                _ud2.metric("短/中/长", f"{_hz_decision.get('short_score','—')}/"
                            f"{_hz_decision.get('medium_score','—')}/{_hz_decision.get('long_score','—')}")
                _ud3.metric("短窗方向分", f"{_hz_decision.get('p_up','—')} / 100",
                            help="历史量价规则分，非未来2周上涨概率")
                _ud4.metric("盈亏比", f"{_hz_decision.get('rr',0):.2f}",
                            help="潜在收益÷潜在风险，越大越好")
                _ud5.metric("规则加权空间", f"{_hz_decision.get('expected_pct',0):+.1f}%",
                            help="用未标定规则分加权的技术空间，不是统计期望收益")
                st.info(f"**量价状态：{_hz_decision.get('cycle_status','待核')}**｜执行仍查中央原合同｜"
                        f"口径{_hz_decision.get('score_version')}｜数据签名"
                        f"{_hz_decision.get('data_signature')}｜分析{_hz_decision.get('analysis_time')}")
                _hz_rows = _stock_horizon.table_rows(_hz_result)
                _hz_visual = _stock_horizon.historical_visual_html(
                    _hz_result,
                    st.session_state.get("scan_selected_name") or target_c,
                    target_c,
                    f"v88-stock-cycle-{target_c}",
                )
                if _hz_visual:
                    st.markdown(_hz_visual, unsafe_allow_html=True)
                if _hz_rows:
                    st.dataframe(_hz_rows, hide_index=True, use_container_width=True)
                if _hz_review.get("status") in ("completed", "cached"):
                    st.info(
                        f"🧠 **思考复核**：{_hz_review.get('summary', '五周期复核完成')} ｜ "
                        f"周期相位：{_hz_review.get('cycle_phase', '震荡')} ｜ "
                        f"周期口径：{_hz_align.get('note', '待核')} ｜ "
                        f"综合动作：{_hz_action} ｜ "
                        f"失效条件：{_hz_review.get('invalid_summary', '破位后重评')}"
                    )
                    st.caption(
                        f"模型：{_hz_review.get('model', 'gpt-6-astra')} · reasoning-high ｜ "
                        f"分析于 {_hz_review.get('analysis_time', '缓存时间待核')}"
                    )
                elif _hz_review.get("status") == "deterministic":
                    st.caption("本区为量价辅助研究；GPT双审与书理逐项证据见页首，打开页面未新增模型调用。")
                else:
                    st.warning(
                        f"GPT-6 Astra思考复核未完成（{_hz_review.get('reason', _hz_review.get('status', '未知'))}）；"
                        "当前仅展示量化底稿，不冒充AI结论。"
                    )
            except Exception as _hz_exc:
                logging.exception("个股五周期分析失败")
                st.warning(f"五周期走势暂不可用：{type(_hz_exc).__name__}")

        # ═══════════════════════════════════════════════════════════════
        # 【V88·参数图例】用户不熟的指标一次讲清：值大好还是小好。纯说明，不改数据。
        # ═══════════════════════════════════════════════════════════════
        with st.expander("📖 参数怎么看（括号里=大了好还是小了好）", expanded=False):
            st.markdown(
                "- **中央审核分**：GPT主审、反审与适用书理的证据分，评级只以中央当前结果为准。\n"
                "- **量价辅助分**（0–100）：描述规则下的趋势与赔率；不是审核分，不与审核分平均，也不是胜率。\n"
                "- **方向分 p_up**：历史量价规则分，不是未来上涨概率，不证明越高越容易盈利。\n"
                "- **盈亏比 RR**（越大越好）：上涨空间÷下跌空间。≥2 优秀，1.5–2 可关注，<1 冒险不划算。\n"
                "- **情景期望 EV**：规则情景加权空间；正值不证明策略盈利，须另看扣费结算样本。\n"
                "- **情景差值 edge**：规则估计 − 赔率要求的保本概率；不是已验证的真实概率优势。\n"
                "- **RSI**（**不是越大越好**）：50 中性；>70 超买（偏贵、易回调），<30 超卖（偏便宜、易反弹）。\n"
                "- **ATR / 波动率**：描述波动宽度；先定逻辑失效位再算仓位，不能为凑仓位改止损。\n"
                "- **历史价格位置**：仅描述所用行情区间的相对位置；低位不等于安全，也不等于估值便宜。\n"
                "- **52周区间位置**：0–100%，0接近区间低点、100接近高点；不足一年须按实际样本解释。\n"
                "- **量比**（>1 为放量）：越大资金越活跃；放量上涨=承接强，放量下跌=出逃要警惕。\n"
                "- **换手率**（适中为宜）：越高越活跃/分歧大；异常放大要警惕见顶。\n"
                "- **乖离率 / 距均线**（正=强，过大要防回踩）：价在均线上方为强势，偏离过大易回调。\n"
                "- **目标价 / 风险价**：目标价=上行看到的位置；**跌破风险价就该重新评估**，不是自动卖点。\n"
                "- **市盈率 PE / 市净率 PB**（一般越低越便宜）：需结合成长性，并非绝对越小越好。"
            )

        # ═══════════════════════════════════════════════════════════════
        # 【V88·个人决策锚点】按“当时的时间+价格”重建 5/10/20/60/120 交易日判断。
        # 旧预测单独留档；锚点后的行情只用于到期复盘，绝不反向改写原结论。
        # ═══════════════════════════════════════════════════════════════
        st.markdown("### 🧷 我的决策锚点(事后复盘) · 5 / 10 / 20 / 60 / 120 交易日")
        st.caption(
            "填你当时分析/买卖的**时间和价格**，系统只读取该时点以前的行情，"
            "计算当时各档规则方向分与技术空间，再用后续行情复盘。"
            "规则方向分未经概率标定，后续行情仅用于复盘。"
        )
        try:
            from v88_decision_core import evaluate_anchor_outlook as _evaluate_anchor_outlook

            _anchor_name = st.session_state.get("scan_selected_name") or target_c
            _anchor_store = core_root() / "journal" / "decision_anchors.json"

            def _anchor_code_key(_value):
                _raw = str(_value or "").strip().upper().replace(" ", "")
                _raw = _raw.replace(".SS", ".SH")
                if _raw.endswith(".HK"):
                    try:
                        return f"HK:{int(_raw[:-3])}"
                    except ValueError:
                        return _raw
                _base = _raw.split(".")[0]
                if _base.isdigit() and len(_base) == 6:
                    return f"CN:{_base}"
                return _raw

            def _read_anchor_records():
                try:
                    _payload = json.loads(_anchor_store.read_text(encoding="utf-8"))
                    return _payload if isinstance(_payload, list) else list(_payload.get("records") or [])
                except Exception:
                    return []

            def _save_anchor_record(_record):
                _records = _read_anchor_records()
                _record_key = (f"{_anchor_code_key(_record.get('code'))}|{_record.get('anchor_time')}|"
                               f"{_record.get('anchor_price')}|{_record.get('anchor_action')}")
                _records = [r for r in _records if
                            f"{_anchor_code_key(r.get('code'))}|{r.get('anchor_time')}|"
                            f"{r.get('anchor_price')}|{r.get('anchor_action')}" != _record_key]
                _records.append(_record)
                _anchor_store.parent.mkdir(parents=True, exist_ok=True)
                _tmp = _anchor_store.with_suffix(".tmp")
                _tmp.write_text(json.dumps(_records[-300:], ensure_ascii=False, indent=2), encoding="utf-8")
                _tmp.replace(_anchor_store)

            # 能匹配成交日志时一键带入；腾讯等未记账的历史卖出仍可手动输入。
            _trade_choices = [{"label": "手动输入", "trade": None}]
            _trade_path = core_root() / "journal" / "trades.json"
            try:
                _all_trades = json.loads(_trade_path.read_text(encoding="utf-8"))
                for _trade in reversed(_all_trades if isinstance(_all_trades, list) else []):
                    if _anchor_code_key(_trade.get("code")) != _anchor_code_key(target_c):
                        continue
                    _trade_price = _trade.get("sell_price") or _trade.get("cost")
                    if not _trade_price:
                        continue
                    _trade_choices.append({
                        "label": (f"{str(_trade.get('date',''))[:16]}  {_trade.get('action','操作')} "
                                  f"@{_trade_price}"),
                        "trade": _trade,
                    })
            except Exception:
                pass

            _anchor_pick = st.selectbox(
                "从历史成交带入（没有记录就手动输入）",
                options=list(range(len(_trade_choices))),
                format_func=lambda i: _trade_choices[i]["label"],
                key=f"anchor_trade_pick_{target_c}",
            )
            _picked_trade = _trade_choices[_anchor_pick]["trade"]
            _pick_state_key = f"anchor_trade_last_{target_c}"
            if _picked_trade and st.session_state.get(_pick_state_key) != _anchor_pick:
                try:
                    _picked_ts = pd.Timestamp(_picked_trade.get("date"))
                    st.session_state[f"anchor_date_{target_c}"] = _picked_ts.date()
                    st.session_state[f"anchor_time_{target_c}"] = (
                        _picked_ts.time() if (_picked_ts.hour or _picked_ts.minute)
                        else datetime.strptime("09:45", "%H:%M").time())
                    st.session_state[f"anchor_price_{target_c}"] = float(
                        _picked_trade.get("sell_price") or _picked_trade.get("cost"))
                    _picked_action = str(_picked_trade.get("action") or "观察")
                    if "清" in _picked_action:
                        _picked_action = "清仓"
                    elif "卖" in _picked_action:
                        _picked_action = "卖出"
                    elif "减" in _picked_action:
                        _picked_action = "减仓"
                    elif "加" in _picked_action:
                        _picked_action = "加仓"
                    elif "买" in _picked_action:
                        _picked_action = "买入"
                    else:
                        _picked_action = "观察"
                    st.session_state[f"anchor_action_{target_c}"] = _picked_action
                except Exception:
                    pass
            st.session_state[_pick_state_key] = _anchor_pick

            _last_trade_date = pd.Timestamp(df.index[-1]).date()
            _last_trade_price = float(pd.to_numeric(df["Close"], errors="coerce").dropna().iloc[-1])
            _ac1, _ac2, _ac3, _ac4 = st.columns([1.2, 1, 1.1, 1.1])
            with _ac1:
                _anchor_date = st.date_input(
                    "分析/操作日期", value=_last_trade_date,
                    min_value=pd.Timestamp(df.index[12]).date(), max_value=datetime.now().date(),
                    key=f"anchor_date_{target_c}",
                )
            with _ac2:
                _anchor_clock = st.time_input(
                    "当时时间", value=datetime.strptime("09:45", "%H:%M").time(),
                    key=f"anchor_time_{target_c}",
                )
            with _ac3:
                _anchor_price = st.number_input(
                    "当时价格", min_value=0.0001, value=_last_trade_price,
                    format="%.4f", key=f"anchor_price_{target_c}",
                    help="使用你的实际成交价或当时分析价",
                )
            with _ac4:
                _anchor_action = st.selectbox(
                    "当时动作", ["观察", "买入", "加仓", "减仓", "卖出", "清仓"],
                    key=f"anchor_action_{target_c}",
                )

            _anchor_run = st.button(
                "🧠 按当时视角推算并保存",
                key=f"anchor_run_{target_c}", type="primary", width="stretch",
                help="生成可留档的 5/10/20/60/120 交易日概率、盈亏比、期望值和失效条件",
            )
            _anchor_result_key = f"anchor_result_{target_c}"
            if _anchor_run:
                _anchor_dt = datetime.combine(_anchor_date, _anchor_clock)
                _anchor_bar = st.progress(15, text="正在截断锚点后的行情…")
                _anchor_bar.progress(55, text="正在计算各档规则方向分与技术空间…")
                _anchor_result = _evaluate_anchor_outlook(
                    df, _anchor_dt, _anchor_price, action=_anchor_action,
                    name=_anchor_name, code=target_c,
                    analysis_time=datetime.now().strftime("%Y-%m-%d %H:%M"),
                )
                _anchor_bar.progress(90, text="正在固化预测版本，供以后复盘…")
                if not _anchor_result.get("error"):
                    _save_anchor_record(_anchor_result)
                    st.session_state[_anchor_result_key] = _anchor_result
                    _anchor_bar.progress(100, text="决策锚点已保存")
                    st.success("已保存：未来行情不会改写这份当时结论。")
                else:
                    st.error(_anchor_result.get("error"))
                _anchor_bar.empty()

            _saved_for_stock = [r for r in _read_anchor_records()
                                if _anchor_code_key(r.get("code")) == _anchor_code_key(target_c)]
            if _saved_for_stock:
                _saved_for_stock.sort(key=lambda r: str(r.get("anchor_time") or ""), reverse=True)
                _saved_pick = st.selectbox(
                    "已保存的决策锚点",
                    options=list(range(len(_saved_for_stock))),
                    format_func=lambda i: (f"{_saved_for_stock[i].get('anchor_time')} "
                                           f"{_saved_for_stock[i].get('anchor_action')} "
                                           f"@{_saved_for_stock[i].get('anchor_price')}"),
                    key=f"anchor_saved_pick_{target_c}",
                )
                if not _anchor_run:
                    st.session_state[_anchor_result_key] = _saved_for_stock[_saved_pick]

            _anchor_result = st.session_state.get(_anchor_result_key) or {}
            if _anchor_result and not _anchor_result.get("error"):
                _am1, _am2, _am3, _am4 = st.columns(4)
                _am1.metric("规则方向分", f"{_anchor_result.get('weighted_p_up')} / 100",
                            help="规则情景估计，不是回测胜率")
                _am2.metric("综合盈亏比", f"{_anchor_result.get('weighted_rr', 0):.2f}",
                            help="估计上涨空间÷估计下跌空间，越大越好")
                _am3.metric("规则加权空间", f"{_anchor_result.get('weighted_expected_pct', 0):+.1f}%",
                            help="用规则分加权的技术空间，不能证明正期望收益")
                _anchor_track = _anchor_result.get('tracking') or {}
                _anchor_since = _anchor_track.get('since_anchor_pct')
                _am4.metric("锚点后价格变化", (f"{_anchor_since:+.1f}%" if _anchor_since is not None else "待最新行情"),
                            help="仅用于复盘，不参与当时预测")
                st.info(
                    f"**当时结论：{_anchor_result.get('overall_action')}**｜"
                    f"动作复盘：{_anchor_result.get('decision_review')}｜"
                    f"锚点{_anchor_result.get('anchor_time')} @ {_anchor_result.get('anchor_price')}"
                )
                _anchor_rows = []
                for _row in _anchor_result.get("horizons") or []:
                    _anchor_rows.append({
                        "周期(交易日)": _row.get("label"),
                        "规则方向分/100": _row.get("p_up"),
                        "上涨空间": f"+{_row.get('upside_pct')}%",
                        "下跌风险": f"-{_row.get('downside_pct')}%",
                        "目标/风险价": f"{_row.get('target_price')} / {_row.get('risk_price')}",
                        "盈亏比(越大越好)": _row.get("rr"),
                        "规则加权空间（非收益期望）": f"{_row.get('expected_pct'):+.1f}%",
                        "判断": _row.get("view"),
                        "触发": _row.get("trigger"),
                        "失效": _row.get("invalid"),
                    })
                st.dataframe(_anchor_rows, hide_index=True, use_container_width=True)
                _track_rows = (_anchor_result.get("tracking") or {}).get("rows") or []
                st.caption(
                    f"🔒 原锚点时间截断｜口径{_anchor_result.get('score_version')}｜"
                    f"预测签名{_anchor_result.get('data_signature')}｜生成于{_anchor_result.get('analysis_time')}｜"
                    f"行情截至{_anchor_track.get('market_asof') or '未知'}｜"
                    + "；".join(f"{r.get('days', r.get('weeks'))}日{r.get('status')}" for r in _track_rows)
                )
        except Exception as _anchor_exc:
            logging.exception("个人决策锚点失败")
            st.warning(f"个人决策锚点暂不可用：{type(_anchor_exc).__name__}")
        
        # ═══════════════════════════════════════════════════════════════
        # 【V90 升级】K线图 + 机构作战层（VWAP + Chandelier Exit）
        # ═══════════════════════════════════════════════════════════════
        
        # 预先计算 VWAP 和 Chandelier Exit（K线图和后续分析共用）
        _chart_predictor = None
        _chart_vwap = None
        _chart_ce = None
        if HAS_PREDICTION_ENGINE:
            try:
                _chart_predictor = InstitutionalPredictor(df, target_c)
                _chart_vwap = _chart_predictor.calculate_vwap(window=20)
                _chart_ce = _chart_predictor.calculate_chandelier_exit()
            except Exception as _ce_err:
                logging.warning(f"K线叠加层计算失败: {_ce_err}")
        
        # K线蜡烛图（基础层）
        fig = go.Figure(data=[go.Candlestick(
            x=df.index,
            open=df['Open'],
            high=df['High'],
            low=df['Low'],
            close=df['Close'],
            name='K线'
        )])
        
        # 叠加 VWAP 金线
        if _chart_vwap is not None and not _chart_vwap.empty:
            fig.add_trace(go.Scatter(
                x=df.index,
                y=_chart_vwap,
                mode='lines',
                name='20日量权均价（HLC3近似）',
                line=dict(color='#FFD700', width=2.5, dash='solid'),
                hovertemplate='VWAP: %{y:.2f}<extra></extra>'
            ))
        
        # 叠加 Chandelier Exit 通道
        if _chart_ce and _chart_ce.get('chandelier_long') is not None:
            _ce_long = _chart_ce['chandelier_long']
            _ce_short = _chart_ce['chandelier_short']
            
            # 多头止损线（绿色虚线）
            fig.add_trace(go.Scatter(
                x=df.index,
                y=_ce_long,
                mode='lines',
                name='多头波动参照',
                line=dict(color='#10b981', width=1.5, dash='dash'),
                hovertemplate='多头波动参照: %{y:.2f}<extra></extra>'
            ))
            
            # 空头止损线（红色虚线）
            fig.add_trace(go.Scatter(
                x=df.index,
                y=_ce_short,
                mode='lines',
                name='空头波动参照',
                line=dict(color='#ef4444', width=1.5, dash='dash'),
                hovertemplate='空头波动参照: %{y:.2f}<extra></extra>'
            ))
        
        # 添加可点击的收盘价散点层（用于选点交互）
        fig.add_trace(go.Scatter(
            x=df.index,
            y=df['Close'],
            mode='markers',
            name='收盘价（点击选点）',
            marker=dict(color='rgba(99,102,241,0.4)', size=6, symbol='circle'),
            hovertemplate='<b>%{x|%Y-%m-%d}</b><br>收盘价: %{y:.2f}<br><i>👆 点击此处选定入场点</i><extra></extra>',
            selected=dict(marker=dict(color='#ff6b00', size=14)),
            unselected=dict(marker=dict(opacity=0.3))
        ))
        
        fig.update_layout(
            title="历史K线与波动参照（点击圆点记录研究锚点）",
            xaxis_title="日期",
            yaxis_title="价格",
            height=600,
            template="plotly_white",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=0.5,
                font=dict(size=11)
            ),
            # 十字准星 - 光标移动时显示精确价格和日期
            hovermode='x unified',
            xaxis=dict(
                showspikes=True,
                spikecolor='#6366f1',
                spikethickness=1,
                spikedash='dot',
                spikemode='across',
                spikesnap='cursor'
            ),
            yaxis=dict(
                showspikes=True,
                spikecolor='#6366f1',
                spikethickness=1,
                spikedash='dot',
                spikemode='across',
                spikesnap='cursor'
            ),
            # 启用框选模式（用于选点）
            dragmode='select',
            clickmode='event+select'
        )
        
        # 使用 on_select 捕获用户点击
        _kline_event = st.plotly_chart(fig, width='stretch', on_select="rerun", key=f"kline_select_{target_c}")
        
        # K线图注释说明
        _chart_note_cols = st.columns(3)
        with _chart_note_cols[0]:
            st.markdown('<p style="font-size:12px;color:#b45309;font-weight:600;">━━ 20日量权均价</p>', unsafe_allow_html=True)
            st.caption('日线典型价HLC3按成交量加权，观察价格偏离；没有机构身份数据，不能推断机构成本或盈亏。')
        with _chart_note_cols[1]:
            st.markdown('<p style="font-size:12px;color:#15803d;font-weight:600;">┅┅ 多头波动参照</p>', unsafe_allow_html=True)
            st.caption('22日最高价－3×ATR。跌破只触发结构复核，不能替换原合同止损或自动授予卖出许可。')
        with _chart_note_cols[2]:
            st.markdown('<p style="font-size:12px;color:#b91c1c;font-weight:600;">┅┅ 空头波动参照</p>', unsafe_allow_html=True)
            st.caption('22日最低价＋3×ATR。突破仍须确认；两条线之间不代表安全，也不能确定趋势反转。')
        
        # Chandelier Exit 当前状态速览
        if _chart_ce and _chart_ce.get('ce_long_latest', 0) > 0:
            _ce_signal = _chart_ce.get('signal', '')
            _ce_long_val = _chart_ce.get('ce_long_latest', 0)
            _ce_short_val = _chart_ce.get('ce_short_latest', 0)
            _curr_price = float(df['Close'].iloc[-1])
            _ce_signal_color = "#ef4444" if "跌破" in _ce_signal else ("#10b981" if "突破" in _ce_signal else "#f59e0b")
            st.markdown(f'<div style="background: {_ce_signal_color}15; border-left: 4px solid {_ce_signal_color}; padding: 0.7rem 1rem; border-radius: 4px; margin: 0.5rem 0;"><span style="font-weight:600;">{_ce_signal}</span> &nbsp;|&nbsp; 当前价 <b>{_curr_price:.2f}</b> &nbsp;|&nbsp; 多头参照 <b style="color:#10b981">{_ce_long_val:.2f}</b> &nbsp;|&nbsp; 空头参照 <b style="color:#ef4444">{_ce_short_val:.2f}</b></div>', unsafe_allow_html=True)
        
        # ═══════════════════════════════════════════════════════════════
        # 【V93】财务数据 & 行业背景面板
        # ═══════════════════════════════════════════════════════════════
        st.markdown("---")
        st.markdown("### 📊 财务数据 & 行业背景")
        st.caption("供应商财务原始资料；报告期不等于公告日。核实程度与技术反证共同见上方统一研判，未进入同包双审的新增资料不改变当前评级。")
        _fundamentals_cache_key = f"_fundamentals_{target_c}"
        _fundamentals = (_deep_extras.get('fundamentals')
                         or st.session_state.get(_fundamentals_cache_key) or {})
        if not _fundamentals:
            st.caption('财务原始三表尚未请求；可在上方趋势分项内按需补充。同包财务核验与原评级见页首。')
        render_fundamentals_panel(_fundamentals, target_c)

        # ═══════════════════════════════════════════════════════════════
        # 【V93】AI 综合分析（整合: 技术面 + 止损止盈 + 风控 + 行业 + 日线复盘）
        # 文件缓存 + 自动加载 + 强制刷新
        # ═══════════════════════════════════════════════════════════════
        st.markdown("---")
        # 【V88·原地交互铁律 2026-07-18 用户点单】fragment=点击只重跑本块：
        # 页面不滚动、深度页其余部分不重画，生成结果原地出现。
        @st.fragment
        def _unified_ai_frag():
            st.markdown("### 🤖 AI 综合分析")

            # v2缓存强制淘汰未接入五周期裁决的旧报告，防止旧“推荐”继续与顶部结论冲突。
            from stock_verdict import _triad_record as _ai_cache_record
            _ai_cache_sel, _ai_cache_row, _ai_cache_allowed = _ai_cache_record(target_c)
            _ai_decision_sig = hashlib.sha256(json.dumps({
                "pack": _ai_cache_sel.get("factpack_id"), "row": _ai_cache_row,
                "executable": _ai_cache_allowed, "joint_conclusion": _deep_synthesis.get('input_id')}, sort_keys=True, default=str).encode()).hexdigest()[:16]
            _stock_ai_report_key = f"stock_consensus_cross_v1_{target_c}_{_deep_sig[:16]}_{_ai_decision_sig}"
            _unified_ai_cache_key = f"_unified_ai_consensus_cross_v1_{target_c}_{_deep_sig[:16]}_{_ai_decision_sig}"

            # 从文件缓存恢复（session_state 没有时）
            if _unified_ai_cache_key not in st.session_state:
                _cached_report, _cached_ts = _load_ai_report_cache(_stock_ai_report_key)
                if _cached_report and isinstance(_cached_report, str):
                    st.session_state[_unified_ai_cache_key] = _cached_report

            _has_stock_cache = _unified_ai_cache_key in st.session_state
            if _has_stock_cache:
                _, _sc_ts = _load_ai_report_cache(_stock_ai_report_key)
                _sc_time = datetime.fromtimestamp(_sc_ts).strftime('%H:%M') if _sc_ts else ""
                st.caption(f"技术研判 · 止损止盈 · 风控评估 · 行业分析 · 操作建议{f' · 缓存 {_sc_time}' if _sc_time else ''}")
            else:
                st.caption("一键生成: 技术研判 · 止损止盈 · 风控评估 · 行业分析 · 操作建议")

            _btn_c1, _btn_c2 = st.columns([3, 1])
            with _btn_c1:
                _run_unified_ai = st.button("⚡ 一键 AI 综合分析" if not _has_stock_cache else "⚡ 重新生成 AI 综合分析",
                                            key=f"btn_unified_ai_{target_c}", type="primary", use_container_width=True)
            with _btn_c2:
                _refresh_stock_ai = st.button("🔄 刷新", key=f"btn_refresh_stock_ai_{target_c}", use_container_width=True)

            if _refresh_stock_ai:
                st.session_state.pop(_unified_ai_cache_key, None)
                try:
                    _rf = _AI_REPORT_CACHE_DIR / f"ai_report_{_stock_ai_report_key}.json"
                    if _rf.exists():
                        _rf.unlink()
                except Exception:
                    pass
                _run_unified_ai = True
                _has_stock_cache = False

            # 点击个股只读已有中央审核；额外文字分析由按钮明确触发。

            if _run_unified_ai and MY_GEMINI_KEY:
                with _v88_running(f"🤖 {_ai_model_label()} 综合分析中 · 预计 15-30 秒..."):
                    try:
                        _curr_p = float(df['Close'].iloc[-1])
                        _rsi_v = metrics.get('rsi', 50)
                        _score_v = metrics.get('score', 0)  # 旧研究质量分，仅用于下方因子归因
                        _sharpe_v = quant.get('sharpe', 'N/A')
                        _maxdd_v = quant.get('max_dd', 'N/A')
                        _pattern_v = metrics.get('pattern', '无')
                        _vwap_v = ""
                        if _chart_predictor:
                            _af = _chart_predictor.calculate_alpha_factors()
                            _rm = _chart_predictor.calculate_risk_engine()
                            _vwap_v = f"日线量权均值(20条): {_af.get('vwap_20')}, 偏离百分数: {_af.get('vwap_deviation')}, 信号: {_af.get('vwap_signal','待核')}"
                            _vwap_v += f"\nATR技术参照价: {_rm.get('stop_loss')}, ATR距离分档: {_rm.get('risk_grade','待核')}。{_rm.get('position_basis','仓位须查中央风险预算')}；技术参照不替换原合同止损。"
                        _fund_ctx = ""
                        if _fundamentals:
                            _f = _fundamentals
                            _is = _f.get("income_stmt", {})
                            _bs = _f.get("balance_sheet", {})
                            _cf = _f.get("cashflow", {})
                            _fin_dates = set()
                            for _st in [_is, _bs, _cf]:
                                for _v in _st.values():
                                    if isinstance(_v, dict):
                                        _fin_dates.update(_v.keys())
                            _fin_dates = sorted(_fin_dates, reverse=True)[:3]

                            def _fv(stmt, key, yr):
                                return (stmt.get(key, {}) or {}).get(yr)

                            _fin_lines = []
                            if _fin_dates:
                                for _yr in _fin_dates:
                                    _rev = _fv(_is, "Total Revenue", _yr)
                                    _op = _fv(_is, "Operating Income", _yr)
                                    _ni = _fv(_is, "Net Income", _yr)
                                    _ta = _fv(_bs, "Total Assets", _yr)
                                    _tl = _fv(_bs, "Total Liabilities Net Minority Interest", _yr)
                                    _eq = _fv(_bs, "Stockholders Equity", _yr)
                                    _ocf = _fv(_cf, "Operating Cash Flow", _yr)
                                    _fcf = _fv(_cf, "Free Cash Flow", _yr)
                                    _fin_lines.append(
                                        f"{_yr}: 营收{_fmt_fin(_rev)} 营业利润{_fmt_fin(_op)} 净利润{_fmt_fin(_ni)} "
                                        f"总资产{_fmt_fin(_ta)} 总负债{_fmt_fin(_tl)} 股东权益{_fmt_fin(_eq)} "
                                        f"经营现金流{_fmt_fin(_ocf)} 自由现金流{_fmt_fin(_fcf)}"
                                    )
                            _fund_ctx = f"""
    【财报数据（年报）】
    {chr(10).join(_fin_lines) if _fin_lines else '暂无'}
    市值: {_fmt_fin(_f.get('market_cap',0))} | P/E: {_f.get('trailing_pe',0):.1f} | P/B: {_f.get('price_to_book',0):.2f}
    行业: {_f.get('sector','')} - {_f.get('industry','')}
    公司简介: {_f.get('business_summary','')[:200]}"""
                        _mc_ctx = ""
                        if mc:
                            _mc_ctx = f"蒙特卡洛10日: 乐观P90={mc['p90']:.2f}, 中性P50={mc['p50']:.2f}, 悲观P10={mc['p10']:.2f}"

                        _vol_ctx = ""
                        _va = analyze_volume_anomaly(df)
                        if _va:
                            _vol_ctx = f"""
    【交易量异常解读（系统已标注）】
    类型: {_va['anomaly_type']} | 量比: {_va['vol_ratio']:.1f}x | 日涨跌: {_va['price_chg_1d']:+.1f}%
    信号: {_va['signal']} | 5日量能趋势: {_va['vol_trend_5d']:+.1f}%
    解读: {_va['explanation'].replace('**', '')}
    {_va.get('trend_note', '')}"""

                        # 【V94.4】把统一操作指引喂给 AI，强制与系统口径对齐
                        _guide_ctx = ""
                        try:
                            if _hz_decision:
                                _guide_ctx = (
                                    f"量价研究状态: {_hz_decision.get('cycle_status')}｜量价辅助分{_hz_decision['unified_score']}"
                                    f"（短{_hz_decision['short_score']}/中{_hz_decision['medium_score']}/长{_hz_decision['long_score']}）\n"
                                    f"2周上/下: {_hz_decision['p_up']}%/{_hz_decision['p_down']}%｜"
                                    f"盈亏比: {_hz_decision['rr']:.2f}｜期望: {_hz_decision['expected_pct']:+.1f}%｜"
                                    f"{_hz_decision['entry_note']}｜口径{_hz_decision['score_version']}")
                            if not _guide_ctx:
                                _guide_ctx = "量价底稿暂不可用：只解释中央原合同和证据缺口。"
                        except Exception:
                            pass

                        # 【V94.5】评分归因：把五维评分的通过/未通过因子摊开，让 AI 的每个判断
                        # 都能锚定到具体因子——这是"参考性"的核心（等同日报的事实台账，可追溯）
                        _score_ctx = ""
                        try:
                            def _factor_digest(rows):
                                ok, bad = [], []
                                for r in (rows or []):
                                    tag = str(r.get("因子", "")).strip()
                                    note = str(r.get("说明", "")).strip()
                                    state = str(r.get("状态", ""))
                                    item = f"{tag}({note})" if note else tag
                                    if "✅" in state:
                                        ok.append(item)
                                    elif "❌" in state or "⚠️" in state:
                                        bad.append(item)
                                return ok, bad
                            _c_ok, _c_bad = _factor_digest(metrics.get("canslim_rows"))
                            _s_ok, _s_bad = _factor_digest(metrics.get("spec_rows"))
                            _score_ctx = f"""【辅助研究质量归因（研究分 {_score_v}/100，不决定买卖，勿逐条复述）】
    成长质量(CANSLIM) — 达标: {', '.join(_c_ok) or '无'} ｜ 未达标: {', '.join(_c_bad) or '无'}
    趋势与动能 — 达标: {', '.join(_s_ok) or '无'} ｜ 未达标: {', '.join(_s_bad) or '无'}
    动能维度 {metrics.get('mom_score','N/A')}/100 ｜ ESG {metrics.get('esg_total','N/A')}({metrics.get('esg_grade','N/A')})"""
                        except Exception:
                            pass

                        # 【V88·统一裁决硬门】所有后续AI必须服从顶部同源五周期结论。
                        # 周期冲突时，基本面再好、短期期望再正，也不能输出推荐/建仓。
                        _cycle_gate_ctx = ""
                        if _hz_decision:
                            _cycle_gate_ctx = f"""
    【五周期量价研究（不是授级裁决）】
    量价辅助分：{_hz_decision.get('unified_score')}（短{_hz_decision.get('short_score')}/中{_hz_decision.get('medium_score')}/长{_hz_decision.get('long_score')}）；
    上/下估计：{_hz_decision.get('p_up')}%/{_hz_decision.get('p_down')}%；盈亏比{_hz_decision.get('rr')}；期望{_hz_decision.get('expected_pct')}%；
    周期状态：{_hz_decision.get('cycle_status')}；是否冲突：{'是' if _hz_decision.get('cycle_conflict') else '否'}。
    保留并解释周期冲突的反证，不得擅自改变中央评级、进场、目标、止损或期限；需要新方案时列明重新双审条件。
    相同日线的模型、书理解释与GPT论述不构成统计独立证据，不得把分数或情景值描述为已验证胜率。
    必须明确区分2周与4-16周，不得把短期反弹解释成中长期转多。"""

                        # 【V94.5】注入真实新闻日报：本股/其行业若在今日新闻中，催化必须锚定真实
                        # 事件并注明媒体；无相关新闻则写明，严禁编造（与新闻日报同一条铁律）
                        _stock_news_ctx = ""
                        try:
                            _rnr = _load_real_news_report()
                            if _rnr:
                                _stock_news_ctx = f"""
    【今日真实新闻报告（催化事件的唯一合法来源）】
    {_rnr[:3200]}
    ——若上文出现与 {target_c} 直接相关的公司/行业/宏观事件，须在分析中引用并注明媒体来源；若无，须明确写"今日无直接相关新闻催化"。严禁编造任何未在上文出现的事件、财报数字或政策。"""
                        except Exception:
                            pass

                        # 【V94.5】证据链纪要：移植 AI 新闻日报的"参考性"内核——先证据后判断、
                        # 每个结论走"信号→传导→价格/估值影响"链条、事实/推断/策略三分、失效条件可证伪、
                        # 标注置信度、禁绝对化语言。深度来自证据密度，不是字数堆砌。
                        from stock_verdict import _triad_record as _deep_ai_record
                        _ai_sel, _ai_row, _ai_allowed = _deep_ai_record(target_c)
                        from deep_prompt_context import build as _deep_prompt_build
                        _deep_central_prompt = json.dumps(_deep_prompt_build(
                            central={"tier": _ai_row.get("tier"), "executable": _ai_allowed,
                                     "trade_plan": _ai_row.get("trade_plan"), "audit_score": _ai_row.get("audit_score"),
                                     "audit_id": _ai_row.get("audit_id"), "factpack_id": _ai_sel.get("factpack_id")},
                            cross_validation=_deep_cross, joint_conclusion=_deep_synthesis,
                            annual_outlook=_deep_annual), ensure_ascii=False, separators=(',', ':'))
                        _unified_prompt = f"""你是买方机构的首席分析师，为投委会写一份可直接决策的个股研判。标准对标机构晨会纪要：事实可追溯、推理有链条、结论可执行、风险能证伪。禁止聊天体、行业科普、教科书式铺陈。

    【标的】{target_c}
    【中央评级约束】{_deep_central_prompt}
    只能解释所给中央合同；无正式执行资格时必须中性/回避、仓位0%、买点不参与。不得创造新的评级、止损或目标价。

    【最近完整收盘数据】
    最新价: {_curr_p:.2f} | RSI: {_rsi_v:.1f} | 量价辅助分: {_hz_decision.get('unified_score','待核')}/100 | 技术状态: {_hz_decision.get('cycle_status','待核')}
    {_guide_ctx}
    K线形态: {_pattern_v} | 夏普比率: {_sharpe_v} | 最大回撤: {_maxdd_v}
    {_vwap_v}
    {_mc_ctx}
    {_vol_ctx}
    {_score_ctx}
    {_cycle_gate_ctx}

    {_fund_ctx}
    {_stock_news_ctx}

    ━━━ 写作纪律（违反任一条即不合格）━━━
    0. 中央签名评级与原合同是唯一执行授权；必须逐条回应【joint_conclusion】的技术反证、基本面限制及review_blocks。必须结合【annual_outlook】解释未来0–3、3–6、6–9、9–12个月的条件路径；区分已发生的年线结构与未来推演，不得把短期分数、目标价或双审改称一年结论，不得编造季度收益或确定性拐点日期。五周期分只属量价辅助证据，不能压过企业事实，也不能覆盖原失效线。任何分歧未解决时禁止新增开仓。
    1. 先证据、后判断：每个判断必须挂靠上方某个具体数据/因子/新闻，不得空谈。
    2. 事实 / 推断 / 策略三分：事实照录不夸大；推断必须写出传导链（信号→对盈利或资金的影响→对价格或估值的影响）；策略必须带失效条件。
    3. 不复述数据原文，要给数字背后的含义与相互印证/矛盾之处（如"RS为负但站上年线"这类冲突必须点破并裁决）。
    4. 催化只能引用上方【真实新闻报告】中的事件并注明媒体；无相关新闻写"今日无直接相关新闻催化，基于基本面/技术结构判断"。严禁编造事件、财报数字、政策、订单。
    5. 禁绝对化语言（必涨/确定/无风险/一定）。凡推断用"可能/倾向/若…则…"。数据缺失直接写"数据不足"。
    6. 与【系统操作指引】结论一致时明确认同；不一致时必须给出分歧理由并说明你更信哪一方及为什么。
    7. 全文 550-800 字，信息密度优先——每句都要么是证据、要么是由证据推出的判断，无一句废话。

    ━━━ 严格按此结构输出（中文）━━━

    ## 📌 一句话结论
    **【操作评级：强烈推荐 / 推荐 / 中性 / 回避】** ｜ 核心逻辑一句话（≤50字，点明主要矛盾）

    ## 🔗 核心逻辑链（2-3条，每条是一条完整传导链）
    - [信号/证据] → [对盈利或资金面的影响] → [对价格/估值的含义] → [因此该怎么看]
    （示例格式，需替换为真实内容；成长股写盈利兑现链，题材股写资金/情绪链，价值股写估值修复链）

    ## 📊 证据台账
    | 证据 | 归属 | 含义 | 置信度 |
    |---|---|---|---|
    | [具体数据/因子/新闻] | 技术/基本面/资金/催化 | [对决策的直接含义] | 高/中/低 |
    （至少4行，须覆盖技术面、基本面、资金/动能、催化四类各≥1条；相互矛盾的证据要并列并在结论中裁决）

    ## 🎯 执行方案
    - **买点**：具体价位 + 触发条件（如"缩量回踩X并收阳"）；不建议买写"不参与，等信号"
    - **止损**：具体价位 + 一句理由（破位含义）
    - **目标**：第一目标 / 第二目标 具体价位 + 各自阻力依据
    - **仓位与节奏**：百分比 + 分批方式（回避=0%）
    - **盈亏比**：结合上方系统盈亏比给出你的评估

    ## ⚠️ 失效条件（三类各1条，须可证伪、可执行）
    - **技术失效**：出现什么形态/价位立即离场
    - **基本面失效**：哪个经营指标或财务信号恶化则证伪逻辑
    - **催化失效**：预期中的催化未兑现或反向的判定标准

    ## 🔄 跟踪信号
    - **转多**：出现什么可上调评级（1条）
    - **转空**：出现什么必须立刻放弃（1条）"""

                        _unified_result = ""
                        _unified_ph = st.empty()
                        for _chunk in call_model_api_stream(_unified_prompt, model_name=GEMINI_MODEL_NAME, max_output_tokens=4096):
                            _unified_result += _chunk
                            _unified_ph.markdown(_unified_result + " ▌")
                        _unified_ph.empty()

                        if _unified_result and not _unified_result.startswith("❌"):
                            st.session_state[_unified_ai_cache_key] = _unified_result
                            _save_ai_report_cache(_stock_ai_report_key, _unified_result)
                        else:
                            st.error(_unified_result or "❌ AI 分析生成失败，请重试")
                    except Exception as _uae:
                        st.error(f"❌ AI 综合分析失败: {str(_uae)[:100]}")

            if _unified_ai_cache_key in st.session_state:
                _ua_res = st.session_state[_unified_ai_cache_key]
                _ua_action = str((_hz_decision or {}).get("action") or "观察")
                _ua_conflict = bool((_hz_decision or {}).get("cycle_conflict"))
                _ua_no_entry_actions = {
                    "回避", "仅观察·不追涨", "等待短线止跌", "趋势偏多·等待回踩",
                    "观察", "持有观察·不加仓", "减仓", "评估减仓", "退出", "清仓",
                }
                from stock_verdict import _triad_record as _deep_central_record
                _deep_selection, _deep_row, _deep_can_execute = _deep_central_record(target_c)
                _ua_entry_blocked = not _deep_can_execute or _ua_conflict or _ua_action in _ua_no_entry_actions or bool(_deep_synthesis.get("entry_recheck_required", True))
                # 不能只检查“推荐”两个字：周期冲突时，AI若偷偷给了非零仓位或买点，
                # 同样属于可执行性冲突。三项必须同时通过才允许作为建议展示。
                _ua_plain = re.sub(r"[*_#]", "", str(_ua_res))
                _ua_safe_rating = bool(re.search(r"【操作评级：\s*(?:中性|回避)", _ua_plain))
                _ua_zero_position = bool(re.search(r"仓位与节奏\s*[:：]\s*0%", _ua_plain))
                _ua_no_buy = bool(re.search(r"买点\s*[:：]\s*(?:不参与|不建议买|等待)", _ua_plain))
                _ua_unsafe = bool(_ua_entry_blocked and not (
                    _ua_safe_rating and _ua_zero_position and _ua_no_buy))
                if _ua_entry_blocked:
                    st.warning(
                        f"⚠️ **V88唯一决策裁决**：{_hz_decision.get('cycle_note', '当前不满足新开仓条件')}；"
                        f"统一动作：**{_ua_action}**。"
                        "正期望和高盈亏比只代表2周情景，不得升级为买入。")
                if _ua_unsafe:
                    st.error("⛔ 该AI报告未同时满足‘中性/回避＋0%仓位＋不参与’，与V88唯一决策底稿冲突，已停止作为操作建议展示。请点击重新生成。")
                    with st.expander("查看被否决的旧报告（仅供审计，不可执行）", expanded=False):
                        st.markdown(_ua_res)
                    _ua_res = ""
                if _ua_res:
                    st.markdown(f"""<style>
    .unified-report {{background:#f9fafb;padding:1.5rem;border-radius:8px;border-left:4px solid #6366f1;font-size:14px;line-height:1.8;color:#374151;}}
    .unified-report h2 {{font-size:17px !important;font-weight:700 !important;margin:1.2rem 0 0.5rem 0 !important;color:#1f2937 !important;border-bottom:1px solid #e5e7eb;padding-bottom:0.3rem;}}
    .unified-report h3 {{font-size:15px !important;font-weight:600 !important;margin:0.9rem 0 0.4rem 0 !important;color:#374151 !important;}}
    .unified-report p {{font-size:14px !important;margin:0.5rem 0 !important;}}
    .unified-report ul,.unified-report ol {{font-size:13px !important;margin:0.4rem 0 !important;padding-left:1.5rem !important;}}
    .unified-report li {{margin:0.3rem 0 !important;}}
    .unified-report strong {{font-weight:600 !important;color:#1f2937 !important;}}
    </style><div class="unified-report">{_ua_res}</div>""", unsafe_allow_html=True)
                    st.caption(f"📌 AI 综合分析 · 模型: {_ai_model_label()}")
                    if COPY_UTILS_AVAILABLE:
                        CopyUtils.create_copy_button(_ua_res, button_text="📋 复制分析报告", key=f"copy_unified_{target_c}")
                    st.download_button("📥 下载报告", data=_ua_res, file_name=f"AI综合分析_{target_c}_{datetime.now().strftime('%Y%m%d')}.md", mime="text/markdown", key=f"dl_unified_{target_c}")
            elif not _run_unified_ai:
                st.info("👆 点击上方按钮，一键生成包含技术面、止损止盈、风控、财务、行业的 AI 综合分析报告")
        _unified_ai_frag()

        # 【V80.1修复】添加"清除分析"按钮，不自动清空
        st.markdown("---")
        if st.button("🔄 清除当前分析", key="clear_analysis", width='stretch'):
            st.session_state.scan_selected_code = None
            st.session_state.scan_selected_name = None
            st.rerun()
    else:
        # 【V87.4】增强深度分析错误提示
        st.error("❌ 数据获取失败，无法进行深度分析")
        
        # 检查是否是已知的退市股票
        delisted_stocks = {
            "ATVI": "动视暴雪 - 已被微软收购退市",
        }
        
        if code in delisted_stocks:
            st.warning(f"🚨 **{delisted_stocks[code]}**")
            st.info("💡 该股票已无法获取历史数据，建议分析其他活跃交易的股票")
        else:
            st.info("🔍 **可能的原因及解决方案：**")
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**📋 检查清单：**")
                st.markdown("""
                - ✅ 股票代码格式是否正确
                - ✅ 股票是否仍在交易
                - ✅ 网络连接是否正常
                - ✅ 代理设置是否有效
                """)
            
            with col2:
                st.markdown("**🛠️ 建议操作：**")
                st.markdown("""
                - 🔍 使用左侧搜索功能查找股票
                - 🛠️ 运行系统自检检查网络
                - 🏥 执行股票池健康检查
                - 🔄 尝试其他股票代码
                """)
        
        # 提供快速测试按钮
        st.markdown("**🚀 快速测试推荐股票：**")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🇺🇸 测试苹果(AAPL)", width='stretch'):
                st.session_state.scan_selected_code = "AAPL"
                st.session_state.scan_selected_name = "苹果"
                st.rerun()
                
        with col2:
            if st.button("🇭🇰 测试腾讯(00700)", width='stretch'):
                st.session_state.scan_selected_code = "00700"
                st.session_state.scan_selected_name = "腾讯控股"
                st.rerun()
                
        with col3:
            if st.button("🇨🇳 测试茅台(600519)", width='stretch'):
                st.session_state.scan_selected_code = "600519"
                st.session_state.scan_selected_name = "贵州茅台"
                st.rerun()


# ═══════════════════════════════════════════════════════════════
# 【引擎模式守卫】v88_lite 等以库形式加载本模块时设 V88_ENGINE_ONLY=1：
# 此处之上的引擎函数(fetch_stock_data/calculate_metrics_all/run_unified_scan/
# build_action_guidance)与股票池(RAW_US/HK/CN)均已就绪，此处之下是 Streamlit
# UI 渲染(含全市场重扫描)。引擎模式直接停止模块执行，避免 import 空跑数分钟 UI。
# ═══════════════════════════════════════════════════════════════
class _V88EngineReady(Exception):
    """引擎就绪哨兵：由 v88_lite 的自定义加载器捕获，非错误。"""
    pass

if os.environ.get("V88_ENGINE_ONLY") == "1":
    raise _V88EngineReady()




# Optional manual research shares the engine; individual analysis is rendered above.
st.markdown('<div id="v88-strategy-research"></div>', unsafe_allow_html=True)
tab_scanner = st.expander("🔎 手动策略研究", expanded=False)

# 【V91.8】用 st.fragment 包装猎手战位：缓存命中时仅 fragment 重跑，跳过全局市场分析，10 秒内显示
# 【V99.5】一键全选节流：每天最多3次，锚定 9:00/16:00/22:30（北京），省流量
_AUTOSCAN_SLOTS = ("09:00", "16:00", "22:30")
_AUTOSCAN_FILE = SCAN_CACHE_DIR / "autoscan_slots.json"

def _autoscan_state():
    from datetime import datetime as _d, timezone as _tz, timedelta as _td
    today = _d.now(_tz(_td(hours=8))).strftime("%Y-%m-%d")
    try:
        st_ = json.loads(_AUTOSCAN_FILE.read_text(encoding="utf-8"))
    except Exception:
        st_ = {}
    if st_.get("date") != today:
        st_ = {"date": today, "done": []}
    return st_

def _autoscan_due_slot():
    """返回当前应补跑的锚点(HH:MM)；无则 None。规则：已过时点且今日未跑过该时点，且当日<3次。"""
    from datetime import datetime as _d, timezone as _tz, timedelta as _td
    now = _d.now(_tz(_td(hours=8)))
    hm = now.strftime("%H:%M")
    st_ = _autoscan_state()
    if len(st_.get("done", [])) >= 3:
        return None
    passed = [s for s in _AUTOSCAN_SLOTS if s <= hm and s not in st_.get("done", [])]
    return passed[-1] if passed else None

def _autoscan_mark(slot):
    st_ = _autoscan_state()
    if slot and slot not in st_["done"]:
        st_["done"].append(slot)
    st_["last_ts"] = time.time()
    try:
        _AUTOSCAN_FILE.write_text(json.dumps(st_, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


with tab_scanner:
    @st.fragment
    def _scanner_fragment():
        st.markdown("#### 智能筛选引擎")

        # Scheduled market updates are owned by the background service.
        # Page opening only reads cache; it must not launch a second full scan.
        us_count, hk_count, cn_count = len(RAW_US), len(RAW_HK), len(RAW_CN_TOP)
        total_count = us_count + hk_count + cn_count
        st.caption(f'🔎 手动策略研究池：美股{us_count}、港股{hk_count}、A股{cn_count}，共{total_count}只。'
                   '全市场目录与有效日线覆盖率以页首验收表为准；后台独立更新，打开页面不另启动全量扫描。')

        # 初始化 session_state
        if 'scanner_results' not in st.session_state:
            st.session_state.scanner_results = {}
        # 【NEW V88 Phase 2】初始化取消标志
        if 'cancel_scan' not in st.session_state:
            st.session_state.cancel_scan = {'cancel': False}
        
        # 【V89.6.4 + V91.2】显示扫描缓存状态（10分钟有效）
        if 'scanner_results' in st.session_state and st.session_state.scanner_results:
            if 'scan_timestamp' in st.session_state.scanner_results:
                scan_time = st.session_state.scanner_results['scan_timestamp']
                scan_age = time.time() - scan_time
                ttl = get_smart_cache_ttl('daily')  # 全模块统一1小时
                remaining_sec = ttl - scan_age
                if remaining_sec > 0:
                    scan_time_str = time.strftime('%H:%M:%S', time.localtime(scan_time))
                    st.info(f"📦 使用缓存扫描结果 | 扫描时间: {scan_time_str} | 剩余 {remaining_sec/60:.1f} 分钟有效（1小时内不重复扫描）")
                else:
                    _expire_str = f"{ttl//3600}小时" if ttl >= 3600 else f"{ttl//60}分钟"
                    st.warning(f"⏰ 扫描缓存已过期（超过{_expire_str}），请重新扫描")
        
        # 【V89.6.4】添加清除缓存按钮
        clear_col1, clear_col2 = st.columns([3, 1])
        with clear_col2:
            if st.button("🗑️ 清除扫描缓存", help="清除所有扫描结果缓存（含文件持久化）", width='stretch'):
                st.session_state.scanner_results = {}
                _clear_scan_cache_files()
                st.toast("✅ 扫描缓存已清除", icon="🗑️")
                st.rerun()
        
        # 【NEW V88 Phase 2】扫描辅助函数
        def run_scan(scan_type, ma_target, pool, use_concurrent, scan_name, icon):
            """统一的扫描执行函数"""
            st.session_state.cancel_scan = {'cancel': False}
            scan_mode = "⚡ 并发" if use_concurrent else "🔄 串行"
            st.toast(f"扫描 {scan_name}... ({scan_mode})", icon=icon)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            _scan_t0 = time.time()

            def update_progress(current, total, stock_name):
                progress = current / total
                progress_bar.progress(progress)
                mode_text = "并发扫描" if use_concurrent else "扫描"
                _el = time.time() - _scan_t0
                _eta = (_el / current * (total - current)) if current > 3 else 0
                status_text.text(f"⏱ 已用 {_el:.0f}s · 预计剩余 {_eta:.0f}s ｜ 正在{mode_text}{scan_name} {current}/{total} ({progress*100:.0f}%) - {stock_name}")
            
            if use_concurrent and USE_NEW_MODULES:
                res, stats = batch_scan_analysis_concurrent(
                    pool, scan_type=scan_type, ma_target=ma_target,
                    progress_callback=update_progress, max_workers=10,
                    cancel_flag=st.session_state.cancel_scan
                )
            else:
                res, stats = batch_scan_analysis(
                    pool, scan_type=scan_type, ma_target=ma_target,
                    progress_callback=update_progress
                )
            
            progress_bar.empty()
            status_text.empty()
            
            if stats.get('cancelled', False):
                st.warning("⚠️ 扫描已取消")
            
            st.caption(f"✅ 成功扫描: {stats['success']} 只 | ❌ 失败/无数据: {stats['failed']} 只")
            
            if stats['failed'] > 0:
                display_scan_failures(stats['errors'], stats['failed'])
            
            return res, stats
        
        c_ctrl = st.container()
        with c_ctrl:
            # 【NEW V88 Phase 2】添加并发选项和取消按钮
            col_market, col_concurrent, col_cancel = st.columns([2, 2, 1])
            with col_market:
                scan_market = st.radio("市场", ["🌍 中美港全部", "美股", "港股", "A股"], horizontal=True, label_visibility="collapsed")
            with col_concurrent:
                use_concurrent = st.checkbox("⚡ 并发扫描（10线程，速度快3-4倍）", value=True, help="默认开启，15分钟内同类型扫描使用缓存不重复执行")
            with col_cancel:
                if st.button("🛑 取消", help="取消当前扫描", width='stretch'):
                    st.session_state.cancel_scan['cancel'] = True
                    st.toast("正在取消扫描...", icon="🛑")
            
            # 【V92 一页全策略】一键跑全部策略，一张表看全关键信息（单次取数，最快）
            do_scan_all = st.button(
                "🔍 一键全策略筛选（一页看全：MA30/MA60/MA120/综合评分/多重支撑/市场状态）",
                help="一次取数同时评估全部策略，汇成一张可点击表格，比逐个点更快",
                type="primary", width='stretch',
            )
            st.caption("👆 推荐：一页看全所有策略关键信息；或用下方单策略按钮单独筛选")

            # 【V82.12 + Regime-Adaptive】专业均线策略 + 市场状态自适应
            st.caption("💡 均线触底反弹策略：股价触及关键均线时，反弹概率大 | 🎯 市场状态自适应：先判状态再分流")
            c_btn1, c_btn2, c_btn3, c_btn4, c_btn5, c_btn6 = st.columns(6)
            
            do_scan_ma30 = c_btn1.button("📊 MA30短线", help="月线支撑，适合短线波段（3-7天）", width='stretch')
            do_scan_ma60 = c_btn2.button("📈 MA60季线", help="季线支撑，适合波段交易（1-3周）", width='stretch')
            do_scan_ma120 = c_btn3.button("📉 MA120半年", help="半年线支撑，适合中线布局（1-3月）", width='stretch')
            do_scan_top = c_btn4.button("🏆 V88统一分", help="短中长期、概率、盈亏比与周期轮转统一评分", width='stretch')
            do_scan_regime = c_btn5.button("🎯 市场状态自适应", help="先判 BULL/RANGE/BEAR，再给动作建议", type="primary", width='stretch', disabled=not REGIME_ENGINE_AVAILABLE)
            do_scan_safe = c_btn6.button("🛡️ 多重支撑", help="同时靠近多条均线，风险低", width='stretch')
            
            risk_preference = st.selectbox("风险偏好（市场状态自适应）", ["保守", "平衡", "进攻"], index=1, key="risk_pref_scanner")

        # 【V99.3】统一选池：「🌍 中美港全部」= 三市场合并一次扫完，不用分别点
        def _pick_pool():
            if scan_market == "🌍 中美港全部":
                return list(RAW_US) + list(RAW_HK) + list(RAW_CN_TOP)
            return RAW_US if scan_market == "美股" else (RAW_HK if scan_market == "港股" else RAW_CN_TOP)

        _mkt_of_code = market_of_code  # 全局统一口径（V99.6）
        
        # 【V91.2+V91.3+V91.4】扫描缓存：15分钟内同类型+同市场命中则跳过；支持文件持久化（刷新/新标签页后仍有效）
        def _scan_cache_hit(scan_type: str, risk_pref: str = None) -> bool:
            r = st.session_state.get('scanner_results') or {}
            if r.get('type') == scan_type and r.get('scan_market') == scan_market:
                if scan_type == 'regime' and risk_pref is not None and r.get('risk_preference') != risk_pref:
                    pass  # 继续检查文件
                else:
                    ts = r.get('scan_timestamp', 0)
                    ttl = get_smart_cache_ttl('daily')
                    if (time.time() - ts) < ttl:
                        return True
            # session_state 未命中时，尝试从文件加载（跨会话/刷新后仍有效）
            loaded = _load_scan_cache_from_file(scan_type, scan_market, risk_pref)
            if loaded:
                st.session_state.scanner_results = loaded
                return True
            return False
        
        # 【V92 一页全策略】一键全策略扫描处理
        if do_scan_all:
            if _scan_cache_hit('unified', risk_preference):
                st.toast("📦 使用缓存，无需重新扫描", icon="📦")
            else:
                pool = _pick_pool()
                st.toast(f"🔍 一键全策略扫描中...（{len(pool)}只）", icon="🔍")
                _u_pbar = st.progress(0)
                _u_status = st.empty()
                _u_t0 = time.time()

                def _update_unified_progress(current, total, stock_name):
                    _pct = current / total if total else 0
                    _u_pbar.progress(min(1.0, _pct))
                    _el = time.time() - _u_t0
                    _eta = (_el / current * (total - current)) if current > 3 else 0
                    _u_status.text(f"⏱ 已用 {_el:.0f}s · 预计剩余 {_eta:.0f}s ｜ 全策略扫描 {current}/{total} ({_pct*100:.0f}%) - {stock_name}")

                try:
                    # 「全部」模式：regime 基准用美股；个股 RS 引擎内本就按各自市场大盘算
                    _reg_mkt = "美股" if scan_market == "🌍 中美港全部" else scan_market
                    _u_rows, _u_stats, _u_meta = run_unified_scan(
                        pool, _reg_mkt, risk_preference, use_concurrent,
                        progress_callback=_update_unified_progress,
                    )
                    # 「全部」模式：结果表最前加「市场」列，一张表看三市场
                    if scan_market == "🌍 中美港全部" and _u_rows:
                        _u_rows = [{"市场": _mkt_of_code(r.get("代码", "")), **r} for r in _u_rows]
                    _u_pbar.empty(); _u_status.empty()
                    _reg = _u_meta.get('regime', 'N/A'); _conf = _u_meta.get('confidence', 0)
                    st.caption(f"✅ 命中: {_u_stats['success']} 只 | ❌ 失败: {_u_stats['failed']} 只 | 市场状态: {_reg} (置信度 {_conf:.0%})")
                    if _u_stats.get('failed', 0) > 0 and _u_stats.get('errors'):
                        display_scan_failures(_u_stats['errors'], _u_stats['failed'])
                    st.session_state.scanner_results = {
                        'type': 'unified', 'scan_market': scan_market, 'risk_preference': risk_preference,
                        'title': f"#### 🔍 全策略一页榜单 ({scan_market}) · 市场状态 {_reg}",
                        'caption': '量价辅助筛选；评级与交易条件引用中央原合同。',
                        'data': _u_rows, 'stats': _u_stats, 'key': 'unified_table',
                        'scan_timestamp': time.time(),
                    }
                    _save_scan_cache_to_file(st.session_state.scanner_results)
                except Exception as _ue:
                    _u_pbar.empty(); _u_status.empty()
                    st.error(f"❌ 全策略扫描异常: {str(_ue)[:120]}")
                    logging.error(f"run_unified_scan error: {_ue}")

        # 【V82.12 + NEW V88 Phase 2】重构扫描按钮逻辑（使用统一扫描函数）
        if do_scan_ma30:
            if _scan_cache_hit('ma30'):
                st.toast("📦 使用缓存，无需重新扫描", icon="📦")
            else:
                pool = _pick_pool()
                res, stats = run_scan("MA_TOUCH", 30, pool, use_concurrent, "MA30 短线反弹", "📊")
                st.session_state.scanner_results = {
                    'type': 'ma30', 'scan_market': scan_market,
                    'title': f"#### 📊 MA30 短线反弹 ({scan_market})",
                    'caption': "💡 适合短线波段交易，持仓3-7天，快进快出捕捉超跌反弹",
                    'data': res, 'stats': stats, 'key': 'ma30_table',
                    'scan_timestamp': time.time(),
                }
                _save_scan_cache_to_file(st.session_state.scanner_results)
        
        if do_scan_ma60:
            if _scan_cache_hit('ma60'):
                st.toast("📦 使用缓存，无需重新扫描", icon="📦")
            else:
                pool = _pick_pool()
                res, stats = run_scan("MA_TOUCH", 60, pool, use_concurrent, "MA60 季线机会", "📈")
                st.session_state.scanner_results = {
                    'type': 'ma60', 'scan_market': scan_market,
                    'title': f"#### 📈 MA60 季线机会 ({scan_market})",
                    'caption': "MA60量价观察；实际周期与进场条件查看中央原合同。",
                    'data': res, 'stats': stats, 'key': 'ma60_table',
                    'scan_timestamp': time.time(),
                }
                _save_scan_cache_to_file(st.session_state.scanner_results)
        
        if do_scan_ma120:
            if _scan_cache_hit('ma120'):
                st.toast("📦 使用缓存，无需重新扫描", icon="📦")
            else:
                pool = _pick_pool()
                res, stats = run_scan("MA_TOUCH", 120, pool, use_concurrent, "MA120 半年线布局", "📉")
                st.session_state.scanner_results = {
                    'type': 'ma120', 'scan_market': scan_market,
                    'title': f"#### 📉 MA120 半年线布局 ({scan_market})",
                    'caption': "MA120量价观察；均线支撑不构成企业价值或入场许可。",
                    'data': res, 'stats': stats, 'key': 'ma120_table',
                    'scan_timestamp': time.time(),
                }
                _save_scan_cache_to_file(st.session_state.scanner_results)
        
        if do_scan_top:
            if _scan_cache_hit('top'):
                st.toast("📦 使用缓存，无需重新扫描", icon="📦")
            else:
                pool = _pick_pool()
                res, stats = run_scan("TOP", None, pool, use_concurrent, "V88统一分 Top", "🏆")
                st.session_state.scanner_results = {
                    'type': 'top', 'scan_market': scan_market,
                    'title': f"#### 🏆 V88唯一统一分 Top 榜单 ({scan_market})",
                    'caption': "💡 唯一口径：短20%＋中25%＋长20%＋趋势质量15%＋入场胜算20%；同时显示概率、盈亏比、EV与动作",
                    'data': res, 'stats': stats, 'key': 'top_table',
                    'scan_timestamp': time.time(),
                }
                _save_scan_cache_to_file(st.session_state.scanner_results)
        
        if do_scan_regime and REGIME_ENGINE_AVAILABLE:
            if _scan_cache_hit('regime', risk_preference):
                st.toast("📦 使用缓存，无需重新扫描", icon="📦")
            else:
                pool = _pick_pool()
                st.toast("🎯 市场状态自适应扫描中...", icon="🎯")
                progress_bar = st.progress(0)
                status_text = st.empty()

                def update_regime_progress(current, total, stock_name):
                    pct = current / total
                    progress_bar.progress(pct)
                    status_text.text(f"🎯 市场状态自适应... {current}/{total} ({pct*100:.1f}%) - {stock_name}")

                try:
                    res, stats, regime_info, meta = run_regime_scan(
                        pool, use_concurrent, scan_market, risk_preference,
                        progress_callback=update_regime_progress
                    )
                    progress_bar.empty()
                    status_text.empty()
                    regime_str = meta.get("regime", "N/A")
                    conf = meta.get("confidence", 0)
                    st.caption(f"✅ 成功: {stats['success']} 只 | ❌ 失败: {stats['failed']} 只 | 市场状态: {regime_str} (置信度 {conf:.0%})")
                    if stats.get('failed', 0) > 0 and stats.get('errors'):
                        display_scan_failures(stats['errors'], stats['failed'])
                    _meta = meta or {}
                    _ts = _meta.get("scan_timestamp", "")
                    _dual = _meta.get("use_potential_engine", False)
                    _cap = f"💡 市场状态: {regime_str} | 风险偏好: {risk_preference} | 动作池: BUILD_NOW / FOLLOW_MID / LONG_CORE"
                    if _dual:
                        _cap += f" | 双引擎+三池 | {_ts}"
                    st.session_state.scanner_results = {
                        'type': 'regime', 'scan_market': scan_market, 'risk_preference': risk_preference,
                        'title': f"#### 🎯 市场状态自适应 榜单 ({scan_market})" + (" (Top50 双引擎)" if _dual else ""),
                        'caption': _cap,
                        'data': res, 'stats': stats, 'key': 'regime_table',
                        'scan_timestamp': time.time(),
                        'regime_info': regime_info,
                        'meta': meta,
                    }
                    _save_scan_cache_to_file(st.session_state.scanner_results)
                except Exception as e:
                    st.error(f"❌ 市场状态自适应扫描异常: {str(e)[:100]}")
                    logging.error(f"run_regime_scan error: {e}")
                    status_text.text("⚠️ 降级为综合评分...")
                    res, stats = batch_scan_analysis(pool, scan_type="TOP", ma_target=None, progress_callback=update_regime_progress)
                    progress_bar.empty()
                    status_text.empty()
                    st.session_state.scanner_results = {
                        'type': 'top', 'scan_market': scan_market,
                        'title': f"#### 🏆 综合评分 Top 榜单 ({scan_market})（降级）",
                        'caption': "⚠️ 市场状态引擎异常，已降级为综合评分",
                        'data': res, 'stats': stats, 'key': 'top_table',
                        'scan_timestamp': time.time(),
                    }
                    _save_scan_cache_to_file(st.session_state.scanner_results)
        
        if do_scan_safe:
            if _scan_cache_hit('safe_zone'):
                st.toast("📦 使用缓存，无需重新扫描", icon="📦")
            else:
                st.toast("扫描多重均线支撑标的...", icon="🛡️")
                st.markdown("#### 🛡️ 多重均线支撑 (安全区)")
                st.caption("💡 同时靠近MA30/MA60/MA120，多重支撑共振，风险低，适合保守型投资者")
            
                progress_bar = st.progress(0)
                status_text = st.empty()
            
                res_combined = []
                stats_safe = {'success': 0, 'failed': 0, 'errors': []}
            
                if scan_market == "美股":
                    all_pools = [(RAW_US, "美股")]
                elif scan_market == "港股":
                    all_pools = [(RAW_HK, "港股")]
                elif scan_market == "A股":
                    all_pools = [(RAW_CN_TOP, "A股")]
                else:
                    all_pools = [(RAW_US, "美股"), (RAW_HK, "港股"), (RAW_CN_TOP, "A股")]
            
                total_stocks = sum(len(pool) for pool, _ in all_pools)
                current_idx = 0
            
                for pool, mkt_label in all_pools:
                    for idx, item in enumerate(pool):
                        current_idx += 1
                        progress_pct = current_idx / total_stocks
                        progress_bar.progress(progress_pct)
                        stock_name = item[1] if len(item) > 1 else item[0]
                        status_text.text(f"正在扫描 {stock_name}... ({current_idx}/{total_stocks}, {progress_pct*100:.1f}%)")
                        try:
                            code = item[0]
                            name = item[1]
                            # 【V82.9关键修复】如果pool有3个元素，直接使用第3个
                            if len(item) >= 3:
                                c_fixed = item[2]
                            else:
                                c_fixed = to_yf_cn_code(code)
                            
                            # 【V87.1】添加请求间隔，避免触发API限流（每10个股票延迟0.5秒）
                            if idx > 0 and idx % 10 == 0:
                                time.sleep(0.5)
                            
                            df = fetch_stock_data(c_fixed)
                            
                            # 【V84.3】防御性检查
                            if df is None or df.empty:
                                stats_safe['failed'] += 1
                                continue
                            
                            m = calculate_metrics_all(df, c_fixed)
                            # 【V86修复】多重均线支撑：同时靠近MA30/MA60/MA120
                            # 降低评分要求，放宽容差
                            if m:
                                from v88_decision_core import evaluate_decision as _evaluate_safe_decision
                                _safe_dc = _evaluate_safe_decision(
                                    df, m.get('trend_full') or {}, name=name, code=code)
                            else:
                                _safe_dc = {}
                            if m and _safe_dc.get('unified_score', 0) > 35:
                                last_close = m['last']['Close']
                                last_low = m['last']['Low']
                                last_high = m['last']['High']
                                touch_count = 0
                                touch_mas = []
                                
                                for ma_n in [30, 60, 120]:
                                    ma_col = f'MA{ma_n}'
                                    if ma_col in m['df'].columns:
                                        ma_val = m['df'][ma_col].iloc[-1]
                                        # 【V86】放宽容差到8%，或者K线触及均线
                                        touched = (last_low <= ma_val <= last_high)
                                        close_enough = (ma_val > 0 and abs(last_close - ma_val) / ma_val < 0.08)
                                        
                                        if touched or close_enough:
                                            touch_count += 1
                                            distance_pct = abs(last_close - ma_val) / ma_val * 100 if ma_val > 0 else 0
                                            touch_mas.append(f"MA{ma_n}({distance_pct:.1f}%)")
                                
                                # 【V86】打印调试信息
                                if touch_count >= 2:
                                    _safe_print(f"[多重支撑] ✅ {code} ({name}): 触及{touch_count}条均线 - {', '.join(touch_mas)}, 统一分={_safe_dc['unified_score']}")
                                
                                # 只有触及2条或以上均线才算"多重支撑"
                                if touch_count >= 2:
                                    res_combined.append({
                                        "市场": mkt_label, "代码": code, "名称": name,
                                        "统一分": _safe_dc['unified_score'],
                                        "短/中/长": f"{_safe_dc['short_score']}/{_safe_dc['medium_score']}/{_safe_dc['long_score']}",
                                        "上/下估计": f"{_safe_dc['p_up']}%/{_safe_dc['p_down']}%",
                                        "盈亏比": f"{_safe_dc['rr']:.2f}",
                                        "期望值": f"{_safe_dc['expected_pct']:+.1f}%",
                                        "动作": _safe_dc['action'], "口径": _safe_dc['score_version'],
                                        "现价": f"{m['last_price']:.2f}",
                                        "触发": " + ".join(touch_mas)
                                    })
                                    stats_safe['success'] += 1
                        except Exception as e:
                            stats_safe['failed'] += 1
                            error_msg = f"{type(e).__name__}: {str(e)[:80]}"
                            stats_safe['errors'].append({
                                'code': item[0] if item else 'Unknown',
                                'name': item[1] if len(item) > 1 else 'Unknown',
                                'error': error_msg
                            })
            
                # 【V87.17】清除进度条
                progress_bar.empty()
                status_text.empty()
            
                # 【V85】显示统计和失败详情
                st.caption(f"✅ 成功扫描: {stats_safe['success']} 只 | ❌ 失败/无数据: {stats_safe['failed']} 只")
            
                # 【V87.8】如果有失败的股票,显示详情
                if stats_safe['failed'] > 0:
                    display_scan_failures(stats_safe['errors'], stats_safe['failed'])
            
                res_combined = sorted(res_combined, key=lambda x: x['统一分'], reverse=True)[:50]
                st.session_state.scanner_results = {
                    'type': 'safe_zone', 'scan_market': scan_market,
                    'title': f"#### 🛡️ 多重均线支撑 (安全区) ({scan_market})",
                    'caption': "💡 同时靠近MA30/MA60/MA120，多重支撑共振，风险低，适合保守型投资者",
                    'data': res_combined,
                    'stats': stats_safe,
                    'key': 'safe_zone',
                    'scan_timestamp': time.time()  # 【V89.6.4】添加扫描时间戳
                }
                _save_scan_cache_to_file(st.session_state.scanner_results)
        
        # 【V82.12】显示保存的扫描结果（支持caption）
        if st.session_state.scanner_results:
            # Every sink, including restored cache / CSV / copy, shares this
            # current projection. Never write it back over research history.
            from scanner_central import project_rows as _project_scan_central
            result_info = dict(st.session_state.scanner_results)
            result_info['data'] = [
                {k: v for k, v in r.items() if not k.startswith('_')}
                for r in _project_scan_central(result_info.get('data') or [])
            ]
            result_info['title'] = '### 🔎 策略扫描 · 中央评级核对'
            result_info['caption'] = '量价辅助分仅筛选研究线索；评级、执行许可和进场/止盈/失效条件均引用当前3A中央原合同。'

            # 【V99.6】市场列自愈：旧缓存/旧版本存的「市场」可能整列误标美股，
            # 渲染前一律按代码重判（全局唯一口径 market_of_code），CSV导出同步修正
            try:
                _rows_heal = result_info.get('data') or []
                if _rows_heal and isinstance(_rows_heal[0], dict) and '市场' in _rows_heal[0]:
                    for _rh in _rows_heal:
                        _rh['市场'] = market_of_code(_rh.get('代码', ''))
            except Exception:
                pass

            # 【Regime-Adaptive】市场状态简报（仅 type=regime 时）
            if result_info.get('type') == 'regime' and result_info.get('regime_info'):
                ri = result_info['regime_info']
                st.info(f"📊 **市场状态**: {ri.get('regime', 'N/A')} | 置信度: {ri.get('confidence', 0):.0%} | 驱动: {' '.join(ri.get('drivers_top3', []))}")
            
            # 【NEW V88 Phase 2】标题和导出按钮并排
            col_title, col_export = st.columns([4, 1])
            with col_title:
                st.markdown(result_info['title'])
                if 'caption' in result_info and result_info['caption']:
                    st.caption(result_info['caption'])
            with col_export:
                # 【NEW V88 Phase 2】CSV导出功能
                if result_info['data']:
                    df_export = pd.DataFrame(result_info['data'])
                    csv = df_export.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="📥 导出CSV",
                        data=csv,
                        file_name=f"scan_{result_info['type']}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        help="下载扫描结果为CSV文件",
                        width='stretch'
                    )
                    # 【V88·复制】榜单Top20一键复制成微信友好文本
                    with st.popover("📋 复制Top20", use_container_width=True):
                        _cp_rows = result_info['data'][:20]
                        _cp_lines = [f"📊 V88·{result_info.get('type','')}榜单 {pd.Timestamp.now().strftime('%m-%d %H:%M')}"]
                        for _i9, _r9 in enumerate(_cp_rows):
                            _cp_lines.append(
                                f"{_i9+1}. {_r9.get('市场','')}{_r9.get('名称','')}({_r9.get('代码','')}) "
                                f"{_r9.get('中央评级','未获当前评级')} · 审核分{_r9.get('中央审核分') if _r9.get('中央审核分') is not None else '—'} "
                                f"{_r9.get('中央动作','')} · 原周期{_r9.get('原周期','未核实')}".strip())
                        st.code("\n".join(_cp_lines), language=None)
            
            # 【NEW V88 Phase 2】表格筛选功能
            df_results = pd.DataFrame(result_info['data'])

            if not df_results.empty:
                if '市场' in df_results.columns:
                    _mkt_opts = ['🌍 全部'] + sorted(df_results['市场'].dropna().unique().tolist())
                    _sel_mkt = st.selectbox('🌏 市场筛选', _mkt_opts, key=f"filter_market_{result_info['key']}")
                    if _sel_mkt != '🌍 全部':
                        df_results = df_results[df_results['市场'] == _sel_mkt]
                filter_cols = st.columns(2)
                with filter_cols[0]:
                    col_name = '行业' if '行业' in df_results.columns else '板块'
                    if col_name in df_results.columns:
                        industries = ['全部'] + sorted(df_results[col_name].dropna().astype(str).unique().tolist())
                        selected_industry = st.selectbox('🏷️ 行业', industries, key=f"filter_industry_{result_info['key']}")
                        if selected_industry != '全部':
                            df_results = df_results[df_results[col_name] == selected_industry]
                with filter_cols[1]:
                    selected_grade = st.selectbox('🔖 中央评级', ['全部', '3A', '2A', '1A', '未获当前评级'],
                                                  key=f"filter_central_grade_{result_info['key']}")
                    if selected_grade != '全部':
                        df_results = df_results[df_results['中央评级'] == selected_grade]
                _leading = [c for c in ('代码', '名称', '股票', '市场', '行业', '中央评级', '中央审核分',
                                       '原周期', '中央动作', '原合同进场', '原合同止盈', '原合同失效',
                                       '原合同截止') if c in df_results.columns]
                df_results = df_results[_leading + [c for c in df_results.columns if c not in _leading]]

                # 首字母筛选（修复 A 段扎堆时快速找其他字母）
                if '代码' in df_results.columns:
                    _letters = sorted({
                        str(c)[0].upper() for c in df_results['代码'].astype(str)
                        if c and str(c)[0].isalpha()
                    })
                    _letter_opts = ["全部"] + _letters
                    _sel_letter = st.selectbox(
                        "🔤 首字母筛选",
                        _letter_opts,
                        key=f"filter_letter_{result_info['key']}",
                        help="按股票代码首字母筛选（A-Z）",
                    )
                    if _sel_letter != "全部":
                        df_results = df_results[
                            df_results['代码'].astype(str).str.upper().str.startswith(_sel_letter)
                        ]

                # 资金/水位快速筛选（手机端）
                if '资金' in df_results.columns:
                    _fc_extra = st.selectbox(
                        "💰 量能筛选",
                        ["全部", "💰 放量", "📉 缩量"],
                        key=f"filter_capital_{result_info['key']}",
                    )
                    if _fc_extra == "💰 放量":
                        df_results = df_results[df_results['资金'].str.contains('放量', na=False)]
                    elif _fc_extra == "📉 缩量":
                        df_results = df_results[df_results['资金'].str.contains('缩量', na=False)]
                
                # 显示筛选后的结果数量
                if len(df_results) < len(result_info['data']):
                    st.caption(f"🔍 筛选后: {len(df_results)} 只 / 总共 {len(result_info['data'])} 只")
            
            render_clickable_table(df_results, result_info['key'])
            with st.expander("📖 术语速查（得分/RSI/RS/量比…高低怎么看）"):
                try:
                    import cloud_engine as _ce_g2
                    st.markdown(_ce_g2.GLOSSARY_MD)
                except Exception:
                    pass
        
        st.divider()
        st.markdown("---")
    
    _scanner_fragment()

# ═══════════════════════════════════════════════════════════════
# 【后台扫描工具函数】供 tab_quant 使用
# ═══════════════════════════════════════════════════════════════
import subprocess as _subprocess

_SCAN_RESULTS_FILE  = _BRIEF_CACHE_DIR / "scan_results.json"
_SCAN_PROGRESS_FILE = _BRIEF_CACHE_DIR / "scan_progress.json"
_SCAN_HEARTBEAT_FILE = _BRIEF_CACHE_DIR / "scan_heartbeat.json"
_SCAN_PID_FILE      = _BRIEF_CACHE_DIR / "scan_worker.pid"
_SCAN_WORKER_SCRIPT = Path(__file__).parent / "scan_worker.py"
_SCAN_RESULT_TTL    = 8 * 3600    # 8 小时（留余量，GitHub Actions 每6h一次）

# Gist 配置：从 Streamlit Secrets 或环境变量读取
_GIST_ID = (
    st.secrets.get("GIST_ID", "")
    if hasattr(st, "secrets") else ""
) or os.environ.get("GIST_ID", "")
# Gist 本地缓存（避免每 20 秒都请求）
_gist_local_cache: dict = {}
_GIST_LOCAL_TTL  = 600    # 10 分钟内复用，不重复请求
_gist_last_sync_ts: float = 0.0
_gist_last_sync_ok: bool  = False


def _ssl_http_get(url: str, headers: dict | None = None, timeout: int = 12) -> bytes:
    """
    带 macOS SSL fallback 的 GET 请求。
    优先用 requests（SSL 更稳定），其次 urllib + ssl fallback。
    """
    hdrs = headers or {}
    try:
        import requests as _req
        resp = _req.get(url, headers=hdrs, timeout=timeout, verify=True)
        resp.raise_for_status()
        return resp.content
    except ImportError:
        pass
    except Exception:
        pass
    # urllib fallback（附带 macOS SSL 自动修复）
    import urllib.request as _ur
    import ssl as _ssl
    try:
        ctx = _ssl.create_default_context()
        req = _ur.Request(url, headers=hdrs)
        with _ur.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.read()
    except _ssl.SSLError:
        # macOS 本地证书缺失时跳过验证（开发环境兜底）
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode    = _ssl.CERT_NONE
        req = _ur.Request(url, headers=hdrs)
        with _ur.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.read()


def _ssl_http_post(url: str, payload: bytes,
                   headers: dict | None = None, timeout: int = 10) -> bytes:
    """
    带 macOS SSL fallback 的 POST 请求。
    """
    hdrs = {"Content-Type": "application/json", **(headers or {})}
    try:
        import requests as _req
        resp = _req.post(url, data=payload, headers=hdrs, timeout=timeout, verify=True)
        resp.raise_for_status()
        return resp.content
    except ImportError:
        pass
    except Exception:
        pass
    import urllib.request as _ur
    import ssl as _ssl
    try:
        ctx = _ssl.create_default_context()
        req = _ur.Request(url, data=payload, headers=hdrs)
        with _ur.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.read()
    except _ssl.SSLError:
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode    = _ssl.CERT_NONE
        req = _ur.Request(url, data=payload, headers=hdrs)
        with _ur.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.read()


def _scan_fetch_from_gist() -> dict | None:
    """
    用 GitHub API 拉取 Gist 内容（只需 GIST_ID，不需用户名）。
    支持 GIST_TOKEN（可读 secret gist）；带 10 分钟本地短缓存。
    """
    global _gist_local_cache, _gist_last_sync_ts, _gist_last_sync_ok, _gist_last_err
    if not _GIST_ID:
        return None
    # 本地缓存有效则直接返回
    cached = _gist_local_cache
    if cached and time.time() - cached.get("_fetched_at", 0) < _GIST_LOCAL_TTL:
        data = {k: v for k, v in cached.items() if k != "_fetched_at"}
        if time.time() - data.get("timestamp", 0) < _SCAN_RESULT_TTL:
            return data
    # 读取可选的 GIST_TOKEN（用于 secret gist 或提高 API rate limit）
    _gist_token = (
        st.secrets.get("GIST_TOKEN", "") if hasattr(st, "secrets") else ""
    ) or os.environ.get("GIST_TOKEN", "")
    try:
        api_url = f"https://api.github.com/gists/{_GIST_ID}"
        headers = {
            "Accept":     "application/vnd.github+json",
            "User-Agent": "StockAI-V88",
        }
        if _gist_token:
            headers["Authorization"] = f"Bearer {_gist_token}"
        raw = _ssl_http_get(api_url, headers=headers, timeout=12)
        gist_json = json.loads(raw.decode("utf-8"))
        # 从 Gist API 响应里取文件内容
        files = gist_json.get("files", {})
        if not files:
            raise ValueError("Gist 为空（GitHub Actions 可能尚未运行）")
        content = None
        for fname, fdata in files.items():
            if "scan_results" in fname.lower() or fname.endswith(".json"):
                content = fdata.get("content", "")
                break
        if not content:
            raise ValueError(f"Gist 文件中无 scan_results，现有文件: {list(files.keys())}")
        data = json.loads(content)
        if not data.get("timestamp"):
            raise ValueError("Gist 数据无 timestamp 字段")
        # 同步写本地文件备用：只在 Gist 数据比本地文件更新时才覆写
        # 防止旧 Gist 数据覆盖刚完成的本地扫描结果
        try:
            _BRIEF_CACHE_DIR.mkdir(exist_ok=True)
            gist_ts = data.get("timestamp", 0)
            local_ts = 0
            if _SCAN_RESULTS_FILE.exists():
                try:
                    _local = json.loads(_SCAN_RESULTS_FILE.read_text(encoding="utf-8"))
                    local_ts = _local.get("timestamp", 0)
                except Exception:
                    pass
            if gist_ts >= local_ts:   # Gist 更新或相同才写入
                _SCAN_RESULTS_FILE.write_text(
                    json.dumps(data, ensure_ascii=False), encoding="utf-8"
                )
        except Exception:
            pass
        _gist_local_cache  = {**data, "_fetched_at": time.time()}
        _gist_last_sync_ts = time.time()
        _gist_last_sync_ok = True
        _gist_last_err     = ""
        return data
    except Exception as _e:
        _gist_last_sync_ts = time.time()
        _gist_last_sync_ok = False
        _gist_last_err     = str(_e)[:120]
        return None


_gist_last_err: str = ""


def _gist_sync_status() -> str:
    """返回云端同步状态字符串，用于 UI 展示"""
    if not _GIST_ID:
        return "⚙️ 未配置 GIST_ID（Secrets 里加 GIST_ID = \"...\" 即可）"
    if _gist_last_sync_ts == 0:
        return "🔄 云端尚未同步（页面加载后首次轮询中）"
    ago   = int(time.time() - _gist_last_sync_ts)
    t_str = f"{ago//60}分{ago%60}秒前" if ago >= 60 else f"{ago}秒前"
    if _gist_last_sync_ok:
        return f"☁️ 云端同步成功（{t_str}）"
    hint = ""
    if "尚未运行" in _gist_last_err or "为空" in _gist_last_err:
        hint = " · GitHub Actions 尚未写入数据，可手动触发"
    elif "GIST_ID" in _gist_last_err or "404" in _gist_last_err:
        hint = " · GIST_ID 有误，请检查 Secrets"
    return f"⚠️ 云端读取失败（{t_str}）{hint}"


def _scan_write_heartbeat():
    """更新心跳文件（页面每次 fragment 执行时调用）"""
    try:
        _BRIEF_CACHE_DIR.mkdir(exist_ok=True)
        _SCAN_HEARTBEAT_FILE.write_text(
            json.dumps({"ts": time.time()}), encoding="utf-8"
        )
    except Exception:
        pass


def _scan_read_results() -> dict | None:
    """
    读取扫描结果：比较本地文件与 GitHub Gist 的 timestamp，取最新的。
    这样本地「重扫」的结果不会被旧 Gist 数据覆盖。

    云端模式（_GIST_ID 已配置）：即使 Gist 数据过期，也返回并标记 _stale=True，
    避免 GitHub Actions 下一次执行前出现"无结果"的空窗期。
    """
    now = time.time()

    # 1. 尝试本地文件
    local_data = None
    try:
        local_data = json.loads(_SCAN_RESULTS_FILE.read_text(encoding="utf-8"))
        if now - local_data.get("timestamp", 0) >= _SCAN_RESULT_TTL:
            local_data = None
    except Exception:
        local_data = None

    # 2. 尝试 Gist
    gist_data = _scan_fetch_from_gist()
    gist_stale = False
    if gist_data and now - gist_data.get("timestamp", 0) >= _SCAN_RESULT_TTL:
        if _GIST_ID:
            gist_stale = True  # 云端模式：保留过期数据，标记为 stale
        else:
            gist_data = None

    # 3. 优先使用更新（timestamp 更大）的来源
    if local_data and gist_data:
        chosen = local_data if local_data.get("timestamp", 0) >= gist_data.get("timestamp", 0) else gist_data
        if chosen is gist_data and gist_stale:
            chosen["_stale"] = True
        return chosen
    result = local_data or gist_data
    if result is gist_data and gist_stale and result is not None:
        result["_stale"] = True
    return result


def _scan_read_progress() -> dict:
    """读取进度文件；返回 {pct, status, detail, ts} 或默认 idle"""
    try:
        return json.loads(_SCAN_PROGRESS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"pct": 0, "status": "idle", "detail": "", "ts": 0}


def _scan_worker_running() -> bool:
    """检查 scan_worker.py 进程是否存活"""
    try:
        pid = int(_SCAN_PID_FILE.read_text(encoding='utf-8').strip())
        from runtime_guard import process_alive
        return process_alive(pid)
    except Exception:
        return False


def _scan_start_worker(force: bool = False):
    """启动后台扫描进程（非阻塞）"""
    import sys as _sys
    if _scan_worker_running():
        return
    _BRIEF_CACHE_DIR.mkdir(exist_ok=True)
    cmd = [_sys.executable, str(_SCAN_WORKER_SCRIPT)]
    if force:
        cmd.append("--force")
    _subprocess.Popen(
        cmd,
        stdout=open(_BRIEF_CACHE_DIR / "scan_worker.log", "a"),
        stderr=_subprocess.STDOUT,
        close_fds=True,
    )


def _scan_result_remaining() -> int | None:
    """返回结果缓存剩余秒数，无结果返回 None"""
    try:
        data = json.loads(_SCAN_RESULTS_FILE.read_text(encoding="utf-8"))
        rem  = int(_SCAN_RESULT_TTL - (time.time() - data.get("timestamp", 0)))
        return max(0, rem)
    except Exception:
        return None


def _scan_result_label() -> str:
    """返回缓存剩余时间字符串"""
    rem = _scan_result_remaining()
    if rem is None:
        return ""
    if rem <= 0:
        return "⏰ 缓存已过期"
    h, m = divmod(rem // 60, 60)
    return f"⏱ 缓存剩余 {h}h {m:02d}m"


def _scan_force_clear():
    """清除扫描结果，触发重新扫描"""
    for f in (_SCAN_RESULTS_FILE, _SCAN_PROGRESS_FILE):
        try:
            f.unlink(missing_ok=True)
        except Exception:
            pass


# ── 宏观风险评估（Gemini）────────────────────────────────────────
_MACRO_RISK_CACHE_FILE = _BRIEF_CACHE_DIR / "macro_risk_cache.json"
_MACRO_RISK_TTL        = 6 * 3600   # 6小时刷新一次

def _load_macro_risk_cache() -> dict | None:
    try:
        if _MACRO_RISK_CACHE_FILE.exists():
            data = json.loads(_MACRO_RISK_CACHE_FILE.read_text(encoding="utf-8"))
            if time.time() - data.get("ts", 0) < _MACRO_RISK_TTL:
                return data
    except Exception:
        pass
    return None


def _save_macro_risk_cache(data: dict):
    try:
        _BRIEF_CACHE_DIR.mkdir(exist_ok=True)
        _MACRO_RISK_CACHE_FILE.write_text(
            json.dumps({**data, "ts": time.time()}, ensure_ascii=False),
            encoding="utf-8"
        )
    except Exception:
        pass


def _fetch_macro_risk(force_refresh: bool = False) -> dict:
    """
    调用 Gemini 评估当前全球宏观 / 地缘风险，返回结构化结果。
    缓存优先级：文件缓存（6h）> st.session_state（会话级）> Gemini API
    """
    import re as _re_json

    # 1. 文件缓存（6小时 TTL）
    if not force_refresh:
        cached = _load_macro_risk_cache()
        if cached and not cached.get("_error"):   # 只返回成功的文件缓存
            return cached
        # 2. 会话缓存（防止每 20 秒重复调用）——只返回成功结果，错误结果不缓存
        _ss_key = "_macro_risk_result"
        _cached_ss = st.session_state.get(_ss_key)
        if _cached_ss and not _cached_ss.get("_error"):
            return _cached_ss

    if not GPT_SUBSCRIPTION_READY:
        fb = _macro_risk_fallback("GPT-6 Codex订阅未登录")
        st.session_state["_macro_risk_result"] = fb
        return fb

    today = datetime.now().strftime("%Y年%m月%d日")
    prompt = f"""今天是 {today}。以全球宏观对冲基金视角评估当前市场风险。

直接输出JSON，不要代码块、不要注释、不要多余文字：
{{"risk_level":3,"risk_label":"中等风险","summary":"一句话宏观概述","key_risks":["风险A","风险B","风险C"],"hot_sectors":["板块1","板块2"],"warn_sectors":["板块1","板块2"],"bias":"均衡","bias_reason":"简短理由"}}

risk_level为1-5整数，其余字段用简短中文填写。"""

    _err_msg = ""
    try:
        raw = str(call_model_api(prompt) or "").strip()
        if not raw:
            raise ValueError("API 返回空文本")

        # 提取 JSON：贪婪匹配最外层 {} 块
        _m = _re_json.search(r'\{[\s\S]*\}', raw)
        if _m:
            raw = _m.group(0)
        else:
            raw = raw.strip('`').strip()
            if raw.lower().startswith('json'):
                raw = raw[4:].strip()

        # JSON 修复：补全截断的 JSON（末尾缺少 "、] 或 }）
        def _repair_json(s: str) -> str:
            s = s.rstrip()
            # 统计未闭合的引号（奇数个说明字符串未关闭）
            in_str = False
            escaped = False
            for ch in s:
                if escaped:
                    escaped = False
                    continue
                if ch == '\\':
                    escaped = True
                    continue
                if ch == '"':
                    in_str = not in_str
            if in_str:
                s += '"'   # 补上未关闭的字符串
            # 补上未关闭的数组和对象
            opens = {'[': ']', '{': '}'}
            stack = []
            in_s = False
            esc = False
            for ch in s:
                if esc:
                    esc = False
                    continue
                if ch == '\\':
                    esc = True
                    continue
                if ch == '"':
                    in_s = not in_s
                    continue
                if not in_s:
                    if ch in opens:
                        stack.append(opens[ch])
                    elif ch in opens.values():
                        if stack and stack[-1] == ch:
                            stack.pop()
            while stack:
                s += stack.pop()
            return s

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            repaired = _repair_json(raw)
            data = json.loads(repaired)

        _colors = {1: "#10b981", 2: "#22c55e", 3: "#f59e0b", 4: "#f97316", 5: "#ef4444"}
        data["risk_color"] = _colors.get(int(data.get("risk_level", 3)), "#6b7280")
        data["_error"] = ""
        _save_macro_risk_cache(data)
        st.session_state["_macro_risk_result"] = data
        return data
    except Exception as e:
        _err_msg = f"{type(e).__name__}: {e}"
        _safe_print(f"⚠️ 宏观风险评估失败: {_err_msg}")

    fb = _macro_risk_fallback(_err_msg)
    # 失败结果不存入 session_state，下次 fragment 刷新时自动重试
    # （只有成功结果才缓存，防止错误锁死）
    return fb


def _macro_risk_fallback(err_detail: str = "") -> dict:
    return {
        "risk_level": 3,
        "risk_label": "风险评估不可用",
        "risk_color": "#6b7280",
        "summary": "宏观风险评估暂时不可用，建议保持均衡仓位。",
        "key_risks": [],
        "hot_sectors": [],
        "warn_sectors": [],
        "bias": "均衡",
        "bias_reason": "无法评估，建议保持均衡仓位",
        "_error": err_detail,  # 保存真实错误便于 UI 展示和诊断
    }


# ── 钉钉推送 ──────────────────────────────────────────────────────
def _dingtalk_send(text: str) -> tuple[bool, str]:
    """发送文本到钉钉机器人（支持加签）"""
    webhook = (
        st.secrets.get("DINGTALK_WEBHOOK", "")
        if hasattr(st, "secrets") else ""
    ) or os.environ.get("DINGTALK_WEBHOOK", "")
    secret = (
        st.secrets.get("DINGTALK_SECRET", "")
        if hasattr(st, "secrets") else ""
    ) or os.environ.get("DINGTALK_SECRET", "")

    if not webhook:
        return False, "未配置 DINGTALK_WEBHOOK"
    try:
        import urllib.parse as _up
        url = webhook
        if secret:
            import hmac as _hmac, hashlib as _hs, base64 as _b64
            ts       = str(round(time.time() * 1000))
            sign_str = f"{ts}\n{secret}"
            sig      = _b64.b64encode(
                _hmac.new(secret.encode(), sign_str.encode(), _hs.sha256).digest()
            ).decode()
            url = f"{webhook}&timestamp={ts}&sign={_up.quote_plus(sig)}"
        payload = json.dumps({
            "msgtype": "text",
            "text":    {"content": text},
            "at":      {"isAtAll": False},
        }).encode("utf-8")
        raw    = _ssl_http_post(url, payload=payload, timeout=10)
        result = json.loads(raw.decode("utf-8"))
        if result.get("errcode", -1) == 0:
            return True, "ok"
        return False, result.get("errmsg", "unknown")
    except Exception as e:
        return False, str(e)[:120]


def _dingtalk_push_top30(res: dict | None) -> tuple[bool, str]:
    """
    把 Top30 趋势榜推送到钉钉。
    消息头包含「股票行情」确保通过钉钉关键词安全校验。
    """
    if not res:
        return False, "暂无扫描结果"
    from datetime import datetime as _dt_push
    ts_str = _dt_push.fromtimestamp(res.get("timestamp", 0)).strftime("%m-%d %H:%M")

    # ⚠️ 钉钉关键词安全校验：消息必须包含机器人配置的关键词
    # 默认在标题中包含「股票行情」，覆盖大多数常见关键词设置
    # 如机器人设置了其他关键词，请在 DINGTALK_KEYWORD 里配置
    _kw = (
        st.secrets.get("DINGTALK_KEYWORD", "") if hasattr(st, "secrets") else ""
    ) or os.environ.get("DINGTALK_KEYWORD", "股票行情")
    if not _kw:
        _kw = "股票行情"

    lines = [
        f"【{_kw}】V88 AI选股 · {ts_str}",
        "━━━━━━━━━━━━━━━━━━━━━━",
    ]

    mkt_map = {"US": "🇺🇸 美股", "HK": "🇭🇰 港股", "CN": "🇨🇳 A股"}
    for mkt_key, mkt_name in mkt_map.items():
        mkt_data = res.get(mkt_key, {})
        top_list = mkt_data.get("top", [])[:5]
        if not top_list:
            continue
        lines.append(f"\n{mkt_name} 趋势 Top5")
        for i, item in enumerate(top_list, 1):
            name  = item.get("name", item.get("股票", ""))
            code  = item.get("code", item.get("代码", ""))
            score = item.get("score", item.get("得分", 0))
            shape = item.get("shape", item.get("形态", ""))
            lines.append(f"  {i}. {name}({code})  得分{score}  {shape}")

    lines += [
        "\n━━━━━━━━━━━━━━━━━━━━━━",
        "⚠️ 以上仅供参考，不构成投资建议",
        "🔗 来源：V88 GitHub Actions 云端扫描",
    ]
    return _dingtalk_send("\n".join(lines))



    # ═══════════════════════════════════════════════════════════════
    # 【模块 ④】股票PK对决（仅在有对比股票时显示）
    # ═══════════════════════════════════════════════════════════════
if st.session_state.get('pk_codes') and len(st.session_state.pk_codes) >= 2:
    _module_header("⚔️", "股票深度对比", "2～4只中央评级、原合同、共同日期走势及相关性核对；各股行情日期单列。")
    
    pk_codes = st.session_state.pk_codes
    pk_names = st.session_state.get('pk_names', pk_codes)
    
    st.markdown(f"### 📊 对比：{' vs '.join(pk_names)}")
    
    # 【V87.17】添加进度条
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    pk_results = []
    _pk_hist, _pk_rank = {}, []      # 深度对比用:{名称:收盘序列} / 排序卡行
    total_stocks = len(pk_codes)
    
    for idx, code in enumerate(pk_codes):
        name = pk_names[idx] if idx < len(pk_names) else code
        yf_code = to_yf_cn_code(code)
        
        # 【V87.17】更新进度
        progress_pct = (idx + 1) / total_stocks
        progress_bar.progress(progress_pct)
        status_text.text(f"正在获取 {name} 数据... ({idx + 1}/{total_stocks}, {progress_pct*100:.1f}%)")
        
        df_pk = fetch_stock_data(yf_code)
        
        if df_pk is not None and len(df_pk) > 0:
            metrics = calculate_metrics_all(df_pk, yf_code)
            quant = calculate_advanced_quant(df_pk)
            
            # 安全获取metrics数据
            if metrics:
                from v88_decision_core import evaluate_decision as _evaluate_pk_decision
                _pk_dc = _evaluate_pk_decision(
                    df_pk, metrics.get('trend_full') or {}, name=name, code=code)
                # 【2026-08-01 深度对比】同一份已拉到的K线顺手带出:收盘序列(走势图+相关性)
                # 与排序卡字段。不额外拉数据、不调AI,零成本。
                try:
                    _pk_hist[name] = {str(day)[:10]: float(px) for day, px in df_pk['Close'].items()}
                    _pk_rank.append({
                        'name': name, 'code': code,
                        'technical_score': _pk_dc.get('unified_score'),
                        'data_asof': str(df_pk.index[-1])[:10]})
                except Exception:
                    logging.exception("[V88] 深度对比字段收集失败 %s", code)
                pk_results.append({
                    '股票': name, '代码': code, '市场': market_of_code(code),
                    '当前价': float(df_pk['Close'].iloc[-1]),
                    '行情日期': str(df_pk.index[-1])[:10],
                    '量化辅助分': _pk_dc.get('unified_score'),
                    '短/中/长辅助分': f"{_pk_dc.get('short_score',0)}/{_pk_dc.get('medium_score',0)}/{_pk_dc.get('long_score',0)}",
                    '技术价位RR（非合同）': _pk_dc.get('rr'),
                    '量价口径': _pk_dc.get('score_version', 'V88-U2.0'),
                    'RSI': round(float(metrics.get('rsi', 50)), 1),
                    '历史价格夏普（非策略）': quant.get('sharpe', 'N/A'),
                    '历史价格最大回撤': quant.get('max_dd', 'N/A')})

    # 【V87.17】清除进度条
    progress_bar.empty()
    status_text.empty()
    
    if pk_results:
        from deep_cross_validation import load_context as _load_pk_context
        from compare_ui import comparison_records as _pk_records
        _pk_contexts = {str(r['code']): _load_pk_context(r['code']) for r in _pk_rank}
        _pk_central_records = _pk_records(_pk_rank, contexts=_pk_contexts)
        _pk_records_by_code = {r['code']: r for r in _pk_central_records}
        # 【V88·深度对比 2026-08-01 用户"最多四只·帮我设计"】
        # 原来只有一张12列指标表=参数并列,看完还得自己心算"那我买哪只"。
        # 补三块回答真正的问题:谁值得买 / 是不是同一个赌注 / 同期谁跑赢。原表格保留在下方。
        try:
            from compare_ui import verdict_html as _vh9, family_html as _fh9, trend_svg as _ts9
            if _pk_rank:
                st.markdown("#### ① 中央评级与原合同对照")
                st.markdown(_vh9(_pk_rank, contexts=_pk_contexts), unsafe_allow_html=True)
            if len(_pk_hist) >= 2:
                st.markdown("#### ② 同日收益相关性")
                st.markdown(_fh9(_pk_hist), unsafe_allow_html=True)
                st.markdown("#### ③ 共同交易日期走势")
                _cw9 = st.radio("走势窗口", ["20日", "60日", "120日"], horizontal=True,
                                index=1, key="_pk_win9", label_visibility="collapsed")
                st.markdown(_ts9(_pk_hist, days=int(_cw9[:-1])), unsafe_allow_html=True)
            st.markdown("#### ④ 全指标横排")
        except Exception:
            logging.exception("[V88] 深度对比区块渲染失败")
        # The table and existing GPT explanations use the same validated
        # records as the comparison cards, with no independent AI winner.
        _pk_display_rows = []
        for _aux in pk_results:
            _rec = _pk_records_by_code.get(_aux['代码']) or {}
            _plan = _rec.get('trade_plan') or {}
            _pc = _plan.get('profit_contract') or {}
            _band_pk = lambda v: '～'.join(str(n) for n in v) if isinstance(v, (list, tuple)) and len(v) == 2 else '未核实'
            _pk_display_rows.append({
                '代码': _aux['代码'], '名称': _rec.get('name') or _aux['股票'],
                '中央评级': _rec.get('tier') or '未获当前评级',
                '中央审核分': _rec.get('audit_score'),
                '原周期': {'short':'短期', 'medium':'中期', 'long':'长期'}.get(_rec.get('horizon'), '未核实'),
                '原合同进场': _band_pk(_plan.get('entry_range')),
                '原合同止盈': _band_pk(_plan.get('take_profit_range')),
                '原合同失效': str(_plan.get('stop') or '未核实'),
                '原合同截止': _pc.get('thesis_deadline') or '未核实',
                '核对结论': '；'.join(_rec.get('errors') or []) or '一致；执行另查中央当次闸门',
                **{k:v for k,v in _aux.items() if k not in ('代码','股票')}})
        st.dataframe(pd.DataFrame(_pk_display_rows), width='stretch', hide_index=True)
        st.caption('同一中央发布、同一原合同；量价指标不参与审核分排序。原合同仅供研究，不新增交易许可。')
        st.markdown('#### 🧠 GPT双审与书籍依据')
        st.caption('直接复用已签名的当前审核；查看不额外调用模型。缺证或过期结论保留提示，不由临时点评替代。')
        from review_display import current_scorecard as _pk_current_card
        for _rec in _pk_central_records:
            _ctx = _pk_contexts.get(_rec['code']) or {}
            with st.expander(f"{_rec['name']} · GPT依据与反证", expanded=False):
                _card = _pk_current_card(_ctx.get('selection') or {}, _ctx.get('row') or {})
                _g = _card.get('gpt') or {}
                if not _g.get('current'):
                    st.caption('当前GPT审核缺失或过期；历史解释不能形成新的推荐。')
                else:
                    for _criterion in _g.get('criteria') or []:
                        _score = _criterion.get('score')
                        st.markdown(f"**{_criterion.get('title','核对项')} · {_score if _score is not None else '缺证'}/20**")
                        st.caption(_criterion.get('reason') or '缺少可核验理由')
                    with st.expander('独立反审与书籍逐条原证据', expanded=False):
                        st.json({'双审': _card.get('review_pair'), '书籍': (_card.get('books') or {}).get('checks')}, expanded=False)
        if st.button('🔄 清除对比', key='btn_clear_pk'):
            st.session_state.pk_codes = None
            st.session_state.pk_names = None
            st.rerun()



# 【V88·瘦身 2026-07-17】「与AI对话」问答区已删除（被权威日报+深度分析AI综合替代,用户确认基本不用;git可回溯）
