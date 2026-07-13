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

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import requests
import os
import time
import json
import urllib3
from datetime import datetime
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
_AUTHORITATIVE_REPORT = Path.home() / "Desktop" / "ai-daily-report-v2" / "data" / "daily_report.md"
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
    check_snapshot=False 仅保留为旧协议兼容；当前Plan B必须绑定当天快照。"""
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
    """【V88·全站统一 Plan A/B】唯一数据源：桌面今日导航/AI简报/云端 三处必须调用同一份，禁止各读各的。
    Plan A=今日报告硬质检通过；Plan B=基于当天新闻/快照/榜单生成的纯观察安全版，绝不复制历史日报；
    都没有则如实告知。返回 (content, meta)。
    文件很小，逐次读取以确保Plan B生成后立即生效，避免旧“missing”状态残留。"""
    global _AUTHORITATIVE_BRIEF_META

    content, ts, meta = _validate_plan_a(_AUTHORITATIVE_REPORT, _AUTHORITATIVE_MANIFEST, _AUTHORITATIVE_SNAPSHOT)
    if content is not None and meta.get("status") in ("passed", "legacy"):
        result = (content, {**meta, "plan": "A", "ts": ts})
        _AUTHORITATIVE_BRIEF_META = result[1]
        return result

    # Plan A 不合格：只接受当天、同快照、校验和一致的安全 Plan B。
    _today_issues = meta.get("issues") or []
    pb_content, pb_ts, pb_meta = _validate_plan_a(
        _AUTHORITATIVE_PLAN_B_REPORT, _AUTHORITATIVE_PLAN_B_MANIFEST, _AUTHORITATIVE_SNAPSHOT)
    _today_bj = datetime.now().strftime("%Y-%m-%d")
    if (pb_content is not None and pb_meta.get("status") == "plan_b"
            and str(pb_meta.get("generated_at") or "")[:10] == _today_bj):
        result = (pb_content, {**pb_meta, "plan": "B", "status": "plan_b",
                               "ts": pb_ts, "today_issues": _today_issues, "today_ts": ts})
        _AUTHORITATIVE_BRIEF_META = result[1]
        return result

    # Plan A/B 均不可用：真空，如实告知
    result = (None, {"plan": None, "status": "missing", "issues": _today_issues, "ts": ts})
    _AUTHORITATIVE_BRIEF_META = result[1]
    return result


def _load_authoritative_brief():
    """兼容旧调用名：Plan A为硬质检版，Plan B为当天纯观察安全版。"""
    content, meta = _load_report_planab()
    return content, meta.get("ts")


def _load_brief_cache():
    """优先加载三端共用的权威日报（Plan A/B 统一出口）；旧缓存仅用于尚未迁移的环境。"""
    authoritative, authoritative_ts = _load_authoritative_brief()
    if _AUTHORITATIVE_REPORT.exists():
        return authoritative, authoritative_ts
    try:
        if _BRIEF_CACHE_FILE.exists():
            data = json.loads(_BRIEF_CACHE_FILE.read_text(encoding="utf-8"))
            age = time.time() - data.get("timestamp", 0)
            if age < _BRIEF_CACHE_TTL:
                return data.get("content"), data.get("timestamp")
    except Exception:
        pass
    return None, None


def _save_brief_cache(content: str):
    """将简报内容保存到文件缓存，同时追加历史推荐记录"""
    try:
        _BRIEF_CACHE_DIR.mkdir(exist_ok=True)
        _BRIEF_CACHE_FILE.write_text(
            json.dumps({"content": content, "timestamp": time.time()}, ensure_ascii=False),
            encoding="utf-8",
        )
        # 追加今日推荐记录到历史（用于跨日去重）
        _append_brief_history(content)
    except Exception as _e:
        logging.warning(f"简报缓存写入失败: {_e}")


# ── AI报告通用文件缓存（市场分析 / 个股分析 共用）──────────────────────────
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
    Path.home() / "Desktop" / "ai-daily-report-v2" / "data" / "daily_report.md",  # Mac 本地优先
    Path("/root/ai-daily-report-v2/data/daily_report.md"),  # VPS 备选
]
if os.environ.get("AI_DAILY_REPORT_PATH"):
    _AI_DAILY_REPORT_PATHS.insert(0, Path(os.environ["AI_DAILY_REPORT_PATH"]))


# 日报最大可用天数：超过该天数视为过期。
# 与 Action Gate「触发时效 ≤ 72h」对齐，过期新闻无法支撑任何「触发」字段，
# 注入只会与【校验时间=今日】产生硬矛盾，导致 DeepSeek 拒绝出报。
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


def _audit_professional_brief(content: str, has_real_news_report: bool = False) -> list:
    """
    本地质检 AI 市场简报，拦截最常见的“看似完整但不可核验”问题。
    返回问题列表；空列表表示通过基础结构与证据检查。
    """
    issues = []
    if not content or len(content.strip()) < 800:
        return ["正文过短，无法构成完整日报"]

    required_sections = [
        "## 核心结论",
        "## 事实台账",
        "## 市场驱动",
        "## 市场格局",
        "## 催化事件板",
        "## 可执行推荐",
        "## 数据/时间戳",
    ]
    for section in required_sections:
        if section not in content:
            issues.append(f"缺少章节：{section}")

    if "日报未通过质检" in content:
        issues.append("模型自检未通过")

    rec_start = content.find("## 可执行推荐")
    rec_text = content[rec_start:] if rec_start >= 0 else content
    markets = [
        ("美股", "🇺🇸"),
        ("港股", "🇭🇰"),
        ("A股", "🇨🇳"),
    ]
    for market_name, flag in markets:
        mkt = re.search(rf"###\s*{re.escape(flag)}.*?(?=\n###\s*[🇺🇸🇭🇰🇨🇳]|\n##\s|\Z)", rec_text, re.S)
        if not mkt:
            issues.append(f"可执行推荐缺少{market_name}分区")
            continue
        block = mkt.group(0)
        cards = re.findall(r"^\d+\.\s+\*\*", block, re.M)
        if len(cards) > 3:
            issues.append(f"{market_name}推荐数量最多3只，当前为{len(cards)}只")
        for field in ["触发", "来源", "机会概率/风险概率", "建仓区间", "仓位上限", "R/R", "失效条件", "数据时间戳"]:
            if field not in block:
                issues.append(f"{market_name}推荐缺字段：{field}")

    source_mentions = len(re.findall(r"(来源|Tier|Reuters|Bloomberg|WSJ|FT|财新|公告|交易所|SEC|统计局|央行)", content))
    if source_mentions < 8:
        issues.append("来源与事实锚点不足，真实性不可核验")

    if not has_real_news_report and "立即建仓" in rec_text:
        issues.append("未注入新鲜真实新闻报告时仍输出“立即建仓”，应降级为中期跟进或观察")

    if re.search(r"(传闻|市场消息称|据说|可能有重大利好)", content) and "Tier C" not in content:
        issues.append("疑似使用传闻但未标注 Tier C 或降级处理")

    return issues

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

# ── 全局 Tushare 熔断器 ───────────────────────────────────────────────────
# Tushare token 失效/无权限时，整个会话直接跳过，A股 走 yfinance(.SS/.SZ)。
_TS_DISABLED = False

def _ts_blocked() -> bool:
    return _TS_DISABLED

def _ts_mark_dead(reason: str = ""):
    global _TS_DISABLED
    if not _TS_DISABLED:
        _TS_DISABLED = True
        logging.warning(f"🚫 Tushare 不可用（{reason}），本会话跳过 Tushare，A股直接走 yfinance")

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

# 【DeepSeek迁移】
try:
    from openai import OpenAI as _OpenAI
    HAS_GEMINI = True
    try:
        import google.generativeai as genai
    except ImportError:
        genai = None
except ImportError:
    HAS_GEMINI = False
    _OpenAI = None
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
    if c.endswith((".SS", ".SZ")) or (len(c) >= 6 and c[:6].isdigit()):
        return "🇨🇳A股"
    return "🇺🇸美股"

@st.cache_data(ttl=3600, show_spinner=False)
def _ath_pct(symbol: str):
    """距历史最高点：(水位%, 高点日期, 距今天数, 最新收盘价)。全量历史真ATH（上证含2007年顶），1小时缓存"""
    try:
        import yfinance as _yfa
        h = _yfa.Ticker(symbol).history(period="max")["Close"].dropna()
        if len(h) < 100:
            return None
        pct = (float(h.iloc[-1]) / float(h.max()) - 1) * 100
        d_ath = h.idxmax()
        days = (h.index[-1] - d_ath).days
        return (pct, str(d_ath)[:10], int(days), float(h.iloc[-1]))
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
    _dur = f"{days/365:.1f}年" if days >= 365 else f"{days}天"
    return f" | 距历史最高 {pct:+.1f}%（{d} 创下·已 {_dur}）"


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
        return _raw, f"距历史最高{float(_pct):+.1f}%｜高点相隔{int(_days)}天"

    with _AthPool(max_workers=min(8, max(1, len(codes)))) as _ex:
        return dict(_ex.map(_one, codes))


def _scan_cache_key(scan_type: str, scan_market: str, risk_pref: str = None) -> str:
    """生成扫描缓存文件键"""
    key = f"{scan_type}_{scan_market}"
    if scan_type == 'regime' and risk_pref:
        key += f"_{risk_pref}"
    return key.replace(" ", "_")

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
                if time.time() - float(marker.read_text().strip() or 0) < 600:
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

_DIRECT_SESSION = requests.Session()
_DIRECT_SESSION.trust_env = False
_DIRECT_SESSION.verify = False

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
        _safe_print(f"[AlphaVantage] ❌ {symbol}: {e}")
        return None

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
        _safe_print(f"[AlphaVantage] ❌ {symbol}: {e}")
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
        cached_value, is_stale = self.cache_mgr.get(cache_key, data_type, force_refresh)
        if cached_value is not None and not is_stale:
            elapsed = (time.time() - start_time) * 1000
            self.perf.record('fetch', elapsed)
            return cached_value
        
        # 1b. Rate limit 冷却期：直接返回过期缓存（有总比没有好）
        if cached_value is not None and is_stale and _yf_is_rate_limited():
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

        # 2a. A股：优先 Tushare（带熔断：token 失效后本会话跳过，直接走 yfinance）
        if (symbol.endswith(".SS") or symbol.endswith(".SZ")) and not _ts_blocked():
            try:
                from ts_helper import fetch_df as _ts_fetch
                _ts_df = _ts_fetch(symbol, period=period)
                if _ts_df is not None and len(_ts_df) >= min_rows:
                    self.cache_mgr.set(cache_key, _ts_df, data_type)
                    elapsed = (time.time() - start_time) * 1000
                    self.perf.record('fetch', elapsed)
                    self.logger.info(f"✅ Tushare 获取 {symbol}，共 {len(_ts_df)} 条记录")
                    return _ts_df
            except Exception as _e:
                _msg = str(_e)
                if any(k in _msg for k in ('token', 'Token', '权限', '积分', '不对')):
                    _ts_mark_dead(_msg[:50])
                self.logger.debug(f"Tushare {symbol} 失败，降级 yfinance: {_e}")

        # 2b. 尝试从 yfinance 获取（带重试 + rate limit 熔断）
        def _make_yf_session(attempt_idx: int):
            try:
                import curl_cffi.requests as _cffi
                _impersonates = ["chrome110", "chrome120", "safari17_0"]
                return _cffi.Session(impersonate=_impersonates[attempt_idx % len(_impersonates)])
            except ImportError:
                return None

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
        
        # 2c. A股：yfinance 失败时尝试东方财富备用（Tushare 需 token，Cloud 环境常失败）
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
    # call_gemini_api函数在后面定义，这里先设为None，后续再绑定
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

st.set_page_config(layout="wide", page_title="AI 皇冠双核", page_icon="👑", initial_sidebar_state="collapsed")

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
[data-testid="stMarkdownContainer"] p,
[data-testid="stMarkdownContainer"] li,
[data-testid="stMarkdownContainer"] span,
[data-testid="stText"] { font-size: 14px !important; line-height: 1.6 !important; color: #1a1a2e; }
[data-testid="stCaptionContainer"] { font-size: 12px !important; color: #5a6378 !important; }
h1, [data-testid="stHeading"] h1 { font-size: 22px !important; font-weight: 700 !important; color: #1a1a2e !important; }
h2, [data-testid="stHeading"] h2 { font-size: 18px !important; font-weight: 700 !important; color: #1e3a5f !important; }
h3, [data-testid="stHeading"] h3 { font-size: 16px !important; font-weight: 600 !important; color: #2c4a6e !important; }

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

/* caption / 辅助文字：从灰色升级为蓝灰 */
.stCaption, [data-testid="stCaptionContainer"] span { color: #5a6378 !important; }
</style>
""", unsafe_allow_html=True)

# 【V100·统一运行反馈】未知时长任务使用浏览器端持续动画，Python阻塞时动画仍会运动，
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
    MY_DEEPSEEK_KEY = (st.secrets.get("DEEPSEEK_API_KEY","") if hasattr(st,"secrets") else "") or os.getenv("DEEPSEEK_API_KEY","")
    # V88 默认且唯一的 AI 服务为 DeepSeek；Gemini 仅保留旧变量兼容，不参与调用。
    MY_GEMINI_KEY_RAW = ""
    if MY_DEEPSEEK_KEY:
        MY_GEMINI_KEY = MY_DEEPSEEK_KEY
        AI_PROVIDER = "deepseek"
    else:
        MY_GEMINI_KEY = ""
        AI_PROVIDER = "none"
    GEMINI_MODEL_NAME = "gemini-2.5-flash"
    DEEPSEEK_MODEL_NAME = "deepseek-v4-flash"
    if MY_DEEPSEEK_KEY and _OpenAI:
        try:
            import httpx as _httpx
            _ds_http_client = _httpx.Client(
                proxy="http://127.0.0.1:7897",
                timeout=120.0
            )
            _deepseek_client = _OpenAI(
                api_key=MY_DEEPSEEK_KEY,
                base_url="https://api.deepseek.com/v1",
                http_client=_ds_http_client
            )
        except Exception:
            _deepseek_client = _OpenAI(api_key=MY_DEEPSEEK_KEY, base_url="https://api.deepseek.com/v1")
        logging.info("✅ DeepSeek API配置完成: deepseek-v4-flash")
    else:
        _deepseek_client = None
except Exception as e:
    MY_GEMINI_KEY = ""; MY_DEEPSEEK_KEY = ""; GEMINI_MODEL_NAME = "gemini-2.5-flash"; DEEPSEEK_MODEL_NAME = "deepseek-v4-flash"
    AI_PROVIDER = "none"; _deepseek_client = None
    logging.error(f"⚠️ AI API配置失败: {e}")

# 【V91.9】AI分析统一模型说明：所有spinner和报告统一使用
def _ai_model_label(model=None):
    """返回模型显示名称，用于 spinner 和报告底部"""
    if AI_PROVIDER == "deepseek":
        return "DeepSeek V4 Flash"
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
def render_cloud_search():
    """东方财富API全量搜索 - 渲染在主内容区（深度作战室顶部）"""
    st.markdown("""
    <div style="padding: 0.4rem 0 0.2rem 0; margin-bottom: 0.5rem; border-left: 3px solid #00d4aa; padding-left: 0.8rem;">
        <span style="font-size: 13px; font-weight: 700; color: #00d4aa;">🔍 个股搜索</span>
        <span style="font-size: 11px; color: #888; margin-left: 0.6rem;">美股 / 港股 / A股</span>
    </div>
    """, unsafe_allow_html=True)
    col_search, col_filter = st.columns([3, 1])
    with col_search:
        search_input = st.text_input(
            "输入股票名字或代码（全量云端搜索）",
            placeholder="例如：宁波 / 紫金 / 腾讯 / AAPL / NVDA",
            key="stock_search_input",
            label_visibility="collapsed"
        )
    with col_filter:
        search_market_filter = st.selectbox(
            "市场筛选",
            ["全部", "🇺🇸 美股", "🇭🇰 港股", "🇨🇳 A股"],
            key="search_market_filter",
            help="筛选搜索结果为指定市场"
        )
    
    if search_input:
        search_key = search_input.strip()
        
        if search_key:
            _search_t0 = time.time()
            _search_prog = st.progress(0)
            _search_status = st.empty()
            _search_status.text("🔍 请求东方财富API... (0%)")
            all_matches = []
            try:
                search_url = f"https://searchapi.eastmoney.com/api/suggest/get"
                params = {
                    "input": search_key,
                    "type": "14",
                    "token": "D43BF722C8E33BDC906FB84D85E326E8",
                    "count": 50
                }
                response = _DIRECT_SESSION.get(search_url, params=params, timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    if data and 'QuotationCodeTable' in data and 'Data' in data['QuotationCodeTable']:
                        results = data['QuotationCodeTable']['Data']
                        for item in results:
                            code_raw = item.get('Code', '')
                            name = item.get('Name', '')
                            market_code = item.get('MktNum', '')
                            yf_code = None
                            if market_code == '1':
                                yf_code = f"{code_raw}.SS"
                            elif market_code == '0':
                                yf_code = f"{code_raw}.SZ"
                            elif market_code == '116':
                                yf_code = f"{code_raw.zfill(5)}.HK"
                            elif market_code == '155':
                                yf_code = code_raw
                            else:
                                yf_code = code_raw
                            if yf_code and name:
                                all_matches.append((yf_code, name))
                        _safe_print(f"[东方财富API] 搜索 '{search_key}' 找到 {len(all_matches)} 个结果")
                        _search_prog.progress(0.5)
                        _search_status.text(f"✅ API返回 {len(all_matches)} 个结果 (50%)")

                if len(all_matches) == 0:
                    _search_prog.progress(0.3)
                    _search_status.text("🔍 API失败，降级到本地索引...")
                    _safe_print("[东方财富API] 失败，降级到本地索引")
                    _idx_total = len(STOCK_NAME_INDEX)
                    for _idx, (code, name) in enumerate(STOCK_NAME_INDEX.items()):
                        if _idx_total > 0 and _idx % 50 == 0:
                            _search_prog.progress(0.3 + 0.6 * (_idx / _idx_total))
                            _search_status.text(f"🔍 本地索引搜索... {_idx}/{_idx_total} ({100*_idx/_idx_total:.0f}%)")
                        if (search_key.upper() in code.upper() or search_key in name or search_key.upper() in code.split('.')[0].upper()):
                            all_matches.append((code, name))
                
            except Exception as e:
                _safe_print(f"[东方财富API] 错误: {e}")
                _search_prog.progress(0.2)
                _search_status.text("🔍 API异常，降级到本地索引...")
                _idx_total = len(STOCK_NAME_INDEX)
                for _idx, (code, name) in enumerate(STOCK_NAME_INDEX.items()):
                    if _idx_total > 0 and _idx % 50 == 0:
                        _search_prog.progress(0.2 + 0.7 * (_idx / _idx_total))
                        _search_status.text(f"🔍 本地索引... {_idx}/{_idx_total} ({100*_idx/_idx_total:.0f}%)")
                    if (search_key.upper() in code.upper() or search_key in name or search_key.upper() in code.split('.')[0].upper()):
                        all_matches.append((code, name))

            _search_prog.progress(1.0)
            _search_status.text(f"✅ 搜索完成，共 {len(all_matches)} 个结果 · 用时 {time.time()-_search_t0:.1f}s")
            time.sleep(0.3)
            _search_prog.empty()
            _search_status.empty()

            if len(all_matches) > 0:
                us_stocks = [(c, n) for c, n in all_matches if "." not in c]
                hk_stocks = [(c, n) for c, n in all_matches if ".HK" in c]
                cn_stocks = [(c, n) for c, n in all_matches if ".SS" in c or ".SZ" in c]
                # 【V93】按市场筛选
                if search_market_filter == "🇺🇸 美股":
                    us_stocks, hk_stocks, cn_stocks = us_stocks, [], []
                elif search_market_filter == "🇭🇰 港股":
                    us_stocks, hk_stocks, cn_stocks = [], hk_stocks, []
                elif search_market_filter == "🇨🇳 A股":
                    us_stocks, hk_stocks, cn_stocks = [], [], cn_stocks
                filtered_count = len(us_stocks) + len(hk_stocks) + len(cn_stocks)
                if filtered_count == 0:
                    st.warning(f"该市场下无匹配结果，请尝试「全部」或切换其他市场")
                else:
                    st.success(f"✅ 找到 {filtered_count} 个结果" + (f"（已筛选 {search_market_filter}）" if search_market_filter != "全部" else ""))
                
                options = ["请选择要分析的股票..."]
                code_map = {}
                
                if us_stocks:
                    options.append("─────── 🇺🇸 美股 ───────")
                    for code, name in us_stocks:
                        option_text = f"🇺🇸 {name} ({code})"
                        options.append(option_text)
                        code_map[option_text] = (code, name)
                
                if hk_stocks:
                    options.append("─────── 🇭🇰 港股 ───────")
                    for code, name in hk_stocks:
                        option_text = f"🇭🇰 {name} ({code})"
                        options.append(option_text)
                        code_map[option_text] = (code, name)
                
                if cn_stocks:
                    options.append("─────── 🇨🇳 A股 ───────")
                    for code, name in cn_stocks:
                        option_text = f"🇨🇳 {name} ({code})"
                        options.append(option_text)
                        code_map[option_text] = (code, name)
                
                selected_option = st.selectbox(
                    "② 从结果中选择股票",
                    options=options,
                    key="stock_select_dropdown"
                )
                
                if filtered_count > 0 and selected_option != "请选择要分析的股票..." and selected_option not in ["─────── 🇺🇸 美股 ───────", "─────── 🇭🇰 港股 ───────", "─────── 🇨🇳 A股 ───────"]:
                    if selected_option in code_map:
                        code, name = code_map[selected_option]
                        
                        if (code, name) not in st.session_state.search_history:
                            st.session_state.search_history.insert(0, (code, name))
                            if len(st.session_state.search_history) > 10:
                                st.session_state.search_history = st.session_state.search_history[:10]
                        
                        _prev_code = st.session_state.get('scan_selected_code')
                        if _prev_code != code:
                            st.session_state.scan_selected_code = code
                            st.session_state.scan_selected_name = name
                            st.session_state.pk_codes = []
                            st.session_state.pk_names = []
                            _search_history_persist(code, name)  # 【V88·搜索习惯】
                            # 【V96.1】搜索过的个股自动加入自选股（上限20只，先进先出）
                            if _watchlist_add(code, name):
                                st.session_state["_wl_new_pick"] = (code, name)  # rerun后弹窗选A/B/C
                                st.toast(f"✅ 已选中 {name}，并加入自选股", icon="🎯")
                            else:
                                st.toast(f"✅ 已选中 {name}，正在分析...", icon="🎯")
                            st.rerun()
                        
                        is_in_basket = (code, name) in st.session_state.compare_basket
                        if is_in_basket:
                            st.button("✅ 已在对比篮", key="search_compare", disabled=True, width='stretch')
                        else:
                            if st.button("➕ 加入对比篮", key="search_compare", width='stretch'):
                                st.session_state.compare_basket.append((code, name))
                                st.toast(f"✅ 已加入对比篮: {name}", icon="➕")
                                st.rerun()
            else:
                st.warning("❌ 未找到匹配的股票")
                st.caption("💡 搜索提示：")
                st.caption("• 关键字：宁波、紫金、腾讯")
                st.caption("• 代码：AAPL、02899、600519")
    
    if len(st.session_state.search_history) > 0:
        st.markdown('<p style="font-size: 12px; font-weight: 600; margin-top: 1rem; margin-bottom: 0.3rem;">📜 搜索历史</p>', unsafe_allow_html=True)
        st.caption(f"最近搜索 {len(st.session_state.search_history)} 只")
        
        for i, (code, name) in enumerate(st.session_state.search_history):
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                st.markdown(f"**{name}**")
                st.caption(code)
            
            with col2:
                if st.button("🔍", key=f"hist_analyze_{i}", help="分析", width='stretch'):
                    st.session_state.scan_selected_code = code
                    st.session_state.scan_selected_name = name
                    st.session_state.pk_codes = []
                    st.session_state.pk_names = []
                    st.rerun()
            
            with col3:
                is_in_basket = (code, name) in st.session_state.compare_basket
                if is_in_basket:
                    st.button("✅", key=f"hist_compare_{i}", disabled=True, width='stretch')
                else:
                    if st.button("➕", key=f"hist_compare_{i}", help="加入对比", width='stretch'):
                        st.session_state.compare_basket.append((code, name))
                        st.toast(f"✅ 已加入对比篮: {name}", icon="➕")
                        st.rerun()
        
        if st.button("🗑️ 清空历史", key="search_clear_history", width='stretch'):
            st.session_state.search_history = []
            st.rerun()


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
    
    df_display = df_results.copy()
    if "得分" in df_display.columns:
        df_display["得分"] = pd.to_numeric(df_display["得分"], errors="coerce").fillna(0).astype(int)
    
    st.markdown("##### 📊 扫描结果")
    st.caption("💡 快捷入口选股点击「深度分析」| 勾选1只=深度分析 | 勾选2只以上=立即对比")
    
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
                lambda r: f"?q={str(r['代码']).strip()}&stk={str(r[_name_col]).strip()}", axis=1)
            _link_cfg[_name_col] = st.column_config.LinkColumn(
                _name_col, display_text=r"stk=(.+)$", help="点击=深度分析并加入重点观察")
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
                        st.session_state.compare_basket.append((code, name))
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
    to_yf_cn_code = mod_utils.to_yf_cn_code
    batch_scan_analysis_concurrent = mod_analysis.batch_scan_analysis_concurrent
    logging.info("✅ 模块别名映射完成（3项）")

# 无论哪种模式，确保舆情分析器绑定了AI调用函数
try:
    if SENTIMENT_ANALYZER_AVAILABLE and _sentiment_analyzer:
        _sentiment_analyzer.call_ai = call_gemini_api
        logging.info("✅ 舆情分析器已绑定 call_gemini_api")
except NameError:
    logging.warning("⚠️ call_gemini_api 尚未定义，稍后绑定")

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
    _repo = Path.home() / "Desktop" / "ai-daily-report-v2"
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
        lines = [f"[{mname}] 温度{t.get('temp', '?')}/100 {t.get('label', '')}·建议仓位{t.get('position', '?')}"]
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
    新版=本地快照+真实新闻拼上下文 → 一次 DeepSeek 调用（速度≈提升3倍+，热点有真实依据）。
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
    _llm = globals().get("call_gemini_api")
    text = ""
    try:
        if callable(_llm):
            text = _llm(prompt) or ""
        elif MY_DEEPSEEK_KEY:
            import requests as _rq99
            _resp99 = _rq99.post(
                "https://api.deepseek.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {MY_DEEPSEEK_KEY}"},
                json={"model": DEEPSEEK_MODEL_NAME,
                      "messages": [{"role": "user", "content": prompt}],
                      "max_tokens": 1200, "temperature": 0.4},
                timeout=90)
            text = _resp99.json()["choices"][0]["message"]["content"]
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
    if not (MY_GEMINI_KEY or MY_DEEPSEEK_KEY) or not _market_ai_auto_due():
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


@st.fragment
def _render_ai_market_analysis():
    from datetime import datetime as _dt_ai
    # 【V99.8】不再要求先加载宏观脉搏：分析上下文全部来自本地快照+新闻日报文件
    _all = st.session_state.get('all_markets', {})
    us_result = _all.get('us_market', {'data_ok': False, 'verdict': 'Unknown', 'reason': ''})
    hk_result = _all.get('hk_market', {'data_ok': False, 'verdict': 'Unknown', 'reason': ''})
    cn_result = _all.get('cn_market', {'data_ok': False, 'verdict': 'Unknown', 'reason': ''})

    _has_any_ai = bool(MY_GEMINI_KEY or MY_DEEPSEEK_KEY)
    if not _has_any_ai:
        return

    st.caption(f"复用权威AI日报的行情快照与真实新闻 · {_dt_ai.now().strftime('%Y-%m-%d')}")

    _has_cached = any(k in st.session_state for k in ['market_ai_us', 'market_ai_hk', 'market_ai_cn'])
    if _has_cached:
        _cached_ts = None
        for mk in ['us', 'hk', 'cn']:
            _, _t = _load_ai_report_cache(f"market_{mk}")
            if _t:
                _cached_ts = _t
                break
        _ts_str = _dt_ai.fromtimestamp(_cached_ts).strftime('%H:%M') if _cached_ts else ""
        st.caption(f"走势研判+真实新闻热点+3-5日预判 · 单次调用三市场 · 缓存自动加载{f' · 生成于 {_ts_str}' if _ts_str else ''}")
    else:
        st.caption("走势研判+真实新闻热点+3-5日预判 · 单次调用三市场 · 快照与新闻均为1小时内真实数据")

    _btn_cols = st.columns([3, 1])
    with _btn_cols[0]:
        _do_gen = st.button("⚡ 一键分析全市场（美股＋港股＋A股）", key="btn_one_click_all_markets",
                     type="primary", use_container_width=True)
    with _btn_cols[1]:
        _do_refresh = st.button("🔄 强制刷新", key="btn_refresh_market_ai", use_container_width=True)

    if _do_refresh:
        for _k in ['market_ai_us', '_us_tech_data', 'market_sentiment_us',
                    'market_ai_hk', '_hk_tech_data', 'market_sentiment_hk',
                    'market_ai_cn', '_cn_tech_data', 'market_sentiment_cn',
                    '_market_ai_auto_done']:
            st.session_state.pop(_k, None)
        for mk in ['us', 'hk', 'cn']:
            try:
                _rf = _AI_REPORT_CACHE_DIR / f"ai_report_market_{mk}.json"
                if _rf.exists():
                    _rf.unlink()
            except Exception:
                pass
        _do_gen = True

    _trigger_all = _do_gen

    _markets_config = [
        ('美股', '^GSPC', us_result, 'market_ai_us', '_us_tech_data', 'market_sentiment_us', 'us'),
        ('港股', '^HSI', hk_result, 'market_ai_hk', '_hk_tech_data', 'market_sentiment_hk', 'hk'),
        ('A股', '000001.SS', cn_result, 'market_ai_cn', '_cn_tech_data', 'market_sentiment_cn', 'cn'),
    ]

    if _trigger_all:
        # 【V99.8】单次 LLM 调用产出三市场（原三次串行调用），上下文=本地快照+真实新闻
        _need99 = _do_refresh or any(
            _sp not in st.session_state for _, _, _, _sp, _, _, _ in _markets_config)
        if _need99:
            _t099 = time.time()
            with _v88_running("AI分析三市场 · 快照与真实新闻"):
                _res_all = _run_all_markets_ai()
            _ok99 = 0
            for _mname, _mcode, _mresult, _ss_pred, _ss_tech, _ss_sent, _mk in _markets_config:
                _r = _res_all.get(_mk) or {}
                if _r.get('pred'):
                    st.session_state[_ss_pred] = _r['pred']
                    _save_ai_report_cache(f"market_{_mk}", _r)
                    _ok99 += 1
                if _r.get('tech'):
                    st.session_state[_ss_tech] = _r['tech']
            if _ok99:
                st.session_state['_market_ai_auto_done'] = True
                st.success(f"✅ 全市场AI分析完成（{time.time() - _t099:.0f}秒 · 单次调用 · 真实新闻锚定）")
            else:
                st.error("❌ 分析失败：LLM 不可用或行情/日报数据缺失，请稍后重试")
        else:
            st.session_state['_market_ai_auto_done'] = True
            st.success("✅ 已有本次分析结果（点「🔄 强制刷新」重新生成）")

    ai_tabs = st.tabs(["🇺🇸 美股", "🇭🇰 港股", "🇨🇳 A股"])

    for _tab, (_mname, _mcode, _mresult, _ss_pred, _ss_tech, _ss_sent, _mk) in zip(ai_tabs, _markets_config):
        with _tab:
            if _ss_pred in st.session_state:
                _tech_d = st.session_state.get(_ss_tech, {})
                if _tech_d:
                    _tc1, _tc2, _tc3 = st.columns(3)
                    with _tc1:
                        st.metric("当前价格", f"{_tech_d.get('current_price', 0):.2f}")
                    with _tc2:
                        st.metric("技术趋势", _tech_d.get('trend', '震荡'))
                    with _tc3:
                        st.metric("技术强度", f"{_tech_d.get('strength', 50)}/100")
                if COPY_UTILS_AVAILABLE:
                    CopyUtils.create_copy_button(st.session_state[_ss_pred], button_text="📋 复制全文", key=f"copy_{_mk}_pred_full")
                    CopyUtils.render_markdown_with_section_copy(st.session_state[_ss_pred], key_prefix=f"{_mk}_pred")
                else:
                    st.markdown(st.session_state[_ss_pred])
                st.caption(f"📌 AI生成 · 模型: {_ai_model_label()}")
            else:
                st.caption("点击「一键分析全市场」或等待自动生成")

            if _ss_sent in st.session_state:
                _sent_d = st.session_state[_ss_sent]
                _sent_m = _sent_d.get('metrics', {})
                with st.expander(f"📰 舆情 | 评分 {_sent_m.get('sentiment_score', 50)}/100 · {_sent_m.get('sentiment_level', '中性')}", expanded=False):
                    if COPY_UTILS_AVAILABLE:
                        CopyUtils.create_copy_button(_sent_d['response'], button_text="📋 复制全文", key=f"copy_{_mk}_sent_full")
                        CopyUtils.render_markdown_with_section_copy(_sent_d['response'], key_prefix=f"{_mk}_sent")
                    else:
                        st.markdown(_sent_d.get('response', ''))
                    st.caption(f"📌 AI生成 · 模型: {_ai_model_label()}")

    _link_items = []
    for _mk, _mr in [('🇺🇸 美股', us_result), ('🇭🇰 港股', hk_result), ('🇨🇳 A股', cn_result)]:
        if _mr.get('data_ok'):
            _link_items.append(f"{_mk}：{_mr.get('verdict', 'Unknown')}")
    if _link_items and ('market_ai_us' in st.session_state or 'market_ai_hk' in st.session_state or 'market_ai_cn' in st.session_state):
        st.caption("🌐 体制 · " + " | ".join(_link_items))
        _strong = [n for n, d in [('美股', us_result), ('港股', hk_result), ('A股', cn_result)] if d.get('data_ok') and d.get('verdict') == 'Risk On']
        _weak = [n for n, d in [('美股', us_result), ('港股', hk_result), ('A股', cn_result)] if d.get('data_ok') and d.get('verdict') == 'Risk Off']
        if _strong:
            st.success(f"✅ 风险偏好：{', '.join(_strong)}")
        if _weak:
            st.warning(f"⚠️ 避险模式：{', '.join(_weak)}")


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
                f'<div style="font-size:11px;color:#374151;margin-top:4px;">{_in_txt}</div></div>',
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
                f'<div style="font-size:11px;color:#374151;margin-top:4px;">{_out_txt}</div></div>',
                unsafe_allow_html=True,
            )
        with _sum_c3:
            if "_forum_avg" in _rank_df.columns:
                _hot = _rank_df.nlargest(1, "_forum_avg").iloc[0]
                st.markdown(
                    f'<div class="heat-summary-card" style="border-left:4px solid #8b5cf6;">'
                    f'<div style="font-weight:700;color:#8b5cf6;font-size:12px;">💬 论坛最热</div>'
                    f'<div style="font-size:11px;color:#374151;margin-top:4px;">'
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
        from ts_helper import fetch_daily_tushare as _ts_daily, is_cn as _is_cn
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
    from datetime import datetime as _dt_global
    from zoneinfo import ZoneInfo
    _global_today = _dt_global.now().strftime("%Y-%m-%d")
    _global_weekday_cn = {"Monday": "周一", "Tuesday": "周二", "Wednesday": "周三", "Thursday": "周四", "Friday": "周五", "Saturday": "周六", "Sunday": "周日"}
    _global_weekday = _global_weekday_cn.get(_dt_global.now().strftime("%A"), "")
    _bj_time = _dt_global.now(ZoneInfo("Asia/Shanghai")).strftime("%H:%M")
    _nasdaq_time = _dt_global.now(ZoneInfo("America/New_York")).strftime("%m/%d %H:%M")
    st.markdown(
        f'<div style="display:flex;justify-content:space-between;align-items:center;padding:.15rem .45rem;'
        f'margin:0 0 .25rem;border-bottom:1px solid #e2e8f0;font-size:10px">'
        f'<b style="color:#334155;font-size:10px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">🌍 全球市场概览 · 实时监控三大市场体制 · 把握全球资金流向</b>'
        f'<span style="color:#1e3a5f;font-weight:600">{_global_today} {_global_weekday} · 纽约{_nasdaq_time} · 北京{_bj_time}</span></div>',
        unsafe_allow_html=True)

    try:
        # 检查是否请求强制刷新
        force_refresh = st.session_state.get('force_refresh_requested', False)
        if force_refresh:
            st.session_state['force_refresh_requested'] = False
            st.cache_data.clear()
            try:
                local_cache.clear_all()   # 穿透文件缓存层，彻底刷新
            except Exception:
                pass

        # 启动性能监控
        _perf_monitor.start()

        _cache_ts = int(time.time() // 300)  # 每5分钟变一次，触发缓存刷新
        # 首页只保留宏观脉搏。行业热力首页模块已移除，底层函数仍保留供其他功能复用。
        _overview_mode = "macro"
        st.checkbox(
            "首屏自动请求AI（人工智能）市场分析",
            key="v88_auto_ai_market",
            help="仅美股、港股或A股交易日盘中运行；每3小时一次，每天最多3次；一次请求同时分析三市场。",
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
        # 【V100·紧凑首页】三市场关键指标一屏展示；完整明细默认折叠。
        def _macro_ai_text(_text, _market):
            _txt = str(_text or "").replace(f"【{_market}】", "").strip()
            _txt = _txt.split("\n\n---", 1)[0].strip()
            _txt = _txt.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            _txt = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", _txt)
            return _macro_cn(_txt).replace("\n", "<br>")

        def _macro_card(_title, _verdict, _items, _reason, _ai_text="", _market=""):
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
                f'<div class="v88-macro-reason">{_macro_cn(_reason)}</div>'
                f'<div class="v88-macro-ai"><b>🤖 AI（人工智能）增强</b><br>{_ai_html}</div></div>',
                unsafe_allow_html=True)

        st.markdown("""<style>
        .v88-macro-title{display:flex;align-items:center;justify-content:space-between;margin:.15rem 0 .45rem}
        .v88-macro-title b{font-size:15px;color:#1e3a5f}.v88-macro-title span{font-size:11px;color:#5a6378}
        .v88-macro-card{background:#fff;border:1px solid #dce3ed;border-radius:10px;padding:.4rem .55rem;height:174px;min-height:174px;overflow:hidden;box-shadow:0 1px 3px rgba(30,58,95,.06)}
        .v88-macro-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:.4rem}
        .v88-macro-head b{font-size:13px;color:#1a1a2e}.v88-macro-head em{font-style:normal;font-weight:700;font-size:11px}
        .v88-macro-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:.35rem}
        .v88-macro-kpi{min-width:0}.v88-macro-kpi>span{display:block;color:#5a6378;font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
        .v88-macro-kpi b{display:block;font-size:14px;line-height:1.2;color:#1a1a2e;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
        .v88-macro-kpi small{font-size:10px}.v88-up{color:#dc2626!important}.v88-down{color:#16a34a!important}.v88-flat{color:#5a6378!important}
        .v88-cn-note{display:inline!important;font-size:.58em!important;line-height:1!important;color:#8893a7;font-weight:400;margin-left:1px}.v88-macro-reason{font-size:10px;color:#5a6378;margin-top:.35rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
        .v88-macro-dense{height:174px;min-height:174px;padding:.38rem .52rem}
        .v88-macro-dense .v88-macro-head{margin-bottom:.2rem}
        .v88-macro-dense .v88-macro-grid{grid-template-columns:repeat(3,minmax(0,1fr));gap:.18rem .3rem}
        .v88-macro-dense .v88-macro-kpi>span{font-size:9px}
        .v88-macro-dense .v88-macro-kpi b{font-size:13px;line-height:1.08}
        .v88-macro-dense .v88-macro-kpi small{display:block;font-size:9px;line-height:1.1;white-space:nowrap}
        .v88-macro-dense .v88-cn-note{font-size:.52em!important}
        .v88-macro-dense .v88-macro-reason{font-size:9px;margin-top:.18rem}
        .v88-macro-ai{font-size:10px;line-height:1.35;color:#3d4f6a;border-top:1px dashed #dce3ed;margin-top:.28rem;padding-top:.25rem;max-height:34px;overflow:hidden}
        .v88-macro-ai-active{height:232px;min-height:232px}
        .v88-macro-ai-active .v88-macro-ai{max-height:82px;overflow:auto}
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
                        f'<span>AI解读 {_macro_ai_time_text} · 今日{_macro_ai_state.get("runs", 0)}/3次 · 盘中每3h ｜ 最近收盘 {_dt_global.now().strftime("%m/%d %H:%M")}</span></div>',
                        unsafe_allow_html=True)
        with _mc_b1:
            _macro_ai_generate = st.button("⚡ AI解读", key="btn_macro_ai_generate", help="更新AI增强解读", use_container_width=True)
        with _mc_b2:
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
            _duration = f"{_days / 365:.1f}年前" if _days >= 365 else f"{_days}天前"
            return f"{float(_pct):+.1f}%", f"高点{_duration}"

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
            ], str(us_result.get("reason", ""))[:44], _macro_ai_us, "美股")
        with _mc2:
            _cyb9 = float(cn_result.get("cyb_price") or 0)
            _cyb_txt9 = f"{_cyb9:.3f}元" if cn_result.get("cyb_use_etf") else f"{_cyb9:.0f}点"
            _macro_card("🇨🇳 A股", cn_result.get("verdict", "—"), [
                ("上证", f"{float(cn_result.get('index_level') or 0):.0f}", f"{float(cn_result.get('index_change_pct') or 0):+.1f}%"),
                ("沪深300", f"{float(cn_result.get('hs300_price') or 0):.0f}", f"{float(cn_result.get('hs300_change_pct') or 0):+.1f}%"),
                ("创业板", _cyb_txt9, f"{float(cn_result.get('cyb_change_pct') or 0):+.1f}%"),
                ("波动率", f"{float(cn_result.get('volatility') or 0):.1f}%", "风险温度"),
                ("人民币", f"{float(cn_result.get('cny_price') or 0):.4f}", f"{float(cn_result.get('cny_change_pct') or 0):+.1f}%"),
                ("水位", _water_cn[0], _water_cn[1]),
            ], str(cn_result.get("reason", ""))[:44], _macro_ai_cn, "A股")
        with _mc3:
            _macro_card("🇭🇰 港股", hk_result.get("verdict", "—"), [
                ("恒指", f"{float(hk_result.get('index_level') or 0):.0f}", f"{float(hk_result.get('index_change_pct') or 0):+.1f}%"),
                ("恒生科技", f"{float(hk_result.get('hstech_price') or 0):.2f}", f"{float(hk_result.get('hstech_change_pct') or 0):+.1f}%"),
                ("国企指数", f"{float(hk_result.get('hsce_price') or 0):.0f}", f"{float(hk_result.get('hsce_change_pct') or 0):+.1f}%"),
                ("波动率", f"{float(hk_result.get('volatility') or 0):.1f}%", "风险温度"),
                ("港币", f"{float(hk_result.get('hkd_price') or 0):.4f}", f"{float(hk_result.get('hkd_change_pct') or 0):+.1f}%"),
                ("水位", _water_hk[0], _water_hk[1]),
            ], str(hk_result.get("reason", ""))[:44], _macro_ai_hk, "港股")

        _macro_cross = ""
        for _macro_ai_source in (_macro_ai_us, _macro_ai_hk, _macro_ai_cn):
            _macro_link_match = re.search(r"(?:\*\*)?跨市场联动(?:\*\*)?[：:]\s*([^\n]+)", str(_macro_ai_source or ""))
            if _macro_link_match:
                _macro_cross = _macro_link_match.group(1).strip()
                break
        if _macro_cross:
            _macro_cross_html = _macro_cn(_macro_cross.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))
            st.markdown(f'<div style="font-size:9px;color:#64748b;margin:.18rem .2rem"><b>🔗 跨市场联动</b>：{_macro_cross_html}</div>', unsafe_allow_html=True)

        # 原宏观解读内容不删除：改为紧凑文字直接展示，不再占用大型指标区。
        try:
            _compact_caps = [int(float(x.get('position_cap', 80))) for x in (us_result, cn_result, hk_result)]
            _compact_cap = min(x for x in _compact_caps if 0 <= x <= 100)
        except Exception:
            _compact_cap = 30 if _r_off >= 2 else 80
        st.markdown(
            f'<div style="font-size:11px;color:#3d4f6a;line-height:1.55;margin:.15rem .2rem .35rem">'
            f'<b style="color:#1e3a5f">宏观解读</b>：{_macro_cn(_gr)}　｜　<b style="color:#1e3a5f">全局仓位上限 {_compact_cap}%</b><br>'
            f'美国：{_macro_cn(us_result.get("reason", ""))}　｜　A股：{_macro_cn(cn_result.get("reason", ""))}　｜　'
            f'港股：{_macro_cn(hk_result.get("reason", ""))}</div>', unsafe_allow_html=True)

        if False:  # 旧版完整明细保留在代码中，不再显示下拉框
            # 【V90】宏观脉搏监控 - 三行布局，每行一个市场，指标横向排列
            # ═══════════════════════════════════════════════════════════════
            st.markdown("### 📡 宏观脉搏监控")
            _macro_fetch_bj = _dt_global.now(ZoneInfo("Asia/Shanghai")).strftime("%m/%d %H:%M")
            _macro_fetch_ny = _dt_global.now(ZoneInfo("America/New_York")).strftime("%H:%M ET")
            st.caption(f"💡 机构交易员的「上帝视角」——在看个股之前，先看天气 · 📅 数据截至最近收盘 · 页面加载于 {_macro_fetch_bj} 北京 / {_macro_fetch_ny}")
        
            # 修复 InvalidCharacterError：移除控制字符、null、无效 Unicode，转义 HTML 特殊字符
            def _sanitize_html(s):
                if s is None: return ""
                s = str(s)
                if s.lower() in ("nan", "inf", "-inf", "infinity", "-infinity"):
                    return "N/A"
                s = "".join(c for c in s if ord(c) >= 32 or c in "\n\t\r")
                s = s.replace("\x00", "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
                return s
            def _safe_num(val, fmt="{:.1f}", default="N/A"):
                try:
                    v = float(val)
                    if v != v or v == float("inf") or v == float("-inf"): return default
                    return fmt.format(v)
                except (TypeError, ValueError): return default
            # 英文术语→括号中文（小一号）辅助函数
            def _reason_cn(s):
                s = _sanitize_html(s or "")
                _s = '<span style="font-size: 0.85em;">'
                return (s.replace("SPY在", f'SPY{_s}(标普500ETF)</span>在').replace("SPY(", f'SPY{_s}(标普500ETF)</span>(')
                        .replace("VIX=", f'VIX{_s}(波动率指数)</span>=').replace("VIX(", f'VIX{_s}(波动率指数)</span>(')
                        .replace("MA50(", f'MA50{_s}(50日均线)</span>(').replace("MA200(", f'MA200{_s}(200日均线)</span>('))
            st.markdown("#### 🇺🇸 美国")
            us_cols = st.columns(9)
            with us_cols[0]:
                _v = us_result.get('vix_level', 0)
                _val = _safe_str_for_dom(_safe_num(_v, "{:.1f}") if _v else "N/A")
                _d = _safe_str_for_dom(f"{_safe_num(us_result.get('vix_change_pct', 0), '{:+.1f}')}%" if _v else None) or None
                st.metric("VIX (波动率)", _val, delta=_d, delta_color="inverse")
            with us_cols[1]:
                _v = us_result.get('spy_price', 0)
                _val = _safe_str_for_dom(f"${_safe_num(_v, '{:.1f}')}" if _v and (_v == _v) else "N/A")
                _d = _safe_str_for_dom(f"{_safe_num(us_result.get('spy_change_pct', 0), '{:+.1f}')}%" if _v else None) or None
                st.metric("SPY（标普500 ETF）", _val, delta=_d)
            with us_cols[2]:
                _v = us_result.get('qqq_price', 0)
                _val = _safe_str_for_dom(f"${_safe_num(_v, '{:.1f}')}" if _v else "N/A")
                _d = _safe_str_for_dom(f"{_safe_num(us_result.get('qqq_change_pct', 0), '{:+.1f}')}%" if _v else None) or None
                st.metric("QQQ（纳指100 ETF）", _val, delta=_d)
            with us_cols[3]:
                _v = us_result.get('tlt_price', 0)
                _val = _safe_str_for_dom(f"${_safe_num(_v, '{:.1f}')}" if _v else "N/A")
                _d = _safe_str_for_dom(f"{_safe_num(us_result.get('tlt_change_pct', 0), '{:+.1f}')}%" if _v else None) or None
                st.metric("TLT (美债ETF)", _val, delta=_d)
            with us_cols[4]:
                _v = us_result.get('gld_price', 0)
                _val = _safe_str_for_dom(f"${_safe_num(_v, '{:.1f}')}" if _v else "N/A")
                _d = _safe_str_for_dom(f"{_safe_num(us_result.get('gld_change_pct', 0), '{:+.1f}')}%" if _v else None) or None
                st.metric("GLD (黄金)", _val, delta=_d)
            with us_cols[5]:
                _v = us_result.get('tnx_yield', 0)
                _val = _safe_str_for_dom(f"{_safe_num(_v, '{:.2f}')}%" if _v else "N/A")
                _d = _safe_str_for_dom(_safe_num(us_result.get('tnx_change', 0), "{:+.2f}") if _v else None) or None
                st.metric("10Y美债", _val, delta=_d, delta_color="inverse")
            with us_cols[6]:
                _v = us_result.get('dxy_level', 0)
                _val = _safe_str_for_dom(_safe_num(_v, "{:.1f}") if _v else "N/A")
                _d = _safe_str_for_dom(f"{_safe_num(us_result.get('dxy_change_pct', 0), '{:+.1f}')}%" if _v else None) or None
                st.metric("DXY (美元)", _val, delta=_d, delta_color="inverse")
            with us_cols[7]:
                _corr = us_result.get('correlation', None)
                _corr_val = _safe_str_for_dom(f"{_corr:.2f}" if _corr is not None else "N/A")
                _corr_desc = _sanitize_html(us_result.get('corr_desc', ''))
                st.metric("股债相关性", _corr_val)
                if _corr_desc:
                    st.caption(_corr_desc[:20])
            with us_cols[8]:
                _us_v = us_result.get('verdict', 'Unknown')
                _us_color = "#10b981" if _us_v == "Risk On" else ("#ef4444" if _us_v == "Risk Off" else "#f59e0b")
                _us_v_safe = _sanitize_html(_us_v)
                st.markdown(f'<div style="font-family: inherit; background:{_us_color};color:white;padding:0.6rem;border-radius:6px;font-weight:600;text-align:center;margin-top:1.2rem;font-size:12px;">{_us_v_safe}</div>', unsafe_allow_html=True)
            _vix_st = _sanitize_html(us_result.get("vix_status", ""))
            _reason_safe = _sanitize_html(us_result.get("reason", ""))[:120]
            st.caption(f"[美国] {_vix_st} | {_reason_safe}{_ath_txt('^GSPC')}")
        
            # 第二行：A股（6个指标：上证、沪深300、创业板、波动率、人民币、体制）
            st.markdown("#### 🇨🇳 A股")
            cn_cols = st.columns(6)
            _cn_idx = cn_result.get('index_level', 0)
            _cn_chg = cn_result.get('index_change_pct', 0)
            _cn_vol = cn_result.get('volatility', 0)
            _cn_v = cn_result.get('verdict', 'Unknown')
            _cn_color = "#10b981" if _cn_v == "Risk On" else ("#ef4444" if _cn_v == "Risk Off" else "#f59e0b")
            with cn_cols[0]:
                _cn_val = f"{_cn_idx:.0f}" if _cn_idx and _cn_idx == _cn_idx and _cn_idx > 0 else "N/A"
                _cn_d = f"{_cn_chg:+.2f}%" if _cn_idx and _cn_idx > 0 and _cn_chg == _cn_chg else None
                st.metric("上证指数", _safe_str_for_dom(_cn_val), delta=_safe_str_for_dom(_cn_d) if _cn_d else None)
            with cn_cols[1]:
                _hs300 = cn_result.get('hs300_price', 0)
                _hs_val = f"{_hs300:.0f}" if _hs300 and _hs300 > 0 else "N/A"
                _hs_d = f"{cn_result.get('hs300_change_pct', 0):+.2f}%" if _hs300 and _hs300 > 0 else None
                st.metric("沪深300", _safe_str_for_dom(_hs_val), delta=_safe_str_for_dom(_hs_d) if _hs_d else None)
            with cn_cols[2]:
                _cyb = cn_result.get('cyb_price', 0)
                _cy_val = f"{_cyb:.0f}" if _cyb and _cyb > 0 else "N/A"
                _cy_d = f"{cn_result.get('cyb_change_pct', 0):+.2f}%" if _cyb and _cyb > 0 else None
                _cy_is_etf = bool(cn_result.get("cyb_use_etf"))
                _cy_label = "创业板ETF代理" if _cy_is_etf else "创业板指"
                if _cy_is_etf:
                    _cy_val = f"{_cyb:.3f} 元" if _cyb and _cyb > 0 else "N/A"
                else:
                    _cy_val = f"{_cyb:.0f} 点" if _cyb and _cyb > 0 else "N/A"
                st.metric(_cy_label, _safe_str_for_dom(_cy_val), delta=_safe_str_for_dom(_cy_d) if _cy_d else None)
            with cn_cols[3]:
                st.metric("波动率", _safe_str_for_dom(f"{_cn_vol:.1f}%" if _cn_vol and _cn_vol > 0 else "N/A"))
            with cn_cols[4]:
                _cny = cn_result.get('cny_price', 0)
                _cny_val = f"{_cny:.4f}" if _cny and _cny > 0 else "N/A"
                _cny_d = f"{cn_result.get('cny_change_pct', 0):+.2f}%" if _cny and _cny > 0 else None
                st.metric("人民币", _safe_str_for_dom(_cny_val), delta=_safe_str_for_dom(_cny_d) if _cny_d else None)
            with cn_cols[5]:
                _cn_v_safe = _sanitize_html(_cn_v)
                st.markdown(f'<div style="font-family: inherit; background:{_cn_color};color:white;padding:0.6rem;border-radius:6px;font-weight:600;text-align:center;margin-top:1.2rem;">{_cn_v_safe}</div>', unsafe_allow_html=True)
            _cn_vol_st = _sanitize_html(cn_result.get("vol_status", ""))
            st.markdown(f'<p style="font-size: 12px; color: #666;">[A股] {_cn_vol_st} | {_reason_cn(cn_result.get("reason", ""))[:80]}{_ath_txt("000001.SS")}</p>', unsafe_allow_html=True)
        
            # 第三行：港股（6个指标：恒指、恒生科技、国企指数、波动率、港币、体制）
            st.markdown("#### 🇭🇰 港股")
            hk_cols = st.columns(6)
            _hk_idx = hk_result.get('index_level', 0)
            _hk_chg = hk_result.get('index_change_pct', 0)
            _hk_vol = hk_result.get('volatility', 0)
            _hk_v = hk_result.get('verdict', 'Unknown')
            _hk_color = "#10b981" if _hk_v == "Risk On" else ("#ef4444" if _hk_v == "Risk Off" else "#f59e0b")
            with hk_cols[0]:
                _hk_val = f"{_hk_idx:.0f}" if _hk_idx and _hk_idx == _hk_idx and _hk_idx > 0 else "N/A"
                _hk_d = f"{_hk_chg:+.2f}%" if _hk_idx and _hk_idx > 0 and _hk_chg == _hk_chg else None
                st.metric("恒生指数", _safe_str_for_dom(_hk_val), delta=_safe_str_for_dom(_hk_d) if _hk_d else None)
            with hk_cols[1]:
                _hstech = hk_result.get('hstech_price', 0)
                _use_etf = hk_result.get('hstech_use_etf', False)
                _label = _safe_str_for_dom("恒生科技(ETF)" if _use_etf else "恒生科技")
                _fmt = f"{_hstech:.2f}" if _use_etf else f"{_hstech:.0f}"
                _hst_val = _fmt if _hstech and _hstech == _hstech and _hstech > 0 else "N/A"
                _hst_d = f"{hk_result.get('hstech_change_pct', 0):+.2f}%" if _hstech and _hstech > 0 else None
                st.metric(_label, _safe_str_for_dom(_hst_val), delta=_safe_str_for_dom(_hst_d) if _hst_d else None)
            with hk_cols[2]:
                _hsce = hk_result.get('hsce_price', 0)
                _hsce_val = f"{_hsce:.0f}" if _hsce and _hsce > 0 else "N/A"
                _hsce_d = f"{hk_result.get('hsce_change_pct', 0):+.2f}%" if _hsce and _hsce > 0 else None
                st.metric("国企指数", _safe_str_for_dom(_hsce_val), delta=_safe_str_for_dom(_hsce_d) if _hsce_d else None)
            with hk_cols[3]:
                st.metric("波动率", _safe_str_for_dom(f"{_hk_vol:.1f}%" if _hk_vol and _hk_vol > 0 else "N/A"))
            with hk_cols[4]:
                _hkd = hk_result.get('hkd_price', 0)
                _hkd_val = f"{_hkd:.4f}" if _hkd and _hkd > 0 else "N/A"
                _hkd_d = f"{hk_result.get('hkd_change_pct', 0):+.2f}%" if _hkd and _hkd > 0 else None
                st.metric("港币", _safe_str_for_dom(_hkd_val), delta=_safe_str_for_dom(_hkd_d) if _hkd_d else None)
            with hk_cols[5]:
                _hk_v_safe = _sanitize_html(_hk_v)
                st.markdown(f'<div style="font-family: inherit; background:{_hk_color};color:white;padding:0.6rem;border-radius:6px;font-weight:600;text-align:center;margin-top:1.2rem;">{_hk_v_safe}</div>', unsafe_allow_html=True)
            _hk_vol_st = _sanitize_html(hk_result.get("vol_status", ""))
            st.markdown(f'<p style="font-size: 12px; color: #666;">[港股] {_hk_vol_st} | {_reason_cn(hk_result.get("reason", ""))[:80]}{_ath_txt("^HSI")}</p>', unsafe_allow_html=True)
        
            # 宏观综合解读条 - 英文术语括号中文小一号
            try:
                _market_caps = [int(float(r.get('position_cap', 80))) for r in (us_result, hk_result, cn_result)]
                _market_caps = [x for x in _market_caps if 0 <= x <= 100]
                _pos_cap = min(_market_caps) if _market_caps else 80
                if summary.get('risk_off_count', 0) >= 2:
                    _pos_cap = min(_pos_cap, 30)
            except (TypeError, ValueError):
                _pos_cap = 30 if summary.get('risk_off_count', 0) >= 2 else 80
            _macro_reason = _reason_cn(
                f"{summary.get('global_reason', '')}；美国：{us_result.get('reason', '')}"
            )
            _cap_color = "#10b981" if _pos_cap >= 70 else ("#f59e0b" if _pos_cap >= 50 else "#ef4444")
            st.markdown(f'<div style="font-family: inherit; background: linear-gradient(135deg, #1e293b 0%, #334155 100%); padding: 1rem 1.5rem; border-radius: 8px; margin-top: 0.5rem; display: flex; align-items: center; gap: 1rem;"><div style="font-family: inherit; color: white; flex: 1; font-size: 12px;">💡 <b>宏观解读</b>：{_macro_reason}</div><div style="font-family: inherit; background: {_cap_color}; color: white; padding: 0.5rem 1rem; border-radius: 6px; font-weight: 700; font-size: 12px; white-space: nowrap;">仓位上限 {_pos_cap}%</div></div>', unsafe_allow_html=True)
            st.markdown('📖 全局仓位上限取三市场建议中的最低值；当至少两个市场处于 Risk Off<span style="font-size: 0.9em;">(风险规避)</span>时，上限固定不超过30%。', unsafe_allow_html=True)

        # 【2026-07-12 用户要求】此处原有 st.divider()：下方行业热力已停用(if False)，
        # 连续空分隔线只留末尾一条，删除以压缩版面。

        # ═══════════════════════════════════════════════════════════════
        # 行业热力（懒加载：仅在选择「仅行业热力」或「全部」时拉取）
        # ═══════════════════════════════════════════════════════════════
        # 行业热力不再在首页渲染，避免空占位和完整模块占用大量纵向空间。
        if False:
            pass
        elif False:
            # 缓存倒计时
            _heat_remain = _heat_remaining_seconds()
            if _heat_remain is not None and _heat_remain > 0:
                _heat_h, _heat_m = divmod(_heat_remain // 60, 60)
                _heat_countdown = f"⏱ 缓存剩余 {_heat_h}h {_heat_m:02d}m"
            else:
                _heat_countdown = "⏱ 缓存已过期"
            _heat_load_bj = _dt_global.now(ZoneInfo("Asia/Shanghai")).strftime("%H:%M")
            st.caption(f"环球行业资金走向 · 轮动天数 · 52周水位 · 中美港论坛热度(大数据代理) · 点击行业可 AI 分析 · {_heat_countdown} · 加载于 {_heat_load_bj} 北京")

            _heat_rcol = st.columns([5, 1])[1]
            with _heat_rcol:
                if st.button("🔄 强制刷新", key="refresh_heat", help="穿透全部缓存，重新拉取行业数据", width='stretch'):
                    get_market_heat.clear()
                    try:
                        _cached_expectation_all_markets.clear()
                    except Exception:
                        pass
                    st.cache_data.clear()
                    try:
                        local_cache.clear_all()
                    except Exception:
                        pass
                    try:
                        _HEAT_CACHE_TS_FILE.unlink(missing_ok=True)
                        _HEAT_DF_FILE.unlink(missing_ok=True)
                    except Exception:
                        pass
                    st.rerun()

            selected_heat = _render_sector_heat_panel(heat_df)

            if selected_heat and hasattr(selected_heat, 'selection') and len(selected_heat.selection.rows) > 0:
                selected_idx = selected_heat.selection.rows[0]
                _disp_df = st.session_state.get("_heat_view_df", heat_df)
                _sel_row = _disp_df.iloc[selected_idx]
                sector_name = _sel_row["行业"]
                selected_row = heat_df[heat_df["行业"] == sector_name].iloc[0]
                _flow_sum = selected_row.get("_flow_summary", "")
                st.info(f"📊 **{sector_name}** · {selected_row.get('资金动向', '—')} · 建议：**{selected_row.get('操作建议', '观望')}**")
                if _flow_sum:
                    st.caption(f"💰 {_flow_sum} · {selected_row.get('跨市场对比', '')}")
                # 论坛热度详情
                _forum_detail_cols = st.columns(3)
                for _fi, (_fk, _fl, _ff) in enumerate([
                    ("US", "🇺🇸 美股论坛", "Reddit/WSB"),
                    ("HK", "🇭🇰 港股论坛", "雪球/富途"),
                    ("CN", "🇨🇳 A股论坛", "雪球/东财股吧"),
                ]):
                    _fd = selected_row.get(f"_forum_{_fk}", {})
                    with _forum_detail_cols[_fi]:
                        st.metric(
                            _fl,
                            f"{_fd.get('icon', '⚪')} {_fd.get('score', 50)}/100",
                            delta=_fd.get("level", "平淡"),
                        )
                        st.caption(f"数据源: {_ff} · 轮动/量价大数据代理")
                st.caption("💡 **轮动天数**：连续跑赢基准=资金流入(🟢+N天)；跑输=流出(🔴-N天)。**水位**：52周区间位置，高=近高点，低=近低点。")
                if st.button(f"🌍 一键分析全球{sector_name}行业（美股+港股+A股）", key="analyze_global_sector", width='stretch', type="primary"):
                    st.session_state.sector_analysis_name = sector_name
                    st.session_state.sector_analysis_market = "全球"
                    st.session_state.sector_analysis_codes = {
                        "us": selected_row["_us_code"],
                        "hk": selected_row["_hk_code"],
                        "cn": selected_row["_cn_code"]
                    }
                    st.session_state.sector_analysis_heat = {
                        "us_rot": selected_row.get("🇺🇸 轮动·水位", "N/A"),
                        "hk_rot": selected_row.get("🇭🇰 轮动·水位", "N/A"),
                        "cn_rot": selected_row.get("🇨🇳 轮动·水位", "N/A"),
                        "forum": selected_row.get("💬 论坛热度(大数据)", "N/A"),
                        "us_forum": selected_row.get("_forum_US", {}),
                        "hk_forum": selected_row.get("_forum_HK", {}),
                        "cn_forum": selected_row.get("_forum_CN", {}),
                    }
                    st.toast(f"🚀 AI分析全球{sector_name}行业中...", icon="🌍")
                    st.rerun()

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

            if not MY_DEEPSEEK_KEY:
                st.error("❌ 未配置 DeepSeek API Key")
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
                    analysis_text_s = st.write_stream(call_gemini_api_stream(prompt_s))
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
    [data-testid="stMarkdown"] div, [data-testid="stMarkdown"] p, [data-testid="stMarkdown"] span,
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
    h1, [data-testid="stMarkdown"] h1 { font-family: var(--v88-sans) !important; font-weight: 700 !important; color: #1a1a2e !important; font-size: 22px !important; }
    h2, [data-testid="stMarkdown"] h2 { font-family: var(--v88-sans) !important; font-weight: 700 !important; color: #1e3a5f !important; font-size: 18px !important; }
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

    [data-testid="stMarkdown"] p, [data-testid="stMarkdown"] li, [data-testid="stMarkdown"] span {
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
            font-size: 11px !important;
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
            font-size: 11px !important;
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
            font-size: 10px !important;
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
            font-size: 11px !important;
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
            font-size: 10px !important;
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
        .heat-card-sub { font-size: 11px; color: #64748b; margin: 4px 0 6px; }
        .heat-card-body { font-size: 11px; color: #374151; line-height: 1.6; }
        .heat-card-forum { font-size: 10px; color: #8b5cf6; margin-top: 6px; }
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
    .heat-card-sub { font-size: 11px; color: #64748b; margin: 4px 0 6px; }
    .heat-card-body { font-size: 11px; color: #374151; line-height: 1.6; }
    .heat-card-forum { font-size: 10px; color: #8b5cf6; margin-top: 6px; }
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
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
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
    # GEMINI_MODEL_NAME = "gemini-2.5-flash"  # 已在前面定义
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
        e = d.get(str(code)) or {"name": name, "n": 0}
        e["n"] = int(e.get("n", 0)) + 1
        e["name"], e["ts"] = name, time.time()
        d[str(code)] = e
        _SEARCH_HIST_FILE.write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        pass

_WATCHLIST_MAX = 20

def _watchlist_save(d):
    # 【V88·重点观察同步】搜索过的个股自动入观察池，镜像到私仓目录随日报提交上云
    try:
        import json as _j
        (Path.home() / "Desktop" / "ai-daily-report-v2" / "watchlist_v88.json").write_text(
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
            return {k: [tuple(x) for x in d.get(k, [])] for k in ("US", "HK", "CN")}
    except Exception:
        pass
    d = {k: list(v) for k, v in WATCHLIST.items()}  # 首次从内置初始化
    _watchlist_save(d)
    return d

def _watchlist_market(code):
    c = str(code).upper()
    if c.endswith(".HK"):
        return "HK"
    if c.endswith(".SS") or c.endswith(".SZ"):
        return "CN"
    return "US"

def _watchlist_add(code, name):
    """搜索选中的个股自动加入自选股；总量>20 时从最长的市场列表头部淘汰最早的。"""
    d = _watchlist_load()
    mkt = _watchlist_market(code)
    if any(c == code for c, _ in d.get(mkt, [])):
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
                _repo_lv = Path.home() / "Desktop" / "ai-daily-report-v2"
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
    if False:  # removed fallback placeholder
        special_codes = {
            'BRK.B': 'BRK-B',
            'BRK.A': 'BRK-A',
            'BF.B': 'BF-B',
            'BF.A': 'BF-A',
        }
        if code in special_codes:
            old_code = code
            code = special_codes[code]
            logging.info(f"📝 特殊代码修正: {old_code} -> {code}")
        
        # 【V87.4 Critical Fix】已经有后缀的需要检查港股前导零问题
        if code.endswith(".SS") or code.endswith(".SZ"): 
            return code
        elif code.endswith(".HK"):
            # 检查港股前导零问题：09992.HK -> 9992.HK
            hk_num = code[:-3]  # 去掉 .HK
            if hk_num.isdigit() and len(hk_num) == 5 and hk_num.startswith('0'):
                # 去掉前导零：09992 -> 9992
                corrected_num = hk_num[1:]
                return f"{corrected_num}.HK"
            else:
                return code
        
        # 沪市改为 .SS
        if code.endswith(".SH"): 
            return code[:-3] + ".SS"
        
        # 纯数字代码判断
        if code.isdigit():
            # 【V75.2 最终修复】港股代码：保留4位数字（去掉最左边的一个0）
            if len(code) == 5:
                # 00700 -> 0700.HK (✅ Yahoo Finance 要求)
                # 02318 -> 2318.HK
                # 09988 -> 9988.HK
                hk_code = code[1:]  # 去掉第一个字符（最左边的0）
                return f"{hk_code}.HK"
            elif len(code) == 4:
                # 已经是4位的直接加后缀
                return f"{code}.HK"
            
            # A股代码（6位）
            if code.startswith("6") or code.startswith("5"): 
                return f"{code}.SS"  # 沪市
            if code.startswith("0") or code.startswith("3"): 
                return f"{code}.SZ"  # 深市
        
        return code

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

    # 【V92】A股优先 Tushare（直连不走代理、~0.2s、稳定）——扫描提速 & 修复 A 股 N/A
    if target_code.endswith('.SS') or target_code.endswith('.SZ'):
        try:
            from ts_helper import fetch_daily_tushare as _ts_daily
            _ts_df = _ts_daily(target_code, days=400)
            if _ts_df is not None and len(_ts_df) >= 30:
                _safe_print(f"[fetch] ✅ Tushare 获取 {target_code}")
                data_quality = {
                    'source': 'Tushare', 'last_updated': pd.Timestamp.now(),
                    'is_delayed': True, 'data_points': len(_ts_df),
                    'date_range': f"{_ts_df.index[0].date()} 至 {_ts_df.index[-1].date()}"
                }
                if return_quality:
                    result = (_ts_df, data_quality)
                elif return_source:
                    result = (_ts_df, 'Tushare')
                else:
                    result = _ts_df
                local_cache.set(cache_key, result)
                return result
        except Exception as _ts_e:
            _safe_print(f"[fetch] Tushare {target_code} 失败: {_ts_e}")

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
# 根因：东财熔断降级到 Tushare 后，A股日线只有截至昨日的收盘数据，
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
        s = requests.Session(); s.trust_env = False
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
        # 成交量单位自适应：Tushare日线是"手"、腾讯是"股"，混用会让量能因子误判100倍
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
        _p = Path.home() / "Desktop" / "ai-daily-report-v2" / "data" / "market_snapshot.json"
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
            st.metric("共识", rec_cn)
        _biz_cache_key = f"_biz_cn_{target_c}"
        _biz_cn = st.session_state.get(_biz_cache_key, "")
        if not _biz_cn:
            _cname = fundamentals.get("company_name", target_c)
            _sector = fundamentals.get("sector", "")
            _industry = fundamentals.get("industry", "")
            try:
                _biz_cn = call_gemini_api(
                    f"请用中文撰写「{_cname}」（股票代码：{target_c}，行业：{_sector}/{_industry}）的公司简介，"
                    f"包括：主营业务、核心产品/服务、商业模式、市场地位。"
                    f"要求：200-300字，专业简洁，全部中文，不要英文。",
                    model_name="gemini-2.0-flash"
                )
            except Exception:
                _biz_cn = ""
            if _biz_cn and not _biz_cn.startswith("❌") and any('\u4e00' <= ch <= '\u9fff' for ch in _biz_cn[:30]):
                st.session_state[_biz_cache_key] = _biz_cn
            else:
                _biz_cn = ""
        if _biz_cn:
            with st.expander("📖 公司简介 & 业务概况", expanded=False):
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

    # ── 公司简介（AI 直接生成中文）──
    _biz_cache_key = f"_biz_cn_{target_c}"
    _biz_cn = st.session_state.get(_biz_cache_key, "")
    if not _biz_cn:
        _cname = fundamentals.get("company_name", target_c)
        _sector = fundamentals.get("sector", "")
        _industry = fundamentals.get("industry", "")
        try:
            _biz_cn = call_gemini_api(
                f"请用中文撰写「{_cname}」（股票代码：{target_c}，行业：{_sector}/{_industry}）的公司简介，"
                f"包括：主营业务、核心产品/服务、商业模式、市场地位。"
                f"要求：200-300字，专业简洁，全部中文，不要英文。",
                model_name="gemini-2.0-flash"
            )
        except Exception:
            _biz_cn = ""
        if _biz_cn and not _biz_cn.startswith("❌") and any('\u4e00' <= ch <= '\u9fff' for ch in _biz_cn[:30]):
            st.session_state[_biz_cache_key] = _biz_cn
        else:
            _biz_cn = ""
    if _biz_cn:
        with st.expander("📖 公司简介 & 业务概况", expanded=False):
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
        sess = requests.Session()  # trust_env=True，自动用环境代理
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
    【V94 扩容】总量1300只（美500+港300+A500），配合批量预取+Tushare直连
    - 美股: 500只（东财按市值降序）
    - 港股: 300只（东财按市值降序）
    - A股: 500只（东财按市值降序，已剔除ST/退市/ETF）
    - 总计: 1300只

    源优先级（【V94.3】三级回退，不绑死单一数据源）：
      美股: 东财 → 维基百科标普500 → 本地备用池
      A股: 东财 → Tushare市值榜 → 本地备用池
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
            ("03366", "中兴通讯", "3366.HK"), ("00941", "中国移动", "0941.HK"), ("00728", "中国电信", "0728.HK"),
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
        # 【V94.3】二级云端源：Tushare 市值榜（国内直连，稳定；已剔除ST/退市/北交所）
        try:
            from ts_helper import fetch_cn_top_pool
            cn_pool = fetch_cn_top_pool(500)
            if cn_pool:
                _safe_print(f"[股票池] ✅ 二级源(Tushare市值榜): {len(cn_pool)} 只")
        except Exception as _tse:
            _safe_print(f"[股票池] ⚠️ Tushare市值榜失败: {str(_tse)[:60]}")
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
    _pool_cache_age  = time.time() - json.loads(_pool_cache_path.read_text()).get("ts", 0) \
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
def call_gemini_api(prompt, model_name=None):
    """
    V88 统一 AI 调用入口（历史函数名保留；实际只调用 DeepSeek）
    
    参数:
        prompt: 提示词
        model_name: 模型名称（可选，默认使用GEMINI_MODEL_NAME）
    
    返回:
        AI生成的文本响应
    """
    _ds_key = os.getenv("DEEPSEEK_API_KEY", "")
    if _ds_key:
        try:
            _r = requests.post(
                "https://api.deepseek.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {_ds_key}", "Content-Type": "application/json"},
                json={"model": "deepseek-v4-flash",
                      "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.3, "max_tokens": 8192},
                timeout=120
            )
            if _r.status_code == 200:
                return _r.json()["choices"][0]["message"]["content"]
        except Exception as _de:
            logging.warning(f"DeepSeek requests failed: {_de}")
    if not MY_DEEPSEEK_KEY:
        return "❌ 请配置 DEEPSEEK_API_KEY"
    if AI_PROVIDER == "deepseek" and _deepseek_client:
        try:
            response = _deepseek_client.chat.completions.create(
                model="deepseek-v4-flash",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3, max_tokens=8192, timeout=120,
            )
            return response.choices[0].message.content
        except Exception as e:
            logging.error(f"❌ DeepSeek API异常: {str(e)}")
            return f"❌ DeepSeek API错误: {str(e)}"
    return "❌ DeepSeek API调用失败，请检查API Key和网络"


def call_gemini_api_stream(prompt, model_name=None, max_output_tokens=8192):
    """历史流式入口兼容层；实际固定调用 DeepSeek。"""
    yield call_gemini_api(prompt, model_name=None)


# ═══════════════════════════════════════════════════════════════
# 7. CANSLIM + 专业投机原理（完整双核评级）
# ═══════════════════════════════════════════════════════════════
def calculate_metrics_all(df, code):
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
        _idx_code = "000001.SS" if (_tc.endswith(".SS") or _tc.endswith(".SZ")) else ("^HSI" if _tc.endswith(".HK") else "^GSPC")
        _idx_df = fetch_stock_data(_idx_code)
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
        mom_score += 10  # 大盘数据不可用时按中性计，避免误伤
        momentum_rows.append({"因子": "11. 相对强度RS", "状态": "—", "说明": "大盘数据不可用，按中性计"})

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
        _mk_t = "A股" if _cu.endswith((".SS", ".SZ")) else ("港股" if _cu.endswith(".HK") else "美股")
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
        final_prices = []
        for _ in range(sims):
            price = last_p * np.exp((mu - 0.5 * sigma**2) * days + sigma * np.sqrt(days) * np.random.normal(0, 1))
            final_prices.append(price)
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
    elif stock_code.endswith('.SS') or stock_code.endswith('.SZ'):
        return '000001.SS'  # 上证指数
    else:
        return '^GSPC'  # 标普500

def calculate_risk_metrics(df, stock_code):
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
        benchmark_df = fetch_stock_data(benchmark_code)
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
            response = requests.get(test_url, proxies=proxies, timeout=5, verify=False)
        else:
            response = requests.get(test_url, timeout=5, verify=False)
        
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
            # 【V87.5】增加超时控制，避免卡顿
            import signal
            
            def timeout_handler(signum, frame):
                raise TimeoutError("新闻获取超时")
            
            # 设置5秒超时
            try:
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(5)
                news = ticker.news
                signal.alarm(0)  # 取消超时
            except:
                # Windows不支持signal.SIGALRM，直接获取
                news = ticker.news
        
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
def _render_portfolio_section():
  """持仓管理渲染函数 - 在主内容区调用"""
  if not (Config.PORTFOLIO_ENABLED and PORTFOLIO_MANAGER_AVAILABLE and _portfolio_manager):
    return
  try:
    from datetime import datetime as _dt_port
    _port_today = _dt_port.now().strftime("%Y-%m-%d")
    st.markdown(f'<div style="font-family: inherit; background: linear-gradient(135deg, #10b981 0%, #059669 100%); padding: 1.2rem; border-radius: 10px; margin: 0.5rem 0;"><h3 style="font-family: inherit; color: white; margin: 0; text-align: center; font-size: 14px; font-weight: 700;">💼 我的持仓</h3><p style="font-family: inherit; color: rgba(255,255,255,0.85); margin: 0.3rem 0 0 0; text-align: center; font-size: 12px;">Excel数据源 · 实时盈亏 · AI分析</p><p style="font-family: inherit; color: rgba(255,255,255,0.6); margin: 0.2rem 0 0 0; text-align: center; font-size: 12px;">📅 {_port_today}</p></div>', unsafe_allow_html=True)
    
    try:
        # 【V89.6.2】显示文件信息和自动检测文件变更
        file_path = os.path.abspath(Config.PORTFOLIO_FILE)
        file_mtime_str = "未知"  # 默认值
        file_mtime = None
        
        if os.path.exists(file_path):
            import time
            file_mtime = os.path.getmtime(file_path)
            file_mtime_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(file_mtime))
            
            # 【V89.6.2】自动检测文件是否被修改
            if 'portfolio_last_mtime' not in st.session_state:
                st.session_state.portfolio_last_mtime = file_mtime
                logging.info(f"📝 初始化持仓文件修改时间: {file_mtime_str}")
            elif st.session_state.portfolio_last_mtime != file_mtime:
                # 文件已被修改！
                st.info(f"🔔 检测到持仓文件已更新！（{file_mtime_str}）正在自动刷新...")
                st.session_state.portfolio_last_mtime = file_mtime
                logging.info(f"🔄 持仓文件已变更，自动刷新: {file_mtime_str}")
                
                # 清除所有可能的缓存
                if hasattr(_portfolio_manager, '_cached_df'):
                    delattr(_portfolio_manager, '_cached_df')
                if 'portfolio_data_cache' in st.session_state:
                    del st.session_state.portfolio_data_cache
                
                time.sleep(0.5)  # 短暂延迟确保文件写入完成
            
            info_col1, info_col2, info_col3 = st.columns([2, 2, 1])
            with info_col1:
                st.caption(f"📁 文件位置: `{file_path}`")
            with info_col2:
                st.caption(f"🕒 最后修改: {file_mtime_str}")
            with info_col3:
                if st.button("🔄 强制刷新", key="force_reload_portfolio", help="重新加载Excel文件"):
                    # 清除所有可能的缓存
                    if hasattr(_portfolio_manager, '_cached_df'):
                        delattr(_portfolio_manager, '_cached_df')
                    if 'portfolio_data_cache' in st.session_state:
                        del st.session_state.portfolio_data_cache
                    if 'portfolio_last_mtime' in st.session_state:
                        del st.session_state.portfolio_last_mtime
                    st.toast("🔄 正在重新加载Excel...", icon="🔄")
                    st.rerun()
        
        # 【V89.6.2】强制每次都重新读取Excel，不使用任何缓存
        # 先清除 PortfolioManager 内部可能的缓存
        if hasattr(_portfolio_manager, '_cached_df'):
            delattr(_portfolio_manager, '_cached_df')
        
        # 直接读取Excel文件，完全绕过缓存
        try:
            import pandas as pd
            portfolio_df = pd.read_excel(Config.PORTFOLIO_FILE, sheet_name='我的持仓', engine='openpyxl')
            
            # 数据验证和清洗
            if '股票代码' in portfolio_df.columns and len(portfolio_df) > 0:
                original_count = len(portfolio_df)
                
                # 清理空值
                portfolio_df = portfolio_df.dropna(subset=['股票代码'])
                
                # 数据类型转换
                portfolio_df['持仓数量'] = pd.to_numeric(portfolio_df['持仓数量'], errors='coerce')
                portfolio_df['买入价格'] = pd.to_numeric(portfolio_df['买入价格'], errors='coerce')
                
                # 移除无效数据
                portfolio_df = portfolio_df.dropna(subset=['持仓数量', '买入价格'])
                portfolio_df = portfolio_df[portfolio_df['持仓数量'] > 0]
                portfolio_df = portfolio_df[portfolio_df['买入价格'] > 0]
                
                if len(portfolio_df) < original_count:
                    st.caption(f"⚠️ 已过滤 {original_count - len(portfolio_df)} 行无效数据")
                
                logging.info(f"✅ 直接读取Excel成功: {len(portfolio_df)}只股票")
            else:
                portfolio_df = None
                logging.warning("⚠️ Excel文件格式不正确或为空")
                
        except Exception as e:
            logging.error(f"❌ 直接读取Excel失败: {str(e)}")
            # 降级到 PortfolioManager
            portfolio_df = _portfolio_manager.get_dataframe() if _portfolio_manager else None
        
        # 【V89.6.2】显示读取状态
        if portfolio_df is not None and len(portfolio_df) > 0:
            st.success(f"✅ 成功读取持仓: {len(portfolio_df)}只股票 | 文件: {os.path.basename(Config.PORTFOLIO_FILE)} | 最后修改: {file_mtime_str}")
            
            # 显示读取到的股票名称
            stock_names = ', '.join([f"{row['股票名称']}" for _, row in portfolio_df.head(5).iterrows()])
            if len(portfolio_df) > 5:
                stock_names += f" 等{len(portfolio_df)}只"
            st.caption(f"📋 持仓股票: {stock_names}")
            
            # 【V89.6.7】醒目的价格缓存状态显示
            st.markdown("---")
            cache_status_col1, cache_status_col2, cache_status_col3 = st.columns([1, 2, 1])
            
            with cache_status_col1:
                if 'portfolio_prices_cache' in st.session_state and 'portfolio_prices_timestamp' in st.session_state:
                    cache_age = time.time() - st.session_state['portfolio_prices_timestamp']
                    if cache_age < 86400:
                        st.metric("📦 缓存状态", "✅ 有效")
                    else:
                        st.metric("⏰ 缓存状态", "❌ 已过期")
                else:
                    st.metric("🆕 缓存状态", "无缓存")
            
            with cache_status_col2:
                if 'portfolio_prices_timestamp' in st.session_state:
                    cache_age = time.time() - st.session_state['portfolio_prices_timestamp']
                    remaining_hours = (86400 - cache_age) / 3600
                    if remaining_hours > 0:
                        cache_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(st.session_state['portfolio_prices_timestamp']))
                        st.caption(f"🕐 更新时间: {cache_time}")
                        st.caption(f"⏳ 剩余有效期: {remaining_hours:.1f}小时")
                    else:
                        st.caption(f"⏰ 缓存已过期: {-remaining_hours:.1f}小时前")
                else:
                    st.caption("首次获取价格数据")
            
            with cache_status_col3:
                if 'portfolio_prices_cache' in st.session_state:
                    cached_count = len(st.session_state['portfolio_prices_cache'])
                    st.metric("缓存股票数", f"{cached_count}只")
            
            st.markdown("---")
        
        if portfolio_df is None or len(portfolio_df) == 0:
            st.warning(f"⚠️ 未读取到持仓数据。请检查 **{Config.PORTFOLIO_FILE}** 文件是否有有效数据。")
            
            # 【V89.6】添加调试信息
            with st.expander("🔍 调试信息（如果Excel有数据但不显示，请查看此处）"):
                st.code(f"""
文件路径: {file_path}
文件是否存在: {os.path.exists(file_path)}
文件大小: {os.path.getsize(file_path) if os.path.exists(file_path) else 'N/A'} 字节
最后修改时间: {file_mtime_str}

可能的原因:
1. Excel文件正在被其他程序打开（请关闭Excel后刷新）
2. Excel文件格式不正确（sheet名称必须是"我的持仓"）
3. Excel中没有有效数据（检查必填列: 股票代码、股票名称、持仓数量、买入价格）
4. 数据被清洗过滤掉了（持仓数量和买入价格必须>0）

解决方法:
→ 点击下方"📝 创建持仓模板"重新生成模板
→ 或手动打开Excel文件检查内容
→ 确认保存后点击"🔄 强制刷新"
                """)
            
            col_create, col_open = st.columns(2)
            with col_create:
                if st.button("📝 创建持仓模板", type="primary", width='stretch'):
                    if _portfolio_manager.create_template():
                        st.success(f"✅ 已创建持仓模板: {Config.PORTFOLIO_FILE}")
                        st.info("💡 请手动编辑Excel文件，添加您的真实持仓数据后刷新页面。")
                    else:
                        st.error("❌ 创建模板失败")
            
            with col_open:
                if st.button("📂 打开Excel编辑", width='stretch'):
                    import subprocess
                    import platform
                    try:
                        file_path = os.path.abspath(Config.PORTFOLIO_FILE)
                        if platform.system() == 'Darwin':  # macOS
                            subprocess.call(['open', file_path])
                        elif platform.system() == 'Windows':
                            os.startfile(file_path)
                        else:  # Linux
                            subprocess.call(['xdg-open', file_path])
                        st.success(f"✅ 已打开文件: {Config.PORTFOLIO_FILE}")
                        st.info("💡 编辑并保存后，刷新页面即可自动加载新数据")
                        st.caption("⚡ 应用会自动检测文件变化！")
                    except Exception as e:
                        st.error(f"❌ 打开文件失败: {str(e)}")
                        st.caption(f"💡 请手动打开: {os.path.abspath(Config.PORTFOLIO_FILE)}")
        
        else:
            # 【V90.4】直接交互式编辑持仓表格 - 点击即可修改/删除/添加
            st.caption("💡 直接点击单元格修改 | 底部 ➕ 添加新股票 | 勾选左侧复选框后按 Delete 删除")
            
            # 准备编辑用的DataFrame：只保留预期列 + 强制转换类型避免报错
            expected_cols = ['股票代码', '股票名称', '持仓数量', '买入价格', '买入日期', '备注']
            edit_df = portfolio_df.copy()
            
            # 确保所有预期列都存在
            for _ec in expected_cols:
                if _ec not in edit_df.columns:
                    edit_df[_ec] = ""
            
            # 只保留预期列，丢弃多余列
            edit_df = edit_df[expected_cols]
            
            # 【关键修复】强制将文本列转为 str，避免 NaN/float 与 TextColumn 配置冲突
            edit_df['股票代码'] = edit_df['股票代码'].astype(str).replace('nan', '')
            edit_df['股票名称'] = edit_df['股票名称'].astype(str).replace('nan', '')
            edit_df['买入日期'] = edit_df['买入日期'].astype(str).replace('nan', '').replace('NaT', '')
            edit_df['备注'] = edit_df['备注'].astype(str).replace('nan', '')
            
            # 直接用 data_editor 显示，用户可即时编辑
            edited_df = st.data_editor(
                edit_df,
                num_rows="dynamic",
                width='stretch',
                column_config={
                    "股票代码": st.column_config.TextColumn(
                        "股票代码",
                        required=True,
                        help="美股: AAPL | 港股: 00700.HK | A股: 600519.SS"
                    ),
                    "股票名称": st.column_config.TextColumn(
                        "股票名称",
                        required=True,
                        help="股票中文名称"
                    ),
                    "持仓数量": st.column_config.NumberColumn(
                        "持仓数量",
                        required=True,
                        min_value=1,
                        help="持有股数（>0）"
                    ),
                    "买入价格": st.column_config.NumberColumn(
                        "买入价格",
                        required=True,
                        min_value=0.01,
                        format="%.2f",
                        help="成本价（>0）"
                    ),
                    "买入日期": st.column_config.TextColumn(
                        "买入日期",
                        help="格式: YYYY-MM-DD"
                    ),
                    "备注": st.column_config.TextColumn(
                        "备注",
                        help="个人备注"
                    ),
                },
                hide_index=True,
                key="portfolio_data_editor"
            )
            
            # 检测是否有修改
            _has_change = False
            if len(edited_df) != len(edit_df):
                _has_change = True
            elif not edited_df.equals(edit_df):
                _has_change = True
            
            # 操作按钮行
            btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 2])
            with btn_col1:
                _save_clicked = st.button("💾 保存修改", type="primary", width='stretch', disabled=not _has_change)
            with btn_col2:
                if st.button("📂 打开Excel", width='stretch', key="open_excel_btn"):
                    import subprocess
                    import platform
                    try:
                        file_path = os.path.abspath(Config.PORTFOLIO_FILE)
                        if platform.system() == 'Darwin':
                            subprocess.call(['open', file_path])
                        elif platform.system() == 'Windows':
                            os.startfile(file_path)
                        else:
                            subprocess.call(['xdg-open', file_path])
                        st.success(f"✅ 已打开: {Config.PORTFOLIO_FILE}")
                    except Exception as e:
                        st.error(f"❌ 打开失败: {str(e)}")
            with btn_col3:
                if _has_change:
                    if len(edited_df) > len(edit_df):
                        st.info(f"➕ 新增 {len(edited_df) - len(edit_df)} 只股票，点击「保存修改」生效")
                    elif len(edited_df) < len(edit_df):
                        st.warning(f"🗑️ 删除 {len(edit_df) - len(edited_df)} 只股票，点击「保存修改」生效")
                    else:
                        st.info("✏️ 检测到数据修改，点击「保存修改」生效")
            
            # 保存逻辑
            if _save_clicked:
                try:
                    valid_df = edited_df.copy()
                    
                    # 清理空行
                    valid_df = valid_df.dropna(subset=['股票代码', '股票名称'])
                    valid_df = valid_df[valid_df['股票代码'].str.strip() != '']
                    valid_df = valid_df[valid_df['股票名称'].str.strip() != '']
                    
                    # 验证数值
                    valid_df['持仓数量'] = pd.to_numeric(valid_df['持仓数量'], errors='coerce')
                    valid_df['买入价格'] = pd.to_numeric(valid_df['买入价格'], errors='coerce')
                    
                    # 过滤无效数据
                    before_count = len(valid_df)
                    valid_df = valid_df.dropna(subset=['持仓数量', '买入价格'])
                    valid_df = valid_df[valid_df['持仓数量'] > 0]
                    valid_df = valid_df[valid_df['买入价格'] > 0]
                    filtered_count = before_count - len(valid_df)
                    
                    if filtered_count > 0:
                        st.warning(f"⚠️ 已过滤 {filtered_count} 行无效数据")
                    
                    # 保存到Excel
                    valid_df.to_excel(Config.PORTFOLIO_FILE, index=False, sheet_name='我的持仓')
                    
                    # 清除价格缓存
                    if 'portfolio_prices_cache' in st.session_state:
                        del st.session_state['portfolio_prices_cache']
                    if 'portfolio_prices_timestamp' in st.session_state:
                        del st.session_state['portfolio_prices_timestamp']
                    
                    st.success(f"✅ 已保存（共{len(valid_df)}只股票）")
                    time.sleep(0.5)
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ 保存失败: {str(e)}")
                    logging.error(f"保存持仓失败: {str(e)}", exc_info=True)
            
            st.markdown("---")
            
            # 【V89.6.7】获取当前价格 - 【V91.10】统一缓存：交易日15分钟，非交易日24小时
            cache_key = 'portfolio_prices_cache'
            cache_timestamp_key = 'portfolio_prices_timestamp'
            cache_ttl = get_smart_cache_ttl('daily')
            
            current_time = time.time()
            current_prices = None  # 使用None而不是{}，方便判断是否已从缓存加载
            
            # 【V89.6.7】调试信息：检查缓存状态
            force_refresh_price = st.session_state.get('force_refresh_price', False)
            has_cache = cache_key in st.session_state
            has_timestamp = cache_timestamp_key in st.session_state
            
            st.markdown("---")
            st.markdown("### 💰 价格数据")
            
            # 【调试面板】显示当前缓存状态
            with st.expander("🔍 缓存状态（调试）", expanded=False):
                st.code(f"""
强制刷新: {force_refresh_price}
缓存存在: {has_cache}
时间戳存在: {has_timestamp}
当前时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))}
""")
                if has_timestamp:
                    cache_age = current_time - st.session_state[cache_timestamp_key]
                    st.code(f"""
缓存时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(st.session_state[cache_timestamp_key]))}
缓存年龄: {cache_age:.0f}秒 ({cache_age/3600:.2f}小时)
缓存TTL: {cache_ttl}秒 ({cache_ttl/3600:.0f}小时)
是否有效: {cache_age < cache_ttl}
""")
            
            logging.info(f"📊 持仓价格缓存检查: force_refresh={force_refresh_price}, has_cache={has_cache}, has_timestamp={has_timestamp}")
            
            # 尝试使用缓存
            if not force_refresh_price and has_cache and has_timestamp:
                try:
                    cache_age = current_time - st.session_state[cache_timestamp_key]
                    remaining_hours = (cache_ttl - cache_age) / 3600
                    
                    if cache_age < cache_ttl:
                        # 缓存有效 - 直接使用！
                        current_prices = st.session_state[cache_key]
                        cache_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(st.session_state[cache_timestamp_key]))
                        st.success(f"✅ 使用缓存价格数据 | 更新时间: {cache_time_str} | 剩余有效期: {remaining_hours:.1f}小时")
                        logging.info(f"✅ 使用持仓价格缓存，剩余{remaining_hours:.1f}小时")
                    else:
                        # 缓存过期
                        st.info(f"⏰ 价格缓存已过期（{cache_age/3600:.1f}小时前），正在重新获取...")
                        logging.info(f"⏰ 持仓价格缓存过期: {cache_age/3600:.1f}小时")
                except Exception as e:
                    st.warning(f"⚠️ 读取缓存失败: {str(e)}，将重新获取")
                    logging.error(f"读取缓存失败: {str(e)}")
            elif force_refresh_price:
                st.info("🔄 强制刷新模式，忽略缓存")
                logging.info("🔄 强制刷新持仓价格")
            elif not has_cache or not has_timestamp:
                _ttl_hint = f"{cache_ttl//3600}小时" if cache_ttl >= 3600 else f"{cache_ttl//60}分钟"
                st.info(f"🆕 首次获取价格数据，将缓存{_ttl_hint}")
                logging.info("🆕 首次获取持仓价格数据")
            
            # 如果缓存无效（current_prices仍为None），重新获取价格
            if current_prices is None:
                st.markdown("---")
                st.markdown("#### 📡 正在获取最新价格...")
                
                current_prices = {}
                price_progress = st.progress(0)
                price_status = st.empty()
                
                for idx, row in portfolio_df.iterrows():
                    code = str(row['股票代码']).strip()
                    stock_name = row['股票名称']
                    price_status.text(f"正在获取 {stock_name}({code}) 最新价格...")
                    
                    try:
                        # 获取数据
                        df_stock = fetch_stock_data(to_yf_cn_code(code))
                        if df_stock is not None and len(df_stock) > 0:
                            current_prices[code] = float(df_stock['Close'].iloc[-1])
                            logging.info(f"✅ {stock_name}({code}) 当前价格: {current_prices[code]}")
                        else:
                            current_prices[code] = None
                            logging.warning(f"⚠️ {stock_name}({code}) 价格获取失败：数据为空")
                    except Exception as e:
                        current_prices[code] = None
                        logging.error(f"❌ {stock_name}({code}) 价格获取异常: {str(e)}")
                    
                    price_progress.progress((idx + 1) / len(portfolio_df))
                
                price_progress.empty()
                price_status.empty()
                
                # 显示价格获取统计
                success_count = sum(1 for v in current_prices.values() if v is not None)
                st.caption(f"📊 价格获取: {success_count}/{len(portfolio_df)} 成功")
                
                # 【V89.6.7】强制保存到缓存
                try:
                    st.session_state[cache_key] = current_prices
                    st.session_state[cache_timestamp_key] = current_time
                    st.session_state['force_refresh_price'] = False
                    
                    cache_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))
                    st.success(f"✅ 价格数据已更新并缓存（有效期: 24小时） | 缓存时间: {cache_time_str}")
                    logging.info(f"✅ 持仓价格已保存到缓存: {len(current_prices)}只股票, 时间戳: {current_time}")
                except Exception as e:
                    st.error(f"❌ 缓存保存失败: {str(e)}")
                    logging.error(f"缓存保存失败: {str(e)}")
            
            # 添加强制刷新价格按钮
            refresh_col1, refresh_col2, refresh_col3 = st.columns([2, 2, 2])
            with refresh_col1:
                if st.button("🔄 强制刷新价格", key="force_refresh_prices_btn"):
                    st.session_state['force_refresh_price'] = True
                    if cache_key in st.session_state:
                        del st.session_state[cache_key]
                    if cache_timestamp_key in st.session_state:
                        del st.session_state[cache_timestamp_key]
                    st.toast("🔄 价格缓存已清除，正在重新获取...", icon="🔄")
                    st.rerun()
            
            with refresh_col2:
                if cache_timestamp_key in st.session_state:
                    cache_time = time.strftime('%H:%M:%S', time.localtime(st.session_state[cache_timestamp_key]))
                    st.caption(f"⏰ 价格数据时间: {cache_time}")
            
            with refresh_col3:
                st.caption("💡 价格每天自动更新一次")
            
            # 计算持仓指标
            try:
                metrics_df = _portfolio_manager.calculate_portfolio_metrics(portfolio_df, current_prices)
            except Exception as e:
                st.error(f"❌ 计算持仓指标失败: {str(e)}")
                logging.error(f"计算持仓指标失败: {str(e)}", exc_info=True)
                # 使用简化计算
                metrics_df = portfolio_df.copy()
                for code in current_prices:
                    if code in metrics_df['股票代码'].values:
                        idx = metrics_df[metrics_df['股票代码'] == code].index[0]
                        current_price = current_prices[code]
                        if current_price is not None:
                            metrics_df.loc[idx, '当前价格'] = current_price
                            cost = metrics_df.loc[idx, '买入价格']
                            quantity = metrics_df.loc[idx, '持仓数量']
                            metrics_df.loc[idx, '盈亏比例'] = ((current_price - cost) / cost * 100)
                            metrics_df.loc[idx, '盈亏金额'] = (current_price - cost) * quantity
                            metrics_df.loc[idx, '持仓市值'] = current_price * quantity
                        else:
                            metrics_df.loc[idx, '当前价格'] = None
                            metrics_df.loc[idx, '盈亏比例'] = None
                            metrics_df.loc[idx, '盈亏金额'] = None
                            metrics_df.loc[idx, '持仓市值'] = None
            
            # 获取汇总信息
            summary = _portfolio_manager.get_portfolio_summary(metrics_df)
            
            if summary:
                # 显示汇总
                st.markdown("### 📊 持仓汇总")
                sum_col1, sum_col2, sum_col3, sum_col4 = st.columns(4)
                
                with sum_col1:
                    st.metric("总市值", f"¥{summary['total_market_value']:,.2f}")
                
                with sum_col2:
                    st.metric("总成本", f"¥{summary['total_cost']:,.2f}")
                
                with sum_col3:
                    profit_color = "normal" if summary['total_profit'] >= 0 else "inverse"
                    st.metric("总盈亏", f"¥{summary['total_profit']:,.2f}", 
                             delta=f"{summary['total_profit_pct']:.2f}%",
                             delta_color=profit_color)
                
                with sum_col4:
                    st.metric("持仓股票", f"{summary['stock_count']}只",
                             delta=f"盈利{summary['profitable_count']}只")
                
                st.markdown("---")
            
            # 显示持仓明细
            st.markdown("### 💼 持仓明细")
            
            # 格式化显示
            display_df = metrics_df.copy()
            display_df = display_df[['股票代码', '股票名称', '持仓数量', '买入价格', 
                                    '当前价格', '盈亏比例', '盈亏金额', '持仓市值', '备注']]
            
            # 添加样式
            def highlight_profit(row):
                if pd.isna(row['盈亏比例']):
                    return [''] * len(row)
                
                color = ''
                if row['盈亏比例'] > 0:
                    color = 'background-color: #10b98120'
                elif row['盈亏比例'] < 0:
                    color = 'background-color: #ef444420'
                
                return [color] * len(row)
            
            styled_df = display_df.style.apply(highlight_profit, axis=1)
            st.dataframe(styled_df, width='stretch', height=400)
            
            # 【V89.5】AI持仓组合分析
            st.markdown("---")
            st.markdown("### 🤖 AI持仓组合分析")
            st.caption("💡 按市场分组分析您的持仓组合（美股/港股/A股）")
            
            # 按市场分组持仓
            def classify_market(code):
                """判断股票所属市场"""
                code_str = str(code).strip()
                if code_str[0].isalpha():  # 以字母开头，美股
                    return "🇺🇸 美股"
                elif len(code_str) == 5 or (len(code_str) >= 4 and code_str[0] == '0' and not code_str.startswith('00')):  # 港股
                    return "🇭🇰 港股"
                elif code_str.startswith('6') or code_str.startswith('0') or code_str.startswith('3'):  # A股
                    return "🇨🇳 A股"
                else:
                    return "❓ 其他"
            
            # 为每只股票分类
            metrics_df['市场'] = metrics_df['股票代码'].apply(classify_market)
            
            # 按市场分组统计
            market_summary = {}
            for market in ["🇺🇸 美股", "🇭🇰 港股", "🇨🇳 A股"]:
                market_stocks = metrics_df[metrics_df['市场'] == market]
                if len(market_stocks) > 0:
                    market_summary[market] = {
                        'count': len(market_stocks),
                        'total_value': market_stocks['持仓市值'].sum(),
                        'total_profit': market_stocks['盈亏金额'].sum(),
                        'stocks': market_stocks,
                        'top_stock': market_stocks.nlargest(1, '持仓市值').iloc[0] if len(market_stocks) > 0 else None
                    }
            
            # 显示市场分组
            if market_summary:
                st.markdown("#### 📊 市场分布")
                market_cols = st.columns(len(market_summary))
                
                for idx, (market, data) in enumerate(market_summary.items()):
                    with market_cols[idx]:
                        profit_pct = (data['total_profit'] / (data['total_value'] - data['total_profit']) * 100) if (data['total_value'] - data['total_profit']) > 0 else 0
                        st.metric(
                            market,
                            f"{data['count']}只",
                            delta=f"{profit_pct:+.2f}%"
                        )
                        st.caption(f"市值: ¥{data['total_value']:,.0f}")
                
                st.markdown("---")
                
                # AI分析选择
                analysis_option = st.radio(
                    "选择分析类型",
                    options=["📊 分市场组合分析", "🎯 单只股票深度分析"],
                    horizontal=True,
                    key="portfolio_analysis_type"
                )
                
                if analysis_option == "📊 分市场组合分析":
                    # 选择要分析的市场
                    available_markets = list(market_summary.keys())
                    selected_market = st.selectbox(
                        "选择市场进行AI组合分析",
                        options=available_markets,
                        key="portfolio_market_select"
                    )
                    
                    if st.button("🚀 启动市场组合分析", type="primary", key="portfolio_market_ai_btn", width='stretch'):
                        if MY_GEMINI_KEY:
                            with _v88_running(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · {selected_market}持仓组合"):
                                try:
                                    market_data = market_summary[selected_market]
                                    stocks_info = []
                                    
                                    # 收集该市场的所有持仓信息
                                    for _, row in market_data['stocks'].iterrows():
                                        stock_info = {
                                            '代码': row['股票代码'],
                                            '名称': row['股票名称'],
                                            '持仓数量': row['持仓数量'],
                                            '买入价': row['买入价格'],
                                            '当前价': row['当前价格'],
                                            '盈亏': f"{row['盈亏比例']:.2f}%" if not pd.isna(row['盈亏比例']) else 'N/A',
                                            '市值': f"¥{row['持仓市值']:,.0f}",
                                            '市值占比': f"{row['持仓市值'] / market_data['total_value'] * 100:.1f}%"
                                        }
                                        stocks_info.append(stock_info)
                                    
                                    # 生成AI分析提示词
                                    prompt = f"""作为专业投资顾问，请分析以下{selected_market}持仓组合：

【组合概况】
- 持仓股票数: {market_data['count']}只
- 总市值: ¥{market_data['total_value']:,.2f}
- 总盈亏: ¥{market_data['total_profit']:,.2f}
- 盈亏比例: {market_data['total_profit'] / (market_data['total_value'] - market_data['total_profit']) * 100:.2f}%

【持仓明细】
"""
                                    for stock in stocks_info:
                                        prompt += f"\n{stock['名称']}({stock['代码']}): 持仓{stock['持仓数量']}股, 成本{stock['买入价']}, 现价{stock['当前价']}, 盈亏{stock['盈亏']}, 市值{stock['市值']} (占比{stock['市值占比']})"
                                    
                                    prompt += f"""

请从以下维度进行专业分析：

## 📊 组合结构分析
1. 仓位配置是否合理？是否过于集中？
2. 行业分散度如何？（根据股票名称判断）
3. 单只股票占比是否合适？（建议单只不超过20%）

## 💰 盈亏表现分析
1. 整体盈亏情况评价（优秀/良好/一般/较差）
2. 哪些股票贡献了主要收益？
3. 哪些股票拖累了组合表现？

## 🎯 持仓建议
1. 建议增持的股票及理由
2. 建议减持的股票及理由
3. 建议止盈/止损的股票及价位

## ⚖️ 风险评估
1. 组合整体风险等级（低/中/高）
2. 主要风险点
3. 风险控制建议

## 🔮 后市展望
1. {selected_market}市场短期展望（1-2周）
2. 该组合在当前市场环境下的适应性
3. 未来1-2个月的操作策略

请提供专业、具体、可操作的分析建议，字数600-800字。"""
                                    
                                    # 调用Gemini API
                                    ai_response = call_gemini_api(prompt)
                                    
                                    # 显示分析结果
                                    st.success(f"✅ {selected_market}持仓组合分析完成")
                                    
                                    # 显示组合信息
                                    with st.expander("📊 查看持仓明细", expanded=False):
                                        st.dataframe(
                                            pd.DataFrame(stocks_info),
                                            width='stretch',
                                            hide_index=True
                                        )
                                    
                                    # 显示AI分析
                                    st.markdown("---")
                                    st.markdown("##### 🤖 AI组合分析报告")
                                    # 【V90.3】段落级复制
                                    if COPY_UTILS_AVAILABLE:
                                        CopyUtils.render_markdown_with_section_copy(ai_response, key_prefix=f"port_{selected_market}")
                                    else:
                                        st.markdown(ai_response)
                                    st.caption(f"📌 本报告由 AI 生成 · 模型: {_ai_model_label()}")
                                
                                except Exception as e:
                                    st.error(f"❌ AI分析失败: {str(e)[:100]}")
                                    logging.error(f"持仓组合AI分析异常: {e}")
                        else:
                            st.warning("⚠️ 请配置DeepSeek API Key以使用AI分析功能")
                
                else:  # 单只股票深度分析
                    # 原有的单只股票分析
                    analyze_options = [f"{row['股票名称']} ({row['股票代码']})" 
                                      for _, row in portfolio_df.iterrows()]
                    
                    selected_stock = st.selectbox("选择股票进行深度分析", 
                                                 options=analyze_options,
                                                 key="portfolio_single_stock_select")
                    
                    if selected_stock and st.button("🚀 启动深度分析", type="primary", key="portfolio_single_stock_btn", width='stretch'):
                        # 提取股票代码
                        import re
                        match = re.search(r'\(([^)]+)\)', selected_stock)
                        if match:
                            selected_code = match.group(1)
                            st.session_state.scan_selected_code = selected_code
                            st.session_state.scan_selected_name = selected_stock.split('(')[0].strip()
                            st.toast(f"🎯 已选中: {selected_stock}，请向上滚动查看作战室", icon="🎯")
                            st.info("👆 **请向上滚动到「⚔️ 深度作战室」（模块①）查看完整AI分析报告**")
    
    except Exception as e:
        st.error(f"❌ 持仓管理加载异常: {str(e)[:100]}")
        logging.error(f"持仓管理异常: {e}")
  except Exception as e:
    st.warning(f"⚠️ 持仓模块异常: {str(e)[:80]}")
    logging.error(f"持仓模块渲染异常: {e}")

# 旧位置不再直接渲染，在主内容区域通过 _render_portfolio_section() 调用

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

    # 【V92 修复】扫描前预热行情：美股/港股批量下载(避免限流只取到A段)，A股 Tushare
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

    # 【V92 修复】扫描前预热行情：美股/港股批量下载(避免限流只取到A段)，A股 Tushare
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
                is_hit = False
                
                if scan_type == "TOP":
                    if m['score'] > 40: is_hit = True
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
                    
                    if m['score'] > min_score:
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
                    score = m['score']
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
                    
                    _display_score = m.get('_special_score', m['score'])
                    _signals_str   = " ".join(m.get('_special_signals', []))
                    _setup_str     = m.get('_special_setup', m['suggestion'])
                    results.append({
                        "股票": name,
                        "代码": code,
                        "行业": get_sector(code, name),
                        "得分": _display_score,
                        "ESG": f"{m.get('esg_total', 0)} ({m.get('esg_grade', 'N/A')})",
                        "长期": long_term,
                        "短期": short_term,
                        "建议": _setup_str if scan_type in ("COIL", "BREAKOUT") else m['suggestion'],
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
#       导致只取到字母A段、后面全无数据），A股走 Tushare 直连。
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
    """扫描前预热行情缓存：美股/港股批量下载，A股 Tushare 并发逐只。"""
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
                    _bump(1, "⚡预取(A股·Tushare)")
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
            invalid = f"收盘跌破MA55({ma[55]:.2f})，趋势失效"
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

    # ── 预热缓存：美股/港股批量下载（避免限流只取到A段），A股 Tushare ──
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
            if not m or m['score'] <= 35:
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

            score = int(m['score'])
            _bear = str(regime_str).upper().startswith("BEAR")

            # 【V94.3】操作指引与止损/目标：统一决策函数（与个股搜索共用，口径一致）
            # 【V88·时机闸门】趋势结论复用评分内核已算好的（评分本身已做时机压分）
            action, stop_target = build_action_guidance(
                score, m.get('rs20'), pos_pct, touch_count, last_close,
                m.get('trade_plan'), regime_str, trend=m.get('trend_full'))

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
                "推荐理由": _mk_reason(m, score),
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
            if not m or m['score'] < 40:
                continue
            mdf = m['df']
            last = float(m['last_price'])
            score = int(m['score'])
            rs = m.get('rs20')

            # 【V88·三期限引擎】三期限可买性评分：三端唯一实现 cloud_engine.horizon_scores
            # （因子权重表+逻辑链全透明，末端含 V88 时机闸门），公式不再本地重复
            import cloud_engine as _ce101
            _icode = "000001.SS" if mkt == "A股" else ("^HSI" if mkt == "港股" else "^GSPC")
            try:
                _idf101 = fetch_stock_data(_icode)
                _iclose = _idf101['Close'] if _idf101 is not None else None
            except Exception:
                _iclose = None
            _hs = _ce101.horizon_scores(mdf, idx_close=_iclose, full=m.get('trend_full'))
            if not _hs:
                continue
            _gate = f"｜{_hs['gate_note']}" if _hs.get('gate_note') else ""

            l250 = float(mdf['Low'].tail(250).min())
            h250 = float(mdf['High'].tail(250).max())
            pos = (last - l250) / (h250 - l250) * 100 if h250 > l250 else 50.0
            act, stp = build_action_guidance(score, rs, pos, 0, last, m.get('trade_plan'),
                                             trend=m.get('trend_full'))

            rows.append({
                "市场": mkt, "代码": code, "名称": name,
                "现价": f"{last:.2f}", "综合分": score,
                "RS强度": (f"{rs:+.1f}" if rs is not None else "—"),
                "20日动量": f"{m.get('chg20d', 0) or 0:+.1f}%",
                "操作指引": act, "止损/目标": stp,
                "_s": _hs['short']['score'], "_m": _hs['mid']['score'], "_l": _hs['long']['score'],
                "_why_s": _hs['short']['why'] + _gate,
                "_why_m": _hs['mid']['why'] + _gate,
                "_why_l": _hs['long']['why'] + _gate,
                "_plan_s": _hs['short'].get('plan', ''),
                "_plan_m": _hs['mid'].get('plan', ''),
                "_plan_l": _hs['long'].get('plan', ''),
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

    # 【V92 修复】扫描前预热行情：美股/港股批量下载(避免限流只取到A段)，A股 Tushare
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
            if not m or m['score'] <= score_threshold:
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
            if not qr["pass"] and qr["data_quality_flag"] == "FAIL":
                continue

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
            if vt_check.get("is_value_trap"):
                stats['failed'] += 1
                continue

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
            score = m['score']
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
                "得分": m['score'], "ESG": f"{m.get('esg_total', 0)} ({m.get('esg_grade', 'N/A')})",
                "硬事实校验": qa_label,
                "长期": long_term, "短期": short_term, "建议": m['suggestion'],
                "策略": m['logic'], "资金": capital, "水位": water_str,
                "现价": f"{last_price:.2f}",
                "动作标签": f"{action_emoji} {action_label}",
                "机会概率": f"{risk_probs['p_up_continuation']*100:.0f}%",
                "风险概率": f"{risk_probs['p_drawdown']*100:.0f}%",
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
    sorted_results = sorted(results, key=lambda x: x.get('regime_adjusted_score', x['得分']), reverse=True)[:top_n]
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
    """
    一键AI选股：扫描中美港三市场，取每市场前15只候选，由Gemini选出各市场Top3，
    输出：理由、背景、增长点（短中长期）
    返回: (result_dict, error_msg)
    result_dict: {'us': [], 'hk': [], 'cn': [], 'ai_report': str}
    """
    def _update(msg):
        if progress_callback:
            progress_callback(msg)
    
    result = {'us': [], 'hk': [], 'cn': [], 'ai_report': ''}
    
    # 1. 三市场并行扫描，各取 Top15 候选
    markets_data = [
        ("美股", RAW_US),
        ("港股", RAW_HK),
        ("A股", RAW_CN_TOP),
    ]
    
    all_candidates = {}
    for idx, (market_name, pool) in enumerate(markets_data):
        def _make_progress(mkt):
            def _cb(c, t, name):
                _update(f"正在扫描 {mkt}... {c}/{t} {name[:12]}")
            return _cb
        _update(f"正在扫描 {market_name}...")
        try:
            res, stats = batch_scan_analysis(pool, scan_type="TOP", ma_target=None, progress_callback=_make_progress(market_name))
            # 按得分排序，取前15
            sorted_res = sorted(res, key=lambda x: x.get('得分', 0), reverse=True)[:15]
            all_candidates[market_name] = sorted_res
        except Exception as e:
            logging.error(f"AI选股扫描 {market_name} 失败: {e}")
            all_candidates[market_name] = []
    
    # 2. 构建 Gemini 输入
    _update("正在构建 AI 分析数据...")
    prompt_data = []
    for mkt, candidates in all_candidates.items():
        if not candidates:
            prompt_data.append(f"\n【{mkt}】无有效候选")
            continue
        lines = [f"\n【{mkt}】"]
        for i, r in enumerate(candidates[:15], 1):
            name = r.get('股票', r.get('名称', 'N/A'))
            code = r.get('代码', 'N/A')
            score = r.get('得分', 0)
            sector = r.get('行业', 'N/A')
            suggestion = r.get('建议', '')[:80]
            lines.append(f"  {i}. {name}({code}) 得分:{score} 行业:{sector} 建议:{suggestion}")
        prompt_data.append("\n".join(lines))
    
    input_summary = "\n".join(prompt_data)
    
    # 3. 调用 Gemini
    _update(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · AI选股...")
    prompt = f"""你是顶级量化分析师，根据以下三市场量化扫描候选（每市场前15只，按得分排序），为每个市场选出 **Top 3 最值得关注** 的股票。

【候选数据】
{input_summary}

【任务要求】
对每个市场（美股、港股、A股）各选出 Top 3 只股票，综合短中长期考量。对每只股票必须输出：
1. **选股理由**：为何入选，核心逻辑（1-2句）
2. **背景概况**：公司/行业背景（1-2句）
3. **增长点**：分别说明短期(1-4周)、中期(1-3月)、长期(3-12月)主要增长驱动

【输出格式】（严格按以下 Markdown 结构，便于解析）
## 🇺🇸 美股 Top3
### 1. [股票名](代码)
- **理由**：...
- **背景**：...
- **增长点**：短期... | 中期... | 长期...

### 2. ...
### 3. ...

## 🇭🇰 港股 Top3
### 1. ...
### 2. ...
### 3. ...

## 🇨🇳 A股 Top3
### 1. ...
### 2. ...
### 3. ...

要求：内容专业、具体、可操作，每只股票分析 80-150 字。"""
    
    ai_report = ""
    if MY_GEMINI_KEY:
        try:
            ai_report = call_gemini_api(prompt)
            if ai_report.startswith("❌"):
                return result, ai_report
        except Exception as e:
            err = f"❌ Gemini 调用失败: {type(e).__name__}: {str(e)[:80]}"
            logging.error(err)
            return result, err
    else:
        return result, "❌ 未配置 DeepSeek API Key"
    
    result['ai_report'] = ai_report or "无输出"
    
    # 4. 简单解析：提取每市场 Top3 代码（用于匹配表格）
    import re
    for mkt_tag, mkt_key in [("美股", "us"), ("港股", "hk"), ("A股", "cn")]:
        candidates = all_candidates.get(mkt_tag, [])
        if not candidates:
            continue
        # 从 AI 报告中提取提到的股票名
        for r in candidates[:5]:  # 只看前5，AI 通常从里面选
            name = r.get('股票', r.get('名称', ''))
            if name and name in ai_report:
                result[mkt_key].append(r)
                if len(result[mkt_key]) >= 3:
                    break
    
    return result, None


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
    """
    自选股分析：按中美港划分，对每只逐只给出近期催化、技术面、风险点、操作建议。
    注入V88扫描信号，强制差异化（加仓/减仓/持仓/观望），禁止全部观望。
    返回: (ai_report_str, error_msg)
    """
    def _update(msg):
        if progress_callback:
            progress_callback(msg)
    
    # 1. 获取现价
    _update("正在获取自选股现价...")
    price_lines = []
    for mkt, pfx, key in [("美股", "$", "US"), ("港股", "HK$", "HK"), ("A股", "¥", "CN")]:
        lines = [f"\n【{mkt}】"]
        for code, name in WATCHLIST.get(key, []):
            p = _get_watchlist_price(code)
            s = f"  {name}({code}): {pfx}{p:.2f}" if p is not None else f"  {name}({code}): 数据获取中"
            lines.append(s)
        price_lines.append("\n".join(lines))
    
    input_data = "\n".join(price_lines)
    
    # 2. 获取V88扫描信号（与钉钉日报同源）
    _update("正在读取V88量化扫描信号...")
    scan_sigs = _get_watchlist_scan_signals()
    scan_block_lines = []
    for mkt, key in [("美股", "US"), ("港股", "HK"), ("A股", "CN")]:
        in_scan = []
        for code, name in WATCHLIST.get(key, []):
            c = str(code).upper().strip()
            if c in scan_sigs:
                lbl, reason, _ = scan_sigs[c]
                in_scan.append(f"{name}({code})【{lbl}】{reason}")
        if in_scan:
            em = "🇺🇸" if key == "US" else ("🇭🇰" if key == "HK" else "🇨🇳")
            scan_block_lines.append(f"- {em} {mkt}：{'；'.join(in_scan)}")
    scan_block = "\n".join(scan_block_lines) if scan_block_lines else "今日无持仓进榜"

    # 3. 调用 Gemini
    _update(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · 自选股分析...")
    prompt = f"""你是顶级量化分析师，对以下用户的跨账户自选股进行逐只分析。

【自选股及现价】
{input_data}

【V88量化扫描信号】以下持仓今日进入扫描榜（强势=趋势向好，蓄势=未启动，拐点=弱势反转）：
{scan_block}

【任务要求】
按中美港划分，对每只自选股**逐只**给出：
1. **近期催化**：24-72h 内可能影响股价的事件或数据
2. **技术面**：关键支撑/压力、趋势判断
3. **风险点**：1-2 条主要风险
4. **操作建议**：持有/加仓/减仓/观望（简洁可执行）
5. **简要理由**：基本面、技术面各一句，每句不超过20个字；无数据须直说

【操作规则】⚠️ 必须差异化，禁止全部或多数为观望：
- 📈加仓：强势进榜+逻辑支持、或蓄势突破+催化明确，至少1-2只
- 📉减仓：拐点进榜、技术破位、估值过高、基本面恶化，至少1只
- 📌持仓：逻辑未变、继续持有
- 🔍观望：短期不明朗、等待信号，不超过半数

【输出格式】（严格按以下 Markdown 结构）
## 🇺🇸 美股自选
### 1. [股票名](代码)
- **催化**：...
- **技术面**：...
- **风险**：...
- **基本面理由**：...（≤20字）
- **技术面理由**：...（≤20字）
- **建议**：持有/加仓/减仓/观望

### 2. ...
（逐只分析至第11只，含TSLA）

## 🇭🇰 港股自选
### 1. ...
### 2. ...
（逐只分析 4 只）

## 🇨🇳 A股自选
### 1. ...
### 2. ...
（逐只分析 3 只）

要求：每只 2-4 句，简洁可执行，避免空泛套话；操作建议必须差异化。"""
    
    if not MY_GEMINI_KEY:
        return "", "❌ 未配置 DeepSeek API Key"
    
    try:
        ai_report = call_gemini_api(prompt)
        if ai_report.startswith("❌"):
            return "", ai_report
        return ai_report or "无输出", None
    except Exception as e:
        err = f"❌ 自选股分析失败: {type(e).__name__}: {str(e)[:80]}"
        logging.error(err)
        return "", err


# ═══════════════════════════════════════════════════════════════
# 9. 【V89.6.2】注释：call_gemini_api已在前面定义（2815行）
# ═══════════════════════════════════════════════════════════════
# call_gemini_api函数已提前定义，确保所有模块都能正常调用

# 【V89.4】绑定舆情分析器的AI调用函数
if SENTIMENT_ANALYZER_AVAILABLE and _sentiment_analyzer:
    _sentiment_analyzer.call_ai = call_gemini_api

# ═══════════════════════════════════════════════════════════════
# 10. Session State 初始化
# ═══════════════════════════════════════════════════════════════
if 'proxy_port' not in st.session_state: st.session_state.proxy_port = _detect_system_proxy_port()
if 'scan_selected_code' not in st.session_state: st.session_state.scan_selected_code = None
if 'scan_selected_name' not in st.session_state: st.session_state.scan_selected_name = None
if 'trigger_analysis' not in st.session_state: st.session_state.trigger_analysis = False
# 【V87.7】全局对比篮
if 'compare_basket' not in st.session_state: st.session_state.compare_basket = []  # [(code, name), ...]
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
    # 📱 手机端访问地址（与电脑同一 WiFi）
    try:
        import socket as _sock
        _lan_ip = "本机IP"
        _s = _sock.socket(_sock.AF_INET, _sock.SOCK_DGRAM)
        _s.connect(("8.8.8.8", 80))
        _lan_ip = _s.getsockname()[0]
        _s.close()
    except Exception:
        _lan_ip = "本机IP"
    _mobile_url = f"http://{_lan_ip}:8501"
    with st.expander("📱 手机端访问", expanded=False):
        st.markdown(f"**同一 WiFi 下手机浏览器打开：**")
        st.code(_mobile_url, language=None)
        st.caption("💡 行业热力 → 勾选「📱卡片」可筛选 · 扫描结果可按首字母/得分筛选")
        st.caption("⚠️ 若打不开：启动脚本需带 `--server.address 0.0.0.0`（run_app.sh 已配置）")
    st.divider()
    
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
    st.markdown('[📰 跳转 AI市场简报](#ai-market-brief)')
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
                r = requests.get("https://www.google.com", timeout=5, verify=False)
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
    
    # 【V87】刷新股票池
    st.markdown("### 🔄 股票池管理")
    col_pool1, col_pool2 = st.columns(2)
    
    with col_pool1:
        if st.button("🔄 刷新股票池", width='stretch', help="重新从云端获取最新股票列表"):
            with _v88_running("正在刷新股票池..."):
                # 清除缓存
                st.cache_data.clear()
                # 重新加载
                st.rerun()
    
    with col_pool2:
        if st.button("🗑️ 清除全部缓存", width='stretch'):
            st.cache_data.clear()
            st.success("✅ 缓存已清除")
            time.sleep(0.5)
            st.rerun()

# ═══════════════════════════════════════════════════════════════
# 12. 主界面 - 标题（修复遮挡）
# ═══════════════════════════════════════════════════════════════
st.markdown(
    '''<style>
    div[data-testid="stElementContainer"]:has(.v88-mini-brand){
        margin-top:-1.75rem!important;margin-bottom:-.55rem!important;min-height:10px!important;
    }
    .v88-mini-brand{text-align:left;margin:0!important;padding:0!important;color:#7b8798;
        font-size:9px!important;line-height:1!important;white-space:nowrap;}
    </style><div class="v88-mini-brand">👑 V88 · 同源行情 / AI日报 / 持仓决策</div>''',
    unsafe_allow_html=True
)
# 【V90.7】选中股票后置顶提示
if st.session_state.get('scan_selected_code'):
    st.markdown('<div style="background: linear-gradient(135deg, #10b981 0%, #059669 100%); padding: 1rem; border-radius: 8px; margin-bottom: 1rem; color: white; font-size: 12px; font-weight: 600; text-align: center;">🎯 深度分析报告已生成，请向下滚动查看「⚔️ 深度作战室」完整内容</div>', unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# 【V89.8 布局重构】模块分隔函数
# ═══════════════════════════════════════════════════════════════
def _module_header(icon, title, subtitle="", color_from="#667eea", color_to="#764ba2", compact=False):
    """统一的模块标题样式 - compact=True 时窄边化显示"""
    from datetime import datetime as _dt_hdr
    _weekday_cn = {"Monday": "周一", "Tuesday": "周二", "Wednesday": "周三", "Thursday": "周四", "Friday": "周五", "Saturday": "周六", "Sunday": "周日"}
    _today_display = _dt_hdr.now().strftime("%Y-%m-%d") + " " + _weekday_cn.get(_dt_hdr.now().strftime("%A"), "")
    if compact:
        # 上下变窄：标题+副标题同一行，日期单独一行
        title_line = f"{icon} {title}" + (f" · {subtitle}" if subtitle else "")
        st.markdown(f'''<div style="background: linear-gradient(135deg, {color_from} 0%, {color_to} 100%); 
            padding: 0.4rem 1rem; border-radius: 8px; margin: 1rem 0 0.8rem 0; width: 100%;">
            <div style="color: white; text-align: center; font-size: 12px; font-weight: 700; margin: 0;">{title_line}</div>
            <div style="color: rgba(255,255,255,0.7); text-align: center; font-size: 11px; margin: 0.15rem 0 0 0;">📅 {_today_display}</div>
        </div>''', unsafe_allow_html=True)
    else:
        sub_html = f'<p style="color: rgba(255,255,255,0.85); margin: 0.3rem 0 0 0; text-align: center; font-size: 12px;">{subtitle}</p>' if subtitle else ''
        st.markdown(f'''<div style="background: linear-gradient(135deg, {color_from} 0%, {color_to} 100%); 
            padding: 1.2rem; border-radius: 10px; margin: 1.5rem 0 1rem 0;">
            <h3 style="color: white; margin: 0; text-align: center; font-size: 14px; font-weight: 700;">{icon} {title}</h3>
            {sub_html}
            <p style="color: rgba(255,255,255,0.6); margin: 0.4rem 0 0 0; text-align: center; font-size: 12px;">📅 数据日期: {_today_display}</p>
        </div>''', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════
# 14. 【V78关键修复】深度作战室 - 独立于所有tabs之外
# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# 【V96】首页·今日导航：回答"我今天该关注/买什么"
# 数据全部来自本地日报/快照文件（每日3次自动更新），零网络请求、秒开、无闪烁
# ═══════════════════════════════════════════════════════════════


def _linkify_md(md: str) -> str:
    """【V88·全局个股可点击 v2】两件事：①个股名/token→内联链接（?q=深链）
    ②markdown表格整体转HTML表格——md表格单元格内的HTML前端渲染不可靠，HTML表格100%可点。"""
    import re as _re
    A = '<a href="?q={c}" target="_self" style="color:#1e3a5f;text-decoration:underline;cursor:pointer;font-weight:600">{t}</a>'

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
def _stk_link(name, code):
    """【V88·内联可点个股】不改字体字号，名字即链接（?q=深链→自动深度分析+入观察池）"""
    return f'<a href="?q={code}" target="_self" style="color:#1e3a5f;text-decoration:underline;cursor:pointer;font-weight:600">{name}</a>'


def _render_today_nav():
    _repo = Path.home() / "Desktop" / "ai-daily-report-v2"
    # ?q= 深链：点击任何内联个股名到达这里
    try:
        _q0 = st.query_params.get("q")
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

    # 【V88·非交易日判定】周末/节假日：无"今日盘中"，改看"下一交易日前瞻"
    def _v88_is_trading_day(_d=None):
        from datetime import datetime as _dtt
        _d = _d or _dtt.now().date()
        if _d.weekday() >= 5:
            return False
        try:
            _hol = {ln.strip().split()[0] for ln in (_repo / "holidays.txt").read_text(encoding="utf-8").splitlines()
                    if ln.strip() and not ln.strip().startswith("#")}
            if _d.strftime("%Y-%m-%d") in _hol:
                return False
        except Exception:
            pass
        return True

    _is_trading = _v88_is_trading_day()

    # 【V99.7】及时性保障：快照/日报文件超过1小时 → 后台自动重跑生成流水线
    # （launchd 定时之外的兜底：只要打开 V88 就能触发，1小时节流防重复）
    # 交易日兜底跑完整流水线；非交易日兜底只生成前瞻 outlook.md
    _stale_note = ""
    try:
        _watch_fp = (_repo / "data" / ("daily_report.md" if _is_trading else "outlook.md"))
        _mts = [fp.stat().st_mtime for fp in (_repo / "data" / "market_snapshot.json", _watch_fp) if fp.exists()]
        _age = (time.time() - max(_mts)) if _mts else None
        _thresh = 3600 if _is_trading else 6 * 3600  # 非交易日前瞻低频，6小时兜底足矣
        if _age is None or _age > _thresh:
            _mk99 = SCAN_CACHE_DIR / "nav_refresh_last.txt"
            _last99 = 0.0
            if _mk99.exists():
                try:
                    _last99 = float(_mk99.read_text().strip() or 0)
                except Exception:
                    _last99 = 0.0
            if time.time() - _last99 > _thresh:
                SCAN_CACHE_DIR.mkdir(parents=True, exist_ok=True)
                _mk99.write_text(str(time.time()))
                import subprocess as _sp99
                if _is_trading:
                    _sp99.Popen(["/bin/bash", str(_repo / "run_trading_day_push.sh")],
                                stdout=_sp99.DEVNULL, stderr=_sp99.DEVNULL, start_new_session=True)
                else:
                    _env99 = dict(os.environ, FEISHU_WEBHOOK="")  # 页面兜底不重复推飞书
                    _sp99.Popen(["python3", str(_repo / "src" / "outlook_report.py")],
                                cwd=str(_repo), env=_env99,
                                stdout=_sp99.DEVNULL, stderr=_sp99.DEVNULL, start_new_session=True)
            _stale_note = (" ｜ ⏳ 数据已超时，正在后台重新生成（约1-2分钟，稍后刷新页面即最新）"
                           if _is_trading else " ｜ ⏳ 前瞻正在后台生成（约1-2分钟，稍后刷新）")
    except Exception:
        pass

    st.markdown("### 🧭 今日导航 · 该关注什么" if _is_trading else "### 🔮 下一交易日前瞻 · 非交易日看这里")

    # 【V88·Plan A/B统一标注】与下方AI简报模块共用同一状态，避免"这里显示分数、简报说数据不可信"的割裂
    _pab_status = _rep_planab_meta.get("status")
    if _pab_status == "plan_b":
        _pab_ts = _rep_planab_meta.get("ts")
        _pab_str = datetime.fromtimestamp(_pab_ts).strftime("%m-%d %H:%M") if _pab_ts else "—"
        _pab_issues = _rep_planab_meta.get("today_issues") or []
        st.warning(f"🟡 Plan B当日安全版（{'；'.join(_pab_issues) or 'Plan A条件不足'}）· {_pab_str}生成。"
                  "内容来自今日新闻、今日快照及今日引擎榜单；所有标的仅作观察，不沿用历史日报。")
    elif _pab_status == "missing":
        st.error("📭 今日Plan A与当天安全Plan B均未生成，下方暂无操作榜/评分数据，请等待本轮流水线完成。")

    # 【V88·非交易日前瞻置顶】把 outlook.md 前瞻正文醒目展示（个股名可点深度分析）
    if not _is_trading:
        _outlook_md = ""
        try:
            _outlook_md = (_repo / "data" / "outlook.md").read_text(encoding="utf-8")
        except Exception:
            _outlook_md = ""
        if _outlook_md.strip():
            st.markdown(_linkify_md(_outlook_md), unsafe_allow_html=True)
            with st.popover("📋 复制前瞻"):
                st.code(_outlook_md, language=None)
        else:
            st.info("🔮 下一交易日前瞻正在生成中，请稍后刷新页面。" + _stale_note)
        st.divider()
        st.caption("下方为最近交易日的行情快照与温度定位（供延续参考）：")
    # 【V88·今日焦点】醒目置顶：重点推荐（引擎买入档）+ 重点观察（搜索过的个股）
    def _tok2code(tok):
        tok = str(tok).strip("`[] ")
        return tok.split(":", 1)[1] if ":" in tok else tok

    try:
        _fxp = []
        _iop = _rep.find("## 🎯 今日操作榜")
        _pb_focus_lines = []
        if _pab_status == "plan_b":
            _pb_sec = _rep[_rep.find("## 六、🔭 明日与本周参考"):]
            _pb_market = ""
            for _pbl in _pb_sec.splitlines():
                if _pbl.startswith("### "):
                    _pb_market = _pbl.replace("### ", "").strip()
                elif "**观察个股**：" in _pbl:
                    _pb_focus_lines.append(f"<b>{_pb_market}·机会观察</b>：{_pbl.split('**：', 1)[-1] if '**：' in _pbl else _pbl.split('：', 1)[-1]}")
                elif "**风险保护**：" in _pbl:
                    _pb_focus_lines.append(f"<b>{_pb_market}·风险保护</b>：{_pbl.split('**：', 1)[-1] if '**：' in _pbl else _pbl.split('：', 1)[-1]}")
        if _iop > 0:
            for _lnf in _rep[_iop:_iop + 4000].splitlines():
                if "买入/建仓" in _lnf and _lnf.strip().startswith("|"):
                    _cf = [x.strip() for x in _lnf.split("|") if x.strip()]
                    if len(_cf) >= 7:
                        _fxp.append((_cf[2], _tok2code(_cf[1]), _cf[3].replace('**', '')[:40],
                                     _cf[6].split("｜")[0].replace("**", "").strip()[:60]))
                if len(_fxp) >= 3:
                    break
        if not _fxp:
            if _pab_status == "plan_b":
                st.info("⭐ **今日策略：不强制给买入指令，转为机会观察＋风险保护**——优先保护仓位、已有利润和整体胜率")
                if _pb_focus_lines:
                    st.markdown("<div style='line-height:1.75;font-size:12px'>" + "<br>".join(_pb_focus_lines) + "</div>", unsafe_allow_html=True)
            else:
                st.info("⭐ **今日无强制买入信号**（未同时通过75分＋72小时催化）——继续观察并保护仓位，现金也是仓位")
        if _fxp:
            _fx_html = "<br>".join(
                f"🟢 <b>{_stk_link(_n9, _c9)}</b> {_d9}<br>&nbsp;&nbsp;└ {_r9}"
                for _n9, _c9, _d9, _r9 in _fxp)
            st.success("**⭐ 今日重点关注（引擎买入档 Top3）** · 点股票名直接深度分析")
            st.markdown(f"<div style='line-height:1.9'>{_fx_html}</div>", unsafe_allow_html=True)
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
        _mk_html9 = []
        for _mk3 in ("🇺🇸 美股", "🇨🇳 A股", "🇭🇰 港股"):
            _seen3, _lst3 = set(), []
            for _m3, _s3, _n3, _c3 in sorted([r for r in _rows_mk if r[0] == _mk3], key=lambda x: -x[1]):
                if _c3 in _seen3:
                    continue
                _seen3.add(_c3)
                _lst3.append(f"{_stk_link(_n3, _c3)}({_s3})")
                if len(_lst3) >= 3:
                    break
            if _lst3:
                _mk_html9.append(f"<b>{_mk3}</b>：" + "、".join(_lst3))
        if _mk_html9:
            st.markdown("**🌍 各市场引擎高分榜（操作榜 Top3 · 点名直接深度分析）**")
            st.markdown("<div style='line-height:1.9'>" + "<br>".join(_mk_html9) + "</div>", unsafe_allow_html=True)
        _wl9 = _watchlist_load() or {}
        _obs = []
        for _mk9, _lst9 in _wl9.items():
            for _c9, _n9 in list(_lst9)[-4:]:
                _obs.append((_n9, _c9))
        if _obs:
            _wa9 = st.session_state.get('watch_alerts_v88') or {}
            _obs_html = "、".join(_stk_link(_n9, _c9) for _n9, _c9 in _obs[:12])
            st.markdown(
                f"👁 <b>重点观察个股</b>（搜索/点击即自动加入，移除在「自选股」Tab）：{_obs_html}"
                + (f" ｜ ⚡{len(_wa9.get('alerts') or [])} 条触发（见下方预警）" if _wa9.get('alerts') else ""),
                unsafe_allow_html=True)
        # 显式新增入口：无需先去搜索页，今日导航内即可加入自选。
        _wl_add_c1, _wl_add_c2 = st.columns([5, 1])
        _wl_add_token = _wl_add_c1.text_input(
            "新增自选", placeholder="输入名称或代码，如：英伟达 / 0700.HK / 600519",
            key="_nav_wl_add", label_visibility="collapsed")
        if _wl_add_c2.button("＋ 加入自选", key="_nav_wl_add_btn", use_container_width=True):
            _tok9 = _wl_add_token.strip()
            if not _tok9:
                st.warning("请先输入股票名称或代码")
            else:
                try:
                    import cloud_engine as _ce_add9
                    _cands9 = _ce_add9.search_candidates(_tok9, limit=1) or []
                    if _cands9:
                        _nm_add9, _cd_add9 = _cands9[0][0], _cands9[0][1]
                    else:
                        _cd_add9 = _ce_add9.to_yf(_tok9)
                        _nm_add9 = _ce_add9.name_of(_cd_add9) or _tok9
                    if _watchlist_add(_cd_add9, _nm_add9):
                        st.session_state["_wl_new_pick"] = (_cd_add9, _nm_add9)
                        st.toast(f"已加入自选：{_nm_add9}（{_cd_add9}）", icon="⭐")
                        st.rerun()
                    else:
                        st.info(f"{_nm_add9}（{_cd_add9}）已在自选中")
                except Exception as _add_e9:
                    st.error(f"未识别该股票，请换用准确代码：{str(_add_e9)[:60]}")
    except Exception:
        pass
    _gen = (_snap or {}).get("generated_at", "")
    st.caption(f"💡 不知道买什么先看这里：温度定仓位 → 水位定方向 → 轮动定板块 → 操作榜定标的 → 持仓提醒定纪律 ｜ 数据时间 {_gen}{_stale_note}")

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

    # 【V88·关注股预警】自选股+搜索习惯+持仓 → 拐点/止盈止损/纪律提示
    try:
        _wa = st.session_state.get('watch_alerts_v88')
        if not _wa or _wa.get('rule_version') != 4 or time.time() - _wa.get('ts', 0) > 3600:
            _pool_wa = {}
            _holds_wa = set()
            _claims_wa = {}
            _hold_map_wa = {}
            try:
                for _mk9, _lst9 in (_watchlist_load() or {}).items():
                    for _c9, _n9 in list(_lst9)[:10]:
                        _pool_wa[str(_c9)] = _n9
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
            _risk_holds_wa = _holds_wa | set(_claims_wa)
            import cloud_engine as _ce_wa
            import sys as _sys_wa
            if str(_repo / "src") not in _sys_wa.path:
                _sys_wa.path.insert(0, str(_repo / "src"))
            from watch_alerts import sharp_drop_signal as _sharp_wa, market_change_for as _mchg_wa, watch_levels as _levels_fn_wa
            from position_lifecycle import dynamic_priority as _dyn_level_wa, load_peaks as _load_peaks_wa
            _levels_wa = _levels_fn_wa()
            _peaks_wa = _load_peaks_wa()
            try:  # 【行业热度维度】与飞书/云端同一份冻结快照
                _mkts9 = json.loads((_repo / "data" / "market_snapshot.json").read_text(encoding="utf-8")).get("markets") or {}
            except Exception:
                _mkts9 = {}
            _pool_wa = dict(sorted(_pool_wa.items(), key=lambda kv: (
                0 if kv[0] in _risk_holds_wa else (1 if _levels_wa.get(kv[0], "B") == "A" else 2))))
            _alerts9 = []
            _holding_levels9 = {}
            for _c9, _n9 in list(_pool_wa.items())[:60]:
                try:
                    _df9 = fetch_stock_data(to_yf_cn_code(_c9))
                    _f9 = _ce_wa.analyze_trend_full(_df9)
                    if not _f9:
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
                    if _last9 < _f9["stop"]:
                        if _c9 in _claims_wa:
                            _alerts9.append(f"❗ [已确认持仓·资料待补录] **{_n9}**({_c9})：现价{_last9}已破技术防守位{_f9['stop']}——立即复核仓位；成本/股数待补录")
                        else:
                            _alerts9.append(f"❗ [持仓·A自动] **{_n9}**({_c9})：现价{_last9}已破止损位{_f9['stop']}——纪律：离场/减仓，不要扛")
                    else:
                        if _sharp9:
                            _who9 = "正式持仓" if _c9 in _holds_wa else ("已确认持仓" if _c9 in _claims_wa else "A级重点")
                            _level_tag9 = (f"[持仓·{_dyn9}自动]" if _c9 in _holds_wa else
                                           ("[已确认持仓·资料待补录]" if _c9 in _claims_wa else "[A级重点]"))
                            _alerts9.insert(0, f"{_sharp9['severity']} {_level_tag9} **{_n9}**({_c9})：{_who9}急跌预警｜"
                                            + "＋".join(_sharp9["facts"]) + f"｜{_sharp9['action']}"
                                            + ("｜成本/股数待补录" if _c9 in _claims_wa else ""))
                            continue
                        # 【V88·多因子共振】买入/减仓须≥2维度（技术/量价/消息）共振，单指标不触发（与云端/飞书同源）
                        _sw9 = _ce_wa.smart_watch_signal(_f9, sector_heat=_ce_wa.sector_heat_of(_c9, _n9, _mkts9))
                        if _sw9:
                            _ic9 = "🛒" if _sw9["side"] == "buy" else "⚠️"
                            _hd9 = "触发条件" if _sw9["side"] == "buy" else "风险原因"
                            _alerts9.append(f"{_ic9} **{_n9}**({_c9})：**{_sw9['action']}**｜{_hd9}："
                                            + "＋".join(_sw9["conditions"][:4]) + f"｜{_sw9['zone']}")
                        elif _c9 in _holds_wa and _dyn9 == "A":
                            _alerts9.insert(0, f"⚠️ [持仓·A自动] **{_n9}**({_c9})：{_dyn_reason9}｜"
                                                "优先复核减仓/止损与利润保护")
                except Exception:
                    continue
            _wa = {"ts": time.time(), "alerts": _alerts9, "n": len(_pool_wa), "rule_version": 4,
                   "holding_levels": _holding_levels9}
            st.session_state['watch_alerts_v88'] = _wa
        if _wa.get("alerts"):
            _critical9 = [a for a in _wa["alerts"] if ("急跌预警" in a or "已破止损" in a
                                                               or "顶部拐点" in a or "持仓·A自动" in a
                                                               or "已确认持仓" in a)]
            if _critical9:
                st.error("🚨 **持仓/重点风险优先**\n\n" + "\n\n".join(f"- {a}" for a in _critical9[:8]))
            with st.expander(f"⚡ 自选股智能预警（自选+常搜+持仓 共{_wa['n']}只 · {len(_wa['alerts'])}条触发 · 多因子共振）", expanded=True):
                st.markdown("\n".join(f"- {a}" for a in _wa["alerts"]))
                with st.popover("📋 复制预警"):
                    st.code("\n".join(a.replace("**", "") for a in _wa["alerts"]), language=None)
        else:
            st.caption(f"⚡ 关注股预警：{_wa.get('n', 0)}只关注股暂无拐点/止损触发（1小时自动复查）")
    except Exception as _we9:
        logging.debug(f"关注股预警异常: {_we9}")

    # 【V88·持仓终端】结构化表单+常驻持仓表（录入即落盘私仓 positions.json 并显示——所见即所存）
    with st.expander("💼 持仓终端（简称自动识别全称 · 卖出价留空=买入 · 每笔带成交日期）", expanded=False):
        import sys as _sysf
        if str(_repo / "src") not in _sysf.path:
            _sysf.path.insert(0, str(_repo / "src"))
        import position_manager as _pmf
        if st.session_state.get("_pt_flash"):
            st.success(st.session_state.pop("_pt_flash"))
        _claim_rows9 = _pmf.claimed_holding_rows()
        if _claim_rows9:
            _claim_names9 = "、".join(f"{r['名称']}({r['代码']})" for r in _claim_rows9)
            st.warning(f"⚠️ 已确认持仓·资料待补录：{_claim_names9}。已按持仓优先预警；补齐账户、股数和成本后自动启用浮盈/峰值回撤/个性化止损。")
        # ── 结构化录单表单 ──
        _f1, _f2, _f3, _f4, _f5 = st.columns([2.2, 1.3, 1.3, 1.3, 1.6])
        _pt_name = _f1.text_input("名称/简称/代码", placeholder="腾讯 / 海油 / NVDA", key="_ptf_name")
        _pt_buy = _f2.text_input("买入价", placeholder="469", key="_ptf_buy")
        _pt_sell = _f3.text_input("卖出价(空=买入)", placeholder="", key="_ptf_sell")
        _pt_qty = _f4.text_input("股数", placeholder="100", key="_ptf_qty")
        from datetime import date as _date9
        _pt_date = _f5.date_input("成交日期", value=_date9.today(), key="_ptf_date")
        _pt_reason = st.text_input("原因(选填,随日志留档)", placeholder="如：回踩买点 / 止盈一半", key="_pt_reason_desk")

        def _pt_git_sync(_label):
            import subprocess as _sp9
            _sp9.run(["git", "-C", str(_repo), "add", "-f", "positions.json", "position_claims.json",
                      "journal/trades.json", "watch_levels.json"], capture_output=True, text=True)
            _sp9.run(["git", "-C", str(_repo), "commit", "-m", f"持仓终端(桌面): {_label[:40]}"],
                     capture_output=True, text=True)
            _p9 = _sp9.run(["git", "-C", str(_repo), "push", "origin", "main"], capture_output=True, text=True)
            if _p9.returncode != 0:
                _sp9.run(["git", "-C", str(_repo), "pull", "--rebase", "-X", "theirs", "origin", "main"],
                         capture_output=True, text=True)
                _p9 = _sp9.run(["git", "-C", str(_repo), "push", "origin", "main"], capture_output=True, text=True)
            st.caption("☁️ 已同步私仓（云端/飞书下一轮生效）" if _p9.returncode == 0
                       else "⚠️ 本地已改，私仓推送失败——网络恢复后自动随下次提交带上")

        def _pt_run_form(_kw, _chosen=None):
            _msg9, _needs9 = _pmf.record_trade(chosen_code=_chosen, **_kw)
            if _needs9:  # 简称多解（"腾讯"→腾讯/腾讯控股/腾讯音乐…）→ 弹窗确认
                st.session_state["_pt_pending"] = {"kw": _kw, "cands": _needs9}
                st.rerun()
            if _msg9.startswith(("已录入", "已加仓", "已清仓", "已减仓")):
                _pt_git_sync(f"{_kw.get('token', '')}")
                st.session_state["_pt_flash"] = _msg9 + "　✅已落盘并同步私仓"
                st.rerun()  # 立刻刷新下方持仓表——所见即所存
            else:
                st.error(_msg9)

        if st.button("▶ 记一笔", type="primary", key="_pt_go_desk") and _pt_name.strip():
            try:
                _pt_run_form({"token": _pt_name.strip(), "shares": _pt_qty or 0,
                              "buy_px": float(_pt_buy) if _pt_buy.strip() else None,
                              "sell_px": float(_pt_sell) if _pt_sell.strip() else None,
                              "date": str(_pt_date), "reason": _pt_reason.strip()})
            except ValueError:
                st.error("价格/股数须为数字")
            except Exception as _pe9:
                st.error(f"持仓终端异常: {_pe9}")

        if st.session_state.get("_pt_pending"):
            @st.dialog("该简称有多个匹配，请确认标的")
            def _pt_pick_dialog():
                _pd = st.session_state["_pt_pending"]
                _opts = [f"{nm}（{cd}·{mk}）" for nm, cd, mk in _pd["cands"]]
                _sel = st.selectbox("候选", _opts, key="_pt_pick_sel")
                _c1d, _c2d = st.columns(2)
                if _c1d.button("✅ 确认", type="primary", key="_pt_pick_ok"):
                    _code_sel = _pd["cands"][_opts.index(_sel)][1]
                    st.session_state.pop("_pt_pending")
                    try:
                        _pt_run_form(_pd["kw"], _chosen=_code_sel)
                    except Exception as _pe9:
                        st.error(f"持仓终端异常: {_pe9}")
                if _c2d.button("✕ 取消", key="_pt_pick_no"):
                    st.session_state.pop("_pt_pending")
                    st.rerun()
            _pt_pick_dialog()

        # ── 常驻持仓表+最近交易（读盘实时渲染，刷新页面不丢——这就是"记忆"）──
        try:
            _rows_pt = _pmf.holdings_rows()
            if _rows_pt:
                _pt_codes = tuple(dict.fromkeys(str(r.get("代码", "")).upper() for r in _rows_pt if r.get("代码")))
                with _v88_running("计算持仓历史最高水位"):
                    _pt_water = _ath_many_display(_pt_codes) if _pt_codes else {}
                    _pt_px = _last_px_many(_pt_codes) if _pt_codes else {}
                # 【2026-07-12 用户要求】新增现价/盈亏列 + 表尾整体盈亏汇总（红涨绿跌）
                _pt_ccy = {"🇨🇳A股": "¥", "🇭🇰港股": "HK$", "🇺🇸美股": "$"}
                _pt_totals = {}  # 市场 → [盈亏额, 成本额]
                def _pnl_html(_amt, _pct=None):
                    _c = "#dc2626" if _amt >= 0 else "#16a34a"
                    _t = f"{_amt:+,.0f}" + (f"（{_pct:+.1f}%）" if _pct is not None else "")
                    return f'<span style="color:{_c};font-size:13px">{_t}</span>'
                # 自定义逐行渲染，提供与自选股一致的 × 删除入口。
                _pt_widths = [1.05, 1.2, 1, .7, .7, .65, .75, 1.05, 1.35, .65]
                _ph = st.columns(_pt_widths)
                for _hc, _ht in zip(_ph, ("账户", "名称", "代码", "股数", "成本", "类别", "现价", "盈亏", "历史水位", "操作")):
                    _hc.caption(_ht)
                for _ri9, _row9 in enumerate(_rows_pt):
                    _edit_id9 = f"{_row9.get('账户')}|{_row9.get('代码')}"
                    _row_water9 = _pt_water.get(str(_row9.get("代码", "")).upper(), "历史水位待核")
                    # 逐行盈亏：现价来自 _last_px_many（与水位同一份历史数据，1小时缓存）
                    _row_px9 = _pt_px.get(str(_row9.get("代码", "")).upper())
                    _row_mkt9 = market_of_code(str(_row9.get("代码", "")))
                    _row_ccy9 = _pt_ccy.get(_row_mkt9, "")
                    _px_txt9, _pnl_txt9 = "—", "—"
                    try:
                        _sh9 = float(str(_row9.get("股数", "")).replace(",", ""))
                        _co9 = float(str(_row9.get("成本", "")).replace(",", ""))
                        if _row_px9 and _co9 > 0 and _sh9 > 0:
                            _pnl_amt9 = (_row_px9 - _co9) * _sh9
                            _pnl_pct9 = (_row_px9 / _co9 - 1) * 100
                            _px_txt9 = f"{_row_px9:,.3f}" if _row_px9 < 10 else f"{_row_px9:,.2f}"
                            _pnl_txt9 = _pnl_html(_pnl_amt9, _pnl_pct9)
                            _tot9 = _pt_totals.setdefault(_row_mkt9, [0.0, 0.0])
                            _tot9[0] += _pnl_amt9
                            _tot9[1] += _co9 * _sh9
                    except (TypeError, ValueError):
                        pass
                    if st.session_state.get("_pt_edit_row") == _edit_id9:
                        _pc = st.columns(_pt_widths)
                        _ev_acc9 = _pc[0].text_input("账户", value=str(_row9.get("账户", "")), key=f"_pe_acc_{_ri9}", label_visibility="collapsed")
                        _ev_nm9 = _pc[1].text_input("名称", value=str(_row9.get("名称", "")), key=f"_pe_nm_{_ri9}", label_visibility="collapsed")
                        _ev_cd9 = _pc[2].text_input("代码", value=str(_row9.get("代码", "")), key=f"_pe_cd_{_ri9}", label_visibility="collapsed")
                        _ev_sh9 = _pc[3].text_input("股数", value=str(_row9.get("股数", "")), key=f"_pe_sh_{_ri9}", label_visibility="collapsed")
                        _ev_co9 = _pc[4].text_input("成本", value=str(_row9.get("成本", "")), key=f"_pe_co_{_ri9}", label_visibility="collapsed")
                        _ev_cl9 = _pc[5].text_input("类别", value=str(_row9.get("类别", "")), key=f"_pe_cl_{_ri9}", label_visibility="collapsed")
                        _pc[6].caption(_px_txt9)
                        _pc[7].markdown(_pnl_txt9, unsafe_allow_html=True)
                        _pc[8].caption(_row_water9)
                        _save_col9, _cancel_col9 = _pc[9].columns(2)
                        if _save_col9.button("✓", key=f"_pt_save_{_ri9}", help="保存修改"):
                            _msg_edit9 = _pmf.update_holding(
                                _row9.get("账户", ""), _row9.get("代码", ""), account=_ev_acc9,
                                name=_ev_nm9, code=_ev_cd9, shares=_ev_sh9, cost=_ev_co9, category=_ev_cl9)
                            if _msg_edit9.startswith("已修改"):
                                st.session_state.pop("_pt_edit_row", None)
                                _pt_git_sync(_msg_edit9)
                                st.session_state["_pt_flash"] = _msg_edit9 + "　✅已落盘并同步私仓"
                                st.rerun()
                            else:
                                st.error(_msg_edit9)
                        if _cancel_col9.button("↩", key=f"_pt_cancel_{_ri9}", help="取消修改"):
                            st.session_state.pop("_pt_edit_row", None)
                            st.rerun()
                    else:
                        _pc = st.columns(_pt_widths)
                        for _ci9, _key9 in enumerate(("账户", "名称", "代码", "股数", "成本", "类别")):
                            _val9 = str(_row9.get(_key9, ""))
                            if _key9 == "类别":
                                _dl9 = ((_wa.get("holding_levels") or {}).get(str(_row9.get("代码", ""))) or {})
                                if _dl9.get("level"):
                                    _val9 = f"{_val9 or '持仓'} · {_dl9['level']}级"
                            _pc[_ci9].write(_val9)
                        _pc[6].write(_px_txt9)
                        _pc[7].markdown(_pnl_txt9, unsafe_allow_html=True)
                        _pc[8].caption(_row_water9)
                        _edit_col9, _del_col9 = _pc[9].columns(2)
                        if _edit_col9.button("✎", key=f"_pt_edit_{_ri9}", help=f"修改 {_row9.get('名称')}"):
                            st.session_state["_pt_edit_row"] = _edit_id9
                            st.rerun()
                        if _del_col9.button("×", key=f"_pt_del_{_ri9}_{_row9.get('账户')}_{_row9.get('代码')}",
                                           help=f"删除 {_row9.get('名称')}（仅从持仓中移除）"):
                            _msg_del9 = _pmf.remove_holding(_row9.get("账户", ""), _row9.get("代码", ""))
                            if _msg_del9.startswith("已删除"):
                                _pt_git_sync(_msg_del9)
                                st.session_state["_pt_flash"] = _msg_del9 + "　✅已落盘并同步私仓"
                                st.rerun()
                            else:
                                st.error(_msg_del9)
                # ── 整体盈亏汇总：分市场原币种 + 折合人民币合计（汇率取不到则省略合计）──
                if _pt_totals:
                    _sum_parts9 = []
                    _cny_total9, _cny_ok9 = 0.0, True
                    _fx_map9 = {"🇺🇸美股": "USDCNY=X", "🇭🇰港股": "HKDCNY=X"}
                    for _mk9, (_pnl_s9, _cost_s9) in _pt_totals.items():
                        _pct_s9 = (_pnl_s9 / _cost_s9 * 100) if _cost_s9 > 0 else 0.0
                        _sum_parts9.append(f"{_mk9} {_pt_ccy.get(_mk9,'')}{_pnl_html(_pnl_s9, _pct_s9)}")
                        if _mk9 in _fx_map9:
                            _fx9 = _fx_to_cny(_fx_map9[_mk9])
                            if _fx9:
                                _cny_total9 += _pnl_s9 * _fx9
                            else:
                                _cny_ok9 = False
                        else:
                            _cny_total9 += _pnl_s9
                    _sum_line9 = "　｜　".join(_sum_parts9)
                    if _cny_ok9 and len(_pt_totals) > 1:
                        _sum_line9 += f"　｜　合计折合 ¥{_pnl_html(_cny_total9)}"
                    st.markdown(
                        f'<div style="font-size:13px;margin:.3rem .1rem"><b>💰 整体盈亏</b>：{_sum_line9}'
                        f'<span style="color:#94a3b8;font-size:10px">　（按最新收盘价，未含手续费）</span></div>',
                        unsafe_allow_html=True)
            _tr_fp = _repo / "journal" / "trades.json"
            if _tr_fp.exists():
                _trs = json.loads(_tr_fp.read_text(encoding="utf-8"))[-5:]
                if _trs:
                    st.caption("最近5笔：" + "　".join(
                        f"{t.get('date','')[:10]} {t.get('action','')}{t.get('name','')}{t.get('shares','')}股"
                        f"@{t.get('sell_price') or t.get('cost','')}" for t in reversed(_trs)))
        except Exception:
            pass
        # 高级：一行指令（老语法保留）
        with st.popover("⌨️ 一行指令"):
            _pt_cmd = st.text_input("指令", placeholder="中国海油 18.5 1000 ｜ 卖 海油 500 #止盈 ｜ 查",
                                    key="_pt_cmd_desk", label_visibility="collapsed")
            if st.button("执行", key="_pt_go_line") and _pt_cmd.strip():
                try:
                    _msgL, _needsL = _pmf.handle_ex(_pt_cmd.strip(), reason=_pt_reason.strip())
                    if _needsL:
                        st.warning(_msgL + "——请改用上方表单录入以弹窗选择")
                    else:
                        st.success(_msgL)
                        if _pt_cmd.strip() not in ("查", "查询"):
                            _pt_git_sync(_pt_cmd.strip())
                except Exception as _pe9:
                    st.error(f"异常: {_pe9}")
        _lc0 = _rep.find("## 💼 持仓生命周期") if _rep else -1
        if _lc0 > 0:
            _lc1 = _rep.find("\n## ", _lc0 + 5)
            st.markdown(_linkify_md(_rep[_lc0:_lc1 if _lc1 > 0 else len(_rep)]), unsafe_allow_html=True)

    # 【V88·深度回调机会池】优质股回撤≥30%关注名单（日报流水线生成，三端同源）
    _ipb = _rep.find("## 💎 深度回调机会池")
    if _ipb <= 0 and _rep:
        st.caption("💎 深度回调机会池：今日无新入池标的")
    if _ipb > 0:
        _jpb = _rep.find("\n## ", _ipb + 5)
        with st.expander("💎 深度回调机会池（优质股·回撤≥30%·企稳信号）", expanded=False):
            _pbtxt = _rep[_ipb:_jpb if _jpb > 0 else _ipb + 2500]
            st.markdown(_linkify_md(_pbtxt), unsafe_allow_html=True)
            with st.popover("📋 复制机会池"):
                st.code(_pbtxt, language=None)

    # ③ 今日操作榜（短线/长线 Top3，来自日报，AI+真实新闻锚定）＋ ④ 持仓触发提醒
    _c_ops, _c_hold = st.columns([3, 2])
    with _c_ops:
        _i = _rep.find("## 🎯 今日操作榜")
        if _i > 0:
            _j = _rep.find("## 二、", _i)
            with st.expander("🎯 今日操作榜（短线/长线 Top3）", expanded=False):
                _ops_txt99 = _linkify_md(_rep[_i + len("## 🎯 今日操作榜"):_j if _j > 0 else _i + 2500])
                with st.popover("📋 复制操作榜"):
                    st.code(_ops_txt99, language=None)
                st.markdown(_ops_txt99, unsafe_allow_html=True)
        else:
            st.caption("操作榜待日报生成后显示")
    with _c_hold:
        _k = _rep.find("## 💼 我的持仓·框架化建议")
        _alerts = []
        if _k > 0:
            for _ln in _rep[_k:_k + 2500].splitlines():
                if any(x in _ln for x in ("⚠️", "🛑", "🔔")) and "|" in _ln:
                    _p = [x.strip() for x in _ln.split("|") if x.strip()]
                    if len(_p) >= 7:
                        _alerts.append(f"- **{_p[0]}**：{_p[-1]}")  # 最后一列=框架行动,防列数变化
        if _alerts:
            st.markdown("**⚡ 持仓触发提醒**")
            st.markdown("\n".join(_alerts[:6]))
        elif _k > 0:
            st.markdown("**⚡ 持仓触发提醒**：今日无触发 ✅")


try:
    _render_today_nav()
except Exception as _nav_e:
    st.caption(f"今日导航暂不可用: {str(_nav_e)[:50]}")
st.markdown("---")

# ═══════════════════════════════════════════════════════════════
# 【模块 ②】我的持仓（标题在 _render_portfolio_section 内部）
# ═══════════════════════════════════════════════════════════════
try:
    _render_portfolio_section()
except Exception as _e_port:
    st.warning(f"⚠️ 持仓模块加载异常: {str(_e_port)[:60]}")

# ═══════════════════════════════════════════════════════════════
# 【V90.3】行业热力已整合到「全球市场概览」第4个Tab
# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# 【模块 ⑤】AI市场简报
# ═══════════════════════════════════════════════════════════════
# 【V91.8】锚点：方便从深度作战室/侧边栏快速跳转
st.markdown('<div id="ai-market-brief"></div>', unsafe_allow_html=True)
_module_header("📰", "AI市场简报", "DeepSeek实时市场分析", "#3b82f6", "#8b5cf6", compact=True)
# ═══════════════════════════════════════════════════════════════
st.markdown("---")

from datetime import datetime as _dt_brief
from zoneinfo import ZoneInfo as _ZI_brief

# 共用样式常量
_BRIEF_CONTENT_STYLE = """<style>
.news-brief {
    background-color: #f9fafb;
    padding: 1.5rem;
    border-radius: 8px;
    border-left: 4px solid #3b82f6;
    font-size: 14px;
    line-height: 1.8;
    color: #374151;
}
.news-brief h1 { font-size: 20px !important; font-weight: 700 !important; margin: 1.4rem 0 0.6rem 0 !important; color: #111827 !important; }
.news-brief h2 { font-size: 17px !important; font-weight: 700 !important; margin: 1.2rem 0 0.5rem 0 !important; color: #1f2937 !important; border-bottom: 1px solid #e5e7eb; padding-bottom: 0.3rem; }
.news-brief h3 { font-size: 15px !important; font-weight: 600 !important; margin: 0.9rem 0 0.4rem 0 !important; color: #374151 !important; }
.news-brief p  { font-size: 14px !important; margin: 0.5rem 0 !important; }
.news-brief ul, .news-brief ol { font-size: 13px !important; margin: 0.4rem 0 !important; padding-left: 1.5rem !important; }
.news-brief li { margin: 0.3rem 0 !important; }
.news-brief strong { font-weight: 600 !important; color: #1f2937 !important; }
</style>"""

def _render_brief_with_ledger(_content, _key):
    """日报主体正常显示；可核验来源台账保留完整但默认折叠、最小字体。"""
    _marker = "## 🔗 可核验来源台账"
    _idx = str(_content).find(_marker)
    _main = str(_content)[:_idx].rstrip() if _idx >= 0 else str(_content)
    _ledger = str(_content)[_idx:].strip() if _idx >= 0 else ""
    st.markdown(f'<div class="news-brief">{_linkify_md(_main)}</div>', unsafe_allow_html=True)
    if _ledger:
        _count = sum(1 for _ln in _ledger.splitlines() if _ln.lstrip().startswith("- "))
        with st.expander(f"🔗 可核验来源台账（{_count}条）", expanded=False):
            _html = _ledger.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            _html = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)",
                           r'<a href="\2" target="_blank">\1</a>', _html)
            _html = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", _html)
            _html = _html.replace("\n", "<br>")
            st.markdown(f'<div style="font-size:9px;line-height:1.35;color:#64748b">{_html}</div>',
                        unsafe_allow_html=True)

st.markdown(f"""
<div style="margin-bottom:0.5rem;">
  <span style="color:#1f2937; font-size:18px; font-weight:700;">📰 AI市场简报 · {_dt_brief.now(_ZI_brief("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M")} CST</span>
</div>
""", unsafe_allow_html=True)

# 【V92】固定使用 DeepSeek V3，不再提供模型选择
BRIEF_MODEL = "deepseek-v4-flash"

# ── 三端统一权威日报：桌面/云端/飞书使用同一正文与冻结快照 ───────────────────
_brief_cached_content, _brief_cached_ts = _load_brief_cache()
if _brief_cached_content:
    st.session_state["market_brief_latest"] = _brief_cached_content

_brief_cache_info_col, _brief_btn_col = st.columns([4, 1])
with _brief_cache_info_col:
    if _brief_cached_ts:
        _brief_age_h = (time.time() - _brief_cached_ts) / 3600
        _brief_remain_h = max(0.0, (_BRIEF_CACHE_TTL / 3600) - _brief_age_h)
        _brief_remain_m = int(_brief_remain_h * 60)
        _brief_gen_dt = _dt_brief.fromtimestamp(_brief_cached_ts).strftime("%m-%d %H:%M")
        _brief_remain_str = (
            f"{_brief_remain_h:.1f}h" if _brief_remain_h >= 1
            else f"{_brief_remain_m}min"
        )
        _brief_status = _AUTHORITATIVE_BRIEF_META.get("status")
        _sid = _AUTHORITATIVE_BRIEF_META.get("snapshot_id", "")
        if _brief_status == "passed":
            st.caption(f"✅ 权威日报(Plan A) · {_brief_gen_dt} 生成 · Snapshot `{_sid}` · 已缓存 {_brief_age_h:.1f}h")
        elif _brief_status == "legacy":
            st.caption(f"ℹ️ 权威日报旧协议 · {_brief_gen_dt} 生成 · 下一轮任务自动升级质检清单")
        elif _brief_status == "plan_b":
            st.caption(f"🟡 Plan B当日安全版 · {_brief_gen_dt}生成 · 今日新闻/快照/榜单 · 仅观察")
        else:
            st.caption(f"📭 今日Plan A与当天安全Plan B均不可用")
        if _brief_status == "plan_b":
            _today_issues9 = _AUTHORITATIVE_BRIEF_META.get("today_issues") or ["未知质检问题"]
            st.warning(f"⚠️ Plan A未过质检（{'；'.join(_today_issues9)}）；以下为当天重新生成的Plan B，"
                      "保留今日热点、观察股及明日/本周参考，但不提供直接交易动作。")
        elif _brief_age_h > _BRIEF_CACHE_TTL / 3600:
            st.warning("行情快照已超过1小时：报告仅作历史阅读，交易动作需等待下一轮权威日报。")
    else:
        st.caption("📭 权威日报尚未生成；桌面端不会另写一份口径不同的报告。")
with _brief_btn_col:
    _reload_authoritative = st.button("🔄 重新载入", key="btn_market_brief", type="primary", width='stretch')
    do_generate = False
    if _reload_authoritative:
        st.session_state.pop("market_brief_latest", None)
        st.rerun()

if _AUTHORITATIVE_BRIEF_META.get("status") == "missing":
    _issues = _AUTHORITATIVE_BRIEF_META.get("issues") or ["未知质检错误"]
    st.error("今日Plan A未过质检，且当天安全Plan B生成失败：" + "；".join(_issues))

# 不再由桌面端二次改写日报。Plan A失败时只读取流水线当天生成的安全Plan B。
# ─────────────────────────────────────────────────────────────────────────────

if do_generate:
    try:
        _skip_heavy = st.session_state.get("_brief_skip_heavy_bundle", False)
        with _v88_running("🤖 DeepSeek V3 分析中..."):
            # 【V87.4】增强市场简报 - 获取实时数据
            us_pool, hk_pool, cn_pool = init_stock_pools()
            
            # 【V90修复】获取代表性指数数据 - 使用真实指数代码 + 标注日期避免误导
            indices_data = {}
            
            def _safe_index_change(code, label):
                """安全获取指数涨跌幅，返回带日期的描述"""
                try:
                    _idx_df = fetch_stock_data(code)
                    if _idx_df is not None and len(_idx_df) >= 2:
                        _last_date = _idx_df.index[-1]
                        _prev_date = _idx_df.index[-2]
                        _last_close = float(_idx_df['Close'].iloc[-1])
                        _prev_close = float(_idx_df['Close'].iloc[-2])
                        _chg = ((_last_close - _prev_close) / _prev_close * 100) if _prev_close > 0 else 0
                        _last_str = _last_date.strftime('%m/%d') if hasattr(_last_date, 'strftime') else str(_last_date)[-5:]
                        _prev_str = _prev_date.strftime('%m/%d') if hasattr(_prev_date, 'strftime') else str(_prev_date)[-5:]
                        return f"{label}: {_last_close:.2f}（{_prev_str}→{_last_str} 涨跌 {_chg:+.2f}%）"
                except Exception as _ie:
                    pass
                return f"{label}: 数据获取中"
            
            try:
                indices_data['US'] = _safe_index_change("^GSPC", "标普500指数")
                indices_data['HK'] = _safe_index_change("^HSI", "恒生指数")
                indices_data['CN'] = _safe_index_change("000001.SS", "上证综指")
            except:
                pass
            
            # 【选股引擎】二层候选（首次自动简报 _skip_heavy 时跳过，避免 684 池+全池询价卡死）
            _date_str = datetime.now().strftime("%Y-%m-%d")
            _cache_key = f"_market_brief_bundle_{_date_str}"
            _sel_data = None
            _use_expanded_pool = False
            us_candidates = hk_candidates = cn_candidates = []

            if _skip_heavy:
                us_candidates = [f"{it[1]}({it[2]})" for it in us_pool[:15]]
                hk_candidates = [f"{it[1]}({it[2]})" for it in hk_pool[:15]]
                cn_candidates = [f"{it[1]}({it[2]})" for it in cn_pool[:15]]
            elif SELECTION_ENGINE_AVAILABLE and mod_selection:
                if _cache_key not in st.session_state:
                    with _v88_running("📊 选股引擎：684池二层候选筛选中（Explore+Trade）..."):
                        try:
                            _sel_data = mod_selection.build_candidates_bundle(
                                us_pool, hk_pool, cn_pool,
                                fetch_fn=fetch_stock_data,
                                date_str=_date_str,
                            )
                            st.session_state[_cache_key] = _sel_data
                            mod_selection.verify_bundle_print(_sel_data)
                        except Exception as _e:
                            _safe_print(f"⚠️ 选股引擎异常，降级 pool[:15]: {_e}")
                            st.session_state[_cache_key] = None
                _sel_data = st.session_state.get(_cache_key)
                if _sel_data:
                    us_candidates = mod_selection.format_bundle_wsj_candidates(_sel_data, "US", "$", 100)
                    hk_candidates = mod_selection.format_bundle_wsj_candidates(_sel_data, "HK", "HK$", 100)
                    cn_candidates = mod_selection.format_bundle_wsj_candidates(_sel_data, "CN", "¥", 100)
                    if not hk_candidates and hk_pool:
                        hk_candidates = [f"{it[1]}({it[2]})" for it in hk_pool[:15]]
                    if not cn_candidates and cn_pool:
                        cn_candidates = [f"{it[1]}({it[2]})" for it in cn_pool[:15]]
                    if not us_candidates and us_pool:
                        us_candidates = [f"{it[1]}({it[2]})" for it in us_pool[:15]]
                    _use_expanded_pool = True
                else:
                    _sel_data = None
                    _use_expanded_pool = False

            if (not _skip_heavy
                    and (not SELECTION_ENGINE_AVAILABLE or not mod_selection or not _sel_data)):
                from concurrent.futures import ThreadPoolExecutor, as_completed
                def _get_close_price(yf_code):
                    try:
                        _df = fetch_stock_data(yf_code)
                        if _df is not None and len(_df) > 0:
                            return float(_df['Close'].iloc[-1])
                    except Exception:
                        pass
                    return None
                _all_items = (
                    [(item, "$") for item in us_pool[:15]] +
                    [(item, "HK$") for item in hk_pool[:15]] +
                    [(item, "¥") for item in cn_pool[:15]]
                )
                with ThreadPoolExecutor(max_workers=8) as _exec:
                    _price_cache = {}
                    _futures = {_exec.submit(_get_close_price, it[0][2]): (it[0], it[1]) for it in _all_items}
                    for _f in as_completed(_futures):
                        _item, _pfx = _futures[_f]
                        try:
                            _price_cache[(_item[2], _pfx)] = _f.result()
                        except Exception:
                            _price_cache[(_item[2], _pfx)] = None
                def _fmt_cand(it, pfx):
                    p = _price_cache.get((it[2], pfx))
                    return f"{it[1]}({it[2]}): 日报价 {pfx}{p:.2f}" if p is not None else f"{it[1]}({it[2]})"
                us_candidates = [_fmt_cand(it, "$") for it in us_pool[:15]]
                hk_candidates = [_fmt_cand(it, "HK$") for it in hk_pool[:15]]
                cn_candidates = [_fmt_cand(it, "¥") for it in cn_pool[:15]]
                _use_expanded_pool = False
            
            # 获取当前日期与校验时间（Asia/Shanghai）
            from datetime import datetime
            from zoneinfo import ZoneInfo
            today = datetime.now().strftime("%Y年%m月%d日")
            _ts_shanghai = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")

            # 跨日去重：读取近3天已推荐代码
            _recent_codes = _get_recent_recommended_codes(days=3)
            _recent_block = (
                f"【近3日已推荐代码（禁止重复推荐）】{', '.join(_recent_codes)}\n"
                if _recent_codes else ""
            )

            # 读取真实新闻报告（ai-daily-report-v2 日报），约束触发事件禁止编造
            _real_news_report = _load_real_news_report()
            _real_news_block = (
                f"\n\n【真实新闻报告（以下为今日真实新闻，可执行推荐的「触发」字段必须来自此处，禁止编造）】\n{_real_news_report}\n"
                if _real_news_report else ""
            )

            # 【V99.4】口径统一：日报推荐必须建立在「一键全选」扫描结果之上
            # 15分钟内的扫描缓存直接复用；无缓存且非首屏轻载时，自动先扫中美港全部
            _scan_ctx = ""
            try:
                _uni_rows = None
                _sr = st.session_state.get('scanner_results') or {}
                if (_sr.get('type') == 'unified' and _sr.get('data')
                        and (time.time() - _sr.get('scan_timestamp', 0)) < get_smart_cache_ttl('daily')):
                    _uni_rows = _sr['data']
                if _uni_rows is None:
                    for _mk_try in ("🌍 中美港全部", "美股", "A股", "港股"):
                        _ld = _load_scan_cache_from_file('unified', _mk_try)
                        if _ld and _ld.get('data'):
                            _uni_rows = _ld['data']
                            break
                if _uni_rows is None and not _skip_heavy:
                    _bp_bar = st.progress(0)
                    _bp_txt = st.empty()
                    _bp_t0 = time.time()

                    def _bp_cb(cur, total, name):
                        _bp_bar.progress(min(1.0, cur / max(1, total)))
                        _el = time.time() - _bp_t0
                        _eta = (_el / cur * (total - cur)) if cur > 3 else 0
                        _bp_txt.text(f"⏱ 简报前置·中美港一键全选 已用{_el:.0f}s·剩余约{_eta:.0f}s ｜ {cur}/{total} - {name}")

                    _pool_all = list(RAW_US) + list(RAW_HK) + list(RAW_CN_TOP)
                    _uni_rows, _u_st99, _u_mt99 = run_unified_scan(
                        _pool_all, "美股", "平衡", True, progress_callback=_bp_cb)
                    _bp_bar.empty(); _bp_txt.empty()
                    st.session_state.scanner_results = {
                        'type': 'unified', 'scan_market': '🌍 中美港全部', 'risk_preference': '平衡',
                        'title': '#### 🔍 全策略一页榜单 (🌍 中美港全部)', 'caption': '',
                        'data': _uni_rows, 'stats': _u_st99, 'key': 'unified_table',
                        'scan_timestamp': time.time()}
                    try:
                        _save_scan_cache_to_file(st.session_state.scanner_results)
                    except Exception:
                        pass
                if _uni_rows:
                    def _mkb99(code):
                        return market_of_code(code)[-2:]  # 去国旗emoji：美股/A股/港股
                    _by_m = {'美股': [], 'A股': [], '港股': []}
                    for _r in _uni_rows:
                        _m0 = str(_r.get('市场', '')).replace('🇺🇸', '').replace('🇨🇳', '').replace('🇭🇰', '') or _mkb99(_r.get('代码', ''))
                        if _m0 in _by_m and len(_by_m[_m0]) < 8:
                            _by_m[_m0].append(
                                f"{_r.get('名称')}({_r.get('代码')}) 得分{_r.get('得分')} RS{_r.get('RS强度')} "
                                f"20日{_r.get('20日动量')} 指引:{str(_r.get('操作指引', ''))[:14]}")
                    _lines_sc = [f"- {_mm}: " + "；".join(_ll) for _mm, _ll in _by_m.items() if _ll]
                    if _lines_sc:
                        _scan_ctx = ("\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                     "【一键全选扫描榜（五维引擎实时扫描·推荐的强制基础）】\n"
                                     "硬性规则：操作榜(中长短×中美港)与二/三/四节的推荐个股，每市场至少2只必须从下方扫描榜选取；"
                                     "期限归属由你结合指标与新闻判定——RS高/20日动量强或有72h内催化→短线，得分高且趋势稳→中线/长线；"
                                     "扫描榜外的个股仅当有重大新闻催化时才可推荐且须注明「榜外·新闻驱动」。价位仍以候选池日报价为唯一基准。\n"
                                     + "\n".join(_lines_sc) + "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                        _safe_print(f"[简报] ✅ 已注入一键全选扫描榜: 美{len(_by_m['美股'])}/A{len(_by_m['A股'])}/港{len(_by_m['港股'])}")
            except Exception as _sce:
                _safe_print(f"[简报] 扫描候选注入失败: {_sce}")

            prompt = f"""生成机构级市场日报：写法参考华尔街日报、彭博、路透的市场稿逻辑，但不要模仿任何版权文本。
目标不是“写得热闹”，而是做到：事实可追溯、推理有链条、结论可执行、风险能证伪。全文为正式中文财经报道，不使用聊天体、社群体、钉钉模板。

【总编辑准则】
1) 先证据、后判断：每个关键判断必须有数据、来源、事件或价格行为支撑。
2) 区分事实/推断/策略：事实不能夸大，推断必须写出传导链，策略必须写失效条件。
3) 不为凑推荐而编故事：没有可核验催化时，触发必须写“无近期相关新闻触发，基于基本面/技术结构判断”，动作自动降级为中期跟进或观察。
4) 不写绝对化语言：禁止“必涨、确定、无风险、资金明显回流”等不可证伪表述。
5) 战争/地缘政治（美国、伊朗、以色列、中东冲突、俄乌、制裁、能源供应等）若有新闻必须输出，不得遗漏；若无可靠新闻，明确写“未见可核验新增事件”。

【数据口径】雅虎财经收盘价 | 【校验时间】{_ts_shanghai} (Asia/Shanghai 上海时区)

【日期】{today}
{_real_news_block}

【指数数据】（括号内为实际交易日对比，请严格引用，禁止编造）
{indices_data.get('US', '美股数据获取中')}
{indices_data.get('HK', '港股数据获取中')}
{indices_data.get('CN', 'A股数据获取中')}

【候选池】必须从以下选择，日报价必须全文引用。候选池已从684母池筛选扩大，请从以下选择。
- 美股：{chr(10).join('- ' + c for c in us_candidates)}
- 港股：{chr(10).join('- ' + c for c in hk_candidates)}
- A股：{chr(10).join('- ' + c for c in cn_candidates)}
严禁编造代码！A股/港股必须用数字代码（如 600519.SS、00700.HK）。

【事实与真实性规则】
1) 只能使用本 prompt 中给出的指数、候选池、真实新闻报告和可从常识层面成立的公开公司属性；不得捏造财报数字、订单、政策、产能、回购、监管动作或媒体报道。
2) 对每条新闻按 Source Tier 标注来源等级；不能确认来源时，必须写“来源不足”，且不得支撑立即建仓。
3) “已发生”只写已经落地且可核验的事件；“待验证”只写需要后续确认的线索；二者不得混写。
4) 推荐理由必须形成“三段链”：事件/数据 → 盈利或流动性影响 → 估值/价格行为影响。
5) 若真实新闻报告为空，不得声称“今日/近日某媒体报道了某事件”；只能基于指数数据、候选池价格和基本面常识做保守判断。

【硬性规则】
1) 必须覆盖美股、港股、A股三个市场；每市场必须给1-3只重点标的。高置信不足3只可减少数量；若无买点，必须从趋势、基本面、行业或大跌风险逻辑给出观察股
2) 动作由证据决定；不满足 BUILD_NOW 必须降级为观察或风险观察。不得硬凑买入，但不能整段空白
3) **最终推荐 3 只里至少 2 只必须来自 Trade 池**（若候选池含 Trade 标记，优先选质量闸门通过的标的）
4) 每只推荐首行必须写为 **名称(代码)** 格式（如 **苹果(AAPL)**、**腾讯控股(00700.HK)**、**贵州茅台(600519.SS)**），便于系统标注现价
5) 观察 禁止给目标位和买入建议
6) 每只必须含：证据状态灯、R/R、三类失效、触发、基本面承接、技术确认、动作标签、仓位建议
7) 文末固定输出 数据 与 时间戳（Asia/Shanghai），并标注数据截点
8) **跨日去重**：{_recent_block}上述代码在近3日已推荐，本次9只推荐中禁止出现这些代码；若候选池内无其他合格标的，则可降级为「观察」后选入，但不得再次列为「立即建仓」或「中期跟进」
9) **行业多样性**：某市场推荐达到2只以上时，应覆盖至少2个不同行业/板块；不得为满足多样性降低准入门槛
10) **触发事件禁止编造**：若上方提供了【真实新闻报告】，则所有「触发」字段的事件必须来自该报告，且必须注明具体来源媒体名称（如 Bloomberg、Reuters、WSJ、财新、公司公告等）；若未提供或找不到对应新闻，触发字段必须写：无近期相关新闻触发，基于基本面判断。严禁编造任何新闻事件（如产能扩充、财报数据等）
11) **建仓区间必须基于当前实时价格计算**：建仓区间 = 候选池中传入的日报价（即当前实时收盘价）× (1 ± 3%~5%)。严禁使用任何历史高价、52周高点、历史缓存价格计算建仓区间。若候选池已提供「日报价 $X.XX」，则建仓区间下限不得高于该价格的110%，上限不得高于该价格的115%。

【V2.1 Action Gate】立即建仓 仅当以下全满足，否则自动降级 中期跟进 或 观察：
a) 证据状态灯 == ✅
b) 触发时效 ≤ 72h
c) 来源 Tier 为 A 或 B（禁止 Tier C）
d) R/R ≥ 2.0
e) 失效条件含 基本面+结构+事件 三类

【Source Tiering 来源分级】
- Tier A：交易所/SEC/公司公告/财报电话会原文/央行与部委官网/统计局
- Tier B：Bloomberg/Reuters/WSJ/FT/财新等权威媒体
- Tier C：未具名消息/二手转述/社媒（立即建仓 禁止 Tier C）

【Professional QA Gate】日报发布前必须通过以下检查：
1) 立即建仓 须满足 Action Gate 全部条件
2) 若「触发=无」则 Action 只能是 观察 或 中期跟进，不得 立即建仓
3) 每只标的必须输出 Card Schema 全部字段
4) 市场驱动、市场格局、催化事件板按 美/港/A 三市场分别输出，不得缺区
5) 催化事件板：每市场「已发生」栏必须有至少1条可验证事件
6) 每段 90-150 字；先事实后影响再动作提示；禁用空泛语句除非给证据
7) 所有时间统一 Asia/Shanghai，并标注数据截点
8) 若任一推荐缺字段、缺来源、缺传导链或规则冲突，文末输出「日报未通过质检」并列出错误项
9) 在“事实台账”中列出至少6条事实锚点；每条必须标注来源/数据口径、时间、对市场的直接含义

【BUILD_NOW 判定器】仅当以下 6 项同时满足，动作标签才允许输出「立即建仓」；任一不满足则输出「中期跟进」或「观察」，严禁给「立即建仓」：
1) 24h/72h 内存在可核验硬催化：来源仅限公司公告/交易所文件/监管公告/财报电话会/权威媒体；若仅为传闻或二手转述，直接降级 中期跟进
2) 事件-利润-估值传导链完整：触发事件 → 关键经营指标变化 → EPS/FCF 修正方向 → 估值中枢影响
3) 预期差成立：说明当前市场共识与本策略判断的差异点（至少1条）
4) 赔率达标：5-20交易日上行概率≥60%、回撤风险概率≤35%、Reward/Risk≥2.0
5) 技术结构未破坏：未出现结构性破位+放量走坏
6) 失效条件可执行：必须给出 基本面失效、结构失效、事件失效 三类触发；缺任一项不得输出 立即建仓

【Card Schema】每只推荐必须含以下字段，缺一不可：
- 代码|名称
- 动作标签（BUILD_NOW/FOLLOW_MID/WATCH）
- 触发（24h/72h）
- 来源（含 tier）
- 机会概率/风险概率
- 建仓区间
- 仓位上限 + 分批节奏（如 40/30/30）
- R/R
- 失效条件（基本面/结构/事件）
- 数据时间戳（Asia/Shanghai）

【Market Section Format】市场驱动/市场格局/催化事件板按美股、港股、A股分别输出，不能缺区。每段限制在 90-150 字，先事实后影响再动作提示。

【Watch Upgrade Logic】观察 标的必须给出：
- 升级条件：满足 2/3 项时升级为 中期跟进
- 降级条件

【写作层硬约束】上半部分采用日报体，专业、有深度：
- 禁用「驱动1/驱动2/状态/主导变量/交易倾向/暂无新增可验证催化」等机器标签词
- 每个段落 90-150 字
- 禁用空泛语句（如「龙头稳固」「资金明显回流」）除非给证据
- 每个市场必须写出“核心矛盾”：估值/利率/盈利/政策/汇率/风险偏好至少选一项作为主线
- 每个市场点评必须是判断，不是复述；但判断后要给可证伪条件
- 市场驱动、催化事件板：每市场至少1条，当日有新事件可多列
- **战争/地缘政治**：美伊以、中东、俄乌等若有新闻必须在「战争/地缘政治」节输出，并在市场驱动中体现影响
- 市场格局：每市场可写2-4句连贯段落，体现核心矛盾、资金风格、明日观察
- **必须含 AI 点评**：每个市场在事实陈述后，附带「点评」1-2句，有态度、有判断
- 句式有变化，避免重复「今日…受…影响…」模板句
- 催化事件板：每市场「已发生」栏必须有至少1条可验证的已落地事件（政策/数据/财报/公告/宏观数据）；禁止港股、A股「已发生」写「线索仍在形成中」，必须列举具体事件（如央行数据、统计局PMI/CPI、贸易数据、监管政策、行业公告等）；「线索仍在形成中」仅允许用于「待验证」栏

【输出要求】全文使用中文，禁止英文术语（除必要代码如 AAPL、600519.SS）

请按以下结构输出（不要称呼和结尾废话）：

---

## 标题
[一句话概括当日市场核心变化]

---

## 导语
[2-3句新闻导语，概括主要事实与结论，含时间锚点。若有美伊以/中东/俄乌等战争地缘新闻，必须在导语中体现]

---

## 核心结论
- [结论1：一句话事实 + 影响 + 今日策略含义]
- [结论2：一句话事实 + 影响 + 今日策略含义]
- [结论3：一句话事实 + 影响 + 今日策略含义]

---

## 🎯 今日操作榜（中长短 × 中美港 · 每市场最多3只）

3期限 × 3市场各列1-3只。短线看技术+动能+72h催化；中线看综合+趋势排列；长线看基本面+估值+长期趋势。高置信不足3只列实际数量；若无人达到买入门槛，必须补充候补观察或潜在大跌风险股，用于保护仓位、利润与胜率，禁止硬凑买入。价位以候选池「日报价」为唯一基准±3%计算，观察股不输出交易价位。

### ⚡ 短线（1-5日）
| 市场 | 代码 | 名称 | 方向 | 参考价位 | 理由 |
|---|---|---|---|---|---|
| 🇺🇸美股 | [US:代码] | 名称 | 买入/观察 | 买X·损Y·标Z | 一句 |
| （美股3只、A股3只、港股3只）||||||

### 🚀 中线（1-3月）
| 市场 | 代码 | 名称 | 方向 | 参考价位 | 理由 |
|---|---|---|---|---|---|
| 🇺🇸美股 | [US:代码] | 名称 | 建仓/观察 | 建仓区间·损Y | 一句 |
| （美股3只、A股3只、港股3只）||||||

### 🏛 长线（数周-数月）
| 市场 | 代码 | 名称 | 方向 | 参考价位 | 理由 |
|---|---|---|---|---|---|
| 🇺🇸美股 | [US:代码] | 名称 | 建仓/观察 | 建仓区间 | 一句 |
| （美股3只、A股3只、港股3只）||||||

---

## 事实台账
| 事实 | 来源/口径 | 时间 | 市场含义 | 置信度 |
|---|---|---|---|---|
| [指数/新闻/政策/公告事实] | [来源或雅虎财经] | [时间] | [直接影响] | [高/中/低] |

---

## 战争/地缘政治（必含）
若有美国、伊朗、以色列、中东冲突、俄乌、制裁、能源供应等新闻，**必须**在本节输出，不得遗漏。格式：事件+来源+对股市/原油/避险资产的影响。

---

## 市场驱动

美/港/A 三市场必须分别输出，不得缺区。每市场至少1条，有新事件可多列。每段 90-130 字，先事实后影响再动作提示。每市场必须附带「点评」。

### 🇺🇸 美股
[短段落或要点，含事实+传导链+市场反应。可2-3句。]
**点评**：[1-2句，有态度、有判断，华尔街日报式编辑点评]

### 🇭🇰 港股
[短段落，可适当展开。]
**点评**：[1-2句，有态度、有判断]

### 🇨🇳 A股
[短段落，可适当展开。]
**点评**：[1-2句，有态度、有判断]

---

## 市场格局

美/港/A 三市场必须分别输出，不得缺区。每段 90-130 字，先事实后影响再动作提示。每市场必须附带「点评」。

### 🇺🇸 美股
[2-4句：核心矛盾→资金风格→明日观察，写成自然判断段。]
**点评**：[1-2句，有态度、有判断]

### 🇭🇰 港股
[同上，2-4句连贯段落。]
**点评**：[1-2句，有态度、有判断]

### 🇨🇳 A股
[同上，2-4句连贯段落。]
**点评**：[1-2句，有态度、有判断]

---

## 催化事件板

美/港/A 三市场必须分别输出，不得缺区。每段 90-130 字，先事实后影响再动作提示。
**硬性要求**：每市场「已发生」栏必须有至少1条可验证的已落地事件（含时间+来源+影响），禁止写「线索仍在形成中」；港股、A股必须列举具体事件（如央行/统计局数据、贸易数据、监管政策、行业公告等），无个股催化可写市场/宏观层面催化。「线索仍在形成中」仅允许用于「待验证」栏。

### 🇺🇸 美股
已发生：[必须1条以上，时间+事件+来源+影响]
待验证：[待确认线索；若无则写「线索仍在形成中」]

### 🇭🇰 港股
已发生：[必须1条以上，禁止「线索仍在形成中」；可写宏观/政策/行业数据]
待验证：[待确认线索；若无则写「线索仍在形成中」]

### 🇨🇳 A股
已发生：[必须1条以上，禁止「线索仍在形成中」；可写宏观/政策/行业数据]
待验证：[待确认线索；若无则写「线索仍在形成中」]

---

## 可执行推荐

【⚠️ 硬性要求】必须覆盖美股、港股、A股三个市场，每市场1-3只。高置信不足3只可以减少；若无买点，必须给出至少1只重点观察或风险保护标的，不得空白，也不得降低买入门槛。
动作标签必须由证据决定：只有满足 BUILD_NOW 的标的才能写「立即建仓」；否则写「中期跟进」或「观察」，并说明缺失的触发条件。

每只推荐必须符合 Card Schema，含：代码|名称、动作标签、触发、来源(tier)、机会/风险概率、建仓区间、仓位上限+分批节奏、R/R、失效条件、时间戳。

### 🇺🇸 美股（1-3只：无买点时列观察/风险保护）
1. **[代码|名称]** · **[立即建仓/中期跟进/观察]**（立即建仓须满足 Action Gate 全条件）
   - 触发: [24h/72h] [事件] [来源·Tier A/B]
   - 机会概率/风险概率: [%/%]
   - 建仓区间: [区间]
   - 仓位上限 + 分批节奏: [如 40/30/30]
   - R/R: [≥2.0]
   - 失效条件: ① 基本面 ② 结构 ③ 事件
   - 证据状态灯: ✅
   - 数据时间戳: Asia/Shanghai

2. **[代码|名称]** · **[中期跟进/观察]**
   - [同上 Card Schema 格式]

3. **[代码|名称]** · **观察**
   - [Card Schema 格式，观察 不输出目标位和买入建议]
   - **升级条件**：满足 2/3 项 → 升级为 中期跟进
   - **降级条件**：[具体条件]

### 🇭🇰 港股（1-3只：无买点时列观察/风险保护）
1. **[代码|名称]** · **[立即建仓/中期跟进/观察]** · [Card Schema 全字段]
2. **[代码|名称]** · **[中期跟进/观察]** · [Card Schema 全字段]
3. **[代码|名称]** · **观察** · [Card Schema 全字段 + 升级/降级条件]

### 🇨🇳 A股（1-3只：无买点时列观察/风险保护）
1. **[代码|名称]** · **[立即建仓/中期跟进/观察]** · [Card Schema 全字段]
2. **[代码|名称]** · **[中期跟进/观察]** · [Card Schema 全字段]
3. **[代码|名称]** · **观察** · [Card Schema 全字段 + 升级/降级条件]

---

## 风险提示
- [风险1]
- [风险2]
- [风险3]

---

## 明日触发-动作对照
若 事件A成立 → 动作X
若 事件B落空 → 动作Y
若 事件C发生 → 动作Z

---

## 数据/时间戳
数据: 雅虎财经收盘价
时间戳: {_ts_shanghai} (Asia/Shanghai 上海时区)
数据截点: {_ts_shanghai}

【QA Checker】若以下任一成立，则输出「日报未通过质检」并列出错误项：
- 美股/港股/A股任一市场超过3只，或在无合格标的时仍硬凑推荐
- 任一推荐缺 Card Schema 字段
- 与 Action Gate/Source Tiering 规则冲突
错误项示例：「美股仅2只缺1只」「港股整段缺失」「A股整段缺失」「美股推荐1 缺 R/R」"""
            
            # 【V99.4】把一键全选扫描榜拼进提示词（推荐必须以扫描结果为基础）
            if _scan_ctx:
                prompt += _scan_ctx

            _brief_ph = st.empty()
            res = ""
            for _chunk in call_gemini_api_stream(prompt, model_name=BRIEF_MODEL, max_output_tokens=32768):
                res += _chunk
                _brief_ph.markdown(res + " ▌")
            _brief_ph.empty()
            
            # 【补全】若首次生成缺失港股/A股，或区块存在但推荐数不足3只，则补充调用
            def _count_recs_in_section(text, emoji_flag):
                """统计可执行推荐中某市场的推荐数量（按编号 1./2./3. 计）"""
                import re as _r
                rec_idx = text.find("## 可执行推荐")
                if rec_idx < 0:
                    rec_idx = 0
                section = text[rec_idx:]
                mkt_idx = section.find(f"### {emoji_flag}")
                if mkt_idx < 0:
                    return 0
                sub = section[mkt_idx:]
                next_sec = _r.search(r'\n###\s|\n##\s', sub[5:])
                if next_sec:
                    sub = sub[:next_sec.start() + 5]
                return len(_r.findall(r'^\d+\.\s+\*\*', sub, _r.MULTILINE))

            _hk_count = _count_recs_in_section(res, "🇭🇰")
            _cn_count = _count_recs_in_section(res, "🇨🇳")
            _need_hk = _hk_count < 3
            _need_cn = _cn_count < 3

            if res and not res.startswith("❌") and (_need_hk or _need_cn) and (hk_candidates or cn_candidates):
                import re as _re_supp
                _labels = ["中期跟进", "中期跟进", "观察"]

                # ── 分别补全港股 / A股，各自单独调用和插入 ────────────────
                def _supp_one_market(cur_res, emoji_flag, count, candidates, mkt_header, price_prefix, key_suffix):
                    """补全单个市场缺失的推荐，返回更新后的 res"""
                    if count >= 3 or not candidates:
                        return cur_res
                    missing_indices = list(range(count, 3))
                    # 构建 prompt（只输出缺失的那几只，不重复 section 标题）
                    _sp_lines = [
                        f"日报推荐补全：以下是缺失的{mkt_header.split('（')[0].strip()}推荐，"
                        f"当前已有{count}只，请输出第{count+1}到第3只。",
                        "【严格要求】",
                        "1. 不要输出 section 标题（### 开头的行），直接输出股票编号",
                        "2. 每只输出完整 Card Schema（触发、机会概率/风险概率、建仓区间、仓位+分批、R/R、失效条件×3、证据灯、时间戳）",
                        f"3. 必须从候选池选股，严禁编造",
                        "4. 触发事件必须来自真实新闻，且必须注明具体来源媒体名称（如 Bloomberg、Reuters、WSJ、财新、公司公告等）；若无相关真实新闻，触发字段写：无近期相关新闻触发，基于基本面判断",
                        "5. 建仓区间必须基于传入的当前实时价格（日报价）计算（现价 ×(1±3%~5%)），严禁使用历史高价或缓存价格",
                        "6. 不满足 BUILD_NOW 条件时禁止写立即建仓；缺少真实硬催化时只能写中期跟进或观察",
                        "",
                    ]
                    if _real_news_report:
                        _sp_lines.insert(2, f"【真实新闻报告】\n{_real_news_report}\n")
                    for i in missing_indices:
                        _sp_lines.append(f"{i+1}. **[名称(代码)]** · **{_labels[i]}** · 现价 {price_prefix}X.XX")
                    _sp_lines += [
                        "",
                        f"候选池：{', '.join(candidates[:15])}",
                        "",
                        "只输出以上格式，不要其他内容。"
                    ]
                    _sp = "\n".join(_sp_lines)
                    try:
                        with _v88_running(f"📝 补全{mkt_header.split('（')[0].strip().replace('### ','')}推荐..."):
                            _sr = ""
                            for _ck in call_gemini_api_stream(_sp, model_name=BRIEF_MODEL, max_output_tokens=4096):
                                _sr += _ck
                        if not _sr or _sr.startswith("❌"):
                            return cur_res
                        # 插入位置：找到该市场 section，在其末尾（下一个 ### 或 ## 之前）插入
                        _mkt_pos = cur_res.find(f"### {emoji_flag}")
                        if _mkt_pos >= 0:
                            # 已有部分内容，在 section 末尾插入
                            _after = cur_res[_mkt_pos + 5:]
                            _ns = _re_supp.search(r'\n###\s|\n##\s', _after)
                            if _ns:
                                _ins = _mkt_pos + 5 + _ns.start()
                                cur_res = cur_res[:_ins].rstrip() + "\n" + _sr.strip() + "\n\n" + cur_res[_ins:].lstrip()
                            else:
                                cur_res = cur_res.rstrip() + "\n" + _sr.strip()
                        else:
                            # 整个 section 缺失，追加在 风险提示 前或末尾
                            _full_sec = f"\n{mkt_header}\n" + _sr.strip()
                            if "## 风险提示" in cur_res:
                                cur_res = cur_res.replace("## 风险提示", _full_sec.strip() + "\n\n## 风险提示", 1)
                            else:
                                cur_res = cur_res.rstrip() + "\n\n" + _full_sec.strip()
                        return cur_res
                    except Exception as _es:
                        _safe_print(f"⚠️ 补全{key_suffix}失败: {_es}")
                        return cur_res

                if _need_hk and hk_candidates:
                    res = _supp_one_market(res, "🇭🇰", _hk_count, hk_candidates,
                                           "### 🇭🇰 港股（固定3只：1 立即建仓 + 1 中期跟进 + 1 观察）",
                                           "HK$", "港股")
                if _need_cn and cn_candidates:
                    _cn_count2 = _count_recs_in_section(res, "🇨🇳")  # 重新计数（港股可能已插入）
                    res = _supp_one_market(res, "🇨🇳", _cn_count2, cn_candidates,
                                           "### 🇨🇳 A股（固定3只：1 立即建仓 + 1 中期跟进 + 1 观察）",
                                           "¥", "A股")
            
            # 【推荐个股现价】解析报告中的股票代码，拉取现价并标注
            if res and not res.startswith("❌"):
                def _inject_current_prices(text, _fetch_fn):
                    import re
                    lines = text.split("\n")
                    out = []
                    for line in lines:
                        m = re.search(r'\(([A-Z0-9]{2,}\.[A-Z]{2}|[A-Z0-9]{4,5}\.HK|[A-Z]{2,5})\)', line)
                        if m and any(kw in line for kw in ["立即建仓", "中期跟进", "观察"]):
                            code = m.group(1)
                            try:
                                df = _fetch_fn(code)
                                if df is not None and len(df) > 0 and "Close" in df.columns:
                                    p = float(df["Close"].iloc[-1])
                                    if ".HK" in code: pfx = "HK$"
                                    elif ".SS" in code or ".SZ" in code: pfx = "¥"
                                    else: pfx = "$"
                                    line = line.rstrip() + f" 现价 {pfx}{p:.2f}"
                            except Exception:
                                pass
                        out.append(line)
                    return "\n".join(out)
                try:
                    res = _inject_current_prices(res, fetch_stock_data)
                except Exception as _ep:
                    _safe_print(f"⚠️ 现价注入跳过: {_ep}")

            _brief_qa_issues = []
            if res and not res.startswith("❌"):
                try:
                    _brief_qa_issues = _audit_professional_brief(
                        res,
                        has_real_news_report=bool(_real_news_report),
                    )
                    if _brief_qa_issues:
                        _safe_print("⚠️ AI市场简报本地质检未通过: " + "；".join(_brief_qa_issues))
                except Exception as _qa_e:
                    _brief_qa_issues = [f"本地质检异常: {type(_qa_e).__name__}"]
                    _safe_print(f"⚠️ AI市场简报本地质检异常: {_qa_e}")
            # 【选股引擎】复盘元数据：单独存储，不混入正文
            _meta_html = ""
            if _use_expanded_pool and _sel_data and res and not res.startswith("❌"):
                def _bundle_line(mkt):
                    d = _sel_data.get(mkt, {})
                    s = d.get("subpool_stats", {})
                    parts = [f"母池{s.get('mother_pool_size',0)} 子池{s.get('subpool_size',0)} 覆盖率{s.get('coverage_pct',0):.1f}%"]
                    for h in ["ST","MT","LT"]:
                        hd = d.get(h, {})
                        ex, tr = len(hd.get("explore",[])), len(hd.get("trade",[]))
                        q = hd.get("meta",{}).get("quantile_used",0)
                        parts.append(f"{h}:Ex={ex} Tr={tr}(q={q})")
                    return " ".join(parts)
                _meta_html = (
                    f'<div style="font-size:9px;color:#bbb;margin-top:6px;line-height:1.4;">'
                    f'选股复盘 · 日期:{_sel_data.get("date","")} · '
                    f'美{_bundle_line("US")} · 港{_bundle_line("HK")} · A{_bundle_line("CN")}'
                    f'</div>'
                )
            # 显示日报内容（仅在生成成功时保存缓存，防止将错误文本写入缓存）
            if res.startswith("❌"):
                st.error(res)
                st.caption("💡 生成失败，请稍后点击「🔄 刷新简报」重试")
            else:
                st.markdown(_BRIEF_CONTENT_STYLE, unsafe_allow_html=True)
                import re as _re
                def _clean_brief(txt):
                    if _AUTHORITATIVE_REPORT.exists():
                        return txt.rstrip()
                    for pat in [
                        r'\n?#{1,3}\s*风险提示.*?(?=\n#{1,3}\s|\Z)',
                        r'\n?#{1,3}\s*明日触发.*?(?=\n#{1,3}\s|\Z)',
                        r'\n?#{1,3}\s*数据[/／]时间[戳]?.*?(?=\n#{1,3}\s|\Z)',
                        r'\n?数据[：:][^\n]*时间[戳]?[^\n]*\n?',
                        r'\n?\*数据[：:][^\n]*\n?',
                        r'\n?---\s*\n#{1,3}\s*选股复盘.*',
                        r'\n?#{1,3}\s*选股复盘.*',
                    ]:
                        txt = _re.sub(pat, '', txt, flags=_re.DOTALL)
                    return txt.rstrip()
                _res_display = _clean_brief(res)
                if _brief_qa_issues:
                    st.warning("本地质检提示：这版日报未写入缓存，请根据问题刷新重试。")
                    st.markdown(
                        "\n".join(f"- {issue}" for issue in _brief_qa_issues)
                    )
                _render_brief_with_ledger(_res_display, "new")
                if COPY_UTILS_AVAILABLE:
                    CopyUtils.create_copy_button(_res_display, button_text="📋 复制全文", key="brief_copy_new")
                st.caption("📌 本报告由 AI 生成 · DeepSeek V3")
                st.download_button(
                    "📥 下载简报",
                    data=res,
                    file_name=f"AI市场简报_{datetime.now().strftime('%Y%m%d')}.md",
                    mime="text/markdown",
                    key="download_market_brief"
                )
                # 生成成功且通过本地质检才保存到文件缓存，避免不合格日报占用12小时缓存
                st.session_state["market_brief_latest"] = res
                st.session_state["_brief_auto_gen_done"] = True
                if not _brief_qa_issues:
                    _save_brief_cache(res)
                st.session_state.pop("_brief_skip_heavy_bundle", None)

    except Exception as _brief_err:
        logging.exception("AI市场简报生成失败")
        st.error(f"❌ 简报生成失败：{type(_brief_err).__name__}: {str(_brief_err)[:280]}")
        st.caption("💡 请检查网络与 Gemini API；点击「🔄 刷新简报」可重试（将走完整选股+询价流程）。")

# ── 权威日报自动展示：刷新页面后直接恢复同一份正文 ───────────────────────────
elif "market_brief_latest" in st.session_state:
    _auto_res = st.session_state["market_brief_latest"]
    st.markdown(_BRIEF_CONTENT_STYLE, unsafe_allow_html=True)
    import re as _re
    def _clean_brief(txt):
        if _AUTHORITATIVE_REPORT.exists():
            return txt.rstrip()
        for pat in [
            r'\n?#{1,3}\s*风险提示.*?(?=\n#{1,3}\s|\Z)',
            r'\n?#{1,3}\s*明日触发.*?(?=\n#{1,3}\s|\Z)',
            r'\n?#{1,3}\s*数据[/／]时间[戳]?.*?(?=\n#{1,3}\s|\Z)',
            r'\n?数据[：:][^\n]*时间[戳]?[^\n]*\n?',
            r'\n?\*数据[：:][^\n]*\n?',
            r'\n?---\s*\n#{1,3}\s*选股复盘.*',
            r'\n?#{1,3}\s*选股复盘.*',
        ]:
            txt = _re.sub(pat, '', txt, flags=_re.DOTALL)
        return txt.rstrip()
    _auto_clean = _clean_brief(_auto_res)
    _render_brief_with_ledger(_auto_clean, "cached")
    # 【Fix】使用 CopyUtils（components.html iframe）确保复制按钮跨 DOM 边界可用
    if COPY_UTILS_AVAILABLE:
        CopyUtils.create_copy_button(_auto_clean, button_text="📋 复制全文", key="brief_copy_cached")
    st.caption("📌 权威日报 · AI叙事 + 确定性选股/报价/质检 · 桌面/云端/飞书同源")
    st.download_button(
        "📥 下载简报",
        data=_auto_res,
        file_name=f"AI市场简报_{datetime.now().strftime('%Y%m%d')}.md",
        mime="text/markdown",
        key="download_market_brief_cached",
    )
# ── 兜底：session_state 丢失但文件缓存存在时，重新加载显示 ──────────────────
else:
    _fallback_content, _fallback_ts = _load_brief_cache()
    if _fallback_content:
        st.session_state["market_brief_latest"] = _fallback_content
        st.rerun()
    elif st.session_state.get("_brief_auto_gen_attempted") and not st.session_state.get("market_brief_latest"):
        st.warning("📭 简报尚未生成成功。请点击「🔄 刷新简报」重试（完整模式含选股引擎）。")
    else:
        st.info("📭 暂无简报数据，请点击「🔄 刷新简报」生成")
# ─────────────────────────────────────────────────────────────────────────────


# 【V92】全量云端搜索 - 从侧边栏移至主区域，作为作战室入口
render_cloud_search()
st.markdown("---")

# 【V77.1调试】检测点击触发 - 添加详细日志
q_input = None
execute_analysis = False

_safe_print(f"[深度作战室] scan_selected_code = {st.session_state.get('scan_selected_code')}")

if st.session_state.get('scan_selected_code'):
    # 从 session_state 读取选中的股票
    q_input = st.session_state.scan_selected_code
    stock_name = st.session_state.scan_selected_name
    execute_analysis = True
    
    _safe_print(f"[深度作战室] ✅ 检测到选中股票: {stock_name} ({q_input}), execute_analysis = {execute_analysis}")
    
    # 明显的提示
    st.success(f"🎯 已自动选中：**{stock_name}** ({q_input})")
    st.caption('[📰 跳转 AI市场简报](#ai-market-brief)（报告在页面底部）')
    
    # 【V82.9新增】显示扫描分析表格
    st.markdown("#### 📊 扫描结果（勾选2-4只股票进行对比）")
    st.caption("💡 提示：以下是该股票的综合评分和策略建议")
    
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
    df_temp = fetch_stock_data(target_c)
    _scan_prog.progress(0.4)
    _scan_status.text("📊 计算指标... (40%)")

    if df_temp is not None:
        m = calculate_metrics_all(df_temp, target_c)
        _scan_prog.progress(0.8)
        _scan_status.text("📊 构建表格... (80%)")
        if m:
                # 判断市场（美股/港股/A股）
                if q_input[0].isalpha(): 
                    sector = "美股"
                elif len(q_input) == 5 or (len(q_input) >= 4 and q_input[0] == '0'): 
                    sector = "港股"
                elif q_input.startswith('6') or q_input.startswith('5'): 
                    sector = "A股(沪)"
                elif q_input.startswith('0') or q_input.startswith('3'): 
                    sector = "A股(深)"
                else: 
                    sector = "其他"
                
                # 长期趋势
                ma200 = m['last'].get('MA200', 0)
                if ma200 > 0 and m['last_price'] > ma200:
                    long_term = "📈 多头"
                elif ma200 > 0 and m['last_price'] < ma200 * 0.9:
                    long_term = "📉 空头"
                else:
                    long_term = "➡️ 震荡"
                
                # 短期趋势
                rsi = m['rsi']
                if rsi > 70:
                    short_term = "🔥 超买"
                elif rsi > 50:
                    short_term = "📈 强势"
                elif rsi > 30:
                    short_term = "📉 弱势"
                else:
                    short_term = "❄️ 超卖"
                
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
                # 【V88·时机闸门】操作指引与趋势引擎对齐（评分内核已时机压分，结论直接复用）
                _action, _stop_target = build_action_guidance(
                    int(m['score']), m.get('rs20'), _pos_pct, _touch,
                    float(m['last_price']), m.get('trade_plan'), trend=m.get('trend_full'))
                scan_result = pd.DataFrame([{
                    "代码": q_input,
                    "名称": stock_name,
                    "市场": sector,
                    "得分": m['score'],
                    "20日动量": f"{m.get('chg20d', 0) or 0:+.1f}%",
                    "RS强度": (f"{m['rs20']:+.1f}" if m.get('rs20') is not None else "—"),
                    "ESG": f"{_esg_t} ({_esg_g})",
                    "长期": long_term,
                    "短期": short_term,
                    "资金": capital,
                    "水位": water_level,
                    "操作指引": _action,
                    "止损/目标": _stop_target,
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
                        "得分": st.column_config.ProgressColumn(
                            "得分",
                            format="%d",
                            min_value=0,
                            max_value=100,
                        ),
                    }
                )

                # 【V99】综合量价趋势判断（8分拆解/9态量价/9段趋势/6级水位/全价位）
                # 复用 cloud_engine（三端同一套引擎），桌面版注入真实板块强度
                try:
                    import cloud_engine as _ce
                    _sec_str = None
                    try:
                        _mt99 = _load_market_temp()
                        _mk99 = "A股" if str(target_c).upper().endswith((".SS", ".SZ")) else ("港股" if str(target_c).upper().endswith(".HK") else "美股")
                        from modules.sector_map import get_sector as _gs99
                        _sname = str(_gs99(target_c, stock_name) or "")
                        _rot99 = (_mt99.get("_rotation") or {}).get(_mk99) or {}
                        if any(_sname and (_h in _sname or _sname in _h) for _h in _rot99.get("hot", [])):
                            _sec_str = 80
                        elif any(_sname and (_h in _sname or _sname in _h) for _h in _rot99.get("cold", [])):
                            _sec_str = 25
                    except Exception:
                        pass
                    _F = _ce.analyze_trend_full(df_temp, sector_strength=_sec_str)
                    if _F:
                        # 【V88·拐点识别】放量+破趋势=拐点，卡片最顶端直接亮出来
                        _turn99 = _F.get("turning") or {}
                        if _turn99.get("side"):
                            (st.error if _turn99["side"] == "top" else st.success)(
                                f"**{_turn99['label']}**：" + "；".join(_turn99["signals"])
                                + f"\n\n👉 {_turn99['prompt']}")
                        with st.expander(f"🔥 综合量价趋势 · {_F['stage']} · 趋势分{_F['total']} · 结论「{_F['conclusion']}」", expanded=True):
                            st.markdown(
                                f"**一句话结论：{_F['conclusion']}** ｜ 操作建议：{_F['action']}\n\n"
                                f"- 趋势总分：**{_F['total']}/100**\n"
                                f"- 趋势阶段：{_F['stage']}\n"
                                f"- 量价状态：{_F['vp']}\n"
                                f"- 水位判断：{_F['water']}（{_F['pos52']}%）→ {_F['water_adv']}\n"
                                f"- MACD状态：{_F['macd_txt']}\n"
                                f"- 均线状态：{_F['ma_state']}（{_F['ma_txt']}）\n"
                                f"- 买入区间：{_F['buy_zone']} ｜ 回踩买点：{_F['pullback']} ｜ 突破加仓：{_F['breakout']}\n"
                                f"- 止损位：{_F['stop']} ｜ 减仓位：{_F['reduce']}\n"
                                f"- 失效条件：{_F['invalid']}")
                            # 【V88·明白话判读】量价/K线/MACD 的事实与判断要点（不是分数）
                            _ro99 = _ce.plain_readout(_F, _turn99 if _turn99.get("side") else None)
                            if _ro99:
                                st.markdown("##### 📖 量价判读（事实+要点，你来拍板）")
                                st.markdown("\n".join(f"- {ln}" for ln in _ro99))
                            _fu99 = None
                            try:
                                _fu99 = _ce.fundamentals(target_c)
                                if _fu99:
                                    st.markdown(f"**🧾 基本面**：`{_fu99['tag']}`  \n{_fu99['line']}")
                            except Exception:
                                pass
                            _pl99 = _ce.horizon_plans(_F, df_temp)
                            if _pl99:
                                st.markdown("##### ⏱ 分期限剧本（短线做T｜中线锚MA55｜长线锚年线）")
                                st.markdown("\n".join(f"- {_pl99[k]}" for k in ("short", "mid", "long") if _pl99.get(k)))
                            with st.expander("📖 术语速查（每个数值高低代表什么，非专业版）"):
                                st.markdown(_ce.GLOSSARY_MD)
                            # 【V88·复制纪要】整段分析一键复制（与云端同格式）
                            _cp99 = _ce.analysis_text(stock_name, target_c, _F, fund=_fu99)
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
                    _tp = analyze_trend_pulse(df_temp, target_c)
                    if _tp:
                        with st.expander(f"🔥 趋势脉搏 · {_tp['stage']} · 趋势分{_tp['score']}", expanded=True):
                            st.markdown(render_trend_pulse_md(_tp, stock_name))
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
    
    # 【V91.9】深度作战室缓存：K 线点击等 rerun 时复用数据，减少 Running 时长与灰屏
    # 【V91.10】统一缓存：交易日15分钟，非交易日24小时
    _cache_key = f"_warroom_{target_c}"
    _cache_ttl = get_smart_cache_ttl('daily')
    import time as _time_module
    _now = _time_module.time()
    _cached = (_cache_key in st.session_state and
               (_now - st.session_state.get(f"{_cache_key}_ts", 0)) <= _cache_ttl)
    if _cached:
        df, data_quality = st.session_state[_cache_key]
        _safe_print(f"[深度作战室] 使用缓存数据 (剩余 {int(_cache_ttl - (_now - st.session_state[f'{_cache_key}_ts']))}s)")
    else:
        try:
            df, data_quality = fetch_stock_data(target_c, return_quality=True)
            _safe_print(f"[深度作战室] 数据获取: df={'有数据' if df is not None else '无数据'}")
            if df is not None:
                st.session_state[_cache_key] = (df, data_quality or {})
                st.session_state[f"{_cache_key}_ts"] = _now
        except Exception as e:
            df, data_quality = None, {}
            _safe_print(f"[深度作战室] 数据异常: {e}")
    
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
        _computed_key = f"_warroom_computed_{target_c}"
        if _cached and _computed_key in st.session_state:
            metrics = st.session_state[_computed_key].get("metrics")
            quant = st.session_state[_computed_key].get("quant")
            mc = st.session_state[_computed_key].get("mc")
            risk_metrics = st.session_state[_computed_key].get("risk_metrics")
            news_headlines = st.session_state[_computed_key].get("news_headlines")
            _safe_print(f"[深度作战室] 使用缓存指标")
        else:
            try:
                _safe_print(f"[深度作战室] 📊 开始计算指标...")
                metrics = calculate_metrics_all(df, target_c)
                quant = calculate_advanced_quant(df)
                mc = monte_carlo_forecast(df)
                risk_metrics = calculate_risk_metrics(df, target_c)
                news_headlines = fetch_news_headlines(target_c)
                if _cache_key in st.session_state:
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
                name='VWAP(20日) 机构成本线',
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
                name='Chandelier多头止损',
                line=dict(color='#10b981', width=1.5, dash='dash'),
                hovertemplate='多头止损: %{y:.2f}<extra></extra>'
            ))
            
            # 空头止损线（红色虚线）
            fig.add_trace(go.Scatter(
                x=df.index,
                y=_ce_short,
                mode='lines',
                name='Chandelier空头止损',
                line=dict(color='#ef4444', width=1.5, dash='dash'),
                hovertemplate='空头止损: %{y:.2f}<extra></extra>'
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
            title="K线图 + 机构作战层 （点击紫色圆点选定入场价位）",
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
            st.markdown('<p style="font-size:12px;color:#FFD700;font-weight:600;">━━ VWAP(20日) 机构成本线</p>', unsafe_allow_html=True)
            st.markdown('<p style="font-size:12px;color:#888;">📖 成交量加权平均价=机构大资金的平均持仓成本。<b>价格在VWAP上方</b>=机构盈利、多头主导；<b>跌破VWAP</b>=机构被套、可能抛售</p>', unsafe_allow_html=True)
        with _chart_note_cols[1]:
            st.markdown('<p style="font-size:12px;color:#10b981;font-weight:600;">┅┅ Chandelier多头止损线</p>', unsafe_allow_html=True)
            st.markdown('<p style="font-size:12px;color:#888;">📖 22日最高价 - 3×ATR = 动态追踪止损。<b>价格跌破此线</b>=趋势可能反转，多头应离场。比固定止损更科学，随趋势自动上移</p>', unsafe_allow_html=True)
        with _chart_note_cols[2]:
            st.markdown('<p style="font-size:12px;color:#ef4444;font-weight:600;">┅┅ Chandelier空头止损线</p>', unsafe_allow_html=True)
            st.markdown('<p style="font-size:12px;color:#888;">📖 22日最低价 + 3×ATR = 空头追踪止损。<b>价格突破此线</b>=下跌趋势可能结束，空头应离场。两线之间=安全通道</p>', unsafe_allow_html=True)
        
        # Chandelier Exit 当前状态速览
        if _chart_ce and _chart_ce.get('ce_long_latest', 0) > 0:
            _ce_signal = _chart_ce.get('signal', '')
            _ce_long_val = _chart_ce.get('ce_long_latest', 0)
            _ce_short_val = _chart_ce.get('ce_short_latest', 0)
            _curr_price = float(df['Close'].iloc[-1])
            _ce_signal_color = "#ef4444" if "跌破" in _ce_signal else ("#10b981" if "突破" in _ce_signal else "#f59e0b")
            st.markdown(f'<div style="background: {_ce_signal_color}15; border-left: 4px solid {_ce_signal_color}; padding: 0.7rem 1rem; border-radius: 4px; margin: 0.5rem 0;"><span style="font-weight:600;">{_ce_signal}</span> &nbsp;|&nbsp; 当前价 <b>{_curr_price:.2f}</b> &nbsp;|&nbsp; 多头止损 <b style="color:#10b981">{_ce_long_val:.2f}</b> &nbsp;|&nbsp; 空头止损 <b style="color:#ef4444">{_ce_short_val:.2f}</b></div>', unsafe_allow_html=True)
        
        # ═══════════════════════════════════════════════════════════════
        # 【V93】财务数据 & 行业背景面板
        # ═══════════════════════════════════════════════════════════════
        st.markdown("---")
        st.markdown("### 📊 财务数据 & 行业背景")
        st.caption("💡 来源: Yahoo Finance · 估值/盈利/资产质量/行业信息")
        _fundamentals_cache_key = f"_fundamentals_{target_c}"
        if _fundamentals_cache_key not in st.session_state:
            with _v88_running("📥 获取财务数据..."):
                st.session_state[_fundamentals_cache_key] = fetch_stock_fundamentals(target_c)
        _fundamentals = st.session_state[_fundamentals_cache_key]
        render_fundamentals_panel(_fundamentals, target_c)

        # ═══════════════════════════════════════════════════════════════
        # 【V93】AI 综合分析（整合: 技术面 + 止损止盈 + 风控 + 行业 + 日线复盘）
        # 文件缓存 + 自动加载 + 强制刷新
        # ═══════════════════════════════════════════════════════════════
        st.markdown("---")
        st.markdown("### 🤖 AI 综合分析")

        _unified_ai_cache_key = f"_unified_ai_{target_c}"

        # 从文件缓存恢复（session_state 没有时）
        if _unified_ai_cache_key not in st.session_state:
            _cached_report, _cached_ts = _load_ai_report_cache(f"stock_{target_c}")
            if _cached_report and isinstance(_cached_report, str):
                st.session_state[_unified_ai_cache_key] = _cached_report

        _has_stock_cache = _unified_ai_cache_key in st.session_state
        if _has_stock_cache:
            _, _sc_ts = _load_ai_report_cache(f"stock_{target_c}")
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
                _rf = _AI_REPORT_CACHE_DIR / f"ai_report_stock_{target_c}.json"
                if _rf.exists():
                    _rf.unlink()
            except Exception:
                pass
            _run_unified_ai = True
            _has_stock_cache = False

        # 自动生成：无缓存时首次自动触发
        _stock_auto_key = f"_stock_ai_auto_{target_c}"
        if not _has_stock_cache and not st.session_state.get(_stock_auto_key) and MY_GEMINI_KEY and not _run_unified_ai:
            st.session_state[_stock_auto_key] = True
            _run_unified_ai = True

        if _run_unified_ai and MY_GEMINI_KEY:
            with _v88_running(f"🤖 Gemini 综合分析中 · 模型: {_ai_model_label()} · 预计 15-30 秒..."):
                try:
                    _curr_p = float(df['Close'].iloc[-1])
                    _last5 = df.tail(5)[['Open','High','Low','Close','Volume']].to_string()
                    _rsi_v = metrics.get('rsi', 50)
                    _score_v = metrics.get('score', 0)
                    _suggestion_v = metrics.get('suggestion', '观望')
                    _sharpe_v = quant.get('sharpe', 'N/A')
                    _maxdd_v = quant.get('max_dd', 'N/A')
                    _pattern_v = metrics.get('pattern', '无')
                    _vwap_v = ""
                    if _chart_predictor:
                        _af = _chart_predictor.calculate_alpha_factors()
                        _rm = _chart_predictor.calculate_risk_engine()
                        _vwap_v = f"VWAP(20日): {_af.get('vwap_20',0):.2f}, 偏离: {_af.get('vwap_deviation',0):+.2f}%, 信号: {_af.get('vwap_signal','无')}"
                        _vwap_v += f"\n止损价(ATR): {_rm.get('stop_loss',0):.2f}, 建议仓位(Kelly): {_rm.get('kelly_position',5):.1f}%, 风险评级: {_rm.get('risk_grade','N/A')}"
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
                        _mdf_g = metrics.get('df')
                        if _mdf_g is not None and len(_mdf_g) > 20:
                            _lc_g = float(_mdf_g['Close'].iloc[-1])
                            _l250g = float(_mdf_g['Low'].tail(250).min())
                            _h250g = float(_mdf_g['High'].tail(250).max())
                            _ppg = (_lc_g - _l250g) / (_h250g - _l250g) * 100 if _h250g > _l250g else 50.0
                            _act_g, _st_g = build_action_guidance(
                                int(_score_v), metrics.get('rs20'), _ppg, 0, _lc_g, metrics.get('trade_plan'),
                                trend=metrics.get('trend_full'))
                            _rs_txt = (f"{metrics['rs20']:+.1f}%" if metrics.get('rs20') is not None else "N/A")
                            _guide_ctx = (f"系统操作指引: {_act_g} | {_st_g}\n"
                                          f"20日动量: {metrics.get('chg20d', 0) or 0:+.1f}% | RS强度(相对大盘): {_rs_txt}")
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
                        _score_ctx = f"""【五维评分归因（总分 {_score_v}/100，用于解释分数来源，勿逐条复述）】
成长质量(CANSLIM) — 达标: {', '.join(_c_ok) or '无'} ｜ 未达标: {', '.join(_c_bad) or '无'}
趋势与动能 — 达标: {', '.join(_s_ok) or '无'} ｜ 未达标: {', '.join(_s_bad) or '无'}
动能维度 {metrics.get('mom_score','N/A')}/100 ｜ ESG {metrics.get('esg_total','N/A')}({metrics.get('esg_grade','N/A')})"""
                    except Exception:
                        pass

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
                    _unified_prompt = f"""你是买方机构的首席分析师，为投委会写一份可直接决策的个股研判。标准对标机构晨会纪要：事实可追溯、推理有链条、结论可执行、风险能证伪。禁止聊天体、行业科普、教科书式铺陈。

【标的】{target_c}

【实时数据】
最新价: {_curr_p:.2f} | RSI: {_rsi_v:.1f} | 综合评分: {_score_v}/100 | 系统建议: {_suggestion_v}
{_guide_ctx}
K线形态: {_pattern_v} | 夏普比率: {_sharpe_v} | 最大回撤: {_maxdd_v}
{_vwap_v}
{_mc_ctx}
{_vol_ctx}
{_score_ctx}

【最近5日行情】
{_last5}
{_fund_ctx}
{_stock_news_ctx}

━━━ 写作纪律（违反任一条即不合格）━━━
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
                    for _chunk in call_gemini_api_stream(_unified_prompt, model_name=GEMINI_MODEL_NAME, max_output_tokens=4096):
                        _unified_result += _chunk
                        _unified_ph.markdown(_unified_result + " ▌")
                    _unified_ph.empty()

                    if _unified_result and not _unified_result.startswith("❌"):
                        st.session_state[_unified_ai_cache_key] = _unified_result
                        _save_ai_report_cache(f"stock_{target_c}", _unified_result)
                    else:
                        st.error(_unified_result or "❌ AI 分析生成失败，请重试")
                except Exception as _uae:
                    st.error(f"❌ AI 综合分析失败: {str(_uae)[:100]}")

        if _unified_ai_cache_key in st.session_state:
            _ua_res = st.session_state[_unified_ai_cache_key]
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

        # ═══════════════════════════════════════════════════════════════
        # 【V90 新增】AI入场顾问 - 点击K线选定入场价 → AI给止损止盈
        # ═══════════════════════════════════════════════════════════════
        st.markdown("---")
        with st.expander("🎯 入场点位选择 & AI止损止盈（可选）", expanded=False):
            st.caption("📖 在K线图上框选/点击选定入场日期，或手动选择。AI给出精确止损止盈方案")
        
        # 解析图表点击事件
        _selected_entry_date = None
        _selected_entry_price = None
        _selected_candle = None
        
        if _kline_event and hasattr(_kline_event, 'selection') and _kline_event.selection:
            _sel = _kline_event.selection
            _sel_points = _sel.get('points', []) if isinstance(_sel, dict) else (getattr(_sel, 'points', []) if hasattr(_sel, 'points') else [])
            if _sel_points and len(_sel_points) > 0:
                _first_point = _sel_points[0]
                _sel_x = _first_point.get('x', None)
                if _sel_x:
                    try:
                        import pandas as pd
                        _sel_date = pd.Timestamp(_sel_x)
                        # 找到对应日期的数据
                        if _sel_date in df.index:
                            _selected_entry_date = _sel_date
                            _selected_entry_price = float(df.loc[_sel_date, 'Close'])
                            _selected_candle = {
                                'Open': float(df.loc[_sel_date, 'Open']),
                                'High': float(df.loc[_sel_date, 'High']),
                                'Low': float(df.loc[_sel_date, 'Low']),
                                'Close': float(df.loc[_sel_date, 'Close']),
                                'Volume': float(df.loc[_sel_date, 'Volume'])
                            }
                            st.success(f"✅ 已从K线图选中：**{_sel_date.strftime('%Y-%m-%d')}** | 收盘价 **{_selected_entry_price:.2f}**")
                    except Exception as _sel_err:
                        logging.warning(f"选点解析失败: {_sel_err}")
        
        # 手动选择（备用 + 微调）
        with st.expander("📅 手动选择日期 / 微调入场价", expanded=(_selected_entry_date is None)):
            _ea_col1, _ea_col2, _ea_col3 = st.columns([2, 2, 1])
            
            # 日期列表（最近60个交易日倒序）
            _date_options = df.index[-60:].tolist()[::-1]
            _date_labels = [d.strftime('%Y-%m-%d (%a)') if hasattr(d, 'strftime') else str(d) for d in _date_options]
            
            with _ea_col1:
                _default_idx = 0
                if _selected_entry_date and _selected_entry_date in _date_options:
                    _default_idx = _date_options.index(_selected_entry_date)
                _manual_date_label = st.selectbox(
                    "选择交易日",
                    options=_date_labels,
                    index=_default_idx,
                    key=f"entry_date_sel_{target_c}",
                    help="选择你打算入场的交易日"
                )
                _manual_date_idx = _date_labels.index(_manual_date_label)
                _manual_date = _date_options[_manual_date_idx]
                _manual_candle = {
                    'Open': float(df.loc[_manual_date, 'Open']),
                    'High': float(df.loc[_manual_date, 'High']),
                    'Low': float(df.loc[_manual_date, 'Low']),
                    'Close': float(df.loc[_manual_date, 'Close']),
                    'Volume': float(df.loc[_manual_date, 'Volume'])
                }
            
            with _ea_col2:
                _default_price = _selected_entry_price if _selected_entry_price else _manual_candle['Close']
                _manual_price = st.number_input(
                    "入场价格（可微调）",
                    min_value=0.01,
                    value=float(_default_price),
                    step=0.01,
                    format="%.2f",
                    key=f"entry_price_input_{target_c}",
                    help="默认为选定日的收盘价，你可以改为你的实际/计划买入价"
                )
            
            with _ea_col3:
                st.markdown("<br>", unsafe_allow_html=True)
                _use_manual = st.checkbox("使用手动选择", value=(_selected_entry_date is None), key=f"use_manual_{target_c}")
            
            # 显示选定K线信息
            _final_date = _manual_date if _use_manual else (_selected_entry_date or _manual_date)
            _final_price = _manual_price if _use_manual else (_selected_entry_price or _manual_price)
            _final_candle = _manual_candle if _use_manual else (_selected_candle or _manual_candle)
            
            _ohlc_cols = st.columns(5)
            with _ohlc_cols[0]:
                st.metric("开盘", f"{_final_candle['Open']:.2f}")
            with _ohlc_cols[1]:
                st.metric("最高", f"{_final_candle['High']:.2f}")
            with _ohlc_cols[2]:
                st.metric("最低", f"{_final_candle['Low']:.2f}")
            with _ohlc_cols[3]:
                st.metric("收盘", f"{_final_candle['Close']:.2f}")
            with _ohlc_cols[4]:
                st.metric("成交量", f"{_final_candle['Volume']:,.0f}")
        
        # 确认入场信息条
        _final_date_str = _final_date.strftime('%Y-%m-%d') if hasattr(_final_date, 'strftime') else str(_final_date)
        st.markdown(f'<div style="background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%); padding: 0.8rem 1.5rem; border-radius: 8px; display: flex; align-items: center; justify-content: space-between;"><div style="color: white;"><span style="font-size: 12px;">📍 入场点位确认</span><br><span style="font-size: 12px; font-weight: 700;">{target_c} @ {_final_price:.2f}</span><span style="font-size: 12px; margin-left: 12px; opacity: 0.85;">({_final_date_str})</span></div></div>', unsafe_allow_html=True)
        
        # AI分析按钮
        if MY_GEMINI_KEY and HAS_PREDICTION_ENGINE:
            _ea_cache_key = f"entry_advisor_{target_c}_{_final_date_str}_{_final_price:.2f}"
            
            _run_ea = st.button(
                "🤖 AI分析止损止盈",
                key=f"btn_entry_advisor_{target_c}",
                type="primary",
                width='stretch',
                help="AI根据支撑位、压力位、均线和量价结构，智能给出止损和多级止盈建议"
            )
            
            if _run_ea:
                _ea_prog = st.progress(0)
                _ea_stat = st.empty()
                
                try:
                    import time as _ea_time
                    _ea_start = _ea_time.time()
                    
                    _ea_stat.text("🤖 AI策略师正在分析你的入场点位... 20%")
                    _ea_prog.progress(0.2)
                    
                    _ea_predictor = _chart_predictor if _chart_predictor else InstitutionalPredictor(df, target_c)
                    _ea_predictor.calculate_alpha_factors()
                    _ea_predictor.calculate_risk_engine()
                    
                    _ea_stat.text("🤖 正在计算止损止盈... 50%")
                    _ea_prog.progress(0.5)
                    
                    _macro_ctx = st.session_state.get('all_markets', {}).get('us_market', {})
                    
                    _ea_result = _ea_predictor.call_gemini_entry_advisor(
                        MY_GEMINI_KEY,
                        entry_price=_final_price,
                        entry_date=_final_date_str,
                        candle_data=_final_candle,
                        model_name=GEMINI_MODEL_NAME,
                        macro_context=_macro_ctx
                    )
                    
                    _ea_elapsed = _ea_time.time() - _ea_start
                    _ea_prog.progress(0.9)
                    _ea_stat.text(f"✅ 分析完成（耗时{_ea_elapsed:.1f}秒）")
                    _ea_time.sleep(0.3)
                    _ea_prog.progress(1.0)
                    _ea_time.sleep(0.3)
                    _ea_prog.empty()
                    _ea_stat.empty()
                    
                    st.session_state[_ea_cache_key] = _ea_result
                
                except Exception as _ea_err:
                    _ea_prog.empty()
                    _ea_stat.empty()
                    st.error(f"❌ AI分析失败: {str(_ea_err)[:80]}")
            
            elif _ea_cache_key in st.session_state:
                _ea_result = st.session_state[_ea_cache_key]
            else:
                _ea_result = None
            
            # 显示AI止损止盈结果
            if _ea_result and _ea_result.get('stop_loss', 0) > 0:
                st.markdown("#### 📊 AI止损止盈方案")
                
                # 入场评分
                _grade = _ea_result.get('entry_grade', '')
                _grade_color = "#10b981" if 'A' in _grade else ("#3b82f6" if 'B' in _grade else ("#f59e0b" if 'C' in _grade else "#ef4444"))
                
                # 价格可视化条
                _sl = _ea_result.get('stop_loss', 0)
                _tp1 = _ea_result.get('take_profit_1', 0)
                _tp2 = _ea_result.get('take_profit_2', 0)
                _sl_pct = ((_final_price - _sl) / _final_price * 100) if _sl > 0 else 0
                _tp1_pct = ((_tp1 - _final_price) / _final_price * 100) if _tp1 > 0 else 0
                _tp2_pct = ((_tp2 - _final_price) / _final_price * 100) if _tp2 > 0 else 0
                
                # 四列展示
                _ea_show_cols = st.columns(4)
                
                with _ea_show_cols[0]:
                    st.markdown(f'<div style="background: {_grade_color}18; border: 2px solid {_grade_color}; padding: 1rem; border-radius: 8px; text-align: center;"><div style="font-size: 12px; color: #888;">入场评分</div><div style="font-size: 12px; font-weight: 800; color: {_grade_color}; margin: 0.3rem 0;">{_grade}</div></div>', unsafe_allow_html=True)
                    st.markdown(f'<p style="font-size:12px;color:#888;">📖 A=绝佳入场点，B=不错可做，C=一般谨慎，D=不建议入场</p>', unsafe_allow_html=True)
                
                with _ea_show_cols[1]:
                    st.markdown(f'<div style="background: #fef2f2; border: 2px solid #ef4444; padding: 1rem; border-radius: 8px; text-align: center;"><div style="font-size: 12px; color: #888;">🔻 止损价</div><div style="font-size: 12px; font-weight: 700; color: #ef4444; margin: 0.3rem 0;">{_sl:.2f}</div><div style="font-size: 12px; color: #ef4444;">-{_sl_pct:.1f}%</div></div>', unsafe_allow_html=True)
                    st.markdown(f'<p style="font-size:12px;color:#888;">📖 {_ea_result.get("stop_loss_reason", "")}</p>', unsafe_allow_html=True)
                
                with _ea_show_cols[2]:
                    st.markdown(f'<div style="background: #f0fdf4; border: 2px solid #10b981; padding: 1rem; border-radius: 8px; text-align: center;"><div style="font-size: 12px; color: #888;">🎯 止盈1（保守）</div><div style="font-size: 12px; font-weight: 700; color: #10b981; margin: 0.3rem 0;">{_tp1:.2f}</div><div style="font-size: 12px; color: #10b981;">+{_tp1_pct:.1f}%</div></div>', unsafe_allow_html=True)
                    st.markdown(f'<p style="font-size:12px;color:#888;">📖 {_ea_result.get("take_profit_1_reason", "")}</p>', unsafe_allow_html=True)
                
                with _ea_show_cols[3]:
                    st.markdown(f'<div style="background: #eff6ff; border: 2px solid #3b82f6; padding: 1rem; border-radius: 8px; text-align: center;"><div style="font-size: 12px; color: #888;">🚀 止盈2（激进）</div><div style="font-size: 12px; font-weight: 700; color: #3b82f6; margin: 0.3rem 0;">{_tp2:.2f}</div><div style="font-size: 12px; color: #3b82f6;">+{_tp2_pct:.1f}%</div></div>', unsafe_allow_html=True)
                    st.markdown(f'<p style="font-size:12px;color:#888;">📖 {_ea_result.get("take_profit_2_reason", "")}</p>', unsafe_allow_html=True)
                
                # 盈亏比可视化
                if _sl_pct > 0 and _tp1_pct > 0:
                    _rr1 = _tp1_pct / _sl_pct
                    _rr2 = _tp2_pct / _sl_pct if _tp2_pct > 0 else 0
                    _rr_color = "#10b981" if _rr1 >= 2 else ("#f59e0b" if _rr1 >= 1.5 else "#ef4444")
                    st.markdown(f'<div style="background: #f8fafc; padding: 0.6rem 1rem; border-radius: 6px; border: 1px solid #e2e8f0; margin-top: 0.5rem;"><span style="font-size:12px;">📐 <b>盈亏比</b>：保守目标 <b style="color:{_rr_color};">{_rr1:.1f}:1</b>{"&nbsp;&nbsp;|&nbsp;&nbsp;激进目标 <b style=" + chr(34) + "color:#3b82f6;" + chr(34) + ">" + f"{_rr2:.1f}:1</b>" if _rr2 > 0 else ""}&nbsp;&nbsp;|&nbsp;&nbsp;持仓周期 <b>{_ea_result.get("hold_period", "")}</b></span></div>', unsafe_allow_html=True)
                    st.caption("📖 盈亏比 = 预期盈利/预期亏损。≥2:1是好交易，<1.5:1不值得冒险")
                
                # 策略总结
                _strategy = _ea_result.get('strategy_summary', '')
                if _strategy:
                    st.markdown(f'<div style="background: linear-gradient(135deg, #1e293b 0%, #334155 100%); padding: 1rem 1.5rem; border-radius: 8px; margin-top: 0.8rem;"><div style="color: #94a3b8; font-size: 12px; margin-bottom: 0.3rem;">📝 AI策略总结</div><div style="color: white; font-size: 12px; line-height: 1.6;">{_strategy}</div></div>', unsafe_allow_html=True)
                    st.caption(f"📌 本报告由 AI 生成 · 模型: {_ai_model_label()}")
            
            elif _ea_result is None:
                st.info("👆 在K线图上选定入场点（框选紫色圆点），或在上方手动选择日期和价格，然后点击按钮获取AI止损止盈建议")
        else:
            if not MY_GEMINI_KEY:
                st.info("💡 配置 DeepSeek API Key 即可使用AI入场顾问")
        
        # 【V88.12】前瞻预测层 - 机构生命线 + AI预测（已整合到AI综合分析，此处折叠备用）
        if HAS_PREDICTION_ENGINE:
            
            with st.expander("📊 机构生命线 & AI预测（详细数据）", expanded=False):
                try:
                    # 【V90】复用已创建的predictor（避免重复计算）
                    predictor = _chart_predictor if _chart_predictor else InstitutionalPredictor(df, target_c)
                    alpha_factors = predictor.calculate_alpha_factors()
                    risk_metrics = predictor.calculate_risk_engine()
                    
                    # 1. VWAP机构生命线
                    st.markdown("#### 💰 机构生命线 (VWAP)")
                    st.caption("成交量加权平均价 - 机构大单平均成本线")
                    
                    vwap_cols = st.columns([2, 2, 3])
                    with vwap_cols[0]:
                        vwap_val = alpha_factors.get('vwap_20', 0)
                        if vwap_val:
                            st.metric("VWAP(20日)", f"{vwap_val:.2f}")
                            st.markdown('<p style="font-size:12px;color:#888;">📖 过去20天机构大单的平均买入成本</p>', unsafe_allow_html=True)
                    
                    with vwap_cols[1]:
                        vwap_dev = alpha_factors.get('vwap_deviation', 0)
                        st.metric("偏离度", f"{vwap_dev:+.2f}%", 
                                 delta_color="normal" if vwap_dev > 0 else "inverse")
                        _vwap_dev_hint = "价格高于机构成本→多头" if vwap_dev > 0 else "价格低于机构成本→空头"
                        st.markdown(f'<p style="font-size:12px;color:#888;">📖 {_vwap_dev_hint}。偏离>5%=强势但追高风险大</p>', unsafe_allow_html=True)
                    
                    with vwap_cols[2]:
                        st.info(alpha_factors.get('vwap_signal', '⚪ 无信号'))
                        st.markdown('<p style="font-size:12px;color:#888;">📖 VWAP信号：强势=安全持有，偏空=警惕回调，弱势=不建议新仓</p>', unsafe_allow_html=True)
                    
                    st.divider()
                    
                    # 2. Alpha因子矩阵
                    st.markdown("#### 🎯 Alpha因子矩阵")
                    st.caption("📖 Alpha因子 = 超越大盘的收益来源。机构用这些因子寻找「别人看不到的信号」")
                    alpha_cols = st.columns(3)
                    
                    with alpha_cols[0]:
                        st.markdown("**RSI背离**")
                        st.info(alpha_factors.get('rsi_divergence', '⚪ 无数据'))
                        st.markdown('<p style="font-size:12px;color:#888;">📖 RSI=相对强弱指标(0-100)。<b>底背离</b>=价格新低但RSI未新低→反弹信号；<b>顶背离</b>=价格新高但RSI未新高→见顶信号</p>', unsafe_allow_html=True)
                    
                    with alpha_cols[1]:
                        st.markdown("**布林带挤压**")
                        st.info(alpha_factors.get('bb_squeeze', '⚪ 无数据'))
                        st.markdown('<p style="font-size:12px;color:#888;">📖 布林带=价格波动通道。<b>极度挤压</b>=波动率极低，即将爆发大行情（方向不定）；<b>扩张</b>=趋势正在展开</p>', unsafe_allow_html=True)
                    
                    with alpha_cols[2]:
                        st.markdown("**量价背离**")
                        st.info(alpha_factors.get('volume_price_divergence', '⚪ 无数据'))
                        st.markdown('<p style="font-size:12px;color:#888;">📖 价格上涨但成交量下降=上涨无力，假突破风险高；价格下跌但成交量萎缩=抛压减弱，可能见底</p>', unsafe_allow_html=True)
                    
                    st.divider()
                    
                    # 3. 风险引擎
                    st.markdown("#### ⚡ 风险引擎 - 动态止损 & 仓位管理")
                    st.caption("📖 风险引擎 = 机构的「安全气囊」。不是帮你赚钱，而是帮你在撞车时活下来")
                    
                    risk_cols = st.columns(4)
                    with risk_cols[0]:
                        stop_loss = risk_metrics.get('stop_loss', 0)
                        if stop_loss:
                            st.metric("止损价", f"{stop_loss:.2f}")
                            st.caption(f"🛡️ {risk_metrics.get('stop_loss_pct', 0):.2f}% ATR止损")
                            st.markdown('<p style="font-size:12px;color:#888;">📖 止损价=当前价-2.5×ATR。跌到此价必须卖出，不抱幻想。这是机构铁律</p>', unsafe_allow_html=True)
                    
                    with risk_cols[1]:
                        kelly_pos = risk_metrics.get('kelly_position', 5)
                        st.metric("建议仓位", f"{kelly_pos:.1f}%")
                        st.caption("📊 Kelly公式计算")
                        st.markdown('<p style="font-size:12px;color:#888;">📖 Kelly公式=数学最优仓位。基于历史胜率和盈亏比。用0.25倍Kelly（保守策略），防止过度自信</p>', unsafe_allow_html=True)
                    
                    with risk_cols[2]:
                        st.metric("风险评级", risk_metrics.get('risk_grade', '未评级'))
                        st.caption("💎 A级最优")
                        st.markdown('<p style="font-size:12px;color:#888;">📖 A级(&lt;3%)=低风险可重仓；B级(3-5%)=正常仓位；C级(5-8%)=轻仓；D级(&gt;8%)=不建议</p>', unsafe_allow_html=True)
                    
                    with risk_cols[3]:
                        atr = risk_metrics.get('atr', 0)
                        if atr:
                            st.metric("ATR波动率", f"{atr:.2f}")
                            st.caption("📈 14日平均真实范围")
                            st.markdown('<p style="font-size:12px;color:#888;">📖 ATR=平均真实波幅，衡量股票每天的「正常」波动幅度。ATR越大=波动越剧烈=止损要设更宽</p>', unsafe_allow_html=True)
                    
                    st.divider()
                    
                    # 4. AI智能风控预测（合并预测+风控，一键触发）
                    st.markdown("#### 🤖 AI智能风控预测")
                    st.caption("💡 基于VWAP、Alpha因子、风险指标，预测未来3-5日走势 + 开仓前风控评估（潜在风险预判、盈亏比、纪律建议）")
                    
                    if MY_GEMINI_KEY:
                        prediction_cache_key = f"ai_prediction_{target_c}"
                        pm_cache_key = f"pre_mortem_{target_c}"
                        
                        run_combined = st.button(
                            "⚡ 启动AI智能风控预测",
                            key=f"btn_ai_pred_{target_c}",
                            type="primary",
                            width='stretch',
                            help="一键获取：AI预测（看涨概率/操作建议）+ 风控评估（三大风险预判 + 盈亏比 + 开仓建议）"
                        )
                        
                        if run_combined:
                            prog = st.progress(0)
                            status = st.empty()
                            try:
                                import time as _t
                                _start = _t.time()
                                status.text(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · 准备数据... 10%")
                                prog.progress(0.1)
                                
                                ai_prediction = predictor.call_gemini_oracle(MY_GEMINI_KEY, GEMINI_MODEL_NAME)
                                st.session_state[prediction_cache_key] = ai_prediction
                                status.text(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · 风控评估... 50%")
                                prog.progress(0.5)
                                
                                _macro_ctx = st.session_state.get('all_markets', {}).get('us_market', {})
                                _pm_predictor = predictor
                                _pm_predictor.calculate_alpha_factors()
                                _pm_predictor.calculate_risk_engine()
                                pm_result = _pm_predictor.call_gemini_pre_mortem(
                                    MY_GEMINI_KEY, GEMINI_MODEL_NAME, macro_context=_macro_ctx
                                )
                                st.session_state[pm_cache_key] = pm_result
                                
                                status.text(f"✅ 分析完成（耗时{_t.time()-_start:.1f}秒）")
                                prog.progress(1.0)
                                _t.sleep(0.5)
                            except Exception as e:
                                st.error(f"❌ AI智能风控预测失败: {str(e)[:100]}")
                                # 清除缓存，避免显示旧结果
                                if prediction_cache_key in st.session_state:
                                    del st.session_state[prediction_cache_key]
                                if pm_cache_key in st.session_state:
                                    del st.session_state[pm_cache_key]
                                ai_prediction = {}
                                pm_result = None
                            finally:
                                prog.empty()
                                status.empty()
                        
                        ai_prediction = st.session_state.get(prediction_cache_key, {})
                        pm_result = st.session_state.get(pm_cache_key)
                        
                        if ai_prediction or pm_result:
                            if ai_prediction:
                                st.markdown("##### 📈 AI预测结果")
                                ai_cols = st.columns([1, 1, 2])
                                with ai_cols[0]:
                                    prob = ai_prediction.get('bullish_prob', 50)
                                    prob_color = "🟢" if prob > 55 else ("🔴" if prob < 45 else "🟡")
                                    st.metric(f"{prob_color} 看涨概率", f"{prob}%")
                                with ai_cols[1]:
                                    st.metric("市场状态", ai_prediction.get('regime', '震荡'))
                                with ai_cols[2]:
                                    st.info(f"**操作建议**: {ai_prediction.get('verdict', '观望')}")
                                st.warning(f"⚠️ **关键风险**: {ai_prediction.get('key_risk', '需关注市场变化')}")
                                st.markdown("---")
                            
                            if pm_result:
                                st.markdown("##### 🛡️ 风控评估 - 潜在风险预判")
                                st.caption("📖 开仓前预判：若这笔交易亏损，最可能的原因")
                                _risk_items = [
                                    ('1', pm_result.get('risk_1', '分析中...')),
                                    ('2', pm_result.get('risk_2', '分析中...')),
                                    ('3', pm_result.get('risk_3', '分析中...'))
                                ]
                                for _ri_num, _ri_text in _risk_items:
                                    st.markdown(f'<div style="background: #fef2f2; border-left: 4px solid #ef4444; padding: 0.8rem 1rem; border-radius: 4px; margin-bottom: 0.5rem;"><span style="font-weight:700; color:#ef4444;">风险 #{_ri_num}</span>：{_ri_text}</div>', unsafe_allow_html=True)
                                st.markdown("##### 🎯 交易纪律")
                                _pm_risk = predictor.risk_metrics if hasattr(predictor, 'risk_metrics') else {}
                                _pm_stop = _pm_risk.get('stop_loss', 0)
                                _pm_kelly = _pm_risk.get('kelly_position', 5)
                                _pm_stop_pct = _pm_risk.get('stop_loss_pct', 0)
                                _rr_ratio = pm_result.get('reward_risk_ratio', 0)
                                _position_amt = 100000 * _pm_kelly / 100 if _pm_kelly else 0
                                _max_loss = _position_amt * _pm_stop_pct / 100 if _pm_stop_pct > 0 else 0
                                disc_cols = st.columns(4)
                                with disc_cols[0]:
                                    st.metric("硬止损价", f"{_pm_stop:.2f}" if _pm_stop > 0 else "N/A")
                                with disc_cols[1]:
                                    st.metric("建议仓位", f"{_pm_kelly:.1f}%")
                                with disc_cols[2]:
                                    st.metric("最大亏损", f"¥{_max_loss:,.0f}" if _max_loss > 0 else "N/A")
                                with disc_cols[3]:
                                    _rr_color = "#10b981" if _rr_ratio >= 2.0 else ("#f59e0b" if _rr_ratio >= 1.5 else "#ef4444")
                                    st.metric("盈亏比", f"{_rr_ratio:.1f}:1" if _rr_ratio > 0 else "N/A")
                                _pm_verdict = pm_result.get('verdict', '分析中...')
                                if '允许开仓' in _pm_verdict:
                                    _verdict_bg = "linear-gradient(135deg, #10b981 0%, #059669 100%)"
                                    _verdict_icon = "🟢"
                                elif '减半' in _pm_verdict:
                                    _verdict_bg = "linear-gradient(135deg, #f59e0b 0%, #d97706 100%)"
                                    _verdict_icon = "🟡"
                                else:
                                    _verdict_bg = "linear-gradient(135deg, #ef4444 0%, #dc2626 100%)"
                                    _verdict_icon = "🔴"
                                st.markdown(f'<div style="background: {_verdict_bg}; padding: 1.2rem 1.5rem; border-radius: 10px; margin-top: 0.5rem;"><div style="color: white; font-size: 12px; font-weight: 700;">{_verdict_icon} 风控判定：{_pm_verdict}</div></div>', unsafe_allow_html=True)
                            st.caption(f"📌 本报告由 AI 生成 · 模型: {_ai_model_label()}")
                        else:
                            st.info("👆 点击上方按钮一键获取AI预测 + 风控评估（预测未来走势 + 开仓前风险预判）")
                    else:
                        st.info("💡 配置 DeepSeek API Key 即可启用AI智能风控预测")
                
                except Exception as e:
                    st.error(f"预测层加载失败: {str(e)[:100]}")
                    logging.error(f"预测层异常: {e}")
        
        # ═══════════════════════════════════════════════════════════════
        # 【V90 新增】一键生成分享卡片（iPhone 17 尺寸）
        # ═══════════════════════════════════════════════════════════════
        if COPY_UTILS_AVAILABLE:
            st.markdown("---")
            
            _card_col1, _card_col2 = st.columns([1, 3])
            with _card_col1:
                _gen_card = st.button("📸 生成卡片", key=f"btn_share_card_{target_c}", type="primary", width='stretch')
            with _card_col2:
                st.caption("包含：价格涨跌、核心指标(VWAP/ATR/Kelly)、AI止损止盈、风控官警告、宏观环境")
            
            if _gen_card:
                with _v88_running("正在生成分享卡片..."):
                    try:
                        # 收集当前所有分析数据
                        _card_price = float(df['Close'].iloc[-1])
                        _card_prev = float(df['Close'].iloc[-2]) if len(df) >= 2 else _card_price
                        _card_chg = ((_card_price - _card_prev) / _card_prev * 100) if _card_prev else 0
                        
                        # 从已有的分析结果中提取数据
                        _card_alpha = _chart_predictor.alpha_factors if _chart_predictor and hasattr(_chart_predictor, 'alpha_factors') else {}
                        _card_risk = _chart_predictor.risk_metrics if _chart_predictor and hasattr(_chart_predictor, 'risk_metrics') else {}
                        
                        # 宏观数据
                        _card_macro = st.session_state.get('all_markets', {}).get('us_market', {})
                        _card_macro_v = _card_macro.get('verdict', '') if _card_macro.get('data_ok', False) else ''
                        
                        # AI入场顾问数据（如果有）
                        _card_ea = None
                        for _ck in st.session_state:
                            if _ck.startswith(f"entry_advisor_{target_c}"):
                                _card_ea = st.session_state[_ck]
                                break
                        
                        # Pre-Mortem数据（如果有）
                        _card_pm = None
                        for _pk in st.session_state:
                            if _pk.startswith(f"pre_mortem_{target_c}"):
                                _card_pm = st.session_state[_pk]
                                break
                        
                        _pm_risks = []
                        if _card_pm:
                            for _rk in ['risk_1', 'risk_2', 'risk_3']:
                                _rv = _card_pm.get(_rk, '')
                                if _rv and '分析中' not in _rv:
                                    _pm_risks.append(_rv)
                        
                        # 提取最近30天收盘价用于迷你走势图
                        _recent_closes = []
                        try:
                            _rc_series = df['Close'].tail(30)
                            _recent_closes = [float(v) for v in _rc_series.values if v == v]  # 排除NaN
                        except Exception:
                            _recent_closes = []
                        
                        # 尝试获取AI市场简报（如果有）
                        _card_brief = ""
                        for _brief_key in st.session_state:
                            if _brief_key.startswith("market_brief_") and isinstance(st.session_state[_brief_key], str):
                                _card_brief = st.session_state[_brief_key][:300]  # 限制长度
                                break
                        
                        # 生成卡片
                        _card_bytes = ShareCardGenerator.generate_stock_card(
                            code=target_c,
                            price=_card_price,
                            change_pct=_card_chg,
                            score=int(m.get('score', 0)) if m else 0,
                            suggestion=m.get('suggestion', '') if m else '',
                            vwap=_card_alpha.get('vwap_20', 0),
                            vwap_dev=_card_alpha.get('vwap_deviation', 0),
                            atr=_card_risk.get('atr', 0),
                            stop_loss=_card_risk.get('stop_loss', 0),
                            kelly_pct=_card_risk.get('kelly_position', 0),
                            risk_grade=_card_risk.get('risk_grade', ''),
                            entry_grade=_card_ea.get('entry_grade', '') if _card_ea else '',
                            ai_stop_loss=_card_ea.get('stop_loss', 0) if _card_ea else 0,
                            ai_tp1=_card_ea.get('take_profit_1', 0) if _card_ea else 0,
                            ai_tp2=_card_ea.get('take_profit_2', 0) if _card_ea else 0,
                            ai_strategy=_card_ea.get('strategy_summary', '') if _card_ea else '',
                            macro_verdict=_card_macro_v,
                            position_cap=_card_macro.get('position_cap', 80),
                            pre_mortem_risks=_pm_risks,
                            recent_prices=_recent_closes,
                            market_brief=_card_brief
                        )
                        
                        st.session_state[f"share_card_{target_c}"] = _card_bytes
                        st.success("✅ 分享卡片已生成！")
                    
                    except Exception as _card_err:
                        st.error(f"❌ 卡片生成失败: {str(_card_err)[:80]}")
            
            # 显示和下载卡片
            _card_cache_key = f"share_card_{target_c}"
            if _card_cache_key in st.session_state:
                _card_data = st.session_state[_card_cache_key]
                
                _show_col1, _show_col2 = st.columns([2, 1])
                with _show_col1:
                    st.image(_card_data, caption=f"{target_c} 分析卡片（iPhone 17 尺寸）", width='stretch')
                with _show_col2:
                    st.download_button(
                        "📥 保存卡片到本地",
                        data=_card_data,
                        file_name=f"StockAI_{target_c}_{datetime.now().strftime('%Y%m%d_%H%M')}.png",
                        mime="image/png",
                        key=f"download_card_{target_c}",
                        width='stretch'
                    )
                    st.caption("💡 保存后可直接在微信/钉钉/朋友圈分享")
                    st.caption("📐 卡片尺寸：1170×2532px（iPhone 17 Pro Max）")
        
        # 【V89.2】机构研究中心 - 个股深度分析
        if INSTITUTIONAL_RESEARCH_AVAILABLE and _institutional_research:
            st.markdown("---")
            from datetime import datetime as _dt_research
            st.markdown(f"### 🏦 机构研究报告 · {_dt_research.now().strftime('%Y-%m-%d')}")
            
            with st.expander("📑 机构深度研究 + 机会风险评估（详细）", expanded=False):
                try:
                    # 【V91.9】机构研究报告缓存：避免每次 rerun 都调 LLM，Running 结束后按钮可正常响应
                    # 【V91.10】统一缓存：交易日15分钟，非交易日24小时
                    _research_cache_key = f"_research_report_{target_c}"
                    _research_ttl = get_smart_cache_ttl('daily')
                    _research_now = _time_module.time()
                    _research_cached = (_research_cache_key in st.session_state and
                        (_research_now - st.session_state.get(f"{_research_cache_key}_ts", 0)) <= _research_ttl)
                    if _research_cached:
                        research_report = st.session_state[_research_cache_key]
                        _safe_print(f"[机构研究] 使用缓存报告")
                    else:
                        # 【V91.10】延迟加载：点击生成才执行，首屏秒开，目标 20 秒内完成
                        _gen_btn_key = f"_research_gen_btn_{target_c}"
                        if st.button("🚀 生成机构研究报告", key=_gen_btn_key, type="primary"):
                            with _v88_running(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · 机构研究报告..."):
                                try:
                                    _all_mkts = st.session_state.get('all_markets', {})
                                    if target_c.endswith('.SS') or target_c.endswith('.SZ'):
                                        market_regime = _all_mkts.get('cn_market', {})
                                    elif target_c.endswith('.HK'):
                                        market_regime = _all_mkts.get('hk_market', {})
                                    else:
                                        market_regime = _all_mkts.get('us_market', {})
                                    _df_r = df.tail(126) if df is not None and len(df) > 126 else df  # 半年数据，加快生成
                                    research_report = _institutional_research.comprehensive_report(
                                        target_c, _df_r, market_regime
                                    )
                                    if research_report and isinstance(research_report, dict) and len(research_report) > 0:
                                        st.session_state[_research_cache_key] = research_report
                                        st.session_state[f"{_research_cache_key}_ts"] = _research_now
                                        st.rerun()
                                    else:
                                        st.error("❌ 报告生成失败（返回为空），请检查数据或稍后重试")
                                        research_report = None
                                except Exception as _re:
                                    st.error(f"❌ 机构研究报告生成异常: {str(_re)[:100]}")
                                    logging.error(f"机构研究报告异常: {_re}")
                                    research_report = None
                        else:
                            research_report = None
                            st.info("👆 点击上方按钮生成机构研究报告（约 20 秒内完成，报告生成后缓存：1小时）")
                    
                    if research_report:
                        # 执行摘要
                        st.markdown("#### 📋 执行摘要")
                        exec_summary = research_report.get('executive_summary', {})
                        
                        summary_cols = st.columns(5)
                        with summary_cols[0]:
                            rating = exec_summary.get('rating', '未评级')
                            rating_color = "#10b981" if '推荐' in rating else "#f59e0b" if '中性' in rating else "#ef4444"
                            st.markdown(f'<div style="background: {rating_color}20; padding: 0.8rem; border-radius: 6px; border-left: 3px solid {rating_color};"><div style="font-size: 12px; color: gray;">综合评级</div><div style="font-size: 12px; font-weight: 600; color: {rating_color}; margin-top: 0.2rem;">{rating}</div></div>', unsafe_allow_html=True)
                        
                        with summary_cols[1]:
                            action = exec_summary.get('action', '观望')
                            st.markdown(f'<div style="background: #3b82f620; padding: 0.8rem; border-radius: 6px; border-left: 3px solid #3b82f6;"><div style="font-size: 12px; color: gray;">操作建议</div><div style="font-size: 12px; font-weight: 600; color: #3b82f6; margin-top: 0.2rem;">{action}</div></div>', unsafe_allow_html=True)
                        
                        with summary_cols[2]:
                            tech_score = exec_summary.get('technical_score', 0)
                            st.metric("技术评分", f"{tech_score}/100", 
                                     delta="优秀" if tech_score >= 70 else "一般" if tech_score >= 50 else "较弱")
                        
                        with summary_cols[3]:
                            opp_score = exec_summary.get('opportunity_score', 0)
                            st.metric("机会评分", f"{opp_score}/100",
                                     delta="高机会" if opp_score >= 70 else "中等" if opp_score >= 50 else "低机会")
                        
                        with summary_cols[4]:
                            risk_score = exec_summary.get('risk_score', 50)
                            st.metric("风险评分", f"{risk_score}/100",
                                     delta="高风险" if risk_score >= 70 else "中等" if risk_score >= 40 else "低风险",
                                     delta_color="inverse")
                        
                        st.divider()
                        
                        # Tab布局：个股研究 | 机会雷达 | 风险预警 | 舆情分析
                        research_tabs = st.tabs(["📊 个股深度研究", "🎯 机会雷达", "⚠️ 风险预警", "📰 舆情分析"])
                        
                        # Tab 1: 个股深度研究
                        with research_tabs[0]:
                            stock_research = research_report.get('stock_research', {})
                            
                            # 趋势分析
                            st.markdown("##### 📈 趋势分析")
                            trend = stock_research.get('trend_analysis', {})
                            trend_col1, trend_col2 = st.columns(2)
                            
                            with trend_col1:
                                st.info(f"**趋势状态**: {trend.get('status', '未知')}")
                                st.metric("趋势评分", f"{trend.get('score', 0)}/100")
                            
                            with trend_col2:
                                st.metric("MA5", f"${trend.get('ma5', 0):.2f}")
                                st.metric("MA20", f"${trend.get('ma20', 0):.2f}")
                                st.metric("MA50", f"${trend.get('ma50', 0):.2f}")
                            
                            st.caption(f"💡 偏离MA20: {trend.get('deviation_from_ma20', 0):+.2f}%")
                            
                            # 动量信号
                            st.markdown("##### ⚡ 动量信号")
                            signals = stock_research.get('momentum_signals', [])
                            if signals:
                                for signal in signals:
                                    if '⚠️' in signal or '🔴' in signal:
                                        st.warning(signal)
                                    elif '✅' in signal or '🟢' in signal:
                                        st.success(signal)
                                    else:
                                        st.info(signal)
                            
                            # 价格目标
                            st.markdown("##### 🎯 价格目标")
                            target = stock_research.get('price_target', {})
                            target_cols = st.columns(4)
                            
                            with target_cols[0]:
                                st.metric("当前价", f"${target.get('current_price', 0):.2f}")
                            with target_cols[1]:
                                st.metric("目标高位", f"${target.get('target_high', 0):.2f}", delta="上涨空间")
                            with target_cols[2]:
                                st.metric("目标低位", f"${target.get('target_low', 0):.2f}", delta="下跌空间", delta_color="inverse")
                            with target_cols[3]:
                                st.metric("止损价", f"${target.get('stop_loss', 0):.2f}", delta="风控线", delta_color="inverse")
                            
                            st.caption(f"⏰ 时间视野: {target.get('time_horizon', '1-2月')}")
                        
                        # Tab 2: 机会雷达
                        with research_tabs[1]:
                            opportunity = research_report.get('opportunity', {})
                            
                            # 机会评分卡
                            st.markdown("##### 🎯 机会评估")
                            opp_col1, opp_col2, opp_col3 = st.columns(3)
                            
                            with opp_col1:
                                opp_level = opportunity.get('opportunity_level', '低')
                                st.markdown(f'<div style="background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%); padding: 1.2rem; border-radius: 8px; color: white;"><div style="font-size: 12px; opacity: 0.9;">机会等级</div><div style="font-size: 12px; font-weight: 600; margin-top: 0.3rem;">{opp_level}</div></div>', unsafe_allow_html=True)
                            
                            with opp_col2:
                                entry_timing = opportunity.get('entry_timing', '观望')
                                st.markdown(f'<div style="background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%); padding: 1.2rem; border-radius: 8px; color: white;"><div style="font-size: 12px; opacity: 0.9;">入场时机</div><div style="font-size: 12px; font-weight: 600; margin-top: 0.3rem;">{entry_timing}</div></div>', unsafe_allow_html=True)
                            
                            with opp_col3:
                                position_size = opportunity.get('position_size_suggestion', '轻仓')
                                st.markdown(f'<div style="background: linear-gradient(135deg, #8b5cf6 0%, #7c3aed 100%); padding: 1.2rem; border-radius: 8px; color: white;"><div style="font-size: 12px; opacity: 0.9;">建议仓位</div><div style="font-size: 12px; font-weight: 600; margin-top: 0.3rem;">{position_size}</div></div>', unsafe_allow_html=True)
                            
                            # 催化剂
                            st.markdown("##### 💡 催化剂分析")
                            catalysts = opportunity.get('catalysts', [])
                            if catalysts:
                                for i, catalyst in enumerate(catalysts, 1):
                                    st.success(f"**{i}.** {catalyst}")
                            else:
                                st.info("暂无明确催化剂")
                            
                            # 最优入场价
                            st.markdown("##### 💰 最优入场价")
                            optimal_price = opportunity.get('optimal_entry_price', 0)
                            st.metric("建议买入价", f"${optimal_price:.2f}")
                            st.caption("💡 根据技术分析和风险收益比计算")
                        
                        # Tab 3: 风险预警
                        with research_tabs[2]:
                            risk_warn = research_report.get('risk_warning', {})
                            
                            # 风险等级
                            st.markdown("##### ⚠️ 风险评估")
                            risk_level = risk_warn.get('risk_level', '中')
                            risk_color = "#ef4444" if '高' in risk_level else "#f59e0b" if '中' in risk_level else "#10b981"
                            
                            risk_col1, risk_col2 = st.columns([1, 2])
                            with risk_col1:
                                st.markdown(f'<div style="background: {risk_color}20; padding: 1.5rem; border-radius: 8px; border: 2px solid {risk_color}; text-align: center;"><div style="font-size: 12px; color: gray;">风险等级</div><div style="font-size: 12px; font-weight: 700; color: {risk_color}; margin-top: 0.5rem;">{risk_level}</div></div>', unsafe_allow_html=True)
                            
                            with risk_col2:
                                st.metric("风险评分", f"{risk_warn.get('risk_score', 50)}/100",
                                         delta="高风险" if risk_warn.get('risk_score', 50) >= 70 else "低风险",
                                         delta_color="inverse")
                                
                                max_dd = risk_warn.get('max_drawdown_tolerance', 0.15)
                                st.metric("最大回撤容忍", f"{max_dd*100:.1f}%")
                            
                            # 风险因素
                            st.markdown("##### 🚨 风险因素")
                            risk_factors = risk_warn.get('risk_factors', [])
                            if risk_factors:
                                for factor in risk_factors:
                                    st.warning(factor)
                            else:
                                st.success("✅ 暂无重大风险因素")
                            
                            # 风险缓释建议
                            st.markdown("##### 🛡️ 风险缓释建议")
                            mitigations = risk_warn.get('risk_mitigation', [])
                            if mitigations:
                                for i, mitigation in enumerate(mitigations, 1):
                                    st.info(f"**{i}.** {mitigation}")
                            
                            # 止损价
                            st.markdown("##### 🔻 止损管理")
                            stop_loss_price = risk_warn.get('stop_loss_price', 0)
                            if stop_loss_price > 0:
                                st.error(f"**严格止损价**: ${stop_loss_price:.2f}")
                                st.caption("⚠️ 跌破该价位应立即止损，不抱幻想")
                        
                        # Tab 4: 舆情分析
                        with research_tabs[3]:
                            st.markdown("##### 📰 AI舆情分析")
                            st.caption("💡 基于最新市场新闻、公司公告、行业动态的综合舆情评估")
                            
                            # 【V89.4】使用舆情分析器生成提示词
                            if SENTIMENT_ANALYZER_AVAILABLE and _sentiment_analyzer and MY_GEMINI_KEY:
                                # 生成按钮（使用session_state缓存）
                                sentiment_cache_key = f"sentiment_{target_c}"
                                
                                run_sentiment = st.button(
                                    "🚀 启动舆情分析", 
                                    key=f"btn_sentiment_{target_c}",
                                    type="primary",
                                    width='stretch',
                                    help="AI分析最新新闻、市场情绪、影响预判"
                                )
                                
                                if run_sentiment:
                                    # 【V89.7 修复】根据股票代码选择正确的市场regime
                                    _all_mkts_sent = st.session_state.get('all_markets', {})
                                    if target_c.endswith('.SS') or target_c.endswith('.SZ'):
                                        _sent_regime = _all_mkts_sent.get('cn_market', {})
                                    elif target_c.endswith('.HK'):
                                        _sent_regime = _all_mkts_sent.get('hk_market', {})
                                    else:
                                        _sent_regime = _all_mkts_sent.get('us_market', {})
                                    
                                    prompt = _sentiment_analyzer.generate_stock_sentiment_prompt(
                                        target_c, df, _sent_regime
                                    )
                                    
                                    # 进度显示
                                    sentiment_progress = st.progress(0)
                                    sentiment_status = st.empty()
                                    
                                    try:
                                        sentiment_status.info(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · 舆情分析...")
                                        sentiment_progress.progress(0.2)
                                        
                                        sentiment_status.info(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · 评估市场情绪...")
                                        sentiment_progress.progress(0.4)
                                        
                                        sentiment_status.info(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · 预判影响...")
                                        sentiment_progress.progress(0.6)
                                        
                                        # 调用AI
                                        ai_response = call_gemini_api(prompt)
                                        sentiment_progress.progress(1.0)
                                        
                                        # 【V91.8】API 返回错误时直接提示，不当作成功报告
                                        if isinstance(ai_response, str) and (ai_response.startswith("❌") or "失败" in ai_response[:20]):
                                            sentiment_progress.empty()
                                            sentiment_status.empty()
                                            st.error(ai_response[:150])
                                        else:
                                            # 解析评分
                                            sentiment_metrics = _sentiment_analyzer.parse_sentiment_score(ai_response)
                                            
                                            # 缓存结果
                                            st.session_state[sentiment_cache_key] = {
                                                'response': ai_response,
                                                'metrics': sentiment_metrics
                                            }
                                            
                                            sentiment_progress.empty()
                                            sentiment_status.empty()
                                    
                                    except Exception as e:
                                        sentiment_progress.empty()
                                        sentiment_status.empty()
                                        st.error(f"❌ 舆情分析失败: {str(e)[:80]}")
                                
                                # 显示结果
                                if sentiment_cache_key in st.session_state:
                                    sentiment_data = st.session_state[sentiment_cache_key]
                                    sentiment_metrics = sentiment_data.get('metrics', {})
                                    ai_response = sentiment_data.get('response', '')
                                    
                                    # 显示舆情评分卡
                                    st.markdown("---")
                                    st.markdown("##### 📊 舆情评分卡")
                                    
                                    sent_col1, sent_col2, sent_col3 = st.columns(3)
                                    
                                    with sent_col1:
                                        score = sentiment_metrics.get('sentiment_score', 50)
                                        color = _sentiment_analyzer.get_sentiment_color(score)
                                        icon = _sentiment_analyzer.get_sentiment_icon(score)
                                        st.markdown(f'<div style="background: {color}20; padding: 1rem; border-radius: 8px; border-left: 4px solid {color};"><div style="font-size: 12px; color: gray;">舆情评分</div><div style="font-size: 12px; font-weight: 700; color: {color}; margin-top: 0.3rem;">{icon} {score}/100</div></div>', unsafe_allow_html=True)
                                    
                                    with sent_col2:
                                        level = sentiment_metrics.get('sentiment_level', '中性')
                                        st.metric("舆情等级", level)
                                    
                                    with sent_col3:
                                        impact = sentiment_metrics.get('short_term_impact', '震荡')
                                        impact_icon = "📈" if impact == '上涨' else "📉" if impact == '下跌' else "📊"
                                        st.metric("短期影响", f"{impact_icon} {impact}")
                                    
                                    st.markdown("---")
                                    
                                    # 显示完整报告
                                    st.markdown("##### 📑 完整舆情报告")
                                    st.markdown("""
                                    <style>
                                    .sentiment-report {
                                        font-size: 12px !important;
                                        line-height: 1.8;
                                        color: #374151;
                                        padding: 1rem;
                                        background-color: #f9fafb;
                                        border-radius: 8px;
                                        border-left: 4px solid #3b82f6;
                                    }
                                    .sentiment-report h2 {
                                        font-size: 12px !important;
                                        font-weight: 600 !important;
                                        margin: 1rem 0 0.5rem 0 !important;
                                        color: #1f2937 !important;
                                    }
                                    </style>
                                    """, unsafe_allow_html=True)
                                    
                                    # 【V90.3】段落级复制
                                    if COPY_UTILS_AVAILABLE:
                                        CopyUtils.render_markdown_with_section_copy(ai_response, key_prefix=f"sent_{target_c}")
                                    else:
                                        st.markdown(f'<div class="sentiment-report">{ai_response}</div>', unsafe_allow_html=True)
                                    st.caption(f"📌 本报告由 AI 生成 · 模型: {_ai_model_label()}")
                                else:
                                    st.info("👆 点击上方按钮启动AI舆情分析（分析新闻、市场情绪、影响预判）")
                            
                            elif not MY_GEMINI_KEY:
                                st.info("💡 配置 DeepSeek API Key 即可启用AI舆情分析功能")
                            else:
                                st.warning("⚠️ 舆情分析模块未加载")
                
                except Exception as e:
                    st.error(f"机构研究报告生成失败: {str(e)[:100]}")
                    logging.error(f"机构研究异常: {e}")
        
        # 【V90.3】独立个股舆情区块已整合到上方「机构研究报告→舆情分析」Tab中，不再重复显示
        
        # 【V93】一键生成完整报告已整合到上方「AI综合分析」
        st.markdown("---")

        # ═══════════════════════════════════════════════════════════════
        # 【V93 精简】综合评分 + 核心量化指标（一屏呈现）
        # ═══════════════════════════════════════════════════════════════
        st.markdown("### 🎯 综合评分 & 量化指标")

        # 【V88.13】交易量异常解读面板
        _vol_anomaly = analyze_volume_anomaly(df)
        if _vol_anomaly:
            _va_color = _vol_anomaly.get("color", "#6b7280")
            _va_trend = _vol_anomaly.get("trend_note", "")
            _va_action = _vol_anomaly.get("action_hint", "")
            st.markdown(
                f'<div style="background:{_va_color}12;border-left:4px solid {_va_color};'
                f'padding:0.9rem 1.1rem;border-radius:6px;margin-bottom:0.8rem;">'
                f'<div style="font-weight:700;color:{_va_color};font-size:13px;">'
                f'📊 交易量异常 · {_vol_anomaly["anomaly_type"]} '
                f'({_vol_anomaly["vol_ratio"]:.1f}x均量 · 日涨跌{_vol_anomaly["price_chg_1d"]:+.1f}%)</div>'
                f'<div style="font-size:12px;color:#374151;margin-top:0.4rem;line-height:1.7;">'
                f'{_vol_anomaly["explanation"]}</div>'
                f'{f"<div style=\'font-size:12px;font-weight:600;color:{_va_color};margin-top:0.35rem;\'>👉 {_va_action}</div>" if _va_action else ""}'
                f'{f"<div style=\'font-size:11px;color:#6b7280;margin-top:0.3rem;\'>{_va_trend}</div>" if _va_trend else ""}'
                f'</div>',
                unsafe_allow_html=True,
            )
            _va_c1, _va_c2, _va_c3, _va_c4 = st.columns(4)
            with _va_c1:
                st.metric("量比(20日)", f"{_vol_anomaly['vol_ratio']:.2f}x")
            with _va_c2:
                st.metric("日涨跌", f"{_vol_anomaly['price_chg_1d']:+.2f}%")
            with _va_c3:
                st.metric("5日量能趋势", f"{_vol_anomaly['vol_trend_5d']:+.1f}%")
            with _va_c4:
                _sig_map = {
                    "bullish": "📈 看涨信号", "bearish": "📉 看跌信号",
                    "caution_up": "⚠️ 缩量涨·谨慎", "caution_down": "🔍 缩量跌·观察",
                    "turning": "🔄 变盘信号", "neutral": "➖ 中性",
                }
                st.metric("量能信号", _sig_map.get(_vol_anomaly["signal"], "中性"))

        # Row 1: 综合评分 + 交易建议 + K线形态
        _s_c1, _s_c2, _s_c3, _s_c4, _s_c5 = st.columns(5)
        with _s_c1:
            st.metric("综合评分", f"{metrics.get('score', 0)}/100", delta=f"{metrics.get('logic', '计算中')}")
        with _s_c2:
            st.metric("交易建议", metrics.get('suggestion', '观望'))
        with _s_c3:
            rsi_val = metrics['rsi']
            _rsi_icon = "🔴" if rsi_val > 70 else ("🟢" if rsi_val < 30 or rsi_val > 50 else "🟡")
            st.metric(f"RSI {_rsi_icon}", f"{rsi_val:.1f}")
        with _s_c4:
            st.metric("夏普比率", quant.get('sharpe', 'N/A'))
        with _s_c5:
            st.metric("最大回撤", quant.get('max_dd', 'N/A'))

        # Row 2: Alpha/Beta + 胜率/盈亏比 + MACD
        _s2_c1, _s2_c2, _s2_c3, _s2_c4, _s2_c5 = st.columns(5)
        alpha_val = risk_metrics.get('alpha', 0) if risk_metrics else 0
        beta_val = risk_metrics.get('beta', 1) if risk_metrics else 1
        with _s2_c1:
            st.metric("Alpha (α)", f"{alpha_val*100:+.2f}%")
        with _s2_c2:
            st.metric("Beta (β)", f"{beta_val:.2f}")
        with _s2_c3:
            st.metric("胜率", quant.get('win_rate', 'N/A'))
        with _s2_c4:
            st.metric("盈亏比", quant.get('pl_ratio', 'N/A'))
        with _s2_c5:
            st.metric("K线形态", metrics.get('pattern', '无')[:8])

        if quant.get('macd_signal'):
            st.caption(f"📊 MACD: {quant.get('macd_signal','N/A')} | 布林带: {quant.get('bb_position','N/A')} | 乖离率: {metrics.get('bias',0):.2f}% | ATR: {metrics.get('atr',0):.2f}")

        st.divider()

        # ═══════════════════════════════════════════════════════════════
        # 【V93 精简】评级引擎（折叠展示）
        # ═══════════════════════════════════════════════════════════════
        with st.expander("🏛️ 评级体系（CANSLIM / 专业投机 / ESG / 长线法宝）", expanded=False):
            c_score1, c_score2 = st.columns(2)
            with c_score1:
                st.markdown("#### CANSLIM 因子")
                st.table(pd.DataFrame(metrics['canslim_rows']))
            with c_score2:
                st.markdown("#### 专业投机原理")
                st.table(pd.DataFrame(metrics['spec_rows']))

            _esg_e = metrics.get('esg_e', 0)
            _esg_s = metrics.get('esg_s', 0)
            _esg_g = metrics.get('esg_g', 0)
            _esg_total = metrics.get('esg_total', 0)
            _esg_grade = metrics.get('esg_grade', 'N/A')
            esg_c1, esg_c2, esg_c3, esg_c4 = st.columns(4)
            with esg_c1:
                st.metric("🌿 环境 (E)", f"{_esg_e}/100")
            with esg_c2:
                st.metric("👥 社会 (S)", f"{_esg_s}/100")
            with esg_c3:
                st.metric("🏛️ 治理 (G)", f"{_esg_g}/100")
            with esg_c4:
                st.metric("📊 ESG综合", f"{_esg_total}/100 ({_esg_grade})")
            _esg_rows = metrics.get('esg_rows', [])
            if _esg_rows:
                st.table(pd.DataFrame(_esg_rows))

            if LongCompounderGate:
                try:
                    lc_gate = LongCompounderGate()
                    lc_result = lc_gate.compute(df, target_c)
                    lc_rows = []
                    lc_score = lc_result.get("long_compounder_score", 50)
                    lc_pass = lc_result.get("passes_long_compounder_gate", False)
                    for fname, fkey, desc in [
                        ("护城河代理", "moat_proxy", "价格稳定性、趋势稳健"),
                        ("ROIC代理", "roic_proxy", "动量与回撤质量"),
                        ("FCF质量代理", "fcf_quality_proxy", "量价配合"),
                        ("利润率稳定代理", "margin_stability_proxy", "低波动=稳定性高"),
                    ]:
                        val = lc_result.get(fkey, 0.5)
                        score_int = int(val * 100)
                        status = "✅" if score_int >= 60 else ("❌" if score_int < 40 else "🟡")
                        lc_rows.append({"要素": fname, "状态": status, "说明": desc, "得分": f"{score_int}/100"})
                    lc_rows.append({"要素": "📊 长线法宝综合", "状态": "✅" if lc_pass else "❌", "说明": "护城河+ROIC+FCF+利润率稳定", "得分": f"{lc_score:.1f}/100"})
                    st.markdown("#### 长线法宝评级")
                    st.table(pd.DataFrame(lc_rows))
                except Exception:
                    pass

            st.caption("📐 **五维综合评分** = CANSLIM × 25% + 专业投机 × 25% + 动能与相对强度 × 20% + ESG × 15% + 风控 × 15%")

        st.divider()
        
        # 【V93】基础指标已整合到上方「综合评分 & 量化指标」面板

        # 蒙特卡洛预测
        if mc:
            st.markdown("#### 🔮 蒙特卡洛推演 (10日)")
            m1, m2, m3 = st.columns(3)
            curr = df['Close'].iloc[-1]
            p90_chg = (mc['p90'] - curr) / curr * 100 if curr > 0 else 0
            p50_chg = (mc['p50'] - curr) / curr * 100 if curr > 0 else 0
            p10_chg = (mc['p10'] - curr) / curr * 100 if curr > 0 else 0
            # 【V87.4】蒙特卡洛预测趋势说明
            def get_trend_desc(change_pct):
                if change_pct > 15:
                    return "大幅上涨"
                elif change_pct > 5:
                    return "温和上涨"
                elif change_pct > -5:
                    return "横盘震荡"
                elif change_pct > -15:
                    return "温和下跌"
                else:
                    return "大幅下跌"
            
            with m1: 
                st.metric("乐观 (P90)", f"{mc['p90']:.2f}", f"{p90_chg:+.1f}%")
                st.caption(f"📈 {get_trend_desc(p90_chg)}")
            with m2: 
                st.metric("中性 (P50)", f"{mc['p50']:.2f}", f"{p50_chg:+.1f}%")
                st.caption(f"📊 {get_trend_desc(p50_chg)}")
            with m3: 
                st.metric("悲观 (P10)", f"{mc['p10']:.2f}", f"{p10_chg:+.1f}%")
                st.caption(f"📉 {get_trend_desc(p10_chg)}")
        
        st.divider()
        
        # 【V93】AI通用问答（日线/周线已整合到AI综合分析）
        st.markdown("#### 💬 AI 问答")
        _qa_col1, _qa_col2 = st.columns([4, 1])
        with _qa_col1:
            q = st.text_input("向 AI 提问", placeholder="如：该股适合长期持有吗？止损位设在哪？", key="qa_input")
        with _qa_col2:
            st.markdown("<br>", unsafe_allow_html=True)
            _qa_go = st.button("🚀 提问", key="btn_qa", type="primary", width='stretch')
        if _qa_go and q and MY_GEMINI_KEY:
            with _v88_running(f"🤖 {_ai_model_label()} 思考中..."):
                curr_price = df['Close'].iloc[-1]
                prompt = f"{target_c} 当前价格 {curr_price:.2f}。问题：{q}\n\n简洁专业回答（300字内），可结合技术面和基本面。"
                result = call_gemini_api(prompt)
                st.info(result)
                st.caption(f"📌 AI 生成 · {_ai_model_label()}")
        elif _qa_go and not q:
            st.warning("请先输入问题")
        
        st.divider()
        
        # 【V83 P1】机构式交易计划与风险预算
        if metrics and metrics.get('trade_plan'):
            trade_plan = metrics['trade_plan']
            st.markdown("### 📋 机构式交易计划")
            st.caption("💡 基于ATR和均线系统计算的专业交易计划，仅供参考，不构成投资建议")
            
            # 交易计划表格
            tp_col1, tp_col2, tp_col3 = st.columns(3)
            
            with tp_col1:
                st.markdown("##### 🎯 入场策略")
                
                # 【V87.4】入场策略趋势判断
                current_price = trade_plan['current_price']
                entry_low = trade_plan['entry_low']
                entry_high = trade_plan['entry_high']
                entry_mid = trade_plan['entry_mid']
                
                # 判断当前价格位置
                if current_price < entry_low:
                    entry_status = "🟢 当前价格偏低，适合买入"
                elif current_price > entry_high:
                    entry_status = "🔴 当前价格偏高，建议等待"
                else:
                    entry_status = "🟡 当前价格在区间内，可考虑"
                
                st.metric("入场区间（低）", f"${entry_low:.2f}")
                st.metric("入场区间（高）", f"${entry_high:.2f}")
                st.metric("最佳入场价", f"${entry_mid:.2f}", 
                         delta=f"{((entry_mid - current_price) / current_price * 100):+.1f}%")
                st.caption(entry_status)
            
            with tp_col2:
                st.markdown("##### 🛡️ 风险控制")
                
                # 【V87.15修复】风险判断结合评分和止损距离
                risk_pct = trade_plan['risk_per_share'] / current_price * 100
                score = metrics.get('score', 50)
                
                # 综合评分和止损距离判断风险
                if score >= 75:
                    # 高分股票：风险较低
                    if risk_pct > 20:
                        risk_status = "🟡 止损较宽，但基本面优秀"
                    else:
                        risk_status = "🟢 风险较低，基本面优秀"
                elif score >= 60:
                    # 中高分股票：风险适中
                    if risk_pct > 15:
                        risk_status = "🟡 风险适中，止损较宽"
                    else:
                        risk_status = "🟢 风险可控，基本面良好"
                else:
                    # 低分股票：风险较高
                    if risk_pct > 15:
                        risk_status = "🔴 风险较高，谨慎操作"
                    elif risk_pct > 10:
                        risk_status = "🟡 风险适中，需谨慎"
                    else:
                        risk_status = "🟢 止损较紧，风险可控"
                
                st.metric("止损位", f"${trade_plan['stop_loss']:.2f}",
                         delta=f"-{trade_plan['risk_per_share']:.2f} ({risk_pct:.1f}%)")
                st.metric("单股风险", f"${trade_plan['risk_per_share']:.2f}")
                st.metric("风险预算", f"{trade_plan['risk_budget_pct']:.1f}%", help="单笔交易风险占总资金比例")
                st.caption(risk_status)
            
            with tp_col3:
                st.markdown("##### 🎁 目标获利")
                
                # 【V87.4】目标获利趋势判断
                risk_reward_ratio = trade_plan['risk_reward_ratio']
                reward_15r_pct = trade_plan['reward_15r'] / current_price * 100
                reward_2r_pct = trade_plan['reward_2r'] / current_price * 100
                
                if risk_reward_ratio >= 2.0:
                    rr_status = "🟢 优秀盈亏比，值得考虑"
                elif risk_reward_ratio >= 1.5:
                    rr_status = "🟡 良好盈亏比，可接受"
                elif risk_reward_ratio >= 1.0:
                    rr_status = "🟡 基本盈亏比，谨慎考虑"
                else:
                    rr_status = "🔴 盈亏比偏低，不建议"
                
                st.metric("目标位1 (1.5R)", f"${trade_plan['take_profit_15r']:.2f}",
                         delta=f"+{trade_plan['reward_15r']:.2f} ({reward_15r_pct:.1f}%)")
                st.metric("目标位2 (2R)", f"${trade_plan['take_profit_2r']:.2f}",
                         delta=f"+{trade_plan['reward_2r']:.2f} ({reward_2r_pct:.1f}%)")
                st.metric("盈亏比", f"{risk_reward_ratio:.2f}:1")
                st.caption(rr_status)
            
            st.divider()
            
            # 【V83 P1.5】风险预算仓位建议
            st.markdown("##### 💰 仓位建议（基于风险预算）")
            st.info(f"""
**建议仓位上限**：{trade_plan['max_position']} 股（约 ${trade_plan['position_value']:,.0f}）

📌 **计算逻辑**：
- 总资金：$100,000（可在代码中调整）
- 风险预算：{trade_plan['risk_budget_pct']:.1f}%（单笔最大亏损 $1,000）
- 单股风险：${trade_plan['risk_per_share']:.2f}（当前价 - 止损价）
- 最大仓位 = 风险预算金额 ÷ 单股风险

⚠️ **注意**：这是理论最大仓位，实际操作应结合资金管理策略分批建仓。
            """)
        
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




# ═══════════════════════════════════════════════════════════════
# 【模块 ③】深度作战室 + 猎手战位 + Top30（作战室为首 Tab，点击后自动切换）
# ═══════════════════════════════════════════════════════════════
# 【V90.7】深度作战室作为第一个 Tab，解决"点击无反应"——选中后自动显示
tab_warroom, tab_scanner, tab_top30, tab_ai_select, tab_watchlist = st.tabs(["⚔️ 深度作战室", "📡 猎手战位", "🏆 Top30 扫描", "🤖 AI选股", "📋 自选股分析"])

# 【V90.7】深度作战室 Tab - 完整分析内容在顶部区块渲染，此处仅占位
with tab_warroom:
    _warroom_code = st.session_state.get('scan_selected_code')
    if _warroom_code:
        st.success(f"🎯 正在分析：**{st.session_state.get('scan_selected_name', '')}** ({_warroom_code})")
    # 无选中时空白，顶部深度作战室区块会显示分析内容

# 【V89.7】模块独立化 - 各Tab互不影响
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

        # 【V99.5】自动一键全选：打开页面时，若已过锚点(9:00/16:00/22:30)且今日该时点未跑，
        # 且缓存已过1小时 → 自动扫中美港全部。每天最多3次，省流量。
        try:
            _due = _autoscan_due_slot()
            _sr0 = st.session_state.get('scanner_results') or {}
            _fresh0 = (_sr0.get('type') == 'unified'
                       and (time.time() - _sr0.get('scan_timestamp', 0)) < 3600)
            if _due and not _fresh0 and not st.session_state.get('_autoscan_running'):
                st.session_state['_autoscan_running'] = True
                st.info(f"🕘 已过 {_due} 自动时点，正在自动执行「中美港一键全选」（每日3次·省流量）…")
                _as_bar = st.progress(0); _as_txt = st.empty(); _as_t0 = time.time()

                def _as_cb(cur, total, name):
                    _as_bar.progress(min(1.0, cur / max(1, total)))
                    _el = time.time() - _as_t0
                    _eta = (_el / cur * (total - cur)) if cur > 3 else 0
                    _as_txt.text(f"⏱ 自动全选 已用{_el:.0f}s·剩余约{_eta:.0f}s ｜ {cur}/{total} - {name}")
                try:
                    _ap = list(RAW_US) + list(RAW_HK) + list(RAW_CN_TOP)
                    _arows, _ast, _amt = run_unified_scan(_ap, "美股", "平衡", True, progress_callback=_as_cb)
                    _arows = [{"市场": market_of_code(r.get("代码", "")), **r} for r in (_arows or [])]
                    _as_bar.empty(); _as_txt.empty()
                    st.session_state.scanner_results = {
                        'type': 'unified', 'scan_market': '🌍 中美港全部', 'risk_preference': '平衡',
                        'title': '#### 🔍 全策略一页榜单 (🌍 中美港全部 · 自动)', 'caption': '',
                        'data': _arows, 'stats': _ast, 'key': 'unified_table',
                        'scan_timestamp': time.time()}
                    _save_scan_cache_to_file(st.session_state.scanner_results)
                    _autoscan_mark(_due)
                    st.toast(f"✅ 自动全选完成（今日第{len(_autoscan_state()['done'])}/3次）", icon="🎯")
                except Exception as _ae:
                    _as_bar.empty(); _as_txt.empty()
                    st.caption(f"自动全选异常：{str(_ae)[:60]}")
                finally:
                    st.session_state['_autoscan_running'] = False
        except Exception:
            pass

        
        # 【V87.1】显示股票池大小和来源（安全限流模式）
        us_count, hk_count, cn_count = len(RAW_US), len(RAW_HK), len(RAW_CN_TOP)
        total_count = us_count + hk_count + cn_count
        
        # 判断是否使用云端数据（阈值降低到50）
        is_cloud_us = us_count >= 50
        is_cloud_hk = hk_count >= 50
        is_cloud_cn = cn_count >= 50
        
        source_icon = "☁️" if (is_cloud_us and is_cloud_hk and is_cloud_cn) else "💾"
        source_text = "云端实时" if (is_cloud_us and is_cloud_hk and is_cloud_cn) else "本地备用"
        
        _as_done = len(_autoscan_state().get("done", []))
        st.caption(f"{source_icon} **股票池来源**: {source_text} | 美股 {us_count} 只 | 港股 {hk_count} 只 | A股 {cn_count} 只 | 总计 {total_count} 只 | 📦 1小时缓存 | 🕘 自动全选 09:00/16:00/22:30（今日已 {_as_done}/3）")
        
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
            do_scan_top = c_btn4.button("🏆 综合评分", help="多维度量化评分，不限均线", width='stretch')
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
                        'caption': "💡 一表看全：得分=五维综合评分×时机修正（**高分=现在值得买**；高位背离/减仓类自动降分，持仓提醒看操作指引）| 20日动量=近月自身涨幅 | RS强度=近月跑赢大盘幅度 | 52周位置：+100贴近52周高点 / -100贴近低点 | 多重支撑=同时靠近≥2条均线 | MACD/量价：明显放量≥+20%/明显缩量≤-20% | 操作指引（🟢买入区 · 🟡持有减仓/别追 · 🔵持有 · ⚪回避观望）| 止损/目标：破止损离场，到目标减仓",
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
                    'caption': "💡 适合波段交易，持仓1-3周，中期趋势确认，胜率更高",
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
                    'caption': "💡 适合价值投资，持仓1-3月，长期支撑位，适合分批建仓",
                    'data': res, 'stats': stats, 'key': 'ma120_table',
                    'scan_timestamp': time.time(),
                }
                _save_scan_cache_to_file(st.session_state.scanner_results)
        
        if do_scan_top:
            if _scan_cache_hit('top'):
                st.toast("📦 使用缓存，无需重新扫描", icon="📦")
            else:
                pool = _pick_pool()
                res, stats = run_scan("TOP", None, pool, use_concurrent, "综合评分 Top", "🏆")
                st.session_state.scanner_results = {
                    'type': 'top', 'scan_market': scan_market,
                    'title': f"#### 🏆 综合评分 Top 榜单 ({scan_market})",
                    'caption': "💡 多维度量化评分：CANSLIM + 专业投机原理 + 技术指标，不限均线",
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
                            if m and m['score'] > 35:  # 【V86】从45降到35
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
                                    _safe_print(f"[多重支撑] ✅ {code} ({name}): 触及{touch_count}条均线 - {', '.join(touch_mas)}, 评分={m['score']}")
                                
                                # 只有触及2条或以上均线才算"多重支撑"
                                if touch_count >= 2:
                                    res_combined.append({
                                        "市场": mkt_label, "代码": code, "名称": name,
                                        "评分": m['score'], "策略": m['logic'],
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
            
                res_combined = sorted(res_combined, key=lambda x: x['评分'], reverse=True)[:50]
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
            result_info = st.session_state.scanner_results

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
                                f"分{_r9.get('得分', _r9.get('评分',''))} {_r9.get('拐点','')} {_r9.get('操作指引','')}".strip())
                        st.code("\n".join(_cp_lines), language=None)
            
            # 【NEW V88 Phase 2】表格筛选功能
            df_results = pd.DataFrame(result_info['data'])

            if not df_results.empty:
                # 【V99.6】中美港分市场排名：先按全量结果算「市场排名」（得分降序、同分RS靠前），
                # 再提供市场筛选——选单一市场时即是该市场的完整名次
                if '市场' in df_results.columns and '得分' in df_results.columns:
                    df_results['市场排名'] = (df_results.groupby('市场')['得分']
                                          .rank(ascending=False, method='first').astype(int))
                    _cols_mr = df_results.columns.tolist()
                    _cols_mr.insert(_cols_mr.index('市场') + 1, _cols_mr.pop(_cols_mr.index('市场排名')))
                    df_results = df_results[_cols_mr]
                    _mkt_opts = ["🌍 全部"] + [m for m in ("🇺🇸美股", "🇨🇳A股", "🇭🇰港股")
                                             if m in df_results['市场'].unique()]
                    _sel_mkt = st.selectbox("🌏 市场筛选（看单一市场排名）", _mkt_opts,
                                            key=f"filter_market_{result_info['key']}")
                    if _sel_mkt != "🌍 全部":
                        df_results = df_results[df_results['市场'] == _sel_mkt]

                # 筛选器
                n_cols = 3 if (result_info.get('type') == 'regime' and 'pool_assignment' in df_results.columns
                    and result_info.get('meta', {}).get('use_potential_engine', False)) else 2
                filter_cols = st.columns(n_cols)
                
                with filter_cols[0]:
                    # 行业筛选（用户要求：板块→行业）
                    col_name = '行业' if '行业' in df_results.columns else '板块'
                    if col_name in df_results.columns:
                        industries = ['全部'] + sorted(df_results[col_name].unique().tolist())
                        selected_industry = st.selectbox("🏷️ 筛选行业", industries, key=f"filter_industry_{result_info['key']}")
                        if selected_industry != '全部':
                            df_results = df_results[df_results[col_name] == selected_industry]
                
                with filter_cols[1]:
                    # 三池筛选（仅双引擎模式显示）
                    if (result_info.get('type') == 'regime' and 'pool_assignment' in df_results.columns
                        and result_info.get('meta', {}).get('use_potential_engine', False)):
                        pool_options = ['全部', 'A-已验证强势', 'B-预期差潜力', 'C-左侧观察']
                        selected_pool = st.selectbox("🏊 三池筛选", pool_options, key=f"filter_pool_{result_info['key']}")
                        if selected_pool != '全部':
                            pool_map = {'A-已验证强势': 'A', 'B-预期差潜力': 'B', 'C-左侧观察': 'C'}
                            df_results = df_results[df_results['pool_assignment'] == pool_map[selected_pool]]
                    elif '得分' in df_results.columns:
                        # 手机友好：下拉代替滑块
                        score_band = st.selectbox(
                            "📊 得分筛选",
                            ["全部", "≥70 强势", "≥55 良好", "≥40 及格"],
                            key=f"filter_score_band_{result_info['key']}",
                            help="快速筛选高分股"
                        )
                        _min_map = {"≥70 强势": 70, "≥55 良好": 55, "≥40 及格": 40}
                        if score_band in _min_map:
                            df_results = df_results[df_results['得分'] >= _min_map[score_band]]
                
                if n_cols >= 3:
                    with filter_cols[2]:
                        if '得分' in df_results.columns:
                            score_band2 = st.selectbox(
                                "📊 得分筛选",
                                ["全部", "≥70 强势", "≥55 良好", "≥40 及格"],
                                key=f"filter_score_band2_{result_info['key']}",
                            )
                            _min_map2 = {"≥70 强势": 70, "≥55 良好": 55, "≥40 及格": 40}
                            if score_band2 in _min_map2:
                                df_results = df_results[df_results['得分'] >= _min_map2[score_band2]]
                
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
        pid = int(_SCAN_PID_FILE.read_text().strip())
        os.kill(pid, 0)     # 不抛异常说明进程存活
        return True
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

    if not MY_DEEPSEEK_KEY:
        fb = _macro_risk_fallback("DeepSeek API Key 未配置")
        st.session_state["_macro_risk_result"] = fb
        return fb

    today = datetime.now().strftime("%Y年%m月%d日")
    prompt = f"""今天是 {today}。以全球宏观对冲基金视角评估当前市场风险。

直接输出JSON，不要代码块、不要注释、不要多余文字：
{{"risk_level":3,"risk_label":"中等风险","summary":"一句话宏观概述","key_risks":["风险A","风险B","风险C"],"hot_sectors":["板块1","板块2"],"warn_sectors":["板块1","板块2"],"bias":"均衡","bias_reason":"简短理由"}}

risk_level为1-5整数，其余字段用简短中文填写。"""

    _err_msg = ""
    try:
        raw = str(call_gemini_api(prompt) or "").strip()
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


with tab_top30:

    @st.fragment(run_every=20)
    def _top30_fragment():
        # ── 心跳：告知 scan_worker 页面仍在线 ────────────────────
        _scan_write_heartbeat()

        # ── 读取当前状态 ──────────────────────────────────────────
        _prog   = _scan_read_progress()
        _status = _prog.get("status", "idle")
        _pct    = _prog.get("pct", 0)
        _detail = _prog.get("detail", "")
        _res    = _scan_read_results()           # None → 无有效结果

        # ── 标题行 ────────────────────────────────────────────────
        _h_col1, _h_col2, _h_col3 = st.columns([4, 2, 2])
        with _h_col1:
            st.markdown(
                '<p style="font-size:13px;font-weight:700;margin-bottom:0.2rem;">' +
                '🏆 Top30 后台扫描 · 三市场 × 四策略</p>',
                unsafe_allow_html=True,
            )
        with _h_col3:
            if st.button("📲 推送钉钉", key="top30_dingtalk_push",
                         width='stretch',
                         help="一键把 Top30 推荐发送到钉钉群"):
                with _v88_running("推送中..."):
                    _ok, _msg = _dingtalk_push_top30(_res)
                if _ok:
                    st.toast("✅ 已推送到钉钉", icon="📲")
                else:
                    st.toast(f"❌ 推送失败：{_msg}", icon="⚠️")
        with _h_col2:
            _lbl = _scan_result_label()
            if _lbl:
                st.caption(_lbl)

        # ── 进行中：进度条 ────────────────────────────────────────
        if _status == "running" or (_scan_worker_running() and _res is None):
            st.info(f"⏳ 后台扫描中… {_detail}")
            st.progress(min(_pct, 99) / 100)
            st.caption("扫描完成后结果将自动展示，页面将每 20 秒自动刷新")
            return

        # ── 无结果 / 过期：展示启动区域 ───────────────────────────
        if _res is None:
            if _GIST_ID:
                st.info("🔍 尚无扫描结果。云端 GitHub Actions 每天自动扫描4次，结果将自动同步。也可点下方按钮立即在本机后台扫描。")
            else:
                st.info("🔍 尚无扫描结果。点击下方按钮在**后台**启动全市场扫描（约 5-8 分钟），期间可正常使用其他功能。")
            _us_c, _hk_c, _cn_c = len(RAW_US), len(RAW_HK), len(RAW_CN_TOP)
            st.caption(f"扫描池: 美股 {_us_c} + 港股 {_hk_c} + A股 {_cn_c} = {_us_c+_hk_c+_cn_c} 只 · 结果缓存 8 小时")
            _btn_col1, _btn_col2 = st.columns([2, 1])
            with _btn_col1:
                if st.button("🚀 启动后台全市场扫描", type="primary", width='stretch', key="top30_start_bg"):
                    _scan_start_worker(force=False)
                    st.toast("✅ 后台扫描已启动，约 5-8 分钟后结果自动刷新", icon="🚀")
                    st.rerun()
            with _btn_col2:
                if st.button("🔄 强制重扫", width='stretch', key="top30_force_bg",
                             help="清除缓存并重新扫描"):
                    _scan_force_clear()
                    _scan_start_worker(force=True)
                    st.toast("🔄 已清除缓存，强制重新扫描", icon="🔄")
                    st.rerun()
            return

        # ── 有结果：4-Tab 展示 ────────────────────────────────────
        _ts_str  = datetime.fromtimestamp(_res["timestamp"]).strftime("%m-%d %H:%M")
        _is_gist = _GIST_ID and _gist_local_cache.get("timestamp") == _res.get("timestamp")
        _is_stale = _res.get("_stale", False)
        _src_tag = "☁️ 云端缓存" if _is_gist else "💻 本地扫描"
        if _is_stale:
            _src_tag += " (待更新)"
        _sync_st = _gist_sync_status()
        _lbl = _scan_result_label()
        if _is_stale and _lbl == "":
            _lbl = "⏰ 等待云端自动更新"
        st.caption(f"📅 扫描完成于 {_ts_str}  ·  {_src_tag}  ·  {_lbl}  ·  20s 自动刷新")
        if _is_stale:
            st.info("☁️ 云端数据等待 GitHub Actions 下一轮自动更新（每6小时），当前展示最近一次结果。")
        if _GIST_ID:
            _sync_color = "#10b981" if _gist_last_sync_ok else "#f59e0b"
            st.markdown(f'<p style="font-size:11px;color:{_sync_color};margin:0">{_sync_st}</p>',
                        unsafe_allow_html=True)

        _btn_c1, _btn_c2 = st.columns([8, 1])
        with _btn_c2:
            if st.button("🔄 重扫", key="top30_rescan",
                         help="清除缓存并重新扫描", width='stretch'):
                _scan_force_clear()
                _scan_start_worker(force=True)
                st.toast("🔄 已触发重新扫描", icon="🔄")
                st.rerun()

        # ── 宏观风险面板（Gemini 实时评估）──────────────────────────
        # 重试按钮（在 expander 外，方便点击）
        _macro_retry_col, _ = st.columns([1, 8])
        with _macro_retry_col:
            if st.button("🔄", key="macro_risk_retry", help="重新获取宏观风险评估"):
                st.session_state.pop("_macro_risk_result", None)
                try:
                    _MACRO_RISK_CACHE_FILE.unlink(missing_ok=True)
                except Exception:
                    pass
                st.rerun()

        _mr = _fetch_macro_risk()
        _rl  = _mr.get("risk_level", 3)
        _rc  = _mr.get("risk_color", "#6b7280")
        _rlb = _mr.get("risk_label", "")
        _bias= _mr.get("bias", "均衡")
        _bias_icons = {"进攻": "⚔️", "均衡": "⚖️", "防御": "🛡️"}
        _bias_icon  = _bias_icons.get(_bias, "⚖️")
        _risk_bar   = "█" * _rl + "░" * (5 - _rl)
        _mr_error   = _mr.get("_error", "")

        with st.expander(
            f"🌍 宏观风险评估  {_risk_bar}  等级 {_rl}/5 · {_rlb}  |  {_bias_icon} 建议：{_bias}",
            expanded=(_rl >= 3)
        ):
            # 摘要
            _summary = _mr.get("summary", "")
            if _summary:
                st.markdown(
                    f'<div style="background:{_rc}18;border-left:4px solid {_rc};'
                    f'padding:10px 14px;border-radius:6px;font-size:13px;'
                    f'color:#1e293b;margin-bottom:10px;">'
                    f'<b>📋 宏观背景</b><br>{_summary}</div>',
                    unsafe_allow_html=True
                )
            # 真实错误信息（便于诊断）
            if _mr_error:
                st.markdown(
                    f'<div style="font-size:11px;color:#ef4444;padding:4px 8px;'
                    f'background:#fef2f2;border-radius:4px;margin-bottom:6px;">'
                    f'⚠️ API 错误：{_mr_error}<br>'
                    f'<span style="color:#6b7280">点击上方 🔄 按钮重试</span></div>',
                    unsafe_allow_html=True
                )
            # 三列：关键风险 / 受益板块 / 受压板块
            _mc1, _mc2, _mc3 = st.columns(3)
            with _mc1:
                st.markdown("**⚠️ 关键风险**")
                for r in _mr.get("key_risks", []):
                    st.markdown(f"<small>• {r}</small>", unsafe_allow_html=True)
            with _mc2:
                st.markdown("**🚀 受益板块**")
                for s in _mr.get("hot_sectors", []):
                    st.markdown(f"<small style='color:#10b981'>▲ {s}</small>", unsafe_allow_html=True)
            with _mc3:
                st.markdown("**🔻 受压板块**")
                for s in _mr.get("warn_sectors", []):
                    st.markdown(f"<small style='color:#ef4444'>▼ {s}</small>", unsafe_allow_html=True)
            # 操作建议
            _br = _mr.get("bias_reason", "")
            if _br:
                _bias_bg = {"进攻": "#10b981", "均衡": "#3b82f6", "防御": "#ef4444"}
                _bbg = _bias_bg.get(_bias, "#6b7280")
                st.markdown(
                    f'<div style="background:{_bbg};color:white;padding:8px 14px;'
                    f'border-radius:6px;font-size:12px;margin-top:8px;">'
                    f'{_bias_icon} <b>操作建议 · {_bias}</b>：{_br}</div>',
                    unsafe_allow_html=True
                )
            st.caption(f"⏱ 宏观评估每6小时刷新 · 由 Gemini AI 生成 · 仅供参考，不构成投资建议")

        # ── 4-Tab 扫描结果 ────────────────────────────────────────
        # 把受压板块传给渲染函数，用于标注高风险个股
        _warn_sectors_set = set(_mr.get("warn_sectors", []))

        _t1, _t2, _t3, _t4 = st.tabs(
            ["🔥 趋势强势", "🎯 蓄势潜伏", "🎯 拐点Top10（赔率）", "🚀 启动Top10（胜率）"]
        )

        def _sector_warn_tag(industry: str) -> str:
            """如果个股行业在宏观受压板块中，返回警示标记"""
            if not industry or not _warn_sectors_set:
                return ""
            for ws in _warn_sectors_set:
                if ws in industry or industry in ws:
                    return " ⚠️"
            return ""

        def _render_market_col(col, items, mkt_label, key_prefix, tab_type="top"):
            """渲染单市场结果列（含宏观风险行业警示）
            tab_type: 'top'=趋势强势, 'coil'=蓄势潜伏, 'inflection'=拐点, 'breakout'=启动
            """
            with col:
                bm = _res.get(mkt_label[-2:] if mkt_label.endswith(("美股","港股","A股")) else mkt_label, {})
                st.markdown(
                    f'<p style="font-size:12px;font-weight:600;margin-bottom:4px;">{mkt_label}</p>',
                    unsafe_allow_html=True,
                )
                if not items:
                    st.caption("暂无符合条件标的")
                    if "港" in mkt_label or "HK" in mkt_label:
                        if tab_type == "top":
                            # 趋势强势：Risk Off时正常没有，给出解释
                            st.markdown(
                                '<div style="font-size:11px;color:#f59e0b;line-height:1.5;">'
                                '⚠️ 港股当前偏弱（Risk Off），趋势多头标的稀少。'
                                '<br>👉 可看「蓄势潜伏」找筑底机会，或「拐点Top10」低吸。</div>',
                                unsafe_allow_html=True
                            )
                        elif tab_type == "coil":
                            # 蓄势潜伏：Risk Off也应该有，若仍空说明数据刚扫描/阈值未过
                            st.markdown(
                                '<div style="font-size:11px;color:#6b7280;line-height:1.5;">'
                                '暂无达标蓄势标的（需量缩+ATR收缩+守住支撑同时满足）。'
                                '<br>👉 可点「🔄 重扫」触发新一轮扫描。</div>',
                                unsafe_allow_html=True
                            )
                    return
                # 在"信号"列追加宏观行业警示标
                items_display = []
                for it in items:
                    it2 = dict(it)
                    tag = _sector_warn_tag(it2.get("行业", ""))
                    if tag and "信号" in it2:
                        it2["信号"] = str(it2["信号"]) + tag
                    items_display.append(it2)
                df_show = pd.DataFrame(items_display)
                show_cols = [c for c in ["股票","代码","得分","形态","信号"] if c in df_show.columns]
                sel = st.dataframe(
                    df_show[show_cols] if show_cols else df_show,
                    width='stretch',
                    hide_index=True,
                    height=min(400, 40 + 35 * len(items)),
                    on_select="rerun",
                    selection_mode="single-row",
                    key=f"{key_prefix}_df",
                )
                try:
                    if sel and hasattr(sel, "selection") and sel.selection and sel.selection.rows:
                        idx  = sel.selection.rows[0]
                        row  = items[idx]
                        st.session_state.scan_selected_code = row["代码"]
                        st.session_state.scan_selected_name = row["股票"]
                        st.toast(f"✅ 已选中 {row['股票']}", icon="🎯")
                        st.rerun()
                except Exception:
                    pass

        def _render_dual_col(col, items, title, color, key_prefix):
            """渲染拐点/启动双通道列（含可展开理由）"""
            with col:
                st.markdown(
                    f'<p style="font-size:12px;font-weight:700;color:{color};margin-bottom:4px;">{title}</p>',
                    unsafe_allow_html=True,
                )
                if not items:
                    st.caption("暂无符合条件标的")
                    return
                for i, row in enumerate(items, 1):
                    exp_label = (f"{i}. **{row['股票']}** `{row['代码']}` · "
                                 f"{row.get('形态','')} · 得分 {row['得分']}")
                    with st.expander(exp_label, expanded=(i <= 3)):
                        st.markdown(f"**信号**：{row.get('信号','')}")
                        st.markdown(f"**理由**：{row.get('理由','')}")
                        st.caption(f"行业：{row.get('行业','')} ｜ 现价：{row.get('现价','')}")
                        if st.button(f"🔍 深度分析 {row['股票']}",
                                     key=f"{key_prefix}_{i}_{row['代码']}"):
                            st.session_state.scan_selected_code = row["代码"]
                            st.session_state.scan_selected_name = row["股票"]
                            st.rerun()

        _mkt_cfg = [
            ("US", "🇺🇸 美股"),
            ("HK", "🇭🇰 港股"),
            ("CN", "🇨🇳 A股"),
        ]

        # Tab 1：趋势强势
        with _t1:
            _c1, _c2, _c3 = st.columns(3)
            for (_mkey, _mlabel), _col in zip(_mkt_cfg, [_c1, _c2, _c3]):
                _items = _res.get(_mkey, {}).get("top", [])
                _render_market_col(_col, _items, _mlabel, f"top_{_mkey}", tab_type="top")

        # Tab 2：蓄势潜伏（筑底积累，不受市场体制限制，Risk Off也应显示）
        with _t2:
            _c1, _c2, _c3 = st.columns(3)
            for (_mkey, _mlabel), _col in zip(_mkt_cfg, [_c1, _c2, _c3]):
                _items = _res.get(_mkey, {}).get("coil", [])
                _render_market_col(_col, _items, _mlabel, f"coil_{_mkey}", tab_type="coil")

        # Tab 3：拐点（赔率）
        with _t3:
            st.caption("三关全中（底部位置 + 结构不创新低 + 止跌买量）· 赔率佳 · 低吸布局窗口")
            for _mkey, _mlabel in _mkt_cfg:
                _mkt_data = _res.get(_mkey, {})
                _bm_r     = _mkt_data.get("bm_ret5", 0)
                st.markdown(f"**{_mlabel}** · 基准5日 {_bm_r:+.1f}%")
                _dc1, _dc2 = st.columns(2)
                _render_dual_col(_dc1, _mkt_data.get("inflection", []),
                                 "🎯 拐点Top10", "#8b5cf6", f"inf_{_mkey}")
                # 占位（右列空）
                with _dc2:
                    st.empty()
                st.divider()

        # Tab 4：启动（胜率）
        with _t4:
            st.caption("突破20日高 / 放量1.3x / 相对强度领跑（满足≥1项）· 趋势启动窗口")
            for _mkey, _mlabel in _mkt_cfg:
                _mkt_data = _res.get(_mkey, {})
                _bm_r     = _mkt_data.get("bm_ret5", 0)
                st.markdown(f"**{_mlabel}** · 基准5日 {_bm_r:+.1f}%")
                _dc1, _dc2 = st.columns(2)
                with _dc1:
                    st.empty()
                _render_dual_col(_dc2, _mkt_data.get("breakout", []),
                                 "🚀 启动Top10", "#10b981", f"bo_{_mkey}")
                st.divider()

    _top30_fragment()

# 【V91.9】AI选股 Tab - Gemini 筛选短中长期好股，中美港各 Top3，15分钟缓存
with tab_ai_select:
    st.markdown("#### 🤖 三期限选股 · 短/中/长线各 Top30")
    st.markdown("<div style='font-size:13px;color:#4b5563;line-height:1.9'>"
                "中美港<b>每市场各10只</b>组成 Top30 · 五维引擎确定性打分（无AI编造）｜"
                "<span style='background:#fef3c7;padding:1px 8px;border-radius:8px'>⚡短线 = 动能45% + RS30% + 综合25%</span>　"
                "<span style='background:#dbeafe;padding:1px 8px;border-radius:8px'>🚀中线 = 综合45% + 动能30% + 趋势排列25%</span>　"
                "<span style='background:#dcfce7;padding:1px 8px;border-radius:8px'>🏛长线 = 综合40% + 年线趋势30% + 低波动30%</span>"
                "</div>", unsafe_allow_html=True)

    if 'horizon_top10' not in st.session_state:
        st.session_state.horizon_top10 = None

    ttl = get_smart_cache_ttl('daily')
    cached = False
    if st.session_state.horizon_top10:
        ts = st.session_state.horizon_top10.get('scan_timestamp', 0)
        if (time.time() - ts) < ttl:
            cached = True
            remaining = (ttl - (time.time() - ts)) / 60
            st.info(f"📦 使用缓存 | 剩余 {remaining:.1f} 分钟有效")
    if not cached:
        loaded = _load_scan_cache_from_file('horizon_top10', 'all')
        if loaded:
            st.session_state.horizon_top10 = loaded
            cached = True
            st.info("📦 使用缓存（文件持久化）")

    c_run, c_clear = st.columns([3, 1])
    with c_run:
        if st.button("🚀 生成 短/中/长线 Top30（约400只池，2-3分钟）", type="primary", width='stretch'):
            if cached:
                st.toast("📦 使用缓存，无需重新扫描", icon="📦")
            else:
                progress_bar = st.progress(0)
                status_text = st.empty()
                _hz_t0 = time.time()

                def _hz_prog(cur, total, name):
                    progress_bar.progress(min(1.0, cur / max(1, total)))
                    _el = time.time() - _hz_t0
                    _eta = (_el / cur * (total - cur)) if cur > 3 else 0
                    status_text.text(f"⏱ 已用 {_el:.0f}s · 预计剩余 {_eta:.0f}s ｜ 扫描 {cur}/{total} - {name}")

                _hz = run_horizon_top10(progress_callback=_hz_prog)
                progress_bar.empty()
                status_text.empty()
                st.session_state.horizon_top10 = {
                    'type': 'horizon_top10', 'scan_market': 'all',
                    **_hz, 'scan_timestamp': time.time(),
                }
                _save_scan_cache_to_file(st.session_state.horizon_top10)
                st.toast("✅ 三期限 Top10 已生成", icon="🎯")
                st.rerun()
    with c_clear:
        if st.button("🗑️ 清缓存", width='stretch', key="hz_clear"):
            st.session_state.horizon_top10 = None
            try:
                fp = SCAN_CACHE_DIR / f"{_scan_cache_key('horizon_top10', 'all')}.pkl"
                if fp.exists():
                    fp.unlink()
            except Exception:
                pass
            st.rerun()

    if st.session_state.horizon_top10:
        _hz = st.session_state.horizon_top10
        _t_s, _t_m, _t_l = st.tabs(["⚡ 短线 Top30（1-5日）", "🚀 中线 Top30（1-3月）", "🏛 长线 Top30（6月+）"])
        _col_order = ["排名", "市场", "代码", "名称", "期限分", "综合分", "RS强度", "20日动量", "现价", "操作指引", "止损/目标"]
        for _tab, _key, _note in (
            (_t_s, "short", "动能与相对强度主导，配合日报操作榜使用；破止损当天离场"),
            (_t_m, "mid", "综合评分+均线多头排列，适合分批建仓、周报轮动信号跟踪"),
            (_t_l, "long", "年线之上+低波动的质量型标的，适合核心仓，逢大跌加仓"),
        ):
            with _tab:
                st.caption(f"💡 {_note}")
                _arr = _hz.get(_key, [])
                if _arr:
                    _df = pd.DataFrame(_arr)
                    _df = _df[[c for c in _col_order if c in _df.columns]]
                    render_clickable_table(_df, f"horizon_{_key}")
                else:
                    st.info("暂无合格标的（宁缺毋滥）")

# 【自选股分析】按中美港划分，逐只分析：催化、技术面、风险、操作建议（与钉钉日报同源）
with tab_watchlist:
    st.markdown("#### 📋 自选股分析")
    st.caption("💡 按中美港划分，对每只自选股逐只分析：近期催化、技术面、风险点、操作建议（持有/加仓/减仓/观望）")
    
    # 【V96.1】动态自选股：搜索过的个股自动加入，上限20只，可单只移除
    _wl_total = sum(len(v) for v in WATCHLIST.values())
    st.markdown(f"**当前自选股 {_wl_total}/{_WATCHLIST_MAX}**　"
                f"<span style='font-size:12px;color:#6b7280'>🔍 搜索过的个股自动加入 · 满{_WATCHLIST_MAX}只时淘汰最早的 · 点 ✕ 移除</span>",
                unsafe_allow_html=True)
    # 【V88·自选分级】A=交易日盘中每3小时 B=每天 C=每周低频；单一权威=私仓 watch_levels.json
    try:
        import sys as _syswl
        _repo_wl = Path.home() / "Desktop" / "ai-daily-report-v2"
        if str(_repo_wl / "src") not in _syswl.path:
            _syswl.path.insert(0, str(_repo_wl / "src"))
        from watch_alerts import watch_levels as _wl_levels_load, save_watch_levels as _wl_levels_save
        _wl_lv = _wl_levels_load()
    except Exception:
        _wl_lv, _wl_levels_save = {}, None
    st.caption("级别：**A**=对应市场交易日盘中每3小时（休市不扫）｜**B**=每天1次｜**C**=每周低频。持仓仍按原风险频率检查。")
    _wl_codes_all = tuple(str(c).upper() for _lst in WATCHLIST.values() for c, _n in _lst)
    with _v88_running("计算自选股历史最高水位"):
        _wl_water = _ath_many_display(_wl_codes_all) if _wl_codes_all else {}
    col1, col2, col3 = st.columns(3)
    _wl_changed = False
    for _col, _mk, _flag in ((col1, "US", "🇺🇸 美股"), (col2, "HK", "🇭🇰 港股"), (col3, "CN", "🇨🇳 A股")):
        with _col:
            st.markdown(f"**{_flag}**（{len(WATCHLIST.get(_mk, []))}）")
            for code, name in WATCHLIST.get(_mk, []):
                _c1, _clv, _c2 = st.columns([4, 2, 1])
                _water_line = _wl_water.get(str(code).upper(), "历史水位待核")
                _c1.markdown(f"<div style='font-size:13px;padding:4px 0 0 0'>• {name} <span style='color:#9ca3af'>({code})</span>"
                             f"<div style='font-size:9px;color:#64748b;margin-left:10px'>{_water_line}</div></div>",
                             unsafe_allow_html=True)
                _cur_lv = _wl_lv.get(str(code), "B")
                _new_lv = _clv.selectbox("级别", ["A", "B", "C"], index=["A", "B", "C"].index(_cur_lv),
                                         key=f"wl_lv_{code}", label_visibility="collapsed")
                if _new_lv != _cur_lv and _wl_levels_save:
                    _wl_lv[str(code)] = _new_lv
                    _wl_levels_save(_wl_lv)
                    _wl_changed = True
                if _c2.button("✕", key=f"wl_rm_{code}", help=f"从自选股移除 {name}"):
                    _watchlist_remove(code)
                    st.toast(f"已移除 {name}", icon="🗑️")
                    st.rerun()
    if _wl_changed:
        try:
            import subprocess as _spwl
            _spwl.run(["git", "-C", str(_repo_wl), "add", "-f", "watch_levels.json"], capture_output=True)
            _spwl.run(["git", "-C", str(_repo_wl), "commit", "-m", "自选分级调整(桌面)"], capture_output=True)
            _spwl.Popen(["git", "-C", str(_repo_wl), "push", "origin", "main"],
                        stdout=_spwl.DEVNULL, stderr=_spwl.DEVNULL)
            st.toast("✅ 级别已保存并后台同步私仓", icon="🏷️")
        except Exception:
            st.toast("级别已本地保存，私仓同步失败", icon="⚠️")
    
    if 'watchlist_analysis' not in st.session_state:
        st.session_state.watchlist_analysis = None
    
    ttl = get_smart_cache_ttl('daily')
    cached = False
    if st.session_state.watchlist_analysis:
        ts = st.session_state.watchlist_analysis.get('timestamp', 0)
        if (time.time() - ts) < ttl:
            cached = True
            remaining = (ttl - (time.time() - ts)) / 60
            st.info(f"📦 使用缓存 | 剩余 {remaining:.1f} 分钟有效")
    
    if st.button("🚀 一键自选股分析（中美港逐只）", type="primary", width='stretch', key="btn_watchlist"):
        if cached:
            st.toast("📦 使用缓存，无需重新分析", icon="📦")
        else:
            progress_bar = st.progress(0)
            status_text = st.empty()
            report, err = run_watchlist_analysis(progress_callback=lambda m: status_text.text(m))
            progress_bar.progress(1.0)
            progress_bar.empty()
            status_text.empty()
            if err:
                st.error(err)
            else:
                st.session_state.watchlist_analysis = {'report': report, 'timestamp': time.time()}
                st.toast("✅ 自选股分析完成", icon="📋")
                st.rerun()
    
    if st.button("🗑️ 清除自选股分析缓存", help="清除自选股分析结果", width='stretch', key="btn_watchlist_clear"):
        st.session_state.watchlist_analysis = None
        st.toast("✅ 已清除", icon="🗑️")
        st.rerun()
    
    if st.session_state.watchlist_analysis:
        report = st.session_state.watchlist_analysis.get('report', '')
        if report:
            st.markdown("---")
            st.markdown("### 📋 自选股分析报告")
            st.markdown(report)
            st.caption(f"📌 本报告由 AI 生成 · 模型: {_ai_model_label()}")


# ═══════════════════════════════════════════════════════════════
# 【模块 ④】股票PK对决（仅在有对比股票时显示）
# ═══════════════════════════════════════════════════════════════
if st.session_state.get('pk_codes') and len(st.session_state.pk_codes) >= 2:
    _module_header("⚔️", "股票PK对决", "勾选2-4只股票对比分析", "#f093fb", "#f5576c")
    
    pk_codes = st.session_state.pk_codes
    pk_names = st.session_state.get('pk_names', pk_codes)
    
    st.markdown(f"### 📊 对比：{' vs '.join(pk_names)}")
    
    # 【V87.17】添加进度条
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    pk_results = []
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
                pk_results.append({
                    "股票": name,
                    "代码": code,
                    "当前价": f"{df_pk['Close'].iloc[-1]:.2f}",
                    "综合评分": metrics.get('score', 0),
                    "建议": metrics.get('suggestion', '观望'),
                    "RSI": f"{metrics.get('rsi', 50):.1f}",
                    "夏普比率": quant.get('sharpe', 'N/A'),
                    "最大回撤": quant.get('max_dd', 'N/A'),
                    "胜率": quant.get('win_rate', 'N/A'),
                    "盈亏比": quant.get('pl_ratio', 'N/A')
                })
    
    # 【V87.17】清除进度条
    progress_bar.empty()
    status_text.empty()
    
    if pk_results:
        # 显示对比表格
        df_pk_display = pd.DataFrame(pk_results)
        st.dataframe(df_pk_display, width='stretch', hide_index=True)
        
        # AI 综合点评
        st.markdown("---")
        st.markdown("#### 🤖 AI 综合点评")
        
        col_ai1, col_ai2 = st.columns([1, 4])
        with col_ai1:
            gen_pk_ai = st.button("⚡ 生成分析", key="btn_pk_ai_main", type="primary", width='stretch')
        with col_ai2:
            clear_pk = st.button("🔄 清除对比", key="btn_clear_pk", width='stretch')
        
        if clear_pk:
            st.session_state.pk_codes = None
            st.session_state.pk_names = None
            st.rerun()
        
        if gen_pk_ai:
            with _v88_running(f"🤖 Gemini 分析中 · 模型: {_ai_model_label()} · PK对比分析"):
                pk_summary = "\n".join([
                    f"{r['股票']}({r['代码']}): 评分{r['综合评分']}, {r['建议']}, RSI={r['RSI']}, 夏普={r['夏普比率']}"
                    for r in pk_results
                ])
                
                prompt = _load_prompt("pk_analysis.txt", pk_summary=pk_summary)
                result = st.write_stream(call_gemini_api_stream(prompt))
                st.caption(f"📌 AI生成 · 模型: {_ai_model_label()}")
                if COPY_UTILS_AVAILABLE:
                    CopyUtils.create_copy_button(result, button_text="📋 复制", key="copy_pk")



# ═══════════════════════════════════════════════════════════════
# 【V92】AI 对话区 - 与 DeepSeek V4 Flash 互动
# ═══════════════════════════════════════════════════════════════
st.markdown("---")
_chat_col1, _chat_col2 = st.columns([3, 1])
with _chat_col1:
    st.markdown("### 💬 与 AI 对话")
with _chat_col2:
    if st.button("🗑️ 清空对话", key="clear_brief_chat"):
        st.session_state.brief_chat_messages = []
        st.rerun()
st.caption("向 AI 提问市场、个股、策略等，DeepSeek V4 Flash 实时回答")

if "brief_chat_messages" not in st.session_state:
    st.session_state.brief_chat_messages = [
        {"role": "assistant", "content": (
            "你好！我是 **V88 StockAI**，底层由 **DeepSeek V4 Flash** 驱动。\n\n"
            "我可以帮你：\n"
            "- 📊 **个股深度分析**（基本面+技术面+风险）\n"
            "- 🌍 **市场解读**（美股/港股/A股走势）\n"
            "- 🎯 **策略讨论**（建仓/止损/仓位管理）\n"
            "- 📰 **今日简报解读**（基于左侧实时报告）\n\n"
            "直接输入问题即可，如：`茅台现在能买吗？` 或 `今天纳斯达克为什么跌？`"
        )}
    ]

for msg in st.session_state.brief_chat_messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

def _detect_stock_in_prompt(text: str):
    """从用户问题中识别个股（中文名/英文代码），返回 (yf_code, display_name) 或 None"""
    _name_to_code = {}
    for code, name in STOCK_NAME_INDEX.items():
        _name_to_code[name] = code
    for pool in [RAW_US, RAW_HK, RAW_CN_TOP]:
        for item in pool:
            if len(item) >= 3:
                _name_to_code[item[1]] = item[2]
            elif len(item) >= 2:
                _name_to_code[item[1]] = item[0]

    for name, code in sorted(_name_to_code.items(), key=lambda x: -len(x[0])):
        if name in text:
            return (code, name)

    _code_pat = re.findall(r'\b([A-Z]{1,5})\b', text.upper())
    for c in _code_pat:
        if c in STOCK_NAME_INDEX:
            return (c, STOCK_NAME_INDEX[c])

    _cn_pat = re.findall(r'(\d{5,6}(?:\.(?:SS|SZ|HK))?)', text)
    for c in _cn_pat:
        yf_c = to_yf_cn_code(c)
        if yf_c in STOCK_NAME_INDEX:
            return (yf_c, STOCK_NAME_INDEX[yf_c])
        return (yf_c, c)

    return None


def _build_stock_context(yf_code: str, display_name: str) -> str:
    """为检测到的个股自动获取行情 + 财报，构建 AI 上下文"""
    parts = []
    try:
        df = fetch_stock_data(yf_code)
        if df is not None and len(df) >= 5:
            last = df.iloc[-1]
            prev = df.iloc[-2]
            chg = (last["Close"] - prev["Close"]) / prev["Close"] * 100 if prev["Close"] else 0
            _bar_d = df.index[-1].strftime("%m-%d") if hasattr(df.index[-1], "strftime") else str(df.index[-1])[:10]
            parts.append(f"【{display_name} ({yf_code}) 实时行情】")
            parts.append(f"最新价: {last['Close']:.2f} | 涨跌: {chg:+.2f}% | 数据截至: {_bar_d}（A股/港股已含当日实时补条）")
            parts.append(f"今日: 开{last['Open']:.2f} 高{last['High']:.2f} 低{last['Low']:.2f} 量{last['Volume']:,.0f}")
            last5 = df.tail(5)[["Open", "High", "Low", "Close", "Volume"]]
            parts.append("最近5日行情:")
            for idx, row in last5.iterrows():
                d = idx.strftime("%m-%d") if hasattr(idx, "strftime") else str(idx)[:5]
                parts.append(f"  {d} 开{row['Open']:.2f} 高{row['High']:.2f} 低{row['Low']:.2f} 收{row['Close']:.2f} 量{row['Volume']:,.0f}")

            m = calculate_metrics_all(df, yf_code)
            if m:
                parts.append(f"RSI: {m['last'].get('RSI', 50):.1f} | 综合评分: {m.get('score', 0)}/100 | 建议: {m.get('suggestion', 'N/A')}")
                parts.append(f"MA5: {m['last'].get('MA5',0):.2f} MA20: {m['last'].get('MA20',0):.2f} MA60: {m['last'].get('MA60',0):.2f}")
            # 【V99】综合量价趋势注入：8分拆解/9段/9态/6水位/全价位——回答必须与此口径一致
            try:
                import cloud_engine as _ce2
                _F2 = _ce2.analyze_trend_full(df)
                if _F2:
                    parts.append(
                        f"\n【综合量价趋势判断(系统判定,回答须与此一致)】\n"
                        f"趋势总分: {_F2['total']}/100 | 一句话结论: {_F2['conclusion']}\n"
                        f"趋势阶段: {_F2['stage']} | 量价状态: {_F2['vp']}\n"
                        f"水位: {_F2['water']}({_F2['pos52']}%)→{_F2['water_adv']} | MACD: {_F2['macd_txt']}\n"
                        f"均线: {_F2['ma_state']}({_F2['ma_txt']})\n"
                        f"操作建议: {_F2['action']}\n"
                        f"买入区: {_F2['buy_zone']} | 回踩买点: {_F2['pullback']} | 突破加仓: {_F2['breakout']}\n"
                        f"止损: {_F2['stop']} | 减仓: {_F2['reduce']} | 失效条件: {_F2['invalid']}")
            except Exception:
                pass
    except Exception as e:
        _safe_print(f"[AI问答] 行情获取失败 {yf_code}: {e}")

    try:
        fund = fetch_stock_fundamentals(yf_code)
        if fund:
            _is = fund.get("income_stmt", {})
            _bs = fund.get("balance_sheet", {})
            _cf = fund.get("cashflow", {})
            _fin_dates = set()
            for _st in [_is, _bs, _cf]:
                for _v in _st.values():
                    if isinstance(_v, dict):
                        _fin_dates.update(_v.keys())
            _fin_dates = sorted(_fin_dates, reverse=True)[:3]
            if _fin_dates:
                parts.append(f"\n【财报数据（年报）】")
                for yr in _fin_dates:
                    rev = (_is.get("Total Revenue", {}) or {}).get(yr)
                    op = (_is.get("Operating Income", {}) or {}).get(yr)
                    ni = (_is.get("Net Income", {}) or {}).get(yr)
                    ta = (_bs.get("Total Assets", {}) or {}).get(yr)
                    eq = (_bs.get("Stockholders Equity", {}) or {}).get(yr)
                    ocf = (_cf.get("Operating Cash Flow", {}) or {}).get(yr)
                    fcf = (_cf.get("Free Cash Flow", {}) or {}).get(yr)
                    parts.append(
                        f"{yr}: 营收{_fmt_fin(rev)} 营业利润{_fmt_fin(op)} 净利润{_fmt_fin(ni)} "
                        f"总资产{_fmt_fin(ta)} 股东权益{_fmt_fin(eq)} "
                        f"经营现金流{_fmt_fin(ocf)} 自由现金流{_fmt_fin(fcf)}"
                    )
            sect = fund.get("sector", "")
            ind = fund.get("industry", "")
            if sect or ind:
                parts.append(f"行业: {sect} - {ind}")
            pe = fund.get("trailing_pe", 0)
            pb = fund.get("price_to_book", 0)
            mc = fund.get("market_cap", 0)
            if mc:
                parts.append(f"市值: {_fmt_fin(mc)} | P/E: {pe:.1f} | P/B: {pb:.2f}")
            biz = fund.get("business_summary", "")
            if biz:
                parts.append(f"公司简介: {biz[:200]}")
    except Exception as e:
        _safe_print(f"[AI问答] 财报获取失败 {yf_code}: {e}")

    return "\n".join(parts) if parts else ""


@st.cache_data(ttl=3600, show_spinner=False)
def _build_holdings_context() -> str:
    """【V95】AI问答持仓注入：云端权威持仓 + FRAMEWORK 硬规则 + 当日规则引擎结论。
    让问答框直接回答"我的持仓怎么办/英伟达要不要减"，不用去点猎手战位。"""
    import json as _json
    parts = []
    _repo = Path.home() / "Desktop" / "ai-daily-report-v2"
    # 1) 持仓明细（唯一权威版 positions.json）
    try:
        _d = _json.loads((_repo / "positions.json").read_text(encoding="utf-8"))
        lines = []
        for _acc, _info in _d.get("accounts", {}).items():
            hs = [f"{h['name']}({h['code']}) {h.get('shares')}股 成本{h.get('cost')}"
                  for h in _info.get("holdings", []) if h.get("shares")]
            if hs:
                lines.append(f"  [{_acc}] " + "；".join(hs))
        if lines:
            parts.append("【用户真实持仓（4账户）】\n" + "\n".join(lines))
    except Exception:
        pass
    # 2) 框架硬规则（云端 FRAMEWORK.md 摘要，读不到用内置兜底）
    _rules = ""
    try:
        _fw = (_repo / "FRAMEWORK.md").read_text(encoding="utf-8")
        _i = _fw.find("## 硬规则")
        if _i > 0:
            _rules = _fw[_i:_i + 500]
    except Exception:
        pass
    if not _rules:
        _rules = ("硬规则：三层一池=长线核心45-55%/中线主题20-30%(单一注≤25%)/短线≤10%(单笔风险≤总资产1%)/现金10-20%；"
                  "盈利40%仅触发多维复核，不机械减仓；基本面、技术面、新闻面共同决定动作；按'注'算集中度"
                  "(半导体7标的=一注,PM+MO=一注)；季度或偏离±5pp再平衡。")
    parts.append(f"【投资框架硬规则】\n{_rules}")
    # 3) 当日规则引擎结论（日报里的持仓段，含实时触发的减仓/卖出候选）
    try:
        _rep, _rep_meta9 = _load_report_planab()
        _rep = _rep or ""
        _j = _rep.find("## 💼 我的持仓·框架化建议")
        if _j > 0:
            _tag9 = "（⚠️Plan B当日安全版，仅观察）" if _rep_meta9.get("status") == "plan_b" else ""
            parts.append(f"【今日规则引擎结论】{_tag9}\n" + _rep[_j:_j + 1800])
    except Exception:
        pass
    return "\n\n".join(parts) if parts else ""


if prompt := st.chat_input("输入问题，如：我的持仓怎么办？英伟达要不要减？今天美股怎么看？"):
    st.session_state.brief_chat_messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    _stock_match = _detect_stock_in_prompt(prompt)
    _stock_ctx = ""
    if _stock_match:
        _yf_code, _disp_name = _stock_match
        with _v88_running(f"📊 正在获取 {_disp_name}({_yf_code}) 实时数据 + 财报..."):
            _stock_ctx = _build_stock_context(_yf_code, _disp_name)

    import datetime as _dt
    now_ts_str = _dt.datetime.now(_dt.timezone(  # type: ignore
        _dt.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M")
    with _v88_running("🤖 DeepSeek V4 分析中..." if _stock_ctx else "🤖 DeepSeek V4 思考中..."):
        _brief_ctx = ""
        if "market_brief_latest" in st.session_state and st.session_state.market_brief_latest:
            _brief_ctx = f"\n\n【参考：今日市场简报】\n{st.session_state.market_brief_latest[:800]}"

        # 【V95】持仓上下文常驻注入：任何问题都知道用户手里有什么
        _hold_ctx = _build_holdings_context()
        _hold_block = f"\n\n{_hold_ctx}" if _hold_ctx else ""
        _hold_rule_line = ("\n6. **持仓关联**：若问题涉及用户持有的标的或整体持仓，必须：①指出该持仓在框架中的层级（长线核心/中线主题/短线）；"
                          "②引用对应硬规则给出明确动作（加/减/持/清+数量比例）；③与【今日规则引擎结论】一致，不一致须说明原因。"
                          if _hold_ctx else "")

        _model_identity = f"""你是 V88 StockAI 的核心分析引擎，由 DeepSeek V4 Flash 驱动（模型：deepseek-v4-flash）。
当前日期：{now_ts_str}（Asia/Shanghai）。
你的角色：华尔街机构研究员，擅长基本面分析、技术面判断、风险管理和可执行交易建议。
如果用户询问"你是什么模型/你是谁/你用的什么AI"，直接回答：我是 V88 StockAI，底层模型为 DeepSeek V4 Flash。"""

        if _stock_ctx:
            _chat_prompt = f"""{_model_identity}

用户问题：{prompt}

【实时市场数据】
{_stock_ctx}
{_brief_ctx}{_hold_block}

请基于以上实际数据进行全面专业分析（600-1000字），结构如下：
1. **基本面**：财报趋势（营收/利润增长、利润率变化）、关键财务指标
2. **技术面**：当前价格、均线位置（20/60日）、RSI、支撑/压力位
3. **行业与竞争**：行业景气度、公司护城河、主要竞争对手
4. **风险提示**：至少3条具体风险，含触发条件
5. **综合结论**：明确操作建议（买入/持有/减仓/回避）+ 目标价 + 止损位{_hold_rule_line}

硬性要求：引用上方具体数字，禁止空泛描述；目标价必须基于估值模型或技术位给出；
每个判断和操作建议必须附基本面、技术面文字理由，每项不超过20个字，不能只给数值。"""
        else:
            _chat_prompt = f"""{_model_identity}

用户问题：{prompt}
{_brief_ctx}{_hold_block}

请简洁专业回答（300字内，涉及持仓操作可放宽到500字）。若问题涉及具体股票但无实时数据，明确说明数据局限，给出方向性判断而非具体价格。
所有判断和操作建议都要附基本面、技术面文字理由，每项不超过20个字，不能只给数值。{_hold_rule_line}"""

        try:
            reply = call_gemini_api(_chat_prompt, model_name=BRIEF_MODEL)
            reply = reply if reply and not reply.startswith("❌") else "抱歉，当前无法回答，请稍后重试。"
        except Exception as e:
            reply = f"❌ 调用失败: {str(e)[:80]}"

    st.session_state.brief_chat_messages.append({"role": "assistant", "content": reply})
    st.rerun()

# 【V90.3 清理】render_clickable_table 已移至文件顶部（含快捷入口与深度分析）

# EOF - V77 交互彻底重构版
