"""Source clocks and local cache clocks are distinct, never manufactured on read."""
import json
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from zoneinfo import ZoneInfo
from v88_paths import core_root

BJT = ZoneInfo('Asia/Shanghai')
MODULES = {
    'strategy_board.json': '🎯 3A / 2A / 1A',
    'market_snapshot.json': '🌍 市场概览 / 板块',
    'cycle_scan.json': '◉ 持仓 / 自选周期',
    'astra_cycle.json': '⭐ Astra 计划',
    'recommendation_journal_pub.json': '📅 推荐日历',
    'market_watch_pub.json': '🔭 市场视角',
    'autonomous_report.json': '📊 日报 / 周报',
    'market_adaptation_pub.json': '🧭 市场环境',
    'rotation_forecast.json': '🔄 板块轮转',
    'weekly_report_facts.json': '📆 周报依据',
    'autonomous_status.json': '⚙️ 云端运行',
    'feishu_cloud_refresh_status.json': '✉️ 飞书行情刷新',
    'discovery_refresh_status.json': '📡 市场扫描',
}

def read(path):
    try:
        doc=json.loads(Path(path).read_text())
        return doc if isinstance(doc,dict) else {}
    except (OSError,ValueError): return {}

# Status fragments poll every few seconds while *_pub.json files reach hundreds
# of MB; only reparse when a file's (mtime, size) actually changes.
_SNAPSHOTS={}
_STAMPS={}
STAMP_KEYS=('screening_finished_at','generated_at','analysis_time','updated_at','checked_at')

def _signature(path):
    try:
        st=Path(path).stat();return (st.st_mtime_ns,st.st_size)
    except OSError:return None

def read_snapshot(path):
    """Shared parsed document for read-only status views; callers must not mutate it."""
    key=str(path);sig=_signature(path)
    if sig is None:_SNAPSHOTS.pop(key,None);return {}
    hit=_SNAPSHOTS.get(key)
    if hit and hit[0]==sig:return hit[1]
    doc=read(path);_SNAPSHOTS[key]=(sig,doc)
    return doc

def generated_at(path):
    key=str(path);sig=_signature(path)
    if sig is None:_STAMPS.pop(key,None);return None
    hit=_STAMPS.get(key)
    if hit and hit[0]==sig:return hit[1]
    doc=read(path)
    value=next((doc.get(k) for k in STAMP_KEYS if doc.get(k)),None)
    _STAMPS[key]=(sig,value)
    return value

def parse(value):
    try:
        dt=datetime.fromisoformat(str(value).replace('Z','+00:00'))
        # Existing V88 legacy publications explicitly use Beijing local time.
        return dt.replace(tzinfo=BJT) if dt.tzinfo is None else dt
    except (ValueError,TypeError): return None

def display(value):
    dt=parse(value)
    return dt.astimezone(BJT).strftime('%m-%d %H:%M:%S') if dt else '未记录'

def age(value,now=None):
    dt=parse(value);now=now or datetime.now(timezone.utc)
    if not dt:return '未知'
    seconds=max(0,int((now-dt).total_seconds()))
    if seconds<60:return f'{seconds}秒'
    if seconds<3600:return f'{seconds//60}分钟'
    return f'{seconds//3600}小时{seconds%3600//60}分钟'

def record(filename,doc=None,base=None,now=None):
    path=(Path(base) if base else core_root()/'data')/filename
    generated=generated_at(path) if doc is None else next((doc.get(k) for k in STAMP_KEYS if doc.get(k)),None)
    try:cached=datetime.fromtimestamp(path.stat().st_mtime,timezone.utc).isoformat()
    except OSError:cached=None
    return {'module':MODULES.get(filename,filename.removesuffix('.json')), 'generated':display(generated),
        'age':age(generated,now),'cached':display(cached),'cache_age':age(cached,now)}

def html(filename,doc=None,base=None,now=None):
    r=record(filename,doc,base,now)
    e=lambda value:escape(str(value))
    return ('<div class="module-freshness" style="display:flex;align-items:center;gap:10px 24px;flex-wrap:wrap;padding:12px 16px;margin:10px 0;background:#f5f5f7;border-radius:14px;color:#86868b;font-family:-apple-system,BlinkMacSystemFont,sans-serif;font-size:13px!important;line-height:1.7">'
        f'<span>筛选完成 <b style="color:#424245;font-weight:500">{e(r["generated"])}</b></span>'
        f'<span>结果距今 <b style="color:#424245;font-weight:500">{e(r["age"])}</b></span>'
        f'<span>缓存年龄 <b style="color:#424245;font-weight:500">{e(r["cache_age"])}</b></span>'
        f'<details style="margin-left:auto"><summary style="cursor:pointer;color:#007aff">时间详情</summary><span>缓存写入 {e(r["cached"])} · 北京时间</span></details></div>')


def inventory(base=None,now=None):
    base=Path(base) if base else core_root()/'data'
    # All derived view modules published by the cloud are listed, not raw inputs.
    names=dict(MODULES)
    names.update({p.name:p.stem for p in base.glob('*_pub.json') if p.name not in names})
    return [record(n,base=base,now=now) for n in names if (base/n).exists()]
