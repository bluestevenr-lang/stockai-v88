"""Project scanner rows onto current central authority without changing caches.

Raw technical rankings are research observations. They cannot create grades,
entry permission, or replace a signed research contract. Keep desktop identical.
"""
from copy import deepcopy
from datetime import datetime, timezone, timedelta
import math
from pathlib import Path
import re
import sys

VERSION='scanner-central-v1'
BJT=timezone(timedelta(hours=8))
HORIZONS={'short':'短期（1–30天）','medium':'中期（31–90天）','long':'长期（91–365天）'}
SCORES={'得分':'量化辅助分','统一分':'量化辅助分','综合分':'量化辅助分','评分':'量化辅助分',
        'score':'量化辅助分','unified_score':'量化辅助分','短线分':'短线辅助分',
        '中线分':'中线辅助分','长线分':'长线辅助分','短/中/长':'短/中/长辅助分',
        '市场排名':'量价辅助排名','排名':'量价辅助排名','期望值':'规则情景值（非收益预测）',
        '上/下估计':'规则方向估计（非胜率）','2周上/下估计':'两周方向情景（未标定）',
        '机会概率':'上涨方向情景（未标定）','风险概率':'下跌方向情景（未标定）',
        '长期':'量价长期状态','短期':'量价短期状态','盈亏比':'技术价位RR（非合同）'}
ACTION_KEY=re.compile(r'买入|卖出|买点|卖点|建仓|开仓|仓位|推荐|建议|策略|计划|操作|动作|触发|入场|进场|止盈|止损|失效|分批|节奏|持有期|目标|合同|中央|评级|审核|胜率|(?i:action|recommend|signal|strategy|trade_plan|entry|stop|target|tier|audit|formal)')
ACTION_TEXT=re.compile(r'买入|卖出|买点|卖点|进场|入场|出场|止盈|止损|开仓|建仓|加仓|减仓|清仓|持有|可买|可卖|逢低|低吸|获利了结|分批布局|必涨|稳赚|保证收益|值得买|强烈推荐|(?i:strong buy|buy now|sell now)')
NUMERIC_PRICE=re.compile(r'^[\d.\s,，~～→\-\[\]()]+$')


def canonical(value):
    value=str(value or '').strip().upper().replace('.SH','.SS')
    if re.fullmatch(r'\d{1,5}\.HK',value):return str(int(value[:-3]))+'.HK'
    if re.fullmatch(r'\d{6}\.(?:SS|SZ|BJ)|[A-Z][A-Z0-9.$-]{0,14}',value):return value
    return ''


def row_code(row):
    for key in ('代码','code','ticker','symbol','股票'):
        raw=str(row.get(key) or '').strip()
        code=canonical(raw)
        if code:return code
        match=re.search(r'[（(]([A-Za-z0-9.$-]+)[）)]',raw)
        if match and canonical(match[1]):return canonical(match[1])
    return ''


def _load_current_snapshot(now):
    # Desktop uses the same installed central verifier as every public outlet.
    core=Path(__file__).resolve().parents[1]
    if not (core/'src/feishu_snapshot.py').exists():core=Path.home()/'Desktop/ai-daily-report-v2'
    src=str(core/'src')
    if src not in sys.path:sys.path.insert(0,src)
    from feishu_snapshot import build
    return build(now=now)


def _fresh(value,now):
    try:
        at=datetime.fromisoformat(str(value).replace('（北京时间）','').strip())
        if at.tzinfo is None:at=at.replace(tzinfo=BJT)
        elapsed=(now-at).total_seconds()
        return 0<=elapsed<=86400
    except (ValueError,TypeError):return False


def _band(value):
    if not isinstance(value,(list,tuple)) or len(value)!=2:return '未提供'
    if any(type(v) not in (float,int) or not math.isfinite(v) or v<=0 for v in value):return '未提供'
    return '～'.join(f'{v:.4f}'.rstrip('0').rstrip('.') for v in value)


def _safe_observations(row):
    out={}
    for key,value in row.items():
        key=str(key)
        if key.startswith('_'):continue  # Old cached AI plans are never projected.
        if isinstance(value,str) and ACTION_TEXT.search(value):continue
        if key in SCORES:out.setdefault(SCORES[key],deepcopy(value));continue
        if ACTION_KEY.search(key):
            # Preserve only numeric technical price observations under an
            # explicit historical label; never retain legacy trade prose.
            if any(word in key for word in ('买入区间','止损','目标')) and isinstance(value,(str,int,float)) and NUMERIC_PRICE.fullmatch(str(value)):
                out['历史技术参考·'+key+'（无执行权）']=deepcopy(value)
            continue
        if isinstance(value,(dict,list,tuple)):continue  # Nested cached actions stay in the original cache only.
        out[key]=deepcopy(value)
    return out


def project_rows(rows, *, snapshot=None, selection=None, now=None):
    """Return display copies, revalidating one central snapshot for this batch.

    Supplied snapshot/selection are version expectations, never alternative
    authority: a stale cached copy cannot resurrect a grade or buy permission.
    `_central` holds exact machine-readable references; hide it from CSV/UI.
    """
    now=now or datetime.now(BJT)
    if now.tzinfo is None:now=now.replace(tzinfo=BJT)
    error=None
    try:
        current=_load_current_snapshot(now)
        if not current.get('factpack_id') or not _fresh(current.get('source_generated_at'),now):
            raise ValueError('中央发布已过期或缺失')
        for expected in (snapshot,selection):
            if expected is None:continue
            if (expected.get('factpack_id')!=current['factpack_id'] or
                (expected.get('source_generated_at') or expected.get('generated_at'))!=current['source_generated_at']):
                raise ValueError('提供的中央版本与当前发布不一致')
        indexed={canonical(row.get('code')):row for row in current.get('rows',[]) if row.get('tier') in {'1A','2A','3A'}}
    except Exception as exc:
        current={};indexed={};error='中央当前审核不可核实（'+type(exc).__name__+'）'
    output=[]
    for raw in rows or []:
        if not isinstance(raw,dict):continue
        row=_safe_observations(raw);code=row_code(raw);central=indexed.get(code)
        if code:row['代码']=code
        valid=bool(central)
        if valid and central.get('name'):
            row['名称']=(central.get('company_profile') or {}).get('name_zh') or central['name']
        plan=deepcopy((central or {}).get('trade_plan') or {})
        formal=bool(valid and central.get('tier')=='3A' and central.get('formal_recommendation') is True)
        opportunity=(central or {}).get('entry_opportunity') or {}
        # A current 3A may be preparation/frozen; only the central executable
        # subset plus its current entry check can grant a scanner buy action.
        formal=bool(formal and opportunity.get('executable') is True)
        label='🟢 中央3A·入场许可有效' if formal else ('🔎 '+central['tier']+'·研究跟踪，不可执行' if valid else '⚪ 未获当前中央许可·仅线索')
        reasons=(central or {}).get('action_blocks') or opportunity.get('reasons') or (central or {}).get('reason_codes') or []
        reason='；'.join(str(v) for v in reasons[:3]) if isinstance(reasons,list) else str(reasons)
        explanation=error or (reason if valid and reason else '量价辅助分不等于中央审核分；仅中央有效3A现买可形成入场许可。')
        if not valid and not error:explanation='当前中央无有效1A/2A/3A评级；可能尚未完成、未达标或已过期。旧技术分和价位不能形成推荐。'
        horizon=plan.get('horizon') or (central or {}).get('horizon')
        contract=plan.get('profit_contract') or {}
        row.update({'中央评级':central['tier'] if valid else '未获当前评级',
                    '中央审核分':central.get('audit_score') if valid else None,
                    '原周期':HORIZONS.get(horizon,'未核实'), '中央动作':label,
                    '原合同进场':_band(plan.get('entry_range')),
                    '原合同止盈':_band(plan.get('take_profit_range')),
                    '原合同失效':plan.get('stop') if plan.get('stop') is not None else '未提供',
                    '原合同截止':contract.get('thesis_deadline') or '未提供',
                    '中央说明':explanation,
                    '_central':{'version':VERSION,'code':code,'current':valid,'formal':formal,
                                'tier':central['tier'] if valid else None,
                                'audit_score':central.get('audit_score') if valid else None,
                                'horizon':horizon,'factpack_id':current.get('factpack_id'),
                                'source_generated_at':current.get('source_generated_at'),
                                'master_ref':deepcopy((central or {}).get('master_ref')),
                                'trade_plan':plan,'model_calls':0}})
        output.append(row)
    return output
