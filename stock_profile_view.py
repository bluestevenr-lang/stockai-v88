"""Shared, read-only company identity overlay. Keep core and desktop identical."""
from v88_paths import core_root
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from html import escape
import json
import math
import re
from pathlib import Path

DEFAULT = core_root()/'data/stock_profiles_pub.json'


@lru_cache(maxsize=4)
def _read(path, mtime, size):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def load(path=None):
    path = Path(path or DEFAULT)
    try:
        stat = path.stat()
        return _read(str(path), stat.st_mtime_ns, stat.st_size)
    except (OSError, ValueError):
        return {}


def profile(code, doc=None, now=None):
    from grade_focus import canonical
    from exchange_sessions import latest_completed
    doc = load() if doc is None else doc
    if doc.get('version') not in ('stock-profiles-v1','stock-profiles-v2-industry-peers'): return {}
    key = canonical(code); row = dict((doc.get('records') or {}).get(key) or {})
    if row.get('code') != key: return {}
    now = now or datetime.now(timezone.utc)
    try:
        at = datetime.fromisoformat(row['profile_captured_at'])
        current = (at.tzinfo is not None and timedelta(0) <= now-at <= timedelta(days=7))
    except (KeyError, ValueError, TypeError): current = False
    rank = row.get('industry_rank') or {}
    try:
        rank_current = current and rank.get('session') == latest_completed(row.get('market') or '美股', now).isoformat()
    except (ValueError, KeyError): rank_current = False
    cap = row.get('market_cap')
    if (not rank_current or type(cap) not in (int, float) or not math.isfinite(cap) or cap <= 0
            or type(rank.get('rank')) is not int or type(rank.get('comparable')) is not int
            or type(rank.get('classified')) is not int or not 1 <= rank['rank'] <= rank['comparable'] <= rank['classified']):
        row['industry_rank'] = None
    row['profile_fresh'] = current
    coverage = doc.get('markets',{}).get(row.get('market')) or doc
    row['classified_universe'] = coverage.get('classified')
    row['market_universe'] = coverage.get('universe') or coverage.get('us_universe')
    return row


def display_name(name, code, doc=None):
    """Display-only identity: US English / Chinese; CN/HK Chinese.

    Resolve by the exact canonical security, never by a similar company name.
    This overlay does not change the source row, its code, or review signature.
    Stale profiles can still name the same security; ``profile`` independently
    removes expired industry ranks so a name does not renew market evidence.
    """
    from grade_focus import canonical
    supplied = str(name or '').strip()
    key = canonical(code)
    if not key:
        return supplied
    market = '港股' if key.endswith('.HK') else 'A股' if key.endswith(('.SS', '.SZ', '.BJ')) else '美股'
    p = profile(code, doc)
    if p.get('market') != market:
        p = {}
    has_zh = lambda value: bool(re.search(r'[\u4e00-\u9fff]', str(value or '')))
    zh = str(p.get('name_zh') or '').strip()
    if not has_zh(zh):
        zh = supplied if has_zh(supplied) else ''
    en = str(p.get('name_en') or '').strip()
    if not en or has_zh(en):
        en = supplied if supplied and not has_zh(supplied) and canonical(supplied) != key else ''
    if market == '美股':
        return f"{en or key} · {zh or '中文名待核'}"
    return zh or (f"{en} · 中文名待核" if en else '中文名待核')


def display_label(name, code, doc=None):
    """Standalone label always includes the original security code exactly once."""
    label = display_name(name, code, doc)
    code = str(code or '').strip()
    if not code:
        return label
    if label == code:
        return code
    # The fallback US label already starts with its only known identifier.
    if label == f'{code} · 中文名待核':
        return label
    return f'{label}（{code}）'


def link_html(name, code, doc=None, *, style='color:inherit;text-decoration:underline;text-underline-offset:2px'):
    """One safe deep link for cycle/trend lists; no symbol remapping or HTTP calls."""
    from urllib.parse import quote
    code = str(code or '').strip()
    label = escape(display_label(name, code, doc))
    if not code:
        return label
    return (f'<a href="?q={quote(code, safe=".-")}&amp;focus=deep#v88-deep-analysis" '
            f'target="_blank" rel="noopener" style="{escape(style, quote=True)}">{label}</a>')


def text(code, doc=None, now=None):
    doc=load() if doc is None else doc
    p = profile(code, doc, now)
    if not p: return ''
    r = p.get('industry_rank')
    if p.get('industry_group_id') and not industry_peers(code,doc,now)['ok']:r=None
    rank = f"行业排名（按总市值）第{r['rank']}/{r['comparable']}名（已分类同业{r['classified']}只；{r['session']}）" if r else '行业市值排名暂不可用'
    return f"{p.get('name_zh') or p.get('name_en')}｜{p.get('industry') or '行业尚未核实'}｜{rank}"


def industry_peers(code, doc=None, now=None):
    """Verify a single published industry list against its underlying profiles.

    No network, model, currency conversion or new investment score. Ranking and
    the ten names must describe the same group/session and original cap values.
    """
    from grade_focus import canonical
    from exchange_sessions import latest_completed
    doc=load() if doc is None else doc
    now=now or datetime.now(timezone.utc)
    p=profile(code,doc,now);key=canonical(code)
    failure=lambda message:{'ok':False,'reason':message,'rows':[]}
    if not p or not p.get('industry'):return failure('行业尚未核实，暂无法确定同业名单')
    if not p.get('profile_fresh'):return failure('公司行业资料待更新，暂不展示旧前十')
    group=(doc.get('industry_groups') or {}).get(p.get('industry_group_id'))
    if not group:return failure('同业前十数据待更新')
    try:
        fields=('market','industry','industry_taxonomy','currency')
        if any(group.get(k)!=p.get(k) for k in fields):raise ValueError('行业分组口径不一致')
        if group['session']!=latest_completed(p['market'],now).isoformat():raise ValueError('同业市值日期待更新')
        classified=group['classified_codes'];ranked=group['ranked_codes'];records=doc['records']
        if (len(set(classified))!=len(classified) or len(set(ranked))!=len(ranked) or
            key not in classified or not set(ranked)<=set(classified) or
            group['classified']!=len(classified) or group['comparable']!=len(ranked)):
            raise ValueError('行业可比数量或身份不一致')
        for c in classified:
            q=records[c]
            if q.get('code')!=c or any(q.get(k)!=group.get(k) for k in fields):
                raise ValueError('同业成员或分类口径不一致')
        expected=[];last=None;rank=0;values=[]
        for pos,c in enumerate(ranked,1):
            q=profile(c,doc,now);r=q.get('industry_rank') or {};cap=q.get('market_cap')
            if (not r or r.get('group_id')!=p['industry_group_id'] or
                r.get('session')!=group['session'] or r.get('currency')!=group['currency'] or
                r.get('comparable')!=len(ranked) or r.get('classified')!=len(classified)):
                raise ValueError('同业排名已过期或口径不一致')
            if cap!=last:rank=pos
            if r.get('rank')!=rank:raise ValueError('同业名次与市值顺序不一致')
            last=cap;values.append((c,cap))
            if pos<=10:expected.append({'code':c,'rank':rank,'market_cap':cap})
        if values!=sorted(values,key=lambda x:(-x[1],x[0])):raise ValueError('同业市值顺序不一致')
        if group.get('top10')!=expected:raise ValueError('行业前十与排名原表不一致')
        return {'ok':True,'group':group,'rows':[{**records[r['code']],'rank':r['rank']} for r in expected],
                'target_rank':(p.get('industry_rank') or {}).get('rank')}
    except (KeyError,TypeError,ValueError,AttributeError):
        return failure('行业排名与同业原表未通过一致性核验，等待数据更新')


def _cap_text(cap,currency):
    unit={'CNY':'人民币','USD':'美元','HKD':'港元'}.get(currency,currency)
    value=f'{cap/100_000_000:,.2f}亿' if cap>=100_000_000 else f'{cap/10_000:,.2f}万'
    return value+unit


def industry_rank_html(code, doc=None, now=None, *, compact=False):
    from grade_focus import canonical
    code=canonical(code)
    doc=load() if doc is None else doc
    p=profile(code,doc,now)
    if not p:return ''
    peer=industry_peers(code,doc,now);r=p.get('industry_rank')
    # Once the new published group exists, a contradictory group invalidates
    # its visible rank too; a stale Top10 must not accompany a current headline.
    if p.get('industry_group_id') and not peer['ok']:r=None
    label=(f"行业排名（按市值）<b>{r['rank']}/{r['comparable']}</b>" if r else '行业排名：缺当期可比数据')
    if peer['ok']:
        g=peer['group'];peers=peer['rows'][:5];n=len(peers)
        heading=f"{escape(g['market'])} · {escape(g['industry'])} · 前{n}家公司"
        rows=''.join(f"<div class='v88-industry-peer-row' data-peer-code='{escape(q['code'],quote=True)}' "
              "style='display:grid;grid-template-columns:26px minmax(0,1fr);gap:5px;padding:5px 0;border-bottom:1px solid #dbe4ef'>"
              f"<b>{q['rank']}</b><div>{link_html(q.get('name_zh') or q.get('name_en'),q['code'],doc)}"
              f"<br><span style='font-size:10px;color:#64748b'>总市值 {escape(_cap_text(q['market_cap'],g['currency']))}</span></div></div>"
              for q in peers)
        body=(f"<div style='font-weight:600'>{heading}</div>"
              f"<div style='font-size:10px;color:#64748b'>{escape(g['session'])} · 可比{g['comparable']}/{g['classified']}只已分类同业</div>"
              +rows+f"<div style='font-size:10px;color:#64748b;margin-top:5px'>{'同业不足5家，按实际数量显示。' if n<5 else ''}"
              '按股票总市值排序，仅表示规模；不代表竞争力、GPT审核分或推荐顺序。相同市值并列；不同股类分别计数，边界同值按代码顺序取5只。</div>')
    else:body=f"<div>{escape(peer['reason'].replace('前十','名单'))}</div>"
    style='display:inline-block;vertical-align:top;' if compact else ''
    return (f"<details class='v88-industry-ranking' data-industry-code='{escape(str(code),quote=True)}' style='{style}font-size:11px'>"
            f"<summary style='cursor:pointer;color:#0369a1'>{label} · 查看同业Top5</summary>"
            f"<div class='v88-industry-peer-list' style='min-width:210px;max-width:420px;white-space:normal;padding:7px;background:#f0f7ff;border:1px solid #dbeafe;border-radius:5px'>{body}</div></details>")


def compact_html(code, doc=None):
    doc=load() if doc is None else doc
    p = profile(code, doc)
    if not p: return ''
    return ("<span class='v88-profile-inline' style='font-size:11px;color:#64748b;font-weight:400' "
            f"title='{escape(text(code, doc) + '；仅本市场已覆盖同业，非审核分/胜率排名', quote=True)}'>"
            + ' · ' + escape(p.get('industry') or '行业待核') + '</span> · '
            + industry_rank_html(code,doc,compact=True))


def html(code, doc=None, now=None):
    doc=load() if doc is None else doc
    p = profile(code, doc, now)
    if not p:
        return ("<div class='v88-stock-profile' style='font-size:11px;color:#64748b'>"
                "行业待核 · 排名暂不可用</div>") if code else ''
    e = lambda v: escape(str(v or '未提供'), quote=True)
    r = p.get('industry_rank')
    if p.get('industry_group_id') and not industry_peers(code,doc,now)['ok']:
        r = None
    rank = industry_rank_html(code,doc,now)
    detail = (f"{r['scope']}；已分类同业 {r['classified']}只，市值可比 {r['comparable']}只。"
              f"{r['session']}收盘行情；{r['ties']}。原始总市值：{p['market_cap']:,.0f} {r.get('currency')}。" if r else
              '无有效同日市值或行业资料已过期，不用旧排名冒充当前排名。')
    return (f"<div class='v88-stock-profile' data-profile-code='{e(code)}' style='font-size:11px;color:#475569;line-height:1.45;margin-top:3px'>"
            f"<span>{e(p.get('industry') or '行业尚未核实')}{' · 资料待更新' if not p['profile_fresh'] else ''}</span><br>{rank}"
            f"<details><summary>公司与行业口径</summary>{e(p.get('name_en'))}<br>"
            f"名称：{e(p.get('name_kind'))}；行业：{e(p.get('industry_taxonomy'))}。<br>{e(detail)}"
            '<br>市值排名表示规模；不改变V88审核分、Top顺位或评级。未分类证券不参与该排名。'
            f"<br>{e(p.get('market') or '美股')}池行业已归类 {e(p.get('classified_universe'))}/{e(p.get('market_universe'))}只；不声称已覆盖全行业。"
            f"<br>资料采集 {e(p.get('profile_captured_at'))}；公司资料按周更新，市值按交易日更新。"
            f"<br><a href='https://data.eastmoney.com/' target='_blank' rel='noopener'>东方财富资料来源</a></details></div>")
