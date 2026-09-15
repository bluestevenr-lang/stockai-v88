"""Read the same discovery publication as Feishu; no live fetch or grading."""
from datetime import datetime,timezone
from html import escape
from pathlib import Path
from urllib.parse import urlencode
import json,re
from v88_paths import core_root
from market_badge import html as _market_badge
from display_limits import market_top
from observation_score import details_html as observation_html,watch_order

def market_badge(market):
    return _market_badge(market,image_mode=True)

from zoneinfo import ZoneInfo


def html(doc=None,*,now=None,base=None):
    now=now or datetime.now(timezone.utc)
    try:
        if doc is None:doc=json.loads((Path(base or core_root())/'data/market_watch_pub.json').read_text())
        at=datetime.fromisoformat(doc.get('checked_at') or doc['generated_at'])
        if doc.get('version')!='market-watch-v1-evidence-discovery' or not 0<=(now-at).total_seconds()<=43200:raise ValueError('old')
    except (ValueError,TypeError,KeyError,OSError):
        return "<div style='padding:12px;color:#64748b'>🔭 市场视角 · 等待最新扫描</div>"
    from market_adaptation_ui import nontechnical_html
    source_rows=doc.get('rows') or []
    rows=market_top(watch_order(source_rows,now=now),per_market=3)
    e=lambda s:escape(str(s or ''),quote=True)
    clean=lambda s:re.sub(r'\d+\.\d{4}',lambda m:f'{float(m[0]):g}',str(s))
    out=["<style>.v88-market-watch .v88-market-badge span{font-size:12px!important}.v88-market-watch td{font-size:14px!important;line-height:1.45!important}.v88-market-watch th,.v88-market-watch small,.v88-market-watch details{font-size:12px!important}.v88-market-watch details>summary{cursor:pointer;padding:4px 0}.v88-market-watch .evidence{margin-top:5px}</style>",
         "<section id='v88-market-watch' class='v88-market-watch' style='border:1px solid #bfdbfe;border-radius:12px;padding:14px;margin:12px 0;background:#f8fbff;color:#1e293b'>",
         f"<div style='font-size:21px;font-weight:800;color:#075985'>🔭 市场视角 · {len(rows)}只</div>",
         "<div style='margin:6px 0 12px'>未来1–5交易日观察 · 🔭 发现 → 🏢 核经营 → 🧪 双审 → ⭐ 中央评级<br><small>同覆盖组内按已核贡献排序；未双审不作中央审核分</small></div>",
         "<div style='overflow-x:auto'><table style='width:100%;min-width:920px;border-collapse:collapse;text-align:left'><thead><tr style='background:#e0f2fe'>",
         ''.join(f"<th style='padding:9px'>{t}</th>" for t in ('市场 / 关注顺序','个股 / 行业','技术线索','复合评分 / 经营依据','接下来观察 / 转弱条件','行情时点')),
         '</tr></thead><tbody>']
    for market,flag in (('A股','🇨🇳'),('港股','🇭🇰'),('美股','🇺🇸')):
        group=[r for r in rows if r['market']==market]
        if not group:out.append(f"<tr><td colspan='6' style='padding:10px'>{market_badge(market)} · 0只，报价或线索待补</td></tr>")
        for display_rank,r in enumerate(group,1):
            p=r.get('profile') or {};code=r['code'];shown=code[:-3].zfill(4)+'.HK' if code.endswith('.HK') else code
            name=(f"{p.get('name_en') or code} · {p.get('name_zh') or '中文名待核'}" if market=='美股' else p.get('name_zh') or r['name'])
            from stock_profile_view import industry_rank_html
            rank=industry_rank_html(code,compact=True)
            if '英文原名＋中文行业说明' in (p.get('name_kind') or '') and market=='美股':name=(p.get('name_en') or code)+' · 中文名待核'
            href='?'+urlencode({'q':code,'focus':'deep'})+'#v88-deep-analysis'
            try:since=datetime.fromisoformat(r['first_seen_at']).astimezone(ZoneInfo('Asia/Shanghai')).strftime('%m-%d %H:%M')
            except (ValueError,KeyError,TypeError):since='待核'
            quote=r['quote_asof'];clock='纽约' if market=='美股' else '北京'
            cells=[f"{market_badge(market)} #{display_rank}<br><small>{e(r.get('evolution'))}</small>",
                   f"<a href='{e(href)}' style='color:#075985;font-weight:700'>{e(name)}</a><br>{e(shown)}<br><small>{e(p.get('industry') or '行业待核')}</small><br>{rank}",
                   f"<b>◉ {e(r['kind'])}</b><br>{e(r['why'])}<br><small>{'已核日线结构' if r['structural_verified'] else '报价线索 · 日线待补'}</small>",
                   observation_html(r,now=now)+nontechnical_html(r),
                   f"👁 {e(clean(r['next_check']))}<br><small>↘ {e(clean(r['invalidation']))}</small>",
                   f"<b>{r['last']:g} · {r['change_pct']:+.2f}%</b><br><small>{clock} {e(quote[5:10])} {e(quote[11:16])}<br>{e(r['quote_kind'])}<br>首次进入 北京{e(since)}</small>"]
            out.append('<tr>'+''.join(f"<td style='padding:10px;border-bottom:1px solid #dbeafe;vertical-align:top'>{c}</td>" for c in cells)+'</tr>')
    out+=['</tbody></table></div>']
    if len(source_rows)>len(rows):out.append("<div style='font-size:12px;color:#64748b;margin-top:8px'>其余线索继续后台跟踪。</div>")
    out+=[
          "<details style='font-size:12px;color:#64748b;margin-top:8px'><summary>筛选与跟进规则 · 未授级</summary>",
          e(doc.get('rules'))+'<br>非技术依据缺失时仅保留线索，不作为推荐理由。视角名单不产生审核分、买点或收益承诺。观察位不替代原交易合同；移出Top3仍保留历次档案。',
          '</details></section>']
    return ''.join(out)
