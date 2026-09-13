"""Industry reference samples with current central authority only."""
from datetime import datetime,timezone,timedelta
from html import escape


def html(doc,*,market='全部',factpack_id=None,stock_link=None,now=None):
    now=now or datetime.now(timezone.utc)
    esc=lambda x:escape(str(x if x is not None else '—'))
    try:
        at=datetime.fromisoformat(doc['generated_at'])
        usable=(doc.get('version')=='sector-reference-v2-central-only' and at.tzinfo is not None
                and 0<=(now-at).total_seconds()<=21600 and doc.get('factpack_id')==factpack_id)
    except (KeyError,ValueError,TypeError):usable=False
    if not usable:return '<small>行业样本版本待刷新；当前评级见3A中央列表。</small>'
    selected=('A股','港股','美股') if market=='全部' else (market.lstrip('🇨🇳🇭🇰🇺🇸'),)
    lines=['<style>@media(max-width:600px){[data-testid="stHorizontalBlock"]:has(.v88-sector-reference){flex-wrap:wrap!important}[data-testid="stHorizontalBlock"]:has(.v88-sector-reference)>[data-testid="stColumn"]{width:100%!important;flex:1 1 100%!important;min-width:0!important}}</style>',
           '<div class="v88-sector-reference" style="font-size:12px"><b>🎖️ 五行业参考样本</b><small> · '+esc(doc['generated_at'])+'</small>',
           '<details style="font-size:11px;color:#64748b"><summary>样本与中央评级的关系</summary>行业分类仅用于研究分组，样本不是全行业排名或独立推荐；评分、原合同和执行权限由3A中央审核决定。</details>']
    from history_calendar import latest_completed
    for m in selected:
        lines.append('<div style="margin-top:5px"><b>'+esc(m)+'</b></div>')
        for group in (doc.get('markets') or {}).get(m,[]):
            r=group.get('watch') or {};c=r.get('central') or {};code=r.get('sym') or r.get('code')
            if not code:continue
            try:
                source=datetime.fromisoformat(r['source_asof'])
                fresh=r.get('source_current') is True and source.tzinfo is not None and source<=now and source.date()==latest_completed(m,now)
            except (KeyError,ValueError,TypeError):fresh=False
            if not fresh:
                lines.append('<div>'+esc(group.get('sector'))+' · 原件日期待更新，暂停展示样本价格。</div>');continue
            try:
                reviewed=datetime.fromisoformat(str(c.get('reviewed_at','')).replace('（北京时间）','').strip())
                if reviewed.tzinfo is None:reviewed=reviewed.replace(tzinfo=timezone(timedelta(hours=8)))
                review_fresh=0<=(now-reviewed).total_seconds()<86400
            except (ValueError,TypeError):review_fresh=False
            linked=(c.get('master_ref') or {}).get('factpack_id')==factpack_id
            current=linked and review_fresh and c.get('current') is True and c.get('tier') in ('1A','2A','3A')
            label=esc(c.get('tier'))+' / '+esc(c.get('audit_score'))+'分' if current else '研究线索 · 未获当前评级'
            if not current and linked and review_fresh and c.get('review_complete') is True:
                label='已复审未授级 / '+esc(c.get('audit_score'))+'分'
            name=stock_link(r.get('name'),code) if stock_link else esc(r.get('name'))+' '+esc(code)
            color='#0369a1' if current else '#64748b'
            t=r.get('technical') or {}
            lines.append('<div style="margin:4px 0"><b>'+esc(group.get('sector'))+'</b> · '+name
                +' <span style="color:'+color+'">'+label+'</span> · 源价 '+esc(r.get('last'))
                +' <small>'+esc(r.get('source_asof'))+'</small>'
                +'<details style="font-size:11px;color:#64748b"><summary>趋势与原审核依据</summary>'
                +'20日涨跌 '+esc(t.get('chg20d_pct'))+'%；MA20 '+esc(t.get('ma20'))+' / MA60 '+esc(t.get('ma60'))
                +'<br>'+esc(c.get('reason')))
            if current:
                p=c.get('trade_plan') or {}
                lines.append('<br>中央原进场 '+esc(p.get('entry_range'))+'；原止盈 '+esc(p.get('take_profit_range'))+'；原失效 '+esc(p.get('stop'))+'。这里只引用原研究合同，不增加开仓许可。')
            lines.append('</details></div>')
    return ''.join(lines)+'</div>'
