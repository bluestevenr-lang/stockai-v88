"""Readable weekly publication history; rendering never calls models or regrades."""
from datetime import date,datetime,timedelta,timezone
from html import escape
from urllib.parse import urlencode
from pathlib import Path
import json, math
from v88_paths import core_root

BJT=timezone(timedelta(hours=8))
MARKETS=('A股','港股','美股')
HORIZONS={'short':'短期','medium':'中期','long':'长期','':'—'}


def week_rows(doc,week,query=''):
    start=date.fromisoformat(week);end=start+timedelta(days=7)
    rows=[r for r in doc.get('rows',[]) if any(start<=date.fromisoformat(day)<end for day in r['days'])]
    q=query.strip().casefold()
    if q:rows=[r for r in rows if q in (r['code']+' '+r['name']).casefold()]
    return sorted(rows,key=lambda r:(MARKETS.index(r['market']),r['kind']!='formal',r['first_at'],r['code'],r['horizon']))


def pages(rows):
    """At most five rows per market on each page; no weekly record is dropped."""
    groups={m:[r for r in rows if r['market']==m] for m in MARKETS}
    count=max([math.ceil(len(g)/5) for g in groups.values()]+[1])
    return [sum((g[i*5:i*5+5] for g in groups.values()),[]) for i in range(count)]


def html(rows,week,now=None):
    from market_badge import html as badge
    from stock_profile_view import display_name,load
    profiles=load(core_root()/'data/stock_profiles_pub.json')
    now=now or datetime.now(BJT);today=now.astimezone(BJT).date();start=date.fromisoformat(week)
    esc=lambda v:escape(str(v if v is not None else '—'),quote=True)
    def label(event):
        if event['state']=='check':return event['label']
        if event['state']=='left':return '↘ 移出当次榜'
        if event.get('tier'):return HORIZONS.get(event.get('horizon'),'旧制')+f" {event['tier']} · {event['score']:g}分 · #{event['rank']}"
        return '👁 观察 #'+str(event.get('rank') or '—')
    out=["<style>.v88-week-journal{overflow-x:auto;border:1px solid #cbd5e1;border-radius:10px}.v88-week-journal table{width:100%;min-width:1300px;border-collapse:collapse}.v88-week-journal td,.v88-week-journal th{padding:10px;border:1px solid #e2e8f0;vertical-align:top;font-size:13px;line-height:1.5}.v88-week-journal th{background:#eaf2ff;color:#1e3a8a}.v88-week-journal small,.v88-week-journal details{font-size:11px;color:#64748b}.v88-week-journal summary{cursor:pointer}.v88-week-journal tr:nth-child(even){background:#f8fafc}</style>",
         "<div class='v88-week-journal'><table><thead><tr><th>个股 / 名单类型</th><th>首次上榜 / 原始说明</th>"]
    for i in range(7):
        day=start+timedelta(days=i);out.append(f"<th>{'● ' if day==today else ''}{['周一','周二','周三','周四','周五','周六','周日'][i]}<br>{day:%m-%d}</th>")
    out.append('<th>现在怎样 / 为什么变化</th></tr></thead><tbody>')
    for r in rows:
        href='?'+urlencode({'q':r['code'],'focus':'deep'})+'#v88-deep-analysis'
        kind='⭐ 正式研究榜 · '+HORIZONS.get(r['horizon'],'—') if r['kind']=='formal' else '👁 机会观察 · 未授级'
        if r['kind'].startswith('strategy_'):kind='🎯 新策略榜 · '+r['horizon']
        first=r['first'];score=f"{first.get('tier')} · {first['audit_score']:g}分" if first.get('audit_score') is not None else '未授中央评分'
        why=first.get('reason') or '旧档仅保存评级、分数与名次；原说明未留存。'
        out.append(f"<tr><td>{badge(r['market'],image_mode=True)}<br><a href='{esc(href)}'><b>{esc(display_name(r['name'],r['code'],profiles))}</b></a><br>{esc(r['code'])}<br><small>{esc(kind)}</small></td>")
        out.append(f"<td>{esc(r['first_at'][5:10])} {esc(r['first_at'][11:16])}<br>{esc(score)}<details><summary>当时说明</summary>{esc(why)}</details></td>")
        for i in range(7):
            day=start+timedelta(days=i);events=r['days'].get(day.isoformat(),[])
            if not events:
                out.append('<td style="color:#94a3b8">'+('待到日期' if day>today else '— 未留记录')+'</td>');continue
            appearances=[e for e in events if e['state']=='listed'];last=events[-1]
            headline=label(appearances[-1]) if appearances else label(last)
            status='<br>'+esc(label(last)) if appearances and last['state']!='listed' else ''
            details=''.join(f"<div>{esc(e['at'][11:16])} {esc(label(e))}<br>{esc(e.get('reason'))}</div>" for e in events)
            out.append(f"<td><b>{esc(headline)}</b>{status}<details><summary>当天记录 {len(events)} 条</summary>{details}</details></td>")
        current=r['current'];out.append(f"<td><b>{esc(current['label'])}</b><br>{esc(current['reason'][:110])}<details><summary>完整跟进说明</summary>{esc(current['reason'])}<br>历史评级不代表今日获准买入；榜单变化不等于投资结论已被证伪。</details></td></tr>")
    if not rows:out.append('<tr><td colspan="10">本周暂无已留存记录；不会补造此前的每日推荐。</td></tr>')
    out.append('</tbody></table></div>')
    return ''.join(out)


def day_rows(doc,day,query=''):
    q=query.strip().casefold();out=[]
    for row in doc.get('rows',[]):
        events=row.get('days',{}).get(day,[])
        if not events or (q and q not in (row['name']+' '+row['code']).casefold()):continue
        appearances=[e for e in events if e['state']=='listed']
        out.append({**row,'day_events':events,'appearance':appearances[-1] if appearances else None})
    return sorted(out,key=lambda r:(r['appearance'] is None,r['kind']!='formal',MARKETS.index(r['market']),
        -(r['appearance'].get('score') or 0) if r['appearance'] else 0,
        (r['appearance'].get('rank') or 999) if r['appearance'] else 999,r['code']))


def calendar_html(doc,month,selected,query='',now=None):
    import calendar
    now=now or datetime.now(BJT);today=now.astimezone(BJT).date()
    year,mon=map(int,month.split('-'));esc=lambda x:escape(str(x),quote=True)
    def href(day,code=''):
        return '?'+urlencode({'focus':'journal','month':day[:7],'day':day,'journal_query':query,
                             'journal_stock':code})+'#v88-day-detail'
    out=['''<style>
.v88-cal-wrap{overflow-x:auto;border:1px solid #dde3eb;border-radius:14px;background:white}
.v88-cal{width:100%;min-width:850px;table-layout:fixed;border-collapse:collapse;font-family:system-ui,sans-serif}
.v88-cal th{font-size:13px;font-weight:500;color:#64748b;padding:11px;background:#f8fafc;border-bottom:1px solid #e2e8f0}
.v88-cal td{border:1px solid #e7ebf0;vertical-align:top;height:96px;padding:7px 6px}
.v88-cal .outside{background:#f8fafc;color:#cbd5e1}.v88-cal .selected{background:#f0f6ff;box-shadow:inset 0 0 0 2px #6d9feb}
.v88-cal a{text-decoration:none;color:inherit}.v88-cal .date{display:block;text-align:right;margin-bottom:6px;font-size:16px;font-weight:600}
.v88-cal .today{display:inline-flex;background:#ef4444;color:white;width:28px;height:28px;align-items:center;justify-content:center;border-radius:50%}
.v88-cal .event{display:block;padding:4px 6px;margin:4px 0;border-radius:5px;border-left:3px solid #3b82f6;background:#e8f0ff;color:#1746a2;font-size:12px;line-height:1.5;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.v88-cal .event:hover,.v88-cal .event:focus{filter:brightness(.94);outline:2px solid #93b4ec}
.v88-cal .observe{background:#e2f5ef;border-color:#0d9488;color:#0f766e}.v88-cal .follow{background:#fff3dc;border-color:#d99b28;color:#936316}
.v88-cal .more{font-size:11px;color:#64748b;display:block;margin:5px 2px}.v88-cal .empty{font-size:11px;color:#b2bccb;margin-top:20px;text-align:center}
.v88-day-table{overflow-x:auto;border:1px solid #dce5ef;border-radius:10px}.v88-day-table table{border-collapse:collapse;width:100%;min-width:900px}
.v88-day-table th{background:#eaf2ff;color:#234269}.v88-day-table td,.v88-day-table th{padding:10px;border-bottom:1px solid #e2e8f0;font-size:13px;text-align:left;vertical-align:top}.v88-day-table details{font-size:11px;color:#64748b}.v88-day-table summary{cursor:pointer}
</style><div class="v88-cal-wrap"><table class="v88-cal" aria-label="推荐月历"><thead><tr>''']
    out.extend('<th>'+label+'</th>' for label in ('周日','周一','周二','周三','周四','周五','周六'))
    out.append('</tr></thead><tbody>')
    flags={'A股':'🇨🇳','港股':'🇭🇰','美股':'🇺🇸'}
    for week in calendar.Calendar(firstweekday=6).monthdatescalendar(year,mon):
        out.append('<tr>')
        for day in week:
            iso=day.isoformat();outside=day.month!=mon
            out.append('<td class="'+('outside' if outside else 'selected' if iso==selected else '')+'">')
            label=f'<span class="today">{day.day}</span>' if day==today else str(day.day)
            out.append(f'<a class="date" href="{esc(href(iso))}" aria-label="查看{iso}推荐">{label}</a>')
            members=day_rows(doc,iso,query) if not outside and day<=today else []
            listed=[r for r in members if r['appearance']]
            shown=[]
            for market in MARKETS:
                row=next((r for r in listed if r['market']==market),None)
                if row:shown.append(row)
            for r in shown:
                e=r['appearance'];formal=r['kind']=='formal' or r['kind'].startswith('strategy_')
                grade=f"{e.get('tier','')} {e['score']:g}分" if formal and e.get('score') is not None else '观察'
                if r['kind'].startswith('strategy_'):grade+=' · 策略'
                reason=e.get('reason') or '原说明未留存'
                title=f"{r['name']}（{r['code']}）\n{e['at'][11:16]} {grade} · #{e.get('rank') or '—'}\n{reason}\n点击查看当天内容"
                out.append(f'<a class="event {"" if formal else "observe"}" href="{esc(href(iso,r["code"]))}" title="{esc(title)}">{flags[r["market"]]} {esc(r["name"])} · {esc(grade)}<br><span>{esc(reason[:20])}</span></a>')
            follow=sum(r['appearance'] is None for r in members)
            if listed:out.append(f'<a class="more" href="{esc(href(iso))}">当天{len(listed)}条上榜 / 观察 · 查看全部 ›</a>')
            if follow:out.append(f'<a class="event follow" href="{esc(href(iso))}" title="原上榜个股的今日跟进；不计为新推荐">⏳ {follow}条跟进变化</a>')
            if not members and not outside:out.append('<div class="empty">'+('尚未到期' if day>today else '未留记录')+'</div>')
            out.append('</td>')
        out.append('</tr>')
    out.append('</tbody></table></div>')
    return ''.join(out)


def detail_html(rows,day):
    esc=lambda x:escape(str(x or '—'),quote=True)
    out=['<div class="v88-day-table"><table><thead><tr><th>市场 / 个股</th><th>当天记录</th><th>当时简述 / 变化</th><th>现在的跟进状态</th></tr></thead><tbody>']
    for r in rows:
        e=r['appearance'];events=r['day_events'];last=events[-1]
        state=(f"⭐ {e.get('tier')} · {e['score']:g}分 · #{e.get('rank')}" if e and e.get('score') is not None else '👁 机会观察' if e else '⏳ 后续跟进')
        reason=(e or last).get('reason') or '原说明未留存'
        href='?'+urlencode({'focus':'deep','q':r['code']})
        trail=[]
        for event in events:
            desc=(event.get('label') or '↘ 移出当次榜') if event['state']!='listed' else (f"{event.get('tier')} {event.get('score')}分" if event.get('tier') else '机会观察')
            trail.append(esc(event['at'][11:16]+' '+desc+'：'+str(event.get('reason') or '原说明未留存')))
        out.append(f'<tr><td>{esc(r["market"])}<br><a href="{esc(href)}" target="_blank"><b>{esc(r["name"])}</b></a><br>{esc(r["code"])}</td><td>{esc(state)}<br><small>{esc((e or last)["at"][11:16])} 北京</small></td><td>{esc(reason)}<details><summary>当天全部变化 · {len(events)}条</summary>'+ '<hr>'.join(trail)+f'</details></td><td>{esc(r["current"]["label"])}<br>{esc(r["current"]["reason"])}</td></tr>')
    if not rows:out.append('<tr><td colspan="4">这一天没有符合当前筛选的留存记录。</td></tr>')
    out.append('</tbody></table></div>');return ''.join(out)


def render(doc=None):
    import streamlit as st
    path=core_root()/'data/recommendation_journal_pub.json'
    if doc is None:
        try:doc=json.loads(path.read_text())
        except (OSError,ValueError):st.info('📅 推荐日历 · 正在整理原始记录');return
    from module_freshness import html as freshness_html
    st.html(freshness_html('recommendation_journal_pub.json',doc))
    today=datetime.now(BJT).date()
    dates={day for r in doc.get('rows',[]) for day in r.get('days',{})}
    month_arg=st.query_params.get('month','')
    try:date.fromisoformat(month_arg+'-01')
    except ValueError:month_arg=today.strftime('%Y-%m')
    months=sorted({day[:7] for day in dates}|{today.strftime('%Y-%m'),month_arg},reverse=True)
    st.markdown('<div id="v88-week-journal"></div><div style="font-size:24px;font-weight:800;color:#1e3a8a">📅 推荐日历</div>',unsafe_allow_html=True)
    st.caption('🔵 策略榜 / 旧制研究　🟢 机会观察　🟠 后续跟进 · 新评分标注“策略”；悬停看摘要，点击查看当天详情。北京时间留档。')
    left,right=st.columns([1,2])
    with left:month=st.selectbox('选择月份',months,index=months.index(month_arg),format_func=lambda m:m[:4]+'年'+str(int(m[5:]))+'月',key='v88_calendar_month_'+month_arg)
    with right:query=st.text_input('查找历史个股',value=st.query_params.get('journal_query',''),placeholder='名称 / 代码，例如 睿创、688002',key='v88_calendar_query')
    selected=st.query_params.get('day','')
    if not selected.startswith(month+'-'):
        selected=today.isoformat() if month==today.strftime('%Y-%m') else max([d for d in dates if d.startswith(month)] or [month+'-01'])
    try:date.fromisoformat(selected)
    except ValueError:selected=month+'-01'
    year,mon=map(int,month.split('-'));prev=(date(year,mon,1)-timedelta(days=1)).strftime('%Y-%m')
    nxt=(date(year+1,1,1) if mon==12 else date(year,mon+1,1)).strftime('%Y-%m')
    nav=lambda m:'?'+urlencode({'focus':'journal','month':m,'journal_query':query})
    st.html('<nav style="display:flex;gap:20px;font-size:14px">'+''.join(
        f'<a href="{escape(nav(m),quote=True)}" target="_self">{label}</a>' for label,m in
        [('‹ 上月',prev),('今天',today.strftime('%Y-%m')),('下月 ›',nxt),('↗ 单独打开日历',month)])+'</nav>')
    st.markdown(f'### {year}年{mon}月')
    st.html(calendar_html(doc,month,selected,query))
    st.markdown(f'<h3 id="v88-day-detail">{selected} · 当天推荐与跟进</h3>',unsafe_allow_html=True)
    rows=day_rows(doc,selected,query)
    chosen=st.query_params.get('journal_stock','')
    if chosen:
        rows=[r for r in rows if r['code']==chosen]
        st.html('<a target="_self" href="?'+escape(urlencode({'focus':'journal','month':month,'day':selected,'journal_query':query}),quote=True)+'#v88-day-detail">查看这一天全部个股</a>')
    groups=pages(rows)
    index=0
    if len(groups)>1:index=st.selectbox('当天详情分页 · 每市场最多5条 / 每页最多15条',range(len(groups)),format_func=lambda i:f'第 {i+1} / {len(groups)} 页',key='v88_calendar_page_'+selected+'_'+query+'_'+chosen)
    st.html(detail_html(groups[index],selected))
    st.caption('旧分保留当时值；跟进不算新推荐。未留记录不补造，全部历史保留，可按月或个股查找。')
