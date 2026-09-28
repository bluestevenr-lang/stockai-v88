"""Monthly Astra calendar; records survive selection changes and cycle rollover."""
import calendar
from collections import defaultdict
from datetime import datetime, timezone
from html import escape
from zoneinfo import ZoneInfo


def html(doc,now=None):
    now=now or datetime.now(timezone.utc);today=now.astimezone(ZoneInfo('Asia/Shanghai')).date()
    records=doc.get('calendar_records',[]);events=defaultdict(list)
    esc=lambda v:escape(str(v if v is not None else '待核'),quote=True)
    band=lambda v:'～'.join(f'{x:.2f}' for x in v) if v else '待核'
    for record in records:
        day=datetime.fromisoformat(record['at']).astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat()
        events[day].append(('📝 推荐记录',record))
        for seen in record.get('seen_dates',[]):
            if seen!=day:events[seen].append(('↻ 继续跟踪',record))
        outcome=record.get('outcome') or {}
        if outcome.get('date'):events[outcome['date']].append((outcome['label'],record))
    months=sorted({day[:7] for day in events}|{today.isoformat()[:7]},reverse=True)
    out=['''<style>.astra-calendar{margin:20px 0}.astra-calendar table{border-collapse:collapse;width:100%;min-width:800px;table-layout:fixed}.astra-calendar th,.astra-calendar td{border:1px solid #dbe3f3;padding:7px;vertical-align:top}.astra-calendar td{height:104px}.astra-calendar summary{cursor:pointer}.astra-calendar .event{background:#eaf0ff;border-left:3px solid #6366f1;padding:5px;margin-top:5px;font-size:13px;border-radius:4px}.astra-calendar .hit{background:#e7f8ef;border-color:#16945a}.astra-calendar .day{font-weight:700}.astra-calendar small{color:#64748b}.astra-calendar details p{overflow-wrap:anywhere}</style>
    <section class="astra-calendar" id="v88-astra-calendar"><h3>📅 Astra月度记录 · 15日目标跟踪</h3>
    <p>推荐、原区间与结果持续留档。🎯 代表发布后触及原止盈下沿的行情记录；实际盈利以成交账本为准。</p>''']
    for month in months:
        year,mon=map(int,month.split('-'))
        month_records=[r for r in records if datetime.fromisoformat(r['at']).astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat()[:7]==month]
        hits=[r for r in records if (r.get('outcome') or {}).get('status')=='target_touched' and (r['outcome'].get('date') or '').startswith(month)]
        out.append(f'<details {"open" if month==today.isoformat()[:7] else ""}><summary style="font-size:18px;font-weight:700">{year}年{mon}月 · {len({r["row"]["code"] for r in month_records})}只个股 · {len(month_records)}条留档版本 · 🎯 {len(hits)}条目标触及记录</summary>')
        if not hits:out.append('<p><small>本月暂无已核实的15日内目标触及记录；不会把历史上涨回填为新推荐成绩。</small></p>')
        out.append('<div style="overflow-x:auto"><table><thead><tr>'+''.join(f'<th>{x}</th>' for x in ('周一','周二','周三','周四','周五','周六','周日'))+'</tr></thead><tbody>')
        for week in calendar.Calendar().monthdatescalendar(year,mon):
            out.append('<tr>')
            for day in week:
                style='background:#eff5ff;box-shadow:inset 0 0 0 2px #6393ff' if day==today else 'background:#f4f6f9' if day.month!=mon else ''
                out.append(f'<td style="{style}"><div class="day">{day.day}</div>')
                daily=events.get(day.isoformat(),[]) if day.month==mon else []
                counts=defaultdict(int);shown=set()
                for label,record in daily:
                    r=record['row'];key=(r['code'],record['id'],label)
                    if key in shown or counts[r['market']]>=5:continue
                    shown.add(key);counts[r['market']]+=1
                    score=(r.get('strategy_score') or {}).get('value');outcome=record.get('outcome') or {}
                    flags={'A股':'🇨🇳','港股':'🇭🇰','美股':'🇺🇸'}
                    description=f'{label} · 原评分 {score}（历史规则 {(r.get('strategy_score') or {}).get('revision','旧版')}） · 入场 {band(r.get("entry_range"))} · 止盈 {band(r.get("take_profit_range"))} · 止损 {band(r.get("stop_range"))} · {r.get("business_reason", "")} '
                    out.append(f'<details class="event {"hit" if label.startswith("🎯") else ""}" title="{esc(description)}"><summary>{flags.get(r["market"],"")} {esc(r["name"])}<br>{esc(label)}</summary><p>{esc(description)}</p><p>计划轮次 {esc(record["cycle_id"])}<br>实际记录 {esc(record["at"])}<br>截止 {esc((r.get("window") or {}).get("deadline"))}</p><p>{esc(outcome.get("label","○ 待核"))}<br>{esc(outcome.get("reason",outcome.get("scope","")))}</p></details>')
                if not daily and day.month==mon:out.append('<small>'+('尚未到期' if day>today else '暂无记录')+'</small>')
                if len(daily)>len(shown):out.append('<small>其余变化已归档；每日各市场最多显示5条</small>')
                out.append('</td>')
            out.append('</tr>')
        out.append('</tbody></table></div></details>')
    out.append('<small>从发布后首个完整交易日起核验。先触及风险线不计目标达成；同日双触及单列顺序不明，缺失行情不计成功。鼠标悬停看摘要，点击查看原计划。</small></section>')
    return ''.join(out)
