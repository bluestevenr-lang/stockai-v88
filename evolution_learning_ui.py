"""Shared read-only evolution display; no fetch, model, grading or ledger writes."""
from datetime import datetime, timezone
from html import escape
import json
import math
from pathlib import Path

ROOT = Path('/Users/bluesteven/Desktop/ai-daily-report-v2/data')
LABELS = {'ADDED':'首次建档','PROMOTED':'升级','DEMOTED':'降级','UPDATED':'条件/分数更新',
    'REVIEW_STATUS_CHANGED':'审核状态变化','REENTERED':'重新入选','EXITED':'价值失效',
    'HORIZON_CHANGED':'审核周期变化','RETAINED_OUTSIDE_POOL':'扫描落选·持续跟踪',
    'RETURNED_TO_POOL':'重返扫描池','FOCUS_ENTERED':'进入重点榜','FOCUS_LEFT':'退出重点榜·保留档案',
    'CURRENT_GRADE_LOST':'审核失效·保留历史评级','CURRENT_GRADE_RESTORED':'恢复当前评级',
    'MONITOR_STARTED':'演变监控接入','MONITOR_REFRESH':'监控快照更新',
    'WATCH_ENTERED':'进入固定研究席位','WATCH_LEFT':'退出固定席位·档案保留'}
HORIZONS = {'short':'短期','medium':'中期','long':'长期'}


def esc(value):
    return escape(str(value if value is not None else '—'))


def read(root=ROOT):
    try:
        return json.loads((Path(root)/'evolution_learning_pub.json').read_text())
    except (OSError, ValueError):
        return {}


def health(doc, status, selection=None, now=None):
    now = now or datetime.now(timezone.utc)
    try:
        age = (now-datetime.fromisoformat(doc['generated_at'])).total_seconds()
        okay = status.get('ok') is True and status.get('generated_at') == doc['generated_at'] and 0 <= age <= 900
        if selection is not None:
            okay = okay and bool(doc.get('factpack_id') and doc.get('source_generated_at')) and doc.get('factpack_id') == selection.get('factpack_id') and doc.get('source_generated_at') == selection.get('generated_at')
        return '演变数据已同步' if okay else '演变数据待同步·以下保留原始记录与时间'
    except (ValueError, TypeError, KeyError):
        return '演变记录尚未生成'


def event_rows(stock):
    rows = []
    for e in stock.get('events', []):
        historical = e['kind'] == 'registry_event'
        body = e['event'] if historical else {}
        labels = [body.get('kind')] if historical else e.get('changes') or ['MONITOR_REFRESH']
        before = body.get('from') if historical else (e.get('previous') or {}).get('tier')
        after = body.get('to') if historical else e.get('state', {}).get('tier')
        fmt = lambda t: t if t in {'1A','2A','3A'} else '无当前评级'
        rows.append({'时间':e['event_at'], '事件':' / '.join(LABELS.get(k,k) for k in labels),
            '评级变化':fmt(before)+' → '+fmt(after),
            '原审核分':body.get('score') if historical else e.get('state',{}).get('score'),
            '原因':body.get('reason') if historical else e.get('state',{}).get('reason'),
            '审核编号':body.get('audit_id') if historical else e.get('state',{}).get('audit_id'),
            '证据来源':'历史账本导入（非回填业绩）' if historical else '当时监控捕获',
            '本模块捕获时间':e['captured_at']})
    return rows


def grade_chart(stock):
    points = []
    for e in stock.get('events', []):
        grade = e['event'].get('to') if e['kind']=='registry_event' else e['state'].get('tier')
        points.append((e['event_at'], {'1A':1,'2A':2,'3A':3}.get(grade,0)))
    if not points:
        return '<small>尚无评级沿革</small>'
    try:
        parsed=[(datetime.fromisoformat(at),level,at) for at,level in points]
        if any(t.tzinfo is None for t,_,_ in parsed):raise ValueError('missing timezone')
        parsed.sort(key=lambda p:p[0])
    except (ValueError,TypeError):return '<small>评级事件时间待核；原始记录见下表，不推断先后走势。</small>'
    points=[(at,level) for _,level,at in parsed]
    times = [t.timestamp() for t,_,_ in parsed]
    lo, hi = min(times), max(times)
    width = max(1, hi-lo)
    chart = '<svg role="img" aria-label="历史评级变更记录，非股价走势或未来预测；0表示无当前评级" viewBox="0 0 680 145" style="width:100%;max-width:850px">'
    for level in range(4):
        y=108-level*28
        chart += f'<text x="1" y="{y+4}" font-size="10" fill="#64748b">{str(level)+"A" if level else "待核"}</text><path d="M40 {y}H665" stroke="#e2e8f0"/>'
    last=None
    for (at,level),t in zip(points,times):
        x,y=40+(t-lo)/width*625,108-level*28
        if last:
            chart+=f'<path d="M{last[0]} {last[1]}H{x}V{y}" stroke="#2563eb" stroke-width="1.5" fill="none"/>'
        chart+=f'<circle cx="{x}" cy="{y}" r="3" fill="{("#2563eb" if level else "#94a3b8")}"><title>{esc(at)} · {level if level else "无当前"}A</title></circle>'
        last=(x,y)
    return chart+f'<text x="40" y="135" font-size="10">{esc(points[0][0][:10])}</text><text x="590" y="135" font-size="10">{esc(points[-1][0][:10])}</text></svg>'


def price_chart(observation):
    points=(observation or {}).get('prices') or []
    if len(points)<2:
        return '<div>尚无足够登记后交易日；不绘制虚构走势。</div>'
    try:
        dates=[datetime.fromisoformat(p['date']).date() for p in points]
        if dates!=sorted(set(dates)) or any(type(p['close']) not in (int,float) or not math.isfinite(p['close']) or p['close']<=0 for p in points):raise ValueError('invalid prices')
    except (ValueError,TypeError,KeyError):return '<div>价格或日期证据待核；不绘制为已验证走势。</div>'
    low=min(p['close'] for p in points);high=max(p['close'] for p in points)
    span=max(high-low,high*.01)
    path=' '.join(f'{"M" if i==0 else "L"}{40+625*i/(len(points)-1):.2f} {100-80*(p["close"]-low)/span:.2f}' for i,p in enumerate(points))
    return (f'<small>原参考收盘与登记后收盘；不是成交收益。来源 {esc(observation.get("source"))} · 口径 {esc(observation.get("basis"))}</small>'
            f'<svg role="img" aria-label="原参考收盘与登记后收盘观察，非实际成交收益" viewBox="0 0 680 135" style="width:100%;max-width:850px">'
            f'<path d="{path}" stroke="#0f766e" stroke-width="2" fill="none"/>'
            f'<text x="2" y="22" font-size="10">{high:g}</text><text x="2" y="104" font-size="10">{low:g}</text>'
            f'<text x="40" y="127" font-size="10">{esc(points[0]["date"])}</text><text x="590" y="127" font-size="10">{esc(points[-1]["date"])}</text></svg>')


def stock_html(stock, *, compact=False):
    if not stock:
        return '<div class="v88-evolution-stock">尚未进入持续评级档案；不倒填历史推荐。</div>'
    from stock_profile_view import display_name, load as load_profiles
    name = display_name(stock['name'], stock['code'], load_profiles(ROOT/'stock_profiles_pub.json'))
    value = (f'<b>{esc(name)} · 历史评级与原合同核验</b>'
             f'<div>最近监控快照评级 {esc(stock.get("current_tier") or "无有效评级")} · 最近价值评级记录 {esc(stock.get("last_value_tier"))}'
             f' · {"固定研究席位" if stock.get("watch_seat") else "保留跟踪"}；退出重点榜不等于评级降级。</div>')
    value += '<div style="font-size:11px;color:#64748b">下图仅记录真实评级变更时间，不是股价趋势；未来一年走向见年度趋势分析。</div>'+grade_chart(stock)
    rows = event_rows(stock)
    value += f'<details><summary>历次变更与原因（{len(rows)}条）</summary><div style="overflow:auto"><table style="font-size:11px;width:100%"><thead><tr><th>时间</th><th>事件</th><th>评级</th><th>审核分</th><th>原因</th></tr></thead><tbody>'
    for row in reversed(rows):
        value += '<tr>'+''.join(f'<td>{esc(row[k])}</td>' for k in ('时间','事件','评级变化','原审核分','原因'))+'</tr>'
    value += '</tbody></table></div><small>历史事件保留原时点；本模块接入前记录不充当历史已知凭据或已验证业绩。</small></details>'
    for c in stock.get('contracts', []):
        obs=c.get('observation') or {}
        val=obs.get('observed_change_pct')
        net_r=c.get('net_r')
        verified=bool(c.get('settlement_verified') is True and c.get('status')=='SETTLED'
                      and c.get('actual_fill') is False and type(net_r) in (int,float) and math.isfinite(net_r))
        value += (f'<details {"" if compact else "open"}><summary>原合同 {esc(c["recorded_at"][:10])} · {esc(c["original_tier"])} / {esc(c["original_score"])}分 · {esc(c["status_label"])}</summary>'
                  f'<div>原进场 {esc(c["entry_range"])} · 原止盈 {esc(c["take_profit_range"])} · 原失效 {esc(c["stop"])} · 截止 {esc(c["deadline"][:10])}</div>'
                  f'<div>观察涨跌 {f"{val:+.2f}%" if type(val) in (int,float) and math.isfinite(val) else "尚未形成"}；模拟净R {esc(round(net_r,3)) if verified else "未结算/未验证"} · 非实际成交</div>'
                  +price_chart(obs)+f'<div>{esc(c.get("reason") or obs.get("meaning"))}</div>'
                  f'<small>原审核编号 {esc(c["original_audit_id"])} · 合同 {esc(c["contract_id"][:12])} · {esc(HORIZONS.get(c["horizon"],c["horizon"]))}</small></details>')
    if stock.get('history_gap'):
        value += '<div>'+esc(stock['history_gap'])+'</div>'
    if stock.get('next_action'):
        value += '<div><b>下一步反查：</b>'+esc(stock['next_action']['action'])+'</div>'
    if compact:
        return ('<details class="v88-evolution-stock v88-grade-history-fold" style="font-size:11px;color:#475569;margin:6px 0">'
                +f'<summary>🗂 评级升降级与原合同记录 · {len(rows)}条（展开）</summary>'+value+'</details>')
    return '<section class="v88-evolution-stock" style="font-size:12px;border:1px solid #dbe3ec;border-radius:6px;padding:10px;margin:8px 0">'+value+'</section>'


def render(doc, status, selection):
    import streamlit as st
    st.markdown('**📈 评级演变 · 行情验证 · 周/月复盘**')
    summary=doc.get('summary') or {}
    st.caption(f'{health(doc,status,selection)} · 跟踪 {summary.get("tracked_stocks",0)}只 · 原合同 {summary.get("contracts",0)}份 · 已核验模拟结算 {summary.get("verified_settlements",0)}笔 · 更新 {doc.get("generated_at","未生成")}')
    with st.expander('查看个股升降级、后续走势与周/月学习记录', expanded=False):
        stocks=doc.get('stocks') or {}
        if not stocks:
            st.info('演变档案尚未加载；当前列表与原始历史继续保留。')
            return
        market=st.selectbox('演变市场',['全部','A股','港股','美股'],key='evolution_market')
        from stock_profile_view import display_name, load as load_profiles
        profiles=load_profiles(ROOT/'stock_profiles_pub.json')
        names={c:display_name(s['name'],c,profiles) for c,s in stocks.items()}
        codes=sorted([c for c,s in stocks.items() if market=='全部' or s.get('market')==market],key=lambda c:(not stocks[c].get('watch_seat'),names[c]))
        if codes:
            code=st.selectbox('跟踪个股（含降级与退出）',codes,format_func=lambda c:f'{names[c]} · {c}',key='evolution_stock')
            st.markdown(stock_html(stocks[code]),unsafe_allow_html=True)
        reports=doc.get('periods') or []
        if reports:
            cadence=st.radio('复盘周期',['周度','月度'],horizontal=True,key='evolution_cadence')
            filtered=[r for r in reports if r['cadence']==('week' if cadence=='周度' else 'month')]
            selected=st.selectbox('复盘归档',[r['period'] for r in filtered],key='evolution_period')
            p=next(r for r in filtered if r['period']==selected)
            st.caption(f'{p["period"]} · {"进行中" if p["status"]=="OPEN" else "已封存"} · 新合同 {p["new_contracts"]} · 新核验结算 {p["new_verified_settlements"]}；{p["scope"]}')
            st.write('变更：'+' · '.join(f'{LABELS.get(k,k)} {v}' for k,v in p['changes'].items()))
            st.write('原合同状态：'+' · '.join(f'{k} {v}' for k,v in p['denominator'].items()))
            if p['groups']:
                st.dataframe(p['groups'],hide_index=True,width='stretch')
            else:
                st.caption('本期尚无已验证结算样本；不显示胜率，不把未成交上涨计入盈利。')
            st.caption(p['rule_update_status'])
        st.caption(doc.get('learning_policy',''))
        feedback=doc.get('review_feedback') or {}
        if feedback:
            st.dataframe([{'代码':c,'反查优先级':r['priority'],'后续动作':r['action']} for c,r in feedback.items()],hide_index=True,width='stretch')
