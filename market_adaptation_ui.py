"""Same read-only regime/risk publication used by the Feishu confirmation."""
from datetime import datetime,timezone
from html import escape
from pathlib import Path
from urllib.parse import urlencode
from zoneinfo import ZoneInfo
import json
from v88_paths import core_root
from market_badge import html as _market_badge
from display_limits import market_top

def market_badge(market):
    return _market_badge(market,image_mode=True)



def name(row,full_profile=True):
    p=row.get('profile') or {};code=row['code']
    zh='中文名待核' if '英文原名＋中文行业说明' in (p.get('name_kind') or '') else p.get('name_zh') or '中文待核'
    label=(f"{p.get('name_en') or code} · {zh}" if row['market']=='美股' else p.get('name_zh') or row.get('name') or code)
    shown=code[:-3].zfill(4)+'.HK' if code.endswith('.HK') else code
    href='?'+urlencode({'q':code,'focus':'deep'})+'#v88-deep-analysis'
    from stock_profile_view import industry_rank_html
    rank=industry_rank_html(code,compact=True) if full_profile else ""
    return f"<a href='{escape(href,quote=True)}'>{escape(label)}</a><br><small>{escape(shown)} · {escape(p.get('industry') or '行业待核')}</small><br>{rank}"



def nontechnical_html(row):
    """Complete evidence stays on the page; unsupported observations stay labelled."""
    doc=row.get('nontechnical') or {};status=doc.get('status','pending')
    e=lambda value:escape(str(value or ''),quote=True)
    label={'supported':'🏢 非技术依据','counterevidence':'⚠️ 非技术反证','pending':'⏳ 非技术依据待补'}.get(status,'⏳ 非技术依据待补')
    summary=doc.get('summary') or ('发现反证，需重新核验' if status=='counterevidence' else '目前仅有量价线索，暂不作为推荐理由')
    color={'supported':'#166534','counterevidence':'#be123c'}.get(status,'#64748b')
    support=str(doc.get('support_summary') or '')
    risk=str(doc.get('risk_summary') or '')
    mixed=status=='counterevidence' and doc.get('has_current_non_technical_support') is True and bool(support)
    if mixed:
        out=["<b style='color:#166534'>🏢 经营支持＋⚠️ 风险并存</b>",
             f"<div style='color:#166534'>{e(support)}</div>",
             f"<div style='color:#be123c'>⚠️ {e(risk or summary or '反证需复核')}</div>"]
    else:
        out=[f"<b style='color:{color}'>{label}</b><br>{e(summary)}"]
    evidence=doc.get('evidence') or [];risks=doc.get('risks') or [];gaps=doc.get('gaps') or []
    out.append("<details class='evidence'><summary>来源、反证与待核项</summary>")
    if doc.get('search_checked_at'):
        try: checked=datetime.fromisoformat(doc['search_checked_at']).astimezone(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M 北京时间')
        except (ValueError,TypeError): checked='时间待核'
        out.append('<div>📅 资料检索核至 '+e(checked)+(' · 后续公告待检' if doc.get('search_fresh') is False else '')+'</div>')
    for item in evidence:
        url=str(item.get('source_url') or '')
        source=(f"<a href='{e(url)}' target='_blank' rel='noopener noreferrer'>原始来源 ↗</a>" if url.startswith(('https://','http://')) else '来源链接待核')
        out.append(f"<div>• {e(item.get('claim'))}<br><small>{e(item.get('kind'))} · {e(item.get('asof') or '日期待核')} · {source}</small></div>")
    for item in risks:out.append(f"<div>⚠️ {e(item)}</div>")
    for item in gaps:out.append(f"<div>○ {e(item)}</div>")
    if not evidence:out.append('○ 尚无可引用的当前企业、财务或事件证据。')
    out.append('<div>非技术依据不能单独授级；仍需中央独立复审、入场及风险条件。</div></details>')
    return ''.join(out)


def left_entry_html(doc,*,now=None):
    from observation_score import details_html as observation_html
    e=lambda value:escape(str(value or ''),quote=True)
    low=doc.get('left_entry_watch')
    if not isinstance(low,dict):
        legacy=(doc.get('low_reversals') or {}).get('rows') or []
        low={'rows':[{**r,'signals':[{'kind':'low52','label':'近一年低位','window_start':r.get('window_start'),'window_end':r.get('window_end'),'position_pct':r.get('position_pct'),'distance_low_pct':r.get('distance_low_pct')}],
                            'setup_state':'breakout_watch' if r.get('rank_priority')==0 else 'stabilizing','state_label':r.get('kind')} for r in legacy],
             'scan':(doc.get('low_reversals') or {}).get('scan') or {}}
    selected=market_top(low.get('rows') or [],rank_key='rank')
    kinds=(('decline4','🔔 连跌≥4日'),('history_low','🕰 已有历史低位'),('low52','📉 52周低位'),('swing_low','〰 波段低位'))
    counts={kind:sum(any(s.get('kind')==kind for s in row.get('signals',[])) for row in selected) for kind,_ in kinds}
    out=[f"<h3 id='v88-left-entry-watch'>③ 左侧观察 · {len(selected)}只</h3><small>中美港各最多5只 · 按观察优先级排列，非审核分排名</small>",'<div class="signal-counts">'+''.join(f"<span class='chip'>{label} {counts[kind]}</span> " for kind,label in kinds)+'</div>',
         "<div class='note'>🔴 未确认企稳 → 🟡 企稳迹象 → 🔵 突破待验。本栏标签可重叠；接近低位不等于已见底。</div>"]
    if selected:
        out.append("<div class='scroll'><table><thead><tr><th>市场 / 个股</th><th>提醒类型 / 当前阶段</th><th>下一步 / 风险边界</th><th>技术面以外的依据</th></tr></thead><tbody>")
        for row in selected:
            state=row.get('setup_state','falling');state_label={'falling':'未确认企稳','stabilizing':'企稳迹象','breakout_watch':'突破待验'}.get(state,'待核')
            icon={'falling':'🔴','stabilizing':'🟡','breakout_watch':'🔵'}.get(state,'○')
            chips=[];windows=[]
            for signal in row.get('signals') or []:
                kind=signal.get('kind');label=signal.get('label') or dict(kinds).get(kind,kind)
                chips.append(f"<span class='chip'>{e(label)}</span>")
                values=[]
                if signal.get('days') is not None:values.append(f"连续 {signal['days']} 个完整交易日")
                if signal.get('low') is not None:values.append(f"低点 {signal['low']:g}")
                if signal.get('distance_low_pct') is not None:values.append(f"距低点 {signal['distance_low_pct']:+.1f}%")
                if signal.get('position_pct') is not None:values.append(f"区间位置 {signal['position_pct']:.1f}%")
                windows.append(f"<div>• {e(label)} · {e(' · '.join(values))}<br>{e(signal.get('window_start') or '起点待核')} → {e(signal.get('window_end') or '终点待核')}</div>")
            bounds='失效位待核' if row.get('invalidation') is None else f"跌破 {row['invalidation']:g}，撤销当前企稳线索"
            if state=='falling':bounds=('观察低位 '+f"{row['invalidation']:g}"+'；' if row.get('invalidation') is not None else '')+'原风险线优先，继续破低加强风险复核'
            amount='现价待核' if row.get('last') is None else f"现 {row['last']:g}"
            out.append(f"<tr><td>{market_badge(row.get('market'))} · {name(row)}<br><small>{amount} · 日线 {e(row.get('source_asof') or row.get('window_end'))}</small></td><td>{''.join(chips)}<br><b>{icon} {e(state_label)}</b><details><summary>低位窗口与信号依据</summary>{''.join(windows)}{e(row.get('why'))}</details></td><td><b>👁 {e(row.get('next_check'))}</b><br><small>↘ {e(bounds)}<br>观察位不替代原合同止损。</small></td><td>{observation_html(row,now=now)}{nontechnical_html(row)}</td></tr>")
        out.append('</tbody></table></div>')
    else:out.append("<div class='note'>本次已核范围没有触发以上提醒；缺失行情继续补核。</div>")
    out.append('<small>其余对象继续后台监控。历史低位仅指已核数据范围，具体窗口见个股依据。</small>')
    return ''.join(out)

def html(doc=None,*,base=None,now=None):
    from observation_score import details_html as observation_html
    e=lambda v:escape(str(v or ''),quote=True)
    now=now or datetime.now(timezone.utc)
    try:
        if doc is None:doc=json.loads((Path(base or core_root())/'data/market_adaptation_pub.json').read_text())
        if doc.get('version')!='market-adaptation-v1-preview' or not 0<=(now-datetime.fromisoformat(doc['generated_at'])).total_seconds()<=43200:raise ValueError('old')
    except (ValueError,TypeError,KeyError,OSError):
        return "<section id='v88-market-adaptation' class='v88-adaptation'>🧭 环境与风险观察 · 等待最新核验</section>"
    review=doc.get('model_review') or {};model={r['market']:r for r in review.get('markets',[])} if review.get('status')=='REVIEWED' else {}
    preview=doc.get('mode')!='ACTIVE'
    out=["""<style>
    .v88-adaptation{background:#fff;border:1px solid #cbd5e1;border-radius:16px;padding:18px;margin:14px 0;color:#16324f}
    .v88-adaptation h2{font-size:23px!important;margin:0 0 8px!important}.v88-adaptation h3{font-size:18px!important;margin:18px 0 8px!important}
    .v88-adaptation td{padding:12px;border-bottom:1px solid #e2e8f0;vertical-align:top;font-size:14px!important;line-height:1.5!important}
    .v88-adaptation th{padding:10px;background:#eff6ff;text-align:left;font-size:13px!important}.v88-adaptation small,.v88-adaptation details{font-size:12px!important;color:#64748b}
    .v88-adaptation .signal-counts{display:flex;gap:5px;flex-wrap:wrap}.v88-adaptation .evidence{margin-top:5px}.v88-adaptation details>summary{cursor:pointer;padding:4px 0}.v88-adaptation .v88-market-badge span{font-size:12px!important}.v88-adaptation a{font-weight:700;color:#075985}.v88-adaptation .metric{font-size:24px;font-weight:800}.v88-adaptation .chip{display:inline-block;padding:4px 10px;border-radius:20px;background:#e0f2fe;font-weight:700}
    .v88-adaptation table{width:100%;min-width:920px;border-collapse:collapse}.v88-adaptation .scroll{overflow-x:auto}.v88-adaptation .note{background:#f1f5f9;padding:9px 12px;border-radius:8px;margin:8px 0;font-size:13px}
    </style><section id='v88-market-adaptation' class='v88-adaptation'><h2>🧭 市场环境与风险观察</h2>
    <div class='chip'>📝 整改确认预览</div> <small>正式推送等待确认 · 与飞书共用数据</small>
    <h3>① 先看环境，再选观察周期</h3><div class='scroll'><table><thead><tr><th>市场 / 当前结构</th><th>未来条件路径</th><th>观察方案</th></tr></thead><tbody>"""]
    for market,flag in (('A股','🇨🇳'),('港股','🇭🇰'),('美股','🇺🇸')):
        r=(doc.get('markets') or {}).get(market) or {};v=model.get(market) or {}
        icon={'trend_up':'↗','transition':'↘','range':'↔','trend_down':'↓','mixed':'⇄','unknown':'○'}.get(r.get('regime'),'○')
        color={'trend_up':'#15803d','transition':'#b45309','range':'#0369a1','trend_down':'#be123c'}.get(r.get('regime'),'#475569')
        stamps=' · '.join(f"{s['code']} {s['asof']}" for s in r.get('benchmarks',[])) or '指数待核'
        summary=v.get('summary') or '规则初判；当前事实尚待GPT专项复核'
        signals=r.get('benchmarks') or []
        structure=('长期结构仍向上' if all(s.get('long_up_structure') for s in signals) else '长期结构待修复') if signals else '等待数据'
        weakness=('短线共同偏弱' if all(s.get('weak_signals',0)>=2 for s in signals) else '短线观察分歧') if signals else '指数证据不足'
        paths=("<div style='padding:7px 0;color:#0369a1'>↗ 修复均线 → 突破区间 → 再评延续</div><small>↘ 持续破位 → 防守复核</small>" if signals else '<div>○ 待证据更新后显示条件路径</div>')
        out.append(f"<tr><td style='width:28%'><b>{market_badge(market)}</b><br><strong style='font-size:19px;color:{color}'>{icon} {e(r.get('label'))}</strong><br><small>{e(stamps)}</small></td><td style='width:43%'><b>{e(weakness)} · {e(structure)}</b>{paths}<details><summary>GPT判断与具体条件</summary>{e(summary)}<br>↗ {e(v.get('up_condition') or r.get('up_case'))}<br>↘ {e(v.get('down_condition') or r.get('down_case'))}</details></td><td><b>{e(r.get('mode'))}</b><br>{e(r.get('horizon'))}<br><small>{e(r.get('intraday_rule'))}</small></td></tr>")
    out+=['</tbody></table></div>',"<div class='note'>👁 环境识别 → 候选观察 → 核验触发与风险 → 独立复审。模式变化不放宽原止损、不延长原期限；持续下跌先防守。</div>"]
    monthly=doc.get('monthly_monitor') or {};rows=monthly.get('rows') or [];alerts=[r for r in rows if r.get('status')=='alert'];pending=[r for r in rows if r.get('status')=='data_pending'];near=[r for r in rows if r.get('status')=='checked' and (r.get('streak') or {}).get('days',0)==3]
    ranked=sorted(alerts,key=lambda r:-(r.get('streak') or {}).get('days',0))+sorted(near,key=lambda r:-(r.get('streak') or {}).get('days',0))
    displayed=market_top(ranked)
    out.append(f"<h3>② 月度名单 · 连跌风险 · {len(displayed)}只</h3><div><small>按提醒优先、连跌天数排列 · 各市场最多5只 · ⏳ 待核 {monthly.get('pending',0)}只</small></div>")
    if not alerts:out.append("<div class='note'>当前已核部分未触发四连跌；待核部分不能据此排除风险。</div>" if pending else "<div class='note'>当前已核名单未触发四连跌，继续按完整交易日监控。</div>")
    if alerts or near:
        out.append("<div class='scroll'><table><thead><tr><th>月度观察股</th><th>连续收盘下跌</th><th>需要关注</th></tr></thead><tbody>")
        for r in displayed:
            s=r['streak'];level='🔴 提醒' if r['status']=='alert' else '🟠 接近提醒'
            out.append(f"<tr><td>{name(r)}<br><small>{e(r['membership'])}</small>{observation_html(r,now=now)}</td><td><b>{level} · {s['days']}交易日 / {s['change_pct']:+.2f}%</b><br><small>{e(s['start'])} → {e(s['end'])}</small></td><td>核对原失效位、下跌原因与退出条件<br><small>连跌涨跌幅以该段前一日收盘为基准；不等于入选后收益，不自动抄底或卖出。</small></td></tr>")
        out.append('</tbody></table></div>')
    out.append("<small>本月曾入选、已退榜及其余对象继续后台监控；待核不等于安全。</small>")
    out.append(left_entry_html(doc,now=now))
    out.append("<details style='margin-top:12px'><summary>经典依据、模型复核与数据范围</summary>")
    out.append(('🧠 GPT Astra已复核当前市场事实 · '+e(review.get('reviewed_at'))+'<br>'+e(review.get('limitations'))) if model else '🧠 GPT专项复核待完成；现显示规则初判。')
    out.append('<br>每个市场只是指数代理判断，不等于所有个股；港股当前使用恒生指数。近一年低位不等于上市以来最低；缺失年度数据不补造。数值阈值是V88研究参数，未经收益校准，不是书中原文或胜率。')
    for c in doc.get('classics',[]):out.append(f"<br><a href='{e(c['url'])}' target='_blank' rel='noopener'>{e(c['title'])}</a>：{e(c['principle'])}")
    out.append('<br>页面与消息读取缓存，新增模型调用0；市场事实变化时需单独复核，未返回有效结果时显示待核。</details></section>')
    result=''.join(out)
    return result if preview else result.replace('📝 整改确认预览','✓ 持续观察已启用').replace('正式推送等待确认 · 与飞书共用数据','与飞书共用数据 · 随完整交易日复核')
