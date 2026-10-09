"""Same strategy publication on desktop and cloud; no inference in the UI."""
import json
from datetime import datetime,timezone
from html import escape
from urllib.parse import urlencode
from v88_paths import core_root

def html(doc=None,code=None,now=None,profiles=None):
    if doc is None:
        try:doc=json.loads((core_root()/'data/strategy_board.json').read_text())
        except (OSError,ValueError):doc={}
    if doc.get('version')!='strategy-taxonomy-2026-09-28-v1':return ''
    now=now or datetime.now(timezone.utc)
    from screening_clock import display,screened_at,quote_clocks
    opened=now
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        if get_script_run_ctx(suppress_warning=True):
            import streamlit as st
            opened=st.session_state.setdefault('_v88_opened_at',now.isoformat())
    except ImportError:pass
    clock='🧮 筛选完成：'+display(screened_at(doc))+'（北京时间）'
    read_clock='🖥 本次打开：'+display(opened)+' · 页面读取：'+display(now)+'（北京时间）'
    from stock_profile_view import html as profile_html
    from strategy_board_state import view
    doc=view(doc,now)
    esc=lambda v:escape(str(v if v is not None else '—'),quote=True)
    flags={'A股':'🇨🇳','港股':'🇭🇰','美股':'🇺🇸'}
    rows=[r for r in doc.get('rows',[])+doc.get('investment_candidates',[]) if not code or r['code']==code]
    out=['''<style>.strategy-board .lane{margin:14px 0;border:1px solid #d8e4f3;border-radius:14px;overflow:hidden;background:#fff}
    .strategy-board .head{padding:18px 20px;background:#eef5ff;display:flex;justify-content:space-between;align-items:center;gap:12px}
    .strategy-board h3{font-size:22px!important;margin:0 0 5px}.strategy-board small{font-size:13px!important;color:#64748b}
    .strategy-board .count{font-size:28px;color:#2459c7;font-weight:700;white-space:nowrap}
    .strategy-board table{border-collapse:collapse;width:100%;min-width:1450px}.strategy-board th{background:#f5f8fc;text-align:left}
    .strategy-board td,.strategy-board th{padding:12px 14px;border-bottom:1px solid #e5ebf4;vertical-align:top;font-size:14px!important;line-height:1.6;overflow-wrap:anywhere}
    .strategy-board details{font-size:13px}.strategy-board .score{font-size:21px;color:#2459c7;font-weight:700}.strategy-board .market{background:#edf4ff;font-weight:600}
    </style><div class="strategy-board"><h3>🎯 3A / 2A / 1A 重点榜</h3>
    <p>3A 最高分档 ≥80 · 2A 短中线 · 1A 超短线。每档各市场 Top3；同股同数据版本共用一个综合分；周期适用性、入场、止盈与止损分别列示。</p>
    <small>2026-10-09 中期统一分（企业基本面25 · 估值10 · 中期趋势20 · 中期动量10 · 量能10 · 成交承载10 · 风险约束15）；1A另设盘中时机门槛 · 筛选分不代表胜率或 GPT 复审分；入场状态单列。旧评级与日历原样保留。</small>''']
    from module_freshness import html as freshness_html, record as freshness_record
    out.append(freshness_html('strategy_board.json',doc,now=now))
    out.append('<p class="screen-clock">'+esc(read_clock)+'</p>')
    out.append('<p>🔄 交易日盘中云端每30分钟筛选 · 页面每60秒核对 · 免费规则计算，不消耗模型额度；实际完成时间以下方为准。</p>')
    source_check=doc.get('quote_refresh_status') or {}
    if source_check.get('status') not in (None,'complete'):
        out.append('<p>⏳ 报价采集未完整成功 · 最近尝试 '+esc(display(source_check.get('finished_at') or source_check.get('requested_at')))+'；逐股行情时间保留，重新筛选不等于报价已更新。</p>')
    try:
        if get_script_run_ctx(suppress_warning=True) and st.session_state.get('_v88_open_sync'):
            out.append('<p>'+esc(st.session_state['_v88_open_sync'])+'；筛选与报价时间仍以数据原始记录为准。</p>')
    except (NameError,ImportError):pass
    out.append('<p class="screen-clock">📈 报价截至（北京时间）：'+esc(' · '.join(m+' '+t for m,t in quote_clocks(doc).items()))+'</p>')
    if doc.get('refresh_state')=='source_unavailable_retained':
        out.append('<p>⏳ 最近核对 '+esc(display(doc.get('checked_at')))+'；源数据不足，保留原筛选时间。</p>')
    settings=[('3A','3A · 最高分优选','≥80分且有企业依据（公告原文或数据商财报）· 中期统一分1–6个月 · 价值投资 / 成长潜力 / 超跌反弹 / 优质量价机会'),('2A','2A · 短线 / 月度','2周–1个月'),('1A','1A · 超短线','3–5个交易日'),('watch','投资研究候选 · 未授3A','保留线索与原分数；不冒充最高评级')]
    for tier,title,period in settings:
        members=[r for r in rows if (bool(r.get('grade_pending')) if tier=='watch' else r['strategy_tier']==tier and not r.get('grade_pending'))]
        if tier=='watch' and not members:continue
        if code and not members:continue
        matched=sum(r['status']=='条件匹配' for r in members)
        retained=sum(not r.get('current_source') for r in members)
        out.append(f'<section class="lane" id="strategy-{tier}"><div class="head"><div><h3>{title}</h3><small>{period} · '+ ' · '.join(f'{m} {sum(r["market"]==m for r in members)}只' for m in flags)+f'</small></div><div><span class="count">{len(members)} 只</span><br><small>条件匹配 {matched} · 研究候选 {len(members)-matched-retained} · 待更新留存 {retained}</small></div></div>')
        out.append('<div class="screen-clock" style="padding:10px 20px;font-size:15px;color:#2459c7">'+esc(clock)+' · 💾 缓存写入 '+esc(freshness_record('strategy_board.json',doc,now=now)['cached'])+'（北京时间）</div>')
        if not members:
            out.append('<p style="padding:0 20px">本轮所需证据不足或未达到策略条件；不会用其他周期填补名额。</p>')
        else:
            out.append('<div style="overflow-x:auto"><table><thead><tr><th>市场 / 排名 / 个股 / 所属板块</th><th>评级 / 统一综合分</th><th>现价 / 行情时间</th><th>周期 / 推荐原因</th><th>入场区间 / 条件</th><th>止盈区间 / 离场</th><th>止损区间 / 失效</th></tr></thead><tbody>')
            for market,flag in flags.items():
                chosen=sorted([r for r in members if r['market']==market],key=lambda r:r['rank'])[:3]
                out.append(f'<tr class="market"><td colspan="7">{flag} {market} · {len(chosen)}/3只</td></tr>')
                for r in chosen:
                    href='?'+urlencode({'q':r['code'],'focus':'deep'})
                    s=r['strategy_score'];parts=''.join(f'<div>{esc(p["factor"])} · 权重 {p["weight"]}% · 贡献 {p["contribution"]:g}分</div>' for p in s['factors'])
                    periods=''.join(f'<div>{esc(p.get("period"))} · {esc(p.get("lane"))}</div>' for p in r.get('applicable_periods',[]))
                    band='–'.join(f'{p:g}' for p in r['entry_range']) if r.get('entry_range') else '待核结构'
                    missing=' / '.join(r.get('missing',[]))
                    profit = r.get('take_profit_range')
                    profit_label = '–'.join(f'{v:g}' for v in profit) if profit else '区间待核实'
                    net=r.get('net_target_return_pct');loss=r.get('stop_loss_pct')
                    returns_label='～'.join(f'{v:+.2f}%' for v in net) if net else '待核算'
                    loss_label='～'.join(f'{v:+.2f}%' for v in loss) if loss else '待核算'
                    stop_band = r.get('stop_range')
                    stop_label = '–'.join(f'{v:g}' for v in stop_band) if stop_band else '区间待核实'
                    profile_markup = ''
                    if r.get('industry_snapshot') and (r['industry_snapshot'].get('rank') or profiles is not None and not profiles.get('records')):
                        info=r['industry_snapshot'];rank=info.get('rank');session=info.get('source_session')
                        from exchange_sessions import latest_completed
                        old=info.get('historical') or session!=latest_completed(r['market'],now or datetime.now(timezone.utc)).isoformat()
                        rank_label=(f"最近行业市值排名 {rank}/{info.get('comparable')} · {session}（{'历史参考' if old else '最近完整交易日'}）" if rank else '行业排名：可比数据待补')
                        peers=''.join(f'<div>#{esc(peer["rank"])} <a href="?{esc(urlencode({"q":peer["code"],"focus":"deep"}))}">{esc(peer["name"])}</a></div>' for peer in info.get('peers',[])[:5])
                        profile_markup=f'<div>{esc(info.get("industry") or "所属板块待核")}<br><small>{esc(rank_label)}</small><details><summary>同业Top5 · 按市值</summary>{peers or "同业名单待补"}<small>以上述源日期为准；仅展示公开可比同业，市值排名不代表推荐评级。</small></details></div>'
                    if not profile_markup:profile_markup = profile_html(r['code'], profiles, now)
                    out.append(f'<tr><td><b>#{r["rank"]} <a href="{esc(href)}">{esc(r["name"])}</a></b><br><small>{esc(r["code"])}</small>{profile_markup}</td>'
                        f'<td><b>{"未授级" if r.get("grade_pending") else esc(r["strategy_tier"])}</b><br><span class="score">{s["value"]:g}</span> /100<br><small>证据覆盖 {s["coverage"]}%</small><br><small>{esc(s.get("label", "旧版周期分"))}</small><details><summary>权重与贡献</summary>{parts}<div>数据版本 {esc(s.get("snapshot_id"))} · 评分规则 {esc(s.get("revision"))}</div><div>共振 +{s["interaction"]} · 过热扣 {s["penalty"]}</div></details></td>'
                        f'<td><b>{r["last"]:g} {esc(r.get("currency") or {"A股":"CNY","港股":"HKD","美股":"USD"}.get(r["market"],""))}</b> · {r["change_pct"]:+.2f}%<br><small>{esc(display(r["quote_asof"]))}<br>北京时间</small></td>'
                        f'<td><b>{esc(r["lane"])}</b> · {esc(r["period"])}<br>{esc(r["reason"])}{source_tags(r,esc)}<details><summary>周期适用性 · 共用综合分</summary>{periods}<small>仅表示筛选适用；本行价带对应上方周期，其它周期须独立核验。</small></details><details><summary>🏢 企业依据与反证</summary>{esc(r["business_reason"])}<br>{esc(r.get("business_risk"))}</details></td>'
                        f'<td><b>{esc(band)}</b><br>◉ {esc(r["status"])}<br><small>{esc(missing or "结构条件匹配；核对最新价格")}</small></td>'
                        f'<td><b>{esc(profit_label)}</b><br><small>💰 到目标净空间：{esc(returns_label)}<br>首段目标参考：{esc(r.get("target"))}<br>净收益风险比：{esc(r.get("net_rr"))}<br>{esc(r.get("price_plan_method"))}<br>结构截至：{esc(r.get("price_plan_source_asof"))}<br>条件测算，非预期必得利润<br>往返费用假设0.5%</small><details><summary>区间依据与适用范围</summary>{esc(r.get("price_plan_scope") or "区间依据尚待补齐")}</details></td>'
                        f'<td><b>{esc(stop_label)}</b><br><small>止损情景：{esc(loss_label)}<br>失效触发参考：{esc(r.get("stop"))}</small><br>{esc(r["invalidation"])}<details><summary>模型审核状态</summary>{esc(r["model_review"])}</details></td></tr>')
            out.append('</tbody></table></div>')
        if tier=='3A':
            if doc.get('independent_validation'):
                out.append('<small style="display:block;padding:4px 20px;color:#64748b">'+esc(doc['independent_validation'].get('note'))+'</small>')
            out.append('<small style="display:block;padding:10px 20px">'+esc(doc.get('value_lane_status'))+'</small>')
            near=[r for r in doc.get('top_grade_watch',[]) if not code or r['code']==code]
            if near and not code:out.append(near_html(near,doc,esc,flags,display))
        out.append('</section>')
    out.append('</div>')
    return ''.join(out)


STRUCTURE_LABEL={'verified':'日线结构：正式核验','screening':'日线结构：筛选用日线（非正式核验）'}


def source_tags(r,esc):
    tags=[]
    if r.get('business_source'):tags.append('企业依据：'+r['business_source'])
    if r.get('structure_source'):tags.append(STRUCTURE_LABEL.get(r['structure_source'],r['structure_source']))
    f=r.get('fundamentals') or {}
    facts=[f'{k}{f[v]:+.1f}%' for k,v in (('营收同比','revenue_yoy'),('净利同比','profit_yoy')) if isinstance(f.get(v),(int,float))]
    if isinstance(f.get('roe_annual'),(int,float)):facts.append(f'年化ROE {f["roe_annual"]:.1f}%')
    if facts:tags.append('财报（数据商口径）'+' '.join(facts)+(' · 报告期 '+f['period_end'] if f.get('period_end') else ''))
    return '<br><small>'+esc(' ｜ '.join(tags))+'</small>' if tags else ''


def near_html(near,doc,esc,flags,display):
    counts=doc.get('pool_counts') or {}
    pool=' · '.join(f'{m} 评分{c.get("scored",0)} / 企业依据{c.get("business_supported",0)} / 有日线结构{c.get("with_structure",0)} / ≥70分{c.get("score_ge_70",0)}' for m,c in counts.items())
    out=['<div style="padding:4px 20px 14px"><h4 style="margin:8px 0">🔎 3A候补 · 未授3A，逐项列出差距</h4>',
         '<small>每市场最高分的非3A候选；差距逐项列示，补齐后下一轮自动重评。不冒充3A，也不代表买入信号。</small>',
         f'<p><small>候选池：{esc(pool or "待核")}</small></p>' if pool else '',
         '<div style="overflow-x:auto"><table style="min-width:1100px"><thead><tr><th>市场 / 个股</th><th>统一综合分</th><th>现价 / 行情时间</th><th>当前所在档 / 依据来源</th><th>距3A的差距</th></tr></thead><tbody>']
    for market,flag in flags.items():
        for r in sorted([r for r in near if r['market']==market],key=lambda r:r['rank']):
            s=r.get('strategy_score') or {}
            out.append(f'<tr><td><b>{flag} #{esc(r["rank"])} {esc(r["name"])}</b><br><small>{esc(r["code"])}</small></td>'
                f'<td><span class="score">{esc(s.get("value"))}</span> /100<br><small>证据覆盖 {esc(s.get("coverage"))}%</small></td>'
                f'<td><b>{esc(r.get("last"))}</b> · {r.get("change_pct",0):+.2f}%<br><small>{esc(display(r.get("quote_asof")))} 北京时间</small></td>'
                f'<td>{esc(r.get("strategy_tier") or r.get("source_strategy"))} · {esc(r.get("lane"))}{source_tags(r,esc)}</td>'
                f'<td>{"".join("<div>· "+esc(g)+"</div>" for g in r.get("gaps",[])) or "—"}</td></tr>')
    out.append('</tbody></table></div></div>')
    return ''.join(out)
