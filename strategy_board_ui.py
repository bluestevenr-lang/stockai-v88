"""Same strategy publication on desktop and cloud; no inference in the UI."""
import json
from datetime import datetime,timezone
from html import escape
from urllib.parse import urlencode
from v88_paths import core_root


def score_value_html(row):
    value=(row.get('strategy_score') or {}).get('value')
    label=escape(str(value if value is not None else '—'))
    if row.get('score_current_unknown'):
        return '<b>当前分待核</b><br><small>上次已核 '+label+' /100</small>'
    return '<span class="score">'+label+'</span> /100'


def html(doc=None,code=None,now=None,profiles=None):
    if doc is None:
        try:
            from module_freshness import read_snapshot
            doc=read_snapshot(core_root()/'data/strategy_board.json')
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
    raw_doc=doc; raw_rows={r['code']:r for r in doc.get('rows',[])}
    doc=view(doc,now)
    from muse_display import row_badge, panel as muse_panel, load as muse_load
    muse_cache = muse_load()
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
    <p>3A为最高分优选（≥80分且有企业依据），按各市场完整交易日日度评审，盘中不因短时排名波动换股；分“买入条件已满足”和“高价值可跟进 · 重点观察”；两组合计每市场最多3只，日度选入按分数排序、同业最多1只。当前分仍随最新行情更新，跌破80当日仅续列研究、不可买。2A为60–不足80分的短中线波段，1A为50–不足60分的超短线。空间不足仍保留3A重点观察；当前净空间5%与净收益风险比2只用于区分入场状态，不改变3A名单。研究优先级与买入时机分开；同股同快照共用一个分数，按实际计划标注周期，短期计划不延长为长期。</p>
    <small>中期统一分（企业基本面25 · 估值10 · 中期趋势20 · 中期动量10 · 量能10 · 成交承载10 · 风险约束15）· 形态验证：样本≥300、平均≥+0.05R、盈亏比≥1.5、前后两段均为正 · 筛选分不代表胜率或 GPT 复审分。旧评级与日历原样保留。</small>''']
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
    settings=[('3A','3A · 最高分优选','日度入选≥80分＋企业依据 · 当前分随行情更新 · 买入条件已满足 / 高价值可跟进 · 重点观察 · 两组合计各市场Top3'),
              ('2A','2A · 短中线波段','60≤分数<80 · 2周–1个月（最长20个交易日）· 经回测验证的形态'),
              ('1A','1A · 超短线','50≤分数<60 · 3–5个交易日 · 经回测验证的形态'),
              ('watch','投资研究候选 · 未授3A','保留线索与原分数；不冒充最高评级')]
    if doc.get('grade_policy')!='2026-10-11-quality-followup-v8':
        out.append('<p>历史版本结果：下列名单保留原分数与原分类，等待新规则重算；不代表已按新口径筛选。</p>')

    for tier,title,period in settings:
        members=[r for r in rows if (bool(r.get('grade_pending')) if tier=='watch' else r['strategy_tier']==tier and not r.get('grade_pending'))]
        if tier=='watch' and not members:continue
        if code and not members:continue
        matched=sum(r['status']=='条件匹配' for r in members)
        retained=sum(not r.get('current_source') or bool(r.get('research_retained')) for r in members)
        out.append(f'<section class="lane" id="strategy-{tier}"><div class="head"><div><h3>{title}</h3><small>{period} · '+ ' · '.join(f'{m} {sum(r["market"]==m for r in members)}只' for m in flags)+f'</small></div><div><span class="count">{len(members)} 只</span><br><small>条件匹配 {matched} · {'高价值可跟进 · 重点观察' if tier=='3A' else '研究候选'} {len(members)-matched-retained} · 待更新留存 {retained}</small></div></div>')
        out.append('<div class="screen-clock" style="padding:10px 20px;font-size:15px;color:#2459c7">'+esc(clock)+' · 💾 缓存写入 '+esc(freshness_record('strategy_board.json',doc,now=now)['cached'])+'（北京时间）</div>')
        status=(doc.get('tier_status') or {}).get(tier) or {}
        if tier in ('2A','1A') and status and not any(v.get('enabled_cells') for v in status.values()):
            out.append(f'<p style="padding:0 20px"><b>{tier}暂停：</b>A股、港股、美股的{tier}候选形态历史回测扣费后均无稳定正期望，不填充名额；每月重跑回测，出现合格形态后自动启用。</p>')
        elif not members and tier=='watch':
            out.append('<p style="padding:0 20px">本轮所需证据不足或未达到策略条件；不会用其他周期填补名额。</p>')
        else:
            out.append('<div style="overflow-x:auto"><table><thead><tr><th>市场 / 排名 / 个股 / 所属板块</th><th>评级 / 统一综合分</th><th>现价 / 行情时间</th><th>周期 / 推荐原因</th><th>入场区间 / 条件</th><th>止盈区间 / 离场</th><th>止损区间 / 失效</th></tr></thead><tbody>')
            for market,flag in flags.items():
                chosen=sorted([r for r in members if r['market']==market],key=lambda r:r['rank'])[:3]
                if code and not chosen:continue
                src=' · 云端行情' if (doc.get('market_sources') or {}).get(market)=='cloud' else ''
                out.append(f'<tr class="market"><td colspan="7">{flag} {market} · {len(chosen)}/3只{src}</td></tr>')
                if not chosen and tier!='watch':
                    out.append('<tr><td colspan="7"><small>'+esc(market_note(doc,tier,market))+'</small></td></tr>')
                for r in chosen:
                    href='?'+urlencode({'q':r['code'],'focus':'deep'})
                    s=r['strategy_score'];parts=''.join(f'<div>{esc(p["factor"])} · 权重 {p["weight"]}% · 贡献 {p["contribution"]:g}分</div>' for p in s['factors'])
                    periods=''.join(f'<div>{esc(p.get("period"))} · {esc(p.get("lane"))}</div>' for p in r.get('applicable_periods',[]))
                    band='–'.join(f'{p:g}' for p in r['entry_range']) if r.get('entry_range') else '待核结构'
                    missing=' / '.join(r.get('missing',[]))
                    o=r.get('opportunity') or {}
                    geometry=(f'<b>当前首段净空间 {o["net_space_pct"]:.2f}% · 当前净收益风险比 {o["current_net_rr"]:.2f}</b><br><small>首段结构观察位 {o["first_objective"]:g}；不是最终止盈承诺</small><br>' if o.get('current_net_rr') is not None else '')
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
                    roster=r.get('research_roster') or {}
                    roster_note=('<br><small>日度研究 · '+esc(roster.get('review_session'))+' · 入选 '+esc(roster.get('selection_score'))+'分 · '+('当前分待核' if roster.get('current_score') is None else '当前 '+esc(roster.get('current_score'))+'分')+'</small>') if roster else ''
                    out.append(f'<tr><td><b>#{r["rank"]} <a href="{esc(href)}">{esc(r["name"])}</a></b><br><small>{esc(r["code"])}</small>{profile_markup}{roster_note}{row_badge(raw_rows.get(r['code'],r), '3A', muse_cache, now, (raw_doc.get('market_source_generated_at') or {}).get(r['market']) or raw_doc.get('generated_at'), display_row=r) if tier=='3A' else ''}</td>'
                        f'<td><b>{"未授级" if r.get("grade_pending") else esc(r["strategy_tier"])}</b>{('<br><b>'+esc(r.get('selection_category'))+'</b>') if tier=='3A' and r.get('selection_category') else ''}<br>{score_value_html(r)}<br><small>评分字段覆盖 {s["coverage"]}%（非质量通过率）</small><br><small>{esc(s.get("label", "旧版周期分"))}</small><details><summary>权重与贡献</summary>{parts}<div>数据版本 {esc(s.get("snapshot_id"))} · 评分规则 {esc(s.get("revision"))}</div><div>共振 +{s["interaction"]} · 过热扣 {s["penalty"]}</div></details></td>'
                        f'<td><b>{r["last"]:g} {esc(r.get("currency") or {"A股":"CNY","港股":"HKD","美股":"USD"}.get(r["market"],""))}</b> · {r["change_pct"]:+.2f}%<br><small>{esc(display(r["quote_asof"]))}<br>北京时间</small></td>'
                        f'<td>'+('<b>原研究留存 · 本轮未重算</b><br><small>原评估 '+esc(display(r.get('last_evaluated_at')))+'</small><br>' if r.get('research_retained') else '')+f'<b>{esc(r["lane"])}</b> · {esc(r["period"])}'+(f' · 年度{esc(r["annual_label"])}' if r.get('annual_label') else '')+f'<br>{esc(r["reason"])}{source_tags(r,esc)}<details><summary>周期适用性 · 共用综合分</summary>{periods}<small>仅表示筛选适用；本行价带对应上方周期，其它周期须独立核验。</small></details><details><summary>🏢 企业依据与反证</summary>{esc(r["business_reason"])}<br>{esc(r.get("business_risk"))}</details></td>'
                        f'<td><b>{esc(band)}</b><br><b style="color:{"#0f7b45" if r.get("buyable") else "#a66316"}">{esc(r.get("readiness_label") or r["status"])}</b><br><small>{esc(r.get("readiness_detail") or "")}</small>'+(f'<br><b style="color:#0f7b45">✅ {esc(TIMING_TEXT.get(r.get("buy_timing"),""))}</b>' if tier in ('3A','2A','1A') and r['status']=='条件匹配' and not r.get('readiness_detail') else '')+f'<br><small>{esc(missing or "结构条件匹配；核对最新价格")}</small></td>'
                        f'<td>{geometry}<b>{esc(profit_label)}</b><br><small>💰 原计划情景净空间：{esc(returns_label)}<br>首段目标参考：{esc(r.get("target"))}<br>{"历史同型盈亏比（回测）" if r.get("rr_basis") else "净收益风险比"}：{esc(r.get("net_rr"))}<br>{("离场：收盘跌破60日线 "+esc(r.get("trend_exit_ma60"))+"<br>") if r.get("trend_exit_ma60") and tier=='3A' else ("离场："+esc(r.get("tier_exit_text"))+(" "+esc(r.get("tier_exit_ma")) if r.get("tier_exit_ma") else "")+"<br>") if r.get("tier_exit_text") else ""}{esc(r.get("price_plan_method"))}<br>结构截至：{esc(r.get("price_plan_source_asof"))}<br>条件测算，非预期必得利润<br>往返费用假设0.5%</small><details><summary>区间依据与适用范围</summary>{esc(r.get("price_plan_scope") or "区间依据尚待补齐")}</details></td>'
                        f'<td><b>{esc(stop_label)}</b><br><small>止损情景：{esc(loss_label)}<br>失效触发参考：{esc(r.get("stop"))}</small><br>{esc(r["invalidation"])}<details><summary>模型审核状态</summary>{esc(r["model_review"])}</details></td></tr>')
            out.append('</tbody></table></div>')
        if tier=='3A':
            if doc.get('independent_validation'):
                out.append('<small style="display:block;padding:4px 20px;color:#64748b">'+esc(doc['independent_validation'].get('note'))+'</small>')
            out.append('<small style="display:block;padding:10px 20px">'+esc(doc.get('value_lane_status'))+'</small>')
            near=[r for r in doc.get('top_grade_watch',[]) if not code or r['code']==code]
            if near and not code:out.append(near_html(near,doc,esc,flags,display))
        if tier in ('2A','1A') and not code:
            waiting=(doc.get('tier_watch') or {}).get(tier) or []
            if waiting:out.append(tier_watch_html(tier,waiting,esc,flags,display))
        out.append('</section>')
    if not code:out.append(muse_panel(muse_cache, now))
    out.append('</div>')
    return ''.join(out)


from strategy_board_state import TIMING_TEXT
STRUCTURE_LABEL={'verified':'日线结构：正式核验','screening':'日线结构：筛选用日线（非正式核验）'}


def source_tags(r,esc):
    tags=[]
    if r.get('business_source'):tags.append('企业依据：'+r['business_source'])
    if r.get('structure_source'):tags.append(STRUCTURE_LABEL.get(r['structure_source'],r['structure_source']))
    f=r.get('fundamentals') or {}
    from earnings_quality import summary as earnings_summary
    tags.append('利润质量：'+earnings_summary(f))
    facts=[f'{k}{f[v]:+.1f}%' for k,v in (('营收同比','revenue_yoy'),('净利同比','profit_yoy')) if isinstance(f.get(v),(int,float))]
    if isinstance(f.get('roe_annual'),(int,float)):facts.append(f'年化ROE {f["roe_annual"]:.1f}%')
    if facts:tags.append('财报（数据商口径）'+' '.join(facts)+(' · 报告期 '+f['period_end'] if f.get('period_end') else ''))
    return '<br><small>'+esc(' ｜ '.join(tags))+'</small>' if tags else ''


def market_note(doc,tier,market):
    """Why a market has no buyable name this round (never filled from other tiers)."""
    m=((doc.get('markets') or {}).get(market) or {})
    if not ((m.get('scan_counts') or {}).get('usable_quotes') or 0):
        return f'{market}本轮无最新交易日报价（本机与云端均未取得），待下次刷新；不以旧报价入选。'
    if tier=='3A':
        c=(doc.get('pool_counts') or {}).get(market) or {}
        return f'{market}本轮未选出≥80分且有企业依据的3A：评分{c.get("scored",0)} / 企业依据{c.get("business_supported",0)} / 年度上行{c.get("annual_up",0)} / ≥80分{c.get("score_ge_80",0)} / 可买{c.get("buyable",0)}；见下方待买点。'
    st=((doc.get('tier_status') or {}).get(tier) or {}).get(market) or {}
    cells='、'.join(f'{x["setup"]}（{"上行" if x["regime"]=="up" else "震荡"}）' for x in st.get('enabled_cells',[])) or '无'
    return f'{market}本轮未选出符合{tier}评分与形态条件的股票：形态匹配{st.get("matched",0)} / 计划启用{st.get("plan_enabled",0)} / 可买{st.get("buyable",0)}；本市场经验证形态：{cells}。'


def tier_watch_html(tier,rows,esc,flags,display):
    out=[f'<div style="padding:4px 20px 14px"><h4 style="margin:8px 0">🔎 {tier}待买点 · 形态与回测已通过，暂不可买</h4>',
         '<small>差在价格离开信号收盘区间、报价过时、公告待核或综合分不足；条件满足后下一轮自动进入。</small>',
         '<div style="overflow-x:auto"><table style="min-width:1100px"><thead><tr><th>市场 / 个股</th><th>统一综合分</th><th>现价 / 行情时间</th><th>形态 / 年度结构 / 离场</th><th>入场区间 / 差距</th></tr></thead><tbody>']
    for market,flag in flags.items():
        for r in sorted([r for r in rows if r['market']==market],key=lambda r:r.get('rank',9)):
            s=r.get('strategy_score') or {}
            band='–'.join(f'{p:g}' for p in r['entry_range']) if r.get('entry_range') else '待核'
            out.append(f'<tr><td><b>{flag} #{esc(r.get("rank"))} {esc(r["name"])}</b><br><small>{esc(r["code"])}</small></td>'
                f'<td><span class="score">{esc(s.get("value"))}</span> /100</td>'
                f'<td><b>{esc(r.get("last"))} {esc({"A股":"CNY","港股":"HKD","美股":"USD"}.get(r["market"],""))}</b><br><small>{esc(display(r.get("quote_asof")))} 北京时间</small></td>'
                f'<td><b>{esc(r.get("tier_setup"))}</b> · 年度{esc(r.get("annual_label") or "待核")}<br><small>{esc(r.get("tier_exit_text"))} · 历史同型盈亏比 {esc(r.get("net_rr"))}（回测）</small></td>'
                f'<td><b>{esc(band)}</b><br><small>{esc((r.get("trigger") or {}).get("text"))}</small></td></tr>')
    out.append('</tbody></table></div></div>')
    return ''.join(out)


def near_html(near,doc,esc,flags,display):
    from industry_diversity import candidate_top3
    near=candidate_top3(near)
    counts=doc.get('pool_counts') or {}
    pool=' · '.join(f'{m} 评分{c.get("scored",0)} / 企业依据{c.get("business_supported",0)} / 有日线结构{c.get("with_structure",0)} / ≥80分{c.get("score_ge_80",0)}' for m,c in counts.items())
    out=['<div style="padding:4px 20px 14px"><h4 style="margin:8px 0">🔎 3A名额外候补 / 接近3A · 列出差距</h4>',
         '<small>名额外候补保留原分数与缺口；已入3A研究栏不重复列出。每市场最多3只，不因买点已满足而跳过更高分证券。研究价值与入场资格分别判断。</small>',
         f'<p><small>候选池：{esc(pool or "待核")}</small></p>' if pool else '',
         '<div style="overflow-x:auto"><table style="min-width:1100px"><thead><tr><th>市场 / 个股</th><th>统一综合分</th><th>现价 / 行情时间</th><th>状态 / 年度结构 / 依据来源</th><th>买点触发条件 / 差距</th></tr></thead><tbody>']
    for market,flag in flags.items():
        for r in sorted([r for r in near if r['market']==market],key=lambda r:r['rank']):
            s=r.get('strategy_score') or {}
            out.append(f'<tr><td><b>{flag} #{esc(r["rank"])} {esc(r["name"])}</b><br><small>{esc(r["code"])}</small></td>'
                f'<td><span class="score">{esc(s.get("value"))}</span> /100<br><small>评分字段覆盖 {esc(s.get("coverage"))}%（非质量通过率）</small></td>'
                f'<td><b>{esc(r.get("last"))} {esc(r.get("currency") or {"A股":"CNY","港股":"HKD","美股":"USD"}.get(r["market"],""))}</b> · {r.get("change_pct",0):+.2f}%<br><small>{esc(display(r.get("quote_asof")))} 北京时间</small></td>'
                f'<td><b>{esc(r.get("watch_kind") or "候补")}</b> · 年度{esc(r.get("annual_label") or "待核")} · {esc(r.get("lane"))}{source_tags(r,esc)}</td>'
                f'<td>{("<b>"+esc((r.get("trigger") or {}).get("text"))+"</b>") if (r.get("trigger") or {}).get("text") else ""}{"".join("<div>· "+esc(g)+"</div>" for g in r.get("gaps",[])) or ("" if r.get("trigger") else "—")}</td></tr>')
    out.append('</tbody></table></div></div>')
    return ''.join(out)
