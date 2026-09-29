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
    <small>2026-09-28 新策略制 · 筛选分不代表胜率或 GPT 复审分；入场状态单列。旧评级与日历原样保留。</small>''']
    out.append('<p class="screen-clock">'+esc(read_clock)+'</p>')
    try:
        if get_script_run_ctx(suppress_warning=True) and st.session_state.get('_v88_open_sync'):
            out.append('<p>'+esc(st.session_state['_v88_open_sync'])+'；筛选与报价时间仍以数据原始记录为准。</p>')
    except (NameError,ImportError):pass
    out.append('<p class="screen-clock">📈 报价截至（北京时间）：'+esc(' · '.join(m+' '+t for m,t in quote_clocks(doc).items()))+'</p>')
    if doc.get('refresh_state')=='source_unavailable_retained':
        out.append('<p>⏳ 最近核对 '+esc(display(doc.get('checked_at')))+'；源数据不足，保留原筛选时间。</p>')
    settings=[('3A','3A · 最高分优选','≥80分且有企业依据 · 价值投资 / 成长潜力 / 超跌反弹 / 优质量价机会'),('2A','2A · 短线 / 月度','2周–1个月'),('1A','1A · 超短线','3–5个交易日'),('watch','投资研究候选 · 未授3A','保留线索与原分数；不冒充最高评级')]
    for tier,title,period in settings:
        members=[r for r in rows if (bool(r.get('grade_pending')) if tier=='watch' else r['strategy_tier']==tier and not r.get('grade_pending'))]
        if tier=='watch' and not members:continue
        if code and not members:continue
        matched=sum(r['status']=='条件匹配' for r in members)
        retained=sum(not r.get('current_source') for r in members)
        out.append(f'<section class="lane"><div class="head"><div><h3>{title}</h3><small>{period} · '+ ' · '.join(f'{m} {sum(r["market"]==m for r in members)}只' for m in flags)+f'</small></div><div><span class="count">{len(members)} 只</span><br><small>条件匹配 {matched} · 研究候选 {len(members)-matched-retained} · 待更新留存 {retained}</small></div></div>')
        out.append('<div class="screen-clock" style="padding:10px 20px;font-size:15px;color:#2459c7">'+esc(clock)+'</div>')
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
                    profile_markup = profile_html(r['code'], profiles, now)
                    if r.get('industry_snapshot') and (r['industry_snapshot'].get('rank') or profiles is not None and not profiles.get('records')):
                        info=r['industry_snapshot'];rank=info.get('rank');session=info.get('source_session')
                        from exchange_sessions import latest_completed
                        old=info.get('historical') or session!=latest_completed(r['market'],now or datetime.now(timezone.utc)).isoformat()
                        rank_label=(f"最近行业市值排名 {rank}/{info.get('comparable')} · {session}（{'历史参考' if old else '最近完整交易日'}）" if rank else '行业排名：可比数据待补')
                        peers=''.join(f'<div>#{esc(peer["rank"])} <a href="?{esc(urlencode({"q":peer["code"],"focus":"deep"}))}">{esc(peer["name"])}</a></div>' for peer in info.get('peers',[])[:5])
                        profile_markup=f'<div>{esc(info.get("industry") or "所属板块待核")}<br><small>{esc(rank_label)}</small><details><summary>同业Top5 · 按市值</summary>{peers or "同业名单待补"}<small>以上述源日期为准；仅展示公开可比同业，市值排名不代表推荐评级。</small></details></div>'
                    out.append(f'<tr><td><b>#{r["rank"]} <a href="{esc(href)}">{esc(r["name"])}</a></b><br><small>{esc(r["code"])}</small>{profile_markup}</td>'
                        f'<td><b>{"未授级" if r.get("grade_pending") else esc(r["strategy_tier"])}</b><br><span class="score">{s["value"]:g}</span> /100<br><small>证据覆盖 {s["coverage"]}%</small><br><small>{esc(s.get("label", "旧版周期分"))}</small><details><summary>权重与贡献</summary>{parts}<div>数据版本 {esc(s.get("snapshot_id"))} · 评分规则 {esc(s.get("revision"))}</div><div>共振 +{s["interaction"]} · 过热扣 {s["penalty"]}</div></details></td>'
                        f'<td><b>{r["last"]:g}</b> · {r["change_pct"]:+.2f}%<br><small>{esc(r["quote_asof"][5:16])}<br>源时区</small></td>'
                        f'<td><b>{esc(r["lane"])}</b> · {esc(r["period"])}<br>{esc(r["reason"])}<details><summary>周期适用性 · 共用综合分</summary>{periods}<small>仅表示筛选适用；本行价带对应上方周期，其它周期须独立核验。</small></details><details><summary>🏢 企业依据与反证</summary>{esc(r["business_reason"])}<br>{esc(r.get("business_risk"))}</details></td>'
                        f'<td><b>{esc(band)}</b><br>◉ {esc(r["status"])}<br><small>{esc(missing or "结构条件匹配；核对最新价格")}</small></td>'
                        f'<td><b>{esc(profit_label)}</b><br><small>💰 到目标净空间：{esc(returns_label)}<br>首段目标参考：{esc(r.get("target"))}<br>净收益风险比：{esc(r.get("net_rr"))}<br>{esc(r.get("price_plan_method"))}<br>结构截至：{esc(r.get("price_plan_source_asof"))}<br>条件测算，非预期必得利润<br>往返费用假设0.5%</small><details><summary>区间依据与适用范围</summary>{esc(r.get("price_plan_scope") or "区间依据尚待补齐")}</details></td>'
                        f'<td><b>{esc(stop_label)}</b><br><small>止损情景：{esc(loss_label)}<br>失效触发参考：{esc(r.get("stop"))}</small><br>{esc(r["invalidation"])}<details><summary>模型审核状态</summary>{esc(r["model_review"])}</details></td></tr>')
            out.append('</tbody></table></div>')
        if tier=='3A':out.append('<small style="display:block;padding:10px 20px">'+esc(doc.get('value_lane_status'))+'</small>')
        out.append('</section>')
    out.append('</div>')
    return ''.join(out)
