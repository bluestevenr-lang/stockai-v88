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


DIRECTIVE_COLOR={'立即买入':('#0f7b45','#f6ffed','#b7eb8f'),'限价买入':('#0f7b45','#f6ffed','#b7eb8f'),
                 '挂单买入':('#ad4e00','#fff7e6','#ffd591'),'财报后买入':('#531dab','#f9f0ff','#d3adf7'),
                 '不买':('#595959','#fafafa','#d9d9d9')}


def execution_metadata(row):
    return (row.get('directive') or {}).get('execution_readiness') or row.get('execution_readiness') or {}


def in_entry_window(row,module='3A'):
    """Read the canonical gate; the display never manufactures eligibility."""
    if (row.get('entry_window_qualification') or {}).get('eligible') is not True:
        return False
    return module=='Astra' or row.get('strategy_tier')!='3A' or (row.get('three_a_qualification') or {}).get('qualified') is True


def execution_ready(row,module='3A'):
    """A nonempty research/wait window never means executable now."""
    if not in_entry_window(row,module) or not row.get('buyable') or not execution_metadata(row).get('executable_now'):
        return False
    return module=='Astra' or row.get('strategy_tier')!='3A' or (row.get('three_a_qualification') or {}).get('qualified') is True


def public_rating(row):
    if row.get('grade_pending'):
        return '未授级'
    if row.get('strategy_tier')=='3A':
        q=row.get('three_a_qualification') or {}
        return q.get('public_label') or ('3A' if q.get('qualified') is True else '研究候选（3A未达标）')
    return row.get('strategy_tier') or '研究候选'


def execution_status(row):
    """Show the current operational requirement separately from qualification."""
    meta=execution_metadata(row)
    if meta.get('label'):
        return meta['label']
    action=(row.get('directive') or {}).get('action')
    if action in ('不买','财报后买入'):
        return '暂不入场' if action=='不买' else '财报后复核'
    if action=='挂单买入' or (row.get('directive') or {}).get('order')=='条件单':
        return '等待触发'
    # Older snapshots can carry a buy window without a current session check.
    return '当前执行状态待核'


def directive_html(r,esc=escape,module='3A',now=None):
    """Render the published plan without recalculating prices or admission."""
    d=r.get('directive') or {}
    if not d.get('text'):
        return ''
    shown_text=d['text']
    legacy=not d.get('execution_readiness') or not in_entry_window(r,module)
    if legacy:
        shown_text=str(shown_text).replace('立即买入','原入场计划').replace('今日可买','当前执行状态待核').replace('限价买入','原限价计划')
    ready=execution_ready(r,module)
    fg,bg,bd=(DIRECTIVE_COLOR['限价买入'] if ready else
              DIRECTIVE_COLOR['不买'] if d.get('action')=='不买' else DIRECTIVE_COLOR['挂单买入'])
    extra=[('原记录依据' if legacy else '依据','；'.join(d.get('why') or [])),('待补资料与复核事项','；'.join(d.get('checks') or [])),('离场',d.get('exit') or '')]
    tail=''.join(f'<br><small style="color:#595959">{k}：{esc(v)}</small>' for k,v in extra if v)
    w=execution_status(r)
    from action_directive import directive_label
    label='原计划待核' if legacy else directive_label({**r,'directive':d})
    badge=(f'<span style="display:inline-block;margin-bottom:4px;padding:1px 8px;border-radius:10px;color:#fff;'
           f'background:{"#0f7b45" if ready else "#d46b08"}">{"✓" if ready else "⏳"} {esc(w)}</span><br>') if w else ''
    return (f'<div style="margin:6px 0;padding:6px 10px;border-radius:8px;background:{bg};border:1px solid {bd}">'
            f'{badge}<b style="color:{fg}">📌 {"" if label=="操作指令" else esc(label)+"｜"}{esc(shown_text)}</b>{tail}</div>')


def audit_html(doc,module='3A',now=None,esc=escape):
    """Display the completed audit; rendering never mutates the plan."""
    s=doc.get('directive_summary') or {}
    if not (s.get('audit') or {}).get('text'):
        return '<div style="color:#64748b;font-size:13px">本轮规则一致性复核结果待更新。</div>'
    a=s.get('audit') or {}
    ok=a.get('consistent')
    return (f'<div style="margin:6px 0;padding:6px 10px;border-radius:8px;font-size:13px;'
            f'background:{"#f6ffed" if ok else "#fff7e6"};border:1px solid {"#b7eb8f" if ok else "#ffd591"};'
            f'color:{"#0f7b45" if ok else "#ad4e00"}"><b>{esc(a.get("text") or "")}</b>'
            '<br><small style="color:#595959">云端按同一份规则代码计算；每次运行先复算Copilot判例库，再独立复核每条指令'
            '（价格/止损/目标顺序、追高、财报、硬伤、逆风半仓、匹配周期的净盈亏比门槛），不一致即降为不买。这里只验证规则一致性，不代表胜率认证或Muse复核通过。无模型调用。</small></div>')


def html(doc=None,code=None,now=None,profiles=None):
    if doc is None:
        try:
            from module_freshness import read_snapshot
            doc=read_snapshot(core_root()/'data/strategy_board.json')
        except (OSError,ValueError):doc={}
    if doc.get('version')!='strategy-taxonomy-2026-09-28-v1':return ''
    now=now or datetime.now(timezone.utc)
    from screening_clock import display,screened_at,quote_clocks,market_screened_at
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
    from action_directive import publishable
    doc=publishable(view(doc,now),'3A',now=now)
    from strategy_crosscheck_view import html as crosscheck_html
    from muse_display import row_badge, panel as muse_panel, load as muse_load
    muse_cache = muse_load()
    esc=lambda v:escape(str(v if v is not None else '—'),quote=True)
    flags={'A股':'🇨🇳','港股':'🇭🇰','美股':'🇺🇸'}
    rows=[r for r in doc.get('rows',[]) if in_entry_window(r) and (not code or r['code']==code)]
    out=['''<style>.strategy-board .lane{margin:14px 0;border:1px solid #d8e4f3;border-radius:14px;overflow:hidden;background:#fff}
    .strategy-board .head{padding:18px 20px;background:#eef5ff;display:flex;justify-content:space-between;align-items:center;gap:12px}
    .strategy-board h3{font-size:22px!important;margin:0 0 5px}.strategy-board small{font-size:13px!important;color:#64748b}
    .strategy-board .count{font-size:28px;color:#2459c7;font-weight:700;white-space:nowrap}
    .strategy-board table{border-collapse:collapse;width:100%;min-width:1450px}.strategy-board th{background:#f5f8fc;text-align:left}
    .strategy-board table.board-main{table-layout:fixed;min-width:1320px}
    .strategy-board td.px{white-space:nowrap;overflow-wrap:normal}
    .strategy-board td.px small{white-space:normal}
    .strategy-board .lead{display:block;max-height:7.2em;overflow:hidden}
    .strategy-board tbody tr>td{border-right:1px solid #eef2f8}
    .strategy-board tbody tr:hover>td{background:#fbfdff}
    .strategy-board td,.strategy-board th{padding:12px 14px;border-bottom:1px solid #e5ebf4;vertical-align:top;font-size:14px!important;line-height:1.6;overflow-wrap:anywhere}
    .strategy-board details{font-size:13px}.strategy-board .score{font-size:21px;color:#2459c7;font-weight:700}.strategy-board .market{background:#edf4ff;font-weight:600}
    </style><div class="strategy-board"><h3>🎯 3A / 2A / 1A 重点榜</h3>
    <p><b>3A：中长期走势向上，今明两个交易日内入场；2A：中期，本周内入场；1A：超短线，今天入场。</b>主推荐只列趋势、证据与近期入场窗口同时符合的个股；观察和远期买点不补位。90%净盈利胜率是持续优化目标，尚未验证，不作为当前硬门槛或成功承诺。综合分与历史规则组胜率均不等于本次获胜概率；当前执行状态另核交易时段、最新报价和触发条件。</p>
    <small>中期统一分（企业基本面25 · 估值10 · 中期趋势20 · 中期动量10 · 量能10 · 成交承载10 · 风险约束15）· 形态验证：样本≥300、平均≥+0.05R、盈亏比≥1.5、前后两段均为正 · 筛选分不代表胜率或 GPT 复审分。旧评级与日历原样保留。</small>''']
    from module_freshness import html as freshness_html, record as freshness_record
    out.append(freshness_html('strategy_board.json',doc,now=now))
    out.append(audit_html(doc,'3A',now,esc))
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
    settings=[('3A','3A · 中长期向上','今明两个交易日内入场 · 各市场最多3只'),
              ('2A','2A · 中期策略','本周内入场 · 条件未触发时明确等待'),
              ('1A','1A · 超短线策略','今天入场 · 已收市不列今日计划')]
    if doc.get('grade_policy')!='2026-10-11-quality-followup-v8':
        out.append('<p>历史版本结果：下列名单保留原分数与原分类，等待新规则重算；不代表已按新口径筛选。</p>')

    for tier,title,period in settings:
        members=[r for r in rows if (bool(r.get('grade_pending')) if tier=='watch' else r['strategy_tier']==tier and not r.get('grade_pending'))]
        if tier=='watch' and not members:continue
        if code and not members:continue
        matched=sum(r['status']=='条件匹配' for r in members)
        retained=sum(not r.get('current_source') or bool(r.get('research_retained')) for r in members)
        out.append(f'<section class="lane" id="strategy-{tier}"><div class="head"><div><h3>{title}</h3><small>{period} · '+ ' · '.join(f'{m} {sum(r["market"]==m for r in members)}只' for m in flags)+f'</small></div><div><span class="count">{len(members)} 只</span><br><small>✓ 当前执行条件满足 {sum(execution_ready(r) for r in members)} · ⏳窗口内待触发 {sum(not execution_ready(r) for r in members)}</small></div></div>')
        out.append('<div class="screen-clock" style="padding:10px 20px;font-size:15px;color:#2459c7">'+esc(clock)+' · 💾 缓存写入 '+esc(freshness_record('strategy_board.json',doc,now=now)['cached'])+'（北京时间）</div>')
        status=(doc.get('tier_status') or {}).get(tier) or {}
        if tier in ('2A','1A') and status and not any(v.get('enabled_cells') for v in status.values()):
            out.append(f'<p style="padding:0 20px"><b>{tier}暂停：</b>A股、港股、美股的{tier}候选形态历史回测扣费后均无稳定正期望，不填充名额；每月重跑回测，出现合格形态后自动启用。</p>')
        elif not members and tier=='watch':
            out.append('<p style="padding:0 20px">本轮所需证据不足或未达到策略条件；不会用其他周期填补名额。</p>')
        else:
            out.append('<div style="overflow-x:auto"><table class="board-main"><colgroup><col style="width:14%"><col style="width:11%"><col style="width:9%"><col style="width:23%"><col style="width:20%"><col style="width:11.5%"><col style="width:11.5%"></colgroup><thead><tr><th>市场 / 排名 / 个股 / 所属板块</th><th>评级 / 统一综合分</th><th>现价 / 行情时间</th><th>周期 / 推荐原因</th><th>入场区间 / 条件</th><th>止盈区间 / 离场</th><th>止损区间 / 失效</th></tr></thead><tbody>')
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
                    # 计划带缺失不等于系统不知道价位：年度震荡结构下中期趋势延续
                    # 计划不启用（这个拒绝是对的），但入场门仍算出了结构止损、结构
                    # 目标与解锁价，指令层已据此发出可执行指令。只读计划带会让页面
                    # 在指令层说"条件单 ≤ 32.328"的同一轮显示"待核结构"。
                    from action_directive import plan_bands
                    cells=plan_bands(r)
                    note=lambda c:(f'<br><small style="color:#8c8c8c">{esc(c["note"])}</small>'
                                   if c['source']=='directive' and c.get('note') else '')
                    band=cells['entry']['text'] or '待核结构'
                    band_note=note(cells['entry'])
                    gaps=[g for g in (r.get('missing') or []) if g]
                    missing=' / '.join(gaps)
                    # 待补条件动辄十几条，直接平铺会把一行撑满整屏；
                    # 首条留在面上，其余折叠，信息一条不减。
                    missing_html=('<small>结构条件匹配；核对最新价格</small>' if not gaps else
                                  '<small>'+esc(gaps[0])+'</small>' if len(gaps)==1 else
                                  '<small>'+esc(gaps[0])+'</small><details><summary>其余待补条件 '
                                  +str(len(gaps)-1)+' 项</summary><small>'
                                  +'<br>'.join(esc(g) for g in gaps[1:])+'</small></details>')
                    o=r.get('opportunity') or {}
                    _ob=o.get('first_obstacle_pct')
                    _obt=(f'；第一上方结构位 {o["nearest_resistance"]:g}（+{_ob:.2f}%，可能在此停顿）'
                          if isinstance(_ob,(int,float)) and o.get('nearest_resistance') else '')
                    geometry=(f'<b>当前净空间 {o["net_space_pct"]:.2f}% · 当前净收益风险比 {o["current_net_rr"]:.2f}</b>'
                              f'<br><small>收益与风险同口径：至计划目标 {o["first_objective"]:g} 对计划止损 {o["stop"]:g}，已扣往返费用{_obt}；不是最终止盈承诺</small><br>'
                              if o.get('current_net_rr') is not None else '')
                    profit_label = cells['profit']['text'] or '区间待核实'
                    profit_note = note(cells['profit'])
                    net=r.get('net_target_return_pct');loss=r.get('stop_loss_pct')
                    returns_label='～'.join(f'{v:+.2f}%' for v in net) if net else '待核算'
                    loss_label='～'.join(f'{v:+.2f}%' for v in loss) if loss else '待核算'
                    stop_label = cells['stop']['text'] or '区间待核实'
                    stop_note = note(cells['stop'])
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
                    out.append(f'<tr><td><b>#{r["rank"]} <a href="{esc(href)}">{esc(r["name"])}</a></b><br><small>{esc(r["code"])}</small>{profile_markup}{roster_note}{row_badge(raw_rows.get(r['code'],r), '3A', muse_cache, now, market_screened_at(raw_doc,r['market']), display_row=r) if tier=='3A' else ''}</td>'
                        f'<td><b>{"未授级" if r.get("grade_pending") else esc(public_rating(r))}</b>{('<br><small>'+esc((r.get('entry_window_qualification') or {}).get('label'))+'</small>') if (r.get('entry_window_qualification') or {}).get('label') else ''}{('<br><b style="color:#0958d9">🪑 '+esc(r.get('seat_fill'))+'</b><br><small>'+esc(r.get('seat_fill_reason') or '同业已有更高分入选，为避免空席按统一分补入本席；80分线与企业依据未放宽')+'</small>') if tier=='3A' and r.get('seat_fill') else ''}<br>{score_value_html(r)}<details><summary>权重与贡献</summary><div><small>评分字段覆盖 {s["coverage"]}%（非质量通过率）· {esc(s.get("label", "旧版周期分"))}</small></div>{parts}<div>数据版本 {esc(s.get("snapshot_id"))} · 评分规则 {esc(s.get("revision"))}</div><div>共振 +{s["interaction"]} · 过热扣 {s["penalty"]}</div></details></td>'
                        f'<td class="px"><b>{r["last"]:g} {esc(r.get("currency") or {"A股":"CNY","港股":"HKD","美股":"USD"}.get(r["market"],""))}</b> · {r["change_pct"]:+.2f}%<br><small>{esc(display(r["quote_asof"]))}<br>北京时间</small></td>'
                        f'<td>'+('<b>原研究留存 · 本轮未重算</b><br><small>原评估 '+esc(display(r.get('last_evaluated_at')))+'</small><br>' if r.get('research_retained') else '')+f'<b>{esc(r["lane"])}</b> · {esc(r["period"])}'+(f' · 年度{esc(r["annual_label"])}' if r.get('annual_label') else '')+f'<br>{esc(r["reason"])}{source_tags(r,esc)}<details><summary>周期适用性 · 共用综合分</summary>{periods}<small>仅表示筛选适用；本行价带对应上方周期，其它周期须独立核验。</small></details><details><summary>🏢 企业依据与反证</summary>{esc(r["business_reason"])}<br>{esc(r.get("business_risk"))}</details>{crosscheck_html(r,now=now) if tier=="3A" else ""}</td>'
                        f'<td><b>{esc(band)}</b>{band_note}<br><b style="color:{"#0f7b45" if execution_ready(r) else "#a66316"}">{esc(execution_status(r))}</b><br><small>{esc("" if r.get("buy_window") else (r.get("readiness_detail") or ""))}</small>'+directive_html(r,esc,now=now)+(f'<br><b style="color:#0f7b45">✅ {esc(TIMING_TEXT.get(r.get("buy_timing"),""))}</b>' if execution_ready(r) and not r.get('readiness_detail') else '')+(f'<br><b style="color:#a8071a">⏳ 入场质量门·{esc((r.get("entry_gate") or {}).get("entry_state") or "暂不可买")}：{esc("；".join((r.get("live_gate") or {}).get("reasons") or []))}</b>' if (r.get("live_gate") or {}).get("demoted") else '')+(f'<br><b style="color:#a8071a">🎯 解禁价 {esc((r.get("entry_gate") or {}).get("unlock_price"))}｜{esc((r.get("entry_gate") or {}).get("unlock_note"))}</b>' if (r.get("entry_gate") or {}).get("unlock_price") else '')+(f'<br><b style="color:#a8071a">📅 财报事件窗·{esc((r.get("earnings_risk") or {}).get("state"))}：{esc("；".join((r.get("earnings_risk") or {}).get("reasons") or []))}</b>' if (r.get("earnings_risk") or {}).get("state") in ("blackout","stale","window") else '')+(f'<br><b style="color:#0f7b45">🚀 {esc((r.get("live_gate") or {}).get("note"))}</b>' if (r.get("live_gate") or {}).get("rescued") else '')+(f'<br><small>{esc((r.get("entry_gate") or {}).get("note"))}</small>' if (r.get("entry_gate") or {}).get("note") else '')+'<br>'+missing_html+'</td>'
                        f'<td>{geometry}<b>{esc(profit_label)}</b>{profit_note}<br><small>💰 原计划情景净空间：{esc(returns_label)}<br>首段目标参考：{esc(r.get("target"))}<br>{"历史同型盈亏比（回测）" if r.get("rr_basis") else "净收益风险比"}：{esc(r.get("net_rr"))}<br>{("离场：收盘跌破60日线 "+esc(r.get("trend_exit_ma60"))) if r.get("trend_exit_ma60") and tier=='3A' else ("离场："+esc(r.get("tier_exit_text"))+(" "+esc(r.get("tier_exit_ma")) if r.get("tier_exit_ma") else "")) if r.get("tier_exit_text") else ""}</small><details><summary>测算口径与适用范围</summary><small>{esc(r.get("price_plan_method"))}<br>结构截至：{esc(r.get("price_plan_source_asof"))}<br>条件测算，非预期必得利润；往返费用假设0.5%</small><br>{esc(r.get("price_plan_scope") or "区间依据尚待补齐")}</details></td>'
                        f'<td><b>{esc(stop_label)}</b>{stop_note}<br><small>止损情景：{esc(loss_label)}<br>失效触发参考：{esc(r.get("stop"))}</small><br>{esc(r["invalidation"])}<details><summary>模型审核状态</summary>{esc(r["model_review"])}</details></td></tr>')
            out.append('</tbody></table></div>')
        if tier=='3A':
            if doc.get('independent_validation'):
                out.append('<small style="display:block;padding:4px 20px;color:#64748b">'+esc(doc['independent_validation'].get('note'))+'</small>')
            out.append('<small style="display:block;padding:10px 20px">'+esc(doc.get('value_lane_status'))+'</small>')
            out.append(entry_gate_html(doc,esc,flags,code))
        out.append('</section>')
    # Observation/early/learning pools stay in source data, outside the main recommendation list.
    if not code:
        try:
            try:
                import pick_review
            except ModuleNotFoundError as exc:
                if exc.name != "pick_review":
                    raise
                pick_review = None  # Private desktop review is optional in the public runtime.
            if pick_review is not None:
                out.append(pick_review.html(pick_review.read(),esc=esc))
        except Exception:  # noqa: BLE001 — 复盘失败不影响看板
            import logging;logging.exception('[V88] 推荐复盘渲染失败')
        out.append(muse_panel(muse_cache, now))
    out.append('</div>')
    return ''.join(out)


def learning_html(doc,esc,flags):
    """启用形态（按市场宽度查证据表）＋学习型候选：正式可买不足3只时补足，轻仓跟踪复盘，不是买入级。"""
    ladder=doc.get('ladder') or {}
    book=ladder.get('playbook') or {};learn=ladder.get('learning') or {}
    if not book and not learn:return ''
    out=['<div style="padding:4px 20px 14px"><h4 style="margin:8px 0">🧭 今日启用形态 · 📚 学习型候选</h4>']
    for market,flag in flags.items():
        if book.get(market):out.append(f'<div><small>{flag} {esc(book[market].get("text"))}</small></div>')
    rows=[r for m in flags for r in (learn.get(m) or [])]
    if rows:
        out.append('<small>正式可买不足3只的市场用学习型候选补足：好股（企业依据＋基本面≥60、无利润质量/公告/财报反证）＋当前最接近证据表形态者。'
                   '<b>轻仓（≤正常仓位1/3）、按止损执行、15个交易日复盘</b>；期望见每只的「回测参考」（形态命中按周期证据表格子，无形态者期望≈0），用于学习与验证，不是买入级推荐。</small>'
                   '<div style="overflow-x:auto"><table style="min-width:1000px;margin-top:6px"><thead><tr><th>市场 / 个股</th><th>现价</th><th>形态 / 相对强度</th>'
                   '<th>止损（距离）</th><th>目标（2R）</th><th>回测参考</th><th>为何不是正式级</th></tr></thead><tbody>')
        for r in rows:
            rs=r.get('rs20_pct')
            out.append(f'<tr><td><b>{flags.get(r["market"],"")} {esc(r.get("name"))}</b><br><small>{esc(r.get("code"))} · {esc(r.get("industry") or "")} · 基本面{esc(r.get("business_score"))}</small></td>'
                f'<td><b>{esc(r.get("last"))} {esc(r.get("currency"))}</b></td>'
                f'<td>{esc(r.get("pattern"))}'+(f'<br><small>20日涨幅分位 {rs*100:.0f}%</small>' if isinstance(rs,(int,float)) else '')+'</td>'
                f'<td>{esc(r.get("stop"))}<br><small>-{esc(r.get("stop_pct"))}%</small></td>'
                f'<td>{esc(r.get("target"))}<br><small>持有≤{esc(r.get("hold_sessions"))}日 · {esc(r.get("sizing"))}</small></td>'
                f'<td><small>{esc(r.get("expectation"))}</small></td><td><small>{esc(r.get("why_not_formal"))}</small></td></tr>')
        out.append('</tbody></table></div>')
    out.append('</div>')
    return ''.join(out)


def entry_gate_html(doc,esc,flags,code=None):
    """等回调/不追高（含解禁价）与昨日对比：同一入场质量门，名单不清空。"""
    out=[]
    ch=doc.get('board_changes') or {}
    gone=[x for x in ch.get('out') or [] if not code or x.get('code')==code]
    fresh=[x for x in ch.get('in') or [] if not code or x.get('code')==code]
    if gone or fresh:
        rows=''.join('<tr><td>'+flags.get(x.get('market'),'')+' '+esc(x.get('market'))+'</td>'
                     '<td>📤 离场</td><td><b>'+esc(x.get('name'))+'</b><br><small>'+esc(x.get('code'))+'</small></td>'
                     '<td>'+esc(x.get('reason'))+'</td></tr>' for x in gone)
        rows+=''.join('<tr><td>'+flags.get(x.get('market'),'')+' '+esc(x.get('market'))+'</td>'
                      '<td>📥 新进</td><td><b>'+esc(x.get('name'))+'</b><br><small>'+esc(x.get('code'))+'</small></td>'
                      '<td>'+esc(x.get('reason'))+'</td></tr>' for x in fresh)
        out.append(f'<details open style="margin:8px 20px"><summary><b>🔁 与上一版3A的变动（离场 {len(gone)} · 新进 {len(fresh)}，逐只写明理由）</b></summary>'
                   '<small>昨日在榜、今日不在的每一只都必须给出原因；新进的必须给出入选理由。均非买卖指令。</small>'
                   '<div style="overflow-x:auto"><table style="min-width:760px"><thead><tr><th>市场</th><th>变动</th><th>个股</th><th>理由</th></tr></thead><tbody>'
                   +rows+'</tbody></table></div></details>')
    blocked=[r for r in doc.get('entry_blocked') or [] if not code or r['code']==code]
    if blocked:
        def cell(r):
            g=r.get('entry_gate') or {}
            space=g.get('net_space_pct');rr=g.get('net_rr')
            return (esc('—' if space is None else f'{space:.2f}%')+' / '+esc('—' if rr is None else f'{rr:.2f}')
                    +'<br><small>'+esc(g.get('rr_source') or '')+'</small>')
        rows=''.join('<tr><td>'+flags.get(r['market'],'')+' '+esc(r['market'])+'</td><td><b>'+esc(r['name'])+'</b><br><small>'+esc(r['code'])+'</small></td>'
                     f'<td>{(r.get("strategy_score") or {}).get("value",0):g}</td><td>{esc(r.get("last"))}</td>'
                     '<td>'+esc((r.get('entry_gate') or {}).get('entry_state') or '')+'</td>'
                     '<td>'+cell(r)+'</td>'
                     '<td><b>'+esc('—' if (r.get('entry_gate') or {}).get('unlock_price') is None else f"{r['entry_gate']['unlock_price']:g}")+'</b></td>'
                     '<td>'+esc((r.get('entry_gate') or {}).get('unlock_note') or '；'.join((r.get('entry_gate') or {}).get('reasons') or []))+'</td></tr>' for r in blocked)
        out.append(f'<details style="margin:8px 20px"><summary><b>⏳ 等回调 / 不追高（{len(blocked)}只 · 仍在跟踪，给出解禁价）</b></summary>'
                   '<small>同一入场质量门：追高熔断、净空间&lt;5%/净收益风险比&lt;2（两者均以「计划目标 对 计划止损」同一口径计量），或参考入场价已站在最近上方结构位之上时，只收回买入权，不清出名单。解禁价＝回调到该价即同时满足净空间5%与净收益风险比2（已计0.5%费用）。不是卖出指令。</small>'
                   '<div style="overflow-x:auto"><table style="min-width:980px"><thead><tr><th>市场</th><th>个股</th><th>统一分</th><th>现价</th><th>入场状态</th><th>净空间 / 净收益风险比</th><th>解禁价</th><th>说明</th></tr></thead><tbody>'
                   +rows+'</tbody></table></div></details>')
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
    quarter=f.get('original_quarter_growth') or {}
    if isinstance(quarter.get('profit_yoy'),(int,float)):
        tags.append(f"公告单季归母净利同比 {quarter['profit_yoy']:+.1f}%")
    if facts:tags.append('财报（'+(f.get('growth_period') or '数据商期间待核')+'）'+' '.join(facts)+(' · 报告期 '+f['period_end'] if f.get('period_end') else ''))
    return '<br><small>'+esc(' ｜ '.join(tags))+'</small>' if tags else ''


def market_note(doc,tier,market):
    """Why a market has no buyable name this round (never filled from other tiers)."""
    m=((doc.get('markets') or {}).get(market) or {})
    if not ((m.get('scan_counts') or {}).get('usable_quotes') or 0):
        return f'{market}本轮无最新交易日报价（本机与云端均未取得），待下次刷新；不以旧报价入选。'
    return f'{market}本轮无符合{tier}趋势与近期入场窗口的个股。'


def tier_watch_html(tier,rows,esc,flags,display):
    out=[f'<div style="padding:4px 20px 14px"><h4 style="margin:8px 0">🔎 {tier}待买点 · 形态与回测已通过，暂不可买</h4>',
         '<small>差在价格离开信号收盘区间、报价过时、公告待核、综合分不足，或被与3A同一道入场风险门拦下（追高熔断／财报事件窗／大盘逆风）；条件满足后下一轮自动进入。</small>',
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
    out=['<div style="padding:4px 20px 14px"><h4 style="margin:8px 0">🔎 中期研究候补 · 原筛选差距</h4>',
         '<small>候补保留原分数与缺口；已入中期研究栏不重复列出。每市场最多3只，沿用统一分排序；评分不能替代趋势、证据与近期入场窗口核验。</small>',
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
