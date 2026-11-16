"""The same bounded, dated Astra research plan on desktop and cloud."""
from html import escape
from datetime import datetime, timezone
from astra_cycle import live_note
from urllib.parse import urlencode
from astra_cycle_state import view
from strategy_board_ui import directive_html, audit_html, execution_ready, execution_status, in_entry_window


def text(v): return escape(str(v if v is not None else '待核'))


def clock(v):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    try:return datetime.fromisoformat(v).astimezone(ZoneInfo('Asia/Shanghai')).strftime('%m-%d %H:%M 北京时间')
    except (ValueError,TypeError):return '待核'


def band(v):
    return '～'.join(f'{x:.2f}' for x in v) if isinstance(v, (list, tuple)) and len(v) == 2 else '待核'


def html(doc, now=None, *, journal=None):
    now=now or datetime.now(timezone.utc)
    from strategy_crosscheck_view import html as crosscheck_html
    from muse_display import row_badge, load as muse_load
    from screening_clock import market_screened_at
    muse_cache = muse_load()
    raw_rows={r['code']:r for r in doc.get('rows',[])}
    from action_directive import publishable
    from shadow_screen import ASTRA_POSITION_NOTE, FADE_NOTE
    checked = view(doc, now)
    d = publishable(checked, 'Astra', now=now)
    d={**d,'rows':[r for r in d.get('rows',[]) if in_entry_window(r,'Astra')]}
    if not d.get('version'): return ''
    out = ['<section id="v88-astra-monthly" style="border:1px solid #c7d2fe;border-left:5px solid #6366f1;border-radius:12px;padding:18px;margin:18px 0;background:#f8faff">',
           '<div style="display:flex;justify-content:space-between;flex-wrap:wrap;gap:10px"><b style="font-size:23px;color:#3730a3">🎯 Astra · 定期短线计划</b>',
           f'<b style="font-size:20px">✓ 策略条件匹配 {sum(execution_ready(r,"Astra") for r in d.get("rows", []))} 只　·　⏳研究 / 等待 {sum(not execution_ready(r,"Astra") for r in d.get("rows", []))} 只　·　🌱启动跟踪 {sum(r.get("buy_window")=="启动跟踪·等买点" for r in d.get("rows", []))} 只</b></div>',
           f'<p style="color:#0f7b45;font-weight:600">{text(ASTRA_POSITION_NOTE)}</p>',
           f'<p style="color:#b42318;font-weight:600">{text(FADE_NOTE)}</p>',
           f'<p style="color:#334155">当前大盘反弹标记：{text("、".join(m for m, v in (d.get("market_bottom_rebound") or {}).items() if v) or "未确认")}。个股分别列示策略条件匹配、待开市核价、报价待更新或等待触发；原计划保留。</p>',
           f'<p>📅 {text(d.get("cycle_id"))} 轮　｜　🔄 下轮 {text(d.get("next_recalculation"))}　｜　每月1、10、20日开新轮 · 每天复筛补空位</p>',
           '<p style="color:#64748b">中港优先 · 共最多3只 · 美股最多1只，仅在中港合格名单留有空位时列为参考。重算按自然日；新增入场限于本月；原合同的价格、期限与跟踪记录保留。</p>',
           money_html(d, now=now),
           f'<p style="font-size:12px;color:#64748b">本轮实际建立 {text(clock(d.get("created_at")))} · 最近计算 {text(clock(d.get("generated_at")))} · 原行情日期逐股保留</p>']
    from module_freshness import html as freshness_html
    out.append(freshness_html('astra_cycle.json',doc,now=now))
    out.append(audit_html(d, 'Astra', now, text))
    if not d.get('rows'): out.append('<p><b>本轮Astra 0只</b>；本轮暂无同时满足月内期限与近期入场条件的计划。</p>')
    published = {(r.get('market'), r.get('code'), r.get('plan_id')) for r in d.get('rows', [])}
    retained = [r for r in checked.get('rows', []) if (r.get('market'), r.get('code'), r.get('plan_id')) not in published]
    if retained:
        out.append('<details><summary>原计划留档 · 当前不可执行 '+str(len(retained))+' 只</summary>')
        for r in retained:
            out.append('<p>'+text(r.get('name'))+' · '+text(r.get('code'))+' · '+text(r.get('status'))+
                       ' · 现价 '+text(r.get('last'))+' '+text(r.get('currency'))+' · 报价 '+text(clock(r.get('quote_asof')))+
                       ' · 研究入场 '+band(r.get('entry_range'))+' · 止盈 '+band(r.get('take_profit_range'))+
                       ' · 止损 '+band(r.get('stop_range'))+' · 原截止 '+text((r.get('window') or {}).get('deadline'))+'</p>')
        out.append('</details>')
    if d.get('plan_stability_version'):out.append('<p style="font-size:12px;color:#64748b">本轮计划固定 · 盘中更新报价、分数与风险；原入场、目标、止损和期限不随刷新移动。</p>')
    out.append(backtest_html(d.get('backtest')))
    for r in d.get('rows', []):
        info = r.get('industry_snapshot') or {}; window = r.get('window') or {}
        flags = {'A股': '🇨🇳', '港股': '🇭🇰', '美股': '🇺🇸'}
        currency = {'A股':'人民币', '港股':'港元', '美股':'美元'}
        rank = f'行业市值 #{info["rank"]}/{info.get("comparable", "—")} · {info.get("source_session", "待核")}' if info.get('rank') else '行业排名待核'
        link = '?'+urlencode({'focus':'deep','q':r['code']})
        out += ['<article style="border:1px solid #dbe3f3;border-radius:10px;background:white;padding:14px;margin:12px 0">',
                f'<div style="font-size:18px"><b>{flags.get(r["market"], "")} #{r["rank"]} <a href="{escape(link,quote=True)}">{text(r["name"])} · {text(r["code"])}</a></b>　<b style="color:#4338ca">{text((r.get("strategy_score") or {}).get("value"))}分 · {text((r.get("strategy_score") or {}).get("label", "旧版周期分"))}</b>　<b style="color:{"#0f7b45" if execution_ready(r,"Astra") else "#a66316"}">{text(execution_status(r))}</b></div>',
                (f'<div style="margin:8px 0;padding:8px 12px;border-radius:8px;background:#fff1f0;border:1px solid #ffa39e;color:#a8071a;font-weight:600">⚠️ {text(live_note(r))}</div>' if (r.get("live_gate") or {}).get("hold") is False else ''),
                (f'<div style="margin:8px 0;padding:8px 12px;border-radius:8px;background:#f6ffed;border:1px solid #b7eb8f;color:#0f7b45;font-weight:600">🚀 {text((r.get("live_gate") or {}).get("note"))}</div>' if (r.get("live_gate") or {}).get("trend_trial") else ''),
                (f'<div style="margin:6px 0;color:#065f46"><b>{text((r.get("astra_position") or {}).get("state"))}</b> · {text((r.get("astra_position") or {}).get("reason"))}</div>' if r.get("astra_position") else ''),
                directive_html(r, text, 'Astra', now=now),
                row_badge(raw_rows.get(r['code'],r), 'Astra', muse_cache, now, market_screened_at(doc,r['market']), display_row=r),
                f'<p><b>现价 {text(r.get("last"))} {text(r.get("currency") or {"A股":"CNY","港股":"HKD","美股":"USD"}.get(r["market"]))}</b> · 报价 {text(clock(r.get("quote_asof")))}</p>',
                f'<p style="font-size:13px;color:#64748b">{text(info.get("industry"))} · {text(rank)} · {text(r.get("role"))}</p>',
                '<div style="display:flex;gap:22px;flex-wrap:wrap;font-size:16px">'
                f'<span>📥 研究入场 <b>{band(r.get("entry_range"))}</b></span>'
                f'<span>🎯 止盈 <b>{band(r.get("take_profit_range"))}</b></span>'
                f'<span>🛑 止损 <b>{band(r.get("stop_range"))}</b></span></div>',
                f'<p>💰 原计划情景净空间 {band(r.get("net_target_return_pct"))}%　｜　止损情景 {band(r.get("stop_loss_pct"))}%　｜　原计划净盈亏比 {text(r.get("net_rr"))}</p>',
                (f'<p>当前净空间（至原计划目标） {text(r.get("plan_current_net_space_pct"))}% · 当前净盈亏比 {text(r.get("plan_current_net_rr"))} · 原计划版本 {text(r.get("plan_revision"))}</p>' if r.get('plan_id') else ''),
                contribution_html(r),
                crosscheck_html(r,now=now),
                f'<p>🏢 {text(r.get("business_reason"))}</p>',
                f'<p>⚠️ {text(r.get("business_risk") or "未提供新增反证；仍需核验最新公告")}</p>',
                f'<p>⏱ 原计划截止 {text(window.get("deadline"))} · 新增入场限于 {text(r.get("monthly_entry_deadline") or window.get("deadline"))} · 剩余 {text(window.get("remaining_sessions"))} 交易日；到期复盘，未入场不延长原计划。</p>',
                (f'<p style="font-size:13px;color:#64748b">待补资料与复核事项：{text("；".join(r.get("missing",[])))}</p>' if r.get("buy_window") and r.get("missing") else f'<p style="font-size:13px">下一步：{text("；".join(r.get("missing",[])) or "核对实际账户资金、费用与交易单位后再决定")}</p>'),
                f'<details><summary>价格依据 · 复合评分 · 同业Top5</summary><p>{text(r.get("price_plan_scope"))}</p>',
                f'<p>报价 {text(r.get("last"))} {currency.get(r["market"], "")} · {text(r.get("quote_asof"))}；结构日 {text(r.get("price_plan_source_asof"))}；往返费用假设0.5%，非收益承诺。</p>']
        for f in (r.get('strategy_score') or {}).get('factors', []):
            out.append(f'<span style="margin-right:14px">{text(f["factor"])} · 权重{text(f["weight"])}% · 分值{text(f.get("value"))}</span>')
        for peer in info.get('peers', [])[:5]:
            out.append(f'<p>#{text(peer.get("rank"))} {text(peer.get("name"))} · {text(peer.get("code"))}</p>')
        out.append('</details></article>')
    if d.get('exited_rows'):
        out.append('<details><summary>本轮退出与原计划留档</summary>')
        for old in d['exited_rows']:out.append('<p>'+text(old.get('name'))+' · '+text(old.get('plan_exit_reason'))+' · 原止损 '+band(old.get('stop_range'))+'</p>')
        out.append('</details>')
    from astra_calendar_view import html as calendar_html
    out.append(calendar_html(d,now,journal=journal))
    out += ['<details><summary>📚 历轮记录 · 首次研究区间永久保留</summary>']
    for key, h in sorted(d.get('history', {}).items(), reverse=True):
        out.append(f'<p><b>{text(key)}轮</b> · 实际建立 {text(h.get("created_at"))} · {text(h.get("updates"))}次变化</p>')
        for first in list(h.get('first', {}).values())[:15]:
            r = first['row']
            out.append(f'<p>{text(first["at"])} · {text(r.get("name"))} · 首次入场 {band(r.get("entry_range"))} / 止盈 {band(r.get("take_profit_range"))} / 止损 {band(r.get("stop_range"))}</p>')
    out += ['</details><p style="font-size:12px;color:#64748b">规则筛选分不是GPT审核分或胜率。研究区间不改变已有持仓合同；月度实际收益与风险账本独立核算，未对账不计零。GitHub更新不依赖模型额度。</p></section>']
    try:
        import streamlit as st
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        if get_script_run_ctx(suppress_warning=True) and st.query_params.get('focus') == 'astra':
            from muse_display import panel
            out.append(panel(muse_cache, now))
    except ImportError:pass
    return ''.join(out)


def money_html(d, now=None):
    ms = d.get('month_status') or {}; pol = d.get('selection_policy') or {}
    target = ms.get('monthly_target_usd', 200); risk = ms.get('risk_per_trade_usd'); cap = ms.get('monthly_risk_cap_usd')
    realized = '待对账（未对账不计零）' if ms.get('realized_net_usd') is None else f'{ms["realized_net_usd"]:g} 美元'
    out = ['<div style="background:#eef2ff;border-radius:10px;padding:10px 14px;margin:8px 0">',
           f'<p style="margin:2px 0">🎯 月净收益目标 <b>${text(target)}</b>（目标，非收益承诺） · 本月已实现 <b>{text(realized)}</b> · 台账 {text(ms.get("ledger_state"))}</p>']
    from datetime import datetime, timezone
    from astra_cycle import month_sessions_left, MARKETS, SELECTION_POLICY
    now = now or datetime.now(timezone.utc)
    left = {m: month_sessions_left(m, now) for m in MARKETS}
    rows = d.get('rows') or []
    scenario = round(sum(sorted(((r.get('target_contribution') or {}).get('profit_at_target_usd') or 0 for r in rows if r.get('market') != '美股'),reverse=True)[:ms.get('remaining_entries') if ms.get('remaining_entries') is not None else 2]), 1)
    if left:
        known = ms.get('realized_net_usd') is not None
        gap = max(0.0, float(target or 200) - float(ms['realized_net_usd'])) if known else None
        verdict = (f'本月目标 ${target:g}，实际缺口待对账；上列仅为目标价情景' if gap is None else '✅ 最多两次的理论情景覆盖目标；实际金额待核' if scenario >= gap and rows else
                   '⚠️ 当前无可推票，本月目标暂无路径；每日复筛补位，有合格启动股即补上' if not rows else
                   f'⚠️ 在推票止盈情景仅 ${scenario:g}，距目标缺口 ${gap:g} 还差 ${gap - scenario:g}；每日复筛补位')
        out.append(f'<p style="margin:2px 0">📆 本月剩余交易日：A股 {text(left.get("A股"))} · 港股 {text(left.get("港股"))} · 美股 {text(left.get("美股"))}'
                   f'　｜　在推 {len(rows)} 只，最多两次理论情景 <b>${scenario:g}</b>　｜　{text(verdict)}</p>'
                   '<p style="margin:2px 0;font-size:13px;color:#7c2d12">节奏规则：每天全市场复筛补空位，不等1/10/20日；新入选的目标必须在本月底前可达（ATR14×√本月剩余交易日）；波动尺度只作筛选，不是兑现概率。</p>')
    if risk:
        out.append(f'<p style="margin:2px 0">🧮 单笔风险 ${text(risk)} · 月风险上限 ${text(cap)} · 每月最多{text(ms.get("max_new_entries"))}次新开仓 · 止盈收益 ≈ 净盈亏比 × ${text(risk)}</p>')
    if pol.get('version'):
        out.append(f'<p style="margin:2px 0;font-size:13px">选股规则 {text(SELECTION_POLICY["version"])}：{text(SELECTION_POLICY["position_rule"])} · 净空间≥{text(pol.get("min_net_target_pct"))}% · 规则分≥{text(pol.get("min_score"))} · {text(SELECTION_POLICY["time_rule"])}；美股本轮门槛 {text(pol.get("us_floor_this_run"))}分</p>')
    if ms.get('note'): out.append(f'<p style="margin:2px 0;font-size:12px;color:#64748b">{text(ms["note"])}</p>')
    out.append('</div>')
    return ''.join(out)


def contribution_html(r):
    c = r.get('target_contribution') or {}
    gate = '✅ 通过盈利门槛' if r.get('profit_gate') else '⏳ 未过盈利门槛：' + '；'.join(r.get('profit_gaps') or [])
    setup = f' · 形态 {text(r.get("setup"))}' if r.get('setup') else ''
    if not c.get('profit_at_target_usd'): return f'<p>{text(gate)}{setup}</p>'
    return (f'<p>{text(gate)}{setup}　｜　按单笔风险${text(c.get("risk_per_trade_usd"))}计：止盈情景约 <b>+${text(c["profit_at_target_usd"])}</b>'
            f'（月目标 {text(c.get("target_coverage_pct"))}%） · 每股风险 {text(c.get("per_share_risk"))}</p>'
            f'<p style="font-size:12px;color:#64748b">{text(c.get("sizing_rule"))}；{text(c.get("scope"))}</p>')


def backtest_html(bt):
    if not (bt or {}).get('by_market'): return ''
    names = {'all': '合计', 'A股': '🇨🇳 A股', '港股': '🇭🇰 港股', '美股': '🇺🇸 美股'}
    rows = ''.join(f'<tr><td>{names.get(m, text(m))}</td><td>{text(v.get("trades"))}</td><td>{text(v.get("target_hit_pct"))}%</td><td>{text(v.get("stop_pct"))}%</td>'
                   f'<td>{text(v.get("time_exit_pct"))}%</td><td>{text(v.get("mean_r"))}R</td><td>{text(v.get("mean_pct"))}%</td></tr>'
                   for m, v in sorted(bt['by_market'].items(), key=lambda kv: kv[0] != 'all') if v.get('trades'))
    return ('<details><summary>📊 规则历史回顾（技术面，非收益保证）</summary>'
            f'<p style="font-size:13px">{text(bt.get("scope"))} · 生成 {text(clock(bt.get("generated_at")))} · {text(bt.get("symbols"))}只</p>'
            '<div style="overflow-x:auto"><table style="border-collapse:collapse;min-width:640px"><thead><tr><th>市场</th><th>模拟次数</th><th>触及止盈</th><th>止损</th><th>到期离场</th><th>平均每笔</th><th>平均涨跌</th></tr></thead>'
            f'<tbody>{rows}</tbody></table></div></details>')


def watch_html(watch):
    if not watch: return ''
    out = ['<details open><summary>🔎 Astra候补 · 未过盈利门槛，逐项列出差距</summary>']
    for r in watch:
        s = r.get('strategy_score') or {}
        out.append(f'<p>{text(r.get("market"))} {text(r.get("name"))} · {text(r.get("code"))} · 现价 {text(r.get("last"))} · {text(clock(r.get("quote_asof")))} · {text(s.get("value"))}分'
                   f' · 入场 {band(r.get("entry_range"))} / 止盈 {band(r.get("take_profit_range"))} / 止损 {band(r.get("stop_range"))}'
                   f'<br><small>差距：{text("；".join(r.get("profit_gaps") or r.get("missing") or []) or "待核")}</small></p>')
    out.append('</details>')
    return ''.join(out)
