"""The same bounded, dated Astra research plan on desktop and cloud."""
from html import escape
from urllib.parse import urlencode
from astra_cycle_state import view


def text(v): return escape(str(v if v is not None else '待核'))


def clock(v):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    try:return datetime.fromisoformat(v).astimezone(ZoneInfo('Asia/Shanghai')).strftime('%m-%d %H:%M 北京时间')
    except (ValueError,TypeError):return '待核'


def band(v):
    return '～'.join(f'{x:.2f}' for x in v) if isinstance(v, (list, tuple)) and len(v) == 2 else '待核'


def html(doc, now=None):
    d = view(doc, now)
    if not d.get('version'): return ''
    out = ['<section id="v88-astra-monthly" style="border:1px solid #c7d2fe;border-left:5px solid #6366f1;border-radius:12px;padding:18px;margin:18px 0;background:#f8faff">',
           '<div style="display:flex;justify-content:space-between;flex-wrap:wrap;gap:10px"><b style="font-size:23px;color:#3730a3">🎯 Astra · 定期短线计划</b>',
           f'<b style="font-size:20px">研究 {len(d.get("rows", []))} 只　·　条件匹配 {d.get("rule_matched_count", 0)} 只</b></div>',
           f'<p>📅 {text(d.get("cycle_id"))} 轮　｜　🔄 下轮 {text(d.get("next_recalculation"))}　｜　每月1、10、20日重算 · 休市也更新</p>',
           '<p style="color:#64748b">中港优先 · 共最多3只 · 美股最多1只且为当轮美股前5%（≥80分），仅参考。重算按自然日；目标跟踪仍按原15个交易日计算。</p>',
           money_html(d),
           f'<p style="font-size:12px;color:#64748b">本轮实际建立 {text(clock(d.get("created_at")))} · 最近计算 {text(clock(d.get("generated_at")))} · 原行情日期逐股保留</p>']
    from module_freshness import html as freshness_html
    out.append(freshness_html('astra_cycle.json',doc,now=now))
    if not d.get('rows'): out.append('<p>'+text(d.get('status'))+'</p>')
    out.append(backtest_html(d.get('backtest')))
    for r in d.get('rows', []):
        info = r.get('industry_snapshot') or {}; window = r.get('window') or {}
        flags = {'A股': '🇨🇳', '港股': '🇭🇰', '美股': '🇺🇸'}
        currency = {'A股':'人民币', '港股':'港元', '美股':'美元'}
        rank = f'行业市值 #{info["rank"]}/{info.get("comparable", "—")} · {info.get("source_session", "待核")}' if info.get('rank') else '行业排名待核'
        link = '?'+urlencode({'focus':'deep','q':r['code']})
        out += ['<article style="border:1px solid #dbe3f3;border-radius:10px;background:white;padding:14px;margin:12px 0">',
                f'<div style="font-size:18px"><b>{flags.get(r["market"], "")} #{r["rank"]} <a href="{escape(link,quote=True)}">{text(r["name"])} · {text(r["code"])}</a></b>　<b style="color:#4338ca">{text((r.get("strategy_score") or {}).get("value"))}分 · {text((r.get("strategy_score") or {}).get("label", "旧版周期分"))}</b>　{text(r["status"])}</div>',
                f'<p style="font-size:13px;color:#64748b">{text(info.get("industry"))} · {text(rank)} · {text(r.get("role"))}</p>',
                '<div style="display:flex;gap:22px;flex-wrap:wrap;font-size:16px">'
                f'<span>📥 研究入场 <b>{band(r.get("entry_range"))}</b></span>'
                f'<span>🎯 止盈 <b>{band(r.get("take_profit_range"))}</b></span>'
                f'<span>🛑 止损 <b>{band(r.get("stop_range"))}</b></span></div>',
                f'<p>💰 到止盈净空间 {band(r.get("net_target_return_pct"))}%　｜　止损情景 {band(r.get("stop_loss_pct"))}%　｜　净盈亏比 {text(r.get("net_rr"))}</p>',
                contribution_html(r),
                f'<p>🏢 {text(r.get("business_reason"))}</p>',
                f'<p>⚠️ {text(r.get("business_risk") or "未提供新增反证；仍需核验最新公告")}</p>',
                f'<p>⏱ 截止 {text(window.get("deadline"))} · 剩余 {text(window.get("remaining_sessions"))} 交易日；到期复盘，未入场不延长原计划。</p>',
                f'<p style="font-size:13px">下一步：{text("；".join(r.get("missing",[])) or "核对实际账户资金、费用与交易单位后再决定")}</p>',
                f'<details><summary>价格依据 · 复合评分 · 同业Top5</summary><p>{text(r.get("price_plan_scope"))}</p>',
                f'<p>报价 {text(r.get("last"))} {currency.get(r["market"], "")} · {text(r.get("quote_asof"))}；结构日 {text(r.get("price_plan_source_asof"))}；往返费用假设0.5%，非收益承诺。</p>']
        for f in (r.get('strategy_score') or {}).get('factors', []):
            out.append(f'<span style="margin-right:14px">{text(f["factor"])} · 权重{text(f["weight"])}% · 分值{text(f.get("value"))}</span>')
        for peer in info.get('peers', [])[:5]:
            out.append(f'<p>#{text(peer.get("rank"))} {text(peer.get("name"))} · {text(peer.get("code"))}</p>')
        out.append('</details></article>')
    out.append(watch_html(d.get('watch') or []))
    from astra_calendar_view import html as calendar_html
    out.append(calendar_html(d,now))
    out += ['<details><summary>📚 历轮记录 · 首次研究区间永久保留</summary>']
    for key, h in sorted(d.get('history', {}).items(), reverse=True):
        out.append(f'<p><b>{text(key)}轮</b> · 实际建立 {text(h.get("created_at"))} · {text(h.get("updates"))}次变化</p>')
        for first in list(h.get('first', {}).values())[:15]:
            r = first['row']
            out.append(f'<p>{text(first["at"])} · {text(r.get("name"))} · 首次入场 {band(r.get("entry_range"))} / 止盈 {band(r.get("take_profit_range"))} / 止损 {band(r.get("stop_range"))}</p>')
    out += ['</details><p style="font-size:12px;color:#64748b">规则筛选分不是GPT审核分或胜率。研究区间不改变已有持仓合同；月度实际收益与风险账本独立核算，未对账不计零。GitHub更新不依赖模型额度。</p></section>']
    return ''.join(out)


def money_html(d):
    ms = d.get('month_status') or {}; pol = d.get('selection_policy') or {}
    target = ms.get('monthly_target_usd', 200); risk = ms.get('risk_per_trade_usd'); cap = ms.get('monthly_risk_cap_usd')
    realized = '待对账（未对账不计零）' if ms.get('realized_net_usd') is None else f'{ms["realized_net_usd"]:g} 美元'
    out = ['<div style="background:#eef2ff;border-radius:10px;padding:10px 14px;margin:8px 0">',
           f'<p style="margin:2px 0">🎯 月净收益目标 <b>${text(target)}</b>（目标，非收益承诺） · 本月已实现 <b>{text(realized)}</b> · 台账 {text(ms.get("ledger_state"))}</p>']
    if risk:
        out.append(f'<p style="margin:2px 0">🧮 单笔风险 ${text(risk)} · 月风险上限 ${text(cap)} · 每月最多{text(ms.get("max_new_entries"))}次新开仓 · 止盈收益 ≈ 净盈亏比 × ${text(risk)}</p>')
    if pol.get('version'):
        out.append(f'<p style="margin:2px 0;font-size:13px">盈利门槛 {text(pol["version"])}：净空间≥{text(pol.get("min_net_target_pct"))}% · 净盈亏比≥{text(pol.get("min_net_rr"))} · 规则分≥{text(pol.get("min_score"))} · {text(pol.get("time_rule"))}；美股本轮门槛 {text(pol.get("us_floor_this_run"))}分</p>')
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
