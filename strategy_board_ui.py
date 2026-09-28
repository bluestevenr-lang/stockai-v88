"""Same strategy publication on desktop and cloud; no inference in the UI."""
import json
from html import escape
from urllib.parse import urlencode
from v88_paths import core_root

def html(doc=None,code=None,now=None):
    if doc is None:
        try:doc=json.loads((core_root()/'data/strategy_board.json').read_text())
        except (OSError,ValueError):doc={}
    if doc.get('version')!='strategy-taxonomy-2026-09-28-v1':return ''
    from strategy_board_state import view
    doc=view(doc,now)
    esc=lambda v:escape(str(v if v is not None else '—'),quote=True)
    flags={'A股':'🇨🇳','港股':'🇭🇰','美股':'🇺🇸'}
    rows=[r for r in doc.get('rows',[]) if not code or r['code']==code]
    out=['''<style>.strategy-board .lane{margin:14px 0;border:1px solid #d8e4f3;border-radius:14px;overflow:hidden;background:#fff}
    .strategy-board .head{padding:18px 20px;background:#eef5ff;display:flex;justify-content:space-between;align-items:center;gap:12px}
    .strategy-board h3{font-size:22px!important;margin:0 0 5px}.strategy-board small{font-size:13px!important;color:#64748b}
    .strategy-board .count{font-size:28px;color:#2459c7;font-weight:700;white-space:nowrap}
    .strategy-board table{border-collapse:collapse;width:100%;min-width:900px}.strategy-board th{background:#f5f8fc;text-align:left}
    .strategy-board td,.strategy-board th{padding:12px 14px;border-bottom:1px solid #e5ebf4;vertical-align:top;font-size:14px!important;line-height:1.6;overflow-wrap:anywhere}
    .strategy-board details{font-size:13px}.strategy-board .score{font-size:21px;color:#2459c7;font-weight:700}.strategy-board .market{background:#edf4ff;font-weight:600}
    </style><div class="strategy-board"><h3>🎯 1A / 2A / 3A 策略榜</h3>
    <p>1A 抓 3–5 日机会 · 2A 跟 2 周至 1 个月波段 · 3A 分价值、成长、反弹。每档各市场 Top3，独立排名。</p>
    <small>2026-09-28 新策略制 · 筛选分不代表胜率或 GPT 复审分；入场状态单列。旧评级与日历原样保留。</small>''']
    settings=[('1A','1A · 超短线','3–5个交易日'),('2A','2A · 短线 / 月度','2周–1个月'),('3A','3A · 投资机会','价值投资 / 成长潜力 / 超跌反弹')]
    for tier,title,period in settings:
        members=[r for r in rows if r['strategy_tier']==tier]
        if code and not members:continue
        matched=sum(r['status']=='条件匹配' for r in members)
        retained=sum(not r.get('current_source') for r in members)
        out.append(f'<section class="lane"><div class="head"><div><h3>{title}</h3><small>{period} · '+ ' · '.join(f'{m} {sum(r["market"]==m for r in members)}只' for m in flags)+f'</small></div><div><span class="count">{len(members)} 只</span><br><small>条件匹配 {matched} · 研究候选 {len(members)-matched-retained} · 待更新留存 {retained}</small></div></div>')
        if not members:
            out.append('<p style="padding:0 20px">本轮所需证据不足或未达到策略条件；不会用其他周期填补名额。</p>')
        else:
            out.append('<div style="overflow-x:auto"><table><thead><tr><th>市场 / 排名 / 个股</th><th>复合筛选分</th><th>为什么入选 / 周期</th><th>入场状态 / 价格结构</th><th>报价 / 时间</th></tr></thead><tbody>')
            for market,flag in flags.items():
                chosen=sorted([r for r in members if r['market']==market],key=lambda r:r['rank'])[:3]
                out.append(f'<tr class="market"><td colspan="5">{flag} {market} · {len(chosen)}/3只</td></tr>')
                for r in chosen:
                    href='?'+urlencode({'q':r['code'],'focus':'deep'})
                    s=r['strategy_score'];parts=''.join(f'<div>{esc(p["factor"])} · 权重 {p["weight"]}% · 贡献 {p["contribution"]:g}分</div>' for p in s['factors'])
                    band='–'.join(f'{p:g}' for p in r['entry_range']) if r.get('entry_range') else '待核结构'
                    missing=' / '.join(r.get('missing',[]))
                    out.append(f'<tr><td><b>#{r["rank"]} <a href="{esc(href)}">{esc(r["name"])}</a></b><br><small>{esc(r["code"])}</small></td><td><span class="score">{s["value"]:g}</span> /100<br><small>证据覆盖 {s["coverage"]}%</small><details><summary>权重与贡献</summary>{parts}<div>共振 +{s["interaction"]} · 过热扣 {s["penalty"]}</div></details></td><td><b>{esc(r["lane"])}</b> · {esc(r["period"])}<br>{esc(r["reason"])}<details><summary>🏢 企业依据与反证</summary>{esc(r["business_reason"])}<br>{esc(r.get("business_risk"))}</details></td><td><b>◉ {esc(r["status"])}</b><br>研究入场区：{esc(band)}<br><small>{esc(missing or "结构条件匹配；下单前核对最新价格")}</small><details><summary>目标 / 失效 / 复审</summary>结构目标 {esc(r.get("target"))} · 失效 {esc(r.get("stop"))}<br>{esc(r["invalidation"])}<br>{esc(r["model_review"])}<br>净收益风险比按往返费用0.5%研究假设计算；不是交易执行指令。</details></td><td><b>{r["last"]:g}</b> · {r["change_pct"]:+.2f}%<br><small>{esc(r["quote_asof"][5:16])}<br>源时区</small></td></tr>')
            out.append('</tbody></table></div>')
        if tier=='3A':out.append('<small style="display:block;padding:10px 20px">'+esc(doc.get('value_lane_status'))+'</small>')
        out.append('</section>')
    out.append('</div>')
    return ''.join(out)
