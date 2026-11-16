"""Shared horizontal early-opportunity table for desktop and public cloud."""
from html import escape
from urllib.parse import urlencode

def html(doc, code=None):
    rows=[r for r in doc.get('early_opportunities',[]) if not code or r.get('code')==code]
    esc=lambda x:escape(str(x if x is not None else '待核'),quote=True)
    out=['<section style="background:#f0fdf7;border:1px solid #bbf7d0;border-radius:14px;padding:16px;margin:14px 0">',
         '<h3>🌱 底部与量价拐点 · 提前关注</h3>',
         '<p>按已完成日线核验低点抬高、回撤缩量与上涨量能改善；左侧提醒与正式评级分别跟踪。失效位固定，未触发不计成交。</p>']
    if not rows:out.append('<p>本轮暂无已核拐点提醒；资料缺失不代表全市场没有机会。</p>')
    else:
        out.append('<p>共 '+str(len(rows))+' 条已核提醒；每市场先展示 3 条，重点关注优先，其余完整保留在展开区。</p>')
        out.append('<div style="overflow-x:auto"><table style="min-width:1000px;width:100%"><tr><th>个股 / 阶段</th><th>现价 / 行情时间</th><th>关注区间 / 失效</th><th>量价变化 / 风险</th></tr>')
        for m in ('A股','港股','美股'):
            market_rows=[r for r in rows if r['market']==m]
            for index,r in enumerate(market_rows):
                if index==3:
                    out.append('</table></div><details><summary>'+esc(m)+' · 展开其余 '+str(len(market_rows)-3)+' 条拐点提醒</summary><div style="overflow-x:auto"><table style="min-width:1000px;width:100%">')
                href='?'+urlencode({'q':r['code'],'focus':'deep'})
                currency=r.get('currency') or {'A股':'CNY','港股':'HKD','美股':'USD'}[m]
                out.append(f'<tr><td><a href="{esc(href)}">{esc(r.get("name") or r["code"])} {esc(r["code"])}</a><br>{esc(r["stage"])}{" · ⭐重点关注" if r.get("attention") else ""}</td>'
                           f'<td>{esc(r["last"])} {currency}<br>{esc(r.get("quote_asof"))}</td>'
                           f'<td>{esc(r.get("entry_range"))}<br>失效 {esc(r.get("stop"))} · 目标参照 {esc(r.get("target"))}<br>5交易日复核</td>'
                           f'<td>{esc(r["reason"])}<br>{esc(r.get("risk") or "新规则跟踪，胜率待积累")}</td></tr>')
            if len(market_rows)>3:out.append('</table></div></details><div style="overflow-x:auto"><table style="min-width:1000px;width:100%">')
        out.append('</table></div>')
    s=doc.get('learning_status') or {}
    if s:out.append('<p>📒 前瞻纸面跟踪：已结算 '+esc(s.get('settled'))+' 笔 · 胜率 '+esc(s.get('win_rate_pct'))+' · 出现率 '+esc(s.get('appearance_rate_pct'))+'%；参数升级须独立验证。</p>')
    return ''.join(out)+'</section>'
