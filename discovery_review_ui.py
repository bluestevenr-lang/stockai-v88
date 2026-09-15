"""Compact, read-only comparison of discovery clues with the central decision."""
from html import escape
import math
from display_limits import market_top
from urllib.parse import urlencode


def text(value):
    return escape(str(value if value is not None and value != '' else '—'))


def number(value):
    return f'{value:g}' if type(value) in (int, float) and math.isfinite(value) else '—'


def band(value):
    return '～'.join(number(v) for v in value) if isinstance(value, (list, tuple)) and len(value) == 2 else '—'


def render(projection, stock_link=None):
    original = projection.get('rows') or []
    rows = market_top(original)
    parts = ['<div class="v88-discovery-review">',
             '<p style="font-size:12px">发现线索 · '+str(len(rows))+' 条（每市场Top5，总计至多15条）。等级、审核分和原合同取自3A中央记录。</p>',
             '<small>发现记录 '+text(projection.get('discovery_generated_at'))+
             (' · 已过期，仅供追溯' if not projection.get('discovery_fresh') else '')+
             ' ｜ 中央版本 '+text(projection.get('central_generated_at'))+'</small>',
             '<div style="overflow-x:auto"><table style="min-width:1100px;width:100%;font-size:12px;border-collapse:collapse">',
             '<thead style="background:#edf4ff"><tr><th>名称·市场</th><th>中央评级·审核分</th><th>原周期·研究合同</th><th>与3A主榜的关系</th><th>发现证据与下一步</th></tr></thead><tbody>']
    for row in rows:
        central = row.get('central') or {}
        current = central.get('current') is True
        tier = central.get('tier') if current else None
        palette = {'3A': ('🟢', '#166534'), '2A': ('🟠', '#92400e'), '1A': ('🔵', '#1d4ed8')}
        icon, color = palette.get(tier, ('🔎', '#64748b'))
        name, code = row.get('name') or row.get('code'), row.get('code')
        link = stock_link(name, code) if stock_link else '<a href="?'+escape(urlencode({'q': code, 'focus': 'deep'}), quote=True)+'">'+text(name)+'</a>'
        score = number(central.get('audit_score')) if current or (central.get('review_complete') and central.get('fresh')) else '—'
        score_label = '审核分' if current else '复审分（未授级）'
        plan = central.get('trade_plan') or {}
        profit = plan.get('profit_contract') or {}
        horizon = central.get('horizon') or plan.get('horizon')
        horizon = {'short': '短期1–30天', 'medium': '中期31–90天', 'long': '长期91–365天'}.get(horizon, horizon)
        contract = ('原进场 '+band(plan.get('entry_range'))+'<br>原止盈 '+band(plan.get('take_profit_range') or profit.get('take_profit_range'))+
                    ' · 失效 '+number(plan.get('stop'))+'<br><small>截止 '+text(profit.get('thesis_deadline'))+'</small>') if plan else '尚无可引用的中央合同'
        if plan and not current:
            contract = '<small>历史合同·当前未授级</small><br>'+contract
        auxiliary = row.get('auxiliary') or {}
        aux = '量价辅助分 '+number(auxiliary.get('quant_score'))+' / 100；短期方向分 '+number(auxiliary.get('short_direction_score'))+' / 100'
        aux += '<br>技术毛RR '+number(auxiliary.get('technical_gross_rr'))+' · '+text(auxiliary.get('stage'))
        aux += '<br><small>未标定为上涨概率，不能替代原合同净RR或审核分。</small>'
        channels = row.get('discovery_channels') or []
        channel_text = '、'.join(str(v) for v in channels)
        gaps = row.get('gaps') or []
        gap_text = '；'.join(str(v) for v in gaps)
        relation = central.get('reason') or central.get('status') or '中央证据尚未齐全'
        parts.append('<tr style="border-bottom:1px solid #e2e8f0;vertical-align:top">'
                     '<td style="padding:8px">'+link+'<br><small>'+text(code)+' · '+text(row.get('market'))+'</small></td>'
                     '<td style="padding:8px;color:'+color+'">'+icon+' '+text(tier or central.get('status') or '未授级')+
                     '<br>'+score_label+' '+score+'<br><small>'+text(central.get('reviewed_at'))+'</small></td>'
                     '<td style="padding:8px">'+text(horizon)+'<br>'+contract+'</td>'
                     '<td style="padding:8px">'+text(relation)+'<details><summary>缺口与未入榜原因</summary>'+text(gap_text)+'</details></td>'
                     '<td style="padding:8px">'+text(row.get('next_step'))+
                     '<details><summary>量价线索与来源</summary>'+aux+'<br>发现渠道：'+text(channel_text)+
                     '<br><small>同引擎不同周期、重复名单不计独立数据源。</small></details></td></tr>')
    if not rows:
        parts.append('<tr><td colspan="5">本轮没有可关联的发现记录；继续查看3A中央列表。</td></tr>')
    parts.append('</tbody></table></div></div>')
    return ''.join(parts)
