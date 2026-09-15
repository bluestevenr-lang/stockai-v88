"""Compact discovery/qualification explanation; no competing recommendation list."""
from html import escape


def html(report):
    if not report:
        return ''
    esc = lambda v: escape(str(v if v is not None else '待核'))
    fmt = lambda v: f'{v:.2f}'.rstrip('0').rstrip('.') if isinstance(v, (int, float)) else '待核'
    palette = {'pass': ('#166534', '✓'), 'fail': ('#9a3412', '×'),
               'unknown': ('#64748b', '○'), 'not_applicable': ('#64748b', '—')}
    cards = ''
    detail_rows = ''
    for b in report['branches']:
        color, icon = palette[b['state']]
        cards += (f'<div style="border:1px solid #dde5ef;border-top:3px solid {color};border-radius:7px;'
                  'padding:8px;min-width:0">'
                  f'<b style="font-size:13px">{esc(b["title"])}</b>'
                  f'<div style="color:{color};font-size:12px;margin-top:4px">{icon} {esc(b["summary"])}</div></div>')
        for c in b['checks']:
            _, mark = palette[c['state']]
            detail_rows += '<tr>' + ''.join(f'<td style="padding:5px;border-bottom:1px solid #e2e8f0">{esc(v)}</td>'
                for v in (b['title'], c['title'], mark, c['detail'])) + '</tr>'
    clue = report['local_repair_clue']
    clue_text = ('🌱 局部修复线索：' + clue['detail']) if clue['value'] is True else (
        '○ 局部线索待核' if clue['value'] is None else '○ 最近三日低点未连续抬高')
    central = report['central']
    score = (f'中央审核 {fmt(central["score"])} / 100 · {esc(central["grade"])}'
             if central['current'] else '中央当前审核待核')
    blockers = report['blockers']
    brief = ' · '.join(str(b['title']) for b in blockers[:5])
    if len(blockers) > 5:
        brief += f' · 另{len(blockers)-5}项'
    blocker_details = ''.join(f'<li><b>{esc(b["title"])}</b>：{esc(b["detail"])}</li>' for b in blockers)
    reason_details = ''.join(f'<li>{esc(x)}</li>' for x in central['reason_codes'])
    coverage = ' · '.join(f'{d["title"]}：'+ {'observed':'有记录','limited':'部分资料','missing':'缺资料','conflict':'有冲突'}.get(d['status'], '待核')
                          for d in report['domain_coverage'])
    return ('<style>.v88-path-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px}'
        '@media(max-width:900px){.v88-path-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}</style>'
        '<section class="v88-research-paths" style="margin:10px 0;padding:12px;border:1px solid #dbe5f2;border-radius:9px">'
        '<b style="font-size:15px">研究线索 ↔ 中央审核</b>'
        f'<div style="font-size:13px;margin:7px 0">{esc(clue_text)} <span style="color:#64748b">｜{score}</span></div>'
        f'<div class="v88-path-grid">{cards}</div>'
        + (f'<div style="font-size:12px;color:#9a3412;margin-top:8px">待解决：{esc(brief)}</div>' if blockers else '')
        + '<details style="font-size:12px;color:#64748b;margin-top:8px"><summary>展开路径依据、中央未入选原因与数据日期</summary>'
        f'<p>行情截至 {esc(report["source_asof"])}；{esc(report["source"])}；{esc(report["price_basis"])}。</p>'
        '<p>✓仅表示该形态条件满足；○待核；×未满足；—当前不适用。新路径为影子研究，不授级或新增入场许可。'
        '趋势形态、局部修复与深跌各自计算；同源量价不重复计为独立证据。</p>'
        '<div style="overflow-x:auto"><table style="font-size:12px;width:100%;min-width:650px"><tbody>'
        + detail_rows + '</tbody></table></div>'
        + (f'<p>当前分数：GPT {fmt(central["gpt_score"])}，书理 {central["book_pass_n"]}/{central["book_required"]}项'
           f'（{fmt(central["book_score"])}分），按既有保守口径取 {fmt(central["score"])}。分数不等于胜率。</p>' if central['current'] else '')
        + '<p>中央条件与核验卡点：</p><ul>' + blocker_details + '</ul>'
        + '<details><summary>中央发布保留的全部原因</summary><ul>' + reason_details + '</ul></details>'
        + f'<p>{esc(coverage)}。有记录不等于支持或通过；长期路径须按对应周期重新综合复审。</p>'
        + f'<p>{esc(report["scope"])} 本地计算；新增模型/网络调用均为0。</p>'
        + '</details></section>')
