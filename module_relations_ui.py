"""Compact, shared cross-module evidence links for desktop/cloud/deep pages."""
from html import escape
from stock_reference import canonical, reference


def html(doc,selection,code=None):
    from joint_review_ui import html as joint_html
    joint=joint_html(selection,code)
    if not doc or doc.get('version')!='module-relations-v1':
        return joint+"<div class='v88-module-relations' style='font-size:12px;color:#b45309'>跨模块关联尚未生成；不能把多个模块出现视为多重验证。</div>"
    esc=lambda x:escape(str(x if x is not None else '未标明'))
    same=bool(doc.get('factpack_id') and doc.get('central_generated_at')
              and doc.get('factpack_id')==selection.get('factpack_id') and doc.get('central_generated_at')==selection.get('generated_at'))
    summary=doc.get('summary') or {}
    label=(f"跨模块关联 · {summary.get('domains',0)}个数据域 / {summary.get('files',0)}份文件 · "
           f"快照引用冲突 {summary.get('conflicts','未核实')}项")
    if not same:label+=' · 旧版关联，等待同版更新'
    content='<div>股票身份、审核分和原合同统一引用；板块、新闻、量价等是带时间的辅助证据，同源数据不重复计为独立验证。</div>'
    content+='<div>关联快照时间 '+esc(doc.get('generated_at'))+'；以下状态为捕获时结果，不代表来源已在本次打开页面时重新核验。</div>'
    if code:
        item=(doc.get('stocks') or {}).get(canonical(code)) or {}
        matches=[r for b in ('recommendations','preparations','blocked_3a','conditional','observations','pending','excluded') for r in selection.get(b,[]) if canonical(r.get('code'))==canonical(code)]
        central=matches[0] if len(matches)==1 else None
        bound=bool(same and central and item.get('master_ref')==reference(selection,central))
        content+=('<div>与所载中央分数及原合同引用一致；仅核对引用内容，不代表当前审核有效或独立验证。</div>' if bound else '<div style="color:#b45309">本票关联缺失或版本待更新；下列仅为来源线索，不参与当前评分。</div>')
        links=item.get('evidence_links') or []
        content+="<div style='overflow-x:auto'><table style='font-size:11px;min-width:720px;width:100%;border-collapse:collapse'><tr><th>模块</th><th>来源时间</th><th>关联量</th><th>交叉结果</th></tr>"
        for link in links:
            labels={'audit_score':'原审核分','score':'模块原始分','rank_score':'原模块排序分','screen_score':'周度辅助分',
                    'last':'来源价','px':'来源价','price':'来源价','chg5d':'5日涨跌%','chg20d':'20日涨跌%',
                    'return5_pct':'5日涨跌%','vol_ratio':'量比','pe':'市盈率','pe_ttm':'滚动市盈率','position252_pct':'252日位置%'}
            metrics='；'.join(labels.get(k,k)+'='+str(v) for k,v in (link.get('metrics') or {}).items()) or '身份/状态关联，无可比较数值'
            content+='<tr>'+''.join('<td style="padding:5px;border:1px solid #e2e8f0">'+esc(x)+'</td>' for x in
                (link['module'],link.get('source_asof'),metrics,link['status']+'；'+link['file_status']))+'</tr>'
        content+='</table></div>'
    else:
        content+="<div style='overflow-x:auto'><table style='font-size:11px;min-width:640px;width:100%'><tr><th>数据域</th><th>文件</th><th>股票关联数</th><th>来源状态</th></tr>"
        for row in doc.get('modules',[]):
            content+='<tr>'+''.join('<td>'+esc(x)+'</td>' for x in (row['module'],row['file'],row['stock_links'],row['file_status']))+'</tr>'
        content+='</table></div><div>逐股来源、原始分数及时间见个股深度页“跨模块关联”。未授级股票只关联身份，不生成审核分。</div>'
    return joint+"<details class='v88-module-relations' style='font-size:12px;color:#475569;margin:5px 0'><summary>"+esc(label)+'</summary>'+content+'</details>'
