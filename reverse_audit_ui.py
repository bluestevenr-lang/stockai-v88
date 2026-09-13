"""Compact read-only reverse-audit panel. Keep the original stock list layout."""
from datetime import datetime, timezone, timedelta
from html import escape


def html(doc=None, status=None, selection=None, now=None):
    doc, status, selection = doc or {}, status or {}, selection or {}
    now = now or datetime.now(timezone.utc)
    try:
        checked = datetime.fromisoformat(str(status.get('checked_at')).replace('Z', '+00:00'))
        fresh = checked.tzinfo is not None and 0 <= (now-checked).total_seconds() <= 600
    except (ValueError, TypeError):
        fresh = False
    try:
        published=datetime.fromisoformat(str(doc.get('generated_at')).replace('Z','+00:00'))
        report_time=published.tzinfo is not None and published<=checked<=now
    except (ValueError,TypeError,UnboundLocalError):report_time=False
    valid = bool(fresh and report_time and doc.get('version')=='reverse-audit-v1'
                 and status.get('ok') is True and doc.get('input_id') and status.get('input_id') == doc.get('input_id')
                 and doc.get('factpack_id') and doc.get('source_generated_at')
                 and (not status.get('factpack_id') or status['factpack_id']==doc['factpack_id'])
                 and doc.get('factpack_id') == selection.get('factpack_id')
                 and doc.get('source_generated_at') == selection.get('generated_at'))
    e = lambda value: escape(str(value if value is not None else '—'))
    focus = ("<div class='v88-investment-focus' style='font-size:12px;color:#334155;margin:5px 0'>"
             "<b>三周期3A研究</b> · 短期未来8周、中期8–24周、长期12–36周，各自Top3，按审核分排序。"
             "入场统一核未来5个交易日；持有与退出沿用各周期原合同，3A是审核等级。</div>")
    if not valid:
        return focus + "<div class='v88-reverse-audit' style='font-size:12px;color:#92400e'>反查本轮未就绪或超过10分钟未核对；历史记录保留，等待同步。</div>"
    summary = doc.get('summary') or {}; by = summary.get('by_horizon') or {}
    totals = {t: sum((by.get(h) or {}).get(t, 0) for h in ('medium', 'long')) for t in ('3A', '2A', '1A')}
    head = (f"中长期：3A {totals['3A']} · 2A {totals['2A']} · 1A {totals['1A']}"
            f" ｜ 反查 {e(summary.get('audited_rows'))}条 · 待修 {e(summary.get('open_issues'))}项")
    issues = ''.join(f"<tr><td>{e(i.get('priority'))}</td><td>{e(i.get('title'))}</td>"
                     f"<td>{e(i.get('next_action'))}</td></tr>" for i in doc.get('issues', []))
    active = [r for r in doc.get('rows', []) if r.get('current_tier')]
    rows = ''
    for r in active:
        gaps = '；'.join(c.get('title', '') for c in r.get('checks', []) if c.get('status') != 'pass')
        rows += (f"<tr><td><a href='?q={e(r['code'])}&amp;focus=deep#v88-deep-analysis'>{e(r.get('name') or r['code'])}</a>"
                 f" · {e(r['code'])}</td><td>{e(r.get('lane'))} · {e(r['current_tier'])} · {e(r.get('published_score'))}</td>"
                 f"<td>{e(gaps or '一致性检查通过；收益仍待验证')}</td></tr>")
    return (focus + "<details class='v88-reverse-audit' style='font-size:12px;margin:4px 0;color:#475569'>"
            f"<summary>{head}</summary><p style='font-size:12px'>核对时间 {checked.astimezone(timezone(timedelta(hours=8))).strftime('%m-%d %H:%M')} 北京时间；"
            "结论→事实→合同→主审/反审→书理→后续结算。反查不修改评级；审核分不是胜率。</p>"
            "<div style='overflow:auto;max-height:320px'><table style='font-size:12px;width:100%;min-width:680px'><thead><tr><th>级别</th><th>待修事项</th><th>完成条件</th></tr></thead>"
            f"<tbody>{issues}</tbody></table></div><details><summary>当前有效评级逐股反查</summary>"
            "<div style='overflow-x:auto'><table style='font-size:12px;width:100%'><thead><tr><th>个股</th><th>定位·原评级·分数</th><th>仍缺证据</th></tr></thead>"
            f"<tbody>{rows}</tbody></table></div></details></details>")
