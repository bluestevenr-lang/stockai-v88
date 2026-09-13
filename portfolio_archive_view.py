"""Read-only private portfolio history with retained security deep links."""
from html import escape


def records_html(rows, *, stock_link):
    rows = [r for r in rows if isinstance(r, dict)]
    if not rows:
        return ""
    columns = list(dict.fromkeys(k for r in rows for k in r))
    body = []
    for row in rows:
        code = row.get("代码") or row.get("code") or row.get("ticker")
        cells = []
        for column in columns:
            value = row.get(column)
            if column in {"名称", "name"} and column in row and code:
                text = stock_link(str(value or code), str(code))
            else:
                text = escape(str(value)) if value is not None else "—"
            cells.append(f"<td>{text}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    return ('<div style="font-size:11px;color:#64748b">🗂 账户历史原记录 · '+str(len(rows))+'条；历史持仓与浮盈不等于当前持仓或已对账净收益。</div>'
            '<div style="overflow-x:auto;font-size:12px"><table>'
            '<thead><tr>' + ''.join(f"<th>{escape(str(k))}</th>" for k in columns)
            + '</tr></thead><tbody>' + ''.join(body) + '</tbody></table></div>')
