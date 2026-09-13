"""Keep the user's original table on every screen; small screens scroll sideways."""
from html import escape
import re

CSS='''<style>
.v88-grade-scroll{width:100%;max-width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch}
.v88-grade-table{display:table!important;min-width:1500px}
.v88-grade-table thead{display:table-header-group!important}
.v88-grade-table tbody{display:table-row-group!important}
.v88-grade-table tr{display:table-row!important}
.v88-grade-table td,.v88-grade-table th{display:table-cell!important}
.v88-grade-table td,.v88-grade-table th,.v88-grade-table td span,.v88-grade-table td div,.v88-grade-table td summary{font-size:12px!important;line-height:1.4!important}
.v88-grade-table td .v88-listing-history{font-size:11px!important;color:#64748b!important}
.v88-grade-table th{font-weight:600}
.v88-grade-table td[data-label]::before{content:none!important}
</style>'''

def label_cells(rows_html,headers):
    def row(match):
        raw=match.group(0)
        if re.search(r'<td[^>]*colspan=',raw):return raw
        index=iter(headers)
        return re.sub(r'<td\b',lambda m:'<td data-label="'+escape(str(next(index,'')),quote=True)+'"',raw)
    return re.sub(r'<tr\b[^>]*>.*?</tr>',row,rows_html,flags=re.S)
