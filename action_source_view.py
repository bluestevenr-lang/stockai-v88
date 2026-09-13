"""Identity-only links from legacy signals to the rendered central board.

This consumer has no grading, ranking, pricing, or execution authority. The
source files remain intact for review; their old trade instructions never enter
the page. Board membership comes from the very HTML displayed in this fragment,
so refreshing cannot pair a new technical list with an older central shortlist.
"""
from html import escape
from html.parser import HTMLParser
from urllib.parse import urlencode

from scanner_central import canonical
from stock_profile_view import display_label


SOURCES = {
    'trend_quality.json': ('技术结构', ('rows',)),
    'intraday_decisions.json': ('盘中观察', ('rows',)),
    'value_zone.json': ('价值区间', ('rows',)),
    'trend_shift.json': ('趋势变化', ('up', 'down')),
    'opportunity_scan.json': ('机会扫描', ('exec',)),
    'sector_reps.json': ('行业代表', ('sectors',)),
}


class _BoardLinks(HTMLParser):
    def __init__(self, body):
        super().__init__()
        self.links = {}
        self.fallback = {}
        self.has_watch_board = False
        self.feed(body)
        if not self.has_watch_board:
            self.links = self.fallback

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag != 'tr':
            return
        code = canonical(attrs.get('data-code'))
        classes = (attrs.get('class') or '').split()
        if not code:
            return
        if 'v88-watch-row' in classes:
            self.has_watch_board = True
            anchor = attrs.get('id')
            if attrs.get('data-current') == 'true' and anchor == 'v88-watch-' + code:
                self.links[code] = '#' + anchor
        elif 'v88-focus-row' in classes:
            self.fallback[code] = '#v88-grade-list'


def records(documents):
    """Whitelist identity and original dates; never copy arbitrary row fields."""
    result = {}
    for filename, (source, buckets) in SOURCES.items():
        doc = documents.get(filename) or {}
        if not isinstance(doc, dict):
            continue
        for bucket in buckets:
            rows = doc.get(bucket)
            if not isinstance(rows, list):
                continue
            for raw in rows:
                if not isinstance(raw, dict):
                    continue
                row = raw.get('pick') if bucket == 'sectors' else raw
                if not isinstance(row, dict):
                    continue
                code = canonical(row.get('code') or row.get('sym'))
                if not code:
                    continue
                entry = result.setdefault(code, {'code': code, 'name': row.get('name') or code,
                                                  'sources': []})
                item = {'source': source, 'file': filename,
                        'source_asof': str(row.get('source_asof') or row.get('asof') or ''),
                        'recorded_at': str(doc.get('generated_at') or '')}
                if item not in entry['sources']:
                    entry['sources'].append(item)
    return [result[code] for code in sorted(result)]


def html(documents, central_html, *, profiles=None):
    links = _BoardLinks(central_html).links
    rows = records(documents)
    if not rows:
        return ''
    groups = {'A股': [], '港股': [], '美股': []}
    for row in rows:
        code = row['code']
        market = '港股' if code.endswith('.HK') else 'A股' if code.endswith(('.SS', '.SZ', '.BJ')) else '美股'
        deep = '?' + urlencode({'q': code, 'focus': 'deep'}) + '#v88-deep-analysis'
        name = escape(display_label(row['name'], code, profiles))
        source = '、'.join(dict.fromkeys(x['source'] for x in row['sources']))
        dates = '<br>'.join(escape(x['source']) + '：记录 ' + escape(x['recorded_at'] or '未提供')
                            + (' · 行情 ' + escape(x['source_asof']) if x['source_asof'] else '')
                            for x in row['sources'])
        relation = ('<a href="' + escape(links[code], quote=True) + '">↗ 定位中央榜行</a>'
                    if code in links else '未列当前重点榜')
        groups[market].append(
            '<tr class="v88-source-link-row" data-code="' + escape(code, quote=True) + '">'
            '<td><a target="_blank" rel="noopener" href="' + escape(deep, quote=True) + '">' + name + '</a></td>'
            '<td>' + escape(source) + '<details><summary>原记录时间</summary>' + dates + '</details></td>'
            '<td>' + relation + '<br><a target="_blank" rel="noopener" href="' + escape(deep, quote=True)
            + '">查看中央审核与原合同</a></td></tr>')
    body = ''.join('<details><summary>' + market + ' · ' + str(len(items)) + '条来源记录</summary>'
                   '<div style="overflow-x:auto"><table><thead><tr><th>个股</th><th>发现来源</th>'
                   '<th>关联中央记录</th></tr></thead><tbody>' + ''.join(items) + '</tbody></table></div></details>'
                   for market, items in groups.items() if items)
    return ('<details id="v88-action-source-links" class="v88-source-links"><summary>🔗 技术线索关联核对 · '
            + str(len(rows)) + '只</summary><p>当前推荐统一见 <a href="#v88-grade-list">3A中央重点榜</a>；'
            '持仓处置见 <a href="#v88-central-out">中央OUT</a>。以下按代码列出发现来源，保留原日期，'
            '点击个股查看当前审核及合同。</p>' + body + '</details>'
            '<style>.v88-source-links{font-size:12px;color:#475569;margin:6px 0}'
            '.v88-source-links p,.v88-source-links td details{font-size:11px!important}'
            '.v88-source-links table{width:100%;border-collapse:collapse}'
            '.v88-source-links td,.v88-source-links th{padding:6px;border-bottom:1px solid #e2e8f0;text-align:left}'
            '.v88-source-links a{color:#0369a1;text-decoration:underline}</style>')
