from copy import deepcopy
from html import escape
from portfolio_archive_view import records_html


def test_original_records_keep_links_values_and_never_mutate():
    rows = [{'名称': '示例', '代码': '700.HK', '成本': 0},
            {'name': 'Example', 'code': 'TEST', 'date': '2026-08-01', 'notes': '<script>x</script>'}]
    before = deepcopy(rows)
    calls = []
    def link(name, code):
        calls.append((name, code))
        return f'<a href="?q={code}&amp;focus=deep">{escape(name)}</a>'
    text = records_html(rows, stock_link=link)
    assert rows == before
    assert calls == [('示例', '700.HK'), ('Example', 'TEST')]
    assert 'focus=deep' in text and '>0</td>' in text and '2026-08-01' in text
    assert '&lt;script&gt;' in text and '<script>' not in text
    assert '账户历史原记录 · 2条' in text and '历史持仓与浮盈不等于当前持仓或已对账净收益' in text


def test_empty_and_missing_identity_do_not_invent_a_security():
    def forbidden(*args):
        raise AssertionError('No code present')
    assert records_html([], stock_link=forbidden) == ''
    text = records_html([{'name': '旧记录', 'quantity': None}], stock_link=forbidden)
    assert '旧记录' in text and '>—</td>' in text
