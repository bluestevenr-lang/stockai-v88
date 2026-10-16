"""Read-only presentation of dated, benchmark-relative sector observations."""
from datetime import datetime, timezone
from html import escape
import math
import re
from urllib.parse import quote

from sector_rotation import VERSION, BENCHMARKS


MARKETS = ("A股", "港股", "美股")
STATES = ("持续占优", "短期改善", "短期转弱", "持续落后")
COLORS = ("#19735a", "#2678c8", "#a96515", "#7b6a93")


def _text(value):
    return escape(str(value if value is not None else "待核"))


def _number(value, suffix=""):
    if type(value) not in (int, float) or not math.isfinite(value):
        return "待核"
    return f"{value:+.2f}{suffix}"


def _day(value):
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date().isoformat()
    except (ValueError, TypeError):
        return None


def _finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def _mapping(value):
    return value if isinstance(value, dict) else {}


def _transition(row):
    item = _mapping(row.get("transition"))
    status = item.get("status")
    prior, current = item.get("from_state"), item.get("to_state")
    count = item.get("consecutive_sessions")
    if status == "first_observation":
        label = "首次观察 · 待跨日确认"
    elif status == "nonconsecutive":
        label = "观察日期不连续 · 待确认"
    elif status == "pending_confirmation":
        label = f"{prior or '原状态待核'} → {current or row.get('quadrant')} · 待跨日确认"
    elif (status == "confirmed" and item.get("confirmed") is True
          and type(count) is int and count >= 2
          and _day(item.get("prior_session")) and _day(row.get("source_asof"))
          and _day(item["prior_session"]) < _day(row["source_asof"])):
        label = f"{prior or '原状态待核'} → {current or row.get('quadrant')} · 已跨日确认"
    elif status == "unchanged":
        label = "状态延续"
    else:
        label = "跨日确认待核"
    if type(count) is int and count > 0:
        label += f" · 连续 {count} 个交易日"
    if item.get("prior_session"):
        label += " · 前次 " + str(item["prior_session"])
    return _text(label)


def _linked_stocks(row, market):
    """Only explicit upstream identity associations; never infer constituents."""
    members = row.get("linked_stocks") or []
    out = []
    for item in members if isinstance(members, list) else []:
        if not isinstance(item, dict) or item.get("market") != market:
            continue
        code = str(item.get("code") or "")
        if not code or item.get("module") not in ("3A", "Astra", "持仓", "自选"):
            continue
        price = _number(item.get("last")).lstrip("+")
        out.append('<a href="/?q=' + quote(code, safe="") + '&amp;focus=deep">'
                   + _text(item.get("name") or code) + '</a> · ' + _text(item["module"])
                   + ' · 现 ' + price + ' ' + _text(item.get("currency"))
                   + ' · 报价 ' + _text(item.get("quote_asof")))
    return '<div class="sr-links">' + '<br>'.join(out) + '</div>' if out else ''


def _row(row, market, *, historical=False):
    date_note = ('<span class="sr-history-tag">历史 ' + _text(row.get("source_asof")) + '</span>') if historical else ''
    basis = {"aligned_daily_closes": "对齐完整日线", "same_session_summary": "同日摘要窗口"}.get(row.get("basis"), "计算口径待核")
    return ('<div class="sr-row"><div class="sr-name">' + _text(row.get("name") or row.get("symbol"))
            + date_note + '</div><div class="sr-numbers">'
            + '<span>5日超额 <b>' + _number(row.get("rs5_pp"), "pp") + '</b></span>'
            + '<span>20日超额 <b>' + _number(row.get("rs20_pp"), "pp") + '</b></span></div>'
            + '<div class="sr-actual">代理5日涨跌 ' + _number(row.get("sector_chg5d"), "%")
            + ' · 行情 ' + _text(row.get("source_asof")) + ' · ' + basis + '</div>'
            + ('<div class="sr-transition">' + _transition(row) + '</div>' if not historical else '')
            + _linked_stocks(row, market) + '</div>')


_CSS = """
#__ID__{font:12px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#263447;background:#fff;border:1px solid #e7ebf0;border-radius:12px;padding:10px 12px;box-sizing:border-box}
#__ID__ *{box-sizing:border-box}#__ID__ .sr-header{display:flex;align-items:baseline;justify-content:space-between;gap:8px;flex-wrap:wrap;margin-bottom:4px}
#__ID__ .sr-title{font-size:13px;font-weight:650}#__ID__ .sr-coverage{font-size:11px;font-variant-numeric:tabular-nums;color:#607086}
#__ID__ .sr-meta,#__ID__ .sr-note,#__ID__ .sr-transition,#__ID__ .sr-actual{font-size:10px;color:#66758a;overflow-wrap:anywhere}
#__ID__ .sr-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:0 12px;margin:7px 0}
#__ID__ .sr-group{min-width:0;border-top:1px solid #edf0f4;padding-top:6px;margin-top:3px}#__ID__ .sr-group h4{font-size:11px;line-height:1.3;margin:0 0 4px;font-weight:650}
#__ID__ .sr-group h4 span{font-weight:400;color:#8791a1;margin-left:5px}#__ID__ .sr-row{padding:3px 0 5px;min-width:0}
#__ID__ .sr-name{font-size:11px;font-weight:600;overflow-wrap:anywhere}#__ID__ .sr-numbers{display:flex;flex-wrap:wrap;gap:0 10px;font-size:10px;font-variant-numeric:tabular-nums}#__ID__ .sr-numbers b{font-weight:550}
#__ID__ .sr-empty{font-size:10px;color:#8a94a3;padding:3px 0 7px}#__ID__ details{font-size:10px;margin:5px 0}#__ID__ summary{color:#61738a;cursor:pointer}#__ID__ .sr-pending{padding:3px 0;color:#7c6a4e;overflow-wrap:anywhere}
#__ID__ .sr-history-tag{font-weight:400;font-size:9px;margin-left:5px;color:#897c6b}#__ID__ .sr-links{font-size:10px;overflow-wrap:anywhere;margin-top:2px}#__ID__ a{color:#1767a0;text-decoration:none}
#__ID__ .sr-foot{font-size:9px;color:#7b8798;border-top:1px solid #edf0f4;padding-top:6px;margin-top:5px}
@media(max-width:520px){#__ID__{padding:8px}#__ID__ .sr-grid{grid-template-columns:minmax(0,1fr);gap:0}#__ID__ .sr-numbers{gap:0 8px}}
"""


def sector_rotation_html(doc, market="美股", *, now=None, element_id="v88-sector-relative"):
    """Render supplied facts only; recheck session expiry, never recalculate signals."""
    from exchange_sessions import latest_completed
    now = now or datetime.now(timezone.utc)
    safe_id = re.sub(r"[^A-Za-z0-9_-]", "-", str(element_id))
    expected, calendar_error = None, None
    try:
        expected = latest_completed(market, now).isoformat()
    except (ValueError, TypeError, AttributeError):
        calendar_error = "市场或交易日历未核验，暂停当前判定"
    doc = _mapping(doc)
    compatible = doc.get("version") == VERSION
    block = _mapping(_mapping(doc.get("markets")).get(market)) if compatible else {}
    if block.get("market") != market:
        block = {}
    benchmark = _mapping(block.get("benchmark"))
    benchmark_symbol, benchmark_name = BENCHMARKS.get(market, (None, "基准待核"))
    fixed_benchmark = bool(benchmark_symbol and benchmark.get("symbol") == benchmark_symbol)
    source_rows = block.get("rows") or []
    rows, seen = [], set()
    for row in source_rows if isinstance(source_rows, list) else []:
        if not isinstance(row, dict) or row.get("market") != market:
            continue
        symbol = row.get("symbol")
        if not isinstance(symbol, str) or not symbol or symbol in seen:
            continue
        seen.add(symbol)
        rows.append(row)
    current, historical, pending = [], [], []
    for row in rows:
        has_facts = all(_finite(row.get(key)) for key in ("rs5_pp", "rs20_pp", "sector_chg5d"))
        aligned = (expected is not None and fixed_benchmark
                   and row.get("benchmark_symbol") == benchmark_symbol
                   and _day(row.get("source_asof")) == expected == _day(row.get("benchmark_asof"))
                   and row.get("basis") in ("aligned_daily_closes", "same_session_summary"))
        if row.get("status") == "current" and has_facts and aligned and row.get("quadrant") in (*STATES, "中性"):
            current.append(row)
        elif (has_facts and fixed_benchmark and row.get("benchmark_symbol") == benchmark_symbol
              and _day(row.get("source_asof")) == _day(row.get("benchmark_asof"))
              and _day(row.get("source_asof"))
              and expected is not None
              and (_day(row.get("source_asof")) < expected
                   or (row.get("status") == "historical" and _day(row.get("source_asof")) == expected))):
            historical.append(row)
        else:
            pending.append(row)
    configured = _mapping(block.get("coverage")).get("expected")
    configured = max(configured, len(rows)) if type(configured) is int and configured >= 0 else len(rows)
    current_dates = sorted({str(r.get("source_asof")) for r in current})
    source = '、'.join(current_dates) if current_dates else '当前行情待更新'
    output = [f'<style>{_CSS.replace("__ID__", safe_id)}</style>',
              f'<section id="{safe_id}" aria-label="板块相对强弱观察" data-market="{_text(market)}" data-current="{len(current)}">',
              '<div class="sr-header"><span class="sr-title">' + _text(market) + ' · 板块轮动观察</span>',
              '<span class="sr-coverage">当前有效 ' + str(len(current)) + ' / 配置 ' + (str(configured) if configured else '待核') + '</span></div>',
              '<div class="sr-meta">同市场固定基准 ' + _text(benchmark_name) + '（' + _text(benchmark_symbol) + '）'
              + ' · 行情截至 ' + _text(source) + ' · 应有完整交易日 ' + _text(expected) + '</div>',
              '<div class="sr-meta">计算 ' + _text(doc.get("generated_at")) + ' · 超额为代理涨跌减基准涨跌（百分点）</div>']
    if calendar_error:
        output.append('<div class="sr-pending" role="status">' + calendar_error + '</div>')
    if block and not fixed_benchmark:
        output.append('<div class="sr-pending" role="status">收到的基准与本市场固定基准不一致，暂停当前判定。</div>')
    if not current:
        output.append('<div class="sr-pending" role="status">当前轮动待更新；缺数据不代表没有强势板块。</div>')
    elif len(current) < configured:
        output.append('<div class="sr-note">仅覆盖部分已配置代理，不能视为完整市场轮动。</div>')
    output.append('<div class="sr-grid">')
    for state, color in zip(STATES, COLORS):
        members = [r for r in current if r.get("quadrant") == state]
        output.append('<div class="sr-group"><h4 style="color:' + color + '">' + state + '<span>' + str(len(members)) + '</span></h4>')
        output.extend(_row(row, market) for row in members)
        if not members:
            output.append('<div class="sr-empty">' + ('当前已核代理无此状态' if current else '等待当前行情') + '</div>')
        output.append('</div>')
    output.append('</div>')
    neutral = [r for r in current if r.get("quadrant") == '中性']
    if neutral:
        output.append('<details><summary>中性边界 · ' + str(len(neutral)) + '</summary>' + ''.join(_row(r, market) for r in neutral) + '</details>')
    if historical:
        output.append('<details><summary>历史观察 · ' + str(len(historical)) + '，不计当前</summary>' + ''.join(_row(r, market, historical=True) for r in historical) + '</details>')
    if pending or configured > len(rows):
        output.append('<details><summary>数据待补 · ' + str(len(pending) + max(0, configured-len(rows))) + '</summary>')
        for row in pending:
            output.append('<div class="sr-pending">' + _text(row.get('name') or row.get('symbol')) + ' · '
                          + _text(row.get('reason') or '基准、行情日期或相对强弱证据待核')
                          + ' · 原行情 ' + _text(row.get('source_asof')) + '</div>')
        if configured > len(rows):
            output.append('<div class="sr-pending">另有 ' + str(configured-len(rows)) + ' 个配置代理未返回记录</div>')
        output.append('</details>')
    if block.get('note'):
        output.append('<div class="sr-note">' + _text(block['note']) + '</div>')
    output.append('<div class="sr-foot">ETF代理不等于全行业 · 相对占优也可能只是抗跌 · 资金流未核验 · 板块观察不是买入信号，不改个股评分与排名。</div></section>')
    return ''.join(output)
