"""Bounded report projection of the same observation shown on the web.

No fetching, ranking of securities, model calls, or trade authority. Reports
freeze this text with the rest of the presend draft; retries never rebuild it.
"""
from datetime import datetime, timezone
import math
from exchange_sessions import latest_completed
from sector_rotation import VERSION, BENCHMARKS


def _valid(row, expected, market):
    return (row.get('status') == 'current' and row.get('source_asof') == expected
            and row.get('benchmark_asof') == expected
            and row.get('market') == market
            and row.get('benchmark_symbol') == BENCHMARKS[market][0]
            and row.get('basis') in ('aligned_daily_closes', 'same_session_summary')
            and all(isinstance(row.get(k), (int, float)) and not isinstance(row.get(k), bool)
                    and math.isfinite(row[k]) for k in ('rs5_pp', 'rs20_pp', 'sector_chg5d')))


def digest_lines(doc, markets, now=None):
    now = now or datetime.now(timezone.utc)
    lines = ['🧭 板块相对强弱｜观察用途']
    for market in markets:
        block = (doc.get('markets') or {}).get(market) or {}
        expected = latest_completed(market, now).isoformat()
        rows = [r for r in block.get('rows', []) if _valid(r, expected, market)] if (
            doc.get('version') == VERSION and block.get('market') == market
            and (block.get('benchmark') or {}).get('symbol') == BENCHMARKS[market][0]) else []
        coverage = block.get('coverage') or {}
        total = coverage.get('expected', {'A股': 10, '港股': 3, '美股': 11}.get(market, 0))
        benchmark = (block.get('benchmark') or {}).get('name') or '基准待核'
        lines.append(f'{market}｜截至{expected}完整日线｜同日可比{len(rows)}/{total}｜相对{benchmark}')
        if not rows:
            dates = sorted({str(r.get('source_asof')) for r in block.get('rows', []) if r.get('source_asof')})
            lines.append('  数据待补齐，不能判作没有轮动。' + (' 原行情：' + '/'.join(dates) if dates else ''))
            continue
        # Fixed window order and tie-breaking; maximum three observations per market.
        priority = {'持续占优': 0, '短期改善': 1, '短期转弱': 2, '持续落后': 3, '中性': 4}
        ranked = sorted(rows, key=lambda r: (priority.get(r.get('quadrant'), 9), -r['rs5_pp'], r['symbol']))
        chosen = ranked[:2]
        fading = sorted([r for r in rows if r.get('quadrant') in ('短期转弱', '持续落后')], key=lambda r: (r['rs5_pp'], r['symbol']))
        if fading and fading[0] not in chosen: chosen.append(fading[0])
        for row in chosen:
            transition = row.get('transition') or {}
            change = ('；跨日变化已确认' if transition.get('status') == 'confirmed' else
                      '；状态变化待确认' if transition.get('status') == 'pending_confirmation' else '')
            absolute = '；自身5日下跌' if row['sector_chg5d'] < 0 else ''
            lines.append(f"  {row['name']}·{row['quadrant']}：超额5日{row['rs5_pp']:+.2f} / 20日{row['rs20_pp']:+.2f}百分点{absolute}{change}")
        if market == '港股': lines.append('  港股为风格ETF代理，非完整行业覆盖。')
    lines.append('ETF价格相对强弱≠资金流向或买入信号；资金流未核验。')
    return lines
