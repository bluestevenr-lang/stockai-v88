"""Recurring-earnings evidence, separate from ranking and trade geometry.

Vendor reconciliations are not original-filing audits. Unknown profitability
keeps a dated research score, but cannot certify a new entry. No model calls.
"""
import math
import re

VERSION = '2026-10-12-recurring-v1'


def number(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def assess(record):
    r = record or {}
    e = r.get('recurring') or {}
    out = {'version': VERSION, 'status': 'pending', 'entry_verified': False,
           'label': '利润质量待核 · 原口径筛选分暂存', 'gaps': [],
           'score_basis': 'reported_provisional', 'period_end': r.get('period_end')}
    # Same security, period, currency and parent-profit basis; a raw headline
    # growth percentage never proves either a one-off or recurring earnings.
    valid = (e.get('code') == r.get('code') and e.get('period_end') == r.get('period_end')
             and e.get('currency') == r.get('currency') and bool(r.get('currency'))
             and e.get('basis') == 'parent_profit_ex_nonrecurring'
             and e.get('verification') == 'vendor_reconciled'
             and bool(re.fullmatch(r'[0-9a-f]{64}', str(e.get('source_sha256', ''))))
             and str(e.get('source_url', '')).startswith('https://'))
    fields = ('reported_profit', 'recurring_profit', 'recurring_profit_yoy',
              'recurring_roe_annual', 'recurring_eps_annual')
    valid = valid and all(number(e.get(k)) for k in fields) and number(r.get('parent_profit'))
    if valid:
        valid = math.isclose(e['reported_profit'], r['parent_profit'], rel_tol=1e-6, abs_tol=.01)
    if not valid:
        out['gaps'].append('经常性利润、一次性项目及同口径ROE/估值依据待核')
        if number(r.get('profit_yoy')) and number(r.get('revenue_yoy')):
            if r['profit_yoy'] > 0 and r['revenue_yoy'] < 0:
                out['gaps'].append('营收下降但归母净利增长，需解释差异；尚不能认定为一次性收益')
        return out
    out.update(status='normalized', score_basis='recurring',
               label='扣非/经常性口径已接入（数据商，非公告原文复核）',
               recurring_profit=e['recurring_profit'], recurring_profit_yoy=e['recurring_profit_yoy'],
               recurring_roe_annual=e['recurring_roe_annual'], recurring_eps_annual=e['recurring_eps_annual'],
               nonrecurring_net=e['reported_profit']-e['recurring_profit'],
               source_sha256=e['source_sha256'], source_url=e['source_url'])
    if e['recurring_profit'] <= 0:
        out['gaps'].append('扣非/经常性利润不为正，不通过新入场质量检查')
    if (r.get('profit_yoy') or 0) > 0 and e['recurring_profit_yoy'] < 0:
        out['gaps'].append('归母增长但扣非/经常性利润下降，主业增长反证待复核')
    if number(r.get('revenue_yoy')) and r['revenue_yoy'] < 0:
        out['gaps'].append('主营收入下降，需核实利润增长可持续性')
    out['entry_verified'] = not out['gaps']
    return out


def entry_gaps(fundamentals):
    q = (fundamentals or {}).get('earnings_quality') or {}
    if (q.get('version') == VERSION and q.get('status') == 'normalized'
            and q.get('score_basis') == 'recurring' and q.get('entry_verified') is True
            and not q.get('gaps') and number(q.get('recurring_profit')) and q['recurring_profit'] > 0):
        return []
    return ['利润质量：' + s for s in (q.get('gaps') or ['经常性利润与一次性项目待核'])]


def summary(fundamentals):
    q = (fundamentals or {}).get('earnings_quality') or {}
    label = q.get('label') or '利润质量待核 · 原口径筛选分暂存'
    if number(q.get('recurring_profit_yoy')):
        label += f'；扣非/经常性利润同比{q["recurring_profit_yoy"]:+.1f}%'
    if q.get('gaps'):
        label += '；' + '；'.join(q['gaps'])
    return label
