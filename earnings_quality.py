"""Recurring-earnings evidence, separate from ranking and trade geometry.

Vendor reconciliations are not original-filing audits. Unknown profitability
keeps a dated research score, but cannot certify a new entry. No model calls.
"""
import math
import re

VERSION = '2026-10-12-recurring-v1'
# 港股/美股没有扣非字段：用"利润来自主营且有现金支撑"代替（格雷厄姆/斯隆盈利质量），
# 不把标题利润改名为经常性利润，评分口径保持原样，只用于新入场质量检查。
MAX_NONOPERATING_SHARE = 0.20
MIN_CASH_CONVERSION = 0.5
OPERATING_LABEL = '经营利润口径核验（港美股无扣非字段：税前利润≈营业利润、经营现金流覆盖净利润；数据商，非公告原文复核）'


DIVERGENCE_MIN_PROFIT_YOY = 100.0
DIVERGENCE_GAP = 80.0


def divergence(r, recurring_yoy=None):
    """统一触发：归母净利增速远超营收增速（≥100% 且高出营收≥80个百分点）＝疑含一次性因素。
    已接入扣非且扣非增速同样≥100% 时视为主业真实增长，不触发。"""
    p, v = r.get('profit_yoy'), r.get('revenue_yoy')
    if not (number(p) and number(v)) or p < DIVERGENCE_MIN_PROFIT_YOY or p - v < DIVERGENCE_GAP:
        return None
    if number(recurring_yoy) and recurring_yoy >= DIVERGENCE_MIN_PROFIT_YOY:
        return None
    return f'利润质量待核：归母净利同比{p:+.1f}%远超营收{v:+.1f}%，疑含一次性因素'


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
    # 一/三季报数据源常不给扣非ROE/EPS；它们是附加指标，缺失不能让扣非利润与增速整体作废。
    fields = ('reported_profit', 'recurring_profit', 'recurring_profit_yoy')
    valid = valid and all(number(e.get(k)) for k in fields) and number(r.get('parent_profit'))
    if valid:
        valid = math.isclose(e['reported_profit'], r['parent_profit'], rel_tol=1e-6, abs_tol=.01)
    if not valid and r.get('market') in ('港股', '美股'):
        operating = _operating(r)
        if operating:
            flag = divergence(r)
            if flag:
                # 经营口径已核过非经营占比与现金含量：只统一标注，不改变入场判定。
                operating['notes'] = [flag]
                operating['label'] = '利润质量待核（增速背离） · ' + operating['label']
            return out | operating
    if not valid:
        out['gaps'].append('经常性利润、一次性项目及同口径ROE/估值依据待核')
        flag = divergence(r)
        if flag:
            out['gaps'].append(flag)
        if number(r.get('profit_yoy')) and number(r.get('revenue_yoy')):
            if r['profit_yoy'] > 0 and r['revenue_yoy'] < 0:
                out['gaps'].append('营收下降但归母净利增长，需解释差异；尚不能认定为一次性收益')
        return out
    out.update(status='normalized', score_basis='recurring',
               label='扣非/经常性口径已接入（数据商，非公告原文复核）',
               recurring_profit=e['recurring_profit'], recurring_profit_yoy=e['recurring_profit_yoy'],
               recurring_roe_annual=e.get('recurring_roe_annual') if number(e.get('recurring_roe_annual')) else None,
               recurring_eps_annual=e.get('recurring_eps_annual') if number(e.get('recurring_eps_annual')) else None,
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


def _operating(r):
    """港美股经营利润核验；事实不齐返回 None（仍按待核处理）。"""
    o = r.get('operating_check') or {}
    if not (o.get('code') == r.get('code') and o.get('period_end') == r.get('period_end')
            and o.get('currency') == r.get('currency') and bool(r.get('currency'))
            and o.get('basis') == 'operating_profit_reconciled' and o.get('verification') == 'vendor_reconciled'
            and bool(re.fullmatch(r'[0-9a-f]{64}', str(o.get('source_sha256', ''))))
            and str(o.get('source_url', '')).startswith('https://')
            and all(number(o.get(k)) for k in ('operating_profit', 'pretax_profit', 'net_profit'))):
        return None
    op, pretax, net, ocf = o['operating_profit'], o['pretax_profit'], o['net_profit'], o.get('operating_cash_flow')
    gaps = []
    share = conversion = None
    if op <= 0 or pretax <= 0 or net <= 0:
        gaps.append('经营利润、税前利润或净利润不为正，不通过新入场质量检查')
    else:
        share = (pretax - op) / pretax
        if share > MAX_NONOPERATING_SHARE:
            gaps.append(f'税前利润中非经营项目占{math.ceil(share*100)}%（>{MAX_NONOPERATING_SHARE*100:.0f}%），不能认定为经常性利润')
        if not o.get('financial'):
            if number(ocf):
                conversion = ocf / net
                if conversion < MIN_CASH_CONVERSION:
                    gaps.append(f'经营现金流仅为净利润的{math.floor(conversion*100)}%（<{MIN_CASH_CONVERSION*100:.0f}%），利润现金含量不足')
            else:
                gaps.append('经营现金流缺失，利润现金含量待核')
    if number(r.get('revenue_yoy')) and r['revenue_yoy'] < 0:
        gaps.append('主营收入下降，需核实利润增长可持续性')
    return {'status': 'operating_verified', 'score_basis': 'reported_operating_verified',
            'label': OPERATING_LABEL, 'gaps': gaps, 'entry_verified': not gaps,
            'operating_profit': op, 'nonoperating_share_pct': None if share is None else round(share * 100, 1),
            'cash_conversion': None if conversion is None else round(conversion, 2),
            'financial_cash_exempt': bool(o.get('financial')), 'check_scope': o.get('scope'),
            'source_sha256': o['source_sha256'], 'source_url': o['source_url']}


def certified(q):
    """唯一的"利润质量可支撑新入场"判定；入场门与发布校验共用。"""
    q = q or {}
    if q.get('version') != VERSION or q.get('entry_verified') is not True or q.get('gaps'):
        return False
    if q.get('status') == 'normalized' and q.get('score_basis') == 'recurring':
        return number(q.get('recurring_profit')) and q['recurring_profit'] > 0
    if q.get('status') == 'operating_verified' and q.get('score_basis') == 'reported_operating_verified':
        return number(q.get('operating_profit')) and q['operating_profit'] > 0
    return False


def entry_gaps(fundamentals):
    q = (fundamentals or {}).get('earnings_quality') or {}
    if certified(q):
        return []
    return ['利润质量：' + s for s in (q.get('gaps') or ['经常性利润与一次性项目待核'])]


def summary(fundamentals):
    q = (fundamentals or {}).get('earnings_quality') or {}
    label = q.get('label') or '利润质量待核 · 原口径筛选分暂存'
    if number(q.get('recurring_profit_yoy')):
        label += f'；扣非/经常性利润同比{q["recurring_profit_yoy"]:+.1f}%'
    if q.get('gaps') or q.get('notes'):
        label += '；' + '；'.join((q.get('gaps') or []) + (q.get('notes') or []))
    return label
