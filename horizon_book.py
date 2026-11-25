"""按周期分档的唯一入场证据表（2026-11-11 用户：3A＝中长期稳定、2A＝中短线稳定、1A＝超短线、Astra＝一个月内回报）。

3A/2A/1A 与 Astra 的买入判定、档位、止损、目标、持有天数全部由本表决定，不另写第二套判断
（shadow_screen.entry_timing 是唯一入口，内部只查这里）。

2026-11-12 用户：凡是给出"现在可以买入"，历史成功概率必须≥50%，否则就是逻辑问题。旧表各规则用 3R/2R 目标，
实测胜率 29–47%（多数交易先碰 5–8% 止损），全部改掉。现表口径（SPAN）：云端 screen-history 日线
（A股2090/港股725/美股2870只，信号日 2026-01-19~11-11），逐日用线上同一套 screen_history.derive 重建结构与形态；
信号日收盘后、次日开盘入场（开盘已≤止损不入场）；A股一字涨停不买、T+1 入场当日不判出场；跳空越过止损/目标按开盘价成交；
同一天既碰止损又碰目标按止损计；到期收盘了结；扣费0.5%。止损仍按各档低点×0.99、深度 stop_min–8%；
目标改为固定百分比、持有天数按规则（出场参数网格：目标3–15%×持有5–40日）。

入选（防过拟合）：以 2026-06-20 为界切前后两半段，两半段**各自**都须 胜率≥53%、均值>0、按日t≥1.0、样本n≥40
（只看单段时，样本内通过的规则仅31%在样本外仍通过）。表内 win＝全样本胜率（净收益>0 的笔数占比）。
宽度格子（CELLS）：市场×宽度×形态×分组的全部信号；排名规则（RANKED）：每天在本市场候选中按规则排序键只取前3。
港股：任何档、任何形态/排序/出场参数都没有两半段同时达标的规则——港股不出买入级，只观察；A股非低宽度时 1A 同理。
quality＝quality_pool.json 优质池（含幸存者偏差）；线上 rs20 分位按当轮候选池计算，是近似口径。非收益保证。
"""
import math

SPAN = ('云端 screen-history 日线（A股2090/港股725/美股2870只，信号日2026-01-19~11-11），信号次日开盘入场、A股T+1、'
        '一字涨停不买、同日碰止损止盈按止损、扣费0.5%；以2026-06-20切前后两半段，两半段各自胜率≥53%才入表')
TIERS = ('3A', '2A', '1A')
STOP_MAX = 8.0
FEE = 0.5
MIN_WIN = 50          # 用户底线：可买规则的历史胜率不得低于50%
ADMIT_WIN = 53        # 入表门槛：前后两半段各自≥53%（给样本外留余量）
# 各档止损口径（与回测同一口径）：止损＝max(对应低点×0.99, 入场×0.92)，止损距离须在[stop_min, 8]%。
# hold/target_pct 为本档缺省（仅用于说明文字与排名席预检）；实际持有天数与目标按命中规则。
PLAN = {
    '3A': {'hold': 40, 'target_pct': 12.0, 'low': 'low20', 'stop_min': 5.0, 'kind': '中长线'},
    '2A': {'hold': 15, 'target_pct': 10.0, 'low': 'low10', 'stop_min': 5.0, 'kind': '中短线'},
    '1A': {'hold': 5, 'target_pct': 5.0, 'low': 'low5', 'stop_min': 2.0, 'kind': '超短线'},
    'Astra': {'hold': 20, 'target_pct': 10.0, 'low': 'low10', 'stop_min': 5.0, 'kind': '一个月内'},
}
SETUP_NAMES = {
    'cross2060': '均线金叉（MA20上穿MA60）', 'new120': '半年新高突破', 'bottom': '低位企稳',
    'oversold': '超跌放量反弹', 'shrink_pull': '缩量回踩', 'breakout': '放量突破', 'rs_trend': '强势趋势',
}
GROUP_NAMES = {'q': '优质池', 'all': '全部'}
BAND_NAMES = {'low': '<35%', 'mid': '35–50%', 'high': '≥50%'}
WEAK_T = 1.5
# 规则：hold 持有交易日、target_pct 固定目标%、score 排序键（排名规则）、band 宽度条件（None＝任意）、
# stats＝全样本(信号数n, 交易日数, 均值%, 胜率%, 按日t, 覆盖交易日占比)、halves＝前后两半段各(n, 交易日数, 均值%, 胜率%, 按日t)。
# knife_ok：回测中"跌破20日线且5日跌≥3%"子集胜率仍≥56%且均值为正，接飞刀否决不适用；
# breach_ok：回测中"20日内同档信号止损已被跌破"子集胜率仍≥50%，原推荐破位否决不适用。
CELLS = {
    ('1A', 'A股', 'low', 'bottom', 'q'): {'hold': 5, 'target_pct': 5.0, 'score': None, 'band': 'low', 'stats': (1457, 55, 0.92, 62, 4.29, 0.28), 'halves': ((523, 16, 0.7, 55, 1.25), (934, 39, 1.04, 66, 4.35)), 'knife_ok': False, 'breach_ok': True},
    ('2A', 'A股', 'low', 'bottom', 'all'): {'hold': 15, 'target_pct': 10.0, 'score': None, 'band': 'low', 'stats': (13937, 46, 3.46, 67, 6.02, 0.23), 'halves': ((5657, 17, 3.67, 68, 3.9), (8280, 29, 3.32, 65, 4.63)), 'knife_ok': True, 'breach_ok': True},
    ('3A', 'A股', 'low', 'bottom', 'all'): {'hold': 40, 'target_pct': 12.0, 'score': None, 'band': 'low', 'stats': (13066, 40, 4.6, 63, 9.22, 0.2), 'halves': ((5657, 17, 4.24, 60, 4.77), (7409, 23, 4.87, 66, 8.53)), 'knife_ok': True, 'breach_ok': True},
    ('Astra', 'A股', 'low', 'bottom', 'all'): {'hold': 20, 'target_pct': 10.0, 'score': None, 'band': 'low', 'stats': (13103, 41, 4.1, 69, 8.53, 0.21), 'halves': ((5657, 17, 4.03, 68, 4.52), (7446, 24, 4.15, 70, 7.62)), 'knife_ok': True, 'breach_ok': True},
    ('2A', '美股', 'low', 'bottom', 'all'): {'hold': 15, 'target_pct': 8.0, 'score': None, 'band': 'low', 'stats': (6869, 26, 3.13, 69, 6.36, 0.13), 'halves': ((4835, 14, 3.82, 73, 5.02), (2034, 12, 1.51, 62, 5.18)), 'knife_ok': True, 'breach_ok': True},
    ('3A', '美股', 'low', 'bottom', 'all'): {'hold': 20, 'target_pct': 8.0, 'score': None, 'band': 'low', 'stats': (6218, 24, 3.43, 70, 7.01, 0.12), 'halves': ((4835, 14, 3.95, 72, 6.13), (1383, 10, 1.6, 62, 4.75)), 'knife_ok': True, 'breach_ok': True},
    ('Astra', '美股', 'low', 'bottom', 'all'): {'hold': 20, 'target_pct': 8.0, 'score': None, 'band': 'low', 'stats': (6218, 24, 3.43, 70, 7.01, 0.12), 'halves': ((4835, 14, 3.95, 72, 6.13), (1383, 10, 1.6, 62, 4.75)), 'knife_ok': True, 'breach_ok': True},
}
MIN_PRICE = {'美股': 5.0, '港股': 1.0}
SCORE_NAMES = {'r20': '20日涨幅从高到低', 'neg_r60': '60日跌幅从深到浅', 'neg_r20': '20日涨幅从低到高'}
RANK_TOP = 3
RANKED = {
    ('1A', 'A股', 'rank', 'bottom', 'all'): {'hold': 5, 'target_pct': 4.0, 'score': 'neg_r60', 'band': 'low', 'stats': (166, 56, 1.26, 69, 2.53, 0.29), 'halves': ((51, 17, 0.98, 61, 2.06), (115, 39, 1.38, 72, 2.0)), 'knife_ok': False, 'breach_ok': True},
    ('2A', 'A股', 'rank', 'shrink_pull', 'all'): {'hold': 15, 'target_pct': 8.0, 'score': 'r20', 'band': None, 'stats': (428, 157, 0.78, 55, 1.84, 0.8), 'halves': ((247, 91, 0.75, 55, 1.31), (181, 66, 0.82, 56, 1.3)), 'knife_ok': False, 'breach_ok': True},
    ('3A', 'A股', 'rank', 'shrink_pull', 'all'): {'hold': 40, 'target_pct': 8.0, 'score': 'r20', 'band': None, 'stats': (370, 137, 0.75, 56, 1.53, 0.7), 'halves': ((247, 91, 0.53, 54, 1.01), (123, 46, 1.17, 59, 1.24)), 'knife_ok': False, 'breach_ok': True},
    ('Astra', 'A股', 'rank', 'shrink_pull', 'all'): {'hold': 20, 'target_pct': 8.0, 'score': 'r20', 'band': None, 'stats': (422, 154, 0.65, 54, 1.55, 0.79), 'halves': ((247, 91, 0.61, 54, 1.06), (175, 63, 0.71, 54, 1.17)), 'knife_ok': False, 'breach_ok': True},
    ('2A', '美股', 'rank', 'bottom', 'q'): {'hold': 15, 'target_pct': 8.0, 'score': 'r20', 'band': None, 'stats': (494, 173, 0.92, 55, 2.51, 0.89), 'halves': ((251, 87, 0.83, 54, 1.47), (243, 86, 1.02, 56, 2.07)), 'knife_ok': False, 'breach_ok': True},
    ('3A', '美股', 'rank', 'cross2060', 'all'): {'hold': 40, 'target_pct': 8.0, 'score': 'r20', 'band': None, 'stats': (434, 153, 1.2, 59, 2.39, 0.79), 'halves': ((278, 98, 1.26, 58, 2.03), (156, 55, 1.08, 60, 1.27)), 'knife_ok': False, 'breach_ok': False},
    ('Astra', '美股', 'rank', 'cross2060', 'all'): {'hold': 10, 'target_pct': 6.0, 'score': 'neg_r20', 'band': None, 'stats': (534, 189, 0.39, 56, 2.02, 0.97), 'halves': ((282, 98, 0.48, 57, 1.41), (252, 91, 0.28, 54, 1.45)), 'knife_ok': False, 'breach_ok': True},
}
NO_RULE = '观察·回测胜率不足50%，不可买'
HK_NOTE = ('港股：7种形态×4种排序×全部出场参数（目标3–15%×持有5–40日）×4档，没有一个规则在前后两半段同时达到胜率≥53%；'
           '港股不出买入级，只观察（' + NO_RULE + '）')


def _f(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def band_of(breadth):
    if not _f(breadth):
        return None
    return 'low' if breadth < 0.35 else 'mid' if breadth < 0.50 else 'high'


def setups(q):
    """信号日（收盘或盘中现价）命中的形态；字段缺失的形态不判（不凭空命中）。"""
    q = q or {}
    e = q.get('structure') or {}
    c = q.get('last')
    if not _f(c) or c <= 0 or c < MIN_PRICE.get(q.get('market'), 0):
        return []
    g = lambda k: e.get(k) if _f(e.get(k)) else None
    chg = q.get('change_pct') if _f(q.get('change_pct')) else g('change_pct')
    ma5, ma10, ma20, ma60, ma120 = g('ma5'), g('ma10'), g('ma20'), g('ma60'), g('ma120')
    vr, r3, r5, r20, r60 = g('volume_ratio20'), g('return3_pct'), g('return5_pct'), g('return20_pct'), g('return60_pct')
    low10, low20 = g('low10'), g('low20')
    ok = lambda *xs: all(x is not None for x in xs)
    out = []
    if ok(ma20, ma60, ma120, r60, g('ma20_prev5'), g('ma60_prev5')) and ma20 > ma60 \
            and e['ma20_prev5'] <= e['ma60_prev5'] and c > ma120 and -15 <= r60 <= 30:
        out.append('cross2060')
    if ok(g('high120_prev'), vr, ma60, g('ma60_prev10'), r20) and e['ma60_prev10'] > 0 and c >= e['high120_prev'] \
            and vr >= 1.3 and ma60 / e['ma60_prev10'] > 1 and r20 < 25:
        out.append('new120')
    if ok(low10, low20, ma5, r3, ma60) and low10 <= low20 * 1.0001 and c > ma5 and r3 > 0 and c < ma60:
        out.append('bottom')
    if ok(r5, chg, vr) and r5 <= -10 and chg >= 3 and vr >= 1.2:
        out.append('oversold')
    if ok(r20, ma10, chg, vr, ma5, ma20) and r20 >= 10 and c > ma10 and -3 <= chg <= 0.5 and vr < 0.7 and ma5 > ma20:
        out.append('shrink_pull')
    if ok(g('previous_high20'), vr, ma20, ma60, chg, r5) and c >= e['previous_high20'] and vr >= 1.5 and ma20 > ma60 \
            and 2 <= chg <= 7 and r5 < 15:
        out.append('breakout')
    if ok(g('rs20_pct'), ma20, ma60, chg) and e['rs20_pct'] >= 0.9 and c > ma20 and ma20 > ma60 and 0 <= chg <= 5:
        out.append('rs_trend')
    return out


def rule(key):
    """规则字典（格子或排名规则）；不在表内返回 None。"""
    key = tuple(key or ())
    return CELLS.get(key) or RANKED.get(key)


def plan(tier, q, key=None, *, asof=None):
    """回测同口径价格计划；止损距离不在[stop_min, 8]%返回 (None, 原因)。
    持有天数与目标按命中规则 key（缺省用本档缺省值，仅供排名席预检止损几何）。"""
    p = PLAN[tier]
    r = rule(key) or {}
    hold, tp = r.get('hold') or p['hold'], r.get('target_pct') or p['target_pct']
    from structural_entry_plan import active, build, FLOORS
    if active(asof) and tier in FLOORS:
        return build(q or {}, tier, hold, p['kind'], p['stop_min'], STOP_MAX, FEE)
    e = (q or {}).get('structure') or {}
    last, low = (q or {}).get('last'), e.get(p['low'])
    if not (_f(last) and _f(low)) or last <= 0 or low <= 0:
        return None, f'缺{p["low"]}，无法定止损'
    stop = round(max(low * 0.99, last * (1 - STOP_MAX / 100)), 4)
    stop_pct = (1 - stop / last) * 100
    if stop_pct < p['stop_min'] - 1e-6:
        return None, f'止损距离{stop_pct:.1f}%<{p["stop_min"]:g}%（回测只取{p["stop_min"]:g}–8%）'
    t1, t2 = round(last * (1 + tp / 100), 4), round(last * (1 + tp * 1.5 / 100), 4)
    lo = round(min(last, max(stop / (1 - p['stop_min'] / 100), last * 0.98)), 4)
    net = (t1 / last - 1) * 100 - FEE
    return {'tier': tier, 'hold': hold, 'kind': p['kind'], 'target_pct': tp, 'entry_range': [lo, round(last, 4)],
            'stop': stop, 'stop_pct': round(stop_pct, 2), 'stop_range': [round(stop * 0.995, 4), stop], 'target': t1,
            'take_profit_range': [t1, t2], 'net_rr': round(net / (stop_pct + FEE), 2),
            'net_target_return_pct': [round(net, 2), round((t2 / last - 1) * 100 - FEE, 2)]}, None


def rr_floor(key):
    """规则自身最差几何的净盈亏比（目标固定%、止损最深8%），留0.02取整余量；回测胜率已按此几何计入。"""
    r = rule(key)
    if not r:
        return None
    return round((r['target_pct'] - FEE) / (STOP_MAX + FEE) - 0.02, 2)


def exempt(key):
    """回测检验过、不改变该规则胜率≥50%的内部否决：返回可豁免的否决名集合（'knife' 接飞刀、'breach' 原推荐破位）。"""
    r = rule(key) or {}
    return {k for k in ('knife', 'breach') if r.get(k + '_ok')}


# 证据表计划替代旧计划后，旧计划留下的这些价位缺项不再适用（3A/2A/1A 与 Astra 同一口径）。
PLAN_GAPS = ('现价未进入计划区间', '入场 / 目标', '止损距离', '止盈 / 止损区间', '入场 / 止盈 / 止损区间',
             '结构净收益风险比', '上方阻力', '年度结构')


def apply_plan(row, pl):
    """把证据表价格计划写进推荐行（入场区间、止损、目标、净盈亏比、持有天数）。"""
    row.update({k: pl[k] for k in ('entry_range', 'stop', 'stop_range', 'target', 'take_profit_range', 'net_rr',
                                   'net_target_return_pct')})
    if pl.get('price_plan_version'):
        for field in ('price_plan_version','price_plan_method','price_plan_scope','stop_anchor','target_evidence'):
            row[field] = pl[field]
        row['price_plan_basis'] = pl['target_evidence']['basis']
        row['price_plan_source_asof'] = pl['target_evidence']['source_asof']
        row['nearest_resistance'] = pl['target_evidence']['resistance']
        row['resistance_levels'] = [row['nearest_resistance']] if row['nearest_resistance'] else []
        row['target_return_pct'] = [round((pl['target']/p-1)*100,2) for p in reversed(pl['entry_range'])]
        row['invalidation'] = f"跌破结构止损 {pl['stop']:g}，原计划失效"
    last = pl['entry_range'][1]
    row['stop_loss_pct'] = [round((pl['stop_range'][0] / last - 1) * 100, 2), round((pl['stop'] / last - 1) * 100, 2)]
    row['missing'] = [m for m in row.get('missing') or [] if not str(m).startswith(PLAN_GAPS)]
    row['max_sessions'] = pl['hold']
    return row


def stats(key):
    """规则统一统计：{'n','days','mean','win','t','rt','halves','kind', ...}；不在表内返回 None。
    rt＝两半段按日t的较小者，多处命中与排序都比 rt（再比胜率）。"""
    key = tuple(key or ())
    r = rule(key)
    if not r:
        return None
    n, days, mean, win, t, cov = r['stats']
    (a, b) = r['halves']
    return {'n': n, 'days': days, 'mean': mean, 'win': win, 't': t, 'coverage': cov, 'rt': min(a[4], b[4]),
            'win_min': min(a[3], b[3]), 'halves': r['halves'], 'hold': r['hold'], 'target_pct': r['target_pct'],
            'kind': 'rank' if key in RANKED else 'cell'}


def live(key):
    """规则是否启用：须在表内且两半段胜率都≥ADMIT_WIN、均值>0。返回 (启用, 说明)。"""
    st = stats(key)
    if not st:
        return False, '不在证据表'
    (a, b) = st['halves']
    if min(a[3], b[3]) < ADMIT_WIN or min(a[2], b[2]) <= 0:
        return False, f'两半段胜率{a[3]}%/{b[3]}%未同时≥{ADMIT_WIN}%'
    return True, f'前后两半段胜率{a[3]}%/{b[3]}%'


def weak(cell):
    """证据弱（轻仓）：两半段按日t较小者<1.5。"""
    st = stats(cell)
    return bool(st) and st['rt'] < WEAK_T


def _rule_text(key):
    st = stats(key)
    (a, b) = st['halves']
    return (f'{key[0]}口径（{st["hold"]}日持有、+{st["target_pct"]:g}%目标、止损{PLAN[key[0]]["stop_min"]:g}–8%）'
            f'n={st["n"]}（{st["days"]}个交易日、覆盖{st["coverage"]:.0%}）胜率{st["win"]}%、均{st["mean"]:+.2f}%，'
            f'前后两半段胜率{a[3]}%/{b[3]}%、均{a[2]:+.2f}%/{b[2]:+.2f}%，按日t={st["t"]:g}'
            + ('（证据弱，轻仓）' if weak(key) else '') + f'；{SPAN}；非收益保证')


def rank_text(key):
    key = tuple(key)
    tier, m, _, setup, grp = key
    r = RANKED[key]
    cond = f'宽度{BAND_NAMES[r["band"]]}时' if r['band'] else ''
    return (f'排名席回测：{m}{cond}{SETUP_NAMES[setup]}×{GROUP_NAMES[grp]}，每天按{SCORE_NAMES[r["score"]]}取前{RANK_TOP}，'
            + _rule_text(key))


def cell_text(key):
    key = tuple(key or ())
    if key in RANKED:
        return rank_text(key)
    if key not in CELLS:
        return '无回测规则'
    tier, m, band, setup, grp = key
    return f'回测：{m}{SETUP_NAMES[setup]}×宽度{BAND_NAMES[band]}×{GROUP_NAMES[grp]}，' + _rule_text(key)


def _score(kind, e):
    if kind == 'r20':
        v = e.get('return20_pct')
    elif kind == 'neg_r20':
        v = -e['return20_pct'] if _f(e.get('return20_pct')) else None
    elif kind == 'neg_r60':
        v = -e['return60_pct'] if _f(e.get('return60_pct')) else None
    else:
        v = None
    return v if _f(v) else None


def rank_pool(rows, ok=None, breadth=None, *, asof=None):
    """排名席：每条 RANKED 规则在本市场当轮候选中按规则排序键取前 RANK_TOP（价格计划须成立、ok(q) 为真；
    规则带宽度条件时，breadth＝{市场: 宽度} 须满足）。结果写入 structure['ranked']＝{档位: [{'key','rank','of','score'}]}；
    每轮整体重算（先清旧标记）。"""
    by, found = {}, {}
    for q in rows or []:
        e = q.get('structure')
        if not isinstance(e, dict):
            continue
        e.pop('ranked', None)
        if q.get('market') not in {k[1] for k in RANKED} or (ok and not ok(q)):
            continue
        found[id(q)] = setups(q)
        by.setdefault(q['market'], []).append(q)
    for key, r in RANKED.items():
        tier, m, _, setup, grp = key
        if r['band'] and breadth is not None and band_of((breadth or {}).get(m)) != r['band']:
            continue
        pool = []
        for q in by.get(m, []):
            if setup not in found[id(q)] or (grp == 'q' and not _in_quality(q.get('code'))):
                continue
            sc = _score(r['score'], q['structure'])
            if sc is None or plan(tier, q, key, asof=asof)[0] is None:
                continue
            pool.append((sc, str(q.get('code')), q))
        pool.sort(key=lambda x: (-x[0], x[1]))
        for k, (sc, _, q) in enumerate(pool[:RANK_TOP], 1):
            q['structure'].setdefault('ranked', {}).setdefault(tier, []).append(
                {'key': list(key), 'rank': k, 'of': len(pool), 'score': round(sc, 2)})
    return rows


def match(q, breadth, tiers=TIERS, *, asof=None):
    """按档位查表：返回 {'band', 'setups', 'hits': {tier: 命中}, 'misses': {tier: 原因}}。
    候选来源两类、同一张表：①宽度格子（CELLS）；②排名席（RANKED，须当轮已由 rank_pool 排进前3、且满足规则宽度条件）。
    同一档多处命中取两半段按日t较小者（rt）最高者，再比胜率。"""
    q = q or {}
    m, band = q.get('market'), band_of(breadth)
    e = q.get('structure') or {}
    found = setups(q)
    quality = _in_quality(q.get('code'))
    groups = ('q', 'all') if quality else ('all',)
    hits, misses = {}, {}
    for tier in tiers:
        ranked_tags, cands = {}, []
        if band is not None:
            cands = [k for k in ((tier, m, band, s, g) for s in found for g in groups) if k in CELLS and live(k)[0]]
        for x in (e.get('ranked') or {}).get(tier) or []:
            k = tuple(x.get('key') or ())
            r = RANKED.get(k)
            if r and k[0] == tier and k[1] == m and k[3] in found and (not r['band'] or r['band'] == band):
                cands.append(k)
                ranked_tags[k] = x
        if not cands:
            mine = [k for k in list(CELLS) + list(RANKED) if k[0] == tier and k[1] == m]
            if not mine:
                misses[tier] = HK_NOTE if m == '港股' else f'{m}{tier}：{NO_RULE}'
                continue
            usable = [k for k in mine if rule(k)['band'] in (None, band)]
            if band is None and not [k for k in usable if rule(k)['band'] is None]:
                misses[tier] = '市场宽度缺失'
                continue
            if not usable:
                misses[tier] = (f'{m}宽度{BAND_NAMES[band]}时{tier}没有胜率达标的规则（达标规则只在宽度'
                                + '、'.join(sorted({BAND_NAMES[rule(k)["band"]] for k in mine})) + f'时启用）：{NO_RULE}')
                continue
            parts = ['市场宽度缺失（宽度格子不可用）'] if band is None else []
            rk = [k for k in usable if k in RANKED and k[3] in found]
            if rk:
                parts.append(f'{SETUP_NAMES[rk[0][3]]}当轮排名未进本市场前{RANK_TOP}')
            else:
                parts.append('未命中胜率达标形态（' + '、'.join(sorted({SETUP_NAMES[k[3]] + '·' + GROUP_NAMES[k[4]] +
                                                                  ('·排名前3' if k in RANKED else '') for k in usable})) + '）')
            misses[tier] = '；'.join(parts)
            continue
        key = max(cands, key=lambda k: (stats(k)['rt'], stats(k)['win']))
        pl, why = plan(tier, q, key, asof=asof)
        if not pl:
            misses[tier] = f'{SETUP_NAMES[key[3]]}命中但{why}'
            continue
        st = stats(key)
        (a, b) = st['halves']
        odds = f'旧出口形态回测胜率{st["win"]}%（前后两半段{a[3]}%/{b[3]}%）、均{st["mean"]:+.2f}%、{st["hold"]}日+{st["target_pct"]:g}%目标'
        if key in RANKED:
            tag = ranked_tags[key]
            reason = (f'{SETUP_NAMES[key[3]]}（{GROUP_NAMES[key[4]]}）当轮{m}第{tag["rank"]}/{tag["of"]}名'
                      f'（按{SCORE_NAMES[RANKED[key]["score"]]}）→{tier}{PLAN[tier]["kind"]}（{odds}）')
        else:
            reason = f'{SETUP_NAMES[key[3]]}（{GROUP_NAMES[key[4]]}）×宽度{breadth:.0%}→{tier}{PLAN[tier]["kind"]}（{odds}）'
        hits[tier] = {'tier': tier, 'cell': list(key), 'setup': key[3], 'group': key[4], 'plan': pl,
                      'weak': weak(key), 'evidence': cell_text(key), 'mean': st['mean'], 't': st['t'], 'rt': st['rt'],
                      'win': st['win'], 'win_halves': [a[3], b[3]], 'exempt': sorted(exempt(key)),
                      'source': st['kind'], 'rank': ranked_tags.get(key), 'reason': reason}
    return {'band': band, 'setups': found, 'hits': hits, 'misses': misses}


def best(match_result, tiers=TIERS):
    """多档同时命中取周期最长、最稳的一档（3A>2A>1A）。"""
    for t in tiers:
        if t in match_result['hits']:
            return match_result['hits'][t]
    return None


def active(market, breadth, tiers=TIERS + ('Astra',)):
    """当前启用的规则：本宽度的格子＋本市场排名席规则（宽度条件须满足）。"""
    band = band_of(breadth)
    out = []
    for k, r in list(CELLS.items()) + list(RANKED.items()):
        if k[1] != market or k[0] not in tiers or (r['band'] and r['band'] != band) or not live(k)[0]:
            continue
        src = 'rank' if k in RANKED else 'cell'
        out.append({'tier': k[0], 'setup': SETUP_NAMES[k[3]], 'group': GROUP_NAMES[k[4]] + ('·每日前3' if src == 'rank' else ''),
                    'weak': weak(k), 'win': r['stats'][3], 'evidence': cell_text(k), 'cell': list(k), 'source': src})
    return out


def _in_quality(code):
    try:
        from shadow_screen import quality_codes
        return code in quality_codes()
    except Exception:
        return False
