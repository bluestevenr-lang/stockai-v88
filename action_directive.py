"""action_directive.py — 操作指令层：每个席位最后只给一条可执行指令。

为什么需要这一层：入场门、多重核查、环境门、财报窗各自给出"不可买"的理由，
但用户拿到的是一堆"等回调/重点观察/待核"——没有一条能直接下单。这里不新增
任何选股阈值，只把已经算好的价格计划、解禁价、财报日翻译成五种指令之一：

  立即买入     现在就满足全部条件：开盘后限价不高于入场上沿买入。
  限价买入     只差价格或报价刷新：挂限价单（不高于解禁价/入场上沿），当日有效。
  挂单买入     需要回踩：在明确价位挂限价单，N 个交易日有效，到期未成交作废。
  财报后买入   已核财报日在静默窗内或财务数据落后：财报落地后按条件买入。
  不买         硬伤（利润质量不过、年度结构非上行、公告风险、大盘逆风非领头股…），写明理由。

经典依据（只用于解释，不改变任何门槛）：
  - 欧奈尔 CAN SLIM / 米奈尔维尼 VCP：不追离均线过远的票，回踩 20 日线附近买；
  - 范·撒普《通向财务自由之路》：先定止损，再按"单笔风险=账户1%"倒推仓位；
  - 利弗莫尔：试探仓先行（逆风/财报窗半仓），对了再加；
  - 温斯坦阶段分析：年度下行（第四阶段）不建仓；
  - 达瓦斯箱体：年度震荡只在回踩箱体下部半仓建仓，不在箱顶追；
  - 格雷厄姆安全边际：利润质量与现金含量不过，不因价格便宜而买。

内部否决（guard，2026-10-22 起）：推荐违背预警不再上页面/飞书，而是回灌到这里——
  - 原推荐已破位（预警🔴/🟠）：不补仓、不抄底，收盘重新站上60日线（行业弱时还须行业转强）才成交；
  - 接飞刀：跌破20日线且5日跌≥3%，收盘站回20日线才成交。
  两者都把"立即/限价买入"改为"挂单买入（条件单）"，席位排序随之后移、由下一候选补位，不空席。
  2026-11-12：证据表买入级行（astra_regime.band=horizon）按 horizon_book.exempt 豁免——全市场回测中
  命中破位/接飞刀子集的规则胜率仍≥50%的（2A/3A/Astra 低宽度低位企稳等），只披露、不拦截；其余照拦。
  依据：9/01–10/22 推荐实盘回放（破位后再推荐 n=15 胜率13%、接飞刀 n=17 胜率18%，其余45%），
  接飞刀另经 2025-07~2026-10 全市场趋势股 14.8 万笔回测（A股、港股为最差档，美股不明显）。
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta

VERSION = 'action-directive-2026-11-16-v8-entry-progression'
ACTIONS = ('立即买入', '限价买入', '挂单买入', '财报后买入', '不买')
ACCOUNT_RISK_PCT = 1.0       # 3A 标准仓：单笔最大亏损 = 账户 1%（范·撒普）
MAX_POSITION_PCT = 25.0      # 单票仓位上限
FEE_PCT = 0.5
PULLBACK_SESSIONS_3A = 3
ASTRA_RISK_USD = 62.5

# 硬伤：任何价格都不该买（不是"等一等"）。只认已经写进 missing 的现成结论。
HARD_GAP_MARKERS = ('不能认定', '利润现金含量不足', '不为正', '近期公告含风险事项',
                    '年度结构为下行', '低于Astra质量门槛', '统一分级门未过')
# 年度震荡不是硬伤：按达瓦斯箱体，只在回踩箱体下部时半仓建仓，不在箱顶追。
RANGE_MARKER = '年度结构为震荡'
SAME_DAY = '当日有效'
STALE_MARKERS = ('最新交易日报价', '入场前更新报价', '最新收盘日线待更新')
# 软缺口：报价/日线刷新、复核类，限价单本身就能消化（成交价不高于计划价）。
SOFT_GAP_PREFIX = ('入场前更新报价', '盘中报价', '本日研究续列', '收盘价有效', '最新收盘日线待更新',
                   '多重核查', '利润质量：经常性利润、一次性项目', '当前日线结构', '现价未进入原计划区间',
                   '原计划在剩余', '原计划当前净盈亏比')
CANON = {
    '立即买入': '全部入场门已过：按计划入场上沿限价买入，止损先于买入设定（范·撒普）',
    '限价买入': '门槛不降，只按限价计量：成交价不高于此价即同时满足净空间与收益风险比（格雷厄姆安全边际）',
    '挂单买入': '不追高，回踩到计划价才买（欧奈尔/米奈尔维尼：离20日线过远不追）',
    '财报后买入': '财报是未定价事件，不在事件前下注；落地后确认再买（ISRG 教训）',
    '不买': '硬伤不因价格便宜而买（格雷厄姆）；年度下行结构不建仓（温斯坦第四阶段）',
}

# Stable precedent identifiers. The wording above may be sharpened over time; an
# id never changes meaning, so a downstream reviewer can cite one decision basis
# across versions. Adding a rule adds an id; repurposing an id is not allowed.
CANON_ID = {
    '立即买入': 'CANON-ENTRY-01',
    '限价买入': 'CANON-ENTRY-02',
    '挂单买入': 'CANON-ENTRY-03',
    '财报后买入': 'CANON-ENTRY-04',
    '不买': 'CANON-REJECT-01',
}


def _num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x) and x > 0


def _fmt(x):
    if not _num(x):
        return '—'
    return f'{x:.3f}'.rstrip('0').rstrip('.') if x < 100 else f'{x:.2f}'.rstrip('0').rstrip('.')


def hard_gaps(row):
    gaps = list(row.get('missing') or row.get('gaps') or [])
    return [g for g in gaps if any(m in str(g) for m in HARD_GAP_MARKERS)]


def _risk_pct(price, stop):
    if not (_num(price) and _num(stop)) or stop >= price:
        return None
    return (1 - stop / price) * 100 + FEE_PCT


def size_3a(price, stop, half=False):
    """范·撒普：仓位% = 账户风险% ÷ 止损距离%，上限 25%；试探仓减半。"""
    risk = _risk_pct(price, stop)
    if risk is None:
        return None, '仓位待核（止损无效）'
    pct = min(MAX_POSITION_PCT, ACCOUNT_RISK_PCT / risk * 100) * (0.5 if half else 1)
    label = '半仓试探' if half else '标准仓'
    return round(pct, 1), (f'{label}：约占账户 {pct:.1f}%（止损距离 {risk:.1f}%，'
                           f'单笔最大亏损≈账户{ACCOUNT_RISK_PCT * (0.5 if half else 1):g}%）')


def size_astra(price, stop, half=False):
    risk = _risk_pct(price, stop)
    if risk is None:
        return None, '仓位待核（止损无效）'
    usd = ASTRA_RISK_USD * (0.5 if half else 1)
    notional = usd / (risk / 100)
    return round(notional), (f'{"半仓试探" if half else "标准仓"}：约 {notional:,.0f} 美元等值'
                             f'（止损距离 {risk:.1f}%，单笔最大亏损≈{usd:g} 美元）')


def _add_sessions(start, n):
    d = start
    while n > 0:
        d += timedelta(days=1)
        if d.weekday() < 5:
            n -= 1
    return d


def _today(now):
    if isinstance(now, datetime):
        from zoneinfo import ZoneInfo
        return now.astimezone(ZoneInfo('Asia/Shanghai')).date() if now.tzinfo else now.date()
    if isinstance(now, date):
        return now
    return datetime.now().date()


def pullback_price(last, stop, gate=None, entry_range=None, unlock=None):
    """回踩挂单价：20日线与"现价−2ATR"取高者，且至少低于现价0.5ATR（米奈尔维尼回踩买点）。
    缺 MA20/ATR 时退回计划入场下沿；不得低于止损上方 0.5ATR（否则一成交就贴着止损）。"""
    gate = gate or {}
    ma20, atr = gate.get('ma20'), gate.get('atr14')
    if _num(ma20) and _num(atr) and _num(last):
        price = min(last - 0.5 * atr, max(ma20, last - 2 * atr))
        floor = (stop + 0.5 * atr) if _num(stop) else 0
    else:
        price = entry_range[0] if entry_range and _num(entry_range[0]) else None
        floor = stop * 1.01 if _num(stop) else 0
    if _num(unlock) and _num(price):
        price = min(price, unlock)
    if not _num(price) or price <= floor:
        return None
    return round(price, 3)


KNIFE_RET5_PCT = -3.0
BREACH_LEVELS = ('红', '橙')
WEAK_QUADS = ('短期转弱', '持续落后')
GUARDED = ('立即买入', '限价买入', '挂单买入')


def _exempt(row):
    """证据表买入级行可豁免的否决（horizon_book.exempt：回测检验该否决不会把规则胜率拉到50%以下）。"""
    reg = row.get('astra_regime') or {}
    if reg.get('band') != 'horizon' or reg.get('grade') != 'buy':
        return set()
    from horizon_book import exempt
    return exempt(reg.get('cell'))


def _vetoes(row):
    """全部内部否决：[(名, 理由, 成交条件)]。只读行上已有字段，不另取数据。"""
    out = []
    last = row.get('last')
    gate = row.get('entry_gate') or {}
    b = row.get('promise_breach') or {}
    if b.get('level') in BREACH_LEVELS:
        ma60 = gate.get('ma60') if _num(gate.get('ma60')) else row.get('trend_exit_ma60')
        weak = b.get('quad') in WEAK_QUADS
        if not (_num(ma60) and _num(last) and last >= ma60 and not weak):
            dd = f"，较推荐价{b['dd']:+.1f}%" if _num(b.get('dd')) else ''
            need = f'收盘重新站上60日线（{_fmt(ma60)}）' if _num(ma60) else '收盘重新站上60日线'
            out.append(('breach', f"原推荐已破位（{b.get('cause') or '跌破计划'}{dd}）：不补仓、不抄底",
                        need + ('且所属行业转强' if weak else '') + '后才成交'))
    ma20, r5 = gate.get('ma20'), gate.get('return5_pct')
    r5 = r5 if isinstance(r5, (int, float)) and not isinstance(r5, bool) and math.isfinite(r5) else None
    if _num(ma20) and _num(last) and r5 is not None and last < ma20 and r5 <= KNIFE_RET5_PCT:
        out.append(('knife', f'接飞刀：跌破20日线且5日{r5:+.1f}%', f'收盘站回20日线（{_fmt(ma20)}）后才成交'))
    return out


def guard(row):
    """内部否决：返回 (理由, 成交条件)；None = 不否决。证据表规则已回测豁免的否决只披露（见 guard_notes）。"""
    free = _exempt(row)
    for kind, why, cond in _vetoes(row):
        if kind not in free:
            return why, cond
    return None


def guard_notes(row):
    """被证据表豁免、只披露不拦截的否决理由。"""
    free = _exempt(row)
    return [f'{why}（披露：该规则回测含此情形胜率仍≥50%，不拦截）' for kind, why, _ in _vetoes(row) if kind in free]


def _guarded(d, row, valid):
    """把被否决的买入指令改为条件单（价格、仓位、止损、目标不变），挂单类只追加成交条件。"""
    g = guard(row) if d.get('action') in GUARDED else None
    if not g:
        notes = guard_notes(row) if d.get('action') in GUARDED else []
        if notes:
            d = dict(d)
            d['why'] = list(dict.fromkeys(list(d.get('why') or []) + notes))
            d['guard_disclosed'] = notes
        return d
    why, cond = g
    out = _directive('挂单买入', price=d.get('price'), order='条件单',
                     valid=d.get('valid_until') if d['action'] == '挂单买入' and d.get('valid_until') else valid,
                     size=d.get('size'), size_pct=d.get('size_pct'), stop=d.get('stop'), target=d.get('target'),
                     exit_rule=d.get('exit') or '', why=[why] + list(d.get('why') or []),
                     condition=_join(cond, d.get('condition') or ''), module=d.get('module'),
                     checks=d.get('checks') or ())
    out['guard'] = why
    return out


def _directive(action, *, price=None, order=None, valid=None, size=None, size_pct=None, stop=None,
               target=None, exit_rule='', why=(), condition='', module='3A', checks=()):
    why = [w for w in dict.fromkeys(str(x) for x in why if x)]
    # 3A sizes as a percentage of the account; Astra sizes as a USD notional.
    # Two different units must never share one field name, or a reader (and the
    # Muse directive channel) will read 774 USD as 774% of the account.
    # 3A/2A/1A 同属策略看板，仓位都是账户百分比；只有 Astra 是美元金额。
    account_pct = size_pct if module in ('3A', '2A', '1A') else None
    notional_usd = size_pct if module not in ('3A', '2A', '1A') else None
    d = {'version': VERSION, 'module': module, 'action': action, 'order': order,
         'price': round(price, 4) if _num(price) else None, 'valid_until': valid,
         'size': size, 'size_pct': account_pct, 'size_notional_usd': notional_usd,
         'size_unit': 'account_pct' if module in ('3A', '2A', '1A') else 'usd_notional',
         'stop': round(stop, 4) if _num(stop) else None,
         'target': round(target, 4) if _num(target) else None, 'exit': exit_rule,
         'condition': condition, 'why': why[:4], 'checks': list(checks)[:3],
         'canon': CANON[action], 'canon_id': CANON_ID[action]}
    parts = [action]
    if action != '不买':
        if _num(price):
            parts.append(f'{order or "限价"} ≤ {_fmt(price)}')
        if valid:
            parts.append(valid if valid == SAME_DAY else f'有效至 {valid}')
        if size:
            parts.append(size.split('：')[0])
        if _num(stop):
            parts.append(f'止损 {_fmt(stop)}')
        if _num(target):
            parts.append(f'目标 {_fmt(target)}')
        if condition:
            parts.append(condition)
    else:
        parts.append('；'.join(why[:2]) or '条件不足')
    d['text'] = ' · '.join(parts)
    return d


HEADWIND_COND = '大盘逆风：须指数收盘站回20日线或本股转为趋势领头股（站上60日线且强于大盘）后才成交，否则撤单'
MARKET_PREFIX = ('大盘逆风', '逆风试仓')
ROTATION_KEY = '板块'
PRICE_KEYS = ('计划退化', '追高熔断', '盈亏比门', '财报窗内', '追过头', '净空间', '净盈亏比')


def classify(reasons):
    """把实时门理由分成：大盘环境 / 板块轮转 / 价格 / 其他（硬伤）。"""
    market, rotation, price, other = [], [], [], []
    for x in reasons or []:
        x = str(x)
        if x.startswith(MARKET_PREFIX):
            market.append(x)
        elif ROTATION_KEY in x and '转弱' in x:
            rotation.append(x)
        elif any(k in x for k in PRICE_KEYS):
            price.append(x)
        else:
            other.append(x)
    return market, rotation, price, other


def unlock_price(o, min_space=5.0, min_rr=2.0):
    """与 strategy_board.unlock_price 同式（净空间与净盈亏比同时达标的最高买入价）；
    旧版桌面核心无此函数时用本地同式，不另立口径。"""
    try:
        from strategy_board import unlock_price as board_unlock
        return board_unlock(o, min_space, min_rr)
    except ImportError:
        t, s = o.get('first_objective'), o.get('stop')
        if not (_num(t) and _num(s)) or t <= 0 or s <= 0 or t <= s:
            return None
        fee = 0.5
        p = math.floor(min(t / (1 + (min_space + fee) / 100),
                           (t + min_rr * s) / ((1 + min_rr) * (1 + fee / 100))) * 0.999 * 1000) / 1000
        return p if p > s else None


LIMIT_REACH_PCT = 1.0


def _meets(price, stop, target, space_bar=5.0, rr_bar=2.0):
    """净空间与净收益风险比同时达标（与入场门同一算式，含 0.5% 往返费用）。"""
    if not (_num(price) and _num(stop) and _num(target)) or not stop < price < target:
        return False
    reward = (target / price - 1) * 100 - FEE_PCT
    return reward >= space_bar and reward / ((1 - stop / price) * 100 + FEE_PCT) >= rr_bar - 1e-9


def _drop(p, last):
    return (f'现价 {_fmt(last)} 已不高于此价，挂单即可成交' if p >= last
            else f'现价 {_fmt(last)}，需回落 {(1 - p / last) * 100:.1f}%')


def _join(*conds):
    return '；'.join(c for c in conds if c)


def _exit_3a(tier, ma60):
    if tier != '3A':
        return '触及止损或目标离场；超出持有期按收盘价了结'
    return (f'收盘跌破60日线（{_fmt(ma60)}）或触及止损离场；到目标先卖一半，余仓沿60日线持有'
            if _num(ma60) else '触及止损离场；到目标先卖一半，余仓沿60日线持有')


def _decide_3a(row, *, now=None):
    """3A/2A/1A 单行指令（中长期：收盘跌破60日线或止损离场，目标处减半、余仓用60日线跟踪）。"""
    today = _today(now)
    last = row.get('last'); er = row.get('entry_range') or []
    gate = row.get('entry_gate') or {}; lg = row.get('live_gate') or {}
    er_state = row.get('earnings_risk') or {}
    es = er_state.get('state') or gate.get('earnings_state')
    stop = row.get('stop') if _num(row.get('stop')) else gate.get('stop')
    target = row.get('target') if _num(row.get('target')) else gate.get('first_objective')
    tier = row.get('strategy_tier') or '3A'
    kw = dict(stop=stop, target=target, exit_rule=row.get('exit_rule_text') or _exit_3a(tier, row.get('trend_exit_ma60')), module=tier)
    hard = hard_gaps(row)
    if hard:
        return _directive('不买', why=hard, module=tier)
    if not (_num(last) and _num(stop)) or stop >= last:
        return _directive('不买', why=['缺可核实的中期价格计划（止损/入场），给不出可执行价'], module=tier)
    if gate.get('entry_state') == '等财报' or es in ('blackout', 'stale'):
        when = er_state.get('earnings_date')
        cond = (f'财报（{when}）后首个交易日收盘不破止损且站稳20日线，次日按计划价买入；否则作废'
                if when else '新报告期财报更新入库后，系统重评过入场门即按新计划买入')
        return _directive('财报后买入', why=er_state.get('reasons') or gate.get('reasons') or [],
                          condition=cond, valid=when, **kw)
    missing = [str(m) for m in (row.get('missing') or [])]
    stale = ['报价待更新：开盘前核价，高于限价不追'] if any(m.startswith(STALE_MARKERS) for m in missing) else []
    soft = stale + [m for m in missing if m.startswith(SOFT_GAP_PREFIX) and not m.startswith(STALE_MARKERS)]
    in_range = any(RANGE_MARKER in m for m in missing)
    half_window = es == 'window'
    evidence_half = row.get('fundamentals_state') == 'pending' or (row.get('ladder') or {}).get('fundamentals') == 'pending' or (row.get('ladder') or {}).get('weak') is True
    upper = er[1] if len(er) == 2 and _num(er[1]) else last * 1.005
    pend_valid = _add_sessions(today, PULLBACK_SESSIONS_3A).isoformat()
    window_note = ['财报事件窗内：半仓'] if half_window else []

    if row.get('buyable'):
        half = bool(lg.get('trend_trial') or lg.get('level') == '试仓' or half_window or in_range or evidence_half)
        why = [lg.get('note')] if lg.get('rescued') else ['全部入场门已过']
        # 只推可立刻买/1-3日内可买：财报窗外不再用 5%/2 的安全边际压价，目标在买价之上即按上沿买入。
        space_bar, rr_bar = (8.0, 2.5) if half_window else (0.0, 0.0)
        price = upper
        if _num(target) and not _meets(upper, stop, target, space_bar, rr_bar):
            # 计划上沿处净空间/净盈亏比不达标（如 2A 放宽了几何门）：门槛不降，只把价格降到解禁价。
            unlock = unlock_price({'first_objective': target, 'stop': stop}, space_bar, rr_bar)
            if not _num(unlock) or unlock <= stop:
                return _directive('不买', why=['计划上沿处净盈亏比不足且到止损前没有达标买点'], module=tier)
            price = min(upper, unlock)
            why = why + [f'计划上沿 {_fmt(upper)} 处净盈亏比不足{rr_bar:g}，按解禁价 {_fmt(price)} 计量']
            if last > price:
                pct, size = size_3a(price, stop, half)
                if price >= last * (1 - LIMIT_REACH_PCT / 100):
                    return _directive('限价买入', price=price, order='限价', valid=SAME_DAY, size=size,
                                      size_pct=pct, why=why + window_note, checks=soft,
                                      condition='开盘价高于限价不追', **kw)
                return _directive('挂单买入', price=price, order='限价', valid=pend_valid, size=size,
                                  size_pct=pct, why=why + window_note, checks=soft,
                                  condition=f'回调到此价才成交（{_drop(price, last)}）', **kw)
        pct, size = size_3a(price, stop, half)
        return _directive('立即买入', price=price, order='限价', valid=SAME_DAY, size=size, size_pct=pct,
                          why=why + window_note, checks=soft, **kw)

    if not gate:
        return _directive('不买', why=[*missing[:1], '本轮未经入场门评估（追高/盈亏比/财报窗未核），不给买价'], module=tier)
    market, rotation, price_r, other = classify(lg.get('reasons') if lg.get('demoted') else [])
    if other:
        return _directive('不买', why=other, module=tier)
    headwind = bool(market) or (gate.get('env') == 'headwind' and not lg.get('rescued'))
    h_cond = HEADWIND_COND if headwind else ''
    half = half_window or in_range or headwind or evidence_half
    range_note = ['年度结构为震荡：按箱体策略只在回踩时半仓建仓'] if in_range else []

    def pending(p, why, cond):
        pct, size = size_3a(p, stop, half)
        return _directive('挂单买入', price=p, order='条件单' if headwind else '限价', valid=pend_valid,
                          size=size, size_pct=pct, why=range_note + why + window_note, checks=soft,
                          condition=_join(cond, h_cond), **kw)

    def pullback(why):
        p = pullback_price(last, stop, gate, er, gate.get('unlock_price'))
        if not p:
            return _directive('不买', why=why + ['回踩价贴近止损，到止损前没有满足安全边际的买点'], module=tier)
        return pending(p, why, f'回踩到位才成交（{_drop(p, last)}）')

    if gate.get('entry_state') == '限价可买' and _num(gate.get('limit_price')) and not in_range:
        p = gate['limit_price']
        if headwind:
            return pending(p, [gate.get('unlock_note')], '价格不高于此价')
        pct, size = size_3a(p, stop, half)
        return _directive('限价买入', price=p, order='限价', valid=SAME_DAY, size=size, size_pct=pct,
                          why=[gate.get('unlock_note')] + window_note, checks=soft,
                          condition='开盘价高于限价不追', **kw)
    if gate.get('blocked'):
        unlock = gate.get('unlock_price')
        if gate.get('entry_state') == '不追高' or in_range:
            return pullback(gate.get('reasons') or [])
        if _num(unlock) and unlock > stop:
            return pending(unlock, gate.get('reasons') or [],
                           f'回调到此价即同时满足全部价格门（{_drop(unlock, last)}）')
        return _directive('不买', why=(gate.get('reasons') or []) + ['到止损前给不出满足安全边际的买点'], module=tier)
    if lg.get('demoted'):
        if rotation or in_range:
            return pullback(rotation + market)
        return pending(upper, market + price_r, '价格不高于计划入场上沿')
    if gate.get('entry_state') or soft:
        # 已过全部入场门，只差多重核查/报价刷新等软缺口：限价单本身就消化这些缺口。
        if in_range:
            return pullback([])
        if headwind:
            return pending(upper, [], '价格不高于计划入场上沿')
        pct, size = size_3a(upper, stop, half)
        return _directive('限价买入', price=upper, order='限价', valid=SAME_DAY, size=size, size_pct=pct,
                          why=['已过追高熔断、盈亏比、财报窗与环境门'] + window_note, checks=soft,
                          condition='开盘价高于限价不追', **kw)
    return _directive('不买', why=missing[:2] or ['入场条件未满足'], module=tier)


def _decide_astra(row, *, now=None):
    """Astra 15 交易日波段：止损/目标/到期三选一离场。"""
    last = row.get('last'); er = row.get('entry_range') or []
    lg = row.get('live_gate') or {}
    stop, target = row.get('stop'), row.get('target')
    deadline = (row.get('window') or {}).get('deadline')
    if row.get('monthly_entry_deadline'):
        deadline = min(deadline or row['monthly_entry_deadline'], row['monthly_entry_deadline'])
    kw = dict(stop=stop, target=target, module='Astra',
              exit_rule=f'触及止损或目标离场；{deadline or "窗口截止日"}收盘仍未到目标按收盘价了结（时间止损）')
    status = str(row.get('status') or '')
    if row.get('time_feasible') is False:
        return _directive('不买', why=['本月剩余窗口内目标可达性未满足；保留原计划跟踪'], module='Astra')
    if deadline and _today(now).isoformat() > deadline:
        return _directive('不买', why=['月内新增入场期限已结束；原持仓保护继续'], module='Astra')
    if any(k in status for k in ('待重算', '到期', '终止', '已结束', '已退出')):
        return _directive('不买', why=[f'本轮计划状态：{status}，不新开仓'], module='Astra')
    hard = hard_gaps(row)
    if hard:
        return _directive('不买', why=hard, module='Astra')
    if not (_num(last) and _num(stop) and len(er) == 2 and all(_num(x) for x in er)):
        return _directive('不买', why=['缺可核实的波段计划，给不出可执行价'], module='Astra')
    er_state = row.get('earnings_risk') or {}
    if er_state.get('state') in ('blackout', 'stale'):
        when = er_state.get('earnings_date')
        if when and deadline and when < deadline:
            return _directive('财报后买入', valid=deadline, why=er_state.get('reasons') or [],
                              condition=f'财报（{when}）后首个交易日收盘仍在止损之上且回到计划区间 {_fmt(er[0])}–{_fmt(er[1])}，次日买入', **kw)
        return _directive('不买', why=(er_state.get('reasons') or []) + ['本轮窗口内财报未落地，不做事件赌博'],
                          module='Astra')
    if last <= stop:
        return _directive('不买', why=['现价已在止损之下，计划失效'], module='Astra')
    reasons = list(lg.get('reasons') or []) if lg.get('hold') is False else []
    market, rotation, price_r, other = classify(reasons)
    if other:
        return _directive('不买', why=other, module='Astra')
    headwind = bool(market)
    half = bool(lg.get('trend_trial') or lg.get('earnings_half') or headwind or row.get('fundamentals_state') == 'pending' or (row.get('astra_regime') or {}).get('weak'))
    if (row.get('rule_matched') or row.get('buyable')) and not reasons and er[0] <= last <= er[1]:
        pct, size = size_astra(er[1], stop, half)
        return _directive('立即买入', price=er[1], order='限价', valid=SAME_DAY, size=size, size_pct=pct,
                          why=[lg.get('note') or '计划区间内、全部实时门已过'], **kw)
    tail = '剩余不足5个交易日未成交即作废'
    h_cond = HEADWIND_COND if headwind else ''
    order = '条件单' if headwind else '限价'
    if price_r or rotation:
        t_now = row.get('plan_current_structure_target')
        u = unlock_price({'first_objective': t_now if _num(t_now) else target, 'stop': stop}, 6.0, 2.0)
        cands = [x for x in (u, er[0]) if _num(x) and x > stop]
        if not cands:
            return _directive('不买', why=price_r + rotation + ['到止损前给不出满足安全边际的买点'], module='Astra')
        p = min(cands); pct, size = size_astra(p, stop, half)
        return _directive('挂单买入', price=p, order=order, valid=deadline, size=size, size_pct=pct,
                          why=price_r + rotation + market,
                          condition=_join(f'回踩到此价才成交（现价 {_fmt(last)}）', h_cond, tail), **kw)
    if last > er[1]:
        pct, size = size_astra(er[1], stop, half)
        return _directive('挂单买入', price=er[1], order=order, valid=deadline, size=size, size_pct=pct,
                          why=['现价高于计划区间，不追价'] + market,
                          condition=_join(f'回落到计划上沿才成交（现价 {_fmt(last)}）', h_cond, tail), **kw)
    if last < er[0]:
        pct, size = size_astra(er[0], stop, half)
        return _directive('挂单买入', price=er[0], order='条件单', valid=deadline, size=size, size_pct=pct,
                          why=['现价低于计划区间：回到区间下沿再确认，不接飞刀'] + market,
                          condition=_join(f'突破区间下沿 {_fmt(er[0])} 触发、限价 {_fmt(er[1])}', h_cond, tail), **kw)
    if headwind:
        pct, size = size_astra(er[1], stop, half)
        return _directive('挂单买入', price=er[1], order='条件单', valid=deadline, size=size, size_pct=pct,
                          why=market, condition=_join(h_cond, tail), **kw)
    # 计划/复核层已判缺口（追过头、本月不可达、推后即跌等）＝条件不匹配：指令层不得另开买入，与 rule_matched 同一结论。
    gaps = [str(m) for m in (row.get('missing') or []) if not str(m).startswith(SOFT_GAP_PREFIX)]
    if gaps:
        return _directive('不买', why=gaps[:3], module='Astra')
    pct, size = size_astra(er[1], stop, half)
    soft = [m for m in (row.get('missing') or []) if str(m).startswith(SOFT_GAP_PREFIX)]
    return _directive('限价买入', price=er[1], order='限价', valid=SAME_DAY, size=size, size_pct=pct,
                      why=['现价在计划区间内、实时门已过'], checks=soft, condition='开盘价高于限价不追', **kw)


def _entry_checked(d, row):
    from entry_progression import location
    loc = row.get('entry_location') or location(row)
    if loc.get('blocked') and d.get('action') in ('立即买入','限价买入','挂单买入'):
        d = _directive('挂单买入', price=min(d.get('price') or loc['entry_ceiling'],loc['entry_ceiling']),
                       order='条件单', condition=loc['reason'], why=[loc['reason']],
                       size='等待回踩确认后重算', stop=d.get('stop'), target=d.get('target'),
                       module=d.get('module'), valid=d.get('valid_until'), exit_rule=d.get('exit'))
        d['guard']='位置过度延伸'
    weak=(row.get('ladder') or {}).get('weak') or (row.get('astra_regime') or {}).get('weak')
    if weak and not d.get('guard') and d.get('action') in ('立即买入','限价买入','挂单买入'):
        risk=_risk_pct(d.get('price'),d.get('stop'))
        if risk:
            old=d.get('size') or ''
            if d.get('module')=='Astra':
                d['size_notional_usd']=round(ASTRA_RISK_USD/3/(risk/100))
                d['size']=f'轻仓1/3：理论上限约 {d["size_notional_usd"]} 美元等值；整手与账户预算待核'
            else:
                d['size_pct']=round(min(MAX_POSITION_PCT,ACCOUNT_RISK_PCT/risk*100)/3,1)
                d['size']=f'轻仓1/3：约占账户 {d["size_pct"]}%（弱证据仓位上限）'
            if old:d['text']=d.get('text','').replace(old.split('：')[0],'轻仓1/3')
    return d


def decide_3a(row, *, now=None):
    """3A/2A/1A 指令 = 门控结论 + 内部否决（guard）。"""
    return _entry_checked(_guarded(_decide_3a(row, now=now), row, _add_sessions(_today(now), PULLBACK_SESSIONS_3A).isoformat()), row)


def decide_astra(row, *, now=None):
    """Astra 指令 = 门控结论 + 内部否决（guard）。"""
    return _entry_checked(_guarded(_decide_astra(row, now=now), row, (row.get('window') or {}).get('deadline')), row)


_SENTRY_CACHE = {}


def _load_sentry():
    """无快照内台账时（Astra 页/周期）读核心 strategy_board.json 的预警台账；按修改时间缓存。"""
    try:
        from pathlib import Path
        import json
        p = Path(__file__).resolve().parents[1] / 'data' / 'strategy_board.json'
        m = p.stat().st_mtime
        if _SENTRY_CACHE.get('mtime') != m:
            _SENTRY_CACHE.update(mtime=m, sentry=json.loads(p.read_text()).get('promise_sentry') or {})
        return _SENTRY_CACHE['sentry']
    except Exception:
        return {}


def stamp_breaches(rows, sentry):
    """把🔴/🟠违背预警写到行上（promise_breach），供 guard 读取；预警本身不展示。"""
    alerts = {a.get('code'): a for a in (sentry or {}).get('alerts') or [] if a.get('level') in BREACH_LEVELS}
    for row in rows:
        a = alerts.get(row.get('code'))
        if a:
            row['promise_breach'] = {k: a.get(k) for k in ('level', 'cause', 'dd', 'quad', 'module', 'ref_price')}
        else:
            row.pop('promise_breach', None)


RANK = {'立即买入': 0, '限价买入': 1, '挂单买入': 2, '财报后买入': 2, '不买': 3}
WINDOW_ATR = 1.5          # 挂单价距现价 ≤1.5×ATR14 视为 1–3 个交易日内可成交
WINDOW_PCT_NO_ATR = 3.0   # 无 ATR 时按 3% 计
TODAY, SOON = '今日可买', '1-3日内可买'
LAUNCH = '启动跟踪·等买点'
ARMED_ACTIONS = ('立即买入', '限价买入', '挂单买入')


_AH = None


def issuer_key(row):
    import re, json, os
    global _AH
    if _AH is None:
        try:
            _AH = json.load(open(os.path.join(os.path.dirname(__file__), 'ah_pairs.json')))
        except Exception:
            _AH = {}
    code = str(row.get('code') or '')
    m = re.fullmatch(r'(\d{1,5})\.HK', code)
    hk = str(int(m.group(1))).zfill(4) + '.HK' if m else None
    if hk and _AH.get(hk):
        return 'AH:' + _AH[hk]
    if code in _AH.values():
        return 'AH:' + code
    name = re.sub(r'(股份有限公司|有限公司|股份|公司|[-－]?[HhＨ]股?$|\s)', '', str(row.get('name') or ''))
    return name or None


def dedupe_dual_listed(rows, dropped):
    """同一发行人 A+H 两地同时上榜只计一次：保留买点更近、分数更高的一边。"""
    rank = {TODAY: 0, SOON: 1, LAUNCH: 2}
    best = {}
    for r in rows:
        if r.get('market') not in ('A股', '港股') or not issuer_key(r):
            continue
        k = issuer_key(r)
        cur = best.get(k)
        key = (rank.get(r.get('buy_window'), 9), -((r.get('strategy_score') or {}).get('value') or 0))
        if cur is None or key < cur[0]:
            best[k] = (key, r)
    out = []
    for r in rows:
        k = issuer_key(r) if r.get('market') in ('A股', '港股') else None
        if k and best[k][1] is not r:
            m = r.get('market') or '未知'
            dropped[m] = dropped.get(m, 0) + 1
            continue
        if k:
            twin = [x for x in rows if x is not r and x.get('market') in ('A股', '港股') and issuer_key(x) == k]
            if twin:
                r = {**r, 'dual_listed_with': twin[0].get('code')}
        out.append(r)
    return out


PENDING_MAX_DAYS = 14


def long_pending(row, doc):
    """待核项挂账超过约 10 个交易日仍未补全：撤下，不再进对外名单（存档保留，补全后自动回归）。"""
    from datetime import datetime
    gaps = list(row.get('missing') or []) + list(row.get('profit_gaps') or [])
    if not gaps:
        return False
    try:
        seen = datetime.fromisoformat(str(row.get('first_seen_at'))[:10])
        now = datetime.fromisoformat(str(doc.get('generated_at') or doc.get('asof') or datetime.now().isoformat())[:10])
    except ValueError:
        return False
    return (now - seen).days > PENDING_MAX_DAYS


def stale_price(row, d):
    rng = row.get('entry_range') or []
    price = d.get('price')
    if d.get('action') not in ARMED_ACTIONS or len(rng) != 2 or not (_num(price) and _num(rng[0]) and _num(rng[1])):
        return False
    return not (min(rng) * 0.995 <= price <= max(rng) * 1.005)


def directive_label(row):
    """指令行标签与 buy_window 同源：进入今日/1–3日买点才叫「操作指令」，否则是未激活的条件单。"""
    act = (row.get('directive') or {}).get('action')
    if act in ARMED_ACTIONS and row.get('buy_window') not in (TODAY, SOON):
        return '条件单（未激活）'
    return '操作指令'   # 仅 Astra：在席证据表已命中股，暂未进入买点窗口


def fade_of(row):
    """推后即跌形态（下跌途中 / 冲高回落M头）——3A 记在 entry_gate，Astra 记在行上；命中一律不推。"""
    from entry_progression import location
    loc = row.get('entry_location') or location(row)
    if loc.get('version') and loc.get('state') != 'data_pending':
        return loc.get('reason') if loc.get('blocked') else None
    fade = row.get('fade_risk') or (row.get('entry_gate') or {}).get('fade_risk')
    # 与 shadow_screen.entry_timing 同一规则（fade_exempt 唯一口径）。
    from shadow_screen import fade_exempt, EVIDENCE_BANDS
    reg = row.get('astra_regime') or {}
    if fade_exempt(fade, reg.get('grade'), row.get('annual_regime') == 'down', reg.get('band') in EVIDENCE_BANDS):
        return None
    return fade


MIN_NET_RR = 2.0
RR_FEE = 0.5


def rr_ok(row, d):
    """生成端硬校验：指令价处、以及正文显示的净盈亏比，任一 <2 就不能标可买（与 Muse 审核同一门槛）。"""
    from directive_audit import rr_floor   # 分级各档 / Astra 证据表回测底线，唯一来源
    floor = rr_floor(row, d.get('module')) or MIN_NET_RR
    shown = row.get('net_rr')
    if _num(shown) and shown < floor - 0.005:
        return False
    price, stop, target = d.get('price'), d.get('stop'), d.get('target')
    if _num(price) and _num(stop) and _num(target) and stop < price < target:
        reward = (target / price - 1) * 100 - RR_FEE
        risk = (1 - stop / price) * 100 + RR_FEE
        return reward / risk >= floor - 0.05
    return True


def buy_window(row, d):
    """唯一的推荐口径：只推今天立刻可买、或 1–3 个交易日内大概率可成交的票；其余返回 None（不推荐）。

    立即买入/限价买入 → 今日；挂单买入且挂单价距现价 ≤1.5×ATR（无 ATR 用 3%）→ 1–3 日；
    被内部否决（接飞刀/原推荐破位）、须等大盘转好才成交的条件单、财报后买入、不买 → None。"""
    if fade_of(row):
        return None
    action = d.get('action')
    if action in ('立即买入', '限价买入', '挂单买入') and not rr_ok(row, d):
        return None
    if action in ('立即买入', '限价买入'):
        return TODAY
    if action != '挂单买入' or d.get('guard') or HEADWIND_COND in str(d.get('condition') or ''):
        return None
    last, price = row.get('last'), d.get('price')
    if not (_num(last) and _num(price)):
        return None
    if d.get('order') == '条件单':
        return '等待触发'
    if price >= last:
        return TODAY
    atr = (row.get('entry_gate') or {}).get('atr14') or row.get('atr14')
    reach = WINDOW_ATR * atr if _num(atr) else last * WINDOW_PCT_NO_ATR / 100
    return SOON if last - price <= reach else None


def priority(row, module='3A', *, now=None):
    """席位排序 = 指令等级（选席与展示同一口径）：0 立即买入；1 限价买入；2 1–3日内可成交的挂单；
    3 其余（远挂单/条件单/财报后/不买——都不推荐，不该占席位）。"""
    try:
        d = decide_astra(row, now=now) if module == 'Astra' else decide_3a(row, now=now)
        return RANK[d['action']] if buy_window(row, d) else 3
    except Exception:
        return 3


MATCHED_STATUS, MATCHED_CATEGORY = '条件匹配', '买入条件已满足'


def sync_status(row):
    """buyable 的唯一裁决在 annotate；状态/类别文字跟随它，不再各写各的（长鑫科技「条件匹配」却不可买）。"""
    if row.get('buyable'):
        if row.get('status') == '研究候选' and not row.get('missing'):
            row['status'] = MATCHED_STATUS
            row['selection_category'] = MATCHED_CATEGORY
        return
    if row.get('status') == MATCHED_STATUS:
        row['status'] = '研究候选'
    if row.get('selection_category') == MATCHED_CATEGORY:
        from strategy_board_state import RESEARCH_CATEGORY
        row['selection_category'] = RESEARCH_CATEGORY


def annotate(doc, module='3A', *, now=None, sentry=None):
    """最终一遍：在全部门（含多重核查）之后给每行写 directive，并统计各市场可执行数。"""
    rows = doc.get('rows') or []
    if sentry is None:
        sentry = doc.get('promise_sentry') if isinstance(doc.get('promise_sentry'), dict) else _load_sentry()
    stamp_breaches(rows, sentry)
    summary = {'version': VERSION, 'markets': {}}
    for row in rows:
        try:
            d = decide_astra(row, now=now) if module == 'Astra' else decide_3a(row, now=now)
        except Exception as exc:  # 指令层永不拖垮名单
            d = _directive('不买', why=[f'指令计算异常：{type(exc).__name__}'], module=module)
        row['directive'] = d
    try:  # GitHub 结论 ≡ Copilot 结论：独立复核 + 判例一致性，违规行降为不买
        from directive_audit import enforce
        summary['audit'] = enforce(doc, module, now)
    except Exception as exc:
        summary['audit'] = {'consistent': False, 'text': f'🔒 一致性复核异常：{type(exc).__name__}'}
    for row in rows:
        w = buy_window(row, row['directive'])
        row['buyable'] = w == TODAY
        if 'rule_matched' in row:row['rule_matched'] = w == TODAY
        if w:
            row['buy_window'] = w
        else:
            row.pop('buy_window', None)
        sync_status(row)
    for row in rows:
        m = summary['markets'].setdefault(row.get('market') or '未知', {a: 0 for a in ACTIONS})
        m[row['directive']['action']] += 1
    summary['windows'] = {w: sum(r.get('buy_window') == w for r in rows) for w in (TODAY, SOON)}
    summary['guarded'] = sum(bool(r['directive'].get('guard')) for r in rows)
    doc['directive_summary'] = summary
    return summary


def publishable(doc, module='3A'):
    """对外输出（页面、飞书、推荐日志）的唯一过滤：只留今日可买 / 1–3日内可买，其余不推，允许 0 只；
    Astra 另列在席的证据表已命中股（启动跟踪·等买点）。
    存档文件保留全量（固定计划、历史留档、续持复核仍要用），只是不再展示、不再推送。"""
    if not isinstance(doc, dict):
        return doc
    out = dict(doc)
    rows = list(doc.get('rows') or [])
    keep, dropped, incomplete, vetoed = [], {}, [], []
    from muse_evidence import publication_gaps
    # 退出状态的单一事实源：plan_changes 里该票最后一次变更若是 exited（同一计划），正文/页面/名单一律移除。
    last_change = {}
    for c in doc.get('plan_changes') or []:
        if c.get('action') in ('added', 'exited'):
            last_change[(c.get('market'), c.get('code'))] = c
    for r in rows:
        c = last_change.get((r.get('market'), r.get('code')))
        exited = (c and c.get('action') == 'exited' and (not c.get('plan_id') or not r.get('plan_id') or c.get('plan_id') == r.get('plan_id'))
                  and str(c.get('at') or '') >= str(r.get('plan_created_at') or ''))
        if exited or str(r.get('status') or '').startswith(('已退出本轮研究', '本轮到期复盘', '已止损', '计划已失效', '原轮次跟踪 · 待重算', '原研究保留 · 报价待更新')):
            m = r.get('market') or '未知'
            dropped[m] = dropped.get(m, 0) + 1
            continue
        if long_pending(r, doc):
            m = r.get('market') or '未知'
            dropped[m] = dropped.get(m, 0) + 1
            continue
        gaps = publication_gaps(r)
        if gaps:
            incomplete.append({'code':r.get('code'),'name':r.get('name'),'market':r.get('market'),
                               'status':'数据不完整','gaps':gaps})
            m = r.get('market') or '未知'
            dropped[m] = dropped.get(m, 0) + 1
            continue
        d = r.get('directive')
        if d and stale_price(r, d):
            # 指令限价必须随当前入场带更新；旧限价落在入场带外即按同一指令层重算。
            try:
                d = decide_astra(r) if module == 'Astra' else decide_3a(r)
                r = {**r, 'directive': d, 'directive_repriced_from': (r.get('directive') or {}).get('price')}
            except Exception:
                d = {}
        if not d:
            try:
                d = decide_astra(r) if module == 'Astra' else decide_3a(r)
            except Exception:
                d = {}
        r = {**r, 'directive':d}
        w = buy_window(r, d)
        # Astra 在席股证据表已命中：今日/1–3日内买不到的也照列，标「启动跟踪·等买点」。
        if not w and module == 'Astra' and (r.get('astra_position') or {}).get('ok') and not fade_of(r):
            w = LAUNCH
        if w:
            r = {**r, 'buy_window':w, 'buyable':w==TODAY}
            if 'rule_matched' in r:r['rule_matched'] = w==TODAY
            keep.append(r)
        else:
            m = r.get('market') or '未知'
            dropped[m] = dropped.get(m, 0) + 1
            if r.get('grade_basis') == 'grade-ladder-v1' and r.get('strategy_tier') in ('3A', '2A', '1A'):
                vetoed.append({'code': r.get('code'), 'name': r.get('name'), 'market': m,
                               'tier': r.get('strategy_tier'), 'action': d.get('action'),
                               'why': (d.get('guard') or '；'.join((d.get('why') or [])[:1]) or '指令层未给出3日内可成交价'),
                               'condition': d.get('condition') or ''})
    keep = dedupe_dual_listed(keep, dropped)
    if vetoed:
        out['ladder'] = {**(out.get('ladder') or {}), 'vetoed': vetoed}
    out['rows'] = keep
    out['publication_excluded'] = incomplete
    if module == 'Astra':
        out['watch'] = []
        out['rule_matched_count'] = sum(r.get('buy_window') in (TODAY, SOON) for r in keep)
    else:
        out.update(investment_candidates=[], top_grade_watch=[], entry_blocked=[], tier_watch={})
    summary = dict(out.get('directive_summary') or {})
    summary['dropped'] = dropped
    out['directive_summary'] = summary
    return out


def plan_bands(row):
    """Board 三列的展示口径：回测计划带优先，缺失时回退指令层的结构价位。

    3A 的中期计划只在年度上行结构下生成——趋势延续计划用在震荡结构上实测亏钱，
    这个拒绝是对的，不该绕过。但入场门在任何结构下都算出了结构止损、结构目标
    与解锁价，指令层已据此发出可执行指令。

    页面若只读计划带，就会在指令层说"条件单 ≤ 32.328 · 止损 31.096 · 目标 36.09"
    的同一轮，对用户显示"待核结构"。同一轮、同一只票、两种说法，这是必须消除的
    第二套逻辑，不是显示细节。

    回退值标 source='directive'，由渲染层注明它是结构位而非回测计划区间——
    口径不同必须说清，但"说不清"不等于"不说"。
    """
    d = row.get('directive') or {}

    def band(values):
        if not isinstance(values, (list, tuple)):
            return None
        return '–'.join(f'{v:g}' for v in values if _num(v)) or None

    def pick(plan_key, fallback, note):
        text = band(row.get(plan_key))
        if text:
            return {'text': text, 'source': 'plan', 'note': None}
        if _num(fallback):
            return {'text': f'{fallback:g}', 'source': 'directive', 'note': note}
        return {'text': None, 'source': None, 'note': d.get('text')}

    entry = band(row.get('entry_range'))
    price, order, action = d.get('price'), d.get('order') or '', d.get('action') or ''
    if entry:
        entry_cell = {'text': entry, 'source': 'plan', 'note': None}
    elif _num(price):
        entry_cell = {'text': f'{order} ≤ {price:g}'.strip(), 'source': 'directive',
                      'note': f'{action}·结构位定价（本轮未启用趋势延续计划）'}
    else:
        entry_cell = {'text': None, 'source': None, 'note': d.get('text')}

    return {'entry': entry_cell,
            'profit': pick('take_profit_range', d.get('target'), '结构目标·指令层'),
            'stop': pick('stop_range', d.get('stop'), '结构止损·指令层')}


def public_directive(d, module=None):
    """发布版指令：3A 仓位是账户百分比可公开；Astra 仓位是美元金额，发布时隐去。"""
    if not isinstance(d, dict) or not d:
        return {}
    out = dict(d)
    if (module or out.get('module')) not in ('3A', '2A', '1A'):
        out.pop('size', None)
        out.pop('size_pct', None)
        out.pop('size_notional_usd', None)
    return out


def public_summary(summary):
    """发布版复核摘要：保留结论与计数，去掉个股名单（身份边界与内部台账不外发）。"""
    if not isinstance(summary, dict) or not summary:
        return {}
    out = {k: summary[k] for k in ('version', 'markets', 'guarded') if k in summary}
    audit = summary.get('audit') or {}
    if not audit:
        return out
    pub = {k: audit[k] for k in ('version', 'module', 'checked', 'passed', 'casebook', 'consistent') if k in audit}
    failed = len(audit.get('failed') or [])
    pub['failed'] = failed
    cb = audit.get('casebook') or {}
    if 'checked' in pub and cb:
        pub['text'] = (f"🔒 GitHub结论与Copilot逻辑一致性：判例 {cb.get('passed')}/{cb.get('total')} 一致 · "
                       f"指令复核 {pub['passed']}/{pub['checked']} 通过"
                       + ('' if not failed else f'（{failed} 条降为不买）')
                       + ('' if not cb.get('drift') else ' · ⚠️ 判例不一致，买入指令已停发'))
    else:  # 复核异常路径本就不含个股名
        pub['text'] = audit.get('text') or ''
        pub.setdefault('consistent', audit.get('consistent', False))
    out['audit'] = pub
    return out


def line(row):
    d = row.get('directive') or {}
    return ('📌 ' + d['text']) if d.get('text') else ''
