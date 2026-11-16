"""Shared pure recommendation review rules and cases. No portfolio, files or network access."""
import math

LEVELS = ('', '黄', '橙', '红')

THRESH = {'short': (5.0, 8.0, 12.0), 'mid': (8.0, 12.0, 15.0)}

FLOOR = {'3A': 80, '3A名额外': 80, '2A': 60, '1A': 50, 'Astra': 60}

HORIZON = {'3A': 'mid', '3A名额外': 'mid', '2A': 'short', '1A': 'short', 'Astra': 'short'}

WEAK_QUADS = ('短期转弱', '持续落后')

DEFENSIVE = ('银行', '炼化', '石化', '油气', '保险', '电力', '公用', '石油', '煤炭', '电信', '高速', '港口', '燃气', '水务', '铁路', '烟草', '综合油气', '运营商')

GROWTH = ('半导体', '软件', '电池', '汽车', '互联网', '新能源', '光伏', '电子', '通信设备', '医疗器械', '生物', '游戏', '传媒', '计算机', '芯片', '家庭电器', '消费电子')

def _n(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)

def _kind(industry):
    s = str(industry or '')
    if any(k in s for k in DEFENSIVE):
        return '防御'
    if any(k in s for k in GROWTH):
        return '成长'
    return ''

def attribute(e, q, ind):
    """下跌归因。e=评估行（annual_metrics/fundamentals/business_status/last），q=行情结构，ind=行业统计。
    返回 (类别, 依据)。顺序固定，先命中先返回。"""
    e, q, ind = e or {}, q or {}, ind or {}
    s = q.get('structure') or q
    a = e.get('annual_metrics') or {}
    f = e.get('fundamentals') or {}
    chg, r20 = s.get('change_pct'), s.get('return20_pct')
    ir5, ir20 = ind.get('r5'), ind.get('r20')
    last = e.get('last') if _n(e.get('last')) else q.get('last')
    if _n(chg) and chg <= -15 and not (_n(ir5) and ir5 <= -2):
        return '数据待核', f'单日{chg:.1f}%而行业5日未跌，疑除权/拆股/报价错误，先核价再处置'
    status = e.get('business_status') or f.get('status')
    if status == 'counterevidence' or (_n(f.get('profit_yoy')) and f['profit_yoy'] < 0 and e.get('_profit_at_ref_nonneg')):
        return '基本面转弱', f'企业依据转为反证或利润同比转负（{f.get("profit_yoy")}%）'
    ann = a.get('annual_return_pct')
    if _n(ann) and ann >= 50 and a.get('top_warning'):
        return '高位回吐', f'一年涨{ann:.0f}%且处高位预警区，属涨幅透支后的获利回吐'
    if _n(ir20) and ir20 <= -5 and _n(r20) and r20 >= 1.3 * ir20:
        return '板块拖累', f'行业「{ind.get("industry")}」20日{ir20:+.1f}%（{ind.get("quad")}），个股{r20:+.1f}%与行业同步'
    ma200 = a.get('ma200')
    if status == 'support' and _n(last) and _n(ma200) and last <= 0.8 * ma200 and _n(ann) and ann <= -30:
        return '重新定价', f'基本面仍有支撑但股价低于200日线{(last / ma200 - 1) * 100:.0f}%、一年{ann:.0f}%，市场在下调估值中枢'
    vs, ey = f.get('valuation_score'), f.get('earnings_yield_pct')
    if (_n(vs) and vs < 50) or (_n(ey) and ey < 2.5):
        return '估值回落', f'估值分{vs}、盈利收益率{ey}%，偏贵标的在回落'
    return '个股走弱', '行业与基本面未见同步恶化，原因未确认，按止损纪律处理'

HOLD = {
    '数据待核': '先核对复权与报价；核实前不加仓、不按此跌幅止损',
    '基本面转弱': '基本面证据变化：不补仓，待新财报确认',
    '高位回吐': '涨幅透支型回落：不在下跌中补仓，守住离场线',
    '板块拖累': '跟随板块下跌：不加仓、止损不动；行业5日相对强弱转正前不追加',
    '重新定价': '估值中枢下移：不加仓，跌破止损即离场',
    '估值回落': '估值消化：不追加，止损不动',
    '个股走弱': '原因未确认：不补仓，按原止损执行',
}

REENTRY = {
    '数据待核': '先核对复权与报价；核实前不按此跌幅处置',
    '基本面转弱': '新财报确认企业依据恢复前不接回',
    '高位回吐': '不在下跌中接回；等回踩60日线企稳或新平台形成后由系统重评',
    '板块拖累': '等行业5日相对强弱转正、个股站回20日线后由系统重评',
    '重新定价': '不抄底；等重新站上60日线且行业转强后由系统重评',
    '估值回落': '等估值消化、回到计划入场区下沿后由系统重评',
    '个股走弱': '原因未确认前不接回',
}

ADVICE = HOLD

def assess(p, e, q, ind, *, now=None):
    """对单条承诺复核：返回 {level, dd, reasons, cause, cause_note, action}。纯函数，供判例复算。"""
    e, q = e or {}, q or {}
    s = q.get('structure') or q
    last = e.get('last') if _n(e.get('last')) else q.get('last')
    if not _n(last) or not _n(p.get('ref_price')) or p['ref_price'] <= 0:
        return {'level': '', 'reasons': ['缺现价'], 'dd': None}
    horizon = HORIZON.get(p.get('module'), 'short')
    y, o, r = THRESH[horizon]
    dd_ref = (last / p['ref_price'] - 1) * 100
    rp = p.get('recent_price') if _n(p.get('recent_price')) else p['ref_price']
    dd = min(dd_ref, (last / rp - 1) * 100)
    lv, reasons = 0, []

    def up(k, why):
        nonlocal lv
        lv = max(lv, k); reasons.append(why)

    if dd <= -r:
        up(3, f'较推荐价回撤{dd:.1f}%（≥{r:g}%）')
    elif dd <= -o:
        up(2, f'较推荐价回撤{dd:.1f}%（≥{o:g}%）')
    elif dd <= -y:
        up(1, f'较推荐价回撤{dd:.1f}%（≥{y:g}%）')
    stop = p.get('stop')
    if _n(stop) and last < stop:
        up(3 if horizon == 'mid' else 2, f'跌破计划止损{stop:g}')
    ma60 = (e.get('annual_metrics') or {}).get('ma60')
    if horizon == 'mid' and _n(ma60) and last < ma60:
        up(2, f'收盘低于60日线{ma60:g}（3A离场规则）')
    if len(set(p.get('low_score_days') or [])) >= 2:
        up(1, f'当前分连续2个交易日低于入选线{FLOOR.get(p.get("module"), 60)}')
    chg = s.get('change_pct')
    if _n(chg) and chg <= -5:
        up(1, f'单日跌{chg:.1f}%')
    if ind and ind.get('quad') in WEAK_QUADS and dd <= -3:
        up(1, f'所属行业「{ind.get("industry")}」{ind.get("quad")}')
    f = e.get('fundamentals') or {}
    if (e.get('business_status') == 'counterevidence' and p.get('status_at') == 'support') or \
            (_n(f.get('profit_yoy')) and f['profit_yoy'] < 0 and _n(p.get('profit_at')) and p['profit_at'] >= 0):
        up(2, '企业依据转弱（利润同比转负或转为反证）')
    level = LEVELS[lv]
    if not level:
        return {'level': '', 'dd': round(dd, 2), 'reasons': reasons}
    cause, note = attribute({**e, '_profit_at_ref_nonneg': _n(p.get('profit_at')) and p['profit_at'] >= 0}, q, ind)
    if cause == '数据待核':
        return {'level': level, 'dd': round(dd, 2), 'dd_ref': round(dd_ref, 2), 'last': last, 'reasons': reasons,
                'cause': cause, 'cause_note': note, 'action': HOLD[cause]}
    if level == '红':
        head = '原计划失效：按止损/离场规则退出，不补仓'
    elif level == '橙':
        head = (f'按3A离场规则：收盘已低于60日线{ma60:g}，离场或至少减半' if horizon == 'mid' and _n(ma60) and last < ma60
                else f'止损{stop:g}已破：离场' if _n(stop) and last < stop else '减仓一半，余仓以原止损为界')
    else:
        head = '持有不加仓，止损不动'
    return {'level': level, 'dd': round(dd, 2), 'dd_ref': round(dd_ref, 2), 'last': last, 'reasons': reasons,
            'cause': cause, 'cause_note': note, 'action': f'{head}；{(HOLD if level == "黄" else REENTRY)[cause]}'}

def rotation(stats, means, *, min_n=5, top=3):
    """各市场轮动摘要：领涨/领跌行业（按20日相对强弱）+ 风格判断。"""
    out = {}
    for m in sorted({k[0] for k in stats}):
        xs = [v for (mm, _), v in stats.items() if mm == m and v['n'] >= min_n and _n(v.get('rs20'))]
        if len(xs) < 2 * top:
            continue
        xs.sort(key=lambda v: v['rs20'], reverse=True)
        lead, lag = xs[:top], xs[-top:][::-1]
        kinds = lambda vs: [_kind(v['industry']) for v in vs]
        style = ''
        if kinds(lead).count('防御') >= 2 and kinds(lag).count('成长') >= 1:
            style = '防御轮动：资金从成长/高估值板块转向高股息低估值板块（估值修复＋避险），多在成长股止跌（行业5日相对转正）后回摆；防御板块不追高，成长板块不抄底'
        elif kinds(lead).count('成长') >= 2 and kinds(lag).count('防御') >= 1:
            style = '进攻轮动：资金回流成长板块，防御板块相对落后；成长股只在回踩时买，不追高'
        def cell(v):
            bits = [f'{v["industry"]} 20日{v["r20"]:+.1f}%（相对{v["rs20"]:+.1f}，{v["quad"]}）']
            if _n(v.get('ey_median')):
                bits.append(f'盈利收益率中位{v["ey_median"]:g}%')
            if _n(v.get('profit_median')):
                bits.append(f'利润同比中位{v["profit_median"]:+g}%')
            return '·'.join(bits)
        why = []
        for v in lead:
            if _kind(v['industry']) == '防御' and _n(v.get('ey_median')) and v['ey_median'] >= 8 and \
                    (not _n(v.get('profit_median')) or v['profit_median'] < 10):
                why.append(f'{v["industry"]}上涨主因是估值修复与高股息避险（盈利收益率中位{v["ey_median"]:g}%、利润增速中位{v.get("profit_median")}%），非盈利加速，成长股企稳后易回落')
        out[m] = {'market_r5': (means.get(m) or (None, None))[0], 'market_r20': (means.get(m) or (None, None))[1],
                  'lead': [cell(v) for v in lead], 'lag': [cell(v) for v in lag], 'style': style, 'why': why}
    return out

def _case_p(module, ref, **kw):
    return {'module': module, 'ref_price': ref, 'recent_price': ref, 'low_score_days': [], **kw}

CASES = (
    ('TCL电子型：3A名额外可买后高位回吐、跌破60日线 → 橙/高位回吐', _case_p('3A名额外', 17.08),
     {'last': 16.0, 'score': 64.8, 'business_status': 'support',
      'annual_metrics': {'ma60': 16.68, 'ma200': 13.97, 'annual_return_pct': 79.4, 'top_warning': True},
      'fundamentals': {'profit_yoy': 40.2, 'valuation_score': 72.6, 'earnings_yield_pct': 6.95}},
     {'structure': {'change_pct': -1.2, 'return20_pct': -4.0}},
     {'industry': '家庭电器及用品', 'quad': '中性', 'r5': -1.0, 'r20': -3.0}, ('橙', '高位回吐')),
    ('宁德时代型：1A推荐后止损早破、远低于200日线 → 红/重新定价（不抄底）', _case_p('1A', 297.75, stop=285.17),
     {'last': 220.41, 'score': 59, 'business_status': 'support',
      'annual_metrics': {'ma60': 262.0, 'ma200': 371.9, 'annual_return_pct': -39.5, 'top_warning': False},
      'fundamentals': {'profit_yoy': 31.6, 'valuation_score': 80, 'earnings_yield_pct': 8.55}},
     {'structure': {'change_pct': -2.0, 'return20_pct': -20.0}},
     {'industry': '电池', 'quad': '持续落后', 'r5': -3.0, 'r20': -8.0}, ('红', '重新定价')),
    ('腾讯型：持仓随软件板块同步下跌 → 黄/板块拖累（不加仓）', _case_p('2A', 405.0),
     {'last': 381.6, 'score': 66, 'business_status': 'support',
      'annual_metrics': {'ma60': 441.0, 'ma200': 430.0, 'annual_return_pct': -5.0, 'top_warning': False},
      'fundamentals': {'profit_yoy': 10.0, 'valuation_score': 77, 'earnings_yield_pct': 7.45}},
     {'structure': {'change_pct': -1.5, 'return20_pct': -10.0}},
     {'industry': '软件服务', 'quad': '持续落后', 'r5': -2.0, 'r20': -9.6}, ('黄', '板块拖累')),
    ('分数抖动：只低于入选线1个交易日 → 不预警', _case_p('3A', 10.0, low_score_days=['2026-10-21']),
     {'last': 9.9, 'score': 76, 'business_status': 'support',
      'annual_metrics': {'ma60': 9.5, 'ma200': 9.0, 'annual_return_pct': 20, 'top_warning': False}, 'fundamentals': {}},
     {'structure': {'change_pct': -1.0, 'return20_pct': 1.0}}, {'quad': '中性'}, ('', None)),
)

def run_cases():
    """返回 (一致数, 判例数, 不一致明细)；含轮动风格判例（银行型：防御领涨＋成长领跌）。"""
    drift = []
    for name, p, e, q, ind, (level, cause) in CASES:
        try:
            a = assess(p, e, q, ind)
        except Exception as exc:
            drift.append(f'{name}：异常 {type(exc).__name__}'); continue
        if a.get('level') != level or (cause and a.get('cause') != cause):
            drift.append(f'{name}：应为{level}/{cause}，实际{a.get("level")}/{a.get("cause")}')
    name = '银行型：防御板块领涨、成长板块领跌 → 防御轮动＋估值修复说明'
    mk = lambda ind, rs, ey, pr: {'industry': ind, 'n': 10, 'quad': '持续占优' if rs > 0 else '持续落后', 'rs5': rs, 'rs20': rs,
                                  'r5': rs, 'r20': rs, 'ey_median': ey, 'profit_median': pr}
    st = {('A股', i): mk(i, rs, ey, pr) for i, rs, ey, pr in
          (('银行Ⅱ', 6, 14.2, 2), ('电力', 4, 7, 5), ('煤炭开采', 3, 10, -5), ('化学制品', 0.5, 4, 3),
           ('电池', -9, 8.5, 30), ('半导体', -8, 1.5, 20), ('汽车整车', -7, 4, 10))}
    try:
        r = rotation(st, {'A股': (0, 0)}).get('A股') or {}
        if not (r.get('style', '').startswith('防御轮动') and any('银行' in w and '估值修复' in w for w in r.get('why') or [])):
            drift.append(f'{name}：风格/原因未识别')
    except Exception as exc:
        drift.append(f'{name}：异常 {type(exc).__name__}')
    total = len(CASES) + 1
    return total - len(drift), total, drift
