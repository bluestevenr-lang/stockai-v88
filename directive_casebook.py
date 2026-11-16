"""Copilot 判例库：对典型情形的判断固化为判例，GitHub 每次运行先复算，逐条一致才发买入指令。

每条判例 = 一个情形 + Copilot 的结论（动作，必要时含价格/半仓）。新增或修改指令逻辑时，
先在这里写下"应当如何判"，代码复算与之不符即视为逻辑漂移——云端结论与 Copilot 结论不再一致。
"""
from copy import deepcopy
from datetime import datetime, timezone

NOW = datetime(2026, 10, 21, 8, 0, tzinfo=timezone.utc)


def _3a(**kw):
    r = {'code': 'C', 'name': '判例', 'market': '美股', 'strategy_tier': '3A', 'last': 100.0,
         'entry_range': [99.0, 100.5], 'stop': 92.0, 'target': 120.0, 'trend_exit_ma60': 95.0,
         'missing': [], 'buyable': False,
         'entry_gate': {'blocked': False, 'reasons': [], 'entry_state': '可入场', 'env': 'neutral',
                        'ma20': 97.0, 'atr14': 2.0}}
    gate = kw.pop('gate', {})
    r.update(deepcopy(kw))
    r['entry_gate'].update(gate)
    return r


def _astra(**kw):
    r = {'code': 'A', 'name': '判例', 'market': 'A股', 'last': 10.0, 'entry_range': [9.9, 10.05],
         'stop': 9.4, 'target': 11.4, 'rule_matched': True, 'buyable': True, 'status': '条件匹配',
         'missing': [], 'window': {'deadline': '2026-11-09'}, 'live_gate': {'hold': True, 'reasons': []}}
    r.update(deepcopy(kw))
    return r


def _no_gate():
    r = _3a(missing=['本日研究续列；当前分77.9低于80，等待下一完整交易日评审'])
    r['entry_gate'] = {}
    return r


# (判例名, 模块, 行, 期望动作, 附加核对)
CASES = (
    ('招商银行型：全部入场门已过 → 计划上沿立即买入、标准仓', '3A', _3a(buyable=True),
     '立即买入', {'price': 100.5, 'half': False}),
    ('URBN型：离全部价格门≤1% → 当日限价买入', '3A',
     _3a(gate={'blocked': True, 'entry_state': '限价可买', 'limit_price': 99.6}), '限价买入', {'price': 99.6}),
    ('富春环保型：逆风非领头股 → 条件单半仓，不再"等待"', '3A',
     _3a(gate={'blocked': True, 'entry_state': '等回调', 'unlock_price': 97.5, 'env': 'headwind',
               'reasons': ['盈亏比门：当前净收益风险比1.6不足2']}), '挂单买入', {'price': 97.5, 'half': True}),
    ('追高熔断 → 不追，回踩20日线挂单', '3A',
     _3a(gate={'blocked': True, 'entry_state': '不追高', 'reasons': ['追高熔断：偏离MA20 +9%']}),
     '挂单买入', {'price': 97.0}),
    ('ISRG教训：已核财报日在7日静默窗 → 财报后买入，不赌事件', '3A',
     _3a(buyable=True, earnings_risk={'state': 'blackout', 'earnings_date': '2026-10-23',
                                      'reasons': ['财报静默窗：已核财报日 2026-10-23']}), '财报后买入', {}),
    ('利润现金含量不足 → 任何价格都不买（格雷厄姆）', '3A',
     _3a(buyable=True, missing=['利润质量：经营现金流仅为净利润的3%（<50%），利润现金含量不足']), '不买', {}),
    ('公告风险事项 → 不买', '3A', _3a(missing=['近期公告含风险事项：立案调查']), '不买', {}),
    ('年度下行（温斯坦第四阶段） → 不买', '3A', _3a(missing=['年度结构为下行，不属中长期上行']), '不买', {}),
    ('THC型：当前分<80、未经入场门 → 不给买价', '3A', _no_gate(), '不买', {}),
    ('年度震荡（达瓦斯箱体） → 只在回踩时半仓', '3A',
     _3a(missing=['年度结构为震荡（均线未形成上行排列），不属中长期上行']), '挂单买入', {'half': True}),
    ('宁德/腾讯型：原推荐已破位（🔴重新定价、行业持续落后）→ 不补仓不抄底，站回60日线且行业转强才成交', '3A',
     _3a(buyable=True, promise_breach={'level': '红', 'cause': '重新定价', 'dd': -26.0, 'quad': '持续落后'}),
     '挂单买入', {'price': 100.5}),
    ('接飞刀：跌破20日线且5日跌≥3% → 条件单，站回20日线才成交', '3A',
     _3a(buyable=True, gate={'ma20': 101.0, 'return5_pct': -4.0}), '挂单买入', {'price': 100.5}),
    ('Astra：区间内、全部实时门已过 → 立即买入', 'Astra', _astra(), '立即买入', {'price': 10.05}),
    ('Astra：现价高于区间，旧数据仍写可买 → 挂单回上沿，不追', 'Astra',
     _astra(last=10.4, rule_matched=False), '挂单买入', {'price': 10.05}),
    ('Astra：大盘逆风 → 条件单半仓', 'Astra',
     _astra(rule_matched=False, live_gate={'hold': False, 'reasons': ['大盘逆风（指数在MA60下方且MA20下行），环境门不发买入级']}),
     '挂单买入', {'half': True}),
    ('Astra：当前分跌破入选线 → 不买', 'Astra',
     _astra(rule_matched=False, live_gate={'hold': False, 'reasons': ['当前分55<入选线60']}), '不买', {}),
    ('Astra ISRG教训：持有期内有已核财报 → 财报后买入', 'Astra',
     _astra(earnings_risk={'state': 'blackout', 'earnings_date': '2026-11-03', 'reasons': ['波段不跨财报']}),
     '财报后买入', {}),
    ('Astra：本轮到期 → 不新开仓', 'Astra', _astra(status='本轮到期复盘'), '不买', {}),
)


def run():
    """返回 (一致数, 判例数, 不一致明细)。"""
    from action_directive import decide_3a, decide_astra
    drift = []
    for name, module, row, action, extra in CASES:
        try:
            d = (decide_astra if module == 'Astra' else decide_3a)(deepcopy(row), now=NOW)
        except Exception as exc:  # 判例都跑不动即视为漂移
            drift.append(f'{name}：异常 {type(exc).__name__}')
            continue
        bad = d['action'] != action
        if 'price' in extra and d.get('price') != extra['price']:
            bad = True
        if 'half' in extra and ('半仓' in str(d.get('size') or '')) != extra['half']:
            bad = True
        if bad:
            drift.append(f"{name}：应为{action}，实际{d['action']}")
    total = len(CASES)
    # 推荐违背预警判例（TCL/宁德/腾讯/分数抖动/银行轮动）同一库复算
    try:
        from promise_sentry import run_cases
        _, n, sentry_drift = run_cases()
        total += n
        drift += ['预警·' + x for x in sentry_drift]
    except Exception as exc:
        total += 1
        drift.append(f'预警判例：异常 {type(exc).__name__}')
    return total - len(drift), total, drift
