"""GitHub 结论 ≡ Copilot 结论：操作指令的独立复核与判例一致性。

不调用 action_directive 的任何判定函数，只从行数据重新推出硬规则，逐条核对已发布指令：
  1. 判例库（directive_casebook）：Copilot 对典型情形给出的判断固化为判例，每次运行先全部复算，
     与判例逐条一致才算"逻辑未漂移"；任何一条不一致，本轮所有买入类指令降为"不买"。
  2. 不变量复核：价格/止损/目标顺序、追高、财报、硬伤、逆风半仓、净盈亏比、未经入场门、
     页面与飞书同一文本。违反者该行降为"不买"，写明哪条规则不通过。
无模型调用、无额度；云端与桌面同一份代码，结论同源。
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta

VERSION = 'directive-audit-2026-10-21-v1'
BUY = ('立即买入', '限价买入', '挂单买入')
FEE = 0.5
# 与指令层的硬伤口径独立维护：这些结论已写进行数据，任何价格都不该给买价。
HARD = ('不能认定', '利润现金含量不足', '近期公告含风险事项', '年度结构为下行', '低于Astra质量门槛')
BLACKOUT_DAYS_3A = 7
MIN_RR = {'3A': 1.95, 'Astra': 1.95}   # 指令价处净盈亏比底线：入场门要求2，仅留取整余量
# 好股×时机×获利分级（grade_ladder）与 Astra 证据表行：底线按命中规则自身的最差几何（固定目标%、止损最深8%）
# 计算——回测胜率正是在这个几何下测得，不另立 3R/2R 口径（旧口径会把胜率≥53%的 +8% 目标规则一律拒成挂单）。
def rr_floor(row, module):
    """指令价处净盈亏比底线的唯一口径（指令生成端 rr_ok 与本复核共用）；无专门口径时返回 None。"""
    reg = row.get('astra_regime') or {}
    if module == 'Astra':
        from shadow_screen import EVIDENCE_BANDS
        if not (reg.get('band') in EVIDENCE_BANDS and reg.get('grade') == 'buy'):
            return None
    elif row.get('grade_basis') != 'grade-ladder-v1':
        return None
    from horizon_book import rr_floor as rule_floor
    return rule_floor(reg.get('cell'))


_CASEBOOK = None


def _n(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x) and x > 0


def _day(now):
    if isinstance(now, datetime):
        return now.date()
    if isinstance(now, date):
        return now
    return datetime.now().date()


def _date(s):
    try:
        return date.fromisoformat(str(s)[:10])
    except (TypeError, ValueError):
        return None


def violations(row, module, now=None):
    """返回该行指令违反的规则列表（空 = 通过）。"""
    d = row.get('directive') or {}
    act = d.get('action')
    out = []
    if act in ('研究候选', '策略条件匹配', '等待开市', '等待核价', '等待核验', '等待触发'):
        original = d.get('signal_plan') or {}
        if original.get('action') not in BUY:
            return ['公开计划缺原始策略指令']
        for field in ('price', 'stop', 'target', 'valid_until', 'size_pct', 'size_notional_usd'):
            if d.get(field) != original.get(field):
                out.append('公开计划改写原始字段：' + field)
        q = row.get('three_a_qualification') or {}
        audit_plan = dict(original)
        # A previously immediate plan is now a fixed research limit. A changed
        # live quote may wait above that limit; this never authorizes chasing it.
        if (audit_plan.get('action') == '立即买入'
                and not (d.get('execution_readiness') or {}).get('executable_now')):
            audit_plan['action'] = '限价买入'
            audit_plan['text'] = str(audit_plan.get('text') or '').replace('立即买入', '限价买入', 1)
        candidate = dict(row, directive=audit_plan,
                         buyable=q.get('pre_requirement_buyable', row.get('buyable')))
        out.extend(violations(candidate, module, now))
        ready = d.get('execution_readiness') or {}
        if act == '策略条件匹配' and not ready.get('executable_now'):
            out.append('执行前置条件未满足却标策略条件匹配')
        if act == '研究候选' and (row.get('buyable') or q.get('qualified')):
            out.append('未认证研究候选被标可买或达标')
        return out
    if act not in BUY + ('财报后买入', '不买'):
        return ['指令动作不在五种之内']
    if not str(d.get('text') or '').startswith(act):
        out.append('页面/飞书文本与指令动作不一致')
    if act == '不买':
        return out
    price, stop, target = d.get('price'), d.get('stop'), d.get('target')
    gaps = [str(g) for g in (row.get('missing') or row.get('gaps') or [])]
    hard = [g for g in gaps if any(m in g for m in HARD)]
    if hard:
        out.append('存在硬伤仍给买入：' + hard[0][:30])
    er = row.get('earnings_risk') or {}
    when = _date(er.get('earnings_date'))
    today = _day(now)
    if act in BUY and when:
        if module == 'Astra':
            deadline = _date((row.get('window') or {}).get('deadline'))
            if deadline and today - timedelta(days=1) <= when <= deadline:
                out.append(f'持有期内有财报（{when}）仍给买入')
        elif today <= when <= today + timedelta(days=BLACKOUT_DAYS_3A):
            out.append(f'财报静默窗内（{when}）仍给买入')
    if act == '财报后买入':
        return out
    if not (_n(price) and _n(stop) and _n(target)):
        return out + ['买入指令缺价格/止损/目标']
    if not stop < price < target:
        out.append('价格不在止损与目标之间')
        return out
    reward = (target / price - 1) * 100 - FEE
    risk = (1 - stop / price) * 100 + FEE
    floor = rr_floor(row, module) or MIN_RR[module]
    if reward / risk < floor:
        out.append(f'指令价处净盈亏比{reward / risk:.2f}不足{floor}')
    last = row.get('last')
    if act == '立即买入' and _n(last) and last > price * 1.001:
        out.append('现价高于立即买入限价（追高）')
    if module == '3A':
        gate = row.get('entry_gate') or {}
        if not gate:
            out.append('未经入场门评估仍给买入')
        if act == '立即买入' and gate.get('blocked') and not row.get('buyable'):
            out.append('入场门拦截仍给立即买入')
        headwind = gate.get('env') == 'headwind' and not (row.get('live_gate') or {}).get('trend_trial')
        if headwind and act == '挂单买入' and '半仓' not in str(d.get('size') or ''):
            out.append('逆风挂单未减半仓')
    else:
        rng = row.get('entry_range') or []
        if act == '立即买入' and len(rng) == 2 and _n(last) and not rng[0] <= last <= rng[1]:
            out.append('现价不在计划区间仍给立即买入')
        reasons = (row.get('live_gate') or {}).get('reasons') or []
        if any(str(x).startswith('大盘逆风') for x in reasons) and '半仓' not in str(d.get('size') or ''):
            out.append('逆风未减半仓')
    pct = d.get('size_pct')
    if module == '3A' and _n(pct) and pct > 25:
        out.append('单票仓位超过25%')
    return out


def casebook():
    """判例复算：返回 (通过数, 总数, 不一致明细)。同一进程只算一次。"""
    global _CASEBOOK
    if _CASEBOOK is None:
        from directive_casebook import run
        _CASEBOOK = run()
    return _CASEBOOK


def enforce(doc, module, now=None):
    """就地复核 doc['rows'] 的 directive；违规行降为不买。返回审计摘要并写入 doc['directive_audit']。"""
    from action_directive import _directive
    passed, total, drift = casebook()
    rows = doc.get('rows') or []
    failed = []
    checks = []
    for row in rows:
        d = row.get('directive') or {}
        bad = violations(row, module, now)
        underlying = d.get('signal_plan') or d
        if drift and underlying.get('action') in BUY + ('财报后买入',):
            bad = bad + ['判例库不一致（逻辑漂移），本轮不发买入指令']
        if bad:
            row['directive_original'] = d
            row['directive'] = _directive('不买', why=['GitHub一致性复核未通过：' + bad[0]] + bad[1:3],
                                          module=module)
            failed.append({'code': row.get('code'), 'name': row.get('name'), 'action': d.get('action'),
                           'rules': bad})
        from action_directive import buy_window, TODAY
        window = buy_window(row, row.get('directive') or {})
        current = row.get('directive') or {}
        row['buyable'] = (bool((current.get('execution_readiness') or {}).get('executable_now'))
                          if current.get('action') == '策略条件匹配' else window == TODAY)
        if 'rule_matched' in row:
            row['rule_matched'] = row['buyable']
        checks.append({'code': row.get('code'), 'name': row.get('name'), 'market': row.get('market'),
                       'original_action': d.get('action'),
                       'final_action': (row.get('directive') or {}).get('action'),
                       'passed': not bad, 'rules': bad, 'buy_window': window,
                       'buyable': row['buyable']})
    audit = {'version': VERSION, 'module': module, 'checked': len(rows), 'passed': len(rows) - len(failed),
             'failed': failed, 'rows': checks, 'casebook': {'passed': passed, 'total': total, 'drift': drift},
             'consistent': not failed and not drift}
    audit['text'] = (f"🔒 GitHub结论与Copilot逻辑一致性：判例 {passed}/{total} 一致 · 指令复核 "
                     f"{audit['passed']}/{audit['checked']} 通过"
                     + ('' if not failed else f"（{len(failed)} 条降为不买：" +
                        '、'.join(f"{x['name']}·{x['rules'][0][:16]}" for x in failed[:3]) + '）')
                     + ('' if not drift else ' · ⚠️ 判例不一致，买入指令已停发'))
    doc['directive_audit'] = audit
    return audit
