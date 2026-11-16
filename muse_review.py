"""Non-blocking, version-bound Muse shadow annotations. No decision authority."""
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone, timedelta
import hashlib
import json
import unicodedata

BJT = timezone(timedelta(hours=8))
SCHEMA = 1
CRITERIA = 'muse-shadow-v1'
DELIVERY_POLICY = 'published-draft-v1'
STOP_QUALITY_NOTICE = '止损质量核查：波幅与结构锚、止损触发口径、仓位风险（含0.3%–0.5%滑点）；是否完成以本条裁决证据为准，不自动改动原止损。'
CAPTURE = ContextVar('muse_capture', default=None)
CLOCKS = {'morning': (6, 20, 6, 30), 'afternoon': (11, 55, 12, 1), 'evening': (18, 50, 19, 1)}
MARKETS = {'A股': 'cn', '港股': 'hk', '美股': 'us'}


def display_code(code):
    import re
    m = re.fullmatch(r'(\d{1,4})\.HK', str(code or ''))
    return m.group(1).zfill(4) + '.HK' if m else code


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def key(delivery_id):
    return hashlib.sha256(delivery_id.encode()).hexdigest()


@contextmanager
def capture():
    rows = []
    token = CAPTURE.set(rows)
    try:
        yield rows
    finally:
        CAPTURE.reset(token)


BOARD_MODULES = ('3A', '2A', '1A')   # 策略看板三档，均送 Muse 双重验证


def item(row, module):
    fields = ('code', 'name', 'last', 'currency', 'quote_asof', 'source_session', 'strategy_score',
              'entry_range', 'take_profit_range', 'stop_range', 'opportunity', 'net_rr',
              'net_target_return_pct', 'period', 'window', 'reason', 'business_reason', 'business_risk',
              'buyable', 'rule_matched', 'status', 'selection_category', 'missing', 'profit_gaps',
              'first_seen_at', 'business_source', 'filing_check', 'fundamentals',
              'crosscheck', 'price_plan_basis', 'price_plan_method', 'price_plan_scope', 'target_evidence',
              'financial_evidence', 'directive', 'research_roster', 'plan_id', 'plan_created_at', 'plan_state', 'plan_waiting_reasons',
              'plan_current_structure_target', 'plan_current_net_space_pct', 'plan_current_net_rr',
              'plan_current_first_obstacle_pct', 'plan_current_nearest_resistance',
              'grade_basis', 'ladder', 'fundamentals_state', 'astra_regime', 'astra_stop_check',
              'entry_gate', 'live_gate', 'earnings_risk', 'entry_location', 'price_volume',
              'decision_policy', 'max_sessions', 'monthly_entry_deadline', 'month_sessions_left',
              'time_feasible', 'fee_allowance_pct', 'price_plan_version', 'price_plan_source_asof',
              'stop_anchor', 'invalidation', 'three_a_qualification', 'entry_window_qualification')
    facts = {k: row[k] for k in fields if k in row}
    window = row.get('buy_window')
    planned = 'buy_window' in row or bool(row.get('entry_range'))
    if window is None and planned:
        import action_directive as ad
        d = row.get('directive') or (ad.decide_astra(row) if module == 'Astra' else ad.decide_3a(row))
        window = ad.buy_window(row, d)
    # Legacy rounds use their buy window. Current projections additionally bind
    # readiness and qualification; their new labels need not equal legacy TODAY.
    if planned:
        facts['buy_window'] = window
    if module in BOARD_MODULES and planned:
        import action_directive as ad
        facts['buyable'] = window == ad.TODAY
    readiness = (row.get('directive') or {}).get('execution_readiness')
    if isinstance(readiness, dict):
        facts['buyable'] = bool(row.get('buyable') is True and readiness.get('executable_now') is True)
    qualification = row.get('three_a_qualification')
    if module == '3A' and isinstance(qualification, dict) and qualification.get('qualified') is not True:
        facts['buyable'] = False
    if row.get('directive'):
        from muse_evidence import review_directive
        facts['directive'] = review_directive(row, module)
    from muse_evidence import review_evidence, decision_contract, stable_review_facts
    facts['review_evidence'] = review_evidence(row, module)
    facts['decision_contract'] = decision_contract(row, module)
    facts = stable_review_facts(facts)
    identity = f'{module}:{MARKETS.get(row.get("market"), "unknown")}:{row["code"]}'
    return {'item_id': identity, 'module': module, 'market': MARKETS.get(row.get('market'), 'unknown'),
            'code': row['code'], 'display_code': display_code(row['code']), 'name': row['name'],
            'system_state': str(window or row.get('selection_category') or row.get('status') or '待核'),
            'plan_anchor': row.get('first_seen_at') or (row.get('window') or {}).get('start'),
            'facts': facts, 'item_sha256': digest(facts)}


def collect(row, module, lines):
    rows = CAPTURE.get()
    if rows is not None:
        try:
            value = item(row, module)
        except Exception:
            lines.append('  Muse 验证（试运行）：⚪ 未验证（条目校验异常）')
            return
        rows.append(value)
        # Removed before the normal body is frozen; never reaches existing reports.
        lines.append('[[MUSE:' + value['item_id'] + ']]')


def build(snapshot, text, rows, delivery_id, slot, now, *, freeze_for_delivery=False):
    lines = text.splitlines()
    if freeze_for_delivery:
        # Part of the reviewed body/hash, not a timestamp added at send time.
        lines.insert(1, '🧊 本时段定版：' + now.astimezone(BJT).strftime('%Y-%m-%d %H:%M:%S')
                     + '（北京时间）；名单、价格及认证均对应本版。最新筛选见V88，交易前须重新核对。')
    items = []
    clean = []
    by_id = {r['item_id']: r for r in rows}
    for line in lines:
        if line.startswith('[[MUSE:') and line.endswith(']]'):
            identity = line[7:-2]
            items.append(dict(by_id[identity], insert_after=len(clean)))
        else:
            clean.append(line)
    if items:
        # Count derived from the very items Muse reviews.
        counts = ' · '.join(f'{m} {sum(i["module"] == m for i in items)}只' for m in BOARD_MODULES + ('Astra',))
        clean.insert(2 if freeze_for_delivery else 1, f'🔍 本报送审条目 {len(items)} 条（{counts}）；审核只认这些条目。')
        items = [dict(i, insert_after=i['insert_after'] + 1) for i in items]
    body = '\n'.join(clean)
    # Only compilation clocks vary. Quote, screening, plan and state lines remain bound.
    semantic = '\n'.join(x for x in clean[1:] if not x.startswith('📝 摘要编制：'))
    payload = {'schema_version': SCHEMA, 'delivery_id': delivery_id, 'slot': slot['id'],
               'criteria_version': CRITERIA, 'items': items, 'semantic_body': semantic}
    from muse_evidence import snapshot_context
    payload['review_context'] = snapshot_context(snapshot)
    if freeze_for_delivery:
        payload['delivery_policy'] = DELIVERY_POLICY
    h, m, sh, sm = CLOCKS[slot['id']]
    date = now.astimezone(BJT).date()
    cutoff = datetime(date.year, date.month, date.day, h, m, tzinfo=BJT)
    from feishu_push import split_messages
    result = {**payload, 'review_sha256': digest(payload), 'created_at': now.isoformat(),
            'date': str(date), 'cutoff_at': cutoff.isoformat(),
            'scheduled_at': cutoff.replace(hour=sh, minute=sm).isoformat(),
            'resubmission_until': (cutoff.replace(hour=sh, minute=sm) + timedelta(minutes=2)).isoformat(),
            'resubmission_policy': 'invalid-only-once-before-send-v1',
            'source_generated_at': snapshot.get('source_generated_at'), 'snapshot_id': snapshot.get('snapshot_id'),
            'verdict_repository': 'bluestevenr-lang/v88-muse-verdicts',
            'verdict_file': f'verdicts/{date}/{key(delivery_id)}.json',
            'text': body, 'payload_parts': split_messages(body, max_len=2800)}
    if freeze_for_delivery:
        # Beyond the normal catch-up window, use a fresh unverified report.
        result['send_until'] = (cutoff.replace(hour=sh, minute=sm) + timedelta(minutes=90)).isoformat()
    result['draft_sha256'] = digest(result)
    return result


def moment(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError('timezone required')
    return result


def one_line(value, limit):
    return (isinstance(value, str) and 0 < len(value) <= limit
            and not any(unicodedata.category(c).startswith('C') or c in '\r\n' for c in value))


SLOT_ALIASES = {'noon': 'afternoon'}


def _canonical_envelope(verdict):
    """午报协议名历史上出现过 noon；系统统一用 afternoon，裁决侧写 noon 也按同一时段绑定。"""
    if not isinstance(verdict, dict):
        return verdict
    out = dict(verdict)
    slot = out.get('slot')
    if slot in SLOT_ALIASES:
        out['slot'] = SLOT_ALIASES[slot]
    ident = out.get('delivery_id')
    if isinstance(ident, str):
        head, _, tail = ident.rpartition(':')
        if head and tail in SLOT_ALIASES:
            out['delivery_id'] = head + ':' + SLOT_ALIASES[tail]
    return out


def validate(draft, verdict, received_at, rejected=None, *, retry_until=None):
    """Strict envelope, independently reject bad items. received_at is our collector clock.

    rejected：可选列表，逐条记录被拒条目与原因，供收集状态文件诊断（不影响判定）。
    条目未写 checked_at 时沿用裁决外层 checked_at（Muse 实际提交格式），仍须落在草案创建与接收之间。
    """
    note = (lambda item, why: rejected.append({'item_id': item, 'reason': why})) if rejected is not None else (lambda item, why: None)
    verdict = _canonical_envelope(verdict)
    if not isinstance(verdict, dict) or any(verdict.get(k) != draft[k] for k in
            ('schema_version', 'delivery_id', 'slot', 'draft_sha256', 'criteria_version')):
        raise ValueError('draft binding mismatch')
    received = moment(received_at)
    cutoff = moment(draft['cutoff_at'])
    if retry_until is not None:
        deadline = moment(retry_until)
        scheduled = moment(draft['scheduled_at'])
        if (deadline != scheduled + timedelta(minutes=2) or deadline <= cutoff
                or draft.get('resubmission_policy') != 'invalid-only-once-before-send-v1'
                or draft.get('resubmission_until') != retry_until):
            raise ValueError('invalid resubmission window')
        cutoff = deadline
    if not moment(draft['created_at']) <= received <= cutoff:
        raise ValueError('outside review window')
    if not isinstance(verdict.get('items'), list) or len(verdict['items']) > 30:
        raise ValueError('invalid item envelope')
    expected = {r['item_id']: r for r in draft['items']}
    counts = {}
    for v in verdict['items']:
        if isinstance(v, dict) and isinstance(v.get('item_id'), str):
            counts[v['item_id']] = counts.get(v['item_id'], 0) + 1
    envelope_checked = verdict.get('checked_at')
    accepted = {}
    for v in verdict['items']:
        if not isinstance(v, dict):
            note(None, 'not_object')
            continue
        identity = v.get('item_id')
        if not isinstance(identity, str):
            note(None, 'item_id_missing')
            continue
        r = expected.get(identity)
        if not r:
            note(identity, 'unknown_item')
            continue
        if counts.get(identity) != 1:
            note(identity, 'duplicate_item')
            continue
        bad = [k for k in ('module', 'market', 'code', 'name', 'system_state', 'item_sha256') if v.get(k) != r[k]]
        if bad:
            note(identity, 'field_mismatch:' + ','.join(bad))
            continue
        state = v.get('verdict', v.get('claude_verdict'))
        if state not in ('pass', 'fail', 'unverified'):
            note(identity, 'verdict_value_invalid')
            continue
        if not one_line(v.get('reason'), 40):
            note(identity, 'reason_invalid')
            continue
        if not one_line(v.get('evidence_ref'), 500):
            note(identity, 'evidence_ref_invalid')
            continue
        checked = v.get('checked_at') or envelope_checked
        try:
            if not moment(draft['created_at']) <= moment(checked) <= received:
                note(identity, 'checked_at_outside_window')
                continue
        except (ValueError, TypeError, KeyError):
            note(identity, 'checked_at_invalid')
            continue
        accepted[identity] = {'item_id': identity, 'reason': v['reason'], 'evidence_ref': v['evidence_ref'],
                              'checked_at': checked, 'verdict': state}
    return accepted


def merge(draft, accepted=None, *, error=None, verdict_sha=None):
    accepted = accepted or {}
    counts = dict.fromkeys(('pass', 'fail', 'unverified'), 0)
    insertions = {}
    for row in draft['items']:
        v = accepted.get(row['item_id'], {})
        state = v.get('verdict', 'unverified')
        counts[state] += 1
        ready = bool(row['facts'].get('buyable') if row['module'] in BOARD_MODULES else row['facts'].get('rule_matched'))
        label = {'pass': '✅ 认可买入条件' if ready else '✅ 认可重点观察',
                 'fail': '❌ 不认可', 'unverified': '⚪ 未验证'}[state]
        suffix = '｜' + v['reason'] if v.get('reason') and state != 'unverified' else ''
        insertions.setdefault(row['insert_after'], []).extend([
            '  标准 ' + draft['criteria_version'] + '｜核查 ' + str(v.get('checked_at') or '—') + '｜裁决 ' + str(verdict_sha or '—')[:12],
            '  Muse 验证（试运行）：' + label + suffix])
    lines = []
    for index, line in enumerate(draft['text'].splitlines(), 1):
        lines.append(line)
        lines.extend(insertions.get(index, []))
    lines += ['', f'Muse 双重验证（试运行）：通过{counts["pass"]}｜未通过{counts["fail"]}｜未验证{counts["unverified"]}；仅针对本版草案，不代表收益保证；认证≠买入授权，观察通过不代表买入。',
              STOP_QUALITY_NOTICE,
              '核对编号：' + draft['delivery_id'] + ' · 草案 ' + draft['draft_sha256'][:12]]
    audit = {'schema_version': SCHEMA, 'mode': 'shadow', 'draft_sha256': draft['draft_sha256'],
             'criteria_version': draft['criteria_version'], 'counts': counts, 'verdict_blob_sha': verdict_sha,
             'delivery_id': draft['delivery_id'], 'slot': draft['slot'], 'date': draft.get('date'),
             'created_at': draft.get('created_at'), 'cutoff_at': draft.get('cutoff_at'),
             'scheduled_at': draft.get('scheduled_at'), 'review_sha256': draft.get('review_sha256'),
             'source_generated_at': draft.get('source_generated_at'), 'display_version': 'muse-display-v2-stop-notice',
             'review_notice': STOP_QUALITY_NOTICE,
             'fallback': error, 'accepted_items': accepted, 'items': draft['items']}
    if draft.get('delivery_policy'):
        audit.update(delivery_policy=draft['delivery_policy'], send_until=draft.get('send_until'),
                     source_snapshot_id=draft.get('snapshot_id'))
    return '\n'.join(lines), audit
