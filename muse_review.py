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
STOP_QUALITY_NOTICE = '止损质量核查：波幅与结构锚、止损触发口径、仓位风险（含0.3%–0.5%滑点）；是否完成以本条裁决证据为准，不自动改动原止损。'
CAPTURE = ContextVar('muse_capture', default=None)
CLOCKS = {'morning': (6, 20, 6, 30), 'afternoon': (11, 55, 12, 1), 'evening': (18, 50, 19, 1)}
MARKETS = {'A股': 'cn', '港股': 'hk', '美股': 'us'}


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


def item(row, module):
    fields = ('code', 'name', 'last', 'currency', 'quote_asof', 'source_session', 'strategy_score',
              'entry_range', 'take_profit_range', 'stop_range', 'opportunity', 'net_rr',
              'net_target_return_pct', 'period', 'window', 'reason', 'business_reason', 'business_risk',
              'buyable', 'rule_matched', 'status', 'selection_category', 'missing', 'profit_gaps',
              'first_seen_at', 'business_source', 'filing_check', 'fundamentals')
    facts = {k: row[k] for k in fields if k in row}
    identity = f'{module}:{MARKETS.get(row.get("market"), "unknown")}:{row["code"]}'
    return {'item_id': identity, 'module': module, 'market': MARKETS.get(row.get('market'), 'unknown'),
            'code': row['code'], 'name': row['name'],
            'system_state': str(row.get('selection_category') or row.get('status') or '待核'),
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


def build(snapshot, text, rows, delivery_id, slot, now):
    lines = text.splitlines()
    items = []
    clean = []
    by_id = {r['item_id']: r for r in rows}
    for line in lines:
        if line.startswith('[[MUSE:') and line.endswith(']]'):
            identity = line[7:-2]
            items.append(dict(by_id[identity], insert_after=len(clean)))
        else:
            clean.append(line)
    body = '\n'.join(clean)
    # Only compilation clocks vary. Quote, screening, plan and state lines remain bound.
    semantic = '\n'.join(x for x in clean[1:] if not x.startswith('📝 摘要编制：'))
    payload = {'schema_version': SCHEMA, 'delivery_id': delivery_id, 'slot': slot['id'],
               'criteria_version': CRITERIA, 'items': items, 'semantic_body': semantic}
    h, m, sh, sm = CLOCKS[slot['id']]
    date = now.astimezone(BJT).date()
    cutoff = datetime(date.year, date.month, date.day, h, m, tzinfo=BJT)
    from feishu_push import split_messages
    result = {**payload, 'review_sha256': digest(payload), 'created_at': now.isoformat(),
            'date': str(date), 'cutoff_at': cutoff.isoformat(),
            'scheduled_at': cutoff.replace(hour=sh, minute=sm).isoformat(),
            'source_generated_at': snapshot.get('source_generated_at'), 'snapshot_id': snapshot.get('snapshot_id'),
            'verdict_repository': 'bluestevenr-lang/v88-muse-verdicts',
            'verdict_file': f'verdicts/{date}/{key(delivery_id)}.json',
            'text': body, 'payload_parts': split_messages(body, max_len=2800)}
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


def validate(draft, verdict, received_at):
    """Strict envelope, independently reject bad items. received_at is our collector clock."""
    if not isinstance(verdict, dict) or any(verdict.get(k) != draft[k] for k in
            ('schema_version', 'delivery_id', 'slot', 'draft_sha256', 'criteria_version')):
        raise ValueError('draft binding mismatch')
    received = moment(received_at)
    if not moment(draft['created_at']) <= received <= moment(draft['cutoff_at']):
        raise ValueError('outside review window')
    if not isinstance(verdict.get('items'), list) or len(verdict['items']) > 30:
        raise ValueError('invalid item envelope')
    expected = {r['item_id']: r for r in draft['items']}
    counts = {}
    for v in verdict['items']:
        if isinstance(v, dict) and isinstance(v.get('item_id'), str):
            counts[v['item_id']] = counts.get(v['item_id'], 0) + 1
    accepted = {}
    for v in verdict['items']:
        if not isinstance(v, dict):
            continue
        identity = v.get('item_id')
        if not isinstance(identity, str):
            continue
        r = expected.get(identity)
        if not r or counts.get(identity) != 1:
            continue
        if any(v.get(k) != r[k] for k in ('module', 'market', 'code', 'name', 'system_state', 'item_sha256')):
            continue
        state = v.get('verdict', v.get('claude_verdict'))
        if state not in ('pass', 'fail', 'unverified') or not one_line(v.get('reason'), 40):
            continue
        if not one_line(v.get('evidence_ref'), 500):
            continue
        try:
            if not moment(draft['created_at']) <= moment(v['checked_at']) <= received:
                continue
        except (ValueError, TypeError, KeyError):
            continue
        accepted[identity] = {k: v[k] for k in ('item_id', 'reason', 'evidence_ref', 'checked_at')}
        accepted[identity]['verdict'] = state
    return accepted


def merge(draft, accepted=None, *, error=None, verdict_sha=None):
    accepted = accepted or {}
    counts = dict.fromkeys(('pass', 'fail', 'unverified'), 0)
    insertions = {}
    for row in draft['items']:
        v = accepted.get(row['item_id'], {})
        state = v.get('verdict', 'unverified')
        counts[state] += 1
        ready = bool(row['facts'].get('buyable') if row['module'] == '3A' else row['facts'].get('rule_matched'))
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
    return '\n'.join(lines), audit
