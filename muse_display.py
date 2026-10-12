"""Read-only Muse receipt projection and shared UI. Never computes a verdict."""
from datetime import datetime
from functools import lru_cache
from html import escape
import json
from pathlib import Path
import re
from muse_review import BJT, CRITERIA, digest, item, moment

SLOTS = {'morning': '早报 06:30 · 中港', 'afternoon': '午报 12:01 · 中港', 'evening': '晚报 19:01 · 美股'}


def slot_of(record):
    slot = (record.get('muse_review') or {}).get('slot')
    if slot in SLOTS:
        return slot
    identity = str(record.get('delivery_id', ''))
    return next((k for k in SLOTS if re.search(r'(?:[:\-])' + k + r'(?:$|[:\-])', identity)), 'unknown')


def date_of(record):
    identity = str(record.get('delivery_id', ''))
    match = re.search(r'v88-three-slot:(\d{4}-\d{2}-\d{2}):', identity)
    if match:
        return match[1]
    try:
        return moment(record.get('completed_at') or record['at']).astimezone(BJT).date().isoformat()
    except (KeyError, ValueError, TypeError):
        return 'unknown'


def project(ledger):
    """Successful receipts only; keep debug and superseded versions visibly separate."""
    rows = []
    for legacy_key, record in (ledger.get('deliveries') or {}).items():
        if record.get('complete') is not True or record.get('accepted') is not True:
            continue
        row = {k: record[k] for k in ('delivery_id', 'at', 'completed_at', 'analysis_time', 'data_time',
               'content_sha256', 'payload_parts', 'muse_review', 'supersedes') if k in record}
        row.setdefault('delivery_id', 'legacy-receipt:'+str(legacy_key))
        row.update(date=date_of(record), slot=slot_of(record), debug='debug-' in str(record.get('delivery_id', '')))
        rows.append(row)
    return sorted(rows, key=lambda r: r.get('completed_at') or r.get('at') or '')


def effective(records):
    regular = [r for r in records if not r.get('debug')]
    replaced = {r.get('supersedes') for r in regular if r.get('supersedes')}
    return [r for r in regular if r.get('delivery_id') not in replaced]


def result(review, row):
    vote = (review.get('accepted_items') or {}).get(row.get('item_id'), {})
    state = vote.get('verdict', 'unverified')
    # Accepted receipt is authoritative, but malformed/legacy incomplete metadata cannot create a green badge.
    if (state not in ('pass', 'fail', 'unverified') or not review.get('draft_sha256')
            or not row.get('item_sha256') or not review.get('verdict_blob_sha')
            or not all(vote.get(k) for k in ('reason', 'checked_at', 'evidence_ref'))):
        state = 'unverified'
    ready = bool((row.get('facts') or {}).get('buyable' if row.get('module') == '3A' else 'rule_matched'))
    return {'state': state, 'meaning': '认可买入条件' if ready else '认可重点观察（不代表可买）',
            'reason': vote.get('reason') if state != 'unverified' else '无本版有效裁决',
            'checked_at': vote.get('checked_at'), 'evidence_ref': vote.get('evidence_ref'),
            'criteria_version': review.get('criteria_version') or CRITERIA,
            'verdict_version': review.get('verdict_blob_sha'), 'draft_sha256': review.get('draft_sha256'),
            'verdict_commit_sha': review.get('verdict_commit_sha'),
            'item_sha256': row.get('item_sha256'), 'delivery_id': review.get('delivery_id')}


def unverified(reason):
    return {'state': 'unverified', 'reason': reason, 'criteria_version': CRITERIA}


def current(row, module, cache, now=None):
    now = now or datetime.now(BJT)
    try:
        if cache.get('sync_status') != 'complete' or not 0 <= (now-moment(cache['synced_at'])).total_seconds() <= 300:
            return unverified('回执同步待核；历史徽章见下方冻结记录')
        binding = item(row, module)
        market = binding['market']
        candidates = [r for r in effective(cache.get('records', []))
                      if (r['slot'] == 'evening' if market == 'us' else r['slot'] in ('morning', 'afternoon'))]
        if not candidates:
            return unverified('尚无对应时段的已投递裁决')
        record = candidates[-1]
        review = record.get('muse_review') or {}
        if record['date'] != now.astimezone(BJT).date().isoformat():
            return unverified('此前裁决仅适用于历史版本')
        # Diagnose missing collection before version drift; neither can grant certification.
        vote = (review.get('accepted_items') or {}).get(binding['item_id'])
        if not vote:
            snap = (cache.get('verdict_snapshots') or {}).get(record['delivery_id']) or {}
            submitted = (snap.get('snapshot') or {}).get('items') or []
            if any(r.get('item_id') == binding['item_id'] for r in submitted if isinstance(r, dict)):
                return unverified('Muse已提交本股裁决；本次投递未采用，详见双重验证')
            if snap.get('present') is False:
                return unverified('尚未收到本时段Muse裁决；未验证不等于不通过')
            if snap.get('present') is True:
                return unverified('本时段裁决未包含本股；等待Muse核查')
            return unverified('本次投递无本股有效裁决；提交状态待核')
        latest = (cache.get('latest_drafts') or {}).get(record['delivery_id'])
        if not latest or latest.get('draft_sha256') != review.get('draft_sha256'):
            return unverified('草案改版或版本未核对，等待本版裁决')
        expected = next((r for r in review.get('items', []) if r.get('item_id') == binding['item_id']), None)
        if not expected or expected.get('item_sha256') != binding['item_sha256']:
            return unverified('当前行情或条件已变化，旧裁决不沿用')
        if expected.get('system_state') != binding['system_state']:
            return unverified('系统状态已变化，等待本版裁决')
        return dict(result(review, expected), delivery_id=record['delivery_id'])
    except (KeyError, TypeError, ValueError, AttributeError):
        return unverified('绑定校验失败')


def esc(value):
    return escape(str(value if value is not None else '—'), quote=True)


def clock(value):
    try:
        return moment(value).astimezone(BJT).strftime('%m-%d %H:%M:%S 北京时间')
    except (ValueError, TypeError):
        return '—'


def badge(value):
    state = value.get('state', 'unverified')
    color, label = {'pass': ('#16734b', '✅ Muse 认证·通过'), 'fail': ('#b42332', '❌ 不通过'),
                    'unverified': ('#667085', '⚪ 未验证')}[state]
    meaning = (' · '+value['meaning']) if state == 'pass' else ''
    info = '标准 '+esc(value.get('criteria_version'))+' · 核查 '+esc(clock(value.get('checked_at')))+' · 裁决 '+esc(str(value.get('verdict_version') or '—')[:12])
    return ('<div class="muse-badge" style="font-size:11px;line-height:1.5;margin-top:5px">'
            '<span style="display:inline-block;border-radius:5px;padding:1px 5px;background:#f1f4f7;color:'+color+'">'
            '<b>M</b> '+label+'（试运行）</span>'+esc(meaning)+'<div>'+esc(value.get('reason'))+'</div>'
            '<details><summary style="color:#667085">'+info+'</summary>'
            '<div>evidence_ref：'+esc(value.get('evidence_ref'))+'</div><div>delivery_id：'+esc(value.get('delivery_id'))+
            '</div><div>draft_sha256：'+esc(value.get('draft_sha256'))+'</div><div>item_sha256：'+esc(value.get('item_sha256'))+
            '</div><div>裁决完整版本：'+esc(value.get('verdict_version'))+'</div><div>裁决提交：'+esc(value.get('verdict_commit_sha'))+'</div></details></div>')


@lru_cache(maxsize=4)
def _load(path, stamp, size):
    return json.loads(Path(path).read_text())


def load(base=None):
    if base is None:
        from v88_paths import core_root
        base = core_root()
    path = Path(base)/'data/muse_review_web.json'
    try:
        stat = path.stat()
        return _load(str(path), stat.st_mtime_ns, stat.st_size)
    except (OSError, ValueError):
        return {}


def row_badge(row, module, cache=None, now=None):
    return badge(current(row, module, load() if cache is None else cache, now))


def counts(record):
    review = record.get('muse_review') or {}
    out = dict.fromkeys(('pass', 'fail', 'unverified'), 0)
    for row in review.get('items') or []:
        out[result(review, row)['state']] += 1
    return out


def panel(cache=None, now=None, day=None):
    cache = load() if cache is None else cache
    now = now or datetime.now(BJT); today = now.astimezone(BJT).date().isoformat()
    records = cache.get('records', [])
    if day:
        records = [r for r in records if r['date'] == day]
        today = day
    active = effective(records)
    out = ['<section id="v88-muse-review" style="margin:12px 0;padding:12px;border:1px solid #e2e8f0;border-radius:10px">',
           '<b>M · Muse 双重验证（试运行）</b><p style="font-size:12px">认证≠买入授权。认可重点观察只认可观察资格，不代表可买；不改变评分、榜单或持仓。无裁决、超时、改版、校验失败统一为未验证。</p>',
           '<small>只读飞书冻结回执 · 同步 '+esc(clock(cache.get('synced_at')))+' · '+esc(cache.get('sync_status') or '待同步')+'</small>']
    if cache.get('receipt_conflicts'):
        out.append('<p>回执同ID内容变更待核：保留首次成功版本，冲突副本只归档，不改历史徽章。</p>')
    for slot, label in SLOTS.items():
        selected = [r for r in active if r['date'] == today and r['slot'] == slot]
        total = dict.fromkeys(('pass', 'fail', 'unverified'), 0)
        for r in selected:
            for k, v in counts(r).items():total[k] += v
        summary = ('通过 {pass}｜未通过 {fail}｜未验证 {unverified}'.format(**total) if selected else '尚无已投递回执')
        if selected and not any(r.get('muse_review') for r in selected):
            summary = '未验证 · 数量未留档'
        out.append('<p><b>'+label+'</b> · '+summary)
        if selected and not any(r.get('muse_review') for r in selected):out.append(' · 旧回执无认证字段，不能按已验证计数')
        out.append('</p>')
    pending = cache.get('verdict_snapshots') or {}
    if pending:
        out.append('<details><summary>裁决仓库快照（仅供核对；不覆盖投递回执）</summary>')
        for identity, snap in pending.items():
            if day and f':{today}:' not in identity:
                continue
            out.append('<p>'+esc(identity)+' · '+('已看到裁决文件，是否采用以回执为准' if snap.get('present') else '未见裁决文件')+' · 快照 '+esc(str(snap.get('snapshot_sha256') or '—')[:12])+'</p>')
            submitted = snap.get('snapshot') or {}
            for vote in submitted.get('items') or []:
                if not isinstance(vote, dict):continue
                label = {'pass':'认可', 'fail':'不认可', 'unverified':'未验证'}.get(vote.get('verdict',vote.get('claude_verdict')), '格式待核')
                out.append('<details><summary>'+esc(vote.get('code'))+' · 仓库原裁决：'+label+'（非当前认证）</summary><p>'+
                           esc(vote.get('reason'))+'<br>核查 '+esc(clock(vote.get('checked_at')))+' · 标准 '+esc(submitted.get('criteria_version'))+
                           '<br>证据 '+esc(vote.get('evidence_ref'))+'<br>草案 '+esc(submitted.get('draft_sha256'))+
                           '</p><small>仅展示Muse原始提交；不据此推断已赶上截止或通过版本校验，认证仍以冻结投递回执为准。</small></details>')
        out.append('</details>')
    stats = cache.get('statistics') or {}
    out.append('<details><summary>验收进度与冻结核查标准</summary><p>已投递复核覆盖 '+esc(stats.get('delivered_review_days', 0))+
        ' 天 · 有锚点独立样本 '+esc(stats.get('unique_anchored_verdict_plans', 0))+'/30；工程观察至少21天，成熟配对结果尚需独立验收，不自动转正、不调权重。</p>'
        '<p>muse-shadow-v1：原公告；经常性利润；多年利润/现金流/周期；当前结构与情景分开；买入净空间≥5%且净盈亏比≥2；行情/价带/止损；治理未出清可否决观察；独立可追溯证据；重复持仓仅提醒；标准冻结。</p></details>')
    replaced = {r.get('supersedes'): r['delivery_id'] for r in records if r.get('supersedes') and not r.get('debug')}
    out.append('<details><summary>逐份明细与历史冻结记录 · '+str(len(records))+'份</summary>')
    for day in sorted({r['date'] for r in records}, reverse=True):
        out.append('<details'+(' open' if day == today else '')+'><summary>'+esc(day)+' · 日报与历史冻结认证</summary>')
        for record in reversed([r for r in records if r['date'] == day]):
            identity = record['delivery_id'];review = record.get('muse_review') or {}
            out.append('<details><summary>'+esc(SLOTS.get(record['slot'], '未标明原时段'))+' · '+esc(clock(record.get('completed_at') or record.get('at')))+
                       (' · 测试记录（不计汇总）' if record.get('debug') else '')+(' · 已被替代' if identity in replaced else '')+'</summary>')
            out.append('<p>'+esc(identity)+'<br>supersedes：'+esc(record.get('supersedes'))+' · 替代本版：'+esc(replaced.get(identity))+'</p>')
            arrived = review.get('received_at');cutoff = review.get('cutoff_at')
            try:timely = '赶上截止' if arrived and cutoff and moment(arrived) <= moment(cutoff) else '没有有效的截止前裁决'
            except (TypeError, ValueError):timely = '截止校验失败'
            if not cutoff:timely = '旧回执未记录截止元数据，不推断是否准时'
            out.append('<p>原定 '+esc(clock(review.get('scheduled_at')))+' · 实际发送 '+esc(clock(record.get('completed_at') or record.get('at')))+
                '<br>筛选 '+esc(record.get('analysis_time'))+' · 行情 '+esc(record.get('data_time'))+
                '<br>截止 '+esc(clock(cutoff))+' · 收到裁决 '+esc(clock(arrived))+' · '+timely+'</p>')
            for row in review.get('items') or []:
                out.append('<p><b>'+esc(row.get('module'))+' · '+esc(row.get('name'))+' '+esc(row.get('code'))+'</b></p>'+badge(dict(result(review,row),delivery_id=identity)))
            if not review:out.append('<p>⚪ 未验证 · 旧回执没有 Muse 认证字段；不补写历史认证。</p>')
            out.append('<details><summary>已投递正文（按 payload_parts 原序）</summary><pre style="white-space:pre-wrap;font-size:12px">'+esc('\n\n'.join(record.get('payload_parts') or []))+'</pre><small>旧回执可能不含发送层时间行与分段编号，不宣称与飞书完整消息逐字一致。</small></details></details>')
        out.append('</details>')
    out.append('</details></section>')
    return ''.join(out)
