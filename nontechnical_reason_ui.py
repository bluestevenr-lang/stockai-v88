"""Read-only business-evidence display shared by central lists and deep analysis."""
from datetime import datetime, timezone
from html import escape
from pathlib import Path
import sys
from threading import RLock
import time
from urllib.parse import urlsplit

from v88_paths import core_root

_CACHE = {}
_LOCK = RLock()
_MAX_AGE_SECONDS = 30
_SOURCES = ('financial_primary_evidence.json', 'joint_public_sources.json',
            'nontechnical_primary_reasons.json')


def _api():
    src = str(core_root() / 'src')
    if src not in sys.path:
        sys.path.insert(0, src)
    from recommendation_rationale import build_index, rationale_for
    return build_index, rationale_for


def load_index(base=None):
    """One verified read per file generation/30 seconds, shared across all rows."""
    root = Path(base or core_root()).resolve()
    stamps = []
    for name in _SOURCES:
        try:
            stat = (root / 'data' / name).stat()
            stamps.append((name, stat.st_mtime_ns, stat.st_size))
        except OSError:
            stamps.append((name, None, None))
    key = (str(root), tuple(stamps), datetime.now(timezone.utc).date().isoformat())
    with _LOCK:
        saved = _CACHE.get(str(root))
        if saved and saved[0] == key and time.monotonic() - saved[1] < _MAX_AGE_SECONDS:
            return saved[2]
        try:
            build, _ = _api()
            doc = build(root)
        except Exception:
            doc = {'version': 'recommendation-rationale-v1', 'records': {},
                   'errors': ['经营依据暂未读取成功，补齐后重新核验']}
        _CACHE[str(root)] = (key, time.monotonic(), doc)
        return doc


def for_code(code, index=None, row=None, base=None):
    """A missing or broken evidence reader cannot take down the stock page."""
    try:
        _, resolve = _api()
        result = resolve(str(code or ''), index if index is not None else load_index(base), row)
        if not isinstance(result, dict):
            raise ValueError('invalid rationale result')
        return result
    except Exception:
        # Keep the signed grade/contract readable, but never infer business
        # support from the prior render or a technical recommendation.
        return {'code': str(code or ''), 'status': 'pending',
                'has_current_non_technical_support': False,
                'summary': '经营依据暂未读取成功，补齐后重新核验',
                'evidence': [], 'risks': [],
                'gaps': ['经营依据暂未读取成功，补齐后重新核验'],
                'no_grade_authority': True}


def has_support(rationale):
    return rationale.get('has_current_non_technical_support') is True


def _short(value, size=76):
    value = str(value or '')
    return value if len(value) <= size else value[:size] + '…'


def html(rationale, *, compact=True):
    """Show support AND contrary facts; sources and full wording stay folded."""
    esc = lambda value: escape(str(value or ''), quote=True)
    support = has_support(rationale)
    risks = rationale.get('risks') or []
    counter = rationale.get('risk_summary') or (risks[0] if risks else '')
    label = ('🏭 经营支持 · ⚠ 有反证' if support and counter else '🏭 经营支持已核') if support else '○ 经营依据待核 · 仅技术观察'
    color = '#0369a1' if support else '#b45309'
    body = f'<b style="font-size:13px;color:{color}">{label}</b>'
    if support and rationale.get('support_summary'):
        body += '<div style="margin-top:4px">⊕ ' + esc(_short(rationale['support_summary'])) + '</div>'
    if counter:
        body += '<div style="color:#b45309;margin-top:3px">⚠ ' + esc(_short(counter)) + '</div>'
    sources = []
    for item in rationale.get('evidence') or []:
        url = str(item.get('source_url') or '')
        parsed = urlsplit(url)
        if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password:
            continue
        direction = {'support': '⊕ 支持', 'counterevidence': '⚠ 反证', 'neutral': '○ 中性'}.get(item.get('direction'), '○ 待核')
        current = '当前报告期' if item.get('current') else '历史/待更新'
        sources.append('<div style="margin:5px 0">'+direction+' · '+esc(item.get('claim'))
            +'<br><a href="'+esc(url)+'" target="_blank" rel="noopener noreferrer">↗ 原始来源</a> · '
            +esc(str(item.get('published_at') or '发布时间待核')[:10])+' · 数据截至 '+esc(item.get('asof'))+' · '+current+'</div>')
    detail = ''.join(sources) or '<div>财报、经营或催化正文证据尚未补齐。</div>'
    if risks:
        detail += '<div>⚠ '+esc('；'.join(dict.fromkeys(risks)))+'</div>'
    gaps = rationale.get('gaps') or []
    if gaps:
        detail += '<div>○ 待核：'+esc('；'.join(dict.fromkeys(gaps)))+'</div>'
    detail += '<div>原中央评级与合同保留；经营证据不单独授予买入权限。</div>'
    body += '<details style="font-size:11px;color:#64748b;margin-top:4px"><summary>证据来源 · 反证与待核</summary>'+detail+'</details>'
    return '<section class="v88-nontechnical-reason" data-business-support="'+str(support).lower()+'" style="font-size:12px;line-height:1.5;margin:5px 0;padding:6px 8px;background:#f8fafc;border-left:3px solid '+color+';border-radius:4px">'+body+'</section>'


def action_html(original, rationale):
    """A missing business case cannot surface as a fresh executable suggestion."""
    if has_support(rationale):
        return original
    return ('<div class="v88-business-action-hold" style="color:#b45309;font-size:13px">'
            '<b>○ 经营依据待核 · 仅技术观察</b>'
            '<div style="font-size:11px">补齐经营支持后再核入场；原评级与合同保留。</div>'
            '<details style="font-size:11px;color:#64748b"><summary>原触发核验 · 不作本次执行许可</summary>'
            + original + '</details></div>')
