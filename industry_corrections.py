"""Audited classification corrections; never renew quotes or rewrite raw sources."""
from copy import deepcopy
from datetime import datetime
from zoneinfo import ZoneInfo
import hashlib
import json
import math

REVISION = '2026-10-10-pr-industry-v1'
PR_INDUSTRY = '石油与天然气的勘探与生产'


def corrected_record(row):
    # Exact security AND company identity: a future reuse of ticker PR must not
    # inherit today's correction. Keep the provider value in the audit trail.
    if (row.get('code') != 'PR' or row.get('market') != '美股'
            or row.get('source_security_id') != 'PR.N'
            or str(row.get('name_en', '')).strip().casefold() != 'permian resources corporation'
            or row.get('industry') == PR_INDUSTRY):
        return row
    out = dict(row)
    out['industry_correction'] = {
        'revision': REVISION, 'provider_industry': row.get('industry'),
        'verified_on': '2026-10-10',
        'source_url': 'https://permianres.com/operations/',
        'reason': '发行人主营油气勘探生产；映射到现有同业分类，不沿用银行组排名。'}
    out['industry'] = PR_INDUSTRY
    out['industry_rank'] = None
    payload = json.dumps([out['market'], out['industry_taxonomy'], PR_INDUSTRY],
                         ensure_ascii=False, separators=(',', ':'))
    out['industry_group_id'] = hashlib.sha256(payload.encode()).hexdigest()[:24]
    if row.get('name_kind') == '英文原名＋中文行业说明；非官方中文名':
        out['name_zh'] = out['name_en'] + '（' + PR_INDUSTRY + '）'
    return out


def corrected_document(doc):
    records = doc.get('records') or {}
    old = records.get('PR') or {}
    new = corrected_record(old)
    if new is old:
        return doc
    out = dict(doc, records=dict(records), industry_groups=dict(doc.get('industry_groups') or {}))
    out['records']['PR'] = new
    # Rebuild only the two affected peer groups, at their ORIGINAL sessions.
    # A stale historical ranking stays stale. No cap/date is synthesized.
    groups = out['industry_groups']
    for gid in {old.get('industry_group_id'), new['industry_group_id']} - {None}:
        template = groups.get(gid)
        if not template:
            continue
        group = deepcopy(template)
        fields = ('market', 'industry', 'industry_taxonomy', 'currency')
        codes = sorted(c for c, r in out['records'].items()
                       if all(r.get(k) == group.get(k) for k in fields))
        eligible = []
        for code in codes:
            rec = dict(out['records'][code]); original = records[code]
            rec['industry_group_id'] = gid; rec['industry_rank'] = None
            out['records'][code] = rec
            cap = rec.get('market_cap'); rank = original.get('industry_rank') or {}
            try:
                at = datetime.fromisoformat(rec['cap_asof'])
                valid_date = at.tzinfo is not None and at.astimezone(ZoneInfo('America/New_York')).date().isoformat() == group['session']
            except (KeyError, TypeError, ValueError):
                valid_date = False
            if (valid_date and rank.get('session') == group['session']
                    and rank.get('currency') == group['currency']
                    and type(cap) in (int, float) and math.isfinite(cap) and cap > 0):
                eligible.append((code, cap))
        eligible.sort(key=lambda item: (-item[1], item[0]))
        group.update(classified_codes=codes, classified=len(codes), comparable=len(eligible),
                     ranked_codes=[c for c, _ in eligible], top10=[], correction_revision=REVISION)
        previous = None; rank_number = 0
        for pos, (code, cap) in enumerate(eligible, 1):
            if cap != previous: rank_number = pos
            previous = cap
            rank = dict(records[code]['industry_rank'])
            rank.update(group_id=gid, rank=rank_number, comparable=len(eligible), classified=len(codes))
            out['records'][code]['industry_rank'] = rank
            if pos <= 10: group['top10'].append(dict(code=code, rank=rank_number, market_cap=cap))
        groups[gid] = group
    out['industry_correction_revision'] = REVISION
    return out
