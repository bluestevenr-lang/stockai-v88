"""Fail-closed display contract for local and public GPT-6 review snapshots."""
from datetime import datetime, timedelta, timezone

MODEL = 'gpt-6-astra'
VERSION = 'gpt-classics-selection-v9-tharp'
from review_scorecard import card_passed, gpt_result, book_result, scorecard
from review_contract import SCHEMA_VERSION, PROMPT_HASH, known_protocol
BJT = timezone(timedelta(hours=8))
from evidence_freshness import source_times_fresh

def fresh(value, hours=24, now=None):
    raw = str(value or '').replace('（北京时间）', '').strip()
    if len(raw) < 16:
        return False
    try:
        at = datetime.fromisoformat(raw.replace('Z', '+00:00'))
        at = at if at.tzinfo else at.replace(tzinfo=BJT)
        age = (now or datetime.now(BJT)) - at
        return timedelta(0) <= age <= timedelta(hours=hours)
    except ValueError:
        return False

def current_scorecard(snapshot, row, now=None):
    now = (now or datetime.now(BJT)).astimezone(BJT)
    pack = snapshot.get('factpack_id')
    reviews = row.get('reviews') or {}
    g, b = reviews.get('gpt') or {}, reviews.get('classics') or {}
    hours = 72 if now.weekday() >= 5 else 30
    shared = bool(pack and snapshot.get('version') == VERSION
                  and fresh(snapshot.get('generated_at'), hours, now)
                  and row.get('factpack_id') == pack)
    g_current = bool(not g.get('review_suspended') and shared and g.get('factpack_id') == pack and g.get('model') == MODEL
                     and g.get('review_scope') == 'buy' and g.get('horizon') == row.get('horizon')
                     and known_protocol(g,row)
                     and fresh(g.get('at'), 24, now))
    b_current = bool(shared and b.get('factpack_id') == pack and b.get('horizon') == row.get('horizon')
                     and fresh(b.get('at'), hours, now) and b.get('source_timestamps')
                     and source_times_fresh(row, now, sources=b['source_timestamps'])
                     and source_times_fresh(row, now))
    card = scorecard(g, b, gpt_current=g_current, book_current=b_current)
    card['audit_id'] = row.get('audit_id') or (row.get('scorecard') or {}).get('audit_id') or card['audit_id']
    return card

def actionable(snapshot, row, now=None):
    from investment_maturity import assess
    now = (now or datetime.now(BJT)).astimezone(BJT)
    pack = snapshot.get('factpack_id')
    reviews = row.get('reviews') or {}
    gpt, book = reviews.get('gpt') or {}, reviews.get('classics') or {}
    sources = row.get('source_timestamps') or {}
    book_sources = book.get('source_timestamps') or {}
    hours = 72 if now.weekday() >= 5 else 30
    return bool(assess(current_scorecard(snapshot, row, now), row.get('horizon'), row.get('trade_plan'))['tier'] == '3A' and card_passed(current_scorecard(snapshot, row, now)) and card_passed(row.get('scorecard') or {}) and gpt_result(gpt)['passed']
        and book_result(book)['passed']
        and gpt_result(gpt)['total'] == (row.get('scorecard') or {}).get('total')
        and snapshot.get('version') == VERSION and pack
        and snapshot.get('factpack_fresh') is True and fresh(snapshot.get('generated_at'), hours, now)
        and row.get('factpack_id') == gpt.get('factpack_id') == book.get('factpack_id') == pack
        and row.get('tier') == '3A' and row.get('state') == '3A_PUBLISHABLE'
        and row.get('publish_eligible') is True and row.get('formal_recommendation') is True
        and row.get('support_count') == 2 and row.get('data_fresh') is True
        and gpt.get('model') == MODEL and gpt.get('horizon') == row.get('horizon')
        and gpt.get('review_scope') == 'buy' and book.get('horizon') == row.get('horizon')
        and (gpt.get('thesis_verdict') or gpt.get('verdict')) == '通过'
        and gpt.get('execution_status') == '现在买' and gpt.get('risk_veto') == '无'
        and fresh(gpt.get('at'), 24, now) and fresh(book.get('at'), hours, now)
        and all((row.get('votes') or {}).get(key) == 'pass' for key in ('gpt', 'classics'))
        and source_times_fresh(row, now, sources=sources)
        and source_times_fresh(row, now, sources=book_sources)
        and (reviews.get('claude_cs') or {}).get('verdict') in {'gate_pass', 'pass', '通过'}
        and (row.get('trade_plan') or {}).get('ready') is True)
