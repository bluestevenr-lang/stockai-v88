"""Pure news evidence projection: publication clocks never inherit cache clocks.

This module reads no files and certifies neither news truth nor model reviews.
The original document and rejected historical rows remain untouched.
"""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import escape
import math

BJT=timezone(timedelta(hours=8))


def _published(value):
    if not isinstance(value,str) or not value.strip():return None
    try:
        stamp=datetime.fromisoformat(value.strip().replace('Z','+00:00'))
    except ValueError:
        try:stamp=parsedate_to_datetime(value.strip())
        except (TypeError,ValueError,OverflowError):return None
    if stamp.tzinfo is None or stamp.utcoffset() is None:return None
    return stamp


def _date_label(row):
    stamp=_published(row.get('published_time'))
    return (f'原发布时间 {stamp.astimezone(BJT):%Y-%m-%d %H:%M:%S} 北京时间'
            if stamp is not None else '原发布时间待核')


def _review_method(row,doc=None):
    method=str(row.get('classification_method') or '').lower()
    disabled=((doc or {}).get('stats') or {}).get('llm_disabled') is True
    if disabled or method.startswith(('deterministic','rule')):
        return '规则分类 · 未GPT审核'
    # Free-form method/model names and confidence are not verified receipts.
    return '原新闻线索 · GPT审核未核实'


def current_news(doc, now=None, max_age_hours=72):
    """Copy news whose original, zoned publication time is within the window.

    Preserves source order and all original fields, including published_time.
    Adds date_label and review_method; generated_at/fetched_at are never clocks.
    Invalid or naive reference clocks fail closed instead of using local time.
    """
    if not isinstance(doc,dict):return []
    now=now or datetime.now(timezone.utc)
    if not isinstance(now,datetime) or now.tzinfo is None or now.utcoffset() is None:return []
    if type(max_age_hours) not in (int,float) or not math.isfinite(max_age_hours) or max_age_hours<0:return []
    try:window=timedelta(hours=max_age_hours)
    except OverflowError:return []
    rows=doc.get('news')
    if not isinstance(rows,list):return []
    result=[]
    for row in rows:
        if not isinstance(row,dict):continue
        stamp=_published(row.get('published_time'))
        if stamp is None or not timedelta(0)<=now-stamp<=window:continue
        result.append({**deepcopy(row),'date_label':_date_label(row),
                       'review_method':_review_method(row,doc)})
    return result


def news_note(row):
    """Plain-text provenance note; no price direction or execution authority."""
    row=row if isinstance(row,dict) else {}
    method=('规则分类 · 未GPT审核' if row.get('review_method')=='规则分类 · 未GPT审核'
            else _review_method(row))
    return _date_label(row)+'；'+method+'；仅新闻线索，不代表独立验证或中央评级/交易许可。'


def news_note_html(row):
    """Escaped note for existing unsafe_allow_html render boundaries."""
    return escape(news_note(row),quote=True)
