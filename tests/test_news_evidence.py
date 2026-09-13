from copy import deepcopy
from datetime import datetime,timedelta,timezone
import pytest

from news_evidence import current_news,news_note,news_note_html

NOW=datetime(2026,9,13,12,tzinfo=timezone(timedelta(hours=8)))


def row(at,**extra):
    return {'title':'原新闻','published_time':at,'classification_method':'deterministic-public-fact-pass-through',**extra}


def test_publication_clock_survives_recent_fetch_and_document_generation():
    doc={'generated_at':NOW.isoformat(),'stats':{'generated_at':NOW.isoformat()},'news':[
        row((NOW-timedelta(hours=73)).isoformat(),fetched_at=NOW.isoformat()),
        row((NOW-timedelta(hours=1)).isoformat(),fetched_at='1999-01-01')]}
    original=deepcopy(doc)
    out=current_news(doc,now=NOW)
    assert len(out)==1 and out[0]['published_time']==doc['news'][1]['published_time']
    assert '2026-09-13 11:00:00 北京时间' in out[0]['date_label']
    assert doc==original
    out[0]['title']='modified'
    assert doc==original


@pytest.mark.parametrize('stamp',[None,'','bad','2026-09-13','2026-09-13T10:00:00',
                                  (NOW+timedelta(seconds=1)).isoformat()])
def test_missing_naive_future_or_invalid_published_time_is_excluded(stamp):
    doc={'news':[row(stamp,fetched_at=NOW.isoformat())],'generated_at':NOW.isoformat()}
    assert current_news(doc,now=NOW)==[]


def test_window_boundary_uses_exact_instants_with_offsets_and_rfc_dates():
    oldest=NOW-timedelta(hours=72)
    docs={'news':[row(oldest.astimezone(timezone.utc).isoformat().replace('+00:00','Z')),
                  row((oldest-timedelta(microseconds=1)).isoformat()),
                  row('Sun, 13 Sep 2026 04:00:00 GMT')]}
    assert len(current_news(docs,now=NOW))==2
    assert len(current_news(docs,now=NOW,max_age_hours=1))==1


def test_rule_confidence_is_not_gpt_review_and_claimed_model_name_is_not_receipt():
    out=current_news({'news':[row(NOW.isoformat(),confidence=1.0,analysis_summary='分类结论'),
        row(NOW.isoformat(),classification_method='gpt-6-astra',reviewed=True)]},now=NOW)
    assert out[0]['review_method']=='规则分类 · 未GPT审核'
    assert out[1]['review_method']=='原新闻线索 · GPT审核未核实'
    assert '不代表独立验证或中央评级/交易许可' in news_note(out[0])


def test_doc_model_disabled_never_becomes_reviewed_news():
    out=current_news({'stats':{'llm_disabled':True,'analyzed_count':80},
                      'news':[row(NOW.isoformat(),classification_method=None)]},now=NOW)
    assert '规则分类 · 未GPT审核' in news_note(out[0])


@pytest.mark.parametrize('window',[-1,True,float('nan'),float('inf'),'72'])
def test_bad_window_fails_closed(window):
    assert current_news({'news':[row(NOW.isoformat())]},now=NOW,max_age_hours=window)==[]


def test_naive_now_missing_list_and_note_cannot_inherit_forged_date_or_review():
    assert current_news({'news':[row(NOW.isoformat())]},now=NOW.replace(tzinfo=None))==[]
    assert current_news({'news':None},now=NOW)==[]
    unsafe={'published_time':'<script>bad</script>','date_label':'今天已核实','review_method':'GPT已审核'}
    text=news_note_html(unsafe)
    assert '原发布时间待核' in text and 'GPT审核未核实' in text and '<script>' not in text
    assert '今天已核实' not in text
