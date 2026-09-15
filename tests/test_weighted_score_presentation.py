from copy import deepcopy
from datetime import datetime, timezone
import json

from review_scorecard import CRITERIA, VERSION, BOOK_IDS, scorecard, choose_conservative
from scorecard_html import gpt_html, books_html, score_label, rubric_html
from observation_score import summary
from deep_analysis_data import observation_html


def reviewed():
    review = {'horizon':'short', 'thesis_verdict':'通过',
              'evidence':[{'field':'last','value':10}, {'field':'volume','value':120}],
              'criteria':[{'id':k,'score':20 if k=='risk' else 15,
                           'reason':'已核证据 <script>不得执行</script>',
                           'evidence_fields':['last','volume']} for k in CRITERIA]}
    books = {'horizon':'short','rubric_version':VERSION,
             'checks':[{'id':k,'ok':True,'label':k,'detail':'已核',
                        'book':'独立原则','threshold':'可核条件','evidence':{'last':10}}
                       for k in BOOK_IDS['short']]}
    card = scorecard(choose_conservative(deepcopy(review),deepcopy(review)), books,
                    gpt_current=True,book_current=True)
    return review, books, card


def test_ui_shows_the_bound_weighted_contribution_and_escapes_evidence():
    review, books, card = reviewed()
    before = deepcopy(card)
    body = gpt_html(card) + books_html(card)
    assert '加权 81.25/100' in body
    assert '权重 25% · 贡献 25分' in body
    assert '权重 10% · 贡献 7.5分' in body
    assert '书理 100分' in body
    assert '<script>' not in body and '&lt;script&gt;' in body
    assert score_label(card) == '加权审核分 81.25/100'
    assert card == before
    assert '书籍通过率分' not in rubric_html()


def test_partial_score_never_displays_as_a_complete_score():
    _, _, card = reviewed()
    card.update(total=None, known_contribution=45, coverage_pct=75)
    shown = score_label(card)
    assert '已核贡献 45分' in shown and '覆盖75%' in shown
    assert '/100' not in shown
    legacy = {'total':80,'gpt':{'total':80,'current':False},'books':{}}
    assert '原审核分' in score_label(legacy)
    assert 'GPT‑6 加权' not in gpt_html(legacy)


def test_deep_reads_one_current_observation_with_the_shared_score(tmp_path):
    now = datetime(2026,9,15,13,30,tzinfo=timezone.utc)
    row = {'code':'TEST','market':'美股','name':'Example','last':10,'structural_verified':True,
           'structure':{'ma20':9.8,'ma60':9,'previous_high20':10.1,'low5':9.6,
                        'return5_pct':2,'volume_ratio20':1.3,'change_pct':1,
                        'source_asof':'2026-09-14','raw_sha256':'a'*64},
           'nontechnical':{'code':'TEST','status':'pending','evidence':[]}}
    other = {**row,'code':'OTHER','name':'Must not show'}
    doc = {'generated_at':now.isoformat(),'rows':[other,row]}
    (tmp_path/'market_watch_pub.json').write_text(json.dumps(doc))
    html = observation_html('TEST',tmp_path,now=now)
    assert summary(row,now=now) in html
    assert '市场视角' in html and 'Must not show' not in html
    assert observation_html('MISSING',tmp_path,now=now) == ''
    doc['generated_at'] = '2026-09-12T10:00:00+00:00'
    (tmp_path/'market_watch_pub.json').write_text(json.dumps(doc))
    assert observation_html('TEST',tmp_path,now=now) == ''
