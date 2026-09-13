from copy import deepcopy
from datetime import timedelta
from html.parser import HTMLParser

import pytest

from persistent_watchlist_ui import html
from test_weekly_candidates_ui import linked_fixture


@pytest.fixture(autouse=True)
def isolate_profiles(monkeypatch):
    import stock_profile_view
    monkeypatch.setattr(stock_profile_view,'load',lambda *args,**kwargs:{})


def fixture():
    import grade_focus
    weekly,selection,doc,now=linked_fixture()
    focus=grade_focus.build(selection['observations'],now=now)
    seat=doc['current_focus']['rows'][0]
    seat.update(market='美股',horizon='short',reason='原审核与合同继续跟踪',
                focus_rank=1,central_rank=3,focus_eligible=True,weekly_link=True)
    reserve=deepcopy(selection['observations'][0]);rec=focus['records'][reserve['code']]
    reserve.update(market='美股',watch_rank=1,seat_kind='已评级跟踪',source_asof=reserve['factpack_asof'],
        focus_rank=None,central_rank=1,focus_eligible=False,weekly_link=False,
        entry_opportunity=rec['entry_opportunity'],reason='原区间与趋势参考无交集，保留研究')
    doc['current_focus'].update(rows=[seat,reserve],graded=2,awaiting_review=0,
        rule='条件关注优先；其余席位保留研究跟踪，全部沿用原合同')
    doc['rows']=deepcopy(doc['current_focus']['rows'])
    return selection,doc,now


def test_main_list_uses_grade_ranks_instead_of_entry_ranks_with_original_scores():
    selection,doc,now=fixture()
    out=html(doc,selection,view='current')
    assert out.count('<th ')==11 and out.count('class="v88-watch-row"')==2
    assert '美股 1A 第1名' in out and '美股 1A 第3名' in out
    assert '美股 1A 第2名' not in out
    assert out.index('data-code="AA"') < out.index('data-code="TEST"')
    assert '进场关注序' not in out and '审核质量序' not in out
    assert '港股：1A 0/3–5 · 2A 0/1–2 · 3A 0/1–2 · 1A缺口3' in out
    assert '📌 本周主观察 · 与上方周度同股同分' in out
    assert 'id="v88-watch-TEST"' in out and '研究Top' not in out
    assert out.count('审核分 75/100')==2 and '原审核分' not in out
    assert '原区间' in out and '?q=TEST' in out


def test_stale_or_changed_ranks_cannot_claim_current_attention_or_weekly_link():
    selection,doc,now=fixture()
    for change in ('generation','stale','quality_rank','focus_rank','focus_eligible','score','contract'):
        bad=deepcopy(doc);seat=bad['current_focus']['rows'][0]
        if change=='generation':bad['source_generated_at']='old'
        elif change=='stale':bad['generated_at']=(now-timedelta(minutes=16)).isoformat()
        elif change=='quality_rank':seat['central_rank']=1
        elif change=='focus_rank':seat['focus_rank']=2
        elif change=='focus_eligible':seat['focus_eligible']=False
        elif change=='score':seat['audit_score']=99
        else:seat['trade_plan']['stop']=1
        out=html(bad,selection,code='TEST',view='current')
        assert '条件关注 1' not in out,change
        assert '与上方周度同股同分' not in out,change
        assert '当前证据待更新·不可执行' in out,change
        assert '原1A·复核中' in out and '?q=TEST' in out,change


def test_original_period_keeps_research_identity_without_current_ranking():
    selection,doc,now=fixture()
    out=html(doc,selection,view='history')
    assert '本期固定跟踪档案' in out and '美股 跟踪 3' in out
    assert '研究Top' not in out and 'id="v88-watch-' not in out
    assert out.count('class="v88-watch-history-row"')==2
    assert '?q=TEST' in out and '?q=AA' in out


def paused_hk_fixture(*,bucket='pending',expired=False):
    from test_grade_card_safety import central_row
    from review_display import current_scorecard
    selection,doc,now=fixture()
    selection[bucket]=[]
    doc['tracking']={'rows':[]}
    for rank,code in enumerate(('661.HK','3330.HK','9926.HK','1686.HK','2696.HK'),1):
        central=central_row(code,'港股'+str(rank),'1A','RESEARCH_1A')
        central.update(tier='PENDING' if bucket=='pending' else '0A',
                       factpack_asof='2026-09-11T16:00:00+08:00')
        gpt=central['reviews']['gpt']
        for review in [gpt,*gpt['review_pair'].values()]:
            review['verdict']=review['thesis_verdict']='不否定'
            next(c for c in review['criteria'] if c['id']=='thesis')['score']=10
            if expired:review['at']=(now-timedelta(days=2)).isoformat()
        central['scorecard']={}
        card=current_scorecard(selection,central)
        complete=bool(card['gpt']['current'] and card['gpt']['complete'])
        selection[bucket].append(central)
        doc['tracking']['rows'].append({
            'code':code,'name':central['name'],'market':'港股','horizon':central['horizon'],
            'watch_rank':rank,'tier':None,'audit_score':card['total'] if complete else None,
            'scorecard':card,'review_complete':complete,'previous_tier':'1A',
            'seat_kind':'暂停研究·双审未达标' if complete else '暂停研究·证据待更新',
            'continuity':True,'focus_eligible':False,'executable':False,
            'entry_opportunity':{'state':'PAUSED','executable':False,'focus_eligible':False},
            'trade_plan':deepcopy(central['trade_plan']),'source_asof':central['factpack_asof'],
            'reason':'逻辑成立尚未过审 <新证据需复核>','gaps':['补充因果证据'],
        })
    # Counts must be derived from the visible, rechecked rows; copied totals
    # alone cannot turn a continuity seat into an approved recommendation.
    doc['current_focus'].update(graded=99,awaiting_review=5,paused=0)
    return selection,doc,now


@pytest.mark.parametrize('bucket',['pending','excluded'])
def test_hk_paused_seats_stay_in_separate_tracking_with_real_scores_and_grade_gap(bucket):
    selection,doc,now=paused_hk_fixture(bucket=bucket)
    formal=html(doc,selection,view='current')
    assert '港股：1A 0/3–5 · 2A 0/1–2 · 3A 0/1–2 · 1A缺口3' in formal
    assert '?q=661.HK' not in formal
    out=html(doc,selection,view='tracking')
    assert out.count('<th ')==11 and out.count('class="v88-watch-tracking-row"')==5
    assert out.count('data-market="港股" data-tier="none" data-paused="true" data-continuity="true" data-current="true"')==5
    assert '持续跟踪与补审（不计入正式评级） · 5只' in out
    assert '港股 5只（暂停5 / 待双审0）' in out
    assert '美股 0只（暂停0 / 待双审0）' in out and 'A股 0只（暂停0 / 待双审0）' in out
    assert '已评级99' not in out and '待双审5' not in out
    assert out.count('⏸ 持续跟踪·暂停')==5 and out.count('审核分 70/100')==5
    assert out.count('<summary>暂停原因与原评级</summary>原1A → 暂停研究')==5
    assert '&lt;新证据需复核&gt;' in out and '<新证据需复核>' not in out
    assert 'id="v88-tracking-661.HK"' in out and '?q=661.HK' in out
    assert '港股 条件关注' not in out and '港股 1A价值跟踪' not in out


@pytest.mark.parametrize('change',['score','contract','card','horizon','source','complete','row_execution',
    'row_focus','entry_execution','entry_focus','central_changed','central_missing','central_duplicate','expired'])
def test_continuity_does_not_trust_matching_document_hash_when_central_evidence_changed(change):
    selection,doc,now=paused_hk_fixture()
    seat=doc['tracking']['rows'][0]
    central=selection['pending'][0]
    if change=='score':seat['audit_score']=99
    elif change=='contract':seat['trade_plan']['stop']=1
    elif change=='card':seat['scorecard']['gpt']['criteria'][0]['reason']='tampered'
    elif change=='horizon':seat['horizon']='long'
    elif change=='source':seat['source_asof']='old'
    elif change=='complete':seat['review_complete']=False
    elif change=='row_execution':seat['executable']=True
    elif change=='row_focus':seat['focus_eligible']=True
    elif change=='entry_execution':seat['entry_opportunity']['executable']=True
    elif change=='entry_focus':seat['entry_opportunity']['focus_eligible']=True
    elif change=='central_changed':central['trade_plan']['stop']=2
    elif change=='central_missing':selection['pending']=selection['pending'][1:]
    elif change=='central_duplicate':selection['excluded']=[deepcopy(central)]
    else:central['reviews']['gpt']['at']=(now-timedelta(days=2)).isoformat()
    out=html(doc,selection,code='661.HK',view='tracking')
    assert 'data-current="false"' in out
    assert '当前证据待更新·不可执行' in out
    assert '⏸ 持续跟踪·证据待更新' in out
    assert '原审核分' in out and '当前双审证据待更新；未授级' in out
    assert '港股 1只（暂停1 / 待双审0）' in out
    assert '港股 条件关注' not in out


def test_expired_review_retains_identity_without_inheriting_prior_score():
    selection,doc,now=paused_hk_fixture(expired=True)
    out=html(doc,selection,code='661.HK',view='tracking')
    assert 'data-current="true"' in out
    assert '审核分：证据待更新' in out and '审核分 70/100' not in out
    assert '原1A → 暂停研究' in out
    assert '当前双审证据待更新；未授级' in out and '?q=661.HK' in out


def test_lower_scored_entry_candidate_cannot_outrank_higher_scored_research():
    import grade_focus
    from review_display import current_scorecard
    selection,doc,now=fixture()
    central=selection['observations'][-1]
    gpt=central['reviews']['gpt']
    for review in [gpt,*gpt['review_pair'].values()]:
        review['criteria'][3]['score']=10
        review['verdict']=review['thesis_verdict']='不否定'
    central['scorecard']=current_scorecard(selection,central)
    central['audit_score']=central['scorecard']['total']
    records=grade_focus.build([{**r,'scorecard':current_scorecard(selection,r)} for r in selection['observations']])['records']
    for row in doc['current_focus']['rows']:
        record=records[row['code']]
        row['central_rank']=record['central_rank'];row['watch_rank']=record['central_rank'];row['focus_rank']=record['rank']
        if row['code']=='TEST':
            row['audit_score']=central['audit_score'];row['scorecard']=deepcopy(central['scorecard'])
    out=html(doc,selection,view='current')
    assert '审核分 70/100' in out and '审核分 75/100' in out
    assert out.index('data-code="AA"') < out.index('data-code="TEST"')
    assert '美股 1A 第1名' in out and '美股 1A 第3名' in out
    assert '条件关注 1' not in out


def test_formal_view_never_lists_unscored_candidates_or_paused_seats():
    selection,doc,now=paused_hk_fixture()
    doc['current_focus']['rows'].extend(deepcopy(doc['tracking']['rows']))
    for value in (None,float('nan'),float('inf'),True):
        row=deepcopy(doc['current_focus']['rows'][0]);row.update(code='UNSCORED',audit_score=value)
        doc['current_focus']['rows'].append(row)
    out=html(doc,selection,view='current')
    assert 'UNSCORED' not in out and '?q=661.HK' not in out
    assert '尚未完成双审' not in out and '暂停原因与原评级' not in out
    assert out.count('class="v88-watch-row"')==2


def test_empty_formal_view_keeps_all_market_grade_gaps_visible():
    selection,doc,now=fixture();doc['current_focus']['rows']=[]
    out=html(doc,selection,view='current')
    assert '三市场正式评级榜 · 0只' in out
    assert out.count('1A缺口3')==3
    assert all('data-grade="'+g+'"' in out for g in ('3A','2A','1A'))
    assert '尚未完成双审' not in out


def test_per_market_grade_limits_and_complete_numeric_scores():
    import re
    import grade_focus
    from test_grade_card_safety import central_row
    from review_display import current_scorecard
    selection,doc,now=fixture()
    central_rows=[]
    for market in ('A股','美股','港股'):
        for tier,count in (('3A',3),('2A',3),('1A',6)):
            for index in range(count):
                number=int(tier[0])*100+index
                code=(f'{600000+number}.SS' if market=='A股' else
                      f'{number}.HK' if market=='港股' else f'US{tier[0]}A{index}')
                row=central_row(code,code,tier,'RESEARCH_'+tier)
                row.update(market=market,factpack_asof='2026-09-11T16:00:00+08:00')
                central_rows.append(row)
    selection['observations']=central_rows
    records=grade_focus.build([{**r,'scorecard':current_scorecard(selection,r)} for r in central_rows])['records']
    doc['current_focus']['rows']=[]
    for row in reversed(central_rows):
        rec=records[grade_focus.canonical(row['code'])]
        doc['current_focus']['rows'].append({**deepcopy(row),
            'watch_rank':rec['central_rank'],'seat_kind':'已评级跟踪','source_asof':row['factpack_asof'],
            'focus_rank':rec['rank'],'central_rank':rec['central_rank'],
            'focus_eligible':rec['entry_opportunity']['focus_eligible'],
            'entry_opportunity':rec['entry_opportunity'],'reason':'当前双审证据'})
    out=html(doc,selection,view='current')
    assert out.count('class="v88-watch-row"')==27
    for market in ('A股','美股','港股'):
        for tier,limit in (('3A',2),('2A',2),('1A',5)):
            assert len(re.findall(f'data-market="{market}" data-tier="{tier}"',out))==limit
            assert re.search(f'{market} {tier} (?:原榜)?第{limit}名',out)
            assert not re.search(f'{market} {tier} (?:原榜)?第{limit+1}名',out)
    assert '尚未完成双审' not in out and 'nan/100' not in out


class WatchTableParser(HTMLParser):
    """Read the produced table rather than inferring columns from helper calls."""
    def __init__(self):
        super().__init__()
        self.rows=[];self.row=None;self.cell=None;self.history_titles=[]

    def handle_starttag(self,tag,attrs):
        attrs=dict(attrs)
        if tag=='tr' and 'data-code' in attrs:
            self.row={'attrs':attrs,'cells':[]};self.rows.append(self.row)
        elif tag=='td' and self.row is not None:
            self.cell=[];self.row['cells'].append(self.cell)
        elif attrs.get('class')=='v88-listing-history':
            self.history_titles.append(attrs.get('title'))

    def handle_endtag(self,tag):
        if tag=='tr':self.row=None
        elif tag=='td':self.cell=None

    def handle_data(self,data):
        if self.cell is not None:self.cell.append(data)


def listing_history():
    return {'previous':{'listed_at':'2026-09-12T21:44:05-04:00','market':'美股',
                        'tier':'1A','rank':3,'publication_key':'publication-previous'},
            'status':'verified','rank_scope':'同市场同评级正式榜','timezone':'Asia/Shanghai'}


def history_view_fixture(view,history):
    selection,doc,now=fixture()
    doc['tracking']={'rows':deepcopy(doc['rows'])}
    source=(doc['current_focus']['rows'] if view=='current' else
            doc['tracking']['rows'] if view=='tracking' else doc['rows'])
    row=next(r for r in source if r['code']=='TEST')
    row['listing_history']=history
    return selection,doc,row


@pytest.mark.parametrize('view',['current','tracking','history'])
def test_previous_listing_in_rating_cell_preserves_eleven_columns_and_current_score(view):
    selection,doc,row=history_view_fixture(view,listing_history())
    row['name']='样例 <script>alert("bad")</script> & 公司'
    out=html(doc,selection,code='TEST',view=view)
    parsed=WatchTableParser();parsed.feed(out)
    assert len(parsed.rows)==1 and len(parsed.rows[0]['cells'])==11
    assert out.count('<th ')==11 and "class='v88-grade-scroll'" in out
    rating=''.join(parsed.rows[0]['cells'][1])
    assert '上次上榜 09-13 09:44' in rating and '美股1A 第3名' in rating
    assert '审核分 75/100' in rating
    assert ('美股 1A 第3名' if view=='current' else '美股 跟踪 3') in rating
    assert parsed.history_titles==['2026-09-13 09:44:05 北京时间（BJT） · 美股1A 第3名']
    assert 'class="v88-listing-history" style="font-size:11px!important;' in out
    assert '&lt;script&gt;alert(&quot;bad&quot;)&lt;/script&gt; &amp; 公司' in out
    assert '<script>' not in out and '?q=TEST&focus=deep#v88-deep-analysis' in out
    assert '上次上榜指上一版真实正式榜' in out
    assert '同版后台刷新不重复计次，历史名次按当时市场及评级' in out
    assert '首次上榜' not in out


def test_previous_rating_and_rank_are_not_replaced_by_current_grade_or_tracking_rank():
    history=listing_history();history['previous'].update(tier='2A',rank=2)
    selection,doc,row=history_view_fixture('tracking',history)
    row.update(watch_rank=91,previous_tier='3A')
    out=html(doc,selection,code='TEST',view='tracking')
    assert '美股2A 第2名' in out and '美股 跟踪 91' in out
    assert '美股1A 第2名' not in out and '美股3A 第91名' not in out


@pytest.mark.parametrize('view',['current','tracking','history'])
@pytest.mark.parametrize('history',[None,{},[], 'invalid',
    {'status':'no_verified_previous','previous':None}])
def test_missing_previous_listing_is_explicit_without_inventing_first_listing(view,history):
    selection,doc,row=history_view_fixture(view,history)
    out=html(doc,selection,code='TEST',view=view)
    parsed=WatchTableParser();parsed.feed(out)
    assert len(parsed.rows[0]['cells'])==11
    assert '上次上榜：暂无可核验历史' in ''.join(parsed.rows[0]['cells'][1])
    assert parsed.history_titles==[None]
    assert '首次上榜' not in out and '美股1A 第3名' not in out


@pytest.mark.parametrize('field,value',[
    ('listed_at','2026-09-13T09:44:05'),('listed_at','not-a-date'),
    ('listed_at','<img src=x onerror=alert(1)>'),('listed_at',42),
    ('rank',None),('rank',True),('rank',0),('rank',-1),('rank',3.0),('rank','3'),
    ('rank',float('nan')),('rank','<script>3</script>'),
    ('market','<img src=x onerror=alert(1)>'),('market',[]),
    ('tier','0A'),('tier','<script>1A</script>'),('tier',{}),
    ('publication_key',None),('publication_key',' '),('publication_key',[]),
])
def test_malformed_previous_receipt_does_not_render_a_historical_date_or_rank(field,value):
    history=listing_history();history['previous'][field]=value
    selection,doc,row=history_view_fixture('current',history)
    out=html(doc,selection,code='TEST',view='current')
    parsed=WatchTableParser();parsed.feed(out)
    assert '上次上榜：暂无可核验历史' in ''.join(parsed.rows[0]['cells'][1])
    assert '上次上榜 09-13' not in out and '美股1A 第3名' not in out
    assert '<script>' not in out and '<img src=x' not in out
    assert parsed.history_titles==[None]


@pytest.mark.parametrize('field,value',[
    ('previous',[]),('previous','invalid'),('previous',None),
    ('status','unverified'),('rank_scope','全市场'),('timezone','UTC'),
])
def test_unverified_or_wrong_scope_history_is_not_presented_as_verified(field,value):
    history=listing_history();history[field]=value
    selection,doc,row=history_view_fixture('current',history)
    out=html(doc,selection,code='TEST',view='current')
    assert '上次上榜：暂无可核验历史' in out
    assert '上次上榜 09-13' not in out and '美股1A 第3名' not in out


def test_publication_key_is_internal_and_not_an_html_attribute():
    history=listing_history()
    history['previous']['publication_key']='pub" onmouseover="alert(1)<script>&'
    selection,doc,row=history_view_fixture('current',history)
    out=html(doc,selection,code='TEST',view='current')
    assert '美股1A 第3名' in out and '上次上榜 09-13 09:44' in out
    assert 'onmouseover=' not in out and '<script>' not in out


def test_missing_competitor_does_not_promote_published_third_place_to_second():
    selection,doc,now=fixture()
    page=html(doc,selection,code='TEST',view='current')
    assert '美股 1A 第3名' in page and '美股 1A 第1名' not in page and '美股 1A 第2名' not in page
    assert 'data-current="true"' in page


@pytest.mark.parametrize('rank',[1,99,True])
def test_wrong_published_position_cannot_claim_current_rank_or_weekly_link(rank):
    selection,doc,now=fixture()
    doc['current_focus']['rows'][0]['watch_rank']=rank
    page=html(doc,selection,code='TEST',view='current')
    assert 'data-current="false"' in page and '当前证据待更新·不可执行' in page
    assert '同股同分' not in page


@pytest.mark.parametrize('score',[-1,101])
def test_out_of_range_review_scores_do_not_enter_formal_table(score):
    selection,doc,now=fixture()
    doc['current_focus']['rows'][0]['audit_score']=score
    page=html(doc,selection,view='current')
    assert 'data-code="TEST"' not in page


def test_current_grade_action_uses_recomputed_entry_not_unverified_cached_label():
    selection,doc,now=fixture()
    row=doc['current_focus']['rows'][0]
    row['entry_opportunity']={'label':'FAKE_EXECUTION_SENTINEL','state':'EXECUTABLE','executable':True}
    page=html(doc,selection,code='TEST',view='current')
    assert 'data-current="true"' in page
    assert 'FAKE_EXECUTION_SENTINEL' not in page and '条件观察' in page


def test_current_grade_displays_recomputed_review_and_books_not_cached_detail():
    selection,doc,now=fixture()
    row=doc['current_focus']['rows'][0]
    row['scorecard']={'gpt':{'total':100,'current':True,'criteria':[{'reason':'FAKE_REVIEW_SENTINEL'}]}}
    row['book_checks']=[{'label':'FAKE_BOOK_SENTINEL','ok':True}]
    page=html(doc,selection,code='TEST',view='current')
    assert 'data-current="true"' in page
    assert 'FAKE_REVIEW_SENTINEL' not in page and 'FAKE_BOOK_SENTINEL' not in page


def test_stale_review_preserves_original_evidence_with_historical_label():
    selection,doc,now=fixture();doc['source_generated_at']='prior-package'
    page=html(doc,selection,code='TEST',view='current')
    assert '原GPT审核快照 · 待重核' in page and '原书理快照 · 待重核' in page
    assert '独立反审' in page and '原榜第3名' in page
