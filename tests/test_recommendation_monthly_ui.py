from copy import deepcopy
from datetime import datetime

from recommendation_journal_ui import (calendar_html,day_rows,detail_html,html,
    pages,week_rows,with_astra)

NOW=datetime.fromisoformat('2026-10-16T12:00:00+08:00')


def row(code='700.HK',name='腾讯',kind='formal',days=('2026-10-01','2026-10-03','2026-10-09')):
    return {'code':code,'market':'港股','name':name,'kind':kind,'horizon':'short',
        'first_at':days[0]+'T10:00:00+08:00','first':{'tier':'1A','audit_score':80,'reason':'original'},
        'days':{day:[{'at':day+'T10:00:00+08:00','state':'listed','tier':'1A','score':80,'rank':1,'reason':'原依据'}] for day in days},
        'latest':{'observation_price':400,'source_asof':'2026-10-15T16:08:00+08:00'},
        'current':{'label':'跟踪','reason':'继续核验'}}


def test_month_cells_show_historical_ordinals_without_rewriting_records():
    doc={'rows':[row()]};before=deepcopy(doc)
    page=calendar_html(doc,'2026-10','2026-10-09',now=NOW)
    for n in (1,2,3):assert f'腾讯*{n}' in page and f'本月第{n}个推荐日' in page
    assert '400 HKD' in page and '2026-10-15T16:08:00+08:00' in page
    assert '非实时' in page
    assert doc==before


def test_full_month_cross_module_count_precedes_search_and_daily_pagination():
    early=row('00700.HK','旧简称','strategy_3a',('2026-10-01','2026-10-03'))
    target=row('700.HK','腾讯','formal',('2026-10-09',))
    other=[row(str(i)+'.HK','其他'+str(i),days=('2026-10-09',)) for i in range(10,21)]
    doc={'rows':[early,target,*other]}
    actual=day_rows(doc,'2026-10-09','腾讯')
    assert len(actual)==1 and actual[0]['monthly_recommendation_count']==3
    assert '腾讯*3' in detail_html(actual,'2026-10-09')
    all_pages=pages(day_rows(doc,'2026-10-09'))
    chosen=next(r for page in all_pages for r in page if r['name']=='腾讯')
    assert chosen['monthly_recommendation_count']==3
    assert sum(len(page) for page in all_pages)==12


def test_astra_and_formal_same_day_share_one_count_and_next_month_resets():
    doc={'rows':[row(days=('2026-09-30','2026-10-01','2026-10-03'))]}
    astra={'calendar_records':[{'cycle_id':'2026-10-01','at':'2026-10-03T02:00:00Z',
      'row':{'code':'00700.HK','market':'港股','name':'腾讯','strategy_score':{'value':80}},
      'seen_dates':['2026-10-04'],'outcome':{'date':'2026-10-05','label':'目标触及'}}]}
    union=with_astra(doc,astra)
    rows=day_rows(union,'2026-10-03')
    assert len(rows)==2 and all(r['monthly_recommendation_count']==2 for r in rows)
    assert '腾讯*2' in detail_html(rows,'2026-10-03')
    assert day_rows(union,'2026-10-04')==[]
    assert day_rows(union,'2026-10-01')[0]['monthly_recommendation_count']==1


def test_followup_keeps_previous_count_but_does_not_claim_new_recommendation():
    r=row();r['days']['2026-10-10']=[{'at':'2026-10-10T12:00:00+08:00','state':'check','label':'复核','reason':'价格跟踪'}]
    doc={'rows':[r]};selected=day_rows(doc,'2026-10-10')
    assert selected[0]['monthly_recommendation_count']==3 and selected[0]['appearance'] is None
    rendered=detail_html(selected,'2026-10-10')
    assert '腾讯*3' in rendered and '本条仅跟踪，不新增推荐' in rendered
    assert '本月第4个推荐日' not in rendered


def test_weekly_renderer_uses_full_month_and_each_days_own_ordinal():
    doc={'rows':[row()]}
    selected=week_rows(doc,'2026-10-05','腾讯')
    rendered=html(selected,'2026-10-05',now=NOW)
    assert '腾讯*3' in rendered and '本月第3个推荐日' in rendered
    earlier=html(week_rows(doc,'2026-09-28'),'2026-09-28',now=NOW)
    assert '腾讯*1' in earlier and '腾讯*2' in earlier
    assert '腾讯*3' not in earlier


def test_badge_name_and_tooltip_are_html_escaped():
    r=row(name='<bad "name">')
    rendered=calendar_html({'rows':[r]},'2026-10','2026-10-09',now=NOW)
    assert '<bad' not in rendered and '&lt;bad &quot;name&quot;&gt;*3' in rendered


def test_astra_union_never_hides_existing_history_when_source_is_partial():
    doc={'rows':[row(days=('2026-10-01',))]}
    record={'cycle_id':'2026-10-01','at':'2026-10-03T02:00:00Z',
            'row':{'code':'700.HK','market':'港股','name':'腾讯','strategy_score':{'value':80}}}
    once=with_astra(doc,{'calendar_records':[record]})
    twice=with_astra(once,{'calendar_records':[record]})
    missing=with_astra(once,{'calendar_records':[]})
    assert once==twice==missing
    assert day_rows(missing,'2026-10-03')[0]['monthly_recommendation_count']==2
