from datetime import datetime
from recommendation_journal_ui import html,pages,week_rows


def row(code='688002.SS',market='A股'):
    return {'code':code,'market':market,'name':'睿创微纳','kind':'formal','horizon':'short',
        'first_at':'2026-09-15T10:00:00+08:00','first':{'tier':'1A','audit_score':67,'reason':'<script>bad</script>'},
        'days':{'2026-09-15':[{'at':'2026-09-15T10:00:00+08:00','state':'listed','tier':'1A','score':67,'rank':1,'reason':'条件研究'}],
        '2026-09-17':[{'at':'2026-09-17T10:00:00+08:00','state':'check','label':'待复审','reason':'缺证'}]},
        'current':{'label':'待复审','reason':'缺证'}}


def test_weekly_table_retains_old_score_pending_today_and_missing_dates():
    page=html([row()],'2026-09-14',now=datetime.fromisoformat('2026-09-17T10:01:00+08:00'))
    assert '67分' in page and '待复审' in page and '— 未留记录' in page and '待到日期' in page
    assert '?q=688002.SS' in page and '<script>' not in page
    assert page.count('<th>')==10


def test_pagination_preserves_every_row_with_max_five_per_market():
    rows=[row(str(i)+m,m) for m in ('A股','港股','美股') for i in range(18)]
    grouped=pages(rows)
    assert len(grouped)==4 and sum(map(len,grouped))==54
    assert all(len(g)<=15 and all(sum(r['market']==m for r in g)<=5 for m in ('A股','港股','美股')) for g in grouped)
    assert len({r['code'] for g in grouped for r in g})==54


def test_week_and_stock_filter_never_remove_stored_records():
    doc={'rows':[row(),{**row('OTHER'), 'days':{'2026-09-07':[]}}]}
    assert len(week_rows(doc,'2026-09-14','688002'))==1
    assert len(doc['rows'])==2


def test_calendar_has_real_month_days_hover_summaries_and_clickable_details():
    from recommendation_journal_ui import calendar_html
    page=calendar_html({'rows':[row()]},'2026-09','2026-09-17',now=datetime.fromisoformat('2026-09-17T10:01:00+08:00'))
    assert page.count('<th>')==7 and page.count('<td class=')==35
    assert '查看2026-09-15推荐' in page and 'day=2026-09-15' in page
    assert 'journal_stock=688002.SS' in page and 'title="睿创微纳' in page
    assert '条件研究' in page and '67分' in page
    assert '⏳ 1条跟进变化' in page and '尚未到期' in page


def test_day_detail_keeps_old_score_and_all_events_without_new_grade():
    from recommendation_journal_ui import day_rows,detail_html
    doc={'rows':[row()]}
    actual=day_rows(doc,'2026-09-15')
    assert actual[0]['appearance']['score']==67
    follow=day_rows(doc,'2026-09-17')
    assert follow[0]['appearance'] is None
    page=detail_html(follow,'2026-09-17')
    assert '后续跟进' in page and '缺证' in page and '67分' not in page
    assert day_rows(doc,'2026-09-15','没有的股')==[]


def test_calendar_escapes_tooltips_and_never_moves_a_record_to_another_day():
    from recommendation_journal_ui import calendar_html
    r=row();r['name']='<script>alert(1)</script>';r['days']['2026-09-15'][0]['reason']='\" onclick=\"bad'
    page=calendar_html({'rows':[r]},'2026-09','2026-09-17',now=datetime.fromisoformat('2026-09-17T10:01:00+08:00'))
    assert '<script>' not in page and '&quot;' in page
    assert page.count('class="event "')==1
