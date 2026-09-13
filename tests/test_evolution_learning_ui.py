from datetime import datetime,timezone,timedelta
import json
from pathlib import Path
from bs4 import BeautifulSoup
import evolution_learning_ui as ui
import persistent_watchlist_ui as watch


def test_history_keeps_downgrade_and_displays_non_grade_focus_exit():
    stock={'code':'TEST','name':'<script>x</script>','last_value_tier':'3A','events':[
        {'kind':'registry_event','event_at':'2026-09-01T10:00:00+08:00','captured_at':'2026-09-12T10:00:00+08:00',
         'event':{'kind':'DEMOTED','from':'3A','to':'2A','score':75,'reason':'逻辑变化'}},
        {'kind':'monitor_state','event_at':'2026-09-12T11:00:00+08:00','captured_at':'2026-09-12T11:00:00+08:00',
         'changes':['FOCUS_LEFT'],'previous':{'tier':'2A'},'state':{'tier':'2A','score':75}}]}
    html=ui.stock_html(stock)
    assert '<script>' not in html and '&lt;script&gt;' in html
    assert '降级' in html and '退出重点榜·保留档案' in html
    assert '3A → 2A' in html and '2A → 2A' in html
    assert 'role="img"' in html


def test_failed_stale_or_mismatched_projection_cannot_claim_current():
    now=datetime.now(timezone.utc)
    d={'generated_at':now.isoformat(),'factpack_id':'pack','source_generated_at':'source'}
    s={'ok':True,'generated_at':d['generated_at']}
    selection={'factpack_id':'pack','generated_at':'source'}
    assert ui.health(d,s,selection,now)=='演变数据已同步'
    assert '待同步' in ui.health(d,{**s,'ok':False},selection,now)
    assert '待同步' in ui.health(d,s,{**selection,'factpack_id':'other'},now)
    assert '待同步' in ui.health(d,s,selection,now+timedelta(minutes=16))
    assert '待同步' in ui.health({'generated_at':d['generated_at']},s,{},now)


def test_fifteen_seats_same_eleven_columns_and_no_fake_candidate_grade(monkeypatch):
    import stock_profile_view
    monkeypatch.setattr(stock_profile_view,'load',lambda *a:{})
    rows=[]
    for m in ['A股','美股','港股']:
        for i in range(5):
            rows.append({'code':str(i)+m,'name':'测试','market':m,'watch_rank':i+1,
                         'tier':None,'audit_score':None,'horizon':'short','trade_plan':{},
                         'book_checks':[],'gaps':['缺GPT'],'reason':'研究线索'})
    # This case covers fresh, unreviewed candidates, not an expired projection.
    doc={'factpack_id':'pack','source_generated_at':'source','rows':rows,
         'generated_at':datetime.now(timezone.utc).isoformat()}
    rendered=watch.html(doc,{'factpack_id':'pack','generated_at':'source'})
    soup=BeautifulSoup(rendered,'html.parser')
    assert len(soup.select('tr.v88-watch-row'))==15
    assert all(len(r.find_all('td',recursive=False))==11 for r in soup.select('tr.v88-watch-row'))
    for row in soup.select('tr.v88-watch-row'):
        cells = row.find_all('td', recursive=False)
        assert row['data-tier'] == 'none'
        assert '新候选·待双审' in cells[1].get_text()
        assert '审核分：尚未完成双审' in cells[1].get_text()
        assert '候选研究·不可执行' in cells[3].get_text()
        assert '当前双审证据待更新；未授级' in cells[8].get_text()
    assert 'PENDING' not in rendered


def test_single_point_is_not_fake_market_trend():
    assert '不绘制虚构走势' in ui.price_chart({'prices':[{'date':'2026-09-11','close':10}]})
    assert 'role="img"' in ui.price_chart({'prices':[{'date':'2026-09-11','close':10},{'date':'2026-09-14','close':9}]})


def test_deep_history_is_collapsed_and_cannot_be_mistaken_for_year_price_trend():
    stock={'code':'TEST','name':'测试','current_tier':'1A','last_value_tier':'1A','events':[
        {'kind':'registry_event','event_at':'2026-09-06T10:00:00+08:00','captured_at':'2026-09-13T10:00:00+08:00',
         'event':{'kind':'ADDED','from':None,'to':'1A','score':65}},
        {'kind':'registry_event','event_at':'2026-09-13T10:00:00+08:00','captured_at':'2026-09-13T10:00:00+08:00',
         'event':{'kind':'UPDATED','from':'1A','to':'1A','score':70}}]}
    soup=BeautifulSoup(ui.stock_html(stock,compact=True),'html.parser')
    fold=soup.select_one('details.v88-grade-history-fold')
    assert fold is not None and not fold.has_attr('open')
    assert '2条' in fold.find('summary').text
    assert '不是股价趋势' in fold.text and '未来一年走向见年度趋势分析' in fold.text
    chart=fold.find('svg')
    assert chart and '非股价走势或未来预测' in chart['aria-label']
    assert '2026-09-06' in chart.text and '2026-09-13' in chart.text
    assert '2025-' not in chart.text
    assert '65' in fold.text and '70' in fold.text


def test_old_projection_labels_grade_as_snapshot_and_net_r_requires_settlement_proof():
    stock={'code':'TEST','name':'测试','current_tier':'1A','events':[],
        'contracts':[{'recorded_at':'2026-09-01','original_tier':'1A','original_score':70,
        'status_label':'模拟待核','entry_range':[10,11],'take_profit_range':[13,14],
        'stop':9,'deadline':'2026-09-30','net_r':9.876,'status':'WAITING_DATA',
        'settlement_verified':False,'actual_fill':False,'original_audit_id':'a','contract_id':'c','horizon':'short'}]}
    page=ui.stock_html(stock)
    assert '最近监控快照评级 1A' in page and '当前 1A' not in page and '9.876' not in page
    stock['contracts'][0].update(status='SETTLED',settlement_verified=True)
    assert '模拟净R 9.876' in ui.stock_html(stock)


def test_grade_chart_orders_events_by_real_clock_without_changing_original_rows():
    stock={'events':[{'kind':'registry_event','event_at':at,'event':{'to':grade}} for at,grade in
        [('2026-09-13T10:00:00+08:00','2A'),('2026-09-06T10:00:00+08:00','1A')]]}
    page=ui.grade_chart(stock)
    assert page.index('2026-09-06T')<page.index('2026-09-13T')
    assert stock['events'][0]['event_at'].startswith('2026-09-13')
    stock['events'][0]['event_at']='2026-09-13T10:00:00'
    assert '时间待核' in ui.grade_chart(stock)


def test_observation_rejects_bad_prices_and_names_reference_anchor():
    for value in [True,0,float('nan')]:
        assert '<svg' not in ui.price_chart({'prices':[{'date':'2026-09-01','close':10},{'date':'2026-09-02','close':value}]})
    page=ui.price_chart({'prices':[{'date':'2026-09-01','close':10},{'date':'2026-09-02','close':11}],
                        'source':'known source','basis':'unadjusted'})
    assert '原参考收盘与登记后收盘' in page and 'known source' in page and '非实际成交收益' in page
