"""Future clock/curve selection keeps identities, provenance, and source scores."""
from copy import deepcopy
from datetime import datetime, timezone
import re
import sys
from types import SimpleNamespace

import rotation_ui


def sector_forecast():
    from exchange_sessions import latest_completed
    source=latest_completed('A股',datetime.now(timezone.utc)).isoformat()
    return {'markets':{'A股':{}}, 'source_asof':source,
            'analysis_time':'2026-09-13 12:00',
            'source_dates_by_market':{'A股':[source]},
            'trajectories':{'A股':[
                {'name':'强结构','symbol':'512660.SS','facts':{'1d':1,'5d':3,'20d':6,'vs_ma20':2,'vs_ma60':4,'vol_ratio':1.2},'now':55,
                 'points':{'2周':{'score':93,'trigger':'量价确认','invalid':'回落'},'8周':{'score':1}}},
                {'name':'弱结构','symbol':'512800.SS','facts':{'1d':-1,'5d':-3,'20d':-6,'vs_ma20':-2,'vs_ma60':-4,'vol_ratio':1.2},'now':40,'points':{}}]}}


def test_future_clock_and_selectable_curves_keep_all_sector_objects():
    forecast=sector_forecast();before=deepcopy(forecast)
    html=rotation_ui.rotation_map_html(forecast)
    assert 'rf-phase-clock' in html and html.count('class="rf-future-panel"')==2
    assert '未来一年变化路径' in html and '历史因子与原条件档 · 推演依据' in html
    ids=re.findall(r'<section class="rf-future-panel" id="([^"]+)"',html)
    assert len(ids)==len(set(ids))==2
    assert all(f'href="#{i}"' in html for i in ids)
    assert html.count('<style>')==1
    assert forecast==before


def test_past_score_change_does_not_change_future_lines():
    f=sector_forecast()
    first=rotation_ui.rotation_map_html(f)
    f['trajectories']['A股'][0]['points']['2周']['score']=2
    f['trajectories']['A股'][0]['points']['8周']['score']=99
    second=rotation_ui.rotation_map_html(f)
    paths=lambda html:re.findall(r'<(?:path|polyline)[^>]*(?:class="[^"]*vf-|data-branch)[^>]*>',html)
    # Entire future panels remain identical; only folded original-score table changes.
    panels=lambda html:re.findall(r'<section class="rf-future-panel".*?</section></section>',html,re.S)
    assert panels(first) and panels(first)==panels(second)
    assert first!=second


def test_analysis_timestamp_cannot_supply_missing_source_clock():
    f=sector_forecast();f.pop('source_asof');f.pop('source_dates_by_market')
    html=rotation_ui.rotation_map_html(f)
    assert html.count('data-status="missing"')==2
    assert '2026-09-13 12:00' in html
    assert '证据截至 时间待核' in html


def test_stock_clock_all_records_max_three_preview_calls_and_canonical_links(monkeypatch):
    calls=[]
    def build(code,name=''):
        calls.append(code)
        return {'status':'missing','code':code,'name':name,'source_asof':'2026-09-11','input_id':code,'points':[]}
    monkeypatch.setitem(sys.modules,'stock_future_context',SimpleNamespace(for_stock=build))
    monkeypatch.setitem(sys.modules,'stock_profile_view',SimpleNamespace(load=lambda:{},
        display_label=lambda name,code,profiles:name,
        link_html=lambda name,code,profiles:f'<a href="/?q={code}&focus=deep">{name}</a>'))
    stocks=[{'code':f'STOCK{i}','name':f'对象{i}','phase':['蓄势→领涨','派发→退潮','整理'][i%3],
             'direction':['up','down','hold'][i%3], 'source_asof':'2026-09-11','up':60,'down':30,'pos52':0} for i in range(12)]
    cycle={'stocks':stocks,'analysis_time':'2026-09-13 08:00'};before=deepcopy(cycle)
    html=rotation_ui.stock_cycle_html(cycle,profiles={})
    assert len(calls)==3 and len(set(calls))==3
    assert '全部 12 只保留' in html
    assert all(f'对象{i}' in html for i in range(12))
    assert '/?q=STOCK11&amp;focus=deep' in html
    assert 'rf-phase-clock' in html and '<td>0%<div>' in html
    assert cycle==before


def test_stale_cycle_record_never_builds_current_future_preview(monkeypatch):
    def forbidden(*args,**kwargs):raise AssertionError('stale source must not launch a current preview')
    monkeypatch.setitem(sys.modules,'stock_future_context',SimpleNamespace(for_stock=forbidden))
    monkeypatch.setitem(sys.modules,'stock_profile_view',SimpleNamespace(load=lambda:{},
        display_label=lambda name,code,profiles:name,
        link_html=lambda name,code,profiles:f'<a>{name}</a>'))
    html=rotation_ui.stock_cycle_html({'stocks':[{'code':'OLD','name':'旧记录','phase':'历史跟踪·等待新证据',
         'source_asof':'2026-08-12','stale_preserved':True,'direction':'hold'}]},profiles={})
    assert '旧记录' in html and '相位待核' in html
    assert 'class="rf-future-panel"' not in html
