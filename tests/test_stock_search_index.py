"""Identity/coverage regressions for the no-network detail-page picker."""
import json
from pathlib import Path
import pytest
import stock_search_index as picker


@pytest.fixture
def catalog():
    rows = [
        {'code':'688002.SH','name':'睿创微纳','market':'A股'},
        {'code':'920976.BJ','name':'视声智能','market':'A股'},
        {'code':'000001.SZ','name':'平安银行','market':'A股'},
        {'code':'00700.HK','name':'TENCENT','market':'港股'},
        {'code':'AAPL','name':'Apple Inc. Common Stock','market':'美股'},
        {'code':'BRK.B','name':'Berkshire Hathaway B','market':'美股'},
        {'code':'BRK.A','name':'Berkshire Hathaway A','market':'美股'},
        {'code':'AA.PA','name':'Separate class','market':'美股'},
        {'code':'900901.SS','name':'云赛B股','market':'B股'},
    ]
    profiles = {'records':{
        '700.HK':{'code':'700.HK','market':'港股','name_zh':'腾讯控股','name_en':'Tencent Holdings'},
        'AAPL':{'code':'AAPL','market':'美股','name_zh':'苹果','name_en':'Apple Inc.'},
        'BRK_B':{'code':'BRK_B','market':'美股','name_zh':'伯克希尔-B','name_en':'Berkshire Hathaway Inc.'},
    }}
    names = [{'c':'0700.HK','n':'腾讯','m':'港股'}, {'c':'688002.SS','n':'睿创','m':'A股'},
             {'c':'688002.SS','n':'错误市场别名','m':'港股'}, {'c':'DELISTED','n':'不再在目录','m':'美股'}]
    return picker.build_catalog(rows, profiles, names)


@pytest.mark.parametrize('query,code',[
    ('688002.SH','688002.SS'), ('SH688002','688002.SS'), ('688002','688002.SS'),
    ('视声智能','920976.BJ'), ('920976','920976.BJ'), ('000001','000001.SZ'),
    ('700','0700.HK'), ('00700','0700.HK'), ('HK00700','0700.HK'),
    ('00700.HK','0700.HK'), ('700.HK','0700.HK'), ('腾讯','0700.HK'),
    ('苹果','AAPL'), ('aapl','AAPL'), ('ＡＡＰＬ','AAPL'),
    ('BRK.B','BRK-B'), ('BRK_B','BRK-B'), ('BRK-A','BRK-A'), ('900901','900901.SS'),
])
def test_exact_code_name_alias_class_identity(catalog, query, code):
    row = picker.resolve_exact(query, catalog)
    assert row['code'] == code
    assert picker.resolve_exact(row['label'],catalog)['identity'] == row['identity']


def test_label_order_and_market(catalog):
    us = catalog['by_code']['AAPL']
    assert us['label'] == 'Apple Inc. · 苹果 · AAPL · 美股'
    assert catalog['by_code']['700.HK']['label'] == '腾讯控股 · 0700.HK · 港股'
    assert catalog['by_code']['688002.SS']['label'] == '睿创微纳 · 688002.SS · A股'


def test_partial_search_requires_explicit_selection(catalog):
    assert picker.search('微纳',catalog)[0]['code'] == '688002.SS'
    assert picker.resolve_exact('微纳',catalog) is None
    assert len(picker.search('Berkshire',catalog)) == 2
    assert picker.resolve_exact('Berkshire',catalog) is None
    assert picker.search('完全不存在',catalog) == []
    assert picker.resolve_exact('NOTLISTED',catalog) is None
    assert picker.resolve_exact('999999',catalog) is None


def test_duplicate_names_preserve_distinct_securities():
    doc = picker.build_catalog([{'code':'0011.HK','name':'同名','market':'港股'},
                               {'code':'600011.SS','name':'同名','market':'A股'}])
    assert len(picker.search('同名',doc)) == 2
    assert picker.resolve_exact('同名',doc) is None
    assert picker.resolve_exact('600011',doc)['market'] == 'A股'


def test_profiles_never_cross_identity_and_stale_names_cannot_add_retired(catalog):
    assert 'DELISTED' not in catalog['by_code']
    assert not picker.search('错误市场别名',catalog)
    doc = picker.build_catalog([{'code':'AAPL','name':'Apple directory','market':'美股'}],
        {'records':{'AAPL':{'code':'MSFT','market':'美股','name_en':'Microsoft','name_zh':'微软'}}})
    assert picker.search('微软',doc) == []
    assert doc['rows'][0]['name'] == 'Apple directory'


def test_full_directory_is_not_capped_or_graded():
    rows = [{'code':f'600{x:03}.SS', 'name':f'公司{x}', 'market':'A股'} for x in range(200)]
    doc = picker.build_catalog(rows)
    assert doc['directory_total'] == 200
    assert doc['counts'] == {'A股':200}
    assert doc['model_calls'] == doc['network_calls'] == 0
    assert doc['no_grade_authority'] is True
    assert len(picker.search('公司',doc,limit=150)) == 150
    with pytest.raises(ValueError): picker.search('公司',doc,limit=0)


def test_invalid_footer_or_code_does_not_become_option():
    doc=picker.build_catalog([{'code':'File Creation Time: 09112026','name':'footer','market':'美股'},
        {'code':'<script>alert(1)</script>','market':'美股'}, {'code':'0.HK','market':'港股'},
        {'code':'688002.SS','name':'ok','market':'港股'}])
    assert doc['rows'] == []


def test_market_filter(catalog):
    assert picker.search('腾讯',catalog,market='A股') == []
    assert picker.search('腾讯',catalog,market='港股')[0]['code'] == '0700.HK'


def test_file_cache_refreshes_without_restart(tmp_path):
    core=tmp_path/'core';(core/'data').mkdir(parents=True);(core/'src').mkdir()
    names=tmp_path/'names.json'
    names.write_text(json.dumps([{'n':'公司甲','c':'600001.SS','m':'A股'}]))
    picker._load_cached.cache_clear()
    first=picker.load_catalog(core,names_path=names)
    assert first['rows'][0]['name']=='公司甲'
    names.write_text(json.dumps([{'n':'公司甲更新名称','c':'600001.SS','m':'A股'},
                                 {'n':'公司乙','c':'600002.SS','m':'A股'}]))
    second=picker.load_catalog(core,names_path=names)
    assert len(second['rows'])==2
    assert second['rows'][0]['name']=='公司甲更新名称'
    assert first['source_signature'] != second['source_signature']
    assert '旧名称索引' in second['scope']


def test_profile_updates_invalidate_cache(tmp_path):
    core=tmp_path/'core';(core/'data').mkdir(parents=True);(core/'src').mkdir()
    names=tmp_path/'names.json';names.write_text(json.dumps([{'n':'Apple','c':'AAPL','m':'美股'}]))
    first=picker.load_catalog(core,names_path=names)
    (core/'data/stock_profiles_pub.json').write_text(json.dumps({'records':{'AAPL':{
        'code':'AAPL','market':'美股','name_en':'Apple Inc.','name_zh':'苹果'}}}))
    second=picker.load_catalog(core,names_path=names)
    assert '中文名待核' in first['rows'][0]['label']
    assert '苹果' in second['rows'][0]['label']


def test_latin_query_does_not_match_across_unrelated_word_boundaries():
    doc=picker.build_catalog([
        {'code':'000100.SZ','name':'TCL科技','market':'A股'},
        {'code':'1070.HK','name':'TCL ELECTRONICS','market':'港股'},
        {'code':'SKYY','name':'First Cloud Fund','market':'美股'}])
    assert {r['code'] for r in picker.search('TCL',doc)}=={'000100.SZ','1070.HK'}
    assert picker.search('Cloud',doc)[0]['code']=='SKYY'


@pytest.mark.parametrize('query', ['zgjs', 'ZGJS', 'ｚｇｊｓ', 'zhongguojushi', 'zhong guo ju shi'])
def test_pinyin_restores_user_reported_stock(query):
    doc=picker.build_catalog([{'code':'600176.SS','name':'中国巨石','market':'A股'}])
    assert picker.search(query,doc)[0]['code']=='600176.SS'
    assert picker.resolve_exact(query,doc)['code']=='600176.SS'


def test_pinyin_duplicates_require_selection_and_codes_take_priority():
    rows=[{'code':'600001.SS','name':'中光技术','market':'A股'},
          {'code':'600176.SS','name':'中国巨石','market':'A股'}]
    doc=picker.build_catalog(rows)
    assert len(picker.search('zgjs',doc))==2
    assert picker.resolve_exact('zgjs',doc) is None
    doc=picker.build_catalog(rows+[{'code':'ZGJS','name':'Exact ticker','market':'美股'}])
    assert picker.search('zgjs',doc)[0]['code']=='ZGJS'
    assert picker.resolve_exact('zgjs',doc)['code']=='ZGJS'
    assert len(picker.search('zgjs',doc,market='A股'))==2


def test_pinyin_for_phrases_aliases_and_mixed_latin_names(catalog):
    assert picker.resolve_exact('rcwn',catalog)['code']=='688002.SS'
    assert picker.resolve_exact('txkg',catalog)['code']=='0700.HK'
    assert picker.resolve_exact('pingan yinhang',catalog)['code']=='000001.SZ'
    doc=picker.build_catalog([{'code':'1070.HK','name':'TCL电子','market':'港股'},
                             {'code':'1963.HK','name':'重庆银行','market':'港股'}])
    assert picker.resolve_exact('tcldz',doc)['code']=='1070.HK'
    assert picker.resolve_exact('cqyh',doc)['code']=='1963.HK'
