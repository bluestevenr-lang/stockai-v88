from market_coverage_view import coverage_rows

def test_coverage_does_not_merge_markets_or_promote_financial_availability():
    daily={'markets':{'A股':{'denominator':100,'quote_available':100,'verified_daily':20,
                           'target_minimum':90,'daily_target_met':False}}}
    financial={'markets':{'A股':{'denominator':100,'three_statement_source_available':100}}}
    rows=coverage_rows(daily,financial)
    assert rows[0]['日线验收']=='未达到90%'
    assert rows[0]['财务研究完整核验']==0
    assert rows[1]['三表资料可得']=='尚未核验'
    assert rows[2]['日线验收']=='未达到90%'


def test_display_revalidates_session_and_retains_old_data_as_history():
    from datetime import datetime
    entry={'denominator':100,'verified_daily':93,'attempted':100,'target_minimum':90,
           'daily_target_met':True,'directories_complete':True,
           'verified_daily_by_session':{'2026-09-04':93},'quote_available_by_session':{'2026-09-04':100}}
    daily={'markets':{'A股':entry,'港股':entry,'美股':entry}}
    before=coverage_rows(daily,{},datetime.fromisoformat('2026-09-07T14:59:59+08:00'))
    assert all(r['日线验收']=='达到90%' for r in before)
    after=coverage_rows(daily,{},datetime.fromisoformat('2026-09-07T15:00:00+08:00'))
    assert after[0]['日线已校验'].startswith('0 / 100')
    assert after[0]['历史日线待更新']==93
    assert after[0]['日线验收']=='待刷新至 2026-09-07'
    assert after[1]['日线验收']==after[2]['日线验收']=='达到90%'


def test_undated_legacy_success_has_no_current_acceptance_authority():
    r=coverage_rows({'markets':{'美股':{'denominator':10,'verified_daily':10,'daily_target_met':True}}},{})
    assert r[2]['日线验收']=='历史快照·日期待复核'
