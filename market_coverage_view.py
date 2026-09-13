"""Show full-universe evidence coverage without changing the rating list."""
from pathlib import Path
import json
import pandas as pd
from session_coverage import project


def coverage_rows(daily, financial, now=None):
    result=[]
    for market in ['A股','港股','美股']:
        d=daily.get('markets',{}).get(market,{})
        f=financial.get('markets',{}).get(market,{})
        n=d.get('denominator',0)
        current=project(market,d,now)
        dated=current['dated_receipt']
        count=current['verified_daily'] if dated else d.get('verified_daily',0)
        quoted=current['quote_available'] if dated else d.get('quote_available',0)
        status='达到90%' if current['daily_target_met'] else '未达到90%'
        if dated and current['expired_or_undated_daily'] and not current['daily_target_met']:
            status='待刷新至 '+str(current['required_session'] or '已核实交易日')
        elif not dated and d.get('daily_target_met'):
            status='历史快照·日期待复核'
        if any(c.get('market')==market and '未来' in c.get('time_status','') for c in daily.get('catalogs',[])):
            status='目录含未来版本·暂不验收'
        def cell(count,denominator):
            return f'{count:,} / {denominator:,}（{count/denominator:.2%}）' if denominator else '尚未核验'
        result.append({'市场':market,'目录分母':n,'90%至少':d.get('target_minimum',0),
                       '有时点报价':cell(quoted,n),
                       '日线已校验':cell(count,n),
                       '当前所需交易日':current['required_session'] or '日历待核验',
                       '历史日线待更新':current['expired_or_undated_daily'] if dated else '日期待核验',
                       '三表资料可得':cell(f.get('three_statement_source_available',0),f.get('denominator',0)),
                       '日线未尝试':d.get('unattempted',n),'日线待补/口径复核':max(0,d.get('attempted',n)-count),
                       '日线验收':status,
                       '财务研究完整核验':f.get('financial_research_fully_verified',0)})
    return result


def render(st,core):
    def read(name):
        path=Path(core)/'data'/name
        return json.loads(path.read_text()) if path.exists() else {}
    daily=read('universe_data_coverage.json');financial=read('financial_data_coverage.json')
    with st.expander('📊 三市场数据覆盖验收 · 每个市场至少90%',expanded=False):
        if not daily:
            st.warning('全市场数据验收结果尚未生成。');return
        st.caption('新增现金费用：0元。分别核验中、美、港市场；目录检索数量、报价数量和推荐数量分开记录。')
        from evidence_visuals import CSS, coverage_card
        bars = []
        for market in ('A股', '港股', '美股'):
            raw = daily.get('markets', {}).get(market, {})
            current = project(market, raw)
            catalog_pending = any(c.get('market') == market and '未来' in c.get('time_status', '') for c in daily.get('catalogs', []))
            bars.append(coverage_card(market, current['verified_daily'] if current['dated_receipt'] else None,
                raw.get('denominator'), current['daily_target_met'], ('保守目录并集 · ' if catalog_pending else '有效日线 · ') + str(current['required_session'] or '时点待核'), pending_reason=('日线比例≥90% · 目录待核' if current['daily_target_met'] else '日线及目录待核') if catalog_pending else ''))
        st.markdown(CSS+'<div class="v88-evidence-cards">'+''.join(bars)+'</div>', unsafe_allow_html=True)
        frame=pd.DataFrame(coverage_rows(daily,financial))
        st.dataframe(frame,hide_index=True,use_container_width=True)
        running=daily.get('in_progress') or financial.get('in_progress')
        st.caption(f"{'采集进行中' if running else '本轮采集已结束'}；日线进度时间 {daily.get('generated_at','未知')}；"
                   f"财报进度时间 {financial.get('generated_at','未知')}。本区域每60秒读取最新记录。")
        st.caption('分母覆盖交易所目录，剔除明确标注的ETF/ETP，其余未细分证券仍保留。'
                   '日线校验包含证券身份、OHLCV、最近完整交易日、同日跨提供方价格比对和近61根连续性。'
                   '停牌、新上市、缺口和源错误分别留痕；请求两年历史不等于每只两年齐备。')
        st.caption('三表资料可得，仅表示原始聚合表已下载并核对分页。公告时点、修订版本、币种单位和盈利预测需继续核验；'
                   '这些覆盖数不授予1A/2A/3A，也不是上涨概率。')
        st.caption('当前日线覆盖随各市场完整交易日重算；新一交易日收盘后，旧日线保留为历史资料并列入待更新，'
                   '不能继续冒称最新行情已达标。盘中仍以最近完整交易日核验；此处不提供实时成交保证。')
        semantics=read('hk_quote_semantics_audit.json')
        if semantics.get('corroborated_examples'):
            st.caption('港股口径复核：已用港交所原始日报确认，部分官方收市参考价可超出成交高低价；'
                       '不能一概判作错误数据，也不能据此假定可成交。相关序列继续单独核对，不改高低价、不虚增日线通过数。')
            with st.expander('查看港交所与原始行情的已核实反例'):
                st.dataframe(pd.DataFrame([{'代码':r['code'],'日期':r['date'],'官方收市参考价':r['close'],
                                           '成交最高':r['trade_high'],'成交最低':r['trade_low']}
                                          for r in semantics['corroborated_examples']]),hide_index=True)
                st.markdown('[港交所收市价与成交高低价定义](https://www.hkex.com.hk/Services/Trading/Securities/Overview/Trading-Mechanism?sc_lang=en)')
        reference=read('hk_reference_quotes_status.json')
        if reference.get('all_indexed_fields_match_source'):
            def keep_reference_open():
                st.session_state['v88_hk_reference_open']=True
            with st.expander('港交所原始参考报价查询',expanded=st.session_state.get('v88_hk_reference_open',False)):
                reports=reference.get('reports',[])
                st.caption('已保存官方主板日报日期：'+ '、'.join(r['date'] for r in reports)+
                           '。收市参考价与成交区间分别列示；此处没有开盘价，不计入连续日线覆盖，也不用于模拟成交。')
                with st.form('v88_hk_reference_search'):
                    reference_code=st.text_input('查询港股官方参考报价',placeholder='例如 00022.HK / 01810.HK')
                    reference_submit=st.form_submit_button('查看官方报价原字段',on_click=keep_reference_open)
                if reference_submit:
                    st.session_state['v88_hk_reference_code']=reference_code.strip()
                if st.session_state.get('v88_hk_reference_code'):
                    from hk_reference_quotes import read as read_reference
                    observations=read_reference(st.session_state['v88_hk_reference_code'],base=core)
                    if observations:
                        def reference_value(row,key):
                            value=row[key]
                            if value is not None:return f'{value:g}'
                            return '停牌未报' if row['trading_status']=='suspended' else '源未提供'
                        st.dataframe(pd.DataFrame([{'代码':r['code'],'日期':r['date'],'币种':r['currency'],
                            '收市参考价':reference_value(r,'reference_close'),'自动对盘成交最高':reference_value(r,'automatch_trade_high'),
                            '自动对盘成交最低':reference_value(r,'automatch_trade_low'),'原始成交股数':reference_value(r,'reported_shares_traded'),
                            '状态':{'suspended':'停牌','automatch_range_unavailable':'未提供成交区间',
                                  'automatch_range_reported':'提供成交区间','invalid_reported_range':'区间需复核'}[r['trading_status']]}
                            for r in observations]),hide_index=True)
                        st.download_button('下载官方参考报价凭据',json.dumps(observations,ensure_ascii=False,indent=2),
                                           file_name='v88_hk_reference_quotes.json',mime='application/json')
                    else:st.caption('已存日报中未找到该证券；不表示该证券没有价值。')
        concerns=[c for c in daily.get('catalogs',[]) if c.get('time_status')!='目录日期可核对']
        if concerns:
            st.caption('目录时间仍需核对：'+'；'.join(f"{c['directory']}：{c.get('time_status')}" for c in concerns))
        if daily.get('missing_directories'):
            st.warning('缺少交易所目录：'+'、'.join(daily['missing_directories']))
        st.download_button('下载三市场覆盖验收 CSV',frame.to_csv(index=False).encode('utf-8-sig'),
                           file_name='v88_market_coverage.csv',mime='text/csv')
        if (Path(core)/'data/financial_statement_index.sqlite').exists():
            with st.form('v88_financial_source_search'):
                code=st.text_input('查询个股三表原始资料',placeholder='例如 600519.SH / 01810.HK / AAPL')
                submit=st.form_submit_button('查看已有财报与凭据')
            if submit:
                st.session_state['v88_financial_source_code']=code.strip()
            if st.session_state.get('v88_financial_source_code'):
                from financial_statement_index import read
                statements=read(st.session_state['v88_financial_source_code'],base=core)
                st.caption(f'已索引 {len(statements)} 个报表/报告周期；不等于财务核验或评级通过。')
                if statements:
                    st.dataframe(pd.DataFrame([{'表类':r['label'],'发行人ID':r['issuer_id'],'报告期':r['period'],
                        '报告间隔代码':r['interval'],'币种原字段':r['currency'] or '源表未注明',
                        '原始页数':len(r['receipts']),'记录/科目数':len(r['source_data']['items']) or len(r['source_data']['source_rows'])} for r in statements]),hide_index=True)
                    st.download_button('下载原字段及数据凭据 JSON',json.dumps(statements,ensure_ascii=False,indent=2),
                                       file_name='v88_financial_sources.json',mime='application/json')
