"""Focused stock route. Read existing evidence; never run the overview as setup.

The main app dispatches here before any pool, provider or homepage initialization.
All decision calculations below are the same pure engines as the full deep view.
The complete overview/manual research tools remain at the non-focused URL.
"""
from html import escape
import json
import logging
import re
from urllib.parse import quote


def normalize_code(value):
    """Only bounded stock symbols belong to this route; no arbitrary paths."""
    raw = str(value or '').strip().upper()
    if not re.fullmatch(r'[A-Z0-9][A-Z0-9.\-$]{0,19}', raw):
        return None
    from modules.utils import to_yf_cn_code
    return to_yf_cn_code(raw)


def navigation(code):
    full = '/?q=' + quote(code, safe='') + '#v88-deep-analysis'
    return ('<nav class="v88-focused-nav"><a href="/" target="_self">← 返回总览</a>'
            f' · <a href="{escape(full, quote=True)}" target="_self">完整研究工具</a></nav>')


def calculate(context, frame, quality, name, code):
    """Do not cache signed context or infer a central grade from a technical score."""
    from deep_cross_validation import reconcile
    from deep_synthesis import build
    technical, trend, cycles = {}, {}, {}
    if frame is not None:
        import cloud_engine
        from v88_decision_core import evaluate_decision
        from stock_horizon import analyze
        trend = cloud_engine.analyze_trend_full(frame) or {}
        technical = evaluate_decision(frame, trend, name=name, code=code)
        cycles = analyze(name, code, frame, full=trend, allow_ai=False)
    cross = reconcile(context, frame, quality, technical)
    synthesis = build(context, frame, quality, technical, trend, cross)
    cross['joint_conclusion'] = synthesis
    from annual_outlook import build as annual_build
    annual = annual_build(context, frame, quality, synthesis=synthesis)
    from period_consistency import build as period_build
    period = period_build(code, frame, quality, cycles, trend, annual, synthesis)
    cross['period_consistency'] = period
    from trend_scenarios import build_stock
    future = build_stock(annual, synthesis, period, code=code, name=name)
    cross['future_scenario'] = future
    if cycles:
        cycles = dict(cycles, period_consistency=period, future_scenario=future)
    return {'technical': technical, 'trend': trend, 'cycles': cycles,
            'cross': cross, 'synthesis': synthesis, 'annual_outlook': annual,
            'period_consistency': period, 'future_scenario': future}


def history_window(frame):
    """One calendar year ending on the latest real bar, never a forecast axis."""
    if frame is None or frame.empty:
        return frame
    import pandas as pd
    start = pd.Timestamp(frame.index[-1]) - pd.DateOffset(years=1)
    return frame.loc[frame.index >= start]


def _download_evidence(st, result, code):
    # Keep the central cross-check schema intact, extending only this download.
    payload = {**result['cross'], 'annual_outlook': result.get('annual_outlook') or {}}
    st.download_button('下载本票交叉核验凭据',
                       json.dumps(payload, ensure_ascii=False, indent=2),
                       file_name=f'V88-{code}-cross-validation.json', mime='application/json',
                       key=f'focus_cross_download_{code}')


def _sell_evidence(st, code):
    """Read only the already published independent sell lane, never recompute it."""
    from market_data_helper import CORE
    from modules.utils import to_yf_cn_code
    try:
        doc = json.loads((CORE / 'data' / 'sell_grade.json').read_text(encoding='utf-8'))
    except (OSError, ValueError):
        st.caption('独立卖侧风险资料暂未读取成功；原合同失效条件仍须遵守。')
        return
    row = next((r for r in doc.get('rows', [])
                if to_yf_cn_code(r.get('code', '')) == code), None)
    if row:
        negative = str(row.get('level')) in {'-1A', '-2A', '-3A'}
        hard_stop = any(str(reason).startswith(('破止损', '右侧仓机械认错'))
                        for reason in (row.get('bypass') or []))
        if negative or hard_stop:
            st.warning('本票已有独立卖侧风险线索，请核对原始日期、复核状态和条件；'
                       '买侧评级不能抵消保护要求。')
        with st.expander('独立卖侧风险证据', expanded=False):
            st.caption(f"原发布：{doc.get('generated_at') or '时间未注明'}；以下为原记录，"
                       '本页不重新授予卖出许可。0级代表该次未触发，不等于安全或建议持有。')
            st.json(row)


def _technical_view(st, result, frame, quality, name, code):
    import cloud_engine
    from deep_cross_validation import html as cross_html
    from deep_synthesis import html as synthesis_html
    from annual_outlook import html as annual_html
    from stock_horizon import cycle_visual_html, table_rows
    from evidence_visuals import deep_overview, contract_strip
    st.markdown(deep_overview(result.get('synthesis'), result.get('annual_outlook'),
                             result.get('period_consistency')), unsafe_allow_html=True)
    st.markdown(cycle_visual_html(dict(result.get('cycles') or {},
        future_scenario=result.get('future_scenario')), name, code), unsafe_allow_html=True)
    from period_consistency import html as period_html
    with st.expander('未来一年分阶段条件与年度依据', expanded=False):
        st.markdown(annual_html(result.get('annual_outlook') or {}), unsafe_allow_html=True)
        st.markdown(period_html(result.get('period_consistency')), unsafe_allow_html=True)
    st.markdown(cross_html(result['cross']) + synthesis_html(result['synthesis'], details=False),
                unsafe_allow_html=True)
    st.markdown(contract_strip((result.get('synthesis') or {}).get('original_plan'),
        ((result.get('synthesis') or {}).get('observations') or {}).get('last'),
        verified=(result.get('cross') or {}).get('status') in {'一致·等原条件', '一致·仍按中央执行闸'}), unsafe_allow_html=True)
    with st.expander('研究核验凭据 · 含未来一年条件框架', expanded=False):
        _download_evidence(st, result, code)
    if frame is None:
        st.warning('本地合格完整日线暂缺，无法计算当前技术判断。中央原合同与历史仍保留。')
        st.caption(str(quality.get('error_detail') or '日线覆盖未完成'))
        st.markdown('需要补查免费日线或公司、三表、新闻时，可打开上方“完整研究工具”按需请求。')
        return

    tech, trend = result['technical'], result['trend']
    st.caption(f"完整日线截至 {quality.get('source_asof') or '未注明'} · "
               f"来源：{quality.get('source') or '未注明'} · "
               f"口径：{quality.get('price_basis') or '未声明'} · "
               f"{quality.get('data_points', len(frame))} 根；不拼接盘中报价。")
    st.markdown(f"**量价辅助分：{tech.get('unified_score', '未形成')} / 100** · "
                f"短 / 中 / 长：{tech.get('short_score', '—')} / "
                f"{tech.get('medium_score', '—')} / {tech.get('long_score', '—')}")
    st.caption('同源技术计算用于研究，不能补齐中央审核、授级或改动原目标、失效价及截止日。')
    turning = trend.get('turning') or {}
    if turning.get('side'):
        st.warning(f"{turning.get('label') or '技术转折预警'}："
                   + '；'.join(turning.get('signals') or [])
                   + '。请按原合同复核，预警不自动产生交易动作。')
    with st.expander('趋势分项拆解', expanded=False):
        st.markdown(f"**趋势状态：{trend.get('stage', '未形成')}** · "
                    f"量价：{trend.get('vp', '未形成')} · 水位：{trend.get('water', '未形成')}")
        readout = cloud_engine.plain_readout(trend, turning if turning.get('side') else None)
        if readout:
            st.markdown('\n'.join('- ' + line for line in readout))
        rows = [{'维度': k, '得分': score, '权重': f'{weight:.0%}', '实际情况': detail}
                for k, (score, weight, detail) in (trend.get('breakdown') or {}).items()]
        if rows:
            st.dataframe(rows, hide_index=True, width='stretch')
    with st.expander('历史量价计算明细 · 按需查看', expanded=False):
        st.dataframe(table_rows(result['cycles']), hide_index=True, width='stretch')

    # Raw bars stay local for verified calculations/downloads; do not build two
    # duplicate candlestick payloads on every focused page visit.
    st.caption('本页省略重复K线；年度历史图可按需展开。本地计算与绘图不调用GPT。')
    with st.expander('更多研究与按需资料', expanded=False):
        st.markdown('完整研究工具保留公司与三表补查、新闻、指数辅助、个人决策锚点、'
                    'K线选点和手动策略研究。')
        st.markdown(navigation(code), unsafe_allow_html=True)


def render(st, raw_code):
    st.set_page_config(layout='wide', page_title='V88 · 个股深度', page_icon='👑',
                       initial_sidebar_state='collapsed')
    from presentation_style import CSS
    st.markdown(CSS, unsafe_allow_html=True)
    code = normalize_code(raw_code)
    from stock_switcher import render as render_stock_switcher
    render_stock_switcher(st, code or '', key='v88_focused_stock_switch')
    if code is None:
        if raw_code:
            st.warning('链接中的代码无效，请在上方选择个股。')
        else:
            st.caption('在上方输入名称或代码，选中后直接打开深度分析。')
        st.markdown('<a href="/" target="_self">← 返回总览</a>', unsafe_allow_html=True)
        return
    st.markdown(navigation(code), unsafe_allow_html=True)
    st.markdown('<div id="v88-deep-analysis"></div>', unsafe_allow_html=True)
    st.subheader(f'个股深度 · {code}')
    # Render the central contract before loading the local history or chart stack.
    from deep_cross_validation import load_context
    from deep_analysis_data import fetch, report_html
    context = {'code': code, 'selection': {}, 'row': {}, 'formal': False, 'card': {}}
    try:
        context = load_context(code)
        st.markdown(report_html(code, context=context), unsafe_allow_html=True)
    except Exception as exc:
        logging.exception('聚焦深度：中央原合同读取失败')
        st.warning(f'中央原合同暂未读取成功（{type(exc).__name__}）；技术研究不授予评级。')
    name = (context.get('row') or {}).get('name') or code
    _sell_evidence(st, code)
    try:
        frame, quality = fetch(code, allow_network=False)
        result = calculate(context, frame, quality, name, code)
        _technical_view(st, result, frame, quality, name, code)
    except Exception as exc:
        logging.exception('聚焦深度：技术研究读取失败')
        st.warning(f'技术研究暂未完成（{type(exc).__name__}）；请保留原合同条件并核查数据缺口。')
    st.markdown(navigation(code), unsafe_allow_html=True)
    st.markdown('<div id="v88-focused-deep-complete">本页读取结束；未覆盖或不一致之处以上方提示为准。</div>',
                unsafe_allow_html=True)
