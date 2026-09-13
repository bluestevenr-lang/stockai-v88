"""Evidence-bound rolling twelve-month outlook. Pure, no model/network/order I/O."""
from datetime import date, datetime, timezone
from html import escape
from calendar import monthrange
import hashlib
import json
import math
from zoneinfo import ZoneInfo

VERSION = 'annual-outlook-v2'
MARKETS = {'A股': 'Asia/Shanghai', '港股': 'Asia/Shanghai', '美股': 'America/New_York'}
COLUMNS = ['Open', 'High', 'Low', 'Close', 'Volume']
# Emitted by tencent_history only after complete source factors prove identity.
# This exact label remains a distinct basis, not generic unadjusted history.
VERIFIED_IDENTITY_BASIS = '腾讯原始OHLC整段；新浪明确完整前复权因子在所载日期均为1且偏移0，价格无修改；不是含息总回报'
BASIS = ('split-adjusted ohlc', 'yahoo ohlc拆股口径', '前复权', 'qfq', 'forward-adjusted',
         '不复权', 'none', 'unadjusted', 'sina cn original ohlc divided by source qfq factor',
         '腾讯前复权ohlc整段序列')


def _canonical(code):
    code = str(code or '').upper().strip().replace('.SH', '.SS')
    if code.endswith('.HK') and code[:-3].isdigit():
        return str(int(code[:-3])) + '.HK'
    return code


def _finite(n):
    return type(n) in (int, float) and math.isfinite(n)


def _months(day, n):
    year, month = divmod(day.year * 12 + day.month - 1 + n, 12)
    month += 1
    return day.replace(year=year, month=month, day=min(day.day, monthrange(year, month)[1]))


def _signature(frame):
    payload = frame[COLUMNS].to_csv()
    payload += json.dumps({k: frame.attrs.get(k) for k in ('source', 'source_asof', 'price_basis')}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


def _validate(context, frame, quality, now):
    """Do not repair, sort, rescale, splice or drop suspect bars here."""
    code = _canonical(context.get('code')); fact = context.get('fact') or {}; row = context.get('row') or {}
    market = row.get('market') or fact.get('market')
    market = market if market in MARKETS else '港股' if code.endswith('.HK') else 'A股' if code.endswith(('.SS', '.SZ', '.BJ')) else '美股'
    issues = []
    if frame is None or getattr(frame, 'empty', True):
        return None, market, ['缺少可核验的完整日线'], None
    try:
        if quality.get('error_detail') or frame.attrs.get('quality_error'):
            raise ValueError('行情源已有质量错误，须先修复')
        if not code or _canonical(quality.get('code')) != code:
            raise ValueError('证券身份与行情不一致')
        if any(_canonical(x.get('code')) != code for x in (fact, row) if x.get('code')):
            raise ValueError('中央个股身份冲突')
        if any(k not in frame for k in COLUMNS):
            raise ValueError('完整OHLCV字段缺失')
        if not frame.index.is_unique or not frame.index.is_monotonic_increasing:
            raise ValueError('行情日期重复或乱序')
        data = frame[COLUMNS].astype(float)
        if not all(math.isfinite(float(x)) for x in data.to_numpy().flat):
            raise ValueError('行情包含空值或非有限值')
        if (data[COLUMNS[:-1]] <= 0).any().any() or (data.Volume < 0).any():
            raise ValueError('价格或成交量无效')
        if ((data.Low > data[['Open', 'Close']].min(axis=1)) | (data.High < data[['Open', 'Close']].max(axis=1))).any():
            raise ValueError('高低价未包围开收盘')
        dates = [x.date() for x in data.index]
        if len(set(dates)) != len(dates):
            raise ValueError('同一交易日存在重复行情')
        if dates[-1] > now.astimezone(ZoneInfo(MARKETS[market])).date():
            raise ValueError('行情日期来自未来')
        from exchange_sessions import latest_completed
        expected = latest_completed(market, now)
        if dates[-1] != expected:
            raise ValueError('缺少最新已完成交易日；不能用旧行情作当前年度研判')
        if (str(quality.get('source_asof') or '')[:10] != dates[-1].isoformat()
                or str(frame.attrs.get('source_asof') or '')[:10] != dates[-1].isoformat()):
            raise ValueError('行情来源时点不一致')
        if not quality.get('source') or quality.get('source') != frame.attrs.get('source'):
            raise ValueError('同源行情标记缺失或冲突')
        basis = str(quality.get('price_basis') or '').lower()
        exact = {'前复权', 'qfq', 'forward-adjusted', '不复权', 'none', 'unadjusted',
                 VERIFIED_IDENTITY_BASIS.lower()}
        known_basis = basis in exact or any(basis.startswith(x) for x in BASIS if x not in exact)
        if not known_basis or quality.get('price_basis') != frame.attrs.get('price_basis'):
            raise ValueError('复权口径未核实或不一致')
        if quality.get('snapshot_signature') != _signature(frame):
            raise ValueError('行情快照签名不匹配')
        return data, market, issues, expected
    except (ValueError, TypeError, KeyError, AttributeError, ImportError) as exc:
        return None, market, [str(exc)], None


def _metrics(data, market, end):
    """A calendar year, not a mislabeled trailing 200-bar sample."""
    start = _months(end, -12); dates = [d.date() for d in data.index]
    window = data.loc[[start <= d <= end for d in dates]]
    from history_calendar import coverage
    proof = coverage([d.date().isoformat() for d in window.index], market, start.isoformat(), end.isoformat())
    close = data.Close; last = float(close.iloc[-1]); metrics = {
        'bars': len(data), 'window_start': start.isoformat(), 'window_end': end.isoformat(),
        'annual_bars': len(window), 'expected_annual_bars': proof['expected_sessions'],
        'calendar_basis': proof['calendar_basis'], 'last': last,
        'annual_complete': proof['continuity_ok'], 'missing_sessions': proof['missing_sessions'],
        'unexpected_dates': proof['unexpected_dates']}
    for n in (60, 120, 200):
        metrics[f'ma{n}'] = float(close.tail(n).mean()) if len(close) >= n else None
        metrics[f'ma{n}_slope20_pct'] = float((close.tail(n).mean()/close.iloc[-n-20:-20].mean()-1)*100) if len(close) >= n+20 else None
    metrics.update(return60_pct=float((last/close.iloc[-61]-1)*100) if len(close) >= 61 else None,
                   high60=float(data.High.tail(60).max()) if len(data) >= 60 else None,
                   low60=float(data.Low.tail(60).min()) if len(data) >= 60 else None,
                   return1_pct=float((last/close.iloc[-2]-1)*100) if len(close) >= 2 else None,
                   volume_ratio20=float(data.Volume.iloc[-1]/data.Volume.iloc[-21:-1].mean()) if len(data) >= 21 and data.Volume.iloc[-21:-1].mean() > 0 else None)
    if not window.empty:
        metrics['observed_window_high'] = float(window.High.max())
        metrics['observed_window_low'] = float(window.Low.min())
    if proof['continuity_ok'] and len(window) >= 200:
        wc = window.Close
        metrics.update(annual_high=float(window.High.max()), annual_low=float(window.Low.min()),
                       annual_return_pct=float((wc.iloc[-1]/wc.iloc[0]-1)*100),
                       annual_max_drawdown_pct=float((wc/wc.cummax()-1).min()*100),
                       current_drawdown_from_annual_high_pct=float((last/window.High.max()-1)*100),
                       annual_position_pct=float((last-window.Low.min())/(window.High.max()-window.Low.min())*100) if window.High.max() > window.Low.min() else 50.)
    else:
        metrics['annual_complete'] = False
    return metrics


def _history(frame, data, market, end, quality, metrics, now):
    """Export exactly the plotted observations; never fill absent sessions.

    The full input close series is retained for independent SMA recalculation.
    Missing expected sessions reset the SMA window rather than being silently
    skipped. Its digest also binds the calendar, source and original signature.
    """
    original = {'start': None, 'end': None, 'bars': 0}
    if frame is not None and not getattr(frame, 'empty', True):
        original = {'start': str(frame.index[0]), 'end': str(frame.index[-1]), 'bars': len(frame)}
    if end is None:
        try:
            from exchange_sessions import latest_completed
            end = latest_completed(market, now)
        except (ValueError, KeyError, ImportError):
            end = now.astimezone(ZoneInfo(MARKETS[market])).date()
    start = _months(end, -12)
    result = {'version': 'annual-history-series-v1', 'window_start': start.isoformat(),
              'window_end': end.isoformat(), 'source_range': original,
              'source': quality.get('source'), 'source_asof': quality.get('source_asof'),
              'price_basis': quality.get('price_basis'), 'snapshot_signature': quality.get('snapshot_signature'),
              'annual_complete': bool(metrics.get('annual_complete')),
              'expected_annual_bars': metrics.get('expected_annual_bars'),
              'missing_sessions': metrics.get('missing_sessions', []),
              'unexpected_dates': metrics.get('unexpected_dates', []),
              'calendar_basis': metrics.get('calendar_basis'),
              'points': [], 'source_closes': [], 'source_missing_sessions': [],
              'status': 'unavailable', 'gaps': [],
              'series_basis': '同源已核验收盘价；均线按连续完整交易日计算，窗口不足或缺日即留空；不补价、不拼接、不外推'}
    if data is not None:
        try:
            from history_calendar import coverage
            dates = [d.date().isoformat() for d in data.index]
            expected = coverage([], market, dates[0], dates[-1])['missing_sessions']
            positions = {d: i for i, d in enumerate(expected)}
            result['source_missing_sessions'] = sorted(set(expected) - set(dates))
            if set(dates) - set(expected):
                raise ValueError('含非交易日行情，暂不绘制')
            if not metrics:
                raise ValueError('年度日历核验未完成，暂不绘制')
            run = []; previous = None
            for day, raw in zip(dates, data.Close):
                value = float(raw); position = positions[day]
                interrupted = previous is not None and position != previous + 1
                if interrupted:
                    run = []
                run.append(value)
                result['source_closes'].append({'date': day, 'close': value})
                if start.isoformat() <= day <= end.isoformat():
                    point = {'date': day, 'close': value, 'break_before': interrupted}
                    point.update({f'ma{n}': math.fsum(run[-n:])/n if len(run) >= n else None for n in (60, 120, 200)})
                    result['points'].append(point)
                previous = position
            result['status'] = 'complete' if result['annual_complete'] else 'limited'
            for n in (60, 120, 200):
                missing = sum(p[f'ma{n}'] is None for p in result['points'])
                if missing:
                    result['gaps'].append(f'MA{n}有{missing}个显示日缺少连续{n}个交易日的前置行情，均线留空')
        except (ValueError, TypeError, KeyError, ImportError, IndexError) as exc:
            result['points'] = []; result['source_closes'] = []
            result['gaps'].append(str(exc))
    result['series_id'] = hashlib.sha256(json.dumps(result, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()).hexdigest()
    return result


def _evidence(context, synthesis, quality):
    synthesis = synthesis or {}; row = context.get('row') or {}; fact = context.get('fact') or {}
    pack = (context.get('selection') or {}).get('factpack_id'); card = context.get('card') or {}
    code = _canonical(context.get('code'))
    current = bool(pack and pack == context.get('loaded_factpack_id') == row.get('factpack_id') == synthesis.get('factpack_id')
                   and code == _canonical(synthesis.get('code')) and synthesis.get('joint_review_current') is True
                   and synthesis.get('snapshot_signature') == quality.get('snapshot_signature')
                   and (card.get('gpt') or {}).get('current') and (card.get('gpt') or {}).get('complete')
                   and row.get('joint_evidence') == fact.get('joint_evidence') and row.get('joint_evidence'))
    domains = synthesis.get('domains') or []
    current = current and {d.get('domain') for d in domains} == {'technical','sector','market','news','flow','fundamental','valuation','validation'}
    rendered = []
    for key in ('fundamental', 'valuation', 'sector', 'news', 'market', 'flow', 'validation', 'technical'):
        d = next((d for d in domains if d.get('domain') == key), {}) if current else {}
        votes = d.get('reviews') or []
        conclusions = [v.get('conclusion') for v in votes]
        state = '支持' if d.get('status') == 'observed' and len(votes) == 2 and all(x == '支持' for x in conclusions) else '反对' if any(x in ('反对', '阻断') for x in conclusions) or d.get('status') == 'conflict' else '分歧' if '混合' in conclusions else '有限/缺失'
        rendered.append({'domain': key, 'title': d.get('title') or {'fundamental':'基本面','valuation':'估值','sector':'板块轮动','news':'新闻催化','market':'市场环境','flow':'资金流','validation':'历史验证','technical':'技术量价'}[key],
                         'state': state, 'reasons': [v.get('reason', '') for v in votes] if current else ['尚无本票当前同包八域双审；不借用其它周期或证券结论']})
    by = {d['domain']: d for d in rendered}
    horizon = row.get('horizon'); long_current = bool(current and horizon == 'long' and (card.get('books') or {}).get('current') and (card.get('books') or {}).get('complete'))
    # max_calendar_days=365 is only a ceiling, not proof that both GPT legs
    # reviewed an actual full-year thesis. The current schema has no such seal.
    annual_gpt = False
    return {'joint_review_current': bool(current), 'original_horizon': horizon,
            'central_grade': row.get('tier') if current else None,
            'central_audit_score': card.get('total') if current and _finite(card.get('total')) else None,
            'long_review_current': long_current, 'annual_gpt_current': annual_gpt,
            'company_support': current and all(by[k]['state'] == '支持' for k in ('fundamental','valuation')),
            'company_adverse': current and any(by[k]['state'] == '反对' for k in ('fundamental','valuation')),
            'domains': rendered,
            'scope': '当前长期GPT证据按原期限引用，不能视为未来12个月必然兑现。' if long_current else '原GPT审核为'+{'short':'短期','medium':'中期','long':'过期/不完整长期'}.get(horizon,'未明确周期')+'，不构成一年GPT结论。'}


def _price(x):
    return f'{x:.2f}' if _finite(x) else '待补'


def _phases(start, regime, metrics, evidence):
    ma = _price(metrics.get('ma200')); high = _price(metrics.get('annual_high')); low = _price(metrics.get('low60'))
    texts = {
      'up': [('趋势延续，先核对回撤能否守住支撑', f'回撤后重回60日均线上方；200日线{ma}及其20日斜率保持向上', '跌破60日结构低点且120/200日均线转弱，转入下行情景'),
             ('强势能否转成持续增长', f'突破或站稳本年度高点{high}参考位，随后回踩不破；同期盈利/现金流披露支持', '价格创新高而盈利、现金流反向，削弱延续判断'),
             ('若前两阶段兑现，观察趋势扩展', '新高与回撤低点同步抬升；估值扩张须有盈利兑现承接', '低点持续下移或重大企业反证出现，取消扩展假设'),
             ('以滚动证据判断延续还是进入收敛', '按新财报及新完整年度窗口重审，保留旧判断履历', '下一周期证据不足则转研究，不自动延长原合同')],
      'up_caution': [('上升结构中的回撤风险，先消化顶部预警', '量能/跌幅预警解除；回撤不再扩大且重新收复60日均线', f'60日结构低点{low}失守且200日线{ma}转弱，转入下行情景'),
             ('回撤修复后才讨论再上行', '重新形成抬高的低点并突破回撤前高；企业证据不恶化', '反弹量价衰减、再破低点，转入震荡或下行'),
             ('修复成功可延续，失败则保守收缩', '均线斜率恢复向上且财报兑现，才保留扩展分支', '技术恢复但基本面反对，维持分歧判断'),
             ('重新核对趋势成熟度与兑现质量', '新年度高低结构和最新财报同时检查', '不把一轮反弹自动外推为全年上涨')],
      'down': [('下行结构延续风险较高，反弹先按修复看', f'先收复60日均线并停止创新低；200日线{ma}暂作结构参照', f'60日低点{low}失守且均线继续下倾，维持下行分支'),
             ('观察能否形成可验证底部', '回落不破前低，再突破反弹前高；120日斜率由负转平', '仅短暂反弹、再创新低，不判趋势已反转'),
             ('底部成立后才研究趋势反转', '重新站上200日线且斜率转正，盈利/现金流反证已处理', '技术转强但估值和企业逻辑仍恶化，不签长期支持'),
             ('区分完成修复和仍在下行通道', '滚动年度结构与最新企业数据共同复核', '修复条件未兑现，则下一年仍以保守研究为基准')],
      'range': [('多空结构未同向，先观察区间选择', f'以60日高低点和200日线{ma}观察，避免用一次穿线判断整年', '多日失守区间下沿且长均线下倾，切换下行情景'),
             ('等待趋势选择得到复核', f'突破年度高点{high}后回踩稳住并有企业证据，转上行分支', '向上突破失败又跌回区间，仍按震荡处理'),
             ('方向明确后才形成扩展判断', '均线方向、关键高低点和新财报共同改善或恶化', '证据分歧持续，延长观察而不外推单边趋势'),
             ('按年度实际路径重新归类', '核对四阶段条件兑现情况，并重算下一完整年度窗口', '旧研判不能因时间过去而自动变为正确')],
      'limited': [('补齐全年及最新完整日线后确定起点', '同票同源OHLCV、复权口径、最近交易日和日历完整性全部通过', '数据仍有缺口，则不生成年度方向结论'),
             ('完成趋势与企业资料交叉', '200/120/60日均线、斜率及同期财报/估值证据可核验', '短期反弹或单项高分不能替代长期证据'),
             ('以新证据建立条件方向', '上涨、区间或下行分支有实证支持后再选主线', '确认条件未成立时保留不确定性'),
             ('完成首次完整年度回看', '检查实际价格路径与原阶段假设，并记录偏差', '不以事后换目标或换周期补成“预测通过”')]}
    return [{'phase': f'{i*3}–{(i+1)*3}个月', 'start': _months(start, i*3).isoformat(),
             'end': _months(start, (i+1)*3).isoformat(), 'outlook': t[0], 'confirmation': t[1], 'invalidation': t[2]}
            for i, t in enumerate(texts[regime])]


def build(context, frame, quality, synthesis=None, now=None):
    """Four future calendar quarters; historical levels are never targets."""
    context = context or {}; quality = quality or {}; now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError('年度研判参考时间必须带时区')
    data, market, gaps, end = _validate(context, frame, quality, now)
    start = now.astimezone(ZoneInfo(MARKETS[market])).date()
    metrics = {}; regime = 'limited'; data_status = 'unavailable'
    if data is not None:
        try:
            metrics = _metrics(data, market, end)
            complete = metrics['annual_complete'] and all(_finite(metrics.get(f'ma{n}_slope20_pct')) for n in (60,120,200))
            if not complete:
                gaps.append(f"自然年窗口不完整或长均线斜率不足：{metrics['annual_bars']}/{metrics['expected_annual_bars']}个应有交易日；缺失{len(metrics['missing_sessions'])}日")
            else:
                last = metrics['last']; up = last > metrics['ma60'] > metrics['ma120'] > metrics['ma200'] and metrics['ma200_slope20_pct'] > 0 and metrics['annual_return_pct'] > 0
                down = last < metrics['ma200'] and metrics['ma60'] < metrics['ma120'] and metrics['ma200_slope20_pct'] < 0
                regime = 'up' if up else 'down' if down else 'range'
            data_status = 'complete' if complete else 'limited'
        except (ValueError, TypeError, KeyError, ImportError, IndexError) as exc:
            gaps.append('年度交易日完整性未通过：'+str(exc)); data_status = 'limited'
    evidence = _evidence(context, synthesis, quality)
    if data_status != 'complete':
        evidence['annual_gpt_current'] = False
    top = bool(data_status == 'complete' and ((metrics.get('return1_pct') or 0) <= -3 and (metrics.get('volume_ratio20') or 0) >= 1.5))
    synthesis_bound = bool(synthesis and _canonical(synthesis.get('code')) == _canonical(context.get('code'))
                           and synthesis.get('snapshot_signature') == quality.get('snapshot_signature')
                           and synthesis.get('source_asof') == quality.get('source_asof'))
    if synthesis_bound and data_status == 'complete':
        top = top or (synthesis.get('turning') or {}).get('side') == 'top' or bool((synthesis.get('turning') or {}).get('mixed'))
    long_up = bool(data_status == 'complete' and metrics['last'] > metrics['ma120']
                   and metrics['ma60'] > metrics['ma120'] > metrics['ma200']
                   and metrics['ma200_slope20_pct'] > 0 and metrics['annual_return_pct'] > 0)
    if top and (regime == 'up' or long_up):
        regime = 'up_caution'
    headlines = {'up':'上升结构占优，未来一年以延续后验证兑现为主线', 'up_caution':'长结构仍向上，未来一年先防回撤、再验修复与延续',
                 'down':'下行结构占优，未来一年先看止跌修复、再谈趋势反转',
                 'range':'方向尚未形成共识，未来一年以区间选择与条件切换为主线',
                 'limited':'年度证据不足，先补全年资料再形成方向研判'}
    thesis = {'up':'现价与60/120/200日均线形成上行排列，200日线斜率和完整年收益同向；这是当前结构对未来条件路径的支持，不能确定上涨持续多久。',
              'up_caution':'长均线与年度结构偏上，但近期转弱信号需要先消化。近3个月以回撤确认和风险复核为重点，不能直接沿用强势标签。',
              'down':'现价处于下倾200日线下方，60日均线弱于120日线。基准路径为反弹修复伴随再探底风险，只有底部结构与企业证据兑现才上调判断。',
              'range':'均线排列、斜率或年度方向未形成同向证据。上行与下行都有条件，先观察区间突破能否持续，再决定后续半年主线。',
              'limited':'当前输入不能证明一整年的完整走势；下面仍保留未来12个月的核验计划，但不输出伪精确方向、概率或一年目标。'}[regime]
    if evidence['company_adverse']:
        thesis += ' 同包基本面或估值存在反对证据，技术走强也不能抵消；年度企业前景保留分歧。'
    elif evidence['company_support']:
        thesis += ' 同包基本面与估值均有支持，引用仅限原审核周期；未来财报仍须持续兑现。'
    else:
        thesis += ' 基本面与估值尚未形成当前完整支持，一年企业价值判断仍有缺口。'
    if not evidence['long_review_current']:
        gaps.append('缺少当前长期GPT与书理完整结论；本模块是条件推演，不继承短期评级')
    annual_high = _price(metrics.get('annual_high')); annual_low = _price(metrics.get('annual_low')); ma200 = _price(metrics.get('ma200'))
    scenarios = [
        {'scenario':'上行情景', 'path':'趋势延续或完成修复后上行', 'confirmation':f'重新形成抬高的高低点，站稳200日线{ma200}并使其斜率向上；年度高点{annual_high}突破须后续回踩确认及财报支持', 'invalidation':'突破失败、低点持续下移或企业/估值反证出现，取消上行分支'},
        {'scenario':'基准情景', 'path':headlines[regime], 'confirmation':'逐季核对下表条件，以实际兑现决定是否延续；缺证不视为通过', 'invalidation':'结构或企业证据变化即切换情景，保留本次判断供以后回看'},
        {'scenario':'下行情景', 'path':'趋势转弱或原有下行继续', 'confirmation':f'200日线{ma200}持续下倾、60日低点失守；年度低点{annual_low}仅作历史压力测试参照', 'invalidation':'停止创新低，收复关键均线并形成更高低点，同时主要企业反证已处理'}]
    if data_status != 'complete':
        for item in scenarios:
            item['path'] = '证据补齐后核验：'+item['path']
            item['confirmation'] = '先完成全年同源数据核验；'+item['confirmation']
    phases = _phases(start, regime, metrics, evidence)
    if top and regime not in ('up_caution', 'limited'):
        phases[0]['outlook'] = '先复核短线转弱预警；'+phases[0]['outlook']
        thesis += ' 当前另有短线转弱预警，第一阶段须先确认风险是否解除。'
    result = {'version': VERSION, 'code': _canonical(context.get('code')), 'market': market,
              'generated_at': now.isoformat(), 'outlook_start': start.isoformat(), 'outlook_end': _months(start,12).isoformat(),
              'source_asof': quality.get('source_asof'), 'source': quality.get('source'),
              'price_basis': quality.get('price_basis'), 'return_basis': '价格收益，非含股息再投资的总回报',
              'snapshot_signature': quality.get('snapshot_signature'),
              'factpack_id': (context.get('selection') or {}).get('factpack_id'),
              'data_status': data_status, 'valid12month': data_status == 'complete', 'regime': regime, 'headline': headlines[regime], 'thesis': thesis,
              'top_warning': top, 'metrics': metrics, 'phases': phases,
              'scenarios': scenarios, 'evidence': evidence, 'gaps': gaps,
              'structure_scope':'所有均线、高低点均为已发生历史结构参考；不是一年目标、买卖区间或对原合同的修改。',
              'method_scope':'V88确定性条件框架；均线顺序、20交易日斜率与放量跌幅预警是工程参数，未宣称经典原文阈值或经验证未来胜率。',
              'no_grade_authority': True, 'entry_permission': False, 'model_calls': 0, 'network_calls': 0}
    result['history'] = _history(frame, data, market, end, quality, metrics, now)
    result['input_id'] = hashlib.sha256(json.dumps({k:v for k,v in result.items() if k != 'generated_at'},sort_keys=True,ensure_ascii=False,allow_nan=False).encode()).hexdigest()
    return result


def _history_html(doc):
    e = lambda x: escape(str(x if x is not None else '待补'))
    h = doc.get('history') or {}; points = h.get('points') or []
    original = h.get('source_range') or {}
    text = '<div class="v88-annual-history"><b>过去一年 · 真实价格与长期均线</b>'
    text += '<div class="v88-annual-caption">一年横轴 '+e(h.get('window_start'))+' — '+e(h.get('window_end'))+'；原始输入 '+e(original.get('start'))+' — '+e(original.get('end'))+'（'+e(original.get('bars'))+'条）</div>'
    if not h.get('annual_complete'):
        text += '<div class="v88-annual-gap">全年资料未齐：已核验显示 '+e(len(points))+' / '+e(h.get('expected_annual_bars'))+' 个应有交易日；保留一年尺度，缺失处留白。'
        if h.get('missing_sessions'):
            text += ' 缺失 '+e(len(h['missing_sessions']))+' 日，含 '+e('、'.join(h['missing_sessions'][:6]))+('…' if len(h['missing_sessions']) > 6 else '')+'。'
        text += '</div>'
    if not points:
        reasons = list(doc.get('gaps') or []) + list(h.get('gaps') or [])
        return text+'<div class="v88-annual-gap">未绘制行情：'+e('；'.join(reasons) or '缺少可核验日线')+'</div></div>'
    # Native SVG uses real calendar distance, not array positions. A one-week
    # fragment occupies one week of this axis, even when the rest is absent.
    left, right, top, bottom = 66., 908., 20., 260.
    start = date.fromisoformat(h['window_start']); end = date.fromisoformat(h['window_end'])
    days = max((end-start).days, 1)
    values = [p[k] for p in points for k in ('close', 'ma60', 'ma120', 'ma200') if _finite(p.get(k))]
    lo, hi = min(values), max(values); margin = (hi-lo)*.08 or hi*.02
    lo = max(0., lo-margin); hi += margin
    x = lambda day: left+(date.fromisoformat(day)-start).days/days*(right-left)
    y = lambda value: bottom-(value-lo)/(hi-lo)*(bottom-top)
    svg = '<svg class="v88-annual-price-chart" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 940 320" role="img" aria-label="过去一年真实收盘价及60、120、200日均线，按真实日期绘制，缺失行情不补齐" style="display:block;width:100%;min-width:760px;background:#fff">'
    svg += '<title>'+e(doc.get('code'))+' 过去一年价格与长期趋势</title><desc>'+e(h['series_basis'])+'</desc>'
    for i in range(5):
        value = lo+(hi-lo)*i/4; py = y(value)
        svg += f'<line x1="{left}" y1="{py:.2f}" x2="{right}" y2="{py:.2f}" stroke="#e2e8f0"/><text x="58" y="{py+4:.2f}" text-anchor="end" font-size="11" fill="#64748b">{value:.2f}</text>'
    month = start.replace(day=1)
    if month < start:
        month = _months(month, 1)
    while month <= end:
        px = x(month.isoformat())
        svg += f'<line x1="{px:.2f}" y1="{top}" x2="{px:.2f}" y2="{bottom}" stroke="#f1f5f9"/><text x="{px:.2f}" y="281" text-anchor="middle" font-size="10" fill="#64748b">{month:%Y-%m}</text>'
        month = _months(month, 1)
    for key, color, width in (('ma200','#9333ea',1.8), ('ma120','#d97706',1.8), ('ma60','#0891b2',1.8), ('close','#2563eb',2.2)):
        segments = []; current = []
        for point in points:
            value = point.get(key)
            if point.get('break_before') or not _finite(value):
                if current:
                    segments.append(current); current = []
            if _finite(value):
                current.append((x(point['date']), y(value)))
        if current:
            segments.append(current)
        for segment in segments:
            if len(segment) == 1:
                px, py = segment[0]
                svg += f'<circle class="v88-annual-series-{key}" cx="{px:.2f}" cy="{py:.2f}" r="2" fill="{color}"/>'
            else:
                path = 'M '+' L '.join(f'{px:.2f} {py:.2f}' for px, py in segment)
                svg += f'<path class="v88-annual-series-{key}" d="{path}" fill="none" stroke="{color}" stroke-width="{width}" stroke-linejoin="round"/>'
    svg += '<text x="66" y="308" font-size="11" fill="#64748b">已发生的价格路径；均线用于历史结构观察，不延伸为未来价格预测</text></svg>'
    text += '<div class="v88-annual-scroll" style="overflow-x:auto">'+svg+'</div>'
    text += '<div class="v88-annual-legend"><span style="color:#2563eb">━ 收盘价</span>　<span style="color:#0891b2">━ MA60</span>　<span style="color:#d97706">━ MA120</span>　<span style="color:#9333ea">━ MA200</span>　最新已核验收盘 '+e(_price(points[-1]['close']))+'</div>'
    if h.get('gaps'):
        text += '<div class="v88-annual-gap">'+e('；'.join(h['gaps']))+'</div>'
    text += '<div class="v88-annual-caption">行情来源 '+e(h.get('source'))+'；口径 '+e(h.get('price_basis'))+'；缺失交易日处断线，均线缺足够前置日线处留空。序列校验 '+e(h.get('series_id', '')[:12])+'，完整序列随年度研判下载。</div></div>'
    return text


def _timeline_html(doc):
    e = lambda x: escape(str(x if x is not None else '待补'))
    text = '<div class="v88-annual-future"><b>未来一年 · 四阶段条件趋势</b><div class="v88-annual-caption">以下是逐阶段核验路径；后续方向取决于条件兑现，无未来价格刻度。</div>'
    text += '<div class="v88-annual-scroll" style="overflow-x:auto"><ol class="v88-annual-timeline" style="display:grid;grid-template-columns:repeat(4,minmax(240px,1fr));min-width:1000px;list-style:none;margin:10px 0;padding:0;border-top:3px solid #93c5fd">'
    for i, phase in enumerate(doc.get('phases') or []):
        text += '<li style="position:relative;padding:10px 12px;border-right:1px solid #e2e8f0"><span style="position:absolute;top:-8px;left:12px;background:#2563eb;border:2px solid white;width:12px;height:12px;border-radius:50%"></span>'
        text += '<b style="color:#1d4ed8">'+e(phase['phase'])+(' →' if i < 3 else '')+'</b><div class="v88-annual-caption">'+e(phase['start'])+' → '+e(phase['end'])+'</div>'
        text += '<div style="font-weight:600;margin:6px 0">'+e(phase['outlook'])+'</div><div>确认：'+e(phase['confirmation'])+'</div></li>'
    return text+'</ol></div></div>'


def html(doc):
    if not doc:
        return '<section class="v88-annual-outlook" id="v88-annual-outlook">年度证据尚未生成。</section>'
    e = lambda x: escape(str(x if x is not None else '未核实'))
    color = {'up':'#15803d','up_caution':'#b45309','down':'#b91c1c','range':'#1d4ed8','limited':'#64748b'}.get(doc.get('regime'),'#64748b')
    icon = {'up':'↗','up_caution':'⚠','down':'↘','range':'↔','limited':'◷'}.get(doc.get('regime'),'◷')
    text = f'<b class="v88-annual-headline" style="color:{color}">{icon} 未来12个月 · {e(doc["headline"])}</b><p style="margin:4px 0">{e(doc["thesis"])}</p>'
    text += f'<div style="font-size:11px;color:#64748b">{e(doc["outlook_start"])} → {e(doc["outlook_end"])} · 行情截至 {e(doc.get("source_asof"))} · 条件推演，不含概率或新目标</div>'
    text += _timeline_html(doc)
    text += '<details class="v88-annual-history-toggle"><summary>过去一年走势与均线 · 本地绘图，按需展开</summary>'+_history_html(doc)+'</details>'
    def table(cls, headers, rows):
        body = '<tr>'+''.join('<th>'+e(x)+'</th>' for x in headers)+'</tr>'
        for row in rows:
            body += '<tr>'+''.join('<td>'+e(x)+'</td>' for x in row)+'</tr>'
        return f'<div class="v88-annual-scroll" style="overflow-x:auto"><table class="{cls}" style="min-width:840px;width:100%;font-size:11px;border-collapse:collapse">{body}</table></div>'
    text += '<details class="v88-annual-phase-details"><summary>四阶段完整条件与失效切换</summary>'
    text += table('v88-annual-phases', ['阶段','主线','确认条件','失效/切换条件'],
                  [(p['phase']+' · '+p['start']+'至'+p['end'],p['outlook'],p['confirmation'],p['invalidation']) for p in doc['phases']])+'</details>'
    text += '<details class="v88-annual-scenario-details"><summary>上行 / 基准 / 下行情景</summary>'
    text += table('v88-annual-scenarios',['情景','可能路径','确认条件','反证'],[(s['scenario'],s['path'],s['confirmation'],s['invalidation']) for s in doc['scenarios']])+'</details>'
    m = doc.get('metrics') or {}; ev = doc.get('evidence') or {}
    text += '<details class="v88-annual-evidence"><summary>全年依据、中央审核关联与缺口</summary>'
    text += '<div>'+e(doc['structure_scope'])+'</div>'
    text += '<div>行情来源 '+e(doc.get('source'))+'；口径 '+e(doc.get('price_basis'))+'；'+e(doc.get('return_basis'))+'</div>'
    text += '<div>自然年窗口 '+e(m.get('window_start'))+'—'+e(m.get('window_end'))+'；完整日线 '+e(m.get('annual_bars'))+'/'+e(m.get('expected_annual_bars'))+'；日历依据 '+e(m.get('calendar_basis'))+'</div>'
    text += '<div>历史年度收益 '+e(_price(m.get('annual_return_pct')))+'%；最大收盘回撤 '+e(_price(m.get('annual_max_drawdown_pct')))+'%；年度高/低 '+e(_price(m.get('annual_high')))+' / '+e(_price(m.get('annual_low')))+'</div>'
    text += '<div>'+e('；'.join(f"MA{n} {_price(m.get(f'ma{n}'))} / 20日斜率 {_price(m.get(f'ma{n}_slope20_pct'))}%" for n in (60,120,200)))+'</div>'
    text += '<div>中央原周期 '+e(ev.get('original_horizon'))+' · 原审核分 '+e(ev.get('central_audit_score'))+' · '+e(ev.get('scope'))+'</div>'
    for d in ev.get('domains',[]):
        text += '<div>'+e(d['title']+'：'+d['state']+'；'+'；'.join(d['reasons']))+'</div>'
    if doc.get('gaps'):
        text += '<div style="color:#b45309">待补：'+e('；'.join(doc['gaps']))+'</div>'
    if (doc.get('history') or {}).get('missing_sessions'):
        text += '<div>全部缺失交易日：'+e('、'.join(doc['history']['missing_sessions']))+'</div>'
    text += '<div>'+e(doc['method_scope'])+'</div></details>'
    text += '<div style="font-size:10px;color:#64748b">年度框架 '+e(doc['input_id'][:12])+' · 本次模型/网络调用0；评级、原进场、止盈、失效线及期限保持原合同。</div>'
    return '<section id="v88-annual-outlook" class="v88-annual-outlook" style="font-size:12px;line-height:1.55;padding:8px;margin:8px 0;border:1px solid #cbd5e1;border-radius:6px"><style>.v88-annual-outlook td,.v88-annual-outlook th{padding:5px;border:1px solid #e2e8f0;vertical-align:top;text-align:left}.v88-annual-outlook th{background:#eff6ff}.v88-annual-outlook summary{font-size:11px;color:#475569;cursor:pointer}.v88-annual-history,.v88-annual-future{margin:12px 0}.v88-annual-caption{font-size:10px;color:#64748b;overflow-wrap:anywhere}.v88-annual-gap{font-size:11px;color:#b45309;margin:4px 0}.v88-annual-scroll{max-width:100%;-webkit-overflow-scrolling:touch}.v88-annual-legend{font-size:11px;flex-wrap:wrap}.v88-annual-timeline li:last-child{border-right:0!important}</style>'+text+'</section>'
