"""Compact, read-only presentation shared by web and deep stock views."""
from html import escape
from datetime import date, datetime, timezone
from strategy_multicheck import LABELS, VERSION


def esc(value):return escape(str(value if value is not None else '待核'),quote=True)
def num(value):return f'{value:.2f}' if isinstance(value,(int,float)) else '待核'
DIRECTION={'up':'上行','down':'下行','flat':'走平','unknown':'待核'}


def _freshness(row, data, now):
    """Project cache age at read time; never rewrite its original signed facts."""
    from exchange_sessions import latest_completed
    try:
        expected = latest_completed(row.get('market'), now).isoformat()
    except (ValueError, TypeError, AttributeError):
        return 'pending', None, None
    alignment = data.get('alignment') or {}
    technical = data.get('technical') or {}
    dates = [data.get('asof'), alignment.get('expected_session'),
             technical.get('source_asof') or technical.get('asof')]
    dates += [(alignment.get(key) or {}).get('source_asof') for key in
              ('market_context', 'sector_context', 'stock_context')]
    valid = []
    for value in dates:
        try:
            valid.append(date.fromisoformat(str(value)[:10]).isoformat())
        except (ValueError, TypeError):
            pass
    if any(value < expected for value in valid):
        return 'historical', min(valid), expected
    if len(valid) != len(dates) or any(value != expected for value in valid):
        return 'pending', data.get('asof'), expected
    return 'current', data.get('asof'), expected


def html(row, now=None):
    d=row.get('crosscheck') or {}
    if d.get('version')!=VERSION:return '<small>多重核查：待本轮数据更新</small>'
    freshness, observed, expected = _freshness(row, d, now or datetime.now(timezone.utc))
    a=d.get('alignment') or {};t=d.get('technical') or {};g=t.get('gmma') or {}
    long=(g.get('slopes') or {}).get('long') or {};short=(g.get('slopes') or {}).get('short') or {}
    patterns=(t.get('candlesticks') or {}).get('patterns') or []
    pattern_text='；'.join(str(p.get('pattern'))+' '+str(p.get('date'))+' '+{
        'CONFIRMED':'下一完整日已确认','PENDING_CONFIRMATION':'等下一完整日确认',
        'NOT_CONFIRMED':'未获确认','CONTEXT_MISSING':'前置趋势/位置不符'}.get(p.get('status'),'待核') for p in patterns[-3:])
    pv=t.get('price_volume') or {};s=t.get('structure') or {};f=t.get('graham') or {};stop=d.get('stop_check') or {}
    structure={'higher_highs_higher_lows':'高低点均抬高','lower_highs_lower_lows':'高低点均下移',
               'mixed_or_range':'结构震荡/分歧','insufficient_confirmed_pivots':'已确认拐点不足'}
    desc=[('顾比趋势线', '短组'+DIRECTION.get(short.get('direction'),'待核')+' / 长组'+DIRECTION.get(long.get('direction'),'待核')+'；'+{
        'short_above_long':'短组位于长组上方','short_below_long':'短组位于长组下方','overlapping':'两组缠绕'}.get(g.get('group_relationship'),str(g.get('reason') or '完整日线待补'))),
        ('日本蜡烛图',pattern_text or ('最近两根完整K线无所测形态' if (t.get('candlesticks') or {}).get('status')=='OBSERVED' else str((t.get('candlesticks') or {}).get('reason') or '形态待核'))),
        ('专业投机·结构与风险',structure.get(s.get('trend'),'结构待核')+'；ATR14 '+num(s.get('atr14'))+'；原止损距离 '+num(stop.get('distance_atr'))+' ATR；'+('锚有已确认低点' if stop.get('anchor_status')=='confirmed' else '止损锚待核')),
        ('聪明的投资者·价值基础','经常性利润/现金流/估值 '+str(f.get('status','MISSING'))+'；缺项 '+','.join(f.get('missing') or [])+'；安全边际尚未证实'),
        ('量价关系','末日/前20日均量 '+num(pv.get('volume_ratio20'))+'；'+{'price_up_volume_down':'价升量降','price_down_volume_up':'价跌量增','not_observed':'未见所测背离','unknown':'量能待核'}.get(pv.get('divergence'),'量能待核'))]
    heading=LABELS.get(a.get('state'),LABELS['unknown'])
    if a.get('state')=='aligned' and (a.get('stock_context') or {}).get('direction')=='down':heading='三层同向下行 · 先核风险'
    original_heading = heading
    if freshness == 'historical':
        heading='历史核查 · 截至 '+str(observed)+' · 当前待更新'
    elif freshness != 'current':
        heading='当前核查待更新 · 完整交易日证据待核'
    out=['<div style="margin-top:6px;font-size:12px;color:#475569"><b>'+esc(heading)+'</b>',
         '<details><summary>'+('历史版本 · ' if freshness != 'current' else '')+'五类依据 · 大盘→板块→个股</summary><div>完整日线 '+esc(d.get('asof'))+' · '+esc(d.get('provenance',{}).get('source'))+'</div>']
    if freshness != 'current':
        out.append('<p>原版结论：'+esc(original_heading)+'；仅保留当时依据，不代表当前核查。最近完整交易日 '+esc(expected)+'</p>')
    for title,text in desc:out.append('<p style="margin:4px 0"><b>'+esc(title)+'：</b>'+esc(text)+'</p>')
    for key,label in [('market_context','大盘'),('sector_context','板块'),('stock_context','个股')]:
        c=a.get(key) or {}
        out.append('<p style="margin:4px 0">'+label+'：'+esc(c.get('direction') or c.get('absolute_direction') or c.get('status'))+' · '+esc(c.get('source_asof'))+'</p>')
    out.append('<div>'+esc('；'.join(a.get('reasons') or []))+'</div>')
    ex=a.get('exception') or {}
    if a.get('state')=='countertrend':out.append('<b>逆势催化：'+esc('有原文待复核，未自动认可' if ex.get('status')=='documented_for_review' else '未取得可追溯证据')+'</b>')
    for evidence in ex.get('evidence') or []:
        out.append('<div>'+esc(evidence)+'</div>')
    out.append('<p>GitHub云端规则复核 · '+('该版' if freshness != 'current' else '本轮')+'0次模型调用 · 算法版本 '+esc(d.get('version'))+'</p>')
    out.append('<small>技术指标同属一份行情证据，不重复计票；阈值为工程规则，非原书承诺。缺证不视为通过，原止损与计划不随分析移动。</small></details></div>')
    return ''.join(out)
