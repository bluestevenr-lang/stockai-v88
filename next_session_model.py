"""Read-only stock -> sector context -> central decision projection.

The view owns no rating, probability, order, or alternative trade contract.
Industry-to-broad-sector mappings are explicit proxies, never constituent claims.
"""
from collections import Counter
from datetime import datetime, timezone, timedelta
import math
from copy import deepcopy
from zoneinfo import ZoneInfo

VERSION = 'next-session-linked-observations-v1'
BJT = timezone(timedelta(hours=8))
MARKETS = ('A股', '港股', '美股')
# Only declared taxonomy mappings. Unknown/high-dividend themes cannot be inferred
# from an issuer name, a price move, or membership in the same market.
PROXIES = {
    'A股': {'军工电子Ⅱ': '军工', '地面兵装Ⅱ': '军工', '航空装备Ⅱ': '军工',
            '航天装备Ⅱ': '军工', '半导体': '半导体芯片', '煤炭开采': '煤炭',
            '焦炭Ⅱ': '煤炭', '国有大型银行Ⅱ': '银行', '股份制银行Ⅱ': '银行',
            '城商行Ⅱ': '银行', '农商行Ⅱ': '银行'},
    '美股': {'半导体产品': '科技', '半导体材料与设备': '科技', '系统软件': '科技',
            '应用软件': '科技', '信息科技咨询与其它服务': '科技', '通信设备': '科技',
            '电影与娱乐': '通信', '互动媒体与服务': '通信', '综合性石油与天然气企业': '能源',
            '石油与天然气的勘探与生产': '能源', '多元化银行': '金融', '铁路': '工业',
            '工业气体': '工业', '电力公用事业': '公用事业'},
    '港股': {},
}


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def _market(code):
    return '港股' if code.endswith('.HK') else ('A股' if code.endswith(('.SS', '.SZ', '.BJ')) else '美股')


def sector_context(code, direction, profiles, rotation, now):
    from stock_profile_view import profile
    from exchange_sessions import latest_completed
    p = profile(code, profiles, now)
    market = _market(code)
    industry = p.get('industry') or '行业待核'
    missing = {'industry': industry, 'sector_state': 'missing', 'sector_label': '板块待关联',
               'sector_detail': '未取得同市场、对应行业的板块证据；不能解释为没有风险或没有机会。'}
    if not p.get('profile_fresh') or p.get('market') != market:
        return {**missing, 'sector_detail': '行业资料缺失或过期，等待更新后关联。'}
    candidates = [r for r in (rotation.get('trajectories') or {}).get(market, []) if isinstance(r, dict)]
    matched = [r for r in candidates if r.get('name') == industry]
    proxy = False
    if not matched:
        target = PROXIES.get(market, {}).get(industry)
        matched = [r for r in candidates if target and r.get('name') == target]
        proxy = bool(matched)
    if len(matched) != 1:
        return missing
    row = matched[0]
    try:
        expected = latest_completed(market, now).isoformat()
        dates = (rotation.get('source_dates_by_market') or {}).get(market) or []
        fresh = bool(dates) and all(str(day)[:10] == expected for day in dates)
        at = datetime.fromisoformat(str(rotation.get('analysis_time')))
        if at.tzinfo is None: at = at.replace(tzinfo=BJT)
        fresh = fresh and 0 <= (now-at).total_seconds() <= 86400
    except (ValueError, TypeError):
        fresh = False
    label = str(row.get('name')) + ('代理' if proxy else '')
    if not fresh:
        return {**missing, 'sector_label': label+' · 日期待更新', 'sector_detail': '板块行情日期与最近完整交易日未一致。'}
    facts = row.get('facts') or {}
    week, month = facts.get('5d'), facts.get('20d')
    if not (finite(week) and finite(month)):
        return {**missing, 'sector_label': label+' · 量价待补', 'sector_detail': '缺少5日或20日原始涨跌幅，不能判断板块配合。'}
    trend = 'up' if week > 0 and month > 0 else 'down' if week < 0 and month < 0 else 'mixed'
    state = 'mixed' if trend == 'mixed' or direction == 'mixed' else ('aligned' if trend == direction else 'opposed')
    state_text = {'aligned': '同向', 'opposed': '反向', 'mixed': '周月分歧'}[state]
    detail = f"{industry} → {label}；截至{expected}，5日{week:+.2f}% / 20日{month:+.2f}%。"
    if proxy: detail += '行业映射到宽板块作环境参考，不代表已核实成分股或个股必随板块。'
    detail += '拐点时点尚未验证。'
    return {'industry': industry, 'sector_state': state, 'sector_label': label+' · 周'+('↑' if week>0 else '↓' if week<0 else '→')+' 月'+('↑' if month>0 else '↓' if month<0 else '→'),
            'sector_detail': detail, 'sector_source_date': expected, 'sector_proxy': proxy,
            'sector_source_id': rotation.get('snapshot_id'), 'sector_observed': {'5d': week, '20d': month}}


def build(signals, rotation, *, central, profiles, now=None):
    from grade_focus import canonical
    from stock_profile_view import profile
    from exchange_sessions import next_session, latest_completed, is_session
    now = now or datetime.now(BJT)
    if now.tzinfo is None: now = now.replace(tzinfo=BJT)
    central = central or {}
    try:
        stamp = datetime.fromisoformat(str(central['source_generated_at']).replace('（北京时间）', '').strip())
        if stamp.tzinfo is None: stamp = stamp.replace(tzinfo=BJT)
        usable = bool(central.get('factpack_id')) and 0 <= (now-stamp).total_seconds() <= 86400
    except (KeyError, ValueError, TypeError):
        usable = False
    indexed = {canonical(r.get('code')): r for r in central.get('rows', [])
               if isinstance(r, dict) and r.get('tier') in ('1A','2A','3A')} if usable else {}
    grouped = {market: [] for market in MARKETS}
    seen = set()
    for source in signals.get('stocks') or []:
        code = canonical(source.get('code'))
        if not code or code in seen: continue
        seen.add(code)
        market = _market(code)
        current_source = source.get('source_status') == 'verified'
        try: current_source = current_source and source.get('source_date') == latest_completed(market, now).isoformat()
        except ValueError: current_source = False
        direction = source.get('direction') if current_source and source.get('direction') in ('up','down') else 'mixed'
        row = {'code': code, 'name': source.get('name') or code, 'direction': direction,
               'phase': source.get('phase') if current_source else '原信号待行情复核',
               'rule_score': source.get('strength') if current_source and finite(source.get('strength')) else None,
               'source_date': source.get('source_date') or '未核实',
               'source_status': 'verified' if current_source else 'missing',
               'trigger': source.get('trigger') if current_source else '补齐最近完整交易日的行情后重算',
               'invalid': source.get('invalid') if current_source else '旧信号不继续作为当前方向',
               'source_signature': source.get('snapshot_signature')}
        row.update(sector_context(code, direction, profiles, rotation or {}, now))
        authority = indexed.get(code) or {}
        opportunity = authority.get('entry_opportunity') or {}
        tier = authority.get('tier')
        row.update(central_tier=tier, central_current=bool(authority), central_score=authority.get('audit_score'),
                   central_ref=deepcopy(authority.get('master_ref')), central_plan=deepcopy(authority.get('trade_plan') or {}))
        # A risk conflict is escalated even if the central original contract is
        # still present. This observation never overwrites or grants that permit.
        if not authority:
            row['central_action'] = '未获当前评级' if usable else '中央版本待核'
        elif tier == '3A' and authority.get('formal_recommendation') and opportunity.get('executable'):
            row['central_action'] = '中央有入场条件 · 需核对本栏风险' if direction == 'down' else '按中央原入场条件复核'
        else:
            row['central_action'] = tier+'研究 · 原条件待满足'
        if not current_source:
            action = '先补当期行情，暂不沿用旧方向。'
        elif direction == 'down':
            action = '先核对持仓原失效条件；技术转弱进入风险复核。'
        elif direction == 'mixed':
            action = '原切换信号已变化，查看深度分析重新判断。'
        elif row['sector_state'] == 'opposed':
            action = '个股与板块反向，等待分歧解除及中央条件确认。'
        elif row['sector_state'] == 'missing':
            action = '板块证据待补，先观察个股触发条件。'
        elif row['sector_state'] == 'mixed':
            action = '板块周月不一致，等待同向确认及中央条件。'
        elif not authority:
            action = '量价与板块同向，仍需完成中央复审。'
        else:
            action = '观察个股触发，并核对中央原入场条件。'
        row['next_action'] = action
        row['priority'] = 0 if authority else 1
        grouped[market].append(row)
    markets = []
    for market in MARKETS:
        rows = sorted(grouped[market], key=lambda r: (r['priority'], -(r['rule_score'] if finite(r['rule_score']) else -1), r['code']))
        try:
            local = now.astimezone(ZoneInfo('America/New_York' if market == '美股' else 'Asia/Shanghai'))
            from exchange_sessions import session_view
            timing=session_view(market,now)
            next_day=timing['attention_session']
            source_day = latest_completed(market, now).isoformat()
        except ValueError:
            next_day, source_day = '交易日待核', '行情日待核'
            timing={}
        markets.append({'market': market, 'next_session': next_day, 'source_date': source_day,
                        'market_status':timing.get('market_status','待核'), 'rows': rows, 'counts': dict(Counter(r['direction'] for r in rows))})
    return {'version': VERSION, 'generated_at': signals.get('generated_at'),
            'verified_at': signals.get('verified_at') or signals.get('snapshot_at'), 'pool_size': signals.get('scanned',0),
            'signal_count': len(seen), 'verified_count': sum(r['source_status']=='verified' for m in markets for r in m['rows']),
            'central_version': central.get('factpack_id') if usable else '待核', 'markets': markets,
            'notes': list(signals.get('notes') or []) + [
                '本页复核已有发现队列，不代表全市场扫描；优先显示中央已有评级的标的，同组按规则强度排序。',
                '个股规则强度、板块历史动量、中央审核分分别列示；不相加、不代替彼此。',
                '下一交易日是观察检查点，不是承诺转折日；入场与退出继续引用中央原合同。'],
            'model_calls': 0, 'network_calls': 0, 'entry_permission': False}
