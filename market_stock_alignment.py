"""Dated market → sector → stock context, without score or trading authority.

The adapter consumes cached facts. It neither fetches data nor certifies the
economic interpretation of a source. A documented catalyst remains a reason to
review an exception, never an override of a risk plan or a buy permission.
"""
from datetime import date, datetime, timezone
import math
import re
from urllib.parse import urlsplit

from exchange_sessions import latest_completed
from market_symbols import canonical
from sector_rotation import VERSION as ROTATION_VERSION, BENCHMARKS, PROXIES


VERSION = 'market-stock-alignment-v1'
MAPPING_VERSION = 'explicit-industry-proxy-v1'
CATALYST_MAX_AGE_DAYS = 30
SEARCH_MAX_AGE_HOURS = 30

# Exact taxonomy labels only. These are environment proxies, not a claim that
# the individual security belongs to the ETF, or that the ETF covers an industry.
# In particular: CN oil/chemicals are not coal, US industrial gases are materials,
# and HK banks/utilities are not inferred to be SOEs/high-dividend constituents.
INDUSTRY_PROXIES = {
    'A股': {
        '银行': '512800.SS', '银行Ⅱ': '512800.SS', '国有大型银行Ⅱ': '512800.SS',
        '股份制银行Ⅱ': '512800.SS', '城商行Ⅱ': '512800.SS', '农商行Ⅱ': '512800.SS',
        '证券': '512880.SS', '证券Ⅱ': '512880.SS', '半导体': '512760.SS',
        '半导体芯片': '512760.SS', '煤炭': '515220.SS', '煤炭开采': '515220.SS',
        '焦炭Ⅱ': '515220.SS', '白酒': '512690.SS', '白酒Ⅱ': '512690.SS',
        '医药': '512010.SS', '化学制药': '512010.SS', '生物制品': '512010.SS',
        '医疗器械': '512010.SS', '中药Ⅱ': '512010.SS', '有色金属': '512400.SS',
        '工业金属': '512400.SS', '贵金属': '512400.SS', '小金属': '512400.SS',
        '军工': '512660.SS', '军工电子Ⅱ': '512660.SS', '地面兵装Ⅱ': '512660.SS',
        '航空装备Ⅱ': '512660.SS', '航天装备Ⅱ': '512660.SS',
        '新能源车': '515030.SS', '消费': '159928.SZ',
    },
    '港股': {
        '恒生科技': '3033.HK', '软件服务': '3033.HK', '互联网服务': '3033.HK',
        '资讯科技器材': '3033.HK', '半导体': '3033.HK',
    },
    '美股': {
        '科技': 'XLK', 'Technology': 'XLK', '半导体产品': 'XLK',
        '半导体材料与设备': 'XLK', '系统软件': 'XLK', '应用软件': 'XLK',
        '信息科技咨询与其它服务': 'XLK', '通信设备': 'XLK', 'Semiconductors': 'XLK',
        '通信': 'XLC', 'Communication Services': 'XLC', '电影与娱乐': 'XLC',
        '互动媒体与服务': 'XLC', '可选消费': 'XLY', 'Consumer Cyclical': 'XLY',
        '必选消费': 'XLP', 'Consumer Defensive': 'XLP', '金融': 'XLF',
        'Financial Services': 'XLF', '多元化银行': 'XLF', '区域性银行': 'XLF',
        '医疗': 'XLV', 'Healthcare': 'XLV', '制药': 'XLV', '生物科技': 'XLV',
        '保健护理产品经销商': 'XLV', '保健护理设施': 'XLV',
        '能源': 'XLE', 'Energy': 'XLE', '综合性石油与天然气企业': 'XLE',
        '石油与天然气的勘探与生产': 'XLE', '工业': 'XLI', 'Industrials': 'XLI',
        '铁路': 'XLI', '材料': 'XLB', 'Basic Materials': 'XLB', '工业气体': 'XLB',
        '公用事业': 'XLU', 'Utilities': 'XLU', '电力公用事业': 'XLU',
        '独立电力生产商与能源贸易商': 'XLU',
        '房地产': 'XLRE', 'Real Estate': 'XLRE',
    },
}


def _dict(value):
    return value if isinstance(value, dict) else {}


def _rows(value):
    return [r for r in value if isinstance(r, dict)] if isinstance(value, list) else []


def _finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def _day(value):
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except (ValueError, TypeError):
        return None


def _moment(value):
    try:
        at = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return at.astimezone(timezone.utc) if at.tzinfo else None
    except (ValueError, TypeError):
        return None


def _market(code):
    if re.fullmatch(r'\d{6}\.(SS|SZ|BJ)', code): return 'A股'
    if re.fullmatch(r'\d+\.HK', code): return '港股'
    if re.fullmatch(r'[A-Z][A-Z0-9._$-]*', code): return '美股'
    return None


def _url(value):
    if not isinstance(value, str): return None
    try:
        p = urlsplit(value)
        return value if p.scheme in ('https', 'http') and p.hostname and not p.username and not p.password else None
    except ValueError:
        return None


def _ref(level, facts, path, asof, *, family='market_price_facts'):
    provenance = _dict(facts.get('provenance'))
    return {'level': level, 'path': path, 'source_asof': asof,
            'source_sha256': facts.get('source_sha256') or facts.get('raw_sha256') or provenance.get('source_sha256'),
            'input_sha256': provenance.get('input_sha256'), 'provider_source': provenance.get('source'),
            'source_url': _url(facts.get('source_url')) or _url(provenance.get('source')), 'source_family': family}


def _context(status='missing', reason='当前证据不足', **fields):
    return {'status': status, 'direction': 'unknown', 'reason': reason, 'source_asof': None,
            'method': None, 'source_refs': [], **fields}


def _window_direction(five, twenty):
    if not (_finite(five) and _finite(twenty)): return 'unknown'
    if five > 0 and twenty > 0: return 'up'
    if five < 0 and twenty < 0: return 'down'
    return 'mixed'


def _market_context(snapshot, market, expected):
    symbol, name = BENCHMARKS[market]
    block = _dict(_dict(snapshot.get('markets')).get(market))
    indices = [r for r in _rows(block.get('indices')) if r.get('symbol') == symbol]
    out = _context(benchmark_symbol=symbol, benchmark_name=name)
    if len(indices) != 1:
        return dict(out, reason='固定基准缺失或重复，不能借用其他指数')
    r = indices[0]; asof = _day(r.get('source_asof'))
    out.update(source_asof=asof, source_refs=[_ref('market', r, f'market_snapshot.markets.{market}.indices[{symbol}]', asof)])
    if not expected or asof != expected:
        return dict(out, status='stale' if asof and expected and asof < expected else 'invalid', reason='大盘行情未匹配最近完整交易日')
    if _finite(r.get('last')) and r['last'] > 0 and _finite(r.get('ma60')) and r['ma60'] > 0:
        value = r['last']/r['ma60']-1; method = 'price_vs_ma60'
    elif _finite(r.get('last')) and r['last'] > 0 and _finite(r.get('vs_ma60')):
        value = r['vs_ma60']; method = 'reported_price_vs_ma60'
    else:
        direction = _window_direction(r.get('chg5d'), r.get('chg20d'))
        return dict(out, status='current' if direction != 'unknown' else 'missing', direction=direction,
                    method='window_5d20d_only', reason='仅5/20日价格窗口方向；MA60依据缺失，不称中期趋势确认',
                    change5_pct=r.get('chg5d'), change20_pct=r.get('chg20d'))
    return dict(out, status='current', direction='up' if value > 0 else 'down' if value < 0 else 'mixed',
                method=method, reason='价格相对MA60位置；不是独立买入许可', last=r.get('last'),
                ma60=r.get('ma60'), vs_ma60_pct=r.get('vs_ma60'),
                change5_pct=r.get('chg5d'), change20_pct=r.get('chg20d'))


def _sector_context(row, rotation, market, expected):
    profile = _dict(row.get('industry_snapshot'))
    industry = profile.get('industry')
    symbol = INDUSTRY_PROXIES.get(market, {}).get(industry)
    names = dict(PROXIES[market])
    mapping = {'version': MAPPING_VERSION, 'industry': industry, 'proxy_symbol': symbol,
               'proxy_name': names.get(symbol), 'method': 'exact_taxonomy_label',
               'profile_source_asof': profile.get('profile_captured_at') or profile.get('source_session'),
               'limitation': ('恒生科技是宽风格ETF环境参考，不证明成分股资格或完整行业覆盖' if market == '港股'
                              else '固定ETF是宽板块代理，不证明成分股资格或个股必随板块')}
    out = _context(industry=industry, mapping=mapping, absolute_direction='unknown', relative_state=None)
    if (profile.get('market') and profile['market'] != market) or (profile.get('code') and canonical(profile['code']) != canonical(row.get('code'))):
        return dict(out, status='invalid', reason='行业身份与个股不一致')
    if profile.get('profile_fresh') is False:
        return dict(out, status='stale', reason='行业身份资料已明确过期，暂不关联代理')
    if not symbol:
        return dict(out, reason='该精确行业暂无已配置代表代理；不猜测板块归属')
    doc = _dict(rotation.get('relative_strength')) if 'relative_strength' in rotation else rotation
    if doc.get('version') != ROTATION_VERSION:
        return dict(out, status='invalid', reason='轮动版本不兼容，旧条件分不能冒充相对强弱')
    block = _dict(_dict(doc.get('markets')).get(market)); baseline = BENCHMARKS[market][0]
    if block.get('market') != market or _dict(block.get('benchmark')).get('symbol') != baseline:
        return dict(out, status='invalid', reason='板块市场或固定基准不一致')
    matches = [r for r in _rows(block.get('rows')) if r.get('symbol') == symbol]
    if len(matches) != 1:
        return dict(out, reason='代表代理缺失或重复')
    r = matches[0]; asof = _day(r.get('source_asof'))
    out.update(source_asof=asof, source_refs=[_ref('sector', r, f'rotation.relative_strength.markets.{market}.rows[{symbol}]', asof)])
    if r.get('market') != market or r.get('benchmark_symbol') != baseline:
        return dict(out, status='invalid', reason='代理跨市场或基准身份不一致')
    if r.get('status') != 'current' or not expected or asof != expected or _day(r.get('benchmark_asof')) != expected:
        return dict(out, status='stale' if asof and expected and asof <= expected else 'invalid', reason='板块/基准不是当前同日可比行情')
    fields = ('sector_chg5d', 'sector_chg20d', 'benchmark_chg5d', 'benchmark_chg20d', 'rs5_pp', 'rs20_pp')
    if not all(_finite(r.get(k)) for k in fields) or r.get('basis') not in ('aligned_daily_closes', 'same_session_summary'):
        return dict(out, reason='板块绝对涨跌、超额表现或同窗口径缺失')
    if any(abs(r[f'rs{n}_pp']-(r[f'sector_chg{n}d']-r[f'benchmark_chg{n}d'])) > 1e-5 for n in (5,20)):
        return dict(out, status='invalid', reason='板块相对表现与原始涨幅不一致')
    short, medium = r['rs5_pp'], r['rs20_pp']
    state = '中性' if short == 0 or medium == 0 else ('持续占优' if short > 0 else '短期转弱') if medium > 0 else ('短期改善' if short > 0 else '持续落后')
    if r.get('quadrant') != state:
        return dict(out, status='invalid', reason='板块相对状态与原始超额方向不一致')
    absolute = _window_direction(r['sector_chg5d'], r['sector_chg20d'])
    return dict(out, status='current', direction=absolute, absolute_direction=absolute, relative_state=state,
                method=r['basis'], reason='绝对涨跌和相对基准表现分开；占优可能只是抗跌',
                **{k:r[k] for k in fields})


def _stock_context(row, technical, market, expected):
    supplied = technical is not None
    facts = _dict(technical) if supplied else _dict(row.get('structure'))
    annual = _dict(facts.get('annual')) if facts else _dict(row.get('annual_metrics'))
    regime = annual.get('regime') or (row.get('annual_regime') if not supplied else None)
    asof = _day(facts.get('source_asof') or facts.get('asof') or annual.get('asof'))
    out = _context(source_asof=asof,
        source_refs=[_ref('stock', facts, f'strategy[{canonical(row.get("code"))}].technical', asof)])
    out['source_refs'][0]['context_receipt_sha256'] = row.get('source_sha256')
    if (facts.get('code') and canonical(facts['code']) != canonical(row.get('code'))) or (facts.get('market') and facts['market'] != market):
        return dict(out, status='invalid', reason='个股技术事实身份不一致')
    if not expected or asof != expected:
        return dict(out, status='stale' if asof and expected and asof < expected else 'invalid', reason='个股技术事实未到最近完整交易日；报价时间不能代替结构时间')
    if supplied and ('gmma' in facts or str(facts.get('version') or '').startswith('classics-evidence')):
        from classics_evidence import VERSION as CLASSICS_VERSION
        if facts.get('version') != CLASSICS_VERSION or facts.get('status') != 'OBSERVED':
            return dict(out, status='invalid' if facts.get('version') != CLASSICS_VERSION else 'missing',
                        method='classics_cached_geometry', reason='经典技术证据版本或完成状态未通过')
        gmma, structure = _dict(facts.get('gmma')), _dict(facts.get('structure'))
        slope = _dict(_dict(gmma.get('slopes')).get('long')).get('direction')
        relationship, trend = gmma.get('group_relationship'), structure.get('trend')
        if (gmma.get('status') != 'OBSERVED' or structure.get('status') != 'OBSERVED'
                or slope not in ('up','down','flat')
                or relationship not in ('short_above_long','short_below_long','overlapping')
                or trend not in ('higher_highs_higher_lows','lower_highs_lower_lows','mixed_or_range')):
            return dict(out, method='classics_cached_geometry', reason='GMMA或已确认高低点结构不足，不借旧年度方向补齐')
        up = slope == 'up' and relationship == 'short_above_long' and trend == 'higher_highs_higher_lows'
        down = slope == 'down' and relationship == 'short_below_long' and trend == 'lower_highs_lower_lows'
        return dict(out, status='current', direction='up' if up else 'down' if down else 'mixed',
                    method='classics_cached_geometry', long_group_slope=slope, group_relationship=relationship,
                    structure_trend=trend, basis=_dict(facts.get('provenance')).get('basis'),
                    reason='GMMA长期组斜率、组间位置与已确认高低点合看；同源视角不独立投票')
    direction = {'up':'up', 'down':'down', 'up_caution':'mixed', 'range':'mixed'}.get(regime)
    method = 'dated_annual_structure'
    if supplied and facts.get('direction') in ('up','down','mixed'):
        direction, method = facts['direction'], 'supplied_dated_technical'
    if not direction:
        return dict(out, reason='技术方向缺失或年度结构证据不足')
    return dict(out, status='current', direction=direction, method=method, regime=regime,
                basis=facts.get('basis') or annual.get('basis') or row.get('structure_source'),
                reason='有原始日期的技术方向；技术分与公司价值分不混合')


def _catalyst(row, now, required):
    n = _dict(row.get('nontechnical'))
    result = {'status':'not_needed' if not required else 'missing', 'evidence':[], 'rejected':[],
              'requirements':{'kind':'primary_catalyst', 'max_publication_age_days':CATALYST_MAX_AGE_DAYS,
                              'max_search_age_hours':SEARCH_MAX_AGE_HOURS},
              'authority':'documented_exception_for_review_only'}
    if not required: return result
    search = _moment(n.get('search_checked_at'))
    identity = bool(n.get('code') and canonical(n['code']) == canonical(row.get('code')))
    seen = {}; counters = []
    for item in _rows(n.get('evidence')):
        why = None; claim = item.get('claim'); sha = item.get('source_sha256')
        published = _day(item.get('published_at')); available = _moment(item.get('available_at'))
        asof = _day(item.get('asof')); url = _url(item.get('source_url'))
        if item.get('kind') != 'primary_catalyst': why = '普通成长、估值或经营说明不是独立事件催化'
        elif not identity or (item.get('code') and canonical(item['code']) != canonical(row.get('code'))): why = '公司证据身份未绑定'
        elif not search or not 0 <= (now-search).total_seconds() <= SEARCH_MAX_AGE_HOURS*3600 or n.get('search_fresh') is not True: why = '公司最新公告核查时间失效'
        elif not published or not asof or asof > published or not 0 <= (now.date()-date.fromisoformat(asof)).days <= CATALYST_MAX_AGE_DAYS or not 0 <= (now.date()-date.fromisoformat(published)).days <= CATALYST_MAX_AGE_DAYS: why = '催化事件日期过期、未来或缺失'
        elif not available or available > now or date.fromisoformat(published) > available.date() or search < available: why = '公开与可获取时间未核验'
        elif len(str(item.get('published_at'))) > 10 and (not _moment(item['published_at']) or _moment(item['published_at']) > available): why = '公告发布时点晚于可获取时点'
        elif (not url or not isinstance(sha,str) or not re.fullmatch(r'[0-9a-f]{64}',sha)
              or item.get('verification') != 'primary_document_read_and_capture_rechecked'
              or not isinstance(item.get('source_excerpt'), str) or len(item['source_excerpt']) < 8): why = '缺少可追溯原文与已读核查凭据'
        elif not isinstance(claim,str) or len(claim.strip()) < 8 or item.get('current') is not True: why = '事件实质说明或当前证据不足'
        elif item.get('direction') not in ('support','counterevidence'): why = '事件方向尚未确认'
        if why:
            result['rejected'].append({'kind':item.get('kind'), 'reason':why, 'source_url':url})
            continue
        if item['direction'] in seen.get(sha,set()):
            result['rejected'].append({'kind':item.get('kind'), 'reason':'重复原件不算第二份证据', 'source_url':url})
            continue
        seen.setdefault(sha,set()).add(item['direction'])
        evidence = {k:item.get(k) for k in ('claim','kind','direction','source_url','source_sha256','source_excerpt','asof','published_at','available_at','verification')}
        evidence.update(source_family='issuer_primary_document', identity_code=canonical(row.get('code')))
        if item['direction'] == 'counterevidence': counters.append(evidence)
        else: result['evidence'].append(evidence)
    result['counterevidence'] = counters
    if counters: result['status'] = 'conflicting'
    elif result['evidence']: result['status'] = 'documented_for_review'
    return result


def evaluate(row, market_snapshot, rotation_doc, technical=None, now=None):
    """Project cached context without mutating inputs, scores, ranks or risk plans."""
    now = now or datetime.now(timezone.utc)
    row, snapshot, rotation = _dict(row), _dict(market_snapshot), _dict(rotation_doc)
    code, market = canonical(row.get('code') or ''), row.get('market')
    expected, invalid = None, None
    try:
        if market not in BENCHMARKS or _market(code) != market: raise ValueError('证券代码与市场身份不一致')
        expected = latest_completed(market, now).isoformat()
    except (ValueError, TypeError, AttributeError) as exc:
        invalid = str(exc)
    if invalid:
        contexts = [_context(status='invalid', reason='市场、证券或交易日历未核验') for _ in range(3)]
    else:
        contexts = [_market_context(snapshot, market, expected), _sector_context(row, rotation, market, expected),
                    _stock_context(row, technical, market, expected)]
    m, s, t = contexts
    conflict_levels = [level for level, c in (('market',m),('sector',s))
                       if c['status'] == 'current' and c['direction'] == 'down' and t['status'] == 'current' and t['direction'] == 'up']
    missing = any(c['status'] != 'current' or c['direction'] == 'unknown' for c in contexts)
    if missing: state = 'unknown'
    elif conflict_levels: state = 'countertrend'
    elif len({c['direction'] for c in contexts}) == 1 and t['direction'] in ('up','down'):
        state = 'aligned'
        if (t['direction'] == 'up' and s.get('relative_state') not in ('持续占优', '短期改善')) or (t['direction'] == 'down' and s.get('relative_state') not in ('持续落后', '短期转弱')):
            state = 'mixed'
    else: state = 'mixed'
    exception = _catalyst(row, now, bool(conflict_levels)) if not invalid else {'status':'not_evaluated','evidence':[],'rejected':[]}
    reasons = ([invalid] if invalid else []) + [label+'：'+c['reason'] for label,c in zip(('大盘','板块','个股'),contexts)]
    reasons.append({'unknown':'当前证据未齐，不作正向共振判断', 'aligned':'价格方向同向；下行同向不代表买入机会',
                    'countertrend':'个股向上但环境承压，逆势需另查公司事件证据', 'mixed':'层级或窗口方向存在分歧，不能合称共振'}[state])
    if conflict_levels:
        reasons.append('已记录可追溯事件，仍须人工复核，不覆盖逆势风险' if exception['status'] == 'documented_for_review'
                       else '逆势事件例外尚未成立；普通财报增长或重复来源不能代替催化')
    refs = [ref for context in contexts for ref in context.get('source_refs',[])]
    refs += [_ref('catalyst', e, 'row.nontechnical.evidence', e['asof'], family='issuer_primary_document') for e in exception.get('evidence',[])]
    families = sorted({r['source_family'] for r in refs})
    return {'version':VERSION, 'code':code, 'market':market, 'evaluated_at':now.isoformat(),
            'expected_session':expected, 'state':state, 'reasons':reasons,
            'market_context':m, 'sector_context':s, 'stock_context':t,
            'conflict':{'detected':bool(conflict_levels), 'levels':conflict_levels}, 'exception':exception,
            'source_refs':refs, 'source_families':families,
            'same_source_groups':[{'family':family,'levels':sorted({r['level'] for r in refs if r['source_family']==family})} for family in families],
            'independence_note':'大盘、ETF与个股量价同属价格证据家族，不计为三份独立验证；不同URL或重复原件不自动增加独立性。',
            'no_grade_authority':True, 'entry_permission':False, 'model_calls':0, 'network_calls':0}
