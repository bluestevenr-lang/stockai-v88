"""Local, exact-identity stock picker catalog; no model, quote fetch or writes.

Directory membership provides discoverability, never current price/grade authority.
Names from old dictionaries only enrich identities retained in current directories.
"""
from __future__ import annotations
from v88_paths import core_root
from collections import Counter
from datetime import datetime, timezone
from functools import lru_cache
from importlib.util import module_from_spec, spec_from_file_location
import json
import hashlib
from pathlib import Path
import re
import unicodedata
from zoneinfo import ZoneInfo

VERSION = 'local-stock-picker-v1'
CORE = core_root()
_MARKETS = ('A股', '港股', '美股', 'B股')
_CLASSES = {'BRK.A':'BRK_A', 'BRK-A':'BRK_A', 'BRK.B':'BRK_B', 'BRK-B':'BRK_B',
            'BF.A':'BF_A', 'BF-A':'BF_A', 'BF.B':'BF_B', 'BF-B':'BF_B'}


def identity(value):
    code = unicodedata.normalize('NFKC', str(value or '')).strip().upper()
    if re.fullmatch(r'\d{6}\.SH', code): code = code[:-3] + '.SS'
    if re.fullmatch(r'\d{1,5}\.HK', code): return str(int(code[:-3])) + '.HK'
    if code in _CLASSES: return _CLASSES[code]
    if re.fullmatch(r'[A-Z]+-P[A-Z]+', code): return code.replace('-P', '$', 1)
    for a, b in (('-WT', '.W'), ('-UN', '.U')):
        if re.fullmatch(r'[A-Z]+' + re.escape(a), code): return code[:-len(a)] + b
    if re.fullmatch(r'[A-Z]+-[AB]', code): return code[:-2] + '.' + code[-1]
    return code


def _symbol(code):
    if code.endswith('.HK'): return code[:-3].zfill(4) + '.HK'
    if code in {'BRK_A', 'BRK_B', 'BF_A', 'BF_B'}: return code.replace('_', '-')
    if re.fullmatch(r'[A-Z]+\$[A-Z]+', code): return code.replace('$', '-P')
    for a, b in (('.W', '-WT'), ('.U', '-UN'), ('.A', '-A'), ('.B', '-B')):
        if re.fullmatch(r'[A-Z]+' + re.escape(a), code): return code[:-len(a)] + b
    return code


def _norm(value):
    return re.sub(r'\s+', '', unicodedata.normalize('NFKC', str(value or '')).casefold())


def _has_zh(value):
    return bool(re.search(r'[\u3400-\u9fff]', str(value or '')))


def _valid(code, market):
    if market in ('A股', 'B股'): return bool(re.fullmatch(r'\d{6}\.(SS|SZ|BJ)', code))
    if market == '港股': return bool(re.fullmatch(r'\d{1,5}\.HK', code)) and code != '0.HK'
    if market == '美股': return bool(re.fullmatch(r'[A-Z][A-Z0-9._$^+\-=]{0,19}', code))
    return False


def _code_aliases(code):
    result = {code, _symbol(code)}
    if code.endswith('.HK'):
        n = code[:-3]
        result.update((n, n.zfill(4), n.zfill(5), n.zfill(5)+'.HK', 'HK'+n.zfill(5), 'HK'+n.zfill(4)))
    elif re.fullmatch(r'\d{6}\.(SS|SZ|BJ)', code):
        number, suffix = code.split('.')
        exchange = 'SH' if suffix == 'SS' else suffix
        result.update((number, number+'.'+exchange, exchange+number))
    else:
        result.update(alias for alias, key in _CLASSES.items() if key == code)
    return result


def build_catalog(directory_rows, profiles=None, legacy_names=(), *, scope='', source_signature=''):
    """Pure catalog assembly for UI/tests; never merges distinct share classes."""
    profiles = profiles if isinstance(profiles, dict) else {}
    records = profiles.get('records') or {}
    if not isinstance(records, dict): records = {}
    by_identity = {}
    for original in directory_rows:
        if not isinstance(original, dict): continue
        key = identity(original.get('code')); market = original.get('market')
        if not _valid(key, market): continue
        existing = by_identity.get(key)
        if existing and existing['market'] != market: continue
        row = by_identity.setdefault(key, {'identity':key, 'code':_symbol(key), 'market':market,
            'names':set(), 'catalog_asof':original.get('catalog_asof'),
            'time_status':original.get('time_status'), 'security_type':original.get('security_type')})
        if original.get('name'): row['names'].add(str(original['name']).strip())
    legacy = {}
    for old in legacy_names:
        if not isinstance(old, dict): continue
        key = identity(old.get('c'))
        if key in by_identity and old.get('m') == by_identity[key]['market'] and old.get('n'):
            legacy.setdefault(key, []).append(str(old['n']).strip())
    for key, row in by_identity.items():
        profile = records.get(key) or {}
        if not isinstance(profile, dict): profile = {}
        if identity(profile.get('code')) != key or profile.get('market') != row['market']: profile = {}
        original_names = sorted(row['names'])
        names = original_names + legacy.get(key, [])
        zh = str(profile.get('name_zh') or '').strip()
        en = str(profile.get('name_en') or '').strip()
        if not _has_zh(zh): zh = next((n for n in names if _has_zh(n)), '')
        if not en or _has_zh(en): en = next((n for n in original_names if not _has_zh(n)), '')
        # Prefer actual English directory name over a Chinese-only fallback.
        row['name_zh'], row['name_en'] = zh, en
        if row['market'] == '美股':
            row['name'] = en or row['code']
            display = f"{en or row['code']} · {zh or '中文名待核'}"
        else:
            row['name'] = zh or en or row['code']
            display = zh or (f'{en} · 中文名待核' if en else '中文名待核')
        row['label'] = f"{display} · {row['code']} · {row['market']}"
        aliases = sorted(set(names + [zh, en]) - {''})
        codes = sorted(_code_aliases(key))
        row['aliases'] = aliases
        row['code_aliases'] = codes
        row['search_text'] = ' '.join(aliases + codes + [row['market']])
        row['_names'] = tuple(_norm(n) for n in aliases)
        row['_codes'] = tuple(_norm(n) for n in codes)
        row['_label'] = _norm(row['label'])
        del row['names']
    rows = sorted(by_identity.values(), key=lambda r:(_MARKETS.index(r['market']),r['code']))
    by_code = {}
    for row in rows:
        for code in row['code_aliases']:
            # Ambiguous abbreviations are resolved by resolve_exact, never last-write wins.
            if code not in by_code:
                by_code[code] = row
            elif by_code[code] and by_code[code]['identity'] != row['identity']:
                by_code[code] = False
        by_code[row['identity']] = row
    by_code = {k:v for k,v in by_code.items() if v}
    return {'version':VERSION, 'rows':rows, 'by_code':by_code, 'counts':dict(Counter(r['market'] for r in rows)),
        'directory_total':len(rows), 'scope':scope, 'source_signature':source_signature,
        'model_calls':0, 'network_calls':0, 'cash_cost_cny':0, 'no_grade_authority':True}


def search(query, catalog=None, *, limit=30, market='全部'):
    """Rank all exact identities/names first; partial hits still require selection."""
    if type(limit) is not int or limit < 1: raise ValueError('limit must be positive')
    catalog = load_catalog() if catalog is None else catalog
    q = _norm(query)
    if not q: return []
    terms = [_norm(t) for t in str(query).split() if _norm(t)]
    candidates = []
    for row in catalog['rows']:
        if market != '全部' and row['market'] != market: continue
        codes, names = row['_codes'], row['_names']
        if q in codes: rank = 0
        elif q in names or q == row['_label']: rank = 1
        elif any(t.startswith(q) for t in codes + names): rank = 2
        elif all(any(term in t for t in codes + names) for term in terms): rank = 3
        else: continue
        candidates.append((rank, row['label'], row))
    candidates.sort(key=lambda t:(t[0], t[1]))
    return [r for _,_,r in candidates[:limit]]


def resolve_exact(value, catalog=None):
    """Only a unique exact code, alias or full visible label can be selected."""
    catalog = load_catalog() if catalog is None else catalog
    q = _norm(value)
    if not q: return None
    code_matches = [r for r in catalog['rows'] if q in r['_codes']]
    if code_matches: return code_matches[0] if len(code_matches) == 1 else None
    names = [r for r in catalog['rows'] if q in r['_names'] or q == r['_label']]
    return names[0] if len(names) == 1 else None


def _fingerprint(paths):
    result = []
    for path in sorted(set(paths)):
        try:
            stat = path.stat(); result.append((str(path), stat.st_mtime_ns, stat.st_size))
        except OSError: result.append((str(path), None, None))
    return tuple(result)


def _read_json(path, default):
    try: return json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, ValueError): return default


@lru_cache(maxsize=3)
def _load_cached(core, names_path, signature, date):
    root = Path(core)
    path = root/'src/market_directory_search.py'
    directories = []
    scope = '本地名称目录；目录身份不代表已具备有效行情或评级。'
    missing = []
    try:
        spec = spec_from_file_location('_v88_picker_directory_source', path)
        module = module_from_spec(spec); spec.loader.exec_module(module)
        found = module.search(base=root)
        directories, scope = found['rows'], found['scope']
        missing = found['missing_directories']
    except (OSError, ValueError, TypeError, AttributeError, ImportError):
        missing = ['交易所目录模块暂不可用']
    legacy = _read_json(names_path, [])
    if not isinstance(legacy, list): legacy = []
    if not directories:
        directories = [{'code':r.get('c'), 'name':r.get('n'), 'market':r.get('m'),
            'time_status':'旧名称名录·目录日期待核', 'security_type':'未细分证券类型'}
            for r in legacy if isinstance(r, dict)]
        scope = '当前交易所目录不可用，暂用旧名称索引；不代表当前在市或有效行情。'
    doc = build_catalog(directories, _read_json(root/'data/stock_profiles_pub.json', {}), legacy,
                        scope=scope, source_signature=hashlib.sha256(repr((signature,date)).encode()).hexdigest())
    doc['missing_directories'] = missing
    return doc


def load_catalog(core=None, *, names_path=None):
    """Cache by every local source's mtime/size and date; no network/LLM calls.

    Treat the returned catalog as read-only. Do not store user-selected/recent
    symbols here: those belong to the caller's private session state.
    """
    root = Path(core or CORE)
    names_path = Path(names_path or Path(__file__).with_name('stock_names.json'))
    folder = root/'data/free_market_data'
    paths = list(folder.glob('directory_*.json')) + list((folder/'history/directories').glob('*/*.json'))
    paths += [root/'data/stock_profiles_pub.json', root/'src/market_directory_search.py', names_path]
    signature = _fingerprint(paths)
    now = datetime.now(timezone.utc)
    dates = tuple(now.astimezone(ZoneInfo(zone)).date().isoformat()
                  for zone in ('Asia/Shanghai', 'America/New_York'))
    return _load_cached(str(root), str(names_path), signature, dates)
