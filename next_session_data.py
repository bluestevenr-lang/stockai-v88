"""Recheck existing cycle discoveries against the same local history as deep view.

No collector, model, publication, grade or order authority. A scan timestamp is
never accepted as a market-data date. Results are a bounded in-memory snapshot.
"""
from v88_paths import core_root
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
from threading import RLock

VERSION = 'next-session-local-cycle-v2'
_CACHE = {}
_LOCK = RLock()


def _store_stamp():
    root = core_root()/'data'
    result = []
    for name in ('wdata.db', 'wdata.db-wal', 'portable_history_pub.json'):
        try:
            stat = (root / name).stat()
            result.append((name, stat.st_mtime_ns, stat.st_size))
        except OSError:
            result.append((name, None, None))
    return tuple(result)


def discovery_source(legacy, *, base=None, now=None):
    """Union current candidates/current daily identities with the legacy queue.

    Inclusion is only permission to recheck; neither strategy membership nor a
    metadata date grants a direction, grade, or verified-history status.
    """
    import sqlite3
    from contextlib import closing
    from market_data_helper import _core
    _core()
    from exchange_sessions import latest_completed
    from market_symbols import canonical
    base = Path(base or core_root())
    now = now or datetime.now(timezone.utc)
    source = deepcopy(legacy or {})
    indexed = {}
    def add(row, origin):
        code = canonical(str(row.get('code') or ''))
        if not code or code.startswith('^'):
            return
        if code not in indexed:
            indexed[code] = {**row, 'code': code, 'origins': []}
        indexed[code]['origins'].append(origin)
    # Fresh candidates take precedence for identity/name, never direction.
    try:
        board = json.loads((base/'data/strategy_board.json').read_text())
        for row in board.get('rows', []):
            if row.get('source_session') == latest_completed(_market(canonical(row.get('code') or '')), now).isoformat():
                add(row, 'current_strategy')
        source['strategy_generated_at'] = board.get('generated_at')
    except (OSError, ValueError, TypeError):
        pass
    try:
        with closing(sqlite3.connect((base/'data/wdata.db').resolve().as_uri()+'?mode=ro', uri=True)) as con:
            for code, day in con.execute('SELECT code,last_date FROM verified_series'):
                if day == latest_completed(_market(code), now).isoformat():
                    add({'code': code, 'name': code}, 'current_daily')
    except (sqlite3.Error, ValueError):
        pass
    for row in source.get('stocks') or []:
        add(row, 'legacy_discovery')
    source['stocks'] = list(indexed.values())
    source['legacy_generated_at'] = source.get('generated_at')
    source['discovery_scope'] = '当期策略＋最新日线＋历史发现队列；逐股重核，不代表全市场'
    return source


def _dependencies():
    # Load the shared modules before worker threads; no sys.path races per row.
    from market_data_helper import _core
    _core()
    from deep_analysis_data import fetch
    from cloud_engine import analyze_trend_full
    from stock_cycle import cycle_phase
    from exchange_sessions import latest_completed
    from market_symbols import canonical
    return fetch, analyze_trend_full, cycle_phase, latest_completed, canonical


def _available_dates():
    """Cheap rejection only: avoid decoding the same portable pack per old row.

    A current metadata date never admits a series: fetch still validates it.
    Failed catalogue reads disable this optimization, not the normal checker.
    """
    import sqlite3
    from contextlib import closing
    from market_symbols import canonical
    root=core_root()/'data'
    try:
        with closing(sqlite3.connect((root/'wdata.db').resolve().as_uri()+'?mode=ro',uri=True)) as con:
            dates={canonical(code):str(day) for code,day in con.execute('SELECT code,last_date FROM verified_series')}
        pack=root/'portable_history_pub.json'
        if pack.exists():
            doc=json.loads(pack.read_text())
            for code,row in (doc.get('records') or {}).items():
                code=canonical(code)
                dates[code]=max(dates.get(code,''),str(row.get('source_asof') or ''))
        return dates
    except (OSError,ValueError,TypeError,AttributeError,sqlite3.Error):
        return None


def _market(code):
    return ('港股' if code.endswith('.HK') else
            'A股' if code.endswith(('.SS', '.SZ', '.BJ')) else '美股')


def _number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def load_signals(phase_doc, now=None):
    """Recheck every existing discovery locally; retain missing/ended signals.

    Cache keys include the source document, completed sessions, minute and the
    database/WAL stat. Missing inputs may be retried the next minute (or earlier
    after local ingestion); callers cannot mutate the cached result.
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError('next-session recheck requires a zoned timestamp')
    source = deepcopy(phase_doc) if isinstance(phase_doc, dict) else {}
    fetch, analyze, phase, latest_completed, canonical = _dependencies()
    expected = {m: latest_completed(m, now).isoformat() for m in ('A股', '港股', '美股')}
    source_hash = hashlib.sha256(json.dumps(source, sort_keys=True, ensure_ascii=False,
                                          default=str).encode()).hexdigest()
    key = (VERSION, source_hash, int(now.timestamp() // 60), tuple(expected.items()), _store_stamp())
    with _LOCK:
        if key in _CACHE:
            return deepcopy(_CACHE[key])
        originals = [row for row in source.get('stocks', []) if isinstance(row, dict)]
        available = _available_dates()

        def one(old):
            code = canonical(str(old.get('code') or ''))
            result = {'code': code, 'name': old.get('name') or code,
                      'direction': old.get('direction') or 'mixed',
                      'phase': old.get('phase') or '旧发现线索',
                      'original_direction': old.get('direction'),
                      'original_phase': old.get('phase'),
                      'discovery_generated_at': source.get('generated_at'),
                      'source_status': 'missing', 'source_date': None,
                      'source_asof': None, 'strength': None, 'rule_strength': None,
                      'trigger': '', 'invalid': '', 'horizon': '',
                      'confidence': None, 'confidence_kind': '规则强度分档，非概率或胜率',
                      'no_grade_authority': True, 'entry_permission': False}
            try:
                if not code:
                    raise ValueError('发现记录缺少证券代码')
                if available is not None and available.get(code, '') < expected[_market(code)]:
                    raise ValueError('本地/同步日线截至' + (available.get(code) or '未入库')
                                     + '；应到' + expected[_market(code)])
                frame, quality = fetch(code, allow_network=False)
                if frame is None or frame.empty:
                    raise ValueError(quality.get('error_detail') or '本地合格日线暂缺')
                source_day = str(quality.get('source_asof') or '')[:10]
                if (source_day != expected[_market(code)] or str(frame.index[-1])[:10] != source_day
                        or canonical(str(quality.get('code') or '')) != code):
                    raise ValueError('证券身份或最近完整交易日与深度数据不一致')
                current = phase(analyze(frame) or {})
                if not current or current.get('direction') not in ('up', 'down', 'hold'):
                    raise ValueError('本地周期计算未形成可核实结果')
                strengths = (current.get('up'), current.get('down'))
                if not all(_number(value) for value in strengths):
                    raise ValueError('周期规则强度缺失或无效')
                strength = round(max(strengths), 1)
                result.update({k: current.get(k) for k in
                               ('phase', 'confidence', 'trigger', 'invalid', 'horizon', 'pos52')})
                result.update(direction=('mixed' if current['direction'] == 'hold' else current['direction']),
                              cycle_direction=current['direction'], strength=strength, rule_strength=strength,
                              source_status='verified', source_date=source_day, source_asof=source_day,
                              source=quality.get('source'), price_basis=quality.get('price_basis'),
                              snapshot_signature=quality.get('snapshot_signature'),
                              data_points=quality.get('data_points') or len(frame),
                              last=float(frame.Close.iloc[-1]),
                              direction_changed=current['direction'] != old.get('direction'))
            except Exception as exc:
                result['source_note'] = ('本地复核未通过：' + str(exc)[:160]
                                         + '；原扫描时间不代表行情日期，旧方向暂停采用')
            return result

        with ThreadPoolExecutor(max_workers=4) as workers:
            rows = list(workers.map(one, originals))
        verified = sum(row['source_status'] == 'verified' for row in rows)
        out = {'version': VERSION, 'stocks': rows, 'scanned': source.get('scanned'),
               'generated_at': source.get('generated_at'), 'snapshot_at': now.isoformat(),
               'source_hash': source_hash, 'expected_sessions': expected,
               'discovery_count': len(originals), 'verified_count': verified,
               'missing_count': len(rows) - verified,
               'changed_count': sum(bool(row.get('direction_changed')) for row in rows),
               'mixed_count': sum(row['source_status'] == 'verified' and row['direction'] == 'mixed' for row in rows),
               'notes': [source.get('discovery_scope') or '仅复核原扫描已经发现的股票，不代表重新扫描原研究池或全市场。',
                         '行情截至各市场最近完整交易日；扫描时间与本页复核时间另列。',
                         '个股量价规则为观察线索；板块关联与中央评级由联动视图另行核对。'],
               'network_calls': 0, 'model_calls': 0, 'cache_seconds': 60}
        _CACHE.clear()
        _CACHE[key] = deepcopy(out)
        return out
