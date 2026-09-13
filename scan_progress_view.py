"""Read-only scan accounting: directory checks, usable data and GPT are distinct."""
from collections import Counter
from datetime import datetime, timezone, timedelta
from html import escape
import json
import hashlib
from pathlib import Path
import sqlite3
import re
import os
from zoneinfo import ZoneInfo

from session_coverage import project
from evidence_freshness import source_times_fresh
from runtime_guard import process_alive

MARKETS = ('A股', '港股', '美股')
SCAN_STATES = {'facts_generated', 'missing_history', 'source_rejected', 'fact_rejected'}


def active_review_progress(core, *, now=None):
    """Read the active staged run; never publish its provisional decisions."""
    from review_scorecard import gpt_result,pair_complete
    from review_contract import MODEL,SCHEMA_VERSION,PROMPT_HASH,known_protocol
    data=Path(core)/'data'
    marker=data/'.gpt_classics_review_running'
    try:
        pid=marker.read_text().strip()
        if not pid.isdigit() or int(pid)<=0:
            return None
        if not process_alive(int(pid)):return None
        started=marker.stat().st_mtime
        runs=data/'full_market_review_runs'
        candidates=sorted((p for p in runs.iterdir() if p.is_dir() and re.fullmatch(r'\d{8}T\d{6}-[0-9a-f]{8}',p.name)),reverse=True)
        if not candidates:return None
        run=candidates[0];input_path=run/'input.json'
        # A prior run must not look active merely because a new process owns the lease.
        if input_path.stat().st_mtime<started or (run/'result.json').exists():return None
        inputs=json.loads(input_path.read_text())
        pack=json.loads((run/'data/review_factpack.json').read_text())
        cohort=json.loads((run/'review-cohort.json').read_text())
        if inputs['factpack_id']!=pack['factpack_id'] or cohort['factpack_id']!=pack['factpack_id']:return None
        if hashlib.sha256((run/'data/review_factpack.json').read_bytes()).hexdigest()!=cohort['factpack_sha256']:return None
        now=now or datetime.now(timezone.utc)
        rows=[r for r in pack['items'] if r['code'] in cohort['eligible']]
        if len(rows)!=len(cohort['eligible']) or any(r['sig']!=cohort['eligible'][r['code']] for r in rows):return None
        def fresh(rec,item):
            if rec.get('review_suspended') or rec.get('model')!=MODEL or not known_protocol(rec,item):return False
            if rec.get('sig')!=item['sig'] or rec.get('review_scope')!='buy' or not pair_complete(rec) or not gpt_result(rec)['valid']:return False
            try:at=datetime.strptime(str(rec.get('ts',''))[:16],'%Y-%m-%d %H:%M').replace(tzinfo=ZoneInfo('Asia/Shanghai'))
            except ValueError:return False
            return timedelta(0)<=now-at<=timedelta(hours=24) and source_times_fresh(item,now)
        live=json.loads((data/'gpt_verify.json').read_text()).get('rows',{})
        stage=json.loads((run/'data/gpt_verify.json').read_text())
        staged=stage.get('rows',{}) if stage.get('factpack_id')==pack['factpack_id'] else {}
        reused={r['code'] for r in rows if fresh(live.get(r['code'],{}),r)}
        completed=reused|{r['code'] for r in rows if fresh(staged.get(r['code'],{}),r)}
        return {'eligible':len(rows),'complete':len(completed),'reused':len(reused),
                'new_complete':len(completed-reused),'remaining':len(rows)-len(completed),
                'no_grade_authority':True,'run':run.name}
    except (OSError,ValueError,TypeError,KeyError):
        return None


def snapshot(core, *, now=None):
    now = now or datetime.now(timezone.utc)
    data = Path(core)/'data'
    daily = json.loads((data/'universe_data_coverage.json').read_text())
    path = data/'full_market_research.sqlite'
    with sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True) as db:
        row = db.execute('SELECT s.id,s.source_available_at FROM snapshots s JOIN active a ON a.snapshot=s.id WHERE a.id=1').fetchone()
        if not row or datetime.fromisoformat(row[1]) > now:
            raise ValueError('尚无当前可核对的完整扫描记录')
        members = db.execute('SELECT code,market,status,sources FROM members WHERE snapshot=?', (row[0],)).fetchall()
    # Compare identities, not equal totals: delistings and additions can cancel.
    manifest_ref = daily.get('universe_manifest') or {}
    manifest_path = Path(manifest_ref.get('path') or '')
    if not manifest_path.resolve().is_relative_to((data/'universe_manifests').resolve()):
        raise ValueError('目录凭据路径未通过核验')
    manifest = json.loads(manifest_path.read_text())
    frozen = {k: manifest[k] for k in ('codes_by_market','catalogs','excluded_exchange_declared_etf_etp','invalid_entries')}
    digest = hashlib.sha256(json.dumps(frozen,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
    if digest != manifest_ref.get('sha256_canonical_content') or digest != manifest.get('sha256_canonical_content'):
        raise ValueError('目录凭据不一致')
    counts = {m: Counter() for m in MARKETS}
    fresh, removed, identities = Counter(), Counter(), {}
    for market in MARKETS:
        codes = manifest['codes_by_market'][market]
        if len(codes) != len(set(codes)) or len(codes) != daily['markets'][market]['denominator']:
            raise ValueError('目录数量与身份不一致')
        identities[market] = set(codes)
    clock_cache = {}
    for code, market, status, sources in members:
        if code not in identities.get(market, set()):
            removed[market] += 1
            continue
        counts[market][status] += 1
        if status == 'facts_generated':
            key = (market,sources)
            if key not in clock_cache:
                clock_cache[key] = source_times_fresh({'market':market},now,sources=json.loads(sources or '{}'))
            fresh[market] += int(clock_cache[key])
    return {'receipt': row[0], 'available_at': row[1], 'daily': daily,
            'counts': counts, 'current_facts':dict(fresh), 'removed_from_directory':dict(removed),
            'identity_verified':True,'active_review':active_review_progress(core,now=now)}


def metrics(source, rows, *, now=None):
    current = Counter()
    seen = set()
    for row in rows:
        gpt = (row.get('scorecard') or {}).get('gpt') or {}
        code = row.get('code')
        if code and code not in seen and gpt.get('current') and gpt.get('complete'):
            market = row.get('market')
            if market not in MARKETS:
                market = '港股' if code.endswith('.HK') else 'A股' if code.endswith(('.SZ','.SS','.SH','.BJ')) else '美股'
            current[market] += 1
            seen.add(code)
    result = []
    for market in MARKETS:
        d = source['daily'].get('markets', {}).get(market, {})
        counts = source['counts'].get(market, {})
        n = d.get('denominator', 0)
        checked = sum(v for k, v in counts.items() if k in SCAN_STATES)
        clock = project(market, d, now=now)
        catalog_issues = [c for c in source['daily'].get('catalogs',[]) if c.get('market') == market and '未来' in c.get('time_status','')]
        result.append({'market': market, 'directory': n, 'checked': checked,
                       'facts': counts.get('facts_generated', 0), 'reviewed': current[market],
                       'current_facts':source.get('current_facts',{}).get(market),
                       'catalog_future':bool(catalog_issues),
                       'catalog_conservative':bool(catalog_issues) and all(c.get('conservative_effective_union') is True for c in catalog_issues),
                       'unscanned':max(0,n-checked),
                       'removed':source.get('removed_from_directory',{}).get(market,0),
                       'valid_daily': clock['verified_daily'] if clock['dated_receipt'] else None,
                       'required_session': clock['required_session'],
                       'daily_target_met': clock['daily_target_met'] and not catalog_issues,
                       'rows_checked': bool(n > 0 and checked == n and sum(counts.values()) == n
                                            and d.get('directories_complete') is True),
                       'scan_complete': bool(n > 0 and checked == n and sum(counts.values()) == n
                                             and d.get('directories_complete') is True and not catalog_issues),
                       'failures': {k:v for k,v in counts.items() if k != 'facts_generated'}})
    return result


CSS = '''<style>
.v88-scan{margin:6px 0;padding:8px 12px;background:#fff;border:1px solid #dbe3ed;border-radius:6px;color:#334155;font-size:12px;line-height:1.4;text-align:left}
.v88-scan-heading{display:flex;justify-content:space-between;align-items:baseline;gap:4px 12px;flex-wrap:wrap;margin-bottom:4px}
.v88-scan-title{font-size:13px;font-weight:650}
.v88-scan-time{font-size:12px;color:#64748b;font-weight:400}
.v88-scan-status{font-size:12px;line-height:1.5;margin:3px 0 6px;color:#475569}
.v88-scan-scroll{max-width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch}
.v88-scan table{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums}
.v88-scan .v88-scan-primary{margin-bottom:6px!important}
.v88-scan .v88-scan-primary{table-layout:fixed;min-width:0}
.v88-scan-primary th:nth-child(1){width:10%}
.v88-scan-primary th:nth-child(2){width:31%}
.v88-scan-primary th:nth-child(3){width:37%}
.v88-scan-primary th:nth-child(4){width:22%}
.v88-scan .v88-scan-detail-table{min-width:700px}
.v88-scan th,.v88-scan td{font-size:12px!important;padding:5px 8px!important;text-align:right;white-space:nowrap;border-bottom:1px solid #eef2f6}
.v88-scan .v88-scan-primary th,.v88-scan .v88-scan-primary td{white-space:normal;overflow-wrap:normal;padding:4px!important}
.v88-scan th:first-child,.v88-scan td:first-child{text-align:left}
.v88-scan th{color:#64748b;font-weight:500}
.v88-scan-note{color:#64748b;font-size:12px;margin-top:6px}
.v88-scan details{margin-top:5px;font-size:12px}
.v88-scan summary{cursor:pointer;color:#475569}
@media(max-width:600px){.v88-scan{padding:8px 6px}.v88-scan-title{font-size:12px}}
</style>'''


def html(rows, *, core=None, source=None, now=None):
    try:
        source = source if source is not None else snapshot(core or Path.home()/'Desktop/ai-daily-report-v2', now=now)
        entries = metrics(source, rows, now=now)
        total = sum(r['directory'] for r in entries)
        checked = sum(r['checked'] for r in entries)
        facts = sum(r['facts'] for r in entries)
        complete = all(r['scan_complete'] for r in entries)
        state = '目录检查：已全量（含缺数项）' if complete else '目录检查尚未齐全'
        if not complete and all(r['rows_checked'] for r in entries) and any(r['catalog_future'] for r in entries):
            state = '目录逐只检查完成·港股成分时效待核'
        all_daily = all(r['directory'] > 0 and r['valid_daily'] == r['directory'] for r in entries)
        numeric_target = all(r['directory'] > 0 and (r['valid_daily'] or 0) >= (9*r['directory']+9)//10 for r in entries)
        conservative=all(not r['catalog_future'] or r['catalog_conservative'] for r in entries)
        if numeric_target and any(r['catalog_future'] for r in entries) and conservative:
            data_state = '日线：三市均达90%（港股按保守目录并集）'
        elif numeric_target and any(r['catalog_future'] for r in entries):
            data_state = '日线比例已达90%·目录时效待核'
        else:
            data_state = '日线已全覆盖' if all_daily else '日线：三市均达90%目标' if numeric_target else '日线覆盖待达90%'
        reviewed = sum(r['reviewed'] for r in entries)
        review_state = 'GPT双审：尚未全量' if reviewed < total or not complete else 'GPT全量状态：待逐股核对'
        title = f'全市场扫描 · 已扫描 {checked:,} / {total:,} 只'
        current_facts = sum(r['current_facts'] or 0 for r in entries)
        cells, primary = [], []
        for r in entries:
            n = r['directory']
            progress = f"{r['checked']:,} / {n:,}" if n else '目录待核'
            progress += ' · 100%' if r['scan_complete'] else ' · 100%·成分时效待核' if r['rows_checked'] else ' · 未齐'
            v = r['valid_daily']
            valid = f'{v:,} / {n:,} · {v/n:.2%}' if v is not None and n else '日期待核'
            color = '#334155' if r['daily_target_met'] else '#b45309'
            current = f'{v:,} · {v/n:.2%}' if v is not None and n else '日期待核'
            primary.append('<tr>'
                +f'<td>{escape(r["market"])}</td><td>{escape(progress.split(" · ")[0])}</td>'
                +f'<td style="color:{color}">{current}</td>'
                +f'<td>{escape(str(r["required_session"] or "待核"))}</td></tr>')
            cells.append('<tr>'+''.join(f'<td>{escape(str(x))}</td>' for x in
                         (r['market'], f'{n:,}', progress, f"{r['facts']:,}"))
                         +f'<td style="color:{color}">{valid}</td>'
                         +f"<td>{r['reviewed']:,}</td><td>{escape(str(r['required_session'] or '待核'))}</td></tr>")
        at = datetime.fromisoformat(source['available_at']).astimezone(ZoneInfo('Asia/Shanghai')).strftime('%m-%d %H:%M')
        gaps = '；'.join(f"{r['market']}有效日线距90%还差 {max(0,(9*r['directory']+9)//10-(r['valid_daily'] or 0)):,} 只"
                        for r in entries if (r['valid_daily'] or 0) < (9*r['directory']+9)//10)
        details = '；'.join(f"{r['market']}：缺历史 {r['failures'].get('missing_history',0):,}，来源未过 {r['failures'].get('source_rejected',0):,}，事实条件未过 {r['failures'].get('fact_rejected',0):,}" for r in entries)
        heads = ('市场','目录数','完成检查','已生成事实','当前有效日线','当前完整双审','行情截止日')
        refresh_note = ''
        active=source.get('active_review')
        review_note=''
        if active:
            review_note=(f'<div class="v88-scan-note" data-testid="v88-review-progress">GPT补审进行中 · 本轮可审 {active["eligible"]:,} 只'
                +f' · 已完成双审 {active["complete"]:,}（复用 {active["reused"]:,} / 新增 {active["new_complete"]:,}）'
                +f' · 剩余 {active["remaining"]:,}。暂存审核完成后统一发布；评级以当前列表为准。</div>')
        if source['daily'].get('in_progress'):
            try:
                data_at = datetime.fromisoformat(source['daily']['generated_at']).astimezone(ZoneInfo('Asia/Shanghai')).strftime('%m-%d %H:%M')
            except (KeyError, TypeError, ValueError):
                data_at = '时点待核'
            refresh_note = (f'<div class="v88-scan-note">收盘行情采集进度 · 最新回报 {escape(data_at)}；表内只计已通过核验的数据。</div>')
        return (CSS+'<section id="v88-market-scan" class="v88-scan" aria-label="全市场扫描进度">'
                +'<div class="v88-scan-heading">'
                +f'<div class="v88-scan-title">{title}</div>'
                +f'<div class="v88-scan-time">扫描记录：{at} 北京时间</div></div>'
                +f'<div class="v88-scan-status">{state} · {data_state} · {review_state}</div>'
                +'<table class="v88-scan-primary"><thead><tr>'
                +''.join(f'<th>{h}</th>' for h in ('市场','已检查 / 目录数','有效日线 / 覆盖率','日线日期'))
                +'</tr></thead><tbody>'+''.join(primary)+'</tbody></table>'
                +refresh_note
                +f'<div class="v88-scan-note">上次量化初筛生成事实 {facts:,} 只；其中行情时效仍有效 {current_facts:,} 只；当前完整GPT双审 {reviewed:,} 只。</div>'
                +review_note
                +('<div class="v88-scan-note">港股按已生效与预披露目录的保守并集统计，保留预披露中移除的现有代码；精确当日成分仍待生效核对。</div>' if any(r['catalog_future'] for r in entries) and conservative else '<div class="v88-scan-note" style="color:#b45309">港股目录包含未来生效版本，分母待生效核对，暂不认定全量覆盖达标。</div>' if any(r['catalog_future'] for r in entries) else '')
                +(f'<div class="v88-scan-note" style="color:#b45309">数据覆盖未全部达标：{gaps}。</div>' if gaps else '')
                +'<details><summary>分市场审核数、失败原因与扫描凭据</summary>'
                +'<div class="v88-scan-scroll"><table class="v88-scan-detail-table"><thead><tr>'
                +''.join(f'<th>{h}</th>' for h in heads)+'</tr></thead><tbody>'+''.join(cells)+'</tbody></table></div>'
                +f'目录检查包含缺数与失败项，不代表每只都已完成分析。扫描凭据留存于 {at}；行情截止日另列，页面刷新不改变原始数据时间。{details}。'
                +f'当前目录新增或未扫描 {sum(r["unscanned"] for r in entries):,} 只；原扫描中已不在当前目录的 {sum(r["removed"] for r in entries):,} 只保留历史。'
                +'分母为已收录交易所目录，剔除明确标注的ETF/ETP；未细分证券仍保留。目录日期、缺失来源与三表核验见下方覆盖详情。'
                +f'<br>扫描凭据：{escape(source["receipt"])}</details></section>')
    except (OSError, ValueError, TypeError, KeyError, sqlite3.Error):
        return CSS+'<section id="v88-market-scan" class="v88-scan">扫描进度暂不可核对；不以报价条数或审核池数量代替全市场扫描。</section>'
