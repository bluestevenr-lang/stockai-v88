"""Offline checks for the installed desktop + core; no model, message or fetch."""
import ast
import importlib
import hashlib
import subprocess
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo
ROOT=Path(__file__).resolve().parents[1]
REPORT=ROOT.parent/'ai-daily-report-v2'

# Both import orders occur in V88: desktop pages and core-generated digests.
SCORING_SHARED_FILES=(
    'observation_score.py','horizon_score_policy.py','display_limits.py',
    'review_scorecard.py','profit_contract.py','investment_maturity.py',
    'market_watch_ui.py','market_adaptation_ui.py','exchange_sessions.py',
)
SCORING_CORE_FILES=(
    'market_symbols.py','history_calendar.py',
    'market_watch.py','market_adaptation.py','left_entry_watch.py',
    'recommendation_rationale.py','feishu_digest.py',
)


def verify_scoring_dependencies(root=ROOT, report=REPORT):
    """Check copied source packages without collector/model/network execution."""
    root,report=Path(root).resolve(),Path(report).resolve()
    source=report/'src'
    for name in SCORING_SHARED_FILES:
        paths=(root/name,source/name)
        for path in paths:
            if not path.is_file():raise RuntimeError(f'Missing scoring dependency: {path}')
        if paths[0].read_bytes().replace(b'\r\n',b'\n') != paths[1].read_bytes().replace(b'\r\n',b'\n'):
            raise RuntimeError(f'Scoring mirror version mismatch: {name}')
    for name in SCORING_CORE_FILES:
        if not (source/name).is_file():raise RuntimeError(f'Missing scoring dependency: {source/name}')
    # Fresh isolated interpreters prevent sys.modules/PYTHONPATH from masking a
    # missing packaged file with a module from the developer's other checkout.
    probe=r'''
import importlib, socket, sys
from datetime import datetime
from pathlib import Path
network_calls=[]
def offline(*args, **kwargs):
    network_calls.append(True)
    raise RuntimeError('Network access is forbidden in offline scoring verification')
socket.create_connection=offline
socket.socket.connect=offline
root,source=Path(sys.argv[1]),Path(sys.argv[2])
order=sys.argv[3]
sys.path[:0]=[str(root),str(source)] if order=='desktop' else [str(source),str(root)]
expected=root if order=='desktop' else source
for name in sys.argv[4].split(','):
    module=importlib.import_module(name)
    assert Path(module.__file__).resolve()==expected/(name+'.py'), (order,name,module.__file__)
for name in sys.argv[5].split(','):
    module=importlib.import_module(name)
    if not (root/(name+'.py')).exists():
        assert Path(module.__file__).resolve()==source/(name+'.py'), (order,name,module.__file__)
import observation_score, horizon_score_policy, profit_contract, investment_maturity, review_scorecard
score=observation_score.assess({'code':'TEST','market':'美股'},now=datetime.fromisoformat('2026-09-15T21:00:00+08:00'))
assert score['coverage_pct']==0 and score['total'] is None
assert horizon_score_policy.horizon_score({'4周':{'rule_score':60},'8周':{'rule_score':80}},'medium')['score']==73
assert not profit_contract.evaluate({},'short')['valid']
assert investment_maturity.assess({},'short')['tier']=='PENDING'
assert review_scorecard.score_policy()['version']==review_scorecard.SCORE_POLICY_VERSION
assert not network_calls, 'An imported scoring dependency attempted a network call'
'''
    shared=','.join(Path(name).stem for name in SCORING_SHARED_FILES)
    core=','.join(Path(name).stem for name in SCORING_CORE_FILES)
    for order in ('desktop','core'):
        subprocess.run([sys.executable,'-I','-c',probe,str(root),str(source),order,shared,core],
                       cwd=root,check=True,capture_output=True,text=True,encoding='utf-8',timeout=30)
    return {'shared_files':len(SCORING_SHARED_FILES),'core_dependencies':len(SCORING_CORE_FILES),
            'isolated_import_orders':['desktop','core'],'network_calls':0}

def verify_startup_imports(root=ROOT):
    """Import the actual UI prelude, including names imported inside branches.

    Syntax checks alone cannot detect missing OS modules. Do not execute the
    page, its data collectors, model calls or messaging during installation.
    """
    tree=ast.parse((root/'app_v88_integrated.py').read_text(encoding='utf-8-sig'))
    for statement in tree.body:
        if isinstance(statement,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)):break
        for node in ast.walk(statement):
            if isinstance(node,ast.Import):
                for item in node.names:importlib.import_module(item.name)
            elif isinstance(node,ast.ImportFrom) and node.module and not node.level:
                module=importlib.import_module(node.module)
                for item in node.names:
                    if item.name != '*' and not hasattr(module,item.name):
                        importlib.import_module(node.module+'.'+item.name)
    from runtime_guard import ensure_file_capacity,process_alive
    ensure_file_capacity()
    import os
    if not process_alive(os.getpid()):raise RuntimeError('Process liveness check failed')


def verify_research_views(report=REPORT, *, allow_stale=False):
    """Exercise actual mirror inputs, never the developer's home-directory DB."""
    from stock_profile_view import load, profile, industry_peers
    from portable_history import read
    from market_data_helper import validate
    from focused_deep_view import calculate
    from deep_cross_validation import load_context
    from deep_analysis_data import snapshot_signature
    from market_badge import html
    profiles=load(report/'data/stock_profiles_pub.json')
    if not profiles.get('records'):raise RuntimeError('Company profiles are missing or unreadable')
    for code,market in [('688002.SS','A股'),('SEPN','美股'),('661.HK','港股')]:
        p=profile(code,profiles)
        if not p.get('industry') or not industry_peers(code,profiles)['ok']:
            raise RuntimeError(f'Industry ranking/Top10 unavailable: {code}')
        frame=read(code,'2000-01-01','2100-01-01',path=report/'data/portable_history_pub.json')
        frame=frame.rename(columns={k:k.title() for k in frame.columns})
        try:
            frame=validate(frame,code,source=frame.attrs['provider_source'])
        except ValueError as exc:
            if allow_stale and str(exc)=='缺少最近已完成交易日行情':
                print(f'NOTICE: {code} source date {frame.attrs.get("source_asof")} needs market refresh; no current forecast granted.')
                continue
            raise
        quality={'code':code,**frame.attrs,'snapshot_signature':snapshot_signature(frame)}
        context=load_context(code,data_dir=report/'data')
        result=calculate(context,frame,quality,p.get('name_zh') or code,code)
        outlook=result['annual_outlook']
        if not outlook.get('valid12month'):
            raise RuntimeError(f'Annual evidence incomplete: {code}: {outlook.get("gaps")}')
        future=result['future_scenario']
        if future.get('status')!='ready':raise RuntimeError(f'Future trend view not ready: {code}')
        if '<svg' not in html(market):raise RuntimeError(f'Market symbol missing: {market}')
    print('PASS: CN/HK/US portable research inputs checked; current-source charts verified, any stale source explicitly reported.')

def verify():
    sys.path[:0]=[str(ROOT),str(REPORT/'src')]
    manifest=json.loads((ROOT/'win/release.json').read_text(encoding='utf-8'))
    subprocess.run(['git','-C',str(REPORT),'merge-base','--is-ancestor',manifest['required_core_commit'],'HEAD'],check=True)
    for base,key in ((ROOT,'desktop_files'),(REPORT,'core_files')):
        for name,expected in manifest[key].items():
            raw=(base/name).read_bytes().replace(b'\r\n',b'\n')
            if hashlib.sha256(raw).hexdigest()!=expected:
                raise RuntimeError(f'Program version mismatch: {name}')
    for zone in ('Asia/Shanghai','Asia/Hong_Kong','America/New_York'):ZoneInfo(zone)
    for module in ('streamlit','pandas','numpy','plotly','exchange_calendars','requests','dotenv','bs4','feedparser','openpyxl','schedule','psutil','portalocker'):
        importlib.import_module(module)
    import streamlit
    if not hasattr(streamlit,'html'):raise RuntimeError('Streamlit st.html is required')
    for p in list(ROOT.glob('*.py'))+list((ROOT/'modules').glob('*.py'))+list((REPORT/'src').glob('*.py')):
        ast.parse(p.read_text(encoding='utf-8-sig'),filename=str(p))
    for module in ('grade_card','annual_outlook','future_trend_visual','next_session_model','next_session_view',
                   'next_session_data','stock_switcher','deep_cross_validation','astra_trade_entry_view','action_source_view',
                   'runtime_guard','scan_progress_view','platform_lock'):
        importlib.import_module(module)
    verify_scoring_dependencies()
    verify_startup_imports()
    for name in ('triad_selection.json','triad_selection_pub.json','gpt_verify.json','classics_lens.json','review_factpack.json'):
        if not (REPORT/'data'/name).exists():raise RuntimeError(f'Missing central data: {name}')
    docs=[json.loads((REPORT/'data'/n).read_text(encoding='utf-8')) for n in ('triad_selection.json','triad_selection_pub.json','gpt_verify.json','classics_lens.json','review_factpack.json')]
    ids={d.get('factpack_id') for d in docs}
    if len(ids)!=1 or None in ids:raise RuntimeError('Mixed central snapshot versions')
    verify_research_views(REPORT,allow_stale=True)
    print('PASS: syntax, desktop startup imports, process checks, time zones and central factpack consistency.')

if __name__=='__main__':verify()
