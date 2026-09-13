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
    verify_startup_imports()
    for name in ('triad_selection.json','triad_selection_pub.json','gpt_verify.json','classics_lens.json','review_factpack.json'):
        if not (REPORT/'data'/name).exists():raise RuntimeError(f'Missing central data: {name}')
    docs=[json.loads((REPORT/'data'/n).read_text(encoding='utf-8')) for n in ('triad_selection.json','triad_selection_pub.json','gpt_verify.json','classics_lens.json','review_factpack.json')]
    ids={d.get('factpack_id') for d in docs}
    if len(ids)!=1 or None in ids:raise RuntimeError('Mixed central snapshot versions')
    verify_research_views(REPORT,allow_stale=True)
    print('PASS: syntax, desktop startup imports, process checks, time zones and central factpack consistency.')

if __name__=='__main__':verify()
