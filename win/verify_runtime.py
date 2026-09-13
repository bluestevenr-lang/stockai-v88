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
    for module in ('streamlit','pandas','numpy','plotly','exchange_calendars','requests','dotenv','bs4','feedparser','openpyxl','schedule'):
        importlib.import_module(module)
    import streamlit
    if not hasattr(streamlit,'html'):raise RuntimeError('Streamlit st.html is required')
    for p in list(ROOT.glob('*.py'))+list((ROOT/'modules').glob('*.py'))+list((REPORT/'src').glob('*.py')):
        ast.parse(p.read_text(encoding='utf-8-sig'),filename=str(p))
    for module in ('grade_card','annual_outlook','future_trend_visual','next_session_model','next_session_view',
                   'next_session_data','stock_switcher','deep_cross_validation','astra_trade_entry_view','action_source_view'):
        importlib.import_module(module)
    for name in ('triad_selection.json','triad_selection_pub.json','gpt_verify.json','classics_lens.json','review_factpack.json'):
        if not (REPORT/'data'/name).exists():raise RuntimeError(f'Missing central data: {name}')
    docs=[json.loads((REPORT/'data'/n).read_text(encoding='utf-8')) for n in ('triad_selection.json','triad_selection_pub.json','gpt_verify.json','classics_lens.json','review_factpack.json')]
    ids={d.get('factpack_id') for d in docs}
    if len(ids)!=1 or None in ids:raise RuntimeError('Mixed central snapshot versions')
    print('PASS: syntax, desktop imports, time zones and central factpack consistency.')

if __name__=='__main__':verify()
