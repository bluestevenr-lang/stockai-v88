"""Windows mirror updater: checked pulls, dependencies and an optional data bundle.

Never stages/commits/pushes user files and never invokes reviews or messaging.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
import webbrowser
import zipfile

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT.parent / 'ai-daily-report-v2'
RELEASE = '2026.09.13'


def run(args, cwd=None, capture=False):
    result = subprocess.run([str(a) for a in args], cwd=cwd, check=True,
                            stdout=subprocess.PIPE if capture else None,
                            stderr=subprocess.PIPE if capture else None,
                            text=True, encoding='utf-8', errors='replace')
    return result.stdout.strip() if capture else ''


def git(root, *args, capture=False):
    return run(['git', '-C', root, *args], capture=capture)


def sync_repo(root, private=False):
    if not (root / '.git').exists():
        raise RuntimeError(f'Missing repository: {root}')
    if git(root, 'diff', '--name-only', '--diff-filter=U', capture=True):
        raise RuntimeError(f'Unresolved Git conflict: {root}. No files changed.')
    # Do not silently archive locally edited code or change branch history.
    # Existing generated data can use the established private merge drivers.
    if private:
        bash = shutil.which('bash')
        if not bash:
            candidate = Path(os.environ.get('ProgramFiles', 'C:/Program Files')) / 'Git/bin/bash.exe'
            bash = str(candidate) if candidate.exists() else None
        if not bash:
            raise RuntimeError('Git for Windows bash is required.')
        # Older checkouts may have CRLF before the new .gitattributes arrives.
        for name in ('safe_pull.sh','setup_merge_driver.sh'):
            path=root/'scripts'/name
            if path.exists():
                raw=path.read_bytes()
                if b'\r\n' in raw:path.write_bytes(raw.replace(b'\r\n',b'\n'))
        # Older Windows Git helpers call python3 although only py.exe exists.
        with tempfile.TemporaryDirectory(prefix='v88-python-') as tmp:
            shim=Path(tmp)/'python3'
            shim.write_bytes(b'#!/usr/bin/env bash\nexec "$V88_PYTHON" "$@"\n')
            shim.chmod(0o700)
            previous=os.environ.get('PATH','')
            os.environ['PATH']=tmp+os.pathsep+previous
            try:run([bash, root / 'scripts/safe_pull.sh'], cwd=root)
            finally:os.environ['PATH']=previous
    else:
        git(root, 'pull', '--ff-only', '--autostash', 'origin', 'main')
    if git(root, 'diff', '--name-only', '--diff-filter=U', capture=True):
        raise RuntimeError(f'Git conflict after update: {root}; resolve before continuing.')
    print(f'Updated {root.name}: {git(root, "rev-parse", "--short", "HEAD", capture=True)}')


def install_dependencies(root, report):
    paths = [root/'requirements.txt', report/'requirements.txt']
    stamp = hashlib.sha256(sys.executable.encode()+b''.join(p.read_bytes() for p in paths)).hexdigest()
    marker = root/'win/logs/dependencies.sha256'
    if marker.exists() and marker.read_text().strip() == stamp:
        print('Dependencies unchanged.'); return
    run([sys.executable, '-m', 'pip', 'install', '-r', paths[0], '-r', paths[1], 'tzdata'])
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(stamp, encoding='utf-8')


def install_snapshot(report):
    manifest_path = report/'sync/windows_snapshot.json'
    if not manifest_path.exists():
        print('No bundled snapshot; existing local data retained.'); return
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    archive = report/'sync/windows_snapshot.zip'
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    if digest != manifest['sha256']:
        raise ValueError('Data bundle checksum mismatch')
    marker = report/'data/.windows_snapshot.json'
    if marker.exists() and json.loads(marker.read_text(encoding='utf-8')).get('sha256') == digest:
        print('Data bundle already installed; preserving subsequent local updates.'); return
    members = manifest['files']
    # This allowlist cannot overwrite local positions, trades, secrets or databases.
    if any('/' in n or '\\' in n or not n.endswith('.json') or
           (n.startswith(('positions', 'watchlist', 'astra_trade', '.')) or n in {'accounts.json','financial_primary_evidence.json','financial_capital_evidence.json','financial_statement_index.json'}) for n in members):
        raise ValueError('Unsafe snapshot member')
    data = report/'data'; data.mkdir(exist_ok=True)
    backups = report/'.v88-work/windows-backups'/datetime.now().strftime('%Y%m%d-%H%M%S')
    with tempfile.TemporaryDirectory(dir=report) as temp:
        stage = Path(temp)
        with zipfile.ZipFile(archive) as z:
            if set(z.namelist()) != set(members):
                raise ValueError('Snapshot manifest/member mismatch')
            for name, expected in members.items():
                raw = z.read(name)
                if hashlib.sha256(raw).hexdigest() != expected:
                    raise ValueError(f'Snapshot file checksum mismatch: {name}')
                json.loads(raw)
                (stage/name).write_bytes(raw)
        # Preserve exact prior versions and roll back the full set on write failure.
        backups.mkdir(parents=True)
        installed = []
        try:
            for name in members:
                dest=data/name
                if dest.exists(): shutil.copy2(dest, backups/name)
                installed.append(name)
                os.replace(stage/name, dest)
            marker.write_text(json.dumps({'sha256':digest,'installed_at':datetime.now(timezone.utc).isoformat(),
                                          'source_at':manifest['source_at']}),encoding='utf-8')
        except Exception:
            for name in reversed(installed):
                old=backups/name
                if old.exists(): shutil.copy2(old,data/name)
                else: (data/name).unlink(missing_ok=True)
            raise
    print(f'Installed {len(members)} data views, original source timestamps retained.')
    print(f'Previous data backup: {backups}')


def start_app(root):
    url='http://127.0.0.1:8501'
    # Ask the user to close the existing server rather than kill unrelated Python.
    try:
        with urllib.request.urlopen(url+'/_stcore/health',timeout=2) as r:
            if r.status == 200:
                print('V88 is running. Refresh the page; restart its console if still showing old modules.')
                webbrowser.open(url); return
    except OSError: pass
    logs=root/'win/logs';logs.mkdir(parents=True,exist_ok=True)
    with (logs/'streamlit.log').open('ab') as stream:
        subprocess.Popen([sys.executable,'-m','streamlit','run','app_v88_integrated.py',
                          '--server.address','127.0.0.1','--server.headless','true','--server.port','8501'],
                         cwd=root,stdout=stream,stderr=subprocess.STDOUT,
                         creationflags=getattr(subprocess,'CREATE_NEW_PROCESS_GROUP',0))
    for _ in range(40):
        try:
            with urllib.request.urlopen(url+'/_stcore/health',timeout=2) as r:
                if r.status==200:webbrowser.open(url);return
        except OSError:time.sleep(1)
    raise RuntimeError(f'V88 did not start. See {logs / "streamlit.log"}')


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--start',action='store_true');parser.add_argument('--after-pull',action='store_true')
    args=parser.parse_args()
    if sys.version_info < (3,12):raise RuntimeError('Python 3.12 or newer is required')
    os.environ['PYTHONUTF8']='1';os.environ['V88_PYTHON']=sys.executable
    if not args.after_pull:
        sync_repo(ROOT)
        # Execute the newly downloaded updater instead of the old in-memory version.
        return run([sys.executable,ROOT/'win/update_v88.py','--after-pull',*(['--start'] if args.start else [])])
    sync_repo(REPORT,private=True)
    install_dependencies(ROOT,REPORT)
    install_snapshot(REPORT)
    run([sys.executable,ROOT/'win/verify_runtime.py'],cwd=ROOT)
    print(f'V88 {RELEASE}: UPDATE VERIFIED. No model call or message was sent.')
    if args.start:start_app(ROOT)


if __name__=='__main__':
    try:main()
    except Exception as exc:
        print(f'UPDATE FAILED: {exc}',file=sys.stderr)
        sys.exit(1)
