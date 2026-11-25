"""A desktop-only cloud mirror for source records used by the stock screener."""
import base64
from datetime import datetime, timezone
import gzip
from hashlib import sha1
from io import BytesIO
import json
from pathlib import Path
import subprocess

FILE = 'fundamental_cloud_snapshot.json.gz'
REMOTE = 'fundamental-snapshot.json.gz'


def decode(raw, now):
    with gzip.open(BytesIO(raw), 'rb') as stream:
        content = stream.read(128_000_001)
    if len(content) > 128_000_000:
        raise ValueError('cloud financial snapshot too large')
    doc = json.loads(content)
    if (doc.get('version') != 'fundamental-snapshot-v2-recurring'
            or not isinstance(doc.get('records'), dict) or not doc['records']):
        raise ValueError('unexpected cloud financial schema')
    clock = datetime.fromisoformat(doc['fetched_at'])
    if clock.tzinfo is None or clock > now:
        raise ValueError('invalid cloud financial snapshot time')
    return doc


def records(base, now):
    from fundamental_snapshot import load
    local = load(base).get('records', {})
    path = Path(base) / 'data' / FILE
    if not path.is_file():
        return local
    try:
        cloud = decode(path.read_bytes(), now)['records']
    except (OSError, ValueError, KeyError, TypeError, EOFError):
        return local
    for code, row in cloud.items():
        prior = local.get(code,{})
        period = row.get('period_end') or ''
        if row.get('code') != code or not period:
            continue
        if (period, row.get('notice_date') or '') > (prior.get('period_end') or '',prior.get('notice_date') or ''):
            local[code] = row
    return local


def sync(base):
    from autonomous_sync import REPO, atomic_bytes, github_cli, remote_sha
    from sync_network import network_env
    path = Path(base) / 'data' / FILE
    mark = path.with_suffix('.sha')
    try:
        oid = remote_sha(REMOTE, timeout=10)
        if len(oid) != 40 or any(c not in '0123456789abcdef' for c in oid):
            raise ValueError('invalid blob id')
        if path.is_file() and mark.is_file() and mark.read_text() == oid:
            return {'status':'unchanged'}
        receipt = subprocess.run([github_cli(),'api',f'repos/{REPO}/git/blobs/{oid}'],
                                 capture_output=True, timeout=30, check=True, env=network_env())
        blob = json.loads(receipt.stdout)
        raw = base64.b64decode(blob['content'])
        if (blob.get('encoding') != 'base64' or len(raw) != blob['size']
                or sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest() != oid):
            raise ValueError('invalid cloud financial blob')
        incoming = decode(raw, datetime.now(timezone.utc))
        if path.is_file():
            try:
                previous = decode(path.read_bytes(), datetime.now(timezone.utc))
            except (OSError, ValueError, KeyError, TypeError, EOFError):
                previous = None
            if previous and datetime.fromisoformat(previous['fetched_at']) > datetime.fromisoformat(incoming['fetched_at']):
                return {'status':'retained_newer'}
        atomic_bytes(path,raw)
        atomic_bytes(mark,oid.encode())
        return {'status':'updated','source_asof':incoming['fetched_at']}
    except (OSError, ValueError, EOFError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        return {'status':'deferred','reason':type(exc).__name__}
