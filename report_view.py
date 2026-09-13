"""Desktop adapter for the same report authority used by background consumers."""
from datetime import datetime
from pathlib import Path
import sys


def load_report(*, loader=None):
    try:
        if loader is None:
            source = str(Path(__file__).resolve().parent.parent/'ai-daily-report-v2'/'src')
            if source not in sys.path:
                sys.path.insert(0,source)
            from report_contract import load_publishable_report
            loader = load_publishable_report
        content, manifest = loader()
    except Exception as exc:
        return None, {'plan':None,'status':'missing','issues':['报告核验失败：'+type(exc).__name__],'ts':None}
    quality = manifest.get('quality') or {}
    status = quality.get('status')
    if not content or status not in ('passed','plan_b'):
        return None, {**manifest,'plan':None,'status':'missing','issues':quality.get('issues') or ['当前没有通过原件和交易日核验的报告'],'ts':None}
    try:
        stamp = datetime.fromisoformat(str(manifest.get('generated_at')).replace('Z','+00:00'))
        ts = stamp.timestamp() if stamp.tzinfo else None
    except (ValueError,TypeError):
        ts=None
    return content, {**manifest,'plan':'B' if status=='plan_b' else 'A','status':status,
                     'issues':quality.get('issues') or [],'today_issues':quality.get('issues') or [],'ts':ts}
