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
        content = None
        manifest = {'quality': {'status':'missing','issues':['报告核验失败：'+type(exc).__name__]}}
    quality = manifest.get('quality') or {}
    status = quality.get('status')
    if not content or status not in ('passed','plan_b'):
        # A missing AI/central publication cannot suppress independently dated
        # free-market observations. This fallback grants no trading authority.
        try:
            import json
            from v88_paths import core_root
            doc=json.loads((core_root()/'data/autonomous_report.json').read_text())
            if doc.get('version')=='autonomous-reports-v1' and doc.get('model_calls')==0 and doc.get('daily'):
                return doc['daily'], {'plan':'B','status':'plan_b','generated_at':doc['generated_at'],
                    'issues':['行情与规则观察稿；AI复审独立更新，不授交易权限'],'ts':datetime.fromisoformat(doc['generated_at']).timestamp(),
                    'basis':'autonomous_market_facts','quality':{'status':'plan_b'},'no_rating_authority':True}
        except (OSError,ValueError,KeyError,TypeError):pass
        return None, {**manifest,'plan':None,'status':'missing','issues':quality.get('issues') or ['当前没有通过原件和交易日核验的报告'],'ts':None}
    try:
        stamp = datetime.fromisoformat(str(manifest.get('generated_at')).replace('Z','+00:00'))
        ts = stamp.timestamp() if stamp.tzinfo else None
    except (ValueError,TypeError):
        ts=None
    return content, {**manifest,'plan':'B' if status=='plan_b' else 'A','status':status,
                     'issues':quality.get('issues') or [],'today_issues':quality.get('issues') or [],'ts':ts}
