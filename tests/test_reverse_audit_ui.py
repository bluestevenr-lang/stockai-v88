from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reverse_audit_ui import html

def inputs():
    now = datetime.now(timezone.utc)
    return ({'version':'reverse-audit-v1','generated_at':now.isoformat(),'input_id': 'hash', 'factpack_id': 'pack', 'source_generated_at': 'original',
             'summary': {'audited_rows': 8, 'open_issues': 1, 'by_horizon': {'short': {'1A': 3}}},
             'issues': [{'priority':'P0', 'title':'<script>bad</script>', 'next_action':'补源'}]},
            {'ok': True, 'input_id': 'hash', 'checked_at': now.isoformat()},
            {'factpack_id': 'pack', 'generated_at': 'original'}, now)

def test_medium_long_position_does_not_relabel_short_and_escapes():
    out = html(*inputs())
    assert '中长期：3A 0 · 2A 0 · 1A 0' in out
    assert '&lt;script&gt;' in out and '<script>' not in out
    assert '短期未来8周' in out and '中期8–24周' in out and '长期12–36周' in out
    assert '审核分不是胜率' in out

def test_old_or_failed_or_mixed_report_is_not_current():
    doc, status, selection, now = inputs()
    assert '超过10分钟' in html(doc, status, selection, now+timedelta(minutes=11))
    status['ok'] = False
    assert '本轮未就绪' in html(doc, status, selection, now)
    status['ok'] = True; selection['factpack_id'] = 'new'
    assert '本轮未就绪' in html(doc, status, selection, now)

def test_missing_audit_keeps_clear_horizon_without_fake_counts():
    out = html()
    assert '三周期3A研究' in out and '本轮未就绪' in out
    assert '中长期：3A 0' not in out


def test_missing_identity_future_report_and_mismatched_status_fail_closed():
    doc,status,selection,now=inputs()
    for change in [{'input_id':None},{'factpack_id':None},{'source_generated_at':None},
                   {'generated_at':(now+timedelta(minutes=1)).isoformat()},{'version':'old'}]:
        assert '本轮未就绪' in html({**doc,**change},status,selection,now)
    assert '本轮未就绪' in html(doc,{**status,'factpack_id':'other'},selection,now)
