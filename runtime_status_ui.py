"""Compact runtime freshness shared by phone, desktop, and Feishu."""
from datetime import datetime,timezone
from html import escape
import json
from pathlib import Path
ROOT=Path('/Users/bluesteven/Desktop/ai-daily-report-v2/data')

def status(doc,now=None):
 now=now or datetime.now(timezone.utc)
 try:
  age=(now-datetime.fromisoformat(doc['checked_at'])).total_seconds()
  if not 0<=age<=900:return '🔴 后台心跳过期：保留旧数据，不能认定最新'
 except (ValueError,KeyError,TypeError):return '🔴 后台状态未核实'
 icon='🟢' if doc.get('ui_ok') and doc.get('data_current_90') and doc.get('state')=='后台运行正常' else '🟠'
 return icon+' '+doc.get('state','状态待核')

def html(root=ROOT):
 try:d=json.loads((Path(root)/'runtime_status_pub.json').read_text())
 except (OSError,ValueError):d={}
 e=lambda v:escape(str(v if v is not None else '未核实'))
 lines=[f"{m}：有效日线{r.get('valid_pct')}%，应到{r.get('required_session')}" for m,r in d.get('markets',{}).items()]
 return '<details class="v88-runtime" style="font-size:11px;margin:4px 0;color:#475569"><summary>'+e(status(d))+' · 不打开网页也定时更新</summary><div>'+e('；'.join(lines))+'</div><div>后台检查 '+e(d.get('checked_at'))+'；中央发布 '+e(d.get('central_generated_at'))+'</div><div>'+e(d.get('meaning'))+'</div></details>'
