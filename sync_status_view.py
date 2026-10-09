"""One-line sync status; all secondary diagnostics stay collapsed."""
from html import escape
from module_freshness import record,display

STYLE='''<style>
.v88-sync-inline{font-family:-apple-system,BlinkMacSystemFont,sans-serif;color:#86868b;margin:2px 0 8px;padding:0;font-size:12px!important;line-height:1.5}
.v88-sync-inline>details>summary{display:flex;align-items:center;gap:10px;flex-wrap:wrap;list-style:none;cursor:pointer;min-height:28px;padding:2px 0;font-size:12px!important}
.v88-sync-inline summary::-webkit-details-marker{display:none}.v88-sync-inline .sync-track{display:inline-block;flex:0 0 64px;height:3px;border-radius:4px;background:#e1e6ec;overflow:hidden}.v88-sync-inline .sync-fill{display:block;height:100%;border-radius:4px;background:#007aff}
.v88-sync-inline .sync-number{font-size:12px!important;font-weight:600;color:#007aff;font-variant-numeric:tabular-nums}.v88-sync-inline .sync-time{font-size:12px!important;color:#86868b}.v88-sync-inline .sync-separator{color:#d1d1d6}.v88-sync-inline .sync-more{font-size:11px!important;color:#86868b}.v88-sync-inline .sync-more:after{content:' ＋'}.v88-sync-inline details[open] .sync-more:after{content:' −'}
.v88-sync-inline .sync-info{padding:12px 14px;background:#f5f5f7;border-radius:10px;margin:6px 0 10px;max-width:1000px;font-size:12px!important;line-height:1.8}.v88-sync-inline .sync-modules{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:10px}.v88-sync-inline .sync-module{font-size:12px!important;color:#86868b}.v88-sync-inline strong{color:#515154;font-weight:500}
@media(max-width:600px){.v88-sync-inline>details>summary{gap:6px}.v88-sync-inline .sync-modules{grid-template-columns:1fr}.v88-sync-inline .sync-track{flex-basis:40px}}
</style>'''

def html(board,receipt,state,*,running=False,opened=None,modules=(),base=None,now=None):
    e=lambda v:escape(str(v))
    complete=bool(board) and bool(receipt) and not running and state.get('status')!='failed'
    percent=100 if complete else 50 if board else 0
    failed=state.get('status')=='failed' and not running
    label='同步中' if running else '待重试' if failed else '已同步' if complete else '待同步'
    r=record('strategy_board.json',board,base,now)
    out=[STYLE,f'<div class="v88-sync-inline"><details><summary aria-label="同步进度与时间详情"><span class="sync-track" role="progressbar" aria-label="数据同步进度" aria-valuemin="0" aria-valuemax="100" aria-valuenow="{percent}"><span class="sync-fill" style="width:{percent}%"></span></span><span class="sync-number">{percent}%</span><span>{label}</span><span class="sync-separator">·</span><span class="sync-time">筛选 {e(r["generated"])}</span><span class="sync-separator">·</span><span class="sync-time">缓存 {e(r["cache_age"])}</span><span class="sync-more">详情</span></summary>',
         f'<div class="sync-info">北京时间 · 结果距今 {e(r["age"])} · 缓存写入 {e(r["cached"])}<br>本次打开 {e(display(opened))} · 云端核对 {e(display(receipt.get("at")))} · 云端发布 {e(display(receipt.get("cloud_generated_at")))}<br>进度表示读取与云端核对阶段，不代表行情覆盖率；刷新页面不改写筛选时间。<div class="sync-modules">']
    for m in modules:
        ago=lambda value:'未记录' if value=='未知' else value+'前'
        out.append(f'<div class="sync-module"><strong>{e(m["module"])}</strong><br>计算 {e(m["generated"])} · {e(ago(m["age"]))}<br>缓存写入 {e(m["cached"])} · {e(ago(m["cache_age"]))}</div>')
    out.append('</div></div></details></div>')
    return ''.join(out)
