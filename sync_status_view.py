"""Compact, accessible sync status. Percentages describe completed sync stages."""
from html import escape
from module_freshness import record,display

STYLE='''<style>
.v88-sync-card{box-sizing:border-box;max-width:1180px;margin:12px auto 24px;padding:26px 30px;border:1px solid rgba(0,0,0,.055);border-radius:24px;background:linear-gradient(135deg,#fff 55%,#f8faff);box-shadow:0 8px 30px rgba(30,45,70,.045);color:#1d1d1f;font-family:-apple-system,BlinkMacSystemFont,"SF Pro Display","PingFang SC",sans-serif}
.v88-sync-card *{box-sizing:border-box}.v88-sync-card .sync-top{display:flex;justify-content:space-between;align-items:center;gap:24px}
.v88-sync-card .sync-eyebrow{font-size:11px!important;letter-spacing:1.5px;color:#86868b;margin-bottom:10px;font-weight:600}
.v88-sync-card .sync-title{font-size:24px!important;line-height:1.25;font-weight:650;letter-spacing:-.7px;margin:0!important}
.v88-sync-card .sync-subtitle{font-size:13px!important;color:#86868b;margin-top:9px;line-height:1.6}
.v88-sync-card .sync-number{font-size:38px!important;line-height:1;font-weight:600;letter-spacing:-1.8px;color:#007aff;font-variant-numeric:tabular-nums;white-space:nowrap}
.v88-sync-card .sync-number span{font-size:18px!important;font-weight:500;margin-left:3px;letter-spacing:0}
.v88-sync-card .sync-track{height:6px;border-radius:9px;background:#e9edf3;overflow:hidden;margin:20px 0 17px}
.v88-sync-card .sync-fill{height:100%;border-radius:9px;background:linear-gradient(90deg,#007aff,#5aabff);transition:width .45s ease}
.v88-sync-card .sync-steps{display:flex;gap:20px;font-size:12px!important;color:#86868b;flex-wrap:wrap}
.v88-sync-card .sync-done{color:#35746a}.v88-sync-card .sync-active{color:#007aff}.v88-sync-card .sync-meta{display:flex;gap:12px 30px;flex-wrap:wrap;margin-top:23px;font-size:13px!important;color:#86868b}.v88-sync-card .sync-meta b{display:inline-block;color:#424245;font-weight:500;margin-left:7px;font-variant-numeric:tabular-nums;font-size:13px!important}
.v88-sync-card .sync-footer{display:flex;justify-content:space-between;gap:14px;align-items:center;flex-wrap:wrap;margin-top:20px;padding-top:17px;border-top:1px solid #f0f0f2}
.v88-sync-card .sync-markets{display:flex;gap:9px;flex-wrap:wrap}.v88-sync-card .sync-market{padding:7px 11px;border-radius:9px;background:#f5f5f7;color:#6e6e73;font-size:12px!important}.v88-sync-card .sync-market b{color:#424245;font-weight:600;margin-right:9px;font-size:12px!important}
.v88-sync-card details{margin-top:15px}.v88-sync-card summary{cursor:pointer;color:#007aff;font-size:13px!important;list-style:none;width:fit-content}.v88-sync-card summary::-webkit-details-marker{display:none}.v88-sync-card summary:after{content:' ＋'}.v88-sync-card details[open]>summary:after{content:' −'}
.v88-sync-card .sync-detail-note{color:#86868b;font-size:12px!important;line-height:1.7;margin:15px 0 9px}.v88-sync-card .sync-modules{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.v88-sync-card .sync-module{background:#f7f8fa;border-radius:12px;padding:13px 15px;font-size:12px!important;line-height:1.8;color:#86868b}.v88-sync-card .sync-module strong{display:block;color:#424245;font-weight:550;font-size:13px!important}
@media(max-width:650px){.v88-sync-card{padding:22px 20px;border-radius:20px}.v88-sync-card .sync-title{font-size:21px!important}.v88-sync-card .sync-number{font-size:32px!important}.v88-sync-card .sync-modules{grid-template-columns:1fr}.v88-sync-card .sync-meta{gap:9px 18px}}
@media(prefers-reduced-motion:reduce){.v88-sync-card .sync-fill{transition:none}}
</style>'''

def html(board,receipt,state,*,running=False,opened=None,modules=(),base=None,now=None):
    e=lambda v:escape(str(v))
    complete=bool(board) and bool(receipt) and not running and state.get('status')!='failed'
    percent=100 if complete else 50 if board else 0
    failed=state.get('status')=='failed' and not running
    title='正在同步' if running else '已载入保存结果' if failed or not receipt else '准备就绪'
    subtitle='已有结果可以查看，正在核对云端版本' if running else '云端暂未连接，稍后自动重试' if failed else '已核对云端版本 · 行情日期以各股记录为准' if receipt else '等待核对云端版本'
    r=record('strategy_board.json',board,base,now)
    steps=f'<span class="sync-done">✓ 读取结果</span><span class="{"sync-done" if complete else "sync-active"}">{"✓" if complete else "②"} 核对云端</span>' if board else '<span class="sync-active">① 读取结果</span><span>② 核对云端</span>'
    out=[STYLE,f'<section class="v88-sync-card" aria-label="V88 数据同步状态"><div class="sync-top"><div><div class="sync-eyebrow">V88 · 数据状态</div><h2 class="sync-title">{title}</h2><div class="sync-subtitle">{subtitle}</div></div><div class="sync-number">{percent}<span>%</span></div></div>',
         f'<div class="sync-track" role="progressbar" aria-label="数据同步进度" aria-valuemin="0" aria-valuemax="100" aria-valuenow="{percent}"><div class="sync-fill" style="width:{percent}%"></div></div>',
         '<div class="sync-steps">'+steps+'</div>',
         f'<div class="sync-meta"><span>筛选完成<b>{e(r["generated"])}</b></span><span>结果距今<b>{e(r["age"])}</b></span><span>缓存年龄<b>{e(r["cache_age"])}</b></span></div><div class="sync-footer"><div class="sync-markets">']
    for market in ('A股','港股','美股'):
        counts=' · '.join(f'{tier} {sum(r.get("market")==market and r.get("strategy_tier")==tier and not r.get("grade_pending") for r in board.get("rows",[]))}' for tier in ('3A','2A','1A'))
        out.append(f'<span class="sync-market"><b>{market}</b>{counts}</span>')
    out.append('</div><span style="color:#a1a1a6;font-size:11px!important">北京时间 · 名单数量</span></div><details><summary>查看更新时间与缓存详情</summary>')
    out.append(f'<div class="sync-detail-note">本次打开 {e(display(opened))}　·　云端核对 {e(display(receipt.get("at")))}　·　云端发布 {e(display(receipt.get("cloud_generated_at")))}<br>进度表示读取与同步阶段，不代表行情覆盖率。刷新页面不改变原始筛选时间。</div><div class="sync-modules">')
    for m in modules:
        m={**m,'age':('未记录' if m['age']=='未知' else m['age']+'前'),'cache_age':('未记录' if m['cache_age']=='未知' else m['cache_age']+'前')}
        out.append(f'<div class="sync-module"><strong>{e(m["module"])}</strong>计算 {e(m["generated"])} · {e(m["age"])}<br>缓存写入 {e(m["cached"])} · {e(m["cache_age"])}</div>')
    out.append('</div></details></section>')
    return ''.join(out)
